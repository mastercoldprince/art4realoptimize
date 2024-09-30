#include "Tree.h"
#include "RdmaBuffer.h"
#include "Timer.h"
#include "Node.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>
#include <atomic>
#include <mutex>
#include <fstream>


double cache_miss[MAX_APP_THREAD];
double cache_hit[MAX_APP_THREAD];
uint64_t lock_fail[MAX_APP_THREAD];
// uint64_t try_lock[MAX_APP_THREAD];
uint64_t write_handover_num[MAX_APP_THREAD];
uint64_t try_write_op[MAX_APP_THREAD];
uint64_t read_handover_num[MAX_APP_THREAD];
uint64_t try_read_op[MAX_APP_THREAD];
uint64_t read_leaf_retry[MAX_APP_THREAD];
uint64_t leaf_cache_invalid[MAX_APP_THREAD];
uint64_t try_read_leaf[MAX_APP_THREAD];
uint64_t read_node_repair[MAX_APP_THREAD];
uint64_t try_read_node[MAX_APP_THREAD];
uint64_t read_node_type[MAX_APP_THREAD][MAX_NODE_TYPE_NUM];
uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
volatile bool need_stop = false;
uint64_t retry_cnt[MAX_APP_THREAD][MAX_FLAG_NUM];
uint64_t MN_iops[MAX_APP_THREAD][MEMORY_NODE_NUM];
uint64_t MN_datas[MAX_APP_THREAD][MEMORY_NODE_NUM];

int update_retry_flag[MAX_APP_THREAD];
uint64_t retry_time[MAX_APP_THREAD];
uint64_t insert_time[MAX_APP_THREAD];

uint64_t insert_cnt[MAX_APP_THREAD];
uint64_t internal_empty_entry[MAX_APP_THREAD];
uint64_t internal_extend_empty_entry[MAX_APP_THREAD];
uint64_t internal_header_split[MAX_APP_THREAD];
uint64_t buffer_empty_entry[MAX_APP_THREAD];
uint64_t buffer_header_split[MAX_APP_THREAD];
uint64_t buffer_reconstruct[MAX_APP_THREAD];
uint64_t in_place_update[MAX_APP_THREAD];





thread_local CoroCall Tree::worker[MAX_CORO_NUM];
thread_local CoroCall Tree::master;
thread_local CoroQueue Tree::busy_waiting_queue;
std::atomic<int> cnt = 0;




Tree::Tree(DSM *dsm, uint16_t tree_id) : dsm(dsm), tree_id(tree_id) {
  assert(dsm->is_register());

#ifdef TREE_ENABLE_CACHE
  // init local cache
// #ifdef CACHE_ENABLE_ART
  index_cache = new RadixCache(define::kIndexCacheSize, dsm);
// #else
//   index_cache = new NormalCache(define::kIndexCacheSize, dsm);
// #endif
#endif

  local_lock_table = new LocalLockTable();

  root_ptr_ptr = get_root_ptr_ptr();

  // init root entry to Null
  auto entry_buffer = (dsm->get_rbuf(0)).get_entry_buffer();
  dsm->read_sync((char *)entry_buffer, root_ptr_ptr, sizeof(InternalEntry));
  auto root_ptr = *(InternalEntry *)entry_buffer;
  if (dsm->getMyNodeID() == 0 && root_ptr != InternalEntry::Null()) {
    auto cas_buffer = (dsm->get_rbuf(0)).get_cas_buffer();
retry:
    bool res = dsm->cas_sync(root_ptr_ptr, (uint64_t)root_ptr, (uint64_t)InternalEntry::Null(), cas_buffer);
    if (!res && (root_ptr = *(InternalEntry *)cas_buffer) != InternalEntry::Null()) {
      goto retry;
    }
  }
}

void Tree::clear_cache() {
  index_cache->clear();
}

GlobalAddress Tree::get_root_ptr_ptr() {
  GlobalAddress addr;
  addr.nodeID = 0;
  addr.offset = define::kRootPointerStoreOffest + sizeof(GlobalAddress) * tree_id;
  return addr;
}

InternalEntry Tree::get_root_ptr(CoroContext *cxt, int coro_id) {
  auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  dsm->read_sync((char *)entry_buffer, root_ptr_ptr, sizeof(InternalEntry), cxt);
  return *(InternalEntry *)entry_buffer;
}

void Tree::insert(const Key &k, Value v, CoroContext *cxt, int coro_id, bool is_update, bool is_load) {
  assert(dsm->is_register());
  int leaf_type=-1;
  int leaf_size =0;
  int klen=128,vlen=1024;   //应该从后往前找！
  {
  int i=127,j=1023;
  while(k[i--] == 0) klen --;
  while(v[j--] == 0) vlen --;
  if(klen<=8) {leaf_type = 1; leaf_size = 8;}
  else if ( 8< klen&& klen <=16) {leaf_type = 5;leaf_size = 16;}
  else if (16<klen && klen <= 32 ) {leaf_type = 9;leaf_size = 32;}
  else if (32 <klen && klen <=64 ) {leaf_type = 13;leaf_size = 64;}
  else {leaf_type = 17;leaf_size = 128;}
  if(vlen<=16) {leaf_type += 1;leaf_size += 16;}
  else if ( 16< vlen&& vlen <=256) {leaf_type += 2; leaf_size += 256;}
  else if (256<vlen && vlen <= 512 ) {leaf_type += 3;leaf_size += 512;}
  else {leaf_type += 4;leaf_size += 1024;}
  }
  int cnt_res=cnt.fetch_add(1);
  uint64_t k_v = key2int(k);

 // if(cnt_res >22653500)printf("%d thread %d insert kv: %d\n",cnt_res ,(int)dsm->getMyThreadID( ),(int)key2int(k));
  // traversal
  GlobalAddress p_ptr;
  InternalEntry p;
  BufferEntry bp;
  GlobalAddress node_ptr;  // node address(excluding header)
  int depth;
  int retry_flag = FIRST_TRY;
//  uint32_t fp = generateFingerprint(k);

  // cache
  bool from_cache = false;
  CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  CacheEntry* cache_entry_parent;
  int entry_idx = -1;
  int cache_depth = 0;

  // temp
  GlobalAddress leaf_addr = GlobalAddress::Null();
  char* page_buffer;
  bool is_valid, type_correct;
  InternalPage* p_node = nullptr;
  InternalBuffer* bp_node = nullptr;
  Header hdr;
  BufferHeader bhdr;
  int max_num;
  uint64_t* cas_buffer;
  int debug_cnt = 0;
  int parent_type = 0; //0 ->internal 1->buffer
  int parent_parent_type = -1;
  bool buffer_from_cache_flag = 0;
  int first_buffer = 0;
  InternalPage parent_page;
  InternalBuffer parent_buffer;
  insert_cnt[dsm->getMyThreadID()] ++ ;

  //search from cache

  from_cache = index_cache->search_from_cache(k, entry_ptr_ptr, entry_ptr, parent_parent_type,entry_idx,cache_entry_parent,first_buffer);   //check   直接从cache里面找到一个 
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    node_ptr = entry_ptr->addr;
    depth = entry_ptr->depth;
    parent_type  = entry_ptr->node_type;
    if(entry_ptr->node_type == 1)   //如果cache找到的缓冲节点则直接去读吧！！！  后面如果是从cache来的 并且类型就是一个缓冲节点就不用再读一遍了 还是再读一次吧、、、
    { 
      if(first_buffer) 
      {
        p_ptr = root_ptr_ptr;
        p = get_root_ptr(cxt, coro_id);
        parent_type = 0;
        depth = 0;
      }
      else{
        p_ptr = GADD(cache_entry_parent->addr,sizeof(InternalEntry)*entry_idx);
        p = cache_entry_parent->records[entry_idx];
        parent_type = cache_entry_parent->node_type;
        depth = cache_entry_parent->depth;
        node_ptr = cache_entry_parent->addr;
      }
      buffer_from_cache_flag = true;
    }
    bp.val = p.val;
  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }


  depth ++;  // partial key in entry is matched
  cache_depth = depth;

  UNUSED(is_update);  // is_update is only used in ROWEX_ART baseline

  int retry_read_buffer = 0;
next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;
if(parent_type ==0)  //一个内部节点    1.继续往下找  2. 有一个空槽 生成新的缓冲节点 3.内部节点分裂 分裂之后生成新的缓冲节点 4.内部节点满了扩展  并生成新的缓冲节点  
{
  if (p == InternalEntry::Null()) {  //只有可能是根节点
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    //新建一个缓冲节点 和叶节点 一起写过去 最后cas
    bool res = out_of_place_write_buffer_n_leaf(k,v,depth,leaf_addr,leaf_type,klen,vlen,p_ptr,p,node_ptr, cas_buffer,cxt,coro_id); //partial key正确

    // cas fail, retry
    if (!res) {   //只会发生一次 所以一定会成功匹配上
      update_retry_flag[dsm->getMyThreadID()]=1;
      retry_flag = CAS_NULL;
      p = *(InternalEntry*) cas_buffer;
      goto next;
    }
    internal_empty_entry[dsm->getMyThreadID()] ++;
    goto insert_finish;
  }
  if(p.child_type == 1)   //找buffer node 看有没有空的
  {

    bool is_match;
    auto buffer_buffer =  (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    GlobalAddress addr = p.addr();
//    if(buffer_from_cache_flag)
    {
//      bp_node =new InternalBuffer(entry_ptr->depth,entry_ptr->records);
      //is_valid？ 本地的节点如何验证 is valid？？   不用验证 ？
    }
//    else
{
      is_valid = read_buffer_node(addr, buffer_buffer, p_ptr, depth, from_cache,cxt, coro_id);   
      bp_node = (InternalBuffer *)buffer_buffer;
//      parent_buffer = *bp_node;
          //3.1 check partial key
      if (!is_valid) {  // node deleted || outdated cache entry in cached node
        if (from_cache) {
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
        }
        // re-read node entry
        auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
        dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
        p = *(InternalEntry *)entry_buffer;
        from_cache = false;
        retry_flag = INVALID_Buffer_NODE;
        goto next;
      }
    }

    bhdr=bp_node->hdr;
    if (depth == bhdr.depth) {
    //  printf("thread  %d 3 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)bp_node->hdr);
      index_cache->add_to_cache(k, 1,(InternalPage *)bp_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(BufferHeader)));
    }
/*    if(depth >bhdr.depth) 
    printf("noooooooooooooooooooooooooooooooooooooooooooooo1!!!!!!!");*/

    for (int i = 0; i < bhdr.partial_len; ++ i) {    //缓冲节点分裂   新建一个共同前缀的内部节点
    if (get_partial(k, bhdr.depth + i) != bhdr.partial[i]) {     //
      //3.2 partial key not match, need split
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      int partial_len = bhdr.depth + i - depth;  // hdr.depth may be outdated, so use partial_len wrt. depth
      bool res = out_of_place_write_node(k, v, depth, leaf_addr, leaf_type,  klen,vlen,partial_len,bhdr.partial[i], p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);   //partial key正确
      if (!res) {  //失败的话一定是对同一个缓冲节点做分裂
        p = *(InternalEntry*) cas_buffer;
        from_cache = false;
        retry_flag = SPLIT_Buffer_HEADER;
        goto next;
      }
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
      // udpate cas header. Optimization: no need to snyc; mask node_type
      auto header_buffer = (dsm->get_rbuf(coro_id)).get_header_buffer();
      auto new_hdr = BufferHeader::split_header(bhdr, i);

      bool res_d = dsm->cas_sync(GADD(p.addr(), sizeof(GlobalAddress)), (uint64_t)bhdr, (uint64_t)new_hdr, header_buffer, cxt);
      buffer_header_split[dsm->getMyThreadID()] ++;
      goto insert_finish;
    }
    }
    assert(bhdr.depth !=0);
    depth = bhdr.depth + bhdr.partial_len;
    auto partial = get_partial(k, depth);  //获取需要匹配的关键字 应该是缓冲节点的深度再加上partial len
    GlobalAddress leaf_addrs[256];
    GlobalAddress leaves_ptr[256];
    memset(leaf_addrs,0,256*sizeof(GlobalAddress));
    memset(leaves_ptr,0,256*sizeof(GlobalAddress));
    int leaf_cnt = 0;
    //3.3 search an exists slot first 
    for(int i=0;i < 256;i++)   //bp node 全空？
    {
      if(bp_node->records[i] != BufferEntry::Null()&&bp_node->records[i].partial == partial )
      {
      //  assert(bp_node->records[i].addr().nodeID == 0);
        if(bp_node->records[i].node_type == 1 || bp_node->records[i].node_type == 2)   //是一个缓冲节点 或者内部节点 继续往下找 
        {
          bp = bp_node->records[i];
          p_ptr = GADD(p.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          depth ++;
          parent_type = 1;
          from_cache = false;
          retry_flag = FIND_NEXT;
          goto next;
        }
        else 
        {
          leaf_addrs[leaf_cnt] = bp_node->records[i].addr();
          leaves_ptr[leaf_cnt]  = GADD(p.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          leaf_cnt ++;   
        }
      }
    }
    if(leaf_cnt !=0)   //将所有的叶子读过来 看有没有重复的 
    {
        auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_range_buffer(); 

        is_valid = read_leaves(leaf_addrs, leaf_buffer,leaf_cnt,leaves_ptr,from_cache,cxt,coro_id);

        if (!is_valid) {
          if (from_cache) {
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
          }
          // re-read leaf entry
          auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
          dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(BufferEntry), cxt);
          p = *(InternalEntry *)entry_buffer;
          from_cache = false;
          buffer_from_cache_flag = false;
          retry_flag = INVALID_LEAF;
          goto next;
        }
        for(int i =0;i<leaf_cnt;i++)
        {
          auto leaf = (Leaf_kv*) leaf_buffer + i* define::allocAlignKVLeafSize;
          auto _k = leaf->get_key();

          // 2.3 Check if it is the key we search
          if (_k == k) {    //叶节点不相等咋办  不相等在后面找空位插入
            if (is_load) {
               goto insert_finish;
                }
              in_place_update_leaf(k,v,leaf_addrs[i],leaf_type,leaf,cxt,coro_id);   
              in_place_update[dsm->getMyThreadID()]++;
              goto insert_finish;
          }
        }
    }
    //3.4 still have empty slot  不存在部分键相同的情况  有的话 则往下找 否则放空位 
  //  if(bhdr.count_1+bhdr.count_2 < 256)
   // {
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

      GlobalAddress be_ptr;
      BufferEntry old_be;
     // uint8_t partial;

//      if(get_partial(k, bhdr.depth + bhdr.partial_len-1) == bhdr.partial[bhdr.partial_len-1])
//      {
        for(int i=0;i < 256;i++)
        {
          if(bp_node->records[i] == BufferEntry::Null()) //If we are at a  buffer  empty and partial key match
          {
           depth ++;
           old_be = bp_node->records[i];
           be_ptr=GADD(p.addr(), sizeof(GlobalAddress) + sizeof(BufferHeader) + i * sizeof(BufferEntry));
           auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
           bool res = out_of_place_write_leaf(k,v,depth,leaf_addr,leaf_type ,klen,vlen,be_ptr,old_be,cas_buffer,cxt,coro_id);  //直接写空槽
           if(res) 
           {
            buffer_empty_entry[dsm->getMyThreadID()] ++;
            goto insert_finish;
           }
           else {
            auto e = *(BufferEntry*) cas_buffer;
            if (e.partial == get_partial(k, depth - 1)) {  // same partial keys insert to the same empty slot  再次查找本层 
              bp = e;
              p_ptr=be_ptr;
              from_cache = false;
              parent_type = 1;
              retry_flag = CAS_Buffer_EMPTY;
              goto next;  // search next level
              }
          }
        }
        }
        bool res=out_of_place_write_buffer_node(k, v,depth,bp_node,leaf_type,klen,vlen,leaf_addr,entry_ptr_ptr,entry_ptr,from_cache,p, p_ptr,cxt,coro_id);
        if (!res) {  //获取锁失败
        //  p = *(InternalEntry*) cas_buffer;
          retry_flag = Buffer_Switch_type;
          from_cache = false;
          goto next;
        }
        buffer_reconstruct[dsm->getMyThreadID()]++;
        goto insert_finish;

 //         }
  //  }
  }
  //内部节点
  // 3. Find out a node
  // 3.1 read the node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth,from_cache,cxt, coro_id);
  p_node = (InternalPage *)page_buffer;
  parent_page = *p_node;


  if (!is_valid) {
    update_retry_flag[dsm->getMyThreadID()]=1;

    // invalidate the old node cache
    if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }

    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_Internal_NODE;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
  if (from_cache && !type_correct) {  // invalidate the out dated node type
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
  if (depth == hdr.depth) {
 //   printf("thread  %d 4 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)p_node->hdr);
    index_cache->add_to_cache(k, 0,p_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }

  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) {
      // need split
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      int partial_len = hdr.depth + i - depth;  // hdr.depth may be outdated, so use partial_len wrt. depth
      bool res = out_of_place_write_node(k, v,depth,leaf_addr,leaf_type,klen,vlen,partial_len,hdr.partial[i], p_ptr, p, node_ptr, cas_buffer, cxt, coro_id);   //内部节点分裂  分裂后往新的内部节点下申请一个新的缓冲节点和叶节点 partial key没问题

      // cas fail, retry
      if (!res) {
        update_retry_flag[dsm->getMyThreadID()]=1;
        p = *(InternalEntry*) cas_buffer;
        retry_flag = SPLIT_Internal_HEADER;
        from_cache = false;
        goto next;
      }
      // invalidate cache node due to outdated cache entry in cache node
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
      // udpate cas header. Optimization: no need to snyc; mask node_type
      auto header_buffer = (dsm->get_rbuf(coro_id)).get_header_buffer();
      auto new_hdr = Header::split_header(hdr, i);
      dsm->cas(GADD(p.addr(), sizeof(GlobalAddress)), (uint64_t)hdr, (uint64_t)new_hdr, header_buffer, false, cxt);
      internal_header_split[dsm->getMyThreadID()] ++;
      goto insert_finish;
    }
  }
      assert(hdr.depth !=0);
  depth = hdr.depth + hdr.partial_len;

  node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));


  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(p.type());
//  max_num = 256;
  // search a exists slot first
  for (int i = 0; i < max_num; ++ i) {   //可能是节点的类型没有cas成功？
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, depth)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      from_cache = false;
      depth ++;
      retry_flag = FIND_NEXT;
      parent_type = 0;
      goto next;  // search next level
    }
  }

  // if no match slot, then find an empty slot to insert leaf directly
  for (int i = 0; i < max_num; ++ i) {   //怎么会在中间有空的？？？？
    auto old_e = p_node->records[i];
    if (old_e == InternalEntry::Null()) {   
      auto e_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      bool res = out_of_place_write_buffer_n_leaf(k,v,depth +1,leaf_addr,leaf_type,klen,vlen,e_ptr,old_e,node_ptr,cas_buffer,cxt,coro_id);
      // cas success, return
      if (res) {
        internal_empty_entry[dsm->getMyThreadID()] ++;
        goto insert_finish;
      }
      else{
      auto e = *(InternalEntry*) cas_buffer;
      if (e.partial == get_partial(k, depth))
      {
      p = e;
      p_ptr = e_ptr;
      parent_type = 0;
      from_cache = false;
      retry_flag = CAS_Internal_EMPTY;
      depth++;  
      goto next;
      }
      }
    }
  }
  // 3.4 node is full, switch node type

  int slot_id;
  cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();  //可能存了一样的partial
  if (insert_behind(k, v,p.addr(), depth, leaf_addr,get_partial(k,depth), p.type(),leaf_type,klen, vlen,node_ptr,cas_buffer,slot_id,cxt,coro_id)){  // insert success
      auto page_buffer2 = (dsm->get_rbuf(coro_id)).get_page_buffer();
      read_node(p, type_correct, page_buffer2, p_ptr, depth,from_cache,cxt, coro_id);
    
    
    auto next_type = num_to_node_type(slot_id);
    cas_node_type(next_type, p_ptr, p, hdr, cxt, coro_id);
    if (from_cache) {  // cache is outdated since node type is changed
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    internal_extend_empty_entry[dsm->getMyThreadID()];
    goto insert_finish;
  }
  else {  // same partial keys insert to the same empty slot
    p_ptr = GADD(node_ptr, slot_id * sizeof(InternalEntry));
    p = *(InternalEntry*) cas_buffer;
    from_cache = false;
    depth ++;
    retry_flag = INSERT_BEHIND_EMPTY;
    goto next;
  }
}
else{  //一个缓冲节点 1.找到一样的叶节点了 2.插空槽 3.缓冲节点头部分裂 4.缓冲节点满了 结构化修改 

  if (bp == BufferEntry::Null()) {      //直接写 写了cas  

      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

      //新建一个缓冲节点 和叶节点 一起写过去 最后cas

      auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();
      new (leaf_buffer) Leaf_kv(p_ptr,leaf_type,klen,vlen,k, v);
      leaf_addr = dsm->alloc(sizeof(Leaf_kv));

      auto new_be = BufferEntry(0,get_partial(k,depth-1), 1,leaf_type,leaf_addr);

      dsm->write_sync(leaf_buffer, leaf_addr, sizeof(Leaf_kv), cxt);
      bool res = dsm->cas_sync(p_ptr, (uint64_t)bp, (uint64_t)new_be, cas_buffer, cxt);

      // cas fail, retry
      if (!res) {
        update_retry_flag[dsm->getMyThreadID()]=1;
        bp = *(BufferEntry*) cas_buffer;
        retry_flag = CAS_Buffer_EMPTY;
        from_cache = false;
        goto next;
      }
      buffer_empty_entry[dsm->getMyThreadID()] ++;
      goto insert_finish;
    }


  if(bp.node_type == 1)   //找buffer node 看有没有空的
  {

    bool is_match;
    auto buffer_buffer =  (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    GlobalAddress addr = bp.addr();
  //  if(buffer_from_cache_flag)
    {
    //  bp_node =new InternalBuffer(entry_ptr->depth,entry_ptr->records);
      //is_valid？
    }
   // else
   {  retry_read_buffer ++;
      is_valid = read_buffer_node(addr, buffer_buffer, p_ptr, depth, from_cache,cxt, coro_id);   
      bp_node = (InternalBuffer *)buffer_buffer;
      parent_buffer =*bp_node;
          //3.1 check partial key
      if (!is_valid) {  // node deleted || outdated cache entry in cached node
        if (from_cache) {
          index_cache->invalidate(entry_ptr_ptr, entry_ptr);
        }
        // re-read node entry
        auto entry_buffer = (dsm->get_rbuf(coro_id)).get_buffer_entry_buffer();
        dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
        bp = *(BufferEntry *)entry_buffer;
        from_cache = false;
        retry_flag = INVALID_Buffer_NODE;
        goto next;
      }
    } 

    bhdr=bp_node->hdr;
    if (depth == bhdr.depth) {
    //      printf("thread  %d 5 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)bp_node->hdr);
    index_cache->add_to_cache(k, 1,(InternalPage *)bp_node, GADD(bp.addr(), sizeof(GlobalAddress) + sizeof(BufferHeader)));
    }

    for (int i = 0; i < bhdr.partial_len; ++ i) {    //缓冲节点分裂   新建一个共同前缀的内部节点
    if (get_partial(k, bhdr.depth + i) != bhdr.partial[i]) {
      //3.2 partial key not match, need split
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      int partial_len = bhdr.depth + i - depth;  // hdr.depth may be outdated, so use partial_len wrt. depth
      bool res = out_of_place_write_node_from_buffer(k, v, depth, leaf_addr, leaf_type,  klen,vlen,partial_len,bhdr.partial[i], p_ptr, bp, node_ptr, cas_buffer, cxt, coro_id);   //缓冲节点下面的缓冲节点进行分裂
      if (!res) {
        bp = *(BufferEntry*) cas_buffer;
        retry_flag = SPLIT_Buffer_HEADER;
        from_cache = false;
        goto next;
      }
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
      // udpate cas header. Optimization: no need to snyc; mask node_type
      auto header_buffer = (dsm->get_rbuf(coro_id)).get_header_buffer();
      auto new_hdr = BufferHeader::split_header(bhdr, i);

      bool res_d=dsm->cas_sync(GADD(bp.addr(), sizeof(GlobalAddress)), (uint64_t)bhdr, (uint64_t)new_hdr, header_buffer,cxt);
      buffer_header_split[dsm->getMyThreadID()] ++;
      goto insert_finish;
    }
    }
    depth = bhdr.depth + bhdr.partial_len;
    auto partial = get_partial(k, depth);
    GlobalAddress leaf_addrs[256];
    GlobalAddress leaves_ptr[256];
    memset(leaf_addrs,0,256*sizeof(GlobalAddress));
    memset(leaves_ptr,0,256*sizeof(GlobalAddress));
    int leaf_cnt = 0;
    //3.3 search an exists slot first 
    for(int i=0;i < 256;i++)   // 找当前键是否已经存在  并且是叶子还是buffer   是buffer继续往下层找  是叶子看看重复不 不重复就正常插入本层
    {
      if(bp_node->records[i] != BufferEntry::Null() && bp_node->records[i].partial == partial )
      {      //  assert(bp_node->records[i].addr().nodeID == 0);
        if(bp_node->records[i].node_type == 1 || bp_node->records[i].node_type == 2) 
        {
          bp = bp_node->records[i];
          p_ptr = GADD(bp.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          depth ++;
          parent_type = 1;
          from_cache = false;
          goto next;
        }
        else  //是叶节点则读过来看
        {
          leaf_addrs[leaf_cnt] = bp_node->records[i].addr();
          leaves_ptr[leaf_cnt]  = GADD(bp.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          leaf_cnt ++;
        }
      }
    }
    if(leaf_cnt !=0)   //将所有的叶子读过来 看有没有重复的 
    {
        auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_range_buffer(); 
    
        is_valid = read_leaves(leaf_addrs, leaf_buffer,leaf_cnt,leaves_ptr,from_cache,cxt,coro_id);

        if (!is_valid) {
          if (from_cache) {
            index_cache->invalidate(entry_ptr_ptr, entry_ptr);
          }
          // re-read leaf entry
          auto entry_buffer = (dsm->get_rbuf(coro_id)).get_buffer_entry_buffer();
          dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(BufferEntry), cxt);
          bp = *(BufferEntry *)entry_buffer;
          from_cache = false;
          buffer_from_cache_flag = false;
          retry_flag = INVALID_LEAF;
          goto next;
        }
        for(int i =0;i<leaf_cnt;i++)
        {
          auto leaf = (Leaf_kv*) leaf_buffer + i* define::allocAlignKVLeafSize;
          auto _k = leaf->get_key();

          // 2.3 Check if it is the key we search
          if (_k == k) {
            if (is_load) {
               goto insert_finish;
                }
              in_place_update_leaf(k,v,leaf_addrs[i],leaf_type,leaf,cxt,coro_id);   
              in_place_update[dsm->getMyThreadID()] ++;
              goto insert_finish;
          }
        }
    }
    //3.4 still have empty slot  不存在部分键相同的情况  有的话 则往下找 否则放空位 
  //  if(bhdr.count_1+bhdr.count_2 < 256)
    //{
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

      GlobalAddress be_ptr;
      BufferEntry old_be;
    //  uint8_t partial;
        for(int i=0;i < 256;i++)
        {
          if(bp_node->records[i] == BufferEntry::Null()||bp_node->records[i].val ==0) //If we are at a  buffer  empty and partial key match
          {
           depth ++;
           old_be = bp_node->records[i];
           be_ptr=GADD(bp.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(BufferEntry));
           auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
           bool res = out_of_place_write_leaf(k,v,depth,leaf_addr,leaf_type ,klen,vlen,be_ptr,old_be,cas_buffer,cxt,coro_id);
           if(res)
           {
            buffer_empty_entry[dsm->getMyThreadID()]++;
            goto insert_finish;
           } 
           else {
            auto e = *(BufferEntry*) cas_buffer;
            if (e.partial == get_partial(k, depth)) {  // same partial keys insert to the same empty slot  再次查找本层 
              bp = e;
              from_cache = false;
              retry_flag = CAS_Buffer_EMPTY;
              goto next;  // search next level
              }
          }
        }
      }
  //有重复的 需要将重复的拿下来到下一级缓冲节点
          bool res=out_of_place_write_buffer_node_from_buffer(k, v,depth,bp_node,leaf_type,klen,vlen,leaf_addr,entry_ptr_ptr, entry_ptr,from_cache,bp, p_ptr,cxt,coro_id);

          if (!res) {
          //  bp = *(BufferEntry*) cas_buffer;
            retry_flag = Buffer_Switch_type;
            from_cache = false;
            goto next;
          }
          buffer_reconstruct[dsm->getMyThreadID()] ++;
          goto insert_finish;

     //     }
   // }
  }

  //内部节点
  // 3. Find out a node
  // 3.1 read the node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node_from_buffer(bp, type_correct,page_buffer,p_ptr,depth, from_cache,cxt,coro_id);
  p_node = (InternalPage *)page_buffer;
  parent_buffer =*bp_node;
  if (!is_valid) {  // node deleted || outdated cache entry in cached node

    // invalidate the old node cache
    if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_buffer_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(BufferEntry), cxt);
    bp = *(BufferEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_Internal_NODE;
    goto next;
  }
  // 3.2 Check header
  hdr = p_node->hdr;

  if (from_cache && !type_correct) {  // invalidate the out dated node type
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
  if (depth == hdr.depth) {
      //    printf("thread  %d 6 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)p_node->hdr);
    index_cache->add_to_cache(k, 0,p_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }


  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) {
      // need split
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      int partial_len = hdr.depth + i - depth;  // hdr.depth may be outdated, so use partial_len wrt. depth
      bool res = out_of_place_write_node_from_buffer(k, v, depth, leaf_addr,leaf_type,klen,vlen, partial_len,hdr.partial[i], p_ptr, bp, node_ptr, cas_buffer, cxt, coro_id);   //内部节点分裂  分裂后往新的内部节点下申请一个新的缓冲节点和叶节点
      // cas fail, retry
      if (!res) {
        update_retry_flag[dsm->getMyThreadID()]=1;
        bp = *(BufferEntry*) cas_buffer;
        retry_flag = SPLIT_Internal_HEADER;
        from_cache = false;
        goto next;
      }
      // invalidate cache node due to outdated cache entry in cache node
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }

      // udpate cas header. Optimization: no need to snyc; mask node_type
      auto header_buffer = (dsm->get_rbuf(coro_id)).get_header_buffer();
      auto new_hdr = Header::split_header(hdr, i);
      dsm->cas(GADD(bp.addr(), sizeof(GlobalAddress)), (uint64_t)hdr, (uint64_t)new_hdr, header_buffer, false, cxt);
      internal_header_split[dsm->getMyThreadID()] ++;
      goto insert_finish;
    }
  }
      assert(hdr.depth !=0);
  depth = hdr.depth + hdr.partial_len;
#ifdef TREE_TEST_ROWEX_ART
  if (!is_update) unlock_node(node_ptr, cxt, coro_id);
  node_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));
  if (!is_update) lock_node(node_ptr, cxt, coro_id);
#else
  node_ptr = GADD(bp.addr(), sizeof(GlobalAddress) + sizeof(Header));
#endif

  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(bp.type());
  // search a exists slot first
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, depth)) {
      p_ptr = GADD(bp.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      from_cache = false;
      depth ++;
      retry_flag = FIND_NEXT;
      parent_type = 0;
      goto next;  // search next level
    }
  }
  // if no match slot, then find an empty slot to insert leaf directly
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e == InternalEntry::Null()) {
      auto e_ptr = GADD(bp.addr(), sizeof(GlobalAddress) + sizeof(BufferHeader) + i * sizeof(BufferEntry));
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      bool res = out_of_place_write_buffer_n_leaf(k,v,depth +1,leaf_addr,leaf_type,klen,vlen,e_ptr,old_e,node_ptr,cas_buffer,cxt,coro_id);
      // cas success, return
      if (res) {
        internal_empty_entry[dsm->getMyThreadID()] ++;
        goto insert_finish;
      }
      else{
      auto e = *(InternalEntry*) cas_buffer;
      if (e.partial == get_partial(k, depth))
      {
      p = old_e;
      p_ptr = e_ptr;
      parent_type = 0;
      from_cache = false;
      retry_flag = CAS_Internal_EMPTY;
      depth++;  
      goto next;
      }
      }
    }
  }
    // 3.4 node is full, switch node type

  int slot_id;
  cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  if (insert_behind(k, v, bp.addr(), depth,leaf_addr,get_partial(k,depth), bp.type(),leaf_type,klen, vlen,node_ptr,cas_buffer,slot_id,cxt,coro_id)){  // insert success
    auto next_type = num_to_node_type(slot_id);
    cas_node_type_from_buffer(next_type, p_ptr, bp, hdr, cxt, coro_id);
    if (from_cache) {  // cache is outdated since node type is changed
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    internal_extend_empty_entry[dsm->getMyThreadID()] ++;
    goto insert_finish;
  }
  else {  // same partial keys insert to the same empty slot
    p_ptr = GADD(node_ptr, slot_id * sizeof(InternalEntry));
    p = *(InternalEntry*) cas_buffer;
    from_cache = false;
    depth ++;
    retry_flag = INSERT_BEHIND_EMPTY;
    goto next;
  }

}

insert_finish:


#ifdef TREE_TEST_ROWEX_ART
  if (!is_update) unlock_node(node_ptr, cxt, coro_id);
#endif



    auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
    cache_hit[dsm->getMyThreadID()] += hit;
    cache_miss[dsm->getMyThreadID()] += (1 - hit);

  return;
}



bool Tree::read_leaf(GlobalAddress &leaf_addr, char *leaf_buffer, int leaf_size, const GlobalAddress &p_ptr, bool from_cache, CoroContext *cxt, int coro_id) {
  try_read_leaf[dsm->getMyThreadID()] ++;
re_read:
  dsm->read_sync(leaf_buffer, leaf_addr, leaf_size, cxt);
  auto leaf = (Leaf_kv *)leaf_buffer;
  // udpate reverse pointer if needed
  if (!from_cache && leaf->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(leaf_addr, leaf->rev_ptr, p_ptr, cas_buffer, false, cxt);
    // dsm->cas_sync(leaf_addr, leaf->rev_ptr, p_ptr, cas_buffer, cxt);
  }
  // invalidation
  if (!leaf->is_valid(p_ptr, from_cache)) {
    leaf_cache_invalid[dsm->getMyThreadID()] ++;
    return false;
  }
  if (!leaf->is_consistent()) {
    read_leaf_retry[dsm->getMyThreadID()] ++;
    goto re_read;
  }
  return true;
}



bool Tree::read_leaves(GlobalAddress* leaf_addrs, char *leaf_buffer,int leaf_cnt, GlobalAddress* p_ptr, bool from_cache,CoroContext *cxt, int coro_id) {  //read_batch  !!!问题在哪里！
  try_read_leaf[dsm->getMyThreadID()] ++;
  std::vector<RdmaOpRegion> rs;
  int retry_time = 0;
re_read:
  std::memset(leaf_buffer, 0, leaf_cnt*define::allocAlignPageSize);
  rs.clear();
    Leaf_kv * leaf;
    // 2.3.1 read the leaf
//    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaves_buffer(leaf_cnt); 
    for(int i =0;i<leaf_cnt;i++)
    {
      RdmaOpRegion r;
      memset(&r,0,sizeof(RdmaOpRegion));
      r.source     = (uint64_t)leaf_buffer + i * define::allocAlignPageSize;
      r.dest       = leaf_addrs[i];
      r.size       = sizeof(Leaf_kv);
      r.is_on_chip = false;
      rs.push_back(r);
    }
    dsm->read_batches_sync(rs);

    for(int i =0;i<leaf_cnt;i++)
    {
      leaf = (Leaf_kv *)(leaf_buffer + i*define::allocAlignPageSize);
      uint64_t kk_v =  key2int(leaf->key);
    //  printf("leaf key is %d %d\n",(int)key2int(leaf->key),cnt);
 //     printf("leaf value is %d\n",(int)key2int(leaf->value));
      if (!from_cache && leaf->rev_ptr != p_ptr[i]) {
      auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
      dsm->cas(leaf_addrs[i], leaf->rev_ptr, p_ptr[i], cas_buffer, false, cxt);
      // dsm->cas_sync(leaf_addr, leaf->rev_ptr, p_ptr, cas_buffer, cxt);
      }
      // invalidation
      if (!leaf->is_valid(p_ptr[i], from_cache)) {
      leaf_cache_invalid[dsm->getMyThreadID()] ++;
      return false;
      }
      if (!leaf->is_consistent()) {   //判断校验和的时候 ？？？  
      retry_time ++;
      read_leaf_retry[dsm->getMyThreadID()] ++;
      goto re_read;
      }
    }
  return true;
}

bool Tree::out_of_place_write_buffer_n_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr,int leaf_type,int klen,int vlen,const GlobalAddress &p_ptr, const InternalEntry &p, const GlobalAddress& node_addr, uint64_t *ret_buffer,CoroContext *cxt, int coro_id)
{
    GlobalAddress b_addr;
    b_addr = dsm->alloc(sizeof(InternalBuffer));   
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();
    Leaf_kv *leaf = new (leaf_buffer) Leaf_kv(GADD(b_addr,sizeof(GlobalAddress)+sizeof(BufferHeader)),leaf_type,klen,vlen,k, v);
    leaf_addr = dsm->alloc(sizeof(Leaf_kv));
    auto b_buffer=(dsm->get_rbuf(coro_id)).get_buffer_buffer();
   // if(p.addr().val == 0)printf("0002!\n");
    InternalBuffer* buffer = new (b_buffer) InternalBuffer(k,2,depth,1,1,p_ptr);  // 暂时定初始2B作为partial key buffer地址
   // printf("thread  %d 1 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)buffer->hdr);
    buffer->records[0] = BufferEntry(0,get_partial(k,depth+buffer->hdr.partial_len),1,leaf_type,leaf_addr);
    auto new_e = InternalEntry(get_partial(k,depth-1), 1, b_addr);
    RdmaOpRegion *rs =  new RdmaOpRegion[2];
    {
      rs[0].source     = (uint64_t)b_buffer;
      rs[0].dest       = b_addr;
      rs[0].size       = sizeof(InternalBuffer);
      rs[0].is_on_chip = false;
    }
    {
      rs[1].source     = (uint64_t)leaf_buffer;
      rs[1].dest       = leaf_addr;
      rs[1].size       = sizeof(Leaf_kv);
      rs[1].is_on_chip = false;
    }
    dsm->write_batches_sync(rs, 2, cxt, coro_id);
    bool res = dsm->cas_sync(p_ptr, (uint64_t)p, (uint64_t)new_e, ret_buffer, cxt);
    if(res)
    {
    //  printf("thread  %d 2 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)buffer->hdr);
      index_cache->add_to_cache(k, 1,(InternalPage*)buffer, GADD(b_addr, sizeof(GlobalAddress) + sizeof(BufferHeader)));
    }

    delete[] rs;
    return res;
}

/*
void Tree::in_place_update_leaf(const Key &k, Value &v, const GlobalAddress &leaf_addr, Leaf* leaf,
                               CoroContext *cxt, int coro_id) {
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  static const uint64_t lock_cas_offset = ROUND_DOWN(STRUCT_OFFSET(Leaf, lock_byte), 3);
  static const uint64_t lock_mask       = 1UL << ((STRUCT_OFFSET(Leaf, lock_byte) - lock_cas_offset) * 8);
#endif

  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &unique_leaf_addr) {
#ifdef TREE_ENABLE_EMBEDDING_LOCK
    bool res=dsm->cas_mask_sync(GADD(unique_leaf_addr, lock_cas_offset), 0UL, ~0UL, cas_buffer, lock_mask, cxt);
    MN_iops[dsm->getMyThreadID()][GADD(unique_leaf_addr, lock_cas_offset).nodeID]++;
    MN_datas[dsm->getMyThreadID()][GADD(unique_leaf_addr, lock_cas_offset).nodeID]+=8;
    return res;
#else
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_leaf_addr, lock_addr, mask);
    bool res=dsm->cas_dm_mask_sync(lock_addr, 0UL, ~0UL, cas_buffer, mask, cxt);
    MN_iops[dsm->getMyThreadID()][lock_addr.nodeID]++;
    MN_datas[dsm->getMyThreadID()][lock_addr.nodeID]+=8;
    return res;
#endif
  };

  // unlock function
  auto unlock = [=](const GlobalAddress &unique_leaf_addr){
#ifdef TREE_ENABLE_EMBEDDING_LOCK
    dsm->cas_mask_sync(GADD(unique_leaf_addr, lock_cas_offset), ~0UL, 0UL, cas_buffer, lock_mask, cxt);
    MN_iops[dsm->getMyThreadID()][GADD(unique_leaf_addr, lock_cas_offset).nodeID]++;
    MN_datas[dsm->getMyThreadID()][GADD(unique_leaf_addr, lock_cas_offset).nodeID]+=8;
#else
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_leaf_addr, lock_addr, mask);
    dsm->cas_dm_mask_sync(lock_addr, ~0UL, 0UL, cas_buffer, mask, cxt);
    MN_iops[dsm->getMyThreadID()][lock_addr.nodeID]++;
    MN_datas[dsm->getMyThreadID()][lock_addr.nodeID]+=8;
#endif
  };

  // start lock & write & unlock
  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write w/o unlock
  auto write_without_unlock = [=](const GlobalAddress &unique_leaf_addr){
    dsm->write_sync((const char*)leaf, unique_leaf_addr, sizeof(Leaf), cxt);
    MN_iops[dsm->getMyThreadID()][unique_leaf_addr.nodeID]++;
    MN_datas[dsm->getMyThreadID()][unique_leaf_addr.nodeID]+=sizeof(Leaf);
  };
  // write and unlock
  auto write_and_unlock = [=](const GlobalAddress &unique_leaf_addr){
    leaf->unlock();
    dsm->write_sync((const char*)leaf, unique_leaf_addr, sizeof(Leaf), cxt);
    MN_iops[dsm->getMyThreadID()][unique_leaf_addr.nodeID]++;
    MN_datas[dsm->getMyThreadID()][unique_leaf_addr.nodeID]+=sizeof(Leaf);
  };
#endif

  lock_handover = local_lock_table->acquire_local_lock(leaf_addr, &busy_waiting_queue, cxt, coro_id);
#endif
  if (lock_handover) {
    goto write_leaf;
  }
  // try_lock[dsm->getMyThreadID()] ++;

re_acquire:
  if (!acquire_lock(leaf_addr)){
    if (cxt != nullptr) {
      busy_waiting_queue.push(std::make_pair(coro_id, [](){ return true; }));
      (*cxt->yield)(*cxt->master);
    }
    lock_fail[dsm->getMyThreadID()] ++;
    update_retry_flag[dsm->getMyThreadID()]=1;
    goto re_acquire;
  }

write_leaf:
#ifdef TREE_TEST_HOCL_HANDOVER
  // in-place write leaf & unlock
  assert(leaf->get_key() == k);
  leaf->set_value(v);
  leaf->set_consistent();
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write back the lock at the same time
  local_lock_table->release_local_lock(leaf_addr, unlock, write_without_unlock, write_and_unlock);
#else
  dsm->write_sync((const char*)leaf, leaf_addr, sizeof(Leaf), cxt);
  MN_iops[dsm->getMyThreadID()][leaf_addr.nodeID]++;
  MN_datas[dsm->getMyThreadID()][leaf_addr.nodeID]+=sizeof(Leaf);
  local_lock_table->release_local_lock(leaf_addr, unlock);
#endif

#else
  UNUSED(unlock);
  // in-place write leaf & unlock
  assert(leaf->get_key() == k);
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->get_combining_value(k, v);
#endif
  leaf->set_value(v);
  leaf->set_consistent();
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write back the lock at the same time
  leaf->unlock();
  dsm->write_sync((const char*)leaf, leaf_addr, sizeof(Leaf), cxt);
  MN_iops[dsm->getMyThreadID()][leaf_addr.nodeID]++;
  MN_datas[dsm->getMyThreadID()][leaf_addr.nodeID]+=sizeof(Leaf);
#else
  // batch write updated leaf and on-chip lock
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)leaf;
  rs[0].dest = leaf_addr;
  rs[0].size = sizeof(Leaf);
  rs[0].is_on_chip = false;
  GlobalAddress lock_addr;
  uint64_t mask;
  get_on_chip_lock_addr(leaf_addr, lock_addr, mask);
  rs[1].source = (uint64_t)cas_buffer;  // unlock
  rs[1].dest = lock_addr;
  rs[1].is_on_chip = true;
  dsm->write_cas_mask_sync(rs[0], rs[1], ~0UL, 0UL, mask, cxt);
  MN_iops[dsm->getMyThreadID()][leaf_addr.nodeID]++;
  MN_datas[dsm->getMyThreadID()][leaf_addr.nodeID]+=sizeof(Leaf);
  MN_iops[dsm->getMyThreadID()][lock_addr.nodeID]++;
  MN_datas[dsm->getMyThreadID()][lock_addr.nodeID]+=8;
#endif
#endif
  return;
}
*/
void Tree::in_place_update_leaf(const Key &k, Value &v, const GlobalAddress &leaf_addr, int leaf_type,Leaf_kv* leaf,  
                               CoroContext *cxt, int coro_id) {
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  static const uint64_t lock_cas_offset = ROUND_DOWN(STRUCT_OFFSET(Leaf_kv, lock_byte), 3);
  static const uint64_t lock_mask       = 1UL << ((STRUCT_OFFSET(Leaf_kv, lock_byte) - lock_cas_offset) * 8);
#endif

  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &unique_leaf_addr) {
#ifdef TREE_ENABLE_EMBEDDING_LOCK
    bool res=dsm->cas_mask_sync(GADD(unique_leaf_addr, lock_cas_offset), 0UL, ~0UL, cas_buffer, lock_mask, cxt);

    return res;
#else
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_leaf_addr, lock_addr, mask);
    bool res=dsm->cas_dm_mask_sync(lock_addr, 0UL, ~0UL, cas_buffer, mask, cxt);

    return res;
#endif
  };

  // unlock function
  auto unlock = [=](const GlobalAddress &unique_leaf_addr){
#ifdef TREE_ENABLE_EMBEDDING_LOCK
    dsm->cas_mask_sync(GADD(unique_leaf_addr, lock_cas_offset), ~0UL, 0UL, cas_buffer, lock_mask, cxt);

#else
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_leaf_addr, lock_addr, mask);
    dsm->cas_dm_mask_sync(lock_addr, ~0UL, 0UL, cas_buffer, mask, cxt);

#endif
  };

  // start lock & write & unlock
  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write w/o unlock
  auto write_without_unlock = [=](const GlobalAddress &unique_leaf_addr){
    dsm->write_sync((const char*)leaf, unique_leaf_addr, sizeof(Leaf_kv), cxt);

  };
  // write and unlock
  auto write_and_unlock = [=](const GlobalAddress &unique_leaf_addr){
    leaf->unlock();
    dsm->write_sync((const char*)leaf, unique_leaf_addr, sizeof(Leaf_kv), cxt);

  };
#endif

  lock_handover = local_lock_table->acquire_local_lock(leaf_addr, &busy_waiting_queue, cxt, coro_id);
#endif
  if (lock_handover) {
    goto write_leaf;
  }
  // try_lock[dsm->getMyThreadID()] ++;

re_acquire:
  if (!acquire_lock(leaf_addr)){
    if (cxt != nullptr) {
      busy_waiting_queue.push(std::make_pair(coro_id, [](){ return true; }));
      (*cxt->yield)(*cxt->master);
    }
    lock_fail[dsm->getMyThreadID()] ++;
    update_retry_flag[dsm->getMyThreadID()]=1;
    goto re_acquire;
  }

write_leaf:
#ifdef TREE_TEST_HOCL_HANDOVER
  // in-place write leaf & unlock
  assert(leaf->get_key() == k);
  leaf->set_value(v);
  leaf->set_consistent();
  leaf->leaf_type = leaf_type;
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write back the lock at the same time
  local_lock_table->release_local_lock(leaf_addr, unlock, write_without_unlock, write_and_unlock);
#else
  dsm->write_sync((const char*)leaf, leaf_addr, sizeof(Leaf_kv), cxt);
  local_lock_table->release_local_lock(leaf_addr, unlock);
#endif

#else
  UNUSED(unlock);
  // in-place write leaf & unlock
  assert(leaf->get_key() == k);
#ifdef TREE_ENABLE_WRITE_COMBINING
  local_lock_table->get_combining_value(k, v);
#endif
  leaf->set_value(v);
  leaf->set_consistent();
  leaf->leaf_type = leaf_type;
#ifdef TREE_ENABLE_EMBEDDING_LOCK
  // write back the lock at the same time
  leaf->unlock();
  dsm->write_sync((const char*)leaf, leaf_addr, sizeof(Leaf_kv), cxt);

#else
  // batch write updated leaf and on-chip lock
  RdmaOpRegion rs[2];
  rs[0].source = (uint64_t)leaf;
  rs[0].dest = leaf_addr;
  rs[0].size = sizeof(Leaf_kv);
  rs[0].is_on_chip = false;
  GlobalAddress lock_addr;
  uint64_t mask;
  get_on_chip_lock_addr(leaf_addr, lock_addr, mask);
  rs[1].source = (uint64_t)cas_buffer;  // unlock
  rs[1].dest = lock_addr;
  rs[1].is_on_chip = true;
  dsm->write_cas_mask_sync(rs[0], rs[1], ~0UL, 0UL, mask, cxt);

#endif
#endif
  return;
}
/*
bool Tree::out_of_place_update_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, const GlobalAddress &e_ptr, InternalEntry &old_e, const GlobalAddress& node_addr,
                                    CoroContext *cxt, int coro_id, bool disable_handover) {
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  bool res = false;

  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
  if (!disable_handover) {
    lock_handover = local_lock_table->acquire_local_lock(k, &busy_waiting_queue, cxt, coro_id);
  }
#endif
  if (lock_handover) {
    goto update_finish;
  }
  // try_lock[dsm->getMyThreadID()] ++;
  res = out_of_place_write_leaf(k, v, depth, leaf_addr, old_e.partial, e_ptr, old_e, node_addr, cas_buffer, cxt, coro_id);
  if (res) {
    // invalid the old leaf
    auto zero_byte = (dsm->get_rbuf(coro_id)).get_zero_byte();
    dsm->write(zero_byte, GADD(old_e.addr(), STRUCT_OFFSET(Leaf, valid_byte)), sizeof(uint8_t), false, cxt);
  }
  else {
    old_e = *(InternalEntry*) cas_buffer;
  }
update_finish:
#ifdef TREE_TEST_HOCL_HANDOVER
  if (!disable_handover) {
    printf("before releas 1\n");
    local_lock_table->release_local_lock(k, res, old_e);
    printf("after releas 1\n");
  }
#endif
  return res;
}
*/
/*
bool Tree::out_of_place_update_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, const GlobalAddress &e_ptr, InternalEntry &old_e, const GlobalAddress& node_addr,
                                    CoroContext *cxt, int coro_id, bool disable_handover) {
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  bool res = false;

  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
  if (!disable_handover) {
    lock_handover = local_lock_table->acquire_local_lock(k, &busy_waiting_queue, cxt, coro_id);
  }
#endif
  if (lock_handover) {
    goto update_finish;
  }
  // try_lock[dsm->getMyThreadID()] ++;
  res = out_of_place_write_leaf(k, v, depth, leaf_addr, old_e.partial, e_ptr, old_e, node_addr, cas_buffer, cxt, coro_id);
  if (res) {
    // invalid the old leaf
    auto zero_byte = (dsm->get_rbuf(coro_id)).get_zero_byte();
    dsm->write(zero_byte, GADD(old_e.addr(), STRUCT_OFFSET(Leaf, valid_byte)), sizeof(uint8_t), false, cxt);
  }
  else {
    old_e = *(InternalEntry*) cas_buffer;
  }
update_finish:
#ifdef TREE_TEST_HOCL_HANDOVER
  if (!disable_handover) {
    printf("before releas 1\n");
    local_lock_table->release_local_lock(k, res, old_e);
    printf("after releas 1\n");
  }
#endif
  return res;
}

void Tree::get_on_chip_lock_addr(const GlobalAddress &leaf_addr, GlobalAddress &lock_addr, uint64_t &mask) {
  auto leaf_offset = leaf_addr.offset;
  auto lock_index = CityHash64((char *)&leaf_offset, sizeof(leaf_offset)) % define::kOnChipLockNum;
  lock_addr.nodeID = leaf_addr.nodeID;
  lock_addr.offset = lock_index / 64 * sizeof(uint64_t);
  mask = 1UL << (lock_index % 64);
}

#ifdef TREE_TEST_ROWEX_ART
void Tree::lock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id) {
  // HOCL
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

  // lock function
  auto acquire_lock = [=](const GlobalAddress &unique_node_addr) {
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_node_addr, lock_addr, mask);
    return dsm->cas_dm_mask_sync(lock_addr, 0UL, ~0UL, cas_buffer, mask, cxt);
  };

  bool lock_handover = false;
#ifdef TREE_TEST_HOCL_HANDOVER
  lock_handover = local_lock_table->acquire_local_lock(node_addr, &busy_waiting_queue, cxt, coro_id);
#endif
  if (lock_handover) {
    return;
  }
  // try_lock[dsm->getMyThreadID()] ++;
re_acquire:
  if (!acquire_lock(node_addr)){
    if (cxt != nullptr) {
      busy_waiting_queue.push(std::make_pair(coro_id, [](){ return true; }));
      (*cxt->yield)(*cxt->master);
    }
    lock_fail[dsm->getMyThreadID()] ++;
    goto re_acquire;
  }
  return;
}

void Tree::unlock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id) {
  // HOCL
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();

  // unlock function
  auto unlock = [=](const GlobalAddress &unique_node_addr){
    GlobalAddress lock_addr;
    uint64_t mask;
    get_on_chip_lock_addr(unique_node_addr, lock_addr, mask);
    dsm->cas_dm_mask_sync(lock_addr, ~0UL, 0UL, cas_buffer, mask, cxt);
  };

#ifdef TREE_TEST_HOCL_HANDOVER
    printf("before releas 2\n");
  local_lock_table->release_local_lock(node_addr, unlock);
      printf("after releas 2\n");
#else
  unlock(node_addr);
#endif
  return;
}
#endif
*/

/*
bool Tree::out_of_place_write_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, uint8_t partial_key,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr, uint64_t *ret_buffer,
                                   CoroContext *cxt, int coro_id) {
  bool unwrite = leaf_addr == GlobalAddress::Null();

  // allocate & write
  if (unwrite) {  // !ONLY allocate once
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
    new (leaf_buffer) Leaf(k, v, e_ptr);
    leaf_addr = dsm->alloc(sizeof(Leaf));
    dsm->write_sync(leaf_buffer, leaf_addr, sizeof(Leaf)- (define::simulatedValLen - v.at(0) -1), cxt);

  }
  else {  // write the changed e_ptr inside leaf
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
  }

  // cas entry
  auto new_e = InternalEntry(partial_key, sizeof(Leaf)- (define::simulatedValLen - v.at(0) -1) < 128 ? sizeof(Leaf) - (define::simulatedValLen - v.at(0) -1) : 0, leaf_addr);

  auto remote_cas = [=](){
    auto insert_empty_slot_cas_start = std::chrono::high_resolution_clock::now();
    bool res=dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
    return res;

  };

  bool res=remote_cas();
  if(res) insert_empty_slot_other[dsm->getMyThreadID()] += insert_empty_slot_other_duration.count();
  return res;

}
*/
//向缓冲节点空槽插入
bool Tree::out_of_place_write_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, int leaf_type ,int klen,int vlen,
                                   const GlobalAddress &e_ptr, const BufferEntry &old_e, uint64_t *ret_buffer,
                                   CoroContext *cxt, int coro_id) {
  bool unwrite = leaf_addr == GlobalAddress::Null();

  // allocate & write
  if (unwrite) {  // !ONLY allocate once
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();
    new (leaf_buffer) Leaf_kv(e_ptr,leaf_type,klen,vlen,k, v);
    leaf_addr = dsm->alloc(sizeof(Leaf_kv));
    dsm->write_sync(leaf_buffer, leaf_addr,sizeof(Leaf_kv), cxt);

  }
  else {  // write the changed e_ptr inside leaf
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
  }

  // cas entry
  auto new_e = BufferEntry(0,get_partial(k,depth-1),1,leaf_type,leaf_addr);   

  auto remote_cas = [=](){
    bool res=dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt); //传参问题啊啊啊啊啊！
    return res;
  };

  bool res=remote_cas();

  return res;

}


bool Tree::read_node(InternalEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                     CoroContext *cxt, int coro_id) {
  auto read_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(p.type()) * sizeof(InternalEntry) ;
  //auto read_size = sizeof(GlobalAddress) + sizeof(Header) + 256 * sizeof(InternalEntry) + 1;
  dsm->read_sync(node_buffer, p.addr(), read_size, cxt);

  auto p_node = (InternalPage *)node_buffer;
  auto& hdr = p_node->hdr;


  if (hdr.node_type != p.node_type) {
    if (hdr.node_type > p.node_type) {  // need to read the rest part
      read_node_repair[dsm->getMyThreadID()] ++;
      auto remain_size = (node_type_to_num(hdr.type()) - node_type_to_num(p.type())) * sizeof(InternalEntry);
      dsm->read_sync(node_buffer + read_size, GADD(p.addr(), read_size), remain_size, cxt);
    }
    p.node_type = hdr.node_type;
    type_correct = false;
  }
  else type_correct = true;
  // udpate reverse pointer if needed
  if ( p_node->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, false, cxt);

    // dsm->cas_sync(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, cxt);
  }
  return p_node->is_valid(p_ptr, depth,from_cache);
}

bool Tree::read_node_from_buffer(BufferEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                     CoroContext *cxt, int coro_id) {
  auto read_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(p.type()) * sizeof(InternalEntry);
  dsm->read_sync(node_buffer, p.addr(), read_size, cxt);

  auto p_node = (InternalPage *)node_buffer;
  auto& hdr = p_node->hdr;


  if (hdr.node_type != p.leaf_type) {
    if (hdr.node_type > p.leaf_type) {  // need to read the rest part
      read_node_repair[dsm->getMyThreadID()] ++;
      auto remain_size = (node_type_to_num(hdr.type()) - node_type_to_num(p.type())) * sizeof(InternalEntry);
      dsm->read_sync(node_buffer + read_size, GADD(p.addr(), read_size), remain_size, cxt);
    }
    p.leaf_type = hdr.node_type;
    type_correct = false;
  }
  else type_correct = true ;

  // udpate reverse pointer if needed
  if ( p_node->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, false, cxt);

    // dsm->cas_sync(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, cxt);
  }
  return p_node->is_valid(p_ptr, depth,from_cache);
}
//读出一个buffer node并且验证其正确性  
bool Tree::read_buffer_node(GlobalAddress node_addr, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,   //只需要判断反向指针对不对就可以了 （有没有分裂）
                     CoroContext *cxt, int coro_id) {
  size_t read_size = 0;
  read_size += sizeof(GlobalAddress) + sizeof(BufferHeader) + 256*sizeof(BufferEntry) +1;
  dsm->read_sync(node_buffer, node_addr, read_size, cxt);

  auto p_node = (InternalBuffer *)node_buffer;
      

  // udpate reverse pointer if needed
  if (!from_cache &&p_node->rev_ptr != p_ptr) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(node_addr, p_node->rev_ptr, p_ptr, cas_buffer, false, cxt);

    // dsm->cas_sync(p.addr(), p_node->rev_ptr, p_ptr, cas_buffer, cxt);
  }
  return p_node->is_valid(p_ptr, depth,from_cache);
}


/*
bool Tree::out_of_place_write_node(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, int partial_len, uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {

  auto insert_leaf_merge_write_start = std::chrono::high_resolution_clock::now();                                  
  int new_node_num = partial_len / (define::hPartialLenMax + 1) + 1;
  auto leaf_unwrite = (leaf_addr == GlobalAddress::Null());

  // allocate node
  GlobalAddress *node_addrs = new GlobalAddress[new_node_num];
  dsm->alloc_nodes(new_node_num, node_addrs);

  // allocate & write new leaf
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
  auto leaf_e_ptr = GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header) + sizeof(InternalEntry) * 1);
#ifdef TREE_ENABLE_WRITE_COMBINING
  if (local_lock_table->get_combining_value(k, v)) leaf_unwrite = true;
#endif
  if (leaf_unwrite) {  // !ONLY allocate once
    new (leaf_buffer) Leaf(k, v, leaf_e_ptr);
    leaf_addr = dsm->alloc(sizeof(Leaf));
  }
  else {  // write the changed e_ptr inside new leaf  TODO: batch
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = leaf_e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
    MN_iops[dsm->getMyThreadID()][leaf_addr.nodeID]++;
    MN_datas[dsm->getMyThreadID()][leaf_addr.nodeID]+=sizeof(GlobalAddress);
  }

  // init inner nodes
  NodeType nodes_type = num_to_node_type(2);
  InternalPage ** node_pages = new InternalPage* [new_node_num];
  auto rev_ptr = e_ptr;
  for (int i = 0; i < new_node_num - 1; ++ i) {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    node_pages[i] = new (node_buffer) InternalPage(k, define::hPartialLenMax, depth, nodes_type, rev_ptr);
    node_pages[i]->records[0] = InternalEntry(get_partial(k, depth + define::hPartialLenMax),
                                              nodes_type, node_addrs[i + 1]);
    rev_ptr = GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header));
    partial_len -= define::hPartialLenMax + 1;
    depth += define::hPartialLenMax + 1;
  }

  // insert the two leaf into the last node
  auto node_buffer  = (dsm->get_rbuf(coro_id)).get_page_buffer();
  node_pages[new_node_num - 1] = new (node_buffer) InternalPage(k, partial_len, depth, nodes_type, rev_ptr);
  node_pages[new_node_num - 1]->records[0] = InternalEntry(diff_partial, old_e);
  node_pages[new_node_num - 1]->records[1] = InternalEntry(get_partial(k, depth + partial_len),
                                                           sizeof(Leaf) < 128 ? sizeof(Leaf) : 0, leaf_addr);

  // init the parent entry
  auto new_e = InternalEntry(old_e.partial, nodes_type, node_addrs[0]);
  auto page_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(nodes_type) * sizeof(InternalEntry);

  // batch_write nodes (doorbell batching)
  int i;
  RdmaOpRegion *rs =  new RdmaOpRegion[new_node_num + 1];
  for (i = 0; i < new_node_num; ++ i) {
    rs[i].source     = (uint64_t)node_pages[i];
    rs[i].dest       = node_addrs[i];
    rs[i].size       = page_size;
    rs[i].is_on_chip = false;
  }
  if (leaf_unwrite) {
    rs[new_node_num].source     = (uint64_t)leaf_buffer;
    rs[new_node_num].dest       = leaf_addr;
    rs[new_node_num].size       = sizeof(Leaf);
    rs[new_node_num].is_on_chip = false;
  }
  dsm->write_batches_sync(rs, (leaf_unwrite ? new_node_num + 1 : new_node_num), cxt, coro_id);

  auto insert_leaf_merge_write_stop = std::chrono::high_resolution_clock::now();
  auto insert_leaf_merge_write_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_leaf_merge_write_stop - insert_leaf_merge_write_start);
  leaf_merge_write[dsm->getMyThreadID()] += insert_leaf_merge_write_duration.count();
  for(i=0;i<new_node_num;++ i)
  {
    MN_iops[dsm->getMyThreadID()][node_addrs[i].nodeID]++;
    MN_datas[dsm->getMyThreadID()][node_addrs[i].nodeID]+=page_size;
  }
  if(leaf_unwrite) 
  {
    MN_iops[dsm->getMyThreadID()][leaf_addr.nodeID]++;
    MN_datas[dsm->getMyThreadID()][leaf_addr.nodeID]+=sizeof(Leaf);
  }
  // cas
  auto remote_cas = [=](){
    auto insert_leaf_merge_cas_old_start = std::chrono::high_resolution_clock::now();
    bool res=dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
    auto insert_leaf_merge_cas_old_stop = std::chrono::high_resolution_clock::now();
  auto insert_leaf_merge_cas_old_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_leaf_merge_cas_old_stop - insert_leaf_merge_cas_old_start);
  if(res)  leaf_merge_cas_old[dsm->getMyThreadID()] += insert_leaf_merge_cas_old_duration.count();
    MN_iops[dsm->getMyThreadID()][e_ptr.nodeID]++;
    MN_datas[dsm->getMyThreadID()][e_ptr.nodeID]+=8;
    return res;
  };
  auto reclaim_memory = [=](){
    for (int i = 0; i < new_node_num; ++ i) {
      dsm->free(node_addrs[i], define::allocAlignPageSize);
    }
  };
// #ifndef TREE_TEST_ROWEX_ART
  bool res = remote_cas();
// #else
//   bool res = lock_and_cas_in_node(node_addr, remote_cas, cxt, coro_id);
// #endif
  if (!res) reclaim_memory();

  // cas the updated rev_ptr inside old leaf / old node
  if (res) {
    auto insert_leaf_merge_cas_rev_start = std::chrono::high_resolution_clock::now();
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(old_e.addr(), e_ptr, GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header)), cas_buffer, false, cxt);
        auto insert_leaf_merge_cas_rev_stop = std::chrono::high_resolution_clock::now();
  auto insert_leaf_merge_cas_rev_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_leaf_merge_cas_rev_stop - insert_leaf_merge_cas_rev_start);
   leaf_merge_cas_rev[dsm->getMyThreadID()] += insert_leaf_merge_cas_rev_duration.count();
    MN_iops[dsm->getMyThreadID()][old_e.addr().nodeID]++;
    MN_datas[dsm->getMyThreadID()][old_e.addr().nodeID]+=8;
  }


#ifdef TREE_ENABLE_CACHE
  if (res) {
    for (int i = 0; i < new_node_num; ++ i) {
        auto add_cache_start = std::chrono::high_resolution_clock::now();
      index_cache->add_to_cache(k, node_pages[i], GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header)));
       auto add_cache_stop = std::chrono::high_resolution_clock::now();
      auto add_cache_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(add_cache_stop - add_cache_start);
      cache_update_time[dsm->getMyThreadID()] +=add_cache_duration.count();
      leaf_merge_cache_update[dsm->getMyThreadID()] += add_cache_duration.count();
    }
  }
#endif
  // free
  delete[] rs; delete[] node_pages; delete[] node_addrs;
  return res;
}
*/

//新建一个内部节点、缓冲节点和叶节点
bool Tree::out_of_place_write_node(const Key &k, Value &v,const int depth_i, GlobalAddress& leaf_addr, int leaf_type,int klen,int vlen,int partial_len,uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e,const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {
  int depth = depth_i;
  int new_node_num = partial_len / (define::hPartialLenMax + 1) + 1;
  auto leaf_unwrite = (leaf_addr == GlobalAddress::Null());

  // allocate node
  GlobalAddress *node_addrs = new GlobalAddress[new_node_num];
  GlobalAddress bnode_addr = dsm->alloc(sizeof(InternalBuffer));

  dsm->alloc_nodes(new_node_num, node_addrs);
  

  // allocate & write new leaf
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();
  auto leaf_e_ptr = GADD(bnode_addr, sizeof(GlobalAddress) + sizeof(BufferHeader) + sizeof(BufferEntry) * 1);
 // printf("leaf buffer:  %d\n",leaf_buffer);
  if (leaf_unwrite) {  // !ONLY allocate once
    new (leaf_buffer) Leaf_kv(leaf_e_ptr,leaf_type,klen,vlen,k, v);
    leaf_addr = dsm->alloc(sizeof(Leaf_kv));
  }
  else {  // write the changed e_ptr inside new leaf  TODO: batch
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = leaf_e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
  }
//  printf("internal node addr:  %" PRIu64" bnode addr: %" PRIu64" leaf addr:  %" PRIu64"\n",node_addrs[0].val,bnode_addr.val,leaf_addr.val);
  // init inner nodes
  NodeType nodes_type = num_to_node_type(2);
  InternalPage ** node_pages = new InternalPage* [new_node_num];
  auto rev_ptr = e_ptr;
  for (int i = 0; i < new_node_num -1; ++ i) {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
 //   printf("internal node buffer:  %d\n",node_buffer);
    node_pages[i] = new (node_buffer) InternalPage(k, define::hPartialLenMax, depth, nodes_type, rev_ptr);
    node_pages[i]->records[0] = InternalEntry(get_partial(k, depth + define::hPartialLenMax),
                                              nodes_type, node_addrs[i + 1]);
    rev_ptr = GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header));
    partial_len -= define::hPartialLenMax + 1;
    depth += define::hPartialLenMax + 1;
  }
  { 
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
 //   printf("internal node buffer:  %d\n",node_buffer);
    node_pages[new_node_num -1] = new (node_buffer) InternalPage(k, partial_len, depth, nodes_type, rev_ptr);
    depth += partial_len + 1;
    node_pages[new_node_num -1]->records[0] = InternalEntry(diff_partial,old_e);   
    node_pages[new_node_num -1]->records[1] = InternalEntry(get_partial(k,depth - 1),1,bnode_addr);
     //     printf("thread  %d 7 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(node_pages[new_node_num -1]->hdr));
  }
  // init buffer nodes
  auto b_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
 //   printf("buffer node buffer:  %d\n",b_buffer);
 // if(node_addrs[0].val == 0) printf("0003!\n");
  InternalBuffer* buffernode = new (b_buffer) InternalBuffer(k,2,depth,1,2,node_addrs[0]);  // 暂时定初始2B作为partial key buffer地址
      //    printf("thread  %d 8 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(buffernode->hdr));
  buffernode->records[0] = BufferEntry(0,get_partial(k, depth+2 ),1,leaf_type,leaf_addr);
  
  // init the parent entry
  auto new_e = InternalEntry(old_e.partial,2,nodes_type, node_addrs[0]);
  auto page_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(nodes_type) * sizeof(InternalEntry);

  // batch_write nodes (doorbell batching)
  int i;
  RdmaOpRegion *rs =  new RdmaOpRegion[new_node_num + 2];
  for (i = 0; i < new_node_num; ++ i) {
    rs[i].source     = (uint64_t)node_pages[i];
    rs[i].dest       = node_addrs[i];
    rs[i].size       = page_size;
    rs[i].is_on_chip = false;
  }
  {
    rs[new_node_num].source     = (uint64_t)b_buffer;
    rs[new_node_num].dest       = bnode_addr;
    rs[new_node_num].size       = sizeof(InternalBuffer);
    rs[new_node_num].is_on_chip = false;
  }
  if (leaf_unwrite) {
    rs[new_node_num + 1].source     = (uint64_t)leaf_buffer;
    rs[new_node_num + 1].dest       = leaf_addr;
    rs[new_node_num + 1].size       = sizeof(Leaf_kv);
    rs[new_node_num + 1].is_on_chip = false;
  }
  dsm->write_batches_sync(rs,new_node_num + 2 , cxt, coro_id);

  // cas
  auto remote_cas = [=](){
    bool res=dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
    return res;
  };
  auto reclaim_memory = [=](){
    for (int i = 0; i < new_node_num; ++ i) {
      dsm->free(node_addrs[i], define::allocAlignPageSize);
    }
  };
// #ifndef TREE_TEST_ROWEX_ART
  bool res = remote_cas();
// #else
//   bool res = lock_and_cas_in_node(node_addr, remote_cas, cxt, coro_id);
// #endif
  if (!res) reclaim_memory();

  // cas the updated rev_ptr and depth inside buffer node 
  if (res) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(old_e.addr(), e_ptr, GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header)), cas_buffer, false, cxt);
  }

  if (res) {   //将内部节点和缓冲节点都加入cache
    for (int i = 0; i < new_node_num; ++ i) {
    //  printf("thread  %d 9 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(node_pages[i]->hdr));
      index_cache->add_to_cache(k, 0,node_pages[i], GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header)));
    }
//printf("thread  %d 10 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(buffernode->hdr));
    index_cache->add_to_cache(k, 1,(InternalPage *)buffernode, GADD(bnode_addr, sizeof(GlobalAddress) + sizeof(BufferHeader)));
  }

  // free
  delete[] rs; delete[] node_pages; delete[] node_addrs;
  return res;
}


bool Tree::out_of_place_write_node_from_buffer(const Key &k, Value &v,const int depth_i, GlobalAddress& leaf_addr, int leaf_type,int klen,int vlen,int partial_len,uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const BufferEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {
  int depth = depth_i;                           
  int new_node_num = partial_len / (define::hPartialLenMax + 1) + 1;
  auto leaf_unwrite = (leaf_addr == GlobalAddress::Null());

  // allocate node
  GlobalAddress *node_addrs = new GlobalAddress[new_node_num];
  GlobalAddress bnode_addr = dsm->alloc(sizeof(InternalBuffer));
  dsm->alloc_nodes(new_node_num, node_addrs);


  // allocate & write new leaf
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();
  auto leaf_e_ptr = GADD(bnode_addr, sizeof(GlobalAddress) + sizeof(BufferHeader) + sizeof(BufferEntry) * 1);

  if (leaf_unwrite) {  // !ONLY allocate once
    new (leaf_buffer) Leaf_kv(leaf_e_ptr,leaf_type,klen,vlen,k, v);
    leaf_addr = dsm->alloc(sizeof(Leaf_kv));
  }
  else {  // write the changed e_ptr inside new leaf  TODO: batch
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = leaf_e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
  }

  // init inner nodes
  NodeType nodes_type = num_to_node_type(2);
  InternalPage ** node_pages = new InternalPage* [new_node_num];
  auto rev_ptr = e_ptr;
  for (int i = 0; i < new_node_num - 1; ++ i) {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    node_pages[i] = new (node_buffer) InternalPage(k, define::hPartialLenMax, depth, nodes_type, rev_ptr);
    node_pages[i]->records[0] = InternalEntry(get_partial(k, depth + define::hPartialLenMax),
                                              nodes_type, node_addrs[i + 1]);
    rev_ptr = GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header));
    partial_len -= define::hPartialLenMax + 1;
    depth += define::hPartialLenMax + 1;
  }
  {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
 //   printf("internal node buffer:  %d\n",node_buffer);
    node_pages[new_node_num -1] = new (node_buffer) InternalPage(k, partial_len, depth, nodes_type, rev_ptr);
    depth += partial_len + 1;
    node_pages[new_node_num -1]->records[0] = InternalEntry(diff_partial,old_e);
    node_pages[new_node_num -1]->records[1] = InternalEntry(get_partial(k,depth-1),1,bnode_addr);
       //       printf("thread  %d 11 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(node_pages[new_node_num -1]->hdr));
  }

  // init buffer nodes
  auto b_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
 // if(node_addrs[0].val == 0) printf("0004!\n");
  InternalBuffer* buffernode = new (b_buffer) InternalBuffer(k,2,depth ,1,0,node_addrs[0]);  // 暂时定初始2B作为partial key buffer地址
         //   printf("thread  %d 12 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(buffernode->hdr));
  buffernode->records[0] = BufferEntry(0,get_partial(k, depth + 2  ),1,leaf_type,leaf_addr);
  
  // init the parent entry
  auto new_e = BufferEntry(2,old_e.partial, 1,nodes_type, node_addrs[0]);
  auto page_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(nodes_type) * sizeof(InternalEntry);

  // batch_write nodes (doorbell batching)
  int i;
  RdmaOpRegion *rs =  new RdmaOpRegion[new_node_num + 2];
  for (i = 0; i < new_node_num; ++ i) {
    rs[i].source     = (uint64_t)node_pages[i];
    rs[i].dest       = node_addrs[i];
    rs[i].size       = page_size;
    rs[i].is_on_chip = false;
  }
  {
    rs[new_node_num].source     = (uint64_t)buffernode;
    rs[new_node_num].dest       = bnode_addr;
    rs[new_node_num].size       = sizeof(InternalBuffer);
    rs[new_node_num].is_on_chip = false;

  }
  if (leaf_unwrite) {
    rs[new_node_num + 1].source     = (uint64_t)leaf_buffer;
    rs[new_node_num + 1].dest       = leaf_addr;
    rs[new_node_num + 1].size       = sizeof(Leaf_kv);
    rs[new_node_num + 1].is_on_chip = false;
  }
  dsm->write_batches_sync(rs, (leaf_unwrite ? new_node_num + 2 : new_node_num +1), cxt, coro_id);

  // cas
  auto remote_cas = [=](){
    bool res=dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
    return res;
  };
  auto reclaim_memory = [=](){
    for (int i = 0; i < new_node_num; ++ i) {
      dsm->free(node_addrs[i], define::allocAlignPageSize);
    }
  };
// #ifndef TREE_TEST_ROWEX_ART
  bool res = remote_cas();
// #else
//   bool res = lock_and_cas_in_node(node_addr, remote_cas, cxt, coro_id);
// #endif
  if (!res) reclaim_memory();

  // cas the updated rev_ptr inside old leaf / old node
  if (res) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(old_e.addr(), e_ptr, GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header)), cas_buffer, false, cxt);
  }

  if (res) {   //将内部节点和缓冲节点都加入cache
    for (int i = 0; i < new_node_num; ++ i) {
       //     printf("thread  %d 13 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(node_pages[i]->hdr));
      index_cache->add_to_cache(k, 0,node_pages[i], GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header)));
    }
 //   printf("thread  %d 14 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(buffernode->hdr));
    index_cache->add_to_cache(k, 1,(InternalPage *)buffernode, GADD(bnode_addr, sizeof(GlobalAddress) + sizeof(BufferHeader)));
  }

  // free
  delete[] rs; delete[] node_pages; delete[] node_addrs;
  return res;
}

/*
bool Tree::out_of_place_write_node_from_buffer(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, int leaf_type,int klen,int vlen,int partial_len, uint8_t partial,uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const BufferEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id) {

  auto insert_leaf_merge_write_start = std::chrono::high_resolution_clock::now();                                  
  int new_node_num = partial_len / (define::hPartialLenMax + 1) + 1;
  auto leaf_unwrite = (leaf_addr == GlobalAddress::Null());

  // allocate node
  GlobalAddress *node_addrs = new GlobalAddress[new_node_num];
  GlobalAddress bnode_addr = dsm->alloc(sizeof(InternalBuffer));
  dsm->alloc_nodes(new_node_num, node_addrs);


  // allocate & write new leaf
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_leaf_buffer();
  auto leaf_e_ptr = GADD(bnode_addr, sizeof(GlobalAddress) + sizeof(BufferHeader) + sizeof(BufferEntry) * 1);

  if (leaf_unwrite) {  // !ONLY allocate once
    new (leaf_buffer) Leaf_kv(leaf_e_ptr,leaf_type,klen,vlen,k, v);
    leaf_addr = dsm->alloc(sizeof(Leaf_kv));
  }
  else {  // write the changed e_ptr inside new leaf  TODO: batch
    auto ptr_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    *ptr_buffer = leaf_e_ptr;
    dsm->write((const char *)ptr_buffer, leaf_addr, sizeof(GlobalAddress), false, cxt);
  }

  // init inner nodes
  NodeType nodes_type = num_to_node_type(2);
  InternalPage ** node_pages = new InternalPage* [new_node_num];
  auto rev_ptr = e_ptr;
  for (int i = 0; i < new_node_num - 1; ++ i) {
    auto node_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
    node_pages[i] = new (node_buffer) InternalPage(k, define::hPartialLenMax, depth, nodes_type, rev_ptr);
    node_pages[i]->records[0] = InternalEntry(get_partial(k, depth + define::hPartialLenMax),
                                              nodes_type, node_addrs[i + 1]);
    rev_ptr = GADD(node_addrs[i], sizeof(GlobalAddress) + sizeof(Header));
    partial_len -= define::hPartialLenMax + 1;
    depth += define::hPartialLenMax + 1;
  }
  // init buffer nodes
  auto b_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
  InternalBuffer buffernode = new (b_buffer) InternalBuffer(k,3,depth +1 ,1,0,node_addrs[0]);  // 暂时定初始3B作为partial key
  buffernode.records[0].leaf_type= leaf_type;
  buffernode.records[0].partial= partial;  
  buffernode.records[0].prefix_type= 1;  
  buffernode.records[0].addr=leaf_addr;
  


  // init the parent entry
  auto new_e = BufferEntry(old_e.partial, nodes_type, node_addrs[0]);
  auto page_size = sizeof(GlobalAddress) + sizeof(Header) + node_type_to_num(nodes_type) * sizeof(InternalEntry);

  // batch_write nodes (doorbell batching)
  int i;
  RdmaOpRegion *rs =  new RdmaOpRegion[new_node_num + 2];
  for (i = 0; i < new_node_num; ++ i) {
    rs[i].source     = (uint64_t)node_pages[i];
    rs[i].dest       = node_addrs[i];
    rs[i].size       = page_size;
    rs[i].is_on_chip = false;
  }
  {
    rs[new_node_num].source     = (uint64_t)b_buffer;
    rs[new_node_num].dest       = bnode_addr;
    rs[new_node_num].size       = sizeof(InternalBuffer);
    rs[new_node_num].is_on_chip = false;

  }
  if (leaf_unwrite) {
    rs[new_node_num + 1].source     = (uint64_t)leaf_buffer;
    rs[new_node_num + 1].dest       = leaf_addr;
    rs[new_node_num + 1].size       = sizeof(Leaf_kv);
    rs[new_node_num + 1].is_on_chip = false;
  }
  dsm->write_batches_sync(rs, (leaf_unwrite ? new_node_num + 1 : new_node_num), cxt, coro_id);

  // cas
  auto remote_cas = [=](){
    bool res=dsm->cas_sync(e_ptr, (uint64_t)old_e, (uint64_t)new_e, ret_buffer, cxt);
    return res;
  };
  auto reclaim_memory = [=](){
    for (int i = 0; i < new_node_num; ++ i) {
      dsm->free(node_addrs[i], define::allocAlignPageSize);
    }
  };
// #ifndef TREE_TEST_ROWEX_ART
  bool res = remote_cas();
// #else
//   bool res = lock_and_cas_in_node(node_addr, remote_cas, cxt, coro_id);
// #endif
  if (!res) reclaim_memory();

  // cas the updated rev_ptr inside old leaf / old node
  if (res) {
    auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas(old_e.addr(), e_ptr, GADD(node_addrs[new_node_num - 1], sizeof(GlobalAddress) + sizeof(Header)), cas_buffer, false, cxt);
  }

  // free
  delete[] rs; delete[] node_pages; delete[] node_addrs;
  return res;
}
*/
void Tree::cas_node_type(NodeType next_type, GlobalAddress p_ptr, InternalEntry p, Header hdr,   //在这里没cas成功？？？？
                         CoroContext *cxt, int coro_id) {
  auto node_addr = p.addr();
  auto header_addr = GADD(node_addr, sizeof(GlobalAddress));
  auto cas_buffer_1 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto cas_buffer_2 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  std::pair<bool, bool> res = std::make_pair(false, false);

  // batch cas old_entry & node header to change node type
  auto remote_cas_both = [=, &p_ptr, &p, &hdr](){
    auto new_e = InternalEntry(next_type, p);
    RdmaOpRegion rs[2];
    rs[0].source     = (uint64_t)cas_buffer_1;
    rs[0].dest       = p_ptr;
    rs[0].is_on_chip = false;
    rs[1].source     = (uint64_t)cas_buffer_2;
    rs[1].dest       = header_addr;
    rs[1].is_on_chip = false;
    std::pair<bool, bool> res=dsm->two_cas_mask_sync(rs[0], (uint64_t)p, (uint64_t)new_e, ~0UL,rs[1], hdr, Header(next_type,hdr), Header::node_type_mask, cxt);

    return res;
  };

  // only cas old_entry
  auto remote_cas_entry = [=, &p_ptr, &p](){
    auto new_e = InternalEntry(next_type, p);
    return dsm->cas_sync(p_ptr, (uint64_t)p, (uint64_t)new_e, cas_buffer_1, cxt);
  };

  // only cas node_header
  auto remote_cas_header = [=, &hdr](){
    return dsm->cas_mask_sync(header_addr, hdr, Header(next_type,hdr), cas_buffer_2, Header::node_type_mask, cxt);
  };

  // read down to find target entry when split
  auto read_first_entry = [=, &p_ptr, &p](){
    p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
  };

re_switch:
  auto old_res = res;
  if (!old_res.first && !old_res.second) {
    res = remote_cas_both();
  }
  else {
    if (!old_res.first)  res.first  = remote_cas_entry();
    if (!old_res.second) res.second = remote_cas_header();
  }
  if (!res.first) {
    p = *(InternalEntry *)cas_buffer_1;
    // handle the conflict when switch & split/delete happen at the same time
    while (p != InternalEntry::Null() && p.node_type != 1 && p.addr() != node_addr) {
      read_first_entry();
      retry_cnt[dsm->getMyThreadID()][SWITCH_FIND_TARGET] ++;
    }
    if (p.addr() != node_addr || p.type() >= next_type) res.first = true;  // no need to retry
  }
  if (!res.second) {
    hdr = *(Header *)cas_buffer_2;
    if (hdr.type() >= next_type) res.second = true;  // no need to retry
  }
  if (!res.first || !res.second) {
    retry_cnt[dsm->getMyThreadID()][SWITCH_RETRY] ++;
    goto re_switch;
  }
}
void Tree::cas_node_type_from_buffer(NodeType next_type, GlobalAddress p_ptr, BufferEntry p, Header hdr,
                         CoroContext *cxt, int coro_id) {
  auto node_addr = p.addr();
  auto header_addr = GADD(node_addr, sizeof(GlobalAddress));
  auto cas_buffer_1 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto cas_buffer_2 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
  std::pair<bool, bool> res = std::make_pair(false, false);
  auto new_e = BufferEntry(next_type, p);

//  bool res1 = dsm->cas_sync(p_ptr, (uint64_t)p,(uint64_t) new_e,cas_buffer_1,cxt);
//  bool res2 = dsm->cas_sync(header_addr, hdr,Header(next_type,hdr),cas_buffer_2,cxt);
  // batch cas old_entry & node header to change node type
  auto remote_cas_both = [=, &p_ptr, &p, &hdr](){
    auto new_e = BufferEntry(next_type, p);
    RdmaOpRegion rs[2];
    rs[0].source     = (uint64_t)cas_buffer_1;
    rs[0].dest       = p_ptr;
    rs[0].is_on_chip = false;
    rs[1].source     = (uint64_t)cas_buffer_2;
    rs[1].dest       = header_addr;
    rs[1].is_on_chip = false;
    std::pair<bool, bool> res=dsm->two_cas_mask_sync(rs[0], (uint64_t)p, (uint64_t)new_e, ~0UL,
                                  rs[1], hdr, Header(next_type,hdr), Header::node_type_mask, cxt);

    return res;
  };

  // only cas old_entry
  auto remote_cas_entry = [=, &p_ptr, &p](){
    auto new_e = BufferEntry(next_type, p);
    return dsm->cas_sync(p_ptr, (uint64_t)p, (uint64_t)new_e, cas_buffer_1, cxt);
  };

  // only cas node_header
  auto remote_cas_header = [=, &hdr](){
    return dsm->cas_mask_sync(header_addr, hdr, Header(next_type,hdr), cas_buffer_2, Header::node_type_mask, cxt);
  };

  // read down to find target entry when split
  auto read_first_entry = [=, &p_ptr, &p](){
    p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header));
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(BufferEntry), cxt);
    p = *(BufferEntry *)entry_buffer;
  };

re_switch:
  auto old_res = res;
  if (!old_res.first && !old_res.second) {
    res = remote_cas_both();
  }
  else {
    if (!old_res.first)  res.first  = remote_cas_entry();
    if (!old_res.second) res.second = remote_cas_header();
  }
  if (!res.first) {
    p = *(BufferEntry *)cas_buffer_1;
    // handle the conflict when switch & split/delete happen at the same time
    while (p != BufferEntry::Null() && p.node_type != 1 && p.addr() != node_addr) {
      read_first_entry();
      retry_cnt[dsm->getMyThreadID()][SWITCH_FIND_TARGET] ++;
    }
    if (p.addr() != node_addr || p.type() >= next_type) res.first = true;  // no need to retry
  }
  if (!res.second) {
    hdr = *(Header *)cas_buffer_2;
    if (hdr.type() >= next_type) res.second = true;  // no need to retry
  }
  if (!res.first || !res.second) {
    retry_cnt[dsm->getMyThreadID()][SWITCH_RETRY] ++;
    goto re_switch;
  }
}
//新建很多个缓冲节点 有重复的往里面放  
bool Tree::out_of_place_write_buffer_node(const Key &k, Value &v, int depth,InternalBuffer* bnode,int leaf_type,int klen,int vlen,GlobalAddress leaf_addr,CacheEntry**&entry_ptr_ptr,CacheEntry*& entry_ptr,bool from_cache,InternalEntry old_e, GlobalAddress p_ptr,CoroContext *cxt, int coro_id) {
  //先获取锁 再修改 否则不修改
  static const uint64_t lock_cas_offset = ROUND_DOWN(STRUCT_OFFSET(InternalBuffer, lock_byte), 3);  //8B对齐
  static const uint64_t lock_mask       = 1UL << ((STRUCT_OFFSET(InternalBuffer, lock_byte) - lock_cas_offset) * 8);
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto acquire_lock = dsm->cas_mask_sync(GADD(old_e.addr(), lock_cas_offset), 0UL, ~0UL, cas_buffer, lock_mask, cxt);
  if(!acquire_lock) return false;

  depth ++;
  int first_empty=0;
  bool first_empty_set = false;
  int count_index[256][256];  //[][0] -> count  [1~] ->index
  int leaf_cnt = 0;
  BufferEntry leaf_addrs[256][256];
  thread_local std::vector<RdmaOpRegion> rs;
  int new_bnode_num = 0;
  int leaf_flag = 0; //叶节点的部分键是否重复
  uint8_t new_leaf_partial = get_partial(k,depth-1);
  BufferEntry *new_leaf_be;
  GlobalAddress *bnode_addrs;
  int bnodes_entry_index[256][256];
  memset(count_index,0,256*256*sizeof(int));
  memset(bnodes_entry_index,0,256*256*sizeof(int));

  for(int i=0; i <256 ;i++)
  {
    if(bnode->records[i].node_type == 0)   //统计叶节点
    {
      count_index[(int)bnode->records[i].partial][0] ++;
      count_index[(int)bnode->records[i].partial][count_index[(int)bnode->records[i].partial][0]] = i;
  //  if(count_index[(int)bnode.records[i].partial][0] > 1) printf("partial is %d \n",i);
    }
  }

  for(int i=0; i <256 ;i++)
  {
    if(count_index[i][0] >= 1)
    {
      if(i == (int)get_partial(k,depth)) leaf_flag =1;  //往下拿的时候有叶节点的 没有的话多生成一个新的缓冲节点
      new_bnode_num ++;
      leaf_cnt += count_index[i][0];
      bnodes_entry_index[new_bnode_num - 1][0] = count_index[i][0];
      for(int j = 0;j < count_index[i][0] ;j++)
      {
        bnodes_entry_index[new_bnode_num - 1][j+1] = count_index[i][j+1];
        leaf_addrs[new_bnode_num - 1][j].val = bnode->records[count_index[i][j + 1]].val;
        RdmaOpRegion r;
        r.dest       = bnode->records[count_index[i][j + 1]].addr();
        assert(r.dest !=0);
        r.size       = sizeof(Leaf_kv);
        r.is_on_chip = false;
        rs.push_back(r);
        if(j > 0 ) 
        {
          if(!first_empty_set)
          {
            first_empty = count_index[i][j + 1];first_empty_set = true;
          } 
         bnode->records[count_index[i][j + 1]] = BufferEntry::Null();
         }
      }
    }
  }
//  if(!leaf_flag) new_bnode_num ++;

  bnode_addrs = new GlobalAddress[new_bnode_num + 1];
  leaf_flag?  dsm->alloc_bnodes(new_bnode_num+1, bnode_addrs) :dsm->alloc_bnodes(new_bnode_num, bnode_addrs);
  auto leaves_buffer =(dsm->get_rbuf(0)).get_range_buffer();
  for(int i =0;i<(int) rs.size();i++)
  {
    rs[i].source = (uint64_t)leaves_buffer + i * define::allocAlignPageSize;
  }
  //读需要放在下一层的叶节点 read_batch
  dsm->read_batches_sync(rs);   //没读过来？？？搞成单次读呢？
  //写叶节点
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();

  leaf_addr = dsm->alloc(sizeof(Leaf_kv));


  Leaf_kv *leaves = new Leaf_kv [leaf_cnt];
  //读到了leaves_buffer
  for(int i = 0;i<leaf_cnt;i++)
  {
 
    leaves[i] = *(Leaf_kv *)(leaves_buffer + i * define::allocAlignPageSize);

  }
  leaf_cnt = 0;
  InternalBuffer **new_bnodes = new InternalBuffer* [new_bnode_num +1];  //预留一个 可能需要给叶节点 

  for (int i = 0; i < new_bnode_num ; ++ i) {    //会涉及到多次cas 开销 --->上锁
    auto bnode_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    std::vector<Key> leaf_key;
    GlobalAddress rev_ptr = GADD(old_e.addr(), sizeof(GlobalAddress) + sizeof(Header) + bnodes_entry_index[i][1] * sizeof(BufferEntry));
    new_bnodes[i] = new (bnode_buffer) InternalBuffer();
    new_bnodes[i]->rev_ptr.val = rev_ptr.val;
    for(int j =0;j<bnodes_entry_index[i][0];j++)
    {
      new_bnodes[i]->records[j].val = leaf_addrs[i][j].val;
      assert(new_bnodes[i]->records[j].packed_addr.mn_id == 0);
      leaf_key.push_back(leaves[leaf_cnt].get_key());
      leaf_cnt ++;
    }
    if(leaf_flag && bnode->records[bnodes_entry_index[i][1]].partial == new_leaf_partial)
    {
      leaf_key.push_back(k);
      leaf_cnt++;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].leaf_type = leaf_type;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].node_type = 0;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].prefix_type = 0;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].packed_addr={leaf_addr.nodeID,leaf_addr.offset >> ALLOC_ALLIGN_BIT};
      new (leaf_buffer) Leaf_kv(GADD(bnode_addrs[i],sizeof(GlobalAddress)+sizeof(BufferHeader)+bnodes_entry_index[i][0]*sizeof(BufferEntry)),leaf_type,klen,vlen,k,v);   //修改  叶节点的反向指针应该指向槽的地址 
      bnodes_entry_index[i][0] ++;
    }
    leaf_cnt -= bnodes_entry_index[i][0];

    int com_par_len = get_2B_partial(leaf_key,depth);
    if(com_par_len >2) com_par_len = 2;
    BufferHeader  bhdr(leaf_key[0], com_par_len, depth , bnodes_entry_index[i][0], 0);
    new_bnodes[i]->hdr.val = bhdr.val;
    
    for(int j =0;j<bnodes_entry_index[i][0];j++)
    {
      new_bnodes[i]->records[j].partial = get_partial(leaf_key.at(leaf_cnt),depth + com_par_len);
    }
     //修改bufferentry的地址 
    bnode->records[bnodes_entry_index[i][1]].packed_addr={bnode_addrs[i].nodeID, bnode_addrs[i].offset >> ALLOC_ALLIGN_BIT};
    bnode->records[bnodes_entry_index[i][1]].node_type = 1;
   // printf("thread  %d 15 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(new_bnodes[i]->hdr));
   assert(bnode->records[bnodes_entry_index[i][1]].packed_addr.mn_id == 0);
   assert(new_bnodes[i]->hdr.val != 0);
  }

  if(!leaf_flag)  //多搞一个缓冲节点
  { 
    auto bnode_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    new_bnodes[new_bnode_num] =new(bnode_buffer)  InternalBuffer(k,2,depth,1,0,GADD(old_e.addr(),sizeof(GlobalAddress)+sizeof(BufferHeader)+first_empty*sizeof(BufferEntry)));
    new_bnodes[new_bnode_num]->records[0].leaf_type = leaf_type;
    new_bnodes[new_bnode_num]->records[0].node_type = 0;
    new_bnodes[new_bnode_num]->records[0].prefix_type = 0;
    new_bnodes[new_bnode_num]->records[0].packed_addr={leaf_addr.nodeID,leaf_addr.offset >> ALLOC_ALLIGN_BIT};
    new (leaf_buffer) Leaf_kv(GADD(bnode_addrs[new_bnode_num],sizeof(GlobalAddress)+sizeof(BufferHeader)),leaf_type,klen,vlen,k,v); 
    new_bnode_num ++;
  }

  //修改原来的buffer node 为一个internal node  要上锁 
  bnode->hdr.count_1 = new_bnode_num;
/*
  for(int i=0;i<new_bnode_num;i++)
  {
    bnode.records[bnodes_entry_index[i][1]].packed_addr={bnode_addrs[i].nodeID, bnode_addrs[i].offset >> ALLOC_ALLIGN_BIT};
  }
*/
  bnode->unlock();
  auto old_page_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
  InternalBuffer * old_page;
  old_page = new (old_page_buffer) InternalBuffer(*bnode);
  Header new_hdr(bnode->hdr);
  old_page->hdr.val = new_hdr.val;
  old_page->lock_byte = 99;
  assert(old_page->hdr.val !=0);


  //整一个write_batch  写所有的缓冲节点和叶节点 还有写旧的叶节点
  /*  */
  RdmaOpRegion *rs_write =  new RdmaOpRegion[new_bnode_num + 2];
  memset(rs_write,0,sizeof(RdmaOpRegion)*(new_bnode_num + 2));

  for (int i = 0; i < new_bnode_num; ++ i) {
    rs_write[i].source     = (uint64_t)new_bnodes[i];
    rs_write[i].dest       = bnode_addrs[i];
    rs_write[i].size       = sizeof(InternalBuffer);
    rs_write[i].is_on_chip = false;
   // dsm->write((const char*)new_bnodes[i], bnode_addrs[i], sizeof(InternalBuffer), false, cxt);
  }
  {
    rs_write[new_bnode_num].source     = (uint64_t)leaf_buffer;
    rs_write[new_bnode_num].dest       = leaf_addr;
    rs_write[new_bnode_num].size       = sizeof(Leaf_kv);
    rs_write[new_bnode_num].is_on_chip = false;
  //  dsm->write((const char*)leaf_buffer, leaf_addr, sizeof(Leaf_kv), false, cxt);

    rs_write[new_bnode_num +1].source     = (uint64_t)old_page_buffer;
    rs_write[new_bnode_num +1].dest       = old_e.addr();
    rs_write[new_bnode_num +1].size       = sizeof(InternalBuffer);
    rs_write[new_bnode_num +1].is_on_chip = false;
  //  dsm->write((const char*)old_bnode_buffer, e_ptr, sizeof(InternalBuffer), false, cxt);
  }

  int internal_node_repeat = false;
  int i_1,j_1;
  for(int i = 0;i < 256;i++)
  {
    for(int j = 0;j< 256;j++)
    {
      if(i!= j && bnode->records[i]!=BufferEntry::Null() && bnode->records[i].partial == bnode->records[j].partial)
      {
        i_1 =i;
        j_1 =j;
        internal_node_repeat = true;
        break;
      }
    }
    if (internal_node_repeat) break;
  }


  dsm->write_batches_sync(rs_write, new_bnode_num + 2, cxt, coro_id);
  auto cas_node_type_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  InternalEntry new_entry(old_e);
  new_entry.child_type = 2;
  new_entry.node_type = static_cast<uint8_t>(NODE_256);
  new (cas_node_type_buffer) InternalEntry(new_entry);
  bool res =dsm->cas_sync(p_ptr, (uint64_t)old_e, (uint64_t)new_entry, cas_node_type_buffer, cxt);


  //先失效 再加
  if(from_cache)
  {
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
//  index_cache->add_to_cache(k, 1,(InternalPage*)old_bnode, GADD(e_ptr, sizeof(GlobalAddress) + sizeof(BufferHeader)));
if(res)
{
  for (int i = 0; i < new_bnode_num; ++ i) {
   //   printf("thread  %d 16 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(new_bnodes[i]->hdr));
      index_cache->add_to_cache(k,1,(InternalPage*)new_bnodes[i], GADD(bnode_addrs[i], sizeof(GlobalAddress) + sizeof(BufferHeader)));
  }
  return true;
}
return false;
}

//新建很多个缓冲节点 有重复的往里面放  
bool Tree::out_of_place_write_buffer_node_from_buffer(const Key &k, Value &v, int depth,InternalBuffer* bnode,int leaf_type,int klen,int vlen,GlobalAddress leaf_addr,CacheEntry**&entry_ptr_ptr,CacheEntry*& entry_ptr,bool from_cache,BufferEntry old_e, GlobalAddress p_ptr,CoroContext *cxt, int coro_id) {
  //先获取锁 再修改 否则不修改
  static const uint64_t lock_cas_offset = ROUND_DOWN(STRUCT_OFFSET(InternalBuffer, lock_byte), 3);  //8B对齐
  static const uint64_t lock_mask       = 1UL << ((STRUCT_OFFSET(InternalBuffer, lock_byte) - lock_cas_offset) * 8);
  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  auto acquire_lock = dsm->cas_mask_sync(GADD(old_e.addr(), lock_cas_offset), 0UL, ~0UL, cas_buffer, lock_mask, cxt);
  if(!acquire_lock) return false;

  depth ++;
  int first_empty=0;
  bool first_empty_set = false;
  int count_index[256][256];  //[][0] -> count  [1~] ->index
  int leaf_cnt = 0;
  BufferEntry leaf_addrs[256][256];
  thread_local std::vector<RdmaOpRegion> rs;
  int new_bnode_num = 0;
  int leaf_flag = 0; //叶节点的部分键是否重复
  uint8_t new_leaf_partial = get_partial(k,depth-1);
  BufferEntry *new_leaf_be;
  GlobalAddress *bnode_addrs;
  int bnodes_entry_index[256][256];
  memset(count_index,0,256*256*sizeof(int));
  memset(bnodes_entry_index,0,256*256*sizeof(int));

  for(int i=0; i <256 ;i++)
  {
    if(bnode->records[i].node_type == 0)   //统计叶节点
    {
      count_index[(int)bnode->records[i].partial][0] ++;
      count_index[(int)bnode->records[i].partial][count_index[(int)bnode->records[i].partial][0]] = i;
  //  if(count_index[(int)bnode.records[i].partial][0] > 1) printf("partial is %d \n",i);
    }
  }

  for(int i=0; i <256 ;i++)
  {
    if(count_index[i][0] >= 1)
    {
      if(i == (int)get_partial(k,depth)) leaf_flag =1;  //往下拿的时候有叶节点的 没有的话多生成一个新的缓冲节点
      new_bnode_num ++;
      leaf_cnt += count_index[i][0];
      bnodes_entry_index[new_bnode_num - 1][0] = count_index[i][0];
      for(int j = 0;j < count_index[i][0] ;j++)
      {
        bnodes_entry_index[new_bnode_num - 1][j+1] = count_index[i][j+1];
        leaf_addrs[new_bnode_num - 1][j].val = bnode->records[count_index[i][j + 1]].val;
        RdmaOpRegion r;
        r.dest       = bnode->records[count_index[i][j + 1]].addr();
        assert(r.dest !=0);
        r.size       = sizeof(Leaf_kv);
        r.is_on_chip = false;
        rs.push_back(r);
        if(j > 0 ) 
        {
          if(!first_empty_set)
          {
            first_empty = count_index[i][j + 1];first_empty_set = true;
          } 
         bnode->records[count_index[i][j + 1]] = BufferEntry::Null();
         }
      }
    }
  }
//  if(!leaf_flag) new_bnode_num ++;

  bnode_addrs = new GlobalAddress[new_bnode_num + 1];
  leaf_flag?  dsm->alloc_bnodes(new_bnode_num+1, bnode_addrs) :dsm->alloc_bnodes(new_bnode_num, bnode_addrs);
  auto leaves_buffer =(dsm->get_rbuf(0)).get_range_buffer();
  for(int i =0;i<(int) rs.size();i++)
  {
    rs[i].source = (uint64_t)leaves_buffer + i * define::allocAlignPageSize;
  }
  //读需要放在下一层的叶节点 read_batch
  dsm->read_batches_sync(rs);   //没读过来？？？搞成单次读呢？
  //写叶节点
  auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();

  leaf_addr = dsm->alloc(sizeof(Leaf_kv));


  Leaf_kv *leaves = new Leaf_kv [leaf_cnt];
  //读到了leaves_buffer
  for(int i = 0;i<leaf_cnt;i++)
  {
 
    leaves[i] = *(Leaf_kv *)(leaves_buffer + i * define::allocAlignPageSize);

  }
  leaf_cnt = 0;
  InternalBuffer **new_bnodes = new InternalBuffer* [new_bnode_num +1];  //预留一个 可能需要给叶节点 

  for (int i = 0; i < new_bnode_num ; ++ i) {    //会涉及到多次cas 开销 --->上锁
    auto bnode_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    std::vector<Key> leaf_key;
    GlobalAddress rev_ptr = GADD(old_e.addr(), sizeof(GlobalAddress) + sizeof(Header) + bnodes_entry_index[i][1] * sizeof(BufferEntry));
    new_bnodes[i] = new (bnode_buffer) InternalBuffer();
    new_bnodes[i]->rev_ptr.val = rev_ptr.val;
    for(int j =0;j<bnodes_entry_index[i][0];j++)
    {
      new_bnodes[i]->records[j].val = leaf_addrs[i][j].val;
      assert(new_bnodes[i]->records[j].packed_addr.mn_id == 0);
      leaf_key.push_back(leaves[leaf_cnt].get_key());
      leaf_cnt ++;
    }
    if(leaf_flag && bnode->records[bnodes_entry_index[i][1]].partial == new_leaf_partial)
    {
      leaf_key.push_back(k);
      leaf_cnt++;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].leaf_type = leaf_type;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].node_type = 0;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].prefix_type = 0;
      new_bnodes[i]->records[bnodes_entry_index[i][0]].packed_addr={leaf_addr.nodeID,leaf_addr.offset >> ALLOC_ALLIGN_BIT};
      new (leaf_buffer) Leaf_kv(GADD(bnode_addrs[i],sizeof(GlobalAddress)+sizeof(BufferHeader)+bnodes_entry_index[i][0]*sizeof(BufferEntry)),leaf_type,klen,vlen,k,v);   //修改  叶节点的反向指针应该指向槽的地址 
      bnodes_entry_index[i][0] ++;
    }
    leaf_cnt -= bnodes_entry_index[i][0];

    int com_par_len = get_2B_partial(leaf_key,depth);
    if(com_par_len >2) com_par_len = 2;
    BufferHeader  bhdr(leaf_key[0], com_par_len, depth , bnodes_entry_index[i][0], 0);
    new_bnodes[i]->hdr.val = bhdr.val;

    for(int j =0;j<bnodes_entry_index[i][0];j++)
    {
      new_bnodes[i]->records[j].partial = get_partial(leaf_key.at(leaf_cnt),depth + com_par_len);
    }
     //修改bufferentry的地址 
    bnode->records[bnodes_entry_index[i][1]].packed_addr={bnode_addrs[i].nodeID, bnode_addrs[i].offset >> ALLOC_ALLIGN_BIT};
    bnode->records[bnodes_entry_index[i][1]].node_type = 1;
   // printf("thread  %d 15 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(new_bnodes[i]->hdr));
   assert(bnode->records[bnodes_entry_index[i][1]].packed_addr.mn_id == 0);
   assert(new_bnodes[i]->hdr.val != 0);
  }

  if(!leaf_flag)  //多搞一个缓冲节点
  { 
    auto bnode_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    new_bnodes[new_bnode_num] =new(bnode_buffer)  InternalBuffer(k,2,depth,1,0,GADD(old_e.addr(),sizeof(GlobalAddress)+sizeof(BufferHeader)+first_empty*sizeof(BufferEntry)));
    new_bnodes[new_bnode_num]->records[0].leaf_type = leaf_type;
    new_bnodes[new_bnode_num]->records[0].node_type = 0;
    new_bnodes[new_bnode_num]->records[0].prefix_type = 0;
    new_bnodes[new_bnode_num]->records[0].packed_addr={leaf_addr.nodeID,leaf_addr.offset >> ALLOC_ALLIGN_BIT};
    new (leaf_buffer) Leaf_kv(GADD(bnode_addrs[new_bnode_num],sizeof(GlobalAddress)+sizeof(BufferHeader)),leaf_type,klen,vlen,k,v); 
    new_bnode_num ++;
  }

  //修改原来的buffer node 为一个internal node  要上锁 
  bnode->hdr.count_1 = new_bnode_num;
/*
  for(int i=0;i<new_bnode_num;i++)
  {
    bnode.records[bnodes_entry_index[i][1]].packed_addr={bnode_addrs[i].nodeID, bnode_addrs[i].offset >> ALLOC_ALLIGN_BIT};
  }
*/
  bnode->unlock();
  auto old_page_buffer = (dsm->get_rbuf(coro_id)).get_buffer_buffer();
  InternalBuffer * old_page;
  old_page = new (old_page_buffer) InternalBuffer(*bnode);
  Header new_hdr(bnode->hdr);
  old_page->hdr.val = new_hdr.val;
  assert(old_page->hdr.val !=0);


  //整一个write_batch  写所有的缓冲节点和叶节点 还有写旧的叶节点
  /*  */
  RdmaOpRegion *rs_write =  new RdmaOpRegion[new_bnode_num + 2];
  memset(rs_write,0,sizeof(RdmaOpRegion)*(new_bnode_num + 2));

  for (int i = 0; i < new_bnode_num; ++ i) {
    rs_write[i].source     = (uint64_t)new_bnodes[i];
    rs_write[i].dest       = bnode_addrs[i];
    rs_write[i].size       = sizeof(InternalBuffer);
    rs_write[i].is_on_chip = false;
   // dsm->write((const char*)new_bnodes[i], bnode_addrs[i], sizeof(InternalBuffer), false, cxt);
  }
  {
    rs_write[new_bnode_num].source     = (uint64_t)leaf_buffer;
    rs_write[new_bnode_num].dest       = leaf_addr;
    rs_write[new_bnode_num].size       = sizeof(Leaf_kv);
    rs_write[new_bnode_num].is_on_chip = false;
  //  dsm->write((const char*)leaf_buffer, leaf_addr, sizeof(Leaf_kv), false, cxt);

    rs_write[new_bnode_num +1].source     = (uint64_t)old_page_buffer;
    rs_write[new_bnode_num +1].dest       = old_e.addr();
    rs_write[new_bnode_num +1].size       = sizeof(InternalBuffer);
    rs_write[new_bnode_num +1].is_on_chip = false;
  //  dsm->write((const char*)old_bnode_buffer, e_ptr, sizeof(InternalBuffer), false, cxt);
  }

    int internal_node_repeat = false;
  int i_1,j_1;
  for(int i = 0;i < 256;i++)
  {
    for(int j = 0;j< 256;j++)
    {
      if(i!= j && bnode->records[i]!=BufferEntry::Null() && bnode->records[i].partial == bnode->records[j].partial)
      {
        i_1 =i;
        j_1 =j;
        internal_node_repeat = true;
        break;
      }
    }
    if (internal_node_repeat) break;
  }

  dsm->write_batches_sync(rs_write, new_bnode_num + 2, cxt, coro_id);
  auto cas_node_type_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
  BufferEntry new_entry(old_e);
  new_entry.node_type = 2;
  new_entry.leaf_type = static_cast<uint8_t>(NODE_256);
  new (cas_node_type_buffer) BufferEntry(new_entry);
  bool res =dsm->cas_sync(p_ptr, (uint64_t)old_e, (uint64_t)new_entry, cas_node_type_buffer, cxt);


  //先失效 再加
  if(from_cache)
  {
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);
  }
//  index_cache->add_to_cache(k, 1,(InternalPage*)old_bnode, GADD(e_ptr, sizeof(GlobalAddress) + sizeof(BufferHeader)));
if(res)
{
  for (int i = 0; i < new_bnode_num; ++ i) {
   //   printf("thread  %d 16 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(new_bnodes[i]->hdr));
      index_cache->add_to_cache(k,1,(InternalPage*)new_bnodes[i], GADD(bnode_addrs[i], sizeof(GlobalAddress) + sizeof(BufferHeader)));
  }
  return true;
}
return false;
}


bool Tree::insert_behind(const Key &k, Value &v, GlobalAddress p_ptr,int depth, GlobalAddress& leaf_addr, uint8_t partial_key, NodeType node_type,int leaf_type,int klen,int vlen,
                         const GlobalAddress &node_addr, uint64_t *ret_buffer, int& inserted_idx,
                         CoroContext *cxt, int coro_id) {
  int max_num, i;
  assert(node_type != NODE_256);
  max_num = node_type_to_num(node_type);
  // try cas an empty slot
  for (i = 0; i < 256 - max_num; ++ i) {
    auto slot_id = max_num + i;
    GlobalAddress e_ptr = GADD(node_addr, slot_id * sizeof(InternalEntry));
  //  auto cas_buffer = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    //新建一个缓冲节点 和叶节点 一起写过去 最后cas
    GlobalAddress b_addr;
    b_addr = dsm->alloc(sizeof(InternalBuffer));
    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaf_buffer();
    new (leaf_buffer) Leaf_kv(GADD(b_addr,sizeof(GlobalAddress)+sizeof(BufferHeader)),leaf_type,klen,vlen,k, v);
    leaf_addr = dsm->alloc(sizeof(Leaf_kv));
    auto b_buffer=(dsm->get_rbuf(coro_id)).get_buffer_buffer();
   // if(GADD(node_addr, slot_id * sizeof(InternalEntry)).val == 0) printf("0001!\n");
    InternalBuffer* buffer = new (b_buffer) InternalBuffer(k,2,depth +1 ,1,3,GADD(node_addr, slot_id * sizeof(InternalEntry)));  // 暂时定初始2B作为partial key buffer地址
    buffer->records[0] = BufferEntry(0,get_partial(k,depth+3),1,leaf_type,leaf_addr);

  
    auto new_e = InternalEntry(partial_key,1,b_addr);
    RdmaOpRegion *rs =  new RdmaOpRegion[2];
    {
      rs[0].source     = (uint64_t)b_buffer;
      rs[0].dest       = b_addr;
      rs[0].size       = sizeof(InternalBuffer);
      rs[0].is_on_chip = false;
    }
    {
        rs[1].source     = (uint64_t)leaf_buffer;
        rs[1].dest       = leaf_addr;
        rs[1].size       = sizeof(Leaf_kv);
        rs[1].is_on_chip = false;
    }
    dsm->write_batches_sync(rs, 2, cxt, coro_id);
    bool res = dsm->cas_sync(e_ptr, InternalEntry::Null(), (uint64_t)new_e, ret_buffer, cxt);  //可能这里cas不成功？？？
    delete[] rs; 

    if (res) {
      inserted_idx = slot_id;
      return true;
    }
    // cas fail, check
    else {
      auto e = *(InternalEntry*) ret_buffer;   //按理说e不应该为空呀 
      if (e.partial == partial_key) {  // same partial keys insert to the same empty slot
        inserted_idx = slot_id;
        return false;  // search next level
      }
    }
    retry_cnt[dsm->getMyThreadID()][INSERT_BEHIND_TRY_NEXT] ++;
  }
  assert(false);
}

bool Tree::search(const Key &k, Value &v, CoroContext *cxt, int coro_id) {   ///设置上限
  assert(dsm->is_register());
  bool search_res = false;
  // traversal
  GlobalAddress p_ptr;
  InternalEntry p;
  BufferEntry bp;
  int depth;
  int retry_flag = FIRST_TRY;
  int leaf_type = -1;
  int parent_type = 0; //至上上层节点是internal node（0）还是internal buffer（1）


  // cache
  bool from_cache = false;
  CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  CacheEntry* cache_entry_parent;
  int entry_idx = -1;
  int cache_depth = 0;

  // temp
  char* page_buffer;
  bool is_valid, type_correct;
  InternalPage* p_node = nullptr;
  InternalBuffer* bp_node = nullptr;
  Header hdr;
  BufferHeader bhdr;
  int max_num;
  int parent_parent_type = 0;
  int buffer_from_cache_flag = 0;
  int first_buffer = 0;

  from_cache = index_cache->search_from_cache(k, entry_ptr_ptr, entry_ptr, parent_parent_type,entry_idx,cache_entry_parent,first_buffer);   //check   直接从cache里面找到一个 
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    depth = entry_ptr->depth;
    parent_type  = entry_ptr->node_type;
    if(entry_ptr->node_type == 1)   //如果cache找到的缓冲节点则直接去读吧！！！  后面如果是从cache来的 并且类型就是一个缓冲节点就不用再读一遍了 还是再读一次吧、、、
    { 
      if(first_buffer) 
      {
        p_ptr = root_ptr_ptr;
        p = get_root_ptr(cxt, coro_id);
        parent_type = 0;
        depth = 0;
      }
      else{
        p_ptr = GADD(cache_entry_parent->addr,sizeof(InternalEntry)*entry_idx);
        p = cache_entry_parent->records[entry_idx];
        parent_type = cache_entry_parent->node_type;
        depth = cache_entry_parent->depth;
      }
      buffer_from_cache_flag = true;
    }
    bp.val = p.val;

  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }

  depth ++;
  cache_depth = depth;
  assert(p != InternalEntry::Null());

next:
  retry_cnt[dsm->getMyThreadID()][retry_flag] ++;
  // 1. If we are at a NULL node


  if(parent_type == 0)   //一个内部节点 顺着往下找
  {
  if (p == InternalEntry::Null()) {
    search_res = false;
    goto search_finish;
  }

  // 2. If we are at a buffer, read the buffer
  if (p.child_type == 1) {

    auto buffer_buffer =  (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    is_valid = read_buffer_node(p.addr(),  buffer_buffer, p_ptr, depth, from_cache,cxt, coro_id);
    bp_node = (InternalBuffer *)buffer_buffer;

    if (!is_valid) {  // node deleted || outdated cache entry in cached node
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_Buffer_NODE;
    goto next;
  }
    //2.1 check partial key
    bhdr=bp_node->hdr;
    if (depth == hdr.depth) {
    //      printf("thread  %d 18 node value is %" PRIu64" \n",(int)dsm->getMyThreadID( ),(uint64_t)(bp_node->hdr));
    index_cache->add_to_cache(k, 1,(InternalPage*)bp_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(BufferHeader)));
    }

    for (int i = 0; i < bhdr.partial_len; ++ i) {      //查看部分键前n个字节
    if (get_partial(k, bhdr.depth + i) != bhdr.partial[i]) {
      search_res = false;
      goto search_finish;
    }
    }
    depth = bhdr.depth + bhdr.partial_len ;
  //  uint16_t fp = generateFingerprint(k);

    //2.2 if all partial key match search from the start else from the end 
//    if(get_partial(k, bhdr.depth + bhdr.partial_len -1 ) == bhdr.partial[bhdr.partial_len -1 ] )
//    {
      int leaf_cnt = 0;
      GlobalAddress leaf_addrs[256];
      GlobalAddress leaves_ptr[256];

      uint8_t partial = get_partial(k, bhdr.depth + bhdr.partial_len);
      for(int i =0 ; i < 256 ;i++)
      {
      if(bp_node->records[i] != BufferEntry::Null()&&bp_node->records[i].partial == partial )
      {
      //  assert(bp_node->records[i].addr().nodeID == 0);
        if(bp_node->records[i].node_type == 1 || bp_node->records[i].node_type == 2)   //是一个缓冲节点 或者内部节点 继续往下找 
        {
          bp = bp_node->records[i];
          p_ptr = GADD(p.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          depth ++;
          parent_type = 1;
          from_cache = false;
          retry_flag = FIND_NEXT;
          goto next;
        }
        else 
        {
          leaf_addrs[leaf_cnt] = bp_node->records[i].addr();
          leaves_ptr[leaf_cnt]  = GADD(p.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          leaf_cnt ++;   
        }
      }
      }
      if(leaf_cnt == 0)
      {
        search_res = false;
        goto search_finish;
      }
      //read_batch 都读过来检查 


    auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaves_buffer(leaf_cnt); 
    is_valid = read_leaves(leaf_addrs, leaf_buffer,leaf_cnt,leaves_ptr,from_cache,cxt,coro_id);

    if (!is_valid) {
      // re-read leaf entry
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
      auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
      dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
      p = *(InternalEntry *)entry_buffer;
      from_cache = false;
      retry_flag = INVALID_LEAF;
      goto next;
    }
    for(int i =0;i<leaf_cnt;i++)
    {
      auto leaf = (Leaf_kv*) leaf_buffer + i* define::allocAlignKVLeafSize;
      auto _k = leaf->get_key();

      // 2.3 Check if it is the key we search
      if (_k == k) {
        v = leaf->get_value();
        search_res = true;
      }
      goto search_finish;
    }
    return false;

    }

  // 3. Find out a node
  // 3.1 read the node
if(p.child_type == 2)
{
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache,cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
    // re-read node entry
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_Internal_NODE;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
  if (depth == hdr.depth) {
    index_cache->add_to_cache(k,0,p_node, GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header)));
  }


  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) {
      search_res = false;
      goto search_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;

  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(p.type());
  // find from the exist slot
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, hdr.depth + hdr.partial_len)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      parent_type = 0;
      depth ++;
      from_cache = false;      
      retry_flag = FIND_NEXT;
      goto next;  // search next level
    }
  }
}
}
else{   //parent是一个buffernode
  if (bp == BufferEntry::Null()) {
    search_res = false;
    goto search_finish;
  }

  // 2. If we are at a buffer, read the buffer
  if (bp.node_type == 1) {

    auto buffer_buffer =  (dsm->get_rbuf(coro_id)).get_buffer_buffer();
    is_valid = read_buffer_node(bp.addr(), buffer_buffer, p_ptr, depth,from_cache, cxt, coro_id);
    bp_node = (InternalBuffer *)buffer_buffer;
    if (!is_valid) {  // node deleted || outdated cache entry in cached node
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_buffer_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(BufferEntry), cxt);
    bp = *(BufferEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_Buffer_NODE;
    goto next;
  }
    //2.1 check partial key
    bhdr=bp_node->hdr;

    for (int i = 0; i < bhdr.partial_len; ++ i) {      //查看部分键前n个字节
    if (get_partial(k, bhdr.depth + i) != bhdr.partial[i]) {
      search_res = false;
      goto search_finish;
    }
    }
    depth = bhdr.depth + bhdr.partial_len;
  //  uint16_t fp = generateFingerprint(k);

    //2.2 if all partial key match search from the start else from the end 
//    if(get_partial(k, bhdr.depth + bhdr.partial_len -1 ) == bhdr.partial[bhdr.partial_len -1 ] )
//    {
      int leaf_cnt = 0;
      GlobalAddress leaf_addrs[256];
      GlobalAddress leaves_ptr[256];

      uint8_t partial = get_partial(k, bhdr.depth + bhdr.partial_len);
      for(int i =0 ; i < 256 ;i++)
      {
      if(bp_node->records[i] != BufferEntry::Null()&&bp_node->records[i].partial == partial )
      {
      //  assert(bp_node->records[i].addr().nodeID == 0);
        if(bp_node->records[i].node_type == 1 || bp_node->records[i].node_type == 2)   //是一个缓冲节点 或者内部节点 继续往下找 
        {
          bp = bp_node->records[i];
          p_ptr = GADD(p.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          depth ++;
          parent_type = 1;
          from_cache = false;
          retry_flag = FIND_NEXT;
          goto next;
        }
        else 
        {
          leaf_addrs[leaf_cnt] = bp_node->records[i].addr();
          leaves_ptr[leaf_cnt]  = GADD(p.addr(), sizeof(GlobalAddress)+sizeof(BufferHeader) + i*sizeof(BufferEntry));
          leaf_cnt ++;   
        }
      }
      }
      if(leaf_cnt == 0)
      {
        search_res = false;
        goto search_finish;
      }
    //2.3 kv leaf
     auto leaf_buffer = (dsm->get_rbuf(coro_id)).get_kvleaves_buffer(leaf_cnt); 

    
    is_valid = read_leaves(leaf_addrs, leaf_buffer,leaf_cnt,leaves_ptr,from_cache,cxt,coro_id);

    if (!is_valid) {
      if (from_cache) {
        index_cache->invalidate(entry_ptr_ptr, entry_ptr);
      }
      auto entry_buffer = (dsm->get_rbuf(coro_id)).get_buffer_entry_buffer();
      dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(BufferEntry), cxt);
      bp = *(BufferEntry *)entry_buffer;
      retry_flag = INVALID_LEAF;
      goto next;
    }
    for(int i =0;i<leaf_cnt;i++)
    {
      auto leaf = (Leaf_kv*) leaf_buffer + i* define::allocAlignKVLeafSize;
      auto _k = leaf->get_key();

      // 2.3 Check if it is the key we search
      if (_k == k) {
        v = leaf->get_value();
        search_res = true;
      }
      goto search_finish;
    }
    return false;
  }

  // 3. Find out a node
  // 3.1 read the node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node_from_buffer(bp, type_correct, page_buffer, p_ptr, depth, from_cache,cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
    // re-read node entry

    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_buffer_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(BufferEntry), cxt);
    bp = *(BufferEntry *)entry_buffer;
    from_cache = false;
    retry_flag = INVALID_Internal_NODE;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
  if (depth == hdr.depth) {
    index_cache->add_to_cache(k,0,p_node, GADD(bp.addr(), sizeof(GlobalAddress) + sizeof(BufferHeader)));
  }

  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(k, hdr.depth + i) != hdr.partial[i]) {
      search_res = false;
      goto search_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;

  // 3.3 try get the next internalEntry
  max_num = node_type_to_num(p.type());
  // find from the exist slot
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(k, hdr.depth + hdr.partial_len)) {
      p_ptr = GADD(bp.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      depth ++;
      parent_type = 0;
      from_cache = false;
      retry_flag = FIND_NEXT;
      goto next;  // search next level
    }
  }

  }
search_finish:
    auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
    cache_hit[dsm->getMyThreadID()] += hit;
    cache_miss[dsm->getMyThreadID()] += (1 - hit);
  return search_res;
}


/*
void Tree::search_entries(const Key &from, const Key &to, int target_depth, std::vector<ScanContext> &res, CoroContext *cxt, int coro_id) {
  assert(dsm->is_register());

  GlobalAddress p_ptr;
  InternalEntry p;
  int depth;
  bool from_cache = false;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;
  int cache_depth = 0;

  bool type_correct;
  char* page_buffer;
  bool is_valid;
  InternalPage* p_node;
  Header hdr;
  int max_num;

  // search local cache
#ifdef TREE_ENABLE_CACHE
  from_cache = index_cache->search_from_cache(from, entry_ptr_ptr, entry_ptr, entry_idx);
  if (from_cache) { // cache hit
    assert(entry_idx >= 0);
    p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
    p = entry_ptr->records[entry_idx];
    depth = entry_ptr->depth;
  }
  else {
    p_ptr = root_ptr_ptr;
    p = get_root_ptr(cxt, coro_id);
    depth = 0;
  }
#else
  p_ptr = root_ptr_ptr;
  p = get_root_ptr(cxt, coro_id);
  depth = 0;
#endif
  depth ++;
  cache_depth = depth;

next:
  // 1. If we are at a NULL node
  if (p == InternalEntry::Null()) {
    goto search_finish;
  }

  // 2. Check if it is the target depth
  if (depth == target_depth) {
    res.push_back(ScanContext(p, p_ptr, depth-1, from_cache, entry_ptr_ptr, entry_ptr, from, to, BORDER, BORDER));
    goto search_finish;
  }
  if (p.is_leaf) {
    goto search_finish;
  }

  // 3. Find out a node
  // 3.1 read the node
  page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  is_valid = read_node(p, type_correct, page_buffer, p_ptr, depth, from_cache, cxt, coro_id);
  p_node = (InternalPage *)page_buffer;

  if (!is_valid) {  // node deleted || outdated cache entry in cached node
#ifdef TREE_ENABLE_CACHE
    // invalidate the old node cache
    if (from_cache) {
      index_cache->invalidate(entry_ptr_ptr, entry_ptr);
    }
#endif
    // re-read node entry
    auto entry_buffer = (dsm->get_rbuf(coro_id)).get_entry_buffer();
    dsm->read_sync((char *)entry_buffer, p_ptr, sizeof(InternalEntry), cxt);
    p = *(InternalEntry *)entry_buffer;
    from_cache = false;
    goto next;
  }

  // 3.2 Check header
  hdr = p_node->hdr;
#ifdef TREE_ENABLE_CACHE
  if (from_cache && !type_correct) {
    index_cache->invalidate(entry_ptr_ptr, entry_ptr);  // invalidate the out dated node type
  }
#else
  UNUSED(type_correct);
#endif
  for (int i = 0; i < hdr.partial_len; ++ i) {
    if (get_partial(from, hdr.depth + i) != hdr.partial[i]) {
      goto search_finish;
    }
    if (hdr.depth + i + 1 == target_depth) {
      range_query_on_page(p_node, from_cache, depth-1,
                          p_ptr, p,
                          from, to, BORDER, BORDER, res);
      goto search_finish;
    }
  }
  depth = hdr.depth + hdr.partial_len;

  // 3.3 try get the next internalEntry
  // find from the exist slot
  max_num = node_type_to_num(p.type());
  for (int i = 0; i < max_num; ++ i) {
    auto old_e = p_node->records[i];
    if (old_e != InternalEntry::Null() && old_e.partial == get_partial(from, hdr.depth + hdr.partial_len)) {
      p_ptr = GADD(p.addr(), sizeof(GlobalAddress) + sizeof(Header) + i * sizeof(InternalEntry));
      p = old_e;
      from_cache = false;
      depth ++;
      goto next;  // search next level
    }
  }
search_finish:
#ifdef TREE_ENABLE_CACHE
  auto hit = (cache_depth == 1 ? 0 : (double)cache_depth / depth);
  cache_hit[dsm->getMyThreadID()] += hit;
  cache_miss[dsm->getMyThreadID()] += (1 - hit);
#endif
  return;
}
*/
/*
  range query, DO NOT support corotine currently
*/
// [from, to)
/**/
void Tree::range_query(const Key &from, const Key &to, std::map<Key, Value> &ret) {

}


void Tree::range_query_on_page(InternalPage* page, bool from_cache, int depth,
                               GlobalAddress p_ptr, InternalEntry p,
                               const Key &from, const Key &to, State l_state, State r_state,
                               std::vector<ScanContext>& res) {

}


void Tree::run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req, int req_num) {
  using namespace std::placeholders;

  assert(coro_cnt <= MAX_CORO_NUM);
  for (int i = 0; i < coro_cnt; ++i) {
    RequstGen *gen = gen_func(dsm, req, req_num, i, coro_cnt);
    worker[i] = CoroCall(std::bind(&Tree::coro_worker, this, _1, gen, work_func, i));
  }

  master = CoroCall(std::bind(&Tree::coro_master, this, _1, coro_cnt));

  master();
}


void Tree::coro_worker(CoroYield &yield, RequstGen *gen, WorkFunc work_func, int coro_id) {
  CoroContext ctx;
  ctx.coro_id = coro_id;
  ctx.master = &master;
  ctx.yield = &yield;

  Timer coro_timer;
  auto thread_id = dsm->getMyThreadID();

  while (!need_stop) {
    auto r = gen->next();

    coro_timer.begin();
    work_func(this, r, &ctx, coro_id);
    auto us_10 = coro_timer.end() / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][coro_id][us_10]++;
  }
}


void Tree::coro_master(CoroYield &yield, int coro_cnt) {
  for (int i = 0; i < coro_cnt; ++i) {
    yield(worker[i]);
  }
  while (!need_stop) {
    uint64_t next_coro_id;

    if (dsm->poll_rdma_cq_once(next_coro_id)) {
      yield(worker[next_coro_id]);
    }
    // uint64_t wr_ids[POLL_CQ_MAX_CNT_ONCE];
    // int cnt = dsm->poll_rdma_cq_batch_once(wr_ids, POLL_CQ_MAX_CNT_ONCE);
    // for (int i = 0; i < cnt; ++ i) {
    //   yield(worker[wr_ids[i]]);
    // }

    if (!busy_waiting_queue.empty()) {
    // int cnt = busy_waiting_queue.size();
    // while (cnt --) {
      auto next = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      next_coro_id = next.first;
      if (next.second()) {
        yield(worker[next_coro_id]);
      }
      else {
        busy_waiting_queue.push(next);
      }
    }
  }
}


void Tree::statistics() {
#ifdef TREE_ENABLE_CACHE
  index_cache->statistics();
#endif
}

void Tree::clear_debug_info() {
  memset(cache_miss, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(cache_hit, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  // memset(try_lock, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(write_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_handover_num, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_leaf_retry, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(leaf_cache_invalid, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_leaf, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_node_repair, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(try_read_node, 0, sizeof(uint64_t) * MAX_APP_THREAD);
  memset(read_node_type, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_NODE_TYPE_NUM);
  memset(retry_cnt, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_FLAG_NUM);
}
