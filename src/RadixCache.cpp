#include "RadixCache.h"

#include <random>
#include <queue>
#include <vector>
#include <set>
#include <queue>
#include <chrono>


RadixCache::RadixCache(int cache_size, DSM *dsm) : cache_size(cache_size), dsm(dsm) {
  free_manager = new FreeMemManager(define::MB * cache_size);
  cache_root = new CacheNode();
  node_queue = new tbb::concurrent_queue<CacheNode*>();
  node_queue->push(cache_root);
}
void RadixCache::clear() {
  free_manager = new FreeMemManager(define::MB * cache_size);
  cache_root = new CacheNode();
  node_queue = new tbb::concurrent_queue<CacheNode*>();
  node_queue->push(cache_root);
}

void RadixCache::add_to_cache(const Key& k, int node_type, const InternalPage* p_node, const GlobalAddress &node_addr) {
//if(p_node->rev_ptr.val == 88841248571392) printf("its meeeeeeeeeeeeeeeeeeeeeeee!\n");
  auto depth = p_node->hdr.depth - 1;
  if (depth == 0) return;   //如果是基数树根节点指向的第一个内部节点不放在cache？

  std::vector<uint8_t> byte_array(k.begin(), k.begin() + depth);  //存到这个深度的所有字节
  for (int i = 0; i < (int)p_node->hdr.partial_len; ++ i) byte_array.push_back(p_node->hdr.partial[i]);  //再存下新的内部节点的partialkey  也就是 byte_arry里面存放由根节点到这个内部节点的所有键（包括内部节点本身的部分键）

  auto new_entry = new CacheEntry(p_node,node_type,node_addr);
  
  _insert(byte_array, new_entry);
#ifndef CACHE_ENABLE_ART
  free_manager->consume(sizeof(Key));  // emulate hash-based cache
#endif
  if (free_manager->remain_size() < 0) {
    _evict();
  }

  return;
}

void RadixCache::_insert(const std::vector<uint8_t>& byte_array, CacheEntry* new_entry) {
  CacheNode* parent_node = nullptr;
  CacheNode* node = cache_root;
  int idx = 0;

next:
  // 1. parse header
  auto hdr = (CacheHeader *)node->header;
  for (int i = 0; i < (int)hdr->partial.size(); ++ i) {   //要进行分裂   也是新建一个cache node
    auto cur_partial = byte_array[hdr->depth + i];
    if (hdr->depth + i == (int)byte_array.size() - 1 || cur_partial != hdr->partial[i]) {
      // split
      auto partial_len = hdr->depth + i - idx;
      CacheNode* nested_node = nullptr;
      auto new_node = new CacheNode(byte_array, idx, partial_len, hdr->partial[i], node, cur_partial, new_entry, nested_node);
      auto& parent_node_entry = parent_node->records[byte_array[idx - 1]];
      auto ret_node = __sync_val_compare_and_swap(&(parent_node_entry.next), node, new_node);
      if (ret_node == node) {  // cas success
        auto new_hdr = CacheHeader::split_header(hdr, i);
        // update header
        auto ret_hdr = (CacheHeader *)__sync_val_compare_and_swap(&(node->header), hdr, new_hdr);
        if (ret_hdr == hdr) _safely_delete(ret_hdr);  // cas success
        else delete new_hdr;
        free_manager->consume_by_node(new_node);
        if (nested_node) free_manager->consume_by_node(nested_node);
        free_manager->consume_by_node(node);
        free_manager->consume(new_entry->content_size());
      }
      else {  // cas fail
        if (ret_node) {
          node = (CacheNode *)ret_node;
          delete new_node;
          if (nested_node) delete nested_node;
          goto next;
        }
        else  {  // node is deleted
          node = (CacheNode *)__sync_val_compare_and_swap(&(parent_node_entry.next), 0UL, new_node);
          if (node) {
            delete new_node;
            if (nested_node) delete nested_node;
            goto next;
          }
          free_manager->consume_by_node(new_node);
          if (nested_node) free_manager->consume_by_node(nested_node);
          free_manager->consume(new_entry->content_size());
        }
      }
      // record
      node_queue->push(new_node);
      if (nested_node) node_queue->push(nested_node);
      CacheMap::const_iterator tmp = (nested_node ? nested_node->records.find(byte_array.back()) : new_node->records.find(cur_partial));
      eviction_list.push(std::make_pair(&(tmp->second.cache_entry), new_entry));
      return;
    }
  }
  idx = hdr->depth + hdr->partial.size();  //和depth功能一致

  // 2. parse_node
  auto& cache_map = node->records;
  auto partial = byte_array[idx];

  // 2.1 last level
  if (idx == (int)byte_array.size() - 1) {
    auto& node_entry = cache_map[partial];
    auto old_entry = (CacheEntry *)node_entry.cache_entry;
    if (__sync_bool_compare_and_swap(&(node_entry.cache_entry), old_entry, new_entry)) {
      free_manager->consume_by_node(node);
      free_manager->consume(new_entry->content_size());
      if (old_entry) {
        free_manager->free(old_entry->content_size());
        _safely_delete(old_entry);
      }
      eviction_list.push(std::make_pair(&(node_entry.cache_entry), new_entry));
    }
    else {
      delete new_entry;
    }
    return;
  }
  // 2.2 internal level
  else {    //一直要找到最下面一层的节点
    auto& node_entry = cache_map[partial];
    if (node_entry.next == nullptr) {
      auto next_node = new CacheNode(byte_array, idx + 1, new_entry);
      auto ret_node = __sync_val_compare_and_swap(&(node_entry.next), 0UL, next_node);
      if (ret_node == 0UL) {  // cas success
        // record
        node_queue->push(next_node);
        CacheMap::const_iterator tmp = next_node->records.find(byte_array.back());
        eviction_list.push(std::make_pair(&(tmp->second.cache_entry), new_entry));
        free_manager->consume_by_node(node);
        free_manager->consume_by_node(next_node);
        free_manager->consume(new_entry->content_size());
        return;
      }
      else {  // cas fail
        delete next_node;
        parent_node = node;
        node = (CacheNode *)ret_node;
        idx ++;
        goto next;
      }
    }
    else {
      parent_node = node;
      node = (CacheNode *)(node_entry.next);
      idx ++;
      goto next;
    }
  }
}

void change_node_type(CacheEntry*& entry_ptr)
{
  entry_ptr ->node_type = 0;
}


bool RadixCache::search_from_cache(const Key& k,CacheEntry**& entry_ptr_ptr, CacheEntry*& entry_ptr, int& parent_parent_type,int& entry_idx,CacheEntry* & cache_entry_parent,int& first_buffer) {  //当发现是一个缓冲节点直接返回内部节点？  entry_ptr_ptr是地址 entry_ptr的地址

  CacheKey byte_array(k.begin(), k.begin() + define::maxkeyLen - 1);

  SearchRetStk ret;
  if(_search(byte_array, ret)) {
    while(!ret.empty()) {
      const auto& item = ret.top();    //已经是最接近叶节点的一个缓冲节点了    一定是一个缓冲节点？   不一定 可能失效 
      if(item.entry_ptr == 0) return false;
      auto cache_entry = item.entry_ptr;
      auto next_partial = k.at(item.next_idx);
      if (cache_entry) {
        if(cache_entry->node_type == 0)
        {
            for (int i = 0; i < (int)cache_entry->records.size(); ++ i) {  //一个个查看slot
            const auto& e = cache_entry->records[i];
            if (e != InternalEntry::Null() && e.partial == next_partial) {       //找到部分键匹配的了
              entry_ptr = cache_entry;
              // __sync_fetch_and_add(&(entry_ptr->counter), 1UL);
              entry_ptr_ptr = item.entry_ptr_ptr;
              entry_idx = i;
              return true;
            }
          }

        }
        else{       //如果是最接近叶节点的缓冲节点直接返回该缓冲节点  或者返回多个槽？
            for (int i = 0; i < (int)cache_entry->records.size(); ++ i) {  //一个个查看slot
               BufferEntry e = *((BufferEntry*)&cache_entry->records[i]);
            if (e != BufferEntry::Null() && e.partial == next_partial) {       //找到部分键匹配的了  应该返回这个缓冲节点本身 而不是缓冲节点的槽  所以需要在上一个entry里面去找buffer对应的slot的位置  现在是buffer 上一级起码还有一个节点
            
              entry_ptr = cache_entry;
              // __sync_fetch_and_add(&(entry_ptr->counter), 1UL);
              entry_ptr_ptr = item.entry_ptr_ptr;
              entry_idx = i; //叶节点开始的位置 也可能不是一个叶节点
               //有可能是生成第一个缓冲节点 所以不会有上一节的节点

              ret.pop();
              if(ret.empty())  //已经是最后一个节点了
              {
                first_buffer = 1;
              }
              else{
              cache_entry = ret.top().entry_ptr;//获取上一级的entry  找一个这个buffer在上一级是个啥？ 
              if(cache_entry == 0) return false;
              parent_parent_type = cache_entry->node_type;
              cache_entry_parent = cache_entry;
              uint8_t partial = k.at(ret.top().next_idx);
              for (int i = 0; i < (int)cache_entry->records.size(); ++ i) {  //一个个查看slot
                const auto& e = cache_entry->records[i];
                if (e != BufferEntry::Null()&&e != InternalEntry::Null() && e.partial == partial) { 
                entry_idx = i;   //返回这个buffer在父节点的下标
                return true;
                }
              }
              }
              return true;
            }
          }
        }

      }
      ret.pop();
    }
  }

  return false;
}

bool RadixCache::_search(const CacheKey& byte_array, SearchRetStk& ret) {  //找到缓冲节点的时候判断一下是不是叶节点 是叶节点就停止啦啦啦
  CacheNode* node = cache_root;
  int idx = 0;  //和depth作用一样
  CacheEntry* parent = nullptr;
  bool parent_type = false;  // 是否是缓冲节点

next:
  if (idx >= (int)byte_array.size()) {  // exit
    return !ret.empty();
  }

  // 1. parse header
  auto hdr = (CacheHeader *)node->header;
  for (int i = 0; i < (int)hdr->partial.size(); ++ i) {
    if (hdr->depth + i == (int)byte_array.size() - 1 || byte_array[hdr->depth + i] != hdr->partial[i]) {
      return !ret.empty();
    }
  }
  idx = hdr->depth + hdr->partial.size();

  // 2. parse_node
  auto& cache_map = node->records;
  auto partial = byte_array[idx];   //共同前缀的后一个字节 
  if(parent && parent_type) //上一个entry是缓冲节点  看一下再上一层的slot中是不是叶节点 是叶节点直接返回？
  {
    for(int i =0;i<(int)parent->records.size();i++)
    {
      if(parent->records[i].partial == partial && parent->records[i].node_type == 0) return !ret.empty();
    }
  }

  CacheMap::const_iterator r_entry = cache_map.find(partial);  //直接map过去的
  if (r_entry != cache_map.end()) {
    auto cache_entry = (CacheEntry *)r_entry->second.cache_entry;
    if(cache_entry == 0) return !ret.empty();
    parent_type = cache_entry->node_type;
    // ret.push(std::make_pair(std::make_pair(&(r_entry->second.cache_entry), cache_entry), idx + 1));
    ret.push(SearchRet(&(r_entry->second.cache_entry), cache_entry, idx + 1));//存下来的是CacheEntry  相当于存下来了一整个内部节点或者缓冲节点 idx存的是
    parent=cache_entry;
    node = (CacheNode *)(r_entry->second.next);  //看下一层还能不能继续往下 应该是在插入函数修改的  next应该指向的是和该内部节点所指向的所有内部节点
    if (node) {
      idx ++;
      goto next;
    }
  }
  return !ret.empty();
}


void RadixCache::search_range_from_cache(const Key &from, const Key &to, std::vector<RangeCache> &result) {
/*  GlobalAddress p_ptr;
  InternalEntry p;
  int depth;
  volatile CacheEntry** entry_ptr_ptr = nullptr;
  CacheEntry* entry_ptr = nullptr;
  int entry_idx = -1;

  for (auto k = from; k < to; k = k + 1) {
    auto e = search_from_cache(k, entry_ptr_ptr, entry_ptr, entry_idx);
    if (e) {
      assert(entry_idx >= 0);
      p_ptr = GADD(entry_ptr->addr, sizeof(InternalEntry) * entry_idx);
      p = entry_ptr->records[entry_idx];
      depth = entry_ptr->depth;

      auto leftmost = p.is_leaf ? k : get_leftmost(k, depth);
      auto rightmost = p.is_leaf ? k : get_rightmost(k, depth);
      result.push_back(RangeCache(leftmost, rightmost, p_ptr, p, depth, entry_ptr_ptr, entry_ptr));
    }
  }
  */
  return;
}

void RadixCache::invalidate(CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr) {
      
  if (entry_ptr_ptr && entry_ptr && __sync_bool_compare_and_swap(entry_ptr_ptr, entry_ptr, 0UL)) {
    free_manager->free(entry_ptr->content_size());
    _safely_delete(entry_ptr);
  }

}

void RadixCache::_evict() {
  bool flag;
  do {
    // _evict_one();
    std::pair<CacheEntry**, CacheEntry*> next;
    if(eviction_list.try_pop(next) && *next.first == next.second) {
      invalidate(next.first, next.second);
    }
    flag = eviction_list.empty();
  } while (free_manager->remain_size() < 0 && !flag);
  if (flag) {
    // rebuild cache  TODO: memory leak
    if (__sync_bool_compare_and_swap(&cache_root, cache_root, new CacheNode())) {
      free_manager = new FreeMemManager(define::MB * cache_size);
      node_queue = new tbb::concurrent_queue<CacheNode*>();
      node_queue->push(cache_root);
    }
  }
}

// void RadixCache::_evict_one() {
//   SearchRetStk stk1, stk2;
//   Key k1, k2;
//   do {
//     k1 = dsm->getRandomKey();
//   } while(!_search(CacheKey(k1.begin(), k1.begin() + define::keyLen - 1), stk1));
//   do {
//     k2 = dsm->getRandomKey();
//   } while(!_search(CacheKey(k2.begin(), k2.begin() + define::keyLen - 1), stk2));

//   // while(!_random_search(stk1));
//   // while(!_random_search(stk2));
//   // evict
//   uint64_t min_cnt = UINT_MAX;
//   CacheEntry * min_entry = nullptr;
//   volatile CacheEntry** min_entry_ptr = nullptr;

//   auto select_smallest = [&](SearchRetStk& stk){
//     while(!stk.empty()) {
//       const auto& item = stk.top();
//       auto cache_entry = item.entry_ptr;
//       if (!cache_entry) {
//         stk.pop();
//         continue;
//       }
//       if (item.counter <= min_cnt) {
//         min_cnt = item.counter;
//         min_entry = cache_entry;
//         min_entry_ptr = item.entry_ptr_ptr;
//       }
//       stk.pop();
//     }
//   };
//   select_smallest(stk1);
//   select_smallest(stk2);
//   invalidate(min_entry_ptr, min_entry);
// }

// bool RadixCache::_random_search(SearchRetStk& ret) {
//   static std::default_random_engine e;
//   static std::uniform_int_distribution<uint8_t> u(0, 255);

//   CacheNode* node = cache_root;
//   int idx = 0;

// next:
//   if (idx >= (int)define::keyLen - 1) {  // exit
//     return !ret.empty();
//   }

//   // 1. ignore header
//   auto hdr = (CacheHeader *)node->header;
//   idx = hdr->depth + hdr->partial.size();

//   // 2. parse_node
//   auto& cache_map = node->records;

//   CacheMap::const_iterator r_entry;
//   int s = cache_map.size();
//   if (s == 0) {
//     return !ret.empty();
//   }
//   else if (s < 16) {
//     int i = u(e) % s;
//     r_entry = cache_map.begin();
//     while(i --) ++ r_entry;
//   }
//   else {
// retry:
//     r_entry = cache_map.find(u(e));
//     if (r_entry == cache_map.end()) goto retry;
//   }

//   if (r_entry != cache_map.end()) {
//     auto cache_entry = (CacheEntry *)r_entry->second.cache_entry;
//     if (cache_entry) {
//       ret.push(SearchRet(&(r_entry->second.cache_entry), cache_entry, idx + 1, cache_entry->counter));
//     }
//     node = (CacheNode *)(r_entry->second.next);
//     if (node) {
//       idx ++;
//       goto next;
//     }
//   }
//   return !ret.empty();
// }

void RadixCache::_safely_delete(CacheEntry* cache_entry) {
  cache_entry_gc.push(cache_entry);
  while (cache_entry_gc.unsafe_size() > safely_free_epoch) {
    CacheEntry* next = nullptr;
    if (cache_entry_gc.try_pop(next) && next) {
      delete next;
    }
  }
}

void RadixCache::_safely_delete(CacheHeader* cache_hdr) {
  cache_hdr_gc.push(cache_hdr);
  while (cache_hdr_gc.unsafe_size() > safely_free_epoch) {
    CacheHeader* next = nullptr;
    if (cache_hdr_gc.try_pop(next) && next) {
      delete next;
    }
  }
}

void RadixCache::statistics() {
  std::cout << " ----- [IndexCache]: " << " cache size=" << cache_size << " MB"
                                       << " free_size=" << free_manager->remain_size() / define::MB << " MB" 
                                       << " node_cnt=" << node_queue->unsafe_size() << " ----- " << std::endl;
  std::map<int, int64_t> cnt;
  uint64_t kp_cnt = 0;
  for (auto node_iter = node_queue->unsafe_begin(); node_iter != node_queue->unsafe_end(); ++ node_iter) {
    auto& node = *node_iter;
    auto& cache_map = node->records;
    // auto header = (CacheHeader *)node->header;
    for (auto entry_iter = cache_map.begin(); entry_iter != cache_map.end(); ++ entry_iter) {
      auto cache_entry = (CacheEntry *)entry_iter->second.cache_entry;
      if (cache_entry) {
        int depth = cache_entry->depth;
        if (cnt.find(depth) == cnt.end()) cnt[depth] = 0;
        cnt[depth] ++;
        kp_cnt += cache_entry->records.size();
      }
    }
  }
  for (const auto& e : cnt) {
    std::cout << "depth=" << e.first << " cnt=" << e.second << std::endl;
  }
  printf("consumed cache size = %.3lf MB\n", (double)cache_size - (double)free_manager->remain_size() / define::MB);
}
