#if !defined(_TREE_H_)
#define _TREE_H_

#include "RadixCache.h"
#include "DSM.h"
#include "Common.h"
#include "LocalLockTable.h"

#include <atomic>
#include <city.h>
#include <functional>
#include <map>
#include <algorithm>
#include <queue>
#include <set>
#include <iostream>


/*
  Workloads
*/
struct Request {
  bool is_search;
  bool is_insert;
  bool is_update;
  Key k;
  Value v;
  int range_size;
};


class RequstGen {
public:
  RequstGen() = default;
  virtual Request next() { return Request{}; }
};


/*
  Tree
*/
using GenFunc = std::function<RequstGen *(DSM*, Request*, int, int, int)>;
#define MAX_FLAG_NUM 15
enum {
  FIRST_TRY,//尝试第一次
  CAS_Internal_NULL,//cas 内部节点 空槽失败
  INVALID_LEAF, //叶节点失效
  INVALID_Internal_NODE, //内部结点失效
  INVALID_Buffer_NODE, //缓冲结点失效
  SPLIT_Internal_HEADER,  //内部节点头部分裂失效
  SPLIT_Buffer_HEADER,  //缓冲节点头部分裂失效
  FIND_NEXT,
  CAS_NULL,
  CAS_Internal_EMPTY,   
  CAS_Buffer_EMPTY,
  INSERT_BEHIND_EMPTY,  //内部节点扩展
  INSERT_BEHIND_TRY_NEXT, //内部节点扩展找下一个
  SWITCH_RETRY,  
  SWITCH_FIND_TARGET,
  Buffer_Switch_type,
};

class Tree {
public:
  Tree(DSM *dsm, uint16_t tree_id = 0);

  using WorkFunc = std::function<void (Tree *, const Request&, CoroContext *, int)>;
  void run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req = nullptr, int req_num = 0);

  void insert(const Key &k, Value v, CoroContext *cxt = nullptr, int coro_id = 0, bool is_update = false, bool is_load = false);
  bool search(const Key &k, Value &v, CoroContext *cxt = nullptr, int coro_id = 0);
  void range_query(const Key &from, const Key &to, std::map<Key, Value> &ret);
  void statistics();
  void clear_debug_info();

  GlobalAddress get_root_ptr_ptr();
  InternalEntry get_root_ptr(CoroContext *cxt, int coro_id);

private:
  void coro_worker(CoroYield &yield, RequstGen *gen, WorkFunc work_func, int coro_id);
  void coro_master(CoroYield &yield, int coro_cnt);

  bool read_leaf(GlobalAddress &leaf_addr, char *leaf_buffer, int leaf_size, const GlobalAddress &p_ptr, bool from_cache, CoroContext *cxt, int coro_id);
  bool read_leaves(GlobalAddress* leaf_addr, char *leaf_buffer,int leaf_cnt, GlobalAddress* p_ptr, bool from_cache,CoroContext *cxt, int coro_id);
  bool out_of_place_write_buffer_n_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr,int leaf_type,int klen,int vlen,const GlobalAddress &p_ptr, const InternalEntry &p,
                             const GlobalAddress& node_addr, uint64_t *ret_buffer,CoroContext *cxt, int coro_id);
  void in_place_update_leaf(const Key &k, Value &v, const GlobalAddress &leaf_addr, int leaf_type,Leaf_kv* leaf,
                               CoroContext *cxt, int coro_id);
  bool out_of_place_update_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, const GlobalAddress &e_ptr, InternalEntry &old_e, const GlobalAddress& node_addr,
                                CoroContext *cxt, int coro_id, bool disable_handover = false);
  bool out_of_place_write_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, int leaf_type ,int klen,int vlen,
                                    const GlobalAddress &e_ptr, const BufferEntry &old_e, uint64_t *ret_buffer,
                                    CoroContext *cxt, int coro_id);

  bool read_node(InternalEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                 CoroContext *cxt, int coro_id);
  bool read_buffer_node(GlobalAddress node_addr, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,   //只需要判断反向指针对不对就可以了 （有没有分裂）
                     CoroContext *cxt, int coro_id);
  bool read_node_from_buffer(BufferEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                     CoroContext *cxt, int coro_id);
  bool out_of_place_write_node(const Key &k, Value &v,const int depth, GlobalAddress& leaf_addr, int leaf_type,int klen,int vlen,int partial_len,uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id);
  bool out_of_place_write_node_from_buffer(const Key &k, Value &v,const int depth, GlobalAddress& leaf_addr, int leaf_type,int klen,int vlen,int partial_len,uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const BufferEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id);
  bool out_of_place_write_buffer_node(const Key &k, Value &v, int depth,InternalBuffer* bnode,int leaf_type,int klen,int vlen,GlobalAddress leaf_addr,CacheEntry**&entry_ptr_ptr,CacheEntry*& entry_ptr,bool from_cache,InternalEntry old_e,GlobalAddress p_ptr,
                                   CoroContext *cxt, int coro_id);
  bool out_of_place_write_buffer_node_from_buffer(const Key &k, Value &v, int depth,InternalBuffer* bnode,int leaf_type,int klen,int vlen,GlobalAddress leaf_addr,CacheEntry**&entry_ptr_ptr,CacheEntry*& entry_ptr,bool from_cache,BufferEntry old_e,GlobalAddress p_ptr,
                                   CoroContext *cxt, int coro_id);
                                   /*
  bool out_of_place_write_node_from_buffer(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, int leaf_type,int klen,int vlen,int partial_len, uint8_t partial,uint8_t diff_partial,
                                   const GlobalAddress &e_ptr, const BufferEntry &old_e, const GlobalAddress& node_addr,
                                   uint64_t *ret_buffer, CoroContext *cxt, int coro_id);*/
  bool insert_behind(const Key &k, Value &v, GlobalAddress addr,int depth, GlobalAddress& leaf_addr, uint8_t partial_key, NodeType node_type,int leaf_type,int klen,int vlen,
                         const GlobalAddress &node_addr, uint64_t *ret_buffer, int& inserted_idx,
                         CoroContext *cxt, int coro_id);
  void search_entries(const Key &from, const Key &to, int target_depth, std::vector<ScanContext> &res,
                      CoroContext *cxt, int coro_id);
  void cas_node_type(NodeType next_type, GlobalAddress p_ptr, InternalEntry p, Header hdr,
                     CoroContext *cxt, int coro_id);
  void cas_node_type_from_buffer(NodeType next_type, GlobalAddress p_ptr, BufferEntry p, Header hdr,
                         CoroContext *cxt, int coro_id);
  void range_query_on_page(InternalPage* page, bool from_cache, int depth,
                           GlobalAddress p_ptr, InternalEntry p,
                           const Key &from, const Key &to, State l_state, State r_state,
                           std::vector<ScanContext>& res);
  void get_on_chip_lock_addr(const GlobalAddress &leaf_addr, GlobalAddress &lock_addr, uint64_t &mask);
#ifdef TREE_TEST_ROWEX_ART
  void lock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id);
  void unlock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id);
#endif

private:
  DSM *dsm;
// #ifdef CACHE_ENABLE_ART
  RadixCache *index_cache;
// #else
//   NormalCache *index_cache;
// #endif
  LocalLockTable *local_lock_table;

  static thread_local CoroCall worker[MAX_CORO_NUM];
  static thread_local CoroCall master;
  static thread_local CoroQueue busy_waiting_queue;

  uint64_t tree_id;
  GlobalAddress root_ptr_ptr; // the address which stores root pointer;
};


#endif // _TREE_H_
