#if !defined(_RDMA_BUFFER_H_)
#define _RDMA_BUFFER_H_

#include "Common.h"

// abstract rdma registered buffer
class RdmaBuffer {

private:
  // async, buffer safty
  static const int kCasBufferCnt    = 256;
  static const int kPageBufferCnt   = 256;  // big enough to hold batch internal node write in out_of_place_write_node
  static const int kBufferBufferCnt = 1024;
  static const int kKVLeafBufferCnt   = 256;    //分配32 *最大的叶节点
  static const int kPTRLeafBufferCnt   = 32;    //分配32 *最大的叶节点
  static const int kHeaderBufferCnt = 32;
  static const int kEntryBufferCnt  = 32;
  static const int kBufferEntryBufferCnt  = 32;


  char *buffer;

  uint64_t *cas_buffer;
  char *page_buffer;
  char *buffer_buffer;
  char *kvleaf_buffer;
  char *ptrleaf_buffer;
  uint64_t * header_buffer;
  uint64_t * entry_buffer;
  uint64_t * buffer_entry_buffer;
  char   *range_buffer;
  char   *zero_byte;
  char   *key_buffer;
  char   *value_buffer;
  

  int cas_buffer_cur;
  int page_buffer_cur;
  int buffer_buffer_cur;
  int kvleaf_buffer_cur;
  int ptrleaf_buffer_cur;
  int header_buffer_cur;
  int entry_buffer_cur;
  int buffer_entry_buffer_cur;
  int key_buffer_cur;
  int value_buffer_cur;

public:
  RdmaBuffer(char *buffer) {
    set_buffer(buffer);

    cas_buffer_cur    = 0;
    page_buffer_cur   = 0;
    buffer_buffer_cur = 0;
    kvleaf_buffer_cur   = 0;
    ptrleaf_buffer_cur   = 0;
    header_buffer_cur = 0;
    entry_buffer_cur  = 0;
    buffer_entry_buffer_cur =0;
    key_buffer_cur    =0;
    value_buffer_cur  =0;
  }

  RdmaBuffer() = default;

  void set_buffer(char *buffer) {
    // printf("set buffer %p\n", buffer);
    this->buffer        = buffer;
    cas_buffer          = (uint64_t *)buffer;
    page_buffer         = (char     *)((char *)cas_buffer    + sizeof(uint64_t)   * kCasBufferCnt);
    buffer_buffer       = (char     *)((char *)page_buffer   + define::allocationPageSize * kPageBufferCnt);
    kvleaf_buffer         = (char     *)((char *)buffer_buffer   + define::allocationBufferSize * kBufferBufferCnt);
    ptrleaf_buffer         = (char     *)((char *)kvleaf_buffer   + define::allocAlignKVLeafSize * kKVLeafBufferCnt);    
    header_buffer       = (uint64_t *)((char *)ptrleaf_buffer   + define::allocAlignPTRLeafSize * kPTRLeafBufferCnt);   //待解决、、、、
    entry_buffer        = (uint64_t *)((char *)header_buffer + sizeof(uint64_t)   * kHeaderBufferCnt);
    buffer_entry_buffer = (uint64_t *)((char *)entry_buffer  + sizeof(uint64_t)   * kEntryBufferCnt);
    zero_byte           = (char     *)((char *)buffer_entry_buffer  + sizeof(uint64_t)   * kBufferEntryBufferCnt);
    range_buffer        = (char     *)((char *)zero_byte     + sizeof(char));
    key_buffer          = (char     *)((char *)range_buffer  + define::keybuffer);
    value_buffer        = (char     *)((char *)key_buffer    + define::valuebuffer);
    *zero_byte          = '\0';

    assert(range_buffer - buffer < define::kPerCoroRdmaBuf);
  }

  uint64_t *get_cas_buffer() {
    cas_buffer_cur = (cas_buffer_cur + 1) % kCasBufferCnt;
    return cas_buffer + cas_buffer_cur;
  }

  char *get_page_buffer() {
    page_buffer_cur = (page_buffer_cur + 1) % kPageBufferCnt;
    return page_buffer + page_buffer_cur * define::allocationPageSize;
  }
  char *get_buffer_buffer() {
    buffer_buffer_cur = (buffer_buffer_cur + 1) % kPageBufferCnt;
    return buffer_buffer + buffer_buffer_cur * define::allocationBufferSize;
  }

  char *get_kvleaf_buffer( ) {
    kvleaf_buffer_cur = (kvleaf_buffer_cur + 1)  % ( kKVLeafBufferCnt * define::allocAlignKVLeafSize);
    return kvleaf_buffer + kvleaf_buffer_cur * define::allocAlignKVLeafSize;
  }
  char *get_kvleaves_buffer(int k ) {
    kvleaf_buffer_cur = (kvleaf_buffer_cur + k)  % ( kKVLeafBufferCnt * define::allocAlignKVLeafSize);
    return kvleaf_buffer + kvleaf_buffer_cur * define::allocAlignKVLeafSize ;
  }
  char *get_ptrleaf_buffer( ) {
    ptrleaf_buffer_cur = (ptrleaf_buffer_cur + 1)  % ( kPTRLeafBufferCnt * define::allocAlignPTRLeafSize);
    return ptrleaf_buffer + ptrleaf_buffer_cur ;
  }

  uint64_t *get_header_buffer() {
    header_buffer_cur = (header_buffer_cur + 1) % kHeaderBufferCnt;
    return header_buffer + header_buffer_cur;
  }
  uint64_t *get_buffer_entry_buffer() {
    buffer_entry_buffer_cur = (buffer_entry_buffer_cur + 1) % kBufferEntryBufferCnt;
    return buffer_entry_buffer + buffer_entry_buffer_cur;
  }

  uint64_t *get_entry_buffer() {
    entry_buffer_cur = (entry_buffer_cur + 1) % kEntryBufferCnt;
    return entry_buffer + entry_buffer_cur;
  }

  char *get_range_buffer() {
    return range_buffer;
  }

  char *get_zero_byte() {
    return zero_byte;
  }
  char *get_key_buffer(int size) {
    key_buffer_cur = (key_buffer_cur + size) % define::keybuffer;
    return key_buffer + key_buffer_cur;
  }

  char *get_value_buffer(int size) {
    value_buffer_cur = (value_buffer_cur + size) % define::valuebuffer;
    return value_buffer + value_buffer_cur;
  }
};

#endif // _RDMA_BUFFER_H_
