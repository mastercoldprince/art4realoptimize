#if !defined(_NODE_H_)
#define _NODE_H_

#include "Common.h"
#include "GlobalAddress.h"
#include "Key.h"



struct PackedGAddr {  // 48-bit, used by node addr/leaf addr (not entry addr)
  uint64_t mn_id     : define::mnIdBit;
  uint64_t offset    : define::offsetBit;

  operator uint64_t() { return (offset << define::mnIdBit) | mn_id; }
} __attribute__((packed));


static_assert(sizeof(PackedGAddr) == 6);

static CRCProcessor crc_processor;

/*
  Leaf Node
*/
class Leaf_kv {
public:
  // for invalidation
  GlobalAddress rev_ptr;
  // TODO: add key len & value len for out-of-place updates

  union {
  struct {
  uint8_t f_padding   : 2;
  uint8_t leaf_type   : 4;
  uint8_t valid       : 1;
  };
  uint8_t type_valid_byte;
  };

  uint64_t checksum;  // checksum(kv)

  Key key;
  union {
  Value value;
  uint8_t _padding[define::simulatedValLen];
  };

  union {
  struct {
    uint8_t w_lock    : 1;
    uint8_t r_padding : 7;
  };
  uint8_t lock_byte;
  };

public:
  Leaf() {}
  Leaf(const Key& key, const Value& value, const GlobalAddress& rev_ptr) : rev_ptr(rev_ptr), f_padding(0), valid(1), key(key), value(value), lock_byte(0) { set_consistent(); }

  const Key& get_key() const { return key; }
  Value get_value() const { return value; }
  bool is_valid(const GlobalAddress& p_ptr, bool from_cache) const { return valid && (!from_cache || p_ptr == rev_ptr); }
  bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key) + sizeof(uint8_t) * define::simulatedValLen);
    return crc_processor.checksum() == checksum;
  }

  void set_value(const Value& val) { value = val; }

  void unlock() { w_lock = 0; };
  void lock() { w_lock = 1; };

  static uint8_t get_partial(const Key& key, int depth);
  static Key get_leftmost(const Key& key, int depth);
  static Key get_rightmost(const Key& key, int depth);
  static Key remake_prefix(const Key& key, int depth, uint8_t diff_partial);
  static int longest_common_prefix(const Key &k1, const Key &k2, int depth);
};
//结构？？？ ????????????????????????????????????????????????????????????????????????????????????????????????????????????????wtffffffffffffffffffffffffffffffffffffffffffff
class Leaf_ptr {
public:
  // for invalidation
  GlobalAddress rev_ptr;
  // TODO: add key len & value len for out-of-place updates
  union {
  struct {
  uint8_t f_padding   : 2;
  uint8_t leaf_type   : 4;
  uint8_t valid       : 1;
  };
  uint8_t type_valid_byte;
  };

  uint16_t keylen;
  uint16_t valen;
  GlobalAddress k_ptr;
  GlobalAddress v_ptr;

public:
  Leaf_ptr() {}
  Leaf_ptr(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const GlobalAddress& k_ptr,const GlobalAddress& v_ptr) : rev_ptr(rev_ptr), f_padding(0), valid(1), k_ptr(k_ptr), v_ptr(v_ptr){}// { set_consistent(); }


  bool is_valid(const GlobalAddress& p_ptr) { return valid && ( p_ptr == rev_ptr); }

  static uint8_t get_partial(const Key& key, int depth);
  static Key get_leftmost(const Key& key, int depth);
  static Key get_rightmost(const Key& key, int depth);
  static Key remake_prefix(const Key& key, int depth, uint8_t diff_partial);
  static int longest_common_prefix(const Key &k1, const Key &k2, int depth);
};
/*存储64B*/


/*
class leaf_1:public Leaf 
{
public:
  // kv
  Key_1 key;
  Value_1 value;
  leaf_1(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_1 key,const Value_1 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_1& get_key() const { return key;};
  Value_1 get_value() const { return value;};
  void set_value(const Value_1& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_1));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_1));
    checksum = crc_processor.checksum();
  }


}__attribute__((packed));
*/
/*存储128B*/
/*
class leaf_2:public Leaf 
{
  public:
  // kv
  Key_1 key;
  Value_2 value;

  leaf_2(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_1 key,const Value_2 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_1& get_key() const { return key;};
  Value_2 get_value() const { return value;};
  void set_value(const Value_2& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_2));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_2));
    checksum = crc_processor.checksum();
  }
}__attribute__((packed));
*/
/*存储256B  key   */
/*
class leaf_3:public Leaf 
{
  public:
  // kv
  Key_1 key;
  Value_3 value;

  leaf_3(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_1 key,const Value_3 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_1& get_key() const { return key;};
  Value_3 get_value() const { return value;};
  void set_value(const Value_3& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_3));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_3));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
*/
/*存储512B*/
/*
class leaf_4:public Leaf 
{
  public:
  // kv
  Key_1 key;
  Value_4 value;

  leaf_4(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_1 key,const Value_4 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_1& get_key() const { return key;};
  Value_4 get_value() const { return value;};
  void set_value(const Value_4& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_4));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_1) + sizeof(uint8_t) * sizeof(Value_4));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
*/
/*存储1024B*/
/*
class leaf_5:public Leaf 
{
  public:
  // kv
  Key_2 key;
  Value_1 value;

  leaf_5(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_2 key,const Value_1 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_2& get_key() const { return key;};
  Value_1 get_value() const { return value;};
  void set_value(const Value_1& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_1));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_1));
    checksum = crc_processor.checksum();
  }
}__attribute__((packed));
*/
/*
class leaf_6:public Leaf 
{
  public:
  // kv
  Key_2 key;
  Value_2 value;

  leaf_6(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_2 key,const Value_2 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_2& get_key() const { return key;};
  Value_2 get_value() const { return value;};
  void set_value(const Value_2& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_2));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_2));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
*/
/*
class leaf_7:public Leaf 
{
  public:
  // kv
  Key_2 key;
  Value_3 value;

  leaf_7(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_2 key,const Value_3 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_2& get_key() const { return key;};
  Value_3 get_value() const { return value;};
  void set_value(const Value_3& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_3));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_3));
    checksum = crc_processor.checksum();
  }
}__attribute__((packed));
*/
/*
class leaf_8:public Leaf 
{
  public:
  // kv
  Key_2 key;
  Value_4 value;

  leaf_8(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_2 key,const Value_4 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_2& get_key() const { return key;};
  Value_4 get_value() const { return value;};
  void set_value(const Value_4& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_4));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_2) + sizeof(uint8_t) * sizeof(Value_4));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
*/
/*
class leaf_9:public Leaf 
{
  public:
  // kv
  Key_3 key;
  Value_1 value;

  leaf_9(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_3 key,const Value_1 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_3& get_key() const { return key;};
  Value_1 get_value() const { return value;};
  void set_value(const Value_1& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_1));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_1));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
*/
/*
class leaf_10:public Leaf 
{
  public:
  // kv
  Key_3 key;
  Value_2 value;

  leaf_10(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_3 key,const Value_2 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_3& get_key() const { return key;};
  Value_2 get_value() const { return value;};
  void set_value(const Value_2& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_2));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_2));
    checksum = crc_processor.checksum();
  }
}__attribute__((packed));
class leaf_11:public Leaf 
{
  public:
  // kv
  Key_3 key;
  Value_3 value;

  leaf_11(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_3 key,const Value_3 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_3& get_key() const { return key;};
  Value_3 get_value() const { return value;};
  void set_value(const Value_3& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_3));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_3));
    checksum = crc_processor.checksum();
  }
}__attribute__((packed));
class leaf_12:public Leaf 
{
  public:
  // kv
  Key_3 key;
  Value_4 value;

  leaf_12(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_3 key,const Value_4 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_3& get_key() const { return key;};
  Value_4 get_value() const { return value;};
  void set_value(const Value_4& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_4));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_3) + sizeof(uint8_t) * sizeof(Value_4));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
class leaf_13:public Leaf 
{
  public:
  // kv
  Key_4 key;
  Value_1 value;

  leaf_13(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_4 key,const Value_1 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_4& get_key() const { return key;};
  Value_1 get_value() const { return value;};
  void set_value(const Value_1& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_1));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_1));
    checksum = crc_processor.checksum();
  }
}__attribute__((packed));
class leaf_14:public Leaf 
{
  public:
  // kv
  Key_4 key;
  Value_2 value;

  leaf_14(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_4 key,const Value_2 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_4& get_key() const { return key;};
  Value_2 get_value() const { return value;};
  void set_value(const Value_2& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_2));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_2));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
class leaf_15:public Leaf 
{
  public:
  // kv
  Key_4 key;
  Value_3 value;

  leaf_15(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_4 key,const Value_3 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_4& get_key() const { return key;};
  Value_3 get_value() const { return value;};
  void set_value(const Value_3& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_3));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_3));
    checksum = crc_processor.checksum();
  }
}__attribute__((packed));
class leaf_16:public Leaf 
{
  public:
  // kv
  Key_4 key;
  Value_4 value;

  leaf_16(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_4 key,const Value_4 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_4& get_key() const { return key;};
  Value_4 get_value() const { return value;};
  void set_value(const Value_4& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_4));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_4) + sizeof(uint8_t) * sizeof(Value_4));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
class leaf_17:public Leaf 
{
  public:
  // kv
  Key_5 key;
  Value_1 value;

  leaf_17(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_5 key,const Value_1 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_5& get_key() const { return key;};
  Value_1 get_value() const { return value;};
  void set_value(const Value_1& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_1));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_1));
    checksum = crc_processor.checksum();
  }


}__attribute__((packed));
class leaf_18:public Leaf 
{
  public:
  // kv
  Key_5 key;
  Value_2 value;

  leaf_18(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_5 key,const Value_2 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_5& get_key() const { return key;};
  Value_2 get_value() const { return value;};
  void set_value(const Value_2& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_2));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_2));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
class leaf_19:public Leaf 
{
  public:
  // kv
  Key_5 key;
  Value_3 value;

  leaf_19(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_5 key,const Value_3 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_5& get_key() const { return key;};
  Value_3 get_value() const { return value;};
  void set_value(const Value_3& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_3));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_3));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
class leaf_20:public Leaf 
{
  public:
  // kv
  Key_5 key;
  Value_4 value;

  leaf_17(const GlobalAddress& rev_ptr,const int& keylen,const int& valen,const Key_5 key,const Value_4 value):Leaf(rev_ptr,keylen,valen),key(key),value(value) 
  {set_consistent(); }
  const Key_5& get_key() const { return key;};
  Value_4 get_value() const { return value;};
  void set_value(const Value_4& val) { value = val; }

  virtual bool is_consistent() const {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_4));
    return crc_processor.checksum() == checksum;
  }


  virtual void set_consistent()
   {
    crc_processor.reset();
    crc_processor.process_bytes((char *)&key, sizeof(Key_5) + sizeof(uint8_t) * sizeof(Value_4));
    checksum = crc_processor.checksum();
  }

}__attribute__((packed));
*/
/*
  Header
*/
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
#define MAX_NODE_TYPE_NUM 8
enum NodeType : uint8_t {
  NODE_DELETED,
  NODE_4,
  NODE_8,
  NODE_16,
  NODE_32,
  NODE_64,
  NODE_128,
  NODE_256
};
#else
#define MAX_NODE_TYPE_NUM 5
enum NodeType : uint8_t {
  NODE_DELETED,
  NODE_4,
  NODE_16,
  NODE_48,
  NODE_256
};
#endif


inline int node_type_to_num(NodeType type) {
  if (type == NODE_DELETED) return 0;
#ifndef TREE_ENABLE_ART
  type = NODE_256;
#endif
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
  return 1 << (static_cast<int>(type) + 1);
#else
  switch (type) {
    case NODE_4  : return 4;
    case NODE_16 : return 16;
    case NODE_48 : return 48;
    case NODE_256: return 256;
    default:  assert(false);
  }
#endif
}


inline NodeType num_to_node_type(int num) {
  if (num == 0) return NODE_DELETED;
#ifndef TREE_ENABLE_ART
  return NODE_256;
#endif
#ifdef TREE_ENABLE_FINE_GRAIN_NODE
  for (int i = 1; i < MAX_NODE_TYPE_NUM; ++ i) {
    if (num < (1 << (i + 1))) return static_cast<NodeType>(i);
  }
#else
  if (num < 4) return NODE_4;
  if (num < 16) return NODE_16;
  if (num < 48) return NODE_48;
  if (num < 256) return NODE_256;
#endif
  assert(false);
}
class BufferHeader {
public:
  union {
  struct {
    uint8_t depth;  //8bit
    uint8_t partial_len  : define::partial_len; //2bit
    uint8_t bn_padding1 : 6;  
    uint8_t partial[define::bPartialLenMax]; //16bit
    uint8_t count_1  ; // 8bit
    uint8_t count_2  ; //8bit
    uint16_t bn_padding2;  
  };

  uint64_t val;
  };

public:
  BufferHeader() : depth(0),partial_len(0) ,count_1(0),count_2(0){ memset(partial, 0, sizeof(uint8_t) * define::bPartialLenMax); }
  BufferHeader(int depth) : depth(depth),partial_len(0) ,count_1(0),count_2(0){ memset(partial, 0, sizeof(uint8_t) * define::bPartialLenMax); }
  BufferHeader(const Key &k, int partial_len, int depth, int count_1,int count_2) : depth(depth),partial_len(partial_len),count_1(count_1),count_2(count_2) {
    assert((uint32_t)partial_len <= define::bPartialLenMax);
    for (int i = 0; i < partial_len; ++ i) partial[i] = get_partial(k, depth + i);
  }

  static BufferHeader split_header(const BufferHeader& old_hdr, int diff_idx) {
  auto new_hdr = BufferHeader();
  for (int i = diff_idx + 1; i < old_hdr.partial_len; ++ i) new_hdr.partial[i - diff_idx - 1] = old_hdr.partial[i];
  new_hdr.partial_len = old_hdr.partial_len - diff_idx - 1;
  new_hdr.depth = old_hdr.depth + diff_idx + 1;
  new_hdr.count_1 = old_hdr.count_1;
  new_hdr.count_2 = old_hdr.count_2;
  return new_hdr;
}

  operator uint64_t() { return val; }

  bool is_match(const Key& k) {
    for (int i = 0; i < partial_len; ++ i) {
      if (get_partial(k, depth + i) != partial[i]) return false;
    }
    return true;
  }

  static const uint64_t count_1_mask = (((1UL << define::count_1) - 1) << define::count_2);
  static const uint64_t count_2_mask = (((1UL << define::count_2) - 1));
} __attribute__((packed));


static_assert(sizeof(BufferHeader) == 8);
static_assert(1UL << (define::partial_len) >= define::bPartialLenMax);

class Header {
public:
  union {
  struct {
    uint8_t depth : 7;
    uint8_t node_type   : define::nodeTypeNumBit;
    uint8_t partial_len : 8 - define::nodeTypeNumBit;
    uint8_t partial[define::hPartialLenMax];
  };

  uint64_t val;
  };

public:
  Header() : depth(0), node_type(0), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(int depth) : depth(depth), node_type(0), partial_len(0) { memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax); }
  Header(NodeType node_type,Header hdr) : depth(hdr.depth), node_type(node_type), partial_len(hdr.partial_len)
   { //memset(partial, 0, sizeof(uint8_t) * define::hPartialLenMax);
     for(int i =0 ;i<partial_len;i++)
     partial[i] = hdr.partial[i]; 
   }
  Header(const Key &k, int partial_len, int depth, NodeType node_type) : depth(depth), node_type(node_type), partial_len(partial_len) {
    assert((uint32_t)partial_len <= define::hPartialLenMax);
    for (int i = 0; i < partial_len; ++ i) partial[i] = get_partial(k, depth + i);
  }
  Header(char* partial, int partial_len, int depth, NodeType node_type) : depth(depth), node_type(node_type), partial_len(partial_len) {
    assert((uint32_t)partial_len <= define::hPartialLenMax);
    for (int i = 0; i < partial_len; ++ i) this->partial[i] = partial[i];
  }
  Header(BufferHeader bhdr) : depth(bhdr.depth),node_type(node_type_to_num(NODE_256)),partial_len(bhdr.partial_len)
  {
    for(int i =0;i<partial_len;i++) partial[i] = bhdr.partial[i];
  }

  operator uint64_t() { return val; }

  bool is_match(const Key& k) {
    for (int i = 0; i < partial_len; ++ i) {
      if (get_partial(k, depth + i) != partial[i]) return false;
    }
    return true;
  }

  static Header split_header(const Header& old_hdr, int diff_idx) {
    auto new_hdr = Header();
    for (int i = diff_idx + 1; i < old_hdr.partial_len; ++ i) new_hdr.partial[i - diff_idx - 1] = old_hdr.partial[i];
    new_hdr.partial_len = old_hdr.partial_len - diff_idx - 1;
    new_hdr.depth = old_hdr.depth + diff_idx + 1;
    new_hdr.node_type = old_hdr.node_type;
    return new_hdr;
  }

  NodeType type() const {
    return static_cast<NodeType>(node_type);
  }
  static const uint64_t node_type_mask = (((1UL << define::nodeTypeNumBit) - 1) << 7);
} __attribute__((packed));


static_assert(sizeof(Header) == 8);
static_assert(1UL << (8 - define::nodeTypeNumBit) >= define::hPartialLenMax);


/*
  Internal Nodes
*/
class BufferEntry {
public:
  union {
  union {
    struct {
      uint8_t  partial;
//      uint8_t  fp :define::fp;
      uint8_t node_type : 2;   // 0 -> leaf  1->buffer  2->internal node
      uint8_t  prefix_type : 1;  //1-> match
      uint8_t  leaf_type : define::leaf_type;  //指向内部节点的时候是内部节点的节点类型
      PackedGAddr packed_addr;
    }__attribute__((packed));
  };

  uint64_t val;
  };

public:
  BufferEntry() : val(0) {}
  BufferEntry(uint8_t node_type, uint8_t partial,uint8_t prefix_type,uint8_t leaf_type,const GlobalAddress &addr) :
                partial(partial),node_type(node_type), prefix_type(prefix_type),leaf_type(leaf_type),packed_addr{addr.nodeID, addr.offset >> ALLOC_ALLIGN_BIT} {}
  BufferEntry(const BufferEntry& e) :
                partial(e.partial),node_type(e.node_type), prefix_type(e.prefix_type), leaf_type(e.leaf_type),packed_addr(e.packed_addr) {}
  BufferEntry(NodeType node_type, const BufferEntry& e) :
                partial(e.partial),node_type(e.node_type),prefix_type(e.prefix_type),leaf_type(static_cast<uint8_t>(node_type)),packed_addr(e.packed_addr) {}
  operator uint64_t() const { return val; }

  static BufferEntry Null() {
    static BufferEntry zero;
    return zero;
  }
  NodeType type() const {
    return static_cast<NodeType>(leaf_type);
  }

  GlobalAddress addr() const {
    return GlobalAddress{packed_addr.mn_id, packed_addr.offset << ALLOC_ALLIGN_BIT};
  }
} __attribute__((packed));
class InternalEntry {
public:
  union {
  union {
    // is_buffer = 0
    struct {
      uint8_t  partial;
      uint8_t  child_type   : 2;  // 0 -> leaf  1->buffer  2->internal node
      uint8_t  empty     : 8 - 2 - define::nodeTypeNumBit;
      uint8_t  node_type : define::nodeTypeNumBit;

      PackedGAddr packed_addr;
    }__attribute__((packed));


  };

  uint64_t val;
  };

public:
  InternalEntry() : val(0) {}
  InternalEntry(uint8_t partial, uint8_t child_type,const GlobalAddress &addr) :
                partial(partial), child_type(child_type), packed_addr{addr.nodeID, addr.offset >> ALLOC_ALLIGN_BIT} {}
  InternalEntry(uint8_t partial, uint8_t child_type,NodeType node_type, const GlobalAddress &addr) :
                partial(partial),child_type(child_type), empty(0), node_type(static_cast<uint8_t>(node_type)),  packed_addr{addr.nodeID, addr.offset >> ALLOC_ALLIGN_BIT} {}
  InternalEntry(NodeType node_type, const InternalEntry& e) :
                partial(e.partial),child_type(e.child_type), empty(0), node_type(static_cast<uint8_t>(node_type)),  packed_addr(e.packed_addr) {}
  InternalEntry(uint8_t partial, const InternalEntry& e) :
                partial(partial), child_type(e.child_type), empty(0), node_type(e.node_type),packed_addr(e.packed_addr) {}
  InternalEntry(uint8_t partial, const BufferEntry& e) :
                partial(e.partial),child_type(e.node_type),packed_addr(e.packed_addr) {}
  operator uint64_t() const { return val; }

  static InternalEntry Null() {
    static InternalEntry zero;
    return zero;
  }

  NodeType type() const {
    return static_cast<NodeType>(node_type);
  }

  GlobalAddress addr() const {
    return GlobalAddress{packed_addr.mn_id, packed_addr.offset << ALLOC_ALLIGN_BIT};
  }
} __attribute__((packed));

inline bool operator==(const InternalEntry &lhs, const InternalEntry &rhs) { return lhs.val == rhs.val; }
inline bool operator!=(const InternalEntry &lhs, const InternalEntry &rhs) { return lhs.val != rhs.val; }

static_assert(sizeof(InternalEntry) == 8);


class InternalPage {
public:
  // for invalidation
  GlobalAddress rev_ptr;

  Header hdr;
  InternalEntry records[256];

public:
  InternalPage() { std::fill(records, records + 256, InternalEntry::Null()); }
  InternalPage(const Key &k, int partial_len, int depth, NodeType node_type, const GlobalAddress& rev_ptr) : rev_ptr(rev_ptr), hdr(k, partial_len, depth, node_type) {
    std::fill(records, records + 256, InternalEntry::Null());
  }

  bool is_valid(const GlobalAddress& p_ptr, int depth, bool from_cache) const { return hdr.type() != NODE_DELETED && hdr.depth <= depth && (!from_cache || p_ptr == rev_ptr); }
} __attribute__((packed));


static_assert(sizeof(InternalPage) == 8 + 8 + 256 * 8);


/*缓冲节点  */



class InternalBuffer {
public:
  // for invalidation
  GlobalAddress rev_ptr;

  BufferHeader hdr;
  BufferEntry records[256];
  union {
  struct {
    uint8_t w_lock    : 1;
    uint8_t r_padding : 7;
  };
  uint8_t lock_byte;
  };


public:
  InternalBuffer() { std::fill(records, records + (define::count_1 + define::count_2) -2, BufferEntry::Null()); }
  InternalBuffer(const InternalBuffer &bnode)
   {  rev_ptr = bnode.rev_ptr;
      hdr = bnode.hdr;
      for(int i=0;i<256;i++)
      {
        records[i] = bnode.records[i];
      }
  
    }

  InternalBuffer(const Key &k, int partial_len, int depth, int count_1,int count_2, const GlobalAddress& rev_ptr) : rev_ptr(rev_ptr), hdr(k, partial_len, depth, count_1,count_2) {
    std::fill(records, records + 256, BufferEntry::Null());

  }
  InternalBuffer(int depth,std::vector<InternalEntry> records)
  {
    hdr.depth = depth;
    for(int i=0;i<(int)records.size();i++)
    {
      this->records[i] = *((BufferEntry*)&records[i]);
    }
  }

  bool is_valid(const GlobalAddress& p_ptr, int depth, bool from_cache) const { return hdr.depth <= depth && (!from_cache || p_ptr == rev_ptr); }
  void unlock() { w_lock = 0; };
  void lock() { w_lock = 1; };

}__attribute__((packed));


static_assert(sizeof(InternalBuffer) == 8 + 8 + 256 * 8 + 1);



/*
  Range Query
*/
enum State : uint8_t {
  INSIDE,
  BORDER,
  OUTSIDE
};


class CacheEntry;

struct RangeCache {
  Key from;
  Key to;  // include
  GlobalAddress e_ptr;
  InternalEntry e;
  int depth;
  volatile CacheEntry** entry_ptr_ptr;
  CacheEntry* entry_ptr;
  RangeCache() {}
  RangeCache(const Key& from, const Key& to, const GlobalAddress& e_ptr, const InternalEntry& e, int depth, volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr) :
             from(from), to(to), e_ptr(e_ptr), e(e), depth(depth), entry_ptr_ptr(entry_ptr_ptr), entry_ptr(entry_ptr) {}
};


struct ScanContext {
  InternalEntry e;
  GlobalAddress e_ptr;
  int depth;
  bool from_cache;
  volatile CacheEntry** entry_ptr_ptr;
  CacheEntry* entry_ptr;
  Key from;
  Key to;  // include
  State l_state;
  State r_state;
  ScanContext() {}
  ScanContext(const InternalEntry& e, const GlobalAddress& e_ptr, int depth, bool from_cache, volatile CacheEntry** entry_ptr_ptr, CacheEntry* entry_ptr,
              const Key& from, const Key& to, State l_state, State r_state) :
              e(e), e_ptr(e_ptr), depth(depth), from_cache(from_cache), entry_ptr_ptr(entry_ptr_ptr), entry_ptr(entry_ptr),
              from(from), to(to), l_state(l_state), r_state(r_state) {}
};

#endif // _NODE_H_
