#if !defined(_KEY_H_)
#define _KEY_H_

#include "Common.h"


inline uint8_t get_partial(const Key& key, int depth) {
  return depth == 0 ? 0 : key.at(depth - 1);
}


inline Key get_leftmost(const Key& key, int depth) {
  Key res{};
  std::copy(key.begin(), key.begin() + depth, res.begin());
  return res;
}


inline Key get_rightmost(const Key& key, int depth) {
  Key res{};
  std::copy(key.begin(), key.begin() + depth, res.begin());
  std::fill(res.begin() + depth, res.end(), (1UL << 8) - 1);
  return res;
}


using Prefix = std::vector<uint8_t>;
inline Key get_leftmost(const Prefix& prefix) {
  Key res{};
  std::copy(prefix.begin(), prefix.end(), res.begin());
  return res;
}


inline Key get_rightmost(const Prefix& prefix) {
  Key res{};
  std::copy(prefix.begin(), prefix.end(), res.begin());
  std::fill(res.begin() + prefix.size(), res.end(), (1UL << 8) - 1);
  return res;
}


inline Key remake_prefix(const Key& key, int depth, uint8_t diff_partial) {
  Key res{};
  if (depth > 0) {
    std::copy(key.begin(), key.begin() + depth - 1, res.begin());
    res.at(depth - 1) = diff_partial;
  }
  return res;
}


inline int longest_common_prefix(const Key &k1, const Key &k2, int depth) {
  assert((uint32_t)depth <= sizeof(k1) && (uint32_t)depth <= sizeof(k2)  );

  int idx, max_cmp = (sizeof(k1)>sizeof(k2) ? sizeof(k2):sizeof(k1)) - depth;
 //  int idx, max_cmp = (int)define::keyLen - depth; 

  for (idx = 0; idx <= max_cmp; ++ idx) {
    if (get_partial(k1, depth + idx) != get_partial(k2, depth + idx))
      return idx;
  }
  return idx;
}

inline void add_one(Key& a) {
/*
  for (int i = 0; i < (int)define::keyLen; ++ i) {
    auto& partial = a.at(define::keyLen - 1 - i);
    if ((int)partial + 1 < (1 << 8)) {
      partial ++;
      return;
    }
    else {
      partial = 0;
    }
  }
  */
  int keylen=a.size();
  for (int i = 0; i < keylen; ++ i) {
    auto& partial = a.at(keylen - 1 - i);
    if ((int)partial + 1 < (1 << 8)) {
      partial ++;
      return;
    }
    else {
      partial = 0;
    }
  }
}

inline Key operator+(const Key& a, uint8_t b) {
  Key res = a;
/*  
  for (int i = 0; i < (int)define::keyLen; ++ i) {
    auto& partial = res.at(define::keyLen - 1 - i);
    if ((int)partial + b < (1 << 8)) {
      partial += b;
      break;
    }
    else {
      auto tmp = ((int)partial + b);
      partial = tmp % (1 << 8);
      b = tmp / (1 << 8);
    }
  }
*/
  int keylen=a.size();
  for (int i = 0; i < keylen; ++ i) {
    auto& partial = res.at(keylen - 1 - i);
    if ((int)partial + b < (1 << 8)) {
      partial += b;
      break;
    }
    else {
      auto tmp = ((int)partial + b);
      partial = tmp % (1 << 8);
      b = tmp / (1 << 8);
    }
    if(i == keylen-1 && b !=0 ) res.insert(res.begin(), static_cast<uint8_t>(b));
  }
  
  return res;
}

inline Key operator-(const Key& a, uint8_t b) {
  Key res = a;
/*  for (int i = 0; i < (int)define::keyLen; ++ i) {
    auto& partial = res.at(define::keyLen - 1 - i);
    if (partial >= b) {
      partial -= b;
      break;
    }
    else {
      int carry = 0, tmp = partial;
      while(tmp < b) tmp += (1 << 8), carry ++;
      partial = ((int)partial + carry * (1 << 8)) - b;
      b = carry;
    }
  }
  */
  int  keylen=a.size();
  for (int i = 0; i < keylen; ++ i) {
    auto& partial = res.at(keylen - 1 - i);
    if (partial >= b) {
      partial -= b;
      break;
    }
    else {
      int carry = 0, tmp = partial;
      while(tmp < b) tmp += (1 << 8), carry ++;
      partial = ((int)partial + carry * (1 << 8)) - b;
      b = carry;
    }
  }
  //把高位为0的去掉
  std::vector<uint8_t>::iterator it;
  for(it = res.begin();it != res.end();it ++)
  {
    if(* it == 0) res.erase(it);
    else break;
  }
  return res;
}

inline Key int2key(uint64_t key) {    //将key每8位存储 从高位到低位
#ifdef KEY_SPACE_LIMIT
  key = key % (kKeyMax - kKeyMin) + kKeyMin;
#endif
  Key res{};
/*  for (int i = 1; i <= (int)define::keyLen; ++ i) {
    auto shr = (define::keyLen - i) * 8;
    res.at(i - 1) = (shr >= 64u ? 0 : ((key >> shr) & ((1 << 8) - 1))); // Is equivalent to padding zero for short key
  }*/
  uint32_t maxkeylen =64;

  for (uint32_t i = 1; i <= maxkeylen; ++ i) {
    auto shr = (maxkeylen - i) * 8;
    res.at(i - 1) = (shr >= 64u ? 0 : ((key >> shr) & ((1 << 8) - 1))); // Is equivalent to padding zero for short key
  }
  std::vector<uint8_t>::iterator it;
  for(it = res.begin();it != res.end();it ++)
  {
    if(* it == 0) res.erase(it);
    else break;
  }
  return res;
}

inline Key str2key(const std::string &key) {
  // assert(key.size() <= define::keyLen);
  Key res{};
//  std::copy(key.begin(), key.size() <= define::keyLen ? key.end() : key.begin() + define::keyLen, res.begin());
    std::copy(key.begin(),  key.end(), res.begin());
  return res;
}

inline uint64_t key2int(const Key& key) {
  uint64_t res = 0;
  for (auto a : key) res = (res << 8) + a;
  return res;
}

#endif // _KEY_H_
