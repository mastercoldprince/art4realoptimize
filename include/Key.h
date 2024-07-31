#if !defined(_KEY_H_)
#define _KEY_H_

#include "Common.h"


inline uint8_t get_partial(const Key& key, int depth) {
  return depth == 0 ? 0 : key.at(depth - 1);
}

inline int get_2B_partial(const std::vector<Key> keys, int depth) {   //key 从第depth个字节开始 有没有共同的  前depth个都是一样的 看后面的

  if (keys.empty() || depth >=(int) keys[0].size()) {
      return 0;
  }
  // Initialize the common prefix length to the maximum possible length
  int common_prefix_length = keys[0].size() - depth;

  // Compare each character starting from depth
  for (int i = depth; i <(int) keys[0].size(); ++i) {
      char current_char = keys[0][i];
      for (const auto& key : keys) {
          if (key[i] != current_char) {
              common_prefix_length = i - depth;
              return common_prefix_length;
          }
      }
  }
  return common_prefix_length;
}

inline Key get_leftmost(const Key& key, int depth) {
  Key res{};
  res.at(64) = depth;
  std::copy(key.begin(), key.begin() + depth, res.begin());
  return res;
}


inline Key get_rightmost(const Key& key, int depth) {
  Key res{};
  res.at(64) = depth;
  std::copy(key.begin(), key.begin() + depth, res.begin());
  std::fill(res.begin() + depth, res.end() -1 , (1UL << 8) - 1);
  return res;
}


using Prefix = std::vector<uint8_t>;
inline Key get_leftmost(const Prefix& prefix) {
  Key res{};
  res.at(64) = prefix.size();
  std::copy(prefix.begin(), prefix.end(), res.begin());
  return res;
}


inline Key get_rightmost(const Prefix& prefix) {
  Key res{};
  res.at(64) = prefix.size();
  std::copy(prefix.begin(), prefix.end(), res.begin());
  std::fill(res.begin() + prefix.size(), res.end(), (1UL << 8) - 1);
  return res;
}


inline Key remake_prefix(const Key& key, int depth, uint8_t diff_partial) {
  Key res{};
  if (depth > 0) {
    res.at(64) = depth;
    std::copy(key.begin(), key.begin() + depth - 1, res.begin());
    res.at(depth - 1) = diff_partial;
  }
  return res;
}


inline int longest_common_prefix(const Key &k1, const Key &k2, int depth) {
  assert((uint32_t)depth <= k1.at(64) && (uint32_t)depth <= k2.at(64));

  int idx, max_cmp =( k1.at(64)>k2.at(64)? k2.at(64): k1.at(64) )- depth;

  for (idx = 0; idx <= max_cmp; ++ idx) {
    if (get_partial(k1, depth + idx) != get_partial(k2, depth + idx))
      return idx;
  }
  return idx;
}

inline void add_one(Key& a) {
  for (int i = 0; i < (int)a.at(64); ++ i) {
    auto& partial = a.at(a.at(64) - 1 - i);
    if ((int)partial + 1 < (1 << 8)) {
      partial ++;
      return;
    }
    else {
      partial = 0;
    }
  }
}
inline Value v_add_one(Value a) {
  Value res=a;
  for (int i = 1; i < (int)res.at(0); ++ i) {
    auto& partial = res.at(a.at(64) - 1 - i);
    if ((int)partial + 1 < (1 << 8)) {
      partial ++;
      return res;
    }
    else {
      partial = 0;
    }
  }
  return res;

}

inline Key operator+(const Key& a, uint8_t b) {
  Key res = a;
  for (int i = 0; i < (int)a.at(64); ++ i) {
    auto& partial = res.at(a.at(64) - 1 - i);
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
  return res;
}

inline Key operator-(const Key& a, uint8_t b) {
  Key res = a;
  for (int i = 0; i < (int)a.at(64); ++ i) {
    auto& partial = res.at(a.at(64) - 1 - i);
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
  int zero_cnt=0;
  for(int i=0;i<(int)a.at(64);i++)
  {
    if(res.at(i)== 0) zero_cnt ++;
    else break;
  }
  for(int i=zero_cnt; i<res.at(64); i++)
  {
    res.at(i-zero_cnt) = res.at(i);
  }
  res.at(64) -= zero_cnt;

  return res;
}

inline Key int2key(uint64_t key) {
#ifdef KEY_SPACE_LIMIT
  key = key % (kKeyMax - kKeyMin) + kKeyMin;
#endif
  Key res{};
  uint16_t keylen=0;
  uint64_t a=key;
  while(a!=0)
  {
    a= a>>8;
    keylen++;
  }
  for (int i = 1; i <=(int) keylen; ++ i) {
    auto shr = (keylen - i) * 8;
    res.at(i - 1) = ((uint64_t)shr >= 64u ? 0 : ((key >> (uint64_t)shr) & ((1 << 8) - 1))); // Is equivalent to padding zero for short key
  }
  std::fill(res.begin() + keylen, res.end() -1 , 0);
  res.at(64)=keylen;
  return res;
}

inline Key str2key(const std::string &key) {
  // assert(key.size() <= define::keyLen);
  Key res{};
  res.at(64) = key.size();
  std::copy(key.begin(), key.size() <= define::maxkeyLen ? key.end() : key.begin() + define::maxkeyLen -1, res.begin());
  return res;
}

inline uint64_t key2int(const Key& key) {
  uint64_t res = 0;
//  for (auto a : key) res = (res << 8) + a;
  for(int i=0;i<key.at(64);i++) res = (res << 8) + key.at(i);
  return res;
}

inline Value int2value(uint64_t value) {
  Value res{};
  uint16_t vlen=0;
  uint64_t a=value;
  while(a!=0)
  {
    a= a>>8;
    vlen++;
  }
  res.at(0) = vlen;
  for (int i = 1; i <=(int) vlen; ++ i) {
    auto shr = (vlen - i) * 8;
    res.at(i) = ((uint64_t)shr >= 64u ? 0 : ((value >> (uint64_t)shr) & ((1 << 8) - 1))); // Is equivalent to padding zero for short key
  }
  std::fill(res.begin() + vlen + 1, res.end() , 0);

  return res;
}

inline unsigned int generateFingerprint(const Key& key) {
    // Convert array to string
    std::string input(key.begin(), key.end());
    
    // Generate hash value
    std::hash<std::string> hasher;
    size_t hashValue = hasher(input);
    
    // Mask to get the lowest 11 bits
    size_t mask = (1 << 11) - 1;
    
    // Apply mask to get the 11-bit fingerprint
    unsigned int fingerprint = hashValue & mask;
    
    return fingerprint;
}


#endif // _KEY_H_
