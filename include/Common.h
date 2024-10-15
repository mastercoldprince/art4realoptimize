#ifndef __COMMON_H__
#define __COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <queue>
#include <bitset>
#include <limits>

#include "Debug.h"
#include "HugePageAlloc.h"
#include "Rdma.h"
#include <string>
#include <functional>
#include <array>
#include <iostream>

#include "WRLock.h"

// Environment Config
#define MAX_MACHINE 5
#define MEMORY_NODE_NUM 1
#define CPU_PHYSICAL_CORE_NUM 28  // [CONFIG]
#define MAX_CORO_NUM 8

#define LATENCY_WINDOWS 100000
#define ALLOC_ALLIGN_BIT 8
#define MAX_KEY_SPACE_SIZE 60000000
// #define KEY_SPACE_LIMIT


// Auxiliary function
#define STRUCT_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define UNUSED(x) (void)(x)

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define ROUND_UP(x, n) (((x) + (1<<(n)) - 1) & ~((1<<(n)) - 1))

#define ROUND_DOWN(x, n) ((x) & ~((1<<(n)) - 1))

#define MESSAGE_SIZE 96 // byte

#define RAW_RECV_CQ_COUNT 4096 // 128


// app thread
#define MAX_APP_THREAD 50    // one additional thread for data statistics(main thread)  [config]
#define APP_MESSAGE_NR 96
#define POLL_CQ_MAX_CNT_ONCE 8

// dir thread
#define NR_DIRECTORY 1
#define DIR_MESSAGE_NR 128


void bindCore(uint16_t core);
char *getIP();
char *getMac();

inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}

#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <boost/coroutine/all.hpp>
#include <boost/crc.hpp>

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

using CheckFunc = std::function<bool ()>;
using CoroQueue = std::queue<std::pair<uint16_t, CheckFunc> >;
struct CoroContext {
  CoroYield *yield;
  CoroCall *master;
  CoroQueue *busy_waiting_queue;
  int coro_id;
};

using CRCProcessor = boost::crc_optimal<64, 0x42F0E1EBA9EA3693, 0xffffffffffffffff, 0xffffffffffffffff, false, false>;

namespace define {   // namespace define

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// Remote Allocation
constexpr uint64_t dsmSize           =100;        // GB  [CONFIG]
constexpr uint64_t kChunkSize        = 16 * MB;   // B

// Rdma Buffer
constexpr uint64_t rdmaBufferSize    = 4;         // GB  [CONFIG]
constexpr int64_t kPerThreadRdmaBuf  = rdmaBufferSize * define::GB / MAX_APP_THREAD;
constexpr int64_t kPerCoroRdmaBuf    = kPerThreadRdmaBuf / MAX_CORO_NUM;

// Cache (MB)
constexpr int kIndexCacheSize = 800;

// KV
constexpr uint32_t maxkeyLen = 128;   
constexpr uint32_t simulatedValLen = 128;//value 用array存，在第一个位置记录value长度
constexpr uint32_t allocAlignKVLeafSize = ROUND_UP(maxkeyLen + simulatedValLen +8 + 1 + 1 + 2 + 2 +1 +1, ALLOC_ALLIGN_BIT);   
constexpr uint32_t allocAlignPTRLeafSize= ROUND_UP(8 + 8 + 8 + 1 + 2 + 2, ALLOC_ALLIGN_BIT);   
constexpr uint32_t keybuffer   =1024;
constexpr uint32_t valuebuffer =512*MB;

// Tree
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0);

// Internal Node
constexpr uint32_t allocationPageSize = 8 + 8 + 256 * 8 + 1;
constexpr uint32_t allocAlignPageSize = ROUND_UP(allocationPageSize, ALLOC_ALLIGN_BIT);

// Internal Entry
constexpr uint32_t LeafCntBit      = 7;
constexpr uint32_t nodeTypeNumBit  = 5;
constexpr uint32_t mnIdBit         = 8;
constexpr uint32_t offsetBit       = 48 - ALLOC_ALLIGN_BIT;
constexpr uint32_t hPartialLenMax  = 6;

//Buffer ndoe
constexpr uint32_t partial_len  = 2;
constexpr uint32_t bPartialLenMax  = 0;
constexpr uint32_t count_1  = 9;
constexpr uint32_t count_2  = 9;
constexpr uint32_t leaf_type  = 5;
//constexpr uint32_t fp  = 2;
constexpr uint32_t allocationBufferSize = 8 + 8 +  256* 8 + 1;
constexpr uint32_t allocAlignBufferSize = ROUND_UP(allocationBufferSize, ALLOC_ALLIGN_BIT);





// On-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = ON_CHIP_SIZE * 1024;
constexpr uint64_t kLocalLockNum = 4 * MB;  // tune to an appropriate value (as small as possible without affect the performance)
constexpr uint64_t kOnChipLockNum = kLockChipMemSize * 8;  // 1bit-lock
}


using Key = std::array<uint8_t, define::maxkeyLen>; 
using Value = std::array<uint8_t, define::simulatedValLen>; 

constexpr uint64_t kKeyMin = 1;
#ifdef KEY_SPACE_LIMIT
constexpr uint64_t kKeyMax = 60000000;  // only for int workloads
#endif
constexpr uint64_t kValueNull = std::numeric_limits<uint64_t>::min();
constexpr uint64_t kValueMin = 1;
constexpr uint64_t kValueMax = std::numeric_limits<uint64_t>::max();

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

#endif /* __COMMON_H__ */
