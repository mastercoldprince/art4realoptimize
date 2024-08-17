#ifndef __HUGEPAGEALLOC_H__
#define __HUGEPAGEALLOC_H__


#include "Debug.h"

#include <cstdint>

#include <sys/mman.h>
#include <memory.h>
#include <errno.h>
#include <stdio.h>


char *getIP();
inline void *hugePageAlloc(size_t size) {

    void *res = mmap(NULL, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (res == MAP_FAILED) {
        Debug::notifyError("%s mmap failed!\n", getIP());
        fprintf(stderr, "mmap failed: %s\n", strerror(errno));
    }

    return res;
}

inline void hugePageFree(void *addr, size_t size) {
    int res = munmap(addr, size);
    if (res == -1) {
        Debug::notifyError("%s munmap failed! %d\n", getIP(), errno);
    }
    return;
}

#endif /* __HUGEPAGEALLOC_H__ */
