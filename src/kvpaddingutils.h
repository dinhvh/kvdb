//
//  kvpaddingutils.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/2/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef KVPADDINGUTILS_H
#define KVPADDINGUTILS_H

#include <sys/mman.h>

static inline uint64_t power2_round_up(uint64_t value)
{
    uint64_t power = 1;
    while (power < value)
        power <<= 1;
    return power;
}

static inline uint64_t block_size_round_up(uint64_t value)
{
    if (value < 16) {
        value = 16;
    }
    return power2_round_up(value);
}

static inline unsigned int log2_round_up(uint64_t value)
{
    uint64_t power = 1;
    unsigned int log2_value = 1;
    while (power < value) {
        power <<= 1;
        log2_value ++;
    }
    return log2_value;
}

#define KV_ULONG_PTR unsigned long
#define KV_PAGE_ROUND_UP(db, x) ( (((KV_ULONG_PTR)(x)) + db->kv_pagesize-1)  & (~(db->kv_pagesize-1)) )
#define KV_PAGE_ROUND_DOWN(db, x) ( ((KV_ULONG_PTR)(x)) & (~(db->kv_pagesize-1)) )
#define KV_BYTE_ROUND_UP(x) ( (((KV_ULONG_PTR)(x)) + 8-1)  & (~(8-1)) )

#endif
