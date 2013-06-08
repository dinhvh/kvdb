//
//  kvbloom.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/2/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef KVBLOOM_H
#define KVBLOOM_H

#include "kvmurmurhash.h"

static inline void table_bloom_filter_set(struct kvdb_table * table, uint32_t * hash_values,
                                          int hash_count)
{
    //fprintf(stderr, "----set\n");
    for(unsigned int i = 0 ; i < hash_count ; i ++) {
        uint64_t idx = hash_values[i] % ntoh64(* table->kv_bloom_filter_size);
        //fprintf(stderr, "%u\n", (unsigned int) idx);
        table->kv_bloom_filter[idx / 8] |= 1 << (idx % 8);
    }
}

static inline int table_bloom_filter_might_contain(struct kvdb_table * table, uint32_t * hash_values,
                                                   int hash_count)
{
    //fprintf(stderr, "----get\n");
    for(unsigned int i = 0 ; i < hash_count ; i ++) {
        uint64_t idx = hash_values[i] % ntoh64(* table->kv_bloom_filter_size);
        //fprintf(stderr, "%u\n", (unsigned int) idx);
        if ((table->kv_bloom_filter[idx / 8] & (1 << (idx % 8))) == 0) {
            //fprintf(stderr, "----not found\n");
            return 0;
        }
    }
    //fprintf(stderr, "----found\n");
    return 1;
}

static inline void table_bloom_filter_compute_hash(uint32_t * hash_values, unsigned int hash_count,
                                                   const char * key, size_t key_size)
{
    uint32_t previous_hash_value = 0;
    for(unsigned int i = 0 ; i < hash_count ; i ++) {
        hash_values[i] = kv_murmur_hash(key, key_size, previous_hash_value);
        previous_hash_value = hash_values[i];
    }
}

#endif
