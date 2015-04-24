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

#if 0
static inline void table_transaction_bloom_filter_set(struct kvdb * db, unsigned int table_index,
                                                      uint32_t * hash_values, int hash_count)
{
    for(unsigned int i = 0 ; i < hash_count ; i ++) {
        uint64_t idx = hash_values[i] % db->kv_transaction->tables[table_index].bloomsize;
        uint64_t byte = idx / 8;
        int bit = idx % 8;
        std::unordered_map<uint64_t, uint8_t>::iterator it = db->kv_transaction->tables[table_index].bloom_table.find(idx / 8);
        if (it != db->kv_transaction->tables[table_index].bloom_table.end()) {
            it->second |= 1 << bit;
        }
        else {
            db->kv_transaction->tables[table_index].bloom_table.insert(std::pair<uint64_t, uint8_t>(byte, 1 << bit));
        }
    }
}

static inline int table_bloom_filter_might_contain(struct kvdb_table * table, uint32_t * hash_values,
                                                   int hash_count)
{
    for(unsigned int i = 0 ; i < hash_count ; i ++) {
        uint64_t idx = hash_values[i] % ntoh64(* table->kv_bloom_filter_size);
        if ((table->kv_bloom_filter[idx / 8] & (1 << (idx % 8))) == 0) {
            return 0;
        }
    }
    return 1;
}
#endif

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
