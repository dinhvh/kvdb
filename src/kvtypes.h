//
//  kvtypes.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/1/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef KVTYPES_H
#define KVTYPES_H

#include <inttypes.h>
#include <sys/types.h>

#include "kvdb.h"

/*
 header:
 marker: KVDB: 32 bits
 version: 32bits
 firstmaxcount: 64bits
 filesize: 64 bits
 free list: 64 bytes
 firsttable: table data
 */

#define KV_HEADER_SIZE (4 + 4 + 8 + 8 + 64 * 8)
#define KV_HEADER_MARKER_OFFSET 0
#define KV_HEADER_VERSION_OFFSET 4
#define KV_HEADER_FIRSTMAXCOUNT_OFFSET (4 + 4)
#define KV_HEADER_FILESIZE_OFFSET (8 + 4 + 8)
#define KV_HEADER_FREELIST_OFFSET (8 + 4 + 8 + 8)

/*
 table:
 next offset: 64 bits
 count: 64 bits
 bloom_size: 64 bits
 maxcount: 64 bits
 bloom filter table: BLOOM_FILTER_SIZE(size) bytes
 offset to items (actual hash table): maxcount items of 64 bits
 
 table mapping size: 8 + 8 + 8 + 8 + BLOOM_FILTER_SIZE(size) + (size * 8)
*/

#define KV_TABLE_NEXT_TABLE_OFFSET_OFFSET 0
#define KV_TABLE_COUNT_OFFSET 8
#define KV_TABLE_BLOOM_SIZE_OFFSET 16
#define KV_TABLE_MAX_COUNT_OFFSET 24
#define KV_TABLE_BLOOM_FILTER_OFFSET 32
#define KV_TABLE_ITEMS_OFFSET_OFFSET(maxcount) (KV_TABLE_HEADER_SIZE + KV_TABLE_BLOOM_FILTER_SIZE(maxcount))

#define KV_TABLE_HEADER_SIZE (8 + 8 + 8 + 8)

#define KV_TABLE_SIZE(maxcount) (KV_TABLE_HEADER_SIZE + KV_TABLE_BLOOM_FILTER_SIZE(maxcount) + maxcount * 8)
#define KV_FIRST_TABLE_MAX_COUNT (1 << 17)

#define KV_TABLE_BITS_FOR_BLOOM_FILTER 5
#define KV_TABLE_BLOOM_FILTER_SIZE(maxcount) (KV_BYTE_ROUND_UP(kv_getnextprime(maxcount * KV_TABLE_BITS_FOR_BLOOM_FILTER)) / 8)
#define KV_BLOOM_FILTER_HASH_COUNT 3

#define KV_MAX_MEAN_COLLISION 3

/*
 item:
 next offset 64 bits
 hash_value 32 bits
 key size: 64 bits
 key bytes
 data size: 64 bits
 data bytes
 */

#define KV_BLOCK_NEXT_OFFSET_OFFSET 0
#define KV_BLOCK_HASH_VALUE_OFFSET 8
#define KV_BLOCK_LOG2SIZE_OFFSET 9
#define KV_BLOCK_KEY_SIZE_OFFSET 13
#define KV_BLOCK_KEY_BYTES_OFFSET 21

struct kvdb_mapping {
    char * kv_bytes;
    size_t kv_size;
};

struct kvdb {
    char * kv_filename;
    int kv_pagesize;
    int kv_fd;
    int kv_opened;
    uint64_t kv_firstmaxcount;
    uint64_t * kv_filesize; // host order
    uint64_t * kv_free_blocks; // host order
    struct kvdb_table * kv_first_table;
    struct kvdb_table * kv_current_table;
};

struct kvdb_item {
    // host order
    uint64_t kv_offset;
};

struct kvdb_table {
    struct kvdb_mapping kv_mapping;
    char * kv_table_start;
    struct kvdb_item * kv_items;
    uint64_t * kv_bloom_filter_size; // host order
    uint8_t * kv_bloom_filter;
    uint64_t * kv_next_table_offset; // host order
    uint64_t * kv_count; // host order
    uint64_t * kv_maxcount; // host order
    struct kvdb_table * kv_next_table;
};

struct find_key_cb_params {
    const char * key;
    size_t key_size;
    uint64_t previous_offset;
    uint64_t current_offset;
    uint64_t next_offset;
    struct kvdb_item * item;
    uint64_t * table_count;
    size_t log2_size;
};

typedef void findkey_callback(kvdb * db, struct find_key_cb_params * params,
                              void * data);

#endif
