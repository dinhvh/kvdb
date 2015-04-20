//
//  table.c
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/2/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#include "kvtable.h"

#include <strings.h>
#include <unistd.h>
#include <sys/mman.h>
#include <stdio.h>

#include "kvtypes.h"
#include "kvprime.h"
#include "kvpaddingutils.h"
#include "kvassert.h"
#include "kvblock.h"

static int map_table(kvdb * db, struct kvdb_table ** result, uint64_t offset, uint64_t filesize, int is_first);
static int mapping_setup(struct kvdb_mapping * mapping, int fd, off_t offset, size_t size);
static void mapping_unsetup(struct kvdb_mapping * mapping);
static void unmap_table(struct kvdb_table * table);
static uint64_t transaction_table_create(kvdb * db, uint64_t size, struct kvdb_table ** result);

int kv_table_header_write(kvdb * db, uint64_t table_start, uint64_t maxcount)
{
    uint64_t bloomsize = kv_getnextprime(maxcount * KV_TABLE_BITS_FOR_BLOOM_FILTER);
    char data[KV_TABLE_HEADER_SIZE];
    bzero(data, KV_TABLE_HEADER_SIZE);
    h64_to_bytes(&data[KV_TABLE_BLOOM_SIZE_OFFSET], bloomsize);
    h64_to_bytes(&data[KV_TABLE_MAX_COUNT_OFFSET], maxcount);
    ssize_t r = pwrite(db->kv_fd, data, KV_TABLE_HEADER_SIZE, table_start);
    if (r <= 0) {
        return KVDB_ERROR_IO;
    }
    return KVDB_ERROR_NONE;
}

int kv_tables_setup(kvdb * db, uint64_t filesize)
{
    return map_table(db, &db->kv_first_table, KV_HEADER_SIZE, filesize, 1);
}

void kv_tables_unsetup(kvdb * db)
{
    unmap_table(db->kv_first_table);
}

uint64_t kv_table_create(kvdb * db, uint64_t size, struct kvdb_table ** result)
{
    kv_assert(db->kv_transaction != NULL);
    uint64_t mapping_size = KV_TABLE_SIZE(size);
    uint64_t offset = db->kv_transaction->filesize;
    int r;
    
    kv_block_buffer_flush(db);
    
    r = ftruncate(db->kv_fd, offset + mapping_size);
    if (r < 0) {
        return 0;
    }
    db->kv_transaction->filesize += mapping_size;
    r = kv_table_header_write(db, offset, size);
    if (r < 0)
        return 0;
    
    return offset;
}

static int map_table(kvdb * db, struct kvdb_table ** result, uint64_t offset, uint64_t filesize, int is_first)
{
    struct kvdb_table * table;
    uint64_t maxcount;
    ssize_t read_result;
    char data[8];
    int r;
    off_t pre_page_align_size;
    
    table = (kvdb_table *) calloc(1, sizeof(* table));
    if (is_first) {
        pre_page_align_size = KV_HEADER_SIZE;
    }
    else {
        off_t mapping_offset = KV_PAGE_ROUND_DOWN(db, offset);
        pre_page_align_size = offset - mapping_offset;
    }
    
    read_result = pread(db->kv_fd, data, 8, offset + KV_TABLE_MAX_COUNT_OFFSET);
    if (read_result <= 0) {
        return KVDB_ERROR_IO;
    }
    maxcount = bytes_to_h64(data);
    uint64_t mapping_size = pre_page_align_size + KV_TABLE_SIZE(maxcount);
    if (offset + KV_TABLE_SIZE(maxcount) > filesize) {
        return KVDB_ERROR_IO;
    }
    r = mapping_setup(&table->kv_mapping, db->kv_fd, offset - pre_page_align_size, (size_t) mapping_size);
    if (r < 0) {
        return r;
    }
    table->kv_offset = offset;
    table->kv_table_start = table->kv_mapping.kv_bytes + pre_page_align_size;
    
    table->kv_items = (struct kvdb_item *) (table->kv_table_start + KV_TABLE_ITEMS_OFFSET_OFFSET(maxcount));
    table->kv_next_table_offset = (uint64_t *) (table->kv_table_start + KV_TABLE_NEXT_TABLE_OFFSET_OFFSET);
    table->kv_count = (uint64_t *) (table->kv_table_start + KV_TABLE_COUNT_OFFSET);
    table->kv_bloom_filter_size = (uint64_t *) (table->kv_table_start + KV_TABLE_BLOOM_SIZE_OFFSET);
    table->kv_maxcount = (uint64_t *) (table->kv_table_start + KV_TABLE_MAX_COUNT_OFFSET);
    table->kv_bloom_filter = (uint8_t *) (table->kv_table_start + KV_TABLE_BLOOM_FILTER_OFFSET);
    
    * result = table;
    
    if (* table->kv_next_table_offset != 0) {
        r = map_table(db, &table->kv_next_table, ntoh64(* table->kv_next_table_offset), filesize, 0);
        if (r < 0) {
            return r;
        }
    }
    else {
        table->kv_next_table = NULL;
    }
    
    return KVDB_ERROR_NONE;
}

static void unmap_table(struct kvdb_table * table)
{
    if (table == NULL)
        return;
    
    struct kvdb_table * next_table = table->kv_next_table;
    mapping_unsetup(&table->kv_mapping);
    free(table);
    
    unmap_table(next_table);
}

static int mapping_setup(struct kvdb_mapping * mapping, int fd, off_t offset, size_t size)
{
    mapping->kv_offset = offset;
    mapping->kv_bytes = (char *) mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
    if (mapping->kv_bytes == MAP_FAILED) {
        mapping->kv_bytes = NULL;
        mapping->kv_size = 0;
        return KVDB_ERROR_IO;
    }
    mapping->kv_size = size;
    
    return KVDB_ERROR_NONE;
}

static void mapping_unsetup(struct kvdb_mapping * mapping)
{
    if (mapping->kv_bytes == NULL) {
        return;
    }
    
    munmap(mapping->kv_bytes, mapping->kv_size);
    mapping->kv_bytes = NULL;
    mapping->kv_size = 0;
}

int kv_map_table(kvdb * db, struct kvdb_table ** result, uint64_t offset, uint64_t filesize)
{
    return map_table(db, result, offset, filesize, 0);
}
