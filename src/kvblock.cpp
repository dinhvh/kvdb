//
//  kvblock.c
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/2/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#include "kvblock.h"

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>

#include "kvdb.h"
#include "kvtypes.h"
#include "kvendian.h"
#include "kvpaddingutils.h"
#include "kvassert.h"

int kv_block_recycle(kvdb * db, uint64_t offset)
{
    kv_assert(db->kv_transaction != NULL);
    
    uint8_t log2_size;
    
    if ((db->kv_write_buffer_location != 0) && (offset + 8 + 4 >= db->kv_write_buffer_location)) {
        log2_size = * (db->kv_write_buffer + (offset + 8 + 4 - db->kv_write_buffer_location));
    }
    else {
        ssize_t count = pread(db->kv_fd, &log2_size, 1, offset + 8 + 4);
        if (count <= 0)
            return KVDB_ERROR_IO;
    }
    db->kv_transaction->recycled_blocks[log2_size].push_back(offset);
    return KVDB_ERROR_NONE;
}

int kv_block_buffer_flush(kvdb * db)
{
    if (db->kv_write_buffer_location == 0) {
        return KVDB_ERROR_NONE;
    }
    
    uint64_t write_offset = db->kv_write_buffer_location;
    size_t size_to_write = db->kv_write_buffer_size - db->kv_write_buffer_remaining;
    size_t remaining = size_to_write;
    char * remaining_data = db->kv_write_buffer;
    while (remaining > 0) {
        ssize_t count = pwrite(db->kv_fd, remaining_data, remaining, write_offset);
        if (count <= 0) {
            return KVDB_ERROR_IO;
        }
        write_offset += count;
        remaining_data += count;
        remaining -= count;
    }
    
    db->kv_write_buffer_remaining = db->kv_write_buffer_size;
    db->kv_write_buffer_next_block = db->kv_write_buffer;
    db->kv_write_buffer_location = 0;
    return KVDB_ERROR_NONE;
}

static int on_disk_block_create(kvdb * db, uint64_t next_block_offset, uint32_t hash_value,
                                const char * key, size_t key_size,
                                const char * value, size_t value_size,
                                int use_new_block,
                                uint64_t block_size, uint8_t log2_size, uint64_t offset)
{
    uint64_t current_key_size = key_size;
    uint64_t current_value_size = value_size;
    char * data;
    char * allocated = NULL;
    size_t size = 8 + 4 + 1 + 8 + 8 + block_size;
    if (size > 4096) {
        allocated = (char *) calloc(size, 1);
        data = allocated;
    }
    else {
        data = (char *) alloca(size);
        bzero(data, size);
    }
    char * p = data;
    next_block_offset = hton64(next_block_offset);
    memcpy(p, &next_block_offset, sizeof(next_block_offset));
    p += sizeof(next_block_offset);
    hash_value = htonl(hash_value);
    memcpy(p, &hash_value, sizeof(hash_value));
    p += sizeof(hash_value);
    memcpy(p, &log2_size, sizeof(log2_size));
    p += sizeof(log2_size);
    current_key_size = hton64(current_key_size);
    memcpy(p, &current_key_size, sizeof(current_key_size));
    p += sizeof(current_key_size);
    memcpy(p, key, key_size);
    p += key_size;
    current_value_size = hton64(current_value_size);
    memcpy(p, &current_value_size, sizeof(current_value_size));
    p += sizeof(current_value_size);
    memcpy(p, value, value_size);
    p += value_size;
    size_t remaining = (8 + 4 + 1 + 8 + 8 + block_size);
    uint64_t write_offset = offset;
    char * remaining_data = data;
    while (remaining > 0) {
        ssize_t count = pwrite(db->kv_fd, remaining_data, remaining, write_offset);
        if (count <= 0) {
            if (allocated != NULL) {
                free(allocated);
            }
            return KVDB_ERROR_IO;
        }
        write_offset += count;
        remaining_data += count;
        remaining -= count;
    }
    if (allocated != NULL) {
        free(allocated);
    }
    if (use_new_block) {
        db->kv_transaction->filesize += size;
    }
    
    return KVDB_ERROR_NONE;
}

uint64_t kv_block_create(kvdb * db, uint64_t next_block_offset, uint32_t hash_value,
                         const char * key, size_t key_size,
                         const char * value, size_t value_size)
{
    kv_assert(db->kv_transaction != NULL);
    
    uint64_t block_size = block_size_round_up(key_size + value_size);
    uint8_t log2_size = log2_round_up(block_size);
    
    int use_new_block = 0;
    uint64_t offset = 0;
    
    // can't reuse in-transaction-recycled blocks because we can't overwrite of
    // data that might need to be restored if the transaction is aborted.
    // therefore, we don't try to use db->kv_transaction->recycled_blocks.
    
    if (db->kv_transaction->first_recycled_blocks[log2_size] != 0) {
        offset = db->kv_transaction->first_recycled_blocks[log2_size];
        uint64_t next_free_offset;
        if ((db->kv_write_buffer_location != 0) && (offset >= db->kv_write_buffer_location)) {
            next_free_offset = * (uint64_t *) (db->kv_write_buffer + (offset - db->kv_write_buffer_location));
        }
        else {
            ssize_t r = pread(db->kv_fd, &next_free_offset, sizeof(next_free_offset), offset);
            if (r <= 0) {
                return 0;
            }
        }
        db->kv_transaction->first_recycled_blocks[log2_size] = ntoh64(next_free_offset);
    }
    else {
        offset = db->kv_transaction->filesize;
        use_new_block = 1;
    }
    
    if ((db->kv_write_buffer_location == 0) && use_new_block)  {
        db->kv_write_buffer_remaining = db->kv_write_buffer_size;
        db->kv_write_buffer_location = db->kv_transaction->filesize;
        db->kv_write_buffer_next_block = db->kv_write_buffer;
    }
    
    uint64_t size = 8 + 4 + 1 + 8 + 8 + block_size;
    if (size > db->kv_write_buffer_size) {
        int r = kv_block_buffer_flush(db);
        if (r < 0) {
            return 0;
        }
        r = on_disk_block_create(db, next_block_offset, hash_value, key, key_size,
                                 value, value_size,
                                 use_new_block,
                                 block_size, log2_size, offset);
        if (r < 0) {
            return 0;
        }
        return offset;
    }
    
    if ((db->kv_write_buffer_location == 0) || (offset < db->kv_write_buffer_location)) {
        int r = on_disk_block_create(db, next_block_offset, hash_value, key, key_size,
                                     value, value_size,
                                     use_new_block,
                                     block_size, log2_size, offset);
        if (r < 0) {
            return 0;
        }
        return offset;
    }
    
    if (use_new_block && (size > db->kv_write_buffer_remaining)) {
        int r = kv_block_buffer_flush(db);
        if (r < 0) {
            return 0;
        }
    }
    
    // in buffer.
    
    uint64_t current_key_size = key_size;
    uint64_t current_value_size = value_size;
    char * data;
    data = db->kv_write_buffer_next_block;
    bzero(data, size);
    char * p = data;
    next_block_offset = hton64(next_block_offset);
    memcpy(p, &next_block_offset, sizeof(next_block_offset));
    p += sizeof(next_block_offset);
    hash_value = htonl(hash_value);
    memcpy(p, &hash_value, sizeof(hash_value));
    p += sizeof(hash_value);
    memcpy(p, &log2_size, sizeof(log2_size));
    p += sizeof(log2_size);
    current_key_size = hton64(current_key_size);
    memcpy(p, &current_key_size, sizeof(current_key_size));
    p += sizeof(current_key_size);
    memcpy(p, key, key_size);
    p += key_size;
    current_value_size = hton64(current_value_size);
    memcpy(p, &current_value_size, sizeof(current_value_size));
    p += sizeof(current_value_size);
    memcpy(p, value, value_size);
    p += value_size;
    
    if (use_new_block) {
        db->kv_transaction->filesize += size;
        db->kv_write_buffer_remaining -= size;
        db->kv_write_buffer_next_block += size;
    }
    
    return offset;
}
