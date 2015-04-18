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

#include "kvdb.h"
#include "kvtypes.h"
#include "kvendian.h"
#include "kvpaddingutils.h"

int kv_block_recycle(kvdb * db, uint64_t offset)
{
    uint8_t log2_size;
    ssize_t count;
    
    count = pread(db->kv_fd, &log2_size, 1, offset + 8 + 4);
    if (count < 0)
        return -1;
    uint64_t next_free_offset = db->kv_free_blocks[log2_size];
    // keep it in network order.
    count = pwrite(db->kv_fd, &next_free_offset, sizeof(next_free_offset), offset);
    if (count < 0)
        return -1;
    db->kv_free_blocks[log2_size] = hton64(offset);
    
    return 0;
}

uint64_t kv_block_create(kvdb * db, uint64_t next_block_offset, uint32_t hash_value,
                         const char * key, size_t key_size,
                         const char * value, size_t value_size)
{
    uint64_t block_size = block_size_round_up(key_size + value_size);
    uint8_t log2_size = log2_round_up(block_size);
    uint64_t offset = ntoh64(db->kv_free_blocks[log2_size]);
    int use_new_block = 0;
    //fprintf(stderr, "key, value: %i %i\n", (int) key_size, (int) value_size);
    if (offset != 0) {
        // Use free block.
        uint64_t next_free_offset;
        //fprintf(stderr, "Use free block %i %i %i\n", (int) offset, (int) log2_size, (int)block_size);
        // keep it in network order.
        pread(db->kv_fd, &next_free_offset, sizeof(next_free_offset), offset);
        db->kv_free_blocks[log2_size] = next_free_offset;
    }
    else {
        // Use new block.
        offset = ntoh64(* db->kv_filesize);
        use_new_block = 1;
    }
    
    uint64_t current_key_size = key_size;
    uint64_t current_value_size = value_size;
    char * data;
    char * allocated = NULL;
    if (8 + 4 + 1 + 8 + 8 + block_size > 4096) {
        allocated = calloc(1, 8 + 4 + 1 + 8 + 8 + (size_t) block_size);
        data = allocated;
    }
    else {
        data = alloca(8 + 4 + 1 + 8 + 8 + (size_t) block_size);
        bzero(data, 8 + 4 + 1 + 8 + 8 + (size_t) block_size);
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
    size_t write_offset = offset;
    char * remaining_data = data;
    while (remaining > 0) {
        ssize_t count = pwrite(db->kv_fd, remaining_data, remaining, write_offset);
        if (count < 0) {
            if (allocated != NULL) {
                free(allocated);
            }
            return 0;
        }
        write_offset += count;
        remaining_data += count;
        remaining -= count;
    }
    if (allocated != NULL) {
        free(allocated);
    }
    if (use_new_block) {
        uint64_t filesize = ntoh64(* db->kv_filesize);
        filesize += 8 + 4 + 1 + 8 + 8 + block_size;
        (* db->kv_filesize) = hton64(filesize);
    }
    
    return offset;
}
