//
//  kvblock.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/2/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef kvdb_kvblock_h
#define kvdb_kvblock_h

#include "kvtypes.h"

#ifdef __cplusplus
extern "C" {
#endif

uint64_t kv_block_create(kvdb * db, uint64_t next_block_offset, uint32_t hash_value,
                         const char * key, size_t key_size,
                         const char * value, size_t value_size);

int kv_block_recycle(kvdb * db, uint64_t offset);

#ifdef __cplusplus
}
#endif

#endif
