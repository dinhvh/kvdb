//
//  kvtable.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/2/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef kvdb_kvtable_h
#define kvdb_kvtable_h

#include <stdlib.h>

#include "kvtypes.h"
#include "kvendian.h"
#include "kvprime.h"

#ifdef __cplusplus
extern "C" {
#endif

int kv_table_header_write(kvdb * db, uint64_t table_start, uint64_t maxcount);
uint64_t kv_table_create(kvdb * db, uint64_t size, struct kvdb_table ** result);
int kv_map_table(kvdb * db, struct kvdb_table ** result, uint64_t offset);

int kv_tables_setup(kvdb * db);
void kv_tables_unsetup(kvdb * db);

#ifdef __cplusplus
}
#endif

#endif
