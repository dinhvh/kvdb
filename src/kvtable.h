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

int kv_table_header_write(kvdb * db, uint64_t table_start, uint64_t maxcount);
uint64_t kv_table_create(kvdb * db, uint64_t size, struct kvdb_table ** result);

int kv_tables_setup(kvdb * db);
void kv_tables_unsetup(kvdb * db);

static inline int kv_select_table(kvdb * db)
{
    if (db->kv_current_table == NULL) {
        db->kv_current_table = db->kv_first_table;
    }
    
    //fprintf(stderr, "count %i\n", (int) (* db->kv_current_table->kv_count));
    while (ntoh64(* db->kv_current_table->kv_count) > ntoh64(* db->kv_current_table->kv_maxcount) * KV_MAX_MEAN_COLLISION) {
        if (db->kv_current_table->kv_next_table == NULL) {
            uint64_t nextsize = kv_getnextprime(ntoh64(* db->kv_current_table->kv_maxcount) * 2);
            uint64_t offset = kv_table_create(db, nextsize, &db->kv_current_table->kv_next_table);
            if (offset == 0) {
                return -1;
            }
            * db->kv_current_table->kv_next_table_offset = hton64(offset);
        }
        
        db->kv_current_table = db->kv_current_table->kv_next_table;
    }
    
    return 0;
}


#endif
