//
//  KVDBTypes.h
//  kvdb
//
//  Created by Hoa Dinh on 4/9/15.
//  Copyright (c) 2015 etpan. All rights reserved.
//

#ifndef kvdb_KVDBTypes_h
#define kvdb_KVDBTypes_h

#include <kvdb/kvdb-types.h>

typedef enum KVDBError {
    KVDBErrorNone = KVDB_ERROR_NONE,
    KVDBErrorNotFound = KVDB_ERROR_NOT_FOUND,
    KVDBErrorIO = KVDB_ERROR_IO,
    KVDBErrorCorrupted = KVDB_ERROR_CORRUPTED,
    KVDBErrorKeyNotAllowed = KVDB_ERROR_KEY_NOT_ALLOWED,
} KVDBError;

#endif
