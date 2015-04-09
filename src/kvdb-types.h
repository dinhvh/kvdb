//
//  kvdb-types.h
//  kvdb
//
//  Created by Hoa Dinh on 4/8/15.
//  Copyright (c) 2015 etpan. All rights reserved.
//

#ifndef kvdb_kvdb_types_h
#define kvdb_kvdb_types_h

#ifdef __cplusplus
extern "C" {
#endif

enum {
    KVDB_COMPRESSION_TYPE_RAW,
    KVDB_COMPRESSION_TYPE_LZ4,
};

enum {
    KVDB_ERROR_NONE = 0,
    KVDB_ERROR_NOT_FOUND = -1,
    KVDB_ERROR_IO = -2,
    KVDB_ERROR_CORRUPTED = -3,
    KVDB_ERROR_KEY_NOT_ALLOWED = -4,
    // KVDB_ERROR_INVALID_JOURNAL is an internal error.
    // The public API won't return it as a result.
    KVDB_ERROR_INVALID_JOURNAL = -5,
};


// kvdb

typedef struct kvdb kvdb;

struct kvdb_enumerate_cb_params {
    const char * key;
    size_t key_size;
};

typedef void kvdb_enumerate_callback(kvdb * db,
                                     struct kvdb_enumerate_cb_params * params,
                                     void * data, int * stop);


// kvdbo

typedef struct kvdbo kvdbo;

    
// sfts
    
// We're using the same UChar as the ICU library.
#if defined(__CHAR16_TYPE__)
    typedef __CHAR16_TYPE__ UChar;
#else
    typedef uint16_t UChar;
#endif
    
typedef struct sfts sfts;
    
// prefix provides the best performance, two other options
// have poor performance.
typedef enum sfts_search_kind {
    sfts_search_kind_prefix, // Search documents that has strings that start with the given token.
    sfts_search_kind_substr, // Search documents that has strings that contain the given token.
    sfts_search_kind_suffix, // Search documents that has strings that end the given token.
} sfts_search_kind;

#ifdef __cplusplus
}
#endif

#endif
