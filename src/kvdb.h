#ifndef KVDB_H

#define KVDB_H

#include <sys/types.h>

#include "kvdb-types.h"

#ifdef __cplusplus
extern "C" {
#endif

// kvdb is a key-value database.

// creates a kvdb.
kvdb * kvdb_new(const char * filename);

// destroys a kvdb.
void kvdb_free(kvdb * db);

// returns the filename of the kvdb.
const char * kvdb_get_filename(kvdb * db);

// sets the compression type for the values of the kvdb.
// default is KVDB_COMPRESSION_TYPE_LZ4.
void kvdb_set_compression_type(kvdb * db, int compression_type);

// gets the compression type for the values of the kvdb.
int kvdb_get_compression_type(kvdb * db);

// the database will be resistant to crashes (durability) if fsync() is enabled.
// it will be faster if fsync() is disabled.
void kvdb_set_fsync_enabled(kvdb * db, int enabled);

// returns whether fsync() is enabled.
int kvdb_is_fsync_enabled(kvdb * db);

// sets the write buffer size. Default is 0.
void kvdb_set_write_buffer_size(kvdb * db, size_t size);

// returns the write buffer size.
size_t kvdb_get_write_buffer_size(kvdb * db);

// opens a kvdb.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdb_open(kvdb * db);

// closes a kvdb.
// All changes will be written upon kvdb_close().
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdb_close(kvdb * db);

// creates a new transaction.
void kvdb_transaction_begin(kvdb * db);
    
// aborts the transaction.
void kvdb_transaction_abort(kvdb * db);

// commits the transaction to disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdb_transaction_commit(kvdb * db);

// inserts a key / value in the database,
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdb_set(kvdb * db, const char * key, size_t key_size,
             const char * value, size_t value_size);

// retrieves the value for the given key.
//
// result stored in p_value should be released using free().
// Returns KVDB_ERROR_NOT_FOUND if item is not found.
// Returns KVDB_ERROR_NONE when it succeeded.
// See other error codes in kvdb-types.h.
int kvdb_get(kvdb * db, const char * key, size_t key_size,
             char ** p_value, size_t * p_value_size);

// removes the given key.
//
// Returns KVDB_ERROR_NOT_FOUND if item is not found.
// Returns KVDB_ERROR_NONE when it succeeded.
// See other error codes in kvdb-types.h.
int kvdb_delete(kvdb * db, const char * key, size_t key_size);

// Iterates over the keys in the kvdb.
// This function will be highly inefficient since it's going to go through all
// the database on disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdb_enumerate_keys(kvdb * db, kvdb_enumerate_callback callback, void * cb_data);

#ifdef __cplusplus
}
#endif

#endif
