#ifndef KVDBO_H

#define KVDBO_H

#include <sys/types.h>

#include "kvdb-types.h"

#ifdef __cplusplus
extern "C" {
#endif

// kvdbo is like kvdb except it maintains an efficient ordered list of keys.
// It will let you iterate on the list of keys efficiently.

// create a kvdbo.
kvdbo * kvdbo_new(const char * filename);

// destroy a kvdbo.
void kvdbo_free(kvdbo * db);

// the database will be resistant to crashes (durability) if fsync() is enabled.
// it will be faster if fsync() is disabled.
void kvdbo_set_fsync_enabled(kvdbo * db, int enabled);

// returns whether fsync() is enabled.
int kvdbo_is_fsync_enabled(kvdbo * db);

// returns the filename of the kvdbo.
const char * kvdbo_get_filename(kvdbo * db);

// opens a kvdbo.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_open(kvdbo * db);

// closes a kvdbo.
// All changes will be written upon kvdbo_close().
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_close(kvdbo * db);

// insert a key / value. if the key already exists, it's replaced.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// Returns KVDB_ERROR_KEY_NOT_ALLOWED if the key is not allowed (starting with \0kvdbo).
// See error codes in kvdb-types.h.
int kvdbo_set(kvdbo * db, const char * key, size_t key_size,
              const char * value, size_t value_size);

// retrieve the value for the given key.
// result stored in p_value should be released using free().
//
// Returns KVDB_ERROR_NOT_FOUND if item is not found.
// Returns KVDB_ERROR_NONE when it succeeded.
// See other error codes in kvdb-types.h.
int kvdbo_get(kvdbo * db, const char * key, size_t key_size,
              char ** p_value, size_t * p_value_size);

// remove the given key.
//
// Returns KVDB_ERROR_NOT_FOUND if item is not found.
// Returns KVDB_ERROR_NONE when it succeeded.
// See other error codes in kvdb-types.h.
int kvdbo_delete(kvdbo * db, const char * key, size_t key_size);

typedef struct kvdbo_iterator kvdbo_iterator;

// create an iterator on the given kvdbo (order is lexicographical).
kvdbo_iterator * kvdbo_iterator_new(kvdbo * db);

// destroy an iterator.
void kvdbo_iterator_free(kvdbo_iterator * iterator);

// seek to the first key.
// You need to check whether the seek moved the iterator to a valid position using kvdbo_iterator_is_valid().
// This function might return an error since it might read on disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_iterator_seek_first(kvdbo_iterator * iterator);

// seek to the position of the given key or after.
// You need to check whether the seek moved the iterator to a valid position using kvdbo_iterator_is_valid().
// This function might return an error since it might read on disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_iterator_seek_after(kvdbo_iterator * iterator, const char * key, size_t key_size);

// seek to the last key.
// You need to check whether the seek moved the iterator to a valid position using kvdbo_iterator_is_valid().
// This function might return an error since it might read on disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_iterator_seek_last(kvdbo_iterator * iterator);

// seek to the next key.
// You need to check whether the seek moved the iterator to a valid position using kvdbo_iterator_is_valid().
// This function might return an error since it might read on disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_iterator_next(kvdbo_iterator * iterator);

// seek to the previous key.
// You need to check whether the seek moved the iterator to a valid position using kvdbo_iterator_is_valid().
// This function might return an error since it might read on disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_iterator_previous(kvdbo_iterator * iterator);

// returns the key at the position of the iterator.
// result is valid until the next call to any iterator function.
void kvdbo_iterator_get_key(kvdbo_iterator * iterator, const char ** p_key, size_t * p_key_size);

// returns whether the iterator is valid.
//
// Returns 1 if the iterator is on a valid position.
// Returns 0 if it's not.
int kvdbo_iterator_is_valid(kvdbo_iterator * iterator);

// creates a new transaction.
void kvdbo_transaction_begin(kvdbo * db);

// abort the transaction.
void kvdbo_transaction_abort(kvdbo * db);

// commit the transaction to disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int kvdbo_transaction_commit(kvdbo * db);

#ifdef __cplusplus
}
#endif

#endif
