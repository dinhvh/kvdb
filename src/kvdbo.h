#ifndef KVDBO_H

#define KVDBO_H

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

// kvdbo is like kvdb except it maintains an efficient ordered list of keys.
// It will let you iterate on the list of keys.

typedef struct kvdbo kvdbo;

// create a kvdbo.
kvdbo * kvdbo_new(const char * filename);
// destroy a kvdbo.
void kvdbo_free(kvdbo * db);

// opens a kvdbo.
int kvdbo_open(kvdbo * db);
// closes a kvdbo.
void kvdbo_close(kvdbo * db);

// write pending changes.
int kvdbo_flush(kvdbo * db);

// insert a key / value. if the key already exists, it's replaced.
// Returns -2 if there's a I/O error.
// kvdbo_flush() must be called to write on disk all pending changes.
int kvdbo_set(kvdbo * db, const char * key, size_t key_size,
              const char * value, size_t value_size);

// retrieve the value for the given key.
// result stored in p_value should be released using free().
// Returns -1 if item is not found.
// Returns -2 if there's a I/O error.
// kvdbo_flush() must be called to write on disk all pending changes.
int kvdbo_get(kvdbo * db, const char * key, size_t key_size,
              char ** p_value, size_t * p_value_size);

// remove the given key.
// Returns -1 if item is not found.
// Returns -2 if there's a I/O error.
// kvdbo_flush() must be called to write on disk all pending changes.
int kvdbo_delete(kvdbo * db, const char * key, size_t key_size);

typedef struct kvdbo_iterator kvdbo_iterator;

// create an iterator on the given kvdbo (order is lexicographic).
kvdbo_iterator * kvdbo_iterator_new(kvdbo * db);

// destroy an iterator.
void kvdbo_iterator_free(kvdbo_iterator * iterator);

// seek to the first key.
void kvdbo_iterator_seek_first(kvdbo_iterator * iterator);

// seek to the position of the given key or after.
void kvdbo_iterator_seek_after(kvdbo_iterator * iterator, const char * key, size_t key_size);

// seek to the last key.
void kvdbo_iterator_seek_last(kvdbo_iterator * iterator);

// seek to the next key.
void kvdbo_iterator_next(kvdbo_iterator * iterator);

// seek to the previous key.
void kvdbo_iterator_previous(kvdbo_iterator * iterator);

// returns the key at the position of the iterator.
// result is valid until the next call to any iterator function.
void kvdbo_iterator_get_key(kvdbo_iterator * iterator, const char ** p_key, size_t * p_key_size);

// returns whether the iterator is valid.
int kvdbo_iterator_is_valid(kvdbo_iterator * iterator);

#ifdef __cplusplus
}
#endif

#endif
