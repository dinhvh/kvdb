#ifndef KVDB_H

#define KVDB_H

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

// kvdb is a key-value database.

typedef struct kvdb kvdb;

enum {
    KVDB_COMPRESSION_TYPE_RAW,
    KVDB_COMPRESSION_TYPE_LZ4,
};

// creates a kvdb.
kvdb * kvdb_new(const char * filename);

void kvdb_set_compression_type(kvdb * db, int compression_type);
int kvdb_get_compression_type(kvdb * db);

// destroy a kvdb.
void kvdb_free(kvdb * db);

// opens a kvdb.
int kvdb_open(kvdb * db);

// closes a kvdb.
void kvdb_close(kvdb * db);

// insert a key / value in the database.
// Returns -2 if there's a I/O error.
int kvdb_set(kvdb * db, const char * key, size_t key_size,
             const char * value, size_t value_size);

// result stored in p_value should be released using free().
// Returns -1 if item is not found.
// Returns -2 if there's a I/O error.
int kvdb_get(kvdb * db, const char * key, size_t key_size,
             char ** p_value, size_t * p_value_size);

// Returns -1 if item is not found.
// Returns -2 if there's a I/O error.
int kvdb_delete(kvdb * db, const char * key, size_t key_size);

struct kvdb_enumerate_cb_params {
	const char * key;
	size_t key_size;
};

typedef void kvdb_enumerate_callback(kvdb * db,
	                                 struct kvdb_enumerate_cb_params * params,
                                     void * data, int * stop);

// Returns -2 if there's a I/O error.
int kvdb_enumerate_keys(kvdb * db, kvdb_enumerate_callback callback, void * cb_data);


// optimizations for sfts
int kvdb_get2(kvdb * db, const char * key, size_t key_size,
              char ** p_value, size_t * p_value_size, size_t * p_free_size);

int kvdb_append(kvdb * db, const char * key, size_t key_size,
                const char * value, size_t value_size);

#ifdef __cplusplus
}
#endif

#endif
