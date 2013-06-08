#ifndef KVDB_H

#define KVDB_H

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kvdb kvdb;

kvdb * kvdb_new(const char * filename);
void kvdb_free(kvdb * db);

int kvdb_open(kvdb * db);
void kvdb_close(kvdb * db);

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

#ifdef __cplusplus
}
#endif

#endif
