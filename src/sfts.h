#ifndef SFTS_H

#define SFTS_H

#include <inttypes.h>
#include <stdlib.h>

#include "kvdb-types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Create a new indexer.
sfts * sfts_new(const char * filename);

// Release resource of the new indexer.
void sfts_free(sfts * index);

// returns the filename of the indexer.
const char * sfts_get_filename(sfts * index);

// Open the indexer.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_open(sfts * index);

// Close the indexer.
// All changes will be written upon sfts_close().
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_close(sfts * index);

// Adds a UTF-8 document to the indexer.
// `doc`: document identifier (numerical identifier in a 64-bits range)
// `text`: content of the document in UTF-8 encoding.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_set(sfts * index, uint64_t doc, const char * text);

// Adds an unicode document to the indexer.
// `utext`: content of the document in UTF-16 encoding.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_u_set(sfts * index, uint64_t doc, const UChar * utext);

// Adds a UTF-8 document to the indexer.
// `doc`: document identifier (numerical identifier in a 64-bits range)
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_set2(sfts * index, uint64_t doc, const char ** text, int count);

// Adds an unicode document to the indexer.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_u_set2(sfts * index, uint64_t doc, const UChar ** utext, int count);

// Removes a document from the indexer.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// Returns KVDB_ERROR_NOT_FOUND if item is not found.
// See error codes in kvdb-types.h.
int sfts_remove(sfts * index, uint64_t doc);

// Searches a UTF-8 token in the indexer.
// `token`: string to search in UTF-8 encoding.
// `kind`: kind of matching to perform. See `lidx_search_kind`.
// The result is an array of documents IDs. The array is stored in `*p_docsids`.
// The number of items in the result array is stored in `*p_count`.
//
// The result array has to be freed using `free()`.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_search(sfts * index, const char * token, sfts_search_kind kind,
    uint64_t ** p_docsids, size_t * p_count);

// Searches a unicode token in the indexer.
// `token`: string to search in UTF-16 encoding.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_u_search(sfts * index, const UChar * utoken, sfts_search_kind kind,
    uint64_t ** p_docsids, size_t * p_count);

// creates a new transaction.
void sfts_transaction_begin(sfts * index);

// abort the transaction.
void sfts_transaction_abort(sfts * index);

// commit the transaction to disk.
//
// Returns KVDB_ERROR_NONE when it succeeded.
// See error codes in kvdb-types.h.
int sfts_transaction_commit(sfts * index);

#ifdef __cplusplus
}
#endif

#endif
