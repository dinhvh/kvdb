#ifndef LIDX_H

#define LIDX_H

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>
#include <stdlib.h>

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

// Create a new indexer.
sfts * sfts_new(void);

// Release resource of the new indexer.
void sfts_free(sfts * index);

// Open the indexer.
int sfts_open(sfts * index, const char * filename);

// Close the indexer.
void sfts_close(sfts * index);

// Adds a UTF-8 document to the indexer.
// `doc`: document identifier (numerical identifier in a 64-bits range)
// `text`: content of the document in UTF-8 encoding.
int sfts_set(sfts * index, uint64_t doc, const char * text);

// Adds an unicode document to the indexer.
// `utext`: content of the document in UTF-16 encoding.
int sfts_u_set(sfts * index, uint64_t doc, const UChar * utext);

// Adds a UTF-8 document to the indexer.
// `doc`: document identifier (numerical identifier in a 64-bits range)
int sfts_set2(sfts * index, uint64_t doc, char * const * text, int count);

// Adds an unicode document to the indexer.
int sfts_u_set2(sfts * index, uint64_t doc, UChar * const * utext, int count);

// Removes a document from the indexer.
int sfts_remove(sfts * index, uint64_t doc);

// Searches a UTF-8 token in the indexer.
// `token`: string to search in UTF-8 encoding.
// `kind`: kind of matching to perform. See `lidx_search_kind`.
// The result is an array of documents IDs. The array is stored in `*p_docsids`.
// The number of items in the result array is stored in `*p_count`.
//
// The result array has to be freed using `free()`.
int sfts_search(sfts * index, const char * token, sfts_search_kind kind,
    uint64_t ** p_docsids, size_t * p_count);

// Searches a unicode token in the indexer.
// `token`: string to search in UTF-16 encoding.
int sfts_u_search(sfts * index, const UChar * utoken, sfts_search_kind kind,
    uint64_t ** p_docsids, size_t * p_count);

// Writes changes to disk if they are still pending in memory.
int sfts_flush(sfts * index);

#ifdef __cplusplus
}
#endif

#endif
