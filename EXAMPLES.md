kvdb
====

A Lightweight Key-Value Database.

- Use only one file
- Low memory usage
- Good performance

Example:

```c
#include <stdio.h>
#include <kvdb/kvdb.h>

int main(int argc, char ** argv)
{
  struct kvdb * db;
  db = kvdb_new("kvdb-test.kvdb");
  kvdb_open(db);

  int r;

  char * key = "some key";
  char * value = "some value";
  r = kvdb_set(db, key, strlen(key), value, strlen(value));
  switch (r) {
    case 0:
      fprintf(stderr, "value stored\n");
      break;
      
    case -2:
      fprintf(stderr, "I/O error\n");
      break;
  }

  key = "some other key";
  char * read_value = NULL;
  size_t read_value_size = 0;
  r = kvdb_get(db, key, strlen(key), &read_value, &read_value_size);
  switch (r) {
    case 0:
      fprintf(stderr, "key: %.*s\n", (int) read_value, read_value_size);
      free(read_value);  
      break;
      
    case -1:
      fprintf(stderr, "not found\n");
      break;
      
    case -2:
      fprintf(stderr, "I/O error\n");
      break;
  }

  key = "yet another key";
  r = kvdb_delete(db, key, strlen(key));
  switch (r) {
    case 0:
      fprintf(stderr, "value removed\n");
      break;
      
    case -2:
      fprintf(stderr, "I/O error\n");
      break;
  }
  
  kvdb_close(db);
  kvdb_free(db);
  exit(EXIT_SUCCESS);
}
```

kvdbo
=====

A Lightweight ordered Key-Value Database.

- Use only one file
- Low memory usage
- Good performance
- Iteratable

Example:

```c
#include <stdio.h>
#include <kvdb/kvdbo.h>

int main(int argc, char ** argv)
{
  struct kvdbo * db;
  db = kvdbo_new("kvdb-test.kvdbo");
  kvdb_open(db);

  int r;

  char * key = "some key";
  char * value = "some value";
  r = kvdbo_set(db, key, strlen(key), value, strlen(value));
  switch (r) {
    case 0:
      fprintf(stderr, "value stored\n");
      break;
      
    case -2:
      fprintf(stderr, "I/O error\n");
      break;
  }

  key = "some other key";
  char * read_value = NULL;
  size_t read_value_size = 0;
  r = kvdbo_get(db, key, strlen(key), &read_value, &read_value_size);
  switch (r) {
    case 0:
      fprintf(stderr, "key: %.*s\n", (int) read_value, read_value_size);
      free(read_value);  
      break;
      
    case -1:
      fprintf(stderr, "not found\n");
      break;
      
    case -2:
      fprintf(stderr, "I/O error\n");
      break;
  }

  key = "yet another key";
  r = kvdbo_delete(db, key, strlen(key));
  switch (r) {
    case 0:
      fprintf(stderr, "value removed\n");
      break;
      
    case -2:
      fprintf(stderr, "I/O error\n");
      break;
  }
  
  struct kvdbo_iterator * iterator = kvdbo_iterator_new(db);
  kvdbo_iterator_seek_first(iterator);
  while (kvdbo_iterator_is_valid(iterator)) {
    const char * key;
    size_t size;
    kvdbo_iterator_get_key(iterator, &key, &size);
    printf("key: %.*s\n", size, key);
  }
  kvdbo_iterator_free(db);
  
  kvdbo_close(db);
  kvdbo_free(db);
  exit(EXIT_SUCCESS);
}
```

sfts
====

A Simple Full Text Search.

- Use only one file
- Low memory usage
- Good performance
- Unicode support

Example:

```c
#include <stdio.h>
#include <kvdb/sfts.h>

int main(int argc, char ** argv)
{
  sfts * indexer;
  int r;
  uint64_t * result;
  size_t result_count;

  // Opens the index.
  indexer = sfts_new();
  sfts_open(indexer, "index.sfts");

  // Adds data to the index.
  sfts_set(indexer, 0, "George Washington");
  sfts_set(indexer, 1, "John Adams");
  sfts_set(indexer, 2, "Thomas Jefferson");
  sfts_set(indexer, 3, "George Michael");
  sfts_set(indexer, 4, "George Méliès");

  // Search "geor".
  print("searching geor");
  sfts_search(indexer, "geor", sfts_search_kind_prefix, &result, &result_count);
  for(size_t i = 0 ; i < result_count ; i ++) {
    printf("found: %i\n", result[i]);
  }
  // returns 0, 3 and 4.
  free(result);

  // Search "mel".
  print("searching mel");
  sfts_search(indexer, "mel", sfts_search_kind_prefix, &result, &result_count);
  for(size_t i = 0 ; i < result_count ; i ++) {
    printf("found: %i\n", result[i]);
  }
  // return 4
  free(result);

  sfts_close(indexer);
  sfts_free(indexer);
}
```
