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
      fprintf(stderr, "value remove\n");
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
