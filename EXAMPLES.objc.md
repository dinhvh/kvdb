KVDatabase
==========

A Lightweight Key-Value Database.

- Use only one file
- Low memory usage
- Good performance

Example:

```objc
#include <stdio.h>
#include <kvdb/KVDatabase.h>

int main(int argc, char ** argv)
{
  KVDatabase * db;
  db = [[KVDatabase alloc] initWithPath:@"kvdb-test.kvdb"];
  [db open];

  [db setData:[NSData dataWithBytes:"some value" length:10] forKey:@"some key"];
  NSData * data = [db dataForKey:@"some other key"];
  NSLog(@"value; %@", data);

  [db removeDataForKey:@"yet another key"];
  
  [db close];
  exit(EXIT_SUCCESS);
}
```

KVOrderedDatabase
=================

A Lightweight ordered Key-Value Database.

- Use only one file
- Low memory usage
- Good performance
- Iteratable

Example:

```objc
#include <stdio.h>
#include <kvdb/KVOrderedDatabase.h>

int main(int argc, char ** argv)
{
  KVOrderedDatabase * db;
  db = [[KVOrderedDatabase alloc] initWithPath:@"kvdb-test.kvdb"];
  [db open];

  [db setData:[NSData dataWithBytes:"some value" length:10] forKey:@"some key"];
  NSData * data = [db dataForKey:@"some other key"];
  NSLog(@"value; %@", data);

  [db removeDataForKey:@"yet another key"];
  
  KVOrderedDatabaseIterator * iterator = [db keyIterator];
  [iterator seekToFirstKey];
  while ([iterator isValid]) {
    NSLog(@"key: %@", [iterator currentKey]);
    [iterator next];
  }
  
  [db close];
  exit(EXIT_SUCCESS);
}
```

KVIndexer
=========

A Simple Full Text Search.

- Use only one file
- Low memory usage
- Good performance
- Unicode support

Example:

```objc
#include <stdio.h>
#include <kvdb/KVIndexer.h>

int main(int argc, char ** argv)
{
  KVIndexer * indexer;

  // Opens the index.
  indexer = [[KVIndexer alloc] initWithPath:@"index.sfts"];

  // Adds data to the index.
  [indexer setString:@"George Washington" forDocID:0];
  [indexer setString:@"John Adams" forDocID:1];
  [indexer setString:@"Thomas Jefferson" forDocID:2];
  [indexer setString:@"George Michael" forDocID:3];
  [indexer setString:@"George Méliès" forDocID:4];

  // Search "geor".
  NSLog(@"searching geor");
  NSArray * result = [indexer search:@"geor" kind:KVIndexerSearchKindPrefix];
  NSLog(@"found: %@", result);

  // Search "mel".
  NSLog(@"searching mel");
  NSArray * result = [indexer search:@"mel" kind:KVIndexerSearchKindPrefix];
  NSLog(@"found: %@", result);

  [indexer close];
}
```
