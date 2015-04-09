#import "KVOrderedDatabase.h"

#include "kvdbo.h"

enum {
    KVDBIOErrorCode = -2,
    KVDBNotFoundErrorCode = -1,
};

@interface KVOrderedDatabaseIterator ()

- (id) initWithDatabase:(KVOrderedDatabase *)database;

@end

@implementation KVOrderedDatabase {
    kvdbo * _db;
    NSString * _path;
}

- (id) initWithPath:(NSString *)path
{
    self = [super init];
    _path = [path copy];
    _db = kvdbo_new([path fileSystemRepresentation]);
    return self;
}

- (void) dealloc
{
    kvdbo_free(_db);
}

- (NSString *) path
{
    return _path;
}

- (BOOL) open
{
    int r = kvdbo_open(_db);
    if (r < 0) {
        return NO;
    }
    return YES;
}

- (void) close
{
    kvdbo_close(_db);
}

- (NSData *) dataForKey:(NSString *)key
{
    const char * cKey = [key UTF8String];
    char * value = NULL;
    size_t value_size;
    int code = kvdbo_get(_db, cKey, strlen(cKey), &value, &value_size);
    if (code == KVDBIOErrorCode) {
        NSLog(@"[%@]: I/O error reading key \"%@\"", self, key);
        return nil;
    }
    else if (code < 0) {
        return nil;
    }
    else {
        return [NSData dataWithBytesNoCopy:value length:value_size freeWhenDone:YES];
    }
}

- (BOOL) setData:(NSData *)data forKey:(NSString *)key
{
    const char * cKey = [key UTF8String];
    int code = kvdbo_set(_db, cKey, strlen(cKey), [data bytes], [data length]);
    if (code == KVDBIOErrorCode) {
        NSLog(@"[%@]: I/O error writing key \"%@\"", self, key);
        return NO;
    }
    else if (code < 0) {
        return NO;
    }
    else {
        return YES;
    }
}

- (void) removeDataForKey:(NSString *)key
{
    const char * cKey = [key UTF8String];
    int code = kvdbo_delete(_db, cKey, strlen(cKey));
    if (code == KVDBIOErrorCode) {
        NSLog(@"[%@]: I/O error removing key \"%@\"", self, key);
    }
}

- (kvdbo *) _db
{
    return _db;
}

- (KVOrderedDatabaseIterator *) keyIterator
{
    return [[KVOrderedDatabaseIterator alloc] initWithDatabase:self];
}

- (void) beginTransaction
{
    kvdbo_transaction_begin(_db);
}

- (BOOL) commitTransaction
{
    int r = kvdbo_transaction_commit(_db);
    return r == 0;
}

- (void) abortTransaction
{
    kvdbo_transaction_abort(_db);
}

@end


@implementation KVOrderedDatabaseIterator {
    kvdbo_iterator * _iterator;
}

- (id) initWithDatabase:(KVOrderedDatabase *)database
{
    self = [super init];
    _iterator = kvdbo_iterator_new([database _db]);
    return self;
}

- (void) dealloc
{
    kvdbo_iterator_free(_iterator);
}

- (void) seekToFirstKey
{
    kvdbo_iterator_seek_first(_iterator);
}

- (void) seekToLastKey
{
    kvdbo_iterator_seek_last(_iterator);
}

- (void) seekAfterKey:(NSString *)key
{
    const char * cKey = [key UTF8String];
    kvdbo_iterator_seek_after(_iterator, cKey, strlen(cKey));
}

- (void) next
{
    kvdbo_iterator_next(_iterator);
}

- (void) previous
{
    kvdbo_iterator_previous(_iterator);
}

- (NSString *) currentKey
{
    const char * key;
    size_t size;
    kvdbo_iterator_get_key(_iterator, &key, &size);
    return [[NSString alloc] initWithBytes:key length:size encoding:NSUTF8StringEncoding];
}

- (BOOL) isValid
{
    return kvdbo_iterator_is_valid(_iterator);
}

@end

