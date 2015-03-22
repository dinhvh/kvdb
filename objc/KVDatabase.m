#import "KVDatabase.h"

#include "kvdb.h"

enum {
    KVDBIOErrorCode = -2,
    KVDBNotFoundErrorCode = -1,
};

@interface KVDatabase ()

@property (nonatomic, copy) void(^enumerationBlock)(NSString *, BOOL *);

@end

@implementation KVDatabase {
    kvdb * _db;
}

- (id) initWithPath:(NSString *)path
{
    self = [super init];
    _path = [path copy];
    _db = kvdb_new([path fileSystemRepresentation]);
    return self;
}

- (BOOL) open
{
    int r = kvdb_open(_db);
    if (r < 0) {
        return NO;
    }
    return YES;
}

- (void) close
{
    kvdb_close(_db);
}

- (NSData *) dataForKey:(NSString *)key
{
    const char * cKey = [key UTF8String];
    char * value = NULL;
    size_t value_size;
    int code = kvdb_get(_db, cKey, strlen(cKey), &value, &value_size);
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
    int code = kvdb_set(_db, cKey, strlen(cKey), [data bytes], [data length]);
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
    int code = kvdb_delete(_db, cKey, strlen(cKey));
    if (code == KVDBIOErrorCode) {
        NSLog(@"[%@]: I/O error removing key \"%@\"", self, key);
    }
}

- (void)enumerateKeysAndValuesUsingBlock:(void(^)(NSString *key, BOOL * stop))block
{
    if (block == nil) {
        return;
    }
    self.enumerationBlock = block;
    kvdb_enumerate_keys(_db, enumeration_callback, (__bridge void *)self);
}

static void enumeration_callback(kvdb * db, struct kvdb_enumerate_cb_params * params,
                                 void * data, int * stop)
{
    KVDatabase * database = (__bridge id) data;
    NSString * key = [[NSString alloc] initWithBytes:params->key length:params->key_size encoding:NSUTF8StringEncoding];
    database.enumerationBlock(key, (BOOL *) stop);
}

@end
