#import <Foundation/Foundation.h>

@class KVOrderedDatabaseIterator;

@interface KVOrderedDatabase : NSObject

@property (nonatomic, copy, readonly) NSString *path;

- (id) initWithPath:(NSString *)path;

- (BOOL) open;
- (void) close;
- (BOOL) flush;

- (NSData *) dataForKey:(NSString *)key;
- (BOOL) setData:(NSData *)data forKey:(NSString *)key;
- (void) removeDataForKey:(NSString *)key;

- (KVOrderedDatabaseIterator *) keyIterator;

@end

@interface KVOrderedDatabaseIterator : NSObject

- (void) seekToFirstKey;
- (void) seekToLastKey;
- (void) seekAfterKey:(NSString *)key;

- (void) next;
- (void) previous;

- (NSString *) currentKey;
- (BOOL) isValid;

@end

