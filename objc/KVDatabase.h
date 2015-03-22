#import <Foundation/Foundation.h>

@interface KVDatabase : NSObject

@property (nonatomic, copy, readonly) NSString *path;

- (id) initWithPath:(NSString *)path;

- (BOOL) open;
- (void) close;

- (NSData *) dataForKey:(NSString *)key;
- (BOOL) setData:(NSData *)data forKey:(NSString *)key;
- (void) removeDataForKey:(NSString *)key;

/**
 *  Enumerate all keys and values in the receiver.
 *
 *  @param block Block to be called on each enumerated value.
 */
- (void)enumerateKeysAndValuesUsingBlock:(void(^)(NSString *key, BOOL *stop))block;

@end
