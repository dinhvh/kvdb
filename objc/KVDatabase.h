#import <Foundation/Foundation.h>

@interface KVDatabase : NSObject

@property (nonatomic, copy, readonly) NSString *path;

// Create a key value store.
- (id) initWithPath:(NSString *)path;

// Opens the database.
- (BOOL) open;

// Closes the database.
- (void) close;

// Returns the data associated with the key.
- (NSData *) dataForKey:(NSString *)key;

// Sets the data to associate with a key.
- (BOOL) setData:(NSData *)data forKey:(NSString *)key;

// Remove the given key.
- (void) removeDataForKey:(NSString *)key;

// Enumerate all keys of the database.
// Be careful, this method will iterate over all the on-disk database, then
// will perform slowly.
- (void)enumerateKeysAndValuesUsingBlock:(void(^)(NSString *key, BOOL *stop))block;

@end
