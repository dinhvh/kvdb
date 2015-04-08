#import <Foundation/Foundation.h>

@class KVOrderedDatabaseIterator;

@interface KVOrderedDatabase : NSObject

@property (nonatomic, copy, readonly) NSString *path;

// Create a ordered key value store.
- (id) initWithPath:(NSString *)path;

// Opens the database.
- (BOOL) open;

// Closes the database.
- (void) close;

// Start a transaction.
- (void) beginTransaction;

// Commit the transaction.
- (BOOL) commitTransaction;

// Abort the transaction.
- (void) abortTransaction;

// Returns the data associated with the key.
- (NSData *) dataForKey:(NSString *)key;

// Sets the data to associate with a key.
- (BOOL) setData:(NSData *)data forKey:(NSString *)key;

// Remove the given key.
- (void) removeDataForKey:(NSString *)key;

// Returns an efficient ordered iterator.
// The order is lexicographical.
- (KVOrderedDatabaseIterator *) keyIterator;

@end

@interface KVOrderedDatabaseIterator : NSObject

// Seeks to the first key.
- (void) seekToFirstKey;

// Seeks to the last key.
- (void) seekToLastKey;

// Seeks to the key larger or equal to the given key.
- (void) seekAfterKey:(NSString *)key;

// Iterate to the next key.
- (void) next;

// Iterate to the previous key.
- (void) previous;

// Returns the current key.
- (NSString *) currentKey;

// Returns whether the iterator is at a valid location.
- (BOOL) isValid;

@end

