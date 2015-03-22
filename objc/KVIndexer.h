#import <Foundation/Foundation.h>

typedef enum {
    KVIndexerSearchKindPrefix, // Search documents that has strings that start with the given token.
    KVIndexerSearchKindSubstr, // Search documents that has strings that contain the given token.
    KVIndexerSearchKindSuffix, // Search documents that has strings that end the given token.
} KVIndexerSearchKind;
// KVIndexerSearchKindPrefix provides the best performance.

@interface KVIndexer : NSObject

@property (nonatomic, copy, readonly) NSString *path;

- (id) initWithPath:(NSString *)path;

- (BOOL) open;
- (void) close;
- (BOOL) flush;

- (BOOL) setString:(NSString *)string forDocID:(uint64_t)docID;
- (BOOL) setStrings:(NSArray * /* NSString */)strings forDocID:(uint64_t)docID;
- (void) removeDocID:(uint64_t)docID;

- (NSArray *) search:(NSString *)token kind:(KVIndexerSearchKind)kind;

@end
