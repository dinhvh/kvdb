#import <Foundation/Foundation.h>

typedef enum {
    KVIndexerSearchKindPrefix, // Search documents that has strings that start with the given token.
    KVIndexerSearchKindSubstr, // Search documents that has strings that contain the given token.
    KVIndexerSearchKindSuffix, // Search documents that has strings that end the given token.
} KVIndexerSearchKind;
// KVIndexerSearchKindPrefix provides the best performance.

@interface KVIndexer : NSObject

@property (nonatomic, copy, readonly) NSString *path;

// Create a full text indexer.
- (id) initWithPath:(NSString *)path;

// Opens the indexer.
- (BOOL) open;

// Closes the indexer.
- (void) close;

// Start a transaction.
- (void) beginTransaction;

// Commit the transaction.
- (BOOL) commitTransaction;

// Abort the transaction.
- (void) abortTransaction;

// Add a document to the indexer. string is the content to index.
// the string will be tokenized.
// The document is designated by an identifier docID.
- (BOOL) setString:(NSString *)string forDocID:(uint64_t)docID;

// Add a document to the indexer. strings is the result of a custom tokenizer.
// It's the list of tokens to index.
// The document is designated by an identifier docID.
- (BOOL) setStrings:(NSArray * /* NSString */)strings forDocID:(uint64_t)docID;

// Remove a document from the indexer.
- (void) removeDocID:(uint64_t)docID;

// Search a token. Returns a list of documents IDs.
- (NSArray *) search:(NSString *)token kind:(KVIndexerSearchKind)kind;

@end
