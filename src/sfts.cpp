#include "sfts.h"

#include <stdlib.h>

#include "kvdbo.h"

#include "kvunicode.h"
#include "kvserialization.h"

#include <set>
#include <map>

#if __APPLE__
#include <CoreFoundation/CoreFoundation.h>
#endif

static int db_put(sfts * index, std::string & key, std::string & value);
static int db_get(sfts * index, std::string & key, std::string * p_value);
static int db_delete(sfts * index, std::string & key);
static int db_flush(sfts * index);
static int tokenize(sfts * index, uint64_t doc, const UChar * text);
static int add_to_indexer(sfts * index, uint64_t doc, const char * word,
                          std::set<uint64_t> & wordsids_set);

// . -> next word id
// ,[docid] -> [words ids]
// /[word id] -> word
// word -> [word id], [docs ids]

struct sfts {
    kvdbo * sfts_db;
    std::map<std::string, std::string> sfts_buffer;
    std::set<std::string> sfts_buffer_dirty;
    std::set<std::string> sfts_deleted;
};

sfts * sfts_new(void)
{
    sfts * result = new sfts;
    result->sfts_db = NULL;
    return result;
}

void sfts_free(sfts * index)
{
    free(index);
}

int sfts_open(sfts * index, const char * filename)
{
    index->sfts_db = kvdbo_new(filename);
    kvdbo_open(index->sfts_db);
    
    return 0;
}

void sfts_close(sfts * index)
{
    if (index->sfts_db == NULL) {
        return;
    }
    db_flush(index);
    kvdbo_close(index->sfts_db);
    kvdbo_free(index->sfts_db);
    index->sfts_db = NULL;
}

int sfts_flush(sfts * index)
{
    return db_flush(index);
}

//int lidx_set(lidx * index, uint64_t doc, const char * text);
// text -> wordboundaries -> transliterated word -> store word with new word id
// word -> append doc id to docs ids
// store doc id -> words ids

int sfts_set(sfts * index, uint64_t doc, const char * text)
{
    UChar * utext = kv_from_utf8(text);
    int r = sfts_u_set(index, doc, utext);
    free(utext);
    return r;
}

int sfts_set2(sfts * index, uint64_t doc, const char ** text, int count)
{
    UChar ** utext = (UChar ** ) malloc(count * sizeof(* utext));
    for(int i = 0 ; i < count ; i ++) {
        utext[i] = kv_from_utf8(text[i]);
    }
    int result = sfts_u_set2(index, doc, utext, count);
    for(int i = 0 ; i < count ; i ++) {
        free((void *) utext[i]);
    }
    free((void *) utext);
    return result;
}

int sfts_u_set(sfts * index, uint64_t doc, const UChar * utext)
{
    int r = sfts_remove(index, doc);
    if (r < 0) {
        return r;
    }
    r = tokenize(index, doc, utext);
    if (r < 0) {
        return r;
    }
    return 0;
}

int sfts_u_set2(sfts * index, uint64_t doc, UChar * const * utext, int count)
{
    int r = sfts_remove(index, doc);
    if (r < 0) {
        return r;
    }
    int result = 0;
    std::set<uint64_t> wordsids_set;
    for(unsigned int i = 0 ; i < count ; i ++) {
        char * transliterated = kv_transliterate(utext[i], kv_u_get_length(utext[i]));
        if (transliterated == NULL) {
            continue;
        }
        int r = add_to_indexer(index, doc, transliterated, wordsids_set);
        if (r < 0) {
            result = r;
            break;
        }
        free(transliterated);
    }
    if (result != 0) {
        return result;
    }
    
    std::string key(",");
    kv_encode_uint64(key, doc);
    
    std::string value_str;
    for(std::set<uint64_t>::iterator wordsids_set_iterator = wordsids_set.begin() ; wordsids_set_iterator != wordsids_set.end() ; ++ wordsids_set_iterator) {
        kv_encode_uint64(value_str, * wordsids_set_iterator);
    }
    r = db_put(index, key, value_str);
    if (r < 0) {
        return r;
    }
    
    return 0;
}

static int tokenize(sfts * index, uint64_t doc, const UChar * text)
{
    int result = 0;
    std::set<uint64_t> wordsids_set;
#if __APPLE__
    unsigned int len = kv_u_get_length(text);
    CFStringRef str = CFStringCreateWithBytes(NULL, (const UInt8 *) text, len * sizeof(* text), kCFStringEncodingUTF16LE, false);
    CFStringTokenizerRef tokenizer = CFStringTokenizerCreate(NULL, str, CFRangeMake(0, len), kCFStringTokenizerUnitWord, NULL);
    while (1) {
        CFStringTokenizerTokenType wordKind = CFStringTokenizerAdvanceToNextToken(tokenizer);
        if (wordKind == kCFStringTokenizerTokenNone) {
            break;
        }
        if (wordKind == kCFStringTokenizerTokenHasNonLettersMask) {
            continue;
        }
        CFRange range = CFStringTokenizerGetCurrentTokenRange(tokenizer);
        char * transliterated = kv_transliterate(&text[range.location], (int) range.length);
        if (transliterated == NULL) {
            continue;
        }
        int r = add_to_indexer(index, doc, transliterated, wordsids_set);
        if (r < 0) {
            result = r;
            break;
        }
        
        free(transliterated);
    }
    CFRelease(str);
    CFRelease(tokenizer);
#else
    UErrorCode status;
    status = U_ZERO_ERROR;
    UBreakIterator * iterator = ubrk_open(UBRK_WORD, NULL, text, u_strlen(text), &status);
    LIDX_ASSERT(status <= U_ZERO_ERROR);
    
    int32_t left = 0;
    int32_t right = 0;
    int word_kind = 0;
    ubrk_first(iterator);
    
    while (1) {
        left = right;
        right = ubrk_next(iterator);
        if (right == UBRK_DONE) {
            break;
        }
        
        word_kind = ubrk_getRuleStatus(iterator);
        if (word_kind == 0) {
            // skip punctuation and space.
            continue;
        }
        
        char * transliterated = lidx_transliterate(&text[left], right - left);
        if (transliterated == NULL) {
            continue;
        }
        int r = add_to_indexer(index, doc, transliterated, wordsids_set);
        if (r < 0) {
            result = r;
            break;
        }
        
        free(transliterated);
    }
    ubrk_close(iterator);
#endif
    if (result != 0) {
        return result;
    }
    
    std::string key(",");
    kv_encode_uint64(key, doc);
    
    std::string value_str;
    for(std::set<uint64_t>::iterator wordsids_set_iterator = wordsids_set.begin() ; wordsids_set_iterator != wordsids_set.end() ; ++ wordsids_set_iterator) {
        kv_encode_uint64(value_str, * wordsids_set_iterator);
    }
    int r = db_put(index, key, value_str);
    if (r < 0) {
        return r;
    }
    
    return 0;
}

static int add_to_indexer(sfts * index, uint64_t doc, const char * word,
                          std::set<uint64_t> & wordsids_set)
{
    std::string word_str(word);
    std::string value;
    uint64_t wordid;
    
    //fprintf(stderr, "adding word: %s\n", word);
    
    int r = db_get(index, word_str, &value);
    if (r < -1) {
        return -1;
    }
    if (r == 0) {
        // Adding doc id to existing entry.
        kv_decode_uint64(value, 0, &wordid);
        kv_encode_uint64(value, doc);
        int r = db_put(index, word_str, value);
        if (r < 0) {
            return r;
        }
    }
    else /* r == -1 */ {
        // Not found.
        
        // Creating an entry.
        // store word with new id
        
        // read next word it
        std::string str;
        std::string nextwordidkey(".");
        int r = db_get(index, nextwordidkey, &str);
        if (r == -1) {
            wordid = 0;
        }
        else if (r < 0) {
            return -1;
        }
        else {
            kv_decode_uint64(str, 0, &wordid);
        }
        
        // write next word id
        std::string value;
        uint64_t next_wordid = wordid;
        next_wordid ++;
        kv_encode_uint64(value, next_wordid);
        r = db_put(index, nextwordidkey, value);
        if (r < 0) {
            return r;
        }
        
        std::string value_str;
        kv_encode_uint64(value_str, wordid);
        kv_encode_uint64(value_str, doc);
        r = db_put(index, word_str, value_str);
        if (r < 0) {
            return r;
        }
        
        std::string key("/");
        kv_encode_uint64(key, wordid);
        r = db_put(index, key, word_str);
        if (r < 0) {
            return r;
        }
    }
    
    wordsids_set.insert(wordid);
    
    return 0;
}

//int lidx_remove(lidx * index, uint64_t doc);
// docid -> words ids -> remove docid from word
// if docs ids for word is empty, we remove the word id

static std::string get_word_for_wordid(sfts * index, uint64_t wordid);
static int remove_docid_in_word(sfts * index, std::string word, uint64_t doc);
static int remove_word(sfts * index, std::string word, uint64_t wordid);

int sfts_remove(sfts * index, uint64_t doc)
{
    std::string key(",");
    kv_encode_uint64(key, doc);
    std::string str;
    int r = db_get(index, key, &str);
    if (r == -1) {
        // do nothing
    }
    else if (r < 0) {
        return -1;
    }
    
    size_t position = 0;
    while (position < str.size()) {
        uint64_t wordid;
        position = kv_decode_uint64(str, position, &wordid);
        std::string word = get_word_for_wordid(index, wordid);
        if (word.size() == 0) {
            continue;
        }
        int r = remove_docid_in_word(index, word, doc);
        if (r < 0) {
            return -1;
        }
    }
    
    return 0;
}

static std::string get_word_for_wordid(sfts * index, uint64_t wordid)
{
    std::string wordidkey("/");
    kv_encode_uint64(wordidkey, wordid);
    std::string str;
    int r = db_get(index, wordidkey, &str);
    if (r < 0) {
        return std::string();
    }
    return str;
}

static int remove_docid_in_word(sfts * index, std::string word, uint64_t doc)
{
    std::string str;
    int r = db_get(index, word, &str);
    if (r == -1) {
        return 0;
    }
    else if (r < 0) {
        return -1;
    }
    
    uint64_t wordid;
    std::string buffer;
    size_t position = 0;
    position = kv_decode_uint64(str, position, &wordid);
    while (position < buffer.size()) {
        uint64_t current_docid;
        position = kv_decode_uint64(str, position, &current_docid);
        if (current_docid != doc) {
            kv_encode_uint64(buffer, current_docid);
        }
    }
    if (buffer.size() == 0) {
        // remove word entry
        int r = remove_word(index, word, wordid);
        if (r < 0) {
            return -1;
        }
    }
    else {
        // update word entry
        int r = db_put(index, word, buffer);
        if (r < 0) {
            return r;
        }
    }
    
    return 0;
}

static int remove_word(sfts * index, std::string word, uint64_t wordid)
{
    std::string wordidkey("/");
    kv_encode_uint64(wordidkey, wordid);
    int r;
    r = db_delete(index, wordidkey);
    if (r < 0) {
        return -1;
    }
    r = db_delete(index, word);
    if (r < 0) {
        return -1;
    }
    
    return 0;
}

//int lidx_search(lidx * index, const char * token);
// token -> transliterated token -> docs ids

int sfts_search(sfts * index, const char * token, sfts_search_kind kind, uint64_t ** p_docsids, size_t * p_count)
{
    int result;
    UChar * utoken = kv_from_utf8(token);
    result = sfts_u_search(index, utoken, kind, p_docsids, p_count);
    free((void *) utoken);
    return result;
}

int sfts_u_search(sfts * index, const UChar * utoken, sfts_search_kind kind,
                  uint64_t ** p_docsids, size_t * p_count)
{
    db_flush(index);
    
    char * transliterated = kv_transliterate(utoken, -1);
    unsigned int transliterated_length = (unsigned int) strlen(transliterated);
    std::set<uint64_t> result_set;
    
    kvdbo_iterator * iterator = kvdbo_iterator_new(index->sfts_db);
    if (kind == sfts_search_kind_prefix) {
        kvdbo_iterator_seek_after(iterator, transliterated, strlen(transliterated));
    }
    else {
        kvdbo_iterator_seek_first(iterator);
    }
    while (kvdbo_iterator_is_valid(iterator)) {
        int add_to_result = 0;
        
        const char * key;
        size_t key_size;
        kvdbo_iterator_get_key(iterator, &key, &key_size);
        std::string key_str(key, key_size);
        if (key_str.find(".") == 0 || key_str.find(",") == 0 || key_str.find("/") == 0) {
            kvdbo_iterator_next(iterator);
            continue;
        }
        if (kind == sfts_search_kind_prefix) {
            if (key_str.find(transliterated) != 0) {
                break;
            }
            add_to_result = 1;
        }
        else if (kind == sfts_search_kind_substr) {
            //fprintf(stderr, "matching: %s %s\n", key_str.c_str(), transliterated);
            if (key_str.find(transliterated) != std::string::npos) {
                add_to_result = 1;
            }
        }
        else if (kind == sfts_search_kind_suffix) {
            if ((key_str.length() >= transliterated_length) &&
                (key_str.compare(key_str.length() - transliterated_length, transliterated_length, transliterated) == 0)) {
                add_to_result = 1;
            }
        }
        if (add_to_result) {
            size_t position = 0;
            uint64_t wordid;
            char * value;
            size_t value_size;
            int r = kvdbo_get(index->sfts_db, key_str.c_str(), key_str.length(), &value, &value_size);
            if (r != 0) {
                fprintf(stderr, "VALUE NOT FOUND for key %s\n", key_str.c_str());
            }
            std::string value_str(value, value_size);
            free(value);
            position = kv_decode_uint64(value_str, position, &wordid);
            while (position < value_str.size()) {
                uint64_t docid;
                position = kv_decode_uint64(value_str, position, &docid);
                result_set.insert(docid);
            }
        }
        
        kvdbo_iterator_next(iterator);
    }
    kvdbo_iterator_free(iterator);
    
    free(transliterated);
    
    uint64_t * result = (uint64_t *) calloc(result_set.size(), sizeof(* result));
    unsigned int count = 0;
    for(std::set<uint64_t>::iterator set_iterator = result_set.begin() ; set_iterator != result_set.end() ; ++ set_iterator) {
        result[count] = * set_iterator;
        count ++;
    }
    
    * p_docsids = result;
    * p_count = count;
    
    return 0;
}

static int db_put(sfts * index, std::string & key, std::string & value)
{
    index->sfts_deleted.erase(key);
    index->sfts_buffer[key] = value;
    index->sfts_buffer_dirty.insert(key);
    
    return 0;
}

static int db_get(sfts * index, std::string & key, std::string * p_value)
{
    if (index->sfts_deleted.find(key) != index->sfts_deleted.end()) {
        return -1;
    }
    
    if (index->sfts_buffer.find(key) != index->sfts_buffer.end()) {
        * p_value = index->sfts_buffer[key];
        return 0;
    }
    
    char * value;
    size_t value_size;
    int r = kvdbo_get(index->sfts_db, key.c_str(), key.length(), &value, &value_size);
    if (r != 0) {
        return r;
    }
    * p_value = std::string(value, value_size);
    index->sfts_buffer[key] = * p_value;
    return 0;
}

static int db_delete(sfts * index, std::string & key)
{
    index->sfts_deleted.insert(key);
    index->sfts_buffer_dirty.erase(key);
    index->sfts_buffer.erase(key);
    return 0;
}

static int db_flush(sfts * index)
{
    if ((index->sfts_buffer_dirty.size() == 0) && (index->sfts_deleted.size() == 0)) {
        return 0;
    }
    for(std::set<std::string>::iterator set_iterator = index->sfts_buffer_dirty.begin() ; set_iterator != index->sfts_buffer_dirty.end() ; ++ set_iterator) {
        std::string key = * set_iterator;
        std::string value = index->sfts_buffer[key];
        kvdbo_set(index->sfts_db, key.c_str(), key.length(), value.c_str(), value.length());
    }
    for(std::set<std::string>::iterator set_iterator = index->sfts_deleted.begin() ; set_iterator != index->sfts_deleted.end() ; ++ set_iterator) {
        std::string key = * set_iterator;
        kvdbo_delete(index->sfts_db, key.c_str(), key.length());
    }
    kvdbo_flush(index->sfts_db);
    index->sfts_buffer.clear();
    index->sfts_buffer_dirty.clear();
    index->sfts_deleted.clear();
    return 0;
}
