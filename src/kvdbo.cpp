#include "kvdbo.h"

#include "kvdb.h"
#include "kvendian.h"
#include "kvassert.h"
#include "kvserialization.h"

#include <set>
#include <string>
#include <vector>
#include <stdlib.h>
#include <string.h>

struct kvdbo {
    // underlaying kvdb.
    kvdb * db;
    bool opened;
    
    // in memory buffers for operations.
    std::set<std::string> pending_keys;
    std::set<std::string> pending_keys_delete;
    bool master_node_changed;
    // node identifier allocation.
    uint64_t next_node_id;
    
    // master node.
    // identifiers of the nodes.
    std::vector<uint64_t> nodes_ids;
    // first keys of the nodes.
    std::vector<std::string> nodes_first_keys;
    // number of keys in each node.
    std::vector<uint32_t> nodes_keys_count;
    
    // whether a transaction has been opened.
    bool in_transaction;
    // if a transaction is opened, whether it's an implicit transaction.
    bool implicit_transaction;
    // number of pending changes in the transaction.
    unsigned int implicit_transaction_op_count;

    bool checking_sorted;
};

// iterator over kvdbo.
// it will also cache the keys of the current node.
struct kvdbo_iterator {
    kvdbo * db;
    // identifier of the node.
    uint64_t node_id;
    // index of the node.
    unsigned int node_index;
    // keys in the node.
    std::vector<std::string> keys;
    // current key index in the node.
    int key_index;
    // error when trying to flush data.
    int flush_result;
};

#define NODE_PREFIX "n"

static void kvdbo_check_sorted(kvdbo * db);
static void kvdbo_check_first_keys(kvdbo * db);
static void show_nodes_content(kvdbo * db);

static int flush_pending_keys(kvdbo * db);
static int write_master_node(kvdbo * db);
static int read_master_node(kvdbo * db);
static unsigned int find_node(kvdbo * db, const std::string key);
static unsigned int find_key(kvdbo_iterator * iterator, const std::string key);
static void unserialize_words_list(std::vector<std::string> & word_list, char * value, size_t size);
static void serialize_words_list(std::string & value, std::vector<std::string> & word_list);
static int iterator_load_node(kvdbo_iterator * iterator, uint64_t node_id);
static void add_first_node(kvdbo * db);
static int load_node(struct modified_node * node, unsigned int node_index);
static int load_from_node_id(struct modified_node * node, uint64_t node_id);
static int write_loaded_node(struct modified_node * node);
static int write_single_loaded_node(struct modified_node * node);
static int try_merge(kvdbo * db, unsigned int node_index, bool * pDidMerge);
static int remove_node_id(kvdbo * db, uint64_t node_id);
static int remove_node(kvdbo * db, unsigned int node_index);
static int split_node(kvdbo * db, unsigned int node_index, unsigned int count,
                      std::set<std::string> & keys);
static int start_implicit_transaction_if_needed(kvdbo * db);
static void unserialize_keys(std::set<std::string> & keys, std::string & buffer);
static void node_unserialize_keys_to_vector(std::vector<std::string> & result,
                                            std::string buffer);
static void node_unserialize_keys(struct modified_node * node);
static void node_serialize_keys(struct modified_node * node);

static void show_nodes(kvdbo * db);

#pragma mark kvdbo data structure management.

kvdbo * kvdbo_new(const char* filename)
{
    kvdbo * db;
    db = new kvdbo;
    db->db = kvdb_new(filename);
    db->opened = false;
    db->next_node_id = 1;
    db->in_transaction = false;
    db->implicit_transaction = false;
    db->implicit_transaction_op_count = 0;
    db->checking_sorted = false;
    return db;
}

void kvdbo_free(kvdbo * db)
{
    if (db->opened) {
        fprintf(stderr, "kvdbo: %s should be closed before freeing\n", kvdbo_get_filename(db));
    }
    kvdb_free(db->db);
    delete db;
}

void kvdbo_set_fsync_enabled(kvdbo * db, int enabled)
{
    kvdb_set_fsync_enabled(db->db, enabled);
}

int kvdbo_is_fsync_enabled(kvdbo * db)
{
    return kvdb_is_fsync_enabled(db->db);
}

const char * kvdbo_get_filename(kvdbo * db)
{
    return kvdb_get_filename(db->db);
}

#pragma mark opening / closing the database.

int kvdbo_open(kvdbo * db)
{
    if (db->opened) {
        fprintf(stderr, "kvdbo: %s already opened\n", kvdbo_get_filename(db));
        return KVDB_ERROR_NONE;
    }
    
    int r = kvdb_open(db->db);
    if (r < 0) {
        return r;
    }
    r = read_master_node(db);
    if (r < 0) {
        kvdbo_close(db);
        return r;
    }
    db->opened = true;
    return KVDB_ERROR_NONE;
}

int kvdbo_close(kvdbo * db)
{
    int r;
    
    if (!db->opened) {
        fprintf(stderr, "kvdbo: %s not opened\n", kvdbo_get_filename(db));
        return KVDB_ERROR_NONE;
    }
    
    if (db->in_transaction) {
        if (!db->implicit_transaction) {
            fprintf(stderr, "kvdbo: transaction not closed properly.\n");
        }
        r = kvdbo_transaction_commit(db);
        if (r < 0) {
            return r;
        }
    }
    kv_assert(db->pending_keys.size() == 0 && db->pending_keys_delete.size() == 0);
    db->nodes_keys_count.clear();
    db->nodes_first_keys.clear();
    db->nodes_ids.clear();
    
    r = kvdb_close(db->db);
    // no error should happen since we closed any implicit transaction.
    kv_assert(r == KVDB_ERROR_NONE);
    db->opened = false;
    return KVDB_ERROR_NONE;
}

#pragma mark key insertion / deletion / retrieval.

const char METAKEY_PREFIX[7] = "\0kvdbo";
#define METAKEY_PREFIX_SIZE (sizeof(METAKEY_PREFIX) - 1)

int kvdbo_set(kvdbo * db,
              const char * key,
              size_t key_size,
              const char * value,
              size_t value_size)
{
    int r;
    
    r = start_implicit_transaction_if_needed(db);
    if (r < 0) {
        return r;
    }
    std::string key_str(key, key_size);
    if (key_str.find(std::string(METAKEY_PREFIX, METAKEY_PREFIX_SIZE)) == 0) {
        // invalid key.
        return KVDB_ERROR_KEY_NOT_ALLOWED;
    }
    r = kvdb_set(db->db, key, key_size, value, value_size);
    if (r < 0) {
        return r;
    }
    db->pending_keys_delete.erase(key_str);
    db->pending_keys.insert(key_str);
    db->implicit_transaction_op_count ++;
    return KVDB_ERROR_NONE;
}

int kvdbo_get(kvdbo * db,
              const char * key,
              size_t key_size,
              char ** p_value,
              size_t * p_value_size)
{
    if (db->pending_keys_delete.find(std::string(key, key_size)) != db->pending_keys_delete.end()) {
        return KVDB_ERROR_NOT_FOUND;
    }
    return kvdb_get(db->db, key, key_size, p_value, p_value_size);
}

int kvdbo_delete(kvdbo * db, const char* key, size_t key_size)
{
    int r;
    
    r = start_implicit_transaction_if_needed(db);
    if (r < 0) {
        return r;
    }
    std::string key_str(key, key_size);
    if (key_str.find(std::string(METAKEY_PREFIX, METAKEY_PREFIX_SIZE)) == 0) {
        // invalid key.
        return KVDB_ERROR_KEY_NOT_ALLOWED;
    }
    r = kvdb_delete(db->db, key, key_size);
    if (r < 0) {
        // not found or other error.
        return r;
    }
    db->pending_keys.erase(key_str);
    db->pending_keys_delete.insert(key_str);
    db->implicit_transaction_op_count ++;
    return KVDB_ERROR_NONE;
}

#pragma mark iterator management.

kvdbo_iterator * kvdbo_iterator_new(kvdbo * db)
{
    kvdbo_iterator * iterator = new kvdbo_iterator;
    iterator->key_index = -1;
    iterator->db = db;
    
    iterator->flush_result = flush_pending_keys(db);
    
    return iterator;
}

void kvdbo_iterator_free(kvdbo_iterator * iterator)
{
    delete iterator;
}

int kvdbo_iterator_seek_first(kvdbo_iterator * iterator)
{
    if (iterator->flush_result < 0) {
        return iterator->flush_result;
    }
    if (iterator->db->nodes_ids.size() == 0) {
        return KVDB_ERROR_NONE;;
    }
    uint64_t node_id = iterator->db->nodes_ids[0];
    iterator->node_index = 0;
    iterator->key_index = 0;
    int r = iterator_load_node(iterator, node_id);
    if (r < 0) {
        iterator->key_index = -1;
        return r;
    }
    return KVDB_ERROR_NONE;
}

int kvdbo_iterator_seek_last(kvdbo_iterator * iterator)
{
    if (iterator->flush_result < 0) {
        return iterator->flush_result;
    }
    if (iterator->db->nodes_ids.size() == 0) {
        return KVDB_ERROR_NONE;
    }
    uint64_t node_id = iterator->db->nodes_ids[iterator->db->nodes_ids.size() - 1];
    iterator->node_index = (unsigned int) (iterator->db->nodes_ids.size() - 1);
    iterator->key_index = 0;
    int r = iterator_load_node(iterator, node_id);
    if (r < 0) {
        iterator->key_index = -1;
        return r;
    }
    iterator->key_index = (unsigned int) (iterator->keys.size() - 1);
    return KVDB_ERROR_NONE;
}

int kvdbo_iterator_seek_after(kvdbo_iterator * iterator,
                               const char * key,
                               size_t key_size)
{
    if (iterator->flush_result < 0) {
        return iterator->flush_result;
    }
    if (iterator->db->nodes_ids.size() == 0) {
        return KVDB_ERROR_NONE;
    }
    std::string key_string(key, key_size);
    unsigned int idx = find_node(iterator->db, key_string);
    uint64_t node_id = iterator->db->nodes_ids[idx];
    iterator->node_index = idx;
    iterator->key_index = 0;
    int r = iterator_load_node(iterator, node_id);
    if (r < 0) {
        iterator->key_index = -1;
        return r;
    }
    iterator->key_index = find_key(iterator, key_string);

    while (kvdbo_iterator_is_valid(iterator)) {
        const char * other_key;
        size_t other_key_size;
        kvdbo_iterator_get_key(iterator, &other_key, &other_key_size);
        if (std::string(other_key, other_key_size) < key_string) {
            r = kvdbo_iterator_next(iterator);
            if (r < 0) {
                return r;
            }
        }
        else {
            break;
        }
    }

    return KVDB_ERROR_NONE;
}

int kvdbo_iterator_next(kvdbo_iterator * iterator)
{
    iterator->key_index ++;
    if (iterator->key_index < iterator->keys.size()) {
        return KVDB_ERROR_NONE;
    }
    
    // reached end of the node.
    if (iterator->node_index == iterator->db->nodes_ids.size() - 1) {
        // was in the last node.
        return KVDB_ERROR_NONE;
    }
    iterator->node_index ++;
    
    uint64_t node_id = iterator->db->nodes_ids[iterator->node_index];
    int r = iterator_load_node(iterator, node_id);
    if (r < 0) {
        iterator->key_index = -1;
        return r;
    }
    iterator->key_index = 0;
    return KVDB_ERROR_NONE;
}

int kvdbo_iterator_previous(kvdbo_iterator * iterator)
{
    iterator->key_index --;
    if (iterator->key_index >= 0) {
        return KVDB_ERROR_NONE;
    }
    
    // reached beginning of the node.
    if (iterator->node_index == 0) {
        // was in the first node.
        return KVDB_ERROR_NONE;
    }
    iterator->node_index --;
    
    uint64_t node_id = iterator->db->nodes_ids[iterator->node_index];
    int r = iterator_load_node(iterator, node_id);
    if (r < 0) {
        iterator->key_index = -1;
        return r;
    }
    iterator->key_index = (unsigned int) (iterator->keys.size() - 1);
    return KVDB_ERROR_NONE;
}

void kvdbo_iterator_get_key(kvdbo_iterator * iterator, const char ** p_key, size_t * p_key_size)
{
    if (!kvdbo_iterator_is_valid(iterator)) {
        * p_key = NULL;
        * p_key_size = 0;
        return;
    }
    
    std::string & key = iterator->keys[iterator->key_index];
    * p_key = key.c_str();
    * p_key_size = key.length();
}

int kvdbo_iterator_is_valid(kvdbo_iterator * iterator)
{
    return (iterator->key_index != -1) && (iterator->key_index < iterator->keys.size());
}

static void node_unserialize_keys_to_vector(std::vector<std::string> & result,
                                            std::string buffer)
{
    result.clear();
    const char * p = buffer.c_str();
    p += sizeof(uint64_t);
    size_t size = buffer.length() - sizeof(uint64_t);
    std::set<std::string> keys;
    while (size > 0) {
        char command = * p;
        p ++;
        size --;
        uint64_t length;
        size_t position = kv_cstr_decode_uint64(p, size, 0, &length);
        p += position;
        size -= position;
        std::string word = std::string(p, length);
        if (command) {
            keys.insert(word);
        }
        else {
            keys.erase(word);
        }
        p += length;
        size -= length;
    }
    std::set<std::string>::iterator it = keys.begin();
    while (it != keys.end()) {
        result.push_back(* it);
        it ++;
    }
}

static int iterator_load_node(kvdbo_iterator * iterator, uint64_t node_id)
{
    iterator->node_id = node_id;
    
    // load all keys of the node in memory.
    std::string node_key;
    node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    node_key.append(NODE_PREFIX, strlen(NODE_PREFIX));
    uint64_t identifier = hton64(node_id);
    node_key.append((const char *) &identifier, sizeof(identifier));
    char * value = NULL;
    size_t size = 0;
    int r = kvdb_get(iterator->db->db, node_key.c_str(), node_key.length(), &value, &size);
    if (r == KVDB_ERROR_NOT_FOUND) {
        // the node was not found in the storage.
        return KVDB_ERROR_NONE;
    }
    else if (r < 0) {
        return r;
    }
    // load all nodes in a vector.
    node_unserialize_keys_to_vector(iterator->keys, std::string(value, size));
    free(value);

#if 0
    std::string first_key = iterator->db->nodes_first_keys[iterator->node_index];
    for(unsigned int i = 0 ; i < iterator->keys.size() ; i ++) {
        if (iterator->keys[i] < first_key) {
            fprintf(stderr, "has a key lower than first key %s\n", first_key.c_str());
            abort();
        }
    }
#endif

    return KVDB_ERROR_NONE;
}

#pragma mark master node reading / writing.

#define MASTER_NODE_KEY "m"

static int write_master_node(kvdbo * db)
{
    std::string buffer;
    kv_encode_uint64(buffer, db->nodes_ids.size());
    for(uint64_t i = 0 ; i < db->nodes_ids.size() ; i ++) {
        kv_encode_uint64(buffer, db->nodes_ids[i]);
    }
    for(uint64_t i = 0 ; i < db->nodes_keys_count.size() ; i ++) {
        kv_encode_uint64(buffer, db->nodes_keys_count[i]);
    }
    serialize_words_list(buffer, db->nodes_first_keys);
    std::string master_node_key;
    master_node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    master_node_key.append(MASTER_NODE_KEY, strlen(MASTER_NODE_KEY));
    return kvdb_set(db->db, master_node_key.c_str(), master_node_key.length(),
                    buffer.c_str(), buffer.length());
}

static int read_master_node(kvdbo * db)
{
    char * value = NULL;
    size_t size = 0;
    uint64_t max_node_id = 0;
    
    std::string master_node_key;
    master_node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    master_node_key.append(MASTER_NODE_KEY, strlen(MASTER_NODE_KEY));
    int r = kvdb_get(db->db, master_node_key.c_str(), master_node_key.length(),
                     &value, &size);
    if (r == KVDB_ERROR_NOT_FOUND) {
        return KVDB_ERROR_NONE;
    }
    else if (r < 0) {
        return r;
    }
    std::string buffer(value, size);
    db->nodes_ids.clear();
    uint64_t count = 0;
    size_t position = 0;
    position = kv_decode_uint64(buffer, position, &count);
    for(uint64_t i = 0 ; i < count ; i ++) {
        uint64_t node_id = 0;
        position = kv_decode_uint64(buffer, position, &node_id);
        db->nodes_ids.push_back(node_id);
        if (node_id > max_node_id) {
            max_node_id = node_id;
        }
    }
    for(uint64_t i = 0 ; i < count ; i ++) {
        uint64_t keys_count = 0;
        position = kv_decode_uint64(buffer, position, &keys_count);
        db->nodes_keys_count.push_back((uint32_t) keys_count);
    }
    size_t remaining = size - position;
    unserialize_words_list(db->nodes_first_keys, value + position, remaining);
    free(value);
    return KVDB_ERROR_NONE;
}

// binary search of a node that should contain the given key.
// returns the index of the node within the given boundaries.
// used by find_node() below.
static unsigned int find_node_with_boundaries(kvdbo * db, const std::string key,
                                              unsigned int left, unsigned int right)
{
    unsigned int middle = (left + right) / 2;
    if (key >= db->nodes_first_keys[right]) {
        return right;
    }
    if (left == middle) {
        return left;
    }
    
    if (key >= db->nodes_first_keys[middle]) {
        return find_node_with_boundaries(db, key, middle, right);
    }
    else {
        return find_node_with_boundaries(db, key, left, middle - 1);
    }
}

// binary search of a node that should contain the given key.
// returns the index of the node.
static unsigned int find_node(kvdbo * db, const std::string key)
{
    return find_node_with_boundaries(db, key, 0, (unsigned int) db->nodes_first_keys.size() - 1);
}

// binary search of a key in the node loaded by the iterator.
// returns the index of the key within the node, in the given range.
// used by find_key() below.
static unsigned int find_key_with_boundaries(kvdbo_iterator * iterator, const std::string key,
                                             unsigned int left, unsigned int right)
{
    unsigned int middle = (left + right) / 2;
    if (key >= iterator->keys[right]) {
        return right;
    }
    if (left == middle) {
        return left;
    }
    
    if (key >= iterator->keys[middle]) {
        return find_key_with_boundaries(iterator, key, middle, right);
    }
    else {
        return find_key_with_boundaries(iterator, key, left, middle - 1);
    }
}

// binary search of a key in the node loaded by the iterator.
// returns the index of the key within the node.
static unsigned int find_key(kvdbo_iterator * iterator, const std::string key)
{
    return find_key_with_boundaries(iterator, key, 0, (unsigned int) (iterator->keys.size() - 1));
}

// unserialize a list of words to a vector.
static void unserialize_words_list(std::vector<std::string> & word_list, char * value, size_t size)
{
    word_list.clear();
    const char * p = value;
    while (size > 0) {
        uint64_t length;
        size_t position = kv_cstr_decode_uint64(p, size, 0, &length);
        p += position;
        size -= position;
        std::string word = std::string(p, length);
        word_list.push_back(word);
        p += length;
        size -= length;
    }
}

static void serialize_words_list(std::string & value, std::vector<std::string> & word_list)
{
    for(unsigned int i = 0 ; i < word_list.size() ; i ++) {
        kv_encode_uint64(value, word_list[i].length());
        value.append(word_list[i]);
    }
}

// pending modification to a node.
struct modified_node {
    kvdbo * db;
    uint64_t node_id;
    unsigned int node_index;
    std::string buffer;
    uint64_t changes_count;
    // flushed.
    bool has_first_key;
    std::string first_key;
    uint32_t keys_count;
    std::set<std::string> keys;
};

static void node_delete_key(struct modified_node * node, std::string key)
{
    char command = 0;
    node->buffer.append(&command, 1);
    kv_encode_uint64(node->buffer, key.length());
    node->buffer.append(key);
    node->changes_count ++;
}

static void node_add_key(struct modified_node * node, std::string key)
{
    char command = 1;
    node->buffer.append(&command, 1);
    kv_encode_uint64(node->buffer, key.length());
    node->buffer.append(key);
    node->changes_count ++;
}

static int flush_pending_keys(kvdbo * db)
{
    int r;

    if (db->pending_keys.size() == 0 && db->pending_keys_delete.size() == 0) {
        return KVDB_ERROR_NONE;
    }

    if ((db->pending_keys.size() > 0) && (db->nodes_ids.size() == 0)) {
        add_first_node(db);
    }
    
    struct modified_node current_node;
    current_node.db = db;
    current_node.node_id = 0;
    current_node.node_index = -1;
    current_node.changes_count = 0;
    current_node.has_first_key = false;
    current_node.keys_count = 0;
    current_node.keys.clear();
    
    std::set<std::string>::iterator addition_it = db->pending_keys.begin();
    std::set<std::string>::iterator deletion_it = db->pending_keys_delete.begin();
    for(unsigned int node_index = 0 ; node_index < db->nodes_ids.size() ; node_index ++) {

        if (current_node.node_index != node_index) {
            int r = write_loaded_node(&current_node);
            if (r < 0) {
                return r;
            }
        }

        // if it's the last node.
        if (node_index == db->nodes_ids.size() - 1) {
            // also applies when nodes_ids->size() == 1, node_index == 0
            while (deletion_it != db->pending_keys_delete.end()) {
                // case of the last node, removes all the pending deletions from this node.
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                node_delete_key(&current_node, * deletion_it);
                deletion_it ++;
            }
            while (addition_it != db->pending_keys.end()) {
                // case of the last node, adds all the pending additions to this node.
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                node_add_key(&current_node, * addition_it);
                addition_it ++;
            }
        }
        else {
            // applies when nodes_ids->size() >= 2
            while (deletion_it != db->pending_keys_delete.end()) {
                // make sure that we don't reach the boundary of the next node.
                if (* deletion_it >= db->nodes_first_keys[node_index + 1]) {
                    // stop here if the key is not part of the node.
                    break;
                }
                // removes the key from the node.
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                node_delete_key(&current_node, * deletion_it);
                deletion_it ++;
            }
            while (addition_it != db->pending_keys.end()) {
                // make sure that we don't reach the boundary of the next node.
                if (* addition_it >= db->nodes_first_keys[node_index + 1]) {
                    // stop here if the key is not part of the node.
                    break;
                }
                // adds the key to the node.
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                node_add_key(&current_node, * addition_it);
                addition_it ++;
            }
        }
    }
    // write the last node.
    r = write_loaded_node(&current_node);
    if (r < 0) {
        return r;
    }

    if (db->master_node_changed) {
        r = write_master_node(db);
        if (r < 0) {
            return r;
        }
    }
    
    db->pending_keys.clear();
    db->pending_keys_delete.clear();

    //kvdbo_check_sorted(db);
    return KVDB_ERROR_NONE;
}

static void node_serialize_keys(struct modified_node * node)
{
    node->buffer.clear();
    uint64_t count = 0;
    node->buffer.append((char *) &count, sizeof(uint64_t));
    std::set<std::string>::iterator it = node->keys.begin();
    while (it != node->keys.end()) {
        char command = 1;
        node->buffer.append(&command, 1);
        kv_encode_uint64(node->buffer, it->length());
        node->buffer.append(* it);
        it ++;
    }
}

static void node_unserialize_keys(struct modified_node * node)
{
    node->keys.clear();
    unserialize_keys(node->keys, node->buffer);
}

static void unserialize_keys(std::set<std::string> & keys, std::string & buffer)
{
    const char * p = buffer.c_str();
    p += sizeof(uint64_t);
    size_t size = buffer.length() - sizeof(uint64_t);
    while (size > 0) {
        char command = * p;
        p ++;
        size --;
        uint64_t length;
        size_t position = kv_cstr_decode_uint64(p, size, 0, &length);
        p += position;
        size -= position;
        std::string word = std::string(p, length);
        if (command) {
            keys.insert(word);
        }
        else {
            keys.erase(word);
        }
        p += length;
        size -= length;
    }
}

static void flush_node(struct modified_node * node)
{
    node_unserialize_keys(node);
    node_serialize_keys(node);
    
    std::set<std::string>::iterator it = node->keys.begin();
    if (it != node->keys.end()) {
        node->has_first_key = true;
        node->first_key = * it;
    }
    else {
        node->has_first_key = false;
    }
    node->changes_count = 0;
    node->keys_count = (uint32_t) node->keys.size();
}

// load the given node in memory.
static int load_node(struct modified_node * node, unsigned int node_index)
{
    int r = write_loaded_node(node);
    if (r < 0) {
        return r;
    }

    uint64_t node_id = node->db->nodes_ids[node_index];
    node->node_index = node_index;
    node->node_id = node_id;
    node->buffer.clear();
    node->changes_count = 0;
    node->has_first_key = false;
    node->keys_count = 0;
    node->keys.clear();
    
    r = load_from_node_id(node, node_id);
    if (r != 0) {
        return r;
    }
    
    return r;
}

// add the keys from the given node to the data structure.
static int load_from_node_id(struct modified_node * node, uint64_t node_id)
{
    std::string node_key;
    node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    node_key.append(NODE_PREFIX, strlen(NODE_PREFIX));
    uint64_t identifier = hton64(node_id);
    node_key.append((const char *) &identifier, sizeof(identifier));
    char * value;
    size_t value_size;
    int r = kvdb_get(node->db->db, node_key.c_str(), node_key.length(), &value, &value_size);
    if (r == KVDB_ERROR_NOT_FOUND) {
        // normal situation.
        node->buffer.append(sizeof(uint64_t), 0);
        return KVDB_ERROR_NONE;
    }
    else if (r < 0) {
        node->node_index = -1;
        return r;
    }
    
    node->buffer.append(value, value_size);
    node->changes_count = ntoh64(* (uint64_t *) value);
    
    free(value);
    
    return KVDB_ERROR_NONE;
}

static int remove_node_id(kvdbo * db, uint64_t node_id)
{
    std::string node_key;
    node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    node_key.append(NODE_PREFIX, strlen(NODE_PREFIX));
    uint64_t identifier = hton64(node_id);
    node_key.append((const char *) &identifier, sizeof(identifier));
    int r = kvdb_delete(db->db, node_key.c_str(), node_key.length());
    if (r == KVDB_ERROR_NOT_FOUND) {
        return KVDB_ERROR_NONE;
    }
    else if (r < 0) {
        return r;
    }
    return KVDB_ERROR_NONE;
}

static int write_single_loaded_node(struct modified_node * node)
{
    // write the node.
    * (uint64_t *) node->buffer.c_str() = hton64(node->changes_count);
    std::string node_key;
    node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    node_key.append(NODE_PREFIX, strlen(NODE_PREFIX));
    uint64_t identifier = hton64(node->node_id);
    node_key.append((const char *) &identifier, sizeof(identifier));
    int r = kvdb_set(node->db->db, node_key.c_str(), node_key.length(), node->buffer.c_str(), node->buffer.length());
    if (r < 0) {
        return r;
    }
    
    // update the master node.
    bool changed = false;
    if (node->node_id != node->db->nodes_ids[node->node_index]) {
        node->db->nodes_ids[node->node_index] = node->node_id;
        node->db->master_node_changed = true;
    }
    if (node->has_first_key) {
        if (node->db->nodes_keys_count[node->node_index] != node->keys_count) {
            node->db->nodes_keys_count[node->node_index] = (uint32_t) node->keys_count;
            node->db->master_node_changed = true;
        }
        if (node->db->nodes_first_keys[node->node_index] != node->first_key) {
            node->db->nodes_first_keys[node->node_index] = node->first_key;
            node->db->master_node_changed = true;
        }
    }
    
    return KVDB_ERROR_NONE;
}

// returns the next usable node identifier.
static uint64_t allocate_node_id(kvdbo * db)
{
    uint64_t node_id = db->next_node_id;
    db->next_node_id ++;
    return node_id;
}

// create the first node.
static void add_first_node(kvdbo * db)
{
    uint64_t node_id = allocate_node_id(db);
    db->nodes_ids.push_back(node_id);
    db->nodes_first_keys.push_back("");
    db->nodes_keys_count.push_back(0);
    db->master_node_changed = true;
}

#define MAX_CHANGES_COUNT 16384
#define MAX_KEYS_PER_NODE 16384
#define KEYS_PER_NODE_MERGE_THRESHOLD_FACTOR 4
#define KEYS_PER_NODE_MERGE_THRESHOLD (MAX_KEYS_PER_NODE / KEYS_PER_NODE_MERGE_THRESHOLD_FACTOR)
#define MEAN_KEYS_PER_NODE_FACTOR 2
#define MEAN_KEYS_PER_NODE (MAX_KEYS_PER_NODE / MEAN_KEYS_PER_NODE_FACTOR)

static void kvdbo_check_sorted(kvdbo * db)
{
    //fprintf(stderr, "check sorted\n");
    if (db->checking_sorted) {
        return;
    }
    db->checking_sorted = true;

    show_nodes_content(db);

    kvdbo_check_first_keys(db);

    kvdbo_iterator * iterator = kvdbo_iterator_new(db);
    kvdbo_iterator_seek_first(iterator);
    std::string last_key = "";
    while (kvdbo_iterator_is_valid(iterator)) {
        const char * key;
        size_t key_size;
        kvdbo_iterator_get_key(iterator, &key, &key_size);
        std::string cur_key(key, key_size);
        if (cur_key < last_key) {
            fprintf(stderr, "ERROR: cur_key: %s, last_key: %s\n", cur_key.c_str(), last_key.c_str());
            abort();
        }
        last_key = cur_key;
        kvdbo_iterator_next(iterator);
    }
    db->checking_sorted = false;
}

static void show_nodes_content(kvdbo * db)
{
    printf("******* all keys ******\n");
    for(unsigned int k = 0 ; k < db->nodes_ids.size() ; k ++) {
        kvdbo_iterator * iterator = kvdbo_iterator_new(db);
        iterator->node_index = k;
        iterator->node_id = db->nodes_ids[iterator->node_index];
        iterator_load_node(iterator, iterator->node_id);
        fprintf(stderr, "keys (%i, %llu, %i, %s): ", k, db->nodes_ids[k], db->nodes_keys_count[k], db->nodes_first_keys[k].c_str());
        for(unsigned int i = 0 ; i < iterator->keys.size() ; i ++) {
            fprintf(stderr, "%s ", iterator->keys[i].c_str());
        }
        fprintf(stderr, "\n");
    }
    printf("*******\n");
}

static void kvdbo_check_first_keys(kvdbo * db)
{
    std::string last_key = "";
    for(unsigned int i = 0 ; i < db->nodes_first_keys.size() ; i ++) {
        if (i > 0) {
            if (db->nodes_first_keys[i] <= last_key) {
                fprintf(stderr, "ERROR %i first keys: cur_key: %s, last_key: %s\n", i, db->nodes_first_keys[i].c_str(), last_key.c_str());
                show_nodes(db);
                abort();
            }
        }
        last_key = db->nodes_first_keys[i];
    }
}

// write the node to disk.
static int write_loaded_node(struct modified_node * node)
{
    // not valid.
    if (node->node_index == -1) {
        return KVDB_ERROR_NONE;
    }
    
    if (node->changes_count < MAX_CHANGES_COUNT) {
        int r = write_single_loaded_node(node);
        // invalidate.
        node->node_index = -1;
        return r;
    }
    
    flush_node(node);
    
    if (node->keys_count == 0) {
        // if there's no keys.
        int r = remove_node(node->db, node->node_index);
        // invalidate.
        node->node_index = -1;
        return r;
    }
    else if (node->keys_count > MAX_KEYS_PER_NODE) {
        // if there's more keys than the limit, split node.
        unsigned int node_index = node->node_index;
        // compute the number of nodes to create to replace this one.
        unsigned int count = (unsigned int) ((node->keys.size() + MEAN_KEYS_PER_NODE - 1) / MEAN_KEYS_PER_NODE);
        int r = split_node(node->db, node_index, count, node->keys);
        if (r < 0) {
            return r;
        }
        bool didMerge = false;
        // try to merge the last one with the next one.
        r = try_merge(node->db, node_index + count - 1, &didMerge);
        if (r < 0) {
            return r;
        }
        // invalidate.
        node->node_index = -1;
        return KVDB_ERROR_NONE;
    }
    else if (node->keys_count < KEYS_PER_NODE_MERGE_THRESHOLD) {
        // if there's a low number of keys.
        int r = write_single_loaded_node(node);
        if (r < 0) {
            return r;
        }
        
        // try to merge node with previous...
        unsigned int node_index = node->node_index;
        bool didMerge = false;
        if (node_index > 0) {
            r = try_merge(node->db, node_index - 1, &didMerge);
            if (r < 0) {
                return r;
            }
            if (didMerge) {
                node_index --;
            }
        }
        // then, with next.
        r = try_merge(node->db, node_index, &didMerge);
        if (r != 0) {
            return r;
        }
        // invalidate.
        node->node_index = -1;
        return KVDB_ERROR_NONE;
    }
    else {
        // in other cases.
        int r = write_single_loaded_node(node);
        // invalidate.
        node->node_index = -1;
        return r;
    }

    return KVDB_ERROR_NONE;
}

// try to merge with the next node.
static int try_merge(kvdbo * db, unsigned int node_index, bool * pDidMerge)
{
    // there's no next node.
    if (node_index + 1 >= db->nodes_ids.size()) {
        * pDidMerge = false;
        return KVDB_ERROR_NONE;
    }
    
    // would it make the number of keys larger than the threshold?
    if (db->nodes_keys_count[node_index] + db->nodes_keys_count[node_index + 1] > MEAN_KEYS_PER_NODE) {
        * pDidMerge = false;
        return KVDB_ERROR_NONE;
    }
    
    struct modified_node current_node;
    current_node.db = db;
    current_node.node_id = db->nodes_ids[node_index];
    current_node.node_index = node_index;
    current_node.changes_count = 0;
    current_node.has_first_key = false;
    current_node.keys_count = 0;
    current_node.keys.clear();
    
    struct modified_node next_node;
    next_node.db = db;
    next_node.node_id = db->nodes_ids[node_index + 1];
    next_node.node_index = node_index + 1;
    next_node.changes_count = 0;
    next_node.has_first_key = false;
    next_node.keys_count = 0;
    next_node.keys.clear();
    
    int r = load_from_node_id(&current_node, db->nodes_ids[node_index]);
    if (r < 0) {
        return r;
    }
    // add keys of node at (node_index + 1) into memory.
    r = load_from_node_id(&next_node, db->nodes_ids[node_index + 1]);
    if (r < 0) {
        return r;
    }
    
    node_unserialize_keys(&current_node);
    unserialize_keys(current_node.keys, next_node.buffer);
    
    // write the result.
    r = write_single_loaded_node(&current_node);
    if (r < 0) {
        return r;
    }
    
    // remove the node at (node_index + 1).
    r = remove_node(db, node_index + 1);
    if (r < 0) {
        return r;
    }
    
    * pDidMerge = true;
    
    return KVDB_ERROR_NONE;
}

// remove node at the given index.
static int remove_node(kvdbo * db, unsigned int node_index)
{
    int r = remove_node_id(db, db->nodes_ids[node_index]);
    if (r < 0) {
        return r;
    }
    db->nodes_ids.erase(db->nodes_ids.begin() + node_index);
    db->nodes_first_keys.erase(db->nodes_first_keys.begin() + node_index);
    db->nodes_keys_count.erase(db->nodes_keys_count.begin() + node_index);
    db->master_node_changed = true;
    
    return KVDB_ERROR_NONE;
}

// create 'count' new nodes to replace the given node at node_index.
// the given keys will be used to fill the new nodes.
static int split_node(kvdbo * db, unsigned int node_index, unsigned int count,
                      std::set<std::string> & keys)
{
    // creates as many nodes as needed for the split.
    struct modified_node * nodes = new modified_node[count];
    for(unsigned int i = 0 ; i < count ; i ++) {
        nodes[i].db = db;
        nodes[i].node_id = allocate_node_id(db);
        nodes[i].node_index = node_index + i;
        nodes[i].buffer.append(sizeof(uint64_t), 0);
        nodes[i].changes_count = 0;
        nodes[i].has_first_key = false;
        nodes[i].keys_count = 0;
        nodes[i].keys.clear();
    }
    
    // fill the new nodes with keys.
    struct modified_node * current_node = &nodes[0];
    uint32_t added_count = 0;
    current_node->keys_count = 0;
    std::set<std::string>::iterator it = keys.begin();
    while (it != keys.end()) {
        if (added_count >= MAX_KEYS_PER_NODE / MEAN_KEYS_PER_NODE_FACTOR) {
            current_node ++;
            added_count = 0;
        }
        if (!current_node->has_first_key) {
            current_node->first_key = * it;
            current_node->has_first_key = true;
        }
        node_add_key(current_node, * it);
        added_count ++;
        current_node->keys_count ++;
        it ++;
    }
    
    // adjust the master node information.
    int r;
    remove_node_id(db, db->nodes_ids[node_index]);
    db->nodes_ids.erase(db->nodes_ids.begin() + node_index);
    db->nodes_first_keys.erase(db->nodes_first_keys.begin() + node_index);
    db->nodes_keys_count.erase(db->nodes_keys_count.begin() + node_index);
    db->nodes_ids.insert(db->nodes_ids.begin() + node_index, count, 0);
    db->nodes_first_keys.insert(db->nodes_first_keys.begin() + node_index, count, "");
    db->nodes_keys_count.insert(db->nodes_keys_count.begin() + node_index, count, 0);
    // write the nodes.
    for(unsigned int i = 0 ; i < count ; i ++) {
        r = write_single_loaded_node(&nodes[i]);
        if (r != 0) {
            return r;
        }
    }
    delete [] nodes;
    db->master_node_changed = true;

    return KVDB_ERROR_NONE;
}

void kvdbo_transaction_begin(kvdbo * db)
{
    db->in_transaction = true;
    db->master_node_changed = false;
    kvdb_transaction_begin(db->db);
}

void kvdbo_transaction_abort(kvdbo * db)
{
    db->pending_keys.clear();
    db->pending_keys_delete.clear();
    kvdb_transaction_abort(db->db);
    db->in_transaction = false;
    db->implicit_transaction = false;
}

int kvdbo_transaction_commit(kvdbo * db)
{
    if ((db->pending_keys.size() == 0) && (db->pending_keys_delete.size() == 0)) {
        kvdb_transaction_abort(db->db);
        return KVDB_ERROR_NONE;
    }
    
    db->in_transaction = false;
    db->implicit_transaction = false;
    int r = flush_pending_keys(db);
    if (r < 0) {
        kvdb_transaction_abort(db->db);
        return r;
    }
    r = kvdb_transaction_commit(db->db);
    if (r < 0) {
        return r;
    }
    return KVDB_ERROR_NONE;
}

// for debug purpose.
static void show_nodes(kvdbo * db)
{
    printf("*******\n");
    printf("node_ids: ");
    for(unsigned int i = 0 ; i < db->nodes_ids.size() ; i ++) {
        printf("%i ", (int) db->nodes_ids[i]);
    }
    printf("\n");
    printf("keys: ");
    for(unsigned int i = 0 ; i < db->nodes_first_keys.size() ; i ++) {
        printf("%s ", db->nodes_first_keys[i].c_str());
    }
    printf("\n");
    printf("count: ");
    for(unsigned int i = 0 ; i < db->nodes_keys_count.size() ; i ++) {
        printf("%i ", (int) db->nodes_keys_count[i]);
    }
    printf("\n");
    printf("*******\n");
}

#define IMPLICIT_TRANSACTION_MAX_OP 10000

static int start_implicit_transaction_if_needed(kvdbo * db)
{
    int r;
    
    if (db->implicit_transaction && (db->implicit_transaction_op_count > IMPLICIT_TRANSACTION_MAX_OP)) {
        r = kvdbo_transaction_commit(db);
        if (r < 0) {
            return r;
        }
    }
    
    if (db->in_transaction) {
        return KVDB_ERROR_NONE;
    }
    
    db->implicit_transaction = true;
    db->implicit_transaction_op_count = 0;
    kvdbo_transaction_begin(db);
    return KVDB_ERROR_NONE;
}
