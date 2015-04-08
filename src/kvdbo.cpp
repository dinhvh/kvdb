#include "kvdbo.h"

#include "kvdb.h"
#include "kvendian.h"
#include "kvassert.h"
#include "kvserialization.h"

#include <set>
#include <string>
#include <vector>
#include <stdlib.h>

struct kvdbo {
    // underlaying kvdb.
    kvdb * db;
    
    // in memory buffers for operations.
    std::set<std::string> pending_keys;
    std::set<std::string> pending_keys_delete;
    // node identifier allocation.
    uint64_t next_node_id;
    
    // master node.
    // identifiers of the nodes.
    std::vector<uint64_t> nodes_ids;
    // first keys of the nodes.
    std::vector<std::string> nodes_first_keys;
    // number of keys in each node.
    std::vector<uint32_t> nodes_keys_count;
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
};

#define NODE_PREFIX "n"

static int flush_pending_keys(kvdbo * db);
static int write_master_node(kvdbo * db);
static int read_master_node(kvdbo * db);
static unsigned int find_node(kvdbo * db, const std::string key);
static unsigned int find_key(kvdbo_iterator * iterator, const std::string key);
static void unserialize_words_list(std::vector<std::string> & word_list, char * value, size_t size);
static void unserialize_words_set(std::set<std::string> & word_set, char * value, size_t size, bool clear_words_set);
static void serialize_words_set(std::string & value, std::set<std::string> & word_set);
static int iterator_load_node(kvdbo_iterator * iterator, uint64_t node_id);
static int add_first_node(kvdbo * db);
static int load_node(struct modified_node * node, unsigned int node_index);
static int load_from_node_id(struct modified_node * node, uint64_t node_id);
static int write_loaded_node(struct modified_node * node);
static int write_single_loaded_node(struct modified_node * node);
static int try_merge(kvdbo * db, unsigned int node_index, bool * pDidMerge);
static int remove_node_id(kvdbo * db, uint64_t node_id);
static int remove_node(kvdbo * db, unsigned int node_index);
static int split_node(kvdbo * db, unsigned int node_index, unsigned int count,
                      std::set<std::string> & keys);

static void show_nodes(kvdbo * db);

#pragma mark kvdbo data structure management.

kvdbo * kvdbo_new(const char* filename)
{
    kvdbo * db;
    db = new kvdbo;
    db->db = kvdb_new(filename);
    db->next_node_id = 1;
    return db;
}

void kvdbo_free(kvdbo * db)
{
    kvdb_free(db->db);
    delete db;
}

#pragma mark opening / closing the database.

int kvdbo_open(kvdbo * db)
{
    int r = kvdb_open(db->db);
    if (r < 0) {
        return r;
    }
    r = read_master_node(db);
    if (r < 0) {
        kvdbo_close(db);
        return r;
    }
    return 0;
}

void kvdbo_close(kvdbo * db)
{
    db->nodes_keys_count.clear();
    db->nodes_first_keys.clear();
    db->nodes_ids.clear();
    flush_pending_keys(db);
    kvdb_close(db->db);
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
#warning implement implicit creation of transaction and flush.
    int r;
    
    std::string key_str(key, key_size);
    if (key_str.find(std::string(METAKEY_PREFIX, METAKEY_PREFIX_SIZE)) == 0) {
        // invalid key.
        return -3;
    }
    db->pending_keys_delete.erase(key_str);
    db->pending_keys.insert(key_str);
    r = kvdb_set(db->db, key, key_size, value, value_size);
    if (r != 0) {
        return r;
    }
    return 0;
}

int kvdbo_get(kvdbo * db,
              const char * key,
              size_t key_size,
              char ** p_value,
              size_t * p_value_size)
{
    if (db->pending_keys_delete.find(std::string(key, key_size)) != db->pending_keys_delete.end()) {
        return -1;
    }
    return kvdb_get(db->db, key, key_size, p_value, p_value_size);
}

int kvdbo_delete(kvdbo * db, const char* key, size_t key_size)
{
    std::string key_str(key, key_size);
    db->pending_keys.erase(key_str);
    db->pending_keys_delete.insert(key_str);
    return kvdb_delete(db->db, key, key_size);
}

#pragma mark iterator management.

kvdbo_iterator * kvdbo_iterator_new(kvdbo * db)
{
    kvdbo_iterator * iterator = new kvdbo_iterator;
    iterator->key_index = -1;
    iterator->db = db;
    return iterator;
}

void kvdbo_iterator_free(kvdbo_iterator * iterator)
{
    delete iterator;
}

void kvdbo_iterator_seek_first(kvdbo_iterator * iterator)
{
    if (iterator->db->nodes_ids.size() == 0) {
        return;
    }
    uint64_t node_id = iterator->db->nodes_ids[0];
    int r = iterator_load_node(iterator, node_id);
    KVDBAssert(r == 0);
    iterator->node_index = 0;
    iterator->key_index = 0;
}

void kvdbo_iterator_seek_last(kvdbo_iterator * iterator)
{
    if (iterator->db->nodes_ids.size() == 0) {
        return;
    }
    uint64_t node_id = iterator->db->nodes_ids[iterator->db->nodes_ids.size() - 1];
    int r = iterator_load_node(iterator, node_id);
    KVDBAssert(r == 0);
    iterator->node_index = (unsigned int) (iterator->db->nodes_ids.size() - 1);
    iterator->key_index = (unsigned int) (iterator->keys.size() - 1);
}

void kvdbo_iterator_seek_after(kvdbo_iterator * iterator,
                               const char * key,
                               size_t key_size)
{
    if (iterator->db->nodes_ids.size() == 0) {
        return;
    }
    std::string key_string(key, key_size);
    unsigned int idx = find_node(iterator->db, key_string);
    uint64_t node_id = iterator->db->nodes_ids[idx];
    int r = iterator_load_node(iterator, node_id);
    KVDBAssert(r == 0);
    iterator->node_index = idx;
    iterator->key_index = find_key(iterator, key_string);
}

void kvdbo_iterator_next(kvdbo_iterator * iterator)
{
    iterator->key_index ++;
    if (iterator->key_index < iterator->keys.size()) {
        return;
    }
    
    // reached end of the node.
    if (iterator->node_index == iterator->db->nodes_ids.size() - 1) {
        // was in the last node.
        return;
    }
    iterator->node_index ++;
    
    uint64_t node_id = iterator->db->nodes_ids[iterator->node_index];
    int r = iterator_load_node(iterator, node_id);
    KVDBAssert(r == 0);
    iterator->key_index = 0;
}

void kvdbo_iterator_previous(kvdbo_iterator * iterator)
{
    iterator->key_index --;
    if (iterator->key_index >= 0) {
        return;
    }
    
    // reached beginning of the node.
    if (iterator->node_index == 0) {
        // was in the first node.
        return;
    }
    iterator->node_index --;
    
    uint64_t node_id = iterator->db->nodes_ids[iterator->node_index];
    int r= iterator_load_node(iterator, node_id);
    KVDBAssert(r == 0);
    iterator->key_index = (unsigned int) (iterator->keys.size() - 1);
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
    if (r == -1) {
        return 0;
    }
    if (r == -2) {
        return -2;
    }
    // load all nodes in a vector.
    unserialize_words_list(iterator->keys, value, size);
    free(value);
    return 0;
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
    for(uint64_t i = 0 ; i < db->nodes_first_keys.size() ; i ++) {
        // write first key of the node.
        std::string key = db->nodes_first_keys[i];
        buffer.append(key.c_str(), key.length());
        buffer.push_back(0);
    }
    std::string master_node_key;
    master_node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    master_node_key.append(MASTER_NODE_KEY, strlen(MASTER_NODE_KEY));
    int r = kvdb_set(db->db, master_node_key.c_str(), master_node_key.length(),
                     buffer.c_str(), buffer.length());
    return r;
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
    if (r == -1) {
        return 0;
    }
    if (r == -2) {
        return -2;
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
    //size_t remaining = size - (p - value);
    size_t remaining = size - position;
    unserialize_words_list(db->nodes_first_keys, value + position, remaining);
    return 0;
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
    const char * key_start = value;
    while (size > 0) {
        if (* p == 0) {
            // add key.
            size_t len = p - key_start;
            word_list.push_back(std::string(key_start, len));
            key_start = p + 1;
        }
        p ++;
        size --;
    }
}

// unserialize a list of words to a set.
static void unserialize_words_set(std::set<std::string> & word_set, char * value, size_t size, bool clear_words_set)
{
    if (clear_words_set) {
        word_set.clear();
    }
    const char * p = value;
    const char * key_start = value;
    while (size > 0) {
        if (* p == 0) {
            // add key.
            size_t len = p - key_start;
            word_set.insert(std::string(key_start, len));
            key_start = p + 1;
        }
        p ++;
        size --;
    }
}

// serialize a list of words stored in a set.
// the result will be stored in the variable value.
static void serialize_words_set(std::string & value, std::set<std::string> & word_set)
{
    std::set<std::string>::iterator it = word_set.begin();
    while (it != word_set.end()) {
        value.append(* it);
        value.push_back(0);
        it ++;
    }
}

// pending modification to a node.
struct modified_node {
    kvdbo * db;
    uint64_t node_id;
    unsigned int node_index;
    std::set<std::string> keys;
};

// flush the pending changes of the keys list in memory.
static int flush_pending_keys(kvdbo * db)
{
    if ((db->pending_keys.size() > 0) && (db->nodes_ids.size() == 0)) {
        add_first_node(db);
    }
    
    struct modified_node current_node;
    current_node.db = db;
    current_node.node_id = 0;
    current_node.node_index = -1;
    
    std::set<std::string>::iterator addition_it = db->pending_keys.begin();
    std::set<std::string>::iterator deletion_it = db->pending_keys_delete.begin();
    for(unsigned int node_index = 0 ; node_index < db->nodes_ids.size() ; node_index ++) {
        // if it's the last node.
        if (node_index == db->nodes_ids.size() - 1) {
            // also applies when nodes_ids->size() == 1, node_index == 0
            while (deletion_it != db->pending_keys_delete.end()) {
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                current_node.keys.erase(* deletion_it);
                deletion_it ++;
            }
            while (addition_it != db->pending_keys.end()) {
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                current_node.keys.insert(* addition_it);
                addition_it ++;
            }
        }
        else {
            // applies when nodes_ids->size() >= 2
            while (deletion_it != db->pending_keys_delete.end()) {
                // make sure that we don't reach the boundary of the next node.
                if (* deletion_it >= db->nodes_first_keys[node_index + 1]) {
                    // stop here.
                    break;
                }
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                current_node.keys.erase(* deletion_it);
                deletion_it ++;
            }
            while (addition_it != db->pending_keys.end()) {
                // make sure that we don't reach the boundary of the next node.
                if (* addition_it >= db->nodes_first_keys[node_index + 1]) {
                    // stop here.
                    break;
                }
                if (current_node.node_index != node_index) {
                    load_node(&current_node, node_index);
                }
                current_node.keys.insert(* addition_it);
                addition_it ++;
            }
        }
    }
    // write the last node.
    write_loaded_node(&current_node);
    db->pending_keys.clear();
    db->pending_keys_delete.clear();
    
    return 0;
}

// load the given node in memory.
static int load_node(struct modified_node * node, unsigned int node_index)
{
    write_loaded_node(node);
    
    uint64_t node_id = node->db->nodes_ids[node_index];
    node->node_index = node_index;
    node->node_id = node_id;
    node->keys.clear();
    
    int r = load_from_node_id(node, node_id);
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
    if (r == -2) {
        node->node_index = -1;
        return -2;
    }
    if (r == 0) {
        unserialize_words_set(node->keys, value, value_size, false);
        free(value);
    }
    
    return 0;
}

static int remove_node_id(kvdbo * db, uint64_t node_id)
{
    std::string node_key;
    node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    node_key.append(NODE_PREFIX, strlen(NODE_PREFIX));
    uint64_t identifier = hton64(node_id);
    node_key.append((const char *) &identifier, sizeof(identifier));
    int r = kvdb_delete(db->db, node_key.c_str(), node_key.length());
    if (r == -1) {
        return 0;
    }
    if (r != 0) {
        return r;
    }
    return 0;
}

// returns the next usable node identifier.
static uint64_t allocate_node_id(kvdbo * db)
{
    uint64_t node_id = db->next_node_id;
    db->next_node_id ++;
    return node_id;
}

// create the first node.
static int add_first_node(kvdbo * db)
{
    uint64_t node_id = allocate_node_id(db);
    db->nodes_ids.push_back(node_id);
    db->nodes_first_keys.push_back("");
    db->nodes_keys_count.push_back(0);
    int r = write_master_node(db);
    if (r != 0) {
        return r;
    }
    return 0;
}

#define MAX_KEYS_PER_NODE 16384
#define KEYS_PER_NODE_MERGE_THRESHOLD_FACTOR 4
#define KEYS_PER_NODE_MERGE_THRESHOLD (MAX_KEYS_PER_NODE / KEYS_PER_NODE_MERGE_THRESHOLD_FACTOR)
#define MEAN_KEYS_PER_NODE_FACTOR 2
#define MEAN_KEYS_PER_NODE (MAX_KEYS_PER_NODE / MEAN_KEYS_PER_NODE_FACTOR)

// write the node to disk.
static int write_loaded_node(struct modified_node * node)
{
    // not valid.
    if (node->node_index == -1) {
        return 0;
    }
    
    if (node->keys.size() == 0) {
        // if there's no keys.
        int r = remove_node(node->db, node->node_index);
        // invalidate.
        node->node_index = -1;
        return r;
    }
    else if (node->keys.size() > MAX_KEYS_PER_NODE) {
        // if there's more keys than the limit, split node.
        unsigned int node_index = node->node_index;
        // compute the number of nodes to create to replace this one.
        unsigned int count = (unsigned int) ((node->keys.size() + MEAN_KEYS_PER_NODE - 1) / MEAN_KEYS_PER_NODE);
        int r = split_node(node->db, node_index, count, node->keys);
        if (r != 0) {
            return r;
        }
        bool didMerge = false;
        // try to merge the last one with the next one.
        r = try_merge(node->db, node_index + count - 1, &didMerge);
        if (r != 0) {
            return r;
        }
        // invalidate.
        node->node_index = -1;
        return 0;
    }
    else if (node->keys.size() < KEYS_PER_NODE_MERGE_THRESHOLD) {
        // if there's a low number of keys.
        int r = write_single_loaded_node(node);
        if (r != 0) {
            return r;
        }
        
        // try to merge node with previous...
        unsigned int node_index = node->node_index;
        bool didMerge = false;
        if (node_index > 0) {
            r = try_merge(node->db, node_index - 1, &didMerge);
            if (r != 0) {
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
        return 0;
    }
    else {
        // in other cases.
        int r = write_single_loaded_node(node);
        // invalidate.
        node->node_index = -1;
        return r;
    }
    return 0;
}

static int write_single_loaded_node(struct modified_node * node)
{
    // write the node.
    std::string value;
    serialize_words_set(value, node->keys);
    std::string node_key;
    node_key.append(METAKEY_PREFIX, METAKEY_PREFIX_SIZE);
    node_key.append(NODE_PREFIX, strlen(NODE_PREFIX));
    uint64_t identifier = hton64(node->node_id);
    node_key.append((const char *) &identifier, sizeof(identifier));
    int r = kvdb_set(node->db->db, node_key.c_str(), node_key.length(), value.c_str(), value.length());
    if (r != 0) {
        return r;
    }
    // update the master node.
    bool changed = false;
    if (node->node_id != node->db->nodes_ids[node->node_index]) {
        node->db->nodes_ids[node->node_index] = node->node_id;
        changed = true;
    }
    if (node->db->nodes_keys_count[node->node_index] != node->keys.size()) {
        node->db->nodes_keys_count[node->node_index] = (uint32_t) node->keys.size();
        changed = true;
    }
    std::string first_key;
    if (node->keys.begin() != node->keys.end()) {
        first_key = * node->keys.begin();
    }
    if (node->db->nodes_first_keys[node->node_index] != first_key) {
        node->db->nodes_first_keys[node->node_index] = first_key;
        changed = true;
    }
    if (changed) {
        r = write_master_node(node->db);
        if (r != 0) {
            return r;
        }
    }
    
    return 0;
}

// try to merge with the next node.
static int try_merge(kvdbo * db, unsigned int node_index, bool * pDidMerge)
{
    // there's no next node.
    if (node_index + 1 >= db->nodes_ids.size()) {
        * pDidMerge = false;
        return 0;
    }
    
    // would it make the number of keys larger than the threshold?
    if (db->nodes_keys_count[node_index] + db->nodes_keys_count[node_index + 1] > MEAN_KEYS_PER_NODE) {
        * pDidMerge = false;
        return 0;
    }
    
    struct modified_node current_node;
    current_node.db = db;
    current_node.node_id = db->nodes_ids[node_index];
    current_node.node_index = node_index;
    
    // add keys of node at node_index into memory.
    int r = load_from_node_id(&current_node, db->nodes_ids[node_index]);
    if (r != 0) {
        return r;
    }
    // add keys of node at (node_index + 1) into memory.
    r = load_from_node_id(&current_node, db->nodes_ids[node_index + 1]);
    if (r != 0) {
        return r;
    }
    
    // write the result.
    r = write_single_loaded_node(&current_node);
    if (r != 0) {
        return r;
    }
    
    //delete current_node.keys;
    
    // remove the node at (node_index + 1).
    r = remove_node(db, node_index + 1);
    if (r != 0) {
        return r;
    }
    
    * pDidMerge = true;
    
    return 0;
}

// remove node at the given index.
static int remove_node(kvdbo * db, unsigned int node_index)
{
    int r = remove_node_id(db, db->nodes_ids[node_index]);
    if (r != 0) {
        return r;
    }
    db->nodes_ids.erase(db->nodes_ids.begin() + node_index);
    db->nodes_first_keys.erase(db->nodes_first_keys.begin() + node_index);
    db->nodes_keys_count.erase(db->nodes_keys_count.begin() + node_index);
    if (r != 0) {
        return r;
    }
    r = write_master_node(db);
    if (r != 0) {
        return r;
    }
    
    return 0;
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
        //nodes[i].keys = new std::set<std::string>();
    }
    
    // fill the new nodes with keys.
    struct modified_node * current_node = &nodes[0];
    unsigned int added_count = 0;
    std::set<std::string>::iterator it = keys.begin();
    while (it != keys.end()) {
        if (added_count >= MAX_KEYS_PER_NODE / MEAN_KEYS_PER_NODE_FACTOR) {
            current_node ++;
            added_count = 0;
        }
        current_node->keys.insert(* it);
        added_count ++;
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
    
    return 0;
}

void kvdbo_transaction_begin(kvdbo * db)
{
    kvdb_transaction_begin(db->db);
}

void kvdbo_transaction_abort(kvdbo * db)
{
    db->pending_keys.clear();
    db->pending_keys_delete.clear();
    kvdb_transaction_abort(db->db);
}

int kvdbo_transaction_commit(kvdbo * db)
{
    int r = flush_pending_keys(db);
    if (r < 0) {
        kvdb_transaction_abort(db->db);
        return r;
    }
    r = kvdb_transaction_commit(db->db);
    if (r < 0) {
        return r;
    }
    return 0;
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