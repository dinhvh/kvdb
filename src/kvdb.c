#include "kvdb.h"

#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <lz4.h>

#include "kvassert.h"
#include "kvendian.h"
#include "kvtypes.h"
#include "kvprime.h"
#include "kvpaddingutils.h"
#include "kvbloom.h"
#include "kvmurmurhash.h"
#include "kvtable.h"
#include "kvblock.h"

#define MARKER "KVDB"
#define VERSION 5

static int kvdb_debug = 0;

static int internal_kvdb_set(kvdb * db, const char * key, size_t key_size, const char * value, size_t value_size);
static int internal_kvdb_get2(kvdb * db, const char * key, size_t key_size,
              char ** p_value, size_t * p_value_size, size_t * p_free_size);
static int kvdb_get2(kvdb * db, const char * key, size_t key_size,
                     char ** p_value, size_t * p_value_size, size_t * p_free_size);

kvdb * kvdb_new(const char * filename)
{
    kvdb * db = malloc(sizeof(* db));
    KVDBAssert(filename != NULL);
    db->kv_filename = strdup(filename);
    KVDBAssert(db->kv_filename != NULL);
    db->kv_fd = -1;
    db->kv_opened = 0;
    db->kv_firstmaxcount = kv_getnextprime(KV_FIRST_TABLE_MAX_COUNT);
    db->kv_compression_type = KVDB_COMPRESSION_TYPE_LZ4;
    db->kv_filesize = NULL;
    db->kv_free_blocks = NULL;
    db->kv_first_table = NULL;
    db->kv_current_table = NULL;
    
    return db;
}

void kvdb_free(kvdb * db)
{
    if (db->kv_opened) {
        fprintf(stderr, "should be closed before freeing - %s\n", db->kv_filename);
    }
    free(db->kv_filename);
    free(db);
}

void kvdb_set_compression_type(kvdb * db, int compression_type)
{
    if (db->kv_opened) {
        return;
    }
    db->kv_compression_type = compression_type;
}

int kvdb_get_compression_type(kvdb * db)
{
    return db->kv_compression_type;
}

int kvdb_open(kvdb * db)
{
    int r;
    struct stat stat_buf;
    int create_file = 0;
    
    if (db->kv_opened)
        return -1;
    
    db->kv_pagesize = getpagesize();
    
    db->kv_fd = open(db->kv_filename, O_RDWR | O_CREAT, 0600);
    if (db->kv_fd == -1) {
        fprintf(stderr, "open failed\n");
        return -1;
    }
    
    r = fstat(db->kv_fd, &stat_buf);
    if (r < 0) {
        close(db->kv_fd);
        // close file.
        fprintf(stderr, "fstat failed\n");
        return -1;
    }
    
    uint64_t firstmaxcount = kv_getnextprime(KV_FIRST_TABLE_MAX_COUNT);
    uint64_t first_mapping_size = KV_HEADER_SIZE + KV_TABLE_SIZE(firstmaxcount);
    
    char data[4 + 4 + 8 + 1];
    
    if (stat_buf.st_size == 0) {
        create_file = 1;
        r = ftruncate(db->kv_fd, KV_PAGE_ROUND_UP(db, first_mapping_size));
        if (r < 0) {
            close(db->kv_fd);
            // close file.
            fprintf(stderr, "truncate failed\n");
            return -1;
        }
        memcpy(data, MARKER, 4);
        h32_to_bytes(&data[4], VERSION);
        h64_to_bytes(&data[4 + 4], firstmaxcount);
        data[4 + 4 + 8] = db->kv_compression_type;
        write(db->kv_fd, data, sizeof(data));
        
        kv_table_header_write(db, KV_HEADER_SIZE, firstmaxcount);
    }
    
    char marker[4];
    uint32_t version;
    int compression_type;
    pread(db->kv_fd, data, sizeof(data), 0);
    memcpy(marker, data, 4);
    version = bytes_to_h32(&data[4]);
    firstmaxcount = bytes_to_h64(&data[4 + 4]);
    compression_type = data[4 + 4 + 8];
    
    r = memcmp(marker, MARKER, 4);
    if (r != 0) {
        fprintf(stderr, "file corrupted\n");
        return -1;
    }
    if (version != VERSION) {
        fprintf(stderr, "bad file version\n");
        return -1;
    }
    
    db->kv_firstmaxcount = firstmaxcount;
    db->kv_compression_type = compression_type;
    db->kv_opened = 1;
    
    r = kv_tables_setup(db);
    if (r < 0) {
        fprintf(stderr, "can't map files\n");
        return -1;
    }
    
    char * first_mapping = db->kv_first_table->kv_mapping.kv_bytes;
    db->kv_filesize = (uint64_t *) (first_mapping + KV_HEADER_FILESIZE_OFFSET);
    db->kv_free_blocks = (uint64_t *) (first_mapping + KV_HEADER_FREELIST_OFFSET);
    if (create_file) {
        * db->kv_filesize = hton64(first_mapping_size);
    }
    
    return 0;
}

void kvdb_close(kvdb * db)
{
    if (!db->kv_opened) {
        return;
    }
    
    kv_tables_unsetup(db);
    close(db->kv_fd);
    db->kv_opened = 0;
}

int kvdb_set(kvdb * db, const char * key, size_t key_size, const char * value, size_t value_size)
{
    if (db->kv_compression_type == KVDB_COMPRESSION_TYPE_RAW) {
        return internal_kvdb_set(db, key, key_size, value, value_size);
    }
    else if (db->kv_compression_type == KVDB_COMPRESSION_TYPE_LZ4) {
        if (value_size == 0) {
            return internal_kvdb_set(db, key, key_size, value, value_size);
        }
        else {
            int max_compressed_size = LZ4_compressBound((int) value_size);
            char * compressed_value = NULL;
            int allocated = 0;
            if (max_compressed_size < 4096) {
                compressed_value = alloca(sizeof(uint32_t) + max_compressed_size);
            }
            else {
                allocated = 1;
                compressed_value = malloc(sizeof(uint32_t) + max_compressed_size);
            }
            * (uint32_t *) compressed_value = htonl(value_size);
            int compressed_value_size = LZ4_compress(value, compressed_value + sizeof(uint32_t), (int) value_size);
            int r = internal_kvdb_set(db, key, key_size, compressed_value, sizeof(uint32_t) + compressed_value_size);
            if (allocated) {
                free(compressed_value);
            }
            return r;
        }
    }
    else {
        KVDBAssert(0);
        return 0;
    }
}

static int internal_kvdb_set(kvdb * db, const char * key, size_t key_size, const char * value, size_t value_size)
{
    uint32_t hash_value[KV_BLOOM_FILTER_HASH_COUNT];
    table_bloom_filter_compute_hash(hash_value, KV_BLOOM_FILTER_HASH_COUNT, key, key_size);
    
    int r;
    r = kvdb_delete(db, key, key_size);
    if (r == -1) {
        // Not found: ignore.
    }
    else if (r == -2) {
        return -2;
    }
    
    r = kv_select_table(db);
    if (r < 0) {
        return -2;
    }
    struct kvdb_table * table = db->kv_current_table;
    
    uint32_t idx = hash_value[0] % ntoh64(* table->kv_maxcount);
    struct kvdb_item * item = &table->kv_items[idx];
    uint64_t offset = kv_block_create(db, ntoh64(item->kv_offset), hash_value[0], key, key_size, value, value_size);
    if (offset == 0) {
        return -2;
    }
    item->kv_offset = hton64(offset);
    table_bloom_filter_set(table, hash_value + 1, KV_BLOOM_FILTER_HASH_COUNT - 1);
    
    uint64_t count;
    count = ntoh64(* table->kv_count);
    count ++;
    * table->kv_count = hton64(count);
    
    return 0;
}

#define PRE_READ_KEY_SIZE 128
#define MAX_ALLOCA_SIZE 4096

static void show_bucket(kvdb * db, uint32_t idx)
{
    struct kvdb_table * table = db->kv_first_table;
    struct kvdb_item * item = &table->kv_items[idx];
    uint64_t next_offset = ntoh64(item->kv_offset);
    
    fprintf(stderr, "bucket: %llu\n", (unsigned long long) idx);
    
    uint64_t previous_offset = 0;
    
    // Run through all chained blocks in the bucket.
    while (next_offset != 0) {
        uint32_t current_hash_value;
        uint64_t current_offset;
        uint8_t log2_size;
        uint64_t current_key_size;
        char * current_key;
        ssize_t r;
        
        current_offset = next_offset;
        char block_header_data[KV_BLOCK_KEY_BYTES_OFFSET + PRE_READ_KEY_SIZE];
        
        r = pread(db->kv_fd, block_header_data, sizeof(block_header_data), (off_t) next_offset);
        if (r < 0)
            return;
        char * p = block_header_data;
        next_offset = bytes_to_h64(p);
        p += 8;
        current_hash_value = bytes_to_h32(p);
        p += 4;
        log2_size = bytes_to_h8(p);
        p += 1;
        current_key_size = bytes_to_h64(p);
        p += 8;
        current_key = block_header_data + KV_BLOCK_KEY_BYTES_OFFSET;
        
        fprintf(stderr, "previous, current, next: %llu, %llu , %llu\n", (unsigned long long) previous_offset, (unsigned long long) current_offset, (unsigned long long) next_offset);
        fprintf(stderr, "hash: %llu\n", (unsigned long long) current_hash_value);
        
        char * allocated = NULL;
        if (current_key_size > PRE_READ_KEY_SIZE) {
            if (current_key_size <= MAX_ALLOCA_SIZE) {
                current_key = alloca(current_key_size);
            }
            else {
                allocated = malloc((size_t) current_key_size);
                current_key = allocated;
            }
            r = pread(db->kv_fd, current_key, (size_t) current_key_size, (off_t) (current_offset + KV_BLOCK_KEY_BYTES_OFFSET));
            if (r < 0) {
                if (allocated != NULL) {
                    free(allocated);
                }
                return;
            }
        }
        fprintf(stderr, "key: %.*s\n", (int) current_key_size, current_key);
        if (allocated != NULL) {
            free(allocated);
        }
        previous_offset = current_offset;
    }
    fprintf(stderr, "-----\n");
}

static int find_key(kvdb * db, const char * key, size_t key_size,
                    findkey_callback callback, void * cb_data)
{
    uint32_t hash_values[KV_BLOOM_FILTER_HASH_COUNT];
    table_bloom_filter_compute_hash(hash_values, KV_BLOOM_FILTER_HASH_COUNT, key, key_size);
    
    struct find_key_cb_params params;
    params.key = key;
    params.key_size = key_size;
    
    // Run through all tables.
    struct kvdb_table * table = db->kv_first_table;
    while (table != NULL) {
        // Is the key likely to be in this table?
        // Use a bloom filter to guess.
        if (!table_bloom_filter_might_contain(table, hash_values + 1, KV_BLOOM_FILTER_HASH_COUNT - 1)) {
            table = table->kv_next_table;
            continue;
        }
        
        // Find a bucket.
        uint64_t previous_offset = 0;
        uint32_t idx = hash_values[0] % ntoh64(* table->kv_maxcount);
        struct kvdb_item * item = &table->kv_items[idx];
        uint64_t next_offset = ntoh64(item->kv_offset);
        if (kvdb_debug) {
            fprintf(stderr, "before\n");
            show_bucket(db, idx);
        }
        
        // Run through all chained blocks in the bucket.
        while (next_offset != 0) {
            uint32_t current_hash_value;
            uint64_t current_offset;
            uint8_t log2_size;
            uint64_t current_key_size;
            char * current_key;
            ssize_t r;
            
            current_offset = next_offset;
            char block_header_data[KV_BLOCK_KEY_BYTES_OFFSET + PRE_READ_KEY_SIZE];
            
            r = pread(db->kv_fd, block_header_data, sizeof(block_header_data), (off_t) next_offset);
            if (r < 0)
                return -1;
            char * p = block_header_data;
            next_offset = bytes_to_h64(p);
            p += 8;
            current_hash_value = bytes_to_h32(p);
            p += 4;
            log2_size = bytes_to_h8(p);
            p += 1;
            current_key_size = bytes_to_h64(p);
            p += 8;
            current_key = block_header_data + KV_BLOCK_KEY_BYTES_OFFSET;
            
            if (current_hash_value != hash_values[0]) {
                previous_offset = current_offset;
                continue;
            }
            
            int cmp_result;
            
            if (current_key_size != key_size) {
                previous_offset = current_offset;
                continue;
            }
            char * allocated = NULL;
            if (current_key_size > PRE_READ_KEY_SIZE) {
                if (current_key_size <= MAX_ALLOCA_SIZE) {
                    current_key = alloca(current_key_size);
                }
                else {
                    allocated = malloc((size_t) current_key_size);
                    current_key = allocated;
                }
                r = pread(db->kv_fd, current_key, (size_t) current_key_size, (off_t) (current_offset + KV_BLOCK_KEY_BYTES_OFFSET));
                if (r < 0) {
                    if (allocated != NULL) {
                        free(allocated);
                    }
                    return -1;
                }
            }
            cmp_result = memcmp(key, current_key, key_size) != 0;
            if (allocated != NULL) {
                free(allocated);
            }
            if (cmp_result != 0) {
                previous_offset = current_offset;
                continue;
            }
            
            params.previous_offset = previous_offset;
            params.current_offset = current_offset;
            params.next_offset = next_offset;
            params.item = item;
            params.table_count = table->kv_count;
            params.log2_size = log2_size;
            
            callback(db, &params, cb_data);
            
            if (kvdb_debug) {
                fprintf(stderr, "after\n");
                show_bucket(db, idx);
            }
            
            return 0;
        }
        table = table->kv_next_table;
    }

    return 0;
}

struct delete_key_params {
    int result;
    int found;
};

static void delete_key_callback(kvdb * db, struct find_key_cb_params * params,
                                void * data) {
    struct delete_key_params * deletekeyparams = data;
    ssize_t write_count;
    int r;
    
    if (params->previous_offset == 0) {
        params->item->kv_offset = hton64(params->next_offset);
    }
    else {
        uint64_t offset_to_write = hton64(params->next_offset);
        write_count = pwrite(db->kv_fd, &offset_to_write, sizeof(offset_to_write), params->previous_offset);
        if (write_count < 0) {
            deletekeyparams->result = -2;
            return;
        }
    }
    r = kv_block_recycle(db, params->current_offset);
    if (r < 0) {
        deletekeyparams->result = -2;
        return;
    }
    
    * params->table_count = hton64(ntoh64(* params->table_count) - 1);
    deletekeyparams->result = 0;
    deletekeyparams->found = 1;
}

int kvdb_delete(kvdb * db, const char * key, size_t key_size)
{
    int r;
    struct delete_key_params data;
    
    data.found = 0;
    data.result = -1;
    
    r = find_key(db, key, key_size, delete_key_callback, &data);
    if (r < 0) {
        return -2;
    }
    if (data.result < 0) {
        return data.result;
    }
    if (!data.found) {
        return -1;
    }
    
    return 0;
}

struct read_value_params {
    uint64_t value_size;
    char * value;
    int result;
    int found;
    size_t free_size;
};

static void read_value_callback(kvdb * db, struct find_key_cb_params * params,
                                void * data)
{
    struct read_value_params * readparams = data;
    ssize_t r;
    
    uint64_t value_size;
    r = pread(db->kv_fd, &value_size, sizeof(value_size),
              params->current_offset + 8 + 4 + 1 + 8 + params->key_size);
    if (r < 0) {
        readparams->result = -2;
        return;
    }
    
    value_size = ntoh64(value_size);
    readparams->value_size = value_size;
    readparams->value = malloc((size_t) value_size);
    
    uint64_t remaining = value_size;
    char * value_p = readparams->value;
    while (remaining > 0) {
        ssize_t count = pread(db->kv_fd, value_p, (size_t) remaining,
                              params->current_offset + 8 + 4 + 1 + 8 + params->key_size + 8);
        if (count < 0) {
            readparams->result = -2;
            free(readparams->value);
            readparams->value = NULL;
            return;
        }
        remaining -= count;
        value_p += count;
    }
    
    readparams->result = 0;
    readparams->found = 1;
    readparams->free_size = (1 << params->log2_size) - (value_size + params->key_size);
}

int kvdb_get(kvdb * db, const char * key, size_t key_size,
             char ** p_value, size_t * p_value_size)
{
    return kvdb_get2(db, key, key_size, p_value, p_value_size, NULL);
}

static int kvdb_get2(kvdb * db, const char * key, size_t key_size,
                     char ** p_value, size_t * p_value_size, size_t * p_free_size)
{
    if (db->kv_compression_type == KVDB_COMPRESSION_TYPE_RAW) {
        return internal_kvdb_get2(db, key, key_size, p_value, p_value_size, p_free_size);
    }
    else if (db->kv_compression_type == KVDB_COMPRESSION_TYPE_LZ4) {
        char * compressed_value;
        size_t compressed_value_size;
        int r = internal_kvdb_get2(db, key, key_size, &compressed_value, &compressed_value_size, p_free_size);
        if (r < 0) {
            return r;
        }
        if (compressed_value_size == 0) {
            * p_value = NULL;
            * p_value_size = 0;
            return 0;
        }
    
        size_t value_size = ntohl(* (uint32_t *) compressed_value);
        char * value = malloc(value_size);
        LZ4_decompress_fast(compressed_value + sizeof(uint32_t), value, (int) value_size);
        free(compressed_value);
        if (p_free_size != NULL) {
            * p_free_size = 0;
        }
        * p_value_size = value_size;
        * p_value = value;
        return 0;
    }
    else {
        KVDBAssert(0);
        return 0;
    }
}

static int internal_kvdb_get2(kvdb * db, const char * key, size_t key_size,
              char ** p_value, size_t * p_value_size, size_t * p_free_size)
{
    int r;
    struct read_value_params data;
    
    data.value_size = 0;
    data.value = NULL;
    data.result = -1;
    data.found = 0;
    data.free_size = 0;

    r = find_key(db, key, key_size, read_value_callback, &data);
    if (r < 0) {
        return -2;
    }
    if (data.result < 0) {
        return data.result;
    }
    if (!data.found) {
        return -1;
    }
    
    if (p_free_size != NULL) {
        * p_free_size = data.free_size;
    }
    
    * p_value = data.value;
    * p_value_size = (size_t) data.value_size;
    
    return 0;
}

int kvdb_enumerate_keys(kvdb * db, kvdb_enumerate_callback callback, void * cb_data)
{
    struct kvdb_table * table = db->kv_first_table;
	struct kvdb_enumerate_cb_params cb_params;
	int stop = 0;
	
    // Run through all tables.
    while (table != NULL) {
		struct kvdb_item * item = table->kv_items;
		// Run through all buckets.
		uint64_t count = ntoh64(*table->kv_maxcount);
		while (count) {
			uint64_t current_offset = ntoh64(item->kv_offset);
			// Run through all chained blocks in the bucket.
			while (current_offset != 0) {
				char block_header_data[KV_BLOCK_KEY_BYTES_OFFSET + PRE_READ_KEY_SIZE];
				ssize_t r = pread(db->kv_fd, block_header_data, sizeof(block_header_data), (off_t) current_offset);
				if (r < 0) {
					return -2;
				}
				char * p = block_header_data;
				uint64_t next_offset = bytes_to_h64(p);
				p += 8+4+1; // ignore hash_value and log2_size
				size_t current_key_size = (size_t) bytes_to_h64(p);
				p += 8;
				char * current_key = block_header_data + KV_BLOCK_KEY_BYTES_OFFSET;
				char * allocated = NULL;
				if (current_key_size > PRE_READ_KEY_SIZE) {
					if (current_key_size <= MAX_ALLOCA_SIZE) {
						current_key = alloca(current_key_size);
					}
					else {
						allocated = malloc(current_key_size);
						current_key = allocated;
					}
					r = pread(db->kv_fd, current_key, current_key_size, (off_t) (current_offset + KV_BLOCK_KEY_BYTES_OFFSET));
					if (r < 0) {
						if (allocated != NULL) {
							free(allocated);
						}
						return -2;
					}
				}
				cb_params.key = current_key;
				cb_params.key_size = current_key_size;
				callback(db, &cb_params, cb_data, &stop);
				if (allocated != NULL) {
					free(allocated);
				}
				if (stop) {
					return 0;
				}
                current_offset = next_offset;
			}
			item ++;
			count --;
		}
		table = table->kv_next_table;
	}
	return 0;
}
