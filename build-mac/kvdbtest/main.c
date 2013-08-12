//
//  main.c
//  kvdbtest
//
//  Created by DINH Viêt Hoà on 6/8/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#include <stdio.h>
#include "kvdb.h"
#include <uuid/uuid.h>
#include <stdlib.h>
#include <string.h>

static void enumerate_keys_callback(kvdb * db, struct kvdb_enumerate_cb_params * params, void * data, int * stop) {
	printf("key = %.*s\n", (int) params->key_size, params->key);
}

int main(void)
{
    uuid_t key;
    uuid_string_t keyString;
    uuid_t value;
    uuid_string_t valueString;
    
    struct kvdb * db;
    db = kvdb_new("kvdb-test.kvdb");
    kvdb_open(db);
    
    int r;
    
    char * data;
    size_t data_size;
    r = kvdb_get(db, "hoa", 3, &data, &data_size);
    fprintf(stderr, "1: ");
    if (r == 0) {
        fprintf(stderr, "found\n");
        free(data);
    }
    else {
        fprintf(stderr, "not found\n");
    }
    
    kvdb_set(db, "hoa", 3, "test", 4);
    r = kvdb_get(db, "hoa", 3, &data, &data_size);
    fprintf(stderr, "2: ");
    if (r == 0) {
        fprintf(stderr, "found\n");
        free(data);
    }
    else {
        fprintf(stderr, "not found\n");
    }
	
	r = kvdb_enumerate_keys(db, enumerate_keys_callback, NULL);
    
    kvdb_delete(db, "hoa", 3);
    r = kvdb_get(db, "hoa", 3, &data, &data_size);
    fprintf(stderr, "3: ");
    if (r == 0) {
        fprintf(stderr, "found\n");
        free(data);
    }
    else {
        fprintf(stderr, "not found\n");
    }
    
    kvdb_set(db, "hoa", 3, "test", 4);
    r = kvdb_get(db, "hoa", 3, &data, &data_size);
    fprintf(stderr, "4: ");
    if (r == 0) {
        fprintf(stderr, "found\n");
        free(data);
    }
    else {
        fprintf(stderr, "not found\n");
    }
    kvdb_delete(db, "hoa", 3);
    
#define COUNT 1000
    char * keys[COUNT];
    for(unsigned int i = 0 ; i < COUNT ; i ++) {
        //fprintf(stderr, "add %i\n", i);
        uuid_generate(key);
        uuid_unparse_lower(key, keyString);
        uuid_generate(value);
        uuid_unparse_lower(value, valueString);
        char * dupKey = malloc(37);
        memcpy(dupKey, keyString, 36);
        dupKey[36] = 0;
        keys[i] = dupKey;
        kvdb_set(db, keyString, 36, valueString, 36);
    }
    
    kvdb_close(db);
    kvdb_free(db);
    
    db = kvdb_new("kvdb-test.kvdb");
    kvdb_open(db);
    
    for(unsigned int i = 0 ; i < COUNT / 2 ; i ++) {
        char * key = keys[i];
        kvdb_delete(db, key, 36);
    }
    
    for(unsigned int i = 0 ; i < COUNT / 2 ; i ++) {
        char * value;
        size_t value_size;
        char * key = keys[i];
        int r = kvdb_get(db, key, 36, &value, &value_size);
        if (r == 0) {
            fprintf(stderr, "still exists %s\n", key);
            free(value);
        }
    }

    for(unsigned int i = COUNT / 2 ; i < COUNT ; i ++) {
        char * value;
        size_t value_size;
        char * key = keys[i];
        int r = kvdb_get(db, key, 36, &value, &value_size);
        if (r < 0) {
            fprintf(stderr, "could not get key %s %i\n", key, i);
        }
        else {
            free(value);
        }
    }
    
    kvdb_close(db);
    kvdb_free(db);
}
