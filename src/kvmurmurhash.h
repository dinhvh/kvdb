//
//  murmurhash.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/2/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef KVMURMURHASH_H
#define KVMURMURHASH_H

static inline uint32_t kv_murmur_hash(const char * data, size_t length, uint32_t seed)
{
    uint32_t m = 0x5bd1e995;
    uint32_t r = 24;
    unsigned char * bytes = (unsigned char *) data;
    
    uint32_t h = seed ^ (uint32_t) length;
    
    size_t len_4 = length >> 2;
    
    for (int i = 0; i < len_4; i++) {
        int i_4 = i << 2;
        uint32_t k = bytes[i_4 + 3];
        k = k << 8;
        k = k | bytes[i_4 + 2];
        k = k << 8;
        k = k | bytes[i_4 + 1];
        k = k << 8;
        k = k | bytes[i_4 + 0];
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }
    
    // avoid calculating modulo
    size_t len_m = len_4 << 2;
    size_t left = length - len_m;
    
    if (left != 0) {
        if (left >= 3) {
            h ^= (uint32_t) data[length - 3] << 16;
        }
        if (left >= 2) {
            h ^= (uint32_t) data[length - 2] << 8;
        }
        if (left >= 1) {
            h ^= (uint32_t) data[length - 1];
        }
        
        h *= m;
    }
    
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    
    return h;
}

#endif
