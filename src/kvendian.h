//
//  kvendian.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/1/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef kvdb_kvendian_h
#define kvdb_kvendian_h

#include <inttypes.h>

// Convert a 64 bit value to network byte order.
static inline uint64_t hton64(uint64_t val)
{
    union { uint64_t ll;
        uint32_t l[2];
    } w, r;
    
    // platform already in network byte order?
    if (htonl(1) == 1L)
        return val;
    w.ll = val;
    r.l[0] = htonl(w.l[1]);
    r.l[1] = htonl(w.l[0]);
    return r.ll;
}

// Convert a 64 bit value from network to host byte order.
static inline uint64_t ntoh64(uint64_t val)
{
    union { uint64_t ll;
        uint32_t l[2];
    } w, r;
    
    // platform already in network byte order?
    if (htonl(1) == 1L)
        return val;
    w.ll = val;
    r.l[0] = ntohl(w.l[1]);
    r.l[1] = ntohl(w.l[0]);
    return r.ll;
}

static inline uint64_t bytes_to_h64(char * bytes)
{
    uint64_t result = * (uint64_t *) bytes;
    return ntoh64(result);
}

static inline void h64_to_bytes(char * bytes, uint64_t value)
{
    value = hton64(value);
    * (uint64_t *) bytes = value;
}

static inline uint32_t bytes_to_h32(char * bytes)
{
    uint32_t result = * (uint32_t *) bytes;
    return ntohl(result);
}

static inline void h32_to_bytes(char * bytes, uint32_t value)
{
    value = htonl(value);
    * (uint32_t *) bytes = value;
}

static inline uint8_t bytes_to_h8(char * bytes)
{
    uint8_t result = * (uint8_t *) bytes;
    return result;
}

static inline void h8_to_bytes(char * bytes, uint8_t value)
{
    * (uint8_t *) bytes = value;
}

#endif
