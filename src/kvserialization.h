#ifndef KVSERIALIZATION_H

#define KVSERIALIZATION_H

#include <string>

void kv_encode_uint64(std::string & buffer, uint64_t value);
size_t kv_decode_uint64(std::string & buffer, size_t position, uint64_t * p_value);
//size_t kv_cstr_decode_uint64(const char * buffer, size_t size, size_t position, uint64_t * p_value);

static inline size_t kv_cstr_decode_uint64(const char * buffer, size_t size, size_t position, uint64_t * p_value)
{
#if 0
    uint64_t value = 0;
    int s = 0;
    
    while (position < size) {
        unsigned char remainder = buffer[position];
        position ++;
        value += ((uint64_t) remainder & 0x7f) << s;
        if ((remainder & 0x80) == 0) {
            break;
        }
        s += 7;
    }
    
    * p_value = value;
    
    return position;
#endif
    const char * p = buffer + position;
    const char * final = p + size;
    uint64_t value = 0;
    int s = 0;
    
    while (p < final) {
        //position ++;
        value += (* p & 0x7f) << s;
        if ((* p & 0x80) == 0) {
            break;
        }
        p ++;
        s += 7;
    }
    
    * p_value = value;
    
    position = p + 1 - buffer;
    return position;
}

#endif

