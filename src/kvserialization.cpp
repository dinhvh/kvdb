#include "kvserialization.h"

#include <stdlib.h>

void kv_encode_uint64(std::string & buffer, uint64_t value)
{
    char valuestr[10];
    int len = 0;
    while (1) {
        unsigned char remainder = value & 0x7f;
        value = value >> 7;
        if (value == 0) {
            // last item to write.
            valuestr[len] = remainder;
            len ++;
            break;
        }
        else {
            valuestr[len] = remainder | 0x80;
            len ++;
        }
    }
    buffer.append(valuestr, len);
}

static inline size_t internal_cstr_decode_uint64(const char * buffer, size_t size, size_t position, uint64_t * p_value)
{
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

size_t kv_decode_uint64(std::string & buffer, size_t position, uint64_t * p_value)
{
    return internal_cstr_decode_uint64(buffer.c_str(), buffer.length(), position, p_value);
}
