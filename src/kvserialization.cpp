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

size_t kv_decode_uint64(std::string & buffer, size_t position, uint64_t * p_value)
{
    uint64_t value = 0;
    int s = 0;
    
    while (1) {
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
}
