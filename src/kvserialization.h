#ifndef KVSERIALIZATION_H

#define KVSERIALIZATION_H

#include <string>

void kv_encode_uint64(std::string & buffer, uint64_t value);
size_t kv_decode_uint64(std::string & buffer, size_t position, uint64_t * p_value);

#endif

