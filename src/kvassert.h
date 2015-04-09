//
//  kvassert.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/1/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef KV_ASSERT_H
#define KV_ASSERT_H

#ifdef __cplusplus
extern "C" {
#endif

#define kv_assert(cond) kvdb_assert_internal(__FILE__, __LINE__, cond, #cond)

void kvdb_assert_internal(const char * filename, unsigned int line, int cond, const char * condString);

#ifdef __cplusplus
}
#endif

#endif
