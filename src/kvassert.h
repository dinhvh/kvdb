//
//  kvassert.h
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/1/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#ifndef KV_ASSERT_H
#define KV_ASSERT_H

#define KVDBAssert(cond) assertInternal(__FILE__, __LINE__, cond, #cond)

void assertInternal(const char * filename, unsigned int line, int cond, const char * condString);

#endif
