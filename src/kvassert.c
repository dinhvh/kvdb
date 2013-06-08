//
//  kvassert.c
//  kvdb
//
//  Created by DINH Viêt Hoà on 6/1/13.
//  Copyright (c) 2013 etpan. All rights reserved.
//

#include <stdio.h>
#include <stdlib.h>

void assertInternal(const char * filename, unsigned int line, int cond, const char * condString)
{
    if (cond) {
        return;
    }
    
    fprintf(stderr, "%s:%i: assert %s\n", filename, line, condString);
    abort();
}
