#set(CMAKE_INCLUDE_CURRENT_DIR ON)

file(GLOB_RECURSE
    source_files
    *.h
    *.m
    *.c
)

SET_SOURCE_FILES_PROPERTIES(
	${source_files}
    PROPERTIES LANGUAGE C
)

file(COPY
kvdb.h
DESTINATION
${CMAKE_CURRENT_BINARY_DIR}/include/kvdb
)

add_library (kvdb
    kvassert.c
    kvblock.c
    kvdb.c
    kvprime.c
    kvtable.c
    kvdbo.cpp
)
