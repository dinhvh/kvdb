#set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_C_FLAGS "-std=c99 -g -O2")
IF(APPLE)
  set(CMAKE_CXX_FLAGS "-std=c++11 -stdlib=libc++ -g -O2")
ELSE()
  set(CMAKE_CXX_FLAGS "-std=gnu++0x")
ENDIF()

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
kvdbo.h
sfts.h
kvdb-types.h
DESTINATION
${CMAKE_CURRENT_BINARY_DIR}/include/kvdb
)

set(LZ4_DIR "../third-party/lz4/lib")

include_directories(${LZ4_DIR})

add_library (kvdb
    kvassert.c
    kvblock.cpp
    kvdb.cpp
    kvprime.c
    kvtable.cpp
    kvdbo.cpp
    sfts.cpp
    kvunicode.c
    kvserialization.cpp
    ConvertUTF.c
    ${LZ4_DIR}/lz4.c
    ${LZ4_DIR}/lz4hc.c
    ${LZ4_DIR}/lz4frame.c
    ${LZ4_DIR}/xxhash.c
)
