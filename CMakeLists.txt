cmake_minimum_required(VERSION 3.22)
project(mwax_udb2sub LANGUAGES C)

set(CMAKE_C_STANDARD 17)
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)


set(CMAKE_CXX_FLAGS -O3 -Wall)

#set(CMAKE_EXE_LINKER_FLAGS -fopenmp)

add_executable(mwax_u2s
        src/mwax_udp2sub.c
)

include_directories(src)

target_link_libraries(mwax_u2s
        pthread
        m
        rt
        cfitsio
        stdc++
)
