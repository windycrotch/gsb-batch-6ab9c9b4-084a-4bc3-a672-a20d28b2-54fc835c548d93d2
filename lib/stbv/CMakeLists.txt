cmake_minimum_required(VERSION 3.20)
project(stb LANGUAGES C)

add_library(stb STATIC stb_vorbis.c)
target_include_directories(stb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(stb PROPERTIES LINKER_LANGUAGE C)