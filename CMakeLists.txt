cmake_minimum_required(VERSION 3.20)
project(mixr)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# When ON, build only the self-contained core tests and the subset of the
# library they need, skipping every external submodule (SDL/FLAC/Opus/...).
option(MIXR_CORE_TESTS_ONLY "Build only the dependency-free core tests" OFF)

if (MIXR_CORE_TESTS_ONLY)
    add_library(mixr++ STATIC
            src/Impl/Impl.cpp
            src/mixr++/Context.cpp
            src/mixr++/AudioSource.cpp
            src/mixr++/AudioBuffer.cpp
            src/mixr++/Utils/ADPCM.cpp
    )
    target_include_directories(mixr++ PUBLIC include/)
    target_include_directories(mixr++ PRIVATE src/Impl)
    find_package(Threads REQUIRED)
    target_link_libraries(mixr++ PUBLIC Threads::Threads)

    add_executable(test_mixr++_core tests/test_mixr++_core.cpp)
    target_include_directories(test_mixr++_core PRIVATE src/Impl)
    target_link_libraries(test_mixr++_core PRIVATE mixr++)
    enable_testing()
    add_test(NAME mixr++_core COMMAND test_mixr++_core)
    return()
endif ()

file(GLOB MIXR++_SRC CONFIGURE_DEPENDS
        include/mixr++/*.h
        include/mixr++/Utils/*.h
        include/mixr++/Stream/*.h
        src/mixr++/*.h
        src/mixr++/*.cpp
        src/mixr++/Stream/*.h
        src/mixr++/Stream/*.cpp
        src/mixr++/Utils/*.h
        src/mixr++/Utils/*.cpp
        
        src/Impl/*.h
        src/Impl/*.cpp
)

if (WIN32)
    list(APPEND MIXR++_SRC src/mixr++/Device/SdlDevice.h src/mixr++/Device/SdlDevice.cpp)
elseif (LINUX)
    list(APPEND MIXR++_SRC src/mixr++/Device/AlsaDevice.h src/mixr++/Device/AlsaDevice.cpp)
endif()

add_library(mixr++ STATIC ${MIXR++_SRC})
target_include_directories(mixr++ PUBLIC include/)
target_include_directories(mixr++ PRIVATE src/Impl)

if (WIN32)
    add_subdirectory(lib/SDL)
    target_link_libraries(mixr++ PUBLIC SDL2::SDL2-static)
endif ()

add_subdirectory(lib/stbv)
target_link_libraries(mixr++ PUBLIC stb)

if (LINUX)
    target_link_libraries(mixr++ PUBLIC asound)
endif()

set(WITH_OGG OFF CACHE BOOL "" FORCE)
set(INSTALL_MANPAGES OFF CACHE BOOL "" FORCE)
set(ENABLE_MULTITHREADING OFF CACHE BOOL "" FORCE)
add_subdirectory(lib/flac)
target_link_libraries(mixr++ PUBLIC FLAC::FLAC++)

set(OP_DISABLE_DOCS ON)
set(OP_DISABLE_EXAMPLES ON)
set(OP_DISABLE_HTTP ON)
add_subdirectory(lib/ogg)
add_subdirectory(lib/opus)
add_subdirectory(lib/opusfile)
target_link_libraries(mixr++ PUBLIC OpusFile::opusfile)

target_include_directories(mixr++ PUBLIC lib/minimp3)

file(GLOB MIXR_SRC CONFIGURE_DEPENDS
        include/mixr/*.h
        include/mixr/Stream/*.h
        src/mixr/*.h
        src/mixr/*.cpp
)

add_library(mixr SHARED ${MIXR_SRC})
target_include_directories(mixr PUBLIC include/)
target_include_directories(mixr PRIVATE src/Impl)
target_link_libraries(mixr PUBLIC mixr++)

if (MSVC)
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /O2")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2")
    target_compile_definitions(mixr++ PUBLIC _DISABLE_CONSTEXPR_MUTEX_CONSTRUCTOR)
elseif (WIN32)
    target_link_options(mixr++ PRIVATE -static)
else()
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -s")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -s")
endif ()

add_executable(test_mixr++ tests/test_mixr++.cpp)
target_link_libraries(test_mixr++ PRIVATE mixr++)

add_executable(test_mixr tests/test_mixr.c)
target_link_libraries(test_mixr PRIVATE mixr)

# Self-contained core tests (synthetic audio, no device/file dependencies).
# Can be built without the SDL/FLAC/Opus submodules via:
#   cmake -DMIXR_BUILD_CORE_TESTS=ON -DMIXR_CORE_TESTS_ONLY=ON ..
option(MIXR_BUILD_CORE_TESTS "Build the self-contained core concurrency tests" ON)
if (MIXR_BUILD_CORE_TESTS)
    add_executable(test_mixr++_core tests/test_mixr++_core.cpp)
    target_include_directories(test_mixr++_core PRIVATE src/Impl)
    target_link_libraries(test_mixr++_core PRIVATE mixr++)
    find_package(Threads REQUIRED)
    target_link_libraries(test_mixr++_core PRIVATE Threads::Threads)
    add_test(NAME mixr++_core COMMAND test_mixr++_core)
endif ()