#!/bin/bash
# Build and run the core mixr++ tests with no external dependencies
# (no SDL/FLAC/Opus submodules or audio device required).
#
# Usage: ./tests/run_core_tests.sh [--sanitize]
set -e

cd "$(dirname "$0")/.."

BUILD_DIR="build-core-tests"
mkdir -p "$BUILD_DIR"

CORE_SOURCES=(
    src/Impl/Impl.cpp
    src/mixr++/Context.cpp
    src/mixr++/AudioSource.cpp
    src/mixr++/AudioBuffer.cpp
    src/mixr++/Utils/ADPCM.cpp
    tests/test_mixr++_core.cpp
)

SANITIZER_FLAGS=""
if [ "$1" == "--sanitize" ]; then
    # ThreadSanitizer where the toolchain supports it; fall back to ASan+UBSan,
    # and finally to a plain build if no sanitizer runtime is installed
    # (e.g. some MinGW distributions ship the compiler without them).
    if echo 'int main(){}' | g++ -fsanitize=thread -x c++ - -o "$BUILD_DIR/san_probe" 2>/dev/null; then
        SANITIZER_FLAGS="-fsanitize=thread -g -O1"
        echo "Building with ThreadSanitizer"
    elif echo 'int main(){}' | g++ -fsanitize=address,undefined -x c++ - -o "$BUILD_DIR/san_probe" 2>/dev/null; then
        SANITIZER_FLAGS="-fsanitize=address,undefined -g -O1"
        echo "Building with AddressSanitizer/UBSan"
    else
        echo "No sanitizer runtime available in this toolchain; building without sanitizers"
    fi
fi

g++ -std=c++17 -Wall -Wextra -pthread \
    -Iinclude -Isrc/Impl \
    $SANITIZER_FLAGS \
    "${CORE_SOURCES[@]}" \
    -o "$BUILD_DIR/test_mixr++_core"

"$BUILD_DIR/test_mixr++_core"
