# The mixr Audio API
mixr is a fast, simple API for playing sounds on the CPU.

## Testing

The core concurrency suite (`tests/test_mixr++_core.cpp`) exercises the mixer
with synthetic PCM data and needs no audio device, media files or external
submodules. It covers stopping a source during playback, clearing its queue,
destroying a buffer that is queued/in use, source destruction and slot reuse,
state/finished callback de-duplication and ordering, and concurrent
stop/clear/destroy against a running mixer.

Run it directly (no CMake required):

```sh
./tests/run_core_tests.sh           # plain build
./tests/run_core_tests.sh --sanitize # TSan/ASan where the toolchain supports it
```

Or through CMake without any submodules:

```sh
cmake -S . -B build -DMIXR_CORE_TESTS_ONLY=ON
cmake --build build
ctest --test-dir build --output-on-failure
```
