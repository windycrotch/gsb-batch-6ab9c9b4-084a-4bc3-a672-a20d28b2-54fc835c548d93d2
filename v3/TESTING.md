# Player session / event / media-reader lifecycle tests

The tests in `player_session_test.go` verify media-switch event fencing,
re-entrant `Stop`/`Release`, idempotent `Detach`, volume/mute persistence and
the read-seeker reference lifetime.

## No real audio/video files are required

The suite avoids media fixtures in three ways:

1. **Deterministic event injection.** Session generation, stale callback
   suppression and shutdown behaviour are exercised through
   `dispatchEventByID`, the same registry -> session gate -> user callback
   path used by the cgo event trampoline. No media timing is involved.
2. **In-memory reader spies.** Reader lifetime and `Duplicate` sharing are
   verified through the real `mediaBufferOpen` / `mediaBufferRead` /
   `mediaBufferSeek` production entry points using `io.ReadSeeker` spies.
3. **Generated WAV.** The single end-to-end playback test synthesises a short
   silent PCM WAV in memory (`psMakeWAV`) and plays it with the `dummy` audio
   output. Nothing is read from disk. Set `PS_SKIP_PLAYBACK=1` to skip it on
   minimal/libVLC-less setups; the lifecycle tests still run whenever libVLC
   can be initialised.

## Running

libVLC and its development headers are a build prerequisite of the package
(see the repository README). With a standard install:

```sh
# from the v3 module directory
go test -race -count=1 ./...

# or, from the repository root (a go.work exposes ./v3)
go test -race -count=1 ./v3
```

### Custom Windows + MinGW SDK location

If `vlc/vlc.h` and `libvlc.dll` are not in the default compiler/search paths,
point cgo and `PATH` at the extracted VLC SDK/runtime:

```bash
VLC=/c/path/to/vlc-3.0.x
# Directory containing the MinGW import library libvlc.dll.a (or the SDK lib dir).
IMPLIB=/c/path/to/import-lib-dir

PATH="$VLC:$PATH" \
VLC_PLUGIN_PATH="C:/path/to/vlc/plugins" \
CGO_CFLAGS="-IC:/path/to/vlc/sdk/include" \
CGO_LDFLAGS="-L$IMPLIB" \
go test -race -count=1 ./...
```

A MSVC `vlc.lib` import library may be rejected by GNU `ld`
("linker script file ... appears multiple times"). Generate a MinGW import
library from `libvlc.dll` instead:

```bash
gendef "$VLC/libvlc.dll"
dlltool -d libvlc.def -l libvlc.dll.a -D libvlc.dll -m i386:x86-64
```

## Coverage map

| Acceptance | Test |
| --- | --- |
| A1 stale first-media callback suppressed after switch | `TestPSStaleMediaCallbacksSuppressed`, `TestPSSyntheticWAVSwitchPlaysAndEnds` |
| A2 `Stop`/`Release` inside a callback; no double teardown | `TestPSStopInsideCallback`, `TestPSReleaseInsideCallback`, `TestPSReleaseIdempotentUnderConcurrency` |
| A3 repeated/concurrent `Detach` of one `EventID` is safe | `TestPSEventManagerDoubleDetachSafe`, `TestPSEventManagerConcurrentDetach` |
| A4 volume/mute persist across media switches | `TestPSPlayerSettingsPersistAcrossSwitch` |
| A5 released media denies late reader callbacks | `TestPSReaderDeniedAfterMediaRelease` |
| A6 `Duplicate` shares the reader until the last copy is released | `TestPSDuplicateSharesReaderLifetime`, `TestPSDuplicateReverseReleaseOrder` |
| A7 no user callback after `Player.Release` | `TestPSReleaseNoCallbacksAfterRelease` |
| Session gate semantics/drain | `TestPSEventGateEpochSuppression`, `TestPSEventGateWaitDrainsCallbacks`, `TestPSLifetimeAttachmentSurvivesSwitch` |
