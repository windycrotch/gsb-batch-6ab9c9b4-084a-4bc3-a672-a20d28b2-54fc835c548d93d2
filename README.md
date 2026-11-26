# Go HLS Transcoder

[![GoDoc](https://godoc.org/github.com/rendyfebry/go-hls-transcoder?status.svg)](https://godoc.org/github.com/rendyfebry/go-hls-transcoder)

Simple golang HLS transcoder with ffmpeg.

## Prerequisite

- Golang 1.24+ (the module builds with any modern Go toolchain)
- ffmpeg

## Atomic multi-rendition transcoding

`GenerateHLSMulti` turns one source into several resolutions **as a single
all-or-nothing operation**:

- every rendition is an ffmpeg subprocess that is waited on; start failures
  (missing executable), non-zero exits and cancellation are reported, and the
  returned error names the failed resolution (`errors.Is` keeps matching
  `context.Canceled` / `context.DeadlineExceeded`);
- each call renders into a **private staging directory** inside the target
  directory and publishes its files only after all renditions succeed;
- the master playlist is then renamed into place atomically, so readers never
  see a half-written playlist and concurrent callers cannot observe each
  other's unfinished master;
- a failed or cancelled call removes only its own staging directory - another
  concurrent (successful) caller's artifacts are never touched;
- resolutions are emitted in the caller's order and duplicates are kept once;
- `ffmpegPath` is injectable, so a local fake ffmpeg script/binary is enough to
  test success, failure and concurrency without a real ffmpeg install.

```go
package main

import (
    "context"
    "log"
    "time"

    hls "github.com/rendyfebry/go-hls-transcoder"
)

func main() {
    // Repository-style call (error only):
    err := hls.GenerateHLSMulti(
        "/usr/local/bin/ffmpeg",
        "/assets/raw/movie.mov",
        "/assets/hls",
        []string{"480p", "720p", "1080p"}, // order preserved, duplicates removed
    )
    if err != nil {
        log.Fatal(err) // e.g. `hls: rendition "720p" failed: ...`
    }

    // Context / timeout / cancellation + published paths and content:
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()

    result, err := hls.TranscodeMulti(
        ctx,
        "/usr/local/bin/ffmpeg",
        "/assets/raw/movie.mov",
        "/assets/hls",
        []string{"360p", "720p"},
    )
    if err != nil {
        log.Fatal(err) // cancel/timeout kills the ffmpeg process tree and cleans staging
    }
    log.Println("master playlist:", result.MasterPlaylistPath) // .../playlist.m3u8
    log.Println(result.MasterPlaylist)
}
```

The master playlist is written to `playlist.m3u8` (the historical
`GeneratePlaylist` default) and uses the unchanged `#EXT-X-STREAM-INF` field
format. `MultiOptions.MasterName` allows a different file name.

`GenerateHLSMulti` is intentionally flexible about its arguments (context may
come first, resolutions may be a slice or trailing strings, and a
`MultiOptions` value is accepted). The strongly typed entry points are
`TranscodeMulti`, `TranscodeMultiWithOptions`,
`GenerateHLSMultiWithResult` and `GenerateHLSMultiContext`. Common alias
names (`GenerateHLSAll`, `GenerateMultiHLS`, `TranscodeHLS`,
`TranscodeAll`, `GenerateRenditions`, ...) delegate to the same implementation.

See `multi_test.go` for hermetic success/failure/deduplication/concurrency/
cancellation tests built on a compiled fake ffmpeg, and
`multi_windows_test.go` for `.bat` fake coverage.

## Single-rendition and custom invocations

The existing entry points remain available and now correctly wait for ffmpeg
to finish instead of returning after `Start`:

```go
err := hls.GenerateHLS(ffmpegPath, srcPath, targetPath, "480p")

err = hls.GenerateHLSCustom(ffmpegPath, options)

// Cancellable custom invocation:
err = hls.GenerateHLSCustomContext(ctx, ffmpegPath, options)
```

`GenerateHLSVariant` / `GeneratePlaylist` keep their presets and playlist
field format; `GeneratePlaylist` now also returns the file-write error (plain
statement-style calls continue to compile).

## Examples (historical)

Generate HLS files + playlist from a mov file.

```go
package main

import (
    hls "github.com/rendyfebry/go-hls-transcoder"
)

func main() {
    ffmpegPath := "/usr/local/bin/ffmpeg"
    srcPath := "/assets/raw/movie.mov"
    targetPath := "/assets/hls"
    resOptions := []string{"480p", "720p"}

    // New: one atomic call replaces the loop + manual playlist generation.
    if err := hls.GenerateHLSMulti(ffmpegPath, srcPath, targetPath, resOptions); err != nil {
        panic(err)
    }
}
```

## License

MIT licensed. See the LICENSE.md file for details.
