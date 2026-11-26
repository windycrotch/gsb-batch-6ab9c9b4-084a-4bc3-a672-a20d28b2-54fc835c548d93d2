# Goffmpeg
[![Build & Test](https://github.com/xfrr/goffmpeg/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/xfrr/goffmpeg/actions/workflows/build_and_test.yml)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/93e018e5008b4439acbb30d715b22e7f)](https://www.codacy.com/app/francisco.romero/goffmpeg?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=xfrr/goffmpeg&amp;utm_campaign=Badge_Grade)
[![Go Report Card](https://goreportcard.com/badge/github.com/xfrr/goffmpeg)](https://goreportcard.com/report/github.com/xfrr/goffmpeg)
[![GoDoc](https://godoc.org/github.com/xfrr/goffmpeg?status.svg)](https://godoc.org/github.com/xfrr/goffmpeg)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

FFMPEG wrapper written in GO

## Features

- [x] Transcoding
- [x] Streaming
- [x] Progress
- [x] Filters
- [x] Thumbnails
- [x] Watermark
- [ ] Concatenation
- [ ] Subtitles

## Dependencies
- [FFmpeg](https://www.ffmpeg.org/)
- [FFProbe](https://www.ffmpeg.org/ffprobe.html)

## Supported platforms

 - Linux
 - OS X
 - Windows

## Installation
Install the package with the following command:
```shell
go get github.com/xfrr/goffmpeg
```

## Usage
Check the [examples](./examples)

### Stopping a transcode

`Run` returns a channel that carries the process result; `Output` returns the
progress channel of the current run. Both belong to one transcoding
lifecycle:

- `Stop` on a transcoder that was never started, already finished or already
  stopping is safe and returns immediately.
- After `Stop` on a running task, that task's progress channel is closed and
  no progress parsed after the stop is published.
- Starting the same transcoder again installs a fresh process, stderr pipe and
  progress channel; the previous run is torn down first, so late output from
  the old task can never appear as progress of the new one.
- A failed process start follows the same teardown path, so no progress reader
  or pipe is left dangling.

The `examples/lifecycle-check` program (also `make lifecycle-check`) exercises
a midway stop and a stop-then-restart against the real `ffmpeg`/`ffprobe`
binaries using the repository 3gp sample.
