[![GoReportCard](http://goreportcard.com/badge/github.com/asticode/go-astisub)](http://goreportcard.com/report/github.com/asticode/go-astisub)
[![GoDoc](https://godoc.org/github.com/asticode/go-astisub?status.svg)](https://godoc.org/github.com/asticode/go-astisub)
[![Test](https://github.com/asticode/go-astisub/actions/workflows/test.yml/badge.svg)](https://github.com/asticode/go-astisub/actions/workflows/test.yml)
[![Coveralls](https://coveralls.io/repos/github/asticode/go-astisub/badge.svg?branch=master)](https://coveralls.io/github/asticode/go-astisub)

This is a Golang library to manipulate subtitles. 

It allows you to manipulate `srt`, `stl`, `ttml`, `ssa/ass`, `webvtt` and `teletext` files for now.

Available operations are `parsing`, `writing`, `applying linear correction`, `syncing`, `fragmenting`, `unfragmenting`, `merging` and `optimizing`.

# Installation

To install the library:

    go get github.com/asticode/go-astisub

To install the CLI:

    go install github.com/asticode/go-astisub/cmd/astisub        

# Using the library in your code

WARNING: the code below doesn't handle errors for readibility purposes. However you SHOULD!

```go
// Open subtitles
s1, _ := astisub.OpenFile("/path/to/example.ttml")
s2, _ := astisub.ReadFromSRT(bytes.NewReader([]byte("1\n00:01:00.000 --> 00:02:00.000\nCredits")))

// Add a duration to every subtitles (syncing)
s1.Add(-2*time.Second)

// Fragment the subtitles
s1.Fragment(2*time.Second)

// Merge subtitles
s1.Merge(s2)

// Optimize subtitles
s1.Optimize()

// Unfragment the subtitles
s1.Unfragment()

// Apply linear correction
s1.ApplyLinearCorrection(1*time.Second, 2*time.Second, 5*time.Second, 7*time.Second)

// Write subtitles
s1.Write("/path/to/example.srt")
var buf = &bytes.Buffer{}
s2.WriteToTTML(buf)
```

# Batch converting files

`ConvertBatch` converts any number of source/target pairs in one call. It
returns exactly one `BatchResult` per `BatchJob`, at the same index and in the
same order as the inputs; a parse or write failure on one file never aborts the
other files:

```go
jobs := []astisub.BatchJob{
    {Source: "first.srt",  Target: "first.ttml"},
    {Source: "second.srt", Target: "second.vtt", Optimize: true},
}
results := astisub.ConvertBatch(ctx, jobs, astisub.WithConcurrency(4))
for _, r := range results {
    if !r.OK {
        log.Printf("%s -> %s failed (cancelled=%v): %s", r.Source, r.Target, r.Cancelled, r.Reason)
    }
}
```

Guarantees:

- **Same parse/write chain per file**: jobs go through `Open` and the same
  extension-dispatched `WriteTo*` implementations as single-file conversion.
- **Atomic target replacement**: each job is rendered into a unique temporary
  file in the target directory and committed with a rename only after full
  success. If parsing, rendering, cancellation or commit fails, the temporary
  file is removed and a pre-existing target keeps its original bytes. Repeating
  the same batch therefore produces identical results and file contents.
- **Ordered, exactly-once results under concurrency**: result `i` always
  corresponds to job `i`.
- **Cancellation**: when `ctx` is cancelled, completed jobs are kept, jobs not
  handed to a worker are not opened, and an in-flight write is aborted with its
  previous target preserved. Re-running the batch afterwards converges to the
  same state as an uninterrupted run.
- **Style-safe `Optimize`**: `Subtitles.Optimize` keeps styles and regions
  referenced by items or inline fragments, including inherited parent styles
  and region-referenced styles; only unreferenced definitions are removed.
  Documents are never shared between batch jobs, so one file's failure cannot
  affect another file's style graph.

Parse/render steps are injectable through `WithOpen` and `WithRenderer`, which
makes mid-write failure and cancellation scenarios easy to exercise in tests
(see `batch_test.go`).

The CLI exposes the same entry point:

        astisub convert-batch -i first.srt -i second.srt -os first.ttml -os second.vtt -c 4

# Using the CLI

If **astisub** has been installed properly you can:

- convert any type of subtitle to any other type of subtitle:

        astisub convert -i example.srt -o example.ttml

- batch convert several pairs at once (ordered per-file results, atomic
  replacement, cancellation-safe):

        astisub convert-batch -i first.srt -i second.srt -os first.ttml -os second.vtt -c 4 -batch-optimize

- apply linear correction to any type of subtitle:

        astisub apply-linear-correction -i example.srt -a1 1s -d1 2s -a2 5s -d2 7s -o example.out.srt

- fragment any type of subtitle:

        astisub fragment -i example.srt -f 2s -o example.out.srt

- merge any type of subtitle into any other type of subtitle:

        astisub merge -i example.srt -i example.ttml -o example.out.srt

- optimize any type of subtitle:

        astisub optimize -i example.srt -o example.out.srt

- unfragment any type of subtitle:

        astisub unfragment -i example.srt -o example.out.srt

- sync any type of subtitle:

        astisub sync -i example.srt -s "-2s" -o example.out.srt

# Features and roadmap

- [x] parsing
- [x] writing
- [x] syncing
- [x] fragmenting/unfragmenting
- [x] merging
- [x] ordering
- [x] optimizing
- [x] linear correction
- [x] .srt
- [x] .ttml
- [x] .vtt
- [x] .stl
- [x] .ssa/.ass
- [x] .teletext
- [ ] .smi
