package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/asticode/go-astisub"
)

// runConvertBatch implements the "astisub convert-batch" sub-command.
//
// Inputs and outputs are paired by index:
//
//	astisub convert-batch -i first.srt -i second.srt \
//	    -os first.ttml -os second.vtt [-c 4] [-batch-optimize]
//
// It prints one tab-separated, index-ordered result line per input and exits
// non-zero if any job failed or was cancelled. A Ctrl+C cancels the batch:
// completed files stay on disk, unstarted files are not processed and
// in-flight writes leave their previous targets untouched.
func runConvertBatch() {
	inputs := *inputPath.Slice
	outputs := *outputPaths.Slice
	if len(outputs) == 0 {
		log.Fatal("convert-batch: provide at least one output with -os, paired by index with -i")
	}
	if len(inputs) != len(outputs) {
		log.Fatalf("convert-batch: received %d -i input(s) but %d -os output(s)", len(inputs), len(outputs))
	}

	jobs := make([]astisub.BatchJob, len(inputs))
	for i := range inputs {
		jobs[i] = astisub.BatchJob{Source: inputs[i], Target: outputs[i]}
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		stop()
	}()

	results := astisub.ConvertBatch(
		ctx,
		jobs,
		astisub.WithConcurrency(*batchConcurrency),
		astisub.WithOptimize(*batchOptimize),
	)

	failed := 0
	for _, r := range results {
		status := "OK"
		switch {
		case r.OK:
			status = "OK"
		case r.Cancelled:
			status = "CANCELLED"
			failed++
		default:
			status = "ERROR"
			failed++
		}
		reason := strings.ReplaceAll(r.Reason, "\n", " ")
		fmt.Printf("%d\t%s\t%s\t%s\t%s\n", r.Index, status, r.Source, r.Target, reason)
	}

	if failed > 0 {
		log.Fatalf("convert-batch: %d/%d job(s) failed or were cancelled", failed, len(results))
	}
	log.Printf("convert-batch: %d/%d job(s) succeeded", len(results), len(results))
}
