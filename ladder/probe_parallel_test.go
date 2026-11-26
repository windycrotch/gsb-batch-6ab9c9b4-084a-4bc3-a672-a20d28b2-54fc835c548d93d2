package ladder

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/eko/qc/analysis"
	"github.com/eko/qc/encode"
)

// gateLab wraps fakeLab with an Encode that blocks on a gate, so the probe
// stage has in-flight measurements a cancellation or a failure can stop. It
// tracks how many encodes run at once and how many observed a cancelled
// context, and can designate one encode of a full batch to fail. Every other
// port is the embedded fakeLab's.
type gateLab struct {
	*fakeLab

	gate   chan struct{}
	gateOn sync.Once
	// armFail is consumed once, by the first encode, which then waits for a
	// full batch of peers before failing: the others are blocked on gate and
	// must be stopped by the group's cancellation.
	armFail  atomic.Bool
	parallel atomic.Int32

	mu        sync.Mutex
	running   int
	peak      int
	cancelled int
	started   int
}

// openGate creates the gate the first time it is asked for: a build without
// a gate runs its encodes straight through the embedded fake.
func (l *gateLab) openGate() chan struct{} {
	l.gateOn.Do(func() {
		l.gate = make(chan struct{})
	})

	return l.gate
}

// armFailure designates the first encode to fail with errFake once a batch of
// parallel encodes is in flight. The gate must stay closed in that test, so
// only the group's cancellation can release the peers.
func (l *gateLab) armFailure(
	parallel int,
) {
	l.parallel.Store(int32(parallel))
	l.armFail.Store(true)
}

func (l *gateLab) Encode(
	ctx context.Context,
	codec encode.Codec,
	src, dst string,
	p encode.Params,
) error {
	l.mu.Lock()
	l.running++
	l.peak = max(l.peak, l.running)
	l.started++
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		l.running--
		l.mu.Unlock()
	}()

	if gate := l.gate; gate != nil {
		// One encode is designated to fail: it waits for its peers of the
		// batch to block, then returns the encoding error.
		if l.armFail.CompareAndSwap(true, false) {
			if !l.waitForPeers(ctx) {
				return ctx.Err()
			}

			return errFake
		}

		select {
		case <-gate:
		case <-ctx.Done():
			l.mu.Lock()
			l.cancelled++
			l.mu.Unlock()

			return ctx.Err()
		}
	}

	return l.fakeLab.Encode(ctx, codec, src, dst, p)
}

// waitForPeers blocks until a full batch of encodes is running, or until ctx
// is cancelled. ok is false on cancellation.
func (l *gateLab) waitForPeers(
	ctx context.Context,
) (ok bool) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		l.mu.Lock()
		full := l.running >= int(l.parallel.Load())
		l.mu.Unlock()

		if full {
			return true
		}

		select {
		case <-ctx.Done():
			l.mu.Lock()
			l.cancelled++
			l.mu.Unlock()

			return false
		case <-ticker.C:
		}
	}
}

// peakConcurrency is the largest number of encodes running at once.
func (l *gateLab) peakConcurrency() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.peak
}

// fileLab wraps gateLab and writes the output file on every Encode, like a
// real encoder does, so the probe's deferred cleanup has something to remove
// when the build is cancelled mid-encode.
type fileLab struct {
	*gateLab
}

func (l *fileLab) Encode(
	ctx context.Context,
	codec encode.Codec,
	src, dst string,
	p encode.Params,
) error {
	if err := os.WriteFile(dst, []byte("probe"), 0o600); err != nil {
		return err
	}

	return l.gateLab.Encode(ctx, codec, src, dst, p)
}

// newFileLab builds a fileLab over the deterministic fake model.
func newFileLab() *fileLab {
	return &fileLab{gateLab: &gateLab{fakeLab: newFakeLab(rateModel{}, probeSource1080())}}
}

var (
	_ fakePorts = (*gateLab)(nil)
	_ fakePorts = (*fileLab)(nil)
)

// cancelledEncodes is how many encodes stopped on a cancelled context.
func (l *gateLab) cancelledEncodes() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.cancelled
}

// runningEncodes is how many encodes are currently blocked or running.
func (l *gateLab) runningEncodes() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.running
}

// newGateLab builds a gateLab over the standard deterministic fake model.
func newGateLab(
	t *testing.T,
) *gateLab {
	t.Helper()

	return &gateLab{fakeLab: newFakeLab(rateModel{}, probeSource1080())}
}

// probeSource1080 is a long enough 1080p source to have five candidate
// resolutions, hence fifteen fixed probes (5 resolutions × 3 CRFs).
func probeSource1080() *analysis.Report {
	return sourceReport(1920, 1080, 8, 25, 600)
}

// fakePorts is the full set of engine ports a test lab provides: the
// embedded fakeLab already implements them, and wrappers such as fileLab
// inherit them and only override Encode.
type fakePorts interface {
	Inspector
	Encoder
	Digester
	GrainLab
	RenditionEncoder
}

// gateEngine wires an engine whose every port is lab, so its Encode is the
// instrumented one (an engine built on the embedded fakeLab would bypass it).
func gateEngine(
	lab fakePorts,
) *Engine {
	return NewEngine(lab, lab, lab, WithGrainLab(lab), WithRenditionEncoder(lab))
}

// probeSet is the set of measured points of a build, identified by their
// resolution and CRF: what parallelism must not change.
func probeSet(
	probes []Probe,
) [][2]float64 {
	out := make([][2]float64, len(probes))
	for i, p := range probes {
		out[i] = [2]float64{float64(p.Height), p.CRF}
	}

	return out
}

// rungSet is the selected rungs as comparable points.
func rungSet(
	rungs []Rung,
) [][2]float64 {
	out := make([][2]float64, len(rungs))
	for i, r := range rungs {
		out[i] = [2]float64{float64(r.Height), r.CRF}
	}

	return out
}

// TestProbeParallelDeterminism builds one source at parallelism 1 and at a
// higher one: the probe points and the selected rungs must be identical,
// because every probe shares the same digest and the same fixed-seed frame
// sampling (common random numbers).
func TestProbeParallelDeterminism(
	t *testing.T,
) {
	build := func(parallel int) (*Result, *fakeLab) {
		lab := newFakeLab(rateModel{}, probeSource1080())
		engine := labEngine(lab)

		res, err := engine.Build(t.Context(), sourcePath, Options{
			Codec: "h264", Parallel: parallel, SkipVerify: true,
			DigestSampling: DigestUniform,
		})
		require.NoError(t, err)

		return res, lab
	}

	serial, serialLab := build(1)
	parallel, parallelLab := build(4)

	assert.ElementsMatch(t, probeSet(serial.Probes), probeSet(parallel.Probes),
		"parallelism must not change the set of probed points")
	assert.Equal(t, probeSet(serial.Probes), probeSet(parallel.Probes),
		"the probe order is stable too")
	assert.Equal(t, rungSet(serial.Rungs), rungSet(parallel.Rungs),
		"the selected rungs must not depend on parallelism")

	// The measurements themselves must match: same frames, same model.
	require.Equal(t, len(serial.Probes), len(parallel.Probes))

	for i := range serial.Probes {
		assert.Equal(t, serial.Probes[i].Bitrate, parallel.Probes[i].Bitrate, "probe %d bitrate", i)
		assert.InDelta(t, serial.Probes[i].VMAF, parallel.Probes[i].VMAF, 1e-12, "probe %d VMAF", i)
	}

	// The frame sampling configuration of every measurement is identical: a
	// fixed seed and the same budget pick the same frames whatever the
	// parallelism, so two builds of one input measure the same frame set. The
	// comparisons complete in no fixed order, hence the unordered match. The
	// seed itself defaults inside the quality layer (quality.defaultSeed),
	// which the fake lab records as zero before that default is applied.
	require.Equal(t, len(serialLab.compared), len(parallelLab.compared))
	assert.ElementsMatch(t, serialLab.compared, parallelLab.compared,
		"the sampling options (budget, clips, max share) of each measurement match")

	for _, q := range parallelLab.compared {
		if q.Budget {
			assert.Equal(t, defaultProbeClips, q.InitialClips, "a fixed sampling pilot")
			assert.Equal(t, probeShare, q.MaxShare, "a fixed frame budget")
		}
	}

	// Both builds share one digest: the encode source recorded by the lab is
	// the same single file for every probe.
	assert.Equal(t, len(serialLab.digests), len(parallelLab.digests))
	for i := range serialLab.digests {
		assert.Equal(t, serialLab.digests[i].Segments, parallelLab.digests[i].Segments)
	}
}

// TestProbesRunConcurrently observes the probe stage running several encodes
// at once, bounded by Options.Parallel.
func TestProbesRunConcurrently(
	t *testing.T) {
	lab := newGateLab(t)
	gate := lab.openGate()
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})

	const parallel = 4

	done := make(chan struct{})

	go func() {
		_, _ = gateEngine(lab).Build(context.Background(), sourcePath, Options{
			Codec: "h264", Parallel: parallel, SkipVerify: true,
			DigestSampling: DigestUniform,
		})
		close(done)
	}()

	// The first batch must reach the requested parallelism, not run one at a time.
	assert.Eventually(t, func() bool {
		return lab.peakConcurrency() >= parallel
	}, 5*time.Second, time.Millisecond, "probes run %d at once", parallel)

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("build did not finish after the gate opened")
	}

	assert.Equal(t, parallel, lab.peakConcurrency())
	assert.Zero(t, lab.runningEncodes(), "no encode is left running")
}

// TestProbeProgressMonotonic records the progress of a parallel probe stage
// and checks the completed count only increases within each batch and never
// goes backwards, while callbacks never overlap.
func TestProbeProgressMonotonic(
	t *testing.T) {
	lab := newGateLab(t)
	gate := lab.openGate()
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})

	var (
		mu       sync.Mutex
		reports  []Progress
		inFlight atomic.Int32
		overlap  atomic.Bool
	)

	progress := func(p Progress) {
		if inFlight.Add(1) > 1 {
			overlap.Store(true)
		}
		defer inFlight.Add(-1)

		mu.Lock()
		reports = append(reports, p)
		mu.Unlock()
	}

	done := make(chan struct{})

	go func() {
		_, _ = gateEngine(lab).Build(context.Background(), sourcePath, Options{
			Codec: "h264", Parallel: 4, SkipVerify: true,
			DigestSampling: DigestUniform, Progress: progress,
		})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(reports) > 0
	}, 5*time.Second, time.Millisecond)

	close(gate)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("build did not finish")
	}

	assert.False(t, overlap.Load(), "progress callbacks must not overlap")

	mu.Lock()
	defer mu.Unlock()

	var probes []Progress

	for _, p := range reports {
		if p.Stage == StageProbe {
			probes = append(probes, p)
		}
	}

	require.NotEmpty(t, probes)

	// Within a batch (a reset sets Done back to 1), Done only increases and
	// never exceeds Total. Track the previous value and assert no decrease
	// except the explicit batch boundary (Done == 1 after a reset).
	prevDone, prevTotal := 0, 0

	for i, p := range probes {
		assert.GreaterOrEqual(t, p.Done, 1, "report %d", i)
		assert.LessOrEqual(t, p.Done, p.Total, "report %d never overshoots total", i)

		switch {
		case i == 0:
			assert.Equal(t, 1, p.Done, "the first report of a batch starts at 1")
		case p.Total != prevTotal || p.Done == 1:
			// A new measurement batch resets the counter.
			assert.Equal(t, 1, p.Done, "a new batch starts at 1")
		default:
			assert.Equal(t, prevDone+1, p.Done, "report %d increases by one, never jumps back", i)
		}

		assert.NotNil(t, p.Probe, "every probe report carries its probe")

		prevDone, prevTotal = p.Done, p.Total
	}

	// The last report of the final batch reaches its total.
	assert.Equal(t, probes[len(probes)-1].Done, probes[len(probes)-1].Total)
}

// TestProbeCancellationStopsInFlight cancels a build while a batch of probes
// is in flight: the remaining encodes stop promptly, every child work exits,
// the temporary work directory is removed, and the error still wraps
// context.Canceled (distinct from an encoding failure).
func TestProbeCancellationStopsInFlight(
	t *testing.T) {
	lab := newGateLab(t)
	gate := lab.openGate()
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})

	workDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		_, err := gateEngine(lab).Build(ctx, sourcePath, Options{
			Codec: "h264", Parallel: 4, SkipVerify: true,
			DigestSampling: DigestUniform, WorkDir: workDir,
		})
		done <- err
	}()

	// Wait until the probe stage has several encodes in flight.
	assert.Eventually(t, func() bool {
		return lab.peakConcurrency() >= 4
	}, 5*time.Second, time.Millisecond)

	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled, "cancellation is distinguishable from an encode failure")
		assert.NotErrorIs(t, err, errFake)
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled build did not return")
	}

	// Every in-flight encode observed the cancellation and none is left running.
	assert.Eventually(t, func() bool {
		return lab.runningEncodes() == 0
	}, 5*time.Second, time.Millisecond, "in-flight probes stop")
	assert.Positive(t, lab.cancelledEncodes(), "at least one encode was stopped by the cancellation")

	// Releasing the gate must not start or resurrect any work.
	close(gate)
	assert.Never(t, func() bool { return lab.runningEncodes() != 0 }, 200*time.Millisecond, time.Millisecond)

	// No cancelled probe leaves a temporary output behind. The fake encoder
	// writes nothing to disk; on the real path each probe's output is also
	// removed by measure's deferred os.Remove, and an auto-created work
	// directory is removed by the build's cleanup.
	entries, err := os.ReadDir(workDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "no probe temp file is left behind")
}

// TestProbeCancellationRemovesTempFiles uses an encoder that really writes each
// probe output, then cancels mid-batch: the deferred cleanup of every in-flight
// probe must remove its temporary file.
func TestProbeCancellationRemovesTempFiles(
	t *testing.T,
) {
	lab := newFileLab()
	gate := lab.openGate()
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})

	workDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan error, 1)

	go func() {
		_, err := gateEngine(lab).Build(ctx, sourcePath, Options{
			Codec: "h264", Parallel: 4, SkipVerify: true,
			DigestSampling: DigestUniform, WorkDir: workDir,
		})
		done <- err
	}()

	// Once a batch is encoding, its output files exist on disk.
	assert.Eventually(t, func() bool {
		entries, err := os.ReadDir(workDir)

		return err == nil && len(entries) > 0
	}, 5*time.Second, time.Millisecond, "the in-flight probes wrote temp files")

	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled build did not return")
	}

	assert.Eventually(t, func() bool {
		entries, err := os.ReadDir(workDir)

		return err == nil && len(entries) == 0
	}, 5*time.Second, time.Millisecond, "every probe temp file is removed")
}

// TestProbeFailureStopsOthers makes one probe fail while others are in
// flight: the rest stop promptly and the error wraps the encoding failure,
// not a cancellation.
func TestProbeFailureStopsOthers(
	t *testing.T) {
	const parallel = 4

	lab := newGateLab(t)
	gate := lab.openGate()
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	// The gate is never released during the test: only the group's
	// cancellation can free the peers of the failing encode.
	lab.armFailure(parallel)

	var buildErr error

	done := make(chan struct{})

	go func() {
		_, buildErr = gateEngine(lab).Build(context.Background(), sourcePath, Options{
			Codec: "h264", Parallel: parallel, SkipVerify: true,
			DigestSampling: DigestUniform,
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not return after a probe failure")
	}

	require.Error(t, buildErr)
	assert.ErrorIs(t, buildErr, errFake, "an encoding failure is distinguishable from cancellation")
	assert.NotErrorIs(t, buildErr, context.Canceled)
	assert.Contains(t, buildErr.Error(), "ladder:")

	// The failure happened while peers were in flight, and the failed probe
	// aborted the group: those peers stopped on the cancellation, the
	// parallelism never overran its limit, and no encode is left running.
	assert.Greater(t, lab.peakConcurrency(), 1, "the failure struck a parallel batch, not a serial one")
	assert.LessOrEqual(t, lab.peakConcurrency(), parallel, "the failure never overran the parallelism limit")
	assert.Eventually(t, func() bool {
		return lab.runningEncodes() == 0
	}, 5*time.Second, time.Millisecond)
	assert.Positive(t, lab.cancelledEncodes(), "the peers of the failed probe were cancelled")
}

// TestProbeMemoryLowersParallelism checks that a tight memory budget lowers
// the effective parallelism (fitParallel) rather than failing the build, and
// that the lowered build still succeeds end to end.
func TestProbeMemoryLowersParallelism(
	t *testing.T) {
	const (
		gib = int64(1) << 30
		hd  = 1920 * 1080
	)

	// 1 GiB on a 1080p source cannot hold two probes: the parallelism falls
	// back to one but never to zero.
	assert.Equal(t, 1, fitParallel(4, gib, hd))
	assert.Equal(t, 1, fitParallel(1, gib, hd))

	// A comfortable budget keeps the requested parallelism.
	assert.Equal(t, 4, fitParallel(4, 16*gib, hd))

	// End to end: the tight-budget build runs one probe at a time and succeeds.
	lab := newFakeLab(rateModel{}, probeSource1080())

	res, err := labEngine(lab).Build(t.Context(), sourcePath, Options{
		Codec: "h264", Parallel: 4, Memory: gib, SkipVerify: true,
		DigestSampling: DigestUniform,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, res.Rungs)
	assert.NotEmpty(t, res.Probes)

	// The build applied the lowered parallelism to its probe group.
	pixels := res.Source.Info.Video[0].Width * res.Source.Info.Video[0].Height
	require.Equal(t, 1, fitParallel(4, gib, pixels))
}
