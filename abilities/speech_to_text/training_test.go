package speech_to_text

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/asticode/go-astibob"
	"github.com/asticode/go-astikit"
	"github.com/julienschmidt/httprouter"
)

// ---------------------------------------------------------------------------------------------------------------------
// Fake parser
// ---------------------------------------------------------------------------------------------------------------------

// fakeTrainCall gives the test full control over a training session started by the runnable
type fakeTrainCall struct {
	ctx      context.Context
	progress func(Progress)
	release  chan struct{}
	speeches []SpeechFile
}

// fakeParser is a local parser implementing Parse and Train. Train blocks until the test releases it so that the test
// can drive progress callbacks at will, including after the session has been cancelled.
type fakeParser struct {
	calls chan fakeTrainCall
}

func newFakeParser() *fakeParser {
	return &fakeParser{calls: make(chan fakeTrainCall, 8)}
}

func (p *fakeParser) Parse(samples []int, bitDepth, numChannels, sampleRate int) (string, error) {
	return "fake", nil
}

func (p *fakeParser) Train(ctx context.Context, speeches []SpeechFile, progressFunc func(Progress)) {
	c := fakeTrainCall{
		ctx:      ctx,
		progress: progressFunc,
		release:  make(chan struct{}),
		speeches: speeches,
	}
	p.calls <- c
	<-c.release
}

// waitCall waits for the next training session to be handed to the parser
func (p *fakeParser) waitCall(t *testing.T) fakeTrainCall {
	t.Helper()
	select {
	case c := <-p.calls:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for parser train call")
	}
	return fakeTrainCall{}
}

// ---------------------------------------------------------------------------------------------------------------------
// Progress recorder
// ---------------------------------------------------------------------------------------------------------------------

// progressRecorder records every progress message dispatched by the runnable, mimicking what the UI receives
type progressRecorder struct {
	mu   sync.Mutex
	msgs []ProgressJSON
	raw  []*astibob.Message
}

func (r *progressRecorder) dispatchFunc() astibob.DispatchFunc {
	return func(m *astibob.Message) {
		if m.Name != progressMessage {
			return
		}
		var pj ProgressJSON
		if err := json.Unmarshal(m.Payload, &pj); err != nil {
			panic(err)
		}
		r.mu.Lock()
		r.msgs = append(r.msgs, pj)
		r.raw = append(r.raw, m)
		r.mu.Unlock()
	}
}

func (r *progressRecorder) all() []ProgressJSON {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := make([]ProgressJSON, len(r.msgs))
	copy(o, r.msgs)
	return o
}

func (r *progressRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *progressRecorder) last() (ProgressJSON, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.msgs) == 0 {
		return ProgressJSON{}, false
	}
	return r.msgs[len(r.msgs)-1], true
}

// ---------------------------------------------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------------------------------------------

const (
	testPreparingStep = "Preparing"
	testTrainingStep  = "Training"
)

func testSteps() []string { return []string{testPreparingStep, testTrainingStep} }

// pct returns the progress percentage carried by a progress JSON
func pct(p ProgressJSON) float64 {
	return p.Progress.Progress
}

func prog(step string, pct float64) Progress {
	return Progress{
		CurrentStep: step,
		Progress:    pct,
		Steps:       testSteps(),
	}
}

type harness struct {
	base     string
	client   *http.Client
	parser   *fakeParser
	recorder *progressRecorder
	runnable *Runnable
	server   *httptest.Server
	worker   *astikit.Worker
}

// newHarness builds a runnable wired to a fake parser, a temporary speeches directory and a real HTTP server exposing
// the operatable routes.
func newHarness(t *testing.T) *harness {
	t.Helper()

	// Create logger
	l := log.New(ioutil.Discard, "", 0)

	// Create runnable with a temporary speeches directory
	p := newFakeParser()
	r := NewRunnable("speech_to_text", p, l, RunnableOptions{SpeechesDirPath: t.TempDir(), StoreNewSpeeches: true})
	if err := r.Init(); err != nil {
		t.Fatalf("initializing runnable failed: %v", err)
	}

	// Wire base runnable requirements
	w := astikit.NewWorker(astikit.WorkerOptions{Logger: l})
	r.SetRootCtx(w.Context())
	r.SetTaskFunc(w.NewTask)

	// Record dispatched messages
	rec := &progressRecorder{}
	r.SetDispatchFunc(rec.dispatchFunc())

	// Build a real router exposing the operatable routes needed by the acceptance scenarios
	router := httprouter.New()
	for _, p := range []string{"/train", "/train/cancel", "/references/train"} {
		methods, ok := r.Routes()[p]
		if !ok {
			t.Fatalf("missing route %s", p)
		}
		for m, h := range methods {
			router.Handle(m, p, h)
		}
	}

	// Serve
	s := httptest.NewServer(router)

	// Make sure everything is released properly
	t.Cleanup(func() {
		s.Close()
		r.Close()
		w.Stop()
		w.Wait()
	})

	return &harness{
		base:     s.URL,
		client:   s.Client(),
		parser:   p,
		recorder: rec,
		runnable: r,
		server:   s,
		worker:   w,
	}
}

func (h *harness) get(t *testing.T, path string) *http.Response {
	t.Helper()
	resp, err := h.client.Get(h.base + path)
	if err != nil {
		t.Fatalf("GET %s failed: %v", path, err)
	}
	return resp
}

func (h *harness) train(t *testing.T) *http.Response {
	t.Helper()
	return h.get(t, "/train")
}

func (h *harness) cancel(t *testing.T) *http.Response {
	t.Helper()
	return h.get(t, "/train/cancel")
}

// progress queries the current progress through the real /references/train route
func (h *harness) progress(t *testing.T) *ProgressJSON {
	t.Helper()
	resp := h.get(t, "/references/train")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /references/train returned %d", resp.StatusCode)
	}
	var rf TrainReferences
	if err := json.NewDecoder(resp.Body).Decode(&rf); err != nil {
		t.Fatalf("decoding references failed: %v", err)
	}
	return rf.Progress
}

func (h *harness) mustStartTraining(t *testing.T) fakeTrainCall {
	t.Helper()
	resp := h.train(t)
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /train returned %d, want 200", resp.StatusCode)
	}
	return h.parser.waitCall(t)
}

// ---------------------------------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------------------------------

// TestTrainTwiceRejectedAndSessionUnchanged ensures a second train request while training is in progress is rejected
// and leaves the current session untouched.
func TestTrainTwiceRejectedAndSessionUnchanged(t *testing.T) {
	h := newHarness(t)

	call := h.mustStartTraining(t)
	defer close(call.release)

	// Drive the first session
	call.progress(prog(testPreparingStep, 42))

	before := h.progress(t)
	if before == nil {
		t.Fatal("expected a progress while training")
	}
	if before.Session != 1 {
		t.Fatalf("session = %d, want 1", before.Session)
	}
	if pct(*before) != 42 {
		t.Fatalf("progress = %v, want 42", pct(*before))
	}

	// Train again
	resp := h.train(t)
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("second GET /train returned %d, want 400", resp.StatusCode)
	}

	// Session must be unchanged
	after := h.progress(t)
	if after == nil {
		t.Fatal("expected the progress to remain after the rejected train request")
	}
	if after.Session != 1 {
		t.Fatalf("session after rejected train = %d, want 1", after.Session)
	}
	if pct(*after) != 42 {
		t.Fatalf("progress after rejected train = %v, want 42", pct(*after))
	}
}

// TestCancelWithoutTrainingIsNoop ensures cancelling when no training is in progress succeeds without side effect.
func TestCancelWithoutTrainingIsNoop(t *testing.T) {
	h := newHarness(t)

	resp := h.cancel(t)
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /train/cancel returned %d, want 200", resp.StatusCode)
	}

	if pg := h.progress(t); pg != nil {
		t.Fatalf("progress = %+v, want nil", pg)
	}
	if n := h.recorder.len(); n != 0 {
		t.Fatalf("%d progress messages dispatched, want 0", n)
	}
}

// TestCancelConvergesAndLateCallbacksIgnored ensures cancelling ends the session immediately, that the stale progress
// is cleared, and that late progress callbacks from the cancelled session can't resurrect it or leak into the next one.
func TestCancelConvergesAndLateCallbacksIgnored(t *testing.T) {
	h := newHarness(t)

	call := h.mustStartTraining(t)
	defer close(call.release)

	// Drive the first session
	call.progress(prog(testPreparingStep, 60))

	// Cancel
	resp := h.cancel(t)
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /train/cancel returned %d, want 200", resp.StatusCode)
	}

	// Progress must be empty right after cancelling
	if pg := h.progress(t); pg != nil {
		t.Fatalf("progress after cancel = %+v, want nil", pg)
	}

	// The terminal message must have been delivered
	last, ok := h.recorder.last()
	if !ok {
		t.Fatal("expected a terminal progress message after cancel")
	}
	if !last.Done {
		t.Fatalf("last progress done = false, want true")
	}

	// A late callback from the cancelled parser must be ignored
	n := h.recorder.len()
	call.progress(prog(testTrainingStep, 100))

	if got := h.recorder.len(); got != n {
		t.Fatalf("%d messages dispatched after a late callback, want %d", got, n)
	}
	if pg := h.progress(t); pg != nil {
		t.Fatalf("late callback resurrected the session: %+v", pg)
	}

	// A new training session must start from scratch
	call2 := h.mustStartTraining(t)
	defer close(call2.release)

	if pg := h.progress(t); pg == nil {
		t.Fatal("expected a progress for the new session")
	} else {
		if pg.Session != 2 {
			t.Fatalf("new session = %d, want 2", pg.Session)
		}
		if len(pg.Steps) != 0 {
			t.Fatalf("new session inherited steps %v", pg.Steps)
		}
		if pct(*pg) != 0 {
			t.Fatalf("new session inherited progress %v, want 0", pct(*pg))
		}
		if pg.Done {
			t.Fatal("new session must not be done")
		}
	}

	// The new session must drive its own messages
	call2.progress(prog(testPreparingStep, 10))
	if last, ok := h.recorder.last(); !ok || last.Session != 2 || pct(last) != 10 {
		t.Fatalf("last progress = %+v, want session 2 at 10%%", last)
	}
}

// TestNaturalCompletionAllowsRestart ensures a session reaching 100% ends cleanly and can be restarted right away.
func TestNaturalCompletionAllowsRestart(t *testing.T) {
	h := newHarness(t)

	call := h.mustStartTraining(t)
	defer close(call.release)

	// Intermediate step reaching 100% must not end the session
	call.progress(prog(testPreparingStep, 100))
	if pg := h.progress(t); pg == nil {
		t.Fatal("session ended on an intermediate step")
	} else if pg.Done {
		t.Fatal("progress done = true on an intermediate step")
	}

	// Final step reaching 100% ends the session
	call.progress(prog(testTrainingStep, 100))

	if pg := h.progress(t); pg != nil {
		t.Fatalf("progress after natural completion = %+v, want nil", pg)
	}
	last, ok := h.recorder.last()
	if !ok || !last.Done {
		t.Fatalf("last progress = %+v, want done", last)
	}
	if pct(last) != 100 {
		t.Fatalf("last progress = %v, want 100", pct(last))
	}

	// Restart immediately
	call2 := h.mustStartTraining(t)
	defer close(call2.release)

	pg := h.progress(t)
	if pg == nil || pg.Session != 2 || pct(*pg) != 0 {
		t.Fatalf("restarted session = %+v, want session 2 at 0%%", pg)
	}
}

// TestProgressMonotonicWithinSession ensures progress never goes backwards within a session.
func TestProgressMonotonicWithinSession(t *testing.T) {
	h := newHarness(t)

	call := h.mustStartTraining(t)
	defer close(call.release)

	call.progress(prog(testPreparingStep, 50))
	if pg := h.progress(t); pg == nil || pct(*pg) != 50 {
		t.Fatalf("progress = %+v, want 50", pg)
	}

	// Going backwards within the same step is ignored
	call.progress(prog(testPreparingStep, 30))
	if pg := h.progress(t); pg == nil || pct(*pg) != 50 {
		t.Fatalf("progress after backwards callback = %+v, want 50", pg)
	}

	// Going back to a previous step is ignored
	call.progress(prog(testPreparingStep, 90))
	call.progress(prog(testPreparingStep, 10))
	if pg := h.progress(t); pg == nil || pct(*pg) != 90 {
		t.Fatalf("progress after backwards callback = %+v, want 90", pg)
	}

	// Going forward is accepted
	call.progress(prog(testTrainingStep, 5))
	if pg := h.progress(t); pg == nil || pg.CurrentStep != testTrainingStep || pct(*pg) != 5 {
		t.Fatalf("progress = %+v, want Training at 5%%", pg)
	}
}

// TestTerminalMessageNotRateLimited ensures the terminal message of a session is delivered even when the rate limiter
// has been saturated by regular progress messages.
func TestTerminalMessageNotRateLimited(t *testing.T) {
	h := newHarness(t)

	call := h.mustStartTraining(t)
	defer close(call.release)

	// Saturate the limiter (20 messages per second)
	for i := 0; i < 40; i++ {
		call.progress(prog(testPreparingStep, float64(i+1)))
	}
	before := h.recorder.len()

	// Terminal message
	call.progress(prog(testTrainingStep, 100))

	msgs := h.recorder.all()
	last := msgs[len(msgs)-1]
	if !last.Done || pct(last) != 100 {
		t.Fatalf("last progress = %+v, want done at 100", last)
	}
	if len(msgs) <= before {
		t.Fatal("terminal message was rate limited")
	}

	// Sanity check: regular messages were actually rate limited
	if before >= 40 {
		t.Fatalf("%d regular progress messages delivered, the limiter did not drop anything", before)
	}
}

// TestParserReturningWithoutTerminalProgressConverges ensures a parser that returns without emitting a terminal
// progress doesn't leave the session hanging: the session ends and a new one can start.
func TestParserReturningWithoutTerminalProgressConverges(t *testing.T) {
	h := newHarness(t)

	call := h.mustStartTraining(t)
	call.progress(prog(testPreparingStep, 20))

	// Parser returns without a terminal progress
	close(call.release)

	// Wait for the session to converge
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.progress(t) == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pg := h.progress(t); pg != nil {
		t.Fatalf("session did not converge: %+v", pg)
	}

	// The terminal message must have been emitted
	msgs := h.recorder.all()
	last := msgs[len(msgs)-1]
	if !last.Done {
		t.Fatalf("last progress = %+v, want done", last)
	}

	// A new session can start
	call2 := h.mustStartTraining(t)
	defer close(call2.release)
	if pg := h.progress(t); pg == nil || pg.Session != 2 {
		t.Fatalf("new session = %+v, want session 2", pg)
	}
}

// TestConcurrentCancelAndCallbacks runs cancel and parser callbacks concurrently to make sure the session state stays
// consistent. Run with -race to catch data races.
func TestConcurrentCancelAndCallbacks(t *testing.T) {
	h := newHarness(t)

	for round := 1; round <= 20; round++ {
		call := h.mustStartTraining(t)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				call.progress(prog(testPreparingStep, float64(i*5)))
			}
		}()
		go func() {
			defer wg.Done()
			resp := h.cancel(t)
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
		wg.Wait()
		close(call.release)

		// Whatever the interleaving, the session must be over and restartable
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if h.progress(t) == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if pg := h.progress(t); pg != nil {
			t.Fatalf("round %d: session did not converge: %+v", round, pg)
		}
	}
}
