// Copyright 2026 The Oto Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mux_test

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ebitengine/oto/v3/internal/mux"
)

// agentWaitFor polls cond until it is true or the timeout elapses.
func agentWaitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// agentWaitChan waits for ch to close or fails after the timeout.
func agentWaitChan(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// agentAssertBlocked fails if any of chs closes during quiet.
func agentAssertBlocked(t *testing.T, quiet time.Duration, what string, chs ...<-chan struct{}) {
	t.Helper()
	check := func() {
		for _, ch := range chs {
			select {
			case <-ch:
				t.Fatalf("%s completed before it was allowed to", what)
			default:
			}
		}
	}
	check()
	time.Sleep(quiet)
	check()
}

func agentGo(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	return done
}

// agentGateSource blocks every Read that reaches it until gate is closed.
type agentGateSource struct {
	beganOnce sync.Once
	began     chan struct{}
	gate      chan struct{}
	reads     atomic.Int64
}

func (s *agentGateSource) Read(buf []byte) (int, error) {
	s.reads.Add(1)
	s.beganOnce.Do(func() { close(s.began) })
	<-s.gate
	for i := range buf {
		buf[i] = 0x40
	}
	return len(buf), nil
}

func (s *agentGateSource) Seek(int64, int) (int64, error) { return 0, nil }

// TestAgentPauseAndStopReadingWaitsAndStopsReads covers A1/A2: PauseAndStopReading
// must wait for an in-flight Read, keep its result, and prevent further reads.
func TestAgentPauseAndStopReadingWaitsAndStopsReads(t *testing.T) {
	src := &agentGateSource{began: make(chan struct{}), gate: make(chan struct{})}
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()
	p := m.NewPlayer(src)
	defer func() { _ = p.Close() }()
	p.Play()

	agentWaitChan(t, "the first Read to start", src.began)

	stopDone := agentGo(p.PauseAndStopReading)
	agentAssertBlocked(t, 80*time.Millisecond, "PauseAndStopReading while a Read is in flight", stopDone)

	close(src.gate)
	agentWaitChan(t, "PauseAndStopReading to return after the Read", stopDone)

	if got := p.BufferedSize(); got == 0 {
		t.Fatal("the in-flight Read result was discarded by PauseAndStopReading")
	}
	if err := p.Err(); err != nil {
		t.Fatalf("Err: got %v; want nil", err)
	}

	// After PauseAndStopReading returns, time and mixing must not prompt another
	// source Read. While stopped the player is unregistered, so mixing does not
	// consume its kept buffer either.
	reads := src.reads.Load()
	time.Sleep(60 * time.Millisecond)
	m.ReadFloat32s(make([]float32, 4096))
	time.Sleep(20 * time.Millisecond)
	if got := src.reads.Load(); got != reads {
		t.Fatalf("source was read after PauseAndStopReading: got %d reads; want %d", got, reads)
	}

	// After Play, mixing consumes the full kept buffer and the source is read again.
	p.Play()
	deadline := time.Now().Add(2 * time.Second)
	for src.reads.Load() == reads && time.Now().Before(deadline) {
		m.ReadFloat32s(make([]float32, 4096))
		time.Sleep(time.Millisecond)
	}
	if got := src.reads.Load(); got == reads {
		t.Fatal("the source was not read again after Play following PauseAndStopReading")
	}
}

// TestAgentSeekDiscardsInFlightRead covers A3: Seek waits for the in-flight Read
// and its old-position result never reaches the buffer.
func TestAgentSeekDiscardsInFlightRead(t *testing.T) {
	src := &agentGateSource{began: make(chan struct{}), gate: make(chan struct{})}
	m := mux.New(48000, 2, mux.FormatSignedInt16LE)
	defer m.Stop()
	p := m.NewPlayer(src)
	defer func() { _ = p.Close() }()
	p.Play()

	agentWaitChan(t, "the first Read to start", src.began)
	// Pause normally so the Seek ends in the stop-reading state and the buffer
	// stays observably empty instead of being refilled immediately.
	p.Pause()

	seekDone := agentGo(func() {
		if _, err := p.Seek(0, io.SeekStart); err != nil {
			t.Error(err)
		}
	})
	agentAssertBlocked(t, 80*time.Millisecond, "Seek while a Read is in flight", seekDone)

	close(src.gate)
	agentWaitChan(t, "Seek to return after the Read", seekDone)

	// Give a misbehaving implementation time to write the stale result back.
	time.Sleep(80 * time.Millisecond)
	if got := p.BufferedSize(); got != 0 {
		t.Fatalf("BufferedSize after Seek: got %d; want 0 (stale Read result survived)", got)
	}
}

// agentBlockSeekSource fills reads immediately but blocks every Seek until
// seekRelease is closed.
type agentBlockSeekSource struct {
	value       byte
	seekEntered chan struct{}
	seekRelease chan struct{}
	reads       atomic.Int64
	seeks       atomic.Int64
}

func (s *agentBlockSeekSource) Read(buf []byte) (int, error) {
	s.reads.Add(1)
	for i := range buf {
		buf[i] = s.value
	}
	return len(buf), nil
}

func (s *agentBlockSeekSource) Seek(int64, int) (int64, error) {
	s.seeks.Add(1)
	if s.seekEntered != nil {
		select {
		case s.seekEntered <- struct{}{}:
		default:
		}
	}
	if s.seekRelease != nil {
		<-s.seekRelease
	}
	return 0, nil
}

func newAgentBlockedSeekPlayer(t *testing.T) (*mux.Mux, *mux.Player, *agentBlockSeekSource) {
	t.Helper()
	src := &agentBlockSeekSource{
		value:       128,
		seekEntered: make(chan struct{}, 8),
		seekRelease: make(chan struct{}),
	}
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	t.Cleanup(m.Stop)
	p := m.NewPlayer(src)
	t.Cleanup(func() { _ = p.Close() })
	p.Play()
	ticketsAfterPlay := p.AgentCtrlArrivals()

	seekDone := agentGo(func() {
		if _, err := p.Seek(0, io.SeekStart); err != nil {
			t.Error(err)
		}
	})
	select {
	case <-src.seekEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("source Seek was not entered")
	}
	agentWaitFor(t, "the first Seek ticket", time.Second, func() bool {
		return p.AgentCtrlArrivals() == ticketsAfterPlay+1
	})
	t.Cleanup(func() {
		// Ensure the blocked Seek is always released before the test tears down.
		select {
		case <-src.seekRelease:
		default:
			close(src.seekRelease)
		}
		agentWaitChan(t, "the first Seek to finish during cleanup", seekDone)
	})
	return m, p, src
}

// TestAgentControlPairsArrivalOrder is the strict FIFO regression: when Pause
// and Play arrive in a known order behind a blocked Seek, their effects must
// end in that same order, regardless of goroutine scheduling.
func TestAgentControlPairsArrivalOrder(t *testing.T) {
	for _, tt := range []struct {
		name     string
		first    func(*mux.Player)
		second   func(*mux.Player)
		wantPlay bool
	}{
		{"pause then play", (*mux.Player).Pause, (*mux.Player).Play, true},
		{"play then pause", (*mux.Player).Play, (*mux.Player).Pause, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				src := &agentBlockSeekSource{
					seekEntered: make(chan struct{}, 1),
					seekRelease: make(chan struct{}),
				}
				m := mux.New(48000, 1, mux.FormatUnsignedInt8)
				p := m.NewPlayer(src)
				p.Play()

				seekDone := agentGo(func() {
					if _, err := p.Seek(0, io.SeekStart); err != nil {
						t.Error(err)
					}
				})
				agentWaitChan(t, "blocked Seek entry", src.seekEntered)
				base := p.AgentCtrlArrivals() // Play and Seek have both taken tickets

				d1 := agentGo(func() { tt.first(p) })
				agentWaitFor(t, "the first control arrival ticket", time.Second, func() bool {
					return p.AgentCtrlArrivals() == base+1
				})
				d2 := agentGo(func() { tt.second(p) })
				agentWaitFor(t, "the second control arrival ticket", time.Second, func() bool {
					return p.AgentCtrlArrivals() == base+2
				})

				close(src.seekRelease)
				agentWaitChan(t, "Seek", seekDone)
				agentWaitChan(t, "first control", d1)
				agentWaitChan(t, "second control", d2)

				if got := p.IsPlaying(); got != tt.wantPlay {
					t.Fatalf("iteration %d: IsPlaying=%v; want %v (controls were reordered)", i, got, tt.wantPlay)
				}
				_ = p.Close()
				m.Stop()
			}
		})
	}
}

// TestAgentMultipleControlsArrivalOrder queues a longer scripted sequence behind
// a blocked Seek and checks the final state matches applying it in arrival order.
func TestAgentMultipleControlsArrivalOrder(t *testing.T) {
	type op struct {
		name string
		run  func(*mux.Player)
	}
	sequences := []struct {
		name         string
		ops          []op
		wantPlaying  bool
		wantRegister bool
		wantBuffer0  bool
	}{
		{
			name: "pause play pause",
			ops: []op{
				{"pause", (*mux.Player).Pause},
				{"play", (*mux.Player).Play},
				{"pause", (*mux.Player).Pause},
			},
			wantPlaying: false, wantRegister: true,
		},
		{
			name: "play pause play",
			ops: []op{
				{"play", (*mux.Player).Play},
				{"pause", (*mux.Player).Pause},
				{"play", (*mux.Player).Play},
			},
			wantPlaying: true, wantRegister: true,
		},
		{
			name: "stop play",
			ops: []op{
				{"stop", (*mux.Player).PauseAndStopReading},
				{"play", (*mux.Player).Play},
			},
			wantPlaying: true, wantRegister: true,
		},
		{
			name: "pause stop play",
			ops: []op{
				{"pause", (*mux.Player).Pause},
				{"stop", (*mux.Player).PauseAndStopReading},
				{"play", (*mux.Player).Play},
			},
			wantPlaying: true, wantRegister: true,
		},
		{
			name: "play stop",
			ops: []op{
				{"play", (*mux.Player).Play},
				{"stop", (*mux.Player).PauseAndStopReading},
			},
			wantPlaying: false, wantRegister: false,
		},
		{
			name: "reset play",
			ops: []op{
				{"reset", (*mux.Player).Reset},
				{"play", (*mux.Player).Play},
			},
			wantPlaying: true, wantRegister: true, wantBuffer0: false,
		},
		{
			name: "pause reset",
			ops: []op{
				{"pause", (*mux.Player).Pause},
				{"reset", (*mux.Player).Reset},
			},
			wantPlaying: false, wantRegister: false, wantBuffer0: true,
		},
	}

	for _, tc := range sequences {
		t.Run(tc.name, func(t *testing.T) {
			_, p, src := newAgentBlockedSeekPlayer(t)
			base := p.AgentCtrlArrivals() // 1 (the blocked Seek)

			dones := make([]<-chan struct{}, len(tc.ops))
			for i, o := range tc.ops {
				dones[i] = agentGo(func() { o.run(p) })
				want := base + uint64(i+1)
				agentWaitFor(t, fmt.Sprintf("arrival of %s", o.name), time.Second, func() bool {
					return p.AgentCtrlArrivals() >= want
				})
			}

			// None of the queued controls may take effect before Seek returns.
			agentAssertBlocked(t, 60*time.Millisecond, "queued controls", dones...)

			close(src.seekRelease)
			for i, d := range dones {
				agentWaitChan(t, tc.ops[i].name, d)
			}

			if got := p.IsPlaying(); got != tc.wantPlaying {
				t.Errorf("IsPlaying after %q: got %v; want %v", tc.name, got, tc.wantPlaying)
			}
			if got := p.IsRegistered(); got != tc.wantRegister {
				t.Errorf("IsRegistered after %q: got %v; want %v", tc.name, got, tc.wantRegister)
			}
			if tc.wantBuffer0 {
				if got := p.BufferedSize(); got != 0 {
					t.Errorf("BufferedSize after %q: got %d; want 0", tc.name, got)
				}
			}
		})
	}
}

// TestAgentBlockedSeekCloseDrains checks Close queued behind a blocked Seek
// returns once the Seek is released and leaves a closed, unregistered player.
func TestAgentBlockedSeekCloseDrains(t *testing.T) {
	_, p, src := newAgentBlockedSeekPlayer(t)

	closeDone := agentGo(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	agentWaitFor(t, "Close arrival", time.Second, func() bool {
		return p.AgentCtrlArrivals() == 3
	})
	agentAssertBlocked(t, 60*time.Millisecond, "Close behind blocked Seek", closeDone)

	close(src.seekRelease)
	agentWaitChan(t, "Close", closeDone)

	if p.IsPlaying() || p.IsRegistered() {
		t.Fatalf("after queued Close: playing=%v registered=%v; want both false", p.IsPlaying(), p.IsRegistered())
	}
	// Later control calls must return rather than block forever.
	for _, fn := range []func(){p.Play, p.Pause, p.PauseAndStopReading, p.Reset} {
		done := agentGo(fn)
		agentWaitChan(t, "control after Close", done)
	}
}

// agentErrSeekSource's Seek always returns seekErr.
type agentErrSeekSource struct {
	seekErr error
}

func (s *agentErrSeekSource) Read(buf []byte) (int, error) {
	for i := range buf {
		buf[i] = 128
	}
	return len(buf), nil
}

func (s *agentErrSeekSource) Seek(int64, int) (int64, error) { return 0, s.seekErr }

// TestAgentSeekErrorKeepsStateVolumeAndLiveness covers A5 for a returned error.
func TestAgentSeekErrorKeepsStateVolumeAndLiveness(t *testing.T) {
	seekErr := errors.New("agent: seek failed")
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()
	p := m.NewPlayer(&agentErrSeekSource{seekErr: seekErr})
	defer func() { _ = p.Close() }()
	p.SetBufferSize(256)
	p.SetVolume(0.37)
	p.Play()
	agentWaitFor(t, "buffer fill", time.Second, func() bool { return p.BufferedSize() > 0 })

	if _, err := p.Seek(11, io.SeekStart); !errors.Is(err, seekErr) {
		t.Fatalf("Seek error: got %v; want %v", err, seekErr)
	}
	if !p.IsPlaying() {
		t.Error("a failed Seek changed the playing state")
	}
	if got := p.Volume(); got != 0.37 {
		t.Errorf("Volume after failed Seek: got %v; want 0.37", got)
	}

	stopDone := agentGo(p.PauseAndStopReading)
	agentWaitChan(t, "PauseAndStopReading after failed Seek", stopDone)

	// The player stays usable: Play must make the buffer fill again.
	buffered := p.BufferedSize()
	p.Play()
	agentWaitFor(t, "buffer refill after failed Seek", 2*time.Second, func() bool {
		return p.BufferedSize() > buffered || p.BufferedSize() > 0
	})
	if err := p.Err(); err != nil {
		t.Errorf("Err: got %v; want nil", err)
	}
}

// agentPanicSeekSource panics from Seek.
type agentPanicSeekSource struct{}

func (*agentPanicSeekSource) Read(buf []byte) (int, error) {
	for i := range buf {
		buf[i] = 128
	}
	return len(buf), nil
}

func (*agentPanicSeekSource) Seek(int64, int) (int64, error) {
	panic("agent: seek panic")
}

// TestAgentSeekPanicReleasesTurn covers A5 for a panicking source Seek.
func TestAgentSeekPanicReleasesTurn(t *testing.T) {
	m := mux.New(48000, 2, mux.FormatSignedInt16LE)
	defer m.Stop()
	p := m.NewPlayer(&agentPanicSeekSource{})
	defer func() { _ = p.Close() }()
	p.SetVolume(0.5)
	p.Play()

	func() {
		defer func() {
			if recover() == nil {
				t.Error("Seek did not propagate the source panic")
			}
		}()
		_, _ = p.Seek(0, io.SeekStart)
	}()

	if !p.IsPlaying() {
		t.Error("the Seek panic changed the playing state")
	}
	if got := p.Volume(); got != 0.5 {
		t.Errorf("Volume after Seek panic: got %v; want 0.5", got)
	}
	done := agentGo(p.PauseAndStopReading)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PauseAndStopReading blocked forever after a Seek panic")
	}
}

// agentRampSource produces byte(pos+i) at an absolute, Seekable position.
type agentRampSource struct {
	mu  sync.Mutex
	pos int64
}

func (r *agentRampSource) Read(buf []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range buf {
		buf[i] = byte(r.pos + int64(i))
	}
	r.pos += int64(len(buf))
	return len(buf), nil
}

func (r *agentRampSource) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch whence {
	case io.SeekStart:
		r.pos = offset
	case io.SeekCurrent:
		r.pos += offset
	case io.SeekEnd:
		return 0, errors.New("agentRampSource: SeekEnd unsupported")
	}
	return r.pos, nil
}

// agentReadBufferedBytes mixes exactly n buffered samples and converts the
// unsigned 8-bit mono samples back to their source byte values.
func agentReadBufferedBytes(t *testing.T, m *mux.Mux, p *mux.Player, n int) []byte {
	t.Helper()
	if n == 0 {
		return nil
	}
	if got := p.BufferedSize(); got < n {
		t.Fatalf("reading %d bytes with only %d buffered", n, got)
	}
	mix := make([]float32, n)
	m.ReadFloat32s(mix)
	out := make([]byte, n)
	for i, v := range mix {
		out[i] = byte(int(v*(1<<7)) + (1 << 7))
	}
	return out
}

func agentCollectBytes(t *testing.T, m *mux.Mux, p *mux.Player, want int) []byte {
	t.Helper()
	out := make([]byte, 0, want)
	deadline := time.Now().Add(5 * time.Second)
	for len(out) < want {
		if time.Now().After(deadline) {
			t.Fatalf("collected %d/%d bytes before timeout", len(out), want)
		}
		// Consume at most the bytes still wanted: reading a larger chunk would
		// consume bytes a later collection needs, silently skipping them.
		n := min(p.BufferedSize(), want-len(out), 48)
		if n == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		out = append(out, agentReadBufferedBytes(t, m, p, n)...)
	}
	return out
}

// TestAgentSeekThenPlayBytesContinuous covers A6 for Seek specifically: after
// seeking to a known position and resuming, every mixed byte must come from the
// new position and form a continuous sequence, with no old-position replay.
func TestAgentSeekThenPlayBytesContinuous(t *testing.T) {
	const bufferSize = 512
	const seekPos = 300

	src := &agentRampSource{}
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()
	p := m.NewPlayer(src)
	defer func() { _ = p.Close() }()
	p.SetBufferSize(bufferSize)
	p.SetVolume(1)
	p.Play()

	agentWaitFor(t, "the buffer to fill before Seek", time.Second, func() bool {
		return p.BufferedSize() >= bufferSize
	})

	pos, err := p.Seek(seekPos, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if pos != seekPos {
		t.Fatalf("Seek position: got %d; want %d", pos, seekPos)
	}

	got := agentCollectBytes(t, m, p, 1024)
	for i, b := range got {
		if want := byte(seekPos + i); b != want {
			t.Fatalf("mixed byte %d after Seek: got %d; want %d (stale data or discontinuity)", i, b, want)
		}
	}
}

// TestAgentStopPlayBytesContinuous covers A6 for stop-reading then Play: the
// output stays continuous with no lost or replayed bytes.
func TestAgentStopPlayBytesContinuous(t *testing.T) {
	const bufferSize = 256

	src := &agentRampSource{}
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()
	p := m.NewPlayer(src)
	defer func() { _ = p.Close() }()
	p.SetBufferSize(bufferSize)
	p.Play()

	// Consume a prefix continuously.
	played := agentCollectBytes(t, m, p, bufferSize)

	// Let the buffer top up again, then stop and resume.
	agentWaitFor(t, "buffer refill", time.Second, func() bool { return p.BufferedSize() >= bufferSize })
	p.PauseAndStopReading()

	buffered := p.BufferedSize()
	if buffered == 0 {
		t.Fatal("PauseAndStopReading dropped the buffered data")
	}
	time.Sleep(40 * time.Millisecond)
	if got := p.BufferedSize(); got != buffered {
		t.Fatalf("BufferedSize changed while stopped: got %d; want %d", got, buffered)
	}

	p.Play()
	played = append(played, agentCollectBytes(t, m, p, 4*bufferSize)...)
	for i, b := range played {
		if want := byte(i); b != want {
			t.Fatalf("played byte %d: got %d; want %d (loss or replay around %d)", i, b, want, int(b)-i)
		}
	}
}
