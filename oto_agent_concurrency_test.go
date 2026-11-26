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

package oto_test

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAgentPlayerStopSeekConcurrency drives a player created through the
// production entry point oto.Context.NewPlayer with concurrent Play, Pause,
// PauseAndStopReading, Reset and Seek calls. Run under -race, the mux
// serializer must keep source Read and Seek from overlapping and must not
// deadlock; afterwards Err must keep its mux-layer (nil here) semantics.
func TestAgentPlayerStopSeekConcurrency(t *testing.T) {
	src := bytes.NewReader(make([]byte, 1<<16))
	p := theContext.NewPlayer(src)
	p.SetBufferSize(1024)

	var stop atomic.Bool
	var wg sync.WaitGroup
	worker := func(fn func(i int)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var i int
			for !stop.Load() {
				fn(i)
				i++
			}
		}()
	}

	worker(func(int) { p.Play() })
	worker(func(int) { p.Pause() })
	worker(func(int) { p.PauseAndStopReading() })
	worker(func(i int) {
		_, _ = p.Seek(int64((i%64)*256), io.SeekStart)
	})
	worker(func(i int) {
		if i%8 == 0 {
			p.Reset()
		} else {
			p.Play()
		}
	})

	time.Sleep(400 * time.Millisecond)
	stop.Store(true)

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("concurrent player controls did not finish")
	}

	// Quiesce the player and verify liveness plus Err semantics.
	done := make(chan struct{})
	go func() {
		p.PauseAndStopReading()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PauseAndStopReading blocked after concurrent controls")
	}
	if err := p.Err(); err != nil {
		t.Fatalf("Err after concurrent stop/Seek: got %v; want nil", err)
	}
}
