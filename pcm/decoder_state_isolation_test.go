package pcm

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	flac "github.com/tphakala/go-flac"
)

// TestDecoderFailedNilResetOnZeroValuePoisonedAndRecoverable covers the one Reset
// failure that is rejected before init's normal clearStream: Reset(nil) on a zero
// value has no live stream to preserve, so Read and WriteTo must report that failure
// instead of dereferencing the nil bit reader. A later valid Reset recovers.
func TestDecoderFailedNilResetOnZeroValuePoisonedAndRecoverable(t *testing.T) {
	good := makeStream(t, resetShapes["stereo16"], 9000)

	var d Decoder
	rerr := d.Reset(nil)
	if rerr == nil {
		t.Fatal("Reset(nil): want error, got nil")
	}
	if n, err := d.Read(make([]byte, 16)); n != 0 || !errors.Is(err, rerr) {
		t.Fatalf("Read after Reset(nil) = (%d, %v), want (0, %v)", n, err, rerr)
	}
	if n, err := d.WriteTo(io.Discard); n != 0 || !errors.Is(err, rerr) {
		t.Fatalf("WriteTo after Reset(nil) = (%d, %v), want (0, %v)", n, err, rerr)
	}
	rerr2 := d.Reset(nil)
	if rerr2 == nil {
		t.Fatal("second Reset(nil): want error, got nil")
	}
	if n, err := d.Read(make([]byte, 16)); n != 0 || !errors.Is(err, rerr2) || err == rerr {
		t.Fatalf("Read after second Reset(nil) = (%d, %v), want the latest error %v", n, err, rerr2)
	}

	if err := d.Reset(bytes.NewReader(good)); err != nil {
		t.Fatalf("recovery Reset: %v", err)
	}
	got, err := io.ReadAll(&d)
	if err != nil {
		t.Fatalf("decode after recovery: %v", err)
	}
	_, want := decodeAll(t, bytes.NewReader(good))
	if !bytes.Equal(got, want) {
		t.Fatalf("recovered decode differs from fresh decode: %d vs %d bytes", len(got), len(want))
	}
}

// TestDecoderZeroValueReadReturnsErrorWithoutPanic keeps direct use of an
// uninitialized zero value safe as well. Reset remains the supported entry point.
func TestDecoderZeroValueReadReturnsErrorWithoutPanic(t *testing.T) {
	var d Decoder
	if n, err := d.Read(make([]byte, 16)); n != 0 || err == nil {
		t.Fatalf("zero-value Read = (%d, %v), want (0, non-nil error)", n, err)
	}
	if _, err := d.WriteTo(io.Discard); err == nil {
		t.Fatal("zero-value WriteTo: want non-nil error, got nil")
	}
}

// TestDecoderArgumentSeekErrorsPreserveReadPosition pins the state-independent
// argument/capability failures: after either error, reading from the current
// position must still reproduce the complete stream.
func TestDecoderArgumentSeekErrorsPreserveReadPosition(t *testing.T) {
	data, _ := encodeRamp(t, 2, 16, 5*4096+11)
	_, want := decodeAll(t, bytes.NewReader(data))

	t.Run("unsupported source", func(t *testing.T) {
		d, err := NewDecoder(readerOnly{bytes.NewReader(data)})
		if err != nil {
			t.Fatal(err)
		}
		pre := make([]byte, 37)
		if _, err := io.ReadFull(d, pre); err != nil {
			t.Fatal(err)
		}
		if _, err := d.SeekToSample(123); !errors.Is(err, flac.ErrSeekUnsupported) {
			t.Fatalf("SeekToSample err = %v, want ErrSeekUnsupported", err)
		}
		rest, err := io.ReadAll(d)
		if err != nil {
			t.Fatal(err)
		}
		got := append(bytes.Clone(pre), rest...)
		if !bytes.Equal(got, want) {
			t.Fatalf("unsupported seek changed readable state: got %d bytes, want %d", len(got), len(want))
		}
	})

	t.Run("negative index", func(t *testing.T) {
		d, err := NewDecoder(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		pre := make([]byte, 41)
		if _, err := io.ReadFull(d, pre); err != nil {
			t.Fatal(err)
		}
		if _, err := d.SeekToSample(-1); !errors.Is(err, flac.ErrInvalidSeek) {
			t.Fatalf("SeekToSample(-1) err = %v, want ErrInvalidSeek", err)
		}
		rest, err := io.ReadAll(d)
		if err != nil {
			t.Fatal(err)
		}
		got := append(bytes.Clone(pre), rest...)
		if !bytes.Equal(got, want) {
			t.Fatalf("negative seek changed readable state: got %d bytes, want %d", len(got), len(want))
		}
	})
}

// TestDecoderPoolMixedFailuresAndRecovery exercises independent pooled instances in
// parallel. Some leases hit a sticky seek failure and recover; others Reset to a
// different shape and decode it. A shared stream-level field would cross wires here.
func TestDecoderPoolMixedFailuresAndRecovery(t *testing.T) {
	good := makeStream(t, resetShapes["stereo16"], 12000)
	other := makeStream(t, resetShapes["mono24"], 9000)

	ref, err := NewDecoder(bytes.NewReader(good))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ref.SeekToSample(128); err != nil {
		t.Fatal(err)
	}
	wantRecovered, err := io.ReadAll(ref)
	if err != nil {
		t.Fatal(err)
	}
	_, wantOther := decodeAll(t, bytes.NewReader(other))

	pool := sync.Pool{New: func() any { return new(Decoder) }}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for range 10 {
				func() {
					d := pool.Get().(*Decoder)
					defer pool.Put(d)

					if g%3 == 0 {
						if err := d.Reset(bytes.NewReader(other)); err != nil {
							t.Errorf("Reset(other): %v", err)
							return
						}
						got, err := io.ReadAll(d)
						if err != nil {
							t.Errorf("decode other: %v", err)
							return
						}
						if !bytes.Equal(got, wantOther) {
							t.Errorf("other stream mismatch: %d vs %d bytes", len(got), len(wantOther))
						}
						return
					}

					src := newFailingSource(good)
					if err := d.Reset(src); err != nil {
						t.Errorf("Reset(good): %v", err)
						return
					}
					src.arm(0)
					if _, err := d.SeekToSample(8192); !errors.Is(err, errSourceIO) {
						t.Errorf("armed SeekToSample err = %v, want errSourceIO", err)
						return
					}
					src.disarm()
					if n, err := d.Read(make([]byte, 16)); n != 0 || !errors.Is(err, errSourceIO) {
						t.Errorf("sticky Read = (%d, %v), want (0, errSourceIO)", n, err)
						return
					}
					if _, err := d.SeekToSample(128); err != nil {
						t.Errorf("recovery SeekToSample: %v", err)
						return
					}
					got, err := io.ReadAll(d)
					if err != nil {
						t.Errorf("decode after recovery: %v", err)
						return
					}
					if !bytes.Equal(got, wantRecovered) {
						t.Errorf("recovered stream mismatch: %d vs %d bytes", len(got), len(wantRecovered))
					}
				}()
			}
		}(g)
	}
	wg.Wait()
}
