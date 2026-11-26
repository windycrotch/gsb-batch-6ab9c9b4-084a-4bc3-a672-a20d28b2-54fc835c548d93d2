// SPDX-License-Identifier: LGPL-2.1-or-later

package pcm

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	aac "github.com/tphakala/go-aac"
)

// TestFrameDecoderTransactionByteExact extends the recovery contract to the
// reconstructed path: a corrupt ADTS or raw access unit that returns an error
// must leave the decoder as if it was never offered, so every following frame
// (in particular a noise band drawing on the PNS generator and the overlap the
// next frame reads) decodes byte-identically to a decoder that never saw it.
// The corruption set includes a shape flip (fails after the window history and
// M/S mask were written) and payload truncations at many lengths.
func TestFrameDecoderTransactionByteExact(t *testing.T) {
	for _, s := range recoveryStreams(t) {
		if len(s.frames) < 6 {
			t.Skipf("%s: only %d frames", s.name, len(s.frames))
		}
		for _, k := range []int{2, len(s.frames) - 4} {
			if k <= 0 || k+2 >= len(s.frames) {
				continue
			}
			f := s.frames[k]
			bads := map[string][]byte{"shape_flip": mustShapeFlip(t, f, s.hdr)}
			for _, n := range payloadCuts(len(f)) {
				bads["trunc_"+strconv.Itoa(n)] = bytes.Clone(f[:n])
			}
			for name, bad := range bads {
				// The shape flip is the failure that lands after the most state
				// was written, so it also gets a deeper repeat run; truncations
				// get one.
				depth := 1
				if name == "shape_flip" {
					depth = 3
				}
				t.Run(s.name+"/"+name, func(t *testing.T) {
					runTransaction(t, s.mk, s.frames, k, bad, depth)
				})
			}
		}
	}
}

func mustShapeFlip(t *testing.T, f []byte, hdr int) []byte {
	t.Helper()
	bad, err := shapeFlipUnit(f, hdr)
	if err != nil {
		t.Skipf("cannot shape-flip unit: %v", err)
	}
	return bad
}

func payloadCuts(n int) []int {
	if n <= adtsHeaderLen+1 {
		return nil
	}
	step := max(1, n/6)
	var out []int
	for c := adtsHeaderLen + 1; c < n; c += step {
		out = append(out, c)
	}
	return out
}

// runTransaction decodes frames[0:k], then d copies of bad (each must error,
// else the subtest is skipped as the unit is one that legitimately parses),
// then the rest, and compares the whole output after frame k with a decoder
// that saw only the accepted units.
func runTransaction(t *testing.T, mk func() *FrameDecoder, frames [][]byte, k int, bad []byte, d int) {
	t.Helper()
	dec, ref := mk(), mk()
	warm := func(x *FrameDecoder) []byte {
		var out []byte
		for i, f := range frames[:k] {
			var err error
			if out, _, err = x.DecodeFrame(out, f); err != nil {
				t.Fatalf("warm frame %d: %v", i, err)
			}
		}
		return out
	}
	warm(dec)
	warm(ref)
	for range d {
		if _, _, err := dec.DecodeFrame(nil, bad); err != nil {
			continue
		}
		// It parsed: state legitimately advances, and the reference must agree.
		if _, _, err := ref.DecodeFrame(nil, bad); err != nil {
			t.Fatalf("reference rejected a unit the decoder accepted: %v", err)
		}
		t.Skip("corrupt unit parsed on this stream")
	}
	var got, want []byte
	for i := k + 1; i < len(frames); i++ {
		var err error
		if got, _, err = dec.DecodeFrame(got, frames[i]); err != nil {
			t.Fatalf("frame %d after %d corrupt units: %v", i, d, err)
		}
		if want, _, err = ref.DecodeFrame(want, frames[i]); err != nil {
			t.Fatalf("reference frame %d: %v", i, err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("output after %d corrupt units differs from a decoder that never saw them", d)
	}
}

// TestFrameDecoderPooledLayoutSwitchMatchesFresh pins the pooled-reuse
// contract: decoding one stream, Reset, then decoding a stream of a different
// channel layout on the same FrameDecoder must match a fresh instance over the
// second stream, with no leftover element or noise state from the first.
func TestFrameDecoderPooledLayoutSwitchMatchesFresh(t *testing.T) {
	stereoCfg := frameDecoderConfigs[2].cfg // stereo 48k
	monoCfg := frameDecoderConfigs[0].cfg   // mono 48k
	otherCfg := frameDecoderConfigs[1].cfg  // stereo 44k1

	sets := map[string][][]byte{
		"stereo": splitADTSFrames(t, encodeADTS(t, stereoCfg, testPCM(6000, stereoCfg))),
		"mono":   splitADTSFrames(t, encodeADTS(t, monoCfg, testPCM(6000, monoCfg))),
		"other":  splitADTSFrames(t, encodeADTS(t, otherCfg, testPCM(6000, otherCfg))),
	}
	decode := func(d *FrameDecoder, frames [][]byte) []byte {
		var out []byte
		for i, f := range frames {
			var err error
			if out, _, err = d.DecodeFrame(out, f); err != nil {
				t.Fatalf("frame %d: %v", i, err)
			}
		}
		return out
	}
	type pair struct{ a, b string }
	for _, p := range []pair{{"stereo", "mono"}, {"mono", "stereo"}, {"stereo", "other"}} {
		t.Run(p.a+"_then_"+p.b, func(t *testing.T) {
			d := NewADTSDecoder()
			decode(d, sets[p.a]) // prime the old layout, overlap and PNS state
			if err := d.Reset(); err != nil {
				t.Fatalf("Reset: %v", err)
			}
			got := decode(d, sets[p.b])
			want := decode(NewADTSDecoder(), sets[p.b])
			if !bytes.Equal(got, want) {
				t.Fatalf("pooled %s->%s output differs from a fresh decoder", p.a, p.b)
			}
			if n := d.Channels(); n != channelCount(p.b) {
				t.Errorf("Channels() = %d after switch, want %d", n, channelCount(p.b))
			}
		})
	}
}

func channelCount(name string) int {
	switch name {
	case "mono":
		return 1
	default:
		return 2
	}
}

// TestFrameDecoderRepeatedFailuresRecover pins that any number of consecutive
// failed units is recoverable, and that a first-frame failure leaves the
// decoder unconfigured so a different layout's stream is accepted afterwards.
func TestFrameDecoderRepeatedFailuresRecover(t *testing.T) {
	stereoCfg := frameDecoderConfigs[2].cfg
	monoCfg := frameDecoderConfigs[0].cfg
	stereo := splitADTSFrames(t, encodeADTS(t, stereoCfg, testPCM(6000, stereoCfg)))
	mono := splitADTSFrames(t, encodeADTS(t, monoCfg, testPCM(6000, monoCfg)))

	// Repeated failures on a failed first frame: the stereo header it carries
	// must not stick, so a mono stream decodes as on a fresh decoder.
	d := NewADTSDecoder()
	badFirst := mustShapeFlip(t, stereo[0], adtsHeaderLen)
	for i := range 5 {
		if _, _, err := d.DecodeFrame(nil, badFirst); !errors.Is(err, ErrCorruptStream) {
			t.Fatalf("failed first frame %d err = %v, want ErrCorruptStream", i, err)
		}
		if ch := d.Channels(); ch != 0 {
			t.Fatalf("Channels() = %d after failed first frame, want 0", ch)
		}
	}
	var got []byte
	for i, f := range mono {
		var err error
		if got, _, err = d.DecodeFrame(got, f); err != nil {
			t.Fatalf("mono frame %d after failed first frame: %v", i, err)
		}
	}
	fresh := NewADTSDecoder()
	var want []byte
	for _, f := range mono {
		var err error
		if want, _, err = fresh.DecodeFrame(want, f); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatal("mono decode after a failed first frame differs from a fresh decoder")
	}
	if n := d.Channels(); n != monoCfg.Channels {
		t.Fatalf("Channels() = %d after mono stream, want %d", n, monoCfg.Channels)
	}
	if got, want := len(got), len(mono)*aac.FrameSize*monoCfg.Channels*2; got != want {
		t.Fatalf("emitted %d bytes, want %d", got, want)
	}
}

// TestFrameDecoderFailureDoesNotAllocatePerFrame pins that the failure
// transaction copies state, not heap: a warmed decoder that rejects a corrupt
// unit performs a small constant number of allocations (the wrapped error
// value), never one per sample or per copied field, and the valid frame after
// it still allocates nothing.
func TestFrameDecoderFailureDoesNotAllocatePerFrame(t *testing.T) {
	cfg := frameDecoderConfigs[2].cfg
	frames := splitADTSFrames(t, encodeADTS(t, cfg, testPCM(6000, cfg)))
	if len(frames) < 4 {
		t.Fatalf("only %d frames", len(frames))
	}
	bad := mustShapeFlip(t, frames[1], adtsHeaderLen)
	d := NewADTSDecoder()
	dst := make([]byte, 0, aac.FrameSize*cfg.Channels*2)
	// Warm the success path, the failure path and the pooled buffers.
	for range 3 {
		if _, _, err := d.DecodeFrame(dst[:0], frames[0]); err != nil {
			t.Fatal(err)
		}
		if _, _, err := d.DecodeFrame(dst[:0], bad); err == nil {
			t.Fatal("corrupt unit decoded; the failure path was not exercised")
		}
	}
	// The wrapper error value is a couple of allocations and is unavoidable;
	// what must not happen is the count scaling with the state restored.
	perFailure := testing.AllocsPerRun(50, func() {
		if _, _, err := d.DecodeFrame(dst[:0], bad); err == nil {
			t.Fatal("corrupt unit decoded")
		}
	})
	perSuccess := testing.AllocsPerRun(50, func() {
		if _, _, err := d.DecodeFrame(dst[:0], frames[0]); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("allocs: corrupt unit %.1f, valid unit %.1f", perFailure, perSuccess)
	if perSuccess != 0 {
		t.Errorf("valid frame after a failure allocates %.1f, want 0", perSuccess)
	}
	if failureAllocCeiling >= 0 && perFailure > float64(failureAllocCeiling) {
		t.Errorf("rejecting a corrupt unit allocates %.1f, want a small constant (the wrapped error)", perFailure)
	}
}
