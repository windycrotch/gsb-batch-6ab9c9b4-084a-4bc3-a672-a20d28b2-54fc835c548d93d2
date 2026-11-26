// SPDX-License-Identifier: LGPL-2.1-or-later

package dec

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestFailedUnitIsTransaction pins the observable contract: inserting a
// corrupt unit between valid ones must leave every following frame's PCM
// byte-identical to a decoder that never saw the corrupt unit, however many
// times the corruption repeats, and whether the corrupt unit fails before or
// after it has written overlap, PNS or presence state.
//
// The corruptions exercise every parse stage the transaction must undo: a
// shape-flip truncation fails inside the side info after decodeICSInfo shifted
// the window history and decodeCPE wrote the M/S mask; a mid-payload byte zero
// can fail later, after some bands' scalefactors and spectral symbols were
// written; a truncation at most lengths covers the rest. The follow-on frames
// are decoded with the same decoder under test, so a leaked PNS seed shows up
// as a different noise band in the next frame, and a leaked window history as a
// different overlap.
func TestFailedUnitIsTransaction(t *testing.T) {
	streams := []string{
		"sine_m8_24k", "tonal_s48_128k", "goenc_s48_128k",
		"pns_m48_24k", "is_s22_48k_fast", "click_s44_128k", "noise_m44_96k_tns",
	}
	for _, name := range streams {
		data, err := os.ReadFile(filepath.Join("testdata", name+".adts"))
		if err != nil {
			t.Skipf("%s: %v", name, err)
		}
		frames := splitADTSFrames(t, data)
		if len(frames) < 8 {
			t.Skipf("%s: only %d frames", name, len(frames))
		}
		// Two positions: an early frame and one near the tail, so a leaked
		// overlap or PNS seed has room to change several following frames.
		for _, k := range []int{2, len(frames) - 4} {
			if k <= 0 || k+3 >= len(frames) {
				continue
			}
			// repeat=1 for every corruption, and a single deeper repeat (5) for
			// the shape flip, which is the one that fails after the window
			// history, M/S mask and PNS-adjacent parse state were written.
			for _, bad := range corruptUnits(t, frames[k]) {
				if !failsEveryTime(frames, k, bad.data) {
					// A zeroed or truncated unit can still parse on this
					// stream; it then advances state legitimately and the
					// transaction contract does not apply.
					continue
				}
				for _, repeat := range repeatsFor(bad.name) {
					t.Run(name+"/"+bad.name+"/x"+itoa(repeat), func(t *testing.T) {
						checkTransaction(t, frames, k, bad.data, repeat)
					})
				}
			}
		}
	}
}

// repeatsFor returns the number of consecutive corrupt units to feed for a
// corruption kind. Every kind gets one; the shape flip also gets a deeper run
// to pin that repeated failures do not accumulate.
func repeatsFor(name string) []int {
	if name == "shape_flip" {
		return []int{1, 5}
	}
	return []int{1}
}

// corruptUnit is one damaged access unit and a name for it.
type corruptUnit struct {
	name string
	data []byte
}

// corruptUnits returns a set of corruptions of f that must fail to decode: the
// shape flip (fails after the window history and M/S mask were written), a
// zeroed byte at a few payload offsets, and truncations at every length.
func corruptUnits(t *testing.T, f []byte) []corruptUnit {
	t.Helper()
	out := []corruptUnit{{"shape_flip", shapeFlipTruncated(t, f)}}
	for _, off := range []int{4, 12, 40} {
		if off >= len(f) {
			continue
		}
		b := bytes.Clone(f)
		b[off] = 0
		out = append(out, corruptUnit{"zero_" + itoa(off), b})
	}
	step := max(1, len(f)/6)
	for n := adtsHeaderSize + 1; n < len(f); n += step {
		out = append(out, corruptUnit{"trunc_" + itoa(n), bytes.Clone(f[:n])})
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// failsEveryTime reports whether bad fails to decode after frames[0:k] on
// every one of a few warm decoders, so the test only asserts the transaction
// contract on units that deterministically error.
func failsEveryTime(frames [][]byte, k int, bad []byte) bool {
	for range 3 {
		d := NewADTS()
		dst := make([]byte, 0, 1<<14)
		ok := true
		for _, f := range frames[:k] {
			var err error
			if dst, _, err = d.AppendS16(dst[:0], f); err != nil {
				ok = false
				break
			}
		}
		if !ok {
			return false
		}
		if _, _, err := d.AppendS16(dst[:0], bad); err == nil {
			return false
		}
	}
	return true
}

// checkTransaction feeds frames[0:k], then repeat copies of bad, then the rest
// of the stream, to a decoder, and compares the PCM from frame k+1 on with a
// decoder that saw frames[0:k] and the rest without the corrupt units. Every
// corrupt copy must fail (or, if it parses, be fed to the reference too, since
// a parse that succeeds legitimately advances state).
func checkTransaction(t *testing.T, frames [][]byte, k int, bad []byte, repeat int) {
	t.Helper()
	d := NewADTS()
	ref := NewADTS()
	dst := make([]byte, 0, 1<<14)
	for _, f := range frames[:k] {
		var err error
		if dst, _, err = d.AppendS16(dst[:0], f); err != nil {
			t.Fatalf("warm frame: %v", err)
		}
		if dst, _, err = ref.AppendS16(dst[:0], f); err != nil {
			t.Fatalf("ref warm frame: %v", err)
		}
	}
	errored := 0
	for range repeat {
		if _, _, err := d.AppendS16(dst[:0], bad); err != nil {
			errored++
		}
	}
	if errored != repeat {
		t.Fatalf("%d of %d corrupt units errored", errored, repeat)
	}
	var got, want []byte
	for _, f := range frames[k+1:] {
		var err error
		if got, _, err = d.AppendS16(got, f); err != nil {
			t.Fatalf("follow-on frame after %d corrupt units: %v", repeat, err)
		}
		if want, _, err = ref.AppendS16(want, f); err != nil {
			t.Fatalf("reference follow-on frame: %v", err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("output after %d corrupt units differs from a decoder that never saw them", repeat)
	}
}

// TestFailedFirstADTSFrameLeavesNoConfig checks that a first ADTS frame whose
// header configures the decoder but whose payload fails rolls back the
// configuration too, so a later frame of a different layout decodes as it would
// on a fresh decoder.
func TestFailedFirstADTSFrameLeavesNoConfig(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "tonal_s48_128k.adts"))
	if err != nil {
		t.Skip(err)
	}
	frames := splitADTSFrames(t, data)
	if len(frames) < 4 {
		t.Skip("too few frames")
	}
	bad := shapeFlipTruncated(t, frames[0])
	d := NewADTS()
	for range 3 {
		if _, _, err := d.AppendS16(nil, bad); err == nil {
			t.Fatal("corrupt first frame decoded without error")
		}
		if d.Channels() != 0 {
			t.Fatalf("Channels() = %d after a failed first frame, want 0", d.Channels())
		}
		if d.Config() != (Config{}) {
			t.Fatalf("Config() = %+v after a failed first frame, want the zero value", d.Config())
		}
	}
	// A mono stream must then be accepted: the stereo header the failed frame
	// latched must not stick as a mid-stream change.
	monoData, err := os.ReadFile(filepath.Join("testdata", "sine_m8_24k.adts"))
	if err != nil {
		t.Skip(err)
	}
	mono := splitADTSFrames(t, monoData)
	var got []byte
	for i, f := range mono {
		if got, _, err = d.AppendS16(got, f); err != nil {
			t.Fatalf("mono frame %d after failed first frame: %v", i, err)
		}
	}
	fresh := NewADTS()
	var want []byte
	for _, f := range mono {
		var err error
		if want, _, err = fresh.AppendS16(want, f); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatal("mono decode after a failed first frame differs from a fresh decoder")
	}
}

// TestPooledLayoutSwitchMatchesFresh pins that reusing one decoder across a
// channel-layout change yields byte-identical output to a fresh instance: no
// element from the old layout (or its noise or overlap state) may leak into the
// new stream.
func TestPooledLayoutSwitchMatchesFresh(t *testing.T) {
	pairs := [][2]string{
		{"goenc_s48_128k", "sine_m8_24k"},     // stereo then mono
		{"sine_m8_24k", "goenc_s48_128k"},     // mono then stereo
		{"tonal_s48_128k", "is_s22_48k_fast"}, // stereo then stereo, different rate
	}
	load := func(t *testing.T, name string) [][]byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("testdata", name+".adts"))
		if err != nil {
			t.Skipf("%s: %v", name, err)
		}
		return splitADTSFrames(t, data)
	}
	decodeAll := func(d *Decoder, frames [][]byte) []byte {
		var out []byte
		for i, f := range frames {
			var err error
			if out, _, err = d.AppendS16(out, f); err != nil {
				t.Fatalf("frame %d: %v", i, err)
			}
		}
		return out
	}
	for _, pair := range pairs {
		first, second := load(t, pair[0]), load(t, pair[1])
		t.Run(pair[0]+"->"+pair[1], func(t *testing.T) {
			pooled := NewADTS()
			decodeAll(pooled, first) // warm and leave the old layout's state behind
			pooled.ResetADTS()
			got := decodeAll(pooled, second)
			want := decodeAll(NewADTS(), second)
			if !bytes.Equal(got, want) {
				t.Fatal("pooled decoder output after a layout switch differs from a fresh decoder")
			}
		})
	}
}

// stateView is a fingerprint of every cross-frame field a failed unit must
// leave untouched: the visible element list, each element's stereo flags and
// M/S mask, each channel's window history, per-band parse state and overlap,
// and the decoder configuration and PNS seed.
type stateView struct {
	cfg        Config
	configured bool
	random     int32
	elems      []ElemRef
	slots      []slotView
}

type slotView struct {
	typ, id      int
	che          *CPE
	present      bool
	commonWindow bool
	msPresent    int
	maxSFBSte    int
	msMask       [128]uint8
	ch           [2]chView
}

type chView struct {
	ics       ICSInfo
	tns       TNSData
	pulse     Pulse
	pulsePres bool
	bandType  [128]uint8
	sfo       [128]int32
	sf        [128]int32
	saved     [overlapLen]int32
}

func snapshotState(d *Decoder) stateView {
	v := stateView{cfg: d.cfg, configured: d.configured, random: d.randomState}
	v.elems = append([]ElemRef(nil), d.Elems...)
	for t := range d.che {
		for id, che := range d.che[t] {
			if che == nil {
				continue
			}
			sv := slotView{typ: t, id: id, che: che, present: che.present,
				commonWindow: che.commonWindow, msPresent: che.msPresent,
				maxSFBSte: che.MaxSFBSte, msMask: che.MSMask}
			for c := range 2 {
				sv.ch[c] = chView{ics: che.Ch[c].ICS, tns: che.Ch[c].TNS,
					pulse: che.Ch[c].Pulse, pulsePres: che.Ch[c].PulsePresent,
					bandType: che.Ch[c].BandType, sfo: che.Ch[c].SFO, sf: che.Ch[c].SF}
				copy(sv.ch[c].saved[:], che.Ch[c].Saved[:overlapLen])
			}
			v.slots = append(v.slots, sv)
		}
	}
	return v
}

func statesEqual(a, b stateView) bool {
	if a.cfg != b.cfg || a.configured != b.configured || a.random != b.random {
		return false
	}
	if len(a.elems) != len(b.elems) || len(a.slots) != len(b.slots) {
		return false
	}
	for i := range a.elems {
		if a.elems[i] != b.elems[i] {
			return false
		}
	}
	for i := range a.slots {
		x, y := a.slots[i], b.slots[i]
		if x.typ != y.typ || x.id != y.id || x.che != y.che || x.present != y.present ||
			x.commonWindow != y.commonWindow || x.msPresent != y.msPresent ||
			x.maxSFBSte != y.maxSFBSte || x.msMask != y.msMask {
			return false
		}
		for c := range 2 {
			if x.ch[c].ics.WindowSequence != y.ch[c].ics.WindowSequence ||
				x.ch[c].ics.UseKBWindow != y.ch[c].ics.UseKBWindow ||
				x.ch[c].ics.MaxSFB != y.ch[c].ics.MaxSFB ||
				x.ch[c].ics.NumWindows != y.ch[c].ics.NumWindows ||
				x.ch[c].ics.NumWindowGroups != y.ch[c].ics.NumWindowGroups ||
				x.ch[c].ics.GroupLen != y.ch[c].ics.GroupLen ||
				x.ch[c].pulse != y.ch[c].pulse || x.ch[c].pulsePres != y.ch[c].pulsePres ||
				x.ch[c].tns != y.ch[c].tns || x.ch[c].bandType != y.ch[c].bandType ||
				x.ch[c].sfo != y.ch[c].sfo || x.ch[c].sf != y.ch[c].sf ||
				x.ch[c].saved != y.ch[c].saved {
				return false
			}
		}
	}
	return true
}

// TestFailedUnitRestoresAllCrossFrameState pins that after a failed unit the
// decoder's externally visible element list, configuration and every
// cross-frame field are bit-identical to their values before the call. It
// deliberately tests state, not just PCM: the visible element list is part of
// what a caller can observe, and baseline cleared it on some failure paths.
func TestFailedUnitRestoresAllCrossFrameState(t *testing.T) {
	for _, name := range []string{"tonal_s48_128k", "sine_m8_24k", "pns_m48_24k", "goenc_s48_128k"} {
		data, err := os.ReadFile(filepath.Join("testdata", name+".adts"))
		if err != nil {
			continue
		}
		frames := splitADTSFrames(t, data)
		if len(frames) < 8 {
			continue
		}
		for _, k := range []int{3} {
			if k+1 >= len(frames) {
				continue
			}
			// Every corruption that fails must restore every field.
			for _, bad := range corruptUnits(t, frames[k]) {
				if !failsEveryTime(frames, k, bad.data) {
					continue
				}
				d := NewADTS()
				dst := make([]byte, 0, 1<<14)
				ok := true
				for _, f := range frames[:k] {
					if dst, _, err = d.AppendS16(dst[:0], f); err != nil {
						ok = false
						break
					}
				}
				if !ok {
					continue
				}
				before := snapshotState(d)
				for range 2 { // consecutive failures must each restore
					if _, _, err := d.AppendS16(dst[:0], bad.data); err == nil {
						break
					}
					after := snapshotState(d)
					if !statesEqual(before, after) {
						t.Fatalf("%s k=%d %s: cross-frame state changed after a failed unit", name, k, bad.name)
					}
				}
			}
		}
	}
}
