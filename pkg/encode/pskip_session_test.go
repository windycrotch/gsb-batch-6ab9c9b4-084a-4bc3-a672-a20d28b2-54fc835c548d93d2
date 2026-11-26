package encode

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Eyevinn/mp4ff/avc"
)

// The tests in this file cover the session behaviour of PSkipExtender on top of the numbering
// rules already checked in pskip_extender_test.go: that generated frames continue across calls,
// that overlapping source access units are refused, that an IDR starts a new baseline, that Close
// ends the session, and that all of it is safe under concurrent use.

// sessionExt returns an extender that has observed source and is ready to generate.
func sessionExt(t *testing.T, source []byte) (*PSkipExtender, *avc.SPS, *avc.PPS) {
	t.Helper()
	sps, pps := mustParseSPSPPS(t, source)
	ext, err := NewPSkipExtender(sps, pps)
	if err != nil {
		t.Fatalf("NewPSkipExtender: %v", err)
	}
	if err := ext.ObserveAnnexB(source); err != nil {
		t.Fatalf("ObserveAnnexB: %v", err)
	}
	return ext, sps, pps
}

// sliceHeaderOf parses the header of a generated Annex-B slice against ext's parameter sets.
func sliceHeaderOf(t *testing.T, ext *PSkipExtender, annexB []byte) *avc.SliceHeader {
	t.Helper()
	nalus := avc.ExtractNalusFromByteStream(annexB)
	if len(nalus) != 1 {
		t.Fatalf("expected one NAL unit, got %d", len(nalus))
	}
	ext.mu.Lock()
	spsMap, ppsMap := ext.spsMap, ext.ppsMap
	ext.mu.Unlock()
	sh, err := avc.ParseSliceHeader(nalus[0], spsMap, ppsMap)
	if err != nil {
		t.Fatalf("ParseSliceHeader: %v", err)
	}
	return sh
}

// requireContinues checks that a generated slice carries the expected numbering.
func requireContinues(t *testing.T, ext *PSkipExtender, slice []byte, wantFrameNum, wantPOC uint32) {
	t.Helper()
	sh := sliceHeaderOf(t, ext, slice)
	if sh.FrameNum != wantFrameNum || sh.PicOrderCntLsb != wantPOC {
		t.Fatalf("generated slice numbered (%d, %d), want (%d, %d)",
			sh.FrameNum, sh.PicOrderCntLsb, wantFrameNum, wantPOC)
	}
}

// pSkipObserveNALU turns a generated slice into an observable NAL unit, as a source that resumed
// with the same numbering would send.
func pSkipObserveNALU(t *testing.T, annexB []byte) []byte {
	t.Helper()
	return avc.ExtractNalusFromByteStream(annexB)[0]
}

// sourceNALU returns the first NAL unit of the given type in an Annex-B stream.
func sourceNALU(t *testing.T, stream []byte, want avc.NaluType) []byte {
	t.Helper()
	for _, nalu := range avc.ExtractNalusFromByteStream(stream) {
		if len(nalu) > 0 && avc.GetNaluType(nalu[0]) == want {
			return nalu
		}
	}
	t.Fatalf("no NAL unit of type %d in the stream", want)
	return nil
}

// TestPSkipExtenderNumberingContinuesAcrossCalls covers the self-consistency of the numbering: two
// successive generation calls continue where the first left off, and the second batch repeats no
// value the first used.
func TestPSkipExtenderNumberingContinuesAcrossCalls(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, _, _ := sessionExt(t, source)
	defer ext.Close()

	first, err := ext.NextSlices(4)
	if err != nil {
		t.Fatalf("first NextSlices: %v", err)
	}
	second, err := ext.NextSlices(3)
	if err != nil {
		t.Fatalf("second NextSlices: %v", err)
	}

	// The IDR is (frame_num 0, poc 0); the first generated frame is therefore (1, 2).
	for i, slice := range first {
		requireContinues(t, ext, slice, uint32(i+1), uint32(2*(i+1)))
	}
	// The second batch continues at (5, 10) rather than restarting.
	for i, slice := range second {
		requireContinues(t, ext, slice, uint32(5+i), uint32(10+2*i))
	}

	// No numbering is used twice when both batches are concatenated and parsed in order.
	extended := bytes.Join(append(append([][]byte{source}, first...), second...), nil)
	frameNums, pocLsbs := parseFrameNumAndPoc(t, extended)
	if len(frameNums) != 8 {
		t.Fatalf("got %d slices, want 8", len(frameNums))
	}
	for i := 1; i < len(frameNums); i++ {
		if frameNums[i] == frameNums[i-1] {
			t.Errorf("frame_num %d repeated at slice %d", frameNums[i], i)
		}
		if pocLsbs[i] == pocLsbs[i-1] {
			t.Errorf("pic_order_cnt_lsb %d repeated at slice %d", pocLsbs[i], i)
		}
	}
	if got := ext.Generated(); got != 7 {
		t.Errorf("Generated = %d, want 7", got)
	}
	if fn, poc := ext.State(); fn != 7 || poc != 14 {
		t.Errorf("State = (%d, %d), want (7, 14)", fn, poc)
	}
}

// TestPSkipExtenderContinuesAfterSingleSlices checks the same continuation through NextSlice, so
// the one-frame and many-frame entry points advance identically.
func TestPSkipExtenderContinuesAfterSingleSlices(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, _, _ := sessionExt(t, source)
	defer ext.Close()

	a, err := ext.NextSlice()
	if err != nil {
		t.Fatalf("NextSlice: %v", err)
	}
	b, err := ext.NextSlice()
	if err != nil {
		t.Fatalf("NextSlice: %v", err)
	}
	requireContinues(t, ext, a, 1, 2)
	requireContinues(t, ext, b, 2, 4)

	// Mixing NextSlices after NextSlice keeps the sequence unbroken.
	rest, err := ext.NextSlices(2)
	if err != nil {
		t.Fatalf("NextSlices: %v", err)
	}
	requireContinues(t, ext, rest[0], 3, 6)
	requireContinues(t, ext, rest[1], 4, 8)
}

// TestPSkipExtenderRejectsOverlappingSource covers a source that comes back with the numbering the
// generated frames already used, which a decoder cannot tell apart from the filler. The access unit
// is refused with a readable error, and the session keeps its state so the caller can retry after
// an IDR.
func TestPSkipExtenderRejectsOverlappingSource(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, _, _ := sessionExt(t, source)
	defer ext.Close()

	slices, err := ext.NextSlices(3) // (1,2), (2,4), (3,6)
	if err != nil {
		t.Fatalf("NextSlices: %v", err)
	}
	stateBefore, pocBefore := ext.State()
	generatedBefore := ext.Generated()

	// A non-IDR picture reusing frame_num 2 and poc 4, both already spent on filler.
	overlap := pSkipObserveNALU(t, slices[1])
	err = ext.ObserveNALU(overlap)
	if !errors.Is(err, ErrNumberingConflict) {
		t.Fatalf("ObserveNALU error = %v, want ErrNumberingConflict", err)
	}
	// The message has to be readable on its own, without the sentinel.
	for _, want := range []string{"frame_num 2", "pic_order_cnt_lsb 4", "generated"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}

	// Nothing moved: no numbering consumed, no state advanced.
	if fn, poc := ext.State(); fn != stateBefore || poc != pocBefore {
		t.Errorf("State = (%d, %d) after the rejection, want (%d, %d)", fn, poc, stateBefore, pocBefore)
	}
	if got := ext.Generated(); got != generatedBefore {
		t.Errorf("Generated = %d after the rejection, want %d", got, generatedBefore)
	}
	// Generation still works and continues from where it was.
	next, err := ext.NextSlice()
	if err != nil {
		t.Fatalf("NextSlice after the rejection: %v", err)
	}
	requireContinues(t, ext, next, 4, 8)

	// An AVCC-wrapped overlap is refused the same way.
	overlapSample := avccSample(pSkipObserveNALU(t, slices[0]))
	if err := ext.ObserveAVCCSample(overlapSample); !errors.Is(err, ErrNumberingConflict) {
		t.Errorf("ObserveAVCCSample error = %v, want ErrNumberingConflict", err)
	}
	// And so is an Annex-B one.
	annexBOverlap := append([]byte{0, 0, 0, 1}, pSkipObserveNALU(t, slices[2])...)
	if err := ext.ObserveAnnexB(annexBOverlap); !errors.Is(err, ErrNumberingConflict) {
		t.Errorf("ObserveAnnexB error = %v, want ErrNumberingConflict", err)
	}
}

// TestPSkipExtenderAcceptsNonOverlappingSource checks the other side of the conflict rule: a
// non-IDR picture that does not reuse generated numbering is accepted, and generation continues
// from it. This is a source that resumed without an IDR but stayed ahead of the filler.
func TestPSkipExtenderAcceptsNonOverlappingSource(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, sps, pps := sessionExt(t, source)
	defer ext.Close()

	if _, err := ext.NextSlices(2); err != nil { // (1,2), (2,4)
		t.Fatalf("NextSlices: %v", err)
	}
	// A real picture numbered (5, 10), beyond the filler, is in step again.
	ahead := pSkipNALU(t, sps, pps, 5, 10)
	if err := ext.ObserveNALU(ahead); err != nil {
		t.Fatalf("ObserveNALU of a non-overlapping slice: %v", err)
	}
	if fn, poc := ext.State(); fn != 5 || poc != 10 {
		t.Fatalf("State = (%d, %d), want (5, 10)", fn, poc)
	}
	next, err := ext.NextSlice()
	if err != nil {
		t.Fatalf("NextSlice: %v", err)
	}
	requireContinues(t, ext, next, 6, 12)
}

// TestPSkipExtenderIDRStartsNewBaseline covers a source that resumes with an IDR. The IDR is always
// accepted, the numbering restarts from it, and the filler that follows uses the new baseline
// rather than continuing the discarded one.
func TestPSkipExtenderIDRStartsNewBaseline(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, sps, pps := sessionExt(t, source)
	defer ext.Close()

	if _, err := ext.NextSlices(4); err != nil { // (1,2) .. (4,8)
		t.Fatalf("NextSlices: %v", err)
	}
	if fn, poc := ext.State(); fn != 4 || poc != 8 {
		t.Fatalf("State after filler = (%d, %d), want (4, 8)", fn, poc)
	}

	// The source resumes with an IDR that reuses (0, 0), the baseline the filler started after.
	if err := ext.ObserveNALU(sourceNALU(t, source, avc.NALU_IDR)); err != nil {
		t.Fatalf("ObserveNALU(IDR) = %v, want nil (an IDR is a new baseline)", err)
	}
	if fn, poc := ext.State(); fn != 0 || poc != 0 {
		t.Fatalf("State after the resuming IDR = (%d, %d), want (0, 0)", fn, poc)
	}

	// Filler after the IDR restarts at (1, 2) and never continues the old session's numbering.
	after, err := ext.NextSlices(2)
	if err != nil {
		t.Fatalf("NextSlices after the IDR: %v", err)
	}
	requireContinues(t, ext, after[0], 1, 2)
	requireContinues(t, ext, after[1], 2, 4)
	// The generated counter, by contrast, counts the whole session: 4 before the IDR, 2 after.
	if got := ext.Generated(); got != 6 {
		t.Errorf("Generated = %d, want 6", got)
	}

	// The parameter sets of the resumed stream are still those the slices use.
	if gotSPS, gotPPS := ext.ParameterSets(); gotSPS.ParameterID != sps.ParameterID ||
		gotPPS.PicParameterSetID != pps.PicParameterSetID {
		t.Errorf("ParameterSets changed across the IDR")
	}

	// An out-of-place non-IDR that overlaps the new run is still refused.
	if err := ext.ObserveNALU(pSkipObserveNALU(t, after[1])); !errors.Is(err, ErrNumberingConflict) {
		t.Errorf("ObserveNALU overlap after IDR error = %v, want ErrNumberingConflict", err)
	}
}

// TestPSkipExtenderIDRAfterOverlapRejectionRecovers covers the documented recovery path: a conflict
// is refused, then the source sends an IDR, and the session works again.
func TestPSkipExtenderIDRAfterOverlapRejectionRecovers(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, _, _ := sessionExt(t, source)
	defer ext.Close()

	slices, err := ext.NextSlices(5)
	if err != nil {
		t.Fatalf("NextSlices: %v", err)
	}
	if err := ext.ObserveNALU(pSkipObserveNALU(t, slices[0])); !errors.Is(err, ErrNumberingConflict) {
		t.Fatalf("ObserveNALU error = %v, want ErrNumberingConflict", err)
	}
	if err := ext.ObserveAnnexB(source); err != nil { // SPS, PPS, IDR
		t.Fatalf("ObserveAnnexB(IDR) after a conflict: %v", err)
	}
	if fn, poc := ext.State(); fn != 0 || poc != 0 {
		t.Fatalf("State after the IDR = (%d, %d), want (0, 0)", fn, poc)
	}
	next, err := ext.NextSlice()
	if err != nil {
		t.Fatalf("NextSlice after recovery: %v", err)
	}
	requireContinues(t, ext, next, 1, 2)
}

// TestPSkipExtenderConflictReportedForFrameNumOrPOCAlone checks each half of the overlap test in
// isolation: a picture reusing only the frame_num, or only the picture order count, is refused too.
func TestPSkipExtenderConflictReportedForFrameNumOrPOCAlone(t *testing.T) {
	source := buildSourceStream(t, false)
	sps, pps := mustParseSPSPPS(t, source)

	newExt := func(t *testing.T) *PSkipExtender {
		t.Helper()
		ext, _, _ := sessionExt(t, source)
		t.Cleanup(func() { ext.Close() })
		return ext
	}

	t.Run("frame_num overlap with a fresh POC", func(t *testing.T) {
		ext := newExt(t)
		if _, err := ext.NextSlices(2); err != nil { // frame_num 1, 2
			t.Fatalf("NextSlices: %v", err)
		}
		// frame_num 1 overlaps; poc 8 is outside the run's 2, 4. Only the frame_num should match.
		err := ext.ObserveNALU(pSkipNALU(t, sps, pps, 1, 8))
		if !errors.Is(err, ErrNumberingConflict) {
			t.Fatalf("ObserveNALU = %v, want ErrNumberingConflict", err)
		}
		if strings.Contains(err.Error(), "pic_order_cnt_lsb 8 falls") {
			t.Errorf("error %q wrongly reports a POC conflict", err)
		}
	})

	t.Run("POC overlap with a fresh frame_num", func(t *testing.T) {
		ext := newExt(t)
		if _, err := ext.NextSlices(2); err != nil { // poc 2, 4
			t.Fatalf("NextSlices: %v", err)
		}
		// frame_num 9 is outside the run's 1, 2; poc 2 is inside.
		err := ext.ObserveNALU(pSkipNALU(t, sps, pps, 9, 2))
		if !errors.Is(err, ErrNumberingConflict) {
			t.Fatalf("ObserveNALU = %v, want ErrNumberingConflict", err)
		}
		if strings.Contains(err.Error(), "frame_num 9 falls") {
			t.Errorf("error %q wrongly reports a frame_num conflict", err)
		}
	})
}

// TestOverlapsStride covers the arithmetic behind the conflict check directly: a run occupies
// strided values, wraps at the modulus, and a run as long as the modulus covers every value.
func TestOverlapsStride(t *testing.T) {
	for _, tc := range []struct {
		name             string
		v, first, stride uint32
		count            uint64
		max              uint32
		want             bool
	}{
		{"first value", 3, 3, 1, 3, 16, true},
		{"last value", 5, 3, 1, 3, 16, true},
		{"past the end", 6, 3, 1, 3, 16, false},
		{"before the start", 2, 3, 1, 3, 16, false},
		{"strided gap", 4, 3, 2, 3, 16, false}, // run 3, 5, 7: 4 is not in it
		{"strided value", 7, 3, 2, 3, 16, true},
		{"wrapped value", 1, 14, 1, 4, 16, true}, // run 14, 15, 0, 1
		{"wrapped past end", 2, 14, 1, 4, 16, false},
		{"stride 2 wrapped", 0, 14, 2, 2, 16, true}, // run 14, 0
		{"full cycle", 9, 1, 1, 16, 16, true},
		{"empty run", 3, 3, 1, 0, 16, false},
		{"zero modulus", 3, 3, 1, 3, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := overlapsStride(tc.v, tc.first, tc.stride, tc.count, tc.max); got != tc.want {
				t.Errorf("overlapsStride(%d, %d, stride %d, count %d, max %d) = %v, want %v",
					tc.v, tc.first, tc.stride, tc.count, tc.max, got, tc.want)
			}
		})
	}
}

// TestPSkipExtenderConflictChecksOnlyFrameNumForPOCType2 covers an SPS that carries no
// pic_order_cnt_lsb in the slice header (pic_order_cnt_type 2). Only frame_num can then be compared,
// and a picture whose frame_num spares the generated run is accepted even though its POC field
// reads 0.
func TestPSkipExtenderConflictChecksOnlyFrameNumForPOCType2(t *testing.T) {
	ps := &paramSets{}
	ps.addSPS(t, spsNALU(0, 0, 2)) // pic_order_cnt_type 2, frame_num and POC modulo 16
	ps.addPPS(t, ppsNALU(0, 0))
	ext, err := NewPSkipExtender(ps.sps[0], ps.pps[0])
	if err != nil {
		t.Fatalf("NewPSkipExtender: %v", err)
	}
	defer ext.Close()
	if err := ext.ObserveAnnexB(ps.annexB()); err != nil {
		t.Fatalf("ObserveAnnexB: %v", err)
	}
	// A reference P slice on (frame_num 4) joins the stream mid-way; POC type 2 has no LSB field.
	if err := ext.ObserveNALU(pSkipNALU(t, ps.sps[0], ps.pps[0], 4, 0)); err != nil {
		t.Fatalf("ObserveNALU: %v", err)
	}
	if fn, _ := ext.State(); fn != 4 {
		t.Fatalf("frame_num = %d, want 4", fn)
	}
	if _, err := ext.NextSlices(2); err != nil { // frame_num 5, 6
		t.Fatalf("NextSlices: %v", err)
	}
	// frame_num 6 overlaps and must be refused.
	if err := ext.ObserveNALU(pSkipNALU(t, ps.sps[0], ps.pps[0], 6, 0)); !errors.Is(err, ErrNumberingConflict) {
		t.Errorf("ObserveNALU(frame_num 6) = %v, want ErrNumberingConflict", err)
	}
	if err := ext.ObserveNALU(pSkipNALU(t, ps.sps[0], ps.pps[0], 7, 0)); err != nil {
		t.Errorf("ObserveNALU(frame_num 7) = %v, want nil", err)
	}
}

// TestPSkipExtenderClose covers session teardown: after Close, observing and generating both fail
// with ErrSessionClosed and produce no bytes, the accessors keep working, and Close is idempotent.
func TestPSkipExtenderClose(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, sps, pps := sessionExt(t, source)

	if _, err := ext.NextSlices(2); err != nil {
		t.Fatalf("NextSlices: %v", err)
	}
	stateBefore, pocBefore := ext.State()
	generatedBefore := ext.Generated()

	if ext.Closed() {
		t.Fatal("Closed before Close")
	}
	if err := ext.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !ext.Closed() {
		t.Fatal("not Closed after Close")
	}
	// Close is idempotent.
	if err := ext.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Every producing method reports ErrSessionClosed and returns no bytes.
	if slice, err := ext.NextSlice(); !errors.Is(err, ErrSessionClosed) || slice != nil {
		t.Errorf("NextSlice after Close = (%v, %v), want (nil, ErrSessionClosed)", slice, err)
	}
	if slices, err := ext.NextSlices(3); !errors.Is(err, ErrSessionClosed) || slices != nil {
		t.Errorf("NextSlices after Close = (%v, %v), want (nil, ErrSessionClosed)", slices, err)
	}
	if err := ext.ObserveNALU(pSkipNALU(t, sps, pps, 3, 6)); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("ObserveNALU after Close = %v, want ErrSessionClosed", err)
	}
	if err := ext.ObserveAVCCSample(avccSample(pSkipNALU(t, sps, pps, 3, 6))); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("ObserveAVCCSample after Close = %v, want ErrSessionClosed", err)
	}
	if err := ext.ObserveAnnexB(source); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("ObserveAnnexB after Close = %v, want ErrSessionClosed", err)
	}

	// The state did not move, and the read-only accessors still report it.
	if fn, poc := ext.State(); fn != stateBefore || poc != pocBefore {
		t.Errorf("State after Close = (%d, %d), want (%d, %d)", fn, poc, stateBefore, pocBefore)
	}
	if got := ext.Generated(); got != generatedBefore {
		t.Errorf("Generated after Close = %d, want %d", got, generatedBefore)
	}
	if !ext.Ready() {
		t.Error("Ready after Close = false; the session state should still be readable")
	}
	if gotSPS, gotPPS := ext.ParameterSets(); gotSPS.ParameterID != sps.ParameterID ||
		gotPPS.PicParameterSetID != pps.PicParameterSetID {
		t.Error("ParameterSets changed after Close")
	}
}

// TestPSkipExtenderCloseBeforeObserve covers Close on a session that never observed anything: it
// ends cleanly and keeps reporting not-ready rather than panicking.
func TestPSkipExtenderCloseBeforeObserve(t *testing.T) {
	sps, pps := mustParseSPSPPS(t, buildSourceStream(t, false))
	ext, err := NewPSkipExtender(sps, pps)
	if err != nil {
		t.Fatalf("NewPSkipExtender: %v", err)
	}
	if err := ext.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ext.NextSlice(); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("NextSlice after Close = %v, want ErrSessionClosed", err)
	}
	if ext.Ready() {
		t.Error("Ready on a closed, never-observed session")
	}
}

// TestPSkipExtenderFailedBatchConsumesNoNumbering checks that a batch which cannot be generated
// leaves the numbering where it was, so the next attempt repeats it rather than skipping values.
func TestPSkipExtenderFailedBatchConsumesNoNumbering(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, _, _ := sessionExt(t, source)
	defer ext.Close()

	if _, err := ext.NextSlices(2); err != nil {
		t.Fatalf("NextSlices: %v", err)
	}
	stateBefore, pocBefore := ext.State()
	generatedBefore := ext.Generated()

	// Make the SPS in use one that P_Skip slices cannot be written for, so generation fails.
	ext.mu.Lock()
	goodSPS := ext.sps
	badSPS := *goodSPS
	badSPS.PicOrderCntType = 1
	ext.sps = &badSPS
	ext.mu.Unlock()

	if _, err := ext.NextSlices(3); err == nil || !strings.Contains(err.Error(), "pic_order_cnt_type=1") {
		t.Fatalf("NextSlices with a POC type 1 SPS = %v, want an error about pic_order_cnt_type=1", err)
	}
	if fn, poc := ext.State(); fn != stateBefore || poc != pocBefore {
		t.Errorf("State = (%d, %d) after a failed batch, want (%d, %d)", fn, poc, stateBefore, pocBefore)
	}
	if got := ext.Generated(); got != generatedBefore {
		t.Errorf("Generated = %d after a failed batch, want %d", got, generatedBefore)
	}

	ext.mu.Lock()
	ext.sps = goodSPS
	ext.mu.Unlock()
	slices, err := ext.NextSlices(3)
	if err != nil {
		t.Fatalf("NextSlices after restoring the SPS: %v", err)
	}
	// The batch restarts from where the successful one ended, so no value is skipped or repeated.
	requireContinues(t, ext, slices[0], 3, 6)
	requireContinues(t, ext, slices[1], 4, 8)
	requireContinues(t, ext, slices[2], 5, 10)
}

// TestPSkipExtenderConcurrentUse shares one session between an observer and a generator while a
// third goroutine closes it. Every method takes the session lock, so there must be no data race
// (run with -race) and no result that mixes numbering from two calls: whatever a batch returns is
// a run of consecutive identifiers, and the session's counters equal what was handed out.
func TestPSkipExtenderConcurrentUse(t *testing.T) {
	source := buildSourceStream(t, false)
	ext, _, _ := sessionExt(t, source)

	// Observed pictures are placed far beyond any filler this test can generate relative to the
	// current state, so observing them never conflicts. The IDR is always accepted.
	idr := sourceNALU(t, source, avc.NALU_IDR)

	const rounds = 200
	var (
		wg        sync.WaitGroup
		stop      atomic.Bool
		generated atomic.Uint64
		observed  atomic.Uint64
	)
	// Each worker closes its own channel after its first successful operation, so the closer can
	// wait for both to have used the session. One shared channel would let two signals from the
	// same worker satisfy the closer before the other ever ran.
	generatedOnce := make(chan struct{})
	observedOnce := make(chan struct{})

	// Generator.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var once sync.Once
		for range rounds {
			if stop.Load() {
				return
			}
			slices, err := ext.NextSlices(2)
			if errors.Is(err, ErrSessionClosed) {
				return
			}
			if err != nil {
				t.Errorf("NextSlices: %v", err)
				return
			}
			for i := 1; i < len(slices); i++ {
				// A batch must be internally consistent: consecutive identifiers.
				prev := sliceHeaderOf(t, ext, slices[i-1])
				cur := sliceHeaderOf(t, ext, slices[i])
				if cur.FrameNum != (prev.FrameNum+1)%16 || cur.PicOrderCntLsb != (prev.PicOrderCntLsb+2)%16 {
					t.Errorf("batch step from (%d, %d) to (%d, %d) is not consecutive",
						prev.FrameNum, prev.PicOrderCntLsb, cur.FrameNum, cur.PicOrderCntLsb)
					return
				}
			}
			generated.Add(uint64(len(slices)))
			once.Do(func() { close(generatedOnce) })
		}
	}()

	// Observer: feed the IDR repeatedly, which is always accepted and, after any filler, starts a
	// new baseline. This exercises the observe path against a session that is being generated.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var once sync.Once
		for range rounds {
			if stop.Load() {
				return
			}
			err := ext.ObserveNALU(idr)
			if errors.Is(err, ErrSessionClosed) {
				return
			}
			if err != nil {
				t.Errorf("ObserveNALU: %v", err)
				return
			}
			observed.Add(1)
			once.Do(func() { close(observedOnce) })
		}
	}()

	// Closer: let both workers complete at least one operation, then end the session.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, ready := range []<-chan struct{}{generatedOnce, observedOnce} {
			select {
			case <-ready:
			case <-time.After(10 * time.Second):
				t.Error("workers did not use the session before the close deadline")
				stop.Store(true)
				_ = ext.Close()
				return
			}
		}
		stop.Store(true)
		if err := ext.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	wg.Wait()

	if !ext.Closed() {
		t.Fatal("the session was not closed")
	}
	// Whatever happened before the end, the counters agree and the session is unusable afterwards.
	if got := ext.Generated(); got != generated.Load() {
		t.Errorf("Generated = %d, want %d", got, generated.Load())
	}
	if observed.Load() == 0 {
		t.Error("the observer never saw an access unit through")
	}
	if _, err := ext.NextSlice(); !errors.Is(err, ErrSessionClosed) {
		t.Errorf("NextSlice after the concurrent run = %v, want ErrSessionClosed", err)
	}
}
