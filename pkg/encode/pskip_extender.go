package encode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/Eyevinn/mp4ff/avc"
)

// ErrSessionClosed is returned by a PSkipExtender whose Close has been called. Observing and
// generating both fail with it, so a shutdown never produces stray bytes.
var ErrSessionClosed = errors.New("PSkipExtender: session closed")

// ErrNumberingConflict is returned when an observed access unit carries a frame_num or
// pic_order_cnt_lsb that the session's generated frames have already used. A decoder cannot tell
// such a picture from the filler it already received, so the access unit is rejected rather than
// silently accepted. Only an IDR starts a new baseline and lifts the rejection.
var ErrNumberingConflict = errors.New("PSkipExtender: access unit numbering overlaps generated frames")

// PSkipExtender continues an existing H.264 stream with P_Skip frames, tracking the frame
// numbering as access units go past instead of re-parsing the stream. Where AppendPSkipFrames
// rewrites a complete Annex-B bitstream, a PSkipExtender can be asked for continuation frames at
// any point, which is what a live pipeline needs when its source stalls: a handful of bytes per
// frame holds the last picture on screen until real video returns, so the receiver never sees the
// stream stop.
//
//	ext, err := encode.NewPSkipExtenderFromDecConfRec(avcC)
//	defer ext.Close()
//	for _, au := range accessUnits { // every real access unit, as it is sent
//	    err = ext.ObserveAVCCSample(au)
//	}
//	filler, err := ext.NextSlices(25) // one second at 25 fps, continuing the stream
//
// The source's SPS and PPS are reused verbatim, so no parameter set is re-signalled and a decoder
// sees no configuration change. Generated slices use the PPS of the most recently observed slice and
// the SPS that PPS refers to, so a stream with several parameter sets continues with the ones its
// last picture used.
//
// # Session lifetime
//
// Close ends the session. After it, NextSlice, NextSlices and the Observe methods return
// ErrSessionClosed and produce no bytes; Close itself may be called repeatedly and reports no
// error. The read-only accessors keep reporting the final state.
//
// # Concurrency
//
// A session may be shared between an ingest goroutine and a send goroutine: every method takes the
// same lock, so observing, generating and closing never interleave and no partially updated
// numbering is ever visible. A generation request that fails consumes no numbering, and NextSlices
// is all-or-nothing: either every requested frame is produced or none is, leaving the tracked
// numbering where it was.
//
// # Resuming
//
// Real video must resume at an IDR. The generated frames advance frame_num and the picture order
// count past what the source encoder will emit next, and only an IDR resets both. An IDR observed
// after filler is accepted as a new baseline, so the frames generated after it start from that
// IDR's numbering rather than continuing the discarded session's.
//
// A non-IDR access unit observed while generated frames are outstanding is checked against the
// numbering those frames occupy. A frame_num or pic_order_cnt_lsb that falls in that range is
// reported as ErrNumberingConflict and leaves the session unchanged, since replaying such a
// picture would duplicate identifiers a decoder has already seen. A source that resumes in place,
// without an IDR, therefore has to be restarted through a new session.
//
// Timing stays with the caller: the first generated frame must be placed after the highest
// presentation time the source reached, not merely after its last decode time. With B frames those
// differ by the reorder delay, and a frame placed between them is displayed out of order.
//
// Supported streams are progressive, with pic_order_cnt_type 0 or 2, in CAVLC or CABAC.
type PSkipExtender struct {
	// mu guards every field below, so an ingest goroutine, a send goroutine and a closer can share
	// one session. It is taken by all methods, including the read-only accessors.
	mu sync.Mutex

	// sps and pps are the parameter sets of the most recently observed slice, or those given to
	// the constructor until a slice is observed.
	sps *avc.SPS
	pps *avc.PPS

	spsMap map[uint32]*avc.SPS
	ppsMap map[uint32]*avc.PPS

	sh avc.SliceHeader // reused for every observed slice

	have      bool
	frameNum  uint32 // frame_num of the most recent reference picture
	pocLsb    uint32 // highest pic_order_cnt_lsb seen since the last IDR
	generated uint64 // frames produced over the whole session, including before an IDR reset

	// genRun describes the frames generated since the last observed slice. genRunCount is their
	// number, and genRunFrameNum/genRunPOCLsb are the numbering of the first of them; the rest
	// follow at stride one for frame_num and two for the picture order count, modulo the SPS
	// widths saved with the run. An observed non-IDR access unit that reuses any of those values
	// is a conflict. The run starts when the first frame is generated after an observed slice and
	// ends when the next slice is observed, so every frame in it uses one SPS.
	genRunCount    uint32
	genRunFrameNum uint32
	genRunPOCLsb   uint32
	genRunMaxFrame uint32
	genRunMaxPOC   uint32
	// genRunHasPOC is false when the run's SPS uses pic_order_cnt_type 2, which carries no
	// pic_order_cnt_lsb in the slice header. Only frame_num can then be compared.
	genRunHasPOC bool

	closed bool
}

// pskipNumbering is the mutable numbering of a PSkipExtender, captured so that a failed
// NextSlices can restore it in full.
type pskipNumbering struct {
	frameNum, pocLsb             uint32
	genRunFrameNum, genRunPOCLsb uint32
	genRunMaxFrame, genRunMaxPOC uint32
	genRunCount                  uint32
	generated                    uint64
	genRunHasPOC                 bool
}

// NewPSkipExtender returns a PSkipExtender for a stream that starts with the given parameter sets,
// such as those of an MP4 sample entry; pps must refer to sps. Slices are generated with the
// parameter sets of the most recently observed slice, which may be others that the stream carries.
func NewPSkipExtender(sps *avc.SPS, pps *avc.PPS) (*PSkipExtender, error) {
	if sps == nil || pps == nil {
		return nil, fmt.Errorf("NewPSkipExtender: sps and pps are required")
	}
	if pps.SeqParameterSetID != sps.ParameterID {
		return nil, fmt.Errorf("NewPSkipExtender: PPS %d refers to SPS %d, not %d",
			pps.PicParameterSetID, pps.SeqParameterSetID, sps.ParameterID)
	}
	if err := checkSPS(sps); err != nil {
		return nil, fmt.Errorf("NewPSkipExtender: %w", err)
	}
	e := &PSkipExtender{
		sps:    sps,
		pps:    pps,
		spsMap: map[uint32]*avc.SPS{sps.ParameterID: sps},
		ppsMap: map[uint32]*avc.PPS{pps.PicParameterSetID: pps},
	}
	return e, nil
}

// NewPSkipExtenderFromDecConfRec returns a PSkipExtender built from an
// AVCDecoderConfigurationRecord, the form fragmented MP4 and RTMP carry parameter sets in. All of
// its parameter sets are kept, and the extender starts with the first PPS and the SPS it refers to.
func NewPSkipExtenderFromDecConfRec(avcC []byte) (*PSkipExtender, error) {
	rec, err := avc.DecodeAVCDecConfRec(avcC)
	if err != nil {
		return nil, fmt.Errorf("NewPSkipExtenderFromDecConfRec: %w", err)
	}
	if len(rec.SPSnalus) == 0 || len(rec.PPSnalus) == 0 {
		return nil, fmt.Errorf("NewPSkipExtenderFromDecConfRec: record has no SPS or PPS")
	}
	spsMap := make(map[uint32]*avc.SPS, len(rec.SPSnalus))
	for _, nalu := range rec.SPSnalus {
		sps, err := avc.ParseSPSNALUnit(nalu, true)
		if err != nil {
			return nil, fmt.Errorf("NewPSkipExtenderFromDecConfRec: parse SPS: %w", err)
		}
		spsMap[sps.ParameterID] = sps
	}
	ppss := make([]*avc.PPS, 0, len(rec.PPSnalus))
	for _, nalu := range rec.PPSnalus {
		pps, err := avc.ParsePPSNALUnit(nalu, spsMap)
		if err != nil {
			return nil, fmt.Errorf("NewPSkipExtenderFromDecConfRec: parse PPS: %w", err)
		}
		ppss = append(ppss, pps)
	}
	sps, ok := spsMap[ppss[0].SeqParameterSetID]
	if !ok {
		return nil, fmt.Errorf("NewPSkipExtenderFromDecConfRec: PPS %d refers to SPS %d, which the record lacks",
			ppss[0].PicParameterSetID, ppss[0].SeqParameterSetID)
	}
	e, err := NewPSkipExtender(sps, ppss[0])
	if err != nil {
		return nil, err
	}
	// The other parameter sets are kept so that slices referencing them parse.
	maps.Copy(e.spsMap, spsMap)
	for _, pps := range ppss[1:] {
		e.ppsMap[pps.PicParameterSetID] = pps
	}
	return e, nil
}

// checkSPS returns why slices cannot be generated for sps, or nil if they can.
func checkSPS(sps *avc.SPS) error {
	if sps.PicOrderCntType != 0 && sps.PicOrderCntType != 2 {
		return fmt.Errorf("pic_order_cnt_type=%d not supported (only types 0 and 2)", sps.PicOrderCntType)
	}
	if !sps.FrameMbsOnlyFlag {
		return fmt.Errorf("interlaced not supported (frame_mbs_only_flag=0)")
	}
	return nil
}

// Close ends the session. Afterwards NextSlice, NextSlices and the Observe methods return
// ErrSessionClosed without producing bytes; the read-only accessors keep working. Close is
// idempotent and always reports success, so it may be deferred and called again on shutdown paths.
func (e *PSkipExtender) Close() error {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	return nil
}

// Closed reports whether Close has been called.
func (e *PSkipExtender) Closed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

// Ready reports whether a coded slice has been observed, which is required before frames can be
// generated.
func (e *PSkipExtender) Ready() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.have
}

// State returns the frame_num of the most recent reference picture and the highest
// pic_order_cnt_lsb seen since the last IDR. The next generated frame continues from these.
func (e *PSkipExtender) State() (frameNum, picOrderCntLsb uint32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.frameNum, e.pocLsb
}

// Generated returns how many frames the PSkipExtender has produced over the whole session.
func (e *PSkipExtender) Generated() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.generated
}

// ParameterSets returns the SPS and PPS that generated slices use: those of the most recently
// observed slice, or those the PSkipExtender was created with until a slice is observed. A caller
// that encodes its own pictures into the stream, such as an IDR to splice in, uses these.
func (e *PSkipExtender) ParameterSets() (*avc.SPS, *avc.PPS) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sps, e.pps
}

// maxFrameNum returns MaxFrameNum of the current SPS. The caller holds the lock.
func (e *PSkipExtender) maxFrameNum() uint32 { return 1 << (e.sps.Log2MaxFrameNumMinus4 + 4) }

// maxPOCLsb returns MaxPicOrderCntLsb of the current SPS. The caller holds the lock.
func (e *PSkipExtender) maxPOCLsb() uint32 { return 1 << (e.sps.Log2MaxPicOrderCntLsbMinus4 + 4) }

// ObserveNALU tracks one NAL unit, given without a start code or length prefix. Parameter sets are
// remembered so that later slices parse; coded slices update the numbering. Anything else is
// ignored, so a whole access unit can be passed through NAL unit by NAL unit. A NAL unit that does
// not parse, such as a truncated slice, returns an error and leaves the numbering unchanged, so the
// caller can drop a damaged access unit and keep observing.
//
// A non-IDR slice whose numbering overlaps frames this session has already generated returns an
// error wrapping ErrNumberingConflict and also leaves the session unchanged. After Close it returns
// ErrSessionClosed.
func (e *PSkipExtender) ObserveNALU(nalu []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrSessionClosed
	}
	return e.observeNALU(nalu)
}

// ObserveAVCCSample tracks one access unit in AVCC form, with 4-byte length prefixes, as carried by
// fragmented MP4 and by FLV or RTMP video messages. A sample whose length prefixes run past its end
// returns an error before any of its NAL units is observed.
func (e *PSkipExtender) ObserveAVCCSample(sample []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrSessionClosed
	}
	if err := checkAVCCLengths(sample); err != nil {
		return fmt.Errorf("PSkipExtender: split access unit: %w", err)
	}
	for pos := 0; pos+4 < len(sample); {
		n := int(binary.BigEndian.Uint32(sample[pos:]))
		pos += 4
		if err := e.observeNALU(sample[pos : pos+n]); err != nil {
			return err
		}
		pos += n
	}
	return nil
}

// checkAVCCLengths checks that the 4-byte length prefixes of an AVCC sample stay within it. As in
// avc.GetNalusFromSample, up to four bytes after the last NAL unit are ignored.
func checkAVCCLengths(sample []byte) error {
	if len(sample) < 4 {
		return fmt.Errorf("%d bytes, shorter than a length prefix", len(sample))
	}
	for pos := 0; pos+4 < len(sample); {
		n := uint64(binary.BigEndian.Uint32(sample[pos:]))
		if uint64(pos+4)+n > uint64(len(sample)) {
			return fmt.Errorf("NAL unit length %d at byte %d runs past the end of the %d-byte sample",
				n, pos, len(sample))
		}
		pos += 4 + int(n)
	}
	return nil
}

// ObserveAnnexB tracks every NAL unit of an Annex-B byte stream.
func (e *PSkipExtender) ObserveAnnexB(stream []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrSessionClosed
	}
	for _, nalu := range avc.ExtractNalusFromByteStream(stream) {
		if err := e.observeNALU(nalu); err != nil {
			return err
		}
	}
	return nil
}

// observeNALU tracks one NAL unit. The caller holds the lock and has checked that the session is
// open.
func (e *PSkipExtender) observeNALU(nalu []byte) error {
	if len(nalu) == 0 {
		return nil
	}
	switch avc.GetNaluType(nalu[0]) {
	case avc.NALU_SPS:
		sps, err := avc.ParseSPSNALUnit(nalu, true)
		if err != nil {
			return fmt.Errorf("PSkipExtender: parse SPS: %w", err)
		}
		e.spsMap[sps.ParameterID] = sps
	case avc.NALU_PPS:
		pps, err := avc.ParsePPSNALUnit(nalu, e.spsMap)
		if err != nil {
			return fmt.Errorf("PSkipExtender: parse PPS: %w", err)
		}
		e.ppsMap[pps.PicParameterSetID] = pps
	case avc.NALU_IDR, avc.NALU_NON_IDR:
		if err := avc.ParseSliceHeaderInto(&e.sh, nalu, e.spsMap, e.ppsMap); err != nil {
			return fmt.Errorf("PSkipExtender: parse slice header: %w", err)
		}
		// Continue with the parameter sets this slice uses. A picture may use any PPS that refers
		// to the sequence's SPS, and an IDR may switch to another SPS. The parse found both. Keep
		// the previous pair so that a rejected access unit leaves the session untouched.
		prevSPS, prevPPS := e.sps, e.pps
		e.pps = e.ppsMap[e.sh.PicParamID]
		e.sps = e.spsMap[e.sh.SeqParamID]
		if err := e.observeSlice(&e.sh, avc.GetNaluType(nalu[0]) == avc.NALU_IDR, nalu[0]&0x60 != 0); err != nil {
			e.sps, e.pps = prevSPS, prevPPS
			return err
		}
	}
	return nil
}

// observeSlice folds one coded slice into the tracked numbering.
//
// frame_num identifies the most recent reference picture, so a non-reference picture must not
// advance it. The picture order count continues from the highest value seen rather than from the
// most recent picture, because in a stream with B frames the last picture in decode order is not
// the last in display order. An IDR resets both, and discards anything seen before it.
//
// A non-IDR slice that reuses numbering already spent on generated frames is rejected with an
// error wrapping ErrNumberingConflict, leaving the numbering unchanged. An IDR is always accepted:
// it is a new baseline, and frames generated after it start from its numbering.
//
// The caller holds the lock.
func (e *PSkipExtender) observeSlice(sh *avc.SliceHeader, isIDR, isReference bool) error {
	if !isIDR && e.genRunCount > 0 {
		if err := e.checkOverlap(sh); err != nil {
			return err
		}
	}
	switch {
	case isIDR:
		e.frameNum = sh.FrameNum
		e.pocLsb = sh.PicOrderCntLsb
	case !e.have:
		e.frameNum = sh.FrameNum
		e.pocLsb = sh.PicOrderCntLsb
	default:
		if isReference {
			e.frameNum = sh.FrameNum
		}
		if pocIsLater(sh.PicOrderCntLsb, e.pocLsb, e.maxPOCLsb()) {
			e.pocLsb = sh.PicOrderCntLsb
		}
	}
	e.have = true
	// The source is in step again: the next generated frame starts a fresh run that a later
	// observed access unit is checked against.
	e.genRunCount = 0
	return nil
}

// checkOverlap reports an error wrapping ErrNumberingConflict when sh reuses a frame_num or
// pic_order_cnt_lsb that the frames generated since the last observed slice occupy. The caller
// holds the lock and checks that genRunCount > 0.
func (e *PSkipExtender) checkOverlap(sh *avc.SliceHeader) error {
	var conflicts []string

	maxFrameNum := e.genRunMaxFrame
	if overlapsStride(sh.FrameNum, e.genRunFrameNum, 1, uint64(e.genRunCount), maxFrameNum) {
		last := (e.genRunFrameNum + e.genRunCount - 1) % maxFrameNum
		conflicts = append(conflicts, fmt.Sprintf(
			"frame_num %d falls among the %d generated frame(s) numbered %d..%d (mod %d)",
			sh.FrameNum, e.genRunCount, e.genRunFrameNum, last, maxFrameNum))
	}

	maxPOC := e.genRunMaxPOC
	if e.genRunHasPOC && overlapsStride(sh.PicOrderCntLsb, e.genRunPOCLsb, 2, uint64(e.genRunCount), maxPOC) {
		last := (e.genRunPOCLsb + 2*(e.genRunCount-1)) % maxPOC
		conflicts = append(conflicts, fmt.Sprintf(
			"pic_order_cnt_lsb %d falls among the %d generated frame(s) counted %d..%d (step 2, mod %d)",
			sh.PicOrderCntLsb, e.genRunCount, e.genRunPOCLsb, last, maxPOC))
	}

	if len(conflicts) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s; resume with an IDR or start a new session",
		ErrNumberingConflict, strings.Join(conflicts, "; "))
}

// overlapsStride reports whether v equals first + k*stride (mod max) for some k in [0, count). The
// values a run occupies are strided rather than contiguous, since frame_num advances by one and the
// picture order count by two per generated frame. A run that covers the whole modulus is matched
// for every v.
func overlapsStride(v, first, stride uint32, count uint64, max uint32) bool {
	if max == 0 || count == 0 {
		return false
	}
	if count >= uint64(max) {
		return true
	}
	d := (uint64(v) + uint64(max) - uint64(first)) % uint64(max)
	if d%uint64(stride) != 0 {
		return false
	}
	return d/uint64(stride) < count
}

// NextSlice generates one P_Skip frame continuing the stream. It is a reference picture, so the
// numbering advances and successive calls continue where the last one left off. On failure, no
// numbering is consumed. After Close it returns ErrSessionClosed.
func (e *PSkipExtender) NextSlice() ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nextSlice()
}

// NextSlices generates count P_Skip frames in order, continuing from the numbering the previous
// call left. It is all-or-nothing: if any frame cannot be generated, the session's numbering is
// restored and no slices are returned. After Close it returns ErrSessionClosed.
func (e *PSkipExtender) NextSlices(count int) ([][]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrSessionClosed
	}
	if count <= 0 {
		return nil, nil
	}
	saved := e.numbering()
	out := make([][]byte, 0, count)
	for range count {
		slice, err := e.nextSlice()
		if err != nil {
			e.restoreNumbering(saved)
			return nil, err
		}
		out = append(out, slice)
	}
	return out, nil
}

// nextSlice generates one frame. The caller holds the lock. The numbering is committed only once
// the slice has been encoded, so a failure leaves the session exactly as it was.
func (e *PSkipExtender) nextSlice() ([]byte, error) {
	if e.closed {
		return nil, ErrSessionClosed
	}
	if !e.have {
		return nil, fmt.Errorf("PSkipExtender: no coded slice observed yet")
	}
	if err := checkSPS(e.sps); err != nil {
		return nil, fmt.Errorf("PSkipExtender: %w", err)
	}
	frameNum := (e.frameNum + 1) % e.maxFrameNum()
	pocLsb := (e.pocLsb + 2) % e.maxPOCLsb()
	slice, err := EncodePSkipSlice(e.sps, e.pps, frameNum, pocLsb, 0)
	if err != nil {
		return nil, fmt.Errorf("PSkipExtender: %w", err)
	}
	e.advance(frameNum, pocLsb)
	return slice, nil
}

// advance commits the numbering of a freshly generated frame. The caller holds the lock.
func (e *PSkipExtender) advance(frameNum, pocLsb uint32) {
	if e.genRunCount == 0 {
		e.genRunFrameNum = frameNum
		e.genRunPOCLsb = pocLsb
		e.genRunMaxFrame = e.maxFrameNum()
		e.genRunMaxPOC = e.maxPOCLsb()
		e.genRunHasPOC = e.sps.PicOrderCntType == 0
	}
	e.frameNum = frameNum
	e.pocLsb = pocLsb
	e.genRunCount++
	e.generated++
}

// numbering captures the mutable numbering. The caller holds the lock.
func (e *PSkipExtender) numbering() pskipNumbering {
	return pskipNumbering{
		frameNum:       e.frameNum,
		pocLsb:         e.pocLsb,
		genRunFrameNum: e.genRunFrameNum,
		genRunPOCLsb:   e.genRunPOCLsb,
		genRunMaxFrame: e.genRunMaxFrame,
		genRunMaxPOC:   e.genRunMaxPOC,
		genRunCount:    e.genRunCount,
		generated:      e.generated,
		genRunHasPOC:   e.genRunHasPOC,
	}
}

// restoreNumbering puts back a captured numbering. The caller holds the lock.
func (e *PSkipExtender) restoreNumbering(n pskipNumbering) {
	e.frameNum = n.frameNum
	e.pocLsb = n.pocLsb
	e.genRunFrameNum = n.genRunFrameNum
	e.genRunPOCLsb = n.genRunPOCLsb
	e.genRunMaxFrame = n.genRunMaxFrame
	e.genRunMaxPOC = n.genRunMaxPOC
	e.genRunCount = n.genRunCount
	e.genRunHasPOC = n.genRunHasPOC
	e.generated = n.generated
}

// pocIsLater reports whether a comes after b in a modulo-max picture order count sequence.
func pocIsLater(a, b, max uint32) bool {
	if max == 0 {
		return false
	}
	half := int64(max / 2)
	d := int64(a) - int64(b)
	switch {
	case d > half:
		d -= int64(max)
	case d < -half:
		d += int64(max)
	}
	return d > 0
}
