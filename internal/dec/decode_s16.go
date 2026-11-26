// SPDX-License-Identifier: LGPL-2.1-or-later

package dec

// frameSamples is the per-channel time-domain sample count of one AAC-LC
// frame (1024). Every reconstructed frame emits exactly this many
// inter-channel samples.
const frameSamples = 1024

// AppendS16 decodes one access unit and appends the frame's reconstructed
// interleaved little-endian signed 16-bit PCM to dst, returning the extended
// slice and the number of inter-channel samples emitted (frameSamples on
// success). It runs the full DecodeFrame parse followed by the internal
// reconstruction pass, so the pcm package reaches PCM without touching the
// unexported reconstruct.
//
// The S16 samples are the decoder's native S32P output arithmetic-shifted
// right by 16. That conversion is proven byte-identical to
// "ffmpeg -bitexact -c:a aac_fixed -f s16le" (plan-d2 section 6): swresample
// under BITEXACT is a plain truncating >>16 with no dither, and clip_output
// already baked in the round-to-nearest bias.
//
// On a decode error dst is returned unmodified with a zero sample count and
// the wrapped error; the caller (pcm.Decoder) decides whether the error ends
// the stream or is skipped. The decoder's cross-frame state is then exactly as
// it was before the call, configuration included: the whole decode, header
// parse and reconstruction, runs as one transaction (beginFrame captured it,
// restoreFrame puts it back), so a skipped unit leaves no trace in the next
// frame's output.
//
// A unit that parses cleanly but carries no audio element is not an error at
// this level: it reconstructs nothing and appends nothing, matching the
// reconstructed frame count the caller expects. A caller that requires audio
// from every unit (pcm.FrameDecoder) detects the empty result and rolls the
// whole transaction back via RollbackUnit, so the accepted-but-empty unit
// leaves no more state than a failed one.
func (d *Decoder) AppendS16(dst, pkt []byte) (out []byte, samples int, err error) {
	d.beginFrame()
	if err := d.decodePacket(pkt); err != nil {
		d.restoreFrame()
		return dst, 0, err
	}
	d.reconstruct(nil)
	return d.appendElemsS16(dst), frameSamples, nil
}

// RollbackUnit undoes the state of the most recent AppendS16 call, whether it
// succeeded or failed. It exists for a caller that rejects a unit the decoder
// itself accepted (pcm.FrameDecoder rejects an access unit that carries no
// audio element): rolling the whole unit back makes that rejection consume no
// state, exactly as a failed parse does. It is a no-op before any AppendS16
// call. Because AppendS16 reconstructs only frames that carry audio, a
// rollback of an empty unit cannot have advanced the PNS generator or the
// overlap, but restoreFrame restores those fields regardless, so the rollback
// is correct for any unit.
func (d *Decoder) RollbackUnit() {
	d.restoreFrame()
}

// Channels reports the decoded channel count (1 or 2 for AAC-LC mono/stereo).
// It is valid once the decoder is configured (always for NewRaw; after the
// first DecodeFrame for NewADTS) and returns 0 before that.
func (d *Decoder) Channels() int {
	if !d.configured {
		return 0
	}
	return d.cfg.ChanConfig
}

// appendElemsS16 appends the interleaved S16 PCM of the frame's decoded audio
// elements (in bitstream order, from Elems) to dst. For an AAC-LC mono stream
// Elems holds one SCE (one channel); for a stereo stream one CPE (L then R
// interleaved per sample). reconstruct has already written each channel's
// S32P samples into SCE.Output; only DSE/FIL are absent from Elems, so every
// listed element is audio.
func (d *Decoder) appendElemsS16(dst []byte) []byte {
	for i := range d.Elems {
		e := &d.Elems[i]
		if e.Type == TypeCPE {
			dst = appendInterleavedS16(dst,
				e.CPE.Ch[0].Output[:frameSamples],
				e.CPE.Ch[1].Output[:frameSamples])
		} else {
			dst = appendMonoS16(dst, e.CPE.Ch[0].Output[:frameSamples])
		}
	}
	return dst
}

// growS16 extends dst by n S16 samples (2*n bytes), reusing capacity once the
// backing buffer is warmed up. It returns the grown slice and the write index
// where the new samples begin.
func growS16(dst []byte, n int) (out []byte, idx int) {
	oldLen := len(dst)
	newLen := oldLen + 2*n
	if cap(dst) < newLen {
		grown := make([]byte, newLen)
		copy(grown, dst)
		return grown, oldLen
	}
	return dst[:newLen], oldLen
}

// appendMonoS16 appends one channel of S32P output as little-endian S16.
func appendMonoS16(dst []byte, ch []int32) []byte {
	dst, idx := growS16(dst, len(ch))
	for _, v := range ch {
		s := uint16(int16(v >> 16))
		dst[idx] = byte(s)
		dst[idx+1] = byte(s >> 8)
		idx += 2
	}
	return dst
}

// appendInterleavedS16 appends two equal-length channels (L, R) of S32P
// output as interleaved little-endian S16: L0,R0,L1,R1,... matching the
// oracle's s16le channel order.
func appendInterleavedS16(dst []byte, l, r []int32) []byte {
	n := min(len(l), len(r))
	dst, idx := growS16(dst, 2*n)
	for i := range n {
		sl := uint16(int16(l[i] >> 16))
		dst[idx] = byte(sl)
		dst[idx+1] = byte(sl >> 8)
		sr := uint16(int16(r[i] >> 16))
		dst[idx+2] = byte(sr)
		dst[idx+3] = byte(sr >> 8)
		idx += 4
	}
	return dst
}
