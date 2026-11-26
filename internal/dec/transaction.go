// SPDX-License-Identifier: LGPL-2.1-or-later

package dec

// beginFrame captures the decoder's cross-frame state before decoding one
// access unit, so a unit that fails can be rolled back as one transaction by
// restoreFrame. The state captured is exactly what a parse or a reconstruction
// writes and a later frame reads (see frameSnapshot); the copy targets a value
// the Decoder owns, so it allocates nothing.
func (d *Decoder) beginFrame() {
	s := &d.snapshot
	s.cfg = d.cfg
	s.adts = d.adts
	s.configured = d.configured
	s.randomState = d.randomState

	s.nelems = min(len(d.Elems), maxFrameElems)
	for i := range s.nelems {
		s.elems[i] = d.Elems[i]
	}

	// Snapshot the element grid verbatim, so a configure the unit latches is
	// undone by putting the grid back, and the per-field state of every live
	// element.
	s.cheGrid = d.che

	s.nelem = 0
	for t := range d.che {
		for id, che := range d.che[t] {
			if che == nil || s.nelem == maxLiveElems {
				continue
			}
			slot := &s.slots[s.nelem]
			s.nelem++
			slot.typ, slot.id = t, id
			slot.nch = 1
			if t == TypeCPE {
				slot.nch = 2
			}
			slot.present = che.present
			slot.commonWindow = che.commonWindow
			slot.msPresent = che.msPresent
			slot.MaxSFBSte = che.MaxSFBSte
			slot.msMask = che.MSMask
			for c := range slot.nch {
				sn := &slot.ch[c]
				sn.ics = che.Ch[c].ICS
				sn.tns = che.Ch[c].TNS
				sn.pulse = che.Ch[c].Pulse
				sn.pulsePres = che.Ch[c].PulsePresent
				sn.bandType = che.Ch[c].BandType
				sn.sfo = che.Ch[c].SFO
				sn.sf = che.Ch[c].SF
				copy(sn.saved[:], che.Ch[c].Saved[:overlapLen])
			}
		}
	}
}

// restoreFrame puts the decoder back exactly as beginFrame found it. It runs
// only on a decode error: a successful unit's writes are the frame's output
// and must survive. The restoration is unconditional (it writes rather than
// comparing) because a failed unit may have dirtied any subset of these
// fields, and writing the captured value is the only cheap way to be sure.
func (d *Decoder) restoreFrame() {
	s := &d.snapshot
	d.cfg = s.cfg
	d.adts = s.adts
	d.configured = s.configured
	d.randomState = s.randomState

	// Restore the grid verbatim, undoing any configure the unit latched.
	d.che = s.cheGrid

	// Write each captured element's parse state back. The element pointers are
	// the ones just restored into the grid, so this restores the exact
	// pre-unit object.
	for i := range s.nelem {
		slot := &s.slots[i]
		che := d.che[slot.typ][slot.id]
		che.present = slot.present
		che.commonWindow = slot.commonWindow
		che.msPresent = slot.msPresent
		che.MaxSFBSte = slot.MaxSFBSte
		che.MSMask = slot.msMask
		for c := range slot.nch {
			sn := &slot.ch[c]
			che.Ch[c].ICS = sn.ics
			che.Ch[c].TNS = sn.tns
			che.Ch[c].Pulse = sn.pulse
			che.Ch[c].PulsePresent = sn.pulsePres
			che.Ch[c].BandType = sn.bandType
			che.Ch[c].SFO = sn.sfo
			che.Ch[c].SF = sn.sf
			copy(che.Ch[c].Saved[:overlapLen], sn.saved[:])
		}
	}

	// Restore the element list to its captured length and entries. A unit can
	// publish an element and then fail on a later one, so both the length and
	// the entries have to be put back; the backing array is kept for reuse.
	if s.nelems > cap(d.Elems) {
		// Unreachable: s.nelems <= maxFrameElems and the list never grows past
		// it, but never index past the slice.
		d.Elems = make([]ElemRef, s.nelems)
	} else {
		d.Elems = d.Elems[:s.nelems]
	}
	for i := range s.nelems {
		d.Elems[i] = s.elems[i]
	}
}
