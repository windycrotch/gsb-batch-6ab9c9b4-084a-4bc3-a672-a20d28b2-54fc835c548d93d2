// Copyright 2018 The ZikiChombo Authors. All rights reserved.  Use of this source
// code is governed by a license that can be found in the License file.

package fft

import (
	"math"
	"math/cmplx"
	"math/rand"
	"testing"
)

// realHcTol is the tolerance for float comparisons involving a
// forward+inverse round trip or a convolution through the spectra.
const realHcTol = 1e-9

// realHcSizes covers even and odd transform lengths, including
// non powers of two on both sides.
var realHcSizes = []int{4, 8, 16, 64, 256, 3, 5, 7, 15, 33, 65, 127}

func randFrame(rng *rand.Rand, n int) []float64 {
	d := make([]float64, n)
	for i := range d {
		d[i] = rng.Float64() - 0.5
	}
	return d
}

func clone(d []float64) []float64 {
	c := make([]float64, len(d))
	copy(c, d)
	return c
}

// mustPanic runs f and reports a test error unless f panics.
func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected panic, got none", what)
		}
	}()
	f()
}

// TestRealRoundTripSizes checks that Do followed by Inv restores the
// original samples within floating point error, for even and odd
// lengths.  With the default (unitary) scaling the round trip is the
// identity; with scaling off the transforms are an unscaled DFT pair
// and the round trip gains a fixed factor (n/2 for even lengths, which
// use a half-size complex transform, n for odd lengths).
func TestRealRoundTripSizes(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, n := range realHcSizes {
		for _, scale := range []bool{true, false} {
			factor := 1.0
			if !scale {
				factor = float64(n)
				if n%2 == 0 {
					factor = float64(n / 2)
				}
			}
			rft := NewReal(n)
			rft.Scale(scale)
			src := randFrame(rng, n)
			d := clone(src)
			hc := rft.Do(d)
			if len(hc) != n {
				t.Fatalf("n=%d scale=%v: Do returned HalfComplex of length %d", n, scale, len(hc))
			}
			back := rft.Inv(hc)
			for i := range back {
				if want := factor * src[i]; math.Abs(back[i]-want) > realHcTol*factor {
					t.Errorf("n=%d scale=%v: round trip idx %d got %v want %v", n, scale, i, back[i], want)
				}
			}
		}
	}
}

// TestRealMultiFrameReuse runs many frames through one transformer and
// checks each frame against a fresh transformer, so that no residue
// from a previous frame can leak into later results.  Each frame is
// also round-tripped through Inv on the same transformer.
func TestRealMultiFrameReuse(t *testing.T) {
	const frames = 8
	rng := rand.New(rand.NewSource(2))
	for _, n := range realHcSizes {
		rft := NewReal(n)
		for f := 0; f < frames; f++ {
			src := randFrame(rng, n)

			// reference: a fresh transformer sees only this frame.
			ref := NewReal(n)
			refHC := ref.Do(clone(src))

			hc := rft.Do(clone(src))
			for i := range hc {
				if hc[i] != refHC[i] {
					t.Fatalf("n=%d frame=%d idx=%d: reused transformer gave %v, fresh gave %v",
						n, f, i, hc[i], refHC[i])
				}
			}
			back := rft.Inv(hc)
			for i := range back {
				if math.Abs(back[i]-src[i]) > realHcTol {
					t.Errorf("n=%d frame=%d: round trip idx %d got %v want %v", n, f, i, back[i], src[i])
				}
			}
		}
	}
}

// TestRealDoBadLengthRecovery checks that a Do call with a mismatched
// length panics without modifying the passed slice, and that the
// transformer keeps working correctly on valid frames afterwards.
func TestRealDoBadLengthRecovery(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for _, n := range []int{16, 64, 15, 33} {
		rft := NewReal(n)
		// prime the transformer with a valid frame so its scratch
		// state is dirty before the failing calls.
		rft.Do(randFrame(rng, n))

		for _, bad := range []int{0, n - 1, n + 1, 2 * n} {
			badBuf := randFrame(rng, bad)
			snapshot := clone(badBuf)
			mustPanic(t, "Do with wrong length", func() { rft.Do(badBuf) })
			for i := range badBuf {
				if badBuf[i] != snapshot[i] {
					t.Fatalf("n=%d bad=%d: Do modified input at idx %d on failure", n, bad, i)
				}
			}
		}

		// the transformer must be unaffected by the failed calls.
		src := randFrame(rng, n)
		hc := rft.Do(clone(src))
		refHC := NewReal(n).Do(clone(src))
		for i := range hc {
			if hc[i] != refHC[i] {
				t.Fatalf("n=%d: after failed Do, idx %d got %v want %v", n, i, hc[i], refHC[i])
			}
		}
		back := rft.Inv(hc)
		for i := range back {
			if math.Abs(back[i]-src[i]) > realHcTol {
				t.Errorf("n=%d: after failed Do, round trip idx %d got %v want %v", n, i, back[i], src[i])
			}
		}
	}
}

// TestRealInvBadLengthRecovery checks that an Inv call with a
// mismatched length panics without modifying the passed HalfComplex,
// and that the transformer still inverts valid spectra afterwards.
func TestRealInvBadLengthRecovery(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	for _, n := range []int{16, 64, 15, 33} {
		rft := NewReal(n)
		src := randFrame(rng, n)
		hc := rft.Do(clone(src))

		for _, bad := range []int{0, n - 1, n + 1, 2 * n} {
			badHC := HalfComplex(randFrame(rng, bad))
			snapshot := clone(badHC)
			mustPanic(t, "Inv with wrong length", func() { rft.Inv(badHC) })
			for i := range badHC {
				if badHC[i] != snapshot[i] {
					t.Fatalf("n=%d bad=%d: Inv modified input at idx %d on failure", n, bad, i)
				}
			}
		}

		// the spectrum computed before the failures still inverts
		// back to the original frame.
		back := rft.Inv(hc)
		for i := range back {
			if math.Abs(back[i]-src[i]) > realHcTol {
				t.Errorf("n=%d: after failed Inv, round trip idx %d got %v want %v", n, i, back[i], src[i])
			}
		}
	}
}

// TestHalfComplexViewConsistency checks that the real/imag accessors
// and the complex view of a HalfComplex agree with each other for
// even and odd lengths, including conjugation.
func TestHalfComplexViewConsistency(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for _, n := range []int{64, 65} {
		hc := HalfComplex(randFrame(rng, n))
		if want := n/2 + 1; hc.Len() != want {
			t.Fatalf("n=%d: Len()=%d want %d", n, hc.Len(), want)
		}
		for i := 0; i < hc.Len(); i++ {
			// Cmplx must agree with the Real/Imag accessors.
			if c := hc.Cmplx(i); c != complex(hc.Real(i), hc.Imag(i)) {
				t.Errorf("n=%d i=%d: Cmplx=%v but Real/Imag give %v",
					n, i, c, complex(hc.Real(i), hc.Imag(i)))
			}
			// SetReal/SetImag must be reflected in the complex view.
			re, im := rng.Float64(), rng.Float64()
			hc.SetReal(i, re)
			hc.SetImag(i, im)
			wantIm := im
			if i == 0 || 2*i == n {
				wantIm = 0.0 // DC and Nyquist have no imaginary part.
			}
			if c := hc.Cmplx(i); c != complex(re, wantIm) {
				t.Errorf("n=%d i=%d: after SetReal/SetImag Cmplx=%v want %v", n, i, c, complex(re, wantIm))
			}
			// SetCmplx/Cmplx round trip, and conjugation is exact.
			c := complex(rng.Float64(), rng.Float64())
			want := c
			if i == 0 || 2*i == n {
				want = complex(real(c), 0.0)
			}
			hc.SetCmplx(i, c)
			if got := hc.Cmplx(i); got != want {
				t.Errorf("n=%d i=%d: SetCmplx/Cmplx got %v want %v", n, i, got, want)
			}
			hc.SetCmplx(i, cmplx.Conj(c))
			if got := hc.Cmplx(i); got != cmplx.Conj(want) {
				t.Errorf("n=%d i=%d: conjugate got %v want %v", n, i, got, cmplx.Conj(want))
			}
		}
	}
}

// TestHalfComplexDCNyquist checks the special bins: the DC bin always,
// and the Nyquist bin for even lengths, have zero imaginary part, and
// writes to those imaginary parts are consistently dropped rather than
// corrupting neighbouring bins.  For odd lengths there is no Nyquist
// bin and the middle bin keeps its imaginary part.
func TestHalfComplexDCNyquist(t *testing.T) {
	for _, n := range []int{8, 9} {
		hc := HalfComplex(make([]float64, n))
		// DC bin.
		if hc.Imag(0) != 0.0 {
			t.Errorf("n=%d: Imag(0)=%v want 0", n, hc.Imag(0))
		}
		hc.SetImag(0, 3.5) // documented no-op
		if hc.Imag(0) != 0.0 {
			t.Errorf("n=%d: after SetImag(0, 3.5), Imag(0)=%v want 0", n, hc.Imag(0))
		}
		hc.SetCmplx(0, complex(2.0, 9.0))
		if c := hc.Cmplx(0); c != complex(2.0, 0.0) {
			t.Errorf("n=%d: after SetCmplx on DC, Cmplx(0)=%v want (2+0i)", n, c)
		}
		if hc.Real(0) != 2.0 {
			t.Errorf("n=%d: Real(0)=%v want 2", n, hc.Real(0))
		}
		if n%2 == 0 {
			ny := n / 2
			if hc.Imag(ny) != 0.0 {
				t.Errorf("n=%d: Imag(Nyquist)=%v want 0", n, hc.Imag(ny))
			}
			hc.SetImag(ny, 3.5) // documented no-op
			if hc.Imag(ny) != 0.0 {
				t.Errorf("n=%d: after SetImag, Imag(Nyquist)=%v want 0", n, hc.Imag(ny))
			}
			hc.SetCmplx(ny, complex(-1.0, 7.0))
			if c := hc.Cmplx(ny); c != complex(-1.0, 0.0) {
				t.Errorf("n=%d: after SetCmplx on Nyquist, Cmplx=%v want (-1+0i)", n, c)
			}
			// the dropped imaginary writes must not have leaked
			// into any other slot.
			for i, v := range hc {
				if i == 0 && v != 2.0 {
					t.Errorf("n=%d: hc[0]=%v want 2", n, v)
				} else if i == ny && v != -1.0 {
					t.Errorf("n=%d: hc[Nyquist]=%v want -1", n, v)
				} else if i != 0 && i != ny && v != 0.0 {
					t.Errorf("n=%d: stray write at hc[%d]=%v", n, i, v)
				}
			}
		} else {
			// odd length: no Nyquist bin; the middle bin has a
			// real imaginary part stored at h[n-1-mid].
			mid := n / 2
			hc.SetImag(mid, 1.25)
			if hc.Imag(mid) != 1.25 {
				t.Errorf("n=%d: Imag(mid)=%v want 1.25", n, hc.Imag(mid))
			}
		}
	}
}

// TestHalfComplexZeroLen checks that zero-length HalfComplex values
// behave consistently: no stored bins, and the bulk converters and
// MulElems are no-ops instead of panicking or reading out of bounds.
func TestHalfComplexZeroLen(t *testing.T) {
	var hc HalfComplex
	if hc.Len() != 0 {
		t.Errorf("empty HalfComplex Len()=%d want 0", hc.Len())
	}
	hc.ToCmplx(nil)
	hc.FromCmplx(nil)
	hc.ToPolar(nil, nil)
	hc.FromPolar(nil, nil)
	a, b := HalfComplex{}, HalfComplex{}
	if got := a.MulElems(b); len(got) != 0 {
		t.Errorf("MulElems on empty HalfComplex returned length %d", len(got))
	}
}

// TestHalfComplexToCmplxHermitian checks that expanding a HalfComplex
// to a full complex spectrum yields Hermitian symmetry with real DC
// (and Nyquist for even lengths), and that FromCmplx inverts ToCmplx.
func TestHalfComplexToCmplxHermitian(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	for _, n := range []int{64, 65} {
		d := randFrame(rng, n)
		hc := HalfComplex(d)
		full := make([]complex128, n)
		hc.ToCmplx(full)
		if imag(full[0]) != 0.0 {
			t.Errorf("n=%d: DC has imaginary part %v", n, imag(full[0]))
		}
		if n%2 == 0 && imag(full[n/2]) != 0.0 {
			t.Errorf("n=%d: Nyquist has imaginary part %v", n, imag(full[n/2]))
		}
		for i := 1; i < n; i++ {
			if full[n-i] != cmplx.Conj(full[i]) {
				t.Errorf("n=%d i=%d: %v != conj(%v)", n, i, full[n-i], full[i])
			}
		}
		back := make([]float64, n)
		HalfComplex(back).FromCmplx(full)
		for i := range back {
			if back[i] != d[i] {
				t.Errorf("n=%d i=%d: ToCmplx/FromCmplx got %v want %v", n, i, back[i], d[i])
			}
		}
	}
}

// TestHalfComplexMulElems checks that elementwise multiplication of
// HalfComplex spectra equals pointwise multiplication through the
// complex view, for even and odd lengths.
func TestHalfComplexMulElems(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, n := range []int{2, 4, 8, 64, 3, 5, 9, 65} {
		a := HalfComplex(randFrame(rng, n))
		b := HalfComplex(randFrame(rng, n))
		want := make([]complex128, a.Len())
		for i := range want {
			want[i] = a.Cmplx(i) * b.Cmplx(i)
		}
		if got := a.MulElems(b); &got[0] != &a[0] {
			t.Errorf("n=%d: MulElems did not return its receiver", n)
		}
		for i := range want {
			if got := a.Cmplx(i); got != want[i] {
				t.Errorf("n=%d i=%d: MulElems gave %v, complex view product is %v", n, i, got, want[i])
			}
		}
	}
}

// TestHalfComplexMulElemsBadLength checks that a length mismatch in
// MulElems panics before touching the receiver.
func TestHalfComplexMulElemsBadLength(t *testing.T) {
	a := HalfComplex{1, 2, 3, 4}
	b := HalfComplex{1, 2, 3}
	snapshot := clone(a)
	mustPanic(t, "MulElems with mismatched lengths", func() { a.MulElems(b) })
	for i := range a {
		if a[i] != snapshot[i] {
			t.Fatalf("MulElems modified receiver at idx %d on length mismatch", i)
		}
	}
}

// TestRealMulElemsConvolution is the cross scenario: two frames are
// transformed by the same reused transformer, their HalfComplex
// spectra are multiplied elementwise, and the inverse transform of
// the product is compared against the naive circular convolution.
// With the default (unitary) scaling, Inv(F(x)*F(y)) = conv(x, y)/sqrt(n).
func TestRealMulElemsConvolution(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	for _, n := range []int{32, 33} {
		rft := NewReal(n)
		x := randFrame(rng, n)
		y := randFrame(rng, n)

		hx := rft.Do(clone(x))
		hy := rft.Do(clone(y)) // second frame on the same transformer
		prod := hx.MulElems(hy)
		conv := rft.Inv(prod)

		sqrtN := math.Sqrt(float64(n))
		for k := 0; k < n; k++ {
			var want float64
			for j := 0; j < n; j++ {
				want += x[j] * y[(k-j+n)%n]
			}
			want /= sqrtN
			if math.Abs(conv[k]-want) > realHcTol {
				t.Errorf("n=%d k=%d: Inv(F(x).*F(y))=%v want conv/sqrt(n)=%v", n, k, conv[k], want)
			}
		}
	}
}
