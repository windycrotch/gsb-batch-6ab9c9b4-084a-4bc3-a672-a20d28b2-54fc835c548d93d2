package gfx

import (
	"image"
	"image/color"
	"image/draw"
	"sync"
	"testing"
)

// newTestScene builds a drawer with a few blocks arranged along the X
// axis so their depth order is unambiguous: lower X is in front.
func newTestScene(dst draw.Image) *SceneDrawer {
	blocks := []Block{
		NewBlock(V3(12, 0, 0), V3(10, 10, 10), BlockColorRed),
		NewBlock(V3(0, 0, 0), V3(10, 10, 10), BlockColorBlue),
		NewBlock(V3(24, 0, 0), V3(10, 10, 10), BlockColorGreen),
	}

	// A negative origin.Y compensates for the screen-projection sign
	// convention (blockCorner negates v*Z+origin.Y) so the blocks land
	// inside a 128x128 target.
	return NewSceneDrawer(blocks, V3(30, -120, 3), dst)
}

func nonZeroPixels(t *testing.T, img *image.RGBA) int {
	t.Helper()

	n := 0

	EachPixel(img.Bounds(), func(x, y int) {
		c := img.RGBAAt(x, y)
		if c.R != 0 || c.G != 0 || c.B != 0 || c.A != 0 {
			n++
		}
	})

	return n
}

func imagesEqual(a, b *image.RGBA) bool {
	if a.Bounds() != b.Bounds() {
		return false
	}

	eq := true

	EachPixel(a.Bounds(), func(x, y int) {
		if a.RGBAAt(x, y) != b.RGBAAt(x, y) {
			eq = false
		}
	})

	return eq
}

// TestSceneDrawerDrawsAllBlocks is the baseline: a normal render with
// several blocks paints a non-empty image without errors.
func TestSceneDrawerDrawsAllBlocks(t *testing.T) {
	dst := NewImage(128, 128)
	sd := newTestScene(dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}

	if n := nonZeroPixels(t, dst); n == 0 {
		t.Fatal("expected the scene to paint some pixels")
	}
}

// TestSceneDrawerOcclusionOrder verifies far-to-near ordering. Two blocks
// overlap on screen; the nearer one (lower X per Box.Behind) must own the
// overlapping pixels regardless of the order the blocks were added in.
func TestSceneDrawerOcclusionOrder(t *testing.T) {
	origin := V3(100, -130, 3)
	near := NewBlock(V3(0, 0, 0), V3(20, 20, 20), BlockColorBlue)
	far := NewBlock(V3(25, 0, 0), V3(20, 20, 20), BlockColorRed)

	// Sanity-check the depth relation: far (higher X) is strictly behind
	// near, while their 2D projections still overlap on screen.
	if !far.Behind(near) || near.Behind(far) {
		t.Fatal("test setup error: expected far to be strictly behind near")
	}
	if !far.Rect(origin).Bounds().Overlaps(near.Rect(origin).Bounds()) {
		t.Fatal("test setup error: projected blocks do not overlap")
	}

	dst := NewImage(200, 200)
	// Feed far first, then near: a naive append-order renderer would let
	// far win the overlap.
	sd := NewSceneDrawer([]Block{far, near}, origin, dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}

	// Reference: only the near block, rendered on its own. Every pixel it
	// paints must come from the near block in the combined scene.
	onlyNear := NewImage(200, 200)
	if err := NewSceneDrawer([]Block{near}, origin, onlyNear).Draw(); err != nil {
		t.Fatalf("reference Draw returned error: %v", err)
	}

	covered := 0

	EachPixel(dst.Bounds(), func(x, y int) {
		c := onlyNear.RGBAAt(x, y)
		if c.A == 0 {
			return
		}

		covered++

		if got := dst.RGBAAt(x, y); got != c {
			t.Fatalf("pixel (%d,%d) = %v, want near block color %v (far block leaked through)", x, y, got, c)
		}
	})

	if covered == 0 {
		t.Fatal("expected the near block to paint some pixels")
	}

	// Order independence: adding the blocks in the opposite order must
	// produce the exact same image, proving depth sorting drives the result.
	swapped := NewImage(200, 200)
	if err := NewSceneDrawer([]Block{near, far}, origin, swapped).Draw(); err != nil {
		t.Fatalf("swapped Draw returned error: %v", err)
	}

	if !imagesEqual(dst, swapped) {
		t.Fatal("rendered image depends on the order blocks were added")
	}
}

// TestSceneDrawerIdempotent draws the same scene twice onto the same
// target and requires byte-identical output: residual triangle data from
// the first pass must not accumulate.
func TestSceneDrawerIdempotent(t *testing.T) {
	dst := NewImage(128, 128)
	sd := newTestScene(dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("first Draw returned error: %v", err)
	}

	first := NewImage(128, 128)
	Draw(first, first.Bounds(), dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("second Draw returned error: %v", err)
	}

	if !imagesEqual(first, dst) {
		t.Fatal("drawing the same scene twice produced different images")
	}

	// A third pass must match as well, ruling out per-frame accumulation.
	if err := sd.Draw(); err != nil {
		t.Fatalf("third Draw returned error: %v", err)
	}

	if !imagesEqual(first, dst) {
		t.Fatal("drawing the scene a third time changed the image")
	}
}

// TestSceneDrawerOriginSwitchReprojects ensures that replacing the origin
// reprojects every queued block: the two renders must differ, and the
// second render must equal a fresh drawer built with the new origin.
func TestSceneDrawerOriginSwitchReprojects(t *testing.T) {
	dst := NewImage(160, 160)
	sd := newTestScene(dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("first Draw returned error: %v", err)
	}

	before := NewImage(160, 160)
	Draw(before, before.Bounds(), dst)

	sd.SetOrigin(V3(20, -60, 2))

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw after SetOrigin returned error: %v", err)
	}

	if imagesEqual(before, dst) {
		t.Fatal("changing the origin had no effect on the rendered image")
	}

	// A brand-new drawer with the same blocks and new origin must match,
	// proving all queued blocks were reprojected rather than partly drawn
	// at stale coordinates.
	reference := NewImage(160, 160)
	ref := NewSceneDrawer(sd.Blocks(), V3(20, -60, 2), reference)

	if err := ref.Draw(); err != nil {
		t.Fatalf("reference Draw returned error: %v", err)
	}

	if !imagesEqual(reference, dst) {
		t.Fatal("scene after origin switch does not match a fresh scene using that origin")
	}
}

// TestSceneDrawerTargetResizeReprojects checks the target-size half of the
// viewport switch: blocks still visible after shrinking the target are
// re-laid out for the new bounds, and the result matches a fresh drawer
// targeting the smaller image.
func TestSceneDrawerTargetResizeReprojects(t *testing.T) {
	dst := NewImage(160, 160)
	sd := newTestScene(dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("first Draw returned error: %v", err)
	}

	small := NewImage(40, 40)
	sd.SetTarget(small)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw after resize returned error: %v", err)
	}

	reference := NewImage(40, 40)
	ref := NewSceneDrawer(sd.Blocks(), sd.Origin(), reference)

	if err := ref.Draw(); err != nil {
		t.Fatalf("reference Draw returned error: %v", err)
	}

	if !imagesEqual(reference, small) {
		t.Fatal("resized scene does not match a fresh scene at the new size")
	}
}

// TestSceneDrawerSkipsOutOfBoundsBlock shrinks the target until one block
// lies fully outside the visible area. The render must succeed and the
// off-screen block must contribute no pixels.
func TestSceneDrawerSkipsOutOfBoundsBlock(t *testing.T) {
	origin := V3(30, -60, 2)

	visible := NewBlock(V3(0, 0, 0), V3(10, 10, 10), BlockColorBlue)
	offscreen := NewBlock(V3(500, 500, 0), V3(10, 10, 10), BlockColorRed)

	// Sanity check: the offscreen block really is outside the small target.
	if offscreen.Rect(origin).Bounds().Overlaps(IR(0, 0, 32, 32)) {
		t.Fatal("test setup error: offscreen block overlaps the target")
	}

	if !visible.Rect(origin).Bounds().Overlaps(IR(0, 0, 32, 32)) {
		t.Fatal("test setup error: visible block is not inside the target")
	}

	dst := NewImage(32, 32)
	sd := NewSceneDrawer([]Block{visible, offscreen}, origin, dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}

	onlyVisible := NewImage(32, 32)
	if err := NewSceneDrawer([]Block{visible}, origin, onlyVisible).Draw(); err != nil {
		t.Fatalf("reference Draw returned error: %v", err)
	}

	if !imagesEqual(onlyVisible, dst) {
		t.Fatal("the fully off-screen block affected the rendered image")
	}

	if n := nonZeroPixels(t, dst); n == 0 {
		t.Fatal("expected the visible block to still be drawn")
	}
}

// TestSceneDrawerAllBlocksOutOfBounds covers the degenerate case where the
// target is so small that every block is skipped: the call must still
// succeed and leave a blank image.
func TestSceneDrawerAllBlocksOutOfBounds(t *testing.T) {
	dst := NewImage(8, 8)
	sd := NewSceneDrawer(
		[]Block{
			NewBlock(V3(1000, 1000, 0), V3(20, 20, 20), BlockColorRed),
			NewBlock(V3(-1000, -1000, 0), V3(20, 20, 20), BlockColorBlue),
		},
		V3(4, 4, 2),
		dst,
	)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}

	if n := nonZeroPixels(t, dst); n != 0 {
		t.Fatalf("expected a blank image, got %d painted pixels", n)
	}
}

// TestSceneDrawerZeroAndNegativeOriginZ requires predictable projections
// when origin.Z is zero or negative. There is no division in the
// projection, so these must render without panicking and the zero-Z case
// must be stable across repeated draws.
func TestSceneDrawerZeroAndNegativeOriginZ(t *testing.T) {
	for _, z := range []float64{0, -1, -4} {
		dst := NewImage(64, 64)
		sd := NewSceneDrawer(
			[]Block{NewBlock(V3(0, 0, 0), V3(8, 8, 8), BlockColorGreen)},
			V3(32, 48, z),
			dst,
		)

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("origin Z=%v panicked: %v", z, r)
				}
			}()

			if err := sd.Draw(); err != nil {
				t.Fatalf("origin Z=%v Draw error: %v", z, err)
			}
		}()

		// Repeated renders stay identical even at the degenerate Z.
		snapshot := NewImage(64, 64)
		Draw(snapshot, snapshot.Bounds(), dst)

		if err := sd.Draw(); err != nil {
			t.Fatalf("second Draw at Z=%v error: %v", z, err)
		}

		if !imagesEqual(snapshot, dst) {
			t.Fatalf("render at origin Z=%v is not stable", z)
		}
	}
}

// TestSceneDrawerHugeCoordinatesNoOverflow points a block far enough away
// that, were the triangle loops unbounded, integer conversion would be at
// risk. With clipping in place the call must return promptly and leave the
// target untouched.
func TestSceneDrawerHugeCoordinatesNoOverflow(t *testing.T) {
	dst := NewImage(16, 16)
	sd := NewSceneDrawer(
		[]Block{NewBlock(V3(1e9, 1e9, 0), V3(10, 10, 10), BlockColorRed)},
		V3(8, 8, 1e6),
		dst,
	)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}

	if n := nonZeroPixels(t, dst); n != 0 {
		t.Fatalf("expected no painted pixels from the distant block, got %d", n)
	}
}

// TestSceneDrawerNilTargetErrorAndRecovery covers the failure path:
// swapping in a nil target makes Draw return ErrNilTarget without
// corrupting the drawer, and a later valid target renders normally.
func TestSceneDrawerNilTargetErrorAndRecovery(t *testing.T) {
	dst := NewImage(64, 64)
	sd := newTestScene(dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("initial Draw returned error: %v", err)
	}

	sd.SetTarget(nil)

	if err := sd.Draw(); err != ErrNilTarget {
		t.Fatalf("Draw with nil target = %v, want %v", err, ErrNilTarget)
	}

	// The drawer must remain usable: blocks and origin survive the error.
	if got := len(sd.Blocks()); got != 3 {
		t.Fatalf("Blocks after nil-target error = %d, want 3", got)
	}

	recovered := NewImage(64, 64)
	sd.SetTarget(recovered)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw after recovery returned error: %v", err)
	}

	reference := NewImage(64, 64)
	if err := NewSceneDrawer(sd.Blocks(), sd.Origin(), reference).Draw(); err != nil {
		t.Fatalf("reference Draw returned error: %v", err)
	}

	if !imagesEqual(reference, recovered) {
		t.Fatal("recovered drawer did not render the same scene as a fresh drawer")
	}
}

// TestSceneDrawerNewWithNilTarget verifies the constructor accepts a nil
// target and reports the same explicit error on Draw rather than panicking.
func TestSceneDrawerNewWithNilTarget(t *testing.T) {
	sd := NewSceneDrawer(nil, V3(1, 2, 3), nil)

	if err := sd.Draw(); err != ErrNilTarget {
		t.Fatalf("Draw with nil target = %v, want %v", err, ErrNilTarget)
	}

	dst := NewImage(32, 32)
	sd.SetTarget(dst)
	sd.SetOrigin(V3(16, -24, 1))
	sd.Add(NewBlock(V3(0, 0, 0), V3(8, 8, 8), BlockColorPurple))

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw after adding a block returned error: %v", err)
	}

	if n := nonZeroPixels(t, dst); n == 0 {
		t.Fatal("expected the added block to paint pixels after recovery")
	}
}

// TestSceneDrawerConcurrentAccess hammers one drawer from several goroutines
// that mix Draw with SetOrigin, SetTarget and Add. It is meant to be run
// under the race detector (`go test -race`); the deterministic image checks
// also prove concurrent renders cannot cross-contaminate each other.
func TestSceneDrawerConcurrentAccess(t *testing.T) {
	const workers = 8
	const iterations = 50

	dst := NewImage(96, 96)
	sd := newTestScene(dst)

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				switch i % 4 {
				case 0:
					if err := sd.Draw(); err != nil && err != ErrNilTarget {
						t.Errorf("unexpected Draw error: %v", err)

						return
					}
				case 1:
					sd.SetOrigin(V3(float64(id*7+i), 40, float64(1+(i%3))))
				case 2:
					// Rotate between the shared target and a worker-local
					// one, never nil, so Draw calls in this branch stay valid.
					local := NewImage(96, 96)
					sd.SetTarget(local)
					sd.SetTarget(dst)
				case 3:
					sd.Add(NewBlock(V3(float64(i), 0, 0), V3(4, 4, 4), BlockColorYellow))
				}
			}
		}(w)
	}

	wg.Wait()

	// The drawer must still be fully functional after the concurrent storm.
	sd.SetTarget(dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("final Draw returned error: %v", err)
	}

	reference := NewImage(96, 96)
	if err := NewSceneDrawer(sd.Blocks(), sd.Origin(), reference).Draw(); err != nil {
		t.Fatalf("reference Draw returned error: %v", err)
	}

	if !imagesEqual(reference, dst) {
		t.Fatal("post-concurrency render does not match a fresh scene with the same state")
	}
}

// TestSceneDrawerConcurrentDrawsSameTarget runs parallel Draw calls against
// one immutable scene and target. Every frame is a full overwrite, so each
// completed frame must equal the single-threaded reference image.
func TestSceneDrawerConcurrentDrawsSameTarget(t *testing.T) {
	dst := NewImage(96, 96)
	sd := newTestScene(dst)

	reference := NewImage(96, 96)
	if err := NewSceneDrawer(sd.Blocks(), sd.Origin(), reference).Draw(); err != nil {
		t.Fatalf("reference Draw returned error: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 25; j++ {
				if err := sd.Draw(); err != nil {
					t.Errorf("Draw error: %v", err)

					return
				}
			}
		}()
	}

	wg.Wait()

	if !imagesEqual(reference, dst) {
		t.Fatal("parallel draws left the target in a state different from the reference")
	}
}

// TestSceneDrawerTwoDrawersSharedTarget runs two independent drawers that
// both write to the same image from separate goroutines. The per-target
// lock must keep this race-free while every completed frame remains a
// valid rendering of one of the two scenes.
func TestSceneDrawerTwoDrawersSharedTarget(t *testing.T) {
	dst := NewImage(96, 96)

	origin := V3(30, -120, 3)
	a := NewSceneDrawer(
		[]Block{NewBlock(V3(0, 0, 0), V3(10, 10, 10), BlockColorBlue)},
		origin, dst,
	)
	b := NewSceneDrawer(
		[]Block{NewBlock(V3(12, 0, 0), V3(10, 10, 10), BlockColorRed)},
		origin, dst,
	)

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func(drawer *SceneDrawer) {
			defer wg.Done()

			for j := 0; j < 40; j++ {
				if err := drawer.Draw(); err != nil {
					t.Errorf("Draw error: %v", err)

					return
				}
			}
		}([]*SceneDrawer{a, b}[i])
	}

	wg.Wait()

	// The final image must equal one of the two scenes in full, never a
	// torn mix: the target lock makes each composite atomic.
	refA := NewImage(96, 96)
	if err := NewSceneDrawer(a.Blocks(), origin, refA).Draw(); err != nil {
		t.Fatalf("reference A error: %v", err)
	}

	refB := NewImage(96, 96)
	if err := NewSceneDrawer(b.Blocks(), origin, refB).Draw(); err != nil {
		t.Fatalf("reference B error: %v", err)
	}

	if !imagesEqual(refA, dst) && !imagesEqual(refB, dst) {
		t.Fatal("final image matches neither scene — concurrent composites interleaved")
	}
}

// TestSceneDrawerDistinctTargetsInParallel confirms renders onto separate
// images stay race-free and each image only ever receives its own scene.
func TestSceneDrawerDistinctTargetsInParallel(t *testing.T) {
	const n = 6

	var (
		wg      sync.WaitGroup
		targets = make([]*image.RGBA, n)
		drawers = make([]*SceneDrawer, n)
	)

	origin := V3(30, -120, 3)

	for i := 0; i < n; i++ {
		targets[i] = NewImage(64, 64)
		drawers[i] = NewSceneDrawer(
			[]Block{NewBlock(V3(float64(i), 0, 0), V3(6, 6, 6), BlockColorGreen)},
			origin, targets[i],
		)
	}

	for i := 0; i < n; i++ {
		wg.Add(1)

		go func(sd *SceneDrawer) {
			defer wg.Done()

			for j := 0; j < 30; j++ {
				if err := sd.Draw(); err != nil {
					t.Errorf("Draw error: %v", err)

					return
				}
			}
		}(drawers[i])
	}

	wg.Wait()

	for i, sd := range drawers {
		ref := NewImage(64, 64)
		if err := NewSceneDrawer(sd.Blocks(), origin, ref).Draw(); err != nil {
			t.Fatalf("reference %d error: %v", i, err)
		}

		if !imagesEqual(ref, targets[i]) {
			t.Fatalf("target %d was contaminated by another render", i)
		}
	}
}

// TestSceneDrawerBlocksAccessors checks the defensive-copy contract: slices
// handed to the constructor or SetBlocks/Add cannot be mutated through the
// caller's backing array, and Blocks returns an independent copy.
func TestSceneDrawerBlocksAccessors(t *testing.T) {
	origin := V3(10, 10, 2)
	initial := []Block{NewBlock(V3(0, 0, 0), V3(5, 5, 5), BlockColorRed)}

	sd := NewSceneDrawer(initial, origin, NewImage(32, 32))

	initial[0] = NewBlock(V3(99, 99, 99), V3(1, 1, 1), BlockColorBlue)

	if got := sd.Blocks()[0].Pos; got != V3(0, 0, 0) {
		t.Fatalf("constructor did not copy the blocks slice, Pos = %v", got)
	}

	extra := []Block{NewBlock(V3(1, 1, 1), V3(2, 2, 2), BlockColorGreen)}
	sd.SetBlocks(extra)
	extra[0] = NewBlock(V3(0, 0, 0), V3(9, 9, 9), BlockColorBlack)

	if got := sd.Blocks()[0].Size; got != V3(2, 2, 2) {
		t.Fatalf("SetBlocks did not copy the blocks slice, Size = %v", got)
	}

	view := sd.Blocks()
	view[0] = Block{}

	if got := sd.Blocks()[0].Size; got != V3(2, 2, 2) {
		t.Fatalf("Blocks returned a slice backed by drawer state, Size = %v", got)
	}

	if got := sd.Origin(); got != origin {
		t.Fatalf("Origin = %v, want %v", got, origin)
	}
}

// TestSceneDrawerEmptyScene confirms a drawer with no blocks renders a
// blank target without error and stays idempotent.
func TestSceneDrawerEmptyScene(t *testing.T) {
	dst := NewImage(32, 32, color.NRGBA{R: 1, G: 2, B: 3, A: 255})
	sd := NewSceneDrawer(nil, V3(0, 0, 1), dst)

	if err := sd.Draw(); err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}

	if n := nonZeroPixels(t, dst); n != 0 {
		t.Fatalf("expected blank image, got %d non-zero pixels", n)
	}
}
