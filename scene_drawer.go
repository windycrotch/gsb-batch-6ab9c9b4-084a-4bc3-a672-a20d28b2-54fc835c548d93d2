package gfx

import (
	"image"
	"image/draw"
	"sort"
	"sync"
)

// ErrNilTarget is returned by SceneDrawer.Draw when it is asked to render
// onto a nil target. The drawer itself is left intact, so after receiving
// this error a valid target can be set and drawing can be retried.
var ErrNilTarget = Error("gfx: scene drawer target is nil")

// targetLocks serializes renders that write to the same draw.Image.
//
// A draw.Image's pixel memory is not safe for concurrent writes, so two
// Draw calls — even from different SceneDrawer instances — must not touch
// the same target at the same time. Distinct targets are still rendered in
// parallel because each image gets its own mutex. The registry is keyed by
// the interface value's identity; locks are retained for the image's
// lifetime, which is bounded by the number of distinct images ever drawn.
var targetLocks = struct {
	sync.Mutex
	m map[draw.Image]*sync.Mutex
}{
	m: make(map[draw.Image]*sync.Mutex),
}

// lockForTarget returns the mutex guarding writes to target, creating it on
// first use.
func lockForTarget(target draw.Image) *sync.Mutex {
	targetLocks.Lock()
	l := targetLocks.m[target]
	if l == nil {
		l = &sync.Mutex{}
		targetLocks.m[target] = l
	}
	targetLocks.Unlock()

	return l
}

// SceneDrawer renders a collection of Block values onto a draw.Image using
// layered redraw: the blocks are depth-sorted and drawn onto a fresh
// offscreen canvas first, then the whole canvas is composited onto the
// target in one operation.
//
// Because every render starts from a blank offscreen canvas, drawing the
// same scene repeatedly onto the same target is idempotent — no triangle
// data from a previous render can accumulate.
//
// A SceneDrawer is safe for concurrent use: its blocks, origin and target
// are guarded by a mutex, every Draw works on its own offscreen canvas, and
// writes to any given target are serialized through a per-target lock while
// different targets can still be rendered concurrently.
type SceneDrawer struct {
	mu     sync.Mutex
	blocks []Block
	origin Vec3
	target draw.Image
}

// NewSceneDrawer creates a SceneDrawer holding the given blocks, viewport
// origin and destination target. Any of the arguments may be zero/nil and
// replaced later through SetBlocks, SetOrigin and SetTarget.
func NewSceneDrawer(blocks []Block, origin Vec3, target draw.Image) *SceneDrawer {
	return &SceneDrawer{
		// Copy the slice so later changes by the caller cannot mutate the
		// scene behind the drawer's back.
		blocks: append([]Block(nil), blocks...),
		origin: origin,
		target: target,
	}
}

// Add appends one or more blocks to the scene. The blocks are projected
// with the origin and target size in effect at the next Draw call, so
// blocks added before a viewport change are reprojected rather than drawn
// at stale coordinates.
func (sd *SceneDrawer) Add(blocks ...Block) {
	if len(blocks) == 0 {
		return
	}

	sd.mu.Lock()
	sd.blocks = append(sd.blocks, blocks...)
	sd.mu.Unlock()
}

// SetBlocks replaces the entire set of blocks in the scene.
func (sd *SceneDrawer) SetBlocks(blocks []Block) {
	sd.mu.Lock()
	sd.blocks = append([]Block(nil), blocks...)
	sd.mu.Unlock()
}

// Blocks returns a copy of the blocks currently held by the drawer.
func (sd *SceneDrawer) Blocks() []Block {
	sd.mu.Lock()
	blocks := append([]Block(nil), sd.blocks...)
	sd.mu.Unlock()

	return blocks
}

// SetOrigin replaces the viewport origin. Blocks already in the scene keep
// no cached projection, so the next Draw reprojects every block with this
// origin and no old screen coordinates survive.
func (sd *SceneDrawer) SetOrigin(origin Vec3) {
	sd.mu.Lock()
	sd.origin = origin
	sd.mu.Unlock()
}

// Origin returns the current viewport origin.
func (sd *SceneDrawer) Origin() Vec3 {
	sd.mu.Lock()
	origin := sd.origin
	sd.mu.Unlock()

	return origin
}

// SetTarget replaces the destination image. Passing nil is allowed; the
// next Draw then returns ErrNilTarget and the drawer can be reused once a
// valid target is set.
func (sd *SceneDrawer) SetTarget(target draw.Image) {
	sd.mu.Lock()
	sd.target = target
	sd.mu.Unlock()
}

// Target returns the current destination image, which may be nil.
func (sd *SceneDrawer) Target() draw.Image {
	sd.mu.Lock()
	target := sd.target
	sd.mu.Unlock()

	return target
}

// Draw renders the scene onto the current target.
//
// The blocks are ordered from far to near using Block.Behind so nearer
// blocks cover farther ones, then drawn onto a freshly allocated
// offscreen canvas of the target's size. Blocks whose projected shape
// falls completely outside the target bounds are skipped without error.
// Finally the canvas is composited onto dst, overwriting whatever was
// there before — repeated Draw calls therefore produce identical results.
//
// Draw returns ErrNilTarget when the current target is nil. The drawer is
// not modified by such a failure and stays usable with a later target.
// Concurrent Draw calls targeting the same image are serialized; distinct
// images render in parallel.
func (sd *SceneDrawer) Draw() error {
	sd.mu.Lock()
	dst := sd.target
	if dst == nil {
		sd.mu.Unlock()
		return ErrNilTarget
	}

	// Snapshot the scene state under the lock and render on local copies so
	// concurrent SetOrigin/SetTarget/Add calls cannot change the projection
	// halfway through a frame or cross-contaminate two frames.
	origin := sd.origin
	blocks := append([]Block(nil), sd.blocks...)
	sd.mu.Unlock()

	// Hold the target lock for the whole render, including the final
	// composite, so no other goroutine can write to this image's pixels
	// concurrently. Canvas allocation and projection happen inside the
	// critical section too; the canvas is per-call and never shared.
	targetMu := lockForTarget(dst)
	targetMu.Lock()
	defer targetMu.Unlock()

	bounds := dst.Bounds()

	// Blank offscreen canvas: the stable "sort — project — land" entry
	// point. Allocating it per Draw guarantees that no triangle data from
	// previous renders remains.
	canvas := image.NewRGBA(bounds)

	if len(blocks) > 0 {
		// Far to near. Behind reports whether the receiver is in front of
		// the argument, so ordering on it (the same comparator used by
		// Blocks.Sort) places the farthest block first and the nearest
		// last, which makes later draws cover earlier ones.
		sort.SliceStable(blocks, func(i, j int) bool {
			return blocks[i].Behind(blocks[j])
		})

		for _, block := range blocks {
			// Skip blocks fully outside the visible area. Rect already
			// accounts for the current origin, so a viewport or target
			// size change is reflected here on every render.
			if !block.Rect(origin).Bounds().Overlaps(bounds) {
				continue
			}

			drawBlockClipped(canvas, block, origin, bounds)
		}
	}

	// Composite the completed layer onto the target in one operation.
	// draw.Src overwrites the destination instead of blending into it,
	// which is what makes repeated renders idempotent.
	draw.Draw(dst, bounds, canvas, bounds.Min, draw.Src)

	return nil
}

// drawBlockClipped renders a single block onto dst, clamping every
// triangle's pixel iteration to clip so coordinates produced by an origin
// with a zero or negative Z component (or any other extreme projection)
// can never drive an out-of-range or overflowing loop. The projection in
// blockCorner is a multiplication plus an offset, so it never divides by
// zero; intersecting with the real image rectangle keeps the integer side
// bounded and turns fully off-screen triangles into empty loops.
func drawBlockClipped(dst draw.Image, block Block, origin Vec3, clip image.Rectangle) {
	for _, t := range block.Triangles(origin) {
		b := t.Bounds().Intersect(clip)

		for x := b.Min.X; x < b.Max.X; x++ {
			for y := b.Min.Y; y < b.Max.Y; y++ {
				u := IV(x, y)
				if t.Contains(u) {
					SetVec(dst, u, t.Color(u))
				}
			}
		}
	}
}
