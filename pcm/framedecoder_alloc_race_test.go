// SPDX-License-Identifier: LGPL-2.1-or-later

//go:build race

package pcm

// failureAllocCeiling bounds the per-failure allocation count in
// TestFrameDecoderFailureDoesNotAllocatePerFrame. Under -race the detector
// instruments the runtime, so AllocsPerRun stops reflecting real allocation and
// becomes scheduler and GC noise, and a numeric ceiling would be a magic number
// a loaded runner could exceed. Disable the bound; the non-race lane asserts it.
const failureAllocCeiling = -1
