// SPDX-License-Identifier: LGPL-2.1-or-later

//go:build !race

package pcm

// failureAllocCeiling bounds the per-failure allocation count in
// TestFrameDecoderFailureDoesNotAllocatePerFrame. mapErr composes a handful of
// wrapped sentinels, so a small constant is the ceiling; if the transaction
// copied state to the heap instead of into the decoder-owned snapshot, the
// count would be far above it.
const failureAllocCeiling = 8
