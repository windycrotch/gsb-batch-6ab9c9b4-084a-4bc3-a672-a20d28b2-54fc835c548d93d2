package m3u8

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// decodeString decodes an inline playlist, failing the test on error.
func decodeString(t *testing.T, s string) *Playlist {
	t.Helper()
	p, err := Decode(strings.NewReader(s))
	if err != nil {
		t.Fatalf("decode playlist: %v\ninput:\n%s", err, s)
	}
	return p
}

// everySegmentHasKey checks that every segment carries an effective,
// non-nil key after inheritance is resolved.
func assertEverySegmentHasKey(t *testing.T, p *Playlist) {
	t.Helper()
	for i, seg := range p.Segments {
		if seg.Key == nil {
			t.Errorf("segment %d (%s): missing effective key", i, seg.URI)
		}
	}
}

func TestKeyInheritance(t *testing.T) {
	// A single EXT-X-KEY applies to every following segment until a
	// new key is declared, even though the tag appears once.
	const playlist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0x00000000000000000000000000000001
#EXTINF:6.000,
seg1.ts
#EXTINF:6.000,
seg2.ts
#EXTINF:6.000,
seg3.ts
`
	p := decodeString(t, playlist)
	if len(p.Segments) != 3 {
		t.Fatalf("want 3 segments, got %d", len(p.Segments))
	}
	assertEverySegmentHasKey(t, p)
	for i, seg := range p.Segments {
		if seg.Key.Method != EncryptMethodAES128 || seg.Key.URI != "key1" {
			t.Errorf("segment %d: did not inherit key1, got %v", i, seg.Key)
		}
	}
}

func TestKeySwitchAndNone(t *testing.T) {
	// key1 -> METHOD=NONE (clears encryption) -> key2.
	const playlist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0x00000000000000000000000000000001
#EXTINF:6.000,
seg1.ts
#EXTINF:6.000,
seg2.ts
#EXT-X-KEY:METHOD=NONE
#EXTINF:6.000,
seg3.ts
#EXTINF:6.000,
seg4.ts
#EXT-X-KEY:METHOD=AES-128,URI="key2",IV=0x00000000000000000000000000000002
#EXTINF:6.000,
seg5.ts
#EXTINF:6.000,
seg6.ts
`
	p := decodeString(t, playlist)
	if len(p.Segments) != 6 {
		t.Fatalf("want 6 segments, got %d", len(p.Segments))
	}
	assertEverySegmentHasKey(t, p)

	wantMethod := []EncryptMethod{
		EncryptMethodAES128, EncryptMethodAES128, // seg1, seg2 inherit key1
		EncryptMethodNone, EncryptMethodNone, // seg3, seg4: encryption cleared
		EncryptMethodAES128, EncryptMethodAES128, // seg5, seg6 inherit key2
	}
	wantURI := []string{"key1", "key1", "", "", "key2", "key2"}
	for i, seg := range p.Segments {
		if seg.Key.Method != wantMethod[i] {
			t.Errorf("segment %d: method = %s, want %s", i, seg.Key.Method, wantMethod[i])
		}
		if seg.Key.URI != wantURI[i] {
			t.Errorf("segment %d: key URI = %q, want %q", i, seg.Key.URI, wantURI[i])
		}
	}

	// NONE must serialise without URI/IV attributes.
	if got := p.Segments[2].Key.String(); got != "#EXT-X-KEY:METHOD=NONE" {
		t.Errorf("NONE key text = %q, want #EXT-X-KEY:METHOD=NONE", got)
	}
}

func TestMapInheritance(t *testing.T) {
	// A single EXT-X-MAP initialises every following segment until a
	// new map is declared.
	const playlist = `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init1.mp4"
#EXTINF:6.000,
seg1.m4s
#EXTINF:6.000,
seg2.m4s
#EXT-X-MAP:URI="init2.mp4",BYTERANGE="128@0"
#EXTINF:6.000,
seg3.m4s
#EXTINF:6.000,
seg4.m4s
`
	p := decodeString(t, playlist)
	if len(p.Segments) != 4 {
		t.Fatalf("want 4 segments, got %d", len(p.Segments))
	}
	wantMapURI := []string{"init1.mp4", "init1.mp4", "init2.mp4", "init2.mp4"}
	for i, seg := range p.Segments {
		if seg.Map == nil {
			t.Fatalf("segment %d: missing effective map", i)
		}
		if seg.Map.URI != wantMapURI[i] {
			t.Errorf("segment %d: map URI = %q, want %q", i, seg.Map.URI, wantMapURI[i])
		}
	}
	last := p.Segments[3].Map
	if last.ByteRange != (ByteRange{128, 0}) {
		t.Errorf("segment 3: map byte range = %v, want {128 0}", last.ByteRange)
	}
}

func TestDiscontinuityScopedToNextSegment(t *testing.T) {
	// Each EXT-X-DISCONTINUITY marks only the segment immediately
	// following it and must never propagate to later segments.
	const playlist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXTINF:6.000,
seg1.ts
#EXT-X-DISCONTINUITY
#EXTINF:6.000,
seg2.ts
#EXTINF:6.000,
seg3.ts
#EXT-X-DISCONTINUITY
#EXTINF:6.000,
seg4.ts
`
	p := decodeString(t, playlist)
	want := []bool{false, true, false, true}
	if len(p.Segments) != len(want) {
		t.Fatalf("want %d segments, got %d", len(want), len(p.Segments))
	}
	for i, seg := range p.Segments {
		if seg.Discontinuity != want[i] {
			t.Errorf("segment %d: discontinuity = %v, want %v", i, seg.Discontinuity, want[i])
		}
	}
}

func TestDiscontinuityDoesNotResetKeyOrMap(t *testing.T) {
	// A discontinuity changes the media timeline but does not clear
	// the inherited key or init section.
	const playlist = `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4"
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0x00000000000000000000000000000001
#EXTINF:6.000,
seg1.ts
#EXT-X-DISCONTINUITY
#EXTINF:6.000,
seg2.ts
#EXTINF:6.000,
seg3.ts
`
	p := decodeString(t, playlist)
	for i, seg := range p.Segments {
		if seg.Key == nil || seg.Key.URI != "key1" {
			t.Errorf("segment %d: discontinuity must not reset key, got %v", i, seg.Key)
		}
		if seg.Map == nil || seg.Map.URI != "init.mp4" {
			t.Errorf("segment %d: discontinuity must not reset map, got %v", i, seg.Map)
		}
	}
	if !p.Segments[1].Discontinuity {
		t.Error("segment 2 should be marked as the discontinuity boundary")
	}
	if p.Segments[2].Discontinuity {
		t.Error("the discontinuity must not carry over to segment 3")
	}
}

func TestEncodeMergesRepeatedKeyAndMap(t *testing.T) {
	// After inheritance every segment holds an effective key/map. The
	// encoder must write each tag only where the value changes rather
	// than repeating it for every segment.
	const playlist = `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4"
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0x00000000000000000000000000000001
#EXTINF:6.000,
seg1.ts
#EXTINF:6.000,
seg2.ts
#EXT-X-KEY:METHOD=NONE
#EXTINF:6.000,
seg3.ts
#EXTINF:6.000,
seg4.ts
`
	p := decodeString(t, playlist)
	var buf bytes.Buffer
	if err := Encode(&buf, p); err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := buf.String()
	if n := strings.Count(out, tagKey+":METHOD=AES-128"); n != 1 {
		t.Errorf("AES-128 key written %d times, want 1\n%s", n, out)
	}
	if n := strings.Count(out, tagKey+":METHOD=NONE"); n != 1 {
		t.Errorf("NONE key written %d times, want 1\n%s", n, out)
	}
	if n := strings.Count(out, tagMap+":"); n != 1 {
		t.Errorf("map written %d times, want 1\n%s", n, out)
	}
	if n := strings.Count(out, tagDiscontinuity); n != 0 {
		t.Errorf("unexpected discontinuity tags: %d", n)
	}
}

func TestEncodeDeterministic(t *testing.T) {
	const playlist = `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4"
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0x00000000000000000000000000000001
#EXTINF:6.000,
seg1.ts
#EXT-X-DISCONTINUITY
#EXT-X-KEY:METHOD=NONE
#EXTINF:6.000,
seg2.ts
`
	p := decodeString(t, playlist)
	var first string
	for i := 0; i < 5; i++ {
		var buf bytes.Buffer
		if err := Encode(&buf, p); err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		if first == "" {
			first = buf.String()
		} else if buf.String() != first {
			t.Fatalf("encode iteration %d differs from the first", i)
		}
	}
}

// TestRoundTripTestdata decodes each representative testdata playlist,
// re-encodes it, decodes the result and asserts that every segment keeps
// the same key, map and discontinuity assignment.
func TestRoundTripTestdata(t *testing.T) {
	files := []string{
		"testdata/discontinuities.m3u8",   // many key switches + METHOD=NONE + discontinuities
		"testdata/media_x_byterange.m3u8", // EXT-X-MAP and byte ranges
		"testdata/big_buck_bunny.m3u8",    // plain VOD, no key/map
		"testdata/segment_titles.m3u8",    // segment titles
	}
	for _, name := range files {
		t.Run(filepath.Base(name), func(t *testing.T) {
			f, err := os.Open(name)
			if err != nil {
				t.Fatal(err)
			}
			orig, err := Decode(f)
			f.Close()
			if err != nil {
				t.Fatalf("decode %s: %v", name, err)
			}
			var buf bytes.Buffer
			if err := Encode(&buf, orig); err != nil {
				t.Fatalf("encode %s: %v", name, err)
			}
			round, err := Decode(&buf)
			if err != nil {
				t.Fatalf("re-decode %s: %v\n%s", name, err, buf.String())
			}
			if len(round.Segments) != len(orig.Segments) {
				t.Fatalf("segment count changed: %d -> %d", len(orig.Segments), len(round.Segments))
			}
			for i := range orig.Segments {
				a, b := orig.Segments[i], round.Segments[i]
				if !keyEqual(a.Key, b.Key) {
					t.Errorf("segment %d key changed: %v -> %v", i, a.Key, b.Key)
				}
				if !mapEqual(a.Map, b.Map) {
					t.Errorf("segment %d map changed: %v -> %v", i, a.Map, b.Map)
				}
				if a.Discontinuity != b.Discontinuity {
					t.Errorf("segment %d discontinuity changed: %v -> %v", i, a.Discontinuity, b.Discontinuity)
				}
				if a.URI != b.URI {
					t.Errorf("segment %d URI changed: %q -> %q", i, a.URI, b.URI)
				}
			}
		})
	}
}

// TestDecodeErrors covers malformed input that must produce a non-nil
// error and must not leak a partially built playlist.
func TestDecodeErrors(t *testing.T) {
	cases := map[string]string{
		"key missing method": `#EXTM3U
#EXT-X-KEY:URI="key1"
#EXTINF:6.000,
seg1.ts
`,
		"iv wrong length": `#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0x010203
#EXTINF:6.000,
seg1.ts
`,
		"iv not hex": `#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0xnothex
#EXTINF:6.000,
seg1.ts
`,
		"byterange missing length": `#EXTM3U
#EXT-X-BYTERANGE:@100
#EXTINF:6.000,
seg1.ts
`,
		"segment missing uri": `#EXTM3U
#EXTINF:6.000,
`,
		"segment missing duration": `#EXTM3U
seg1.ts
`,
		"map byte range missing length": `#EXTM3U
#EXT-X-MAP:URI="init.mp4",BYTERANGE="@0"
#EXTINF:6.000,
seg1.ts
`,
		"trailing key without segment": `#EXTM3U
#EXTINF:6.000,
seg1.ts
#EXT-X-KEY:METHOD=AES-128,URI="key1"`,
		"trailing extinf without uri": `#EXTM3U
#EXTINF:6.000,
`,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			p, err := Decode(strings.NewReader(in))
			if err == nil {
				if p == nil {
					t.Fatal("expected a non-nil error, got nil error and nil playlist")
				}
				t.Fatalf("expected error, got playlist with %d segments", len(p.Segments))
			}
			if p != nil {
				t.Fatalf("partial playlist must not be returned on error, got %d segments: %v", len(p.Segments), err)
			}
		})
	}
}

func TestParseKeyRequiresMethod(t *testing.T) {
	// METHOD is a required attribute; its absence is an error even when
	// other attributes are present.
	_, err := Decode(strings.NewReader("#EXTM3U\n#EXT-X-KEY:URI=\"key1\"\n#EXTINF:6.000,\na.ts\n"))
	if err == nil {
		t.Fatal("expected error for key without METHOD")
	}
	if !strings.Contains(err.Error(), "METHOD") {
		t.Fatalf("error should mention missing METHOD, got: %v", err)
	}
}

func TestConcurrentDecodeIndependence(t *testing.T) {
	const playlist = `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4"
#EXT-X-KEY:METHOD=AES-128,URI="key1",IV=0x00000000000000000000000000000001
#EXTINF:6.000,
seg1.ts
#EXTINF:6.000,
seg2.ts
#EXT-X-KEY:METHOD=NONE
#EXT-X-DISCONTINUITY
#EXTINF:6.000,
seg3.ts
`
	const n = 64
	var wg sync.WaitGroup
	results := make([]*Playlist, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = Decode(strings.NewReader(playlist))
		}(i)
	}
	wg.Wait()

	base := results[0]
	if errs[0] != nil || base == nil {
		t.Fatalf("decode 0 failed: %v", errs[0])
	}
	for i := 1; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("decode %d: %v", i, errs[i])
		}
		// No two decodes may share key/map pointers, otherwise one
		// playlist's state could pollute another.
		for j := range base.Segments {
			if base.Segments[j].Key == results[i].Segments[j].Key {
				t.Fatalf("decode %d segment %d shares a key pointer with decode 0", i, j)
			}
			if base.Segments[j].Map == results[i].Segments[j].Map {
				t.Fatalf("decode %d segment %d shares a map pointer with decode 0", i, j)
			}
		}
	}

	// Mutating one decoded playlist must not be visible to the others.
	results[0].Segments[0].Key.URI = "mutated"
	results[0].Segments[0].Map.URI = "mutated-init"
	for i := 1; i < n; i++ {
		if results[i].Segments[0].Key.URI == "mutated" {
			t.Fatalf("decode %d polluted by a mutation in decode 0", i)
		}
		if results[i].Segments[0].Map.URI == "mutated-init" {
			t.Fatalf("decode %d map polluted by a mutation in decode 0", i)
		}
	}
}

func TestRoundTripPreservesDurationAndTitle(t *testing.T) {
	const playlist = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXTINF:9.967,first segment
seg1.ts
#EXTINF:10.000,
seg2.ts
`
	p := decodeString(t, playlist)
	var buf bytes.Buffer
	if err := Encode(&buf, p); err != nil {
		t.Fatal(err)
	}
	round := decodeString(t, buf.String())
	if len(round.Segments) != 2 {
		t.Fatalf("want 2 segments, got %d", len(round.Segments))
	}
	wantDur := []time.Duration{9967 * time.Millisecond, 10 * time.Second}
	wantTitle := []string{"first segment", ""}
	for i := range round.Segments {
		if round.Segments[i].Duration != wantDur[i] {
			t.Errorf("segment %d duration = %v, want %v", i, round.Segments[i].Duration, wantDur[i])
		}
		if round.Segments[i].Title != wantTitle[i] {
			t.Errorf("segment %d title = %q, want %q", i, round.Segments[i].Title, wantTitle[i])
		}
	}
}
