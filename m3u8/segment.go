package m3u8

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Media segment tags specified in RFC 8216 section 4.4.4.
const (
	tagSegmentDuration = "#EXTINF"
	tagByteRange       = "#EXT-X-BYTERANGE"
	tagDiscontinuity   = "#EXT-X-DISCONTINUITY"
	tagKey             = "#EXT-X-KEY"
	tagMap             = "#EXT-X-MAP"
	tagDateTime        = "#EXT-X-PROGRAM-DATE-TIME"
	tagGap             = "#EXT-X-GAP"
	tagBitrate         = "#EXT-X-BITRATE"
	tagPart            = "#EXT-X-PART"
	tagDateRange       = "#EXT-X-DATERANGE"
)

// parseSegment parses the tags belonging to a single media segment.
// The leading item is the first tag already read by the caller
// (one of the segment-level tags) and items is the shared lexer
// channel. Parsing stops at the segment's URL, leaving any later
// items for the caller.
func parseSegment(items chan item, leading item) (*Segment, error) {
	var seg Segment
	// handle processes a single lexer item. The leading item from the
	// caller is handled before reading the shared channel, so the
	// original lexer order is preserved without re-emitting items.
	handle := func(it item) (done bool, err error) {
		switch it.typ {
		case itemError:
			return true, errors.New(it.val)
		case itemNewline:
			return false, nil
		case itemURL:
			seg.URI = it.val
			if seg.Duration == 0 {
				return true, fmt.Errorf("segment %q: missing %s duration tag", seg.URI, tagSegmentDuration)
			}
			return true, nil
		}

		switch it.val {
		case tagSegmentDuration:
			it = <-items
			if it.typ != itemAttrName && it.typ != itemNumber {
				return true, fmt.Errorf("parse segment duration: unexpected %s: want attribute name or number", it)
			}
			dur, err := parseSegmentDuration(it.val)
			if err != nil {
				return true, fmt.Errorf("parse segment duration: %w", err)
			}
			seg.Duration = dur

			// check for the optional segment title
			it = <-items
			if it.typ == itemNewline {
				return false, nil
			} else if it.typ != itemComma {
				return true, fmt.Errorf("expected comma after segment duration, got %s", it)
			}
			it = <-items
			seg.Title = it.val

		case tagByteRange:
			it = <-items
			r, err := parseByteRange(it.val)
			if err != nil {
				return true, fmt.Errorf("parse byte range: %w", err)
			}
			seg.Range = r
		case tagDiscontinuity:
			// A discontinuity only marks the segment immediately
			// following the tag; it is never carried forward.
			seg.Discontinuity = true
		case tagKey:
			key, err := parseKey(items)
			if err != nil {
				return true, fmt.Errorf("parse key: %w", err)
			}
			seg.Key = &key
		case tagMap:
			m, err := parseMap(items)
			if err != nil {
				return true, fmt.Errorf("parse map: %w", err)
			}
			seg.Map = &m
		case tagDateTime:
			it = <-items
			t, err := time.Parse(rfc3339Milli, it.val)
			if err != nil {
				return true, fmt.Errorf("bad date time tag: %w", err)
			}
			seg.DateTime = t
		default:
			return true, fmt.Errorf("parsing %s unsupported", it)
		}
		return false, nil
	}

	if done, err := handle(leading); err != nil {
		return nil, err
	} else if done {
		return &seg, nil
	}
	for it := range items {
		done, err := handle(it)
		if err != nil {
			return nil, err
		}
		if done {
			return &seg, nil
		}
	}
	// The channel closed before we found a segment URI: the input
	// ends with an unclosed tag. This must be reported, not treated
	// as a complete segment.
	return nil, fmt.Errorf("segment %q: missing URI", seg.URI)
}

func parseSegmentDuration(s string) (time.Duration, error) {
	// Some numbers can be converted straight to ints, e.g.:
	// 	10
	// 	10.000
	// Others need to be converted from floating point, e.g:
	// 	9.967
	// Try the easiest paths first.
	if !strings.Contains(s, ".") {
		i, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		return time.Duration(i) * time.Second, nil
	}

	// 10.000
	before, after, _ := strings.Cut(s, ".")
	var allZeroes = true
	for r := range after {
		if r != '0' {
			allZeroes = false
		}
	}
	if allZeroes {
		i, err := strconv.Atoi(before)
		if err != nil {
			return 0, err
		}
		return time.Duration(i) * time.Second, nil
	}
	seconds, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0, err
	}
	// precision based on a 90KHz clock.
	microseconds := seconds * 1e6
	return time.Duration(microseconds) * time.Microsecond, nil
}

func parseKey(items chan item) (Key, error) {
	var key Key
	methodSet := false
	for it := range items {
		switch it.typ {
		case itemError:
			return key, errors.New(it.val)
		case itemNewline:
			if !methodSet {
				return key, fmt.Errorf("key tag missing required METHOD attribute")
			}
			return key, nil
		case itemComma:
			continue
		default:
			if it.typ != itemAttrName {
				return Key{}, fmt.Errorf("expected attribute name, got %s", it.val)
			}
		}
		v := <-items
		if v.typ != itemEquals {
			return key, fmt.Errorf("expected %q after %s, got %s", "=", it.typ, v)
		}

		switch it.val {
		case "METHOD":
			v = <-items
			key.Method = parseEncryptMethod(v.val)
			if key.Method == encryptMethodInvalid {
				return key, fmt.Errorf("bad encrypt method %q", v.val)
			}
			methodSet = true
		case "URI":
			v = <-items
			key.URI = strings.Trim(v.val, `"`)
		case "IV":
			v = <-items
			b, err := hex.DecodeString(strings.TrimPrefix(v.val, "0x"))
			if err != nil {
				return key, fmt.Errorf("parse initialisation vector: %w", err)
			}
			if len(b) != len(key.IV) {
				return key, fmt.Errorf("bad initialisation vector length %d, want %d bytes", len(b), len(key.IV))
			}
			copy(key.IV[:], b)
		case "KEYFORMAT":
			v = <-items
			key.Format = strings.Trim(v.val, `"`)
		case "KEYFORMATVERSIONS":
			v = <-items
			ss := strings.Split(strings.Trim(v.val, `"`), "/")
			key.FormatVersions = make([]uint32, len(ss))
			for i := range ss {
				n, err := strconv.Atoi(ss[i])
				if err != nil {
					return key, fmt.Errorf("parse key format version: %w", err)
				}
				key.FormatVersions[i] = uint32(n)
			}
		default:
			return key, fmt.Errorf("unexpected attribute %q", it.val)
		}
	}
	return key, fmt.Errorf("unexpected end of key tag")
}

func parseMap(items chan item) (Map, error) {
	var mmap Map
	for it := range items {
		switch it.typ {
		case itemError:
			return mmap, errors.New(it.val)
		case itemNewline:
			return mmap, nil
		case itemComma:
			continue
		}
		if it.typ != itemAttrName {
			return Map{}, fmt.Errorf("unexpected %s %q", it.typ, it.val)
		}
		attr := it.val
		it = <-items
		if it.typ != itemEquals {
			return Map{}, fmt.Errorf("expected %q after %s, got %q", "=", attr, it.val)
		}

		it = <-items
		switch attr {
		case "URI":
			mmap.URI = strings.Trim(it.val, `"`)
		case "BYTERANGE":
			r, err := parseByteRange(strings.Trim(it.val, `"`))
			if err != nil {
				return Map{}, fmt.Errorf("parse byte range: %w", err)
			}
			mmap.ByteRange = r
		default:
			return Map{}, fmt.Errorf("unexpected attribute %q", it.val)
		}
	}
	return Map{}, fmt.Errorf("unexpected end of map tag")
}

// resolveInheritance applies the HLS inheritance rules of RFC 8216
// sections 4.3.2.3 (EXT-X-KEY) and 4.3.2.5 (EXT-X-MAP) to every
// segment.
//
// A segment that does not declare its own key or map inherits a copy
// of the most recent value. A key with METHOD=NONE explicitly clears
// encryption for itself and every following segment until a new key
// appears. Each segment receives its own copy so later playlists
// decoded concurrently cannot share or mutate each other's state.
func resolveInheritance(segments []Segment) {
	var currentKey *Key
	var currentMap *Map
	for i := range segments {
		seg := &segments[i]
		switch {
		case seg.Key != nil:
			// An explicit key always replaces the current one.
			// METHOD=NONE is carried as an explicit value so the
			// segment can state that it is no longer encrypted.
			k := *seg.Key
			currentKey = &k
		case currentKey != nil:
			k := *currentKey
			seg.Key = &k
		}
		switch {
		case seg.Map != nil:
			m := *seg.Map
			currentMap = &m
		case currentMap != nil:
			m := *currentMap
			seg.Map = &m
		}
	}
}

// keyEqual reports whether two keys describe the same effective key.
func keyEqual(a, b *Key) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Method != b.Method || a.URI != b.URI || a.Format != b.Format || a.IV != b.IV {
		return false
	}
	if len(a.FormatVersions) != len(b.FormatVersions) {
		return false
	}
	for i := range a.FormatVersions {
		if a.FormatVersions[i] != b.FormatVersions[i] {
			return false
		}
	}
	return true
}

// mapEqual reports whether two maps describe the same EXT-X-MAP value.
func mapEqual(a, b *Map) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.URI == b.URI && a.ByteRange == b.ByteRange
}

// writeSegments writes each segment. EXT-X-KEY and EXT-X-MAP tags are
// only emitted where the effective value actually changes relative to
// the previous segment; repeated declarations are merged. A
// discontinuity tag is only written for the single segment it marks.
func writeSegments(w io.Writer, segments []Segment) (n int, err error) {
	var prevKey *Key
	var prevMap *Map
	for i, seg := range segments {
		b, err := marshalSegmentText(&seg, prevKey, prevMap)
		if err != nil {
			return n, fmt.Errorf("segment %d: %w", i, err)
		}
		nn, err := fmt.Fprintln(w, string(b))
		n += nn
		if err != nil {
			return n, err
		}
		if seg.Key != nil {
			prevKey = seg.Key
		}
		if seg.Map != nil {
			prevMap = seg.Map
		}
	}
	return n, nil
}

// marshalSegmentText renders one segment. Inherited key and map tags
// are only written when their effective value differs from the
// previous segment's value (tracked through prevKey and prevMap).
// This keeps an inherited key or map from being written for every
// segment.
func marshalSegmentText(seg *Segment, prevKey *Key, prevMap *Map) ([]byte, error) {
	if seg.URI == "" {
		return nil, fmt.Errorf("empty URI")
	}
	if seg.Duration == 0 {
		return nil, fmt.Errorf("zero duration")
	}
	var tags []string
	if seg.Discontinuity {
		tags = append(tags, tagDiscontinuity)
	}
	if seg.DateRange != nil {
		buf := &bytes.Buffer{}
		if err := writeDateRange(buf, seg.DateRange); err != nil {
			return nil, fmt.Errorf("write date range: %w", err)
		}
		tags = append(tags, buf.String())
	}
	if seg.Range != [2]int{0, 0} {
		if seg.Range[0] <= 0 {
			return nil, fmt.Errorf("invalid byte range: length must be larger than zero, got %d", seg.Range[0])
		}
		tags = append(tags, fmt.Sprintf("%s:%s", tagByteRange, seg.Range))
	}
	// Only write the key tag when the effective key changed. A nil
	// effective key means no encryption has been signalled yet.
	if seg.Key != nil && !keyEqual(seg.Key, prevKey) {
		tags = append(tags, seg.Key.String())
	}
	if seg.Map != nil && !mapEqual(seg.Map, prevMap) {
		tags = append(tags, seg.Map.String())
	}
	if !seg.DateTime.IsZero() {
		tags = append(tags, fmt.Sprintf("%s:%s", tagDateTime, seg.DateTime.Format(rfc3339Milli)))
	}
	us := seg.Duration / time.Microsecond
	// we do .03f for the same precision as test-streams.mux.dev.
	durTag := fmt.Sprintf("%s:%.03f", tagSegmentDuration, float32(us)/1e6)
	if seg.Title != "" {
		durTag += "," + seg.Title
	}
	tags = append(tags, durTag)
	tags = append(tags, seg.URI)
	return []byte(strings.Join(tags, "\n")), nil
}

// MarshalText renders a segment to the HLS text format. Key and map
// tags are always written when set; callers writing a full playlist
// should use [Encode] which merges repeated declarations.
func (seg *Segment) MarshalText() ([]byte, error) {
	if seg.URI == "" {
		return nil, fmt.Errorf("empty URI")
	}
	if seg.Duration == 0 {
		return nil, fmt.Errorf("zero duration")
	}
	var tags []string
	if seg.Discontinuity {
		tags = append(tags, tagDiscontinuity)
	}
	if seg.DateRange != nil {
		buf := &bytes.Buffer{}
		if err := writeDateRange(buf, seg.DateRange); err != nil {
			return nil, fmt.Errorf("write date range: %w", err)
		}
		tags = append(tags, buf.String())
	}
	if seg.Range != [2]int{0, 0} {
		if seg.Range[0] <= 0 {
			return nil, fmt.Errorf("invalid byte range: length must be larger than zero, got %d", seg.Range[0])
		}
		tags = append(tags, fmt.Sprintf("%s:%s", tagByteRange, seg.Range))
	}
	if seg.Key != nil {
		tags = append(tags, seg.Key.String())
	}
	if seg.Map != nil {
		tags = append(tags, seg.Map.String())
	}
	if !seg.DateTime.IsZero() {
		tags = append(tags, fmt.Sprintf("%s:%s", tagDateTime, seg.DateTime.Format(rfc3339Milli)))
	}
	us := seg.Duration / time.Microsecond
	// we do .03f for the same precision as test-streams.mux.dev.
	durTag := fmt.Sprintf("%s:%.03f", tagSegmentDuration, float32(us)/1e6)
	if seg.Title != "" {
		durTag += "," + seg.Title
	}
	tags = append(tags, durTag)
	tags = append(tags, seg.URI)
	return []byte(strings.Join(tags, "\n")), nil
}
