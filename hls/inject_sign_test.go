package hls

import (
	"errors"
	"strings"
	"sync"
	"testing"

	manifestor "github.com/datvietvac-techhub/manifestor"
)

// injectMaster is a small HLS master playlist with relative variant and media
// URIs used by the inject/sign cross-state tests.
const injectMaster = `#EXTM3U
#EXT-X-VERSION:6
#EXT-X-STREAM-INF:BANDWIDTH=1000000,CODECS="avc1.4d401e",RESOLUTION=640x360
360p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2000000,CODECS="avc1.640028",RESOLUTION=1280x720
720p.m3u8
`

// TestFilter_InjectedTracks_FullTransformPipeline verifies injected audio and
// subtitle URIs get the same absolute-resolution, CDN, token, and signer
// treatment as origin variant URIs — and each URI is signed exactly once.
func TestFilter_InjectedTracks_FullTransformPipeline(t *testing.T) {
	var signed []string
	var mu sync.Mutex
	signer := func(u string) string {
		mu.Lock()
		signed = append(signed, u)
		mu.Unlock()
		if strings.Contains(u, "sig=") {
			t.Errorf("signer received an already-signed URL (double sign): %s", u)
		}
		return u + "&sig=x"
	}

	out, err := Filter(injectMaster,
		WithAbsoluteURIs("https://s3.example.com/bucket/"),
		WithCDNBaseURL("https://cdn.example.com"),
		WithAuthToken("tok123"),
		WithURISigner(signer),
		WithInjectAudioTrack(AudioTrackParams{
			GroupID: "aud-vi", Name: "Vietnamese", Language: "vi",
			URI: "audio/vi/index.m3u8", Default: true, AutoSelect: true,
		}),
		WithInjectSubtitle(SubtitleTrackParams{
			GroupID: "sub-vi", Name: "Vietnamese", Language: "vi",
			URI: "subs/vi/index.m3u8",
		}),
		WithVariantSubtitleGroup("sub-vi"),
	)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}

	want := []string{
		"https://cdn.example.com/bucket/audio/vi/index.m3u8?token=tok123&sig=x",
		"https://cdn.example.com/bucket/subs/vi/index.m3u8?token=tok123&sig=x",
		"https://cdn.example.com/bucket/360p.m3u8?token=tok123&sig=x",
		"https://cdn.example.com/bucket/720p.m3u8?token=tok123&sig=x",
	}
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("missing fully-transformed URI %q in:\n%s", w, out)
		}
	}
	// Exactly one signature per emitted URI line.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "sig=x") && strings.Count(line, "sig=x") != 1 {
			t.Errorf("line signed more than once: %s", line)
		}
	}
	// Every surviving variant must reference the injected subtitle group.
	p, err := Parse(out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if len(p.Subtitles) != 1 || p.Subtitles[0].GroupID != "sub-vi" {
		t.Fatalf("injected subtitle missing: %+v", p.Subtitles)
	}
	for _, v := range p.Variants {
		if v.SubtitleGroupID != "sub-vi" {
			t.Errorf("variant %q SubtitleGroupID = %q, want sub-vi", v.URI, v.SubtitleGroupID)
		}
	}
}

// TestFilter_InjectedVariant_WiredToSubtitleGroup ensures an injected variant
// is also attached to the active subtitle group instead of orphaning it.
func TestFilter_InjectedVariant_WiredToSubtitleGroup(t *testing.T) {
	out, err := Filter(injectMaster,
		WithInjectSubtitle(SubtitleTrackParams{
			GroupID: "subs", Name: "EN", Language: "en", URI: "en.m3u8",
		}),
		WithVariantSubtitleGroup("subs"),
		WithInjectVariant(VariantParams{
			URI:       "extra-4k.m3u8",
			Bandwidth: 8000000,
			Width:     3840,
			Height:    2160,
		}),
	)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	p, err := Parse(out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	var injected *Variant
	for i := range p.Variants {
		if strings.Contains(p.Variants[i].URI, "extra-4k.m3u8") {
			injected = &p.Variants[i]
		}
	}
	if injected == nil {
		t.Fatal("injected variant missing from output")
	}
	if injected.SubtitleGroupID != "subs" {
		t.Errorf("injected variant SubtitleGroupID = %q, want subs", injected.SubtitleGroupID)
	}
}

// TestFilter_InjectedTracks_DoNotCountAsSurvivingVariant pins the 422
// semantics: an injected track cannot keep a playlist whose origin variants
// were all filtered out alive.
func TestFilter_InjectedTracks_DoNotCountAsSurvivingVariant(t *testing.T) {
	_, err := Filter(injectMaster,
		WithCodec(manifestor.AV1), // no av1 variants in fixture
		WithInjectAudioTrack(AudioTrackParams{
			GroupID: "a", Name: "vi", Language: "vi", URI: "vi.m3u8",
		}),
		WithInjectSubtitle(SubtitleTrackParams{
			GroupID: "s", Name: "vi", Language: "vi", URI: "vi-subs.m3u8",
		}),
	)
	if !errors.Is(err, ErrNoVariantsRemain) {
		t.Fatalf("got %v, want ErrNoVariantsRemain (injected tracks must not count as variants)", err)
	}
}

// TestFilter_InjectedTracks_ConcurrentIsolation runs many concurrent filters
// over the same content with different inject parameters. Identical parameter
// sets must produce byte-identical output, and each request must only see its
// own injected tracks.
func TestFilter_InjectedTracks_ConcurrentIsolation(t *testing.T) {
	const workers = 32
	const iterations = 20

	mkOpts := func(group, lang string) []Option {
		return []Option{
			WithAbsoluteURIs("https://s3.example.com/bucket/"),
			WithCDNBaseURL("https://cdn.example.com"),
			WithURISigner(func(u string) string { return u + "?sig=x" }),
			WithInjectAudioTrack(AudioTrackParams{
				GroupID: group, Name: strings.ToUpper(lang), Language: lang,
				URI: "audio/" + lang + "/index.m3u8",
			}),
		}
	}

	refA, err := Filter(injectMaster, mkOpts("g-vi", "vi")...)
	if err != nil {
		t.Fatal(err)
	}
	refB, err := Filter(injectMaster, mkOpts("g-tm", "tm")...)
	if err != nil {
		t.Fatal(err)
	}
	if refA == refB {
		t.Fatal("reference outputs for different params must differ")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if id%2 == 0 {
					got, err := Filter(injectMaster, mkOpts("g-vi", "vi")...)
					if err != nil {
						errCh <- err
						return
					}
					if got != refA {
						errCh <- errors.New("non-deterministic or polluted HLS result for param set A")
						return
					}
					if strings.Contains(got, "audio/tm/") {
						errCh <- errors.New("HLS request A saw request B's injected audio")
						return
					}
				} else {
					got, err := Filter(injectMaster, mkOpts("g-tm", "tm")...)
					if err != nil {
						errCh <- err
						return
					}
					if got != refB {
						errCh <- errors.New("non-deterministic or polluted HLS result for param set B")
						return
					}
					if strings.Contains(got, "audio/vi/") {
						errCh <- errors.New("HLS request B saw request A's injected audio")
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestFilter_InjectedTracks_SignerInvokedOncePerURI uses a counting signer to
// prove every injected and origin URI reaches the signer exactly once.
func TestFilter_InjectedTracks_SignerInvokedOncePerURI(t *testing.T) {
	counts := map[string]int{}
	var mu sync.Mutex
	signer := func(u string) string {
		mu.Lock()
		counts[u]++
		mu.Unlock()
		return u + "?sig=x"
	}
	out, err := Filter(injectMaster,
		WithAbsoluteURIs("https://s3.example.com/bucket/"),
		WithURISigner(signer),
		WithInjectAudioTrack(AudioTrackParams{
			GroupID: "a", Name: "vi", Language: "vi", URI: "audio/vi.m3u8",
		}),
		WithInjectSubtitle(SubtitleTrackParams{
			GroupID: "s", Name: "vi", Language: "vi", URI: "subs/vi.m3u8",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	for u, n := range counts {
		if n != 1 {
			t.Errorf("URI %q signed %d times, want 1", u, n)
		}
	}
	// 2 variants + 1 audio + 1 subtitle = 4 distinct signed URIs.
	if len(counts) != 4 {
		t.Errorf("signed URI count = %d, want 4. Output:\n%s", len(counts), out)
	}
}
