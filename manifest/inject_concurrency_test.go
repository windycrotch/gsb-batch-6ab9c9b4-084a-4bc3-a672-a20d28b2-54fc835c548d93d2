package manifest

import (
	"errors"
	"strings"
	"sync"
	"testing"

	manifestor "github.com/datvietvac-techhub/manifestor"
	"github.com/datvietvac-techhub/manifestor/dash"
	"github.com/datvietvac-techhub/manifestor/hls"
)

// TestFilter_Unified_ConcurrentInjectIsolation_HLS exercises the format-
// agnostic Filter entry point with concurrent HLS requests carrying different
// injected audio tracks. Each request must only observe its own injection and
// repeated identical option sets must serialize byte-for-byte identically.
func TestFilter_Unified_ConcurrentInjectIsolation_HLS(t *testing.T) {
	content := mustReadFixture(t, "../testdata/hls/bento4_mixed_codecs.m3u8")

	mkOpts := func(lang, uri string) []Option {
		return []Option{
			WithAbsoluteURIs("https://s3.example.com/bucket/"),
			WithCDNBaseURL("https://cdn.example.com"),
			WithURISigner(func(u string) string { return u + "?sig=x" }),
			WithHLSInjectAudioTrack(hls.AudioTrackParams{
				GroupID: "grp-" + lang, Name: strings.ToUpper(lang), Language: lang, URI: uri,
			}),
		}
	}

	refA, err := Filter(content, mkOpts("vi", "audio/vi.m3u8")...)
	if err != nil {
		t.Fatal(err)
	}
	refB, err := Filter(content, mkOpts("tm", "audio/tm.m3u8")...)
	if err != nil {
		t.Fatal(err)
	}
	if refA == refB {
		t.Fatal("different inject params must produce different output")
	}

	const workers = 24
	const iterations = 15
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if id%2 == 0 {
					got, err := Filter(content, mkOpts("vi", "audio/vi.m3u8")...)
					if err != nil {
						errCh <- err
						return
					}
					if got != refA {
						errCh <- errors.New("unified HLS result A drifted or was polluted")
						return
					}
					if strings.Contains(got, "audio/tm.m3u8") {
						errCh <- errors.New("unified HLS request A observed request B injection")
						return
					}
				} else {
					got, err := Filter(content, mkOpts("tm", "audio/tm.m3u8")...)
					if err != nil {
						errCh <- err
						return
					}
					if got != refB {
						errCh <- errors.New("unified HLS result B drifted or was polluted")
						return
					}
					if strings.Contains(got, "audio/vi.m3u8") {
						errCh <- errors.New("unified HLS request B observed request A injection")
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

// TestFilter_Unified_DASHInjectedRelativeBaseURLIsSigned proves the unified
// option path also resolves and signs a relative BaseURL on an injected DASH
// AdaptationSet — the exact "injected track never gets a signature" failure
// mode reported in production.
func TestFilter_Unified_DASHInjectedRelativeBaseURLIsSigned(t *testing.T) {
	content := mustReadFixture(t, "../testdata/dash/bento4_mixed_codecs.mpd")

	out, err := Filter(content,
		WithAbsoluteURIs("https://s3.example.com/bucket/"),
		WithCDNBaseURL("https://cdn.example.com"),
		WithURISigner(func(u string) string {
			sep := "?"
			if strings.Contains(u, "?") {
				sep = "&"
			}
			return u + sep + "sig=x"
		}),
		WithDASHInjectAdaptationSet(dash.AdaptationSetParams{
			MimeType: "audio/mp4",
			Lang:     "vi",
			Representations: []dash.RepresentationParams{
				{ID: "injected-vi", Bandwidth: 128000, BaseURL: "dub/vi/audio.mp4"},
			},
		}),
	)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	want := "https://cdn.example.com/bucket/dub/vi/audio.mp4?sig=x"
	if !strings.Contains(out, want) {
		t.Fatalf("injected relative BaseURL not resolved, CDN-rewritten and signed:\n%s", out)
	}
	if strings.Contains(out, ">dub/vi/audio.mp4<") {
		t.Fatalf("raw relative injected BaseURL leaked into output:\n%s", out)
	}
	// Exactly one signature on the injected BaseURL line.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "dub/vi/audio.mp4") && strings.Count(line, "sig=x") != 1 {
			t.Fatalf("injected BaseURL signature count != 1: %s", strings.TrimSpace(line))
		}
	}
}

// TestFilter_Unified_DASHInjectionDoesNotPreventNoVariants verifies the
// unified API still surfaces ErrNoVariantsRemain when all origin
// representations are filtered, regardless of injected AdaptationSets.
func TestFilter_Unified_DASHInjectionDoesNotPreventNoVariants(t *testing.T) {
	mpd := `<?xml version="1.0"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static">
  <Period>
    <AdaptationSet mimeType="video/mp4" contentType="video">
      <Representation id="v1" bandwidth="1000000" codecs="avc1.64001F" width="640" height="360"/>
    </AdaptationSet>
  </Period>
</MPD>`
	_, err := Filter(mpd,
		WithCodec(manifestor.AV1),
		WithDASHInjectAdaptationSet(dash.AdaptationSetParams{
			MimeType: "audio/mp4",
			Lang:     "vi",
			Representations: []dash.RepresentationParams{
				{ID: "a1", Bandwidth: 128000, BaseURL: "https://cdn.example.com/a.mp4"},
			},
		}),
	)
	if !errors.Is(err, ErrNoVariantsRemain) {
		t.Fatalf("got %v, want ErrNoVariantsRemain", err)
	}
}
