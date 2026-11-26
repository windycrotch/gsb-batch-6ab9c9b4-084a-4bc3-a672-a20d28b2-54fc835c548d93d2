package dash

import (
	"errors"
	"strings"
	"sync"
	"testing"

	manifestor "github.com/datvietvac-techhub/manifestor"
)

// multiPeriodMPD has two Periods, each with one video AdaptationSet whose
// representation uses a relative BaseURL.
const multiPeriodMPD = `<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" profiles="urn:mpeg:dash:profile:isoff-on-demand:2011" type="static" mediaPresentationDuration="PT60S" minBufferTime="PT2S">
  <Period id="p1">
    <AdaptationSet contentType="video" mimeType="video/mp4">
      <Representation id="r1" bandwidth="1000000" codecs="avc1.42001e" width="640" height="360">
        <BaseURL>video/p1.mp4</BaseURL>
      </Representation>
    </AdaptationSet>
  </Period>
  <Period id="p2">
    <AdaptationSet contentType="video" mimeType="video/mp4">
      <Representation id="r2" bandwidth="1000000" codecs="avc1.42001e" width="640" height="360">
        <BaseURL>video/p2.mp4</BaseURL>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>`

// signOnceTracker records how many times each input URL reached the signer and
// fails the test if the same input is signed more than once within a call.
// It is safe for concurrent use.
type signOnceTracker struct {
	mu     sync.Mutex
	counts map[string]int
}

func newSignOnceTracker() *signOnceTracker {
	return &signOnceTracker{counts: map[string]int{}}
}

func (s *signOnceTracker) sign(u string) string {
	s.mu.Lock()
	s.counts[u]++
	s.mu.Unlock()
	sep := "?"
	if strings.Contains(u, "?") {
		sep = "&"
	}
	return u + sep + "sig=x"
}

func (s *signOnceTracker) snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}

// TestFilter_InjectedSet_FullTransformPipeline verifies that an injected
// AdaptationSet's relative BaseURL gets the exact same treatment as an origin
// representation: absolute resolution, CDN host rewrite, auth token, then a
// single signer invocation.
func TestFilter_InjectedSet_FullTransformPipeline(t *testing.T) {
	tracker := newSignOnceTracker()
	out, err := Filter(multiPeriodMPD,
		WithAbsoluteURIs("https://s3.example.com/bucket/"),
		WithCDNBaseURL("https://cdn.example.com"),
		WithAuthToken("tok123"),
		WithURISigner(tracker.sign),
		WithInjectAdaptationSet(AdaptationSetParams{
			MimeType: "audio/mp4",
			Lang:     "vi",
			Name:     "Vietnamese",
			Representations: []RepresentationParams{
				{ID: "a1", Bandwidth: 64000, BaseURL: "audio/vi/a.mp4"},
			},
		}),
	)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}

	wantInjected := "https://cdn.example.com/bucket/audio/vi/a.mp4?token=tok123&amp;sig=x"
	if !strings.Contains(out, wantInjected) {
		t.Fatalf("injected BaseURL not fully transformed/signed:\n%s", out)
	}
	wantOrigin := "https://cdn.example.com/bucket/video/p1.mp4?token=tok123&amp;sig=x"
	if !strings.Contains(out, wantOrigin) {
		t.Fatalf("origin BaseURL not transformed/signed:\n%s", out)
	}
	// No raw relative BaseURL must survive.
	if strings.Contains(out, ">audio/vi/a.mp4<") {
		t.Errorf("relative injected BaseURL leaked into output:\n%s", out)
	}

	// Each distinct URL reaches the signer exactly once per Period it appears
	// in; the origin per-period URLs are distinct, the injected URL appears in
	// two Periods but as two independent representations.
	for u, n := range tracker.snapshot() {
		switch {
		case strings.HasSuffix(u, "audio/vi/a.mp4?token=tok123"):
			if n != 2 { // once per Period
				t.Errorf("injected URL signed %d times, want 2 (one per Period): %s", n, u)
			}
		case strings.HasSuffix(u, "video/p1.mp4?token=tok123"), strings.HasSuffix(u, "video/p2.mp4?token=tok123"):
			if n != 1 {
				t.Errorf("origin URL signed %d times, want 1: %s", n, u)
			}
		default:
			t.Errorf("unexpected signed URL: %s", u)
		}
	}

	// Every emitted BaseURL must contain exactly one signature marker.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "<BaseURL>") {
			if got := strings.Count(line, "sig=x"); got != 1 {
				t.Errorf("BaseURL line has %d signatures, want 1: %s", got, strings.TrimSpace(line))
			}
		}
	}
}

// TestFilter_InjectedSet_AppearsInEveryPeriod requires the injected
// AdaptationSet to be present in every Period of a multi-Period MPD.
func TestFilter_InjectedSet_AppearsInEveryPeriod(t *testing.T) {
	out, err := Filter(multiPeriodMPD,
		WithInjectAdaptationSet(AdaptationSetParams{
			ContentType: "text",
			MimeType:    "text/vtt",
			Lang:        "vi",
			Roles:       []Role{{SchemeIDURI: "urn:mpeg:dash:role:2011", Value: "subtitle"}},
			Representations: []RepresentationParams{
				{ID: "sub-vi", Bandwidth: 16, BaseURL: "https://static.example.com/subs/vi.vtt"},
			},
		}),
	)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	m, err := Parse(out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if len(m.Periods) != 2 {
		t.Fatalf("Periods = %d, want 2", len(m.Periods))
	}
	for pi, p := range m.Periods {
		var found bool
		for _, as := range p.AdaptationSets {
			if as.Lang == "vi" && as.MimeType == "text/vtt" {
				found = true
				if len(as.Roles) != 1 || as.Roles[0].Value != "subtitle" {
					t.Errorf("Period %d injected subtitle Role = %+v", pi, as.Roles)
				}
				if len(as.Representations) != 1 || as.Representations[0].ID != "sub-vi" {
					t.Errorf("Period %d injected reps = %+v", pi, as.Representations)
				}
			}
		}
		if !found {
			t.Errorf("Period %d missing injected subtitle AdaptationSet", pi)
		}
	}
}

// TestFilter_InjectedSet_DoesNotCountAsSurvivingVariant pins the semantics:
// when filtering kills every origin representation, ErrNoVariantsRemain is
// still returned even though an injected AdaptationSet was supplied.
func TestFilter_InjectedSet_DoesNotCountAsSurvivingVariant(t *testing.T) {
	_, err := Filter(multiPeriodMPD,
		WithCodec(manifestor.AV1), // no av1 reps in the fixture
		WithInjectAdaptationSet(AdaptationSetParams{
			MimeType: "audio/mp4",
			Lang:     "vi",
			Representations: []RepresentationParams{
				{ID: "a1", Bandwidth: 64000, BaseURL: "https://cdn.example.com/a.mp4"},
			},
		}),
	)
	if !errors.Is(err, ErrNoVariantsRemain) {
		t.Fatalf("got %v, want ErrNoVariantsRemain (injected sets must not keep a dead MPD alive)", err)
	}
}

// TestFilter_InjectedSet_ConcurrentIsolation runs many filters over the same
// content with different inject/transform parameters concurrently. Results for
// identical option sets must be byte-for-byte equal, and requests with
// different parameters must never see each other's injected sets.
func TestFilter_InjectedSet_ConcurrentIsolation(t *testing.T) {
	const workers = 32
	const iterations = 20

	baseOpts := []Option{
		WithAbsoluteURIs("https://s3.example.com/bucket/"),
		WithCDNBaseURL("https://cdn.example.com"),
		WithURISigner(func(u string) string { return u + "?sig=x" }),
	}

	mkOpts := func(lang, id string) []Option {
		opts := append([]Option{}, baseOpts...)
		return append(opts, WithInjectAdaptationSet(AdaptationSetParams{
			MimeType: "audio/mp4",
			Lang:     lang,
			Representations: []RepresentationParams{
				{ID: id, Bandwidth: 64000, BaseURL: "audio/" + id + "/a.mp4"},
			},
		}))
	}

	// Reference outputs computed serially.
	refA, err := Filter(multiPeriodMPD, mkOpts("vi", "a-vi")...)
	if err != nil {
		t.Fatal(err)
	}
	refB, err := Filter(multiPeriodMPD, mkOpts("tm", "a-tm")...)
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
					got, err := Filter(multiPeriodMPD, mkOpts("vi", "a-vi")...)
					if err != nil {
						errCh <- err
						return
					}
					if got != refA {
						errCh <- errors.New("non-deterministic or polluted result for param set A")
						return
					}
					if strings.Contains(got, "a-tm") {
						errCh <- errors.New("request A saw request B's injected track")
						return
					}
				} else {
					got, err := Filter(multiPeriodMPD, mkOpts("tm", "a-tm")...)
					if err != nil {
						errCh <- err
						return
					}
					if got != refB {
						errCh <- errors.New("non-deterministic or polluted result for param set B")
						return
					}
					if strings.Contains(got, "a-vi") {
						errCh <- errors.New("request B saw request A's injected track")
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

// TestFilter_InjectedSet_SignerOnlyOnceWithToken combines token + signer and
// verifies the signer never sees an already-signed URL (no double signing) for
// either origin or injected representations.
func TestFilter_InjectedSet_SignerOnlyOnceWithToken(t *testing.T) {
	var signedInputs []string
	var mu sync.Mutex
	signer := func(u string) string {
		mu.Lock()
		signedInputs = append(signedInputs, u)
		mu.Unlock()
		if strings.Contains(u, "sig=x") {
			t.Errorf("signer received an already-signed URL (double sign): %s", u)
		}
		return u + "&sig=x"
	}
	out, err := Filter(multiPeriodMPD,
		WithAbsoluteURIs("https://s3.example.com/bucket/"),
		WithAuthToken("tok"),
		WithURISigner(signer),
		WithInjectAdaptationSet(AdaptationSetParams{
			MimeType: "audio/mp4",
			Lang:     "vi",
			Representations: []RepresentationParams{
				{ID: "a1", Bandwidth: 64000, BaseURL: "audio/a.mp4"},
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(out, "sig=x") != 4 { // 2 origin + 2 injected (2 Periods)
		t.Errorf("expected 4 signatures in output, got %d:\n%s", strings.Count(out, "sig=x"), out)
	}
}
