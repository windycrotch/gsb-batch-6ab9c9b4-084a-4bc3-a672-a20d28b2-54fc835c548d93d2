package server_test

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/datvietvac-techhub/manifestor/manifest"
	"github.com/datvietvac-techhub/manifestor/server"
)

// b64enc base64-encodes a payload for the inject_* query parameters.
func b64enc(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// newSignedTestServer builds a proxy whose signer appends "&sig=<path>" (or
// "?sig=..." when the URL has no query) and records every signed input.
func newSignedTestServer(t *testing.T) (*httptest.Server, *countingSigner) {
	t.Helper()
	signer := &countingSigner{}
	srv := server.New(server.Config{URISigner: manifest.URISigner(signer.sign)})
	return httptest.NewServer(srv), signer
}

type countingSigner struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *countingSigner) sign(u string) string {
	c.mu.Lock()
	if c.counts == nil {
		c.counts = map[string]int{}
	}
	c.counts[u]++
	c.mu.Unlock()
	sep := "?"
	if strings.Contains(u, "?") {
		sep = "&"
	}
	return u + sep + "sig=h"
}

func (c *countingSigner) get(u string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[u]
}

func (c *countingSigner) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.counts {
		n += v
	}
	return n
}

func getBody(t *testing.T, url string) (int, string) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// ---- 4xx validation for malformed inject payloads ----

func TestHandleFilter_InjectAudio_MalformedJSON_Returns400(t *testing.T) {
	upstream := mockDASHUpstream(sampleDASH)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	// Valid base64, but the JSON is an object rather than an array of tracks.
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.mpd&inject_audio="+b64enc(`{"lang":"vi"}`))
	if code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-array inject_audio, got %d: %s", code, body)
	}
}

func TestHandleFilter_InjectSubtitle_EmptyTrack_Returns400(t *testing.T) {
	upstream := mockDASHUpstream(sampleDASH)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	// An entry with neither HLS nor DASH fields.
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.mpd&inject_subtitle="+b64enc(`[{}]`))
	if code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty inject_subtitle entry, got %d: %s", code, body)
	}
}

func TestHandleFilter_InjectAudio_HLS_MissingGroupID_Returns400(t *testing.T) {
	upstream := mockUpstream(sampleHLS)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	payload := b64enc(`[{"name":"vi","uri":"https://cdn.example.com/a.m3u8","lang":"vi"}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.m3u8&inject_audio="+payload)
	if code != http.StatusBadRequest {
		t.Errorf("expected 400 for HLS audio without group_id, got %d: %s", code, body)
	}
}

func TestHandleFilter_InjectSubtitle_HLS_MissingURI_Returns400(t *testing.T) {
	upstream := mockUpstream(sampleHLS)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	payload := b64enc(`[{"group_id":"subs","name":"vi","lang":"vi"}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.m3u8&inject_subtitle="+payload)
	if code != http.StatusBadRequest {
		t.Errorf("expected 400 for HLS subtitle without uri, got %d: %s", code, body)
	}
}

func TestHandleFilter_InjectAudio_DASH_BadRepresentation_Returns400(t *testing.T) {
	upstream := mockDASHUpstream(sampleDASH)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	cases := []string{
		// Missing id.
		`[{"mime_type":"audio/mp4","lang":"vi","representations":[{"bandwidth":128000,"base_url":"https://x/a.mp4"}]}]`,
		// Zero bandwidth.
		`[{"mime_type":"audio/mp4","lang":"vi","representations":[{"id":"a1","bandwidth":0,"base_url":"https://x/a.mp4"}]}]`,
		// Missing base_url.
		`[{"mime_type":"audio/mp4","lang":"vi","representations":[{"id":"a1","bandwidth":128000}]}]`,
		// Empty representations.
		`[{"mime_type":"audio/mp4","lang":"vi","representations":[]}]`,
	}
	for i, c := range cases {
		code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.mpd&inject_audio="+b64enc(c))
		if code != http.StatusBadRequest {
			t.Errorf("case %d: expected 400, got %d: %s", i, code, body)
		}
	}
}

func TestHandleFilter_Inject_InvalidLanguage_Returns400(t *testing.T) {
	upstream := mockDASHUpstream(sampleDASH)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	payload := b64enc(`[{"mime_type":"audio/mp4","lang":"not a tag!!","representations":[{"id":"a1","bandwidth":128000,"base_url":"https://x/a.mp4"}]}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.mpd&inject_audio="+payload)
	if code != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed BCP-47 lang, got %d: %s", code, body)
	}
}

// ---- Signing reaches injected tracks and signs each address exactly once ----

func TestHandleFilter_InjectDASH_SignedExactlyOnce(t *testing.T) {
	upstream := mockDASHUpstream(sampleDASH)
	defer upstream.Close()
	srv, signer := newSignedTestServer(t)
	defer srv.Close()

	payload := b64enc(`[{"lang":"vi","name":"Vietnamese","mime_type":"audio/mp4","representations":[{"id":"audio-vi","bandwidth":128000,"base_url":"https://cdn.example.com/audio-vi.mp4"}]}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.mpd&cdn=https://cdn.example.com&inject_audio="+payload)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", code, body)
	}
	if !strings.Contains(body, "audio-vi.mp4") {
		t.Fatalf("injected audio missing:\n%s", body)
	}
	// Every BaseURL in the output must carry exactly one signature.
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "<BaseURL>") && strings.Count(line, "sig=h") != 1 {
			t.Errorf("BaseURL not signed exactly once: %s", strings.TrimSpace(line))
		}
	}
	// The injected URL was signed (origin URLs derive from the httptest host).
	signedInjected := false
	for u := range signer.counts {
		if strings.Contains(u, "audio-vi.mp4") {
			signedInjected = true
			if signer.get(u) != 1 {
				t.Errorf("injected URL signed %d times, want 1: %s", signer.get(u), u)
			}
		}
	}
	if !signedInjected {
		t.Errorf("signer never received injected URL; signed inputs: %+v", signer.counts)
	}
}

func TestHandleFilter_InjectHLS_SignedExactlyOnce(t *testing.T) {
	upstream := mockUpstream(sampleHLS)
	defer upstream.Close()
	srv, signer := newSignedTestServer(t)
	defer srv.Close()

	payload := b64enc(`[{"group_id":"audio","lang":"vi","name":"Vietnamese","uri":"https://cdn.example.com/audio-vi.m3u8","default":true}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.m3u8&cdn=https://cdn.example.com&inject_audio="+payload)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", code, body)
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "http") && strings.Contains(line, "sig=h") && strings.Count(line, "sig=h") != 1 {
			t.Errorf("URI signed more than once: %s", line)
		}
	}
	found := false
	for u := range signer.counts {
		if strings.Contains(u, "audio-vi.m3u8") {
			found = true
			if signer.get(u) != 1 {
				t.Errorf("injected HLS audio signed %d times, want 1", signer.get(u))
			}
		}
	}
	if !found {
		t.Errorf("injected HLS audio never reached signer: %+v", signer.counts)
	}
}

func TestHandleFilter_InjectHLS_SubtitleGroupWiredToVariants(t *testing.T) {
	upstream := mockUpstream(sampleHLS)
	defer upstream.Close()
	srv, _ := newSignedTestServer(t)
	defer srv.Close()

	payload := b64enc(`[{"group_id":"subs","lang":"vi","name":"Vietnamese","uri":"https://cdn.example.com/sub-vi.m3u8"}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.m3u8&inject_subtitle="+payload)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", code, body)
	}
	if !strings.Contains(body, `TYPE=SUBTITLES`) || !strings.Contains(body, `SUBTITLES="subs"`) {
		t.Fatalf("subtitle group not wired to surviving variants:\n%s", body)
	}
	if !strings.Contains(body, "sub-vi.m3u8") || !strings.Contains(body, "sig=h") {
		t.Fatalf("injected subtitle URI missing or unsigned:\n%s", body)
	}
}

// ---- 422 semantics: injected tracks never keep an empty filter result alive ----

func TestHandleFilter_InjectDoesNotPrevent422_HLS(t *testing.T) {
	hevcOnly := `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=3000000,CODECS="hvc1.1.2.L120.90",RESOLUTION=1920x1080
video-hevc.m3u8
`
	upstream := mockUpstream(hevcOnly)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	payload := b64enc(`[{"group_id":"a","lang":"vi","name":"vi","uri":"https://cdn.example.com/a.m3u8"}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.m3u8&codec=h264&inject_audio="+payload)
	if code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d: %s", code, body)
	}
}

func TestHandleFilter_InjectDoesNotPrevent422_DASH(t *testing.T) {
	// Video-only MPD; av1 filter removes the single representation.
	mpd := `<?xml version="1.0"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static">
  <Period>
    <AdaptationSet mimeType="video/mp4" contentType="video">
      <Representation id="v1" bandwidth="1000000" codecs="avc1.64001F" width="640" height="360">
        <BaseURL>v.mp4</BaseURL>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>`
	upstream := mockDASHUpstream(mpd)
	defer upstream.Close()
	srv := newTestServer()
	defer srv.Close()

	payload := b64enc(`[{"mime_type":"audio/mp4","lang":"vi","representations":[{"id":"a1","bandwidth":128000,"base_url":"https://cdn.example.com/a.mp4"}]}]`)
	code, body := getBody(t, srv.URL+"/filter?url="+upstream.URL+"/test.mpd&codec=av1&inject_audio="+payload)
	if code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d: %s", code, body)
	}
}

// ---- Concurrency: same upstream, different inject params, no cross-talk ----

func TestHandleFilter_ConcurrentInjectIsolation(t *testing.T) {
	upstream := mockDASHUpstream(sampleDASH)
	defer upstream.Close()
	srv, _ := newSignedTestServer(t)
	defer srv.Close()

	mkURL := func(lang, id string) string {
		payload := b64enc(`[{"mime_type":"audio/mp4","lang":"` + lang + `","representations":[{"id":"` + id + `","bandwidth":128000,"base_url":"https://cdn.example.com/` + id + `.mp4"}]}]`)
		return srv.URL + "/filter?url=" + upstream.URL + "/test.mpd&inject_audio=" + payload
	}

	urlA := mkURL("vi", "audio-vi")
	urlB := mkURL("tm", "audio-tm")

	refAcode, refA := getBody(t, urlA)
	refBcode, refB := getBody(t, urlB)
	if refAcode != http.StatusOK || refBcode != http.StatusOK {
		t.Fatalf("reference requests failed: %d %d", refAcode, refBcode)
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
					code, body := getBody(t, urlA)
					if code != http.StatusOK || body != refA {
						errCh <- errConcurrentMismatch("A", code)
						return
					}
					if strings.Contains(body, "audio-tm") {
						errCh <- errConcurrentMismatch("A saw B track", code)
						return
					}
				} else {
					code, body := getBody(t, urlB)
					if code != http.StatusOK || body != refB {
						errCh <- errConcurrentMismatch("B", code)
						return
					}
					if strings.Contains(body, "audio-vi") {
						errCh <- errConcurrentMismatch("B saw A track", code)
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

type concurrencyError string

func (e concurrencyError) Error() string { return string(e) }

func errConcurrentMismatch(what string, code int) error {
	return concurrencyError("concurrent filter mismatch (" + what + "), status " + http.StatusText(code))
}
