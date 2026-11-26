package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newRefreshUpstream builds a media source that rejects the old address/old
// cookies with 403 and serves playlists, segments and rangeable MP4 bytes only
// for the refreshed address/cookies.
func newRefreshUpstream(t *testing.T, payload []byte) (*httptest.Server, *atomic.Int32, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var refreshHits, oldPathHits, newPathHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/old"):
			oldPathHits.Add(1)
			http.Error(w, "expired", http.StatusForbidden)
			return
		case r.URL.Path == "/fresh/master.m3u8":
			newPathHits.Add(1)
			if r.Header.Get("Cookie") != "sid=fresh" {
				http.Error(w, "bad cookie", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nmedia.m3u8\n#EXT-X-ENDLIST\n")
		case r.URL.Path == "/fresh/media.m3u8":
			newPathHits.Add(1)
			if r.Header.Get("Cookie") != "sid=fresh" {
				http.Error(w, "bad cookie", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg0.ts\n#EXT-X-ENDLIST\n")
		case r.URL.Path == "/fresh/seg0.ts":
			newPathHits.Add(1)
			if r.Header.Get("Cookie") != "sid=fresh" {
				http.Error(w, "missing cookie on segment", http.StatusForbidden)
				return
			}
			_, _ = w.Write([]byte("SEGMENT-BYTES"))
		case r.URL.Path == "/fresh/movie.mp4":
			newPathHits.Add(1)
			if r.Header.Get("Cookie") != "sid=fresh" {
				http.Error(w, "bad cookie", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "video/mp4")
			http.ServeContent(w, r, "movie.mp4", time.Unix(0, 0), bytes.NewReader(payload))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &refreshHits, &oldPathHits, &newPathHits
}

// TestRefreshCoheresPlaylistSegmentsAndPrefetch: a source that first answers
// 403 and only works after refresh must make the playlist rewrite, every
// segment and every prefetch chunk use the same new address and headers.
func TestRefreshCoheresPlaylistSegmentsAndPrefetch(t *testing.T) {
	payload := make([]byte, 3*1024*1024+777) // spans several 1 MiB prefetch chunks
	_, _ = rand.Read(payload)
	upstream, refreshHits, oldPathHits, newPathHits := newRefreshUpstream(t, payload)

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	hlsRefresh := func(context.Context) (string, map[string]string, error) {
		refreshHits.Add(1)
		return upstream.URL + "/fresh/master.m3u8", map[string]string{"Cookie": "sid=fresh"}, nil
	}
	mp4Refresh := func(context.Context) (string, map[string]string, error) {
		refreshHits.Add(1)
		return upstream.URL + "/fresh/movie.mp4", map[string]string{"Cookie": "sid=fresh"}, nil
	}

	// 1. HLS session: master 403 -> one refresh -> fresh master, child
	// playlist and segment all flow through the same session route.
	hlsID, hlsURL := p.RegisterSession(SessionConfig{
		URL:      upstream.URL + "/old/master.m3u8",
		Headers:  map[string]string{"Cookie": "sid=old"},
		Filename: "show.m3u8",
		Refresh:  hlsRefresh,
	})

	resp, err := http.Get(hlsURL)
	if err != nil {
		t.Fatal(err)
	}
	master, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("master status=%d body=%s", resp.StatusCode, master)
	}
	var mediaURL string
	for _, line := range strings.Split(string(master), "\n") {
		if strings.HasPrefix(line, "http://") {
			mediaURL = strings.TrimSpace(line)
		}
	}
	if !strings.Contains(mediaURL, "/s/"+hlsID+"/sub?url=") {
		t.Fatalf("child playlist left the session route: %s", mediaURL)
	}
	if !strings.Contains(mediaURL, url.QueryEscape(upstream.URL+"/fresh/media.m3u8")) {
		t.Fatalf("child playlist still points at the old address: %s", mediaURL)
	}

	mediaResp, err := http.Get(mediaURL)
	if err != nil {
		t.Fatal(err)
	}
	mediaBody, _ := io.ReadAll(mediaResp.Body)
	mediaResp.Body.Close()
	if mediaResp.StatusCode != http.StatusOK {
		t.Fatalf("media playlist status=%d body=%s", mediaResp.StatusCode, mediaBody)
	}
	var segURL string
	for _, line := range strings.Split(string(mediaBody), "\n") {
		if strings.HasPrefix(line, "http://") {
			segURL = strings.TrimSpace(line)
		}
	}
	segResp, err := http.Get(segURL)
	if err != nil {
		t.Fatal(err)
	}
	segData, _ := io.ReadAll(segResp.Body)
	segResp.Body.Close()
	if segResp.StatusCode != http.StatusOK || string(segData) != "SEGMENT-BYTES" {
		t.Fatalf("segment status=%d data=%q", segResp.StatusCode, segData)
	}
	if got := refreshHits.Load(); got != 1 {
		t.Fatalf("HLS path triggered %d refreshes, want 1", got)
	}
	if got := oldPathHits.Load(); got != 1 {
		t.Fatalf("expected exactly one attempt against the old address, got %d", got)
	}
	if newPathHits.Load() < 3 {
		t.Fatalf("fresh address not used by playlist + segment, hits=%d", newPathHits.Load())
	}

	// 2. Prefetch MP4 session: first chunk 403 -> one refresh -> all chunks
	// use the new address with the new cookie and reassemble the full file.
	mp4ID, mp4URL := p.RegisterSession(SessionConfig{
		URL:      upstream.URL + "/old/movie.mp4",
		Headers:  map[string]string{"Cookie": "sid=old"},
		Filename: "movie.mp4",
		Prefetch: true,
		Refresh:  mp4Refresh,
	})
	mp4Resp, err := http.Get(mp4URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(mp4Resp.Body)
	mp4Resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if mp4Resp.StatusCode != http.StatusOK || !bytes.Equal(got, payload) {
		t.Fatalf("prefetched MP4 mismatch: status=%d len=%d want=%d", mp4Resp.StatusCode, len(got), len(payload))
	}
	if _, found := p.session(mp4ID); !found {
		t.Fatal("mp4 session vanished")
	}
	if got := refreshHits.Load(); got != 2 {
		t.Fatalf("total refreshes=%d, want exactly one per session (2)", got)
	}
}

// TestEnsureRefreshIsSingleFlight: concurrent callers on an expired session
// must collapse onto exactly one in-flight Refresh call, all receive the same
// coherent result, and a failure leaves the previous snapshot untouched.
func TestEnsureRefreshIsSingleFlight(t *testing.T) {
	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var refreshHits atomic.Int32
	leaderInRefresh := make(chan struct{})
	release := make(chan struct{})
	id, _ := p.RegisterSession(SessionConfig{
		URL:     "http://example.invalid/old.mp4",
		Headers: map[string]string{"Cookie": "sid=old"},
		Refresh: func(context.Context) (string, map[string]string, error) {
			refreshHits.Add(1)
			close(leaderInRefresh)
			<-release
			return "http://example.invalid/fresh.mp4", map[string]string{"Cookie": "sid=fresh"}, nil
		},
	})
	sess, found := p.session(id)
	if !found {
		t.Fatal("session missing")
	}

	// Start the leader and wait until its refresh is definitively in flight.
	leaderResult := make(chan refreshResult, 1)
	go func() { leaderResult <- sess.ensureRefresh() }()
	<-leaderInRefresh

	// Now launch the joiners. Because inflight is already set, each must park
	// on the same channel rather than starting its own refresh.
	const joiners = 15
	var wg sync.WaitGroup
	results := make([]refreshResult, joiners)
	for i := range joiners {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = sess.ensureRefresh()
		}(i)
	}
	// Let the joiners reach and park on the in-flight channel.
	time.Sleep(50 * time.Millisecond)
	if got := refreshHits.Load(); got != 1 {
		close(release)
		t.Fatalf("in-flight refresh duplicated: %d callbacks", got)
	}

	close(release)
	wg.Wait()

	lead := <-leaderResult
	if lead.outcome != refreshSucceeded || lead.url != "http://example.invalid/fresh.mp4" ||
		lead.headers["Cookie"] != "sid=fresh" || lead.version != 1 {
		t.Fatalf("leader got incoherent result: %+v", lead)
	}
	if got := refreshHits.Load(); got != 1 {
		t.Fatalf("concurrent callers caused %d refreshes, want 1", got)
	}
	for i, res := range results {
		if res.outcome != refreshSucceeded || res.url != lead.url ||
			res.headers["Cookie"] != "sid=fresh" || res.version != lead.version {
			t.Fatalf("joiner %d got incoherent result: %+v", i, res)
		}
	}
	if target, headers, version := sess.snapshot(); target != "http://example.invalid/fresh.mp4" ||
		headers["Cookie"] != "sid=fresh" || version != 1 {
		t.Fatalf("session not coherently swapped: %q %v v%d", target, headers, version)
	}

	// A failing refresh while cooling down must not invoke the callback again
	// and must leave the successful snapshot in place.
	failID, _ := p.RegisterSession(SessionConfig{
		URL:     "http://example.invalid/a.mp4",
		Headers: map[string]string{"Cookie": "sid=a"},
		Refresh: func(context.Context) (string, map[string]string, error) {
			refreshHits.Add(1)
			return "", nil, errors.New("refresh endpoint down")
		},
	})
	failSess, _ := p.session(failID)
	before := refreshHits.Load()
	// First call runs the refresh and fails; the snapshot stays untouched.
	first := failSess.ensureRefresh()
	if first.outcome != refreshFailed {
		t.Fatalf("failing refresh outcome=%v, want refreshFailed", first.outcome)
	}
	// Further calls within the cooldown must not invoke the callback again;
	// they return the cooldown result and keep the same snapshot.
	for range 5 {
		if res := failSess.ensureRefresh(); res.outcome != refreshCooling {
			t.Fatalf("cooldown outcome=%v, want refreshCooling", res.outcome)
		}
	}
	if got := refreshHits.Load() - before; got != 1 {
		t.Fatalf("repeated failed refreshes invoked the callback %d times, want 1 (cooldown)", got)
	}
	if target, headers, version := failSess.snapshot(); target != "http://example.invalid/a.mp4" ||
		headers["Cookie"] != "sid=a" || version != 0 {
		t.Fatalf("failed refresh mutated the session: %q %v v%d", target, headers, version)
	}
}

// TestCooldownRatesLimitRepeatedHTTP403: through the real HTTP path, repeated
// expired responses after a failed refresh call the Refresh callback once and
// pass the upstream 403 through on the rest instead of hammering upstream.
func TestCooldownRatesLimitRepeatedHTTP403(t *testing.T) {
	var refreshHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "expired", http.StatusForbidden)
	}))
	defer upstream.Close()

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id, localURL := p.RegisterSession(SessionConfig{
		URL:      upstream.URL + "/old/movie.mp4",
		Headers:  map[string]string{"Cookie": "sid=old"},
		Filename: "movie.mp4",
		Refresh: func(context.Context) (string, map[string]string, error) {
			refreshHits.Add(1)
			return "", nil, errors.New("refresh endpoint down")
		},
	})
	for range 4 {
		r, err := http.Get(localURL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	if got := refreshHits.Load(); got != 1 {
		t.Fatalf("repeated 403 invoked the refresh callback %d times, want 1 (cooldown)", got)
	}
	// The old address stays mounted and keeps answering (here, with 403)
	// rather than the session disappearing into a half-refreshed state.
	if _, found := p.session(id); !found {
		t.Fatal("failed-refresh session was removed; old address must stay mounted")
	}
}

// TestRefreshFailureKeepsOldAddressUsable: a failed refresh must not swap
// anything. The session keeps the old snapshot and version, passes the
// upstream 403 through, and resumes normally once the upstream recovers.
func TestRefreshFailureKeepsOldAddressUsable(t *testing.T) {
	var expired atomic.Bool
	expired.Store(true)
	var refreshCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/media.mp4" {
			if expired.Load() {
				http.Error(w, "expired", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", "video/mp4")
			_, _ = w.Write([]byte("RECOVERED-BYTES"))
			return
		}
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id, localURL := p.RegisterSession(SessionConfig{
		URL:     upstream.URL + "/media.mp4",
		Headers: map[string]string{"Cookie": "sid=old"},
		Refresh: func(context.Context) (string, map[string]string, error) {
			refreshCalls.Add(1)
			return "", nil, errors.New("refresh failed")
		},
	})
	sess, found := p.session(id)
	if !found {
		t.Fatal("session missing")
	}
	_, headersBefore, versionBefore := sess.snapshot()

	resp, err := http.Get(localURL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected upstream 403 to pass through, got %d: %s", resp.StatusCode, body)
	}
	targetAfter, headersAfter, versionAfter := sess.snapshot()
	if targetAfter != upstream.URL+"/media.mp4" || versionAfter != versionBefore {
		t.Fatalf("failed refresh mutated the session: target=%q version %d->%d", targetAfter, versionBefore, versionAfter)
	}
	if headersAfter["Cookie"] != headersBefore["Cookie"] {
		t.Fatal("failed refresh changed headers")
	}

	// Old address stays mounted and becomes usable when the upstream recovers.
	expired.Store(false)
	resp2, err := http.Get(localURL)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK || string(got) != "RECOVERED-BYTES" {
		t.Fatalf("old address not usable after recovery: status=%d body=%q", resp2.StatusCode, got)
	}
}

// TestRemoveSessionStopsPrefetchAndReturnsNotFound: removing a session cancels
// its in-flight upstream work, later requests get an explicit 404, and a cached
// playlist for the removed session can never be served.
func TestRemoveSessionStopsPrefetchAndReturnsNotFound(t *testing.T) {
	payload := make([]byte, 4*1024*1024)
	_, _ = rand.Read(payload)
	cancelled := make(chan struct{}, 16)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Record when the proxy abandons an upstream request for this session.
		context.AfterFunc(r.Context(), func() {
			if r.Context().Err() != nil {
				select {
				case cancelled <- struct{}{}:
				default:
				}
			}
		})
		w.Header().Set("Content-Type", "video/mp4")
		http.ServeContent(w, r, "movie.mp4", time.Unix(0, 0), bytes.NewReader(payload))
	}))
	defer upstream.Close()

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id, localURL := p.RegisterSession(SessionConfig{
		URL:      upstream.URL + "/movie.mp4",
		Filename: "movie.mp4",
		Prefetch: true,
	})

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		r, err := http.Get(localURL)
		if err != nil {
			return
		}
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)
	}()
	// Wait for the prefetch to establish an upstream connection.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(cancelled) > 0 || p.BytesServed() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	p.RemoveSession(id)

	// An upstream request for this session must be cancelled when it is removed.
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight upstream work was not cancelled when the session was removed")
	}
	// The player-side stream also ends rather than continuing to pull data.
	select {
	case <-clientDone:
	case <-time.After(2 * time.Second):
		t.Fatal("player stream kept running after session removal")
	}

	// Every later request to the removed session is an explicit 404.
	gone, err := http.Get(localURL)
	if err != nil {
		t.Fatal(err)
	}
	gone.Body.Close()
	if gone.StatusCode != http.StatusNotFound {
		t.Fatalf("removed session status=%d, want 404", gone.StatusCode)
	}

	// Seed a playlist cache under a different session, remove it, and prove the
	// cache entry cannot be read even though the key would otherwise be valid.
	playlistUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, "#EXTM3U\n#EXTINF:4,\nseg.ts\n#EXT-X-ENDLIST\n")
	}))
	defer playlistUpstream.Close()
	pid, purl := p.RegisterSession(SessionConfig{URL: playlistUpstream.URL + "/vod.m3u8", Filename: "vod.m3u8"})
	pr, err := http.Get(purl)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, pr.Body)
	pr.Body.Close()
	p.RemoveSession(pid)
	pg, err := http.Get(purl)
	if err != nil {
		t.Fatal(err)
	}
	pg.Body.Close()
	if pg.StatusCode != http.StatusNotFound {
		t.Fatalf("removed playlist session status=%d, want 404", pg.StatusCode)
	}
}

// TestPrefetchStopsOnClientDisconnect: when the player abandons a prefetch
// mid-stream, remaining chunks stop and upstream connections are released.
func TestPrefetchStopsOnClientDisconnect(t *testing.T) {
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		defer inFlight.Add(-1)
		w.Header().Set("Content-Type", "video/mp4")
		http.ServeContent(w, r, "movie.mp4", time.Unix(0, 0), bytes.NewReader(make([]byte, 8*1024*1024)))
	}))
	defer upstream.Close()

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, localURL := p.RegisterSession(SessionConfig{
		URL:      upstream.URL + "/movie.mp4",
		Filename: "movie.mp4",
		Prefetch: true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, localURL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	// Read just the first chunk then abandon the stream.
	buf := make([]byte, 1024)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatal(err)
	}
	cancel()
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if inFlight.Load() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if inFlight.Load() != 0 {
		t.Fatalf("upstream connections still in flight after disconnect: %d", inFlight.Load())
	}
	if maxInFlight.Load() > prefetchWorkers {
		t.Fatalf("prefetch concurrency %d exceeded worker bound %d", maxInFlight.Load(), prefetchWorkers)
	}
}

// TestPrefetchAbortsOnUpstreamError: a chunk that errors must stop the whole
// prefetch instead of splicing partial data into the player's stream.
func TestPrefetchAbortsOnUpstreamError(t *testing.T) {
	const total = 4 * 1024 * 1024
	var chunkReqs atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := chunkReqs.Add(1)
		// First chunk (the probe) succeeds normally; a later chunk fails.
		if n >= 3 {
			http.Error(w, "upstream blew up", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		http.ServeContent(w, r, "movie.mp4", time.Unix(0, 0), bytes.NewReader(make([]byte, total)))
	}))
	defer upstream.Close()

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var refreshCalls atomic.Int32
	id, localURL := p.RegisterSession(SessionConfig{
		URL:      upstream.URL + "/movie.mp4",
		Filename: "movie.mp4",
		Prefetch: true,
		Refresh: func(context.Context) (string, map[string]string, error) {
			refreshCalls.Add(1)
			return "", nil, errors.New("refresh unavailable")
		},
	})

	resp, err := http.Get(localURL)
	if err != nil {
		t.Fatal(err)
	}
	// The proxy committed a 200 with the full Content-Length. It must NOT
	// deliver that full length from a truncated upstream: the body ends early
	// with an error rather than presenting incomplete data as complete.
	n, copyErr := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if copyErr == nil && n == total {
		t.Fatal("prefetch reported a complete stream despite an upstream chunk failure")
	}
	if n >= total {
		t.Fatalf("wrote %d bytes from a failing upstream, expected an early abort", n)
	}
	if got := refreshCalls.Load(); got != 0 {
		t.Fatalf("chunk 500 path should not refresh, got %d", got)
	}
	if _, found := p.session(id); !found {
		t.Fatal("erroring session was removed")
	}
}

// TestPrefetchFailedRefreshAbortsAndKeepsSession: when every prefetch chunk is
// rejected with 401/403 and the refresh callback fails, the prefetch must abort
// the remaining chunks, invoke the refresh only once, and leave the session
// mounted with its old address rather than a half-refreshed state.
func TestPrefetchFailedRefreshAbortsAndKeepsSession(t *testing.T) {
	var refreshCalls atomic.Int32
	expired := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "expired", http.StatusForbidden)
	}))
	defer expired.Close()

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	eid, eurl := p.RegisterSession(SessionConfig{
		URL:      expired.URL + "/movie.mp4",
		Filename: "expired.mp4",
		Prefetch: true,
		Refresh: func(context.Context) (string, map[string]string, error) {
			refreshCalls.Add(1)
			return "", nil, errors.New("refresh unavailable")
		},
	})
	er, err := http.Get(eurl)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, er.Body)
	er.Body.Close()
	if got := refreshCalls.Load(); got != 1 {
		t.Fatalf("failed prefetch refresh invoked %d times, want 1", got)
	}
	if _, found := p.session(eid); !found {
		t.Fatal("expired session was removed on failed refresh; it must stay mounted")
	}
	target, headers, version := p.sessions[eid].snapshot()
	if target != expired.URL+"/movie.mp4" || version != 0 {
		t.Fatalf("failed refresh mutated the session: %q v%d", target, version)
	}
	if headers["Cookie"] != "" {
		t.Fatalf("unexpected header mutation: %v", headers)
	}
}

// TestRefreshedPlaylistCacheIsNeverServedStale: a cached VOD playlist written
// against the old address must become unreachable after the refresh, even on a
// second request that would otherwise hit the cache.
func TestRefreshedPlaylistCacheIsNeverServedStale(t *testing.T) {
	var masterReqs atomic.Int32
	var mode atomic.Int32 // 0 = expired, 1 = refreshed
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/old/master.m3u8", "/fresh/master.m3u8":
			if r.URL.Path == "/old/master.m3u8" && mode.Load() == 0 {
				http.Error(w, "expired", http.StatusForbidden)
				return
			}
			masterReqs.Add(1)
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			base := "old"
			if r.URL.Path == "/fresh/master.m3u8" {
				base = "fresh"
			}
			_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXTINF:4,\n%s-seg.ts\n#EXT-X-ENDLIST\n", base)
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id, localURL := p.RegisterSession(SessionConfig{
		URL:     upstream.URL + "/old/master.m3u8",
		Headers: map[string]string{"Cookie": "sid=old"},
		Refresh: func(context.Context) (string, map[string]string, error) {
			mode.Store(1)
			return upstream.URL + "/fresh/master.m3u8", map[string]string{"Cookie": "sid=fresh"}, nil
		},
	})

	fetch := func() string {
		r, err := http.Get(localURL)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("playlist status=%d body=%s", r.StatusCode, b)
		}
		return string(b)
	}

	first := fetch()
	if !strings.Contains(first, "fresh-seg.ts") {
		t.Fatalf("refresh did not take effect on first fetch:\n%s", first)
	}
	// Second fetch must revalidate against the new version, not return any
	// entry that could reference the old address.
	second := fetch()
	if strings.Contains(second, "old-seg.ts") {
		t.Fatalf("stale cached playlist served after refresh:\n%s", second)
	}
	if !strings.Contains(second, "fresh-seg.ts") {
		t.Fatalf("second playlist lost the refreshed address:\n%s", second)
	}
	if _, found := p.session(id); !found {
		t.Fatal("session missing")
	}
}

// TestSessionCacheInvalidationPrefixesMatchKeys guards the prefix/key format
// coupling: the versioned session cache key must be removed by the
// invalidation prefixes used on refresh and session removal.
func TestSessionCacheInvalidationPrefixesMatchKeys(t *testing.T) {
	p, err := Start(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id := "deadbeef"
	keys := []string{
		sessionCacheKey("s", id, "http://x/a.m3u8", 0),
		sessionCacheKey("s", id, "http://x/a.m3u8", 1),
		sessionCacheKey("u", id, "http://x/b.m3u8", 3),
	}
	p.mu.Lock()
	for _, k := range keys {
		p.playlists[k] = cachedPlaylist{body: "stale", expires: time.Now().Add(time.Hour)}
	}
	// An unrelated session must be untouched by prefix invalidation.
	other := sessionCacheKey("s", "other1234", "http://x/c.m3u8", 0)
	p.playlists[other] = cachedPlaylist{body: "keep", expires: time.Now().Add(time.Hour)}
	p.mu.Unlock()

	p.invalidateSessionPlaylists(id)

	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, k := range keys {
		if _, ok := p.playlists[k]; ok {
			t.Fatalf("invalidation did not remove versioned key %q", k)
		}
	}
	if _, ok := p.playlists[other]; !ok {
		t.Fatal("prefix invalidation removed an unrelated session's playlist")
	}
}
