package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/arixbit/xiaomu-desktop/internal/httpx"
)

// refreshCooldown bounds how often one session may invoke its Refresh
// callback. Concurrent 401/403 responses collapse onto the in-flight refresh
// (single-flight); after an attempt, further failures wait for the cooldown
// instead of hammering the refresh endpoint again.
const refreshCooldown = 5 * time.Second

// SessionConfig configures an isolated loopback stream endpoint (/s/<id>/<filename>)
// with its own headers, optional range-prefetch acceleration and URL refresh callback.
type SessionConfig struct {
	URL      string
	Headers  map[string]string
	Filename string
	Prefetch bool
	Refresh  func(ctx context.Context) (string, map[string]string, error)
}

type sessionHeadersKey struct{}

// refreshOutcome is what a caller blocked on ensureRefresh receives.
type refreshOutcome int

const (
	refreshNotConfigured refreshOutcome = iota // session has no Refresh callback
	refreshCooling                             // an attempt happened too recently; current state stands
	refreshSucceeded                           // url/headers now hold the fresh snapshot
	refreshFailed                              // the callback failed; the previous snapshot stays valid
)

type refreshResult struct {
	outcome refreshOutcome
	url     string
	headers map[string]string
	version int64
}

// refreshCall is one in-flight refresh. The leader writes the result and then
// closes done, which releases every waiter at once regardless of how many
// joined, so no caller can be left parked on a spent channel.
type refreshCall struct {
	done chan struct{}
	res  refreshResult
}

type proxySession struct {
	mu        sync.RWMutex
	targetURL string
	headers   map[string]string
	filename  string
	prefetch  bool
	refresh   func(ctx context.Context) (string, map[string]string, error)

	// version bumps on every successful refresh so in-flight work can tell
	// a stale snapshot apart from the session's current address/headers.
	version int64
	// lastAttempt is set after every refresh attempt, success or failure,
	// and drives the cooldown that rate-limits repeated failures.
	lastAttempt time.Time
	// inflight, if non-nil, is the single in-flight refresh. Every other
	// 401/403 waits on its done channel instead of starting its own refresh.
	inflight *refreshCall

	// ctx is cancelled by RemoveSession so in-flight prefetches and upstream
	// connections belonging to a removed session release immediately.
	ctx    context.Context
	cancel context.CancelCauseFunc
	closed bool
}

// RegisterSession mounts a dedicated local route that keeps its own headers and
// preserves the filename extension.
func (p *Proxy) RegisterSession(cfg SessionConfig) (id, localURL string) {
	var raw [8]byte
	_, _ = rand.Read(raw[:])
	id = hex.EncodeToString(raw[:])

	name := sanitizeFilename(cfg.Filename, cfg.URL)
	ctx, cancel := context.WithCancelCause(context.Background())
	s := &proxySession{
		targetURL: strings.TrimSpace(cfg.URL),
		headers:   clone(cfg.Headers),
		filename:  name,
		prefetch:  cfg.Prefetch,
		refresh:   cfg.Refresh,
		ctx:       ctx,
		cancel:    cancel,
	}
	p.mu.Lock()
	if p.sessions == nil {
		p.sessions = map[string]*proxySession{}
	}
	p.sessions[id] = s
	p.mu.Unlock()
	return id, p.base + "/s/" + id + "/" + url.PathEscape(name)
}

// RemoveSession unmounts a registered session, drops every playlist cached for
// it and cancels its lifecycle context so in-flight prefetch chunks and
// upstream connections stop. Later requests receive 404.
func (p *Proxy) RemoveSession(id string) {
	if id == "" {
		return
	}
	p.mu.Lock()
	sess := p.sessions[id]
	delete(p.sessions, id)
	p.invalidatePlaylistsLocked("s:" + id + ":")
	p.invalidatePlaylistsLocked("u:" + id + ":")
	p.mu.Unlock()
	if sess != nil {
		sess.markRemoved()
	}
}

func (s *proxySession) markRemoved() {
	s.mu.Lock()
	wasClosed := s.closed
	s.closed = true
	s.mu.Unlock()
	if !wasClosed {
		s.cancel(errSessionRemoved)
	}
}

// errSessionRemoved is the cancellation cause for a session's lifecycle context.
var errSessionRemoved = errors.New("proxy: playback session removed")

// isRemoved reports whether the session has been unmounted.
func (s *proxySession) isRemoved() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// sessionContext returns a context that is cancelled when the request ends,
// the upstream idle timer fires (via doWithTimeout) or the session is removed.
func (p *Proxy) sessionContext(sess *proxySession, parent context.Context) context.Context {
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(sess.ctx, func() { cancel(context.Cause(sess.ctx)) })
	context.AfterFunc(ctx, func() { stop() })
	return ctx
}

func (p *Proxy) session(id string) (*proxySession, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.sessions[id]
	return s, ok
}

// snapshot returns the session's current target, headers and version.
func (s *proxySession) snapshot() (target string, headers map[string]string, version int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.targetURL, clone(s.headers), s.version
}

// ensureRefresh makes address/header refresh a single atomic operation for the
// whole session. Only one refresh runs at a time; concurrent callers share its
// result. A successful refresh swaps the target and headers together under the
// session lock and bumps the version. A failed refresh leaves the previous
// snapshot untouched, so the session can never end up half new, half old.
func (s *proxySession) ensureRefresh() refreshResult {
	if s.refresh == nil {
		t, h, v := s.snapshot()
		return refreshResult{outcome: refreshNotConfigured, url: t, headers: h, version: v}
	}

	s.mu.Lock()
	if s.closed {
		t, h, v := s.targetURL, clone(s.headers), s.version
		s.mu.Unlock()
		return refreshResult{outcome: refreshFailed, url: t, headers: h, version: v}
	}
	// A refresh is already running: join it. Closing done releases every
	// waiter at once with the same result, so no caller is stranded.
	if call := s.inflight; call != nil {
		s.mu.Unlock()
		<-call.done
		return call.res
	}
	if !s.lastAttempt.IsZero() && time.Since(s.lastAttempt) < refreshCooldown {
		t, h, v := s.targetURL, clone(s.headers), s.version
		s.mu.Unlock()
		return refreshResult{outcome: refreshCooling, url: t, headers: h, version: v}
	}
	call := &refreshCall{done: make(chan struct{})}
	s.inflight = call
	s.lastAttempt = time.Now()
	s.mu.Unlock()

	// This goroutine is the single leader: it runs the refresh, swaps the
	// target and headers together under the session lock, then closes done to
	// broadcast the result. Callers that arrive after inflight clears hit the
	// cooldown and reuse the just-installed snapshot — nobody observes a
	// half-swapped state. The refresh belongs to the session rather than the
	// triggering request, so a player disconnecting mid-refresh does not
	// strand in-flight requests; removing the session cancels it via sess.ctx.
	newURL, newHeaders, err := s.refresh(s.ctx)

	s.mu.Lock()
	outcome := refreshFailed
	if err == nil && strings.TrimSpace(newURL) != "" && !s.closed {
		s.targetURL = strings.TrimSpace(newURL)
		if newHeaders != nil {
			s.headers = clone(newHeaders)
		}
		s.version++
		outcome = refreshSucceeded
	}
	call.res = refreshResult{
		outcome: outcome,
		url:     s.targetURL,
		headers: clone(s.headers),
		version: s.version,
	}
	s.inflight = nil
	s.mu.Unlock()
	close(call.done)
	return call.res
}

// invalidateSessionPlaylists drops every cached playlist (both /s/ and /u?sid=
// routes) for a session after its address changes, so a playlist rewritten with
// the old address can never be served again.
func (p *Proxy) invalidateSessionPlaylists(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.invalidatePlaylistsLocked("s:" + id + ":")
	p.invalidatePlaylistsLocked("u:" + id + ":")
}

// sessionCacheKey names a playlist cache entry with the session's refresh
// version. After a successful refresh the version changes, so an entry written
// for the old address cannot be matched again even if invalidation races with
// an in-flight request. Callers re-check the version after a cache hit.
func sessionCacheKey(route, id, target string, version int64) string {
	return fmt.Sprintf("%s:%s:v%d|%s", route, id, version, target)
}

func (p *Proxy) serveSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	// Path format: /s/<id>/<filename> or /s/<id>/sub?url=<encoded>
	rest := strings.TrimPrefix(r.URL.Path, "/s/")
	id, tail, ok := strings.Cut(rest, "/")
	if !ok || id == "" {
		http.NotFound(w, r)
		return
	}
	sess, found := p.session(id)
	if !found || sess.isRemoved() {
		http.NotFound(w, r)
		return
	}

	target, headers, version := sess.snapshot()
	isSub := tail == "sub"
	if isSub {
		target = r.URL.Query().Get("url")
	}
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		http.Error(w, "url must be http or https", http.StatusBadRequest)
		return
	}

	isPreview := isPreviewRequest(r)
	reqLimit := time.Duration(0)
	if isPreview {
		reqLimit = previewIdleTimeout
	}
	cacheKey := sessionCacheKey("s", id, target, version)
	if r.Method == http.MethodGet {
		if cached, ok := p.cachedPlaylist(cacheKey); ok {
			// A refresh can land between the snapshot and the cache lookup.
			// Only serve the entry if the session is still on this version and
			// still mounted; otherwise fall through and fetch with the new
			// address/headers, never handing out a stale playlist.
			if curTarget, _, curVersion := sess.snapshot(); !sess.isRemoved() && curVersion == version && (isSub || curTarget == target) {
				w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, cached)
				return
			}
		}
	}

	isHLSPath := playlist("", u.Path) || playlist("", sess.filename)
	if r.Method == http.MethodGet && sess.prefetch && !isSub && !isHLSPath && !isPreview {
		if p.servePrefetch(w, r, sess, id, target, headers) {
			return
		}
	}

	reqCtx := p.sessionContext(sess, r.Context())
	resp, err := p.doSessionRequestWithTimeout(reqCtx, r.Method, target, headers, r.Header.Get("Range"), isHLSPath, reqLimit)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(context.Cause(reqCtx), errSessionRemoved) {
			return
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// A 401/403 means the direct link's auth expired mid-playback. Refresh
	// the session once and retry with the new, coherent address/headers.
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		res := sess.ensureRefresh()
		if sess.isRemoved() {
			httpx.DrainAndClose(resp.Body)
			return
		}
		if res.outcome == refreshSucceeded {
			httpx.DrainAndClose(resp.Body)
			p.invalidateSessionPlaylists(id)
			retryTarget := res.url
			if isSub {
				// Segments/keys keep their own URL; only the headers change.
				retryTarget = target
			}
			cacheKey = sessionCacheKey("s", id, retryTarget, res.version)
			resp, err = p.doSessionRequestWithTimeout(reqCtx, r.Method, retryTarget, res.headers, r.Header.Get("Range"), isHLSPath, reqLimit)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(context.Cause(reqCtx), errSessionRemoved) {
					return
				}
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			headers = res.headers
		}
		// On any other outcome (no callback, cooldown, failed refresh) the
		// original 401/403 response is passed through unchanged, keeping the
		// old address usable instead of entering a half-refreshed state.
	}
	defer httpx.DrainAndClose(resp.Body)

	base := u
	if resp.Request != nil && resp.Request.URL != nil {
		base = resp.Request.URL
	}
	if r.Method == http.MethodHead {
		copyMediaHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		copyMediaHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}
	wrapSub := func(raw string) string {
		return p.base + "/s/" + id + "/sub?url=" + url.QueryEscape(raw)
	}
	if playlist(resp.Header.Get("Content-Type"), base.Path) {
		p.writeSessionPlaylist(w, resp, base.String(), cacheKey, headers, wrapSub, reqCtx, reqLimit, sess, id)
		return
	}
	buf := make([]byte, 64)
	n, readErr := io.ReadFull(resp.Body, buf)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		http.Error(w, readErr.Error(), http.StatusBadGateway)
		return
	}
	if n > 0 && bytes.HasPrefix(bytes.TrimSpace(buf[:n]), []byte("#EXTM3U")) {
		if resp.StatusCode == http.StatusPartialContent {
			p.writeSessionPlaylist(w, resp, base.String(), cacheKey, headers, wrapSub, reqCtx, reqLimit, sess, id)
			return
		}
		restBody, _, err := httpx.ReadLimit(resp.Body, resp.ContentLength, playlistLimit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		body := append(append([]byte{}, buf[:n]...), restBody...)
		p.writeRewrittenWith(w, base.String(), cacheKey, body, wrapSub, sess)
		return
	}
	copyMediaHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if n > 0 {
		_, _ = w.Write(buf[:n])
	}
	rc := http.NewResponseController(w)
	if resp.Header.Get("Content-Length") == "" && rc.Flush() == nil {
		copyFlushing(w, resp.Body, rc)
		return
	}
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) doSessionRequest(ctx context.Context, method, target string, headers map[string]string, rangeHdr string, isHLS bool) (*http.Response, error) {
	return p.doSessionRequestWithTimeout(ctx, method, target, headers, rangeHdr, isHLS, 0)
}

func (p *Proxy) doSessionRequestWithTimeout(ctx context.Context, method, target string, headers map[string]string, rangeHdr string, isHLS bool, timeoutLimit time.Duration) (*http.Response, error) {
	reqCtx := context.WithValue(ctx, sessionHeadersKey{}, headers)
	req, err := http.NewRequestWithContext(reqCtx, method, target, nil)
	if err != nil {
		return nil, err
	}
	applyMap(req, headers)
	if rangeHdr != "" && !isHLS {
		req.Header.Set("Range", rangeHdr)
	}
	return p.doWithTimeout(req, timeoutLimit)
}

func (p *Proxy) writeSessionPlaylist(w http.ResponseWriter, resp *http.Response, playlistURL, cacheKey string, headers map[string]string, wrap func(string) string, ctx context.Context, timeoutLimit time.Duration, sess *proxySession, id string) {
	if resp.StatusCode == http.StatusPartialContent {
		reqCtx := context.WithValue(ctx, sessionHeadersKey{}, headers)
		req := resp.Request.Clone(reqCtx)
		req.Header.Del("Range")
		applyMap(req, headers)
		httpx.DrainAndClose(resp.Body)
		full, err := p.doWithTimeout(req, timeoutLimit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer httpx.DrainAndClose(full.Body)
		if full.StatusCode != http.StatusOK {
			http.Error(w, "upstream did not return a complete playlist", http.StatusBadGateway)
			return
		}
		resp = full
		playlistURL = full.Request.URL.String()
	}
	body, _, err := httpx.ReadLimit(resp.Body, resp.ContentLength, playlistLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	p.writeRewrittenWith(w, playlistURL, cacheKey, body, wrap, sess)
}

// writeRewrittenWith rewrites a playlist and caches it only while its session
// is still mounted. A playlist for a removed session is never stored, so stale
// segment addresses cannot be served later. The /u route passes a nil session.
func (p *Proxy) writeRewrittenWith(w http.ResponseWriter, playlistURL, cacheKey string, body []byte, wrap func(string) string, sess *proxySession) {
	if !bytes.HasPrefix(bytes.TrimSpace(body), []byte("#EXTM3U")) {
		http.Error(w, "upstream returned a page instead of an HLS playlist", http.StatusBadGateway)
		return
	}
	out := rewriteM3U8(string(body), playlistURL, wrap)
	p.storePlaylist(cacheKey, out, body, sess)
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, out)
}

func applyMap(req *http.Request, headers map[string]string) {
	for k, v := range headers {
		if v == "" || strings.EqualFold(k, "Host") {
			continue
		}
		req.Header.Set(k, v)
	}
}

func sanitizeFilename(filename, rawURL string) string {
	name := strings.TrimSpace(filename)
	if name == "" {
		if u, err := url.Parse(rawURL); err == nil {
			name = path.Base(u.Path)
		}
	}
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == "/" {
		return "stream.mp4"
	}
	return name
}
