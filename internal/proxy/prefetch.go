package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/arixbit/xiaomu-desktop/internal/httpx"
)

const (
	prefetchChunkSize = 1 << 20 // 1 MiB per ranged upstream chunk
	prefetchWorkers   = 3       // bounded concurrency to avoid rate-limit bans
	prefetchWindow    = 6       // max in-flight/buffered chunks (~6 MiB cap per stream)
)

// servePrefetch accelerates large non-HLS range streams (such as cloud-drive 4K MP4/MKV
// files throttled per connection) using bounded concurrent range reads.
// Returns true if the response was completely handled; false falls back to direct proxy.
//
// All chunks share the session's single address/header snapshot: when an
// upstream 401/403 triggers a refresh, the refresh is single-flight and every
// subsequent chunk uses the new URL and headers together. The stream context
// is cancelled when the player disconnects, the session is removed or any
// chunk fails, so remaining chunks stop and upstream connections are released.
func (p *Proxy) servePrefetch(w http.ResponseWriter, r *http.Request, sess *proxySession, id, target string, headers map[string]string) bool {
	reqStart, reqEnd, hasRange, ok := parseSingleByteRange(r.Header.Get("Range"))
	if !ok {
		return false
	}
	if !hasRange {
		reqStart, reqEnd = 0, -1
	}
	// Small explicit probes (e.g. player reading initial 64 KiB header or trailer) do not need chunking.
	if reqEnd >= 0 && (reqEnd-reqStart+1) <= prefetchChunkSize {
		return false
	}

	// Bind the whole prefetch to the session lifecycle: removing the session
	// cancels every in-flight chunk, and a player disconnect cancels it too.
	streamCtx, cancel := context.WithCancel(p.sessionContext(sess, r.Context()))
	defer cancel()

	firstEnd := reqStart + prefetchChunkSize - 1
	if reqEnd >= 0 && reqEnd < firstEnd {
		firstEnd = reqEnd
	}
	firstRange := fmt.Sprintf("bytes=%d-%d", reqStart, firstEnd)
	resp, err := p.doSessionRequest(streamCtx, http.MethodGet, target, headers, firstRange, false)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(context.Cause(streamCtx), errSessionRemoved) {
			return true
		}
		return false
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		httpx.DrainAndClose(resp.Body)
		res := sess.ensureRefresh()
		if sess.isRemoved() || streamCtx.Err() != nil {
			return true
		}
		if res.outcome != refreshSucceeded {
			// No new snapshot: fall through to direct proxy so the upstream
			// 401/403 reaches the player instead of a fabricated response.
			return false
		}
		p.invalidateSessionPlaylists(id)
		target, headers = res.url, res.headers
		resp, err = p.doSessionRequest(streamCtx, http.MethodGet, target, headers, firstRange, false)
		if err != nil {
			return errors.Is(err, context.Canceled) || errors.Is(context.Cause(streamCtx), errSessionRemoved)
		}
	}
	if resp.StatusCode != http.StatusPartialContent {
		httpx.DrainAndClose(resp.Body)
		return false
	}
	_, actualEnd, total, validCR := parseContentRange(resp.Header.Get("Content-Range"))
	if !validCR || total <= 0 {
		httpx.DrainAndClose(resp.Body)
		return false
	}

	// Read first chunk completely before committing headers so errors can still fall back cleanly.
	firstData, _, err := httpx.ReadLimit(resp.Body, firstEnd-reqStart+1, prefetchChunkSize)
	ct := resp.Header.Get("Content-Type")
	httpx.DrainAndClose(resp.Body)
	if err != nil {
		return errors.Is(err, context.Canceled) || errors.Is(context.Cause(streamCtx), errSessionRemoved)
	}

	finalEnd := total - 1
	if reqEnd >= 0 && reqEnd < finalEnd {
		finalEnd = reqEnd
	}
	if finalEnd < reqStart {
		return false
	}

	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(finalEnd-reqStart+1, 10))
	if hasRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", reqStart, finalEnd, total))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	if _, err := w.Write(firstData); err != nil {
		return true
	}
	nextOffset := actualEnd + 1
	if nextOffset > finalEnd {
		return true
	}

	type chunkRange struct {
		start int64
		end   int64
	}
	type chunkSlot struct {
		done chan struct{}
		data []byte
		err  error
	}

	jobs := make(chan struct {
		r    chunkRange
		slot *chunkSlot
	}, prefetchWindow)

	var wg sync.WaitGroup
	for range prefetchWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				// Every chunk reads the session's current snapshot. A refresh
				// swaps URL and headers atomically, so chunks can never mix an
				// old address with new headers (or vice versa).
				curTarget, curHeaders, _ := sess.snapshot()
				data, fetchErr := p.fetchChunk(streamCtx, sess, id, curTarget, curHeaders, job.r.start, job.r.end)
				job.slot.data, job.slot.err = data, fetchErr
				close(job.slot.done)
				if fetchErr != nil {
					cancel()
				}
			}
		}()
	}

	ordered := make(chan *chunkSlot, prefetchWindow)
	go func() {
		defer close(jobs)
		defer close(ordered)
		for off := nextOffset; off <= finalEnd; off += prefetchChunkSize {
			end := min(off+prefetchChunkSize-1, finalEnd)
			slot := &chunkSlot{done: make(chan struct{})}
			select {
			case <-streamCtx.Done():
				return
			case ordered <- slot:
			}
			select {
			case <-streamCtx.Done():
				slot.err = streamCtx.Err()
				close(slot.done)
				return
			case jobs <- struct {
				r    chunkRange
				slot *chunkSlot
			}{r: chunkRange{start: off, end: end}, slot: slot}:
			}
		}
	}()

	for slot := range ordered {
		select {
		case <-streamCtx.Done():
			cancel()
			wg.Wait()
			return true
		case <-slot.done:
		}
		if slot.err != nil || streamCtx.Err() != nil {
			// Player disconnect, removed session, upstream error or failed
			// refresh: stop the remaining chunks and never deliver partial data
			// as a complete response.
			cancel()
			wg.Wait()
			return true
		}
		if len(slot.data) > 0 {
			if _, err := w.Write(slot.data); err != nil {
				cancel()
				wg.Wait()
				return true
			}
		}
	}
	wg.Wait()
	return true
}

func (p *Proxy) fetchChunk(ctx context.Context, sess *proxySession, id, target string, headers map[string]string, start, end int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rng := fmt.Sprintf("bytes=%d-%d", start, end)
	resp, err := p.doSessionRequest(ctx, http.MethodGet, target, headers, rng, false)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		httpx.DrainAndClose(resp.Body)
		// Single-flight: concurrent chunks blocked on the same 401/403 share
		// one refresh and all continue with the same fresh snapshot.
		res := sess.ensureRefresh()
		if ctx.Err() != nil || sess.isRemoved() {
			return nil, context.Canceled
		}
		if res.outcome != refreshSucceeded {
			return nil, fmt.Errorf("upstream chunk auth expired and refresh did not succeed")
		}
		p.invalidateSessionPlaylists(id)
		resp, err = p.doSessionRequest(ctx, http.MethodGet, res.url, res.headers, rng, false)
		if err != nil {
			return nil, err
		}
	}
	defer httpx.DrainAndClose(resp.Body)
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upstream chunk HTTP %d", resp.StatusCode)
	}
	expected := end - start + 1
	data, _, err := httpx.ReadLimit(resp.Body, expected, expected)
	if err != nil {
		return nil, err
	}
	// A short chunk means an upstream error or truncation: fail the whole
	// prefetch instead of splicing incomplete bytes into the player's stream.
	if int64(len(data)) != expected {
		return nil, fmt.Errorf("upstream chunk %d-%d returned %d bytes, want %d", start, end, len(data), expected)
	}
	return data, nil
}

// parseSingleByteRange parses an HTTP Range header like "bytes=0-" or "bytes=100-200".
// Multi-range or suffix-range headers return ok=false so callers fall back to direct proxy.
func parseSingleByteRange(hdr string) (start, end int64, hasRange, ok bool) {
	hdr = strings.TrimSpace(hdr)
	if hdr == "" {
		return 0, -1, false, true
	}
	if !strings.HasPrefix(strings.ToLower(hdr), "bytes=") {
		return 0, 0, false, false
	}
	spec := strings.TrimSpace(hdr[len("bytes="):])
	if strings.Contains(spec, ",") {
		return 0, 0, false, false
	}
	sStr, eStr, found := strings.Cut(spec, "-")
	if !found || strings.TrimSpace(sStr) == "" {
		return 0, 0, false, false
	}
	s, err := strconv.ParseInt(strings.TrimSpace(sStr), 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false, false
	}
	e := int64(-1)
	if strings.TrimSpace(eStr) != "" {
		e, err = strconv.ParseInt(strings.TrimSpace(eStr), 10, 64)
		if err != nil || e < s {
			return 0, 0, false, false
		}
	}
	return s, e, true, true
}

func parseContentRange(hdr string) (start, end, total int64, ok bool) {
	hdr = strings.TrimSpace(hdr)
	if !strings.HasPrefix(strings.ToLower(hdr), "bytes ") {
		return 0, 0, 0, false
	}
	rest := strings.TrimSpace(hdr[len("bytes "):])
	span, totalStr, found := strings.Cut(rest, "/")
	if !found || totalStr == "*" {
		return 0, 0, 0, false
	}
	sStr, eStr, found := strings.Cut(span, "-")
	if !found {
		return 0, 0, 0, false
	}
	s, err1 := strconv.ParseInt(strings.TrimSpace(sStr), 10, 64)
	e, err2 := strconv.ParseInt(strings.TrimSpace(eStr), 10, 64)
	t, err3 := strconv.ParseInt(strings.TrimSpace(totalStr), 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || s < 0 || e < s || t <= e {
		return 0, 0, 0, false
	}
	return s, e, t, true
}
