package controllers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	mw "video-streaming-server/middleware"
	"video-streaming-server/types"
	"video-streaming-server/utils"
)

// TestHTTPEntry_CompleteAbortListOverRealListener wires the production
// middleware chain and handlers on a real TCP listener, then drives them with
// an HTTP client. Only the object store is replaced (see RecordingStore);
// auth, cookies, routing path parsing, JSON encoding and PostgreSQL are all
// the real production path.
func TestHTTPEntry_CompleteAbortListOverRealListener(t *testing.T) {
	h := newHarness(t)

	mux := http.NewServeMux()
	// Mirror main.go's /video/ dispatch for the three routes under test.
	mux.HandleFunc("/video/", utils.Chain(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/complete") && r.Method == http.MethodPost:
			CompleteVideoUpload(w, r, h.db)
		case strings.HasSuffix(path, "/abort") && r.Method == http.MethodPost:
			AbortVideoUpload(w, r, h.db)
		case path == "/video/" && r.Method == http.MethodGet:
			GetVideos(w, r, h.db)
		default:
			http.NotFound(w, r)
		}
	}, mw.AuthRequired))

	server := httptest.NewServer(mux)
	defer server.Close()

	do := func(method, path, body string) *http.Response {
		t.Helper()
		var reader io.Reader
		if body != "" {
			reader = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, server.URL+path, reader)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(h.cookie)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("http %s %s: %v", method, path, err)
		}
		return resp
	}

	// --- complete idempotency over real HTTP ---
	v := h.seedVideo(seedVideo{status: types.UploadPending})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	resp := do(http.MethodPost, "/video/"+v.videoID+"/complete", onePartBody)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first complete over HTTP = %d: %s", resp.StatusCode, string(body))
	}
	resp = do(http.MethodPost, "/video/"+v.videoID+"/complete", onePartBody)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("second complete over HTTP = %d, want 202", resp.StatusCode)
	}
	if n := h.store.Count("CompleteMultipartUpload"); n != 1 {
		t.Fatalf("merges=%d want 1", n)
	}

	// --- list over real HTTP contains the advanced video ---
	resp = do(http.MethodGet, "/video/", "")
	listBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list = %d", resp.StatusCode)
	}
	rows := decodeList(t, listBody)
	if st, ok := listContains(rows, v.videoID); !ok || st == types.UploadPending {
		t.Fatalf("advanced video missing/pending in HTTP list: ok=%v status=%d body=%s", ok, st, string(listBody))
	}

	// --- abort an advanced video over real HTTP -> 204 and preserved object ---
	resp = do(http.MethodPost, "/video/"+v.videoID+"/abort", "")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("abort advanced = %d want 204", resp.StatusCode)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("object must remain after aborting an advanced video")
	}

	// --- unauthenticated request is rejected by the real auth middleware ---
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	plainResp, err := noRedirect.Post(server.URL+"/video/"+v.videoID+"/abort", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer plainResp.Body.Close()
	_, _ = io.Copy(io.Discard, plainResp.Body)
	if plainResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("unauthenticated abort status=%d want 303 redirect to /login", plainResp.StatusCode)
	}
}
