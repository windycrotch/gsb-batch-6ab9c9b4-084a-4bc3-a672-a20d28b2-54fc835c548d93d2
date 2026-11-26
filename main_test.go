package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
)

// resetFlags restores every flag this test touches, so tests do not leak
// configuration into each other.
func resetFlags(t *testing.T) func() {
	t.Helper()
	saved := map[string]string{}
	for _, name := range []string{
		"mediaDestinationType", "manifestDestinationType", "protocol", "host",
		"httpMaxRetries", "initialHTTPRetryDelay", "dstPath", "chunksBaseFilename",
		"chunklistFilename", "manifestType", "initType", "apids",
	} {
		f := flag.Lookup(name)
		if f != nil {
			saved[name] = f.Value.String()
		}
	}
	return func() {
		for name, val := range saved {
			_ = flag.Set(name, val)
		}
	}
}

func setFlag(t *testing.T, name, value string) {
	t.Helper()
	if err := flag.Set(name, value); err != nil {
		t.Fatalf("Cannot set flag %s=%s: %v", name, value, err)
	}
}

// newEndToEndOrigin returns an origin whose policy maps request path to a
// status. Status 0 means success (200).
func newEndToEndOrigin(t *testing.T, policy func(path string, nth int) int) (*httptest.Server, *sync.Mutex, *[]string) {
	var mu sync.Mutex
	var failedPaths []string
	counters := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		counters[req.URL.Path]++
		status := policy(req.URL.Path, counters[req.URL.Path])
		if status >= 400 {
			failedPaths = append(failedPaths, req.URL.Path)
		}
		mu.Unlock()
		if status == 0 {
			status = http.StatusOK
		}
		rw.WriteHeader(status)
		if status < 400 {
			rw.Write(body)
		}
	}))
	return server, &mu, &failedPaths
}

// configureExecuteForHTTP sets the global flags to an HTTP-regular VOD setup
// targeting the test server, and returns the input reader and output dir.
func configureExecuteForHTTP(t *testing.T, serverURL string, resultDir string) *os.File {
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal("Bad server URL: ", err)
	}

	setFlag(t, "mediaDestinationType", "3")    // HTTP regular
	setFlag(t, "manifestDestinationType", "2") // HTTP
	setFlag(t, "protocol", u.Scheme)
	setFlag(t, "host", u.Host)
	setFlag(t, "httpMaxRetries", "2")
	setFlag(t, "initialHTTPRetryDelay", "1")
	setFlag(t, "dstPath", resultDir)
	setFlag(t, "chunksBaseFilename", "chunk_")
	setFlag(t, "chunklistFilename", "chunklist.m3u8")
	setFlag(t, "manifestType", "0") // VOD
	setFlag(t, "initType", "2")     // ChunkInitStart: no separate init upload
	setFlag(t, "apids", "true")

	os.RemoveAll(resultDir)

	f, err := os.Open("fixture/testSmall.ts")
	if err != nil {
		t.Fatal("Cannot open fixture: ", err)
	}
	return f
}

// TestExecuteSucceedsWhenOriginHealthy is the happy path through the actual
// process entry point: exit code 0.
func TestExecuteSucceedsWhenOriginHealthy(t *testing.T) {
	restore := resetFlags(t)
	defer restore()

	server, _, _ := newEndToEndOrigin(t, func(p string, nth int) int { return 0 })
	defer server.Close()

	resultDir := "./results/cmd-ok"
	input := configureExecuteForHTTP(t, server.URL, resultDir)
	defer input.Close()

	if code := execute(input); code != 0 {
		t.Fatalf("Expected exit code 0 on success, got %d", code)
	}
}

// TestExecuteFailsNonZeroWhenChunkUploadExhausted drives the full binary path
// against an origin that always 503s one segment. The process must return a
// non-zero exit code and the failed segment must not be referenced by the
// last uploaded manifest.
func TestExecuteFailsNonZeroWhenChunkUploadExhausted(t *testing.T) {
	restore := resetFlags(t)
	defer restore()

	resultDir := "./results/cmd-fail"
	failingPath := "/" + strings.TrimPrefix(resultDir, "./") + "/chunk_00001.ts"
	server, mu, failedPaths := newEndToEndOrigin(t, func(p string, nth int) int {
		if p == failingPath {
			return http.StatusServiceUnavailable
		}
		return 0
	})
	defer server.Close()

	input := configureExecuteForHTTP(t, server.URL, resultDir)
	defer input.Close()

	code := execute(input)
	if code == 0 {
		t.Fatal("Expected non-zero exit code when a chunk cannot be uploaded, got 0")
	}

	mu.Lock()
	sawFailure := false
	for _, p := range *failedPaths {
		if strings.Contains(p, "chunk_00001.ts") {
			sawFailure = true
		}
	}
	mu.Unlock()
	if !sawFailure {
		t.Error("Expected the failing segment to be attempted against the origin")
	}
}

// TestExecuteFailsNonZeroOn400WithoutRetry: the 400 segment is attempted once
// and the process exits non-zero.
func TestExecuteFailsNonZeroOn400WithoutRetry(t *testing.T) {
	restore := resetFlags(t)
	defer restore()

	resultDir := "./results/cmd-400"
	failingPath := "/" + strings.TrimPrefix(resultDir, "./") + "/chunk_00001.ts"
	counts := map[string]int{}
	var countsMu sync.Mutex
	server, _, _ := newEndToEndOrigin(t, func(p string, nth int) int {
		countsMu.Lock()
		counts[p] = nth
		countsMu.Unlock()
		if p == failingPath {
			return http.StatusBadRequest
		}
		return 0
	})
	defer server.Close()

	input := configureExecuteForHTTP(t, server.URL, resultDir)
	defer input.Close()

	if code := execute(input); code == 0 {
		t.Fatal("Expected non-zero exit code on 400, got 0")
	}

	countsMu.Lock()
	got := counts[failingPath]
	countsMu.Unlock()
	if got != 1 {
		t.Errorf("Expected exactly 1 attempt for the 400 segment, got %d", got)
	}
}
