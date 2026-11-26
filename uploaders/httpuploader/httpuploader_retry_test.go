package httpuploader

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// attemptRecord stores what the origin saw on a single POST attempt.
type attemptRecord struct {
	path    string
	payload []byte
	headers http.Header
}

// failingThenSuccessServer returns status failStatus for the first failCount
// requests of each destination path, then successStatus. Only successful
// requests store the object, allowing the tests to assert that exactly one
// complete object remains (no duplicates, no half content).
func failingThenSuccessServer(t *testing.T, failStatus int, failCount int, successStatus int, records *[]attemptRecord, stored *map[string][]byte, mu *sync.Mutex) *httptest.Server {
	counters := map[string]int{}
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			t.Error("Error reading request body. Err: ", err)
		}

		mu.Lock()
		*records = append(*records, attemptRecord{path: req.URL.Path, payload: body, headers: req.Header.Clone()})
		counters[req.URL.Path]++
		attempt := counters[req.URL.Path]

		if attempt <= failCount {
			mu.Unlock()
			rw.WriteHeader(failStatus)
			return
		}

		// Store (overwriting the same key) only on a successful response.
		(*stored)[req.URL.Path] = append([]byte(nil), body...)
		mu.Unlock()

		rw.WriteHeader(successStatus)
		rw.Write([]byte(`OK`))
	}))
}

func newTestUploader(t *testing.T, server *httptest.Server, maxRetries, initialDelayMs int) HTTPUploader {
	u, errURL := url.Parse(server.URL)
	if errURL != nil {
		t.Fatal("Error parsing test server URL. Err ", errURL)
	}
	return New(nil, false, u.Scheme, u.Host, maxRetries, initialDelayMs)
}

// TestUploadDataRetriesThenSuccess: 503 twice then 200. The origin must end up
// with exactly one complete object, every attempt must have carried the full
// identical payload and headers, and UploadData must return nil.
func TestUploadDataRetriesThenSuccess(t *testing.T) {
	var (
		mu      sync.Mutex
		records []attemptRecord
		stored  = map[string][]byte{}
	)
	server := failingThenSuccessServer(t, http.StatusServiceUnavailable, 2, http.StatusOK, &records, &stored, &mu)
	defer server.Close()

	data := []byte("the quick brown fox jumps over the lazy dog")
	uploadPath := "test/retry-then-success.ts"
	headers := map[string]string{
		"Content-Type":             "video/MP2T",
		"Joc-Hls-Chunk-Seq-Number": "7",
	}

	up := newTestUploader(t, server, 3, 1)
	start := time.Now()
	if err := up.UploadData(data, uploadPath, headers); err != nil {
		t.Fatalf("Expected upload to succeed after retries, got error: %v", err)
	}
	elapsed := time.Since(start)

	mu.Lock()
	defer mu.Unlock()

	// 2 failures + 1 success
	if len(records) != 3 {
		t.Fatalf("Expected 3 HTTP attempts, got %d", len(records))
	}

	// Every attempt must hit the same destination path with the full payload
	// (no half / empty body on retries) and identical headers.
	for i, rec := range records {
		if rec.path != "/"+uploadPath {
			t.Errorf("Attempt %d: wrong path, got %s want /%s", i, rec.path, uploadPath)
		}
		if !testBinary(rec.payload, data) {
			t.Errorf("Attempt %d: payload differs from original (%d vs %d bytes)", i, len(rec.payload), len(data))
		}
		if got := rec.headers.Get("Content-Type"); got != "video/MP2T" {
			t.Errorf("Attempt %d: Content-Type = %q, want video/MP2T", i, got)
		}
		if got := rec.headers.Get("Joc-Hls-Chunk-Seq-Number"); got != "7" {
			t.Errorf("Attempt %d: chunk seq header = %q, want 7", i, got)
		}
	}

	// Exactly one object stored, with the full content.
	if len(stored) != 1 {
		t.Fatalf("Expected exactly 1 stored object, got %d", len(stored))
	}
	gotBody, ok := stored["/"+uploadPath]
	if !ok || !testBinary(gotBody, data) {
		t.Errorf("Stored object missing or content mismatch (ok=%v, len=%d)", ok, len(gotBody))
	}

	// Backoff must actually be waited for: 1ms + 2ms = 3ms minimum.
	if elapsed < 3*time.Millisecond {
		t.Errorf("Retry backoff not applied, elapsed %v", elapsed)
	}
}

// TestUploadLocalFileRetriesThenSuccess exercises the file based entry point:
// a 503 is retried and the final stored object matches the source file.
func TestUploadLocalFileRetriesThenSuccess(t *testing.T) {
	testFilePath := "../../fixture/testSmall.ts"
	var (
		mu      sync.Mutex
		records []attemptRecord
		stored  = map[string][]byte{}
	)
	server := failingThenSuccessServer(t, http.StatusServiceUnavailable, 1, http.StatusOK, &records, &stored, &mu)
	defer server.Close()

	expected, err := ioutil.ReadFile(testFilePath)
	if err != nil {
		t.Fatal("Error reading expected fixture. Err: ", err)
	}

	uploadPath := "test/local-retry.ts"
	up := newTestUploader(t, server, 3, 1)
	if err := up.UploadLocalFile(testFilePath, uploadPath, map[string]string{"Content-Type": "video/MP2T"}); err != nil {
		t.Fatalf("Expected upload to succeed after retry, got error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(records) != 2 {
		t.Fatalf("Expected 2 HTTP attempts, got %d", len(records))
	}
	for i, rec := range records {
		if !testBinary(rec.payload, expected) {
			t.Errorf("Attempt %d: uploaded bytes differ from fixture (%d vs %d)", i, len(rec.payload), len(expected))
		}
	}
	if len(stored) != 1 || !testBinary(stored["/"+uploadPath], expected) {
		t.Errorf("Expected one stored object equal to the fixture")
	}
}

// TestUploadDataRetriesExhausted: persistent 503. With maxRetries=2 the
// uploader performs exactly 3 attempts, returns an error naming the segment,
// and the origin stores nothing.
func TestUploadDataRetriesExhaust(t *testing.T) {
	var (
		mu      sync.Mutex
		records []attemptRecord
		stored  = map[string][]byte{}
	)
	server := failingThenSuccessServer(t, http.StatusServiceUnavailable, 1000000, http.StatusOK, &records, &stored, &mu)
	defer server.Close()

	uploadPath := "test/always-fails.ts"
	up := newTestUploader(t, server, 2, 1)
	err := up.UploadData([]byte("payload"), uploadPath, nil)
	if err == nil {
		t.Fatal("Expected an error when retries are exhausted, got nil")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(records) != 3 {
		t.Fatalf("Expected 3 attempts (1 initial + 2 retries), got %d", len(records))
	}
	if len(stored) != 0 {
		t.Errorf("No object should be stored on failure, got %d", len(stored))
	}

	var uploadErr *UploadError
	if !errors.As(err, &uploadErr) {
		t.Fatalf("Expected *UploadError, got %T: %v", err, err)
	}
	if uploadErr.DstPathFile != uploadPath {
		t.Errorf("UploadError.DstPathFile = %q, want %q", uploadErr.DstPathFile, uploadPath)
	}
	if uploadErr.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("UploadError.StatusCode = %d, want 503", uploadErr.StatusCode)
	}
	if !strings.Contains(err.Error(), uploadPath) {
		t.Errorf("Error message must name the failed segment %q, got: %v", uploadPath, err)
	}
}

// TestUploadDataNonRetryable4xx: a 400 must fail immediately with a single
// request, wrapped as ErrNonRetryableResponse, regardless of the retry budget.
func TestUploadDataNonRetryable4xx(t *testing.T) {
	var (
		mu      sync.Mutex
		records []attemptRecord
		stored  = map[string][]byte{}
	)
	server := failingThenSuccessServer(t, http.StatusBadRequest, 1000000, http.StatusOK, &records, &stored, &mu)
	defer server.Close()

	uploadPath := "test/bad-request.ts"
	up := newTestUploader(t, server, 5, 10)
	start := time.Now()
	err := up.UploadData([]byte("payload"), uploadPath, nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Expected an error on 400, got nil")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(records) != 1 {
		t.Fatalf("Expected exactly 1 attempt for a non-retryable 400, got %d", len(records))
	}
	if len(stored) != 0 {
		t.Errorf("No object should be stored after a 400, got %d", len(stored))
	}
	if !errors.Is(err, ErrNonRetryableResponse) {
		t.Errorf("Expected error to wrap ErrNonRetryableResponse, got: %v", err)
	}
	var uploadErr *UploadError
	if !errors.As(err, &uploadErr) || uploadErr.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected *UploadError with status 400, got %v", err)
	}
	// No retry sleep may happen on the non-retryable path.
	if elapsed > 5*time.Millisecond {
		t.Errorf("Non-retryable error should fail without backoff, took %v", elapsed)
	}
}

// TestOther5xxIsRetriedAnd4xxAreNot covers additional status classes: 5xx is
// retryable, 4xx is not.
func TestOther5xxIsRetriedAnd4xxAreNot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		wantRetry  bool
		wantNonRet bool
	}{
		{"500 retried", http.StatusInternalServerError, true, false},
		{"502 retried", http.StatusBadGateway, true, false},
		{"404 not retried", http.StatusNotFound, false, true},
		{"403 not retried", http.StatusForbidden, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu      sync.Mutex
				records []attemptRecord
				stored  = map[string][]byte{}
			)
			server := failingThenSuccessServer(t, tc.status, 1000000, http.StatusOK, &records, &stored, &mu)
			defer server.Close()

			up := newTestUploader(t, server, 2, 1)
			err := up.UploadData([]byte("payload"), "test/status.ts", nil)
			if err == nil {
				t.Fatal("Expected an error, got nil")
			}

			mu.Lock()
			defer mu.Unlock()

			if tc.wantRetry && len(records) != 3 {
				t.Errorf("Expected 3 attempts for retryable status %d, got %d", tc.status, len(records))
			}
			if tc.wantNonRet {
				if len(records) != 1 {
					t.Errorf("Expected 1 attempt for non-retryable status %d, got %d", tc.status, len(records))
				}
				if !errors.Is(err, ErrNonRetryableResponse) {
					t.Errorf("Expected ErrNonRetryableResponse for status %d, got %v", tc.status, err)
				}
			}
		})
	}
}

// TestRetryDelayIncreases verifies the wait grows with the attempt number.
func TestRetryDelayIncreases(t *testing.T) {
	up := HTTPUploader{InitialHTTPRetryDelayMs: 10}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond}
	var prev time.Duration
	for attempt, expected := range want {
		got := up.retryDelay(attempt)
		if got != expected {
			t.Errorf("retryDelay(%d) = %v, want %v", attempt, got, expected)
		}
		if attempt > 0 && got <= prev {
			t.Errorf("Delay must increase with attempt number, attempt %d got %v <= %v", attempt, got, prev)
		}
		prev = got
	}
}

// TestMaxRetriesZeroMeansSingleAttempt guards the attempt cap semantics: with
// zero retries only the initial attempt is made.
func TestMaxRetriesZeroMeansSingleAttempt(t *testing.T) {
	var (
		mu      sync.Mutex
		records []attemptRecord
		stored  = map[string][]byte{}
	)
	server := failingThenSuccessServer(t, http.StatusServiceUnavailable, 1000000, http.StatusOK, &records, &stored, &mu)
	defer server.Close()

	up := newTestUploader(t, server, 0, 1)
	if err := up.UploadData([]byte("payload"), "test/no-retries.ts", nil); err == nil {
		t.Fatal("Expected an error, got nil")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(records) != 1 {
		t.Errorf("Expected 1 attempt when MaxHTTPRetries=0, got %d", len(records))
	}
}
