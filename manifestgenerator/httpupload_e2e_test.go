package manifestgenerator

import (
	"bufio"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator/hls"
	"github.com/jordicenzano/go-ts-segmenter/manifestgenerator/mediachunk"
	"github.com/jordicenzano/go-ts-segmenter/uploaders/httpuploader"
)

// originPath normalizes a request path the same way net/http does when
// routing, so map lookups are consistent regardless of "/.." segments.
func originPath(p string) string {
	if cleaned := path.Clean(p); cleaned == "." || cleaned == "" {
		return "/"
	} else if strings.HasPrefix(p, "/") && !strings.HasPrefix(cleaned, "/") {
		return "/" + cleaned
	} else {
		return cleaned
	}
}

// originEvent is one POST observed by the test origin.
type originEvent struct {
	id          int
	path        string
	contentType string
	seqHeader   string
	headers     http.Header
	payload     []byte
}

// mockOrigin is a controllable HTTP origin. Per-path policies decide which
// status to return. Only 2xx responses persist an object, so a 503 attempt
// never leaves half content behind.
type mockOrigin struct {
	mu sync.Mutex

	events  []originEvent
	stored  map[string][]byte
	headers map[string]http.Header

	// policy returns the status for the nth request (1 based) of a path.
	policy func(reqPath string, requestNumber int) int
}

func newMockOrigin(policy func(reqPath string, requestNumber int) int) *mockOrigin {
	return &mockOrigin{
		stored:  map[string][]byte{},
		headers: map[string]http.Header{},
		policy:  policy,
	}
}

func (o *mockOrigin) handler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		reqPath := originPath(req.URL.Path)

		o.mu.Lock()
		n := 0
		for _, e := range o.events {
			if e.path == reqPath {
				n++
			}
		}
		n++
		o.events = append(o.events, originEvent{
			id:          len(o.events),
			path:        reqPath,
			contentType: req.Header.Get("Content-Type"),
			seqHeader:   req.Header.Get("Joc-Hls-Chunk-Seq-Number"),
			headers:     req.Header.Clone(),
			payload:     append([]byte(nil), body...),
		})
		status := o.policy(reqPath, n)
		if status < 400 {
			o.stored[reqPath] = append([]byte(nil), body...)
			o.headers[reqPath] = req.Header.Clone()
		}
		o.mu.Unlock()

		rw.WriteHeader(status)
	})
}

func (o *mockOrigin) snapshot() ([]originEvent, map[string][]byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	events := append([]originEvent(nil), o.events...)
	stored := make(map[string][]byte, len(o.stored))
	for k, v := range o.stored {
		stored[k] = append([]byte(nil), v...)
	}
	return events, stored
}

func (o *mockOrigin) requestCount(p string) int {
	p = originPath(p)
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, e := range o.events {
		if e.path == p {
			n++
		}
	}
	return n
}

// isChunkPath reports whether an origin path is a media segment (.ts).
func isChunkPath(p string) bool {
	return strings.HasSuffix(p, ".ts")
}

// chunkPathsFromManifest parses the stored chunklist body and returns the
// segment URI paths referenced by it.
func chunkPathsFromManifest(manifestBody []byte, chunklistPath string) []string {
	var out []string
	dir := path.Dir(chunklistPath)
	for _, line := range strings.Split(string(manifestBody), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, path.Join(dir, line))
	}
	return out
}

// runHTTPGenerator feeds the fixture through a ManifestGenerator wired to the
// given test origin in HTTP regular mode (VOD, ChunkInitStart so no separate
// init segment is uploaded). It returns the generator and any error raised
// while processing the input stream.
func runHTTPGenerator(t *testing.T, server *httptest.Server, resultDir string) (*ManifestGenerator, error) {
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal("Error parsing server URL: ", err)
	}
	// Small retry budget and tiny delay so the tests stay fast but still
	// exercise backoff and the attempt cap.
	up := httpuploader.New(nil, false, u.Scheme, u.Host, 2, 1)

	clearResultsDir(resultDir)

	mg := New(nil,
		mediachunk.ChunkOutputModeHTTPRegular,
		hls.HlsOutputModeHTTP,
		resultDir, "chunk_", "chunklist.m3u8",
		4.0, ChunkInitStart, true, -1, -1,
		hls.Vod, 3, 0, &up, nil)

	f, err := os.Open("../fixture/testSmall.ts")
	if err != nil {
		t.Fatal("Error opening fixture: ", err)
	}
	defer f.Close()

	var streamErr error
	reader := bufio.NewReader(f)
	buf := make([]byte, 0, 4*1024)
	for {
		n, errRead := reader.Read(buf[:cap(buf)])
		if n > 0 {
			if errAdd := mg.AddData(buf[:n]); errAdd != nil {
				streamErr = errAdd
				break
			}
		}
		if errRead == io.EOF {
			break
		}
		if errRead != nil {
			t.Fatal("Error reading fixture: ", errRead)
		}
	}
	return &mg, streamErr
}

// TestHTTPEndToEndTransient503Succeeds: first 503s then success. Final state
// must contain exactly one complete copy of every segment and a manifest
// referencing all of them, with unchanged headers and byte content.
func TestHTTPEndToEndTransient503Succeeds(t *testing.T) {
	origin := newMockOrigin(func(reqPath string, n int) int {
		if isChunkPath(reqPath) && n <= 2 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})
	server := httptest.NewServer(origin.handler())
	defer server.Close()

	resultDir := "../results/e2e-transient-503"
	mg, streamErr := runHTTPGenerator(t, server, resultDir)
	if streamErr != nil {
		t.Fatalf("Unexpected stream error: %v", streamErr)
	}
	if err := mg.Close(); err != nil {
		t.Fatalf("Close returned error after transient failures: %v", err)
	}

	events, stored := origin.snapshot()
	chunklistPath := path.Join(resultDir, "chunklist.m3u8")

	manifestBody, ok := stored[originPath("/"+chunklistPath)]
	if !ok {
		t.Fatal("Manifest was never uploaded")
	}

	// Every segment listed in the manifest must exist at the origin.
	for _, rel := range chunkPathsFromManifest(manifestBody, chunklistPath) {
		body, present := stored[originPath("/"+rel)]
		if !present {
			t.Errorf("Invariant violated: manifest references %s but origin has no such object", rel)
			continue
		}
		if len(body) == 0 {
			t.Errorf("Stored object %s is empty (half content)", rel)
		}
	}

	// Exactly one stored object per media segment: retries must not create
	// duplicates.
	storedChunks := 0
	for p := range stored {
		if isChunkPath(p) {
			storedChunks++
		}
	}
	manifestChunks := chunkPathsFromManifest(manifestBody, chunklistPath)
	if storedChunks != len(manifestChunks) {
		t.Errorf("Stored chunk objects (%d) differ from manifest entries (%d)", storedChunks, len(manifestChunks))
	}

	// Each segment was attempted 3 times (2 x 503 + 1 x 200) and every
	// attempt carried the full identical payload and the same headers.
	chunkAttempts := 0
	for _, e := range events {
		if !isChunkPath(e.path) {
			continue
		}
		chunkAttempts++
		final := stored[originPath(e.path)]
		if len(final) == 0 || len(e.payload) != len(final) {
			t.Errorf("Attempt for %s carried %d bytes, final object has %d", e.path, len(e.payload), len(final))
		}
		if e.contentType != "video/MP2T" {
			t.Errorf("Chunk %s Content-Type = %q, want video/MP2T", e.path, e.contentType)
		}
		if e.seqHeader == "" {
			t.Errorf("Chunk %s missing Joc-Hls-Chunk-Seq-Number header", e.path)
		}
	}
	if chunkAttempts != storedChunks*3 {
		t.Errorf("Expected %d chunk attempts (3 per segment), got %d", storedChunks*3, chunkAttempts)
	}

	// Manifest uploads use the HLS content type and succeed on first try.
	manifestCT := origin.headers[originPath("/"+chunklistPath)].Get("Content-Type")
	if manifestCT != "application/vnd.apple.mpegurl" {
		t.Errorf("Manifest Content-Type = %q, want application/vnd.apple.mpegurl", manifestCT)
	}
}

// TestHTTPEndToEndPersistent503ExcludesChunkFromManifest: a segment that keeps
// returning 503 must fail the run, never appear in any uploaded manifest and
// never be stored. Earlier segments remain available; later work never runs.
func TestHTTPEndToEndPersistent503ExcludesChunkFromManifest(t *testing.T) {
	failingChunk := path.Join("../results/e2e-persistent-503", "chunk_00001.ts")
	origin := newMockOrigin(func(reqPath string, n int) int {
		if reqPath == originPath("/"+failingChunk) {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})
	server := httptest.NewServer(origin.handler())
	defer server.Close()

	resultDir := "../results/e2e-persistent-503"
	mg, streamErr := runHTTPGenerator(t, server, resultDir)
	err := streamErr
	if err == nil {
		err = mg.Close()
	}
	if err == nil {
		t.Fatal("Expected a failure when a chunk upload is exhausted, got nil")
	}
	if !strings.Contains(err.Error(), "chunk_00001.ts") {
		t.Errorf("Error must identify the failed segment, got: %v", err)
	}

	events, stored := origin.snapshot()

	// The failed segment was attempted 1 + maxRetries(2) = 3 times and never
	// stored.
	if got := origin.requestCount("/" + failingChunk); got != 3 {
		t.Errorf("Expected 3 attempts for the failing chunk, got %d", got)
	}
	if _, present := stored[originPath("/"+failingChunk)]; present {
		t.Error("Failed chunk must not be stored at the origin")
	}

	// No uploaded manifest may reference the failed chunk (or any missing
	// object). This is the core observable invariant.
	chunklistPath := path.Join(resultDir, "chunklist.m3u8")
	var lastManifest []byte
	for _, e := range events {
		if e.path == originPath("/"+chunklistPath) {
			lastManifest = e.payload
		}
	}
	if lastManifest == nil {
		t.Fatal("No manifest upload observed at all")
	}
	for _, rel := range chunkPathsFromManifest(lastManifest, chunklistPath) {
		if strings.Contains(rel, "chunk_00001.ts") {
			t.Errorf("Failed chunk appears in manifest:\n%s", string(lastManifest))
		}
		if _, present := stored[originPath("/"+rel)]; !present {
			t.Errorf("Manifest references missing object %s", rel)
		}
	}

	// Only the first segment could have been stored before the failure.
	storedChunks := []string{}
	for p := range stored {
		if isChunkPath(p) {
			storedChunks = append(storedChunks, p)
		}
	}
	if len(storedChunks) != 1 || !strings.HasSuffix(storedChunks[0], "chunk_00000.ts") {
		t.Errorf("Expected only chunk_00000.ts stored, got %v", storedChunks)
	}
}

// TestHTTPEndToEnd400FailsImmediately: a 400 on a segment must abort with a
// single request (no retries consumed) and keep the segment out of the
// manifest.
func TestHTTPEndToEnd400FailsImmediately(t *testing.T) {
	failingChunk := path.Join("../results/e2e-400", "chunk_00001.ts")
	origin := newMockOrigin(func(reqPath string, n int) int {
		if reqPath == originPath("/"+failingChunk) {
			return http.StatusBadRequest
		}
		return http.StatusOK
	})
	server := httptest.NewServer(origin.handler())
	defer server.Close()

	resultDir := "../results/e2e-400"
	mg, streamErr := runHTTPGenerator(t, server, resultDir)
	err := streamErr
	if err == nil {
		err = mg.Close()
	}
	if err == nil {
		t.Fatal("Expected failure on 400, got nil")
	}
	if got := origin.requestCount("/" + failingChunk); got != 1 {
		t.Errorf("400 must not be retried: expected 1 attempt, got %d", got)
	}

	_, stored := origin.snapshot()
	if _, present := stored[originPath("/"+failingChunk)]; present {
		t.Error("400-rejected chunk must not be stored")
	}

	chunklistPath := path.Join(resultDir, "chunklist.m3u8")
	events, _ := origin.snapshot()
	var lastManifest []byte
	for _, e := range events {
		if e.path == originPath("/"+chunklistPath) {
			lastManifest = e.payload
		}
	}
	if lastManifest == nil {
		t.Fatal("No manifest upload observed")
	}
	if strings.Contains(string(lastManifest), "chunk_00001.ts") {
		t.Errorf("400-rejected chunk appears in manifest:\n%s", string(lastManifest))
	}
}

// TestHTTPEndToEndManifestUploadFailureStopsPipeline: when the chunklist
// itself cannot be uploaded, the error must propagate and no later manifest
// may be generated. Segments already uploaded remain consistent with every
// manifest that did reach the origin.
func TestHTTPEndToEndManifestUploadFailureStopsPipeline(t *testing.T) {
	resultDir := "../results/e2e-manifest-fails"
	chunklistPath := path.Join(resultDir, "chunklist.m3u8")
	origin := newMockOrigin(func(reqPath string, n int) int {
		if reqPath == originPath("/"+chunklistPath) {
			return http.StatusServiceUnavailable // manifest never uploads
		}
		return http.StatusOK
	})
	server := httptest.NewServer(origin.handler())
	defer server.Close()

	mg, streamErr := runHTTPGenerator(t, server, resultDir)
	err := streamErr
	if err == nil {
		err = mg.Close()
	}
	if err == nil {
		t.Fatal("Expected failure when the manifest cannot be uploaded, got nil")
	}

	// Manifest attempts are bounded by the retry cap (3), proving the error
	// was surfaced instead of swallowed and retried forever.
	if got := origin.requestCount("/" + chunklistPath); got != 3 {
		t.Errorf("Expected 3 manifest attempts, got %d", got)
	}

	events, stored := origin.snapshot()
	// Every manifest that reached the origin references only stored chunks.
	for _, e := range events {
		if e.path != originPath("/"+chunklistPath) {
			continue
		}
		for _, rel := range chunkPathsFromManifest(e.payload, chunklistPath) {
			if _, present := stored[originPath("/"+rel)]; !present {
				t.Errorf("Uploaded manifest references missing chunk %s", rel)
			}
		}
	}
}

// TestHTTPEndToEndChunkBeforeManifestOrdering verifies the interleaving
// invariant: for every segment, the successful chunk upload is observed
// before the manifest that first references it.
func TestHTTPEndToEndChunkBeforeManifestOrdering(t *testing.T) {
	resultDir := "../results/e2e-ordering"
	chunklistPath := path.Join(resultDir, "chunklist.m3u8")
	origin := newMockOrigin(func(reqPath string, n int) int {
		// Fail the first segment once to interleave retries with the flow.
		if reqPath == originPath("/"+path.Join(resultDir, "chunk_00000.ts")) && n == 1 {
			return http.StatusServiceUnavailable
		}
		return http.StatusOK
	})
	server := httptest.NewServer(origin.handler())
	defer server.Close()

	mg, streamErr := runHTTPGenerator(t, server, resultDir)
	if streamErr != nil {
		t.Fatalf("Unexpected stream error: %v", streamErr)
	}
	if err := mg.Close(); err != nil {
		t.Fatalf("Unexpected failure: %v", err)
	}

	events, stored := origin.snapshot()

	// id of the first successful (stored) upload per path.
	firstSuccess := map[string]int{}
	for _, e := range events {
		key := e.path
		if _, ok := stored[key]; ok {
			if _, seen := firstSuccess[key]; !seen {
				firstSuccess[key] = e.id
			}
		}
	}

	for _, e := range events {
		if e.path != originPath("/"+chunklistPath) {
			continue
		}
		for _, rel := range chunkPathsFromManifest(e.payload, chunklistPath) {
			chunkIdx, ok := firstSuccess[originPath("/"+rel)]
			if !ok {
				t.Errorf("Manifest references chunk %s that was never successfully uploaded", rel)
				continue
			}
			if chunkIdx > e.id {
				t.Errorf("Chunk %s uploaded (event %d) after manifest referencing it (event %d)", rel, chunkIdx, e.id)
			}
		}
	}
}

// feedFileGenerator runs the same fixture through a file-mode generator and
// returns the result directory, providing the pre-change byte-level baseline.
func feedFileGenerator(t *testing.T, resultDir string) {
	clearResultsDir(resultDir)
	mg := New(nil,
		mediachunk.ChunkOutputModeFile,
		hls.HlsOutputModeFile,
		resultDir, "chunk_", "chunklist.m3u8",
		4.0, ChunkInitStart, true, -1, -1,
		hls.Vod, 3, 0, nil, nil)

	f, err := os.Open("../fixture/testSmall.ts")
	if err != nil {
		t.Fatal("Error opening fixture: ", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	buf := make([]byte, 0, 4*1024)
	for {
		n, errRead := reader.Read(buf[:cap(buf)])
		if n > 0 {
			if errAdd := mg.AddData(buf[:n]); errAdd != nil {
				t.Fatal("Unexpected AddData error: ", errAdd)
			}
		}
		if errRead == io.EOF {
			break
		}
		if errRead != nil {
			t.Fatal("Error reading fixture: ", errRead)
		}
	}
	if err := mg.Close(); err != nil {
		t.Fatal("Unexpected Close error: ", err)
	}
}

// TestHTTPNormalPathParityWithFileOutput proves the "normal path is
// unchanged" requirement: against a healthy origin, every segment's bytes and
// the full request header set (Content-Type plus all Joc-Hls-* headers) must
// be byte-for-byte identical to what the pre-existing file output produces,
// and the manifest bytes must match too.
func TestHTTPNormalPathParityWithFileOutput(t *testing.T) {
	fileDir := "../results/parity-file"
	httpDir := "../results/parity-http"
	feedFileGenerator(t, fileDir)

	origin := newMockOrigin(func(reqPath string, n int) int { return http.StatusOK })
	server := httptest.NewServer(origin.handler())
	defer server.Close()

	mg, streamErr := runHTTPGenerator(t, server, httpDir)
	if streamErr != nil {
		t.Fatalf("Unexpected stream error: %v", streamErr)
	}
	if err := mg.Close(); err != nil {
		t.Fatalf("Unexpected Close error: %v", err)
	}

	_, stored := origin.snapshot()

	// Sizes known from the file-mode golden test (ChunkInitStart, this fixture).
	wantSizes := map[string]int64{
		"chunk_00000.ts": 103400,
		"chunk_00001.ts": 108664,
		"chunk_00002.ts": 115056,
	}

	for name, wantSize := range wantSizes {
		// 1) HTTP payload equals the file-mode bytes exactly.
		fileBytes, err := ioutil.ReadFile(path.Join(fileDir, name))
		if err != nil {
			t.Fatalf("Missing baseline file %s: %v", name, err)
		}
		httpKey := originPath("/" + path.Join(httpDir, name))
		httpBytes, ok := stored[httpKey]
		if !ok {
			t.Fatalf("Segment %s was not uploaded on the normal path", name)
		}
		if int64(len(httpBytes)) != wantSize {
			t.Errorf("%s: uploaded size %d, want golden %d", name, len(httpBytes), wantSize)
		}
		if string(httpBytes) != string(fileBytes) {
			t.Errorf("%s: uploaded bytes differ from file-mode baseline", name)
		}
	}

	// 2) Full header set is present and well formed on every segment attempt.
	events, _ := origin.snapshot()
	seqSeen := map[string]bool{}
	for _, e := range events {
		if !isChunkPath(e.path) {
			continue
		}
		h := e.headers
		if got := h.Get("Content-Type"); got != "video/MP2T" {
			t.Errorf("%s: Content-Type = %q, want video/MP2T", e.path, got)
		}
		if got := h.Get("Joc-Hls-Chunk-Seq-Number"); got == "" {
			t.Errorf("%s: missing Joc-Hls-Chunk-Seq-Number", e.path)
		} else {
			seqSeen[e.path] = true
		}
		if got := h.Get("Joc-Hls-Targetduration-Ms"); got != "4000.00000000" {
			t.Errorf("%s: Joc-Hls-Targetduration-Ms = %q", e.path, got)
		}
		if got := h.Get("Joc-Hls-Createdat-Ns"); got == "" {
			t.Errorf("%s: missing Joc-Hls-Createdat-Ns", e.path)
		}
		if got := h.Get("Joc-Hls-Duration-Ms"); got == "" {
			t.Errorf("%s: missing Joc-Hls-Duration-Ms (added at close)", e.path)
		}
	}
	if len(seqSeen) != len(wantSizes) {
		t.Errorf("Expected seq header on %d distinct segments, got %d", len(wantSizes), len(seqSeen))
	}

	// 3) Manifest bytes equal the file-mode golden manifest.
	fileManifest, err := ioutil.ReadFile(path.Join(fileDir, "chunklist.m3u8"))
	if err != nil {
		t.Fatal("Missing baseline manifest: ", err)
	}
	httpManifest, ok := stored[originPath("/"+path.Join(httpDir, "chunklist.m3u8"))]
	if !ok {
		t.Fatal("Manifest was not uploaded on the normal path")
	}
	if string(httpManifest) != string(fileManifest) {
		t.Errorf("Uploaded manifest differs from file-mode baseline.\nGot:\n%s\nWant:\n%s", httpManifest, fileManifest)
	}
}
