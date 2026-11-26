package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"ffmpeg-api/api"
	"ffmpeg-api/internal"
)

const (
	statusTrailer = "X-Ffmpeg-Status"
	bytesTrailer  = "X-Ffmpeg-Bytes"
)

func main() {
	server := &http.Server{
		Addr:              ":8080",
		Handler:           newMux(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	log.Println("server listening on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("server failed:", err)
	}
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/process", handleProcess)
	return mux
}

func handleProcess(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		internal.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	defer r.Body.Close()

	// The request context is canceled by the HTTP server when the client
	// disconnects or the caller shuts the request down. Every long-running
	// operation below derives from it, so cancellation stops downloads and
	// FFmpeg processes and suppresses further response writes. The explicit
	// cancel is also used by the streaming writer, where a failed response
	// write must stop FFmpeg immediately instead of draining into a dead
	// connection.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	req, err := decodeRequest(r.Body)
	if err != nil {
		log.Println("invalid request:", err)
		respondError(ctx, w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateRequest(req); err != nil {
		log.Println("invalid request:", err)
		respondError(ctx, w, http.StatusBadRequest, err.Error())
		return
	}

	jobPath, cleanup, err := createJobPath()
	if err != nil {
		log.Println("job directory creation failed:", err)
		respondError(ctx, w, http.StatusInternalServerError, "failed to create job directory")
		return
	}
	defer cleanup()
	log.Println("job started:", jobPath)

	var s3Client *s3.Client
	if requestUsesS3(req) {
		s3Client = internal.GetS3Client(&req.S3Config)
	}

	if err := fetchInputs(ctx, s3Client, req.Inputs, jobPath); err != nil {
		if requestCanceled(ctx) {
			log.Println("input fetch canceled:", err)
			return
		}
		log.Println("input fetch failed:", err)
		respondError(ctx, w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Output.Stream {
		if err := runFFmpegStreaming(ctx, cancel, w, req, jobPath); err != nil {
			log.Println("streaming ffmpeg execution failed:", err)
			return
		}
		log.Printf("job finished in %s (streamed)", time.Since(start))
		return
	}

	if stderr, err := runFFmpeg(ctx, req.Commands, jobPath); err != nil {
		logFFmpegError(stderr)
		if requestCanceled(ctx) {
			log.Println("ffmpeg canceled, response suppressed")
			return
		}
		respondConsoleError(ctx, w, http.StatusInternalServerError, err.Error(), stderr)
		return
	}

	if req.Output.InlineContentType != "" {
		if err := serveFirstResult(ctx, w, r, jobPath, req); err != nil {
			log.Println("inline result failed:", err)
			respondError(ctx, w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	results, err := collectResults(ctx, s3Client, req, jobPath)
	if err != nil {
		log.Println("result collection failed:", err)
		if requestCanceled(ctx) {
			return
		}
		respondError(ctx, w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("job finished in %s (%d outputs)", time.Since(start), len(results))
	if requestCanceled(ctx) {
		return
	}
	internal.WriteJSON(w, http.StatusOK, api.ProcessResponse{Results: results})
}

// requestCanceled reports whether the request context is done. Response
// writers must check this before emitting, because a disconnected client has
// nowhere to receive a status or error body.
func requestCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func respondError(ctx context.Context, w http.ResponseWriter, status int, msg string) {
	if requestCanceled(ctx) {
		log.Println("request canceled, error response suppressed:", msg)
		return
	}
	internal.WriteError(w, status, msg)
}

func respondConsoleError(ctx context.Context, w http.ResponseWriter, status int, msg string, console []string) {
	if requestCanceled(ctx) {
		log.Println("request canceled, error response suppressed:", msg)
		return
	}
	internal.WriteConsoleError(w, status, msg, console)
}

func decodeRequest(body io.Reader) (api.ProcessRequest, error) {
	var req api.ProcessRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return req, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return req, fmt.Errorf("request body must contain one JSON object")
		}
		return req, err
	}
	return req, nil
}

func validateRequest(req api.ProcessRequest) error {
	if len(req.Commands) == 0 {
		return fmt.Errorf("at least one command is required")
	}
	for i, command := range req.Commands {
		if len(command) == 0 {
			return fmt.Errorf("command %d is empty", i+1)
		}
	}
	for name, input := range req.Inputs {
		if name == "" || name == "." || filepath.Base(name) != name {
			return fmt.Errorf("input name %q must be a filename without a path", name)
		}
		sources := 0
		for _, present := range []bool{input.S3 != "", input.HTTP != "", input.Base64 != "", input.Temporary} {
			if present {
				sources++
			}
		}
		if sources != 1 {
			return fmt.Errorf("input %q must define exactly one source", name)
		}
	}
	if req.Output.InlineContentType != "" {
		if _, _, err := mime.ParseMediaType(req.Output.InlineContentType); err != nil {
			return fmt.Errorf("invalid inlineContentType: %w", err)
		}
		if req.Output.S3 != "" || req.Output.Base64 {
			return fmt.Errorf("inline output cannot be combined with s3 or base64 output")
		}
	}
	if req.Output.Stream {
		if req.Output.InlineContentType == "" {
			return fmt.Errorf("stream output requires inlineContentType")
		}
		last := req.Commands[len(req.Commands)-1]
		if last[len(last)-1] != "pipe:1" && last[len(last)-1] != "-" {
			return fmt.Errorf("the final streaming command must write to pipe:1")
		}
	}
	if req.Output.S3 == "" && !req.Output.Base64 && req.Output.InlineContentType == "" {
		return fmt.Errorf("at least one output option is required")
	}
	if requestUsesS3(req) {
		if req.S3Config.Endpoint == "" || req.S3Config.Region == "" || req.S3Config.AccessKey == "" || req.S3Config.SecretKey == "" {
			return fmt.Errorf("s3Config endpoint, region, accessKey, and secretKey are required for S3 transfers")
		}
	}
	return nil
}

func requestUsesS3(req api.ProcessRequest) bool {
	if req.Output.S3 != "" {
		return true
	}
	for _, input := range req.Inputs {
		if input.S3 != "" {
			return true
		}
	}
	return false
}

// jobTempRoot resolves the directory holding job working directories. It is a
// variable so tests can isolate their temporary files from the host.
var jobTempRoot = os.TempDir

// createJobPath allocates this request's working directory. The returned
// cleanup is idempotent: repeated cancellation or overlapping shutdown paths
// cannot delete the directory twice or panic.
func createJobPath() (string, func(), error) {
	tempRoot := jobTempRoot()
	if err := os.MkdirAll(tempRoot, 0700); err != nil {
		return "", nil, err
	}
	path, err := os.MkdirTemp(tempRoot, "ffmpeg-api-")
	if err != nil {
		return "", nil, err
	}
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			log.Println("cleaning job dir:", path)
			if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
				log.Println("job directory cleanup failed:", err)
			}
		})
	}
	return path, cleanup, nil
}

// fetchInputs downloads all non-temporary inputs concurrently. The first
// genuine failure cancels the remaining transfers; cancellation errors caused
// by that shutdown are not reported as the primary failure.
func fetchInputs(ctx context.Context, s3Client *s3.Client, inputs map[string]api.Input, jobPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(inputs))
	for name, input := range inputs {
		if input.Temporary {
			continue
		}
		wg.Add(1)
		go func(name string, input api.Input) {
			defer wg.Done()
			log.Println("fetching input:", name)
			dst := filepath.Join(jobPath, name)
			if err := fetchInput(ctx, s3Client, input, dst); err != nil {
				errCh <- fmt.Errorf("input %s: %w", name, err)
				cancel()
			}
		}(name, input)
	}
	wg.Wait()
	close(errCh)

	var primary error
	for err := range errCh {
		if primary == nil || (isCancellation(primary) && !isCancellation(err)) {
			primary = err
		}
	}
	return primary
}

func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func fetchInput(ctx context.Context, s3Client *s3.Client, input api.Input, dst string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch {
	case input.S3 != "":
		return internal.DownloadFromS3(ctx, s3Client, input.S3, dst)
	case input.HTTP != "":
		return internal.DownloadFromHTTP(ctx, input.HTTP, dst)
	case input.Base64 != "":
		return internal.WriteBase64ToFile(ctx, input.Base64, dst)
	default:
		return fmt.Errorf("input source missing")
	}
}

func runFFmpeg(ctx context.Context, commands [][]string, dir string) ([]string, error) {
	for i, args := range commands {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		start := time.Now()
		log.Printf("ffmpeg step %d: %s", i+1, strings.Join(sanitizeArgs(args), " "))
		stderr, err := executeFFmpeg(ctx, args, dir, nil)
		if err != nil {
			return stderr, err
		}
		log.Printf("ffmpeg step %d finished in %s", i+1, time.Since(start))
	}
	return nil, nil
}

// ffmpegBinary returns the executable to run. FFMPEG_BINARY allows an
// alternative binary to be injected for tests and staged deployments.
func ffmpegBinary() string {
	if value := os.Getenv("FFMPEG_BINARY"); value != "" {
		return value
	}
	return "ffmpeg"
}

func executeFFmpeg(ctx context.Context, args []string, dir string, stdout io.Writer) ([]string, error) {
	cmd := internal.CommandContext(ctx, ffmpegBinary(), sanitizeArgs(args)...)
	cmd.Dir = dir
	if stdout != nil {
		cmd.Stdout = stdout
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	return consoleLines(stderr.String()), err
}

func runFFmpegStreaming(ctx context.Context, cancel context.CancelFunc, w http.ResponseWriter, req api.ProcessRequest, dir string) error {
	head := req.Commands[:len(req.Commands)-1]
	last := req.Commands[len(req.Commands)-1]
	if err := ctx.Err(); err != nil {
		return err
	}
	if stderr, err := runFFmpeg(ctx, head, dir); err != nil {
		logFFmpegError(stderr)
		if requestCanceled(ctx) {
			log.Println("streaming preparation canceled, response suppressed")
			return err
		}
		respondConsoleError(ctx, w, http.StatusInternalServerError, err.Error(), stderr)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	w.Header().Set("Trailer", statusTrailer+", "+bytesTrailer)
	w.Header().Set("Content-Type", req.Output.InlineContentType)
	w.WriteHeader(http.StatusOK)
	stream := &streamWriter{
		writer:  w,
		control: http.NewResponseController(w),
		cancel:  cancel,
	}

	step := len(req.Commands)
	start := time.Now()
	log.Printf("ffmpeg step %d: %s", step, strings.Join(sanitizeArgs(last), " "))
	stderr, err := executeFFmpeg(ctx, last, dir, stream)

	// A failed response write invalidates the transfer even if FFmpeg itself
	// happened to exit successfully: the caller did not receive the whole body.
	written := stream.bytesWritten()
	if writeErr := stream.err(); writeErr != nil && err == nil {
		err = writeErr
	}
	w.Header().Set(bytesTrailer, strconv.FormatInt(written, 10))
	if err != nil {
		w.Header().Set(statusTrailer, "error")
		logFFmpegError(stderr)
		return fmt.Errorf("ffmpeg failed after %d bytes: %w", written, err)
	}
	w.Header().Set(statusTrailer, "ok")
	log.Printf("ffmpeg step %d finished in %s (%d bytes streamed)", step, time.Since(start), written)
	return nil
}

// streamWriter forwards FFmpeg stdout to the response and records how many
// media bytes the response accepted. Every chunk is flushed immediately so a
// broken connection surfaces while FFmpeg is still running: the failed write
// cancels the request, which terminates the FFmpeg process instead of letting
// it drain output into a dead connection.
type streamWriter struct {
	writer  io.Writer
	control *http.ResponseController
	cancel  context.CancelFunc

	mu      sync.Mutex
	failed  error
	written int64
}

func (w *streamWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if n > 0 {
		w.add(int64(n))
	}
	if err == nil && n > 0 && w.control != nil {
		err = w.control.Flush()
	}
	if err != nil {
		w.recordFailure(err)
		if w.cancel != nil {
			w.cancel()
		}
		return n, err
	}
	return n, nil
}

func (w *streamWriter) add(delta int64) {
	w.mu.Lock()
	w.written += delta
	w.mu.Unlock()
}

func (w *streamWriter) recordFailure(err error) {
	w.mu.Lock()
	if w.failed == nil {
		w.failed = err
	}
	w.mu.Unlock()
}

func (w *streamWriter) err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed
}

func (w *streamWriter) bytesWritten() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

func consoleLines(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return strings.Split(value, "\n")
}

func logFFmpegError(stderr []string) {
	log.Println("ffmpeg execution failed")
	for _, line := range stderr {
		log.Println("ffmpeg:", line)
	}
}

func sanitizeArgs(args []string) []string {
	out := make([]string, 1, len(args)+1)
	out[0] = "-hide_banner"
	for _, arg := range args {
		if arg != "-hide_banner" {
			out = append(out, arg)
		}
	}
	return out
}

func collectResults(ctx context.Context, s3Client *s3.Client, req api.ProcessRequest, jobPath string) (map[string]api.Result, error) {
	files, err := outputFiles(jobPath, req.Inputs)
	if err != nil {
		return nil, err
	}
	results := make(map[string]api.Result, len(files))
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		log.Println("collecting output:", file.Name())
		path := filepath.Join(jobPath, file.Name())
		result := api.Result{}
		if req.Output.S3 != "" {
			url, err := internal.UploadToS3(ctx, s3Client, req.Output.S3, path, file.Name())
			if err != nil {
				return nil, err
			}
			result.URL = url
		}
		if req.Output.Base64 {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			result.Base64 = base64.StdEncoding.EncodeToString(data)
		}
		results[file.Name()] = result
	}
	return results, nil
}

func outputFiles(jobPath string, inputs map[string]api.Input) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(jobPath)
	if err != nil {
		return nil, err
	}
	files := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, isInput := inputs[entry.Name()]; isInput {
			continue
		}
		files = append(files, entry)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("ffmpeg produced no output files")
	}
	return files, nil
}

func serveFirstResult(ctx context.Context, w http.ResponseWriter, r *http.Request, jobPath string, req api.ProcessRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	files, err := outputFiles(jobPath, req.Inputs)
	if err != nil {
		return err
	}
	file := files[0]
	log.Println("serving inline output:", file.Name())
	w.Header().Set("Content-Type", req.Output.InlineContentType)
	http.ServeFile(w, r, filepath.Join(jobPath, file.Name()))
	return nil
}
