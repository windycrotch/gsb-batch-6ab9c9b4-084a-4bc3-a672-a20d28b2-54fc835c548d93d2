package httpuploader

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// ErrNonRetryableResponse is returned when the origin answers with a response
// that must not be retried (for example any 4xx). It is wrapped (use
// errors.Is) inside the error returned to the caller.
var ErrNonRetryableResponse = errors.New("non-retryable upload error")

// UploadError is the error returned to the caller when an upload fails after
// the retry policy has been exhausted (or immediately for non-retryable
// responses). It carries the destination path so logs can point at the exact
// segment / manifest that failed.
type UploadError struct {
	DstPathFile string
	StatusCode  int // 0 when the request never got an HTTP response (transport error)
	Retries     int // number of retries already consumed
	Err         error
}

func (e *UploadError) Error() string {
	if e.StatusCode > 0 {
		return fmt.Sprintf("upload error for %q after %d retries, last HTTP status %d: %v",
			e.DstPathFile, e.Retries, e.StatusCode, e.Err)
	}
	return fmt.Sprintf("upload error for %q after %d retries: %v",
		e.DstPathFile, e.Retries, e.Err)
}

// Unwrap allows errors.Is / errors.As against the inner error
// (e.g. ErrNonRetryableResponse).
func (e *UploadError) Unwrap() error {
	return e.Err
}

// HTTPUploader HTTP uploader class class
type HTTPUploader struct {
	HTTPClient *http.Client

	Log                     *logrus.Logger
	HTTPSInsecure           bool
	HTTPScheme              string
	HTTPHost                string
	MaxHTTPRetries          int
	InitialHTTPRetryDelayMs int
}

// New Creates a chunk instance
func New(log *logrus.Logger, httpsInsecure bool, httpScheme string, httpHost string, maxHTTPRetries int, initialHTTPRetryDelayMs int) HTTPUploader {
	if log == nil {
		log = logrus.New()
		log.SetLevel(logrus.DebugLevel)
	}

	var tr = http.DefaultTransport
	if (strings.Compare(httpScheme, "https") == 0) && (httpsInsecure) {
		// Setup HTTPS client in dev env, skips CA verification
		log.Warn("Skipping CA cert verification!")
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		tr = &http.Transport{TLSClientConfig: tlsConfig}
	}
	client := http.Client{
		Transport: tr,
		Timeout:   0,
	}
	h := HTTPUploader{&client, log, httpsInsecure, httpScheme, httpHost, maxHTTPRetries, initialHTTPRetryDelayMs}

	return h
}

// UploadLocalFile Uploads a file from the filesystem
func (h *HTTPUploader) UploadLocalFile(localFilename string, dstPathFile string, headers map[string]string) error {
	f, errOpen := os.Open(localFilename)
	if errOpen != nil {
		h.Log.Error("ERROR reading ", localFilename, "(", dstPathFile, ")")
		return errOpen
	}
	defer f.Close()

	return h.uploadDataRetries(f, dstPathFile, headers)
}

// UploadData Uploads data array
func (h *HTTPUploader) UploadData(data []byte, dstPathFile string, headers map[string]string) error {
	return h.uploadDataRetries(bytes.NewReader(data), dstPathFile, headers)
}

// UploadChunkedTransfer Uploads data as soon as arrives to the returned channel (no retries for chunked transfer, future improvement)
func (h *HTTPUploader) UploadChunkedTransfer(dstPathFile string, headers map[string]string) chan []byte {
	r, w := io.Pipe()
	writeChan := make(chan []byte)

	// open request
	req := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: h.HTTPScheme,
			Host:   h.HTTPHost,
			Path:   "/" + dstPathFile,
		},
		ProtoMajor:    1,
		ProtoMinor:    1,
		ContentLength: -1,
		Body:          r,
		Header:        http.Header{},
	}

	// Add headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	go func() {
		defer w.Close()

		for buf := range writeChan {
			n, err := w.Write(buf)
			h.Log.Debug("Wrote ", n, " bytes to ", dstPathFile)
			if n != len(buf) && err != nil {
				panic(err)
			}
		}
	}()

	go func() {
		h.Log.Debug("Opening connection to upload to ", dstPathFile)
		h.Log.Debug("Req: ", req)
		_, err := h.HTTPClient.Do(req)

		if err != nil {
			h.Log.Error("Error uploading to ", dstPathFile, ". Error: ", err)
		} else {
			h.Log.Debug("Upload to ", dstPathFile, " complete")
		}
	}()

	return writeChan
}

// retryDelay returns the time to wait before the attempt that follows the
// given attempt number (0 based). The delay grows linearly with the number of
// attempts already made: attempt 0 waits InitialHTTPRetryDelayMs, attempt 1
// waits 2x, etc., matching the original "Value = intent * initialHttpRetryDelay"
// flag description.
func (h *HTTPUploader) retryDelay(attempt int) time.Duration {
	return time.Duration(int64(h.InitialHTTPRetryDelayMs)*int64(attempt+1)) * time.Millisecond
}

// uploadDataRetries uploads the payload applying the retry policy.
//
// Invariant: this function returns nil only when the origin acknowledged the
// full object with a success status. Any other outcome (retryable responses
// such as 503 exhausted, non-retryable responses such as 4xx, transport
// errors) is returned as an error so the caller can keep the object out of the
// HLS manifest and terminate with a non-zero status.
//
// Seekable payloads (the *os.File used by UploadLocalFile and the
// *bytes.Reader used by UploadData) are rewound to the start before every
// attempt, so a retry never resends a half-consumed body and the streaming
// memory profile is preserved. Any other reader is buffered once as a
// fallback. Every attempt targets the same destination path with the same
// full payload, therefore the origin ends up with a single complete object
// (an overwrite of the same key) instead of duplicate or truncated objects.
func (h *HTTPUploader) uploadDataRetries(dataReader io.Reader, dstPathFile string, headers map[string]string) error {
	// prepareReader yields a reader positioned at the start of the payload.
	var prepareReader func() (io.Reader, error)
	if seeker, ok := dataReader.(io.Seeker); ok {
		prepareReader = func() (io.Reader, error) {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
			return dataReader, nil
		}
	} else {
		payload, errRead := ioutil.ReadAll(dataReader)
		if errRead != nil {
			h.Log.Error("Error reading the data to upload to ", dstPathFile, ". Error: ", errRead)
			return &UploadError{DstPathFile: dstPathFile, Retries: 0, Err: errRead}
		}
		prepareReader = func() (io.Reader, error) { return bytes.NewReader(payload), nil }
	}

	// MaxHTTPRetries keeps its original meaning: number of retries, on top of
	// the initial attempt. A non-positive value means a single attempt.
	maxAttempts := h.MaxHTTPRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	lastStatusCode := 0

	for attempt := 0; attempt < maxAttempts; attempt++ {
		bodyReader, errPrepare := prepareReader()
		if errPrepare != nil {
			h.Log.Error("Error rewinding payload for ", dstPathFile, ". Error: ", errPrepare)
			return &UploadError{DstPathFile: dstPathFile, StatusCode: lastStatusCode, Retries: attempt, Err: errPrepare}
		}

		statusCode, attemptErr := h.uploadData(bodyReader, dstPathFile, headers)
		lastStatusCode = statusCode

		if attemptErr == nil {
			if attempt > 0 {
				h.Log.Info("Upload to ", dstPathFile, " complete after ", attempt, " retries")
			}
			return nil
		}
		lastErr = attemptErr

		// Non-retryable responses (e.g. 4xx) fail immediately, they do not
		// consume any remaining retry budget.
		if errors.Is(attemptErr, ErrNonRetryableResponse) {
			h.Log.Error("Non-retryable error uploading to ", dstPathFile, ", failing immediately. HTTP status: ", statusCode)
			return &UploadError{DstPathFile: dstPathFile, StatusCode: statusCode, Retries: attempt, Err: attemptErr}
		}

		h.Log.Warn("Retryable error uploading to ", dstPathFile, " (attempt ", attempt+1, "/", maxAttempts, "). Error: ", attemptErr)

		if attempt < maxAttempts-1 {
			delay := h.retryDelay(attempt)
			h.Log.Debug("Waiting ", delay, " before retrying upload to ", dstPathFile)
			time.Sleep(delay)
		}
	}

	h.Log.Error("ERROR data lost because server busy, ", dstPathFile, ". Retries exhausted: ", h.MaxHTTPRetries)
	return &UploadError{DstPathFile: dstPathFile, StatusCode: lastStatusCode, Retries: h.MaxHTTPRetries, Err: lastErr}
}

// uploadData performs a single POST. It returns the received HTTP status code
// (0 when no response was received) together with an error. A nil error means
// the origin accepted the object (any 2xx/3xx status). 4xx responses are
// flagged as non-retryable via ErrNonRetryableResponse, 5xx responses and
// transport errors are flagged as retryable.
func (h *HTTPUploader) uploadData(fileData io.Reader, dstPathFile string, headers map[string]string) (int, error) {
	req := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: h.HTTPScheme,
			Host:   h.HTTPHost,
			Path:   "/" + dstPathFile,
		},
		ProtoMajor:    1,
		ProtoMinor:    1,
		ContentLength: -1,
		Body:          ioutil.NopCloser(fileData),
		Header:        http.Header{},
	}

	// Add headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, errReq := h.HTTPClient.Do(req)
	if errReq != nil {
		// Transport error: nothing was acknowledged by the origin, safe to
		// retry with the full payload.
		h.Log.Error("Error uploading to ", dstPathFile, ")", "Error: ", errReq)
		return 0, errReq
	}
	defer resp.Body.Close()
	// Drain the body so the underlying TCP connection can be reused.
	io.Copy(ioutil.Discard, resp.Body)

	if resp.StatusCode < 400 {
		// Done
		h.Log.Info("Upload to ", dstPathFile, " complete")
		return resp.StatusCode, nil
	}

	if resp.StatusCode >= 500 {
		// Retryable origin error (503 and any other 5xx)
		h.Log.Debug("Warning server busy, uploading to ", dstPathFile, ", RETRYING! HTTP status: ", resp.StatusCode)
		return resp.StatusCode, fmt.Errorf("retryable upload error, HTTP status %d", resp.StatusCode)
	}

	// 4xx and any other non-retryable error
	h.Log.Error("Error server uploading to ", dstPathFile, ")", "HTTP Error: ", resp.StatusCode)
	return resp.StatusCode, fmt.Errorf("%w, HTTP status %d", ErrNonRetryableResponse, resp.StatusCode)
}
