package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, map[string]string{"error": msg})
}

func WriteConsoleError(w http.ResponseWriter, status int, msg string, console []string) {
	WriteJSON(w, status, map[string]any{"error": msg, "console": console})
}

// DownloadFromHTTP streams src into dst. The transfer stops as soon as ctx is
// canceled, and a partial file is removed instead of being left behind.
func DownloadFromHTTP(ctx context.Context, src, dst string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request for %s: %w", src, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to GET %s: %w", src, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed: %s", resp.Status)
	}

	return WriteReaderToFile(ctx, dst, resp.Body)
}

// WriteReaderToFile writes src into a new file at dst. On cancellation or any
// write error the partial file is removed, so callers never observe a
// truncated input as if it were complete.
func WriteReaderToFile(ctx context.Context, dst string, src io.Reader) error {
	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", dst, err)
	}
	_, copyErr := CopyWithContext(ctx, out, src)
	closeErr := out.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		os.Remove(dst)
		return fmt.Errorf("failed to write file %s: %w", dst, copyErr)
	}
	return nil
}

// CopyWithContext copies src into dst and checks for cancellation between
// reads, so an in-flight transfer does not keep the request alive.
func CopyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			copied, writeErr := dst.Write(buf[:n])
			written += int64(copied)
			if writeErr != nil {
				return written, writeErr
			}
			if copied != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}
