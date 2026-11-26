package internal

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type endlessReader struct{}

func (endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestCopyWithContextStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := CopyWithContext(ctx, &nopWriter{}, endlessReader{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestCopyWithContextHonorsCancelMidStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		_, err := CopyWithContext(ctx, &nopWriter{}, &slowReader{})
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CopyWithContext did not stop after cancellation")
	}
}

func TestWriteReaderToFileRemovesPartialOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dst := filepath.Join(t.TempDir(), "input.bin")
	err := WriteReaderToFile(ctx, dst, strings.NewReader("data"))
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Fatalf("partial file %s was not removed", dst)
	}
}

func TestWriteReaderToFileWritesCompleteFile(t *testing.T) {
	ctx := context.Background()
	dst := filepath.Join(t.TempDir(), "input.bin")
	if err := WriteReaderToFile(ctx, dst, strings.NewReader("payload")); err != nil {
		t.Fatalf("WriteReaderToFile failed: %v", err)
	}
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "payload" {
		t.Fatalf("file content = %q, err = %v", data, err)
	}
}

type nopWriter struct{}

func (*nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// slowReader returns data slowly so cancellation can occur mid-transfer.
type slowReader struct{ n int }

func (r *slowReader) Read(p []byte) (int, error) {
	if r.n >= 1000 {
		return 0, io.EOF
	}
	time.Sleep(5 * time.Millisecond)
	r.n++
	for i := range p {
		p[i] = 'y'
	}
	return len(p), nil
}
