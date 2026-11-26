package storage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// mockS3 records the wire request sequence and answers just enough S3 XML to
// drive the real aws-sdk-go-v2 client through S3UploadStore. This verifies
// the production adapter without real cloud credentials.
type mockS3 struct {
	mu        sync.Mutex
	requests  []string
	listCount int
}

func (m *mockS3) handler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	m.mu.Lock()
	var label string
	switch {
	case r.Method == http.MethodGet && q.Has("list-type"):
		label = "LIST_OBJECTS prefix=" + q.Get("prefix")
	case r.Method == http.MethodGet && q.Get("uploadId") != "":
		label = r.Method + " " + r.URL.Path + "?uploadId=" + q.Get("uploadId")
	default:
		label = r.Method + " " + r.URL.Path
		if id := q.Get("uploadId"); id != "" {
			label += "?uploadId=" + id
		}
		if q.Has("uploads") {
			label += "?uploads"
		}
	}
	m.requests = append(m.requests, label)
	listCount := m.listCount
	if r.Method == http.MethodGet && q.Has("list-type") {
		m.listCount++
	}
	m.mu.Unlock()

	w.Header().Set("x-amz-request-id", "mock-request-id")

	switch {
	case r.Method == http.MethodGet && q.Has("list-type"):
		w.Header().Set("Content-Type", "application/xml")
		if listCount == 0 {
			// Pre-merge reconcile: no final object yet.
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name><Prefix>raw/vid.mp4</Prefix><KeyCount>0</KeyCount>
</ListBucketResult>`))
			return
		}
		// Post-merge verification: exact key with the expected size.
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name><Prefix>raw/vid.mp4</Prefix><KeyCount>1</KeyCount>
  <Contents><Key>raw/vid.mp4</Key><Size>1024</Size></Contents>
</ListBucketResult>`))

	case r.Method == http.MethodGet && q.Get("uploadId") == "u1":
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>test-bucket</Bucket><Key>raw/vid.mp4</Key><UploadId>u1</UploadId>
  <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag><Size>1024</Size></Part>
</ListPartsResult>`))

	case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "u1":
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://localhost/raw/vid.mp4</Location>
  <Bucket>test-bucket</Bucket><Key>raw/vid.mp4</Key><ETag>"etag-final"</ETag>
</CompleteMultipartUploadResult>`))

	case r.Method == http.MethodPost:
		// CreateMultipartUpload (?uploads)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>test-bucket</Bucket><Key>raw/new.mp4</Key><UploadId>u-new</UploadId>
</InitiateMultipartUploadResult>`))

	case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "missing-upload":
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchUpload</Code><Message>upload does not exist</Message></Error>`))

	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func newMockS3Store(t *testing.T) (*S3UploadStore, *mockS3) {
	t.Helper()
	mock := &mockS3{}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	t.Cleanup(server.Close)

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("AKIATEST", "secret", "token"),
	})
	return NewS3UploadStore(client, "test-bucket"), mock
}

func (m *mockS3) snapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.requests))
	copy(out, m.requests)
	return out
}

// The production adapter must drive the reconcile -> list -> merge -> verify
// sequence against S3 and expose not-found conditions as sentinel errors.
func TestS3Adapter_CompleteSequenceAndNotFoundMapping(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockS3Store(t)
	const key, uploadID = "raw/vid.mp4", "u1"

	// 1. Pre-merge reconcile: absent object maps to ErrObjectNotFound.
	if _, err := store.ObjectSize(ctx, key); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("ObjectSize missing object err=%v, want ErrObjectNotFound", err)
	}

	// 2. List parts through the real XML adapter.
	parts, err := store.ListParts(ctx, key, uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts) != 1 || parts[0].PartNumber != 1 || parts[0].ETag != `"etag-1"` {
		t.Fatalf("unexpected parts: %#v", parts)
	}

	// 3. Merge once.
	if err := store.CompleteMultipartUpload(ctx, key, uploadID, parts); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	// 4. Post-merge verification HEAD succeeds.
	size, err := store.ObjectSize(ctx, key)
	if err != nil {
		t.Fatalf("ObjectSize merged object: %v", err)
	}
	if size != 1024 {
		t.Fatalf("size=%d want 1024", size)
	}

	got := mock.snapshot()
	wantPrefix := []string{
		"LIST_OBJECTS prefix=raw/vid.mp4",
		"GET /test-bucket/raw/vid.mp4?uploadId=u1",
		"POST /test-bucket/raw/vid.mp4?uploadId=u1",
		"LIST_OBJECTS prefix=raw/vid.mp4",
	}
	if len(got) < len(wantPrefix) {
		t.Fatalf("wire sequence too short: %v", got)
	}
	for i, want := range wantPrefix {
		if got[i] != want {
			t.Fatalf("request #%d = %q, want %q\nfull sequence: %v", i, got[i], want, got)
		}
	}
}

func TestS3Adapter_CreatePresignAbortDelete(t *testing.T) {
	ctx := context.Background()
	store, mock := newMockS3Store(t)

	id, err := store.CreateMultipartUpload(ctx, "raw/new.mp4", "video/mp4")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if id != "u-new" {
		t.Fatalf("upload id=%q want u-new", id)
	}
	url, err := store.PresignUploadPart(ctx, "raw/new.mp4", id, 1)
	if err != nil || !strings.Contains(url, "partNumber=1") {
		t.Fatalf("presign url=%q err=%v", url, err)
	}
	if err := store.AbortMultipartUpload(ctx, "raw/new.mp4", id); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	if err := store.DeleteObject(ctx, "raw/new.mp4"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// NoSuchUpload XML must map to the sentinel so callers tolerate it.
	err = store.AbortMultipartUpload(ctx, "raw/x.mp4", "missing-upload")
	if !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("abort missing upload err=%v, want ErrUploadNotFound", err)
	}

	seq := mock.snapshot()
	wantCalls := map[string]bool{
		"POST /test-bucket/raw/new.mp4?uploads":                 false,
		"DELETE /test-bucket/raw/new.mp4?uploadId=u-new":        false,
		"DELETE /test-bucket/raw/x.mp4?uploadId=missing-upload": false,
		"DELETE /test-bucket/raw/new.mp4":                       false,
	}
	for _, call := range seq {
		if _, ok := wantCalls[call]; ok {
			wantCalls[call] = true
		}
	}
	for call, seen := range wantCalls {
		if !seen {
			t.Fatalf("wire sequence missing exact call %q\nfull sequence: %v", call, seq)
		}
	}
}
