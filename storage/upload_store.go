package storage

import (
	"context"
	"errors"
)

// ErrObjectNotFound is returned by UploadStore.ObjectSize when the final
// object does not exist. Implementations translate provider-specific
// "not found" errors to this sentinel so callers can reconcile state
// without depending on an SDK or cloud vendor.
var ErrObjectNotFound = errors.New("storage object not found")

// ErrUploadNotFound is returned when a multipart upload ID no longer exists
// (already completed or aborted). Abort paths treat it as success because
// the desired state - no resumable multipart upload - already holds.
var ErrUploadNotFound = errors.New("multipart upload not found")

// UploadPart is one uploaded multipart part as reported by the store.
type UploadPart struct {
	PartNumber int32
	ETag       string
}

// UploadStore is the narrow object-storage surface used by the upload
// creation / completion / abort / cleanup entries.
//
// The production implementation is the S3 adapter in s3_upload_store.go.
// Tests replace it with an in-memory recording store to assert call
// sequences without real cloud credentials.
//
// Implementations must be safe for concurrent use.
type UploadStore interface {
	// CreateMultipartUpload starts a new multipart upload and returns its ID.
	CreateMultipartUpload(ctx context.Context, key, contentType string) (uploadID string, err error)

	// PresignUploadPart returns a time-limited URL for one browser PUT.
	PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int32) (string, error)

	// ListParts returns the parts currently stored for the upload.
	// Returns ErrUploadNotFound when the upload ID is gone.
	ListParts(ctx context.Context, key, uploadID string) ([]UploadPart, error)

	// CompleteMultipartUpload merges the given ordered parts into the final object.
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []UploadPart) error

	// AbortMultipartUpload discards uploaded parts. ErrUploadNotFound is
	// tolerated by callers; adapters may return it or nil for missing uploads.
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error

	// ObjectSize returns the size of the final merged object.
	// Returns ErrObjectNotFound when the key is absent.
	ObjectSize(ctx context.Context, key string) (int64, error)

	// DeleteObject removes the final object. Deleting an absent object is
	// expected to succeed (S3 semantics).
	DeleteObject(ctx context.Context, key string) error
}
