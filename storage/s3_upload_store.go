package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3UploadStore is the production UploadStore backed by an S3-compatible
// object store (AWS S3, Appwrite's S3 gateway, MinIO, ...).
type S3UploadStore struct {
	client *s3.Client
	bucket string
}

// NewS3UploadStore adapts a configured S3 client to UploadStore.
func NewS3UploadStore(client *s3.Client, bucket string) *S3UploadStore {
	return &S3UploadStore{client: client, bucket: bucket}
}

// Client exposes the underlying SDK client for code paths that still need
// streaming access (video download during processing).
func (s *S3UploadStore) Client() *s3.Client { return s.client }

func (s *S3UploadStore) CreateMultipartUpload(ctx context.Context, key, contentType string) (string, error) {
	result, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", fmt.Errorf("create S3 multipart upload: %w", err)
	}
	if result.UploadId == nil || *result.UploadId == "" {
		return "", fmt.Errorf("create S3 multipart upload: empty upload ID")
	}
	return *result.UploadId, nil
}

func (s *S3UploadStore) PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int32) (string, error) {
	request, err := s3.NewPresignClient(s.client).PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID), PartNumber: aws.Int32(partNumber),
	})
	if err != nil {
		return "", fmt.Errorf("presign S3 upload part: %w", err)
	}
	return request.URL, nil
}

func (s *S3UploadStore) ListParts(ctx context.Context, key, uploadID string) ([]UploadPart, error) {
	storedParts, err := s.client.ListParts(ctx, &s3.ListPartsInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID), MaxParts: aws.Int32(1000),
	})
	if err != nil {
		if isNoSuchUpload(err) {
			return nil, fmt.Errorf("list S3 parts: %w", errors.Join(ErrUploadNotFound, err))
		}
		return nil, fmt.Errorf("list S3 parts: %w", err)
	}
	parts := make([]UploadPart, 0, len(storedParts.Parts))
	for _, part := range storedParts.Parts {
		if part.PartNumber != nil && part.ETag != nil {
			parts = append(parts, UploadPart{PartNumber: *part.PartNumber, ETag: *part.ETag})
		}
	}
	return parts, nil
}

func (s *S3UploadStore) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []UploadPart) error {
	completedParts := make([]s3types.CompletedPart, len(parts))
	for i, part := range parts {
		completedParts[i] = s3types.CompletedPart{PartNumber: aws.Int32(part.PartNumber), ETag: aws.String(part.ETag)}
	}
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: completedParts},
	})
	if err != nil {
		if isNoSuchUpload(err) {
			return fmt.Errorf("complete S3 multipart upload: %w", errors.Join(ErrUploadNotFound, err))
		}
		return fmt.Errorf("complete S3 multipart upload: %w", err)
	}
	return nil
}

func (s *S3UploadStore) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
	if err != nil {
		if isNoSuchUpload(err) {
			return fmt.Errorf("abort S3 multipart upload: %w", errors.Join(ErrUploadNotFound, err))
		}
		return fmt.Errorf("abort S3 multipart upload: %w", err)
	}
	return nil
}

func (s *S3UploadStore) ObjectSize(ctx context.Context, key string) (int64, error) {
	// Uses the same ListObjectsV2 exact-key lookup as the original
	// GetObjectSize helper, so credentials/mocks that permit listing keep
	// working and behavior is identical across S3-compatible gateways.
	result, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(key), MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return 0, fmt.Errorf("list S3 object %q: %w", key, err)
	}
	for _, object := range result.Contents {
		if object.Key != nil && *object.Key == key && object.Size != nil {
			return *object.Size, nil
		}
	}
	return 0, fmt.Errorf("list S3 object %q: %w", key, errors.Join(ErrObjectNotFound, fmt.Errorf("S3 object %q was not found", key)))
}

func (s *S3UploadStore) DeleteObject(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete S3 object %q: %w", key, err)
	}
	return nil
}

// isNoSuchUpload reports whether err represents a gone multipart upload,
// covering both the modeled type and generic API errors from third-party gateways.
func isNoSuchUpload(err error) bool {
	var noSuchUpload *s3types.NoSuchUpload
	if errors.As(err, &noSuchUpload) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" {
		return true
	}
	return false
}

// IsNotFound reports whether err is (or wraps) ErrObjectNotFound.
func IsNotFound(err error) bool { return errors.Is(err, ErrObjectNotFound) }

// IsUploadNotFound reports whether err is (or wraps) ErrUploadNotFound.
func IsUploadNotFound(err error) bool { return errors.Is(err, ErrUploadNotFound) }
