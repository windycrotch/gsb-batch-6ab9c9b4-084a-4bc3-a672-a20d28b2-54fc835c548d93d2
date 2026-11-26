package controllers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"video-streaming-server/config"
	"video-streaming-server/shared/logger"
	"video-streaming-server/storage"
	"video-streaming-server/storagekeys"
	"video-streaming-server/types"
	"video-streaming-server/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

const (
	abandonedUploadTTL      = 24 * time.Hour
	abandonedUploadInterval = 15 * time.Minute
)

// UploadStoreFactory builds the object-storage gateway used by every upload
// entry point. Production returns the S3 adapter; automated tests replace it
// with an in-memory recording store so call sequences are verifiable without
// cloud credentials.
var UploadStoreFactory = func(ctx context.Context) (storage.UploadStore, error) {
	client, err := storage.NewS3Client(ctx)
	if err != nil {
		return nil, err
	}
	return storage.NewS3UploadStore(client, config.AppConfig.AppwriteBucketID), nil
}

// SpawnUploadProcessing starts the asynchronous download/transcode pipeline
// after an upload record commits to UploadedOnServer. Tests replace it with a
// recording no-op so scenarios stay hermetic and fast.
var SpawnUploadProcessing = func(db *sql.DB, key, videoID, title, userID string) {
	go func() {
		ctx := context.Background()
		client, err := storage.NewS3Client(ctx)
		if err != nil {
			logger.Log.Error("failed to create S3 client for post-upload processing", "video_id", videoID, "error", err)
			return
		}
		downloadAndProcessVideo(ctx, client, key, videoID, title, db, types.UserID(userID))
	}()
}

type createUploadRequest struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	FileName    string `json:"file_name"`
	FileSize    int64  `json:"file_size"`
	ContentType string `json:"content_type"`
}

type completeUploadRequest struct {
	Parts []completedUploadPart `json:"parts"`
}

type completedUploadPart struct {
	PartNumber int32  `json:"part_number"`
	ETag       string `json:"etag"`
}

type uploadPartURL struct {
	PartNumber int32  `json:"part_number"`
	UploadURL  string `json:"upload_url"`
}

// pendingUploadRow is the authoritative upload state read under the
// per-video advisory lock.
type pendingUploadRow struct {
	Title        string
	Key          string
	UploadID     string
	ExpectedSize int64
	UserID       string
}

var errNoPendingUpload = errors.New("no pending upload row")

func CreateVideoUpload(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	user, err := utils.GetUserFromRequest(r)
	if err != nil {
		utils.SendError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	var input createUploadRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&input); err != nil {
		utils.SendError(w, http.StatusBadRequest, "Invalid upload request")
		return
	}
	if strings.TrimSpace(input.Title) == "" || input.FileSize <= 0 || filepath.Base(input.FileName) != input.FileName || input.FileName == "" {
		utils.SendError(w, http.StatusBadRequest, "Title, valid file name, and positive file size are required")
		return
	}
	allowed := map[string]bool{"video/mp4": true, "video/x-matroska": true, "video/quicktime": true}
	if !allowed[input.ContentType] {
		utils.SendError(w, http.StatusBadRequest, "Unsupported video type")
		return
	}
	limit, err := strconv.ParseInt(config.AppConfig.FileSizeLimit, 10, 64)
	if err != nil {
		logger.Log.Error("invalid FILE_SIZE_LIMIT configuration", "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Upload service is not configured correctly")
		return
	}
	if limit <= 0 {
		logger.Log.Error("invalid FILE_SIZE_LIMIT configuration", "reason", "must be a positive integer")
		utils.SendError(w, http.StatusInternalServerError, "Upload service is not configured correctly")
		return
	}
	if input.FileSize > limit {
		utils.SendError(w, http.StatusRequestEntityTooLarge, "File too large.")
		return
	}
	if (input.FileSize-1)/storage.MultipartPartSize+1 > 10000 {
		utils.SendError(w, http.StatusBadRequest, "File size exceeds the multipart upload limit")
		return
	}
	ext := strings.ToLower(filepath.Ext(input.FileName))
	validExt := map[string]bool{".mp4": true, ".mkv": true, ".mov": true}
	if !validExt[ext] {
		utils.SendError(w, http.StatusBadRequest, "Unsupported video file extension")
		return
	}
	store, err := UploadStoreFactory(r.Context())
	if err != nil {
		logger.Log.Error("failed to create storage client", "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Unable to prepare video upload")
		return
	}
	videoID := uuid.NewString()
	key := storagekeys.RawFile(videoID, ext)
	uploadID, err := store.CreateMultipartUpload(r.Context(), key, input.ContentType)
	if err != nil {
		logger.Log.Error("failed to create multipart upload", "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Unable to prepare video upload")
		return
	}
	_, err = db.Exec(`INSERT INTO videos (video_id,title,description,upload_initiate_time,status,delete_flag,user_id,source_file_key,source_file_size,source_upload_id) VALUES ($1,$2,$3,$4,$5,0,$6,$7,$8,$9)`, videoID, input.Title, input.Description, time.Now(), types.UploadPending, user.ID, key, input.FileSize, uploadID)
	if err != nil {
		logger.Log.Error("failed to create video record", "error", err)
		if abortErr := store.AbortMultipartUpload(r.Context(), key, uploadID); abortErr != nil && !storage.IsUploadNotFound(abortErr) {
			logger.Log.Error("failed to abort untracked multipart upload", "error", abortErr)
		}
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}
	partCount := int((input.FileSize-1)/storage.MultipartPartSize + 1)
	partURLs := make([]uploadPartURL, partCount)
	for i := range partURLs {
		partNumber := int32(i + 1)
		uploadURL, presignErr := store.PresignUploadPart(r.Context(), key, uploadID, partNumber)
		if presignErr != nil {
			logger.Log.Error("failed to presign multipart upload part", "video_id", videoID, "part_number", partNumber, "error", presignErr)
			if abortErr := store.AbortMultipartUpload(r.Context(), key, uploadID); abortErr != nil && !storage.IsUploadNotFound(abortErr) {
				logger.Log.Error("failed to abort multipart upload after presigning error", "video_id", videoID, "error", abortErr)
			}
			if _, deleteErr := db.Exec(`DELETE FROM videos WHERE video_id=$1 AND user_id=$2 AND status=$3`, videoID, user.ID, types.UploadPending); deleteErr != nil {
				logger.Log.Error("failed to remove upload after presigning error", "video_id", videoID, "error", deleteErr)
			}
			utils.SendError(w, http.StatusInternalServerError, "Unable to prepare video upload")
			return
		}
		partURLs[i] = uploadPartURL{PartNumber: partNumber, UploadURL: uploadURL}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"video_id": videoID, "part_size": storage.MultipartPartSize, "parts": partURLs})
}

func AbortVideoUpload(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	user, err := utils.GetUserFromRequest(r)
	if err != nil {
		utils.SendError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) != 3 {
		utils.SendError(w, http.StatusBadRequest, "Invalid upload request")
		return
	}
	videoID := pathParts[1]
	releaseVideoLock, err := utils.AcquireVideoLock(r.Context(), db, videoID)
	if err != nil {
		logger.Log.Error("failed to lock video upload for abort", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Unable to cancel upload")
		return
	}
	defer releaseVideoLock()

	upload, err := loadPendingUpload(r.Context(), db, videoID, user.ID)
	if errors.Is(err, errNoPendingUpload) {
		// Already completed/advanced, already aborted, or never existed:
		// abort is idempotent and must not touch storage (A7).
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		logger.Log.Error("failed to find pending upload to abort", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}

	store, err := UploadStoreFactory(r.Context())
	if err != nil {
		logger.Log.Error("failed to create storage client to abort upload", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}

	// Reconcile before destroying anything. A previous completion attempt may
	// already have merged the object but failed to advance the record. In that
	// case abort must adopt the merge instead of orphaning/deleting it.
	presence, _, err := inspectSourceObject(r.Context(), store, upload.Key, upload.ExpectedSize)
	if err != nil {
		logger.Log.Error("failed to inspect source object before abort", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusBadGateway, "Unable to cancel upload; retry later")
		return
	}
	if presence == objectMatches {
		committed, commitErr := commitPendingToUploaded(r.Context(), db, videoID, user.ID, true)
		if commitErr != nil {
			logger.Log.Error("failed to adopt already-merged upload on abort", "video_id", videoID, "error", commitErr)
			utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
			return
		}
		if committed {
			SpawnUploadProcessing(db, upload.Key, videoID, upload.Title, upload.UserID)
		}
		logger.Log.Info("abort arrived after a merged object; upload adopted instead of deleted", "video_id", videoID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"video_id": videoID, "status": types.UploadedOnServer, "detail": "upload was already completed"})
		return
	}
	if presence == objectMismatch {
		// An object exists at the key but cannot be the promised source.
		// Remove the invalid object before discarding the record.
		if err := store.DeleteObject(r.Context(), upload.Key); err != nil {
			logger.Log.Error("failed to delete mismatched source object during abort", "video_id", videoID, "error", err)
			utils.SendError(w, http.StatusBadGateway, "Unable to cancel upload; retry later")
			return
		}
	}

	if err := store.AbortMultipartUpload(r.Context(), upload.Key, upload.UploadID); err != nil && !storage.IsUploadNotFound(err) {
		logger.Log.Error("failed to abort multipart upload", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Unable to cancel upload")
		return
	}
	if _, err := db.ExecContext(r.Context(), `DELETE FROM videos WHERE video_id=$1 AND user_id=$2 AND status=$3`, videoID, user.ID, types.UploadPending); err != nil {
		logger.Log.Error("failed to delete aborted upload record", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Unable to cancel upload")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// StartAbandonedUploadCleanup periodically removes multipart uploads that were
// created but never completed. The row's initiation time is the expiry clock.
func StartAbandonedUploadCleanup(db *sql.DB) {
	go func() {
		RunAbandonedUploadCleanup(db)
		ticker := time.NewTicker(abandonedUploadInterval)
		defer ticker.Stop()
		for range ticker.C {
			RunAbandonedUploadCleanup(db)
		}
	}()
}

// RunAbandonedUploadCleanup executes one sweep synchronously. It is invoked by
// the background ticker and by operational/test callers that need to trigger
// cleanup deterministically.
func RunAbandonedUploadCleanup(db *sql.DB) {
	cutoff := time.Now().Add(-abandonedUploadTTL)
	rows, err := db.Query(`SELECT video_id FROM videos WHERE status=$1 AND upload_initiate_time <= $2 ORDER BY upload_initiate_time ASC LIMIT 100`, types.UploadPending, cutoff)
	if err != nil {
		logger.Log.Error("failed to find abandoned uploads", "error", err)
		return
	}
	var videoIDs []string
	for rows.Next() {
		var videoID string
		if err := rows.Scan(&videoID); err != nil {
			logger.Log.Error("failed to read abandoned upload", "error", err)
			continue
		}
		videoIDs = append(videoIDs, videoID)
	}
	if err := rows.Err(); err != nil {
		logger.Log.Error("failed while reading abandoned uploads", "error", err)
	}
	_ = rows.Close()

	for _, videoID := range videoIDs {
		if err := cleanupAbandonedUpload(db, videoID, cutoff); err != nil {
			logger.Log.Error("failed to clean abandoned upload", "video_id", videoID, "error", err)
		}
	}
}

// cleanupAbandonedUploads is the historical unexported sweep entry point; it
// delegates to the exported, test-reachable RunAbandonedUploadCleanup.
func cleanupAbandonedUploads(db *sql.DB) {
	RunAbandonedUploadCleanup(db)
}

func cleanupAbandonedUpload(db *sql.DB, videoID string, cutoff time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	releaseVideoLock, err := utils.AcquireVideoLock(ctx, db, videoID)
	if err != nil {
		return fmt.Errorf("lock video upload: %w", err)
	}
	defer releaseVideoLock()

	// Re-read under the lock with the expiry predicate. A completion that
	// committed meanwhile makes this ErrNoPendingUpload -> nothing to do.
	upload, err := loadExpiredPendingUpload(ctx, db, videoID, cutoff)
	if errors.Is(err, errNoPendingUpload) {
		return nil
	}
	if err != nil {
		return err
	}

	store, err := UploadStoreFactory(ctx)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	// Critical reconciliation (A3): if a completion already merged the source
	// object but failed to advance the record, adopt the merge instead of
	// deleting the object. This is what prevents "record pending, object
	// merged" from being torn down into a missing video.
	presence, actualSize, err := inspectSourceObject(ctx, store, upload.Key, upload.ExpectedSize)
	if err != nil {
		return fmt.Errorf("inspect source object before cleanup: %w", err)
	}
	if presence == objectMatches {
		committed, commitErr := commitPendingToUploaded(ctx, db, videoID, "", false)
		if commitErr != nil {
			return fmt.Errorf("adopt merged upload: %w", commitErr)
		}
		if committed {
			SpawnUploadProcessing(db, upload.Key, videoID, upload.Title, upload.UserID)
		}
		logger.Log.Info("cleanup adopted an already-merged upload instead of deleting it", "video_id", videoID, "object_size", actualSize)
		return nil
	}
	if presence == objectMismatch {
		logger.Log.Warn("removing object whose size does not match the upload record", "video_id", videoID, "expected_size", upload.ExpectedSize, "actual_size", actualSize)
		if err := store.DeleteObject(ctx, upload.Key); err != nil {
			return fmt.Errorf("delete mismatched source object: %w", err)
		}
	}

	if upload.Key != "" && upload.UploadID != "" {
		if err := store.AbortMultipartUpload(ctx, upload.Key, upload.UploadID); err != nil && !storage.IsUploadNotFound(err) {
			return fmt.Errorf("abort multipart upload: %w", err)
		}
	}
	if presence == objectAbsent && upload.Key != "" {
		// Defensive: removes stray final objects that predate reconciliation.
		// S3 DeleteObject is idempotent for absent keys.
		if err := store.DeleteObject(ctx, upload.Key); err != nil {
			return fmt.Errorf("delete source object: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM videos WHERE video_id=$1 AND status=$2 AND upload_initiate_time <= $3`, videoID, types.UploadPending, cutoff); err != nil {
		return fmt.Errorf("delete expired upload record: %w", err)
	}
	logger.Log.Info("abandoned upload and its storage objects deleted", "video_id", videoID)
	return nil
}

func CompleteVideoUpload(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	videoID := strings.TrimSuffix(strings.TrimPrefix(path, "/video/"), "/complete")
	user, err := utils.GetUserFromRequest(r)
	if err != nil {
		utils.SendError(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	// Lock-free fast path for retries once the record has advanced: no storage
	// calls at all, and always 202 (A1).
	var currentStatus types.VideoStatus
	err = db.QueryRow(`SELECT status FROM videos WHERE video_id=$1 AND user_id=$2 AND delete_flag=0`, videoID, user.ID).Scan(&currentStatus)
	if err == sql.ErrNoRows {
		utils.SendError(w, http.StatusNotFound, "Pending upload not found")
		return
	}
	if err != nil {
		logger.Log.Error("failed to check upload status", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}
	if currentStatus != types.UploadPending {
		writeUploadAccepted(w)
		return
	}

	var request completeUploadRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024)).Decode(&request); err != nil {
		utils.SendError(w, http.StatusBadRequest, "Invalid upload completion request")
		return
	}

	releaseVideoLock, err := utils.AcquireVideoLock(r.Context(), db, videoID)
	if err != nil {
		logger.Log.Error("failed to lock video upload for completion", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Unable to complete upload")
		return
	}
	defer releaseVideoLock()

	upload, err := loadPendingUpload(r.Context(), db, videoID, user.ID)
	if errors.Is(err, errNoPendingUpload) {
		// The row was removed (abort/cleanup won) or advanced between the
		// fast-path read and lock acquisition.
		var status types.VideoStatus
		advancedErr := db.QueryRow(`SELECT status FROM videos WHERE video_id=$1 AND user_id=$2 AND delete_flag=0`, videoID, user.ID).Scan(&status)
		if advancedErr == nil && status != types.UploadPending {
			writeUploadAccepted(w)
			return
		}
		if advancedErr != nil && advancedErr != sql.ErrNoRows {
			logger.Log.Error("failed to check completion retry status", "video_id", videoID, "error", advancedErr)
			utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
			return
		}
		utils.SendError(w, http.StatusNotFound, "Pending upload not found")
		return
	}
	if err != nil {
		logger.Log.Error("failed to find pending upload", "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}

	expectedParts := (upload.ExpectedSize-1)/storage.MultipartPartSize + 1
	if upload.ExpectedSize <= 0 || len(request.Parts) != int(expectedParts) {
		utils.SendError(w, http.StatusBadRequest, "Upload is missing one or more parts")
		return
	}
	completedParts := make([]storage.UploadPart, len(request.Parts))
	for i, part := range request.Parts {
		if part.PartNumber != int32(i+1) || strings.TrimSpace(part.ETag) == "" {
			utils.SendError(w, http.StatusBadRequest, "Upload parts must be complete and ordered")
			return
		}
		completedParts[i] = storage.UploadPart{PartNumber: part.PartNumber, ETag: part.ETag}
	}

	store, err := UploadStoreFactory(r.Context())
	if err != nil {
		logger.Log.Error("failed to create storage client", "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}

	// Happy path keeps the historical sequence ListParts -> Complete ->
	// ObjectSize. A retry after a merge whose status update failed reaches an
	// already-completed upload ID; ListParts then reports ErrUploadNotFound and
	// tryAdoptAfterAmbiguousStorageError reconciles against the final object
	// instead of merging twice.
	storedParts, listErr := store.ListParts(r.Context(), upload.Key, upload.UploadID)
	if listErr != nil {
		if adopted := tryAdoptAfterAmbiguousStorageError(w, r, db, store, upload, videoID, user.ID, listErr); adopted {
			return
		}
		logger.Log.Error("failed to list multipart parts before completion", "video_id", videoID, "upload_id", upload.UploadID, "error", listErr)
		utils.SendError(w, http.StatusBadGateway, "File upload failed. Check the file and its size, then try again later.")
		return
	}
	storedETags := make(map[int32]string, len(storedParts))
	for _, part := range storedParts {
		storedETags[part.PartNumber] = part.ETag
	}
	partsMatch := len(storedETags) == len(request.Parts)
	for _, part := range request.Parts {
		if storedETags[part.PartNumber] != part.ETag {
			partsMatch = false
			break
		}
	}
	if !partsMatch {
		logger.Log.Warn("uploaded parts do not match completion request", "video_id", videoID, "upload_id", upload.UploadID, "stored_part_count", len(storedETags), "client_part_count", len(request.Parts))
		utils.SendError(w, http.StatusConflict, "Uploaded parts do not match; please retry the upload")
		return
	}

	if err := store.CompleteMultipartUpload(r.Context(), upload.Key, upload.UploadID, completedParts); err != nil {
		// Ambiguous failure: the merge may have landed despite the error.
		if adopted := tryAdoptAfterAmbiguousStorageError(w, r, db, store, upload, videoID, user.ID, err); adopted {
			return
		}
		logger.Log.Error("failed to complete multipart upload; record left pending for retry or cleanup", "video_id", videoID, "upload_id", upload.UploadID, "key", upload.Key, "error", err)
		utils.SendError(w, http.StatusBadGateway, "File upload completion failed; retry the completion request")
		return
	}

	// Merge has succeeded. Every failure below this point must leave the
	// pending record + merged object in place so a retry or cleanup sweep can
	// adopt it (A4) - no blind deletion.
	actualSize, sizeErr := store.ObjectSize(r.Context(), upload.Key)
	if sizeErr != nil {
		logger.Log.Error("failed to verify uploaded source after merge; leaving recoverable intermediate state", "video_id", videoID, "error", sizeErr)
		utils.SendError(w, http.StatusBadGateway, "Upload merged but verification is unavailable; retry shortly")
		return
	}
	if actualSize != upload.ExpectedSize {
		handleSizeMismatch(w, r, db, store, upload, videoID, user.ID)
		return
	}

	committed, err := commitPendingToUploaded(r.Context(), db, videoID, user.ID, true)
	if err != nil {
		// DB update failed: merged object + pending row remain; retry/cleanup adopts.
		logger.Log.Error("failed to update video status after merge; state recoverable via retry or cleanup", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return
	}
	if !committed {
		// RowsAffected == 0 despite the lock: re-read and stay idempotent.
		var status types.VideoStatus
		if scanErr := db.QueryRow(`SELECT status FROM videos WHERE video_id=$1 AND user_id=$2 AND delete_flag=0`, videoID, user.ID).Scan(&status); scanErr == nil && status != types.UploadPending {
			writeUploadAccepted(w)
			return
		}
		logger.Log.Error("status transition affected no rows", "video_id", videoID)
		utils.SendError(w, http.StatusConflict, "Upload has already been confirmed")
		return
	}

	writeUploadAccepted(w)
	SpawnUploadProcessing(db, upload.Key, videoID, upload.Title, upload.UserID)
}

// finalizeAdoptedUpload commits a pending row whose final object already
// exists with the expected size, and writes the idempotent accepted response.
func finalizeAdoptedUpload(w http.ResponseWriter, r *http.Request, db *sql.DB, store storage.UploadStore, upload *pendingUploadRow, videoID, userID string) error {
	committed, err := commitPendingToUploaded(r.Context(), db, videoID, userID, true)
	if err != nil {
		return err
	}
	if committed {
		SpawnUploadProcessing(db, upload.Key, videoID, upload.Title, upload.UserID)
	}
	writeUploadAccepted(w)
	return nil
}

// tryAdoptAfterAmbiguousStorageError re-checks the final object after a
// ListParts/Complete error that might mean the upload already completed.
// It writes the response and returns true when it resolved the request.
func tryAdoptAfterAmbiguousStorageError(w http.ResponseWriter, r *http.Request, db *sql.DB, store storage.UploadStore, upload *pendingUploadRow, videoID, userID string, cause error) bool {
	if !storage.IsUploadNotFound(cause) {
		return false
	}
	presence, _, inspectErr := inspectSourceObject(r.Context(), store, upload.Key, upload.ExpectedSize)
	if inspectErr != nil || presence != objectMatches {
		return false
	}
	if err := finalizeAdoptedUpload(w, r, db, store, upload, videoID, userID); err != nil {
		logger.Log.Error("failed to adopt upload after ambiguous storage error", "video_id", videoID, "error", err)
		utils.SendError(w, http.StatusInternalServerError, "Internal Server Error")
		return true
	}
	return true
}

func handleSizeMismatch(w http.ResponseWriter, r *http.Request, db *sql.DB, store storage.UploadStore, upload *pendingUploadRow, videoID, userID string) {
	if deleteErr := store.DeleteObject(r.Context(), upload.Key); deleteErr != nil {
		logger.Log.Error("failed to remove upload with unexpected size", "video_id", videoID, "error", deleteErr)
		utils.SendError(w, http.StatusInternalServerError, "Uploaded file size does not match; storage cleanup failed")
		return
	}
	if _, deleteErr := db.ExecContext(r.Context(), `DELETE FROM videos WHERE video_id=$1 AND user_id=$2 AND status=$3`, videoID, userID, types.UploadPending); deleteErr != nil {
		logger.Log.Error("failed to remove mismatched upload record", "video_id", videoID, "error", deleteErr)
	}
	utils.SendError(w, http.StatusConflict, "Uploaded file size does not match the upload request")
}

type objectPresence int

const (
	objectAbsent objectPresence = iota
	objectMatches
	objectMismatch
)

// inspectSourceObject classifies the final source object relative to the
// expected size. A storage error is returned to the caller; destructive
// decisions must never be made on an unknown state.
func inspectSourceObject(ctx context.Context, store storage.UploadStore, key string, expectedSize int64) (objectPresence, int64, error) {
	if key == "" {
		return objectAbsent, 0, nil
	}
	size, err := store.ObjectSize(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			return objectAbsent, 0, nil
		}
		return objectAbsent, 0, err
	}
	if expectedSize > 0 && size != expectedSize {
		return objectMismatch, size, nil
	}
	return objectMatches, size, nil
}

// loadPendingUpload reads the user-scoped pending row. Caller holds the lock.
func loadPendingUpload(ctx context.Context, db *sql.DB, videoID, userID string) (*pendingUploadRow, error) {
	var row pendingUploadRow
	err := db.QueryRowContext(ctx, `
		SELECT title, source_file_key, source_upload_id, source_file_size, user_id
		FROM videos
		WHERE video_id=$1 AND user_id=$2 AND status=$3 AND delete_flag=0`,
		videoID, userID, types.UploadPending,
	).Scan(&row.Title, &row.Key, &row.UploadID, &row.ExpectedSize, &row.UserID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNoPendingUpload
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// loadExpiredPendingUpload reads a pending row that is due for cleanup.
// Caller holds the lock.
func loadExpiredPendingUpload(ctx context.Context, db *sql.DB, videoID string, cutoff time.Time) (*pendingUploadRow, error) {
	var row pendingUploadRow
	err := db.QueryRowContext(ctx, `SELECT title,source_file_key,source_upload_id,source_file_size,user_id FROM videos WHERE video_id=$1 AND status=$2 AND upload_initiate_time <= $3`, videoID, types.UploadPending, cutoff).
		Scan(&row.Title, &row.Key, &row.UploadID, &row.ExpectedSize, &row.UserID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNoPendingUpload
	}
	if err != nil {
		return nil, fmt.Errorf("read pending upload: %w", err)
	}
	return &row, nil
}

// commitPendingToUploaded performs the monotonic status transition 0 -> 1.
// userScoped adds the owner predicate for HTTP callers; cleanup passes "".
// It reports whether THIS call moved the row.
func commitPendingToUploaded(ctx context.Context, db *sql.DB, videoID, userID string, userScoped bool) (bool, error) {
	var query string
	var args []any
	switch {
	case userScoped && userID != "":
		query = `UPDATE videos SET status=$1,upload_end_time=$2 WHERE video_id=$3 AND user_id=$4 AND status=$5 AND delete_flag=0`
		args = []any{types.UploadedOnServer, time.Now(), videoID, userID, types.UploadPending}
	default:
		query = `UPDATE videos SET status=$1,upload_end_time=$2 WHERE video_id=$3 AND status=$4`
		args = []any{types.UploadedOnServer, time.Now(), videoID, types.UploadPending}
	}
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func writeUploadAccepted(w http.ResponseWriter) {
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("Video upload confirmed and processing started"))
}

func downloadAndProcessVideo(ctx context.Context, client *s3.Client, key, videoID, title string, db *sql.DB, userID types.UserID) {
	ctx, finishProcessing, err := utils.BeginVideoProcessing(ctx, db, videoID)
	if err != nil {
		logger.Log.Info("video processing skipped", "video_id", videoID, "error", err)
		return
	}
	defer finishProcessing()

	object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(config.AppConfig.AppwriteBucketID), Key: aws.String(key)})
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logger.Log.Error("failed to download source from S3 storage", "video_id", videoID, "error", err)
		_ = utils.UpdateVideoStatus(db, videoID, types.ProcessingFailed)
		return
	}
	defer object.Body.Close()
	if err := os.MkdirAll("./video", 0755); err != nil {
		if ctx.Err() != nil {
			return
		}
		logger.Log.Error("failed to prepare video directory", "error", err)
		_ = utils.UpdateVideoStatus(db, videoID, types.ProcessingFailed)
		return
	}
	tmpFile, err := os.Create(filepath.Join("./video", filepath.Base(key)))
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logger.Log.Error("failed to create local source file", "error", err)
		_ = utils.UpdateVideoStatus(db, videoID, types.ProcessingFailed)
		return
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()
	if _, err = io.Copy(tmpFile, object.Body); err != nil {
		logger.Log.Error("failed to download source", "error", err)
		tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		if ctx.Err() != nil {
			return
		}
		_ = utils.UpdateVideoStatus(db, videoID, types.ProcessingFailed)
		return
	}
	if _, err = tmpFile.Seek(0, io.SeekStart); err != nil {
		logger.Log.Error("failed to rewind source", "error", err)
		tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		if ctx.Err() != nil {
			return
		}
		_ = utils.UpdateVideoStatus(db, videoID, types.ProcessingFailed)
		return
	}
	utils.PostUploadProcessFile(ctx, filepath.Base(key), videoID, title, tmpFile, db, userID)
}
