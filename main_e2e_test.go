package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"video-streaming-server/config"
	"video-streaming-server/controllers"
	"video-streaming-server/database"
	"video-streaming-server/shared/logger"
	"video-streaming-server/storage"
	"video-streaming-server/types"
	"video-streaming-server/utils"

	_ "github.com/lib/pq"
)

// e2eRoutesOnce registers main.go's routes on http.DefaultServeMux at most
// once per test binary (repeated -count iterations would otherwise panic on
// duplicate patterns).
var e2eRoutesOnce sync.Once

// e2eStore is a minimal in-memory UploadStore for the real-router E2E test.
type e2eStore struct {
	objects map[string]int64
	parts   map[string][]storage.UploadPart
	merges  int
}

func (s *e2eStore) CreateMultipartUpload(context.Context, string, string) (string, error) {
	return "mpu-e2e", nil
}
func (s *e2eStore) PresignUploadPart(context.Context, string, string, int32) (string, error) {
	return "https://storage.invalid/signed", nil
}
func (s *e2eStore) ListParts(_ context.Context, _, uploadID string) ([]storage.UploadPart, error) {
	return s.parts[uploadID], nil
}
func (s *e2eStore) CompleteMultipartUpload(_ context.Context, key, uploadID string, parts []storage.UploadPart) error {
	s.merges++
	var size int64 = 1024
	s.objects[key] = size
	return nil
}
func (s *e2eStore) AbortMultipartUpload(context.Context, string, string) error { return nil }
func (s *e2eStore) ObjectSize(_ context.Context, key string) (int64, error) {
	if n, ok := s.objects[key]; ok {
		return n, nil
	}
	return 0, storage.ErrObjectNotFound
}
func (s *e2eStore) DeleteObject(context.Context, string) error {
	return nil
}

// TestRealServer_UploadFinalizationLifecycle boots the ACTUAL route table from
// main.go on an httptest listener, backed by a REAL PostgreSQL migrated from
// database/migrations. It is the "locally runnable service + real database"
// acceptance path; only object storage is replaced in-process.
func TestRealServer_UploadFinalizationLifecycle(t *testing.T) {
	adminDSN := os.Getenv("TEST_DATABASE_DSN")
	if adminDSN == "" {
		adminDSN = "postgres://dekho:dekho@127.0.0.1:5433/dekho_test?sslmode=disable"
	}
	// Use a dedicated database so this package-level test binary can reset
	// schema while `go test ./...` runs the controllers package in parallel.
	dsn, err := ensureE2EDatabase(adminDSN, "dekho_test_e2e")
	if err != nil {
		t.Skipf("provision e2e database: %v", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("open test db: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		t.Skipf("real PostgreSQL unavailable at %s: %v", dsn, err)
	}
	cancel()
	defer db.Close()

	logger.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := migrateRootTestDB(db); err != nil {
		t.Skipf("migrate test db: %v", err)
	}
	database.DB = db
	config.AppConfig = &config.Config{JWTSecretKey: "e2e-secret", AppwriteBucketID: "b", FileSizeLimit: "5368709120"}

	store := &e2eStore{objects: map[string]int64{}, parts: map[string][]storage.UploadPart{
		"mpu-e2e": {{PartNumber: 1, ETag: "etag-1"}},
	}}
	oldFactory := controllers.UploadStoreFactory
	controllers.UploadStoreFactory = func(context.Context) (storage.UploadStore, error) { return store, nil }
	controllers.SpawnUploadProcessing = func(*sql.DB, string, string, string, string) {}
	t.Cleanup(func() { controllers.UploadStoreFactory = oldFactory })

	userID := "e2e-user"
	now := time.Now()
	_, _ = db.Exec(`TRUNCATE videos, users RESTART IDENTITY CASCADE`)
	if _, err := db.Exec(`INSERT INTO users (id,username,email,password_hash,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6)`,
		userID, "e2euser", "e2e@test.invalid", "x", now, now); err != nil {
		t.Fatal(err)
	}
	videoID := "e2e-video-1"
	if _, err := db.Exec(`INSERT INTO videos (video_id,title,upload_initiate_time,status,delete_flag,user_id,source_file_key,source_file_size,source_upload_id)
		VALUES ($1,'E2E',NOW(),0,0,$2,'raw/e2e.mp4',1024,'mpu-e2e')`, videoID, userID); err != nil {
		t.Fatal(err)
	}
	token, err := utils.GenerateJWT(userID, "e2euser")
	if err != nil {
		t.Fatal(err)
	}

	e2eRoutesOnce.Do(setUpRoutes)
	server := httptest.NewServer(http.DefaultServeMux)
	defer server.Close()

	post := func(path, body string) *http.Response {
		t.Helper()
		var r io.Reader
		if body != "" {
			r = strings.NewReader(body)
		}
		req, err := http.NewRequest(http.MethodPost, server.URL+path, r)
		if err != nil {
			t.Fatal(err)
		}
		req.AddCookie(&http.Cookie{Name: "auth_token", Value: token})
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	get := func(path string) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		req.AddCookie(&http.Cookie{Name: "auth_token", Value: token})
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// First completion through the REAL /video/ regex router + auth middleware.
	resp := post("/video/"+videoID+"/complete", `{"parts":[{"part_number":1,"etag":"etag-1"}]}`)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first complete status=%d want 202", resp.StatusCode)
	}
	// Retry: same 202, no second merge.
	resp = post("/video/"+videoID+"/complete", `{"parts":[{"part_number":1,"etag":"etag-1"}]}`)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("retry complete status=%d want 202", resp.StatusCode)
	}
	if store.merges != 1 {
		t.Fatalf("merges=%d want 1", store.merges)
	}

	// List shows the advanced video via the real route.
	resp = get("/video/")
	listBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status=%d body=%s", resp.StatusCode, string(listBody))
	}
	var rows []types.VideoResponseType
	if err := json.Unmarshal(listBody, &rows); err != nil {
		t.Fatalf("decode list: %v (%s)", err, string(listBody))
	}
	found := false
	for _, row := range rows {
		if row.ID == videoID {
			found = true
		}
	}
	if !found {
		t.Fatalf("advanced video not in list: %s", string(listBody))
	}

	// Abort on advanced record is 204 and does not remove the object.
	resp = post("/video/"+videoID+"/abort", "")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("abort status=%d want 204", resp.StatusCode)
	}
	if _, ok := store.objects["raw/e2e.mp4"]; !ok {
		t.Fatal("merged object must survive abort of advanced record")
	}
	var status int
	if err := db.QueryRow(`SELECT status FROM videos WHERE video_id=$1`, videoID).Scan(&status); err != nil || status != int(types.UploadedOnServer) {
		t.Fatalf("status=%d err=%v want 1", status, err)
	}
}

func migrateRootTestDB(db *sql.DB) error {
	var name string
	if err := db.QueryRow(`SELECT current_database()`).Scan(&name); err != nil {
		return err
	}
	if os.Getenv("ALLOW_NON_TEST_DB") != "1" && !strings.Contains(strings.ToLower(name), "test") {
		return errRefuseReset{name}
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS videos CASCADE; DROP TABLE IF EXISTS users CASCADE`); err != nil {
		return err
	}
	paths, err := filepath.Glob("database/migrations/*.up.sql")
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if _, err := db.Exec(string(b)); err != nil {
			return err
		}
	}
	return nil
}

type errRefuseReset struct{ db string }

func (e errRefuseReset) Error() string {
	return "refusing to reset non-test database " + e.db
}

// ensureE2EDatabase creates name on the same server as adminDSN when missing
// and returns a DSN pointed at it. It supports both URL and key=value DSNs.
func ensureE2EDatabase(adminDSN, name string) (string, error) {
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return "", err
	}
	defer admin.Close()
	if err := admin.Ping(); err != nil {
		return "", err
	}
	if _, err := admin.Exec(`CREATE DATABASE ` + name); err != nil {
		// 42P04 duplicate_database is fine; anything else that isn't a
		// "already exists" message is surfaced.
		if !strings.Contains(err.Error(), "already exists") {
			return "", err
		}
	}

	if u, perr := url.Parse(adminDSN); perr == nil && u.Scheme != "" {
		u.Path = "/" + name
		return u.String(), nil
	}
	if strings.Contains(adminDSN, "dbname=") {
		return replaceDBNameKV(adminDSN, name), nil
	}
	return adminDSN + " dbname=" + name, nil
}

func replaceDBNameKV(dsn, name string) string {
	parts := strings.Fields(dsn)
	replaced := false
	for i, p := range parts {
		if strings.HasPrefix(p, "dbname=") {
			parts[i] = "dbname=" + name
			replaced = true
		}
	}
	if !replaced {
		parts = append(parts, "dbname="+name)
	}
	return strings.Join(parts, " ")
}
