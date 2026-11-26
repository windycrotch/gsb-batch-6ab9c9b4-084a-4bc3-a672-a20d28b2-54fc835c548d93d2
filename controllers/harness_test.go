package controllers

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"video-streaming-server/config"
	"video-streaming-server/database"
	"video-streaming-server/shared/logger"
	"video-streaming-server/storage"
	"video-streaming-server/types"
	"video-streaming-server/utils"

	_ "github.com/lib/pq"
)

var (
	testDB    *sql.DB
	testDBErr error
)

// TestMain connects to a REAL PostgreSQL database. Set TEST_DATABASE_DSN to
// override the default local test DSN. When no database is reachable, tests
// that need it call t.Skip, so `go test ./...` stays green everywhere; in CI
// or local acceptance, bring the database up with
// `docker compose -f docker-compose.test.yml up -d` (see
// scripts/test-upload-consistency.sh).
func TestMain(m *testing.M) {
	logger.Log = slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, dsn := range testDSNCandidates() {
		db, err := connectTestDB(dsn)
		if err != nil {
			testDBErr = err
			continue
		}
		testDB = db
		database.DB = db
		break
	}

	if testDB != nil {
		if err := resetAndMigrate(testDB); err != nil {
			// A reachable but unusable database (e.g. refusal to reset a
			// non-test database) disables the DB-backed scenarios instead of
			// failing unrelated package tests.
			fmt.Fprintf(os.Stderr, "upload consistency tests will SKIP: %v\n", err)
			_ = testDB.Close()
			testDB = nil
			testDBErr = err
			database.DB = nil
		} else {
			config.AppConfig = &config.Config{
				JWTSecretKey:     "unit-test-secret",
				AppwriteBucketID: "test-bucket",
				FileSizeLimit:    "5368709120",
				AppwriteS3Region: "auto",
				Addr:             "127.0.0.1",
				Port:             "0",
			}
		}
	}

	code := m.Run()
	if testDB != nil {
		_ = testDB.Close()
	}
	os.Exit(code)
}

func connectTestDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// testDSNCandidates returns the DSNs to try, in priority order. An explicit
// TEST_DATABASE_DSN is authoritative and returned alone. Otherwise the local
// docker-compose.test.yml database is preferred, then the project's DB_*
// variables (including values loaded from ../.env).
func testDSNCandidates() []string {
	if dsn := os.Getenv("TEST_DATABASE_DSN"); dsn != "" {
		return []string{dsn}
	}
	var candidates []string
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		candidates = append(candidates, dsn)
	}
	candidates = append(candidates, "postgres://dekho:dekho@127.0.0.1:5433/dekho_test?sslmode=disable")
	// Best-effort load of the repository .env when running from controllers/.
	_ = config.LoadEnvFile("../.env")
	if dsn := dsnFromDBEnv(); dsn != "" {
		candidates = append(candidates, dsn)
	}
	return candidates
}

func dsnFromDBEnv() string {
	host := os.Getenv("DB_HOST")
	if host == "" {
		return ""
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	sslmode := os.Getenv("SSL_MODE")
	if sslmode == "" {
		sslmode = "disable"
	}
	u := &url.URL{
		Scheme:   "postgres",
		Host:     host + ":" + port,
		Path:     "/" + os.Getenv("DB_NAME"),
		RawQuery: "sslmode=" + sslmode,
	}
	if user := os.Getenv("DB_USER"); user != "" {
		u.User = url.UserPassword(user, os.Getenv("DB_PASSWORD"))
	}
	return u.String()
}

func resetAndMigrate(db *sql.DB) error {
	// Refuse to wipe what looks like a non-test database.
	name := ""
	if err := db.QueryRow(`SELECT current_database()`).Scan(&name); err != nil {
		return err
	}
	if os.Getenv("ALLOW_NON_TEST_DB") != "1" && !strings.Contains(strings.ToLower(name), "test") {
		return fmt.Errorf("refusing to reset database %q: name must contain 'test' (or set ALLOW_NON_TEST_DB=1)", name)
	}

	if _, err := db.Exec(`DROP TABLE IF EXISTS videos CASCADE; DROP TABLE IF EXISTS users CASCADE; DROP TABLE IF EXISTS schema_migrations_test CASCADE; DROP TABLE IF EXISTS fail_status_update CASCADE;`); err != nil {
		return err
	}

	dir := os.Getenv("MIGRATIONS_DIR")
	if dir == "" {
		// `go test` runs with the package directory as cwd.
		candidates := []string{"../database/migrations", "database/migrations", "../../database/migrations"}
		for _, c := range candidates {
			if st, err := os.Stat(c); err == nil && st.IsDir() {
				dir = c
				break
			}
		}
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, p := range paths {
		content, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if _, err := db.Exec(string(content)); err != nil {
			return fmt.Errorf("apply %s: %w", filepath.Base(p), err)
		}
	}
	return nil
}

// harness bundles per-test dependencies.
type harness struct {
	t     *testing.T
	db    *sql.DB
	store *RecordingStore

	userID   string
	username string
	cookie   *http.Cookie

	// processingCalls records spawned post-upload processing instead of running ffmpeg.
	processingCalls chan string

	oldFactory func(context.Context) (storage.UploadStore, error)
	oldSpawn   func(*sql.DB, string, string, string, string)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	if testDB == nil {
		t.Skipf("real PostgreSQL test database unavailable: %v (start it with docker compose -f docker-compose.test.yml up -d)", testDBErr)
	}

	// Clean rows before each test (migrations/schema persist for the run).
	if _, err := testDB.Exec(`TRUNCATE videos, users RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	store := NewRecordingStore()
	h := &harness{
		t:               t,
		db:              testDB,
		store:           store,
		processingCalls: make(chan string, 16),
	}

	h.oldFactory = UploadStoreFactory
	UploadStoreFactory = func(context.Context) (storage.UploadStore, error) { return store, nil }
	h.oldSpawn = SpawnUploadProcessing
	SpawnUploadProcessing = func(_ *sql.DB, _, videoID, _, _ string) {
		select {
		case h.processingCalls <- videoID:
		default:
		}
	}

	h.userID = "user-" + uniqueSuffix(t)
	h.username = "tester_" + uniqueSuffix(t)
	now := time.Now()
	_, err := testDB.Exec(`INSERT INTO users (id,username,email,password_hash,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6)`,
		h.userID, h.username, h.username+"@test.invalid", "$2a$10$not-a-real-hash", now, now)
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}
	token, err := utils.GenerateJWT(h.userID, h.username)
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	h.cookie = &http.Cookie{Name: "auth_token", Value: token, Path: "/"}

	t.Cleanup(func() {
		UploadStoreFactory = h.oldFactory
		SpawnUploadProcessing = h.oldSpawn
	})
	return h
}

func uniqueSuffix(t *testing.T) string {
	return strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
}

type seedVideo struct {
	videoID    string
	status     types.VideoStatus
	size       int64
	key        string
	uploadID   string
	age        time.Duration // time since upload_initiate_time; 0 = now
	deleteFlag int16
}

func (h *harness) seedVideo(s seedVideo) seedVideo {
	h.t.Helper()
	if s.videoID == "" {
		s.videoID = "vid-" + uniqueSuffix(h.t)
	}
	if s.key == "" {
		s.key = "raw/" + s.videoID + ".mp4"
	}
	if s.uploadID == "" {
		s.uploadID = "mpu-" + s.videoID
	}
	if s.size == 0 {
		s.size = 1024
	}
	initiated := time.Now()
	if s.age > 0 {
		initiated = initiated.Add(-s.age)
	}
	del := s.deleteFlag
	_, err := h.db.Exec(`
		INSERT INTO videos (video_id,title,description,upload_initiate_time,upload_end_time,status,delete_flag,user_id,source_file_key,source_file_size,source_upload_id)
		VALUES ($1,$2,$3,$4,NULL,$5,$6,$7,$8,$9,$10)`,
		s.videoID, "Title "+s.videoID, "desc", initiated, s.status, del, h.userID, s.key, s.size, s.uploadID)
	if err != nil {
		h.t.Fatalf("seed video %s: %v", s.videoID, err)
	}
	return s
}

func (h *harness) authRequest(method, target string, body string) *http.Request {
	h.t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	req.AddCookie(h.cookie)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func (h *harness) complete(videoID, body string) *httptest.ResponseRecorder {
	if body == "" {
		body = `{"parts":[{"part_number":1,"etag":"etag-1"}]}`
	}
	req := h.authRequest(http.MethodPost, "/video/"+videoID+"/complete", body)
	rec := httptest.NewRecorder()
	CompleteVideoUpload(rec, req, h.db)
	return rec
}

func (h *harness) abort(videoID string) *httptest.ResponseRecorder {
	req := h.authRequest(http.MethodPost, "/video/"+videoID+"/abort", "")
	rec := httptest.NewRecorder()
	AbortVideoUpload(rec, req, h.db)
	return rec
}

func (h *harness) list() *httptest.ResponseRecorder {
	req := h.authRequest(http.MethodGet, "/video/", "")
	rec := httptest.NewRecorder()
	GetVideos(rec, req, h.db)
	return rec
}

func (h *harness) dbStatus(videoID string) (status types.VideoStatus, exists bool, deleteFlag int16) {
	h.t.Helper()
	err := h.db.QueryRow(`SELECT status, delete_flag FROM videos WHERE video_id=$1`, videoID).Scan(&status, &deleteFlag)
	if err == sql.ErrNoRows {
		return 0, false, 0
	}
	if err != nil {
		h.t.Fatalf("query status: %v", err)
	}
	return status, true, deleteFlag
}

func (h *harness) waitForAdvisoryWaiter(timeout time.Duration) bool {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var waiting int
		err := h.db.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted`).Scan(&waiting)
		if err == nil && waiting > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func (h *harness) waitForGoroutine(fn func()) {
	done := make(chan struct{})
	go func() { fn(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		h.t.Fatal("goroutine did not finish within timeout")
	}
}
