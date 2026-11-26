package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"video-streaming-server/types"
)

const onePartBody = `{"parts":[{"part_number":1,"etag":"etag-1"}]}`

func seedOnePart(t *testing.T, h *harness, videoID, key, uploadID string, size int64) {
	t.Helper()
	h.store.SeedMultipart(key, uploadID, []fakePart{{number: 1, etag: "etag-1", size: size}})
}

func decodeList(t *testing.T, body []byte) []types.VideoResponseType {
	t.Helper()
	var items []types.VideoResponseType
	if err := json.Unmarshal(body, &items); err != nil {
		t.Fatalf("decode list %q: %v", string(body), err)
	}
	return items
}

func listContains(items []types.VideoResponseType, videoID string) (types.VideoStatus, bool) {
	for _, item := range items {
		if item.ID == videoID {
			return item.Status, true
		}
	}
	return 0, false
}

// A1: two completion submissions for the same video both return 202 and the
// object store merges exactly once.
func TestComplete_IdempotentDuplicateSubmission(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	first := h.complete(v.videoID, onePartBody)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first complete: expected 202, got %d: %s", first.Code, first.Body.String())
	}
	second := h.complete(v.videoID, onePartBody)
	if second.Code != http.StatusAccepted {
		t.Fatalf("second complete: expected 202, got %d: %s", second.Code, second.Body.String())
	}

	if n := h.store.Count("CompleteMultipartUpload"); n != 1 {
		t.Fatalf("CompleteMultipartUpload calls = %d, want 1; sequence=%v", n, h.store.CallNames())
	}
	if st, ok, _ := h.dbStatus(v.videoID); !ok || st != types.UploadedOnServer {
		t.Fatalf("db status = %v exists=%v, want UploadedOnServer", st, ok)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("merged source object must exist")
	}
	select {
	case id := <-h.processingCalls:
		if id != v.videoID {
			t.Fatalf("processing spawned for %q, want %q", id, v.videoID)
		}
	default:
		t.Fatal("expected processing to be spawned after commit")
	}
}

// A1 also covers the case where the record was already advanced out-of-band:
// complete must accept without any storage merge.
func TestComplete_AlreadyAdvanced_NoStorageCalls(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.ProcessingCompleted})
	h.store.SeedMergedObject(v.key, v.size)

	rec := h.complete(v.videoID, onePartBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(h.store.Calls()) != 0 {
		t.Fatalf("expected no storage calls on advanced record, got %v", h.store.CallNames())
	}
}

// A7: abort on an already-advanced record is 204, leaves record and object
// untouched, and performs no destructive storage calls.
func TestAbort_AdvancedRecord_IdempotentNoDelete(t *testing.T) {
	for _, status := range []types.VideoStatus{types.UploadedOnServer, types.ProcessingCompleted, types.ProcessingFailed} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			h := newHarness(t)
			v := h.seedVideo(seedVideo{status: status})
			h.store.SeedMergedObject(v.key, v.size)

			rec := h.abort(v.videoID)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("abort: expected 204, got %d: %s", rec.Code, rec.Body.String())
			}
			if st, ok, _ := h.dbStatus(v.videoID); !ok || st != status {
				t.Fatalf("record changed: status=%v exists=%v, want %d", st, ok, status)
			}
			if !h.store.HasObject(v.key) {
				t.Fatal("advanced record's source object must not be deleted by abort")
			}
			for _, name := range h.store.CallNames() {
				if name == "DeleteObject" || name == "AbortMultipartUpload" {
					t.Fatalf("abort on advanced record made destructive call %q; sequence=%v", name, h.store.CallNames())
				}
			}
		})
	}
}

// A2 ordering 1: completion holds the lock and merges first; abort arrives
// concurrently and must not delete the merged object/record.
func TestConcurrent_CompleteVsAbort_CompleteWins(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	enteredComplete, releaseComplete := h.store.EnableCompleteBarrier()

	var wg sync.WaitGroup
	completeRec := newResponseResult()
	abortRec := newResponseResult()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := h.complete(v.videoID, onePartBody)
		completeRec.set(rec.Code, rec.Body.String())
	}()
	select {
	case <-enteredComplete:
	case <-time.After(10 * time.Second):
		t.Fatal("complete never reached storage merge barrier")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := h.abort(v.videoID)
		abortRec.set(rec.Code, rec.Body.String())
	}()
	if !h.waitForAdvisoryWaiter(5 * time.Second) {
		t.Fatal("abort did not block on the per-video advisory lock")
	}

	close(releaseComplete)
	wg.Wait()

	if completeRec.code != http.StatusAccepted {
		t.Fatalf("complete = %d %s, want 202", completeRec.code, completeRec.body)
	}
	if abortRec.code != http.StatusNoContent {
		t.Fatalf("abort = %d %s, want 204", abortRec.code, abortRec.body)
	}
	if st, ok, _ := h.dbStatus(v.videoID); !ok || st != types.UploadedOnServer {
		t.Fatalf("after complete-wins: status=%v exists=%v, want UploadedOnServer", st, ok)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("merged object must survive when completion wins")
	}
	if n := h.store.Count("CompleteMultipartUpload"); n != 1 {
		t.Fatalf("merge count=%d want 1; seq=%v", n, h.store.CallNames())
	}
	if n := h.store.Count("DeleteObject"); n != 0 {
		t.Fatalf("abort/cleanup deleted object %d times after completion won", n)
	}
}

// A2 ordering 2: abort holds the lock and removes the pending upload first;
// a concurrent/late complete must observe the deletion and must not merge.
func TestConcurrent_CompleteVsAbort_AbortWins(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	enteredAbort, releaseAbort := h.store.EnableAbortBarrier()

	var wg sync.WaitGroup
	abortRec := newResponseResult()
	completeRec := newResponseResult()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := h.abort(v.videoID)
		abortRec.set(rec.Code, rec.Body.String())
	}()
	select {
	case <-enteredAbort:
	case <-time.After(10 * time.Second):
		t.Fatal("abort never reached storage abort barrier")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := h.complete(v.videoID, onePartBody)
		completeRec.set(rec.Code, rec.Body.String())
	}()
	if !h.waitForAdvisoryWaiter(5 * time.Second) {
		t.Fatal("complete did not block on the per-video advisory lock")
	}

	close(releaseAbort)
	wg.Wait()

	if abortRec.code != http.StatusNoContent {
		t.Fatalf("abort = %d %s, want 204", abortRec.code, abortRec.body)
	}
	if completeRec.code != http.StatusNotFound {
		t.Fatalf("late complete = %d %s, want 404 after abort won", completeRec.code, completeRec.body)
	}
	if _, ok, _ := h.dbStatus(v.videoID); ok {
		t.Fatal("record must be deleted when abort wins")
	}
	if h.store.HasObject(v.key) {
		t.Fatal("no final object may remain when abort wins")
	}
	if n := h.store.Count("CompleteMultipartUpload"); n != 0 {
		t.Fatalf("complete merged %d times after abort won", n)
	}
	if n := h.store.Count("AbortMultipartUpload"); n != 1 {
		t.Fatalf("abort count=%d want 1; seq=%v", n, h.store.CallNames())
	}
}

// Stress: many unsynchronized complete/abort races must always end in one of
// the two non-crossed terminal states.
func TestConcurrent_CompleteVsAbort_StressInvariant(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency stress in -short mode")
	}
	h := newHarness(t)
	const rounds = 12
	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		v := h.seedVideo(seedVideo{status: types.UploadPending, videoID: fmt.Sprintf("stress-%d-%s", i, uniqueSuffix(t))})
		seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

		wg.Add(2)
		go func(id string) {
			defer wg.Done()
			_ = h.complete(id, onePartBody)
		}(v.videoID)
		go func(id string) {
			defer wg.Done()
			_ = h.abort(id)
		}(v.videoID)
	}
	wg.Wait()

	for i := 0; i < rounds; i++ {
		id := fmt.Sprintf("stress-%d-%s", i, uniqueSuffix(t))
		st, exists, _ := h.dbStatus(id)
		key := "raw/" + id + ".mp4"
		switch {
		case !exists:
			if h.store.HasObject(key) {
				t.Fatalf("round %d: record deleted but object %s remains", i, key)
			}
		case st == types.UploadedOnServer:
			if !h.store.HasObject(key) {
				t.Fatalf("round %d: record advanced but merged object missing", i)
			}
		default:
			t.Fatalf("round %d: race ended in non-terminal status %d (exists=%v)", i, st, exists)
		}
	}
}

// A3 / cleanup-vs-complete ordering 1: completion wins; cleanup blocked on
// the lock must re-check state and adopt/no-op rather than delete the object.
func TestConcurrent_CleanupVsComplete_CompleteWins(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending, age: 25 * time.Hour})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	enteredComplete, releaseComplete := h.store.EnableCompleteBarrier()

	var wg sync.WaitGroup
	completeRec := newResponseResult()
	var cleanupErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := h.complete(v.videoID, onePartBody)
		completeRec.set(rec.Code, rec.Body.String())
	}()
	<-enteredComplete

	wg.Add(1)
	go func() {
		defer wg.Done()
		cleanupErr = cleanupAbandonedUpload(h.db, v.videoID, time.Now())
	}()
	if !h.waitForAdvisoryWaiter(5 * time.Second) {
		t.Fatal("cleanup did not block on advisory lock")
	}

	close(releaseComplete)
	wg.Wait()

	if completeRec.code != http.StatusAccepted {
		t.Fatalf("complete=%d %s want 202", completeRec.code, completeRec.body)
	}
	if cleanupErr != nil {
		t.Fatalf("cleanup after completion committed should no-op, got %v", cleanupErr)
	}
	if st, ok, _ := h.dbStatus(v.videoID); !ok || st != types.UploadedOnServer {
		t.Fatalf("status=%v exists=%v want UploadedOnServer", st, ok)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("cleanup must not delete the merged object after completion won")
	}
	if n := h.store.Count("DeleteObject"); n != 0 {
		t.Fatalf("DeleteObject calls=%d want 0", n)
	}
}

// Cleanup-vs-complete ordering 2: cleanup wins and removes the expired pending
// upload; late completion gets 404 and never merges.
func TestConcurrent_CleanupVsComplete_CleanupWins(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending, age: 25 * time.Hour})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	enteredAbort, releaseAbort := h.store.EnableAbortBarrier()

	var wg sync.WaitGroup
	var cleanupErr error
	completeRec := newResponseResult()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cleanupErr = cleanupAbandonedUpload(h.db, v.videoID, time.Now())
	}()
	<-enteredAbort

	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := h.complete(v.videoID, onePartBody)
		completeRec.set(rec.Code, rec.Body.String())
	}()
	if !h.waitForAdvisoryWaiter(5 * time.Second) {
		t.Fatal("complete did not block on advisory lock")
	}

	close(releaseAbort)
	wg.Wait()

	if cleanupErr != nil {
		t.Fatalf("cleanup: %v", cleanupErr)
	}
	if completeRec.code != http.StatusNotFound {
		t.Fatalf("late complete=%d want 404", completeRec.code)
	}
	if _, ok, _ := h.dbStatus(v.videoID); ok {
		t.Fatal("cleanup must delete the expired pending row")
	}
	if n := h.store.Count("CompleteMultipartUpload"); n != 0 {
		t.Fatal("late completion must not merge after cleanup won")
	}
}

// A3: merge landed but status never advanced; the expired sweep must adopt the
// merged object instead of deleting it.
func TestCleanup_AdoptsAlreadyMergedObject(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending, age: 25 * time.Hour})
	h.store.SeedMergedObject(v.key, v.size)

	RunAbandonedUploadCleanup(h.db)

	if st, ok, _ := h.dbStatus(v.videoID); !ok || st != types.UploadedOnServer {
		t.Fatalf("status=%v exists=%v, want adopted UploadedOnServer", st, ok)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("cleanup must preserve the merged source object")
	}
	if n := h.store.Count("DeleteObject"); n != 0 {
		t.Fatalf("DeleteObject=%d want 0; seq=%v", n, h.store.CallNames())
	}
	if n := h.store.Count("AbortMultipartUpload"); n != 0 {
		t.Fatalf("AbortMultipartUpload=%d want 0 for adopted merge", n)
	}
	select {
	case <-h.processingCalls:
	default:
		t.Fatal("adopted upload should spawn processing")
	}

	// A6: a second sweep changes nothing and makes no new storage calls.
	before := len(h.store.Calls())
	RunAbandonedUploadCleanup(h.db)
	if after := len(h.store.Calls()); after != before {
		t.Fatalf("second sweep added %d calls (%v), want zero new calls", after-before, h.store.CallNames()[before:])
	}
	if st, _, _ := h.dbStatus(v.videoID); st != types.UploadedOnServer {
		t.Fatalf("status changed to %d on second sweep", st)
	}
}

// A6: a genuinely abandoned pending upload is removed once; repeated sweeps
// are idempotent and produce no extra calls/errors.
func TestCleanup_ExpiredPending_Idempotent(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending, age: 25 * time.Hour})
	h.store.SeedMultipart(v.key, v.uploadID, nil)

	RunAbandonedUploadCleanup(h.db)
	if _, ok, _ := h.dbStatus(v.videoID); ok {
		t.Fatal("expired pending row should be deleted")
	}
	if n := h.store.Count("AbortMultipartUpload"); n != 1 {
		t.Fatalf("abort calls=%d want 1", n)
	}
	if n := h.store.Count("DeleteObject"); n != 1 {
		t.Fatalf("delete calls=%d want 1", n)
	}

	RunAbandonedUploadCleanup(h.db)
	RunAbandonedUploadCleanup(h.db)
	if n := h.store.Count("AbortMultipartUpload"); n != 1 {
		t.Fatalf("abort calls after repeat sweeps=%d want 1", n)
	}
	if n := h.store.Count("DeleteObject"); n != 1 {
		t.Fatalf("delete calls after repeat sweeps=%d want 1", n)
	}
}

// A4 + completion retry: injected post-merge verification failure leaves the
// merged object + pending row; retrying /complete adopts without re-merging.
func TestCompleteRetry_AfterInjectedSizeFailure(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	// ObjectSize call #1 is the pre-merge reconcile; fail only call #2,
	// the verification immediately after a successful merge.
	h.store.objectSizeHook = func(callNum int, _ string) error {
		if callNum == 1 { // first ObjectSize is the post-merge verification
			return errors.New("injected object-size outage")
		}
		return nil
	}

	first := h.complete(v.videoID, onePartBody)
	if first.Code != http.StatusBadGateway {
		t.Fatalf("first complete with injected size failure = %d %s, want 502", first.Code, first.Body.String())
	}
	if st, ok, _ := h.dbStatus(v.videoID); !ok || st != types.UploadPending {
		t.Fatalf("record must remain recoverable pending, status=%v exists=%v", st, ok)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("merged object must be retained for recovery")
	}
	if n := h.store.Count("CompleteMultipartUpload"); n != 1 {
		t.Fatalf("merge count=%d want 1", n)
	}

	// Client retries completion: reconcile finds the object and advances
	// without calling CompleteMultipartUpload again.
	h.store.objectSizeHook = nil
	retry := h.complete(v.videoID, onePartBody)
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry complete = %d %s, want 202", retry.Code, retry.Body.String())
	}
	if n := h.store.Count("CompleteMultipartUpload"); n != 1 {
		t.Fatalf("merge count after retry=%d want still 1; seq=%v", n, h.store.CallNames())
	}
	if st, _, _ := h.dbStatus(v.videoID); st != types.UploadedOnServer {
		t.Fatalf("status=%d want UploadedOnServer", st)
	}
}

// A4 + cleanup convergence: after the same injected failure, triggering the
// cleanup sweep adopts the merged object and converges state.
func TestCleanup_ConvergesAfterInjectedCompletionFailure(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending, age: 25 * time.Hour})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)
	h.store.objectSizeHook = func(callNum int, _ string) error {
		if callNum == 1 { // first ObjectSize is the post-merge verification
			return errors.New("injected object-size outage")
		}
		return nil
	}

	first := h.complete(v.videoID, onePartBody)
	if first.Code != http.StatusBadGateway {
		t.Fatalf("complete=%d want 502", first.Code)
	}

	h.store.objectSizeHook = nil
	if err := cleanupAbandonedUpload(h.db, v.videoID, time.Now()); err != nil {
		t.Fatalf("cleanup convergence: %v", err)
	}
	if st, ok, _ := h.dbStatus(v.videoID); !ok || st != types.UploadedOnServer {
		t.Fatalf("after cleanup status=%v exists=%v want UploadedOnServer", st, ok)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("merged object must survive cleanup convergence")
	}
	if n := h.store.Count("DeleteObject"); n != 0 {
		t.Fatalf("DeleteObject=%d want 0", n)
	}
}

// A4 with a REAL database-side failure of the status UPDATE: merge succeeds,
// the update is rejected by a trigger, then retry adopts the merged object.
func TestCompleteRetry_AfterDatabaseStatusUpdateFailure(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending})
	seedOnePart(t, h, v.videoID, v.key, v.uploadID, v.size)

	if _, err := h.db.Exec(`
		CREATE OR REPLACE FUNCTION fail_all_status_advances() RETURNS trigger AS $$
		BEGIN
			IF NEW.status = 1 AND OLD.status = 0 THEN
				RAISE EXCEPTION 'injected status update failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		DROP TRIGGER IF EXISTS fail_status_advance ON videos;
		CREATE TRIGGER fail_status_advance BEFORE UPDATE ON videos
			FOR EACH ROW EXECUTE FUNCTION fail_all_status_advances();
	`); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = h.db.Exec(`DROP TRIGGER IF EXISTS fail_status_advance ON videos`)
		_, _ = h.db.Exec(`DROP FUNCTION IF EXISTS fail_all_status_advances()`)
	})

	first := h.complete(v.videoID, onePartBody)
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("complete under injected DB failure = %d %s, want 500", first.Code, first.Body.String())
	}
	if st, _, _ := h.dbStatus(v.videoID); st != types.UploadPending {
		t.Fatalf("status=%d want still pending after failed update", st)
	}
	if !h.store.HasObject(v.key) {
		t.Fatal("merged object must remain after failed status update")
	}

	// Repair the database and retry: object reconciliation advances state
	// without a second merge.
	if _, err := h.db.Exec(`DROP TRIGGER fail_status_advance ON videos`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	retry := h.complete(v.videoID, onePartBody)
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry after repair = %d, want 202: %s", retry.Code, retry.Body.String())
	}
	if n := h.store.Count("CompleteMultipartUpload"); n != 1 {
		t.Fatalf("merge count=%d want 1; seq=%v", n, h.store.CallNames())
	}
	if st, _, _ := h.dbStatus(v.videoID); st != types.UploadedOnServer {
		t.Fatalf("status=%d want UploadedOnServer", st)
	}
}

// A4/A6 storage failure then another cleanup run converges to deleted state.
func TestCleanup_DeleteFailureThenRetryConverges(t *testing.T) {
	h := newHarness(t)
	v := h.seedVideo(seedVideo{status: types.UploadPending, age: 25 * time.Hour})
	// No multipart registered: abort reports NoSuchUpload which is tolerated.
	var failOnce sync.Once
	h.store.deleteHook = func(int, string) error {
		var err error
		failOnce.Do(func() { err = errors.New("injected delete outage") })
		return err
	}

	err := cleanupAbandonedUpload(h.db, v.videoID, time.Now())
	if err == nil {
		t.Fatal("first cleanup should surface injected delete failure")
	}
	if _, ok, _ := h.dbStatus(v.videoID); !ok {
		t.Fatal("row must remain when object deletion fails")
	}

	if err := cleanupAbandonedUpload(h.db, v.videoID, time.Now()); err != nil {
		t.Fatalf("second cleanup should converge, got %v", err)
	}
	if _, ok, _ := h.dbStatus(v.videoID); ok {
		t.Fatal("row should be gone after cleanup retry")
	}
	if n := h.store.Count("DeleteObject"); n != 2 {
		t.Fatalf("DeleteObject calls=%d want 2 (one failed, one successful)", n)
	}
}

// A5: list never shows pending rows; during a concurrent completion the
// racing video's visibility is monotonic (no appear-then-disappear flashback),
// and stable advanced rows are always present.
func TestList_ConcurrentComplete_NoFlashback(t *testing.T) {
	h := newHarness(t)
	racing := h.seedVideo(seedVideo{status: types.UploadPending})
	seedOnePart(t, h, racing.videoID, racing.key, racing.uploadID, racing.size)
	stable := h.seedVideo(seedVideo{videoID: "stable-done", status: types.ProcessingCompleted})

	enteredComplete, releaseComplete := h.store.EnableCompleteBarrier()
	completeDone := make(chan *responseResult, 1)
	go func() {
		rec := h.complete(racing.videoID, onePartBody)
		completeDone <- &responseResult{code: rec.Code, body: rec.Body.String()}
	}()
	<-enteredComplete

	// While completion is blocked at the storage barrier the row is still
	// pending and must be invisible.
	items := decodeList(t, h.list().Body.Bytes())
	if st, ok := listContains(items, racing.videoID); ok {
		t.Fatalf("racing pending video visible mid-completion with status %d", st)
	}
	if _, ok := listContains(items, stable.videoID); !ok {
		t.Fatal("stable completed video missing from list")
	}

	stop := make(chan struct{})
	var listWG sync.WaitGroup
	var mu sync.Mutex
	sawRacing := false
	var flashback bool
	var stableMissing int
	listWG.Add(1)
	go func() {
		defer listWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			rec := h.list()
			rows := decodeList(t, rec.Body.Bytes())
			_, present := listContains(rows, racing.videoID)
			_, stablePresent := listContains(rows, stable.videoID)
			mu.Lock()
			if present {
				sawRacing = true
			} else if sawRacing {
				flashback = true
			}
			if !stablePresent {
				stableMissing++
			}
			mu.Unlock()
		}
	}()

	// Let listers run against the blocked state, then release completion.
	time.Sleep(50 * time.Millisecond)
	close(releaseComplete)
	res := <-completeDone
	close(stop)
	listWG.Wait()

	if res.code != http.StatusAccepted {
		t.Fatalf("complete=%d %s want 202", res.code, res.body)
	}
	mu.Lock()
	defer mu.Unlock()
	if flashback {
		t.Fatal("list visibility flashback detected: racing video disappeared after first appearing")
	}
	if stableMissing > 0 {
		t.Fatalf("stable advanced video was missing from %d list responses", stableMissing)
	}

	// After commit the racing video is stably listed as advanced.
	final := decodeList(t, h.list().Body.Bytes())
	st, ok := listContains(final, racing.videoID)
	if !ok {
		t.Fatal("completed video missing from final list")
	}
	if st == types.UploadPending {
		t.Fatal("listed video must not carry pending status")
	}
}

// A5 with abort: the pending racing video must NEVER appear while abort runs;
// the stable advanced row stays visible throughout.
func TestList_ConcurrentAbort_PendingNeverVisible(t *testing.T) {
	h := newHarness(t)
	racing := h.seedVideo(seedVideo{status: types.UploadPending})
	h.store.SeedMultipart(racing.key, racing.uploadID, nil)
	stable := h.seedVideo(seedVideo{videoID: "stable-done-2", status: types.UploadedOnServer})

	enteredAbort, releaseAbort := h.store.EnableAbortBarrier()
	abortDone := make(chan int, 1)
	go func() {
		abortDone <- h.abort(racing.videoID).Code
	}()
	<-enteredAbort

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	timesSeenRacing := 0
	stableMissing := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			rows := decodeList(t, h.list().Body.Bytes())
			if _, ok := listContains(rows, racing.videoID); ok {
				mu.Lock()
				timesSeenRacing++
				mu.Unlock()
			}
			if _, ok := listContains(rows, stable.videoID); !ok {
				mu.Lock()
				stableMissing++
				mu.Unlock()
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(releaseAbort)
	code := <-abortDone
	close(stop)
	wg.Wait()

	if code != http.StatusNoContent {
		t.Fatalf("abort code=%d want 204", code)
	}
	mu.Lock()
	defer mu.Unlock()
	if timesSeenRacing != 0 {
		t.Fatalf("pending/aborting video appeared in list %d times; it must stay invisible", timesSeenRacing)
	}
	if stableMissing > 0 {
		t.Fatalf("stable video missing from %d responses", stableMissing)
	}
}

// Baseline list filter without concurrency.
func TestList_FiltersPendingShowsAdvanced(t *testing.T) {
	h := newHarness(t)
	pending := h.seedVideo(seedVideo{videoID: "p", status: types.UploadPending})
	done := h.seedVideo(seedVideo{videoID: "d", status: types.ProcessingCompleted})
	failed := h.seedVideo(seedVideo{videoID: "f", status: types.ProcessingFailed})

	rows := decodeList(t, h.list().Body.Bytes())
	if _, ok := listContains(rows, pending.videoID); ok {
		t.Fatal("pending video must be hidden from list")
	}
	for _, id := range []string{done.videoID, failed.videoID} {
		if _, ok := listContains(rows, id); !ok {
			t.Fatalf("advanced video %s must be listed", id)
		}
	}
}

type responseResult struct {
	code int
	body string
}

func newResponseResult() *responseResult { return &responseResult{} }

func (r *responseResult) set(code int, body string) {
	r.code = code
	r.body = body
}
