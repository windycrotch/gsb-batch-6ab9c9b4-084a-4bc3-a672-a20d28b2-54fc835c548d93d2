package controllers

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"video-streaming-server/storage"
)

// StoreCall is one recorded invocation of the replaceable storage gateway.
type StoreCall struct {
	Name     string
	Key      string
	UploadID string
}

type fakePart struct {
	number int32
	etag   string
	size   int64
}

type fakeMultipart struct {
	key     string
	parts   map[int32]fakePart
	aborted bool
}

// RecordingStore is an in-memory UploadStore that records the full call
// sequence and supports barriers / failure injection. Tests can assert
// storage side effects without real cloud credentials.
type RecordingStore struct {
	mu sync.Mutex

	calls []StoreCall

	// objects maps final object key -> size in bytes.
	objects map[string]int64
	uploads map[string]*fakeMultipart
	nextID  int

	// Barrier/hook channels. When non-nil, the corresponding method signals
	// "entered" and then waits on "release" (or ctx cancellation).
	completeEntered chan struct{}
	releaseComplete chan struct{}
	abortEntered    chan struct{}
	releaseAbort    chan struct{}
	deleteEntered   chan struct{}
	releaseDelete   chan struct{}

	// Optional failure hooks. They receive the 1-based call number for that
	// operation and may return an error to inject.
	objectSizeHook func(callNum int, key string) error
	deleteHook     func(callNum int, key string) error
	abortHook      func(callNum int, key, uploadID string) error
	completeHook   func(callNum int, key, uploadID string) error

	objectSizeCalls int
	deleteCalls     int
	abortCalls      int
	completeCalls   int
}

func NewRecordingStore() *RecordingStore {
	return &RecordingStore{
		objects: map[string]int64{},
		uploads: map[string]*fakeMultipart{},
	}
}

func (s *RecordingStore) record(name, key, uploadID string) {
	s.calls = append(s.calls, StoreCall{Name: name, Key: key, UploadID: uploadID})
}

// Calls returns a copy of the recorded call sequence.
func (s *RecordingStore) Calls() []StoreCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]StoreCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// CallNames returns just operation names, convenient for sequence assertions.
func (s *RecordingStore) CallNames() []string {
	calls := s.Calls()
	names := make([]string, len(calls))
	for i, c := range calls {
		names[i] = c.Name
	}
	return names
}

func (s *RecordingStore) Count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c.Name == name {
			n++
		}
	}
	return n
}

// HasObject reports whether a final merged object currently exists.
func (s *RecordingStore) HasObject(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key]
	return ok
}

func (s *RecordingStore) ObjectSizeOf(key string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.objects[key]
	return n, ok
}

// SeedMergedObject simulates an already-merged final object (the state left
// behind when completion merged but failed before advancing the DB row).
func (s *RecordingStore) SeedMergedObject(key string, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = size
}

// SeedMultipart registers an in-progress multipart upload with stored parts.
func (s *RecordingStore) SeedMultipart(key, uploadID string, parts []fakePart) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mp := &fakeMultipart{key: key, parts: map[int32]fakePart{}}
	for _, p := range parts {
		mp.parts[p.number] = p
	}
	s.uploads[uploadID] = mp
}

// EnableCompleteBarrier makes CompleteMultipartUpload block once entered.
func (s *RecordingStore) EnableCompleteBarrier() (entered, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeEntered = make(chan struct{}, 1)
	s.releaseComplete = make(chan struct{})
	return s.completeEntered, s.releaseComplete
}

// EnableAbortBarrier makes AbortMultipartUpload block once entered.
func (s *RecordingStore) EnableAbortBarrier() (entered, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abortEntered = make(chan struct{}, 1)
	s.releaseAbort = make(chan struct{})
	return s.abortEntered, s.releaseAbort
}

// EnableDeleteBarrier makes DeleteObject block once entered.
func (s *RecordingStore) EnableDeleteBarrier() (entered, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteEntered = make(chan struct{}, 1)
	s.releaseDelete = make(chan struct{})
	return s.deleteEntered, s.releaseDelete
}

func (s *RecordingStore) wait(ctx context.Context, entered chan struct{}, release chan struct{}) error {
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if release == nil {
		return nil
	}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *RecordingStore) CreateMultipartUpload(_ context.Context, key, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := fmt.Sprintf("upload-%d", s.nextID)
	s.uploads[id] = &fakeMultipart{key: key, parts: map[int32]fakePart{}}
	s.record("CreateMultipartUpload", key, id)
	return id, nil
}

func (s *RecordingStore) PresignUploadPart(_ context.Context, key, uploadID string, partNumber int32) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record("PresignUploadPart", key, uploadID)
	return fmt.Sprintf("https://storage.invalid/%s?part=%d", key, partNumber), nil
}

func (s *RecordingStore) ListParts(_ context.Context, key, uploadID string) ([]storage.UploadPart, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record("ListParts", key, uploadID)
	mp, ok := s.uploads[uploadID]
	if !ok || mp.aborted {
		return nil, storage.ErrUploadNotFound
	}
	numbers := make([]int32, 0, len(mp.parts))
	for n := range mp.parts {
		numbers = append(numbers, n)
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] < numbers[j] })
	out := make([]storage.UploadPart, 0, len(numbers))
	for _, n := range numbers {
		p := mp.parts[n]
		out = append(out, storage.UploadPart{PartNumber: p.number, ETag: p.etag})
	}
	return out, nil
}

func (s *RecordingStore) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []storage.UploadPart) error {
	s.mu.Lock()
	s.completeCalls++
	callNum := s.completeCalls
	hook := s.completeHook
	entered, release := s.completeEntered, s.releaseComplete
	s.record("CompleteMultipartUpload", key, uploadID)
	s.mu.Unlock()

	if hook != nil {
		if err := hook(callNum, key, uploadID); err != nil {
			return err
		}
	}
	if err := s.wait(ctx, entered, release); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	mp, ok := s.uploads[uploadID]
	if !ok || mp.aborted {
		return storage.ErrUploadNotFound
	}
	var size int64
	for _, p := range parts {
		stored, exists := mp.parts[p.PartNumber]
		if !exists || stored.etag != p.ETag {
			return fmt.Errorf("fake store: part %d mismatch", p.PartNumber)
		}
		size += stored.size
	}
	s.objects[key] = size
	delete(s.uploads, uploadID)
	return nil
}

func (s *RecordingStore) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	s.mu.Lock()
	s.abortCalls++
	callNum := s.abortCalls
	hook := s.abortHook
	entered, release := s.abortEntered, s.releaseAbort
	s.record("AbortMultipartUpload", key, uploadID)
	s.mu.Unlock()

	if hook != nil {
		if err := hook(callNum, key, uploadID); err != nil {
			return err
		}
	}
	if err := s.wait(ctx, entered, release); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	mp, ok := s.uploads[uploadID]
	if !ok {
		return storage.ErrUploadNotFound
	}
	mp.aborted = true
	delete(s.uploads, uploadID)
	return nil
}

func (s *RecordingStore) ObjectSize(_ context.Context, key string) (int64, error) {
	s.mu.Lock()
	s.objectSizeCalls++
	callNum := s.objectSizeCalls
	hook := s.objectSizeHook
	size, ok := s.objects[key]
	s.record("ObjectSize", key, "")
	s.mu.Unlock()

	if hook != nil {
		if err := hook(callNum, key); err != nil {
			return 0, err
		}
	}
	if !ok {
		return 0, storage.ErrObjectNotFound
	}
	return size, nil
}

func (s *RecordingStore) DeleteObject(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deleteCalls++
	callNum := s.deleteCalls
	hook := s.deleteHook
	entered, release := s.deleteEntered, s.releaseDelete
	s.record("DeleteObject", key, "")
	s.mu.Unlock()

	if hook != nil {
		if err := hook(callNum, key); err != nil {
			return err
		}
	}
	if err := s.wait(ctx, entered, release); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}
