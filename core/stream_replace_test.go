package core

import (
	"testing"
	"time"

	"github.com/im-pingo/liveforge/config"
	"github.com/im-pingo/liveforge/pkg/avframe"
)

func TestReplacePublisherOpensIndependentGeneration(t *testing.T) {
	s := NewStream("live/replace", newTestStreamConfig(), config.LimitsConfig{}, NewEventBus())
	oldPub := &testPublisher{id: "old-publisher", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if err := s.SetPublisher(oldPub); err != nil {
		t.Fatal(err)
	}
	oldSnapshot := s.StartupSnapshot()
	oldHeader := avframe.NewAVFrame(
		avframe.MediaTypeVideo, avframe.CodecH264, avframe.FrameTypeSequenceHeader,
		0, 0, []byte{0x01, 0x64, 0x00, 0x28},
	)
	oldKey := avframe.NewAVFrame(
		avframe.MediaTypeVideo, avframe.CodecH264, avframe.FrameTypeKeyframe,
		0, 0, []byte{0x65, 0x01},
	)
	for _, frame := range []*avframe.AVFrame{oldHeader, oldKey} {
		if !s.WriteFrameForPublisher(oldPub, frame) {
			t.Fatalf("old publisher frame rejected: %+v", frame)
		}
	}
	cursorBeforeReplace := s.RingBuffer().WriteCursor()

	newPub := &testPublisher{id: "new-publisher", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH265}}
	displaced, err := s.ReplacePublisher(newPub)
	if err != nil {
		t.Fatalf("ReplacePublisher: %v", err)
	}
	if displaced != oldPub {
		t.Fatalf("displaced publisher = %v, want old publisher", displaced)
	}
	if s.Publisher() != newPub || s.State() != StreamStatePublishing {
		t.Fatalf("replacement did not become the active publisher: state=%s", s.State())
	}

	newSnapshot := s.StartupSnapshot()
	if newSnapshot.Generation != oldSnapshot.Generation+1 {
		t.Fatalf("replacement generation = %d, want %d", newSnapshot.Generation, oldSnapshot.Generation+1)
	}
	if newSnapshot.StreamInstanceID != oldSnapshot.StreamInstanceID {
		t.Fatal("replacement created a new stream instance instead of a new generation")
	}
	if newSnapshot.GenerationStartCursor != cursorBeforeReplace {
		t.Fatalf("generation start cursor = %d, want %d", newSnapshot.GenerationStartCursor, cursorBeforeReplace)
	}
	if newSnapshot.VideoSequenceHeader != nil || len(newSnapshot.ReplayFrames) != 0 {
		t.Fatal("replacement generation inherited GOP cache or sequence headers")
	}
	if newSnapshot.MediaInfo.VideoCodec != avframe.CodecH265 {
		t.Fatalf("replacement media info = %+v, want H265 from new publisher", newSnapshot.MediaInfo)
	}
	select {
	case <-oldSnapshot.GenerationDone:
	default:
		t.Fatal("old generation completion signal was not closed by replacement")
	}
	if end, ended := oldSnapshot.GenerationEndCursor(); !ended || end != cursorBeforeReplace {
		t.Fatalf("old generation boundary = (%d, %v), want (%d, true)", end, ended, cursorBeforeReplace)
	}
	if s.IsPublisherGeneration(oldSnapshot.Generation) {
		t.Fatal("retired generation still reports as active")
	}
	if !s.IsPublisherGeneration(newSnapshot.Generation) {
		t.Fatal("new generation is not recognized as active")
	}
}

func TestReplacePublisherRejectsStaleWritesAndDetachment(t *testing.T) {
	s := NewStream("live/replace-stale", newTestStreamConfig(), config.LimitsConfig{}, NewEventBus())
	oldPub := &testPublisher{id: "old", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if err := s.SetPublisher(oldPub); err != nil {
		t.Fatal(err)
	}
	newPub := &testPublisher{id: "new", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if _, err := s.ReplacePublisher(newPub); err != nil {
		t.Fatal(err)
	}
	startCursor := s.StartupSnapshot().GenerationStartCursor

	staleFrame := avframe.NewAVFrame(
		avframe.MediaTypeVideo, avframe.CodecH264, avframe.FrameTypeKeyframe,
		0, 0, []byte{0x65, 0x99},
	)
	if s.WriteFrameForPublisher(oldPub, staleFrame) {
		t.Fatal("stale publisher wrote into the replacement generation")
	}
	if got := s.RingBuffer().WriteCursor(); got != startCursor {
		t.Fatalf("stale write advanced cursor to %d, want %d", got, startCursor)
	}
	if s.RemovePublisherIf(oldPub) {
		t.Fatal("stale publisher cleanup detached the replacement")
	}
	if s.Publisher() != newPub || s.State() != StreamStatePublishing {
		t.Fatal("replacement publisher was removed by stale cleanup")
	}

	freshFrame := avframe.NewAVFrame(
		avframe.MediaTypeVideo, avframe.CodecH264, avframe.FrameTypeKeyframe,
		0, 0, []byte{0x65, 0x01},
	)
	if !s.WriteFrameForPublisher(newPub, freshFrame) {
		t.Fatal("replacement publisher frame was rejected")
	}
	if got := s.RingBuffer().WriteCursor(); got != startCursor+1 {
		t.Fatalf("replacement write cursor = %d, want %d", got, startCursor+1)
	}
}

func TestReplacePublisherWithoutIncumbentBehavesLikeAdmission(t *testing.T) {
	s := NewStream("live/replace-empty", newTestStreamConfig(), config.LimitsConfig{}, NewEventBus())
	pub := &testPublisher{id: "first", info: &avframe.MediaInfo{AudioCodec: avframe.CodecMP3}}
	displaced, err := s.ReplacePublisher(pub)
	if err != nil {
		t.Fatalf("ReplacePublisher on idle stream: %v", err)
	}
	if displaced != nil {
		t.Fatalf("displaced = %v, want nil on an idle stream", displaced)
	}
	if s.Publisher() != pub || s.State() != StreamStatePublishing {
		t.Fatalf("publisher not installed: state=%s", s.State())
	}
	if snapshot := s.StartupSnapshot(); snapshot.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", snapshot.Generation)
	}
}

func TestReplacePublisherRejectsInvalidAndReusedIdentities(t *testing.T) {
	s := NewStream("live/replace-invalid", newTestStreamConfig(), config.LimitsConfig{}, NewEventBus())
	oldPub := &testPublisher{id: "old", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if err := s.SetPublisher(oldPub); err != nil {
		t.Fatal(err)
	}
	before := s.StartupSnapshot()

	if _, err := s.ReplacePublisher(nil); err == nil {
		t.Fatal("ReplacePublisher accepted a nil publisher")
	}
	if _, err := s.ReplacePublisher(oldPub); err == nil {
		t.Fatal("ReplacePublisher reused an already-used publisher ID")
	}
	if s.Publisher() != oldPub {
		t.Fatal("rejected replacement displaced the incumbent")
	}
	after := s.StartupSnapshot()
	if after.Generation != before.Generation || after.GenerationStartCursor != before.GenerationStartCursor {
		t.Fatal("rejected replacement changed generation state")
	}
	if got := s.RingBuffer().WriteCursor(); got != before.LiveCursor {
		t.Fatal("rejected replacement advanced the write cursor")
	}
}

func TestReplacePublisherRejectedAfterDestroy(t *testing.T) {
	cfg := newTestStreamConfig()
	cfg.NoPublisherTimeout = time.Hour
	s := NewStream("live/replace-destroy", cfg, config.LimitsConfig{}, NewEventBus())
	pub := &testPublisher{id: "only", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if err := s.SetPublisher(pub); err != nil {
		t.Fatal(err)
	}
	s.Close()
	replacement := &testPublisher{id: "late", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if _, err := s.ReplacePublisher(replacement); err == nil {
		t.Fatal("ReplacePublisher succeeded on a destroying stream")
	}
}

func TestReplacePublisherKeepsNoPublisherTimerFromRunning(t *testing.T) {
	cfg := newTestStreamConfig()
	cfg.NoPublisherTimeout = 50 * time.Millisecond
	s := NewStream("live/replace-timer", cfg, config.LimitsConfig{}, NewEventBus())
	oldPub := &testPublisher{id: "old", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if err := s.SetPublisher(oldPub); err != nil {
		t.Fatal(err)
	}
	s.RemovePublisherIf(oldPub)
	if s.State() != StreamStateNoPublisher {
		t.Fatalf("state after detach = %s, want no_publisher", s.State())
	}
	newPub := &testPublisher{id: "new", info: &avframe.MediaInfo{VideoCodec: avframe.CodecH264}}
	if _, err := s.ReplacePublisher(newPub); err != nil {
		t.Fatalf("ReplacePublisher after detach: %v", err)
	}
	time.Sleep(120 * time.Millisecond)
	if s.State() != StreamStatePublishing || s.Publisher() != newPub {
		t.Fatalf("stream state = %s publisher=%v after no-publisher timeout window", s.State(), s.Publisher())
	}
}
