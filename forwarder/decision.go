package forwarder

import (
	"fmt"
	"time"

	"github.com/alwitt/livemix/common"
)

// SegmentProcessingDecision describes why a segment should be handled the way it is by
// either forwarding path. Both the live stream and recording segment forwarders derive
// their "should this segment be processed" conclusion through these values so that the
// two paths give consistent and explainable outcomes.
type SegmentProcessingDecision int

const (
	// SegmentDecisionProcess the segment must be processed: sent to the live stream
	// receiver, or uploaded and broadcast for a recording session.
	SegmentDecisionProcess SegmentProcessingDecision = iota
	// SegmentDecisionSkippedStreamingDisabled the source is not streaming, so the live
	// segment is intentionally skipped. No outbound request is allowed for this segment.
	SegmentDecisionSkippedStreamingDisabled
	// SegmentDecisionSkippedNoActiveRecording none of the recording sessions attached to
	// the segment are active anymore, so the segment must not be counted toward any
	// recording and must not be broadcast.
	SegmentDecisionSkippedNoActiveRecording
)

// String human readable explanation of the decision
func (d SegmentProcessingDecision) String() string {
	switch d {
	case SegmentDecisionProcess:
		return "process"
	case SegmentDecisionSkippedStreamingDisabled:
		return "skipped: source streaming is disabled"
	case SegmentDecisionSkippedNoActiveRecording:
		return "skipped: no active recording session"
	default:
		return fmt.Sprintf("unknown-decision-%d", int(d))
	}
}

// SegmentProcessStatus terminal status of a single segment submission
type SegmentProcessStatus int

const (
	// SegmentStatusProcessed the segment was successfully handled (uploaded / forwarded,
	// and broadcast where applicable).
	SegmentStatusProcessed SegmentProcessStatus = iota
	// SegmentStatusSkipped the segment was intentionally skipped without any outbound
	// request, see the associated decision for the reason.
	SegmentStatusSkipped
	// SegmentStatusFailed all processing attempts failed; the failure is observable
	// through the result and the logs rather than being silently dropped.
	SegmentStatusFailed
	// SegmentStatusCancelled processing was cancelled because the forwarder was stopped.
	SegmentStatusCancelled
)

// String human readable status
func (s SegmentProcessStatus) String() string {
	switch s {
	case SegmentStatusProcessed:
		return "processed"
	case SegmentStatusSkipped:
		return "skipped"
	case SegmentStatusFailed:
		return "failed"
	case SegmentStatusCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unknown-status-%d", int(s))
	}
}

// SegmentForwardResult the explainable conclusion for one submitted segment
type SegmentForwardResult struct {
	// SegmentID ID of the segment this result belongs to
	SegmentID string
	// SegmentName name of the segment
	SegmentName string
	// URI target storage / forward URI of the segment when applicable
	URI string
	// Status terminal status of the submission
	Status SegmentProcessStatus
	// Decision the processing decision explaining the outcome
	Decision SegmentProcessingDecision
	// Attempts number of upload / forward attempts made (0 when skipped or cancelled
	// before any attempt)
	Attempts int
	// Err the last observed error when Status == SegmentStatusFailed; nil otherwise
	Err error
}

// FailedSegmentRecord observable record of a segment whose processing retries were
// exhausted
type FailedSegmentRecord struct {
	SegmentID   string
	SegmentName string
	URI         string
	Err         error
	Attempts    int
}

// RetryConfig controls how a forwarder retries a failed segment processing attempt
type RetryConfig struct {
	// MaxAttempts total number of attempts made per segment (1 disables retries)
	MaxAttempts int
	// InitialBackoff wait before the first retry
	InitialBackoff time.Duration
	// MaxBackoff upper bound on the wait between retries
	MaxBackoff time.Duration
}

// defaultRetryConfig returns the retry behavior used when a caller does not provide one
func defaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}
}

// normalizeRetryConfig fills in safe defaults for an unset / partial retry config
func normalizeRetryConfig(cfg RetryConfig) RetryConfig {
	def := defaultRetryConfig()
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = def.MaxAttempts
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = def.InitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = def.MaxBackoff
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = cfg.InitialBackoff
	}
	return cfg
}

// nextBackoff computes the wait before retry attempt `retryIdx` (0-based) using an
// exponential backoff capped at MaxBackoff
func (c RetryConfig) nextBackoff(retryIdx int) time.Duration {
	wait := c.InitialBackoff
	for i := 0; i < retryIdx && wait < c.MaxBackoff; i++ {
		wait *= 2
		if wait > c.MaxBackoff || wait <= 0 {
			wait = c.MaxBackoff
		}
	}
	return wait
}

// segmentIdentity returns the key used to identify a segment for de-duplication. The
// segment ID is unique per registered segment; the name is a fallback for callers that
// build segments outside of persistence. The storage URI is included so retries
// targeting a different object location are not collapsed onto an earlier run.
func segmentIdentity(segment common.VideoSegmentWithData) string {
	id := segment.ID
	if id == "" {
		id = segment.SourceID + "/" + segment.Name
	}
	return id + "|" + segment.URI
}
