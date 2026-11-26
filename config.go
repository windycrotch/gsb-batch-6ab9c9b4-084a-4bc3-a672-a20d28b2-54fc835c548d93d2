package adler

import "time"

// Protocol defines the serialization format for messages between server and clients.
type Protocol int

// defaultReadMaxPayloadSize bounds per-message memory for typical chat/control traffic.
const defaultReadMaxPayloadSize = 64 * 1024

const (
	// JSON protocol: messages are serialized as JSON text.
	JSON Protocol = iota
	// Protobuf protocol: messages are serialized as binary protobuf.
	Protobuf
	None
)

// Config contains runtime tuning knobs for Adler sessions.
//
// Use constructor options (With...) to override individual fields while
// keeping stable defaults for all other values.
type Config struct {
	// WriteWait is the maximum duration allowed for a single outbound write.
	// A non-positive value disables the per-write deadline.
	// Typical values are between 5 and 30 seconds for internet clients.
	WriteWait time.Duration
	// PongWait is the read timeout, renewed whenever a pong is received.
	// A non-positive value disables the timeout; the default is zero.
	// If enabled, common values are 30-120 seconds.
	PongWait time.Duration
	// PingPeriod defines how often server-side ping frames are sent.
	// Keep it lower than PongWait when PongWait is enabled.
	// A common ratio is PingPeriod = ~80-90% of PongWait.
	// Non-positive values fall back to 54 seconds; the default is 60 seconds.
	PingPeriod time.Duration
	// MessageBufferSize is the per-session outbound queue capacity.
	// Start with 64-256 for chat-like traffic and increase only if you observe
	// ErrBufferFull under normal expected bursts.
	// Higher values increase per-session memory usage.
	// When this value is 0, runtime falls back to a minimal queue.
	MessageBufferSize int
	// DispatchAsync controls inbound message dispatch mode.
	// true dispatches each message on its own goroutine.
	DispatchAsync bool
	// DeleteRoomOnEmpty controls whether rooms are removed automatically
	// when the last session leaves or disconnects. The default is true.
	DeleteRoomOnEmpty bool
	// Preferred protocol by server.
	// This is the used protocol in matchmaker and in other structures of Adler.
	Protocol Protocol
	// ReadMaxPayloadSize limits incoming message payloads in bytes, including
	// the combined payload of fragmented messages. Oversized messages close the connection.
	// The default is 64 KiB; non-positive values use the default during construction.
	ReadMaxPayloadSize int
}

// Option mutates Config during construction.
type Option func(*Config)

// WithWriteWait overrides Config.WriteWait.
// Pass a duration such as 10*time.Second; non-positive values disable write deadlines.
func WithWriteWait(d time.Duration) Option {
	return func(c *Config) {
		c.WriteWait = d
	}
}

// WithPongWait overrides Config.PongWait.
// Pass a duration such as 60*time.Second; non-positive values disable the timeout.
// When enabled, configure PingPeriod below PongWait so peers can reply in time.
func WithPongWait(d time.Duration) Option {
	return func(c *Config) {
		c.PongWait = d
	}
}

// WithPingPeriod overrides Config.PingPeriod.
// Pass a duration such as 54*time.Second; non-positive values fall back to 54 seconds.
func WithPingPeriod(d time.Duration) Option {
	return func(c *Config) {
		c.PingPeriod = d
	}
}

// WithMessageBufferSize overrides Config.MessageBufferSize.
func WithMessageBufferSize(size int) Option {
	return func(c *Config) {
		c.MessageBufferSize = size
	}
}

// WithDispatchAsync overrides Config.DispatchAsync.
func WithDispatchAsync(dispatch bool) Option {
	return func(c *Config) {
		c.DispatchAsync = dispatch
	}
}

// WithDeleteRoomOnEmpty controls automatic room removal when room size reaches 0.
func WithDeleteRoomOnEmpty(enabled bool) Option {
	return func(c *Config) {
		c.DeleteRoomOnEmpty = enabled
	}
}

// WithProtocol sets the serialization protocol for the Adler instance.
// Use JSON for text-based messaging or Protobuf for binary protobuf messages.
func WithProtocol(p Protocol) Option {
	return func(c *Config) {
		c.Protocol = p
	}
}

// WithReadMaxPayloadSize sets the incoming message limit in bytes.
// The default is 64 KiB. Non-positive values restore the default during construction.
// Messages exceeding the limit close the connection before reaching message handlers.
func WithReadMaxPayloadSize(size int) Option {
	return func(c *Config) {
		c.ReadMaxPayloadSize = size
	}
}

// defaultMessageBufferSize balances burst tolerance with per-session memory use.
const defaultMessageBufferSize = 128

// newConfig builds a Config with defaults, then applies options in order.
func newConfig(opts ...Option) *Config {
	cfg := &Config{
		WriteWait:          10 * time.Second,
		PongWait:           0,
		PingPeriod:         60 * time.Second,
		MessageBufferSize:  defaultMessageBufferSize,
		DispatchAsync:      true,
		DeleteRoomOnEmpty:  true,
		Protocol:           JSON,
		ReadMaxPayloadSize: defaultReadMaxPayloadSize,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt(cfg)
	}

	if cfg.ReadMaxPayloadSize <= 0 {
		cfg.ReadMaxPayloadSize = defaultReadMaxPayloadSize
	}

	return cfg
}
