package relay

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrStreamAlreadyExists = errors.New("stream already exists")
	ErrStreamNotExisting   = errors.New("stream does not exist")
)

type RelayConfig struct {
	BufferSize uint
	PacketSize uint
}

type StreamStatistics struct {
	Name    string    `json:"name"`
	URL     string    `json:"url"`
	Clients int       `json:"clients"`
	Created time.Time `json:"created"`
}

// Relay represents a multi-channel stream relay
type Relay struct {
	mutex    sync.Mutex
	channels map[string]*Channel
	config   *RelayConfig
	pool     *BufferPool
}

// NewRelay creates a relay
func NewRelay(config *RelayConfig) *Relay {
	if config.BufferSize == 0 {
		config.BufferSize = 384000 // 1s @ 3Mbits/s
	}
	if config.PacketSize == 0 {
		config.PacketSize = 1316 // default packet size
	}
	return &Relay{
		channels: make(map[string]*Channel),
		config:   config,
		pool:     NewBufferPool(config.PacketSize),
	}
}

func (s *Relay) AcquireBuffer() *Buffer {
	return s.pool.Get()
}

// Publish claims a stream name for publishing
func (s *Relay) Publish(name string) (chan<- *Buffer, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if previous, exists := s.channels[name]; exists {
		// A previous publisher whose input has already been closed can be
		// reaped immediately, allowing a reconnecting publisher to claim the
		// name without waiting for its teardown goroutine to be scheduled.
		if !s.reapClosedPublisherLocked(name, previous) {
			return nil, ErrStreamAlreadyExists
		}
	}

	channel := NewChannel(name, s.config.BufferSize/s.config.PacketSize)

	ch := make(chan *Buffer)
	channel.pub = ch
	s.channels[name] = channel

	// Setup publisher goroutine
	go func() {
		for {
			buf, ok := <-ch

			// Channel closed, Teardown pubsub
			if !ok {
				// Need a lock on the map first to stop new subscribers
				s.mutex.Lock()
				// Only evict the entry when it still belongs to this channel
				// generation. A same-name replacement publisher may already
				// have installed a new channel during reconnect interleaving.
				if current, exists := s.channels[name]; exists && current == channel {
					delete(s.channels, name)
				}
				channel.Close()
				s.mutex.Unlock()
				return
			}

			// Publish buf to subscribers
			channel.Pub(buf)
		}
	}()
	return ch, nil
}

// reapClosedPublisherLocked reports whether previous's publisher input is
// closed and, when it is, finishes teardown and removes the channel from the
// relay map so the name can be reused. It returns false when a live publisher
// still owns the name.
//
// The caller must hold s.mutex.
func (s *Relay) reapClosedPublisherLocked(name string, previous *Channel) bool {
	if previous.pub == nil {
		return false
	}

	select {
	case buf, ok := <-previous.pub:
		if ok {
			// A live publisher had a packet immediately available; do not
			// swallow it. Forward it exactly as the publisher goroutine would
			// and keep the name claimed.
			previous.Pub(buf)
			return false
		}

		// Publisher closed its input channel: complete teardown synchronously.
		if current, exists := s.channels[name]; exists && current == previous {
			delete(s.channels, name)
		}
		previous.Close()
		return true

	default:
		// Open channel with no packet ready: publisher is still active.
		return false
	}
}

type Subscriber struct {
	ch         chan *Buffer
	chanClosed <-chan struct{}
}

var (
	errUpstreamClosed = errors.New("upstream closed")
	errLateSubscriber = errors.New("late subscriber")
)

func (s *Subscriber) Read() (*Buffer, error) {
	// A closed upstream channel is the permanent, authoritative termination
	// signal. Check it before blocking so a concurrently closed data channel
	// cannot make publisher teardown observable as a different end state.
	select {
	case <-s.chanClosed:
		return nil, errUpstreamClosed
	default:
	}

	select {
	case <-s.chanClosed:
		return nil, errUpstreamClosed
	case buf, ok := <-s.ch:
		if !ok {
			// The data channel is closed for individual removal (overflow or
			// unsubscribe). If the whole upstream went away at the same time,
			// report that instead.
			select {
			case <-s.chanClosed:
				return nil, errUpstreamClosed
			default:
			}
			return nil, errLateSubscriber
		}
		return buf, nil
	}
}

// Subscribe subscribes to a stream by name
func (s *Relay) Subscribe(name string) (*Subscriber, UnsubscribeFunc, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	channel, ok := s.channels[name]
	if !ok {
		return nil, nil, ErrStreamNotExisting
	}
	ch, unsub := channel.Sub()
	return ch, unsub, nil
}

func (s *Relay) GetStatistics() []*StreamStatistics {
	statistics := make([]*StreamStatistics, 0)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for name, channel := range s.channels {
		stats := channel.Stats()
		statistics = append(statistics, &StreamStatistics{
			Name:    name,
			Clients: stats.clients,
			Created: stats.created,
		})
	}
	return statistics
}

func (s *Relay) ChannelExists(name string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, exists := s.channels[name]
	return exists
}
