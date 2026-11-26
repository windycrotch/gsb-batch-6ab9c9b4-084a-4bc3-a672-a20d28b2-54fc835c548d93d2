package relay

import (
	"log"

	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/voc/srtrelay/internal/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const relaySubsystem = "relay"

var (
	activeClients = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(metrics.Namespace, relaySubsystem, "active_clients"),
			Help: "The number of active clients per channel",
		},
		[]string{"channel_name"},
	)
	channelCreatedTimestamp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(metrics.Namespace, relaySubsystem, "created_timestamp_seconds"),
			Help: "The UNIX timestamp when the channel was created",
		},
		[]string{"channel_name"},
	)
)

type UnsubscribeFunc func()

type Channel struct {
	name       string
	mutex      sync.Mutex
	subs       Subs
	maxPackets uint
	closed     chan struct{}
	closeOnce  sync.Once

	// pub is the input channel owned by the publisher forwarder. It is kept
	// here so a concurrent Publish can detect an already-closed previous
	// publisher and reclaim the name synchronously.
	pub chan *Buffer

	// statistics
	clients atomic.Value
	created time.Time

	// Prometheus metrics.
	activeClients    prometheus.Gauge
	createdTimestamp prometheus.Gauge
}
type Subs []*Subscriber

type Stats struct {
	clients int
	created time.Time
}

// Remove single subscriber
func (subs Subs) Remove(sub chan *Buffer) Subs {
	idx := -1
	for i := range subs {
		if subs[i].ch == sub {
			idx = i
			break
		}
	}

	// subscriber was already removed
	if idx < 0 {
		return subs
	}

	for {
		select {
		case buf := <-sub:
			if buf != nil {
				buf.Release()
			}
		default:
			close(sub)
			return slices.Delete(subs, idx, idx+1)
		}
	}
}

func NewChannel(name string, maxPackets uint) *Channel {
	channelActiveClients := activeClients.WithLabelValues(name)
	ch := &Channel{
		name:          name,
		subs:          make(Subs, 0, 10),
		maxPackets:    maxPackets,
		created:       time.Now(),
		activeClients: channelActiveClients,
		closed:        make(chan struct{}),
	}
	ch.clients.Store(0)
	ch.createdTimestamp = channelCreatedTimestamp.WithLabelValues(name)
	ch.createdTimestamp.Set(float64(ch.created.UnixNano()) / 1000000.0)
	return ch
}

// removeSubscriberLocked removes sub from the subscriber set, closes its data
// channel to unblock pending reads and updates the client statistics exactly
// once.
//
// It is a no-op when sub is no longer registered, which is the case when it
// has already been removed for a buffer overflow or by a previous
// unsubscribe/close call. This makes every teardown path idempotent and
// prevents counters from being decremented more than once.
//
// The caller must hold ch.mutex.
func (ch *Channel) removeSubscriberLocked(sub *Subscriber) {
	idx := -1
	for i := range ch.subs {
		if ch.subs[i] == sub {
			idx = i
			break
		}
	}
	if idx < 0 {
		// Already removed and accounted for.
		return
	}

	ch.subs = ch.subs.Remove(sub.ch)
	ch.clients.Store(len(ch.subs))
	ch.activeClients.Dec()
}

// Sub subscribes to a channel
func (ch *Channel) Sub() (*Subscriber, UnsubscribeFunc) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	sub := &Subscriber{
		ch:         make(chan *Buffer, ch.maxPackets),
		chanClosed: ch.closed,
	}
	ch.subs = append(ch.subs, sub)
	ch.clients.Store(len(ch.subs))
	ch.activeClients.Inc()

	var unsub UnsubscribeFunc = func() {
		ch.mutex.Lock()
		defer ch.mutex.Unlock()

		// Safe on a closed channel and after an overflow removal: a missing
		// subscriber makes the removal a no-op instead of double accounting.
		// Close already reset the counter when ch.subs is nil.
		ch.removeSubscriberLocked(sub)
	}
	return sub, unsub
}

// Pub publishes a packet to a channel
func (ch *Channel) Pub(b *Buffer) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	toRemove := make(Subs, 0, 5)
	for i := range ch.subs {
		b.Retain()
		select {
		case ch.subs[i].ch <- b:
			continue

		// Remember overflowed chans for drop
		default:
			b.Release()
			toRemove = append(toRemove, ch.subs[i])
			log.Println("dropping overflowing client", i)
		}
	}
	for _, sub := range toRemove {
		ch.removeSubscriberLocked(sub)
	}
	ch.clients.Store(len(ch.subs))
	b.Release()
}

// Close closes a channel. It is idempotent and safe to call concurrently: the
// teardown, including metric deletion, runs exactly once.
func (ch *Channel) Close() {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	ch.closeOnce.Do(func() {
		close(ch.closed)

		// Remove subscribers explicitly through the same accounting path as
		// unsubscribe/overflow: this returns queued buffers to the pool,
		// closes their read channels and balances the active-clients gauge.
		for len(ch.subs) > 0 {
			ch.removeSubscriberLocked(ch.subs[0])
		}
		ch.subs = nil
		ch.clients.Store(0)
		activeClients.DeleteLabelValues(ch.name)
		channelCreatedTimestamp.DeleteLabelValues(ch.name)
	})
}

func (ch *Channel) Stats() Stats {
	return Stats{
		clients: ch.clients.Load().(int),
		created: ch.created,
	}
}
