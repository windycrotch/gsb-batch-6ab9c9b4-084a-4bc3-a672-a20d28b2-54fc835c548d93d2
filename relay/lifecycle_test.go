package relay

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

const lifecycleWait = 5 * time.Second

// readResult is the outcome of a Subscriber.Read call.
type readResult struct {
	buf *Buffer
	err error
}

func readAsync(sub *Subscriber) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		b, err := sub.Read()
		ch <- readResult{buf: b, err: err}
	}()
	return ch
}

func assertReadEnds(t *testing.T, sub *Subscriber, wantErr error, msg string) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		// Queued packets may still be readable before the terminal signal;
		// drain them and require Read to eventually end with an error.
		for {
			b, err := sub.Read()
			if b != nil {
				b.Release()
			}
			if err != nil {
				done <- err
				return
			}
		}
	}()

	select {
	case err := <-done:
		if wantErr != nil && !errors.Is(err, wantErr) && err != wantErr {
			t.Fatalf("%s: expected error %v, got %v", msg, wantErr, err)
		}
	case <-time.After(lifecycleWait):
		t.Fatalf("%s: Read blocked forever after teardown", msg)
	}
}

func gaugeValue(g prometheusGauge) float64 {
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

// prometheusGauge is the small subset of prometheus.Gauge used here.
type prometheusGauge interface {
	Write(*dto.Metric) error
}

// A1: closing the publisher channel tears the Channel down and every blocked
// reader gets an end signal instead of hanging.
func TestLifecycle_PublisherCloseEndsAllReaders(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		name := fmt.Sprintf("a1-%d", iter)
		r := NewRelay(&RelayConfig{BufferSize: 16, PacketSize: 4})
		pub, err := r.Publish(name)
		if err != nil {
			t.Fatal(err)
		}

		const n = 25
		type entry struct {
			sub   *Subscriber
			unsub UnsubscribeFunc
		}
		subs := make([]entry, n)
		for i := range n {
			s, u, err := r.Subscribe(name)
			if err != nil {
				t.Fatal(err)
			}
			subs[i] = entry{s, u}
		}

		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := range n {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = subs[i].sub.Read()
			}(i)
		}

		// Give readers a chance to park; teardown must work whether parked or not.
		time.Sleep(time.Millisecond)
		close(pub)

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(lifecycleWait):
			t.Fatalf("iter %d: one or more readers hung after publisher close", iter)
		}
		for i, err := range errs {
			if err == nil {
				t.Fatalf("iter %d reader %d: got nil error after publisher close", iter, i)
			}
		}
		for _, e := range subs {
			e.unsub() // must be safe after close
		}
	}
}

// A1 (Channel level): Close unblocks reads with errUpstreamClosed.
func TestLifecycle_ChannelCloseSignalsUpstreamClosed(t *testing.T) {
	ch := NewChannel("a1-channel", 1)
	sub, unsub := ch.Sub()
	defer unsub()

	ch.Close()
	assertReadEnds(t, sub, errUpstreamClosed, "channel close")
}

// A2 + A5: an overflowed subscriber's Read ends, and both the atomic clients
// statistic and the Prometheus gauge stay consistent when the play loop's
// deferred unsubscribe subsequently runs.
func TestLifecycle_OverflowEndsReadAndKeepsStats(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		name := fmt.Sprintf("a2-%d", iter)
		ch := NewChannel(name, 1)
		sub, unsub := ch.Sub()
		gauge := ch.activeClients
		pool := NewBufferPool(1)

		// Capacity is 1: one packet queues, the second overflows the reader.
		for range 2 {
			b := pool.Get()
			b.Resize(1)
			ch.Pub(b)
		}

		assertReadEnds(t, sub, errLateSubscriber, fmt.Sprintf("iter %d overflow", iter))

		// Mimic srt.Server.play's deferred unsubscribe after Read fails.
		unsub()
		// A second unsubscribe must be idempotent as well.
		unsub()

		ch.mutex.Lock()
		actual := len(ch.subs)
		ch.mutex.Unlock()

		if actual != 0 {
			t.Fatalf("iter %d: actual subscribers=%d, want 0", iter, actual)
		}
		if got := ch.Stats().clients; got != 0 {
			t.Fatalf("iter %d: Stats clients=%d, want 0", iter, got)
		}
		if v := gaugeValue(gauge); v != 0 {
			t.Fatalf("iter %d: active_clients gauge=%v, want 0 (double decrement)", iter, v)
		}
	}
}

// A5 through the full Relay statistics surface after overflow + deferred unsub.
func TestLifecycle_RelayStatisticsAfterOverflow(t *testing.T) {
	const packets = 4
	r := NewRelay(&RelayConfig{BufferSize: packets, PacketSize: 1})
	pub, err := r.Publish("a5")
	if err != nil {
		t.Fatal(err)
	}
	defer close(pub)

	sub, unsub, err := r.Subscribe("a5")
	if err != nil {
		t.Fatal(err)
	}

	// No concurrent reader: the buffer fills and the next publication must
	// overflow and remove the subscriber. Publish through the channel and then
	// poll statistics so the asynchronous publisher goroutine has settled.
	for range packets + 2 {
		b := r.AcquireBuffer()
		b.Resize(1)
		pub <- b
	}

	deadline := time.Now().Add(lifecycleWait)
	for time.Now().Before(deadline) {
		for _, st := range r.GetStatistics() {
			if st.Name == "a5" && st.Clients == 0 {
				goto settled
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("statistics never reached 0 after overflow")
settled:

	// The removed subscriber's Read must end as well.
	assertReadEnds(t, sub, nil, "relay overflow")

	// Mimic the play loop's deferred unsubscribe; it must not go negative or
	// change the settled count.
	unsub()

	for _, st := range r.GetStatistics() {
		if st.Name == "a5" && st.Clients != 0 {
			t.Fatalf("clients=%d after overflow+unsub, want 0", st.Clients)
		}
	}
}

// A3 + A6: after a publisher disconnects the name can be republished, new
// subscribers receive the new stream, and old subscribers are terminated and
// never see the new publisher's data.
func TestLifecycle_RepublishReplacesGeneration(t *testing.T) {
	r := NewRelay(&RelayConfig{BufferSize: 64, PacketSize: 4})

	pub1, err := r.Publish("a3")
	if err != nil {
		t.Fatal(err)
	}
	old, oldUnsub, err := r.Subscribe("a3")
	if err != nil {
		t.Fatal(err)
	}

	close(pub1)
	assertReadEnds(t, old, errUpstreamClosed, "old reader after publisher 1 disconnect")
	oldUnsub()

	// Republishing the same name must succeed once teardown completed.
	deadline := time.Now().Add(lifecycleWait)
	var pub2 chan<- *Buffer
	for time.Now().Before(deadline) {
		pub2, err = r.Publish("a3")
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("same stream name should be reusable after disconnect: %v", err)
	}
	defer close(pub2)

	fresh, freshUnsub, err := r.Subscribe("a3")
	if err != nil {
		t.Fatal(err)
	}
	defer freshUnsub()

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	buf := r.AcquireBuffer()
	copy(buf.Bytes(), payload)
	buf.Resize(len(payload))
	pub2 <- buf

	select {
	case res := <-readAsync(fresh):
		if res.err != nil {
			t.Fatalf("new subscriber read failed: %v", res.err)
		}
		if got := res.buf.Bytes(); string(got) != string(payload) {
			t.Fatalf("new subscriber got %x, want %x", got, payload)
		}
		res.buf.Release()
	case <-time.After(lifecycleWait):
		t.Fatal("new subscriber did not receive data from the new publisher")
	}

	// The old subscriber stays terminated and must never yield new data.
	if b, err := old.Read(); err == nil {
		b.Release()
		t.Fatal("old subscriber was resurrected and received new publisher data")
	}
}

// A3/H3 tight interleaving: republish immediately after closing the old
// publisher input, with no sleep or wait. The name must be reclaimable
// synchronously, old readers ended, and new readers served.
func TestLifecycle_ImmediateRepublishReclaimsName(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		name := fmt.Sprintf("imm-%d", iter)
		r := NewRelay(&RelayConfig{BufferSize: 16, PacketSize: 4})

		pub1, err := r.Publish(name)
		if err != nil {
			t.Fatal(err)
		}
		old, oldUnsub, err := r.Subscribe(name)
		if err != nil {
			t.Fatal(err)
		}

		close(pub1)

		// No synchronization delay: reclaim at once.
		pub2, err := r.Publish(name)
		if err != nil {
			t.Fatalf("iter %d: immediate republish failed: %v", iter, err)
		}

		assertReadEnds(t, old, nil, fmt.Sprintf("iter %d old reader", iter))
		oldUnsub()

		fresh, freshUnsub, err := r.Subscribe(name)
		if err != nil {
			t.Fatalf("iter %d: subscribe to new generation: %v", iter, err)
		}

		payload := []byte{byte(iter), 2, 3, 4}
		buf := r.AcquireBuffer()
		copy(buf.Bytes(), payload)
		buf.Resize(4)
		pub2 <- buf

		select {
		case res := <-readAsync(fresh):
			if res.err != nil {
				t.Fatalf("iter %d: fresh read: %v", iter, res.err)
			}
			if got := res.buf.Bytes(); string(got) != string(payload) {
				t.Fatalf("iter %d: got %x want %x", iter, got, payload)
			}
			res.buf.Release()
		case <-time.After(lifecycleWait):
			t.Fatalf("iter %d: new subscriber timed out", iter)
		}
		freshUnsub()
		close(pub2)
	}
}

// A4/H3 concurrency at the Relay level: racing publishers that repeatedly
// claim/close/reclaim a name together with subscribing/unsubscribing readers
// must never panic, strand a reader, or report a negative client count.
func TestLifecycle_RelayGenerationChaos(t *testing.T) {
	r := NewRelay(&RelayConfig{BufferSize: 2, PacketSize: 1})
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Publisher contenders.
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				pub, err := r.Publish("chaos")
				if err != nil {
					time.Sleep(20 * time.Microsecond)
					continue
				}
				for j := 0; j < 30; j++ {
					b := r.AcquireBuffer()
					b.Resize(1)
					select {
					case pub <- b:
					case <-stop:
						b.Release()
						close(pub)
						return
					}
				}
				close(pub)
			}
		}()
	}

	// Subscriber churn with reader reaping.
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				sub, unsub, err := r.Subscribe("chaos")
				if err != nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}
				readerDone := make(chan struct{})
				go func() {
					defer close(readerDone)
					for {
						b, err := sub.Read()
						if err != nil {
							return
						}
						b.Release()
					}
				}()

				select {
				case <-time.After(300 * time.Microsecond):
				case <-stop:
				}
				unsub()
				select {
				case <-readerDone:
				case <-time.After(lifecycleWait):
					t.Errorf("reader did not stop after unsubscribe")
					return
				}
			}
		}()
	}

	// Statistics must never be negative.
	monDone := make(chan struct{})
	go func() {
		defer close(monDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, st := range r.GetStatistics() {
				if st.Clients < 0 {
					t.Errorf("negative client count %d", st.Clients)
					return
				}
			}
		}
	}()

	time.Sleep(400 * time.Millisecond)
	close(stop)

	workers := make(chan struct{})
	go func() { wg.Wait(); close(workers) }()
	select {
	case <-workers:
	case <-time.After(10 * time.Second):
		t.Fatal("chaos workers hung")
	}
	<-monDone

	// Eventually the final generation is torn down with zero clients.
	deadline := time.Now().Add(lifecycleWait)
	for time.Now().Before(deadline) {
		stats := r.GetStatistics()
		bad := false
		for _, st := range stats {
			if st.Clients != 0 {
				bad = true
			}
		}
		if len(stats) == 0 || !bad {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("chaos stream did not settle at zero clients")
}

// A4: concurrent unsubscribe together with one or many Close calls must never
// panic and must settle the client count at zero.
func TestLifecycle_ConcurrentUnsubscribeAndClose(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		name := fmt.Sprintf("a4-%d", iter)
		ch := NewChannel(name, 4)

		const n = 16
		unsubs := make([]UnsubscribeFunc, n)
		for i := range n {
			_, unsubs[i] = ch.Sub()
		}

		var wg sync.WaitGroup
		for _, u := range unsubs {
			wg.Add(1)
			go func(u UnsubscribeFunc) {
				defer wg.Done()
				u()
			}(u)
		}
		// Multiple concurrent/repeated closes must be panic-free.
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch.Close()
			}()
		}
		wg.Wait()

		if got := ch.Stats().clients; got != 0 {
			t.Fatalf("iter %d: clients=%d after concurrent unsub/close, want 0", iter, got)
		}
	}
}

// Under simultaneous sub/unsub/pub-overflow, the atomic statistic must always
// equal the actual subscriber set size and never be negative.
func TestLifecycle_StatsMatchSubscribersUnderChurn(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		name := fmt.Sprintf("churn-%d", iter)
		ch := NewChannel(name, 2)
		pool := NewBufferPool(1)

		stop := make(chan struct{})
		var workers sync.WaitGroup

		for range 6 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					sub, unsub := ch.Sub()
					go func(s *Subscriber) {
						for {
							b, err := s.Read()
							if err != nil {
								return
							}
							b.Release()
						}
					}(sub)
					time.Sleep(20 * time.Microsecond)
					unsub()
				}
			}()
		}

		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				b := pool.Get()
				b.Resize(1)
				ch.Pub(b)
			}
		}()

		// Invariant checker.
		checkDone := make(chan struct{})
		go func() {
			defer close(checkDone)
			for {
				select {
				case <-stop:
					return
				default:
				}
				ch.mutex.Lock()
				actual := len(ch.subs)
				stored := ch.clients.Load().(int)
				ch.mutex.Unlock()
				if stored != actual {
					t.Errorf("stored clients=%d != actual len(subs)=%d", stored, actual)
					return
				}
			}
		}()

		time.Sleep(150 * time.Millisecond)
		close(stop)
		ch.Close() // release any readers still parked
		workers.Wait()
		<-checkDone

		if got := ch.Stats().clients; got != 0 {
			t.Fatalf("iter %d: final clients=%d, want 0", iter, got)
		}
	}
}
