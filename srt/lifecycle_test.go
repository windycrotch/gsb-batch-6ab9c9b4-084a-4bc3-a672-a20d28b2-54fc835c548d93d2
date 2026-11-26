package srt

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/voc/srtrelay/relay"
	"github.com/voc/srtrelay/stream"
)

// A8: through the real srt.Server publish/play wiring, disconnecting the
// publisher (source socket yields EOF) must tear the relay channel down and
// make every blocked play loop return promptly instead of hanging.
func TestLifecycle_PlayEndsWhenPublisherDisconnects(t *testing.T) {
	srv := NewServer(&Config{
		Server: ServerConfig{PacketSize: 1316},
		Relay:  relay.RelayConfig{BufferSize: 1316 * 100, PacketSize: 1316},
	})

	pubID, err := stream.NewStreamID("a8", "", stream.ModePublish)
	if err != nil {
		t.Fatal(err)
	}
	playID, err := stream.NewStreamID("a8", "", stream.ModePlay)
	if err != nil {
		t.Fatal(err)
	}

	rd := &testSocket{ch: make(chan []byte)}
	pubConn := &srtConn{log: slog.Default(), socket: rd, streamid: pubID}

	pubDone := make(chan error, 1)
	go func() {
		pubDone <- srv.publish(pubConn)
	}()

	// Wait until the publish claim created the relay channel.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !srv.relay.ChannelExists("a8") {
		time.Sleep(time.Millisecond)
	}
	if !srv.relay.ChannelExists("a8") {
		t.Fatal("relay channel was not created by publish")
	}

	const players = 4
	var playWg sync.WaitGroup
	playErrs := make(chan error, players)
	for range players {
		wr := &testSocket{}
		playWg.Add(1)
		go func() {
			defer playWg.Done()
			playErrs <- srv.play(&srtConn{
				log:      slog.Default(),
				socket:   wr,
				streamid: playID,
			})
		}()
	}

	// Let players subscribe and park in Subscriber.Read.
	time.Sleep(50 * time.Millisecond)

	for range 10 {
		rd.ch <- []byte{1, 2, 3, 4}
	}

	// Publisher abnormal/clean disconnect: source read starts returning EOF.
	close(rd.ch)

	select {
	case err := <-pubDone:
		if err != io.EOF {
			t.Fatalf("publish expected io.EOF after source close, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publisher did not return after source socket closed")
	}

	playsDone := make(chan struct{})
	go func() { playWg.Wait(); close(playsDone) }()
	select {
	case <-playsDone:
	case <-time.After(3 * time.Second):
		t.Fatal("one or more play loops hung after publisher disconnect")
	}
	close(playErrs)
	for err := range playErrs {
		if err != nil {
			t.Fatalf("play expected clean return after upstream close, got %v", err)
		}
	}

	// Relay must have dropped the stream during teardown.
	if srv.relay.ChannelExists("a8") {
		t.Fatal("relay channel should be removed after publisher disconnect")
	}
}
