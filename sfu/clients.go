package sfu

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"atomirex.com/umbrella/razor"
	"github.com/gorilla/websocket"
)

type RemoteClient interface {
	Label() string
	getStatus() *SFUStatusClient
	stop()
	AddOutgoingTracksForIncomingTrack(*incomingTrack)
	RemoveOutgoingTracksForIncomingTrack(*incomingTrack)
	RequestEvalState()

	// IsRelay reports whether this client is a trunk relay to another node which
	// is managed by the SFU's intended-server set. Relays may only stay attached
	// to the SFU while they are still part of the intended set.
	IsRelay() bool

	// RelayIdentity returns the address and generation of the relay instance
	// this client belongs to. Generation is only meaningful for relays.
	RelayIdentity() (url string, generation uint64, ok bool)
}

type BaseClient struct {
	label  string
	logger *razor.Logger

	// True for trunk relays which are owned by the SFU intended-server set.
	isRelay bool

	// Identity of the relay instance, used so that SFU commands can recognise
	// an instance even before the SFU has stored it (a relay may connect and
	// register itself before NewClient returns).
	relayURL        string
	relayGeneration uint64

	// Relay lifecycle hooks. Both are invoked at most once per instance.
	onFailed     func() // The relay could not be established
	onTerminated func() // The relay has fully torn down

	terminalOnce sync.Once
	stopped      atomic.Bool
}

func (bc *BaseClient) Label() string {
	return bc.label
}

func (bc *BaseClient) IsRelay() bool {
	return bc.isRelay
}

func (bc *BaseClient) RelayIdentity() (string, uint64, bool) {
	if !bc.isRelay {
		return "", 0, false
	}
	return bc.relayURL, bc.relayGeneration, true
}

func (bc *BaseClient) notifyTerminated() {
	bc.terminalOnce.Do(func() {
		if bc.onTerminated != nil {
			bc.onTerminated()
		}
	})
}

func (bc *BaseClient) notifyFailed() {
	if bc.onFailed != nil {
		bc.onFailed()
	}
}

type RemoteClientParameters struct {
	logger   *razor.Logger
	trunkurl string
	s        *Sfu
	ws       *websocket.Conn

	// Only set for relays managed by the intended-server set.
	// Both are invoked exactly once for a given relay instance.
	onFailed     func() // The relay could not be established
	onTerminated func() // The relay has fully torn down

	// Identity assigned by the SFU when it creates the relay instance.
	relayGeneration uint64
}

type RemoteClientFactory interface {
	NewClient(params *RemoteClientParameters) RemoteClient
}

type DefaultRemoteClientFactory struct{}

// This can actually block ever returning . . . thanks to looping on the websocket
// not great
func (rcf *DefaultRemoteClientFactory) NewClient(params *RemoteClientParameters) RemoteClient {
	if params.trunkurl == "" {
		c := &client{
			BaseClient: BaseClient{
				label:  fmt.Sprintf("Incoming client from %s", params.ws.UnderlyingConn().RemoteAddr()),
				logger: params.logger,
			},

			websocket: params.ws,
		}

		c.run(params.ws, params.s)

		c.continueWebsocket(params.s)

		return c
	} else {
		if strings.HasPrefix(params.trunkurl, "rtsp") {
			c := &RtspClient{
				BaseClient: BaseClient{
					label:           fmt.Sprintf("RTSP client of %s", params.trunkurl),
					logger:          params.logger,
					isRelay:         true,
					relayURL:        params.trunkurl,
					relayGeneration: params.relayGeneration,
				},

				url: params.trunkurl,
			}
			c.onFailed = params.onFailed
			c.onTerminated = params.onTerminated

			c.run(params.s)

			return c
		} else {
			c := &client{
				BaseClient: BaseClient{
					label:           fmt.Sprintf("Trunking client to %s", params.trunkurl),
					logger:          params.logger,
					isRelay:         true,
					relayURL:        params.trunkurl,
					relayGeneration: params.relayGeneration,
				},

				trunkurl: params.trunkurl,
			}
			c.onFailed = params.onFailed
			c.onTerminated = params.onTerminated

			c.run(nil, params.s)

			return c
		}
	}
}
