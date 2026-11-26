//go:build !cgo

package enet

// This file is a pure-Go stand-in for the CGO-based ENet binding. It is used
// automatically when CGO is disabled (CGO_ENABLED=0), e.g. on systems without
// the ENet development headers installed. It performs no real networking; it
// exists so the rest of the server (which does not depend on CGO itself) can
// be built and tested locally. Tests can observe traffic by setting SendHook
// on a Peer and DisconnectHook on a Host.

import (
	"net"

	"github.com/sauerbraten/waiter/pkg/protocol/disconnectreason"
)

type PeerState uint

type Peer struct {
	Address *net.UDPAddr
	State   PeerState

	// SendHook, if set, is called with every packet that would be sent to
	// this peer.
	SendHook func(channel uint8, payload []byte)
}

func (p *Peer) Send(channel uint8, payload []byte) {
	if len(payload) == 0 || p.SendHook == nil {
		return
	}
	p.SendHook(channel, payload)
}

type Host struct {
	// DisconnectHook, if set, is called when a peer is disconnected.
	DisconnectHook func(p *Peer, reason disconnectreason.ID)
}

func NewHost(laddr string, lport int) (*Host, error) { return &Host{}, nil }

// Service returns a channel on which no events are ever delivered.
func (h *Host) Service() <-chan Event { return make(chan Event) }

func (h *Host) Disconnect(p *Peer, reason disconnectreason.ID) {
	if h.DisconnectHook != nil {
		h.DisconnectHook(p, reason)
	}
}

type EventType uint

const (
	EventTypeConnect EventType = iota + 1
	EventTypeDisconnect
	EventTypeReceive
)

type Event struct {
	Type      EventType
	Peer      *Peer
	ChannelID uint8
	Data      uint32
	Packet    *Packet
}

const (
	PacketFlagNone               = 0
	PacketFlagReliabe            = (1 << 0)
	PacketFlagUnsequenced        = (1 << 1)
	PacketFlagNoAllocate         = (1 << 2)
	PacketFlagUnreliableFragment = (1 << 3)
	PacketFlagSent               = (1 << 8)
)

type Packet struct {
	Flags uint32 // bitwise-or of ENetPacketFlag constants
	Data  []byte // allocated data for packet
}
