# Adler

Adler is a lightweight WebSocket server toolkit for Go. It gives you a small, focused API for upgrading HTTP requests, handling sessions, broadcasting messages, and organizing clients into rooms.

Built on [gws](https://github.com/lxzan/gws) — a high‑performance, RFC 6455‑compliant WebSocket library with zero‑alloc reads and aggressive buffer reuse.

> **Migrated from gobwas/ws → gws.** The original gobwas/ws implementation is preserved at tags [`v1.4.1`](https://github.com/catalinfl/adler/releases/tag/v1.4.1) and the [`gobwas/ws`](https://github.com/catalinfl/adler/tree/gobwas/ws) branch. The switch was driven by gws's superior throughput (~7.8M msg/s vs ~2.5M msg/s) and zero‑alloc read path, which eliminated GC pressure from frame parsing under high concurrency.

## Features

- WebSocket upgrade from `net/http`
- High‑performance gws engine with zero‑alloc reads and ~7.8M msg/s throughput
- Session lifecycle hooks
- Server-initiated close codes with UTF-8-safe reasons and bounded disconnect cleanup
- Text, binary, and JSON messaging
- Protocol-aware JSON or Protobuf serialization (`Write`, `BroadcastAny`)
- Global broadcast, filtered broadcast, and targeted session sends
- Rooms with join, leave, and room-level broadcast helpers
- Configurable inbound message limits and pong-based connection timeouts
- Per-session key-value storage
- Optional access to the underlying request, protocol, and connection

## Roadmap

- [x] Add a dedicated matchmaker module for queue creation, queue management, and match orchestration.
- [x] Replace gobwas/ws with gws for high-throughput, zero-alloc WebSocket engine.
- [ ] Add ELO-based matchmaking with configurable rating buckets and match quality rules.

## Installation

```bash
go get github.com/catalinfl/adler
```

## Quick Start

```go
package main

import (
    "log"
    "net/http"

    "github.com/catalinfl/adler"
)

func main() {
    a := adler.New()

    a.HandleConnect(func(s *adler.Session) {
        log.Println("connected:", s.RemoteAddr())
    })

    a.HandleMessage(func(s *adler.Session, msg []byte) {
        _ = s.WriteText([]byte("echo: " + string(msg)))
    })

    a.HandleError(func(s *adler.Session, err error) {
        log.Println("ws error:", err)
    })

    http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
        if err := a.HandleRequest(w, r); err != nil {
            log.Println("handle request:", err)
        }
    })

    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

## How The API Works

### 1. Create a server

Use `adler.New()` to build a server instance. You can pass configuration options at construction time:

```go
// Import "time" for duration options.
a := adler.New(
    adler.WithDispatchAsync(true),
    adler.WithMessageBufferSize(256),
    adler.WithPingPeriod(30*time.Second),
)
```

### 2. Register handlers before serving requests

Handlers are set on the `Adler` instance and are called during the session lifecycle.

- `HandleConnect` runs after a session is registered.
- `HandleMessage` receives text frames.
- `HandleMessageBinary` receives binary frames.
- `HandlePong` receives pong frames.
- `HandleClose` receives close code and reason.
- `HandleSentMessage` and `HandleSentMessageBinary` run after server writes succeed.
- `HandleError` receives runtime errors from the session loop.
- `OnRoomJoin` and `OnRoomLeave` receive room membership events.
- `HandleDisconnect` runs after a session is unregistered, closed and removed from its room.
- `AddDisconnectHook` registers an additional disconnect callback that `HandleDisconnect` does not replace (see below).

Example:

```go
a.HandleConnect(func(s *adler.Session) {
    s.Set("userID", "123")
})

a.HandleClose(func(s *adler.Session, code int, reason string) {
    log.Printf("client closed: code=%d reason=%q", code, reason)
})
```

#### Additive disconnect hooks

`HandleDisconnect` stores a single callback, so calling it twice replaces the
previous one. Library modules that need reliable cleanup on disconnect use
`AddDisconnectHook` instead: hooks accumulate, run in registration order, and run
before the `HandleDisconnect` callback. That lets application code own
`HandleDisconnect` without silently disabling module cleanup (for example, the
matchmaker removing a disconnected session from its queues).

```go
a.AddDisconnectHook(func(s *adler.Session) {
    // always runs, even when the application later calls HandleDisconnect
})

a.HandleDisconnect(func(s *adler.Session) {
    // application-level disconnect handling
})
```

`Session.IsClosed()` reports whether the session has already been marked closed.
The flag is set before disconnect callbacks run, so cleanup code can use it to
reject work for a connection that is already gone.

### 3. Serve the websocket endpoint

Call `HandleRequest` from an HTTP handler. It upgrades the connection and blocks until the session ends.

```go
http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
    _ = a.HandleRequest(w, r)
})
```

### 4. Work with a Session

`Session` is the object you receive in callbacks. It exposes message writers and a small key-value store.

Messaging helpers:

- `Write(any)` (protocol-aware JSON text or Protobuf binary)
- `WriteText([]byte)`
- `WriteTextWithDeadline([]byte, time.Duration)`
- `WriteBinary([]byte)`
- `WriteBinaryWithDeadline([]byte, time.Duration)`
- `WriteJSON(adler.Map)`
- `WriteJSONWithDeadline(any, time.Duration)`
- `Close(...[]byte)`
- `CloseWithCode(uint16, string)`

Protocol-aware helpers (`Write`, `BroadcastAny`, `Room.BroadcastAny`) use the server
protocol (JSON by default). Set `adler.WithProtocol(adler.Protobuf)` to emit
protobuf binary frames.

Storage helpers:

- `Set(key, value)`
- `SetNX(key, value)`
- `Get(key)`
- `GetString(key)`
- `GetInt(key)`
- `GetInt64(key)`
- `GetFloat(key)`
- `GetBool(key)`
- `Has(key)`
- `Unset(key)`
- `Keys()`
- `Values()`
- `Clear()`
- `Incr(key)` and `Decr(key)` for `*int64` counters

Metadata:

- `Request()` returns the original HTTP request
- `Protocol()` returns the HTTP protocol string used during upgrade
- `LocalAddr()` and `RemoteAddr()` expose the connection addresses
- `Room()` returns the current room
- `UnsafeConn()` exposes the raw network connection when you need low-level control

Example session storage:

```go
a.HandleConnect(func(s *adler.Session) {
    s.Set("role", "admin")
    s.SetNX("seen", true)
})

a.HandleMessage(func(s *adler.Session, msg []byte) {
    role, _ := s.GetString("role")
    _ = s.WriteText([]byte("role=" + role + " msg=" + string(msg)))
})
```

### 5. Broadcast to all or some clients

Use server-level broadcast helpers when you want to send to multiple sessions.

- `Broadcast([]byte)` sends text to all connected sessions
- `BroadcastFilter([]byte, func(*Session) bool)` sends text to matching sessions
- `BroadcastBinary([]byte)` sends binary to all connected sessions
- `BroadcastBinaryFilter([]byte, func(*Session) bool)` sends binary to matching sessions
- `BroadcastJSON(adler.Map)` broadcasts JSON
- `BroadcastJSONFilter(adler.Map, func(*Session) bool)` broadcasts JSON to matching sessions
- `BroadcastAny(any)` broadcasts using the configured protocol
- `BroadcastOthers([]byte, *Session)` sends to everyone except the target session
- `SendTo([]byte, *Session)` sends only to one session

Example:

```go
a.Broadcast([]byte("server says hello"))

a.SendTo([]byte("private message"), session)

a.BroadcastFilter([]byte("admins only"), func(s *adler.Session) bool {
    role, _ := s.GetString("role")
    return role == "admin"
})
```

### 6. Group clients in rooms

Rooms help you manage subsets of sessions.

```go
room := a.NewRoom("lobby")

a.HandleConnect(func(s *adler.Session) {
    _ = room.Join(s)
})

room.Broadcast([]byte("welcome to the lobby"))
```

Room helpers:

- `NewRoom(name)` returns an existing room or creates one
- `DeleteRoom(name)` removes a room manually when it is empty
- `Name()` returns the room name
- `Len()` returns the number of members
- `Sessions()` returns a snapshot of current members
- `Join(*Session)` adds a session to the room
- `Leave(*Session)` removes a session
- `OpenRoom()` allows joins again
- `CloseRoom()` blocks new joins
- `Broadcast`, `BroadcastBinary`, `BroadcastFilter`, `BroadcastBinaryFilter`, `BroadcastJSON`, `BroadcastJSONFilter`, `BroadcastAny`
- `BroadcastBinaryOther([]byte, *Session)` sends binary to everyone except the target session

With `WithDeleteRoomOnEmpty(true)` (the default), a room is removed when its last
member leaves or disconnects. Leave callbacks run before empty-room cleanup,
and cleanup completes before `HandleDisconnect`. Use `WithDeleteRoomOnEmpty(false)`
to retain empty rooms, for example when keeping a fixed set of chat channels.

### 7. Matchmaking with the Matchmaker Module

The `matchmaker` module provides queue-based matchmaking that groups sessions into rooms automatically. It uses a background goroutine to manage queues and ensure fair player distribution.

Features:

- **Non-blocking queue operations**: `AddToQueue()` and `RemoveFromQueue()` send commands to a worker goroutine
- **Main and waiting queues**: Keeps a main queue and an optional waiting queue when the main queue reaches capacity
- **Automatic room creation**: Creates Adler rooms when enough players are queued
- **JSON event notifications**: Sends events to sessions as they move through the queue

Matchmaker queue events are defined in [matchmaker/matchmaking.proto](matchmaker/matchmaking.proto) and
emitted as protobuf `QueueStatus` messages. When the Adler protocol is JSON (default),
`encoding/json` marshals the generated Go oneof structure using a `Payload` envelope:

- `Payload.QueueJoined` - Session added to main queue
- `Payload.WaitQueueJoined` - Session added to waiting queue (main is full)
- `Payload.PromotedToQueue` - Session promoted from waiting to main queue
- `Payload.MatchFound` - Match room created with `room_id` and player count
- `Payload.QueueError` - Queue operation error

For example, a successful match is sent as:

```json
{"Payload":{"MatchFound":{"room_id":"room_...","players":4}}}
```

This is the generated Go JSON representation, not the legacy flat `type` event
format or the `protojson` representation. Protobuf mode sends binary `QueueStatus`
messages instead.

Example:

```go
import "github.com/catalinfl/adler/matchmaker"

mm := matchmaking.NewMatchmaker(a, 
    matchmaking.WithRoomSize(4),
    matchmaking.WithQueueLength(20),
)

a.HandleConnect(func(s *adler.Session) {
    if err := mm.AddToQueue(s); err != nil {
        log.Printf("enqueue matchmaking: %v", err)
    }
})

a.HandleMessage(func(s *adler.Session, msg []byte) {
    if string(msg) == "leave_queue" {
        if err := mm.RemoveFromQueue(s); err != nil {
            log.Printf("enqueue matchmaking removal: %v", err)
        }
    }
})

// Use adler.WithProtocol(adler.Protobuf) to send queue events as protobuf frames.
```

Both queue methods return `ErrSessionNil` for a nil session and `ErrMatchmakerBusy`
when the shared command buffer is full. A nil error means the command was accepted,
not that the worker has completed it. A busy error means the command was not accepted;
logging it reports the failure but does not complete cleanup. Applications requiring
guaranteed removal must arrange a later retry. Do not wait for buffer space from a
callback running on the matchmaker worker, since that worker drains the same buffer.

`NewMatchmaker` also registers an additive disconnect hook on the Adler instance
(see `AddDisconnectHook`). When a session disconnects it is removed from the main
or waiting queue automatically, whether or not the application calls
`RemoveFromQueue`. If the shared command buffer is full at that moment, the
matchmaker keeps the removal in an internal retry set and applies it on the worker
goroutine, so a rejected command cannot leave a closed session in a queue. Repeated
cleanup and a cancel that races with the disconnect both end with the session
removed and its status set to left.

Queued sessions that have already disconnected are never promoted to the main
queue, never occupy a main-queue slot, and never join a room. Rooms are only built
from sessions that are still connected and still tracked by the queue, and the
`players` count in `match_found` always matches the number of sessions added to the
room. Removing a session preserves the relative order of the remaining players in
both queues.

`WithCommandBuffer(size)` configures the capacity of pending add/remove commands
(default: 1024). This is separate from `WithQueueLength(size)`, which limits players
in the main queue. The worker consumes commands and then updates the main/waiting queues:

```text
AddToQueue / RemoveFromQueue -> commands (1024 pending commands) -> worker -> player queues
```

### 8. Close handling

If the client sends a close frame, `HandleClose` receives the close status code and reason.

```go
a.HandleClose(func(s *adler.Session, code int, reason string) {
    log.Printf("close: code=%d reason=%q", code, reason)
})
```

To initiate a close from the server, use `CloseWithCode` with a valid WebSocket
status code and a UTF-8 reason:

```go
if err := s.CloseWithCode(4001, "session expired"); err != nil {
    log.Printf("close session: %v", err)
}
```

Reasons longer than 123 bytes are truncated without splitting a UTF-8 character.
The connection is forcibly closed after two seconds if the peer does not finish
the close handshake. Like `Close`, this method queues the close frame through the
existing outbound buffer; a nil error does not guarantee delivery, and queue
overflow is reported to `HandleError`. Already closed sessions return `ErrSessionClosed`.

## Configuration

Use these options with `adler.New(...)`:

- `WithWriteWait(time.Duration)` sets the outbound write timeout, e.g. `WithWriteWait(10*time.Second)` (default: 10 seconds); non-positive values disable write deadlines
- `WithPongWait(time.Duration)` sets the read timeout, e.g. `WithPongWait(60*time.Second)`; each pong renews the deadline before `HandlePong` runs, and non-positive values disable the timeout (default: disabled)
- `WithPingPeriod(time.Duration)` sets the ping interval, e.g. `WithPingPeriod(54*time.Second)` (default: 60 seconds); non-positive values fall back to 54 seconds
- `WithReadMaxPayloadSize(int)` sets the inbound message limit in bytes (default: 64 KiB); non-positive values use the default, and oversized messages close the connection before reaching handlers
- `WithMessageBufferSize(int)` sets the outbound queue size; start with 64-256 and increase only if you hit `ErrBufferFull` under normal bursts
- `WithDispatchAsync(bool)` switches inbound dispatch to goroutine-per-message when enabled
- `WithDeleteRoomOnEmpty(bool)` controls automatic room deletion when the last session leaves (default: `true`)
- `WithProtocol(adler.Protocol)` selects `adler.JSON` (default) or `adler.Protobuf` for protocol-aware messaging

When enabling `PongWait`, set `PingPeriod` below it. For example:

```go
a := adler.New(
    adler.WithWriteWait(10*time.Second),
    adler.WithPongWait(60*time.Second),
    adler.WithPingPeriod(54*time.Second),
    adler.WithReadMaxPayloadSize(64*1024),
)
```

The payload limit applies to complete messages, including fragmented messages.
Applications receiving larger messages can explicitly raise it, for example with
`WithReadMaxPayloadSize(1024*1024)` for 1 MiB. The limit is applied when the upgrader
is created; configure it through constructor options.

## Migrating to v1.5.1

- Timeout options now use standard Go durations. Replace `WithWriteWait(10)`,
  `WithPongWait(60)`, and `WithPingPeriod(54)` with `WithWriteWait(10*time.Second)`,
  `WithPongWait(60*time.Second)`, and `WithPingPeriod(54*time.Second)` respectively.
  Bare integers are interpreted as nanoseconds, not seconds.
- The default inbound payload limit is now 64 KiB instead of the gws default of
  16 MiB. Set `WithReadMaxPayloadSize(...)` explicitly if larger messages are expected.
- `RemoveFromQueue` now returns an error. Check it to detect rejected commands when
  the command buffer is full. Code using its previous function type
  `func(*adler.Session)` must be updated to `func(*adler.Session) error`.

See [CHANGELOG.md](CHANGELOG.md#151) for the complete release notes.

## Notes On Concurrency

- Session storage is protected by an internal mutex.
- Server broadcast methods are safe for normal concurrent use.
- `UnsafeConn()` bypasses Adler's internal coordination; only use it if you manage access carefully.

## Minimal Room Example

```go
room := a.NewRoom("test")

a.HandleConnect(func(s *adler.Session) {
    _ = room.Join(s)
})

room.HandleJoin(func(s *adler.Session) {
    _ = s.WriteText([]byte("joined room"))
})
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for the full text.
