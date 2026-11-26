# Changelog

All notable changes for this project are documented here.

## Roadmap

- [x] Replace gobwas/ws with gws for ~2x throughput improvement and lower memory footprint
- [ ] Implement matchmaking based on ELO ranking, including configurable ranking buckets and match quality rules.

## 1.6.0

### Added

- `Session.CloseWithCode(uint16, string)` to queue a WebSocket close frame with a status code and reason, with forced disconnection after two seconds if the peer does not finish the handshake.
- UTF-8-safe truncation of close reasons to the RFC 6455 limit of 123 bytes.
- Regression tests for room closure during joins, concurrent empty-room cleanup, binary broadcasts excluding the sender, close-frame payloads, and unresponsive peers.

### Fixed

- Coordinate empty-room deletion and session joins under the room lock so cleanup cannot remove a room after a successful join.
- Recheck room closure after acquiring join locks and release the session lock when rejecting a join.
- Send binary frames, rather than text frames, from `Room.BroadcastBinaryOther` while still excluding the sender.

### Documentation

- Document server-initiated close codes, reason limits, the handshake timeout, and the existing outbound queue behavior.

## 1.5.1

### Fixed

- Renew the read deadline on every pong when `PongWait` is enabled, keeping responsive connections alive and invoking `HandlePong` after renewal.
- Initialize the read deadline before the first read and report deadline errors through `HandleError`.
- Apply the existing room leave/cleanup logic on disconnect, removing empty rooms when `DeleteRoomOnEmpty` is enabled while preserving leave callbacks.
- Make `Matchmaker.RemoveFromQueue` non-blocking when the command buffer is full, avoiding stalled callers and worker callback deadlocks.
- Normalize non-positive inbound payload limits to the Adler default rather than inheriting the larger gws default.

### Added

- `WithReadMaxPayloadSize(int)` and `Config.ReadMaxPayloadSize` to configure the maximum incoming message payload in bytes.
- Regression tests for pong deadline renewal and callbacks, disconnect room cleanup, payload limits, duration options, and saturated matchmaker commands.

### Changed

- `WithWriteWait`, `WithPongWait`, and `WithPingPeriod` now accept standard Go durations without multiplying them by `time.Second`.
- The default inbound message limit is 64 KiB instead of the gws default of 16 MiB.
- `RemoveFromQueue` now returns `ErrSessionNil` for nil sessions and `ErrMatchmakerBusy` when its command cannot be accepted. A nil error confirms enqueueing, not completed removal.

### Documentation

- Update API comments and the chat example for duration-based configuration.
- Document heartbeat configuration, incoming payload limits, disconnect room cleanup, and the shared matchmaker command buffer.
- Correct matchmaking JSON documentation and integration assertions to match the existing generated `Payload` envelope.

### Migration Notes

- These corrections change public behavior: replace seconds-style calls such as `WithWriteWait(10)` with `WithWriteWait(10*time.Second)`. Bare integer durations now represent nanoseconds. Apply the same change to pong timeouts and ping intervals.
- Applications receiving messages larger than 64 KiB must explicitly configure `WithReadMaxPayloadSize(...)`.
- Handle errors returned by `RemoveFromQueue`. A rejected removal is not scheduled; guaranteed cleanup requires a later retry. Update stored method/function types from `func(*adler.Session)` to `func(*adler.Session) error` where applicable.

## 1.5.0

### Changed

- **WebSocket engine replaced**: `github.com/gobwas/ws` v1.4.0 → `github.com/lxzan/gws` v1.10.0
  - Up to 2x message throughput improvement (echo benchmark: 7.8M vs 3.9M reads/s)
  - Zero-alloc reads (0 allocs/op via `ReadMessage`)
  - Lower per-connection memory footprint (aggressive buffer reuse)
  - Full RFC 6455 / RFC 7692 compliance (passes all Autobahn tests)

### What changed under the hood

- Session connection type changed from `net.Conn` to `*gws.Conn`
- Frame-level reads replaced with `gws.Conn.ReadMessage()` — pings, pongs, and close frames are now handled by the new `Event`-based bridge (`adlerEventHandler`)
- Frame-level writes replaced with `gws.Conn.WriteMessage()` / `WritePing()`
- HTTP upgrade replaced with `gws.Upgrader.Upgrade()`
- Removed internal reader (`wsutil.NewReader`), frame parser (`NextFrame`), and close-payload parser (`parseClosePayload`)
- All opcodes (`ws.OpCode`) replaced with `gws.Opcode` equivalents
- Test suite migrated from `ws.Dial` + `wsutil.ReadServerData` to channel-based gws client wrappers (background `ReadLoop` with message channels)

## 1.4.1

### Added

- `Room.BroadcastBinaryFilter` sends a binary websocket message to room sessions matching a filter function
- `Room.BroadcastBinaryOther` sends a binary websocket message to all room sessions except the sender

## 1.4.0

### Added

- `None` protocol option via `WithProtocol(None)` to disable serialization entirely
- Nil-safety guards in `Session.Write`, `Room.BroadcastAny`, and `Adler.BroadcastAny` when serializer is `nil`

### Changed

- Default protocol fallback changed from `JSON` to `nil` (no serializer) for unrecognized protocol values
- Matchmaker protobuf types (`QueueStatus`, `QueueError`, etc.) moved from nested import path into the `matchmaker` package directly, eliminating the `matchmaker/github.com/catalinfl/adler/matchmaking/pb` intermediate path
- Removed standalone `matchmaking_json.go` — JSON marshaling for `QueueStatus` is no longer needed since the serializer layer handles encoding

### Fixed

- Fixed import path for protobuf generated code inside the matchmaker module

## 1.3.0

### Added

- Protocol selection via `WithProtocol` and `Protocol` constants (`JSON`, `Protobuf`)
- Serializer layer with JSON and Protobuf implementations
- Protocol-aware helpers: `Session.Write`, `Adler.BroadcastAny`, `Room.BroadcastAny`
- Protobuf queue event schema and generated package (`matchmaker/matchmaking.proto`)
- JSON marshaling for `QueueStatus` to preserve existing JSON event shapes
- Protobuf validation errors: `ErrProtobufValue`, `ErrProtobufTarget`

### Changed

- Matchmaker queue notifications now use protobuf `QueueStatus` messages routed through the serializer

### Testing

- Added real WebSocket integration tests for matchmaking room creation and queue events

### Documentation

- Documented protocol configuration and protocol-aware messaging helpers

## 1.2.0

### Added

- **Dedicated Matchmaker Module**: New `matchmaker` module for queue creation, queue management, and match orchestration
- **Channel-Based Matchmaking**: Implemented non-blocking matchmaking system using Go channels for improved concurrency
- **Notification System**: Legacy notification system for matchmaking events and queue updates
- **UUID Support**: Direct UUID support for session identification and management
- **Multiple Room Matchmaking**: Support for matchmaking across multiple rooms with proper test coverage
- **Custom Configuration**: Separated matchmaker configuration into dedicated file

### Changed

- Matchmaking logic refactored for better performance and maintainability
- State management in matchmaking now non-blocking
- Improved notification delivery through channel-based system

### Fixed

- Session variable capture in promotion queue (closure bug fix)
- Partial room creation when `minRoomSize=0` and `partialRoomTimeout=0`

### Documentation

- Improved documentation for normal matchmaking flows

## 1.1.0

### Added

- Room lifecycle configuration option: `WithDeleteRoomOnEmpty(bool)` (default `true`)
- Manual room removal API: `Adler.DeleteRoom(name)`
- New error for manual delete safety: `ErrRoomNotEmpty`

### Changed

- `PongWait` default is now `0` (idle timeout disabled by default)
- `PingPeriod` default is now `60` seconds
- Time options are interpreted as seconds in option helpers:
  - `WithWriteWait(x)` -> `x` seconds
  - `WithPongWait(x)` -> `x` seconds
  - `WithPingPeriod(x)` -> `x` seconds
- Room lookup now uses read locking in `Adler.Room(...)`

### Fixed

- Fixed room cleanup behavior to respect `WithDeleteRoomOnEmpty(false)` and keep empty rooms when configured

### Migration Notes

- If you need idle disconnects, set `WithPongWait(...)` explicitly to a value greater than `0`.
- If you relied on duration-style calls like `WithPingPeriod(30*time.Second)`, update to seconds-style calls like `WithPingPeriod(30)`.

## 1.0.0

### Added

- WebSocket upgrade and session lifecycle management through `Adler.HandleRequest`
- Session hooks: `HandleConnect`, `HandleDisconnect`, `HandleMessage`, `HandleMessageBinary`, `HandlePong`, `HandleClose`, `HandleError`
- Sent-message hooks: `HandleSentMessage` and `HandleSentMessageBinary`
- Room lifecycle hooks: `OnRoomJoin` and `OnRoomLeave`
- Session message writers for text, binary, JSON, and deadline-aware variants
- Session store helpers:
  - `Set`, `SetNX`, `Get`, `GetString`, `GetInt`, `GetInt64`, `GetFloat`, `GetBool`
  - `Has`, `Unset`, `Keys`, `Values`, `Clear`
  - `Incr`, `Decr`
- Session metadata accessors:
  - `Request`, `Protocol`, `LocalAddr`, `RemoteAddr`, `Room`, `Identity`
  - `UnsafeConn` for low-level access when needed
- Global broadcast helpers:
  - `Broadcast`, `BroadcastFilter`
  - `BroadcastBinary`, `BroadcastBinaryFilter`
  - `BroadcastJSON`, `BroadcastJSONFilter`
  - `BroadcastOthers`, `SendTo`
- Room API:
  - `NewRoom`, `Name`, `Len`, `Sessions`
  - `Join`, `Leave`, `OpenRoom`, `CloseRoom`
  - `Broadcast`, `BroadcastBinary`, `BroadcastFilter`, `BroadcastJSON`, `BroadcastJSONFilter`
- Runtime options:
  - `WithWriteWait`
  - `WithPongWait`
  - `WithPingPeriod`
  - `WithMessageBufferSize`
  - `WithDispatchAsync`
