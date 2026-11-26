package matchmaking

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/catalinfl/adler"
	"github.com/google/uuid"
)

const (
	sessionKeyQueueStatus = "__matchmaking_queue_status"
	sessionKeyRoomID      = "__matchmaking_room_id"

	queueStatusLeft    = "left"
	queueStatusQueued  = "queued"
	queueStatusWaiting = "waiting"
	queueStatusPlaying = "playing"
)

var (
	// ErrSessionNil is returned when a queue operation receives a nil session.
	//
	// The matchmaker cannot enqueue or remove a nil session because the worker
	// needs a stable pointer to track the session in its internal maps.
	ErrSessionNil = errors.New("session is nil")
	// ErrMatchmakerBusy means AddToQueue or RemoveFromQueue could not enqueue
	// a command because the shared command buffer is full. The command was not accepted.
	ErrMatchmakerBusy = errors.New("matchmaker busy")
)

type matchmakingCommandKind int8

const (
	matchmakingCommandAdd matchmakingCommandKind = iota
	matchmakingCommandRemove
)

type queueItem struct {
	session    *adler.Session
	enqueuedAt time.Time
}

type matchmakingCommand struct {
	kind    matchmakingCommandKind
	session *adler.Session
}

// Matchmaker owns the matchmaking queues and turns sessions into rooms.
//
// Public methods do not modify the queues directly. They send commands to an
// internal worker goroutine, which serializes all queue mutations. That design
// keeps queue state consistent while still letting the rest of the application
// call AddToQueue and RemoveFromQueue from request handlers, connect hooks, or
// disconnect hooks.
//
// Disconnects are cleaned up automatically: NewMatchmaker registers a disconnect
// hook on the Adler server that removes the session from the main and waiting
// queues. If the shared command buffer is full, the removal is kept in an
// internal retry set and applied by the worker, so a dropped command cannot
// leave a closed session behind in a queue.
type Matchmaker struct {
	commands           chan matchmakingCommand
	waitQueue          *list.List
	queue              *list.List
	inWaitQueueMap     map[*adler.Session]*list.Element
	inQueueMap         map[*adler.Session]*list.Element
	adler              *adler.Adler
	maxQueue           int
	roomSize           int // Number of players per room
	minRoomSize        int // Minimum number of players until the game should start
	partialRoomTimeout time.Duration

	// retryMu guards pendingRemovals, which holds sessions whose removal could
	// not be sent through the full command buffer.
	retryMu         sync.Mutex
	pendingRemovals map[*adler.Session]struct{}
	retrySignal     chan struct{}
}

func (m *Matchmaker) run() {
	var tick <-chan time.Time
	var ticker *time.Ticker

	if m.partialRoomTimeout > 0 {
		ticker = time.NewTicker(matchmakingTickInterval(m.partialRoomTimeout))
		tick = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case cmd := <-m.commands:
			now := time.Now()
			switch cmd.kind {
			case matchmakingCommandAdd:
				m.handleAddToQueue(cmd.session, now)
			case matchmakingCommandRemove:
				m.handleRemoveFromQueue(cmd.session, now)
			}

		case <-m.retrySignal:
			m.drainPendingRemovals()

		case now := <-tick:
			m.processQueueTransitions(now)
		}
	}
}

// scheduleRemovalRetry records a session whose removal command could not be
// queued and asks the worker to apply it directly. The worker owns queue state,
// so retrying there is always possible and cannot be rejected by backpressure.
func (m *Matchmaker) scheduleRemovalRetry(s *adler.Session) {
	m.retryMu.Lock()
	if m.pendingRemovals == nil {
		m.pendingRemovals = make(map[*adler.Session]struct{})
	}
	m.pendingRemovals[s] = struct{}{}
	m.retryMu.Unlock()

	select {
	case m.retrySignal <- struct{}{}:
	default:
	}
}

// drainPendingRemovals applies every deferred removal on the worker goroutine.
func (m *Matchmaker) drainPendingRemovals() {
	for {
		m.retryMu.Lock()
		if len(m.pendingRemovals) == 0 {
			m.retryMu.Unlock()
			return
		}
		batch := m.pendingRemovals
		m.pendingRemovals = nil
		m.retryMu.Unlock()

		now := time.Now()
		for s := range batch {
			m.handleRemoveFromQueue(s, now)
		}
	}
}

// handleDisconnect removes a disconnected session from whichever queue holds it.
// It is registered as an Adler disconnect hook, so cleanup also happens when the
// application does not call RemoveFromQueue itself.
func (m *Matchmaker) handleDisconnect(s *adler.Session) {
	if s == nil {
		return
	}

	if err := m.RemoveFromQueue(s); err != nil {
		if errors.Is(err, ErrMatchmakerBusy) {
			m.scheduleRemovalRetry(s)
		}
	}
}

// AddToQueue asks the matchmaker to place a session into the active queue.
//
// The call is non-blocking. It only enqueues a command for the worker goroutine
// and returns ErrMatchmakerBusy if the command buffer is full. When the worker
// eventually processes the request, it either places the session into the main
// queue, moves it to the waiting queue, or leaves it alone if the session is
// already marked as playing in a room.
func (m *Matchmaker) AddToQueue(s *adler.Session) error {
	if s == nil {
		return ErrSessionNil
	}
	select {
	case m.commands <- matchmakingCommand{
		kind:    matchmakingCommandAdd,
		session: s,
	}:
		return nil
	default:
		return ErrMatchmakerBusy
	}
}

// RemoveFromQueue enqueues removal from the main or waiting queue without blocking.
// A nil error confirms command acceptance, not completion of removal.
// It returns ErrSessionNil for a nil session or ErrMatchmakerBusy when the shared
// command buffer is full. In the latter case, no removal has been scheduled;
// callers must handle or retry the failed cleanup.
func (m *Matchmaker) RemoveFromQueue(s *adler.Session) error {
	if s == nil {
		return ErrSessionNil
	}

	select {
	case m.commands <- matchmakingCommand{
		kind:    matchmakingCommandRemove,
		session: s,
	}:
		return nil
	default:
		return ErrMatchmakerBusy
	}
}

func (m *Matchmaker) handleAddToQueue(s *adler.Session, now time.Time) {
	if s == nil {
		return
	}

	// A session that already disconnected must never re-enter a queue. Its
	// removal may have been deferred behind a full command buffer, in which
	// case this add command can still be sitting in front of the cleanup.
	if s.IsClosed() {
		m.markLeftIfQueued(s)
		return
	}

	queueStatus, ok := s.GetString(sessionKeyQueueStatus)
	if ok && queueStatus == queueStatusPlaying {
		return
	}

	if _, exists := m.inQueueMap[s]; exists {
		msg := &QueueStatus{
			Payload: &QueueStatus_QueueError{
				QueueError: &QueueError{
					Message: "You are already in a queue",
				},
			},
		}
		s.Write(msg)
		return
	}

	if _, exists := m.inWaitQueueMap[s]; exists {
		msg := &QueueStatus{
			Payload: &QueueStatus_QueueError{
				QueueError: &QueueError{
					Message: "You are already in a queue",
				},
			},
		}
		s.Write(msg)
		return
	}

	// A closed session may still occupy a main-queue slot when its removal was
	// deferred behind a full command buffer. Prune before the capacity check so
	// a live session is not sent to the waiting queue because of a dead entry.
	if m.maxQueue > 0 && m.queue.Len() >= m.maxQueue {
		m.pruneInvalidQueueEntries()
	}

	if m.maxQueue > 0 && m.queue.Len() >= m.maxQueue {
		m.addToWaitingQueue(s, now)
		m.processQueueTransitions(now)
	} else {
		m.addToMainQueue(s, now)
		m.processQueueTransitions(now)
	}
}

func (m *Matchmaker) addToMainQueue(s *adler.Session, now time.Time) {
	item := &queueItem{
		session:    s,
		enqueuedAt: now,
	}

	element := m.queue.PushBack(item)
	m.inQueueMap[s] = element
	delete(m.inWaitQueueMap, s)
	s.Set(sessionKeyQueueStatus, queueStatusQueued)
	msg := &QueueStatus{
		Payload: &QueueStatus_QueueJoined{
			QueueJoined: &QueueJoined{
				Message: "You have joined the main queue",
			},
		},
	}
	_ = s.Write(msg)
}

func (m *Matchmaker) addToWaitingQueue(s *adler.Session, now time.Time) {
	item := &queueItem{
		session:    s,
		enqueuedAt: now,
	}

	element := m.waitQueue.PushBack(item)
	m.inWaitQueueMap[s] = element
	s.Set(sessionKeyQueueStatus, queueStatusWaiting)
	msg := &QueueStatus{
		Payload: &QueueStatus_WaitQueueJoined{
			WaitQueueJoined: &WaitQueueJoined{
				Message: "The main queue is full. You have joined the waiting queue.",
			},
		},
	}
	_ = s.Write(msg)
}

func (m *Matchmaker) processQueueTransitions(now time.Time) {
	m.pruneInvalidQueueEntries()
	m.createFullRooms()
	m.promoteFromWaitingQueue(now)
	m.createFullRooms()
	m.createPartialRoomIfAllowed(now)
}

// pruneInvalidQueueEntries drops sessions that can no longer be matched:
// closed connections and list entries whose index no longer points back at them.
// Removing an element from a linked list preserves the relative order of the
// remaining sessions in both queues. Both indexes are rebuilt from the surviving
// elements so the map, the list and the session status stay consistent even if
// an entry was ever duplicated or replaced.
func (m *Matchmaker) pruneInvalidQueueEntries() {
	if !m.hasInvalidQueueEntries() {
		return
	}

	var removed []*adler.Session

	for element := m.queue.Front(); element != nil; {
		next := element.Next()
		item, ok := element.Value.(*queueItem)
		if !ok || item == nil || item.session == nil || item.session.IsClosed() || m.inQueueMap[item.session] != element {
			m.queue.Remove(element)
			if ok && item != nil && item.session != nil {
				removed = append(removed, item.session)
			}
		}
		element = next
	}

	for element := m.waitQueue.Front(); element != nil; {
		next := element.Next()
		item, ok := element.Value.(*queueItem)
		if !ok || item == nil || item.session == nil || item.session.IsClosed() || m.inWaitQueueMap[item.session] != element {
			m.waitQueue.Remove(element)
			if ok && item != nil && item.session != nil {
				removed = append(removed, item.session)
			}
		}
		element = next
	}

	m.rebuildQueueIndexes()

	// Run after rebuilding so a session still tracked by the other queue keeps
	// its status while fully removed sessions are marked as left.
	for _, s := range removed {
		m.markLeftIfQueued(s)
	}
}

// hasInvalidQueueEntries reports whether a prune pass would remove anything.
// It lets the hot add path skip the index rebuild when both queues are clean.
func (m *Matchmaker) hasInvalidQueueEntries() bool {
	for element := m.queue.Front(); element != nil; element = element.Next() {
		item, ok := element.Value.(*queueItem)
		if !ok || item == nil || item.session == nil || item.session.IsClosed() || m.inQueueMap[item.session] != element {
			return true
		}
	}

	for element := m.waitQueue.Front(); element != nil; element = element.Next() {
		item, ok := element.Value.(*queueItem)
		if !ok || item == nil || item.session == nil || item.session.IsClosed() || m.inWaitQueueMap[item.session] != element {
			return true
		}
	}

	return false
}

// rebuildQueueIndexes re-creates both session-to-element maps from the lists.
func (m *Matchmaker) rebuildQueueIndexes() {
	mainIndex := make(map[*adler.Session]*list.Element, m.queue.Len())
	for element := m.queue.Front(); element != nil; element = element.Next() {
		item, ok := element.Value.(*queueItem)
		if !ok || item == nil || item.session == nil {
			continue
		}
		mainIndex[item.session] = element
	}
	m.inQueueMap = mainIndex

	waitIndex := make(map[*adler.Session]*list.Element, m.waitQueue.Len())
	for element := m.waitQueue.Front(); element != nil; element = element.Next() {
		item, ok := element.Value.(*queueItem)
		if !ok || item == nil || item.session == nil {
			continue
		}
		waitIndex[item.session] = element
	}
	m.inWaitQueueMap = waitIndex
}

// markLeftIfQueued clears a stale queue status once the session is no longer
// tracked by any queue. Sessions already playing in a room keep their status.
func (m *Matchmaker) markLeftIfQueued(s *adler.Session) {
	if s == nil {
		return
	}

	if _, exists := m.inQueueMap[s]; exists {
		return
	}
	if _, exists := m.inWaitQueueMap[s]; exists {
		return
	}

	status, ok := s.GetString(sessionKeyQueueStatus)
	if !ok || status == queueStatusQueued || status == queueStatusWaiting {
		s.Set(sessionKeyQueueStatus, queueStatusLeft)
	}
}

func (m *Matchmaker) createFullRooms() {
	for m.queue.Len() >= m.roomSize {
		if !m.createRoomFromQueue(m.roomSize, m.roomSize) {
			return
		}
	}
}

func (m *Matchmaker) createPartialRoomIfAllowed(now time.Time) {
	if m.queue.Len() < m.minRoomSize {
		return
	}

	if m.partialRoomTimeout > 0 {
		front := m.queue.Front()
		if front == nil {
			return
		}

		item, ok := front.Value.(*queueItem)
		if !ok || item == nil {
			return
		}
		if now.Sub(item.enqueuedAt) < m.partialRoomTimeout { // must stay at least partialRoomTimeout time
			return
		}
	}

	size := min(m.queue.Len(), m.roomSize)
	m.createRoomFromQueue(size, m.minRoomSize)
}

// createRoomFromQueue removes up to size eligible sessions from the front of the
// main queue and groups them into a new room.
//
// Only sessions that are still tracked by the queue and still connected are
// eligible; stale entries are discarded as they are encountered. If fewer than
// minPlayers sessions can be joined, the survivors are restored to the front of
// the queue in their original order and no room is kept. The match_found
// notification reports the actual number of room members, which always equals
// the room size at broadcast time.
func (m *Matchmaker) createRoomFromQueue(size, minPlayers int) bool {
	players := make([]*adler.Session, 0, size)
	items := make([]*queueItem, 0, size)

	for len(players) < size {
		front := m.queue.Front()
		if front == nil {
			break
		}

		item, ok := front.Value.(*queueItem)
		if !ok || item == nil || item.session == nil || item.session.IsClosed() || m.inQueueMap[item.session] != front {
			m.queue.Remove(front)
			if ok && item != nil && item.session != nil {
				if current, exists := m.inQueueMap[item.session]; exists && current == front {
					delete(m.inQueueMap, item.session)
				}
				m.markLeftIfQueued(item.session)
			}
			continue
		}

		m.queue.Remove(front)
		delete(m.inQueueMap, item.session)

		players = append(players, item.session)
		items = append(items, item)
	}

	if len(players) == 0 || len(players) < minPlayers {
		m.restoreToQueueFront(items)
		return false
	}

	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New() // CAREFUL! Generating UUIDv4 instead of UUIDv7 could cause issues
	}

	roomID := "room_" + id.String()
	room := m.adler.NewRoom(roomID)

	joined := make([]*adler.Session, 0, len(players))
	joinedItems := make([]*queueItem, 0, len(players))
	for i, player := range players {
		// Re-check right before joining: a session may have disconnected after
		// the eligibility scan. Such a session must not occupy a room slot.
		if player.IsClosed() || room.Join(player) != nil {
			m.markLeftIfQueued(player)
			continue
		}

		player.Set(sessionKeyQueueStatus, queueStatusPlaying)
		player.Set(sessionKeyRoomID, roomID)
		joined = append(joined, player)
		joinedItems = append(joinedItems, items[i])
	}

	// Final consistency pass: a session can disconnect while the remaining
	// players are joined. Drop those from the room so the member count equals
	// the count reported in match_found.
	live := joined[:0]
	liveItems := joinedItems[:0]
	for i, player := range joined {
		if player.IsClosed() {
			room.Leave(player)
			player.Unset(sessionKeyRoomID)
			m.markLeftIfQueued(player)
			continue
		}

		live = append(live, player)
		liveItems = append(liveItems, joinedItems[i])
	}

	if len(live) == 0 || len(live) < minPlayers {
		// Too few live sessions survived the join phase: roll the room back and
		// requeue the survivors so they can be matched with later arrivals.
		for _, player := range live {
			room.Leave(player)
			player.Unset(sessionKeyRoomID)
		}
		_ = m.adler.DeleteRoom(roomID)
		m.restoreToQueueFront(liveItems)
		return false
	}

	msg := &QueueStatus{
		Payload: &QueueStatus_MatchFound{
			MatchFound: &MatchFound{
				RoomId:  roomID,
				Players: int32(len(live)),
			},
		},
	}
	room.BroadcastAny(msg)
	return true
}

// restoreToQueueFront puts collected queue items back at the front of the main
// queue, preserving their original relative order, and marks them as queued.
func (m *Matchmaker) restoreToQueueFront(items []*queueItem) {
	for i := len(items) - 1; i >= 0; i-- {
		item := items[i]
		if item == nil || item.session == nil {
			continue
		}
		if item.session.IsClosed() {
			m.markLeftIfQueued(item.session)
			continue
		}

		element := m.queue.PushFront(item)
		m.inQueueMap[item.session] = element
		item.session.Set(sessionKeyQueueStatus, queueStatusQueued)
	}
}

func (m *Matchmaker) promoteFromWaitingQueue(now time.Time) {
	for m.waitQueue.Len() > 0 && (m.maxQueue == 0 || m.queue.Len() < m.maxQueue) {
		front := m.waitQueue.Front()
		if front == nil {
			break
		}

		item, ok := front.Value.(*queueItem)
		m.waitQueue.Remove(front)
		if !ok || item == nil || item.session == nil {
			continue
		}

		session := item.session
		if m.inWaitQueueMap[session] != front {
			// Stale entry: the index already points at a newer element.
			continue
		}
		delete(m.inWaitQueueMap, session)

		// A session that disconnected while waiting must not be promoted and
		// must not consume a main-queue slot; the loop moves on to the next one.
		if session.IsClosed() {
			m.markLeftIfQueued(session)
			continue
		}

		// Never duplicate a session that is already tracked by the main queue.
		// The main queue is authoritative, so restore its status if the stale
		// waiting entry had overwritten it.
		if _, exists := m.inQueueMap[session]; exists {
			session.Set(sessionKeyQueueStatus, queueStatusQueued)
			continue
		}

		newItem := &queueItem{
			session:    session,
			enqueuedAt: now, // changing the current enqueue time from waiting queue to normal queue
		}

		element := m.queue.PushBack(newItem)
		m.inQueueMap[session] = element

		session.Set(sessionKeyQueueStatus, queueStatusQueued)

		msg := &QueueStatus{
			Payload: &QueueStatus_PromotedToQueue{
				PromotedToQueue: &PromotedToQueue{
					Message: "A spot opened up. You are now in the main queue",
				},
			},
		}
		_ = session.Write(msg)
	}
}

func (m *Matchmaker) handleRemoveFromQueue(s *adler.Session, now time.Time) {
	if s == nil {
		return
	}

	removedFromMain := false

	if element, exists := m.inQueueMap[s]; exists {
		m.queue.Remove(element)
		delete(m.inQueueMap, s)
		removedFromMain = true
	}

	if element, exists := m.inWaitQueueMap[s]; exists {
		m.waitQueue.Remove(element)
		delete(m.inWaitQueueMap, s)
	}

	// A session that is no longer tracked by any queue must not stay marked as
	// queued or waiting. Playing sessions keep their room status.
	m.markLeftIfQueued(s)

	if removedFromMain {
		m.processQueueTransitions(now)
	}
}

func clampTime(v, min, max time.Duration) time.Duration {
	if v <= min {
		return min
	}

	if v >= max {
		return max
	}

	return v
}

func matchmakingTickInterval(timeout time.Duration) time.Duration {
	return clampTime(timeout, 10*time.Millisecond, 1*time.Second)
}
