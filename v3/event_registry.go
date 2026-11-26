package vlc

// #cgo LDFLAGS: -lvlc
// #include <vlc/vlc.h>
import "C"
import "sync"

// EventID uniquely identifies a registered event.
type EventID uint64

// EventCallback represents an event notification callback function.
type EventCallback func(Event, interface{})

type internalEventCallback func(*C.libvlc_event_t, interface{})

type eventContext struct {
	event            Event
	externalCallback EventCallback
	internalCallback internalEventCallback
	userData         interface{}

	// gate, when non-nil, filters dispatches of this context by epoch and
	// tracks in-flight user callbacks for safe shutdown. It is nil for event
	// managers that are not player session scoped.
	gate  *eventGate
	epoch sessionEpoch
}

type eventRegistry struct {
	sync.RWMutex

	contexts map[EventID]*eventContext
	sequence EventID
}

func newEventRegistry() *eventRegistry {
	return &eventRegistry{
		contexts: map[EventID]*eventContext{},
	}
}

func (er *eventRegistry) get(id EventID) (*eventContext, bool) {
	if id == 0 {
		return nil, false
	}

	er.RLock()
	ctx, ok := er.contexts[id]
	er.RUnlock()

	return ctx, ok
}

func (er *eventRegistry) add(event Event, externalCallback EventCallback,
	internalCallback internalEventCallback, userData interface{}) EventID {
	return er.addContext(&eventContext{
		event:            event,
		externalCallback: externalCallback,
		internalCallback: internalCallback,
		userData:         userData,
	})
}

// addScoped registers a context whose dispatch is filtered by the provided
// session gate and epoch.
func (er *eventRegistry) addScoped(event Event, externalCallback EventCallback,
	internalCallback internalEventCallback, userData interface{},
	gate *eventGate, epoch sessionEpoch) EventID {
	return er.addContext(&eventContext{
		event:            event,
		externalCallback: externalCallback,
		internalCallback: internalCallback,
		userData:         userData,
		gate:             gate,
		epoch:            epoch,
	})
}

func (er *eventRegistry) addContext(ctx *eventContext) EventID {
	er.Lock()

	er.sequence++
	id := er.sequence
	er.contexts[id] = ctx

	er.Unlock()
	return id
}

// take atomically removes and returns the context for id. It returns ok=false
// when the id is unknown or has already been taken. This makes concurrent or
// repeated Detach calls perform the native detach exactly once.
func (er *eventRegistry) take(id EventID) (*eventContext, bool) {
	if id == 0 {
		return nil, false
	}

	er.Lock()
	ctx, ok := er.contexts[id]
	if ok {
		delete(er.contexts, id)
	}
	er.Unlock()

	return ctx, ok
}

func (er *eventRegistry) remove(id EventID) {
	if id == 0 {
		return
	}

	er.Lock()
	delete(er.contexts, id)
	er.Unlock()
}
