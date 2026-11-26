package vlc

/*
#cgo LDFLAGS: -lvlc
#include <vlc/vlc.h>

typedef const libvlc_event_t constev;
extern void eventDispatch(constev*, void*);

static inline int eventAttach(libvlc_event_manager_t* em, libvlc_event_type_t et, unsigned long userData) {
    return libvlc_event_attach(em, et, eventDispatch, (void*)userData);
}
static inline int eventDetach(libvlc_event_manager_t* em, libvlc_event_type_t et, unsigned long userData) {
    libvlc_event_detach(em, et, eventDispatch, (void*)userData);
}
*/
import "C"

import (
	"unsafe"
)

// EventManager wraps a libvlc event manager.
type EventManager struct {
	manager *C.libvlc_event_manager_t

	// gate optionally scopes attachments to a player media session. When
	// non-nil, Attach stamps the current session epoch and eventDispatch
	// rejects events from stale sessions and after release. It is nil for
	// media, media list and other non-player event managers.
	gate *eventGate
}

// newEventManager returns a new event manager instance.
func newEventManager(manager *C.libvlc_event_manager_t) *EventManager {
	return &EventManager{
		manager: manager,
	}
}

// newScopedEventManager returns an event manager whose attachments are gated
// by the provided player session gate.
func newScopedEventManager(manager *C.libvlc_event_manager_t, gate *eventGate) *EventManager {
	return &EventManager{
		manager: manager,
		gate:    gate,
	}
}

// Attach registers a callback for an event notification.
func (em *EventManager) Attach(event Event, callback EventCallback, userData interface{}) (EventID, error) {
	return em.attach(event, callback, nil, userData)
}

// attach registers callbacks for an event notification.
func (em *EventManager) attach(event Event, externalCallback EventCallback,
	internalCallback internalEventCallback, userData interface{}) (EventID, error) {
	if err := inst.assertInit(); err != nil {
		return 0, err
	}
	if externalCallback == nil && internalCallback == nil {
		return 0, ErrInvalidEventCallback
	}

	var epoch sessionEpoch
	if em.gate != nil {
		epoch = em.gate.stamp()
	}

	id := inst.events.addScoped(event, externalCallback, internalCallback, userData, em.gate, epoch)
	if em.manager == nil || C.eventAttach(em.manager, C.libvlc_event_type_t(event), C.ulong(id)) != 0 {
		inst.events.remove(id)
		return 0, errOrDefault(getError(), ErrEventAttach)
	}

	if em.gate != nil {
		em.gate.track(id)
	}

	return id, nil
}

// Detach unregisters the specified event notification.
//
// Detach is idempotent: detaching an EventID that was never registered or was
// already detached (including calling Detach more than once with the same ID)
// is a safe no-op. Concurrent calls for the same ID perform the native detach
// exactly once.
func (em *EventManager) Detach(eventIDs ...EventID) {
	if err := inst.assertInit(); err != nil {
		return
	}

	for _, eventID := range eventIDs {
		// Atomically claim the context. Only the winning caller performs the
		// native detach, so repeated/concurrent Detach calls cannot invoke
		// libvlc_event_detach twice for the same registration.
		ctx, ok := inst.events.take(eventID)
		if !ok {
			continue
		}

		if ctx.gate != nil {
			ctx.gate.untrack(eventID)
		}

		if em != nil && em.manager != nil {
			C.eventDetach(em.manager, C.libvlc_event_type_t(ctx.event), C.ulong(eventID))
		}
	}
}

//export eventDispatch
func eventDispatch(event *C.constev, userData unsafe.Pointer) {
	dispatchEventByID(EventID(uintptr(userData)), event)
}

// dispatchEventByID contains the dispatch logic keyed by the parsed
// subscription ID. Splitting it from the cgo trampoline keeps the session
// filtering and re-entrancy behaviour directly testable without synthesising
// an invalid void pointer in tests.
func dispatchEventByID(eventID EventID, event *C.constev) {
	if err := inst.assertInit(); err != nil {
		return
	}

	ctx, ok := inst.events.get(eventID)
	if !ok {
		return
	}

	// Session/lifetime gate. Stale media sessions and released players do not
	// deliver user callbacks. enter also tracks in-flight dispatches so
	// Release can drain them before releasing native resources.
	var done func()
	if ctx.gate != nil {
		var allowed bool
		done, allowed = ctx.gate.enter(ctx.epoch)
		if !allowed {
			return
		}
		defer done()
	}

	// Execute external callback.
	if ctx.externalCallback != nil {
		ctx.externalCallback(ctx.event, ctx.userData)
	}

	// Execute internal callback.
	if ctx.internalCallback != nil {
		ctx.internalCallback(event, ctx.userData)
	}
}
