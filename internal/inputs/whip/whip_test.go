package whip

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Glimesh/waveguide/pkg/types"
	"github.com/pion/webrtc/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return string(body)
}

func TestWhipRejectsUnauthenticatedRequest(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 1

	// No Authorization header at all.
	req, err := http.NewRequest(http.MethodPost, h.endpoint(channelID), strings.NewReader(validOfferSDP(t)))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Wrong stream key. A non-dashed value is treated as the key directly;
	// dashed values are parsed as channel-key pairs and rejected earlier.
	req, err = http.NewRequest(http.MethodPost, h.endpoint(channelID), strings.NewReader(validOfferSDP(t)))
	require.NoError(t, err)
	req.Header.Set("Authorization", "definitelywrongkey")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// A failed authentication must not create a session, a stream, or any
	// external start request, and must not change channel state.
	assert.Equal(t, 0, h.source.activeSessionCount())
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 0, h.svc.starts())
	assert.Equal(t, 0, h.svc.ends())
}

func TestWhipRejectsIllegalChannelAndEmptyBody(t *testing.T) {
	h := newWhipHarness(t)

	// Illegal channel number: never reaches authentication or state.
	req, err := http.NewRequest(http.MethodPost, h.server.URL+"/whip/endpoint/not-a-number", strings.NewReader("v=0"))
	require.NoError(t, err)
	req.Header.Set("Authorization", "whatever")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Empty body on an otherwise valid request.
	req, err = http.NewRequest(http.MethodPost, h.endpoint(2), strings.NewReader(""))
	require.NoError(t, err)
	req.Header.Set("Authorization", streamKeyFor(2))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, 0, h.source.activeSessionCount())
	assert.False(t, h.ctrl.HasStream(types.ChannelID(2)))
	assert.Equal(t, 0, h.svc.starts(), "no external start for invalid requests")
}

func TestWhipPublishAndDeleteCycle(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 10

	resp := h.publish(t, channelID)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, "application/sdp", resp.Header.Get("Content-Type"))
	assert.NotEmpty(t, resp.Header.Get("Expire"))
	answer := readBody(t, resp)
	assert.Contains(t, answer, "v=0")

	assert.True(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	require.Eventually(t, func() bool { return h.source.activeSessionCount() == 1 }, time.Second, 10*time.Millisecond)
	assert.Equal(t, 1, h.svc.starts())

	// Tracks registered for the channel keep the WHEP playback path working.
	tracks, err := h.ctrl.GetTracks(types.ChannelID(channelID))
	require.NoError(t, err)
	assert.Len(t, tracks, 2)

	// Client DELETE ends exactly this session.
	resp = h.delete(channelID, streamKeyFor(channelID))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "session removed after DELETE")
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)), "channel returns to not-live")
	assert.Equal(t, 1, h.svc.ends(), "external stop request sent exactly once")
	assert.Equal(t, 1, h.or.stops())

	// A second DELETE is idempotent and sends no extra stop request.
	resp = h.delete(channelID, streamKeyFor(channelID))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 1, h.svc.ends())
	assert.Equal(t, 1, h.or.stops())

	// The same channel can publish again after going idle.
	resp = h.publish(t, channelID)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.True(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	require.Eventually(t, func() bool { return h.source.activeSessionCount() == 1 }, time.Second, 10*time.Millisecond)
	assert.Equal(t, 2, h.svc.starts())

	resp = h.delete(channelID, streamKeyFor(channelID))
	require.NoError(t, resp.Body.Close())
	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "cleanup after second DELETE")
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 2, h.svc.ends())
}

func TestWhipConcurrentPublishOnlyOneWins(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 20

	const attempts = 8
	var wg sync.WaitGroup
	statuses := make(chan int, attempts)
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			resp := h.publish(t, channelID)
			statuses <- resp.StatusCode
			require.NoError(t, resp.Body.Close())
		}()
	}
	wg.Wait()
	close(statuses)

	created, conflict := 0, 0
	for status := range statuses {
		switch status {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflict++
		default:
			t.Fatalf("unexpected status %d", status)
		}
	}
	assert.Equal(t, 1, created, "exactly one session becomes valid")
	assert.Equal(t, attempts-1, conflict, "every loser receives an explicit failure")

	// No dangling sessions, PeerConnections or control streams.
	assert.Equal(t, 1, h.source.activeSessionCount())
	assert.True(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 1, h.svc.starts(), "only the winner called StartStream")
	assert.Equal(t, 0, h.svc.ends(), "losers must not stop the winner's stream")

	// Cleanup after the overlap leaves the channel idle.
	resp := h.delete(channelID, streamKeyFor(channelID))
	require.NoError(t, resp.Body.Close())
	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "cleanup after conflict test")
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 1, h.svc.ends())
}

func TestWhipOldSessionTeardownCannotKillNewSession(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 30

	// Establish the first (old) session.
	resp := h.publish(t, channelID)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	oldSession := h.source.lookupSession(types.ChannelID(channelID))
	require.NotNil(t, oldSession)

	// Simulate the client immediately retrying: the old connection is dropped
	// while a replacement is installed. We model the takeover directly through
	// the registry, then fire every possible teardown trigger of the OLD session
	// (its connection-state callback via pc.Close, the timeout, repeated ends).
	h.source.sessionsMutex.Lock()
	delete(h.source.sessions, types.ChannelID(channelID))
	newSession := &session{
		id:        oldSession.id + 100,
		channelID: types.ChannelID(channelID),
		pc:        oldSession.pc, // reuse the real PC so Close() exercises callbacks
	}
	h.source.sessions[types.ChannelID(channelID)] = newSession
	h.source.sessionsMutex.Unlock()

	// Old PeerConnection failure callback must not harm the replacement.
	require.NoError(t, oldSession.pc.Close())
	h.source.endSession(oldSession)
	h.source.endSession(oldSession)

	// The replacement survives and remains the sole registered session.
	assert.Same(t, newSession, h.source.lookupSession(types.ChannelID(channelID)))
	assert.Equal(t, 1, h.source.activeSessionCount())
	assert.True(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 0, h.svc.ends(), "old session cleanup must not stop the new stream")

	// Only the current owner can stop the channel.
	h.source.endSession(newSession)
	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "new session cleanup")
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 1, h.svc.ends())
}

func TestWhipReentrantCleanupSendsStopOnce(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 40

	resp := h.publish(t, channelID)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	sess := h.source.lookupSession(types.ChannelID(channelID))
	require.NotNil(t, sess)

	// DELETE, connection-state callback and timeout can all fire at once.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.source.endSession(sess)
		}()
	}
	// Also exercise the real connection callback by closing the PC.
	require.NoError(t, sess.pc.Close())
	wg.Wait()

	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "reentrant cleanup")
	h.waitForEnds(t, 1)
	h.waitForOrStops(t, 1)
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 1, h.svc.ends(), "external stop exactly once despite concurrent triggers")
	assert.Equal(t, 1, h.or.stops())
}

func TestWhipConnectionStateFailureEndsOnlyOwnSession(t *testing.T) {
	h := newWhipHarness(t)
	const channelA, channelB = 50, 51

	respA := h.publish(t, channelA)
	require.Equal(t, http.StatusCreated, respA.StatusCode)
	require.NoError(t, respA.Body.Close())
	respB := h.publish(t, channelB)
	require.Equal(t, http.StatusCreated, respB.StatusCode)
	require.NoError(t, respB.Body.Close())

	sessionA := h.source.lookupSession(types.ChannelID(channelA))
	require.NotNil(t, sessionA)

	// ICE/connection failure on channel A.
	require.NoError(t, sessionA.pc.Close())

	h.waitForEnds(t, 1)
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelA)))

	// Channel B is unaffected and still live.
	assert.True(t, h.ctrl.HasStream(types.ChannelID(channelB)), "unrelated channel must stay live")
	assert.Equal(t, 1, h.svc.ends())

	resp := h.delete(channelB, streamKeyFor(channelB))
	require.NoError(t, resp.Body.Close())
	h.waitForEnds(t, 2)
	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "channel B cleanup")
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelB)))
}

func TestWhipTimeoutReapsStaleSessionAndAllowsRepublish(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 60

	// Shorten the connect timeout for this test.
	h.source.peerConnectionTimeout = 150 * time.Millisecond

	resp := h.publish(t, channelID)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	sess := h.source.lookupSession(types.ChannelID(channelID))
	require.NotNil(t, sess)
	// Negotiation never completes to Connected, so the timer must reap it.
	require.NotEqual(t, webrtc.PeerConnectionStateConnected, sess.pc.ConnectionState())

	waitFor(t, 2*time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "timeout reaps stale session")
	h.waitForEnds(t, 1)
	h.waitForOrStops(t, 1)
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)))

	// After the timeout the channel can publish again. Restore a long timeout
	// so the fresh session is not itself reaped by the shortened test timeout.
	h.source.peerConnectionTimeout = time.Minute
	resp = h.publish(t, channelID)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("republish failed with status %d: %s", resp.StatusCode, readBody(t, resp))
	}
	require.NoError(t, resp.Body.Close())
	assert.True(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 1 }, "republish after timeout")

	resp = h.delete(channelID, streamKeyFor(channelID))
	require.NoError(t, resp.Body.Close())
	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "final cleanup")
}

func TestWhipDeleteWithBadAuthDoesNotChangeState(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 70

	resp := h.publish(t, channelID)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// An unauthorized DELETE must not touch the live session. Use a key without
	// a dash (a dashed value is parsed as a channel-key pair and rejected as a
	// bad parameter before authentication runs).
	delResp := h.delete(channelID, "badkey")
	assert.Equal(t, http.StatusUnauthorized, delResp.StatusCode)
	require.NoError(t, delResp.Body.Close())

	assert.True(t, h.ctrl.HasStream(types.ChannelID(channelID)))
	assert.Equal(t, 1, h.source.activeSessionCount())
	assert.Equal(t, 0, h.svc.ends())

	// The legitimate owner can still end the stream.
	delResp = h.delete(channelID, streamKeyFor(channelID))
	require.NoError(t, delResp.Body.Close())
	waitFor(t, time.Second, func() bool { return h.source.activeSessionCount() == 0 }, "authorized cleanup")
	assert.False(t, h.ctrl.HasStream(types.ChannelID(channelID)))
}

func TestWhipMethodNotAllowed(t *testing.T) {
	h := newWhipHarness(t)
	const channelID = 80

	req, err := http.NewRequest(http.MethodPut, h.endpoint(channelID), strings.NewReader("v=0"))
	require.NoError(t, err)
	req.Header.Set("Authorization", streamKeyFor(channelID))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 0, h.source.activeSessionCount())
}
