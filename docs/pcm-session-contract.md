# PCM session contract

The Babyface Pro has one USB audio session, and playback and capture
share it.  This note records what each side may assume of the other, and
how the two teardown paths (disconnect and suspend) behave.  The code
lives in `tools/kernel/babyfacepro.c` (`babyface_pcm_*`,
`babyface_stream_*`, `bf_teardown_once`, `bf_session_suspend`).

## The shared parameters

Four values define the session, and both directions run on them:

- the rate (and with it the USB altsetting and the **frame size in
  bytes**, 56 / 40 / 32 for alt 1 / 2 / 3),
- the URB size (`urb_frames`) and the number of URBs in flight
  (`urbs_active`).

Any change to one of these is a **change of contract**.  The driver
stops the session and publishes a new generation number; from that point
a substream is runnable only if it negotiated in the new generation.
Negotiating means `hw_params` on that substream.

### Why a generation, not just a field comparison

An application's runtime keeps the parameters it asked for, so comparing
`runtime->rate == chip->rate` and `runtime->period_size >= period` looks
like enough.  It is not:

- **Rate round trip.** Playback goes 48 → 96 → 48 kHz.  Both fields again
  match, yet capture's session was torn down and rebuilt twice.  Only the
  negotiated generation says capture is stale.
- **Frame size vs. rate.** A substream can match the rate and still hold a
  runtime sized for a different frame size if the rate is not what moved.

So each substream records the generation it negotiated in
(`subs_gen[]`) and the parameters it actually published (`subs_rate[]`,
`subs_frame_bytes[]`, `subs_urb_frames[]`, `subs_urbs_active[]`,
`subs_pub_gen[]`).  The runnable check (`bf_subs_runnable_locked()`) is:

```
subs_gen[dir] == session_gen            /* negotiated in the live generation */
subs_pub_gen[dir] == session_gen        /* its published params are that generation's */
runtime->rate == subs_rate[dir]
runtime->period_size >= subs_urb_frames[dir] * ceil(subs_urbs_active[dir] / 2)
subs_frame_bytes[dir] == frame_bytes    /* the session's bytes per frame */
```

All of it is read under `chip->lock`, the same lock `hw_params` takes
when it writes the generation and the parameters together.  A `START`
therefore sees the whole old contract or the whole new one, never half.

### What each side sees

- **The side that changes** stops the session (if running) and, on its
  next `prepare`, starts a new one with its parameters.  Its own other
  direction — the same DAW's playback or capture — is invalidated too,
  and must renegotiate.  That is deliberate: the runtime that was set up
  under the old frame size cannot stream the new one safely.
- **The other side** is not runnable until it calls `hw_params` again.
  `prepare` returns `-EINVAL`; `START` returns `-EPIPE`.  Both are the
  ALSA codes an application already recovers from: `-EPIPE` is the same
  "xrun, re-arm" every sound server handles, and a refused `hw_params`
  is `-EBUSY` when the other owner holds the clock.
- **A refused change** leaves the session untouched: it returns before
  anything is written, so the other side keeps working.  A change whose
  USB control transfer (`SET_INTERFACE`) fails is refused before the old
  session is stopped, so the running session is not torn down for a rate
  the device never took.
- **Sequential and same-process use** are unaffected.  A single app that
  opens playback then capture, negotiates each once, streams; a DAW that
  changes its period is simply invalidated in its own other direction and
  renegotiates it.  A repeated `hw_params` with the same values is not a
  change and keeps both directions valid.

## Teardown

Both teardown paths kill the URBs and wake every blocked `read`/`write`
with a class of error the application can tell apart.

| Event | Wakes with | Application recovery |
|---|---|---|
| Stream error (bad URBs) | `SNDRV_PCM_STATE_XRUN` → `-EPIPE` | prepare + START |
| Disconnect | `SNDRV_PCM_STATE_DISCONNECTED` → `-ENODEV` | reopen the card |
| Suspend (S3) | `SNDRV_PCM_STATE_SUSPENDED` → `-ESTRPIPE` | hw_params + prepare after resume |

### Disconnect

`bf_teardown_once()` is one shot, guarded by `torn_down`.  It marks
`->shutdown`, kills the session and frees the URBs and their DMA buffers
under `chip->mutex`.  A second disconnect — a connect/disconnect race, a
re-probe interleave, the MIDI interface's own disconnect — returns
without freeing anything again.  After it the URB arrays and slots are
NULL, so nothing submits a freed URB; `prepare` and `hw_params` see
`->shutdown` and return `-ENODEV`.

### Suspend and resume

`bf_session_suspend()` sets `->suspended` **before** waking the
substreams, so a wake-up racing the suspend cannot start a session
against a device that is going away.  `hw_params` and `prepare` return
`-ESTRPIPE` while it is set.  The URBs are killed but the buffers stay
allocated: the host controller comes back on resume and the recovery
(`hw_params` + `prepare`) needs them.  `resume` re-runs the cold init and
clears `->suspended` only once the device is live again, so the
application's recovery then negotiates a fresh generation and streams.

### Why the URB completion is safe during teardown

`babyface_stream_kill()` clears `->streaming` **before** killing the
URBs, and the completion handlers test it before resubmitting.  A URB
that completes during the kill loop retires instead of resubmitting into
its own kill.  `bf_free_urbs()` kills each URB before freeing it, so no
completion can touch a buffer that is already gone.

## Testing

`tools/kernel/pcm_session_test.c` runs this contract in user space
against the driver's own code (`extract_laws.py`), over the USB stub in
`pcm_session_shim.h`.  It covers:

- a rate change, and a URB-geometry change, invalidating the other
  direction, which then renegotiates and streams;
- a rate round trip that only the generation catches;
- a refused change leaving the session whole;
- the sequential and same-process duplex cases still working;
- a change versus a `START` in both orders (the change first: START
  refuses; START first: the change stops it);
- disconnect waking blocked I/O with `DISCONNECTED`, and a second
  teardown freeing nothing and submitting nothing after free;
- suspend giving `-ESTRPIPE` and `prepare` after resume building a new
  session.

Run it with `sh tools/kernel/selftests.sh` (no card, no kernel tree).
