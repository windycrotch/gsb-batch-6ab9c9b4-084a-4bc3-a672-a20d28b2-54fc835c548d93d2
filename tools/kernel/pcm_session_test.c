/* pcm_session_test.c - the PCM *session* contract, without the card.
 *
 * pcm_selftest.c covers the URB completion handlers; this covers the
 * session around them: hw_params / prepare / trigger / hw_free, the
 * session start, and the two teardown paths, run against the driver's
 * own code (extract_laws.py) over the USB stub in pcm_session_shim.h.
 *
 * What it pins down:
 *  - the parameter contract: when one direction changes the rate, the
 *    frame size or the URB geometry, the other direction is not runnable
 *    (prepare and START both refuse) until it negotiates again, and then
 *    it streams normally.  That holds across a USB control-transfer
 *    failure too: a refused change must not leave a half-updated
 *    session.
 *  - rate change versus a concurrent START: whichever finishes second
 *    sees the other, so a START never starts a substream on a session
 *    whose frame size it did not negotiate.
 *  - the sequential single-process change and the same-process duplex
 *    change still work.
 *  - disconnect mid-stream wakes the blocked read/write with a
 *    disconnect-class error, and a second disconnect (the re-probe
 *    interleave) frees nothing twice and submits nothing after free.
 *  - suspend gives a recoverable error and prepare after resume builds
 *    a new session.
 *
 * laws: babyface_pcm_open babyface_pcm_close babyface_pcm_hw_params
 *       babyface_pcm_hw_free babyface_pcm_prepare babyface_pcm_trigger
 *       babyface_pcm_stop_both babyface_stream_kill babyface_stream_start
 *       bf_free_urbs bf_teardown_once bf_session_suspend
 *       bf_session_stop_both bf_resume_locked bf_subs_session_current
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int fails;

static void chk(const char *name, long got, long want)
{
	if (got != want) {
		printf("FAIL %-56s got %ld want %ld\n", name, got, want);
		fails++;
	} else {
		printf("ok   %-56s %ld\n", name, got);
	}
}

#define CHANNELS	12
#define RING_FRAMES	2048

/* State the shim's inline functions read. */
int stub_submit_after_free;
int stub_free_calls;
int stub_coherent_free;
int stub_set_interface_alt;
int stub_set_interface_calls;
int stub_set_interface_ret;
int usb_submit_ret;
int stub_tgid = 100;
struct task_struct stub_task;

struct rig {
	struct usb_device udev;
	struct usb_interface iface;
	struct snd_usb_babyface chip;
	struct snd_pcm_substream subs[2];
	struct snd_pcm_runtime rt[2];
	u8 ring[2][RING_FRAMES * CHANNELS * 4];
};

static void stub_reset(void)
{
	stub_submit_after_free = 0;
	stub_free_calls = 0;
	stub_coherent_free = 0;
	stub_set_interface_alt = 0;
	stub_set_interface_calls = 0;
	stub_set_interface_ret = 0;
	usb_submit_ret = 0;
}

/* The URBs and buffers probe() would have allocated. */
static void rig_alloc_urbs(struct rig *r, unsigned int nurbs)
{
	unsigned int i, bytes = 14 * 4 * r->chip.frames_per_urb;

	r->chip.nurbs = nurbs;
	r->chip.urbs_in = calloc(nurbs, sizeof(*r->chip.urbs_in));
	r->chip.urbs_out = calloc(nurbs, sizeof(*r->chip.urbs_out));
	r->chip.buf_in = calloc(nurbs, sizeof(*r->chip.buf_in));
	r->chip.buf_out = calloc(nurbs, sizeof(*r->chip.buf_out));
	r->chip.dma_in = calloc(nurbs, sizeof(*r->chip.dma_in));
	r->chip.dma_out = calloc(nurbs, sizeof(*r->chip.dma_out));
	for (i = 0; i < nurbs; i++) {
		r->chip.urbs_in[i] = usb_alloc_urb(0, 0);
		r->chip.urbs_out[i] = usb_alloc_urb(0, 0);
		r->chip.buf_in[i] = malloc(bytes);
		r->chip.buf_out[i] = malloc(bytes);
		memset(r->chip.buf_in[i], 0, bytes);
		memset(r->chip.buf_out[i], 0, bytes);
		r->chip.dma_in[i] = (dma_addr_t)(uintptr_t)r->chip.buf_in[i];
		r->chip.dma_out[i] = (dma_addr_t)(uintptr_t)r->chip.buf_out[i];
	}
}

static void rig_init(struct rig *r, unsigned int nurbs){
	unsigned int s;

	memset(r, 0, sizeof(*r));
	stub_reset();
	r->chip.dev = &r->udev;
	r->chip.iface = &r->iface;
	r->chip.nurbs = nurbs;
	r->chip.frames_per_urb = 256;
	r->chip.urb_frames_min = 32;
	r->chip.urb_frames = 256;
	r->chip.urbs_active = nurbs;
	r->chip.frame_bytes = 56;
	r->chip.rate = 48000;
	r->chip.alt = 1;
	atomic_set(&r->chip.urb_err, 0);
	atomic_set(&r->chip.torn_down, 0);
	rig_alloc_urbs(r, nurbs);
	for (s = 0; s < 2; s++) {
		r->subs[s].runtime = &r->rt[s];
		r->subs[s].stream = s;
		r->subs[s].chip = &r->chip;
		r->subs[s].state = SNDRV_PCM_STATE_OPEN;
		r->rt[s].dma_area = r->ring[s];
		r->rt[s].buffer_size = RING_FRAMES;
		r->rt[s].period_size = 256;
		r->rt[s].channels = CHANNELS;
		r->rt[s].rate = 48000;
	}
}

/* hw_params with the parameters as the app negotiated them.  The shim's
 * params struct is filled in directly, so this is exactly what the driver
 * reads.
 */
static int rig_params(struct rig *r, int stream, unsigned int rate,
		      unsigned int period)
{
	struct snd_pcm_hw_params p = {
		.rate = rate,
		.period_size = period,
		.buffer_size = period * 2,
		.channels = CHANNELS,
	};
	struct snd_pcm_substream *subs = &r->subs[stream];

	subs->runtime->rate = rate;
	subs->runtime->period_size = period;
	subs->runtime->buffer_size = period * 2;
	return babyface_pcm_hw_params(subs, &p);
}

static void rig_open(struct rig *r, int stream)
{
	babyface_pcm_open(&r->subs[stream]);
}

static int rig_prepare(struct rig *r, int stream)
{
	return babyface_pcm_prepare(&r->subs[stream]);
}

static int rig_start(struct rig *r, int stream)
{
	return babyface_pcm_trigger(&r->subs[stream], SNDRV_PCM_TRIGGER_START);
}

/* Close what the test opened and free what rig_alloc_urbs() took.  The
 * driver's own bf_free_urbs() releases the URBs and buffers; the arrays
 * are freed here so the next rig starts clean.
 */
static void rig_teardown(struct rig *r)
{
	int s;

	for (s = 0; s < 2; s++)
		babyface_pcm_close(&r->subs[s]);
	bf_teardown_once(&r->chip);
	free(r->chip.urbs_in);
	free(r->chip.urbs_out);
	free(r->chip.buf_in);
	free(r->chip.buf_out);
	free(r->chip.dma_in);
	free(r->chip.dma_out);
}

/* A rate that leaves and comes back: the field-by-field checks (rate
 * equals, period long enough) both pass, yet the capture substream
 * negotiated a session that was torn down and rebuilt twice.  Only the
 * negotiated generation tells it is stale, and the contract says it must
 * renegotiate before it runs.
 */static void rate_round_trip_still_invalidates(void)
{
	struct rig r;

	stub_tgid = 400;
	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	chk("round-trip: running at 48k", r.chip.streaming, 1);
	/* Playback goes to 96 kHz and back to 48 kHz; capture never
	 * negotiated again.
	 */
	chk("round-trip: pb goes to 96k", rig_params(&r, 0, 96000, 256), 0);
	chk("round-trip: pb back to 48k", rig_params(&r, 0, 48000, 256), 0);
	chk("round-trip: session rate is 48k again", r.chip.rate, 48000);
	chk("round-trip: cap runtime rate still 48k",
	    r.subs[1].runtime->rate, 48000);
	/* Playback starts the new session.  Capture's START now runs with
	 * the session live and its rate and period both looking right, so
	 * only the generation can tell it never negotiated this one.
	 */
	chk("round-trip: pb prepares the new session", rig_prepare(&r, 0), 0);
	chk("round-trip: session live again", r.chip.streaming, 1);
	chk("round-trip: cap prepare refused", rig_prepare(&r, 1), -EINVAL);
	chk("round-trip: cap START refused", rig_start(&r, 1), -EPIPE);
	chk("round-trip: cap renegotiates", rig_params(&r, 1, 48000, 256), 0);
	chk("round-trip: cap prepare", rig_prepare(&r, 1), 0);
	chk("round-trip: cap START", rig_start(&r, 1), 0);
	rig_teardown(&r);
}

/* ---- the contract: the other direction goes stale on a change -------- */

static void change_invalidates_other_direction(void)
{
	struct rig r;

	rig_init(&r, 4);
	rig_open(&r, SNDRV_PCM_STREAM_PLAYBACK);
	rig_open(&r, SNDRV_PCM_STREAM_CAPTURE);
	chk("setup: pb hw_params", rig_params(&r, 0, 48000, 256), 0);
	chk("setup: cap hw_params", rig_params(&r, 1, 48000, 256), 0);
	chk("setup: pb prepare", rig_prepare(&r, 0), 0);
	chk("setup: cap prepare", rig_prepare(&r, 1), 0);
	chk("setup: a session runs", r.chip.streaming, 1);

	/* Playback switches the rate.  That stops the session; capture has
	 * negotiated the old rate and frame size and must not run again
	 * until it negotiates.
	 */
	chk("pb rate change to 96 kHz", rig_params(&r, 0, 96000, 256), 0);
	chk("rate change: session stopped", r.chip.streaming, 0);
	chk("rate change: cap runtime still 48k", r.subs[1].runtime->rate, 48000);
	chk("rate change: cap prepare refused", rig_prepare(&r, 1), -EINVAL);
	chk("rate change: cap START refused", rig_start(&r, 1), -EPIPE);

	/* After it negotiates again it works. */
	chk("cap renegotiates at 96k", rig_params(&r, 1, 96000, 256), 0);
	chk("cap prepare after renegotiation", rig_prepare(&r, 1), 0);
	chk("cap START after renegotiation", rig_start(&r, 1), 0);
	chk("renegotiated session runs", r.chip.streaming, 1);

	rig_teardown(&r);
}

/* A URB-geometry change (the period/buffer slider) invalidates the other
 * direction the same way, even at the same rate.
 */
static void geometry_invalidates_other_direction(void)
{
	struct rig r;

	rig_init(&r, 4);
	rig_open(&r, SNDRV_PCM_STREAM_PLAYBACK);
	rig_open(&r, SNDRV_PCM_STREAM_CAPTURE);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	/* Playback re-negotiates a bigger period: new URB size and count. */
	chk("pb renegotiates period 1024", rig_params(&r, 0, 48000, 1024), 0);
	chk("geometry change: cap prepare refused", rig_prepare(&r, 1), -EINVAL);
	chk("geometry change: cap START refused", rig_start(&r, 1), -EPIPE);
	chk("geometry change: cap renegotiates", rig_params(&r, 1, 48000, 1024), 0);
	chk("geometry change: cap prepare", rig_prepare(&r, 1), 0);
	chk("geometry change: cap START", rig_start(&r, 1), 0);
	rig_teardown(&r);
}

/* A refused change leaves the session whole: the other direction keeps
 * working on the parameters it had.  This is the cross-process case (a
 * second application that wants a shorter period than the session
 * spans); the refusal comes back before anything is touched.
 */
static void refused_change_keeps_session(void)
{
	struct rig r;

	rig_init(&r, 4);
	stub_tgid = 100;
	rig_open(&r, SNDRV_PCM_STREAM_PLAYBACK);
	stub_tgid = 200;
	rig_open(&r, SNDRV_PCM_STREAM_CAPTURE);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	chk("refused: session runs", r.chip.streaming, 1);
	/* Playback (owner 100) asks for a period below the session's span
	 * while capture (owner 200) holds the clock: refused.
	 */
	stub_tgid = 100;
	chk("refused: short period refused", rig_params(&r, 0, 48000, 32), -EBUSY);
	chk("refused change: cap still current",
	    bf_subs_session_current(&r.chip, &r.subs[1]), 1);
	chk("refused change: cap still prepares", rig_prepare(&r, 1), 0);
	chk("refused change: cap still starts", rig_start(&r, 1), 0);
	chk("refused change: session still runs", r.chip.streaming, 1);
	rig_teardown(&r);
}

/* A change whose USB control transfer fails leaves no half-applied
 * contract: usb_set_interface() errors out before the rate, frame size
 * and generation are published, so the other direction's negotiated
 * session stays valid and it keeps streaming on it.
 */
static void failed_usb_change_keeps_session(void)
{
	struct rig r;

	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	chk("usb-fail: running at 48k", r.chip.streaming, 1);
	/* Playback asks for 96 kHz; the SET_INTERFACE that would follow
	 * fails.
	 */
	stub_set_interface_ret = -EPIPE;
	chk("usb-fail: change fails", rig_params(&r, 0, 96000, 256), -EPIPE);
	stub_set_interface_ret = 0;
	chk("usb-fail: session rate unchanged", r.chip.rate, 48000);
	chk("usb-fail: generation not bumped for the failure",
	    r.chip.subs_pub_gen[0] != r.chip.subs_pub_gen[1], 0);
	chk("usb-fail: session still running", r.chip.streaming, 1);
	chk("usb-fail: cap still current",
	    bf_subs_session_current(&r.chip, &r.subs[1]), 1);
	chk("usb-fail: cap still starts", rig_start(&r, 1), 0);
	rig_teardown(&r);
}

/* ---- the same process: sequential and duplex changes still work ------- */
static void same_process_duplex_change(void)
{
	struct rig r;

	stub_tgid = 200;
	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	chk("duplex: running", r.chip.streaming, 1);
	/* The DAW changes playback's period (its own other direction). */
	chk("duplex: pb renegotiates", rig_params(&r, 0, 48000, 512), 0);
	/* The other direction is invalid now, even though one process owns
	 * both: it must renegotiate before it runs.
	 */
	chk("duplex: cap prepare refused", rig_prepare(&r, 1), -EINVAL);
	chk("duplex: cap renegotiates", rig_params(&r, 1, 48000, 512), 0);
	chk("duplex: cap prepare", rig_prepare(&r, 1), 0);
	chk("duplex: cap START", rig_start(&r, 1), 0);
	chk("duplex: pb START", rig_start(&r, 0), 0);
	chk("duplex: session runs", r.chip.streaming, 1);

	/* A repeated hw_params with the same values is not a change. */
	chk("duplex: cap hw_params repeat", rig_params(&r, 1, 48000, 512), 0);
	chk("duplex: repeat keeps cap current", rig_prepare(&r, 1), 0);
	chk("duplex: repeat keeps pb current", rig_start(&r, 0), 0);
	rig_teardown(&r);
}

/* Sequential capture then playback, each negotiated once: the classic
 * one-app-two-directions open. */
static void same_process_sequential(void)
{
	struct rig r;

	stub_tgid = 300;
	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	chk("seq: pb hw_params", rig_params(&r, 0, 44100, 128), 0);
	chk("seq: pb prepare", rig_prepare(&r, 0), 0);
	chk("seq: cap hw_params", rig_params(&r, 1, 44100, 128), 0);
	chk("seq: cap prepare still ok", rig_prepare(&r, 0), 0);
	chk("seq: cap prepare", rig_prepare(&r, 1), 0);
	chk("seq: pb START", rig_start(&r, 0), 0);
	chk("seq: cap START", rig_start(&r, 1), 0);
	chk("seq: session runs", r.chip.streaming, 1);
	rig_teardown(&r);
}

/* ---- change vs. concurrent START: only one holds --------------------- */

/* A real race, in the two orders it can resolve to.  The shim is single-
 * threaded, so the test runs both serializations explicitly; between them
 * they cover the two lines the driver has to make mutually exclusive.
 *
 * Order 1 (change then START, below): hw_params' publication moved the
 * generation before START checked, so START refuses.
 * Order 2 (START then change, change_stops_a_running_start()): START ran
 * in the old generation, and the change then stopped it - so after both,
 * nothing streams on stale parameters either way.
 */

/* The change wins: hw_params' publication moved the generation before
 * START got the lock, so START refuses - no stale-parameter streaming.
 */
static void start_loses_to_a_finished_change(void)
{
	struct rig r;

	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	/* Capture negotiated in generation N.  Playback changes the
	 * geometry (generation N+1) and the session restarts; capture's
	 * START now looks at the new generation.
	 */
	rig_params(&r, 0, 48000, 1024);
	rig_prepare(&r, 0);
	chk("race: cap START after change refused", rig_start(&r, 1), -EPIPE);
	chk("race: pb START after its own change", rig_start(&r, 0), 0);
	chk("race: no stale running", r.subs[1].running, 0);
	rig_teardown(&r);
}

/* The START wins (it ran first, in the old generation), and the change
 * then stops it - so after both, nothing runs on stale parameters.
 */
static void change_stops_a_running_start(void)
{
	struct rig r;

	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	chk("race2: cap START in gen N", rig_start(&r, 1), 0);
	r.subs[1].running = true;
	/* The other direction now changes the frame size. */
	chk("race2: pb change stops the session", rig_params(&r, 0, 96000, 256), 0);
	chk("race2: session stopped", r.chip.streaming, 0);
	chk("race2: cap not running any more", r.subs[1].running, 0);
	chk("race2: cap prepare refused", rig_prepare(&r, 1), -EINVAL);
	rig_teardown(&r);
}

/* ---- disconnect: the blocked read/write wakes; teardown is once ------ */

static void disconnect_wakes_and_frees_once(void)
{
	struct rig r;
	int cap_free, cap_coherent;

	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	rig_params(&r, 0, 48000, 256);
	rig_params(&r, 1, 48000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	r.subs[0].running = true;
	r.subs[1].running = true;
	r.subs[1].state = SNDRV_PCM_STATE_RUNNING;

	chk("disconnect: first teardown", bf_teardown_once(&r.chip), 1);
	chk("disconnect: chip marked shutdown", r.chip.shutdown, 1);
	bf_session_stop_both(&r.chip, SNDRV_PCM_STATE_DISCONNECTED);
	/* The blocked read/write wakes with the disconnect class, not a
	 * bare xrun: the app can tell "gone" from "re-arm".
	 */
	chk("disconnect: capture woken DISCONNECTED",
	    r.subs[1].state, SNDRV_PCM_STATE_DISCONNECTED);
	chk("disconnect: playback woken DISCONNECTED",
	    r.subs[0].state, SNDRV_PCM_STATE_DISCONNECTED);
	chk("disconnect: nothing left running", r.subs[1].running, 0);
	chk("disconnect: prepare after disconnect", rig_prepare(&r, 1), -ENODEV);
	chk("disconnect: hw_params after disconnect",
	    rig_params(&r, 1, 48000, 256), -ENODEV);

	cap_free = stub_free_calls;
	cap_coherent = stub_coherent_free;
	chk("disconnect: URBs freed", cap_free, 8);	/* 4 in + 4 out */
	chk("disconnect: buffers freed", cap_coherent, 8);

	/* A re-probe interleave delivers disconnect twice.  The second
	 * pass must free nothing and submit nothing.
	 */
	chk("disconnect: second teardown is a no-op",
	    bf_teardown_once(&r.chip), 0);
	chk("disconnect: no URB freed twice", stub_free_calls, cap_free);
	chk("disconnect: no buffer freed twice", stub_coherent_free, cap_coherent);
	/* And nothing may submit the freed URBs afterwards. */
	chk("disconnect: prepare cannot resubmit", rig_prepare(&r, 1), -ENODEV);
	chk("disconnect: a START cannot resubmit", rig_start(&r, 1), -EPIPE);
	chk("disconnect: no submit after free", stub_submit_after_free, 0);

	free(r.chip.urbs_in);
	free(r.chip.urbs_out);
	free(r.chip.buf_in);
	free(r.chip.buf_out);
	free(r.chip.dma_in);
	free(r.chip.dma_out);
}

/* ---- suspend: a recoverable error; resume rebuilds the session ------- */

static void suspend_resume_rebuilds(void)
{
	struct rig r;

	rig_init(&r, 4);
	rig_open(&r, 0);
	rig_open(&r, 1);
	rig_params(&r, 0, 96000, 256);
	rig_params(&r, 1, 96000, 256);
	rig_prepare(&r, 0);
	rig_prepare(&r, 1);
	r.subs[1].running = true;
	r.subs[1].state = SNDRV_PCM_STATE_RUNNING;
	chk("suspend: session running", r.chip.streaming, 1);

	bf_session_suspend(&r.chip);
	bf_session_stop_both(&r.chip, SNDRV_PCM_STATE_SUSPENDED);
	chk("suspend: capture woken SUSPENDED",
	    r.subs[1].state, SNDRV_PCM_STATE_SUSPENDED);
	chk("suspend: suspended flag", r.chip.suspended, 1);
	/* While suspended, and before the app renegotiates, prepare gives
	 * the recoverable error, not a silent start.
	 */
	chk("suspend: prepare gives ESTRPIPE", rig_prepare(&r, 1), -ESTRPIPE);
	chk("suspend: hw_params gives ESTRPIPE",
	    rig_params(&r, 1, 96000, 256), -ESTRPIPE);

	/* Resume: the device is re-initialised and the flag clears. */
	stub_reset();
	chk("resume: re-init ok", bf_resume_locked(&r.chip), 0);
	r.chip.suspended = false;
	/* A plain prepare after the resume builds a new session: the
	 * runtime still holds what the application negotiated, and suspend
	 * does not invalidate it - only a parameter change does.
	 */
	chk("resume: cap prepare rebuilds the session", rig_prepare(&r, 1), 0);
	chk("resume: session runs again", r.chip.streaming, 1);
	chk("resume: pb prepare also works", rig_prepare(&r, 0), 0);
	chk("resume: pb START", rig_start(&r, 0), 0);
	/* And an application that renegotiates (the sound server's usual
	 * recovery) works too: a repeated hw_params with the same values is
	 * not a change, so the other direction stays valid.
	 */
	chk("resume: cap renegotiates", rig_params(&r, 1, 96000, 256), 0);
	chk("resume: pb still current after cap repeat",
	    bf_subs_session_current(&r.chip, &r.subs[0]), 1);
	chk("resume: cap prepare after renegotiation", rig_prepare(&r, 1), 0);
	rig_teardown(&r);
}

int main(void)
{
	rate_round_trip_still_invalidates();
	change_invalidates_other_direction();
	geometry_invalidates_other_direction();
	refused_change_keeps_session();
	failed_usb_change_keeps_session();
	same_process_duplex_change();
	same_process_sequential();
	start_loses_to_a_finished_change();
	change_stops_a_running_start();
	disconnect_wakes_and_frees_once();
	suspend_resume_rebuilds();
	if (fails) {
		printf("%d FAILED\n", fails);
		return 1;
	}
	printf("ok all\n");
	return 0;
}
