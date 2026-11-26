/* pcm_session_shim.h - the kernel stand-ins the PCM *session* path needs
 * to run in userspace: not just the URB completion handlers (pcm_shim.h)
 * but hw_params/prepare/trigger/hw_free, the session start, and the two
 * teardown paths.
 *
 * selftests.sh includes it before the header extract_laws.py builds from
 * the driver sources, so the driver's own code compiles here unchanged.
 * The USB layer is a stub: control transfers, set_interface, URB submit
 * and kill only record what happened, and the test drives the
 * completions by hand.  That is what makes the session contract testable
 * without a card.
 *
 * struct snd_usb_babyface mirrors the driver's field names on purpose: a
 * field the driver reads but this shim does not declare fails the build,
 * which is the point.
 */
#ifndef PCM_SESSION_SHIM_H
#define PCM_SESSION_SHIM_H

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define ESTRPIPE	100	/* not in userspace errno; the driver's value */
#ifndef ESHUTDOWN
#define ESHUTDOWN	108
#endif
#ifndef ECONNRESET
#define ECONNRESET	104
#endif

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t s8;
typedef int16_t s16;
typedef int32_t s32;
typedef int64_t s64;
typedef unsigned long dma_addr_t;
#ifndef _PID_T_
#define _PID_T_		/* keep a later <sys/types.h> from redefining it */
typedef long pid_t;
#endif
typedef long long ktime_t;

#define min(a, b)		((a) < (b) ? (a) : (b))
#define max(a, b)		((a) > (b) ? (a) : (b))
#define min_t(t, a, b)		((t)(a) < (t)(b) ? (t)(a) : (t)(b))
#define max_t(t, a, b)		((t)(a) > (t)(b) ? (t)(a) : (t)(b))
#define clamp_t(t, v, lo, hi)	((t)((t)(v) < (t)(lo) ? (t)(lo) : \
					 (t)(v) > (t)(hi) ? (t)(hi) : (t)(v)))
#define rounddown(x, y)		((x) / (y) * (y))
#define roundup(x, y)		(((x) + (y) - 1) / (y) * (y))
#define DIV_ROUND_UP(n, d)	(((n) + (d) - 1) / (d))
#define READ_ONCE(x)		(x)
#define WRITE_ONCE(x, v)	((x) = (v))
#define GFP_ATOMIC		0
#define GFP_KERNEL		0
#define __rcu
#define rcu_dereference(p)	(p)
#define rcu_assign_pointer(p, v)	((p) = (v))
#define RCU_INIT_POINTER(p, v)	((p) = (v))
#define rcu_read_lock()		((void)0)
#define rcu_read_unlock()	((void)0)
#define synchronize_rcu()	((void)0)
#define lockdep_assert_held(l)	((void)(l))

static inline u64 div_u64(u64 a, u32 b) { return a / b; }
static inline s64 div64_s64(s64 a, s64 b) { return a / b; }
static inline s64 ktime_ms_delta(ktime_t now, ktime_t before)
{
	return (now - before) / 1000000;
}
static inline ktime_t ktime_get(void)
{
	static ktime_t t;

	return t += 1000000000;		/* a ms per call */
}
static inline void msleep(unsigned int ms) { (void)ms; }
static inline void put_unaligned_le32(u32 v, void *p) { memcpy(p, &v, 4); }
static inline void *kmemdup(const void *s, size_t n, int gfp)
{
	void *d = malloc(n);

	(void)gfp;
	if (d)
		memcpy(d, s, n);
	return d;
}
#define kfree(p)		free(p)
#define kcalloc(n, s, g)	calloc((n), (s))

/* ---- locks ----
 *
 * Single-threaded, so the guards do not block; they only mark that the
 * code took the lock.  The serialization the contract relies on -
 * hw_params() publishes the parameters and the generation under
 * chip->lock, trigger() checks them under the same lock - is what the
 * extracted code does with these guards, and the test pins the two
 * possible serializations of a race explicitly rather than by running
 * real threads over state that only the kernel's own locks make safe.
 */
typedef int spinlock_t;
typedef int mutex_t;
#define mutex_init(m)			((void)(m))
#define spin_lock_init(l)		((void)(l))
#define guard(type)			guard_##type
#define guard_spinlock(l)		((void)(l))
#define guard_spinlock_irq(l)		((void)(l))
#define guard_spinlock_irqsave(l)	((void)(l))
#define guard_mutex(l)			((void)(l))
#define guard_rcu()			((void)0)
#define guard_pcm_stream_lock_irqsave(s)	((void)(s))
#define scoped_guard(...)	for (int _sg_once = 1; _sg_once; _sg_once = 0)

/* ---- atomics ---- */
typedef int atomic_t;
static inline int atomic_inc_return(atomic_t *v) { return ++*v; }
static inline int atomic_read(atomic_t *v) { return *v; }
static inline void atomic_set(atomic_t *v, int i) { *v = i; }
static inline int atomic_xchg(atomic_t *v, int i)
{
	int old = *v;

	*v = i;
	return old;
}
static inline bool atomic_try_cmpxchg(atomic_t *v, int *old, int new)
{
	if (*v == *old) {
		*v = new;
		return true;
	}
	*old = *v;
	return false;
}

/* ---- workqueue ---- */
struct work_struct { int scheduled; };
static inline bool schedule_work(struct work_struct *w)
{
	w->scheduled++;
	return true;
}
static inline bool flush_work(struct work_struct *w) { (void)w; return false; }
static inline bool cancel_work_sync(struct work_struct *w)
{
	(void)w;
	return false;
}

/* ---- basic kernel structs the PCM path touches ---- */
struct device { int unused; };
struct usb_device { struct device dev; };
struct usb_interface { void *intfdata; };

#define dev_dbg(d, ...)			((void)(d))
#define dev_dbg_ratelimited(d, ...)	((void)(d))
#define dev_err(d, ...)			((void)(d))
#define dev_err_ratelimited(d, ...)	((void)(d))
#define dev_warn(d, ...)		((void)(d))
#define dev_info(d, ...)		((void)(d))

/* ---- USB stub: records what the session path did ---- */
#define USB_DIR_OUT			0
#define USB_DIR_IN			0x80
#define USB_TYPE_VENDOR			0x40
#define USB_RECIP_DEVICE		0
#define URB_NO_TRANSFER_DMA_MAP		0x0400

struct urb {
	void *context;
	int status;
	unsigned int actual_length;
	unsigned int transfer_buffer_length;
	u8 *transfer_buffer;
	int submitted;
	int killed;
	dma_addr_t transfer_dma;
	unsigned int transfer_flags;
	int freed;
	/* A URB the stub handed out, so a submit after free is visible. */
	int magic;
};

/* Counters the test reads. */
extern int stub_submit_after_free;	/* usb_submit_urb on a freed URB */
extern int stub_free_calls;		/* usb_free_urb calls */
extern int stub_coherent_free;		/* usb_free_coherent calls */
extern int stub_set_interface_alt;	/* last alt passed to set_interface */
extern int stub_set_interface_calls;

static inline int usb_control_msg_send(struct usb_device *d, int e, int req,
				       int type, int val, int idx, void *b,
				       int len, int to, int gfp)
{
	(void)d; (void)e; (void)req; (void)type; (void)val; (void)idx;
	(void)b; (void)len; (void)to; (void)gfp;
	return 0;
}
static inline int usb_control_msg_recv(struct usb_device *d, int e, int req,
				       int type, int val, int idx, void *b,
				       int len, int to, int gfp)
{
	(void)d; (void)e; (void)req; (void)type; (void)val; (void)idx;
	(void)b; (void)len; (void)to; (void)gfp;
	return 0;
}
/* What the next usb_set_interface() returns: 0, or an error to inject. */
extern int stub_set_interface_ret;
static inline int usb_set_interface(struct usb_device *d, int iface, int alt)
{
	(void)d; (void)iface;
	stub_set_interface_calls++;
	if (stub_set_interface_ret)
		return stub_set_interface_ret;
	stub_set_interface_alt = alt;
	return 0;
}
static inline int usb_bulk_msg(struct usb_device *d, unsigned int pipe,
			       void *data, int len, int *actual, int to)
{
	(void)d; (void)pipe; (void)data; (void)len; (void)to;
	if (actual)
		*actual = len;
	return 0;
}
static inline unsigned int usb_rcvintpipe(struct usb_device *d, int ep)
{
	(void)d; return ep;
}
static inline unsigned int usb_sndintpipe(struct usb_device *d, int ep)
{
	(void)d; return ep;
}
static inline unsigned int usb_sndbulkpipe(struct usb_device *d, int ep)
{
	(void)d; return ep;
}
static inline void usb_fill_int_urb(struct urb *u, struct usb_device *d,
				    unsigned int pipe, void *buf,
				    int len, void (*fn)(struct urb *),
				    void *ctx, int interval)
{
	(void)d; (void)pipe; (void)fn; (void)interval;
	u->transfer_buffer = buf;
	u->transfer_buffer_length = len;
	u->context = ctx;
	u->status = 0;
}
static inline struct urb *usb_alloc_urb(int iso, int gfp)
{
	struct urb *u = calloc(1, sizeof(*u));

	(void)iso; (void)gfp;
	if (u)
		u->magic = 0x55524b;	/* "URB" */
	return u;
}
static inline void usb_free_urb(struct urb *u)
{
	stub_free_calls++;
	if (!u)
		return;
	if (u->magic == 0x55524b) {
		u->freed = 1;
		u->magic = 0xdead;
	}
	/* Leaked on purpose: the test keeps the pointer to probe it. */
}
static inline void usb_kill_urb(struct urb *u)
{
	if (!u)
		return;
	u->killed++;
	if (u->submitted)
		u->submitted--;
}
static inline void *usb_alloc_coherent(struct usb_device *d, size_t n,
				       int gfp, dma_addr_t *dma)
{
	void *p = malloc(n);

	(void)d; (void)gfp;
	if (dma)
		*dma = (dma_addr_t)(uintptr_t)p;
	if (p)
		memset(p, 0, n);
	return p;
}
static inline void usb_free_coherent(struct usb_device *d, size_t n,
				     void *p, dma_addr_t dma)
{
	(void)d; (void)n; (void)dma;
	stub_coherent_free++;
	free(p);
}

/* What the next usb_submit_urb() returns: 0, or an error to inject. */
extern int usb_submit_ret;
static inline int usb_submit_urb(struct urb *u, int gfp)
{
	(void)gfp;
	if (u && u->magic == 0xdead)
		stub_submit_after_free++;
	if (usb_submit_ret)
		return usb_submit_ret;
	u->submitted++;
	return 0;
}

/* ---- ALSA PCM stub ---- */
#define SNDRV_PCM_STREAM_PLAYBACK	0
#define SNDRV_PCM_STREAM_CAPTURE	1
#define SNDRV_PCM_STATE_OPEN		0
#define SNDRV_PCM_STATE_SETUP		1
#define SNDRV_PCM_STATE_PREPARED	2
#define SNDRV_PCM_STATE_RUNNING		3
#define SNDRV_PCM_STATE_XRUN		4
#define SNDRV_PCM_STATE_DRAINING	5
#define SNDRV_PCM_STATE_PAUSED		6
#define SNDRV_PCM_STATE_SUSPENDED	7
#define SNDRV_PCM_STATE_DISCONNECTED	8
typedef int snd_pcm_state_t;

#define SNDRV_PCM_INFO_MMAP		0x001
#define SNDRV_PCM_INFO_MMAP_VALID	0x002
#define SNDRV_PCM_INFO_INTERLEAVED	0x004
#define SNDRV_PCM_INFO_BLOCK_TRANSFER	0x008
#define SNDRV_PCM_FMTBIT_S32_LE		0x100

struct snd_pcm_hardware {
	unsigned int info;
	unsigned int formats;
	unsigned int rate_min, rate_max;
	unsigned int channels_min, channels_max;
	unsigned int buffer_bytes_max, period_bytes_max, periods_min, periods_max;
};
struct snd_pcm_hw_constraint_list;

#define SNDRV_PCM_TRIGGER_START		0
#define SNDRV_PCM_TRIGGER_STOP		1

struct snd_pcm_runtime {
	u8 *dma_area;
	unsigned long buffer_size;
	unsigned long period_size;
	unsigned int channels;
	unsigned int rate;		/* the rate the app negotiated */
	unsigned long delay;
	struct snd_pcm_hardware hw;
};
#define frames_to_bytes(rt, f)	((f) * (rt)->channels * 4)

struct snd_pcm_substream {
	struct snd_pcm_runtime *runtime;
	bool running;
	snd_pcm_state_t state;
	int stream;
	int periods_elapsed;
	void *chip;
};
#define snd_pcm_running(s)	((s)->running)
static inline void snd_pcm_period_elapsed(struct snd_pcm_substream *s)
{
	s->periods_elapsed++;
}
static inline void snd_pcm_stop(struct snd_pcm_substream *s,
				snd_pcm_state_t state)
{
	s->running = false;
	s->state = state;
}
static inline int snd_pcm_suspend_all(void *pcm) { (void)pcm; return 0; }
static inline void snd_card_disconnect(void *card) { (void)card; }
static inline void snd_card_free_when_closed(void *card) { (void)card; }
static inline void snd_card_free(void *card) { (void)card; }

/* hw-parameter negotiation: the test fills these in directly, so hw_params
 * reads back exactly what it asked for.
 */
struct snd_interval { unsigned int min, max; unsigned int integer; };
struct snd_pcm_hw_params {
	unsigned int rate;
	unsigned int period_size;
	unsigned int buffer_size;
	unsigned int channels;
};
#define SNDRV_PCM_HW_PARAM_RATE		1
#define SNDRV_PCM_HW_PARAM_PERIOD_SIZE	2
struct snd_pcm_hw_rule { void *private; };
struct snd_mask { int unused; };

static inline unsigned int params_rate(struct snd_pcm_hw_params *p)
{
	return p->rate;
}
static inline unsigned int params_period_size(struct snd_pcm_hw_params *p)
{
	return p->period_size;
}
static inline unsigned int params_buffer_size(struct snd_pcm_hw_params *p)
{
	return p->buffer_size;
}
static inline struct snd_interval *hw_param_interval(struct snd_pcm_hw_params *p,
						     int var)
{
	static struct snd_interval i;

	(void)p; (void)var;
	return &i;
}
static inline const struct snd_interval *
hw_param_interval_c(const struct snd_pcm_hw_params *p, int var)
{
	static struct snd_interval i;

	(void)p; (void)var;
	return &i;
}
static inline int snd_interval_refine(struct snd_interval *i,
				      const struct snd_interval *t)
{
	(void)t;
	return i ? 0 : -EINVAL;
}
static inline int snd_interval_list(struct snd_interval *i, unsigned int n,
				    const unsigned int *l, unsigned int m)
{
	(void)n; (void)l; (void)m;
	return i ? 0 : -EINVAL;
}
static inline int snd_pcm_hw_constraint_list(struct snd_pcm_runtime *r,
					     unsigned int c, int v,
					     const void *l)
{
	(void)r; (void)c; (void)v; (void)l;
	return 0;
}static inline int snd_pcm_hw_constraint_msbits(struct snd_pcm_runtime *r,
					       unsigned int c, unsigned int w,
					       unsigned int m)
{
	(void)r; (void)c; (void)w; (void)m;
	return 0;
}
static inline int snd_pcm_hw_constraint_minmax(struct snd_pcm_runtime *r,
					       int v, unsigned int lo,
					       unsigned int hi)
{
	(void)r; (void)v; (void)lo; (void)hi;
	return 0;
}
static inline int snd_pcm_hw_rule_add(struct snd_pcm_runtime *r,
				      unsigned int c, int v,
				      int (*fn)(struct snd_pcm_hw_params *,
						struct snd_pcm_hw_rule *),
				      void *p, int d, int m)
{
	(void)r; (void)c; (void)v; (void)fn; (void)p; (void)d; (void)m;
	return 0;
}

/* A fake "current" task: the owner[] bookkeeping compares tgids. */
extern int stub_tgid;
struct task_struct { int unused; };
#define current		(&stub_task)
extern struct task_struct stub_task;
#define task_tgid_nr(t)	((void)(t), stub_tgid)

/* Kernel-side rate table entry (babyfacepro.h); the driver's table and
 * the lookup functions are pulled in by extract_laws.py, the type is
 * declared here so those definitions compile.
 */
struct bf_rate {
	unsigned int rate;
	unsigned int alt;
	unsigned int frame_bytes;
	unsigned int min_fpu;
};

struct snd_pcm_hw_constraint_list {
	unsigned int count;
	const unsigned int *list;
	unsigned int mask;
};

/* Module parameters the extracted code pulls in with their own
 * definitions; the test sets the chip fields, so these are unused.
 */
#define BF_MODULE_PARAM __attribute__((unused))

/* The EQ strip state (babyfacepro.h). */
struct bf_eq_channel {
	bool on;
	s32 slope_db;
	s32 lc_hz;
	u32 lc_raw;
	u8 slope;
	s32 band_type[3];
	s32 band_freq[3];
	s32 band_q[3];
	s32 band_gain[3];
	s32 words[3][4];
	s32 shared;
};

/* A crosspoint source (babyfacepro.h). */
struct bf_source {
	const char *name;
	u8 idx_l;
	u8 idx_r;
};

/* ---- the driver state, under the driver's field names ---- */
struct snd_usb_babyface {
	struct usb_device *dev;
	struct usb_interface *iface;
	mutex_t mutex;
	spinlock_t lock;

	struct urb **urbs_in;
	struct urb **urbs_out;
	void **buf_in;
	void **buf_out;
	dma_addr_t *dma_in;
	dma_addr_t *dma_out;
	unsigned int nurbs;
	unsigned int frames_per_urb;
	unsigned int urb_frames_min;
	unsigned int urb_frames;
	unsigned int urbs_active;
	int session_dir;
	unsigned int session_gen;
	unsigned int subs_gen[2];
	unsigned int subs_pub_gen[2];
	unsigned int subs_rate[2];
	unsigned int subs_frame_bytes[2];
	unsigned int subs_urb_frames[2];
	unsigned int subs_urbs_active[2];
	unsigned int frame_bytes;
	unsigned int rate;
	unsigned int alt;
	int pitch;
	int stream_users;
	bool stream_setup[2];
	pid_t owner[2];
	bool streaming;
	ktime_t stream_stopped;
	bool shutdown;
	bool suspended;
	atomic_t torn_down;
	unsigned int slot;
	atomic_t urb_err;
	struct work_struct stream_work;
	struct snd_pcm_substream __rcu *subs[2];
	unsigned long hw_ptr[2];
	unsigned long prev_period[2];
	ktime_t in_done;
	unsigned int out_inflight;

	/* mixer state the extracted restore/reupload code touches */
	bool clock_optical;
	bool optical_spdif;
	bool eq_not_recorded;
	bool host_fx;
	struct bf_eq_channel eq[4];
	u8 gain[4];
	u16 preamp;
	int ref_level;
	bool linked, an12, ms_proc, dim;
	bool split[6], loopback[6];
	u8 flag_cnt;
	int trim[4];
	bool phase[4];
	u16 xpoint[6][14][2];
	u16 master[6][2];
	bool muted[6];
	s8 xwidth[14];
	u16 fxs[14];
	u16 fxr[6];
	u16 dim_steps;
	u16 dim_saved[2];
	bool main_mono;
	u8 cue;
};

static inline struct snd_usb_babyface *
snd_pcm_substream_chip(struct snd_pcm_substream *s)
{
	return s->chip;
}

#endif
