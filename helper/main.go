package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: airplay-helper serve|stop ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "stop":
		fs := flag.NewFlagSet("stop", flag.ExitOnError)
		out := fs.String("out", "", "output dir of the instance to stop")
		token := fs.String("token", "", "cast token; when set, only that cast is stopped (a newer cast is left alone)")
		fs.Parse(os.Args[2:])
		if *out == "" {
			fmt.Fprintln(os.Stderr, "missing required flag: -out")
			os.Exit(2)
		}
		if err := StopCast(*out, *token, handoverGrace); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

func runServe(argv []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	var c JobConfig
	var parent int
	var ipOverride, rangeHint, token string
	fs.StringVar(&c.Source, "source", "", "media file")
	fs.StringVar(&c.OutDir, "out", "", "HLS output dir")
	fs.StringVar(&c.FFmpeg, "ffmpeg", "", "ffmpeg binary")
	fs.IntVar(&parent, "parent", 0, "IINA pid to watch")
	fs.StringVar(&ipOverride, "ip", "", "LAN IPv4 address to advertise (auto-detected by default)")
	fs.Float64Var(&c.Duration, "duration", 0, "source duration seconds")
	fs.StringVar(&c.VCodec, "vcodec", "", "source video codec (mpv name)")
	fs.StringVar(&c.ACodec, "acodec", "", "source audio codec (mpv name)")
	fs.IntVar(&c.VDepth, "vdepth", 0, "source video bit depth (0 = unknown)")
	fs.IntVar(&c.VHeight, "vheight", 0, "source video height in pixels (0 = unknown)")
	fs.IntVar(&c.AChannels, "achannels", 2, "audio channel count")
	fs.IntVar(&c.VMap, "vmap", 0, "video stream ff-index")
	fs.IntVar(&c.AMap, "amap", 1, "audio stream ff-index")
	fs.StringVar(&c.SubMap, "smap", "", "embedded subtitle stream ff-index (empty = no subtitles)")
	fs.StringVar(&c.SubPath, "subpath", "", "external subtitle file (used as input 1; overrides -smap)")
	fs.StringVar(&c.SubLang, "sublang", "", "subtitle language tag for the HLS rendition")
	fs.StringVar(&c.SubName, "subname", "", "subtitle display name for the HLS rendition")
	fs.StringVar(&rangeHint, "vrange", "", "PQ or HLG when the player decodes an HDR transfer; used for the master's VIDEO-RANGE when init.mp4 has no colr box")
	fs.StringVar(&token, "token", "", "opaque id for this cast, recorded in the pidfile so a stale stop cannot hit a newer cast")
	fs.Parse(argv)

	proto := NewProtocol(os.Stdout, token)

	// Registered before anything that can take time: a SIGTERM during the
	// handover or server start must end in the normal teardown (which sweeps and
	// releases the directory) rather than the default disposition, which would
	// leave the work half-done and no event stream to say so. Buffered so the
	// signal is still seen once the serve loop below starts reading.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	// lock owns the output directory; pidfile is this instance's claim on it.
	// outOwned flips only once AcquireCastDir has handed the directory over —
	// before that the files there may belong to a cast another instance is
	// still serving, and this instance must not touch them. ownPidfile alone
	// says whether the pidfile on disk was written by us: a failed takeover
	// must leave the predecessor's records exactly as it found them.
	var lock *DirLock
	var pidfile string
	outOwned, ownPidfile := false, false
	// stopJob is replaced once ffmpeg is running; fail can run before that.
	stopJob := func() {}
	// shutdownServer likewise, once the HTTP server is up.
	shutdownServer := func() {}
	// teardown runs exactly once, from whichever exit path gets there first
	// (signal, parent watchdog, job failure). Sealing the protocol first is
	// what stops this cast's progress and "packaged" events from leaking into
	// the next one: a superseded instance narrates nothing after it is told to
	// stop.
	var teardownOnce sync.Once
	teardown := func() {
		teardownOnce.Do(func() {
			// Seal first: from here on this cast narrates nothing but its own
			// ending, so a superseded instance's progress/packaged events cannot
			// land in the next cast's log.
			proto.Seal()
			stopJob()
			shutdownServer()
			if outOwned {
				cleanOutDir(c.OutDir)
				os.Remove(filepath.Join(c.OutDir, jobPidName))
			}
			if ownPidfile {
				os.Remove(pidfile)
			}
			// Released last: until this happens a succeeding cast cannot take
			// the directory, so it cannot clean files this instance's ffmpeg
			// might still be flushing, and this instance's server cannot be
			// serving a directory a new ffmpeg is already writing.
			lock.Release()
		})
	}
	fail := func(msg string) {
		// Clean up before reporting: by the time the plugin sees the error the
		// directory is swept and the lock released, so "Try again" can start a
		// new cast at once instead of waiting out the handover grace. The error
		// is terminal, so the seal teardown just took does not swallow it.
		teardown()
		proto.Emit("error", map[string]any{"msg": msg})
		os.Exit(1)
	}

	if c.Source == "" || c.OutDir == "" || c.FFmpeg == "" || parent == 0 {
		fail("missing required flags: -source, -out, -ffmpeg, -parent")
	}
	if _, err := os.Stat(c.FFmpeg); err != nil {
		fail("ffmpeg not found at " + c.FFmpeg + " — reinstall the plugin through IINA's plugin installer")
	}
	pidfile = filepath.Join(c.OutDir, pidfileName)

	// Handover. AcquireCastDir waits for the previous instance to release the
	// directory (or fails if it doesn't within the grace), then sweeps: every
	// file left is from a cast that is no longer writing, so nothing here can
	// be mistaken for this cast's output.
	var err error
	lock, err = AcquireCastDir(c.OutDir, token, handoverGrace)
	if err != nil {
		fail("could not take over the output directory: " + err.Error())
	}
	outOwned = true
	if err := WritePidfile(pidfile, token); err != nil {
		fail(err.Error())
	}
	ownPidfile = true

	var srcSize int64
	if fi, serr := os.Stat(c.Source); serr == nil {
		srcSize = fi.Size()
	}
	port, shutdown, err := StartServer(c.OutDir, c.SubName, c.SubLang, AdvertisedBandwidth(c, srcSize), rangeHint)
	if err != nil {
		fail(err.Error())
	}
	shutdownServer = shutdown

	ip, err := LanIP(ipOverride)
	if err != nil {
		fail(err.Error())
	}

	var done <-chan error
	stopJob, done = RunJob(c, proto)

	WatchParent(parent, func() {
		teardown()
		os.Exit(0)
	})

	// The TV can start pulling as soon as the advertised playlist exists.
	// With subs that's ffmpeg's master playlist, and the playlists it points
	// at must exist too — the master names the variant and the subtitle
	// rendition, and the TV 404s on either if it is handed the URL before
	// both have landed. Ready is latched: once announced it is never
	// re-evaluated, so no later poll, cleanup or ffmpeg exit can take the
	// announced stream away.
	playlistName := c.PlaylistName()
	readyURL := fmt.Sprintf("http://%s:%d/%s", ip, port, playlistName)
	readyDeadline := time.Now().Add(120 * time.Second)
	readyTick := time.NewTicker(200 * time.Millisecond)
	defer readyTick.Stop()
	ready := false
	announce := func() bool {
		if missing := MissingPlaylists(c.OutDir, c); len(missing) > 0 {
			return false
		}
		proto.Emit("ready", map[string]any{"url": readyURL})
		ready = true
		readyTick.Stop()
		return true
	}
	// stopCast is every non-error way out: tear the whole cast down (ffmpeg
	// stopped, directory swept, server shut, lock released), then announce it.
	// The order is the contract — the plugin acts on "stopped" by offering a new
	// cast, and a successor must find the directory already free to take.
	stopCast := func() {
		teardown()
		proto.Emit("stopped", nil)
	}
	for {
		select {
		case <-readyTick.C:
			if !ready && !announce() && time.Now().After(readyDeadline) {
				fail("packaging produced no playlist within 120s")
			}
		case err := <-done:
			if err != nil {
				fail(err.Error())
			}
			if !ready && !announce() {
				fail("ffmpeg finished but produced no playlist")
			}
			// Keep serving completed VOD until killed.
			<-sigs
			stopCast()
			return
		case <-sigs:
			stopCast()
			return
		}
	}
}
