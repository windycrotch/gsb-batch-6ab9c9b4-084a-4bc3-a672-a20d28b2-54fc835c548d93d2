package dca

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jonas747/ogg"
)

// AudioApplication is an application profile for opus encoding
type AudioApplication string

var (
	AudioApplicationVoip     AudioApplication = "voip"     // Favor improved speech intelligibility.
	AudioApplicationAudio    AudioApplication = "audio"    // Favor faithfulness to the input
	AudioApplicationLowDelay AudioApplication = "lowdelay" // Restrict to only the lowest delay modes.
)

var (
	ErrBadFrame = errors.New("Bad Frame")

	// ErrNotRunning is returned by Stop when the session has already been
	// stopped or has already finished on its own.
	ErrNotRunning = errors.New("Encode session is not running")

	// ErrSessionStopped is returned by internal writers when the session has
	// been stopped while they were trying to publish a frame.
	ErrSessionStopped = errors.New("Encode session stopped")
)

// EncodeOptions is a set of options for encoding dca
type EncodeOptions struct {
	Volume           int              // change audio volume (256=normal)
	Channels         int              // audio channels
	FrameRate        int              // audio sampling rate (ex 48000)
	FrameDuration    int              // audio frame duration can be 20, 40, or 60 (ms)
	Bitrate          int              // audio encoding bitrate in kb/s can be 8 - 128
	PacketLoss       int              // expected packet loss percentage
	RawOutput        bool             // Raw opus output (no metadata or magic bytes)
	Application      AudioApplication // Audio application
	CoverFormat      string           // Format the cover art will be encoded with (ex "jpeg)
	CompressionLevel int              // Compression level, higher is better qualiy but slower encoding (0 - 10)
	BufferedFrames   int              // How big the frame buffer should be
	VBR              bool             // Wether vbr is used or not (variable bitrate)
	Threads          int              // Number of threads to use, 0 for auto
	StartTime        int              // Start Time of the input stream in seconds

	// The ffmpeg audio filters to use, see https://ffmpeg.org/ffmpeg-filters.html#Audio-Filters for more info
	// Leave empty to use no filters.
	AudioFilter string

	Comment string // Leave a comment in the metadata
}

func (e EncodeOptions) PCMFrameLen() int {
	// DCA needs this
	return 960 * e.Channels * (e.FrameDuration / 20)
}

// Validate returns an error if the options are not correct
func (opts *EncodeOptions) Validate() error {
	if opts.Volume < 0 || opts.Volume > 512 {
		return errors.New("Out of bounds volume (0-512)")
	}

	if opts.FrameDuration != 20 && opts.FrameDuration != 40 && opts.FrameDuration != 60 {
		return errors.New("Invalid FrameDuration")
	}

	if opts.PacketLoss < 0 || opts.PacketLoss > 100 {
		return errors.New("Invalid packet loss percentage")
	}

	if opts.Application != AudioApplicationAudio && opts.Application != AudioApplicationVoip && opts.Application != AudioApplicationLowDelay {
		return errors.New("Invalid audio application")
	}

	if opts.CompressionLevel < 0 || opts.CompressionLevel > 10 {
		return errors.New("Compression level out of bounds (0-10)")
	}

	if opts.Threads < 0 {
		return errors.New("Number of threads can't be less than 0")
	}

	if opts.BufferedFrames < 0 {
		return errors.New("Number of buffered frames can't be less than 0")
	}

	return nil
}

// StdEncodeOptions is the standard options for encoding
var StdEncodeOptions = &EncodeOptions{
	Volume:           256,
	Channels:         2,
	FrameRate:        48000,
	FrameDuration:    20,
	Bitrate:          64,
	Application:      AudioApplicationAudio,
	CompressionLevel: 10,
	PacketLoss:       1,
	BufferedFrames:   100, // At 20ms frames that's 2s
	VBR:              true,
	StartTime:        0,
}

// EncodeStats is transcode stats reported by ffmpeg
type EncodeStats struct {
	Size     int
	Duration time.Duration
	Bitrate  float32
	Speed    float32
}

type Frame struct {
	data     []byte
	metaData bool
}

// EncodeSession is a single transcode session.
//
// The session has exactly one observable outcome: either the frames of the
// input are produced until ffmpeg is done (readers see io.EOF), or the session
// is stopped through Stop/Cleanup (readers see io.EOF as well, and the ffmpeg
// process has been reaped before Cleanup returns).
//
// Ownership rules that make that guarantee hold:
//   - The run goroutine is the only goroutine that sends to and closes
//     frameChannel, and it closes it exactly once when it is completely done.
//   - All sends select on stopCh, so a stop never leaves a writer blocked
//     forever and never sends into a closed channel.
//   - All receives select on stopCh as well, so a blocked ReadFrame can never
//     outlive a stop.
type EncodeSession struct {
	sync.Mutex
	options    *EncodeOptions
	pipeReader io.Reader
	filePath   string

	running bool
	stopped bool
	started time.Time

	frameChannel chan *Frame

	// stopCh is closed exactly once, by the first Stop call.
	stopCh chan struct{}
	// doneCh is closed by the run goroutine once ffmpeg has been reaped and
	// frameChannel has been closed.
	doneCh chan struct{}

	process *os.Process
	// cancel aborts the context the ffprobe/cover helper processes are started
	// with. The ffmpeg process itself is killed synchronously by Stop. May be
	// nil before the run goroutine gets going.
	cancel context.CancelFunc
	// stdin is the write side of ffmpeg's stdin when encoding from memory.
	stdin io.WriteCloser

	lastStats *EncodeStats

	lastFrame int
	err       error

	ffmpegOutput string

	// buffer that stores unread bytes (not full frames)
	// used to implement io.Reader
	buf bytes.Buffer
}

// EncodedMem encodes data from memory
func EncodeMem(r io.Reader, options *EncodeOptions) (session *EncodeSession, err error) {
	err = options.Validate()
	if err != nil {
		return
	}

	session = newEncodeSession(options)
	session.pipeReader = r
	go session.run()
	return
}

// EncodeFile encodes the file/url/other in path
func EncodeFile(path string, options *EncodeOptions) (session *EncodeSession, err error) {
	err = options.Validate()
	if err != nil {
		return
	}

	session = newEncodeSession(options)
	session.filePath = path
	go session.run()
	return
}

func newEncodeSession(options *EncodeOptions) *EncodeSession {
	return &EncodeSession{
		options:      options,
		frameChannel: make(chan *Frame, options.BufferedFrames),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		// The session counts as running from the moment it is created: the run
		// goroutine is about to be started and Stop must be able to cancel it
		// even if it hasn't reached its first statement yet.
		running: true,
	}
}

func (e *EncodeSession) run() {
	// The run goroutine owns frameChannel and doneCh. Closing them here, in a
	// single place, is what makes "the session is over" unambiguous.
	defer func() {
		e.Lock()
		e.running = false
		e.Unlock()

		close(e.frameChannel)
		close(e.doneCh)
	}()

	e.Lock()
	options := e.options
	if options == nil {
		options = StdEncodeOptions
		e.options = options
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.Unlock()
	defer cancel()

	inFile := "pipe:0"
	if e.filePath != "" {
		inFile = e.filePath
	}

	vbrStr := "on"
	if !options.VBR {
		vbrStr = "off"
	}

	// Launch ffmpeg with a variety of different fruits and goodies mixed togheter
	args := []string{
		"-stats",
		"-i", inFile,
		"-reconnect", "1",
		"-reconnect_at_eof", "1",
		"-reconnect_streamed", "1",
		"-reconnect_delay_max", "2",
		"-map", "0:a",
		"-acodec", "libopus",
		"-f", "ogg",
		"-vbr", vbrStr,
		"-compression_level", strconv.Itoa(options.CompressionLevel),
		"-ar", strconv.Itoa(options.FrameRate),
		"-ac", strconv.Itoa(options.Channels),
		"-b:a", strconv.Itoa(options.Bitrate * 1000),
		"-application", string(options.Application),
		"-frame_duration", strconv.Itoa(options.FrameDuration),
		"-packet_loss", strconv.Itoa(options.PacketLoss),
		"-threads", strconv.Itoa(options.Threads),
		"-ss", strconv.Itoa(options.StartTime),
	}

	// Apply the volume as part of the audio filter chain. The old -vol option
	// has been removed from recent ffmpeg releases, this works on all of them.
	filters := make([]string, 0, 2)
	if options.Volume != 256 {
		filters = append(filters, fmt.Sprintf("volume=%.6f", float64(options.Volume)/256))
	}
	if options.AudioFilter != "" {
		filters = append(filters, options.AudioFilter)
	}
	if len(filters) > 0 {
		args = append(args, "-af", strings.Join(filters, ","))
	}

	args = append(args, "pipe:1")

	// Deliberately not exec.CommandContext: the session drives the lifetime of
	// the process itself (Stop kills it synchronously), so there must not be a
	// second, asynchronous killer racing with it. The context below is only used
	// for the short-lived ffprobe/cover helper processes.
	ffmpeg := exec.Command("ffmpeg", args...)

	var stdin io.WriteCloser
	if e.pipeReader != nil {
		var err error
		stdin, err = ffmpeg.StdinPipe()
		if err != nil {
			logln("StdinPipe Error:", err)
			e.setErr(err)
			return
		}
	}

	stdout, err := ffmpeg.StdoutPipe()
	if err != nil {
		logln("StdoutPipe Error:", err)
		e.setErr(err)
		return
	}

	stderr, err := ffmpeg.StderrPipe()
	if err != nil {
		logln("StderrPipe Error:", err)
		e.setErr(err)
		return
	}

	// Starts the ffmpeg command
	err = ffmpeg.Start()
	if err != nil {
		logln("RunStart Error:", err)
		e.setErr(err)
		return
	}

	e.Lock()
	e.started = time.Now()
	e.process = ffmpeg.Process
	e.stdin = stdin
	e.Unlock()

	// A stop may have been requested while we were starting up; if so the
	// process must not be allowed to keep running.
	if e.isStopped() {
		_ = ffmpeg.Process.Kill()
		_ = ffmpeg.Wait()
		return
	}

	if stdin != nil {
		// Copy the input in a goroutine of our own: cmd.Wait would otherwise
		// block on an io.Reader that never returns, keeping Cleanup hanging.
		go func() {
			_, _ = io.Copy(stdin, e.pipeReader)
			_ = stdin.Close()
		}()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go e.readStderr(stderr, &wg)

	// The metadata frame is written only once ffmpeg is actually running, so a
	// failed start can't hand the caller a metadata frame without any audio,
	// and it is always written before any audio frame.
	if !options.RawOutput {
		e.writeMetadataFrame(ctx, options)
	}

	if err := e.readStdout(stdout); err != nil && !e.isStopped() {
		e.setErr(err)
		// Tear the process down so Wait below can't block on live pipes. The
		// process was started without a context, so kill it explicitly.
		_ = ffmpeg.Process.Kill()
	}

	wg.Wait()
	err = ffmpeg.Wait()
	if err != nil && !e.isStopped() {
		e.setErr(err)
	}
}

func (e *EncodeSession) setErr(err error) {
	if err == nil {
		return
	}
	e.Lock()
	if e.err == nil {
		e.err = err
	}
	e.Unlock()
}

// isStopped reports wether a stop has been requested for this session.
func (e *EncodeSession) isStopped() bool {
	if e.stopCh == nil {
		return false
	}
	select {
	case <-e.stopCh:
		return true
	default:
		return false
	}
}

// sendFrame hands a frame to the readers. It never blocks past a stop and it
// never sends on a channel that has already been closed (the run goroutine is
// the only closer, and it stops sending before it closes).
func (e *EncodeSession) sendFrame(f *Frame) error {
	if e.stopCh == nil {
		e.frameChannel <- f
		return nil
	}

	// Prefer reporting the stop over queueing one more frame.
	select {
	case <-e.stopCh:
		return ErrSessionStopped
	default:
	}

	select {
	case <-e.stopCh:
		return ErrSessionStopped
	case e.frameChannel <- f:
		return nil
	}
}

func (e *EncodeSession) writeMetadataFrame(ctx context.Context, options *EncodeOptions) {
	// Setup the metadata
	metadata := Metadata{
		Dca: &DCAMetadata{
			Version: FormatVersion,
			Tool: &DCAToolMetadata{
				Name:    "dca",
				Version: LibraryVersion,
				Url:     GitHubRepositoryURL,
				Author:  "jonas747",
			},
		},
		Opus: &OpusMetadata{
			Bitrate:     options.Bitrate * 1000,
			SampleRate:  options.FrameRate,
			Application: string(options.Application),
			FrameSize:   options.PCMFrameLen(),
			Channels:    options.Channels,
			VBR:         options.VBR,
		},
		SongInfo: &SongMetadata{},
		Origin:   &OriginMetadata{},
		Extra:    &ExtraMetadata{},
	}

	var cmdBuf bytes.Buffer
	// get ffprobe data
	if e.pipeReader == nil {
		// Everything in here is best effort: if ffprobe or the cover extraction
		// fails we still emit the metadata frame, the fields are just left
		// empty. The frame itself must be there exactly once either way.
		e.populateFileMetadata(ctx, options, &metadata, &cmdBuf)
	} else {
		metadata.Origin = &OriginMetadata{
			Source:   "pipe",
			Channels: options.Channels,
		}
	}

	// Write the magic header
	jsonData, err := json.Marshal(metadata)
	if err != nil {
		logln("JSon error:", err)
		return
	}
	var buf bytes.Buffer
	buf.Write([]byte(fmt.Sprintf("DCA%d", FormatVersion)))

	// Write the actual json data and length
	jsonLen := int32(len(jsonData))
	err = binary.Write(&buf, binary.LittleEndian, &jsonLen)
	if err != nil {
		logln("Couldn't write json len:", err)
		return
	}

	buf.Write(jsonData)
	if err := e.sendFrame(&Frame{buf.Bytes(), true}); err != nil {
		// The session was stopped before the metadata could be delivered;
		// dropping it is correct, there will be no audio either.
		return
	}
}

// populateFileMetadata fills in the song/origin/cover fields from ffprobe.
// Failures are logged and ignored: the metadata frame is still sent, just with
// less information in it. Nothing in here may prevent the frame from being
// emitted, because it has to be there exactly once.
func (e *EncodeSession) populateFileMetadata(ctx context.Context, options *EncodeOptions, metadata *Metadata, cmdBuf *bytes.Buffer) {
	// Sensible defaults for a file source; ffprobe refines them below.
	metadata.SongInfo = &SongMetadata{Comments: options.Comment}
	metadata.Origin = &OriginMetadata{
		Source:   "file",
		Channels: options.Channels,
	}

	if e.isStopped() {
		return
	}

	ffprobe := exec.CommandContext(ctx, "ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", e.filePath)
	ffprobe.Stdout = cmdBuf

	err := ffprobe.Start()
	if err != nil {
		logln("RunStart Error:", err)
		return
	}

	err = ffprobe.Wait()
	if err != nil {
		logln("FFprobe Error:", err)
		return
	}
	var ffprobeData *FFprobeMetadata
	err = json.Unmarshal(cmdBuf.Bytes(), &ffprobeData)
	if err != nil {
		logln("Erorr unmarshaling the FFprobe JSON:", err)
		return
	}
	if ffprobeData == nil {
		return
	}

	if ffprobeData.Format == nil {
		ffprobeData.Format = &FFprobeFormat{}
	}

	if ffprobeData.Format.Tags == nil {
		ffprobeData.Format.Tags = &FFprobeTags{}
	}

	metadata.SongInfo = &SongMetadata{
		Title:    ffprobeData.Format.Tags.Title,
		Artist:   ffprobeData.Format.Tags.Artist,
		Album:    ffprobeData.Format.Tags.Album,
		Genre:    ffprobeData.Format.Tags.Genre,
		Comments: options.Comment,
	}

	metadata.Origin = &OriginMetadata{
		Source:   "file",
		Channels: options.Channels,
		Encoding: ffprobeData.Format.FormatLongName,
	}

	// bit_rate is missing for some containers (raw ogg and such); that is not
	// a reason to drop the rest of the metadata.
	if bitrateInt, err := strconv.Atoi(ffprobeData.Format.Bitrate); err == nil {
		metadata.Origin.Bitrate = bitrateInt
	} else {
		logln("Could not convert bitrate to int:", err)
	}

	cmdBuf.Reset()

	if e.isStopped() {
		return
	}

	// get cover art
	cover := exec.CommandContext(ctx, "ffmpeg", "-loglevel", "0", "-i", e.filePath, "-f", "singlejpeg", "pipe:1")
	cover.Stdout = cmdBuf

	err = cover.Start()
	if err != nil {
		logln("RunStart Error:", err)
		return
	}
	var pngBuf bytes.Buffer
	err = cover.Wait()
	if err == nil && cmdBuf.Len() > 0 {
		buf := bytes.NewBufferString(cmdBuf.String())
		var coverImage string
		if options.CoverFormat == "png" {
			img, err := jpeg.Decode(buf)
			if err == nil { // silently drop it, no image
				err = png.Encode(&pngBuf, img)
				if err == nil {
					coverImage = base64.StdEncoding.EncodeToString(pngBuf.Bytes())
				}
			}
		} else {
			coverImage = base64.StdEncoding.EncodeToString(cmdBuf.Bytes())
		}

		metadata.SongInfo.Cover = &coverImage
	}

	cmdBuf.Reset()
	pngBuf.Reset()
}

func (e *EncodeSession) readStderr(stderr io.ReadCloser, wg *sync.WaitGroup) {
	defer wg.Done()

	bufReader := bufio.NewReader(stderr)
	var outBuf bytes.Buffer
	for {
		r, _, err := bufReader.ReadRune()
		if err != nil {
			if err != io.EOF && !e.isStopped() {
				logln("Error Reading stderr:", err)
			}
			break
		}

		// Is this the best way to distinguish stats from messages?
		switch r {
		case '\r':
			// Stats line
			if outBuf.Len() > 0 {
				e.handleStderrLine(outBuf.String())
				outBuf.Reset()
			}
		case '\n':
			// Message
			e.Lock()
			e.ffmpegOutput += outBuf.String() + "\n"
			e.Unlock()
			outBuf.Reset()
		default:
			outBuf.WriteRune(r)
		}
	}
}

func (e *EncodeSession) handleStderrLine(line string) {
	if strings.Index(line, "size=") != 0 {
		return // Not stats info
	}

	// The stats line format changed over ffmpeg versions. Older releases print
	//   size=    1234kB time=00:00:15.06 bitrate=  51.7kbits/s speed=14.1x
	// while newer ones (ffmpeg 6+) print
	//   size=      95KiB time=00:00:15.06 bitrate=  51.7kbits/s speed=14.1x elapsed=0:00:00.10
	// and may omit bitrate/speed entirely, so parse field by field.
	stats := &EncodeStats{}
	recognised := false
	fields := strings.Fields(line)
	for i := 0; i < len(fields); i++ {
		key, value, ok := strings.Cut(fields[i], "=")
		if !ok {
			continue
		}
		if value == "" && i+1 < len(fields) {
			// Padding after '=' ("size=      95KiB") splits key and value
			// into two tokens; rejoin them.
			i++
			value = fields[i]
		}

		switch key {
		case "size":
			// Strip the unit (kB, KiB, ...).
			num := strings.TrimRight(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
			if size, err := strconv.Atoi(num); err == nil {
				stats.Size = size
				recognised = true
			}
		case "time":
			stats.Duration = parseFFmpegDuration(value)
			recognised = true
		case "bitrate":
			num := strings.TrimSuffix(value, "kbits/s")
			if b, err := strconv.ParseFloat(num, 32); err == nil {
				stats.Bitrate = float32(b)
			}
		case "speed":
			num := strings.TrimSuffix(value, "x")
			if s, err := strconv.ParseFloat(num, 32); err == nil {
				stats.Speed = float32(s)
			}
		}
	}

	if !recognised {
		// ffmpeg also emits a bare "size=" line when it shuts down, don't log
		// that as a parsing error.
		if strings.TrimSpace(line) != "size=" {
			logln("Error parsing ffmpeg stats: unrecognised stats line:", line)
		}
		return
	}

	e.Lock()
	e.lastStats = stats
	e.Unlock()
}

// parseFFmpegDuration parses the "HH:MM:SS.mmm" form used in ffmpeg stats.
func parseFFmpegDuration(value string) time.Duration {
	parts := strings.Split(value, ":")
	if len(parts) != 3 {
		return 0
	}

	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0
	}
	minutes, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0
	}
	seconds, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return 0
	}

	dur := time.Duration(hours) * time.Hour
	dur += time.Duration(minutes) * time.Minute
	dur += time.Duration(seconds * float64(time.Second))
	return dur
}

func (e *EncodeSession) readStdout(stdout io.ReadCloser) error {
	decoder := ogg.NewPacketDecoder(ogg.NewDecoder(stdout))

	// the first 2 packets are ogg opus metadata
	skipPackets := 2
	for {
		if e.isStopped() {
			return nil
		}

		// Retrieve a packet
		packet, _, err := decoder.Decode()
		if err != nil {
			if err == io.EOF || e.isStopped() {
				return nil
			}
			return err
		}

		if skipPackets > 0 {
			skipPackets--
			continue
		}

		err = e.writeOpusFrame(packet)
		if err != nil {
			if errors.Is(err, ErrSessionStopped) {
				return nil
			}
			return err
		}
	}
}

func (e *EncodeSession) writeOpusFrame(opusFrame []byte) error {
	var dcaBuf bytes.Buffer

	err := binary.Write(&dcaBuf, binary.LittleEndian, int16(len(opusFrame)))
	if err != nil {
		return err
	}

	_, err = dcaBuf.Write(opusFrame)
	if err != nil {
		return err
	}

	if err := e.sendFrame(&Frame{dcaBuf.Bytes(), false}); err != nil {
		return err
	}

	e.Lock()
	e.lastFrame++
	e.Unlock()

	return nil
}

// Stop stops the encoding session.
//
// It is safe to call Stop at any point in the session's lifetime and from any
// goroutine. Readers blocked in ReadFrame/OpusFrame/Read are woken up and
// return io.EOF. The ffmpeg process is killed; Cleanup waits for it to be
// reaped.
//
// Stop returns ErrNotRunning when the session was already stopped (or had
// already finished).
func (e *EncodeSession) Stop() error {
	e.Lock()
	if e.stopCh == nil || e.stopped || !e.running {
		e.Unlock()
		return ErrNotRunning
	}

	e.stopped = true
	e.running = false
	process := e.process
	cancel := e.cancel
	stdin := e.stdin
	e.Unlock()

	// Closing stopCh is what unblocks every pending reader and writer.
	close(e.stopCh)

	// Kill before cancelling the context: the context's own kill is
	// asynchronous, and racing it makes Kill report "Access is denied" on
	// Windows for a process that is merely already terminating.
	var killErr error
	if process != nil {
		killErr = process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}
	}

	if stdin != nil {
		_ = stdin.Close()
	}
	if cancel != nil {
		cancel()
	}

	if process == nil {
		// The run goroutine hasn't started ffmpeg yet; it will notice the
		// stop as soon as it does and kill the process itself.
		return nil
	}

	if killErr != nil {
		// On Windows, a process that has already exited but not been reaped
		// yet can make Kill fail with "Access is denied". Give the run
		// goroutine a moment to reap it and report success in that case;
		// the session is stopped either way, and returning a spurious error
		// would confuse callers that treat Stop's error as fatal.
		select {
		case <-e.doneCh:
			return nil
		case <-time.After(2 * time.Second):
		}
	}

	return killErr
}

// nextFrame returns the next frame produced by the session.
//
// The result is deterministic once the session has been stopped: a reader that
// was blocked in this call, or that calls it later, always observes io.EOF,
// never a frame belonging to a previous position of the session.
func (e *EncodeSession) nextFrame() (*Frame, error) {
	if e.frameChannel == nil {
		return nil, io.EOF
	}

	// Deterministic behaviour once stopped: no stale frames from an earlier
	// position of the session are handed out.
	if e.isStopped() {
		return nil, io.EOF
	}

	if e.stopCh == nil {
		f := <-e.frameChannel
		if f == nil {
			return nil, io.EOF
		}
		return f, nil
	}

	select {
	case <-e.stopCh:
		return nil, io.EOF
	case f, ok := <-e.frameChannel:
		// The stop may have landed while this select was racing, so check
		// again before handing the frame out.
		if e.isStopped() {
			return nil, io.EOF
		}
		if !ok || f == nil {
			return nil, io.EOF
		}
		return f, nil
	}
}

// ReadFrame blocks until a frame is read or there are no more frames
// Note: If rawoutput is not set, the first frame will be a metadata frame
func (e *EncodeSession) ReadFrame() (frame []byte, err error) {
	f, err := e.nextFrame()
	if err != nil {
		return nil, err
	}

	return f.data, nil
}

// OpusFrame implements OpusReader, returning the next opus frame
func (e *EncodeSession) OpusFrame() (frame []byte, err error) {
	for {
		f, err := e.nextFrame()
		if err != nil {
			return nil, err
		}

		if f.metaData {
			// Skip the metadata frame(s) and return the next audio frame.
			continue
		}

		if len(f.data) < 2 {
			return nil, ErrBadFrame
		}

		return f.data[2:], nil
	}
}

// Running returns true if running
func (e *EncodeSession) Running() (running bool) {
	e.Lock()
	running = e.running
	e.Unlock()
	return
}

// Stats returns ffmpeg stats, NOTE: this is not playback stats but transcode stats.
// To get how far into playback you are
// you have to track the number of frames sent to discord youself
func (e *EncodeSession) Stats() *EncodeStats {
	s := &EncodeStats{}
	e.Lock()
	if e.lastStats != nil {
		*s = *e.lastStats
	}
	e.Unlock()

	return s
}

// Options returns the options used
func (e *EncodeSession) Options() *EncodeOptions {
	return e.options
}

// Truncate is deprecated, use Cleanup instead
// this will be removed in a future version
func (e *EncodeSession) Truncate() {
	e.Cleanup()
}

// Cleanup cleans up the encoding session, throwing away all unread frames and
// stopping ffmpeg, ensuring that no ffmpeg processes starts piling up on your
// system. You should always call this after it's done.
//
// Cleanup blocks until the ffmpeg process has been reaped and the frame channel
// has been closed, then drains whatever is left. It is safe to call Cleanup
// repeatedly and concurrently; it never panics and never deadlocks.
func (e *EncodeSession) Cleanup() {
	if e.frameChannel == nil {
		return
	}

	// ErrNotRunning just means the session already finished on its own.
	_ = e.Stop()

	// Wait for the run goroutine: it reaps ffmpeg and closes the channel.
	if e.doneCh != nil {
		<-e.doneCh
	}

	// Drain whatever is left so no frame buffers are retained.
	for range e.frameChannel {
		// empty till closed
		// Cats can be right-pawed or left-pawed.
	}
}

// Read implements io.Reader,
// n == len(p) if err == nil, otherwise n contains the number bytes read before an error occured
func (e *EncodeSession) Read(p []byte) (n int, err error) {
	if e.buf.Len() >= len(p) {
		return e.buf.Read(p)
	}

	for e.buf.Len() < len(p) {
		f, err := e.ReadFrame()
		if err != nil {
			break
		}
		e.buf.Write(f)
	}

	return e.buf.Read(p)
}

// FrameDuration implements OpusReader, retruning the duratio of each frame
func (e *EncodeSession) FrameDuration() time.Duration {
	return time.Duration(e.options.FrameDuration) * time.Millisecond
}

// Error returns any error that occured during the encoding process
func (e *EncodeSession) Error() error {
	e.Lock()
	defer e.Unlock()
	return e.err
}

// FFMPEGMessages returns messages printed by ffmpeg to stderr, you can use this to see what ffmpeg is saying if your encoding fails
func (e *EncodeSession) FFMPEGMessages() string {
	e.Lock()
	output := e.ffmpegOutput
	e.Unlock()
	return output
}
