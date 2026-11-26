package speech_to_text

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/asticode/go-astibob"
	"github.com/asticode/go-astibob/worker"
	"github.com/asticode/go-astikit"
	"github.com/go-audio/audio"
	"github.com/go-audio/wav"
	"github.com/julienschmidt/httprouter"
)

// Audio formats
const (
	audioFormatPCM = 1
)

// Message names
const (
	optionsBuildUpdatedMessage = "speech_to_text.options.build.updated"
	progressMessage            = "speech_to_text.progress"
	samplesMessage             = "speech_to_text.samples"
	speechCreatedMessage       = "speech_to_text.speech.created"
	speechDeletedMessage       = "speech_to_text.speech.deleted"
	speechUpdatedMessage       = "speech_to_text.speech.updated"
	textMessage                = "speech_to_text.text"
)

type Parser interface {
	Parse(samples []int, bitDepth, numChannels, sampleRate int) (string, error)
	Train(ctx context.Context, speeches []SpeechFile, progressFunc func(Progress))
}

type Progress struct {
	CurrentStep string   `json:"current_step"`
	Error       error    `json:"-"`
	Progress    float64  `json:"progress"` // In percentage
	Steps       []string `json:"steps"`
}

type ProgressJSON struct {
	Progress
	Done    bool   `json:"done,omitempty"`
	Error   string `json:"error,omitempty"`
	Session uint64 `json:"session,omitempty"`
}

func newProgressJSON(p Progress, session uint64, done bool) (pj *ProgressJSON) {
	pj = &ProgressJSON{
		Progress: p,
		Done:     done,
		Session:  session,
	}
	if p.Error != nil {
		pj.Error = astikit.ErrorCause(p.Error).Error()
	}
	return
}

// rootCtx returns ctx or a background context when ctx is nil
func rootCtx(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// clone returns a deep copy of the progress so that the parser can't mutate the state we store afterwards
func (p Progress) clone() Progress {
	o := p
	if p.Steps != nil {
		o.Steps = make([]string, len(p.Steps))
		copy(o.Steps, p.Steps)
	}
	return o
}

// done indicates whether the progress marks the end of a training session. A progress is only done once the last step
// has reached 100% so that intermediate steps (for instance a preparing step reaching 100%) don't end the session.
func (p *Progress) done() bool {
	if p.Error != nil {
		return true
	}
	if p.Progress < 100 {
		return false
	}
	return len(p.Steps) == 0 || p.CurrentStep == p.Steps[len(p.Steps)-1]
}

// position returns the position of the progress in its steps: the index of the current step and the percentage
// within that step
func (p *Progress) position() (idx int, pct float64) {
	idx = -1
	for i, s := range p.Steps {
		if s == p.CurrentStep {
			idx = i
			break
		}
	}
	return idx, p.Progress
}

// trainingSession represents a single training session. A runnable can have at most one at a time. Each session has
// its own context and progress so that late callbacks from a previous session can never leak into the current one.
type trainingSession struct {
	cancel     context.CancelFunc
	ctx        context.Context
	id         uint64
	lastIdx    int
	lastPct    float64
	progress   *Progress
	started    bool
	terminated bool
}

// accept makes sure the progress is monotonous within the session and records it if it is. It returns false when the
// progress would make the session go backwards.
func (s *trainingSession) accept(p Progress) bool {
	// Errors are always accepted since they mark the end of the session
	if p.Error != nil {
		return true
	}

	// Get position
	idx, pct := p.position()

	// First progress of the session
	if !s.started {
		s.started = true
		s.lastIdx, s.lastPct = idx, pct
		return true
	}

	// Progress must never go backwards within a session
	if idx > s.lastIdx || (idx == s.lastIdx && pct >= s.lastPct) {
		s.lastIdx, s.lastPct = idx, pct
		return true
	}
	return false
}

type Speech struct {
	CreatedAt   time.Time `json:"created_at"`
	IsValidated bool      `json:"is_validated"`
	Name        string    `json:"name"`
	Text        string    `json:"text"`
}

type SpeechFile struct {
	Path string `json:"path"`
	Text string `json:"text"`
}

type Runnable struct {
	*astibob.BaseOperatable
	*astibob.BaseRunnable
	b       *astikit.LimiterBucket
	c       *astikit.Chan
	i       *os.File
	l       astikit.SeverityLogger
	mp      *sync.Mutex // Locks session and sid
	ms      *sync.Mutex // Locks ss
	msd     *sync.Mutex // Locks sds
	o       RunnableOptions
	p       Parser
	sds     map[string]*astikit.PCMSilenceDetector
	session *trainingSession // Current training session, nil when no training is in progress
	sid     uint64           // Last training session id
	ss      map[string]*Speech
}

type RunnableOptions struct {
	SpeechesDirPath  string `toml:"speeches_dir_path"`
	StoreNewSpeeches bool   `toml:"store_new_speeches"`
}

func NewRunnable(name string, p Parser, l astikit.StdLogger, o RunnableOptions) *Runnable {
	// Create runnable
	r := &Runnable{
		c:   astikit.NewChan(astikit.ChanOptions{}),
		l:   astikit.AdaptStdLogger(l),
		mp:  &sync.Mutex{},
		ms:  &sync.Mutex{},
		msd: &sync.Mutex{},
		o:   o,
		p:   p,
		sds: make(map[string]*astikit.PCMSilenceDetector),
		ss:  make(map[string]*Speech),
	}

	// Add base operatable
	r.BaseOperatable = newBaseOperatable(r.l)

	// Add routes
	r.BaseOperatable.AddRoute("/options/build", http.MethodPatch, r.updateBuildOptions)
	r.BaseOperatable.AddRoute("/references/build", http.MethodGet, r.buildReferences)
	r.BaseOperatable.AddRoute("/references/train", http.MethodGet, r.trainReferences)
	r.BaseOperatable.AddRoute("/speeches/*path", http.MethodGet, astibob.DirHandle(r.o.SpeechesDirPath))
	r.BaseOperatable.AddRoute("/speeches/:name", http.MethodDelete, r.deleteSpeech)
	r.BaseOperatable.AddRoute("/speeches/:name", http.MethodPatch, r.updateSpeech)
	r.BaseOperatable.AddRoute("/train", http.MethodGet, r.train)
	r.BaseOperatable.AddRoute("/train/cancel", http.MethodGet, r.cancelTraining)

	// Set base runnable
	r.BaseRunnable = astibob.NewBaseRunnable(astibob.BaseRunnableOptions{
		Logger: l,
		Metadata: astibob.Metadata{
			Description: "Executes speech to text analysis when detecting silences in audio samples",
			Name:        name,
		},
		OnMessage: r.onMessage,
		OnStart:   r.onStart,
	})

	// Create progress limiter
	r.b = astikit.NewLimiter().Add("progress", 20, time.Second)
	return r
}

func (r *Runnable) Init() (err error) {
	// No speeches directory
	if r.o.SpeechesDirPath == "" {
		return
	}

	// Make sure speeches directory exists
	if err = os.MkdirAll(r.o.SpeechesDirPath, 0755); err != nil {
		err = fmt.Errorf("speech_to_text: mkdirall %s failed: %w", r.o.SpeechesDirPath, err)
		return
	}

	// Open index
	p := filepath.Join(r.o.SpeechesDirPath, "index.json")
	if r.i, err = os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		err = fmt.Errorf("speech_to_text: opening %s failed: %w", p, err)
		return
	}

	// Stat index
	var fi os.FileInfo
	if fi, err = r.i.Stat(); err != nil {
		err = fmt.Errorf("speech_to_text: stating %s failed: %w", p, err)
		return
	}

	// Unmarshal index
	if fi.Size() > 0 {
		r.ms.Lock()
		if err = json.NewDecoder(r.i).Decode(&r.ss); err != nil {
			err = fmt.Errorf("speech_to_text: unmarshaling failed: %w", err)
			return
		}
		r.ms.Unlock()
	}
	return
}

func (r *Runnable) Close() {
	// Close index
	if r.i != nil {
		r.i.Close()
	}

	// Close limiter
	if r.b != nil {
		r.b.Close()
	}
}

func (r *Runnable) onStart(ctx context.Context) (err error) {
	// Reset silence detectors
	r.msd.Lock()
	for _, sd := range r.sds {
		sd.Reset()
	}
	r.msd.Unlock()

	// Reset chan
	r.c.Reset()

	// Start chan
	r.c.Start(ctx)

	// Stop chan
	r.c.Stop()
	return
}

func (r *Runnable) onMessage(m *astibob.Message) (err error) {
	switch m.Name {
	case samplesMessage:
		if err = r.onSamples(m); err != nil {
			err = fmt.Errorf("speech_to_text: on samples failed: %w", err)
			return
		}
	}
	return
}

type Samples struct {
	BitDepth        int                `json:"bit_depth"`
	From            astibob.Identifier `json:"from"`
	MaxSilenceLevel float64            `json:"max_silence_level"`
	NumChannels     int                `json:"num_channels"`
	SampleRate      int                `json:"sample_rate"`
	Samples         []int              `json:"samples"`
}

func NewSamplesMessage(from astibob.Identifier, samples []int, bitDepth, numChannels, sampleRate int, maxSilenceLevel float64) worker.Message {
	return worker.Message{
		Name: samplesMessage,
		Payload: Samples{
			BitDepth:        bitDepth,
			From:            from,
			MaxSilenceLevel: maxSilenceLevel,
			NumChannels:     numChannels,
			SampleRate:      sampleRate,
			Samples:         samples,
		},
	}
}

func parseSamplesPayload(m *astibob.Message) (s Samples, err error) {
	if err = json.Unmarshal(m.Payload, &s); err != nil {
		err = fmt.Errorf("speech_to_text: unmarshaling failed: %w", err)
		return
	}
	return
}

func (r *Runnable) onSamples(m *astibob.Message) (err error) {
	// Check status
	if r.Status() != astibob.RunningStatus {
		return
	}

	// Parse payload
	var s Samples
	if s, err = parseSamplesPayload(m); err != nil {
		err = fmt.Errorf("speech_to_text: parsing payload failed: %w", err)
		return
	}

	// Invalid from
	if s.From.Type != astibob.RunnableIdentifierType || s.From.Name == nil || s.From.Worker == nil {
		err = errors.New("speech_to_text: invalid from")
		return
	}

	// Make sure samples processing is non blocking but still executed in FIFO order
	r.c.Add(r.samplesFunc(s))
	return
}

func (r *Runnable) samplesFunc(s Samples) func() {
	return func() {
		// Create silence detector key
		k := fmt.Sprintf("worker.%s.runnable.%s", *s.From.Worker, *s.From.Name)

		// Get silence detector
		r.msd.Lock()
		sd, ok := r.sds[k]
		if !ok {
			sd = astikit.NewPCMSilenceDetector(astikit.PCMSilenceDetectorOptions{
				MaxSilenceLevel: s.MaxSilenceLevel,
				SampleRate:      s.SampleRate,
			})
			r.sds[k] = sd
		}
		r.msd.Unlock()

		// Add samples to silence detector
		vss := sd.Add(s.Samples)

		// No valid samples
		if len(vss) == 0 {
			return
		}

		// Loop through valid samples
		for _, ss := range vss {
			// Normalize samples
			ss = astikit.PCMNormalize(ss, s.BitDepth)

			// Parse speech
			text, err := r.parseSpeech(s.From, ss, s.BitDepth, s.NumChannels, s.SampleRate)
			if err != nil {
				r.l.Error(fmt.Errorf("speech_to_text: parsing samples failed: %w", err))
			}

			// Store speech
			if r.o.StoreNewSpeeches && r.o.SpeechesDirPath != "" {
				if err := r.storeSpeech(text, ss, s.BitDepth, s.NumChannels, s.SampleRate); err != nil {
					r.l.Error(fmt.Errorf("speech_to_text: storing speech failed: %w", err))
				}
			}
		}
	}
}

type Text struct {
	From astibob.Identifier `json:"from"`
	Text string             `json:"text"`
}

func (r *Runnable) newTextMessage(from astibob.Identifier, text string) (m *astibob.Message, err error) {
	// Create message
	m = astibob.NewMessage()

	// Set name
	m.Name = textMessage

	// Marshal
	if m.Payload, err = json.Marshal(Text{
		From: from,
		Text: text,
	}); err != nil {
		err = fmt.Errorf("speech_to_text: marshaling payload failed: %w", err)
		return
	}
	return
}

func parseTextPayload(m *astibob.Message) (t Text, err error) {
	if err = json.Unmarshal(m.Payload, &t); err != nil {
		err = fmt.Errorf("speech_to_text: unmarshaling failed: %w", err)
		return
	}
	return
}

func (r *Runnable) parseSpeech(from astibob.Identifier, ss []int, bitDepth, numChannels, sampleRate int) (text string, err error) {
	// No parser
	if r.p == nil {
		return
	}

	// Parse
	r.l.Debugf("speech_to_text: parsing %d samples from runnable %s on worker %s", len(ss), *from.Name, *from.Worker)
	start := time.Now()
	if text, err = r.p.Parse(ss, bitDepth, numChannels, sampleRate); err != nil {
		err = fmt.Errorf("speech_to_text: parsing speech failed: %w", err)
		return
	}
	r.l.Debugf("speech_to_text: parsed %d samples from runnable %s on worker %s in %s", len(ss), *from.Name, *from.Worker, time.Since(start))

	// Dispatch text
	if text != "" {
		// Create message
		var m *astibob.Message
		if m, err = r.newTextMessage(from, text); err != nil {
			err = fmt.Errorf("speech_to_text: creating text message failed: %w", err)
			return
		}

		// Dispatch
		r.Dispatch(m)
	}
	return
}

func (r *Runnable) newSpeechCreatedMessage(s Speech) (m *astibob.Message, err error) {
	// Create message
	m = astibob.NewMessage()

	// Set name
	m.Name = speechCreatedMessage

	// Make sure the message is sent to the UI
	m.To = &astibob.Identifier{Type: astibob.UIIdentifierType}

	// Marshal
	if m.Payload, err = json.Marshal(s); err != nil {
		err = fmt.Errorf("speech_to_text: marshaling payload failed: %w", err)
		return
	}
	return
}

func (r *Runnable) saveIndex() (err error) {
	// Lock
	r.ms.Lock()
	defer r.ms.Unlock()

	// Truncate
	if err = r.i.Truncate(0); err != nil {
		err = fmt.Errorf("speech_to_text: truncating index failed: %w", err)
		return
	}

	// Seek
	if _, err = r.i.Seek(0, 0); err != nil {
		err = fmt.Errorf("speech_to_text: seeking in index failed: %w", err)
		return
	}

	// Marshal
	if err = json.NewEncoder(r.i).Encode(r.ss); err != nil {
		err = fmt.Errorf("speech_to_text: marshaling index failed: %w", err)
		return
	}
	return
}

func (r *Runnable) storeSpeech(text string, ss []int, bitDepth, numChannels, sampleRate int) (err error) {
	// Store speech to wav
	var s *Speech
	if s, err = r.storeSpeechToWav(ss, bitDepth, numChannels, sampleRate); err != nil {
		err = fmt.Errorf("speech_to_text: storing speech to wav failed: %w", err)
		return
	}

	// Update text
	s.Text = text

	// Add speech to index
	r.ms.Lock()
	r.ss[s.Name] = s
	r.ms.Unlock()

	// Save index
	if err = r.saveIndex(); err != nil {
		err = fmt.Errorf("speech_to_text: saving index failed: %w", err)
		return
	}

	// Create message
	var m *astibob.Message
	if m, err = r.newSpeechCreatedMessage(*s); err != nil {
		err = fmt.Errorf("speech_to_text: creating speech created message failed: %w", err)
		return
	}

	// Dispatch
	r.Dispatch(m)
	return
}

func (r *Runnable) storeSpeechToWav(ss []int, bitDepth, numChannels, sampleRate int) (s *Speech, err error) {
	// Create wav file
	var f *os.File
	if f, err = ioutil.TempFile(r.o.SpeechesDirPath, "*.wav"); err != nil {
		err = fmt.Errorf("speech_to_text: creating wav file failed: %w", err)
		return
	}
	defer f.Close()

	// Create speech
	s = &Speech{
		CreatedAt: time.Now(),
		Name:      strings.TrimSuffix(filepath.Base(f.Name()), ".wav"),
	}

	// Create encoder
	e := wav.NewEncoder(f, sampleRate, bitDepth, numChannels, audioFormatPCM)
	defer e.Close()

	// Write
	if err = e.Write(&audio.IntBuffer{
		Data: ss,
		Format: &audio.Format{
			NumChannels: numChannels,
			SampleRate:  sampleRate,
		},
		SourceBitDepth: bitDepth,
	}); err != nil {
		err = fmt.Errorf("speech_to_text: writing wav sample failed: %w", err)
		return
	}
	return
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (r *Runnable) orderedSpeeches(fn func(s Speech)) {
	// Lock
	r.ms.Lock()
	defer r.ms.Unlock()

	// Index speeches
	var ks int64Slice
	is := make(map[int64][]Speech)
	for _, s := range r.ss {
		if _, ok := is[s.CreatedAt.UnixNano()]; !ok {
			ks = append(ks, s.CreatedAt.UnixNano())
		}
		is[s.CreatedAt.UnixNano()] = append(is[s.CreatedAt.UnixNano()], *s)
	}

	// Sort keys
	sort.Sort(ks)

	// Loop through keys
	for _, k := range ks {
		// Loop through speeches
		for _, s := range is[k] {
			fn(s)
		}
	}
}

type BuildReferences struct {
	Options  BuildOptions `json:"options"`
	Speeches []Speech     `json:"speeches"`
}

type BuildOptions struct {
	StoreNewSpeeches bool `json:"store_new_speeches"`
}

func (r *Runnable) buildReferences(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	// Set content type
	rw.Header().Set("Content-Type", "application/json")

	// Create references
	rf := BuildReferences{
		Options: BuildOptions{
			StoreNewSpeeches: r.o.StoreNewSpeeches,
		},
		Speeches: []Speech{},
	}

	// Loop through ordered speeches
	r.orderedSpeeches(func(s Speech) { rf.Speeches = append(rf.Speeches, s) })

	// Write
	astibob.WriteHTTPData(r.l, rw, rf)
}

func (r *Runnable) updateBuildOptions(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	// Set content type
	rw.Header().Set("Content-Type", "application/json")

	// Parse body
	var b BuildOptions
	if err := json.NewDecoder(req.Body).Decode(&b); err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusBadRequest, fmt.Errorf("speech_to_text: parsing build options payload failed: %w", err))
		return
	}

	// Update options
	r.o.StoreNewSpeeches = b.StoreNewSpeeches

	// Create message
	m, err := r.newBuildOptionsUpdatedMessage()
	if err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusInternalServerError, fmt.Errorf("speech_to_text: creating build options updated message failed: %w", err))
		return
	}

	// Dispatch
	r.Dispatch(m)
}

func (r *Runnable) newBuildOptionsUpdatedMessage() (m *astibob.Message, err error) {
	// Create message
	m = astibob.NewMessage()

	// Set name
	m.Name = optionsBuildUpdatedMessage

	// Make sure the message is sent to the UI
	m.To = &astibob.Identifier{Type: astibob.UIIdentifierType}

	// Marshal
	if m.Payload, err = json.Marshal(BuildOptions{StoreNewSpeeches: r.o.StoreNewSpeeches}); err != nil {
		err = fmt.Errorf("speech_to_text: marshaling payload failed: %w", err)
		return
	}
	return
}

func (r *Runnable) newSpeechDeletedMessage(s Speech) (m *astibob.Message, err error) {
	// Create message
	m = astibob.NewMessage()

	// Set name
	m.Name = speechDeletedMessage

	// Make sure the message is sent to the UI
	m.To = &astibob.Identifier{Type: astibob.UIIdentifierType}

	// Marshal
	if m.Payload, err = json.Marshal(s); err != nil {
		err = fmt.Errorf("speech_to_text: marshaling payload failed: %w", err)
		return
	}
	return
}

func (r *Runnable) deleteSpeech(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	// Set content type
	rw.Header().Set("Content-Type", "application/json")

	// Get speech
	r.ms.Lock()
	s, ok := r.ss[p.ByName("name")]
	r.ms.Unlock()

	// No speech
	if !ok {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	// Delete wav
	wp := filepath.Join(r.o.SpeechesDirPath, s.Name+".wav")
	if err := os.Remove(wp); err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusInternalServerError, fmt.Errorf("speech_to_text: deleting %s failed: %w", wp, err))
		return
	}

	// Delete from index
	r.ms.Lock()
	delete(r.ss, s.Name)
	r.ms.Unlock()

	// Save index
	if err := r.saveIndex(); err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusInternalServerError, fmt.Errorf("speech_to_text: saving index failed: %w", err))
		return
	}

	// Create message
	m, err := r.newSpeechDeletedMessage(*s)
	if err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusInternalServerError, fmt.Errorf("speech_to_text: creating speech deleted message failed: %w", err))
		return
	}

	// Dispatch
	r.Dispatch(m)
}

func (r *Runnable) newSpeechUpdatedMessage(s Speech) (m *astibob.Message, err error) {
	// Create message
	m = astibob.NewMessage()

	// Set name
	m.Name = speechUpdatedMessage

	// Make sure the message is sent to the UI
	m.To = &astibob.Identifier{Type: astibob.UIIdentifierType}

	// Marshal
	if m.Payload, err = json.Marshal(s); err != nil {
		err = fmt.Errorf("speech_to_text: marshaling payload failed: %w", err)
		return
	}
	return
}

func (r *Runnable) updateSpeech(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	// Set content type
	rw.Header().Set("Content-Type", "application/json")

	// Get speech
	r.ms.Lock()
	s, ok := r.ss[p.ByName("name")]
	r.ms.Unlock()

	// No speech
	if !ok {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	// Unmarshal
	var b Speech
	if err := json.NewDecoder(req.Body).Decode(&b); err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusInternalServerError, fmt.Errorf("speech_to_text: unmarshaling failed: %w", err))
		return
	}

	// Update speech
	s.IsValidated = b.IsValidated
	s.Text = b.Text

	// Save index
	if err := r.saveIndex(); err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusInternalServerError, fmt.Errorf("speech_to_text: saving index failed: %w", err))
		return
	}

	// Create message
	m, err := r.newSpeechUpdatedMessage(*s)
	if err != nil {
		astibob.WriteHTTPError(r.l, rw, http.StatusInternalServerError, fmt.Errorf("speech_to_text: creating speech updated message failed: %w", err))
		return
	}

	// Dispatch
	r.Dispatch(m)
}

type TrainReferences struct {
	Progress *ProgressJSON `json:"progress,omitempty"`
}

func (r *Runnable) trainReferences(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	// Set content type
	rw.Header().Set("Content-Type", "application/json")

	// Create references
	rf := TrainReferences{}

	// Add progress
	r.mp.Lock()
	if r.session != nil {
		rf.Progress = newProgressJSON(*r.session.progress, r.session.id, false)
	}
	r.mp.Unlock()

	// Write
	astibob.WriteHTTPData(r.l, rw, rf)
}

func (r *Runnable) newProgressMessage(p Progress, session uint64, done bool) (m *astibob.Message, err error) {
	// Create message
	m = astibob.NewMessage()

	// Set name
	m.Name = progressMessage

	// Make sure the message is sent to the UI
	m.To = &astibob.Identifier{Type: astibob.UIIdentifierType}

	// Clone steps so that the dispatched payload can't be mutated by the parser afterwards
	p = p.clone()

	// Marshal
	if m.Payload, err = json.Marshal(newProgressJSON(p, session, done)); err != nil {
		err = fmt.Errorf("speech_to_text: marshaling payload failed: %w", err)
		return
	}
	return
}

// emitProgress dispatches the progress of the session to the UI. The last message of a session is never rate limited.
// r.mp must be held so that no progress message can be dispatched once the session has been terminated, which
// guarantees that the terminal message is always the last one delivered for a given session.
func (r *Runnable) emitProgress(session uint64, p Progress, done bool) {
	// Rate limit here in case Parser is spamming
	if !done && !r.b.Inc() {
		return
	}

	// Create message
	m, err := r.newProgressMessage(p, session, done)
	if err != nil {
		r.l.Error(fmt.Errorf("speech_to_text: creating progress message failed: %w", err))
		return
	}

	// Dispatch
	r.Dispatch(m)
}

func (r *Runnable) train(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	// Set content type
	rw.Header().Set("Content-Type", "application/json")

	// No parser
	if r.p == nil {
		return
	}

	// Lock
	r.mp.Lock()

	// Training in progress
	if r.session != nil {
		r.mp.Unlock()
		astibob.WriteHTTPError(r.l, rw, http.StatusBadRequest, errors.New("speech_to_text: training in progress"))
		return
	}

	// Create session
	r.sid++
	s := &trainingSession{
		id:       r.sid,
		progress: &Progress{},
	}
	s.ctx, s.cancel = context.WithCancel(rootCtx(r.RootCtx()))
	r.session = s

	// Unlock
	r.mp.Unlock()

	// Create speech files
	var fs []SpeechFile
	r.orderedSpeeches(func(s Speech) {
		if s.IsValidated {
			fs = append(fs, SpeechFile{
				Path: filepath.Join(r.o.SpeechesDirPath, s.Name+".wav"),
				Text: s.Text,
			})
		}
	})

	// Create task
	t := r.NewTask()

	// Execute the rest in a goroutine
	go func() {
		// Task is done
		defer t.Done()

		// Train
		r.p.Train(s.ctx, fs, r.progressFunc(s))

		// Make sure a parser returning without a terminal progress doesn't leave the session hanging
		r.finishSession(s)
	}()
}

// finishSession terminates the session if it's still the current one. It makes sure the UI always converges to a
// restartable state even when the parser returns without emitting a terminal progress.
func (r *Runnable) finishSession(s *trainingSession) {
	// Lock
	r.mp.Lock()
	defer r.mp.Unlock()

	// Session has already ended
	if s.terminated || r.session != s {
		return
	}

	// Terminate session
	s.terminated = true
	r.session = nil

	// Snapshot progress
	pg := Progress{}
	if s.progress != nil {
		pg = *s.progress
	}

	// Release resources
	s.cancel()

	// Dispatch terminal progress
	r.emitProgress(s.id, pg, true)
}

// progressFunc returns a progress callback bound to the provided session. Callbacks coming from a session that is not
// the current one are ignored so that late callbacks can never leak into a new session.
func (r *Runnable) progressFunc(s *trainingSession) func(Progress) {
	return func(p Progress) {
		// Lock
		r.mp.Lock()
		defer r.mp.Unlock()

		// Session has already ended: ignore anything coming from it
		if s.terminated || r.session != s {
			return
		}

		// Ignore non monotonic progress
		if !s.accept(p) {
			return
		}

		// Update progress
		*s.progress = p.clone()

		// Progress is done
		done := p.done()
		if done {
			// Log error
			if p.Error != nil {
				r.l.Error(fmt.Errorf("speech_to_text: training failed: %w", p.Error))
			}

			// Terminate session
			s.terminated = true
			r.session = nil

			// Release resources
			s.cancel()
		}

		// Dispatch
		r.emitProgress(s.id, p, done)
	}
}

func (r *Runnable) cancelTraining(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	// Set content type
	rw.Header().Set("Content-Type", "application/json")

	// Lock
	r.mp.Lock()
	defer r.mp.Unlock()

	// No training in progress: succeed without side effect
	if r.session == nil {
		return
	}

	// Terminate session
	s := r.session
	s.terminated = true
	r.session = nil

	// Keep a snapshot of the session's progress for the terminal message
	pg := Progress{}
	if s.progress != nil {
		pg = *s.progress
	}

	// Cancel context
	s.cancel()

	// Make sure the end of the session reaches the UI and that it converges to a restartable state
	r.emitProgress(s.id, pg, true)
}
