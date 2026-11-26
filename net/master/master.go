package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/DheerendraRathor/GoTracer/models"
	"github.com/DheerendraRathor/GoTracer/net/constants"
	"github.com/DheerendraRathor/GoTracer/utils"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"gopkg.in/cheggaaa/pb.v1"
)

var renderSpecFile string
var agentsFile string
var pongTimeout time.Duration
var pingPeriod time.Duration

func init() {
	flag.StringVar(&renderSpecFile, "spec", "sample_world.json", "Name of JSON file containing rendering spec")
	flag.StringVar(&agentsFile, "agents", "agents.json", "Name of JSON file containing list of agents")
	flag.DurationVar(&pongTimeout, "pongtimeout", 30*time.Second, "Max time to wait for an agent pong/message before declaring it dead")
	flag.DurationVar(&pingPeriod, "pingperiod", 10*time.Second, "Interval between heartbeat pings sent to an agent")
}

// patch is a column slice [x0, x1) of the final image. It is the unit of
// work assigned to (and re-assigned between) agents.
type patch struct {
	id       int
	x0, x1   int
	received int // pixels of this patch written so far (across all attempts)
	done     bool
}

func (p *patch) size(height int) int {
	return (p.x1 - p.x0) * height
}

type eventKind int

const (
	evPixel eventKind = iota
	evDead
	evCompleted
	evBusy
)

// agentEvent is one message received from an agent. All messages of a
// connection flow through a single channel in read order, so a
// completion can never overtake the pixels that precede it.
type agentEvent struct {
	kind  eventKind
	agent *agentConn
	pixel models.Pixel
	opId  string
}

// agentConn tracks one connected agent and its current assignment.
type agentConn struct {
	url     string
	cores   int
	conn    *websocket.Conn
	writeMu sync.Mutex
	done    chan struct{} // closed when the agent is torn down

	current *patch // nil when idle
	opId    string // operation id of the current assignment
}

func (a *agentConn) writeJSON(v interface{}) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return a.conn.WriteJSON(v)
}

// coordinator owns all mutable render state. Only its run loop touches
// the image, the written-pixel set and the patch/agent lists, so no
// locks are needed on that state.
type coordinator struct {
	env        models.Specification
	image      *image.RGBA
	written    []bool // width*height, true once a pixel has been written
	duplicates int

	patches []*patch
	pending []*patch
	agents  []*agentConn
	idle    []*agentConn

	inbox chan agentEvent

	progressBar *pb.ProgressBar
}

func (c *coordinator) width() int  { return c.env.Image.Width }
func (c *coordinator) height() int { return c.env.Image.Height }

// run drives the render task to completion and reports whether every
// patch was delivered. On false the task failed and no image must be
// written.
func (c *coordinator) run() bool {
	for {
		c.assignWork()

		if c.allDone() {
			return true
		}

		if len(c.agents) == 0 {
			// Nobody left to serve the remaining patches: stop waiting
			// and fail the task instead of emitting a partial image.
			log.Printf("No agents remain but %d patch(es) are still undelivered", len(c.pending))
			return false
		}

		select {
		case ev := <-c.inbox:
			c.handleEvent(ev)
		}
	}
}

func (c *coordinator) allDone() bool {
	for _, p := range c.patches {
		if !p.done {
			return false
		}
	}
	return true
}

// assignWork hands out pending patches to idle agents.
func (c *coordinator) assignWork() {
	for len(c.pending) > 0 && len(c.idle) > 0 {
		p := c.pending[0]
		c.pending = c.pending[1:]

		a := c.idle[0]
		c.idle = c.idle[1:]

		byteOpId, _ := uuid.NewRandom()
		opId := byteOpId.String()

		agentEnv := models.Specification(c.env)
		agentEnv.Image.Patch[2] = p.x0
		agentEnv.Image.Patch[3] = p.x1

		request := messages.RenderRequestMessage{
			Type:        messages.RenderRequest,
			Data:        agentEnv,
			OperationId: opId,
		}

		a.current = p
		a.opId = opId

		if err := a.writeJSON(request); err != nil {
			log.Printf("Unable to send render request to agent '%s': %s", a.url, err)
			a.current = nil
			c.requeue(p)
			c.removeAgent(a)
			continue
		}

		log.Printf("Assigned patch %d (columns [%d,%d)) to agent '%s' (round %s)", p.id, p.x0, p.x1, a.url, opId)
	}
}

func (c *coordinator) requeue(p *patch) {
	c.pending = append(c.pending, p)
}

func (c *coordinator) handlePixel(ev agentEvent) {
	a := ev.agent

	// Round check: pixels that do not belong to the round currently
	// assigned to this agent are late (or stale) and must be discarded.
	if a.current == nil || ev.opId != a.opId {
		log.Printf("Discarding late pixel from agent '%s' (round %s, current round %s)", a.url, ev.opId, a.opId)
		return
	}

	p := a.current
	pixel := ev.pixel

	// Bounds check: the pixel must belong to the assigned patch.
	if pixel.I < p.x0 || pixel.I >= p.x1 || pixel.J < 0 || pixel.J >= c.height() {
		log.Printf("Discarding out-of-patch pixel (%d,%d) from agent '%s'", pixel.I, pixel.J, a.url)
		return
	}

	idx := pixel.J*c.width() + pixel.I
	if c.written[idx] {
		// Each pixel is written exactly once; duplicates (e.g. from a
		// re-rendered patch) are dropped.
		c.duplicates++
		log.Printf("Duplicate pixel (%d,%d) from agent '%s' dropped", pixel.I, pixel.J, a.url)
		return
	}

	c.written[idx] = true
	p.received++
	c.progressBar.Increment()
	c.image.Set(pixel.I, pixel.J, color.RGBA{pixel.Color[0], pixel.Color[1], pixel.Color[2], 255})
}

func (c *coordinator) handleEvent(ev agentEvent) {
	a := ev.agent

	switch ev.kind {
	case evPixel:
		c.handlePixel(ev)

	case evCompleted:
		if a.current == nil || ev.opId != a.opId {
			log.Printf("Discarding late completion from agent '%s' (round %s)", a.url, ev.opId)
			return
		}
		p := a.current
		a.current = nil
		if p.received == p.size(c.height()) {
			p.done = true
			log.Printf("Agent '%s' delivered patch %d (columns [%d,%d))", a.url, p.id, p.x0, p.x1)
		} else {
			// Completion arrived without all pixels: the patch is not
			// delivered, re-queue the missing work.
			log.Printf("Agent '%s' completed patch %d with only %d/%d pixels; re-queueing",
				a.url, p.id, p.received, p.size(c.height()))
			c.requeue(p)
		}
		c.idle = append(c.idle, a)

	case evBusy:
		log.Printf("Agent '%s' rejected round %s as busy; re-queueing its patch and dropping the agent", a.url, ev.opId)
		if a.current != nil {
			c.requeue(a.current)
			a.current = nil
		}
		c.removeAgent(a)

	case evDead:
		log.Printf("Agent '%s' is unreachable or timed out", a.url)
		if a.current != nil {
			log.Printf("Re-assigning patch %d (columns [%d,%d)) from dead agent '%s'",
				a.current.id, a.current.x0, a.current.x1, a.url)
			c.requeue(a.current)
			a.current = nil
		}
		c.removeAgent(a)
	}
}

func (c *coordinator) removeAgent(a *agentConn) {
	for i, other := range c.agents {
		if other == a {
			c.agents = append(c.agents[:i], c.agents[i+1:]...)
			break
		}
	}
	for i, other := range c.idle {
		if other == a {
			c.idle = append(c.idle[:i], c.idle[i+1:]...)
			break
		}
	}
	select {
	case <-a.done:
	default:
		close(a.done)
	}
	a.conn.Close()
}

// readLoop consumes messages from one agent and forwards them to the
// coordinator. Any read error (disconnect, heartbeat timeout) declares
// the agent dead.
func (c *coordinator) readLoop(a *agentConn) {
	for {
		a.conn.SetReadDeadline(time.Now().Add(pongTimeout))
		_, rawMsg, err := a.conn.ReadMessage()
		if err != nil {
			c.inbox <- agentEvent{kind: evDead, agent: a}
			return
		}

		var message messages.WebSocketMessage
		if err := json.Unmarshal(rawMsg, &message); err != nil {
			continue
		}

		switch message.Type {
		case messages.PixelResult:
			var pixelMessage messages.PixelResultMessage
			if err := json.Unmarshal(rawMsg, &pixelMessage); err != nil {
				continue
			}
			c.inbox <- agentEvent{kind: evPixel, agent: a, pixel: pixelMessage.Data, opId: pixelMessage.OperationId}
		case messages.RenderingCompleted:
			c.inbox <- agentEvent{kind: evCompleted, agent: a, opId: message.OperationId}
		case messages.RenderRequestResponse:
			var response messages.RenderRequestResponseMessage
			if err := json.Unmarshal(rawMsg, &response); err != nil {
				continue
			}
			if response.Code == messages.RenderRequestTracingAlreadyInProgress {
				c.inbox <- agentEvent{kind: evBusy, agent: a, opId: response.OperationId}
			}
		}
	}
}

// pingLoop sends heartbeat pings until the agent is torn down.
func (c *coordinator) pingLoop(a *agentConn) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-ticker.C:
			a.writeMu.Lock()
			err := a.conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(10*time.Second))
			a.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func connectAgents(agentURLs []string) []*agentConn {
	connectedAgents := []*agentConn{}

	myClient := &http.Client{
		Timeout: time.Second * 10,
	}

	for _, agentURL := range agentURLs {
		wsURL := fmt.Sprintf("ws://%s", agentURL)
		httpURL := fmt.Sprintf("http://%s/status", agentURL)

		response, err := myClient.Get(httpURL)
		if err != nil {
			log.Printf("Agent '%s' is unavailable. \n\nError: %s\n", agentURL, err.Error())
			continue
		}

		var agentStatus messages.AgentStatus
		json.NewDecoder(response.Body).Decode(&agentStatus)
		response.Body.Close()

		if !agentStatus.Available {
			log.Printf("Agent '%s' is busy\n", agentURL)
			continue
		}

		var dialer *websocket.Dialer
		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Unable to dial ws connection to agent %s with error %s\n", agentURL, err)
			continue
		}

		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongTimeout))
		})

		connectedAgents = append(connectedAgents, &agentConn{
			url:   agentURL,
			cores: agentStatus.Cores,
			conn:  conn,
			done:  make(chan struct{}),
		})
	}

	return connectedAgents
}

// splitPatches divides the image columns into patches sized by the
// per-core share so each patch is a reasonable re-assignment unit.
func splitPatches(env models.Specification, availableCores int) []*patch {
	width := env.Image.Width
	linesPerCore := int(math.Ceil(float64(width) / float64(availableCores)))
	if linesPerCore < 1 {
		linesPerCore = 1
	}

	patches := []*patch{}
	for x0 := 0; x0 < width; x0 += linesPerCore {
		x1 := x0 + linesPerCore
		if x1 > width {
			x1 = width
		}
		patches = append(patches, &patch{id: len(patches), x0: x0, x1: x1})
	}
	return patches
}

func main() {
	flag.Parse()

	specFile, e := ioutil.ReadFile(renderSpecFile)
	if e != nil {
		panic(fmt.Sprintf("Render Spec File error: %v\n", e))
	}

	agentsFileContent, e := ioutil.ReadFile(agentsFile)
	if e != nil {
		panic(fmt.Sprintf("Agents list file error: %v\n", e))
	}

	agentURLs := make([]string, 0)
	json.Unmarshal(agentsFileContent, &agentURLs)

	var env models.Specification
	json.Unmarshal(specFile, &env)

	pngImage := image.NewRGBA(image.Rectangle{
		image.Point{0, 0},
		image.Point{env.Image.Width, env.Image.Height},
	})

	connectedAgents := connectAgents(agentURLs)

	if len(connectedAgents) == 0 {
		// Observable failure: non-zero exit, no image written.
		log.Println("RENDER FAILED: unable to connect to any agent")
		os.Exit(1)
	}

	availableCores := 0
	for _, a := range connectedAgents {
		availableCores += a.cores
	}

	total := env.Image.Width * env.Image.Height
	progressBar := pb.StartNew(total)
	progressBar.ShowFinalTime = true
	progressBar.ShowTimeLeft = false

	c := &coordinator{
		env:         env,
		image:       pngImage,
		written:     make([]bool, total),
		patches:     splitPatches(env, availableCores),
		agents:      connectedAgents,
		idle:        append([]*agentConn{}, connectedAgents...),
		inbox:       make(chan agentEvent, 1024),
		progressBar: progressBar,
	}
	c.pending = append([]*patch{}, c.patches...)

	for _, a := range connectedAgents {
		go c.readLoop(a)
		go c.pingLoop(a)
	}

	success := c.run()

	if !success {
		// Observable failure result: explicit message on stderr and a
		// non-zero exit code. The incomplete image is never written.
		progressBar.Finish()
		log.Println("RENDER FAILED: not all patches were delivered; no image written")
		os.Exit(1)
	}

	progressBar.Finish()

	pngFile := utils.CreateNestedFile(env.Image.OutputFile)
	defer pngFile.Close()

	if err := png.Encode(pngFile, pngImage); err != nil {
		log.Printf("RENDER FAILED: unable to encode PNG: %s", err)
		os.Exit(1)
	}

	log.Printf("RENDER SUCCEEDED: %d/%d pixels written (%d duplicate(s) dropped), image saved to %s",
		total-c.duplicates, total, c.duplicates, env.Image.OutputFile)
}
