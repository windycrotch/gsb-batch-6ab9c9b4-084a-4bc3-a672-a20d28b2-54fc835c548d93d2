package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/DheerendraRathor/GoTracer/models"
	"github.com/DheerendraRathor/GoTracer/net/constants"
	"github.com/DheerendraRathor/GoTracer/tracer"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	// Agent side heartbeat: if no pong is received within pongTimeout the
	// master is considered gone and the connection (and its render round)
	// is torn down.
	pongTimeout = 60 * time.Second
	pingPeriod  = 20 * time.Second
)

// renderRound is a single rendering round served by this agent. A round
// owns its own result channel so pixels produced by a previous round can
// never leak into the stream of a newer one.
type renderRound struct {
	operationId string
	results     chan *models.Pixel
	cancel      chan struct{}
	cancelOnce  sync.Once
	done        chan struct{} // closed when the round's forwarder exits
}

func (r *renderRound) cancelRound() {
	r.cancelOnce.Do(func() {
		close(r.cancel)
	})
}

type RenderingClient struct {
	Conn    *websocket.Conn
	writeMu sync.Mutex

	mu     sync.Mutex
	round  *renderRound // nil when idle
	closed chan struct{}
}

func (c *RenderingClient) writeJSON(v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.WriteJSON(v)
}

// currentRound returns the active round, or nil if the agent is idle.
func (c *RenderingClient) currentRound() *renderRound {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.round
}

// clearRound marks the agent idle, but only if the given round is still
// the active one (a newer round may already have replaced it).
func (c *RenderingClient) clearRound(round *renderRound) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.round == round {
		c.round = nil
	}
}

// stopActiveRound cancels the active round (if any) and waits until its
// forwarder goroutine has exited, so no goroutine or pixel of the old
// round survives the connection.
func (c *RenderingClient) stopActiveRound() {
	round := c.currentRound()
	if round == nil {
		return
	}
	round.cancelRound()
	<-round.done
}

func (c *RenderingClient) Initialize() {
	c.Conn.SetPongHandler(
		func(string) error {
			return c.Conn.SetReadDeadline(time.Now().Add(pongTimeout))
		},
	)
	c.Conn.SetReadDeadline(time.Now().Add(pongTimeout))

	go c.pingLoop()

	c.readLoop()

	// The connection is gone: the current round must terminate instead of
	// leaking its tracer goroutines or blocking the next connection.
	c.stopActiveRound()
}

func (c *RenderingClient) pingLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			c.writeMu.Lock()
			err := c.Conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(10*time.Second))
			c.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (c *RenderingClient) readLoop() {
	var message messages.WebSocketMessage
	var renderReqMsg messages.RenderRequestMessage
	for {
		_, msgStr, err := c.Conn.ReadMessage()
		if err != nil {
			log.Printf("Connection closed: %s", err)
			return
		}

		if err := json.Unmarshal(msgStr, &message); err != nil {
			log.Printf("Unable to understand message: %s", err)
			continue
		}

		if message.Type == messages.RenderRequest {
			if err := json.Unmarshal(msgStr, &renderReqMsg); err != nil {
				log.Printf("Unable to parse render request: %s", err)
				continue
			}
			c.handleRenderRequest(&renderReqMsg)
		}
	}
}

func (c *RenderingClient) handleRenderRequest(renderReqMsg *messages.RenderRequestMessage) {
	operationId := renderReqMsg.OperationId
	if operationId == "" {
		byteOpId, _ := uuid.NewRandom()
		operationId = byteOpId.String()
	}

	responseMessage := messages.RenderRequestResponseMessage{
		Type:        messages.RenderRequestResponse,
		OperationId: operationId,
	}

	c.mu.Lock()
	if c.round != nil {
		c.mu.Unlock()
		// Exactly one round at a time: reject the new request and keep
		// serving the current round.
		responseMessage.Code = messages.RenderRequestTracingAlreadyInProgress
		if err := c.writeJSON(responseMessage); err != nil {
			log.Printf("Unable to send busy response: %s", err)
		}
		return
	}

	round := &renderRound{
		operationId: operationId,
		results:     make(chan *models.Pixel, 1024),
		cancel:      make(chan struct{}),
		done:        make(chan struct{}),
	}
	c.round = round
	c.mu.Unlock()

	responseMessage.Code = messages.RenderRequestAccepted
	if err := c.writeJSON(responseMessage); err != nil {
		log.Printf("Unable to send accept response: %s", err)
		round.cancelRound()
		c.clearRound(round)
		close(round.done)
		return
	}

	spec := renderReqMsg.Data
	go tracer.GoTrace(&spec, true, round.results, true, round.cancel)
	go c.forwardRound(round)
}

// forwardRound streams the pixels of exactly one round to the master,
// each tagged with the round's operation id. It exits when the round
// completes or is cancelled; pixels of any other round never enter this
// stream.
func (c *RenderingClient) forwardRound(round *renderRound) {
	defer close(round.done)
	defer c.clearRound(round)

	for {
		select {
		case pixel := <-round.results:
			if pixel == nil {
				completion := messages.WebSocketMessage{
					Type:        messages.RenderingCompleted,
					OperationId: round.operationId,
				}
				if err := c.writeJSON(completion); err != nil {
					log.Printf("Unable to send completion: %s", err)
				}
				return
			}

			if pixelDelay > 0 {
				time.Sleep(pixelDelay)
			}

			pixelMessage := messages.PixelResultMessage{
				Type:        messages.PixelResult,
				Data:        *pixel,
				OperationId: round.operationId,
			}
			if err := c.writeJSON(pixelMessage); err != nil {
				log.Printf("Unable to send pixel: %s", err)
				round.cancelRound()
				return
			}
		case <-round.cancel:
			return
		}
	}
}

var addr = flag.String("addr", "", "http service address")
var pixelDelayFlag = flag.Int("pixeldelay", 0, "artificial delay in milliseconds before sending each pixel (testing hook)")

var pixelDelay time.Duration

var upgrader = websocket.Upgrader{}
var mutex = &sync.Mutex{}

var activeConn *websocket.Conn = nil

func agentHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received a connection request")

	mutex.Lock()
	if activeConn != nil {
		mutex.Unlock()
		log.Print("An active connection already exists")
		http.Error(w, "An active connection already exists", http.StatusConflict)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		mutex.Unlock()
		log.Print("upgrade:", err)
		return
	}
	activeConn = conn
	mutex.Unlock()

	defer func() {
		mutex.Lock()
		activeConn = nil
		mutex.Unlock()
		conn.Close()
	}()

	client := RenderingClient{
		Conn:   conn,
		closed: make(chan struct{}),
	}
	defer close(client.closed)

	client.Initialize()
}

func agentStatusHandler(w http.ResponseWriter, r *http.Request) {
	mutex.Lock()
	available := activeConn == nil
	mutex.Unlock()

	status := messages.AgentStatus{
		Available: available,
		Cores:     runtime.NumCPU(),
	}

	jsonStatus, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonStatus)
}

func main() {
	flag.Parse()

	if *addr == "" {
		flag.Usage()
		log.Fatalf("Agent address \"%s\" is invalid.", *addr)
	}

	pixelDelay = time.Duration(*pixelDelayFlag) * time.Millisecond

	log.SetFlags(0)
	http.HandleFunc("/", agentHandler)
	http.HandleFunc("/status", agentStatusHandler)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
