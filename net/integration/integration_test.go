package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image/png"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DheerendraRathor/GoTracer/models"
	"github.com/DheerendraRathor/GoTracer/net/constants"
	"github.com/gorilla/websocket"
)

var (
	masterBin string
	agentBin  string
)

// syncBuffer is a goroutine-safe bytes.Buffer for capturing process output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) Contains(s string) bool {
	return strings.Contains(b.String(), s)
}

func TestMain(m *testing.M) {
	// Build master and agent binaries once for all scenarios.
	tmp, err := os.MkdirTemp("", "gotracer-bins")
	if err != nil {
		fmt.Println("unable to create temp dir:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	exeSuffix := ""
	if runtime.GOOS == "windows" {
		exeSuffix = ".exe"
	}
	masterBin = filepath.Join(tmp, "master"+exeSuffix)
	agentBin = filepath.Join(tmp, "agent"+exeSuffix)

	repoRoot := filepath.Join("..", "..")
	for _, build := range []struct{ out, pkg string }{
		{masterBin, "./net/master"},
		{agentBin, "./net/agent"},
	} {
		cmd := exec.Command("go", "build", "-o", build.out, build.pkg)
		cmd.Dir = repoRoot
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Printf("unable to build %s: %s\n%s\n", build.pkg, err, out)
			os.Exit(1)
		}
	}

	os.Exit(m.Run())
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to reserve a port: %s", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

// writeSpec renders a small scene spec into dir and returns its path.
func writeSpec(t *testing.T, dir string, width, height, samples int, outputFile string) string {
	t.Helper()
	spec := fmt.Sprintf(`{
  "Settings": {"RenderRoutines": 2, "RenderDepth": 3},
  "Image": {"OutputFile": %q, "Width": %d, "Height": %d, "Samples": %d, "Patch": [0, %d, 0, %d]},
  "Scene": {
    "Camera": {"LookFrom": [0, 0, 1], "LookAt": [0, 0, -1], "UpVector": [0, 1, 0], "FieldOfView": 45, "AspectRatio": 1.5, "Focus": 2.0, "Aperture": 0.0},
    "Objects": {"Spheres": [
      {"Center": [0, 0, -1], "Radius": 0.5, "Surface": {"Type": "Lambertian", "Albedo": [0.8, 0.3, 0.3], "Fuzz": 0.0, "RefIndex": 1.0}}
    ]},
    "AmbientLight": [0.2, 0.2, 0.2]
  }
}`, outputFile, width, height, samples, height, width)

	path := filepath.Join(dir, "spec.json")
	if err := os.WriteFile(path, []byte(spec), 0644); err != nil {
		t.Fatalf("unable to write spec: %s", err)
	}
	return path
}

func writeAgentsFile(t *testing.T, dir string, addrs ...string) string {
	t.Helper()
	content, err := json.Marshal(addrs)
	if err != nil {
		t.Fatalf("unable to marshal agents: %s", err)
	}
	path := filepath.Join(dir, "agents.json")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatalf("unable to write agents file: %s", err)
	}
	return path
}

type runningProcess struct {
	cmd    *exec.Cmd
	output *syncBuffer
	done   chan error
}

func startProcess(t *testing.T, name string, args ...string) *runningProcess {
	t.Helper()
	cmd := exec.Command(name, args...)
	output := &syncBuffer{}
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("unable to start %s: %s", name, err)
	}
	p := &runningProcess{cmd: cmd, output: output, done: make(chan error, 1)}
	go func() {
		p.done <- cmd.Wait()
	}()
	return p
}

// waitExit waits for the process to exit and returns its exit code.
func waitExit(t *testing.T, p *runningProcess, timeout time.Duration, name string) int {
	t.Helper()
	select {
	case err := <-p.done:
		if err == nil {
			return 0
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		t.Fatalf("unexpected wait error for %s: %s", name, err)
	case <-time.After(timeout):
		p.cmd.Process.Kill()
		t.Fatalf("%s did not exit within %s.\nOutput:\n%s", name, timeout, p.output.String())
	}
	return -1
}

func (p *runningProcess) kill() {
	if p.cmd.Process != nil {
		p.cmd.Process.Kill()
	}
}

func startAgent(t *testing.T, addr string, extraArgs ...string) *runningProcess {
	t.Helper()
	args := append([]string{"-addr", addr}, extraArgs...)
	return startProcess(t, agentBin, args...)
}

func startMaster(t *testing.T, specFile, agentsFile string) *runningProcess {
	t.Helper()
	return startProcess(t, masterBin,
		"-spec", specFile,
		"-agents", agentsFile,
		"-pongtimeout", "3s",
		"-pingperiod", "1s",
	)
}

// assertCompletePNG decodes the PNG and requires every pixel to be fully
// opaque: an unwritten pixel would have alpha 0.
func assertCompletePNG(t *testing.T, path string, width, height int) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("expected output image %s to exist: %s", path, err)
	}
	defer file.Close()

	img, err := png.Decode(file)
	if err != nil {
		t.Fatalf("unable to decode PNG %s: %s", path, err)
	}

	bounds := img.Bounds()
	if bounds.Dx() != width || bounds.Dy() != height {
		t.Fatalf("unexpected image dimensions: got %dx%d, want %dx%d", bounds.Dx(), bounds.Dy(), width, height)
	}

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			_, _, _, a := img.At(x, y).RGBA()
			if a != 0xffff {
				t.Fatalf("pixel (%d,%d) was never written (alpha=%d)", x, y, a)
			}
		}
	}
}

func waitForAgentReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(fmt.Sprintf("http://%s/status", addr))
		if err == nil {
			var status messages.AgentStatus
			json.NewDecoder(response.Body).Decode(&status)
			response.Body.Close()
			if status.Available {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("agent %s did not become ready", addr)
}

// Scenario 1: normal path. All patches are delivered, the final image is
// complete and every pixel is written exactly once.
func TestNormalCompletion(t *testing.T) {
	dir := t.TempDir()
	width, height := 24, 16
	outputFile := filepath.Join(dir, "out.png")
	spec := writeSpec(t, dir, width, height, 1, outputFile)

	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	agents := make([]*runningProcess, len(addrs))
	for i, addr := range addrs {
		agents[i] = startAgent(t, addr)
		defer agents[i].kill()
		waitForAgentReady(t, addr)
	}

	agentsFile := writeAgentsFile(t, dir, addrs...)
	master := startMaster(t, spec, agentsFile)

	code := waitExit(t, master, 60*time.Second, "master")
	if code != 0 {
		t.Fatalf("master exited with code %d, want 0.\nOutput:\n%s", code, master.output.String())
	}
	if !master.output.Contains("RENDER SUCCEEDED") {
		t.Fatalf("master did not report success.\nOutput:\n%s", master.output.String())
	}
	if master.output.Contains("Duplicate pixel") {
		t.Fatalf("a pixel was written more than once.\nOutput:\n%s", master.output.String())
	}
	assertCompletePNG(t, outputFile, width, height)
}

// Scenario 2: one agent dies mid-render. The master must re-assign its
// patch to the surviving agents and still deliver a complete image.
func TestAgentDisconnectMidRender(t *testing.T) {
	dir := t.TempDir()
	width, height := 32, 24
	outputFile := filepath.Join(dir, "out.png")
	spec := writeSpec(t, dir, width, height, 1, outputFile)

	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	agents := make([]*runningProcess, len(addrs))
	for i, addr := range addrs {
		// Slow the pixel stream down so the kill lands mid-render.
		agents[i] = startAgent(t, addr, "-pixeldelay", "25")
		defer agents[i].kill()
		waitForAgentReady(t, addr)
	}

	agentsFile := writeAgentsFile(t, dir, addrs...)
	master := startMaster(t, spec, agentsFile)

	// Wait until rendering is underway, then kill the first agent.
	deadline := time.Now().Add(20 * time.Second)
	for !master.output.Contains("Assigned patch") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	t.Log("killing agent", addrs[0])
	agents[0].kill()

	code := waitExit(t, master, 120*time.Second, "master")
	if code != 0 {
		t.Fatalf("master exited with code %d, want 0.\nOutput:\n%s", code, master.output.String())
	}
	if !master.output.Contains("Re-assigning patch") {
		t.Fatalf("master did not re-assign the dead agent's patch.\nOutput:\n%s", master.output.String())
	}
	if !master.output.Contains("RENDER SUCCEEDED") {
		t.Fatalf("master did not report success.\nOutput:\n%s", master.output.String())
	}
	assertCompletePNG(t, outputFile, width, height)
}

// Scenario 3a: no agent is reachable at all. The task must fail and no
// image may be written.
func TestAllAgentsUnavailable(t *testing.T) {
	dir := t.TempDir()
	width, height := 16, 16
	outputFile := filepath.Join(dir, "out.png")
	spec := writeSpec(t, dir, width, height, 1, outputFile)

	agentsFile := writeAgentsFile(t, dir, freeAddr(t), freeAddr(t))
	master := startMaster(t, spec, agentsFile)

	code := waitExit(t, master, 30*time.Second, "master")
	if code == 0 {
		t.Fatalf("master exited with code 0, want non-zero.\nOutput:\n%s", master.output.String())
	}
	if !master.output.Contains("RENDER FAILED") {
		t.Fatalf("master did not report an observable failure.\nOutput:\n%s", master.output.String())
	}
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		t.Fatalf("output image %s must not exist after a failed render", outputFile)
	}
}

// Scenario 3b: all agents die mid-render. The master must stop waiting,
// fail the task and not write an image.
func TestAllAgentsDieMidRender(t *testing.T) {
	dir := t.TempDir()
	width, height := 32, 24
	outputFile := filepath.Join(dir, "out.png")
	spec := writeSpec(t, dir, width, height, 1, outputFile)

	addrs := []string{freeAddr(t), freeAddr(t)}
	agents := make([]*runningProcess, len(addrs))
	for i, addr := range addrs {
		agents[i] = startAgent(t, addr, "-pixeldelay", "25")
		waitForAgentReady(t, addr)
	}

	agentsFile := writeAgentsFile(t, dir, addrs...)
	master := startMaster(t, spec, agentsFile)

	deadline := time.Now().Add(20 * time.Second)
	for !master.output.Contains("Assigned patch") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	for _, agent := range agents {
		agent.kill()
	}

	code := waitExit(t, master, 60*time.Second, "master")
	if code == 0 {
		t.Fatalf("master exited with code 0, want non-zero.\nOutput:\n%s", master.output.String())
	}
	if !master.output.Contains("RENDER FAILED") {
		t.Fatalf("master did not report an observable failure.\nOutput:\n%s", master.output.String())
	}
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		t.Fatalf("output image %s must not exist after a failed render", outputFile)
	}
}

// Scenario 3c: an agent accepts the connection and then goes completely
// silent (no pixels, no pong). The master's heartbeat must detect the
// timeout, stop waiting and fail the task instead of hanging.
func TestAgentHeartbeatTimeout(t *testing.T) {
	dir := t.TempDir()
	width, height := 16, 16
	outputFile := filepath.Join(dir, "out.png")
	spec := writeSpec(t, dir, width, height, 1, outputFile)

	// A silent fake agent: upgrades the websocket, then does nothing and
	// never answers pings.
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(messages.AgentStatus{Available: true, Cores: 2})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.SetPingHandler(func(string) error { return nil }) // swallow pings
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unable to listen: %s", err)
	}
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	defer server.Close()

	agentsFile := writeAgentsFile(t, dir, listener.Addr().String())
	master := startProcess(t, masterBin,
		"-spec", spec,
		"-agents", agentsFile,
		"-pongtimeout", "2s",
		"-pingperiod", "500ms",
	)

	code := waitExit(t, master, 60*time.Second, "master")
	if code == 0 {
		t.Fatalf("master exited with code 0, want non-zero.\nOutput:\n%s", master.output.String())
	}
	if !master.output.Contains("RENDER FAILED") {
		t.Fatalf("master did not report an observable failure.\nOutput:\n%s", master.output.String())
	}
	if _, err := os.Stat(outputFile); !os.IsNotExist(err) {
		t.Fatalf("output image %s must not exist after a failed render", outputFile)
	}
}

// Scenario 4: two consecutive rounds against the same agent. The first
// round is cancelled by disconnecting; its late pixels must not leak
// into the second round's stream, and the agent must accept the second
// round instead of staying "busy" forever.
func TestTwoRoundsSameAgent(t *testing.T) {
	addr := freeAddr(t)
	agent := startAgent(t, addr, "-pixeldelay", "5")
	defer agent.kill()
	waitForAgentReady(t, addr)

	width, height := 16, 16
	specJSON := fmt.Sprintf(`{
  "Settings": {"RenderRoutines": 2, "RenderDepth": 3},
  "Image": {"OutputFile": "unused.png", "Width": %d, "Height": %d, "Samples": 1, "Patch": [0, %d, 0, %d]},
  "Scene": {
    "Camera": {"LookFrom": [0, 0, 1], "LookAt": [0, 0, -1], "UpVector": [0, 1, 0], "FieldOfView": 45, "AspectRatio": 1.5, "Focus": 2.0, "Aperture": 0.0},
    "Objects": {"Spheres": [
      {"Center": [0, 0, -1], "Radius": 0.5, "Surface": {"Type": "Lambertian", "Albedo": [0.8, 0.3, 0.3], "Fuzz": 0.0, "RefIndex": 1.0}}
    ]},
    "AmbientLight": [0.2, 0.2, 0.2]
  }
}`, width, height, height, width)

	var spec models.Specification
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		t.Fatalf("unable to parse spec: %s", err)
	}

	dial := func() *websocket.Conn {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			conn, response, err := websocket.DefaultDialer.Dial("ws://"+addr, nil)
			if err == nil {
				return conn
			}
			if response != nil && response.StatusCode == http.StatusConflict {
				// Previous connection not fully torn down yet.
				time.Sleep(100 * time.Millisecond)
				continue
			}
			t.Fatalf("unable to dial agent: %s", err)
		}
		t.Fatal("agent did not accept a new connection in time")
		return nil
	}

	sendRequest := func(conn *websocket.Conn, opId string) {
		request := messages.RenderRequestMessage{
			Type:        messages.RenderRequest,
			Data:        spec,
			OperationId: opId,
		}
		if err := conn.WriteJSON(request); err != nil {
			t.Fatalf("unable to send render request %s: %s", opId, err)
		}
	}

	readMessage := func(conn *websocket.Conn) map[string]interface{} {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("unable to read message: %s", err)
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("unable to parse message: %s", err)
		}
		return msg
	}

	// Round 1: start rendering, read a few pixels, then disconnect.
	conn1 := dial()
	sendRequest(conn1, "round-1")

	msg := readMessage(conn1)
	if msg["Type"] != messages.RenderRequestResponse || msg["OperationId"] != "round-1" {
		t.Fatalf("unexpected round-1 response: %v", msg)
	}
	pixelsRound1 := 0
	for pixelsRound1 < 5 {
		msg = readMessage(conn1)
		if msg["Type"] == messages.PixelResult {
			if msg["OperationId"] != "round-1" {
				t.Fatalf("round-1 pixel carried wrong operation id: %v", msg["OperationId"])
			}
			pixelsRound1++
		}
	}
	conn1.Close()

	// Round 2: the agent must accept the new round (not stay busy), and
	// every message of the stream must carry the round-2 id.
	conn2 := dial()
	sendRequest(conn2, "round-2")

	msg = readMessage(conn2)
	if msg["Type"] != messages.RenderRequestResponse {
		t.Fatalf("expected a render response for round 2, got: %v", msg)
	}
	if msg["OperationId"] != "round-2" {
		t.Fatalf("round-2 response carried wrong operation id: %v", msg["OperationId"])
	}

	pixelsRound2 := 0
	completed := false
	for !completed {
		msg = readMessage(conn2)
		if msg["OperationId"] != "round-2" {
			t.Fatalf("late pixel from a previous round polluted round 2: %v", msg)
		}
		switch msg["Type"] {
		case messages.PixelResult:
			pixelsRound2++
		case messages.RenderingCompleted:
			completed = true
		}
	}

	if want := width * height; pixelsRound2 != want {
		t.Fatalf("round 2 delivered %d pixels, want %d", pixelsRound2, want)
	}

	// The agent must be usable again once the connection is released.
	conn2.Close()
	waitForAgentReady(t, addr)
}
