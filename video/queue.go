package video

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
	"videortc/request"
	"videortc/util"

	"github.com/pion/webrtc/v4"
)

const maxBufferedAmount uint64 = 1024 * 1024 // 1 MB
const chunk = 1024 * 60

var (
	queueManager = newdcQueueManager()

	errTaskCancelled = errors.New("video: task cancelled")
	errChannelClosed = errors.New("video: channel closed")
)

// DataChannel 是队列需要的 *webrtc.DataChannel 子集。
// 抽象成接口是为了能在本地用自造(或替身)通道验证队列行为。
type DataChannel interface {
	ReadyState() webrtc.DataChannelState
	Send(data []byte) error
	BufferedAmount() uint64
	ID() *uint16
	Label() string
}

// ItemStat for queue status
type ItemStat struct {
	ID       string
	Label    string
	State    string
	Tasks    int
	Buffered uint64
}

// dcQueueManager 维护所有 DataChannel 到 dcConnections 的映射,每个通道对应一个 dcQueue
// 注意:DataChannel 的 ID(SCTP 流号)只在单个 PeerConnection 内唯一,不同 peer 的 channel 可能都是 0,
// 所以以通道对象本身作为 key,确保每个 (peer, channel) 有独立队列
type dcQueueManager struct {
	queues sync.Map // DataChannel -> *dcQueue

	// 以下字段在创建队列时读取,生产环境使用默认值,测试可调小以获得确定性
	fetch           func(url string) ([]byte, error)
	pollInterval    time.Duration
	paceUnit        time.Duration
	maxSendDuration time.Duration
}

func newdcQueueManager() *dcQueueManager {
	return &dcQueueManager{
		fetch: func(url string) ([]byte, error) {
			return request.HttpProvider.Get(url, 5)
		},
		pollInterval:    time.Second,
		paceUnit:        time.Millisecond * 100,
		maxSendDuration: time.Second * 5,
	}
}

// CloseChannel 立即停止此通道上所有待办与进行中的任务并移除队列。
// 断链(DataChannel 关闭/PeerConnection 失败)时由 rtc 层调用。
func CloseChannel(d DataChannel) {
	queueManager.closeChannel(d)
}

func (q *dcQueueManager) closeChannel(d DataChannel) {
	if isNilChannel(d) {
		return
	}
	t, ok := q.queues.Load(d)
	if !ok {
		return
	}
	q.dropChannel(t.(*dcQueue))
}

// dropChannel 幂等地停止队列并从 manager 中移除。
// CompareAndDelete 避免误删同一 channel key 上后来新建的队列。
func (q *dcQueueManager) dropChannel(dq *dcQueue) {
	dq.stop()
	q.queues.CompareAndDelete(dq.dc, dq)
}

// send 入队一个分片任务。同一通道上相同 (id,index) 只保留一份,重复请求会被拒绝。
func (q *dcQueueManager) send(d DataChannel, task *bufferTask) {
	if isNilChannel(d) || d.ReadyState() != webrtc.DataChannelStateOpen {
		task.cancel()
		return
	}
	nq := q.newQueue(d)
	t, loaded := q.queues.LoadOrStore(d, nq)
	dq := t.(*dcQueue)
	if !dq.addTask(task) {
		if !loaded {
			// 新建的队列没有接受任何任务(通道已关闭或者是重复任务),直接回收
			dq.stop()
			q.queues.CompareAndDelete(d, dq)
		}
		return
	}
	if !loaded {
		go dq.loop()
	}
}

// quit 撤回分片。会同时停止仍待办以及正在发送中的同 (id,index) 任务,
// 且 quit 返回后该分片不会再有新的发送开始。
func (q *dcQueueManager) quit(d DataChannel, id string, index []uint64) {
	if isNilChannel(d) {
		return
	}
	t, ok := q.queues.Load(d)
	if !ok {
		return
	}
	dq := t.(*dcQueue)
	dq.quit(id, index)
	if dq.isInvalid() {
		q.dropChannel(dq)
	}
}

func (q *dcQueueManager) stats() map[string]*ItemStat {
	res := map[string]*ItemStat{}
	q.queues.Range(func(key, value any) bool {
		dq := value.(*dcQueue)
		if dq.isInvalid() {
			// 通道已关闭/连接失败,清理后不再出现在队列状态中
			q.dropChannel(dq)
			return true
		}
		res[channelKey(dq.dc)] = dq.stat()
		return true
	})
	return res
}

func (q *dcQueueManager) newQueue(d DataChannel) *dcQueue {
	ctx, cancel := context.WithCancel(context.Background())
	return &dcQueue{
		manager: q,
		dc:      d,
		ctx:     ctx,
		cancel:  cancel,
		wake:    make(chan struct{}, 1),
	}
}

// dcQueue 单个 DataChannel 上的串行任务队列。
//
// 锁顺序固定为 sendMu -> mu,任何代码都不能反向获取:
//   - sendMu 覆盖"检查取消状态 + 实际 Send",使 quit/断链与发送互斥,
//     从而保证 quit/close 返回后不会再有新的分片开始发送;
//   - mu 保护 pending/inflight/closed 等队列状态。
type dcQueue struct {
	manager *dcQueueManager
	dc      DataChannel

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	pending  []*bufferTask
	inFlight *bufferTask
	closed   bool

	sendMu sync.Mutex
	wake   chan struct{}
}

func (d *dcQueue) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *dcQueue) hasTaskLocked(id string, index uint64) bool {
	if t := d.inFlight; t != nil && !t.cancelled && t.id == id && t.index == index {
		return true
	}
	for _, t := range d.pending {
		if !t.cancelled && t.id == id && t.index == index {
			return true
		}
	}
	return false
}

// addTask 入队。返回 false 表示被拒绝(通道不可用或重复任务),调用方传入的任务已被取消。
func (d *dcQueue) addTask(t *bufferTask) bool {
	d.mu.Lock()
	if d.closed || d.dc.ReadyState() != webrtc.DataChannelStateOpen || d.hasTaskLocked(t.id, t.index) {
		d.cancelTaskLocked(t)
		d.mu.Unlock()
		return false
	}
	d.pending = append(d.pending, t)
	d.mu.Unlock()
	d.signal()
	return true
}

func (d *dcQueue) cancelTaskLocked(t *bufferTask) {
	if t.cancelled {
		return
	}
	t.cancelled = true
	t.cancel()
}

// takeTask 取出一个可执行任务并设为 inFlight。被取消或已超时的任务直接丢弃。
func (d *dcQueue) takeTask() *bufferTask {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.inFlight != nil {
		return nil
	}
	for len(d.pending) > 0 {
		t := d.pending[0]
		d.pending[0] = nil
		d.pending = d.pending[1:]
		if t.cancelled || t.ctx.Err() != nil {
			d.cancelTaskLocked(t)
			continue
		}
		d.inFlight = t
		return t
	}
	return nil
}

func (d *dcQueue) finishTask(t *bufferTask) {
	d.mu.Lock()
	if d.inFlight == t {
		d.inFlight = nil
	}
	d.mu.Unlock()
}

// quit 取消匹配的分片任务。id 为空且 index 为 [0] 时表示清空全部(与旧行为兼容)。
// 同时作用于待办任务与执行中任务;sendMu 保证返回时没有正在进行的 Send。
func (d *dcQueue) quit(id string, index []uint64) {
	if len(index) == 0 {
		return
	}
	all := id == "" && len(index) == 1 && index[0] == 0
	if !all && id == "" {
		return
	}
	set := make(map[uint64]struct{}, len(index))
	for _, i := range index {
		set[i] = struct{}{}
	}
	match := func(t *bufferTask) bool {
		if all {
			return true
		}
		if t.id != id {
			return false
		}
		_, ok := set[t.index]
		return ok
	}

	d.sendMu.Lock()
	d.mu.Lock()
	kept := d.pending[:0]
	for _, t := range d.pending {
		if match(t) {
			d.cancelTaskLocked(t)
		} else {
			kept = append(kept, t)
		}
	}
	for i := len(kept); i < len(d.pending); i++ {
		d.pending[i] = nil
	}
	d.pending = kept
	if d.inFlight != nil && match(d.inFlight) {
		d.cancelTaskLocked(d.inFlight)
	}
	d.mu.Unlock()
	d.sendMu.Unlock()
}

// stop 终止队列:取消全部待办与进行中任务,取消队列 ctx。可重复调用。
func (d *dcQueue) stop() {
	d.sendMu.Lock()
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		for i, t := range d.pending {
			d.cancelTaskLocked(t)
			d.pending[i] = nil
		}
		d.pending = nil
		if d.inFlight != nil {
			d.cancelTaskLocked(d.inFlight)
		}
	}
	cancel := d.cancel
	d.mu.Unlock()
	d.sendMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (d *dcQueue) isInvalid() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closed || d.dc.ReadyState() != webrtc.DataChannelStateOpen
}

func (d *dcQueue) isDone(t *bufferTask) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closed || t.cancelled || d.ctx.Err() != nil || t.ctx.Err() != nil
}

// sendChunk 在 sendMu 保护下检查取消状态并完成一次发送。
// quit/close 也必须先拿 sendMu,因此其返回后不会再有分片开始发送。
func (d *dcQueue) sendChunk(t *bufferTask, payload []byte) error {
	d.sendMu.Lock()
	defer d.sendMu.Unlock()

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errChannelClosed
	}
	if t.cancelled {
		d.mu.Unlock()
		return errTaskCancelled
	}
	if d.dc.ReadyState() != webrtc.DataChannelStateOpen {
		d.mu.Unlock()
		return errChannelClosed
	}
	d.mu.Unlock()

	return d.dc.Send(payload)
}

func (d *dcQueue) doTask(t *bufferTask) (fatal bool) {
	if d.isDone(t) {
		return false
	}
	bs, err := d.manager.fetch(t.target)
	if err != nil {
		util.Log.Print(err)
		return false
	}
	if d.isDone(t) {
		return false
	}
	buffers := splitBuffer(bs)
	l := len(buffers)
	start := time.Now()
	for i, buf := range buffers {
		if d.manager.maxSendDuration > 0 && i > 0 && time.Since(start) > d.manager.maxSendDuration {
			return false
		}
		payload := append(chunkHeader(t.id, t.index, i, l), buf...)
		if err := d.sendChunk(t, payload); err != nil {
			if errors.Is(err, errTaskCancelled) {
				// 仅此任务被撤回,其他任务继续
				return false
			}
			if !errors.Is(err, errChannelClosed) {
				util.Log.Print(err)
			}
			// 通道关闭或底层发送失败:整个通道不可继续
			return true
		}
		d.pace(t)
	}
	return false
}

// pace 按通道缓冲量限速,并可在撤回/断链时立即退出等待
func (d *dcQueue) pace(t *bufferTask) {
	if d.manager.paceUnit <= 0 {
		return
	}
	n := d.dc.BufferedAmount() / maxBufferedAmount
	if n < 1 {
		n = 1
	}
	timer := time.NewTimer(time.Duration(n) * d.manager.paceUnit)
	defer timer.Stop()
	select {
	case <-d.ctx.Done():
	case <-t.ctx.Done():
	case <-timer.C:
	}
}

func (d *dcQueue) loop() {
	ticker := time.NewTicker(d.manager.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-d.wake:
		case <-ticker.C:
		}
		if d.isInvalid() {
			d.manager.dropChannel(d)
			return
		}
		if d.dc.BufferedAmount() > 1024 {
			continue
		}
		t := d.takeTask()
		if t == nil {
			continue
		}
		fatal := d.doTask(t)
		d.finishTask(t)
		if fatal {
			d.manager.dropChannel(d)
			return
		}
	}
}

func (d *dcQueue) stat() *ItemStat {
	d.mu.Lock()
	defer d.mu.Unlock()
	tasks := 0
	for _, t := range d.pending {
		if !t.cancelled {
			tasks++
		}
	}
	if t := d.inFlight; t != nil && !t.cancelled {
		tasks++
	}
	var id string
	if v := d.dc.ID(); v != nil {
		id = fmt.Sprintf("%d", *v)
	}
	return &ItemStat{
		ID:       id,
		Label:    d.dc.Label(),
		State:    d.dc.ReadyState().String(),
		Tasks:    tasks,
		Buffered: d.dc.BufferedAmount(),
	}
}

func isNilChannel(d DataChannel) bool {
	if d == nil {
		return true
	}
	v := reflect.ValueOf(d)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Func, reflect.Chan, reflect.Slice, reflect.Interface:
		return v.IsNil()
	}
	return false
}

func channelKey(d DataChannel) string {
	if isNilChannel(d) {
		return "<nil>"
	}
	v := reflect.ValueOf(d)
	if v.Kind() == reflect.Ptr {
		return fmt.Sprintf("%p", d)
	}
	return fmt.Sprintf("%T", d)
}

// 自定义分片协议,前端按照此协议组装
// [id, sn, i, n]
// 协议头|版本号|头部长度
// 二进制协议
// protocol 16位 0x7364
// version x0a1

func chunkHeader(id string, index uint64, i int, l int) []byte {
	var meta = fmt.Sprintf(`["%s",%d,%d,%d]`, id, index, i, l)
	var head = []byte{0x73, 0x64, 0xa1, uint8(len(meta))}
	return append(head, []byte(meta)...)
}

func splitBuffer(bs []byte) [][]byte {
	var (
		buffers = [][]byte{}
		start   = 0
		end     int
		l       = len(bs)
		data    []byte
	)
	for start < l {
		end = start + chunk
		if end > l {
			end = l
		}
		data = bs[start:end]
		buffers = append(buffers, data)
		start = end
	}
	return buffers
}
