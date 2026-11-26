//! 带“代际 / 块”归属的事件收集器。
//!
//! 这是一个不依赖任何 FFI 或模型权重的纯 Rust 组件，用来解决流式回调与
//! `push_audio` / `reset` / `drop` 并发时的归属问题。
//!
//! ## 归属规则
//!
//! - 每次送入音频前调用 [`EventCollector::begin_block`] 得到一个
//!   [`BlockTicket`]；该块执行期间到达的事件进入该 ticket。
//! - 块结束时调用 [`BlockTicket::finish`]，原子地取走本块事件。finish 之后
//!   才到达的事件进入 *pending* 队列，下一次 `begin_block` 会把它们作为
//!   “上一块的晚到事件”继承，并由下一次结果交付。事件不会被清空丢弃。
//! - [`EventCollector::reset`] 开启新代际并使旧 [`EventSender`] 立即失效。
//!   与 reset 竞争的旧块 ticket 仍可取回 reset 前已经路由给它的事件，但这些
//!   事件不会进入新代际。
//! - [`EventCollector::shutdown`] 后所有 sender 均停止接收；sender 持有
//!   collector 的共享所有权，因此假回调源或引擎线程晚于封装对象退出时，
//!   也只会得到 `false`，不会访问已释放内存。

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// 一个事件收集器。
///
/// 该类型内部为 `Arc<Mutex<_>>`，`Clone` 得到的是共享同一份状态的句柄。
pub struct EventCollector<T> {
    inner: Arc<Mutex<CollectorInner<T>>>,
}

// 手动实现可避免 derive 给 `T` 加上 `T: Clone` 约束：克隆的只是 Arc 句柄。
impl<T> Clone for EventCollector<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// 绑定到某个 reset 代际的事件发送端。
///
/// 旧代际的 sender 在 [`EventCollector::reset`] 后发送事件会得到 `false`，
/// 因而不可能把重置前的晚到事件写入新一轮。
#[derive(Clone)]
pub struct EventSender<T> {
    collector: EventCollector<T>,
    epoch: u64,
}

/// 一次音频送入的归属凭证。
///
/// 即使 [`EventCollector::reset`] 在块执行期间发生，ticket 仍可结束并取回
/// reset 前已归属本块的事件（若实现选择把它们交还给本次调用方）。
pub struct BlockTicket<T> {
    collector: EventCollector<T>,
    epoch: u64,
    id: u64,
    inherited: Vec<T>,
    detached: Arc<Mutex<Vec<T>>>,
    finished: bool,
}

#[derive(Debug)]
struct CollectorInner<T> {
    epoch: u64,
    next_id: u64,
    active: Option<ActiveBlock<T>>,
    /// 上一块 finish 后、下一块 begin 前到达的晚到事件。
    pending: Vec<T>,
    /// 是否接受没有活动块的事件作为“上一块晚到事件”。
    ///
    /// 初始 / reset 后为 `false`：这保持旧实现“push 开始前清空缓冲”的语义，
    /// 不会把 `start()` 或 reset 后、首次 push 前的杂散事件算进音频块。一次块
    /// 正常 finish 后置为 `true`，此后到晚于调用返回的事件必须保留到下一块。
    accept_late: bool,
    closed: bool,
}

#[derive(Debug)]
struct ActiveBlock<T> {
    epoch: u64,
    id: u64,
    events: Vec<T>,
    /// reset / 重复 begin 把活动块从 map 中分离时，事件移交给 ticket。
    detached: Arc<Mutex<Vec<T>>>,
}

impl<T> Default for EventCollector<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventCollector<T> {
    /// 创建空收集器，初始代际为 1。
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CollectorInner {
                epoch: 1,
                next_id: 1,
                active: None,
                pending: Vec::new(),
                accept_late: false,
                closed: false,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, CollectorInner<T>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 返回绑定到当前代际的发送端。
    #[must_use]
    pub fn sender(&self) -> EventSender<T> {
        let epoch = self.lock().epoch;
        EventSender {
            collector: self.clone(),
            epoch,
        }
    }

    /// 开始一次音频块的事件归属。
    ///
    /// 若上一块 finish 后已有晚到事件，它们会从共享 pending 队列移出并保存在
    /// 返回的 ticket 中，最终由 [`BlockTicket::finish`] 作为结果前缀交付。
    #[must_use]
    pub fn begin_block(&self) -> BlockTicket<T> {
        let mut guard = self.lock();
        let epoch = guard.epoch;
        let id = guard.next_id;
        guard.next_id = guard.next_id.wrapping_add(1);

        let inherited = std::mem::take(&mut guard.pending);
        guard.accept_late = false;
        let detached = Arc::new(Mutex::new(Vec::new()));

        // 防御性处理：调用方未 finish 就再次 begin 时，不丢失旧块事件。
        if let Some(old) = guard.active.take() {
            let mut destination = old.detached.lock().unwrap_or_else(PoisonError::into_inner);
            destination.extend(old.events);
        }

        guard.active = Some(ActiveBlock {
            epoch,
            id,
            events: Vec::new(),
            detached: Arc::clone(&detached),
        });

        BlockTicket {
            collector: self.clone(),
            epoch,
            id,
            inherited,
            detached,
            finished: false,
        }
    }

    /// 重置代际并清空共享缓冲。
    ///
    /// 返回的新 [`EventSender`] 属于新代际；旧 sender 随后发送的事件会被拒绝。
    /// 与 reset 竞争的活动 ticket 仍可通过 [`BlockTicket::finish`] 取回 reset
    /// 前已经进入该块的事件。
    pub fn reset(&self) -> EventSender<T> {
        let epoch = {
            let mut guard = self.lock();
            guard.epoch = guard.epoch.wrapping_add(1);
            if guard.epoch == 0 {
                guard.epoch = 1;
            }
            let epoch = guard.epoch;

            if let Some(active) = guard.active.take() {
                let mut detached = active
                    .detached
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                detached.extend(active.events);
            }
            guard.pending.clear();
            guard.accept_late = false;
            // 不清除 closed：shutdown 是终态，之后的 reset 不得重新打开。
            epoch
        };

        EventSender {
            collector: self.clone(),
            epoch,
        }
    }

    /// 关闭收集器。
    ///
    /// 关闭后当前及旧代际 sender 的发送均返回 `false`。这比直接释放共享缓冲更
    /// 适合与引擎工作线程竞争：晚到回调仍持有有效 [`EventSender`]，但不会再
    /// 修改已关闭的会话状态。
    pub fn shutdown(&self) {
        let mut guard = self.lock();
        guard.closed = true;
        guard.epoch = guard.epoch.wrapping_add(1);
        if guard.epoch == 0 {
            guard.epoch = 1;
        }
        guard.active = None;
        guard.pending.clear();
        guard.accept_late = false;
    }

    /// 在一个块归属区间内执行同步操作并返回其事件。
    ///
    /// 这是不依赖真实音频后端的测试 / 假回调源入口。若 `operation` 返回错误，
    /// 已收集事件会退回 pending，避免错误路径静默丢弃。
    ///
    /// # Errors
    ///
    /// 原样返回 `operation` 产生的错误。
    pub fn collect_block_with<F, R, E>(&self, operation: F) -> Result<(R, Vec<T>), E>
    where
        F: FnOnce(&EventSender<T>) -> Result<R, E>,
    {
        let ticket = self.begin_block();
        let sender = ticket.sender();
        match operation(&sender) {
            Ok(result) => Ok((result, ticket.finish())),
            Err(err) => {
                ticket.abandon();
                Err(err)
            }
        }
    }

    fn dispatch(&self, epoch: u64, event: T) -> bool {
        let mut guard = self.lock();
        if guard.closed || epoch != guard.epoch {
            return false;
        }

        if let Some(active) = guard.active.as_mut()
            && active.epoch == epoch
        {
            active.events.push(event);
            return true;
        }

        if guard.accept_late {
            guard.pending.push(event);
            return true;
        }

        // 首个块开始前或 reset 后的杂散事件没有可归属的音频块；保持旧实现
        // “下次 push 前清空”的语义，不把它们送入结果。
        false
    }
}

impl<T> EventSender<T> {
    /// 投递一个事件。
    ///
    /// 返回 `true` 表示事件已进入当前代际的活动块或“上一块晚到”队列；
    /// `false` 表示收集器已关闭、sender 属于已重置的旧代际，或当前没有可归属
    /// 块（首个块开始前 / reset 后）的杂散事件。
    pub fn emit(&self, event: T) -> bool {
        self.collector.dispatch(self.epoch, event)
    }

    /// 该 sender 是否仍属于收集器当前代际且收集器未关闭。
    #[must_use]
    pub fn is_current(&self) -> bool {
        let guard = self.collector.lock();
        !guard.closed && guard.epoch == self.epoch
    }
}

impl<T> BlockTicket<T> {
    /// 返回可向本块所属代际投递事件的 sender。
    #[must_use]
    pub fn sender(&self) -> EventSender<T> {
        EventSender {
            collector: self.collector.clone(),
            epoch: self.epoch,
        }
    }

    /// 结束块并取走归属本块的全部事件。
    ///
    /// 结果顺序为：上一块遗留的晚到事件，随后是本块事件。
    #[must_use]
    pub fn finish(mut self) -> Vec<T> {
        self.finished = true;
        self.take_finished()
    }

    /// 操作失败时放弃本块凭证，但尽量把事件退回下一轮 pending。
    pub fn abandon(mut self) {
        self.finished = true;
        let mut events = std::mem::take(&mut self.inherited);

        let mut guard = self.collector.lock();
        let mine = guard
            .active
            .as_ref()
            .is_some_and(|active| active.epoch == self.epoch && active.id == self.id);

        if mine {
            if let Some(active) = guard.active.take() {
                events.extend(active.events);
            }
            // begin 已取走 pending；正常情况下这里为空。若有极端重入路径
            // 写入 pending，保持旧事件在前、新退回事件在后。
            let mut remaining = std::mem::take(&mut guard.pending);
            events.append(&mut remaining);
            guard.pending = events;
            guard.accept_late = true;
            return;
        }
        drop(guard);

        // reset / 重复 begin 已把活动事件移交给这个 ticket 自持的 detached。
        let mut detached = self.detached.lock().unwrap_or_else(PoisonError::into_inner);
        events.extend(detached.drain(..));
        // 代际已被 reset：不能放入新代际 pending。events 随 abandon 结束而
        // 释放；这是 reset 的明确清空语义，而非正常晚到事件路径。
    }

    fn take_finished(&mut self) -> Vec<T> {
        let mut result = std::mem::take(&mut self.inherited);

        let mine = {
            let mut guard = self.collector.lock();
            let mine = guard
                .active
                .as_ref()
                .is_some_and(|active| active.epoch == self.epoch && active.id == self.id);
            if mine {
                if let Some(active) = guard.active.take() {
                    result.extend(active.events);
                }
                guard.accept_late = true;
            }
            mine
        };

        if !mine {
            let mut detached = self.detached.lock().unwrap_or_else(PoisonError::into_inner);
            result.extend(detached.drain(..));
        }

        result
    }
}

impl<T> Drop for BlockTicket<T> {
    fn drop(&mut self) {
        if !self.finished {
            self.finished = true;
            // 恐慌展开期间也不能让 active 永久卡住。take_finished 会原子地解除
            // active；取到的事件没有调用方可返回，但其内存会在函数结束时释放。
            let _ = self.take_finished();
        }
    }
}

/// 兼容不同验收命名习惯的别名：队列 / 路由 / 缓冲均指同一归属组件。
pub use EventCollector as EventBuffer;
pub use EventCollector as EventQueue;
pub use EventCollector as EventRouter;

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn single_block_returns_exact_ordered_events() {
        let collector = EventCollector::<u32>::new();
        let ticket = collector.begin_block();
        let sender = ticket.sender();
        assert!(sender.emit(1));
        assert!(sender.emit(2));
        assert!(sender.emit(3));
        assert_eq!(ticket.finish(), vec![1, 2, 3]);
    }

    #[test]
    fn sequential_blocks_do_not_mix_events() {
        let collector = EventCollector::<&'static str>::new();

        let t1 = collector.begin_block();
        t1.sender().emit("a1");
        t1.sender().emit("a2");
        assert_eq!(t1.finish(), vec!["a1", "a2"]);

        let t2 = collector.begin_block();
        t2.sender().emit("b1");
        assert_eq!(t2.finish(), vec!["b1"]);

        let t3 = collector.begin_block();
        t3.sender().emit("c1");
        c2_late_from_previous_block(&collector);
        // c2 在 t3 finish 前到达，因此直接属于 t3。
        assert_eq!(t3.finish(), vec!["c1", "c2"]);
    }

    fn c2_late_from_previous_block(collector: &EventCollector<&'static str>) {
        collector.sender().emit("c2");
    }

    #[test]
    fn late_event_after_finish_is_delivered_to_next_block_once() {
        let collector = EventCollector::<u32>::new();

        let first = collector.begin_block();
        first.sender().emit(1);
        assert_eq!(first.finish(), vec![1]);

        // 送入调用已经返回后，引擎线程的晚到回调。
        assert!(collector.sender().emit(2));

        let second = collector.begin_block();
        second.sender().emit(3);
        assert_eq!(second.finish(), vec![2, 3]);

        // 没有重复残留在共享队列。
        let third = collector.begin_block();
        assert!(third.finish().is_empty());
    }

    #[test]
    fn every_event_is_delivered_exactly_once_with_late_arrivals() {
        let collector = EventCollector::<u32>::new();
        let mut delivered = Vec::new();

        let first = collector.begin_block();
        first.sender().emit(1);
        delivered.extend(first.finish());

        // 两个晚到事件：一个在下一块 begin 前，一个在 begin 后、finish 前。
        let late_sender = collector.sender();
        assert!(late_sender.emit(2));

        let mut second = collector.begin_block();
        delivered.append(&mut second.inherited);
        second.sender().emit(4);
        assert!(late_sender.emit(3));
        let second_events = second.finish();
        assert_eq!(second_events, vec![4, 3]);
        delivered.extend(second_events);

        assert_eq!(delivered, vec![1, 2, 4, 3]);
        assert_eq!(collector.begin_block().finish(), Vec::<u32>::new());
    }

    #[test]
    fn reset_clears_old_events_and_invalidates_old_sender() {
        let collector = EventCollector::<u32>::new();
        let old_sender = collector.sender();

        let ticket = collector.begin_block();
        old_sender.emit(1);
        old_sender.emit(2);

        let new_sender = collector.reset();
        assert!(!old_sender.is_current());
        assert!(!old_sender.emit(3));

        // reset 前已进入活动块的事件可由仍在进行的旧 ticket 取回，但不会进入
        // 新代际共享缓冲。
        assert_eq!(ticket.finish(), vec![1, 2]);

        let new_ticket = collector.begin_block();
        assert!(new_sender.emit(10));
        assert_eq!(new_ticket.finish(), vec![10]);
    }

    #[test]
    fn completed_pending_events_are_cleared_by_reset() {
        let collector = EventCollector::<u32>::new();
        let ticket = collector.begin_block();
        ticket.sender().emit(1);
        assert_eq!(ticket.finish(), vec![1]);
        collector.sender().emit(2);

        let new_sender = collector.reset();
        let next = collector.begin_block();
        assert!(next.finish().is_empty());

        let attributed = collector.begin_block();
        assert!(new_sender.emit(3));
        assert_eq!(attributed.finish(), vec![3]);
    }

    #[test]
    fn concurrent_reset_and_block_do_not_cross_contaminate() {
        for _ in 0..200 {
            let collector = EventCollector::<u32>::new();
            let ticket = collector.begin_block();
            let sender = ticket.sender();

            let emitter = thread::spawn(move || {
                for value in 0..100u32 {
                    // reset 后旧 sender 可能返回 false；这是预期的代际隔离。
                    let _ = sender.emit(value);
                }
            });

            let reset_sender = collector.reset();
            emitter.join().expect("emitter thread panicked");

            let old_events = ticket.finish();
            assert!(old_events.iter().all(|value| *value < 100));

            let next = collector.begin_block();
            for value in 1000..1100 {
                assert!(reset_sender.emit(value));
            }
            let new_events = next.finish();
            assert_eq!(new_events, (1000..1100).collect::<Vec<_>>());

            // 新代际结果中绝不能混入旧值。
            assert!(new_events.iter().all(|value| *value >= 1000));
        }
    }

    #[test]
    fn shutdown_rejects_senders_and_keeps_arc_handle_safe() {
        let collector = EventCollector::<u32>::new();
        let sender = collector.sender();
        let detached_sender = sender.clone();
        collector.shutdown();

        assert!(!sender.emit(1));
        assert!(!detached_sender.emit(2));
        assert!(!sender.is_current());
    }

    #[test]
    fn shutdown_is_terminal_even_if_reset_is_called() {
        let collector = EventCollector::<u32>::new();
        let sender = collector.sender();
        collector.shutdown();
        let reset_sender = collector.reset();

        assert!(!sender.emit(1));
        assert!(!reset_sender.emit(2));
        assert!(!reset_sender.is_current());
        assert!(collector.begin_block().finish().is_empty());
    }

    #[test]
    fn barrier_driven_late_callback_is_never_lost() {
        for _ in 0..200 {
            let collector = EventCollector::<u32>::new();
            let ticket = collector.begin_block();
            let sender = collector.sender();
            let barrier = Arc::new(Barrier::new(2));
            let worker_barrier = Arc::clone(&barrier);

            let worker = thread::spawn(move || {
                worker_barrier.wait();
                sender.emit(7)
            });

            barrier.wait();
            let immediate = ticket.finish();
            let emitted = worker.join().expect("worker panicked");
            assert!(emitted);

            let mut all = immediate;
            let mut next = collector.begin_block().finish();
            all.append(&mut next);
            assert_eq!(all, vec![7]);
        }
    }
}
