//! 任务会话（Session）：离线执行 + 流式处理。
//!
//! 会话由 [`crate::Model::create_task_session`] 创建，按其任务与运行模式
//! 分为：
//! - 离线（`RunMode::Offline`）：`run()` 一次执行、一次结果；
//! - 流式（`RunMode::Streaming`）：`start()` → `process_audio()` → `finish()`，
//!   期间通过事件回调或 `process_audio` 的返回值接收流事件。
//!
//! # 事件回调
//!
//! 流式会话可在创建时调用 [`Session::set_event_callback`] 注册一个 Rust 闭包，
//! shim 会在每个流事件发生时回传给该闭包（详见 `capi.h` 的
//! `audiocpp_stream_event_cb`）。回调数据经 `Mutex` 保护，可从 C++ 侧线程调用。

use std::ffi::{CStr, c_char, c_void};
use std::os::raw::c_int;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::{Arc, Mutex};

use audio_cpp_sys::*;

use crate::error::Error;
use crate::ffi;
use crate::model::Model;
use crate::registry::RegistryGuard;
use crate::request::IntoRequest;
use crate::types::{Backend, RunMode, StreamEvent, StreamingPolicy, TaskKind, TaskResult};

pub use crate::event_collector::{
    BlockTicket as EventBlockTicket, EventBuffer, EventCollector, EventQueue, EventRouter,
};

/// 绑定到 [`StreamEvent`] 收集器代际的回调发送端。
pub type StreamEventSender = crate::event_collector::EventSender<StreamEvent>;

/// 事件回调的底层存储（box 在堆上，地址稳定，可跨线程）。
struct EventSinkInner {
    cb: Mutex<Box<dyn FnMut(StreamEvent) + Send>>,
}

/// shim 事件回调的 C 桥接函数。
///
/// `user_data` 指向 `EventSinkInner`。事件 JSON 由 shim 一次性构造，转为
/// Rust 类型后交给用户闭包。
///
/// 用户闭包中的 panic 会被捕获并忽略，**不会**向上传播：panic 穿过
/// `extern "C"` 是未定义行为，回调内 panic 一律在此截停。
unsafe extern "C" fn stream_event_cb(
    user_data: *mut c_void,
    event_json: *const c_char,
    _is_final: c_int,
) {
    if user_data.is_null() || event_json.is_null() {
        return;
    }
    // SAFETY: shim 保证 user_data 是 EventSinkInner 指针。
    // SAFETY: event_json 是 NUL 结尾的 UTF-8 JSON。
    let json = unsafe { CStr::from_ptr(event_json) }
        .to_string_lossy()
        .into_owned();
    let Ok(event) = serde_json::from_str::<StreamEvent>(&json) else {
        return; // 事件 JSON 契约不符：忽略
    };

    // C 侧持有的是 Arc 裸指针。每次回调先增加一次强引用并在结束时释放，
    // 因此即使 Session 析构 / 替换回调与在途回调竞争，闭包存储也不会在回调
    // 执行期间被释放。C shim 的 set_sink(None) 锁还会保证解绑调用返回前
    // 当前 C 派发已经结束；Arc 是额外的生命周期兜底。
    let inner_ptr = user_data.cast::<EventSinkInner>();
    // SAFETY: C 契约保证进入回调时 user_data 来自仍存活或曾由 Arc 管理的
    // EventSinkInner；C EventSink 锁使解绑与本次进入互斥。
    unsafe { Arc::increment_strong_count(inner_ptr) };
    // SAFETY: 上一行刚增加一个强引用，from_raw 接管这个临时引用。
    let inner = unsafe { Arc::from_raw(inner_ptr) };

    let mut guard = inner
        .cb
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ = catch_unwind(AssertUnwindSafe(|| (guard)(event)));
}

/// 任务会话。
pub struct Session {
    raw: *mut audiocpp_session,
    /// 事件回调的持有者；析构时负责释放并确保 C 侧不再引用。
    /// 用 `Mutex` 保护指针本身，使 `set_event_callback` 能以 `&self` 调用
    /// （内部可变性），从而在持有 `&Session` 的线程上注册回调。
    event_sink: Mutex<Option<Arc<EventSinkInner>>>,
    /// 串行化所有触及 `raw` 的 Rust 调用。
    ///
    /// C++ 会话对象不保证可从多个调用线程并发进入；该锁使 [`Session`] 在 Rust
    /// 侧成为 `Sync`。事件回调线程不会在未违反文档约定的情况下反向获取此锁。
    api_lock: Mutex<()>,
    /// 串行化回调的装配 / 拆除 / 析构全过程：`set_event_callback` 与
    /// `Drop` 都先持有它再动 `event_sink` 与 C 侧绑定，避免两线程并发
    /// replace 造成“引擎指向 A、Rust 存 B”或 Box 泄漏。
    sink_swap: Mutex<()>,
    /// 共享持有 C 注册表：`Model` / `Registry` 先释放不影响会话继续工作。
    _guard: RegistryGuard,
    /// 创建会话时请求的计算后端（见 [`Session::backend`]）。
    backend: Backend,
}

// Session 持有回调 box（要求 Send）与 C 句柄。所有触及 C 句柄的公开方法都经
// `api_lock` 串行化；`sink_swap` 与 C shim 的 EventSink 锁共同保证回调解绑
// 安全。RegistryGuard 只做原子引用计数，因此共享 `&Session` 也是安全的。
unsafe impl Send for Session {}
// SAFETY: 同上：`api_lock` 保护裸 C 句柄，回调状态由各自 Mutex 与 C 侧锁保护。
unsafe impl Sync for Session {}

impl Session {
    /// 从原始 C 句柄包装（仅内部使用）。
    pub(crate) fn from_raw(
        raw: *mut audiocpp_session,
        guard: RegistryGuard,
        backend: Backend,
    ) -> Self {
        Self {
            raw,
            event_sink: Mutex::new(None),
            api_lock: Mutex::new(()),
            sink_swap: Mutex::new(()),
            _guard: guard,
            backend,
        }
    }

    /// 模型族名（借用的 C 字符串，即刻拷贝为 Rust 字符串）。
    pub fn family(&self) -> String {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        unsafe {
            let p = audiocpp_session_family(self.raw);
            if p.is_null() {
                String::new()
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
            }
        }
    }

    /// 任务种类（如 `"vad"`）。
    pub fn task_kind(&self) -> String {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        unsafe {
            let p = audiocpp_session_task_kind(self.raw);
            if p.is_null() {
                String::new()
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
            }
        }
    }

    /// 运行模式（如 `"offline"` / `"streaming"`）。
    pub fn run_mode(&self) -> String {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        unsafe {
            let p = audiocpp_session_run_mode(self.raw);
            if p.is_null() {
                String::new()
            } else {
                std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
            }
        }
    }

    /// 创建会话时请求的计算后端。
    ///
    /// 这是传给 [`crate::Model::create_task_session`] 的 `backend` 参数原样
    /// 记录，不经过上游再查询——C ABI 与上游引擎目前均无“实际落定后端”
    /// 查询接口。显式传 `Backend::Cpu/Vulkan/...` 时返回值即实际后端；
    /// 传 `Backend::Best` 时引擎内部按可用设备解析，返回值只能说明
    /// “请求的是自动选择”，不要据此显示具体设备名。
    pub fn backend(&self) -> Backend {
        self.backend
    }

    /// 流式策略描述（输入/输出类型、推荐分块大小）。
    ///
    /// # Errors
    ///
    /// C ABI 调用失败或返回的 JSON 无法解析时返回对应 [`Error`] 变体。
    pub fn streaming_policy(&self) -> Result<StreamingPolicy, Error> {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out: *mut c_char = ptr::null_mut();
        ffi::check_rc(unsafe { audiocpp_session_streaming_policy_json(self.raw, &mut out) })?;
        let json = unsafe { ffi::take_string(out)? };
        serde_json::from_str(&json).map_err(Error::from)
    }

    /// 为一次请求做准备（离线与流式共用）。
    ///
    /// `request` 为 [`crate::request::IntoRequest`]，可用类型化的
    /// [`crate::request::Request`] 枚举（推荐，每个任务一种变体），或任意
    /// JSON 字符串。携带 `audio` / `text` / `voice` 等输入，或用 `audio_path`
    /// 指向本地 WAV 文件。空请求可传 `()`。
    ///
    /// # Errors
    ///
    /// 请求序列化失败、路径含 NUL，或 C ABI 调用失败时返回对应 [`Error`] 变体。
    pub fn prepare<R: IntoRequest>(&self, request: R) -> Result<(), Error> {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let json = request.into_request()?.to_json()?;
        let req_c = ffi::cstring(&json)?;
        ffi::check_rc(unsafe {
            audiocpp_session_prepare(self.raw, req_c.as_ptr() as *const c_char)
        })
    }

    /// 注册流式事件回调；调用 `None` 清除。
    ///
    /// 回调会在每次流事件产生时被调用（可能来自 C++ 侧线程），其内容是
    /// 解析为 [`StreamEvent`] 的对象。回调内不得再调用本会话的方法
    /// （含 `set_event_callback`/`reset`，否则可能死锁）；回调内的 panic
    /// 会被捕获忽略，不会传播。
    ///
    /// 以 `&self` 提供（内部有 `Mutex` 保护），因此可在持有 `&Session`
    /// 的任意线程上注册 / 更换回调。同一个会话只保留一份回调。
    pub fn set_event_callback<F>(&self, cb: Option<F>)
    where
        F: FnMut(StreamEvent) + Send + 'static,
    {
        // 锁顺序固定为 api_lock -> sink_swap，避免与 process/reset 路径形成环。
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _swap = self
            .sink_swap
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 先清理旧回调，保证 C 侧不再引用旧的 user_data。旧 Arc 在解绑调用
        // 返回后才释放；若仍有 in-flight 的 stream_event_cb，它持有自己通过
        // 强引用计数得到的 Arc，因此不会发生 UAF。
        let old = self.event_sink_take();
        if old.is_some() {
            unsafe {
                audiocpp_session_set_event_sink(self.raw, None, ptr::null_mut());
            }
        }
        drop(old);

        if let Some(cb) = cb {
            let inner = Arc::new(EventSinkInner {
                cb: Mutex::new(Box::new(cb)),
            });
            unsafe {
                audiocpp_session_set_event_sink(
                    self.raw,
                    Some(stream_event_cb),
                    Arc::as_ptr(&inner).cast::<c_void>().cast_mut(),
                );
            }
            *self
                .event_sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(inner);
        }
    }

    /// 开始一次流式任务（先 `prepare` 再进入流式读取循环）。
    ///
    /// `request` 同 [`Session::prepare`]，类型化枚举或 JSON 字符串均可；
    /// 空请求传 `()`。
    ///
    /// # Errors
    ///
    /// 请求序列化失败、路径含 NUL，或 C ABI 调用失败时返回对应 [`Error`] 变体。
    pub fn start<R: IntoRequest>(&self, request: R) -> Result<(), Error> {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let json = request.into_request()?.to_json()?;
        let req_c = ffi::cstring(&json)?;
        ffi::check_rc(unsafe { audiocpp_session_start(self.raw, req_c.as_ptr() as *const c_char) })
    }

    /// 将一段音频送入流式会话，返回该块触发的第一个事件（若有）。
    ///
    /// `samples` 为 `float` 采样（-1..1），`start_sample` 为该块在输入流中的
    /// 起始采样点。若同时注册了事件回调，事件会经回调到达；返回值是
    /// shim 同步返回的事件 JSON。
    ///
    /// # Errors
    ///
    /// C ABI 调用失败或返回的 JSON 无法解析时返回对应 [`Error`] 变体。
    pub fn process_audio(
        &self,
        samples: &[f32],
        sample_rate: i32,
        channels: i32,
        start_sample: i64,
    ) -> Result<Option<StreamEvent>, Error> {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out: *mut c_char = ptr::null_mut();
        // 空输入显式传 null，避免对悬垂指针做算术（C++ 侧视 count==0 跳过读取）。
        let ptr = if samples.is_empty() {
            ptr::null()
        } else {
            samples.as_ptr()
        };
        ffi::check_rc(unsafe {
            audiocpp_session_process_audio(
                self.raw,
                ptr,
                samples.len(),
                sample_rate,
                channels,
                start_sample,
                &mut out,
            )
        })?;
        if out.is_null() {
            return Ok(None);
        }
        let json = unsafe { ffi::take_string(out)? };
        Ok(Some(serde_json::from_str(&json).map_err(Error::from)?))
    }

    /// 结束流式会话，返回最终 TaskResult。
    ///
    /// # Errors
    ///
    /// C ABI 调用失败或返回的 JSON 无法解析时返回对应 [`Error`] 变体。
    /// `check_rc` 通过却无 payload 属于 shim 违约，同样报错而非伪造空结果。
    pub fn finish(&self) -> Result<TaskResult, Error> {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out: *mut c_char = ptr::null_mut();
        ffi::check_rc(unsafe { audiocpp_session_finish(self.raw, &mut out) })?;
        if out.is_null() {
            return Err(Error::Ffi(ffi::last_error()));
        }
        let json = unsafe { ffi::take_string(out)? };
        serde_json::from_str(&json).map_err(Error::from)
    }

    /// 离线执行一次请求，返回 TaskResult。
    ///
    /// 相当于 `prepare` + 一次完整执行（shim 内部已做 prepare）。`request`
    /// 同 [`Session::prepare`]，类型化枚举或 JSON 字符串均可。
    ///
    /// # Errors
    ///
    /// 请求序列化失败、路径含 NUL，或 C ABI 调用失败时返回对应 [`Error`] 变体。
    pub fn run_offline<R: IntoRequest>(&self, request: R) -> Result<TaskResult, Error> {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let json = request.into_request()?.to_json()?;
        let req_c = ffi::cstring(&json)?;
        let mut out: *mut c_char = ptr::null_mut();
        ffi::check_rc(unsafe {
            audiocpp_session_run_offline(self.raw, req_c.as_ptr() as *const c_char, &mut out)
        })?;
        if out.is_null() {
            return Err(Error::Ffi(ffi::last_error()));
        }
        let json = unsafe { ffi::take_string(out)? };
        serde_json::from_str(&json).map_err(Error::from)
    }

    /// 重置流式会话内部状态（可复用会话对象重新开始）。
    ///
    /// # Errors
    ///
    /// C ABI 调用失败时返回对应 [`Error`] 变体。
    pub fn reset(&self) -> Result<(), Error> {
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ffi::check_rc(unsafe { audiocpp_session_reset(self.raw) })
    }

    /// 取出当前事件回调的共享句柄（内部用）。
    fn event_sink_take(&self) -> Option<Arc<EventSinkInner>> {
        self.event_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // 与所有 FFI 入口及 set_event_callback 互斥。安全 Rust 中 `&mut self`
        // 已排除调用线程上的并发方法；这把锁也能兜住之前 panic / poison 后仍
        // 可能观察到的内部状态，并固定 api -> sink_swap 的加锁顺序。
        let _api = self
            .api_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 与 set_event_callback 互斥：先解绑再释放，避免并发 replace 错配。
        // C shim 的 set_event_sink(None) 会等待正在 C EventSink 锁内执行的回调，
        // 因此返回后不会再有引擎线程引用旧 user_data。
        let _swap = self
            .sink_swap
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 先清除事件回调，避免 C 侧在会话析构后再引用 user_data。此处取出的
        // Arc 在 set_sink(None) 返回后才释放；若 in-flight C 回调已通过临时
        // Arc 强引用进入 Rust，闭包内存会安全存活到该回调结束。
        let old_sink = self.event_sink_take();
        if old_sink.is_some() {
            unsafe {
                audiocpp_session_set_event_sink(self.raw, None, ptr::null_mut());
            }
        }
        drop(old_sink);
        unsafe {
            audiocpp_session_free(self.raw);
        }
    }
}

/// 可在 reset 后热替换代际 sender 的共享回调句柄。
///
/// 内部闭包只在锁内克隆 sender，不持这把锁进入 collector，因此锁顺序始终是
/// “shared sink 短暂锁后释放，再取 collector 锁”，reset 不会与回调形成环。
#[derive(Clone)]
struct SharedStreamSink {
    current: Arc<Mutex<StreamEventSender>>,
}

impl SharedStreamSink {
    fn new(sender: StreamEventSender) -> Self {
        Self {
            current: Arc::new(Mutex::new(sender)),
        }
    }

    fn emit(&self, event: StreamEvent) {
        let sender = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let _ = sender.emit(event);
    }

    fn replace(&self, sender: StreamEventSender) {
        *self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = sender;
    }
}

/// 流式会话的便捷封装。
///
/// 包装一个 [`Session`]，自动注册内部事件收集器，把 shim 的逐事件回调
/// 缓冲为 Vec，使调用方无需手动管理回调 + 锁。典型用法（流式 ASR）：
///
/// ```
/// use audio_cpp::{Backend, Model, Registry, Request, RunMode, TaskKind};
/// use audio_cpp::session::StreamingSession;
/// # fn f() -> Result<(), audio_cpp::Error> {
/// # let registry = Registry::new()?;
/// # let model = registry.load("./qwen3-asr-0.6b-q8_0.gguf", None, None)?;
/// let session = model.create_task_session(
///     TaskKind::Asr, RunMode::Streaming, Backend::Cpu, 0, 1, None,
/// )?;
/// let mut stream = StreamingSession::from_session(session);
///
/// stream.start(Request::stream_asr().option("language", "auto"))?;
/// // 每块音频送入后，取出该块触发的全部事件（partial_text / is_final）
/// let samples: Vec<f32> = vec![0.0; 512];
/// let events = stream.push_audio(&samples, 16000, 1, 0)?;
/// for ev in &events {
///     if let Some(t) = &ev.partial_text {
///         println!("增量: {}", t.text);
///     }
/// }
/// let result = stream.finish()?;
/// # Ok(()) }
/// ```
///
/// 与底层 [`Session::process_audio`] 不同，`push_audio` 返回该块触发的
/// **所有**事件（经内部回调收集），而不是单个事件。不消费音频块的模型族
/// （如流式 TTS）可跳过 `push_audio`，直接 `start` → `finish`。
pub struct StreamingSession {
    session: Session,
    collector: EventCollector<StreamEvent>,
    sink: SharedStreamSink,
    /// 串行化多次 `push_audio`，保证收集器中最多只有一个活动块。
    /// `reset` 不获取此锁，因此它可以与仍在执行的块在状态层并发，并通过代际
    /// 标记隔离旧事件，而不是等待整个音频后端调用结束。
    push_lock: Mutex<()>,
}

impl StreamingSession {
    /// 从现有会话包装（流式任务）。
    pub fn from_session(session: Session) -> Self {
        let collector = EventCollector::<StreamEvent>::new();
        let sender = collector.sender();
        let sink = SharedStreamSink::new(sender);
        let callback_sink = sink.clone();
        session.set_event_callback(Some(move |ev| callback_sink.emit(ev)));
        Self {
            session,
            collector,
            sink,
            push_lock: Mutex::new(()),
        }
    }

    /// 从模型创建流式会话并包装（等价于
    /// `create_task_session(Streaming)` + [`Self::from_session`]）。
    ///
    /// # Errors
    ///
    /// 底层无法创建流式会话时返回对应 [`Error`] 变体。
    pub fn from_model(
        model: &Model,
        task: TaskKind,
        backend: Backend,
        device: i32,
        threads: i32,
        session_options: Option<&str>,
    ) -> Result<Self, Error> {
        let session = model.create_task_session(
            task,
            RunMode::Streaming,
            backend,
            device,
            threads,
            session_options,
        )?;
        Ok(Self::from_session(session))
    }

    /// 开始流式任务（等价于 [`Session::start`]）。
    ///
    /// # Errors
    ///
    /// 请求序列化失败、路径含 NUL，或 C ABI 调用失败时返回对应 [`Error`] 变体。
    pub fn start<R: IntoRequest>(&self, request: R) -> Result<(), Error> {
        self.session.start(request)
    }

    /// 送入一段音频，返回该块触发的全部流事件。
    ///
    /// 事件由代际收集器归属：块执行期间的事件进入本次结果；本次结果原子取走
    /// 后才到达的晚到事件会保留，并作为下一次 `push_audio` 结果的前缀交付，
    /// 因此既不会被下一次清空吞掉，也不会重复出现。`samples` 为 `float`
    /// 采样，`start_sample` 为相对输入流的起始采样点。
    ///
    /// # Errors
    ///
    /// C ABI 调用失败时返回对应 [`Error`] 变体；失败前已收集的事件会保留到
    /// 下一次送入，而不是被清空丢弃。
    pub fn push_audio(
        &self,
        samples: &[f32],
        sample_rate: i32,
        channels: i32,
        start_sample: i64,
    ) -> Result<Vec<StreamEvent>, Error> {
        let _push = self
            .push_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ticket = self.collector.begin_block();
        if let Err(err) = self
            .session
            .process_audio(samples, sample_rate, channels, start_sample)
        {
            ticket.abandon();
            return Err(err);
        }
        Ok(ticket.finish())
    }

    /// 结束流式会话，返回最终结果（等价于 [`Session::finish`]）。
    ///
    /// # Errors
    ///
    /// C ABI 调用失败时返回对应 [`Error`] 变体。
    pub fn finish(&self) -> Result<TaskResult, Error> {
        self.session.finish()
    }

    /// 重置会话（等价于 [`Session::reset`]），并清空内部事件归属状态。
    ///
    /// 重置会开启新的事件代际、丢弃上一轮 pending 事件，并热替换内部回调
    /// sender。旧 sender 立即失效；即使引擎工作线程的旧事件在重置后才抢到
    /// collector 锁，也不能写入新一轮。与重置并发的旧 `push_audio` ticket
    /// 仍可取回重置前已归属它的事件。这里不会重新注册 Session 回调，因此调用
    /// 方通过 [`Self::session`] 设置的自定义回调不会被重置动作覆盖。
    ///
    /// # Errors
    ///
    /// C ABI 调用失败时返回对应 [`Error`] 变体，且不清空内部状态（保持与旧
    /// 实现“reset 失败则不清理”一致）。
    pub fn reset(&self) -> Result<(), Error> {
        self.session.reset()?;
        let sender = self.collector.reset();
        self.sink.replace(sender);
        Ok(())
    }

    /// 返回内部事件归属器。
    ///
    /// 这是不依赖真实模型权重的测试 / 假回调源入口：验收代码可通过它的
    /// `sender()` 按任意时序注入 [`StreamEvent`]，直接检验归属、晚到、重置与
    /// 关闭语义。普通业务代码通常只使用 [`Self::push_audio`]。
    #[must_use]
    pub fn event_collector(&self) -> &EventCollector<StreamEvent> {
        &self.collector
    }

    /// 底层会话引用。
    ///
    /// 注意：经此引用调用 [`Session::set_event_callback`] 会替换
    /// [`StreamingSession`] 的内部事件收集桥，导致 `push_audio` 收不到事件；
    /// [`StreamingSession::reset`] 只更新内部代际，不会覆盖用户新注册的回调。
    /// 如需自定义回调，请直接使用 [`Session`] 而非本包装。
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// 创建时请求的计算后端（等价于 [`Session::backend`]）。
    pub fn backend(&self) -> Backend {
        self.session.backend()
    }

    /// 底层会话可变引用。
    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }
}

impl Drop for StreamingSession {
    fn drop(&mut self) {
        // 先经 C shim 解绑并等待当前派发结束，再关闭归属器。这样丢弃流程对
        // “回调已解绑”有一个明确同步点；之后晚到的假源 sender 也只会得到
        // false。即使跳过这一步，Session 字段析构仍会解绑，Arc 生命周期同样
        // 能防止在途 Rust 回调 UAF。
        self.session.set_event_callback(None::<fn(StreamEvent)>);
        self.collector.shutdown();
    }
}

#[cfg(test)]
mod streaming_session_tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_session_types_are_send_and_sync() {
        assert_send_sync::<Session>();
        assert_send_sync::<StreamingSession>();
        assert_send_sync::<EventCollector<StreamEvent>>();
    }
}
