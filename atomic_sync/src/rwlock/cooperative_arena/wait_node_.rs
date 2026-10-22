//! 统一的等待节点：`WaitNode` 与内容枚举 `WakeNode`。
//!
//! # 布局
//!
//! 读者节点与写者节点使用完全相同的布局：一对兄弟指针 `prev_` / `next_`
//! 加一个 [`WakeNode`]。主队列节点同时使用 `prev_` / `next_`；读者链节点
//! 只用 `prev_` 串成一条从“最后加入的读者”出发的 LIFO 单链。
//!
//! ```text
//! 主队列:   RootRead ──> PureWrite ──> RootRead ──> ...
//!
//! 读者链:   RootRead.tail_ ──> 最后加入的 Reader
//!                               │ prev_
//!                               ▼
//!                            更早的 Reader ──> ... ──> None
//! ```
//!
//! # 同步
//!
//! - 主队列、读者链的发布、唤醒遍历、节点回收由队列锁保护；
//! - [`RootRead::tail_`] 是原子指针，向组里加入读者就是一次 CAS 压栈；
//! - [`ReaderNode`] 的 `state` 是原子量，领取、取消、waker 注册都不需要
//!   队列锁：
//!   - `Registering`：持有者正在写 waker；
//!   - `Waiting`：waker 已就绪，可被唤醒者取走；
//!   - `Taking`：唤醒者正在取 waker；
//!   - `Admitted`：已放行，等待持有者 poll；
//!   - `Acquired`：已领取许可；
//!   - `Cancelled`：已取消。
//!
//! `RootRead` 的原子同时承载持锁计数与可升级读标记。写者节点仍由队列锁保护。

use core::{
    cell::{Cell, UnsafeCell},
    hint,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering},
    task::Waker,
};

use crate::rwlock::wait_::WaitKind;

/// 可升级读持锁标记在 [`RootRead::state_`] 中的位置。
const UPGR_BIT: usize = 1 << (usize::BITS - 1);
/// 读者计数掩码。
const COUNT_MASK: usize = !UPGR_BIT;

/// 读者正在写 waker。
pub(super) const READER_REGISTERING: u8 = 0;
/// 读者 waker 已就绪。
pub(super) const READER_WAITING: u8 = 1;
/// 唤醒者正在取 waker。
pub(super) const READER_TAKING: u8 = 2;
/// 读者已被放行。
pub(super) const READER_ADMITTED: u8 = 3;
/// 读者已领取许可。
pub(super) const READER_ACQUIRED: u8 = 4;
/// 读者已取消。
pub(super) const READER_CANCELLED: u8 = 5;

/// 一个等待节点。
///
/// 所有变体共享同一对兄弟指针；具体含义由 [`WakeNode`] 决定。
pub(super) struct WaitNode {
    /// 主队列前驱；读者链节点的 `prev_` 指向上一个加入的读者。
    prev_: UnsafeCell<Option<NonNull<WaitNode>>>,
    /// 主队列后继。
    next_: UnsafeCell<Option<NonNull<WaitNode>>>,
    /// 节点内容。
    content_: WakeNode,
}

// SAFETY: 裸指针只指向同一个 Arena 内、地址稳定的节点。读者链的发布与遍历
// 由队列锁保护（`RootRead.tail_` 是原子指针）；`ReaderNode` 的状态与 waker
// 由自身的原子状态机协调；`RootRead.state_` 是原子量。`Waker` 满足
// `Send + Sync`。
unsafe impl Send for WaitNode {}
// SAFETY: 同上；共享 `&WaitNode` 只允许在队列锁内访问链指针与写者槽位，
// 或通过原子协议访问读者状态/waker。
unsafe impl Sync for WaitNode {}

impl WaitNode {
    /// 建立一个读者组根节点。
    pub(super) fn new_root_read() -> Self {
        WaitNode {
            prev_: UnsafeCell::new(Option::None),
            next_: UnsafeCell::new(Option::None),
            content_: WakeNode::RootRead(RootRead::new()),
        }
    }

    /// 建立一个读者链节点。
    pub(super) fn new_reader(kind: WaitKind, root: NonNull<WaitNode>) -> Self {
        debug_assert!(kind.is_readish(), "new_reader 只接受读类等待者");
        let content = match kind {
            WaitKind::UpgradableRead => WakeNode::UpgrRead(ReaderNode::new(root)),
            _ => WakeNode::PureRead(ReaderNode::new(root)),
        };
        WaitNode {
            prev_: UnsafeCell::new(Option::None),
            next_: UnsafeCell::new(Option::None),
            content_: content,
        }
    }

    /// 建立一个主队列写者节点。
    pub(super) fn new_write(kind: WaitKind, root: Option<NonNull<WaitNode>>) -> Self {
        debug_assert!(!kind.is_readish(), "new_write 只接受独占等待者");
        WaitNode {
            prev_: UnsafeCell::new(Option::None),
            next_: UnsafeCell::new(Option::None),
            content_: WakeNode::PureWrite(WriteNode::new(kind, root)),
        }
    }

    /// 是否读者组根节点。
    #[inline]
    pub(super) fn is_root_read_(&self) -> bool {
        matches!(self.content_, WakeNode::RootRead(_))
    }

    /// 是否读者链节点。
    #[inline]
    pub(super) fn is_reader_(&self) -> bool {
        matches!(self.content_, WakeNode::PureRead(_) | WakeNode::UpgrRead(_))
    }

    /// 是否升级读节点。
    #[inline]
    pub(super) fn is_upgr_read_(&self) -> bool {
        matches!(self.content_, WakeNode::UpgrRead(_))
    }

    /// 是否写者节点。
    #[inline]
    pub(super) fn is_write_(&self) -> bool {
        matches!(self.content_, WakeNode::PureWrite(_))
    }

    /// 以读者组根节点解释自己；内部不变量被破坏时 panic。
    #[inline]
    pub(super) fn as_root_read_(&self) -> &RootRead {
        match &self.content_ {
            WakeNode::RootRead(root) => root,
            _ => panic!("WaitNode: 期望 RootRead"),
        }
    }

    /// 以读者节点解释自己；内部不变量被破坏时 panic。
    #[inline]
    pub(super) fn as_reader_(&self) -> &ReaderNode {
        match &self.content_ {
            WakeNode::PureRead(reader) | WakeNode::UpgrRead(reader) => reader,
            _ => panic!("WaitNode: 期望 ReaderNode"),
        }
    }

    /// 以写者节点解释自己；内部不变量被破坏时 panic。
    #[inline]
    pub(super) fn as_write_(&self) -> &WriteNode {
        match &self.content_ {
            WakeNode::PureWrite(write) => write,
            _ => panic!("WaitNode: 期望 WriteNode"),
        }
    }

    /// 读取前驱。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁，或在节点尚未发布时访问。
    #[inline]
    pub(super) unsafe fn prev_(&self) -> Option<NonNull<WaitNode>> {
        // SAFETY: 由调用方保证。
        unsafe { *self.prev_.get() }
    }

    /// 写入前驱。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁，或在节点尚未发布时访问。
    #[inline]
    pub(super) unsafe fn set_prev_(&self, prev: Option<NonNull<WaitNode>>) {
        // SAFETY: 由调用方保证。
        unsafe { *self.prev_.get() = prev };
    }

    /// 读取后继。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn next_(&self) -> Option<NonNull<WaitNode>> {
        // SAFETY: 由调用方保证持有队列锁。
        unsafe { *self.next_.get() }
    }

    /// 写入后继。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn set_next_(&self, next: Option<NonNull<WaitNode>>) {
        // SAFETY: 由调用方保证持有队列锁。
        unsafe { *self.next_.get() = next };
    }
}

/// 节点内容。
pub(super) enum WakeNode {
    /// 读者组根：持锁计数 + 最后加入的读者。
    RootRead(RootRead),
    /// 普通读等待者。
    PureRead(ReaderNode),
    /// 可升级读等待者；一条读者链至多一个。
    UpgrRead(ReaderNode),
    /// 普通写者或升级写者。
    PureWrite(WriteNode),
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// RootRead
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读者组根节点。
///
/// `state_` 的低位是持锁读者数，最高位是可升级读持锁标记；`tail_` 是最后
/// 加入的读者节点，读者链通过每个节点的 `prev_` 反向串起来。
pub(super) struct RootRead {
    state_: AtomicUsize,
    tail_: AtomicPtr<WaitNode>,
}

impl RootRead {
    fn new() -> Self {
        RootRead {
            state_: AtomicUsize::new(0),
            tail_: AtomicPtr::new(core::ptr::null_mut()),
        }
    }

    /// 当前持锁读者数（含已放行、尚未领取的读者）。
    #[inline]
    pub(super) fn count_(&self) -> usize {
        self.state_.load(Ordering::Acquire) & COUNT_MASK
    }

    /// 放行/领取一个读者时加一。
    #[inline]
    pub(super) fn inc_count_(&self) {
        self.state_.fetch_add(1, Ordering::AcqRel);
    }

    /// 释放/取消一个已放行读者时减一；返回减一前的计数。
    #[inline]
    pub(super) fn dec_count_(&self) -> usize {
        self.state_.fetch_sub(1, Ordering::AcqRel) & COUNT_MASK
    }

    /// 当前是否有已领取的可升级读持锁者。
    #[inline]
    pub(super) fn upgr_active_(&self) -> bool {
        self.state_.load(Ordering::Acquire) & UPGR_BIT != 0
    }

    /// 标记可升级读持锁。
    #[inline]
    pub(super) fn set_upgr_active_(&self) {
        self.state_.fetch_or(UPGR_BIT, Ordering::AcqRel);
    }

    /// 清除可升级读持锁标记。
    #[inline]
    pub(super) fn clear_upgr_active_(&self) {
        self.state_.fetch_and(!UPGR_BIT, Ordering::AcqRel);
    }

    /// 最后加入的读者节点。
    ///
    /// # Safety
    ///
    /// 返回值只在读取瞬间保证是本组当前的尾节点；调用方必须持有队列锁才能
    /// 安全地沿 `prev_` 遍历（遍历期间不会有新的发布者）。
    #[inline]
    pub(super) unsafe fn tail_(&self) -> Option<NonNull<WaitNode>> {
        NonNull::new(self.tail_.load(Ordering::Acquire))
    }

    /// 把读者节点压入组尾（原子 CAS）。
    ///
    /// # Safety
    ///
    /// - `node` 必须是刚分配、尚未发布到任何链表的读者节点；
    /// - 遍历者必须持有队列锁，保证发布后 `prev_` 链可安全读取。
    pub(super) unsafe fn push_reader_(&self, node: NonNull<WaitNode>) {
        let mut old = self.tail_.load(Ordering::Acquire);
        loop {
            // SAFETY: `node` 尚未发布，独占写自己的 `prev_`。
            unsafe { node.as_ref().set_prev_(NonNull::new(old)) };
            match self.tail_.compare_exchange_weak(
                old,
                node.as_ptr(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Result::Ok(_) => return,
                Result::Err(actual) => old = actual,
            }
        }
    }

    /// 链中是否还有尚未放行的读者（`Registering` 或 `Waiting`）。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn has_pending_(&self) -> bool {
        let mut cur = self.tail_.load(Ordering::Acquire);
        while let Some(node) = NonNull::new(cur) {
            // SAFETY: 链节点在组存活期间有效；持有队列锁。
            let node_ref = unsafe { node.as_ref() };
            if node_ref.as_reader_().is_pending_() {
                return true;
            }
            // SAFETY: 同上。
            cur = match unsafe { node_ref.prev_() } {
                Some(prev) => prev.as_ptr(),
                Option::None => core::ptr::null_mut(),
            };
        }
        false
    }

    /// 链中是否已有可升级读节点。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn has_upgr_read_(&self) -> bool {
        let mut cur = self.tail_.load(Ordering::Acquire);
        while let Some(node) = NonNull::new(cur) {
            // SAFETY: 链节点在组存活期间有效；持有队列锁。
            let node_ref = unsafe { node.as_ref() };
            if node_ref.is_upgr_read_() {
                return true;
            }
            // SAFETY: 同上。
            cur = match unsafe { node_ref.prev_() } {
                Some(prev) => prev.as_ptr(),
                Option::None => core::ptr::null_mut(),
            };
        }
        false
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// ReaderNode
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个读者链节点。
pub(super) struct ReaderNode {
    /// 所属读者组。
    root_: NonNull<WaitNode>,
    /// 读者状态机；无需队列锁。
    state_: AtomicU8,
    /// waker；只在 `Registering` / `Taking` 状态下被独占访问。
    waker_: UnsafeCell<Option<Waker>>,
}

impl ReaderNode {
    fn new(root: NonNull<WaitNode>) -> Self {
        ReaderNode {
            root_: root,
            state_: AtomicU8::new(READER_REGISTERING),
            waker_: UnsafeCell::new(Option::None),
        }
    }

    /// 所属读者组。
    #[inline]
    pub(super) fn root_(&self) -> NonNull<WaitNode> {
        self.root_
    }

    /// 当前状态。
    #[inline]
    pub(super) fn state_(&self) -> u8 {
        self.state_.load(Ordering::Acquire)
    }

    /// 是否尚未放行（`Registering` / `Waiting`）。
    #[inline]
    pub(super) fn is_pending_(&self) -> bool {
        matches!(self.state_(), READER_REGISTERING | READER_WAITING)
    }

    /// 记录（或刷新）waker，并把节点置为 `Waiting`。
    ///
    /// 只允许从 `Registering` / `Waiting` 进入；如果期间节点已经被唤醒者
    /// 放行为 `Admitted`，返回 `false`，调用方应转入领取流程而不是把它
    /// 降回 `Waiting`（否则会把已累加的持锁计数弄丢）。
    pub(super) fn register_waker_(&self, waker: &Waker) -> bool {
        loop {
            let old = self.state_.load(Ordering::Acquire);
            match old {
                READER_ADMITTED | READER_ACQUIRED | READER_CANCELLED => return false,
                READER_TAKING => {
                    hint::spin_loop();
                    continue;
                }
                READER_REGISTERING | READER_WAITING => {}
                _ => return false,
            }
            if self
                .state_
                .compare_exchange(old, READER_REGISTERING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            // SAFETY: 状态为 `Registering`，唤醒者不会访问 waker。
            let slot = unsafe { &mut *self.waker_.get() };
            if !slot.as_ref().is_some_and(|w| w.will_wake(waker)) {
                *slot = Option::Some(waker.clone());
            }
            if self
                .state_
                .compare_exchange(
                    READER_REGISTERING,
                    READER_WAITING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                // SAFETY: 状态已不是 `Registering`，没有其他线程会取这个
                // waker；清掉它，避免泄漏。
                unsafe { *self.waker_.get() = Option::None };
                return false;
            }
            return true;
        }
    }

    /// 从 `Admitted`（或 `Registering` / `Waiting`）重新挂起并注册 waker。
    ///
    /// 调用方必须已经把放行时累加的持锁计数减掉。
    pub(super) fn rewait_(&self, waker: &Waker) -> bool {
        loop {
            let old = self.state_.load(Ordering::Acquire);
            match old {
                READER_ACQUIRED | READER_CANCELLED => return false,
                READER_TAKING => {
                    hint::spin_loop();
                    continue;
                }
                READER_ADMITTED | READER_REGISTERING | READER_WAITING => {}
                _ => return false,
            }
            if self
                .state_
                .compare_exchange(old, READER_REGISTERING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            // SAFETY: 状态为 `Registering`，唤醒者不会访问 waker。
            let slot = unsafe { &mut *self.waker_.get() };
            if !slot.as_ref().is_some_and(|w| w.will_wake(waker)) {
                *slot = Option::Some(waker.clone());
            }
            if self
                .state_
                .compare_exchange(
                    READER_REGISTERING,
                    READER_WAITING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                // SAFETY: 状态已不是 `Registering`，没有其他线程会取这个
                // waker；清掉它，避免泄漏。
                unsafe { *self.waker_.get() = Option::None };
                return false;
            }
            return true;
        }
    }

    /// 唤醒者取走 waker 并把节点转成 `Admitted`。
    ///
    /// 返回 `None` 表示节点不在 `Waiting`，或 waker 已经不在。
    pub(super) fn try_admit_(&self) -> Option<Waker> {
        if self
            .state_
            .compare_exchange(
                READER_WAITING,
                READER_TAKING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Option::None;
        }
        // SAFETY: 状态为 `Taking`，唤醒者独占 waker。
        let waker = unsafe { &mut *self.waker_.get() }.take();
        match waker {
            Option::Some(waker) => {
                self.state_.store(READER_ADMITTED, Ordering::Release);
                Option::Some(waker)
            }
            Option::None => {
                // `Waiting` 状态必须已经注册 waker；这里回退，避免把节点
                // 留在一个不会被唤醒的 `Admitted` 状态。
                debug_assert!(false, "ReaderNode: Waiting 状态缺少 waker");
                self.state_.store(READER_WAITING, Ordering::Release);
                Option::None
            }
        }
    }

    /// 标记已领取许可。
    pub(super) fn mark_acquired_(&self) {
        self.state_.store(READER_ACQUIRED, Ordering::Release);
    }

    /// 取消读者；返回取消前的状态。
    ///
    /// 不需要队列锁；如果唤醒者正在 `Taking`，短暂自旋等它取完 waker。
    pub(super) fn cancel_(&self) -> u8 {
        loop {
            let old = self.state_.load(Ordering::Acquire);
            match old {
                READER_TAKING => {
                    hint::spin_loop();
                    continue;
                }
                READER_ACQUIRED | READER_CANCELLED => return old,
                _ => {}
            }
            if self
                .state_
                .compare_exchange(old, READER_CANCELLED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // `Waiting` 的 waker 还没被取走，清掉它。`Registering` 的
                // waker 可能正在写；这里只可能由同一 future 的 Drop 触发，
                // 与注册不会并发，清净处理即可。
                if matches!(old, READER_WAITING | READER_REGISTERING) {
                    // SAFETY: 状态已变成 `Cancelled`，唤醒者不会再取 waker。
                    unsafe { *self.waker_.get() = Option::None };
                }
                return old;
            }
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// WriteNode
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个写者链节点。
///
/// 写者节点仍由队列锁保护；它们不在读者快速路径上。
pub(super) struct WriteNode {
    /// `Write` 或 `Upgrade`。
    kind_: WaitKind,
    /// 升级节点对应的读者组；普通写者为 `None`。
    root_: Option<NonNull<WaitNode>>,
    /// 是否已经计入 `writer_waiters_`；由队列锁保护。
    counted_: Cell<bool>,
    /// 槽位状态；由队列锁保护。
    state_: Cell<SlotState>,
    /// 已注册的 waker；由队列锁保护。
    waker_: UnsafeCell<Option<Waker>>,
}

impl WriteNode {
    fn new(kind: WaitKind, root: Option<NonNull<WaitNode>>) -> Self {
        WriteNode {
            kind_: kind,
            root_: root,
            counted_: Cell::new(false),
            state_: Cell::new(SlotState::Waiting),
            waker_: UnsafeCell::new(Option::None),
        }
    }

    /// 等待者种类。
    #[inline]
    pub(super) fn kind_(&self) -> WaitKind {
        self.kind_
    }

    /// 对应的读者组。
    #[inline]
    pub(super) fn root_(&self) -> Option<NonNull<WaitNode>> {
        self.root_
    }

    /// 是否已计入写者等待计数。
    #[inline]
    pub(super) fn counted_(&self) -> bool {
        self.counted_.get()
    }

    /// 设置写者等待计数标记。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn set_counted_(&self, counted: bool) {
        self.counted_.set(counted);
    }

    /// 当前状态。
    #[inline]
    pub(super) fn state_(&self) -> SlotState {
        self.state_.get()
    }

    /// 设置状态。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn set_state_(&self, state: SlotState) {
        self.state_.set(state);
    }

    /// 取走 waker。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn take_waker_(&self) -> Option<Waker> {
        // SAFETY: 由调用方保证持有队列锁。
        unsafe { &mut *self.waker_.get() }.take()
    }

    /// 记录（或刷新）waker。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn set_waker_(&self, waker: &Waker) {
        // SAFETY: 由调用方保证持有队列锁。
        let slot = unsafe { &mut *self.waker_.get() };
        if slot.as_ref().is_some_and(|w| w.will_wake(waker)) {
            return;
        }
        *slot = Option::Some(waker.clone());
    }

    /// 清空 waker。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn clear_waker_(&self) {
        // SAFETY: 由调用方保证持有队列锁。
        unsafe { *self.waker_.get() = Option::None };
    }
}

/// 写者槽位状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SlotState {
    /// 仍在等待放行。
    Waiting,
    /// 已放行，waker 已交出，等待 future 自己领取。
    Admitted,
    /// 已成功领取许可。
    Acquired,
    /// 等待者被取消。
    Cancelled,
}
