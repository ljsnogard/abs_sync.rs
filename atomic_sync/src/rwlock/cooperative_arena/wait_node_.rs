//! 统一的等待节点：`WaitNode` 与内容枚举 `WakeNode`。
//!
//! # 布局
//!
//! 读者节点与写者节点使用完全相同的布局：一对兄弟指针 `prev_` / `next_`
//! 加一个 [`WakeNode`]。读者链节点只用 `next_` 串成单链（`prev_` 保持
//! `None`），主队列节点同时使用 `prev_` / `next_`。
//!
//! ```text
//! 主队列:   RootRead ──> PureWrite ──> RootRead ──> ...
//!              │
//! 读者链:   PureRead ──> PureRead ──> UpgrRead（至多一个）
//! ```
//!
//! # 同步
//!
//! 节点放进 [`Arena`](super::arena_::Arena) 后地址稳定。主队列/读者链、
//! 节点状态与 waker **全部由等待队列的自旋锁保护**；读者释放只对
//! [`RootRead`] 的单个原子做减一，不需要队列锁。因此这里不再为每个节点
//! 单独加锁。
//!
//! [`RootRead`] 的原子同时承载两项信息：
//!
//! - 低位：已放行且尚未释放的读者数（含已放行、尚未领取的读者）；
//! - 最高位：当前是否有已领取的可升级读持锁者。
//!
//! 最后一个把计数减到 0 的读者负责收尾；没有单独的 pending 计数或
//! release_ops 计数。

use core::{
    cell::{Cell, UnsafeCell},
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
    task::Waker,
};

use crate::rwlock::wait_::WaitKind;

/// 可升级读持锁标记在 [`RootRead::state_`] 中的位置。
const UPGR_BIT: usize = 1 << (usize::BITS - 1);
/// 读者计数掩码。
const COUNT_MASK: usize = !UPGR_BIT;

/// 一个等待节点。
///
/// 所有变体共享同一对兄弟指针；具体含义由 [`WakeNode`] 决定。
pub(super) struct WaitNode {
    /// 主队列前驱；读者链节点为 `None`。
    prev_: UnsafeCell<Option<NonNull<WaitNode>>>,
    /// 主队列后继，或读者链中的下一个读者。
    next_: UnsafeCell<Option<NonNull<WaitNode>>>,
    /// 节点内容。
    content_: WakeNode,
}

// SAFETY: `WaitNode` 里的裸指针只指向同一个 Arena 内、地址稳定的节点；所有
// 链指针、节点状态与 waker 的访问都在队列锁内完成，`RootRead.state_` 是
// 原子量。`Waker` 本身满足 `Send + Sync`。
unsafe impl Send for WaitNode {}
// SAFETY: 同上；共享 `&WaitNode` 只允许在队列锁内访问 `Cell`/`UnsafeCell`，
// 或在锁外读取原子量。
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
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn prev_(&self) -> Option<NonNull<WaitNode>> {
        // SAFETY: 由调用方保证持有队列锁。
        unsafe { *self.prev_.get() }
    }

    /// 写入前驱。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn set_prev_(&self, prev: Option<NonNull<WaitNode>>) {
        // SAFETY: 由调用方保证持有队列锁。
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
    /// 读者组根：持锁计数 + 读者链。
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
/// `state_` 的低位是持锁读者数，最高位是可升级读持锁标记；其余字段由队列锁
/// 保护。
pub(super) struct RootRead {
    state_: AtomicUsize,
    /// 读者链头。
    head_: Cell<Option<NonNull<WaitNode>>>,
    /// 读者链尾，用于 O(1) 追加。
    tail_: Cell<Option<NonNull<WaitNode>>>,
}

impl RootRead {
    fn new() -> Self {
        RootRead {
            state_: AtomicUsize::new(0),
            head_: Cell::new(Option::None),
            tail_: Cell::new(Option::None),
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

    /// 释放/取消一个读者时减一；返回减一前的计数。
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

    /// 读者链头。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn head_(&self) -> Option<NonNull<WaitNode>> {
        self.head_.get()
    }

    /// 设置读者链头。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn set_head_(&self, head: Option<NonNull<WaitNode>>) {
        self.head_.set(head);
    }

    /// 设置读者链尾。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    pub(super) unsafe fn set_tail_(&self, tail: Option<NonNull<WaitNode>>) {
        self.tail_.set(tail);
    }

    /// 把读者节点追加到链尾。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn push_reader_(&self, node: NonNull<WaitNode>) {
        // SAFETY: `node` 是刚分配、尚未入链的读者节点；持有队列锁。
        unsafe {
            node.as_ref().set_next_(Option::None);
            match self.tail_.get() {
                Some(tail) => tail.as_ref().set_next_(Option::Some(node)),
                None => self.head_.set(Option::Some(node)),
            }
        }
        self.tail_.set(Option::Some(node));
    }

    /// 链中是否还有仍在等待的读者。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn has_waiting_(&self) -> bool {
        let mut cur = self.head_.get();
        while let Some(node) = cur {
            // SAFETY: 链节点在组存活期间有效；持有队列锁。
            let node_ref = unsafe { node.as_ref() };
            if node_ref.as_reader_().state_() == SlotState::Waiting {
                return true;
            }
            // SAFETY: 同上。
            cur = unsafe { node_ref.next_() };
        }
        false
    }

    /// 链中是否已有可升级读节点。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn has_upgr_read_(&self) -> bool {
        let mut cur = self.head_.get();
        while let Some(node) = cur {
            // SAFETY: 链节点在组存活期间有效；持有队列锁。
            let node_ref = unsafe { node.as_ref() };
            if node_ref.is_upgr_read_() {
                return true;
            }
            // SAFETY: 同上。
            cur = unsafe { node_ref.next_() };
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
    /// 槽位状态；由队列锁保护。
    state_: Cell<SlotState>,
    /// 已注册的 waker；由队列锁保护。
    waker_: UnsafeCell<Option<Waker>>,
}

impl ReaderNode {
    fn new(root: NonNull<WaitNode>) -> Self {
        ReaderNode {
            root_: root,
            state_: Cell::new(SlotState::Waiting),
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

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// WriteNode
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个写者链节点。
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

/// 槽位状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SlotState {
    /// 仍在等待放行。
    Waiting,
    /// 已放行并计入读者组计数，等待 future 自己领取。
    Admitted,
    /// 已成功领取许可。
    Acquired,
    /// 等待者被取消。
    Cancelled,
}
