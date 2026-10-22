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
//! # 可变性
//!
//! 节点一旦放进 [`Arena`](super::arena_::Arena) 就不会移动，地址在存活期间
//! 稳定。多个线程会同时持有 `&WaitNode`（读者的 `root_`、`PendingWait`），
//! 因此这里**不**制造 `&mut WaitNode`：
//!
//! - 兄弟指针、`RootRead` 的链头/链尾/封口标记由队列锁保护，通过
//!   [`UnsafeCell`] 就地修改；
//! - `RootRead.count_` / `pending_` 用原子量维护，读者释放/领取不需要队列锁；
//! - 读者/写者槽位状态与 waker 由节点自己的 [`SpinLock`] 保护；
//! - 节点内容（`WakeNode` 变体、`kind_`、`root_`）在创建后不再变化。

use core::{
    cell::{Cell, UnsafeCell},
    ptr::NonNull,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Waker,
};

use crate::rwlock::wait_::{SpinLock, WaitKind};

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
// 链指针的读写都在队列锁内完成（读者链封口后 `next_` 只读），其余可变状态由
// 原子量或 `SpinLock` 保护。`Waker` 本身满足 `Send + Sync`。
unsafe impl Send for WaitNode {}
// SAFETY: 同上；共享 `&WaitNode` 只允许访问原子量、`SpinLock`、以及封口后的
// 只读链指针。队列锁是链指针唯一的写入口。
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

    /// 建立一个休眠的升级预留节点（`Reserved`，不计入写者等待计数）。
    pub(super) fn new_reserved_upgrade(root: NonNull<WaitNode>) -> Self {
        WaitNode {
            prev_: UnsafeCell::new(Option::None),
            next_: UnsafeCell::new(Option::None),
            content_: WakeNode::PureWrite(WriteNode::new_reserved(root)),
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
    /// 调用方必须持有队列锁，或保证本节点已经脱离主队列且没有并发写者。
    #[inline]
    pub(super) unsafe fn prev_(&self) -> Option<NonNull<WaitNode>> {
        // SAFETY: 由调用方保证对 `prev_` 的独占访问。
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
    /// 主队列节点要求持有队列锁；读者链节点的 `next_` 在封口后只读，可以在
    /// 队列锁外读取（节点由 `waking_` / `count_` 保持存活）。
    #[inline]
    pub(super) unsafe fn next_(&self) -> Option<NonNull<WaitNode>> {
        // SAFETY: 由调用方保证对 `next_` 的独占访问，或满足只读条件。
        unsafe { *self.next_.get() }
    }

    /// 写入后继。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁；读者链节点的 `next_` 只能在封口前写入。
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
pub(super) struct RootRead {
    /// 当前持锁读者数；领取时加一，释放时减一。
    count_: AtomicUsize,
    /// 已放行但尚未领取的读者数。
    pending_: AtomicUsize,
    /// 当前是否有已领取的可升级读持锁者；用于判断 count 减到 1 时剩下的
    /// 是否就是可升级读。
    upgr_active_: AtomicBool,
    /// 正在进行“释放收尾”的线程数。最后一个计数归零不立刻回收，必须等
    /// 所有并发释放收尾都结束后再由队首 prune 统一回收，避免 2→1 的线程
    /// 还在访问组时被 1→0 的线程释放。
    release_ops_: AtomicUsize,
    /// 由队列锁保护的拓扑与标记。
    state_: UnsafeCell<RootState>,
}

/// [`RootRead`] 中由队列锁保护的部分。
pub(super) struct RootState {
    /// 读者链头。
    pub(super) head_: Option<NonNull<WaitNode>>,
    /// 读者链尾，用于 O(1) 追加。
    pub(super) tail_: Option<NonNull<WaitNode>>,
    /// 是否已封口；封口后不再接收新的读类等待者。
    pub(super) sealed_: bool,
    /// 是否有唤醒者正在遍历本组。
    pub(super) waking_: bool,
    /// 本组预留的升级写节点。
    pub(super) upgrade_: Option<NonNull<WaitNode>>,
    /// 是否仍在主队列中。
    pub(super) linked_: bool,
}

impl RootRead {
    fn new() -> Self {
        RootRead {
            count_: AtomicUsize::new(0),
            pending_: AtomicUsize::new(0),
            upgr_active_: AtomicBool::new(false),
            release_ops_: AtomicUsize::new(0),
            state_: UnsafeCell::new(RootState {
                head_: Option::None,
                tail_: Option::None,
                sealed_: false,
                waking_: false,
                upgrade_: Option::None,
                linked_: false,
            }),
        }
    }

    /// 当前持锁读者数。
    #[inline]
    pub(super) fn count_(&self) -> usize {
        self.count_.load(Ordering::Acquire)
    }

    /// 领取时加一。
    #[inline]
    pub(super) fn inc_count_(&self) {
        self.count_.fetch_add(1, Ordering::AcqRel);
    }

    /// 释放时减一；返回减一前的值。
    #[inline]
    pub(super) fn dec_count_(&self) -> usize {
        self.count_.fetch_sub(1, Ordering::AcqRel)
    }

    /// 已放行未领取的读者数。
    #[inline]
    pub(super) fn pending_(&self) -> usize {
        self.pending_.load(Ordering::Acquire)
    }

    /// 放行一个读者。
    #[inline]
    pub(super) fn inc_pending_(&self) {
        self.pending_.fetch_add(1, Ordering::AcqRel);
    }

    /// 领取或取消一个已放行读者；返回减一前的值。
    #[inline]
    pub(super) fn dec_pending_(&self) -> usize {
        self.pending_.fetch_sub(1, Ordering::AcqRel)
    }

    /// 当前是否有已领取的可升级读持锁者。
    #[inline]
    pub(super) fn upgr_active_(&self) -> bool {
        self.upgr_active_.load(Ordering::Acquire)
    }

    /// 设置可升级读持锁标记。
    #[inline]
    pub(super) fn set_upgr_active_(&self, active: bool) {
        self.upgr_active_.store(active, Ordering::Release);
    }

    /// 进入释放收尾。
    #[inline]
    pub(super) fn begin_release_(&self) {
        self.release_ops_.fetch_add(1, Ordering::AcqRel);
    }

    /// 结束释放收尾。
    #[inline]
    pub(super) fn end_release_(&self) {
        self.release_ops_.fetch_sub(1, Ordering::AcqRel);
    }

    /// 当前是否仍有释放收尾在进行。
    #[inline]
    pub(super) fn release_ops_(&self) -> usize {
        self.release_ops_.load(Ordering::Acquire)
    }

    /// 取得队列锁保护的拓扑状态。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub(super) unsafe fn state_(&self) -> &mut RootState {
        // SAFETY: 队列锁保证同一时刻只有一个执行流访问 `state_`；
        // `count_` / `pending_` 不在 `RootState` 内，可以并发访问。
        unsafe { &mut *self.state_.get() }
    }

    /// 读取读者链头。
    ///
    /// # Safety
    ///
    /// 读者链必须已经封口且当前没有并发追加（通常由 `waking_` 保证）。
    /// 这里只复制 `Option<NonNull<_>>`，不制造 `&mut RootState`。
    #[inline]
    pub(super) unsafe fn head_(&self) -> Option<NonNull<WaitNode>> {
        // SAFETY: 由调用方保证此刻没有对 `state_` 的并发写入。
        unsafe { (*self.state_.get()).head_ }
    }
}

impl RootState {
    /// 把读者节点追加到链尾。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn push_reader_(&mut self, node: NonNull<WaitNode>) {
        // SAFETY: `node` 是刚分配、尚未入链的读者节点。
        unsafe {
            node.as_ref().set_next_(Option::None);
        }
        match self.tail_ {
            // SAFETY: 旧尾节点在本组存活期间有效。
            Some(tail) => unsafe { tail.as_ref().set_next_(Option::Some(node)) },
            None => self.head_ = Option::Some(node),
        }
        self.tail_ = Option::Some(node);
    }

    /// 链中是否还有仍在等待（`Waiting`）的读者。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn has_waiting_(&self) -> bool {
        let mut cur = self.head_;
        while let Some(node) = cur {
            // SAFETY: 链节点在本组存活期间有效。
            let node_ref = unsafe { node.as_ref() };
            if node_ref.as_reader_().is_waiting_() {
                return true;
            }
            // SAFETY: 链已封口或由队列锁保护，`next_` 稳定。
            cur = unsafe { node_ref.next_() };
        }
        false
    }

    /// 链中是否存在任何可升级读节点。
    ///
    /// # Safety
    ///
    /// 调用方必须持有队列锁。
    pub(super) unsafe fn has_upgr_read_(&self) -> bool {
        let mut cur = self.head_;
        while let Some(node) = cur {
            // SAFETY: 链节点在本组存活期间有效。
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
    /// 槽位状态与 waker。
    slot_: SpinLock<ReaderSlot>,
}

/// 读者槽位。
pub(super) struct ReaderSlot {
    /// 当前状态。
    pub(super) state_: SlotState,
    /// 已注册的 waker。
    pub(super) waker_: Option<Waker>,
}

impl ReaderNode {
    fn new(root: NonNull<WaitNode>) -> Self {
        ReaderNode {
            root_: root,
            slot_: SpinLock::new(ReaderSlot {
                state_: SlotState::Waiting,
                waker_: Option::None,
            }),
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
        self.slot_.lock().state_
    }

    /// 是否仍在等待放行。
    #[inline]
    pub(super) fn is_waiting_(&self) -> bool {
        self.state_() == SlotState::Waiting
    }

    /// 放行本节点：`Waiting -> Admitted`，交出 waker。
    ///
    /// 同时把所属组的 `pending_` 加一；返回 `None` 表示没有 waker（仍未放行）。
    pub(super) fn admit_(&self) -> Option<Waker> {
        let mut slot = self.slot_.lock();
        if slot.state_ != SlotState::Waiting {
            return Option::None;
        }
        let waker = slot.waker_.take()?;
        slot.state_ = SlotState::Admitted;
        // SAFETY: `root_` 指向本节点的读者组，组在 `count_`/`pending_` 期间存活。
        unsafe { self.root_.as_ref() }
            .as_root_read_()
            .inc_pending_();
        Option::Some(waker)
    }

    /// 记录（或刷新）waker；返回转换前的状态。
    ///
    /// `Admitted -> Waiting` 时把组的 `pending_` 减一，因为该读者又回到了
    /// “尚未放行”的状态。
    pub(super) fn set_waker_(&self, waker: &Waker) -> SlotState {
        let mut slot = self.slot_.lock();
        let old = slot.state_;
        slot.state_ = SlotState::Waiting;
        if old == SlotState::Admitted {
            // SAFETY: 组仍存活；`root_` 有效。
            unsafe { self.root_.as_ref() }
                .as_root_read_()
                .dec_pending_();
        }
        if slot
            .waker_
            .as_ref()
            .is_some_and(|existing| existing.will_wake(waker))
        {
            return old;
        }
        slot.waker_ = Option::Some(waker.clone());
        old
    }

    /// 标注已领取；返回转换前的状态。
    ///
    /// 领取时把组的 `count_` 加一，并在从 `Admitted` 领取时把 `pending_`
    /// 减一。先加 `count_` 再减 `pending_`，保证最后一个释放者看到
    /// `count_ == 0` 时不可能还有正在领取的节点。
    pub(super) fn claim_(&self, is_upgradable: bool) -> Option<SlotState> {
        let mut slot = self.slot_.lock();
        let old = slot.state_;
        if !matches!(old, SlotState::Waiting | SlotState::Admitted) {
            return Option::None;
        }
        slot.state_ = SlotState::Acquired;
        slot.waker_ = Option::None;
        // SAFETY: 组仍存活；`root_` 有效。
        let root = unsafe { self.root_.as_ref() }.as_root_read_();
        root.inc_count_();
        if is_upgradable {
            root.set_upgr_active_(true);
        }
        if old == SlotState::Admitted {
            root.dec_pending_();
        }
        Option::Some(old)
    }

    /// 标注取消；返回转换前的状态。
    pub(super) fn cancel_(&self) -> SlotState {
        let mut slot = self.slot_.lock();
        let old = slot.state_;
        if matches!(old, SlotState::Waiting | SlotState::Admitted) {
            slot.state_ = SlotState::Cancelled;
            slot.waker_ = Option::None;
            if old == SlotState::Admitted {
                // SAFETY: 组仍存活；`root_` 有效。
                unsafe { self.root_.as_ref() }
                    .as_root_read_()
                    .dec_pending_();
            }
        }
        old
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
    /// 是否已经在主队列中；由队列锁保护。
    linked_: Cell<bool>,
    /// 是否已经计入 `writer_waiters_`；由队列锁保护。
    counted_: Cell<bool>,
    /// 槽位状态与 waker。
    slot_: SpinLock<WriteSlot>,
}

/// 写者槽位。
pub(super) struct WriteSlot {
    /// 当前状态。
    pub(super) state_: SlotState,
    /// 已注册的 waker。
    pub(super) waker_: Option<Waker>,
}

impl WriteNode {
    fn new(kind: WaitKind, root: Option<NonNull<WaitNode>>) -> Self {
        WriteNode {
            kind_: kind,
            root_: root,
            linked_: Cell::new(false),
            counted_: Cell::new(false),
            slot_: SpinLock::new(WriteSlot {
                state_: SlotState::Waiting,
                waker_: Option::None,
            }),
        }
    }

    fn new_reserved(root: NonNull<WaitNode>) -> Self {
        WriteNode {
            kind_: WaitKind::Upgrade,
            root_: Option::Some(root),
            linked_: Cell::new(false),
            counted_: Cell::new(false),
            slot_: SpinLock::new(WriteSlot {
                state_: SlotState::Reserved,
                waker_: Option::None,
            }),
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

    /// 是否已经在主队列中。
    #[inline]
    pub(super) fn linked_(&self) -> bool {
        self.linked_.get()
    }

    /// 设置主队列链接标记。
    #[inline]
    pub(super) fn set_linked_(&self, linked: bool) {
        self.linked_.set(linked);
    }

    /// 是否已计入写者等待计数。
    #[inline]
    pub(super) fn counted_(&self) -> bool {
        self.counted_.get()
    }

    /// 设置写者等待计数标记。
    #[inline]
    pub(super) fn set_counted_(&self, counted: bool) {
        self.counted_.set(counted);
    }

    /// 当前状态。
    #[inline]
    pub(super) fn state_(&self) -> SlotState {
        self.slot_.lock().state_
    }

    /// 接管一个休眠的升级预留节点；返回它此前是否为 `Reserved`。
    pub(super) fn adopt_(&self, waker: &Waker) -> bool {
        let mut slot = self.slot_.lock();
        let old = slot.state_;
        slot.state_ = SlotState::Waiting;
        if slot
            .waker_
            .as_ref()
            .is_some_and(|existing| existing.will_wake(waker))
        {
            return old == SlotState::Reserved;
        }
        slot.waker_ = Option::Some(waker.clone());
        old == SlotState::Reserved
    }

    /// 放行本节点：`Waiting -> Admitted`，交出 waker。
    pub(super) fn admit_(&self) -> Option<Waker> {
        let mut slot = self.slot_.lock();
        if slot.state_ != SlotState::Waiting {
            return Option::None;
        }
        let waker = slot.waker_.take()?;
        slot.state_ = SlotState::Admitted;
        Option::Some(waker)
    }

    /// 记录（或刷新）waker；返回转换前的状态。
    pub(super) fn set_waker_(&self, waker: &Waker) -> SlotState {
        let mut slot = self.slot_.lock();
        let old = slot.state_;
        if old != SlotState::Reserved {
            slot.state_ = SlotState::Waiting;
        }
        if slot
            .waker_
            .as_ref()
            .is_some_and(|existing| existing.will_wake(waker))
        {
            return old;
        }
        slot.waker_ = Option::Some(waker.clone());
        old
    }

    /// 标注已领取；返回转换前的状态。
    pub(super) fn claim_(&self) -> Option<SlotState> {
        let mut slot = self.slot_.lock();
        let old = slot.state_;
        if !matches!(old, SlotState::Waiting | SlotState::Admitted) {
            return Option::None;
        }
        slot.state_ = SlotState::Acquired;
        slot.waker_ = Option::None;
        Option::Some(old)
    }

    /// 标注取消；返回转换前的状态。
    pub(super) fn cancel_(&self) -> SlotState {
        let mut slot = self.slot_.lock();
        let old = slot.state_;
        if matches!(
            old,
            SlotState::Waiting | SlotState::Admitted | SlotState::Reserved
        ) {
            slot.state_ = SlotState::Cancelled;
            slot.waker_ = Option::None;
        }
        old
    }
}

/// 槽位状态。
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
    /// 升级预留节点，尚未被升级 future 接管。
    Reserved,
}
