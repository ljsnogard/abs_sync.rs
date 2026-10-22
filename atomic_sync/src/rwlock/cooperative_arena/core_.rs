//! 协作式读写锁内核：状态字 + `WaitNode` 主队列 + RootRead 读者链。
//!
//! # 队列结构
//!
//! ```text
//! 主队列（prev_/next_）: RootRead ──> PureWrite ──> RootRead ──> ...
//!                          │
//! 读者链（next_ 单链）:     PureRead ──> PureRead ──> UpgrRead（至多一个）
//! ```
//!
//! # 同步模型
//!
//! - 队列锁保护主队列、读者链、节点状态与 waker；不再为每个节点加锁。
//! - 唤醒时在队列锁内把一批读者标成 `Admitted` 并取出 waker，释放锁后再
//!   `wake`；下一轮重新从队首选择，继续下一批。
//! - `RootRead` 的单个原子同时保存持锁计数与可升级读标记；读者释放/取消
//!   只做原子减一，最后一个减到 0 的线程触发 `run_pass_`，由队首 prune
//!   统一回收空组。
//! - 升级请求只有在真正发起时才插入 `PureWrite(Upgrade)`；不做预留。

use alloc::alloc::{Allocator, Global, handle_alloc_error};
use core::{borrow::BorrowMut, hint, ptr::NonNull, sync::atomic::AtomicPtr, task::Waker};

use funty::Unsigned;

use atomex::{AtomexPtr, StrictOrderings, TrAtomicData, TrCmpxchOrderings, x_deps::funty};

use crate::rwlock::{
    state_::CoopRwState,
    wait_::{SpinLock, WaitKind},
};

use super::{
    arena_::Arena,
    wait_node_::{
        READER_ACQUIRED, READER_ADMITTED, READER_CANCELLED, READER_REGISTERING, READER_TAKING,
        READER_WAITING, ReaderNode, SLOT_ACQUIRED, SLOT_ADMITTED, SLOT_CANCELLED, SLOT_WAITING,
        WaitNode,
    },
};

/// 等待中 future 持有的句柄：一个 `WaitNode` 元素。
#[derive(Clone, Copy)]
pub(super) struct PendingWait {
    node_: NonNull<WaitNode>,
    kind_: WaitKind,
}

// SAFETY: 句柄只保存一个 Arena 元素的地址。元素由 Arena 持有，地址在存活期间
// 稳定；所有解引用都发生在队列锁内，移动句柄本身不会产生别名访问。
unsafe impl Send for PendingWait {}
// SAFETY: 同上；共享 `&PendingWait` 只读取地址与 kind，不提供可变访问。
unsafe impl Sync for PendingWait {}

/// 一次获取动作的当前进展。
pub(super) enum AcqProgress {
    /// 已经拿到许可。
    Acquired,
    /// 仍在队列里等待；future 需要记住这个句柄以便重试与取消。
    Waiting(PendingWait),
}

/// 队列自旋锁保护的载荷。
struct QueuePayload<A: Allocator> {
    /// 主队列队首。
    head_: Option<NonNull<WaitNode>>,
    /// 主队列队尾。
    tail_: Option<NonNull<WaitNode>>,
    /// 队列中仍处于待决状态的 `Write` / `Upgrade` 请求数。
    writer_waiters_: usize,
    /// 等待节点池。
    arena_: Arena<WaitNode, A>,
}

// SAFETY: `head_` / `tail_` 只指向同一个 `arena_` 拥有的元素。移动
// `QueuePayload` 时 `Arena` 与链接一起移动，元素地址不变；所有链接读写都在
// 队列自旋锁内完成。Arena 自身在 `WaitNode: Send` 时满足 `Send`。
unsafe impl<A: Allocator + Send> Send for QueuePayload<A> {}

/// 一次选中的放行目标。
enum Selection {
    /// 唤醒一个读者组。
    Group(NonNull<WaitNode>),
    /// 唤醒一个独占等待者。
    Write(NonNull<WaitNode>),
}

/// `run_pass_` 一轮在锁外要执行的动作。
enum WakeAction {
    /// 没有可放行的目标。
    Stop,
    /// 锁外遍历一条读者链；`WAKING` 已由 `select_head_` 置位。
    Group(NonNull<WaitNode>),
    /// 写者 waker 已在锁内取走，锁外只调用 `wake`；`None` 表示状态已变化。
    Write(Option<Waker>),
}

/// Arena 版读写锁的共享内核。
pub(super) struct RwCoreArena<D, B, O = StrictOrderings, A: Allocator = Global>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    stat_: CoopRwState<D, B, O>,
    /// 当前唯一活跃读者组的根；快速路径读者不参与。
    active_read_group_: AtomexPtr<WaitNode>,
    queue_: SpinLock<QueuePayload<A>>,
}

impl<D, B, O> RwCoreArena<D, B, O, Global>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    /// 用全局分配器构造；保留给内部测试与默认路径使用。
    #[cfg(test)]
    pub(super) fn new(cell: B) -> Self {
        Self::new_in(cell, Global)
    }
}

impl<D, B, O, A> RwCoreArena<D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: Allocator,
{
    pub(super) fn new_in(cell: B, alloc: A) -> Self {
        RwCoreArena {
            stat_: CoopRwState::new(cell),
            active_read_group_: AtomexPtr::new(AtomicPtr::new(core::ptr::null_mut())),
            queue_: SpinLock::new(QueuePayload {
                head_: Option::None,
                tail_: Option::None,
                writer_waiters_: 0,
                arena_: Arena::new(alloc),
            }),
        }
    }

    #[inline]
    pub(super) fn reader_count(&self) -> D {
        self.stat_.reader_count()
    }

    /// 队列是否非空（仅供单元测试观测）。
    #[cfg(test)]
    #[inline]
    pub(super) fn waiters_present(&self) -> bool {
        self.stat_.waiters_present()
    }

    /// 队列中是否还有待决的写者/升级等待者（仅供单元测试观测）。
    #[cfg(test)]
    #[inline]
    pub(super) fn writer_queued(&self) -> bool {
        self.stat_.writer_queued()
    }

    /// Arena 池是否为空（仅供单元测试观测批量回收）。
    #[cfg(test)]
    #[inline]
    pub(super) fn pool_is_empty(&self) -> bool {
        self.queue_.lock().arena_.is_empty()
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 快速路径
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    #[inline]
    pub(super) fn try_read_fast(&self) -> bool {
        self.stat_.try_read_fast()
    }

    #[inline]
    pub(super) fn try_write_fast(&self) -> bool {
        self.stat_.try_write_fast()
    }

    #[inline]
    pub(super) fn try_upgradable_read_fast(&self) -> bool {
        self.stat_.try_upgradable_read_fast()
    }

    #[inline]
    pub(super) fn try_upgrade_now(&self) -> bool {
        self.stat_.try_upgrade_to_write()
    }

    #[inline]
    fn try_claim_(&self, kind: WaitKind) -> bool {
        match kind {
            WaitKind::Read => self.stat_.try_read_queued(),
            WaitKind::UpgradableRead => self.stat_.try_upgradable_read_queued(),
            WaitKind::Upgrade => self.stat_.try_upgrade_to_write(),
            WaitKind::Write => self.stat_.try_write_queued(),
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 获取
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn acquire(&self, kind: WaitKind, waker: &Waker) -> AcqProgress {
        self.acquire_inner_(kind, waker)
    }

    /// 发起升级；`tracked` 表示请求者来自队列放行的读者组。
    pub(super) fn acquire_upgrade(&self, waker: &Waker, tracked: bool) -> AcqProgress {
        let (claimed, pending) = {
            let mut q = self.queue_.lock();
            self.prune_front_(&mut q);
            let pending = if tracked {
                match self.active_group_() {
                    Option::Some(group) => self.enqueue_upgrade_in_group_(&mut q, group),
                    Option::None => {
                        debug_assert!(false, "tracked 升级请求缺少活跃读者组");
                        self.enqueue_upgrade_front_(&mut q)
                    }
                }
            } else {
                self.enqueue_upgrade_front_(&mut q)
            };
            if !self.stat_.waiters_present() {
                self.stat_.mark_waiters_present();
            }
            let claimed = self.try_claim_enqueued_(&mut q, &pending);
            if !claimed {
                self.register_waker_(&pending, waker);
            }
            (claimed, pending)
        };
        self.run_pass_();
        if claimed {
            AcqProgress::Acquired
        } else {
            AcqProgress::Waiting(pending)
        }
    }

    fn acquire_inner_(&self, kind: WaitKind, waker: &Waker) -> AcqProgress {
        let (claimed, pending) = {
            let mut q = self.queue_.lock();
            self.prune_front_(&mut q);
            let pending = self.enqueue_(&mut q, kind);
            if !self.stat_.waiters_present() {
                self.stat_.mark_waiters_present();
            }
            let claimed = if kind.is_readish() {
                self.try_claim_reader_locked_(&mut q, &pending)
            } else {
                let claimed = self.try_claim_enqueued_(&mut q, &pending);
                if !claimed {
                    self.register_waker_(&pending, waker);
                }
                claimed
            };
            (claimed, pending)
        };
        if kind.is_readish() && !claimed {
            // 读者 waker 注册不需要队列锁。
            self.register_reader_waker_(pending, waker);
        }
        self.run_pass_();
        if claimed {
            AcqProgress::Acquired
        } else {
            AcqProgress::Waiting(pending)
        }
    }

    /// 读者入队后、仍持有队列锁时尝试直接领取。
    fn try_claim_reader_locked_(&self, q: &mut QueuePayload<A>, pending: &PendingWait) -> bool {
        // SAFETY: `pending.node_` 刚由本队列创建，仍然存活。
        let node = unsafe { pending.node_.as_ref() };
        let reader = node.as_reader_();
        let group = reader.root_();
        if q.head_ != Option::Some(group) {
            return false;
        }
        let snap = self.stat_.snapshot();
        if snap.writer_active {
            return false;
        }
        if pending.kind_ == WaitKind::UpgradableRead && snap.upgrade_active {
            return false;
        }
        if self.try_claim_(pending.kind_) {
            // SAFETY: 组仍存活；`group` 是主队列头。
            let root = unsafe { group.as_ref() }.as_root_read_();
            root.inc_count_();
            if pending.kind_ == WaitKind::UpgradableRead {
                root.set_upgr_active_();
            }
            reader.mark_acquired_();
            self.set_active_group_(group);
            return true;
        }
        false
    }

    fn register_reader_waker_(&self, pending: PendingWait, waker: &Waker) {
        // SAFETY: `pending.node_` 仍存活。
        let node = unsafe { pending.node_.as_ref() };
        let _ = node.as_reader_().register_waker_(waker);
    }

    /// 已被唤醒的等待者重试领取。
    pub(super) fn poll_acquire(&self, pending: &PendingWait, waker: &Waker) -> bool {
        // SAFETY: 句柄对应的节点仍处于待决状态，元素一定还没有被回收。
        let node = unsafe { pending.node_.as_ref() };
        if node.is_reader_() {
            return self.poll_reader_(node.as_reader_(), pending.kind_, waker);
        }

        let mut q = self.queue_.lock();
        let write = node.as_write_();
        let state = write.state_();

        if state == SLOT_ADMITTED {
            if self.try_claim_(pending.kind_) {
                // SAFETY: 持有队列锁。
                unsafe { write.set_state_(SLOT_ACQUIRED) };
                self.finish_write_node_(&mut q, pending.node_);
                drop(q);
                return true;
            }
            // 状态字竞争失败：回到 Waiting。
            // SAFETY: 持有队列锁。
            unsafe {
                write.set_state_(SLOT_WAITING);
                write.set_waker_(waker);
            }
            drop(q);
            self.run_pass_();
            return false;
        }

        if state == SLOT_WAITING {
            self.register_waker_(pending, waker);
            drop(q);
            self.run_pass_();
            return false;
        }

        debug_assert!(false, "poll_acquire: 写者在终态上再次 poll");
        false
    }

    /// 读者领取：只依赖节点原子状态与全局状态字 CAS，不取队列锁。
    fn poll_reader_(&self, reader: &ReaderNode, kind: WaitKind, waker: &Waker) -> bool {
        loop {
            match reader.state_() {
                READER_ADMITTED => {
                    if self.try_claim_(kind) {
                        // SAFETY: 活跃组在计数归零前不会回收。
                        let root = unsafe { reader.root_().as_ref() }.as_root_read_();
                        if kind == WaitKind::UpgradableRead {
                            root.set_upgr_active_();
                        }
                        self.set_active_group_(reader.root_());
                        reader.mark_acquired_();
                        return true;
                    }
                    // 竞争失败：读者回到等待；放行时累加的计数要减一。
                    // SAFETY: 组仍存活；`root_` 有效。
                    unsafe { reader.root_().as_ref() }
                        .as_root_read_()
                        .dec_count_();
                    let _ = reader.rewait_(waker);
                    self.run_pass_();
                    return false;
                }
                READER_REGISTERING | READER_WAITING => {
                    if reader.register_waker_(waker) {
                        self.run_pass_();
                        return false;
                    }
                    // 注册期间节点被放行/取消，重新读取状态。
                }
                READER_TAKING => {
                    hint::spin_loop();
                }
                READER_ACQUIRED | READER_CANCELLED => {
                    debug_assert!(false, "poll_acquire: 读者在终态上再次 poll");
                    return false;
                }
                _ => {
                    debug_assert!(false, "poll_acquire: 未知读者状态");
                    return false;
                }
            }
        }
    }

    /// 等待者被取消（future 被丢弃）。
    pub(super) fn cancel_wait(&self, pending: &PendingWait) {
        // SAFETY: 句柄对应的节点仍处于待决状态，元素一定还没有被回收。
        let node = unsafe { pending.node_.as_ref() };

        if node.is_reader_() {
            let reader = node.as_reader_();
            let old = reader.cancel_();
            if old == READER_ADMITTED {
                // 已放行但还没领取：取消时把放行时累加的计数减一。
                // SAFETY: 组仍存活；`root_` 有效。
                unsafe { reader.root_().as_ref() }
                    .as_root_read_()
                    .dec_count_();
            } else {
                debug_assert!(old != READER_ACQUIRED, "取消已经领取的读者");
            }
            self.run_pass_();
            return;
        }

        let mut q = self.queue_.lock();
        let write = node.as_write_();
        let old = write.state_();
        debug_assert!(old != SLOT_ACQUIRED, "取消已经领取的写者");
        if matches!(old, SLOT_WAITING | SLOT_ADMITTED) {
            // SAFETY: 持有队列锁。
            unsafe {
                write.set_state_(SLOT_CANCELLED);
                write.clear_waker_();
            }
            self.finish_write_node_(&mut q, pending.node_);
        }
        drop(q);
        self.run_pass_();
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 入队
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn enqueue_(&self, q: &mut QueuePayload<A>, kind: WaitKind) -> PendingWait {
        if kind.is_readish() {
            return self.enqueue_reader_(q, kind);
        }
        if kind == WaitKind::Upgrade {
            return self.enqueue_upgrade_front_(q);
        }
        self.enqueue_write_(q)
    }

    fn enqueue_reader_(&self, q: &mut QueuePayload<A>, kind: WaitKind) -> PendingWait {
        if let Option::Some(tail) = q.tail_ {
            // SAFETY: `tail` 是本队列中仍然存活的节点。
            let tail_node = unsafe { tail.as_ref() };
            if tail_node.is_root_read_() {
                let root = tail_node.as_root_read_();
                // 入队先决条件：目标读者链没有被唯一唤醒者独占（WAKING=0）。
                // 若已独占，绝不修改该链，而是新建一个组排到主队列尾；这样
                // 唤醒者遍历的旧链保持静止，新读者也不会被漏掉（新组会在
                // 后续 pass 中被选中）。
                //
                // 另外，主队列尾是 RootRead 才允许追加；一条链最多一个
                // UpgrRead，普通 Read 可以排在它后面。
                let can_join = !root.waking_()
                    && (kind == WaitKind::Read
                        // SAFETY: 持有队列锁，且 WAKING=0 保证链拓扑静止。
                        || !unsafe { root.has_upgr_read_() });
                if can_join {
                    let reader = self.alloc_node_(q, WaitNode::new_reader(kind, tail));
                    // SAFETY: 持有队列锁，且 `reader` 刚分配。
                    unsafe { root.push_reader_(reader) };
                    return PendingWait {
                        node_: reader,
                        kind_: kind,
                    };
                }
            }
        }

        let group = self.alloc_node_(q, WaitNode::new_root_read());
        self.push_back_(q, group);
        let reader = self.alloc_node_(q, WaitNode::new_reader(kind, group));
        // SAFETY: 持有队列锁；`group` 刚插入主队列。
        unsafe { group.as_ref().as_root_read_().push_reader_(reader) };
        PendingWait {
            node_: reader,
            kind_: kind,
        }
    }

    fn enqueue_write_(&self, q: &mut QueuePayload<A>) -> PendingWait {
        let node = self.alloc_node_(q, WaitNode::new_write(WaitKind::Write, Option::None));
        self.push_back_(q, node);
        // SAFETY: 持有队列锁。
        unsafe { node.as_ref().as_write_().set_counted_(true) };
        self.note_exclusive_queued_(q);
        PendingWait {
            node_: node,
            kind_: WaitKind::Write,
        }
    }

    fn enqueue_upgrade_front_(&self, q: &mut QueuePayload<A>) -> PendingWait {
        let node = self.alloc_node_(q, WaitNode::new_write(WaitKind::Upgrade, Option::None));
        self.push_front_(q, node);
        // SAFETY: 持有队列锁。
        unsafe { node.as_ref().as_write_().set_counted_(true) };
        self.note_exclusive_queued_(q);
        PendingWait {
            node_: node,
            kind_: WaitKind::Upgrade,
        }
    }

    fn enqueue_upgrade_in_group_(
        &self,
        q: &mut QueuePayload<A>,
        group: NonNull<WaitNode>,
    ) -> PendingWait {
        let node = self.alloc_node_(
            q,
            WaitNode::new_write(WaitKind::Upgrade, Option::Some(group)),
        );
        self.insert_after_(q, group, node);
        // SAFETY: 持有队列锁。
        unsafe { node.as_ref().as_write_().set_counted_(true) };
        self.note_exclusive_queued_(q);
        PendingWait {
            node_: node,
            kind_: WaitKind::Upgrade,
        }
    }

    /// 尝试对刚入队的写者/升级节点直接领取。
    fn try_claim_enqueued_(&self, q: &mut QueuePayload<A>, pending: &PendingWait) -> bool {
        // SAFETY: `pending.node_` 刚由本队列创建，仍然存活。
        let node = unsafe { pending.node_.as_ref() };
        let write = node.as_write_();
        let snap = self.stat_.snapshot();

        // 组后继的升级写节点：组已只剩可升级读持锁时可直接领取。
        if write.kind_() == WaitKind::Upgrade
            && let Option::Some(group) = write.root_()
        {
            if q.head_ != Option::Some(group) {
                return false;
            }
            // SAFETY: `group` 是主队列中仍然存活的节点。
            let root = unsafe { group.as_ref() }.as_root_read_();
            if root.count_() != 1 || !root.upgr_active_() {
                return false;
            }
            if snap.reader_count == D::ONE
                && snap.upgrade_active
                && !snap.writer_active
                && self.try_claim_(WaitKind::Upgrade)
            {
                // SAFETY: 持有队列锁。
                unsafe { write.set_state_(SLOT_ACQUIRED) };
                self.set_active_group_(group);
                self.finish_write_node_(q, pending.node_);
                return true;
            }
            return false;
        }

        if q.head_ != Option::Some(pending.node_) {
            return false;
        }
        let admitted = match write.kind_() {
            WaitKind::Write => {
                snap.reader_count == D::ZERO && !snap.writer_active && !snap.upgrade_active
            }
            WaitKind::Upgrade => {
                snap.reader_count == D::ONE && snap.upgrade_active && !snap.writer_active
            }
            _ => false,
        };
        if !admitted {
            return false;
        }
        if self.try_claim_(pending.kind_) {
            // SAFETY: 持有队列锁。
            unsafe { write.set_state_(SLOT_ACQUIRED) };
            self.finish_write_node_(q, pending.node_);
            return true;
        }
        false
    }

    fn register_waker_(&self, pending: &PendingWait, waker: &Waker) {
        // SAFETY: `pending.node_` 仍存活；持有队列锁。
        let node = unsafe { pending.node_.as_ref() };
        let write = node.as_write_();
        // SAFETY: 持有队列锁。
        unsafe {
            write.set_state_(SLOT_WAITING);
            write.set_waker_(waker);
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 放行 / 唤醒
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 回收队首已经没有任何持锁者/等待者的读者组。
    fn prune_front_(&self, q: &mut QueuePayload<A>) {
        loop {
            let Option::Some(front) = q.head_ else {
                return;
            };
            // SAFETY: `front` 是主队列中仍然存活的节点。
            let node = unsafe { front.as_ref() };
            if node.is_root_read_() {
                let root = node.as_root_read_();
                // `WAKING` 是读者链拓扑的独占锁：持有者正在锁外遍历整条链。
                // 此时既不能回收该组（节点生命期由遍历者使用），也不能越过
                // 它去处理后面的节点，否则“队首正在被处理”的前提就破了。
                if root.waking_() {
                    return;
                }
                // SAFETY: 持有队列锁，且链拓扑静止。
                if root.count_() == 0 && !unsafe { root.has_pending_() } {
                    debug_assert!(!root.upgr_active_(), "空读者组仍带有可升级读持锁标记");
                    self.free_group_(q, front);
                    continue;
                }
                return;
            }
            // 写者节点由领取/取消方自己收尾，这里不能抢先释放。
            return;
        }
    }

    /// 在队列锁内选出可以放行的目标。
    fn select_head_(&self, q: &mut QueuePayload<A>) -> Option<Selection> {
        let Option::Some(front) = q.head_ else {
            self.stat_.clear_waiters_present();
            if q.writer_waiters_ != 0 {
                debug_assert!(false, "writer_waiters_ leaked while queue is empty");
                q.writer_waiters_ = 0;
                self.stat_.clear_writer_queued();
            }
            return Option::None;
        };

        let snap = self.stat_.snapshot();
        debug_assert_eq!(
            snap.writer_queued,
            q.writer_waiters_ > 0,
            "WRITER_QUEUED 与 writer_waiters_ 不一致"
        );
        if snap.writer_active {
            return Option::None;
        }

        // SAFETY: `front` 是主队列中仍然存活的节点。
        let node = unsafe { front.as_ref() };
        if node.is_root_read_() {
            let root = node.as_root_read_();
            // 已有唯一唤醒者在锁外遍历：本次 pass 直接退出，由该唤醒者在
            // 结束时重新推进队列。不要在此时再选同一个组，否则会出现两个
            // 遍历者同时清 `WAKING`。
            if root.waking_() {
                return Option::None;
            }
            // SAFETY: 持有队列锁，且链拓扑静止。
            if unsafe { root.has_pending_() } {
                // 先占住读者链拓扑，再在锁外遍历。CAS 失败只可能来自并发的
                // `clear_waking_` 之后被另一个唤醒者抢先；让后者负责推进。
                if root.try_set_waking_() {
                    return Option::Some(Selection::Group(front));
                }
                return Option::None;
            }
            if root.count_() == 1 && root.upgr_active_() {
                // SAFETY: 持有队列锁。
                let next = unsafe { front.as_ref().next_() };
                if let Option::Some(next) = next {
                    // SAFETY: `next` 是主队列中仍然存活的节点。
                    let next_node = unsafe { next.as_ref() };
                    if next_node.is_write_() {
                        let write = next_node.as_write_();
                        if write.kind_() == WaitKind::Upgrade
                            && write.root_() == Option::Some(front)
                            && matches!(write.state_(), SLOT_WAITING | SLOT_ADMITTED)
                            && snap.reader_count == D::ONE
                            && snap.upgrade_active
                        {
                            return Option::Some(Selection::Write(next));
                        }
                    }
                }
            }
            return Option::None;
        }

        let write = node.as_write_();
        if matches!(write.state_(), SLOT_ACQUIRED | SLOT_CANCELLED) {
            return Option::None;
        }
        let admitted = match write.kind_() {
            WaitKind::Write => snap.reader_count == D::ZERO && !snap.upgrade_active,
            WaitKind::Upgrade => snap.reader_count == D::ONE && snap.upgrade_active,
            _ => false,
        };
        if admitted {
            Option::Some(Selection::Write(front))
        } else {
            Option::None
        }
    }

    /// 锁外遍历整条读者链并逐个唤醒；返回本轮成功放行的读者数。
    ///
    /// 调用前 `select_head_` 已把 `WAKING` 置 1，因此本函数独占链拓扑：
    ///
    /// - 新读者不会再压入本组：`enqueue_reader_` 看到 `WAKING` 会新建组，
    ///   所以 `tail_` 与所有 `prev_` 在整个遍历期间完全静止；
    /// - `prune_front_` 看到 `WAKING` 不会回收本组，节点生命期覆盖遍历全程；
    /// - 只有本函数在结束时清除 `WAKING`；其他 `pass` 看到它只会直接返回。
    ///
    /// 返回 0 表示链上只剩 `Registering`/`Taking` 或当前不可放行的
    /// `UpgrRead`：调用方应退出，让对应节点的所有者自行触发下一次 pass，
    /// 避免在这里忙循环。
    fn wake_group_(&self, group: NonNull<WaitNode>) -> usize {
        // SAFETY: `group` 是 select_head_ 选中的存活读者组；WAKING 保证它在
        // 本函数返回前不会被回收，且链拓扑不会改变。
        let root = unsafe { group.as_ref() }.as_root_read_();
        let snap = self.stat_.snapshot();
        let mut admitted = 0usize;
        // SAFETY: 遍历期间链静止；节点由 WAKING 保护，不会释放。
        let mut cur = unsafe { root.tail_() };
        while let Option::Some(node) = cur {
            // SAFETY: 同上。
            let node_ref = unsafe { node.as_ref() };
            let prev = unsafe { node_ref.prev_() };
            let skip = node_ref.is_upgr_read_() && snap.upgrade_active;
            if !skip && let Option::Some(waker) = node_ref.as_reader_().try_admit_() {
                root.inc_count_();
                // 锁外唤醒：wake 可能同步重入执行器，但重入的 pass 看到
                // WAKING 会直接返回，不会再次进入本链。
                waker.wake();
                admitted += 1;
            }
            cur = prev;
        }
        root.clear_waking_();
        admitted
    }

    /// 锁内取走一个写者的 waker 并标 `Admitted`。
    ///
    /// 写者最多只放行一个，不涉及读者链拓扑，因此不需要 `WAKING`。
    fn take_write_waker_(node: NonNull<WaitNode>) -> Option<Waker> {
        // SAFETY: `node` 是 select_head_ 选中的存活写者节点；持有队列锁。
        let write = unsafe { node.as_ref() }.as_write_();
        if write.state_() != SLOT_WAITING {
            return Option::None;
        }
        // SAFETY: 持有队列锁。
        let waker = unsafe { write.take_waker_() };
        if waker.is_some() {
            // SAFETY: 持有队列锁。
            unsafe { write.set_state_(SLOT_ADMITTED) };
        }
        waker
    }

    /// 推进放行流程。
    ///
    /// 读者组在锁外整链遍历（`wake_group_`）；写者仍只在锁内取一个 waker。
    /// 每轮结束后重新选队首，以处理遍历期间发生的入队、取消与写者释放。
    fn run_pass_(&self) {
        if !self.stat_.waiters_present() {
            return;
        }
        loop {
            let action = {
                let mut q = self.queue_.lock();
                self.prune_front_(&mut q);
                match self.select_head_(&mut q) {
                    Option::None => WakeAction::Stop,
                    Option::Some(Selection::Group(group)) => WakeAction::Group(group),
                    Option::Some(Selection::Write(node)) => {
                        WakeAction::Write(Self::take_write_waker_(node))
                    }
                }
            };
            match action {
                WakeAction::Stop => break,
                WakeAction::Write(Option::None) => break,
                WakeAction::Write(Option::Some(waker)) => waker.wake(),
                WakeAction::Group(group) => {
                    let admitted = self.wake_group_(group);
                    if admitted == 0 {
                        // 链上只剩暂不可放行的节点；它们的所有者会自行触发
                        // 下一次 pass，这里退出以避免忙循环。
                        break;
                    }
                }
            }
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 领取收尾 / 组回收
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 释放一个已领取/已取消的写者节点，并维护计数。
    fn finish_write_node_(&self, q: &mut QueuePayload<A>, node: NonNull<WaitNode>) {
        // SAFETY: `node` 是主队列中仍然存活的写者节点。
        let write = unsafe { node.as_ref() }.as_write_();
        if write.counted_() {
            // SAFETY: 持有队列锁。
            unsafe { write.set_counted_(false) };
            debug_assert!(q.writer_waiters_ > 0, "writer_waiters_ underflow");
            q.writer_waiters_ = q.writer_waiters_.saturating_sub(1);
            if q.writer_waiters_ == 0 {
                self.stat_.clear_writer_queued();
            }
        }
        self.unlink_(q, node);
        self.free_node_(q, node);
    }

    /// 批量回收整个空读者组：读者链节点与根节点。
    fn free_group_(&self, q: &mut QueuePayload<A>, group: NonNull<WaitNode>) {
        // SAFETY: `group` 是主队列中仍然存活的读者组。
        let root = unsafe { group.as_ref() }.as_root_read_();
        debug_assert_eq!(root.count_(), 0, "回收仍有持锁者的读者组");

        self.unlink_(q, group);

        // SAFETY: 持有队列锁；读者链尾指针与 prev_ 链稳定。
        let mut cur = unsafe { root.tail_() };
        while let Option::Some(node) = cur {
            // SAFETY: 链节点在组存活期间一直有效。
            let prev = unsafe { node.as_ref().prev_() };
            self.free_node_(q, node);
            cur = prev;
        }

        self.clear_active_group_(group);
        self.free_node_(q, group);

        if q.head_.is_none() {
            self.stat_.clear_waiters_present();
            debug_assert_eq!(q.writer_waiters_, 0, "队列已空但写者计数非零");
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 主队列链接
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn push_back_(&self, q: &mut QueuePayload<A>, node: NonNull<WaitNode>) {
        let old_tail = q.tail_;
        // SAFETY: `node` 是刚分配、尚未入链的节点；持有队列锁。
        unsafe {
            node.as_ref().set_next_(Option::None);
            node.as_ref().set_prev_(old_tail);
        }
        match old_tail {
            // SAFETY: 旧 tail 是主队列中仍然存活的节点。
            Some(tail) => unsafe { tail.as_ref().set_next_(Option::Some(node)) },
            None => q.head_ = Option::Some(node),
        }
        q.tail_ = Option::Some(node);
    }

    fn push_front_(&self, q: &mut QueuePayload<A>, node: NonNull<WaitNode>) {
        let old_head = q.head_;
        // SAFETY: `node` 是刚分配、尚未入链的节点；持有队列锁。
        unsafe {
            node.as_ref().set_prev_(Option::None);
            node.as_ref().set_next_(old_head);
        }
        match old_head {
            // SAFETY: 旧 head 是主队列中仍然存活的节点。
            Some(head) => unsafe { head.as_ref().set_prev_(Option::Some(node)) },
            None => q.tail_ = Option::Some(node),
        }
        q.head_ = Option::Some(node);
    }

    fn insert_after_(
        &self,
        q: &mut QueuePayload<A>,
        at: NonNull<WaitNode>,
        node: NonNull<WaitNode>,
    ) {
        // SAFETY: `at` 是主队列中仍然存活的节点；`node` 尚未入链。
        let next = unsafe { at.as_ref().next_() };
        unsafe {
            node.as_ref().set_prev_(Option::Some(at));
            node.as_ref().set_next_(next);
            at.as_ref().set_next_(Option::Some(node));
        }
        match next {
            // SAFETY: `next` 是主队列中仍然存活的节点。
            Some(next) => unsafe { next.as_ref().set_prev_(Option::Some(node)) },
            None => q.tail_ = Option::Some(node),
        }
    }

    /// 从主队列摘除一个确定仍然在链上的节点。
    fn unlink_(&self, q: &mut QueuePayload<A>, node: NonNull<WaitNode>) {
        // SAFETY: `node` 是主队列中仍然存活的节点；持有队列锁。
        let prev = unsafe { node.as_ref().prev_() };
        let next = unsafe { node.as_ref().next_() };
        match prev {
            // SAFETY: `prev` 是主队列中仍然存活的节点。
            Some(prev) => unsafe { prev.as_ref().set_next_(next) },
            None => q.head_ = next,
        }
        match next {
            // SAFETY: `next` 是主队列中仍然存活的节点。
            Some(next) => unsafe { next.as_ref().set_prev_(prev) },
            None => q.tail_ = prev,
        }
        // SAFETY: 持有队列锁；节点已脱链。
        unsafe {
            node.as_ref().set_prev_(Option::None);
            node.as_ref().set_next_(Option::None);
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Arena 分配 / 活跃组指针
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn alloc_node_(&self, q: &mut QueuePayload<A>, node: WaitNode) -> NonNull<WaitNode> {
        match q.arena_.alloc(node) {
            Result::Ok(node) => node,
            Result::Err(_) => handle_alloc_error(Arena::<WaitNode>::chunk_layout()),
        }
    }

    fn free_node_(&self, q: &mut QueuePayload<A>, node: NonNull<WaitNode>) {
        // SAFETY: `node` 已经脱链，且没有其他存活引用。
        drop(unsafe { q.arena_.free(node) });
    }

    fn set_active_group_(&self, group: NonNull<WaitNode>) {
        match self.active_read_group_.try_spin_init(group) {
            Result::Ok(_) => {}
            Result::Err(existing) => {
                debug_assert_eq!(existing, group, "同时出现两个活跃读者组");
            }
        }
    }

    fn clear_active_group_(&self, group: NonNull<WaitNode>) {
        let _ = self.active_read_group_.try_spin_compare_and_reset(group);
    }

    fn active_group_(&self) -> Option<NonNull<WaitNode>> {
        self.active_read_group_.load()
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 计数
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn note_exclusive_queued_(&self, q: &mut QueuePayload<A>) {
        q.writer_waiters_ += 1;
        if q.writer_waiters_ == 1 {
            self.stat_.mark_writer_queued();
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 释放 / 降级
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn release_reader(&self, tracked: bool) {
        let _ok = self.stat_.release_reader();
        debug_assert!(_ok, "state transition `release_reader` failed");
        if tracked {
            self.release_group_reader_(false);
        } else {
            self.run_pass_();
        }
    }

    pub(super) fn release_upgradable_read(&self, tracked: bool) {
        if !self.stat_.release_upgradable_read() {
            // 该守卫可能已经被并行的“写者降级为可升级读”转换成了普通读
            // （`UPGRADE_ACTIVE` 已清除）。此时按普通读归还，避免读者计数泄漏。
            let fallback = self.stat_.release_reader();
            debug_assert!(fallback, "可升级读释放失败且无法回退为普通读");
        }
        if tracked {
            self.release_group_reader_(true);
        } else {
            self.run_pass_();
        }
    }

    fn release_group_reader_(&self, is_upgradable: bool) {
        let Option::Some(group) = self.active_group_() else {
            debug_assert!(false, "tracked 读者释放时缺少活跃读者组");
            self.run_pass_();
            return;
        };
        // SAFETY: 活跃组在 count_ > 0 期间不会回收；本 guard 占着一个计数。
        let root = unsafe { group.as_ref() }.as_root_read_();
        if is_upgradable {
            root.clear_upgr_active_();
        }
        let old = root.dec_count_();
        debug_assert!(old >= 1, "读者组 count_ 下溢");
        if old == 1 {
            // 最后一个计数归零：触发一次 pass，由队首 prune 回收空组。
            // 这里不再使用 `group` 指针，避免与并发 prune 竞争。
            self.run_pass_();
        }
    }

    pub(super) fn release_writer(&self) {
        let _ok = self.stat_.release_writer();
        debug_assert!(_ok, "state transition `release_writer` failed");
        self.run_pass_();
    }

    pub(super) fn downgrade_write_to_read(&self) {
        let _ok = self.stat_.downgrade_write_to_read();
        debug_assert!(_ok, "state transition `downgrade_write_to_read` failed");
        self.run_pass_();
    }

    pub(super) fn downgrade_write_to_upgradable(&self) {
        let _ok = self.stat_.downgrade_write_to_upgradable();
        debug_assert!(
            _ok,
            "state transition `downgrade_write_to_upgradable` failed"
        );
        self.run_pass_();
    }

    pub(super) fn downgrade_upgradable_to_read(&self, tracked: bool) {
        let _ok = self.stat_.downgrade_upgradable_to_read();
        debug_assert!(
            _ok,
            "state transition `downgrade_upgradable_to_read` failed"
        );
        if tracked && let Option::Some(group) = self.active_group_() {
            // SAFETY: 活跃组在 guard 持有期间不会回收。
            unsafe { group.as_ref() }
                .as_root_read_()
                .clear_upgr_active_();
        }
        self.run_pass_();
    }
}

#[cfg(test)]
mod tests_ {
    use core::{sync::atomic::AtomicUsize, task::Waker};

    use super::{AcqProgress, RwCoreArena, WaitKind};

    /// 验证 `WAKING` 是读者链拓扑的独占锁：置位期间新读者不得加入该链，
    /// 空组也不得被 `prune_front_` 回收；清除后队列恢复推进。
    ///
    /// Verifies the `WAKING` bit is an exclusive lock over reader-chain
    /// topology: while set, new readers must not join the chain and an empty
    /// group must not be pruned; clearing it resumes queue progress.
    ///
    /// - 手段：写者持有期间排队读者 A，置位其组的 `WAKING`；再排队读者 B，
    ///   断言 B 落在新组；取消 A 使原组 `count == 0` 且无 pending，此时跑
    ///   pass 断言原组未被回收；清除 `WAKING` 后再跑 pass，原组被回收。
    /// - 判断：B 的组指针与 A 不同；置位期间 head 仍是 A 的组且节点池非空；
    ///   清除后 head 变为 B 的组，释放写者后 B 能正常领取。
    #[test]
    fn waking_group_is_exclusive_for_enqueue_and_prune() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let AcqProgress::Waiting(pending_a) = core.acquire(WaitKind::Read, waker) else {
            panic!("写者持有时读者 A 不应直接领取");
        };
        // SAFETY: A 的节点仍在队列中，组根有效。
        let group_a = unsafe { pending_a.node_.as_ref() }.as_reader_().root_();
        // SAFETY: 组 A 仍存活。
        assert!(
            unsafe { group_a.as_ref() }
                .as_root_read_()
                .try_set_waking_(),
            "首次置位 WAKING 应成功"
        );

        // WAKING 期间新读者必须新建组，不能压入 A 的链。
        let AcqProgress::Waiting(pending_b) = core.acquire(WaitKind::Read, waker) else {
            panic!("写者持有时读者 B 不应直接领取");
        };
        // SAFETY: B 的节点仍在队列中。
        let group_b = unsafe { pending_b.node_.as_ref() }.as_reader_().root_();
        assert_ne!(group_b, group_a, "WAKING 期间新读者应加入新组");

        // 取消 A：原组变为 count=0 且无 pending，但 WAKING 阻止回收。
        core.cancel_wait(&pending_a);
        assert_eq!(
            core.queue_.lock().head_,
            Some(group_a),
            "WAKING 期间原组不得被 prune 回收"
        );
        assert!(!core.pool_is_empty(), "WAKING 期间节点不得被归还");

        // 清除 WAKING 后，原组应被回收，head 推进到 B 的组。
        // SAFETY: 组 A 仍存活（WAKING 刚由本测试清除）。
        unsafe { group_a.as_ref() }.as_root_read_().clear_waking_();
        core.run_pass_();
        assert_eq!(
            core.queue_.lock().head_,
            Some(group_b),
            "清除 WAKING 后空组应被回收"
        );

        core.release_writer();
        assert!(
            core.poll_acquire(&pending_b, waker),
            "释放写者后 B 应能领取"
        );
        core.release_reader(true);
        assert!(!core.waiters_present(), "最终队列应为空");
        assert!(core.pool_is_empty(), "最终 Arena 应为空");
    }

    /// 验证写者持有期间读者的入队、放行与领取流程。
    ///
    /// Verifies a queued reader can be admitted and claimed while the writer holds.
    ///
    /// - 手段：先取写者，再以 Read 身份排队；释放写者后 poll 领取。
    /// - 判断：释放前返回 Waiting；释放后能领取，队列与 Arena 最终清空。
    #[test]
    fn queued_reader_can_be_admitted_and_claimed() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let AcqProgress::Waiting(pending) = core.acquire(WaitKind::Read, waker) else {
            panic!("写者持有时读者不应直接领取");
        };
        assert!(core.waiters_present(), "读者应已进入队列");

        core.release_writer();
        assert!(core.poll_acquire(&pending, waker), "释放写者后读者应能领取");
        core.release_reader(true);
        assert!(!core.waiters_present(), "领取并回收后队列应为空");
        assert!(core.pool_is_empty(), "读者组应被批量归还 Arena");
    }

    /// 验证取消写者等待会收回 `WRITER_QUEUED` 并回收元素。
    ///
    /// Verifies cancelling a queued writer clears the barrier and frees its node.
    ///
    /// - 手段：写者持有期间排队第二个写者，然后取消它。
    /// - 判断：取消后 `writer_queued` 为假，队列为空；释放写者后读者可走快速路径。
    #[test]
    fn cancelled_writer_releases_barrier_and_elements() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let AcqProgress::Waiting(pending) = core.acquire(WaitKind::Write, waker) else {
            panic!("写者持有时第二个写请求不应直接领取");
        };
        assert!(core.writer_queued(), "写者入队后应置位 WRITER_QUEUED");

        core.cancel_wait(&pending);
        assert!(!core.writer_queued(), "取消后应收回 WRITER_QUEUED");
        assert!(!core.waiters_present(), "取消后队列应为空");
        assert!(core.pool_is_empty(), "取消的写者节点应归还 Arena");

        core.release_writer();
        assert!(core.try_read_fast(), "取消的写者不应再挡住读者");
        core.release_reader(false);
    }

    /// 验证已放行但尚未领取的读者取消时直接递减持锁计数。
    ///
    /// Verifies cancelling an admitted-but-unclaimed reader decrements the hold
    /// count directly and marks the node cancelled.
    ///
    /// - 手段：写者持有期间排队两个读者；释放写者使两者都变成 `Admitted`
    ///   （组计数为 2），取消其中一个，再领取并释放另一个。
    /// - 判断：取消后组计数降到 1；另一个读者领取、释放后计数归零，
    ///   整组被批量回收，Arena 为空。
    #[test]
    fn admitted_reader_cancel_decrements_count() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let AcqProgress::Waiting(first) = core.acquire(WaitKind::Read, waker) else {
            panic!("第一个读者应排队");
        };
        let AcqProgress::Waiting(second) = core.acquire(WaitKind::Read, waker) else {
            panic!("第二个读者应排队");
        };

        core.release_writer();
        // 两者都已被放行；取消第二个前，组计数应为 2。
        core.cancel_wait(&second);
        assert!(core.poll_acquire(&first, waker), "第一个读者应能领取");
        core.release_reader(true);
        assert!(!core.waiters_present(), "全部结束后队列应为空");
        assert!(core.pool_is_empty(), "整组应被批量归还 Arena");
    }

    /// 验证 1100 个读者跨 Arena chunk 时仍能整链批量回收。
    ///
    /// Verifies a 1100-reader chain spanning Arena chunks is reclaimed in bulk.
    ///
    /// - 手段：写者持有期间排队 1100 个普通读（超过一个 chunk 的 1024 槽），
    ///   释放写者后逐个领取并释放。
    /// - 判断：全部释放后队列与 Arena 都为空。
    #[test]
    fn large_reader_chain_is_batch_reclaimed() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let total = 1100;
        let mut pending = alloc::vec::Vec::with_capacity(total);
        for _ in 0..total {
            let AcqProgress::Waiting(p) = core.acquire(WaitKind::Read, waker) else {
                panic!("写者持有时读者不应直接领取");
            };
            pending.push(p);
        }

        core.release_writer();
        for p in &pending {
            assert!(core.poll_acquire(p, waker), "读者应能领取");
        }
        for _ in 0..total {
            core.release_reader(true);
        }
        assert!(!core.waiters_present(), "全部释放后队列应为空");
        assert!(core.pool_is_empty(), "整条大读者链应被批量归还 Arena");
    }

    /// 验证读者链中间节点取消后，整链仍能回收。
    ///
    /// Verifies a cancelled middle reader node is reclaimed with the chain.
    ///
    /// - 手段：写者持有期间排队 3 个读者，取消中间一个；释放写者后领取并
    ///   释放其余两个。
    /// - 判断：取消节点不阻塞放行，最终队列与 Arena 都为空。
    #[test]
    fn cancelled_middle_reader_is_reclaimed_with_group() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let AcqProgress::Waiting(first) = core.acquire(WaitKind::Read, waker) else {
            panic!("第一个读者应排队");
        };
        let AcqProgress::Waiting(middle) = core.acquire(WaitKind::Read, waker) else {
            panic!("第二个读者应排队");
        };
        let AcqProgress::Waiting(last) = core.acquire(WaitKind::Read, waker) else {
            panic!("第三个读者应排队");
        };

        core.cancel_wait(&middle);
        core.release_writer();
        assert!(core.poll_acquire(&first, waker), "第一个读者应能领取");
        assert!(core.poll_acquire(&last, waker), "最后一个读者应能领取");
        core.release_reader(true);
        core.release_reader(true);
        assert!(!core.waiters_present(), "全部释放后队列应为空");
        assert!(core.pool_is_empty(), "取消节点应随整组批量回收");
    }

    /// 验证可升级读先于普通读释放时不会误留升级写节点。
    ///
    /// Verifies releasing the upgradable reader before a plain reader does not
    /// leave a spurious upgrade write node.
    ///
    /// - 手段：写者持有期间排队普通读 + 可升级读；释放写者并领取两者，
    ///   先释放可升级读，再释放普通读。
    /// - 判断：可升级读释放后写者仍不能进入（普通读还持有），但
    ///   `writer_queued` 为假；普通读释放后队列为空、Arena 为空。
    #[test]
    fn upgradable_released_first_does_not_insert_writer() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let AcqProgress::Waiting(reader) = core.acquire(WaitKind::Read, waker) else {
            panic!("普通读应进入队列");
        };
        let AcqProgress::Waiting(upgr) = core.acquire(WaitKind::UpgradableRead, waker) else {
            panic!("可升级读应进入队列");
        };
        core.release_writer();
        assert!(core.poll_acquire(&reader, waker), "普通读应能领取");
        assert!(core.poll_acquire(&upgr, waker), "可升级读应能领取");

        core.release_upgradable_read(true);
        assert!(!core.writer_queued(), "不应为已释放的可升级读插入写者");
        assert!(!core.try_write_fast(), "普通读仍持有写者不应进入");

        core.release_reader(true);
        assert!(!core.waiters_present(), "全部释放后队列应为空");
        assert!(core.pool_is_empty(), "整条读者链应被批量归还 Arena");
        assert!(core.try_write_fast(), "读者组结束后写者应能快速进入");
        core.release_writer();
    }

    /// 验证升级请求只在真正发起时插入写节点，并在组计数为 1 时被放行。
    ///
    /// Verifies the upgrade write node is inserted only when the upgrade is
    /// actually requested, and is admitted when the group count is one.
    ///
    /// - 手段：写者持有期间排队普通读 + 可升级读；释放写者并领取两者，
    ///   释放普通读使组计数变成 1；确认此时没有写者等待，再发起升级。
    /// - 判断：升级请求入队后 `writer_queued` 为真，能领取写许可；
    ///   写者释放、可升级读释放后队列与 Arena 清空。
    #[test]
    fn upgrade_request_is_inserted_and_admitted() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let AcqProgress::Waiting(reader) = core.acquire(WaitKind::Read, waker) else {
            panic!("普通读应进入队列");
        };
        let AcqProgress::Waiting(upgr) = core.acquire(WaitKind::UpgradableRead, waker) else {
            panic!("可升级读应进入队列");
        };
        core.release_writer();
        assert!(core.poll_acquire(&reader, waker), "普通读应能领取");
        assert!(core.poll_acquire(&upgr, waker), "可升级读应能领取");

        core.release_reader(true);
        assert!(!core.writer_queued(), "尚未发起升级时不应有写者等待");

        let upgrade = match core.acquire_upgrade(waker, true) {
            AcqProgress::Acquired => Option::None,
            AcqProgress::Waiting(pending) => Option::Some(pending),
        };
        if let Option::Some(upgrade) = &upgrade {
            assert!(core.writer_queued(), "升级请求入队后应置位 WRITER_QUEUED");
            assert!(core.poll_acquire(upgrade, waker), "升级请求应能领取写许可");
        }
        assert!(!core.try_write_fast(), "升级成功后写者应处于独占状态");

        core.release_writer();
        core.release_upgradable_read(true);
        assert!(!core.waiters_present(), "全部释放后队列应为空");
        assert!(core.pool_is_empty(), "整条读者链应被批量归还 Arena");
    }

    /// 验证可升级读释放失败时能回退为普通读，不泄漏读者计数。
    ///
    /// Verifies releasing an upgradable read falls back to a plain reader when
    /// the upgrade flag was already cleared by a parallel downgrade.
    ///
    /// - 手段：快速获取可升级读并升级为写者；把写者降级为可升级读、再降级为
    ///   普通读；释放额外读后，再释放最初的可升级读守卫。
    /// - 判断：`UPGRADE_ACTIVE` 已被降级路径清除，最初守卫释放时必须按普通读
    ///   回退，最终 `reader_count` 归零且队列为空。
    #[test]
    fn release_upgradable_read_falls_back_to_reader() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_upgradable_read_fast(), "可升级读快速路径应成功");
        assert!(core.try_upgrade_now(), "独占时应能立即升级");

        core.downgrade_write_to_upgradable();
        core.downgrade_upgradable_to_read(false);
        core.release_reader(false);
        assert_eq!(core.reader_count(), 1);

        // `UPGRADE_ACTIVE` 已被额外降级清除，这里走普通读回退。
        core.release_upgradable_read(false);
        assert_eq!(core.reader_count(), 0, "可升级读释放不应泄漏计数");
        assert!(!core.waiters_present());
    }
}
