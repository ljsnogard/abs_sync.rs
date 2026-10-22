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
//! - 队列锁只保护主队列拓扑、读者组封口标记与升级预留节点；
//! - 读者领取通过状态字 CAS 完成，成功后在读者链节点上原子累加
//!   `RootRead.count_`，不取队列锁；
//! - 读者释放只做 `count_.fetch_sub`：减到 1 且组内有已领取的 `UpgrRead`
//!   时预分配一个休眠升级写节点；减到 0 时由最后一个读者取一次队列锁，
//!   把整条读者链批量归还 Arena；
//! - 主队列中的活跃 `RootRead` 一直保留到 `count_ == 0`，因此升级节点可以
//!   直接排在它后面，作为下一个待激活写者。

use alloc::alloc::{Global, handle_alloc_error};
use core::{
    borrow::BorrowMut,
    ptr::NonNull,
    sync::atomic::{AtomicPtr, Ordering},
    task::Waker,
};

use funty::Unsigned;

use atomex::{StrictOrderings, TrAtomicData, TrCmpxchOrderings, x_deps::funty};

use crate::rwlock::{
    state_::CoopRwState,
    wait_::{SpinLock, WaitKind},
};

use super::{
    arena_::Arena,
    wait_node_::{SlotState, WaitNode},
};

/// 等待中 future 持有的句柄：一个 `WaitNode` 元素。
#[derive(Clone, Copy)]
pub(super) struct PendingWait {
    node_: NonNull<WaitNode>,
    kind_: WaitKind,
}

// SAFETY: 句柄只保存一个 Arena 元素的地址。元素由 Arena 持有，地址在存活期间
// 稳定；所有解引用都发生在队列/节点锁内，移动句柄本身不会产生别名访问。
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
struct QueuePayload {
    /// 主队列队首。
    head_: Option<NonNull<WaitNode>>,
    /// 主队列队尾。
    tail_: Option<NonNull<WaitNode>>,
    /// 队列中仍处于待决状态、且已计入计数的 `Write` / `Upgrade` 请求数。
    writer_waiters_: usize,
    /// 有线程发现读者组正在被别的唤醒者处理，需要唤醒者在清标记后重跑。
    retry_: bool,
    /// 等待节点池。
    arena_: Arena<WaitNode>,
}

// SAFETY: `head_` / `tail_` 只指向同一个 `arena_` 拥有的元素。移动
// `QueuePayload` 时 `Arena` 与链接一起移动，元素地址不变；所有链接读写都在
// 队列自旋锁内完成。Arena 自身在 `WaitNode: Send` 时满足 `Send`。
unsafe impl Send for QueuePayload {}

/// 一次选中的放行目标。
enum Selection {
    /// 唤醒一个读者组。
    Group(NonNull<WaitNode>),
    /// 唤醒一个独占等待者。
    Write(NonNull<WaitNode>),
}

/// Arena 版读写锁的共享内核。
pub(super) struct RwCoreArena<D, B, O = StrictOrderings>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    stat_: CoopRwState<D, B, O>,
    /// 当前唯一活跃读者组的根；快速路径读者不参与。
    active_read_group_: AtomicPtr<WaitNode>,
    queue_: SpinLock<QueuePayload>,
}

impl<D, B, O> RwCoreArena<D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) const fn new(cell: B) -> Self {
        RwCoreArena {
            stat_: CoopRwState::new(cell),
            active_read_group_: AtomicPtr::new(core::ptr::null_mut()),
            queue_: SpinLock::new(QueuePayload {
                head_: Option::None,
                tail_: Option::None,
                writer_waiters_: 0,
                retry_: false,
                arena_: Arena::new(Global),
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

    /// 活跃组是否持有一个休眠的升级占位节点（仅供单元测试观测）。
    #[cfg(test)]
    pub(super) fn has_reserved_upgrade(&self) -> bool {
        let Option::Some(group) = self.active_group_() else {
            return false;
        };
        let _q = self.queue_.lock();
        // SAFETY: 持有队列锁；活跃组不会在 `count_ > 0` 期间回收。
        let state = unsafe { group.as_ref().as_root_read_().state_() };
        state.upgrade_.is_some_and(|node| {
            // SAFETY: `node` 是本组的升级预留节点，仍然存活。
            unsafe { node.as_ref() }.as_write_().state_() == SlotState::Reserved
        })
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
        if tracked {
            let group = self.active_group_();
            debug_assert!(group.is_some(), "tracked 升级请求缺少活跃读者组");
            // 组存在时把升级节点排在组后继；组缺失时退回队首路径。
            if let Option::Some(group) = group {
                return self.acquire_upgrade_in_group_(group, waker);
            }
        }
        self.acquire_inner_(WaitKind::Upgrade, waker)
    }

    fn acquire_upgrade_in_group_(&self, group: NonNull<WaitNode>, waker: &Waker) -> AcqProgress {
        let (claimed, pending) = {
            let mut q = self.queue_.lock();
            self.prune_front_(&mut q);
            let pending = self.enqueue_upgrade_in_group_(&mut q, group);
            if !self.stat_.waiters_present() {
                self.stat_.mark_waiters_present();
            }
            let claimed = self.try_claim_enqueued_(&mut q, &pending);
            if !claimed {
                self.register_waker_(&mut q, &pending, waker);
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
            let claimed = self.try_claim_enqueued_(&mut q, &pending);
            if !claimed {
                self.register_waker_(&mut q, &pending, waker);
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

    /// 已被唤醒的等待者重试领取。
    pub(super) fn poll_acquire(&self, pending: &PendingWait, waker: &Waker) -> bool {
        // SAFETY: 句柄对应的节点仍处于待决状态，元素一定还没有被回收。
        let node = unsafe { pending.node_.as_ref() };
        let state = self.slot_state_(node);

        if state == SlotState::Admitted {
            if self.try_claim_(pending.kind_) {
                if node.is_reader_() {
                    self.set_active_group_(node.as_reader_().root_());
                }
                if self.claim_node_(node, pending.kind_).is_some() {
                    self.finish_claimed_(pending.kind_, pending.node_);
                    return true;
                }
                debug_assert!(false, "状态字领取成功但节点状态已不是待决");
            }
        } else if state == SlotState::Reserved {
            // 升级预留节点被升级 future 接管。
            let mut q = self.queue_.lock();
            let write = node.as_write_();
            let was_reserved = write.adopt_(waker);
            if was_reserved {
                self.note_exclusive_queued_(&mut q);
                write.set_counted_(true);
            }
            drop(q);
        } else if state != SlotState::Waiting {
            debug_assert!(false, "poll_acquire: 在 Acquired/Cancelled 状态上再次 poll");
            return false;
        }

        {
            let mut q = self.queue_.lock();
            self.register_waker_(&mut q, pending, waker);
        }
        self.run_pass_();
        false
    }

    /// 等待者被取消（future 被丢弃）。
    pub(super) fn cancel_wait(&self, pending: &PendingWait) {
        // SAFETY: 句柄对应的节点仍处于待决状态，元素一定还没有被回收。
        let node = unsafe { pending.node_.as_ref() };
        if node.is_reader_() {
            let old = node.as_reader_().cancel_();
            debug_assert!(old != SlotState::Acquired, "取消已经领取的读者");
            // 读者组只在队首由 `prune_front_` 批量回收，避免释放路径与
            // 取消路径并发地对同一个组调用回收。
            self.run_pass_();
            return;
        }

        let write = node.as_write_();
        let old = write.cancel_();
        debug_assert!(old != SlotState::Acquired, "取消已经领取的写者");
        let mut q = self.queue_.lock();
        self.finish_write_node_(&mut q, pending.node_);
        drop(q);
        self.run_pass_();
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 入队
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn enqueue_(&self, q: &mut QueuePayload, kind: WaitKind) -> PendingWait {
        if kind.is_readish() {
            return self.enqueue_reader_(q, kind);
        }
        if kind == WaitKind::Upgrade {
            return self.enqueue_upgrade_front_(q);
        }
        self.enqueue_write_(q)
    }

    fn enqueue_reader_(&self, q: &mut QueuePayload, kind: WaitKind) -> PendingWait {
        if let Option::Some(tail) = q.tail_ {
            // SAFETY: `tail` 是本队列中仍然存活的节点。
            let tail_node = unsafe { tail.as_ref() };
            if tail_node.is_root_read_() {
                let root = tail_node.as_root_read_();
                // SAFETY: 持有队列锁。
                let state = unsafe { root.state_() };
                // SAFETY: 持有队列锁；链节点在组存活期间有效。
                let has_upgr = unsafe { state.has_upgr_read_() };
                let can_join = !state.sealed_ && (kind == WaitKind::Read || !has_upgr);
                if can_join {
                    let reader = self.alloc_node_(q, WaitNode::new_reader(kind, tail));
                    // SAFETY: 持有队列锁，且 `reader` 刚分配。
                    unsafe { state.push_reader_(reader) };
                    if kind == WaitKind::UpgradableRead {
                        state.sealed_ = true;
                    }
                    return PendingWait {
                        node_: reader,
                        kind_: kind,
                    };
                }
            }
        }

        let group = self.alloc_node_(q, WaitNode::new_root_read());
        self.push_back_(q, group);
        // SAFETY: 持有队列锁；`group` 刚插入主队列。
        let state = unsafe { group.as_ref().as_root_read_().state_() };
        let reader = self.alloc_node_(q, WaitNode::new_reader(kind, group));
        // SAFETY: 持有队列锁。
        unsafe { state.push_reader_(reader) };
        if kind == WaitKind::UpgradableRead {
            state.sealed_ = true;
        }
        PendingWait {
            node_: reader,
            kind_: kind,
        }
    }

    fn enqueue_write_(&self, q: &mut QueuePayload) -> PendingWait {
        if let Option::Some(tail) = q.tail_ {
            // SAFETY: `tail` 是本队列中仍然存活的节点。
            let tail_node = unsafe { tail.as_ref() };
            if tail_node.is_root_read_() {
                // SAFETY: 持有队列锁。
                unsafe { tail_node.as_root_read_().state_() }.sealed_ = true;
            }
        }
        let node = self.alloc_node_(q, WaitNode::new_write(WaitKind::Write, Option::None));
        self.push_back_(q, node);
        self.note_exclusive_queued_(q);
        // SAFETY: 持有队列锁。
        unsafe { node.as_ref().as_write_() }.set_counted_(true);
        PendingWait {
            node_: node,
            kind_: WaitKind::Write,
        }
    }

    fn enqueue_upgrade_front_(&self, q: &mut QueuePayload) -> PendingWait {
        let node = self.alloc_node_(q, WaitNode::new_write(WaitKind::Upgrade, Option::None));
        self.push_front_(q, node);
        self.note_exclusive_queued_(q);
        // SAFETY: 持有队列锁。
        unsafe { node.as_ref().as_write_() }.set_counted_(true);
        PendingWait {
            node_: node,
            kind_: WaitKind::Upgrade,
        }
    }

    fn enqueue_upgrade_in_group_(
        &self,
        q: &mut QueuePayload,
        group: NonNull<WaitNode>,
    ) -> PendingWait {
        // SAFETY: `group` 由活跃读者组指针给出，在本 guard 存活期间不会回收。
        let state = unsafe { group.as_ref().as_root_read_().state_() };
        if let Option::Some(existing) = state.upgrade_ {
            // SAFETY: `existing` 是本组预留的升级节点。
            let write = unsafe { existing.as_ref() }.as_write_();
            let old = write.state_();
            if matches!(
                old,
                SlotState::Reserved | SlotState::Waiting | SlotState::Admitted
            ) {
                return PendingWait {
                    node_: existing,
                    kind_: WaitKind::Upgrade,
                };
            }
            // 已经取消/领取的旧节点：摘除并释放，重新分配。
            state.upgrade_ = Option::None;
            self.unlink_linked_(q, existing);
            self.free_node_(q, existing);
        }

        let node = self.alloc_node_(
            q,
            WaitNode::new_write(WaitKind::Upgrade, Option::Some(group)),
        );
        self.insert_after_(q, group, node);
        state.upgrade_ = Option::Some(node);
        self.note_exclusive_queued_(q);
        // SAFETY: 持有队列锁。
        unsafe { node.as_ref().as_write_() }.set_counted_(true);
        PendingWait {
            node_: node,
            kind_: WaitKind::Upgrade,
        }
    }

    /// 尝试对刚入队的节点直接领取。
    fn try_claim_enqueued_(&self, q: &mut QueuePayload, pending: &PendingWait) -> bool {
        // SAFETY: `pending.node_` 刚由本队列创建，仍然存活。
        let node = unsafe { pending.node_.as_ref() };

        if node.is_reader_() {
            let group = node.as_reader_().root_();
            if q.head_ != Option::Some(group) {
                return false;
            }
            // SAFETY: 持有队列锁。
            let state = unsafe { group.as_ref().as_root_read_().state_() };
            if state.waking_ {
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
                self.set_active_group_(group);
                let claimed = node
                    .as_reader_()
                    .claim_(pending.kind_ == WaitKind::UpgradableRead);
                debug_assert!(claimed.is_some(), "直接领取时读者节点状态异常");
                return claimed.is_some();
            }
            return false;
        }

        let write = node.as_write_();
        if q.head_ != Option::Some(pending.node_) {
            return false;
        }
        let snap = self.stat_.snapshot();
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
            let claimed = write.claim_();
            debug_assert!(claimed.is_some(), "直接领取时写者节点状态异常");
            if claimed.is_some() {
                if pending.kind_ == WaitKind::Upgrade
                    && let Option::Some(group) = write.root_()
                {
                    self.set_active_group_(group);
                }
                self.finish_write_node_(q, pending.node_);
                return true;
            }
        }
        false
    }

    fn register_waker_(&self, q: &mut QueuePayload, pending: &PendingWait, waker: &Waker) {
        // SAFETY: `pending.node_` 仍存活。
        let node = unsafe { pending.node_.as_ref() };
        if node.is_reader_() {
            node.as_reader_().set_waker_(waker);
            return;
        }
        let write = node.as_write_();
        let old = write.state_();
        if old == SlotState::Reserved {
            let was_reserved = write.adopt_(waker);
            if was_reserved {
                self.note_exclusive_queued_(q);
                write.set_counted_(true);
            }
            return;
        }
        write.set_waker_(waker);
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 放行 / 唤醒
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 剪掉队首已经没有任何待决节点的读者组/写者。
    fn prune_front_(&self, q: &mut QueuePayload) {
        loop {
            let Option::Some(front) = q.head_ else {
                return;
            };
            // SAFETY: `front` 是主队列中仍然存活的节点。
            let node = unsafe { front.as_ref() };
            if node.is_root_read_() {
                let root = node.as_root_read_();
                // SAFETY: 持有队列锁。
                let state = unsafe { root.state_() };
                if root.count_() != 0
                    || root.pending_() != 0
                    || root.release_ops_() != 0
                    || state.waking_
                {
                    return;
                }
                // SAFETY: 持有队列锁；链节点在组存活期间有效。
                if unsafe { state.has_waiting_() } {
                    return;
                }
                self.free_group_(q, front);
                continue;
            }
            // 写者节点：已领取/已取消的由领取者/取消者自己收尾。这里不能
            // 抢先释放，否则会与“状态已改、尚未取队列锁收尾”的线程重复释放。
            return;
        }
    }

    /// 在队列锁内选出可以放行的目标。
    fn select_head_(&self, q: &mut QueuePayload) -> Option<Selection> {
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
            // SAFETY: 持有队列锁。
            let state = unsafe { root.state_() };
            if state.waking_ {
                q.retry_ = true;
                return Option::None;
            }
            // SAFETY: 持有队列锁；链节点在组存活期间有效。
            if unsafe { state.has_waiting_() } {
                state.sealed_ = true;
                state.waking_ = true;
                self.set_active_group_(front);
                return Option::Some(Selection::Group(front));
            }
            // 活跃读者组没有待放行读者：只有 count==1 且组内有已领取的
            // UpgrRead 时，才允许把后继的升级写节点作为下一个待激活写者。
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
                            && matches!(write.state_(), SlotState::Waiting | SlotState::Admitted)
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
        if matches!(write.state_(), SlotState::Acquired | SlotState::Cancelled) {
            return Option::None;
        }
        if write.state_() == SlotState::Reserved {
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

    /// 锁外唤醒一个选中目标；返回是否真的放行了槽位。
    fn wake_selection_(&self, selected: Selection) -> bool {
        match selected {
            Selection::Group(group) => self.wake_group_(group),
            Selection::Write(node) => {
                // SAFETY: `node` 是 select_head_ 选中的存活节点。
                let write = unsafe { node.as_ref() }.as_write_();
                match write.admit_() {
                    Option::Some(waker) => {
                        waker.wake();
                        true
                    }
                    Option::None => false,
                }
            }
        }
    }

    fn wake_group_(&self, group: NonNull<WaitNode>) -> bool {
        // SAFETY: `group` 是 select_head_ 选中的存活读者组。
        let root = unsafe { group.as_ref() }.as_root_read_();
        // SAFETY: 读者链已封口，`head_`/`next_` 只读；组由 waking_ 保持存活。
        let mut cur = unsafe { root.head_() };
        let mut woke = false;
        let snap = self.stat_.snapshot();
        while let Option::Some(node) = cur {
            // SAFETY: 链节点在组存活期间一直有效。
            let node_ref = unsafe { node.as_ref() };
            // SAFETY: 链已封口，`next_` 稳定。
            let next = unsafe { node_ref.next_() };
            if node_ref.is_upgr_read_() && snap.upgrade_active {
                cur = next;
                continue;
            }
            if let Option::Some(waker) = node_ref.as_reader_().admit_() {
                waker.wake();
                woke = true;
            }
            cur = next;
        }

        let mut q = self.queue_.lock();
        // SAFETY: `group` 在唤醒期间由 waking_ 保持存活。
        let state = unsafe { group.as_ref().as_root_read_().state_() };
        state.waking_ = false;
        let retry = q.retry_;
        q.retry_ = false;
        drop(q);
        woke || retry
    }

    /// 推进放行流程。
    fn run_pass_(&self) {
        if !self.stat_.waiters_present() {
            return;
        }
        loop {
            let selected = {
                let mut q = self.queue_.lock();
                self.prune_front_(&mut q);
                self.select_head_(&mut q)
            };
            let Option::Some(selected) = selected else {
                break;
            };
            if !self.wake_selection_(selected) {
                break;
            }
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 领取收尾 / 组回收
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn finish_claimed_(&self, kind: WaitKind, node: NonNull<WaitNode>) {
        // SAFETY: `node` 是刚刚领取成功的存活节点。
        if unsafe { node.as_ref() }.is_reader_() {
            return;
        }
        // SAFETY: 同上。
        debug_assert_eq!(kind, unsafe { node.as_ref() }.as_write_().kind_());
        let mut q = self.queue_.lock();
        self.finish_write_node_(&mut q, node);
        drop(q);
        self.run_pass_();
    }

    /// 释放一个已领取/已取消的写者节点，并维护计数。
    fn finish_write_node_(&self, q: &mut QueuePayload, node: NonNull<WaitNode>) {
        // SAFETY: `node` 是主队列中仍然存活的写者节点。
        let write = unsafe { node.as_ref() }.as_write_();
        if write.counted_() {
            write.set_counted_(false);
            debug_assert!(q.writer_waiters_ > 0, "writer_waiters_ underflow");
            q.writer_waiters_ = q.writer_waiters_.saturating_sub(1);
            if q.writer_waiters_ == 0 {
                self.stat_.clear_writer_queued();
            }
        }
        if let Option::Some(group) = write.root_() {
            // SAFETY: `group` 由该升级节点的 `root_` 保持存活。
            let state = unsafe { group.as_ref().as_root_read_().state_() };
            if state.upgrade_ == Option::Some(node) {
                state.upgrade_ = Option::None;
            }
        }
        self.unlink_linked_(q, node);
        self.free_node_(q, node);
    }

    /// 批量回收整个读者组：读者链节点、升级预留节点与根节点。
    fn free_group_(&self, q: &mut QueuePayload, group: NonNull<WaitNode>) {
        // SAFETY: `group` 是主队列中仍然存活的读者组。
        let root = unsafe { group.as_ref() }.as_root_read_();

        // SAFETY: `group` 是主队列中仍然存活的读者组。
        let root_active = unsafe { group.as_ref() }.as_root_read_();
        debug_assert!(
            !root_active.upgr_active_(),
            "读者组回收时仍有可升级读持锁者"
        );
        debug_assert_eq!(root.release_ops_(), 0, "读者组回收时仍有释放收尾在进行");
        // SAFETY: 持有队列锁。
        let upgrade = unsafe { root.state_() }.upgrade_.take();
        if let Option::Some(upgrade) = upgrade {
            // 复用统一收尾：如果它已被升级请求接管并计入写者等待，会在这里
            // 归还 `writer_waiters_`；休眠占位则直接回收。
            // 注意：这里必须先结束对 `state_` 的借用，再调用可能重新取得
            // `&mut RootState` 的收尾函数，避免同一 `UnsafeCell` 上出现别名。
            self.finish_write_node_(q, upgrade);
        }

        let mut cur = {
            // SAFETY: 持有队列锁。
            let state = unsafe { root.state_() };
            let head = state.head_.take();
            state.tail_ = Option::None;
            head
        };
        while let Option::Some(node) = cur {
            // SAFETY: 链节点在组存活期间一直有效。
            let next = unsafe { node.as_ref().next_() };
            self.free_node_(q, node);
            cur = next;
        }

        self.unlink_linked_(q, group);
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

    fn push_back_(&self, q: &mut QueuePayload, node: NonNull<WaitNode>) {
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
        self.mark_linked_(node, true);
    }

    fn push_front_(&self, q: &mut QueuePayload, node: NonNull<WaitNode>) {
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
        self.mark_linked_(node, true);
    }

    fn insert_after_(&self, q: &mut QueuePayload, at: NonNull<WaitNode>, node: NonNull<WaitNode>) {
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
        self.mark_linked_(node, true);
    }

    fn unlink_linked_(&self, q: &mut QueuePayload, node: NonNull<WaitNode>) {
        if !self.is_linked_(node) {
            return;
        }
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
        self.mark_linked_(node, false);
    }

    fn mark_linked_(&self, node: NonNull<WaitNode>, linked: bool) {
        // SAFETY: `node` 仍然存活；持有队列锁。
        let node_ref = unsafe { node.as_ref() };
        if node_ref.is_root_read_() {
            // SAFETY: 持有队列锁。
            unsafe { node_ref.as_root_read_().state_() }.linked_ = linked;
        } else if node_ref.is_write_() {
            node_ref.as_write_().set_linked_(linked);
        }
    }

    fn is_linked_(&self, node: NonNull<WaitNode>) -> bool {
        // SAFETY: `node` 仍然存活；持有队列锁。
        let node_ref = unsafe { node.as_ref() };
        if node_ref.is_root_read_() {
            // SAFETY: 持有队列锁。
            unsafe { node_ref.as_root_read_().state_() }.linked_
        } else if node_ref.is_write_() {
            node_ref.as_write_().linked_()
        } else {
            false
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Arena 分配 / 活跃组指针
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn alloc_node_(&self, q: &mut QueuePayload, node: WaitNode) -> NonNull<WaitNode> {
        match q.arena_.alloc(node) {
            Result::Ok(node) => node,
            Result::Err(_) => handle_alloc_error(Arena::<WaitNode>::chunk_layout()),
        }
    }

    fn free_node_(&self, q: &mut QueuePayload, node: NonNull<WaitNode>) {
        // SAFETY: `node` 已经脱链，且没有其他存活引用。
        drop(unsafe { q.arena_.free(node) });
    }

    fn set_active_group_(&self, group: NonNull<WaitNode>) {
        let expected = core::ptr::null_mut();
        let _ = self.active_read_group_.compare_exchange(
            expected,
            group.as_ptr(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        debug_assert!(
            self.active_read_group_.load(Ordering::Acquire) == group.as_ptr(),
            "同时出现两个活跃读者组"
        );
    }

    fn clear_active_group_(&self, group: NonNull<WaitNode>) {
        let _ = self.active_read_group_.compare_exchange(
            group.as_ptr(),
            core::ptr::null_mut(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn active_group_(&self) -> Option<NonNull<WaitNode>> {
        NonNull::new(self.active_read_group_.load(Ordering::Acquire))
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 计数
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn note_exclusive_queued_(&self, q: &mut QueuePayload) {
        q.writer_waiters_ += 1;
        if q.writer_waiters_ == 1 {
            self.stat_.mark_writer_queued();
        }
    }

    fn slot_state_(&self, node: &WaitNode) -> SlotState {
        if node.is_reader_() {
            node.as_reader_().state_()
        } else {
            node.as_write_().state_()
        }
    }

    fn claim_node_(&self, node: &WaitNode, kind: WaitKind) -> Option<SlotState> {
        if node.is_reader_() {
            node.as_reader_().claim_(kind == WaitKind::UpgradableRead)
        } else {
            debug_assert_eq!(kind, node.as_write_().kind_());
            node.as_write_().claim_()
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
        // 先登记“释放收尾进行中”，保证 2→1 的线程在访问组做升级预留时，
        // 1→0 的线程不会提前把组回收掉。
        root.begin_release_();
        let old = root.dec_count_();
        debug_assert!(old >= 1, "读者组 count_ 下溢");
        if is_upgradable {
            root.set_upgr_active_(false);
        }
        match old {
            1 => {
                // 减到 0：交给队首 prune 批量回收；如果还有别的释放收尾在
                // 进行，prune 会等它们结束。
                root.end_release_();
                self.run_pass_();
            }
            2 => {
                // 减到 1：若剩余的是已领取的 UpgrRead，预分配升级写节点。
                let mut q = self.queue_.lock();
                self.ensure_upgrade_placeholder_(&mut q, group);
                drop(q);
                root.end_release_();
                self.run_pass_();
            }
            _ => {
                root.end_release_();
            }
        }
    }

    /// 在组后继预分配一个休眠升级写节点（若尚无）。
    fn ensure_upgrade_placeholder_(&self, q: &mut QueuePayload, group: NonNull<WaitNode>) {
        // SAFETY: `group` 由活跃组指针保持存活；持有队列锁。
        let state = unsafe { group.as_ref().as_root_read_().state_() };
        if state.upgrade_.is_some() {
            return;
        }
        // SAFETY: `group` 由活跃组指针保持存活。
        if !unsafe { group.as_ref() }.as_root_read_().upgr_active_() {
            return;
        }
        let node = self.alloc_node_(q, WaitNode::new_reserved_upgrade(group));
        self.insert_after_(q, group, node);
        state.upgrade_ = Option::Some(node);
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
                .set_upgr_active_(false);
        }
        self.run_pass_();
    }
}

#[cfg(test)]
mod tests_ {
    use core::{sync::atomic::AtomicUsize, task::Waker};

    use super::{AcqProgress, RwCoreArena, WaitKind};

    /// 验证写者持有期间读者的入队、放行与领取流程。
    ///
    /// Verifies a queued reader can be admitted and claimed while the writer holds.
    ///
    /// - 手段：先取写者，再以 Read 身份排队；释放写者后 poll 领取。
    /// - 判断：释放前返回 Waiting；释放后能领取，队列最终清空。
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

    /// 验证跨 chunk 的大读者链在最后一个读者释放时被整链批量回收。
    ///
    /// Verifies a large reader chain spanning Arena chunks is reclaimed in bulk
    /// when the last reader releases.
    ///
    /// - 手段：写者持有期间排队 1100 个普通读（超过一个 chunk 的 1024 槽），
    ///   释放写者后逐个领取并释放。
    /// - 判断：全部释放后队列与 Arena 都为空，说明没有逐节点残留。
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

    /// 验证读者链中间节点取消后，整链仍能在最后一个读者释放时回收。
    ///
    /// Verifies a cancelled middle reader node is reclaimed with the rest of the
    /// chain when the last reader releases.
    ///
    /// - 手段：写者持有期间排队 3 个读者，取消中间一个；释放写者后领取并
    ///   释放其余两个。
    /// - 判断：取消的节点不阻塞放行，最终队列与 Arena 都为空。
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

    /// 验证可升级读先于普通读释放时不会误留升级占位。
    ///
    /// Verifies releasing the upgradable reader before a plain reader does not
    /// leave a spurious upgrade reservation.
    ///
    /// - 手段：写者持有期间排队普通读 + 可升级读；释放写者并领取两者，
    ///   先释放可升级读，再释放普通读。
    /// - 判断：可升级读释放后写者仍不能进入（普通读还持有），但
    ///   `writer_queued` 为假；普通读释放后队列清空、Arena 清空，写者可快速进入。
    #[test]
    fn upgradable_released_first_does_not_reserve_writer() {
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
        assert!(!core.writer_queued(), "不应为已释放的可升级读预留写者");
        assert!(!core.try_write_fast(), "普通读仍持有写者不应进入");

        core.release_reader(true);
        assert!(!core.waiters_present(), "全部释放后队列应为空");
        assert!(core.pool_is_empty(), "整条读者链应被批量归还 Arena");
        assert!(core.try_write_fast(), "读者组结束后写者应能快速进入");
        core.release_writer();
    }

    /// 验证读者组在首个读者释放后预分配升级占位，并能被升级请求接管。
    ///
    /// Verifies the reader group pre-allocates an upgrade placeholder when the
    /// count drops to one, and that an upgrade request adopts it.
    ///
    /// - 手段：写者持有期间排队一个普通读与一个可升级读；释放写者后两个读者
    ///   领取，再释放普通读（组 count 2→1）。
    /// - 判断：可升级读随后发起的升级能领取写许可；写者释放、可升级读释放后
    ///   队列清空且 Arena 中不再有存活节点（整组批量回收）。
    #[test]
    fn upgrade_placeholder_is_created_and_adopted() {
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

        // 普通读释放后组 count 从 2 减到 1，应生成休眠升级占位。
        core.release_reader(true);
        assert!(
            core.has_reserved_upgrade(),
            "应为剩余的可升级读预分配写节点"
        );

        let AcqProgress::Waiting(upgrade) = core.acquire_upgrade(waker, true) else {
            panic!("升级请求应排队");
        };
        assert!(core.poll_acquire(&upgrade, waker), "升级请求应能领取写许可");

        core.release_writer();
        core.release_upgradable_read(true);
        assert!(!core.waiters_present(), "全部释放后队列应为空");
        assert!(core.pool_is_empty(), "整条读者链应被批量归还 Arena");
    }
}
