//! Arena 版协作式读写锁内核：状态字 + 侵入式等待链 + Arena 节点/槽位。
//!
//! 并发语义与堆分配版 `core_` 一致，但把队列锁缩小到只保护拓扑：
//!
//! - 队列锁只负责入队、摘链、队首选择、`sealed_` / `waking_` 标记与 Arena
//!   回收；**不负责唤醒**；
//! - 被选中的读节点会 `sealed_`，新读者不再合并进来；`waking_` 保证同一节点
//!   只有一个唤醒者，并防止它在唤醒期间被回收；
//! - 唤醒者在锁外逐个槽位取 waker 并唤醒，不需要收集批，也不需要在固定容量
//!   批次之间反复取队列锁；
//! - 已被放行的读者可以直接 CAS 领取，不再检查队首、也不再取队列锁；只有
//!   最后一个 pending 槽位完成时才取一次队列锁推进队列；
//! - 等待节点与槽位都由 [`Arena`] 分配，等待路径除 chunk 外不分配堆内存。

use alloc::alloc::{Global, handle_alloc_error};
use core::{borrow::BorrowMut, ptr::NonNull, task::Waker};

use funty::Unsigned;

use atomex::{StrictOrderings, TrAtomicData, TrCmpxchOrderings, x_deps::funty};

use crate::rwlock::{
    state_::CoopRwState,
    wait_::{SpinLock, WaitKind},
};

use super::{
    arena_::Arena,
    wait_arena_::{ArenaItem, ArenaWaitNode, ArenaWaiterSlot, SlotState},
};

/// 等待中 future 持有的句柄：节点元素 + 槽位元素。
#[derive(Clone, Copy)]
pub(super) struct PendingWait {
    node_: NonNull<ArenaItem>,
    slot_: NonNull<ArenaItem>,
}

// SAFETY: 句柄只保存两个 Arena 元素的地址。元素由 Arena 持有，地址在存活期间
// 稳定；所有解引用都发生在队列/节点锁内，移动句柄本身不会产生别名访问。
unsafe impl Send for PendingWait {}
// SAFETY: 同上；共享 `&PendingWait` 只读取地址，不提供可变访问。
unsafe impl Sync for PendingWait {}

/// 一次获取动作的当前进展。
pub(super) enum AcqProgress {
    /// 已经拿到许可。
    Acquired,
    /// 仍在队列里等待；future 需要记住这个句柄以便重试与取消。
    Waiting(PendingWait),
}

/// 队列自旋锁保护的载荷：侵入式双向链表 + 独占等待者计数 + 节点/槽位池。
struct QueuePayload {
    /// 队首（最先到达的等待节点）。
    head_: Option<NonNull<ArenaItem>>,
    /// 队尾（最新到达的等待节点）。
    tail_: Option<NonNull<ArenaItem>>,
    /// 队列中仍处于待决状态的 `Write` / `Upgrade` 槽位数。
    writer_waiters_: usize,
    /// 有线程发现队首正在被别的唤醒者处理，需要唤醒者在清标记后重跑。
    retry_: bool,
    /// 等待节点与等待槽位共用的池。
    arena_: Arena<ArenaItem>,
}

// SAFETY: `head_` / `tail_` 只指向同一个 `arena_` 拥有的元素。移动
// `QueuePayload` 时 `Arena` 与链接一起移动，元素地址不变；所有链接读写都在
// 队列自旋锁内完成。Arena 自身在 `ArenaItem: Send` 时满足 `Send`。
unsafe impl Send for QueuePayload {}

/// Arena 版读写锁的共享内核。
pub(super) struct RwCoreArena<D, B, O = StrictOrderings>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    stat_: CoopRwState<D, B, O>,
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
        let (claimed, pending) = {
            let mut q = self.queue_.lock();
            self.prune_front_(&mut q);
            let pending = self.enqueue_(&mut q, kind);
            if !self.stat_.waiters_present() {
                self.stat_.mark_waiters_present();
            }

            let claimed = {
                // SAFETY: `pending` 刚由本队列创建，节点在队列锁内保持存活。
                let node = unsafe { pending.node_.as_ref() }.as_node();
                let claimed = self.is_head_(&q, &pending) && self.try_claim_(kind);
                if claimed {
                    let finish = node.mark_acquired(pending.slot_);
                    if finish {
                        // 队列锁已持有，直接完成收尾，稍后统一 prune。
                        node.clear_finalizing_();
                    }
                    self.note_exclusive_slot_finished_(&mut q, &pending);
                } else {
                    node.set_waker(pending.slot_, waker);
                }
                claimed
            };
            (claimed, pending)
        };

        // 入队者已经注册 waker；锁外推进放行流程。
        self.run_pass_();

        if claimed {
            AcqProgress::Acquired
        } else {
            AcqProgress::Waiting(pending)
        }
    }

    /// 已被唤醒的等待者重试领取。
    ///
    /// 如果槽位已经是 `Admitted`，这里直接 CAS 领取，**不获取队列锁**；
    /// 只有独占槽位完成计数，或该槽位是节点最后一个 pending 槽位时，才取
    /// 一次队列锁推进队列。
    pub(super) fn poll_acquire(&self, pending: &PendingWait, waker: &Waker) -> bool {
        // SAFETY: 句柄对应的槽位仍处于待决状态，元素一定还没有被回收。
        let node = unsafe { pending.node_.as_ref() }.as_node();
        let state = node.slot_state_(pending.slot_);

        if state == SlotState::Admitted {
            let kind = node.slot_kind(pending.slot_);
            if self.try_claim_(kind) {
                let finish = node.mark_acquired(pending.slot_);
                self.finish_claimed_(pending, kind, finish);
                return true;
            }
        } else if state != SlotState::Waiting {
            debug_assert!(false, "poll_acquire: 在 Acquired/Cancelled 状态上再次 poll");
            return false;
        }

        node.set_waker(pending.slot_, waker);
        self.run_pass_();
        false
    }

    /// 等待者被取消（future 被丢弃）。
    pub(super) fn cancel_wait(&self, pending: &PendingWait) {
        // SAFETY: 句柄对应的槽位仍处于待决状态，元素一定还没有被回收。
        let node = unsafe { pending.node_.as_ref() }.as_node();
        let kind = node.slot_kind(pending.slot_);
        let finish = node.mark_cancelled(pending.slot_);

        if matches!(kind, WaitKind::Write | WaitKind::Upgrade) {
            let mut q = self.queue_.lock();
            self.note_exclusive_slot_finished_(&mut q, pending);
            if finish {
                // SAFETY: finalizing_ 保证节点在清除前不会被回收。
                unsafe { pending.node_.as_ref() }
                    .as_node()
                    .clear_finalizing_();
            }
            drop(q);
            self.run_pass_();
        } else if finish {
            self.finalize_node_(pending.node_);
        } else {
            self.run_pass_();
        }
    }

    /// 槽位已经领取成功后的收尾。
    fn finish_claimed_(&self, pending: &PendingWait, kind: WaitKind, finish: bool) {
        if matches!(kind, WaitKind::Write | WaitKind::Upgrade) {
            let mut q = self.queue_.lock();
            self.note_exclusive_slot_finished_(&mut q, pending);
            if finish {
                // SAFETY: finalizing_ 保证节点在清除前不会被回收。
                unsafe { pending.node_.as_ref() }
                    .as_node()
                    .clear_finalizing_();
            }
            drop(q);
            self.run_pass_();
            return;
        }
        if finish {
            self.finalize_node_(pending.node_);
        }
    }

    /// 节点最后一个 pending 槽位完成后的收尾。
    ///
    /// `finalizing_` 在槽位完成时置位，阻止其他线程提前回收；这里在队列锁内
    /// 清除并 prune。节点不是队首时只清除标记，等它到达队首再回收。
    fn finalize_node_(&self, node_item: NonNull<ArenaItem>) {
        let mut q = self.queue_.lock();
        // SAFETY: finalizing_ 在本次调用前保证节点不会被回收。
        unsafe { &mut *node_item.as_ptr() }
            .as_node_mut()
            .clear_finalizing_();
        self.prune_front_(&mut q);
        drop(q);
        self.run_pass_();
    }

    fn enqueue_(&self, q: &mut QueuePayload, kind: WaitKind) -> PendingWait {
        // 读类等待者只与"还没被封口"的读类尾节点合并。
        if kind.is_readish()
            && let Some(tail) = q.tail_
        {
            // SAFETY: `tail_` 指向本队列中仍然存活的节点。
            let tail_node = unsafe { tail.as_ref() }.as_node();
            if tail_node.is_readish() && !tail_node.sealed_ {
                let slot = self.alloc_slot_(q, kind);
                tail_node.push_slot(slot);
                return PendingWait {
                    node_: tail,
                    slot_: slot,
                };
            }
        }

        let node = self.alloc_node_(q, kind);
        let slot = self.alloc_slot_(q, kind);
        // SAFETY: 两个元素都是刚分配、由本队列独占的 Arena 元素。
        unsafe { node.as_ref() }.as_node().push_slot(slot);

        let pending = PendingWait {
            node_: node,
            slot_: slot,
        };
        match kind {
            // 升级请求插队，理由见堆分配版 `core_` 的说明。
            WaitKind::Upgrade => {
                Self::push_front_(q, node);
                self.note_exclusive_slot_queued_(q);
            }
            WaitKind::Write => {
                Self::push_back_(q, node);
                self.note_exclusive_slot_queued_(q);
            }
            // 读类等待者新建的读类节点不构成写者屏障。
            _ => Self::push_back_(q, node),
        }
        pending
    }

    /// 从 Arena 取一个节点元素；chunk 分配失败时按标准分配错误路径终止。
    fn alloc_node_(&self, q: &mut QueuePayload, kind: WaitKind) -> NonNull<ArenaItem> {
        match q.arena_.alloc(ArenaItem::Node(ArenaWaitNode::new(kind))) {
            Result::Ok(node) => node,
            Result::Err(_) => handle_alloc_error(Arena::<ArenaItem>::chunk_layout()),
        }
    }

    /// 从 Arena 取一个槽位元素。
    fn alloc_slot_(&self, q: &mut QueuePayload, kind: WaitKind) -> NonNull<ArenaItem> {
        match q.arena_.alloc(ArenaItem::Slot(ArenaWaiterSlot::new(kind))) {
            Result::Ok(slot) => slot,
            Result::Err(_) => handle_alloc_error(Arena::<ArenaItem>::chunk_layout()),
        }
    }

    fn push_front_(q: &mut QueuePayload, node: NonNull<ArenaItem>) {
        let old_head = q.head_;
        // SAFETY: `node` 非空且是本队列独占的 Arena 元素。
        let node_ref = unsafe { &mut *node.as_ptr() }.as_node_mut();
        node_ref.prev_ = Option::None;
        node_ref.next_ = old_head;
        match old_head {
            Some(head) => {
                // SAFETY: 旧 head 非空且是本队列中仍然存活的节点。
                unsafe { &mut *head.as_ptr() }.as_node_mut().prev_ = Option::Some(node);
            }
            None => q.tail_ = Option::Some(node),
        }
        q.head_ = Option::Some(node);
    }

    fn push_back_(q: &mut QueuePayload, node: NonNull<ArenaItem>) {
        let old_tail = q.tail_;
        // SAFETY: `node` 非空且是本队列独占的 Arena 元素。
        let node_ref = unsafe { &mut *node.as_ptr() }.as_node_mut();
        node_ref.next_ = Option::None;
        node_ref.prev_ = old_tail;
        match old_tail {
            Some(tail) => {
                // SAFETY: 旧 tail 非空且是本队列中仍然存活的节点。
                unsafe { &mut *tail.as_ptr() }.as_node_mut().next_ = Option::Some(node);
            }
            None => q.head_ = Option::Some(node),
        }
        q.tail_ = Option::Some(node);
    }

    fn note_exclusive_slot_queued_(&self, q: &mut QueuePayload) {
        q.writer_waiters_ += 1;
        if q.writer_waiters_ == 1 {
            self.stat_.mark_writer_queued();
        }
    }

    fn note_exclusive_slot_finished_(&self, q: &mut QueuePayload, pending: &PendingWait) {
        // SAFETY: 调用发生在元素被回收之前。
        let node = unsafe { pending.node_.as_ref() }.as_node();
        if node.is_readish() {
            return;
        }
        debug_assert!(q.writer_waiters_ > 0, "writer_waiters_ underflow");
        q.writer_waiters_ = q.writer_waiters_.saturating_sub(1);
        if q.writer_waiters_ == 0 {
            self.stat_.clear_writer_queued();
        }
    }

    fn is_head_(&self, q: &QueuePayload, pending: &PendingWait) -> bool {
        q.head_ == Option::Some(pending.node_)
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 队列推进 / 锁外唤醒
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 剪掉队首已经"没有任何在等的人"的节点，并把节点/槽位还给 Arena。
    ///
    /// `waking_` 的节点正在被唤醒者遍历，即使 pending 为 0 也不能回收。
    fn prune_front_(&self, q: &mut QueuePayload) {
        while let Some(front) = q.head_ {
            // SAFETY: `front` 是链表中仍然存活的节点。
            let (pending_count, next, waking, finalizing) = unsafe {
                let node = front.as_ref().as_node();
                (
                    node.pending_count(),
                    node.next_,
                    node.waking_,
                    node.is_finalizing_(),
                )
            };
            if waking || finalizing || pending_count != 0 {
                break;
            }

            q.head_ = next;
            match next {
                // SAFETY: `next` 是链表中仍然存活的节点。
                Some(next) => unsafe { &mut *next.as_ptr() }.as_node_mut().prev_ = Option::None,
                None => q.tail_ = Option::None,
            }

            // SAFETY: 队首已经摘除，节点的待决槽位为 0，可以安全回收。
            let slots = unsafe { front.as_ref() }.as_node().take_slots();
            let mut cur = slots.head_;
            while let Some(item) = cur {
                // SAFETY: 链上的槽位仍然存活，且节点已经没有待决槽位。
                let next_slot = unsafe { item.as_ref() }.as_slot().next_;
                drop(unsafe { q.arena_.free(item) });
                cur = next_slot;
            }
            drop(unsafe { q.arena_.free(front) });
        }
    }

    /// 在队列锁内选出可以放行的队首节点，并设置 `sealed_` / `waking_`。
    fn select_head_(&self, q: &mut QueuePayload) -> Option<NonNull<ArenaItem>> {
        if q.head_.is_none() {
            self.stat_.clear_waiters_present();
            if q.writer_waiters_ != 0 {
                debug_assert!(false, "writer_waiters_ leaked while queue is empty");
                q.writer_waiters_ = 0;
                self.stat_.clear_writer_queued();
            }
            return Option::None;
        }

        let snap = self.stat_.snapshot();
        debug_assert_eq!(
            snap.writer_queued,
            q.writer_waiters_ > 0,
            "WRITER_QUEUED 与 writer_waiters_ 不一致"
        );
        if snap.writer_active {
            return Option::None;
        }

        let front = q.head_?;
        // SAFETY: `front` 是链表中仍然存活的节点；队列锁保证这里独占修改标记。
        let node = unsafe { &mut *front.as_ptr() }.as_node_mut();
        if node.waking_ {
            // 让当前唤醒者在清除 waking_ 后重跑一次，避免并发设置 waker 的
            // 线程看到 waking_ 就退出、而唤醒者已经扫过该槽位的丢唤醒窗口。
            q.retry_ = true;
            return Option::None;
        }
        if node.is_finalizing_() {
            return Option::None;
        }

        if node.is_readish() {
            // 封口：此后到达的读类等待者不会合并进正在放行的节点。
            node.sealed_ = true;
            node.waking_ = true;
            return Option::Some(front);
        }

        let admitted = match node.solo_kind() {
            WaitKind::Write if snap.reader_count == D::ZERO && !snap.upgrade_active => true,
            WaitKind::Upgrade if snap.reader_count == D::ONE => true,
            _ => false,
        };
        if admitted {
            node.waking_ = true;
            Option::Some(front)
        } else {
            Option::None
        }
    }

    /// 锁外唤醒一个被选中的节点；返回是否真的放行了槽位。
    ///
    /// 期间 `waking_` 阻止节点被 `prune_front_` 回收；槽位状态由节点锁保护，
    /// 每次 `wake` 都在释放节点锁之后执行。
    fn wake_node_(&self, node_item: NonNull<ArenaItem>) -> bool {
        // SAFETY: `node_item` 是 select_head_ 选中的存活节点。
        let node = unsafe { node_item.as_ref() }.as_node();

        let woke = if node.is_readish() {
            let upgrade_active = self.stat_.snapshot().upgrade_active;
            let mut upgrade_taken = upgrade_active || node.has_admitted_upgradable_();
            let mut cur = node.slot_head_();
            let mut woke = false;
            while let Some(slot) = cur {
                // 槽位链只追加、不摘除；节点由 waking_ 保持存活。
                let next = node.slot_next_(slot);
                if let Option::Some(waker) =
                    node.try_admit_readish_slot_(slot, &mut upgrade_taken, upgrade_active)
                {
                    waker.wake();
                    woke = true;
                }
                cur = next;
            }
            woke
        } else {
            match node.slot_head_() {
                Option::Some(slot) => match node.try_admit_exclusive_slot_(slot) {
                    Option::Some(waker) => {
                        waker.wake();
                        true
                    }
                    Option::None => false,
                },
                Option::None => false,
            }
        };

        // 清除 waking_ 后才能回收节点；下一轮 run_pass_ 会负责 prune。
        let mut q = self.queue_.lock();
        // SAFETY: waking_ 在本次唤醒期间阻止了节点回收；队列锁保证独占标记。
        unsafe { &mut *node_item.as_ptr() }.as_node_mut().waking_ = false;
        let retry = q.retry_;
        q.retry_ = false;
        drop(q);
        woke || retry
    }

    /// 推进放行流程。队列锁只用于选择/回收节点，唤醒全部在锁外完成。
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
            let Option::Some(node) = selected else {
                break;
            };
            if !self.wake_node_(node) {
                break;
            }
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 释放 / 降级
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn release_reader(&self) {
        let _ok = self.stat_.release_reader();
        debug_assert!(_ok, "state transition `release_reader` failed");
        self.run_pass_();
    }

    pub(super) fn release_upgradable_read(&self) {
        let _ok = self.stat_.release_upgradable_read();
        debug_assert!(_ok, "state transition `release_upgradable_read` failed");
        self.run_pass_();
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

    pub(super) fn downgrade_upgradable_to_read(&self) {
        let _ok = self.stat_.downgrade_upgradable_to_read();
        debug_assert!(
            _ok,
            "state transition `downgrade_upgradable_to_read` failed"
        );
        self.run_pass_();
    }
}

#[cfg(test)]
mod tests_ {
    use alloc::vec::Vec;

    use core::{sync::atomic::AtomicUsize, task::Waker};

    use super::{AcqProgress, RwCoreArena, WaitKind};

    /// 验证写者持有期间读者的入队、放行与领取流程。
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
        core.release_reader();
        assert!(!core.waiters_present(), "领取并回收后队列应为空");
    }

    /// 验证取消写者等待会收回 `WRITER_QUEUED` 并回收元素。
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

        core.release_writer();
        assert!(core.try_read_fast(), "取消的写者不应再挡住读者");
        core.release_reader();
    }

    /// 验证一次放行大批读者时所有槽位都能被唤醒和领取。
    #[test]
    fn many_queued_readers_are_all_admitted_and_claimed() {
        let core = RwCoreArena::<usize, AtomicUsize>::new(AtomicUsize::new(0));
        assert!(core.try_write_fast(), "写者快速路径应成功");

        let waker = Waker::noop();
        let total = 70;
        let mut pending = Vec::with_capacity(total);
        for _ in 0..total {
            let AcqProgress::Waiting(p) = core.acquire(WaitKind::Read, waker) else {
                panic!("写者持有时读者不应直接领取");
            };
            pending.push(p);
        }
        assert!(core.waiters_present(), "读者应已进入队列");

        core.release_writer();
        for p in &pending {
            assert!(core.poll_acquire(p, waker), "读者应能领取");
        }
        for _ in 0..total {
            core.release_reader();
        }
        assert!(!core.waiters_present(), "全部领取并释放后队列应为空");
    }
}
