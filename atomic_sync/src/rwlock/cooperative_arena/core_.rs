//! Arena 版协作式读写锁内核：状态字 + 侵入式等待链 + Arena 节点分配。
//!
//! 并发语义与堆分配版 `core_` **逐条一致**，差别只在等待队列的承载方式：
//!
//! - 节点由 [`Arena`] 分配，链表中直接保存 `NonNull<ArenaWaitNode>`；
//! - 队列只需要 `head_` / `tail_` 两个指针，入队/出队/插队都是 O(1) 指针操作；
//! - 节点从队首回收后立即归还 Arena 的槽位，Arena 在非当前 chunk 清空时把
//!   整块内存归还分配器。
//!
//! 所有队列操作仍然在 `queue_` 自旋锁内完成；锁内只收集 waker，不调用
//! `Waker::wake`。

use alloc::{
    alloc::{Global, handle_alloc_error},
    vec::Vec,
};
use core::{borrow::BorrowMut, ptr::NonNull, task::Waker};

use funty::Unsigned;

use atomex::{StrictOrderings, TrAtomicData, TrCmpxchOrderings, x_deps::funty};

use crate::rwlock::{
    state_::CoopRwState,
    wait_::{SpinLock, WaitKind},
};

use super::{arena_::Arena, wait_arena_::ArenaWaitNode};

/// 等待中 future 持有的句柄：Arena 节点指针 + 槽位下标。
///
/// 节点在槽位待决期间一定存活；句柄本身不提供访问，只有 [`RwCoreArena`] 在持有
/// 队列锁时才会解引用它。
#[derive(Clone, Copy)]
pub(super) struct PendingWait {
    node_: NonNull<ArenaWaitNode>,
    slot_: usize,
}

// SAFETY: 句柄只保存一个 Arena 内节点的地址与槽位下标。节点由 Arena 持有，
// 地址在存活期间稳定；所有解引用都发生在队列自旋锁内，移动句柄本身不会产生
// 别名访问。`Arena` 已在 `T: Send` 时实现 `Send`。
unsafe impl Send for PendingWait {}
// SAFETY: 同上；共享 `&PendingWait` 只读取地址与下标，不提供可变访问。
unsafe impl Sync for PendingWait {}

/// 一次获取动作的当前进展。
pub(super) enum AcqProgress {
    /// 已经拿到许可。
    Acquired,
    /// 仍在队列里等待；future 需要记住这个句柄以便重试与取消。
    Waiting(PendingWait),
}

/// 队列自旋锁保护的载荷：侵入式双向链表 + 独占等待者计数 + 节点池。
struct QueuePayload {
    /// 队首（最先到达的等待节点）。
    head_: Option<NonNull<ArenaWaitNode>>,
    /// 队尾（最新到达的等待节点）。
    tail_: Option<NonNull<ArenaWaitNode>>,
    /// 队列中仍处于待决状态的 `Write` / `Upgrade` 槽位数。
    writer_waiters_: usize,
    /// 等待节点池。
    arena_: Arena<ArenaWaitNode>,
}

// SAFETY: `head_` / `tail_` 只指向同一个 `arena_` 拥有的节点。移动
// `QueuePayload` 时 `Arena` 与链接一起移动，节点地址不变；所有链接读写都在
// 队列自旋锁内完成。Arena 自身在 `ArenaWaitNode: Send` 时满足 `Send`。
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
    // 获取：入队 / 领取 / 注册 waker 在同一段队列锁临界区内完成
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn acquire(&self, kind: WaitKind, waker: &Waker) -> AcqProgress {
        let mut q = self.queue_.lock();
        self.prune_front_(&mut q);
        let pending = self.enqueue_(&mut q, kind);
        if !self.stat_.waiters_present() {
            self.stat_.mark_waiters_present();
        }

        let claimed = {
            // SAFETY: `pending` 刚由本队列创建，节点在队列锁内保持存活。
            let node = unsafe { pending.node_.as_ref() };
            let claimed = self.is_head_(&q, &pending) && self.try_claim_(kind);
            if claimed {
                node.mark_acquired(pending.slot_);
                self.note_exclusive_slot_finished_(&mut q, &pending);
            } else {
                node.set_waker(pending.slot_, waker);
            }
            claimed
        };

        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);

        if claimed {
            AcqProgress::Acquired
        } else {
            AcqProgress::Waiting(pending)
        }
    }

    /// 已被唤醒的等待者重试领取。
    pub(super) fn poll_acquire(&self, pending: &PendingWait, waker: &Waker) -> bool {
        let mut q = self.queue_.lock();
        self.prune_front_(&mut q);

        let claimed = {
            // SAFETY: 句柄对应的槽位仍处于待决状态，节点一定还没有被回收。
            let node = unsafe { pending.node_.as_ref() };
            let claimed =
                self.is_head_(&q, pending) && self.try_claim_(node.slot_kind(pending.slot_));
            if claimed {
                node.mark_acquired(pending.slot_);
                self.note_exclusive_slot_finished_(&mut q, pending);
            } else {
                node.set_waker(pending.slot_, waker);
            }
            claimed
        };

        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);
        claimed
    }

    /// 等待者被取消（future 被丢弃）。
    pub(super) fn cancel_wait(&self, pending: &PendingWait) {
        let mut q = self.queue_.lock();
        {
            // SAFETY: 句柄对应的槽位仍处于待决状态，节点一定还没有被回收。
            let node = unsafe { pending.node_.as_ref() };
            node.mark_cancelled(pending.slot_);
            self.note_exclusive_slot_finished_(&mut q, pending);
        }
        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);
    }

    fn enqueue_(&self, q: &mut QueuePayload, kind: WaitKind) -> PendingWait {
        // 读类等待者与队尾的读类节点合并；只与队尾合并，保证 FIFO 次序不被破坏。
        if kind.is_readish()
            && let Some(tail) = q.tail_
        {
            // SAFETY: `tail_` 指向本队列中仍然存活的节点。
            let tail_ref = unsafe { tail.as_ref() };
            if tail_ref.is_readish() {
                let slot_ = tail_ref.push_readish_slot(kind);
                return PendingWait { node_: tail, slot_ };
            }
        }

        let pending = self.alloc_wait_node_(q, kind);
        match kind {
            // 升级请求插队，理由见 `core_` 头部说明。
            WaitKind::Upgrade => {
                Self::push_front_(q, pending.node_);
                self.note_exclusive_slot_queued_(q);
            }
            WaitKind::Write => {
                Self::push_back_(q, pending.node_);
                self.note_exclusive_slot_queued_(q);
            }
            // 读类等待者新建的读类节点不构成写者屏障。
            _ => Self::push_back_(q, pending.node_),
        }
        pending
    }

    /// 从 Arena 取一个节点；chunk 分配失败时按标准分配错误处理路径终止。
    fn alloc_wait_node_(&self, q: &mut QueuePayload, kind: WaitKind) -> PendingWait {
        match q.arena_.alloc(ArenaWaitNode::new(kind)) {
            Result::Ok(node_) => PendingWait { node_, slot_: 0 },
            Result::Err(_) => handle_alloc_error(Arena::<ArenaWaitNode>::chunk_layout()),
        }
    }

    fn push_front_(q: &mut QueuePayload, node: NonNull<ArenaWaitNode>) {
        // SAFETY: `node` 是本队列独占的 Arena 节点，`q` 由队列锁独占。
        unsafe {
            let old_head = q.head_;
            (*node.as_ptr()).prev_ = Option::None;
            (*node.as_ptr()).next_ = old_head;
            match old_head {
                Some(head) => (*head.as_ptr()).prev_ = Some(node),
                None => q.tail_ = Some(node),
            }
        }
        q.head_ = Some(node);
    }

    fn push_back_(q: &mut QueuePayload, node: NonNull<ArenaWaitNode>) {
        // SAFETY: 同 `push_front_`。
        unsafe {
            let old_tail = q.tail_;
            (*node.as_ptr()).next_ = Option::None;
            (*node.as_ptr()).prev_ = old_tail;
            match old_tail {
                Some(tail) => (*tail.as_ptr()).next_ = Some(node),
                None => q.head_ = Some(node),
            }
        }
        q.tail_ = Some(node);
    }

    fn note_exclusive_slot_queued_(&self, q: &mut QueuePayload) {
        q.writer_waiters_ += 1;
        if q.writer_waiters_ == 1 {
            self.stat_.mark_writer_queued();
        }
    }

    fn note_exclusive_slot_finished_(&self, q: &mut QueuePayload, pending: &PendingWait) {
        // SAFETY: 调用发生在节点被回收之前。
        let node = unsafe { pending.node_.as_ref() };
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
        q.head_ == Some(pending.node_)
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 放行流程
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 剪掉队首已经"没有任何在等的人"的节点，并立即把节点还给 Arena。
    fn prune_front_(&self, q: &mut QueuePayload) {
        while let Some(front) = q.head_ {
            // SAFETY: `front` 是链表中仍然存活的节点。
            let (pending_count, next) = unsafe {
                let front = front.as_ref();
                (front.pending_count(), front.next_)
            };
            if pending_count != 0 {
                break;
            }

            q.head_ = next;
            match next {
                // SAFETY: `next` 是链表中仍然存活的节点。
                Some(next) => unsafe { (*next.as_ptr()).prev_ = Option::None },
                None => q.tail_ = Option::None,
            }
            // SAFETY: 队首已经摘除，节点的待决槽位为 0，可以安全归还 Arena。
            // 返回的节点在这里析构；它已不再被队列引用。
            drop(unsafe { q.arena_.free(front) });
        }
    }

    fn pass_(&self, q: &mut QueuePayload) -> Vec<Waker> {
        self.prune_front_(q);
        if q.head_.is_none() {
            self.stat_.clear_waiters_present();
            if q.writer_waiters_ != 0 {
                debug_assert!(false, "writer_waiters_ leaked while queue is empty");
                q.writer_waiters_ = 0;
                self.stat_.clear_writer_queued();
            }
            return Vec::new();
        }

        let snap = self.stat_.snapshot();
        debug_assert_eq!(
            snap.writer_queued,
            q.writer_waiters_ > 0,
            "WRITER_QUEUED 与 writer_waiters_ 不一致"
        );
        if snap.writer_active {
            return Vec::new();
        }

        let Some(front) = q.head_ else {
            return Vec::new();
        };
        // SAFETY: `front` 是链表中仍然存活的节点，且 `prune_front_` 后仍有待决槽位。
        let front = unsafe { front.as_ref() };
        let mut out = Vec::new();
        if front.is_readish() {
            let mut upgrade_taken = snap.upgrade_active;
            front.admit_waiting(&mut out, |kind| match kind {
                WaitKind::Read => true,
                WaitKind::UpgradableRead if !upgrade_taken => {
                    upgrade_taken = true;
                    true
                }
                _ => false,
            });
        } else {
            match front.solo_kind() {
                WaitKind::Write if snap.reader_count == D::ZERO && !snap.upgrade_active => {
                    front.admit_waiting(&mut out, |_| true);
                }
                WaitKind::Upgrade if snap.reader_count == D::ONE => {
                    front.admit_waiting(&mut out, |_| true);
                }
                _ => {}
            }
        }
        out
    }

    fn run_pass_(&self) {
        if !self.stat_.waiters_present() {
            return;
        }
        let mut q = self.queue_.lock();
        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);
    }

    fn wake_wakers_(wakers: Vec<Waker>) {
        for w in wakers {
            w.wake();
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
    use core::{sync::atomic::AtomicUsize, task::Waker};

    use super::{AcqProgress, RwCoreArena, WaitKind};

    /// 验证写者持有期间读者的入队、放行与领取流程。
    ///
    /// Verifies enqueue, admission and claim of a reader while a writer holds
    /// the lock, exercising the intrusive queue end to end.
    ///
    /// - 手段：先取得写者快速路径，再让一次读获取进入 Arena 队列；释放写者后
    ///   用同一个 `PendingWait` 重试领取，最后释放读者。
    /// - 判断：读者第一次返回 `Waiting`，释放写者后重试返回 `true`；领取完成
    ///   后队列为空，读者计数也能正常归还。
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

    /// 验证取消写者等待会收回 `WRITER_QUEUED` 并回收节点。
    ///
    /// Verifies cancelling a queued writer clears `WRITER_QUEUED` and reclaims
    /// the arena node.
    ///
    /// - 手段：写者持有时让第二个写请求入队，然后直接取消它；释放原写者后再
    ///   尝试读快速路径。
    /// - 判断：取消后 `writer_queued` 为假、队列为空；原写者释放后读者快速
    ///   路径必须成功。
    #[test]
    fn cancelled_writer_releases_barrier_and_node() {
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
}
