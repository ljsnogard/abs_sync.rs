//! 协作式读写锁的共享内核：状态字 + 等待队列 + 放行流程。
//!
//! # 并发模型
//!
//! - **状态字**（`CoopRwState`）是无锁的，快速路径直接在其上做比较交换；
//! - **等待队列**（`WaitQueue`）由一把自旋锁保护。凡是需要判断"我是不是
//!   队首 / 谁该被放行"的地方都必须持有这把锁；
//! - 任何自旋锁内**只允许**做内存操作与收集 waker，**绝不允许**调用
//!   `Waker::wake`，否则在单线程执行器上会自死锁
//!   （见 `dev-notes/rwlock-20260923-2204.md` §4.6）。
//!
//! # 升级请求为什么插队
//!
//! 可升级读者在升级期间仍然占着一个读名额，而排队的写者要等
//! `READER_COUNT == 0`。若把升级请求排在写者后面，两者互等：
//!
//! ```text
//! [写 W][升级 U] → W 等 R==0（U 占着名额），U 等队首（被 W 挡住）→ 死锁
//! ```
//!
//! 因此升级请求直接插到**队首**：可升级读者此刻是持有者，它的"位置"本来
//! 就在所有排队者之前，升级请求跟着它一起排在前面才是正确顺序。
//! 插队之后该槽位计入 `WRITER_QUEUED`，`WRITER_QUEUED` 自然挡住新读者，
//! 所以升级既不会死锁，也不会被新读者无限推迟。

use alloc::{collections::VecDeque, sync::Arc, vec::Vec};
use core::{borrow::BorrowMut, task::Waker};

use funty::Unsigned;

use atomex::{x_deps::funty, StrictOrderings, TrAtomicData, TrCmpxchOrderings};

use super::{
    state_::CoopRwState,
    wait_::{SpinLock, WaitKind, WaitNode, WaitQueue},
};

/// 一次获取动作的当前进展。
pub(super) enum AcqProgress {
    /// 已经拿到许可。
    Acquired,
    /// 仍在队列里等待；future 需要记住这两个值以便重试与取消。
    Waiting {
        node_: Arc<WaitNode>,
        slot_: usize,
    },
}

/// 读写锁的共享内核。
///
/// 它不承载被保护的资源 `T`（资源内联在 `CooperativeRwLock` 外壳里），
/// 因此与 `T` 的类型和大小都无关，可以稳定地放在 `Arc` 里共享。
pub(super) struct RwCore<D, B, O = StrictOrderings>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    stat_: CoopRwState<D, B, O>,
    queue_: SpinLock<QueuePayload>,
}

/// 队列自旋锁保护的载荷：FIFO 等待队列 + 队列中独占等待者的计数。
///
/// `writer_waiters_` 记录队列中仍处于待决状态（`Waiting` 或 `Admitted`）的
/// `Write` / `Upgrade` 槽位数，它是状态字里 `WRITER_QUEUED` 位的计数来源。
/// 之所以必须精确到槽位、而不能用"队列里有没有独占节点"来推导：被取消的独占
/// 节点可能长期留在队列中段（`prune_front_` 只剪队首），已放行但尚未领取的槽位
/// 也仍然算在等。
///
/// 它与队列共用同一把锁，因此只在持有队列锁时读写。`RwCore` 共享在 `Arc` 里，
/// 所有入口只拿得到 `&self`，把计数器放进锁内载荷是它唯一不需要 `unsafe` 的
/// 可变访问路径。
struct QueuePayload {
    nodes_: WaitQueue,
    writer_waiters_: usize,
}

impl<D, B, O> RwCore<D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) const fn new(cell: B) -> Self {
        RwCore {
            stat_: CoopRwState::new(cell),
            queue_: SpinLock::new(QueuePayload {
                nodes_: VecDeque::new(),
                writer_waiters_: 0,
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
    // 快速路径：无写者、且队列里没有写者/升级等待者时无需分配、无需加锁
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

    /// 同步（不排队）地尝试升级；供 `try_upgrade` 使用。
    ///
    /// **不能**在持有队列锁时调用它；它只改状态字，不需要放行别人。
    #[inline]
    pub(super) fn try_upgrade_now(&self) -> bool {
        self.stat_.try_upgrade_to_write()
    }

    /// 按等待者种类调用对应的"已被放行"领取动作。
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
    // 获取：入队 / 领取 / 注册 waker 必须在同一段队列锁临界区内完成
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 进入等待队列并尝试领取许可。
    ///
    /// 三步（入队、尝试领取、注册 waker）在同一段临界区内完成，这是避免
    /// "丢唤醒"的关键：临界区之外没有任何时刻能让别人替我跑 `pass()`。
    pub(super) fn acquire(&self, kind: WaitKind, waker: &Waker) -> AcqProgress {
        let mut q = self.queue_.lock();
        self.prune_front_(&mut q);
        let (node, slot) = self.enqueue_(&mut q, kind);
        // 队列由空变非空只需置位一次；已置位时省掉一次原子读改写。
        if !self.stat_.waiters_present() {
            self.stat_.mark_waiters_present();
        }

        let claimed = self.is_head_(&q, &node) && self.try_claim_(kind);
        if claimed {
            node.mark_acquired(slot);
            self.note_exclusive_slot_finished_(&mut q, &node);
        } else {
            node.set_waker(slot, waker);
        }
        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);

        if claimed {
            AcqProgress::Acquired
        } else {
            AcqProgress::Waiting {
                node_: node,
                slot_: slot,
            }
        }
    }

    /// 已被唤醒的等待者重试领取。
    ///
    /// 返回 `true` 表示拿到许可；否则刷新 waker 并继续等待。
    pub(super) fn poll_acquire(
        &self,
        node: &Arc<WaitNode>,
        slot: usize,
        waker: &Waker,
    ) -> bool {
        let mut q = self.queue_.lock();
        self.prune_front_(&mut q);

        let claimed =
            self.is_head_(&q, node) && self.try_claim_(node.slot_kind(slot));
        if claimed {
            node.mark_acquired(slot);
            self.note_exclusive_slot_finished_(&mut q, node);
        } else {
            node.set_waker(slot, waker);
        }
        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);
        claimed
    }

    /// 等待者被取消（future 被丢弃）。
    ///
    /// 这一步是**活性所必需**的：队首的等待者被唤醒后立刻取消时，必须由它
    /// 自己重跑一次放行流程，否则排在它后面的等待者将永远无人唤醒。
    ///
    /// 对写者/升级槽位还多一层意义：取消必须把该槽位从 `writer_waiters_` 里
    /// 扣除并收回 `WRITER_QUEUED`，否则后续读者会被一个已经不存在的等待者
    /// 永久挡在快速路径之外。
    pub(super) fn cancel_wait(&self, node: &Arc<WaitNode>, slot: usize) {
        let mut q = self.queue_.lock();
        node.mark_cancelled(slot);
        self.note_exclusive_slot_finished_(&mut q, node);
        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);
    }

    fn enqueue_(
        &self,
        q: &mut QueuePayload,
        kind: WaitKind,
    ) -> (Arc<WaitNode>, usize) {
        // 读类等待者与队尾的读类节点合并；只与队尾合并，保证 FIFO 次序不被破坏。
        if kind.is_readish()
            && let Some(tail) = q.nodes_.back()
            && tail.is_readish()
        {
            let slot = tail.push_readish_slot(kind);
            return (tail.clone(), slot);
        }
        let (node, slot) = WaitNode::new(kind);
        let node = Arc::new(node);
        match kind {
            // 升级请求插队，理由见本文件头部说明。
            WaitKind::Upgrade => {
                q.nodes_.push_front(node.clone());
                self.note_exclusive_slot_queued_(q);
            }
            WaitKind::Write => {
                q.nodes_.push_back(node.clone());
                self.note_exclusive_slot_queued_(q);
            }
            // 读类等待者新建的读类节点不构成写者屏障。
            _ => q.nodes_.push_back(node.clone()),
        }
        (node, slot)
    }

    /// 独占（`Write` / `Upgrade`）槽位入队：计数加一，并在由 0 变 1 时置位。
    fn note_exclusive_slot_queued_(&self, q: &mut QueuePayload) {
        q.writer_waiters_ += 1;
        if q.writer_waiters_ == 1 {
            self.stat_.mark_writer_queued();
        }
    }

    /// 独占槽位离开待决状态（已领取或已取消）：计数减一，归零时清位。
    ///
    /// 读类槽位直接返回。判定用 `is_readish()` 而不是重新取槽位种类，是为了
    /// 避免在已持有队列锁的前提下再去取节点锁。
    fn note_exclusive_slot_finished_(
        &self,
        q: &mut QueuePayload,
        node: &WaitNode,
    ) {
        if node.is_readish() {
            return;
        }
        debug_assert!(q.writer_waiters_ > 0, "writer_waiters_ underflow");
        q.writer_waiters_ = q.writer_waiters_.saturating_sub(1);
        if q.writer_waiters_ == 0 {
            self.stat_.clear_writer_queued();
        }
    }

    fn is_head_(&self, q: &QueuePayload, node: &Arc<WaitNode>) -> bool {
        q.nodes_.front().is_some_and(|f| Arc::ptr_eq(f, node))
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 放行流程
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 剪掉队首已经"没有任何在等的人"的节点。
    fn prune_front_(&self, q: &mut QueuePayload) {
        while q.nodes_.front().is_some_and(|f| f.pending_count() == 0) {
            q.nodes_.pop_front();
        }
    }

    /// 决定谁可以被唤醒。
    ///
    /// `prune_front_` 之后队首一定还有 `Waiting` 槽位，而队首节点在它自己被
    /// 领取完（或取消完）之前始终是后面等待者的屏障——所以这里**只看队首**：
    ///
    /// - 读类节点：写者未持有时放行其中的读者；其中"可升级读者"还要求当前
    ///   没有别的可升级读者（全锁至多一个）；
    /// - 独占节点：写者要 `READER_COUNT == 0` 且无可升级读者；
    ///   升级要 `READER_COUNT == 1`（只剩它自己）。
    ///
    /// 这里**只负责唤醒**，不预先分配任何许可：真正的领取始终由等待者在
    /// `poll` 里自己比较交换完成，因此"被唤醒后立刻取消"不会泄漏许可。
    fn pass_(&self, q: &mut QueuePayload) -> Vec<Waker> {
        self.prune_front_(q);
        if q.nodes_.is_empty() {
            self.stat_.clear_waiters_present();
            // 防御性：队列已空，就不应再有待决的独占等待者。若这里确实非零，
            // 说明计数在某条路径上泄漏了；必须清位，否则读者会被永久挡住。
            if q.writer_waiters_ != 0 {
                debug_assert!(false, "writer_waiters_ leaked while queue is empty");
                q.writer_waiters_ = 0;
                self.stat_.clear_writer_queued();
            }
            return Vec::new();
        }

        let snap = self.stat_.snapshot();
        // `WRITER_QUEUED` 与计数器都只在队列锁内维护，此处必须一致；
        // 不一致就说明计数在某条路径上漏加或漏减。
        debug_assert_eq!(
            snap.writer_queued,
            q.writer_waiters_ > 0,
            "WRITER_QUEUED 与 writer_waiters_ 不一致"
        );
        if snap.writer_active {
            return Vec::new();
        }

        let Some(front) = q.nodes_.front() else {
            return Vec::new();
        };
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
                WaitKind::Write
                    if snap.reader_count == D::ZERO && !snap.upgrade_active =>
                {
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

    /// 跑一次放行流程，并在**释放队列锁之后**唤醒收集到的 waker。
    ///
    /// 先用一次普通原子读避开"没人排队"的常见情形：`WAITERS_PRESENT` 只在持有
    /// 队列锁时置位/清除，因此它为空就说明队列为空，不必去抢队列锁。
    /// 竞态是无害的：若恰有等待者在我们检查之后入队，它自己的 `acquire`
    /// 会带着刚更新的状态再跑一次放行流程。
    fn run_pass_(&self) {
        if !self.stat_.waiters_present() {
            return;
        }
        let mut q = self.queue_.lock();
        let wakers = self.pass_(&mut q);
        drop(q);
        Self::wake_wakers_(wakers);
    }

    /// 唤醒收集到的 waker。
    ///
    /// 这里**不**过滤"自己的 waker"。曾经为了让入队者少一次自我唤醒而加过
    /// `will_wake` 过滤，但配合"槽位放行后转入 `Admitted` 且交出 waker"的
    /// 一次性放行语义，过滤会把这个槽位**唯一**的唤醒机会吞掉：
    /// 本线程在临界区内看到条件成立而放行了自己，却因为"目的就是自己"
    /// 而不唤醒，从此没有任何人再唤醒它 → 永久挂起。
    /// 多一次自我唤醒最多多一次 poll，代价远小于丢失唤醒。
    fn wake_wakers_(wakers: Vec<Waker>) {
        for w in wakers {
            w.wake();
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 释放 / 降级
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn release_reader(&self) {
        // 注意：状态迁移**必须**无条件执行；把它写进 `debug_assert!` 里
        // 会让 release 构建直接跳过迁移（这里曾经真的踩过这个坑）。
        let _ok = self.stat_.release_reader();
        debug_assert!(_ok, "state transition `release_reader` failed");
        self.run_pass_();
    }

    pub(super) fn release_upgradable_read(&self) {
        // 注意：状态迁移**必须**无条件执行；把它写进 `debug_assert!` 里
        // 会让 release 构建直接跳过迁移（这里曾经真的踩过这个坑）。
        let _ok = self.stat_.release_upgradable_read();
        debug_assert!(_ok, "state transition `release_upgradable_read` failed");
        self.run_pass_();
    }

    pub(super) fn release_writer(&self) {
        // 注意：状态迁移**必须**无条件执行；把它写进 `debug_assert!` 里
        // 会让 release 构建直接跳过迁移（这里曾经真的踩过这个坑）。
        let _ok = self.stat_.release_writer();
        debug_assert!(_ok, "state transition `release_writer` failed");
        self.run_pass_();
    }

    pub(super) fn downgrade_write_to_read(&self) {
        // 注意：状态迁移**必须**无条件执行；把它写进 `debug_assert!` 里
        // 会让 release 构建直接跳过迁移（这里曾经真的踩过这个坑）。
        let _ok = self.stat_.downgrade_write_to_read();
        debug_assert!(_ok, "state transition `downgrade_write_to_read` failed");
        self.run_pass_();
    }

    pub(super) fn downgrade_write_to_upgradable(&self) {
        // 注意：状态迁移**必须**无条件执行；把它写进 `debug_assert!` 里
        // 会让 release 构建直接跳过迁移（这里曾经真的踩过这个坑）。
        let _ok = self.stat_.downgrade_write_to_upgradable();
        debug_assert!(_ok, "state transition `downgrade_write_to_upgradable` failed");
        self.run_pass_();
    }

    pub(super) fn downgrade_upgradable_to_read(&self) {
        // 注意：状态迁移**必须**无条件执行；把它写进 `debug_assert!` 里
        // 会让 release 构建直接跳过迁移（这里曾经真的踩过这个坑）。
        let _ok = self.stat_.downgrade_upgradable_to_read();
        debug_assert!(_ok, "state transition `downgrade_upgradable_to_read` failed");
        self.run_pass_();
    }
}
