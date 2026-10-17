//! 等待队列的底层构件：自旋锁、等待节点与队列。

use alloc::{collections::VecDeque, sync::Arc, vec::Vec};
use core::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
    task::Waker,
};

/// 一个极简的自旋锁。
///
/// 它只服务于本模块内部的等待队列与等待节点，临界区里**绝不允许**出现
/// 任何可能让出执行权的操作（尤其是调用 [`Waker::wake`]），否则会在
/// 单线程执行器上自死锁。
pub(super) struct SpinLock<T> {
    locked_: AtomicUsize,
    data_: UnsafeCell<T>,
}

// SAFETY: `data_` 只在持有锁（`locked_` 由 1 表示）时经 `SpinLockGuard`
// 访问，而同一时刻至多只有一个执行流能取得锁，故共享访问必然是串行的；
// `T: Send` 保证临界区数据可以跨执行流移交。
unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    pub(super) const fn new(data: T) -> Self {
        SpinLock {
            locked_: AtomicUsize::new(0),
            data_: UnsafeCell::new(data),
        }
    }

    /// 自旋直到取得锁。
    pub(super) fn lock(&self) -> SpinLockGuard<'_, T> {
        while self
            .locked_
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        SpinLockGuard { lock_: self }
    }

    fn unlock_(&self) {
        self.locked_.store(0, Ordering::Release);
    }
}

/// [`SpinLock`] 的 RAII 守卫；析构即解锁。
pub(super) struct SpinLockGuard<'a, T> {
    lock_: &'a SpinLock<T>,
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: 构造 `SpinLockGuard` 的前提是已持有锁，且在守卫析构前
        // 不会释放，因此这里是该临界区的唯一访问路径。
        unsafe { &*self.lock_.data_.get() }
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: 同 `deref`。
        unsafe { &mut *self.lock_.data_.get() }
    }
}

impl<T> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock_.unlock_()
    }
}

/// 等待者的种类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WaitKind {
    /// 想要获得普通读许可。
    Read,
    /// 想要获得可升级读许可（此时 `UPGRADE_ACTIVE` 尚未置位）。
    UpgradableRead,
    /// 已持有可升级读许可，想要升级为写者。
    Upgrade,
    /// 想要获得写许可。
    Write,
}

impl WaitKind {
    /// 是否可以与其它读类等待者共处同一个节点。
    pub(super) fn is_readish(self) -> bool {
        matches!(self, WaitKind::Read | WaitKind::UpgradableRead)
    }
}

/// 单个槽位的状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotState {
    /// 仍在等待放行。
    Waiting,
    /// 已被放行（waker 已交出），但还没被 poll 到、也就还没领取许可。
    ///
    /// 这个状态是"不重复唤醒"的关键：`pass_` 只放行 `Waiting` 槽位，
    /// 一个槽位在被真正 poll（领取成功或失败后重新挂起）之前不会再被唤醒。
    Admitted,
    /// 已被放行并成功领取许可。
    Acquired,
    /// 等待者被取消（future 被丢弃）。
    Cancelled,
}

impl SlotState {
    /// 是否仍占用排队位置（尚未有最终结果）。
    fn is_pending(self) -> bool {
        matches!(self, SlotState::Waiting | SlotState::Admitted)
    }
}

struct WaiterSlot {
    kind_: WaitKind,
    waker_: Option<Waker>,
    state_: SlotState,
}

/// 一个等待节点。
///
/// 两种形态：
///
/// - **读类节点**（`Read` / `UpgradableRead`）：一个节点承载若干同质等待者，
///   它们共享同一个排队位置，`pass()` 一次就能放行整组；单个等待者的取消
///   只影响它自己的槽位，不波及同组其他人；
/// - **独占节点**（`Write` / `Upgrade`）：只承载一个槽位。
///
/// 节点槽位由自身的自旋锁保护；调用方（`core_`）总是**先持有队列锁、再取
/// 节点锁**，锁序固定为 `队列锁 → 节点锁`，不会成环。
pub(super) struct WaitNode {
    readish_: bool,
    slots_: SpinLock<Vec<WaiterSlot>>,
}

fn push_slot_into_(slots: &mut Vec<WaiterSlot>, kind: WaitKind) -> usize {
    slots.push(WaiterSlot {
        kind_: kind,
        waker_: None,
        state_: SlotState::Waiting,
    });
    slots.len() - 1
}

impl WaitNode {
    /// 建立只带一个槽位的新节点，返回 (节点, 槽位下标)。
    pub(super) fn new(kind: WaitKind) -> (Self, usize) {
        let node = WaitNode {
            readish_: kind.is_readish(),
            slots_: SpinLock::new(Vec::new()),
        };
        node.slots_.lock().push(WaiterSlot {
            kind_: kind,
            waker_: None,
            state_: SlotState::Waiting,
        });
        (node, 0)
    }

    pub(super) fn is_readish(&self) -> bool {
        self.readish_
    }

    /// 指定槽位的种类。
    pub(super) fn slot_kind(&self, slot: usize) -> WaitKind {
        self.slots_
            .lock()
            .get(slot)
            .map(|s| s.kind_)
            .unwrap_or_else(|| {
                debug_assert!(false, "slot index out of range");
                WaitKind::Read
            })
    }

    /// 独占节点里那个槽位的种类。
    pub(super) fn solo_kind(&self) -> WaitKind {
        debug_assert!(!self.readish_);
        self.slot_kind(0)
    }

    /// 追加一个读类槽位；调用者必须持有队列锁，且节点必须是读类节点。
    pub(super) fn push_readish_slot(&self, kind: WaitKind) -> usize {
        debug_assert!(self.readish_ && kind.is_readish());
        let mut slots = self.slots_.lock();
        push_slot_into_(&mut slots, kind)
    }

    /// 记录（或刷新）槽位的 waker，并把它重新置回 `Waiting`。
    ///
    /// 被唤醒却没能领到许可的等待者会走这里：它必须回到 `Waiting`，
    /// 下一次 `pass_` 才会再次放行它。
    pub(super) fn set_waker(&self, slot: usize, waker: &Waker) {
        let mut slots = self.slots_.lock();
        let Some(entry) = slots.get_mut(slot) else {
            debug_assert!(false, "slot index out of range");
            return;
        };
        entry.state_ = SlotState::Waiting;
        if entry.waker_.as_ref().is_some_and(|w| w.will_wake(waker)) {
            return;
        }
        entry.waker_ = Some(waker.clone());
    }

    /// 标注该槽位已成功领取许可。
    pub(super) fn mark_acquired(&self, slot: usize) {
        self.set_state_(slot, SlotState::Acquired);
    }

    /// 标注该槽位已被取消。
    pub(super) fn mark_cancelled(&self, slot: usize) {
        self.set_state_(slot, SlotState::Cancelled);
    }

    fn set_state_(&self, slot: usize, state: SlotState) {
        let mut slots = self.slots_.lock();
        let Some(entry) = slots.get_mut(slot) else {
            debug_assert!(false, "slot index out of range");
            return;
        };
        debug_assert!(entry.state_.is_pending());
        entry.state_ = state;
        // 一旦离开 `Waiting`，waker 就没有保留价值了。
        entry.waker_ = None;
    }

    /// 仍未得出结果（`Waiting` 或 `Admitted`）的槽位数量。
    ///
    /// 该值为 0 时，节点的排队位置可以被回收（`pass()` 会把它从队首剪掉）。
    pub(super) fn pending_count(&self) -> usize {
        self.slots_
            .lock()
            .iter()
            .filter(|s| s.state_.is_pending())
            .count()
    }

    /// 放行本节点中"仍在 `Waiting` 且 `allow(kind)` 成立"的槽位。
    ///
    /// 被放行的槽位转入 `Admitted` 并**交出** waker，因此同一个槽位在它自己
    /// 重新挂起（`set_waker`）之前不会再被放行——这正是避免"每次 `pass_` 都
    /// 把同一批等待者重新唤醒一遍"的关键。反复唤醒会让每次操作付出多次
    /// 跨线程调度，是 8 线程下性能崩塌的主因。
    pub(super) fn admit_waiting(
        &self,
        out: &mut Vec<Waker>,
        mut allow: impl FnMut(WaitKind) -> bool,
    ) {
        let mut slots = self.slots_.lock();
        for s in slots.iter_mut() {
            if s.state_ == SlotState::Waiting
                && allow(s.kind_)
                && let Some(w) = s.waker_.take()
            {
                s.state_ = SlotState::Admitted;
                out.push(w);
            }
        }
    }
}

/// 等待队列：队首即最先到达的等待者。
pub(super) type WaitQueue = VecDeque<Arc<WaitNode>>;
