//! Arena 版等待节点、槽位、唤醒批，以及节点/槽位共用的池元素。
//!
//! # 为什么不再使用 `Vec`
//!
//! 堆分配版用一个 `Vec<WaiterSlot>` 承载读类节点里的多个等待槽位。Arena 版的
//! 目标除 chunk 分配外不再发生任何运行时堆分配，因此这里把节点与槽位都做成
//! [`ArenaItem`]，放进同一个固定尺寸槽位的 [`Arena`](super::arena_::Arena)：
//!
//! - 一个读类节点通过 `SlotList` 侵入式串起自己的槽位；
//! - 槽位被取消/领取后只改状态，等节点整体回收时再还给 Arena；
//! - 唤醒批 `WakerBatch` 是栈上的固定数组，超过容量时分批解锁唤醒。
//!
//! 节点、槽位与槽位链都不再持有任何堆分配。

use core::{mem, ptr::NonNull, task::Waker};

use crate::rwlock::wait_::{SpinLock, WaitKind};

/// 一次锁外唤醒批最多携带多少个 waker。
///
/// 超过这个数量时，`pass` 会先放行这么多，解锁唤醒后再重新加锁继续；因此
/// 不需要为了"一次拿完整个读者批"而分配动态数组。
pub(super) const WAKER_BATCH: usize = 64;

/// 固定在栈上的唤醒批。
pub(super) struct WakerBatch {
    items_: [Option<Waker>; WAKER_BATCH],
    len_: usize,
}

impl WakerBatch {
    /// 建立空批。数组在栈上初始化，不涉及堆分配。
    pub(super) fn new() -> Self {
        WakerBatch {
            items_: core::array::from_fn(|_| Option::None),
            len_: 0,
        }
    }

    /// 批是否已满。
    pub(super) fn is_full(&self) -> bool {
        self.len_ == WAKER_BATCH
    }

    /// 放入一个 waker；调用方必须先用 [`is_full`](Self::is_full) 检查。
    pub(super) fn push(&mut self, waker: Waker) {
        debug_assert!(!self.is_full(), "WakerBatch::push: 批已满");
        self.items_[self.len_] = Option::Some(waker);
        self.len_ += 1;
    }

    /// 在锁外逐个唤醒并清空本批。
    pub(super) fn wake_all(mut self) {
        for item in &mut self.items_ {
            if let Some(waker) = item.take() {
                waker.wake();
            }
        }
    }
}

/// Arena 池中承载的元素：一个等待节点，或一个等待槽位。
///
/// 节点与槽位共用同一个 `Arena<ArenaItem>`，因此整个等待队列只会在 chunk
/// 不足时分配新的 chunk。
pub(super) enum ArenaItem {
    /// 一个等待节点。
    Node(ArenaWaitNode),
    /// 一个等待槽位。
    Slot(ArenaWaiterSlot),
}

impl ArenaItem {
    /// 按节点解释自己；内部不变量被破坏时 panic。
    pub(super) fn as_node(&self) -> &ArenaWaitNode {
        match self {
            ArenaItem::Node(node) => node,
            ArenaItem::Slot(_) => panic!("ArenaItem: 期望 Node，实际是 Slot"),
        }
    }

    /// 按节点可变解释自己；内部不变量被破坏时 panic。
    pub(super) fn as_node_mut(&mut self) -> &mut ArenaWaitNode {
        match self {
            ArenaItem::Node(node) => node,
            ArenaItem::Slot(_) => panic!("ArenaItem: 期望 Node，实际是 Slot"),
        }
    }

    /// 按槽位解释自己；内部不变量被破坏时 panic。
    pub(super) fn as_slot(&self) -> &ArenaWaiterSlot {
        match self {
            ArenaItem::Slot(slot) => slot,
            ArenaItem::Node(_) => panic!("ArenaItem: 期望 Slot，实际是 Node"),
        }
    }

    /// 按槽位可变解释自己；内部不变量被破坏时 panic。
    pub(super) fn as_slot_mut(&mut self) -> &mut ArenaWaiterSlot {
        match self {
            ArenaItem::Slot(slot) => slot,
            ArenaItem::Node(_) => panic!("ArenaItem: 期望 Slot，实际是 Node"),
        }
    }
}

// SAFETY: 节点与槽位里的裸指针只指向同一个 Arena 内的元素；所有链接与槽位状态
// 都在等待队列自旋锁及其节点槽位锁内访问。把 Arena 移到另一个线程时元素地址
// 不变，`Waker` 本身满足 `Send`。
unsafe impl Send for ArenaItem {}

/// 一个等待节点内的槽位链。
#[derive(Clone, Copy)]
pub(super) struct SlotList {
    /// 第一个槽位。
    pub(super) head_: Option<NonNull<ArenaItem>>,
    /// 最后一个槽位，用于 O(1) 追加。
    pub(super) tail_: Option<NonNull<ArenaItem>>,
}

impl SlotList {
    /// 空链。
    pub(super) const fn empty() -> Self {
        SlotList {
            head_: Option::None,
            tail_: Option::None,
        }
    }
}

/// 一个等待节点。
///
/// 读类节点通过 `slots_` 链承载多个同质等待者；独占节点只有一个槽位。
/// 槽位链由节点自己的自旋锁保护，队列锁先于节点锁获取，锁序不会成环。
pub(super) struct ArenaWaitNode {
    /// 是否为读类节点。
    pub(super) readish_: bool,
    /// 队列中的前一个节点。
    pub(super) prev_: Option<NonNull<ArenaItem>>,
    /// 队列中的后一个节点。
    pub(super) next_: Option<NonNull<ArenaItem>>,
    /// 节点内的槽位链。
    slots_: SpinLock<SlotList>,
}

impl ArenaWaitNode {
    /// 建立一个还没有槽位的新节点。
    pub(super) fn new(kind: WaitKind) -> Self {
        ArenaWaitNode {
            readish_: kind.is_readish(),
            prev_: Option::None,
            next_: Option::None,
            slots_: SpinLock::new(SlotList::empty()),
        }
    }

    /// 节点是否为读类节点。
    pub(super) fn is_readish(&self) -> bool {
        self.readish_
    }

    /// 把槽位追加到节点槽位链尾部。
    pub(super) fn push_slot(&self, slot: NonNull<ArenaItem>) {
        let mut list = self.slots_.lock();
        // SAFETY: `slot` 是本节点独占的 Arena 槽位元素。
        let slot_ref = unsafe { slot_mut(slot) };
        slot_ref.next_ = Option::None;
        match list.tail_ {
            // SAFETY: 旧 tail 是链中仍然存活的槽位。
            Some(tail) => unsafe { slot_mut(tail) }.next_ = Option::Some(slot),
            None => list.head_ = Option::Some(slot),
        }
        list.tail_ = Option::Some(slot);
    }

    /// 取走整个槽位链，节点随后可以回收。
    pub(super) fn take_slots(&self) -> SlotList {
        let mut list = self.slots_.lock();
        mem::replace(&mut *list, SlotList::empty())
    }

    /// 读取某个槽位的种类。
    pub(super) fn slot_kind(&self, slot: NonNull<ArenaItem>) -> WaitKind {
        // SAFETY: 调用方传入的是本节点链上的存活槽位；`kind_` 终生不变。
        unsafe { slot_ref(slot) }.kind_
    }

    /// 独占节点里那个槽位的种类。
    pub(super) fn solo_kind(&self) -> WaitKind {
        let list = self.slots_.lock();
        let Some(slot) = list.head_ else {
            debug_assert!(false, "独占节点没有槽位");
            return WaitKind::Write;
        };
        // SAFETY: 链上的槽位在节点存活期间一直有效。
        unsafe { slot_ref(slot) }.kind_
    }

    /// 记录（或刷新）槽位的 waker，并把它重新置回 `Waiting`。
    pub(super) fn set_waker(&self, slot: NonNull<ArenaItem>, waker: &Waker) {
        let _list = self.slots_.lock();
        // SAFETY: 调用方传入的是本节点链上的存活槽位。
        let slot = unsafe { slot_mut(slot) };
        slot.state_ = SlotState::Waiting;
        if slot
            .waker_
            .as_ref()
            .is_some_and(|existing| existing.will_wake(waker))
        {
            return;
        }
        slot.waker_ = Option::Some(waker.clone());
    }

    /// 标注槽位已成功领取许可。
    pub(super) fn mark_acquired(&self, slot: NonNull<ArenaItem>) {
        self.set_state_(slot, SlotState::Acquired);
    }

    /// 标注槽位已被取消。
    pub(super) fn mark_cancelled(&self, slot: NonNull<ArenaItem>) {
        self.set_state_(slot, SlotState::Cancelled);
    }

    fn set_state_(&self, slot: NonNull<ArenaItem>, state: SlotState) {
        let _list = self.slots_.lock();
        // SAFETY: 调用方传入的是本节点链上的存活槽位。
        let slot = unsafe { slot_mut(slot) };
        debug_assert!(slot.state_.is_pending());
        slot.state_ = state;
        slot.waker_ = Option::None;
    }

    /// 仍未得出结果（`Waiting` 或 `Admitted`）的槽位数量。
    pub(super) fn pending_count(&self) -> usize {
        let list = self.slots_.lock();
        let mut count = 0;
        let mut cur = list.head_;
        while let Some(item) = cur {
            // SAFETY: 链上的槽位在节点存活期间一直有效。
            let slot = unsafe { slot_ref(item) };
            if slot.state_.is_pending() {
                count += 1;
            }
            cur = slot.next_;
        }
        count
    }

    /// 放行读类节点里可放行的槽位，把 waker 收进栈上批次。
    ///
    /// 返回 `true` 表示批次已满、可能还有槽位需要下一次 `pass` 继续放行。
    ///
    /// `upgrade_active` 是本次快照里的标志；函数还会检查本节点中是否已经存在
    /// `Admitted` / `Acquired` 的可升级读者，避免跨批放行出第二个可升级读者。
    pub(super) fn admit_readish(&self, out: &mut WakerBatch, upgrade_active: bool) -> bool {
        let list = self.slots_.lock();
        let mut upgrade_taken = upgrade_active;

        // 先检查本节点中是否已经有已放行/已领取的可升级读者。
        let mut cur = list.head_;
        while let Some(item) = cur {
            // SAFETY: 链上的槽位在节点存活期间一直有效。
            let slot = unsafe { slot_ref(item) };
            // 只有 `Admitted` 需要跨批记忆：它已经被放行但还没 poll/领取，
            // 此时状态字里的 `UPGRADE_ACTIVE` 还不会置位。`Acquired` 则由
            // `snap.upgrade_active` 负责；读者释放后它可以重新被放行。
            if slot.kind_ == WaitKind::UpgradableRead && slot.state_ == SlotState::Admitted {
                upgrade_taken = true;
                break;
            }
            cur = slot.next_;
        }

        cur = list.head_;
        while let Some(item) = cur {
            if out.is_full() {
                return true;
            }
            // SAFETY: 链上的槽位在节点存活期间一直有效。
            let next = unsafe { slot_ref(item) }.next_;
            // SAFETY: 同上，且节点槽位锁已持有，可以独占修改。
            let slot = unsafe { slot_mut(item) };
            let allowed = match slot.kind_ {
                WaitKind::Read => true,
                WaitKind::UpgradableRead if !upgrade_taken => true,
                _ => false,
            };
            if allowed
                && slot.state_ == SlotState::Waiting
                && let Some(waker) = slot.waker_.take()
            {
                slot.state_ = SlotState::Admitted;
                if slot.kind_ == WaitKind::UpgradableRead {
                    upgrade_taken = true;
                }
                out.push(waker);
            }
            cur = next;
        }
        false
    }

    /// 放行独占节点的槽位。
    pub(super) fn admit_exclusive(&self, out: &mut WakerBatch) -> bool {
        let list = self.slots_.lock();
        let Some(item) = list.head_ else {
            debug_assert!(false, "独占节点没有槽位");
            return false;
        };
        if out.is_full() {
            return true;
        }
        // SAFETY: 链上的槽位在节点存活期间一直有效。
        let slot = unsafe { slot_mut(item) };
        if slot.state_ == SlotState::Waiting
            && let Some(waker) = slot.waker_.take()
        {
            slot.state_ = SlotState::Admitted;
            out.push(waker);
        }
        false
    }
}

/// 一个等待槽位。
pub(super) struct ArenaWaiterSlot {
    /// 同节点内的下一个槽位。
    pub(super) next_: Option<NonNull<ArenaItem>>,
    /// 等待者种类。
    pub(super) kind_: WaitKind,
    /// 已注册的 waker。
    pub(super) waker_: Option<Waker>,
    /// 当前状态。
    pub(super) state_: SlotState,
}

impl ArenaWaiterSlot {
    /// 建立一个 `Waiting` 槽位。
    pub(super) fn new(kind: WaitKind) -> Self {
        ArenaWaiterSlot {
            next_: Option::None,
            kind_: kind,
            waker_: Option::None,
            state_: SlotState::Waiting,
        }
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
}

impl SlotState {
    fn is_pending(self) -> bool {
        matches!(self, SlotState::Waiting | SlotState::Admitted)
    }
}

/// 按槽位解释 Arena 元素。
///
/// # Safety
///
/// `item` 必须指向一个仍存活的 `ArenaItem::Slot`。
unsafe fn slot_ref<'a>(item: NonNull<ArenaItem>) -> &'a ArenaWaiterSlot {
    // SAFETY: 由调用方保证元素存活且是 Slot 变体。
    unsafe { item.as_ref() }.as_slot()
}

/// 按槽位可变解释 Arena 元素。
///
/// # Safety
///
/// `item` 必须指向一个仍存活、且当前没有其他引用的 `ArenaItem::Slot`。
unsafe fn slot_mut<'a>(item: NonNull<ArenaItem>) -> &'a mut ArenaWaiterSlot {
    // SAFETY: 由调用方保证元素存活、独占且是 Slot 变体。
    unsafe { item.as_ptr().as_mut() }
        .expect("ArenaItem: 空槽位指针")
        .as_slot_mut()
}
