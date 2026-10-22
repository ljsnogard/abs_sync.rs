//! Arena 版等待节点、槽位，以及节点/槽位共用的池元素。
//!
//! # 为什么不再使用 `Vec`
//!
//! 堆分配版用一个 `Vec<WaiterSlot>` 承载读类节点里的多个等待槽位。Arena 版的
//! 目标除 chunk 分配外不再发生任何运行时堆分配，因此这里把节点与槽位都做成
//! [`ArenaItem`]，放进同一个固定尺寸槽位的 [`Arena`](super::arena_::Arena)：
//!
//! - 一个读类节点通过 `SlotList` 侵入式串起自己的槽位；
//! - 槽位被取消/领取后只改状态，等节点整体回收时再还给 Arena；
//! - 节点被选中放行后会 `sealed_`，之后新读者不再合并进这个节点；
//! - 唤醒由 `waking_` 标记的单一执行者完成，槽位锁只保护单个槽位的状态。

use core::{
    mem,
    ptr::NonNull,
    sync::atomic::{AtomicBool, Ordering},
    task::Waker,
};

use crate::rwlock::wait_::{SpinLock, WaitKind};

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
// 都在等待队列自旋锁、节点槽位锁内访问。把 Arena 移到另一个线程时元素地址
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
    /// 是否已经被选中放行过；一旦置位，新读者不再合并进本节点。
    pub(super) sealed_: bool,
    /// 是否有唤醒者正在处理本节点；队列锁下读写，唤醒期间阻止节点被回收。
    pub(super) waking_: bool,
    /// 是否已有线程认领"最后一个 pending 槽位完成后的收尾"。
    ///
    /// 由节点锁下的槽位完成路径 CAS 置位，由队列锁下的收尾者清除；`prune`
    /// 看到它时必须跳过，否则会在完成者还在访问节点时把节点回收。
    finalizing_: AtomicBool,
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
            sealed_: false,
            waking_: false,
            finalizing_: AtomicBool::new(false),
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

    /// 槽位链的第一个元素。
    pub(super) fn slot_head_(&self) -> Option<NonNull<ArenaItem>> {
        self.slots_.lock().head_
    }

    /// 槽位链中某个槽位的后继。
    ///
    /// 槽位链只追加、不摘除；调用期间节点由 `waking_` 或 pending 槽位保证
    /// 存活，因此可以在不持节点锁的情况下读链接。
    pub(super) fn slot_next_(&self, slot: NonNull<ArenaItem>) -> Option<NonNull<ArenaItem>> {
        // SAFETY: 调用方保证 `slot` 是本节点链上的存活槽位。
        unsafe { slot_ref(slot) }.next_
    }

    /// 读取某个槽位的当前状态。
    pub(super) fn slot_state_(&self, slot: NonNull<ArenaItem>) -> SlotState {
        let _list = self.slots_.lock();
        // SAFETY: 调用方传入的是本节点链上的存活槽位。
        unsafe { slot_ref(slot) }.state_
    }

    /// 节点中是否已经有 `Admitted` 的可升级读槽位。
    pub(super) fn has_admitted_upgradable_(&self) -> bool {
        let list = self.slots_.lock();
        let mut cur = list.head_;
        while let Some(item) = cur {
            // SAFETY: 链上的槽位在节点存活期间一直有效。
            let slot = unsafe { slot_ref(item) };
            if slot.kind_ == WaitKind::UpgradableRead && slot.state_ == SlotState::Admitted {
                return true;
            }
            cur = slot.next_;
        }
        false
    }

    /// 尝试放行一个读类槽位；成功时返回需要唤醒的 waker。
    ///
    /// `upgrade_taken` 在本次唤醒循环内保持，保证至多放行一个可升级读。
    pub(super) fn try_admit_readish_slot_(
        &self,
        slot: NonNull<ArenaItem>,
        upgrade_taken: &mut bool,
        upgrade_active: bool,
    ) -> Option<Waker> {
        let _list = self.slots_.lock();
        // SAFETY: 调用方保证 `slot` 是本节点链上的存活槽位。
        let slot = unsafe { slot_mut(slot) };
        if slot.state_ == SlotState::Admitted && slot.kind_ == WaitKind::UpgradableRead {
            *upgrade_taken = true;
            return Option::None;
        }
        if slot.state_ != SlotState::Waiting {
            return Option::None;
        }
        let allowed = match slot.kind_ {
            WaitKind::Read => true,
            WaitKind::UpgradableRead => !*upgrade_taken && !upgrade_active,
            _ => false,
        };
        if !allowed {
            return Option::None;
        }
        let waker = slot.waker_.take()?;
        slot.state_ = SlotState::Admitted;
        if slot.kind_ == WaitKind::UpgradableRead {
            *upgrade_taken = true;
        }
        Option::Some(waker)
    }

    /// 尝试放行独占节点里的唯一槽位。
    pub(super) fn try_admit_exclusive_slot_(&self, slot: NonNull<ArenaItem>) -> Option<Waker> {
        let _list = self.slots_.lock();
        // SAFETY: 调用方保证 `slot` 是本节点链上的存活槽位。
        let slot = unsafe { slot_mut(slot) };
        if slot.state_ != SlotState::Waiting {
            return Option::None;
        }
        let waker = slot.waker_.take()?;
        slot.state_ = SlotState::Admitted;
        Option::Some(waker)
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

    /// 标注槽位已成功领取许可；返回本线程是否认领了节点收尾。
    pub(super) fn mark_acquired(&self, slot: NonNull<ArenaItem>) -> bool {
        self.finish_slot_(slot, SlotState::Acquired)
    }

    /// 标注槽位已被取消；返回本线程是否认领了节点收尾。
    pub(super) fn mark_cancelled(&self, slot: NonNull<ArenaItem>) -> bool {
        self.finish_slot_(slot, SlotState::Cancelled)
    }

    fn finish_slot_(&self, slot: NonNull<ArenaItem>, state: SlotState) -> bool {
        let list = self.slots_.lock();
        // SAFETY: 调用方传入的是本节点链上的存活槽位。
        let slot = unsafe { slot_mut(slot) };
        debug_assert!(slot.state_.is_pending());
        slot.state_ = state;
        slot.waker_ = Option::None;
        let pending = count_pending_(&list);
        pending == 0 && !self.finalizing_.swap(true, Ordering::AcqRel)
    }

    /// 节点收尾者是否已经认领。
    pub(super) fn is_finalizing_(&self) -> bool {
        self.finalizing_.load(Ordering::Acquire)
    }

    /// 收尾者在队列锁内清除认领标记。
    pub(super) fn clear_finalizing_(&self) {
        self.finalizing_.store(false, Ordering::Release);
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

fn count_pending_(list: &SlotList) -> usize {
    let mut count = 0;
    let mut cur = list.head_;
    while let Some(item) = cur {
        // SAFETY: 链表中的槽位在节点存活期间一直有效。
        let slot = unsafe { slot_ref(item) };
        if slot.state_.is_pending() {
            count += 1;
        }
        cur = slot.next_;
    }
    count
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
    // SAFETY: 由调用方保证元素存活、独占且是 Slot 变体；NonNull 保证非空。
    unsafe { &mut *item.as_ptr() }.as_slot_mut()
}
