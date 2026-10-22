//! Arena 的定长 chunk、槽位布局与空闲链。
//!
//! # 布局不变量
//!
//! - [`Slot`] 使用 `#[repr(C)]`，且 `value_` 是首字段：因此"值指针"与"槽位指针"
//!   地址相同，回收时可以从 `NonNull<T>` 直接还原出槽位。
//! - [`Chunk`] 使用 `#[repr(C)]`，槽位数组 `slots_` 紧跟在头部之后；槽位自身的
//!   `self_idx_` 加固定偏移可以**反推所属 chunk**，因此池不需要在槽位里再存
//!   chunk 指针，也不需要沿 chunk 链查找。
//! - 每个 chunk 固定 [`CHUNK_CAPACITY`] 个槽位，索引用 `u16` 足够。

use core::{
    alloc::Allocator,
    marker::PhantomData,
    mem::{self, MaybeUninit},
    ptr::{self, NonNull},
};

/// 每个 chunk 固定的槽位数量。
///
/// 1024 个槽位足以吸收一次较大的等待者突发，同时把 `u16` 索引留出充足余量。
/// 所有池共用这个常量：`Arena<T, A>` 本身的类型参数里没有容量，容量必须能从
/// 槽位指针反推，固定值是最简单且不会失配的选择。
pub(super) const CHUNK_CAPACITY: usize = 1024;

/// 空闲链的结束标记：也等于合法的槽位下标上界。
pub(super) const FREE_END: u16 = CHUNK_CAPACITY as u16;

/// 槽位被占用时写在 `next_free_` 里的标记。
///
/// 空闲链里的链接只会是 `0..=FREE_END`，不可能是 [`LIVE_MARK`]，因此这个标记
/// 既用于析构时判断槽位是否存活，也用于 `get` / `free` 的存活检查。
pub(super) const LIVE_MARK: u16 = u16::MAX;

// 容量必须为正、必须留得出 FREE_END、且索引能放进 u16。
const _: () = {
    assert!(CHUNK_CAPACITY > 0);
    assert!(CHUNK_CAPACITY < LIVE_MARK as usize);
};

/// 对象池中的一个槽位。
///
/// 空闲时 `value_` 未初始化，`next_free_` 指向 chunk 内下一个空闲槽位；
/// 占用时 `next_free_` 被写成 [`LIVE_MARK`]。
///
/// `self_idx_` 在 chunk 初始化时按数组下标写死，此后终生不变。回收路径靠它
/// 计算槽位在 chunk 内的偏移，从而反推 chunk 基址。
#[repr(C)]
pub(super) struct Slot<T> {
    /// 槽位值；只有 `next_free_ == LIVE_MARK` 时才是已初始化的 `T`。
    pub(super) value_: MaybeUninit<T>,
    /// 本槽位在 chunk 内的固定下标。
    pub(super) self_idx_: u16,
    /// 空闲链中的下一个槽位；占用时是 [`LIVE_MARK`]。
    pub(super) next_free_: u16,
}

/// 一段固定容量的连续槽位，以及它在 chunk 双向链表中的兄弟指针。
///
/// `A` 只是类型参数：chunk 的分配与释放都由 [`Arena`](super::Arena) 持有的
/// 那一份分配器完成，chunk 自身不需要再存一份。
#[repr(C)]
pub(super) struct Chunk<T, A: Allocator> {
    /// 链表中前一个 chunk。
    pub(super) prev_: Option<NonNull<Chunk<T, A>>>,
    /// 链表中后一个 chunk。
    pub(super) next_: Option<NonNull<Chunk<T, A>>>,
    /// 空闲链头部；等于 [`FREE_END`] 表示本 chunk 已满。
    pub(super) free_head_: u16,
    /// 当前承载着已初始化值的槽位数量。
    pub(super) used_: u16,
    /// chunk 内的全部槽位。
    pub(super) slots_: [Slot<T>; CHUNK_CAPACITY],
    /// 让 `A` 参与类型，同时不持有分配器值。
    pub(super) marker_: PhantomData<A>,
}

impl<T, A: Allocator> Chunk<T, A> {
    /// 在分配器返回的未初始化内存上就地初始化一个 chunk。
    ///
    /// 初始化只写头部字段与每个槽位的两个 `u16` 索引，`value_` 保持未初始化；
    /// 全部槽位按 `0 -> 1 -> ... -> FREE_END` 串成初始空闲链。
    ///
    /// 之所以逐字段就地写而不是先构造一个 `Chunk` 值再 `write`：后者会在栈上
    /// 先放一整块 `slots_`，对较大的 `T` 很容易把栈撑爆。
    ///
    /// # Safety
    ///
    /// - `chunk` 必须指向一块按 `Layout::new::<Chunk<T, A>>()` 分配、尚未初始化
    ///   且当前独占的内存；
    /// - 初始化完成后，调用方必须把它作为一个合法的 `Chunk` 使用。
    pub(super) unsafe fn init_in_place(chunk: NonNull<Self>) {
        let base = chunk.as_ptr();
        // SAFETY: 调用方保证 `base` 指向一块按本类型布局分配、未被初始化的
        // 独占内存。这里只通过裸指针逐字段写入，不创建任何引用。
        unsafe {
            ptr::addr_of_mut!((*base).prev_).write(None);
            ptr::addr_of_mut!((*base).next_).write(None);
            ptr::addr_of_mut!((*base).free_head_).write(0);
            ptr::addr_of_mut!((*base).used_).write(0);

            let slots = ptr::addr_of_mut!((*base).slots_).cast::<Slot<T>>();
            for i in 0..CHUNK_CAPACITY {
                let slot = slots.add(i);
                ptr::addr_of_mut!((*slot).self_idx_).write(i as u16);
                let next = if i + 1 == CHUNK_CAPACITY {
                    FREE_END
                } else {
                    (i + 1) as u16
                };
                ptr::addr_of_mut!((*slot).next_free_).write(next);
                // `value_` 保持 `MaybeUninit::uninit()` 的未初始化状态。
            }
            // `marker_` 是零大小类型，没有字节需要写。
        }
    }

    /// 由槽位指针和它的固定下标反推出所属 chunk。
    ///
    /// # Safety
    ///
    /// - `slot` 必须指向某个本类 chunk 的 `slots_` 数组中的元素；
    /// - `self_idx` 必须是该元素在数组中的真实下标（即槽位的 `self_idx_`）。
    ///
    /// 满足这两个条件时，减去固定偏移得到的地址仍落在同一块 chunk 分配内。
    pub(super) unsafe fn from_slot(slot: NonNull<Slot<T>>, self_idx: u16) -> NonNull<Self> {
        assert!(
            (self_idx as usize) < CHUNK_CAPACITY,
            "Arena: 槽位下标越界（{self_idx}）"
        );
        let offset =
            mem::offset_of!(Self, slots_) + (self_idx as usize) * mem::size_of::<Slot<T>>();
        // SAFETY: 由调用方保证 `slot` 位于本类 chunk 的槽位数组中，且
        // `self_idx` 是真实下标，因此 `offset` 不超过 chunk 的大小，减完仍
        // 指向同一块分配。使用 `byte_sub` 而不是整数相减，保留指针 provenance。
        let base = unsafe { slot.as_ptr().cast::<u8>().sub(offset) };
        // SAFETY: `base` 来自非空指针减去有限偏移，仍然非空。
        unsafe { NonNull::new_unchecked(base.cast::<Self>()) }
    }
}
