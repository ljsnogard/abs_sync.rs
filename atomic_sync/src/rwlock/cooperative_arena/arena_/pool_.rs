//! 单类型、链式 chunk、槽位地址稳定的对象池。
//!
//! # 结构
//!
//! `Arena<T, A>` 只保存链表的头/尾两个 chunk 指针，以及一份分配器 `A`。
//! chunk 之间用 `prev_` / `next_` 双向链接：
//!
//! ```text
//! head_                                   tail_
//!   │                                       │
//!   ▼                                       ▼
//! [Chunk] <──prev/next──> [Chunk] <──...──> [Chunk]
//! ```
//!
//! 分配总是发生在 `tail_`：chunk 头部只有一个 `free_head_: u16`，槽位里只有
//! `self_idx_: u16` 与 `next_free_: u16`。摘取空闲槽位时只需要把
//! `free_head_` 推到当前槽位的 `next_free_`；回收时把槽位压回 chunk 的空闲链
//! 头部即可，因此少量"不按顺序释放"的槽位也能被复用。
//!
//! # 为什么不需要在槽位上存 chunk 指针
//!
//! chunk 内槽位数组的起点相对 chunk 基址是固定偏移，而槽位的 `self_idx_`
//! 终生不变。于是给定任意 `NonNull<T>`：
//!
//! 1. 由于 `Slot::value_` 是首字段，值指针就是槽位指针；
//! 2. 从槽位里读出 `self_idx_`；
//! 3. 减去 `offset_of!(Chunk, slots_) + self_idx * size_of::<Slot<T>>()`，
//!    就得到 chunk 基址。
//!
//! 整个过程是 O(1) 的指针算术，不需要 chunk 指针、也不需要沿 chunk 链扫描。
//!
//! # chunk 的回收
//!
//! 等待队列是 FIFO：节点从队首释放、在队尾分配，因此最老的 chunk 会先空。
//! 除 `tail_` 之外的 chunk 一旦 `used_ == 0`，就立即从链表摘除并归还分配器；
//! `tail_` 即使空了也保留，作为下一次分配的当前块，避免抖动。这样常驻内存
//! 只跟"仍然存活的节点数 + 一个当前块"有关。
//!
//! # 安全边界
//!
//! `alloc` 是安全函数；`get` / `get_mut` / `free` 依赖调用方保证句柄来自本池、
//! 仍然存活且没有别名引用，因此是 `unsafe`。池本身不做同步：所有修改都需要
//! `&mut Arena`，共享访问必须由外部锁串行化。

use alloc::alloc::Global;
use core::{
    alloc::{AllocError, Allocator, Layout},
    mem::MaybeUninit,
    ptr::NonNull,
};

use super::chunk_::{CHUNK_CAPACITY, Chunk, FREE_END, LIVE_MARK, Slot};

/// 单类型的分块对象池。
///
/// 槽位地址在槽位存活期间稳定；chunk 扩容或回收都不会搬移仍然存活的槽位。
///
/// `A` 是分配 chunk 用的分配器，默认 [`Global`]。
///
/// # 线程安全
///
/// `Arena` 不是 `Sync`：所有会改动池的操作都需要 `&mut self`，共享访问必须由
/// 外部锁（例如等待队列的自旋锁）串行化。它在 `T: Send, A: Send` 时实现
/// `Send`，因此可以随外部锁一起跨线程移动。
///
/// # 内存行为
///
/// 一个 chunk 是 1024 个槽位的整块分配；只有当前 chunk 满时才会再申请一块。
/// 空掉的非当前 chunk 会立即归还分配器，当前 chunk 则保留复用。
pub(in crate::rwlock::cooperative_arena) struct Arena<T, A: Allocator = Global> {
    /// 链表中最早的 chunk。
    head_: Option<NonNull<Chunk<T, A>>>,
    /// 当前用于分配的 chunk（也是链表尾）。
    tail_: Option<NonNull<Chunk<T, A>>>,
    /// 分配与释放 chunk 用的分配器。
    alloc_: A,
}

// SAFETY: 池内所有 `NonNull` 都只指向 `alloc_` 申请、由本池独占的 chunk。
// 移动 `Arena`（包括跨线程移动）移动的是裸指针与分配器所有权，chunk 内存本身
// 不动，因此句柄与链链接始终有效。`T: Send` 保证仍然存活的值可以随池迁移并在
// 新线程读取/析构；`A: Send` 保证分配器状态可以迁移。`Arena` 不是 `Sync`
// （裸指针不自动实现 `Sync`），并发访问仍必须由外部锁串行化。
unsafe impl<T: Send, A: Allocator + Send> Send for Arena<T, A> {}

impl<T, A: Allocator> Arena<T, A> {
    /// 单个 chunk 的布局；用于分配失败时的 `handle_alloc_error` 报告。
    pub(in crate::rwlock::cooperative_arena) fn chunk_layout() -> Layout {
        Layout::new::<Chunk<T, A>>()
    }

    /// 建立一个空池；chunk 在第一次分配时惰性申请。
    pub(in crate::rwlock::cooperative_arena) const fn new(alloc: A) -> Self {
        Arena {
            head_: None,
            tail_: None,
            alloc_: alloc,
        }
    }

    /// 从池中分配一个槽位并写入 `value`，返回指向该值的稳定指针。
    ///
    /// 只有在当前 chunk 已满时才会向分配器申请新 chunk。返回的指针在
    /// [`free`](Self::free) 之前一直有效；只要它对应的 chunk 还有存活槽位，
    /// 池就不会回收这块 chunk。
    ///
    /// # Errors
    ///
    /// 当前 chunk 已满且分配器无法提供新 chunk 时返回 [`AllocError`]，此时
    /// 池的状态不变，`value` 随函数返回被丢弃。
    pub(in crate::rwlock::cooperative_arena) fn alloc(
        &mut self,
        value: T,
    ) -> Result<NonNull<T>, AllocError> {
        let chunk_ptr = match self.tail_ {
            // SAFETY: `tail_` 为本池拥有且仍然存活的 chunk。
            Some(tail) if unsafe { tail.as_ref() }.free_head_ != FREE_END => tail,
            _ => self.push_chunk_()?,
        };

        // SAFETY: `chunk_ptr` 由本池独占；`free_head_` 在非满时必定是
        // `0..CHUNK_CAPACITY` 内的合法下标，`slots` 指向本 chunk 的槽位数组。
        unsafe {
            let chunk = chunk_ptr.as_ptr();
            let idx = (*chunk).free_head_ as usize;
            debug_assert!(idx < CHUNK_CAPACITY, "Arena::alloc: free_head 越界");
            let slots = core::ptr::addr_of_mut!((*chunk).slots_).cast::<Slot<T>>();
            let slot = slots.add(idx);

            // 摘链：chunk 的新表头就是当前空闲槽的后继。
            (*chunk).free_head_ = (*slot).next_free_;
            // 占用标记：空闲链中不可能出现 LIVE_MARK。
            (*slot).next_free_ = LIVE_MARK;
            (*slot).value_ = MaybeUninit::new(value);
            (*chunk).used_ += 1;

            // 从 chunk 指针派生值指针，保留整块 chunk 的 provenance；`value_`
            // 是槽位首字段，所以槽位地址就是值地址。
            Result::Ok(NonNull::new_unchecked(slot.cast::<T>()))
        }
    }

    /// 读取句柄指向的值。
    ///
    /// # Safety
    ///
    /// - `ptr` 必须由本池的 [`alloc`](Self::alloc) 返回；
    /// - `ptr` 对应的槽位尚未被 [`free`](Self::free)；
    /// - 返回的引用存活期间不得再对同一槽位调用 `free` 或 `get_mut`。
    ///
    /// # Panics
    ///
    /// 若槽位不在存活状态，会 panic；这通常意味着句柄属于另一个池、已经被
    /// 释放，或者它所在的空 chunk 已经被归还给分配器。
    #[cfg(test)]
    pub(in crate::rwlock::cooperative_arena) unsafe fn get(&self, ptr: NonNull<T>) -> &T {
        // SAFETY: 调用方保证 `ptr` 指向本池中一个仍然存活的槽位。
        unsafe { self.assert_live_(ptr) };
        // SAFETY: 上面已确认槽位存活；调用方保证指针属于本池且未回收。
        unsafe { ptr.as_ref() }
    }

    /// 取得句柄指向值的可变引用。
    ///
    /// # Safety
    ///
    /// 与 [`get`](Self::get) 相同，并且返回的独占引用存活期间不得再通过其它
    /// 路径访问同一个槽位。
    ///
    /// # Panics
    ///
    /// 若槽位不在存活状态，会 panic。
    #[cfg(test)]
    pub(in crate::rwlock::cooperative_arena) unsafe fn get_mut(&mut self, mut ptr: NonNull<T>) -> &mut T {
        // SAFETY: 同 `get`。
        unsafe { self.assert_live_(ptr) };
        // SAFETY: 上面已确认槽位存活；`&mut self` 保证本池没有其它借用。
        unsafe { ptr.as_mut() }
    }

    /// 回收槽位，把其中的值移出并返回给调用方。
    ///
    /// 返回值是移出池的 `T`：调用方应当在**释放外部锁之后**再丢弃它，避免在
    /// 临界区内执行任意的 `Drop` 代码。槽位立即进入所属 chunk 的空闲链；若这
    /// 使一个非当前的 chunk 变空，该 chunk 会被立即归还给分配器。
    ///
    /// # Safety
    ///
    /// - `ptr` 必须由本池的 [`alloc`](Self::alloc) 返回；
    /// - `ptr` 对应的槽位必须仍然存活，且尚未被回收（不得重复释放）；
    /// - `&mut self` 之外的任何引用（包括 `get` 返回的引用）都必须已经结束。
    ///
    /// # Panics
    ///
    /// 若槽位不在存活状态，会 panic（重复释放是调用方的契约错误）。
    #[must_use = "free 返回被移出池的值，请显式丢弃或复用；不要把它留在池里"]
    pub(in crate::rwlock::cooperative_arena) unsafe fn free(&mut self, ptr: NonNull<T>) -> T {
        let slot = ptr.cast::<Slot<T>>();
        // SAFETY: 调用方保证 `ptr` 指向本池中一个仍然存活的槽位；它的
        // `self_idx_` 与槽位真实下标一致，因此可以反推 chunk。
        let self_idx = unsafe { (*slot.as_ptr()).self_idx_ };
        let chunk_ptr = unsafe { Chunk::from_slot(slot, self_idx) };

        // SAFETY: 下面全部通过裸指针访问同一块 chunk，不创建互相重叠的引用。
        let value = unsafe {
            let chunk = chunk_ptr.as_ptr();
            let slot = slot.as_ptr();
            assert_eq!(
                (*slot).next_free_,
                LIVE_MARK,
                "Arena::free: 槽位未存活或重复释放"
            );
            let value = (*slot).value_.assume_init_read();
            // 压回 chunk 空闲链头部；空闲链里的链接只可能是 0..=FREE_END。
            (*slot).next_free_ = (*chunk).free_head_;
            (*chunk).free_head_ = self_idx;
            debug_assert!((*chunk).used_ > 0, "Arena::free: used_ 下溢");
            (*chunk).used_ -= 1;
            value
        };

        // 非 tail 的 chunk 一旦空掉就可以立即归还；tail 留作当前分配块。
        let empty = unsafe { (*chunk_ptr.as_ptr()).used_ == 0 };
        if empty && self.tail_ != Some(chunk_ptr) {
            self.release_chunk_(chunk_ptr);
        }
        value
    }

    /// 当前承载着已初始化值的槽位总数。
    #[cfg(test)]
    pub(in crate::rwlock::cooperative_arena) fn len(&self) -> usize {
        let mut total = 0;
        let mut cur = self.head_;
        while let Some(chunk) = cur {
            // SAFETY: 链表中的每个 chunk 都由本池拥有且尚未释放。
            let chunk = unsafe { chunk.as_ref() };
            total += chunk.used_ as usize;
            cur = chunk.next_;
        }
        total
    }

    /// 池中是否没有任何存活对象（可能仍保留一个空的当前 chunk）。
    #[cfg(test)]
    pub(in crate::rwlock::cooperative_arena) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 当前链表中的 chunk 数量（包含保留的空当前 chunk）。
    #[cfg(test)]
    pub(in crate::rwlock::cooperative_arena) fn chunk_count(&self) -> usize {
        let mut count = 0;
        let mut cur = self.head_;
        while let Some(chunk) = cur {
            // SAFETY: 链表中的每个 chunk 都由本池拥有且尚未释放。
            count += 1;
            cur = unsafe { chunk.as_ref() }.next_;
        }
        count
    }

    /// 检查句柄对应的槽位是否处于存活状态。
    ///
    /// # Safety
    ///
    /// `ptr` 必须指向本池分配出的、仍然映射着内存的槽位。指向已归还的 chunk
    /// 或其它分配会破坏引用规则。
    #[cfg(test)]
    unsafe fn assert_live_(&self, ptr: NonNull<T>) {
        // SAFETY: 由调用方保证 `ptr` 指向本池的有效槽位。
        let slot = unsafe { ptr.cast::<Slot<T>>().as_ref() };
        assert_eq!(
            slot.next_free_, LIVE_MARK,
            "Arena::get: 槽位未存活或句柄不属于本池"
        );
    }

    /// 申请并链接一个新的 tail chunk。
    fn push_chunk_(&mut self) -> Result<NonNull<Chunk<T, A>>, AllocError> {
        let layout = Layout::new::<Chunk<T, A>>();
        let mem = self.alloc_.allocate(layout)?;
        // SAFETY: 分配器返回的块满足 `Chunk<T, A>` 的对齐与大小要求。
        let chunk = mem.cast::<Chunk<T, A>>();
        // SAFETY: 该块尚未初始化且由本池独占，布局与 `chunk` 的静态类型一致。
        unsafe { Chunk::init_in_place(chunk) };

        match self.tail_ {
            // SAFETY: 旧 tail 与本 chunk 都属于本池；`&mut self` 保证独占。
            Some(tail) => unsafe {
                (*tail.as_ptr()).next_ = Some(chunk);
                (*chunk.as_ptr()).prev_ = Some(tail);
            },
            None => self.head_ = Some(chunk),
        }
        self.tail_ = Some(chunk);
        Result::Ok(chunk)
    }

    /// 把一个已经没有任何存活槽位的非 tail chunk 从链表摘除并归还分配器。
    fn release_chunk_(&mut self, chunk: NonNull<Chunk<T, A>>) {
        debug_assert_ne!(self.tail_, Some(chunk), "不能释放当前 tail chunk");
        // SAFETY: 调用方保证 `chunk` 属于本池、`used_ == 0`，且没有存活槽位；
        // 摘链与归还期间不会再访问它的内容。
        unsafe {
            let chunk_ptr = chunk.as_ptr();
            debug_assert_eq!((*chunk_ptr).used_, 0, "Arena: 释放了仍有值的 chunk");
            let prev = (*chunk_ptr).prev_;
            let next = (*chunk_ptr).next_;
            match prev {
                Some(prev) => (*prev.as_ptr()).next_ = next,
                None => self.head_ = next,
            }
            match next {
                Some(next) => (*next.as_ptr()).prev_ = prev,
                None => self.tail_ = prev,
            }
            self.alloc_
                .deallocate(chunk.cast::<u8>(), Layout::new::<Chunk<T, A>>());
        }
    }
}

impl<T, A: Allocator> Drop for Arena<T, A> {
    fn drop(&mut self) {
        let layout = Layout::new::<Chunk<T, A>>();
        let mut cur = self.head_;
        while let Some(chunk_ptr) = cur {
            // SAFETY: 链表中的 chunk 都属于本池；先取出后继，再析构并归还当前块。
            let next = unsafe { (*chunk_ptr.as_ptr()).next_ };
            {
                // SAFETY: 本池独占该 chunk；`next_free_ == LIVE_MARK` 的槽位
                // 已初始化且尚未移出，每个槽位只会在这里析构一次。
                let chunk = unsafe { &mut *chunk_ptr.as_ptr() };
                for slot in &mut chunk.slots_ {
                    if slot.next_free_ == LIVE_MARK {
                        // SAFETY: 同上。
                        unsafe { slot.value_.assume_init_drop() };
                    }
                }
            }
            // SAFETY: 该块由 `push_chunk_` 按同一布局分配，且还有本池持有；
            // 上面的引用已经结束。
            unsafe {
                self.alloc_.deallocate(chunk_ptr.cast::<u8>(), layout);
            }
            cur = next;
        }
    }
}
