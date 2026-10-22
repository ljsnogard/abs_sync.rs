//! `Arena` 的单元测试。
//!
//! 每个 chunk 固定 1024 个槽位；测试会通过"填满一个 chunk 再多分配一个"来
//! 覆盖扩容、跨 chunk 回收与空 chunk 归还等路径。

use alloc::alloc::Global;
use core::{
    alloc::{AllocError, Allocator, Layout},
    ptr::NonNull,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    vec::Vec,
};

use super::{Arena, chunk_::CHUNK_CAPACITY};

/// 只记录分配/释放次数、其余转发给 `Global` 的测试分配器。
struct CountingAlloc {
    allocs_: Arc<AtomicUsize>,
    frees_: Arc<AtomicUsize>,
}

// SAFETY: 所有分配与释放都转发给 `Global`，只额外做计数，不改变内存布局与
// 对齐；`deallocate` 收到的指针与布局原样交给 `Global`。
unsafe impl Allocator for CountingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ptr = Global.allocate(layout)?;
        self.allocs_.fetch_add(1, Ordering::SeqCst);
        Result::Ok(ptr)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.frees_.fetch_add(1, Ordering::SeqCst);
        // SAFETY: 调用方保证 `ptr` 与 `layout` 来自本分配器的一次分配；本类型
        // 原样转发给 `Global`，契约随之成立。
        unsafe { Global.deallocate(ptr, layout) }
    }
}

/// 永远失败的分配器，用来覆盖 `Arena::alloc` 的错误路径。
struct AlwaysFail;

// SAFETY: `allocate` 永远不返回内存，因此 `deallocate` 永远不会被调用。
unsafe impl Allocator for AlwaysFail {
    fn allocate(&self, _layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Result::Err(AllocError)
    }

    unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {}
}

/// 验证基本的分配、读取、改写与回收流程。
///
/// Verifies the basic allocate / read / mutate / free round trip.
///
/// - 手段：在默认分配器的池中分配两个 `usize`，读取并改写其中一个，再回收它。
/// - 判断：`len` 随分配/回收增减；读到的值、改写后的值与 `free` 返回的旧值
///   全部一致，且未被回收的另一个句柄仍然可读。
#[test]
fn alloc_get_mutate_and_free_roundtrip_works() {
    let mut arena = Arena::<usize>::new(Global);
    assert!(arena.is_empty());
    assert_eq!(arena.chunk_count(), 0);

    let first = arena.alloc(11).expect("分配第一个槽位失败");
    let second = arena.alloc(22).expect("分配第二个槽位失败");
    assert_eq!(arena.len(), 2);
    assert_eq!(arena.chunk_count(), 1);

    // SAFETY: 两个句柄都由本池 `alloc` 返回，且都还没有被回收。
    unsafe {
        assert_eq!(*arena.get(first), 11);
        assert_eq!(*arena.get(second), 22);
        *arena.get_mut(first) = 111;
        assert_eq!(*arena.get(first), 111);
    }

    // SAFETY: `first` 尚未回收，满足 `free` 的前置条件。
    assert_eq!(unsafe { arena.free(first) }, 111);
    assert_eq!(arena.len(), 1);
    assert!(!arena.is_empty());

    // SAFETY: `second` 仍然存活。
    assert_eq!(unsafe { *arena.get(second) }, 22);
}

/// 验证追加新 chunk 不会搬移既有槽位。
///
/// Verifies that pushing a new chunk does not move slots already allocated.
///
/// - 手段：连续分配 `CHUNK_CAPACITY + 1` 个对象，必然触发第二个 chunk；
///   记录第一个 chunk 中所有对象的地址与值。
/// - 判断：分配后 `chunk_count == 2`，第一个 chunk 内每个句柄的地址与分配时
///   完全相同，所有对象读出的值都与写入值一致。
#[test]
fn chunk_growth_keeps_existing_addresses_stable() {
    let mut arena = Arena::<usize>::new(Global);
    let mut handles = Vec::with_capacity(CHUNK_CAPACITY + 1);
    let mut first_chunk_addrs = Vec::with_capacity(CHUNK_CAPACITY);

    for value in 0..CHUNK_CAPACITY + 1 {
        let ptr = arena.alloc(value).expect("分配槽位失败");
        if value < CHUNK_CAPACITY {
            first_chunk_addrs.push(ptr.as_ptr() as usize);
        }
        handles.push(ptr);
    }
    assert_eq!(arena.chunk_count(), 2);

    for (value, ptr) in handles.iter().enumerate() {
        if value < CHUNK_CAPACITY {
            assert_eq!(ptr.as_ptr() as usize, first_chunk_addrs[value]);
        }
        // SAFETY: 句柄由本池 `alloc` 返回且尚未回收。
        assert_eq!(unsafe { *arena.get(*ptr) }, value);
    }
}

/// 验证同一个 chunk 内按"后释放先复用"的顺序回收槽位。
///
/// Verifies that freed slots are reused last-freed-first inside a chunk.
///
/// - 手段：先分配 8 个对象，再依次回收下标 0、5、2；随后连续分配 3 个对象。
/// - 判断：三次新分配依次拿回下标 2、5、0 对应的槽位；此后的分配来自原本
///   尚未使用过的下标 8，而不是这三个被复用的槽位之一。
#[test]
fn freed_slots_are_reused_in_last_freed_first_order() {
    let mut arena = Arena::<usize>::new(Global);
    let handles: Vec<_> = (0..8)
        .map(|value| arena.alloc(value).expect("分配槽位失败"))
        .collect();

    for &index in &[0usize, 5, 2] {
        // SAFETY: 每个句柄只释放一次，且都来自本池。
        assert_eq!(unsafe { arena.free(handles[index]) }, index);
    }

    let reused: Vec<_> = (100..103)
        .map(|value| arena.alloc(value).expect("复用槽位失败"))
        .collect();
    assert_eq!(reused[0], handles[2]);
    assert_eq!(reused[1], handles[5]);
    assert_eq!(reused[2], handles[0]);
    assert_eq!(arena.len(), 8);

    let next = arena.alloc(103).expect("分配下一个空闲槽位失败");
    assert_eq!(arena.len(), 9);
    assert_ne!(next, reused[0]);
    assert_ne!(next, reused[1]);
    assert_ne!(next, reused[2]);
    // SAFETY: `next` 刚刚分配且尚未回收。
    assert_eq!(unsafe { *arena.get(next) }, 103);
}

/// 验证空掉的非当前 chunk 会立即归还分配器，当前 chunk 保留复用。
///
/// Verifies that an empty non-tail chunk is returned to the allocator
/// immediately, while the tail chunk is retained for reuse.
///
/// - 手段：用计数分配器分配 `CHUNK_CAPACITY + 1` 个对象（两个 chunk），先回收
///   第一个 chunk 的全部槽位，再回收当前 chunk 的最后一个槽位，最后再分配一个。
/// - 判断：回收第一个 chunk 后 `chunk_count == 1` 且释放计数为 1；此时当前
///   chunk 仍保留，再分配不会触发新的 chunk 分配。
#[test]
fn empty_non_tail_chunks_are_returned_to_allocator() {
    let allocs = Arc::new(AtomicUsize::new(0));
    let frees = Arc::new(AtomicUsize::new(0));

    {
        let allocator = CountingAlloc {
            allocs_: allocs.clone(),
            frees_: frees.clone(),
        };
        let mut arena = Arena::<usize, CountingAlloc>::new(allocator);

        let handles: Vec<_> = (0..CHUNK_CAPACITY + 1)
            .map(|value| arena.alloc(value).expect("分配槽位失败"))
            .collect();
        assert_eq!(arena.chunk_count(), 2);
        assert_eq!(allocs.load(Ordering::SeqCst), 2);
        assert_eq!(frees.load(Ordering::SeqCst), 0);

        // 释放第一个 chunk 的全部槽位；最后一个释放会触发整块归还。
        for &ptr in &handles[..CHUNK_CAPACITY] {
            // SAFETY: 每个句柄只释放一次。
            let _ = unsafe { arena.free(ptr) };
        }
        assert_eq!(arena.len(), 1);
        assert_eq!(arena.chunk_count(), 1);
        assert_eq!(frees.load(Ordering::SeqCst), 1);

        // 当前 chunk 空了也保留，下一次分配直接复用它。
        // SAFETY: 最后一个句柄只释放一次。
        let _ = unsafe { arena.free(handles[CHUNK_CAPACITY]) };
        assert!(arena.is_empty());
        assert_eq!(arena.chunk_count(), 1);
        assert_eq!(frees.load(Ordering::SeqCst), 1);

        let reused = arena.alloc(7).expect("复用保留的当前 chunk 失败");
        assert_eq!(arena.chunk_count(), 1);
        assert_eq!(allocs.load(Ordering::SeqCst), 2);
        // SAFETY: `reused` 刚刚分配且尚未回收。
        assert_eq!(unsafe { *arena.get(reused) }, 7);
        // SAFETY: `reused` 只释放一次。
        let _ = unsafe { arena.free(reused) };
    }

    // 池析构时归还保留的当前 chunk。
    assert_eq!(frees.load(Ordering::SeqCst), 2);
}

/// 验证清空链表中间的 chunk 时能正确摘链，并保持后续回收可达。
///
/// Verifies that emptying a middle chunk unlinks it correctly and keeps the
/// remaining chunks reachable.
///
/// - 手段：分配 3 个 chunk 的对象；先回收中间 chunk 的全部槽位，再回收最老
///   chunk 的全部槽位，最后回收当前 chunk 的全部槽位。
/// - 判断：每次整块回收后 `chunk_count` 依次降到 2、1、1，`len` 依次降到
///   `2 * CHUNK_CAPACITY`、`CHUNK_CAPACITY`、0；分配器的释放计数与回收的
///   chunk 数一致。
#[test]
fn empty_middle_chunk_is_unlinked_correctly() {
    let allocs = Arc::new(AtomicUsize::new(0));
    let frees = Arc::new(AtomicUsize::new(0));

    {
        let allocator = CountingAlloc {
            allocs_: allocs.clone(),
            frees_: frees.clone(),
        };
        let mut arena = Arena::<usize, CountingAlloc>::new(allocator);
        let handles: Vec<_> = (0..CHUNK_CAPACITY * 3)
            .map(|value| arena.alloc(value).expect("分配槽位失败"))
            .collect();
        assert_eq!(arena.chunk_count(), 3);
        assert_eq!(allocs.load(Ordering::SeqCst), 3);

        // 回收中间 chunk：它既不是 head 也不是 tail，需要用 prev/next 摘链。
        for &ptr in &handles[CHUNK_CAPACITY..CHUNK_CAPACITY * 2] {
            // SAFETY: 每个句柄只释放一次。
            let _ = unsafe { arena.free(ptr) };
        }
        assert_eq!(arena.chunk_count(), 2);
        assert_eq!(arena.len(), CHUNK_CAPACITY * 2);
        assert_eq!(frees.load(Ordering::SeqCst), 1);

        // 回收最老的 chunk：head 必须推进到当前仅剩的 chunk。
        for &ptr in &handles[..CHUNK_CAPACITY] {
            // SAFETY: 每个句柄只释放一次。
            let _ = unsafe { arena.free(ptr) };
        }
        assert_eq!(arena.chunk_count(), 1);
        assert_eq!(arena.len(), CHUNK_CAPACITY);
        assert_eq!(frees.load(Ordering::SeqCst), 2);

        // 当前 chunk 空了也保留。
        for &ptr in &handles[CHUNK_CAPACITY * 2..] {
            // SAFETY: 每个句柄只释放一次。
            let _ = unsafe { arena.free(ptr) };
        }
        assert!(arena.is_empty());
        assert_eq!(arena.chunk_count(), 1);
        assert_eq!(frees.load(Ordering::SeqCst), 2);
    }

    // 池析构时归还保留的当前 chunk。
    assert_eq!(frees.load(Ordering::SeqCst), 3);
}

/// 验证池析构只销毁仍然存活的值，已回收的值不会被二次析构。
///
/// Verifies that dropping the arena destroys only live values exactly once.
///
/// - 手段：用一个在 `Drop` 里累加计数器的探针对象分配 3 个槽位，回收其中 1 个，
///   先检查计数，再让池离开作用域。
/// - 判断：回收后计数为 1；池析构后总计数为 3（3 个值各析构一次）。
#[test]
fn arena_drop_destroys_only_live_values() {
    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let mut arena = Arena::<DropProbe>::new(Global);
        let handles: Vec<_> = (0..3)
            .map(|_| arena.alloc(DropProbe(drops.clone())).expect("分配探针失败"))
            .collect();

        // SAFETY: `handles[1]` 只释放一次。
        drop(unsafe { arena.free(handles[1]) });
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

/// 验证 chunk 分配失败时池保持为空，并且错误会向上返回。
///
/// Verifies that a chunk allocation failure propagates and leaves the arena
/// unchanged.
///
/// - 手段：使用永远失败的分配器，第一次分配即触发 `push_chunk_`。
/// - 判断：`alloc` 返回 `Err`，池仍然为空且没有留下任何 chunk。
#[test]
fn allocation_failure_leaves_arena_unchanged() {
    let mut arena = Arena::<usize, AlwaysFail>::new(AlwaysFail);
    assert!(arena.alloc(1).is_err());
    assert!(arena.is_empty());
    assert_eq!(arena.chunk_count(), 0);
}

/// 验证值类型与分配器都满足 `Send` 时池可以跨线程移动。
///
/// Verifies that the arena is `Send` when both the value and allocator are.
///
/// - 手段：在编译期对 `Arena<usize, Global>` 做 `Send` 约束检查。
/// - 判断：约束成立，函数编译通过。
#[test]
fn arena_is_send_when_value_and_allocator_are_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Arena<usize, Global>>();
}
