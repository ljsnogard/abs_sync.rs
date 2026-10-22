//! 验证两套 `CooperativeRwLock` 的分配器参数真正用于等待队列。
//!
//! Verifies the allocator parameter of both `CooperativeRwLock` variants is
//! actually used by their wait queues.

#![feature(allocator_ext)]

extern crate alloc;

use alloc::alloc::Global;
use core::{
    alloc::{AllocError, Allocator, AllocatorClone, Layout},
    ptr::NonNull,
    task::{Context, Poll, Waker},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use atomic_sync::rwlock::{
    cooperative::CooperativeRwLockOwned as HeapLock,
    cooperative_arena::CooperativeRwLockOwned as ArenaLock,
};
use atomic_sync::x_deps::atomex::StrictOrderings;

/// 计数型分配器：把每次 `allocate` 转发给 `Global`，同时累加计数。
///
/// Counting allocator: forwards every `allocate` to `Global` and bumps a
/// counter so tests can observe whether the lock really used this allocator.
#[derive(Clone, Default)]
struct CountingAlloc {
    allocs_: Arc<AtomicUsize>,
}

// SAFETY: 只是把调用转发给 `Global`，并额外做一次原子计数；不改变内存
// 生命周期、对齐或释放语义。
unsafe impl Allocator for CountingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocs_.fetch_add(1, Ordering::Relaxed);
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: 调用方保证该指针来自本分配器（即 `Global`）且布局一致。
        unsafe { Global.deallocate(ptr, layout) }
    }
}

// SAFETY: `CountingAlloc` 是 `Allocator + Clone`；其内存语义与 `Global` 完全
// 一致，克隆出的句柄共享同一个计数，不会使已分配内存失效。
unsafe impl AllocatorClone for CountingAlloc {}

impl CountingAlloc {
    fn count_(&self) -> usize {
        self.allocs_.load(Ordering::Relaxed)
    }
}

/// 验证堆分配版的等待节点走调用方传入的分配器。
///
/// Verifies the heap-backed variant allocates wait nodes through the provided
/// allocator.
///
/// - 手段：用自定义 `CountingAlloc` 构造锁，记录基线；写者持有时排队一个
///   读者，观察分配计数增长。
/// - 判断：排队后计数严格大于基线，说明节点/`Vec`/`Arc` 没有绕过 `A`。
#[test]
fn cooperative_uses_custom_allocator_for_wait_queue() {
    let alloc = CountingAlloc::default();
    let counter = alloc.clone();
    let lock = HeapLock::<u64, AtomicUsize, StrictOrderings, CountingAlloc>::new_in(
        0,
        AtomicUsize::new(0),
        alloc,
    );
    let base = counter.count_();

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut ws = lock.acquire_session();
    let w = ws.try_write().expect("writer");
    let mut rs = lock.acquire_session();
    let mut fut = core::pin::pin!(rs.read_async().into_future());
    assert!(fut.as_mut().poll(&mut cx).is_pending(), "读者应先排队");
    let after_queue = counter.count_();
    assert!(
        after_queue > base,
        "堆版排队必须使用自定义分配器：{base} -> {after_queue}"
    );

    drop(w);
    let mut spins = 0usize;
    let g = loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(g)) => break g,
            Poll::Ready(Err(e)) => panic!("领取失败：{e:?}"),
            Poll::Pending => {
                spins += 1;
                assert!(spins < (1 << 22), "读者未被放行");
                core::hint::spin_loop();
            }
        }
    };
    drop(g);
}

/// 验证 Arena 版的 chunk 走调用方传入的分配器。
///
/// Verifies the Arena variant allocates its chunk through the provided
/// allocator.
///
/// - 手段：用自定义 `CountingAlloc` 构造锁，记录基线；首次排队一个读者，
///   触发 chunk 分配并观察计数增长。
/// - 判断：排队后计数严格大于基线，说明 chunk 没有绕过 `A`。
#[test]
fn cooperative_arena_uses_custom_allocator_for_chunk() {
    let alloc = CountingAlloc::default();
    let counter = alloc.clone();
    let lock = ArenaLock::<u64, AtomicUsize, StrictOrderings, CountingAlloc>::new_in(
        0,
        AtomicUsize::new(0),
        alloc,
    );
    let base = counter.count_();

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut ws = lock.acquire_session();
    let w = ws.try_write().expect("writer");
    let mut rs = lock.acquire_session();
    let mut fut = core::pin::pin!(rs.read_async().into_future());
    assert!(fut.as_mut().poll(&mut cx).is_pending(), "读者应先排队");
    let after_queue = counter.count_();
    assert!(
        after_queue > base,
        "Arena 排队必须使用自定义分配器：{base} -> {after_queue}"
    );

    drop(w);
    let mut spins = 0usize;
    let g = loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(g)) => break g,
            Poll::Ready(Err(e)) => panic!("领取失败：{e:?}"),
            Poll::Pending => {
                spins += 1;
                assert!(spins < (1 << 22), "读者未被放行");
                core::hint::spin_loop();
            }
        }
    };
    drop(g);
}
