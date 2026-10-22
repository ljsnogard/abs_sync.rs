//! 协作式读写锁等待队列的共享底层构件：自旋锁与等待种类。
//!
//! 堆分配版自己的节点/槽位在 `cooperative/wait_node_` 中；Arena 版在
//! `cooperative_arena/wait_arena_` 中。本模块只保留两边都要用的自旋锁与
//! `WaitKind`。

use core::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
};

/// 一个极简的自旋锁。
///
/// 它只服务于本模块内部的等待队列与等待节点，临界区里**绝不允许**出现
/// 任何可能让出执行权的操作（尤其是调用 `Waker::wake`），否则会在
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
