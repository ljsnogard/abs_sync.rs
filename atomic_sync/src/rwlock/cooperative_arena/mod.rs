//! 协作式（cooperative）读写锁——Arena 等待队列实现。
//!
//! 本模块与堆分配版 **对外 API 完全一致**，仅等待队列的承载方式不同：节点由
//! 内部 Arena 分配，队列是侵入式双向链表。选择哪一版由 `rwlock` 顶层的
//! `cooperative-arena` feature 决定，本文件内部不再出现 feature 分支。
//!
//! # 结构总览
//!
//! ```text
//! CooperativeRwLock<T>              // 栈上的外壳，内联持有资源
//! ├── _pin_  : PhantomPinned
//! ├── core_  : Arc<RwCoreArena<D, B, O>>  // 共享同步状态，Sized 且与 T 无关
//! └── data_  : UnsafeCell<T>
//!
//! RwCoreArena<D, B, O>
//! ├── stat_  : CoopRwState<D, B, O>          // 状态字
//! └── queue_ : SpinLock<QueuePayload>
//!     ├── head_ / tail_ : NonNull<ArenaWaitNode>
//!     ├── writer_waiters_
//!     └── arena_ : Arena<ArenaWaitNode>      // 节点池
//! ```
//!
//! # 并发模型
//!
//! 快速路径、放行语义、取消回收与升级插队规则与堆分配版逐条一致；区别只在
//! 等待节点的生命周期：
//!
//! - 入队时从 Arena 取一个节点，插入链表；
//! - 节点从队首回收后立即归还 Arena 槽位；非当前 chunk 清空时整块归还分配器；
//! - 节点地址在存活期间稳定，因此链表直接保存裸指针，不需要 `Arc` 或索引。
//!
//! 所有队列操作仍然在 `queue_` 自旋锁内完成；锁内只收集 waker，不调用
//! `Waker::wake`。

mod arena_;
mod core_;
mod reader_;
mod rwlock_;
mod upgrade_;
mod wait_arena_;
mod writer_;

#[cfg(test)]
#[path = "../cooperative_tests_.rs"]
mod tests_;

pub use super::error_::CoopRwLockError;
pub use reader_::{ReadAcquireAsync, ReadAcquireFuture, ReaderGuard};
pub use rwlock_::{
    CooperativeAcqSession, CooperativeRwLock, CooperativeRwLockBorrowed, CooperativeRwLockOwned,
};
pub use upgrade_::{
    UpgradableReadAcquireAsync, UpgradableReadAcquireFuture, UpgradableReaderGuard, Upgrade,
    UpgradeAcquireAsync, UpgradeAcquireFuture,
};
pub use writer_::{WriteAcquireAsync, WriteAcquireFuture, WriterGuard};
