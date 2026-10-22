//! 协作式（cooperative）读写锁——Arena 等待队列实现。
//!
//! 本模块与堆分配版 **对外 API 完全一致**，仅等待队列的承载方式不同：所有
//! 等待节点都是同一个 [`WaitNode`](wait_node_::WaitNode)，由内部 Arena 分配；
//! 节点内容用 `WakeNode` 区分读者组根、读者与写者。
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
//! ├── active_read_group_                    // 当前活跃读者组（快速路径不参与）
//! └── queue_ : SpinLock<QueuePayload>
//!     ├── head_ / tail_ : NonNull<WaitNode> // 主队列：RootRead / PureWrite
//!     ├── writer_waiters_
//!     └── arena_ : Arena<WaitNode>          // 统一等待节点池
//! ```
//!
//! # 主队列与读者链
//!
//! ```text
//! 主队列（prev_/next_）: RootRead ──> PureWrite ──> RootRead ──> ...
//!                          │
//! 读者链（next_ 单链）:     PureRead ──> PureRead ──> UpgrRead（至多一个）
//! ```
//!
//! - `RootRead` 保存当前持锁读者数 `count_` 与读者链头；
//! - 一条读者链可混合普通读与可升级读，但至多一个可升级读；
//! - 读者组只要 `count_ > 0` 就留在主队列；可升级读在“升级为写者 → 回退为
//!   可升级读 → 降级为普通读”的整个过程中都靠它连续跟踪持锁者；
//! - 读者领取通过状态字 CAS 完成并原子累加 `count_`，不取队列锁；
//! - 读者释放只做原子减：减到 1 且组内有已领取的 `UpgrRead` 时，在组后继
//!   预分配休眠升级写节点；减到 0 时由最后一个读者取一次队列锁，把整条读者链
//!   批量归还 Arena；
//! - 队列锁只保护主队列拓扑、封口标记与升级预留节点。
//!
//! 因此等待路径除 chunk 分配外不再发生堆分配，也没有按节点逐个摘链的收尾。

mod arena_;
mod core_;
mod reader_;
mod rwlock_;
mod upgrade_;
mod wait_node_;
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
