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
//! - `RootRead` 的单个原子同时保存**持锁计数**与**可升级读持锁标记**；
//!   没有独立的 pending / release_ops 计数；
//! - 一条读者链可混合普通读与可升级读，但至多一个可升级读；
//! - 读者组只要计数 `> 0` 就留在主队列；可升级读在“升级为写者 → 回退为
//!   可升级读 → 降级为普通读”的整个过程中都靠它连续跟踪持锁者；
//! - 主队列、读者链、节点状态与 waker 全部由队列锁保护，节点不再单独加锁；
//! - 放行时在锁内把一批读者标成 `Admitted`、累加组计数并取出 waker，释放锁后
//!   再 `wake`；下一轮重新从队首选择，继续下一批；
//! - 读者释放/取消只做原子减一；最后一个减到 0 的读者触发 `run_pass_`，
//!   由队首 `prune_front_` 把整条读者链批量归还 Arena；
//! - 升级写节点只在真正发起升级时插入组后继，不再预留；
//! - 没有 `sealed` / `waking` / `linked` 标记：能否加入读者链由“组是否主队列
//!   尾 + 链中是否已有 UpgrRead”决定，空组由最后一个计数触发回收。
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
