//! 读写锁实现。
//!
//! [`cooperative`] 与 [`cooperative_arena`] 两套实现**同时编译**，对外 API
//! 完全一致，由调用方按分配/性能特性选择，不再使用 Cargo feature 二选一：
//!
//! - `cooperative/`：等待队列使用 `VecDeque<Arc<WaitNode>>`；
//! - `cooperative_arena/`：等待队列使用内部 `Arena<WaitNode>`，主队列与读者链
//!   都是侵入式链表。

mod error_;
mod state_;
mod wait_;

pub mod cooperative;
pub mod cooperative_arena;

pub mod preemptive;

pub(super) trait TrShareMut<'a, T: ?Sized> {
    fn share_mut(&mut self) -> &'a mut T;
}
