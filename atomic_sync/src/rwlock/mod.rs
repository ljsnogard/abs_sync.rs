//! 读写锁实现。
//!
//! [`cooperative`] 子模块在编译期二选一，对外 API 完全相同：
//!
//! - 默认：等待队列使用 `VecDeque<Arc<WaitNode>>`（`cooperative/`）；
//! - 启用 `cooperative-arena` feature：等待队列使用内部 `Arena` 承载节点，
//!   节点之间是侵入式双向链表（`cooperative_arena/`）。
//!
//! feature 只在这里选择实现，不进入任一实现的内部代码。

mod error_;
mod state_;
mod wait_;

#[cfg(not(feature = "cooperative-arena"))]
pub mod cooperative;

#[cfg(feature = "cooperative-arena")]
#[path = "cooperative_arena/mod.rs"]
pub mod cooperative;

pub mod preemptive;

pub(super) trait TrShareMut<'a, T: ?Sized> {
    fn share_mut(&mut self) -> &'a mut T;
}
