//! 协作式读写锁的错误类型。

use core::fmt;

/// 协作式读写锁的错误类型。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoopRwLockError {
    /// 非阻塞获取失败：当前无法获取，需要排队等待或稍后重试。
    WouldBlock,

    /// 异步获取过程中收到了取消信号（future 被丢弃或取消令牌触发）。
    Cancelled,

    /// 锁已被毒化。
    ///
    /// 目前本实现不会产生该值（`no_std` 下无法捕获持锁期间的 panic），
    /// 预留给将来引入毒化语义时使用。
    Poisoned,
}

impl fmt::Display for CoopRwLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoopRwLockError::WouldBlock => write!(f, "CoopRwLockError::WouldBlock"),
            CoopRwLockError::Cancelled => write!(f, "CoopRwLockError::Cancelled"),
            CoopRwLockError::Poisoned => write!(f, "CoopRwLockError::Poisoned"),
        }
    }
}

impl core::error::Error for CoopRwLockError {}
