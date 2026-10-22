//! Arena 版等待节点：共享槽位逻辑 + 侵入式双向链表链接。
//!
//! [`WaitNode`] 负责"一个节点承载若干同质等待者"的全部槽位逻辑；本类型只是
//! 在它外面加一对前后驱指针，并把内层节点通过 [`Deref`] 暴露出来。这样两种
//! 队列实现对槽位状态机的行为保持完全一致。

use core::{ops::Deref, ptr::NonNull};

use crate::rwlock::wait_::{WaitKind, WaitNode};

/// Arena 中承载一个等待节点的槽位内容。
///
/// 前后驱指针只在持有队列自旋锁时读写；节点本身由 [`super::arena_::Arena`]
/// 分配，地址在存活期间稳定，因此指针可以长期保存进队列链接。
pub(super) struct ArenaWaitNode {
    /// 队列中的前一个节点。
    pub(super) prev_: Option<NonNull<Self>>,
    /// 队列中的后一个节点。
    pub(super) next_: Option<NonNull<Self>>,
    /// 共享的等待槽位逻辑。
    inner_: WaitNode,
}

impl ArenaWaitNode {
    /// 建立一个只带一个槽位的等待节点。
    pub(super) fn new(kind: WaitKind) -> Self {
        let (inner_, _) = WaitNode::new(kind);
        ArenaWaitNode {
            prev_: Option::None,
            next_: Option::None,
            inner_,
        }
    }
}

impl Deref for ArenaWaitNode {
    type Target = WaitNode;

    fn deref(&self) -> &Self::Target {
        &self.inner_
    }
}

// SAFETY: 两个 `NonNull` 只指向同一个 Arena 内的其他等待节点，且所有读写都
// 发生在等待队列自旋锁临界区内。把 `Arena`（连同其中的节点）移到另一个线程
// 时，节点内存不会移动，链接仍然有效；内层 `WaitNode` 本身满足 `Send`。
unsafe impl Send for ArenaWaitNode {}
