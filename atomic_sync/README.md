# atomic_sync

Atomic-based synchronization utils implementing traits in `abs_sync`.

See mod `mutex` and mod `rwlock` for detail.

## Features

- `cooperative-arena`：让 `rwlock::cooperative` 使用内部 Arena + 侵入式双向链表
  承载等待节点；对外 API 与默认的堆分配队列版本完全一致。默认关闭，两个版本
  通过该 feature 单独编译、单独测量。
