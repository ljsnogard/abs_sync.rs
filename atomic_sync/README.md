# atomic_sync

Atomic-based synchronization utils implementing traits in `abs_sync`.

See mod `mutex` and mod `rwlock` for detail.

## Implementations

`rwlock` 同时提供两套协作式（cooperative）读写锁实现，对外 API 完全一致，
不再使用 Cargo feature 二选一：

- `rwlock::cooperative`：堆分配等待队列（`VecDeque<Arc<WaitNode>>`）；
- `rwlock::cooperative_arena`：内部 `Arena<WaitNode>` + 侵入式主队列/读者链，
  等待路径除 chunk 分配外不发生堆分配。

调用方按分配特性与性能选择即可，两套实现会一起编译。
