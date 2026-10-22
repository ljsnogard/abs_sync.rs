//! 分配计数基准：简化 Arena 等待队列版（`rwlock::cooperative_arena`）。
//!
//! 与 `rwlock_alloc_heap` 共用 `rwlock_alloc_common.rs`。本 target 安装计数
//! 型全局分配器，断言"Arena 预热后除 chunk 外 0 次实现分配"。
//!
//! ```sh
//! cargo bench -p atomic_sync --bench rwlock_alloc_arena -- > .tmp/alloc_arena.txt
//! ```

use atomic_sync::rwlock::cooperative_arena as impl_mod;

type TestLock = impl_mod::CooperativeRwLockOwned<u64>;

const IMPL_NAME: &str = "arena";

include!("common/rwlock_alloc_common.rs");
