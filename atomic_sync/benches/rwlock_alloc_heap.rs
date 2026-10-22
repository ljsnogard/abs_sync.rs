//! 分配计数基准：堆分配等待队列版（`rwlock::cooperative`）。
//!
//! 与 `rwlock_alloc_arena` 共用 `rwlock_alloc_common.rs`。本 target 安装计数
//! 型全局分配器，断言"堆版稳态每次入队都会发生实现分配"。
//!
//! ```sh
//! cargo bench -p atomic_sync --bench rwlock_alloc_heap -- > .tmp/alloc_heap.txt
//! ```

use atomic_sync::rwlock::cooperative as impl_mod;

type TestLock = impl_mod::CooperativeRwLockOwned<u64>;

const IMPL_NAME: &str = "heap";

include!("common/rwlock_alloc_common.rs");
