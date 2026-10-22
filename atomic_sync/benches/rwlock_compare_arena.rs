//! 双实现对比基准：简化 Arena 等待队列版（`rwlock::cooperative_arena`）。
//!
//! 本 target 与 `rwlock_compare_heap` 共用 `rwlock_compare_common.rs`，
//! 唯一的区别是这里选择的 `impl_mod`。
//!
//! 运行示例：
//!
//! ```sh
//! taskset -c 0-3 cargo bench -p atomic_sync --bench rwlock_compare_arena -- \
//!     > .tmp/compare_arena.txt
//! ```
//!
//! 场景与输出格式见 `rwlock_compare_common.rs` 顶部的说明。

use atomic_sync::rwlock::cooperative_arena as impl_mod;

type TestLock = impl_mod::CooperativeRwLockOwned<u64>;

const IMPL_NAME: &str = "arena";

include!("common/rwlock_compare_common.rs");
