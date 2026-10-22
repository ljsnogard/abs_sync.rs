//! 双实现对比基准：堆分配等待队列版（`rwlock::cooperative`）。
//!
//! 本 target 与 `rwlock_compare_arena` 共用 `rwlock_compare_common.rs`，
//! 唯一的区别是这里选择的 `impl_mod`。用同一份源码分别编译，避免"临时改
//! import 再还原"造成的不复现。
//!
//! 运行示例：
//!
//! ```sh
//! taskset -c 0-3 cargo bench -p atomic_sync --bench rwlock_compare_heap -- \
//!     > .tmp/compare_heap.txt
//! ```
//!
//! 场景与输出格式见 `rwlock_compare_common.rs` 顶部的说明。

use atomic_sync::rwlock::cooperative as impl_mod;

type TestLock = impl_mod::CooperativeRwLockOwned<u64>;

const IMPL_NAME: &str = "heap";

include!("common/rwlock_compare_common.rs");
