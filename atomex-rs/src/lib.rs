// Declaring our library as `no-std` unconditionally lets us be consistent
// in how we `use` items from `std` or `core`
#![no_std]

// 128 位原子实现（`AtomicI128`/`AtomicU128`）仍受 `integer_atomics` 门控；只在
// 显式开启 `support_u128_i128_atomics` 且目标支持 128 位原子时启用，因此该能力
// 需要 nightly。默认构建不会出现这个 feature 属性，stable 仍可用。
#![cfg_attr(
    all(feature = "support_u128_i128_atomics", target_has_atomic = "128"),
    feature(integer_atomics)
)]

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

mod atomex_ptr_;
mod atomic_cell_;
mod atomic_count_;
mod atomic_flags_;
mod cmpxch_result_;

pub mod fetch;

pub use atomex_ptr_::{AtomexPtr, AtomexPtrMut, AtomexPtrOwned};
pub use atomic_cell_::{
    LocksOrderings, StrictOrderings,
    TrAtomicCell, TrAtomicData, TrCmpxchOrderings,
};
pub use atomic_count_::{AtomicCount, AtomicCountMut, AtomicCountOwned};
pub use atomic_flags_::{AtomicFlags, TrAtomicFlags};
pub use cmpxch_result_::CmpxchResult;

pub mod x_deps {
    pub use funty;
}
