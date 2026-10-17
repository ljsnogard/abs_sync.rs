// Declaring our library as `no-std` unconditionally lets us be consistent
// in how we `use` items from `std` or `core`
#![no_std]

// #![feature(integer_atomics)]

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
