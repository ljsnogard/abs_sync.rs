#![no_std]

// to enable no hand-written poll
// #![feature(async_fn_traits)]
// #![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

#![feature(try_trait_v2)]
// #![feature(type_alias_impl_trait)]

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod demo_;

pub mod async_rwlock;
pub mod async_mutex;
pub mod sync_guard;
pub mod may_break;
pub mod sync_rwlock;
pub mod sync_mutex;

pub mod preludes {
    pub use super::async_rwlock::TrAsyncRwLock;
    pub use super::async_mutex::TrAsyncMutex;
    pub use super::may_break::TrMayBreak;
    pub use super::sync_rwlock::TrSyncRwLock;
    pub use super::sync_mutex::TrSyncMutex;
}

pub mod x_deps {
    pub use abs_cancel;
}
