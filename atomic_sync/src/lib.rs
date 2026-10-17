#![allow(unused_features)]
#![feature(try_trait_v2)]
#![feature(impl_trait_in_assoc_type)]

#![no_std]

// `rwlock::cooperative` 的等待队列与等待节点需要堆分配，因此本 crate 依赖
// `alloc`。`alloc` 只是语言层面的 crate，最终是否需要分配器由链接方决定。
extern crate alloc;

// We always pull in `std` during tests, because it's just easier
// to write tests when you can assume you're on a capable platform
#[cfg(test)]
extern crate std;

pub mod mutex;
pub mod rwlock;

pub mod x_deps {
    pub use abs_sync;

    pub use atomex;
    pub use atomex::x_deps::funty;
}
