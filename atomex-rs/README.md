# atomex

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/ljsnogard/atomex-rs)

Some useful extensions around `Atomic*` in `core::sync::atomic`.

This crate is mainly inspired by [atomic-traits](https://crates.io/crates/atomic-traits)

## Example

```rust
use core::sync::atomic::*;
use atomex::AtomicCount;

let atm = AtomicUsize::new(0usize);
let cnt = AtomicCount::<usize, AtomicUsize>::new(atm);

let mut atm = cnt.into_inner();
let cnt = AtomicCount::<usize, &mut AtomicUsize>::new(&mut atm);

assert_eq!(cnt.inc(), 0usize);
assert_eq!(cnt.dec(), 1usize);
assert_eq!(cnt.val(), 0usize);
```