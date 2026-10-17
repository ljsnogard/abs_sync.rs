# abs_sync

Abstraction of synchronization for sync/async programming in Rust  
This crate provide traits about cancellation, locks and mutex.  

## Required unstable features:

```
#![feature(sync_unsafe_cell)]
#![feature(try_trait_v2)]
#![feature(type_alias_impl_trait)]
```

## Why would I need this?

* To implement async tasks with graceful cancellation
* To implement business with lock and/or mutex that can be injected on demand
