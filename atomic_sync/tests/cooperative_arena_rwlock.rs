//! `rwlock::cooperative_arena` 的集成测试。
//!
//! Integration tests for the Arena-backed cooperative rwlock.
//!
//! 这里刻意**只使用 crate 的公开 API**，并以 `abs_sync` 的 trait 作为唯一入口
//! 驱动整个锁：只要 trait 的某个关联类型或方法没有正确实现，本文件就无法编译
//! 或无法运行。堆分配版另有同构的 `cooperative_rwlock.rs`，两者共同验证两套
//! 实现的对外 API 保持一致。

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    vec::Vec,
};

use atomic_sync::{
    rwlock::cooperative_arena::CooperativeRwLockOwned,
    x_deps::abs_sync::async_rwlock::{
        TrAsyncRwLock, TrAsyncRwLockAcqSess, TrAsyncRwLockUpgradeSession, TrUpgradableReaderGuard,
        TrWriterGuard,
    },
};

type TestLock = CooperativeRwLockOwned<usize>;

/// 只用 trait 接口完成一轮：写 → 读 → 可升级读 → 升级为写 → 降级为读。
///
/// Drives one round of write → read → upgradable read → upgrade → downgrade
/// using only trait methods.
async fn generic_round_trip<L>(lock: &L) -> usize
where
    L: TrAsyncRwLock<Target = usize>,
{
    let mut sess = lock.acq_session();

    {
        let mut w = sess.write_async().await.unwrap();
        *w += 1;
    }

    let observed = {
        let r = sess.read_async().await.unwrap();
        *r
    };

    {
        let upg = sess.upgradable_read_async().await.unwrap();
        let mut upg_sess = upg.upgrade_session();
        let mut w = upg_sess.upgrade_async().await.unwrap();
        *w += 1;
        let r = w.downgrade();
        assert_eq!(*r, observed + 1);
    }

    {
        let mut w = sess.write_async().await.unwrap();
        *w += 1;
    }

    let r = sess.read_async().await.unwrap();
    *r
}

/// 测试 Arena 版完全经由 `abs_sync` trait 驱动的一轮读、写、升级与降级。
///
/// Tests one trait-driven round trip of read / write / upgrade / downgrade on
/// the Arena implementation.
///
/// - 手段：构造 `CooperativeRwLockOwned<usize>`，交给只依赖 trait 的泛型函数
///   `generic_round_trip` 驱动，函数内部完成写、读、可升级读升级、写者降级。
/// - 判断：返回值等于按步骤累加后的结果（3），且之后仍能用公开 API 读到该值；
///   这同时证明关联类型与 trait 方法在 Arena 版上完整可用。
#[compio::test]
async fn generic_trait_driven_round_trip() {
    let lock = TestLock::new_owned(0);
    assert_eq!(generic_round_trip(&lock).await, 3);

    let mut sess = lock.acquire_session();
    assert_eq!(*sess.read_async().await.unwrap(), 3);
}

/// 测试多线程下 Arena 版读写互斥与更新不丢失。
///
/// Tests reader/writer mutual exclusion and update visibility under multiple
/// threads on the Arena implementation.
///
/// - 手段：tokio 多线程运行时上 spawn 2 个读者与 2 个写者，各循环 100 次；
///   读者进入临界区时断言没有写者，写者进入时断言没有读者，写者对数据自增。
/// - 判断：所有任务正常结束、计数归零，最终读到的值等于写者总次数
///   `2 * 100 = 200`，说明公开 API 在多线程下保持互斥且写入可见。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_exclusion_should_hold_across_threads() {
    const READERS: usize = 2;
    const WRITERS: usize = 2;
    const ITERS: usize = 100;

    let lock = Arc::new(TestLock::new_owned(0));
    let inside_readers = Arc::new(AtomicUsize::new(0));
    let inside_writers = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for _ in 0..READERS {
        let l = Arc::clone(&lock);
        let ir = Arc::clone(&inside_readers);
        let iw = Arc::clone(&inside_writers);
        tasks.push(tokio::spawn(async move {
            for _ in 0..ITERS {
                let mut s = l.acquire_session();
                let _g = s.read_async().await.unwrap();
                ir.fetch_add(1, Ordering::SeqCst);
                assert_eq!(iw.load(Ordering::SeqCst), 0, "读者不得与写者共存");
                core::hint::spin_loop();
                ir.fetch_sub(1, Ordering::SeqCst);
            }
        }));
    }
    for _ in 0..WRITERS {
        let l = Arc::clone(&lock);
        let ir = Arc::clone(&inside_readers);
        let iw = Arc::clone(&inside_writers);
        tasks.push(tokio::spawn(async move {
            for _ in 0..ITERS {
                let mut s = l.acquire_session();
                let mut w = s.write_async().await.unwrap();
                iw.fetch_add(1, Ordering::SeqCst);
                assert_eq!(ir.load(Ordering::SeqCst), 0, "写者不得与其他读者共存");
                *w += 1;
                iw.fetch_sub(1, Ordering::SeqCst);
                drop(w);
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(inside_readers.load(Ordering::SeqCst), 0);
    assert_eq!(inside_writers.load(Ordering::SeqCst), 0);

    let mut s = lock.acquire_session();
    assert_eq!(*s.read_async().await.unwrap(), WRITERS * ITERS);
}
