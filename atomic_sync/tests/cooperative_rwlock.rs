//! `rwlock::cooperative` 的集成测试。
//!
//! 这里刻意**只使用 crate 的公开 API**，并以 `abs_sync` 的 trait 作为唯一入口
//! 驱动整个锁：只要 trait 的某个关联类型或方法没有正确实现，本文件就无法编译
//! 或无法运行。
//!
//! 测试跑在两个真实运行时上：`compio`（单线程）与 `tokio`（多线程）。

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
    vec::Vec,
};

use atomic_sync::{
    rwlock::cooperative::{CoopRwLockError, CooperativeRwLockOwned},
    x_deps::abs_sync::{
        async_rwlock::{
            TrAsyncRwLock, TrAsyncRwLockAcqSess, TrAsyncRwLockUpgradeSession,
            TrUpgradableReaderGuard, TrWriterGuard,
        },
        x_deps::abs_cancel::{CancelledToken, NonCancellableToken, TrMayCancel},
    },
};

type TestLock = CooperativeRwLockOwned<usize>;

/// 只用 trait 接口完成一轮：写 → 读 → 可升级读 → 升级为写 → 降级为读。
///
/// 这个函数对具体锁类型一无所知，因此它编译通过本身就说明
/// `CooperativeRwLock` 对 `TrAsyncRwLock` 的实现是完整的。
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
        // 降级得到的读者守卫只能读；要再写必须重新获取写许可。
        let mut w = sess.write_async().await.unwrap();
        *w += 1;
    }

    let r = sess.read_async().await.unwrap();
    *r
}

/// 测试完全经由 `abs_sync` trait 驱动的一轮读、写、升级与降级（compio 单线程）。
/// - 手段：构造 `CooperativeRwLockOwned<usize>`，交给只依赖 trait 的泛型函数驱动。
/// - 判断：返回值等于按步骤累加后的结果，说明各关联类型与方法都可用且语义正确。
#[compio::test]
async fn generic_trait_driven_round_trip() {
    let lock = TestLock::new_owned(0);
    assert_eq!(generic_round_trip(&lock).await, 3);

    let mut sess = lock.acquire_session();
    assert_eq!(*sess.read_async().await.unwrap(), 3);
}

/// 测试通过 `TrMayCancel` 传入永不取消的令牌时行为等同普通等待。
/// - 手段：用 `NonCancellableToken` 包裹读获取并 `.await`。
/// - 判断：获取成功，读到的值与写入值一致。
#[compio::test]
async fn non_cancellable_token_should_behave_like_plain_wait() {
    let lock = TestLock::new_owned(11);
    let mut sess = lock.acquire_session();

    let r = sess
        .read_async()
        .may_cancel_with(NonCancellableToken::new())
        .await
        .unwrap();
    assert_eq!(*r, 11);
}

/// 测试通过 `TrMayCancel` 传入已取消的令牌时获取被中止。
/// - 手段：用 `CancelledToken` 包裹处于竞争状态的写获取。
/// - 判断：返回 `Err(Cancelled)`，且持锁者释放后锁仍可正常使用。
#[compio::test]
async fn cancelled_token_should_abort_contended_write() {
    let lock = TestLock::new_owned(1);
    let mut sess_hold = lock.acquire_session();
    let mut sess = lock.acquire_session();

    let hold = sess_hold.try_write().unwrap();
    let r = sess.write_async().may_cancel_with(CancelledToken::new()).await;
    assert!(matches!(r, Result::Err(CoopRwLockError::Cancelled)));
    drop(r);

    drop(hold);
    assert_eq!(*sess.read_async().await.unwrap(), 1);
}

/// 测试获取 future 可以被 spawn 到**多线程**执行器上（`Send` 回归）。
/// - 手段：把"写一次"的异步逻辑放进 `tokio::spawn`，直接 `.await` 锁的
///   `write_async`，不做任何手工装箱或类型擦除。
/// - 判断：能编译即证明 future 满足 `Send`；运行后累加结果正确。
///
/// 这条断言很重要：若将来把获取 future 换成"内部藏着不透明 future"的实现，
/// `Send` 将无法推导，本测试会在编译期直接失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acquire_future_should_be_spawnable_on_multithread_runtime() {
    const TASKS: usize = 4;
    const ITERS: usize = 100;

    let lock = Arc::new(TestLock::new_owned(0));
    let mut tasks = Vec::new();
    for _ in 0..TASKS {
        let l = Arc::clone(&lock);
        tasks.push(tokio::spawn(async move {
            for _ in 0..ITERS {
                let mut sess = l.acquire_session();
                let mut w = sess.write_async().await.unwrap();
                *w += 1;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let mut sess = lock.acquire_session();
    assert_eq!(*sess.read_async().await.unwrap(), TASKS * ITERS);
}

/// 测试多线程下的读共享与写互斥（tokio 多线程）。
/// - 手段：若干读任务与写任务并发，用原子计数记录临界区人数。
/// - 判断：写者进入时读者计数必须为 0；读者进入时写者计数必须为 0。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_exclusion_should_hold_across_threads() {
    const TASKS: usize = 4;
    const ITERS: usize = 100;

    let lock = Arc::new(TestLock::new_owned(0));
    let readers = Arc::new(AtomicUsize::new(0));
    let writers = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for idx in 0..TASKS {
        let l = Arc::clone(&lock);
        let ir = Arc::clone(&readers);
        let iw = Arc::clone(&writers);
        tasks.push(tokio::spawn(async move {
            for _ in 0..ITERS {
                let mut sess = l.acquire_session();
                if idx.is_multiple_of(2) {
                    let _g = sess.read_async().await.unwrap();
                    ir.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(iw.load(Ordering::SeqCst), 0);
                    core::hint::spin_loop();
                    ir.fetch_sub(1, Ordering::SeqCst);
                } else {
                    let mut w = sess.write_async().await.unwrap();
                    iw.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(ir.load(Ordering::SeqCst), 0);
                    *w += 1;
                    iw.fetch_sub(1, Ordering::SeqCst);
                    drop(w);
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(readers.load(Ordering::SeqCst), 0);
    assert_eq!(writers.load(Ordering::SeqCst), 0);
}

/// 测试多线程下超时取消等待不会把锁弄坏。
/// - 手段：持有写守卫时用 `tokio::time::timeout` 取消一次写获取，随后释放写守卫。
/// - 判断：取消后读获取能在超时内成功，读到正确的值。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_wait_should_leave_lock_usable() {
    let lock = TestLock::new_owned(5);
    let mut sess_hold = lock.acquire_session();
    let mut sess = lock.acquire_session();

    let hold = sess_hold.try_write().unwrap();
    let timed_out =
        tokio::time::timeout(Duration::from_millis(50), sess.write_async()).await;
    assert!(timed_out.is_err(), "持锁期间等待应当超时");
    drop(timed_out);

    drop(hold);
    let got = tokio::time::timeout(Duration::from_millis(500), sess.read_async())
        .await
        .expect("取消之后锁必须仍然可用")
        .unwrap();
    assert_eq!(*got, 5);
}

/// 测试取消排队写者不会让排在它后面的读者永久阻塞（多线程回归）。
/// - 手段：写者持锁；spawn 一个写者任务使其入队，再 spawn 一个读者任务排在它后面；
///   用 `JoinHandle::abort` 取消写者任务（其 future 的 `Drop` 负责收回排队标记），
///   最后释放写者守卫。
/// - 判断：读者任务必须在超时内完成并读到正确值——若取消不收回 `WRITER_QUEUED`，
///   读者会被一个已不存在的写者永久挡在快速路径之外。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_writer_should_not_starve_queued_readers() {
    let lock = Arc::new(TestLock::new_owned(7));
    let mut sess_hold = lock.acquire_session();
    let hold = sess_hold.try_write().unwrap();

    let l_w = Arc::clone(&lock);
    let writer = tokio::spawn(async move {
        let mut s = l_w.acquire_session();
        let _g = s.write_async().await.unwrap();
    });
    let l_r = Arc::clone(&lock);
    let reader = tokio::spawn(async move {
        let mut s = l_r.acquire_session();
        let g = s.read_async().await.unwrap();
        *g
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    writer.abort();
    let _ = writer.await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    drop(hold);
    let got = tokio::time::timeout(Duration::from_millis(500), reader)
        .await
        .expect("写者被取消后读者必须能继续")
        .unwrap();
    assert_eq!(got, 7);
}
