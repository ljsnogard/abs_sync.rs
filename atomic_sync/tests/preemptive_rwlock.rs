//! `rwlock::preemptive` 的集成测试。
//!
//! 这里刻意**只使用 crate 的公开 API**：以 `abs_sync` 的 trait 作为入口驱动
//! 整个锁，用来验证公开表面（类型导出、trait 关联类型与方法）是完整可用的。
//!
//! 注意点：既有实现的阻塞路径（`wait()` / `may_break_with`）是**自旋**
//! 等待，在争用下可能活锁，因此多线程测试一律使用 `try_*` + `yield_now`，
//! 避免整个测试卡死。

use core::sync::atomic::AtomicUsize;
use std::{sync::mpsc, thread, time::Duration};

use atomic_sync::{
    rwlock::preemptive::{
        RwLockError, SpinningRwLock, SpinningRwLockBorrowed, SpinningRwLockOwned,
    },
    x_deps::{
        abs_sync::{
            may_break::TrMayBreak,
            sync_rwlock::{
                TrSyncRwLock, TrSyncRwLockAcqSess, TrSyncUpgradableReaderGuard,
                TrSyncUpgradeSession, TrSyncWriterGuard,
            },
            x_deps::abs_cancel::{CancelledToken, NonCancellableToken},
        },
        atomex::StrictOrderings,
    },
};

type TestLock = SpinningRwLockOwned<usize>;

/// 只用 trait 接口完成一轮：写 → 读 → 可升级读 → 升级为写 → 降级 → 再写 → 读。
///
/// 这个函数对具体锁类型一无所知，因此它编译通过本身就说明
/// `SpinningRwLock` 对 `TrSyncRwLock*` 系列 trait 的实现是完整的。
fn generic_round_trip<L>(lock: &L) -> usize
where
    L: TrSyncRwLock<Target = usize>,
{
    let mut sess = lock.acq_session();

    {
        let mut w = sess.write().wait().unwrap();
        *w += 1;
    }

    let observed = {
        let r = sess.read().wait().unwrap();
        *r
    };

    {
        let upg = sess.upgradable_read().wait().unwrap();
        let mut upg_sess = upg.upgrade_session();
        let mut w = upg_sess.upgrade().wait().unwrap();
        *w += 1;
        let r = w.downgrade_to_reader();
        assert_eq!(*r, observed + 1);
    }

    {
        // 降级得到的读者守卫只能读；要再写必须重新获取写许可。
        let mut w = sess.write().wait().unwrap();
        *w += 1;
    }

    let r = sess.read().wait().unwrap();
    *r
}

/// 测试完全经由 `abs_sync` trait 驱动的一轮读写、升级与降级。
/// - 手段：构造 `SpinningRwLockOwned<usize>`，交给只依赖 trait 的泛型函数驱动。
/// - 判断：返回值等于按步骤累加后的结果，说明各关联类型与方法都可用。
#[test]
fn generic_trait_driven_round_trip() {
    let lock = TestLock::new_owned(0);
    assert_eq!(generic_round_trip(&lock), 3);
    assert_eq!(lock.into_inner(), 3);
}

/// 测试失败与取消两种错误可以被区分（含 `Display` 输出）。
/// - 手段：先在有读者时让 `try_write` 失败，再用已取消的令牌等待读许可。
/// - 判断：分别匹配到 `RwLockError::Retry` 与 `RwLockError::Cancelled`，
///   且 `Display` 输出与变体名一致。
#[test]
fn error_kinds_should_be_distinguishable() {
    let lock = TestLock::new_owned(0);
    let mut acq_r = lock.acquire_session();
    let mut acq_w = lock.acquire_session();

    let r = acq_r.read().wait().unwrap();
    let err = match acq_w.try_write() {
        Ok(_) => panic!("有读者持锁时写尝试不该成功"),
        Err(e) => e,
    };
    assert!(matches!(err, RwLockError::Retry));
    assert_eq!(err.to_string(), "SpinningRwLockError::Retry");
    drop(r);

    let err = match acq_r.read().may_break_with(CancelledToken::new()) {
        Ok(_) => panic!("已取消的令牌不该拿到许可"),
        Err(e) => e,
    };
    assert!(matches!(err, RwLockError::Cancelled));
    assert_eq!(err.to_string(), "SpinningRwLockError::Cancelled");
}

/// 测试永不取消的令牌等价于普通等待。
/// - 手段：空闲锁上用 `NonCancellableToken` 获取读许可。
/// - 判断：获取成功且读到写入值。
#[test]
fn non_cancellable_token_should_behave_like_plain_wait() {
    let lock = TestLock::new_owned(11);
    let mut acq = lock.acquire_session();
    let r = acq
        .read()
        .may_break_with(NonCancellableToken::new())
        .unwrap();
    assert_eq!(*r, 11);
}

/// 测试升级得到的写者释放后，可升级读槽位才归还。
/// - 手段：可升级读 → 升级为写 → 释放写守卫 → 再尝试写。
/// - 判断：写守卫释放后 `reader_count()` 仍为 1 且写尝试失败；
///   升级会话释放后计数归零、写成功。
#[test]
fn upgrade_slot_should_return_after_session_drops() {
    let lock = TestLock::new_owned(0);
    let mut acq_u = lock.acquire_session();
    let mut acq_w = lock.acquire_session();

    let mut upg_sess = acq_u.upgradable_read().wait().unwrap().upgrade_session();
    {
        let mut w = upg_sess.upgrade().wait().unwrap();
        *w += 1;
        assert_eq!(lock.reader_count(), 1);
    }
    assert_eq!(lock.reader_count(), 1, "写者释放后槽位仍被会话持有");
    assert!(acq_w.try_write().is_err());

    drop(upg_sess);
    assert_eq!(lock.reader_count(), 0);
    assert!(acq_w.try_write().is_ok());
}

/// 测试"写者排队"标记会挡住后续新读者，直到有写者完整走一轮。
/// - 手段：用一次失败的 `try_write` 置位标记，释放读者后再让新会话读。
/// - 判断：没有写者持锁、读者计数为 0 时新读者依然失败；写者进入并析构后恢复。
#[test]
fn queued_writer_flag_should_block_new_readers_until_a_writer_passes() {
    let lock = TestLock::new_owned(1);
    let mut acq_r = lock.acquire_session();
    let mut acq_w = lock.acquire_session();
    let mut acq_new = lock.acquire_session();

    let r = acq_r.read().wait().unwrap();
    assert!(acq_w.try_write().is_err(), "有读者时写应失败并置位排队标记");
    drop(r);
    assert_eq!(lock.reader_count(), 0);

    assert!(
        acq_new.try_read().is_err(),
        "排队标记仍在，新读者应被挡住（既有实现的行为）"
    );

    let mut w = acq_w.try_write().unwrap();
    *w += 1;
    drop(w);
    assert_eq!(*acq_new.try_read().unwrap(), 2);
}

/// 测试多个读者可以跨线程同时持有读许可。
/// - 手段：主线程持有读守卫，另一线程用有限次 `try_read` + `yield_now` 尝试，
///   并通过 channel 汇报。
/// - 判断：5 秒内收到成功汇报；读者计数在对方释放后仍为 1。
#[test]
fn readers_should_coexist_across_threads() {
    let lock = TestLock::new_owned(7);
    let mut acq_main = lock.acquire_session();
    let r = acq_main.read().wait().unwrap();

    let (tx, rx) = mpsc::channel();
    let lock_ref = &lock;
    thread::scope(|scope| {
        scope.spawn(move || {
            let mut acq = lock_ref.acquire_session();
            let mut ok = false;
            for _ in 0..1_000_000 {
                if let Ok(g) = acq.try_read() {
                    ok = true;
                    drop(g);
                    break;
                }
                thread::yield_now();
            }
            let _ = tx.send(ok);
        });
        let ok = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("等待跨线程读获取超时");
        assert!(ok, "读者应当能与读者共存");
    });
    assert_eq!(lock.reader_count(), 1);
    drop(r);
}

/// 测试多线程读写混合下不会丢失更新。
/// - 手段：4 个线程各自反复"写一次、读一次"，失败时让出执行权。
/// - 判断：最终值等于全部写入次数；读取值不超过上界。
#[test]
fn multithreaded_updates_should_not_be_lost() {
    const THREADS: usize = 4;
    const ITERS: usize = 200;

    let lock = TestLock::new_owned(0);
    thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                let mut acq = lock.acquire_session();
                for _ in 0..ITERS {
                    loop {
                        if let Ok(mut w) = acq.try_write() {
                            *w += 1;
                            break;
                        }
                        thread::yield_now();
                    }
                    loop {
                        if let Ok(g) = acq.try_read() {
                            assert!(*g <= THREADS * ITERS);
                            break;
                        }
                        thread::yield_now();
                    }
                }
            });
        }
    });
    assert_eq!(lock.reader_count(), 0);
    assert_eq!(lock.into_inner(), THREADS * ITERS);
}

/// 测试用外部原子单元构造的锁可以正常工作。
/// - 手段：栈上准备清零的 `AtomicUsize`，构造 `SpinningRwLockBorrowed`。
/// - 判断：写一次后读者读到的值正确，`into_inner()` 也一致。
#[test]
fn borrowed_cell_lock_should_work() {
    let mut cell = AtomicUsize::new(0);
    let lock: SpinningRwLockBorrowed<'_, usize, AtomicUsize, StrictOrderings> =
        SpinningRwLock::new(5usize, &mut cell);

    let mut acq = lock.acquire_session();
    {
        let mut w = acq.write().wait().unwrap();
        *w += 1;
    }
    assert_eq!(*acq.read().wait().unwrap(), 6);
    assert_eq!(lock.into_inner(), 6);
}
