//! `rwlock::preemptive` 的单元测试。
//!
//! 覆盖状态字迁移、`try_*` 的失败语义、升级路径的记账方式，以及两类
//! 公开的"坑"（写者排队标记的副作用、升级不被排队标记保护）。
//!
//! 错误类型走模块的公开重导出（`preemptive::RwLockError`），
//! 因此这些断言同时验证了导出路径可用。

use std::{
    sync::{mpsc, Barrier},
    thread,
    time::Duration,
};

use atomex::StrictOrderings;
use abs_sync::x_deps::abs_cancel::CancelledToken;

use super::*;

/// 测试新建的锁处于完全空闲状态，且可以取回内部数据。
/// - 手段：用 `new_owned` 构造一个值为常量的锁。
/// - 判断：`reader_count()` 为 0；`into_inner()` 返回最初写入的值。
#[test]
fn rwlock_default_test() {
    const ANSWER: usize = 42;
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(ANSWER);
    assert_eq!(rwlock.reader_count(), 0);
    assert_eq!(rwlock.into_inner(), ANSWER);
}

/// 测试读守卫会阻止写守卫获取。
/// - 手段：会话 A、B 先后取读守卫，再让会话 W 反复 `try_write`。
/// - 判断：只要还有任一读者，写尝试一律返回 `Err`；两个读者都释放后写成功。
#[test]
fn acquire_reader_guard_should_block_acq_writer_guard() {
    const ANSWER: usize = 42;
    const MYSTERY: usize = ANSWER * ANSWER;

    let rwlock = SpinningRwLockOwned::<usize>::new_owned(ANSWER);
    let mut acq_r0 = rwlock.acquire_session();
    let r0 = acq_r0.read().wait().unwrap();
    assert_eq!(*r0, ANSWER);
    assert_eq!(rwlock.reader_count(), 1);

    let mut acq_r1 = rwlock.acquire_session();
    let r1 = acq_r1.read().wait().unwrap();
    assert_eq!(*r1, *r0);
    assert_eq!(rwlock.reader_count(), 2);

    let mut acq_w = rwlock.acquire_session();
    let opt_w = acq_w.try_write();
    assert!(opt_w.is_err());

    drop(opt_w);
    drop(r0);
    assert_eq!(rwlock.reader_count(), 1);

    let opt_w = acq_w.try_write();
    assert!(opt_w.is_err());

    drop(opt_w);
    drop(r1);
    assert_eq!(rwlock.reader_count(), 0);

    let opt_w = acq_w.try_write();
    let mut w = opt_w.unwrap();
    assert_eq!(*w, ANSWER);
    *w = MYSTERY;

    drop(w);
    assert_eq!(rwlock.into_inner(), MYSTERY);
}

/// 测试读守卫会阻止升级，且升级只要求"可升级读者独占"。
/// - 手段：一个普通读者 + 一个可升级读者，先 `try_upgrade` 失败，释放普通读者后重试。
/// - 判断：失败时 `reader_count` 不变且返回原守卫；独占后 `wait()` 升级成功，
///   且升级得到的写者"代表"可升级读槽位（读者计数仍为 1）。
#[test]
fn acquire_reader_guard_should_block_upgrade() {
    const ANSWER: usize = 42;
    const MYSTERY: usize = ANSWER * ANSWER;

    let rwlock = SpinningRwLockOwned::<usize>::new_owned(ANSWER);
    let mut acq_r0 = rwlock.acquire_session();

    let r0 = acq_r0.read().wait().unwrap();
    assert_eq!(*r0, ANSWER);
    assert_eq!(rwlock.reader_count(), 1);

    let mut acq_r1 = rwlock.acquire_session();
    let r1 = acq_r1.upgradable_read().wait().unwrap();
    assert_eq!(*r1, *r0);
    assert_eq!(rwlock.reader_count(), 2);

    let mut upg = r1.upgrade();
    // creating `Upgrade` should not decrease reader count
    assert_eq!(rwlock.reader_count(), 2);
    let opt_u = upg.try_upgrade();
    assert!(opt_u.is_err());

    drop(opt_u);
    drop(r0);
    assert_eq!(rwlock.reader_count(), 1);

    let mut w = upg.upgrade().wait().unwrap();
    // upgraded from an upgradable reader guard will not decrease reader count
    assert_eq!(rwlock.reader_count(), 1);
    assert_eq!(*w, ANSWER);
    *w = MYSTERY;

    drop(w);
    let x = unsafe { *rwlock.as_mut_ptr() };
    assert_eq!(x, MYSTERY);
}

/// 测试 `try_write` 失败会置位"写者排队"标记，并因此挡住后续新读者。
/// - 手段：取一个读守卫让写尝试失败（标记被置位），随后释放该读者，
///   再让一个全新会话尝试 `try_read`。
/// - 判断：即使此刻没有任何写者持锁、读者计数为 0，新读者依然拿不到许可
///   （`Retry`）；直到真的有一个写者进入并析构，标记被清掉，读者才恢复。
///
/// 这条测试记录的是**既有实现的行为**（保护写者不饥饿的代价）：
/// 只要有人尝试过写，读者就要等一个写者走完一轮。它同时也是"取消写尝试
/// 不会收回标记"这一隐患的证据。
#[test]
fn failed_try_write_should_block_later_readers() {
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(1);
    let mut acq_r = rwlock.acquire_session();
    let mut acq_w = rwlock.acquire_session();

    // 让写尝试失败：标记 WRITER_QUEUED 被置位。
    let r = acq_r.read().wait().unwrap();
    assert!(matches!(acq_w.try_write(), Err(RwLockError::Retry)));
    drop(r);
    assert_eq!(rwlock.reader_count(), 0);

    // 没有任何写者持锁，但新读者依然被"排队标记"挡住。
    let mut acq_new = rwlock.acquire_session();
    assert!(matches!(acq_new.try_read(), Err(RwLockError::Retry)));

    // 写者仍然可以进入（`expect_can_write` 不看排队标记），析构后才清掉标记。
    let mut w = acq_w.try_write().unwrap();
    *w += 1;
    drop(w);
    assert_eq!(*acq_new.try_read().unwrap(), 2);
}

/// 测试取消令牌会让等待中的读取立即返回 `Cancelled`。
/// - 手段：让写者持锁，然后用已取消的 `CancelledToken` 调用 `may_break_with`。
/// - 判断：返回 `Err(RwLockError::Cancelled)`，而不是一直自旋。
#[test]
fn may_break_with_cancelled_token_should_return_cancelled() {
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(0);
    let mut acq_hold = rwlock.acquire_session();
    let mut acq_r = rwlock.acquire_session();

    let hold = acq_hold.try_write().unwrap();
    let got = acq_r.read().may_break_with(CancelledToken::new());
    assert!(matches!(got, Err(RwLockError::Cancelled)));
    // `Result` 里含有带 Drop 的守卫类型，会一直占着会话的借用，需显式释放。
    drop(got);
    drop(hold);

    // 取消之后锁仍然可用。
    assert_eq!(*acq_r.read().wait().unwrap(), 0);
}

/// 测试 `wait_or` 在获取成功时返回守卫。
/// - 手段：空闲锁上直接调用 `read().wait_or(|| unreachable!())`。
/// - 判断：拿到守卫且能读到数据；`unreachable!` 分支不应被执行。
#[test]
fn wait_or_should_return_guard_on_success() {
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(9);
    let mut acq = rwlock.acquire_session();
    let g = acq.read().wait_or(|| unreachable!("这里不该走兜底分支"));
    assert_eq!(*g, 9);
}

/// 测试写者守卫的两条降级路径。
/// - 手段：写守卫分别降级为读守卫与可升级读守卫，再各自释放/降级。
/// - 判断：读者计数与可升级标志按预期迁移，最终回到空闲。
#[test]
fn writer_guard_downgrade_paths() {
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(0);
    let mut acq = rwlock.acquire_session();

    let w = acq.try_write().unwrap();
    let r = w.downgrade_to_reader();
    assert_eq!(rwlock.reader_count(), 1);
    drop(r);
    assert_eq!(rwlock.reader_count(), 0);

    let w = acq.try_write().unwrap();
    let upg = w.downgrade_to_upgradable();
    assert_eq!(rwlock.reader_count(), 1);
    let r = upg.downgrade();
    assert_eq!(rwlock.reader_count(), 1);
    drop(r);
    assert_eq!(rwlock.reader_count(), 0);
}

/// 测试升级得到的写者释放后，可升级读槽位仍归 `UpgradeSession` 所有。
/// - 手段：可升级读者升级为写者，先释放写守卫，再释放升级会话。
/// - 判断：写守卫释放后读者计数仍为 1 且写尝试失败（槽位尚未归还）；
///   升级会话释放后计数归零、写尝试成功。
///
/// 这条测试固定住"槽位归 `UpgradeSession` 所有"这一记账方式：升级路径
/// **不**归还读者计数，也不清 `UPGRADE_ACTIVE`，归还发生在可升级读守卫
/// 析构时。cooperative 版本照搬了这套语义，见开发日志 §2.1。
#[test]
fn upgrade_should_keep_slot_until_session_dropped() {
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(0);
    let mut acq_u = rwlock.acquire_session();
    let mut acq_w = rwlock.acquire_session();

    let upg = acq_u.upgradable_read().wait().unwrap();
    let mut upg_sess = upg.upgrade();

    {
        let _w = upg_sess.upgrade().wait().unwrap();
        assert_eq!(rwlock.reader_count(), 1);
    }
    assert_eq!(rwlock.reader_count(), 1, "写者释放后槽位仍在");
    assert!(acq_w.try_write().is_err(), "槽位未归还，写者不该成功");

    drop(upg_sess);
    assert_eq!(rwlock.reader_count(), 0);
    assert!(acq_w.try_write().is_ok());
}

/// 测试升级请求不会挡住新读者（既有实现的已知隐患）。
/// - 手段：可升级读者因存在其他读者而升级失败，此时让第三个会话尝试读。
/// - 判断：新读者**可以**立即拿到读许可。
///
/// 记录的是 §2.2 的隐患：升级路径不置"写者排队"标记，因此在持续有新读者
/// 的负载下，升级可能被无限推迟。cooperative 版本改用"升级请求入队"来
/// 解决，见开发日志 §4.5。
#[test]
fn pending_upgrade_should_not_block_new_readers() {
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(0);
    let mut acq_r = rwlock.acquire_session();
    let mut acq_u = rwlock.acquire_session();
    let mut acq_new = rwlock.acquire_session();

    let r = acq_r.read().wait().unwrap();
    let upg = acq_u.upgradable_read().wait().unwrap();
    let mut upg_sess = upg.upgrade();
    assert!(upg_sess.try_upgrade().is_err(), "还有其他读者，升级应失败");

    // 升级正在等待独占，却完全没有挡住新读者。
    let r_new = acq_new
        .try_read()
        .expect("既有实现下升级请求不阻止新读者");
    assert_eq!(rwlock.reader_count(), 3);
    drop(r_new);
    drop(r);
}

/// 测试借用外部原子单元的构造方式可用。
/// - 手段：在栈上准备一个清零的 `AtomicUsize`，用它构造
///   `SpinningRwLockBorrowed`。
/// - 判断：读写路径正常工作，读者计数与写入值都符合预期。
#[test]
fn borrowed_atomic_cell_should_work() {
    use core::sync::atomic::AtomicUsize;

    let mut cell = AtomicUsize::new(0);
    let lock = SpinningRwLockBorrowed::<usize, AtomicUsize, StrictOrderings>::new(
        5usize, &mut cell,
    );

    let mut acq = lock.acquire_session();
    {
        let mut w = acq.write().wait().unwrap();
        *w += 1;
    }
    assert_eq!(lock.reader_count(), 0);
    assert_eq!(*acq.read().wait().unwrap(), 6);
    assert_eq!(lock.into_inner(), 6);
}

/// 测试多个读者可以同时持有读许可（跨线程）。
/// - 手段：主线程先持有一个读守卫，另一个线程用有限次 `try_read` + `yield_now`
///   尝试获取，并通过 channel 汇报结果。
/// - 判断：在 5 秒内收到"成功"汇报——若读者互斥，对方会一直失败。
#[test]
fn readers_should_coexist_across_threads() {
    let rwlock = SpinningRwLockOwned::<usize>::new_owned(7);
    let mut acq_main = rwlock.acquire_session();
    let r = acq_main.read().wait().unwrap();

    let (tx, rx) = mpsc::channel();
    let lock = &rwlock;
    thread::scope(|scope| {
        scope.spawn(move || {
            let mut acq = lock.acquire_session();
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
    assert_eq!(rwlock.reader_count(), 1);
    drop(r);
}

/// 测试多线程下读者与写者并发时不会丢失更新。
/// - 手段：4 个线程各自反复"写一次、读一次"，获取失败时让出执行权
///   （既有实现的阻塞路径在争用下可能活锁，因此用 `try_*` + `yield_now`）。
/// - 判断：最终值等于全部线程的写入次数。
#[test]
fn multithreaded_readers_and_writers_should_not_lose_updates() {
    const THREADS: usize = 4;
    const ITERS: usize = 200;

    let rwlock = SpinningRwLockOwned::<usize>::new_owned(0);

    // 让所有线程尽量同时开始。
    let barrier = Barrier::new(THREADS);
    thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                let mut acq = rwlock.acquire_session();
                barrier.wait();
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

    assert_eq!(rwlock.reader_count(), 0);
    assert_eq!(rwlock.into_inner(), THREADS * ITERS);
}
