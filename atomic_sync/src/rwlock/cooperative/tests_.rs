//! `rwlock::cooperative` 的行为测试。
//!
//! 全部测试都跑在**真实异步运行时**上：
//!
//! - `#[compio::test]` 是单线程执行器：等待者不会真的并行，唤醒路径、
//!   任务调度与取消都发生在这一个线程上；
//! - `#[tokio::test(flavor = "multi_thread")]` 是多线程执行器：等待者与
//!   释放者会真的并行，用来压跨线程唤醒与内部自旋锁。

use core::future::{Future, IntoFuture};
use std::{
    boxed::Box,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
    vec::Vec,
};

use abs_sync::x_deps::abs_cancel::{CancelledToken, TrMayCancel};

use super::{CoopRwLockError, CooperativeRwLockOwned};

type TestLock = CooperativeRwLockOwned<usize>;

/// 在 `dur` 内驱动 `fut`；超时（`sleep` 先就绪）返回 `None`。
async fn with_timeout<F, S>(fut: F, sleep: S) -> Option<F::Output>
where
    F: IntoFuture,
    S: Future<Output = ()>,
{
    let first = Box::pin(async move { Some(fut.await) });
    let second = Box::pin(async move {
        sleep.await;
        None
    });
    futures_lite::future::race(first, second).await
}

/// 让出执行权，使先前 spawn 的任务跑到它的第一个等待点。
async fn let_tasks_settle() {
    compio::time::sleep(Duration::from_millis(10)).await;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// compio：单线程运行时
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试严格 FIFO：写者排队后新读者不得插队。
/// - 手段：主任务持有写锁，spawn 一个等待写的任务；待它就位后，用
///   "读获取 vs 定时器"的竞速尝试插队。
/// - 判断：竞速必须由定时分支胜出（说明读者被挡住）；释放写锁后写者任务完成。
#[compio::test]
async fn reader_should_not_barge_when_writer_queued() {
    let lock = Arc::new(TestLock::new_owned(0));
    let mut sess_hold = lock.acquire_session();
    let hold = sess_hold.try_write().unwrap();

    let l_w = Arc::clone(&lock);
    let writer = compio::runtime::spawn(async move {
        let mut s = l_w.acquire_session();
        let g = s.write_async().await.unwrap();
        *g
    });
    let_tasks_settle().await;

    let mut sess_read = lock.acquire_session();
    let barged = with_timeout(
        async {
            let _g = sess_read.read_async().await.unwrap();
            true
        },
        compio::time::sleep(Duration::from_millis(20)),
    )
    .await;
    assert_eq!(barged, None, "写者排队期间读者不应插队");

    drop(hold);
    assert_eq!(writer.await.unwrap(), 0);
}

/// 测试队首等待者被取消后，后续等待者不会永久饥饿（活性回归）。
/// - 手段：主任务持有写锁，先 spawn 一个读者任务（排到队首）、再 spawn 一个
///   写者任务；随后丢弃读者任务的 `JoinHandle`（compio 会据此取消该任务），
///   最后释放写锁。
/// - 判断：写者任务必须在超时内完成；若取消路径不做补救，队首会留下永不被
///   唤醒的死节点，写者将一直等下去。
#[compio::test]
async fn cancelled_head_waiter_should_not_starve_followers() {
    let lock = Arc::new(TestLock::new_owned(0));
    let mut sess_hold = lock.acquire_session();
    let hold = sess_hold.try_write().unwrap();

    let l_r = Arc::clone(&lock);
    let reader = compio::runtime::spawn(async move {
        let mut s = l_r.acquire_session();
        let _g = s.read_async().await.unwrap();
    });
    let_tasks_settle().await;

    let l_w = Arc::clone(&lock);
    let writer = compio::runtime::spawn(async move {
        let mut s = l_w.acquire_session();
        let g = s.write_async().await.unwrap();
        *g
    });
    let_tasks_settle().await;

    // 取消队首读者，并给执行器时间真正丢弃它的 future。
    drop(reader);
    let_tasks_settle().await;
    drop(hold);

    let done = with_timeout(
        writer,
        compio::time::sleep(Duration::from_millis(500)),
    )
    .await;
    assert!(
        matches!(done, Some(Ok(_))),
        "队首读者被取消后，写者必须能被放行"
    );
}

/// 测试升级请求插队：可升级读者不会被先前排队的写者挡住。
/// - 手段：主任务持有可升级读与一个普通读；先 spawn 写者任务排队，再释放
///   普通读并发起升级。
/// - 判断：升级必须在超时内完成，且此刻写者尚未进入；释放升级得到的写者
///   与可升级读之后，写者才完成。
#[compio::test]
async fn upgrade_should_jump_ahead_of_queued_writer() {
    let lock = Arc::new(TestLock::new_owned(0));
    let writer_entered = Arc::new(AtomicBool::new(false));

    let mut sess_u = lock.acquire_session();
    let mut sess_r = lock.acquire_session();
    let upg = sess_u.upgradable_read_async().await.unwrap();
    let mut upg_sess = upg.upgrade_session();
    let r = sess_r.read_async().await.unwrap();

    let l_w = Arc::clone(&lock);
    let flag = Arc::clone(&writer_entered);
    let writer = compio::runtime::spawn(async move {
        let mut s = l_w.acquire_session();
        let _g = s.write_async().await.unwrap();
        flag.store(true, Ordering::SeqCst);
    });
    let_tasks_settle().await;

    drop(r);
    let upgraded = with_timeout(
        upg_sess.upgrade_async(),
        compio::time::sleep(Duration::from_millis(500)),
    )
    .await;
    let w = upgraded.expect("升级被排队的写者挡住了").unwrap();
    assert!(
        !writer_entered.load(Ordering::SeqCst),
        "升级必须先于排队的写者"
    );

    drop(w);
    drop(upg_sess);
    let done = with_timeout(
        writer,
        compio::time::sleep(Duration::from_millis(500)),
    )
    .await;
    assert!(matches!(done, Some(Ok(()))), "可升级读归还后写者应能进入");
    assert!(writer_entered.load(Ordering::SeqCst));
}

/// 测试升级得到的写者释放后会回退成原本的可升级读。
/// - 手段：升级写入数据、释放写守卫，然后再次升级、最后取回可升级读守卫。
/// - 判断：首次释放后读者计数仍为 1（槽位仍被会话持有）且能再次升级；
///   `into_guard` 取回守卫后仍能读到最后写入的值；释放后计数归零。
#[compio::test]
async fn upgraded_writer_should_fall_back_to_upgradable() {
    let lock = TestLock::new_owned(0);
    let mut sess = lock.acquire_session();
    let upg = sess.upgradable_read_async().await.unwrap();
    let mut upg_sess = upg.upgrade_session();

    {
        let mut w = upg_sess.upgrade_async().await.unwrap();
        *w = 5;
    }
    assert_eq!(lock.reader_count(), 1, "写者释放后应回退为可升级读");
    match upg_sess.try_upgrade() {
        Ok(w) => assert_eq!(*w, 5, "回退后仍能读到刚才写入的值"),
        Err(_) => panic!("独占状态下应能再次升级"),
    }

    {
        let mut w = upg_sess.upgrade_async().await.unwrap();
        *w = 6;
    }
    let g = upg_sess.into_guard();
    assert_eq!(*g, 6);
    drop(g);
    assert_eq!(lock.reader_count(), 0);
}

/// 测试同质合并的读者会被整组放行。
/// - 手段：写者持有时 spawn 3 个读者任务排队，然后释放写者。
/// - 判断：3 个任务全部完成并读到最新值，读者计数回到 0。
#[compio::test]
async fn coalesced_readers_should_all_be_admitted() {
    let lock = Arc::new(TestLock::new_owned(7));
    let mut sess_hold = lock.acquire_session();
    let hold = sess_hold.try_write().unwrap();

    let mut tasks = Vec::new();
    for _ in 0..3 {
        let l = Arc::clone(&lock);
        tasks.push(compio::runtime::spawn(async move {
            let mut s = l.acquire_session();
            let g = s.read_async().await.unwrap();
            *g
        }));
    }
    let_tasks_settle().await;
    drop(hold);

    for t in tasks {
        assert_eq!(t.await.unwrap(), 7);
    }
    assert_eq!(lock.reader_count(), 0);
}

/// 测试"队列里只有读者"时新读者不被拖进队列（本次公平性调整的核心语义）。
/// - 手段：主任务持有写锁；spawn 一个读者任务使其入队；释放写锁后**不 await**，
///   立刻用一个新会话探测队列状态并尝试同步读。
/// - 判断：此刻队列仍非空、且没有写者排队，`try_read()` 必须成功；旧实现会
///   因为"队列非空"而返回 `WouldBlock`。
#[compio::test]
async fn reader_should_barge_when_only_readers_queued() {
    let lock = Arc::new(TestLock::new_owned(0));
    let mut sess_hold = lock.acquire_session();
    let hold = sess_hold.try_write().unwrap();

    let l_r = Arc::clone(&lock);
    let reader = compio::runtime::spawn(async move {
        let mut s = l_r.acquire_session();
        let g = s.read_async().await.unwrap();
        *g
    });
    let_tasks_settle().await;

    drop(hold);
    // 尚未让出执行权：读者任务已 `Admitted` 但还没被 poll，队列仍然非空。
    let mut sess_probe = lock.acquire_session();
    assert!(
        sess_probe.core().waiters_present(),
        "读者应仍留在队列中（快路径探测的前提）"
    );
    assert!(!sess_probe.core().writer_queued(), "队列里只有读者");
    let barged = sess_probe.try_read();
    assert!(barged.is_ok(), "只有读者排队时，新读者不应被拖进队列");
    drop(barged);

    assert_eq!(reader.await.unwrap(), 0);
}

/// 测试写者不会被持续到达的读者饿死（公平性是硬性要求）。
/// - 手段：主任务持有一个读守卫；spawn 写者任务使其入队；主任务连续尝试新读者
///   （写者排队期间必须全部失败，即屏障生效），随后释放读守卫。
/// - 判断：写者任务必须在超时内完成；若屏障失效，持续到达的读者会把它饿死。
#[compio::test]
async fn writer_should_not_starve_while_readers_keep_arriving() {
    let lock = Arc::new(TestLock::new_owned(0));
    let mut sess_r = lock.acquire_session();
    let r = sess_r.read_async().await.unwrap();

    let l_w = Arc::clone(&lock);
    let writer = compio::runtime::spawn(async move {
        let mut s = l_w.acquire_session();
        let g = s.write_async().await.unwrap();
        *g
    });
    let_tasks_settle().await;

    // 写者已排队：新读者必须被挡住，这是"写者屏障"生效的直接证据。
    let mut probe = lock.acquire_session();
    for _ in 0..64 {
        assert!(probe.try_read().is_err(), "写者排队期间读者不得插队");
    }

    drop(r);
    let done = with_timeout(
        writer,
        compio::time::sleep(Duration::from_millis(500)),
    )
    .await;
    assert!(matches!(done, Some(Ok(0))), "既有读者排空后写者必须进入");
}

/// 测试升级请求排队时同样挡住新读者（升级计入写者屏障）。
/// - 手段：一个可升级读者加一个普通读者；在可升级读者上发起异步升级，手动
///   `poll` 一次让它入队（此刻因还有别的读者而无法立即成功）。
/// - 判断：升级排队期间 `writer_queued` 为真，且新读者的 `try_read` 失败；
///   释放普通读者后升级完成。
#[compio::test]
async fn queued_upgrade_should_block_new_readers() {
    let lock = TestLock::new_owned(0);
    let mut sess_u = lock.acquire_session();
    let mut sess_r = lock.acquire_session();
    let upg = sess_u.upgradable_read_async().await.unwrap();
    let mut upg_sess = upg.upgrade_session();
    let r = sess_r.read_async().await.unwrap();

    let mut upgrade_fut = Box::pin(upg_sess.upgrade_async().into_future());
    let first = core::future::poll_fn(|cx| match upgrade_fut.as_mut().poll(cx) {
        core::task::Poll::Pending => core::task::Poll::Ready(None),
        core::task::Poll::Ready(v) => core::task::Poll::Ready(Some(v)),
    })
    .await;
    assert!(first.is_none(), "还有别的读者时升级必须排队");
    // `first` 的类型携带 `upg_sess` 的可变借用，先丢弃它才能交还会话。
    drop(first);

    let mut probe = lock.acquire_session();
    assert!(
        probe.core().writer_queued(),
        "升级入队必须置位 WRITER_QUEUED"
    );
    assert!(probe.try_read().is_err(), "升级排队期间新读者不得插队");

    drop(r);
    let w = upgrade_fut.await.unwrap();
    drop(w);
    drop(upg_sess);
}

/// 测试取消写者等待会收回 `WRITER_QUEUED`，读者恢复快速路径。
/// - 手段：主任务持有写锁；spawn 一个写者任务使其入队；丢弃其 `JoinHandle`
///   让 compio 取消该任务，然后探测标记并释放写锁。
/// - 判断：取消后 `writer_queued` 为假、`try_read` 成功；若取消不收回标记，
///   读者会被一个已不存在的等待者永久挡住。
#[compio::test]
async fn cancelled_writer_wait_should_release_barrier() {
    let lock = Arc::new(TestLock::new_owned(0));
    let mut sess_hold = lock.acquire_session();
    let hold = sess_hold.try_write().unwrap();

    let l_w = Arc::clone(&lock);
    let writer = compio::runtime::spawn(async move {
        let mut s = l_w.acquire_session();
        let _g = s.write_async().await.unwrap();
    });
    let_tasks_settle().await;

    let mut probe = lock.acquire_session();
    assert!(probe.core().writer_queued(), "写者应已入队并置位");

    drop(writer);
    let_tasks_settle().await;
    assert!(
        !probe.core().writer_queued(),
        "取消后必须收回 WRITER_QUEUED"
    );

    drop(hold);
    assert!(probe.try_read().is_ok(), "取消的写者不应再挡住读者");
}

/// 测试取消令牌触发时异步获取返回 `Cancelled`，且锁状态保持自洽。
/// - 手段：持有写守卫，用已取消的 `CancelledToken` 去获取读；随后释放写守卫。
/// - 判断：获取返回 `Err(Cancelled)`；释放后普通读获取成功。
#[compio::test]
async fn cancelled_token_should_abort_acquire_cleanly() {
    let lock = TestLock::new_owned(3);
    let mut sess_hold = lock.acquire_session();
    let mut sess = lock.acquire_session();

    let hold = sess_hold.try_write().unwrap();
    let r = sess.read_async().may_cancel_with(CancelledToken::new()).await;
    assert!(matches!(r, Result::Err(CoopRwLockError::Cancelled)));
    drop(r);

    drop(hold);
    assert_eq!(*sess.read_async().await.unwrap(), 3);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// tokio：多线程运行时
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试多线程下读者与写者不会丢失更新。
/// - 手段：4 个 tokio 任务各自反复"写一次、读一次"，运行在多线程执行器上。
/// - 判断：最终值等于总写入次数，且过程中读到的值不超过上界。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multithreaded_writers_should_not_lose_updates() {
    const TASKS: usize = 4;
    const ITERS: usize = 100;

    let lock = Arc::new(TestLock::new_owned(0));
    let mut tasks = Vec::new();
    for _ in 0..TASKS {
        let l = Arc::clone(&lock);
        tasks.push(tokio::spawn(async move {
            for _ in 0..ITERS {
                let mut s = l.acquire_session();
                let mut w = s.write_async().await.unwrap();
                *w += 1;
                drop(w);

                let r = s.read_async().await.unwrap();
                assert!(*r <= TASKS * ITERS);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let mut s = lock.acquire_session();
    let r = s.read_async().await.unwrap();
    assert_eq!(*r, TASKS * ITERS);
}

/// 测试多线程下写者的互斥性与读者的共享性。
/// - 手段：若干读任务与写任务并发运行，用两个原子计数记录"当前有多少读者 /
///   写者已经进入临界区"，进入时互相断言。
/// - 判断：写者进入时读者计数必须为 0，读者进入时写者计数必须为 0。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_exclusion_should_hold_under_contention() {
    const READERS: usize = 3;
    const WRITERS: usize = 2;
    const ITERS: usize = 200;

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

/// 测试超时取消等待后不会把锁弄坏（tokio 侧取消路径）。
/// - 手段：持有写守卫时用 `tokio::time::timeout` 等待写许可，令其超时被丢弃；
///   随后释放写守卫并再次获取。
/// - 判断：超时返回 `Err`；之后读获取能在超时内成功，读到正确的值。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_cancelled_wait_should_not_wedge_lock() {
    let lock = TestLock::new_owned(1);
    let mut sess_hold = lock.acquire_session();
    let mut sess = lock.acquire_session();

    let hold = sess_hold.try_write().unwrap();
    let timed_out =
        tokio::time::timeout(Duration::from_millis(50), sess.write_async())
            .await;
    assert!(timed_out.is_err(), "持锁期间等待写许可应当超时");
    // `Result`/`Option` 里含有带 Drop 的守卫类型，会一直占着会话的借用。
    drop(timed_out);

    drop(hold);
    let got = tokio::time::timeout(
        Duration::from_millis(500),
        sess.read_async(),
    )
    .await
    .expect("取消之后锁必须仍然可用")
    .unwrap();
    assert_eq!(*got, 1);
}

/// 测试多线程下可升级读者的升级必须是排他且能反复回退。
/// - 手段：一个任务反复"可升级读 → 升级为写 → 释放回退"，同时另有读任务竞争；
///   升级成功期间用原子计数断言没有其他读者。
/// - 判断：升级成功的那一刻读者计数（外部记账）必须归零，且循环能正常结束。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgradable_reader_should_promote_under_contention() {
    const ITERS: usize = 50;

    let lock = Arc::new(TestLock::new_owned(0));
    let inside_readers = Arc::new(AtomicUsize::new(0));

    let l_r = Arc::clone(&lock);
    let ir_r = Arc::clone(&inside_readers);
    let readers = tokio::spawn(async move {
        for _ in 0..ITERS * 4 {
            let mut s = l_r.acquire_session();
            let _g = s.read_async().await.unwrap();
            ir_r.fetch_add(1, Ordering::SeqCst);
            core::hint::spin_loop();
            ir_r.fetch_sub(1, Ordering::SeqCst);
        }
    });

    let l_u = Arc::clone(&lock);
    let ir_u = Arc::clone(&inside_readers);
    let upgrader = tokio::spawn(async move {
        for _ in 0..ITERS {
            let mut s = l_u.acquire_session();
            let upg = s.upgradable_read_async().await.unwrap();
            let mut upg_sess = upg.upgrade_session();
            // 可升级读本身也是一个读者。
            ir_u.fetch_add(1, Ordering::SeqCst);

            let mut w = upg_sess.upgrade_async().await.unwrap();
            // 升级成功即独占：此刻不应有其他读者。
            ir_u.fetch_sub(1, Ordering::SeqCst);
            assert_eq!(
                ir_u.load(Ordering::SeqCst),
                0,
                "升级成功时必须是独占的"
            );
            *w += 1;
            drop(w);
            // 回退为可升级读。
            ir_u.fetch_add(1, Ordering::SeqCst);

            drop(upg_sess);
            ir_u.fetch_sub(1, Ordering::SeqCst);
        }
    });

    readers.await.unwrap();
    upgrader.await.unwrap();
    assert_eq!(inside_readers.load(Ordering::SeqCst), 0);

    let mut s = lock.acquire_session();
    assert_eq!(*s.read_async().await.unwrap(), ITERS);
}
