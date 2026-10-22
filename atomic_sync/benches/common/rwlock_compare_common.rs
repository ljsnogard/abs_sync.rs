// 两套 cooperative 读写锁的双实现对比基准（共用源）。
//
// 本文件不单独编译，而是由 `rwlock_compare_heap.rs` / `rwlock_compare_arena.rs`
// 通过 `include!` 引入。两个 target 各自定义：
//
// - `impl_mod`：被测实现模块（`cooperative` 或 `cooperative_arena`）；
// - `TestLock`：该实现的 `CooperativeRwLockOwned<u64>`；
// - `IMPL_NAME`：输出里标识实现的短名。
//
// 这样同一份负载源码会分别编译进两个二进制，避免"临时改 import 再还原"带
// 来的不可复现性，也避免复制两份大段 bench 源码。
//
// # 驱动方式
//
// 除 `fastpath` 外的场景都用**单线程手动 poll**驱动：先 poll 一次让等待者
// 入队（写者仍持有），随后精确记录"释放→领取→排空"各阶段耗时。手动 poll
// 使用 noop waker，因此唤醒本身不引入运行时调度噪声；需要观察真实运行时
// 调度时另跑 compio 单线程小节作为 sanity check。
//
// # 输出
//
// 逐行输出 CSV：
//
// ```text
// RES,<impl>,<scenario>,<n>,<round>,<phase>,<value_ns>
// ```
//
// `phase` 为 `total` / `release` / `acquire` / `drop` / `enqueue` 等。
// `per_waiter` 行是总耗时除以等待者数量。分析脚本按 scenario+n+phase 聚合
// 10 轮样本，取中位数与 p95/p99（轮级）；n <= 256 的场景额外输出 `lat_p95`
// / `lat_p99`（等待者级，最后一轮）。

use core::{
    future::{Future, IntoFuture},
    hint::{black_box, spin_loop},
    pin::Pin,
    task::{Context, Poll, Waker},
};
use std::{
    env,
    fmt,
    io::Write,
    sync::{
        Arc, Barrier,
        atomic::{
            AtomicBool as StdAtomicBool, AtomicUsize as StdAtomicUsize,
            Ordering as StdOrdering,
        },
        mpsc,
    },
    thread,
    time::Instant,
};

use atomic_sync::x_deps::atomex::StrictOrderings;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 具体类型别名（两套实现的泛型参数完全一致）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

type RFut<'a, 'g> =
    impl_mod::ReadAcquireFuture<'a, 'g, u64, usize, StdAtomicUsize, StrictOrderings>;
type WFut<'a, 'g> =
    impl_mod::WriteAcquireFuture<'a, 'g, u64, usize, StdAtomicUsize, StrictOrderings>;
type RGuard<'a, 'g> =
    impl_mod::ReaderGuard<'a, 'g, u64, usize, StdAtomicUsize, StrictOrderings>;

/// 混合队列场景里存放两种等待者 future 的容器。
///
/// 两个变体共享同一对生命周期：`'a` 是锁的借用，`'g` 是 `&mut Session` 的
/// 借用。放进同一个 `Vec` 才能保持与真实入队顺序一致的 poll 顺序。
enum AnyFut<'a, 'g> {
    Read(Pin<Box<RFut<'a, 'g>>>),
    Write(Pin<Box<WFut<'a, 'g>>>),
}

/// 手动 poll 时允许的最大连续 `Pending` 次数。
///
/// noop waker 不会被真正唤醒，因此被放行的等待者必须在下一次 poll 就完成；
/// 超过这个次数说明实现没有按预期推进，直接 panic 而不是挂起。
const MAX_SPIN: usize = 1 << 22;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 配置
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

struct Cfg {
    scenarios_: Vec<String>,
    rounds_: usize,
    ops_: usize,
    hold_: usize,
    ns_: Vec<usize>,
    cancel_pcts_: Vec<usize>,
    cancel_k_: usize,
}

fn env_usize_(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_list_(key: &str, default: &[usize]) -> Vec<usize> {
    match env::var(key) {
        Ok(v) => v
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect(),
        Err(_) => default.to_vec(),
    }
}

fn env_scenarios_() -> Vec<String> {
    match env::var("RWLCMP_SCENARIOS") {
        Ok(v) => v
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect(),
        Err(_) => vec![
            "fastpath".to_owned(),
            "reader-wake".to_owned(),
            "reader-release".to_owned(),
            "deep".to_owned(),
            "deep-mixed".to_owned(),
            "cancel".to_owned(),
            "upgrade".to_owned(),
            "cold".to_owned(),
            "memory".to_owned(),
            "concurrent".to_owned(),
        ],
    }
}

impl Cfg {
    fn from_env_() -> Self {
        Cfg {
            scenarios_: env_scenarios_(),
            rounds_: env_usize_("RWLCMP_ROUNDS", 10),
            ops_: env_usize_("RWLCMP_OPS", 200_000),
            hold_: env_usize_("RWLCMP_HOLD", 0),
            ns_: env_list_(
                "RWLCMP_NS",
                &[1, 2, 4, 8, 16, 31, 32, 33, 64, 128, 512, 1024, 2048, 4096],
            ),
            cancel_pcts_: env_list_("RWLCMP_CANCEL", &[0, 25, 50, 75, 100]),
            cancel_k_: env_usize_("RWLCMP_CANCEL_K", 512),
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 统计与输出
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn pct_(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() - 1) as f64) * p / 100.0).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn median_(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut v = values.to_vec();
    v.sort_unstable();
    pct_(&v, 50.0)
}

fn emit_(scenario: &str, n: usize, round: usize, phase: &str, value: u64) {
    println!("RES,{IMPL_NAME},{scenario},{n},{round},{phase},{value}");
}

fn emit_lat_(scenario: &str, n: usize, round: usize, lat: &[u64]) {
    if lat.is_empty() {
        return;
    }
    let mut v = lat.to_vec();
    v.sort_unstable();
    emit_(scenario, n, round, "lat_p50", pct_(&v, 50.0));
    emit_(scenario, n, round, "lat_p95", pct_(&v, 95.0));
    emit_(scenario, n, round, "lat_p99", pct_(&v, 99.0));
    emit_(scenario, n, round, "lat_max", pct_(&v, 100.0));
}

fn progress_(msg: impl fmt::Display) {
    eprintln!("  ... {msg}");
    let _ = std::io::stderr().flush();
}

/// 预热一把刚创建的新锁：让一个写者与一个读者完整走一遍排队/放行/领取/释放。
///
/// 目的是把 Arena 首次入队时的 chunk 分配移出测量区间。`cold` 场景**不**调用
/// 它，因为那里要测的正是首次竞争成本。
///
/// `Box::pin` 在这里刻意保留：future 要与会话借用解耦以便跨语句持有；
/// 预热本身在计时区间之外，堆分配不影响测量结果。
#[allow(clippy::unnecessary_box_pin)]
fn warm_lock_(lock: &TestLock, cx: &mut Context<'_>) {
    let mut ws = lock.acquire_session();
    let w = ws.try_write().expect("warm writer");
    let mut rs = lock.acquire_session();
    let mut fut = Box::pin(rs.read_async().into_future());
    match fut.as_mut().poll(cx) {
        Poll::Pending => {}
        Poll::Ready(_) => panic!("warm: reader 提前领取"),
    }
    drop(w);
    let mut spins = 0usize;
    let g = loop {
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(g)) => break g,
            Poll::Ready(Err(e)) => panic!("warm: 领取失败 {e:?}"),
            Poll::Pending => {
                spins += 1;
                if spins > MAX_SPIN {
                    panic!("warm: 未放行");
                }
                spin_loop();
            }
        }
    };
    drop(g);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 A：fastpath（单线程无竞争）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_fastpath_(cfg: &Cfg) {
    let ops = cfg.ops_;
    for round in 0..cfg.rounds_ {
        let lock = TestLock::new_owned(0u64);
        let mut sess = lock.acquire_session();
        // 读快速路径。
        let t = Instant::now();
        for _ in 0..ops {
            let g = sess.try_read().expect("try_read");
            black_box(*g);
            drop(g);
        }
        let read_ns = t.elapsed().as_nanos() as u64;

        // 写快速路径。
        let t = Instant::now();
        let mut acc = 0u64;
        for _ in 0..ops {
            let g = sess.try_write().expect("try_write");
            acc = acc.wrapping_add(*g);
            drop(g);
        }
        black_box(acc);
        let write_ns = t.elapsed().as_nanos() as u64;

        emit_("fastpath", 1, round, "read_ns_per_op", read_ns / ops as u64);
        emit_(
            "fastpath",
            1,
            round,
            "write_ns_per_op",
            write_ns / ops as u64,
        );
        emit_(
            "fastpath",
            1,
            round,
            "read_min_ns",
            read_ns / ops as u64,
        );
        emit_("fastpath", 1, round, "write_min_ns", write_ns / ops as u64);
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 B：reader-wake（写者持锁排队 N 个读者，释放后排空）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_reader_wake_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    for &n in &cfg.ns_ {
        progress_(format!("reader-wake N={n}"));
        let mut total_samples = Vec::with_capacity(cfg.rounds_);
        for round in 0..cfg.rounds_ {
            let lock = TestLock::new_owned(0u64);
            warm_lock_(&lock, &mut cx);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");

            // 读者各自持有独立会话；future 借用会话，会话借用锁。这里必须
            // 把 sessions 和 futs 放在同一作用域，让借用检查器看到
            // "futs 先于 sessions 结束"，因此不能抽成返回 futs 的辅助函数。
            let mut sessions: Vec<_> = (0..n)
                .map(|_| Box::new(lock.acquire_session()))
                .collect();
            let mut futs = Vec::with_capacity(n);
            let mut enq_ns = 0u64;
            for s in sessions.iter_mut() {
                let mut fut = Box::pin(s.read_async().into_future());
                let t = Instant::now();
                match fut.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => {
                        panic!("reader 在写者持锁时直接领取了许可（n={n}）");
                    }
                    Poll::Ready(Err(e)) => panic!("reader 入队失败：{e:?}"),
                }
                enq_ns = enq_ns.saturating_add(t.elapsed().as_nanos() as u64);
                futs.push(fut);
            }

            let t_drain = Instant::now();
            drop(w);
            let rel_ns = t_drain.elapsed().as_nanos() as u64;

            let mut guards: Vec<RGuard<'_, '_>> = Vec::with_capacity(n);
            let mut lat: Vec<u64> = Vec::new();
            let mut spins = 0usize;
            for f in futs.iter_mut() {
                loop {
                    match f.as_mut().poll(&mut cx) {
                        Poll::Ready(Ok(g)) => {
                            guards.push(g);
                            lat.push(t_drain.elapsed().as_nanos() as u64);
                            break;
                        }
                        Poll::Ready(Err(e)) => panic!("reader 领取失败：{e:?}"),
                        Poll::Pending => {
                            spins += 1;
                            if spins > MAX_SPIN {
                                panic!("reader 释放后仍未被放行（n={n}）");
                            }
                            spin_loop();
                        }
                    }
                }
            }
            // `drain` 是从释放写者到全部读者领取完成的墙钟；`acquire` 只算
            // 读者 poll 领取本身（不含 release）。
            let drain_ns = t_drain.elapsed().as_nanos() as u64;
            let acq_ns = drain_ns.saturating_sub(rel_ns);

            let t_drop = Instant::now();
            drop(guards);
            let drop_ns = t_drop.elapsed().as_nanos() as u64;

            total_samples.push(drain_ns);
            emit_("reader-wake", n, round, "enqueue", enq_ns);
            emit_("reader-wake", n, round, "release", rel_ns);
            emit_("reader-wake", n, round, "acquire", acq_ns);
            emit_("reader-wake", n, round, "drain", drain_ns);
            emit_("reader-wake", n, round, "drop", drop_ns);
            emit_(
                "reader-wake",
                n,
                round,
                "per_waiter",
                drain_ns / n as u64,
            );
            if n <= 256 {
                emit_lat_("reader-wake", n, round, &lat);
            }
        }
        progress_(format!(
            "reader-wake N={n} 中位 drain = {} ns",
            median_(&total_samples)
        ));
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 C：reader-release（N 个读者领取后依次释放）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_reader_release_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    for &n in &cfg.ns_ {
        // 这个场景只关心释放，跳过明显过大的 N 以免运行时间失控。
        if n > 64 {
            continue;
        }
        progress_(format!("reader-release N={n}"));
        let mut samples = Vec::with_capacity(cfg.rounds_);
        for round in 0..cfg.rounds_ {
            let lock = TestLock::new_owned(0u64);
            warm_lock_(&lock, &mut cx);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");

            let mut sessions: Vec<_> = (0..n)
                .map(|_| Box::new(lock.acquire_session()))
                .collect();
            let mut futs = Vec::with_capacity(n);
            for s in sessions.iter_mut() {
                let mut fut = Box::pin(s.read_async().into_future());
                match fut.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => {
                        panic!("reader 在写者持锁时直接领取了许可（n={n}）");
                    }
                    Poll::Ready(Err(e)) => panic!("reader 入队失败：{e:?}"),
                }
                futs.push(fut);
            }
            drop(w);

            let mut guards: Vec<RGuard<'_, '_>> = Vec::with_capacity(n);
            for f in futs.iter_mut() {
                let mut spins = 0usize;
                loop {
                    match f.as_mut().poll(&mut cx) {
                        Poll::Ready(Ok(g)) => {
                            guards.push(g);
                            break;
                        }
                        Poll::Ready(Err(e)) => panic!("reader 领取失败：{e:?}"),
                        Poll::Pending => {
                            spins += 1;
                            if spins > MAX_SPIN {
                                panic!("reader 未被放行（n={n}）");
                            }
                            spin_loop();
                        }
                    }
                }
            }

            let t = Instant::now();
            drop(guards);
            let release_ns = t.elapsed().as_nanos() as u64;

            samples.push(release_ns);
            emit_("reader-release", n, round, "release_all", release_ns);
            emit_(
                "reader-release",
                n,
                round,
                "per_waiter",
                release_ns / n as u64
            );
        }
        progress_(format!(
            "reader-release N={n} 中位 = {} ns",
            median_(&samples)
        ));
    }
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 D：deep-queue（深队列入队 → 释放 → 排空 → 回收）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读取 `/proc/self/status` 中的某个 KB 字段（如 `VmRSS` / `VmHWM`）。
///
/// 取不到时返回 0；这只影响报告里的内存列，不影响计时。
fn rss_kb_(key: &str) -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(key)
            && let Some(rest) = rest.strip_prefix(':')
        {
            let kb = rest.trim().trim_end_matches(" kB").trim();
            return kb.parse().unwrap_or(0);
        }
    }
    0
}

fn scenario_deep_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    for &n in &cfg.ns_ {
        progress_(format!("deep N={n}"));
        for round in 0..cfg.rounds_ {
            let rss_base = rss_kb_("VmRSS");
            let lock = TestLock::new_owned(0u64);
            warm_lock_(&lock, &mut cx);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");

            let mut sessions: Vec<_> = (0..n)
                .map(|_| Box::new(lock.acquire_session()))
                .collect();
            let mut futs = Vec::with_capacity(n);
            let t_enq = Instant::now();
            for s in sessions.iter_mut() {
                let mut fut = Box::pin(s.read_async().into_future());
                match fut.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => panic!("deep: reader 提前领取（n={n}）"),
                    Poll::Ready(Err(e)) => panic!("deep: 入队失败 {e:?}"),
                }
                futs.push(fut);
            }
            let enq_ns = t_enq.elapsed().as_nanos() as u64;
            let rss_peak = rss_kb_("VmRSS");
            let hwm_peak = rss_kb_("VmHWM");

            let t_drain = Instant::now();
            drop(w);
            let t_release_ns = t_drain.elapsed().as_nanos() as u64;
            let mut guards: Vec<RGuard<'_, '_>> = Vec::with_capacity(n);
            let mut spins = 0usize;
            for f in futs.iter_mut() {
                loop {
                    match f.as_mut().poll(&mut cx) {
                        Poll::Ready(Ok(g)) => {
                            guards.push(g);
                            break;
                        }
                        Poll::Ready(Err(e)) => panic!("deep: 领取失败 {e:?}"),
                        Poll::Pending => {
                            spins += 1;
                            if spins > MAX_SPIN {
                                panic!("deep: 未放行（n={n}）");
                            }
                            spin_loop();
                        }
                    }
                }
            }
            let drain_ns = t_drain.elapsed().as_nanos() as u64;
            let acq_ns = drain_ns.saturating_sub(t_release_ns);

            let t_drop = Instant::now();
            drop(guards);
            let drop_ns = t_drop.elapsed().as_nanos() as u64;
            let rss_after = rss_kb_("VmRSS");
            let hwm_after = rss_kb_("VmHWM");

            emit_("deep", n, round, "enqueue", enq_ns);
            emit_("deep", n, round, "release", t_release_ns);
            emit_("deep", n, round, "acquire", acq_ns);
            emit_("deep", n, round, "drain", drain_ns);
            emit_("deep", n, round, "drop", drop_ns);
            emit_("deep", n, round, "total", enq_ns + drain_ns + drop_ns);
            emit_("deep", n, round, "per_waiter", drain_ns / n as u64);
            emit_("deep", n, round, "rss_base_kb", rss_base);
            emit_("deep", n, round, "rss_peak_kb", rss_peak);
            emit_("deep", n, round, "hwm_peak_kb", hwm_peak);
            emit_("deep", n, round, "rss_after_kb", rss_after);
            emit_("deep", n, round, "hwm_after_kb", hwm_after);
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 E：cancel-storm（排队 K 个读者，取消一部分后再释放）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_cancel_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let k = cfg.cancel_k_;

    for &pct in &cfg.cancel_pcts_ {
        progress_(format!("cancel K={k} pct={pct}%"));
        let cancel_n = k * pct / 100;
        for round in 0..cfg.rounds_ {
            let lock = TestLock::new_owned(0u64);
            warm_lock_(&lock, &mut cx);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");

            let mut sessions: Vec<_> = (0..k)
                .map(|_| Box::new(lock.acquire_session()))
                .collect();
            let mut futs: Vec<Option<_>> = Vec::with_capacity(k);
            for s in sessions.iter_mut() {
                let mut fut = Box::pin(s.read_async().into_future());
                match fut.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => panic!("cancel: reader 提前领取"),
                    Poll::Ready(Err(e)) => panic!("cancel: 入队失败 {e:?}"),
                }
                futs.push(Some(fut));
            }

            let t_cancel = Instant::now();
            for slot in futs.iter_mut().take(cancel_n) {
                *slot = None;
            }
            let cancel_ns = t_cancel.elapsed().as_nanos() as u64;

            let t_drain = Instant::now();
            drop(w);
            let mut guards: Vec<RGuard<'_, '_>> = Vec::with_capacity(k - cancel_n);
            let mut spins = 0usize;
            for f in futs.iter_mut() {
                let Some(fut) = f.as_mut() else {
                    continue;
                };
                loop {
                    match fut.as_mut().poll(&mut cx) {
                        Poll::Ready(Ok(g)) => {
                            guards.push(g);
                            break;
                        }
                        Poll::Ready(Err(e)) => panic!("cancel: 领取失败 {e:?}"),
                        Poll::Pending => {
                            spins += 1;
                            if spins > MAX_SPIN {
                                panic!("cancel: 未放行（pct={pct}）");
                            }
                            spin_loop();
                        }
                    }
                }
            }
            let drain_ns = t_drain.elapsed().as_nanos() as u64;
            let t_drop = Instant::now();
            drop(guards);
            let drop_ns = t_drop.elapsed().as_nanos() as u64;

            emit_("cancel", k, round, "cancel_ns", cancel_ns);
            emit_("cancel", k, round, "drain", drain_ns);
            emit_("cancel", k, round, "drop", drop_ns);
            emit_("cancel", k, round, "total", cancel_ns + drain_ns + drop_ns);
            emit_("cancel", k, round, "cancel_pct", pct as u64);
            emit_("cancel", k, round, "cancel_count", cancel_n as u64);
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 F：cold-start（反复创建新锁，首次竞争）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_cold_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let k = env_usize_("RWLCMP_COLD_K", 2000);
    let sample = k.min(4096);

    for round in 0..cfg.rounds_ {
        let mut samples = Vec::with_capacity(sample);
        for i in 0..k {
            let t = Instant::now();
            let lock = TestLock::new_owned(0u64);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");
            let mut rs = lock.acquire_session();
            let mut fut = Box::pin(rs.read_async().into_future());
            match fut.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(_)) => panic!("cold: reader 提前领取"),
                Poll::Ready(Err(e)) => panic!("cold: 入队失败 {e:?}"),
            }
            drop(w);
            let mut spins = 0usize;
            let g = loop {
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(g)) => break g,
                    Poll::Ready(Err(e)) => panic!("cold: 领取失败 {e:?}"),
                    Poll::Pending => {
                        spins += 1;
                        if spins > MAX_SPIN {
                            panic!("cold: 未放行");
                        }
                        spin_loop();
                    }
                }
            };
            drop(g);
            drop(fut);
            // `CooperativeAcqSession` 没有 Drop 实现，`drop()` 只会移动值；
            // 用 `let _ =` 立即结束借用，避免 clippy::drop_non_drop。
            let _ = rs;
            let _ = ws;
            drop(lock);
            if i < sample {
                samples.push(t.elapsed().as_nanos() as u64);
            }
        }
        let head: Vec<u64> = samples.iter().copied().take(32).collect();
        emit_("cold", 1, round, "first_32_median", median_(&head));
        emit_("cold", 1, round, "first_32_min", *head.iter().min().unwrap_or(&0));
        emit_("cold", 1, round, "all_median", median_(&samples));
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        emit_("cold", 1, round, "p95", pct_(&sorted, 95.0));
        emit_("cold", 1, round, "p99", pct_(&sorted, 99.0));
        emit_("cold", 1, round, "max", pct_(&sorted, 100.0));
        emit_(
            "cold",
            1,
            round,
            "rss_after_kb",
            rss_kb_("VmRSS"),
        );
        emit_("cold", 1, round, "hwm_kb", rss_kb_("VmHWM"));
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 G：memory（深队列峰值后排空，采样 RSS/VmHWM）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_memory_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let n = cfg.ns_.iter().copied().max().unwrap_or(4096);
    let bursts = env_usize_("RWLCMP_BURSTS", 3);

    for round in 0..cfg.rounds_ {
        for burst in 0..bursts {
            let base = rss_kb_("VmRSS");
            let lock = TestLock::new_owned(0u64);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");
            let mut sessions: Vec<_> = (0..n)
                .map(|_| Box::new(lock.acquire_session()))
                .collect();
            let mut futs = Vec::with_capacity(n);
            for s in sessions.iter_mut() {
                let mut fut = Box::pin(s.read_async().into_future());
                match fut.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => panic!("memory: reader 提前领取"),
                    Poll::Ready(Err(e)) => panic!("memory: 入队失败 {e:?}"),
                }
                futs.push(fut);
            }
            let peak = rss_kb_("VmRSS");
            let hwm = rss_kb_("VmHWM");

            drop(w);
            let mut guards: Vec<RGuard<'_, '_>> = Vec::with_capacity(n);
            for f in futs.iter_mut() {
                loop {
                    match f.as_mut().poll(&mut cx) {
                        Poll::Ready(Ok(g)) => {
                            guards.push(g);
                            break;
                        }
                        Poll::Ready(Err(e)) => panic!("memory: 领取失败 {e:?}"),
                        Poll::Pending => spin_loop(),
                    }
                }
            }
            let drained = rss_kb_("VmRSS");
            drop(guards);
            let after_release = rss_kb_("VmRSS");
            drop(futs);
            drop(sessions);
            drop(lock);
            let after_drop = rss_kb_("VmRSS");

            emit_("memory", n, round, "burst", burst as u64);
            emit_("memory", n, round, "rss_base_kb", base);
            emit_("memory", n, round, "rss_peak_kb", peak);
            emit_("memory", n, round, "hwm_peak_kb", hwm);
            emit_("memory", n, round, "rss_drained_kb", drained);
            emit_("memory", n, round, "rss_after_release_kb", after_release);
            emit_("memory", n, round, "rss_after_drop_kb", after_drop);
        }
    }
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 D2：deep-mixed（深队列，读/写等待者按 3:1 交错）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_deep_mixed_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    for &n in &cfg.ns_ {
        progress_(format!("deep-mixed N={n}"));
        for round in 0..cfg.rounds_ {
            let lock = TestLock::new_owned(0u64);
            warm_lock_(&lock, &mut cx);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");

            let mut sessions: Vec<_> = (0..n)
                .map(|_| Box::new(lock.acquire_session()))
                .collect();
            let mut futs: Vec<AnyFut<'_, '_>> = Vec::with_capacity(n);
            let mut readers = 0usize;
            let mut writers = 0usize;
            let t_enq = Instant::now();
            for s in sessions.iter_mut() {
                // 每第 4 个等待者是写者（3 读 : 1 写）。
                if futs.len() % 4 == 3 {
                    let mut fut = Box::pin(s.write_async().into_future());
                    match fut.as_mut().poll(&mut cx) {
                        Poll::Pending => {}
                        Poll::Ready(Ok(_)) => {
                            panic!("deep-mixed: writer 提前领取（n={n}）");
                        }
                        Poll::Ready(Err(e)) => panic!("deep-mixed: 入队失败 {e:?}"),
                    }
                    futs.push(AnyFut::Write(fut));
                    writers += 1;
                } else {
                    let mut fut = Box::pin(s.read_async().into_future());
                    match fut.as_mut().poll(&mut cx) {
                        Poll::Pending => {}
                        Poll::Ready(Ok(_)) => {
                            panic!("deep-mixed: reader 提前领取（n={n}）");
                        }
                        Poll::Ready(Err(e)) => panic!("deep-mixed: 入队失败 {e:?}"),
                    }
                    futs.push(AnyFut::Read(fut));
                    readers += 1;
                }
            }
            let enq_ns = t_enq.elapsed().as_nanos() as u64;

            let t_drain = Instant::now();
            drop(w);
            let mut rounds = 0usize;
            while !futs.is_empty() {
                rounds += 1;
                if rounds > 1_000_000 {
                    panic!("deep-mixed: 无法推进（n={n}）");
                }
                let mut i = 0usize;
                let mut progressed = false;
                while i < futs.len() {
                    let ready = match &mut futs[i] {
                        AnyFut::Read(f) => match f.as_mut().poll(&mut cx) {
                            Poll::Ready(Ok(g)) => {
                                drop(g);
                                true
                            }
                            Poll::Ready(Err(e)) => panic!("deep-mixed: 领取失败 {e:?}"),
                            Poll::Pending => false,
                        },
                        AnyFut::Write(f) => match f.as_mut().poll(&mut cx) {
                            Poll::Ready(Ok(g)) => {
                                drop(g);
                                true
                            }
                            Poll::Ready(Err(e)) => panic!("deep-mixed: 领取失败 {e:?}"),
                            Poll::Pending => false,
                        },
                    };
                    if ready {
                        futs.swap_remove(i);
                        progressed = true;
                    } else {
                        i += 1;
                    }
                }
                if !progressed {
                    spin_loop();
                }
            }
            let drain_ns = t_drain.elapsed().as_nanos() as u64;

            emit_("deep-mixed", n, round, "enqueue", enq_ns);
            emit_("deep-mixed", n, round, "drain", drain_ns);
            emit_("deep-mixed", n, round, "per_waiter", drain_ns / n as u64);
            emit_("deep-mixed", n, round, "rounds", rounds as u64);
            emit_("deep-mixed", n, round, "readers", readers as u64);
            emit_("deep-mixed", n, round, "writers", writers as u64);
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 H：upgrade-mixed（可升级读 → 升级 → 写者释放 → 回退 → 降级）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[allow(clippy::unnecessary_box_pin)]
fn scenario_upgrade_(cfg: &Cfg) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    // 普通读者数量：0 表示"升级时没有其它读者"，是升级的最快路径。
    let reader_counts = env_list_("RWLCMP_UPGRADE_READERS", &[0, 1, 4, 16]);

    for &n in &reader_counts {
        progress_(format!("upgrade readers={n}"));
        for round in 0..cfg.rounds_ {
            let lock = TestLock::new_owned(0u64);
            warm_lock_(&lock, &mut cx);
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");

            // 1) 可升级读先入队，随后 n 个普通读者加入同一条读者链。
            let mut us = lock.acquire_session();
            let mut ufut = Box::pin(us.upgradable_read_async().into_future());
            match ufut.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(_) => panic!("upgrade: 可升级读提前领取"),
            }
            // 普通读者各自持有独立会话。
            let mut rs_sessions: Vec<_> = (0..n)
                .map(|_| Box::new(lock.acquire_session()))
                .collect();
            let mut r_futs = Vec::with_capacity(n);
            for s in rs_sessions.iter_mut() {
                let mut fut = Box::pin(s.read_async().into_future());
                match fut.as_mut().poll(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(_)) => panic!("upgrade: reader 提前领取"),
                    Poll::Ready(Err(e)) => panic!("upgrade: 入队失败 {e:?}"),
                }
                r_futs.push(fut);
            }

            // 2) 释放写者：整组读者被放行。
            drop(w);
            let ug = loop {
                match ufut.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(g)) => break g,
                    Poll::Ready(Err(e)) => panic!("upgrade: 可升级读领取失败 {e:?}"),
                    Poll::Pending => spin_loop(),
                }
            };
            let mut r_guards: Vec<RGuard<'_, '_>> = Vec::with_capacity(n);
            for f in r_futs.iter_mut() {
                loop {
                    match f.as_mut().poll(&mut cx) {
                        Poll::Ready(Ok(g)) => {
                            r_guards.push(g);
                            break;
                        }
                        Poll::Ready(Err(e)) => panic!("upgrade: reader 领取失败 {e:?}"),
                        Poll::Pending => spin_loop(),
                    }
                }
            }

            // 3) 发起升级：其它普通读者仍持有时应保持 Pending。
            let mut upgrade = ug.upgrade_session();
            let mut upgfut = Box::pin(upgrade.upgrade_async().into_future());
            let t_upg = Instant::now();
            // n == 0 时升级可以立即成功；n > 0 时必须先等其它读者释放。
            let mut ready_writer = None;
            match upgfut.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(g)) if n == 0 => ready_writer = Some(g),
                Poll::Ready(Ok(_)) => {
                    panic!("upgrade: 其它读者仍持有时升级完成（n={n}）");
                }
                Poll::Ready(Err(e)) => panic!("upgrade: 升级失败 {e:?}"),
            }
            drop(r_guards);
            let wg = match ready_writer {
                Option::Some(g) => g,
                Option::None => {
                    let mut spins = 0usize;
                    loop {
                        match upgfut.as_mut().poll(&mut cx) {
                            Poll::Ready(Ok(g)) => break g,
                            Poll::Ready(Err(e)) => panic!("upgrade: 升级失败 {e:?}"),
                            Poll::Pending => {
                                spins += 1;
                                if spins > MAX_SPIN {
                                    panic!("upgrade: 升级未被放行（n={n}）");
                                }
                                spin_loop();
                            }
                        }
                    }
                }
            };
            let upgrade_ns = t_upg.elapsed().as_nanos() as u64;
            drop(upgfut);

            // 4) 写者持有时再排一个读者；它必须等到降级为普通读之后。
            let mut ws2 = lock.acquire_session();
            let mut wfut = Box::pin(ws2.read_async().into_future());
            match wfut.as_mut().poll(&mut cx) {
                Poll::Pending => {}
                Poll::Ready(_) => panic!("upgrade: 写者持有时 reader 提前领取"),
            }

            // 5) 写者 → 可升级读 → 普通读 → 释放，最后一位触发放行。
            let r = wg.downgrade_to_upgradable();
            let r = r.downgrade();
            drop(r);

            let mut spins = 0usize;
            let g = loop {
                match wfut.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(g)) => break g,
                    Poll::Ready(Err(e)) => panic!("upgrade: 降级后 reader 领取失败 {e:?}"),
                    Poll::Pending => {
                        spins += 1;
                        if spins > MAX_SPIN {
                            panic!("upgrade: 降级后 reader 仍未被放行（n={n}）");
                        }
                        spin_loop();
                    }
                }
            };
            drop(g);
            let total_ns = t_upg.elapsed().as_nanos() as u64;

            emit_("upgrade", n, round, "upgrade", upgrade_ns);
            emit_("upgrade", n, round, "total", total_ns);
            emit_("upgrade", n, round, "readers", n as u64);
        }
    }
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 I：concurrent（真实运行时调度 sanity：tokio 多线程 / compio 单线程）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 临界区内的固定工作量，避免零成本临界区把测量变成纯锁开销。
#[inline]
fn burn_(spins: usize) {
    let mut acc = 0u64;
    for i in 0..spins {
        acc = acc.wrapping_add(black_box(i as u64));
    }
    black_box(acc);
}

/// 一个竞争者的操作序列：读多写少，并周期性升级后降级。
fn worker_fut_(
    lock: std::sync::Arc<TestLock>,
    ops: usize,
    hold: usize,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let mut sess = lock.acquire_session();
        for i in 0..ops {
            match i % 10 {
                0..=6 => {
                    let g = sess.read_async().await.expect("read");
                    black_box(*g);
                    burn_(hold);
                }
                7 | 8 => {
                    let mut g = sess.write_async().await.expect("write");
                    *g = g.wrapping_add(1);
                    burn_(hold);
                }
                _ => {
                    let upg = sess.upgradable_read_async().await.expect("upgradable");
                    let mut us = upg.upgrade_session();
                    let mut w = us.upgrade_async().await.expect("upgrade");
                    *w = w.wrapping_add(1);
                    burn_(hold);
                    let r = w.downgrade_to_reader();
                    black_box(*r);
                }
            }
        }
    })
}

fn scenario_concurrent_(cfg: &Cfg) {
    let n = env_usize_("RWLCMP_CONC_N", 4);
    let ops = env_usize_("RWLCMP_CONC_OPS", 5000);
    let workers = env_usize_("RWLCMP_CONC_WORKERS", 4);
    let hold = env_usize_("RWLCMP_CONC_HOLD", 8);

    for round in 0..cfg.rounds_ {
        // tokio 多线程：固定 worker 数，任务数 = n。
        let t = Instant::now();
        {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .build()
                .expect("build tokio runtime");
            rt.block_on(async {
                let lock = std::sync::Arc::new(TestLock::new_owned(0u64));
                let mut tasks = Vec::with_capacity(n);
                for _ in 0..n {
                    tasks.push(tokio::spawn(worker_fut_(
                        std::sync::Arc::clone(&lock),
                        ops,
                        hold,
                    )));
                }
                for task in tasks {
                    task.await.expect("worker panicked");
                }
            });
        }
        let tokio_ns = t.elapsed().as_nanos() as u64 / (n * ops) as u64;

        // compio 单线程：任务数 = n。
        let t = Instant::now();
        {
            let rt = compio::runtime::Runtime::new().expect("build compio runtime");
            rt.block_on(async {
                let lock = std::sync::Arc::new(TestLock::new_owned(0u64));
                let mut tasks = Vec::with_capacity(n);
                for _ in 0..n {
                    tasks.push(compio::runtime::spawn(worker_fut_(
                        std::sync::Arc::clone(&lock),
                        ops,
                        hold,
                    )));
                }
                for task in tasks {
                    task.await.expect("worker panicked");
                }
            });
        }
        let compio_ns = t.elapsed().as_nanos() as u64 / (n * ops) as u64;

        emit_("concurrent", n, round, "tokio_ns_per_op", tokio_ns);
        emit_("concurrent", n, round, "compio_ns_per_op", compio_ns);
        emit_("concurrent", n, round, "tokio_workers", workers as u64);
    }
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 C2：release-threads（N 个 OS 线程同时释放读者许可）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// N 个线程各自领到一个读者许可，用原子开关**同时**释放。
///
/// 单线程的 `reader-release` 只能测顺序释放；这里验证"堆版每次释放都取队列
/// 锁、Arena 只有最后一个释放者触发 pass"在真实并发下的差异。每线程记录
/// 自己 `drop(guard)` 的墙钟耗时，主线程只负责放行开关与收集结果，避免把
/// 主线程的调度延迟混进释放区间。
#[allow(clippy::unnecessary_box_pin)]
fn scenario_release_threads_(cfg: &Cfg) {
    for &n in &[2usize, 4] {
        progress_(format!("release-threads N={n}"));
        for round in 0..cfg.rounds_ {
            let lock = Arc::new(TestLock::new_owned(0u64));
            {
                let waker = Waker::noop();
                let mut cx = Context::from_waker(waker);
                warm_lock_(&lock, &mut cx);
            }
            let mut ws = lock.acquire_session();
            let w = ws.try_write().expect("writer");

            let barrier_in = Arc::new(Barrier::new(n + 1));
            let ready = Arc::new(StdAtomicUsize::new(0));
            let release = Arc::new(StdAtomicBool::new(false));
            let (tx, rx) = mpsc::channel::<u64>();
            let mut handles = Vec::with_capacity(n);
            for _ in 0..n {
                let lock = Arc::clone(&lock);
                let bi = Arc::clone(&barrier_in);
                let ready = Arc::clone(&ready);
                let release = Arc::clone(&release);
                let tx = tx.clone();
                handles.push(thread::spawn(move || {
                    let waker = Waker::noop();
                    let mut cx = Context::from_waker(waker);
                    let mut sess = lock.acquire_session();
                    let mut fut = Box::pin(sess.read_async().into_future());
                    match fut.as_mut().poll(&mut cx) {
                        Poll::Pending => {}
                        Poll::Ready(_) => panic!("release-threads: reader 提前领取"),
                    }
                    bi.wait();
                    let mut spins = 0usize;
                    let g = loop {
                        match fut.as_mut().poll(&mut cx) {
                            Poll::Ready(Ok(g)) => break g,
                            Poll::Ready(Err(e)) => {
                                panic!("release-threads: 领取失败 {e:?}");
                            }
                            Poll::Pending => {
                                spins += 1;
                                if spins > MAX_SPIN {
                                    panic!("release-threads: 未放行");
                                }
                                spin_loop();
                            }
                        }
                    };
                    ready.fetch_add(1, StdOrdering::AcqRel);
                    while !release.load(StdOrdering::Acquire) {
                        spin_loop();
                    }
                    let t = Instant::now();
                    drop(g);
                    let dt = t.elapsed().as_nanos() as u64;
                    let _ = tx.send(dt);
                }));
            }
            barrier_in.wait();
            drop(w);
            while ready.load(StdOrdering::Acquire) < n {
                thread::yield_now();
            }
            let t0 = Instant::now();
            release.store(true, StdOrdering::Release);
            let mut dts = Vec::with_capacity(n);
            for _ in 0..n {
                dts.push(rx.recv().expect("release-threads: recv"));
            }
            let total = t0.elapsed().as_nanos() as u64;
            for h in handles {
                h.join().expect("release-threads: worker panicked");
            }

            emit_("release-threads", n, round, "release_max", *dts.iter().max().unwrap_or(&0));
            emit_("release-threads", n, round, "release_sum", dts.iter().sum::<u64>());
            emit_("release-threads", n, round, "release_median", median_(&dts));
            emit_("release-threads", n, round, "release_total", total);
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 入口
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn main() {
    let cfg = Cfg::from_env_();
    println!(
        "PHASE,build,impl={IMPL_NAME},rounds={},ops={},hold={}",
        cfg.rounds_, cfg.ops_, cfg.hold_
    );
    for sc in &cfg.scenarios_ {
        match sc.as_str() {
            "fastpath" => scenario_fastpath_(&cfg),
            "reader-wake" => scenario_reader_wake_(&cfg),
            "reader-release" => scenario_reader_release_(&cfg),
            "release-threads" => scenario_release_threads_(&cfg),
            "deep" => scenario_deep_(&cfg),
            "deep-mixed" => scenario_deep_mixed_(&cfg),
            "cancel" => scenario_cancel_(&cfg),
            "upgrade" => scenario_upgrade_(&cfg),
            "concurrent" => scenario_concurrent_(&cfg),
            "cold" => scenario_cold_(&cfg),
            "memory" => scenario_memory_(&cfg),
            other => progress_(format!("未知场景 {other}，跳过")),
        }
    }
}
