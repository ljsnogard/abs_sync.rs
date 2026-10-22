// 分配计数基准（共用源）。
//
// 由 `rwlock_alloc_heap.rs` / `rwlock_alloc_arena.rs` 通过 `include!` 引入。
// 本文件安装一个计数型全局分配器，测量"入队 → 唤醒 → 领取 → 释放"稳态
// 路径上的堆分配次数与字节数，并按分配大小分桶：
//
// - 本 crate 的等待节点 / Vec<Waker> 通常落在 <=1024 字节的桶；
// - Arena 的 chunk 是 >4096 字节的大块；
// - 运行时 Waker 的分配：本基准使用 `Waker::noop()`，所以**没有**运行时
//   Waker 分配；这一点让"本 crate 分配"和"运行时分配"天然分离。
//
// # 扣除基准自身分配
//
// bench 用 `Box::pin` 存放 future、`Box` 存放会话，这些是测量装置的分配，
// 不属于锁实现。因此程序先跑一轮"只创建、不 poll"的对照，再跑完整流程，
// 逐桶相减得到**实现自身**的分配。
//
// # 输出与断言
//
// ```text
// ALLOC,<impl>,<scenario>,<phase>,<bucket>,<count>
// ASSERT,<impl>,<scenario>,<ok|fail>,<detail>
// ```
//
// - Arena 稳态应满足"除预热已分配的 chunk 外 0 次分配"；
// - 堆版稳态必须 >0（每个等待者一个 `Arc` 节点 + `Vec<Waker>`）。
// 断言失败时进程以非零码退出。

use core::{
    alloc::{GlobalAlloc, Layout},
    future::{Future, IntoFuture},
    hint::black_box,
    task::{Context, Poll, Waker},
};
use std::{
    alloc::System,
    env,
    fmt,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use atomic_sync::x_deps::atomex::StrictOrderings;

type Sess<'a> = impl_mod::CooperativeAcqSession<'a, u64, usize, std::sync::atomic::AtomicUsize, StrictOrderings>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 计数分配器
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 分配大小分桶。桶边界是闭区间的上界（字节）。
const BUCKETS: [usize; 7] = [32, 64, 128, 256, 1024, 4096, usize::MAX];

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static FREE_COUNT: AtomicU64 = AtomicU64::new(0);
static BUCKET_COUNT: [AtomicU64; 7] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

struct CountingAlloc;

fn bucket_of_(size: usize) -> usize {
    BUCKETS.iter().position(|&b| size <= b).unwrap_or(6)
}

fn record_alloc_(size: usize) {
    ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOC_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    BUCKET_COUNT[bucket_of_(size)].fetch_add(1, Ordering::Relaxed);
}

// SAFETY: 本分配器只是把调用原样转发给 `System`，并在转发成功后用原子量
// 记账。记账不使用堆内存，也不会 panic；转发失败时保持返回空指针的语义。
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 由 `GlobalAlloc::alloc` 的契约保证 `layout` 合法。
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc_(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREE_COUNT.fetch_add(1, Ordering::Relaxed);
        // SAFETY: 由 `GlobalAlloc::dealloc` 的契约保证 `ptr` 来自同一分配器、
        // 且 `layout` 与分配时一致。
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL_ALLOC_: CountingAlloc = CountingAlloc;

/// 某一时刻的分配计数快照。
#[derive(Clone, Copy, Default)]
struct Counts {
    allocs_: u64,
    bytes_: u64,
    frees_: u64,
    buckets_: [u64; 7],
}

fn snapshot_() -> Counts {
    let mut buckets = [0u64; 7];
    for (i, b) in BUCKET_COUNT.iter().enumerate() {
        buckets[i] = b.load(Ordering::Relaxed);
    }
    Counts {
        allocs_: ALLOC_COUNT.load(Ordering::Relaxed),
        bytes_: ALLOC_BYTES.load(Ordering::Relaxed),
        frees_: FREE_COUNT.load(Ordering::Relaxed),
        buckets_: buckets,
    }
}

fn diff_(after: Counts, before: Counts) -> Counts {
    let mut buckets = [0u64; 7];
    for i in 0..7 {
        buckets[i] = after.buckets_[i].saturating_sub(before.buckets_[i]);
    }
    Counts {
        allocs_: after.allocs_.saturating_sub(before.allocs_),
        bytes_: after.bytes_.saturating_sub(before.bytes_),
        frees_: after.frees_.saturating_sub(before.frees_),
        buckets_: buckets,
    }
}

fn emit_counts_(scenario: &str, phase: &str, c: Counts) {
    println!(
        "ALLOC,{IMPL_NAME},{scenario},{phase},allocs,{}",
        c.allocs_
    );
    println!("ALLOC,{IMPL_NAME},{scenario},{phase},bytes,{}", c.bytes_);
    println!("ALLOC,{IMPL_NAME},{scenario},{phase},frees,{}", c.frees_);
    for (i, &b) in BUCKETS.iter().enumerate() {
        let label = if b == usize::MAX {
            format!("b{i}_gt{}", BUCKETS[i - 1])
        } else {
            format!("b{i}_le{b}")
        };
        println!(
            "ALLOC,{IMPL_NAME},{scenario},{phase},{label},{}",
            c.buckets_[i]
        );
    }
}

fn progress_(msg: impl fmt::Display) {
    eprintln!("  ... {msg}");
    let _ = std::io::stderr().flush();
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 流程：创建 N 个会话 + N 个 future（对照 / 完整）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 对照流程：只创建测量装置（N 个 `Box<Session>` + N 个 `Box::pin(future)`），
/// 不 poll、不入队，随后全部丢弃。
///
/// 它产生与完整流程**完全相同**的装置分配，用来逐桶扣除。
fn bench_only_flow_(lock: &TestLock, n: usize) {
    let mut sessions: Vec<Box<Sess<'_>>> =
        (0..n).map(|_| Box::new(lock.acquire_session())).collect();
    let mut futs = Vec::with_capacity(n);
    for s in sessions.iter_mut() {
        let fut = Box::pin(s.read_async().into_future());
        futs.push(fut);
    }
    black_box(futs.len());
    drop(futs);
    drop(sessions);
    black_box(lock.reader_count());
}

/// 完整流程：写者持有 → N 个读者入队 → 释放写者 → 逐个领取并立即释放。
///
/// 读者领到 guard 后立刻 drop，因此不会产生额外的 `Vec<Guard>` 分配；这也
/// 更接近"每个等待者只测排队/交接开销"的语义。
fn full_flow_(lock: &TestLock, cx: &mut Context<'_>, n: usize) {
    let mut ws = lock.acquire_session();
    let w = ws.try_write().expect("writer");
    let mut sessions: Vec<Box<Sess<'_>>> =
        (0..n).map(|_| Box::new(lock.acquire_session())).collect();
    let mut futs = Vec::with_capacity(n);
    for s in sessions.iter_mut() {
        let mut fut = Box::pin(s.read_async().into_future());
        match fut.as_mut().poll(cx) {
            Poll::Pending => {}
            Poll::Ready(Ok(_)) => panic!("full_flow: reader 提前领取"),
            Poll::Ready(Err(e)) => panic!("full_flow: 入队失败 {e:?}"),
        }
        futs.push(fut);
    }
    drop(w);

    for f in futs.iter_mut() {
        let mut spins = 0usize;
        loop {
            match f.as_mut().poll(cx) {
                Poll::Ready(Ok(g)) => {
                    drop(g);
                    break;
                }
                Poll::Ready(Err(e)) => panic!("full_flow: 领取失败 {e:?}"),
                Poll::Pending => {
                    spins += 1;
                    if spins > (1 << 22) {
                        panic!("full_flow: 等待者未被放行");
                    }
                    core::hint::spin_loop();
                }
            }
        }
    }
    drop(futs);
    drop(sessions);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn scenario_steady_(n: usize, warm: usize, k: usize) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let lock = TestLock::new_owned(0u64);

    for _ in 0..warm {
        full_flow_(&lock, &mut cx, n);
    }

    let c0 = snapshot_();
    for _ in 0..k {
        bench_only_flow_(&lock, n);
    }
    let c1 = snapshot_();
    for _ in 0..k {
        full_flow_(&lock, &mut cx, n);
    }
    let c2 = snapshot_();

    let bench = diff_(c1, c0);
    let total = diff_(c2, c1);
    let imp = diff_(total, bench);

    progress_(format!(
        "steady n={n} warm={warm} k={k}: bench={} total={} impl={} allocs / {} bytes",
        bench.allocs_, total.allocs_, imp.allocs_, imp.bytes_
    ));
    emit_counts_("steady", "bench", bench);
    emit_counts_("steady", "total", total);
    emit_counts_("steady", "impl", imp);

    // 断言：Arena 稳态实现分配为 0；堆版必须为 0 以上。
    if IMPL_NAME == "arena" {
        let ok = imp.allocs_ == 0 && imp.bytes_ == 0;
        println!(
            "ASSERT,{IMPL_NAME},steady,{},impl_allocs={},impl_bytes={}",
            if ok { "ok" } else { "fail" },
            imp.allocs_,
            imp.bytes_
        );
        assert!(
            ok,
            "Arena 稳态出现实现分配：{} 次 / {} 字节（chunk 以外应为 0）",
            imp.allocs_, imp.bytes_
        );
    } else {
        let ok = imp.allocs_ > 0;
        println!(
            "ASSERT,{IMPL_NAME},steady,{},impl_allocs={},impl_bytes={}",
            if ok { "ok" } else { "fail" },
            imp.allocs_,
            imp.bytes_
        );
        assert!(
            ok,
            "堆版稳态应有实现分配，但计数为 0（测量装置扣除可能过度）"
        );
    }
}

fn scenario_cold_(k: usize) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    let c0 = snapshot_();
    for _ in 0..k {
        let lock = TestLock::new_owned(0u64);
        let mut ws = lock.acquire_session();
        let w = ws.try_write().expect("writer");
        let mut rs = lock.acquire_session();
        let mut fut = Box::pin(rs.read_async().into_future());
        match fut.as_mut().poll(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!("cold: reader 提前领取"),
        }
        drop(w);
        let g = loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(g)) => break g,
                Poll::Ready(Err(e)) => panic!("cold: 领取失败 {e:?}"),
                Poll::Pending => core::hint::spin_loop(),
            }
        };
        drop(g);
        drop(fut);
        // 会话类型没有 Drop 实现；用 `let _ =` 结束借用即可。
        let _ = rs;
        let _ = ws;
        drop(lock);
    }
    let c1 = snapshot_();
    let total = diff_(c1, c0);
    progress_(format!(
        "cold k={k}: total={} allocs / {} bytes",
        total.allocs_, total.bytes_
    ));
    emit_counts_("cold", "total", total);
}

fn scenario_deep_(n: usize) {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let lock = TestLock::new_owned(0u64);
    // 先预热一个小的完整流程，确保 Arena 的 chunk 分配发生在测量区间之外。
    full_flow_(&lock, &mut cx, 1);
    let c0 = snapshot_();
    full_flow_(&lock, &mut cx, n);
    let c1 = snapshot_();
    let total = diff_(c1, c0);
    progress_(format!(
        "deep n={n}: total={} allocs / {} bytes",
        total.allocs_, total.bytes_
    ));
    emit_counts_("deep", "total", total);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 入口
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

fn main() {
    let rounds = env_usize_("RWLCMP_ALLOC_ROUNDS", 3);
    let n = env_usize_("RWLCMP_ALLOC_N", 8);
    let warm = env_usize_("RWLCMP_ALLOC_WARM", 64);
    let k = env_usize_("RWLCMP_ALLOC_K", 200);
    let cold_k = env_usize_("RWLCMP_ALLOC_COLD_K", 100);
    let deep_n = env_usize_("RWLCMP_ALLOC_DEEP_N", 4096);
    println!("PHASE,alloc,impl={IMPL_NAME},rounds={rounds}");

    let t = Instant::now();
    for round in 0..rounds {
        progress_(format!("alloc round {round}"));
        scenario_steady_(n, warm, k);
        scenario_cold_(cold_k);
        scenario_deep_(deep_n);
    }
    println!("PHASE,alloc,done,elapsed_ms={}", t.elapsed().as_millis());
}

fn env_usize_(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}
