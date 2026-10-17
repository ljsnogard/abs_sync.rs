//! `rwlock::cooperative` 的并发性能测试。
//!
//! # 方法
//!
//! 用带种子的伪随机序列为**每个竞争者**生成一份"操作脚本"，脚本里混合了
//! 读、写、可升级读→升级为写→降级，以及"让出执行权"（模拟两次操作之间的
//! 思考时间）。N 个竞争者各自把脚本跑完，测量总耗时并换算成每次操作的
//! 纳秒数；再扫描 N = 1/2/4/8，观察竞争强度对性能的影响。
//!
//! 每份脚本的种子由竞争者编号导出，因此**每次运行都是同一批负载**，
//! 结果可复现、不同实现之间可比。
//!
//! # 对照组
//!
//! | 对照 | 出现位置 | 说明 |
//! |---|---|---|
//! | `tokio::sync::RwLock` | 异步两节 | 同为异步读写锁，最直接的对照；但它没有"可升级读"，负载里的升级操作退化为 `write()` |
//! | `rwlock::preemptive::SpinningRwLock` | OS 线程节 | 本 crate 的自旋实现；`wait()` 在争用下会活锁，因此用 `try_*` + `yield_now` 完成等待语义 |
//! | `std::sync::RwLock` | OS 线程节 | 内核阻塞原语，作为线程模型差异的参考；**不是同类对照** |
//!
//! compio 目前没有自带 `RwLock`；不过 `tokio::sync::RwLock` 的锁本身不依赖
//! tokio 运行时，可以跑在 compio 的单线程执行器上，因此单线程一节也用它对照。
//!
//! # 运行
//!
//! ```sh
//! cargo bench --bench rwlock_cooperative
//! ```
//!
//! 规模可用环境变量调整：
//!
//! | 变量 | 默认 | 含义 |
//! |---|---|---|
//! | `RWLOCK_BENCH_OPS` | 20000 | 每个竞争者的操作数 |
//! | `RWLOCK_BENCH_ROUNDS` | 3 | 每个测量取最好成绩的轮数 |
//! | `RWLOCK_BENCH_COMPETITORS` | `1,2,4,8` | 竞争者数量列表 |
//! | `RWLOCK_BENCH_HOLD` | 8 | 临界区内的工作量（自旋次数） |

use core::{
    fmt,
    future::Future,
    hint::black_box,
    pin::Pin,
};
use std::{
    env,
    io::Write,
    sync::{Arc, Barrier as StdBarrier, RwLock as StdRwLock},
    thread,
    time::{Duration, Instant},
    vec::Vec,
};

use rand::{rngs::StdRng, Rng, SeedableRng};

use atomic_sync::{
    rwlock::{
        cooperative::CooperativeRwLockOwned,
        preemptive::SpinningRwLockOwned,
    },
    x_deps::abs_sync::async_rwlock::TrWriterGuard,
};

type TestLock = CooperativeRwLockOwned<u64>;
type PreemptiveLock = SpinningRwLockOwned<u64>;
type BoxFut = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 负载生成
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一次操作。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    /// 取读许可，读一次数据。
    Read,
    /// 取写许可，自增一次。
    Write,
    /// 可升级读 → 升级为写 → 写一次 → 降级回读。
    ///
    /// 对照实现没有这个语义，退化为一次 `write()`。
    Upgrade,
    /// 让出执行权，模拟两次操作之间的思考时间。
    Think,
}

/// 负载权重（百分比，二者之和不超过 100，其余部分归 [`Op::Think`]）。
#[derive(Clone, Copy, Debug)]
struct Weights {
    read: u32,
    write: u32,
    upgrade: u32,
}

impl Weights {
    /// 读多写少：最贴近"配置/缓存"这类真实场景。
    const READ_HEAVY: Self = Weights {
        read: 75,
        write: 15,
        upgrade: 5,
    };

    /// 写多读少：压力集中在互斥路径上。
    const WRITE_HEAVY: Self = Weights {
        read: 40,
        write: 40,
        upgrade: 15,
    };

    /// 纯读：读者之间完全无排他，用来隔离"排队/交接"这一因素。
    const READ_ONLY: Self = Weights {
        read: 95,
        write: 0,
        upgrade: 0,
    };

    /// 读 + 写、但**没有升级**：用来判断 N=2 异常是否由升级路径（插队请求）引起。
    const NO_UPGRADE: Self = Weights {
        read: 70,
        write: 25,
        upgrade: 0,
    };
}

impl fmt::Display for Weights {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "读 {}% / 写 {}% / 升级 {}% / 让出 {}%",
            self.read,
            self.write,
            self.upgrade,
            100 - self.read - self.write - self.upgrade,
        )
    }
}

/// 为每个竞争者生成一份操作脚本。
fn gen_scripts(competitors: usize, ops: usize, w: Weights) -> Vec<Vec<Op>> {
    (0..competitors)
        .map(|worker| {
            let mut rng = StdRng::seed_from_u64(0x5A17_0000 ^ worker as u64);
            (0..ops)
                .map(|_| {
                    let r = rng.random_range(0..100u32);
                    if r < w.read {
                        Op::Read
                    } else if r < w.read + w.write {
                        Op::Write
                    } else if r < w.read + w.write + w.upgrade {
                        Op::Upgrade
                    } else {
                        Op::Think
                    }
                })
                .collect()
        })
        .collect()
}

/// 临界区内的固定工作量，避免"零成本临界区"把测量变成纯锁开销。
#[inline]
fn burn(spins: usize) {
    let mut acc = 0u64;
    for i in 0..spins {
        acc = acc.wrapping_add(black_box(i as u64));
    }
    black_box(acc);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 被测实现的工作循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `rwlock::cooperative` 的工作循环。
fn our_worker(lock: Arc<TestLock>, script: Vec<Op>, hold: usize) -> BoxFut {
    Box::pin(async move {
        let mut sess = lock.acquire_session();
        for op in script {
            match op {
                Op::Read => {
                    let g = sess.read_async().await.expect("read");
                    black_box(*g);
                    burn(hold);
                }
                Op::Write => {
                    let mut g = sess.write_async().await.expect("write");
                    *g = g.wrapping_add(1);
                    burn(hold);
                }
                Op::Upgrade => {
                    let upg = sess.upgradable_read_async().await.expect("upgradable");
                    let mut upg_sess = upg.upgrade_session();
                    let mut w = upg_sess.upgrade_async().await.expect("upgrade");
                    *w = w.wrapping_add(1);
                    burn(hold);
                    // 降级回普通读者再释放，模拟"升级写完立刻退回只读"。
                    let r = w.downgrade();
                    black_box(*r);
                }
                Op::Think => futures_lite::future::yield_now().await,
            }
        }
    })
}

/// `tokio::sync::RwLock` 的工作循环（升级退化为写）。
fn tokio_worker(
    lock: Arc<tokio::sync::RwLock<u64>>,
    script: Vec<Op>,
    hold: usize,
) -> BoxFut {
    Box::pin(async move {
        for op in script {
            match op {
                Op::Read => {
                    let g = lock.read().await;
                    black_box(*g);
                    burn(hold);
                }
                Op::Write | Op::Upgrade => {
                    let mut g = lock.write().await;
                    *g = g.wrapping_add(1);
                    burn(hold);
                }
                Op::Think => futures_lite::future::yield_now().await,
            }
        }
    })
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 运行器
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 在 tokio 多线程执行器上跑完所有工作循环。
fn run_tokio(workers: usize, make: impl FnOnce() -> Vec<BoxFut>) -> Duration {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .expect("build tokio runtime");
    let futs = make();
    let start = Instant::now();
    rt.block_on(async move {
        let mut tasks = Vec::with_capacity(futs.len());
        for f in futs {
            tasks.push(tokio::spawn(f));
        }
        for t in tasks {
            t.await.expect("worker panicked");
        }
    });
    start.elapsed()
}

/// 在 compio 单线程执行器上跑完所有工作循环。
fn run_compio(make: impl FnOnce() -> Vec<BoxFut>) -> Duration {
    let rt = compio::runtime::Runtime::new().expect("build compio runtime");
    let futs = make();
    let start = Instant::now();
    rt.block_on(async move {
        let mut tasks = Vec::with_capacity(futs.len());
        for f in futs {
            tasks.push(compio::runtime::spawn(f));
        }
        for t in tasks {
            t.await.expect("worker panicked");
        }
    });
    start.elapsed()
}

/// 在 N 个 OS 线程上跑 `rwlock::cooperative`（每个线程自带 `block_on`）。
fn run_threads_our(scripts: &[Vec<Op>], hold: usize) -> Duration {
    let lock = Arc::new(TestLock::new_owned(0));
    let n = scripts.len();
    let barrier = Arc::new(StdBarrier::new(n + 1));
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(n);
        for script in scripts {
            let lock = Arc::clone(&lock);
            let barrier = Arc::clone(&barrier);
            let script = script.clone();
            handles.push(scope.spawn(move || {
                barrier.wait();
                futures_lite::future::block_on(async move {
                    let mut sess = lock.acquire_session();
                    for op in script {
                        match op {
                            Op::Read => {
                                let g = sess.read_async().await.expect("read");
                                black_box(*g);
                                burn(hold);
                            }
                            Op::Write => {
                                let mut g = sess.write_async().await.expect("write");
                                *g = g.wrapping_add(1);
                                burn(hold);
                            }
                            Op::Upgrade => {
                                let upg = sess
                                    .upgradable_read_async()
                                    .await
                                    .expect("upgradable");
                                let mut upg_sess = upg.upgrade_session();
                                let mut w =
                                    upg_sess.upgrade_async().await.expect("upgrade");
                                *w = w.wrapping_add(1);
                                burn(hold);
                                let r = w.downgrade();
                                black_box(*r);
                            }
                            Op::Think => futures_lite::future::yield_now().await,
                        }
                    }
                });
            }));
        }
        let start = Instant::now();
        barrier.wait();
        for h in handles {
            h.join().expect("worker panicked");
        }
        start.elapsed()
    })
}


/// `rwlock::preemptive::SpinningRwLock` 的工作循环（同步、自旋等待）。
///
/// 既有实现的 `wait()` 是**无让步自旋**，在争用下会活锁（与
/// `benches/mutex_contention.rs` 里记录的问题同源），因此这里用
/// `try_*` + `yield_now` 完成同样的等待语义。
///
/// 升级操作退化为 `write()`：该实现的升级路径不阻止新读者，竞争下可能被
/// 无限推迟（开发日志 §2.2），直接放进压测会让整轮跑不完。
fn preemptive_worker(lock: Arc<PreemptiveLock>, script: Vec<Op>, hold: usize) {
    let mut sess = lock.acquire_session();
    for op in script {
        match op {
            Op::Read => {
                loop {
                    if let Ok(g) = sess.try_read() {
                        black_box(*g);
                        burn(hold);
                        break;
                    }
                    thread::yield_now();
                }
            }
            Op::Write | Op::Upgrade => {
                loop {
                    if let Ok(mut g) = sess.try_write() {
                        *g = g.wrapping_add(1);
                        burn(hold);
                        break;
                    }
                    thread::yield_now();
                }
            }
            Op::Think => thread::yield_now(),
        }
    }
}

/// 在 N 个 OS 线程上跑 `rwlock::preemptive::SpinningRwLock`。
fn run_threads_preemptive(scripts: &[Vec<Op>], hold: usize) -> Duration {
    let lock = Arc::new(PreemptiveLock::new_owned(0));
    let n = scripts.len();
    let barrier = Arc::new(StdBarrier::new(n + 1));
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(n);
        for script in scripts {
            let lock = Arc::clone(&lock);
            let barrier = Arc::clone(&barrier);
            let script = script.clone();
            handles.push(scope.spawn(move || {
                barrier.wait();
                preemptive_worker(lock, script, hold);
            }));
        }
        let start = Instant::now();
        barrier.wait();
        for h in handles {
            h.join().expect("worker panicked");
        }
        start.elapsed()
    })
}

/// 在 N 个 OS 线程上跑 `std::sync::RwLock`。
fn run_threads_std(scripts: &[Vec<Op>], hold: usize) -> Duration {
    let lock = Arc::new(StdRwLock::new(0u64));
    let n = scripts.len();
    let barrier = Arc::new(StdBarrier::new(n + 1));
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(n);
        for script in scripts {
            let lock = Arc::clone(&lock);
            let barrier = Arc::clone(&barrier);
            let script = script.clone();
            handles.push(scope.spawn(move || {
                barrier.wait();
                for op in script {
                    match op {
                        Op::Read => {
                            let g = lock.read().expect("read");
                            black_box(*g);
                            burn(hold);
                        }
                        Op::Write | Op::Upgrade => {
                            let mut g = lock.write().expect("write");
                            *g = g.wrapping_add(1);
                            burn(hold);
                        }
                        Op::Think => thread::yield_now(),
                    }
                }
            }));
        }
        let start = Instant::now();
        barrier.wait();
        for h in handles {
            h.join().expect("worker panicked");
        }
        start.elapsed()
    })
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 测量与输出
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 取 `rounds` 轮中最好的一次（耗时最短）。
fn best_of(rounds: usize, mut run: impl FnMut() -> Duration) -> Duration {
    (0..rounds).map(|_| run()).min().expect("rounds > 0")
}

/// 每次操作的纳秒数。
fn ns_per_op(elapsed: Duration, ops: usize) -> f64 {
    elapsed.as_nanos() as f64 / ops as f64
}

fn ratio(a: f64, b: f64) -> f64 {
    if b == 0.0 { f64::NAN } else { a / b }
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_weight_sets() -> Vec<Weights> {
    match env::var("RWLOCK_BENCH_WEIGHTS").as_deref() {
        Ok("readonly") => vec![Weights::READ_ONLY],
        Ok("readheavy") => vec![Weights::READ_HEAVY],
        Ok("writeheavy") => vec![Weights::WRITE_HEAVY],
        Ok("noupgrade") => vec![Weights::NO_UPGRADE],
        _ => vec![Weights::READ_HEAVY, Weights::WRITE_HEAVY],
    }
}

fn env_competitors() -> Vec<usize> {
    match env::var("RWLOCK_BENCH_COMPETITORS") {
        Ok(v) => v
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect(),
        Err(_) => vec![1, 2, 4, 8],
    }
}

struct Config {
    ops: usize,
    rounds: usize,
    competitors: Vec<usize>,
    hold: usize,
    weight_sets: Vec<Weights>,
    tokio_workers: Option<usize>,
}

impl Config {
    fn from_env() -> Self {
        Config {
            ops: env_usize("RWLOCK_BENCH_OPS", 20_000),
            rounds: env_usize("RWLOCK_BENCH_ROUNDS", 3),
            competitors: env_competitors(),
            hold: env_usize("RWLOCK_BENCH_HOLD", 8),
            weight_sets: env_weight_sets(),
            tokio_workers: env::var("RWLOCK_BENCH_WORKERS")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }
}

/// 一节的测量结果：每个竞争者数量下，各实现的每次操作开销。
struct Section {
    title: &'static str,
    /// 每列 = (实现名, 各竞争数下的 ns/op)；第 0 列恒为 `atomic_sync`。
    columns: Vec<(&'static str, Vec<(usize, f64)>)>,
    /// 用来算"相对"的对照列下标。
    baseline: usize,
    /// 每个竞争者的操作数，用来换算"整轮总耗时"。
    ///
    /// `ns/op` 是 `总耗时 / (N × ops)`，属于**聚合吞吐**口径：竞争者越多，
    /// 分母越大。当工作负载被排他操作串行化时，总耗时几乎不随 N 变化，
    /// 于是 `ns/op` 会单调下降，让人误以为"N 越大越快"。同时打印总耗时
    /// 才能看出"每个竞争者跑完自己那份脚本要多久"。
    ops: usize,
}

impl Section {
    fn print(&self) {
        let Some((ours_name, ours)) = self.columns.first() else {
            return;
        };
        println!();
        println!("== {} ==", self.title);

        print!("{:>9} |", "竞争数");
        for (name, _) in &self.columns {
            print!(" {name:>18} |");
        }
        println!(" {:>13} | {:>12} | {:>12}", "整轮总耗时", "相对对照", "相对单竞争者");

        let base_ours = ours.first().map(|(_, v)| *v).unwrap_or(f64::NAN);
        let base_other = self.columns[self.baseline]
            .1
            .first()
            .map(|(_, v)| *v)
            .unwrap_or(f64::NAN);

        for (row, (n, ours_v)) in ours.iter().enumerate() {
            print!("{n:>9} |");
            for (_, col) in &self.columns {
                let v = col.get(row).map(|(_, v)| *v).unwrap_or(f64::NAN);
                print!(" {v:>15.1} ns |");
            }
            let other = self.columns[self.baseline]
                .1
                .get(row)
                .map(|(_, v)| *v)
                .unwrap_or(f64::NAN);
            // 整轮总耗时（毫秒）：ns/op × N × ops。
            let total_ms = ours_v * (*n as f64) * (self.ops as f64) / 1e6;
            println!(
                " {:>10.3} ms | {:>11.2}x | {:>11.2}x",
                total_ms,
                ratio(other, *ours_v),
                ratio(*ours_v, base_ours),
            );
        }

        let other_last = self.columns[self.baseline]
            .1
            .last()
            .map(|(_, v)| *v)
            .unwrap_or(f64::NAN);
        println!(
            "  （{ours_name} 相对 {}：N=1 {:.2}x → N={} {:.2}x；对照自身扩展比 {:.2}x）",
            self.columns[self.baseline].0,
            ratio(base_other, base_ours),
            ours.last().map(|(n, _)| *n).unwrap_or(0),
            ratio(other_last, ours.last().map(|(_, v)| *v).unwrap_or(f64::NAN)),
            ratio(other_last, base_other),
        );
        let _ = std::io::stdout().flush();
    }
}

fn progress(msg: impl fmt::Display) {
    println!("  ... {msg}");
    let _ = std::io::stdout().flush();
}

fn main() {
    let cfg = Config::from_env();
    println!("rwlock::cooperative 并发性能测试");
    println!(
        "每个竞争者 {} 次操作；每项取 {} 轮最好成绩；临界区自旋 {}；进程 {} 线程",
        cfg.ops,
        cfg.rounds,
        cfg.hold,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
    );

    for weights in cfg.weight_sets.clone() {
        println!();
        println!("################ 负载：{weights} ################");

        // -- tokio 多线程 ------------------------------------------------
        let mut ours = Vec::new();
        let mut theirs = Vec::new();
        for &n in &cfg.competitors {
            let scripts = gen_scripts(n, cfg.ops, weights);
            let total = n * cfg.ops;
            progress(format!("tokio 多线程, N={n}"));

            let workers = cfg.tokio_workers.unwrap_or(n);
            let t_ours = best_of(cfg.rounds, || {
                let scripts = scripts.clone();
                run_tokio(workers, move || {
                    let lock = Arc::new(TestLock::new_owned(0));
                    scripts
                        .into_iter()
                        .map(|s| our_worker(Arc::clone(&lock), s, cfg.hold))
                        .collect()
                })
            });
            let t_theirs = best_of(cfg.rounds, || {
                let scripts = scripts.clone();
                run_tokio(workers, move || {
                    let lock = Arc::new(tokio::sync::RwLock::new(0u64));
                    scripts
                        .into_iter()
                        .map(|s| tokio_worker(Arc::clone(&lock), s, cfg.hold))
                        .collect()
                })
            });

            ours.push((n, ns_per_op(t_ours, total)));
            theirs.push((n, ns_per_op(t_theirs, total)));
        }
        Section {
            title: "tokio 多线程执行器（N 个 worker 线程）",
            columns: vec![
                ("cooperative", ours),
                ("tokio::RwLock", theirs),
            ],
            baseline: 1,
            ops: cfg.ops,
        }
        .print();

        // -- compio 单线程 ----------------------------------------------
        let mut ours = Vec::new();
        let mut theirs = Vec::new();
        for &n in &cfg.competitors {
            let scripts = gen_scripts(n, cfg.ops, weights);
            let total = n * cfg.ops;
            progress(format!("compio 单线程, N={n}"));

            let t_ours = best_of(cfg.rounds, || {
                let scripts = scripts.clone();
                run_compio(move || {
                    let lock = Arc::new(TestLock::new_owned(0));
                    scripts
                        .into_iter()
                        .map(|s| our_worker(Arc::clone(&lock), s, cfg.hold))
                        .collect()
                })
            });
            let t_theirs = best_of(cfg.rounds, || {
                let scripts = scripts.clone();
                run_compio(move || {
                    let lock = Arc::new(tokio::sync::RwLock::new(0u64));
                    scripts
                        .into_iter()
                        .map(|s| tokio_worker(Arc::clone(&lock), s, cfg.hold))
                        .collect()
                })
            });

            ours.push((n, ns_per_op(t_ours, total)));
            theirs.push((n, ns_per_op(t_theirs, total)));
        }
        Section {
            title: "compio 单线程执行器（N 个任务）",
            columns: vec![
                ("cooperative", ours),
                ("tokio::RwLock", theirs),
            ],
            baseline: 1,
            ops: cfg.ops,
        }
        .print();

        // -- OS 线程：三个线程模型下的实现互为对照 ------------------------
        let mut ours = Vec::new();
        let mut preemptive = Vec::new();
        let mut theirs = Vec::new();
        for &n in &cfg.competitors {
            let scripts = gen_scripts(n, cfg.ops, weights);
            let total = n * cfg.ops;
            progress(format!("OS 线程, N={n}"));

            let t_ours = best_of(cfg.rounds, || run_threads_our(&scripts, cfg.hold));
            let t_pre = best_of(cfg.rounds, || {
                run_threads_preemptive(&scripts, cfg.hold)
            });
            let t_theirs = best_of(cfg.rounds, || run_threads_std(&scripts, cfg.hold));

            ours.push((n, ns_per_op(t_ours, total)));
            preemptive.push((n, ns_per_op(t_pre, total)));
            theirs.push((n, ns_per_op(t_theirs, total)));
        }
        Section {
            title: "N 个 OS 线程（cooperative 用 block_on 驱动；其余为原生阻塞/自旋）",
            columns: vec![
                ("cooperative", ours),
                ("preemptive", preemptive),
                ("std::RwLock", theirs),
            ],
            baseline: 2,
            ops: cfg.ops,
        }
        .print();
    }

    println!();
    println!("说明：");
    println!("- “相对对照” > 1 表示 atomic_sync 更快；");
    println!("- “相对单竞争者”是本实现在 N 个竞争者下相对 N=1 的开销倍数，");
    println!("  越接近 1 说明扩展性越好；");
    println!("- OS 线程一节的三个实现线程模型不同：cooperative 用 block_on 驱动，");
    println!("  preemptive 是自旋等待（用 try_* + yield 实现，避免既有实现的活锁），");
    println!("  std::RwLock 是内核阻塞原语；因此该节只作参考，不是同类对照；");
    println!("- preemptive 没有「升级不被饿死」的保证，脚本里的升级操作在它这侧");
    println!("  退化为 write()（原因见开发日志 §2.2）。");
}
