//! **注意：本 bench 默认不会运行。**
//!
//! 它没有在 `Cargo.toml` 里声明 `harness = false`，所以
//! `cargo bench --bench mutex_contention` 实际走的是 libtest harness，
//! 输出为 `running 0 tests`，下面的 `main` **不会**被执行。
//!
//! 这是有意为之：本文件是 2,000,000 次操作 × 3 轮 × 6 种线程数 × 3 个实现的
//! 重量级测量，不适合默认跑。需要它时，在 `Cargo.toml` 里取消注释
//! `[[bench]] name = "mutex_contention" harness = false` 即可。
//!
//! Contention benchmark: `mutex::preemptive::SpinningMutex` vs
//! `std::sync::Mutex`, with 3 to 8 threads hammering a shared counter.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p atomic_sync --bench mutex_contention
//! ```
//!
//! Note: the spin lock's blocking path (`lock().wait()` /
//! `may_break_with`) livelocks under contention (see the `#[ignore]`d tests
//! in `mutex::preemptive_tests_`), so this benchmark uses the working
//! `try_lock` + `yield_now` spin loop for the spin lock, and the std mutex's
//! blocking `lock()`.

use std::{
    sync::{Arc, Barrier, Mutex as StdMutex},
    thread,
    time::Instant,
    vec::Vec,
};

use atomex::{LocksOrderings, StrictOrderings};
use atomic_sync::mutex::preemptive::{
    MsbAsMutexSignal, SpinningMutexOwned,
};

type SpinStrict = SpinningMutexOwned<usize, std::sync::atomic::AtomicUsize, MsbAsMutexSignal<usize>, StrictOrderings>;
type SpinLocks = SpinningMutexOwned<usize, std::sync::atomic::AtomicUsize, MsbAsMutexSignal<usize>, LocksOrderings>;

const THREADS: &[usize] = &[3, 4, 5, 6, 7, 8];
const TOTAL_OPS: usize = 2_000_000;
const ROUNDS: usize = 3;

macro_rules! define_spin_bench {
    ($name:ident, $ty:ty) => {
        fn $name(lock: &Arc<$ty>, n_threads: usize, iters: usize) -> f64 {
            let barrier = Arc::new(Barrier::new(n_threads + 1));
            let handles: Vec<_> = (0..n_threads)
                .map(|_| {
                    let lock = lock.clone();
                    let barrier = barrier.clone();
                    thread::spawn(move || {
                        let mut acq = lock.lock_session();
                        barrier.wait();
                        let mut done = 0usize;
                        while done < iters {
                            let Result::Ok(mut g) = acq.try_lock() else {
                                thread::yield_now();
                                continue;
                            };
                            *g += 1;
                            done += 1;
                        }
                    })
                })
                .collect();
            let t = Instant::now();
            barrier.wait();
            for h in handles {
                h.join().unwrap();
            }
            t.elapsed().as_secs_f64()
        }
    };
}

define_spin_bench!(bench_spin_strict, SpinStrict);
define_spin_bench!(bench_spin_locks, SpinLocks);

fn bench_std(lock: &Arc<StdMutex<usize>>, n_threads: usize, iters: usize) -> f64 {
    let barrier = Arc::new(Barrier::new(n_threads + 1));
    let handles: Vec<_> = (0..n_threads)
        .map(|_| {
            let lock = lock.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..iters {
                    *lock.lock().unwrap() += 1;
                }
            })
        })
        .collect();
    let t = Instant::now();
    barrier.wait();
    for h in handles {
        h.join().unwrap();
    }
    t.elapsed().as_secs_f64()
}

fn best_of<F: FnMut() -> f64>(rounds: usize, mut f: F) -> f64 {
    // One warm-up round, then take the minimum of `rounds` measured rounds.
    f();
    (0..rounds).map(|_| f()).fold(f64::INFINITY, f64::min)
}

fn main() {
    println!(
        "{:>6} | {:>13} | {:>13} | {:>13} | {:>8} | {:>8}",
        "threads", "spin(seqcst)", "spin(acquire)", "std::Mutex", "vs std", "locks/spin"
    );

    for &n_threads in THREADS {
        let iters = TOTAL_OPS / n_threads;

        let spin_strict = Arc::new(SpinStrict::new_owned(0));
        let spin_locks = Arc::new(SpinLocks::new_owned(0));
        let std_mutex = Arc::new(StdMutex::new(0));

        let t_spin_strict = best_of(ROUNDS, || bench_spin_strict(&spin_strict, n_threads, iters));
        let t_spin_locks = best_of(ROUNDS, || bench_spin_locks(&spin_locks, n_threads, iters));
        let t_std = best_of(ROUNDS, || bench_std(&std_mutex, n_threads, iters));

        let ops = TOTAL_OPS as f64;
        let spin_strict_ops = ops / t_spin_strict;
        let spin_locks_ops = ops / t_spin_locks;
        let std_ops = ops / t_std;

        println!(
            "{n_threads:>6} | {:>13.0} | {:>13.0} | {:>13.0} | {:>7.2}x | {:>7.2}x",
            spin_strict_ops,
            spin_locks_ops,
            std_ops,
            spin_strict_ops / std_ops,
            spin_locks_ops / spin_strict_ops,
        );
    }
}
