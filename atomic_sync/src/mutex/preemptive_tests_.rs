//! Boundary-condition tests for `mutex::preemptive`.
//!
//! These tests target the edge cases of the two mutex signals
//! ([`MsbAsMutexSignal`], [`PtrAsMutexSignal`]) and of the lock state
//! machine, without modifying the implementation.
//!
//! The two contention tests at the bottom are regression tests for a livelock
//! that used to live in [`Acquire::try_spin_acquire_`] (the code path behind
//! [`MayBreakLock::wait`] / [`MayBreakLock::may_break_with`]): when another
//! thread held the lock, the spin loop kept passing a stale "acquired" value
//! to `try_once_compare_exchange_weak`, which kept answering
//! `CmpxchResult::Unexpected`, and the loop `continue`d forever without
//! reloading the state or consulting the cancellation token. They now verify
//! that the loop reloads the state and honours the token.

use std::{
    boxed::Box,
    string::String,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
    vec::Vec,
};

use atomex::StrictOrderings;
use abs_sync::x_deps::abs_cancel::CancelledToken;

use super::preemptive::{
    MsbAsMutexSignal, PtrAsMutexSignal, SpinningMutexEmbedded, SpinningMutexOwned,
    TrMutexSignal,
};

// -------------------------------------------------------------------------
// Signal functions: boundary values
// -------------------------------------------------------------------------

#[test]
fn msb_signal_boundaries() {
    type Sig = MsbAsMutexSignal<usize>;
    let msb = Sig::K_MSB_FLAG();

    // Released state is the default.
    assert!(!Sig::is_acquired(0));
    assert!(Sig::is_released(0));

    // Acquire sets the MSB; release clears it.
    assert_eq!(Sig::make_acquired(0), msb);
    assert!(Sig::is_acquired(msb));
    assert_eq!(Sig::make_released(msb), 0);

    // Data bits are preserved across acquire/release round-trips.
    let data = 0x0F0Fusize;
    assert_eq!(Sig::make_acquired(data) & !msb, data);
    assert_eq!(Sig::make_released(Sig::make_acquired(data)), data);

    // Boundary: all data bits set (MSB clear) is still "released".
    let all_data = !msb;
    assert!(!Sig::is_acquired(all_data));

    // Boundary: all bits set is "acquired" and releases back to all-data.
    assert!(Sig::is_acquired(usize::MAX));
    assert_eq!(Sig::make_released(usize::MAX), all_data);

    // Make-again on an already acquired value keeps it acquired.
    assert!(Sig::is_acquired(Sig::make_acquired(msb)));
}

#[allow(clippy::zero_ptr)]
#[test]
fn ptr_signal_boundaries() {
    type Sig = PtrAsMutexSignal<usize>;

    let null = 0usize as *mut usize;
    let even = 0x10usize as *mut usize;
    let odd = 0x11usize as *mut usize;

    // Null / even pointers are "released".
    assert!(Sig::is_released(null));
    assert!(Sig::is_released(even));

    // Acquire makes the pointer odd; release makes it even again.
    assert!(Sig::is_acquired(Sig::make_acquired(even)));
    assert_eq!(Sig::make_released(Sig::make_acquired(even)), even);
    assert_eq!(Sig::make_acquired(null) as usize, 1);

    // Boundary: an *odd* pointer is mis-detected as acquired. The signal
    // scheme silently requires 2-aligned pointers; this is not enforced
    // anywhere, so a user who stores an odd pointer into the cell gets a
    // mutex that looks permanently locked.
    assert!(Sig::is_acquired(odd));
    // `make_released` on an odd pointer produces an *even* pointer that is a
    // different address than the original — i.e. the round-trip is broken for
    // non-2-aligned pointers.
    assert_eq!(Sig::make_released(odd) as usize, 0x10);
}

#[allow(clippy::manual_dangling_ptr)]
#[test]
fn ptr_signal_odd_cell_breaks_mutex() {
    // Construct an embedded mutex whose cell already holds an ODD pointer.
    // Because `new` (unlike `new_embedded`) does not reset the cell, the
    // mutex believes it is permanently acquired: `try_lock` must fail.
    let mut data = Box::new(7usize);
    let mut cell = Box::new(AtomicPtr::new(0x3usize as *mut usize));
    let lock = SpinningMutexEmbedded::<
        &mut usize,
        AtomicPtr<usize>,
        PtrAsMutexSignal<usize>,
        StrictOrderings,
    >::new(&mut data, &mut cell);

    assert!(lock.is_acquired());
    let mut acq = lock.lock_session();
    assert!(acq.try_lock().is_err());
}

// -------------------------------------------------------------------------
// Lock state machine: single thread
// -------------------------------------------------------------------------

#[test]
fn try_lock_lifecycle() {
    let mutex = SpinningMutexOwned::<usize>::new_owned(42);
    assert!(!mutex.is_acquired());

    let mut acq1 = mutex.lock_session();
    let g = acq1.try_lock().expect("first try_lock should succeed");
    assert!(mutex.is_acquired());
    assert_eq!(*g, 42);

    // The lock is held: a second try_lock (via another Acquire handle) must
    // fail without blocking.
    let mut acq2 = mutex.lock_session();
    assert!(acq2.try_lock().is_err());

    drop(g);
    assert!(!mutex.is_acquired());
    let g = acq2.try_lock().expect("try_lock after release");
    assert_eq!(*g, 42);
}

#[test]
fn lock_wait_single_thread() {
    let mutex = SpinningMutexOwned::<usize>::new_owned(1);
    let mut acq = mutex.lock_session();
    {
        let mut g = acq.lock().wait().expect("lock().wait()");
        *g += 1;
        assert_eq!(*g, 2);
    }
    {
        let g = acq.lock().wait().expect("re-lock");
        assert_eq!(*g, 2);
    }
}

#[test]
fn embedded_new_resets_cell_to_default() {
    // `new_embedded` overwrites the cell with the default (released) value,
    // so a dirty cell must not break the mutex.
    let data = 9usize;
    let mut cell = AtomicUsize::new(usize::MAX); // dirty
    let lock = SpinningMutexEmbedded::<usize, AtomicUsize>::new_embedded(
        data,
        &mut cell,
    );
    assert!(!lock.is_acquired());
    let mut acq = lock.lock_session();
    let g = acq.try_lock().expect("try_lock after reset");
    assert_eq!(*g, 9);
}

#[test]
fn into_inner_roundtrip() {
    let mutex = SpinningMutexOwned::<String>::new_owned(String::from("hi"));
    // (String is used so `into_inner` is exercised on an owned value.)
    assert_eq!(mutex.into_inner(), "hi");
}

// -------------------------------------------------------------------------
// Cancellation semantics (safe cases)
// -------------------------------------------------------------------------

#[test]
fn may_break_precancelled_token_never_acquires() {
    // `TrMayBreak` documents that the job must poll the cancellation token
    // for signal; a pre-cancelled token must therefore never acquire the
    // lock, even when it is immediately available.
    let mutex = SpinningMutexOwned::<usize>::new_owned(3);
    let mut acq = mutex.lock_session();

    let g = acq.lock().may_break_with(CancelledToken::new());
    assert!(g.is_err(), "pre-cancelled token must not acquire");
    assert!(!mutex.is_acquired());
}

// -------------------------------------------------------------------------
// Lock state machine: contention via try_lock (bounded, no livelock)
// -------------------------------------------------------------------------

#[test]
fn contended_increment_3_to_8_threads() {
    for n_threads in 3..=8usize {
        let counter = Arc::new(SpinningMutexOwned::<usize>::new_owned(0));
        let iters = 1000usize;
        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let c = counter.clone();
                thread::spawn(move || {
                    let mut acq = c.lock_session();
                    let mut done = 0usize;
                    while done < iters {
                        // try_lock never livelocks: it is a single CAS
                        // attempt, so busy-waiting with it is safe.
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
        for h in handles {
            h.join().unwrap();
        }
        let final_val = unsafe { counter.as_mut_ptr().read() };
        assert_eq!(
            final_val,
            n_threads * iters,
            "lost increments with {n_threads} threads"
        );
    }
}

// -------------------------------------------------------------------------
// Livelock demonstrations (gated; reveal a bug in `try_spin_acquire_`)
// -------------------------------------------------------------------------

fn held_lock_livelock_probe<F>(work: F) -> mpsc::Receiver<()>
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        work();
        let _ = tx.send(());
    });
    rx
}

#[test]
fn lock_wait_acquires_after_holder_releases() {
    // Regression: under contention the spin loop used to pass a stale
    // "acquired" value to `try_once_compare_exchange_weak`, which answered
    // `CmpxchResult::Unexpected` forever, so `lock().wait()` never acquired
    // even after the holder released. The loop must reload the state and
    // succeed once the lock becomes free.
    let mutex = Arc::new(SpinningMutexOwned::<usize>::new_owned(0));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let holder = {
        let m = mutex.clone();
        let started = started.clone();
        thread::spawn(move || {
            let mut acq = m.lock_session();
            let _g = acq.lock().wait().unwrap();
            started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(300));
        })
    };
    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    let rx = held_lock_livelock_probe(move || {
        let mut acq = mutex.lock_session();
        let _g = acq.lock().wait().expect("should eventually acquire");
    });

    let timeout = Duration::from_secs(2);
    match rx.recv_timeout(timeout) {
        Ok(()) => {
            holder.join().unwrap();
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "LIVELOCK: lock().wait() did not acquire even after the \
                 holder released the lock within {timeout:?}"
            )
        }
        Err(e) => panic!("{e:?}"),
    }
}

#[test]
fn may_break_with_cancelled_token_returns_none_while_held() {
    // Regression: a pre-cancelled token must make `may_break_with` return
    // `None` promptly. The spin loop used to skip the cancellation check on
    // the `Unexpected` path and spin forever while another thread held the
    // lock; now the token is polled on every iteration.
    let mutex = Arc::new(SpinningMutexOwned::<usize>::new_owned(0));
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let holder = {
        let m = mutex.clone();
        let started = started.clone();
        thread::spawn(move || {
            let mut acq = m.lock_session();
            let _g = acq.lock().wait().unwrap();
            started.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(300));
        })
    };
    while !started.load(Ordering::SeqCst) {
        thread::yield_now();
    }

    let rx = held_lock_livelock_probe(move || {
        let mut acq = mutex.lock_session();

        let g = acq.lock().may_break_with(CancelledToken::new());
        assert!(g.is_err(), "pre-cancelled token must not acquire");
    });

    let timeout = Duration::from_secs(2);
    match rx.recv_timeout(timeout) {
        Ok(()) => {
            holder.join().unwrap();
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "LIVELOCK: may_break_with(pre-cancelled token) did not \
                 return within {timeout:?}"
            )
        }
        Err(e) => panic!("{e:?}"),
    }
}
