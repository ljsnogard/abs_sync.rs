use core::{
    borrow::BorrowMut,
    cell::UnsafeCell,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::atomic::*,
};

use funty::Unsigned;

use atomex::{
    x_deps::funty,
    CmpxchResult, StrictOrderings,
    TrAtomicCell, TrAtomicData, TrAtomicFlags, TrCmpxchOrderings,
};
use abs_cancel::TrCancellationToken;
use abs_sync::{
    may_break::TrMayBreak,
    sync_mutex::*,
    x_deps::abs_cancel,
};

pub use super::error_::MutexError;

/// An helper trait to define spinlock behaviour
pub trait TrMutexSignal<V>
where
    V: Copy,
{
    fn is_acquired(val: V) -> bool;

    fn is_released(val: V) -> bool {
        !Self::is_acquired(val)
    }

    fn make_acquired(val: V) -> V;

    fn make_released(val: V) -> V;
}

/// 升级方向，可以设置某一个 bit 作为 flag
#[derive(Debug)]
pub struct MsbAsMutexSignal<V: Unsigned>(PhantomData<V>);

impl<V: Unsigned> MsbAsMutexSignal<V> {
    #[allow(non_snake_case)]
    #[inline(always)]
    pub fn K_MSB_FLAG() -> V {
        V::ONE << (V::BITS - 1)
    }
}

impl<V: Unsigned> TrMutexSignal<V> for MsbAsMutexSignal<V> {
    fn is_acquired(val: V) -> bool {
        val & Self::K_MSB_FLAG() == Self::K_MSB_FLAG()
    }

    fn make_acquired(val: V) -> V {
        val | Self::K_MSB_FLAG()
    }

    fn make_released(val: V) -> V {
        val & (!Self::K_MSB_FLAG())
    }
}

#[derive(Debug)]
pub struct PtrAsMutexSignal<T: Sized>(PhantomData<*mut T>);

impl<T: Sized> PtrAsMutexSignal<T> {
    const K_MOD: usize = 2;
    const K_RES: usize = 1;
}

impl<T: Sized> TrMutexSignal<*mut T> for PtrAsMutexSignal<T> {
    fn is_acquired(val: *mut T) -> bool {
        (val as usize) % Self::K_MOD == Self::K_RES
    }

    fn make_acquired(val: *mut T) -> *mut T {
        ((val as usize) + Self::K_RES) as *mut T
    }

    fn make_released(val: *mut T) -> *mut T {
        ((val as usize) - Self::K_RES) as *mut T
    }
}

pub type SpinningMutexEmbedded<
        'a,
        T,
        C = AtomicUsize,
        S = MsbAsMutexSignal<<C as TrAtomicCell>::Value>,
        O = StrictOrderings,
    > = SpinningMutex<T, <C as TrAtomicCell>::Value, &'a mut C, S, O>;

pub type SpinningMutexOwned<
        T,
        C = AtomicUsize,
        S = MsbAsMutexSignal<<C as TrAtomicCell>::Value>,
        O = StrictOrderings,
    > = SpinningMutex<T, <C as TrAtomicCell>::Value, C, S, O>;

impl<'a, T, C, S, O> SpinningMutexEmbedded<'a, T, C, S, O>
where
    C: TrAtomicCell<Value: TrAtomicData<AtomicCell = C> + Copy + Default>,
    S: TrMutexSignal<<C as TrAtomicCell>::Value>,
    O: TrCmpxchOrderings,
{
    pub fn new_embedded(data: T, cell: &'a mut C) -> Self {
        let val = <<C as TrAtomicCell>::Value as Default>::default();
        cell.store(val, Ordering::Relaxed);
        SpinningMutexEmbedded::<T, C, S, O>::new(data, cell)
    }
}

impl<T, C, S, O> SpinningMutexOwned<T, C, S, O>
where
    C: TrAtomicCell<Value: TrAtomicData<AtomicCell = C> + Copy + Default>,
    S: TrMutexSignal<<C as TrAtomicCell>::Value>,
    O: TrCmpxchOrderings,
{
    pub fn new_owned(data: T) -> Self {
        let val = <<C as TrAtomicCell>::Value as Default>::default();
        let cell = <C as TrAtomicCell>::new(val);
        SpinningMutexOwned::<T, C, S, O>::new(data, cell)
    }
}

/// A configurable spinlock implementation, usually for further encapsulation.
#[derive(Debug)]
pub struct SpinningMutex<
    T,
    D = usize,
    B = AtomicUsize,
    S = MsbAsMutexSignal<D>,
    O = StrictOrderings>
where
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    _use_s: PhantomData<S>,
    _use_d: PhantomData<D>,
    _use_o: PhantomData<O>,
    state_: B,
    value_: UnsafeCell<T>,
}

impl<T, D, B, S, O> SpinningMutex<T, D, B, S, O>
where
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    pub const fn new(data: T, cell: B) -> Self {
        SpinningMutex {
            _use_s: PhantomData,
            _use_d: PhantomData,
            _use_o: PhantomData,
            state_: cell,
            value_: UnsafeCell::new(data),
        }
    }

    pub fn into_inner(self) -> T {
        self.value_.into_inner()
    }
}

impl<T, D, B, S, O> SpinningMutex<T, D, B, S, O>
where
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    pub fn is_acquired(&self) -> bool {
        let state = TrAtomicFlags::value(self);
        S::is_acquired(state)
    }

    pub fn lock_session(&self) -> LockSession<'_, T, D, B, S, O> {
        LockSession(self)
    }

    pub fn as_mut_ptr(&self) -> *mut T {
        self.value_.get()
    }
}

impl<T, D, B, S, O> AsRef<D::AtomicCell> for SpinningMutex<T, D, B, S, O>
where
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    fn as_ref(&self) -> &D::AtomicCell {
        self.state_.borrow()
    }
}

impl<T, D, B, S, O> TrAtomicFlags<D, O> for SpinningMutex<T, D, B, S, O>
where
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{}

impl<T, D, B, S, O> TrSyncMutex for SpinningMutex<T, D, B, S, O>
where
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    type Target = T;

    type LockSess<'f> = LockSession<'f, T, D, B, S, O>
    where
        Self: 'f;

    #[inline]
    fn lock_session(&self) -> Self::LockSess<'_> {
        SpinningMutex::lock_session(self)
    }
}

unsafe impl<T, D, B, S, O> Send for SpinningMutex<T, D, B, S, O>
where
    T: Sync + ?Sized,
    D: Unsigned + TrAtomicData,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{}

unsafe impl<T, D, B, S, O> Sync for SpinningMutex<T, D, B, S, O>
where
    T: Sync + ?Sized,
    D: Unsigned + TrAtomicData,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{}

pub struct LockSession<'a, T, D, B, S, O>(&'a SpinningMutex<T, D, B, S, O>)
where
    T: 'a + ?Sized,
    D: 'a + TrAtomicData + Copy,
    B: 'a + BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: 'a + TrMutexSignal<D>,
    O: TrCmpxchOrderings;

impl<'a, T, D, B, S, O> LockSession<'a, T, D, B, S, O>
where
    T: 'a + ?Sized,
    D: 'a + TrAtomicData + Copy,
    B: 'a + BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: 'a + TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    pub fn lock(&mut self) -> MayBreakLock<'a, '_, T, D, B, S, O> {
        MayBreakLock(self)
    }

    pub fn try_lock<'g>(
        &'g mut self,
    ) -> Result<MutexGuard<'a, 'g, T, D, B, S, O>, MutexError> {
        self.0
            .try_once_compare_exchange_weak(
                self.0.value(),
                S::is_released,
                S::make_acquired)
            .succ()
            .map(|_| MutexGuard::new(self))
            .ok_or(MutexError::Retry)
    }

    fn mutex_(&self) -> &'a SpinningMutex<T, D, B, S, O> {
        self.0
    }

    fn try_spin_acquire_<'g, C>(
        &'g mut self,
        cancel: C,
    ) -> Result<MutexGuard<'a, 'g, T, D, B, S, O>, MutexError>
    where
        C: TrCancellationToken,
    {
        let mut current = self.0.value();
        loop {
            // Poll the cancellation token on every iteration, before
            // attempting the CAS, so a pre-cancelled token never acquires
            // the lock and a cancellation arriving while waiting is honoured
            // promptly.
            if cancel.is_cancelled() {
                break Result::Err(MutexError::Cancelled);
            }
            match self.mutex_().try_once_compare_exchange_weak(
                current,
                S::is_released,
                S::make_acquired,
            ) {
                CmpxchResult::Unexpected(_) =>
                    // The cached `current` no longer satisfies
                    // `S::is_released` (e.g. another thread acquired the
                    // lock in the meantime). Reload the actual state so the
                    // loop keeps making progress and can succeed once the
                    // lock is released; continuing with the stale value
                    // would livelock forever.
                    current = self.0.value(),
                CmpxchResult::Succ(_) =>
                    break Result::Ok(MutexGuard::new(self)),
                CmpxchResult::Fail(x) =>
                    current = x,
            }
        }
    }

    fn try_spin_release_(&self) -> bool {
        self.0
            .try_spin_compare_exchange_weak(
                S::is_acquired,
                S::make_released)
            .is_succ()
    }
}

impl<'a, T, D, B, S, O> TrSyncMutexSession<'a, T> for
    LockSession<'a, T, D, B, S, O>
where
    T: 'a + ?Sized,
    D: 'a + TrAtomicData + Copy,
    B: 'a + BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: 'a + TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    type Guard<'g> = MutexGuard<'a, 'g, T, D, B, S, O> where 'a: 'g;

    type LockMayBreak<'f> = MayBreakLock<'a, 'f, T, D, B, S, O>
    where
        'a: 'f;

    type Err = MutexError;

    #[inline]
    fn try_lock<'g>(&'g mut self) -> Result<Self::Guard<'g>, Self::Err>
    where
        'a: 'g,
    {
        LockSession::try_lock(self)
    }

    #[inline]
    fn lock<'g>(&'g mut self) -> Self::LockMayBreak<'g>
    where
        'a: 'g,
    {
        LockSession::lock(self)
    }
}

pub struct MayBreakLock<'a, 'g, T, D, B, S, O>(&'g mut LockSession<'a, T, D, B, S, O>)
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: 'a + BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, D, B, S, O> MayBreakLock<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: 'a + BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    pub fn may_break_with<C>(
        self,
        cancel: C,
    ) -> Result<MutexGuard<'a, 'g, T, D, B, S, O>, MutexError>
    where
        C: TrCancellationToken,
    {
        self.0.try_spin_acquire_(cancel)
    }

    #[inline]
    pub fn wait(self) -> Result<MutexGuard<'a, 'g, T, D, B, S, O>, MutexError> {
        TrMayBreak::wait(self)
    }
}

impl<'a, 'g, T, D, B, S, O> TrMayBreak for MayBreakLock<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    type MayBreakOutput = Result<
        MutexGuard<'a, 'g, T, D, B, S, O>,
        MutexError,
    >;

    #[inline]
    fn may_break_with<C>(
        self,
        cancel: C,
    ) -> Self::MayBreakOutput
    where
        C: TrCancellationToken,
    {
        MayBreakLock::may_break_with(self, cancel)
    }
}

pub struct MutexGuard<'a, 'g, T, D, B, S, O>(&'g mut LockSession<'a, T, D, B, S, O>)
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, D, B, S, O> MutexGuard<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(
        acquire: &'g mut LockSession<'a, T, D, B, S, O>,
    ) -> Self {
        MutexGuard(acquire)
    }
}

impl<'a, 'g, T, D, B, S, O> Drop for MutexGuard<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        let _ = self.0.try_spin_release_();
    }
}

impl<'a, 'g, T, D, B, S, O> Deref for MutexGuard<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let opt = unsafe { self.0.mutex_().value_.get().as_ref() };
        let Option::Some(t) = opt else {
            unreachable!("[embedded::MutexGuard::deref]")
        };
        t
    }
}

impl<'a, 'g, T, D, B, S, O> DerefMut for MutexGuard<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        let opt = unsafe { self.0.mutex_().value_.get().as_mut() };
        let Option::Some(t) = opt else {
            unreachable!("[embedded::MutexGuard::deref_mut]")
        };
        t
    }
}

impl<'a, 'g, T, D, B, S, O> TrAcqRefGuard<'a, 'g, T>
for MutexGuard<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{}

impl<'a, 'g, T, D, B, S, O> TrAcqMutGuard<'a, 'g, T>
for MutexGuard<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{}

unsafe impl<'a, 'g, T, D, B, S, O> Sync for MutexGuard<'a, 'g, T, D, B, S, O>
where
    'a: 'g,
    T: ?Sized,
    D: TrAtomicData + Copy,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    S: TrMutexSignal<D>,
    O: TrCmpxchOrderings,
{}


#[cfg(test)]
mod tests_ {
    use std::{
        boxed::Box,
        mem,
        ptr,
        sync::Arc,
        sync::atomic::{AtomicUsize, AtomicPtr, Ordering},
    };

    use atomex::{LocksOrderings, StrictOrderings};
    use crate::{mutex::smoke_tests_, x_deps::atomex};
    use super::{
        MsbAsMutexSignal, PtrAsMutexSignal,
        SpinningMutexEmbedded, SpinningMutexOwned,
    };

    #[test]
    fn usize_smoke_test() {
        smoke_tests_::usize_smoke_test(
            SpinningMutexOwned::<usize>::new_owned,
            SpinningMutexOwned::<usize>::as_mut_ptr,
        )
    }

    #[test]
    fn try_acquired_smoke() {
        smoke_tests_::try_acquired_smoke(SpinningMutexOwned::<usize>::new_owned)
    }

    #[test]
    fn multithreaded_smoke_strict_orderings() {
        smoke_tests_::multithreaded_usize_smoke_(&Arc::new(
            SpinningMutexOwned::<
                    usize,
                    AtomicUsize,
                    MsbAsMutexSignal<usize>,
                    StrictOrderings,
                >::new_owned(0)),
            SpinningMutexOwned::as_mut_ptr,
        )
    }

    #[test]
    fn multithreaded_smoke_locks_orderings() {
        smoke_tests_::multithreaded_usize_smoke_(&Arc::new(
            SpinningMutexOwned::<
                    usize,
                    AtomicUsize,
                    MsbAsMutexSignal<usize>,
                    LocksOrderings,
                >::new_owned(0)),
            SpinningMutexOwned::as_mut_ptr,
        )
    }

    #[test]
    fn ptr_smoke_test() {
        const ANSWER: usize = 42;
        const PTR_SIZE: usize = mem::size_of::<*mut usize>();

        let mut cell = Box::new(AtomicPtr::new(PTR_SIZE as *mut usize));
        let ptr = cell.load(Ordering::Relaxed);
        let mut data = Box::new(ANSWER);
        let lock = SpinningMutexEmbedded
            ::<&mut usize, AtomicPtr<usize>, PtrAsMutexSignal<usize>, StrictOrderings>
            ::new(&mut data, &mut cell);

        let mut acq = lock.lock_session();
        let g = acq.lock().wait().unwrap();
        assert_eq!(**g, ANSWER);
        drop(g);
        assert!(ptr::eq(ptr, cell.load(Ordering::Relaxed)))
    }
}
