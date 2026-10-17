use core::{
    borrow::BorrowMut,
    cell::UnsafeCell,
    fmt,
    marker::{PhantomData, PhantomPinned},
    mem::ManuallyDrop,
    sync::atomic::*,
};

use funty::{Integral, Unsigned};

use atomex::{
    fetch::Bitwise,
    x_deps::funty,
    StrictOrderings, TrAtomicCell, TrAtomicData, TrAtomicFlags,
    TrCmpxchOrderings,
};
use abs_cancel::TrCancellationToken;
use abs_sync::{
    sync_rwlock::*,
    x_deps::abs_cancel,
};

use crate::rwlock::TrShareMut;
use super::{
    error_::RwLockError,
    reader_::{MayBreakRead, ReaderGuard},
    writer_::{MayBreakWrite, WriterGuard},
    upgrade_::{MayBreakUpgradableRead, UpgradableReaderGuard},
};

pub type SpinningRwLockBorrowed<'a, T, C = AtomicUsize, O = StrictOrderings> =
    SpinningRwLock<T, <C as TrAtomicCell>::Value, &'a mut C, O>;

pub type SpinningRwLockOwned<T, C = AtomicUsize, O = StrictOrderings> =
    SpinningRwLock<T, <C as TrAtomicCell>::Value, C, O>;

impl<T, C, O> SpinningRwLockOwned<T, C, O>
where
    C: TrAtomicCell + Bitwise,
    <C as TrAtomicCell>::Value: TrAtomicData<AtomicCell = C> + Unsigned,
    O: TrCmpxchOrderings,
{
    pub fn new_owned(data: T) -> Self {
        let val = <<C as TrAtomicCell>::Value as Integral>::ZERO;
        let cell = <C as TrAtomicCell>::new(val);
        SpinningRwLockOwned::<T, C, O>::new(data, cell)
    }
}

#[derive(Debug)]
pub struct SpinningRwLock<T, D = usize, B = AtomicUsize, O = StrictOrderings>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    _pin_: PhantomPinned,
    stat_: RwLockState<D, B, O>,
    data_: UnsafeCell<T>,
}

impl<T, D, B, O> SpinningRwLock<T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    /// Creates a new spinlock wrapping the supplied data.
    pub const fn new(data: T, cell: B) -> Self {
        SpinningRwLock {
            _pin_: PhantomPinned,
            stat_: RwLockState::new(cell),
            data_: UnsafeCell::new(data),
        }
    }

    /// Consumes this `RwLock`, returning the underlying data.
    #[inline]
    pub fn into_inner(self) -> T {
        // We know statically that there are no outstanding references to
        // `self` so there's no need to lock.
        self.data_.into_inner()
    }
}

impl<T, D, B, O> SpinningRwLock<T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    /// Return the number of readers that currently hold the lock, including
    /// upgradable readers and upgraded writer.
    ///
    /// ## Safety
    ///
    /// This function provides no synchronization guarantees and so its result
    /// should be considered 'out of date' the instant it is called. Do not use
    /// it for synchronization purposes. However, it may be useful as a
    /// heuristic.
    ///
    /// ## Example
    ///
    /// ```
    /// use atomic_sync::rwlock::preemptive::SpinningRwLockOwned;
    /// use atomic_sync::x_deps::abs_sync::sync_rwlock::TrSyncUpgradableReaderGuard;
    ///
    /// let lock = SpinningRwLockOwned::<()>::new_owned(());
    /// assert_eq!(lock.reader_count(), 0);
    ///
    /// let mut acq0 = lock.acquire_session();
    /// let r0 = acq0.try_upgradable_read().unwrap();
    /// assert_eq!(lock.reader_count(), 1);
    ///
    /// let mut acq1 = lock.acquire_session();
    /// let r1 = acq1.try_read().unwrap();
    /// assert_eq!(lock.reader_count(), 2);
    ///
    /// let mut upg = r0.upgrade_session();
    /// assert_eq!(lock.reader_count(), 2);
    ///
    /// drop(r1);
    /// assert_eq!(lock.reader_count(), 1);
    ///
    /// let writer_guard = upg.upgrade().wait_or(|| unreachable!());
    /// assert_eq!(lock.reader_count(), 1);
    /// ```
    pub fn reader_count(&self) -> usize {
        let Result::Ok(c) = self.stat_.reader_count().try_into() else {
            unreachable!("[SpinningRwLock::reader_count]")
        };
        c
    }

    pub const fn acquire_session(&self) -> AcqSession<'_, T, D, B, O> {
        AcqSession::new(self)
    }

    /// Returns a mutable pointer to the underlying data.
    ///
    /// This is mostly meant to be used for applications which require manual
    /// unlocking, but where storing both the lock and the pointer to the inner
    /// data gets inefficient.
    ///
    /// While this is safe, writing to the data is undefined behavior unless the
    /// current thread has acquired a writer guard, and reading requires either
    /// a reader or writer guard.
    ///
    /// # Example
    /// ```
    /// use core::mem::ManuallyDrop;
    /// use atomic_sync::rwlock::preemptive::SpinningRwLockOwned;
    ///
    /// let lock = SpinningRwLockOwned::<usize>::new_owned(42);
    /// let mut acq = lock.acquire_session();
    /// unsafe {
    ///     let mut m = ManuallyDrop::new(acq.write().wait_or(|| unreachable!()));
    ///
    ///     assert_eq!(lock.as_mut_ptr().read(), 42);
    ///     lock.as_mut_ptr().write(58);
    ///
    ///     ManuallyDrop::drop(&mut m);
    /// }
    ///
    /// assert_eq!(*(acq.read().wait_or(|| unreachable!())), 58);
    /// ```
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut T {
        self.data_.get()
    }

    fn state_(&self) -> &RwLockState<D, B, O> {
        &self.stat_
    }
}

impl<T, D, B, O> TrSyncRwLock for SpinningRwLock<T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Target = T;

    type AcqSess<'f> = AcqSession<'f, T, D, B, O> where Self: 'f;

    type Err = super::error_::RwLockError;

    #[inline]
    fn acq_session(&self) -> Self::AcqSess<'_> {
        SpinningRwLock::acquire_session(self)
    }
}

// Same unsafe impls as `std::sync::RwLock`
unsafe impl<T, D, B, O> Send for SpinningRwLock<T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{}

unsafe impl<T, D, B, O> Sync for SpinningRwLock<T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{}

#[derive(Debug)]
pub struct AcqSession<'a, T, D, B, O>(&'a SpinningRwLock<T, D, B, O>)
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, T, D, B, O> AcqSession<'a, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    #[inline]
    pub const fn new(lock: &'a SpinningRwLock<T, D, B, O>) -> Self {
        AcqSession(lock)
    }

    pub fn try_read(
        &mut self,
    ) -> Result<ReaderGuard<'a, '_, T, D, B, O>, RwLockError> {
        if self.0.state_().try_read() {
            Result::Ok(ReaderGuard::new(self))
        } else {
            Result::Err(RwLockError::Retry)
        }
    }

    pub fn try_write(
        &mut self,
    ) -> Result<WriterGuard<'a, '_, T, D, B, O>, RwLockError> {
        if self.0.state_().try_write() {
            Result::Ok(WriterGuard::new(self))
        } else {
            Result::Err(RwLockError::Retry)
        }
    }

    pub fn try_upgradable_read<'f>(
        &'f mut self,
    ) -> Result<
        UpgradableReaderGuard<'a, 'f, T, D, B, O>,
        RwLockError,
    > {
        if self.0.state_().try_upgradable_read() {
            Result::Ok(UpgradableReaderGuard::new(self))
        } else {
            Result::Err(RwLockError::Retry)
        }
    }

    #[inline]
    pub fn read(&mut self) -> MayBreakRead<'a, '_, T, D, B, O> {
        MayBreakRead::new(self)
    }

    #[inline]
    pub fn write(&mut self) -> MayBreakWrite<'a, '_, T, D, B, O> {
        MayBreakWrite::new(self)
    }

    #[inline]
    pub fn upgradable_read(
        &mut self,
    ) -> MayBreakUpgradableRead<'a, '_, T, D, B, O> {
        MayBreakUpgradableRead::new(self)
    }
}

impl<'a, T, D, B, O> AcqSession<'a, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn downgrade_writer_to_reader<'g>(
        guard: WriterGuard<'a, 'g, T, D, B, O>,
    ) -> ReaderGuard<'a, 'g, T, D, B, O> {
        let acquire = Self::destruct_guard_(guard);
        let lock = acquire.0;
        if lock.state_().try_downgrade_write_to_read() {
            ReaderGuard::new(acquire)
        } else {
            unreachable!("[Acquire::downgrade_writer_to_reader]")
        }
    }

    pub(super) fn downgrade_writer_to_upgradable<'g>(
        guard: WriterGuard<'a, 'g, T, D, B, O>,
    ) -> UpgradableReaderGuard<'a, 'g, T, D, B, O> {
        let acquire = Self::destruct_guard_(guard);
        let lock = acquire.0;
        if lock.state_().try_downgrade_write_to_upgradable() {
            UpgradableReaderGuard::new(acquire)
        } else {
            unreachable!("[Acquire::downgrade_writer_to_upgradable]")
        }
    }

    pub(super) fn downgrade_upgradable_to_reader<'g>(
        guard: UpgradableReaderGuard<'a, 'g, T, D, B, O>,
    ) -> ReaderGuard<'a, 'g, T, D, B, O> {
        let acquire = Self::destruct_guard_(guard);
        let lock = acquire.0;
        if lock.state_().try_downgrade_upgradable_to_read() {
            ReaderGuard::new(acquire)
        } else {
            unreachable!("[Acquire::downgrade_upgradable_to_reader]")
        }
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn try_upgrade_to_writer<'g>(
        mut guard: UpgradableReaderGuard<'a, 'g, T, D, B, O>,
    ) -> Result<
            WriterGuard<'a, 'g, T, D, B, O>,
            UpgradableReaderGuard<'a, 'g, T, D, B, O>,
        >
    {
        let guard_ptr = &mut guard as *mut UpgradableReaderGuard<'a, 'g, T, D, B, O>;
        if let Result::Ok(g) = Self::try_upgrade_mut_to_writer(unsafe { &mut *guard_ptr }) {
            Result::Ok(g)
        } else {
            Result::Err(guard)
        }
    }

    pub(super) fn try_upgrade_mut_to_writer<'g, 'u>(
        guard: &'u mut UpgradableReaderGuard<'a, 'g, T, D, B, O>,
    ) -> Result<WriterGuard<'a, 'u, T, D, B, O>, RwLockError> {
        let acq_mut = guard.share_mut();
        let lock = acq_mut.0;
        if lock.state_().try_upgrade_upgradable_to_write() {
            Result::Ok(WriterGuard::new(acq_mut))
        } else {
            Result::Err(RwLockError::Retry)
        }
    }

    fn destruct_guard_<'g, G>(guard: G) -> &'g mut Self
    where
        Self: 'g,
        G: TrShareMut<'g, Self>,
    {
        let mut m = ManuallyDrop::new(guard);
        (*m).share_mut()
    }

    pub(super) fn deref_impl(&self) -> &T {
        unsafe { &*self.0.data_.get() }
    }

    pub(super) fn deref_mut_impl(&mut self) -> &mut T {
        unsafe { &mut *self.0.data_.get() }
    }

    pub(super) fn drop_reader_guard(&mut self) {
        let x = self.0.stat_.decrease_reader_count();
        debug_assert!(x.is_ok())
    }

    pub(super) fn drop_writer_guard(&mut self) {
        self.0.stat_.on_writer_guard_drop()
    }

    pub(super) fn drop_upgradable_read_guard(&mut self) {
        let x = self.0.stat_.on_upgradable_read_drop();
        debug_assert!(x);
    }
}

impl<'a, T, D, B, O> TrSyncRwLockAcqSess<'a, T> for AcqSession<'a, T, D, B, O>
where
    Self: 'a,
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type ReaderGuard<'g> = ReaderGuard<'a, 'g, T, D, B, O> where 'a: 'g;

    type WriterGuard<'g> = WriterGuard<'a, 'g, T, D, B, O> where 'a: 'g;

    type UpgradableGuard<'g> = UpgradableReaderGuard<'a, 'g, T, D, B, O> where 'a: 'g;

    type Err = RwLockError;

    #[inline]
    fn try_read<'g>(&'g mut self) -> Result<Self::ReaderGuard<'g>, Self::Err>
    where
        'a: 'g,
    {
        AcqSession::try_read(self)
    }

    #[inline]
    fn try_write<'g>(&'g mut self) -> Result<Self::WriterGuard<'g>, Self::Err>
    where
        'a: 'g,
    {
        AcqSession::try_write(self)
    }

    #[inline]
    fn try_upgradable_read<'g>(
        &'g mut self,
    ) -> Result<Self::UpgradableGuard<'g>, Self::Err>
    where
        'a: 'g,
    {
        AcqSession::try_upgradable_read(self)
    }

    type ReadMayBreak<'f> = MayBreakRead<'a, 'f, T, D, B, O>
    where
        Self: 'f;

    #[inline]
    fn read<'g>(&'g mut self) -> Self::ReadMayBreak<'g>
    where
        'a: 'g,
    {
        AcqSession::read(self)
    }

    type WriteMayBreak<'f> = MayBreakWrite<'a, 'f, T, D, B, O>
    where
        'a: 'f;

    #[inline]
    fn write<'g>(&'g mut self) -> Self::WriteMayBreak<'g>
    where
        'a: 'g,
    {
        AcqSession::write(self)
    }

    type UpgradeMayBreak<'f> = MayBreakUpgradableRead<'a, 'f, T, D, B, O>
    where
        'a: 'f;

    #[inline]
    fn upgradable_read<'g>(&'g mut self) -> Self::UpgradeMayBreak<'g>
    where
        'a: 'g,
    {
        AcqSession::upgradable_read(self)
    }
}

struct RwLockState<D, B, O>(B, PhantomData<D>, PhantomData<O>)
where
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<D, B, O> AsRef<D::AtomicCell> for RwLockState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn as_ref(&self) -> &D::AtomicCell {
        self.0.borrow()
    }
}

impl<D, B, O> TrAtomicFlags<D, O> for RwLockState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{}

impl<D, B, O> RwLockState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub const fn new(cell: B) -> Self {
        RwLockState(cell, PhantomData, PhantomData)
    }

    // const K_NOT_LOCKED: D = D::ZERO;

    #[allow(non_snake_case)]
    #[inline]
    fn K_WRITER_ACTIVE() -> D {
        D::ONE << (D::BITS - 1)
    }
    #[allow(non_snake_case)]
    #[inline]
    fn K_WRITER_QUEUED() -> D {
        D::ONE << (D::BITS - 2)
    }
    #[allow(non_snake_case)]
    #[inline]
    fn K_UPGRADE_ACTIVE() -> D {
        D::ONE << (D::BITS - 3)
    }
    #[allow(non_snake_case)]
    #[inline]
    fn K_MAX_READER_COUNT() -> D {
        D::MAX >> 3
    }

    #[inline]
    fn try_spin_update_(
        &self,
        expect: impl FnMut(D) -> bool,
        desire: impl FnMut(D) -> D,
    ) -> Result<D, D> {
        self.try_spin_compare_exchange_weak(expect, desire)
            .into()
    }

    #[inline]
    fn load_state(&self) -> D {
        TrAtomicFlags::value(self)
    }

    // fn expect_lock_released(state: D) -> bool {
    //     state == Self::K_NOT_LOCKED
    // }

    fn expect_writer_active(s: D) -> bool {
        s & Self::K_WRITER_ACTIVE() == Self::K_WRITER_ACTIVE()
    }
    fn expect_writer_inactive(s: D) -> bool {
        !Self::expect_writer_active(s)
    }
    fn desire_writer_active(s: D) -> D {
        s | Self::K_WRITER_ACTIVE()
    }
    fn desire_writer_inactive(s: D) -> D {
        s & (!Self::K_WRITER_ACTIVE())
    }

    fn expect_writer_queued(s: D) -> bool {
        s & Self::K_WRITER_QUEUED() == Self::K_WRITER_QUEUED()
    }
    fn expect_writer_not_queued(s: D) -> bool {
        !Self::expect_writer_queued(s)
    }
    fn desire_writer_queued(s: D) -> D {
        s | Self::K_WRITER_QUEUED()
    }
    fn desire_writer_not_queued(s: D) -> D {
        s & (!Self::K_WRITER_QUEUED())
    }

    fn expect_upgrade_active(s: D) -> bool {
        s & Self::K_UPGRADE_ACTIVE() == Self::K_UPGRADE_ACTIVE()
    }
    fn expect_upgrade_inactive(s: D) -> bool {
        !Self::expect_upgrade_active(s)
    }
    fn desire_upgrade_inactive(s: D) -> D {
        s & (!Self::K_UPGRADE_ACTIVE())
    }
    fn desire_upgrade_active(s: D) -> D {
        s | Self::K_UPGRADE_ACTIVE()
    }

    fn get_reader_count(s: D) -> D {
        s & Self::K_MAX_READER_COUNT()
    }
    fn expect_reader_lt_max(s: D) -> bool {
        Self::get_reader_count(s) < Self::K_MAX_READER_COUNT()
    }
    fn expect_reader_gt_min(s: D) -> bool {
        Self::get_reader_count(s) > D::ZERO
    }
    fn desire_inc_reader(s: D) -> D {
        s + D::ONE
    }
    fn desire_dec_reader(s: D) -> D {
        s - D::ONE
    }

    #[inline]
    pub fn on_writer_guard_drop(&self) {
        let r = self.try_spin_update_(
            Self::expect_writer_active,
            Self::desire_writer_not_queued_inactive_,
        );
        debug_assert!(r.is_ok())
    }
    fn desire_writer_not_queued_inactive_(s: D) -> D {
        let s = Self::desire_writer_inactive(s);
        Self::desire_writer_not_queued(s)
    }

    #[inline]
    pub fn reader_count(&self) -> D {
        Self::get_reader_count(self.load_state())
    }

    pub fn decrease_reader_count(&self) -> Result<D, D> {
        self.try_spin_update_(
                Self::expect_reader_gt_min,
                Self::desire_dec_reader)
            .map(Self::get_reader_count)
            .map_err(Self::get_reader_count)
    }

    /// When returning true, will increase reader count in RwLock::state_.
    pub fn try_read(&self) -> bool {
        self.try_spin_update_(
                Self::expect_can_read,
                Self::desire_inc_reader)
            .is_ok()
    }
    fn expect_can_read(state: D) -> bool {
        Self::expect_reader_lt_max(state)
            && Self::expect_writer_not_queued(state)
            && Self::expect_writer_inactive(state)
    }

    pub fn try_write(&self) -> bool {
        let r = self.try_spin_update_(
            Self::expect_can_write,
            Self::desire_writer_active,
        );
        if r.is_ok() {
            return true;
        }
        let _ = self.try_spin_update_(
            Self::expect_writer_not_queued,
            Self::desire_writer_queued,
        );
        false
    }
    fn expect_can_write(s: D) -> bool {
        Self::get_reader_count(s) == D::ZERO
            && Self::expect_writer_inactive(s)
    }

    pub fn try_upgradable_read(&self) -> bool {
        self.try_spin_update_(
                Self::expect_write_not_queued_and_upgrade_inactive,
                Self::desire_upgrade_active_and_reader_inc)
            .is_ok()
    }
    fn expect_write_not_queued_and_upgrade_inactive(s: D) -> bool {
        Self::expect_writer_not_queued(s)
            && Self::expect_writer_inactive(s)
            && Self::expect_upgrade_inactive(s)
    }
    fn desire_upgrade_active_and_reader_inc(s: D) -> D {
        let s = Self::desire_upgrade_active(s);
        Self::desire_inc_reader(s)
    }

    pub fn on_upgradable_read_drop(&self) -> bool {
        self.try_spin_update_(
                Self::expect_upgrade_active,
                Self::desire_upgrade_inactive_and_reader_dec)
            .is_ok()
    }
    fn desire_upgrade_inactive_and_reader_dec(s: D) -> D {
        let s = Self::desire_upgrade_inactive(s);
        Self::desire_dec_reader(s)
    }

    pub fn try_downgrade_write_to_read(&self) -> bool {
        self.try_spin_update_(
                Self::expect_writer_active,
                Self::desire_write_to_read_)
            .is_ok()
    }
    fn desire_write_to_read_(s: D) -> D {
        let s = Self::desire_writer_inactive(s);
        Self::desire_inc_reader(s)
    }

    pub fn try_downgrade_write_to_upgradable(&self) -> bool {
        self.try_spin_update_(
                Self::expect_writer_active,
                Self::desire_write_to_upgradable_)
            .is_ok()
    }
    fn desire_write_to_upgradable_(s: D) -> D {
        let s = Self::desire_writer_inactive(s);
        let s = Self::desire_upgrade_active(s);
        Self::desire_inc_reader(s)
    }

    pub fn try_downgrade_upgradable_to_read(&self) -> bool {
        self.try_spin_update_(
                Self::expect_can_downgrade_to_read_,
                Self::desire_upgrade_inactive)
            .is_ok()
    }
    fn expect_can_downgrade_to_read_(s: D) -> bool {
        Self::expect_upgrade_active(s)
            && Self::expect_writer_inactive(s)
            && Self::get_reader_count(s) > D::ZERO
    }

    pub fn try_upgrade_upgradable_to_write(&self) -> bool {
        self.try_spin_update_(
                Self::expect_can_upgrade_,
                Self::desire_upgradable_to_write_)
            .is_ok()
    }
    fn expect_can_upgrade_(s: D) -> bool {
        Self::expect_upgrade_active(s)
            && Self::get_reader_count(s) == D::ONE
    }
    fn desire_upgradable_to_write_(s: D) -> D {
        Self::desire_writer_active(s)
    }
}

impl<D, B, O> fmt::Debug for RwLockState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.load_state();
        write!(
            f,
            "[RwLockState: W({}), Q({}), U({}), R({})]",
            Self::expect_writer_active(s),
            Self::expect_writer_queued(s),
            Self::expect_upgrade_active(s),
            Self::get_reader_count(s),
        )
    }
}

type FpTryAcquire<'a, 'g, T, B, D, O, X> =
    fn(&'g mut AcqSession<'a, T, D, B, O>,
) -> Result<X, RwLockError>;

pub(super) fn may_break_with_impl_<'a, 'g, TTask, T, B, D, O, C, X>(
    mut task: TTask,
    mut get_acq_mut: impl FnMut(&mut TTask) -> &mut AcqSession<'a, T, D, B, O>,
    try_acquire: FpTryAcquire<'a, 'g, T, B, D, O, X>,
    cancel: C,
) -> Result<X, RwLockError>
where
    TTask: 'g,
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    C: TrCancellationToken,
{
    let tp = &mut task as *mut TTask;
    loop {
        let task_mut = unsafe { &mut *tp };
        let acq_mut = get_acq_mut(task_mut);
        if let Result::Ok(g) = try_acquire(acq_mut) {
            break Result::Ok(g);
        };
        if cancel.is_cancelled() {
            break Result::Err(RwLockError::Cancelled);
        }
    }
}
