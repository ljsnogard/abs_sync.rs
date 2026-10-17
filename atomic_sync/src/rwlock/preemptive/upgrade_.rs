use core::{
    borrow::BorrowMut,
    ops::Deref,
};

use funty::Unsigned;
use atomex::{
    fetch::Bitwise,
    x_deps::funty,
    TrAtomicData, TrCmpxchOrderings,
};
use abs_cancel::{NonCancellableToken, TrCancellationToken};
use abs_sync::{
    may_break::TrMayBreak,
    sync_rwlock::*,
    x_deps::abs_cancel,
};

use crate::rwlock::TrShareMut;
use super::{
    error_::RwLockError,
    rwlock_::{AcqSession, may_break_with_impl_},
    reader_::ReaderGuard,
    writer_::WriterGuard,
};

#[derive(Debug)]
pub struct UpgradableReaderGuard<'a, 'g, T, D, B, O>(&'g mut AcqSession<'a, T, D, B, O>)
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, D, B, O> UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(acquire: &'g mut AcqSession<'a, T, D, B, O>) -> Self {
        UpgradableReaderGuard(acquire)
    }

    pub fn downgrade(self) -> ReaderGuard<'a, 'g, T, D, B, O> {
        AcqSession::downgrade_upgradable_to_reader(self)
    }

    pub fn try_upgrade(self) -> Result<WriterGuard<'a, 'g, T, D, B, O>, Self> {
        AcqSession::try_upgrade_to_writer(self)
    }

    pub fn upgrade(self) -> Upgrade<'a, 'g, T, D, B, O> {
        Upgrade::new(self)
    }
}

impl<'a, T, D, B, O> Drop for UpgradableReaderGuard<'a, '_, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        self.0.drop_upgradable_read_guard()
    }
}

impl<'a, 'g, T, D, B, O> TrShareMut<'g, AcqSession<'a, T, D, B, O>> for
    UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn share_mut(&mut self) -> &'g mut AcqSession<'a, T, D, B, O> {
        let p = self.0 as *mut _;
        unsafe { &mut *p }
    }
}

impl<'a, T, D, B, O> Deref for UpgradableReaderGuard<'a, '_, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref_impl()
    }
}

impl<'a, 'g, T, D, B, O> TrAcqRefGuard<'a, 'g, T> for
    UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    Self: 'g,
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{}

impl<'a, 'g, T, D, B, O> TrSyncReaderGuard<'a, 'g, T> for
    UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    Self: 'g,
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type AcqSess = AcqSession<'a, T, D, B, O>;
}

impl<'a, 'g, T, D, B, O> TrSyncUpgradableReaderGuard<'a, 'g, T> for
    UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type UpgradeSession = Upgrade<'a, 'g, T, D, B, O>;

    #[inline]
    fn downgrade(self) -> <Self::AcqSess as TrSyncRwLockAcqSess<'a, T>>::ReaderGuard<'g> {
        UpgradableReaderGuard::downgrade(self)
    }

    #[inline]
    fn try_upgrade(
        self,
    ) -> Result<<Self::AcqSess as TrSyncRwLockAcqSess<'a, T>>::WriterGuard<'g> , Self> {
        UpgradableReaderGuard::try_upgrade(self)
    }

    #[inline]
    fn upgrade_session(self) -> Self::UpgradeSession {
        UpgradableReaderGuard::upgrade(self)
    }
}

pub struct MayBreakUpgradableRead<'a, 'g, T, D, B, O>(&'g mut AcqSession<'a, T, D, B, O>)
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, D, B, O> MayBreakUpgradableRead<'a, 'g, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(acquire: &'g mut AcqSession<'a, T, D, B, O>) -> Self {
        MayBreakUpgradableRead(acquire)
    }

    pub fn may_break_with<C>(
        self,
        cancel: C,
    ) -> Result<UpgradableReaderGuard<'a, 'g, T, D, B, O>, RwLockError>
    where
        C: TrCancellationToken,
    {
        may_break_with_impl_(
            self,
            |t| t.0,
            AcqSession::try_upgradable_read,
            cancel,
        )
    }

    #[inline]
    pub fn wait(
        self,
    ) -> Result<UpgradableReaderGuard<'a, 'g, T, D, B, O>, RwLockError> {
        TrMayBreak::wait(self)
    }

    #[inline]
    pub fn wait_or<F>(self, f: F) -> UpgradableReaderGuard<'a, 'g, T, D, B, O>
    where
        F: FnOnce() -> UpgradableReaderGuard<'a, 'g, T, D, B, O>,
    {
        TrMayBreak::wait_or(self, f)
    }
}

impl<'a, 'g, T, D, B, O> TrMayBreak for MayBreakUpgradableRead<'a, 'g, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type MayBreakOutput = Result<
        UpgradableReaderGuard<'a, 'g, T, D, B, O>,
        RwLockError,
    >;

    #[inline]
    fn may_break_with<C>(self, cancel: C) -> Self::MayBreakOutput
    where
        C: TrCancellationToken,
    {
        MayBreakUpgradableRead::may_break_with(self, cancel)
    }
}

pub struct Upgrade<'a, 'g, T, D, B, O>(UpgradableReaderGuard<'a, 'g, T, D, B, O>)
where
    T: ?Sized,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    D: Copy + Unsigned + TrAtomicData,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, D, B, O> Upgrade<'a, 'g, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub fn new(guard: UpgradableReaderGuard<'a, 'g, T, D, B, O>) -> Self {
        Upgrade(guard)
    }

    pub fn try_upgrade<'u>(
        &'u mut self,
    ) -> Result<WriterGuard<'a, 'u, T, D, B, O>, RwLockError> {
        AcqSession::try_upgrade_mut_to_writer(self.guard_mut())
    }

    pub fn upgrade<'u>(
        &'u mut self,
    ) -> MayBreakUpgrade<'a, 'g, 'u, T, D, B, O>
    where
        'g: 'u,
    {
        MayBreakUpgrade::new(self)
    }

    pub fn into_guard(self) -> UpgradableReaderGuard<'a, 'g, T, D, B, O> {
        self.0
    }

    pub fn upgrade_with_cancel<'u, C>(
        &'u mut self,
        cancel: C,
    ) -> Result<WriterGuard<'a, 'u, T, D, B, O>, RwLockError>
    where
        C: TrCancellationToken,
    {
        let guard_ptr = self.guard_mut() as *mut _;
        loop {
            let guard_mut = unsafe { &mut *guard_ptr };
            let opt = AcqSession::try_upgrade_mut_to_writer(guard_mut);
            if opt.is_ok()  {
                break opt;
            };
            if cancel.is_cancelled() {
                break Result::Err(RwLockError::Cancelled);
            }
        }
    }

    fn guard_mut(&mut self) -> &mut UpgradableReaderGuard<'a, 'g, T, D, B, O> {
        // Safe to get an `Pin<&mut UpgradableReaderGuard>` without moving it
        &mut self.0
    }
}

impl<'a, 'g, T, D, B, O> TrSyncUpgradeSession<'a, 'g, T> for Upgrade<'a, 'g, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type ParentSess = AcqSession<'a, T, D, B, O>;

    #[inline]
    fn try_upgrade<'u>(
        &'u mut self,
    ) -> Result<
        <Self::ParentSess as TrSyncRwLockAcqSess<'a, T>>::WriterGuard<'u>,
        RwLockError,
    >
    where
        'g: 'u,
    {
        Upgrade::try_upgrade(self)
    }

    type UpgradeMayBreak<'f> = MayBreakUpgrade<'a, 'g, 'f, T, D, B, O>
    where
        'g: 'f,
        Self: 'f;

    #[inline]
    fn upgrade<'u>(&'u mut self) -> Self::UpgradeMayBreak<'u>
    where
        'g: 'u,
    {
        Upgrade::upgrade(self)
    }

    #[inline]
    fn into_guard(
        self,
    ) -> <Self::ParentSess as TrSyncRwLockAcqSess<'a, T>>::UpgradableGuard<'g> {
        Upgrade::into_guard(self)
    }
}

pub struct MayBreakUpgrade<'a, 'g, 'u, T, D, B, O>(&'u mut Upgrade<'a, 'g, T, D, B, O>)
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, 'u, T, D, B, O> MayBreakUpgrade<'a, 'g, 'u, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(
        upgrade: &'u mut Upgrade<'a, 'g, T, D, B, O>,
    ) -> Self {
        MayBreakUpgrade(upgrade)
    }

    pub fn may_break_with<C>(
        self,
        cancel: C,
    ) -> Result<WriterGuard<'a, 'u, T, D, B, O>, RwLockError>
    where
        C: TrCancellationToken,
    {
        self.0.upgrade_with_cancel(cancel)
    }

    #[inline]
    pub fn wait(
        self,
    ) -> Result<WriterGuard<'a, 'u, T, D, B, O>, RwLockError> {
        self.may_break_with(NonCancellableToken::new())
    }

    #[inline]
    pub fn wait_or<F>(self, f: F) -> WriterGuard<'a, 'u, T, D, B, O>
    where
        F: FnOnce() -> WriterGuard<'a, 'u, T, D, B, O>,
    {
        TrMayBreak::wait_or(self, f)
    }
}

impl<'a, 'u, T, D, B, O> TrMayBreak for MayBreakUpgrade<'a, '_, 'u, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type MayBreakOutput = Result<
        WriterGuard<'a, 'u, T, D, B, O>,
        RwLockError,
    >;

    #[inline]
    fn may_break_with<C>(self, cancel: C) -> Self::MayBreakOutput
    where
        C: TrCancellationToken,
    {
        MayBreakUpgrade::may_break_with(self, cancel)
    }
}
