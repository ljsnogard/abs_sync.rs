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
use abs_cancel::TrCancellationToken;
use abs_sync::{
    may_break::TrMayBreak,
    sync_rwlock::*,
    x_deps::abs_cancel,
};

use crate::rwlock::preemptive::error_::RwLockError;

use super::rwlock_::{AcqSession, may_break_with_impl_};

#[derive(Debug)]
pub struct ReaderGuard<'a, 'g, T, D, B, O>(&'g mut AcqSession<'a, T, D, B, O>)
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, D, B, O> ReaderGuard<'a, 'g, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(acquire: &'g mut AcqSession<'a, T, D, B, O>) -> Self {
        ReaderGuard(acquire)
    }
}

impl<'a, T, D, B, O> Drop for ReaderGuard<'a, '_, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        self.0.drop_reader_guard()
    }
}

impl<'a, T, D, B, O> Deref for ReaderGuard<'a, '_, T, D, B, O>
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

impl<'a, 'g, T, D, B, O> TrAcqRefGuard<'a, 'g, T> for ReaderGuard<'a, 'g, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{}

impl<'a, 'g, T, D, B, O> TrSyncReaderGuard<'a, 'g, T> for ReaderGuard<'a, 'g, T, D, B, O>
where
    T: 'a + ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type AcqSess = AcqSession<'a, T, D, B, O>;
}

pub struct MayBreakRead<'a, 'g, T, D, B, O>(&'g mut AcqSession<'a, T, D, B, O>)
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T, D, B, O> MayBreakRead<'a, 'g, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(acquire: &'g mut AcqSession<'a, T, D, B, O>) -> Self {
        MayBreakRead(acquire)
    }

    #[inline]
    pub fn may_break_with<C>(
        self,
        cancel: C,
    ) -> Result<ReaderGuard<'a, 'g, T, D, B, O>, RwLockError>
    where
        C: TrCancellationToken,
    {
        may_break_with_impl_(
            self,
            |t| t.0,
            AcqSession::try_read,
            cancel,
        )
    }

    #[inline]
    pub fn wait(self) -> Result<ReaderGuard<'a, 'g, T, D, B, O>, RwLockError> {
        TrMayBreak::wait(self)
    }

    #[inline]
    pub fn wait_or<F>(self, f: F) -> ReaderGuard<'a, 'g, T, D, B, O>
    where
        F: FnOnce() -> ReaderGuard<'a, 'g, T, D, B, O>,
    {
        TrMayBreak::wait_or(self, f)
    }
}

impl<'a, 'g, T, D, B, O> TrMayBreak for MayBreakRead<'a, 'g, T, D, B, O>
where
    T: ?Sized,
    D: TrAtomicData + Unsigned,
    <D as TrAtomicData>::AtomicCell: Bitwise,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type MayBreakOutput = Result<ReaderGuard<'a, 'g, T, D, B, O>, RwLockError>;

    #[inline]
    fn may_break_with<C>(self, cancel: C) -> Self::MayBreakOutput
    where
        C: TrCancellationToken,
    {
        MayBreakRead::may_break_with(self, cancel)
    }
}
