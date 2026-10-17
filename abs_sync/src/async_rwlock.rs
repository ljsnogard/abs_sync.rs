use abs_cancel::TrMayCancel;

use crate::sync_guard::{TrAcqMutGuard, TrAcqRefGuard};

/// Reader-Writer lock that encourages sharing sessions instead of the lock
/// itself to avoid deadlock.
pub trait TrAsyncRwLock {
    type Target: ?Sized;

    type AcqSess<'f>: TrAsyncRwLockAcqSess<'f, Self::Target, Err = Self::Err>
    where
        Self: 'f;

    type Err: core::error::Error;

    /// Get a session that can acquire guard of the rwLock.
    fn acq_session(&self) -> Self::AcqSess<'_>;
}

pub trait TrAsyncRwLockAcqSess<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type ReaderGuard<'g>: TrReaderGuard<'a, 'g, T> where 'a: 'g;

    type WriterGuard<'g>: TrWriterGuard<'a, 'g, T> where 'a: 'g;

    type UpgradableGuard<'g>: TrUpgradableReaderGuard<'a, 'g, T> where 'a: 'g;

    type Err: core::error::Error;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn try_read<'g>(&'g mut self) -> Result<Self::ReaderGuard<'g>, Self::Err>
    where
        'a: 'g;

    fn try_write<'g>(&'g mut self) -> Result<Self::WriterGuard<'g>, Self::Err>
    where
        'a: 'g;

    fn try_upgradable_read<'g>(
        &'g mut self,
    ) -> Result<Self::UpgradableGuard<'g>, Self::Err>
    where
        'a: 'g;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type ReadAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::ReaderGuard<'f>, Self::Err>>
    where
        'a: 'f;

    fn read_async<'g>(&'g mut self) -> Self::ReadAsync<'g> where 'a: 'g;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type WriteAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::WriterGuard<'f>, Self::Err>>
    where
        'a: 'f;

    fn write_async<'g>(&'g mut self) -> Self::WriteAsync<'g> where 'a: 'g;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type UpgradableReadAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::UpgradableGuard<'f>, Self::Err>>
    where
        'a: 'f;

    fn upgradable_read_async<'g>(&'g mut self) -> Self::UpgradableReadAsync<'g>
    where
        'a: 'g;
}

pub trait TrReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + TrAcqRefGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    type Acquire: TrAsyncRwLockAcqSess<'a, T>;
}

pub trait TrUpgradableReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrReaderGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    type UpgradeSess: TrAsyncRwLockUpgradeSession<'a, 'g, T, ParentSess = Self::Acquire>;

    fn downgrade(self) -> <Self::Acquire as TrAsyncRwLockAcqSess<'a, T>>::ReaderGuard<'g>;

    fn upgrade_session(self) -> Self::UpgradeSess;
}

pub trait TrWriterGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrReaderGuard<'a, 'g, T> +TrAcqMutGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    fn downgrade(self) -> <Self::Acquire as TrAsyncRwLockAcqSess<'a, T>>::ReaderGuard<'g>;

    fn downgrade_to_upgradable(
        self,
    ) -> <Self::Acquire as TrAsyncRwLockAcqSess<'a, T>>::UpgradableGuard<'g>;
}

pub trait TrAsyncRwLockUpgradeSession<'a, 'g, T>
where
    'a: 'g,
    T: 'a + ?Sized,
{
    type ParentSess: TrAsyncRwLockAcqSess<'a, T>;

    fn try_upgrade<'f>(
        &'f mut self,
    ) -> Result<
        <Self::ParentSess as TrAsyncRwLockAcqSess<'a, T>>::WriterGuard<'f>,
        <Self::ParentSess as TrAsyncRwLockAcqSess<'a, T>>::Err,
    >
    where
        'g: 'f;

    type UpgradeAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<
        <Self::ParentSess as TrAsyncRwLockAcqSess<'a, T>>::WriterGuard<'f>,
        <Self::ParentSess as TrAsyncRwLockAcqSess<'a, T>>::Err,
    >>
    where
        'g: 'f,
        Self: 'f;

    fn upgrade_async<'f>(&'f mut self) -> Self::UpgradeAsync<'f>
    where
        'g: 'f;

    fn into_guard(
        self,
    ) -> <Self::ParentSess as TrAsyncRwLockAcqSess<'a, T>>::UpgradableGuard<'g>;
}
