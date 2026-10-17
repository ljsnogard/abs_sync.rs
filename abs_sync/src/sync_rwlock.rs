use crate::may_break::TrMayBreak;
pub use crate::sync_guard::{TrAcqMutGuard, TrAcqRefGuard};

pub trait TrSyncRwLock {
    type Target: ?Sized;

    type Err: core::error::Error;

    type AcqSess<'f>: TrSyncRwLockAcqSess<'f, Self::Target, Err = Self::Err>
    where
        Self: 'f;

    fn acq_session(&self) -> Self::AcqSess<'_>;
}

pub trait TrSyncRwLockAcqSess<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type ReaderGuard<'g>: TrSyncReaderGuard<'a, 'g, T> where 'a: 'g;

    type WriterGuard<'g>: TrSyncWriterGuard<'a, 'g, T> where 'a: 'g;

    type UpgradableGuard<'g>: TrSyncUpgradableReaderGuard<'a, 'g, T>
    where
        'a: 'g;

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

    type ReadMayBreak<'f>: TrMayBreak<MayBreakOutput =
        Result<Self::ReaderGuard<'f>, Self::Err>>
    where
        'a: 'f;

    fn read<'g>(&'g mut self) -> Self::ReadMayBreak<'g> where 'a: 'g;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type WriteMayBreak<'f>: TrMayBreak<MayBreakOutput =
        Result<Self::WriterGuard<'f>, Self::Err>>
    where
        'a: 'f;

    fn write<'g>(&'g mut self) -> Self::WriteMayBreak<'g> where 'a: 'g;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type UpgradeMayBreak<'f>: TrMayBreak<MayBreakOutput =
        Result<Self::UpgradableGuard<'f>, Self::Err>>
    where
        'a: 'f;

    fn upgradable_read<'g>(&'g mut self) -> Self::UpgradeMayBreak<'g>
    where
        'a: 'g;
}

pub trait TrSyncReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + TrAcqRefGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    type AcqSess: TrSyncRwLockAcqSess<'a, T>;
}

pub trait TrSyncUpgradableReaderGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrSyncReaderGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    type UpgradeSession: TrSyncUpgradeSession<'a, 'g, T, ParentSess = Self::AcqSess>;

    fn downgrade(
        self,
    ) -> <Self::AcqSess as TrSyncRwLockAcqSess<'a, T>>::ReaderGuard<'g>;

    fn try_upgrade(
        self,
    ) -> Result<<Self::AcqSess as TrSyncRwLockAcqSess<'a, T>>::WriterGuard<'g> , Self>;

    fn upgrade_session(self) -> Self::UpgradeSession;
}

pub trait TrSyncWriterGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + TrSyncReaderGuard<'a, 'g, T> + TrAcqMutGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{
    fn downgrade_to_reader(
        self,
    ) -> <Self::AcqSess as TrSyncRwLockAcqSess<'a, T>>::ReaderGuard<'g>;

    fn downgrade_to_upgradable(
        self,
    ) -> <Self::AcqSess as TrSyncRwLockAcqSess<'a, T>>::UpgradableGuard<'g>;
}

pub trait TrSyncUpgradeSession<'a, 'g, T>
where
    'a: 'g,
    T: 'a + ?Sized,
{
    type ParentSess: TrSyncRwLockAcqSess<'a, T>;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn try_upgrade<'u>(
        &'u mut self,
    ) -> Result<
        <Self::ParentSess as TrSyncRwLockAcqSess<'a, T>>::WriterGuard<'u>,
        <Self::ParentSess as TrSyncRwLockAcqSess<'a, T>>::Err,
    >
    where
        'g: 'u;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type UpgradeMayBreak<'f>: TrMayBreak<MayBreakOutput = Result<
        <Self::ParentSess as TrSyncRwLockAcqSess<'a, T>>::WriterGuard<'f>,
        <Self::ParentSess as TrSyncRwLockAcqSess<'a, T>>::Err,
    >>
    where
        'g: 'f,
        Self: 'f;

    fn upgrade<'u>(&'u mut self) -> Self::UpgradeMayBreak<'u>
    where
        'g: 'u;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn into_guard(
        self,
    ) -> <Self::ParentSess as TrSyncRwLockAcqSess<'a, T>>::UpgradableGuard<'g>;
}
