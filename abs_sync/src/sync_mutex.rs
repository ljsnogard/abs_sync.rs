use crate::may_break::TrMayBreak;

pub use crate::sync_guard::{TrAcqMutGuard, TrAcqRefGuard};

pub trait TrSyncMutex {
    type Target: ?Sized;

    type LockSess<'f>: TrSyncMutexSession<'f, Self::Target>
    where
        Self: 'f;

    fn lock_session(&self) -> Self::LockSess<'_>;
}

pub trait TrSyncMutexSession<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type Guard<'g>: TrAcqMutGuard<'a, 'g, T> where 'a: 'g;

    type Err: core::error::Error;

    fn try_lock<'g>(&'g mut self) -> Result<Self::Guard<'g>, Self::Err>
    where
        'a: 'g;

    type LockMayBreak<'f>: TrMayBreak<MayBreakOutput = Result<Self::Guard<'f>, Self::Err>>
    where
        'a: 'f;

    fn lock<'g>(&'g mut self) -> Self::LockMayBreak<'g> where 'a: 'g;
}
