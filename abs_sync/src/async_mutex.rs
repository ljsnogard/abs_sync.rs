use abs_cancel::TrMayCancel;

use crate::sync_guard::TrAcqMutGuard;

/// Mutex for asynchronous task pattern.
pub trait TrAsyncMutex {
    type Target: ?Sized;

    type LockSess<'f>: TrAsyncMutexLockSess<'f, Self::Target, Err = Self::Err>
    where
        Self: 'f;

    type Err: core::error::Error;

    /// Get a session that can lock the mutex.
    fn lock_session(&self) -> Self::LockSess<'_>;
}

pub trait TrAsyncMutexLockSess<'a, T>
where
    Self: 'a,
    T: 'a + ?Sized,
{
    type Guard<'g>: TrAcqMutGuard<'a, 'g, T> where 'a: 'g;

    type Err: core::error::Error;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn try_lock<'g>(&'g mut self) -> Result<Self::Guard<'g>, Self::Err>
    where
        'a: 'g;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type LockAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::Guard<'f>, Self::Err>>
    where
        'a: 'f;

    fn lock_async<'g>(&'g mut self) -> Self::LockAsync<'g>
    where
        'a: 'g;
}
