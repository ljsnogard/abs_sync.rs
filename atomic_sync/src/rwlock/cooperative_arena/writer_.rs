//! 写许可的守卫与异步获取。

use alloc::alloc::Global;
use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    future::{Future, IntoFuture},
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

use funty::Unsigned;
use pin_project::pin_project;

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_sync::{
    async_rwlock::*,
    sync_guard::{TrAcqMutGuard, TrAcqRefGuard},
    x_deps::abs_cancel,
};
use atomex::{TrAtomicData, TrCmpxchOrderings, x_deps::funty};

use crate::rwlock::{TrShareMut, wait_::WaitKind};

use super::{
    CoopRwLockError,
    core_::{AcqProgress, PendingWait},
    reader_::ReaderGuard,
    rwlock_::CooperativeAcqSession,
    upgrade_::UpgradableReaderGuard,
};

/// 写许可守卫。
pub struct WriterGuard<'a, 'g, T: ?Sized, D, B, O, A = Global>(
    &'g mut CooperativeAcqSession<'a, T, D, B, O, A>,
)
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone;

impl<'a, 'g, T: ?Sized, D, B, O, A> WriterGuard<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O, A>) -> Self {
        WriterGuard(sess)
    }

    /// 降级为普通读守卫。
    pub fn downgrade_to_reader(self) -> ReaderGuard<'a, 'g, T, D, B, O, A> {
        CooperativeAcqSession::downgrade_writer_to_reader(self)
    }

    /// 降级为可升级读守卫。
    pub fn downgrade_to_upgradable(self) -> UpgradableReaderGuard<'a, 'g, T, D, B, O, A> {
        CooperativeAcqSession::downgrade_writer_to_upgradable(self)
    }
}

impl<'a, T: ?Sized, D, B, O, A> Drop for WriterGuard<'a, '_, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    fn drop(&mut self) {
        self.0.core().release_writer()
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, A> TrShareMut<'g, CooperativeAcqSession<'a, T, D, B, O, A>>
    for WriterGuard<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    fn share_mut(&mut self) -> &'g mut CooperativeAcqSession<'a, T, D, B, O, A> {
        let p = self.0 as *mut _;
        // SAFETY: 守卫独占地持有会话借用（`&'g mut`），把它按原生命周期
        // 交还给调用方不会产生别名；调用方（`destruct_guard_`）会先用
        // `ManuallyDrop` 阻止本守卫析构。
        unsafe { &mut *p }
    }
}

impl<'a, T: ?Sized, D, B, O, A> Deref for WriterGuard<'a, '_, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.deref_impl()
    }
}

impl<'a, T: ?Sized, D, B, O, A> DerefMut for WriterGuard<'a, '_, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut_impl()
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, A> TrAcqRefGuard<'a, 'g, T> for WriterGuard<'a, 'g, T, D, B, O, A>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
}

impl<'a, 'g, T: ?Sized, D, B, O, A> TrAcqMutGuard<'a, 'g, T> for WriterGuard<'a, 'g, T, D, B, O, A>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
}

impl<'a, 'g, T: ?Sized, D, B, O, A> TrReaderGuard<'a, 'g, T> for WriterGuard<'a, 'g, T, D, B, O, A>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    type Acquire = CooperativeAcqSession<'a, T, D, B, O, A>;
}

impl<'a, 'g, T: ?Sized, D, B, O, A> TrWriterGuard<'a, 'g, T> for WriterGuard<'a, 'g, T, D, B, O, A>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    #[inline]
    fn downgrade(self) -> <Self::Acquire as TrAsyncRwLockAcqSess<'a, T>>::ReaderGuard<'g> {
        WriterGuard::downgrade_to_reader(self)
    }

    #[inline]
    fn downgrade_to_upgradable(
        self,
    ) -> <Self::Acquire as TrAsyncRwLockAcqSess<'a, T>>::UpgradableGuard<'g> {
        WriterGuard::downgrade_to_upgradable(self)
    }
}

/// 手写的写者获取流程；结构同 [`super::reader_::ReadAcquireInner`]。
pub(super) struct WriteAcquireInner<'a, 'g, T: ?Sized, D, B, O, A = Global>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    sess_: Option<&'g mut CooperativeAcqSession<'a, T, D, B, O, A>>,
    pending_: Option<PendingWait>,
}

impl<'a, 'g, T: ?Sized, D, B, O, A> WriteAcquireInner<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O, A>) -> Self {
        WriteAcquireInner {
            sess_: Option::Some(sess),
            pending_: Option::None,
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, A> Future for WriteAcquireInner<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    type Output = Result<WriterGuard<'a, 'g, T, D, B, O, A>, CoopRwLockError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(sess) = this.sess_.take() else {
            panic!("[WriteAcquireInner::poll] polled after completion");
        };

        if this.pending_.is_none() && sess.core().try_write_fast() {
            return Poll::Ready(Result::Ok(WriterGuard::new(sess)));
        }

        let pending = this.pending_;
        let claimed = match pending {
            Option::Some(pending) => sess.core().poll_acquire(&pending, cx.waker()),
            Option::None => match sess.core().acquire(WaitKind::Write, cx.waker()) {
                AcqProgress::Acquired => true,
                AcqProgress::Waiting(pending) => {
                    this.pending_ = Option::Some(pending);
                    false
                }
            },
        };

        if claimed {
            this.pending_ = Option::None;
            Poll::Ready(Result::Ok(WriterGuard::new(sess)))
        } else {
            this.sess_ = Option::Some(sess);
            Poll::Pending
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, A> Drop for WriteAcquireInner<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    fn drop(&mut self) {
        let (Option::Some(sess), Option::Some(pending)) =
            (self.sess_.as_ref(), self.pending_.as_ref())
        else {
            return;
        };
        sess.core().cancel_wait(pending);
    }
}

/// 写获取的参数载体；用法见 [`super::reader_::ReadAcquireAsync`]。
pub struct WriteAcquireAsync<'a, 'g, T: ?Sized, D, B, O, A = Global>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    sess_: &'g mut CooperativeAcqSession<'a, T, D, B, O, A>,
}

impl<'a, 'g, T: ?Sized, D, B, O, A> WriteAcquireAsync<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O, A>) -> Self {
        WriteAcquireAsync { sess_: sess }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, A> IntoFuture for WriteAcquireAsync<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    type IntoFuture = WriteAcquireFuture<'a, 'g, T, D, B, O, A, NonCancellableToken>;

    type Output = Result<WriterGuard<'a, 'g, T, D, B, O, A>, CoopRwLockError>;

    fn into_future(self) -> Self::IntoFuture {
        WriteAcquireFuture::new(self.sess_)
    }
}

impl<'f, 'a, 'g, T, D, B, O, A> TrMayCancel<'f> for WriteAcquireAsync<'a, 'g, T, D, B, O, A>
where
    'a: 'f,
    'g: 'f,
    T: ?Sized + 'f,
    D: TrAtomicData + Unsigned + 'f,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell> + 'f,
    O: TrCmpxchOrderings + 'f,
    A: AllocatorClone + 'f,
{
    type MayCancelFuture<'lt, C>
        = WriteAcquireFuture<'a, 'g, T, D, B, O, A, C>
    where
        'lt: 'f,
        Self: 'lt,
        C: 'lt + TrCancellationToken;

    type MayCancelOutput = Result<WriterGuard<'a, 'g, T, D, B, O, A>, CoopRwLockError>;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'f, C>
    where
        C: 'f + TrCancellationToken,
    {
        WriteAcquireFuture::with_cancel(self.sess_, cancel)
    }
}

/// 写获取 future；见 [`super::reader_::ReadAcquireFuture`] 的说明。
#[pin_project]
pub struct WriteAcquireFuture<'a, 'g, T: ?Sized, D, B, O, A = Global, C = NonCancellableToken>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    inner_: Option<WriteAcquireInner<'a, 'g, T, D, B, O, A>>,
    #[pin]
    cancel_: Option<C::Cancellation>,
}

impl<'a, 'g, T: ?Sized, D, B, O, A> WriteAcquireFuture<'a, 'g, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O, A>) -> Self {
        WriteAcquireFuture {
            inner_: Option::Some(WriteAcquireInner::new(sess)),
            cancel_: Option::None,
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, A, C> WriteAcquireFuture<'a, 'g, T, D, B, O, A, C>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    pub(super) fn with_cancel(
        sess: &'g mut CooperativeAcqSession<'a, T, D, B, O, A>,
        cancel: C,
    ) -> Self {
        WriteAcquireFuture {
            inner_: Option::Some(WriteAcquireInner::new(sess)),
            cancel_: Option::Some(cancel.cancellation()),
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, A, C> Future for WriteAcquireFuture<'a, 'g, T, D, B, O, A, C>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    type Output = Result<WriterGuard<'a, 'g, T, D, B, O, A>, CoopRwLockError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Some(cancel) = this.cancel_.as_mut().as_pin_mut()
            && cancel.poll(cx).is_ready()
        {
            *this.inner_ = Option::None;
            this.cancel_.set(Option::None);
            return Poll::Ready(Result::Err(CoopRwLockError::Cancelled));
        }
        let Some(inner) = this.inner_.as_mut() else {
            panic!("[WriteAcquireFuture::poll] polled after completion");
        };
        let polled = Pin::new(inner).poll(cx);
        if polled.is_ready() {
            *this.inner_ = Option::None;
            this.cancel_.set(Option::None);
        }
        polled
    }
}
