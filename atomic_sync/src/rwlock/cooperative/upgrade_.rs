//! 可升级读守卫、升级会话，以及它们的异步获取。

use alloc::sync::Arc;
use core::{
    borrow::BorrowMut,
    future::{Future, IntoFuture},
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

use funty::Unsigned;
use pin_project::pin_project;

use atomex::{x_deps::funty, TrAtomicData, TrCmpxchOrderings};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_sync::{async_rwlock::*, sync_guard::TrAcqRefGuard, x_deps::abs_cancel};

use crate::rwlock::TrShareMut;

use super::{
    core_::AcqProgress,
    error_::CoopRwLockError,
    reader_::ReaderGuard,
    rwlock_::CooperativeAcqSession,
    wait_::{WaitKind, WaitNode},
    writer_::WriterGuard,
};

/// 可升级读守卫。
///
/// 它在状态字上同时占一个读者名额与"可升级"标志；全锁至多存在一个。
pub struct UpgradableReaderGuard<'a, 'g, T: ?Sized, D, B, O>(
    &'g mut CooperativeAcqSession<'a, T, D, B, O>,
)
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T: ?Sized, D, B, O> UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        UpgradableReaderGuard(sess)
    }

    /// 降级为普通读守卫。
    pub fn downgrade(self) -> ReaderGuard<'a, 'g, T, D, B, O> {
        CooperativeAcqSession::downgrade_upgradable_to_reader(self)
    }

    /// 立即尝试升级为写者；失败时原样归还自身。
    #[allow(clippy::type_complexity)]
    pub fn try_upgrade(self) -> Result<WriterGuard<'a, 'g, T, D, B, O>, Self> {
        CooperativeAcqSession::try_upgrade_to_writer(self)
    }

    /// 取得升级会话。
    pub fn upgrade_session(self) -> Upgrade<'a, 'g, T, D, B, O> {
        Upgrade::new(self)
    }

    /// 只读地访问底层会话。
    fn sess_ref(&self) -> &CooperativeAcqSession<'a, T, D, B, O> {
        self.0
    }
}

impl<'a, T: ?Sized, D, B, O> Drop for UpgradableReaderGuard<'a, '_, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        self.0.core().release_upgradable_read()
    }
}

impl<'a, 'g, T: ?Sized, D, B, O>
    TrShareMut<'g, CooperativeAcqSession<'a, T, D, B, O>>
    for UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn share_mut(&mut self) -> &'g mut CooperativeAcqSession<'a, T, D, B, O> {
        let p = self.0 as *mut _;
        // SAFETY: 同 `WriterGuard::share_mut`。
        unsafe { &mut *p }
    }
}

impl<'a, T: ?Sized, D, B, O> Deref for UpgradableReaderGuard<'a, '_, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.deref_impl()
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> TrAcqRefGuard<'a, 'g, T>
    for UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
}

impl<'a, 'g, T: ?Sized, D, B, O> TrReaderGuard<'a, 'g, T>
    for UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Acquire = CooperativeAcqSession<'a, T, D, B, O>;
}

impl<'a, 'g, T: ?Sized, D, B, O> TrUpgradableReaderGuard<'a, 'g, T>
    for UpgradableReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type UpgradeSess = Upgrade<'a, 'g, T, D, B, O>;

    #[inline]
    fn downgrade(
        self,
    ) -> <Self::Acquire as TrAsyncRwLockAcqSess<'a, T>>::ReaderGuard<'g> {
        UpgradableReaderGuard::downgrade(self)
    }

    #[inline]
    fn upgrade_session(self) -> Self::UpgradeSess {
        UpgradableReaderGuard::upgrade_session(self)
    }
}

/// 升级会话：持有可升级读守卫，并提供同步/异步升级入口。
pub struct Upgrade<'a, 'g, T: ?Sized, D, B, O>(
    UpgradableReaderGuard<'a, 'g, T, D, B, O>,
)
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T: ?Sized, D, B, O> Upgrade<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(guard: UpgradableReaderGuard<'a, 'g, T, D, B, O>) -> Self {
        Upgrade(guard)
    }

    /// 立即尝试升级；失败时返回错误，会话保持不变。
    pub fn try_upgrade<'u>(
        &'u mut self,
    ) -> Result<WriterGuard<'a, 'u, T, D, B, O>, CoopRwLockError> {
        CooperativeAcqSession::try_upgrade_mut_to_writer(self.guard_mut())
    }

    /// 可取消的异步升级。
    pub fn upgrade_async<'f>(
        &'f mut self,
    ) -> UpgradeAcquireAsync<'a, 'g, 'f, T, D, B, O>
    where
        'g: 'f,
    {
        UpgradeAcquireAsync::new(self)
    }

    /// 取回内部的可升级读守卫。
    pub fn into_guard(self) -> UpgradableReaderGuard<'a, 'g, T, D, B, O> {
        self.0
    }

    fn guard_mut(&mut self) -> &mut UpgradableReaderGuard<'a, 'g, T, D, B, O> {
        &mut self.0
    }

    fn sess_ref(&self) -> &CooperativeAcqSession<'a, T, D, B, O> {
        self.0.sess_ref()
    }

    fn share_guard_sess_mut(&mut self) -> &'g mut CooperativeAcqSession<'a, T, D, B, O> {
        self.0.share_mut()
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> TrAsyncRwLockUpgradeSession<'a, 'g, T>
    for Upgrade<'a, 'g, T, D, B, O>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type ParentSess = CooperativeAcqSession<'a, T, D, B, O>;

    #[inline]
    fn try_upgrade<'f>(
        &'f mut self,
    ) -> Result<
        <Self::ParentSess as TrAsyncRwLockAcqSess<'a, T>>::WriterGuard<'f>,
        CoopRwLockError,
    >
    where
        'g: 'f,
    {
        Upgrade::try_upgrade(self)
    }

    type UpgradeAsync<'f> = UpgradeAcquireAsync<'a, 'g, 'f, T, D, B, O>
    where
        'g: 'f,
        Self: 'f;

    #[inline]
    fn upgrade_async<'f>(&'f mut self) -> Self::UpgradeAsync<'f>
    where
        'g: 'f,
    {
        Upgrade::upgrade_async(self)
    }

    #[inline]
    fn into_guard(
        self,
    ) -> <Self::ParentSess as TrAsyncRwLockAcqSess<'a, T>>::UpgradableGuard<'g>
    {
        Upgrade::into_guard(self)
    }
}

/// 手写的可升级读获取流程。
pub(super) struct UpgradableReadAcquireInner<'a, 'g, T: ?Sized, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    sess_: Option<&'g mut CooperativeAcqSession<'a, T, D, B, O>>,
    pending_: Option<(Arc<WaitNode>, usize)>,
}

impl<'a, 'g, T: ?Sized, D, B, O> UpgradableReadAcquireInner<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        UpgradableReadAcquireInner {
            sess_: Option::Some(sess),
            pending_: Option::None,
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> Future
    for UpgradableReadAcquireInner<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Output =
        Result<UpgradableReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(sess) = this.sess_.take() else {
            panic!("[UpgradableReadAcquireInner::poll] polled after completion");
        };

        if this.pending_.is_none() && sess.core().try_upgradable_read_fast() {
            return Poll::Ready(Result::Ok(UpgradableReaderGuard::new(sess)));
        }

        let pending = this.pending_.clone();
        let claimed = match pending {
            Option::Some((node, slot)) => {
                sess.core().poll_acquire(&node, slot, cx.waker())
            }
            Option::None => {
                match sess.core().acquire(WaitKind::UpgradableRead, cx.waker()) {
                    AcqProgress::Acquired => true,
                    AcqProgress::Waiting { node_, slot_ } => {
                        this.pending_ = Option::Some((node_, slot_));
                        false
                    }
                }
            }
        };

        if claimed {
            this.pending_ = Option::None;
            Poll::Ready(Result::Ok(UpgradableReaderGuard::new(sess)))
        } else {
            this.sess_ = Option::Some(sess);
            Poll::Pending
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> Drop
    for UpgradableReadAcquireInner<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        let (Option::Some(sess), Option::Some((node, slot))) =
            (self.sess_.as_ref(), self.pending_.as_ref())
        else {
            return;
        };
        sess.core().cancel_wait(node, *slot);
    }
}

/// 可升级读获取的参数载体；用法见 [`super::reader_::ReadAcquireAsync`]。
pub struct UpgradableReadAcquireAsync<'a, 'g, T: ?Sized, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    sess_: &'g mut CooperativeAcqSession<'a, T, D, B, O>,
}

impl<'a, 'g, T: ?Sized, D, B, O> UpgradableReadAcquireAsync<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        UpgradableReadAcquireAsync { sess_: sess }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> IntoFuture
    for UpgradableReadAcquireAsync<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type IntoFuture =
        UpgradableReadAcquireFuture<'a, 'g, T, D, B, O, NonCancellableToken>;

    type Output =
        Result<UpgradableReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

    fn into_future(self) -> Self::IntoFuture {
        UpgradableReadAcquireFuture::new(self.sess_)
    }
}

impl<'f, 'a, 'g, T, D, B, O> TrMayCancel<'f>
    for UpgradableReadAcquireAsync<'a, 'g, T, D, B, O>
where
    'a: 'f,
    'g: 'f,
    T: ?Sized + 'f,
    D: TrAtomicData + Unsigned + 'f,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell> + 'f,
    O: TrCmpxchOrderings + 'f,
{
    type MayCancelFuture<'lt, C> =
        UpgradableReadAcquireFuture<'a, 'g, T, D, B, O, C>
    where
        'lt: 'f,
        Self: 'lt,
        C: 'lt + TrCancellationToken;

    type MayCancelOutput =
        Result<UpgradableReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'f, C>
    where
        C: 'f + TrCancellationToken,
    {
        UpgradableReadAcquireFuture::with_cancel(self.sess_, cancel)
    }
}

/// 可升级读获取 future；见 [`super::reader_::ReadAcquireFuture`] 的说明。
#[pin_project]
pub struct UpgradableReadAcquireFuture<
    'a,
    'g,
    T: ?Sized,
    D,
    B,
    O,
    C = NonCancellableToken,
>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    inner_: Option<UpgradableReadAcquireInner<'a, 'g, T, D, B, O>>,
    #[pin]
    cancel_: Option<C::Cancellation>,
}

impl<'a, 'g, T: ?Sized, D, B, O> UpgradableReadAcquireFuture<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        UpgradableReadAcquireFuture {
            inner_: Option::Some(UpgradableReadAcquireInner::new(sess)),
            cancel_: Option::None,
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, C>
    UpgradableReadAcquireFuture<'a, 'g, T, D, B, O, C>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn with_cancel(
        sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>,
        cancel: C,
    ) -> Self {
        UpgradableReadAcquireFuture {
            inner_: Option::Some(UpgradableReadAcquireInner::new(sess)),
            cancel_: Option::Some(cancel.cancellation()),
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, C> Future
    for UpgradableReadAcquireFuture<'a, 'g, T, D, B, O, C>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Output =
        Result<UpgradableReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

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
            panic!("[UpgradableReadAcquireFuture::poll] polled after completion");
        };
        let polled = Pin::new(inner).poll(cx);
        if polled.is_ready() {
            *this.inner_ = Option::None;
            this.cancel_.set(Option::None);
        }
        polled
    }
}

/// 手写的"升级为写者"流程。
///
/// 升级请求同样进入等待队列（插在队首，见 `core_` 的说明），并计入
/// `WRITER_QUEUED`：请求一入队就挡住新读者的快速路径，既有读者排空后
/// 升级必定成功。
///
/// 升级成功后写者守卫"代表"原来的可升级读槽位；写者守卫析构只是清掉
/// `WRITER_ACTIVE`，状态自动回退成"仍持有可升级读"。
pub(super) struct UpgradeAcquireInner<'a, 'g, 'f, T: ?Sized + 'a, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    upg_: Option<&'f mut Upgrade<'a, 'g, T, D, B, O>>,
    pending_: Option<(Arc<WaitNode>, usize)>,
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O> UpgradeAcquireInner<'a, 'g, 'f, T, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn new(upg: &'f mut Upgrade<'a, 'g, T, D, B, O>) -> Self {
        UpgradeAcquireInner {
            upg_: Option::Some(upg),
            pending_: Option::None,
        }
    }
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O> Future
    for UpgradeAcquireInner<'a, 'g, 'f, T, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Output = Result<WriterGuard<'a, 'f, T, D, B, O>, CoopRwLockError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(upg) = this.upg_.take() else {
            panic!("[UpgradeAcquireInner::poll] polled after completion");
        };
        let core = upg.sess_ref().core();
        // 快速路径：已经独占时直接升级，不分配节点、不进队列。
        if this.pending_.is_none() && core.try_upgrade_now() {
            let sess = upg.share_guard_sess_mut();
            return Poll::Ready(Result::Ok(WriterGuard::new(sess)));
        }

        let pending = this.pending_.clone();
        let claimed = match pending {
            Option::Some((node, slot)) => {
                core.poll_acquire(&node, slot, cx.waker())
            }
            Option::None => match core.acquire(WaitKind::Upgrade, cx.waker()) {
                AcqProgress::Acquired => true,
                AcqProgress::Waiting { node_, slot_ } => {
                    this.pending_ = Option::Some((node_, slot_));
                    false
                }
            },
        };

        if claimed {
            this.pending_ = Option::None;
            let sess = upg.share_guard_sess_mut();
            Poll::Ready(Result::Ok(WriterGuard::new(sess)))
        } else {
            this.upg_ = Option::Some(upg);
            Poll::Pending
        }
    }
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O> Drop
    for UpgradeAcquireInner<'a, 'g, 'f, T, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        let (Option::Some(upg), Option::Some((node, slot))) =
            (self.upg_.as_ref(), self.pending_.as_ref())
        else {
            return;
        };
        upg.sess_ref().core().cancel_wait(node, *slot);
    }
}

/// 升级获取的参数载体；用法见 [`super::reader_::ReadAcquireAsync`]。
pub struct UpgradeAcquireAsync<'a, 'g, 'f, T: ?Sized + 'a, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    upg_: &'f mut Upgrade<'a, 'g, T, D, B, O>,
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O> UpgradeAcquireAsync<'a, 'g, 'f, T, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(upg: &'f mut Upgrade<'a, 'g, T, D, B, O>) -> Self {
        UpgradeAcquireAsync { upg_: upg }
    }
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O> IntoFuture
    for UpgradeAcquireAsync<'a, 'g, 'f, T, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type IntoFuture =
        UpgradeAcquireFuture<'a, 'g, 'f, T, D, B, O, NonCancellableToken>;

    type Output = Result<WriterGuard<'a, 'f, T, D, B, O>, CoopRwLockError>;

    fn into_future(self) -> Self::IntoFuture {
        UpgradeAcquireFuture::new(self.upg_)
    }
}

impl<'lt, 'a, 'g, 'f, T, D, B, O> TrMayCancel<'lt>
    for UpgradeAcquireAsync<'a, 'g, 'f, T, D, B, O>
where
    'a: 'lt,
    'g: 'f,
    'f: 'lt,
    'g: 'lt,
    T: ?Sized + 'a + 'lt,
    D: TrAtomicData + Unsigned + 'lt,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell> + 'lt,
    O: TrCmpxchOrderings + 'lt,
{
    type MayCancelFuture<'lt2, C> =
        UpgradeAcquireFuture<'a, 'g, 'f, T, D, B, O, C>
    where
        'lt2: 'lt,
        Self: 'lt2,
        C: 'lt2 + TrCancellationToken;

    type MayCancelOutput =
        Result<WriterGuard<'a, 'f, T, D, B, O>, CoopRwLockError>;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'lt, C>
    where
        C: 'lt + TrCancellationToken,
    {
        UpgradeAcquireFuture::with_cancel(self.upg_, cancel)
    }
}

/// 升级获取 future；见 [`super::reader_::ReadAcquireFuture`] 的说明。
#[pin_project]
pub struct UpgradeAcquireFuture<
    'a,
    'g,
    'f,
    T: ?Sized + 'a,
    D,
    B,
    O,
    C = NonCancellableToken,
>
where
    'g: 'f,
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    inner_: Option<UpgradeAcquireInner<'a, 'g, 'f, T, D, B, O>>,
    #[pin]
    cancel_: Option<C::Cancellation>,
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O> UpgradeAcquireFuture<'a, 'g, 'f, T, D, B, O>
where
    'g: 'f,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(upg: &'f mut Upgrade<'a, 'g, T, D, B, O>) -> Self {
        UpgradeAcquireFuture {
            inner_: Option::Some(UpgradeAcquireInner::new(upg)),
            cancel_: Option::None,
        }
    }
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O, C>
    UpgradeAcquireFuture<'a, 'g, 'f, T, D, B, O, C>
where
    'g: 'f,
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn with_cancel(
        upg: &'f mut Upgrade<'a, 'g, T, D, B, O>,
        cancel: C,
    ) -> Self {
        UpgradeAcquireFuture {
            inner_: Option::Some(UpgradeAcquireInner::new(upg)),
            cancel_: Option::Some(cancel.cancellation()),
        }
    }
}

impl<'a, 'g, 'f, T: ?Sized + 'a, D, B, O, C> Future
    for UpgradeAcquireFuture<'a, 'g, 'f, T, D, B, O, C>
where
    'g: 'f,
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Output = Result<WriterGuard<'a, 'f, T, D, B, O>, CoopRwLockError>;

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
            panic!("[UpgradeAcquireFuture::poll] polled after completion");
        };
        let polled = Pin::new(inner).poll(cx);
        if polled.is_ready() {
            *this.inner_ = Option::None;
            this.cancel_.set(Option::None);
        }
        polled
    }
}
