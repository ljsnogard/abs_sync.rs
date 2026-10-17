//! 读许可的守卫与异步获取。

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

use super::{
    core_::AcqProgress,
    error_::CoopRwLockError,
    rwlock_::CooperativeAcqSession,
    wait_::{WaitKind, WaitNode},
};

/// 读许可守卫。
///
/// 它借用会话，因此同一会话在守卫存活期间不能再发起别的获取。
pub struct ReaderGuard<'a, 'g, T: ?Sized, D, B, O>(
    &'g mut CooperativeAcqSession<'a, T, D, B, O>,
)
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<'a, 'g, T: ?Sized, D, B, O> ReaderGuard<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        ReaderGuard(sess)
    }
}

impl<'a, T: ?Sized, D, B, O> Drop for ReaderGuard<'a, '_, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn drop(&mut self) {
        self.0.core().release_reader()
    }
}

impl<'a, T: ?Sized, D, B, O> Deref for ReaderGuard<'a, '_, T, D, B, O>
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
    for ReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
}

impl<'a, 'g, T: ?Sized, D, B, O> TrReaderGuard<'a, 'g, T>
    for ReaderGuard<'a, 'g, T, D, B, O>
where
    'a: 'g,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Acquire = CooperativeAcqSession<'a, T, D, B, O>;
}

/// 手写的获取流程：入队、领取、注册 waker、以及取消时的出队。
///
/// 这是**具体类型**而不是 `async fn` 生成的不透明类型。不透明类型无法把
/// `Send` 自动特征可靠地渗透出去（rust#100013），会让
/// `tokio::spawn(async { .. .write_async().await .. })` 直接编译不过；
/// 具体类型则按字段结构自然地推导出 `Send`。
pub(super) struct ReadAcquireInner<'a, 'g, T: ?Sized, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    sess_: Option<&'g mut CooperativeAcqSession<'a, T, D, B, O>>,
    pending_: Option<(Arc<WaitNode>, usize)>,
}

impl<'a, 'g, T: ?Sized, D, B, O> ReadAcquireInner<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        ReadAcquireInner {
            sess_: Option::Some(sess),
            pending_: Option::None,
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> Future for ReadAcquireInner<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Output = Result<ReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 本类型所有字段都是 `Unpin`，取 `&mut self` 是安全的。
        let this = self.get_mut();
        let Some(sess) = this.sess_.take() else {
            panic!("[ReadAcquireInner::poll] polled after completion");
        };

        // 快速路径：无竞争时不分配节点、不加锁。
        if this.pending_.is_none() && sess.core().try_read_fast() {
            return Poll::Ready(Result::Ok(ReaderGuard::new(sess)));
        }

        let pending = this.pending_.clone();
        let claimed = match pending {
            Option::Some((node, slot)) => {
                sess.core().poll_acquire(&node, slot, cx.waker())
            }
            Option::None => match sess.core().acquire(WaitKind::Read, cx.waker())
            {
                AcqProgress::Acquired => true,
                AcqProgress::Waiting { node_, slot_ } => {
                    this.pending_ = Option::Some((node_, slot_));
                    false
                }
            },
        };

        if claimed {
            this.pending_ = Option::None;
            Poll::Ready(Result::Ok(ReaderGuard::new(sess)))
        } else {
            this.sess_ = Option::Some(sess);
            Poll::Pending
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> Drop for ReadAcquireInner<'a, 'g, T, D, B, O>
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

/// 读获取的参数载体。
///
/// 它由 `read_async()` 返回：既可 `.await`（不可取消），也可先
/// `.may_cancel_with(token)` 换成可取消的版本再 `.await`。
pub struct ReadAcquireAsync<'a, 'g, T: ?Sized, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    sess_: &'g mut CooperativeAcqSession<'a, T, D, B, O>,
}

impl<'a, 'g, T: ?Sized, D, B, O> ReadAcquireAsync<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        ReadAcquireAsync { sess_: sess }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O> IntoFuture
    for ReadAcquireAsync<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type IntoFuture = ReadAcquireFuture<'a, 'g, T, D, B, O, NonCancellableToken>;

    type Output = Result<ReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

    fn into_future(self) -> Self::IntoFuture {
        ReadAcquireFuture::new(self.sess_)
    }
}

impl<'f, 'a, 'g, T, D, B, O> TrMayCancel<'f>
    for ReadAcquireAsync<'a, 'g, T, D, B, O>
where
    'a: 'f,
    'g: 'f,
    T: ?Sized + 'f,
    D: TrAtomicData + Unsigned + 'f,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell> + 'f,
    O: TrCmpxchOrderings + 'f,
{
    type MayCancelFuture<'lt, C> = ReadAcquireFuture<'a, 'g, T, D, B, O, C>
    where
        'lt: 'f,
        Self: 'lt,
        C: 'lt + TrCancellationToken;

    type MayCancelOutput =
        Result<ReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'f, C>
    where
        C: 'f + TrCancellationToken,
    {
        ReadAcquireFuture::with_cancel(self.sess_, cancel)
    }
}

/// 读获取 future。
///
/// 与 `gen_mcf2` 生成的两件套形状一致（`XxxAsync` + `XxxFuture`），但内部状态
/// 是**具体类型**而非不透明 future：不透明 future 的 `Send` 在泛型生命周期下
/// 无法被证明（rust#100013），会让 `tokio::spawn(async { .. .write_async().await
/// .. })` 编译不过。
#[pin_project]
pub struct ReadAcquireFuture<'a, 'g, T: ?Sized, D, B, O, C = NonCancellableToken>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    inner_: Option<ReadAcquireInner<'a, 'g, T, D, B, O>>,
    #[pin]
    cancel_: Option<C::Cancellation>,
}

impl<'a, 'g, T: ?Sized, D, B, O> ReadAcquireFuture<'a, 'g, T, D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    /// 不可取消的版本。
    pub(super) fn new(sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>) -> Self {
        ReadAcquireFuture {
            inner_: Option::Some(ReadAcquireInner::new(sess)),
            cancel_: Option::None,
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, C> ReadAcquireFuture<'a, 'g, T, D, B, O, C>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    /// 可取消的版本。
    pub(super) fn with_cancel(
        sess: &'g mut CooperativeAcqSession<'a, T, D, B, O>,
        cancel: C,
    ) -> Self {
        ReadAcquireFuture {
            inner_: Option::Some(ReadAcquireInner::new(sess)),
            cancel_: Option::Some(cancel.cancellation()),
        }
    }
}

impl<'a, 'g, T: ?Sized, D, B, O, C> Future
    for ReadAcquireFuture<'a, 'g, T, D, B, O, C>
where
    C: TrCancellationToken,
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    type Output = Result<ReaderGuard<'a, 'g, T, D, B, O>, CoopRwLockError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        // 取消优先：令牌先就绪就放弃获取。
        if let Some(cancel) = this.cancel_.as_mut().as_pin_mut()
            && cancel.poll(cx).is_ready()
        {
            // 丢弃 inner_：它的 Drop 会把自己从等待队列里撤销。
            *this.inner_ = Option::None;
            this.cancel_.set(Option::None);
            return Poll::Ready(Result::Err(CoopRwLockError::Cancelled));
        }
        let Some(inner) = this.inner_.as_mut() else {
            panic!("[ReadAcquireFuture::poll] polled after completion");
        };
        let polled = Pin::new(inner).poll(cx);
        if polled.is_ready() {
            *this.inner_ = Option::None;
            this.cancel_.set(Option::None);
        }
        polled
    }
}
