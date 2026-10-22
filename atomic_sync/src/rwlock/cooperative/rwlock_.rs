//! 协作式读写锁的外壳与会话。

use alloc::{
    alloc::{Allocator, Global},
    sync::Arc,
};
use core::{
    alloc::AllocatorClone, borrow::BorrowMut, cell::UnsafeCell, marker::PhantomPinned,
    sync::atomic::AtomicUsize,
};

use funty::{Integral, Unsigned};

use abs_sync::async_rwlock::*;
use atomex::{StrictOrderings, TrAtomicCell, TrAtomicData, TrCmpxchOrderings, x_deps::funty};

use crate::rwlock::TrShareMut;

use super::{
    core_::RwCore,
    reader_::{ReadAcquireAsync, ReaderGuard},
    upgrade_::{UpgradableReadAcquireAsync, UpgradableReaderGuard},
    writer_::{WriteAcquireAsync, WriterGuard},
};
use crate::rwlock::error_::CoopRwLockError;

/// 以"借用的原子单元"构造的协作式读写锁。
///
/// 最后一个类型参数 `A` 是等待队列使用的分配器，默认 [`Global`]。
pub type CooperativeRwLockBorrowed<'a, T, C = AtomicUsize, O = StrictOrderings, A = Global> =
    CooperativeRwLock<T, <C as TrAtomicCell>::Value, &'a mut C, O, A>;

/// 以"自有的原子单元"构造的协作式读写锁。
///
/// 最后一个类型参数 `A` 是等待队列使用的分配器，默认 [`Global`]。
pub type CooperativeRwLockOwned<T, C = AtomicUsize, O = StrictOrderings, A = Global> =
    CooperativeRwLock<T, <C as TrAtomicCell>::Value, C, O, A>;

impl<T, C, O, A> CooperativeRwLockOwned<T, C, O, A>
where
    C: TrAtomicCell,
    <C as TrAtomicCell>::Value: TrAtomicData<AtomicCell = C> + Unsigned,
    O: TrCmpxchOrderings,
    A: AllocatorClone + Default,
{
    /// 用默认（清零）状态字与默认分配器构造。
    pub fn new_owned(data: T) -> Self {
        let val = <<C as TrAtomicCell>::Value as Integral>::ZERO;
        let cell = <C as TrAtomicCell>::new(val);
        CooperativeRwLockOwned::<T, C, O, A>::new(data, cell)
    }
}

impl<T, C, O, A> CooperativeRwLockOwned<T, C, O, A>
where
    C: TrAtomicCell,
    <C as TrAtomicCell>::Value: TrAtomicData<AtomicCell = C> + Unsigned,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    /// 用默认（清零）状态字与给定分配器构造。
    pub fn new_owned_in(data: T, alloc: A) -> Self {
        let val = <<C as TrAtomicCell>::Value as Integral>::ZERO;
        let cell = <C as TrAtomicCell>::new(val);
        CooperativeRwLockOwned::<T, C, O, A>::new_in(data, cell, alloc)
    }
}

/// 协作式读写锁。
///
/// 它把被保护的资源 `T` 内联持有（`T: ?Sized` 时作为最后一个字段），
/// 而把全部同步状态放在 `Arc<RwCore<..>>` 里。`RwCore` 不泛型于 `T`，
/// 因此自身 `Sized` 且大小固定。
///
/// # 分配器
///
/// 最后一个类型参数 `A`（默认 [`Global`]）用于等待队列与共享内核 `Arc` 的
/// 分配。`new` / `new_owned` 使用 `A::default()`；带状态的分配器请改用
/// `new_in` / `new_owned_in`。
///
/// # 公平性
///
/// 普通读者在没有写者持有、且队列中没有写者/升级等待者时走无锁快速路径；
/// 队列里只有读者时，新读者仍然可以插队。写者与可升级读者则需要队列为空
/// 才能走快速路径；写者/升级一旦入队就会挡住后续读者的快速路径，因此不会被
/// 持续到达的读者饿死。
///
/// # 读者计数上界
///
/// 状态字把 `D` 的最高四位用作标志位，读者计数占其余位，因此读者上限是
/// `D::MAX >> 4`。默认 `usize` 下为 `2^60 - 1`；若显式使用较窄的 `D`
/// （例如 `u8`），上限会显著下降（`u8` 下为 15）。
///
/// # 关于移动
///
/// 本类型带 [`PhantomPinned`]，语义上是"一旦投入使用就不要移动"。
/// 这里不要求调用方 `Pin`（`TrAsyncRwLock::acq_session` 只取 `&self`），
/// 实际安全性由借用检查保证：会话借用外壳（`&'a self`）、Guard 借用会话，
/// 因此只要还有会话或 Guard 存在，外壳就不可能被移动；而
/// `into_inner(self)` 消费外壳时又不可能有存活的借用。
///
/// # Examples
///
/// ```
/// use atomic_sync::rwlock::cooperative::CooperativeRwLockOwned;
/// use atomic_sync::x_deps::abs_sync::async_rwlock::TrAsyncRwLock;
///
/// let lock = CooperativeRwLockOwned::<usize>::new_owned(41);
/// let mut sess = lock.acq_session();
/// let mut w = sess.try_write().unwrap();
/// *w += 1;
/// drop(w);
///
/// let r = sess.try_read().unwrap();
/// assert_eq!(*r, 42);
/// ```
pub struct CooperativeRwLock<
    T: ?Sized,
    D = usize,
    B = AtomicUsize,
    O = StrictOrderings,
    A: Allocator = Global,
> where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    _pin_: PhantomPinned,
    core_: Arc<RwCore<D, B, O, A>, A>,
    data_: UnsafeCell<T>,
}

impl<T, D, B, O, A> CooperativeRwLock<T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone + Default,
{
    /// 用给定的原子单元与默认分配器构造；`cell` 的初值必须是零（空闲状态）。
    pub fn new(data: T, cell: B) -> Self {
        Self::new_in(data, cell, A::default())
    }
}

impl<T, D, B, O, A> CooperativeRwLock<T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    /// 用给定的原子单元与分配器构造；`cell` 的初值必须是零（空闲状态）。
    pub fn new_in(data: T, cell: B, alloc: A) -> Self {
        CooperativeRwLock {
            _pin_: PhantomPinned,
            core_: Arc::new_in(RwCore::new_in(cell, alloc.clone()), alloc),
            data_: UnsafeCell::new(data),
        }
    }

    /// 消费本锁，取出被保护的资源。
    ///
    /// 因为 `self` 被消费，此时不可能存在任何会话或 Guard。
    pub fn into_inner(self) -> T {
        self.data_.into_inner()
    }
}

impl<T: ?Sized, D, B, O, A> CooperativeRwLock<T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    /// 建立一个会话。
    pub const fn acquire_session(&self) -> CooperativeAcqSession<'_, T, D, B, O, A> {
        CooperativeAcqSession::new(self)
    }

    /// 返回指向被保护资源的裸指针。
    ///
    /// 与 `std::sync::RwLock::data_ptr` 类似：读写它需要自行保证已持有相应的
    /// Guard，否则是未定义行为。
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut T {
        self.data_.get()
    }

    /// 当前读者数量（含可升级读者），仅用于启发式判断。
    #[inline]
    pub fn reader_count(&self) -> usize {
        let c = self.core_.reader_count();
        let Result::Ok(c) = c.try_into() else {
            unreachable!("[CooperativeRwLock::reader_count]")
        };
        c
    }
}

impl<T: ?Sized, D, B, O, A> TrAsyncRwLock for CooperativeRwLock<T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    type Target = T;

    type AcqSess<'f>
        = CooperativeAcqSession<'f, T, D, B, O, A>
    where
        Self: 'f;

    type Err = CoopRwLockError;

    #[inline]
    fn acq_session(&self) -> Self::AcqSess<'_> {
        CooperativeRwLock::acquire_session(self)
    }
}

// SAFETY: 与 `std::sync::RwLock` 相同的论证：只要 `T: Send + Sync`，
// 跨线程共享本锁、以及把锁的所有权搬到别的线程都是安全的；
// 独占访问由状态字保证。
unsafe impl<T: ?Sized + Send, D, B, O, A> Send for CooperativeRwLock<T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell> + Send,
    O: TrCmpxchOrderings,
    A: AllocatorClone + Send,
{
}

// SAFETY: 见上。
unsafe impl<T: ?Sized + Send + Sync, D, B, O, A> Sync for CooperativeRwLock<T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell> + Sync,
    O: TrCmpxchOrderings,
    A: AllocatorClone + Sync,
{
}

/// 一次上锁会话：它只是外壳的借用，本身不持有任何状态。
pub struct CooperativeAcqSession<'a, T: ?Sized, D, B, O, A = Global>(
    &'a CooperativeRwLock<T, D, B, O, A>,
)
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone;

impl<'a, T: ?Sized, D, B, O, A> CooperativeAcqSession<'a, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    #[inline]
    pub const fn new(lock: &'a CooperativeRwLock<T, D, B, O, A>) -> Self {
        CooperativeAcqSession(lock)
    }

    #[inline]
    pub fn try_read(&mut self) -> Result<ReaderGuard<'a, '_, T, D, B, O, A>, CoopRwLockError> {
        if self.0.core_.try_read_fast() {
            Result::Ok(ReaderGuard::new(self))
        } else {
            Result::Err(CoopRwLockError::WouldBlock)
        }
    }

    #[inline]
    pub fn try_write(&mut self) -> Result<WriterGuard<'a, '_, T, D, B, O, A>, CoopRwLockError> {
        if self.0.core_.try_write_fast() {
            Result::Ok(WriterGuard::new(self))
        } else {
            Result::Err(CoopRwLockError::WouldBlock)
        }
    }

    #[inline]
    pub fn try_upgradable_read(
        &mut self,
    ) -> Result<UpgradableReaderGuard<'a, '_, T, D, B, O, A>, CoopRwLockError> {
        if self.0.core_.try_upgradable_read_fast() {
            Result::Ok(UpgradableReaderGuard::new(self))
        } else {
            Result::Err(CoopRwLockError::WouldBlock)
        }
    }

    #[inline]
    pub fn read_async<'g>(&'g mut self) -> ReadAcquireAsync<'a, 'g, T, D, B, O, A>
    where
        'a: 'g,
    {
        ReadAcquireAsync::new(self)
    }

    #[inline]
    pub fn write_async<'g>(&'g mut self) -> WriteAcquireAsync<'a, 'g, T, D, B, O, A>
    where
        'a: 'g,
    {
        WriteAcquireAsync::new(self)
    }

    #[inline]
    pub fn upgradable_read_async<'g>(
        &'g mut self,
    ) -> UpgradableReadAcquireAsync<'a, 'g, T, D, B, O, A>
    where
        'a: 'g,
    {
        UpgradableReadAcquireAsync::new(self)
    }
}

impl<'a, T: ?Sized, D, B, O, A> CooperativeAcqSession<'a, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    #[inline]
    pub(super) fn core(&self) -> &RwCore<D, B, O, A> {
        &self.0.core_
    }

    #[inline]
    pub(super) fn deref_impl(&self) -> &T {
        // SAFETY: 只有持有读/写 Guard 时才会经由本方法访问，而 Guard 的存在
        // 意味着当前线程持有对应许可。
        unsafe { &*self.0.data_.get() }
    }

    #[inline]
    pub(super) fn deref_mut_impl(&mut self) -> &mut T {
        // SAFETY: 只有持有写 Guard 时才会经由本方法访问。
        unsafe { &mut *self.0.data_.get() }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Guard 之间的转换
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn downgrade_writer_to_reader<'g>(
        guard: WriterGuard<'a, 'g, T, D, B, O, A>,
    ) -> ReaderGuard<'a, 'g, T, D, B, O, A> {
        let sess = Self::destruct_guard_(guard);
        sess.core().downgrade_write_to_read();
        ReaderGuard::new(sess)
    }

    pub(super) fn downgrade_writer_to_upgradable<'g>(
        guard: WriterGuard<'a, 'g, T, D, B, O, A>,
    ) -> UpgradableReaderGuard<'a, 'g, T, D, B, O, A> {
        let sess = Self::destruct_guard_(guard);
        sess.core().downgrade_write_to_upgradable();
        UpgradableReaderGuard::new(sess)
    }

    pub(super) fn downgrade_upgradable_to_reader<'g>(
        guard: UpgradableReaderGuard<'a, 'g, T, D, B, O, A>,
    ) -> ReaderGuard<'a, 'g, T, D, B, O, A> {
        let sess = Self::destruct_guard_(guard);
        sess.core().downgrade_upgradable_to_read();
        ReaderGuard::new(sess)
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn try_upgrade_to_writer<'g>(
        mut guard: UpgradableReaderGuard<'a, 'g, T, D, B, O, A>,
    ) -> Result<WriterGuard<'a, 'g, T, D, B, O, A>, UpgradableReaderGuard<'a, 'g, T, D, B, O, A>>
    {
        let guard_ptr = &mut guard as *mut UpgradableReaderGuard<'a, 'g, T, D, B, O, A>;
        // SAFETY: `guard` 在本次调用中不会被移动（比较失败时原样返回），
        // 因此这里的可变借用是独占且有效的。
        if let Result::Ok(g) = Self::try_upgrade_mut_to_writer(unsafe { &mut *guard_ptr }) {
            Result::Ok(g)
        } else {
            Result::Err(guard)
        }
    }

    pub(super) fn try_upgrade_mut_to_writer<'g, 'u>(
        guard: &'u mut UpgradableReaderGuard<'a, 'g, T, D, B, O, A>,
    ) -> Result<WriterGuard<'a, 'u, T, D, B, O, A>, CoopRwLockError> {
        let sess = guard.share_mut();
        if sess.core().try_upgrade_now() {
            Result::Ok(WriterGuard::new(sess))
        } else {
            Result::Err(CoopRwLockError::WouldBlock)
        }
    }

    fn destruct_guard_<'g, G>(guard: G) -> &'g mut Self
    where
        Self: 'g,
        G: TrShareMut<'g, Self>,
    {
        let mut m = core::mem::ManuallyDrop::new(guard);
        // SAFETY: `guard` 被封在 `ManuallyDrop` 里，不会执行 Drop，
        // 因此它的生命周期可以安全地延长到 `'g`。
        (*m).share_mut()
    }
}

impl<'a, T: ?Sized, D, B, O, A> TrAsyncRwLockAcqSess<'a, T>
    for CooperativeAcqSession<'a, T, D, B, O, A>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
    A: AllocatorClone,
{
    type ReaderGuard<'g>
        = ReaderGuard<'a, 'g, T, D, B, O, A>
    where
        'a: 'g;

    type WriterGuard<'g>
        = WriterGuard<'a, 'g, T, D, B, O, A>
    where
        'a: 'g;

    type UpgradableGuard<'g>
        = UpgradableReaderGuard<'a, 'g, T, D, B, O, A>
    where
        'a: 'g;

    type Err = CoopRwLockError;

    #[inline]
    fn try_read<'g>(&'g mut self) -> Result<Self::ReaderGuard<'g>, Self::Err>
    where
        'a: 'g,
    {
        CooperativeAcqSession::try_read(self)
    }

    #[inline]
    fn try_write<'g>(&'g mut self) -> Result<Self::WriterGuard<'g>, Self::Err>
    where
        'a: 'g,
    {
        CooperativeAcqSession::try_write(self)
    }

    #[inline]
    fn try_upgradable_read<'g>(&'g mut self) -> Result<Self::UpgradableGuard<'g>, Self::Err>
    where
        'a: 'g,
    {
        CooperativeAcqSession::try_upgradable_read(self)
    }

    type ReadAsync<'f>
        = ReadAcquireAsync<'a, 'f, T, D, B, O, A>
    where
        'a: 'f;

    #[inline]
    fn read_async<'g>(&'g mut self) -> Self::ReadAsync<'g>
    where
        'a: 'g,
    {
        CooperativeAcqSession::read_async(self)
    }

    type WriteAsync<'f>
        = WriteAcquireAsync<'a, 'f, T, D, B, O, A>
    where
        'a: 'f;

    #[inline]
    fn write_async<'g>(&'g mut self) -> Self::WriteAsync<'g>
    where
        'a: 'g,
    {
        CooperativeAcqSession::write_async(self)
    }

    type UpgradableReadAsync<'f>
        = UpgradableReadAcquireAsync<'a, 'f, T, D, B, O, A>
    where
        'a: 'f;

    #[inline]
    fn upgradable_read_async<'g>(&'g mut self) -> Self::UpgradableReadAsync<'g>
    where
        'a: 'g,
    {
        CooperativeAcqSession::upgradable_read_async(self)
    }
}
