//! 协作式读写锁的状态字。
//!
//! 本文件只描述"许可如何在读写者之间迁移"这件纯逻辑的事，不涉及等待队列
//! 与唤醒机制。

use core::{borrow::BorrowMut, fmt, marker::PhantomData};

use funty::Unsigned;

use atomex::{
    x_deps::funty,
    StrictOrderings, TrAtomicData, TrAtomicFlags, TrCmpxchOrderings,
};

/// [`CoopRwState`] 的一次性快照，供放行流程做统一判断。
///
/// `writer_queued` 只用于测试与 `debug_assert` 观测：放行流程本身不依赖它。
pub(super) struct CoopRwStateSnapshot<D> {
    pub(super) writer_active: bool,
    pub(super) writer_queued: bool,
    pub(super) upgrade_active: bool,
    pub(super) reader_count: D,
}

/// 协作式读写锁的状态字。
///
/// 状态字把一个无符号整数 `D` 切成五个域：
///
/// ```text
/// bit N-1        WRITER_ACTIVE   写者持有
/// bit N-2        WAITERS_PRESENT 等待队列非空
/// bit N-3        WRITER_QUEUED   队列中存在写者/升级等待者
/// bit N-4        UPGRADE_ACTIVE  存在可升级读者
/// bit [0, N-5]   READER_COUNT    读者计数（含可升级读者）
/// ```
///
/// `WAITERS_PRESENT` 与 `WRITER_QUEUED` 都由等待队列的槽位状态维护，且**只在
/// 持有队列锁时**更新。两者的分工是：
///
/// - `WAITERS_PRESENT`（队列非空）只用于 `run_pass_` 的快出口，以及挡住写者、
///   可升级读者的快速路径；
/// - `WRITER_QUEUED`（队列里有 `Write`/`Upgrade` 等待者）挡住**普通读者**的
///   快速路径。
///
/// 也就是说：队列里**只有读者**时，新读者仍然可以走无锁快速路径；只有出现写者
/// 或升级等待者，读者才被挡回队列。这样既避免了"存在一个等待者就放大成所有读者
/// 都要排队"的开销，又不会让写者/升级被持续到达的读者饿死。
///
/// # 升级为什么算栅栏
///
/// 可升级读者的升级条件是"其余读者全部退出"。升级请求本身会进入等待队列
/// （插在队首，见 `core_`），并计入 `WRITER_QUEUED`，于是新读者在快速路径上
/// 就被拒绝；既有读者排空后升级必定成功。所以"升级被新读者无限推迟"这件事
/// 在 cooperative 里不会发生，不需要 `preemptive` 那种队列之外的等待槽。
///
/// # 升级得到的写者如何记账
///
/// 升级成功时**保留** `UPGRADE_ACTIVE` 与读者计数，只置 `WRITER_ACTIVE`：
/// 升级得到的写者仍然"代表"那个可升级读槽位，槽位由可升级读守卫自身析构时
/// 归还。这样升级前后 `UpgradeSession` 的所有权模型不变，
/// 写者侧也只剩唯一一条释放路径。
pub(super) struct CoopRwState<D, B, O = StrictOrderings>(
    B,
    PhantomData<D>,
    PhantomData<O>,
)
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<D, B, O> AsRef<<D as TrAtomicData>::AtomicCell> for CoopRwState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn as_ref(&self) -> &<D as TrAtomicData>::AtomicCell {
        self.0.borrow()
    }
}

impl<D, B, O> TrAtomicFlags<D, O> for CoopRwState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{}

impl<D, B, O> CoopRwState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub(super) const fn new(cell: B) -> Self {
        CoopRwState(cell, PhantomData, PhantomData)
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 域常量
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    #[allow(non_snake_case)]
    #[inline]
    fn K_WRITER_ACTIVE() -> D {
        D::ONE << (D::BITS - 1)
    }

    #[allow(non_snake_case)]
    #[inline]
    fn K_WAITERS_PRESENT() -> D {
        D::ONE << (D::BITS - 2)
    }

    #[allow(non_snake_case)]
    #[inline]
    fn K_WRITER_QUEUED() -> D {
        D::ONE << (D::BITS - 3)
    }

    #[allow(non_snake_case)]
    #[inline]
    fn K_UPGRADE_ACTIVE() -> D {
        D::ONE << (D::BITS - 4)
    }

    #[allow(non_snake_case)]
    #[inline]
    fn K_MAX_READER_COUNT() -> D {
        D::MAX >> 4
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 域谓词与期望值
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    fn expect_writer_active_(s: D) -> bool {
        s & Self::K_WRITER_ACTIVE() == Self::K_WRITER_ACTIVE()
    }
    fn expect_writer_inactive_(s: D) -> bool {
        !Self::expect_writer_active_(s)
    }
    fn desire_writer_active_(s: D) -> D {
        s | Self::K_WRITER_ACTIVE()
    }
    fn desire_writer_inactive_(s: D) -> D {
        s & !Self::K_WRITER_ACTIVE()
    }

    fn expect_waiters_present_(s: D) -> bool {
        s & Self::K_WAITERS_PRESENT() == Self::K_WAITERS_PRESENT()
    }
    fn expect_no_waiters_present_(s: D) -> bool {
        !Self::expect_waiters_present_(s)
    }
    fn desire_waiters_present_(s: D) -> D {
        s | Self::K_WAITERS_PRESENT()
    }
    fn desire_no_waiters_present_(s: D) -> D {
        s & !Self::K_WAITERS_PRESENT()
    }

    fn expect_writer_queued_(s: D) -> bool {
        s & Self::K_WRITER_QUEUED() == Self::K_WRITER_QUEUED()
    }
    fn expect_writer_not_queued_(s: D) -> bool {
        !Self::expect_writer_queued_(s)
    }
    fn desire_writer_queued_(s: D) -> D {
        s | Self::K_WRITER_QUEUED()
    }
    fn desire_writer_not_queued_(s: D) -> D {
        s & !Self::K_WRITER_QUEUED()
    }

    fn expect_upgrade_active_(s: D) -> bool {
        s & Self::K_UPGRADE_ACTIVE() == Self::K_UPGRADE_ACTIVE()
    }
    fn expect_upgrade_inactive_(s: D) -> bool {
        !Self::expect_upgrade_active_(s)
    }
    fn desire_upgrade_active_(s: D) -> D {
        s | Self::K_UPGRADE_ACTIVE()
    }
    fn desire_upgrade_inactive_(s: D) -> D {
        s & !Self::K_UPGRADE_ACTIVE()
    }

    fn get_reader_count_(s: D) -> D {
        s & Self::K_MAX_READER_COUNT()
    }
    fn expect_reader_lt_max_(s: D) -> bool {
        Self::get_reader_count_(s) < Self::K_MAX_READER_COUNT()
    }
    fn expect_reader_gt_min_(s: D) -> bool {
        Self::get_reader_count_(s) > D::ZERO
    }
    fn desire_inc_reader_(s: D) -> D {
        s + D::ONE
    }
    fn desire_dec_reader_(s: D) -> D {
        s - D::ONE
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 组合迁移
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 单次比较交换；成功或者谓词不成立时立即返回。
    #[inline]
    fn try_spin_update_(
        &self,
        expect: impl FnMut(D) -> bool,
        desire: impl FnMut(D) -> D,
    ) -> Result<D, D> {
        self.try_spin_compare_exchange_weak(expect, desire).into()
    }

    /// 只在"可升级读者独占全部读者"时才允许升级为写者。
    fn expect_can_upgrade_to_write_(s: D) -> bool {
        Self::expect_upgrade_active_(s)
            && Self::get_reader_count_(s) == D::ONE
            && Self::expect_writer_inactive_(s)
    }

    /// 升级为写者。
    ///
    /// **保留** `UPGRADE_ACTIVE` 与读者计数：升级得到的写者仍然"代表"那个
    /// 可升级读槽位，槽位要等可升级读守卫自己析构时才归还。这样
    /// `UpgradeSession` 在升级成功后依然可以 `try_upgrade`、`into_guard`，
    /// 或者再升级一次；而写者侧只有唯一的一条释放路径。
    /// 写者守卫析构后状态自动"回退"成原来的可升级读者。
    fn desire_upgrade_to_write_(s: D) -> D {
        Self::desire_writer_active_(s)
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 只读观察
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    #[inline]
    pub(super) fn load_state(&self) -> D {
        TrAtomicFlags::value(self)
    }

    #[inline]
    pub(super) fn reader_count(&self) -> D {
        Self::get_reader_count_(self.load_state())
    }

    #[inline]
    pub(super) fn waiters_present(&self) -> bool {
        Self::expect_waiters_present_(self.load_state())
    }

    /// 队列中是否还有写者/升级等待者（仅供单元测试观测）。
    #[cfg(test)]
    #[inline]
    pub(super) fn writer_queued(&self) -> bool {
        Self::expect_writer_queued_(self.load_state())
    }

    /// 一次性取出放行判定所需的全部信息。
    ///
    /// 快速路径可以绕过队列锁直接改状态字，因此放行流程必须基于**同一份**
    /// 状态快照做判断，不能分多次读取。
    #[inline]
    pub(super) fn snapshot(&self) -> CoopRwStateSnapshot<D> {
        let s = self.load_state();
        CoopRwStateSnapshot {
            writer_active: Self::expect_writer_active_(s),
            writer_queued: Self::expect_writer_queued_(s),
            upgrade_active: Self::expect_upgrade_active_(s),
            reader_count: Self::get_reader_count_(s),
        }
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 队列标记：调用者必须持有队列锁
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn mark_waiters_present(&self) -> bool {
        self.try_spin_update_(|_| true, Self::desire_waiters_present_)
            .is_ok()
    }

    pub(super) fn clear_waiters_present(&self) -> bool {
        self.try_spin_update_(|_| true, Self::desire_no_waiters_present_)
            .is_ok()
    }

    pub(super) fn mark_writer_queued(&self) -> bool {
        self.try_spin_update_(|_| true, Self::desire_writer_queued_)
            .is_ok()
    }

    pub(super) fn clear_writer_queued(&self) -> bool {
        self.try_spin_update_(|_| true, Self::desire_writer_not_queued_)
            .is_ok()
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 快速路径：仅当等待队列为空时可用
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 尝试以读者身份直接进入；成功时读者计数加一。
    ///
    /// 准入条件是"没有写者持有、队列里没有写者/升级等待者"。**队列里只有读者时
    /// 仍然放行**——这正是本策略与严格 FIFO 的差别。
    pub(super) fn try_read_fast(&self) -> bool {
        self.try_spin_update_(
            Self::expect_can_read_fast_,
            Self::desire_inc_reader_,
        )
        .is_ok()
    }

    fn expect_can_read_fast_(s: D) -> bool {
        Self::expect_writer_inactive_(s)
            && Self::expect_writer_not_queued_(s)
            && Self::expect_reader_lt_max_(s)
    }

    /// 尝试以写者身份直接进入；成功时置 `WRITER_ACTIVE`。
    pub(super) fn try_write_fast(&self) -> bool {
        self.try_spin_update_(
            Self::expect_can_write_fast_,
            Self::desire_writer_active_,
        )
        .is_ok()
    }

    fn expect_can_write_fast_(s: D) -> bool {
        Self::expect_writer_inactive_(s)
            && Self::expect_no_waiters_present_(s)
            && Self::get_reader_count_(s) == D::ZERO
            && Self::expect_upgrade_inactive_(s)
    }

    /// 尝试以可升级读者身份直接进入；成功时置 `UPGRADE_ACTIVE` 且读者计数加一。
    pub(super) fn try_upgradable_read_fast(&self) -> bool {
        self.try_spin_update_(
            Self::expect_can_upgradable_read_fast_,
            Self::desire_upgradable_read_,
        )
        .is_ok()
    }

    fn expect_can_upgradable_read_fast_(s: D) -> bool {
        Self::expect_writer_inactive_(s)
            && Self::expect_no_waiters_present_(s)
            && Self::expect_upgrade_inactive_(s)
            && Self::expect_reader_lt_max_(s)
    }

    fn desire_upgradable_read_(s: D) -> D {
        let s = Self::desire_upgrade_active_(s);
        Self::desire_inc_reader_(s)
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 排队路径：调用者已被 `pass()` 放行，只校验状态字条件
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 已被放行的读者尝试真正进入。
    ///
    /// 与快速路径的差别在于**不检查**两个队列标记：调用者本身就在队列中。
    /// 失败（例如可升级读者已挂起升级栅栏）时调用者应退回等待，
    /// 等待下一次放行。
    pub(super) fn try_read_queued(&self) -> bool {
        self.try_spin_update_(
            Self::expect_can_read_queued_,
            Self::desire_inc_reader_,
        )
        .is_ok()
    }

    fn expect_can_read_queued_(s: D) -> bool {
        Self::expect_writer_inactive_(s) && Self::expect_reader_lt_max_(s)
    }

    /// 已被放行的写者尝试真正进入。
    pub(super) fn try_write_queued(&self) -> bool {
        self.try_spin_update_(
            Self::expect_can_write_queued_,
            Self::desire_writer_active_,
        )
        .is_ok()
    }

    fn expect_can_write_queued_(s: D) -> bool {
        Self::expect_writer_inactive_(s)
            && Self::get_reader_count_(s) == D::ZERO
            && Self::expect_upgrade_inactive_(s)
    }

    /// 已被放行的可升级读者尝试真正进入。
    pub(super) fn try_upgradable_read_queued(&self) -> bool {
        self.try_spin_update_(
            Self::expect_can_upgradable_read_queued_,
            Self::desire_upgradable_read_,
        )
        .is_ok()
    }

    fn expect_can_upgradable_read_queued_(s: D) -> bool {
        Self::expect_writer_inactive_(s)
            && Self::expect_upgrade_inactive_(s)
            && Self::expect_reader_lt_max_(s)
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 释放、降级与升级
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    pub(super) fn release_reader(&self) -> bool {
        self.try_spin_update_(
            Self::expect_reader_gt_min_,
            Self::desire_dec_reader_,
        )
        .is_ok()
    }

    pub(super) fn release_upgradable_read(&self) -> bool {
        self.try_spin_update_(
            Self::expect_upgrade_active_and_reader_gt_min_,
            Self::desire_release_upgradable_,
        )
        .is_ok()
    }

    fn expect_upgrade_active_and_reader_gt_min_(s: D) -> bool {
        Self::expect_upgrade_active_(s) && Self::expect_reader_gt_min_(s)
    }

    fn desire_release_upgradable_(s: D) -> D {
        let s = Self::desire_upgrade_inactive_(s);
        Self::desire_dec_reader_(s)
    }

    /// 释放写者。
    ///
    /// 普通写者与"由可升级读者升级得到的写者"在这里行为一致：都只清
    /// `WRITER_ACTIVE`。升级路径**不**改动读者计数与 `UPGRADE_ACTIVE`，
    /// 那部分留给可升级读守卫自身析构时归还。
    pub(super) fn release_writer(&self) -> bool {
        self.try_spin_update_(
            Self::expect_writer_active_,
            Self::desire_writer_inactive_,
        )
        .is_ok()
    }

    /// 可升级读者升级为写者。
    pub(super) fn try_upgrade_to_write(&self) -> bool {
        self.try_spin_update_(
            Self::expect_can_upgrade_to_write_,
            Self::desire_upgrade_to_write_,
        )
        .is_ok()
    }

    /// 写者降级为普通读者。
    pub(super) fn downgrade_write_to_read(&self) -> bool {
        self.try_spin_update_(
            Self::expect_writer_active_,
            Self::desire_write_to_read_,
        )
        .is_ok()
    }

    fn desire_write_to_read_(s: D) -> D {
        let s = Self::desire_writer_inactive_(s);
        Self::desire_inc_reader_(s)
    }

    /// 写者降级为可升级读者。
    pub(super) fn downgrade_write_to_upgradable(&self) -> bool {
        self.try_spin_update_(
            Self::expect_writer_active_,
            Self::desire_write_to_upgradable_,
        )
        .is_ok()
    }

    fn desire_write_to_upgradable_(s: D) -> D {
        let s = Self::desire_writer_inactive_(s);
        let s = Self::desire_upgrade_active_(s);
        Self::desire_inc_reader_(s)
    }

    /// 可升级读者降级为普通读者。
    pub(super) fn downgrade_upgradable_to_read(&self) -> bool {
        self.try_spin_update_(
            Self::expect_upgrade_active_,
            Self::desire_downgrade_upgradable_,
        )
        .is_ok()
    }

    fn desire_downgrade_upgradable_(s: D) -> D {
        Self::desire_upgrade_inactive_(s)
    }
}

impl<D, B, O> fmt::Debug for CoopRwState<D, B, O>
where
    D: TrAtomicData + Unsigned,
    B: BorrowMut<<D as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.load_state();
        write!(
            f,
            "[CoopRwState: W({}), Q({}), WQ({}), U({}), R({})]",
            Self::expect_writer_active_(s),
            Self::expect_waiters_present_(s),
            Self::expect_writer_queued_(s),
            Self::expect_upgrade_active_(s),
            Self::get_reader_count_(s),
        )
    }
}

#[cfg(test)]
mod tests_ {
    use core::{fmt::Debug, sync::atomic::AtomicUsize};

    use atomex::StrictOrderings;

    use super::CoopRwState;

    type TestState = CoopRwState<usize, AtomicUsize, StrictOrderings>;

    fn new_state() -> TestState {
        CoopRwState::new(AtomicUsize::new(0))
    }

    /// 测试初始状态字是否为完全空闲。
    /// - 手段：构造一个计数为 0 的 `AtomicUsize` 状态字。
    /// - 判断：读者计数为 0、四个标志位均为假，且快速路径三种获取全部成功。
    #[test]
    fn new_state_should_be_idle() {
        let st = new_state();
        assert_eq!(st.reader_count(), 0);
        assert!(!st.snapshot().writer_active);
        assert!(!st.snapshot().upgrade_active);
        assert!(!st.snapshot().writer_queued);
        assert!(!st.waiters_present());
        assert!(!st.writer_queued());

        assert!(st.try_read_fast());
        assert!(st.release_reader());
        assert!(st.try_write_fast());
        assert!(st.release_writer());
        assert!(st.try_upgradable_read_fast());
        assert!(st.release_upgradable_read());
        assert!(!st.waiters_present());
        assert!(!st.writer_queued());
    }

    /// 测试"队列里只有读者"不再挡住新读者的快速路径。
    /// - 手段：只置位 `WAITERS_PRESENT`（模拟队列中只有读类等待者），
    ///   依次尝试三种快速获取，随后清除标记再试。
    /// - 判断：`try_read_fast` 成功（这是新策略的核心）；写者与可升级读的快速
    ///   路径仍被队列非空挡住；清位后写者快速路径恢复。
    #[test]
    fn read_fast_path_should_ignore_readers_only_queue() {
        let st = new_state();
        assert!(st.mark_waiters_present());
        assert!(st.waiters_present());
        assert!(!st.writer_queued());

        assert!(st.try_read_fast(), "只有读者排队时新读者应走快速路径");
        assert!(st.release_reader());

        assert!(!st.try_write_fast());
        assert!(!st.try_upgradable_read_fast());
        assert_eq!(st.reader_count(), 0);

        assert!(st.clear_waiters_present());
        assert!(!st.waiters_present());
        assert!(st.try_write_fast());
        assert!(st.release_writer());
    }

    /// 测试"队列里有写者/升级等待者"会挡住新读者的快速路径。
    /// - 手段：置位 `WRITER_QUEUED` 后尝试读快速路径，再清位重试。
    /// - 判断：置位期间 `try_read_fast` 失败且读者计数不变；清位后成功。
    #[test]
    fn writer_queued_should_block_read_fast_path() {
        let st = new_state();
        assert!(st.mark_writer_queued());
        assert!(st.writer_queued());
        assert!(!st.waiters_present());

        assert!(!st.try_read_fast());
        assert_eq!(st.reader_count(), 0);

        assert!(st.clear_writer_queued());
        assert!(!st.writer_queued());
        assert!(st.try_read_fast());
        assert!(st.release_reader());
    }

    /// 测试两个队列标记相互独立，且都不会污染读者计数。
    /// - 手段：先置 `WAITERS_PRESENT`，再叠加 `WRITER_QUEUED`，然后逐个清除。
    /// - 判断：两个标记的置位/清除互不影响；期间读者计数始终为 0；只有
    ///   `WRITER_QUEUED` 决定读快速路径的成败。
    #[test]
    fn queue_flags_should_be_independent() {
        let st = new_state();
        assert!(st.mark_waiters_present());
        assert!(st.waiters_present());
        assert!(!st.writer_queued());

        assert!(st.mark_writer_queued());
        assert!(st.writer_queued());

        assert!(st.clear_waiters_present());
        assert!(!st.waiters_present());
        assert!(st.writer_queued(), "清除 WAITERS_PRESENT 不应影响 WRITER_QUEUED");
        assert_eq!(st.reader_count(), 0);
        assert!(!st.try_read_fast());

        assert!(st.clear_writer_queued());
        assert!(!st.writer_queued());
        assert!(st.try_read_fast());
        assert!(st.release_reader());
        assert_eq!(st.reader_count(), 0);
    }

    /// 测试排队路径不受两个队列标记影响。
    /// - 手段：先置位 `WAITERS_PRESENT` 与 `WRITER_QUEUED`（模拟自己正在排队），
    ///   再依次走排队路径获取读者与可升级读者，最后释放全部读者并获取写者。
    /// - 判断：读者与写者的排队获取均成功；读者计数随获取/释放精确增减；
    ///   只要仍有读者，排队写者就必须失败。
    #[test]
    fn queued_path_should_ignore_queue_flags() {
        let st = new_state();
        assert!(st.mark_waiters_present());
        assert!(st.mark_writer_queued());

        assert!(st.try_read_queued());
        assert_eq!(st.reader_count(), 1);
        assert!(!st.try_write_queued());

        assert!(st.try_upgradable_read_queued());
        assert_eq!(st.reader_count(), 2);
        assert!(st.snapshot().upgrade_active);
        assert!(!st.try_write_queued());

        assert!(st.release_upgradable_read());
        assert_eq!(st.reader_count(), 1);
        assert!(!st.snapshot().upgrade_active);

        assert!(st.release_reader());
        assert_eq!(st.reader_count(), 0);
        assert!(st.try_write_queued());
        assert!(st.release_writer());
    }

    /// 测试升级得到的写者与可升级读槽位的记账关系。
    /// - 手段：先获得可升级读者（读者计数为 1），再升级为写者；然后先释放写者、
    ///   最后释放可升级读槽位。
    /// - 判断：升级时只有 `WRITER_ACTIVE` 置位，`UPGRADE_ACTIVE` 与读者计数都保留
    ///   （写者"代表"那个可升级读槽位）；写者释放后仍不能写（槽位还在），
    ///   直到可升级读槽位归还后 `try_write_fast` 才能成功。
    #[test]
    fn upgrade_to_write_should_keep_upgradable_slot() {
        let st = new_state();
        assert!(st.try_upgradable_read_fast());
        assert_eq!(st.reader_count(), 1);
        assert!(st.snapshot().upgrade_active);

        assert!(st.try_upgrade_to_write());
        assert_eq!(st.reader_count(), 1);
        assert!(st.snapshot().writer_active);
        assert!(st.snapshot().upgrade_active);
        assert!(!st.try_write_fast());

        assert!(st.release_writer());
        assert!(!st.snapshot().writer_active);
        assert_eq!(st.reader_count(), 1);
        assert!(!st.try_write_fast(), "可升级读槽位仍占着");

        assert!(st.release_upgradable_read());
        assert_eq!(st.reader_count(), 0);
        assert!(!st.snapshot().upgrade_active);
        assert!(st.try_write_fast(), "state: {st:?}");
    }

    /// 测试可升级读者在还有其他读者时不能升级。
    /// - 手段：一个可升级读者加一个普通读者（读者计数为 2）。
    /// - 判断：`try_upgrade_to_write` 失败，且状态字保持不变；普通读者释放后升级成功，
    ///   且升级仍保留读者计数与可升级标志。
    #[test]
    fn upgrade_to_write_should_fail_when_other_readers_exist() {
        let st = new_state();
        assert!(st.try_upgradable_read_fast());
        assert!(st.try_read_fast());
        assert_eq!(st.reader_count(), 2);

        assert!(!st.try_upgrade_to_write());
        assert_eq!(st.reader_count(), 2);
        assert!(!st.snapshot().writer_active);

        assert!(st.release_reader());
        assert!(st.try_upgrade_to_write());
        assert_eq!(st.reader_count(), 1);
        assert!(st.snapshot().writer_active);
    }

    /// 测试两条写者降级路径的计数迁移。
    /// - 手段：先取写者，再分别降级为普通读者与可升级读者。
    /// - 判断：降级为读者后读者计数为 1 且写者已释放；降级为可升级读者后
    ///   `UPGRADE_ACTIVE` 置位且读者计数为 1；各自释放后回到空闲。
    #[test]
    fn downgrade_write_should_restore_counts() {
        let st = new_state();

        assert!(st.try_write_fast());
        assert!(st.downgrade_write_to_read());
        assert_eq!(st.reader_count(), 1);
        assert!(!st.snapshot().writer_active);
        assert!(st.release_reader());
        assert_eq!(st.reader_count(), 0);

        assert!(st.try_write_fast());
        assert!(st.downgrade_write_to_upgradable());
        assert_eq!(st.reader_count(), 1);
        assert!(st.snapshot().upgrade_active);
        assert!(!st.snapshot().writer_active);
        assert!(st.downgrade_upgradable_to_read());
        assert_eq!(st.reader_count(), 1);
        assert!(!st.snapshot().upgrade_active);
        assert!(st.release_reader());
        assert_eq!(st.reader_count(), 0);
    }

    /// 测试读者计数上界与"仅剩一个读者"的边界条件。
    /// - 手段：在可升级读者持有期间再插入一个普通读者，构造读者计数为 2 的场景。
    /// - 判断：`try_upgrade_to_write` 因计数不为 1 而失败；释放普通读者后成功，
    ///   且升级前后读者计数分别为 2 与 1（槽位由可升级读守卫保留）。
    #[test]
    fn upgrade_should_require_exactly_one_reader() {
        let st = new_state();
        assert!(st.try_upgradable_read_fast());
        assert!(st.try_read_fast());
        assert_eq!(st.reader_count(), 2);
        assert!(!st.try_upgrade_to_write());

        assert!(st.release_reader());
        assert_eq!(st.reader_count(), 1);
        assert!(st.try_upgrade_to_write());
        assert_eq!(st.reader_count(), 1);

        // 用于确认 Debug 输出不会 panic。
        let _ = std::format!("{st:?}");
        fn assert_debug<T: Debug>(_: &T) {}
        assert_debug(&st);
    }
}
