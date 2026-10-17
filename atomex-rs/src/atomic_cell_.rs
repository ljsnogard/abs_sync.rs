use core::sync::atomic::*;
use crate::fetch;

pub trait TrAtomicCell {
    /// The underlying primitive value type
    type Value: Copy;

    fn new(val: Self::Value) -> Self;

    fn into_inner(self) -> Self::Value;

    /// Loads a value from the cell.
    ///
    /// `load` takes an [`Ordering`] argument which describes the memory ordering
    /// of this operation. Possible values are [`SeqCst`], [`Acquire`] and [`Relaxed`].
    ///
    /// # Panics
    ///
    /// Panics if `order` is [`Release`] or [`AcqRel`].
    fn load(
        &self,
        order: Ordering,
    ) -> Self::Value;

    /// Stores a value into the cell.
    ///
    /// `store` takes an [`Ordering`] argument which describes the memory ordering
    /// of this operation. Possible values are [`SeqCst`], [`Release`] and [`Relaxed`].
    ///
    /// # Panics
    ///
    /// Panics if `order` is [`Acquire`] or [`AcqRel`].
    fn store(
        &self,
        val: Self::Value,
        order: Ordering,
    );

    /// Stores a value into the cell, returning the previous value.
    ///
    /// `swap` takes an [`Ordering`] argument which describes the memory ordering
    /// of this operation. All ordering modes are possible. Note that using
    /// [`Acquire`] makes the store part of this operation [`Relaxed`], and
    /// using [`Release`] makes the load part [`Relaxed`].
    ///
    /// **Note:** This method is only available on platforms that support atomic
    /// operations on pointers.
    fn swap(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;

    /// Stores a value into the atomic type if the current value is the same as
    /// the `current` value.
    ///
    /// The return value is a result indicating whether the new value was
    /// written and containing the previous value. On success this value is
    /// guaranteed to be equal to `current`.
    fn compare_exchange(
        &self,
        current: Self::Value,
        desired: Self::Value,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Self::Value, Self::Value>;

    /// Stores a value into the atomic type if the current value is the same as
    /// the current value.
    ///
    /// Unlike `compare_exchange`, this function is allowed to spuriously fail
    /// even when the comparison succeeds, which can result in more
    /// efficient code on some platforms. The return value is a result
    /// indicating whether the new value was written and containing the previous
    /// value.
    fn compare_exchange_weak(
        &self,
        current: Self::Value,
        desired: Self::Value,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Self::Value, Self::Value>;
}

pub trait TrAtomicData {
    type AtomicCell: TrAtomicCell<Value = Self>;
}

/// An helper trait to define spinlock ordering used in atomic operation
pub trait TrCmpxchOrderings {
    const SUCC_ORDERING: Ordering;
    const FAIL_ORDERING: Ordering;
    const LOAD_ORDERING: Ordering;
}

/// Provide the most strict orderings with cost of higher overhead.
///
/// If you don't know which one to use, this is the best choice.
#[derive(Clone, Copy, Debug, Default)]
pub struct StrictOrderings;

impl TrCmpxchOrderings for StrictOrderings {
    const SUCC_ORDERING: Ordering = Ordering::SeqCst;
    const FAIL_ORDERING: Ordering = Ordering::SeqCst;
    const LOAD_ORDERING: Ordering = Ordering::SeqCst;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocksOrderings;

impl TrCmpxchOrderings for LocksOrderings {
    const SUCC_ORDERING: Ordering = Ordering::Acquire;
    const FAIL_ORDERING: Ordering = Ordering::Relaxed;
    const LOAD_ORDERING: Ordering = Ordering::Acquire;
}

#[cfg(target_has_atomic = "8")]
impl TrAtomicData for i8 {
    type AtomicCell = AtomicI8;
}

#[cfg(target_has_atomic = "8")]
impl TrAtomicData for u8 {
    type AtomicCell = AtomicU8;
}

#[cfg(target_has_atomic = "16")]
impl TrAtomicData for i16 {
    type AtomicCell = AtomicI16;
}

#[cfg(target_has_atomic = "16")]
impl TrAtomicData for u16 {
    type AtomicCell = AtomicU16;
}

#[cfg(target_has_atomic = "32")]
impl TrAtomicData for i32 {
    type AtomicCell = AtomicI32;
}

#[cfg(target_has_atomic = "32")]
impl TrAtomicData for u32 {
    type AtomicCell = AtomicU32;
}

#[cfg(target_has_atomic = "64")]
impl TrAtomicData for i64 {
    type AtomicCell = AtomicI64;
}

#[cfg(target_has_atomic = "64")]
impl TrAtomicData for u64 {
    type AtomicCell = AtomicU64;
}

#[cfg(all(target_has_atomic = "128", feature = "support_u128_i128_atomics"))]
impl TrAtomicData for i128 {
    type AtomicCell = AtomicI128;
}

#[cfg(all(target_has_atomic = "128", feature = "support_u128_i128_atomics"))]
impl TrAtomicData for u128 {
    type AtomicCell = AtomicU128;
}

impl TrAtomicData for isize {
    type AtomicCell = AtomicIsize;
}

impl TrAtomicData for usize {
    type AtomicCell = AtomicUsize;
}

impl TrAtomicData for bool {
    type AtomicCell = AtomicBool;
}

impl<T> TrAtomicData for *mut T {
    type AtomicCell = AtomicPtr<T>;
}

macro_rules! impl_atomic {
    ($atomic:ident : $primitive:ty ; $( $traits:tt ),*) => {
        impl_atomic!(__impl atomic $atomic : $primitive);

        $(
            impl_atomic!(__impl $traits $atomic : $primitive);
        )*

    };
    ($atomic:ident < $param:ident >) => {
        impl<$param> TrAtomicCell for $atomic <$param> {
            type Value = *mut $param;

            impl_atomic!(__impl atomic_methods $atomic);
        }
    };

    (__impl atomic $atomic:ident : $primitive:ty) => {
        impl TrAtomicCell for $atomic {
            type Value = $primitive;

            impl_atomic!(__impl atomic_methods $atomic);
        }
    };

    (__impl atomic_methods $atomic:ident) => {
        #[inline(always)]
        fn new(v: Self::Value) -> Self {
            Self::new(v)
        }

        #[inline(always)]
        fn into_inner(self) -> Self::Value {
            Self::into_inner(self)
        }

        #[inline(always)]
        fn load(&self, order: Ordering) -> Self::Value {
            Self::load(self, order)
        }

        #[inline(always)]
        fn store(&self, val: Self::Value, order: Ordering) {
            Self::store(self, val, order)
        }

        #[inline(always)]
        fn swap(&self, val: Self::Value, order: Ordering) -> Self::Value {
            Self::swap(self, val, order)
        }

        #[inline(always)]
        fn compare_exchange(
            &self,
            current: Self::Value,
            desired: Self::Value,
            success: Ordering,
            failure: Ordering,
        ) -> Result<Self::Value, Self::Value> {
            Self::compare_exchange(self, current, desired, success, failure)
        }

        #[inline(always)]
        fn compare_exchange_weak(
            &self,
            current: Self::Value,
            desired: Self::Value,
            success: Ordering,
            failure: Ordering,
        ) -> Result<Self::Value, Self::Value> {
            Self::compare_exchange_weak(self, current, desired, success, failure)
        }
    };

    (__impl bitwise $atomic:ident : $primitive:ty) => {
        impl fetch::Bitwise for $atomic {}

        impl $crate::fetch::And for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_and(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_and(self, val, order)
            }
        }

        impl $crate::fetch::Nand for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_nand(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_nand(self, val, order)
            }
        }

        impl $crate::fetch::Or for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_or(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_or(self, val, order)
            }
        }

        impl $crate::fetch::Xor for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_xor(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_xor(self, val, order)
            }
        }
    };

    (__impl numops $atomic:ident : $primitive:ty) => {
        impl fetch::NumOps for $atomic {}

        impl $crate::fetch::Add for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_add(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_add(self, val, order)
            }
        }

        impl $crate::fetch::Sub for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_sub(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_sub(self, val, order)
            }
        }

        impl $crate::fetch::Update for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_update<F>(
                &self,
                fetch_order: Ordering,
                set_order: Ordering,
                f: F,
            ) -> Result<Self::Value, Self::Value>
            where
                F: FnMut(Self::Value) -> Option<Self::Value> {
                Self::try_update(self, fetch_order, set_order, f)
            }
        }

        impl $crate::fetch::Max for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_max(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_max(self, val, order)
            }
        }

        impl $crate::fetch::Min for $atomic {
            type Value = $primitive;

            #[inline(always)]
            fn fetch_min(&self, val: Self::Value, order: Ordering) -> Self::Value {
                Self::fetch_min(self, val, order)
            }
        }
    };
}

impl_atomic!(AtomicBool: bool; bitwise);
impl_atomic!(AtomicIsize: isize; bitwise, numops);
impl_atomic!(AtomicUsize: usize; bitwise, numops);
impl_atomic!(AtomicPtr<T>);

#[cfg(target_has_atomic = "8")]
impl_atomic!(AtomicI8: i8; bitwise, numops);

#[cfg(target_has_atomic = "16")]
impl_atomic!(AtomicI16: i16; bitwise, numops);

#[cfg(target_has_atomic = "32")]
impl_atomic!(AtomicI32: i32; bitwise, numops);

#[cfg(target_has_atomic = "64")]
impl_atomic!(AtomicI64: i64; bitwise, numops);

#[cfg(all(target_has_atomic = "128", feature = "support_u128_i128_atomics"))]
impl_atomic!(AtomicI128: i128; bitwise, numops);

#[cfg(target_has_atomic = "8")]
impl_atomic!(AtomicU8: u8; bitwise, numops);

#[cfg(target_has_atomic = "16")]
impl_atomic!(AtomicU16: u16; bitwise, numops);

#[cfg(target_has_atomic = "32")]
impl_atomic!(AtomicU32: u32; bitwise, numops);

#[cfg(target_has_atomic = "64")]
impl_atomic!(AtomicU64: u64; bitwise, numops);

#[cfg(all(target_has_atomic = "128", feature = "support_u128_i128_atomics"))]
impl_atomic!(AtomicU128: u128; bitwise, numops);
