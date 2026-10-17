use core::{
    borrow::BorrowMut,
    fmt,
    marker::PhantomData,
    ptr::{self, NonNull},
    sync::atomic::AtomicPtr,
};

use crate::{CmpxchResult, StrictOrderings, TrAtomicFlags, TrCmpxchOrderings};

/// A wrapper around the [`AtomicPtr`](core::sync::atomic::AtomicPtr).
#[derive(Debug)]
pub struct AtomexPtr<T, B = AtomicPtr<T>, O = StrictOrderings>(
    B,
    PhantomData<AtomicPtr<T>>,
    PhantomData<O>,
)
where
    B: BorrowMut<AtomicPtr<T>>,
    O: TrCmpxchOrderings;

impl<T, B, O> AtomexPtr<T, B, O>
where
    B: BorrowMut<AtomicPtr<T>>,
    O: TrCmpxchOrderings,
{
    pub const fn new(a: B) -> Self {
        AtomexPtr(a, PhantomData, PhantomData)
    }

    #[inline(always)]
    pub fn pointer(&self) -> *mut T {
        TrAtomicFlags::value(self)
    }

    #[inline(always)]
    pub fn load(&self) -> Option<NonNull<T>> {
        NonNull::new(self.pointer())
    }

    #[inline(always)]
    pub fn compare_exchange_weak(
        &self,
        current: *mut T,
        desired: *mut T,
    ) -> Result<*mut T, *mut T> {
        self.0
            .borrow()
            .compare_exchange_weak(current, desired, O::SUCC_ORDERING, O::FAIL_ORDERING)
    }

    #[inline(always)]
    pub fn try_once_compare_exchange_weak(
        &self,
        current: *mut T,
        expect: impl FnMut(*mut T) -> bool,
        desire: impl FnMut(*mut T) -> *mut T,
    ) -> CmpxchResult<*mut T> {
        TrAtomicFlags::try_once_compare_exchange_weak(self, current, expect, desire)
    }

    #[inline(always)]
    pub fn try_spin_compare_exchange_weak(
        &self,
        expect: impl FnMut(*mut T) -> bool,
        desire: impl FnMut(*mut T) -> *mut T,
    ) -> CmpxchResult<*mut T> {
        TrAtomicFlags::try_spin_compare_exchange_weak(self, expect, desire)
    }

    /// Try to update the atomic pointer from non-null to null.
    ///
    /// Returns value indicates if the reset is successful and contains the
    /// previous stored value.
    pub fn try_reset(&self) -> Result<NonNull<T>, *mut T> {
        fn expect_not_null<X>(p: *mut X) -> bool {
            !p.is_null()
        }
        fn desire_ptr_null<X>(_: *mut X) -> *mut X {
            ptr::null_mut()
        }
        fn op_ptr_to_non_null<X>(p: *mut X) -> NonNull<X> {
            unsafe { NonNull::new_unchecked(p) }
        }
        let r: Result<_, _> = self
            .try_spin_compare_exchange_weak(expect_not_null, desire_ptr_null)
            .into();
        r.map(op_ptr_to_non_null)
    }

    /// Try to update the atomic pointer from non-null to null, after checking
    /// the the equality between the stored pointer and the argument pointer.
    pub fn try_spin_compare_and_reset(&self, p: NonNull<T>) -> Result<NonNull<T>, *mut T> {
        let expect = |x: *mut T| ptr::eq(x, p.as_ptr());
        let desire = |_| ptr::null_mut();
        let op_ptr_to_non_null = |x| unsafe { NonNull::new_unchecked(x) };
        let r: Result<_, _> = self
            .try_spin_compare_exchange_weak(expect, desire)
            .into();
        r.map(op_ptr_to_non_null)
    }

    /// Try to update the atomic pointer from null to non-null.
    ///
    /// Returns value indicates if the init is successful and contains the
    /// previous stored value.
    pub fn try_spin_init(&self, init: NonNull<T>) -> Result<*mut T, NonNull<T>> {
        let p = init.as_ptr();
        let expect = |x: *mut T| x.is_null();
        let desire = |_| p;
        let r: Result<_, _> = self
            .try_spin_compare_exchange_weak(expect, desire)
            .into();
        r.map_err(|x| unsafe { NonNull::new_unchecked(x) })
    }

    pub fn store(&self, p: *mut T) {
        self.0.borrow().store(p, O::SUCC_ORDERING)
    }
}

impl<'a, T> From<&'a mut AtomicPtr<T>> for AtomexPtr<T, &'a mut AtomicPtr<T>, StrictOrderings> {
    fn from(value: &'a mut AtomicPtr<T>) -> Self {
        AtomexPtr::new(value)
    }
}

impl<T, B, O> AsRef<AtomicPtr<T>> for AtomexPtr<T, B, O>
where
    B: BorrowMut<AtomicPtr<T>>,
    O: TrCmpxchOrderings,
{
    fn as_ref(&self) -> &AtomicPtr<T> {
        self.0.borrow()
    }
}

pub type AtomexPtrMut<'a, T, O = StrictOrderings> = AtomexPtr<T, &'a mut AtomicPtr<T>, O>;
pub type AtomexPtrOwned<T, O = StrictOrderings> = AtomexPtr<T, AtomicPtr<T>, O>;

impl<T, B, O> TrAtomicFlags<*mut T, O> for AtomexPtr<T, B, O>
where
    B: BorrowMut<AtomicPtr<T>>,
    O: TrCmpxchOrderings,
{}

impl<T, B, O> fmt::Display for AtomexPtr<T, B, O>
where
    B: BorrowMut<AtomicPtr<T>>,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Option::Some(p) = self.load() {
            let x = p.as_ptr();
            write!(f, "[{x:p}]")
        } else {
            write!(f, "[null]")
        }
    }
}
