use core::{
    borrow::BorrowMut,
    convert::AsRef,
    fmt::{self, Debug},
    marker::PhantomData,
};

use crate::{
    CmpxchResult, StrictOrderings, 
    TrAtomicCell, TrAtomicData, TrCmpxchOrderings,
};

pub trait TrAtomicFlags<T, O = StrictOrderings>
where
    Self: AsRef<<T as TrAtomicData>::AtomicCell>,
    T: TrAtomicData + Copy,
    <T as TrAtomicData>::AtomicCell: TrAtomicCell<Value = T>,
    O: TrCmpxchOrderings,
{
    fn value(&self) -> T {
        self.as_ref().load(O::LOAD_ORDERING)
    }

    fn try_spin_compare_exchange_weak<FnExpect, FnDesire>(
        &self,
        mut expect: FnExpect,
        mut desire: FnDesire,
    ) -> CmpxchResult<T>
    where
        FnExpect: FnMut(T) -> bool,
        FnDesire: FnMut(T) -> T,
    {
        let atomic = self.as_ref();
        let mut current = atomic.load(O::LOAD_ORDERING);
        loop {
            let r = self.try_once_compare_exchange_weak(
                current,
                &mut expect,
                &mut desire,
            );
            if let CmpxchResult::Fail(x) = r {
                current = x;
            } else {
                break r;
            }
        }
    }

    fn try_once_compare_exchange_weak<FnExpect, FnDesire>(
        &self,
        current: T,
        mut expect: FnExpect,
        mut desire: FnDesire,
    ) -> CmpxchResult<T>
    where
        FnExpect: FnMut(T) -> bool,
        FnDesire: FnMut(T) -> T,
    {
        let atomic = self.as_ref();
        if !expect(current) {
            return CmpxchResult::Unexpected(current);
        };
        let desired = desire(current);
        match atomic.compare_exchange_weak(
            current,
            desired,
            O::SUCC_ORDERING,
            O::FAIL_ORDERING,
        ) {
            Result::Ok(x) => CmpxchResult::Succ(x),
            Result::Err(x) => CmpxchResult::Fail(x),
        }
    }
}

pub struct AtomicFlags<
    T,
    B = <T as TrAtomicData>::AtomicCell,
    O = StrictOrderings,
>(B, PhantomData<T>, PhantomData<O>)
where
    T: TrAtomicData + Copy,
    <T as TrAtomicData>::AtomicCell: TrAtomicCell<Value = T>,
    B: BorrowMut<<T as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings;

impl<T, B, O> AtomicFlags<T, B, O>
where
    T: TrAtomicData + Copy,
    <T as TrAtomicData>::AtomicCell: TrAtomicCell<Value = T>,
    B: BorrowMut<<T as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    pub const fn new(cell: B) -> Self {
        AtomicFlags(cell, PhantomData, PhantomData)
    }

    #[inline(always)]
    pub fn value(&self) -> T {
        TrAtomicFlags::<T, O>::value(self)
    }

    #[inline(always)]
    pub fn try_once_compare_exchange_weak(
        &self,
        current: T,
        expect: impl FnMut(T) -> bool,
        desire: impl FnMut(T) -> T,
    ) -> CmpxchResult<T> {
        TrAtomicFlags::try_once_compare_exchange_weak(
            self,
            current,
            expect,
            desire
        )
    }

    #[inline(always)]
    pub fn try_spin_compare_exchange_weak(
        &self,
        expect: impl FnMut(T) -> bool,
        desire: impl FnMut(T) -> T,
    ) -> CmpxchResult<T> {
        TrAtomicFlags::try_spin_compare_exchange_weak(self, expect, desire)
    }
}

impl<T, B, O> AsRef<<T as TrAtomicData>::AtomicCell>
for AtomicFlags<T, B, O>
where
    T: TrAtomicData + Copy,
    <T as TrAtomicData>::AtomicCell: TrAtomicCell<Value = T>,
    B: BorrowMut<<T as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn as_ref(&self) -> &<T as TrAtomicData>::AtomicCell {
        self.0.borrow()
    }
}

impl<T, B, O> TrAtomicFlags<T, O> for AtomicFlags<T, B, O>
where
    T: TrAtomicData + Copy,
    <T as TrAtomicData>::AtomicCell: TrAtomicCell<Value = T>,
    B: BorrowMut<<T as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{}

impl<T, B, O> Debug for AtomicFlags<T, B, O>
where
    T: TrAtomicData + Copy,
    <T as TrAtomicData>::AtomicCell: TrAtomicCell<Value = T> + Debug,
    B: BorrowMut<<T as TrAtomicData>::AtomicCell>,
    O: TrCmpxchOrderings,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.borrow().fmt(f)
    }
}
