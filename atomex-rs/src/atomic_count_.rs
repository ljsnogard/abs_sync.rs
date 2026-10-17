use core::{
    borrow::BorrowMut,
    fmt::{self, Debug},
    marker::PhantomData,
    sync::atomic::Ordering
};
use crate::{
    fetch::{self, Add, Sub},
    TrAtomicCell, TrAtomicData,
};

pub struct AtomicCount<V, B = <V as TrAtomicData>::AtomicCell>(
    B,
    PhantomData<<V as TrAtomicData>::AtomicCell>)
where
    V: TrAtomicData + funty::Integral,
    <V as TrAtomicData>::AtomicCell: TrAtomicCell<Value = V>
        + fetch::Add<Value = V>
        + fetch::Sub<Value = V>,
    B: BorrowMut<<V as TrAtomicData>::AtomicCell>;

impl<V, B> AtomicCount<V, B>
where
    V: TrAtomicData + funty::Integral,
    <V as TrAtomicData>::AtomicCell: TrAtomicCell<Value = V>
        + fetch::Add<Value = V>
        + fetch::Sub<Value = V>,
    B: BorrowMut<<V as TrAtomicData>::AtomicCell>,
{
    /// Create an instance by moving or borrowing an `TrAtomicCell`
    /// 
    /// # Example
    /// 
    /// ```
    /// use core::sync::atomic::*;
    /// use atomex::AtomicCount;
    ///
    /// let atm = AtomicUsize::new(0usize);
    /// let cnt = AtomicCount::<usize, AtomicUsize>::new(atm);
    ///
    /// let mut atm = cnt.into_inner();
    /// let cnt = AtomicCount::<usize, &mut AtomicUsize>::new(&mut atm);
    ///
    /// assert_eq!(cnt.inc(), 0usize);
    /// assert_eq!(cnt.dec(), 1usize);
    /// assert_eq!(cnt.val(), 0usize);
    /// ```
    pub const fn new(cell: B) -> Self {
        AtomicCount(cell, PhantomData)
    }

    #[inline(always)]
    pub fn inc(&self) -> V {
        self.add(V::ONE)
    }

    pub fn add(&self, val: V) -> V {
        self.0.borrow().fetch_add(val, Ordering::Acquire)
    }

    #[inline(always)]
    pub fn dec(&self) -> V {
        self.sub(V::ONE)
    }

    pub fn sub(&self, val: V) -> V {
        self.0.borrow().fetch_sub(val, Ordering::Release)
    }

    pub fn val(&self) -> V {
        self.0.borrow().load(Ordering::Relaxed)
    }
}

impl<V> AtomicCount<V, <V as TrAtomicData>::AtomicCell>
where
    V: TrAtomicData + funty::Integral,
    <V as TrAtomicData>::AtomicCell: TrAtomicCell<Value = V>
        + fetch::Add<Value = V>
        + fetch::Sub<Value = V>,
{
    pub fn into_inner(self) -> <V as TrAtomicData>::AtomicCell {
        self.0
    }
}

impl<'a, V> From<&'a mut <V as TrAtomicData>::AtomicCell>
for AtomicCount<V, &'a mut <V as TrAtomicData>::AtomicCell>
where
    V: TrAtomicData + funty::Integral,
    <V as TrAtomicData>::AtomicCell: TrAtomicCell<Value = V>
        + fetch::Add<Value = V>
        + fetch::Sub<Value = V> + Debug,
{
    fn from(value: &'a mut <V as TrAtomicData>::AtomicCell) -> Self {
        Self::new(value)
    }
}

impl<V, B> Debug for AtomicCount<V, B>
where
    V: TrAtomicData + funty::Integral,
    <V as TrAtomicData>::AtomicCell: TrAtomicCell<Value = V>
        + fetch::Add<Value = V>
        + fetch::Sub<Value = V> + Debug,
    B: BorrowMut<<V as TrAtomicData>::AtomicCell>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.borrow().fmt(f)
    }
}

impl<V> Default for AtomicCount<V>
where
    V: TrAtomicData + funty::Integral,
    <V as TrAtomicData>::AtomicCell: TrAtomicCell<Value = V>
        + fetch::Add<Value = V>
        + fetch::Sub<Value = V>
        + Default,
{
    fn default() -> Self {
        Self::new(V::AtomicCell::default())
    }
}

pub type AtomicCountOwned<V> = AtomicCount<V, <V as TrAtomicData>::AtomicCell>;
pub type AtomicCountMut<'a, V> =
    AtomicCount<V, &'a mut <V as TrAtomicData>::AtomicCell>;