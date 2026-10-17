use core::ops::{Deref, DerefMut};

/// An acquired guard that can access critical data by reference
pub trait TrAcqRefGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + Deref<Target = T>,
    T: 'a + ?Sized
{}

/// An acquired guard that can access critical data by mutable reference.
pub trait TrAcqMutGuard<'a, 'g, T>
where
    'a: 'g,
    Self: 'g + Sized + DerefMut<Target = T> + TrAcqRefGuard<'a, 'g, T>,
    T: 'a + ?Sized,
{}
