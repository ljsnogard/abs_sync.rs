//! Fetch and apply operations to the current value, returning the previous
//! value.
use core::sync::atomic::Ordering;

use crate::TrAtomicCell;

/// Bitwise "and" with the current value.
pub trait And {
    /// The underlying primitive value type
    type Value;

    /// Bitwise "and" with the current value.
    ///
    /// Performs a bitwise "and" operation on the current value and the argument
    /// `val`, and sets the new value to the result.
    ///
    /// Returns the previous value.
    fn fetch_and(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// Bitwise "nand" with the current value.
pub trait Nand {
    /// The underlying primitive value type
    type Value;

    /// Bitwise "nand" with the current value.
    ///
    /// Performs a bitwise "nand" operation on the current value and the
    /// argument `val`, and sets the new value to the result.
    ///
    /// Returns the previous value.
    fn fetch_nand(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// Bitwise "or" with the current value.
pub trait Or {
    /// The underlying primitive value type
    type Value;

    /// Bitwise "or" with the current value.
    ///
    /// Performs a bitwise "or" operation on the current value and the argument
    /// `val`, and sets the new value to the result.
    ///
    /// Returns the previous value.
    fn fetch_or(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// Bitwise "xor" with the current value.
pub trait Xor {
    /// The underlying primitive value type
    type Value;

    /// Bitwise "xor" with the current value.
    ///
    /// Performs a bitwise "xor" operation on the current value and the argument
    /// `val`, and sets the new value to the result.
    ///
    /// Returns the previous value.
    fn fetch_xor(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// Adds to the current value, returning the previous value.
pub trait Add {
    /// The underlying primitive value type
    type Value;

    /// Adds to the current value, returning the previous value.
    ///
    /// This operation wraps around on overflow.
    fn fetch_add(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// Subtracts from the current value, returning the previous value.
pub trait Sub {
    /// The underlying primitive value type
    type Value;

    /// Subtracts from the current value, returning the previous value.
    ///
    /// This operation wraps around on overflow.
    fn fetch_sub(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// Fetches the value, and applies a function to it that returns an optional new
/// value.
pub trait Update {
    /// The underlying primitive value type
    type Value;

    /// Fetches the value, and applies a function to it that returns an optional
    /// new value.
    ///
    /// Returns a `Result` of `Ok(previous_value)` if the function returned
    /// `Some(_)`, else `Err(previous_value)`.
    fn fetch_update<F>(
        &self,
        fetch_order: Ordering,
        set_order: Ordering,
        f: F,
    ) -> Result<Self::Value, Self::Value>
    where
        F: FnMut(Self::Value) -> Option<Self::Value>;
}

/// Maximum with the current value.
pub trait Max {
    /// The underlying primitive value type
    type Value;

    /// Maximum with the current value.
    ///
    /// Finds the maximum of the current value and the argument `val`, and sets
    /// the new value to the result.
    ///
    /// Returns the previous value.
    fn fetch_max(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// Minimum with the current value.
pub trait Min {
    /// The underlying primitive value type
    type Value;

    /// Minimum with the current value.
    ///
    /// Finds the minimum of the current value and the argument `val`, and sets
    /// the new value to the result.
    ///
    /// Returns the previous value.
    fn fetch_min(
        &self,
        val: Self::Value,
        order: Ordering,
    ) -> Self::Value;
}

/// The trait for types implementing atomic bitwise operations
pub trait Bitwise:
    TrAtomicCell
    + And<Value = <Self as TrAtomicCell>::Value>
    + Nand<Value = <Self as TrAtomicCell>::Value>
    + Or<Value = <Self as TrAtomicCell>::Value>
    + Xor<Value = <Self as TrAtomicCell>::Value>
{}

/// The trait for types implementing atomic numeric operations
pub trait NumOps:
    TrAtomicCell
    + Add<Value = <Self as TrAtomicCell>::Value>
    + Sub<Value = <Self as TrAtomicCell>::Value>
    + Update<Value = <Self as TrAtomicCell>::Value>
    + Max<Value = <Self as TrAtomicCell>::Value>
    + Min<Value = <Self as TrAtomicCell>::Value>
{}
