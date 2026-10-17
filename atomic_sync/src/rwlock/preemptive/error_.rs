use core::fmt;

#[derive(Clone, Copy, Debug)]
pub enum RwLockError {
    Cancelled,
    Poisoned,
    Retry,
}

impl fmt::Display for RwLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RwLockError::Cancelled => write!(f, "SpinningRwLockError::Cancelled"),
            RwLockError::Poisoned => write!(f, "SpinningRwLockError::Poisoned"),
            RwLockError::Retry => write!(f, "SpinningRwLockError::Retry"),
        }
    }
}

impl core::error::Error for RwLockError
{}
