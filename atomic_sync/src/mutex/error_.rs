use core::fmt;

#[derive(Clone, Copy, Debug)]
pub enum MutexError {
    Cancelled,
    Poisoned,
    Retry,
}

impl fmt::Display for MutexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MutexError::Cancelled => write!(f, "SpinningMutexError::Cancelled"),
            MutexError::Poisoned => write!(f, "SpinningMutexError::Poisoned"),
            MutexError::Retry => write!(f, "SpinningMutexError::Retry"),
        }
    }
}

impl core::error::Error for MutexError
{}
