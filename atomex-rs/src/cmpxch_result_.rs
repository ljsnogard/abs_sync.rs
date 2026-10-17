#[derive(Debug, Clone)]
pub enum CmpxchResult<T> {
    /// The compare_exchange successfully updated the value.
    Succ(T),

    /// The compare_exchange fails in contending.
    Fail(T),

    /// The compare_exchange is not performed because the value is unexpected.
    Unexpected(T),
}

impl<T> CmpxchResult<T> {
    pub fn into_inner(self) -> T {
        match self {
            CmpxchResult::Succ(t) => t,
            CmpxchResult::Fail(t) => t,
            CmpxchResult::Unexpected(t) => t,
        }
    }

    pub const fn is_succ(&self) -> bool {
        matches!(self, CmpxchResult::Succ(_))
    }

    pub const fn is_fail(&self) -> bool {
        matches!(self, CmpxchResult::Fail(_))
    }

    pub const fn is_unexpected(&self) -> bool {
        matches!(self, CmpxchResult::Unexpected(_))
    }

    pub fn succ(self) -> Option<T> {
        match self {
            CmpxchResult::Succ(t) => Option::Some(t),
            _ => Option::None,
        }
    }

    pub fn fail(self) -> Option<T> {
        match self {
            CmpxchResult::Fail(t) => Option::Some(t),
            _ => Option::None,
        }
    }

    pub fn unexpected(self) -> Option<T> {
        match self {
            CmpxchResult::Unexpected(t) => Option::Some(t),
            _ => Option::None,
        }
    }
}

impl<T> From<CmpxchResult<T>> for Result<T, T> {
    fn from(value: CmpxchResult<T>) -> Self {
        match value {
            CmpxchResult::Succ(t) => Result::Ok(t),
            CmpxchResult::Fail(t) => Result::Err(t),
            CmpxchResult::Unexpected(t) => Result::Err(t),
        }
    }
}
