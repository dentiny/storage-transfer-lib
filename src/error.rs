use std::fmt;

use tokio::sync::Semaphore;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    pub(crate) field: &'static str,
    pub(crate) value: usize,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} must be in 1..={}, got {}",
            self.field,
            Semaphore::MAX_PERMITS,
            self.value
        )
    }
}

impl std::error::Error for ConfigError {}
