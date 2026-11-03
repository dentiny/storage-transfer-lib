use std::fmt;
use std::num::NonZeroUsize;

use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferLimits {
    max_queued_objects: NonZeroUsize,
    max_ongoing_http_requests: NonZeroUsize,
}

impl TransferLimits {
    pub fn new(
        max_queued_objects: usize,
        max_ongoing_http_requests: usize,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            max_queued_objects: validate_limit("max_queued_objects", max_queued_objects)?,
            max_ongoing_http_requests: validate_limit(
                "max_ongoing_http_requests",
                max_ongoing_http_requests,
            )?,
        })
    }

    pub fn max_queued_objects(self) -> usize {
        self.max_queued_objects.get()
    }

    pub fn max_ongoing_http_requests(self) -> usize {
        self.max_ongoing_http_requests.get()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    field: &'static str,
    value: usize,
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

pub(crate) fn validate_limit(
    field: &'static str,
    value: usize,
) -> Result<NonZeroUsize, ConfigError> {
    NonZeroUsize::new(value)
        .filter(|n| n.get() <= Semaphore::MAX_PERMITS)
        .ok_or(ConfigError { field, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_limits_without_panicking() {
        for invalid in [0, Semaphore::MAX_PERMITS + 1, usize::MAX] {
            assert!(TransferLimits::new(invalid, 1).is_err());
            assert!(TransferLimits::new(1, invalid).is_err());
        }
        let limits = TransferLimits::new(1024, 1).unwrap();
        assert_eq!(limits.max_queued_objects(), 1024);
        assert_eq!(limits.max_ongoing_http_requests(), 1);
        assert!(TransferLimits::new(Semaphore::MAX_PERMITS, Semaphore::MAX_PERMITS).is_ok());
    }
}
