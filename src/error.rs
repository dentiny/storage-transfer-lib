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

#[derive(Debug)]
pub enum ProducerError {
    InvalidRoot,
    UnsupportedEntry(String),
    Listing(opendal::Error),
    ConsumerClosed,
}

impl fmt::Display for ProducerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRoot => write!(f, "source operator must be rooted at the bucket root (/)"),
            Self::UnsupportedEntry(path) => write!(f, "unknown entry type: {path}"),
            Self::Listing(error) => write!(f, "bucket listing failed: {error}"),
            Self::ConsumerClosed => {
                write!(f, "object queue receiver closed before listing completed")
            }
        }
    }
}

impl std::error::Error for ProducerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Listing(error) => Some(error),
            _ => None,
        }
    }
}

impl From<opendal::Error> for ProducerError {
    fn from(error: opendal::Error) -> Self {
        Self::Listing(error)
    }
}
