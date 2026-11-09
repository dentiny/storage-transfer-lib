use std::fmt;

use tokio::sync::Semaphore;

#[derive(Debug)]
pub enum Error {
    InvalidLimit { field: &'static str, value: usize },
    InvalidRoot,
    InvalidConfig(&'static str),
    UnsupportedEntry(String),
    Storage(opendal::Error),
    ConsumerClosed,
    Unsupported(String),
    SourceChanged(String),
    Cancelled,
    VerificationFailed(String),
    Report(std::io::Error),
    Task(tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit { field, value } => {
                write!(
                    f,
                    "{field} must be in 1..={}, got {value}",
                    Semaphore::MAX_PERMITS
                )
            }
            Self::InvalidConfig(field) => write!(f, "invalid transfer configuration: {field}"),
            Self::InvalidRoot => write!(f, "operators must use canonical directory roots"),
            Self::UnsupportedEntry(path) => write!(f, "unknown entry type: {path}"),
            Self::Storage(error) => write!(f, "storage operation failed: {error}"),
            Self::Unsupported(reason) => write!(f, "unsupported copy: {reason}"),
            Self::SourceChanged(path) => write!(
                f,
                "source object changed or returned an invalid range: {path}"
            ),
            Self::Cancelled => write!(f, "transfer cancelled"),
            Self::VerificationFailed(path) => write!(f, "destination verification failed: {path}"),
            Self::Report(error) => write!(f, "transfer report failed: {error}"),
            Self::Task(error) => write!(f, "transfer task failed: {error}"),
            Self::ConsumerClosed => {
                write!(f, "object queue receiver closed before listing completed")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::Report(error) => Some(error),
            _ => None,
        }
    }
}

impl From<opendal::Error> for Error {
    fn from(error: opendal::Error) -> Self {
        Self::Storage(error)
    }
}
