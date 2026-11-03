use std::fmt;

use tokio::sync::Semaphore;

#[derive(Debug)]
pub enum Error {
    InvalidLimit {
        field: &'static str,
        value: usize,
    },
    InvalidRoot,
    UnsupportedEntry(String),
    Storage(opendal::Error),
    ConsumerClosed,
    Unsupported(String),
    SourceChanged(String),
    Cancelled,
    VerificationFailed(String),
    CommitUnknown {
        path: String,
        close: opendal::Error,
        error: Box<Error>,
    },
    Report(std::io::Error),
    Secondary {
        original: Box<Error>,
        secondary: Box<Error>,
    },
    Task(tokio::task::JoinError),
    Cleanup {
        original: Box<Error>,
        cleanup: opendal::Error,
    },
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
            Self::InvalidRoot => write!(f, "operators must be rooted at the bucket root (/)"),
            Self::UnsupportedEntry(path) => write!(f, "unknown entry type: {path}"),
            Self::Storage(error) => write!(f, "storage operation failed: {error}"),
            Self::Unsupported(reason) => write!(f, "unsupported copy: {reason}"),
            Self::SourceChanged(path) => write!(
                f,
                "source object changed or returned an invalid range: {path}"
            ),
            Self::Cancelled => write!(f, "transfer cancelled"),
            Self::VerificationFailed(path) => write!(f, "destination verification failed: {path}"),
            Self::CommitUnknown { path, close, error } => write!(
                f,
                "write completion could not be confirmed for {path}: {close}; verification: {error}"
            ),
            Self::Report(error) => write!(f, "transfer report failed: {error}"),
            Self::Secondary {
                original,
                secondary,
            } => {
                write!(f, "{original}; additional failure: {secondary}")
            }
            Self::Task(error) => write!(f, "transfer task failed: {error}"),
            Self::Cleanup { original, cleanup } => {
                write!(f, "{original}; aborting the upload also failed: {cleanup}")
            }
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
            Self::CommitUnknown { error, .. } => Some(error.as_ref()),
            Self::Secondary { original, .. } => Some(original.as_ref()),
            Self::Cleanup { original, .. } => Some(original.as_ref()),
            _ => None,
        }
    }
}

impl From<opendal::Error> for Error {
    fn from(error: opendal::Error) -> Self {
        Self::Storage(error)
    }
}

impl Error {
    pub(crate) fn has_cleanup_failure(&self) -> bool {
        match self {
            Self::Cleanup { .. } => true,
            Self::Secondary {
                original,
                secondary,
            } => original.has_cleanup_failure() || secondary.has_cleanup_failure(),
            _ => false,
        }
    }
}

pub(crate) fn combine<T>(result: Result<T>, additional: Result<()>) -> Result<T> {
    match (result, additional) {
        (Err(original), Err(secondary)) => Err(Error::Secondary {
            original: Box::new(original),
            secondary: Box::new(secondary),
        }),
        (Ok(_), Err(error)) => Err(error),
        (result, Ok(())) => result,
    }
}
