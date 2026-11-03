use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::{Error, Result};

const DEFAULT_MAX_QUEUED_OBJECTS: usize = 1024;
const DEFAULT_MAX_ONGOING_HTTP_REQUESTS: usize = 32;
pub(crate) const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(crate) const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_MAX_RETRIES: usize = 2;
pub(crate) const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(100);

const MAX_LIST_PAGE_SIZE: usize = 1000;
const MAX_COPY_WORKERS: usize = 32;
pub(crate) const COPY_BUFFER_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const MAX_COPY_PARTS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferLimits {
    max_queued_objects: NonZeroUsize,
    max_ongoing_http_requests: NonZeroUsize,
}

impl TransferLimits {
    pub fn new(max_queued_objects: usize, max_ongoing_http_requests: usize) -> Result<Self> {
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

    pub fn list_page_size(self) -> usize {
        self.max_queued_objects().min(MAX_LIST_PAGE_SIZE)
    }

    pub(crate) fn worker_count(self) -> usize {
        self.max_ongoing_http_requests().min(MAX_COPY_WORKERS)
    }

    pub fn max_ongoing_http_requests(self) -> usize {
        self.max_ongoing_http_requests.get()
    }
}

pub(crate) fn validate_limit(field: &'static str, value: usize) -> Result<NonZeroUsize> {
    NonZeroUsize::new(value)
        .filter(|n| n.get() <= Semaphore::MAX_PERMITS)
        .ok_or(Error::InvalidLimit { field, value })
}

impl Default for TransferLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_QUEUED_OBJECTS,
            DEFAULT_MAX_ONGOING_HTTP_REQUESTS,
        )
        .expect("default limits are valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferConfig {
    pub limits: TransferLimits,
    pub operation_timeout: Duration,
    pub io_timeout: Duration,
    // Additional attempts after the initial attempt; zero disables retries.
    pub max_retries: usize,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            limits: TransferLimits::default(),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            io_timeout: DEFAULT_OPERATION_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_delay: DEFAULT_RETRY_DELAY,
            max_retry_delay: DEFAULT_MAX_RETRY_DELAY,
        }
    }
}
