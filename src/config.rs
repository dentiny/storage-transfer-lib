use std::num::NonZeroUsize;

use tokio::sync::Semaphore;

use crate::{Error, Result};

const MAX_LIST_PAGE_SIZE: usize = 1000;
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

    pub fn max_ongoing_http_requests(self) -> usize {
        self.max_ongoing_http_requests.get()
    }
}

pub(crate) fn validate_limit(field: &'static str, value: usize) -> Result<NonZeroUsize> {
    NonZeroUsize::new(value)
        .filter(|n| n.get() <= Semaphore::MAX_PERMITS)
        .ok_or(Error::InvalidLimit { field, value })
}
