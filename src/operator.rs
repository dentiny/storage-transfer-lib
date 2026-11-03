use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};

use crate::config::{DEFAULT_MAX_RETRIES, DEFAULT_OPERATION_TIMEOUT, DEFAULT_RETRY_DELAY};

pub(crate) fn with_policies(operator: Operator) -> Operator {
    operator
        .layer(
            TimeoutLayer::new()
                .with_timeout(DEFAULT_OPERATION_TIMEOUT)
                .with_io_timeout(DEFAULT_OPERATION_TIMEOUT),
        )
        .layer(
            RetryLayer::new()
                .with_max_times(DEFAULT_MAX_RETRIES)
                .with_min_delay(DEFAULT_RETRY_DELAY)
                .with_max_delay(DEFAULT_RETRY_DELAY * 10),
        )
}
