use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};

use crate::TransferConfig;

pub(crate) fn with_policies(operator: Operator, config: &TransferConfig) -> Operator {
    operator
        .layer(
            TimeoutLayer::new()
                .with_timeout(config.operation_timeout)
                .with_io_timeout(config.io_timeout),
        )
        .layer(
            RetryLayer::new()
                .with_max_times(config.max_retries)
                .with_min_delay(config.retry_delay)
                .with_max_delay(config.max_retry_delay),
        )
}
