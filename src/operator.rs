use std::cell::Cell;

use opendal::layers::{RetryEvent, RetryLayer, TimeoutLayer};
use opendal::{Metadata, Operator, Writer};

use crate::config::{MAX_RETRIES, OPERATION_TIMEOUT, RETRY_DELAY};

tokio::task_local! {
    static CLOSE_RETRIED: Cell<bool>;
}

pub(crate) fn with_policies(operator: Operator) -> Operator {
    operator
        .layer(
            TimeoutLayer::new()
                .with_timeout(OPERATION_TIMEOUT)
                .with_io_timeout(OPERATION_TIMEOUT),
        )
        .layer(
            RetryLayer::new()
                .with_max_times(MAX_RETRIES)
                .with_min_delay(RETRY_DELAY)
                .with_max_delay(RETRY_DELAY * 10)
                .with_notify(|_: RetryEvent<'_>| {
                    let _ = CLOSE_RETRIED.try_with(|retried| retried.set(true));
                }),
        )
}

// Keep retry tracking local to this close; other workers cannot affect reconciliation.
pub(crate) async fn close(writer: &mut Writer) -> (opendal::Result<Metadata>, bool) {
    CLOSE_RETRIED
        .scope(Cell::new(false), async {
            let result = writer.close().await;
            (result, CLOSE_RETRIED.with(Cell::get))
        })
        .await
}
