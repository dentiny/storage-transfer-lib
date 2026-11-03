#[path = "common/engine.rs"]
mod common;

use common::{Fault, TestTransfer};
use storage_transfer_lib::{CancellationToken, Error, TransferLimits};

#[tokio::test]
async fn transfers_bucket_with_bounded_work_and_shared_http_limit() {
    for objects in [0, 9] {
        for (queue_size, http_limit) in [(1, 1), (2, 3)] {
            let limits = TransferLimits::new(queue_size, http_limit).unwrap();
            let bucket = TestTransfer::new(objects, Fault::None);
            let mut engine = bucket.engine(limits);
            let mut progress = engine.subscribe();
            let cancellation = CancellationToken::new();
            let run = engine.run(&cancellation);
            tokio::pin!(run);
            let summary = loop {
                tokio::select! {
                    result = &mut run => break result.unwrap(),
                    result = progress.changed() => {
                        result.unwrap();
                        let snapshot = *progress.borrow_and_update();
                        assert!(snapshot.copied_objects <= snapshot.listed_objects);
                        assert!(snapshot.listed_objects - snapshot.copied_objects <= (queue_size + http_limit) as u64);
                    }
                }
            };
            assert_eq!(*progress.borrow(), summary);
            bucket.assert_completed(summary, limits);
        }
    }
}

#[tokio::test]
async fn propagates_listing_worker_and_panic_failures_after_stopping_tasks() {
    for fault in [Fault::Listing, Fault::Copy, Fault::Panic] {
        let bucket = TestTransfer::new(9, fault);
        let mut engine = bucket.engine(TransferLimits::new(1, 2).unwrap());
        let cancellation = CancellationToken::new();
        let error = engine.run(&cancellation).await.unwrap_err();
        match fault {
            Fault::Panic => assert!(matches!(error, Error::Task(error) if error.is_panic())),
            Fault::Listing => assert!(
                matches!(error, Error::Storage(error) if error.kind() == opendal::ErrorKind::PermissionDenied)
            ),
            Fault::Copy => assert!(
                matches!(error, Error::Storage(error) if error.kind() == opendal::ErrorKind::ConditionNotMatch)
            ),
            _ => unreachable!(),
        }
        assert!(!cancellation.is_cancelled());
        bucket.assert_stopped();
    }
}

#[tokio::test]
async fn cancellation_stops_blocked_requests_and_waiting_workers() {
    let bucket = TestTransfer::new(9, Fault::BlockRead);
    let mut engine = bucket.engine(TransferLimits::new(1, 2).unwrap());
    let cancellation = CancellationToken::new();
    let run = engine.run(&cancellation);
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => panic!("transfer finished before cancellation: {result:?}"),
        _ = bucket.read_started.notified() => {}
    }
    cancellation.cancel();
    assert!(matches!(run.await, Err(Error::Cancelled)));
    bucket.assert_stopped();
}
