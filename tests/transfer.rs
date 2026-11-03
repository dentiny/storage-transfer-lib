#[path = "common/bucket.rs"]
mod common;

use common::{BUFFER_SIZE, Fault, TestBucket};
use storage_transfer_lib::{CancellationToken, Error, TransferLimits, TransferProgress};

#[tokio::test]
async fn copies_objects_in_bounded_ranges_with_one_http_permit() {
    for size in [
        0,
        17,
        BUFFER_SIZE,
        BUFFER_SIZE + 13,
        3 * BUFFER_SIZE + 7,
        16 * BUFFER_SIZE + 31,
    ] {
        let bucket = TestBucket::new(&[size], Fault::None);
        bucket.copy(0).await.unwrap();
        bucket.assert_completed(1);
    }
}

#[tokio::test]
async fn existing_destination_is_not_overwritten() {
    for size in [17, 2 * BUFFER_SIZE + 13] {
        let bucket = TestBucket::new(&[size], Fault::ExistingDestination);
        assert!(
            matches!(bucket.copy(0).await, Err(Error::Storage(error)) if error.kind() == opendal::ErrorKind::ConditionNotMatch)
        );
        bucket.assert_destination_unchanged();
    }
}

#[tokio::test]
async fn transfers_bucket_with_shared_http_limit() {
    let sizes = [0, 17, 2048, BUFFER_SIZE + 13].repeat(2);
    for sizes in [&[][..], sizes.as_slice()] {
        for (queue_size, http_limit) in [(1, 1), (2, 3)] {
            let bucket = TestBucket::new(sizes, Fault::None);
            let mut engine = bucket.engine(TransferLimits::new(queue_size, http_limit).unwrap());
            let progress = engine.subscribe();
            let summary = engine.run(&CancellationToken::new()).await.unwrap();
            assert_eq!(
                summary,
                TransferProgress {
                    listed_objects: sizes.len() as u64,
                    copied_objects: sizes.len() as u64,
                    copied_bytes: sizes.iter().sum::<usize>() as u64,
                }
            );
            assert_eq!(*progress.borrow(), summary);
            bucket.assert_completed(http_limit);
        }
    }
}

#[tokio::test]
async fn propagates_listing_worker_and_panic_failures_after_stopping_tasks() {
    for fault in [Fault::Listing, Fault::Copy, Fault::Panic] {
        let bucket = TestBucket::new(&[0, 17, BUFFER_SIZE + 13], fault);
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
    let bucket = TestBucket::new(&[0, 17, BUFFER_SIZE + 13], Fault::BlockRead);
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
