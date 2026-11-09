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

#[tokio::test(start_paused = true)]
async fn cancellation_stops_blocked_requests_and_waiting_workers() {
    for fault in [Fault::BlockRead, Fault::Throttled] {
        let bucket = TestBucket::new(&[17, 17, BUFFER_SIZE + 13], fault);
        let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
        let cancellation = CancellationToken::new();
        let run = engine.run(&cancellation);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("transfer finished before cancellation: {result:?}"),
            _ = bucket.read_started.notified() => {}
        }
        cancellation.cancel();
        assert!(matches!(run.await, Err(Error::Cancelled)));
        bucket.assert_failures(usize::from(matches!(fault, Fault::Throttled)), 0, 0);
        bucket.assert_stopped();
    }
}

#[tokio::test(start_paused = true)]
async fn retries_both_operators() {
    for size in [17, BUFFER_SIZE + 13] {
        let bucket = TestBucket::new(&[size], Fault::Throttled);
        let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
        let summary = engine.run(&CancellationToken::new()).await.unwrap();
        assert_eq!(summary.copied_objects, 1);
        bucket.assert_completed(1);
        bucket.assert_failures(1, 1, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_attempts_cleanup_and_preserves_first_error() {
    for fault in [Fault::AbortDenied, Fault::AbortHang] {
        let bucket = TestBucket::new(&[BUFFER_SIZE + 13, 17, 17, 17], fault);
        let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
        let cancellation = CancellationToken::new();
        let run = engine.run(&cancellation);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("finished before multipart upload: {result:?}"),
            _ = bucket.read_started.notified() => {}
        }
        cancellation.cancel();
        assert!(matches!(run.await, Err(Error::Cancelled)));
        bucket.assert_failures(
            0,
            0,
            if matches!(fault, Fault::AbortHang) {
                3
            } else {
                1
            },
        );
        bucket.assert_stopped();
    }
}

#[tokio::test(start_paused = true)]
async fn reports_verified_success_and_incomplete_objects() {
    for (fault, expected) in [
        (Fault::None, "copied"),
        (Fault::CorruptDestination, "failed"),
        (Fault::UnavailableDestination, "failed"),
        (Fault::SourceChanged, "failed"),
        (Fault::TimeoutRead, "failed"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("report.jsonl");
        let bucket = TestBucket::new(&[17], fault);
        let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
        let result = engine
            .run_with_report(&CancellationToken::new(), &path)
            .await;
        match fault {
            Fault::None => assert_eq!(result.unwrap().copied_objects, 1),
            Fault::CorruptDestination => {
                assert!(matches!(result, Err(Error::VerificationFailed(_))))
            }
            Fault::UnavailableDestination => {
                assert!(matches!(result, Err(Error::Storage(_))));
                bucket.assert_failures(0, 3, 0);
            }
            Fault::TimeoutRead => {
                assert!(matches!(result, Err(Error::Storage(_))));
                bucket.assert_failures(3, 0, 0);
            }
            _ => assert!(
                matches!(result, Err(Error::Storage(error)) if error.kind() == opendal::ErrorKind::ConditionNotMatch)
            ),
        }
        let report = std::fs::read_to_string(&path).unwrap();
        let records: Vec<serde_json::Value> = report
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["path"], "objects/0");
        assert_eq!(records[0]["status"], expected);
        assert_eq!(records[1]["type"], "finished");
        assert_eq!(
            records[1]["progress"]["copied_objects"],
            u64::from(expected == "copied")
        );
        assert!(matches!(
            engine
                .run_with_report(&CancellationToken::new(), &path)
                .await,
            Err(Error::Report(_))
        ));
        assert_eq!(std::fs::read_to_string(path).unwrap(), report);
    }
}

#[tokio::test]
async fn copies_relative_paths_between_directory_roots_and_reports_full_keys() {
    for (source, destination) in [
        ("", "brightdata/"),
        ("raw/", ""),
        ("input/raw", "datasets/brightdata"),
    ] {
        let bucket = TestBucket::new(&[0, 17, BUFFER_SIZE + 13], Fault::None)
            .with_prefixes(source, destination);
        let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("report.jsonl");
        let summary = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            engine.run_with_report(&CancellationToken::new(), &report),
        )
        .await
        .expect("directory copy deadlocked")
        .unwrap();
        assert_eq!(summary.copied_objects, 3);
        bucket.assert_completed(1);
        let source = opendal::raw::normalize_root(source)
            .trim_start_matches('/')
            .to_owned();
        let destination = opendal::raw::normalize_root(destination)
            .trim_start_matches('/')
            .to_owned();
        let records: Vec<serde_json::Value> = std::fs::read_to_string(report)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for record in &records[..3] {
            let path = record["path"].as_str().unwrap();
            assert_eq!(record["source_path"], format!("{source}{path}"));
            assert_eq!(record["destination_path"], format!("{destination}{path}"));
        }
    }
}

#[tokio::test]
async fn prefixed_collisions_leave_destination_unchanged() {
    let bucket = TestBucket::new(&[BUFFER_SIZE + 13], Fault::ExistingDestination)
        .with_prefixes("raw/", "brightdata/");
    assert!(bucket.copy(0).await.is_err());
    bucket.assert_destination_unchanged();
}
