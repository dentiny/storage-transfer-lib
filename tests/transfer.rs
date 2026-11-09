#[path = "common/bucket.rs"]
mod common;

use common::{BUFFER_SIZE, Fault, TestBucket};
use storage_transfer_lib::{CancellationToken, Error, TransferLimits, TransferProgress};

#[tokio::test]
async fn copies_bounded_ranges_and_streams_verification_with_one_http_permit() {
    for size in [
        0,
        17,
        BUFFER_SIZE,
        BUFFER_SIZE + 13,
        3 * BUFFER_SIZE + 7,
        16 * BUFFER_SIZE + 31,
    ] {
        let bucket = TestBucket::new(&[size], Fault::None);
        tokio::time::timeout(std::time::Duration::from_secs(60), bucket.copy(0))
            .await
            .expect("single-permit copy deadlocked")
            .unwrap();
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
async fn overlaps_upload_with_one_bounded_source_prefetch() {
    let bucket = TestBucket::new(&[4 * BUFFER_SIZE + 13], Fault::RequireReadWriteOverlap);
    let mut engine = bucket.engine(TransferLimits::new(1, 2).unwrap());
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        engine.run(&CancellationToken::new()),
    )
    .await
    .expect("upload and bounded prefetch did not overlap")
    .unwrap();
    bucket.assert_completed(2);
}

#[tokio::test]
async fn cancellation_releases_prefetch_and_streaming_verification_bodies() {
    for fault in [Fault::BlockPrefetch, Fault::BlockVerificationBody] {
        let bucket = TestBucket::new(&[3 * BUFFER_SIZE + 13], fault);
        let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
        let cancellation = CancellationToken::new();
        let run = engine.run(&cancellation);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("finished before cancellation: {result:?}"),
            _ = bucket.read_started.notified() => {}
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => panic!("body was never reached"),
        }
        cancellation.cancel();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(10), run)
                .await
                .unwrap(),
            Err(Error::Cancelled)
        ));
        bucket.assert_failures(0, 0, 0);
    }
}

#[tokio::test]
async fn rejects_truncated_verification_stream_without_advancing_progress() {
    let bucket = TestBucket::new(&[BUFFER_SIZE + 13], Fault::TruncatedVerificationBody);
    let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
    let progress = engine.subscribe();
    assert!(engine.run(&CancellationToken::new()).await.is_err());
    assert_eq!(progress.borrow().copied_objects, 0);
    assert_eq!(progress.borrow().copied_bytes, 0);
    bucket.assert_failures(0, 0, 0);
}

// Run each variant in a fresh process and use /usr/bin/time to compare peak RSS.
// Synthetic 64 KiB HTTP frames retain larger backing allocations, like slices
// of transport receive buffers. This is not an OCI throughput benchmark.
#[tokio::test]
#[ignore = "synthetic read-path benchmark; run explicitly in release mode"]
async fn read_path_benchmark() {
    let ratio = std::env::var("BACKING_RATIO")
        .unwrap_or_else(|_| "8".into())
        .parse()
        .unwrap();
    let bucket = TestBucket::new(&[64 * 1024 * 1024; 4], Fault::None)
        .with_transport_shape(ratio, std::time::Duration::from_millis(2));
    let mut engine = bucket.engine(TransferLimits::new(4, 4).unwrap());
    let start = std::time::Instant::now();
    let summary = engine.run(&CancellationToken::new()).await.unwrap();
    assert_eq!(summary.copied_objects, 4);
    println!(
        "GET counts (source, destination): {:?}",
        bucket.read_counts()
    );
    println!(
        "backing_ratio={ratio} bytes={} elapsed={:?} MiB/s={:.1}",
        summary.copied_bytes,
        start.elapsed(),
        summary.copied_bytes as f64 / 1048576.0 / start.elapsed().as_secs_f64()
    );
}

#[tokio::test]
async fn upload_failure_cancels_blocked_prefetch_before_aborting() {
    let bucket = TestBucket::new(
        &[4 * BUFFER_SIZE + 13],
        Fault::FailUploadWithBlockedPrefetch,
    );
    let mut engine = bucket.engine(TransferLimits::new(1, 2).unwrap());
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        engine.run(&CancellationToken::new()),
    )
    .await
    .expect("failed upload did not cancel its prefetch")
    .unwrap_err();
    assert!(
        matches!(error, Error::Storage(error) if error.kind() == opendal::ErrorKind::PermissionDenied)
    );
    bucket.assert_failures(0, 1, 1);
}

#[tokio::test]
async fn retries_verification_from_last_consumed_offset() {
    let bucket = TestBucket::new(&[BUFFER_SIZE + 13], Fault::RetryVerificationBody);
    let mut engine = bucket.engine(TransferLimits::new(1, 1).unwrap());
    engine.run(&CancellationToken::new()).await.unwrap();
    bucket.assert_completed(1);
    bucket.assert_failures(1, 0, 0);
}
