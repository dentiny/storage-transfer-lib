mod common;

use common::TestBucket;
use futures_util::poll;
use storage_transfer_lib::Error;

#[tokio::test]
async fn lists_all_pages_with_queue_backpressure() {
    for total in [0, 12, 10_000] {
        let bucket = TestBucket::new(total);
        let (producer, mut queue) = bucket.producer(2);
        let mut run = Box::pin(producer.run());

        if total > 0 {
            assert!(poll!(&mut run).is_pending());
            assert_eq!(queue.len(), 2);
            assert_eq!(bucket.request_count(), 2);
            assert!(poll!(&mut run).is_pending());
            assert_eq!(bucket.request_count(), 2);
        }
        let (result, ()) = tokio::join!(run, async {
            for index in (0..total).filter(|index| *index != 1) {
                let task = queue.recv().await.unwrap();
                assert_eq!(task.path, TestBucket::key(index));
                assert_eq!(task.metadata.etag(), Some(format!("etag-{index}").as_str()));
                assert!(task.metadata.is_file());
                assert_eq!(task.metadata.content_length(), index as u64);
            }
            assert!(queue.recv().await.is_none());
        });
        result.unwrap();
        assert_eq!(bucket.request_count(), total.div_ceil(2).max(1));
    }
}

#[tokio::test]
async fn reports_failure_after_a_successful_page() {
    let mut bucket = TestBucket::new(4);
    bucket.fail_on_page = Some(1);
    let (producer, mut queue) = bucket.producer(2);
    let mut run = Box::pin(producer.run());
    assert!(matches!(
        poll!(&mut run),
        std::task::Poll::Ready(Err(Error::Storage(_)))
    ));
    assert_eq!(queue.try_recv().unwrap().path, "empty");
    assert!(queue.is_closed());
    assert!(queue.is_empty());
}

#[tokio::test]
async fn closing_queue_cancels_listing_and_waiting_for_capacity() {
    for blocked_page in [None, Some(0)] {
        let mut bucket = TestBucket::new(4);
        bucket.block_on_page = blocked_page;
        let (producer, queue) = bucket.producer(2);
        let mut run = Box::pin(producer.run());
        assert!(poll!(&mut run).is_pending());
        assert_eq!(queue.len(), if blocked_page.is_some() { 0 } else { 2 });
        drop(queue);
        assert!(matches!(
            poll!(&mut run),
            std::task::Poll::Ready(Err(Error::ConsumerClosed))
        ));
        assert_eq!(
            bucket.request_count(),
            if blocked_page.is_some() { 1 } else { 2 }
        );
    }
}
