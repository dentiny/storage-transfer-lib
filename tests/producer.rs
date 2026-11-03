mod common;

use common::TestBucket;
use futures_util::poll;

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
