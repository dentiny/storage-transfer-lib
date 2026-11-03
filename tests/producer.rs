use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::{poll, stream};
use http::{Request, Response};
use opendal::{
    Buffer, ErrorKind, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator,
    Result, services::S3,
};
use storage_transfer_lib::{
    BucketProducer, Error, HttpConcurrencyLimitLayer, ObjectTask, TransferLimits,
};
use tokio::sync::mpsc;

#[derive(Clone)]
struct TestBucket {
    objects: usize,
    requests: Arc<AtomicUsize>,
    fail_on_page: Option<usize>,
    block_on_page: Option<usize>,
}

impl TestBucket {
    fn new(objects: usize) -> Self {
        Self {
            objects,
            requests: Arc::new(AtomicUsize::new(0)),
            fail_on_page: None,
            block_on_page: None,
        }
    }

    fn producer(&self, queue_size: usize) -> (BucketProducer, mpsc::Receiver<ObjectTask>) {
        let source = Operator::new(
            S3::default()
                .bucket("source")
                .region("us-ashburn-1")
                .endpoint("https://storage.invalid")
                .access_key_id("test-key")
                .secret_access_key("test-secret")
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .unwrap()
        .with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(self.clone())),
        )
        .layer(HttpConcurrencyLimitLayer::new(1).unwrap());
        BucketProducer::new(source, TransferLimits::new(queue_size, 1).unwrap())
    }

    fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn key(index: usize) -> String {
        match index {
            0 => "empty".into(),
            1 => "nested/".into(),
            _ => format!("nested/深い/object {index}"),
        }
    }
}

impl HttpTransport for TestBucket {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        let page = self.requests.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.method(), "GET");
        let query = request.uri().query().unwrap();
        let param = |name| {
            query.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == name).then_some(value)
            })
        };
        assert_eq!(param("list-type"), Some("2"));
        assert!(param("prefix").is_none());
        assert!(param("delimiter").is_none());
        let page_size: usize = param("max-keys").unwrap().parse().unwrap();
        assert!((1..=1000).contains(&page_size));
        let start = param("continuation-token")
            .unwrap_or("0")
            .parse::<usize>()
            .unwrap();
        assert_eq!(start, page * page_size);

        if self.fail_on_page == Some(page) {
            return Err(opendal::Error::new(
                ErrorKind::Unexpected,
                "injected listing failure",
            ));
        }
        if self.block_on_page == Some(page) {
            return pending().await;
        }

        let end = (start + page_size).min(self.objects);
        let mut xml = format!(
            "<ListBucketResult><IsTruncated>{}</IsTruncated>",
            end < self.objects
        );
        if end < self.objects {
            xml.push_str(&format!(
                "<NextContinuationToken>{end}</NextContinuationToken>"
            ));
        }
        // Generate only the requested page, even for a large synthetic inventory.
        for index in start..end {
            let key = Self::key(index);
            xml.push_str(&format!(
                "<Contents><Key>{key}</Key><Size>{index}</Size><ETag>etag-{index}</ETag>\
                <LastModified>2026-01-01T00:00:00Z</LastModified></Contents>"
            ));
        }
        xml.push_str("</ListBucketResult>");
        let body = Buffer::from(xml);
        let size = body.len() as u64;
        Ok(Response::new(HttpBody::new(
            stream::iter([Ok(body)]),
            Some(size),
        )))
    }
}

#[tokio::test]
async fn lists_all_pages_with_queue_backpressure() {
    for total in [0, 12, 10_000] {
        let bucket = TestBucket::new(total);
        let (producer, mut queue) = bucket.producer(2);
        let mut run = Box::pin(producer.run());

        if total > 0 {
            assert!(poll!(&mut run).is_pending());
            assert_eq!(queue.len(), 2);
            assert_eq!(bucket.request_count(), 1);
            assert!(poll!(&mut run).is_pending());
            assert_eq!(bucket.request_count(), 1);
        }
        let (result, ()) = tokio::join!(run, async {
            for index in 0..total {
                let task = queue.recv().await.unwrap();
                assert_eq!(task.path, TestBucket::key(index));
                assert_eq!(task.metadata.etag(), Some(format!("etag-{index}").as_str()));
                if index == 1 {
                    assert!(task.metadata.is_dir());
                } else {
                    assert_eq!(task.metadata.content_length(), index as u64);
                }
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
    assert!(poll!(&mut run).is_pending());
    assert_eq!(queue.try_recv().unwrap().path, "empty");
    assert!(matches!(
        poll!(&mut run),
        std::task::Poll::Ready(Err(Error::Storage(_)))
    ));
    assert_eq!(queue.try_recv().unwrap().path, "nested/");
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
        assert_eq!(bucket.request_count(), 1);
    }
}
