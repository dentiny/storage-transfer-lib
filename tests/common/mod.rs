use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::stream;
use http::{Request, Response};
use opendal::{
    Buffer, ErrorKind, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator,
    Result, services::S3,
};
use storage_transfer_lib::{BucketProducer, HttpConcurrencyLimitLayer, ObjectTask, TransferLimits};
use tokio::sync::mpsc;

#[derive(Clone)]
pub(super) struct TestBucket {
    objects: usize,
    requests: Arc<AtomicUsize>,
    pub(super) fail_on_page: Option<usize>,
    pub(super) block_on_page: Option<usize>,
}

impl TestBucket {
    pub(super) fn new(objects: usize) -> Self {
        Self {
            objects,
            requests: Arc::new(AtomicUsize::new(0)),
            fail_on_page: None,
            block_on_page: None,
        }
    }

    pub(super) fn producer(
        &self,
        queue_size: usize,
    ) -> (BucketProducer, mpsc::Receiver<ObjectTask>) {
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

    pub(super) fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    pub(super) fn key(index: usize) -> String {
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
