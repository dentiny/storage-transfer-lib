use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, poll};
use http::{Request, Response, StatusCode};
use opendal::raw::oio::ReadStream;
use opendal::{
    Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator,
    Result, layers::RetryLayer, services::S3,
};
use storage_transfer_lib::HttpConcurrencyLimitLayer;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

#[derive(Default)]
struct Counts {
    active: AtomicUsize,
    peak: AtomicUsize,
    dispatched: AtomicUsize,
}

struct Active(Arc<Counts>);
impl Active {
    fn new(counts: Arc<Counts>) -> Self {
        let active = counts.active.fetch_add(1, Ordering::SeqCst) + 1;
        counts.peak.fetch_max(active, Ordering::SeqCst);
        counts.dispatched.fetch_add(1, Ordering::SeqCst);
        Self(counts)
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

type BodySender = mpsc::UnboundedSender<Result<Buffer>>;
struct Reply {
    response: Response<mpsc::UnboundedReceiver<Result<Buffer>>>,
    size: Option<u64>,
}
struct Call {
    request: Request<Buffer>,
    reply: oneshot::Sender<Result<Reply>>,
}
impl Call {
    fn respond(self, status: StatusCode, size: Option<u64>) -> BodySender {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut response = Response::builder()
            .status(status)
            .header("etag", "\"test-etag\"");
        if let Some(size) = size {
            response = response.header("content-length", size);
        }
        assert!(
            self.reply
                .send(Ok(Reply {
                    response: response.body(rx).unwrap(),
                    size
                }))
                .is_ok()
        );
        tx
    }

    fn complete(self, body: &'static str) {
        let tx = self.respond(StatusCode::OK, Some(body.len() as u64));
        if !body.is_empty() {
            tx.send(Ok(Buffer::from(body))).unwrap();
        }
    }
}

#[derive(Clone)]
struct ControlledTransport {
    calls: mpsc::UnboundedSender<Call>,
    counts: Arc<Counts>,
}
impl HttpTransport for ControlledTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        let active = Active::new(self.counts.clone());
        let (tx, rx) = oneshot::channel();
        self.calls
            .send(Call { request, reply: tx })
            .map_err(|_| failure())?;
        let reply = rx.await.map_err(|_| failure())??;
        Ok(reply.response.map(|rx| {
            HttpBody::new(
                ControlledBody {
                    rx,
                    _active: active,
                },
                reply.size,
            )
        }))
    }
}
struct ControlledBody {
    rx: mpsc::UnboundedReceiver<Result<Buffer>>,
    _active: Active,
}
impl Stream for ControlledBody {
    type Item = Result<Buffer>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

struct Harness {
    transport: HttpTransporter,
    calls: mpsc::UnboundedReceiver<Call>,
    counts: Arc<Counts>,
}
impl Harness {
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let counts = Arc::new(Counts::default());
        let transport = HttpTransporter::new(ControlledTransport {
            calls: tx,
            counts: counts.clone(),
        });
        Self {
            transport,
            calls: rx,
            counts,
        }
    }
    async fn next(&mut self) -> Call {
        timeout(Duration::from_secs(5), self.calls.recv())
            .await
            .expect("request did not arrive")
            .expect("transport closed")
    }
    fn active(&self) -> usize {
        self.counts.active.load(Ordering::SeqCst)
    }
    fn peak(&self) -> usize {
        self.counts.peak.load(Ordering::SeqCst)
    }
}
fn failure() -> Error {
    Error::new(ErrorKind::Unexpected, "injected transport failure").set_temporary()
}
fn request() -> Request<Buffer> {
    Request::builder()
        .uri("https://storage.invalid/object")
        .body(Buffer::new())
        .unwrap()
}
async fn bounded<T>(f: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(5), f)
        .await
        .expect("operation deadlocked")
}

async fn assert_can_dispatch(transport: &HttpTransporter, harness: &mut Harness) {
    let mut fetch = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut fetch).is_pending());
    harness.next().await.complete("");
    bounded(fetch)
        .await
        .unwrap()
        .into_body()
        .to_buffer()
        .await
        .unwrap();
    assert_eq!(harness.active(), 0);
}

#[tokio::test]
async fn shared_transports_hold_permits_after_headers_and_release_at_eof() {
    for limit in [1, 3] {
        let mut harness = Harness::new();
        let layer = HttpConcurrencyLimitLayer::new(limit).unwrap();
        let source = layer.wrap(harness.transport.clone());
        let destination = layer.clone().wrap(harness.transport.clone());
        let mut bodies = Vec::new();
        let mut senders = Vec::new();
        for i in 0..limit {
            let transport = if i % 2 == 0 { &source } else { &destination };
            let mut fetch = Box::pin(transport.fetch(request()));
            assert!(poll!(&mut fetch).is_pending());
            senders.push(harness.next().await.respond(StatusCode::OK, None));
            bodies.push(fetch.await.unwrap().into_body());
        }
        assert_eq!(harness.active(), limit);
        let mut waiting = Box::pin(destination.fetch(request()));
        assert!(poll!(&mut waiting).is_pending());
        assert!(harness.calls.try_recv().is_err());
        assert_eq!(harness.active(), limit);

        senders.remove(0).send(Ok(Buffer::from("payload"))).unwrap(); // Sender drops: payload then EOF.
        assert_eq!(bodies[0].read().await.unwrap().len(), 7);
        assert_eq!(harness.active(), limit);
        assert!(bodies[0].read().await.unwrap().is_empty());
        // Keep the exhausted HttpBody alive: EOF itself must free capacity.
        assert_eq!(harness.active(), limit - 1);
        assert!(poll!(&mut waiting).is_pending());
        harness.next().await.complete("");
        let mut response = waiting.await.unwrap().into_body();
        response.to_buffer().await.unwrap();
        assert_eq!(harness.active(), limit - 1);
        // Re-polling EOF and dropping exhausted bodies must not release twice.
        bodies[0].read().await.unwrap();
        assert_eq!(harness.active(), limit - 1);
        drop(bodies);
        drop(senders);
        assert_eq!(harness.active(), 0);
        assert_eq!(harness.peak(), limit);
    }
}

#[tokio::test]
async fn transport_and_body_errors_release_capacity_and_fuse_the_body() {
    let mut harness = Harness::new();
    let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
    let transport = layer.wrap(harness.transport.clone());
    let mut fetch = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut fetch).is_pending());
    assert!(harness.next().await.reply.send(Err(failure())).is_ok());
    assert!(fetch.await.is_err());
    assert_eq!(harness.active(), 0);

    let mut fetch = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut fetch).is_pending());
    let sender = harness.next().await.respond(StatusCode::OK, None);
    let mut body = fetch.await.unwrap().into_body();
    sender.send(Ok(Buffer::from("partial"))).unwrap();
    sender.send(Err(failure())).unwrap();
    sender
        .send(Ok(Buffer::from(
            "must not be delivered after terminal error",
        )))
        .unwrap();
    assert_eq!(body.read().await.unwrap().len(), 7);
    assert!(body.read().await.is_err());
    assert_eq!(harness.active(), 0);
    assert!(body.read().await.unwrap().is_empty());
    assert!(sender.send(Ok(Buffer::new())).is_err());
    assert_can_dispatch(&transport, &mut harness).await;
}

#[tokio::test]
async fn truncated_body_releases_at_eof_even_when_httpbody_reports_length_error() {
    let mut harness = Harness::new();
    let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
    let transport = layer.wrap(harness.transport.clone());
    let mut fetch = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut fetch).is_pending());
    drop(harness.next().await.respond(StatusCode::OK, Some(10)));
    let mut body = fetch.await.unwrap().into_body();
    assert!(body.to_buffer().await.is_err());
    assert_eq!(harness.active(), 0);
    assert_can_dispatch(&transport, &mut harness).await;
}

#[tokio::test]
async fn cancellation_while_waiting_dispatching_and_reading_leaks_no_permits() {
    let mut harness = Harness::new();
    let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
    let transport = layer.wrap(harness.transport.clone());
    let mut active = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut active).is_pending());
    let call = harness.next().await;
    let mut waiting = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut waiting).is_pending());
    drop(waiting);
    assert_eq!(harness.counts.dispatched.load(Ordering::SeqCst), 1);
    drop(active); // cancellation before response headers
    assert!(call.reply.is_closed());
    assert_eq!(harness.active(), 0);

    let mut fetch = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut fetch).is_pending());
    let sender = harness.next().await.respond(StatusCode::OK, None);
    let mut body = fetch.await.unwrap().into_body();
    let mut read = Box::pin(body.read());
    assert!(poll!(&mut read).is_pending());
    drop(read); // cancelling a borrowed read retains the still-live response
    assert_eq!(harness.active(), 1);
    drop(body); // cancelling the response releases the request
    assert!(sender.is_closed());
    assert_eq!(harness.active(), 0);

    let mut fetch = Box::pin(transport.fetch(request()));
    assert!(poll!(&mut fetch).is_pending());
    harness.next().await.complete("");
    drop(fetch.await.unwrap());
    assert_eq!(harness.active(), 0);
}

fn s3(bucket: &str, transport: HttpTransporter, layer: &HttpConcurrencyLimitLayer) -> Operator {
    Operator::new(
        S3::default()
            .bucket(bucket)
            .region("us-ashburn-1")
            .endpoint("https://storage.invalid")
            .access_key_id("test-key")
            .secret_access_key("test-secret")
            .disable_config_load()
            .disable_ec2_metadata(),
    )
    .unwrap()
    .with_context(OperationContext::new().with_http_transport(transport))
    .layer(layer.clone())
}

#[tokio::test]
async fn real_s3_operators_share_the_limit_for_reads_and_stats() {
    let mut harness = Harness::new();
    let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
    let source = s3("source", harness.transport.clone(), &layer);
    let destination = s3("destination", harness.transport.clone(), &layer);
    let mut read = Box::pin(async { source.read("file").await });
    assert!(poll!(&mut read).is_pending());
    let call = harness.next().await;
    assert_eq!(call.request.method(), "GET");
    let sender = call.respond(StatusCode::OK, Some(4));
    assert!(poll!(&mut read).is_pending());
    let mut stat = Box::pin(async { destination.stat("file").await });
    assert!(poll!(&mut stat).is_pending());
    assert!(harness.calls.try_recv().is_err());
    sender.send(Ok(Buffer::from("data"))).unwrap();
    drop(sender);
    assert_eq!(bounded(read).await.unwrap().to_vec(), b"data");
    assert!(poll!(&mut stat).is_pending());
    let call = harness.next().await;
    assert_eq!(call.request.method(), "HEAD");
    drop(call.respond(StatusCode::OK, Some(0)));
    bounded(stat).await.unwrap();
    assert_eq!(harness.peak(), 1);
    assert_eq!(harness.active(), 0);
}

#[tokio::test(start_paused = true)]
async fn retry_backoff_releases_the_permit_and_retry_reacquires_it() {
    let mut harness = Harness::new();
    let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
    let source = s3("source", harness.transport.clone(), &layer).layer(
        RetryLayer::new()
            .with_min_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(1))
            .with_max_times(1),
    );
    let destination = s3("destination", harness.transport.clone(), &layer);
    let mut read = Box::pin(async { source.read("file").await });
    assert!(poll!(&mut read).is_pending());
    assert!(harness.next().await.reply.send(Err(failure())).is_ok());
    assert!(poll!(&mut read).is_pending());
    assert_eq!(harness.active(), 0);

    let mut stat = Box::pin(async { destination.stat("file").await });
    assert!(poll!(&mut stat).is_pending());
    let call = harness.next().await;
    assert_eq!(call.request.method(), "HEAD");
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(poll!(&mut read).is_pending());
    assert!(harness.calls.try_recv().is_err()); // retry cannot bypass occupied budget
    drop(call.respond(StatusCode::OK, Some(0)));
    stat.await.unwrap();
    assert!(poll!(&mut read).is_pending());
    let call = harness.next().await;
    assert_eq!(call.request.method(), "GET");
    call.complete("retried");
    assert_eq!(read.await.unwrap().to_vec(), b"retried");
    assert_eq!(harness.counts.dispatched.load(Ordering::SeqCst), 3);
    assert_eq!(harness.peak(), 1);
    assert_eq!(harness.active(), 0);
}

#[tokio::test]
async fn real_s3_multipart_requests_and_listing_share_the_http_budget() {
    let mut harness = Harness::new();
    let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
    let source = s3("source", harness.transport.clone(), &layer);
    let destination = s3("destination", harness.transport.clone(), &layer);
    let upload = tokio::spawn(async move {
        let mut writer = destination
            .writer_with("large")
            .chunk(5 * 1024 * 1024)
            .concurrent(2)
            .await?;
        writer.write(vec![1u8; 5 * 1024 * 1024]).await?;
        writer.write(vec![2u8; 5 * 1024 * 1024]).await?;
        writer.close().await
    });
    let list = tokio::spawn(async move { source.list_with("/").recursive(true).await });
    let mut initiated = 0;
    let mut parts = 0;
    let mut completed = 0;
    let mut listed = 0;
    for _ in 0..5 {
        let call = harness.next().await;
        // The upstream writer allows two uploads, but only one reaches transport.
        assert!(harness.calls.try_recv().is_err());
        assert_eq!(harness.active(), 1);
        let query = call.request.uri().query().unwrap_or_default();
        if query == "uploads" {
            initiated += 1;
            call.complete("<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>");
        } else if query.contains("partNumber=") {
            parts += 1;
            assert_eq!(call.request.method(), "PUT");
            assert_eq!(call.request.body().len(), 5 * 1024 * 1024);
            call.complete("");
        } else if call.request.method() == "POST" && query.contains("uploadId=") {
            completed += 1;
            call.complete("<CompleteMultipartUploadResult><Bucket>destination</Bucket><Key>large</Key><ETag>test-etag</ETag></CompleteMultipartUploadResult>");
        } else if query.contains("list-type=2") {
            listed += 1;
            call.complete("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
        } else {
            panic!(
                "unexpected request: {} {}",
                call.request.method(),
                call.request.uri()
            );
        }
    }
    bounded(upload).await.unwrap().unwrap();
    bounded(list).await.unwrap().unwrap();
    assert_eq!((initiated, parts, completed, listed), (1, 2, 1, 1));
    assert_eq!(harness.peak(), 1);
    assert_eq!(harness.active(), 0);
}

#[test]
fn layer_rejects_invalid_limits() {
    assert!(HttpConcurrencyLimitLayer::new(0).is_err());
    assert!(HttpConcurrencyLimitLayer::new(usize::MAX).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_across_transports_never_exceed_the_global_limit() {
    for limit in [1, 4] {
        let mut harness = Harness::new();
        let layer = HttpConcurrencyLimitLayer::new(limit).unwrap();
        let source = layer.wrap(harness.transport.clone());
        let destination = layer.wrap(harness.transport.clone());
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..32 {
            let transport = if index % 2 == 0 {
                source.clone()
            } else {
                destination.clone()
            };
            tasks.spawn(async move {
                transport
                    .fetch(request())
                    .await
                    .unwrap()
                    .into_body()
                    .to_buffer()
                    .await
                    .unwrap()
            });
        }
        for _ in 0..32 / limit {
            let mut batch = Vec::new();
            for _ in 0..limit {
                batch.push(harness.next().await);
            }
            assert_eq!(harness.active(), limit);
            assert!(harness.calls.try_recv().is_err());
            for call in batch {
                call.complete("ok");
            }
        }
        bounded(async {
            while let Some(result) = tasks.join_next().await {
                assert_eq!(result.unwrap().to_vec(), b"ok");
            }
        })
        .await;
        assert_eq!(harness.counts.dispatched.load(Ordering::SeqCst), 32);
        assert_eq!(harness.peak(), limit);
        assert_eq!(harness.active(), 0);
    }
}
