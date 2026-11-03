use std::time::Duration;

use futures_util::{poll, stream};
use http::{Request, Response};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, Result};
use storage_transfer_lib::HttpConcurrencyLimitLayer;
use tokio::sync::mpsc;
use tokio::time::timeout;

type BodySender = mpsc::UnboundedSender<Result<Buffer>>;

struct TestTransport(mpsc::UnboundedSender<BodySender>);

impl HttpTransport for TestTransport {
    async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.0.send(tx).unwrap();
        let body = HttpBody::new(stream::poll_fn(move |cx| rx.poll_recv(cx)), None);
        Ok(Response::new(body))
    }
}

struct TestClient {
    transport: HttpTransporter,
    calls: mpsc::UnboundedReceiver<BodySender>,
}

impl TestClient {
    fn new(layer: &HttpConcurrencyLimitLayer) -> Self {
        let (tx, calls) = mpsc::unbounded_channel();
        Self {
            transport: layer.wrap(HttpTransporter::new(TestTransport(tx))),
            calls,
        }
    }

    fn request() -> Request<Buffer> {
        Request::new(Buffer::new())
    }

    async fn fetch(&self) -> HttpBody {
        timeout(
            Duration::from_secs(5),
            self.transport.fetch(Self::request()),
        )
        .await
        .expect("request blocked")
        .unwrap()
        .into_body()
    }
}

#[tokio::test]
async fn source_and_destination_share_limit_until_body_eof() {
    let layer = HttpConcurrencyLimitLayer::new(2).unwrap();
    let mut source = TestClient::new(&layer);
    let mut destination = TestClient::new(&layer.clone());
    let mut first = source.fetch().await;
    let _second = destination.fetch().await;
    let sender = source.calls.try_recv().unwrap();
    let _second_sender = destination.calls.try_recv().unwrap();

    let mut waiting = Box::pin(destination.transport.fetch(TestClient::request()));
    assert!(poll!(&mut waiting).is_pending());
    assert!(destination.calls.try_recv().is_err());

    sender.send(Ok(Buffer::from("data"))).unwrap();
    drop(sender);
    assert_eq!(first.to_buffer().await.unwrap().to_vec(), b"data");
    // EOF releases capacity even while the exhausted body remains alive.
    let _response = timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    assert!(destination.calls.try_recv().is_ok());
}

#[tokio::test]
async fn body_error_and_drop_release_capacity() {
    let layer = HttpConcurrencyLimitLayer::new(1).unwrap();
    let mut client = TestClient::new(&layer);
    let mut body = client.fetch().await;
    let sender = client.calls.try_recv().unwrap();
    sender
        .send(Err(Error::new(ErrorKind::Unexpected, "test error")))
        .unwrap();
    assert!(body.to_buffer().await.is_err());

    let next = client.fetch().await;
    let sender = client.calls.try_recv().unwrap();
    drop(next);
    assert!(sender.is_closed());
    let _body = client.fetch().await;
    assert!(client.calls.try_recv().is_ok());
}
