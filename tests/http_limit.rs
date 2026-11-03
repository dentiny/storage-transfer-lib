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

fn transport() -> (HttpTransporter, mpsc::UnboundedReceiver<BodySender>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (HttpTransporter::new(TestTransport(tx)), rx)
}

fn request() -> Request<Buffer> {
    Request::new(Buffer::new())
}

async fn fetch(transport: &HttpTransporter) -> HttpBody {
    timeout(Duration::from_secs(5), transport.fetch(request()))
        .await
        .expect("request blocked")
        .unwrap()
        .into_body()
}

#[tokio::test]
async fn source_and_destination_share_limit_until_body_eof() {
    let (inner, mut calls) = transport();
    let layer = HttpConcurrencyLimitLayer::new(2).unwrap();
    let source = layer.wrap(inner.clone());
    let destination = layer.clone().wrap(inner);
    let mut first = fetch(&source).await;
    let _second = fetch(&destination).await;
    let sender = calls.try_recv().unwrap();
    let _second_sender = calls.try_recv().unwrap();

    let mut waiting = Box::pin(destination.fetch(request()));
    assert!(poll!(&mut waiting).is_pending());
    assert!(calls.try_recv().is_err());

    sender.send(Ok(Buffer::from("data"))).unwrap();
    drop(sender);
    assert_eq!(first.to_buffer().await.unwrap().to_vec(), b"data");
    // EOF releases capacity even while the exhausted body remains alive.
    let _response = timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    assert!(calls.try_recv().is_ok());
}

#[tokio::test]
async fn body_error_and_drop_release_capacity() {
    let (inner, mut calls) = transport();
    let transport = HttpConcurrencyLimitLayer::new(1).unwrap().wrap(inner);
    let mut body = fetch(&transport).await;
    let sender = calls.try_recv().unwrap();
    sender
        .send(Err(Error::new(ErrorKind::Unexpected, "test error")))
        .unwrap();
    assert!(body.to_buffer().await.is_err());

    let next = fetch(&transport).await;
    let sender = calls.try_recv().unwrap();
    drop(next);
    assert!(sender.is_closed());
    let _body = fetch(&transport).await;
    assert!(calls.try_recv().is_ok());
}
