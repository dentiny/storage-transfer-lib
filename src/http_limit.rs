use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures_util::Stream;
use http::{Request, Response};
use opendal::raw::{Layer, Servicer};
use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{ConfigError, validate_limit};

#[derive(Clone, Debug)]
pub struct HttpConcurrencyLimitLayer {
    semaphore: Arc<Semaphore>,
    ongoing: Arc<AtomicUsize>,
    limit: usize,
}

impl HttpConcurrencyLimitLayer {
    pub fn new(max_ongoing_http_requests: usize) -> std::result::Result<Self, ConfigError> {
        let limit = validate_limit("max_ongoing_http_requests", max_ongoing_http_requests)?.get();
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            ongoing: Arc::new(AtomicUsize::new(0)),
            limit,
        })
    }

    pub fn ongoing_http_requests(&self) -> usize {
        // A fair semaphore can reserve a released permit for an unpolled waiter.
        // Such a waiter has not dispatched a request and must not be counted.
        self.ongoing.load(Ordering::Relaxed)
    }

    pub fn max_ongoing_http_requests(&self) -> usize {
        self.limit
    }

    pub fn wrap(&self, inner: HttpTransporter) -> HttpTransporter {
        HttpTransporter::new(LimitedTransport {
            inner,
            semaphore: self.semaphore.clone(),
            ongoing: self.ongoing.clone(),
        })
    }
}

impl Layer for HttpConcurrencyLimitLayer {
    fn apply_context(&self, _service: Servicer, inner: OperationContext) -> OperationContext {
        inner.with_http_transport(self.wrap(inner.http_transport().clone()))
    }
}

struct LimitedTransport {
    inner: HttpTransporter,
    semaphore: Arc<Semaphore>,
    ongoing: Arc<AtomicUsize>,
}

impl HttpTransport for LimitedTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        // This private semaphore is never closed. Dropping a waiting or active
        // future is cancellation-safe; no request is spawned in the background.
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("private semaphore is never closed");
        self.ongoing.fetch_add(1, Ordering::Relaxed);
        let permit = RequestPermit {
            ongoing: self.ongoing.clone(),
            _permit: permit,
        };
        let response = self.inner.fetch(request).await?;
        let (parts, body) = response.into_parts();
        let body = body.map_inner(|stream| {
            Box::new(PermitStream {
                inner: Some(stream),
                permit: Some(permit),
            })
        });
        Ok(Response::from_parts(parts, body))
    }
}

// Field order ensures cancellation drops the underlying body before releasing
// admission to the next request. EOF/error also explicitly tears it down first.
struct PermitStream<S> {
    inner: Option<S>,
    permit: Option<RequestPermit>,
}

struct RequestPermit {
    ongoing: Arc<AtomicUsize>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        // Decrement before the semaphore field drops and wakes another request.
        self.ongoing.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<S> Stream for PermitStream<S>
where
    S: Stream<Item = Result<Buffer>> + Unpin,
{
    type Item = Result<Buffer>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let result = Pin::new(inner).poll_next(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            this.inner.take();
            this.permit.take();
        }
        result
    }
}
