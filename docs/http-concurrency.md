# HTTP concurrency in M0

M0 implements the Rust library skeleton, `TransferLimits`, and
`HttpConcurrencyLimitLayer`. OpenDAL is pinned to **0.59.5**, Tokio to **1.53.3**;
`Cargo.lock` records transitive dependencies. The project targets native Tokio
applications. Rust 1.97.1 was used for the initial validation; the manifest's
minimum Rust version is 1.91, following the pinned OpenDAL release.

## What is counted

One permit covers a call to the underlying OpenDAL `HttpTransport::fetch` and its
response body. It is acquired before dispatch, held after response headers, and
released on body EOF, body error, or drop. An error/cancellation before headers
also releases it. After EOF/error the wrapper drops the underlying stream and is
terminal, so keeping or polling the exhausted `HttpBody` cannot retain or release
a permit twice.

Dropping a *borrowed* `body.read()` future does not cancel the entire request:
the still-owned response body retains the permit. Drop the response/body to
cancel that request. Cancelling a request waiting for admission never invokes
the underlying transport. No tasks are spawned by the limiter.

The cap applies to LIST, HEAD, GET, PUT and multipart HTTP calls from both
operators. Retried attempts reacquire it. Waiting requests are not counted by
`ongoing_http_requests()`; that gauge tracks dispatch/response lifetimes rather
than `limit - semaphore.available_permits()`, because the semaphore can reserve
permits for waiters that have not yet resumed.

This counts calls at the wrapped transport boundary, not TCP connections,
objects, chunks, buffered bytes or remote server work after cancellation. An
underlying client's sequential redirects/retries remain inside the admitted
call. An underlying client that independently fans a call into concurrent
requests needs its own compatible accounting. Unrelated SDK traffic that does
not use this transport is outside the limit.

## Installation order

1. Configure each source/destination operator's endpoint, credentials and base
   HTTP transport. Use OCI's S3-compatible endpoint when connecting to OCI.
2. Create **one** `HttpConcurrencyLimitLayer`; apply clones to both operators.
3. Add operation retry behavior as required. It must use the resulting operator
   context, so each HTTP attempt reaches the wrapped transport.
4. Only then create readers, writers and listers.

```rust
use opendal::{Operator, services::S3};
use storage_transfer_lib::{HttpConcurrencyLimitLayer, TransferLimits};

// With auto-registration disabled, explicitly install the default HTTP client.
// An application can instead set a custom OperationContext before the layer.
opendal::install_default();
let limits = TransferLimits::new(1024, 16)?;
let limit = HttpConcurrencyLimitLayer::new(limits.max_ongoing_http_requests())?;
let source = Operator::new(source_builder)?.layer(limit.clone());
let destination = Operator::new(destination_builder)?.layer(limit.clone());
```

Here `source_builder` and `destination_builder` are application-configured S3
builders. M0 deliberately leaves credentials in OpenDAL's configuration rather
than creating another credential container. It stores no credentials in the
limits or request diagnostics.

For an injected transport, `limit.wrap(base_transport)` is also available.
Use **either** the layer or a wrapped transport once per operator. Wrapping the
same request twice in the same budget can deadlock at H=1. Do not replace the
operator's HTTP transport after installing the limiter, which would bypass it.
Separate `new` calls create independent budgets; share the layer across all
operators/jobs that must respect one process-wide cap.

## Why a small custom layer

The pinned OpenDAL `ConcurrentLimitLayer` includes operation-level admission as
well as HTTP admission. Its HTTP stream guard releases on drop, not explicitly
on EOF/error. Our layer changes only the HTTP transport and tears down the
stream/permit at terminal response events. There is no operation semaphore and
no nested operation/HTTP acquisition.

The adapter implements OpenDAL's `raw::Layer::apply_context` and `HttpTransport`,
using `HttpBody::map_inner` to preserve the response's size validation and
headers. Owned Tokio permits handle cancellation. The body stream is dropped
before its permit, so the next request is not admitted ahead of local teardown.

## Verification and limits of M0

The test transport exposes controlled response headers, delayed body data,
errors and EOF. Its independent counters track actual transport/body lifetimes,
not the limiter's own gauge. Tests cover H=1 and larger caps, multi-threaded
callers, cancellation at admission/dispatch/body read, transport and body errors,
truncated responses, EOF with the body retained, and repeated terminal polls.

Integration tests construct real OpenDAL S3 operators with fake credentials and
an injected transport. They exercise GET/HEAD across two operators, retry backoff
with a competing request, recursive LIST, and multipart initialization, two
parts and completion. OpenDAL's concurrent writer still respects H=1. No test
uses real OCI credentials, touches a cloud bucket or deletes data.

Run `cargo test --locked`, `cargo fmt --all -- --check`, and
`cargo clippy --locked --all-targets -- -D warnings`.

The bounded object queue is only configured in M0; M1 implements it. Copying,
service lifecycle and OCI compatibility remain later milestones. H=1 transport
admission is tested here; the future copy loop must also avoid holding a source
response while waiting for a destination request in the same budget.

Reference implementation inspected:
[OpenDAL 0.59.5 ConcurrentLimitLayer](https://docs.rs/crate/opendal-layer-concurrent-limit/0.59.5/source/src/lib.rs).
