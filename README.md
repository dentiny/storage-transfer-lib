# storage-transfer-lib

Bucket-to-bucket storage transfer service built on Apache OpenDAL.

[Implementation milestone plan](docs/milestone-plan.md) ·
[M0 HTTP concurrency design](docs/http-concurrency.md)

M0 is implemented: validated concurrency settings and a shared HTTP-only
OpenDAL layer. The producer, copy workers and service API follow in M1–M5.

The two concurrency controls are `max_queued_objects` and
`max_ongoing_http_requests`. The object queue is implemented in M1; M0 enforces
HTTP request admission across source and destination, with no chunk semaphore.

```rust
use opendal::{Operator, services::S3};
use storage_transfer_lib::{HttpConcurrencyLimitLayer, TransferLimits};

fn operators() -> Result<(Operator, Operator), Box<dyn std::error::Error>> {
    opendal::install_default();
    let limits = TransferLimits::new(1024, 16)?;
    let layer = HttpConcurrencyLimitLayer::new(limits.max_ongoing_http_requests())?;

    // Set the appropriate S3-compatible endpoint and credentials on these
    // builders for OCI. These bucket names are illustrative.
    let source = Operator::new(S3::default().bucket("source").region("us-ashburn-1"))?
        .layer(layer.clone());
    let destination = Operator::new(S3::default().bucket("destination").region("us-ashburn-1"))?
        .layer(layer.clone());
    Ok((source, destination))
}
```

Configure the base transport before applying the layer, and apply it once per
operator. A request holds a permit until response-body EOF, error or drop.
Layer clones share the budget; constructing separate layers creates separate
budgets. See the architecture note for cancellation and installation details.

OpenDAL 0.59.5 and Tokio 1.53.3 are pinned; commit `Cargo.lock` with dependency
changes. Validation uses Rust 1.97.1.

```sh
cargo test --locked
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
```

Tests use an instrumented fake HTTP transport and real OpenDAL S3 operators.
They require no cloud credentials and perform no production transfers.
