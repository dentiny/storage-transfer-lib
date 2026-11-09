# storage-transfer-lib

A Rust library for copying objects from one S3-compatible bucket to another using Apache OpenDAL. It preserves object keys, streams data in bounded chunks, and verifies the destination with SHA-256 and supported metadata before counting an object as copied.

The engine recursively lists file objects into a bounded queue. Copy workers share one HTTP concurrency limit across source and destination, including listing, reads, multipart writes, verification, and retries.

## Setup

Requires Rust 1.91 or newer and a Tokio runtime. Add these dependencies to your application's `Cargo.toml`:

```toml
[dependencies]
storage-transfer-lib = "0.1.0"
opendal = { version = "0.59.5", default-features = false, features = ["services-s3", "http-transport-reqwest-rustls", "executors-tokio"] }
tokio = { version = "1.53.3", features = ["macros", "rt", "signal"] }
```

## Copy a bucket on OCI

Use existing source and destination buckets. The credentials need permission to list/read the source and read/write the destination, including multipart operations. Destination reads are required for verification.

For OCI, use a **Customer Secret Key** access-key/secret-key pair and the namespace's S3 compatibility endpoint. See [Oracle's S3 compatibility setup](https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi.htm).

Set these environment variables in your application environment:

```sh
export OCI_REGION='us-ashburn-1'
export OCI_S3_ENDPOINT='https://<namespace>.compat.objectstorage.us-ashburn-1.oci.customer-oci.com'
export AWS_ACCESS_KEY_ID='<customer-secret-key-access-key>'
export AWS_SECRET_ACCESS_KEY='<customer-secret-key-secret>'
export SOURCE_BUCKET='source-bucket'
export DESTINATION_BUCKET='destination-bucket'
```

The following `src/main.rs` copies all file objects to the same keys in the destination. Both operators cover the bucket root in this example. This example uses the same region and credentials for both buckets; operators can also be built separately for different endpoints or credentials.

```rust
use std::{env, error::Error, path::Path, time::Duration};

use opendal::{services::S3, Operator};
use storage_transfer_lib::{CancellationToken, TransferConfig, TransferEngine, TransferLimits};

fn bucket(name: &str) -> Result<Operator, Box<dyn Error>> {
    Ok(Operator::new(
        S3::default()
            .bucket(name)
            .root("/")
            .endpoint(&env::var("OCI_S3_ENDPOINT")?)
            .region(&env::var("OCI_REGION")?)
            .access_key_id(&env::var("AWS_ACCESS_KEY_ID")?)
            .secret_access_key(&env::var("AWS_SECRET_ACCESS_KEY")?)
            .disable_config_load()
            .disable_ec2_metadata(),
    )?)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let source = bucket(&env::var("SOURCE_BUCKET")?)?;
    let destination = bucket(&env::var("DESTINATION_BUCKET")?)?;
    let config = TransferConfig {
        limits: TransferLimits::new(1024, 32)?,
        max_retries: 3,
        operation_timeout: Duration::from_secs(30),
        io_timeout: Duration::from_secs(30),
        ..TransferConfig::default()
    };
    let mut engine = TransferEngine::new(source, destination, config)?;
    let cancellation = CancellationToken::new();
    let transfer = engine.run_with_report(&cancellation, Path::new("transfer.jsonl"));
    tokio::pin!(transfer);

    let summary = tokio::select! {
        result = &mut transfer => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            cancellation.cancel();
            transfer.await?
        }
    };
    println!("Copied {} objects ({} bytes)", summary.copied_objects, summary.copied_bytes);
    Ok(())
}
```

Run your application with `cargo run --release`. `transfer.jsonl` must not already exist. Use a new report path for each run. For a transfer without a report, call `engine.run(&cancellation).await` instead.

Ctrl+C requests cancellation and waits for the engine to finish its best-effort cleanup. Completed objects remain in place. Active write/close operations can take time to finish under the configured timeout and retry policy. Cleanup errors are ignored and the first task error is returned. Dropping the transfer future or terminating the process cannot guarantee cleanup of unfinished multipart uploads.

## Configuration

Create a configuration with `TransferConfig::default()` and override the fields you need:

| Setting | Default | Meaning |
| --- | --- | --- |
| `limits.max_queued_objects()` | 1024 | Waiting object descriptors; excludes objects being copied and the current listing page |
| `limits.max_ongoing_http_requests()` | 32 | Shared HTTP request limit across both operators in one engine |
| `operation_timeout` | 30 seconds | Timeout for each operation attempt, such as stat or opening a reader/writer |
| `io_timeout` | 30 seconds | Timeout for each I/O attempt, such as reading, writing, closing, or aborting |
| `max_retries` | 2 | Additional attempts after the first attempt; `0` disables retries |
| `retry_delay` | 100 milliseconds | Initial exponential retry delay |
| `max_retry_delay` | 1 second | Maximum retry delay |

Set both concurrency limits with `TransferLimits::new(queue_capacity, http_concurrency)`. Limits must be positive. Durations must be positive and representable as deadlines; `max_retry_delay` must be at least `retry_delay`.

The engine installs OpenDAL timeout and retry layers on both operators. HTTP capacity is held through the response body and released before retry backoff. Worker count is derived as `min(http_concurrency, 32)`. Multiple engine instances have separate limits.

## Progress and reports

Call `engine.subscribe()` before starting a run to obtain a `tokio::sync::watch::Receiver<TransferProgress>`. Use `changed().await` to observe updates and `borrow_and_update()` to read the latest counters. Watch receivers retain the latest snapshot rather than every update. Release the borrowed snapshot before awaiting again.

Progress includes `listed_objects`, `copied_objects`, and `copied_bytes`. Copied counters advance only after destination verification succeeds. A successful run returns the final `TransferProgress`; after an error, the receiver still exposes partial progress.

`run_with_report` writes JSONL records incrementally for attempted objects, followed by a final progress/error record when the run returns. For example:

```jsonl
{"type":"object","path":"nested/example.bin","bytes":17,"status":"copied","error":null}
{"type":"finished","progress":{"listed_objects":1,"copied_objects":1,"copied_bytes":17},"error":null}
```

Object statuses are `copied`, `failed`, or `cancelled`. A failed run can leave listed objects unattempted; the report is not a complete bucket inventory or a resume database. Process termination can leave a partial report without a `finished` record.

## Directory-scoped transfers

Set the source and destination S3 operator roots independently. Use
`normalize_directory_prefix` before passing user-supplied prefixes to OpenDAL,
which otherwise normalizes some path spellings silently:

```rust
use storage_transfer_lib::normalize_directory_prefix;
use opendal::services::S3;

let source = S3::default().root(&normalize_directory_prefix("input/raw/")?);
let destination = S3::default().root(&normalize_directory_prefix("brightdata/")?);
# Ok::<(), storage_transfer_lib::Error>(())
```

Configure each builder's bucket, endpoint, region and credentials as in the OCI
example, then pass the operators to `TransferEngine::new` as usual.
`input/raw/nested/a.csv` maps to `brightdata/nested/a.csv`: the source directory
is stripped and the destination directory is prepended. Listing is constrained
to `input/raw/`, so `input/raw-other/` is outside the transfer. Empty prefixes
select bucket roots, and prefixes with or without a trailing slash are accepted.
Leading slashes, empty path segments, `.`/`..`, surrounding whitespace and control
characters are rejected by the helper; literal percent escapes are not decoded.

The engine and `copy_object` reject equal or overlapping roots for operators
with the same S3 bucket name, conservatively including different endpoints.
Disjoint sibling directories in the same bucket are allowed. Create-only writes,
conditional source reads, cancellation and destination verification also apply
to scoped transfers. Report `path` stays relative to the configured source root;
new `source_path` and `destination_path` fields contain complete bucket-relative
keys for auditing.
