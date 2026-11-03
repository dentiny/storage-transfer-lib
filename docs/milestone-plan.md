# Storage transfer service milestone plan

Implement a Rust service on Apache OpenDAL that copies every current object from source bucket A to destination bucket B, preserving object keys and bytes. Start with OCI through its S3-compatible endpoint. This is a storage transfer service; it does not convert data formats.

The two public concurrency limits are **`max_queued_objects`** and **`max_ongoing_http_requests`**. There is no chunk counter, chunk semaphore, or public chunk-concurrency setting.

## Scope and architecture

```text
Source bucket A
      |
Paginated recursive lister
      |
Bounded object queue (Q)
      |
Fixed pool of copy workers
      |
Bounded reads -> OpenDAL Writer / multipart upload
      |
Destination bucket B

Source and destination OpenDAL HTTP transports
      |
Shared HTTP concurrency limiter (H)
      |
Underlying HTTP client
```

Each queue entry contains object metadata, never the object's payload. A single producer consumes OpenDAL's streaming lister and awaits queue capacity. Workers copy objects with bounded reads and incremental writes. Successful object completion requires the destination writer to close successfully.

OpenDAL provides a streaming `Lister` and recursive listing options; pagination is handled by the backend, with page-size controls where supported. Use this streaming interface rather than collecting a bucket into a `Vec`. [Listing API](https://opendal.apache.org/docs/rust/opendal/struct.Operator.html#method.lister_with)

The source and destination share one HTTP limiter instance. For the initial service, allow one active transfer job per process; reject another submission as busy. A later multi-job service must retain the process-wide shared limiter. This plan does not claim a distributed limit across replicas.

## Configuration and invariants

| Setting | Meaning |
| --- | --- |
| `source` | Endpoint, region, namespace/bucket as required by the backend, and credential reference; root must cover the whole bucket |
| `destination` | Existing target bucket and its endpoint/region/credential reference; identical keys are used |
| `max_queued_objects = Q` | Maximum waiting object descriptors in the queue; positive integer |
| `max_ongoing_http_requests = H` | Maximum active OpenDAL HTTP requests across both operators; positive integer |

Use a bounded worker pool derived from H, initially `W = min(H, 32)`. This is an initial scheduling choice to benchmark, not another public concurrency limit. Disable speculative read-ahead and keep each writer's internal concurrency bounded initially. Transfer buffer sizing is internal and validated against backend capabilities.

- `queue_depth <= Q` at all times. Objects already assigned to W workers and the lister's current page are outside this count and are separately bounded.
- `ongoing_http_requests <= H` across source and destination combined. Count LIST, HEAD/stat, GET/range GET, PUT, multipart initialization, part upload and completion, and retries that reach the wrapped transport.
- A request obtains a permit before transport dispatch and retains it while the request is pending and while its response body remains active. Release at EOF, error or drop/cancellation; receiving response headers alone is insufficient.
- Every retry attempt passes through the same limiter. Finish/drop a failed attempt before waiting in retry backoff; no leaked or duplicated permits.
- The limiter covers requests made through the configured OpenDAL transport. Unrelated authentication SDK traffic is outside that boundary and must not be described as covered.
- HTTP concurrency is not a payload-memory cap. Bound worker buffers, writer buffering and listing pages independently. Expected memory growth is based on bounded page/queue metadata plus W bounded transfer buffers and backend overhead, not total object count or total bucket bytes.
- H=1 must work without deadlock. Do not retain an active GET response while waiting for a PUT permit from the same exhausted limiter. Initially finish a bounded range read and release its response before scheduling the corresponding write.
- Preserve keys exactly, including unusual characters and actual directory-marker objects. Do not silently skip unknown entry types; stat or report them. Validate that OpenDAL's path handling can represent the relevant keys without collisions.
- No source-object deletion, bucket deletion, moves, or sync-with-delete. Default to create-only destination writes and fail on collisions. A preflight HEAD alone is insufficient protection against concurrent writers; verify conditional-write support through multipart completion.
- v1 copies current live objects, not historical versions or delete markers. Require a quiescent source for a complete bucket-copy guarantee; detect changed/deleted objects where supported and report incomplete work rather than claiming a consistent snapshot during ongoing writes.

## M0 Establish the project and HTTP limiter

**Status: implemented.** Validated configuration, shared HTTP-only layer, pinned dependencies, controlled transport tests, S3 retry/multipart integration tests are in place. See [M0 architecture and verification](http-concurrency.md). M1–M6 remain pending.

**Deliverables:** Cargo project, pinned OpenDAL/Tokio dependencies and lockfile, configuration types, `HttpConcurrencyLimitLayer` or equivalent transport wrapper, instrumented fake HTTP transport.

Reuse OpenDAL's HTTP concurrency mechanism if the pinned version meets the contract. Its current `ConcurrentLimitLayer` supports shared HTTP semaphores, and the implementation retains permits with response bodies. Operation-level limiting and HTTP limiting have separate semantics; do not pass the same finite semaphore into nested operation and HTTP acquisitions. If the built-in layer introduces unsuitable operation limits or cannot meet EOF/cancellation behavior, implement a small HTTP-only wrapper around the supported transport interface. [Layer API](https://opendal.apache.org/docs/rust/opendal_layer_concurrent_limit/struct.ConcurrentLimitLayer.html) · [Implementation](https://github.com/apache/opendal/blob/main/core/layers/concurrent-limit/src/lib.rs)

**Acceptance:**

- Concurrent requests from both operators never exceed H, including H=1.
- Delayed response bodies retain permits after headers; EOF, transport errors, mid-body errors and cancellation release them exactly once.
- Cancelling while waiting does not dispatch a request or leak a permit.
- Retry attempts and multipart requests cannot bypass the limiter; exhausted permits provide backpressure.
- Record the pinned API and layer/transport installation order in a short architecture note. Compilation and tests establish compatibility; unversioned online docs are not the dependency contract.

## M1 Implement the paginated producer and bounded queue

**Deliverables:** `ObjectTask`, bucket lister, bounded channel and producer lifecycle.

List from the bucket root recursively. Consume provider pagination lazily, put one object descriptor at a time into the channel and await a full queue. Keep at most a bounded listing page plus the producer's current descriptor outside the queue. Do not spawn a task per object, collect all keys, or create an unbounded pending-send list.

**Acceptance:**

- A fake paginated backend enumerates more than three pages, nested keys, an empty bucket and zero-byte objects without omission.
- Blocking consumers fills the queue to Q and stops producer advancement beyond the documented bounded page/current-entry allowance.
- Mid-list failure propagates as a failed/incomplete job, not successful EOF. Cancellation unblocks both listing and queue sends.
- Large synthetic inventories do not cause memory or task count to scale with the total number of objects.

## M2 Implement one object copy

**Deliverables:** `copy_object(source, destination, task, cancellation)` using OpenDAL Reader and Writer.

Use bounded range reads or another proven bounded streaming path, then incremental writes. Let the backend implement multipart upload; do not use whole-object `read`/`read_to_end`, server-side copy, or a full-object memory buffer. Start without per-object parallel read-ahead. Preserve supported content metadata and report unsupported required metadata instead of silently claiming full preservation.

OpenDAL Reader supports range reads. Writer supports incremental writes and requires successful close to complete a write. Its concurrent mode may return from a write before the upload finishes, so object success cannot be based on `write()` returning. [Reader API](https://opendal.apache.org/docs/rust/opendal/struct.Reader.html) · [Writer API](https://opendal.apache.org/docs/rust/opendal/struct.Writer.html) · [Write buffering and conditions](https://opendal.apache.org/docs/rust/opendal/options/struct.WriteOptions.html)

**Acceptance:**

- Copy zero-byte, small, exact-buffer-size, non-aligned tail and multi-part objects with identical bytes and keys.
- A large generated object can be copied without memory proportional to its size.
- H=1 progresses through read, upload and finalization; no active source response waits indefinitely for a destination permit.
- Failure during read, write or close never marks the object complete.
- Validate backend part-size/part-count constraints and conditional creation. Reject unsupported object sizes or required capabilities before unsafe writes; destination collision leaves the existing object unchanged.

## M3 Connect the worker pool and bucket transfer engine

**Deliverables:** a `TransferEngine` coordinating the producer, queue, W workers and one shared HTTP limiter; incremental progress counters and a final summary.

Run the fixed worker pool until the queue drains. Do not drain the queue into unbounded spawned futures. Job success requires successful listing, every listed object reaching a verified terminal success, and all workers joining. Keep failure details in a bounded stream or on disk rather than an ever-growing in-memory vector.

**Acceptance:**

- Mixed object sizes transfer end to end across many listing pages.
- Independently observe `queue_depth <= Q` and `ongoing_http_requests <= H` throughout the run.
- Slow reads, slow writes and slow listing all produce backpressure without starving finalization or deadlocking.
- Worker failure/panic and producer failure propagate; the coordinator does not mistake a closed channel for a successful transfer.
- On a clean run, listed/completed object counts and expected/copied byte totals reconcile.

**Usable core checkpoint:** M0–M3 provide the bucket-copy library with both requested limits, suitable for controlled test buckets.

## M4 Add retries cancellation and integrity reporting

**Deliverables:** bounded retry policy, cancellation behavior, destination verification, structured per-object outcomes and failure report.

Retry transient transport/service failures with bounded backoff. Avoid multiplying application retries by backend retries. Distinguish failures before commit from ambiguous completion responses: inspect/reconcile the destination before retrying an object whose write may already have committed. Use source identity/version conditions where available to prevent mixing bytes from different source generations.

On cancellation, stop listing and starting new work, cancel or drain active work according to one documented policy, and await worker/transport teardown. Preserve completed source and destination objects. Any abort may apply only to this job's own unfinished multipart session; never perform general object or bucket cleanup. Report abandoned multipart sessions when safe abort is unavailable.

**Acceptance:**

- Fault injection covers throttling, timeouts, response loss after commit, source changes, destination collision and cancellation at every blocking point.
- No leaked request permits, detached workers or false success after cancellation.
- Verify destination length and content integrity with a supported trustworthy checksum or bounded destination reread; do not treat multipart ETag or length alone as a content checksum.
- Preserve a machine-readable report for retrying failed objects later. Durable cross-process multipart resume and a persistent job database are follow-up work, not requirements for this initial engine.

## M5 Expose the transfer service and CLI

**Deliverables:** library API, thin CLI and a minimal single-process service adapter, all calling the same engine.

Expose submit, status/progress and cancel. Use job states `pending`, `running`, `succeeded`, `failed`, `cancelled`; reject simultaneous jobs initially. Expose only the two concurrency settings Q and H. Keep credentials in backend configuration/environment or a secret provider, out of job responses and logs. Start the HTTP adapter on loopback for v1; deployment authentication belongs to the hosting integration.

**Acceptance:**

- CLI can transfer between configured test buckets and returns a nonzero exit code for incomplete/failed work.
- Service submits one job, reports progress and cancels it; a second active submission is rejected rather than creating another independent limiter.
- Metrics include queue depth/high-water mark, ongoing HTTP requests/high-water mark, request wait time, listed/completed/failed objects, bytes and retries.
- Process restart semantics are explicit: in-memory jobs are not durable and are never reported as completed after restart.

## M6 Qualify against OCI and publish the runbook

**Deliverables:** opt-in integration suite, bounded workload benchmarks and operational documentation.

Use pre-created isolated OCI source/destination test buckets. Seed fixtures under distinct keys; retain them after tests. Exercise the actual S3-compatible endpoint, pagination, multipart upload and conditional writes. A local fake or S3 emulator cannot establish OCI compatibility.

**Acceptance:**

- Verify every fixture's key, byte content, required metadata and final object count, including empty objects and real directory markers.
- Exercise H=1 and several larger limits with mixed small/large objects; measured requests never exceed H and queue depth never exceeds Q.
- Record throughput, peak memory, request counts, retries and the effects of slow destination/throttling. Use measurements to tune the internal worker/buffer defaults.
- Inventory/listing/verification operations also use the same limiter. Detect identical source/destination bucket aliases before work starts.
- Confirm no source objects or buckets were deleted and no existing destination objects overwritten.
- Document configuration, supported capabilities, source-quiescence requirement, cancellation, incomplete-job recovery and report interpretation.

## Implementation sequence

Land one reviewable PR per milestone: **M0 → M1 → M2 → M3 → M4 → M5 → M6**. M0 acceptance criteria are implemented and exercised; M1–M6 remain pending. No production transfer is part of writing this plan.

Suggested initial modules: `config`, `http_limit`, `listing`, `queue`, `copy`, `engine`, `report`, with thin `cli` and `service` binaries. Begin in one crate; split crates only when a concrete reuse boundary emerges.

Related work: [OCI bucket consolidation](https://github.com/Adcock-AGI-Inc/ml-infra/issues/4001) and [storage transfer service](https://github.com/Adcock-AGI-Inc/ml-infra/issues/4002).
