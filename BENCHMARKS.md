# Read-path benchmark

The ignored `read_path_benchmark` integration test copies four 64 MiB objects
through the same mock S3 transport on the baseline and optimized implementations.
It generates 64 KiB HTTP body frames lazily, simulates 2 ms request latency, and
checks every uploaded byte. `BACKING_RATIO` controls how much backing allocation
a frame retains: 1 models tightly packed frames; 8 models a small slice retaining
a larger receive buffer. This is a synthetic transport test, not an OCI or TLS
throughput measurement.

Measured on macOS ARM64 with Rust 1.97.1, release builds, four shared HTTP permits,
and one fresh process per run. The table contains the median of three runs per
variant; process peak RSS comes from `/usr/bin/time -l`.

| Frame backing | Baseline MiB/s | Optimized MiB/s | Baseline peak RSS MiB | Optimized peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| 1× payload | 378.7 | 399.6 | 110.5 | 133.9 |
| 8× payload | 317.5 | 378.8 | 794.2 | 135.2 |

For both shapes, source GETs remain 32 and verification GETs fall from 32 to 4
(one per object, excluding retry requests). Compacting frames prevents their
larger backing allocations from being retained through uploads. The one-part
prefetch costs additional memory for tightly packed frames; this benchmark does
**not** claim universally lower RSS or predict production throughput.

## Reproduce

```sh
cargo test --release --locked --test transfer --no-run
BACKING_RATIO=8 /usr/bin/time -l \
  target/release/deps/transfer-<hash> \
  --exact read_path_benchmark --ignored --nocapture
```

Repeat with `BACKING_RATIO=1`. For the baseline, use commit `f1324e8`, copy the
same `tests/transfer.rs` and `tests/common/bucket.rs` fixtures into that checkout,
and run only the ignored benchmark (its other tests exercise the new behavior).
Use separate binaries/processes for each variant and avoid compilation or other
CPU-heavy work during measurement.
