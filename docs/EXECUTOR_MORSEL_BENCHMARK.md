# Executor Morsel Benchmark

This benchmark separates the isolated shared-pool scheduler from the complete
numeric scan, filter, projection, and streaming-consumer pipeline. It is a
local kernel diagnostic and always reports `production_eligible=false`. It does
not replace the representative-replica qualification protocol.

## Modes

The scheduler mode measures bounded ordered morsel scheduling without graph
storage or result projection:

```bash
SKEIN_EXECUTOR_BENCH_MODE=scheduler \
  SKEIN_MORSEL_BENCH_WORKERS=4 \
  cargo bench --bench executor_vectorization
```

The morsel mode uses the default execution batch size, streams rows into a
consumer, verifies the serial and parallel checksums, and records the observed
admitted and active worker counts:

```bash
for workers in 4 8 16; do
  SKEIN_EXECUTOR_BENCH_MODE=morsel \
    SKEIN_MORSEL_BENCH_WORKERS="$workers" \
    SKEIN_MORSEL_BENCH_ROWS=262144 \
    cargo bench --bench executor_vectorization
done
```

`SKEIN_MORSEL_BENCH_ROWS` is optional. The default gives each requested worker
sixteen morsels. An explicit row count is rejected when it cannot activate the
requested worker count under the default four-morsels-per-worker admission
rule. The fixture has a numeric predicate field and a 256-byte non-projected
payload so the scan retains a production-shaped resident row width.

## Local Result

Measured on 2026-08-06 with an Apple M5 Max, 18 logical CPUs, 36 GiB RAM,
macOS arm64, and Rust 1.97.1. Each worker profile ran in a fresh process over
262,144 rows with three alternating serial/parallel samples. These values are
directional evidence, not a statistical release claim.

| Workers | Active workers | Serial P50 | Parallel P50 | Parallel rows/s | Speedup | Parallel P99 | Peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 4 | 7.157 ms | 5.570 ms | 47.07 M | 1.28x | 5.614 ms | 945,504,256 B |
| 8 | 8 | 7.605 ms | 3.758 ms | 69.75 M | 2.02x | 3.835 ms | 941,260,800 B |
| 16 | 16 | 5.615 ms | 3.689 ms | 71.06 M | 1.52x | 3.726 ms | 941,309,952 B |

The isolated four-worker scheduler processed 1,048,576 rows at 2.45x the
sequential median. The complete pipeline remained faster at every worker count,
while 8 to 16 workers showed only a small throughput increase on this 18-core
host. This supports the existing CPU and work admission caps rather than a
larger unconditional default.

Production admission remains open until `run_production_morsel_profile` records
at least three warmups and 100 measurements for each of 4, 8, and 16 workers on
the same representative read-only replica and query. The three artifacts must
be process-isolated and pass throughput, P99, peak-RSS, cancellation, foreground
admission, and permit-leak gates through `evaluate_production_morsel_matrix`.
