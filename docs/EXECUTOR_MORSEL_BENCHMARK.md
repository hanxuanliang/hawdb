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

The adjacency mode measures the Mem-shaped `LIMIT 50` one-hop expansion at
degrees 1, 32, 1,024, and 100,000 without running the numeric or scheduler
benchmarks:

```bash
SKEIN_EXECUTOR_BENCH_MODE=adjacency \
  cargo bench --bench executor_vectorization
```

The plan is `LimitExec -> ProjectExec -> AdjacencyExpandExec -> IndexNodeSeek`,
matching Mem's exact-identity start-node shape rather than charging an unrelated
full seed scan to the degree measurement.
The benchmark requires the expansion report to visit and return exactly
`min(degree, 50)` nodes and edges, keeps transfer batches at 16 rows, rejects
blocking operators, and requires the query memory ledger to return to zero.
It reports P50/P95/P99 latency, tracked query memory, batch payload, RSS, and
page faults for every degree. Debug builds stop at degree 1,024 so local test
smokes remain bounded; the release benchmark is the performance evidence that
includes degree 100,000.

### Bounded adjacency spot check

Measured on 2026-08-17 with an Apple M5 Max and the release profile. Each
degree used three warmups followed by eleven samples of 64 executions.

| Degree | Returned / expanded edges | P50 | P95 | Query-ledger peak | Batch peak |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 3.102 us | 3.457 us | 1,020 B | 1 row |
| 32 | 32 | 13.757 us | 14.503 us | 12,978 B | 16 rows |
| 1,024 | 50 | 27.634 us | 27.996 us | 12,978 B | 16 rows |
| 100,000 | 50 | 843.990 us | 858.343 us | 12,978 B | 16 rows |

The executor and consumer boundary is bounded: output, expanded edges, batch
rows, and ledger completion do not grow beyond the configured limits. The
100,000-degree latency is nevertheless not constant. An in-memory live
adjacency group currently materializes and sorts compact `(NodeId, RelId)`
keys before its stable ordered cursor can emit the first row. That temporary
vector is protected by `blocking_operator_bytes`, but is not represented in
the query-ledger peak above. The benchmark therefore records the fixture's
ordering-key upper bound explicitly and must not be read as proof that the
complete storage-to-consumer path has constant memory or latency. A follow-up
needs to move live ordering state under the query ledger and replace the
whole-group sort with a bounded order-preserving cursor; canonical persisted
adjacency already streams in stable order.

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

### Typed reorder-window spot check

After moving eligible parallel output from per-row `Binding` maps to typed
`ColumnarBatch` values, the morsel mode was rerun on 2026-08-17 with the default
four-worker, 262,144-row workload. Three paired samples reported a 9.092 ms
serial P50 and 5.948 ms parallel P50, or 44.07 million parallel rows/s and a
1.53x speedup. The bounded stream observed four active workers, four buffered
outputs, four reorder entries, and 133,440 peak buffered output bytes. The
checksums matched and the run remained explicitly non-production evidence.

This spot check demonstrates that the compact transport preserves a positive
end-to-end direction while making its retained queue bytes observable. It does
not replace the process-isolated 4/8/16 qualification matrix below.

Production admission remains open until `run_production_morsel_profile` records
at least three warmups and 100 measurements for each of 4, 8, and 16 workers on
the same representative read-only replica and query. The three artifacts must
be process-isolated and pass throughput, P99, peak-RSS, cancellation, foreground
admission, and permit-leak gates through `evaluate_production_morsel_matrix`.
