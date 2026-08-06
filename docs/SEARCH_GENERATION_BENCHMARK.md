# Search Generation Benchmark

This benchmark compares the bounded `SearchOutOfCoreGenerationWriter` with the
compatibility `SearchIndex` residency path. It is a kernel regression benchmark,
not representative Mem production qualification.

## Workload

- 100,000 documents in strictly increasing ID order
- 512 content bytes per document
- 16-dimensional `f32` embeddings
- two metadata fields per document
- 132,719,007 logical encoded document bytes
- fresh process and fresh database directory for each mode

Run the two modes separately so process high-water RSS remains comparable:

```bash
SKEIN_SEARCH_GENERATION_BENCH_MODE=streaming \
  cargo bench --bench search_generation
SKEIN_SEARCH_GENERATION_BENCH_MODE=resident \
  cargo bench --bench search_generation
```

## Local Result

Measured on 2026-08-06 with an Apple M5 Max, 18 logical CPUs, 36 GiB RAM,
macOS arm64, and Rust 1.97.1. Each value is one release-mode run and is
directional evidence rather than a statistical latency claim.

| Metric | Resident | Streaming | Change |
| --- | ---: | ---: | ---: |
| Elapsed time | 10,329 ms | 9,144 ms | -11.47% |
| Throughput | 9,681 docs/s | 10,935 docs/s | +12.95% |
| Lifetime peak RSS growth | 251,789,312 B | 88,162,304 B | -64.99% |
| Steady RSS growth | 238,780,416 B | 88,162,304 B | -63.08% |
| Minor page faults | 23,923 | 6,610 | -72.37% |
| Major page faults | 0 | 0 | unchanged |
| Resident documents after build | 100,000 | 0 | -100% |

The streaming run produced an 80,760,459-byte immutable generation from a
134,319,015-byte checksummed spool. Its largest document record was 1,340 bytes,
its largest encoded segment was 170,044 bytes, and it retained no full search
documents after reopening. The observed peak RSS was 2.86 times lower than the
resident path while throughput was higher on this workload.

Production admission still requires the separate representative-copy protocol:
multiple samples, P50/P95/P99 query latency, production corpus identity, steady
and peak RSS, page faults, recovery, and release-bound qualification evidence.
