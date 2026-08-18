# Relational Row-Page Lending Benchmark

`cargo bench --bench relational_row_page_lending` compares the owned
compatibility cursor with the GAT lending cursor over the same immutable
snapshot reader, cache, requested fields, row order, predicate, and output
checksum.

The fixture uses the production relational path to create, checkpoint, and
reopen a `messages` table. Each row has a text primary key, one `BIGINT`
predicate field, and a 1 KiB inline text body. The measured query shape is:

```sql
SELECT body
FROM messages
WHERE owner_bucket >= $1
LIMIT 32;
```

The owned path materializes every projected row before applying the predicate.
The lending path evaluates the predicate against `RelationalValueRef`, stops at
the same limit, and owns only selected output bodies. Selectivity cases cover
1%, 10%, 50%, and 100%. The first three are admission cases; the 100% case is
reported but is not an activation gate because every visited body is final
output and therefore must become owned.

The benchmark installs a process-local counting allocator and reports paired
median elapsed nanoseconds, total allocated bytes, materialized variable-width
payload bytes, scanned and selected rows, representation counters, page pins,
and result checksums. A selective case is admitted when checksums and row counts
match and either:

- elapsed throughput improves by at least 15%; or
- allocated bytes fall by at least 50%.

The point-read safety net compares direct point lookup with the existing owned
exact-range path over identical probes. It uses a paired median and admits at
most a 5% relative regression, with a 100 microsecond total-run noise floor.
Allocated bytes have the same 5% relative budget with a 4 KiB noise floor.
Both paths must produce the same checksum. This is a gross-regression guard for
the unchanged point path, not a claim that exact-range lookup is its historical
latency baseline.

Debug execution uses 4,096 rows and three samples so `cargo test --benches`
remains bounded. Release evidence uses 32,768 rows, eleven alternating samples,
and 2,048 point probes. The emitted JSON protocol is
`skein-relational-row-page-lending-evidence-v1`; a rejected gate exits
non-zero.

## Recorded release evidence

An optimized macOS/arm64 run on 2026-08-18 produced the following paired
medians. These values establish direction for this implementation and are not
portable latency targets:

| Selectivity | Rows scanned | Owned allocation | Lending allocation | Allocation reduction | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1% | 3,200 | 4,189,626 B | 49,594 B | 98.8% | 1.03x |
| 10% | 392 | 514,090 B | 35,690 B | 93.1% | 1.05x |
| 50% | 82 | 108,586 B | 34,426 B | 68.3% | 1.00x |
| 100% | 32 | 43,386 B | 34,426 B | 20.7% | 1.00x |

All checksums matched. The three selective shapes were admitted through the
allocated-byte criterion. The 2,048-probe direct point path was within 0.4% of
the owned exact-range latency and allocated slightly fewer bytes, so the point
safety net was admitted.
