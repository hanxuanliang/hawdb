# Relational OLTP Mix Benchmark

`cargo bench --bench relational_oltp_mix` records the TP-shaped baseline that
the row-page and demand-paged-index contract must not regress
([`specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md)).
Point reads and low-concurrency commits may regress by no more than five
percent at p95. Cold and warm index reads, startup phases, resident bytes, and
background interference are reported separately.

## Shape

The real Nowledge Mem content-store schema (`content_documents`,
`thread_messages`, the order and space secondary indexes), 200 threads ×
250 messages = 50,000 rows loaded in batched transactions, checkpointed, and
reopened so reads pay published-artifact costs. The mixed phase runs 4,000
operations from a deterministic generator: 70% primary-key point reads, 10%
ordered `LIMIT 50` pagination, 15% single-message insert transactions, 5%
content updates. Every mutation commits its own transaction because
per-request durability is the workload's truth. The scan/aggregate class
(space `COUNT(*)`, per-thread `GROUP BY` rollup) is sampled separately.

Before the mixed phase, the harness also executes the three-statement Mem
thread-detail read shape on one pinned read transaction per sample:

1. exact thread-document lookup through `(owner_kind, owner_id)`;
2. `COUNT(*)` plus `SUM(token_count)` for one thread;
3. deterministic `(order_index, content_message_id)` pagination with
   `LIMIT 50` and late hydration of 8 KiB message bodies.

The dedicated thread uses large bodies while the rest of the historical mixed
fixture remains unchanged. The JSON report includes p50/p95/p99 latency,
intermediate rows, hydration rows and bytes, row/index page reads, cache
outcomes, and the ordered-page operator list. The harness fails if the ordered
page reintroduces `TopNExec`; an index-ordered `LimitExec -> ProjectionExec ->
IndexRangeScanExec` path is required. This makes the benchmark evidence useful
for both latency comparison and resource-bound regression diagnosis.

The rollup deliberately cannot yet take its production form — the current
aggregate subset rejects `ORDER BY`, so the "top threads by message count"
query is expressed without the ordering. Closing that subset gap is an
executor capability task, not a storage-layout prerequisite.

## Baseline — current row-oriented engine

Recorded 2026-08-12 on the row-oriented engine before the dedicated thread-read
suite was added (branch
`feat/columnar-canonical-redesign`, macOS/arm64, release profile), 50,000
rows:

| Operation class | ops | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| Point read (PK) | 2,781 | 16 µs | 50 µs | 64 µs |
| Page read (`ORDER BY … LIMIT 50`) | 408 | 346 µs | 421 µs | 459 µs |
| Insert (own transaction, fsync) | 598 | 5.16 ms | 6.35 ms | 7.65 ms |
| Update (own transaction, fsync) | 213 | 7.95 ms | ~13 ms | 13.2 ms |
| Space `COUNT(*)` | 21 | 1.65 ms | 1.76 ms | 1.76 ms |
| Thread rollup (`GROUP BY` over 50k rows) | 21 | 43.8 ms | 45.2 ms | 45.2 ms |

Reading the baseline against the target contract:

- Point reads and mutations are the parity target. Persistent index roots and
  page-cache admission must not add an unbounded commit or residency cost.
- The 43.8 ms rollup against 16 µs point reads is the AP tail addressed by
  required-field row-page decoding, typed batches, metadata summaries, and,
  only when measured, optional derived scan projections.

Numbers are single-machine medians for trend tracking, not representative
Mem-replica qualification; production admission still follows
`PRODUCTION_READINESS_SPEC.md`.
