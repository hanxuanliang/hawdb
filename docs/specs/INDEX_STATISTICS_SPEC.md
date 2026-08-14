# Index Statistics Specification

## Status

This document defines the v1 optimizer-statistics contract for explicit graph
property indexes. It covers equality, range, and composite node indexes,
checkpoint persistence, WAL churn accounting, optimizer admission, and system
catalog observability. Full-text token distributions and relationship-property
indexes remain outside this contract.

## Motivation

Generic property statistics and index selectivity have different resource
boundaries. Generic statistics may collect bounded histograms for compact
scalar and `VARCHAR` values, but declared `TEXT`, list, map, and mixed groups
are excluded. An explicit index already owns the cost of maintaining its keys,
so Skein may publish payload-free cardinality metadata for that index without
copying indexed values into a second resident statistics structure.

This follows the useful part of Neo4j's split between exact structural counts
and index-local samples. Skein does not require a universal `ANALYZE` scan to
make an explicit equality or composite index costable.

## Sample contract

Each supported `IndexId` may own one `IndexStatisticsSample` containing:

- `index_size`: indexed row references at the sample epoch
- `unique_values`: distinct scalar or composite keys observed in the sample
- `sample_size`: row references represented by the sample
- `updates_since_sample`: index-key mutations applied after the sample epoch

The resident sample MUST NOT retain `Value`, encoded property payload, posting
lists, histograms, or canonical rows. Memory is therefore proportional to the
number of explicit indexes, not the number or size of indexed values.

An exact sample has `sample_size == index_size`. A sampled implementation MAY
use `sample_size < index_size`, but MUST preserve
`unique_values <= sample_size <= index_size`. Empty indexes retain a zero
sample for observability but do not provide an optimizer selectivity estimate.

Equality and range descriptors over the same `(label, property)` may share one
physical ordered index, but their `IndexId` values and samples remain distinct.
Composite samples count complete composite keys; they MUST NOT multiply
independent per-property NDVs when a joint sample is available. Index IDs are
unique across scalar and composite descriptor kinds.

## Collection and mutation

Materialized stores derive exact samples from already-maintained ordered index
keys and posting cardinalities. Collection MUST NOT clone key payloads. Scalar
range declarations backfill and maintain the same ordered value index used by
equality declarations so the sample and range access path describe complete
data.

Live index DDL against an already out-of-core canonical base does not publish a
sample from the resident WAL delta alone. The descriptor remains usable through
the executor's authoritative fallback, but sample publication waits for a
complete rebuild or the bounded external statistics refresh.

An out-of-core checkpoint publishes exact index samples before releasing the
materialized index payload. WAL replay and live mutations do not rewrite the
sampled size or NDV. They increment `updates_since_sample` once for every
supported index whose indexed key changes. A composite update is counted only
when its complete joint key changes or enters/leaves the index.
Mutation accounting computes only the affected `IndexId` values before applying
the write; it MUST NOT clone the old or new canonical row to compare keys.

External advanced-statistics refresh rebuilds every supported scalar and
composite sample from its pinned canonical scan. Index keys use the same
byte-bounded external sort as other statistics facts, including keys for
declared `TEXT` that remain excluded from generic histograms. Index sampling is
charged to `memory_budget_bytes`, `max_generated_facts`, `max_spill_bytes`, and
`max_spill_runs`; one key that cannot fit the memory budget rejects the refresh
before publication. Resident descriptor lookup state and published samples stay
proportional to the number of explicit indexes.

The refresh publishes only if its source commit epoch is still current. A
budget, I/O, decode, or source-epoch failure leaves the prior sample and churn
counters unchanged. Successful publication atomically replaces every supported
sample with an exact current sample and resets `updates_since_sample` to zero.

## Optimizer admission

The optimizer admits a valid non-empty sample while:

```text
updates_since_sample <= max(1, ceil(index_size * 5 / 100))
```

For a sampled index, estimated NDV is
`ceil(unique_values * index_size / sample_size)`, clamped to
`[1, index_size]`. Equality cardinality uses
`ceil(index_size / estimated_ndv)`. Composite seeks use joint index size and
joint NDV.

Once churn exceeds the threshold, the optimizer MUST ignore the index sample
and use the existing generic-statistics or conservative fallback. Staleness
may change plan quality but never index completeness or query correctness.

## Persistence and recovery

Checkpoint records persist samples as derived metadata. Recovery validates the
numeric bounds and removes samples for missing, full-text, or unsupported index
descriptors. Invalid samples are not canonical corruption: the index remains
queryable and the optimizer falls back conservatively.

Checkpoint loading MUST NOT count checkpoint rows as post-sample updates. WAL
records after the checkpoint do increment churn, so reopen reconstructs the
same admission decision as uninterrupted execution.

## Observability

`system.graph_statistics` exposes one `index_sample` row per supported sample,
including index identity, kind, property names, all four counters, and the
derived `stale` flag. Explain traces continue to report the distinct count used
for access-path costing.

## Verification

Required regressions cover:

- scalar and composite descriptor IDs never collide
- exact and sampled NDV estimates preserve numeric bounds
- declared `TEXT` remains absent from generic property statistics while an
  explicit equality index publishes payload-free selectivity
- range declarations backfill the shared ordered value index
- composite samples use joint-key NDV
- out-of-core checkpoint plus WAL replay reconstructs churn and rejects a
  stale sample
- bounded external refresh creates samples for live out-of-core DDL and resets
  stale sample churn without publishing partial state on failure
- checkpoint text and `system.graph_statistics` expose the sample fields
- `SkeinIndexStatistics.tla` checks coherent resampling, exact zero-churn
  samples, update-age accounting, and the bounded optimizer admission rule
