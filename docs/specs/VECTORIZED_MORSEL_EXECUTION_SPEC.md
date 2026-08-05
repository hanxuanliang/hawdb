# Skein Vectorized Morsel Execution Specification

## Scope

This specification defines the storage-neutral columnar batch contract, the
initial vectorized read fragment, morsel resource admission, deterministic
execution, fallback behavior, and performance evidence.

The canonical graph remains row-oriented storage. Columnar data is an executor
projection and MUST NOT become a second durable representation.

## Columnar Batch Contract

`skein-executor` owns the following storage-neutral types:

- dense `SlotId` values and a `BindingSchema`;
- typed `ColumnVector` values with explicit validity;
- `Selection::All`, dense bitmap, and sparse index representations;
- zero-copy projection by sharing immutable column storage;
- typed integer and floating-point comparison kernels.

All-valid columns MUST NOT allocate a validity bitmap. Selection kernels MUST
build the final sparse-index or dense-bitmap representation adaptively rather
than construct both representations for every filtered batch. Storage adapters
MUST retain and clear reusable batch buffers instead of replacing their
allocation after each emission.

Missing and `NULL` values MUST be invalid in the comparison column and MUST NOT
pass a range predicate. Integer-to-float comparison and floating-point ordering
MUST match the row executor, including `f64::total_cmp` behavior for NaN.

The initial production fragment is:

```text
SeqNodeScan -> PropertyCompare -> Project -> optional Limit
```

It is eligible only when:

- the scan has one exact node label;
- the predicate compares one property with an integer or floating-point
  literal;
- the catalog has a public `Int` or `Float` descriptor for that property;
- projection expressions are node id, node property, or literal values.

Eligibility MUST be decided before scanning. An unsupported fragment MUST use
the existing row pipeline for the whole fragment; execution MUST NOT switch
between row and columnar evaluation after emitting output.

The in-memory adapter MUST borrow scan rows and materialize only selected
projected output. The out-of-core adapter MAY own decoded records, but remains
bounded by row and byte batch limits. A completed projection MUST NOT retain
hidden node bindings that are outside the projected result scope.

## Morsel Contract

A morsel is a scheduling unit containing a bounded row range. It is distinct
from the columnar batch representation. Every morsel has a `PipelineId`, stable
ordinal, start row, and row count.

Admission MUST compute the worker upper bound as:

```text
min(requested workers, morsel count, memory budget / bytes per worker)
```

Positive work MUST fail before execution when one worker cannot fit. Empty work
reserves no workers or memory. Morsel output MUST merge by ordinal unless an
operator defines an explicit order.

The current scheduler executes morsels sequentially and is the deterministic
oracle for future parallel execution. Parallel activation MUST use a
host-shared, runtime-governed pool. It MUST NOT create a thread set per query or
per morsel wave. CPU slots, memory, cancellation, result bytes, and storage I/O
depth remain separate admission dimensions.

## Observability

Execution profiles MUST expose:

- columnar batch count;
- columnar input and selected row counts;
- consumed morsel count;
- maximum admitted workers;
- peak active workers.

These fields MUST be available in structured explain/resource output and in the
printable explain-analyze root summary.

## Performance Evidence

`cargo bench --bench executor_vectorization` compares:

1. row `BTreeMap` predicate evaluation against the typed selection kernel;
2. equivalent row and columnar physical plans over an in-memory graph.

Both comparisons MUST verify identical checksums before reporting timings. The
report includes median duration, per-iteration duration, rows per second, and
speedup. Row and columnar samples MUST be paired with alternating execution
order and a measurement window long enough to avoid timer-scale noise.
Performance conclusions MUST use optimized builds and MUST include the
end-to-end result; a faster isolated kernel does not qualify a slower query
pipeline.

CI MUST check compilation, semantics, and deterministic resource bounds. A
fixed speedup threshold is intentionally not a correctness gate because shared
CI hardware is noisy; release qualification records the benchmark artifact on
the target platform.
