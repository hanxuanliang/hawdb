# skein-fuzz

`skein-fuzz` is a development-only correctness harness over Skein's public embedded API. The
production `skein` crate does not depend on it.

The generator first creates a deterministic graph state, then chooses query shapes and typed
predicate and query AST nodes that are validated against the generated schema before rendering. A
campaign runs six complementary oracles:

- The plan-differential oracle applies mutations once, pins one read snapshot, and executes the
  same parameterized Cypher query through memo search and deterministic direct fallback. Direct
  fallback is an independent planning path, not a source of truth; agreement cannot detect a bug
  shared by both paths.
- The Graph TLP oracle evaluates `Q`, `Q WHERE p`, `Q WHERE NOT p`, and a constrained
  `Q WHERE nullable_operand IS NULL` partition on one pinned snapshot. The generated predicate is
  a single comparison against a non-null parameter, so the null-operand partition is exactly the
  unknown partition. Under bag semantics, the original rows must equal the multiset union of all
  three partitions. This checks nullable node-property, range, and relationship-property
  predicates without implementing another graph executor.
- The Graph TLP Aggregate oracle runs `count(variable)` over the original match and the same three
  predicate partitions on one pinned snapshot. The original count must equal the checked sum of
  the partition counts. This follows SQLancer's TLP Aggregate construction and exercises Skein's
  aggregate execution path without adding a reference executor or host-side graph semantics.
- The graph-metamorphic oracle executes an identifier-bijection transform for every query and a
  direction-reversal transform whenever the typed AST contains a directed relationship. The
  transformed graph, parameters, and relationship pattern change together; identifier values are
  normalized before comparison. Applicability guards and independent mismatch/error signatures
  keep an unsupported transform or setup failure distinct from a semantic mismatch.
- The SQL TLP oracle creates a deterministic PostgreSQL-style relational schema with primary keys,
  nullable scalar columns, optional indexes, and parameterized inner/left joins. It compares an
  unfiltered SELECT with the bag union of its predicate-true, predicate-false, and predicate-null
  partitions through `Database::query_sql_with_params` on one pinned snapshot. Generated
  predicates cover scalar comparisons, `IN`, column comparisons, and nullable `AND`/`OR`
  composition; each shape is constructed so its unknown partition is non-empty.
- The SQL TLP Aggregate oracle applies `COUNT(*)` to the same generated FROM/JOIN and predicate
  variants. The original count must equal the checked sum of all three partition counts. SQL setup,
  typed parameters, queries, `EXPLAIN` plan rows, evidence, and a fresh-state reduced setup are
  retained in the failure report.

The TLP relations rely on Cypher and SQL three-valued predicate logic: missing or null operands
evaluate to unknown, and `NOT unknown` remains unknown. Duplicate rows, missing values, null
values, and floating-point bit patterns remain distinct in oracle comparisons.

Run a deterministic campaign with:

```console
cargo run -p skein-fuzz -- --seed 7 --cases 128
cargo run -p skein-fuzz -- --seed 7 --case-index 19
```

The command prints a multi-oracle JSON report. Any mismatch exits non-zero and contains the exact
mutations, typed parameters, rendered query AST metadata, all query variants, result semantics,
plan fingerprints, optimizer stages, metamorphic transforms, and a direct reproduction command.
Plan-fingerprint novelty is reported as coverage telemetry and never changes a correctness verdict.
The differential reducer first minimizes graph mutations and then typed query AST nodes. It accepts
a candidate only when the same failure signature still triggers, so a setup, parse, or execution
error cannot replace a semantic mismatch during reduction.

NoREC is intentionally deferred until the supported Cypher or SQL subset can express a general
row-wise `SUM(CASE WHEN predicate THEN 1 ELSE 0 END)` relation without adding a fuzz-only executor
path.

The storage campaign mutates one bounded parser input in a generated graph and search fixture per
case. A clean open or a typed storage error are both valid outcomes; a panic is a failure. Reports
include the target artifact, mutation, case seed, and exact replay command:

```console
cargo run -p skein-fuzz --bin skein-storage-fuzz -- --seed 7 --cases 256
cargo run -p skein-fuzz --bin skein-storage-fuzz -- --seed 7 --case-index 19
```
