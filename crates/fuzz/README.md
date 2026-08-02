# skein-fuzz

`skein-fuzz` is a development-only correctness harness over Skein's public embedded API. The
production `skein` crate does not depend on it.

The generator first creates a deterministic graph state, then chooses query shapes and typed
predicate AST nodes that are valid for that state. A campaign runs two complementary oracles:

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

The TLP relation relies on Cypher three-valued predicate logic: missing and null operands evaluate
to unknown, and `NOT unknown` remains unknown. Duplicate rows, missing values, null values, and
floating-point bit patterns remain distinct in oracle comparisons.

Run a deterministic campaign with:

```console
cargo run -p skein-fuzz -- --seed 7 --cases 128
```

The command prints a multi-oracle JSON report. Any mismatch exits non-zero and contains the exact
mutations, typed parameters, all query variants, result semantics, plan fingerprints, optimizer
stages, and a direct reproduction command. Each oracle runs its own bounded mutation reducer and
accepts a candidate only when the same failure signature still triggers; a setup error cannot
replace a semantic mismatch during reduction.

NoREC is intentionally deferred until the supported Cypher subset can express a general
`SUM(CASE WHEN predicate THEN 1 ELSE 0 END)` relation without adding a fuzz-only executor path.
