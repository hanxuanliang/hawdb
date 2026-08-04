# skein-fuzz

`skein-fuzz` is a development-only correctness harness over Skein's public embedded API. The
production `skein` crate does not depend on it.

The generator first creates a deterministic graph state, then chooses query shapes and typed
predicate and query AST nodes that are validated against the generated schema before rendering. A
campaign runs three complementary oracles:

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
- The graph-metamorphic oracle executes an identifier-bijection transform for every query and a
  direction-reversal transform whenever the typed AST contains a directed relationship. The
  transformed graph, parameters, and relationship pattern change together; identifier values are
  normalized before comparison. Applicability guards and independent mismatch/error signatures
  keep an unsupported transform or setup failure distinct from a semantic mismatch.

The TLP relation relies on Cypher three-valued predicate logic: missing and null operands evaluate
to unknown, and `NOT unknown` remains unknown. Duplicate rows, missing values, null values, and
floating-point bit patterns remain distinct in oracle comparisons.

Run a deterministic campaign with:

```console
cargo run -p skein-fuzz -- --seed 7 --cases 128
```

The command prints a multi-oracle JSON report. Any mismatch exits non-zero and contains the exact
mutations, typed parameters, rendered query AST metadata, all query variants, result semantics,
plan fingerprints, optimizer stages, metamorphic transforms, and a direct reproduction command.
The differential reducer first minimizes graph mutations and then typed query AST nodes. It accepts
a candidate only when the same failure signature still triggers, so a setup, parse, or execution
error cannot replace a semantic mismatch during reduction.

NoREC is intentionally deferred until the supported Cypher subset can express a general
`SUM(CASE WHEN predicate THEN 1 ELSE 0 END)` relation without adding a fuzz-only executor path.
