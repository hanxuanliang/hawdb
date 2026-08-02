# skein-fuzz

`skein-fuzz` is a development-only correctness harness over Skein's public embedded API. The
production `skein` crate does not depend on it.

The first oracle executes the same deterministic mutations and parameterized Cypher query against
two independent databases:

- the default memo optimizer;
- the deterministic direct fallback selected with `max_optimizer_groups = Some(0)`.

The direct fallback is an independent planning path, not a source of truth. A matching result only
shows agreement between the two paths. Future oracles should add an independent interpreter and
metamorphic checks.

Run a deterministic Mem-shaped campaign with:

```console
cargo run -p skein-fuzz -- --seed 7 --cases 128
```

The command prints a JSON report. Any mismatch exits non-zero and includes a replay bundle with the
seed, mutations, query, parameters, result semantics, plan fingerprints, optimizer stages, and both
observed outcomes.
