# Agent Notes

## Crate Boundaries

- Do not split crates for their own sake.
- Add a new crate only when it has a clear ownership boundary, dependency-direction benefit, compile-time isolation benefit, or stable reuse contract.
- Keep `skein` as the SQLite-like embedded library facade; internal crates should support that facade instead of becoming accidental production integration points.

## Library-First Integration

- Treat Skein as an embedded Rust database library, similar to SQLite or LanceDB usage from a host process.
- Production Mem integration must call Rust library APIs directly; command-line binaries may exist only as thin developer, fixture, or preflight wrappers over the same library path.
- New readiness, slow-query, blackbox, background-maintenance, and replacement-gate capabilities should expose typed Rust APIs first, then derive JSON or CLI output from those APIs when needed.
- Avoid environment variables, command arguments, helper processes, or shell-out behavior as production control planes.

## Query and Storage Discipline

- Mem route behavior should be expressed as parameterized Cypher and executed through
  the embedded query runtime. Prefer adding a readable query over adding a
  route-specific typed API or handwritten executor branch.
- Add a typed API only when one stable, reusable library contract must coordinate
  multiple statements, a mutation/WAL boundary, recovery, or a capability that
  cannot be represented safely by Cypher alone. Typed APIs must not become a
  convenience wrapper for each REST route.
- Fast paths should be derived from AST or logical-plan shape and remain observable in query reports.
- Keep storage changes recovery-oriented: WAL, checkpoint, pruning, and scan-filter features need targeted tests that prove replay boundaries, torn-tail handling, and no partial mutation recovery.
- Do not add broad indexing, filtering, or optimizer features unless they map to active Mem replacement needs for Kuzu, LanceDB, or the graph-first read path.
