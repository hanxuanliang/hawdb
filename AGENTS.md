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
