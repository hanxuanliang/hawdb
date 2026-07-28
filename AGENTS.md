# Agent Notes

## Crate Boundaries

- Do not split crates for their own sake.
- Add a new crate only when it has a clear ownership boundary, dependency-direction benefit, compile-time isolation benefit, or stable reuse contract.
- Keep `skein` as the SQLite-like embedded library facade; internal crates should support that facade instead of becoming accidental production integration points.
