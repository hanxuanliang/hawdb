# Skein

Skein is an embedded Rust graph database intended for the Nowledge local graph
data plane. It uses Cypher as its query language and a Cascades-style optimizer
for deterministic, explainable planning.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current design.
The staged implementation and compatibility gates are tracked in
[docs/EMBEDDED_DEVELOPMENT_PLAN.md](docs/EMBEDDED_DEVELOPMENT_PLAN.md).
Open development work is tracked in [TODO.md](TODO.md).

## Production Boundary

Skein is intended to be embedded by Mem as a Rust library. Production callers
should open Skein in-process and consume typed readiness APIs such as
`nowledge_mem_final_cutover_preflight`; they should not shell out to the `skein`
binary for read routing, migration gates, or previous-wrapper comparison.

Compatibility commands that execute external previous-wrapper or shadow compare
processes are quarantined as developer/preflight tools. They require
`SKEIN_ENABLE_COMPATIBILITY_TOOLS=1` and are only for isolated CI, release, or
nightly validation against copied data.
