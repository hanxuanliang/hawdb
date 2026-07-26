# Skein TODO

This file tracks the remaining Nowledge Mem replacement work for Skein. The
scope is intentionally product-driven: implement capabilities required to
replace the local Kuzu/Ladybug graph layer and the LanceDB search projection.
Do not expand into general-purpose database features unless a Nowledge Mem route,
query family, or cutover gate requires them.

## P0: Production Replacement Gates

- [ ] Cut active graph reads over without production dual-read compare.
  - Inventory every active Nowledge Mem REST and MCP graph read route that still
    calls Kuzu/Ladybug directly.
  - Route handlers should issue Cypher through the query runtime and select
    `legacy` or `skein` reads through configuration.
  - Do not keep request-time old/new read comparison in production paths.
    Equivalence evidence should come from offline fixtures, preflight bundles,
    and migration reports.
- [ ] Move graph read traffic through the query runtime boundary.
  - Mem should dual-write to Kuzu/Ladybug and Skein from the start of the
    migration window, then choose the read engine through configuration.
  - Keep Kuzu/Ladybug as the default read engine until each active route has
    route-level Skein readiness evidence.
  - Avoid direct hand-written execution paths in application routes when the
    AST, fast-path detector, optimizer, and executor can own the path.
- [ ] Complete dual-engine cutover readiness.
  - Replacement summary must fail closed when route parity, storage recovery,
    background maintenance, search projection parity, or bounded-read coverage is
    missing.
  - Integration readiness must keep legacy Kuzu/Ladybug and LanceDB data
    side-by-side until cutover is proven.
- [ ] Keep Skein embedded-library first.
  - Production Mem integration should start and operate Skein through Rust
    library APIs, similar to SQLite-style embedding.
  - Do not require production command-line wrappers, environment-driven control
    planes, or spawned helper processes for normal operation.
  - Nightly-only import, migration, and diagnostic entrypoints may exist, but
    they must not be required by the production read/write path.
- [ ] Keep sensitive paths and data out of readiness artifacts.
  - Default reports must redact local paths and raw parse or I/O errors.
  - Expose raw local diagnostics only behind explicit debug flags.

## P0: Graph Kernel Compatibility

- [ ] Finish the Nowledge-used Cypher subset.
  - `MATCH`, one-hop and bounded multi-hop patterns.
  - `WHERE` equality, range, boolean, null, and list membership predicates.
  - `RETURN`, aliases, aggregation, ordering, offset, and limit.
  - `CREATE`, `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`.
  - Nowledge schema DDL and migration statements.
- [ ] Keep parser output syntax-only.
  - Parameter binding, catalog lookup, type checks, and semantic validation stay
    outside the parser.
  - Fast paths should be selected from simple AST shape checks, not string
    matching.
- [ ] Strengthen planner, optimizer, and executor ownership.
  - Use Cascades groups, logical rules, implementation rules, physical
    properties, and deterministic costs for non-trivial graph reads.
  - Keep storage-specific choices in catalog metadata and physical rules, not in
    parser or route handlers.
- [ ] Maintain stable Nowledge API behavior.
  - Preserve node, relationship, metadata, pagination, and ordering contracts.
  - Preserve `include_metadata=false` metadata stripping behavior.
  - Compare row shape and error class before allowing replacement readiness.

## P0: Storage and Recovery

- [ ] Keep WAL and checkpoint recovery as cutover blockers.
  - Mutations must recover as whole committed batches or not at all.
  - Torn WAL tails must be detected and bounded.
  - Checkpoint manifests must include replay boundaries.
- [ ] Add storage-level scan pruning where semantics are exact.
  - Equality, numeric range, date/time range, enum/in-list, and unique-key
    summaries should decide whether a segment needs to be read.
  - Bloom or cuckoo filters should be used only for fields where false positives
    are acceptable and false negatives are impossible.
  - Push eligible `WHERE` predicates down to disk scan planning before loading
    record payloads into memory.
  - Store compact per-segment descriptors for fields used by Nowledge filters:
    `unit_type`, `metadata`, `importance`, `confidence`, lifecycle status, and
    latest/history timestamps.
- [ ] Keep memory use bounded by default.
  - User foreground reads are admitted first.
  - Internal background import, projection, compaction, analytics, and shadow
    migration work must be deferrable under resource pressure.
  - Background work should be scheduled through resource classes and QoS limits;
    foreground user requests should not be throttled by internal maintenance.

## P0: Search Projection Replacement

- [ ] Continue replacing LanceDB only as a rebuildable search projection.
  - Canonical facts remain graph/content state, not vector index state.
  - Search projection evidence must prove row count, document identity, embedding
    identity, lifecycle, and incremental watermark parity.
  - Keep content store replacement out of this milestone unless a search or
    graph route needs it.
- [ ] Keep FTS and vector projection maintenance incremental.
  - Full rebuild is a repair path, not the steady-state update mechanism.
  - Background projection updates must respect QoS limits.
  - Metadata and lifecycle filters should be pushed into search candidate
    generation before returning rows to Mem.
- [ ] Replace LanceDB search reads in stages.
  - First cover metadata-filtered search projection reads that do not require
    Kuzu joins.
  - Then cover vector candidate generation, ranking inputs, source chunk
    identity, fail-soft behavior, and repair/rebuild markers.
  - Remove LanceDB from a route only after the matching search projection
    evidence is present in replacement summary.
- [ ] Add retrieval projection options behind advisor gates.
  - Raw float32 or SQ8 remains the safe path.
  - TurboQuant-style compressed projections can be used for cold or constrained
    local segments only after recall and parity evidence is available.

## P1: Operability

- [x] Add compact readiness dashboards for route, query-family, storage, search,
  and background-maintenance blockers.
- [x] Add stable counters for plan cache hit, miss, admission, eviction, and
  memory pressure.
- [x] Add concurrency model and race-oriented tests for shared library state.
  - [x] Cover bounded LFU plan cache invariants with a feature-gated Loom model
    in CI.
  - [x] Cover local QoS scheduler background admission and permit accounting
    with a feature-gated Loom model in CI.
  - [x] Cover bounded slow-query ring capacity and sequence invariants with a
    feature-gated Loom model in CI.
  - [x] Cover the embedded Mem library handle with a multi-threaded query,
    slow-query, and readiness-dashboard access test.
- [ ] Add library readiness APIs for Mem integration.
  - Expose structured readiness, slow-query, blackbox, storage-recovery,
    background-maintenance, and search-projection reports through Rust APIs.
  - [x] Expose typed library readiness and search-projection evidence summaries
    for embedded Mem callers.
  - [x] Expose typed storage-recovery readiness summaries through the embedded
    Mem library facade.
  - [x] Expose typed background-maintenance readiness summaries through the
    embedded Mem library facade.
  - [x] Expose redacted typed slow-query summaries through the embedded Mem
    library facade.
  - [x] Expose typed blackbox manifests, artifacts, and events through Rust
    library APIs while preserving redacted JSON output.
  - [x] Expose replacement summary gate generation through Rust library APIs
    and keep the CLI as a thin wrapper.
  - Keep reports compact and redacted by default so production can keep them on.
  - CLI tools may wrap library APIs for developer workflows, but must not be the
    only supported interface.
- [x] Add explain output that includes semantic checks, selected fast path,
  optimizer budget, chosen indexes, scan-pruning decisions, and resource class.
- [ ] Add typed preflight or harness APIs for all replacement artifacts
  so Python-only validation scripts can be retired from the critical path.
  - `nowledge-query-runtime-preflight` runs JSON-defined probes through the
    read-only query runtime with `EXPLAIN ANALYZE` and emits plan/profile
    evidence without rows, parameters, or local paths.
  - [x] Expose typed query-runtime preflight probes and reports through the
    embedded Mem library facade.
  - [x] Expose previous-wrapper preflight checks as typed Rust library reports
    and keep the CLI as a thin wrapper.
  - [x] Expose Nowledge Mem integration readiness as a typed Rust library gate
    and keep the CLI as a thin wrapper.
  - [x] Expose query-family evidence generation through Rust library APIs and
    keep the CLI as a thin wrapper.
  - [x] Expose graph-route readiness generation through Rust library APIs and
    keep the CLI as a thin wrapper.
  - [x] Expose graph-route evidence generation through Rust library APIs and
    keep the CLI as a thin wrapper.
  - [x] Expose bounded-read evidence parsing and generation through Rust
    library APIs and keep the CLI as a thin wrapper.
  - [x] Expose background-maintenance evidence generation through Rust library
    APIs and keep the CLI as a thin wrapper.
  - [x] Expose storage-recovery evidence generation through Rust library APIs
    and keep the CLI as a thin wrapper.
  - [x] Expose Nowledge Mem library readiness command plumbing through Rust
    library APIs and keep the CLI as a thin wrapper.
  - Do not add new query-shape-specific typed APIs unless they are required for
    compatibility with an existing caller during migration.

## P1: Performance From Architecture

- [ ] Improve statistics maintenance.
  - Prefer incremental label, relationship, distinct-value, and degree summaries
    once correctness is proven.
  - Keep full rebuild as a validation and repair tool.
- [ ] Improve adjacency and index layout for read-heavy local workloads.
  - Optimize for bounded memory and predictable read amplification.
  - Keep row-oriented canonical records and add projection/index layouts only
    where route evidence proves value.
- [ ] Add workload fixtures based on real Nowledge routes before low-level
  tuning.
  - Benchmark graph reads, bounded expansions, metadata-filtered search, and
    mixed foreground/background workloads.

## P2: Deferred Capabilities

- [ ] Advanced graph algorithms beyond Nowledge's active routes.
- [ ] Broad openCypher compatibility not exercised by Nowledge Mem.
- [ ] Distributed storage, replication, or cloud-primary execution inside the
  local embedded engine.
- [ ] Aggressive SIMD work unless route-level evidence shows it is needed.
