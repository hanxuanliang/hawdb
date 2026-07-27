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
    Equivalence evidence should come from offline fuzz harnesses, fixtures, and
    preflight bundles.
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

## P0: Concrete Cutover Blockers

These items are the current kernel and integration gaps that block declaring
Skein a production replacement for Nowledge Mem's Kuzu/Ladybug graph layer and
LanceDB search projection. They do not block incremental development, but they
must block default cutover.

- [ ] Complete route-by-route production read ownership.
  - Every active Mem graph/search read route must select `legacy` or `skein`
    through the embedded Rust library runtime.
  - Each route must have route-level execution evidence tied to the shared
    covered-route catalog.
  - Request-time dual-read compare must stay out of production handlers; parity
    belongs to dedicated readiness, preflight, or migration endpoints.
  - Completion evidence: replacement summary rejects stale or missing route
    evidence, and Mem can run the route with `skein` selected without direct
    Kuzu/Ladybug reads.
- [ ] Close the LanceDB search projection replacement loop.
  - Candidate reads must prove FTS, vector, source chunk identity, embedding
    identity, lifecycle filtering, metadata predicate pushdown, fail-soft
    behavior, rebuild markers, repair markers, and incremental watermarks.
  - Metadata filters for `unit_type`, lifecycle state, `importance`,
    `confidence`, timestamps, and required metadata keys must prune before rows
    are returned to Mem.
  - Completion evidence: search projection, search candidate, and Rust-bridge
    shadow evidence all pass fail-closed replacement gates without CLI-only
    glue.
- [ ] Finish exact storage scan pruning for production filters.
  - Segment descriptors must cover equality, enum/in-list, numeric range,
    date/time range, null/missing, existence, normalized default equality, and
    unique-key filters used by Nowledge.
  - Scan planning must decide whether to read a segment before loading row
    payloads into memory.
  - Bloom or cuckoo filters may only be used where false positives are safe and
    false negatives are impossible.
  - Completion evidence: `EXPLAIN ANALYZE` and readiness reports show payload
    read avoidance for graph and search projection filters.
- [ ] Prove storage recovery under real mutation shapes.
  - WAL replay must recover whole committed batches or nothing.
  - Torn WAL tails, checkpoint boundaries, and replay markers must be detected
    and surfaced through typed readiness APIs.
  - Completion evidence: storage recovery readiness fails closed on missing or
    contradictory WAL/checkpoint evidence and passes replay fixtures that match
    Mem writes.
- [ ] Make resource control a cutover gate.
  - Foreground user reads should be admitted ahead of background import,
    projection, compaction, analytics, and migration tasks.
  - Background work must be bounded by resource class, memory budget, and QoS
    limits on consumer hardware.
  - Completion evidence: background-maintenance readiness and blackbox reports
    show admission, deferral, memory-pressure behavior, and slow-query signals.
- [ ] Keep the Mem integration library-only on the production path.
  - Mem must start and operate Skein in-process through Rust APIs.
  - Production readiness, route evidence, search projection evidence, slow log,
    blackbox, storage recovery, and maintenance reports must all have typed Rust
    API entrypoints.
  - CLI tools may remain thin developer wrappers, but no production route or
    gate may require shelling out.
- [ ] Preserve replacement boundaries.
  - Kuzu/Ladybug graph and LanceDB search projection are the replacement scope.
  - SQLite content store and large blob/value storage remain external unless a
    Nowledge graph/search route requires a narrower value-store API.
  - Completion evidence: replacement summary distinguishes graph replacement,
    search projection replacement, and out-of-scope content storage.

## P0: PR-Sized Cutover Goals

Use these as concrete PR boundaries. Each PR should be reviewable on its own,
have a single owner boundary, and leave replacement readiness stricter or more
complete than before. Avoid bundling Mem route changes, Skein kernel changes,
and readiness gate changes unless the PR explicitly proves the end-to-end
contract.

- [ ] PR 1: Define the active Mem route cutover inventory.
  - Scope: generate or update the shared route catalog for active graph/search
    read routes, including REST and MCP surfaces.
  - Deliverables: route list, legacy/Skein ownership field, required evidence
    kind, and stale-evidence invalidation rule.
  - Acceptance: replacement summary fails closed when a route is missing,
    renamed, or not covered by evidence.
  - [x] Add a shared graph read route catalog version and stable digest to
    query-runtime preflight, graph-route readiness, bounded-read evidence, and
    integration bundle alignment so stale route evidence fails closed after a
    catalog change.
  - [x] Require query-runtime preflight and graph-route readiness alignment to
    validate route catalog version and digest, so stale live evidence or stale
    replacement-summary evidence cannot pass integration readiness.
  - [x] Require graph-route scan pruning evidence to carry the real typed
    `target_kind` (`node` or `relationship`) and reject legacy-only
    `record_kind` relationship pruning claims.
- [ ] PR 2: Remove direct Kuzu reads from one low-risk graph read route.
  - Scope: move one existing read route to the embedded query runtime with
    `legacy`/`skein` selection controlled by configuration.
  - Deliverables: typed route result, no request-time dual-read compare, and a
    dedicated offline fuzz/parity harness.
  - Acceptance: the route can run with `skein` selected through the library
    runtime and legacy remains available as configured fallback.
  - [x] Expose a deterministic library-only query fuzz harness for Nowledge
    graph-read shapes so CI can exercise parser, query runtime, scan pruning,
    plan-cache reporting, and system hints without production dual-read compare.
- [ ] PR 3: Repeat route runtime migration for the remaining graph-first reads.
  - Scope: migrate overview, node details, expansion, shortest path,
    communities, PageRank plan, augmentation state, orphans, and related
    community reads in small route groups.
  - Deliverables: route-group PRs that remove direct application-side graph
    execution and preserve response shape.
  - Acceptance: every migrated group adds route evidence and keeps production
    handlers free of request-time shadow compare.
- [ ] PR 4: Add search projection scan-pruning evidence for production filters.
  - Scope: prove search projection segment descriptors prune `unit_type`,
    lifecycle state, metadata keys, `importance`, `confidence`, and timestamps
    before returning rows to Mem.
  - Deliverables: typed pruning report, `EXPLAIN ANALYZE` evidence, and
    fail-closed readiness checks for missing field summaries.
  - Acceptance: replacement summary rejects search candidate cutover when
    pushed predicates lack segment-level pruning evidence.
  - [x] Require search projection descriptor summaries to expose non-zero
    summary counts for declared value, numeric-range, and timestamp-range scan
    pruning capabilities.
- [ ] PR 5: Close LanceDB candidate read replacement.
  - Scope: run FTS, vector, source chunk identity, embedding identity,
    fail-soft, rebuild marker, repair marker, and incremental watermark evidence
    through typed Rust APIs.
  - Deliverables: Rust-bridge shadow evidence consumed by Mem without
    hand-built cutover JSON.
  - Acceptance: LanceDB candidate replacement gates pass only when all typed
    evidence areas are present and internally consistent.
  - [x] Allow search-candidate shadow probes to carry structured scan-pruning
    field summaries and reject declared summary capabilities whose segment
    counts are zero.
- [ ] PR 6: Prove WAL and checkpoint recovery with Mem-shaped mutations.
  - Scope: add fixtures for committed batch replay, torn WAL tail handling, and
    checkpoint replay boundaries that match Mem graph/search writes.
  - Deliverables: typed storage recovery readiness and negative fixtures.
  - Acceptance: recovery readiness fails closed on missing or contradictory
    WAL/checkpoint evidence and passes Mem-shaped replay cases.
  - [x] Add a Mem-shaped graph recovery fixture that checkpoints Source,
    Thread, and Memory state, replays a post-checkpoint Memory-to-Entity
    relationship WAL batch, and verifies typed storage recovery readiness.
  - [x] Add a Mem-shaped search projection recovery fixture that reopens graph
    and search paths through the embedded library and verifies checkpointed
    segment descriptors still prune lifecycle and numeric candidate filters.
  - [x] Add a storage recovery evidence negative fixture that rejects reports
    whose raw checkpoint, WAL replay, or torn-tail fields contradict claimed
    readiness.
- [x] PR 7: Make background QoS a readiness gate.
  - Scope: connect resource classes for import, projection, compaction,
    analytics, and migration work to typed background-maintenance readiness.
  - Deliverables: foreground-first admission evidence, background deferral
    counters, memory-pressure behavior, and compact blackbox events.
  - Acceptance: production readiness blocks when background tasks can starve
    foreground user reads or exceed configured memory budgets.
  - [x] Add a background-maintenance memory-pressure gate that fails closed
    when reported internal background work exceeds its configured memory budget.
  - [x] Add compact blackbox background QoS events with redacted admission,
    search-projection delta, memory-pressure, and blocker-code evidence.
- [ ] PR 8: Make Mem's Skein startup and readiness library-only.
  - Scope: ensure Mem starts Skein in-process and consumes typed readiness,
    route evidence, search projection evidence, slow log, blackbox, recovery,
    and maintenance APIs.
  - Deliverables: no production shell-out, CLI wrappers remain thin developer
    adapters, and production config selects read engine explicitly.
  - Acceptance: a Mem integration test can initialize Skein as a library and
    evaluate cutover gates without invoking Skein CLI tools.
- [ ] PR 9: Add the final cutover preflight bundle gate.
  - Scope: combine route coverage, graph evidence, LanceDB projection evidence,
    storage recovery, background QoS, redaction, and library-only integration
    into one fail-closed integration readiness report.
  - Deliverables: typed final preflight report plus compact redacted JSON for
    diagnostics.
  - Acceptance: `production_cutover_ready=true` is impossible unless every
    blocker above is proven by current, route-matching evidence.
  - [x] Require blackbox redaction evidence in the Mem integration readiness
    gate so retained diagnostics cannot copy raw query text, parameters,
    artifact payloads, or absolute artifact paths.
  - [x] Require blackbox operational evidence for slow-query logs and compact
    background QoS summaries before integration readiness can pass.
  - [x] Require bounded-read payload budget evidence in replacement summary,
    integration bundle alignment, and integration readiness.
  - [x] Require final previous-wrapper preflight to validate background graph
    delta QoS counts and memory-pressure evidence from replacement summary.
  - [x] Require final previous-wrapper preflight to validate query-runtime
    route catalog version and digest so stale route coverage cannot pass the
    final cutover gate.
  - [x] Require final previous-wrapper preflight to validate replacement
    summary route catalog metadata for bounded-read, graph-route, and
    query-runtime evidence.
  - [x] Require final previous-wrapper preflight to recompute bounded-read
    payload budget, row cap, streaming, blocking-operator, route-readiness, and
    pruning evidence from replacement summary instead of trusting `ready=true`.
- [ ] PR 10: Remove or quarantine obsolete compatibility paths.
  - Scope: after gates are satisfied, remove unused CLI-only, Python-only, and
    request-time shadow compare code from the production path.
  - Deliverables: deleted or nightly-only compatibility entrypoints and updated
    documentation.
  - Acceptance: production Mem still supports explicit legacy read selection
    during the migration window, but no stale compatibility helper bypasses the
    query runtime or readiness gates.

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
    `unit_type`, `metadata`, `importance`, `confidence`, `lifecycle_state`,
    and latest/history timestamps.
  - [x] Push exact graph `IS NOT NULL` property-existence predicates through
    scan planning using the property index before row payload filtering.
  - [x] Push exact graph `IS NULL` missing-or-null predicates through scan
    planning by subtracting non-null property-index candidates before row
    payload filtering.
  - [x] Push exact graph normalized default equality predicates such as
    `CASE WHEN space_id IS NULL OR space_id = '' THEN 'default' ELSE space_id END`
    through scan planning when the comparison is an equality.
    - [x] Cover parameterized Nowledge thread-space predicates such as
      `$source_space_id` with `EXPLAIN ANALYZE` scan-pruning evidence.
    - [x] Push parameterized normalized default inequality predicates such as
      `$target_space_id` through exact scan pruning for thread move reads.
  - [x] Maintain an in-memory relationship-property scan-pruning index for
    exact equality, enum/in-list, missing/null, existence, normalized default,
    and range filters so relationship payload scans can be narrowed before row
    filtering.
  - [x] Push single-relationship-variable `WHERE` predicates that can be
    represented as exact relationship `PropertyFilter`s into relationship scan
    pruning while preserving the original residual predicate evaluation.
  - [x] Expose scan-pruning target kind and relationship type id in execution
    profile reports, and require target kind in query-runtime preflight gates.
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
  - [x] Persist segment descriptor summaries for the fixed Nowledge search
    projection scan-filter fields, including explicit empty summaries for
    missing metadata fields so equality and range filters can be pruned before
    loading row payloads.
  - [x] Require `lifecycle_state` in the search projection scan-filter
    descriptor contract so deleted/forgotten filters are covered by readiness
    evidence.
  - [x] Require search projection segment descriptor evidence to include
    numeric min/max summaries for `importance` and `confidence`, plus timestamp
    min/max summaries for Nowledge history/latest time filters.
  - [x] Expose typed segment-pruning document counts so search projection
    reports can prove row-payload avoidance for numeric and timestamp filters.
  - [x] Require replacement and integration readiness to independently
    validate segment-pruned document counts instead of trusting pushdown
    `ready=true`.
  - [x] Carry segment-pruned document counts from real search projection probes
    through shadow evidence and final preflight reports.
- [ ] Replace LanceDB search reads in stages.
  - First cover metadata-filtered search projection reads that do not require
    Kuzu joins.
    - [x] Expose a typed embedded-library search candidate API that returns
      `SearchResultSet` plus compact predicate-pushdown and segment-pruning
      report data without requiring graph context expansion.
    - [x] Cover vector candidate generation through the same typed
      embedded-library API, including compact ranking-input diagnostics without
      copying embedding values into reports.
    - [x] Cover source chunk identity through the same typed embedded-library
      API and compact diagnostics, preserving `kind`, `external_id`, and
      `source_id` without copying document bodies into reports.
    - [x] Cover fail-soft candidate behavior through compact fallback and empty
      reason codes so Mem can distinguish retriever leg failures from empty
      result sets without parsing error strings.
    - [x] Cover repair/rebuild markers in the same compact candidate report,
      including no-hit responses, without copying marker reasons or document
      bodies into reports.
    - [x] Expose typed search candidate replacement readiness through Rust
      library APIs so Mem can fail closed on missing pushdown, retriever,
      source-chunk identity, fail-soft, or projection-marker evidence without
      shelling out to CLI probes.
    - [x] Expose a library helper that turns legacy primary candidate IDs plus
      Skein candidate output into Rust-bridge shadow evidence, so Mem does not
      need to hand-build cutover JSON for candidate reads.
    - [x] Expose embedded-store and search-projection library APIs that run the
      Skein candidate read and return Rust-bridge shadow evidence in one call.
    - [x] Require Rust-bridge search candidate shadow evidence in replacement
      summary before reporting production cutover readiness.
    - [x] Require Rust-bridge search candidate shadow evidence in final
      previous-wrapper preflight before treating LanceDB candidate replacement
      as production-ready.
    - [x] Require typed search candidate pruning capability evidence in final
      previous-wrapper preflight so value, numeric-range, and timestamp-range
      descriptor gaps fail closed.
    - [x] Recompute search projection and search projection shadow raw fields
      in embedded-library readiness so forged `ready=true` evidence cannot
      bypass LanceDB replacement gates.
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
  - [x] Expose typed search candidate replacement readiness through Rust
    library APIs for route-level LanceDB replacement gates.
  - [x] Include search-candidate shadow evidence as a first-class
    embedded-library readiness area so Mem fails closed before LanceDB candidate
    read cutover.
  - [x] Recompute search-candidate shadow evidence raw fields in library
    readiness instead of trusting `ready=true`.
  - [x] Recompute search-projection and search-projection-shadow raw fields in
    library readiness instead of trusting `ready=true`.
  - [x] Include graph-route readiness as a first-class embedded-library
    readiness area so production callers fail closed before graph read cutover.
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
  - [x] Route query-runtime preflight command plumbing through the embedded Mem
    library facade so CLI and in-process callers use the same execution path.
  - [x] Expose previous-wrapper preflight checks as typed Rust library reports
    and keep the CLI as a thin wrapper.
  - [x] Expose Nowledge Mem integration readiness as a typed Rust library gate
    and keep the CLI as a thin wrapper.
  - [x] Expose Nowledge Mem integration bundle generation through Rust library
    APIs and keep the CLI as a thin wrapper.
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
  - [x] Expose search-candidate shadow evidence parsing through Rust library
    APIs and keep the CLI as a thin wrapper.
  - [x] Expose search-projection probe and evidence command plumbing through
    Rust library APIs and keep the CLI as a thin wrapper.
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
