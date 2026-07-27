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
  - [x] Expose a typed library-readiness redaction summary proving query text,
    parameters, and local paths are not copied into the default report.
  - [x] Require integration readiness to validate library-readiness redaction
    fields so copied query text, parameters, or local paths block cutover.
  - [x] Require query-runtime preflight redaction evidence so copied rows,
    parameters, local paths, or raw errors block cutover.
  - [x] Require final previous-wrapper preflight to validate query-runtime and
    library redaction evidence before release summaries can pass.
  - [x] Redact final previous-wrapper preflight JSON input read and parse
    failures by default so local paths and raw payload fragments are not copied
    into command errors.
  - [x] Redact integration-readiness bundle input read and parse failures by
    default so final cutover diagnostics do not copy local paths or payload
    fragments.
  - [x] Redact integration-bundle input read and parse failures by default so
    bundle assembly does not copy local paths or payload fragments into command
    errors.
  - [x] Redact query-runtime preflight probe JSON parse failures by default so
    Cypher text and parameters are not copied into command errors.
  - [x] Redact library-readiness JSON parse failures by default so nested
    evidence payload fragments are not copied into command errors.
  - [x] Redact bounded-read evidence JSON read and parse failures by default so
    local paths and query payload fragments are not copied into command errors.
  - [x] Redact graph-route query evidence and query-family evidence JSON parse
    failures by default so Cypher and replacement-readiness payload fragments
    are not copied into command errors.
  - [x] Redact fixture-contract input and graph-route readiness evidence parse
    failures by default so local paths, command stdout, and payload fragments
    are not copied into command errors.
  - [x] Redact Nowledge inventory filesystem failures by default so migration
    coverage diagnostics do not copy local paths or source payload fragments
    into command errors.

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

- [x] PR 1: Define the active Mem route cutover inventory.
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
  - [x] Expose a typed route ownership readiness API so Mem can declare each
    active graph/search read route as `legacy` or `skein`; production cutover
    fails closed when required routes are missing, duplicated, conflicting,
    still legacy-owned, or Skein-owned without route readiness evidence.
  - [x] Wire route ownership evidence into the integration bundle and final
    cutover preflight so route coverage is not ready until every required route
    is explicitly Skein-owned through the embedded library runtime.
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
  - [x] Add a library-first Skein read runtime entrypoint for the low-risk
    `/graph/overview` memory ranking shape, returning typed rows and bounded
    route execution evidence without request-time dual-read compare.
  - [x] Provide a typed `/graph/overview` route-query evidence helper that reuses
    the shared overview Cypher contract and feeds graph-route readiness without
    production request-time dual-read compare.
- [ ] PR 3: Repeat route runtime migration for the remaining graph-first reads.
  - Scope: migrate overview, node details, expansion, shortest path,
    communities, PageRank plan, augmentation state, orphans, and related
    community reads in small route groups.
  - Deliverables: route-group PRs that remove direct application-side graph
    execution and preserve response shape.
  - Acceptance: every migrated group adds route evidence and keeps production
    handlers free of request-time shadow compare.
  - [x] Add a library-first `/graph/node-details/{node_id}` Memory detail
    runtime and route-query evidence helper that reuse shared Cypher contracts
    and bounded query execution without request-time dual-read compare.
  - [x] Add a library-first `/graph/community-members/{community_id}` Memory
    member ranking runtime and route-query evidence helper with exact
    `community_id` scan-pruning evidence.
  - [x] Add a library-first `/graph/orphans` orphan Entity runtime and
    route-query evidence helper for Nowledge's relationship-exclusion cleanup
    query shape.
  - [x] Add a library-first `/graph/sample` deterministic Memory sample runtime
    and route-query evidence helper backed by shared Cypher and bounded query
    execution.
  - [x] Add a library-first
    `/library/community/{community_id}/recent-memories` runtime and route-query
    evidence helper for Nowledge's recent community Memory lookup shape.
  - [x] Add a library-first `/library/community/{community_id}/subgraph`
    runtime and route-query evidence helper for community Entity ranking and
    relation-edge reads.
  - [x] Add a library-first `/graph/augmentation/state` runtime and route-query
    evidence helper for the GraphMeta projected-graph state read.
  - [x] Add a library-first `/graph/augmentation/pagerank/plan` runtime and
    route-query evidence helper for PageRank graph counts and changed-counts.
- [x] PR 4: Add search projection scan-pruning evidence for production filters.
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
  - [x] Add typed production-filter pruning proof samples to the Skein search
    projection probe and fail closed when the proof is missing or incomplete.
  - [x] Consume production-filter pruning proof in replacement summary and
    integration cutover readiness so forged projection `ready=true` cannot
    bypass the PR4 gate.
  - [x] Attach compact `EXPLAIN ANALYZE`-style segment scan counters to
    production-filter pruning proof and reject evidence without them.
- [x] PR 5: Close LanceDB candidate read replacement.
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
  - [x] Require Rust-bridge search candidate evidence to prove text and
    vector retriever legs are both observed before LanceDB read cutover.
  - [x] Require search-candidate shadow probe inputs to carry typed text and
    vector retriever-leg evidence instead of producing weak ready probes.
  - [x] Require Rust-bridge search candidate evidence to prove FTS and vector
    top-k overlap from typed single-leg candidate reads before cutover.
  - [x] Carry compact candidate-readiness signals in Rust-bridge search
    candidate shadow evidence, including projection marker, watermark, and
    embedding identity readiness.
  - [x] Require replacement summary, integration readiness, and final preflight
    to fail closed on missing search-candidate readiness signals from
    Rust-bridge shadow evidence.
  - [x] Require search-candidate shadow probe inputs to carry typed
    candidate-readiness signals before producing cutover evidence.
- [x] PR 6: Prove WAL and checkpoint recovery with Mem-shaped mutations.
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
- [x] PR 8: Make Mem's Skein startup and readiness library-only.
  - Scope: ensure Mem starts Skein in-process and consumes typed readiness,
    route evidence, search projection evidence, slow log, blackbox, recovery,
    and maintenance APIs.
  - Deliverables: no production shell-out, CLI wrappers remain thin developer
    adapters, and production config selects read engine explicitly.
  - Acceptance: a Mem integration test can initialize Skein as a library and
    evaluate cutover gates without invoking Skein CLI tools.
  - [x] Expose typed library-readiness area summaries in the Rust report so Mem
    can evaluate area gates without parsing readiness JSON.
  - [x] Expose typed blackbox readiness summaries over redaction, slow-query
    JSONL, and background QoS evidence so Mem can gate diagnostics without
    shelling out to manifest JSON glue.
  - [x] Consume typed blackbox readiness in integration readiness so blackbox
    redaction and operational evidence gates no longer duplicate JSON-path
    rules.
  - [x] Route blackbox redaction and operational integration checks through
    typed conditions so field-level diagnostics consume the same Rust readiness
    summary.
  - [x] Expose and consume typed graph replacement readiness so Mem can gate
    production cutover, shadow evidence, and dual-engine consistency without
    duplicating replacement-summary JSON-path checks.
  - [x] Expose and consume typed query-family replacement readiness so Mem can
    gate required Nowledge query families without duplicating replacement-summary
    JSON-path checks.
  - [x] Expose and consume typed storage-recovery cutover readiness so Mem can
    inspect durable recovery gates without reimplementing replacement-summary
    JSON-path checks.
  - [x] Route storage-recovery integration checks through typed conditions so
    field-level recovery failures and next actions consume the same Rust gate
    summary.
  - [x] Expose and consume typed background-maintenance cutover readiness so Mem
    can inspect QoS and search-projection graph-delta gates without duplicating
    replacement-summary JSON-path checks.
  - [x] Route background-maintenance integration checks through typed conditions
    so QoS evidence failures and next actions consume the same Rust gate summary.
  - [x] Consume typed library-readiness cutover summaries in integration
    readiness so Mem can gate library startup and required areas without
    duplicating readiness JSON-path checks.
  - [x] Route the library-readiness integration check through typed conditions
    so field-level failures and next actions consume the same Rust gate summary.
  - [x] Expose and consume typed search-projection cutover readiness so Mem can
    gate LanceDB projection replacement without duplicating replacement-summary
    JSON-path checks.
  - [x] Expose and consume typed search-candidate cutover readiness so Mem can
    gate Rust-bridge candidate reads without accepting trace-only evidence.
  - [x] Expose and consume typed bounded-read cutover readiness so Mem can gate
    row caps, payload budgets, and route coverage without duplicating
    replacement-summary JSON-path checks.
  - [x] Expose and consume typed bounded-read alignment readiness so Mem can
    reject stale live bounded-read evidence without duplicating alignment
    JSON-path checks.
  - [x] Expose and consume typed graph-route cutover readiness so Mem can gate
    Kuzu/Ladybug graph read route ownership without duplicating route-profile
    JSON-path checks.
  - [x] Expose and consume typed graph-route alignment readiness so Mem can
    reject stale graph route replacement-summary evidence without duplicating
    alignment JSON-path checks.
  - [x] Expose and consume typed graph-route parity alignment readiness so Mem
    can gate route-level shadow parity without duplicating alignment JSON-path
    checks.
  - [x] Expose and consume typed query-runtime preflight readiness so Mem can
    prove library query execution and route coverage without duplicating
    preflight JSON-path checks.
  - [x] Expose and consume typed query-runtime preflight alignment readiness so
    Mem can reject stale replacement-summary query-runtime evidence without
    duplicating alignment JSON-path checks.
  - [x] Expose and consume typed integration-bundle, submodule, coexistence,
    content-store boundary, and previous-wrapper readiness gates so Mem can
    evaluate startup cutover prerequisites through Rust structs instead of
    JSON-path glue.
  - [x] Add an in-process library integration test that initializes Skein,
    produces typed library readiness, and feeds the final cutover preflight
    without invoking CLI tools.
  - [x] Require Mem integration readiness and final cutover preflight to consume
    the typed workload-fixture library readiness area, so graph route, bounded
    expansion, and metadata-filtered search workload evidence cannot be omitted
    from cutover gates.
  - [x] Require replacement summary and final previous-wrapper preflight to
    validate workload-fixture evidence fields directly, so a forged
    `production_cutover_ready=true` summary cannot omit graph route, bounded
    expansion, or metadata-filtered search fixture coverage.
- [x] PR 9: Add the final cutover preflight bundle gate.
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
  - [x] Expose a typed final cutover preflight report through Rust library APIs
    with compact redacted diagnostics over integration readiness and replacement
    summary production cutover evidence.
- [x] PR 10: Remove or quarantine obsolete compatibility paths.
  - Scope: after gates are satisfied, remove unused CLI-only, Python-only, and
    request-time shadow compare code from the production path.
  - Deliverables: deleted or nightly-only compatibility entrypoints and updated
    documentation.
  - Acceptance: production Mem still supports explicit legacy read selection
    during the migration window, but no stale compatibility helper bypasses the
    query runtime or readiness gates.
  - [x] Quarantine command-backed previous-wrapper and request-time shadow
    compatibility CLI tools behind an explicit developer/preflight opt-in while
    keeping production integration on typed Rust library APIs.
  - [x] Update external shadow, replacement matrix, and architecture docs so CLI
    compatibility commands are described as isolated evidence generators, not
    production serving or cutover-decision paths.

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
  - [x] Redact storage-recovery evidence JSON parse failures by default so WAL
    paths and recovery payload fragments are not copied into command errors.
- [x] Add storage-level scan pruning where semantics are exact.
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
  - [x] Cover Nowledge node graph-delta timestamp filters such as
    `created_at > $cutoff OR updated_at > $cutoff` with `EXPLAIN ANALYZE`
    scan-pruning evidence before row payload filtering.
  - [x] Push exact `NodeColumnLookupExec` lookups for single-label graph reads
    through the property index and emit compact scan-pruning evidence, so
    column-driven node lookups avoid preloading all label payloads.
- [ ] Keep memory use bounded by default.
  - User foreground reads are admitted first.
  - Internal background import, projection, compaction, analytics, and shadow
    migration work must be deferrable under resource pressure.
  - Background work should be scheduled through resource classes and QoS limits;
    foreground user requests should not be throttled by internal maintenance.
  - [x] Require background-maintenance cutover evidence to include a passing
    foreground admission probe so user reads are not throttled by background
    QoS limits.
  - [x] Expose a compact typed local QoS snapshot that proves foreground
    admission stays open while background work is enabled, bounded, and within
    total and per-class operation budgets.
  - [x] Require background-maintenance evidence to carry local QoS snapshot
    readiness so cutover gates fail closed when background work is unbounded or
    over budget.
  - [x] Redact background-maintenance evidence JSON parse failures by default
    so local artifact paths and QoS payload fragments are not copied into
    command errors.

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
  - [x] Redact search-projection evidence JSON read and parse failures by
    default so local paths and document identity payload fragments are not
    copied into command errors.
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
    - [x] Require typed search candidate readiness to prove projection
      watermark visibility and embedding manifest identity for LanceDB
      replacement candidate reads.
    - [x] Require LanceDB replacement candidate-read readiness options to fail
      closed when projection embedding manifest identity is missing by default.
    - [x] Require Rust-bridge search candidate evidence to expose and gate
      row-count parity plus compact shadow scan-pruning fields before cutover.
    - [x] Require Rust-bridge search candidate evidence to prove text and
      vector retriever legs are both observed before LanceDB read cutover.
    - [x] Require search-candidate shadow probe inputs to carry typed text and
      vector retriever-leg evidence instead of producing weak ready probes.
    - [x] Require final previous-wrapper preflight to validate search-candidate
      retriever-leg and top-k overlap evidence fields from replacement summary.
    - [x] Redact search-candidate shadow probe JSON parse failures by default
      so candidate ids and filter payload fragments are not copied into command
      errors.
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
- [x] Add library readiness APIs for Mem integration.
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
  - [x] Expose library readiness area gates as typed Rust structs so embedded
    Mem callers do not need to parse `readiness_by_area` JSON.
  - [x] Expose a typed Nowledge Mem library-readiness runner and keep the CLI
    function as a JSON-only wrapper over the Rust report.
  - [x] Expose typed graph replacement and query-family replacement readiness
    through Rust library APIs and consume the same results in integration
    readiness next actions.
  - [x] Expose typed search candidate replacement readiness through Rust
    library APIs for route-level LanceDB replacement gates.
  - [x] Include search-candidate shadow evidence as a first-class
    embedded-library readiness area so Mem fails closed before LanceDB candidate
    read cutover.
  - [x] Recompute search-candidate shadow evidence raw fields in library
    readiness instead of trusting `ready=true`.
  - [x] Recompute search-projection and search-projection-shadow raw fields in
    library readiness instead of trusting `ready=true`.
  - [x] Expose typed search-projection cutover readiness through Rust library
    APIs and consume the same result in integration readiness next actions.
  - [x] Expose typed search-candidate cutover readiness through Rust library
    APIs and consume the same result in integration readiness next actions.
  - [x] Expose typed bounded-read cutover readiness through Rust library APIs
    and consume the same result in integration readiness next actions.
  - [x] Expose typed bounded-read alignment readiness through Rust library APIs
    and consume the same result in integration readiness next actions.
  - [x] Expose typed graph-route cutover readiness through Rust library APIs and
    consume the same result in integration readiness next actions.
  - [x] Expose typed graph-route alignment readiness through Rust library APIs
    and consume the same result in integration readiness next actions.
  - [x] Expose typed graph-route parity alignment readiness through Rust library
    APIs and consume the same result in integration readiness next actions.
  - [x] Expose typed query-runtime preflight readiness through Rust library APIs
    and consume the same result in integration readiness next actions.
  - [x] Expose typed query-runtime preflight alignment readiness through Rust
    library APIs and consume the same result in integration readiness next
    actions.
  - [x] Include graph-route readiness as a first-class embedded-library
    readiness area so production callers fail closed before graph read cutover.
  - [x] Cover the library-only readiness-to-final-preflight path with an
    in-process Rust test so Mem startup gates do not depend on command output.
  - Keep reports compact and redacted by default so production can keep them on.
  - CLI tools may wrap library APIs for developer workflows, but must not be the
    only supported interface.
- [x] Add explain output that includes semantic checks, selected fast path,
  optimizer budget, chosen indexes, scan-pruning decisions, and resource class.
- [x] Add typed preflight or harness APIs for all replacement artifacts
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

- [x] Improve statistics maintenance.
  - Prefer incremental label, relationship, distinct-value, and degree summaries
    once correctness is proven.
  - Keep full rebuild as a validation and repair tool.
  - [x] Expose a typed basic-statistics consistency report that compares
    incrementally maintained node, label, relationship, and relationship-type
    counts against a full recompute after mutations.
  - [x] Expose compact degree-statistics consistency evidence grouped by label,
    relationship type, and direction for bounded expansion and dense-adjacency
    planning.
  - [x] Expose a typed distinct-value statistics consistency report that
    compares incrementally maintained property indexes against full recompute
    counts for node and relationship property filters.
- [x] Improve adjacency and index layout for read-heavy local workloads.
  - Optimize for bounded memory and predictable read amplification.
  - Keep row-oriented canonical records and add projection/index layouts only
    where route evidence proves value.
  - [x] Expose a typed adjacency consistency report that validates maintained
    incoming/outgoing relationship groups against a full relationship scan
    before adding denser read-optimized layouts.
  - [x] Expose a compact property-index consistency report that validates
    maintained node and relationship property indexes against full recompute
    samples before relying on them for read-heavy scan pruning.
- [x] Add workload fixtures based on real Nowledge routes before low-level
  tuning.
  - Benchmark graph reads, bounded expansions, metadata-filtered search, and
    mixed foreground/background workloads.
  - [x] Add a typed graph-route workload fixture that seeds Mem-shaped graph
    data and runs the current graph-first route Cypher catalog through the
    query runtime with plan/profile evidence.
  - [x] Extend the graph-route workload fixture with bounded expansion probes
    for two-hop traversal and dense-adjacency fanout diagnostics.
  - [x] Extend the workload fixture with metadata-filtered search projection
    probes for enum/in-list, lifecycle, numeric range, timestamp range, source,
    and space filters.
  - [x] Feed the graph/search workload fixture into the typed Mem library
    readiness area map so cutover gates can require real route, bounded
    expansion, and metadata-filtered search evidence without production CLI
    wrappers.

## P2: Deferred Capabilities

- [ ] Advanced graph algorithms beyond Nowledge's active routes.
- [ ] Broad openCypher compatibility not exercised by Nowledge Mem.
- [ ] Distributed storage, replication, or cloud-primary execution inside the
  local embedded engine.
- [ ] Aggressive SIMD work unless route-level evidence shows it is needed.
