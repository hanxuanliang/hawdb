# Skein TODO

This file tracks the remaining Nowledge Mem replacement work for Skein. The
scope is intentionally product-driven: implement capabilities required to
replace the local Kuzu/Ladybug graph layer and the LanceDB search projection.
Do not expand into general-purpose database features unless a Nowledge Mem route,
query family, or cutover gate requires them.

## P0: Production Replacement Gates

- [x] Cut active graph reads over without production dual-read compare.
  - Inventory every active Nowledge Mem REST and MCP graph read route that still
    calls Kuzu/Ladybug directly.
  - Route handlers should issue Cypher through the query runtime and select
    `legacy` or `skein` reads through configuration.
  - Do not keep request-time old/new read comparison in production paths.
    Equivalence evidence should come from offline fuzz harnesses, fixtures, and
    preflight bundles.
- [x] Move graph read traffic through the query runtime boundary.
  - Mem should dual-write to Kuzu/Ladybug and Skein from the start of the
    migration window, then choose the read engine through configuration.
  - Keep Kuzu/Ladybug as the default read engine until each active route has
    route-level Skein readiness evidence.
  - Avoid direct hand-written execution paths in application routes when the
    AST, fast-path detector, optimizer, and executor can own the path.
- [x] Complete dual-engine cutover readiness.
  - Replacement summary must fail closed when route parity, storage recovery,
    background maintenance, search projection parity, or bounded-read coverage is
    missing.
  - Integration readiness must keep legacy Kuzu/Ladybug and LanceDB data
    side-by-side until cutover is proven.
- [x] Keep Skein embedded-library first.
  - Production Mem integration should start and operate Skein through Rust
    library APIs, similar to SQLite-style embedding.
  - Do not require production command-line wrappers, environment-driven control
    planes, or spawned helper processes for normal operation.
  - Nightly-only import, migration, and diagnostic entrypoints may exist, but
    they must not be required by the production read/write path.
- [x] Keep sensitive paths and data out of readiness artifacts.
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
  - [x] Redact blackbox manifest and event JSON serialization failures by
    default so retained production diagnostics expose stable error classes
    instead of serde internals.
  - [x] Redact shared CLI JSON file parse failures by default so nightly
    diagnostics do not copy local paths or payload fragments into command
    errors.
  - [x] Expose local open-path diagnostics only behind an explicit debug flag;
    default open diagnostics remain redacted.

## P0: Nowledge Mem Production Integration

The checked kernel and readiness items elsewhere in this file prove that Skein
exposes an API or evidence contract. They do not prove that the shipped Mem
runtime owns the corresponding production path. The items in this section stay
open until Mem consumes the library API and no longer depends on Kuzu/Ladybug or
LanceDB for that path.

- [ ] Add crash-recoverable dual writes for Mem mutations.
  - [x] Dual-write shared `create_memory_core` Memory create/update requests
    and label assignments through one process-lifetime Skein handle.
    - Mem PR #384 persists versioned obligations before either engine applies,
      records independent legacy/Skein acknowledgements, replays unfinished
      work before startup and before a newer foreground mutation, and retires
      only after both sides are durable.
    - Real legacy/Skein tests cover prepare-only, legacy-only,
      completed-but-unretired, and stale-obligation-before-new-write crash
      windows.
  - [x] Route ordinary MCP `memory_add` create/update and label assignments
    through the same durable coordinator.
    - Mem PR #384 injects the process-lifetime coordinator into both external
      and in-process MCP servers, and a real Kuzu/Skein test proves the
      obligation retires only after both engines expose the Memory and Label.
  - [x] Extend the coordinator payload to cover MCP `memory_add` with an
    `EVOLVES` directive.
    - Mem PR #384 freezes the resolved cross-author contested decision in the
      versioned obligation, preserves the legacy node/relationship transaction,
      uses Skein's typed Memory `EVOLVES` batch API, and replays latest-state,
      inherited-label, and contested-metadata side effects idempotently.
    - Real legacy/Skein tests cover normal replacement, a crash after the
      legacy transaction commits but before acknowledgement, and contested
      replacement downgraded to `challenges` in both graphs.
  - [x] Route Memory lifecycle/delete and the remaining entity mutation
    families through the same durable coordinator.
    - [x] Generalize startup replay by durable operation kind and route REST
      archive/forget lifecycle transitions through a versioned, idempotent
      Memory lifecycle obligation (Mem PR #384 commit `6eef2c8d6`).
    - [x] Freeze final scalar/cascade state for Memory hard delete, replay it
      idempotently across Kuzu and Skein, and route the REST and MCP delete
      entrypoints through the durable obligation (Mem PR #384 commit
      `4c17aced0`; Skein commits `cc3a205` and `f004fc1`).
    - [x] Fold supersede/deprecate EVOLVES side effects into the same lifecycle
      obligation.
      - Mem PR #384 freezes the replacement endpoint, cleaned metadata, edge
        audit fields, and final lifecycle state in a versioned payload. Kuzu and
        Skein each apply the relationship, latest-state, label inheritance, and
        lifecycle update in one engine-local transaction.
      - Coordinator tests cover normal convergence and replay after the Kuzu
        transaction commits but before its acknowledgement; REST and MCP route
        tests prove the production entrypoints retire the same obligation.
    - [x] Route Label create/update/delete through versioned durable
      obligations (Mem PR #384 commit `fb3e0d473`).
      - Freeze canonical deduplication and complete scalar state without the
        legacy resolver's pre-obligation canonical-name backfill.
      - Startup and foreground replay now dispatch Memory and Label mutations
        through one operation-kind-aware path.
      - Real Kuzu/Skein tests cover prepare-only and legacy-applied crash
        windows, and an Axum route test proves the production CRUD handlers use
        the durable path.
    - [x] Route Memory `HAS_LABEL` assign/remove through versioned obligations
      (Mem PR #384 commit `f7a643ef8`).
      - Endpoint validation happens before prepare; Skein endpoint validation
        and the idempotent relationship mutation run through the canonical
        query runtime before the obligation can retire.
      - Core tests cover assign/remove convergence and replay after an
        unacknowledged Kuzu commit; an Axum route test covers the production
        POST/DELETE endpoints.
    - [x] Route Source `HAS_LABEL` assign/remove through versioned obligations
      (Mem PR #384 commit `af67babff`).
      - Freeze endpoint existence before prepare and preserve the existing
        relationship properties in idempotent Kuzu/Skein apply queries.
      - Core tests cover convergence and replay after an unacknowledged Kuzu
        commit; an Axum route test covers the production POST/DELETE endpoints.
    - [x] Route Label merge through one versioned composite obligation
      (Mem PR #384 commit `507fc875a`).
      - Freeze Memory, Source, Entity, and Thread assignments with their
        relationship properties, the final target metadata, mutation result,
        and governance receipt before either engine applies.
      - Apply assignment transfer, target finalization, source deletion, and
        receipt creation through one grouped Skein transaction; all REST, MCP,
        compatibility, and background consolidation entrypoints share the same
        coordinator.
      - Real Kuzu/Skein tests cover normal convergence, prepare-only replay,
        replay after an unacknowledged Kuzu commit, exact retry, and production
        REST/MCP routing.
    - [x] Route Entity delete through a versioned durable obligation
      (Mem PR #384 commit `a5cc5acf5`).
      - Freeze the Entity summary, incident-edge counts, result, and plan
        fingerprint before either graph applies the delete.
      - Kuzu revalidates the frozen plan and persists the governance receipt in
        the same transaction as the detached delete; Skein deletes the Entity,
        invalidates graph projections, and persists the receipt in one grouped
        transaction.
      - Real Kuzu/Skein tests cover normal convergence, prepare-only replay,
        replay after an unacknowledged Kuzu commit, stale-plan rejection, and
        production REST/MCP routing.
      - Search projection cleanup remains a separately recoverable projection
        obligation and does not weaken the durable graph mutation boundary.
    - [x] Route Entity merge through one versioned composite obligation
      (Mem PR #384 commit `f739baa18`).
      - Freeze both Entity states, all transferred relationship properties,
        duplicate-resolution decisions, the final target state, mutation
        result, and governance receipt before either graph applies.
      - Kuzu revalidates the complete snapshot and commits the merge with its
        receipt in one transaction; Skein applies relationship transfer,
        target finalization, source deletion, projection invalidation, and the
        receipt in one grouped transaction.
      - Real Kuzu/Skein tests cover normal convergence, prepare-only replay,
        replay after an unacknowledged Kuzu commit, payload tampering, exact
        retry, and production REST/MCP routing.
    - [x] Cover the Entity node mutation family.
      - Entity extraction create/reuse, delete, and merge now use frozen,
        replayable graph obligations.
    - [x] Cover the Thread node mutation family.
      - [x] Route Thread and ThreadIdentity create, failed-content
        compensation, and REST/MCP delete through one versioned, replayable
        graph obligation (Mem PR #384).
      - [x] Route metadata, favorite, bulk space move, denormalized
        message-count, skill-use markers, and scheduler title/summary updates
        through frozen Thread mutation payloads (Mem PR #384 commit
        `5ef1d287f`).
      - The cross-domain Space merge remains a separate composite
        Memory/Source/Thread obligation; do not decompose it into independent
        Thread patches.
    - [ ] Cover the Source node mutation family.
      - [x] Define versioned, replayable Source patch/delete obligations with
        grouped Kuzu/Skein apply and crash-window tests (Mem PR #384 commit
        `9ae06454d`).
      - [x] Route standalone lifecycle, OCR metadata, space move/rollback, and
        graph delete entrypoints through the Source mutation obligation (Mem
        PR #384 commit `94e287bff`).
        - Retain the durable storage-cleanup intent when Kuzu commits the graph
          delete before Skein converges.
      - [ ] Route Source ingest/create, content refresh/reparse, indexed
        transition, revision edges, and search-projection effects through a
        frozen composite Source obligation.
        - [x] Strengthen the Skein transaction kernel so a composite Source
          obligation can create or merge Source nodes, set Source properties,
          create or merge revision relationships, set relationship properties,
          and remove no-op pending creates in one grouped WAL transaction
          without requiring a new typed business API.
        - [x] Make relationship retarget/copy merge mutations read pending
          nodes and pending relationships inside the same grouped transaction,
          so Source revision and label inheritance style Cypher does not fall
          back to committed-only scans.
        - [x] Prove a Source-ingest-shaped grouped commit that creates parsed
          and indexed Source revisions plus a revision edge remains atomic for
          search-projection changefeed batching; a too-small projection batch
          now fails closed instead of advancing only part of the Source
          composite.
        - [x] Carry the Source projection workload evidence through replacement
          summary, library readiness, and final preflight gates so missing
          atomic Source-ingest projection evidence blocks cutover instead of
          being hidden behind generic workload readiness.
        - [x] Expose a typed Source mutation family dual-write readiness
          contract that covers Source patch/delete, lifecycle, graph delete,
          ingest/create, content refresh/reparse, indexed transition, revision
          edges, and search-projection effects with frozen payload,
          independent legacy/Skein acknowledgements, independent watermarks,
          replay idempotency, and projection-payload requirements.
        - [x] Consume Source mutation dual-write readiness in replacement
          summary, integration readiness, and final cutover preflight so
          missing composite Source mutation evidence blocks graph replacement
          cutover explicitly.
        - Mem still needs to freeze and replay the full Source ingest/create,
          refresh/reparse, indexed transition, revision-edge, and
          search-projection payload through its durable dual-write coordinator
          before this item can close.
  - Inject one long-lived writable Skein handle into Mem write resources.
  - Cover Memory create, update, lifecycle, and delete first, then Label,
    Entity, Thread, Source, and relationship mutations.
  - Persist an idempotent mutation obligation before either database can report
    success, and record independent legacy and Skein apply watermarks.
  - Replay incomplete obligations after restart without duplicating nodes,
    relationships, or search projection rows.
  - Acceptance: a successful request is readable from the selected engine, and
    every crash point either has both writes durable or has a durable replay
    obligation that converges them.

- [ ] Add resumable initial import from Kuzu/Ladybug and LanceDB.
  - Run import through bounded Rust library APIs; production startup must not
    spawn a CLI or helper process.
  - [x] Expose a Rust library parser for Graph Lightning graph streams that
    materializes an import-ready canonical snapshot after checksum, endpoint,
    count, and manifest validation; this is the graph-state decode layer for a
    future resumable importer.
  - [x] Expose a typed library readiness contract that blocks initial-import
    cutover until the decoded manifest is import-ready, the target graph has
    reached the manifest epoch, and the search projection has a durable
    checkpointed source-graph watermark at or beyond the target graph epoch.
  - [x] Expose a typed resumable checkpoint readiness contract that verifies
    source schema/checksum identity, caller-owned idempotency keys, batch
    completion, document identity presence, applied graph watermarks, and
    durable search-projection watermarks against the Graph Lightning manifest.
  - [x] Expose a typed resume decision for initial import checkpoints so Mem can
    choose start, resume, ready-for-cutover, or quarantine without parsing CLI
    artifacts.
  - [x] Expose a typed library initial-import plan that combines graph-stream
    validation, import-ready decode evidence, target graph/search projection
    readiness, checkpoint readiness, and resume action into one fail-closed
    report for Mem startup/import orchestration.
  - [x] Import canonical Graph Lightning graph state into an empty Skein target
    through a bounded Rust library API, using one WAL batch, endpoint and
    duplicate-id validation, persisted stable-id mapping, and fail-closed
    non-empty target checks.
  - [x] Expose a typed monotonic checkpoint progress helper that keeps
    source schema/checksum identity and caller-owned idempotency keys stable,
    rejects graph/search watermark, batch, and document-identity regressions,
    and returns updated readiness plus resume action for Mem-owned checkpoint
    persistence.
  - [x] Expose typed document-identity coverage for the six Nowledge search
    projection kinds so Mem can fail closed when Memory, Message, Entity,
    Source, SourceChunk, or Community identities are missing, empty, or
    duplicated before initial-import cutover.
  - [x] Feed document-identity coverage into the typed initial-import plan and
    cutover gate, while keeping the compatibility plan available for callers
    that have not wired durable projection identities yet.
  - [x] Expose a strict initial-import apply API that carries durable projection
    document identities through the returned plan, so graph import can proceed
    while cutover remains fail-closed on identity gaps.
  - [x] Expose a typed search-projection import batch report that validates the
    checkpoint idempotency contract, batch graph epoch, batch position,
    operation limits, delete-free initial import semantics, total-batch
    stability, and cumulative coverage for all six projection document-identity
    kinds before returning checkpoint progress for Mem-owned durable
    persistence, and verifies the returned progress through the checkpoint
    advance path so callers can inspect post-progress readiness and resume
    action without duplicating checkpoint logic.
  - Import canonical graph state from Kuzu/Ladybug and rebuild or import all six
    search projection kinds: Memory, Message, Community, Entity, Source, and
    SourceChunk.
    - [x] Expose a typed initial-import source bundle readiness contract that
      validates the Graph Lightning graph source and the full set of LanceDB
      search-projection batches together, requiring a matching checkpoint,
      manifest graph epoch, delete-free projection batches, and cumulative
      document identity coverage for all six projection kinds before the host
      starts or resumes import work.
  - Persist source schema/version fingerprints, batch checkpoints, document
    identities, and graph/search watermarks.
    - [x] Expose a typed durable-state envelope that packages the source
      fingerprint, checkpoint, document identities, coverage report, checkpoint
      readiness, and resume action, while keeping partial progress persistable
      and cutover readiness fail-closed.
    - [x] Expose a stable library durable-state codec that serializes the
      source fingerprint, checkpoint, document identities, and recomputed
      coverage into a host-persistable payload, and decodes only after
      protocol, manifest fingerprint, and persistability checks pass.
  - Keep foreground dual writes active while import catches up, and make retries
    idempotent.
    - [x] Expose a typed durable-state batch advance helper that merges
      projection document identities idempotently, advances checkpoint progress
      through the same monotonic checkpoint path, reports completed-batch
      replays explicitly, and fails closed without emitting a new state when a
      batch cannot produce accepted checkpoint progress.
    - [x] Expose a typed initial-import session report that combines graph
      stream validation, target readiness, persisted durable state, resume
      action, and durable-state source fingerprint checks. This lets Mem restart
      into start, resume, ready-for-cutover, or quarantine without parsing CLI
      output or reimplementing checkpoint logic.
    - [x] Require production cutover gates to prove initial import is inactive
      for read cutover; an active import now blocks cutover even when dual
      writes are enabled, unless a future Mem-owned session gate proves the
      imported state and live mutations have reached the same durable watermark.
    - [x] Expose a typed initial-import cutover catch-up report that compares
      the durable import checkpoint, live graph commit epoch, and live durable
      search-projection watermark, so Mem can prove the imported state and live
      mutations share a cutover watermark without hand-written JSON path checks.
    - [x] Consume the typed cutover catch-up proof in host-owned cutover
      controls, integration readiness, and final previous-wrapper preflight, so
      active initial import remains blocked by default but can pass read cutover
      only when the library-owned catch-up proof is ready.
    - [x] Expose a typed initial-import session-bundle readiness helper that
      combines source-bundle readiness, durable session state, resume action,
      and optional live cutover catch-up proof so Mem startup can fail closed
      without duplicating readiness logic.
    - [x] Expose a typed initial-import startup readiness report that builds
      source-bundle readiness, session readiness, optional catch-up proof, and
      final session-bundle readiness from one library call, keeping target
      projection freshness separate from live dual-write catch-up freshness.
    - [x] Expose a typed recovery readiness report that accepts the
      host-persisted durable-state payload directly, decodes it fail-closed,
      and returns an explicit quarantine action for malformed or mismatched
      state instead of letting Mem hand-roll decode/start/resume decisions.
    - [x] Expose recovery-backed cutover controls that accept the typed recovery
      report directly and require its own ready catch-up proof before an active
      import can permit read cutover; the legacy catch-up-only overload remains
      available only for compatibility.
  - Acceptance: restart resumes from the last durable checkpoint and read
    cutover remains blocked until imported state and live mutations reach the
    same durable watermark.

- [ ] Migrate the complete Mem graph read route inventory.
  - Treat the shared route catalog as an inventory, not proof of live ownership.
  - Move REST, MCP, read-batch, export, and background-maintenance reads from
    direct `KuzuClient` calls to parameterized Cypher through the embedded query
    runtime.
  - Start with the existing overview, sample, node-details, community,
    augmentation, PageRank-plan, and orphan library APIs.
  - Preserve response shape, ordering, pagination, error classes, and metadata
    stripping without request-time dual-read comparison.
  - Acceptance: each migrated route executes with Skein selected and contains
    no direct Kuzu/Ladybug read in its request path.

- [ ] Replace every active LanceDB search projection read.
  - Wire Mem to Skein candidate reads for Memory, Message, Community, Entity,
    Source, and SourceChunk projections.
  - [x] Expose a typed search projection route-ownership contract for Memory,
    Message, Community, Entity, Source, and SourceChunk, so production cutover
    can fail closed while any route family still selects LanceDB.
  - [x] Consume the search projection route-ownership contract in library
    readiness, Mem integration readiness, and final previous-wrapper preflight
    gates as a dedicated readiness area, so missing ownership evidence or any
    LanceDB-owned search route family blocks cutover.
  - [x] Expose typed active search route ownership for thread/message FTS,
    entity/community discovery, source recall, source chunk recall,
    `/fs/recall`, MCP search, and deep-search graph expansion, and consume it
    through the same library readiness area so active route coverage is checked
    separately from projection-family ownership.
  - [x] Consume projection-family and active search route ownership in the
    replacement summary production cutover gate, missing-evidence list, blocker
    aggregation, and next-action guidance.
  - [x] Require the Mem integration bundle to carry projection-family and
    active search route ownership inputs, emit replacement-summary alignment
    reports for both, and fail the integration readiness gate when either live
    ownership evidence diverges from the replacement summary.
  - [x] Expose active search route read readiness as a typed library contract
    and require it in the library search-route readiness area, so ownership
    alone cannot pass cutover unless every active route also proves Skein
    candidate reads, metadata pushdown, ranking, fail-soft behavior, and no
    LanceDB handle requirement.
  - [x] Carry active search route read readiness through the replacement
    summary and Mem integration bundle alignment gate, so final cutover
    evidence fails closed when any active search route still needs LanceDB or
    lacks candidate-read evidence.
  - [x] Cover thread/message FTS, entity/community discovery, source and
    source chunk recall, `/fs/recall`, MCP search, and deep-search graph
    expansion with a typed active search route read requirement catalog.
  - [x] Preserve embedding identity, zero-vector semantics, CJK tokenization,
    ranking windows, fail-soft reason codes, and repair/rebuild markers in the
    active search route read evidence, replacement summary, and integration
    readiness alignment gate.
  - Acceptance: no active search route requires a LanceDB handle when the Skein
    search engine is selected.

- [x] Split graph and search cutover controls and make status truthful.
  - Configure graph reads, search reads, dual writes, import, and projection
    catch-up independently through host-owned library configuration.
  - Do not report the whole graph or search engine as Skein-owned merely because
    the feature is compiled and paths are configured.
  - Include open state, route ownership, applied and durable watermarks,
    projection freshness, and active blockers in readiness.
  - [x] Split Mem graph and search read selection into independent host
    configuration, retain the old aggregate variable only as a compatibility
    fallback, and route the existing Skein-owned graph and search surfaces only
    when their respective domain is selected.
    - Implemented in Mem PR #384 with domain-specific precedence tests and a
      versioned runtime status payload that reports partial ownership without
      claiming either complete engine.
  - [x] Bind readiness to the actual process-lifetime Skein open state rather
    than configured paths alone.
  - [x] Expose graph and search route ownership, applied and durable watermarks,
    projection freshness, and blockers through the production status surface.
    - Implemented in Mem PR #384 using Skein runtime status protocol
      `skein-nowledge-mem-runtime-status-v1`. The health payload reads the live
      process-owned handle and fails closed on unopened state, projection lag,
      repair/reindex markers, or non-recoverable changefeed state.
  - [x] Expose a Skein embedded-library production status report that keeps
    graph route ownership and search projection freshness as separate cutover
    decisions, and refuses to claim effective graph/search cutover from mode,
    compiled features, or configured paths alone.
  - [x] Expose typed host-owned cutover controls for graph reads, search reads,
    dual writes, initial import, and projection catch-up, and validate selected
    Skein reads against the live production status before reporting them
    effective.
  - [x] Require the integration bundle and final preflight gate to consume the
    typed cutover controls report, so host-selected Skein graph/search reads,
    dual writes, and projection catch-up cannot be omitted from production
    cutover evidence.
  - [x] Require final previous-wrapper preflight to consume typed cutover
    controls directly, so Skein graph/search reads cannot pass release gates
    unless the selected host controls are effective against live production
    status.
  - Acceptance: partial route migration is represented as partial ownership, and
    stale or unopened stores cannot report an effective Skein cutover.
    - Covered by the typed cutover controls report, integration bundle,
      previous-wrapper preflight, and active-initial-import cutover blocker.

- [x] Close Mem search filter and result-semantics parity.
  - [x] Add labels, event-date ranges, recorded-date ranges, temporal context,
    cross-space scope, and the remaining metadata predicate forms.
  - [x] Canonicalize Mem-facing `latest` and `history` search filters into the
    descriptor-safe `is_latest` predicate, with `history=true` mapped to
    `is_latest=false` and boolean range predicates rejected fail-closed.
  - [x] Canonicalize Mem-facing event-date and recorded-date range filters into
    descriptor-safe timestamp predicates, using overlap semantics for
    `event_date_from/to` and `created_at` ranges for `recorded_date_from/to`.
  - [x] Canonicalize Mem-facing temporal context aliases into the
    descriptor-safe `temporal_context` enum predicate and include it in the
    default search projection scan-filter field summaries.
  - [x] Canonicalize Mem-facing cross-space scope aliases into the
    descriptor-safe `space_id` predicate, including default-space normalization
    for missing or empty graph/search projection rows.
  - [x] Lower Mem-facing `__exists` and `__missing` metadata filters into typed
    descriptor-safe presence predicates, with search and graph-seed residual
    evaluation sharing the same semantics.
  - [x] Project relationship-derived `HAS_LABEL -> Label` values into
    descriptor-safe `labels` search metadata, evaluate `labels__in` as a
    multi-value predicate, and include label relationship changes in the
    ordered graph-to-search changefeed.
  - [x] Treat `labels` as an enum-like search predicate field and prove
    persisted segment descriptors can prune `labels__in` before physical
    payload range reads.
  - [x] Push descriptor-safe predicates into segment planning before payload
    reads; keep bounded residual evaluation only for predicates that cannot be
    exact.
    - Covered by persisted segment descriptor pruning tests, physical
      range-read tests, `persisted_segment_ranges_prune_label_in_filters_before_payload_reads`,
      and production pruning evidence samples that require
      `EXPLAIN ANALYZE` payload-read avoidance.
  - [x] Preserve deep mode behavior.
    - `KnowledgeRetrievalRequest::nowledge_deep` preserves visible
      `limit`/`offset` semantics while using a wider `rank_window`,
      `candidate_limit`, graph-seed limit, and bounded two-hop graph context
      expansion for Mem deep-search callers.
    - `nowledge_deep_retrieval_profile_preserves_visible_limit_with_wide_candidate_window`
      and `nowledge_deep_retrieval_profile_uses_filtered_candidate_window`
      cover the no-filter `max(page_end * 5, 20)` window, filtered `200`
      window, metadata-filter preservation, and graph-context expansion
      diagnostics.
  - [x] Preserve search-result offset/limit, stable ordering, and empty-page
    versus error behavior in the typed knowledge retrieval path.
    - `KnowledgeRetrievalRequest`, `SearchQueryOptions`, `SearchResultSet`, and
      `NowledgeMemSearchCandidateReport` now carry `offset`; search execution
      applies offset after stable score/id ordering while keeping total-hit
      counts pre-pagination. The
      `knowledge_retrieval_filter_preserves_stable_offset_limit_pages` test
      covers descriptor-safe filtering, deterministic second-page results, and
      an empty offset page as a non-error result.
  - [x] Acceptance: supported Mem search requests no longer return
    `NOT_IMPLEMENTED`, and `EXPLAIN ANALYZE` proves payload avoidance for
    descriptor-safe filters.
    - Covered by `knowledge_retrieval` filter/result-semantics tests, typed
      search candidate reports, production-filter pruning evidence gates, and
      `rg` verification that the active search/API path has no
      `NOT_IMPLEMENTED` marker.

- [x] Persist the ordered graph-to-search changefeed.
  - [x] Use the graph commit epoch as a stable mutation identity and reconstruct
    checkpointed plus WAL-only ordered deltas after restart.
  - [x] Expose typed changefeed status with the resumable floor, retained
    mutation bounds, and restart-recoverable state.
  - [x] Expose typed changefeed readiness that fails closed when the projection
    watermark is outside the retained window, the graph is not restart
    recoverable for production catch-up, or the configured batch limit disables
    incremental progress.
  - [x] Resume from the search projection's durable source-graph watermark,
    keep one graph commit indivisible across bounded batches, and advance the
    durable watermark only after a successful projection checkpoint.
  - [x] Wire the Mem-owned background scheduler to the long-lived graph and
    search handles so production catch-up consumes this stream.
    - Implemented in Mem PR #384 with a process-lifetime runtime, persistent
      QoS scheduler, bounded batches, typed stop reasons, and host shutdown.
      The parent remains open until the integration is merged and shipped.
  - [x] Prove the Skein changefeed does not split one graph commit across
    bounded projection batches.
    - `search_projection_changefeed_does_not_split_one_commit_across_batches`
      creates two projection mutations in one transaction and verifies a
      too-small batch limit fails closed instead of emitting a partial commit.
  - [x] Prove stale upsert/delete sequences converge through durable catch-up.
    - `durable_search_projection_catch_up_converges_stale_update_delete_sequence`
      checkpoints an initial Memory projection, applies id update, content
      update, and delete mutations, catches up from the durable watermark, and
      verifies neither old nor new document ids remain after reopen.
  - [x] Acceptance: restart, bounded-log truncation, and stale upsert/delete
    sequences converge without losing or splitting a committed graph mutation.
    - Covered by `search_projection_changefeed_replays_wal_only_mutations_after_restart`,
      `search_projection_changefeed_retention_forces_rebuild_for_expired_epoch`,
      `search_projection_delta_request_requires_rebuild_when_changefeed_start_is_too_new`,
      `search_projection_changefeed_does_not_split_one_commit_across_batches`,
      and `durable_search_projection_catch_up_converges_stale_update_delete_sequence`.

- [x] Integrate Skein recovery and operations into the Mem lifecycle.
  - Wire checkpoint, shutdown, WAL replay, corruption quarantine/repair,
    projection catch-up, QoS, slow-query, and blackbox APIs into Mem startup,
    health, readiness, and background scheduling.
  - [x] Expose a typed storage lifecycle decision through the embedded Mem
    library facade so callers get stable `ready`, `run_checkpoint`,
    `repair_wal_tail`, `quarantine`, or `open_read_only_inspect` actions from
    recovery evidence instead of reimplementing WAL/checkpoint blockers.
  - [x] Carry the storage lifecycle decision into typed operations readiness so
    Mem startup, health, and background scheduling can consume one
    library-owned actionable report without re-deriving recovery blockers.
  - [x] Carry the storage lifecycle action into the compact readiness dashboard
    without adding another dashboard area, so human-facing health output can
    show the same library-owned recovery next action.
  - [x] Add host-owned OpenTelemetry metrics for WAL, checkpoint, recovery, index
    maintenance, and background admission without installing a global
    subscriber.
    - `TelemetrySink` stays host-owned and optional. Skein exposes
      `OpenTelemetryMetrics::new(meter)` behind the `opentelemetry` feature and
      never initializes a global subscriber.
    - Kernel telemetry now covers WAL append, checkpoint, recovery, search
      checkpoint, index maintenance, and background admission. The typed
      operations telemetry readiness report fails closed until both graph and
      search projection handles have host sinks configured.
  - [x] Prove overlapping pinned reads while commits remain serialized and
    durable-before-publish.
    - `overlapping_pinned_reads_survive_serialized_durable_commit` holds two
      pinned read transactions across a serialized foreground commit and
      checkpoint, proves old readers keep the pre-commit snapshot, and verifies
      the committed value remains visible after reopen.
    - `reader_keeps_a_stable_snapshot_after_publish`,
      `durability_failure_does_not_publish_staged_value`, and
      `concurrent_commits_are_serialized` cover the storage-facing snapshot
      invariants underneath the embedded library contract.
  - [x] Expose typed operations readiness through the embedded library by
    aggregating runtime watermarks, storage recovery, slow-query, background
    maintenance, and projection-staleness signals without requiring CLI,
    environment-variable, helper-process, or global-subscriber control planes.
  - [x] Require the integration bundle and final preflight gate to consume typed
    operations readiness, so Mem lifecycle cutover fails closed on missing
    recovery actions, stale projections, slow-query/reporting gaps, background
    maintenance blockers, or unsafe redaction.
  - [x] Require final previous-wrapper preflight to consume typed operations
    readiness directly, so release evidence cannot omit storage lifecycle,
    projection freshness, slow-query, background-maintenance, or redaction
    checks.
  - Acceptance: Mem health exposes typed actionable blockers, and crash,
    corruption, memory-pressure, and long-running concurrent workloads have
    integration tests through the embedded library path.
    - Covered by typed operations readiness, storage lifecycle decisions,
      host-owned telemetry readiness, integration bundle, final preflight, and
      pinned-read/serialized-commit tests through the embedded library path.

## P0: Concrete Cutover Blockers

These items are the current kernel and integration gaps that block declaring
Skein a production replacement for Nowledge Mem's Kuzu/Ladybug graph layer and
LanceDB search projection. They do not block incremental development, but they
must block default cutover.

- [x] Complete route-by-route production read ownership.
  - Every active Mem graph/search read route must select `legacy` or `skein`
    through the embedded Rust library runtime.
  - Each route must have route-level execution evidence tied to the shared
    covered-route catalog.
  - Request-time dual-read compare must stay out of production handlers; parity
    belongs to dedicated readiness, preflight, or migration endpoints.
  - Completion evidence: replacement summary rejects stale or missing route
    evidence, and Mem can run the route with `skein` selected without direct
    Kuzu/Ladybug reads.
- [x] Close the LanceDB search projection replacement loop.
  - Candidate reads must prove FTS, vector, source chunk identity, embedding
    identity, lifecycle filtering, metadata predicate pushdown, fail-soft
    behavior, rebuild markers, repair markers, and incremental watermarks.
  - Metadata filters for `unit_type`, lifecycle state, `importance`,
    `confidence`, timestamps, and required metadata keys must prune before rows
    are returned to Mem.
  - Completion evidence: search projection, search candidate, and Rust-bridge
    shadow evidence all pass fail-closed replacement gates without CLI-only
    glue.
- [x] Finish exact storage scan pruning for production filters.
  - Segment descriptors must cover equality, enum/in-list, numeric range,
    date/time range, normalized default equality, and unique-key filters used by
    Nowledge.
  - Capability evidence is required for every supported field, while observed
    payload-read avoidance requires at least one representative pruning sample;
    a field is not required to prune when every segment contains the sampled
    value.
  - Scan planning must decide whether to read a segment before loading row
    payloads into memory.
  - Bloom or cuckoo filters may only be used where false positives are safe and
    false negatives are impossible.
  - Completion evidence: `EXPLAIN ANALYZE` and readiness reports show payload
    read avoidance for graph and search projection filters.
  - [x] Require search projection production-filter pruning evidence to validate
    every required field sample's segment counters, document counters,
    `EXPLAIN ANALYZE` operator, and payload-read avoidance before accepting
    cutover readiness.
  - [x] Require production-filter pruning evidence to cover equality,
    enum/in-list, numeric range, date/time range, null/missing, existence,
    normalized default equality, and unique-key operation families.
- [x] Prove storage recovery under real mutation shapes.
  - WAL replay must recover whole committed batches or nothing.
  - Torn WAL tails, checkpoint boundaries, and replay markers must be detected
    and surfaced through typed readiness APIs.
  - Completion evidence: storage recovery readiness fails closed on missing or
    contradictory WAL/checkpoint evidence and passes replay fixtures that match
    Mem writes.
- [x] Make resource control a cutover gate.
  - Foreground user reads should be admitted ahead of background import,
    projection, compaction, analytics, and migration tasks.
  - Background work must be bounded by resource class, memory budget, and QoS
    limits on consumer hardware.
  - Completion evidence: background-maintenance readiness and blackbox reports
    show admission, deferral, memory-pressure behavior, and slow-query signals.
- [x] Keep the Mem integration library-only on the production path.
  - Mem must start and operate Skein in-process through Rust APIs.
  - Production readiness, route evidence, search projection evidence, slow log,
    blackbox, storage recovery, and maintenance reports must all have typed Rust
    API entrypoints.
  - CLI tools may remain thin developer wrappers, but no production route or
    gate may require shelling out.
  - [x] Require typed library readiness to prove the production path is
    in-process and does not require CLI wrappers, environment control planes, or
    spawned helper processes.
- [x] Preserve replacement boundaries.
  - Kuzu/Ladybug graph and LanceDB search projection are the replacement scope.
  - SQLite content store and large blob/value storage remain external unless a
    Nowledge graph/search route requires a narrower value-store API.
  - [x] Completion evidence: replacement summary distinguishes graph replacement,
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
- [x] PR 2: Remove direct Kuzu reads from one low-risk graph read route.
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
- [x] PR 3: Repeat route runtime migration for the remaining graph-first reads.
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
  - [x] Stop re-exporting CLI-style `run_nowledge_*` command adapters from the
    crate root, so embedded Mem callers default to typed library APIs while
    developer wrappers remain isolated in CLI modules.

## P0: Graph Kernel Compatibility

- [x] Finish the Nowledge-used Cypher subset.
  - `MATCH`, one-hop and bounded multi-hop patterns.
  - `WHERE` equality, range, boolean, null, and list membership predicates.
  - `RETURN`, aliases, aggregation, ordering, offset, and limit.
  - `CREATE`, `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`.
  - Nowledge schema DDL and migration statements.
- [x] Keep parser output syntax-only.
  - Parameter binding, catalog lookup, type checks, and semantic validation stay
    outside the parser.
  - Fast paths should be selected from simple AST shape checks, not string
    matching.
  - [x] Expose a typed Nowledge Mem fast-path classifier over parsed
    `cypher::Statement` and test that whitespace/case variants with the same
    AST shape make the same fast-path decision.
- [x] Strengthen planner, optimizer, and executor ownership.
  - Use Cascades groups, logical rules, implementation rules, physical
    properties, and deterministic costs for non-trivial graph reads.
  - Keep storage-specific choices in catalog metadata and physical rules, not in
    parser or route handlers.
  - [x] Surface structured optimizer rule-event counts through query reports,
    query-runtime preflight, graph-route readiness, integration readiness, and
    final preflight so replacement gates do not depend only on free-form
    decision text.
- [x] Maintain stable Nowledge API behavior.
  - Preserve node, relationship, metadata, pagination, and ordering contracts.
  - Preserve `include_metadata=false` metadata stripping behavior.
  - Compare row shape and error class before allowing replacement readiness.
  - [x] Require graph-route query reports to carry compact API behavior evidence
    proving `include_metadata=false` strips metadata before route cutover
    readiness can pass.
  - [x] Require graph-route query reports to carry compact output row-shape
    evidence before route cutover readiness can pass.
  - [x] Require graph-route query reports to carry compact API behavior evidence
    for ordering, pagination, and error-class stability before route cutover
    readiness can pass.
  - [x] Expose graph-route API behavior evidence as aggregate route and
    readiness counters so replacement gates can consume API compatibility
    directly.
  - [x] Require replacement summary to consume graph-route API behavior
    aggregate evidence before production cutover can pass.
  - [x] Require final previous-wrapper preflight to validate graph-route API
    behavior aggregate evidence from replacement summary before release gates
    can pass.
  - [x] Require route ownership, bounded-read, library-readiness, and
    integration alignment typed paths to consume graph-route API behavior
    evidence before accepting Skein-owned routes.
  - [x] Require replacement summary and final previous-wrapper preflight to
    consume bounded-read graph-route API behavior evidence before bounded-read
    gates can pass.

## P0: Storage and Recovery

- [x] Keep WAL and checkpoint recovery as cutover blockers.
  - Mutations must recover as whole committed batches or not at all.
  - Torn WAL tails must be detected and bounded.
  - Checkpoint manifests must include replay boundaries.
  - [x] Redact storage-recovery evidence JSON parse failures by default so WAL
    paths and recovery payload fragments are not copied into command errors.
  - [x] Require storage-recovery, integration-readiness, and final preflight
    gates to validate replay LSN and recovered commit-epoch boundaries instead
    of trusting `ready=true`.
  - [x] Cover a Mem-shaped post-checkpoint relationship batch followed by a torn
    WAL tail, proving the committed batch is recovered while the torn tail
    still blocks readiness.
  - [x] Require typed Mem storage-recovery reports to fail closed when checkpoint
    commit epoch, WAL replay LSNs, replayed entry count, and recovered commit
    epoch form an inconsistent replay boundary.
  - [x] Require replacement summary to recompute storage-recovery raw fields,
    including replay-boundary consistency, instead of trusting
    `storage_recovery_ready=true`.
  - [x] Cover Mem-shaped post-checkpoint WAL batches that update an old Memory,
    create replacement Memory nodes, and create an `EVOLVES` relationship while
    proving a torn following batch is ignored without partial recovery.
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
  - [x] Expose query-runtime scan-pruning evidence for parameterized normalized
    default equality and inequality predicates used by Nowledge thread-space
    repair reads.
- [x] Keep memory use bounded by default.
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
  - [x] Require blackbox background QoS readiness to include deferral and
    rejection counters, graph-delta operation counts, watermark evidence, and
    memory-pressure budget fields before operational evidence can pass.
  - [x] Require Mem integration readiness to consume strict blackbox background
    QoS evidence so missing memory-pressure budgets block production cutover.
  - [x] Redact background-maintenance evidence JSON parse failures by default
    so local artifact paths and QoS payload fragments are not copied into
    command errors.
  - [x] Apply incremental search projection deltas in place after fail-fast
    validation, avoiding a full document-map clone on steady-state background
    maintenance updates.
  - [x] Require background-maintenance cutover evidence to include compact
    slow-query readiness, capacity, record-count, and redaction signals.
  - [x] Require required background-maintenance evidence to include
    memory-pressure budget fields, and emit those fields from library-generated
    background-maintenance reports.
  - [x] Require Mem integration background-maintenance cutover readiness to
    consume memory-pressure budget fields, so forged `ready=true` evidence
    cannot bypass resource-budget gates.

## P0: Search Projection Replacement

- [x] Continue replacing LanceDB only as a rebuildable search projection.
  - Canonical facts remain graph/content state, not vector index state.
  - Search projection evidence must prove row count, document identity, embedding
    identity, lifecycle, and incremental watermark parity.
  - Keep content store replacement out of this milestone unless a search or
    graph route needs it.
  - [x] Require final previous-wrapper preflight to consume search projection
    document identity evidence and shadow document-identity parity before
    LanceDB replacement gates can pass.
  - [x] Require final previous-wrapper preflight to validate search projection
    evidence protocols and Rust-library shadow evidence source before LanceDB
    replacement gates can pass.
- [x] Keep FTS and vector projection maintenance incremental.
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
  - [x] Require search projection descriptor evidence to expose `document_id`
    unique-key summaries, and make replacement/integration readiness fail
    closed when unique-key scan-pruning capability is missing.
  - [x] Require final previous-wrapper preflight to consume search projection
    production-filter pruning readiness and descriptor field-summary
    capabilities before LanceDB replacement gates can pass.
  - [x] Redact search-projection evidence JSON read and parse failures by
    default so local paths and document identity payload fragments are not
    copied into command errors.
  - [x] Keep steady-state projection delta application incremental in memory by
    validating operation limits and embedding dimensions before mutating rows,
    then applying deletes/upserts without cloning the whole projection.
- [x] Replace LanceDB search reads in stages.
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
    - [x] Require route ownership cutover readiness to reject a Skein-owned
      `/graph/search` route unless search projection replacement evidence is
      present, while leaving readiness details to the dedicated search
      projection gate.
- [x] Add retrieval projection options behind advisor gates.
  - Raw float32 or SQ8 remains the safe path.
  - TurboQuant-style compressed projections can be used for cold or constrained
    local segments only after recall and parity evidence is available.
  - [x] Gate advanced compressed vector retrieval modes behind typed recall,
    parity, and cold/local-segment advisor evidence for embedded store opens
    and search-candidate requests.
  - [x] Add a unified filtered-vector execution path.
    - Push `WHERE` predicates for lifecycle, unit type, metadata, numeric
      ranges, timestamp ranges, and unique document ids into segment descriptors
      before vector scoring whenever the predicate is descriptor-safe.
    - Keep an iterative-filter fallback for complex predicates: bounded vector
      candidate generation first, scalar predicate evaluation second, and repeat
      until `topK` is satisfied or the budget is exhausted.
    - Preserve scalar cosine scan as the exact baseline for small filtered
      candidate sets and high-filter-ratio workloads.
  - [x] Split vector retrieval into candidate generation plus raw-vector
    reranking.
    - Treat compressed or ANN results as candidates only.
    - Rerank the bounded candidate window against raw vectors before returning
      scores to callers.
    - Report both approximate score source and final raw-vector score source.
  - [x] Add an adaptive vector backend selector.
    - Prefer flat scan for small tables, high-filter-ratio predicates, and
      recall-validation probes.
    - Prefer compressed IVF/SQ/PQ-style projections for constrained local
      hardware when memory budgets are tight.
    - [x] Select after metadata pruning from typed candidate count,
      filter-selectivity, raw-vector byte estimate, host memory budget, and
      projection coverage inputs.
    - [x] Expose host-owned policy overrides and stable selection reasons through
      candidate reports, explain analyze, slow logs, and blackbox summaries.
    - [x] Keep required compressed projections fail closed when the artifact is
      unavailable or does not cover the filtered candidate set.
    - Keep graph-heavy indexes such as HNSW optional because they can add large
      resident memory overhead.
    - Consider DiskANN-style or mmap-backed layouts only after local vector
      payloads exceed the configured memory budget.
  - [x] Add sampled recall validation for approximate vector paths.
    - Compare ANN or compressed-vector results against scalar flat-scan
      ground truth on bounded sampled queries.
    - Track recall@k, overlap@k, fallback counts, and filter selectivity.
    - Make production readiness fail closed when recall evidence is absent for
      a required approximate backend.
  - [x] Extend vector observability in explain, explain analyze, slow log, and
    blackbox reports.
    - [x] Carry typed execution counters for candidate scan rounds,
      descriptor-pruned and scalar-filtered candidates, raw-vector bytes read,
      and index coverage through the search retriever report.
    - [x] Propagate typed vector execution reports through `EXPLAIN ANALYZE`,
      query reports, redacted slow-query events, and aggregate blackbox JSONL
      summaries for optimizer-owned vector queries.
    - Include vector backend, compressed projection mode, candidate count,
      descriptor-pruned count, scalar-filtered count, rerank count, raw-vector
      bytes read, index coverage, and fallback reason codes.
    - Keep reports compact and never copy raw embedding values.
  - [x] Add vector seed as a graph query operator.
    - [x] Add parameterized `CALL vector_search($embedding, topK := n)` as an
      optimizer-visible exact scalar `VectorSeedScan`, with plan-cache bypass,
      embedded search-projection injection, and fail-closed graph-only
      execution.
    - Model semantic search as a bounded candidate-producing operator, not as a
      standalone answer path.
    - Feed seed document, entity, or memory ids into graph plans as a typed
      candidate set that can participate in `MATCH` and `WHERE`.
    - Let the optimizer choose whether descriptor-safe filters run before vector
      seed generation or after bounded candidate generation.
  - [x] Add graph-constrained retrieval plans.
    - Use vector seeds to filter the graph, then perform bounded 1-2 hop
      expansion by relation type and candidate budget.
    - Keep graph edges as first-class records in the plan instead of treating the
      graph as a post-search primary-key lookup.
    - Report seed count, expanded node count, expanded edge count, relation
      types, hop count, rerank count, and payload byte budget usage.
  - [x] Add schema-guided query-generation support for GraphRAG callers.
    - Expose compact label, relationship type, property, common-path, and route
      catalog summaries for LLM-assisted Cypher generation.
    - Keep schema context generated from stable graph metadata and Nowledge
      ontology, not from free-form LLM-created labels or edge types.
    - Validate typed node and observed one-hop route drafts against a pinned
      schema fingerprint before rendering bounded, parameterized Cypher.
    - Validate typed two-hop drafts by composing two observed routes with an
      explicit intermediate label and independently addressable edge bindings.
    - [x] Seal generated queries and verify schema-context integrity before
      generation so callers cannot bypass draft validation with invented
      identifiers.
    - [x] Carry schema-derived scalar/list parameter requirements into
      generated queries, reject missing, unexpected, incompatible, or
      conflicting bindings before execution, and reject a query generated for a
      different pinned graph epoch.
      - Core generation tests cover parameter conflicts; read-transaction API
        tests cover missing, unexpected, scalar/list mismatch, and stale
        schema-epoch rejection before canonical execution.
    - [x] Execute validated generated queries through the read transaction's
      canonical parser, optimizer, plan cache, profiler, and executor path;
      do not add a GraphRAG-specific interpreter.
    - [x] Expose schema context and bounded generated-query execution through
      the `NowledgeMemEmbeddedStoreHandle` library API so application-bound
      callers do not need direct `Database` ownership.
    - [x] Return required parameter names and reject invented identifiers,
      unavailable bindings, properties outside the context, and stale schema
      drafts before parsing or planning.
    - [x] Run generated queries through the normal parser, optimizer, execution
      profile, and slow-query/blackbox reporting path.
    - [x] Feed a schema-guided GraphRAG probe into the workload-fixture library
      readiness evidence so cutover gates can reject missing schema context,
      draft validation, bounded generated-query execution, or unsafe runtime
      shape before Mem enables GraphRAG-backed reads.
    - [x] Require final previous-wrapper preflight to consume the library
      workload-fixture GraphRAG probe details directly, so release evidence
      fails closed when schema context, draft validation, bounded execution, or
      safe runtime-shape proof is missing.
    - [x] Carry GraphRAG workload-fixture probe details through replacement
      summary as well, so intermediate cutover evidence cannot hide missing
      schema-guided query generation readiness behind generic workload status.

## P1: Operability

- [x] Add production deployment profiles and optional capability gates.
  - [x] Expose a `DesktopBound` profile for an application-bound, in-process
    database with bounded plan cache, FTS, vector, analytics, and background
    maintenance defaults.
  - [x] Expose a `MobileEmbedded` profile with strict memory, result, plan-cache,
    replay, and incremental-change-log defaults.
  - [x] Disable optional heavy mobile capabilities through typed runtime
    capability gates before adding compile-time feature removal.
  - [x] Give each profile separate CPU and storage I/O budgets with explicit host
    I/O-depth overrides.
  - [x] Add a storage-facing range scheduler that coalesces adjacent reads and
    creates bounded SSD/NVMe I/O waves.
  - [x] Execute scheduled file ranges with bounded per-wave memory and ordered
    payload consumption.
  - [x] Add checksummed physical byte ranges and independent Zstandard frames to
    persisted search segments.
  - [x] Execute pruned candidate ranges through the storage backend in the
    production search path while keeping WAL, manifest publication, and
    per-index delta ordering serialized; propagate range I/O errors without a
    silent in-memory fallback.
  - [x] Add platform-specific device discovery without deriving a claimed SSD
    channel count from CPU count alone.
  - [x] Keep the durable storage format and core Cypher semantics compatible
    across profiles; return typed capability-unavailable errors instead of
    silent unbounded fallbacks.
  - [x] Add build-time feature gates for optional mobile capabilities after the
    runtime capability contract is stable.
- [x] Add optional ACL support after the embedded read/write contract is stable.
  - [x] Keep ACL disabled by default and compile-time removable on mobile.
    - Added the non-default `acl` Cargo feature and `AccessControl` runtime
      capability. Desktop and mobile profiles keep ACL off unless the host
      explicitly enables it and the feature is compiled in.
  - [x] Bind authorization context and policy epoch into binder/planner/executor
    and plan-cache contracts.
    - [x] Bind search candidate reports to a typed access-control policy epoch
      when callers use the explicit ACL search API.
    - [x] Extend Cypher query-runtime planning and plan-cache keys so explicit
      ACL graph query plans cannot cross policy epochs.
      - Added `QueryAccessControlContext` and explicit library query/explain
        entrypoints. The default query path remains unchanged, while ACL query
        planning requires the `AccessControl` runtime capability, rejects a
        zero policy epoch before cache lookup, and records only the policy epoch
        binding in optimizer trace decisions.
      - Runtime capability tests cover fail-closed disabled ACL, zero policy
        epoch rejection, and LFU plan-cache isolation across policy epochs.
    - [x] Bind explicit ACL contexts into executor-facing physical plans and
      slow-query observability without introducing a production CLI or global
      environment control plane.
      - Search, node scan, adjacency expansion, shortest path, and graph
        algorithm execution all apply policy-visible predicates before result
        projection. Slow-query records carry only the numeric policy epoch for
        explicit ACL queries.
  - [x] Enforce visibility before payload materialization where storage
    metadata permits it; result-only filtering is not sufficient.
    - [x] Add `SearchAccessControlContext` and
      `try_search_with_options_access_control`, which inject descriptor-safe
      visibility predicates into the search segment-pruning path before ranking
      while keeping ordinary no-ACL search unchanged.
    - [x] Extend the same policy-visible predicate contract to query-runtime
      node scans.
      - `QueryAccessControlContext::visibility_scope(s)` now carries a
        descriptor-safe visibility property and allowed values. Explicit ACL
        graph queries inject a `PropertyIn` predicate into logical `NodeScan`
        before optimization, so the selected physical plan contains
        `FilterExec(SeqNodeScan)` and the executor can use
        `ScanPruningStrategy::PropertyIn` before result projection.
      - All visibility inputs stay out of plan-cache keys and traces except
        for the policy epoch; ordinary no-ACL query APIs remain unchanged.
    - [x] Extend policy-visible predicates to adjacency expansion endpoint
      materialization.
      - Explicit ACL graph queries now wrap logical `Expand` targets with a
        visibility `PropertyIn` predicate. The executor recognizes simple
        target-only predicates above `AdjacencyExpandExec` and applies the
        target property filter before creating endpoint bindings, while still
        evaluating the predicate after expansion for correctness.
      - Runtime capability tests cover a `Memory` seed that points to visible
        and hidden `Entity` endpoints; only the visible endpoint is projected.
    - [x] Extend policy-visible predicates to shortest-path node
      materialization.
      - Explicit ACL graph queries now bind source and target visibility
        predicates into `ShortestPathExec`. The executor checks endpoint
        visibility before path search and applies the same node visibility
        filter while expanding BFS candidates, so hidden intermediate nodes
        cannot appear through `properties(nodes(p), ...)` projections.
      - Runtime capability tests cover visible endpoints connected only through
        a hidden intermediate node; the ACL path query returns no rows and the
        physical fingerprint records both endpoint visibility predicates.
    - [x] Extend policy-visible predicates to graph-algorithm endpoint
      materialization so every graph read operator has the same fail-closed
      visibility boundary.
      - Explicit ACL graph queries now bind a node visibility predicate into
        `GraphAlgorithm`. When that predicate is present, the executor rebuilds
        the named graph definition as a visibility-filtered graph instead of
        reusing an unscoped projected-graph artifact, then rechecks emitted
        node ids before result projection.
      - Runtime capability tests cover a named graph that was projected with a
        hidden node before the ACL query; PageRank returns only visible nodes
        and the physical fingerprint records `node_visibility`.
  - [x] Fail closed on missing, stale, or unsupported policy state and keep
    credentials and policy inputs out of telemetry.
    - [x] Explicit ACL search fails closed when access control is disabled,
      when the policy epoch is zero, when visibility metadata is missing, or
      when search options carry a conflicting policy epoch.
    - [x] Search telemetry exposes only the policy epoch and filtered counts;
      visibility values are not copied into reported metadata filters.
    - [x] Add readiness and slow-log checks for stale policy state once graph
      query ACL contexts exist.
      - `access_control_policy_readiness` reports missing, invalid, stale, and
        capability-disabled policy state with stable blocker codes while
        exposing only required and observed policy epochs.
      - Slow-query summaries, SQL system-table rows, and JSONL events expose
        only `access_control_policy_epoch`; visibility property names and
        allowed values remain out of default telemetry and redaction metadata.
- [x] Add compact readiness dashboards for route, query-family, storage, search,
  and background-maintenance blockers.
- [x] Add stable counters for plan cache hit, miss, admission, eviction, and
  memory pressure.
- [x] Complete the multi-reader, single durable writer library contract.
  - [x] Cover bounded LFU plan cache invariants with a feature-gated Loom model
    in CI.
  - [x] Cover local QoS scheduler background admission and permit accounting
    with a feature-gated Loom model in CI.
  - [x] Cover bounded slow-query ring capacity and sequence invariants with a
    feature-gated Loom model in CI.
  - [x] Cover the embedded Mem library handle with a multi-threaded query,
    slow-query, and readiness-dashboard access test.
  - [x] Remove whole-store read serialization from the embedded Mem handle.
  - [x] Prove overlapping handle reads and model crash-safe
    durable-before-publish behavior.
  - [x] Prove overlapping pinned query execution while commits remain serialized and
    durable-before-publish.
- [x] Complete host-owned OpenTelemetry coverage.
  - [x] Provide an optional OpenTelemetry metrics adapter that accepts a
    host-provided `Meter` and never initializes global telemetry state.
  - [x] Emit low-cardinality query count, duration, row count, success, language,
    and statement-kind metrics without query text or parameters.
  - [x] Add typed, low-cardinality WAL, checkpoint, recovery, and search
    checkpoint metrics with bounded exporter behavior owned by the host.
  - [x] Add background QoS admission and completion metrics without coupling
    the scheduler crate to a global telemetry provider.
- [x] Complete durable incremental-index catch-up.
  - [x] Distinguish the applied source graph epoch from the durable checkpoint
    watermark and report uncheckpointed changes.
  - [x] Add a bounded library catch-up loop that truncates changefeed batches at
    complete commit boundaries and checkpoints each applied projection batch.
  - [x] Persist ordered delta identity and resume catch-up from the last durable
    watermark without requiring a full rebuild.
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
  - [x] Expose a typed library production-path summary and require integration
    readiness to fail closed when production depends on CLI wrappers,
    environment control planes, or spawned helper processes.
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

- [x] Split more internal packages into focused crates to improve abstraction
  boundaries and compile-time ownership.
  - Do not split crates for their own sake; every new crate must have a clear
    ownership boundary, dependency-direction benefit, compile-time isolation
    benefit, or stable reuse contract.
  - Use a Polars/RisingWave-style workspace layout where stable contracts live
    in small crates and heavy implementations depend inward, not sideways.
  - Candidate split targets: core value/types/error, parser/AST, logical plan,
    optimizer rules, physical executor, storage/WAL/checkpoint, search
    projection, readiness/evidence, and Nowledge Mem facade.
  - [x] Split stable readiness area summary/map contracts into
    `skein-readiness` while keeping the top-level `skein` facade API and JSON
    contract unchanged.
  - Keep the top-level `skein` crate as the SQLite-like embedded library facade;
    do not expose internal crates as production integration points until their
    APIs are stable.
  - Migration should be mechanical and test-preserving first; behavioral
    refactors happen after crate boundaries compile cleanly.
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

- [x] Advanced graph algorithms beyond Nowledge's active routes.
- [x] Broad openCypher compatibility not exercised by Nowledge Mem.
- [x] Distributed storage, replication, or cloud-primary execution inside the
  local embedded engine.
- [x] Aggressive SIMD work unless route-level evidence shows it is needed.
