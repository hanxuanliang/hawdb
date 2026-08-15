# Skein TODO

This file contains only actionable, incomplete work. Completed behavior belongs
in the contracts indexed by [`docs/specs/README.md`](docs/specs/README.md), in
supporting design documents, and in Git history. Do not use checked tasks as a
second specification or completion archive.

The scope remains product-driven: Skein is a single-process embedded Rust
database for Nowledge graph and search workloads. Multi-process writers,
distributed replication, cloud-primary execution, broad openCypher coverage,
and algorithms outside active routes are not implied backlog items.

## P0: Production Release Blockers

- [ ] Qualify storage on a representative production replica.
  - Run the typed larger-than-cache resource profile against a representative
    Mem replica with canonical bytes exceeding the configured cache.
  - Record revision, target, feature set, configuration digest, dataset
    fingerprint, steady and peak RSS, page faults, intermediate rows, payload
    bytes, cache residency, spill, and admission counters.
  - Bind the artifact to the release readiness bundle and reject stale or
    mismatched evidence.
  - Acceptance: traffic readiness is derived from the production-copy report,
    not from an ignored synthetic test.

- [ ] Qualify the generation-bound out-of-core search path on production-shaped
  data.
  - Run exact text, vector, and hybrid parity for metadata, lifecycle,
    incremental, checkpoint, reopen, and corruption cases. Include ACL parity
    only when the release feature set enables `acl`.
  - Exercise at least 100,000 documents and a corpus larger than the admitted
    search memory budget.
  - Record P50, P95, and P99 latency, RSS, page faults, posting and sidecar
    bytes, hydration bytes, update latency, and checkpoint amplification.
  - Exercise the bounded generation-delta merge and require zero resident
    corpus documents while old-generation reads overlap publication.
  - Exercise `Preferred` and `Required` TurboQuant out-of-core serving with
    metadata allowlist pushdown and bounded raw-vector late reranking.
  - Run those probes on the exact source generation recorded by the artifact,
    not only on a disposable post-update generation, and record lifecycle RSS
    and page faults before opening the full-residency oracle.
  - Require the selected projection generation, analyzer identity, embedding
    identity, source graph epoch, and release revision to match the evidence.
  - Acceptance: production routes open the out-of-core facade through its
    production constructor and the final cutover report recomputes readiness
    from the bound raw evidence.

- [ ] Qualify the default TurboQuant candidate projection on representative
  Mem embeddings.
  - Run `run_production_vector_qualification` on representative embeddings,
    compare 4-bit candidate recall against canonical raw-vector TopK, and use
    `skein-qualification/turbovec-oracle` as a differential oracle, not as
    truth.
  - Cover unfiltered, metadata-filtered, incremental, checkpoint, reopen,
    stale-generation, corruption, cancellation, and mixed-load cases. Include
    ACL-filtered cases only when the release feature set enables `acl`.
  - Record candidate recall, final raw-reranked recall, P50/P95/P99 latency,
    steady and peak RSS, page faults, projection bytes, build amplification,
    skipped blocks, admitted workers, and kernel selection.
  - Require evidence for Windows x86_64, Linux x86_64, Linux AArch64, macOS
    AArch64, and the scalar reference before production admission.
  - Bind every target to the exact search generation, document digest, analyzer
    digest, embedding identity, and source graph epoch accepted by the search
    qualification artifact.
  - Keep the full-residency recall oracle offline and require its document,
    analyzer, embedding, and epoch identity to match the released out-of-core
    search generation.
  - Measure the released out-of-core TurboQuant artifact before opening the
    oracle; require serving/oracle final-result parity, payload I/O, raw
    reranking, and cancellation propagation from the serving path. Keep
    candidate recall in the identity-bound offline oracle so serving does not
    retain candidate IDs only for qualification.

## P1: Runtime And Availability Hardening

- [ ] Qualify default parallel morsel execution on production-shaped workloads.
  - Require higher throughput without a p99, peak RSS, cancellation-latency, or
    foreground-admission regression across 4, 8, and 16 workers.

- [ ] Close the remaining blocking-operator availability gaps for active
  workloads.
  - Capture production-route evidence for high-cardinality `DISTINCT` and
    Cartesian build sides through `run_production_blocking_qualification`.
  - Require the existing ordered distinct spill and partitioned Cartesian build
    spill to remain within byte, run, cleanup, and admission limits.
  - Accept an in-memory result only when route-bound evidence proves it remains
    within admission; do not weaken the blocking-operator memory limit.

## P1: Persistent Row And Index Storage

The immutable relational index codec, generation-fenced publisher, demand
reader, bounded page-cache integration, WAL recovery delta, differential
qualification, opt-in PostgreSQL SQL execution path, and checkpoint-bound
artifact identity with backup/restore/scrub coverage are implemented. The
remaining work below makes those derived foundations canonical without allowing
a stale index or a database-sized resident set to become a correctness
dependency. The storage-owned relational snapshot reader now binds one exact
checkpoint, recovery delta, and immutable live view; it performs bounded
live-over-recovery-over-checkpoint point and ordered range reads without a
database-sized base-row collection. Ordinary live admission failures reject
before WAL append, while schema-changing WAL is followed by a mandatory
manifest-last canonical row checkpoint that writable recovery retries after a
crash; read-only recovery remains fail closed.

- [ ] Add Windows rename, reopen, backup, and reclaim fault-injection coverage
  for the exact canonical row/overflow-root path.

- [ ] Qualify canonical row pages for the first Mem relational tables.
  - The SQL runtime now selects one exact checkpoint/recovery/live snapshot
    reader, rejects an unavailable reader, and uses canonical memory only before
    the first checkpoint or inside a transaction-private workspace. Keep this
    engine contract while qualifying the product cutover.
  - The typed runner now qualifies `content_documents`, `thread_messages`,
    `content_chunks`, and `content_anchors` using the frozen PostgreSQL statement
    corpus and graph-plus-relational commits that publish one shared epoch.
  - The four-table runner now proves authoritative checkpoint/reopen,
    exact per-statement row/payload admission, cold/warm result identity, WAL
    recovery delta, live row overlay, cache accounting, and zero leaked page
    pins. Source chunks retain stable source/chunk ordering, and anchors retain
    occurrence identity even when two messages share one legacy `message_id`.
    The `upsert_source_chunks` caller now has complete mixed-transaction
    evidence for shorter and empty whole-document replacement, duplicate-order
    statement rollback, graph/document count agreement, live visibility, and
    checkpoint/reopen identity. Remaining `partial` callers stay blocked.
    The `patch_source_chunks_space` caller now has complete mixed-transaction
    evidence for graph/document workspace agreement, read-your-own-writes,
    exact chunk-count and payload preservation, missing-owner no-op behavior,
    live visibility, and checkpoint/reopen identity. The other `partial`
    callers remain blocked on their own qualification evidence.
    The `patch_thread_space_ownership` caller now has a guarded mixed-source
    batch qualification: graph Thread, relational document, and messages agree
    in one epoch, matching previews move, a stale preview remains unchanged,
    and non-ownership payloads survive live reads and checkpoint/reopen. Other
    `partial` callers remain blocked on independent evidence.
    The `patch_moved_space_ownership` caller now composes selected Threads and
    Sources in one guarded mixed transaction, reports exact document/message
    changes, proves stale selection remains unchanged, and retains payload and
    ordered-read identity through checkpoint/reopen. The remaining
    write/reconcile/delete callers stay blocked on their own gates.
  - Require checkpoint/reopen, WAL replay, corruption, cancellation, and
    locking evidence before enabling the path by default. Prove that a declared
    bounded workload runs within the supported 512 MiB low-memory profile, but
    evaluate cold/warm latency, RSS, page faults, and write amplification on the
    production replica's actual configured resource profile; 512 MiB is not a
    universal activation cutoff.
  - The authoritative transaction core now pins one persistent base and merges
    a bounded private index overlay across SQL statements. The typed runner now
    executes a Content Store-shaped message UPSERT, page read, rejected foreign
    key statement, payload aggregate, and document-summary update as one group,
    and proves transaction-workspace routing plus atomic publication. The same
    runner now proves a cancelled Content Store point read is
    non-poisoning and pin-clean, and that `FOR UPDATE` makes a same-key UPSERT
    time out and abort without changing the row or commit epoch. A disposable
    backup/restore probe now bit-flips the current row-page artifact, requires
    scrub to poison the damaged handle, rejects later SQL, and proves the source
    database remains unchanged. The resource probe now records its declared and
    detected memory profile, all relevant query/cache budgets, warm-read
    percentiles, steady/peak RSS, page faults, WAL bytes, new immutable
    generation bytes, and a clearly labelled durable-write lower bound. A
    regular in-process run does not certify an OS-enforced 512 MiB limit: retain
    the isolated constrained-profile run and production-copy measurements as
    separate evidence gates.
  - Because no storage format has shipped, activation is destructive: remove
    the ordinary materialized row selector instead of retaining a compatibility
    or rollback path. Keep the differential oracle in qualification code only.

- [ ] Persist and activate the remaining canonical graph indexes.
  - Publish stable-id, composite-property, relationship-property, forward
    adjacency, and reverse adjacency roots with the canonical graph epoch.
  - Activate each index class independently after differential, recovery,
    cache-budget, and production-shaped evidence; derived BM25, vector,
    statistics, analytics, and optional columnar projections remain outside
    canonical recovery.

## P1: PostgreSQL-Dialect Relational Content Store

This work reopens a deliberately deferred boundary: selected durable row and
large-value responsibilities currently owned by Mem's SQLite `content.db` may
move into Skein. PostgreSQL defines the SQL syntax and semantics; it is not a
runtime dependency and this backlog does not replace PostgreSQL in Nowledge
Cloud. The initial scope is `content_documents`, `thread_messages`,
`content_chunks`, `content_anchors`, and their migration state. External
artifact/blob files remain sidecars until a separate workload and recovery
qualification justifies moving them.

- [ ] Qualify the partial callers in the frozen SQLite-to-Skein statement
  corpus.
  - The versioned corpus, real v3 schema, parameter/result contracts, source
    inventory, and revision digest are specified by
    `docs/specs/POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`.
  - Complete the remaining graph-plus-relational write ownership for every
    caller currently classified as `partial`; do not infer readiness from
    parser feature counts. `upsert_source_chunks` is now `covered`; ownership
    moves, thread reconciliation, tail deletion, and whole-thread deletion
    remain incomplete.
  - Acceptance: every active Mem caller is `covered`, and cutover fails closed
    when the corpus protocol, revision, or digest differs from the qualified
    Skein artifact.

- [ ] Materialize the scoped Content Store schema and behavior in Skein.
  - Define PostgreSQL-dialect migrations for `content_documents`,
    `thread_messages`, `content_chunks`, `content_anchors`, and the durable
    migration ledger without editing an already-applied migration.
  - Preserve occurrence identity, stable ordering, content hashes,
    distillation exclusions, metadata text, source-chunk ownership, and legacy
    anchor matching semantics.
  - Replace SQLite repository helpers with small named SQL statements for exact
    lookup, bounded page, aggregate/count, tail guard, candidate page, and
    bounded hydration phases.
  - Route graph identity/relationship changes and their content rows through
    the canonical mixed commit already owned by `GraphStore`; do not retain a
    host-side dual-write boundary after cutover.
  - Acceptance: thread append/reconcile/tail delete, whole-thread delete,
    source-chunk replacement, ownership move, anchor creation, and projection
    rebuild all have exact behavior fixtures.

- [ ] Add an idempotent, resumable SQLite-to-Skein migration coordinator in
  Mem.
  - Keep `rusqlite` and SQLite snapshot handling in the Mem adapter; Skein must
    not acquire SQLite as a production dependency.
  - Acquire an explicit legacy write fence or run a durable dual-write
    obligation protocol before copying. Record database identity, schema
    checksums, source snapshot identity, import id, and source high watermark.
  - Copy by stable keyset pages with bounded payload bytes, persist the cursor
    after each committed Skein batch, and make replay idempotent by primary key
    and content hash.
  - Verify per-table counts, ordered identities, payload hashes, aggregate
    totals, anchor reachability, and representative query results before
    generating cutover evidence.
  - Preserve the SQLite database unchanged through qualification and rollback;
    destructive cleanup is a later, separately authorized step.
  - Acceptance: restart at every page and cutover boundary converges without
    missing or duplicate rows, and writes accepted during migration are either
    replayed or prevent cutover.

- [ ] Close the production evidence and decommissioning gates.
  - Add a differential oracle that runs the frozen statement corpus against one
    SQLite snapshot and one Skein snapshot, comparing values, nulls, ordering,
    errors, and transaction outcomes rather than only row counts.
  - Exercise empty stores, duplicate legacy message ids, duplicate order
    indexes, non-ASCII and large content, 50,000-message threads, large source
    corpora, protected tail deletion, cross-space anchors, interrupted writes,
    torn WAL tails, checkpoint/reopen, backup/restore, and corruption.
  - Benchmark cold and warm P50/P95/P99 latency, throughput, database and WAL
    bytes, write amplification, compression ratio, steady/peak RSS, page
    faults, spill, and decompressed bytes on Windows, Linux, and macOS.
  - Run shadow reads and durable dual writes on a production copy, bind parity
    and recovery artifacts to the exact Skein revision and statement-corpus
    revision, then fail closed on stale or incomplete evidence.
  - Remove the SQLite runtime, backup/export branch, and `nmem-content`
    repository only after portable export/import, doctor, projection rebuild,
    rollback, and route ownership all select Skein with no fallback.

## P2: Deferred Delivery Governance

- [ ] Adopt `main` branch protection when Skein enters a release-candidate or
  general-availability phase.
  - Keep direct pushes available during the current rapid-iteration phase.
  - Production release evidence must still bind to an exact revision with green
    required CI; an unprotected branch is not evidence that a red revision is
    production ready.
  - Before general availability, require pull requests, supported Cargo and
    Bazel checks, and prohibit force pushes and branch deletion.

## P2: Deferred Correctness Oracles

- [ ] Add NoREC only after a supported Cypher or SQL subset can express the
  general row-wise boolean-count relation without a fuzz-only executor.

## P2: Deferred Replica Repair

- [ ] Add replica-assisted storage repair after Skein has a replication layer.
  - Require an exact database identity, manifest lineage, generation, LSN range,
    and content-digest match before accepting repair bytes from a follower.
  - Stage and verify replacement WAL or segment data before atomic publication;
    never extend tolerant open into an implicit replica-repair path.
  - Record the source replica, repaired range, and old/new digests in a durable
    repair audit record.
  - Fall back to verified backup restore when no follower covers the missing
    commit. Any lossy salvage must target a new database directory and require
    explicit authorization while preserving the original database unchanged.
