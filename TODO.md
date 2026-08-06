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

- [ ] Complete the query-first public API convergence.
  - Migrate the remaining single-query `Database::knowledge_*` compatibility
    wrappers and their tests to parameterized Cypher.
  - Add bounded `system.*` tables before removing any remaining typed catalog,
    statistics, runtime, or projection introspection surface.
  - Remove route-only request/output DTOs and the `skein-api-types` crate after
    the workspace and the Mem host have no remaining references.
  - Preserve typed APIs for grouped WAL atomicity, recovery, admission,
    generation publication, and bounded multi-statement workflows.

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

- [ ] Add NoREC only after the supported Cypher subset can express the general
  row-wise boolean-count relation without a fuzz-only executor.

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
