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
  - Run exact text, vector, and hybrid parity for metadata, ACL, lifecycle,
    incremental, checkpoint, reopen, and corruption cases.
  - Exercise at least 100,000 documents and a corpus larger than the admitted
    search memory budget.
  - Record P50, P95, and P99 latency, RSS, page faults, posting and sidecar
    bytes, hydration bytes, update latency, and checkpoint amplification.
  - Require the selected projection generation, analyzer identity, embedding
    identity, source graph epoch, and release revision to match the evidence.
  - Acceptance: production routes open the out-of-core facade through its
    production constructor and the final cutover report recomputes readiness
    from the bound raw evidence.

## P1: Runtime And Availability Hardening

- [ ] Add a backpressured asynchronous row-consumer API.
  - Preserve the admitted Tokio facade and cancellation/deadline semantics.
  - Deliver bounded batches without collecting the complete result into a
    `Vec` before returning control to the host.
  - Bound channel capacity and payload bytes, propagate consumer cancellation,
    and keep mutation execution serialized.

- [ ] Close the remaining blocking-operator availability gaps for active
  workloads.
  - Capture route evidence for high-cardinality `DISTINCT` and Cartesian build
    sides before adding storage complexity.
  - If active workloads exceed the in-memory budget, add byte- and run-bounded
    external distinct and a partitioned product/join strategy.
  - Otherwise encode the accepted route-bound admission limit and stable
    resource error in readiness evidence.
  - Do not weaken the existing blocking-operator memory limit.

- [ ] Make production runtime admission non-ambiguous.
  - Define which embedded query entrypoints are production admitted and report
    raw `Database` access as non-production-safe unless the host supplies an
    equivalent global governor.
  - Prove the real Mem serving path uses the admitted facade for foreground
    query, mutation, analytics, and maintenance work.
  - Keep direct low-level access available for tests and controlled hosts
    without allowing it to satisfy production-path readiness accidentally.

- [ ] Complete the Linux cgroup compatibility decision.
  - Add cgroup v1 CPU, cpuset, memory limit, usage, and headroom detection with
    parser tests, or explicitly reject cgroup v1 environments during production
    qualification.
  - Never fall back silently to host-wide limits when a container limit exists
    but cannot be interpreted.

- [ ] Add scheduled supply-chain and long-running quality gates.
  - Add dependency advisory and license-policy checks.
  - Run the optimizer differential/metamorphic corpus on a schedule and retain
    minimized replay bundles for failures.
  - Run larger-than-memory mixed foreground/background soak profiles and retain
    latency, RSS, page-fault, spill, and checkpoint artifacts.

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
