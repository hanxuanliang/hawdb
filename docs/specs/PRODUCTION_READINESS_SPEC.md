# Skein Production Readiness Specification

## Scope

This specification defines the evidence required before a Skein build may
serve production traffic or replace an existing Nowledge graph or search read
owner. It complements the runtime correctness contract in
`EMBEDDED_RUNTIME_SPEC.md`; passing unit tests or implementing an API does not
by itself satisfy this specification.

Skein is an embedded Rust library. Production traffic, readiness collection,
and cutover decisions MUST use typed Rust APIs. Command-line programs MAY
render or transport the same reports for development and CI, but they MUST NOT
be required by the production serving path.

## Readiness States

Readiness is divided into three non-interchangeable states:

1. **development ready**: the capability is implemented and its deterministic
   unit, integration, and model tests pass;
2. **traffic ready**: the exact production build and configuration pass
   recovery, resource, enabled-feature, and platform qualification on a
   representative replica;
3. **cutover ready**: every active route is traffic ready, offline shadow
   evidence demonstrates semantic equivalence, and rollback controls remain
   available.

A higher state MUST imply every lower state. Missing, stale, malformed, or
unbound evidence MUST produce `ready=false`; a caller-provided `ready=true`
field is never authoritative without recomputation from raw evidence.

Graph traffic readiness and search traffic readiness are independent. A host
MAY activate one while the other remains on the previous owner. Full cutover
readiness requires both.

## Evidence Identity

Every production qualification bundle MUST bind at least:

- Skein source revision and Rust toolchain;
- target OS, architecture, and enabled Cargo features;
- durable format and schema versions;
- database configuration digest and deployment profile;
- canonical graph commit epoch and dataset fingerprint;
- search projection generation, analyzer identity, embedding identity, and
  source graph epoch when search is qualified;
- evidence generation time and the policy that evaluates the evidence.

Evidence from another source revision, target, feature set, dataset, schema,
or projection generation MUST NOT qualify the current runtime. Regenerating a
canonical or search projection generation invalidates generation-bound
evidence.

Authorization is feature-bound. The initial Mem release MAY omit the `acl`
feature and MUST record that exact feature set in its qualification identity.
When `acl` is enabled, authorization policy freshness, pre-materialization
enforcement, filtered search parity, and cache isolation become mandatory
traffic-readiness evidence. Evidence from a build without `acl` MUST NOT
qualify a build that enables it.

`ProductionQualificationIdentity` is the typed release identity and
`ProductionEvidenceBinding` adds the generation time. The identity comparison
is exact, including canonical graph epoch and canonicalized Cargo feature set.
Storage production qualification MUST use
`Database::storage_resource_profile_for_production`; the development
`storage_resource_profile` entrypoint may measure resources but MUST serialize
as production-unready because it has no release binding.

Default reports MUST redact local paths, query text, parameters, row payloads,
embeddings, credentials, and raw parser or I/O payload fragments. Debug-only
diagnostics MAY expose local detail through an explicit host decision, but
debug reports MUST NOT be accepted as production cutover evidence.

## Cross-Platform Qualification

Linux, macOS, and Windows are production targets. Required CI checks for each
supported target MUST be green for the exact revision being released.

Resource reports MUST expose a typed metric capability set. A platform MUST
NOT relabel an aggregate counter as a semantically different split counter. In
particular, a Windows total page-fault count MUST NOT be reported as Unix minor
or major faults. A qualification policy MUST require all metrics declared
available for its target and MUST fail closed when a metric required by that
policy is unavailable.

Process resource sampling SHOULD provide:

- steady and peak resident memory;
- total page faults on every target that exposes them;
- minor and major page faults only on targets that expose that distinction;
- intermediate and output row counts;
- intermediate and output payload bytes;
- segment-cache residency, misses, evictions, and admission rejections.

`skein-storage-resource-profile-v2` exposes `resource_ready` separately from
production `ready`. Production readiness additionally requires an exact
evidence/expected-identity match. `metric_capabilities.total_page_faults`
applies on Unix and Windows; `metric_capabilities.split_page_faults` is false
on Windows, where the split fields MUST remain absent.

The Windows storage-platform CI job MUST retain a
`storage-resource-windows-latest-<revision>` artifact containing the bound v2
report and a runner manifest. The report MUST be production-ready for the
platform fixture, identify `target_os` as `windows`, expose resident-memory and
total-page-fault capability, and leave Unix split page-fault fields absent.

Linux runtime sizing MUST derive effective CPU and memory from the smallest
known host and cgroup limits. Cgroup v2 `cpu.max`, effective/inherited cpuset,
`memory.max`, `memory.high`, `memory.current`, and memory headroom detection are
supported. The unified path MUST be resolved from process membership and the
cgroup2 mount root. A detected controller with an unreadable or invalid limit
fails closed; it MUST NOT silently use host-wide limits inside a container.
Cgroup v1 resource controllers are unsupported and fail closed to one CPU and
zero memory admission; a v1-only or resource-controller hybrid host cannot
qualify for production readiness.

## Resource Qualification

Production resource evidence MUST be collected from a representative copy of
the intended workload. Synthetic ignored tests are useful development gates
but do not qualify a production deployment.

The canonical artifact MUST exceed the configured segment-cache budget. Search
qualification MUST also exercise a document corpus larger than the admitted
search memory budget. The report MUST record steady RSS, peak RSS, page faults,
intermediate rows, payload bytes, cache residency, spill bytes, spill runs, and
operator-specific peak tracked memory.

Every query result path MUST have explicit row and payload limits. Every
blocking operator MUST do one of the following before exceeding its admitted
memory:

- spill through a byte- and run-bounded external algorithm;
- reject the operation with a stable resource error; or
- prove through route-bound admission evidence that the active production
  shape cannot exceed the limit.

`SortExec`, `TopNExec`, and grouped `AggregateExec` use byte- and run-bounded
ordered spill. `DistinctExec` uses ordered spill runs followed by bounded merge
deduplication. `NodeCartesianProductExec` partitions an oversized build side
into bounded spill runs and replays those runs for each streamed probe row.
`GraphAlgorithm` admits the direction-specific projection together with a
conservative algorithm scratch and result estimate before PageRank or Louvain
allocates that state. It fails with a stable resource error rather than spilling,
checks cancellation within node and edge loops, and reports its combined
projection, scratch, and materialized-result peak as blocking-operator memory.
All of these paths MUST report tracked peak memory, input rows, spill bytes,
spill runs, and spilled rows, and MUST remove query-scoped runs on success,
error, cancellation, and consumer stop.

Spill admission has two levels. Each blocking operator retains its cumulative
byte and run limits, while all queries whose `ExecutionMemoryConfig` resolves
to the same spill directory share one process-wide live-byte and live-run
pool. A record MUST reserve both levels before it enters the writer buffer.
The pool MUST account for unflushed reservations when preserving
`min_spill_free_bytes`, release live capacity only after its run is removed,
and fail closed when filesystem capacity cannot be inspected. If multiple
configurations use one directory, the process retains the strictest limits it
has observed for that directory. `ExecutionMemoryConfig::spill_pool_snapshot`
exposes active and peak bytes and runs, pending writer bytes, orphan cleanup,
and deletion failures for readiness and monitoring.

Spill filenames are owned by a versioned Skein namespace. On first use of a
spill directory in a process, Skein MUST remove only namespace-matching files
from earlier process identities whose age reaches
`spill_orphan_grace_period`; unrelated files and current-process runs MUST
remain untouched. The default grace period is 24 hours. A failed live-run
deletion remains charged to the shared pool and observable rather than making
unreclaimed disk capacity available to new queries.

Production query entrypoints MUST pass through the runtime governor or an
equivalent host-owned admission boundary. Raw database access MAY remain a
low-level library capability, but its use MUST be reported as non-production
safe unless the host supplies equivalent global admission.

`skein-embedded-query-path-readiness-v1` reports this boundary without
claiming traffic readiness. `SkeinEmbedded::query_admitted`, its parameterized
and task-context variants, and `SkeinTokioEmbedded::query` are
`admission_safe=true`. `SkeinEmbedded::database`, `database_mut`, and
`into_database` remain controlled-host and test surfaces; their report is
`admission_safe=false` with `host_equivalent_governor_not_proven`. An
admission-safe path is only one input to production qualification and MUST NOT
replace revision-, dataset-, and workload-bound evidence.

The Nowledge production facade follows the same boundary. Its parameterized
query and bounded streaming-read entrypoints on `NowledgeMemGraph`,
`NowledgeMemEmbeddedStore`, and `NowledgeMemEmbeddedStoreHandle` MUST acquire a
`RuntimeGovernor` permit before execution. Streaming reads MUST admit the
minimum of the governor result budget, database result limit, and route payload
limit, and MUST retain the permit until the consumer returns. Task-context
variants MUST propagate cancellation and record it in the governor snapshot.
Diagnostic preflight and multi-statement typed transaction helpers are not
traffic-admission evidence until the host binds equivalent admission. The facade's
`database`, `database_mut`, and `into_database` accessors remain explicitly
controlled-host and test surfaces; calling them is not production admission
evidence. Hosts MAY inject one shared governor into the Nowledge facade so all
store handles participate in the same process-level CPU, memory, result, and
I/O limits.

An asynchronous facade that returns a materialized result remains subject to
the result budget. A streaming asynchronous API MUST propagate consumer
backpressure and cancellation without retaining the complete result.
`SkeinTokioEmbedded::query_stream` and its parameterized/options variants use
the admitted query request, add the bounded channel residency to admitted
memory, and deliver execution-memory-sized batches through a finite channel.
The producer retains its runtime permit until the terminal report, observes a
child cancellation token linked to the caller deadline/token, and is cancelled
when the consumer is dropped. Mutation statements are rejected by this API and
continue through the serialized materialized mutation path.

## Durability Qualification

Release qualification MUST exercise real process termination in addition to
in-process failpoints. The crash matrix MUST cover at least:

- before WAL append;
- after append but before synchronization;
- after WAL synchronization but before snapshot publication;
- during checkpoint artifact publication;
- after manifest publication but before obsolete-generation reclamation.

Each crash point MUST reopen the database in a fresh process and verify that a
mutation batch is either entirely absent or entirely recovered. The harness
MUST verify commit epoch, replay LSN, endpoint integrity, projection watermark,
and the absence of partially published artifacts. Ordinary startup MUST reject
a torn WAL tail without changing it. Explicit doctor repair MUST preserve
structured prepared and applied repair records, retain a verified copy of the
original WAL, and report discarded bytes before normal serving can resume. The
repair MUST use a separate typed API: a read-only generation-bound dry run,
followed by explicit acknowledgement and state revalidation. A pending repair
record MUST block ordinary open, and an interrupted repair may be finalized
only when the manifest, retained WAL, and quarantine identities still match.

Durable-before-publish and pinned-reader invariants MUST be model checked for
the released storage protocol. Model-check configuration and results MUST be
part of the release CI artifact set, not only documented as a local command.
The revision-bound `tla-model-check-<revision>` artifact MUST contain successful
TLC logs and exact `.tla` and `.cfg` inputs for `SkeinStorageDurability`,
`SkeinGenerationReclamation`, `SkeinConcurrentSnapshots`, and
`SkeinSourceSegmentPublication`, together with the Java version and the pinned
TLA+ Tools version and SHA-256 digest. A downstream CI job MUST download and
verify the complete artifact before the model-check gate succeeds.

The typed crash artifact protocol is
`skein-storage-crash-recovery-evidence-v1`. Every required crash point MUST
appear for the admitted repetition count and every case MUST prove termination,
whole-batch recovery, epoch/LSN agreement, endpoint integrity, projection
watermark integrity, and active artifact-generation integrity. CI MUST retain
this report per target and source revision.

## Search Qualification

The mutable full-residency search index is a maintenance and compatibility
owner. A larger-than-memory production caller MUST use the generation-bound
out-of-core facade and its production constructor.

Search traffic readiness MUST recompute qualification from raw evidence and
require:

- exact TopK and score parity for text, vector, and hybrid modes;
- lifecycle, space, metadata, and source-filter parity before ranking, plus
  ACL parity when the release feature set enables `acl`;
- incremental update, delete, checkpoint, reopen, stale-generation, and
  corruption cases;
- bounded candidate spill, score state, sidecar reads, and late hydration;
- representative P50, P95, and P99 latency, steady and peak RSS, page faults,
  posting bytes, payload bytes, checkpoint time, and update amplification.

Request-time comparison with the previous engine MUST NOT run on the production
serving path. Shadow and differential evidence belong to an offline or
dedicated preflight path.

`skein-search-lexical-production-qualification` version 2 binds the report to
the release identity and to projection generation, source graph epoch,
document digest, analyzer digest, and embedding model/version/dimension. It
records target-appropriate process-memory capabilities, text/vector/hybrid
TopK score parity, sidecar and hydration bytes, and P50/P95/P99 latency. The
out-of-core production constructor MUST recompute these blockers against the
opened projection identity.

## Route And Cutover Qualification

Every active graph and search route MUST declare one selected read owner. Route
readiness MUST be tied to the shared route catalog, query family, bounded query
evidence, and the current production build identity.

Cutover MUST remain blocked when any required area is missing or not ready,
including:

- route ownership or query-family coverage;
- storage recovery and resource qualification;
- authorization policy freshness and pre-materialization enforcement when the
  release feature set enables `acl`;
- search projection generation, freshness, and parity;
- background QoS and foreground admission;
- redaction and production library-path verification;
- initial import, dual-write convergence, or rollback controls while they are
  required by the migration phase.

Production health MUST report liveness for the selected owner and MUST NOT open
or probe the previous engine solely to make the selected owner appear healthy.

## Release Controls

Branch protection is a delivery-governance control, not a kernel traffic
readiness prerequisite. It MAY be deferred while Skein is in rapid iteration.
An unprotected development branch MUST NOT weaken the evidence required for a
production release or allow branch state alone to imply production readiness.

A production release MUST identify one exact revision and require its green
checks. Required release checks MUST include formatting, strict lint, workspace
tests, supported-platform runtime and storage tests, concurrency models, and
build system parity.

Before a general-availability phase, the project SHOULD protect its release
branch, require pull requests and required checks, and prohibit force pushes and
branch deletion. An equivalent audited release branch or immutable release-tag
workflow MAY satisfy this governance requirement without protecting the
rapid-iteration branch earlier.

The release process SHOULD also include dependency advisory and license policy
checks, a scheduled optimizer fuzz corpus, storage crash/soak campaigns, and
artifact retention for production resource evidence. These checks MUST become
required before their corresponding risk is accepted for production.

The scheduled quality workflow MUST run the advisory and license policy,
deterministic optimizer differential/metamorphic campaigns, persistent-format
corruption campaigns, the scalar/SIMD differential corpus under an address
sanitizer, and a mixed foreground/background runtime soak. Fuzz failures MUST
retain their campaign report and minimized replay bundle before the job fails.
The runtime soak MUST
use an out-of-core fixture whose raw bytes exceed the admitted runtime memory
and whose canonical artifact exceeds the segment cache. It MUST exercise the
admitted Tokio facade, high-cardinality distinct and Cartesian paths, observe
bounded external spill, complete a concurrent mutation and checkpoint, and
retain latency, RSS, page-fault, cache, spill, checkpoint, and governor
counters in a revision-bound typed report.

Scheduled synthetic soak evidence MUST identify its controlled fixture setup
path and carry `production_eligible=false`. It verifies regression behavior but
MUST NOT satisfy the representative Mem replica, production-shaped search, or
representative embedding qualification gates.

No release report may claim production readiness while a required check is
red, skipped without an approved qualification artifact, or evaluated for a
different revision.

## Production Boundary

The supported embedded production boundary is one active root handle in one
application process, with concurrent snapshot readers and serialized durable
writes. Multi-process writers, distributed replication, cloud-primary
execution, broad openCypher compatibility, and algorithms outside active
Nowledge routes are not implied by production readiness.

Expanding this boundary requires a new specification and evidence plan before
implementation. It is not a TODO implied by completing the current embedded
cutover.
