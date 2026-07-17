# Storage Design

## Current V1 Slice

The current storage implementation is a small durable graph store slice. It is
not an LSM tree and it does not depend on RocksDB or another storage engine.

Files:

- `checkpoint.skein`: full durable snapshot of catalog tokens, nodes, and
  relationships, written through the default zstd compression envelope.
- `manifest.skein`: checksummed checkpoint publication metadata with checkpoint
  epoch, checkpoint commit epoch, WAL replay start LSN, and next WAL LSN.
- `projected_graphs.skein`: checksummed, checkpoint-generated CSR/CSC
  projection artifacts derived from persisted projected graph definitions,
  written through the default zstd compression envelope.
- `stable_ids.skein`: checksummed persisted stable-ID mapping for records that
  do not carry an `id` property at the physical export boundary, written
  through the default zstd compression envelope.
- `wal.skein`: append-only committed mutation log.

Recovery:

1. Load `manifest.skein` when present and verify its checksum.
2. Reject unsupported manifest storage versions before using manifest state.
3. Load `checkpoint.skein` when present.
4. Verify the checkpoint checksum.
5. Reject unsupported checkpoint storage versions before importing records.
6. Replay valid WAL entries in order. When configured, the WAL replay entry
   limit is checked after a record is decoded and before applying it.
7. In the default recovery mode, stop replay at a torn tail or checksum
   mismatch. In strict recovery mode, reject the open instead.
8. Rebuild in-memory adjacency indexes from relationship records.
9. Validate that every recovered relationship references existing source and
   target nodes before accepting the graph state.
10. Verify projected graph artifacts when present. Corrupt artifacts are
   discarded because they are rebuildable derived state, not canonical graph
   state.

Checkpoint, manifest, and projected graph artifact publication write a
temporary file, sync the file contents, atomically rename it into place, and
sync the parent directory. Checkpoint and projected graph artifact payloads use
zstd by default inside a checksummed binary envelope while preserving legacy
plain-text read compatibility. Manifest and WAL files remain plain text so boot
metadata and append-only mutation records stay inspectable and avoid compression
work on every mutation. This keeps publication durable while avoiding
per-mutation directory syncs, manifest writes, or WAL compression write
amplification.
Stable-ID mapping publication uses the same synced temp-file rename and parent
directory sync boundary as checkpointed artifacts. It is intentionally outside
the graph WAL: first physical export may create mapping entries, but that action
does not mutate graph records or increase WAL replay work.
Search projection snapshots use the same temporary-file, file sync, atomic
rename, and parent-directory sync boundary when `SearchIndex::checkpoint`
publishes `search_projection.skein`; the snapshot uses the same zstd envelope
by default. The projection remains rebuildable and is not part of canonical
graph WAL recovery.

WAL entries can represent either a single mutation or a batch commit record.
The relationship pattern create path uses a single batch record for source node,
target node, and relationship creation. Recovery only applies a batch after its
whole record passes checksum validation, so a torn tail cannot leave behind a
half-created path.
`max_wal_replay_entries` counts these top-level WAL records, not the child
operations inside a batch, so a budgeted recovery either applies a complete
batch record or rejects the open before applying the next record.

`DatabaseTransaction` buffers mutation statements and commits them as one WAL
batch. Rollback drops the buffered mutations without touching the store.
`DatabaseReadTransaction` owns an immutable catalog and graph snapshot for
read-only Cypher execution. It rejects mutation statements, does not observe
later commits, and remains usable after the writer checkpoints. Active read
transactions register their snapshot commit epoch in a process-local reader pin
registry and unregister on drop. This is an API snapshot slice. The store also
tracks a commit epoch and publishes a checksummed manifest after each successful
checkpoint. The manifest records the checkpoint epoch, the checkpoint-covered
commit epoch, the oldest active reader commit epoch, the safe reclamation commit
epoch, the WAL replay start LSN, and the next WAL LSN. This does not yet provide
page-level MVCC visibility, physical page/segment reclamation, or concurrent
writer coordination. `Database::storage_reclamation_watermark` exposes the same
boundary in structured form for future page/segment garbage collection: current
commit epoch, optional checkpoint epoch and checkpoint commit epoch, active
oldest reader epoch, computed safe reclaim commit epoch, and whether the store
is durable.
`Database::export_canonical_graph_snapshot` and the same method on
`DatabaseReadTransaction` expose the current or pinned graph snapshot as
canonical node and relationship records with a deterministic logical checksum.
The export also carries a stable-identity audit: records with an `id` property
expose that value as their stable ID, while records without one or with duplicate
stable IDs are reported as requiring an external persisted ID mapping before
physical import or delta replay. `CanonicalGraphSnapshotExport::validate`
recomputes the logical checksum and stable-identity audit, checks node and
relationship ID uniqueness, and reports missing relationship endpoints before an
export is handed to an importer, shadow gate, or storage-equivalence oracle.
`CanonicalStableIdMapping` can overlay a caller-persisted mapping for records
that do not carry an `id` property. Applying the mapping recomputes the
stable-identity audit and logical checksum, so `validate().is_import_ready`
remains the gate before first physical import, resumed export, reimport, or
delta comparison.
`Database::export_canonical_graph_snapshot_with_persisted_stable_ids` is the
local physical-export entry point for this path. It generates missing stable IDs
once, writes them to `stable_ids.skein`, and reuses the same mapping after
reopen. The default `export_canonical_graph_snapshot` remains read-only and does
not create persistent export metadata.
`Database::prepare_graph_lightning_bootstrap_export` wraps the same persisted
stable-ID snapshot in a Graph Lightning bootstrap manifest. The manifest records
protocol version, graph commit epoch, logical checksum, GraphStream checksum and
byte length, schema checksum, node/relationship counts, label/type counts,
property counts, and the canonical snapshot validation result. It is the local
v1 gate before a GraphStream encoder or row-staging adapter consumes the
snapshot. The paired GraphStream is deterministic canonical text sorted by
labels, relationship type, stable IDs, and endpoints; it does not copy local
pages, WAL entries, checkpoint bytes, or adjacency pointers.
The CLI command `skein validate-canonical-snapshot [--require-valid]
[--require-import-ready] <database-path>` opens the database read-only, exports
the current canonical snapshot, and prints the validation report as JSON.
`--require-valid` returns a non-zero status when the snapshot is internally
inconsistent. `--require-import-ready` additionally requires every node and
relationship to have unique stable identity, so the export can enter a physical
import path without first creating an external ID mapping.
The CLI command `skein graph-lightning-bootstrap-manifest [--require-ready]
<database-path>` opens the database read-write, creates or reuses
`stable_ids.skein`, and prints the bootstrap manifest as JSON. `--require-ready`
returns a non-zero status if the manifest's embedded validation is not
import-ready.
The CLI command `skein graph-lightning-graph-stream [--require-ready]
<database-path>` uses the same bootstrap export path and prints the deterministic
GraphStream text. The final `checksum` line covers the stream body and matches
the manifest's `graph_stream_checksum`.
`GraphLightningGraphStream::validate_against_manifest` and the CLI command
`skein graph-lightning-verify-export [--require-valid] <database-path>` verify
the local bootstrap artifacts before upload. The report covers GraphStream
format version, body checksum, manifest checksum and byte-length agreement,
declared count agreement, duplicate node/relationship IDs, and relationship
endpoint integrity.
`skein graph-lightning-bootstrap-bundle [--require-ready] <database-path>`
prints one machine-readable bootstrap evidence bundle containing the manifest,
the GraphStream validation report, and a ready/blocked export gate decision. Use
this as the CI or upload preflight entry point when the caller needs one JSON
artifact instead of separate manifest and verifier commands. The export gate
keeps a flattened `blockers` list for logs and also reports manifest and
GraphStream blocker counts plus grouped blocker messages so import automation
can distinguish snapshot readiness failures from stream artifact failures
without parsing strings.
`skein graph-lightning-stage-bootstrap [--require-ready] <database-path>
<staging-dir>` writes a local staging catalog plus manifest, GraphStream, and
bootstrap bundle artifacts with atomic file publication and directory sync. The
catalog is the v1 local checkpoint boundary for offline bootstrap upload/resume;
it is outside the graph WAL and does not alter the published graph snapshot. The
catalog also summarizes staged object count, measured byte count, total bytes,
average object size, and per-kind object counts for upload observability.
`skein graph-lightning-verify-staging [--require-ready] <staging-dir>` reopens
that staging catalog without the source database, verifies artifact byte
lengths and checksums, recomputes GraphStream validation, and checks agreement
between the catalog, manifest, bundle, and GraphStream artifact. Its validation
gate keeps flat errors for logs and grouped artifact, manifest, GraphStream,
bundle, and catalog error arrays for local upload/resume automation. Its
artifact summary reports the same count and byte metrics from the actually
measured artifacts.
`skein graph-lightning-publish-staging [--require-state-marker]
[--fencing-token <token>] [--expected-graph-epoch <epoch>] <staging-dir>
<publish-dir>` verifies a READY staging catalog and atomically writes
`graph_lightning_published_manifest.json`. Repeating the command for the same
manifest is idempotent; attempting to publish a different manifest over an
existing pointer fails instead of overwriting the published graph pointer. When
the optional state-marker/fencing preflight is enabled, publish requires a
VALIDATING import marker, matching fencing token, and matching staged manifest
graph epoch before writing the pointer.
`skein graph-lightning-verify-published <staging-dir> <publish-dir>` verifies
that the published pointer still references the staged catalog by byte length
and checksum, and that the referenced staging catalog still passes the
source-independent verifier. Its validation gate keeps flat errors and grouped
pointer, catalog, and staging error arrays so resume automation can distinguish
pointer corruption from staging catalog drift.
`skein graph-lightning-gc-staging-report <staging-dir> <publish-dir>` fails
closed when a published pointer cannot be verified and groups the propagated
published-pointer verification errors for cleanup automation. The GC report also
summarizes total, pinned, and deletable staging bytes so callers can distinguish
published retention from orphan staging space.
`skein graph-lightning-import-status <staging-dir> <publish-dir>` summarizes
CREATED/READY/PUBLISHED/QUARANTINED state and can merge an optional
caller-owned `graph_lightning_import_state.json` marker for
EXPORTING/UPLOADING/MERGING/VALIDATING/FAILED/CANCELED coordinator states. It
groups presence, staging, published-pointer, state-marker, and resource errors
for resume automation. Active coordinator states require the marker to carry the
idempotent retry tuple `import_id`, `task_id`, `fencing_token`, and
`object_digest`; missing fields quarantine the status report before retry. The
report can also summarize an optional caller-owned
`graph_lightning_import_checkpoints.jsonl` append log. Checkpoint entries keep
resume/failure coordinates such as source range, object digest, partition, and
validation rule; object-level checkpoints must include the same idempotent retry
tuple before status automation treats them as resumable. Reusing one complete
checkpoint idempotency tuple for conflicting source-range, partition, or
manifest-digest coordinates blocks the status report instead of allowing resume
automation to amplify a stale retry marker. Checkpoint status also exposes
machine-readable stage, status, failure-rule, and failure-partition counts so
resume monitors can classify progress and failure hot spots without reparsing
the append log. The report also
includes machine-readable `resume_action`, `state_marker`, `checkpoint_log`, and
`resource_retention` fields that distinguish staging, publishing, active work,
completed, canceled, failed, and quarantined/manual-repair states without
requiring callers to parse human-readable error strings.
The storage-equivalence regression coverage compares canonical exports from the
same graph after live mutation, WAL replay, checkpoint publication, and
checkpoint recovery, and requires byte-for-byte equal export structures plus a
valid self-validation report.
This is the local export boundary for future GraphStream encoding; it does not
copy local pages, WAL records, adjacency pointers, or rebuildable projection
artifacts.

`Database::storage_version` exposes the currently supported storage version.
`GraphStore::open` also validates the stored version in both the manifest and
checkpoint images at boot. Unsupported versions fail with an explicit storage
compatibility error instead of falling through to a generic parse error or a
checksum-corruption path.

The implementation currently persists:

- node labels
- relationship types
- node records
- relationship records
- relationship properties
- node and relationship table descriptors with durable schema state
- property schema descriptors for node and relationship tables
- unique node-property constraint descriptors
- composite equality index descriptors
- projected graph definitions
- checkpoint-generated projected graph CSR/CSC artifacts
- outgoing adjacency index
- incoming adjacency index

The implementation also maintains rebuildable in-memory property indexes. The
single-property index is keyed by `(label_id, property, value)`, the composite
equality index is keyed by `(label_id, [(property, value), ...])`, and the
full-text candidate index is keyed by `(label_id, property, ngram)`. The
catalog stores persistent equality, composite equality, range, and full-text
index descriptors, while the execution indexes remain rebuildable from
canonical records after checkpoint load or WAL replay. The optimizer can choose
`IndexNodeSeek` for simple label plus property equality predicates when an
equality index descriptor exists, `IndexNodeCompositeSeek` for conjunctions
that bind every property in a composite equality descriptor, `IndexNodeTextSeek`
for `CONTAINS` predicates backed by a full-text descriptor, and
`IndexNodeRangeSeek` for single-bound and conjunctive bounded range predicates
when a range index descriptor exists. Text seeks use the ngram index only as a
candidate source and retain a residual `FilterExec` so exact string containment
semantics remain authoritative. Composite and full-text execution projections
can be rebuilt through `Database::rebuild_bounded_property_index_projections`,
which applies complete descriptor rebuilds that fit a caller-supplied operation
budget and reports estimated operations plus indexed entry counts without
writing WAL. Internal background loops can expose the same work as a
`Projection` `BackgroundWorkPlan` through
`Database::property_index_projection_background_work_plan` or execute it through
bounded background/scheduled wrappers that bind QoS admission to the same
descriptor rebuild budget. Conjunctive range seeks keep the complete `AND`
predicate as a residual filter while using merged lower and upper bounds as the
access path. Statistics now include per-label/property and
per-relationship-type/property distinct counts, one-hop path source/target
coverage distinct counts, bounded multi-hop path source/target coverage
distinct counts, bounded sorted value histograms, and exact-versus-sampled
markers. Histograms use deterministic adaptive samples:
small distinct sets remain exact, medium sets keep up to 256 values, and large
sets keep up to 512 values while always retaining the minimum and maximum
sampled bounds. Range costing uses these histograms for selectivity estimates.
Statistics are rebuildable derived data and are written to checkpoints for
observability and future costing.

The catalog also stores persistent property constraint descriptors.
Node unique constraints use
`CREATE CONSTRAINT ON :Label(property) ASSERT UNIQUE`. Relationship unique
constraints use `CREATE CONSTRAINT ON -[:TYPE(property)]-> ASSERT UNIQUE`.
Node existence constraints use
`CREATE CONSTRAINT ON :Label(property) ASSERT EXISTS` or the equivalent
`ASSERT NOT NULL`. Relationship existence constraints use
`CREATE CONSTRAINT ON -[:TYPE(property)]-> ASSERT EXISTS` or the equivalent
`ASSERT NOT NULL`. Constraint creation scans existing canonical records and
fails if duplicate non-null property values already exist for the constrained
node label or relationship type, or if any constrained node or relationship is
missing the required property or stores `NULL`. Subsequent node creation,
relationship pattern creation, merge-created records, and `MATCH ... SET`
updates are validated against the active descriptors before appending the WAL
batch.

Node and relationship table descriptors are persistent catalog metadata created
by `CREATE NODE TABLE Name` and `CREATE RELATIONSHIP TABLE Name`. Descriptor
state transitions are supported through `ALTER NODE TABLE Name SET STATE State`
and `ALTER RELATIONSHIP TABLE Name SET STATE State`, where `State` is
`DELETE_ONLY`, `WRITE_ONLY`, `BACKFILL`, `VALIDATING`, `PUBLIC`, or `GC`. Table
creation also ensures the matching label or relationship type token exists.

Property schema descriptors are persistent catalog metadata created by
`CREATE PROPERTY ON NODE TABLE Name(property) TYPE Type` and
`CREATE PROPERTY ON RELATIONSHIP TABLE Name(property) TYPE Type`, with optional
`NOT NULL`. Supported types are `ANY`, `BOOL`, `INT`, `FLOAT`, `STRING`, and
`LIST`.
Descriptor creation validates existing records for that table before appending
the WAL batch. Later node creation, relationship pattern creation, merge-created
records, and `MATCH ... SET` updates are validated against the active property
schema descriptors before any WAL append. Property descriptor state transitions
are supported through
`ALTER PROPERTY ON NODE TABLE Name(property) SET STATE State` and
`ALTER PROPERTY ON RELATIONSHIP TABLE Name(property) SET STATE State`. Only
`PUBLIC` table and property descriptors participate in write-time and recovery
validation. Promoting a property descriptor to `PUBLIC` validates existing
records before appending the WAL batch.

Schema maintenance is explicit. `Database::run_schema_maintenance` scans table
and property descriptors, validates the next state before appending WAL, and
then writes all selected maintenance operations as one grouped WAL batch.
`BACKFILL` descriptors advance to `VALIDATING`, `VALIDATING` descriptors advance
to `PUBLIC` only after validation, and `GC` descriptors are tombstoned from the
catalog. `Database::plan_schema_maintenance` is a read-only dry-run report for
pending descriptor maintenance, including the source state, target state,
action, and estimated operation count for budget admission.
`Database::run_bounded_schema_maintenance` applies only complete descriptor
maintenance actions that fit a caller-supplied estimated-operation budget, using
one WAL batch for the selected actions and leaving skipped descriptors in their
current state for a later call. This is descriptor-level batching, not yet
inside-object backfill checkpointing. Explicit callers use the direct execution
path without local background admission. Internal
maintenance loops can use the planned wrappers,
`Database::run_planned_background_schema_maintenance` and
`Database::run_planned_scheduled_background_schema_maintenance`, to charge the
current dry-run estimate to the `Mutation` background lane before any descriptor
advancement or maintenance WAL append. The lower-level background wrappers still
accept caller-supplied estimates when the caller already has a stronger external
budget model. For per-tick low-resource loops,
`Database::run_bounded_background_schema_maintenance` and
`Database::run_bounded_scheduled_background_schema_maintenance` bind the same
operation budget to both QoS admission and descriptor-level bounded execution.
WAL replay applies descriptor GC before the database is exposed, while record
pages and index artifacts remain separately rebuildable or reclaimable.

Checkpoint files include a statistics snapshot for observability and future
costing: total node count, total relationship count, per-label counts,
per-relationship-type counts, relationship-type source counts,
label/type/label path cardinalities, bounded exact path cardinalities up to the
current statistics hop limit, per-label/property distinct-value counts, and
per-relationship-type/property distinct-value counts. The statistics snapshot
also records the commit epoch at which it was computed, the histogram sample
limit, and whether each node or relationship property histogram is an exact
value set or a bounded deterministic sample. These statistics are derived data;
the store recomputes the live API view from canonical records and accepts old
checkpoints that do not contain statistics lines.

Checkpoint also writes `projected_graphs.skein` for every persisted projected
graph definition. The artifact records its format version, projection epoch,
covered commit epoch, node IDs, CSR outgoing offsets and targets, and CSC
incoming offsets and sources. It is checksum-protected and atomically replaced.
Recovery parses valid artifacts into an in-memory cache. Graph algorithm
execution reuses a cached artifact only when the artifact commit epoch equals
the store commit epoch and the stored definition still matches the active
projected graph definition; otherwise it rebuilds from canonical records.
Recovery also filters the in-memory artifact cache with the same commit-epoch
and definition checks, so stale artifacts left by a later WAL replay or changed
projection definition are not exposed through projected graph status metadata.
`Database::rebuild_projected_graph_artifacts` can refresh these derived
artifacts independently of checkpoint publication. `Database::rebuild_derived_artifacts`
wraps the same projected graph refresh in a report-oriented orchestration API
that returns artifact type, name, reusable-state transition, projection epoch,
commit epoch, and graph cardinalities. Neither path appends WAL, truncates WAL,
or publishes a new checkpoint manifest; they only advance the projection epoch
and atomically replace `projected_graphs.skein`.
`Database::schedule_derived_artifact_rebuild` and
`Database::run_next_derived_artifact_job` add a small embedded job state machine
for these projected graph artifacts. Jobs expose pending/running/succeeded/failed
state, attempts, and last error without adding threads or hiding rebuild
failures.
Internal background callers can route the same pending jobs through
`Database::run_next_background_derived_artifact_job` for stateless admission or
`Database::run_next_scheduled_background_derived_artifact_job` for the
caller-driven `LocalQosScheduler` path. The scheduler only tracks running
background operation budgets between start and finish, including optional
per-class budgets for projection, import, analytics, and shadow lanes; it does
not own worker threads, reorder jobs, or gate foreground explicit rebuild
requests.
Callers that maintain their own background loop can build a
`BackgroundWorkPlan` with `BackgroundWorkHint` signals for active topic,
recent delta size, query probability, staleness TTL, freshness SLO, and tenant
budget. Schema maintenance can join the same caller-owned candidate list
through `Database::schema_maintenance_background_work_plan`, which returns a
`Mutation` background work plan using the current dry-run estimate when
descriptor maintenance is pending.
`LocalQosPolicy::evaluate_background_work` returns the existing admission result
plus a deterministic expected-value score and reasons, so the caller can rank or
skip internal background work without moving queue ownership into Skein. If a
candidate carries a tenant budget hint below its estimated operations, the
ranked decision is deferred even when the base background policy would admit it.
`LocalQosPolicy::rank_background_work` and the matching
`LocalQosScheduler` method apply the same evaluation to a caller-owned candidate
list, sort admitted work before deferred/rejected work, then sort by score and
original index for deterministic polling loops.
`Database::schedule_external_content_artifact_job` records content/blob parser
work at the same orchestration boundary. Callers that need structured parser
inputs can use `Database::schedule_external_content_artifact_job_with_payload`
to attach object references, checksums, content type, target projection, or
other application-owned metadata as a `Value::Map`.
`Database::pending_external_content_artifact_jobs` returns a bounded pending
view for caller-owned runtimes that poll parser work without scanning the whole
embedded job history. `Database::failed_external_content_artifact_jobs` returns
the matching bounded failed view for recovery queues and operator-facing parser
diagnostics. `Database::external_content_artifact_job_summary` exposes aggregate
pending, running, succeeded, and failed counts plus the next pending and oldest
failed job ids. It also groups pending and failed counts by action so
action-specific parser, crawler, or embedding runtimes can decide whether to
poll their queue before fetching bounded job details. The graph-kernel job
runner still rejects those jobs with a
graph-kernel-external error and preserves the payload in the failed job report,
while
`Database::run_next_external_content_artifact_job_with` and
`Database::run_external_content_artifact_job_with` let the caller supply the
content artifact runtime and complete either the next pending job or a specific
pending job selected from a bounded poll result. Skein records the state
transition without embedding parsing, crawling, chunking, or large-value runtime
logic. That runtime reads the payload and publishes rebuildable projections back
to Skein through caller-owned output. Successful external content jobs keep the
last structured output rows on the job ledger for lightweight lineage and
operator audit; large parser results, raw bytes, chunks, and projection payloads
remain caller-owned artifacts outside the graph kernel. Bounded succeeded-job
views expose those retained output rows globally or per action without requiring
the external runtime to scan all derived-artifact jobs.
Runtimes that want a standard lightweight lineage shape can use
`ExternalContentArtifactJobCompletion` with
`Database::complete_next_external_content_artifact_job_with` or
`Database::complete_external_content_artifact_job_with`. The completion row
records runtime identity, input/output refs, checksums, projection refs, source
graph epoch, produced-row counts, and small metadata. It deliberately stores
only references and audit metadata, not parser result payloads. The matching
background and scheduled completion runners apply the same standard row shape
while charging parser/crawler work to the `Import` QoS lane.
`ExternalContentArtifactRuntimeManifest` lets a caller-owned runtime declare the
actions it can handle, required payload keys, version, and estimated operation
cost. Skein uses that manifest only to return bounded claimable-job views and an
Import-lane background work plan; it is not a sandbox policy and does not grant
the runtime access to graph-kernel execution.
Internal parser/crawler loops can use
`Database::run_next_background_external_content_artifact_job_with` for stateless
`LocalQosPolicy` admission or
`Database::run_next_scheduled_background_external_content_artifact_job_with` for
`LocalQosScheduler` accounting. These paths charge external content work to the
`Import` class and keep direct caller-owned runtime APIs available for explicit
foreground work. Runtimes that choose a concrete pending job from the bounded
poll result can use `Database::run_background_external_content_artifact_job_with`
or `Database::run_scheduled_background_external_content_artifact_job_with` for
the same Import-lane accounting without falling back to a global next-job claim.
Action-specific runtimes can use
`Database::run_next_scheduled_background_external_content_artifact_job_for_action_with`
for the same accounting without claiming unrelated pending work.
Runtimes that only support a subset of content actions can use
`Database::pending_external_content_artifact_jobs_for_action` and
`Database::run_next_external_content_artifact_job_for_action_with` to poll and
claim only matching actions, such as `parse` or `crawl`, without inspecting or
failing unrelated pending work.
`Database::retry_failed_external_content_artifact_job` explicitly resets failed
external content jobs to pending, preserving the payload and attempt history
while clearing the last error. It does not retry graph-kernel projected artifact
jobs, keeping content parser recovery separate from database-owned artifact
rebuilds.

Search projection rebuild has the same orchestration shape at the search layer:
`SearchIndex::rebuild_derived_artifacts` reports the search projection artifact
type, document counts before and after rebuild, scanned graph nodes, indexed
documents, and lifecycle-marker state. It still uses the bounded all-or-nothing
graph-to-search rebuild path, so a row-limit failure keeps the previous search
projection intact.
Incremental search projection deltas can also run through
`SearchIndex::apply_scheduled_background_projection_delta` or the matching
`Database::apply_scheduled_background_search_projection_delta` facade, which
uses `LocalQosScheduler` to account for in-flight internal background
projection work while keeping the direct delta API available for explicit
foreground callers.
The metadata-only repair path follows the same boundary:
`SearchIndex::repair_background_metadata_from_graph` uses stateless
`LocalQosPolicy` admission, and
`SearchIndex::repair_scheduled_background_metadata_from_graph` uses
`LocalQosScheduler` for in-flight background budget tracking without rewriting
document content or embeddings.

## Durability Policy

The default durability policy is `SyncOnCheckpoint`.

Under this policy, each WAL append is flushed to the operating system, but it is
not individually `fsync`ed. Checkpoint creation writes and syncs a temporary
snapshot file, atomically renames it into place, and truncates the WAL.

This default avoids per-mutation fsync write amplification. Workloads that need
stronger single-write durability can opt into `SyncOnEveryWrite`, which calls
`sync_data` after each WAL entry.

The explicit policy is important because graph ingestion often creates many
small node and relationship records. Syncing each tiny WAL append can dominate
runtime and cause pathological write amplification.

## Relationship Locality

The current in-memory adjacency key is:

```text
(node_id, relationship_type_id) -> relationship ids
```

There are two indexes:

- outgoing: `(source, type) -> rel_ids`
- incoming: `(target, type) -> rel_ids`

This is only the first step toward native graph locality. The next storage
layout should split sparse and dense adjacency:

- sparse nodes keep a compact inline/list adjacency representation
- dense nodes use copy-on-write adjacency segments or a B+ tree-like structure
- dense adjacency is ordered by `(edge_type, direction, neighbor_id, edge_id)`
- hub nodes get isolated storage so they do not pollute ordinary traversal
  locality

## What This Is Not

This is not a production page store yet:

- no MVCC snapshots
- no page cache
- no delayed garbage collection or reclamation policy
- no property spill blocks
- no cost model that consumes persistent statistics for join ordering
- no columnar property segments
- no database-owned blob/content parser runtime

It is a correctness-first recovery slice that keeps the public direction
aligned with the intended Adaptive Native Graph Store.

## Next Storage Tasks

1. Add page-level MVCC reader isolation.
2. Add physical page/segment reclamation using pinned manifest epochs.
3. Add physical sparse adjacency blocks before dense adjacency segments. The
   storage API already exposes ordered adjacency entries and sparse/dense group
   classification over the current in-memory adjacency indexes.
4. Add property spill blocks for large values.
5. Add richer index statistics beyond bounded path source/target coverage and
   text analyzer parity.
6. Add richer caller-owned blob/content parser integration at the boundary
   outside the graph kernel.
