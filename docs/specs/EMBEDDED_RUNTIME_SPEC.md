# Skein Embedded Runtime Specification

## Scope

This specification defines the production contract for concurrent access,
durability, recovery, incremental indexes, resource scheduling, and
observability in the embedded Skein library.

Skein is an in-process library. These capabilities MUST be configured and
invoked through Rust APIs. Production correctness MUST NOT depend on a CLI,
helper process, or environment-variable control plane.

## Deployment Profiles

Skein supports two embedded deployment profiles. A profile selects conservative
defaults and capability availability; it does not change the durable storage
format or Cypher semantics.

### Desktop Bound

`DesktopBound` runs inside a desktop application process. It behaves like a
local MySQL or Neo4j data engine from the application's perspective, but its
lifecycle, identity, configuration, and telemetry remain owned by the host
application.

- The host opens one database handle and shares it across application workers.
- Foreground reads may use the effective CPU budget.
- Plan cache, FTS, vector candidate indexes, graph analytics, and bounded
  background maintenance may be enabled.
- Expensive capabilities remain explicitly bounded and may be disabled.
- The library MUST NOT expose a production CLI control plane or start a helper
  database process.

### Mobile Embedded

`MobileEmbedded` runs like SQLite inside a mobile application process. Its
defaults MUST prefer bounded memory, bounded result sets, low background
parallelism, and predictable battery and thermal behavior.

- Canonical graph storage, WAL recovery, checksums, incremental base indexes,
  parameterized Cypher, and transactions remain mandatory.
- ACL, graph analytics, approximate vector indexes, large plan caches,
  continuous compaction, and continuous telemetry export MAY be compiled out or
  disabled.
- Disabling an optional capability MUST return a typed capability-unavailable
  error. It MUST NOT silently use an unbounded fallback.
- Mobile and desktop profiles MUST be able to open the same format version when
  the database does not require a disabled capability.

Explicit configuration overrides MAY further lower resource limits. Raising a
mobile limit is allowed only through an explicit host decision.

### Runtime Capability Gates

Runtime capability checks are independent from resource admission. The default
capability matrix is:

| Capability | DesktopBound | MobileEmbedded |
| --- | --- | --- |
| Full-text search | enabled | enabled |
| Vector search | enabled | enabled |
| Graph analytics | enabled | disabled |
| Background maintenance | enabled | disabled |

The host MAY override this matrix through `SkeinEmbeddedOpenOptions`. Query
capabilities MUST be checked before plan-cache lookup or catalog mutation.
Background capabilities MUST be checked before QoS admission or artifact
mutation. Search capability checks MUST happen before selecting a retriever and
MUST NOT silently substitute a different search mode.

Profile-aware hosts MUST use fallible query and search entry points. A disabled
operation returns `SkeinError::CapabilityUnavailable` with a typed
`RuntimeCapability`; non-fallible search convenience methods are intended only
for hosts that keep the corresponding capability enabled. Capability settings
do not alter the durable format, WAL contract, or core Cypher semantics.

## Concurrency Model

Skein MUST support concurrent readers and a concurrent writer through
multi-version snapshots:

- Readers pin an immutable published snapshot.
- A writer stages changes without mutating a published snapshot.
- Commits are serialized until write-write conflict detection is specified.
- A staged snapshot becomes visible only after its WAL commit is durable.
- Existing readers continue against their pinned snapshot after publication.
- Foreground work MUST remain admissible while internal background work is
  saturated.

The initial contract is multi-reader, single-writer. Multi-writer execution is
out of scope until conflict detection, abort semantics, and index delta ordering
are modeled and tested.

## Runtime Resource Budget

The default concurrency budget MUST be derived from the smallest known limit:

1. `std::thread::available_parallelism`;
2. cgroup v2 `cpu.max` quota when present;
3. the effective cgroup cpuset when present.

Explicit library configuration MAY lower or raise derived defaults. Foreground
requests MAY use the effective CPU budget. Internal background tasks MUST use a
separate conservative budget and pass QoS admission. Background saturation MUST
NOT reject an explicit foreground request.

Runtime reports SHOULD expose host, quota, cpuset, effective, foreground, and
background parallelism without including host paths or secret configuration.

CPU concurrency and storage I/O depth are separate budgets. Modern SSD and NVMe
devices expose multiple queues and channels, so foreground scans MAY issue
independent segment reads concurrently up to a bounded I/O depth.

- Candidate selection and segment pruning MUST happen before issuing payload
  reads.
- Parallel reads SHOULD operate on coarse, independent ranges; the engine MUST
  avoid turning one scan into unbounded tiny random I/O.
- Foreground and background I/O MUST use separate admission budgets.
- WAL commit ordering, manifest publication, and per-index delta ordering remain
  serialized even when data reads are parallel.
- `DesktopBound` defaults SHOULD use multiple foreground I/O slots with a
  bounded upper limit. `MobileEmbedded` defaults MUST use a lower depth.
- The host MAY override I/O depth using device-specific knowledge. The library
  MUST NOT infer a precise hardware queue count from CPU count alone.

Device discovery is path-specific and evidence preserving:

- Linux MAY read the database path's block-device `rotational` and
  `nr_requests` sysfs attributes. A partition must inherit evidence only from
  its actual parent block-device queue.
- Apple platforms MAY classify memory, network, or virtual filesystems. APFS,
  HFS, or another filesystem name does not prove SSD media and MUST remain
  `Unknown` unless the host provides native device evidence.
- Unsupported or unreadable platform metadata yields an `Unknown` device with
  conservative I/O depth. It MUST NOT fall back to a CPU-derived channel count.
- Runtime reports expose only typed media, discovery source, and bounded queue
  hints. Device names, mount paths, and database paths are not included.
- An explicit host profile or I/O budget takes precedence over discovery.

Future async or platform-specific backends MAY use `io_uring`, IOCP, or native
mobile APIs behind the same bounded storage-facing contract. Correctness MUST
not depend on a specific async runtime.

## Commit Durability

The default durability policy is `SyncOnEveryWrite`.

A successful mutation response provides the following guarantee:

> After the response is returned, every mutation in the committed request can
> be recovered after process or machine failure, subject to the guarantees of
> the underlying filesystem and storage device.

The commit order MUST be:

1. validate and stage the complete mutation batch;
2. append one checksummed WAL commit record;
3. flush and synchronize the WAL data;
4. synchronize the parent directory when the WAL file is first created;
5. publish the new snapshot and commit epoch;
6. update rebuildable in-memory projections;
7. return success.

If any operation before publication fails, the staged snapshot MUST NOT become
visible and success MUST NOT be returned. A request that has not returned MAY
be absent after recovery or may be replayed if its complete durable commit
record exists.

`SyncOnCheckpoint` MAY remain available as an explicit relaxed policy, but it
MUST NOT be the embedded default and MUST be reported as non-production-safe
for the response durability contract.

## Recovery And Repair

Every canonical durable artifact MUST carry a version and checksum. Recovery
MUST distinguish:

- a torn or incomplete WAL tail;
- a checksum mismatch in the middle of the WAL;
- a corrupt checkpoint or manifest;
- a corrupt rebuildable index or projection.

Automatic repair is allowed only when the correct state is derivable:

- truncate a torn WAL tail after the last complete committed record;
- rebuild an index or projection from canonical graph state;
- restore a checkpoint from a separately validated previous generation and
  replay the validated WAL suffix.

Middle-of-log corruption and canonical-state ambiguity MUST fail closed.
Corrupt files SHOULD be quarantined with a non-sensitive generated identifier.
Repair reports MUST record the decision, recovered commit epoch, discarded
tail length, and rebuilt artifact kinds without exposing database contents or
absolute paths.

## Incremental Indexes

Canonical graph mutations and index deltas MUST share a commit epoch. Each
incremental index MUST persist:

- format and schema version;
- source graph commit epoch;
- last completely applied delta epoch;
- document or key-space identity;
- checksum.

An index MAY lag canonical state. It MUST NOT claim freshness beyond its durable
watermark. Query planning MAY use a lagging index only when a residual path
preserves correctness; otherwise it MUST catch up or use a canonical scan.

ANN, quantized vectors, FTS, BM25, property indexes, segment summaries, and
membership filters are rebuildable. Their corruption MUST NOT make canonical
graph data unrecoverable.

## OpenTelemetry

OpenTelemetry support MUST be optional and disabled by default. Enabling it
MUST occur through a Rust library configuration object. Skein MUST NOT install
or replace the process-global tracing subscriber implicitly.

The first instrumentation surface SHOULD include:

- query parse, optimize, execute, and result shaping;
- WAL append and synchronization;
- checkpoint publication and recovery;
- index delta application and rebuild;
- background admission, defer, execution, and completion.

Attributes MUST be low-cardinality by default. Raw query text, property values,
embeddings, file paths, tokens, and credentials MUST NOT be exported. Query
fingerprints, operator kinds, commit epochs, row counts, byte counts, durations,
and structured error codes MAY be exported.

Exporter failure MUST NOT fail a database request. Export queues and batch sizes
MUST be bounded so observability cannot exhaust memory.

## Access Control Extension

ACL is a planned optional extension and is not part of the current production
capability surface. Its design MUST:

- remain disabled by default for `MobileEmbedded`;
- support compile-time exclusion for applications that do not need it;
- bind authorization context to planning and execution, not only API handlers;
- include authorization identity and policy epoch in plan-cache keys;
- preserve fail-closed behavior for unsupported or stale policy state;
- avoid storing credentials or secret policy inputs in logs, plans, or
  telemetry.

Storage-level visibility enforcement is required before ACL can be declared
complete. Parser-only or result-filtering implementations are insufficient.

## Verification

Release validation MUST include:

- model checking of durable-before-publish and pinned-reader invariants;
- concurrent reader/writer and serialized-writer tests;
- crash-point tests before append, after append, after sync, and after publish;
- torn-tail, checksum-corruption, checkpoint-fallback, and derived-index rebuild
  tests;
- cgroup quota and cpuset parser tests;
- incremental index catch-up and stale-watermark tests;
- OpenTelemetry disabled, bounded-export, redaction, and exporter-failure tests.
