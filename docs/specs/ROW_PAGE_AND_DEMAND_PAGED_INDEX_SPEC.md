# Row-Page Canonical Storage and Demand-Paged Index Specification

## Scope

This specification defines Skein's target durable layout for a PC-oriented
embedded database. Canonical graph and relational state is row-oriented,
page-bounded, and indexed. Primary, unique, secondary, graph-property, and
adjacency indexes are persistent immutable pages that are read on demand
through a byte-bounded cache.

This contract replaces the former goal of making column groups the canonical
representation. Existing column-group, deletion-vector, and columnar-shadow
artifacts remain valid derived-projection experiments. They MUST NOT become a
recovery dependency or a second canonical writer without a new specification
and workload evidence.

Normative `MUST`, `MUST NOT`, `SHOULD`, and `MAY` clauses take precedence over
descriptive implementation notes, per [`README.md`](README.md).

## Goals and non-goals

Skein is an embedded, TP-first knowledge database. Point reads, short
transactions, predictable resident memory, cross-platform recovery, and graph
locality have priority over scan throughput.

Goals:

1. Canonical immutable row pages ordered by stable primary key or entity id.
2. Persistent indexes whose leaf and posting cardinality does not determine
   startup residency.
3. Snapshot/COW MVCC with durable-before-visible root publication.
4. PostgreSQL-style row, unique-key, and range locking plus graph entity and
   adjacency locking.
5. Storage-neutral vectorized execution over row pages.
6. One byte ledger covering caches, pins, dirty state, WAL replay, execution,
   results, spill, and projections.
7. Derived columnar, BM25, vector, statistics, and analytics projections that
   are removable without changing canonical recovery.

Non-goals:

- multi-process writers;
- distributed consensus or Cloud-primary execution;
- arbitrary historical time travel;
- serializable snapshot isolation;
- reliance on OS swap, `mmap`, or Linux-specific asynchronous I/O for
  correctness;
- making every projection transactionally canonical.

## Current and target boundaries

The current implementation already provides immutable COW row collections,
reader generation pins, strict WAL recovery, generation-scoped segment range
reads, and a byte-bounded digest-verified cache. Relational index postings are
currently rebuilt from all rows when a checkpoint is opened. Graph indexes are
also materialized as in-memory COW collections.

The migration defined here is incremental:

1. write and validate immutable page artifacts without serving from them;
2. publish generation-fenced row and index roots in shadow mode;
3. differentially compare disk-backed lookup with the current oracle;
4. activate relational and graph index reads independently;
5. activate canonical row-page reads only after separate evidence.

No phase may silently serve a mixture of row data and stale index roots.

## Identities and terminology

- **Commit epoch**: monotonically increasing visibility identity; one atomic
  graph/relational WAL batch owns one epoch.
- **Manifest generation**: immutable physical publication identity.
- **PageId**: stable logical page identity within one store and page kind.
- **RowId**: stable logical record identity. A physical rewrite MUST NOT change
  the row's externally visible identity.
- **Page descriptor**: compact metadata containing page kind, key bounds,
  record count, extent offset/length, checksum, and generation.
- **Root descriptor**: the bounded entrypoint for one row or index tree.
- **Dirty overlay**: bounded, mutable transaction or recovery state newer than
  the published root.
- **Index delta page**: immutable disk-backed flush of a dirty index overlay.
- **Page pin**: an RAII lease preventing eviction or physical reclamation.
- **Derived projection**: rebuildable state selected by source epoch and schema
  identity but excluded from canonical recovery.

## Canonical row pages

### Relational layout

1. A relational table MUST have a primary key.
2. Leaf row pages MUST contain complete encoded rows ordered by primary key.
3. Page directories MUST support binary search without decoding every row.
4. A point read MUST decode only the selected row and requested fields.
5. Ordered pagination MUST stop after the admitted `LIMIT` and MUST NOT drain
   later pages.
6. A mutation creates new immutable page images or bounded dirty pages; it
   MUST NOT mutate a page visible to a pinned reader.

### Graph layout

1. Nodes and relationships MUST use stable ids and independently addressable
   row pages.
2. Graph record pages MAY cluster small declared properties with an entity
   when benchmark evidence shows a point-read benefit.
3. Dense or unbounded adjacency MUST use separate pages ordered by
   `(node id, relationship type, direction, neighbor id, relationship id)`.
4. Sparse adjacency MAY be inlined only behind an evidence-derived byte and
   degree threshold.
5. Relationship create/delete MUST preserve endpoint and adjacency-index
   agreement in the same commit epoch.

### Large values

Strings, JSON, binary values, and vectors above the inline threshold MUST use
separate immutable extents. The row stores `(length, digest, location)`. A
query that does not project the value MUST NOT read, decode, or clone its
payload. A single value remains subject to an explicit maximum size even when
it is file-backed.

## Persistent index pages

### Index classes

The persistent page contract applies to:

- relational primary-key trees;
- unique and secondary indexes;
- graph stable-id and declared property indexes;
- forward and reverse adjacency indexes.

BM25, vector ANN, optional columnar scan structures, and algorithm outputs are
derived projections and follow the projection contract below.

### Page structure

1. Every index has one compact root descriptor selected by the manifest.
2. Root, interior, leaf, and posting pages MUST be independently addressable
   and checksummed.
3. Interior entries contain separator keys and child `PageId` values. Leaf
   entries contain index keys and bounded inline postings or posting-page
   references.
4. Oversized posting lists MUST be split into independently addressable pages.
5. Page encoders MUST enforce the same size and count limits as decoders.
6. Unknown optional fields MAY be skipped. Unknown required page kinds or
   versions MUST fail closed.
7. Page descriptors MUST be sufficient to reject impossible offsets, lengths,
   key bounds, generations, and page kinds before allocating the page body.

### Generation agreement

1. A row root, all required index roots, catalog identity, and schema epoch
   become visible through one manifest generation.
2. An index root MUST NOT lead or lag the canonical row epoch it claims to
   index.
3. A unique index used for mutation validation MUST be ready at the same epoch
   as the rows. Missing, stale, or corrupt uniqueness state makes the database
   unusable for writes.
4. A non-constraint query index MAY enter an explicit rebuild state. The
   planner MUST either use a correct canonical fallback or fail the query; it
   MUST NOT silently use stale postings.
5. Rebuild publication is compare-and-publish against its source generation.
   A stale builder cannot replace a newer root.

## Database open and recovery

### Startup contract

Normal open proceeds in this order:

1. acquire the exclusive embedded-directory lease;
2. read and verify the superblock and active manifest;
3. open the catalog, schema, and compact row/index root descriptors;
4. replay every WAL batch required by strict recovery;
5. publish the usable in-process root handle.

WAL replay may make total startup slow. Skein MUST NOT skip a valid batch,
truncate a durable prefix, or change correctness merely to meet a startup
latency target.

Index startup has a stronger bound:

- normal open MUST NOT scan row pages;
- normal open MUST NOT enumerate, decode, or warm all index pages;
- normal open MUST NOT rebuild all postings;
- mandatory index residency MUST be bounded by catalog, root-descriptor, and
  dirty-recovery state, not index entry or leaf-page count.

The current unconditional `rebuild_indexes()` behavior is a migration oracle,
not the target open path.

### WAL index recovery

1. WAL replay applies changes after the published index epoch to a bounded
   dirty overlay.
2. When the overlay reaches its byte budget, recovery MAY flush immutable
   index delta pages and continue replay. It MUST NOT load the complete base
   index to merge the delta during open.
3. Point lookup and range iteration merge the base root, ordered recovery delta
   pages, and the remaining dirty overlay under one visibility rule.
4. Delta ordering and duplicate suppression MUST produce the same result as a
   complete rebuild at the recovered commit epoch.
5. A crash during recovery-delta flush leaves either the previous selected
   root plus replayable WAL or a completely published newer recovery root.

### Integrity boundary

Normal open validates the manifest, root descriptors, file lengths, and the
pages it reads. Cold leaf and posting checksums are verified on first access.
Consequently, a fast normal open is not a full-media scrub. A checksum failure
during query execution poisons the handle and fails closed. Explicit
doctor/deep-scrub mode visits every reachable page and projection artifact.

## Demand paging and cache ownership

1. Skein manages page-in/page-out through its own byte-bounded cache. OS swap
   is neither an accounting mechanism nor a correctness dependency.
2. The default cross-platform path uses bounded positional file reads. `mmap`,
   `io_uring`, and platform-specific direct I/O are optional evidence-gated
   optimizations.
3. A cold point lookup reads only admitted root-to-leaf paths and required
   posting pages. A range query reads an admitted leaf window.
4. Cache identity includes store, manifest generation, page identity, digest,
   and representation kind.
5. A clean unpinned page may be evicted. A pinned page is not evictable. A
   dirty page is not evictable until its WAL and page-publication obligations
   are satisfied.
6. Pin lifetime is bounded to one cursor window or pipeline wave. Query
   cancellation, timeout, error, and panic MUST release every pin.
7. Prefetch is optional, bounded by request count, bytes, pins, deadline, and
   cancellation. Database open MUST NOT launch an unbounded warmup.
8. Oversized pages are rejected before insertion. The cache MUST NOT exceed its
   capacity in order to admit one exceptional entry.

## Transaction and lock contract

Ordinary snapshot reads do not acquire row locks. Locking reads and mutations
use logical identities rather than physical page addresses:

- `Row(table, primary_key)`;
- `UniqueKey(index, key)`, including an absent key;
- `KeyRange(index, lower, upper)` for indexed predicates and gaps;
- `GraphNode(node_id)` and `GraphRelationship(relationship_id)`;
- `NodeDeleteGuard(node_id)`;
- `AdjacencyRange(node_id, type, direction, bounds)`.

`SELECT ... FOR SHARE` and `SELECT ... FOR UPDATE` acquire shared or exclusive
key/range locks through SQL. Exact UPDATE and DELETE acquire exclusive row
locks. INSERT, UPSERT, and MERGE acquire unique-key locks before checking
existence. Foreign-key validation acquires shared referenced-key locks.

Requests are normalized and acquired in deterministic namespace/key order. A
wait-for graph detects cycles and returns a retryable victim. The lock table is
bounded by count and bytes; admitted escalation uses an observable table or
adjacency-range lock. Statement rollback releases locks obtained after its
savepoint. Commit, rollback, cancellation, timeout, and panic release all
transaction locks.

Graph mutation locking uses a two-pass COW protocol. The first pass stages the
mutation only in the transaction-private workspace and captures its exact WAL
footprint. Skein restores that statement workspace, acquires the derived
logical identities, and deterministically replays the statement. Node and
relationship ID allocation locks prevent two pinned snapshots from allocating
the same physical identity. Shared node-delete guards held by relationship
creation conflict with an exclusive guard held by node deletion. Typed incoming
and outgoing adjacency locks protect the posting groups changed by relationship
create/delete. Label and relationship-type locks cover uniqueness and other
catalog constraints; schema changes and any footprint that cannot be derived
completely use the database lock.

The concrete graph identities are valid only for the snapshot used by the
first pass. If the published epoch advances before a newly derived lock set is
admitted, Skein rejects the transaction instead of refreshing and replaying
against an uncovered access set. A property write covered by a uniqueness
constraint takes exclusive constraint-subject coverage; a non-unique property
write retains shared subject coverage plus its exclusive entity lock.

A bounded lock timeout restores the statement's prior lock set and leaves the
transaction usable. A deadlock victim or lock-budget rejection aborts the whole
transaction and releases all locks. A failure during either staging or replay
restores the statement's graph workspace and prior lock set. Successful
statements retain their logical locks until commit or transaction rollback.

Row locks remain correct while index pages are evicted or physically rewritten
because lock identities never contain a `PageId`, file offset, or cache lease.

## Vectorized execution over row pages

Persistent row orientation does not require scalar execution. The runtime
uses:

```text
PhysicalPlanSpec -> OperatorSpec -> OperatorState -> bounded typed Batch
```

A row-page scan decodes only required fields into reusable typed vectors and a
selection. `RowLocator` values defer large-value hydration until filtering and
ranking select admitted rows. A batch has row and byte limits. Page pins live
for at most one pipeline wave.

Scan, filter, project, expand, and limit SHOULD retain typed batches. Scalar
`Binding` values remain compatibility and final-result boundaries. Sort,
aggregate, distinct, and join MUST charge resident state and spill when their
admitted workarea is exhausted.

## Resource governance

One hierarchical byte ledger MUST account for:

- root descriptors and catalog metadata;
- raw and decoded row/index cache pages;
- page pins and dirty overlays;
- WAL and recovery buffers;
- execution batches and blocking operator state;
- result materialization and spill staging;
- derived projection maintenance.

Resident cache bytes and pinned bytes are distinct. Shared `Arc` values are
charged once to the owning cache and separately reported as pinned while a
lease exists. Allocating another reference MUST NOT charge the payload again.

Reclamation order is derived projection cache, cold index/row pages, background
pause, spillable execution state, writer backpressure, and finally rejection
of new work. Process RSS and page-fault observations validate the ledger. An
RSS hard watermark may reject new work even when logical accounting claims
headroom.

## Derived projections

Column groups, deletion vectors, BM25, vector ANN, statistics, and analytics
artifacts are selected by source commit epoch, schema identity, algorithm/index
identity, and manifest generation. Publication is durable-before-visible and
generation fenced. A projection may be rebuilt or discarded without changing
canonical row/index recovery.

SearchIndex remains external derived state. Search documents, ANN internals,
and search cache state MUST NOT enter the canonical graph WAL. Incremental
projection cursors may consume commit-ordered change evidence, but canonical
commit acknowledgment does not depend on projection freshness.

## Formal obligations

`SkeinTransactionConcurrency.tla` already owns the transaction-level subset
of this contract. The remaining model names below are planned ownership
boundaries and MUST land before their corresponding production activation:

- `SkeinTransactionConcurrency.tla`: logical lock namespaces, compatibility,
  wait-for deadlocks, escalation, savepoint release, and durable publication.
- planned `SkeinCowPagePublication.tla`: WAL ordering, immutable page publication,
  reader pins, crash recovery, and reclamation.
- planned `SkeinIndexPublication.tla`: row/index generation agreement, uniqueness,
  failed publication, rebuild publication, and stale-root rejection.
- planned `SkeinIndexRecovery.tla`: base root plus ordered WAL delta equivalence,
  bounded flush, crash recovery, and no partial replay visibility.
- planned `SkeinPageCacheAdmission.tla`: resident/pinned/dirty accounting, eviction,
  cancellation, foreground reserve, and background progress.

Existing `SkeinCompactionVisibility.tla`, `SkeinColumnGroupManifest.tla`, and
`SkeinColumnarShadowIntegration.tla` continue to prove derived column-group
behavior. They do not define canonical row/index recovery.

## Evidence and activation gates

Correctness evidence includes:

- row/index lookup differential tests against the current in-memory oracle;
- restart at every WAL/page/manifest publication boundary;
- missing, stale, truncated, bit-flipped, and cross-generation page rejection;
- same-row serialization, disjoint-writer progress, absent-key uniqueness,
  range phantom prevention, and deadlock victim cleanup;
- cold and warm point/range queries under cache eviction;
- cancellation and panic pin cleanup;
- row-versus-batch execution differential and fuzz corpora.

Performance and resource evidence records separately:

- manifest/root-open latency;
- WAL replay latency;
- total open latency and peak RSS;
- first cold point/range query latency;
- warmed point/range query latency;
- page reads, bytes, cache hits, evictions, resident bytes, and pinned bytes;
- dirty and WAL bytes, spill bytes, page faults, and write amplification.

With fixed schema and WAL input, manifest/root-open work and mandatory index
residency MUST remain independent of row, index-entry, and leaf-page counts.
Total startup MAY grow with the WAL replay work. Point reads and low-concurrency
commits MUST NOT regress by more than five percent at p95 when a production
path is activated. A vectorized fragment requires at least twenty percent
throughput improvement or fifteen percent lower CPU per row.

## Delivery discipline

Codec, shadow publication, demand reader, WAL recovery delta, and production
activation are separate commits. New formats remain unselected until their
reader, differential oracle, corruption tests, formal model, and resource
evidence are present. Relational and graph activation are separate so either
path can remain on the current oracle without changing durable canonical
bytes.
