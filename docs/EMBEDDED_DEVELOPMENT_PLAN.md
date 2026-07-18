# Embedded Graph Database Development Plan

## Objective

Replace the Nowledge local Ladybug/Kuzu graph data plane with Skein without
changing the product's graph semantics or coupling canonical graph storage to
vector search.

The implementation target is the local embedded engine. PostgreSQL remains the
Cloud source of truth, LanceDB remains a rebuildable local search projection,
and large content bodies remain outside the graph store.

## Source Boundaries

Three codebases define the work:

- `nowledge/mem` defines the compatibility contract through `nmem-graph`, its
  Cypher call sites, schema convergence, recovery behavior, and graph algorithm
  usage.
- Skein owns the new parser, planner, optimizer, executor, storage format, WAL,
  checkpoint, and stable embedded API.
- Chryso is a reference for optimizer structure: memo groups, logical and
  physical rules, physical properties, structured costs, deterministic plan
  fingerprints, and explain traces. SQL-specific operators and statistics are
  not copied into Skein.

## Non-Negotiable Invariants

1. Stable logical node and relationship identities never encode physical page
   locations.
2. A committed mutation batch is fully recovered or not recovered at all.
3. Checkpoints are published atomically and never expose a partially written
   snapshot.
4. Search and graph analytics are rebuildable projections, not canonical graph
   state.
5. Parser output is syntax-only. Parameter binding and semantic validation
   happen before logical planning.
6. The optimizer is deterministic under a fixed catalog, statistics snapshot,
   rule set, and search budget.
7. The first concurrency contract is one writer with snapshot readers.
8. A Ladybug compatibility comparison must pass before any production cutover.
9. Embedded deployments are resource constrained by default: foreground graph
   user requests should not be gated by local background budgets, while
   internal projection, import, analytics, and shadow work must be able to
   defer itself under resource pressure.
10. FTS/BM25 and retrieval projections must support incremental maintenance for
    ordinary row upsert/delete changes; full rebuilds are repair paths, not the
    steady-state update mechanism.

## Required Capability Surface

### Embedded API

- open or create by path
- storage format/version inspection
- read-only and recovery configuration
- parameterized query and explain
- explicit read and write transactions
- commit, rollback, and checkpoint
- bounded resource configuration
- basic local QoS hooks for internal background admission, operation budgets,
  optional per-class background budgets, and deferrable work; performance
  should come from clean architecture and bounded work units before low-level
  tuning

### Cypher and Semantic Analysis

The initial production subset is derived from real Nowledge queries:

- `MATCH`, one-hop and bounded multi-hop patterns
- `WHERE` equality, range predicates, boolean predicates, null checks, and list
  membership
- `RETURN`, aliases, aggregation, ordering, offset, and limit
- `CREATE`, `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`
- parameters for property values, predicates, pagination, and list filters
- Nowledge-used zero-argument `CURRENT_TIMESTAMP()` values as epoch-nanos
  integers
- schema DDL and migration statements used by schema convergence
- projected graph lifecycle and graph algorithm procedure calls

Labels, relationship types, and property names remain identifiers rather than
runtime parameters. This keeps catalog resolution deterministic and prevents a
single prepared query from changing its schema dependencies.

### Storage and Recovery

- versioned catalog tokens and index descriptors
- canonical node, relationship, and property records
- outgoing and incoming adjacency indexes
- equality and composite equality indexes first, followed by range indexes
  where real queries require them
- checksummed batch WAL records with torn-tail detection
- checkpoint epoch and WAL replay boundary in a manifest
- copy-on-write or immutable snapshot pages for readers
- bounded compaction and orphan cleanup

### Optimizer

Reuse Chryso's separation of concerns, not its SQL operator set:

- memo groups store equivalent graph plans
- logical rules normalize and reorder graph patterns
- implementation rules produce scan, seek, expand, join, and mutation choices
- physical properties track bound variables, ordering, uniqueness, adjacency
  direction, and index coverage
- graph statistics track label counts, relationship counts, distinct values,
  and degree summaries
- structured costs expose CPU, random I/O, sequential I/O, and output rows
- deterministic tie-breaking and explicit search budgets make plans testable

The optimizer must not hide storage-specific decisions inside the parser or
executor. Storage capabilities enter through catalog metadata and physical
implementation rules.

## Delivery Phases

### Phase 0: Executable Vertical Slice

Scope:

- parser to executor pipeline
- stable logical IDs
- create and one-hop match
- property equality index seek
- batch WAL, recovery, checkpoint, and mutation transaction facade
- deterministic explain output

Exit gate:

- format, unit, and lint checks pass
- restart and torn-WAL tests pass
- one end-to-end indexed query produces a stable physical plan

Status: complete in the initial Skein MVP.

### Phase 1: Compatibility Front Door

Scope:

- parameterized query, explain, and transaction APIs
- a typed adapter matching the `nmem-graph` execution shape
- query inventory generated from real Nowledge call sites
- dual-engine fixtures comparing rows and error classes

Exit gate:

- every supported query binds parameters before planning
- missing parameters fail before mutation or storage access
- the first read and mutation fixture families match Ladybug behavior

Status: parameter binding is implemented. `NowledgeGraphAdapter` now exposes a
typed front door for parameterized query, explain, and grouped mutation
transaction execution without requiring raw string interpolation.
`DatabaseConfig` provides read-only operation and bounded read-result
configuration. `read_only` opens only existing database directories without
creating missing paths, then rejects Cypher mutations, transaction mutations,
checkpoints, schema maintenance, database-owned projected graph artifact
rebuilds, and derived artifact job execution before they write database-owned
state. `max_read_result_rows` caps direct read query and read-transaction
result rows, and `max_optimizer_groups` caps cascades memo search groups with
the existing deterministic direct physical fallback warning. `recovery_mode`
defaults to torn-tail tolerant WAL replay and can be set to strict recovery to
reject a torn WAL tail or checksum mismatch during open.
`max_wal_replay_entries` caps startup WAL replay after valid record decode and
before applying the next record; it counts top-level WAL records rather than
child operations inside a batch, preserving batch replay atomicity. Mutation
queries keep their WAL/commit semantics and are not failed after durable
execution.
`nowledge_memory_core_fixture` and `nowledge_memory_core_inventory` expose the
current Nowledge core compatibility contract as public migration inputs,
including Nowledge-used entity reuse exact, case-insensitive, alias-containment,
same-type bounded scan reads, entity temporal metadata create/update writes, and
the production entity `MENTIONS`/`RELATES_TO` relationship write shapes used by
`entity_write`.
`CompatibilityQueryCallSite` and `build_compatibility_query_inventory` provide a
stable production call-site inventory builder with source metadata and duplicate
check-name validation. `build_compatibility_query_inventory_from_json` and
`build_compatibility_query_inventory_from_json_str` accept scanner JSON
artifacts shaped as `name` plus `call_sites`, while
`compatibility_query_inventory_to_json` exports the validated `required_checks`
artifact for audit or CI reuse. `assess_query_inventory_coverage` turns the
resulting machine-readable required query inventory into covered, missing, and
extra fixture check reports, and `assess_query_inventory_gate` converts that
coverage into `Ready` or `Blocked` with explicit blockers. Coverage, inventory
gate, shadow cutover, and migration gate reports all have JSON exporters with
stable lowercase `decision` values for CI consumption.
`assess_compatibility_migration_gate_bundle` and
`compatibility_migration_gate_bundle_to_json` provide the single-call CI path
that packages coverage, inventory gate, cutover, and migration gate evidence.
`assess_compatibility_migration_gate` combines the inventory gate and shadow
cutover gate into one migration decision. `scan_nowledge_query_inventory` and
the `scan-nowledge-inventory` CLI command provide a production graph-source
scanner for Nowledge Cypher string literals, filtering out non-graph content
store SQL, prompt text, tests, benches, and smoke binaries while emitting the
same audited JSON inventory artifact. `scan-nowledge-cypher-coverage` emits the
same coverage report shape after matching scanner-generated call sites to
fixture checks by normalized Cypher text, preserving source metadata without
requiring duplicate semantic fixture names. `scan-nowledge-cypher-coverage-detail`
also emits `covered_items` and `missing_items` with source, query family, and
Cypher text for fixture work driven by production Nowledge queries. The scanner
filters unresolved Rust format templates such as `{space_clause}` while
preserving valid Cypher map literals such as `{id: $id}` and string values such
as `'{}'`; dynamic query builders should be covered by their concrete
production shapes. The current live scanner coverage gate over the local
Nowledge graph-source tree is complete for the scanned surface:
`nowledge-scanned-inventory` requires 694 Cypher checks, the
`nowledge-memory-core` fixture covers all 694, and `missing_items` is empty.
The refreshed audit artifact used for this status is
`/private/tmp/skein-cypher-coverage-doc-refresh.json`. The remaining Phase 1
work is no longer fixture-gap closure for the current scan; it is to attach the
previous wrapper through the external shadow adapter when migration-gate
evidence is needed, and to rerun the scanner whenever Nowledge adds new graph
call sites.

### Phase 2: Snapshot Transactions and MVCC

Scope:

- committed epoch or LSN per write batch
- immutable reader snapshots
- single-writer serialization
- checkpoint pinning and safe page reclamation

Exit gate:

- readers never observe partial commits
- a long reader survives concurrent commits and checkpoints
- recovery exposes exactly the last durable commit boundary

Status: API-level immutable read snapshots are implemented through
`DatabaseReadTransaction`. A read transaction owns a catalog and graph snapshot,
rejects mutation statements, does not observe later commits, and can continue
after the writer checkpoints. Active readers register their snapshot commit
epoch in a process-local pin registry and unregister on drop. The store also
tracks commit epochs and publishes a checksummed checkpoint manifest with
checkpoint epoch, checkpoint commit epoch, oldest active reader commit epoch,
safe reclaim commit epoch, WAL replay start LSN, and next WAL LSN. The public
`Database::storage_reclamation_watermark` API exposes current commit epoch,
checkpoint boundary, active oldest reader, and safe reclaim commit epoch without
parsing manifest text. Recovery filters projected graph artifact cache entries
that do not match the replayed commit epoch and active projected graph
definition, so stale derived artifacts are not exposed through status metadata.
Checkpoint, manifest, and projected graph artifact publication sync the
published file and parent directory around the atomic rename boundary without
adding per-mutation directory syncs.
Read transactions also expose the typed knowledge entity, neighborhood, path,
and subgraph operations over their pinned graph snapshot, so knowledge
navigation can remain snapshot-stable without constructing ad hoc Cypher.
Page-level MVCC, physical page/segment reclamation, and concurrent writer
coordination remain.

### Phase 3: Cypher Mutation and Query Coverage

Scope:

- `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`
- aggregation, sort, offset, limit, list parameters, and null semantics
- bounded pattern joins and `ExpandInto`
- schema DDL required by Nowledge migrations

Exit gate:

- compatibility fixtures cover every production query family
- unsupported syntax fails with a stable typed error
- no raw string interpolation is needed by the adapter

Status: ordering and pagination are implemented for the current read subset.
`ORDER BY` supports projected aliases and `variable.property` keys with
`ASC`/`DESC`; `SKIP`, `OFFSET`, and `LIMIT` accept literals or bound integer
parameters and reject negative or non-integer values before execution.
Null and list predicates are also implemented for the current read subset:
`IS NULL`, `IS NOT NULL`, and `IN` support literal lists, parameters inside
literal lists such as `[1, $id, 3]`, and bound list parameters, with non-list
`IN` values rejected before execution. Nowledge-used entity alias lookup also
supports `list_contains(e.aliases, $name)` for property-list membership.
Nowledge-used node property patterns in
read `MATCH` clauses, including source and one-hop target node property
patterns, are lowered to the same property equality predicate path. One-hop
unlabeled node matches such as `MATCH (n) WHERE n.id IN $ids RETURN n.id` and
`MATCH (n) WHERE n.id = $node_id SET n.community_id = $community_id` are
supported for current Nowledge graph analysis and community-assignment paths.
Two exact node patterns without a relationship are supported for Nowledge
source-provenance endpoint checks, for example
`MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)`.
The same two-node read plan also accepts the consecutive MATCH spelling used by
entity relationship endpoint checks, for example
`MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id`.
Count-only one-hop `OPTIONAL MATCH` is supported for Nowledge thread cleanup
reads, including
`MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)`
and the legacy extracted-reference count over already matched messages. The
Nowledge graph-analysis degree query is also supported as a narrow
row-preserving shape:
`MATCH (e:Entity) OPTIONAL MATCH (e)-[r]-() WITH e, COUNT(r) as degree RETURN e.id, e.name, degree ORDER BY degree DESC LIMIT 10`.
Direct optional source-projection plus count reads such as
`MATCH (l:Label) OPTIONAL MATCH (m:Memory)-[:HAS_LABEL]->(l) RETURN l.id, l.name, COUNT(m) AS usage_count`
are lowered to the same optional-degree operator when all non-count return
items reference the already bound source node.
Nowledge thread bulk-move reads support the normalized-space predicate shape
`CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END =/<> $space_id`
used to treat missing and empty thread space IDs as `default`. General OPTIONAL
row preservation, general `WITH`, general `CASE`, and general `COLLECT`
expressions remain outside this bounded subset.
The companion bulk-move write shape
`MATCH (t:Thread) WHERE ... SET t.space_id = $target_space_id, t.updated_at = $updated_at RETURN t.thread_id`
is supported as a single-node update-return path over already matched rows; it
does not add generic row-binding relationship updates.
Nowledge thread distillation reads support the optional source filter shape
`($source IS NULL OR t.source = $source)` by binding parameter null checks into
constant predicates before planning.
Nowledge source revision history supports the bounded outgoing path read
`MATCH p = (s:Source {id: $source_id})-[:REVISED_AS*1..10]->(older:Source) RETURN older...`
by accepting an unused path binding prefix and reusing the existing finite
multi-hop expand operator. Path values remain unsupported unless a real
Nowledge caller needs to return them.
Nowledge graph path reads support the endpoint-id bounded
`ALL SHORTEST` shape
`MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, properties(nodes(p), 'name') AS names, length(p) AS hops`
through a dedicated shortest-path read operator. The operator returns all
simple paths at the first target depth and only supports `properties(nodes(p),
...)` plus `length(p)` projections; generic returned path values and
unbounded shortest-path searches remain outside the compatibility subset.
Nowledge feed reads support the bounded synthesized-source aggregation
`MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids`
as a direct group-by-property plus collected-property list. General `WITH`
projection chains and general `COLLECT` expressions remain outside this
bounded subset.
Nowledge synthesized-source coverage lookups also support the bounded grouped
aggregate filter shape
`WITH c.id AS cid, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid LIMIT 1`
and the two-column title variant returning `cid, ct`. This is implemented as
grouped aggregation over matched rows, a column filter on the aggregate alias,
and final column projection; it is not a general HAVING implementation.
Nowledge community memory reads also support the bounded
`WITH m, COUNT(e) AS entity_count` shape after a one-hop relationship match.
The grouped node is represented as a projected map column, so subsequent
`m.property` and limited scalar projections such as `COALESCE(m.is_latest,
true)` read from that map column, and `ORDER BY entity_count, m.importance`
sorts before the final projection.
The same bounded group-count path accepts `COUNT(DISTINCT variable)` for
Nowledge label distribution queries such as `WITH l, COUNT(DISTINCT m) AS
memory_count`.
It also accepts the Nowledge graph-memory shape where `ORDER BY` and `LIMIT`
are attached to the aggregate `WITH` before the final `RETURN`, for example
`WITH m, COUNT(DISTINCT e) AS mention_breadth ORDER BY mention_breadth DESC,
COALESCE(m.importance, 0.5) DESC LIMIT $top_n RETURN ...`.
The same aggregate-with parser accepts multiple count items after a grouped
variable, including `COUNT(DISTINCT variable.property)` and `COUNT(*)`, for
Nowledge bridge entity reads, including the production variant that filters the
aggregate alias before final projection.
The related community bridge lookup shape also supports a bounded
post-aggregate node lookup:
`WITH e2.community_id AS other_cid, COUNT(*) AS shared_edge_count ORDER BY
shared_edge_count DESC LIMIT $limit MATCH (c:Community) WHERE c.community_id =
other_cid RETURN ...`.
The same post-aggregate lookup operator supports the Nowledge row-preserving
optional community lookup used by community memory counts, returning `NULL`
community fields when no `Community` node exists for a grouped community id.
Community list reads also support the bounded presence-ranking order expression
`CASE WHEN c.ai_summary IS NOT NULL AND c.ai_summary <> '' THEN 0 ELSE 1 END`,
used to order summarized communities before unsummarized communities.
Nowledge graph overview reads also support repeated node labels such as
`(neighbor:Entity:Memory)` as an any-of label set; unknown non-empty labels
produce an empty scan instead of falling back to all nodes. One-hop
Nowledge-used whole-record projections such as `RETURN m` and `RETURN r`
produce structured maps with stable logical identifiers, labels or relationship
type metadata, and scalar properties so repository paths like by-id memory
lookup can avoid raw graph object bindings.
One-hop
undirected relationship reads such as `-[:EVOLVES]-` are supported for the
current Nowledge neighbor and cluster queries; bounded undirected expansion
remains rejected. Anonymous relationship endpoints such as `()` and `(:Label)`
are supported for Nowledge relationship-count reads. Untyped one-hop
relationship reads such as `MATCH (a)-[r]->(b)` are supported for Nowledge
overview edge queries, and `label(r)`/`type(r)` can project the bound
relationship type. Nowledge-used `RETURN` projection fallbacks support
`COALESCE(...)` and `LEFT(...)`, including nested forms such as
`COALESCE(m.title, LEFT(COALESCE(m.content, ''), 60))`, and the same limited
expression subset is available for Nowledge-used `ORDER BY COALESCE(...)`
rank fallbacks and `WHERE COALESCE(...)`/`WHERE LEFT(...)` equality and
comparison predicates. Nowledge-used case-insensitive grep predicates such as
`LOWER(COALESCE(m.content, '')) CONTAINS LOWER($needle)` and
`LOWER(e.name) = LOWER($mention)` are supported through the same limited scalar
expression evaluator; broader predicate function composition remains separate
follow-up work.
Parenthesized predicate groups preserve explicit `AND`/`OR` precedence. String
literals support escaped quotes, backslashes, and common control escapes. Graph algorithm procedure options such
as `dampingFactor`, `maxIterations`, and `maxLevels` accept bound parameters and
are type-checked before execution. `COUNT`
aggregation is implemented for `COUNT(*)`, `COUNT(variable)`, and
`COUNT(variable.property)`, including Nowledge-used `COUNT(DISTINCT variable)`
and `COUNT(DISTINCT variable.property)` forms, one-hop relationship matches,
alias-based ordering, `ORDER BY COUNT(variable)` over the projected aggregate
column, and pagination. Nowledge-used `MIN(variable.property)` is
implemented for schema verification reads such as `min(r.weight)`, and
Nowledge-used `AVG(variable.property)` is implemented for health aggregate reads
such as `avg(m.decay_score_cached)`. One-hop
relationship variables can be returned, filtered, sorted, and counted through relationship properties, for example
`RETURN r.weight`, `WHERE r.weight > 1`, `ORDER BY r.weight`, `COUNT(r)`, and
`COUNT(r.weight)`. Stable logical identities can be projected, filtered, and
ordered with `id(m)` and `id(r)` for bound node and relationship variables, and
`id()` filters can select node and relationship mutation targets without
encoding physical page locations. One-hop relationship property patterns such as
`MATCH (m:Memory)-[r:MENTIONS {weight: $weight}]->(e:Entity)` are parsed into
the relationship expand and filter reads, relationship property `SET`, and
relationship `DELETE`; relationship property patterns on bounded multi-hop
patterns are rejected until path/list relationship semantics are implemented.
Nowledge EVOLVES progression list reads are covered as a production-shaped
one-hop read over source node properties, target node properties, relationship
properties, list-parameter filters, and target-property ordering.
One-hop relationship property `SET` and relationship `DELETE` can also split
`WHERE` predicates between the source node and relationship variable, for
example `WHERE m.id = 1 AND r.weight = 3`; mixed node/relationship `OR`
predicates are rejected for mutation filtering until row-binding mutation
semantics exist.
Grouped aggregation is implemented for property projection group keys such as
`RETURN m.kind AS kind, count(*) AS total`; distinct relationship aggregation
matches Nowledge queries such as `count(DISTINCT m)` and
`count(DISTINCT e.id)`. Projection-level `RETURN DISTINCT` deduplicates
projected rows before `ORDER BY`, `SKIP`/`OFFSET`, and `LIMIT`.
Bounded relationship expansion is implemented for finite outgoing patterns such
as `[:TYPE*1..3]`, `[:TYPE*2]`, and `[:TYPE*..3]`; unbounded `*` patterns are
rejected. Mutation coverage includes single-node exact-property `MERGE`:
matching nodes do not write WAL, missing nodes are created, parameters bind
before storage access, and duplicate MERGE statements in one explicit
transaction deduplicate against pending nodes.
Nowledge schema-migration node writes also support
`MERGE (m:SchemaMigrationLog {id: $id}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP()`;
the match key remains separate from create-only values so existing migration
rows are not narrowed by metadata fields, while newly created rows persist both
the key and create-only properties in one WAL entry.
Relationship `MERGE` is implemented for exact node-property and relationship-
property patterns, reuses existing endpoint nodes, avoids WAL writes when the
full pattern already exists, and deduplicates repeated patterns inside one
explicit transaction.
Nowledge's bounded relationship-copy migration shape
`MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, ...`
is implemented for one-hop outgoing matched relationships. It copies selected
properties from the matched relationship into newly created relationships,
reuses existing target relationships on re-run, and persists only ordinary
relationship-create WAL records.
The companion migration verification read
`WHERE NOT EXISTS { MATCH (c)-[:SYNTHESIZED_FROM]->(s) }` is implemented as a
bounded predicate over already-bound endpoints. It checks exact source and
target node ids through the relationship adjacency index and does not introduce
general Cypher subquery execution.
The migration pair-count verification shape
`WITH DISTINCT c.id AS a, s.id AS b RETURN count(*)` is lowered to a bounded
property projection, distinct row set, and global count. Broader `WITH`
projection pipelines remain unsupported until another Nowledge scanner hit
requires them.
Nowledge GraphMeta stamp writes support the bounded
`MERGE (m:GraphMeta {meta_id: 'main'}) SET ...` shape used by PageRank,
community, and lifecycle invalidation paths. Greenfield writes fold SET values
into the create record, and repeated writes inside one explicit transaction
fold into the pending create instead of emitting standalone WAL set records.
Nowledge label assignment also supports matched-endpoint relationship merge:
`MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET ...`.
The relationship match key is kept separate from create-only properties, so
existing labels are not narrowed by edge metadata while newly created HAS_LABEL
edges persist their assignment metadata in one WAL batch.
Nowledge label merge transfer supports the bounded retarget shape
`MATCH (n:Memory)-[:HAS_LABEL]->(src:Label {id: $src}) MATCH (tgt:Label {id: $tgt}) MERGE (n)-[r:HAS_LABEL]->(tgt) ON CREATE SET ...`.
The old label edge selects the source node set, the second `MATCH` selects the
new label target, and the target HAS_LABEL merge is idempotent with one grouped
WAL append for newly created edges.
Nowledge label upsert supports single-node `MERGE ... ON CREATE SET ... ON
MATCH SET ...` for the bounded label shape, including
`l.canonical_name = COALESCE(l.canonical_name, $canonical)`. Pending creates
inside one explicit transaction fold matched updates into the create record, so
repeated upserts do not add standalone WAL set records before commit.
Nowledge source-provenance relationship creation is implemented for the bounded
two-endpoint form
`MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {...}]->(s)`.
Nowledge EVOLVES writes also support the endpoint-equality form
`MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {...}]->(b)`.
These forms match existing endpoint nodes, create only the relationship records,
and persist all relationships from the statement through one grouped WAL append.
Nowledge relationship update paths support multiple assignments on the same
one-hop relationship variable, such as memory-relation review updates filtered
by `r.id`. All matched relationship property writes are emitted as one WAL
batch rather than one durable append per property. The same relationship update
path also supports Nowledge target-node property filters such as
`MATCH (m:Memory {id: $memory_id})-[r:MENTIONS]->(e:Entity {id: $target_id}) SET ...`.
One-hop relationship variable deletion is implemented for patterns such as
`MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r`; it removes
only the matched relationship, preserves endpoint nodes, persists through WAL
replay, and participates in explicit transaction commit/rollback.
One-hop relationship variable property updates are implemented for patterns such
as `MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2`;
they update only the matched relationship records, preserve endpoint nodes,
validate schema and constraints before WAL append, replay from WAL, and
participate in explicit transaction commit/rollback.
Single-node `MATCH ... SET` property updates maintain the property equality
index, persist through WAL replay, and participate in explicit transaction
commit/rollback. Nowledge-used integer self-increment assignments such as
`SET s.memory_count = s.memory_count + 1` are supported for source provenance
repair counters, and the access-tracking form
`SET m.access_count = COALESCE(m.access_count, 0) + 1, m.last_accessed_at = $now`
is supported as a multi-assignment node update. Broader arithmetic SET
expressions remain out of scope.
Nowledge-used `CURRENT_TIMESTAMP()` values in `CREATE` and `SET` bind to the
current UNIX epoch nanoseconds as `Int`, reusing the existing value/WAL encoding
instead of adding a separate timestamp storage type.
Nowledge-used `timestamp(expr)` values bind ISO strings such as
`1970-01-01T00:00:00`, optional `Z` suffixes, fractional seconds, and numeric
epoch values into the same epoch-nanos `Int` representation. This covers
freshness predicates such as `m.updated_at > timestamp($cutoff)` and
relationship property writes such as `created_at: timestamp($now)` without
adding a separate timestamp storage type.
Nowledge-used monthly statistics support `date_part('year', m.created_at)` and
`date_part('month', m.created_at)` as grouped aggregate projection keys over
epoch-nanos timestamps. Other `date_part` components remain outside the
compatibility subset until a real Nowledge call site requires them.
Single-node `MATCH ... DELETE` and `MATCH ... DETACH DELETE`
are implemented for the current predicate subset; regular delete rejects nodes
with attached relationships, detach delete removes attached relationships before
the node, and both paths persist through grouped WAL records. Schema DDL is
implemented for explicit node-label and relationship-type token creation through
`CREATE NODE LABEL` and `CREATE RELATIONSHIP TYPE`; both paths are idempotent,
persist through WAL replay and checkpoint, and participate in explicit
transaction commit/rollback. Explicit equality index DDL is implemented through
`CREATE INDEX ON :Label(property)`, persists through WAL replay and checkpoint,
and feeds optimizer descriptor lookup for equality predicates. Composite
equality index DDL is implemented through `CREATE INDEX ON :Label(a, b)`,
persists through WAL replay and checkpoint, maintains a rebuildable in-memory
composite key index, and enables `IndexNodeCompositeSeek` for conjunctions that
bind every indexed property. Range index DDL is implemented through
`CREATE RANGE INDEX ON :Label(property)`, persists through WAL replay and
checkpoint, and enables `IndexNodeRangeSeek` for
single-bound range predicates and conjunctive bounded range predicates.
Full-text graph index DDL is implemented through
`CREATE FULLTEXT INDEX ON :Label(property)`, persists through WAL replay and
checkpoint, maintains a rebuildable ngram candidate index over canonical string
properties, and enables `IndexNodeTextSeek` for `CONTAINS` predicates while
retaining a residual `FilterExec` for exact string containment semantics.
`WHERE` supports `AND`, `OR`, and unary `NOT` boolean predicates for the current
equality, inequality, range, null, list membership, `CONTAINS`, `STARTS WITH`,
and `ENDS WITH` predicate subset; disjunctions
currently execute as a residual filter rather than an index-union access path.
Per-label/property distinct counts and bounded sorted value histograms are
computed from canonical records, written to checkpoints, and used by range-index
costing for selectivity estimates. Histogram sampling is deterministic and
adaptive by property cardinality, preserving exact small sets and expanding
sample capacity for medium and large distinct sets. Unique
node property constraint DDL is implemented through
`CREATE CONSTRAINT ON :Label(property) ASSERT UNIQUE`; descriptors persist
through WAL replay and checkpoint, existing duplicate data rejects constraint
creation, and later `CREATE`, `MERGE`, and `SET` mutations are checked before a
WAL batch is appended. Relationship property uniqueness constraint DDL is
implemented through `CREATE CONSTRAINT ON -[:TYPE(property)]-> ASSERT UNIQUE`
with the same existing-data validation, WAL/checkpoint persistence, and
pre-WAL write validation. Node property existence constraint DDL is implemented
through `CREATE CONSTRAINT ON :Label(property) ASSERT EXISTS` and the equivalent
`ASSERT NOT NULL`; descriptors persist through WAL replay and checkpoint,
existing missing or null values reject constraint creation, and later `CREATE`,
`MERGE`, and `SET` mutations are checked before a WAL batch is appended. Node
property existence constraints use `CREATE CONSTRAINT ON :Label(property)`,
while relationship property existence constraints use
`CREATE CONSTRAINT ON -[:TYPE(property)]->`; both forms support `ASSERT EXISTS`
and `ASSERT NOT NULL`. Node
and relationship table descriptor DDL is implemented
through `CREATE NODE TABLE Name` and `CREATE RELATIONSHIP TABLE Name`; table
descriptors persist through WAL replay and checkpoint, expose a `PUBLIC` schema
state by default, and create the matching label/type token. Descriptor state
transitions are implemented through `ALTER ... SET STATE` for table descriptors
and property descriptors, persist through WAL replay and checkpoint, and expose
`DELETE_ONLY`, `WRITE_ONLY`, `BACKFILL`, `VALIDATING`, `PUBLIC`, and `GC`.
Only `PUBLIC` table and property descriptors participate in write-time and
recovery validation; promoting a property descriptor to `PUBLIC` validates
existing records before the state-change WAL batch is appended. Property-level
table schema DDL is implemented through
`CREATE PROPERTY ON NODE TABLE Name(property) TYPE Type` and
`CREATE PROPERTY ON RELATIONSHIP TABLE Name(property) TYPE Type`, supports
optional `NOT NULL`, persists through WAL replay and checkpoint, validates
existing records before descriptor creation, and checks later writes before a
WAL batch is appended. `Database::run_schema_maintenance` advances
`BACKFILL` descriptors to `VALIDATING`, validates `VALIDATING` descriptors
before advancing them to `PUBLIC`, and removes `GC` descriptors through a single
grouped WAL batch. `Database::plan_schema_maintenance` exposes a read-only
dry-run report with per-object source/target states and estimated operation
counts so caller-owned background loops can decide admission before taking the
writer. `Database::run_bounded_schema_maintenance` can then apply a prefix of
complete descriptor-level maintenance actions that fit the caller's operation
budget, leaving the rest resumable through later maintenance calls. The bounded
background variants bind that same budget to QoS admission and execution for
per-tick internal loops. Composite and full-text property-index execution
projections expose `Database::rebuild_bounded_property_index_projections` for
bounded descriptor-level rebuild reports without making those projections
canonical durability. The same work can be exposed as a rankable `Projection`
background plan or executed through bounded background/scheduled wrappers that
charge QoS admission against the descriptor rebuild budget. Projected graph
derived artifacts can be refreshed through a report-oriented
`Database::rebuild_derived_artifacts` entry point; search projection artifacts
expose the same report-oriented rebuild shape through
`SearchIndex::rebuild_derived_artifacts`. Search projections also expose
bounded incremental deltas for ordinary FTS/BM25 row upsert/delete changes:
`SearchIndex::apply_projection_delta` accepts an operation budget and fails
without partial index mutation on budget or embedding-dimension errors;
`Database::apply_search_projection_delta` exposes the same caller-owned
projection boundary beside graph operations without moving search state into
the graph WAL.
Caller-owned search projections can inject `SearchAnalyzerLexicon` rules for
Nowledge lifecycle and schema vocabulary; normalized alias rules accept readable
phrases or identifiers and keep application vocabulary out of the default graph
kernel lexicon.
Internal background callers can use `SearchIndex::apply_background_projection_delta`
or `Database::apply_background_search_projection_delta` to pass the same delta
through `LocalQosPolicy` admission before applying it; callers that need
in-flight background budget tracking can use
`SearchIndex::apply_scheduled_background_projection_delta` or
`Database::apply_scheduled_background_search_projection_delta` with
`LocalQosScheduler`. Full search rebuilds expose a rankable background plan and
background/scheduled rebuild wrappers so internal loops can charge the scan
estimate before replacing the projection. Graph-derived metadata repair exposes
the same split through `SearchIndex::metadata_repair_background_work_plan`,
`SearchIndex::repair_background_metadata_from_graph`, and
`SearchIndex::repair_scheduled_background_metadata_from_graph` while preserving
its bounded, metadata-only repair semantics.
`Database::background_maintenance_candidates` and
`Database::rank_background_maintenance` provide a caller-owned scheduling
surface that gathers pending schema maintenance, property-index projection
rebuilds, search rebuild/repair work, graph-derived search deltas, and external
content artifact jobs into named `BackgroundWorkPlan`s. The API returns ranked
plans and QoS decisions only; it does not spawn workers or execute background
work on behalf of the embedded application. Executable search-projection graph
delta candidates expose operation counts, upsert/delete counts, optional
max-operation limits, and the complete-through graph commit epoch, so the
application can distinguish bounded incremental FTS maintenance from
freshness-lag planning signals. Background maintenance summaries also expose
typed active-topic, recent-delta, source-graph-lag, query-probability,
staleness, freshness-SLO, and tenant-budget hints, so callers do not need to
parse human-readable ranking reasons. Tenant budget hints below a candidate's
estimated operations are ranked as deferred background work, while foreground
user-triggered rebuilds, deltas, and repairs can still use the direct APIs.
Database-owned
derived artifact jobs expose the same split through
`Database::run_next_background_derived_artifact_job`, which admits internal
background rebuild work through `LocalQosPolicy` while leaving the direct
`run_next_derived_artifact_job` path available for explicit callers. Callers
that want the engine to track in-flight background operation budgets can use
`LocalQosScheduler` with
`Database::run_next_scheduled_background_derived_artifact_job`; this remains
synchronous and caller-driven rather than a built-in thread pool. Schema
maintenance follows the same foreground/background split:
`Database::run_schema_maintenance` remains the explicit, ungated caller path,
while `Database::run_planned_background_schema_maintenance` and
`Database::run_planned_scheduled_background_schema_maintenance` charge the
current dry-run estimate to the `Mutation` background lane before advancing
schema descriptors or appending maintenance WAL. The lower-level
`run_background_schema_maintenance` and
`run_scheduled_background_schema_maintenance` variants remain available when a
caller has its own estimate. `Database::schema_maintenance_background_work_plan`
also lets caller-owned loops rank pending schema maintenance beside projection,
import, analytics, and shadow work before admission. These wrappers preserve the
existing single-batch validation semantics and only add
admission/accounting. Content
artifact jobs are scheduled at the same boundary; callers can attach structured
job payloads for object
references, checksums, parser hints, and projection targets. The default
graph-kernel runner rejects those jobs while preserving the payload in the job
report, and `Database::run_next_external_content_artifact_job_with` lets a
caller-owned content runtime complete parsing/crawling/chunking jobs without
embedding that runtime in Skein. Successful external content jobs retain the
runtime's last structured `QueryOutput` on the job ledger so callers can audit
published projection refs, parser versions, checksums, chunk counts, and other
small lineage fields without storing large parsed content in the graph kernel.
Caller-owned runtimes can read those successful lineage rows through bounded
`Database::succeeded_external_content_artifact_jobs` or action-scoped
`Database::succeeded_external_content_artifact_jobs_for_action` views instead
of scanning the full derived-artifact history.
`ExternalContentArtifactJobCompletion` provides a standard lightweight
completion manifest for caller-owned parser/crawler runtimes that want to report
runtime identity, input/output refs, checksums, projection refs, source graph
epoch, produced-row counts, and small metadata without storing parser payloads
in the graph kernel. `Database::complete_next_external_content_artifact_job_with`
and `Database::complete_external_content_artifact_job_with` convert that
manifest into the retained audit row. The matching background and scheduled
completion runners preserve the same row shape while charging parser/crawler
work to the `Import` QoS lane.
`ExternalContentArtifactRuntimeManifest` lets caller-owned parser/crawler loops
declare supported actions, required payload keys, runtime version, and estimated
operation cost so Skein can expose bounded claimable-job views and a matching
Import-lane background work plan without treating the manifest as a sandbox or
execution permission.
Internal parser/crawler loops can use
`Database::run_next_background_external_content_artifact_job_with` or
`Database::run_next_scheduled_background_external_content_artifact_job_with` to
charge that work to the `Import` background lane while leaving explicit runtime
calls ungated. Action-specific background loops can use
`Database::run_next_background_external_content_artifact_job_for_action_with` or
`Database::run_next_scheduled_background_external_content_artifact_job_for_action_with`
to charge only their own pending work to the same `Import` lane. Before claiming
work, caller-owned loops can expose pending parser/crawler work through
`Database::external_content_artifact_job_background_work_plan` or
`Database::external_content_artifact_job_background_work_plan_for_action`, then
rank it beside projection, schema maintenance, analytics, and shadow work with
the normal `LocalQosPolicy` and `LocalQosScheduler` surfaces. If the runtime
first polls a bounded pending list and chooses a specific job,
`Database::run_background_external_content_artifact_job_with` and
`Database::run_scheduled_background_external_content_artifact_job_with` apply
the same Import-lane admission to that concrete job. Action-specific runtimes
can also poll failed jobs for their own action through
`Database::failed_external_content_artifact_jobs_for_action` and requeue only
their own failed work through
`Database::retry_failed_external_content_artifact_job_for_action`. They can also
read `Database::external_content_artifact_job_summary_for_action` before
claiming work, so resource-constrained parser/crawler loops can account for
only their own pending and failed queue pressure.

An internal compatibility fixture harness is implemented for Nowledge-shaped
query families. It runs setup statements, parameterized Cypher checks, expected
row comparisons, plan-shape assertions, and projected graph checks through the
public `Database` facade. The fixture harness also exposes a generic shadow
engine interface that applies the same setup statements to a second engine,
compares Cypher check rows against the primary Skein run, compares declared
error classes for failing Cypher checks, compares mutation effects through
follow-up effect queries, and can compare projected graph outputs from shadow
engines that implement the projection hook. Engines without that hook still
report projected graph checks as primary-only. `ExternalShadowCommand` provides
a JSON-lines process adapter for this interface, so a Ladybug/Kuzu wrapper can
be attached as a subprocess without linking Kuzu or Python into Skein.
The current fixture covers indexed parameter lookup, null predicates, list
predicates with pagination, entity alias list lookup, entity reuse lookup reads,
entity temporal metadata create/update writes, current timestamp writes,
entity `MENTIONS` and temporal `RELATES_TO` creation writes, entity count reads,
label resolver null-canonical scans/backfills, label rename collision guards,
label existence reads, label remove-all count/delete writes,
source provenance Source endpoint checks, full `SOURCED_FROM` creation writes,
edge-existence/global counts, exact repair candidate scans, source memory-count
reads,
PageRank membership/visibility reads, score persist/clear writes, central-entity
lookup, GraphMeta clear stamps, planner node/relationship totals, and changed
count reads,
community scheduler GraphMeta, candidate scan, member entity, and summary
write queries,
cleanup scheduler seed scans, bounded EVOLVES pair reads, cleanup fingerprint
row fetches, floor-zero engagement `CASE` ordering, and decay scheduler
EVOLVES/crystal synthesis count reads,
wiki export summary entity/crystal/community count reads, topic entity/crystal
ranking reads, entity listing mention-count and cursor reads, community entity anchor
visibility reads, entity id-or-name lookup and community context reads,
community crystal source visibility reads, and community top memory ranking reads,
OKF export community list, entity list, crystal list, and crystal-source entity
community reads, shared OKF/wiki entity mention detail reads, and shared OKF/wiki
related entity reads, plus OKF memory row exports with row-preserving label
collection and OKF label row exports,
schema migration and label node `MERGE ON CREATE SET ... ON MATCH SET`,
GraphMeta `MERGE SET`, matched HAS_LABEL relationship `MERGE ON CREATE SET`,
label canonical lookup, dynamic label updates, label-edge removal, and label
node `DETACH DELETE`,
source-node `DETACH DELETE` cascade checks,
source metadata `timestamp(...)` update writes,
source attribution reads,
source fan-in relationship count reads,
thread compaction attribution reads,
memory created-at bulk reads, memory bulk metadata and space reads,
entity relationship endpoint checks, count-only optional thread cleanup reads,
thread message target `DETACH DELETE` cleanup writes,
top-entities-by-degree graph analysis reads,
node-detail neighbor count reads,
label usage count reads including direct optional source-projection counts,
agent context activity digest reads,
agent context activity task reads,
agent context stale crystal and EVOLVES cluster reads,
health stale memory count reads,
entity lifecycle impact counts, detail projections, relation/label/community
preview reads, and entity-node `DETACH DELETE` cascade checks,
graph orphan entity reads and cleanup-candidate reads with one-hop relationship
existence predicates,
AugmentationJob lifecycle create/running/progress/completed/failed writes and
status/list reads,
undo-community detection writes for deleting Community nodes, clearing
`community_id`, and resetting GraphMeta community freshness,
CRYSTALLIZED_FROM-to-SYNTHESIZED_FROM relationship-copy migration writes,
thread bulk-move normalized-space selection reads and update-return writes,
thread distillation optional source filters,
feed synthesized-source id collection reads,
skill synthesized memory id reads,
skill stage projection, active, and builder list reads,
skill detail reads,
skill synthesized memory direct-id and detail reads,
skill metadata and version reads, metadata writes, and usage-stat writes,
learning memory latest reads,
source parsed path list reads,
community summarized list reads,
community memory type-filter reads,
community entity memory-count reads,
entity mention-count list reads,
community memory coalesced summary reads,
source detail memory-count reads,
source provenance memory detail reads,
source memory id list reads,
memory source provenance id reads, memory metadata update writes,
source memory-count decrement-floor writes,
source relationship source-reference count reads,
source relationship source-reference deletes,
memory compact detail fallback reads,
memory label name list reads,
memory label bulk name reads,
memory label fallback bulk reads,
memory label endpoint bulk reads,
memory label distinct name count reads,
source label bulk name reads,
source label endpoint bulk reads,
source label relationship count reads,
source label relationship merge writes,
source label relationship delete writes,
source detail normalized-space reads,
source detail chunk-count reads,
source detail file-path reads,
source default metadata fallback reads,
source list fallback page reads,
source overview memory-count ranking reads,
source bulk summary fallback reads,
source count reads,
source extracted id list reads,
source extracted lifecycle mark-indexed writes,
source lifecycle indexed chunk-count writes,
source lifecycle state update writes,
source space update writes,
source bulk normalized-space move writes,
source normalized-space id list reads,
memory normalized-space id list reads,
memory bulk normalized-space move writes,
memory normalized-space limit-one reads,
memory candidate normalized-space id reads,
memory candidate normalized-space exclusion reads,
memory normalized-space count reads,
memory normalized-space limited id reads,
memory id-list normalized-space move-returning writes,
memory id-list normalized-space exclusion move-returning writes,
thread normalized-space id pair reads,
thread bulk node-id normalized-space move writes,
thread identity normalized-space id reads, thread metadata reads and update writes,
thread identity bulk normalized-space move writes,
thread normalized-space logical id reads,
memory entity name list reads,
memory entity endpoint bulk reads,
memory review-status bulk reads,
memory ranked overview reads,
thread message ordered reads,
thread compacted-memory count reads,
thread compacted-memory summary reads,
source coverage aggregate-filter reads,
timestamp and `CAST(... AS TIMESTAMP)` cutoff predicates,
relationship min aggregation, scheduler fingerprint max aggregation, node property pattern
reads, anonymous-endpoint relationship counts, one-hop undirected relationship reads, grouped and distinct
aggregation, one-hop relationship property reads through relationship pattern
property filters, source-provenance endpoint existence checks and relationship
creation between already matched endpoint nodes, source revision history bounded
path reads, source fan-in relationship count reads, source metadata
`timestamp(...)` update writes, label canonical
lookup, dynamic label updates, label-edge removal, label node `DETACH DELETE`,
thread message target `DETACH DELETE` cleanup writes,
endpoint-equality EVOLVES relationship creation, source-node `DETACH DELETE`
cascade checks, untyped overview-edge relationship type projection,
Nowledge-style projection fallback expressions and fallback ordering,
relationship property mutation effects with relationship-variable `WHERE`
filters, and relationship-type filtered projected graph construction.
It also covers production-shaped projected graph definition, PageRank, and
hierarchical Louvain procedure calls, with
fixture-declared floating-point tolerances for shadow row comparison and
projected graph PageRank parity. A cutover assessment gate now consumes shadow
reports and returns `Ready` only when the configured minimum matched-check count
is met and, by default, every check is covered by the shadow engine. The
remaining compatibility work is to implement the nowledge/mem Ladybug/Kuzu
wrapper process for that adapter and run the production fixture set through it.

### Phase 4: Costed Cascades Search

Scope:

- split the flat MVP into stable API, parser, catalog, planner, optimizer,
  storage, search, compatibility, and executor boundaries; use internal module
  splits first when public contracts are still moving, and promote boundaries to
  workspace packages only when the dependency direction is acyclic and stable
- Chryso-style rule and cost interfaces
- persistent statistics and index descriptors
- pattern join ordering and scan/seek/expand costing
- optimizer budget, trace, and plan fingerprint regression tests

Exit gate:

- plan selection changes only with an explainable statistics or rule change
- optimizer-only benchmarks catch search-space regressions
- plan snapshots are deterministic across runs

Current implemented slice:

- persistent equality index descriptors for observed label/property pairs
- checkpointed graph statistics for total nodes, total relationships,
  per-label counts, per-relationship-type counts, relationship-type source
  and target counts, label/type/label one-hop path cardinalities, bounded exact
  multi-hop path cardinalities, per-label/property distinct-value counts, and
  per-relationship-type/property distinct-value counts plus relationship
  property histograms
- statistics freshness metadata with the commit epoch used to compute the
  snapshot, plus histogram sample-limit and node/relationship per-histogram
  sampled/exact markers
- public facade access to index descriptors and statistics for compatibility
  checks and future optimizer costing
- metadata-aware scan/seek costing for simple label plus equality predicates:
  missing descriptors keep `SeqNodeScan + Filter`, selective predicates choose
  `IndexNodeSeek`, and low-selectivity predicates can keep the scan path with an
  explainable optimizer trace decision; indexed property-list predicates can
  choose `IndexNodeMultiSeek` for Nowledge feed and source coverage shapes such
  as `WHERE c.id IN $ids`, including cost-based selection among equality and
  list predicates inside the same conjunction
- metadata-aware expand cardinality estimates for bounded outgoing patterns:
  the optimizer consumes label/type/label path counts and relationship fanout
  summaries, applies relationship-property distinct counts for property pattern
  filters and one-hop relationship-variable equality predicates pushed down
  from `WHERE`, and uses relationship-property histograms for range filters
  over relationship variables, then records per-hop exact/fallback row
  estimates and total estimated rows in the explain trace while keeping the current
  `AdjacencyExpandExec` implementation stable
- `OptimizerTrace::selected_plan_cost` exposes recursive output-row and cost
  estimates for the chosen physical plan; expand rows are scaled by the
  selected input cardinality, so selective seek inputs no longer make the trace
  report full-label expand cost; endpoint cartesian products report estimated
  left/right rows, output rows, and product cost
- `OptimizerTrace::selected_plan_operator_counts` and
  `OptimizerTrace::selected_plan_class_counts` expose stable selected-plan
  histograms derived from physical plan metadata, so diagnostics do not need to
  parse English explain text to detect operator mix or schema/mutation/access/
  traversal/relational/procedure composition
- `skein explain-json [--params-json <json-object>] <database-path> <cypher>`
  opens the database read-only and prints the selected plan, fingerprint,
  recursive cost, typed parameter echo, warnings, decisions, and structured
  operator/class histograms as stable JSON for migration gates and CI artifacts
- endpoint cartesian products whose flattened inputs all estimate to one row
  choose a stable left-deep physical input order by child cost and fingerprint,
  covering Nowledge endpoint-existence checks without changing broader
  unordered multi-row product semantics
- residual node-property filters use the selected physical plan to recover the
  filtered node variable's label and apply node-property distinct counts or
  histograms for equality, `IN`, and range predicates, so low-selectivity scan
  fallbacks and cross-pattern filters are not forced through the generic
  half-selectivity fallback; residual read-side `id(variable)` filters use
  one-row equality, input-minus-one inequality, and literal-list width estimates
  once the selected physical plan proves the variable is a node or relationship
- grouped aggregate cost estimation uses selected-plan variable labels/types
  and explicit node-property or relationship-property distinct counts for
  simple property group keys, and adds bounded work cost for distinct property
  aggregate targets such as `COUNT(DISTINCT e2.community_id)` plus distinct
  variable targets such as `COUNT(DISTINCT m)`, while non-property or
  missing-statistics grouping keeps the conservative fallback estimate
- optional degree and relationship count-sum costing use source label/property
  distinct counts plus relationship type/source counts for outgoing legs and
  relationship type/target counts for incoming legs in Nowledge cleanup,
  extracted-reference, and mention-count paths instead of a fixed leg-count
  constant
- deterministic physical plan fingerprints are exposed through
  `OptimizerTrace::selected_plan_fingerprint` and `PhysicalPlan::fingerprint`
  for regression tests and future compatibility/shadow comparisons
- optimizer memo search has a hard group budget: when the logical group demand
  exceeds `OptimizerConfig::max_groups`, the optimizer skips memo construction,
  emits an explain warning, and uses a deterministic direct physical fallback
  that preserves current scan/seek and expand diagnostics
- `cargo bench --bench optimizer_smoke` covers optimizer-only range-seek plus
  bounded expand stats, composite seek, text seek, low-selectivity scan
  fallback, production-shaped selective seed + bounded expand + aggregate +
  sort/limit, one-hop relationship-property expand reads, a Nowledge-shaped
  pushed-down relationship equality plus relationship range-filter workload, a
  source-to-memory-to-label cross-pattern aggregate workload, a larger
  source-to-memory-to-entity-to-label grouped workload,
  community-to-synthesized-source coverage aggregate workload plus the
  aggregate-alias coverage filter shape used by source coverage checks,
  feed synthesized-source collection reads backed by indexed `IN` seeks,
  entity bridge-span distinct-property aggregate workload,
  thread-cleanup optional relationship count-sum workload with seed/fanout cost
  tracing, incoming mention optional relationship count-sum and optional degree
  workloads backed by relationship target statistics,
  source-attributed entity community export aggregation with memory unit-type
  filtering and distinct memory/entity counts,
  endpoint-existence and nested endpoint-existence cartesian product cost
  tracing and single-row input ordering, residual node-property filter
  equality/inequality/`IN`/range/null selectivity, residual relationship-property
  inequality/`IN`/null selectivity, residual read-side relationship-id filter
  selectivity, residual string predicate selectivity without double-counting
  full-text index candidates, constant/`OR` predicate selectivity for optional
  parameter filters, grouped node-property aggregate cardinality, selected-plan
  cost stability, deterministic fingerprints, budget-fallback paths, one-hop
  and bounded multi-hop path source/target coverage distinct statistics for
  distinct variable aggregate costing without depending on a storage fixture,
  plus optimizer smoke coverage for bounded multi-hop distinct-target
  aggregates

Remaining Phase 4 work:

- richer cross-pattern statistics beyond residual filters, grouped
  node-property aggregates, and bounded path coverage distinct counts
- alternative expand implementation candidates and pattern join-order
  enumeration once multi-pattern logical plans exist
- bounded left-deep join-order enumeration beyond the current all-single-row
  endpoint-product ordering
- broader cross-pattern workload-shaped optimizer benchmark suites beyond the
  current source/memory/entity/community/label smoke cases

### Phase 5: Analytics and Cutover

Scope:

- immutable CSR/CSC projected graph snapshots
- PageRank and Louvain-compatible entry points
- background projection rebuild and versioning
- production shadow reads against Ladybug
- export, rollback, and cutover tooling

Exit gate:

- algorithm outputs meet defined parity tolerances
- shadow reads show no semantic drift on production-shaped fixtures
- rollback can reopen the previous Ladybug database without converting it in
  place

Current implemented slice:

- immutable in-memory CSR/CSC projected graph snapshots derived from the
  canonical graph store or a read-transaction snapshot
- optional node-label and relationship-type filtering for projected graph
  construction
- WAL/checkpoint-persisted projected graph definitions; algorithm calls rebuild
  CSR/CSC snapshots from the canonical graph state at execution time
- checkpoint-generated projected graph artifacts with node IDs, CSR outgoing
  adjacency, CSC incoming adjacency, atomic replacement, and checksum
  validation/discard on recovery
- projected graph artifact format versioning, projection epochs, and public
  status reporting for reusable/stale artifact state
- execution-path reuse of cached projected graph artifacts when commit epoch and
  projected graph definition still match the active store
- explicit background rebuild API that refreshes projected graph artifacts
  without appending WAL, truncating WAL, or publishing a checkpoint manifest
- report-oriented derived artifact rebuild API for projected graph artifacts
- embedded derived-artifact job queue for projected graph rebuilds with
  pending/running/succeeded/failed status, attempt counts, and last-error
  reporting; the queue is intentionally synchronous and caller-driven for the
  embedded engine
- Kuzu-style Cypher procedure entry points:
  `CALL project_graph('Graph', ['Label'], ['TYPE'])`,
  `CALL page_rank('Graph', dampingFactor := 0.85, maxIterations := 20)
  RETURN node, pagerank_score`, and
  `CALL louvain('Graph') RETURN node, louvain_id`
- hierarchical Louvain execution with `maxLevels := N` and optional
  `RETURN node, level, louvain_id`
- PageRank scoring over projected snapshots with dangling-node redistribution
- reverse traversal over incoming CSC sources for analytics and compatibility
  checks
- deterministic Louvain-compatible community assignment over projected snapshots
- fixture-declared floating-point parity tolerances for PageRank rows and
  projected graph shadow outputs
- production-shaped compatibility fixtures for `project_graph`, `page_rank`,
  and hierarchical `louvain` procedure execution
- compatibility cutover gate that converts a shadow report into `Ready` or
  `Blocked` with explicit primary-only coverage blockers
- caller-owned rollback evidence fields in the migration gate so release
  automation can require proof that the previous local graph database can still
  be reopened without making the graph kernel open that database

Remaining Phase 5 work:

- richer caller-owned blob/content parser runtime integration outside the graph
  kernel
- optional `ExternalShadowCommand` wiring to the previous local graph wrapper
  when a specific migration gate needs compatibility evidence
- rollback execution tooling owned by the migration/release layer

## First Implemented Compatibility Slice: Parameters

Skein now represents a parsed value as either a literal or a named parameter.
The planner binds parameters into typed values before creating a logical plan.
The optimizer, executor, and store therefore never handle unresolved parameter
tokens.

The embedded API provides parameterized forms for query, explain, and mutation
transactions. Missing parameters produce a semantic error before any mutation
is appended to the WAL. Extra parameters are tolerated, parameter names are
case-sensitive, and labels or property identifiers cannot be parameterized.

This slice was selected before MVCC because the current Nowledge wrapper relies
heavily on parameterized Cypher. It also proves the intended parser/semantic
boundary without committing to the later storage concurrency design.

## Validation Commands

```bash
cargo fmt --all -- --check
cargo test
cargo clippy --all-targets -- -D warnings
```

The next implementation should build the dual-engine compatibility harness,
then use its first failing production fixture to choose between MVCC work and
additional Cypher coverage. This keeps development driven by the real
replacement boundary rather than by broad openCypher completeness.
