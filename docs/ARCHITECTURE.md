# Skein Architecture

## Goal

Skein is an embedded Rust graph database for Nowledge local runtimes. It is
intended to replace the current Ladybug/Kuzu dependency while preserving the
Cypher-facing behavior that Nowledge relies on today.

The first product target is not a general Neo4j clone. The target is the
Nowledge graph data plane:

- embedded database open/create by path
- schema DDL for node labels and relationship types
- Cypher reads and parameterized queries
- transactional graph mutations
- crash-safe local persistence
- deterministic query planning and explain output
- graph projection hooks for rebuildable analytics

Cloud remains PostgreSQL-first. Skein is the local embedded graph engine and can
share logical semantics with Cloud projections, but Cloud canonical state should
continue to live in PostgreSQL facts, edges, jobs, and op-log tables.

## Non-Goals

- Replacing PostgreSQL in Nowledge Cloud.
- Implementing the entire openCypher surface in the first milestone.
- Storing vector embeddings inside the graph engine.
- Providing a distributed graph database.
- Treating graph analytics as canonical state.

## Compatibility Boundary

Nowledge currently uses the Ladybug fork through the local graph wrapper. Skein
must cover the used surface before it can replace that dependency:

- database lifecycle with configurable memory, thread, size, read-only, and
  recovery behavior
- Cypher `MATCH`, `RETURN`, `WHERE`, `ORDER BY`, `LIMIT`, and parameter binding
- DDL for node and relationship tables
- `CREATE`, `MERGE`, `MATCH SET`, `DELETE`, and `DETACH DELETE`
- ACID transactions with explicit begin, commit, and rollback
- checkpoint and WAL recovery
- storage compatibility/version checks
- projected graph lifecycle and basic algorithm entry points

Arrow integration is not part of the replacement boundary. Nowledge stores graph
identity and scalar properties in the graph database, while vector search remains
outside the graph engine.

## Crate Layout

```text
crates/
  skein-core/          errors, values, ids, common data model
  skein-cypher/        lexer, parser, AST, parameter model
  skein-catalog/       labels, relationship types, property schema, stats
  skein-planner/       semantic analysis, logical plan, physical plan
  skein-optimizer/     Cascades memo, rules, costing, properties, trace
  skein-storage/       embedded persistence, WAL, MVCC, indexes
  skein-executor/      physical operators and query execution
  skein-api/           stable embedded API facade
```

The public API should live in `skein-api`. Internal crates should be allowed to
evolve while the embedded API stays small and stable.

## Data Model

Skein stores a property graph:

- `NodeId`: stable internal node identity
- `RelId`: stable internal relationship identity
- `LabelId`: catalog identity for node labels
- `RelTypeId`: catalog identity for relationship types
- `Value`: null, bool, integer, float, string, bytes, list, map, temporal values
- `NodeRecord`: node id, label set, property map
- `RelRecord`: relationship id, source id, target id, type id, property map

Nowledge usage should prefer explicit stable external ids as properties. Internal
ids are storage identities and should not be exposed as durable cross-version
references.

## Storage Design

The initial storage engine should optimize for correctness and embeddability:

- append-only WAL for transactional durability
- immutable or copy-on-write pages for crash recovery
- column families or logical trees for nodes, relationships, properties, and
  indexes
- adjacency indexes by `(source, type, target)` and `(target, type, source)`
- property indexes for high-selectivity equality and range filters
- catalog metadata versioned independently from data pages

The storage API should be iterator-oriented. The executor should be able to
compose scans, expands, filters, and joins without materializing full graphs.

## Cypher Pipeline

```text
Cypher text
  -> AST
  -> Semantic graph query model
  -> Logical plan
  -> Cascades optimizer
  -> Physical plan
  -> Executor
  -> Rows
```

The semantic graph query model is a separate layer between AST and logical plan.
It resolves labels, relationship types, variable scopes, property references,
and cardinality constraints. This keeps parser syntax compatibility separate
from planning semantics.

## Logical Plan

Core logical operators:

- `NodeScan`
- `NodeIndexSeek`
- `Expand`
- `ExpandInto`
- `RelScan`
- `Filter`
- `Project`
- `Join`
- `AntiJoin`
- `Optional`
- `Aggregate`
- `Sort`
- `Limit`
- `CreateNode`
- `CreateRel`
- `Merge`
- `SetProperty`
- `Delete`
- `DetachDelete`

Graph patterns should first lower into pattern fragments. The planner can then
enumerate pattern join orders instead of committing too early to query text
order.

## Physical Plan

Core physical operators:

- `SeqNodeScan`
- `IndexNodeSeek`
- `SeqRelScan`
- `AdjacencyExpand`
- `ExpandIntoCheck`
- `HashJoin`
- `NestedLoopApply`
- `FilterExec`
- `ProjectExec`
- `SortExec`
- `LimitExec`
- `MutationExec`

For the Nowledge replacement target, adjacency expansion and selective property
index seeks matter more than full relational join sophistication.

## Cascades Optimizer

Skein should use a Cascades model similar to Chryso:

- `Memo`: stores equivalent plan alternatives.
- `Group`: represents a logical equivalence class.
- `GroupExpr`: stores an operator plus child group references.
- `Rule`: transforms logical expressions into equivalent logical alternatives.
- `ImplementationRule`: maps logical expressions to physical alternatives.
- `CostModel`: scores physical alternatives using graph statistics.
- `PhysicalProperties`: required and delivered ordering, distinctness, and
  binding properties.
- `OptimizerTrace`: deterministic diagnostics for rules, groups, candidates,
  costs, warnings, and search limits.

Unlike Chryso, Skein needs graph-specific properties:

- bound variables
- preserved path uniqueness mode
- node/relationship identity uniqueness
- ordering
- expected cardinality
- required adjacency direction
- required index coverage

## Rule Families

Logical rewrite rules:

- push predicates into node and relationship scans
- convert property predicates to index seek candidates
- reorder pattern expansions by estimated selectivity
- merge adjacent projections
- remove redundant filters and projections
- normalize commutative predicates
- split conjunctive predicates
- lower `MERGE` into match-or-create where legal

Implementation rules:

- `NodeScan` to `SeqNodeScan`
- `NodeScan + property predicate` to `IndexNodeSeek`
- `Expand` to `AdjacencyExpand`
- selective pattern fragment to `HashJoin`
- correlated pattern fragment to `NestedLoopApply`
- mutation logical nodes to `MutationExec`

The optimizer must have explicit search budgets and deterministic tie-breaking.
Local-first tooling depends on stable plan output for tests and debugging.

## Statistics

The catalog should track:

- node count per label
- relationship count per type and direction
- property null fraction
- property distinct count
- optional histogram or top-k values for indexed properties
- degree distribution summaries per label/type pair

The first cost model can be simple, but it must be structured enough to improve
without changing optimizer APIs.

## Transactions

Transactions should expose:

- read transaction
- write transaction
- commit
- rollback
- checkpoint

The initial concurrency model can be single-writer/multi-reader. This matches
the embedded local runtime shape and is safer than prematurely designing a
high-concurrency server engine.

## Nowledge Integration

The replacement should preserve the current local wrapper shape:

- one embedded database object per path
- shared read access through guarded connections or snapshots
- exclusive writes, control operations, and checkpoints
- explicit storage version check at boot
- recovery path for WAL/lock sidecars
- projected graph operations as rebuildable outputs, not canonical data

Cloud integration should not embed Skein as canonical storage. Cloud can reuse
Cypher parsing, logical planning, and graph projection semantics if useful, but
the execution backend remains PostgreSQL-backed facts and edges.

Search integration should also preserve the current Nowledge boundary: semantic
and full-text search are rebuildable projections, not source-of-truth graph
state. Skein therefore keeps `SearchIndex` separate from `GraphStore`. This
allows the graph store to replace Kuzu/Ladybug while the search projection
replaces LanceDB without coupling vector lifecycle state to canonical graph
durability.

Full search rebuilds must be bounded and all-or-nothing. A rebuild first derives
typed projection rows from canonical graph nodes into a temporary map, then
replaces the in-memory projection only after the configured row bound is not
exceeded. Successful rebuilds clear projection lifecycle markers; failed rebuilds
leave the previous projection intact and keep a full-reindex marker.

Metadata-only repairs use the same graph-derived projection row mapping but only
replace document metadata for already-present projection rows. They preserve
existing titles, text content, and embeddings. If repair discovers missing rows,
it marks full reindex as needed because metadata repair cannot create the absent
search documents without becoming a rebuild.

## Milestones

1. Parser and AST for the Cypher subset used by Nowledge.
2. In-memory graph store with transactions for semantic and planner tests.
3. Logical plan builder for `MATCH`, `WHERE`, `RETURN`, `CREATE`, `MERGE`, and
   `DELETE`.
4. Cascades memo, logical rules, implementation rules, cost model, and explain
   traces.
5. Persistent storage with WAL, checkpoint, catalog versioning, and recovery
   tests.
6. Compatibility wrapper matching the current Nowledge local graph API.
7. Projection and analytics hooks for PageRank/Louvain-compatible workflows.

## Validation

Required test suites:

- parser golden tests for the supported Cypher subset
- semantic scope and binding tests
- logical plan snapshot tests
- optimizer rule and plan-shape tests
- deterministic optimizer trace tests
- transaction commit/rollback tests
- WAL recovery and checkpoint tests
- compatibility tests against the current Nowledge Ladybug-backed wrapper

Before replacing Ladybug in Nowledge, the compatibility suite should run both
engines against the same fixtures and compare rows, mutation effects, and error
classes.
