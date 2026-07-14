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

## Required Capability Surface

### Embedded API

- open or create by path
- storage format/version inspection
- read-only and recovery configuration
- parameterized query and explain
- explicit read and write transactions
- commit, rollback, and checkpoint
- bounded resource configuration

### Cypher and Semantic Analysis

The initial production subset is derived from real Nowledge queries:

- `MATCH`, one-hop and bounded multi-hop patterns
- `WHERE` equality, boolean predicates, null checks, and list membership
- `RETURN`, aliases, aggregation, ordering, offset, and limit
- `CREATE`, `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`
- parameters for property values, predicates, pagination, and list filters
- schema DDL and migration statements used by schema convergence
- projected graph lifecycle and graph algorithm procedure calls

Labels, relationship types, and property names remain identifiers rather than
runtime parameters. This keeps catalog resolution deterministic and prevents a
single prepared query from changing its schema dependencies.

### Storage and Recovery

- versioned catalog tokens and index descriptors
- canonical node, relationship, and property records
- outgoing and incoming adjacency indexes
- equality indexes first, followed by range indexes where real queries require
  them
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

Status: parameter binding is implemented. The typed adapter and fixture harness
remain.

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

### Phase 4: Costed Cascades Search

Scope:

- split the flat MVP into stable API, parser, catalog, planner, optimizer,
  storage, and executor crates
- Chryso-style rule and cost interfaces
- persistent statistics and index descriptors
- pattern join ordering and scan/seek/expand costing
- optimizer budget, trace, and plan fingerprint regression tests

Exit gate:

- plan selection changes only with an explainable statistics or rule change
- optimizer-only benchmarks catch search-space regressions
- plan snapshots are deterministic across runs

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
