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
- projected graph lifecycle and Kuzu-style algorithm procedure entry points
  for `project_graph`, `page_rank`, and `louvain`

Arrow integration is not part of the replacement boundary. Nowledge stores graph
identity and scalar properties in the graph database, while vector search remains
outside the graph engine.

## Crate Layout

Skein follows Chryso's workspace-and-facade layout. The root crate remains the
stable embedded facade, while implementation crates are split out as the
interfaces harden. The current crate split starts with `skein-core`; parser,
planner, optimizer, storage, executor, search, and API modules remain in the
root crate until their contracts are ready to freeze.

```text
crates/
  core/                errors, values, ids, catalog names, schema descriptors
  cypher/              token cursor, parser, AST, parameter model
  catalog/             labels, relationship types, property schema, stats
  planner/             semantic analysis, logical plan, physical plan
  optimizer/           Cascades memo, rules, costing, properties, trace
  storage/             embedded persistence, WAL, MVCC, indexes
  executor/            physical operators and query execution
  api/                 stable embedded API facade
```

The public facade should stay in the root `skein` crate. Internal crates should
be allowed to evolve while the embedded API stays small and stable.

Inside the root crate, larger subsystems should still be split by ownership. The
current Cypher module uses:

```text
src/cypher.rs          public facade and re-exports
src/cypher/ast.rs      syntax-only statement and expression types
src/cypher/parser.rs   cursor-based parser implementation
src/cypher/tests.rs    parser coverage for the supported subset
```

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
- property indexes for high-selectivity equality, composite equality, range,
  and text filters
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

The Cypher parser should stay systematic as the supported subset grows. The AST
types define syntax data only; parser entry points dispatch by top-level
statement family; reusable cursor helpers own keyword matching, token
expectations, delimiter handling, whitespace movement, and end-of-input checks.
Statement parsers should compose those helpers rather than open-coding byte
movement or separator loops. This keeps syntax changes reviewable and avoids
leaking semantic validation into parsing.

The parser technology choice is deliberately conservative. Skein should not add
a yacc-style generated grammar for the current Nowledge replacement slice. The
supported Cypher surface is production-query-driven, narrow, and tied to
planner/executor semantics that are still changing. A generated grammar would
make it easier to accept syntax that the semantic graph model cannot execute,
and would add another build-time boundary before the subset has stabilized.

The preferred direction is closer to RisingWave's newer parser organization:
keep the top-level statement flow explicit in Rust, keep token/cursor ownership
separate from AST construction, and use small parser helpers or combinators only
where they reduce local ambiguity for expressions, lists, and delimited forms.
Skein can adopt a real lexer or parser-combinator layer later, but only after a
Nowledge scanner hit proves that the current cursor helpers are becoming the
main source of complexity.

This still preserves the useful `parser_yacc` practice from Chryso: grammar
recognition, AST construction, and semantic validation remain separate
concerns. The current hand-written parser should keep that boundary while
avoiding a generated grammar until the Cypher subset is large and stable enough
to justify it.

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
- `IndexNodeCompositeSeek`
- `IndexNodeTextSeek`
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
- `CostModel`: scores physical alternatives using graph statistics. The current
  slice applies this to scan-vs-index-seek choices and records a recursive
  selected-plan row/cost summary that includes bounded expand estimates.
- `PhysicalProperties`: required and delivered ordering, distinctness, and
  binding properties.
- `OptimizerTrace`: deterministic diagnostics for rules, groups, candidates,
  costs, warnings, and search limits. If a logical plan exceeds
  `OptimizerConfig::max_groups`, Skein does not build an oversized memo; it
  records a budget warning and selects a deterministic direct physical fallback.

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
- per-hop exact/fallback path cardinality estimates for bounded expands
- selected physical plan row and cost estimates for optimizer diagnostics

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

`NowledgeGraphAdapter` is the typed front door for this local wrapper shape. It
accepts `NowledgeGraphStatement` values containing Cypher text plus typed
parameters, and exposes query, explain, and grouped mutation transaction
execution through the same planner and storage paths as `Database`. It also
forwards the Knowledge Retrieval facade over a caller-owned `SearchIndex`, plus
typed knowledge navigation APIs for entity lookup, bounded neighbors, bounded
paths, and bounded subgraph expansion, including traversal diagnostics. This
keeps the compatibility boundary parameterized and reviewable without adding an
ACL layer to the embedded built-in core, while preserving the rule that search
projections stay outside canonical graph state.

Migration gates use a machine-readable query inventory. `scan-nowledge-inventory`
walks Nowledge Rust source files, extracts conservative Cypher string-literal
call sites, classifies them as read, mutation, schema, procedure, or transaction
control, and emits the audited `required_checks` JSON artifact. The lower-level
JSON importer also accepts scanner-shaped `name` plus `call_sites` objects; each
call site has `name`, `query_family`, `source`, and optional `cypher`. Skein
validates this through `build_compatibility_query_inventory_from_json`, rejects
duplicate check names, and can export the audited artifact through
`compatibility_query_inventory_to_json` for CI reuse. Coverage and shadow gates
then compare that inventory against the public compatibility fixture instead of
relying on an informal checklist. `scan-nowledge-cypher-coverage` scans the same
source tree and reports fixture coverage by normalized Cypher text, which lets
scanner-generated `file:line:hash` call-site names map to existing semantic
fixture names without duplicating fixtures. `scan-nowledge-cypher-coverage-detail`
adds `covered_items` and `missing_items` with source, query family, and Cypher
text so fixture gaps can be closed from real Nowledge call sites. The scanner
keeps Cypher map literals but skips unresolved Rust format templates such as
`{space_clause}` because they are not executable query text until the caller
selects a concrete shape. Gate reports
can be exported as JSON through the coverage, inventory gate, shadow cutover,
and migration gate report helpers; their `decision` fields are lowercase
`ready` or `blocked` strings so CI does not need to parse Rust debug output.
`assess_compatibility_migration_gate_bundle`
packages the four reports into one result, and
`compatibility_migration_gate_bundle_to_json` preserves the same structure for
artifact upload. The `nowledge-cypher-migration-gate [--require-ready]
[--allow-self-shadow] [--shadow-ready] [--shadow-trace <path>]
[--shadow-timeout-ms <ms>] <root> <shadow-name> <program> [args...]` CLI command
scans a Nowledge source tree,
runs the public Nowledge core fixture through `ExternalShadowCommand`, uses
normalized Cypher coverage so scanner-generated `file:line:hash` names do not
have to match semantic fixture names, and prints the same migration-gate bundle
JSON. With `--require-ready`, the command exits with an error when the migration
gate decision is blocked, making it suitable as a CI cutover gate.
`skein-shadow-self` is a JSON-lines self-shadow process for protocol and CLI
smoke testing; it exercises the process boundary but does not replace the
required previous-wrapper parity run. The process protocol is specified in
`docs/EXTERNAL_SHADOW_PROTOCOL.md` so previous-wrapper adapters can be
implemented without depending on internal fixture code.

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
Persistent search projection snapshots publish through a synced temporary file,
atomic rename, and parent-directory sync, while remaining rebuildable projection
state outside the graph WAL. A graph-derived rebuild records the source graph
commit epoch inside the caller-owned search projection snapshot so retrieval can
compare projection freshness with the live graph snapshot without moving search
state into the graph WAL.
`SearchIndex::rebuild_derived_artifacts` wraps the full rebuild path in a
report-oriented orchestration API with document counts, scanned nodes, indexed
documents, and lifecycle-marker state.

Metadata-only repairs use the same graph-derived projection row mapping but only
replace document metadata for already-present projection rows. They preserve
existing titles, text content, and embeddings. If repair discovers missing rows,
it marks full reindex as needed because metadata repair cannot create the absent
search documents without becoming a rebuild.

Embedding lifecycle is tracked by an explicit model manifest. The search
projection persists the embedding model name, optional model version, and vector
dimension next to the snapshot. Row writes must match the manifest dimension,
query vectors with mismatched dimensions degrade to the text leg, and model or
dimension manifest changes mark full reindex as needed instead of silently
reusing stale vectors.

The text leg uses BM25-style scoring rather than simple token coverage:
case-insensitive tokenizer output preserves Nowledge-style underscore
identifiers, splits camelCase/snake_case/kebab/path-like identifiers, creates
adjacent chunk bigrams, splits acronym-to-titlecase technical identifiers such
as `LSMTree` and `HTTPServer`, normalizes common English suffixes for
memory/source/thread-style terms, and expands conservative knowledge-retrieval
aliases such as `rag`, `graph_rag`, `graph_retrieval`, and `kg`, plus database
system aliases such as `wal`/`write_ahead_log`, `mvcc`, `lsm`, `csr`, and
`csc`. Conservative English stopwords are removed before query scoring, corpus
statistics, and matched-term reporting, while raw compound identifiers such as
`the_source` remain searchable. Term frequency affects rank, inverse document
frequency is computed from the current projection, and document length
normalization prevents verbose rows from dominating short focused matches. This
keeps text fallback useful while the projection remains rebuildable.

Search hits expose the information needed by a knowledge retrieval surface:
fused RRF score, per-child RRF components, vector score, text score, vector
rank, text rank, fallback reasons, projection kind, external ID, source ID,
matched analyzer terms, matched projection-text spans, and projection freshness
derived from the recorded source graph commit epoch, current projection markers,
and embedding manifest state. Hybrid ranking uses weighted reciprocal-rank
fusion over the vector and text child retrievers, preserving each child position
and child RRF component for explainability. Matched spans are byte ranges over
the rebuildable search projection title/content fields; they are not canonical
large-value blob spans. Callers that need bounded candidate growth can use
`SearchIndex::search_with_options` with a rank window, which limits which child
candidates participate in RRF while still reporting each child's total candidate
count. The same options also carry
exact-match metadata filters such as `kind` or `source_id`; filters are applied
before vector scoring, BM25 corpus statistics, retriever candidate counts, and
final truncation so scoped retrieval does not leak unscoped candidates into
ranking diagnostics. `SearchResultSet::candidate_set` reports the exact
projection-local pre-filter set using stable document IDs, including id-space,
representation, cardinality, filtered-out count, exactness, the metadata
filters that produced it, the source graph snapshot commit epoch when the
projection was rebuilt from graph storage, and a policy epoch placeholder.
`policy_epoch` remains `None` until a policy runtime exists. This is a
diagnostic boundary only: projection-local positions are not stable graph
identity across projection generations. Higher-level retrieval APIs can use
those fields for score breakdowns, provenance, and stale projection warnings
without making the search projection canonical. Callers that need
response-level diagnostics can use
`SearchIndex::search_with_report` to get the total document count, post-filter
document count, candidate-set report, pre-limit hit count, requested limit, rank
window, truncation flag, truncation reasons, child retriever availability,
candidate counts, top hit IDs, and per-child top candidate ranks and scores.

The stable embedded facade exposes this boundary without owning search state:
`Database::rebuild_search_projection` derives projection rows from the canonical
graph into a caller-owned `SearchIndex`, and `Database::retrieve_knowledge`
combines that projection report with the current graph commit epoch and a
compact diagnostics summary. Retrieval callers can pass a rank window through
`KnowledgeRetrievalRequest` to bound hybrid child retriever participation, pass
search fusion weights to bias vector or text child retrievers before graph
context expansion, and pass metadata filters that scope both search hits and
graph-native seed candidates. Filter keys align with graph-derived
projection metadata:
`kind` maps to canonical node labels, `external_id` maps to node `id`, and other
keys map to same-name scalar node properties. Returned diagnostics preserve the
search limit, rank window, search fusion weights, graph seed budget, graph
context budget, candidate budget, filtered candidate counts, search document
scope, search hit count, search truncation flag and reasons, graph seed counts,
graph-seed truncation flag and reasons, graph context path count, fan-out reason
count, final candidate count, pre-limit merged candidate count, response-level
candidate truncation flag and reasons, graph-context truncation flag and
reasons, projection source graph commit epoch, stale projection warnings,
projection marker warnings, and empty-result reasons. This keeps Knowledge
Retrieval as the primary application-facing path while preserving the rule that
search artifacts are rebuildable and outside the graph WAL.

`Database::retrieve_knowledge` also includes a bounded graph-native seed
retriever over canonical nodes. `KnowledgeRetrievalRequest::graph_seed_limit`
controls the budget. A limit of zero disables the graph seed leg; otherwise the
facade matches query tokens against stable graph properties such as `id`,
`title`, `name`, `summary`, `content`, `body`, and `text`, then returns
deterministically scored `KnowledgeGraphSeed` entries with canonical entity
snapshots and matched property names. This gives the retriever DAG an explicit
graph child even when the caller has no usable search projection.
At the knowledge facade level, `KnowledgeRetrieverReport` normalizes child
retriever diagnostics for vector, text, and graph seed legs: availability,
candidate count, optional limit, truncation flag, truncation reasons, and top
candidate rank, score, canonical node ID, matched spans, and graph context path
count are exposed in one place. Search child top candidates also carry
projection freshness, while graph seed top candidates leave it empty because
they are read directly from canonical graph state. Search child reports
distinguish rank-window trimming from search-limit truncation, while graph seed
reports record graph-seed limit truncation.
`KnowledgeCandidate` then projects returned search hits and graph-native seeds
into one application-facing candidate surface. Each candidate records its source
leg, source-local rank, merged source legs, combined score, score breakdown,
optional canonical node ID, optional canonical entity snapshot, optional search
evidence summary, matched projection spans, matched graph properties, and
graph-context path count. The canonical node ID is explicit so callers do not
treat search projection hit IDs as stable graph identity.
Search-hit and graph-seed
candidates that resolve to the same canonical node are merged by graph identity,
with the search hit kept as the primary leg and the graph seed recorded in
`merged_sources`. `KnowledgeCandidateScoringPolicy` currently supports default
max scoring and weighted sum scoring over search and graph-seed scores, giving
future rerank policies a stable hook while preserving per-leg score provenance.
`KnowledgeRetrievalRequest::candidate_limit` applies a response-level candidate
budget after this merge and reports a fan-out reason when the budget truncates
the merged candidate set. This gives the future retriever DAG a typed candidate
boundary without making the search projection part of canonical graph state.

The same facade performs bounded multi-hop graph context
expansion for returned search hits whose projection metadata maps back to a
canonical graph node and for graph-native seeds matched directly from canonical
nodes. The result includes hop number, source/target node labels, external IDs,
relationship type, path direction, and fan-out reasons when the configured graph
context limit cuts expansion short. This gives RAG callers graph evidence paths
without issuing ad hoc Cypher for common neighborhood and short-path context,
including pure graph-seed retrieval when no search projection is available. It
also returns `KnowledgeEvidence` summaries that bind each search hit to its
projection kind, external ID, source ID, canonical node ID, matched terms, score
components, ranks, and graph context path count. This keeps raw evidence
provenance explicit even though the search projection remains outside canonical
graph storage.

Typed knowledge operations can bypass the search projection entirely when the
caller already has graph identity. `Database::knowledge_entity` returns a
canonical node snapshot by label and external ID, including node id, labels,
external ID, graph commit epoch, and scalar properties. `Database::knowledge_neighbors`
accepts the same identity plus optional relationship type, direction, hop bound,
and result limit, then returns the same path evidence structure plus fan-out
reasons and typed traversal diagnostics. `Database::knowledge_paths` accepts
source and target identities plus the same traversal budget and returns bounded
graph paths as ordered evidence segments with source/target presence and path
count diagnostics. `Database::knowledge_subgraph` expands a bounded typed
subgraph from one identity, returning canonical node snapshots, relationship
evidence segments, node/relationship fan-out reasons, and node/relationship
count diagnostics. `DatabaseReadTransaction` exposes the same typed knowledge
operations over its pinned catalog and graph snapshot, so callers can perform
stable knowledge navigation without falling back to ad hoc Cypher. This makes common
knowledge-application navigation a first-class API instead of forcing
application code to construct ad hoc Cypher for every entity lookup,
neighborhood lookup, path query, or local subgraph expansion.

## Milestones

1. Parser and AST for the Cypher subset used by Nowledge.
2. In-memory graph store with transactions for semantic and planner tests.
3. Logical plan builder for `MATCH`, `WHERE`, `RETURN`, `CREATE`, `MERGE`, and
   node and relationship `DELETE`.
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
- internal Nowledge-shaped compatibility fixtures against the Skein facade
- external-process shadow adapter tests for Ladybug/Kuzu wrapper wiring
- compatibility tests against the current Nowledge Ladybug-backed wrapper

Before replacing Ladybug in Nowledge, the compatibility suite should run both
engines against the same fixtures through `ExternalShadowCommand` or an
equivalent `CompatibilityShadowEngine` implementation, and compare rows,
mutation effects, error classes, and projected graph outputs. The shadow report
must also pass the compatibility cutover gate: every required check is matched
by the shadow engine, primary-only checks are reported as blockers by default,
and the configured minimum matched-check count is satisfied.
