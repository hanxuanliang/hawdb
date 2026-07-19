# Nowledge Replacement Matrix

This document maps Skein against the current Nowledge Mem local data-plane
needs. The source product boundary is:

- Kuzu/Ladybug is the graph of record for memories, threads, sources, entities,
  relationships, schema migrations, graph algorithms, checkpointing, and
  storage-version checks.
- LanceDB is a rebuildable search projection for semantic vectors, FTS/BM25,
  denormalized filter metadata, and search-table lifecycle markers.
- Large content blobs remain outside the graph/search engine.
- Cloud remains PostgreSQL-first and should not embed Skein as canonical graph
  storage.

## Kuzu/Ladybug Replacement Surface

| Capability | Nowledge need | Skein status |
|---|---|---|
| Embedded open/create by path | One local database object per workspace path | Partial: `Database::open`, `GraphStore::open` |
| Read-only and bounded resource configuration | Local callers need read-only opens and hard query budgets | Partial: `DatabaseConfig::read_only` opens only existing database directories without creating missing paths, then rejects Cypher mutations, transaction mutations, checkpoints, schema maintenance, database-owned projected graph artifact rebuilds, and derived artifact job execution before they write database-owned state; content/blob parser artifact jobs are recorded at the derived-artifact boundary with optional structured payloads for caller-owned object references, checksums, parser hints, and projection targets; `Database::external_content_artifact_job_summary` exposes aggregate pending/failed/succeeded health plus action-grouped pending/failed counts, next pending, and oldest failed job ids, `Database::external_content_artifact_job_summary_for_action` exposes the same bounded health counters for one caller-owned action, `Database::pending_external_content_artifact_jobs` exposes a bounded pending poll surface for caller-owned runtimes, `Database::pending_external_content_artifact_jobs_for_action` lets action-specific runtimes poll only work they can handle, `ExternalContentArtifactRuntimeManifest` lets runtimes declare supported actions, required payload keys, version, and estimated operations for bounded claimable-job polling, manifest-scoped run/complete, and Import-lane work planning, `Database::succeeded_external_content_artifact_jobs` plus `Database::succeeded_external_content_artifact_jobs_for_action` expose bounded successful lineage/output rows, and `Database::failed_external_content_artifact_jobs_for_action` plus `Database::retry_failed_external_content_artifact_job_for_action` let those runtimes inspect and requeue only their own failed retry queue; the default graph-kernel runner rejects them as graph-kernel-external work while preserving payloads in job reports, `Database::run_next_external_content_artifact_job_with` plus `Database::run_next_external_content_artifact_job_for_action_with` let caller-owned runtimes complete those jobs and publish rebuildable projections back to Skein, `ExternalContentArtifactJobCompletion` plus direct completion runners standardize lightweight runtime/input/output/projection/checksum lineage rows without storing parser results in the graph kernel, successful external content jobs retain their last structured output rows for lightweight lineage/audit, `Database::external_content_artifact_job_background_work_plan` and `Database::external_content_artifact_job_background_work_plan_for_action` expose rankable `Import` work plans for pending parser/crawler queues, background wrappers, including action-scoped and manifest-scoped background runners, can charge internal parser/crawler loops to the `Import` QoS lane, explicit search-projection graph-delta maintenance candidates carry the exact executable delta request through ranking, changefeed-backed freshness candidates can derive precise executable search delta requests from source graph commit epochs when the log range covers the caller's freshness, `DatabaseConfig::max_search_projection_change_log_entries` bounds the retained changefeed window for incremental search projection and forces full rebuild when caller freshness falls behind the retained range, freshness candidates fall back to non-executable planning signals when the change log is too new and a full rebuild is required, bounded schema-maintenance background wrappers admit against the actual executable plan cost rather than the caller cap, and over-limit search-projection graph deltas are excluded from background maintenance candidates while direct execution still returns a hard-limit error; `DatabaseConfig::max_read_result_rows` caps direct read query and read-transaction result rows; `DatabaseConfig::max_optimizer_groups` caps cascades memo search groups with deterministic direct physical fallback warnings; `DatabaseConfig::recovery_mode` defaults to torn-tail tolerant WAL replay and can reject torn WAL tails or checksum mismatches during strict open; `DatabaseConfig::max_wal_replay_entries` caps startup WAL replay after valid record decode and before applying the next top-level WAL record, preserving batch replay atomicity; mutation queries keep WAL/commit semantics and are not failed after durable execution |
| Storage version | Boot compatibility checks | Partial: `storage_version()` plus explicit boot-time manifest and checkpoint storage-version validation with unsupported-version errors |
| Cypher reads | `MATCH`, `WHERE`, `RETURN`, ordering, pagination, aggregation, parameter binding | Partial: single-node `MATCH`, comma-separated and consecutive two exact node patterns for Nowledge endpoint existence checks such as `MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)` and `MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id`, count-only one-hop `OPTIONAL MATCH` forms used by thread cleanup such as `MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)` and legacy extracted-reference counts, the Nowledge graph-analysis degree shape `MATCH (e:Entity) OPTIONAL MATCH (e)-[r]-() WITH e, COUNT(r) as degree RETURN e.id, e.name, degree ORDER BY degree DESC LIMIT 10`, Nowledge entity lifecycle impact, detail, relation preview, label preview, and community preview reads, Nowledge graph orphan one-hop relationship-existence predicates shaped as `NOT (e)<-[:MENTIONS]-(:Memory)` and `NOT (e)-[:RELATES_TO]-()`, Nowledge schema-migration verification `WHERE NOT EXISTS { MATCH (c)-[:SYNTHESIZED_FROM]->(s) }` over already-bound `CRYSTALLIZED_FROM` endpoints and bounded `WITH DISTINCT c.id AS a, s.id AS b RETURN count(*)` pair counting, Nowledge label usage count reads shaped as `MATCH (l:Label) OPTIONAL MATCH (l)<-[:HAS_LABEL]-(n) WITH l, COUNT(n) as usage_count RETURN ...` and direct optional projection-count reads shaped as `MATCH (l:Label) OPTIONAL MATCH (m:Memory)-[:HAS_LABEL]->(l) RETURN l.id, l.name, COUNT(m) AS usage_count`, one-hop and finite bounded outgoing relationship expansion including source revision history with unused `MATCH p = (...)` path binding, endpoint-id bounded `ALL SHORTEST` graph path reads returning `properties(nodes(p), ...)` lists and `length(p)`, Nowledge-used unlabeled node scans such as `MATCH (n) WHERE n.id IN $ids`, Nowledge-used repeated-label any-of node patterns such as `(neighbor:Entity:Memory)`, Nowledge-used whole-record projections such as `RETURN m` and `RETURN r`, Nowledge-used anonymous relationship endpoints for count reads, Nowledge-used one-hop undirected relationship expansion including node-detail neighbor and edge counts shaped as `MATCH (n)-[r]-(neighbor) WHERE n.id = $node_id RETURN COUNT(DISTINCT neighbor), COUNT(r)`, Nowledge-used source and one-hop target node property patterns, one-hop relationship property pattern filters, one-hop relationship variable property reads in `RETURN`, `WHERE`, `ORDER BY`, `COUNT(r)`, `COUNT(r.property)`, Nowledge-used `MIN(variable.property)`, Nowledge-used `MAX(variable.property)` for scheduler fingerprints, Nowledge-used `AVG(variable.property)` for health aggregates, and read-side `id(variable)` projection/filtering/ordering, equality/inequality/range/null/list/`CONTAINS`/`STARTS WITH`/`ENDS WITH` predicates, Nowledge-used `timestamp($cutoff)` and `CAST($cutoff AS TIMESTAMP)` values for freshness and cleanup predicates encoded as epoch-nanos integers, Nowledge-used `list_contains(e.aliases, $name)` property-list membership, Nowledge thread bulk-move normalized-space predicates shaped as `CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END =/<> $space_id`, Nowledge community summary presence-ranking order expressions shaped as `CASE WHEN c.ai_summary IS NOT NULL AND c.ai_summary <> '' THEN 0 ELSE 1 END`, Nowledge thread distillation optional source filters shaped as `($source IS NULL OR t.source = $source)`, parameters inside literal lists for `IN`, parenthesized `AND`/`OR` plus unary `NOT` boolean predicates, escaped string literals, parameterized graph algorithm procedure options, global and grouped `COUNT`, Nowledge-used `COUNT(DISTINCT variable)` and `COUNT(DISTINCT variable.property)`, Nowledge bridge grouped reads with `COUNT(DISTINCT e2.community_id)`, `COUNT(*)`, aggregate alias filtering, bounded post-aggregate Community lookup by grouped column, and row-preserving optional post-aggregate Community lookup, Nowledge feed synthesized-source id reads shaped as `WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids`, Nowledge synthesized-source coverage lookups shaped as `WITH c.id AS cid, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid LIMIT 1`, `RETURN DISTINCT`, `ORDER BY`, `SKIP`/`OFFSET`, `LIMIT`, and typed parameters for query/explain/transaction APIs; general OPTIONAL MATCH row preservation, general CASE expressions, general `EXISTS` subqueries, returned path values, relationship variables, relationship property patterns, target node property patterns, and general unbounded shortest-path searches are rejected until path/list relationship semantics exist |
| DDL/schema | Node/rel labels, table descriptors, indexes, constraints, and migrations | Partial: explicit node-label token DDL, relationship-type token DDL, node/relationship table descriptor DDL, table/property descriptor state transitions, property schema DDL, equality, composite equality, range, and full-text property index DDL, node/relationship property uniqueness constraint DDL, node/relationship property existence constraint DDL, `Database::run_schema_maintenance` for BACKFILL/VALIDATING advancement plus descriptor GC with WAL/checkpoint persistence and pre-WAL validation, `Database::plan_schema_maintenance` for read-only pending-work estimates, descriptor-bounded `Database::run_bounded_schema_maintenance` batches, bounded background wrappers that bind QoS admission to descriptor-batch execution, and planned background schema maintenance wrappers that charge dry-run estimates to the `Mutation` QoS lane while preserving the direct caller path; richer online index/content backfill orchestration remains |
| Node mutations | `CREATE`, `MERGE`, `SET`, `DELETE` | Partial: `CREATE` node including Nowledge's variable-bearing `CREATE (j:AugmentationJob {...})` form, single-node exact-property `MERGE`, Nowledge schema-migration `MERGE (m:SchemaMigrationLog {id: $id}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP()` with match-key-only lookup and create-only value assignments, Nowledge GraphMeta stamp `MERGE (m:GraphMeta {meta_id: 'main'}) SET ...` with greenfield create folding and match-side updates, Nowledge label upsert `MERGE (l:Label {id: $label_id}) ON CREATE SET ... ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)` with match-only updates and pending-transaction create folding, single-node `MATCH ... SET`, Nowledge thread bulk-move `MATCH ... SET ... RETURN t.thread_id` update-return writes over normalized-space predicates, Nowledge AugmentationJob lifecycle status/progress/result/error writes, Nowledge undo-community writes for `MATCH (c:Community) DETACH DELETE c`, `MATCH (n) WHERE n.community_id IS NOT NULL SET n.community_id = NULL`, and GraphMeta community reset, Nowledge-used `CURRENT_TIMESTAMP()` values encoded as epoch-nanos integers, Nowledge-used integer self-increment `SET s.memory_count = s.memory_count + 1` for source provenance counters, Nowledge-used `COALESCE(..., 0) + 1` plus multi-assignment node `SET` for memory access tracking, typed endpoint-known knowledge entity creation wrappers plus ordered batch create wrappers over the WAL-backed `CREATE` path for Memory/Source/Entity/Label/Thread/Skill lifecycle ingestion, with id-consistency validation, existing-identity no-write reporting, and grouped WAL batch commits for eligible rows, typed endpoint-known knowledge entity upsert wrappers plus ordered batch upsert wrappers for lifecycle `MERGE`-style create-or-update paths, with separate create/update property maps, id immutability, projected-idless non-writable reporting, duplicate pending identity no-write reporting, and one grouped WAL batch for eligible create/update rows, typed exact-identity knowledge property update wrappers plus ordered batch property update wrappers over the same WAL-backed `SET` path for lightweight metadata/review-status/access-field writes, with per-input missing/filter/idless reporting and grouped WAL batch commits for eligible rows, typed graph-analysis community assignment cleanup through `Database::clear_knowledge_community_assignments` for label-scoped or all-node `community_id` resets, with pre-WAL label validation, overlapping-label deduplication, non-null filtering, and one grouped WAL batch for eligible clears, typed exact-identity knowledge entity detach-delete wrappers plus same-label ordered batch detach-delete wrappers over the same WAL-backed `DETACH DELETE` path for endpoint-known Memory/Source/Entity/Label/Thread/Skill lifecycle cleanup, including `id IN` batches with per-input missing/filter/idless reporting and deduplicated writes, single-node unlabeled `MATCH (n) ... SET` for Nowledge graph-analysis annotations, single-node `MATCH ... DELETE`/`DETACH DELETE`, Nowledge entity lifecycle `MATCH (e:Entity {id: $id}) DETACH DELETE e`, Nowledge thread cleanup target-node `DETACH DELETE` through `MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m`, and `id()` filters for node `SET`/`DELETE` |
| Relationship mutations | relationship tables/groups | Partial: `CREATE (:Label {...})-[:TYPE {...}]->(:Label {...})`, exact-pattern relationship `MERGE`, typed exact-identity relationship creation wrappers and ordered batch creation wrappers for Nowledge endpoint-known writes such as `MENTIONS`, `SOURCED_FROM`, `HAS_LABEL`, `EVOLVES`, and `COMPACTS_TO`, with endpoint metadata filters, identifier validation, parameter-bound relationship properties, per-input missing/filter/idless reporting, and the same WAL-backed `MATCH ... CREATE` path; eligible batch rows are committed through one transaction-level grouped WAL batch, typed endpoint-known relationship upsert wrappers plus ordered batch upsert wrappers for Nowledge endpoint+type `MERGE` writes such as `HAS_LABEL` and `SYNTHESIZED_FROM`, with create-only properties, existing-edge no-write reporting, projected-idless non-writable reporting, duplicate pending edge no-write reporting, and one grouped WAL batch for eligible creates, typed exact-identity relationship property update wrappers and ordered batch update wrappers for endpoint-known weight, provenance, review-field, and lightweight edge metadata writes, with optional relationship-property equality filters and the same WAL-backed `MATCH ... SET r.property` path; eligible batch update rows are committed through one transaction-level grouped WAL batch, typed exact-identity relationship delete wrappers and ordered batch delete wrappers for endpoint-known cleanup such as label/source relation removal with optional relationship-property equality filters and the same WAL-backed `MATCH ... DELETE r` path; eligible batch cleanup rows are committed through one transaction-level grouped WAL batch, Nowledge source-provenance `MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {...}]->(s)` over already-matched endpoint nodes with one grouped WAL append, Nowledge EVOLVES-style `MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {...}]->(b)` endpoint equality writes over already-matched node sets with one grouped WAL append, Nowledge label assignment `MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET ...` with relationship match-key and create-only properties kept separate, Nowledge label-merge transfer `MATCH (n:Memory)-[:HAS_LABEL]->(src:Label {id: $src}) MATCH (tgt:Label {id: $tgt}) MERGE (n)-[r:HAS_LABEL]->(tgt) ON CREATE SET ...` with old-edge source selection and idempotent target-edge creation, Nowledge schema migration `MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, ...` with matched-relationship property copy and idempotent rerun behavior, Nowledge label removal `MATCH (m:Memory {id: $memory_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r` with target-node property filtering, one-hop relationship variable property `SET` through `MATCH (a:Label)-[r:TYPE {key: value}]->(b:Label) SET r.property = value`, Nowledge-used target-filtered and multi-property relationship `SET` on one relationship variable with one WAL batch, one-hop relationship variable `DELETE` through `MATCH (a:Label)-[r:TYPE {key: value}]->(b:Label) DELETE r`, source-node plus relationship-variable `WHERE` filters, and `id()` filters for one-hop relationship `SET`/`DELETE`; mixed node/relationship `OR` predicates are rejected for mutation filtering until row-binding mutation semantics exist |
| Transactions | explicit begin/commit/rollback | Partial: mutation-only `DatabaseTransaction` with commit/rollback over grouped WAL and immutable `DatabaseReadTransaction` snapshots |
| WAL recovery | crash-loop recovery and torn tail handling | Partial: WAL replay with checksum/torn-tail stop, strict recovery mode, configurable top-level replay entry cap, atomic batch replay, and recovery-time relationship endpoint validation before accepting graph state |
| Checkpoint | explicit checkpoint and log pruning | Partial: full snapshot checkpoint, WAL truncate, checksummed manifest publication with parent-directory sync after atomic rename, active reader epoch pins, safe reclamation commit epoch publication, zstd-by-default checkpoint and projected graph artifact payload envelopes with legacy plain-text read compatibility, relationship endpoint validation after checkpoint load and WAL replay, projected graph artifact publication with the same rename durability boundary, and structured storage reclamation watermark reporting |
| Adaptive adjacency | sparse local neighborhoods and dense hub handling | Partial: current incoming/outgoing adjacency indexes expose stable ordered adjacency entries sorted by `(neighbor_id, relationship_id)` and sparse/dense group classification at the store API boundary; executor one-hop and bounded outgoing expansion consume the ordered view, and knowledge retrieval graph-context plus typed knowledge neighbors/paths/subgraph expansion use the same ordered view while surfacing dense relationship-type adjacency groups in traversal diagnostics; physical sparse blocks, copy-on-write dense segments, and page-level hub isolation remain |
| Property indexes | selective equality, range, and text lookups with index-backed plans | Partial: persistent equality, composite equality, range, and full-text index descriptors, rebuildable in-memory ordered property indexes, ngram-backed text candidate indexes, bounded descriptor-level rebuild reports for composite and full-text execution projections, rankable `Projection` work plans plus bounded background/scheduled wrappers for internal projection rebuild loops, `IndexNodeSeek`, `IndexNodeCompositeSeek`, `IndexNodeTextSeek`, bounded `IndexNodeRangeSeek` for conjunctive range predicates, checkpointed graph statistics with commit-epoch freshness and histogram sampling metadata, per-node-property and per-relationship-property distinct counts, deterministic adaptive sorted value histograms for node and relationship properties with exact-versus-sampled markers, histogram-backed range selectivity, one-hop and bounded multi-hop path-cardinality summaries, one-hop and bounded multi-hop path source/target coverage distinct counts for aggregate costing, plus relationship-property filter selectivity for pattern filters, pushed-down one-hop relationship-variable equality predicates, and relationship-variable range filters; richer text analyzer parity remains |
| Optimizer diagnostics | explain traces and deterministic plan identity | Partial: explain output includes selected plans, scan/seek costing decisions, expand cardinality estimates with per-hop exact/fallback rows, optional degree and relationship count-sum seed/fanout costing from label/property plus relationship type/source statistics for outgoing legs and relationship type/target statistics for incoming legs, endpoint cartesian product input/output row and cost estimates plus stable cost/fingerprint input ordering for flattened all-single-row endpoint products, residual node-property equality/inequality/`IN`/range/string/null filter selectivity, residual relationship-property equality/inequality/`IN`/range/string/null filter selectivity, residual read-side node/relationship `id(variable)` filter selectivity, conservative constant/`OR` predicate selectivity for optional parameter filters, full-text candidate costing without residual double-counting, grouped node/relationship-property aggregate cardinality from selected-plan variable labels/types and property statistics, bounded work costing for distinct variable/property aggregate targets, one-hop and bounded multi-hop path source/target coverage statistics for distinct variable aggregate targets, recursive selected-plan row/cost summaries, deterministic physical plan fingerprints, hard memo budget warnings with deterministic fallback, and an optimizer-only smoke benchmark for range seek, bounded expand stats, composite seek, text seek, residual string filters, optional-source `OR` filters, summary-presence null filters, normalized-space exclusion filters, thread candidate normalized-space multi-seek reads, low-selectivity scan fallback, production-shaped selective seed + bounded expand + aggregate + sort/limit, one-hop relationship-property expand reads, pushed-down relationship equality plus relationship range-filter workload reads, source-memory-label and source-memory-entity-label cross-pattern aggregate reads, source-attributed entity community export aggregate reads, source coverage aggregate-alias filter reads, entity bridge-span distinct-property aggregate reads, bounded multi-hop distinct-target aggregate reads, incoming optional relationship count-sum and optional degree target-stat reads, endpoint-existence and nested endpoint-existence cartesian product reads, residual node-property, relationship-property, and relationship-id filter reads, selected-plan cost, selected physical-plan operator and class count histograms, read-only parameterized `explain-json` CLI output with optimizer search mode and plan-cache stats including disabled-miss and bypass counters for CI artifacts, and budget fallback; broader cross-pattern workload-shaped benchmark suites remain |
| Compatibility front door | typed local wrapper shape for Nowledge calls | Partial: `NowledgeGraphAdapter` exposes typed parameterized query, explain, grouped mutation transaction execution over `NowledgeGraphStatement`, and Knowledge Retrieval over a caller-owned `SearchIndex`, plus typed knowledge entity/property projection/entity create/property update/entity delete/relationship create/relationship update/relationship delete/relationships/neighbors/paths/subgraph surfaces with traversal diagnostics, preserving missing-parameter pre-WAL failure semantics and avoiding raw string interpolation; public `nowledge_memory_core_fixture` and `nowledge_memory_core_inventory` constructors expose the current Nowledge core migration contract including whole-node by-id projection, source provenance and entity relationship endpoint existence checks, entity lifecycle impact counts, detail projections, relation/label/community preview reads, entity-node `DETACH DELETE` cascade checks, graph orphan entity reads and cleanup-candidate reads, AugmentationJob lifecycle create/running/progress/completed/failed writes plus status/list reads, undo-community detection writes, CRYSTALLIZED_FROM-to-SYNTHESIZED_FROM relationship-copy migration writes, distinct-pair count verification reads, and unmirrored-edge verification reads, count-only optional thread cleanup reads, thread message target `DETACH DELETE` cleanup writes, top-entities-by-degree graph analysis reads, node-detail neighbor count reads, label usage count reads including direct optional projection-count reads, agent context activity digest reads, agent context activity task reads, agent context stale crystal and EVOLVES cluster reads, health stale memory count reads, cleanup coverage `CAST(... AS TIMESTAMP)` predicates, scheduler fingerprint `MAX(updated_at)` reads, decay scheduler EVOLVES/crystal synthesis count reads, wiki export summary count reads, wiki export topic entity/crystal ranking reads, wiki export entity listing mention-count and cursor reads, wiki export community entity anchor visibility reads, wiki export entity id-or-name lookup and community context reads, wiki export community crystal source visibility reads, wiki export community top-memory ranking reads, OKF export community/entity/crystal list and crystal-source entity community reads, shared OKF/wiki export entity mention-detail reads, shared OKF/wiki export related-entity reads, OKF export memory row label-collection reads, OKF export label row reads, memory monthly `date_part('year'|'month', created_at)` grouped aggregate reads, memory created-at bulk reads, memory bulk metadata and space reads, learning memory latest reads, source parsed path list reads, community summarized list reads, community memory type-filter reads, community entity memory-count reads, entity mention-count list reads, community memory coalesced summary reads, source detail memory-count reads, source provenance memory detail reads, source memory id list reads, memory source provenance id reads, memory metadata update writes, source memory-count decrement-floor writes, source relationship source-reference count reads, source relationship source-reference deletes, memory compact detail fallback reads, memory label name list reads, memory label bulk name reads, memory label fallback bulk reads, memory label endpoint bulk reads, memory label distinct name count reads, source label bulk name reads, source label endpoint bulk reads, source label relationship count reads, source label relationship merge writes, source label relationship delete writes, source detail normalized-space reads, source detail chunk-count reads, source detail file-path reads, source default metadata fallback reads, source list fallback page reads, source overview memory-count ranking reads, source bulk summary fallback reads, source count reads, source extracted id list reads, source extracted lifecycle mark-indexed writes, source lifecycle indexed chunk-count writes, source lifecycle state update writes, source space update writes, source bulk normalized-space move writes, source normalized-space id list reads, memory normalized-space id list reads, memory bulk normalized-space move writes, memory normalized-space limit-one reads, memory candidate normalized-space id reads, memory candidate normalized-space exclusion reads, memory normalized-space count reads, memory normalized-space limited id reads, memory id-list normalized-space move-returning writes, memory id-list normalized-space exclusion move-returning writes, thread normalized-space id pair reads, thread bulk node-id normalized-space move writes, thread identity normalized-space id reads, thread metadata reads and update writes, thread identity bulk normalized-space move writes, thread normalized-space logical id reads, memory entity name list reads, memory entity endpoint bulk reads, memory review-status bulk reads, memory ranked overview reads, thread message ordered reads, thread compacted-memory count reads, thread compacted-memory summary reads, community memory `WITH m, COUNT(e)` grouped reads over projected node-map columns, label distribution `WITH l, COUNT(DISTINCT m)` grouped reads, mention-breadth aggregate `WITH ... ORDER BY ... LIMIT ... RETURN` reads, thread bulk-move normalized-space selection reads and update-return writes, thread distillation optional source count/candidate reads with a typed facade, thread compaction attribution reads, feed synthesized-source id collection reads with a typed facade, skill synthesized memory id reads, skill stage projection, active, and builder list reads, skill detail reads, skill synthesized memory direct-id and detail reads, skill metadata and version reads, metadata writes, and usage-stat writes, source coverage aggregate-filter reads, source provenance memory-count increments, source attribution reads, `SOURCED_FROM` relationship creation, source fan-in relationship count reads, source revision history bounded path reads, source metadata `timestamp(...)` update writes, source-node `DETACH DELETE` cascade checks, label canonical lookup, dynamic label updates, label-edge removal, label node `DETACH DELETE`, EVOLVES endpoint-equality relationship creation, EVOLVES progression list reads, schema migration merge, GraphMeta `MERGE SET`, label node `MERGE ON CREATE SET ... ON MATCH SET`, matched HAS_LABEL relationship `MERGE ON CREATE SET`, memory access counter touches, current timestamp writes, timestamp cutoff freshness predicates, entity alias list lookup, health average reads, relationship min aggregation, relationship property reads, incoming one-hop `MENTIONS` reads, untyped overview-edge relationship type projection, repeated-label graph overview neighbor reads, Nowledge-style `RETURN` projection fallbacks, fallback `ORDER BY COALESCE(...)`, limited fallback `WHERE COALESCE(...)`/`WHERE LEFT(...)` comparison predicates, Nowledge-style case-insensitive `LOWER(COALESCE(...)) CONTAINS LOWER($needle)` grep predicates, unlabeled node community-assignment mutation, and relationship property mutation-effect checks; `CompatibilityQueryCallSite` and `build_compatibility_query_inventory` provide a public production call-site inventory builder with source metadata and duplicate check-name validation; `build_compatibility_query_inventory_from_json`, `build_compatibility_query_inventory_from_json_str`, and `compatibility_query_inventory_to_json` define the scanner JSON artifact boundary for `call_sites` input and audited `required_checks` output; `scan_nowledge_query_inventory` and `scan-nowledge-inventory` generate this artifact from production Nowledge graph-source Cypher string literals while filtering non-graph SQL, prompt text, tests, benches, and smoke binaries; coverage, inventory gate, shadow cutover, and migration gate reports have JSON exporters with stable lowercase decisions for CI; `assess_query_inventory_coverage` reports covered, missing, and extra fixture checks against a machine-readable required query inventory, `assess_query_inventory_gate` converts coverage into `Ready` or `Blocked` with explicit blockers, `assess_compatibility_migration_gate` combines inventory and shadow cutover gates into one migration decision, and `scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json` exposes library-level external-shadow wiring metadata (`shadow_run`, `shadow_ready`, `shadow_trace`, and `cutover_evidence`) for CI/application automation without shelling through the CLI |
| Compatibility shadowing | compare Skein against the previous local graph wrapper or another oracle when needed | Partial: reusable shadow-engine fixture runner compares Cypher rows with fixture-declared floating-point tolerances, declared error classes, mutation effects, and projected graph outputs against a second engine; production-shaped `project_graph`/`page_rank`/hierarchical `louvain` fixtures are covered; cutover assessment reports `Ready` or `Blocked` with explicit primary-only coverage blockers; an external JSON-lines process adapter can connect an optional wrapper without linking another graph engine into Skein |
| Read concurrency | shared readers, exclusive writes/control | Partial: snapshot readers do not observe later commits, survive checkpoints, publish oldest active reader plus safe reclamation epochs to the manifest, expose the same boundary through `Database::storage_reclamation_watermark`, and provide `export_canonical_graph_snapshot` on both live databases and pinned read transactions with canonical node/relationship records, stable-identity audit, deterministic logical checksum, structured snapshot self-validation, stable-ID import-readiness reporting, caller-persisted `CanonicalStableIdMapping` overlays for records without `id` properties, pinned read-transaction search-projection rebuilds and metadata repair over the snapshot epoch, `stable_ids.skein` physical-export mapping persistence without WAL write amplification, `prepare_graph_lightning_bootstrap_export`, `graph-lightning-bootstrap-manifest`, deterministic `graph-lightning-graph-stream` output, `graph-lightning-verify-export` checksum/count/endpoint validation, `graph-lightning-bootstrap-bundle` ready/blocked evidence packaging, `graph-lightning-stage-bootstrap` local staging catalog publication, `graph-lightning-verify-staging` source-independent staging artifact validation, `graph-lightning-publish-staging` idempotent local published-manifest pointer publication with optional state-marker/fencing/expected-epoch preflight, `graph-lightning-verify-published` published-pointer-to-staging validation, `graph-lightning-gc-staging-report` published/pinned artifact protection, and `graph-lightning-import-status` CREATED/EXPORTING/UPLOADING/MERGING/VALIDATING/READY/PUBLISHED/FAILED/CANCELED/QUARANTINED state aggregation for Graph Lightning v1 manifest/stream gating, live/WAL/checkpoint-recovered export equivalence regression coverage, and the `validate-canonical-snapshot` read-only CLI gate for future GraphStream encoding and storage-equivalence oracles; page-level MVCC and physical reclamation remain |
| Projected graph | `PROJECT_GRAPH`, page_rank, louvain | Partial: immutable in-memory CSR/CSC projection over store snapshots with node-label and relationship-type filtering, WAL/checkpoint-persisted projection definitions, checkpoint-generated CSR/CSC projection artifacts with format version, projection epoch, public reusable/stale status, recovery-time filtering of artifacts whose commit epoch or definition no longer matches the replayed graph state, epoch/definition-checked execution reuse, checkpoint-independent background artifact rebuild, report-oriented `Database::rebuild_derived_artifacts` for projected graph artifacts, embedded derived-artifact job queue with pending/running/succeeded/failed status, Kuzu-style `CALL project_graph`, `CALL page_rank`, and `CALL louvain` Cypher procedure entry points, reverse traversal, PageRank scoring with parity tolerances, deterministic Louvain-compatible community assignment, hierarchical Louvain levels via `maxLevels`, production-shaped compatibility fixtures, and cutover gating |

Note: the current compatibility fixture also covers Nowledge-used entity reuse
exact, case-insensitive, alias-containment, same-type bounded scan reads, and
entity temporal metadata create/update writes, entity total count reads, and
entity `MENTIONS`/`RELATES_TO` creation writes. It also covers Nowledge-used
label resolver null-canonical scans/backfills, rename collision guards,
existence reads, and remove-all count/delete writes. Source provenance coverage
includes Source endpoint checks, full `SOURCED_FROM` creation writes,
edge-existence/global counts, exact repair candidate scans, and source
memory-count reads. PageRank coverage includes membership/visibility reads,
score persist/clear writes, central-entity lookup, GraphMeta clear stamps, and
planner node/relationship totals and changed count reads. Community detection
scheduler coverage includes GraphMeta state reads, candidate scans, member
entity lookups, and summary writes. Cleanup scheduler coverage includes bounded
seed scans, EVOLVES pair reads, cleanup fingerprint row fetches, and floor-zero
engagement `CASE` ordering.
External content artifact orchestration also exposes bounded pending/failed
polling, specific-job execution, and explicit failed-job retry for caller-owned
parser runtimes, while keeping database-owned projected graph artifact rebuilds
on the graph-kernel runner.
Migration gate JSON keeps human-readable blockers and adds machine-readable
fixture-mismatch, inventory, and shadow blocker counts, shadow evidence counts,
caller-owned rollback evidence fields, `shadow_run.evidence_kind`, plus grouped
blocker messages so cutover automation can separate scanner coverage gaps,
previous-wrapper parity failures, rollback readiness gaps, and self-shadow
protocol smoke without making Skein open the previous graph database.
The Nowledge migration-gate library options and CLI can require caller-owned
rollback evidence and carry the supplied previous-database reopen proof into the
gate decision.
Graph Lightning bootstrap bundle export gates likewise split manifest and
GraphStream blockers into counts and grouped messages for import preflight
automation.
Staging verification gates also split artifact, manifest, GraphStream, bundle,
and catalog errors into grouped arrays for offline upload/resume automation,
with staged artifact count/byte summaries for upload observability and
fail-closed catalog/manifest protocol-version checks.
Published-pointer verification gates split pointer, catalog, and staging errors
for Graph Lightning resume checks.
Staging GC gates also group published-pointer verification errors before
declaring artifacts deletable and report total/pinned/deletable staging bytes.
Import-status reports add a machine-readable `resume_action`, optional
caller-owned state-marker aggregation with active-state idempotency-key
validation, optional caller-owned checkpoint-log aggregation with failure
coordinates, stage/status/failure-count summaries, and idempotency-coordinate
conflict detection, and active-import `resource_retention` policy so automation can
choose staging, publishing, active resume, completion, failure/cancel handling,
quarantine/manual-repair, or artifact-retention handling without parsing
human-readable error strings or deleting READY artifacts before publish.

## Current Compatibility Evidence

The current live scanner coverage gate over the local Nowledge graph-source tree
is complete for the scanned Cypher surface: `nowledge-scanned-inventory`
requires 698 checks, `nowledge-memory-core` covers all 698, and
`missing_items` is empty. The scanner excludes vendored `upstream_forks`
examples from this production-source gate.

This does not by itself complete migration cutover. The remaining evidence gap
is external shadow comparison against the previous local graph wrapper when a
specific migration gate needs oracle-backed parity evidence. Required cutover
evidence now rejects self-shadow protocol smoke runs and requires the shadow
ready preflight to declare `engine_kind: "previous_wrapper"`. Production
cutover runs that require storage or resource evidence must also attach
`skein-storage-recovery-report` and `skein-background-maintenance-report`
artifacts from the real database path, with matching protocols and full
readiness under the fail-closed cutover evidence rules.

Typed Memory latest updates are exposed through
`Database::update_knowledge_memory_latest_batch` for Nowledge EVOLVES
promotion/demotion writes. The wrapper updates only `is_latest`, supports the
exact `space_id` filter used by in-space demotion, reports missing, filtered,
duplicate, and non-writable rows before writing, and commits eligible updates
through one grouped WAL batch.

The stable way to report "how much of Nowledge can be replaced" is to run
`skein nowledge-replacement-summary <migration-gate-json>` over a generated
migration-gate bundle, or add `--compact`/`--max-family-items <n>` when the
bundle contains large per-family diagnostic arrays. Add `--max-blockers <n>`
when the release artifact needs a bounded blocker sample instead of full blocker
strings. The summary intentionally separates three numbers:
`business_surface.covered_per_million` for scanned Cypher coverage,
`shadow_parity.matched_per_million` for previous-wrapper comparison, and
`production_replacement_per_million` for conservative production replacement
readiness. Production replacement stays `0` unless the migration gate is ready,
shadow parity is complete, `cutover_evidence.eligible` is true, and the bundle
shows full per-query-family replacement readiness. Adapter bring-up should use
`skein external-shadow-adapter-smoke --require-previous-wrapper ...` first to
validate `ready`, `execute_session`, and `project_graph` wiring, but smoke output
does not count as production cutover evidence.

## LanceDB Replacement Surface

| Capability | Nowledge need | Skein status |
|---|---|---|
| Rebuildable projection | Search is derived, not source of truth | Partial: `SearchIndex` is separate from graph store, exposes report-oriented derived artifact rebuild, supports bounded incremental projection deltas for FTS/BM25 row upsert/delete without forcing full rebuilds, and publishes persistent projection snapshots through synced temp-file rename plus parent-directory sync while staying outside graph WAL |
| Vector search | semantic memory/entity/source search | Partial: exact cosine search, with child retriever input candidate-set reports proving metadata filters are applied before vector scoring and rank-window trimming |
| Metadata filters | scoped retrieval by projection metadata | Partial: exact-match metadata filters on graph-derived document metadata, with `kind` accepting canonical node labels or lowercase projection names, `external_id` using the projected node identity (non-empty `id` when present, otherwise canonical node id string) consistently for search hits, graph-native seeds, typed knowledge navigation, and graph context path endpoints, `source_id` using the same non-empty `source_id`/`thread_id`/`source` projection fallback for search hits and graph-native seeds, plus Nowledge normalized-space semantics for `space_id` missing/`NULL`/empty-string values as `default`, applied before vector scoring, BM25 corpus statistics, retriever candidate counts, rank-window trimming, and final truncation, with an exact projection-local candidate-set report carrying id space, representation, cardinality, filtered-out count, exactness, filter metadata, source graph snapshot epoch, and optional caller-supplied policy epoch metadata; the same request filters also scope graph-native seed candidates by label, external ID, and same-name scalar node properties, and `KnowledgeScopedEntityRequest`, `KnowledgeScopedEntityBatchRequest`, `KnowledgeScopedPropertyBatchRequest`, `KnowledgeScopedRelationshipsRequest`, `KnowledgeScopedNeighborsRequest`, `KnowledgeScopedPathRequest`, and `KnowledgeScopedSubgraphRequest` apply those filters to typed entity lookup, ordered bulk entity lookup, ordered property projection, ordered relationship lookup, neighborhood, path endpoint, and subgraph seed selection without changing the existing unscoped navigation API |
| FTS/BM25 | text search and non-vector fallback | Partial: BM25-style term-frequency, inverse-document-frequency, and length-normalized text scoring with case-insensitive Nowledge identifier tokenizer covering camelCase, acronym-to-titlecase technical identifiers, snake_case, kebab/path separators, numeric suffixes, adjacent chunk bigrams, conservative CJK bigrams/trigrams for Chinese/Japanese/Korean knowledge notes, conservative English suffix normalization, conservative English stopword filtering, selected graph-derived projection metadata identifiers (`kind`, `external_id`, `source_id`, `space_id`), configurable application-supplied analyzer lexicons with normalized phrase/identifier alias rules for Nowledge memory lifecycle/schema aliases such as `crystal`/`crystallization`, `episodic_provenance`/`raw_evidence`, `SOURCED_FROM`/`source_provenance`, `MENTIONS`/`entity_mention`, `EVOLVES`/`memory_evolution`, and `ai_summary`/`community_summary`, plus a conservative default technical alias set for knowledge-retrieval aliases (`rag`/`graph_rag`/`graph_retrieval`/`kg`), database-system aliases (`wal`, `mvcc`, `lsm`, `csr`/`csc`, `snapshot`/`checkpoint`), graph import/stream aliases (`GraphLightning`/`bulk_graph_import`, `GraphStream`/`graph_export`, `ContentStream`/`value_stream`, `projection_freshness`/`projection_staleness`), migration/projection aliases (`pg`/`postgres`/`postgresql`, `pgvector`/`vector_search`, `fts`/`full_text_search`, `lance`/`lancedb`, `kuzu`/`ladybug`), and retrieval-algorithm aliases (`rrf`/`reciprocal_rank_fusion`, `ann`/`approximate_nearest_neighbor`, `hybrid_retrieve`/`hybrid_retrieval`/`hybrid_search`); larger analyzer parity remains |
| Hybrid fusion | vector + FTS score fusion | Partial: weighted RRF-style vector/text fusion with optional rank-window budget and per-child vector/text ranks, child retriever input candidate-set reports, child RRF components, and child scores exposed on each hit |
| Retrieval explainability | score breakdown, provenance, projection freshness, and truncation reasons | Partial: search hits include fused RRF score, per-child RRF components, vector/text scores, vector/text ranks, fallback reasons, projection kind, external ID, source ID, matched analyzer terms, matched projection-text spans, document count, source graph commit epoch, projection rebuild/repair marker state and marker reasons, and embedding manifest freshness; incremental projection delta reports expose source graph commit epoch before/after plus whether the delta advanced the freshness watermark; `SearchIndex::search_with_report` exposes total and post-filter document counts, exact candidate-set report, child retriever availability, child fallback reasons for empty text queries and missing or incompatible vector legs, candidate counts, child output candidate-set reports, top hit IDs, top candidate ranks/scores, total pre-limit matches, requested limit, rank window, fusion weights, truncation flag, truncation reasons, machine-readable empty reason codes with stable string encodings, search-level fallback reasons with stable fallback reason codes, and stable search truncation reason codes; Knowledge Retrieval diagnostics lift search empty reason codes into retrieval-level empty reason codes, add graph-seed/candidate empty codes with stable string encodings, expose exact graph-seed input and output candidate-set reports with metadata-filter cardinality/filter-out counters, expose exact graph-context input seed-node and output expanded-relationship candidate-set reports, expose stable truncation reason codes for rank-window, search-limit, graph-seed, graph-context, and candidate-budget truncation, expose stable graph-seed/graph-context fallback reason codes for disabled retrieval budgets, and expose stable fan-out reason codes plus structured fan-out details emitted from typed fan-out events for dense adjacency and bounded retrieval/traversal limits so callers do not parse English diagnostics strings; typed knowledge navigation diagnostics copy fan-out reasons alongside counts, expose traversal input/output candidate-set reports for neighbors, paths, and subgraph expansion, and report missing source/target identity, disabled traversal budgets, and unknown relationship-type fallbacks with stable reason codes so callers do not have to join top-level traversal output back into diagnostics or parse strings |
| Dimension checks | model/dimension changes require rebuild | Partial: persisted embedding model/version/dimension manifests, row dimension validation, query dimension mismatch degradation, and full-reindex marker on model or dimension changes |
| Markers | `.reindex_needed`, `.projection_metadata_repair_needed` | Partial: in-memory and path-backed marker read/write with reason preservation; path-backed projections persist marker files |
| Fail-soft legs | stale vector or FTS should not drop all results | Partial: vector mismatch degrades to text |
| Metadata repair | bounded metadata-only backfill | Partial: graph-derived metadata repair without rewriting content or embeddings, with rankable `Projection` work plans plus stateless `LocalQosPolicy` and caller-driven `LocalQosScheduler` wrappers for internal background repair, plus `Database::repair_search_projection_metadata`, `Database::repair_background_search_projection_metadata`, and `Database::repair_scheduled_background_search_projection_metadata` facade methods over canonical graph evidence |
| Full rebuild orchestration | bounded replacement from authoritative graph | Partial: bounded graph-to-search rebuild with all-or-nothing in-memory replacement, graph-derived incremental projection deltas from canonical node IDs with explicit complete-through source graph commit epoch freshness stamping and report-level watermark before/after fields, rankable `Projection` work plans, background/scheduled QoS wrappers, and `SearchIndex::rebuild_derived_artifacts` reporting of document counts, scanned nodes, lifecycle-marker state, and lifecycle-marker reasons |
| Resource and QoS budgets | embedded devices should not starve foreground reads | Partial: foreground user requests are not locally gated by background budgets; search projection rebuild and metadata repair accept row budgets, full search rebuilds, incremental projection deltas, graph-derived incremental projection deltas, and metadata repair can expose rankable `Projection` work plans, incremental projection deltas accept operation budgets and fail without partially mutating the index, `Database::search_projection_rebuild_background_work_plan` exposes graph-derived full search rebuild estimates for caller-owned scheduling, `Database::rebuild_background_search_projection` and `Database::rebuild_scheduled_background_search_projection` gate caller-owned full search projection rebuilds through `LocalQosPolicy` and `LocalQosScheduler`, `Database::search_projection_metadata_repair_background_work_plan` exposes graph-derived metadata repair estimates for caller-owned scheduling, `Database::repair_background_search_projection_metadata` and `Database::repair_scheduled_background_search_projection_metadata` gate metadata-only projection repair through the same QoS surfaces, `Database::search_projection_graph_delta_freshness_background_work_plan` derives rankable graph-delta work hints from explicit recent delta operations and source graph commit lag, `Database::search_projection_freshness_lag_background_work_plan` and unified background maintenance candidates can surface stale search projection graph-delta work from commit lag without requiring the caller to prebuild a delta request, `Database::apply_background_search_projection_graph_delta` applies internal graph-derived projection deltas through `LocalQosPolicy`, `Database::apply_scheduled_background_search_projection_graph_delta` tracks in-flight internal graph-derived projection delta work through `LocalQosScheduler`, `SearchIndex::apply_background_projection_delta` and `Database::apply_background_search_projection_delta` apply internal projection deltas through `LocalQosPolicy`, `SearchIndex::apply_scheduled_background_projection_delta` and `Database::apply_scheduled_background_search_projection_delta` track in-flight internal background projection delta work through `LocalQosScheduler`, graph-derived metadata repair exposes the same background admission and scheduler wrappers, composite/full-text property-index projection rebuilds can expose a rankable `Projection` work plan and use bounded background/scheduled wrappers, Graph Lightning bootstrap export exposes an `Import` work plan plus background/scheduled wrappers while direct caller exports remain ungated and unified background maintenance ranking can include its pre-export candidate, external content parser/crawler jobs can be charged to the `Import` background lane, database-owned derived artifact rebuild jobs can use `Database::run_next_background_derived_artifact_job` for the same internal background admission while explicit callers keep the direct runner, `LocalQosScheduler` tracks in-flight internal background operation budgets plus optional per-class background budgets for caller-driven projection/import/analytics/shadow lanes without owning worker threads, `BackgroundWorkPlan` and `BackgroundWorkHint` let caller-owned loops rank background work by expected-value signals such as active topic, recent delta size, query probability, source graph commit lag, staleness TTL, freshness SLO, and tenant budget before attempting admission, schema maintenance can expose a rankable `Mutation` work plan from its current dry-run estimate, `Database::background_maintenance_candidates`, `Database::rank_background_maintenance`, and `Database::background_maintenance_summary` gather named schema/property-index/search/Graph Lightning/external-content candidates with stable parseable typed kinds plus admitted/deferred/rejected operation totals for caller-owned multi-queue loops without starting workers, `LocalQosPolicy::rank_background_work` and `LocalQosScheduler::rank_background_work` provide deterministic admitted-first ordering over caller-owned candidate lists with stable background work reason codes, `WorkClass` and `WorkPriority` expose stable parseable lane and priority string encodings, `QosAdmissionCode` exposes stable parseable string encodings for background defer/reject categories without parsing human-readable reasons, `LocalQosPolicy` admits foreground work while deferring oversized or disabled internal background work, and Knowledge Retrieval exposes search/rank-window/graph-seed/graph-context/candidate budgets, fallback reasons, and truncation reasons; worker ownership remains caller-owned |
| Multi-table projections | memories/messages/entities/sources/chunks/communities | Partial: typed projection rows for memory/message/entity/source/source chunk/community |
| Knowledge Retrieval facade | application-facing retrieval over graph-derived projections | Partial: `Database::rebuild_search_projection` derives a caller-owned search projection from canonical graph evidence, `Database::rebuild_background_search_projection` and `Database::rebuild_scheduled_background_search_projection` expose the same full rebuild behind caller-owned background QoS admission, `DatabaseReadTransaction::rebuild_search_projection` and `DatabaseReadTransaction::repair_search_projection_metadata` derive the same projection maintenance inputs from a pinned graph snapshot, `Database::build_search_projection_graph_delta` and graph-delta apply wrappers derive bounded caller-owned incremental search projection updates from canonical node IDs while preserving explicit complete-through source graph commit epoch freshness, `Database::retrieve_knowledge` returns graph commit epoch, projection freshness, metadata-filtered search hits and graph seeds, unified vector/text/graph-seed retriever reports with child-level limits, rank windows, fusion weights, child output candidate-set reports, fallback reasons for unavailable search legs and disabled graph-seed legs, truncation reasons, top-candidate ranks, scores, provenance metadata, canonical node IDs, matched spans, graph context path counts, and search-child projection freshness, compact retrieval diagnostics with search scope counts, exact search candidate-set reports, search candidate filter-out counts, search/rank-window/fusion-weight/graph-seed/graph-context/candidate budgets, search truncation flag/reasons, search fallback reasons, graph seed counts, graph-seed truncation reasons, graph context path/node/relationship counts, graph-context fallback reasons for disabled budgets, fan-out reason count and messages, returned and pre-limit merged candidate counts, response-level candidate truncation flag/reasons, graph-context truncation flag/reasons, projection source graph commit epoch, projection commit lag, stale projection warnings, projection marker warnings, structured projection stale/full-reindex/metadata-repair flags and marker reasons, and empty-result reasons that distinguish metadata misses, search fallback causes, disabled retriever budgets, and search or response-candidate budget exhaustion, a typed `KnowledgeCandidate` surface that exposes canonical node IDs and merges search-hit and graph-seed candidates by canonical graph identity with response-level candidate budgeting, candidate score breakdown, max and weighted-sum candidate scoring policies, per-hit evidence summaries with source IDs, canonical node IDs, score components, child RRF components, matched terms, matched projection-text spans, and graph context path counts, bounded graph-native seed results over canonical nodes, bounded multi-hop graph context paths with relationship properties for both search hits and graph-native seeds with per-seed relationship de-duplication, search truncation diagnostics plus search projection empty-result reasons for empty projections, metadata-filter misses, fallback causes, no matching rows, and limit-zero empty returns, optional hybrid rank-window, fusion-weight, and fallback diagnostics, and graph fan-out reasons without storing search state in the graph WAL, `Database::knowledge_entity` exposes direct canonical entity lookup by label/external ID, `Database::knowledge_entity_batch` exposes ordered bulk canonical entity lookup with found/missing counters, `Database::knowledge_scoped_entity` adds metadata-filtered direct entity lookup, `Database::knowledge_scoped_entity_batch` adds metadata-filtered ordered bulk entity lookup with filtered-out counters, `Database::knowledge_property_batch` exposes ordered bulk property projection with stable requested property keys, `Database::knowledge_scoped_property_batch` adds metadata-filtered ordered bulk property projection with filtered-out counters, `Database::knowledge_relationships` exposes grouped one-hop relationship lookup for ordered seed lists, `Database::knowledge_scoped_relationships` adds metadata-filtered seed selection for grouped one-hop relationship lookup, `Database::knowledge_neighbors` exposes bounded typed neighborhood expansion by label/external ID, relationship type, direction, hop count, and limit with traversal diagnostics including distinct returned path-node and relationship counts plus disabled-budget fallback reasons, `Database::knowledge_scoped_neighbors` adds metadata-filtered typed neighborhood seed selection with filter metadata and filtered-out counts in traversal diagnostics, `Database::knowledge_paths` exposes bounded typed path lookup between two graph identities with source/target presence plus distinct returned path-node, relationship, and path-count diagnostics, `Database::knowledge_scoped_paths` adds separate source and target metadata-filtered endpoint selection with prefixed filter metadata and filtered-out counts in traversal diagnostics, `Database::knowledge_subgraph` exposes bounded typed subgraph expansion with node/relationship limits and traversal diagnostics without requiring a search projection, `Database::knowledge_scoped_subgraph` adds metadata-filtered typed subgraph seed selection with the same traversal diagnostics contract, and `DatabaseReadTransaction` exposes the same Knowledge Retrieval facade and typed knowledge navigation over a pinned graph snapshot |

Typed mutation coverage now also includes normalized-space batch moves for
Nowledge Memory, Source, Thread, and ThreadIdentity-style `id` or `thread_id`
lists. The wrapper validates label and identity-property identifiers, applies
the Nowledge rule that missing, `NULL`, and empty `space_id` map to `default`,
supports optional source-space filtering and target-space no-op reporting,
deduplicates pending node writes, returns moved external IDs in caller order,
can stamp `updated_at`, and commits eligible rows through one grouped WAL batch.
It also includes a typed Memory access touch batch for Nowledge
`mark_memories_accessed` and click-dwell writes. The wrapper increments
`access_count`, `clicks`, and `total_dwell_time_ms` through Cypher
`COALESCE(..., 0) + ...` assignments inside one transaction, updates
`last_accessed_at` and `last_clicked_at`, reports missing/idless rows without
writing, and preserves duplicate same-memory touches as separate increments in
the same grouped WAL batch.
Source provenance count writes are covered by a typed Source memory-count
adjustment batch for Nowledge `memory_count + 1` and floor-to-zero decrement
paths. The wrapper validates non-empty Source ids and non-zero deltas before
WAL, treats missing or `NULL` `memory_count` as zero, rejects non-integer
current counts without writing that row, preserves duplicate same-source
adjustments in request order, materializes floor-decrement results as `0`, and
commits eligible per-source updates through one grouped WAL batch.
Source lifecycle writes are covered by a typed batch for Nowledge extracted
mark-indexed, indexed `chunk_count`, and direct lifecycle-state updates. The
wrapper validates Source ids, target/current lifecycle states, and non-negative
chunk counts before WAL, applies optional current-state filtering, reports
missing/idless/duplicate rows without writing, writes `lifecycle_state`,
optional `chunk_count`, and `updated_at`, and commits eligible Source rows
through one grouped WAL batch.
Source operational reads are covered by typed APIs for Nowledge source detail,
source count, extracted-source id list, and normalized-space id list paths.
`Database::knowledge_source` returns the Source identity, display fields,
normalized space, lifecycle fields, size/count fields, timestamps, and
`SOURCED_FROM` Memory count for one Source id. `Database::knowledge_source_ids`
returns sorted Source ids filtered by lifecycle state and/or normalized space
with bounded limits, while `Database::knowledge_source_count` exposes the total
Source node count. These reads report the graph commit epoch and do not write
WAL.
Source list and summary reads are covered by `Database::knowledge_sources` for
Nowledge bounded Source page, bulk summary, memory-count overview ranking,
parsed-path list, lifecycle attention, and metadata-marker page shapes. The
typed read supports id-bounded bulk rows with missing-id reporting, after-id
pagination, lifecycle-state sets, normalized-space and source-type filters,
metadata substring markers, parsed-path-only selection, offset/limit, Source id,
memory-count, or created-at ordering, display-name and numeric fallback fields,
and no WAL writes.
Source attribution reads are covered by `Database::knowledge_source_memories`.
The typed read resolves one Source id, scans incoming `SOURCED_FROM` Memory
edges, returns Memory id/title/content/unit type/confidence plus chunk
index/range/source version/created-at relationship metadata ordered by chunk
index and Memory id, supports bounded limits, reports found/matched/returned
counts and the graph commit epoch, and does not write WAL.
Bulk Memory/Source attribution reads are covered by
`Database::knowledge_memory_source_attributions`. The typed read scans
`SOURCED_FROM` edges by bounded Memory ids, Source ids, or both, returning
Memory and Source endpoint ids, relationship ids, chunk metadata, Source display
fields, Memory display title/content preview/rank/community/space/source/time
fields, missing-id reporting, bounded limits, and no WAL writes.
Memory lifecycle metadata writes are covered by a typed batch for the Nowledge
`metadata`, `is_latest`, `lifecycle_state`, and `updated_at` update shape. The
wrapper validates Memory ids and non-empty lifecycle states before WAL, reports
missing/idless/duplicate rows without writing, and commits eligible Memory rows
through one grouped WAL batch.
Crystal Memory reads are covered by `Database::knowledge_crystals`. The typed
read scans only `Memory` nodes with `is_crystal = true`, supports wiki key
lookup by exact/prefix/contains id matching, crystal page `id > after`
pagination, OKF importance/created-at ordering, display-title fallback from
`crystal_title` to `title`, read-transaction snapshots, and no WAL writes.
Crystal community aggregation reads are covered by
`Database::knowledge_crystal_communities`. The typed read scans only
`is_crystal = true` Memory nodes, follows `SYNTHESIZED_FROM` to source Memory
nodes and `MENTIONS` to Entity nodes, filters by explicit community ids or
non-null communities, returns hit counts and distinct source-memory counts for
each crystal/community pair, supports topic-ranking and OKF mapping orderings,
read-transaction snapshots, and no WAL writes.
Crystal source visibility reads are covered by
`Database::knowledge_crystal_source_visibility`. The typed read uses the same
Crystal/source/entity community path but preserves one row per visible path,
returning Crystal fields plus source Memory metadata, `COALESCE(is_latest,
true)` semantics, and lifecycle state for Nowledge wiki community crystal
rendering; it supports explicit community-id scopes, read-transaction
snapshots, and no WAL writes.
Synthesized-source coverage lookups are covered by
`Database::knowledge_synthesized_source_coverage`. The typed read validates an
explicit non-empty source Memory id set and positive required distinct coverage
count, scans only `Memory` crystals with outgoing `SYNTHESIZED_FROM` Memory
sources, de-duplicates repeated source relationships, returns matching crystal
ids/titles plus matched source ids for the Nowledge `cid` and `cid, ct`
coverage lookup shapes, supports read-transaction snapshots, and does not write
WAL.
Memory entity mention reads are covered by
`Database::knowledge_memory_entities`. The typed read validates a non-empty
Memory id list, resolves each Memory in caller order, scans outgoing `MENTIONS`
edges to Entity nodes, returns Entity id/name/type/confidence plus relationship
confidence and mention count rows sorted by Entity name/id/relationship id,
supports per-Memory limits and distinct Entity name limits, reports
found/missing Memory counts and the graph commit epoch, and does not write WAL.
Entity mention-count list reads are covered by
`Database::knowledge_entity_mention_counts`. The typed read scans only `Entity`
nodes with non-empty `id` and `name`, counts incoming `MENTIONS` relationships
from `Memory` nodes while preserving zero-mention Entities, returns
id/name/updated_at/mention_count rows ordered by mention count descending then
name ascending, supports the Nowledge cursor predicate over count/name,
bounded limits, read-transaction snapshots, and no WAL writes.
Community Entity visibility reads are covered by
`Database::knowledge_community_entity_visibility`. The typed read scans Entity
nodes in explicit community scopes and preserves the Nowledge optional incoming
`Memory` `MENTIONS` row shape, including zero-Memory Entity rows, Memory
metadata, `COALESCE(is_latest, true)` semantics, lifecycle state,
read-transaction snapshots, and no WAL writes.
Community Memory ranking reads are covered by
`Database::knowledge_community_memories`. The typed read validates explicit
non-null community scopes, returns Nowledge wiki ranking/export rows from either
incoming `Memory` `MENTIONS` over Entity communities or direct
`Memory.community_id` assignment, preserves distinct mentioned Entity ids,
supports false-only and null-or-false crystal filters plus Nowledge
`unit_type IN $types` filters, applies importance and latest-state fallbacks,
supports the Nowledge ordering variants, read-transaction snapshots, and no WAL
writes.
Related Entity name reads are covered by
`Database::knowledge_related_entity_names`. The typed read validates either a
non-empty Memory id list or one Thread id with physical `id` or logical
`thread_id` identity, returns distinct non-empty `Entity.name` values in sorted
order for the Nowledge REST list `Memory` id and `Thread` `COMPACTS_TO` ->
`MENTIONS` shapes, reports missing Memory ids or missing Thread status,
supports bounded limits, and does not write WAL.
Context memory preview reads are covered by
`Database::knowledge_context_memory_preview`. The typed read validates
non-empty unit types, applies the Nowledge context-wiring filters for latest
Memory rows and non-crystal rows, orders by `created_at` descending, supports
bounded limits, can either return Memory title/unit-type preview rows or expand
outgoing `HAS_LABEL` rows to Label id/canonical-name/name fields, reports
matched Memory counts and the graph commit epoch, and does not write WAL.
Memory bulk detail and filtered list reads are covered by
`Database::knowledge_memories`. The typed read supports id-bounded bulk detail
rows, normalized-space inclusion/exclusion using the Nowledge default-space
rule, unit-type/latest/crystal filters, created-at or score ordering, and
bounded limits. It returns Memory title/content/metadata/lifecycle/review/
space/timestamp/source/rank fields, reports missing ids and the graph commit
epoch, rejects unbounded scans without filters, and does not write WAL.
Memory title/content id-list reads are covered by
`Database::knowledge_memory_title_contents`. The typed read accepts Memory ids,
returns title/content rows ordered by `created_at` ascending for the REST Skills
write-path source preview, reports missing Memory ids, supports pinned read
snapshots, and does not write WAL.
Memory EVOLVES latest reads are covered by
`Database::knowledge_memory_evolves_latest`. The typed read accepts old Memory
ids, follows outgoing `EVOLVES` edges to Memory targets, returns distinct
new-memory id/latest-state rows for the REST Skills successor check, reports
matched/missing old Memory ids and relationship counts, supports pinned read
snapshots, and does not write WAL.
Skill usage-stat writes are covered by a typed batch for Nowledge
`use_count`, optional `success_rate`, `last_activity_at`, `updated_at`, and
`metadata` update shapes. The wrapper validates Skill ids, non-negative use
counts, and bounded numeric success rates before WAL, reports missing,
idless, and duplicate rows without writing, and commits eligible Skill rows
through one grouped WAL batch.
Skill lifecycle/write-state updates are covered by a typed batch for Nowledge
stage changes, rejection timestamps, promotion rationale, compiled version
metadata, draft bundle writes, content hashes, bundle paths, triggers, tools,
write origin, and `updated_at` stamping. The wrapper validates Skill ids,
non-empty stages, and non-empty write origins before WAL, reports missing,
idless, and duplicate rows without writing, and commits eligible Skill rows
through one grouped WAL batch.
REST Skills source merges are covered by
`Database::merge_knowledge_skill_source`. The typed write resolves exact
physical `Skill.id` and `Memory.id` endpoints, merges the outgoing
`SYNTHESIZED_FROM` edge, creates only the Nowledge-used `weight`,
`occasion_key`, and `created_at` relationship properties, reports missing
endpoints and idless endpoints without writing, preserves existing-edge
properties for `MERGE ON CREATE SET` semantics, and uses the WAL-backed
relationship write path.
Skill catalog/detail reads are covered by `Database::knowledge_skills`.
The typed read supports exact id lists, key lookup using exact/prefix/contains
matching, stage-filtered catalog and active lists, active after-id pagination,
updated-at or id ordering, missing-id reporting, and pinned read snapshots
without WAL writes.
REST FS Skill detail lookup is covered by
`Database::knowledge_skill_detail_lookup`. The typed read applies the
Nowledge REST FS physical `Skill.id` exact/prefix/contains lookup, returns the
first stable node-id ordered id/name/title/stage/version/created-at/updated-at
projection, reports total matched Skill rows for ambiguity diagnostics,
supports pinned read snapshots, and does not write WAL.
REST Skills exact write-state reads are covered by
`Database::knowledge_skill_state`. The typed read resolves one physical
`Skill.id` without prefix/contains fallback, returns stage/metadata/version/
use-count/bundle-path/content-hash/name/description/title fields for metadata,
version, title, and full write-state call sites, supports pinned read snapshots,
and does not write WAL.
Skill evidence-memory reads are covered by `Database::knowledge_skill_memories`.
The typed read requires either one Skill id or a non-empty stage filter, scans
outgoing `SYNTHESIZED_FROM` edges to Memory nodes, returns Memory id/title/
content/unit type/created-at fields plus Skill and relationship identities,
supports `created_at` ascending or descending ordering with bounded limits,
reports matched/missing Skill counts and the graph commit epoch, and does not
write WAL.
Skill context thread-source reads are covered by
`Database::knowledge_skill_thread_sources`. This is a Nowledge `(:Skill)`
business-node facade, not a graph-kernel builtin: it resolves one Skill id,
follows `SYNTHESIZED_FROM` evidence Memory nodes and incoming `COMPACTS_TO`
Thread nodes, de-duplicates repeated Memory/Thread pairs, returns Thread
title/source rows with stable ordering, supports pinned read snapshots, and
does not write WAL.
Thread metadata writes are covered by a typed batch for Nowledge `metadata`
updates with optional `updated_at` stamping. The wrapper validates Thread ids
before WAL, reports missing, idless, and duplicate rows without writing, keeps
metadata-only updates from changing existing timestamps, and commits eligible
Thread rows through one grouped WAL batch.
Thread denormalized message-count writes are covered by a typed batch for
Nowledge `message_count` refreshes with optional `updated_at` stamping and
`preserve_newer_existing_updated_at` semantics. The wrapper validates Thread
ids and non-negative counts before WAL, keeps newer existing timestamps when
requested, reports missing, idless, and duplicate rows without writing, and
commits eligible Thread rows through one grouped WAL batch.
Thread ordered message reads are covered by
`Database::knowledge_thread_messages`. The typed read resolves one Thread id,
scans outgoing `CONTAINS` Message edges, returns Message id/role/content/order
index/timestamps/token count/metadata plus relationship id and relationship
order index, orders by `COALESCE(c.order_index, m.order_index)`, supports
bounded limits, reports found/matched/returned counts and the graph commit
epoch, and does not write WAL.
Thread compacted-memory reads are covered by
`Database::knowledge_thread_compacted_memories`. The typed read resolves one
Thread by physical `id` or logical `thread_id`, scans outgoing `COMPACTS_TO`
Memory edges, returns Memory id/title/content previews, rank/time/space/
review/reindex/temporal/access fields, relationship metadata, count/id-list/
summary/full-row compatible fallbacks, importance/created-at ordering, bounded
limits, and no WAL writes.
Memory compacting-Thread reads are covered by
`Database::knowledge_memory_compacting_threads`. The typed read resolves
bounded Memory ids, scans incoming `COMPACTS_TO` Thread edges, returns Thread
physical/logical ids, title, source, metadata, normalized space, relationship
ids, missing-Memory rows, per-Memory limits, and no WAL writes.
Thread list and source reads are covered by `Database::knowledge_threads` for
Nowledge bounded Thread page, source lookup, source page, normalized-space
count/list, favorite metadata page, id/thread-id bulk lookup, and
message-count ranking shapes. The typed read supports physical `id` and
logical `thread_id` filters, lookup-key matching, source filters,
normalized-space filters, metadata substring markers, after-id pagination,
thread-id presence filtering, offset/limit, id, thread-id, message-count, and
recent-update ordering, display-title and message-count fallbacks, missing-id
reporting, and no WAL writes.
Distinct Thread source listing is covered by
`Database::knowledge_thread_sources`. The typed read scans Thread nodes,
filters missing and empty `source` values, returns sorted distinct source
strings with optional bounded truncation, supports pinned read snapshots, and
does not write WAL.
Thread attachment title lookup is covered by
`Database::knowledge_thread_title`. The typed read resolves exact physical
`id` or logical `thread_id`, returns the first stable node-id ordered title for
the Nowledge REST agent attached-source shape, reports total matched Thread
rows for ambiguity diagnostics, supports pinned read snapshots, and does not
write WAL.
Thread source summary lookup is covered by
`Database::knowledge_thread_source`. The typed read resolves exact physical
`id` or logical `thread_id`, returns the first stable node-id ordered
thread-id/title/source/created-at projection for the Nowledge REST export
source-thread shape, reports total matched Thread rows for ambiguity
diagnostics, supports pinned read snapshots, and does not write WAL.
Thread message-render lookup is covered by
`Database::knowledge_thread_message_lookup`. The typed read applies the
Nowledge REST FS `id = key OR id STARTS WITH key OR id CONTAINS key` lookup
against Thread physical ids, keeps the exact source filter, returns the first
stable node-id ordered id/message-count/raw-space projection, preserves raw
empty `space_id`, reports total matched Thread rows for ambiguity diagnostics,
supports pinned read snapshots, and does not write WAL.
Thread metadata-render lookup is covered by
`Database::knowledge_thread_meta_lookup`. The typed read applies the same
Thread physical-id exact/prefix/contains plus exact-source REST FS lookup,
returns the first stable node-id ordered id/thread-id/title/summary/
message-count/source/created-at/updated-at/raw-space/project/workspace
projection, preserves raw empty `space_id`, reports total matched Thread rows
for ambiguity diagnostics, supports pinned read snapshots, and does not write
WAL.
ThreadIdentity exact resolution is covered by
`Database::knowledge_thread_identity`. The typed read resolves one
`ThreadIdentity` by external id, returns the Nowledge repo fields
`thread_node_id`, `thread_id`, normalized/raw space, source, identity node id,
missing-identity state, supports pinned read snapshots, and does not write WAL.
Thread sync metadata reads are covered by
`Database::knowledge_thread_sync_metadata`. The typed read resolves one Thread
by physical `id`, returns title/source/project/workspace/space strings with
the same `COALESCE(..., '')` and `COALESCE(space_id, 'default')` fallback
semantics as the Nowledge repo query, reports missing-Thread state, supports
pinned read snapshots, and does not write WAL.
Label lifecycle writes are covered by a typed batch for Nowledge metadata
updates, canonical-name backfill, and rename/canonical-name updates. The
wrapper validates Label ids, non-empty names, and non-empty canonical names
before WAL, reports missing, idless, and duplicate rows without writing, and
commits eligible Label rows through one grouped WAL batch.
Label canonical and usage reads are covered by typed APIs for Nowledge label
merge and list surfaces. `Database::lookup_knowledge_labels_by_canonical_name`
handles duplicate/collision checks, `Database::scan_knowledge_labels_missing_canonical_name`
handles canonical backfill scans, and `Database::knowledge_label_usage` plus
`Database::knowledge_label_canonical_usage` expose single-row and canonical
usage rows with `HAS_LABEL` counts over any source node type.
`Database::knowledge_label_memory_distribution` covers the Nowledge label
distribution and OKF label-row stats shapes by counting distinct Memory nodes
per Label over `HAS_LABEL`, de-duplicating repeated edges, sorting by
Memory-count descending then label name ascending, supporting offset/limit, and
not writing WAL.
Endpoint-known `HAS_LABEL` assignment reads are covered by
`Database::knowledge_entity_labels` for Nowledge Memory, Source, Entity, and
other id-bearing graph identities. The typed read validates the entity label
and non-empty external id list, resolves each input identity in caller order,
returns Label id/name/canonical-name/color/description rows sorted by label
name/id/node id with optional per-entity limits, reports found/missing entity
counts and the graph commit epoch, and does not write WAL.
Induced edge-list reads are covered by `Database::knowledge_induced_edges` for
Nowledge overview and MCP subgraph edge-list shapes. The typed read validates a
non-empty external id set, scans canonical relationships whose source and
target endpoint ids are both in that set, returns endpoint ids/node ids,
relationship id/type, and `strength`/`confidence`/default weight, supports
bounded limits, reports missing external ids and the graph commit epoch, and
does not write WAL.
PageRank score writes are covered by typed batches for Nowledge Memory and
Entity `pagerank_score` persistence and clear operations. The wrapper accepts
only finite non-negative scores for Memory/Entity identities, reports missing,
idless, duplicate, and clear-only non-writable rows without writing, and commits
eligible score writes or clears through one grouped WAL batch.
PageRank plan and read-side helpers are also covered by typed APIs:
`Database::knowledge_pagerank_plan` exposes the Nowledge Memory/Entity node
counts, `MENTIONS`, `RELATES_TO`, active `MEMORY_RELATES_TO`, and cutoff-based
changed-count shapes; `Database::knowledge_pagerank_membership` covers
Memory/Entity id membership splitting; `Database::knowledge_pagerank_memory_visibility`
covers default-visible Memory metadata/latest checks; and
`Database::knowledge_pagerank_central_entity` covers the central-entity name
lookup without constructing application-side Cypher. These reads report the
current graph commit epoch and do not write WAL.
GraphMeta algorithm stamps are covered by a typed batch for Nowledge PageRank
and community-detection state updates shaped as `MERGE (m:GraphMeta {meta_id})
SET ...`. The wrapper validates non-empty `meta_id` values and property names,
rejects attempts to mutate `meta_id`, creates missing GraphMeta rows, updates
existing rows, reports duplicate stamps without writing, and commits eligible
stamps through one grouped WAL batch.
GraphMeta state reads and cleanup deletes are covered by
`Database::knowledge_graph_meta` and `Database::delete_knowledge_graph_meta`.
Both use the Nowledge `meta_id` identity rather than the generic `id`
property; deletes reject empty identities before WAL, do not write WAL for
missing rows, and persist eligible cleanup through the WAL-backed `DELETE`
path.
Schema migration log writes are covered by a typed create-once batch for
Nowledge `SchemaMigrationLog` ids shaped as `MERGE ... ON CREATE SET
applied_at`. The wrapper validates non-empty migration ids before WAL, reports
already-applied and duplicate rows without writing, preserves existing
`applied_at` values, and commits eligible new migration rows through one grouped
WAL batch.
Schema migration log reads are covered by
`Database::knowledge_schema_migrations`, which returns applied migration ids
with optional `applied_at`, deterministic id ordering, bounded limits, and the
current graph commit epoch without writing WAL.
AugmentationJob lifecycle writes are covered by a typed batch for Nowledge job
creation, pending-to-running starts, running progress updates,
running-to-completed results, and pending/running-to-failed errors. The wrapper
validates job ids, job types, progress percentages, progress messages, and
failure messages before WAL, reports missing, existing, status-mismatched, and
duplicate jobs without writing, and commits eligible creates/updates through
one grouped WAL batch.
AugmentationJob status and list reads are covered by typed APIs for Nowledge
graph and REST graph surfaces. `Database::knowledge_augmentation_job` resolves
one job by `job_id`, while `Database::knowledge_augmentation_jobs` supports the
production filtered/all list shapes with `started_at DESC` or `created_at DESC`
ordering and bounded limits without constructing application-side Cypher.
AugmentationJob stale/orphan interrupt writes are covered by
`Database::interrupt_knowledge_augmentation_jobs` for the Nowledge
`interrupt_orphaned_jobs` shape. The wrapper scans only `AugmentationJob`
nodes in `pending` or `running` state, validates the interruption reason before
WAL, marks eligible jobs as `failed` with the production interruption message,
does not write WAL when no eligible jobs exist, and commits eligible updates
through one grouped WAL batch.
Source-reference relationship cleanup is covered by a typed API for Nowledge
memory/source delete flows. `Database::delete_knowledge_source_reference_relationships`
scans only `RELATES_TO.source_reference`, rejects empty references before WAL,
preserves endpoint Entity nodes, and commits eligible relationship deletes
through one grouped WAL batch.
Entity-to-Community membership writes are covered by
`Database::create_knowledge_community_memberships_batch` for the Nowledge
entity lifecycle `BELONGS_TO` creation shape. The wrapper validates non-empty
Entity and Community ids plus finite strengths before WAL, reports missing or
non-writable endpoints, preserves endpoint nodes, and commits eligible
memberships through one grouped WAL batch.
Community detection result creation and scheduler summary refresh writes are
covered by `Database::update_knowledge_communities_batch`. The wrapper validates
non-empty Community ids and names, non-negative `community_id` and
`member_count`, and finite `resolution` before WAL, fixes created communities
to the Nowledge `louvain` algorithm marker, reports existing, missing,
duplicate, and non-writable rows, and commits eligible creates plus summary
updates through one grouped WAL batch.
Community summary list reads are covered by `Database::knowledge_communities`.
The typed read scans only `Community` nodes, supports the Nowledge REST
summary-only list and library summary-presence ranked shapes, filters optional
non-negative `community_id`, orders by member count or summary presence then
member count, returns id/community_id/name/description/ai_summary/member_count/
updated_at fields, supports bounded limits and read-transaction snapshots, and
does not write WAL.
Community detail reads are covered by `Database::knowledge_community`. The
typed read scans only `Community` nodes, supports the Nowledge wiki/MCP single
row lookup shapes by numeric `community_id` or external `id`, returns the same
Community row fields as the list facade plus summary-presence metadata,
reports found/missing state, supports read-transaction snapshots, and does not
write WAL.
Community node cleanup is covered by `Database::delete_knowledge_communities`
for Nowledge replace-community and undo-community flows. It scans only
`Community` nodes, supports the two production cleanup modes (`DELETE` and
`DETACH DELETE`), preserves non-Community endpoint nodes under detach cleanup,
does not write WAL when no Community nodes exist, and commits eligible deletes
through one grouped WAL batch.

## Current Direction

Skein should keep graph and search separated:

```text
canonical graph store
  nodes, relationships, schema tokens, WAL, checkpoint

search projection
  text, embeddings, denormalized filter metadata, lifecycle markers

content store
  large payloads, source chunks, message bodies
```

This matches the existing Nowledge invariant: graph identity is durable,
search is rebuildable, and large content is not duplicated into the graph store.
Resource-constrained embedded deployments should consume background maintenance
summaries through typed QoS hint fields, not by parsing human-readable ranking
reasons.

## Next Implementation Slices

1. Add page-level MVCC reader isolation and physical page/segment reclamation
   once the physical page/segment format exists.
2. Add richer cross-pattern statistics and cross-pattern workload-shaped
   optimizer benchmark suites.
3. Add larger text analyzer parity beyond the current identifier, suffix,
   stopword, and knowledge-retrieval alias set.
4. Add richer caller-owned blob/content parser runtime integrations on top of
   the current graph-kernel-external derived job boundary.
5. Use `ExternalShadowCommand` with the previous local graph wrapper only when
   compatibility evidence is needed for a specific migration gate.
6. Use `nowledge-replacement-summary --require-production-ready` as the final
   reporting guard for production replacement notes after the migration-gate
   bundle has been generated with previous-wrapper, storage recovery, and
   background-maintenance evidence.
7. Continue the chryso-style crate split beyond the current `core` crate once
   parser, planner, optimizer, store, and search contracts stabilize.
