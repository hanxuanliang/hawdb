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
| Storage version | Boot compatibility checks | Partial: `storage_version()` |
| Cypher reads | `MATCH`, `WHERE`, `RETURN`, parameters later | Partial: single-node `MATCH` plus one-hop outgoing relationship expansion |
| DDL/schema | Node/rel labels and migrations | Partial: catalog tokens only |
| Node mutations | `CREATE`, `MERGE`, `SET`, `DELETE` | Partial: `CREATE` node only |
| Relationship mutations | relationship tables/groups | Partial: `CREATE (:Label {...})-[:TYPE {...}]->(:Label {...})` |
| Transactions | explicit begin/commit/rollback | Partial: mutation-only `DatabaseTransaction` with commit/rollback over grouped WAL |
| WAL recovery | crash-loop recovery and torn tail handling | Partial: WAL replay with checksum/torn-tail stop and atomic batch replay |
| Checkpoint | explicit checkpoint and log pruning | Partial: full snapshot checkpoint and WAL truncate |
| Property indexes | selective equality lookups and index-backed plans | Partial: rebuildable in-memory equality index with `IndexNodeSeek` |
| Read concurrency | shared readers, exclusive writes/control | Missing |
| Projected graph | `PROJECT_GRAPH`, page_rank, louvain | Missing |

## LanceDB Replacement Surface

| Capability | Nowledge need | Skein status |
|---|---|---|
| Rebuildable projection | Search is derived, not source of truth | Partial: `SearchIndex` is separate from graph store |
| Vector search | semantic memory/entity/source search | Partial: exact cosine search |
| FTS/BM25 | text search and non-vector fallback | Partial: simple token coverage scoring |
| Hybrid fusion | vector + FTS score fusion | Partial: Nowledge-style vector/text fusion |
| Dimension checks | model/dimension changes require rebuild | Partial: dimension mismatch degrades vector leg |
| Markers | `.reindex_needed`, `.projection_metadata_repair_needed` | Partial: marker read/write |
| Fail-soft legs | stale vector or FTS should not drop all results | Partial: vector mismatch degrades to text |
| Metadata repair | bounded metadata-only backfill | Partial: graph-derived metadata repair without rewriting content or embeddings |
| Full rebuild orchestration | bounded replacement from authoritative graph | Partial: bounded graph-to-search rebuild with all-or-nothing in-memory replacement |
| Multi-table projections | memories/messages/entities/sources/chunks/communities | Partial: typed projection rows for memory/message/entity/source/source chunk/community |

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

## Next Implementation Slices

1. Add snapshot read transactions and MVCC reader isolation.
2. Add persistent index descriptors and richer index statistics.
3. Add projected graph snapshots for PageRank/Louvain-compatible workflows.
4. Add vector embedding rebuild hooks and model/dimension manifests.
5. Add BM25-style scoring and tokenizer parity with Nowledge search.
6. Split the flat crate into chryso-style `core`, `parser`, `planner`,
   `optimizer`, `store`, `search`, and root facade crates once the current
   MVP API surface stabilizes.
