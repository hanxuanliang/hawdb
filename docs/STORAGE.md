# Storage Design

## Current V1 Slice

The current storage implementation is a small durable graph store slice. It is
not an LSM tree and it does not depend on RocksDB or another storage engine.

Files:

- `checkpoint.skein`: full durable snapshot of catalog tokens, nodes, and
  relationships.
- `wal.skein`: append-only committed mutation log.

Recovery:

1. Load `checkpoint.skein` when present.
2. Verify the checkpoint checksum.
3. Replay valid WAL entries in order.
4. Stop replay at a torn tail or checksum mismatch.
5. Rebuild in-memory adjacency indexes from relationship records.

WAL entries can represent either a single mutation or a batch commit record.
The relationship pattern create path uses a single batch record for source node,
target node, and relationship creation. Recovery only applies a batch after its
whole record passes checksum validation, so a torn tail cannot leave behind a
half-created path.

`DatabaseTransaction` buffers mutation statements and commits them as one WAL
batch. Rollback drops the buffered mutations without touching the store. This is
the first transaction slice; it does not yet provide snapshot read transactions,
MVCC visibility, or concurrent writer coordination.

The implementation currently persists:

- node labels
- relationship types
- node records
- relationship records
- relationship properties
- outgoing adjacency index
- incoming adjacency index

The implementation also maintains a rebuildable in-memory property equality
index keyed by `(label_id, property, value)`. The optimizer can choose
`IndexNodeSeek` for simple label plus property equality predicates. The
adjacency and property indexes are rebuilt from canonical records after
checkpoint load or WAL replay. They are not separate canonical state.

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
- no snapshot read transaction API
- no page cache
- no segment manifest
- no delayed garbage collection
- no property spill blocks
- no persistent index descriptors or index statistics
- no columnar property segments
- no CSR/CSC analytical projection
- no relationship delete/update path

It is a correctness-first recovery slice that keeps the public direction
aligned with the intended Adaptive Native Graph Store.

## Next Storage Tasks

1. Add snapshot read transactions and MVCC reader isolation.
2. Add a manifest file with checkpoint epoch and WAL replay boundary.
3. Add sparse adjacency blocks before dense adjacency segments.
4. Add property spill blocks for large values.
5. Add persistent index descriptors and richer index statistics.
6. Add CSR/CSC projection generation as rebuildable checkpoint artifacts.
