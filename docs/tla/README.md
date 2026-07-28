# Concurrent Snapshot Model

`SkeinConcurrentSnapshots.tla` models the storage publication boundary used by
`skein-storage::SnapshotCoordinator`.

The model keeps one writer in the staging and durability path while readers pin
immutable published snapshots. A new epoch can become visible only after its WAL
or equivalent durability action has completed. `Crash` discards staged state and
reader pins while retaining only durable and published epochs.

Run the model with a local TLA+ installation:

```bash
tlc -config docs/tla/SkeinConcurrentSnapshots.cfg \
  docs/tla/SkeinConcurrentSnapshots.tla
```

The checked invariants cover durable-before-publish ordering, writer ownership of
the next epoch, reader visibility of published epochs, and recoverability after
a crash at any modeled write stage.
