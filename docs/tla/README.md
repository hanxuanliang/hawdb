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

## Source Segment Publication Model

`SkeinSourceSegmentPublication.tla` models the future storage-facing Source
scan sidecar. A Source segment is built from one graph epoch, made durable, and
only then published through a manifest for that same epoch. Readers may use the
sidecar only when their pinned graph epoch, the published manifest epoch, and
the durable segment epoch are identical. A newer graph snapshot must use the
authoritative graph path until a matching segment is published; it must never
silently read a stale segment.

Run the model with:

```bash
tlc -config docs/tla/SkeinSourceSegmentPublication.cfg \
  docs/tla/SkeinSourceSegmentPublication.tla
```
