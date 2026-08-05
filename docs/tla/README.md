# Skein Storage TLA+ Models

These models specify the storage publication and recovery protocols that are
implemented by the embedded Skein library. They are executable specifications,
checked over bounded state spaces by TLC.

Run every model with the repository-default pinned TLC release:

```bash
scripts/check-storage-tla.sh
```

Set `TLA2TOOLS_JAR` to use an existing `tla2tools.jar`, or `TLA_JAVA` to select
a Java 11 or newer runtime. Without `TLA2TOOLS_JAR`, the script downloads TLA+
Tools 1.7.4 and verifies its SHA-256 digest before execution.

Set `TLA_RESULTS_DIR` and `TLA_SOURCE_REVISION` to retain a release artifact.
The artifact contains the exact four `.tla` and `.cfg` inputs, one complete TLC
log per model, the Java version, and a revision- and tool-bound manifest. CI
validates the downloaded artifact with:

```bash
scripts/check-storage-tla.sh --verify-results tla-results "$GITHUB_SHA"
```

## Durable WAL and Checkpoint Publication

`SkeinStorageDurability.tla` models the default `SyncOnEveryWrite` path. A WAL
batch becomes a durable commit decision at the WAL sync boundary. Applying that
batch makes it visible, and returning from the mutation acknowledges it. A crash
between sync and acknowledgement may therefore recover a committed batch whose
acknowledgement was not observed, which is the standard ambiguous-commit case.
The model uses strict recovery for ordinary startup. One model epoch represents
one logical batch and its contiguous WAL LSN. A non-newline-terminated final
frame fails startup and can only be discarded through the explicit writable
doctor repair mode; corruption in a complete frame always fails closed.

The model checks:

- one process owns the database directory at a time;
- acknowledged commits are never outside the durable prefix;
- visible commits are durable;
- the active WAL is a contiguous suffix after the manifest checkpoint;
- every durable commit is reachable from the published checkpoint plus WAL;
- a manifest references only a durable checkpoint and prepared WAL generation;
- a failed in-memory apply poisons the handle until crash and reopen;
- complete-record corruption fails closed instead of exposing partial recovered
  state, including corruption in the final WAL record.

The checkpoint actions map directly to `GraphStore::checkpoint_with_reader_epoch`
and `DurableStore::{write_checkpoint,prepare_wal_generation,
publish_checkpoint_manifest}`. WAL actions map to
`DurableStore::{append_entry,finish_wal_append}` and `GraphStore::apply_wal_op`.

## Generation Reclamation

`SkeinGenerationReclamation.tla` models immutable checkpoint generations and the
coarse reader-pin policy used by `DurableStore::reclaim_old_generations`.
Publication requires the target generation to be durable. A pinned reader's
generation remains available, reclamation is disabled while any reader exists,
and the current and immediately previous generations remain after reclamation.

Reader actions map to `Database::begin_read_transaction` and `ReaderPin::drop`.
Publication and reclamation map to `DurableStore::publish_checkpoint_manifest`
and `DurableStore::reclaim_old_generations`.

## Implementation Refinement Evidence

The Rust tests below exercise the concrete boundaries represented by the model.
They are implementation evidence, not a machine-checked refinement proof.

| Protocol obligation | Implementation boundary | Regression evidence |
| --- | --- | --- |
| WAL sync precedes visibility and apply failure closes the handle | `finish_wal_append`, `apply_wal_op`, `ensure_usable` | `post_wal_apply_failure_poisons_handle_until_reopen` |
| A torn WAL batch has no partial recovered visibility | `replay_wal` record decode and batch apply | `stops_replay_at_torn_wal_tail`, `skips_torn_batch_wal_without_partial_path_recovery` |
| Complete-record corruption and LSN gaps fail closed | `replay_wal` framing, checksum, and expected-LSN checks | `rejects_and_quarantines_checksum_corruption_at_wal_tail`, `rejects_and_quarantines_checksum_corruption_before_valid_wal_suffix`, `rejects_and_quarantines_non_contiguous_wal_lsn` |
| Checkpoint publication selects one complete generation | checkpoint failpoints and manifest replacement | `checkpoint_publish_failpoints_recover_one_complete_generation`, `subprocess_crash_matrix_recovers_whole_batches_and_artifact_generations` |
| Reader pins prevent generation reclamation | `ReaderPins`, `reclaim_old_generations` | `read_transaction_pins_checkpoint_manifest_until_drop`, `out_of_core_reader_pin_retains_its_canonical_generation_until_drop` |
| Canonical path aliases share one ownership boundary | `DatabaseDirectoryLease::acquire` | `durable_database_open_is_exclusive_until_owner_drops`, `durable_database_rejects_path_alias_until_owner_drops` |

## In-memory Snapshot Publication

`SkeinConcurrentSnapshots.tla` models `skein-storage::SnapshotCoordinator`.
Readers pin immutable `Arc` snapshots, one writer stages the next epoch, and the
published pointer changes only after the durability callback succeeds.

## Derived Source Segment Publication

`SkeinSourceSegmentPublication.tla` models Source scan sidecars. A reader may
select a sidecar only when its graph epoch equals the published sidecar epoch;
otherwise it must use the authoritative graph path. The published manifest
cannot reference an undurable sidecar.

## Proof Boundary

TLC exhaustively checks the configured finite instances; it is not a proof of
the Rust implementation, the filesystem, or arbitrary-sized instances. The
models establish safety invariants, not operation latency or starvation
freedom, and rely on these environmental assumptions:

- successful `sync_data` or `sync_all` survives a crash;
- durable file replacement is atomic and the parent-directory sync preserves
  the selected name on supported filesystems;
- the OS file lock provides exclusive ownership for a canonical directory;
- checksums detect malformed complete records, and doctor repair may discard
  only a non-newline-terminated frame at the non-synced WAL tail;
- validated WAL batches replay deterministically, or recovery fails without
  publishing a database handle;
- the model's checkpoint artifact represents the checkpoint, canonical graph,
  adjacency, property spill, and property projection artifacts as one validated
  generation selected by the manifest.

`SyncOnCheckpoint` is intentionally outside the acknowledged-commit durability
claim: writes accepted under that policy may be lost before the next successful
checkpoint. Fault-injection, cross-platform recovery, and filesystem tests are
still required to validate that the implementation refines these models and
that the environmental assumptions hold.
