use super::*;
use crate::relational::{
    encode_relational_wal_batch, ImmutableRelationalRowPage, RelationalColumnSchema,
    RelationalInsertMode, RelationalRowPageEntry, RelationalRowPageId, RelationalRowPagePublisher,
    RelationalRowPageTableDelta, RelationalScalarType, RelationalTableSchema,
    RelationalTransaction, RelationalWrite,
};
use skein_integrity::integrity_digest;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[test]
fn strict_wal_replay_builds_a_pinned_primary_key_overlay() {
    let directory = unique_test_dir("strict");
    let publication_config = RelationalRowPagePublicationConfig::default();
    publish_base(&directory, publication_config);
    let mut state = base_state();
    let mut builder = RelationalRowPageRecoveryBuilder::open_latest(
        &directory,
        publication_config,
        RelationalRowPageRecoveryConfig::default(),
    )
    .unwrap()
    .unwrap();

    let replace = transaction(vec![RelationalWrite::Insert {
        table: "documents".to_string(),
        rows: vec![row(2, "two-updated"), row(3, "three")],
        mode: RelationalInsertMode::Replace,
    }]);
    let encoded = encode_relational_wal_batch(2, &replace).unwrap();
    state = builder
        .replay_encoded_wal_batch(
            &state,
            &encoded,
            RelationalDecodeLimits::wal(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    state = builder
        .replay_wal_batch(
            &state,
            RelationalWalBatch {
                epoch: 3,
                transaction: transaction(vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "documents".to_string(),
                    keys: vec![key(1)],
                }]),
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    assert_eq!(state.row_count("documents"), 2);

    let view = builder.finish(3).unwrap();
    assert_eq!(
        view.identity(),
        RelationalRowPageRecoveryIdentity {
            base_generation: 1,
            base_commit_epoch: 1,
            visible_commit_epoch: 3,
        }
    );
    assert_eq!(view.report().replayed_batches, 2);
    assert_eq!(view.report().overlay_entries, 3);
    assert!(matches!(
        view.overlay_value("documents", &key(1)),
        Some(RelationalRowPageRecoveredValue::Deleted)
    ));
    assert_eq!(
        present_text(view.overlay_value("documents", &key(2))),
        Some("two-updated")
    );
    assert_eq!(
        present_text(view.overlay_value("documents", &key(3))),
        Some("three")
    );

    RelationalRowPagePublisher::new(publication_config)
        .publish(
            &directory,
            2,
            3,
            Some(1),
            vec![RelationalRowPageTableDelta {
                table: "documents".to_string(),
                schema_digest: schema_digest(),
                next_page_id: NonZeroU64::new(3).unwrap(),
                dirty_pages: vec![page(2, 3, &[(2, "two-updated"), (3, "three")])],
                deleted_page_ids: vec![page_id(1)],
            }],
        )
        .unwrap();
    assert_eq!(view.base().manifest().generation, 1);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn overlay_coalesces_repeated_keys_and_rejects_epoch_gaps_atomically() {
    let directory = unique_test_dir("coalesce");
    let publication_config = RelationalRowPagePublicationConfig::default();
    publish_base(&directory, publication_config);
    let mut builder = RelationalRowPageRecoveryBuilder::open_latest(
        &directory,
        publication_config,
        RelationalRowPageRecoveryConfig::default(),
    )
    .unwrap()
    .unwrap();
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    let first_bytes = builder.overlay_bytes;
    builder.record(3, capture(2, Some("two-v3"))).unwrap();
    assert_eq!(builder.overlay_entries, 1);
    assert!(builder.overlay_bytes <= first_bytes + "two-v3".len());

    let error = builder.record(5, capture(1, None)).unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPageRecoveryError::Corrupt(message)
            if message.contains("expected 4, found 5")
    ));
    assert_eq!(builder.visible_commit_epoch(), 3);
    assert_eq!(builder.overlay_entries, 1);

    let view = builder.finish(3).unwrap();
    assert_eq!(
        present_text(view.overlay_value("documents", &key(2))),
        Some("two-v3")
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn multiple_relational_fragments_share_one_global_epoch() {
    let directory = unique_test_dir("same-epoch-fragments");
    let publication_config = RelationalRowPagePublicationConfig::default();
    publish_base(&directory, publication_config);
    let mut builder = RelationalRowPageRecoveryBuilder::open_latest(
        &directory,
        publication_config,
        RelationalRowPageRecoveryConfig::default(),
    )
    .unwrap()
    .unwrap();

    builder.record(2, capture(1, None)).unwrap();
    builder.record(2, capture(2, Some("two-v2"))).unwrap();

    let view = builder.finish(2).unwrap();
    assert_eq!(view.report().replayed_batches, 1);
    assert_eq!(view.report().overlay_entries, 2);
    assert!(matches!(
        view.overlay_value("documents", &key(1)),
        Some(RelationalRowPageRecoveredValue::Deleted)
    ));
    assert_eq!(
        present_text(view.overlay_value("documents", &key(2))),
        Some("two-v2")
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn overlay_admission_rejects_a_whole_batch_without_partial_visibility() {
    let directory = unique_test_dir("admission");
    let publication_config = RelationalRowPagePublicationConfig::default();
    publish_base(&directory, publication_config);
    let config = RelationalRowPageRecoveryConfig {
        max_overlay_entries: NonZeroUsize::new(1).unwrap(),
        max_overlay_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
    };
    let mut builder =
        RelationalRowPageRecoveryBuilder::open_latest(&directory, publication_config, config)
            .unwrap()
            .unwrap();
    let error = builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![
                    RelationalRowChange {
                        table: "documents".to_string(),
                        primary_key: key(1),
                        row: None,
                    },
                    RelationalRowChange {
                        table: "documents".to_string(),
                        primary_key: key(2),
                        row: None,
                    },
                ],
                encoded_bytes: 0,
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPageRecoveryError::Admission(message)
            if message.contains("2 entries")
    ));
    assert_eq!(builder.visible_commit_epoch(), 1);
    assert_eq!(builder.overlay_entries, 0);
    assert!(builder.overlay.is_empty());
    assert_eq!(builder.overlay_bytes, 0);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn schema_change_and_overflow_reference_invalidate_row_recovery() {
    let directory = unique_test_dir("invalidate");
    let publication_config = RelationalRowPagePublicationConfig::default();
    publish_base(&directory, publication_config);
    let state = base_state();
    let mut ddl_builder = RelationalRowPageRecoveryBuilder::open_latest(
        &directory,
        publication_config,
        RelationalRowPageRecoveryConfig::default(),
    )
    .unwrap()
    .unwrap();
    let ddl = RelationalWalBatch {
        epoch: 2,
        transaction: transaction(vec![RelationalWrite::CreateIndex {
            table: "documents".to_string(),
            index: crate::relational::RelationalIndexSchema {
                name: "documents_body".to_string(),
                columns: vec!["body".to_string()],
                unique: false,
            },
        }]),
    };
    let error = ddl_builder
        .replay_wal_batch(
            &state,
            ddl,
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPageRecoveryError::Invalidated(message)
            if message.contains("requires new row roots")
    ));
    assert_eq!(ddl_builder.visible_commit_epoch(), 1);

    let mut overflow_builder = RelationalRowPageRecoveryBuilder::open_latest(
        &directory,
        publication_config,
        RelationalRowPageRecoveryConfig::default(),
    )
    .unwrap()
    .unwrap();
    let overflow = crate::relational::RelationalOverflowRef {
        scalar_type: RelationalScalarType::Text,
        digest: integrity_digest(b"overflow").sha256,
        compressed_bytes: 8,
        uncompressed_bytes: 8,
    };
    let error = overflow_builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![RelationalRowChange {
                    table: "documents".to_string(),
                    primary_key: key(2),
                    row: Some(RelationalRow::new(vec![
                        RelationalValue::BigInt(2),
                        RelationalValue::Overflow(overflow),
                    ])),
                }],
                encoded_bytes: 0,
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPageRecoveryError::Admission(message)
            if message.contains("overflow publication")
    ));
    assert_eq!(overflow_builder.visible_commit_epoch(), 1);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn opening_recovery_does_not_read_or_hash_base_page_slots() {
    let directory = unique_test_dir("cold-open");
    let publication_config = RelationalRowPagePublicationConfig::default();
    publish_base(&directory, publication_config);
    let artifact = directory.join(super::super::relational_row_page_artifact_file(1));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(artifact)
        .unwrap();
    file.seek(SeekFrom::Start(32)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();

    let builder = RelationalRowPageRecoveryBuilder::open_latest(
        &directory,
        publication_config,
        RelationalRowPageRecoveryConfig::default(),
    )
    .unwrap()
    .unwrap();
    let view = builder.finish(1).unwrap();
    assert_eq!(view.identity().base_generation, 1);
    assert_eq!(view.report().overlay_entries, 0);
    fs::remove_dir_all(directory).unwrap();
}

fn publish_base(directory: &Path, config: RelationalRowPagePublicationConfig) {
    RelationalRowPagePublisher::new(config)
        .publish(
            directory,
            1,
            1,
            None,
            vec![RelationalRowPageTableDelta {
                table: "documents".to_string(),
                schema_digest: schema_digest(),
                next_page_id: NonZeroU64::new(2).unwrap(),
                dirty_pages: vec![page(1, 1, &[(1, "one"), (2, "two")])],
                deleted_page_ids: Vec::new(),
            }],
        )
        .unwrap();
}

fn base_state() -> RelationalState {
    RelationalState::default()
        .stage_transaction(
            transaction(vec![
                RelationalWrite::CreateTable(schema()),
                RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![row(1, "one"), row(2, "two")],
                    mode: RelationalInsertMode::Error,
                },
            ]),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap()
}

fn schema() -> RelationalTableSchema {
    RelationalTableSchema {
        name: "documents".to_string(),
        columns: vec![
            RelationalColumnSchema {
                name: "id".to_string(),
                scalar_type: RelationalScalarType::BigInt,
                nullable: false,
                default: None,
            },
            RelationalColumnSchema {
                name: "body".to_string(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    }
}

fn transaction(writes: Vec<RelationalWrite>) -> RelationalTransaction {
    RelationalTransaction { writes }
}

fn capture(id: i64, body: Option<&str>) -> RelationalRowChangeCapture {
    RelationalRowChangeCapture::Captured {
        changes: vec![RelationalRowChange {
            table: "documents".to_string(),
            primary_key: key(id),
            row: body.map(|body| row(id, body)),
        }],
        encoded_bytes: 0,
    }
}

fn page(
    generation: u64,
    source_commit_epoch: u64,
    rows: &[(i64, &str)],
) -> ImmutableRelationalRowPage {
    ImmutableRelationalRowPage {
        generation,
        source_commit_epoch,
        page_id: page_id(generation),
        schema_digest: schema_digest(),
        column_count: 2,
        rows: rows
            .iter()
            .map(|(id, body)| RelationalRowPageEntry {
                primary_key: key(*id),
                row: row(*id, body),
            })
            .collect(),
    }
}

fn row(id: i64, body: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::Text(body.to_string()),
    ])
}

fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

fn page_id(value: u64) -> RelationalRowPageId {
    RelationalRowPageId::new(NonZeroU64::new(value).unwrap())
}

fn schema_digest() -> skein_integrity::Sha256Digest {
    integrity_digest(b"documents-schema-v1").sha256
}

fn present_text(value: Option<&RelationalRowPageRecoveredValue>) -> Option<&str> {
    match value {
        Some(RelationalRowPageRecoveredValue::Present(row)) => match &row.values()[1] {
            RelationalValue::Text(text) => Some(text),
            _ => None,
        },
        _ => None,
    }
}

fn unique_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "skein-row-page-recovery-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
