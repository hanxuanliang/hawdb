//! Shadow recovery state for canonical relational row-page roots.

use super::GraphStore;
use skein_storage::{
    RelationalRowChangeCapture, RelationalRowChangeCaptureLimits,
    RelationalRowPagePublicationConfig, RelationalRowPageRecoveryBuilder,
    RelationalRowPageRecoveryConfig, RelationalRowPageRecoveryReport,
    RelationalRowPageRecoveryView,
};
use std::sync::Arc;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RelationalRowPageRecoveryStatus {
    #[default]
    Missing,
    CheckpointReady {
        generation: u64,
        source_commit_epoch: u64,
        root_pages: u64,
    },
    WalRecovered {
        base_generation: u64,
        base_commit_epoch: u64,
        recovered_commit_epoch: u64,
        overlay_entries: usize,
        overlay_bytes: usize,
    },
    Stale {
        generation: u64,
        source_commit_epoch: u64,
        checkpoint_generation: u64,
        checkpoint_commit_epoch: u64,
    },
    Unavailable {
        base_generation: Option<u64>,
        base_commit_epoch: Option<u64>,
        recovered_commit_epoch: u64,
        reason: String,
    },
}

#[derive(Debug, Default)]
pub(super) struct RelationalRowPageState {
    recovery_builder: Option<RelationalRowPageRecoveryBuilder>,
    recovery_view: Option<Arc<RelationalRowPageRecoveryView>>,
    recovery_report: Option<RelationalRowPageRecoveryReport>,
    recovery_status: RelationalRowPageRecoveryStatus,
}

impl RelationalRowPageState {
    pub(super) fn snapshot_at_epoch(&self, commit_epoch: u64) -> Self {
        Self {
            recovery_builder: None,
            recovery_view: self
                .recovery_view
                .as_ref()
                .filter(|view| view.identity().visible_commit_epoch == commit_epoch)
                .cloned(),
            recovery_report: self.recovery_report.clone(),
            recovery_status: self.recovery_status.clone(),
        }
    }

    fn base_identity(&self) -> (Option<u64>, Option<u64>) {
        match &self.recovery_status {
            RelationalRowPageRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                ..
            } => (Some(*generation), Some(*source_commit_epoch)),
            RelationalRowPageRecoveryStatus::WalRecovered {
                base_generation,
                base_commit_epoch,
                ..
            } => (Some(*base_generation), Some(*base_commit_epoch)),
            RelationalRowPageRecoveryStatus::Stale {
                generation,
                source_commit_epoch,
                ..
            } => (Some(*generation), Some(*source_commit_epoch)),
            RelationalRowPageRecoveryStatus::Unavailable {
                base_generation,
                base_commit_epoch,
                ..
            } => (*base_generation, *base_commit_epoch),
            RelationalRowPageRecoveryStatus::Missing => (None, None),
        }
    }
}

impl GraphStore {
    pub(super) fn mount_relational_row_pages_for_recovery(&mut self) {
        let Some(durable) = self.durable.as_ref() else {
            return;
        };
        let checkpoint_generation = durable.checkpoint_epoch;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        let read_only = durable.read_only;
        let root = durable.root_path().to_path_buf();
        let publication_config = RelationalRowPagePublicationConfig::default();
        let recovery_config = RelationalRowPageRecoveryConfig::default();
        let reader = match skein_storage::RelationalRowPageRootReader::open_latest(
            &root,
            publication_config,
        ) {
            Ok(Some(reader)) => reader,
            Ok(None) => {
                self.relational_row_pages = RelationalRowPageState::default();
                return;
            }
            Err(error) => {
                self.mark_relational_row_page_recovery_unavailable(
                    self.commit_epoch,
                    format!("published relational row root could not be opened: {error}"),
                );
                return;
            }
        };
        let manifest = reader.manifest();
        if manifest.generation != checkpoint_generation
            || manifest.source_commit_epoch != checkpoint_commit_epoch
        {
            self.relational_row_pages.recovery_status = RelationalRowPageRecoveryStatus::Stale {
                generation: manifest.generation,
                source_commit_epoch: manifest.source_commit_epoch,
                checkpoint_generation,
                checkpoint_commit_epoch,
            };
            self.relational_row_pages.recovery_builder = None;
            self.relational_row_pages.recovery_view = None;
            return;
        }
        let root_pages = manifest.root_page_count;
        let generation = manifest.generation;
        let source_commit_epoch = manifest.source_commit_epoch;
        let base_view =
            RelationalRowPageRecoveryBuilder::from_base(reader.clone(), recovery_config).and_then(
                |builder| {
                    builder.validate_base_schema(&self.relational_state)?;
                    builder.finish(source_commit_epoch)
                },
            );
        let base_view = match base_view {
            Ok(view) => Arc::new(view),
            Err(error) => {
                self.mark_relational_row_page_recovery_unavailable(
                    self.commit_epoch,
                    format!("published relational row root could not be pinned: {error}"),
                );
                return;
            }
        };
        self.relational_row_pages.recovery_view = Some(base_view);
        self.relational_row_pages.recovery_report = None;
        self.relational_row_pages.recovery_status =
            RelationalRowPageRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                root_pages,
            };
        if !read_only {
            match RelationalRowPageRecoveryBuilder::from_base(reader, recovery_config) {
                Ok(builder) => self.relational_row_pages.recovery_builder = Some(builder),
                Err(error) => self.mark_relational_row_page_recovery_unavailable(
                    self.commit_epoch,
                    format!("relational row recovery builder could not start: {error}"),
                ),
            }
        }
    }

    pub(super) fn relational_row_recovery_capture_limits(
        &self,
    ) -> Option<RelationalRowChangeCaptureLimits> {
        self.relational_row_pages
            .recovery_builder
            .as_ref()
            .map(RelationalRowPageRecoveryBuilder::capture_limits)
    }

    pub(super) fn record_relational_row_recovery_capture(
        &mut self,
        epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
    ) {
        let Some(capture) = capture else {
            return;
        };
        let Some(mut builder) = self.relational_row_pages.recovery_builder.take() else {
            return;
        };
        match builder.record(epoch, capture) {
            Ok(()) => self.relational_row_pages.recovery_builder = Some(builder),
            Err(error) => self.mark_relational_row_page_recovery_unavailable(
                epoch,
                format!("relational WAL row overlay could not advance: {error}"),
            ),
        }
    }

    pub(super) fn advance_relational_row_recovery_epoch(&mut self, epoch: u64) {
        let Some(mut builder) = self.relational_row_pages.recovery_builder.take() else {
            return;
        };
        match builder.advance_empty(epoch) {
            Ok(()) => self.relational_row_pages.recovery_builder = Some(builder),
            Err(error) => self.mark_relational_row_page_recovery_unavailable(
                epoch,
                format!("relational row recovery epoch could not advance: {error}"),
            ),
        }
    }

    pub(super) fn invalidate_relational_row_page_recovery(
        &mut self,
        recovered_commit_epoch: u64,
        reason: impl Into<String>,
    ) {
        if self.relational_row_pages.recovery_builder.is_some() {
            self.mark_relational_row_page_recovery_unavailable(
                recovered_commit_epoch,
                reason.into(),
            );
        }
    }

    pub(super) fn invalidate_relational_row_page_live_view(
        &mut self,
        commit_epoch: u64,
        reason: &'static str,
    ) {
        if self.relational_row_pages.recovery_view.is_some() {
            self.mark_relational_row_page_recovery_unavailable(commit_epoch, reason.to_string());
        }
    }

    pub(super) fn finish_relational_row_page_recovery(&mut self) {
        let Some(builder) = self.relational_row_pages.recovery_builder.take() else {
            if let RelationalRowPageRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                ..
            } = self.relational_row_pages.recovery_status
                && self.commit_epoch > source_commit_epoch
            {
                self.relational_row_pages.recovery_view = None;
                self.relational_row_pages.recovery_status =
                    RelationalRowPageRecoveryStatus::Unavailable {
                        base_generation: Some(generation),
                        base_commit_epoch: Some(source_commit_epoch),
                        recovered_commit_epoch: self.commit_epoch,
                        reason: "read-only recovery cannot retain a canonical row WAL overlay"
                            .to_string(),
                    };
            }
            return;
        };
        match builder.finish(self.commit_epoch) {
            Ok(view) => {
                let report = view.report().clone();
                self.relational_row_pages.recovery_view = Some(Arc::new(view));
                self.relational_row_pages.recovery_status =
                    RelationalRowPageRecoveryStatus::WalRecovered {
                        base_generation: report.identity.base_generation,
                        base_commit_epoch: report.identity.base_commit_epoch,
                        recovered_commit_epoch: report.identity.visible_commit_epoch,
                        overlay_entries: report.overlay_entries,
                        overlay_bytes: report.overlay_bytes,
                    };
                self.relational_row_pages.recovery_report = Some(report);
            }
            Err(error) => self.mark_relational_row_page_recovery_unavailable(
                self.commit_epoch,
                format!("relational row recovery could not finish: {error}"),
            ),
        }
    }

    fn mark_relational_row_page_recovery_unavailable(
        &mut self,
        recovered_commit_epoch: u64,
        reason: String,
    ) {
        let (base_generation, base_commit_epoch) = self.relational_row_pages.base_identity();
        self.relational_row_pages.recovery_builder = None;
        self.relational_row_pages.recovery_view = None;
        self.relational_row_pages.recovery_status = RelationalRowPageRecoveryStatus::Unavailable {
            base_generation,
            base_commit_epoch,
            recovered_commit_epoch,
            reason,
        };
    }

    pub fn relational_row_page_recovery_status(&self) -> &RelationalRowPageRecoveryStatus {
        &self.relational_row_pages.recovery_status
    }

    pub fn relational_row_page_recovery_report(&self) -> Option<&RelationalRowPageRecoveryReport> {
        self.relational_row_pages.recovery_report.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Catalog;
    use crate::store::GraphStore;
    use skein_storage::{
        DurabilityPolicy, ImmutableRelationalRowPage, RelationalColumnSchema, RelationalInsertMode,
        RelationalKey, RelationalRow, RelationalRowPageEntry, RelationalRowPageId,
        RelationalRowPagePublisher, RelationalRowPageTableDelta, RelationalScalarType,
        RelationalTableSchema, RelationalTransaction, RelationalValue, RelationalWrite,
        WalReplayConfig, RELATIONAL_ROW_PAGE_MANIFEST_FILE,
    };
    use std::num::NonZeroU64;

    #[test]
    fn durable_open_replays_wal_into_a_generation_pinned_row_overlay() {
        let path = unique_test_dir("wal-overlay");
        let replay = WalReplayConfig::default();
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .unwrap();
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![
                            RelationalWrite::CreateTable(schema()),
                            RelationalWrite::Insert {
                                table: "documents".to_string(),
                                rows: vec![row(1, "one")],
                                mode: RelationalInsertMode::Error,
                            },
                        ],
                    },
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            let durable = store.durable.as_ref().unwrap();
            let generation = durable.checkpoint_epoch;
            let source_commit_epoch = durable.checkpoint_commit_epoch;
            let schema_digest = store
                .relational_state
                .table_schema_digest("documents")
                .unwrap()
                .unwrap();
            RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default())
                .publish(
                    &path,
                    generation,
                    source_commit_epoch,
                    None,
                    vec![RelationalRowPageTableDelta {
                        table: "documents".to_string(),
                        schema_digest,
                        dirty_pages: vec![ImmutableRelationalRowPage {
                            generation,
                            source_commit_epoch,
                            page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                            schema_digest,
                            column_count: 2,
                            rows: vec![RelationalRowPageEntry {
                                primary_key: key(1),
                                row: row(1, "one"),
                            }],
                        }],
                        deleted_page_ids: Vec::new(),
                    }],
                )
                .unwrap();
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::Insert {
                            table: "documents".to_string(),
                            rows: vec![row(2, "two")],
                            mode: RelationalInsertMode::Error,
                        }],
                    },
                )
                .unwrap();
        }

        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        assert!(matches!(
            store.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::WalRecovered {
                base_generation: 1,
                base_commit_epoch: 1,
                recovered_commit_epoch: 2,
                overlay_entries: 1,
                ..
            }
        ));
        let report = store.relational_row_page_recovery_report().unwrap();
        assert_eq!(report.replayed_batches, 1);
        assert_eq!(report.overlay_entries, 1);
        let view = Arc::clone(store.relational_row_pages.recovery_view.as_ref().unwrap());
        assert!(matches!(
            view.overlay_value("documents", &key(2)),
            Some(skein_storage::RelationalRowPageRecoveredValue::Present(value))
                if value == &row(2, "two")
        ));
        let snapshot = store.snapshot();
        assert!(Arc::ptr_eq(
            &view,
            snapshot
                .relational_row_pages
                .recovery_view
                .as_ref()
                .unwrap()
        ));
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![row(3, "three")],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .unwrap();
        assert!(matches!(
            store.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::Unavailable {
                base_generation: Some(1),
                base_commit_epoch: Some(1),
                recovered_commit_epoch: 3,
                reason,
            } if reason.contains("live commits")
        ));
        assert!(store.relational_row_pages.recovery_view.is_none());
        assert!(Arc::ptr_eq(
            &view,
            snapshot
                .relational_row_pages
                .recovery_view
                .as_ref()
                .unwrap()
        ));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn stale_row_root_is_observable_but_does_not_replace_canonical_recovery() {
        let path = unique_test_dir("stale");
        let replay = WalReplayConfig::default();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(schema())],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default())
            .publish(&path, 1, 1, None, Vec::new())
            .unwrap();
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![row(1, "one")],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);

        let mut reopened_catalog = Catalog::default();
        let reopened = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut reopened_catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        assert!(matches!(
            reopened.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::Stale {
                generation: 1,
                source_commit_epoch: 1,
                checkpoint_generation: 2,
                checkpoint_commit_epoch: 2,
            }
        ));
        assert_eq!(reopened.relational_state.row_count("documents"), 1);
        assert!(reopened.relational_row_pages.recovery_view.is_none());

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn corrupt_row_root_isolated_from_canonical_checkpoint_recovery() {
        let path = unique_test_dir("corrupt");
        let replay = WalReplayConfig::default();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(schema()),
                        RelationalWrite::Insert {
                            table: "documents".to_string(),
                            rows: vec![row(1, "one")],
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let schema_digest = store
            .relational_state
            .table_schema_digest("documents")
            .unwrap()
            .unwrap();
        RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default())
            .publish(
                &path,
                1,
                1,
                None,
                vec![RelationalRowPageTableDelta {
                    table: "documents".to_string(),
                    schema_digest,
                    dirty_pages: vec![ImmutableRelationalRowPage {
                        generation: 1,
                        source_commit_epoch: 1,
                        page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                        schema_digest,
                        column_count: 2,
                        rows: vec![RelationalRowPageEntry {
                            primary_key: key(1),
                            row: row(1, "one"),
                        }],
                    }],
                    deleted_page_ids: Vec::new(),
                }],
            )
            .unwrap();
        drop(store);

        let latest = path.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
        let mut encoded = std::fs::read(&latest).unwrap();
        *encoded.last_mut().unwrap() ^= 1;
        std::fs::write(latest, encoded).unwrap();

        let mut reopened_catalog = Catalog::default();
        let reopened = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut reopened_catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        assert_eq!(reopened.relational_state.row_count("documents"), 1);
        assert!(matches!(
            reopened.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::Unavailable {
                base_generation: None,
                base_commit_epoch: None,
                recovered_commit_epoch: 1,
                reason,
            } if reason.contains("could not be opened")
        ));
        assert!(reopened.relational_row_pages.recovery_view.is_none());

        std::fs::remove_dir_all(path).unwrap();
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

    fn row(id: i64, body: &str) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::BigInt(id),
            RelationalValue::Text(body.to_string()),
        ])
    }

    fn key(id: i64) -> RelationalKey {
        RelationalKey(vec![RelationalValue::BigInt(id)])
    }

    fn unique_test_dir(label: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein-store-relational-row-recovery-{label}-{}-{nonce}",
            std::process::id()
        ))
    }
}
