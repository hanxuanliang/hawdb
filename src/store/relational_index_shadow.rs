//! Non-serving relational index-page shadow publication.
//!
//! The shadow is deliberately derived: canonical checkpoint success never
//! depends on it and SQL never reads it in this stage. A valid shadow is
//! generation/epoch fenced to the checkpoint that supplied its rows.

use super::GraphStore;
use skein_integrity::{IntegrityHasher, Sha256Digest};
use skein_storage::{
    RelationalIndexRecoveryBuilder, RelationalIndexRecoveryConfig, RelationalIndexRecoveryReader,
    RelationalIndexRecoveryReport, RelationalIndexShadowBuildReport, RelationalIndexShadowConfig,
    RelationalIndexShadowManifest, RelationalIndexShadowReader, RelationalIndexShadowWriter,
    RelationalTransaction, RELATIONAL_INDEX_SHADOW_MANIFEST_FILE,
};
use std::{fmt, fs, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationalIndexReadViewKind {
    Base,
    Recovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelationalIndexReadViewIdentity {
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub schema_digest: Sha256Digest,
}

enum RelationalIndexReadBackend {
    Base(RelationalIndexShadowReader),
    Recovered(RelationalIndexRecoveryReader),
}

/// One immutable, generation-bound relational index view.
///
/// The outer `Arc` is cloned into [`GraphStore`] snapshots. The selected base
/// and recovery manifests therefore cannot drift underneath a pinned reader,
/// while a newer store publication can install another view independently.
pub(crate) struct RelationalIndexReadView {
    identity: RelationalIndexReadViewIdentity,
    backend: RelationalIndexReadBackend,
}

impl fmt::Debug for RelationalIndexReadView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationalIndexReadView")
            .field("identity", &self.identity)
            .field("kind", &self.kind())
            .field("poisoned", &self.is_poisoned())
            .finish()
    }
}

impl RelationalIndexReadView {
    fn from_base(reader: RelationalIndexShadowReader) -> Self {
        let manifest = reader.manifest();
        Self {
            identity: RelationalIndexReadViewIdentity {
                base_generation: manifest.generation,
                delta_generation: None,
                base_commit_epoch: manifest.source_commit_epoch,
                visible_commit_epoch: manifest.source_commit_epoch,
                schema_digest: relational_index_schema_digest(manifest),
            },
            backend: RelationalIndexReadBackend::Base(reader),
        }
    }

    fn from_recovered(reader: RelationalIndexRecoveryReader) -> Self {
        let base = reader.base_manifest();
        let recovered = reader.manifest();
        Self {
            identity: RelationalIndexReadViewIdentity {
                base_generation: base.generation,
                delta_generation: Some(recovered.delta_generation),
                base_commit_epoch: base.source_commit_epoch,
                visible_commit_epoch: recovered.recovered_commit_epoch,
                schema_digest: relational_index_schema_digest(base),
            },
            backend: RelationalIndexReadBackend::Recovered(reader),
        }
    }

    pub(crate) fn identity(&self) -> RelationalIndexReadViewIdentity {
        self.identity
    }

    pub(crate) fn kind(&self) -> RelationalIndexReadViewKind {
        match self.backend {
            RelationalIndexReadBackend::Base(_) => RelationalIndexReadViewKind::Base,
            RelationalIndexReadBackend::Recovered(_) => RelationalIndexReadViewKind::Recovered,
        }
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.is_poisoned(),
            RelationalIndexReadBackend::Recovered(reader) => reader.is_poisoned(),
        }
    }

    fn base_manifest(&self) -> &RelationalIndexShadowManifest {
        match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.manifest(),
            RelationalIndexReadBackend::Recovered(reader) => reader.base_manifest(),
        }
    }
}

fn relational_index_schema_digest(manifest: &RelationalIndexShadowManifest) -> Sha256Digest {
    let mut hasher = IntegrityHasher::new();
    hasher.update(&(manifest.roots.len() as u64).to_le_bytes());
    for root in &manifest.roots {
        hash_bounded_bytes(&mut hasher, root.identity.namespace.as_bytes());
        hash_bounded_bytes(&mut hasher, root.identity.name.as_bytes());
        hasher.update(root.schema_digest.as_bytes());
    }
    hasher.finish().sha256
}

fn hash_bounded_bytes(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalIndexShadowCheckpointStatus {
    Published,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexShadowCheckpointReport {
    pub status: RelationalIndexShadowCheckpointStatus,
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub index_roots: usize,
    pub pages_written: u64,
    pub artifact_bytes: u64,
    pub manifest_bytes: u64,
    pub peak_build_metadata_bytes: usize,
    pub error: Option<String>,
}

impl RelationalIndexShadowCheckpointReport {
    fn published(report: RelationalIndexShadowBuildReport) -> Self {
        Self {
            status: RelationalIndexShadowCheckpointStatus::Published,
            generation: report.generation,
            source_commit_epoch: report.source_commit_epoch,
            index_roots: report.index_roots,
            pages_written: report.pages_written,
            artifact_bytes: report.artifact_bytes,
            manifest_bytes: report.manifest_bytes,
            peak_build_metadata_bytes: report.peak_build_metadata_bytes,
            error: None,
        }
    }

    fn failed(generation: u64, source_commit_epoch: u64, error: String) -> Self {
        Self {
            status: RelationalIndexShadowCheckpointStatus::Failed,
            generation,
            source_commit_epoch,
            index_roots: 0,
            pages_written: 0,
            artifact_bytes: 0,
            manifest_bytes: 0,
            peak_build_metadata_bytes: 0,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RelationalIndexShadowRecoveryStatus {
    #[default]
    Disabled,
    Missing,
    CheckpointReady {
        generation: u64,
        source_commit_epoch: u64,
        index_roots: usize,
        page_count: u64,
    },
    WalRecovered {
        base_generation: u64,
        base_commit_epoch: u64,
        recovered_commit_epoch: u64,
        delta_pages: usize,
        delta_entries: usize,
        peak_dirty_bytes: usize,
    },
    RecoveryUnavailable {
        base_generation: u64,
        base_commit_epoch: u64,
        recovered_commit_epoch: u64,
        reason: String,
    },
    Stale {
        generation: u64,
        source_commit_epoch: u64,
        checkpoint_generation: u64,
        checkpoint_commit_epoch: u64,
    },
    DiscardedInvalid {
        error: String,
    },
    InvalidWritable {
        error: String,
    },
    InvalidReadOnly {
        error: String,
    },
}

#[derive(Debug, Clone, Default)]
pub(super) struct RelationalIndexShadowState {
    enabled: bool,
    expected_previous_generation: Option<u64>,
    checkpoint_report: Option<RelationalIndexShadowCheckpointReport>,
    recovery_builder: Option<RelationalIndexRecoveryBuilder>,
    recovery_report: Option<RelationalIndexRecoveryReport>,
    recovery_status: RelationalIndexShadowRecoveryStatus,
    read_view: Option<Arc<RelationalIndexReadView>>,
}

impl RelationalIndexShadowState {
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            recovery_status: if enabled {
                RelationalIndexShadowRecoveryStatus::Missing
            } else {
                RelationalIndexShadowRecoveryStatus::Disabled
            },
            ..Self::default()
        }
    }

    fn current_read_view(&self, commit_epoch: u64) -> Option<&Arc<RelationalIndexReadView>> {
        self.read_view
            .as_ref()
            .filter(|view| view.identity().visible_commit_epoch == commit_epoch)
    }

    pub(super) fn snapshot_at_epoch(&self, commit_epoch: u64) -> Self {
        let mut snapshot = self.clone();
        snapshot.read_view = self.current_read_view(commit_epoch).cloned();
        snapshot.recovery_builder = None;
        snapshot
    }
}

impl GraphStore {
    fn open_base_relational_index_read_view(
        &self,
    ) -> Result<Arc<RelationalIndexReadView>, skein_storage::RelationalIndexShadowError> {
        let durable = self.durable.as_ref().ok_or_else(|| {
            skein_storage::RelationalIndexShadowError::Admission(
                "relational index read view requires a durable store".to_string(),
            )
        })?;
        RelationalIndexShadowReader::open_latest_with_cache(
            durable.root_path(),
            RelationalIndexShadowConfig::default(),
            Arc::clone(&durable.segment_cache),
            durable.store_id(),
        )
        .map(RelationalIndexReadView::from_base)
        .map(Arc::new)
    }

    fn open_recovered_relational_index_read_view(
        &self,
        recovered_commit_epoch: u64,
    ) -> Result<Arc<RelationalIndexReadView>, skein_storage::RelationalIndexShadowError> {
        let durable = self.durable.as_ref().ok_or_else(|| {
            skein_storage::RelationalIndexShadowError::Admission(
                "relational index read view requires a durable store".to_string(),
            )
        })?;
        RelationalIndexRecoveryReader::open_latest_with_cache(
            durable.root_path(),
            recovered_commit_epoch,
            RelationalIndexShadowConfig::default(),
            RelationalIndexRecoveryConfig::default(),
            Arc::clone(&durable.segment_cache),
            durable.store_id(),
        )
        .map(RelationalIndexReadView::from_recovered)
        .map(Arc::new)
    }

    pub(super) fn mount_relational_index_shadow_for_recovery(&mut self) {
        if !self.relational_index_shadow.enabled {
            return;
        }
        let Some(durable) = self.durable.as_ref() else {
            return;
        };
        let root = durable.root_path().to_path_buf();
        let checkpoint_generation = durable.checkpoint_epoch;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        let read_only = durable.read_only;
        let manifest_path = root.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE);
        if !manifest_path.exists() {
            self.relational_index_shadow.recovery_status =
                RelationalIndexShadowRecoveryStatus::Missing;
            return;
        }
        match self.open_base_relational_index_read_view() {
            Ok(view) => {
                let manifest = view.base_manifest();
                self.relational_index_shadow.expected_previous_generation =
                    Some(manifest.generation);
                self.relational_index_shadow.read_view = Some(Arc::clone(&view));
                if manifest.generation == checkpoint_generation
                    && manifest.source_commit_epoch == checkpoint_commit_epoch
                {
                    let recovery_builder = (!read_only).then(|| {
                        RelationalIndexRecoveryBuilder::new(
                            &root,
                            manifest.generation,
                            manifest.source_commit_epoch,
                            RelationalIndexRecoveryConfig::default(),
                        )
                    });
                    match recovery_builder {
                        Some(Ok(builder)) => {
                            self.relational_index_shadow.recovery_builder = Some(builder);
                        }
                        Some(Err(error)) => {
                            self.relational_index_shadow.recovery_status =
                                RelationalIndexShadowRecoveryStatus::RecoveryUnavailable {
                                    base_generation: manifest.generation,
                                    base_commit_epoch: manifest.source_commit_epoch,
                                    recovered_commit_epoch: self.commit_epoch,
                                    reason: error.to_string(),
                                };
                            return;
                        }
                        None => {}
                    }
                    self.relational_index_shadow.recovery_status =
                        RelationalIndexShadowRecoveryStatus::CheckpointReady {
                            generation: manifest.generation,
                            source_commit_epoch: manifest.source_commit_epoch,
                            index_roots: manifest.roots.len(),
                            page_count: manifest.page_count,
                        };
                } else if manifest.generation <= checkpoint_generation
                    && manifest.source_commit_epoch <= checkpoint_commit_epoch
                {
                    self.relational_index_shadow.read_view = None;
                    self.relational_index_shadow.recovery_status =
                        RelationalIndexShadowRecoveryStatus::Stale {
                            generation: manifest.generation,
                            source_commit_epoch: manifest.source_commit_epoch,
                            checkpoint_generation,
                            checkpoint_commit_epoch,
                        };
                } else {
                    self.relational_index_shadow.read_view = None;
                    self.discard_invalid_relational_index_shadow(
                        &manifest_path,
                        read_only,
                        format!(
                            "shadow generation/epoch {}/{} is ahead of checkpoint {checkpoint_generation}/{checkpoint_commit_epoch}",
                            manifest.generation, manifest.source_commit_epoch
                        ),
                    );
                }
            }
            Err(error) => self.discard_invalid_relational_index_shadow(
                &manifest_path,
                read_only,
                error.to_string(),
            ),
        }
    }

    fn discard_invalid_relational_index_shadow(
        &mut self,
        manifest_path: &std::path::Path,
        read_only: bool,
        error: String,
    ) {
        self.relational_index_shadow.expected_previous_generation = None;
        self.relational_index_shadow.recovery_builder = None;
        self.relational_index_shadow.read_view = None;
        if read_only {
            self.relational_index_shadow.recovery_status =
                RelationalIndexShadowRecoveryStatus::InvalidReadOnly { error };
            return;
        }
        match fs::remove_file(manifest_path)
            .and_then(|()| skein_storage::sync_parent_directory(manifest_path))
        {
            Ok(()) => {
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::DiscardedInvalid { error };
            }
            Err(remove_error) => {
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::InvalidWritable {
                        error: format!(
                            "{error}; failed to discard invalid shadow manifest: {remove_error}"
                        ),
                    };
            }
        }
    }

    pub(super) fn record_relational_index_shadow_checkpoint(
        &mut self,
        generation: u64,
        source_commit_epoch: u64,
    ) {
        if !self.relational_index_shadow.enabled {
            return;
        }
        let Some(durable) = self.durable.as_ref() else {
            return;
        };
        let result = RelationalIndexShadowWriter::new(RelationalIndexShadowConfig::default())
            .publish(
                durable.root_path(),
                &self.relational_state,
                generation,
                source_commit_epoch,
                self.relational_index_shadow.expected_previous_generation,
            );
        match result {
            Ok(report) => {
                self.relational_index_shadow.recovery_builder = None;
                self.relational_index_shadow.recovery_report = None;
                self.relational_index_shadow.expected_previous_generation = Some(report.generation);
                match self.open_base_relational_index_read_view() {
                    Ok(view) => {
                        self.relational_index_shadow.read_view = Some(view);
                        self.relational_index_shadow.recovery_status =
                            RelationalIndexShadowRecoveryStatus::CheckpointReady {
                                generation: report.generation,
                                source_commit_epoch: report.source_commit_epoch,
                                index_roots: report.index_roots,
                                page_count: report.pages_written,
                            };
                    }
                    Err(error) => {
                        self.relational_index_shadow.read_view = None;
                        self.relational_index_shadow.recovery_status =
                            RelationalIndexShadowRecoveryStatus::InvalidWritable {
                                error: format!(
                                    "published relational index shadow could not be pinned: {error}"
                                ),
                            };
                    }
                }
                self.relational_index_shadow.checkpoint_report =
                    Some(RelationalIndexShadowCheckpointReport::published(report));
            }
            Err(error) => {
                self.relational_index_shadow.read_view = None;
                self.relational_index_shadow.checkpoint_report =
                    Some(RelationalIndexShadowCheckpointReport::failed(
                        generation,
                        source_commit_epoch,
                        error.to_string(),
                    ));
            }
        }
    }

    pub fn relational_index_shadow_checkpoint_report(
        &self,
    ) -> Option<&RelationalIndexShadowCheckpointReport> {
        self.relational_index_shadow.checkpoint_report.as_ref()
    }

    pub fn relational_index_shadow_recovery_status(&self) -> &RelationalIndexShadowRecoveryStatus {
        &self.relational_index_shadow.recovery_status
    }

    pub fn relational_index_recovery_report(&self) -> Option<&RelationalIndexRecoveryReport> {
        self.relational_index_shadow.recovery_report.as_ref()
    }

    pub(super) fn stage_recovered_relational_transaction(
        &mut self,
        transaction: RelationalTransaction,
        expected_epoch: u64,
    ) -> Result<(), skein_storage::RelationalError> {
        let Some(builder) = self.relational_index_shadow.recovery_builder.as_ref() else {
            self.relational_state = self.relational_state.stage_transaction(
                transaction,
                self.relational_mutation_limits,
                self.relational_overflow_config,
            )?;
            return Ok(());
        };
        let capture_limits = builder.capture_limits();
        let (next, capture) = self.relational_state.stage_transaction_with_index_changes(
            transaction,
            self.relational_mutation_limits,
            self.relational_overflow_config,
            capture_limits,
        )?;
        self.relational_state = next;
        if let Some(mut builder) = self.relational_index_shadow.recovery_builder.take() {
            if let Err(error) = builder.record(expected_epoch, capture) {
                self.mark_relational_index_recovery_unavailable(expected_epoch, error.to_string());
            } else {
                self.relational_index_shadow.recovery_builder = Some(builder);
            }
        }
        Ok(())
    }

    pub(super) fn invalidate_relational_index_recovery(
        &mut self,
        recovered_commit_epoch: u64,
        reason: impl Into<String>,
    ) {
        if self.relational_index_shadow.recovery_builder.is_some() {
            self.mark_relational_index_recovery_unavailable(recovered_commit_epoch, reason.into());
        }
    }

    pub(super) fn finish_relational_index_recovery(&mut self) {
        let Some(builder) = self.relational_index_shadow.recovery_builder.take() else {
            if let RelationalIndexShadowRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                ..
            } = self.relational_index_shadow.recovery_status
                && self.commit_epoch > source_commit_epoch
            {
                self.relational_index_shadow.read_view = None;
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::RecoveryUnavailable {
                        base_generation: generation,
                        base_commit_epoch: source_commit_epoch,
                        recovered_commit_epoch: self.commit_epoch,
                        reason: "read-only recovery cannot publish derived WAL index deltas"
                            .to_string(),
                    };
            }
            return;
        };
        if self.commit_epoch == builder.base_commit_epoch() {
            return;
        }
        match builder.finish(self.commit_epoch) {
            Ok(report) => {
                match self.open_recovered_relational_index_read_view(report.recovered_commit_epoch)
                {
                    Ok(view) => {
                        self.relational_index_shadow.read_view = Some(view);
                        self.relational_index_shadow.recovery_status =
                            RelationalIndexShadowRecoveryStatus::WalRecovered {
                                base_generation: report.base_generation,
                                base_commit_epoch: report.base_commit_epoch,
                                recovered_commit_epoch: report.recovered_commit_epoch,
                                delta_pages: report.delta_pages,
                                delta_entries: report.delta_entries,
                                peak_dirty_bytes: report.peak_dirty_bytes,
                            };
                        self.relational_index_shadow.recovery_report = Some(report);
                    }
                    Err(error) => self.mark_relational_index_recovery_unavailable(
                        self.commit_epoch,
                        format!("recovered relational index view could not be pinned: {error}"),
                    ),
                }
            }
            Err(error) => {
                self.mark_relational_index_recovery_unavailable(
                    self.commit_epoch,
                    error.to_string(),
                );
            }
        }
    }

    fn mark_relational_index_recovery_unavailable(
        &mut self,
        recovered_commit_epoch: u64,
        reason: String,
    ) {
        let (base_generation, base_commit_epoch) =
            match &self.relational_index_shadow.recovery_status {
                RelationalIndexShadowRecoveryStatus::CheckpointReady {
                    generation,
                    source_commit_epoch,
                    ..
                } => (*generation, *source_commit_epoch),
                RelationalIndexShadowRecoveryStatus::WalRecovered {
                    base_generation,
                    base_commit_epoch,
                    ..
                }
                | RelationalIndexShadowRecoveryStatus::RecoveryUnavailable {
                    base_generation,
                    base_commit_epoch,
                    ..
                } => (*base_generation, *base_commit_epoch),
                _ => return,
            };
        self.relational_index_shadow.recovery_builder = None;
        self.relational_index_shadow.recovery_report = None;
        self.relational_index_shadow.read_view = None;
        self.relational_index_shadow.recovery_status =
            RelationalIndexShadowRecoveryStatus::RecoveryUnavailable {
                base_generation,
                base_commit_epoch,
                recovered_commit_epoch,
                reason,
            };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Catalog;
    use skein_storage::{
        DurabilityPolicy, RelationalColumnSchema, RelationalIndexSchema, RelationalScalarType,
        RelationalTableSchema, RelationalTransaction, RelationalValue, RelationalWrite,
        WalReplayConfig,
    };

    #[test]
    fn checkpoint_double_writes_relational_index_shadow_without_serving_it() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-store-relational-index-shadow-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_shadow_checkpoint: true,
            ..WalReplayConfig::default()
        };
        let published_identity;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open shadow-enabled store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![
                            RelationalWrite::CreateTable(RelationalTableSchema {
                                name: "documents".to_string(),
                                columns: vec![
                                    RelationalColumnSchema {
                                        name: "id".to_string(),
                                        scalar_type: RelationalScalarType::Text,
                                        nullable: false,
                                        default: None,
                                    },
                                    RelationalColumnSchema {
                                        name: "owner".to_string(),
                                        scalar_type: RelationalScalarType::Text,
                                        nullable: false,
                                        default: None,
                                    },
                                ],
                                primary_key: vec!["id".to_string()],
                                unique_constraints: Vec::new(),
                                foreign_keys: Vec::new(),
                                indexes: vec![RelationalIndexSchema {
                                    name: "documents_owner_idx".to_string(),
                                    columns: vec!["owner".to_string()],
                                    unique: false,
                                }],
                            }),
                            RelationalWrite::Insert {
                                table: "documents".to_string(),
                                rows: vec![skein_storage::RelationalRow::new(vec![
                                    RelationalValue::Text("doc-1".to_string()),
                                    RelationalValue::Text("owner-1".to_string()),
                                ])],
                                mode: skein_storage::RelationalInsertMode::Error,
                            },
                        ],
                    },
                )
                .expect("commit relational source");
            store
                .checkpoint(&catalog)
                .expect("publish canonical checkpoint");
            let report = store
                .relational_index_shadow_checkpoint_report()
                .expect("shadow report");
            assert_eq!(
                report.status,
                RelationalIndexShadowCheckpointStatus::Published
            );
            assert_eq!(report.index_roots, 2);
            assert!(path.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE).exists());
            let view = current_index_view(&store);
            assert_eq!(view.kind(), RelationalIndexReadViewKind::Base);
            assert_eq!(view.identity().base_generation, report.generation);
            assert_eq!(view.identity().delta_generation, None);
            assert_eq!(view.identity().base_commit_epoch, 1);
            assert_eq!(view.identity().visible_commit_epoch, 1);
            published_identity = view.identity();
            let snapshot = store.snapshot();
            assert!(Arc::ptr_eq(view, current_index_view(&snapshot)));
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("reopen with bounded shadow validation");
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::CheckpointReady { index_roots: 2, .. }
            ));
            assert_eq!(
                current_index_view(&store).kind(),
                RelationalIndexReadViewKind::Base
            );
            assert_eq!(current_index_view(&store).identity(), published_identity);
            assert_eq!(store.relational_state().row_count("documents"), 1);
        }

        std::fs::write(path.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE), b"corrupt")
            .expect("corrupt derived manifest");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("canonical open must survive corrupt non-serving shadow");
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::DiscardedInvalid { .. }
            ));
            assert_eq!(store.relational_state().row_count("documents"), 1);
            store
                .checkpoint(&catalog)
                .expect("rebuild discarded shadow");
            assert_eq!(
                store
                    .relational_index_shadow_checkpoint_report()
                    .expect("rebuild report")
                    .status,
                RelationalIndexShadowCheckpointStatus::Published
            );
        }
        std::fs::remove_dir_all(path).expect("remove shadow checkpoint fixture");
    }

    #[test]
    fn reopen_publishes_bounded_relational_index_wal_deltas_after_replay() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-store-relational-index-recovery-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_shadow_checkpoint: true,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open recovery-enabled store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit recovery base");
            store
                .checkpoint(&catalog)
                .expect("checkpoint recovery base");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::Insert {
                            table: "documents".to_string(),
                            rows: vec![skein_storage::RelationalRow::new(vec![
                                RelationalValue::Text("doc-2".to_string()),
                                RelationalValue::Text("owner-1".to_string()),
                            ])],
                            mode: skein_storage::RelationalInsertMode::Error,
                        }],
                    },
                )
                .expect("append relational WAL after checkpoint");
            assert!(store
                .relational_index_shadow
                .current_read_view(store.commit_epoch)
                .is_none());
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("replay relational WAL and publish index deltas");
            assert_eq!(store.relational_state().row_count("documents"), 2);
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::WalRecovered {
                    base_commit_epoch: 1,
                    recovered_commit_epoch: 2,
                    delta_pages: 1,
                    delta_entries: 2,
                    ..
                }
            ));
            let report = store
                .relational_index_recovery_report()
                .expect("recovery evidence report");
            assert_eq!(report.delta_entries, 2);
            assert!(report.peak_dirty_bytes > 0);
            let view = current_index_view(&store);
            assert_eq!(view.kind(), RelationalIndexReadViewKind::Recovered);
            assert!(view.identity().delta_generation.is_some());
            assert_eq!(view.identity().base_commit_epoch, 1);
            assert_eq!(view.identity().visible_commit_epoch, 2);
            assert!(path
                .join(skein_storage::RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE)
                .exists());
        }
        std::fs::remove_dir_all(path).expect("remove recovery replay fixture");
    }

    #[test]
    fn schema_wal_invalidates_shadow_recovery_without_blocking_canonical_open() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-store-relational-index-schema-recovery-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_shadow_checkpoint: true,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open schema recovery store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit schema recovery base");
            store.checkpoint(&catalog).expect("checkpoint schema base");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::CreateIndex {
                            table: "documents".to_string(),
                            index: RelationalIndexSchema {
                                name: "documents_id_idx".to_string(),
                                columns: vec!["id".to_string()],
                                unique: false,
                            },
                        }],
                    },
                )
                .expect("append schema-changing relational WAL");
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("canonical recovery survives derived schema invalidation");
            assert!(store
                .relational_state()
                .table_schema("documents")
                .expect("recovered documents schema")
                .indexes
                .iter()
                .any(|index| index.name == "documents_id_idx"));
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::RecoveryUnavailable { reason, .. }
                    if reason.contains("schema-changing WAL")
            ));
            assert!(store.relational_index_recovery_report().is_none());
            assert!(store
                .relational_index_shadow
                .current_read_view(store.commit_epoch)
                .is_none());
        }
        std::fs::remove_dir_all(path).expect("remove schema recovery fixture");
    }

    fn create_recovery_documents_table(first_id: &str) -> RelationalTransaction {
        RelationalTransaction {
            writes: vec![
                RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![
                        RelationalColumnSchema {
                            name: "id".to_string(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        },
                        RelationalColumnSchema {
                            name: "owner".to_string(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        },
                    ],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: vec![RelationalIndexSchema {
                        name: "documents_owner_idx".to_string(),
                        columns: vec!["owner".to_string()],
                        unique: false,
                    }],
                }),
                RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![skein_storage::RelationalRow::new(vec![
                        RelationalValue::Text(first_id.to_string()),
                        RelationalValue::Text("owner-1".to_string()),
                    ])],
                    mode: skein_storage::RelationalInsertMode::Error,
                },
            ],
        }
    }

    fn current_index_view(store: &GraphStore) -> &Arc<RelationalIndexReadView> {
        store
            .relational_index_shadow
            .current_read_view(store.commit_epoch)
            .expect("store has a generation-pinned relational index view")
    }
}
