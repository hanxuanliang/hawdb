//! Non-serving relational index-page shadow publication.
//!
//! The shadow is deliberately derived: canonical checkpoint success never
//! depends on it and SQL never reads it in this stage. A valid shadow is
//! generation/epoch fenced to the checkpoint that supplied its rows.

use super::GraphStore;
use skein_storage::{
    RelationalIndexShadowBuildReport, RelationalIndexShadowConfig, RelationalIndexShadowReader,
    RelationalIndexShadowWriter, RELATIONAL_INDEX_SHADOW_MANIFEST_FILE,
};
use std::fs;

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
    recovery_status: RelationalIndexShadowRecoveryStatus,
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
}

impl GraphStore {
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
        match RelationalIndexShadowReader::open_latest(
            &root,
            RelationalIndexShadowConfig::default(),
        ) {
            Ok(reader) => {
                let manifest = reader.manifest();
                self.relational_index_shadow.expected_previous_generation =
                    Some(manifest.generation);
                if manifest.generation == checkpoint_generation
                    && manifest.source_commit_epoch == checkpoint_commit_epoch
                {
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
                    self.relational_index_shadow.recovery_status =
                        RelationalIndexShadowRecoveryStatus::Stale {
                            generation: manifest.generation,
                            source_commit_epoch: manifest.source_commit_epoch,
                            checkpoint_generation,
                            checkpoint_commit_epoch,
                        };
                } else {
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
                self.relational_index_shadow.expected_previous_generation = Some(report.generation);
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::CheckpointReady {
                        generation: report.generation,
                        source_commit_epoch: report.source_commit_epoch,
                        index_roots: report.index_roots,
                        page_count: report.pages_written,
                    };
                self.relational_index_shadow.checkpoint_report =
                    Some(RelationalIndexShadowCheckpointReport::published(report));
            }
            Err(error) => {
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
}
