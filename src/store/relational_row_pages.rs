//! Shadow recovery state for canonical relational row-page roots.

use super::{GraphStore, RelationalRowStorageResidencyReport};
use skein_storage::{
    RelationalOverflowRootReader, RelationalRowChangeCapture, RelationalRowChangeCaptureLimits,
    RelationalRowDeltaBuilder, RelationalRowDeltaConfig, RelationalRowDeltaError,
    RelationalRowDeltaReader, RelationalRowDeltaReport, RelationalRowPageLiveError,
    RelationalRowPageMutationPlanner, RelationalRowPagePublicationConfig,
    RelationalRowPageReadView, RelationalRowPageReadViewIdentity, RelationalRowPageRootReader,
    RelationalRowPageSnapshotReader, RelationalRowPageTableDelta, RelationalState, SegmentCache,
    StorageResidencyMode, StoreId,
};
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
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
        delta_generation: u64,
        base_commit_epoch: u64,
        recovered_commit_epoch: u64,
        delta_runs: usize,
        delta_entries: u64,
        peak_dirty_bytes: Option<usize>,
    },
    LiveCurrent {
        base_generation: u64,
        base_commit_epoch: u64,
        visible_commit_epoch: u64,
        live_batches: usize,
        live_entries: usize,
        live_encoded_bytes: usize,
        live_resident_bytes: usize,
    },
    LiveUnavailable {
        base_generation: u64,
        base_commit_epoch: u64,
        last_visible_commit_epoch: u64,
        failed_commit_epoch: u64,
        checkpoint_required: bool,
        reason: String,
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
        checkpoint_required: bool,
        reason: String,
    },
}

#[derive(Debug, Default)]
pub(super) struct RelationalRowPageState {
    recovery_builder: Option<RelationalRowDeltaBuilder>,
    read_view: Option<Arc<RelationalRowPageReadView>>,
    serving_resources: Option<Arc<RelationalRowPageServingResources>>,
    live_limits: RelationalRowChangeCaptureLimits,
    delta_config: RelationalRowDeltaConfig,
    recovery_report: Option<RelationalRowDeltaReport>,
    recovery_status: RelationalRowPageRecoveryStatus,
    schema_checkpoint_required: bool,
}

#[derive(Debug)]
struct RelationalRowPageServingResources {
    base_overflow: Arc<RelationalOverflowRootReader>,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
}

pub(super) struct RelationalRowPageCheckpointPlan {
    pub base: Option<Arc<RelationalRowPageRootReader>>,
    pub deltas: Vec<RelationalRowPageTableDelta>,
}

impl RelationalRowPageState {
    fn current_read_view(&self, commit_epoch: u64) -> Option<&Arc<RelationalRowPageReadView>> {
        self.read_view
            .as_ref()
            .filter(|view| view.identity().visible_commit_epoch == commit_epoch)
    }

    pub(super) fn residency_report(
        &self,
        commit_epoch: u64,
        state: &RelationalState,
    ) -> RelationalRowStorageResidencyReport {
        let mut report = RelationalRowStorageResidencyReport {
            materialized_rows_resident: state.materialized_rows_resident(),
            checkpoint_state_metadata_only: state.canonical_row_metadata_only(),
            materialized_row_count: state.materialized_row_count(),
            materialized_row_bytes: state.estimated_materialized_row_bytes(),
            logical_row_count: state.total_row_count(),
            ..RelationalRowStorageResidencyReport::default()
        };
        let Some(view) = self.current_read_view(commit_epoch) else {
            return report;
        };
        let Some(resources) = self.serving_resources.as_ref() else {
            return report;
        };
        let identity = view.identity();
        let base = view.base().manifest();
        let overflow = resources.base_overflow.manifest();
        let recovery = view.recovery_delta().map(|delta| delta.manifest());
        report.serving = true;
        report.base_generation = Some(identity.base_generation);
        report.recovery_delta_generation = identity.delta_generation;
        report.base_commit_epoch = Some(identity.base_commit_epoch);
        report.visible_commit_epoch = Some(identity.visible_commit_epoch);
        report.root_page_count = base.root_page_count;
        report.page_artifact_bytes = base.page_artifact.encoded_len;
        report.root_descriptor_artifact_bytes = base.root_descriptor_artifact.encoded_len;
        report.root_key_artifact_bytes = base.root_key_artifact.encoded_len;
        report.overflow_extent_count = overflow.extent_count;
        report.overflow_extent_artifact_bytes = overflow.extent_artifact.encoded_len;
        report.overflow_descriptor_artifact_bytes = overflow.descriptor_artifact.encoded_len;
        report.recovery_delta_runs = recovery.map_or(0, |manifest| manifest.run_count());
        report.recovery_delta_entries = recovery.map_or(0, |manifest| manifest.total_entries());
        report.recovery_delta_artifact_bytes =
            recovery.map_or(0, |manifest| manifest.artifact_bytes());
        report.live_batches = view.live_batch_count();
        report.live_entries = view.live_entry_count();
        report.live_encoded_bytes = view.live_encoded_bytes();
        report.live_resident_bytes = view.live_resident_bytes();
        report
    }

    pub(super) fn snapshot_at_epoch(&self, commit_epoch: u64) -> Self {
        Self {
            recovery_builder: None,
            read_view: self
                .read_view
                .as_ref()
                .filter(|view| view.identity().visible_commit_epoch == commit_epoch)
                .cloned(),
            serving_resources: self.serving_resources.clone(),
            live_limits: self.live_limits,
            delta_config: self.delta_config,
            recovery_report: self.recovery_report.clone(),
            recovery_status: self.recovery_status.clone(),
            schema_checkpoint_required: self.schema_checkpoint_required,
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
            RelationalRowPageRecoveryStatus::LiveCurrent {
                base_generation,
                base_commit_epoch,
                ..
            } => (Some(*base_generation), Some(*base_commit_epoch)),
            RelationalRowPageRecoveryStatus::LiveUnavailable {
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

    fn stage_live_publication(
        &self,
        current_epoch: u64,
        next_epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
    ) -> Option<Result<Arc<RelationalRowPageReadView>, RelationalRowLiveUnavailable>> {
        let view = self.current_read_view(current_epoch)?;
        if let Some(RelationalRowChangeCapture::RequiresCheckpoint { tables }) = capture.as_ref() {
            return Some(Err(RelationalRowLiveUnavailable {
                identity: view.identity(),
                failed_commit_epoch: next_epoch,
                error: RelationalRowPageLiveError::RequiresCheckpoint {
                    tables: tables.clone(),
                },
            }));
        }
        Some(
            view.advance(next_epoch, capture, self.live_limits)
                .map(Arc::new)
                .map_err(|error| RelationalRowLiveUnavailable {
                    identity: view.identity(),
                    failed_commit_epoch: next_epoch,
                    error,
                }),
        )
    }
}

pub(super) struct RelationalRowLiveUnavailable {
    identity: RelationalRowPageReadViewIdentity,
    failed_commit_epoch: u64,
    error: RelationalRowPageLiveError,
}

impl GraphStore {
    pub(super) fn activate_read_only_out_of_core_rows(&mut self) -> crate::error::Result<()> {
        let read_only = self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.read_only);
        if !read_only
            || self.residency_mode != StorageResidencyMode::OutOfCore
            || !matches!(
                self.relational_checkpoint_index_load(),
                skein_storage::RelationalCheckpointIndexLoad::OmitMaterializedPostings
            )
            || self.relational_state.is_empty()
        {
            return Ok(());
        }
        self.open_relational_row_snapshot_reader()?.ok_or_else(|| {
            crate::error::SkeinError::StorageIntegrity(
                "read-only out-of-core relational activation requires a canonical row view"
                    .to_string(),
            )
        })?;
        if !self
            .relational_index_shadow
            .residency_report(self.commit_epoch)
            .serving
        {
            return Err(crate::error::SkeinError::StorageIntegrity(
                "read-only out-of-core relational activation requires an authoritative index view"
                    .to_string(),
            ));
        }
        self.relational_state.omit_materialized_rows();
        Ok(())
    }

    pub(super) fn plan_relational_row_page_checkpoint(
        &self,
        base: Option<RelationalRowPageRootReader>,
        generation: u64,
        source_commit_epoch: u64,
        config: RelationalRowPagePublicationConfig,
    ) -> crate::error::Result<RelationalRowPageCheckpointPlan> {
        let Some(base) = base else {
            return self.plan_relational_row_page_rebuild(generation, source_commit_epoch, config);
        };
        let Some(view) = self.relational_row_pages.read_view.as_ref() else {
            return self.plan_relational_row_page_rebuild(generation, source_commit_epoch, config);
        };
        let base = Arc::new(base);
        let identity = view.identity();
        let base_manifest = base.manifest();
        if identity.base_generation != base_manifest.generation
            || identity.base_commit_epoch != base_manifest.source_commit_epoch
            || identity.root_set_digest != base_manifest.root_set_digest
            || identity.visible_commit_epoch != source_commit_epoch
        {
            return Err(crate::error::SkeinError::Storage(format!(
                "relational row checkpoint view {identity:?} does not match base {}/{}/{} at source epoch {source_commit_epoch}",
                base_manifest.generation,
                base_manifest.source_commit_epoch,
                base_manifest.root_set_digest,
            )));
        }
        let capture = view
            .checkpoint_capture(
                |table, primary_key| self.relational_state.row(table, primary_key).cloned(),
                self.relational_row_pages.live_limits,
            )
            .map_err(|error| crate::error::SkeinError::Storage(error.to_string()))?;
        let RelationalRowChangeCapture::Captured { changes, .. } = capture else {
            return Err(crate::error::SkeinError::Storage(
                "relational row checkpoint capture was unexpectedly invalidated".to_string(),
            ));
        };
        let mut changes_by_table = BTreeMap::<String, Vec<_>>::new();
        for change in changes {
            changes_by_table
                .entry(change.table.clone())
                .or_default()
                .push(change);
        }
        if changes_by_table.len() > config.max_tables.get() {
            return Err(crate::error::SkeinError::Storage(format!(
                "relational row checkpoint changes reference {} tables, exceeding limit {}",
                changes_by_table.len(),
                config.max_tables
            )));
        }
        let mut deltas = Vec::with_capacity(changes_by_table.len());
        let mut planned_dirty_pages = 0usize;
        let mut planned_dirty_bytes = 0u64;
        let slot_bytes = config.page_limits.max_page_bytes.get() as u64;
        for (table, changes) in changes_by_table {
            let remaining_pages = config
                .max_dirty_pages
                .get()
                .checked_sub(planned_dirty_pages)
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| {
                    crate::error::SkeinError::Storage(format!(
                        "relational row checkpoint exhausted its {} dirty-page limit before planning table {table}",
                        config.max_dirty_pages
                    ))
                })?;
            let remaining_bytes = config
                .max_dirty_bytes
                .get()
                .checked_sub(planned_dirty_bytes)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| {
                    crate::error::SkeinError::Storage(format!(
                        "relational row checkpoint exhausted its {} dirty-byte limit before planning table {table}",
                        config.max_dirty_bytes
                    ))
                })?;
            let planner = RelationalRowPageMutationPlanner::new(
                Some(&base),
                generation,
                source_commit_epoch,
                RelationalRowPagePublicationConfig {
                    max_dirty_pages: remaining_pages,
                    max_dirty_bytes: remaining_bytes,
                    ..config
                },
            )
            .map_err(|error| crate::error::SkeinError::Storage(error.to_string()))?;
            let schema = self.relational_state.table_schema(&table).ok_or_else(|| {
                crate::error::SkeinError::Storage(format!(
                    "relational row checkpoint change references missing table {table}"
                ))
            })?;
            let schema_digest = self
                .relational_state
                .table_schema_digest(&table)
                .map_err(|error| crate::error::SkeinError::Storage(error.to_string()))?
                .ok_or_else(|| {
                    crate::error::SkeinError::Storage(format!(
                        "relational row checkpoint cannot derive schema digest for {table}"
                    ))
                })?;
            let plan = planner
                .plan_table(&table, schema_digest, schema.columns.len(), changes)
                .map_err(|error| crate::error::SkeinError::Storage(error.to_string()))?;
            planned_dirty_pages = planned_dirty_pages
                .checked_add(plan.dirty_pages)
                .ok_or_else(|| {
                    crate::error::SkeinError::Storage(
                        "relational row checkpoint dirty-page accounting overflow".to_string(),
                    )
                })?;
            let table_dirty_bytes = u64::try_from(plan.dirty_pages)
                .ok()
                .and_then(|pages| pages.checked_mul(slot_bytes))
                .ok_or_else(|| {
                    crate::error::SkeinError::Storage(
                        "relational row checkpoint dirty-byte accounting overflow".to_string(),
                    )
                })?;
            planned_dirty_bytes = planned_dirty_bytes
                .checked_add(table_dirty_bytes)
                .ok_or_else(|| {
                    crate::error::SkeinError::Storage(
                        "relational row checkpoint dirty-byte accounting overflow".to_string(),
                    )
                })?;
            deltas.push(plan.delta);
        }
        Ok(RelationalRowPageCheckpointPlan {
            base: Some(base),
            deltas,
        })
    }

    fn plan_relational_row_page_rebuild(
        &self,
        generation: u64,
        source_commit_epoch: u64,
        config: RelationalRowPagePublicationConfig,
    ) -> crate::error::Result<RelationalRowPageCheckpointPlan> {
        let deltas = self
            .relational_state
            .row_page_snapshot_deltas(generation, source_commit_epoch, config)
            .map_err(|error| crate::error::SkeinError::Storage(error.to_string()))?;
        Ok(RelationalRowPageCheckpointPlan { base: None, deltas })
    }

    pub(super) fn mount_relational_row_pages_for_recovery(&mut self) -> crate::error::Result<()> {
        let Some(durable) = self.durable.as_ref() else {
            return Ok(());
        };
        if durable.checkpoint_epoch == 0 {
            self.relational_row_pages = RelationalRowPageState::default();
            return Ok(());
        }
        let checkpoint_generation = durable.checkpoint_epoch;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        let read_only = durable.read_only;
        let root = durable.root_path().to_path_buf();
        let delta_config = self.relational_row_pages.delta_config;
        let overflow_root = Arc::new(durable.open_bound_relational_overflow()?);
        let reader = durable.open_bound_relational_row_pages(&overflow_root)?;
        let manifest = reader.manifest();
        if manifest.generation != checkpoint_generation
            || manifest.source_commit_epoch != checkpoint_commit_epoch
        {
            return Err(crate::error::SkeinError::Storage(format!(
                "canonical relational row root {}/{} does not match checkpoint {checkpoint_generation}/{checkpoint_commit_epoch}",
                manifest.generation, manifest.source_commit_epoch
            )));
        }
        let root_pages = manifest.root_page_count;
        let generation = manifest.generation;
        let source_commit_epoch = manifest.source_commit_epoch;
        RelationalRowDeltaBuilder::validate_base_state(
            &reader,
            &self.relational_state,
            delta_config,
        )
        .map_err(|error| {
            crate::error::SkeinError::Storage(format!(
                "canonical relational row root could not be pinned: {error}"
            ))
        })?;
        let expected_previous =
            match RelationalRowDeltaReader::latest_generation(&root, delta_config) {
                Ok(generation) => generation,
                Err(error) => {
                    self.mark_relational_row_page_recovery_unavailable(
                        self.commit_epoch,
                        format!("published relational row delta selector is invalid: {error}"),
                    );
                    return Ok(());
                }
            };
        let reader = Arc::new(reader);
        self.relational_row_pages.read_view = Some(Arc::new(RelationalRowPageReadView::from_base(
            Arc::clone(&reader),
        )));
        self.relational_row_pages.serving_resources =
            Some(Arc::new(RelationalRowPageServingResources {
                base_overflow: overflow_root,
                cache: Arc::clone(&durable.segment_cache),
                store_id: durable.store_id(),
            }));
        self.relational_row_pages.recovery_report = None;
        self.relational_row_pages.schema_checkpoint_required = false;
        self.relational_row_pages.recovery_status =
            RelationalRowPageRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                root_pages,
            };
        if !read_only {
            match RelationalRowDeltaBuilder::new_for_checkpoint_recovery(
                &root,
                &reader,
                expected_previous,
                &self.relational_state,
                delta_config,
            ) {
                Ok(builder) => self.relational_row_pages.recovery_builder = Some(builder),
                Err(error) => self.mark_relational_row_page_recovery_unavailable(
                    self.commit_epoch,
                    format!("relational row recovery builder could not start: {error}"),
                ),
            }
        }
        Ok(())
    }

    pub(super) fn relational_row_recovery_capture_limits(
        &self,
    ) -> Option<RelationalRowChangeCaptureLimits> {
        self.relational_row_pages
            .recovery_builder
            .as_ref()
            .map(RelationalRowDeltaBuilder::capture_limits)
            .or_else(|| {
                (self
                    .durable
                    .as_ref()
                    .is_some_and(|durable| durable.read_only)
                    && self.relational_row_pages.read_view.is_some())
                .then(|| self.relational_row_pages.delta_config.capture_limits())
            })
    }

    pub(super) fn record_relational_row_recovery_capture(
        &mut self,
        epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
    ) {
        let Some(capture) = capture else {
            return;
        };
        if let RelationalRowChangeCapture::RequiresCheckpoint { tables } = &capture {
            self.mark_relational_row_page_schema_checkpoint_required(epoch, tables.clone());
            return;
        }
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

    pub(super) fn relational_row_live_capture_limits(
        &self,
    ) -> Option<RelationalRowChangeCaptureLimits> {
        self.relational_row_pages
            .current_read_view(self.commit_epoch)
            .map(|_| self.relational_row_pages.live_limits)
    }

    pub(super) fn stage_relational_row_live_publication(
        &self,
        next_epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
    ) -> Option<Result<Arc<RelationalRowPageReadView>, RelationalRowLiveUnavailable>> {
        self.relational_row_pages
            .stage_live_publication(self.commit_epoch, next_epoch, capture)
    }

    pub(super) fn require_relational_row_live_publication(
        &self,
        next_epoch: u64,
        publication: &Option<Result<Arc<RelationalRowPageReadView>, RelationalRowLiveUnavailable>>,
    ) -> crate::error::Result<()> {
        match publication {
            Some(Ok(view)) if view.identity().visible_commit_epoch == next_epoch => Ok(()),
            Some(Ok(view)) => Err(crate::error::SkeinError::StorageIntegrity(format!(
                "canonical relational row view staged visible epoch {} for commit {next_epoch}",
                view.identity().visible_commit_epoch
            ))),
            Some(Err(unavailable))
                if matches!(
                    &unavailable.error,
                    RelationalRowPageLiveError::RequiresCheckpoint { .. }
                ) =>
            {
                Ok(())
            }
            Some(Err(unavailable)) => match &unavailable.error {
                RelationalRowPageLiveError::Corrupt(_) => {
                    Err(crate::error::SkeinError::StorageIntegrity(format!(
                        "canonical relational row view could not stage commit {next_epoch}: {}",
                        unavailable.error
                    )))
                }
                RelationalRowPageLiveError::Admission(_)
                | RelationalRowPageLiveError::Invalidated(_) => {
                    Err(crate::error::SkeinError::Storage(format!(
                        "canonical relational row view rejected commit {next_epoch} before WAL append: {}",
                        unavailable.error
                    )))
                }
                RelationalRowPageLiveError::RequiresCheckpoint { .. } => unreachable!(),
            },
            None if matches!(
                self.relational_row_pages.recovery_status,
                RelationalRowPageRecoveryStatus::Missing
            ) => Ok(()),
            None => Err(crate::error::SkeinError::StorageIntegrity(format!(
                "canonical relational row view has no current reader for commit {next_epoch}: {:?}",
                self.relational_row_pages.recovery_status
            ))),
        }
    }

    pub(super) fn publish_relational_row_live_view(
        &mut self,
        publication: Option<Result<Arc<RelationalRowPageReadView>, RelationalRowLiveUnavailable>>,
    ) {
        match publication {
            None => {}
            Some(Ok(view)) => {
                let identity = view.identity();
                self.relational_row_pages.schema_checkpoint_required = false;
                self.relational_row_pages.recovery_status =
                    RelationalRowPageRecoveryStatus::LiveCurrent {
                        base_generation: identity.base_generation,
                        base_commit_epoch: identity.base_commit_epoch,
                        visible_commit_epoch: identity.visible_commit_epoch,
                        live_batches: view.live_batch_count(),
                        live_entries: view.live_entry_count(),
                        live_encoded_bytes: view.live_encoded_bytes(),
                        live_resident_bytes: view.live_resident_bytes(),
                    };
                self.relational_row_pages.read_view = Some(view);
            }
            Some(Err(unavailable)) => {
                let checkpoint_required = matches!(
                    &unavailable.error,
                    RelationalRowPageLiveError::RequiresCheckpoint { .. }
                );
                self.relational_row_pages.read_view = None;
                self.relational_row_pages.schema_checkpoint_required = checkpoint_required;
                self.relational_row_pages.recovery_status =
                    RelationalRowPageRecoveryStatus::LiveUnavailable {
                        base_generation: unavailable.identity.base_generation,
                        base_commit_epoch: unavailable.identity.base_commit_epoch,
                        last_visible_commit_epoch: unavailable.identity.visible_commit_epoch,
                        failed_commit_epoch: unavailable.failed_commit_epoch,
                        checkpoint_required,
                        reason: unavailable.error.to_string(),
                    };
            }
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
                match self.open_relational_row_delta_view(self.commit_epoch) {
                    Ok((view, delta)) => {
                        let manifest = delta.manifest();
                        self.relational_row_pages.read_view = Some(view);
                        self.relational_row_pages.recovery_status =
                            RelationalRowPageRecoveryStatus::WalRecovered {
                                base_generation: manifest.base.generation,
                                delta_generation: manifest.delta_generation,
                                base_commit_epoch: manifest.base.source_commit_epoch,
                                recovered_commit_epoch: manifest.visible_commit_epoch,
                                delta_runs: manifest.run_count(),
                                delta_entries: manifest.total_entries(),
                                peak_dirty_bytes: None,
                            };
                    }
                    Err(error) => {
                        self.relational_row_pages.read_view = None;
                        self.relational_row_pages.recovery_status =
                            RelationalRowPageRecoveryStatus::Unavailable {
                                base_generation: Some(generation),
                                base_commit_epoch: Some(source_commit_epoch),
                                recovered_commit_epoch: self.commit_epoch,
                                checkpoint_required: false,
                                reason: format!(
                                    "read-only recovery requires an exact published row delta: {error}"
                                ),
                            };
                    }
                }
            }
            return;
        };
        if self.commit_epoch == builder.base_commit_epoch() {
            return;
        }
        match builder.finish(self.commit_epoch, None) {
            Ok(report) => match self.open_relational_row_delta_view(self.commit_epoch) {
                Ok((view, _delta)) => {
                    self.relational_row_pages.read_view = Some(view);
                    self.relational_row_pages.recovery_status =
                        RelationalRowPageRecoveryStatus::WalRecovered {
                            base_generation: report.generation.base_generation,
                            delta_generation: report.generation.delta_generation,
                            base_commit_epoch: report.base_commit_epoch,
                            recovered_commit_epoch: report.visible_commit_epoch,
                            delta_runs: report.runs,
                            delta_entries: report.entries,
                            peak_dirty_bytes: Some(report.peak_dirty_bytes),
                        };
                    self.relational_row_pages.recovery_report = Some(report);
                }
                Err(error) => self.mark_relational_row_page_recovery_unavailable(
                    self.commit_epoch,
                    format!("published relational row delta could not be pinned: {error}"),
                ),
            },
            Err(error) => self.mark_relational_row_page_recovery_unavailable(
                self.commit_epoch,
                format!("relational row recovery could not finish: {error}"),
            ),
        }
    }

    fn open_relational_row_delta_view(
        &self,
        expected_visible_commit_epoch: u64,
    ) -> Result<
        (
            Arc<RelationalRowPageReadView>,
            Arc<RelationalRowDeltaReader>,
        ),
        RelationalRowDeltaError,
    > {
        let durable = self.durable.as_ref().ok_or_else(|| {
            RelationalRowDeltaError::Admission(
                "relational row delta recovery requires a durable store".to_string(),
            )
        })?;
        let base = self
            .relational_row_pages
            .read_view
            .as_ref()
            .map(|view| view.pinned_base())
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "relational row delta recovery requires a pinned row root".to_string(),
                )
            })?;
        let delta = RelationalRowDeltaReader::open_latest(
            durable.root_path(),
            &base,
            expected_visible_commit_epoch,
            self.relational_row_pages.delta_config,
        )?
        .ok_or_else(|| {
            RelationalRowDeltaError::Admission(
                "relational row delta selector is missing".to_string(),
            )
        })?;
        let delta = Arc::new(delta);
        let view = Arc::new(RelationalRowPageReadView::from_recovery_delta(
            base,
            Arc::clone(&delta),
        )?);
        Ok((view, delta))
    }

    fn mark_relational_row_page_recovery_unavailable(
        &mut self,
        recovered_commit_epoch: u64,
        reason: String,
    ) {
        self.mark_relational_row_page_unavailable(recovered_commit_epoch, false, reason);
    }

    fn mark_relational_row_page_schema_checkpoint_required(
        &mut self,
        recovered_commit_epoch: u64,
        tables: Vec<String>,
    ) {
        self.mark_relational_row_page_unavailable(
            recovered_commit_epoch,
            true,
            format!(
                "schema-changing WAL requires a canonical row checkpoint for tables {}",
                tables.join(",")
            ),
        );
    }

    fn mark_relational_row_page_unavailable(
        &mut self,
        recovered_commit_epoch: u64,
        checkpoint_required: bool,
        reason: String,
    ) {
        let (base_generation, base_commit_epoch) = self.relational_row_pages.base_identity();
        self.relational_row_pages.recovery_builder = None;
        self.relational_row_pages.read_view = None;
        self.relational_row_pages.schema_checkpoint_required = checkpoint_required;
        self.relational_row_pages.recovery_status = RelationalRowPageRecoveryStatus::Unavailable {
            base_generation,
            base_commit_epoch,
            recovered_commit_epoch,
            checkpoint_required,
            reason,
        };
    }

    pub(crate) fn relational_row_schema_checkpoint_required(&self) -> bool {
        self.relational_row_pages.schema_checkpoint_required
    }

    pub fn relational_row_page_recovery_status(&self) -> &RelationalRowPageRecoveryStatus {
        &self.relational_row_pages.recovery_status
    }

    pub fn relational_row_delta_recovery_report(&self) -> Option<&RelationalRowDeltaReport> {
        self.relational_row_pages.recovery_report.as_ref()
    }

    pub(crate) fn open_relational_row_snapshot_reader(
        &self,
    ) -> crate::error::Result<Option<RelationalRowPageSnapshotReader>> {
        let Some(view) = self
            .relational_row_pages
            .current_read_view(self.commit_epoch)
            .cloned()
        else {
            return match &self.relational_row_pages.recovery_status {
                RelationalRowPageRecoveryStatus::Missing => Ok(None),
                status => Err(crate::error::SkeinError::StorageIntegrity(format!(
                    "canonical relational row reader is unavailable at commit epoch {}: {status:?}",
                    self.commit_epoch
                ))),
            };
        };
        let resources = self
            .relational_row_pages
            .serving_resources
            .as_ref()
            .ok_or_else(|| {
                crate::error::SkeinError::StorageIntegrity(
                    "canonical relational row reader has no pinned serving resources".to_string(),
                )
            })?;
        RelationalRowPageSnapshotReader::new(
            view,
            Arc::clone(&resources.base_overflow),
            None,
            Arc::clone(&resources.cache),
            resources.store_id,
        )
        .map(Some)
        .map_err(|error| {
            crate::error::SkeinError::StorageIntegrity(format!(
                "canonical relational row reader could not open: {error}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Catalog;
    use crate::store::GraphStore;
    use skein_core::RuntimeTaskContext;
    use skein_storage::{
        relational_overflow_extent_file, relational_overflow_manifest_generation_file,
        relational_row_page_manifest_generation_file, DurabilityPolicy, RelationalColumnSchema,
        RelationalHydrationBudget, RelationalIndexMode, RelationalInsertMode, RelationalKey,
        RelationalMutationLimits, RelationalOverflowConfig, RelationalRow,
        RelationalRowPagePublicationConfig, RelationalRowPagePublisher,
        RelationalRowPageRootReader, RelationalRowPageSnapshotReadLimits, RelationalScalarType,
        RelationalTableSchema, RelationalTransaction, RelationalValue, RelationalWrite,
        StorageResidencyMode, WalReplayConfig,
    };
    use std::collections::BTreeMap;

    #[test]
    fn durable_open_replays_wal_into_a_generation_pinned_row_delta() {
        let replay = WalReplayConfig::default();
        let path = seed_row_root_with_wal_insert("wal-delta", replay);

        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let recovery_status = store.relational_row_page_recovery_status();
        assert!(
            matches!(
                recovery_status,
                RelationalRowPageRecoveryStatus::WalRecovered {
                    base_generation: 1,
                    base_commit_epoch: 1,
                    recovered_commit_epoch: 2,
                    delta_entries: 1,
                    peak_dirty_bytes: Some(_),
                    ..
                }
            ),
            "unexpected recovery status: {recovery_status:?}"
        );
        let report = store.relational_row_delta_recovery_report().unwrap();
        assert_eq!(report.replayed_batches, 1);
        assert_eq!(report.entries, 1);
        assert_eq!(report.runs, 1);
        assert!(
            report.peak_dirty_bytes <= RelationalRowDeltaConfig::default().max_dirty_bytes.get()
        );
        let view = Arc::clone(store.relational_row_pages.read_view.as_ref().unwrap());
        assert!(matches!(
            view.overlay_value("documents", &key(2)).unwrap(),
            Some(skein_storage::RelationalRowPageRecoveredValue::Present(value))
                if value == row(2, "two")
        ));
        let snapshot = store.snapshot();
        assert!(Arc::ptr_eq(
            &view,
            snapshot.relational_row_pages.read_view.as_ref().unwrap()
        ));
        let reader = snapshot
            .open_relational_row_snapshot_reader()
            .unwrap()
            .expect("checkpoint snapshot reader");
        let mut hydration = RelationalHydrationBudget::default();
        let (projected, report) = reader
            .point_projected(
                "documents",
                &key(2),
                &[1],
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        assert_eq!(
            projected.unwrap().fields[0].value,
            RelationalValue::Text("two".to_string())
        );
        assert_eq!(
            report.identity.visible_commit_epoch,
            snapshot.commit_epoch()
        );
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
            RelationalRowPageRecoveryStatus::LiveCurrent {
                base_generation: 1,
                base_commit_epoch: 1,
                visible_commit_epoch: 3,
                live_batches: 1,
                live_entries: 1,
                ..
            }
        ));
        let current = store.relational_row_pages.read_view.as_ref().unwrap();
        assert!(matches!(
            current.overlay_value("documents", &key(3)).unwrap(),
            Some(skein_storage::RelationalRowPageRecoveredValue::Present(value))
                if value == row(3, "three")
        ));
        assert!(!Arc::ptr_eq(&view, current));
        assert!(Arc::ptr_eq(
            &view,
            snapshot.relational_row_pages.read_view.as_ref().unwrap()
        ));

        let before_graph_commit = Arc::clone(current);
        store
            .create_node(&mut catalog, "Note", BTreeMap::new())
            .unwrap();
        assert!(matches!(
            store.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::LiveCurrent {
                visible_commit_epoch: 4,
                live_batches: 1,
                live_entries: 1,
                ..
            }
        ));
        let after_graph_commit = store.relational_row_pages.read_view.as_ref().unwrap();
        assert_eq!(after_graph_commit.latest_live_commit_epoch(), Some(3));
        assert!(!Arc::ptr_eq(&before_graph_commit, after_graph_commit));
        assert!(matches!(
            after_graph_commit
                .overlay_value("documents", &key(3))
                .unwrap(),
            Some(skein_storage::RelationalRowPageRecoveredValue::Present(value))
                if value == row(3, "three")
        ));

        let pinned_epoch_four = Arc::clone(after_graph_commit);
        let pinned_before_ddl = store.snapshot();
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::AddColumn {
                        table: "documents".to_string(),
                        column: RelationalColumnSchema {
                            name: "archived".to_string(),
                            scalar_type: RelationalScalarType::Boolean,
                            nullable: false,
                            default: Some(RelationalValue::Boolean(false)),
                        },
                    }],
                },
            )
            .unwrap();
        assert!(matches!(
            store.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::LiveUnavailable {
                base_generation: 1,
                base_commit_epoch: 1,
                last_visible_commit_epoch: 4,
                failed_commit_epoch: 5,
                checkpoint_required: true,
                reason,
            } if reason.contains("requires a schema checkpoint")
        ));
        assert!(store.relational_row_pages.read_view.is_none());
        assert!(matches!(
            store.open_relational_row_snapshot_reader(),
            Err(crate::error::SkeinError::StorageIntegrity(_))
        ));
        assert!(Arc::ptr_eq(
            &pinned_epoch_four,
            pinned_before_ddl
                .relational_row_pages
                .read_view
                .as_ref()
                .unwrap()
        ));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn live_row_admission_rejects_before_wal_append() {
        let replay = WalReplayConfig::default();
        let path = seed_row_root_with_wal_insert("live-admission", replay);
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let commit_epoch = store.commit_epoch;
        let next_lsn = store.durable.as_ref().unwrap().next_lsn;
        let previous_limits = store.relational_row_pages.live_limits;
        store.relational_row_pages.live_limits = RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: previous_limits.max_bytes,
        };

        let error = store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![row(3, "three"), row(4, "four")],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("before WAL append"));
        assert_eq!(store.commit_epoch, commit_epoch);
        assert_eq!(store.durable.as_ref().unwrap().next_lsn, next_lsn);
        assert!(store.relational_state.row("documents", &key(3)).is_none());
        assert!(store.relational_state.row("documents", &key(4)).is_none());
        assert!(matches!(
            store.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::WalRecovered {
                recovered_commit_epoch: 2,
                ..
            }
        ));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn read_only_open_reuses_an_exact_published_row_delta() {
        let replay = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let path = seed_row_root_with_wal_insert("read-only-row-delta", replay);

        let mut writable_catalog = Catalog::default();
        let writable = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut writable_catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let recovery_status = writable.relational_row_page_recovery_status();
        assert!(
            matches!(
                recovery_status,
                RelationalRowPageRecoveryStatus::WalRecovered {
                    recovered_commit_epoch: 2,
                    peak_dirty_bytes: Some(_),
                    ..
                }
            ),
            "unexpected recovery status: {recovery_status:?}"
        );
        drop(writable);

        let mut read_only_catalog = Catalog::default();
        let read_only = GraphStore::open_read_only_with_durability_and_replay_config(
            &path,
            &mut read_only_catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                relational_index_mode: RelationalIndexMode::Shadow,
                ..WalReplayConfig::default()
            },
        )
        .unwrap();
        assert!(read_only.relational_state.materialized_rows_resident());
        assert!(!read_only.relational_state.canonical_row_metadata_only());
        assert!(matches!(
            read_only.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::WalRecovered {
                recovered_commit_epoch: 2,
                delta_entries: 1,
                peak_dirty_bytes: None,
                ..
            }
        ));
        let view = read_only.relational_row_pages.read_view.as_ref().unwrap();
        assert!(matches!(
            view.overlay_value("documents", &key(2)).unwrap(),
            Some(skein_storage::RelationalRowPageRecoveredValue::Present(value))
                if value == row(2, "two")
        ));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn read_only_out_of_core_authoritative_open_detaches_checkpoint_rows() {
        let path = unique_test_dir("read-only-detached-rows");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                relational_index_mode: RelationalIndexMode::Shadow,
                ..WalReplayConfig::default()
            },
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
                            rows: vec![row(1, "one"), row(2, "two")],
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);

        let mut catalog = Catalog::default();
        let read_only = GraphStore::open_read_only_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                relational_index_mode: RelationalIndexMode::Authoritative,
                ..WalReplayConfig::default()
            },
        )
        .unwrap();

        assert!(!read_only.relational_state.materialized_rows_resident());
        assert!(read_only.relational_state.canonical_row_metadata_only());
        assert_eq!(read_only.relational_state.materialized_row_count(), 0);
        assert_eq!(
            read_only
                .relational_state
                .estimated_materialized_row_bytes(),
            0
        );
        assert_eq!(read_only.relational_state.row_count("documents"), 2);
        assert_eq!(read_only.relational_state.total_row_count(), 2);
        let residency = read_only.storage_residency_report().relational_rows;
        assert!(residency.serving);
        assert!(!residency.materialized_rows_resident);
        assert!(residency.checkpoint_state_metadata_only);
        assert_eq!(residency.materialized_row_count, 0);
        assert_eq!(residency.materialized_row_bytes, 0);
        assert_eq!(residency.logical_row_count, 2);

        let reader = read_only
            .open_relational_row_snapshot_reader()
            .unwrap()
            .expect("canonical row reader");
        let mut hydration = RelationalHydrationBudget::default();
        let (projected, _) = reader
            .point_projected(
                "documents",
                &key(2),
                &[1],
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        assert_eq!(
            projected.unwrap().fields[0].value,
            RelationalValue::Text("two".to_string())
        );

        let mutation_error = read_only
            .relational_state
            .stage_transaction(
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![row(3, "three")],
                        mode: RelationalInsertMode::Error,
                    }],
                },
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
            .unwrap_err();
        assert!(mutation_error
            .to_string()
            .contains("requires materialized relational rows"));
        let qualification_error = read_only
            .qualify_relational_index_read_view(
                crate::store::RelationalIndexViewQualificationOptions::default(),
            )
            .unwrap_err();
        assert!(qualification_error
            .to_string()
            .contains("requires materialized relational rows"));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn canonical_checkpoint_rewrites_only_dirty_relational_pages() {
        let path = unique_test_dir("canonical-dirty-pages");
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
                            rows: (0..300).map(|id| row(id, &format!("body-{id}"))).collect(),
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let first = RelationalRowPageRootReader::open_generation(
            &path,
            1,
            RelationalRowPagePublicationConfig::default(),
        )
        .unwrap();
        assert_eq!(first.manifest().dirty_page_count, 2);
        assert_eq!(first.manifest().root_page_count, 2);

        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![row(1, "replacement")],
                        mode: RelationalInsertMode::Replace,
                    }],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let second = RelationalRowPageRootReader::open_generation(
            &path,
            2,
            RelationalRowPagePublicationConfig::default(),
        )
        .unwrap();
        assert_eq!(second.manifest().dirty_page_count, 1);
        assert_eq!(second.manifest().root_page_count, 2);
        assert_eq!(physical_generations(&second), vec![2, 1]);
        let descriptor = second
            .find_table_page_descriptor("documents", &key(1))
            .unwrap()
            .unwrap();
        let page = second.read_page(&descriptor).unwrap();
        assert_eq!(
            page.rows
                .iter()
                .find(|entry| entry.primary_key == key(1))
                .map(|entry| &entry.row),
            Some(&row(1, "replacement"))
        );
        let mismatch = store
            .plan_relational_row_page_checkpoint(
                Some(first),
                3,
                2,
                RelationalRowPagePublicationConfig::default(),
            )
            .err()
            .expect("a stale row root must not trigger a silent rebuild");
        assert!(mismatch.to_string().contains("does not match base"));

        store
            .create_node(&mut catalog, "CheckpointMarker", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let third = RelationalRowPageRootReader::open_generation(
            &path,
            3,
            RelationalRowPagePublicationConfig::default(),
        )
        .unwrap();
        assert_eq!(third.manifest().dirty_page_count, 0);
        assert_eq!(third.manifest().root_page_count, 2);
        assert_eq!(physical_generations(&third), vec![2, 1]);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn recovered_wal_rows_checkpoint_as_incremental_cow_pages() {
        let replay = WalReplayConfig::default();
        let path = seed_row_root_with_wal_insert("recovered-dirty-pages", replay);
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
                recovered_commit_epoch: 2,
                ..
            }
        ));

        store.checkpoint(&catalog).unwrap();
        let root = RelationalRowPageRootReader::open_generation(
            &path,
            2,
            RelationalRowPagePublicationConfig::default(),
        )
        .unwrap();
        assert_eq!(root.manifest().dirty_page_count, 1);
        assert_eq!(root.manifest().root_page_count, 1);
        assert_eq!(physical_generations(&root), vec![2]);
        assert_eq!(
            store.relational_state.row("documents", &key(2)),
            Some(&row(2, "two"))
        );

        drop(store);
        let mut reopened_catalog = Catalog::default();
        let reopened = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut reopened_catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        assert_eq!(
            reopened.relational_state.row("documents", &key(2)),
            Some(&row(2, "two"))
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn schema_change_rebuilds_the_complete_relational_row_root() {
        let path = unique_test_dir("schema-rebuild");
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
                            rows: (0..300).map(|id| row(id, &format!("body-{id}"))).collect(),
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::AddColumn {
                        table: "documents".to_string(),
                        column: RelationalColumnSchema {
                            name: "archived".to_string(),
                            scalar_type: RelationalScalarType::Boolean,
                            nullable: false,
                            default: Some(RelationalValue::Boolean(false)),
                        },
                    }],
                },
            )
            .unwrap();
        assert!(store.relational_row_pages.read_view.is_none());

        store.checkpoint(&catalog).unwrap();
        let rebuilt = RelationalRowPageRootReader::open_generation(
            &path,
            2,
            RelationalRowPagePublicationConfig::default(),
        )
        .unwrap();
        assert_eq!(rebuilt.manifest().dirty_page_count, 2);
        assert_eq!(rebuilt.manifest().root_page_count, 2);
        assert_eq!(physical_generations(&rebuilt), vec![2, 2]);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_planning_enforces_one_global_dirty_page_budget() {
        let path = unique_test_dir("global-dirty-budget");
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
                        RelationalWrite::CreateTable(named_schema("documents")),
                        RelationalWrite::CreateTable(named_schema("messages")),
                        RelationalWrite::Insert {
                            table: "documents".to_string(),
                            rows: vec![row(1, "document")],
                            mode: RelationalInsertMode::Error,
                        },
                        RelationalWrite::Insert {
                            table: "messages".to_string(),
                            rows: vec![row(1, "message")],
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let base = RelationalRowPageRootReader::open_generation(
            &path,
            1,
            RelationalRowPagePublicationConfig::default(),
        )
        .unwrap();
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::Insert {
                            table: "documents".to_string(),
                            rows: vec![row(1, "new-document")],
                            mode: RelationalInsertMode::Replace,
                        },
                        RelationalWrite::Insert {
                            table: "messages".to_string(),
                            rows: vec![row(1, "new-message")],
                            mode: RelationalInsertMode::Replace,
                        },
                    ],
                },
            )
            .unwrap();
        let mut config = RelationalRowPagePublicationConfig::default();
        config.max_dirty_pages = NonZeroUsize::new(1).unwrap();
        config.max_dirty_bytes =
            NonZeroU64::new(config.page_limits.max_page_bytes.get() as u64).unwrap();
        let error = store
            .plan_relational_row_page_checkpoint(Some(base), 2, 2, config)
            .err()
            .expect("two changed tables must exceed one global dirty page");
        assert!(error
            .to_string()
            .contains("exhausted its 1 dirty-page limit"));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn unbound_row_candidate_does_not_replace_canonical_recovery() {
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
        RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default())
            .persist_generation(
                skein_storage::RelationalRowPageGenerationRequest {
                    directory: &path,
                    generation: 3,
                    source_commit_epoch: 2,
                    base: None,
                    expected_previous_generation: Some(2),
                    overflow_root: None,
                },
                Vec::new(),
            )
            .unwrap();
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
            RelationalRowPageRecoveryStatus::CheckpointReady {
                generation: 2,
                source_commit_epoch: 2,
                root_pages: 1,
            }
        ));
        assert_eq!(reopened.relational_state.row_count("documents"), 1);
        assert!(reopened.relational_row_pages.read_view.is_some());

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn corrupt_bound_row_root_fails_database_open() {
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
        drop(store);

        let manifest = path.join(relational_row_page_manifest_generation_file(1));
        let mut encoded = std::fs::read(&manifest).unwrap();
        *encoded.last_mut().unwrap() ^= 1;
        std::fs::write(manifest, encoded).unwrap();

        let mut reopened_catalog = Catalog::default();
        let error = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut reopened_catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("canonical relational row-page generation manifest integrity mismatch"));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reclaim_preserves_overflow_extents_referenced_by_retained_roots() {
        let path = unique_test_dir("overflow-reclaim-closure");
        let backup = unique_test_dir("overflow-backup-closure");
        let restored = unique_test_dir("overflow-restore-closure");
        let replay = WalReplayConfig::default();
        let body = "x".repeat(8 * 1024);
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
                            rows: vec![row(1, &body)],
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        for generation in 2..=3 {
            store
                .create_node(
                    &mut catalog,
                    "CheckpointMarker",
                    BTreeMap::from([("generation".to_string(), crate::Value::Int(generation))]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        assert!(!path
            .join(relational_overflow_manifest_generation_file(1))
            .exists());
        assert!(path.join(relational_overflow_extent_file(1)).exists());
        let overflow = store
            .durable
            .as_ref()
            .unwrap()
            .open_bound_relational_overflow()
            .unwrap();
        let mut reference = None;
        overflow
            .visit_descriptors(|descriptor| {
                assert_eq!(descriptor.physical_generation, 1);
                reference = Some(descriptor.reference);
                Ok(())
            })
            .unwrap();
        let hydrated = overflow
            .hydrate(
                &reference.expect("overflow descriptor"),
                &mut RelationalHydrationBudget::default(),
                None,
            )
            .unwrap();
        assert_eq!(hydrated, RelationalValue::Text(body));
        store.backup_to(&catalog, &backup).unwrap();
        assert!(backup.join(relational_overflow_extent_file(1)).exists());
        assert!(backup.join(relational_overflow_extent_file(4)).exists());
        let extent = path.join(relational_overflow_extent_file(1));
        let mut corrupted = std::fs::read(&extent).unwrap();
        *corrupted.last_mut().expect("non-empty overflow extent") ^= 1;
        std::fs::write(extent, corrupted).unwrap();
        let scrub_error = store.scrub_storage().unwrap_err();
        assert!(scrub_error.to_string().contains("checksum mismatch"));
        drop(store);

        crate::store::restore_storage_backup(&backup, &restored).unwrap();
        let mut reopened_catalog = Catalog::default();
        let reopened = GraphStore::open_with_durability_and_replay_config(
            &restored,
            &mut reopened_catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        assert_eq!(reopened.relational_state.row_count("documents"), 1);

        std::fs::remove_dir_all(path).unwrap();
        std::fs::remove_dir_all(backup).unwrap();
        drop(reopened);
        std::fs::remove_dir_all(restored).unwrap();
    }

    fn seed_row_root_with_wal_insert(name: &str, replay: WalReplayConfig) -> std::path::PathBuf {
        let path = unique_test_dir(name);
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
        drop(store);
        path
    }

    fn physical_generations(reader: &RelationalRowPageRootReader) -> Vec<u64> {
        let mut generations = Vec::new();
        reader
            .visit_table_pages("documents", |descriptor| {
                generations.push(descriptor.physical_generation);
                Ok(())
            })
            .unwrap();
        generations
    }

    fn schema() -> RelationalTableSchema {
        named_schema("documents")
    }

    fn named_schema(name: &str) -> RelationalTableSchema {
        RelationalTableSchema {
            name: name.to_string(),
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
