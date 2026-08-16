//! Shadow recovery state for canonical relational row-page roots.

use super::{GraphStore, RelationalRowStorageResidencyReport};
use skein_storage::{
    RelationalConstraintIndex, RelationalError, RelationalHydrationBudget,
    RelationalIndexChangeCaptureLimits, RelationalOverflowRootReader, RelationalProjectedRow,
    RelationalRecoveryFence, RelationalRecoverySourceIdentity, RelationalReplayAccess,
    RelationalReplayAccessSet, RelationalRow, RelationalRowChangeCapture,
    RelationalRowChangeCaptureLimits, RelationalRowDeltaBuilder, RelationalRowDeltaConfig,
    RelationalRowDeltaError, RelationalRowDeltaReader, RelationalRowDeltaReport,
    RelationalRowPageDemandReadError, RelationalRowPageLiveError, RelationalRowPageMutationPlanner,
    RelationalRowPageProjectedRange, RelationalRowPagePublicationConfig, RelationalRowPageReadView,
    RelationalRowPageReadViewIdentity, RelationalRowPageRecoveredValue,
    RelationalRowPageRootReader, RelationalRowPageSnapshotPointReport,
    RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader,
    RelationalRowPageSnapshotRowSource, RelationalRowPageTableDelta, RelationalSparseIndexProbe,
    RelationalSparseLivePreparationStage, RelationalSparseRecoveryRow,
    RelationalSparseWorkspaceBuilder, RelationalState, RelationalTransaction, SegmentCache,
    StorageResidencyMode, StoreId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Bound;
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

struct RelationalSparseLiveHydrator<'a> {
    state: &'a RelationalState,
    reader: RelationalRowPageSnapshotReader,
    workspace: RelationalSparseWorkspaceBuilder,
    requested_fields: BTreeMap<String, Arc<[usize]>>,
    resolved_probes: BTreeSet<RelationalSparseIndexProbe>,
    hydration: RelationalHydrationBudget,
    task: skein_core::RuntimeTaskContext,
    limits: RelationalRowPageSnapshotReadLimits,
    pages_read: usize,
    rows_decoded: usize,
    bytes_read: usize,
    overlay_entries: usize,
    overlay_resident_bytes: usize,
}

impl<'a> RelationalSparseLiveHydrator<'a> {
    fn new(
        state: &'a RelationalState,
        reader: RelationalRowPageSnapshotReader,
        workspace_limits: RelationalRowChangeCaptureLimits,
    ) -> Self {
        let mut limits = RelationalRowPageSnapshotReadLimits::default();
        limits.demand.max_rows = nonzero_min(limits.demand.max_rows, workspace_limits.max_entries);
        limits.demand.max_bytes = nonzero_min(limits.demand.max_bytes, workspace_limits.max_bytes);
        limits.max_overlay_entries =
            nonzero_min(limits.max_overlay_entries, workspace_limits.max_entries);
        limits.max_overlay_bytes =
            nonzero_min(limits.max_overlay_bytes, workspace_limits.max_bytes);
        let max_rows = limits.demand.max_rows.get();
        let max_bytes = limits.demand.max_bytes.get();
        Self {
            state,
            reader,
            workspace: RelationalSparseWorkspaceBuilder::new(workspace_limits),
            requested_fields: BTreeMap::new(),
            resolved_probes: BTreeSet::new(),
            hydration: RelationalHydrationBudget {
                max_rows,
                max_compressed_bytes: max_bytes,
                max_decompressed_bytes: max_bytes,
                max_memory_bytes: max_bytes,
                ..RelationalHydrationBudget::default()
            },
            task: skein_core::RuntimeTaskContext::default(),
            limits,
            pages_read: 0,
            rows_decoded: 0,
            bytes_read: 0,
            overlay_entries: 0,
            overlay_resident_bytes: 0,
        }
    }

    fn workspace_snapshot(&self) -> Vec<RelationalSparseRecoveryRow> {
        self.workspace.snapshot()
    }

    fn fields(&mut self, table: &str) -> Result<Arc<[usize]>, RelationalError> {
        if let Some(fields) = self.requested_fields.get(table) {
            return Ok(Arc::clone(fields));
        }
        let schema = self.state.table_schema(table).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "sparse relational live hydration references unknown table {table}"
            ))
        })?;
        let fields = Arc::<[usize]>::from((0..schema.columns.len()).collect::<Vec<_>>());
        self.requested_fields
            .insert(table.to_string(), Arc::clone(&fields));
        Ok(fields)
    }

    fn hydrate_point(&mut self, access: &RelationalReplayAccess) -> Result<bool, RelationalError> {
        if self.workspace.contains(access) {
            return Ok(false);
        }
        let fields = self.fields(&access.table)?;
        let limits = self.remaining_limits()?;
        let (mut projected, report) = self
            .reader
            .point_projected(
                &access.table,
                &access.primary_key,
                &fields,
                limits,
                &mut self.hydration,
                &self.task,
            )
            .map_err(map_sparse_live_snapshot_error)?;
        if projected
            .as_ref()
            .is_some_and(|row| row.primary_key != access.primary_key)
        {
            return Err(RelationalError::Corruption(format!(
                "sparse relational live point read returned the wrong primary key for table {}",
                access.table
            )));
        }
        if let Some(row) = &mut projected {
            self.state.hydrate_projected_row_with_context(
                &access.table,
                row,
                &mut self.hydration,
                Some(&self.task),
            )?;
        }
        self.record_point(&report)?;
        let row = projected
            .map(|row| complete_sparse_projected_row(&access.table, &fields, row))
            .transpose()?;
        self.workspace.insert(RelationalSparseRecoveryRow {
            table: access.table.clone(),
            primary_key: access.primary_key.clone(),
            row,
        })
    }

    fn hydrate_table(&mut self, table: &str) -> Result<(), RelationalError> {
        let fields = self.fields(table)?;
        let limits = self.remaining_limits()?;
        let state = self.state;
        let task = &self.task;
        let workspace = &mut self.workspace;
        let mut callback_error = None;
        let read_result = self.reader.visit_projected_range_resolving(
            RelationalRowPageProjectedRange {
                table,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                requested_fields: &fields,
            },
            limits,
            &mut self.hydration,
            task,
            |row, budget, task| {
                state
                    .hydrate_projected_row_with_context(table, row, budget, Some(task))
                    .map_err(map_sparse_live_state_to_demand_error)
            },
            |row, _| {
                let primary_key = row.primary_key.clone();
                match complete_sparse_projected_row(table, &fields, row).and_then(|row| {
                    workspace.insert(RelationalSparseRecoveryRow {
                        table: table.to_string(),
                        primary_key,
                        row: Some(row),
                    })
                }) {
                    Ok(_) => true,
                    Err(error) => {
                        callback_error = Some(error);
                        false
                    }
                }
            },
        );
        let report = read_result.map_err(map_sparse_live_snapshot_error)?;
        self.record_range(&report)?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        if report.demand.stopped_early {
            return Err(RelationalError::Corruption(format!(
                "sparse relational live hydration stopped while scanning table {table}"
            )));
        }
        Ok(())
    }

    fn resolve_probe(
        &mut self,
        probe: &RelationalSparseIndexProbe,
        index: &dyn RelationalConstraintIndex,
    ) -> Result<(), RelationalError> {
        if self.resolved_probes.contains(probe) {
            return Ok(());
        }
        let mut missing = Vec::new();
        let mut stopped_for_budget = false;
        let remaining_entries = self.workspace.remaining_entries();
        index.visit_exact_primary_keys(
            &probe.table,
            &probe.index,
            &probe.index_key,
            &mut |primary_key| {
                let access = RelationalReplayAccess {
                    table: probe.table.clone(),
                    primary_key: primary_key.clone(),
                };
                if self.workspace.contains(&access) {
                    return true;
                }
                if missing.len() >= remaining_entries {
                    stopped_for_budget = true;
                    return false;
                }
                missing.push(access);
                true
            },
        )?;
        if stopped_for_budget {
            return Err(RelationalError::Admission(format!(
                "sparse relational live index probe {}.{} exceeds the remaining {}-entry workspace budget",
                probe.table, probe.index, remaining_entries
            )));
        }
        for access in missing {
            self.hydrate_point(&access)?;
        }
        self.resolved_probes.insert(probe.clone());
        Ok(())
    }

    fn remaining_limits(&self) -> Result<RelationalRowPageSnapshotReadLimits, RelationalError> {
        Ok(RelationalRowPageSnapshotReadLimits {
            demand: skein_storage::RelationalRowPageDemandReadLimits {
                max_pages: sparse_remaining(self.limits.demand.max_pages, self.pages_read, "page")?,
                max_rows: sparse_remaining(self.limits.demand.max_rows, self.rows_decoded, "row")?,
                max_bytes: sparse_remaining(
                    self.limits.demand.max_bytes,
                    self.bytes_read,
                    "read-byte",
                )?,
                max_pins: self.limits.demand.max_pins,
                max_tree_height: self.limits.demand.max_tree_height,
            },
            max_overlay_entries: sparse_remaining(
                self.limits.max_overlay_entries,
                self.overlay_entries,
                "overlay-entry",
            )?,
            max_overlay_bytes: sparse_remaining(
                self.limits.max_overlay_bytes,
                self.overlay_resident_bytes,
                "overlay-byte",
            )?,
        })
    }

    fn record_point(
        &mut self,
        report: &RelationalRowPageSnapshotPointReport,
    ) -> Result<(), RelationalError> {
        let overlay_entries = usize::from(matches!(
            report.source,
            RelationalRowPageSnapshotRowSource::Recovery
                | RelationalRowPageSnapshotRowSource::Live
                | RelationalRowPageSnapshotRowSource::Deleted
        ));
        self.record_snapshot_read(
            report.identity,
            &report.demand,
            report.recovery.bytes_read,
            overlay_entries,
            report.overlay_resident_bytes,
        )
    }

    fn record_range(
        &mut self,
        report: &RelationalRowPageSnapshotRangeReport,
    ) -> Result<(), RelationalError> {
        self.record_snapshot_read(
            report.identity,
            &report.demand,
            report.recovery.bytes_read,
            report.overlay_entries,
            report.overlay_resident_bytes,
        )
    }

    fn record_snapshot_read(
        &mut self,
        identity: RelationalRowPageReadViewIdentity,
        demand: &skein_storage::RelationalRowPageDemandReadReport,
        recovery_bytes: u64,
        overlay_entries: usize,
        overlay_resident_bytes: usize,
    ) -> Result<(), RelationalError> {
        if identity != self.reader.identity() {
            return Err(RelationalError::Corruption(
                "sparse relational live row view identity changed during hydration".to_string(),
            ));
        }
        self.pages_read = sparse_add_with_limit(
            self.pages_read,
            demand.pages_read,
            self.limits.demand.max_pages.get(),
            "page",
        )?;
        self.rows_decoded = sparse_add_with_limit(
            self.rows_decoded,
            demand.rows_decoded,
            self.limits.demand.max_rows.get(),
            "row",
        )?;
        let recovery_bytes = usize::try_from(recovery_bytes).map_err(|_| {
            RelationalError::Admission(
                "sparse relational live recovery bytes exceed platform capacity".to_string(),
            )
        })?;
        let read_bytes = demand
            .bytes_read
            .checked_add(recovery_bytes)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "sparse relational live read-byte counter overflow".to_string(),
                )
            })?;
        self.bytes_read = sparse_add_with_limit(
            self.bytes_read,
            read_bytes,
            self.limits.demand.max_bytes.get(),
            "read-byte",
        )?;
        self.overlay_entries = sparse_add_with_limit(
            self.overlay_entries,
            overlay_entries,
            self.limits.max_overlay_entries.get(),
            "overlay-entry",
        )?;
        self.overlay_resident_bytes = sparse_add_with_limit(
            self.overlay_resident_bytes,
            overlay_resident_bytes,
            self.limits.max_overlay_bytes.get(),
            "overlay-byte",
        )?;
        Ok(())
    }
}

fn nonzero_min(left: NonZeroUsize, right: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::new(left.get().min(right.get())).expect("minimum of non-zero limits is non-zero")
}

fn sparse_remaining(
    limit: NonZeroUsize,
    used: usize,
    name: &'static str,
) -> Result<NonZeroUsize, RelationalError> {
    limit
        .get()
        .checked_sub(used)
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            RelationalError::Admission(format!(
                "sparse relational live hydration exhausted its {} {name} budget",
                limit.get()
            ))
        })
}

fn sparse_add_with_limit(
    current: usize,
    additional: usize,
    limit: usize,
    name: &'static str,
) -> Result<usize, RelationalError> {
    let next = current.checked_add(additional).ok_or_else(|| {
        RelationalError::Admission(format!(
            "sparse relational live hydration {name} counter overflow"
        ))
    })?;
    if next > limit {
        return Err(RelationalError::Admission(format!(
            "sparse relational live hydration requires {next} {name}s, exceeding limit {limit}"
        )));
    }
    Ok(next)
}

fn complete_sparse_projected_row(
    table: &str,
    fields: &[usize],
    projected: RelationalProjectedRow,
) -> Result<RelationalRow, RelationalError> {
    if projected.fields.len() != fields.len()
        || projected
            .fields
            .iter()
            .zip(fields)
            .any(|(field, expected)| field.ordinal != *expected)
    {
        return Err(RelationalError::Corruption(format!(
            "sparse relational live read returned an incomplete row for table {table}"
        )));
    }
    Ok(RelationalRow::new(
        projected
            .fields
            .into_iter()
            .map(|field| field.value)
            .collect(),
    ))
}

fn map_sparse_live_state_to_demand_error(
    error: RelationalError,
) -> RelationalRowPageDemandReadError {
    match error {
        RelationalError::Admission(message) => RelationalRowPageDemandReadError::Admission(message),
        RelationalError::Durability(message) => {
            RelationalRowPageDemandReadError::Durability(message)
        }
        RelationalError::Schema(message)
        | RelationalError::Constraint(message)
        | RelationalError::Corruption(message) => {
            RelationalRowPageDemandReadError::Corrupt(message)
        }
    }
}

fn map_sparse_live_snapshot_error(error: RelationalRowPageSnapshotReadError) -> RelationalError {
    match error {
        RelationalRowPageSnapshotReadError::Admission(message) => {
            RelationalError::Admission(message)
        }
        RelationalRowPageSnapshotReadError::Stopped(reason) => {
            RelationalError::Admission(reason.to_string())
        }
        RelationalRowPageSnapshotReadError::MissingTable(table) => RelationalError::Corruption(
            format!("sparse relational live snapshot is missing table {table}"),
        ),
        RelationalRowPageSnapshotReadError::Corrupt(message) => {
            RelationalError::Corruption(message)
        }
        RelationalRowPageSnapshotReadError::Durability(message) => {
            RelationalError::Durability(message)
        }
    }
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

    /// Hydrates exactly one authenticated WAL access set without attaching the
    /// checkpoint rows to the metadata-only relational state.
    ///
    /// Earlier WAL rows are read from the unpublished recovery builder first;
    /// only misses reach the immutable checkpoint root. Sparse staging admits
    /// the returned vector again before it can affect logical row counts.
    pub(super) fn hydrate_sparse_relational_recovery_access(
        &self,
        replay_access: &RelationalReplayAccessSet,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Result<Vec<RelationalSparseRecoveryRow>, RelationalError> {
        if !self.relational_state.canonical_row_metadata_only() {
            return Err(RelationalError::Admission(
                "sparse WAL hydration requires canonical metadata-only relational state"
                    .to_string(),
            ));
        }
        if replay_access.entries().len() > limits.max_entries.get() {
            return Err(RelationalError::Admission(format!(
                "sparse WAL hydration contains {} entries, exceeding limit {}",
                replay_access.entries().len(),
                limits.max_entries
            )));
        }
        let builder = self
            .relational_row_pages
            .recovery_builder
            .as_ref()
            .ok_or_else(|| {
                RelationalError::Corruption(
                    "sparse WAL hydration requires an unpublished row recovery builder".to_string(),
                )
            })?;
        let view = self
            .relational_row_pages
            .read_view
            .as_ref()
            .ok_or_else(|| {
                RelationalError::Corruption(
                    "sparse WAL hydration requires a pinned checkpoint row view".to_string(),
                )
            })?;
        let resources = self
            .relational_row_pages
            .serving_resources
            .as_ref()
            .ok_or_else(|| {
                RelationalError::Corruption(
                    "sparse WAL hydration requires pinned row serving resources".to_string(),
                )
            })?;
        let base_view = Arc::new(RelationalRowPageReadView::from_base(view.pinned_base()));
        let reader = RelationalRowPageSnapshotReader::new(
            base_view,
            Arc::clone(&resources.base_overflow),
            None,
            Arc::clone(&resources.cache),
            resources.store_id,
        )
        .map_err(map_sparse_snapshot_read_error)?;
        let max_bytes = limits.max_bytes.get();
        let mut hydration = RelationalHydrationBudget {
            max_rows: limits.max_entries.get(),
            max_compressed_bytes: max_bytes,
            max_decompressed_bytes: max_bytes,
            max_memory_bytes: max_bytes,
            ..RelationalHydrationBudget::default()
        };
        let task = skein_core::RuntimeTaskContext::default();
        let mut requested_fields = BTreeMap::<String, Vec<usize>>::new();
        let mut hydrated = Vec::with_capacity(replay_access.entries().len());
        let mut read_bytes = 0usize;
        for access in replay_access.entries() {
            let schema = self
                .relational_state
                .table_schema(&access.table)
                .ok_or_else(|| {
                    RelationalError::Corruption(format!(
                        "sparse WAL access references unknown table {}",
                        access.table
                    ))
                })?;
            let column_count = schema.columns.len();
            let fields = requested_fields
                .entry(access.table.clone())
                .or_insert_with(|| (0..column_count).collect());
            let (staged, recovery_report) = builder
                .lookup_staged(&access.table, &access.primary_key)
                .map_err(map_sparse_row_delta_error)?;
            charge_sparse_recovery_read_bytes(
                &mut read_bytes,
                recovery_report.bytes_read,
                max_bytes,
            )?;
            let row = match staged {
                Some(RelationalRowPageRecoveredValue::Present(row)) => Some(
                    self.relational_state
                        .hydrate_sparse_recovery_row_with_context(
                            &row,
                            &mut hydration,
                            Some(&task),
                        )?,
                ),
                Some(RelationalRowPageRecoveredValue::Deleted) => None,
                None => {
                    let remaining_bytes = max_bytes.checked_sub(read_bytes).ok_or_else(|| {
                        RelationalError::Admission(
                            "sparse WAL hydration read-byte accounting underflow".to_string(),
                        )
                    })?;
                    let Some(remaining_bytes) = NonZeroUsize::new(remaining_bytes) else {
                        return Err(RelationalError::Admission(format!(
                            "sparse WAL hydration exhausted its {max_bytes}-byte read limit"
                        )));
                    };
                    let mut snapshot_limits = RelationalRowPageSnapshotReadLimits {
                        max_overlay_entries: limits.max_entries,
                        max_overlay_bytes: limits.max_bytes,
                        ..RelationalRowPageSnapshotReadLimits::default()
                    };
                    snapshot_limits.demand.max_bytes = remaining_bytes;
                    let (projected, point_report) = reader
                        .point_projected(
                            &access.table,
                            &access.primary_key,
                            fields,
                            snapshot_limits,
                            &mut hydration,
                            &task,
                        )
                        .map_err(map_sparse_snapshot_read_error)?;
                    charge_sparse_recovery_read_bytes(
                        &mut read_bytes,
                        u64::try_from(point_report.demand.bytes_read).map_err(|_| {
                            RelationalError::Admission(
                                "sparse WAL checkpoint read bytes exceed u64".to_string(),
                            )
                        })?,
                        max_bytes,
                    )?;
                    projected
                        .map(|projected| {
                            if projected.fields.len() != column_count
                                || projected
                                    .fields
                                    .iter()
                                    .enumerate()
                                    .any(|(ordinal, field)| field.ordinal != ordinal)
                            {
                                return Err(RelationalError::Corruption(format!(
                                    "sparse WAL point read returned an incomplete row for {}",
                                    access.table
                                )));
                            }
                            Ok(RelationalRow::new(
                                projected
                                    .fields
                                    .into_iter()
                                    .map(|field| field.value)
                                    .collect(),
                            ))
                        })
                        .transpose()?
                }
            };
            hydrated.push(RelationalSparseRecoveryRow {
                table: access.table.clone(),
                primary_key: access.primary_key.clone(),
                row,
            });
        }
        Ok(hydrated)
    }

    /// Closes one live transaction's exact row and constraint working set
    /// against a generation-pinned canonical snapshot before WAL publication.
    pub(super) fn hydrate_sparse_relational_live_workspace(
        &self,
        transaction: &RelationalTransaction,
        index_capture_limits: RelationalIndexChangeCaptureLimits,
        row_capture_limits: RelationalRowChangeCaptureLimits,
        constraint_index: &dyn RelationalConstraintIndex,
    ) -> Result<Vec<RelationalSparseRecoveryRow>, RelationalError> {
        if !self.relational_state.canonical_row_metadata_only() {
            return Err(RelationalError::Admission(
                "sparse relational live hydration requires canonical metadata-only state"
                    .to_string(),
            ));
        }
        let reader = self
            .open_relational_row_snapshot_reader()
            .map_err(|error| RelationalError::Corruption(error.to_string()))?
            .ok_or_else(|| {
                RelationalError::Corruption(
                    "sparse relational live hydration requires a canonical row snapshot"
                        .to_string(),
                )
            })?;
        let plan = self
            .relational_state
            .plan_sparse_transaction_hydration(transaction)?;
        let mut hydrator =
            RelationalSparseLiveHydrator::new(&self.relational_state, reader, row_capture_limits);
        for access in plan.point_access() {
            hydrator.hydrate_point(access)?;
        }
        for table in plan.scan_tables() {
            hydrator.hydrate_table(table)?;
        }
        for probe in plan.index_probes() {
            hydrator.resolve_probe(probe, constraint_index)?;
        }

        loop {
            let previous_entries = hydrator.workspace.len();
            let preparation = self
                .relational_state
                .prepare_sparse_transaction_for_authoritative_live(
                    RelationalSparseLivePreparationStage {
                        transaction: transaction.clone(),
                        hydrated_workspace: hydrator.workspace_snapshot(),
                        mutation_limits: self.relational_mutation_limits,
                        overflow_config: self.relational_overflow_config,
                        index_capture_limits,
                        row_capture_limits,
                    },
                )?;
            for access in preparation.replay_access().entries() {
                hydrator.hydrate_point(access)?;
            }
            for probe in preparation.constraint_probes() {
                hydrator.resolve_probe(probe, constraint_index)?;
            }
            if hydrator.workspace.len() == previous_entries {
                return Ok(hydrator.workspace_snapshot());
            }
        }
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

    pub(super) fn finish_relational_row_page_recovery(
        &mut self,
        recovery_source: Option<RelationalRecoverySourceIdentity>,
    ) {
        let Some(builder) = self.relational_row_pages.recovery_builder.take() else {
            if let RelationalRowPageRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                ..
            } = self.relational_row_pages.recovery_status
                && self.commit_epoch > source_commit_epoch
            {
                let Some(recovery_source) = recovery_source else {
                    self.mark_relational_row_page_recovery_unavailable(
                        self.commit_epoch,
                        "WAL recovery did not produce a relational recovery source identity"
                            .to_string(),
                    );
                    return;
                };
                match self.open_relational_row_delta_view(self.commit_epoch, recovery_source) {
                    Ok((view, delta)) => {
                        let manifest = delta.manifest();
                        if self.relational_state.canonical_row_metadata_only()
                            && let Err(error) = self.relational_state.adopt_recovered_row_counts(
                                manifest
                                    .tables()
                                    .iter()
                                    .map(|table| (table.table.as_str(), table.row_count)),
                            )
                        {
                            self.mark_relational_row_page_recovery_unavailable(
                                self.commit_epoch,
                                format!(
                                    "read-only recovery row counts do not match the canonical catalog: {error}"
                                ),
                            );
                            return;
                        }
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
        let Some(recovery_source) = recovery_source else {
            self.mark_relational_row_page_recovery_unavailable(
                self.commit_epoch,
                "WAL recovery did not produce a relational recovery source identity".to_string(),
            );
            return;
        };
        match builder.finish_with_state(
            self.commit_epoch,
            recovery_source,
            None,
            &self.relational_state,
        ) {
            Ok(report) => {
                match self.open_relational_row_delta_view(self.commit_epoch, recovery_source) {
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
                }
            }
            Err(error) => self.mark_relational_row_page_recovery_unavailable(
                self.commit_epoch,
                format!("relational row recovery could not finish: {error}"),
            ),
        }
    }

    fn open_relational_row_delta_view(
        &self,
        expected_visible_commit_epoch: u64,
        expected_recovery_source: RelationalRecoverySourceIdentity,
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
        let delta = RelationalRowDeltaReader::open_latest_with_recovery_fence(
            durable.root_path(),
            &base,
            RelationalRecoveryFence::new(expected_visible_commit_epoch, expected_recovery_source),
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

fn map_sparse_row_delta_error(error: RelationalRowDeltaError) -> RelationalError {
    match error {
        RelationalRowDeltaError::Admission(message)
        | RelationalRowDeltaError::Invalidated(message) => RelationalError::Admission(message),
        RelationalRowDeltaError::RequiresCheckpoint { tables } => {
            RelationalError::Admission(format!(
                "sparse WAL hydration requires a relational checkpoint for tables {}",
                tables.join(",")
            ))
        }
        RelationalRowDeltaError::Corrupt(message)
        | RelationalRowDeltaError::Durability(message) => RelationalError::Corruption(message),
        error @ (RelationalRowDeltaError::Publication(_)
        | RelationalRowDeltaError::Row(_)
        | RelationalRowDeltaError::StaleGeneration { .. }
        | RelationalRowDeltaError::StaleBase { .. }) => {
            RelationalError::Corruption(error.to_string())
        }
    }
}

fn charge_sparse_recovery_read_bytes(
    read_bytes: &mut usize,
    additional: u64,
    max_bytes: usize,
) -> Result<(), RelationalError> {
    let additional = usize::try_from(additional).map_err(|_| {
        RelationalError::Admission(
            "sparse WAL hydration read bytes exceed platform capacity".to_string(),
        )
    })?;
    let next = read_bytes.checked_add(additional).ok_or_else(|| {
        RelationalError::Admission("sparse WAL hydration read-byte counter overflow".to_string())
    })?;
    if next > max_bytes {
        return Err(RelationalError::Admission(format!(
            "sparse WAL hydration reads require {next} bytes, exceeding limit {max_bytes}"
        )));
    }
    *read_bytes = next;
    Ok(())
}

fn map_sparse_snapshot_read_error(error: RelationalRowPageSnapshotReadError) -> RelationalError {
    match error {
        RelationalRowPageSnapshotReadError::Admission(message) => {
            RelationalError::Admission(message)
        }
        RelationalRowPageSnapshotReadError::Stopped(reason) => {
            RelationalError::Admission(reason.to_string())
        }
        RelationalRowPageSnapshotReadError::MissingTable(table) => RelationalError::Corruption(
            format!("sparse WAL snapshot is missing authenticated table {table}"),
        ),
        RelationalRowPageSnapshotReadError::Corrupt(message)
        | RelationalRowPageSnapshotReadError::Durability(message) => {
            RelationalError::Corruption(message)
        }
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
        RelationalComparisonOp, RelationalConflictAction, RelationalHydrationBudget,
        RelationalIndexMode, RelationalInsertMode, RelationalKey, RelationalMutationLimits,
        RelationalOverflowConfig, RelationalPredicate, RelationalRow,
        RelationalRowPagePublicationConfig, RelationalRowPagePublisher,
        RelationalRowPageRootReader, RelationalRowPageSnapshotReadLimits, RelationalScalarType,
        RelationalTableSchema, RelationalTransaction, RelationalUpdateAssignment,
        RelationalUpdateValue, RelationalUpsertAssignment, RelationalUpsertValue, RelationalValue,
        RelationalWrite, StorageResidencyMode, WalReplayConfig,
        RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE,
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
    fn sparse_writable_recovery_hydrates_checkpoint_and_prior_wal_rows() {
        let seed = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let path = seed_row_root("sparse-writable-hydration", seed);
        let replay = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let index_limits = store.relational_index_live_capture_limits().unwrap();
        let row_limits = store.relational_row_live_capture_limits().unwrap();
        let first = RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: vec![row(2, "two")],
                mode: RelationalInsertMode::Error,
            }],
        };
        let index = store
            .authoritative_relational_constraint_index()
            .unwrap()
            .unwrap();
        let (after_first, _, _, first_access) = store
            .relational_state
            .stage_transaction_with_authoritative_replay_access(
                first.clone(),
                store.relational_mutation_limits,
                store.relational_overflow_config,
                index_limits,
                row_limits,
                &index,
            )
            .unwrap();
        let checkpoint_probe = RelationalTransaction {
            writes: vec![RelationalWrite::DeleteByPrimaryKey {
                table: "documents".to_string(),
                keys: vec![key(1)],
            }],
        };
        let index = store
            .authoritative_relational_constraint_index()
            .unwrap()
            .unwrap();
        let (_, _, _, checkpoint_access) = store
            .relational_state
            .stage_transaction_with_authoritative_replay_access(
                checkpoint_probe,
                store.relational_mutation_limits,
                store.relational_overflow_config,
                index_limits,
                row_limits,
                &index,
            )
            .unwrap();
        let second = RelationalTransaction {
            writes: vec![RelationalWrite::UpdateWhere {
                table: "documents".to_string(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "body".to_string(),
                    value: RelationalUpdateValue::Value(RelationalValue::Text(
                        "updated".to_string(),
                    )),
                }],
                predicate: RelationalPredicate::Compare {
                    column: "body".to_string(),
                    op: RelationalComparisonOp::Eq,
                    value: RelationalValue::Text("two".to_string()),
                },
            }],
        };
        let index = store
            .authoritative_relational_constraint_index()
            .unwrap()
            .unwrap();
        let (expected, _, _, second_access) = after_first
            .stage_transaction_with_authoritative_replay_access(
                second.clone(),
                store.relational_mutation_limits,
                store.relational_overflow_config,
                index_limits,
                row_limits,
                &index,
            )
            .unwrap();
        assert_eq!(first_access.entries().len(), 1);
        assert_eq!(second_access.entries().len(), 2);

        let base = store
            .relational_row_pages
            .read_view
            .as_ref()
            .unwrap()
            .pinned_base();
        store.relational_state = RelationalState::from_canonical_row_root(base.manifest()).unwrap();
        store.mount_relational_row_pages_for_recovery().unwrap();
        store.mount_relational_index_shadow_for_recovery();

        let read_budget_error = store
            .hydrate_sparse_relational_recovery_access(
                &checkpoint_access,
                RelationalRowChangeCaptureLimits {
                    max_entries: row_limits.max_entries,
                    max_bytes: NonZeroUsize::new(1).unwrap(),
                },
            )
            .unwrap_err();
        assert!(matches!(
            read_budget_error,
            RelationalError::Admission(message) if message.contains("byte")
        ));

        store
            .stage_recovered_relational_transaction(first, Some(first_access), 2)
            .unwrap();
        assert!(store.relational_state.canonical_row_metadata_only());
        assert!(!store.relational_state.materialized_rows_resident());
        assert_eq!(store.relational_state.row_count("documents"), 2);
        assert!(matches!(
            store
                .relational_row_pages
                .recovery_builder
                .as_ref()
                .unwrap()
                .lookup_staged("documents", &key(2))
                .unwrap()
                .0,
            Some(RelationalRowPageRecoveredValue::Present(value)) if value == row(2, "two")
        ));

        store
            .stage_recovered_relational_transaction(second, Some(second_access), 3)
            .unwrap();
        assert!(store.relational_state.canonical_row_metadata_only());
        assert_eq!(
            store.relational_state.row_count("documents"),
            expected.row_count("documents")
        );
        assert!(matches!(
            store
                .relational_row_pages
                .recovery_builder
                .as_ref()
                .unwrap()
                .lookup_staged("documents", &key(2))
                .unwrap()
                .0,
            Some(RelationalRowPageRecoveredValue::Present(value)) if value == row(2, "updated")
        ));

        drop(store);
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
    fn metadata_only_writable_commit_hydrates_live_candidates_and_publishes_rows() {
        let seed = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let path = seed_row_root("metadata-only-live-commit", seed);
        let replay = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let base = store
            .relational_row_pages
            .read_view
            .as_ref()
            .unwrap()
            .pinned_base();
        store.relational_state = RelationalState::from_canonical_row_root(base.manifest()).unwrap();

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
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::UpdateWhere {
                        table: "documents".to_string(),
                        assignments: vec![
                            RelationalUpdateAssignment {
                                column: "id".to_string(),
                                value: RelationalUpdateValue::Value(RelationalValue::BigInt(3)),
                            },
                            RelationalUpdateAssignment {
                                column: "body".to_string(),
                                value: RelationalUpdateValue::Value(RelationalValue::Text(
                                    "updated".to_string(),
                                )),
                            },
                        ],
                        predicate: RelationalPredicate::Compare {
                            column: "id".to_string(),
                            op: RelationalComparisonOp::Eq,
                            value: RelationalValue::BigInt(1),
                        },
                    }],
                },
            )
            .unwrap();

        assert!(store.relational_state.canonical_row_metadata_only());
        assert!(!store.relational_state.materialized_rows_resident());
        assert_eq!(store.relational_state.materialized_row_count(), 0);
        assert_eq!(store.relational_state.row_count("documents"), 2);
        assert_eq!(store.commit_epoch, 3);
        let reader = store
            .open_relational_row_snapshot_reader()
            .unwrap()
            .unwrap();
        let mut hydration = RelationalHydrationBudget::default();
        let (projected, report) = reader
            .point_projected(
                "documents",
                &key(3),
                &[0, 1],
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        assert_eq!(
            projected.unwrap().fields[1].value,
            RelationalValue::Text("updated".to_string())
        );
        let (old_primary_key, _) = reader
            .point_projected(
                "documents",
                &key(1),
                &[0, 1],
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        assert!(old_primary_key.is_none());
        assert_eq!(report.identity.visible_commit_epoch, 3);
        assert!(matches!(
            store.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::LiveCurrent {
                visible_commit_epoch: 3,
                ..
            }
        ));

        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn metadata_only_live_hydration_budget_rejects_before_wal() {
        let seed = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let path = seed_row_root("metadata-only-live-budget", seed);
        let replay = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let base = store
            .relational_row_pages
            .read_view
            .as_ref()
            .unwrap()
            .pinned_base();
        store.relational_state = RelationalState::from_canonical_row_root(base.manifest()).unwrap();
        store.relational_row_pages.live_limits.max_entries = NonZeroUsize::new(1).unwrap();
        let commit_epoch = store.commit_epoch;
        let next_lsn = store.durable.as_ref().unwrap().next_lsn;
        let view_identity = store
            .relational_row_pages
            .read_view
            .as_ref()
            .unwrap()
            .identity();

        let error = store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![row(2, "two"), row(3, "three")],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("workspace"));
        assert_eq!(store.commit_epoch, commit_epoch);
        assert_eq!(store.durable.as_ref().unwrap().next_lsn, next_lsn);
        assert_eq!(
            store
                .relational_row_pages
                .read_view
                .as_ref()
                .unwrap()
                .identity(),
            view_identity
        );
        assert_eq!(store.relational_state.row_count("documents"), 1);
        assert!(store.relational_state.canonical_row_metadata_only());

        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn metadata_only_writable_upsert_hydrates_authoritative_unique_postings() {
        let path = unique_test_dir("metadata-only-live-upsert");
        let seed = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            seed,
        )
        .unwrap();
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(upsert_schema()),
                        RelationalWrite::Insert {
                            table: "accounts".to_string(),
                            rows: vec![upsert_row(1, "alice", "old")],
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);

        let replay = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let base = store
            .relational_row_pages
            .read_view
            .as_ref()
            .unwrap()
            .pinned_base();
        store.relational_state = RelationalState::from_canonical_row_root(base.manifest()).unwrap();

        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Upsert {
                        table: "accounts".to_string(),
                        rows: vec![upsert_row(2, "alice", "new")],
                        conflict_columns: vec!["handle".to_string()],
                        action: RelationalConflictAction::Update(vec![
                            RelationalUpsertAssignment {
                                column: "payload".to_string(),
                                value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                            },
                        ]),
                    }],
                },
            )
            .unwrap();

        assert!(store.relational_state.canonical_row_metadata_only());
        assert_eq!(store.relational_state.row_count("accounts"), 1);
        let reader = store
            .open_relational_row_snapshot_reader()
            .unwrap()
            .unwrap();
        let mut hydration = RelationalHydrationBudget::default();
        let (updated, _) = reader
            .point_projected(
                "accounts",
                &key(1),
                &[0, 1, 2],
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        assert_eq!(
            updated.unwrap().fields[2].value,
            RelationalValue::Text("new".to_string())
        );
        let (insert_key, _) = reader
            .point_projected(
                "accounts",
                &key(2),
                &[0, 1, 2],
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        assert!(insert_key.is_none());

        drop(store);
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
        assert_eq!(
            view.recovery_delta().unwrap().manifest().tables()[0].row_count,
            2
        );
        assert!(matches!(
            view.overlay_value("documents", &key(2)).unwrap(),
            Some(skein_storage::RelationalRowPageRecoveredValue::Present(value))
                if value == row(2, "two")
        ));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn read_only_authoritative_wal_reuse_stays_metadata_only() {
        let replay = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let path = seed_row_root_with_wal_insert("read-only-sparse-wal", replay);

        let mut writable_catalog = Catalog::default();
        let writable = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut writable_catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        assert!(matches!(
            writable.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::WalRecovered {
                recovered_commit_epoch: 2,
                ..
            }
        ));
        assert!(matches!(
            writable.relational_index_shadow_recovery_status(),
            crate::store::RelationalIndexShadowRecoveryStatus::WalRecovered {
                recovered_commit_epoch: 2,
                ..
            }
        ));
        drop(writable);

        let mut read_only_catalog = Catalog::default();
        let read_only = GraphStore::open_read_only_with_durability_and_replay_config(
            &path,
            &mut read_only_catalog,
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
        assert_eq!(read_only.relational_state.total_row_count(), 2);
        assert_eq!(read_only.relational_state.row_count("documents"), 2);
        assert!(matches!(
            read_only.relational_row_page_recovery_status(),
            RelationalRowPageRecoveryStatus::WalRecovered {
                recovered_commit_epoch: 2,
                delta_entries: 1,
                peak_dirty_bytes: None,
                ..
            }
        ));
        assert!(matches!(
            read_only.relational_index_shadow_recovery_status(),
            crate::store::RelationalIndexShadowRecoveryStatus::WalRecovered {
                recovered_commit_epoch: 2,
                delta_entries: 1,
                peak_dirty_bytes: 0,
                ..
            }
        ));
        let residency = read_only.storage_residency_report();
        assert!(residency.relational_rows.serving);
        assert_eq!(residency.relational_rows.materialized_row_bytes, 0);
        assert_eq!(residency.relational_rows.logical_row_count, 2);
        assert!(residency.relational_indexes.serving);
        assert_eq!(residency.relational_indexes.recovery_delta_entries, 1);

        let reader = read_only
            .open_relational_row_snapshot_reader()
            .unwrap()
            .expect("source-exact row recovery reader");
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

        drop(reader);
        drop(read_only);
        std::fs::remove_file(path.join(RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE)).unwrap();
        let mut missing_index_catalog = Catalog::default();
        let error = match GraphStore::open_read_only_with_durability_and_replay_config(
            &path,
            &mut missing_index_catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                relational_index_mode: RelationalIndexMode::Authoritative,
                ..WalReplayConfig::default()
            },
        ) {
            Ok(_) => panic!("missing source-exact index recovery artifact must fail closed"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("authoritative relational index view is unavailable"));

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

    fn seed_row_root(name: &str, replay: WalReplayConfig) -> std::path::PathBuf {
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
        drop(store);
        path
    }

    fn seed_row_root_with_wal_insert(name: &str, replay: WalReplayConfig) -> std::path::PathBuf {
        let path = seed_row_root(name, replay);
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

    fn upsert_schema() -> RelationalTableSchema {
        RelationalTableSchema {
            name: "accounts".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "id".to_string(),
                    scalar_type: RelationalScalarType::BigInt,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "handle".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "payload".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            unique_constraints: vec![vec!["handle".to_string()]],
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

    fn upsert_row(id: i64, handle: &str, payload: &str) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::BigInt(id),
            RelationalValue::Text(handle.to_string()),
            RelationalValue::Text(payload.to_string()),
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
