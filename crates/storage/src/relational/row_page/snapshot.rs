//! Snapshot-correct relational reads over checkpoint, recovery, and live rows.

use super::demand::{
    RelationalRowPageOverlayPoint, RelationalRowPageOverlayRange,
    RelationalRowPageProjectedOverlayValue,
};
use super::{
    RelationalProjectedField, RelationalProjectedRow, RelationalRowDeltaError,
    RelationalRowDeltaReadReport, RelationalRowPageDemandReadError,
    RelationalRowPageDemandReadLimits, RelationalRowPageDemandReadReport,
    RelationalRowPageDemandReader, RelationalRowPageProjectedRange, RelationalRowPageReadView,
    RelationalRowPageReadViewIdentity, RelationalRowPageRecoveredValue,
};
use crate::relational::{
    RelationalHydrationBudget, RelationalKey, RelationalOverflowRootReader, RelationalRowPageError,
    RelationalRowPagePublicationError, RelationalValue,
};
use crate::{SegmentCache, StoreId};
use skein_core::{RuntimeCancellationReason, RuntimeTaskContext};
use std::collections::BTreeMap;
use std::fmt;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES: usize = 16 * 1024;
pub const DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageSnapshotReadLimits {
    pub demand: RelationalRowPageDemandReadLimits,
    pub max_overlay_entries: NonZeroUsize,
    pub max_overlay_bytes: NonZeroUsize,
}

impl Default for RelationalRowPageSnapshotReadLimits {
    fn default() -> Self {
        Self {
            demand: RelationalRowPageDemandReadLimits::default(),
            max_overlay_entries: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES)
                .expect("default snapshot overlay entry limit is non-zero"),
            max_overlay_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES)
                .expect("default snapshot overlay byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalRowPageSnapshotRowSource {
    Checkpoint,
    Recovery,
    Live,
    Deleted,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageSnapshotPointReport {
    pub identity: RelationalRowPageReadViewIdentity,
    pub source: RelationalRowPageSnapshotRowSource,
    pub demand: RelationalRowPageDemandReadReport,
    pub recovery: RelationalRowDeltaReadReport,
    pub live_batches_examined: usize,
    pub overlay_resident_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageSnapshotRangeReport {
    pub identity: RelationalRowPageReadViewIdentity,
    pub demand: RelationalRowPageDemandReadReport,
    pub recovery: RelationalRowDeltaReadReport,
    pub live_entries_visited: usize,
    pub overlay_entries: usize,
    pub overlay_resident_bytes: usize,
    pub overlay_replacements: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageSnapshotReadError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    MissingTable(String),
    Stopped(RuntimeCancellationReason),
}

impl fmt::Display for RelationalRowPageSnapshotReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "relational snapshot-read admission failed: {message}"
                )
            }
            Self::Corrupt(message) => write!(formatter, "corrupt relational snapshot: {message}"),
            Self::Durability(message) => {
                write!(
                    formatter,
                    "relational snapshot-read durability failed: {message}"
                )
            }
            Self::MissingTable(table) => {
                write!(formatter, "relational snapshot has no table {table}")
            }
            Self::Stopped(reason) => {
                write!(formatter, "relational snapshot read stopped: {reason}")
            }
        }
    }
}

impl std::error::Error for RelationalRowPageSnapshotReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stopped(reason) => Some(reason),
            _ => None,
        }
    }
}

pub struct RelationalRowPageSnapshotReader {
    view: Arc<RelationalRowPageReadView>,
    demand: RelationalRowPageDemandReader,
    overlay_overflow: Option<Arc<RelationalOverflowRootReader>>,
    poisoned: AtomicBool,
}

impl fmt::Debug for RelationalRowPageSnapshotReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationalRowPageSnapshotReader")
            .field("identity", &self.view.identity())
            .field("has_recovery", &self.view.recovery_delta().is_some())
            .field("live_batches", &self.view.live_batch_count())
            .field("poisoned", &self.is_poisoned())
            .finish()
    }
}

impl RelationalRowPageSnapshotReader {
    pub fn new(
        view: Arc<RelationalRowPageReadView>,
        base_overflow: Arc<RelationalOverflowRootReader>,
        overlay_overflow: Option<Arc<RelationalOverflowRootReader>>,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalRowPageSnapshotReadError> {
        view.validate_serving_fence().map_err(map_delta_error)?;
        match view.recovery_delta() {
            Some(delta) => delta
                .validate_overflow_root(overlay_overflow.as_deref())
                .map_err(map_delta_error)?,
            None if overlay_overflow.is_some() => {
                return Err(RelationalRowPageSnapshotReadError::Admission(
                    "an overlay overflow root requires a recovery delta".to_string(),
                ));
            }
            None => {}
        }
        let demand =
            RelationalRowPageDemandReader::new(view.pinned_base(), base_overflow, cache, store_id)
                .map_err(map_demand_error)?;
        Ok(Self {
            view,
            demand,
            overlay_overflow,
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn identity(&self) -> RelationalRowPageReadViewIdentity {
        self.view.identity()
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
            || self.demand.is_poisoned()
            || self
                .view
                .recovery_delta()
                .is_some_and(|delta| delta.is_poisoned())
    }

    pub fn point_projected(
        &self,
        table: &str,
        primary_key: &RelationalKey,
        requested_fields: &[usize],
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
    ) -> Result<
        (
            Option<RelationalProjectedRow>,
            RelationalRowPageSnapshotPointReport,
        ),
        RelationalRowPageSnapshotReadError,
    > {
        self.checkpoint(task)?;
        let (overlay, recovery, live_batches_examined, selected_live) = self
            .view
            .overlay_value_accounted(table, primary_key)
            .map_err(|error| self.map_delta_error(error))?;
        self.checkpoint(task)?;
        let (row, demand, source, overlay_resident_bytes) = match overlay {
            Some(value) => {
                let deleted = matches!(value, RelationalRowPageRecoveredValue::Deleted);
                let table_root = self
                    .view
                    .base()
                    .table_root(table)
                    .map_err(|error| self.map_row_publication_error(error))?;
                let column_count = table_root.column_count.get() as usize;
                super::validate_requested_fields(
                    requested_fields,
                    column_count,
                    self.view.base().publication_config().page_limits,
                )
                .map_err(|error| self.map_row_error(error))?;
                validate_overlay_row(&value, column_count, self.overlay_overflow.is_some())?;
                let value_resident_bytes =
                    projected_overlay_resident_bytes(&value, requested_fields)?;
                let overlay_resident_bytes =
                    overlay_point_resident_bytes(primary_key, value_resident_bytes)?;
                if overlay_resident_bytes > limits.max_overlay_bytes.get() {
                    return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                        "overlay point requires {overlay_resident_bytes} bytes, exceeding limit {}",
                        limits.max_overlay_bytes
                    )));
                }
                let value = project_overlay_value(&value, requested_fields);
                let (row, demand) = self
                    .demand
                    .point_projected_overlay(
                        RelationalRowPageOverlayPoint {
                            table,
                            primary_key: primary_key.clone(),
                            value,
                            overflow_root: self.overlay_overflow.as_deref(),
                        },
                        limits.demand,
                        hydration,
                        task,
                    )
                    .map_err(|error| self.map_demand_error(error))?;
                let source = if deleted {
                    RelationalRowPageSnapshotRowSource::Deleted
                } else if selected_live {
                    RelationalRowPageSnapshotRowSource::Live
                } else {
                    RelationalRowPageSnapshotRowSource::Recovery
                };
                (row, demand, source, overlay_resident_bytes)
            }
            None => {
                let (row, demand) = self
                    .demand
                    .point_projected(
                        table,
                        primary_key,
                        requested_fields,
                        limits.demand,
                        hydration,
                        task,
                    )
                    .map_err(|error| self.map_demand_error(error))?;
                let source = if row.is_some() {
                    RelationalRowPageSnapshotRowSource::Checkpoint
                } else {
                    RelationalRowPageSnapshotRowSource::Missing
                };
                (row, demand, source, 0)
            }
        };
        Ok((
            row,
            RelationalRowPageSnapshotPointReport {
                identity: self.identity(),
                source,
                demand,
                recovery,
                live_batches_examined,
                overlay_resident_bytes,
            },
        ))
    }

    pub fn visit_projected_range(
        &self,
        range: RelationalRowPageProjectedRange<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
        visit: impl FnMut(RelationalProjectedRow) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        self.checkpoint(task)?;
        let (overlay, overlay_report) = self.collect_overlay(range, limits, task)?;
        self.checkpoint(task)?;
        let demand = self
            .demand
            .visit_projected_range_with_overlay(
                range,
                limits.demand,
                hydration,
                task,
                RelationalRowPageOverlayRange {
                    rows: overlay,
                    overflow_root: self.overlay_overflow.as_deref(),
                },
                visit,
            )
            .map_err(|error| self.map_demand_error(error))?;
        Ok(RelationalRowPageSnapshotRangeReport {
            identity: self.identity(),
            demand,
            recovery: overlay_report.recovery,
            live_entries_visited: overlay_report.live_entries_visited,
            overlay_entries: overlay_report.overlay_entries,
            overlay_resident_bytes: overlay_report.overlay_resident_bytes,
            overlay_replacements: overlay_report.overlay_replacements,
        })
    }

    fn collect_overlay(
        &self,
        range: RelationalRowPageProjectedRange<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        task: &RuntimeTaskContext,
    ) -> Result<
        (
            BTreeMap<RelationalKey, RelationalRowPageProjectedOverlayValue>,
            OverlayCollectionReport,
        ),
        RelationalRowPageSnapshotReadError,
    > {
        let table_root = self
            .view
            .base()
            .table_root(range.table)
            .map_err(|error| self.map_row_publication_error(error))?;
        let column_count = table_root.column_count.get() as usize;
        super::validate_requested_fields(
            range.requested_fields,
            column_count,
            self.view.base().publication_config().page_limits,
        )
        .map_err(|error| self.map_row_error(error))?;
        let mut collector = OverlayCollector::new(
            self.identity(),
            column_count,
            range.requested_fields,
            self.overlay_overflow.is_some(),
            limits,
        );
        let mut collection_error = None;
        let visited = self
            .view
            .visit_overlay_range_entries(
                range.table,
                range.lower,
                range.upper,
                |key, value, epoch| {
                    if let Err(reason) = task.checkpoint() {
                        collection_error =
                            Some(RelationalRowPageSnapshotReadError::Stopped(reason));
                        return false;
                    }
                    if let Err(error) = collector.insert(key, value, epoch) {
                        collection_error = Some(error);
                        return false;
                    }
                    true
                },
            )
            .map_err(|error| self.map_delta_error(error))?;
        if let Some(error) = collection_error {
            self.poison_if_needed(&error);
            return Err(error);
        }
        if visited.stopped_early {
            let error = RelationalRowPageSnapshotReadError::Corrupt(
                "overlay traversal stopped without a typed read error".to_string(),
            );
            self.poison_if_needed(&error);
            return Err(error);
        }
        let (entries, overlay_entries, resident_bytes, replacements) = collector.finish();
        Ok((
            entries,
            OverlayCollectionReport {
                recovery: visited.recovery,
                live_entries_visited: visited.live_entries_visited,
                overlay_entries,
                overlay_resident_bytes: resident_bytes,
                overlay_replacements: replacements,
            },
        ))
    }

    fn checkpoint(
        &self,
        task: &RuntimeTaskContext,
    ) -> Result<(), RelationalRowPageSnapshotReadError> {
        if self.is_poisoned() {
            return Err(RelationalRowPageSnapshotReadError::Corrupt(
                "relational row snapshot reader is poisoned".to_string(),
            ));
        }
        task.checkpoint()
            .map_err(RelationalRowPageSnapshotReadError::Stopped)
    }

    fn map_delta_error(
        &self,
        error: RelationalRowDeltaError,
    ) -> RelationalRowPageSnapshotReadError {
        let mapped = map_delta_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_demand_error(
        &self,
        error: RelationalRowPageDemandReadError,
    ) -> RelationalRowPageSnapshotReadError {
        let mapped = map_demand_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_row_error(&self, error: RelationalRowPageError) -> RelationalRowPageSnapshotReadError {
        let mapped = map_row_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_row_publication_error(
        &self,
        error: RelationalRowPagePublicationError,
    ) -> RelationalRowPageSnapshotReadError {
        let mapped = map_row_publication_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn poison_if_needed(&self, error: &RelationalRowPageSnapshotReadError) {
        if matches!(
            error,
            RelationalRowPageSnapshotReadError::Corrupt(_)
                | RelationalRowPageSnapshotReadError::Durability(_)
        ) {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

#[derive(Debug)]
struct OverlayVersion {
    epoch: u64,
    value: RelationalRowPageProjectedOverlayValue,
    value_resident_bytes: usize,
}

#[derive(Debug)]
struct OverlayCollectionReport {
    recovery: RelationalRowDeltaReadReport,
    live_entries_visited: usize,
    overlay_entries: usize,
    overlay_resident_bytes: usize,
    overlay_replacements: usize,
}

struct OverlayCollector<'a> {
    rows: BTreeMap<RelationalKey, OverlayVersion>,
    resident_bytes: usize,
    replacements: usize,
    identity: RelationalRowPageReadViewIdentity,
    column_count: usize,
    requested_fields: &'a [usize],
    has_overlay_overflow: bool,
    limits: RelationalRowPageSnapshotReadLimits,
}

impl<'a> OverlayCollector<'a> {
    fn new(
        identity: RelationalRowPageReadViewIdentity,
        column_count: usize,
        requested_fields: &'a [usize],
        has_overlay_overflow: bool,
        limits: RelationalRowPageSnapshotReadLimits,
    ) -> Self {
        Self {
            rows: BTreeMap::new(),
            resident_bytes: 0,
            replacements: 0,
            identity,
            column_count,
            requested_fields,
            has_overlay_overflow,
            limits,
        }
    }

    fn insert(
        &mut self,
        key: &RelationalKey,
        value: &RelationalRowPageRecoveredValue,
        epoch: u64,
    ) -> Result<(), RelationalRowPageSnapshotReadError> {
        if epoch <= self.identity.base_commit_epoch || epoch > self.identity.visible_commit_epoch {
            return Err(RelationalRowPageSnapshotReadError::Corrupt(format!(
                "overlay row epoch {epoch} is outside ({}, {}]",
                self.identity.base_commit_epoch, self.identity.visible_commit_epoch
            )));
        }
        if let Some(current_epoch) = self.rows.get(key).map(|current| current.epoch) {
            if epoch == current_epoch {
                return Err(RelationalRowPageSnapshotReadError::Corrupt(format!(
                    "overlay contains duplicate row version at epoch {epoch}"
                )));
            }
            if epoch < current_epoch {
                return Ok(());
            }
        }
        validate_overlay_row(value, self.column_count, self.has_overlay_overflow)?;
        let value_resident_bytes = projected_overlay_resident_bytes(value, self.requested_fields)?;
        if let Some(current) = self.rows.get_mut(key) {
            let next_bytes = self
                .resident_bytes
                .checked_sub(current.value_resident_bytes)
                .and_then(|bytes| bytes.checked_add(value_resident_bytes))
                .ok_or_else(|| {
                    RelationalRowPageSnapshotReadError::Admission(
                        "overlay replacement byte accounting overflow".to_string(),
                    )
                })?;
            if next_bytes > self.limits.max_overlay_bytes.get() {
                return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                    "overlay requires {next_bytes} bytes, exceeding limit {}",
                    self.limits.max_overlay_bytes
                )));
            }
            let value = project_overlay_value(value, self.requested_fields);
            current.epoch = epoch;
            current.value = value;
            current.value_resident_bytes = value_resident_bytes;
            self.resident_bytes = next_bytes;
            self.replacements = self.replacements.checked_add(1).ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay replacement counter overflow".to_string(),
                )
            })?;
            return Ok(());
        }

        let next_entries = self.rows.len().checked_add(1).ok_or_else(|| {
            RelationalRowPageSnapshotReadError::Admission(
                "overlay entry counter overflow".to_string(),
            )
        })?;
        if next_entries > self.limits.max_overlay_entries.get() {
            return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                "overlay contains {next_entries} entries, exceeding limit {}",
                self.limits.max_overlay_entries
            )));
        }
        let entry_bytes = overlay_key_resident_bytes(key)?
            .checked_add(value_resident_bytes)
            .and_then(|bytes| bytes.checked_add(size_of::<OverlayVersion>()))
            .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay entry byte accounting overflow".to_string(),
                )
            })?;
        let next_bytes = self
            .resident_bytes
            .checked_add(entry_bytes)
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay resident-byte accounting overflow".to_string(),
                )
            })?;
        if next_bytes > self.limits.max_overlay_bytes.get() {
            return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                "overlay requires {next_bytes} bytes, exceeding limit {}",
                self.limits.max_overlay_bytes
            )));
        }
        let value = project_overlay_value(value, self.requested_fields);
        self.rows.insert(
            key.clone(),
            OverlayVersion {
                epoch,
                value,
                value_resident_bytes,
            },
        );
        self.resident_bytes = next_bytes;
        Ok(())
    }

    fn finish(
        self,
    ) -> (
        BTreeMap<RelationalKey, RelationalRowPageProjectedOverlayValue>,
        usize,
        usize,
        usize,
    ) {
        let entries = self
            .rows
            .into_iter()
            .map(|(key, version)| (key, version.value))
            .collect::<BTreeMap<_, _>>();
        let entry_count = entries.len();
        (entries, entry_count, self.resident_bytes, self.replacements)
    }
}

fn validate_overlay_row(
    value: &RelationalRowPageRecoveredValue,
    column_count: usize,
    has_overlay_overflow: bool,
) -> Result<(), RelationalRowPageSnapshotReadError> {
    let RelationalRowPageRecoveredValue::Present(row) = value else {
        return Ok(());
    };
    if row.values().len() != column_count {
        return Err(RelationalRowPageSnapshotReadError::Corrupt(format!(
            "overlay row contains {} columns, expected {column_count}",
            row.values().len()
        )));
    }
    if !has_overlay_overflow
        && row
            .values()
            .iter()
            .any(|value| matches!(value, RelationalValue::Overflow(_)))
    {
        return Err(RelationalRowPageSnapshotReadError::Corrupt(
            "overlay row references overflow without a generation-bound overlay root".to_string(),
        ));
    }
    Ok(())
}

fn projected_overlay_resident_bytes(
    value: &RelationalRowPageRecoveredValue,
    requested_fields: &[usize],
) -> Result<usize, RelationalRowPageSnapshotReadError> {
    let RelationalRowPageRecoveredValue::Present(row) = value else {
        return Ok(0);
    };
    requested_fields.iter().try_fold(
        size_of::<RelationalRowPageProjectedOverlayValue>()
            .checked_add(
                requested_fields
                    .len()
                    .checked_mul(size_of::<RelationalProjectedField>())
                    .ok_or_else(|| {
                        RelationalRowPageSnapshotReadError::Admission(
                            "overlay projection allocation accounting overflow".to_string(),
                        )
                    })?,
            )
            .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay projection byte accounting overflow".to_string(),
                )
            })?,
        |bytes, ordinal| {
            bytes
                .checked_add(row.values()[*ordinal].estimated_payload_bytes())
                .ok_or_else(|| {
                    RelationalRowPageSnapshotReadError::Admission(
                        "overlay projection payload accounting overflow".to_string(),
                    )
                })
        },
    )
}

fn project_overlay_value(
    value: &RelationalRowPageRecoveredValue,
    requested_fields: &[usize],
) -> RelationalRowPageProjectedOverlayValue {
    let RelationalRowPageRecoveredValue::Present(row) = value else {
        return RelationalRowPageProjectedOverlayValue::Deleted;
    };
    RelationalRowPageProjectedOverlayValue::Present(
        requested_fields
            .iter()
            .map(|ordinal| RelationalProjectedField {
                ordinal: *ordinal,
                value: row.values()[*ordinal].clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    )
}

fn overlay_point_resident_bytes(
    key: &RelationalKey,
    value_resident_bytes: usize,
) -> Result<usize, RelationalRowPageSnapshotReadError> {
    overlay_key_resident_bytes(key)?
        .checked_add(value_resident_bytes)
        .and_then(|bytes| bytes.checked_add(size_of::<RelationalProjectedRow>()))
        .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
        .ok_or_else(|| {
            RelationalRowPageSnapshotReadError::Admission(
                "overlay point resident-byte accounting overflow".to_string(),
            )
        })
}

fn overlay_key_resident_bytes(
    key: &RelationalKey,
) -> Result<usize, RelationalRowPageSnapshotReadError> {
    key.0.iter().try_fold(
        size_of::<RelationalKey>()
            .checked_add(
                key.0
                    .len()
                    .checked_mul(size_of::<RelationalValue>())
                    .ok_or_else(|| {
                        RelationalRowPageSnapshotReadError::Admission(
                            "overlay key allocation accounting overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay key byte accounting overflow".to_string(),
                )
            })?,
        |bytes, value| {
            bytes
                .checked_add(value.estimated_payload_bytes())
                .ok_or_else(|| {
                    RelationalRowPageSnapshotReadError::Admission(
                        "overlay key payload accounting overflow".to_string(),
                    )
                })
        },
    )
}

fn map_delta_error(error: RelationalRowDeltaError) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowDeltaError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowDeltaError::Durability(message) => {
            RelationalRowPageSnapshotReadError::Durability(message)
        }
        RelationalRowDeltaError::Row(error) => map_row_error(error),
        RelationalRowDeltaError::Publication(error) => map_row_publication_error(error),
        error @ (RelationalRowDeltaError::Corrupt(_)
        | RelationalRowDeltaError::Invalidated(_)
        | RelationalRowDeltaError::StaleGeneration { .. }
        | RelationalRowDeltaError::StaleBase { .. }) => {
            RelationalRowPageSnapshotReadError::Corrupt(error.to_string())
        }
    }
}

fn map_demand_error(error: RelationalRowPageDemandReadError) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowPageDemandReadError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowPageDemandReadError::Corrupt(message) => {
            RelationalRowPageSnapshotReadError::Corrupt(message)
        }
        RelationalRowPageDemandReadError::Durability(message) => {
            RelationalRowPageSnapshotReadError::Durability(message)
        }
        RelationalRowPageDemandReadError::MissingTable(table) => {
            RelationalRowPageSnapshotReadError::MissingTable(table)
        }
        RelationalRowPageDemandReadError::Stopped(reason) => {
            RelationalRowPageSnapshotReadError::Stopped(reason)
        }
    }
}

fn map_row_error(error: RelationalRowPageError) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowPageError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowPageError::Corrupt(message) => {
            RelationalRowPageSnapshotReadError::Corrupt(message)
        }
    }
}

fn map_row_publication_error(
    error: RelationalRowPagePublicationError,
) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowPagePublicationError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowPagePublicationError::Corrupt(message) => {
            RelationalRowPageSnapshotReadError::Corrupt(message)
        }
        RelationalRowPagePublicationError::Durability(message) => {
            RelationalRowPageSnapshotReadError::Durability(message)
        }
        RelationalRowPagePublicationError::MissingTable(table) => {
            RelationalRowPageSnapshotReadError::MissingTable(table)
        }
        error @ RelationalRowPagePublicationError::StaleGeneration { .. } => {
            RelationalRowPageSnapshotReadError::Corrupt(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests;
