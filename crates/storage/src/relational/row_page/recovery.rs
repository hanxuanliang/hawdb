//! Bounded, non-serving WAL overlay for a generation-pinned relational row root.
//!
//! The published root remains canonical. Recovery replays every consecutive WAL
//! batch into an exact primary-key overlay and publishes an immutable in-process
//! view only after the requested durable prefix has been consumed.

use super::{
    RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPageRootReader,
};
use crate::relational::{
    ordered_key::encode_ordered_relational_key, RelationalDecodeLimits, RelationalKey,
    RelationalMutationLimits, RelationalOverflowConfig, RelationalRow, RelationalRowChange,
    RelationalRowChangeCapture, RelationalRowChangeCaptureLimits, RelationalState, RelationalValue,
    RelationalWalBatch,
};
use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

pub const DEFAULT_RELATIONAL_ROW_PAGE_RECOVERY_ENTRIES: usize = 100_000;
pub const DEFAULT_RELATIONAL_ROW_PAGE_RECOVERY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageRecoveryConfig {
    pub max_overlay_entries: NonZeroUsize,
    pub max_overlay_bytes: NonZeroUsize,
}

impl Default for RelationalRowPageRecoveryConfig {
    fn default() -> Self {
        Self {
            max_overlay_entries: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_RECOVERY_ENTRIES)
                .expect("default row recovery entry limit is non-zero"),
            max_overlay_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_RECOVERY_BYTES)
                .expect("default row recovery byte limit is non-zero"),
        }
    }
}

impl RelationalRowPageRecoveryConfig {
    pub const fn capture_limits(self) -> RelationalRowChangeCaptureLimits {
        RelationalRowChangeCaptureLimits {
            max_entries: self.max_overlay_entries,
            max_bytes: self.max_overlay_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageRecoveryIdentity {
    pub base_generation: u64,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageRecoveredValue {
    Present(RelationalRow),
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageRecoveryReport {
    pub identity: RelationalRowPageRecoveryIdentity,
    pub replayed_batches: u64,
    pub overlay_entries: usize,
    pub overlay_bytes: usize,
    pub peak_overlay_entries: usize,
    pub peak_overlay_bytes: usize,
}

#[derive(Debug)]
pub enum RelationalRowPageRecoveryError {
    Admission(String),
    Corrupt(String),
    Invalidated(String),
    Publication(RelationalRowPagePublicationError),
    Relational(super::super::RelationalError),
}

impl fmt::Display for RelationalRowPageRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "relational row recovery admission failed: {message}"
                )
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt relational row recovery: {message}")
            }
            Self::Invalidated(message) => {
                write!(formatter, "relational row recovery invalidated: {message}")
            }
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Relational(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for RelationalRowPageRecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Publication(error) => Some(error),
            Self::Relational(error) => Some(error),
            Self::Admission(_) | Self::Corrupt(_) | Self::Invalidated(_) => None,
        }
    }
}

impl From<RelationalRowPagePublicationError> for RelationalRowPageRecoveryError {
    fn from(error: RelationalRowPagePublicationError) -> Self {
        Self::Publication(error)
    }
}

impl From<super::super::RelationalError> for RelationalRowPageRecoveryError {
    fn from(error: super::super::RelationalError) -> Self {
        Self::Relational(error)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OverlayKey {
    table: String,
    primary_key: RelationalKey,
}

#[derive(Debug, Clone)]
struct OverlayEntry {
    value: RelationalRowPageRecoveredValue,
    charged_bytes: usize,
    last_modified_epoch: u64,
}

type OverlayMap = BTreeMap<String, BTreeMap<RelationalKey, OverlayEntry>>;

#[derive(Debug)]
pub struct RelationalRowPageRecoveryBuilder {
    base: Arc<RelationalRowPageRootReader>,
    config: RelationalRowPageRecoveryConfig,
    visible_commit_epoch: u64,
    replayed_batches: u64,
    overlay: OverlayMap,
    overlay_entries: usize,
    overlay_bytes: usize,
    peak_overlay_entries: usize,
    peak_overlay_bytes: usize,
}

impl RelationalRowPageRecoveryBuilder {
    pub fn open_latest(
        directory: &Path,
        publication_config: RelationalRowPagePublicationConfig,
        recovery_config: RelationalRowPageRecoveryConfig,
    ) -> Result<Option<Self>, RelationalRowPageRecoveryError> {
        RelationalRowPageRootReader::open_latest(directory, publication_config)?
            .map(|base| Self::from_base(base, recovery_config))
            .transpose()
    }

    pub fn from_base(
        base: RelationalRowPageRootReader,
        config: RelationalRowPageRecoveryConfig,
    ) -> Result<Self, RelationalRowPageRecoveryError> {
        let base_epoch = base.manifest().source_commit_epoch;
        if base_epoch == 0 || base.manifest().generation == 0 {
            return Err(RelationalRowPageRecoveryError::Corrupt(
                "row recovery base identity must be non-zero".to_string(),
            ));
        }
        Ok(Self {
            base: Arc::new(base),
            config,
            visible_commit_epoch: base_epoch,
            replayed_batches: 0,
            overlay: BTreeMap::new(),
            overlay_entries: 0,
            overlay_bytes: 0,
            peak_overlay_entries: 0,
            peak_overlay_bytes: 0,
        })
    }

    pub const fn capture_limits(&self) -> RelationalRowChangeCaptureLimits {
        self.config.capture_limits()
    }

    pub fn base_commit_epoch(&self) -> u64 {
        self.base.manifest().source_commit_epoch
    }

    pub fn validate_base_schema(
        &self,
        state: &RelationalState,
    ) -> Result<(), RelationalRowPageRecoveryError> {
        let manifest = self.base.manifest();
        let schema_count = state.table_schemas().count();
        if manifest.tables.len() != schema_count {
            return Err(RelationalRowPageRecoveryError::Corrupt(format!(
                "row root contains {} table schemas, checkpoint contains {schema_count}",
                manifest.tables.len()
            )));
        }
        for schema in state.table_schemas() {
            let table = manifest
                .tables
                .binary_search_by(|table| table.table.as_str().cmp(schema.name.as_str()))
                .ok()
                .map(|index| &manifest.tables[index])
                .ok_or_else(|| {
                    RelationalRowPageRecoveryError::Corrupt(format!(
                        "row root is missing checkpoint table {}",
                        schema.name
                    ))
                })?;
            let expected = state.table_schema_digest(&schema.name)?.ok_or_else(|| {
                RelationalRowPageRecoveryError::Corrupt(format!(
                    "checkpoint table {} disappeared while validating its row root",
                    schema.name
                ))
            })?;
            if table.schema_digest != expected {
                return Err(RelationalRowPageRecoveryError::Corrupt(format!(
                    "row root schema digest for table {} does not match the checkpoint",
                    schema.name
                )));
            }
        }
        Ok(())
    }

    pub fn visible_commit_epoch(&self) -> u64 {
        self.visible_commit_epoch
    }

    pub fn replay_encoded_wal_batch(
        &mut self,
        state: &RelationalState,
        encoded: &[u8],
        decode_limits: RelationalDecodeLimits,
        mutation_limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Result<RelationalState, RelationalRowPageRecoveryError> {
        let batch = super::super::decode_relational_wal_batch(encoded, decode_limits)?;
        self.replay_wal_batch(state, batch, mutation_limits, overflow_config)
    }

    pub fn replay_wal_batch(
        &mut self,
        state: &RelationalState,
        batch: RelationalWalBatch,
        mutation_limits: RelationalMutationLimits,
        overflow_config: RelationalOverflowConfig,
    ) -> Result<RelationalState, RelationalRowPageRecoveryError> {
        self.require_next_epoch(batch.epoch)?;
        let (next, capture) = state.stage_transaction_with_row_changes(
            batch.transaction,
            mutation_limits,
            overflow_config,
            self.capture_limits(),
        )?;
        self.record(batch.epoch, capture)?;
        Ok(next)
    }

    pub fn record(
        &mut self,
        epoch: u64,
        capture: RelationalRowChangeCapture,
    ) -> Result<(), RelationalRowPageRecoveryError> {
        self.require_current_or_next_epoch(epoch)?;
        let RelationalRowChangeCapture::Captured { changes, .. } = capture else {
            let RelationalRowChangeCapture::Invalidated { reason } = capture else {
                unreachable!()
            };
            return Err(RelationalRowPageRecoveryError::Invalidated(reason));
        };

        let mut batch = BTreeMap::new();
        for change in changes {
            validate_change(&change)?;
            let RelationalRowChange {
                table,
                primary_key,
                row,
            } = change;
            let key = OverlayKey { table, primary_key };
            let value = row.map_or(RelationalRowPageRecoveredValue::Deleted, |row| {
                RelationalRowPageRecoveredValue::Present(row)
            });
            let charged_bytes = charged_change_bytes(&key, &value)?;
            batch.insert(
                key,
                OverlayEntry {
                    value,
                    charged_bytes,
                    last_modified_epoch: epoch,
                },
            );
        }

        let replaced_bytes = batch.keys().try_fold(0usize, |bytes, key| {
            bytes
                .checked_add(
                    self.overlay
                        .get(&key.table)
                        .and_then(|table| table.get(&key.primary_key))
                        .map_or(0, |entry| entry.charged_bytes),
                )
                .ok_or_else(|| {
                    RelationalRowPageRecoveryError::Admission(
                        "row recovery replacement byte accounting overflow".to_string(),
                    )
                })
        })?;
        let batch_bytes = batch.values().try_fold(0usize, |bytes, entry| {
            bytes.checked_add(entry.charged_bytes).ok_or_else(|| {
                RelationalRowPageRecoveryError::Admission(
                    "row recovery batch byte accounting overflow".to_string(),
                )
            })
        })?;
        let next_bytes = self
            .overlay_bytes
            .checked_sub(replaced_bytes)
            .and_then(|bytes| bytes.checked_add(batch_bytes))
            .ok_or_else(|| {
                RelationalRowPageRecoveryError::Admission(
                    "row recovery overlay byte accounting overflow".to_string(),
                )
            })?;
        let replaced_entries = batch
            .keys()
            .filter(|key| {
                self.overlay
                    .get(&key.table)
                    .is_some_and(|table| table.contains_key(&key.primary_key))
            })
            .count();
        let next_entries = self
            .overlay_entries
            .checked_sub(replaced_entries)
            .and_then(|entries| entries.checked_add(batch.len()))
            .ok_or_else(|| {
                RelationalRowPageRecoveryError::Admission(
                    "row recovery overlay entry accounting overflow".to_string(),
                )
            })?;
        if next_entries > self.config.max_overlay_entries.get()
            || next_bytes > self.config.max_overlay_bytes.get()
        {
            return Err(RelationalRowPageRecoveryError::Admission(format!(
                "row recovery overlay would contain {next_entries} entries/{next_bytes} bytes, exceeding limits {}/{}",
                self.config.max_overlay_entries, self.config.max_overlay_bytes
            )));
        }
        let next_replayed_batches = if epoch == self.visible_commit_epoch {
            self.replayed_batches
        } else {
            self.replayed_batches.checked_add(1).ok_or_else(|| {
                RelationalRowPageRecoveryError::Admission(
                    "row recovery replayed batch count overflow".to_string(),
                )
            })?
        };

        for (key, entry) in batch {
            self.overlay
                .entry(key.table)
                .or_default()
                .insert(key.primary_key, entry);
        }
        self.overlay_entries = next_entries;
        self.overlay_bytes = next_bytes;
        self.visible_commit_epoch = epoch;
        self.replayed_batches = next_replayed_batches;
        self.peak_overlay_entries = self.peak_overlay_entries.max(self.overlay_entries);
        self.peak_overlay_bytes = self.peak_overlay_bytes.max(self.overlay_bytes);
        Ok(())
    }

    pub fn advance_empty(&mut self, epoch: u64) -> Result<(), RelationalRowPageRecoveryError> {
        self.record(
            epoch,
            RelationalRowChangeCapture::Captured {
                changes: Vec::new(),
                encoded_bytes: 0,
            },
        )
    }

    pub fn finish(
        self,
        expected_recovered_commit_epoch: u64,
    ) -> Result<RelationalRowPageRecoveryView, RelationalRowPageRecoveryError> {
        if self.visible_commit_epoch != expected_recovered_commit_epoch {
            return Err(RelationalRowPageRecoveryError::Corrupt(format!(
                "row recovery consumed through epoch {}, expected {expected_recovered_commit_epoch}",
                self.visible_commit_epoch
            )));
        }
        let identity = RelationalRowPageRecoveryIdentity {
            base_generation: self.base.manifest().generation,
            base_commit_epoch: self.base.manifest().source_commit_epoch,
            visible_commit_epoch: self.visible_commit_epoch,
        };
        let report = RelationalRowPageRecoveryReport {
            identity,
            replayed_batches: self.replayed_batches,
            overlay_entries: self.overlay_entries,
            overlay_bytes: self.overlay_bytes,
            peak_overlay_entries: self.peak_overlay_entries,
            peak_overlay_bytes: self.peak_overlay_bytes,
        };
        Ok(RelationalRowPageRecoveryView {
            base: self.base,
            identity,
            overlay: Arc::new(self.overlay),
            report,
        })
    }

    fn require_next_epoch(&self, epoch: u64) -> Result<(), RelationalRowPageRecoveryError> {
        let expected = self.visible_commit_epoch.checked_add(1).ok_or_else(|| {
            RelationalRowPageRecoveryError::Corrupt(
                "row recovery visible epoch overflow".to_string(),
            )
        })?;
        if epoch != expected {
            return Err(RelationalRowPageRecoveryError::Corrupt(format!(
                "row recovery WAL epoch gap: expected {expected}, found {epoch}"
            )));
        }
        Ok(())
    }

    fn require_current_or_next_epoch(
        &self,
        epoch: u64,
    ) -> Result<(), RelationalRowPageRecoveryError> {
        if epoch == self.visible_commit_epoch {
            return Ok(());
        }
        self.require_next_epoch(epoch)
    }
}

#[derive(Debug, Clone)]
pub struct RelationalRowPageRecoveryView {
    base: Arc<RelationalRowPageRootReader>,
    identity: RelationalRowPageRecoveryIdentity,
    overlay: Arc<OverlayMap>,
    report: RelationalRowPageRecoveryReport,
}

impl RelationalRowPageRecoveryView {
    pub const fn identity(&self) -> RelationalRowPageRecoveryIdentity {
        self.identity
    }

    pub fn base(&self) -> &RelationalRowPageRootReader {
        &self.base
    }

    pub fn report(&self) -> &RelationalRowPageRecoveryReport {
        &self.report
    }

    pub fn overlay_value(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Option<&RelationalRowPageRecoveredValue> {
        self.overlay
            .get(table)
            .and_then(|rows| rows.get(primary_key))
            .map(|entry| &entry.value)
    }

    pub fn visit_overlay(
        &self,
        mut visit: impl FnMut(&str, &RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
    ) {
        for (table, rows) in self.overlay.iter() {
            for (primary_key, entry) in rows {
                if !visit(table, primary_key, &entry.value, entry.last_modified_epoch) {
                    return;
                }
            }
        }
    }
}

fn validate_change(change: &RelationalRowChange) -> Result<(), RelationalRowPageRecoveryError> {
    if change.table.is_empty() {
        return Err(RelationalRowPageRecoveryError::Corrupt(
            "row recovery change has an empty table name".to_string(),
        ));
    }
    encode_ordered_relational_key(&change.primary_key).map_err(|error| {
        RelationalRowPageRecoveryError::Corrupt(format!(
            "row recovery change has an invalid primary key: {error}"
        ))
    })?;
    if change.row.as_ref().is_some_and(|row| {
        row.values()
            .iter()
            .any(|value| matches!(value, RelationalValue::Overflow(_)))
    }) {
        return Err(RelationalRowPageRecoveryError::Admission(
            "row recovery cannot retain overflow references before overflow publication is active"
                .to_string(),
        ));
    }
    Ok(())
}

fn charged_change_bytes(
    key: &OverlayKey,
    value: &RelationalRowPageRecoveredValue,
) -> Result<usize, RelationalRowPageRecoveryError> {
    const FIXED_BYTES: usize = 64;
    let encoded_key = encode_ordered_relational_key(&key.primary_key).map_err(|error| {
        RelationalRowPageRecoveryError::Corrupt(format!(
            "row recovery key cannot be encoded: {error}"
        ))
    })?;
    let row_bytes = match value {
        RelationalRowPageRecoveredValue::Deleted => 0,
        RelationalRowPageRecoveredValue::Present(row) => row.values().iter().try_fold(
            row.values()
                .len()
                .checked_mul(std::mem::size_of::<RelationalValue>())
                .ok_or_else(|| {
                    RelationalRowPageRecoveryError::Admission(
                        "row recovery row allocation accounting overflow".to_string(),
                    )
                })?,
            |bytes, value| {
                bytes
                    .checked_add(value.estimated_payload_bytes())
                    .ok_or_else(|| {
                        RelationalRowPageRecoveryError::Admission(
                            "row recovery row payload accounting overflow".to_string(),
                        )
                    })
            },
        )?,
    };
    FIXED_BYTES
        .checked_add(key.table.len())
        .and_then(|bytes| bytes.checked_add(encoded_key.len()))
        .and_then(|bytes| bytes.checked_add(row_bytes))
        .ok_or_else(|| {
            RelationalRowPageRecoveryError::Admission(
                "row recovery entry accounting overflow".to_string(),
            )
        })
}

#[cfg(test)]
#[path = "recovery/tests.rs"]
mod tests;
