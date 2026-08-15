//! Immutable, bounded live overlays over a generation-pinned row root and
//! optional disk-backed WAL recovery delta.
//!
//! Publication is deliberately separate from SQL selection. A caller stages a
//! complete next-epoch view before its WAL append and installs the returned
//! `Arc` only after that WAL batch is durable.

use super::{
    RelationalRowDeltaError, RelationalRowDeltaReader, RelationalRowPageRecoveredValue,
    RelationalRowPageRootReader,
};
use crate::relational::{
    estimated_row_change_encoding_bytes, RelationalKey, RelationalRowChange,
    RelationalRowChangeCapture, RelationalRowChangeCaptureLimits, RelationalValue,
};
use skein_integrity::Sha256Digest;
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageReadViewIdentity {
    pub base_generation: u64,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageLiveError {
    Admission(String),
    Corrupt(String),
    Invalidated(String),
}

impl fmt::Display for RelationalRowPageLiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "relational row live-view admission failed: {message}"
                )
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt relational row live view: {message}")
            }
            Self::Invalidated(message) => {
                write!(formatter, "relational row live view invalidated: {message}")
            }
        }
    }
}

impl std::error::Error for RelationalRowPageLiveError {}

struct RelationalRowPageLiveBatch {
    commit_epoch: u64,
    changes: Arc<[RelationalRowChange]>,
    previous: Option<Arc<RelationalRowPageLiveBatch>>,
}

impl RelationalRowPageLiveBatch {
    fn overlay_value(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Option<RelationalRowPageRecoveredValue> {
        self.changes
            .binary_search_by(|change| {
                change
                    .table
                    .as_str()
                    .cmp(table)
                    .then_with(|| change.primary_key.cmp(primary_key))
            })
            .ok()
            .map(|index| {
                self.changes[index]
                    .row
                    .clone()
                    .map_or(RelationalRowPageRecoveredValue::Deleted, |row| {
                        RelationalRowPageRecoveredValue::Present(row)
                    })
            })
    }
}

#[derive(Debug, Clone)]
struct RelationalRowPageLiveOverlay {
    head: Option<Arc<RelationalRowPageLiveBatch>>,
    batch_count: usize,
    entry_count: usize,
    encoded_bytes: usize,
    resident_bytes: usize,
}

impl fmt::Debug for RelationalRowPageLiveBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationalRowPageLiveBatch")
            .field("commit_epoch", &self.commit_epoch)
            .field("changes", &self.changes.len())
            .field("has_previous", &self.previous.is_some())
            .finish()
    }
}

impl RelationalRowPageLiveOverlay {
    fn empty() -> Self {
        Self {
            head: None,
            batch_count: 0,
            entry_count: 0,
            encoded_bytes: 0,
            resident_bytes: 0,
        }
    }

    fn append(
        &self,
        commit_epoch: u64,
        capture: RelationalRowChangeCapture,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Result<Self, RelationalRowPageLiveError> {
        let (changes, declared_bytes) = match capture {
            RelationalRowChangeCapture::Captured {
                changes,
                encoded_bytes,
            } => (changes, encoded_bytes),
            RelationalRowChangeCapture::Invalidated { reason } => {
                return Err(RelationalRowPageLiveError::Invalidated(reason));
            }
        };
        let capture_resident_bytes = validate_capture(&changes, declared_bytes)?;
        let entry_count = self.entry_count.checked_add(changes.len()).ok_or_else(|| {
            RelationalRowPageLiveError::Admission("live row entry accounting overflow".to_string())
        })?;
        let encoded_bytes = self
            .encoded_bytes
            .checked_add(declared_bytes)
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row byte accounting overflow".to_string(),
                )
            })?;
        let resident_bytes = self
            .resident_bytes
            .checked_add(capture_resident_bytes)
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row resident byte accounting overflow".to_string(),
                )
            })?;
        if entry_count > limits.max_entries.get() || resident_bytes > limits.max_bytes.get() {
            return Err(RelationalRowPageLiveError::Admission(format!(
                "live row overlay would retain {entry_count} entries/{resident_bytes} resident bytes, exceeding limits {}/{}",
                limits.max_entries, limits.max_bytes
            )));
        }
        if changes.is_empty() {
            return Ok(self.clone());
        }
        let batch_count = self.batch_count.checked_add(1).ok_or_else(|| {
            RelationalRowPageLiveError::Admission("live row batch accounting overflow".to_string())
        })?;
        let head = Arc::new(RelationalRowPageLiveBatch {
            commit_epoch,
            changes: Arc::from(changes),
            previous: self.head.as_ref().map(Arc::clone),
        });
        Ok(Self {
            head: Some(head),
            batch_count,
            entry_count,
            encoded_bytes,
            resident_bytes,
        })
    }

    fn overlay_value(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Option<RelationalRowPageRecoveredValue> {
        let mut current = self.head.as_deref();
        while let Some(batch) = current {
            if let Some(value) = batch.overlay_value(table, primary_key) {
                return Some(value);
            }
            current = batch.previous.as_deref();
        }
        None
    }
}

/// One immutable row view pinned to an exact base generation and visible epoch.
///
/// Live batches retain their payload through `Arc` ownership. Advancing a view
/// installs one new persistent-chain head; pinned readers keep their prior
/// identity and payload without copying earlier batches or a database-sized
/// row set.
#[derive(Debug, Clone)]
pub struct RelationalRowPageReadView {
    identity: RelationalRowPageReadViewIdentity,
    base: Arc<RelationalRowPageRootReader>,
    recovery_delta: Option<Arc<RelationalRowDeltaReader>>,
    live: RelationalRowPageLiveOverlay,
}

impl RelationalRowPageReadView {
    pub fn from_base(base: Arc<RelationalRowPageRootReader>) -> Self {
        let manifest = base.manifest();
        Self {
            identity: RelationalRowPageReadViewIdentity {
                base_generation: manifest.generation,
                base_commit_epoch: manifest.source_commit_epoch,
                visible_commit_epoch: manifest.source_commit_epoch,
                root_set_digest: manifest.root_set_digest,
            },
            base,
            recovery_delta: None,
            live: RelationalRowPageLiveOverlay::empty(),
        }
    }

    pub fn from_recovery_delta(
        base: Arc<RelationalRowPageRootReader>,
        recovery_delta: Arc<RelationalRowDeltaReader>,
    ) -> Result<Self, RelationalRowDeltaError> {
        let base_manifest = base.manifest();
        let delta_manifest = recovery_delta.manifest();
        if delta_manifest.base.generation != base_manifest.generation
            || delta_manifest.base.source_commit_epoch != base_manifest.source_commit_epoch
            || delta_manifest.base.root_set_digest != base_manifest.root_set_digest
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row read view cannot combine mismatched base and delta generations".to_string(),
            ));
        }
        Ok(Self {
            identity: RelationalRowPageReadViewIdentity {
                base_generation: base_manifest.generation,
                base_commit_epoch: base_manifest.source_commit_epoch,
                visible_commit_epoch: delta_manifest.visible_commit_epoch,
                root_set_digest: base_manifest.root_set_digest,
            },
            base,
            recovery_delta: Some(recovery_delta),
            live: RelationalRowPageLiveOverlay::empty(),
        })
    }

    pub const fn identity(&self) -> RelationalRowPageReadViewIdentity {
        self.identity
    }

    pub fn base(&self) -> &RelationalRowPageRootReader {
        &self.base
    }

    pub fn pinned_base(&self) -> Arc<RelationalRowPageRootReader> {
        Arc::clone(&self.base)
    }

    pub fn recovery_delta(&self) -> Option<&RelationalRowDeltaReader> {
        self.recovery_delta.as_deref()
    }

    pub fn advance(
        &self,
        next_commit_epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Result<Self, RelationalRowPageLiveError> {
        let expected = self
            .identity
            .visible_commit_epoch
            .checked_add(1)
            .ok_or_else(|| {
                RelationalRowPageLiveError::Corrupt(
                    "relational row read-view epoch overflow".to_string(),
                )
            })?;
        if next_commit_epoch != expected {
            return Err(RelationalRowPageLiveError::Corrupt(format!(
                "relational row read view expected commit epoch {expected}, got {next_commit_epoch}"
            )));
        }
        if let Some(capture) = capture.as_ref() {
            validate_capture_against_base(&self.base, capture)?;
        }
        let live = capture.map_or_else(
            || Ok(self.live.clone()),
            |capture| self.live.append(next_commit_epoch, capture, limits),
        )?;
        let mut identity = self.identity;
        identity.visible_commit_epoch = next_commit_epoch;
        Ok(Self {
            identity,
            base: Arc::clone(&self.base),
            recovery_delta: self.recovery_delta.as_ref().map(Arc::clone),
            live,
        })
    }

    pub fn live_batch_count(&self) -> usize {
        self.live.batch_count
    }

    pub const fn live_entry_count(&self) -> usize {
        self.live.entry_count
    }

    pub const fn live_encoded_bytes(&self) -> usize {
        self.live.encoded_bytes
    }

    pub const fn live_resident_bytes(&self) -> usize {
        self.live.resident_bytes
    }

    pub fn overlay_value(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<Option<RelationalRowPageRecoveredValue>, RelationalRowDeltaError> {
        if let Some(value) = self.live.overlay_value(table, primary_key) {
            return Ok(Some(value));
        }
        self.recovery_delta.as_ref().map_or(Ok(None), |delta| {
            delta
                .lookup(table, primary_key)
                .map(|(value, _report)| value)
        })
    }

    pub fn latest_live_commit_epoch(&self) -> Option<u64> {
        self.live.head.as_ref().map(|batch| batch.commit_epoch)
    }
}

fn validate_capture_against_base(
    base: &RelationalRowPageRootReader,
    capture: &RelationalRowChangeCapture,
) -> Result<(), RelationalRowPageLiveError> {
    let RelationalRowChangeCapture::Captured { changes, .. } = capture else {
        return Ok(());
    };
    let tables = &base.manifest().tables;
    for change in changes {
        if tables
            .binary_search_by(|table| table.table.cmp(&change.table))
            .is_err()
        {
            return Err(RelationalRowPageLiveError::Corrupt(format!(
                "live row change references table {} outside the pinned row root",
                change.table
            )));
        }
        if change.row.as_ref().is_some_and(|row| {
            row.values()
                .iter()
                .any(|value| matches!(value, RelationalValue::Overflow(_)))
        }) {
            return Err(RelationalRowPageLiveError::Invalidated(
                "live row view cannot retain overflow references before live overflow publication is active"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_capture(
    changes: &[RelationalRowChange],
    declared_bytes: usize,
) -> Result<usize, RelationalRowPageLiveError> {
    let mut actual_bytes = 0usize;
    let mut resident_bytes = if changes.is_empty() {
        0
    } else {
        std::mem::size_of::<RelationalRowPageLiveBatch>()
            .checked_add(4 * std::mem::size_of::<usize>())
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row batch allocation accounting overflow".to_string(),
                )
            })?
    };
    let mut previous: Option<(&str, &RelationalKey)> = None;
    for change in changes {
        if change.table.is_empty() {
            return Err(RelationalRowPageLiveError::Corrupt(
                "live row change has an empty table name".to_string(),
            ));
        }
        let current = (change.table.as_str(), &change.primary_key);
        if previous.is_some_and(|previous| previous >= current) {
            return Err(RelationalRowPageLiveError::Corrupt(
                "live row changes are not strictly ordered by table and primary key".to_string(),
            ));
        }
        let bytes = estimated_row_change_encoding_bytes(change).ok_or_else(|| {
            RelationalRowPageLiveError::Admission(
                "live row change byte accounting overflow".to_string(),
            )
        })?;
        actual_bytes = actual_bytes.checked_add(bytes).ok_or_else(|| {
            RelationalRowPageLiveError::Admission(
                "live row capture byte accounting overflow".to_string(),
            )
        })?;
        let key_bytes = change.primary_key.0.iter().try_fold(
            change
                .primary_key
                .0
                .len()
                .checked_mul(std::mem::size_of::<RelationalValue>())
                .ok_or_else(|| {
                    RelationalRowPageLiveError::Admission(
                        "live row key allocation accounting overflow".to_string(),
                    )
                })?,
            |bytes, value| {
                bytes
                    .checked_add(value.estimated_payload_bytes())
                    .ok_or_else(|| {
                        RelationalRowPageLiveError::Admission(
                            "live row key payload accounting overflow".to_string(),
                        )
                    })
            },
        )?;
        let row_bytes = change.row.as_ref().map_or(Ok(0), |row| {
            row.values().iter().try_fold(
                row.values()
                    .len()
                    .checked_mul(std::mem::size_of::<RelationalValue>())
                    .ok_or_else(|| {
                        RelationalRowPageLiveError::Admission(
                            "live row value allocation accounting overflow".to_string(),
                        )
                    })?,
                |bytes, value| {
                    bytes
                        .checked_add(value.estimated_payload_bytes())
                        .ok_or_else(|| {
                            RelationalRowPageLiveError::Admission(
                                "live row value payload accounting overflow".to_string(),
                            )
                        })
                },
            )
        })?;
        resident_bytes = resident_bytes
            .checked_add(std::mem::size_of::<RelationalRowChange>())
            .and_then(|bytes| bytes.checked_add(change.table.len()))
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .and_then(|bytes| bytes.checked_add(row_bytes))
            .ok_or_else(|| {
                RelationalRowPageLiveError::Admission(
                    "live row resident byte accounting overflow".to_string(),
                )
            })?;
        previous = Some(current);
    }
    if actual_bytes != declared_bytes {
        return Err(RelationalRowPageLiveError::Corrupt(format!(
            "live row capture declares {declared_bytes} bytes but requires {actual_bytes}"
        )));
    }
    Ok(resident_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::{RelationalRow, RelationalValue};
    use std::num::NonZeroUsize;

    #[test]
    fn capture_validation_rejects_undercharged_and_unordered_changes() {
        let changes = vec![change(2, Some("two")), change(1, Some("one"))];
        assert!(matches!(
            validate_capture(&changes, 0),
            Err(RelationalRowPageLiveError::Corrupt(reason))
                if reason.contains("strictly ordered")
        ));

        let changes = vec![change(1, Some("one"))];
        assert!(matches!(
            validate_capture(&changes, 0),
            Err(RelationalRowPageLiveError::Corrupt(reason))
                if reason.contains("declares")
        ));
        let encoded_bytes = estimated_row_change_encoding_bytes(&changes[0]).unwrap();
        let resident_bytes = validate_capture(&changes, encoded_bytes).unwrap();
        assert!(resident_bytes > encoded_bytes);
        let limits = RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(encoded_bytes).unwrap(),
        };
        assert!(matches!(
            RelationalRowPageLiveOverlay::empty().append(2, capture(changes), limits),
            Err(RelationalRowPageLiveError::Admission(reason))
                if reason.contains("resident bytes")
        ));
    }

    #[test]
    fn overlay_admission_is_cumulative_and_atomic() {
        let limits = RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            max_bytes: NonZeroUsize::new(4096).unwrap(),
        };
        let first = capture(vec![change(1, Some("one"))]);
        let overlay = RelationalRowPageLiveOverlay::empty()
            .append(2, first, limits)
            .unwrap();
        let error = overlay
            .append(3, capture(vec![change(2, Some("two"))]), limits)
            .unwrap_err();
        assert!(matches!(error, RelationalRowPageLiveError::Admission(_)));
        assert_eq!(overlay.entry_count, 1);
        assert_eq!(overlay.batch_count, 1);
        assert_eq!(
            overlay.overlay_value("documents", &key(1)),
            Some(RelationalRowPageRecoveredValue::Present(row(1, "one")))
        );
    }

    fn capture(changes: Vec<RelationalRowChange>) -> RelationalRowChangeCapture {
        let encoded_bytes = changes
            .iter()
            .map(|change| estimated_row_change_encoding_bytes(change).unwrap())
            .sum();
        RelationalRowChangeCapture::Captured {
            changes,
            encoded_bytes,
        }
    }

    fn change(id: i64, body: Option<&str>) -> RelationalRowChange {
        RelationalRowChange {
            table: "documents".to_string(),
            primary_key: key(id),
            row: body.map(|body| row(id, body)),
        }
    }

    fn key(id: i64) -> RelationalKey {
        RelationalKey(vec![RelationalValue::BigInt(id)])
    }

    fn row(id: i64, body: &str) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::BigInt(id),
            RelationalValue::Text(body.to_string()),
        ])
    }
}
