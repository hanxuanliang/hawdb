//! Non-serving relational index-page shadow publication.
//!
//! The shadow is deliberately derived: canonical checkpoint success never
//! depends on it and SQL never reads it in this stage. A valid shadow is
//! generation/epoch fenced to the checkpoint that supplied its rows.

use super::{GraphStore, SkeinError};
use skein_integrity::{IntegrityHasher, Sha256Digest};
use skein_storage::{
    RelationalIndexChange, RelationalIndexChangeCapture, RelationalIndexChangeCaptureLimits,
    RelationalIndexChangeKind, RelationalIndexReadLimits, RelationalIndexReadReport,
    RelationalIndexRecoveryBuilder, RelationalIndexRecoveryConfig,
    RelationalIndexRecoveryReadReport, RelationalIndexRecoveryReader,
    RelationalIndexRecoveryReport, RelationalIndexShadowBuildReport, RelationalIndexShadowConfig,
    RelationalIndexShadowError, RelationalIndexShadowManifest, RelationalIndexShadowReader,
    RelationalIndexShadowWriter, RelationalKey, RelationalScalarType, RelationalTableSchema,
    RelationalTransaction, RelationalValue, RELATIONAL_INDEX_SHADOW_MANIFEST_FILE,
    RELATIONAL_PRIMARY_INDEX_NAME,
};
use std::{collections::BTreeSet, fmt, fs, num::NonZeroUsize, sync::Arc};

pub const RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL: &str =
    "skein-relational-index-view-qualification-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationalIndexQualificationProbeKind {
    Exact,
    LeadingPrefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexViewQualificationOptions {
    pub max_tables: NonZeroUsize,
    pub max_rows_per_table: NonZeroUsize,
    pub max_probes: NonZeroUsize,
    pub read_limits: RelationalIndexReadLimits,
}

impl Default for RelationalIndexViewQualificationOptions {
    fn default() -> Self {
        Self {
            max_tables: NonZeroUsize::new(64).expect("default table limit is non-zero"),
            max_rows_per_table: NonZeroUsize::new(8).expect("default row sample limit is non-zero"),
            max_probes: NonZeroUsize::new(512).expect("default probe limit is non-zero"),
            read_limits: RelationalIndexReadLimits::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalIndexReadViewBackendReport {
    Base(RelationalIndexReadReport),
    Recovered(RelationalIndexRecoveryReadReport),
}

impl RelationalIndexReadViewBackendReport {
    fn bytes_read(&self) -> Option<usize> {
        match self {
            Self::Base(report) => Some(report.bytes_read),
            Self::Recovered(report) => report.base.bytes_read.checked_add(report.delta_bytes_read),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexReadViewReport {
    pub backend: RelationalIndexReadViewBackendReport,
    pub live_batches_visited: usize,
    pub live_entries_visited: usize,
    pub live_entries_matched: usize,
    pub live_bytes_visited: usize,
    pub rows_visited: usize,
    pub stopped_early: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexQualificationProbeReport {
    pub ordinal: usize,
    pub table: String,
    pub index: String,
    pub kind: RelationalIndexQualificationProbeKind,
    pub candidate_rows: usize,
    pub oracle_rows: usize,
    pub candidate_digest: String,
    pub oracle_digest: String,
    pub matched: bool,
    pub read: RelationalIndexReadViewReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexViewQualificationReport {
    pub protocol: &'static str,
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub schema_digest: String,
    pub tables_discovered: usize,
    pub tables_sampled: usize,
    pub rows_sampled: usize,
    pub indexes_discovered: usize,
    pub indexes_probed: usize,
    pub probes: Vec<RelationalIndexQualificationProbeReport>,
    pub mismatches: usize,
    pub truncated: bool,
    pub ready: bool,
}

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

#[derive(Clone)]
enum RelationalIndexReadBackend {
    Base(Arc<RelationalIndexShadowReader>),
    Recovered(Arc<RelationalIndexRecoveryReader>),
}

#[derive(Debug)]
struct RelationalIndexLiveBatch {
    commit_epoch: u64,
    changes: Arc<[RelationalIndexChange]>,
    encoded_bytes: usize,
}

#[derive(Debug, Clone)]
struct RelationalIndexLiveOverlay {
    batches: Arc<Vec<Arc<RelationalIndexLiveBatch>>>,
    entry_count: usize,
    encoded_bytes: usize,
}

impl RelationalIndexLiveOverlay {
    fn empty() -> Self {
        Self {
            batches: Arc::new(Vec::new()),
            entry_count: 0,
            encoded_bytes: 0,
        }
    }

    fn append(
        &self,
        commit_epoch: u64,
        capture: RelationalIndexChangeCapture,
        limits: RelationalIndexChangeCaptureLimits,
    ) -> Result<Self, String> {
        let (changes, encoded_bytes) = match capture {
            RelationalIndexChangeCapture::Captured {
                changes,
                encoded_bytes,
            } => (changes, encoded_bytes),
            RelationalIndexChangeCapture::Invalidated { reason } => return Err(reason),
        };
        let entry_count = self
            .entry_count
            .checked_add(changes.len())
            .ok_or_else(|| "relational index live entry accounting overflow".to_string())?;
        let total_bytes = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| "relational index live byte accounting overflow".to_string())?;
        if entry_count > limits.max_entries.get() || total_bytes > limits.max_bytes.get() {
            return Err(format!(
                "relational index live overlay exceeds max_entries={} or max_bytes={}",
                limits.max_entries, limits.max_bytes
            ));
        }
        if changes.is_empty() {
            return Ok(self.clone());
        }
        let mut batches = Arc::clone(&self.batches);
        Arc::make_mut(&mut batches).push(Arc::new(RelationalIndexLiveBatch {
            commit_epoch,
            changes: Arc::from(changes),
            encoded_bytes,
        }));
        Ok(Self {
            batches,
            entry_count,
            encoded_bytes: total_bytes,
        })
    }

    fn batch_count(&self) -> usize {
        self.batches.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RelationalIndexQualificationProbe {
    table: String,
    index: String,
    kind: RelationalIndexQualificationProbeKind,
    key: RelationalKey,
}

enum RelationalIndexReadSelector<'a> {
    Exact(&'a RelationalKey),
    Prefix(&'a RelationalKey),
}

impl RelationalIndexReadSelector<'_> {
    fn matches(&self, key: &RelationalKey) -> bool {
        match self {
            Self::Exact(expected) => key == *expected,
            Self::Prefix(prefix) => key.0.starts_with(&prefix.0),
        }
    }
}

/// One immutable, generation-bound relational index view.
///
/// The outer `Arc` is cloned into [`GraphStore`] snapshots. The selected base
/// and recovery manifests therefore cannot drift underneath a pinned reader,
/// while a newer store publication can install another view independently.
pub(crate) struct RelationalIndexReadView {
    identity: RelationalIndexReadViewIdentity,
    backend: RelationalIndexReadBackend,
    live: RelationalIndexLiveOverlay,
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
            backend: RelationalIndexReadBackend::Base(Arc::new(reader)),
            live: RelationalIndexLiveOverlay::empty(),
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
            backend: RelationalIndexReadBackend::Recovered(Arc::new(reader)),
            live: RelationalIndexLiveOverlay::empty(),
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

    fn advance(
        &self,
        next_commit_epoch: u64,
        capture: Option<RelationalIndexChangeCapture>,
        limits: RelationalIndexChangeCaptureLimits,
    ) -> Result<Self, String> {
        let expected = self
            .identity
            .visible_commit_epoch
            .checked_add(1)
            .ok_or_else(|| "relational index read-view epoch overflow".to_string())?;
        if next_commit_epoch != expected {
            return Err(format!(
                "relational index read view expected commit epoch {expected}, got {next_commit_epoch}"
            ));
        }
        if self.is_poisoned() {
            return Err("relational index read view is poisoned".to_string());
        }
        let live = capture.map_or_else(
            || Ok(self.live.clone()),
            |capture| self.live.append(next_commit_epoch, capture, limits),
        )?;
        let mut identity = self.identity;
        identity.visible_commit_epoch = next_commit_epoch;
        Ok(Self {
            identity,
            backend: self.backend.clone(),
            live,
        })
    }

    fn live_batch_count(&self) -> usize {
        self.live.batch_count()
    }

    fn live_entry_count(&self) -> usize {
        self.live.entry_count
    }

    fn live_encoded_bytes(&self) -> usize {
        self.live.encoded_bytes
    }

    fn visit_exact_postings(
        &self,
        table: &str,
        index: &str,
        key: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.visit_postings(
            table,
            index,
            RelationalIndexReadSelector::Exact(key),
            limits,
            visit,
        )
    }

    fn visit_prefix_postings(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        self.visit_postings(
            table,
            index,
            RelationalIndexReadSelector::Prefix(prefix),
            limits,
            visit,
        )
    }

    fn visit_postings(
        &self,
        table: &str,
        index: &str,
        selector: RelationalIndexReadSelector<'_>,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&RelationalKey) -> bool,
    ) -> std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index read view is poisoned".to_string(),
            ));
        }
        let mut rows = BTreeSet::new();
        let mut collect = |primary_key: &RelationalKey| {
            rows.insert(primary_key.clone());
            true
        };
        let backend =
            match (&self.backend, &selector) {
                (
                    RelationalIndexReadBackend::Base(reader),
                    RelationalIndexReadSelector::Exact(key),
                ) => RelationalIndexReadViewBackendReport::Base(reader.visit_exact_postings(
                    table,
                    index,
                    key,
                    limits,
                    &mut collect,
                )?),
                (
                    RelationalIndexReadBackend::Base(reader),
                    RelationalIndexReadSelector::Prefix(prefix),
                ) => RelationalIndexReadViewBackendReport::Base(reader.visit_prefix_postings(
                    table,
                    index,
                    prefix,
                    limits,
                    &mut collect,
                )?),
                (
                    RelationalIndexReadBackend::Recovered(reader),
                    RelationalIndexReadSelector::Exact(key),
                ) => RelationalIndexReadViewBackendReport::Recovered(reader.visit_exact_postings(
                    table,
                    index,
                    key,
                    limits,
                    &mut collect,
                )?),
                (
                    RelationalIndexReadBackend::Recovered(reader),
                    RelationalIndexReadSelector::Prefix(prefix),
                ) => RelationalIndexReadViewBackendReport::Recovered(
                    reader.visit_prefix_postings(table, index, prefix, limits, &mut collect)?,
                ),
            };
        let mut live_entries_visited = 0usize;
        let mut live_entries_matched = 0usize;
        let mut live_bytes_visited = 0usize;
        let mut previous_epoch = self.durable_commit_epoch();
        for batch in self.live.batches.iter() {
            if batch.commit_epoch <= previous_epoch
                || batch.commit_epoch > self.identity.visible_commit_epoch
            {
                return Err(RelationalIndexShadowError::Corrupt(format!(
                    "relational index live batch epoch {} is outside ({previous_epoch}, {}]",
                    batch.commit_epoch, self.identity.visible_commit_epoch
                )));
            }
            previous_epoch = batch.commit_epoch;
            live_bytes_visited = live_bytes_visited
                .checked_add(batch.encoded_bytes)
                .ok_or_else(|| admission("relational index live byte counter overflow"))?;
            let total_bytes = backend
                .bytes_read()
                .ok_or_else(|| admission("relational index backend byte counter overflow"))?
                .checked_add(live_bytes_visited)
                .ok_or_else(|| admission("relational index read byte counter overflow"))?;
            if total_bytes > limits.max_bytes.get() {
                return Err(admission(format!(
                    "relational index read needs {total_bytes} bytes including live changes, exceeding byte limit {}",
                    limits.max_bytes
                )));
            }
            for change in batch.changes.iter() {
                live_entries_visited = live_entries_visited
                    .checked_add(1)
                    .ok_or_else(|| admission("relational index live entry counter overflow"))?;
                if change.table != table
                    || change.index != index
                    || !selector.matches(&change.index_key)
                {
                    continue;
                }
                live_entries_matched = live_entries_matched
                    .checked_add(1)
                    .ok_or_else(|| admission("relational index matched-live counter overflow"))?;
                match change.kind {
                    RelationalIndexChangeKind::Delete => {
                        rows.remove(&change.primary_key);
                    }
                    RelationalIndexChangeKind::Insert => {
                        rows.insert(change.primary_key.clone());
                    }
                }
                if rows.len() > limits.max_rows.get() {
                    return Err(admission(format!(
                        "relational index read exceeds row limit {} after live merge",
                        limits.max_rows
                    )));
                }
            }
        }
        let mut report = RelationalIndexReadViewReport {
            backend,
            live_batches_visited: self.live.batch_count(),
            live_entries_visited,
            live_entries_matched,
            live_bytes_visited,
            rows_visited: 0,
            stopped_early: false,
        };
        for row in rows {
            report.rows_visited += 1;
            if !visit(&row) {
                report.stopped_early = true;
                break;
            }
        }
        Ok(report)
    }

    fn durable_commit_epoch(&self) -> u64 {
        match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.manifest().source_commit_epoch,
            RelationalIndexReadBackend::Recovered(reader) => {
                reader.manifest().recovered_commit_epoch
            }
        }
    }

    fn base_manifest(&self) -> &RelationalIndexShadowManifest {
        match &self.backend {
            RelationalIndexReadBackend::Base(reader) => reader.manifest(),
            RelationalIndexReadBackend::Recovered(reader) => reader.base_manifest(),
        }
    }
}

fn admission(message: impl Into<String>) -> RelationalIndexShadowError {
    RelationalIndexShadowError::Admission(message.into())
}

fn qualification_probe_error(
    ordinal: usize,
    table: &str,
    index: &str,
    error: RelationalIndexShadowError,
) -> SkeinError {
    let context = format!(
        "relational index qualification probe {ordinal} on {table}.{index} failed: {error}"
    );
    match error {
        RelationalIndexShadowError::Corrupt(_) => SkeinError::StorageIntegrity(context),
        RelationalIndexShadowError::Admission(_)
        | RelationalIndexShadowError::Durability(_)
        | RelationalIndexShadowError::MissingIndex { .. }
        | RelationalIndexShadowError::StaleGeneration { .. } => SkeinError::Storage(context),
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

struct RelationalIndexDefinition {
    name: String,
    columns: Vec<String>,
    primary: bool,
}

fn relational_index_definitions(schema: &RelationalTableSchema) -> Vec<RelationalIndexDefinition> {
    let mut definitions =
        Vec::with_capacity(1 + schema.unique_constraints.len() + schema.indexes.len());
    definitions.push(RelationalIndexDefinition {
        name: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
        columns: schema.primary_key.clone(),
        primary: true,
    });
    definitions.extend(
        schema
            .unique_constraints
            .iter()
            .enumerate()
            .map(|(ordinal, columns)| RelationalIndexDefinition {
                name: format!("__unique_{ordinal}"),
                columns: columns.clone(),
                primary: false,
            }),
    );
    definitions.extend(
        schema
            .indexes
            .iter()
            .map(|index| RelationalIndexDefinition {
                name: index.name.clone(),
                columns: index.columns.clone(),
                primary: false,
            }),
    );
    definitions
}

fn relational_index_key(
    schema: &RelationalTableSchema,
    values: &[RelationalValue],
    columns: &[String],
) -> RelationalKey {
    RelationalKey(
        columns
            .iter()
            .map(|column| {
                let position = schema
                    .column_position(column)
                    .expect("validated relational index column");
                values[position].clone()
            })
            .collect(),
    )
}

fn push_qualification_probe(
    probes: &mut Vec<RelationalIndexQualificationProbe>,
    unique: &mut BTreeSet<RelationalIndexQualificationProbe>,
    probe: RelationalIndexQualificationProbe,
    max_probes: usize,
) -> bool {
    if unique.contains(&probe) {
        return true;
    }
    if probes.len() >= max_probes {
        return false;
    }
    unique.insert(probe.clone());
    probes.push(probe);
    true
}

fn relational_keys_digest(keys: &[RelationalKey]) -> String {
    let mut hasher = IntegrityHasher::new();
    hasher.update(b"skein-relational-index-qualification-keys-v1\0");
    hasher.update(&(keys.len() as u64).to_le_bytes());
    for key in keys {
        hasher.update(&(key.0.len() as u64).to_le_bytes());
        for value in &key.0 {
            hash_relational_value(&mut hasher, value);
        }
    }
    hasher.finish().sha256.to_string()
}

fn hash_relational_value(hasher: &mut IntegrityHasher, value: &RelationalValue) {
    match value {
        RelationalValue::Null => hasher.update(&[0]),
        RelationalValue::Boolean(value) => hasher.update(&[1, u8::from(*value)]),
        RelationalValue::BigInt(value) => {
            hasher.update(&[2]);
            hasher.update(&value.to_le_bytes());
        }
        RelationalValue::DoublePrecision(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_bits().to_le_bytes());
        }
        RelationalValue::Text(value) => {
            hasher.update(&[4]);
            hash_bounded_bytes(hasher, value.as_bytes());
        }
        RelationalValue::Bytea(value) => {
            hasher.update(&[5]);
            hash_bounded_bytes(hasher, value);
        }
        RelationalValue::Overflow(reference) => {
            hasher.update(&[6, relational_scalar_type_tag(reference.scalar_type)]);
            hash_bounded_bytes(hasher, reference.digest.as_bytes());
            hasher.update(&(reference.compressed_bytes as u64).to_le_bytes());
            hasher.update(&(reference.uncompressed_bytes as u64).to_le_bytes());
        }
    }
}

const fn relational_scalar_type_tag(scalar_type: RelationalScalarType) -> u8 {
    match scalar_type {
        RelationalScalarType::Boolean => 0,
        RelationalScalarType::BigInt => 1,
        RelationalScalarType::DoublePrecision => 2,
        RelationalScalarType::Text => 3,
        RelationalScalarType::Bytea => 4,
    }
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
    LiveCurrent {
        base_generation: u64,
        delta_generation: Option<u64>,
        base_commit_epoch: u64,
        visible_commit_epoch: u64,
        live_batches: usize,
        live_entries: usize,
        live_bytes: usize,
    },
    LiveUnavailable {
        base_generation: u64,
        base_commit_epoch: u64,
        last_visible_commit_epoch: u64,
        failed_commit_epoch: u64,
        reason: String,
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
    live_limits: RelationalIndexChangeCaptureLimits,
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

    fn stage_live_publication(
        &self,
        current_epoch: u64,
        next_epoch: u64,
        capture: Option<RelationalIndexChangeCapture>,
    ) -> Option<Result<Arc<RelationalIndexReadView>, RelationalIndexLiveUnavailable>> {
        let view = self.current_read_view(current_epoch)?;
        Some(
            view.advance(next_epoch, capture, self.live_limits)
                .map(Arc::new)
                .map_err(|reason| RelationalIndexLiveUnavailable {
                    identity: view.identity(),
                    failed_commit_epoch: next_epoch,
                    reason,
                }),
        )
    }
}

pub(super) struct RelationalIndexLiveUnavailable {
    identity: RelationalIndexReadViewIdentity,
    failed_commit_epoch: u64,
    reason: String,
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

    pub(super) fn relational_index_live_capture_limits(
        &self,
    ) -> Option<RelationalIndexChangeCaptureLimits> {
        self.relational_index_shadow
            .current_read_view(self.commit_epoch)
            .map(|_| self.relational_index_shadow.live_limits)
    }

    pub(super) fn stage_relational_index_live_publication(
        &self,
        next_epoch: u64,
        capture: Option<RelationalIndexChangeCapture>,
    ) -> Option<Result<Arc<RelationalIndexReadView>, RelationalIndexLiveUnavailable>> {
        self.relational_index_shadow
            .stage_live_publication(self.commit_epoch, next_epoch, capture)
    }

    pub(super) fn publish_relational_index_live_view(
        &mut self,
        publication: Option<Result<Arc<RelationalIndexReadView>, RelationalIndexLiveUnavailable>>,
    ) {
        match publication {
            None => {}
            Some(Ok(view)) => {
                let identity = view.identity();
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::LiveCurrent {
                        base_generation: identity.base_generation,
                        delta_generation: identity.delta_generation,
                        base_commit_epoch: identity.base_commit_epoch,
                        visible_commit_epoch: identity.visible_commit_epoch,
                        live_batches: view.live_batch_count(),
                        live_entries: view.live_entry_count(),
                        live_bytes: view.live_encoded_bytes(),
                    };
                self.relational_index_shadow.read_view = Some(view);
            }
            Some(Err(unavailable)) => {
                self.relational_index_shadow.read_view = None;
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::LiveUnavailable {
                        base_generation: unavailable.identity.base_generation,
                        base_commit_epoch: unavailable.identity.base_commit_epoch,
                        last_visible_commit_epoch: unavailable.identity.visible_commit_epoch,
                        failed_commit_epoch: unavailable.failed_commit_epoch,
                        reason: unavailable.reason,
                    };
            }
        }
    }

    /// Completes one already-durable non-relational commit.
    ///
    /// Callers invoke this only after applying the canonical graph or catalog
    /// mutation. Relational index contents do not change, but their immutable
    /// read view must advance to the same global commit epoch so a later
    /// snapshot cannot combine graph state with a stale relational identity.
    pub(super) fn finish_non_relational_commit(&mut self) {
        let next_commit_epoch = self.commit_epoch + 1;
        let publication = self.stage_relational_index_live_publication(next_commit_epoch, None);
        self.commit_epoch = next_commit_epoch;
        self.publish_relational_index_live_view(publication);
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

    /// Differentially checks the pinned demand-paged relational index view
    /// against the current materialized oracle without changing SQL routing.
    pub fn qualify_relational_index_read_view(
        &self,
        options: RelationalIndexViewQualificationOptions,
    ) -> crate::Result<RelationalIndexViewQualificationReport> {
        let view = self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "relational index read view is unavailable at commit epoch {}",
                    self.commit_epoch
                ))
            })?;
        let tables_discovered = self.relational_state.table_schemas().count();
        let indexes_discovered = self
            .relational_state
            .table_schemas()
            .map(|schema| relational_index_definitions(schema).len())
            .sum();
        let mut probes = Vec::new();
        let mut unique_probes = BTreeSet::new();
        let mut truncated = tables_discovered > options.max_tables.get();
        let tables_sampled = tables_discovered.min(options.max_tables.get());
        let mut rows_sampled = 0usize;
        'tables: for schema in self
            .relational_state
            .table_schemas()
            .take(options.max_tables.get())
        {
            let definitions = relational_index_definitions(schema);
            for (primary_key, row) in self
                .relational_state
                .rows(&schema.name)
                .take(options.max_rows_per_table.get())
            {
                rows_sampled = rows_sampled.checked_add(1).ok_or_else(|| {
                    SkeinError::Storage(
                        "relational index qualification row sample counter overflow".to_string(),
                    )
                })?;
                for definition in &definitions {
                    let key = if definition.primary {
                        primary_key.clone()
                    } else {
                        relational_index_key(schema, row.values(), &definition.columns)
                    };
                    if !push_qualification_probe(
                        &mut probes,
                        &mut unique_probes,
                        RelationalIndexQualificationProbe {
                            table: schema.name.clone(),
                            index: definition.name.clone(),
                            kind: RelationalIndexQualificationProbeKind::Exact,
                            key: key.clone(),
                        },
                        options.max_probes.get(),
                    ) {
                        truncated = true;
                        break 'tables;
                    }
                    if !definition.primary && definition.columns.len() > 1 {
                        for prefix_len in 1..definition.columns.len() {
                            if !push_qualification_probe(
                                &mut probes,
                                &mut unique_probes,
                                RelationalIndexQualificationProbe {
                                    table: schema.name.clone(),
                                    index: definition.name.clone(),
                                    kind: RelationalIndexQualificationProbeKind::LeadingPrefix,
                                    key: RelationalKey(key.0[..prefix_len].to_vec()),
                                },
                                options.max_probes.get(),
                            ) {
                                truncated = true;
                                break 'tables;
                            }
                        }
                    }
                }
            }
        }
        let indexes_probed = probes
            .iter()
            .map(|probe| (probe.table.as_str(), probe.index.as_str()))
            .collect::<BTreeSet<_>>()
            .len();
        let mut probe_reports = Vec::with_capacity(probes.len());
        let mut mismatches = 0usize;
        for (ordinal, probe) in probes.into_iter().enumerate() {
            let mut candidate = Vec::new();
            let read = match probe.kind {
                RelationalIndexQualificationProbeKind::Exact => view.visit_exact_postings(
                    &probe.table,
                    &probe.index,
                    &probe.key,
                    options.read_limits,
                    |primary_key| {
                        candidate.push(primary_key.clone());
                        true
                    },
                ),
                RelationalIndexQualificationProbeKind::LeadingPrefix => view.visit_prefix_postings(
                    &probe.table,
                    &probe.index,
                    &probe.key,
                    options.read_limits,
                    |primary_key| {
                        candidate.push(primary_key.clone());
                        true
                    },
                ),
            }
            .map_err(|error| {
                qualification_probe_error(ordinal, &probe.table, &probe.index, error)
            })?;
            candidate.sort();
            candidate.dedup();
            let oracle = self.relational_index_oracle_rows(&probe, options.read_limits)?;
            let matched = candidate == oracle;
            mismatches += usize::from(!matched);
            probe_reports.push(RelationalIndexQualificationProbeReport {
                ordinal,
                table: probe.table,
                index: probe.index,
                kind: probe.kind,
                candidate_rows: candidate.len(),
                oracle_rows: oracle.len(),
                candidate_digest: relational_keys_digest(&candidate),
                oracle_digest: relational_keys_digest(&oracle),
                matched,
                read,
            });
        }
        let identity = view.identity();
        let ready = mismatches == 0 && !truncated && indexes_probed == indexes_discovered;
        Ok(RelationalIndexViewQualificationReport {
            protocol: RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL,
            base_generation: identity.base_generation,
            delta_generation: identity.delta_generation,
            base_commit_epoch: identity.base_commit_epoch,
            visible_commit_epoch: identity.visible_commit_epoch,
            schema_digest: identity.schema_digest.to_string(),
            tables_discovered,
            tables_sampled,
            rows_sampled,
            indexes_discovered,
            indexes_probed,
            probes: probe_reports,
            mismatches,
            truncated,
            ready,
        })
    }

    fn relational_index_oracle_rows(
        &self,
        probe: &RelationalIndexQualificationProbe,
        limits: RelationalIndexReadLimits,
    ) -> crate::Result<Vec<RelationalKey>> {
        let mut rows = if probe.index == RELATIONAL_PRIMARY_INDEX_NAME {
            self.relational_state
                .row(&probe.table, &probe.key)
                .map(|_| vec![probe.key.clone()])
                .unwrap_or_default()
        } else {
            match probe.kind {
                RelationalIndexQualificationProbeKind::Exact => self
                    .relational_state
                    .index_prefix_lookup(
                        &probe.table,
                        &probe.index,
                        &probe.key,
                        limits.max_rows.get().saturating_add(1),
                    )
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "relational index qualification oracle is missing {}.{}",
                            probe.table, probe.index
                        ))
                    })?
                    .into_iter()
                    .cloned()
                    .collect(),
                RelationalIndexQualificationProbeKind::LeadingPrefix => self
                    .relational_state
                    .index_prefix_lookup(
                        &probe.table,
                        &probe.index,
                        &probe.key,
                        limits.max_rows.get().saturating_add(1),
                    )
                    .ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "relational index qualification oracle is missing {}.{}",
                            probe.table, probe.index
                        ))
                    })?
                    .into_iter()
                    .cloned()
                    .collect(),
            }
        };
        if rows.len() > limits.max_rows.get() {
            return Err(SkeinError::Storage(format!(
                "relational index qualification oracle exceeds row limit {}",
                limits.max_rows
            )));
        }
        rows.sort();
        rows.dedup();
        Ok(rows)
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
        DurabilityPolicy, RelationalColumnSchema, RelationalIndexChangeKind, RelationalIndexSchema,
        RelationalKey, RelationalScalarType, RelationalTableSchema, RelationalTransaction,
        RelationalValue, RelationalWrite, WalReplayConfig,
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
            let base_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify base relational index view");
            assert_qualification_ready(&base_qualification, 1, 4);
            assert!(base_qualification.probes.iter().any(|probe| {
                probe.kind == RelationalIndexQualificationProbeKind::LeadingPrefix
                    && matches!(
                        probe.read.backend,
                        RelationalIndexReadViewBackendReport::Base(_)
                    )
            }));
            let truncated = store
                .qualify_relational_index_read_view(RelationalIndexViewQualificationOptions {
                    max_probes: NonZeroUsize::new(1).unwrap(),
                    ..RelationalIndexViewQualificationOptions::default()
                })
                .expect("bound qualification probe count");
            assert!(truncated.truncated);
            assert!(!truncated.ready);
            assert_eq!(truncated.probes.len(), 1);
            let pinned = store.snapshot();
            let pinned_view = current_index_view(&pinned);
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
            let live_view = current_index_view(&store);
            assert_eq!(live_view.kind(), RelationalIndexReadViewKind::Base);
            assert_eq!(live_view.identity().visible_commit_epoch, 2);
            assert_eq!(live_view.live_batch_count(), 1);
            assert_eq!(live_view.live_entry_count(), 4);
            assert!(live_view.live_encoded_bytes() > 0);
            assert_eq!(pinned_view.identity().visible_commit_epoch, 1);
            assert_eq!(pinned_view.live_batch_count(), 0);
            assert!(!Arc::ptr_eq(pinned_view, live_view));
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::LiveCurrent {
                    visible_commit_epoch: 2,
                    live_batches: 1,
                    live_entries: 4,
                    ..
                }
            ));
            let inserted_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify relational index view with live insert");
            assert_qualification_ready(&inserted_qualification, 2, 4);
            assert!(inserted_qualification
                .probes
                .iter()
                .all(|probe| probe.read.live_entries_visited == 4));

            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::DeleteByPrimaryKey {
                            table: "documents".to_string(),
                            keys: vec![RelationalKey(vec![RelationalValue::Text(
                                "doc-1".to_string(),
                            )])],
                        }],
                    },
                )
                .expect("append relational delete after checkpoint");
            let deleted_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify relational index view with live delete");
            assert_qualification_ready(&deleted_qualification, 3, 4);
            assert!(deleted_qualification
                .probes
                .iter()
                .all(|probe| probe.read.live_entries_visited == 8));
            assert!(deleted_qualification.probes.iter().any(|probe| {
                probe.kind == RelationalIndexQualificationProbeKind::LeadingPrefix
                    && probe.candidate_rows == 1
                    && probe.oracle_rows == 1
            }));

            store
                .create_node(&mut catalog, "Document", Default::default())
                .expect("commit graph-only WAL after checkpoint");
            let graph_advanced_view = current_index_view(&store);
            assert_eq!(graph_advanced_view.identity().visible_commit_epoch, 4);
            assert_eq!(graph_advanced_view.live_batch_count(), 2);
            assert_eq!(graph_advanced_view.live_entry_count(), 8);
            let graph_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify graph-advanced relational index view");
            assert_qualification_ready(&graph_qualification, 4, 4);
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
            assert_eq!(store.relational_state().row_count("documents"), 1);
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::WalRecovered {
                    base_commit_epoch: 1,
                    recovered_commit_epoch: 4,
                    delta_pages: 1,
                    delta_entries: 8,
                    ..
                }
            ));
            let report = store
                .relational_index_recovery_report()
                .expect("recovery evidence report");
            assert_eq!(report.delta_entries, 8);
            assert!(report.peak_dirty_bytes > 0);
            let view = current_index_view(&store);
            assert_eq!(view.kind(), RelationalIndexReadViewKind::Recovered);
            assert!(view.identity().delta_generation.is_some());
            assert_eq!(view.identity().base_commit_epoch, 1);
            assert_eq!(view.identity().visible_commit_epoch, 4);
            let recovered_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify recovered relational index view");
            assert_qualification_ready(&recovered_qualification, 4, 4);
            assert!(recovered_qualification.probes.iter().all(|probe| {
                matches!(
                    probe.read.backend,
                    RelationalIndexReadViewBackendReport::Recovered(_)
                )
            }));
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
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::LiveUnavailable {
                    last_visible_commit_epoch: 1,
                    failed_commit_epoch: 2,
                    reason,
                    ..
                } if reason.contains("schema-changing WAL")
            ));
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

    #[test]
    fn live_relational_index_overlay_fails_closed_at_its_cumulative_budget() {
        let change = RelationalIndexChange {
            table: "documents".to_string(),
            index: "documents_owner_idx".to_string(),
            index_key: RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]),
            primary_key: RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]),
            kind: RelationalIndexChangeKind::Insert,
        };
        let limits = RelationalIndexChangeCaptureLimits {
            max_entries: std::num::NonZeroUsize::new(1).unwrap(),
            max_bytes: std::num::NonZeroUsize::new(1_024).unwrap(),
        };

        let result = RelationalIndexLiveOverlay::empty().append(
            1,
            RelationalIndexChangeCapture::Captured {
                changes: vec![change.clone(), change],
                encoded_bytes: 128,
            },
            limits,
        );

        assert!(matches!(
            result,
            Err(reason) if reason.contains("max_entries=1")
        ));
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
                    unique_constraints: vec![vec!["id".to_string()]],
                    foreign_keys: Vec::new(),
                    indexes: vec![
                        RelationalIndexSchema {
                            name: "documents_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        },
                        RelationalIndexSchema {
                            name: "documents_owner_id_idx".to_string(),
                            columns: vec!["owner".to_string(), "id".to_string()],
                            unique: false,
                        },
                    ],
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
            .unwrap_or_else(|| {
                panic!(
                    "store at epoch {} has no generation-pinned relational index view: {:?}",
                    store.commit_epoch,
                    store.relational_index_shadow_recovery_status()
                )
            })
    }

    fn assert_qualification_ready(
        report: &RelationalIndexViewQualificationReport,
        visible_commit_epoch: u64,
        indexes: usize,
    ) {
        assert_eq!(
            report.protocol,
            RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL
        );
        assert_eq!(report.visible_commit_epoch, visible_commit_epoch);
        assert_eq!(report.tables_discovered, 1);
        assert_eq!(report.tables_sampled, 1);
        assert!(report.rows_sampled > 0);
        assert_eq!(report.indexes_discovered, indexes);
        assert_eq!(report.indexes_probed, indexes);
        assert_eq!(report.mismatches, 0);
        assert!(!report.truncated);
        assert!(report.ready);
        assert!(!report.probes.is_empty());
        assert!(report.probes.iter().all(|probe| {
            probe.matched
                && probe.candidate_rows == probe.oracle_rows
                && probe.candidate_digest == probe.oracle_digest
        }));
    }
}
