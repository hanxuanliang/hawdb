use super::{
    codec, durability, relational_row_delta_manifest_generation_file,
    relational_row_delta_run_file, RelationalRowDeltaConfig, RelationalRowDeltaError,
    RelationalRowDeltaGeneration, RelationalRowDeltaManifest, RelationalRowDeltaReadReport,
    RowDeltaRunDescriptor, RELATIONAL_ROW_DELTA_MANIFEST_FILE,
};
use crate::relational::row_page::RelationalRowPageRecoveredValue;
use crate::relational::{
    ordered_key::{decode_ordered_relational_key, encode_ordered_relational_key},
    RelationalKey, RelationalOverflowRootReader, RelationalRow, RelationalRowPageRootReader,
    RelationalValue,
};
use skein_integrity::IntegrityHasher;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct RelationalRowDeltaReader {
    directory: PathBuf,
    manifest: Arc<RelationalRowDeltaManifest>,
    config: RelationalRowDeltaConfig,
    poisoned: AtomicBool,
}

impl RelationalRowDeltaReader {
    pub fn latest_generation(
        directory: &Path,
        config: RelationalRowDeltaConfig,
    ) -> Result<Option<RelationalRowDeltaGeneration>, RelationalRowDeltaError> {
        let path = directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE);
        Ok(codec::read_manifest_if_exists(&path, config)?.map(|manifest| manifest.generation()))
    }

    pub fn open_latest(
        directory: &Path,
        expected_base: &RelationalRowPageRootReader,
        expected_visible_commit_epoch: u64,
        config: RelationalRowDeltaConfig,
    ) -> Result<Option<Self>, RelationalRowDeltaError> {
        let path = directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE);
        codec::read_manifest_if_exists(&path, config)?
            .map(|manifest| {
                Self::from_manifest(
                    directory,
                    manifest,
                    expected_base,
                    expected_visible_commit_epoch,
                    config,
                )
            })
            .transpose()
    }

    pub fn open_generation(
        directory: &Path,
        generation: RelationalRowDeltaGeneration,
        expected_base: &RelationalRowPageRootReader,
        expected_visible_commit_epoch: u64,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        let path = directory.join(relational_row_delta_manifest_generation_file(
            generation.base_generation,
            generation.delta_generation,
        ));
        let manifest = codec::read_manifest(&path, config)?;
        if manifest.generation() != generation {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta generation manifest {generation:?} identifies {:?}",
                manifest.generation()
            )));
        }
        Self::from_manifest(
            directory,
            manifest,
            expected_base,
            expected_visible_commit_epoch,
            config,
        )
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalRowDeltaManifest,
        expected_base: &RelationalRowPageRootReader,
        expected_visible_commit_epoch: u64,
        config: RelationalRowDeltaConfig,
    ) -> Result<Self, RelationalRowDeltaError> {
        let base = expected_base.manifest();
        if manifest.base.generation != base.generation
            || manifest.base.source_commit_epoch != base.source_commit_epoch
            || manifest.base.root_set_digest != base.root_set_digest
            || manifest.visible_commit_epoch != expected_visible_commit_epoch
        {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta fence {}/{}/{} does not match base {}/{} and visible epoch {expected_visible_commit_epoch}",
                manifest.base.generation,
                manifest.base.source_commit_epoch,
                manifest.visible_commit_epoch,
                base.generation,
                base.source_commit_epoch
            )));
        }
        if manifest.tables.len() != base.tables.len()
            || manifest
                .tables
                .iter()
                .zip(&base.tables)
                .any(|(table, base_table)| {
                    table.table != base_table.table
                        || table.schema_digest != base_table.schema_digest
                })
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta table schemas do not match the selected row root".to_string(),
            ));
        }
        for run in &manifest.runs {
            codec::validate_artifact_length(
                &directory.join(relational_row_delta_run_file(
                    manifest.base.generation,
                    manifest.delta_generation,
                    run.ordinal,
                )),
                run.encoded_len,
            )?;
        }
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest: Arc::new(manifest),
            config,
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn manifest(&self) -> &RelationalRowDeltaManifest {
        &self.manifest
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub fn validate_overflow_root(
        &self,
        overflow_root: Option<&RelationalOverflowRootReader>,
    ) -> Result<(), RelationalRowDeltaError> {
        let actual = overflow_root.map(|reader| reader.manifest().binding());
        if actual != self.manifest.overflow_root {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta overflow binding {:?} differs from selected root {actual:?}",
                self.manifest.overflow_root
            )));
        }
        Ok(())
    }

    /// Visits the immutable runs in publication order using one bounded key
    /// and row buffer at a time.
    ///
    /// Callback effects are provisional until this method returns `Ok`. A
    /// callback that returns `false` stops after the emitted entry's binding
    /// and checksum have been verified, without reading the remainder of that
    /// run or recomputing its complete artifact digest.
    pub fn visit_entries(
        &self,
        visit: impl FnMut(&str, &RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
    ) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
        if self.is_poisoned() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta reader is poisoned".to_string(),
            ));
        }
        let result = visit_manifest_entries(&self.directory, &self.manifest, self.config, visit);
        if result.as_ref().is_err_and(should_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    /// Visits only entries in one ordered table/key range.
    ///
    /// Runs whose key bounds cannot intersect the requested range remain
    /// unopened. Within an intersecting run, keys before the lower bound are
    /// skipped and traversal stops once the upper bound is crossed. Each
    /// emitted entry is independently binding- and checksum-verified; bytes
    /// outside the requested suffix are deliberately not demand-verified.
    pub fn visit_range_entries(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
        mut visit: impl FnMut(&RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
    ) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
        if self.is_poisoned() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta reader is poisoned".to_string(),
            ));
        }
        let result = self.visit_range_entries_inner(table, lower, upper, &mut visit);
        if result.as_ref().is_err_and(should_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn visit_range_entries_inner(
        &self,
        table: &str,
        lower: Bound<&RelationalKey>,
        upper: Bound<&RelationalKey>,
        visit: &mut impl FnMut(&RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
    ) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
        let Ok(table_ordinal) = self
            .manifest
            .tables
            .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
        else {
            return Ok(RelationalRowDeltaReadReport::default());
        };
        let table_ordinal = u32::try_from(table_ordinal).map_err(|_| {
            RelationalRowDeltaError::Corrupt("row delta table ordinal does not fit u32".to_string())
        })?;
        let encoded_lower = encode_range_bound(lower, self.config)?;
        let encoded_upper = encode_range_bound(upper, self.config)?;
        if encoded_bounds_are_empty(encoded_lower.as_ref(), encoded_upper.as_ref()) {
            return Ok(RelationalRowDeltaReadReport::default());
        }

        let mut report = RelationalRowDeltaReadReport::default();
        for run in &self.manifest.runs {
            if !run_intersects_range(
                run,
                table_ordinal,
                encoded_lower.as_ref(),
                encoded_upper.as_ref(),
            ) {
                continue;
            }
            let mut callback_stopped = false;
            let _completed = visit_run(
                &self.directory,
                &self.manifest,
                run,
                self.config,
                |candidate_table, candidate_key, value, epoch| {
                    match candidate_table.cmp(table) {
                        std::cmp::Ordering::Less => return Ok(true),
                        std::cmp::Ordering::Greater => return Ok(false),
                        std::cmp::Ordering::Equal => {}
                    }
                    if key_precedes_lower(candidate_key, lower) {
                        return Ok(true);
                    }
                    if key_exceeds_upper(candidate_key, upper) {
                        return Ok(false);
                    }
                    report.entries_visited =
                        report.entries_visited.checked_add(1).ok_or_else(|| {
                            RelationalRowDeltaError::Admission(
                                "row delta range entry counter overflow".to_string(),
                            )
                        })?;
                    if !visit(candidate_key, value, epoch) {
                        callback_stopped = true;
                        return Ok(false);
                    }
                    Ok(true)
                },
            )?;
            report.runs_read = report.runs_read.checked_add(1).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta range run counter overflow".to_string(),
                )
            })?;
            report.bytes_read =
                report
                    .bytes_read
                    .checked_add(run.encoded_len)
                    .ok_or_else(|| {
                        RelationalRowDeltaError::Admission(
                            "row delta range byte counter overflow".to_string(),
                        )
                    })?;
            if callback_stopped {
                report.stopped_early = true;
                break;
            }
        }
        Ok(report)
    }

    /// Looks up the newest immutable recovery-delta value without materializing
    /// the complete delta generation. Runs are inspected newest first because
    /// a later run supersedes the same key in an earlier run.
    pub fn lookup(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<
        (
            Option<RelationalRowPageRecoveredValue>,
            RelationalRowDeltaReadReport,
        ),
        RelationalRowDeltaError,
    > {
        if self.is_poisoned() {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta reader is poisoned".to_string(),
            ));
        }
        let result = self.lookup_inner(table, primary_key);
        if result.as_ref().is_err_and(should_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn lookup_inner(
        &self,
        table: &str,
        primary_key: &RelationalKey,
    ) -> Result<
        (
            Option<RelationalRowPageRecoveredValue>,
            RelationalRowDeltaReadReport,
        ),
        RelationalRowDeltaError,
    > {
        let Ok(table_ordinal) = self
            .manifest
            .tables
            .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
        else {
            return Ok((None, RelationalRowDeltaReadReport::default()));
        };
        let table_ordinal = u32::try_from(table_ordinal).map_err(|_| {
            RelationalRowDeltaError::Corrupt("row delta table ordinal does not fit u32".to_string())
        })?;
        let encoded_key = encode_ordered_relational_key(primary_key).map_err(|error| {
            RelationalRowDeltaError::Admission(format!(
                "row delta lookup key cannot be encoded: {error}"
            ))
        })?;
        if encoded_key.len() > self.config.row_limits.max_key_bytes.get() {
            return Err(RelationalRowDeltaError::Admission(format!(
                "row delta lookup key contains {} bytes, exceeding limit {}",
                encoded_key.len(),
                self.config.row_limits.max_key_bytes
            )));
        }

        let target = (table_ordinal, encoded_key.as_slice());
        let mut report = RelationalRowDeltaReadReport::default();
        for run in self.manifest.runs.iter().rev() {
            let lower = (
                run.lower_bound.table_ordinal,
                run.lower_bound.encoded_primary_key.as_slice(),
            );
            let upper = (
                run.upper_bound.table_ordinal,
                run.upper_bound.encoded_primary_key.as_slice(),
            );
            if target < lower || target > upper {
                continue;
            }

            let mut found = None;
            let completed = visit_run(
                &self.directory,
                &self.manifest,
                run,
                self.config,
                |candidate_table, candidate_key, value, _| {
                    report.entries_visited =
                        report.entries_visited.checked_add(1).ok_or_else(|| {
                            RelationalRowDeltaError::Admission(
                                "row delta lookup entry counter overflow".to_string(),
                            )
                        })?;
                    match candidate_table
                        .cmp(table)
                        .then_with(|| candidate_key.cmp(primary_key))
                    {
                        std::cmp::Ordering::Less => Ok(true),
                        std::cmp::Ordering::Equal => {
                            found = Some(value.clone());
                            Ok(false)
                        }
                        std::cmp::Ordering::Greater => Ok(false),
                    }
                },
            )?;
            report.runs_read = report.runs_read.checked_add(1).ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta lookup run counter overflow".to_string(),
                )
            })?;
            report.bytes_read = report
                .bytes_read
                .checked_add(run.encoded_len)
                .and_then(|bytes| bytes.checked_add(run.descriptor_bytes))
                .ok_or_else(|| {
                    RelationalRowDeltaError::Admission(
                        "row delta lookup byte counter overflow".to_string(),
                    )
                })?;
            report.stopped_early |= !completed;
            if found.is_some() {
                return Ok((found, report));
            }
        }
        Ok((None, report))
    }
}

fn encode_range_bound(
    bound: Bound<&RelationalKey>,
    config: RelationalRowDeltaConfig,
) -> Result<Option<(Vec<u8>, bool)>, RelationalRowDeltaError> {
    let (key, inclusive) = match bound {
        Bound::Unbounded => return Ok(None),
        Bound::Included(key) => (key, true),
        Bound::Excluded(key) => (key, false),
    };
    let encoded = encode_ordered_relational_key(key).map_err(|error| {
        RelationalRowDeltaError::Admission(format!(
            "row delta range key cannot be encoded: {error}"
        ))
    })?;
    if encoded.len() > config.row_limits.max_key_bytes.get() {
        return Err(RelationalRowDeltaError::Admission(format!(
            "row delta range key contains {} bytes, exceeding limit {}",
            encoded.len(),
            config.row_limits.max_key_bytes
        )));
    }
    Ok(Some((encoded, inclusive)))
}

fn encoded_bounds_are_empty(
    lower: Option<&(Vec<u8>, bool)>,
    upper: Option<&(Vec<u8>, bool)>,
) -> bool {
    match (lower, upper) {
        (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
            lower > upper || (lower == upper && !(*lower_inclusive && *upper_inclusive))
        }
        _ => false,
    }
}

fn run_intersects_range(
    run: &RowDeltaRunDescriptor,
    table_ordinal: u32,
    lower: Option<&(Vec<u8>, bool)>,
    upper: Option<&(Vec<u8>, bool)>,
) -> bool {
    if run.upper_bound.table_ordinal < table_ordinal
        || run.lower_bound.table_ordinal > table_ordinal
    {
        return false;
    }
    if run.upper_bound.table_ordinal == table_ordinal
        && lower.is_some_and(|(key, inclusive)| {
            run.upper_bound.encoded_primary_key < *key
                || (run.upper_bound.encoded_primary_key == *key && !inclusive)
        })
    {
        return false;
    }
    if run.lower_bound.table_ordinal == table_ordinal
        && upper.is_some_and(|(key, inclusive)| {
            run.lower_bound.encoded_primary_key > *key
                || (run.lower_bound.encoded_primary_key == *key && !inclusive)
        })
    {
        return false;
    }
    true
}

fn key_precedes_lower(key: &RelationalKey, lower: Bound<&RelationalKey>) -> bool {
    match lower {
        Bound::Unbounded => false,
        Bound::Included(lower) => key < lower,
        Bound::Excluded(lower) => key <= lower,
    }
}

fn key_exceeds_upper(key: &RelationalKey, upper: Bound<&RelationalKey>) -> bool {
    match upper {
        Bound::Unbounded => false,
        Bound::Included(upper) => key > upper,
        Bound::Excluded(upper) => key >= upper,
    }
}

pub(super) fn validate_candidate_overflow_closure(
    directory: &Path,
    manifest: &RelationalRowDeltaManifest,
    config: RelationalRowDeltaConfig,
    overflow_root: Option<&RelationalOverflowRootReader>,
) -> Result<(), RelationalRowDeltaError> {
    let expected_binding = overflow_root.map(|reader| reader.manifest().binding());
    if manifest.overflow_root != expected_binding {
        return Err(RelationalRowDeltaError::Admission(
            "row delta candidate has a mismatched overflow binding".to_string(),
        ));
    }
    let mut closure_error = None;
    let report = visit_manifest_entries(directory, manifest, config, |_, _, value, _| {
        let RelationalRowPageRecoveredValue::Present(row) = value else {
            return true;
        };
        for reference in row.values().iter().filter_map(|value| match value {
            RelationalValue::Overflow(reference) => Some(reference),
            _ => None,
        }) {
            let Some(root) = overflow_root else {
                continue;
            };
            match root.contains(reference) {
                Ok(true) => {}
                Ok(false) => {
                    closure_error = Some(RelationalRowDeltaError::Admission(format!(
                        "row delta references missing overflow extent {}",
                        reference.digest
                    )));
                    return false;
                }
                Err(error) => {
                    closure_error = Some(match error {
                        crate::relational::RelationalOverflowPublicationError::Admission(
                            message,
                        ) => RelationalRowDeltaError::Admission(message),
                        crate::relational::RelationalOverflowPublicationError::Corrupt(message) => {
                            RelationalRowDeltaError::Corrupt(message)
                        }
                        crate::relational::RelationalOverflowPublicationError::Durability(
                            message,
                        ) => RelationalRowDeltaError::Durability(message),
                        crate::relational::RelationalOverflowPublicationError::MissingExtent(
                            digest,
                        ) => RelationalRowDeltaError::Admission(format!(
                            "row delta references missing overflow extent {digest}"
                        )),
                        crate::relational::RelationalOverflowPublicationError::StaleGeneration {
                            expected_previous,
                            actual_previous,
                        } => RelationalRowDeltaError::Admission(format!(
                            "overflow root changed: expected {expected_previous:?}, found {actual_previous:?}"
                        )),
                    });
                    return false;
                }
            }
        }
        true
    })?;
    if let Some(error) = closure_error {
        return Err(error);
    }
    if report.stopped_early {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta overflow validation stopped without an error".to_string(),
        ));
    }
    Ok(())
}

fn visit_manifest_entries(
    directory: &Path,
    manifest: &RelationalRowDeltaManifest,
    config: RelationalRowDeltaConfig,
    mut visit: impl FnMut(&str, &RelationalKey, &RelationalRowPageRecoveredValue, u64) -> bool,
) -> Result<RelationalRowDeltaReadReport, RelationalRowDeltaError> {
    let mut report = RelationalRowDeltaReadReport::default();
    for run in &manifest.runs {
        let completed = visit_run(
            directory,
            manifest,
            run,
            config,
            |table, key, value, epoch| {
                report.entries_visited =
                    report.entries_visited.checked_add(1).ok_or_else(|| {
                        RelationalRowDeltaError::Admission(
                            "row delta read entry counter overflow".to_string(),
                        )
                    })?;
                Ok(visit(table, key, value, epoch))
            },
        )?;
        report.runs_read += 1;
        report.bytes_read = report
            .bytes_read
            .checked_add(run.encoded_len)
            .and_then(|bytes| bytes.checked_add(run.descriptor_bytes))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission(
                    "row delta read byte counter overflow".to_string(),
                )
            })?;
        if !completed {
            report.stopped_early = true;
            break;
        }
    }
    Ok(report)
}

fn visit_run(
    directory: &Path,
    manifest: &RelationalRowDeltaManifest,
    run: &RowDeltaRunDescriptor,
    config: RelationalRowDeltaConfig,
    mut visit: impl FnMut(
        &str,
        &RelationalKey,
        &RelationalRowPageRecoveredValue,
        u64,
    ) -> Result<bool, RelationalRowDeltaError>,
) -> Result<bool, RelationalRowDeltaError> {
    let path = directory.join(relational_row_delta_run_file(
        manifest.base.generation,
        manifest.delta_generation,
        run.ordinal,
    ));
    codec::validate_artifact_length(&path, run.encoded_len)?;
    let mut descriptor_file = File::open(&path).map_err(durability("open row delta run"))?;
    let mut header = [0u8; codec::RUN_HEADER_BYTES];
    descriptor_file
        .read_exact(&mut header)
        .map_err(durability("read row delta run header"))?;
    let decoded_header = codec::decode_run_header(&header, manifest, run)?;
    let mut content_hasher = IntegrityHasher::new();
    content_hasher.update(codec::run_integrity_prefix(&header));
    let mut artifact_hasher = IntegrityHasher::new();
    artifact_hasher.update(&header);
    let mut descriptor = [0u8; codec::ENTRY_DESCRIPTOR_BYTES];
    for _ in 0..run.entry_count {
        descriptor_file
            .read_exact(&mut descriptor)
            .map_err(durability("read row delta entry descriptor"))?;
        codec::decode_entry_descriptor(&descriptor)?;
        content_hasher.update(&descriptor);
        artifact_hasher.update(&descriptor);
    }

    descriptor_file
        .seek(SeekFrom::Start(codec::RUN_HEADER_BYTES as u64))
        .map_err(durability("seek row delta descriptor directory"))?;
    let payload_start = (codec::RUN_HEADER_BYTES as u64)
        .checked_add(run.descriptor_bytes)
        .ok_or_else(|| {
            RelationalRowDeltaError::Corrupt("row delta payload offset overflow".to_string())
        })?;
    let mut payload_file = File::open(&path).map_err(durability("open row delta payload"))?;
    payload_file
        .seek(SeekFrom::Start(payload_start))
        .map_err(durability("seek row delta payload"))?;
    let mut expected_payload_offset = 0u64;
    let mut previous_key: Option<(u32, Vec<u8>)> = None;
    for entry_ordinal in 0..run.entry_count {
        descriptor_file
            .read_exact(&mut descriptor)
            .map_err(durability("read row delta entry descriptor"))?;
        let decoded = codec::decode_entry_descriptor(&descriptor)?;
        if decoded.table_ordinal as usize >= manifest.tables.len()
            || decoded.last_modified_epoch <= manifest.base.source_commit_epoch
            || decoded.last_modified_epoch < run.start_epoch
            || decoded.last_modified_epoch > run.end_epoch
            || decoded.key_offset != expected_payload_offset
            || decoded.row_offset
                != decoded
                    .key_offset
                    .checked_add(decoded.key_len as u64)
                    .ok_or_else(|| {
                        RelationalRowDeltaError::Corrupt(
                            "row delta row offset overflow".to_string(),
                        )
                    })?
            || decoded.key_len == 0
            || decoded.key_len as usize > config.row_limits.max_key_bytes.get()
            || decoded.row_len as usize > config.row_limits.max_row_bytes.get()
            || (decoded.kind == 0) != (decoded.row_len == 0)
        {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "invalid row delta entry descriptor at ordinal {entry_ordinal}"
            )));
        }
        let mut encoded_key = vec![0u8; decoded.key_len as usize];
        let mut encoded_row = vec![0u8; decoded.row_len as usize];
        payload_file
            .read_exact(&mut encoded_key)
            .map_err(durability("read row delta primary key"))?;
        payload_file
            .read_exact(&mut encoded_row)
            .map_err(durability("read row delta row"))?;
        expected_payload_offset = decoded
            .row_offset
            .checked_add(decoded.row_len as u64)
            .ok_or_else(|| {
                RelationalRowDeltaError::Corrupt("row delta payload range overflow".to_string())
            })?;
        content_hasher.update(&encoded_key);
        content_hasher.update(&encoded_row);
        artifact_hasher.update(&encoded_key);
        artifact_hasher.update(&encoded_row);
        let mut entry_hasher = IntegrityHasher::new();
        entry_hasher.update(&encoded_key);
        entry_hasher.update(&encoded_row);
        let entry_crc32c = entry_hasher.finish().crc32c.get();
        let expected_binding = codec::entry_binding(
            manifest.base,
            manifest.delta_generation,
            run.ordinal,
            entry_ordinal,
            &descriptor[..48],
            &encoded_key,
            &encoded_row,
        );
        if entry_crc32c != decoded.entry_crc32c || expected_binding != decoded.binding {
            return Err(RelationalRowDeltaError::Corrupt(format!(
                "row delta entry {entry_ordinal} checksum or binding mismatch"
            )));
        }
        if previous_key.as_ref().is_some_and(|(table, key)| {
            *table > decoded.table_ordinal
                || (*table == decoded.table_ordinal && key >= &encoded_key)
        }) {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta entries are not strictly ordered".to_string(),
            ));
        }
        if entry_ordinal == 0
            && (decoded.table_ordinal != run.lower_bound.table_ordinal
                || encoded_key != run.lower_bound.encoded_primary_key)
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta lower key bound mismatch".to_string(),
            ));
        }
        if entry_ordinal + 1 == run.entry_count
            && (decoded.table_ordinal != run.upper_bound.table_ordinal
                || encoded_key != run.upper_bound.encoded_primary_key)
        {
            return Err(RelationalRowDeltaError::Corrupt(
                "row delta upper key bound mismatch".to_string(),
            ));
        }

        let primary_key = decode_ordered_relational_key(&encoded_key).map_err(|error| {
            RelationalRowDeltaError::Corrupt(format!(
                "row delta primary key cannot be decoded: {error}"
            ))
        })?;
        let value = decode_value(
            decoded.kind,
            &encoded_row,
            &manifest.tables[decoded.table_ordinal as usize],
            config,
        )?;
        previous_key = Some((decoded.table_ordinal, encoded_key));
        if !visit(
            &manifest.tables[decoded.table_ordinal as usize].table,
            &primary_key,
            &value,
            decoded.last_modified_epoch,
        )? {
            return Ok(false);
        }
    }
    if expected_payload_offset != run.payload_bytes {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta payload coverage mismatch".to_string(),
        ));
    }
    if content_hasher.finish() != decoded_header.content_digest {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta run content checksum mismatch".to_string(),
        ));
    }
    if artifact_hasher.finish() != run.digest {
        return Err(RelationalRowDeltaError::Corrupt(
            "row delta run artifact checksum mismatch".to_string(),
        ));
    }
    Ok(true)
}

fn decode_value(
    kind: u8,
    encoded_row: &[u8],
    table: &super::RelationalRowDeltaTableMetadata,
    config: RelationalRowDeltaConfig,
) -> Result<RelationalRowPageRecoveredValue, RelationalRowDeltaError> {
    if kind == 0 {
        return Ok(RelationalRowPageRecoveredValue::Deleted);
    }
    let fields = super::super::value::decode_row_fields(
        encoded_row,
        table.column_count.get() as usize,
        None,
        config.row_limits,
    )?;
    let values = fields.into_iter().map(|field| field.value).collect();
    Ok(RelationalRowPageRecoveredValue::Present(
        RelationalRow::new(values),
    ))
}

fn should_poison(error: &RelationalRowDeltaError) -> bool {
    matches!(
        error,
        RelationalRowDeltaError::Corrupt(_) | RelationalRowDeltaError::Durability(_)
    )
}
