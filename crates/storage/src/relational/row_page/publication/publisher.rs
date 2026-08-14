use super::{
    durability, manifest, relational_row_page_artifact_file,
    relational_row_page_manifest_generation_file, relational_row_page_root_descriptor_file,
    relational_row_page_root_key_file, root, RelationalRowPagePublicationConfig,
    RelationalRowPagePublicationError, RelationalRowPagePublicationPhase,
    RelationalRowPagePublicationReport, RelationalRowPageRootDescriptor,
    RelationalRowPageRootManifest, RelationalRowPageRootReader, RelationalRowPageTableDelta,
    COMPLETE_PUBLICATION_TRACE, RELATIONAL_ROW_PAGE_MANIFEST_FILE,
    RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE,
};
use crate::relational::RelationalValue;
use crate::{durable_replace_file, sync_directory};
use fs2::FileExt;
use skein_integrity::Sha256Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct RelationalRowPagePublisher {
    config: RelationalRowPagePublicationConfig,
}

impl RelationalRowPagePublisher {
    pub const fn new(config: RelationalRowPagePublicationConfig) -> Self {
        Self { config }
    }

    pub fn publish(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        deltas: Vec<RelationalRowPageTableDelta>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        self.publish_inner(
            directory,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            deltas,
            None,
        )
    }

    pub(super) fn publish_inner(
        &self,
        directory: &Path,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        deltas: Vec<RelationalRowPageTableDelta>,
        stop_after: Option<RelationalRowPagePublicationPhase>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        validate_publication_identity(generation, source_commit_epoch)?;
        let mut deltas = preflight_deltas(deltas, generation, source_commit_epoch, self.config)?;
        fs::create_dir_all(directory).map_err(durability("create row-page directory"))?;
        let _lock = acquire_publication_lock(directory)?;
        let paths = PublicationPaths::new(directory, generation);
        paths.remove_temps()?;
        paths.require_fresh_generation()?;

        let base = RelationalRowPageRootReader::open_latest(directory, self.config)?;
        let actual_previous = base.as_ref().map(|reader| reader.manifest.generation);
        if actual_previous != expected_previous_generation {
            return Err(RelationalRowPagePublicationError::StaleGeneration {
                expected_previous: expected_previous_generation,
                actual_previous,
            });
        }
        if let Some(base) = &base {
            if generation <= base.manifest.generation {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "new row-page generation {generation} must exceed published generation {}",
                    base.manifest.generation
                )));
            }
            if source_commit_epoch < base.manifest.source_commit_epoch {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "row-page source epoch {source_commit_epoch} precedes published epoch {}",
                    base.manifest.source_commit_epoch
                )));
            }
        }
        preflight_root_resources(base.as_ref(), &deltas, self.config)?;

        let result = self.build_and_publish(PublicationBuild {
            paths: &paths,
            base: base.as_ref(),
            generation,
            source_commit_epoch,
            expected_previous_generation,
            deltas: &mut deltas,
            stop_after,
        });
        let _ = paths.remove_temps();
        result
    }

    fn build_and_publish(
        &self,
        build: PublicationBuild<'_>,
    ) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidateStarted,
        )?;

        let (page_artifact, dirty_page_count) =
            root::write_dirty_page_artifact(&build.paths.page_tmp, build.deltas, self.config)?;
        let root = root::write_root_artifacts(
            &build.paths.descriptor_tmp,
            &build.paths.key_tmp,
            build.base,
            build.deltas,
            build.generation,
            build.source_commit_epoch,
            self.config,
        )?;
        let manifest = RelationalRowPageRootManifest {
            generation: build.generation,
            source_commit_epoch: build.source_commit_epoch,
            previous_generation: build.expected_previous_generation,
            page_bytes: self.config.page_limits.max_page_bytes.get() as u64,
            dirty_page_count,
            root_page_count: root.root_page_count,
            page_artifact,
            root_descriptor_artifact: root.descriptor_artifact,
            root_key_artifact: root.key_artifact,
            root_set_digest: manifest::root_set_digest(&root.tables)?,
            tables: root.tables,
        };
        let encoded_manifest = manifest::encode_manifest(&manifest, self.config)?;
        write_synced(&build.paths.generation_manifest_tmp, &encoded_manifest)?;

        durable_publish_immutable(&build.paths.page_tmp, &build.paths.page)?;
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidatePagesDurable,
        )?;
        durable_publish_immutable(&build.paths.descriptor_tmp, &build.paths.descriptor)?;
        durable_publish_immutable(&build.paths.key_tmp, &build.paths.key)?;
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidateRootDurable,
        )?;
        durable_publish_immutable(
            &build.paths.generation_manifest_tmp,
            &build.paths.generation_manifest,
        )?;
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::CandidateManifestDurable,
        )?;

        let actual_previous =
            manifest::read_manifest_if_exists(&build.paths.latest_manifest, self.config)?
                .map(|manifest| manifest.generation);
        if actual_previous != build.expected_previous_generation {
            return Err(RelationalRowPagePublicationError::StaleGeneration {
                expected_previous: build.expected_previous_generation,
                actual_previous,
            });
        }
        maybe_stop(
            build.stop_after,
            RelationalRowPagePublicationPhase::BaseRevalidated,
        )?;
        write_synced(&build.paths.latest_manifest_tmp, &encoded_manifest)?;
        durable_replace_file(
            &build.paths.latest_manifest_tmp,
            &build.paths.latest_manifest,
        )
        .map_err(durability("publish latest row-page manifest"))?;

        Ok(RelationalRowPagePublicationReport {
            generation: build.generation,
            source_commit_epoch: build.source_commit_epoch,
            dirty_pages_written: dirty_page_count,
            root_pages: manifest.root_page_count,
            reused_pages: root.reused_page_count,
            page_artifact_bytes: manifest.page_artifact.encoded_len,
            root_descriptor_bytes: manifest.root_descriptor_artifact.encoded_len,
            root_key_bytes: manifest.root_key_artifact.encoded_len,
            manifest_bytes: encoded_manifest.len() as u64,
            events: COMPLETE_PUBLICATION_TRACE,
        })
    }
}

struct PublicationBuild<'a> {
    paths: &'a PublicationPaths,
    base: Option<&'a RelationalRowPageRootReader>,
    generation: u64,
    source_commit_epoch: u64,
    expected_previous_generation: Option<u64>,
    deltas: &'a mut BTreeMap<String, PreparedTableDelta>,
    stop_after: Option<RelationalRowPagePublicationPhase>,
}

#[derive(Debug)]
pub(super) struct PreparedDirtyPage {
    pub page: super::ImmutableRelationalRowPage,
    pub descriptor: RelationalRowPageRootDescriptor,
}

#[derive(Debug)]
pub(super) struct PreparedTableDelta {
    pub table: String,
    pub schema_digest: Sha256Digest,
    pub dirty_pages: Vec<PreparedDirtyPage>,
    pub deleted_page_ids: BTreeSet<super::RelationalRowPageId>,
}

fn preflight_deltas(
    deltas: Vec<RelationalRowPageTableDelta>,
    generation: u64,
    source_commit_epoch: u64,
    config: RelationalRowPagePublicationConfig,
) -> Result<BTreeMap<String, PreparedTableDelta>, RelationalRowPagePublicationError> {
    let dirty_page_count = deltas.iter().try_fold(0usize, |count, delta| {
        count.checked_add(delta.dirty_pages.len()).ok_or_else(|| {
            RelationalRowPagePublicationError::Admission("dirty page count overflow".to_string())
        })
    })?;
    if dirty_page_count > config.max_dirty_pages.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "publication contains {dirty_page_count} dirty pages, exceeding limit {}",
            config.max_dirty_pages
        )));
    }
    let admitted_dirty_bytes = (dirty_page_count as u64)
        .checked_mul(config.page_limits.max_page_bytes.get() as u64)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "dirty page byte count overflow".to_string(),
            )
        })?;
    if admitted_dirty_bytes > config.max_dirty_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "publication reserves {admitted_dirty_bytes} dirty bytes, exceeding limit {}",
            config.max_dirty_bytes
        )));
    }
    if deltas.len() > config.max_tables.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "publication contains {} table deltas, exceeding limit {}",
            deltas.len(),
            config.max_tables
        )));
    }

    let mut prepared = BTreeMap::new();
    for delta in deltas {
        validate_table_name(&delta.table, config)?;
        if prepared.contains_key(&delta.table) {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "publication contains duplicate table delta {}",
                delta.table
            )));
        }
        let deleted_count = delta.deleted_page_ids.len();
        let deleted_page_ids = delta.deleted_page_ids.into_iter().collect::<BTreeSet<_>>();
        if deleted_page_ids.len() != deleted_count {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "table {} contains duplicate deleted page ids",
                delta.table
            )));
        }
        let mut seen_page_ids = BTreeSet::new();
        let mut dirty_pages = Vec::with_capacity(delta.dirty_pages.len());
        for page in delta.dirty_pages {
            if page.generation != generation || page.source_commit_epoch != source_commit_epoch {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "dirty page {} identifies generation/epoch {}/{}, expected {generation}/{source_commit_epoch}",
                    page.page_id.get(), page.generation, page.source_commit_epoch
                )));
            }
            if page.schema_digest != delta.schema_digest {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "dirty page {} schema digest differs from table {}",
                    page.page_id.get(),
                    delta.table
                )));
            }
            if !seen_page_ids.insert(page.page_id) {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "table {} contains duplicate dirty page id {}",
                    delta.table,
                    page.page_id.get()
                )));
            }
            if deleted_page_ids.contains(&page.page_id) {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "table {} both replaces and deletes page {}",
                    delta.table,
                    page.page_id.get()
                )));
            }
            if page.rows.iter().any(|entry| {
                entry
                    .row
                    .values()
                    .iter()
                    .any(|value| matches!(value, RelationalValue::Overflow(_)))
            }) {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "table {} page {} references an overflow extent before overflow publication is active",
                    delta.table,
                    page.page_id.get()
                )));
            }
            dirty_pages.push(root::prepare_dirty_page(page, config.page_limits)?);
        }
        dirty_pages.sort_by(|left, right| {
            left.descriptor
                .lower_bound
                .cmp(&right.descriptor.lower_bound)
        });
        if dirty_pages.windows(2).any(|pair| {
            pair[0].descriptor.upper_bound.as_slice() >= pair[1].descriptor.lower_bound.as_slice()
        }) {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "table {} dirty page bounds overlap or are unordered",
                delta.table
            )));
        }
        prepared.insert(
            delta.table.clone(),
            PreparedTableDelta {
                table: delta.table,
                schema_digest: delta.schema_digest,
                dirty_pages,
                deleted_page_ids,
            },
        );
    }
    Ok(prepared)
}

fn validate_publication_identity(
    generation: u64,
    source_commit_epoch: u64,
) -> Result<(), RelationalRowPagePublicationError> {
    if generation == 0 || source_commit_epoch == 0 {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page generation and source epoch must be non-zero, got {generation}/{source_commit_epoch}"
        )));
    }
    Ok(())
}

fn preflight_root_resources(
    base: Option<&RelationalRowPageRootReader>,
    deltas: &BTreeMap<String, PreparedTableDelta>,
    config: RelationalRowPagePublicationConfig,
) -> Result<(), RelationalRowPagePublicationError> {
    let base_page_count = base.map_or(0, |reader| reader.manifest.root_page_count);
    let dirty_page_count = deltas.values().try_fold(0u64, |count, delta| {
        count
            .checked_add(delta.dirty_pages.len() as u64)
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "row-page root pre-admission count overflow".to_string(),
                )
            })
    })?;
    let root_page_upper_bound = base_page_count
        .checked_add(dirty_page_count)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row-page root pre-admission count overflow".to_string(),
            )
        })?;
    if root_page_upper_bound > config.max_root_pages.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page root may contain {root_page_upper_bound} pages, exceeding limit {}",
            config.max_root_pages
        )));
    }

    let base_key_bytes = base.map_or(0, |reader| reader.manifest.root_key_artifact.encoded_len);
    let dirty_key_bytes = deltas.values().try_fold(0u64, |table_bytes, delta| {
        delta
            .dirty_pages
            .iter()
            .try_fold(table_bytes, |bytes, page| {
                bytes
                    .checked_add(page.descriptor.lower_bound.len() as u64)
                    .and_then(|bytes| bytes.checked_add(page.descriptor.upper_bound.len() as u64))
                    .ok_or_else(|| {
                        RelationalRowPagePublicationError::Admission(
                            "row-page root key pre-admission overflow".to_string(),
                        )
                    })
            })
    })?;
    let root_key_upper_bound = base_key_bytes.checked_add(dirty_key_bytes).ok_or_else(|| {
        RelationalRowPagePublicationError::Admission(
            "row-page root key pre-admission overflow".to_string(),
        )
    })?;
    if root_key_upper_bound > config.max_root_key_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page root may contain {root_key_upper_bound} key bytes, exceeding limit {}",
            config.max_root_key_bytes
        )));
    }

    let mut table_names = BTreeSet::new();
    if let Some(base) = base {
        table_names.extend(base.manifest.tables.iter().map(|table| table.table.clone()));
    }
    table_names.extend(deltas.keys().cloned());
    if table_names.len() > config.max_tables.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page root may contain {} tables, exceeding limit {}",
            table_names.len(),
            config.max_tables
        )));
    }
    let mut manifest_upper_bound = manifest::MANIFEST_HEADER_BYTES;
    for table_name in table_names {
        let base_table = base.and_then(|reader| {
            reader
                .manifest
                .tables
                .binary_search_by(|table| table.table.cmp(&table_name))
                .ok()
                .map(|index| &reader.manifest.tables[index])
        });
        let delta = deltas.get(&table_name);
        let mut lower_len = base_table.map_or(0, |table| table.lower_bound.len());
        let mut upper_len = base_table.map_or(0, |table| table.upper_bound.len());
        if let Some(delta) = delta {
            for page in &delta.dirty_pages {
                lower_len = lower_len.max(page.descriptor.lower_bound.len());
                upper_len = upper_len.max(page.descriptor.upper_bound.len());
            }
        }
        manifest_upper_bound = manifest_upper_bound
            .checked_add(60)
            .and_then(|bytes| bytes.checked_add(table_name.len()))
            .and_then(|bytes| bytes.checked_add(lower_len))
            .and_then(|bytes| bytes.checked_add(upper_len))
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "row-page manifest pre-admission overflow".to_string(),
                )
            })?;
    }
    if manifest_upper_bound > config.max_manifest_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest may contain {manifest_upper_bound} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    Ok(())
}

fn validate_table_name(
    table: &str,
    config: RelationalRowPagePublicationConfig,
) -> Result<(), RelationalRowPagePublicationError> {
    if table.is_empty() || table.len() > config.max_table_name_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "table name contains {} bytes, outside admitted range 1..={}",
            table.len(),
            config.max_table_name_bytes
        )));
    }
    Ok(())
}

fn acquire_publication_lock(directory: &Path) -> Result<File, RelationalRowPagePublicationError> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE))
        .map_err(durability("open row-page publication lock"))?;
    lock.lock_exclusive()
        .map_err(durability("lock row-page publication"))?;
    Ok(lock)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), RelationalRowPagePublicationError> {
    let mut file = File::create(path).map_err(durability("create row-page candidate"))?;
    file.write_all(bytes)
        .map_err(durability("write row-page candidate"))?;
    file.sync_all()
        .map_err(durability("sync row-page candidate"))
}

fn durable_publish_immutable(
    source: &Path,
    destination: &Path,
) -> Result<(), RelationalRowPagePublicationError> {
    if destination.exists() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "immutable row-page artifact {} already exists",
            destination.display()
        )));
    }
    durable_replace_file(source, destination).map_err(durability("publish row-page artifact"))
}

fn maybe_stop(
    stop_after: Option<RelationalRowPagePublicationPhase>,
    phase: RelationalRowPagePublicationPhase,
) -> Result<(), RelationalRowPagePublicationError> {
    if stop_after == Some(phase) {
        return Err(RelationalRowPagePublicationError::Durability(format!(
            "injected stop after {phase:?}"
        )));
    }
    Ok(())
}

struct PublicationPaths {
    page: PathBuf,
    page_tmp: PathBuf,
    descriptor: PathBuf,
    descriptor_tmp: PathBuf,
    key: PathBuf,
    key_tmp: PathBuf,
    generation_manifest: PathBuf,
    generation_manifest_tmp: PathBuf,
    latest_manifest: PathBuf,
    latest_manifest_tmp: PathBuf,
}

impl PublicationPaths {
    fn new(directory: &Path, generation: u64) -> Self {
        let page = directory.join(relational_row_page_artifact_file(generation));
        let descriptor = directory.join(relational_row_page_root_descriptor_file(generation));
        let key = directory.join(relational_row_page_root_key_file(generation));
        let generation_manifest =
            directory.join(relational_row_page_manifest_generation_file(generation));
        let latest_manifest = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
        Self {
            page_tmp: page.with_extension("skein.tmp"),
            descriptor_tmp: descriptor.with_extension("skein.tmp"),
            key_tmp: key.with_extension("skein.tmp"),
            generation_manifest_tmp: generation_manifest.with_extension("skein.tmp"),
            latest_manifest_tmp: latest_manifest.with_extension("skein.tmp"),
            page,
            descriptor,
            key,
            generation_manifest,
            latest_manifest,
        }
    }

    fn remove_temps(&self) -> Result<(), RelationalRowPagePublicationError> {
        for path in [
            &self.page_tmp,
            &self.descriptor_tmp,
            &self.key_tmp,
            &self.generation_manifest_tmp,
            &self.latest_manifest_tmp,
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(durability("remove stale row-page candidate")(error)),
            }
        }
        sync_directory(
            self.latest_manifest
                .parent()
                .expect("publication paths have a directory"),
        )
        .map_err(durability(
            "sync row-page directory after candidate cleanup",
        ))
    }

    fn require_fresh_generation(&self) -> Result<(), RelationalRowPagePublicationError> {
        for path in [
            &self.page,
            &self.descriptor,
            &self.key,
            &self.generation_manifest,
        ] {
            if path.exists() {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "row-page generation artifact {} already exists",
                    path.display()
                )));
            }
        }
        Ok(())
    }
}
