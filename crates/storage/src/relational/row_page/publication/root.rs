use super::publisher::{PreparedDirtyPage, PreparedTableDelta};
use super::{
    durability, RelationalRowPageArtifactMetadata, RelationalRowPagePublicationConfig,
    RelationalRowPagePublicationError, RelationalRowPageRootDescriptor,
    RelationalRowPageRootReader, RelationalRowPageSlotIntegrity, RelationalRowPageTableRoot,
};
use crate::relational::{
    ordered_key::encode_ordered_relational_key, ImmutableRelationalRowPage,
    RelationalRowPageLimits, RelationalRowPageView,
};
use skein_integrity::{IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

mod codec;

pub(super) use codec::{read_descriptor, ROOT_DESCRIPTOR_BYTES};
use codec::{validate_descriptor, WireDescriptor};
const ROOT_DESCRIPTOR_BINDING_OFFSET: usize = 100;

pub(super) struct RootBuildOutput {
    pub tables: Vec<RelationalRowPageTableRoot>,
    pub root_page_count: u64,
    pub reused_page_count: u64,
    pub descriptor_artifact: RelationalRowPageArtifactMetadata,
    pub key_artifact: RelationalRowPageArtifactMetadata,
}

pub(super) fn prepare_dirty_page(
    page: ImmutableRelationalRowPage,
    limits: RelationalRowPageLimits,
) -> Result<PreparedDirtyPage, RelationalRowPagePublicationError> {
    let first = page.rows.first().ok_or_else(|| {
        RelationalRowPagePublicationError::Admission("dirty row page contains no rows".to_string())
    })?;
    let last = page.rows.last().expect("dirty page has a first row");
    let lower_bound = encode_ordered_relational_key(&first.primary_key).map_err(|error| {
        RelationalRowPagePublicationError::Admission(format!("dirty row-page lower bound: {error}"))
    })?;
    let upper_bound = encode_ordered_relational_key(&last.primary_key).map_err(|error| {
        RelationalRowPagePublicationError::Admission(format!("dirty row-page upper bound: {error}"))
    })?;
    if lower_bound.len() > limits.max_key_bytes.get()
        || upper_bound.len() > limits.max_key_bytes.get()
    {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "dirty row-page bounds exceed key limit {}",
            limits.max_key_bytes
        )));
    }
    let row_count = u32::try_from(page.rows.len()).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page row count does not fit in u32".to_string(),
        )
    })?;
    let descriptor = RelationalRowPageRootDescriptor {
        logical_page_id: page.page_id,
        physical_generation: page.generation,
        physical_slot: 0,
        source_commit_epoch: page.source_commit_epoch,
        row_count,
        lower_bound,
        upper_bound,
        slot_integrity: RelationalRowPageSlotIntegrity {
            encoded_len: 0,
            slot_crc32c: 0,
            slot_sha256: Sha256Digest::from_bytes([0; SHA256_BYTES]),
        },
    };
    Ok(PreparedDirtyPage { page, descriptor })
}

pub(super) fn write_dirty_page_artifact(
    path: &Path,
    deltas: &mut BTreeMap<String, PreparedTableDelta>,
    config: RelationalRowPagePublicationConfig,
) -> Result<(RelationalRowPageArtifactMetadata, u64), RelationalRowPagePublicationError> {
    let file = File::create(path).map_err(durability("create dirty row-page artifact"))?;
    let mut writer = BufWriter::new(file);
    let mut hasher = IntegrityHasher::new();
    let mut slot = 0u64;
    let mut encoded_len = 0u64;
    for delta in deltas.values_mut() {
        for page in &mut delta.dirty_pages {
            let mut encoded_slot = page.page.encode(config.page_limits)?;
            let encoded_page_len = u32::try_from(encoded_slot.len()).map_err(|_| {
                RelationalRowPagePublicationError::Admission(
                    "encoded row page length does not fit in u32".to_string(),
                )
            })?;
            let view = RelationalRowPageView::open(&encoded_slot, config.page_limits)?;
            if view.lower_bound_bytes() != page.descriptor.lower_bound
                || view.upper_bound_bytes() != page.descriptor.upper_bound
                || view.row_count() as u32 != page.descriptor.row_count
            {
                return Err(RelationalRowPagePublicationError::Corrupt(format!(
                    "dirty page {} metadata changed during encoding",
                    page.descriptor.logical_page_id.get()
                )));
            }
            encoded_slot.resize(config.page_limits.max_page_bytes.get(), 0);
            let slot_digest = digest_bytes(&encoded_slot);
            page.descriptor.physical_slot = slot;
            page.descriptor.slot_integrity = RelationalRowPageSlotIntegrity {
                encoded_len: encoded_page_len,
                slot_crc32c: slot_digest.encoded_crc32c,
                slot_sha256: slot_digest.encoded_sha256,
            };
            writer
                .write_all(&encoded_slot)
                .map_err(durability("write dirty row-page slot"))?;
            hasher.update(&encoded_slot);
            encoded_len = encoded_len
                .checked_add(encoded_slot.len() as u64)
                .ok_or_else(|| {
                    RelationalRowPagePublicationError::Admission(
                        "dirty row-page artifact length overflow".to_string(),
                    )
                })?;
            slot = slot.checked_add(1).ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "dirty row-page slot count overflow".to_string(),
                )
            })?;
        }
    }
    writer
        .flush()
        .map_err(durability("flush dirty row-page artifact"))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(durability("sync dirty row-page artifact"))?;
    let expected_len = slot
        .checked_mul(config.page_limits.max_page_bytes.get() as u64)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "dirty row-page artifact length overflow".to_string(),
            )
        })?;
    if encoded_len != expected_len {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "dirty row-page writer produced {encoded_len} bytes, expected {expected_len}"
        )));
    }
    let digest = hasher.finish();
    Ok((
        RelationalRowPageArtifactMetadata {
            encoded_len,
            encoded_crc32c: digest.crc32c.get(),
            encoded_sha256: digest.sha256,
        },
        slot,
    ))
}

pub(super) fn write_root_artifacts(
    descriptor_path: &Path,
    key_path: &Path,
    base: Option<&RelationalRowPageRootReader>,
    deltas: &mut BTreeMap<String, PreparedTableDelta>,
    generation: u64,
    source_commit_epoch: u64,
    config: RelationalRowPagePublicationConfig,
) -> Result<RootBuildOutput, RelationalRowPagePublicationError> {
    let descriptor_file =
        File::create(descriptor_path).map_err(durability("create row-page root descriptors"))?;
    let key_file = File::create(key_path).map_err(durability("create row-page root keys"))?;
    let mut writer = RootWriter::new(
        descriptor_file,
        key_file,
        generation,
        source_commit_epoch,
        config,
    );
    let mut table_names = BTreeSet::new();
    if let Some(base) = base {
        table_names.extend(base.manifest.tables.iter().map(|table| table.table.clone()));
    }
    table_names.extend(deltas.keys().cloned());
    if table_names.len() > config.max_tables.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page root contains {} tables, exceeding limit {}",
            table_names.len(),
            config.max_tables
        )));
    }

    let mut tables = Vec::with_capacity(table_names.len());
    let mut reused_page_count = 0u64;
    for table_name in table_names {
        let base_table = base.and_then(|reader| {
            reader
                .manifest
                .tables
                .binary_search_by(|table| table.table.cmp(&table_name))
                .ok()
                .map(|index| &reader.manifest.tables[index])
        });
        let delta = deltas.remove(&table_name);
        let schema_digest = match (base_table, delta.as_ref()) {
            (Some(base_table), Some(delta)) if base_table.schema_digest != delta.schema_digest => {
                return Err(RelationalRowPagePublicationError::Admission(format!(
                    "table {table_name} schema digest changed during incremental row-page publication"
                )));
            }
            (Some(base_table), _) => base_table.schema_digest,
            (None, Some(delta)) => delta.schema_digest,
            (None, None) => unreachable!("table name originated from base or delta"),
        };
        let first_descriptor = writer.descriptor_count;
        let mut bounds = TableBounds::default();
        match (base, base_table, delta) {
            (Some(base), Some(_), Some(delta)) => {
                reused_page_count = reused_page_count
                    .checked_add(write_merged_table(
                        &mut writer,
                        base,
                        &table_name,
                        delta,
                        &mut bounds,
                    )?)
                    .ok_or_else(|| {
                        RelationalRowPagePublicationError::Admission(
                            "reused row-page count overflow".to_string(),
                        )
                    })?;
            }
            (Some(base), Some(_), None) => {
                base.visit_table_pages(&table_name, |descriptor| {
                    writer.write_descriptor(descriptor, &mut bounds)
                })?;
                reused_page_count = reused_page_count
                    .checked_add(writer.descriptor_count - first_descriptor)
                    .ok_or_else(|| {
                        RelationalRowPagePublicationError::Admission(
                            "reused row-page count overflow".to_string(),
                        )
                    })?;
            }
            (_, None, Some(delta)) => {
                if !delta.deleted_page_ids.is_empty() {
                    return Err(RelationalRowPagePublicationError::Admission(format!(
                        "new table {table_name} deletes pages from a missing base"
                    )));
                }
                for page in &delta.dirty_pages {
                    writer.write_descriptor(&page.descriptor, &mut bounds)?;
                }
            }
            _ => unreachable!("base table and delta combination was exhausted"),
        }
        let page_count = writer.descriptor_count - first_descriptor;
        tables.push(RelationalRowPageTableRoot {
            table: table_name,
            schema_digest,
            first_descriptor,
            page_count,
            lower_bound: bounds.lower.unwrap_or_default(),
            upper_bound: bounds.upper.unwrap_or_default(),
        });
    }
    if !deltas.is_empty() {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page root builder left unprocessed table deltas".to_string(),
        ));
    }
    let finished = writer.finish()?;
    Ok(RootBuildOutput {
        tables,
        root_page_count: finished.root_page_count,
        reused_page_count,
        descriptor_artifact: finished.descriptor_artifact,
        key_artifact: finished.key_artifact,
    })
}

fn write_merged_table(
    writer: &mut RootWriter,
    base: &RelationalRowPageRootReader,
    table: &str,
    delta: PreparedTableDelta,
    bounds: &mut TableBounds,
) -> Result<u64, RelationalRowPagePublicationError> {
    debug_assert_eq!(delta.table, table);
    let dirty_page_ids = delta
        .dirty_pages
        .iter()
        .map(|page| page.descriptor.logical_page_id)
        .collect::<BTreeSet<_>>();
    let mut remaining_deleted = delta.deleted_page_ids.clone();
    let mut dirty_index = 0usize;
    let mut reused = 0u64;
    base.visit_table_pages(table, |base_descriptor| {
        if remaining_deleted.remove(&base_descriptor.logical_page_id)
            || dirty_page_ids.contains(&base_descriptor.logical_page_id)
        {
            return Ok(());
        }
        while let Some(dirty) = delta.dirty_pages.get(dirty_index) {
            if dirty.descriptor.lower_bound >= base_descriptor.lower_bound {
                break;
            }
            writer.write_descriptor(&dirty.descriptor, bounds)?;
            dirty_index += 1;
        }
        writer.write_descriptor(base_descriptor, bounds)?;
        reused = reused.checked_add(1).ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "reused row-page count overflow".to_string(),
            )
        })?;
        Ok(())
    })?;
    if !remaining_deleted.is_empty() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "table {table} deletes {} page ids absent from the selected base",
            remaining_deleted.len()
        )));
    }
    for dirty in &delta.dirty_pages[dirty_index..] {
        writer.write_descriptor(&dirty.descriptor, bounds)?;
    }
    Ok(reused)
}

#[derive(Default)]
struct TableBounds {
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
}

struct RootWriter {
    descriptors: BufWriter<File>,
    keys: BufWriter<File>,
    descriptor_hasher: IntegrityHasher,
    key_hasher: IntegrityHasher,
    descriptor_count: u64,
    key_bytes: u64,
    generation: u64,
    source_commit_epoch: u64,
    config: RelationalRowPagePublicationConfig,
}

impl RootWriter {
    fn new(
        descriptors: File,
        keys: File,
        generation: u64,
        source_commit_epoch: u64,
        config: RelationalRowPagePublicationConfig,
    ) -> Self {
        Self {
            descriptors: BufWriter::new(descriptors),
            keys: BufWriter::new(keys),
            descriptor_hasher: IntegrityHasher::new(),
            key_hasher: IntegrityHasher::new(),
            descriptor_count: 0,
            key_bytes: 0,
            generation,
            source_commit_epoch,
            config,
        }
    }

    fn write_descriptor(
        &mut self,
        descriptor: &RelationalRowPageRootDescriptor,
        table_bounds: &mut TableBounds,
    ) -> Result<(), RelationalRowPagePublicationError> {
        validate_descriptor(
            descriptor,
            self.generation,
            self.source_commit_epoch,
            self.config,
        )?;
        if table_bounds
            .upper
            .as_ref()
            .is_some_and(|upper| upper.as_slice() >= descriptor.lower_bound.as_slice())
        {
            return Err(RelationalRowPagePublicationError::Admission(
                "row-page root bounds overlap or are unordered".to_string(),
            ));
        }
        if self.descriptor_count >= self.config.max_root_pages.get() {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "row-page root exceeds page limit {}",
                self.config.max_root_pages
            )));
        }
        let lower_offset = self.key_bytes;
        self.write_key(&descriptor.lower_bound)?;
        let upper_offset = self.key_bytes;
        self.write_key(&descriptor.upper_bound)?;
        let wire = WireDescriptor {
            logical_page_id: descriptor.logical_page_id.get(),
            physical_generation: descriptor.physical_generation,
            physical_slot: descriptor.physical_slot,
            source_commit_epoch: descriptor.source_commit_epoch,
            row_count: descriptor.row_count,
            encoded_len: descriptor.slot_integrity.encoded_len,
            lower_offset,
            lower_len: descriptor.lower_bound.len() as u32,
            upper_offset,
            upper_len: descriptor.upper_bound.len() as u32,
            slot_crc32c: descriptor.slot_integrity.slot_crc32c,
            slot_sha256: descriptor.slot_integrity.slot_sha256,
            binding_crc32c: 0,
            binding_sha256: Sha256Digest::from_bytes([0; SHA256_BYTES]),
        };
        let mut encoded = wire.encode();
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded[..ROOT_DESCRIPTOR_BINDING_OFFSET]);
        hasher.update(&descriptor.lower_bound);
        hasher.update(&descriptor.upper_bound);
        let digest = hasher.finish();
        encoded[100..104].copy_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded[104..136].copy_from_slice(digest.sha256.as_bytes());
        self.descriptors
            .write_all(&encoded)
            .map_err(durability("write row-page root descriptor"))?;
        self.descriptor_hasher.update(&encoded);
        self.descriptor_count += 1;
        table_bounds
            .lower
            .get_or_insert_with(|| descriptor.lower_bound.clone());
        table_bounds.upper = Some(descriptor.upper_bound.clone());
        Ok(())
    }

    fn write_key(&mut self, key: &[u8]) -> Result<(), RelationalRowPagePublicationError> {
        let next = self
            .key_bytes
            .checked_add(key.len() as u64)
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "row-page root key byte count overflow".to_string(),
                )
            })?;
        if next > self.config.max_root_key_bytes.get() {
            return Err(RelationalRowPagePublicationError::Admission(format!(
                "row-page root keys contain {next} bytes, exceeding limit {}",
                self.config.max_root_key_bytes
            )));
        }
        self.keys
            .write_all(key)
            .map_err(durability("write row-page root key"))?;
        self.key_hasher.update(key);
        self.key_bytes = next;
        Ok(())
    }

    fn finish(mut self) -> Result<FinishedRootWriter, RelationalRowPagePublicationError> {
        self.descriptors
            .flush()
            .map_err(durability("flush row-page root descriptors"))?;
        self.keys
            .flush()
            .map_err(durability("flush row-page root keys"))?;
        self.descriptors
            .get_ref()
            .sync_all()
            .map_err(durability("sync row-page root descriptors"))?;
        self.keys
            .get_ref()
            .sync_all()
            .map_err(durability("sync row-page root keys"))?;
        let descriptor_digest = self.descriptor_hasher.finish();
        let key_digest = self.key_hasher.finish();
        let descriptor_len = self
            .descriptor_count
            .checked_mul(ROOT_DESCRIPTOR_BYTES as u64)
            .ok_or_else(|| {
                RelationalRowPagePublicationError::Admission(
                    "row-page descriptor byte count overflow".to_string(),
                )
            })?;
        Ok(FinishedRootWriter {
            root_page_count: self.descriptor_count,
            descriptor_artifact: RelationalRowPageArtifactMetadata {
                encoded_len: descriptor_len,
                encoded_crc32c: descriptor_digest.crc32c.get(),
                encoded_sha256: descriptor_digest.sha256,
            },
            key_artifact: RelationalRowPageArtifactMetadata {
                encoded_len: self.key_bytes,
                encoded_crc32c: key_digest.crc32c.get(),
                encoded_sha256: key_digest.sha256,
            },
        })
    }
}

struct FinishedRootWriter {
    root_page_count: u64,
    descriptor_artifact: RelationalRowPageArtifactMetadata,
    key_artifact: RelationalRowPageArtifactMetadata,
}

fn digest_bytes(bytes: &[u8]) -> RelationalRowPageArtifactMetadata {
    let mut hasher = IntegrityHasher::new();
    hasher.update(bytes);
    let digest = hasher.finish();
    RelationalRowPageArtifactMetadata {
        encoded_len: bytes.len() as u64,
        encoded_crc32c: digest.crc32c.get(),
        encoded_sha256: digest.sha256,
    }
}
