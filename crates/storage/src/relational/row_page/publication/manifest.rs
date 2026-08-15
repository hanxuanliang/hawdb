use super::{
    durability, RelationalRowPageArtifactMetadata, RelationalRowPagePublicationConfig,
    RelationalRowPagePublicationError, RelationalRowPageRootManifest, RelationalRowPageTableRoot,
};
use skein_integrity::{IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fs::{self, File};
use std::io::Read;
use std::num::NonZeroU64;
use std::path::Path;

const MANIFEST_MAGIC: &[u8; 8] = b"SKRPGM01";
const MANIFEST_VERSION: u16 = 1;
pub(super) const MANIFEST_HEADER_BYTES: usize = 316;
const MANIFEST_INTEGRITY_OFFSET: usize = 280;
const ARTIFACT_METADATA_BYTES: usize = 44;

pub(super) fn root_set_digest(
    tables: &[RelationalRowPageTableRoot],
) -> Result<Sha256Digest, RelationalRowPagePublicationError> {
    let payload = encode_tables(tables)?;
    let mut hasher = IntegrityHasher::new();
    hasher.update(&payload);
    Ok(hasher.finish().sha256)
}

pub(super) fn encode_manifest(
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    validate_manifest(manifest, config, ErrorClass::Admission)?;
    let payload = encode_tables(&manifest.tables)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest payload does not fit in u32".to_string(),
        )
    })?;
    let encoded_len = MANIFEST_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Admission(
                "row-page manifest length overflow".to_string(),
            )
        })?;
    if encoded_len > config.max_manifest_bytes.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }

    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(MANIFEST_MAGIC);
    encoded.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&manifest.generation.to_le_bytes());
    encoded.extend_from_slice(&manifest.source_commit_epoch.to_le_bytes());
    encoded.extend_from_slice(&manifest.previous_generation.unwrap_or(0).to_le_bytes());
    encoded.extend_from_slice(&manifest.page_bytes.to_le_bytes());
    encoded.extend_from_slice(&manifest.dirty_page_count.to_le_bytes());
    encoded.extend_from_slice(&manifest.root_page_count.to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(manifest.tables.len())
            .expect("validated table count fits in u32")
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encode_artifact(manifest.page_artifact, &mut encoded);
    encode_artifact(manifest.root_descriptor_artifact, &mut encoded);
    encode_artifact(manifest.root_key_artifact, &mut encoded);
    encoded.extend_from_slice(manifest.root_set_digest.as_bytes());
    match manifest.overflow_root {
        Some(binding) => {
            encoded.extend_from_slice(&binding.generation.to_le_bytes());
            encoded.extend_from_slice(&binding.source_commit_epoch.to_le_bytes());
            encoded.extend_from_slice(binding.root_set_digest.as_bytes());
        }
        None => encoded.extend_from_slice(&[0u8; 48]),
    }
    debug_assert_eq!(encoded.len(), MANIFEST_INTEGRITY_OFFSET);
    encoded.extend_from_slice(&0u32.to_le_bytes());
    encoded.extend_from_slice(&[0u8; SHA256_BYTES]);
    debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);
    encoded.extend_from_slice(&payload);

    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    hasher.update(&payload);
    let digest = hasher.finish();
    encoded[280..284].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[284..316].copy_from_slice(digest.sha256.as_bytes());
    Ok(encoded)
}

pub(super) fn read_manifest_if_exists(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
) -> Result<Option<RelationalRowPageRootManifest>, RelationalRowPagePublicationError> {
    match fs::metadata(path) {
        Ok(_) => read_manifest(path, config).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(durability("read row-page manifest metadata")(error)),
    }
}

pub(super) fn read_manifest(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
) -> Result<RelationalRowPageRootManifest, RelationalRowPagePublicationError> {
    let encoded_len = fs::metadata(path)
        .map_err(durability("read row-page manifest metadata"))?
        .len();
    if encoded_len > config.max_manifest_bytes.get() as u64 {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let capacity = usize::try_from(encoded_len).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest length overflows usize".to_string(),
        )
    })?;
    let mut encoded = Vec::with_capacity(capacity);
    File::open(path)
        .map_err(durability("open row-page manifest"))?
        .read_to_end(&mut encoded)
        .map_err(durability("read row-page manifest"))?;
    decode_manifest(&encoded, config)
}

fn decode_manifest(
    encoded: &[u8],
    config: RelationalRowPagePublicationConfig,
) -> Result<RelationalRowPageRootManifest, RelationalRowPagePublicationError> {
    if encoded.len() < MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "invalid row-page manifest header".to_string(),
        ));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    if version != MANIFEST_VERSION || flags != 0 {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "unsupported row-page manifest version {version} or flags {flags}"
        )));
    }
    let generation = read_u64(&encoded[12..20]);
    let source_commit_epoch = read_u64(&encoded[20..28]);
    let previous = read_u64(&encoded[28..36]);
    let page_bytes = read_u64(&encoded[36..44]);
    let dirty_page_count = read_u64(&encoded[44..52]);
    let root_page_count = read_u64(&encoded[52..60]);
    let table_count = read_u32(&encoded[60..64]) as usize;
    let payload_len = read_u32(&encoded[64..68]) as usize;
    if table_count > config.max_tables.get() {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest declares {table_count} tables, exceeding limit {}",
            config.max_tables
        )));
    }
    let expected_len = MANIFEST_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page manifest length overflow".to_string(),
            )
        })?;
    if encoded.len() != expected_len {
        return Err(RelationalRowPagePublicationError::Corrupt(format!(
            "row-page manifest contains {} bytes, expected {expected_len}",
            encoded.len()
        )));
    }
    let page_artifact = decode_artifact(&encoded[68..112]);
    let root_descriptor_artifact = decode_artifact(&encoded[112..156]);
    let root_key_artifact = decode_artifact(&encoded[156..200]);
    let root_set_digest = Sha256Digest::from_bytes(
        encoded[200..232]
            .try_into()
            .expect("root-set digest length was checked"),
    );
    let overflow_generation = read_u64(&encoded[232..240]);
    let overflow_source_commit_epoch = read_u64(&encoded[240..248]);
    let overflow_root_set_digest = Sha256Digest::from_bytes(
        encoded[248..280]
            .try_into()
            .expect("overflow root digest length was checked"),
    );
    let overflow_root = if overflow_generation == 0 {
        if overflow_source_commit_epoch != 0 || overflow_root_set_digest.as_bytes() != &[0u8; 32] {
            return Err(RelationalRowPagePublicationError::Corrupt(
                "row-page manifest has a partial overflow binding".to_string(),
            ));
        }
        None
    } else {
        Some(crate::relational::RelationalOverflowRootBinding {
            generation: overflow_generation,
            source_commit_epoch: overflow_source_commit_epoch,
            root_set_digest: overflow_root_set_digest,
        })
    };
    let payload = &encoded[MANIFEST_HEADER_BYTES..];
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    hasher.update(payload);
    let digest = hasher.finish();
    if digest.crc32c.get() != read_u32(&encoded[280..284])
        || digest.sha256.as_bytes() != &encoded[284..316]
    {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page manifest checksum mismatch".to_string(),
        ));
    }
    let tables = decode_tables(payload, table_count, config)?;
    let manifest = RelationalRowPageRootManifest {
        generation,
        source_commit_epoch,
        previous_generation: (previous != 0).then_some(previous),
        page_bytes,
        dirty_page_count,
        root_page_count,
        page_artifact,
        root_descriptor_artifact,
        root_key_artifact,
        root_set_digest,
        overflow_root,
        tables,
    };
    validate_manifest(&manifest, config, ErrorClass::Corrupt)?;
    Ok(manifest)
}

fn validate_manifest(
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
    class: ErrorClass,
) -> Result<(), RelationalRowPagePublicationError> {
    let fail = |message| class.error(message);
    if manifest.generation == 0
        || (manifest.source_commit_epoch == 0
            && (manifest.root_page_count != 0 || !manifest.tables.is_empty()))
    {
        return Err(fail(format!(
            "invalid row-page generation/epoch {}/{}",
            manifest.generation, manifest.source_commit_epoch
        )));
    }
    if manifest
        .previous_generation
        .is_some_and(|previous| previous >= manifest.generation)
    {
        return Err(fail(format!(
            "previous row-page generation {:?} does not precede generation {}",
            manifest.previous_generation, manifest.generation
        )));
    }
    if let Some(binding) = manifest.overflow_root
        && (binding.generation != manifest.generation
            || binding.source_commit_epoch != manifest.source_commit_epoch)
    {
        return Err(fail(format!(
            "row-page overflow root identifies generation/epoch {}/{}, expected {}/{}",
            binding.generation,
            binding.source_commit_epoch,
            manifest.generation,
            manifest.source_commit_epoch
        )));
    }
    if manifest.page_bytes != config.page_limits.max_page_bytes.get() as u64 {
        return Err(fail(format!(
            "row-page slot contains {} bytes, configured for {}",
            manifest.page_bytes, config.page_limits.max_page_bytes
        )));
    }
    if manifest.dirty_page_count > config.max_dirty_pages.get() as u64 {
        return Err(fail(format!(
            "row-page manifest declares {} dirty pages, exceeding limit {}",
            manifest.dirty_page_count, config.max_dirty_pages
        )));
    }
    let expected_page_bytes = manifest
        .dirty_page_count
        .checked_mul(manifest.page_bytes)
        .ok_or_else(|| fail("row-page artifact length overflow".to_string()))?;
    if manifest.page_artifact.encoded_len != expected_page_bytes {
        return Err(fail(format!(
            "row-page artifact declares {} bytes, expected {expected_page_bytes}",
            manifest.page_artifact.encoded_len
        )));
    }
    if manifest.root_page_count > config.max_root_pages.get() {
        return Err(fail(format!(
            "row-page root declares {} pages, exceeding limit {}",
            manifest.root_page_count, config.max_root_pages
        )));
    }
    let expected_descriptor_bytes = manifest
        .root_page_count
        .checked_mul(super::root::ROOT_DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| fail("row-page descriptor length overflow".to_string()))?;
    if manifest.root_descriptor_artifact.encoded_len != expected_descriptor_bytes {
        return Err(fail(format!(
            "row-page descriptor artifact declares {} bytes, expected {expected_descriptor_bytes}",
            manifest.root_descriptor_artifact.encoded_len
        )));
    }
    if manifest.root_key_artifact.encoded_len > config.max_root_key_bytes.get() {
        return Err(fail(format!(
            "row-page root keys contain {} bytes, exceeding limit {}",
            manifest.root_key_artifact.encoded_len, config.max_root_key_bytes
        )));
    }
    if manifest.tables.len() > config.max_tables.get() {
        return Err(fail(format!(
            "row-page manifest contains {} tables, exceeding limit {}",
            manifest.tables.len(),
            config.max_tables
        )));
    }
    let mut expected_descriptor = 0u64;
    let mut previous_table: Option<&str> = None;
    for table in &manifest.tables {
        if table.table.is_empty() || table.table.len() > config.max_table_name_bytes.get() {
            return Err(fail(format!(
                "row-page table name contains {} bytes, outside admitted range",
                table.table.len()
            )));
        }
        if previous_table.is_some_and(|previous| previous >= table.table.as_str()) {
            return Err(fail(
                "row-page table roots are not strictly ordered".to_string(),
            ));
        }
        if table.first_descriptor != expected_descriptor {
            return Err(fail(format!(
                "table {} starts at descriptor {}, expected {expected_descriptor}",
                table.table, table.first_descriptor
            )));
        }
        expected_descriptor = expected_descriptor
            .checked_add(table.page_count)
            .ok_or_else(|| fail("row-page table descriptor count overflow".to_string()))?;
        if table.page_count == 0 {
            if !table.lower_bound.is_empty() || !table.upper_bound.is_empty() {
                return Err(fail(format!(
                    "empty table {} has non-empty key bounds",
                    table.table
                )));
            }
        } else if table.lower_bound.is_empty()
            || table.upper_bound.is_empty()
            || table.lower_bound > table.upper_bound
            || table.lower_bound.len() > config.page_limits.max_key_bytes.get()
            || table.upper_bound.len() > config.page_limits.max_key_bytes.get()
        {
            return Err(fail(format!(
                "table {} has invalid row-page key bounds",
                table.table
            )));
        }
        previous_table = Some(&table.table);
    }
    if expected_descriptor != manifest.root_page_count {
        return Err(fail(format!(
            "table roots cover {expected_descriptor} descriptors, expected {}",
            manifest.root_page_count
        )));
    }
    let payload = encode_tables(&manifest.tables)?;
    let mut hasher = IntegrityHasher::new();
    hasher.update(&payload);
    if hasher.finish().sha256 != manifest.root_set_digest {
        return Err(fail("row-page root-set digest mismatch".to_string()));
    }
    Ok(())
}

fn encode_tables(
    tables: &[RelationalRowPageTableRoot],
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let mut encoded = Vec::new();
    for table in tables {
        encode_bytes(&table.table, &mut encoded)?;
        encoded.extend_from_slice(table.schema_digest.as_bytes());
        encoded.extend_from_slice(&table.next_page_id.get().to_le_bytes());
        encoded.extend_from_slice(&table.first_descriptor.to_le_bytes());
        encoded.extend_from_slice(&table.page_count.to_le_bytes());
        encode_raw_bytes(&table.lower_bound, &mut encoded)?;
        encode_raw_bytes(&table.upper_bound, &mut encoded)?;
    }
    Ok(encoded)
}

fn decode_tables(
    payload: &[u8],
    table_count: usize,
    config: RelationalRowPagePublicationConfig,
) -> Result<Vec<RelationalRowPageTableRoot>, RelationalRowPagePublicationError> {
    let mut tables = Vec::with_capacity(table_count);
    let mut offset = 0usize;
    for _ in 0..table_count {
        let table = decode_utf8_bytes(
            payload,
            &mut offset,
            config.max_table_name_bytes.get(),
            "table name",
        )?;
        let schema_digest = Sha256Digest::from_bytes(
            take(payload, &mut offset, SHA256_BYTES, "table schema digest")?
                .try_into()
                .expect("schema digest length was checked"),
        );
        let next_page_id = NonZeroU64::new(read_u64(take(
            payload,
            &mut offset,
            8,
            "next logical page id",
        )?))
        .ok_or_else(|| {
            RelationalRowPagePublicationError::Corrupt(
                "row-page table allocator contains zero".to_string(),
            )
        })?;
        let first_descriptor = read_u64(take(payload, &mut offset, 8, "first descriptor ordinal")?);
        let page_count = read_u64(take(payload, &mut offset, 8, "table page count")?);
        let lower_bound = decode_raw_bytes(
            payload,
            &mut offset,
            config.page_limits.max_key_bytes.get(),
            "table lower bound",
        )?;
        let upper_bound = decode_raw_bytes(
            payload,
            &mut offset,
            config.page_limits.max_key_bytes.get(),
            "table upper bound",
        )?;
        tables.push(RelationalRowPageTableRoot {
            table,
            schema_digest,
            next_page_id,
            first_descriptor,
            page_count,
            lower_bound,
            upper_bound,
        });
    }
    if offset != payload.len() {
        return Err(RelationalRowPagePublicationError::Corrupt(
            "row-page manifest contains trailing table bytes".to_string(),
        ));
    }
    Ok(tables)
}

fn encode_artifact(metadata: RelationalRowPageArtifactMetadata, encoded: &mut Vec<u8>) {
    encoded.extend_from_slice(&metadata.encoded_len.to_le_bytes());
    encoded.extend_from_slice(&metadata.encoded_crc32c.to_le_bytes());
    encoded.extend_from_slice(metadata.encoded_sha256.as_bytes());
}

fn decode_artifact(encoded: &[u8]) -> RelationalRowPageArtifactMetadata {
    debug_assert_eq!(encoded.len(), ARTIFACT_METADATA_BYTES);
    RelationalRowPageArtifactMetadata {
        encoded_len: read_u64(&encoded[..8]),
        encoded_crc32c: read_u32(&encoded[8..12]),
        encoded_sha256: Sha256Digest::from_bytes(
            encoded[12..44]
                .try_into()
                .expect("artifact digest length was checked"),
        ),
    }
}

fn encode_bytes(
    value: &str,
    encoded: &mut Vec<u8>,
) -> Result<(), RelationalRowPagePublicationError> {
    encode_raw_bytes(value.as_bytes(), encoded)
}

fn encode_raw_bytes(
    value: &[u8],
    encoded: &mut Vec<u8>,
) -> Result<(), RelationalRowPagePublicationError> {
    let len = u32::try_from(value.len()).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest field does not fit in u32".to_string(),
        )
    })?;
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn decode_utf8_bytes(
    encoded: &[u8],
    offset: &mut usize,
    max_len: usize,
    context: &str,
) -> Result<String, RelationalRowPagePublicationError> {
    let bytes = decode_raw_bytes(encoded, offset, max_len, context)?;
    String::from_utf8(bytes).map_err(|error| {
        RelationalRowPagePublicationError::Corrupt(format!(
            "row-page {context} is not valid UTF-8: {error}"
        ))
    })
}

fn decode_raw_bytes(
    encoded: &[u8],
    offset: &mut usize,
    max_len: usize,
    context: &str,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let len = read_u32(take(encoded, offset, 4, context)?) as usize;
    if len > max_len {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page {context} contains {len} bytes, exceeding limit {max_len}"
        )));
    }
    Ok(take(encoded, offset, len, context)?.to_vec())
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    len: usize,
    context: &str,
) -> Result<&'a [u8], RelationalRowPagePublicationError> {
    let end = offset.checked_add(len).ok_or_else(|| {
        RelationalRowPagePublicationError::Corrupt(format!("row-page {context} length overflow"))
    })?;
    let bytes = encoded.get(*offset..end).ok_or_else(|| {
        RelationalRowPagePublicationError::Corrupt(format!("truncated row-page {context}"))
    })?;
    *offset = end;
    Ok(bytes)
}

fn read_u16(encoded: &[u8]) -> u16 {
    u16::from_le_bytes(encoded.try_into().expect("u16 field has a fixed length"))
}

fn read_u32(encoded: &[u8]) -> u32 {
    u32::from_le_bytes(encoded.try_into().expect("u32 field has a fixed length"))
}

fn read_u64(encoded: &[u8]) -> u64 {
    u64::from_le_bytes(encoded.try_into().expect("u64 field has a fixed length"))
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

impl ErrorClass {
    fn error(self, message: String) -> RelationalRowPagePublicationError {
        match self {
            Self::Admission => RelationalRowPagePublicationError::Admission(message),
            Self::Corrupt => RelationalRowPagePublicationError::Corrupt(message),
        }
    }
}
