use crate::{
    content_digest, ContentDigest, FileSegmentRangeReader, ManifestGeneration, SegmentCache,
    SegmentRangeReader, SegmentReadError, SegmentReadRange, StoreId,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINPROPSPILL01";
const BLOCK_HEADER: &[u8; 8] = b"SKNPRP01";
const MANIFEST_HEADER: &str = "SKEIN_PROPERTY_SPILL_MANIFEST_V1";
const ARTIFACT_ID: u64 = 0x534b_5052_5350_4c31;
const BLOCK_ID_BASE: u64 = 1 << 62;
const BLOCK_FIXED_BYTES: u64 = 8 + 8 + 8 + 4;
const RECORD_FIXED_BYTES: u64 = 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertySpillConfig {
    pub spill_threshold_bytes: NonZeroU64,
    pub target_block_bytes: NonZeroU64,
    pub max_value_bytes: NonZeroU64,
}

impl Default for PropertySpillConfig {
    fn default() -> Self {
        Self {
            spill_threshold_bytes: NonZeroU64::new(64 * 1024)
                .expect("default property spill threshold is non-zero"),
            target_block_bytes: NonZeroU64::new(1024 * 1024)
                .expect("default property spill block size is non-zero"),
            max_value_bytes: NonZeroU64::new(1024 * 1024 * 1024)
                .expect("default property spill value limit is non-zero"),
        }
    }
}

#[derive(Debug)]
pub enum PropertySpillError {
    Io(std::io::Error),
    Read(SegmentReadError),
    Corrupt(String),
    ValueTooLarge { value_bytes: u64, max_bytes: u64 },
    BlockTooLarge { block_bytes: u64, max_bytes: u64 },
}

impl Display for PropertySpillError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Corrupt(message) => formatter.write_str(message),
            Self::ValueTooLarge {
                value_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property spill value uses {value_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::BlockTooLarge {
                block_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property spill block uses {block_bytes} bytes, exceeding {max_bytes}"
            ),
        }
    }
}

impl Error for PropertySpillError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PropertySpillError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for PropertySpillError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertySpillBlockDescriptor {
    pub block_id: u64,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub min_spill_id: u64,
    pub max_spill_id: u64,
    pub value_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertySpillManifest {
    pub generation: ManifestGeneration,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub value_count: u64,
    pub value_bytes: u64,
    pub blocks: Vec<PropertySpillBlockDescriptor>,
}

impl PropertySpillManifest {
    pub fn validate(&self) -> Result<(), PropertySpillError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(PropertySpillError::Corrupt(
                "property spill manifest has an unsupported artifact id".to_string(),
            ));
        }
        if self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8 {
            return Err(PropertySpillError::Corrupt(
                "property spill artifact is shorter than its header".to_string(),
            ));
        }
        let mut previous_end = ARTIFACT_HEADER.len() as u64 + 8;
        let mut previous_id = None;
        let mut value_count = 0u64;
        for block in &self.blocks {
            if block.block_id < BLOCK_ID_BASE
                || block.value_count == 0
                || block.min_spill_id > block.max_spill_id
                || block.offset < previous_end
                || previous_id.is_some_and(|id| block.min_spill_id <= id)
            {
                return Err(PropertySpillError::Corrupt(format!(
                    "property spill block {} has invalid bounds",
                    block.block_id
                )));
            }
            previous_end = block
                .offset
                .checked_add(block.length.get())
                .ok_or_else(|| {
                    PropertySpillError::Corrupt(
                        "property spill block range overflows u64".to_string(),
                    )
                })?;
            if previous_end > self.artifact_len {
                return Err(PropertySpillError::Corrupt(
                    "property spill block exceeds its artifact".to_string(),
                ));
            }
            previous_id = Some(block.max_spill_id);
            value_count = value_count.saturating_add(u64::from(block.value_count));
        }
        if previous_end != self.artifact_len || value_count != self.value_count {
            return Err(PropertySpillError::Corrupt(
                "property spill manifest counts or artifact length are inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, PropertySpillError> {
        self.validate()?;
        let mut body = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nvalue_count\t{}\nvalue_bytes\t{}\n",
            self.generation.0,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.value_count,
            self.value_bytes
        );
        for block in &self.blocks {
            body.push_str(&format!(
                "block\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                block.block_id,
                block.offset,
                block.length.get(),
                block.content_digest.0,
                block.min_spill_id,
                block.max_spill_id,
                block.value_count
            ));
        }
        let checksum = content_digest(body.as_bytes()).0;
        Ok(format!("{body}checksum\t{checksum}\n"))
    }

    pub fn decode(encoded: &str) -> Result<Self, PropertySpillError> {
        let marker = "checksum\t";
        let checksum_offset = encoded.rfind(marker).ok_or_else(|| {
            PropertySpillError::Corrupt("property spill manifest is missing checksum".to_string())
        })?;
        let body = &encoded[..checksum_offset];
        let checksum_line = encoded[checksum_offset..].trim_end();
        if checksum_line.contains('\n') {
            return Err(PropertySpillError::Corrupt(
                "property spill manifest has data after checksum".to_string(),
            ));
        }
        let expected = parse_u64(
            checksum_line.strip_prefix(marker).unwrap_or_default(),
            "manifest checksum",
        )?;
        let actual = content_digest(body.as_bytes()).0;
        if expected != actual {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill manifest checksum mismatch: expected {expected}, got {actual}"
            )));
        }
        let mut generation = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut value_count = None;
        let mut value_bytes = None;
        let mut blocks = Vec::new();
        let mut saw_header = false;
        for line in body.lines() {
            if line == MANIFEST_HEADER {
                saw_header = true;
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["generation", value] => generation = Some(parse_u64(value, "generation")?),
                ["artifact_id", value] => artifact_id = Some(parse_u64(value, "artifact id")?),
                ["artifact_len", value] => {
                    artifact_len = Some(parse_u64(value, "artifact length")?)
                }
                ["artifact_digest", value] => {
                    artifact_digest = Some(parse_u64(value, "artifact digest")?)
                }
                ["value_count", value] => value_count = Some(parse_u64(value, "value count")?),
                ["value_bytes", value] => value_bytes = Some(parse_u64(value, "value bytes")?),
                ["block", block_id, offset, length, digest, min_id, max_id, count] => {
                    blocks.push(PropertySpillBlockDescriptor {
                        block_id: parse_u64(block_id, "block id")?,
                        offset: parse_u64(offset, "block offset")?,
                        length: NonZeroU64::new(parse_u64(length, "block length")?).ok_or_else(
                            || {
                                PropertySpillError::Corrupt(
                                    "property spill block length is zero".to_string(),
                                )
                            },
                        )?,
                        content_digest: ContentDigest(parse_u64(digest, "block digest")?),
                        min_spill_id: parse_u64(min_id, "minimum spill id")?,
                        max_spill_id: parse_u64(max_id, "maximum spill id")?,
                        value_count: parse_u32(count, "block value count")?,
                    });
                }
                [""] => {}
                _ => {
                    return Err(PropertySpillError::Corrupt(format!(
                        "invalid property spill manifest line: {line}"
                    )));
                }
            }
        }
        if !saw_header {
            return Err(PropertySpillError::Corrupt(
                "property spill manifest has an invalid header".to_string(),
            ));
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            value_count: required(value_count, "value count")?,
            value_bytes: required(value_bytes, "value bytes")?,
            blocks,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

pub struct PropertySpillWriter {
    path: PathBuf,
    file: File,
    generation: ManifestGeneration,
    config: PropertySpillConfig,
    artifact_digest: DigestState,
    artifact_len: u64,
    next_spill_id: u64,
    next_block_id: u64,
    value_bytes: u64,
    pending: Vec<(u64, Vec<u8>)>,
    pending_bytes: u64,
    blocks: Vec<PropertySpillBlockDescriptor>,
}

impl PropertySpillWriter {
    pub fn create(
        path: impl Into<PathBuf>,
        generation: ManifestGeneration,
        config: PropertySpillConfig,
    ) -> Result<Self, PropertySpillError> {
        let path = path.into();
        let mut file = File::create(&path)?;
        let mut artifact_digest = DigestState::new();
        write_hashed(&mut file, &mut artifact_digest, ARTIFACT_HEADER)?;
        write_hashed(&mut file, &mut artifact_digest, &generation.0.to_le_bytes())?;
        Ok(Self {
            path,
            file,
            generation,
            config,
            artifact_digest,
            artifact_len: ARTIFACT_HEADER.len() as u64 + 8,
            next_spill_id: 0,
            next_block_id: BLOCK_ID_BASE,
            value_bytes: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            blocks: Vec::new(),
        })
    }

    pub fn should_spill(&self, encoded_value_bytes: usize) -> bool {
        encoded_value_bytes as u64 >= self.config.spill_threshold_bytes.get()
    }

    pub fn push(&mut self, encoded_value: Vec<u8>) -> Result<u64, PropertySpillError> {
        let value_bytes = encoded_value.len() as u64;
        if value_bytes > self.config.max_value_bytes.get() {
            return Err(PropertySpillError::ValueTooLarge {
                value_bytes,
                max_bytes: self.config.max_value_bytes.get(),
            });
        }
        let record_bytes = RECORD_FIXED_BYTES.saturating_add(value_bytes);
        if !self.pending.is_empty()
            && BLOCK_FIXED_BYTES
                .saturating_add(self.pending_bytes)
                .saturating_add(record_bytes)
                > self.config.target_block_bytes.get()
        {
            self.flush_block()?;
        }
        let spill_id = self.next_spill_id;
        self.next_spill_id = self
            .next_spill_id
            .checked_add(1)
            .ok_or_else(|| PropertySpillError::Corrupt("property spill id overflow".to_string()))?;
        self.value_bytes = self.value_bytes.saturating_add(value_bytes);
        self.pending_bytes = self.pending_bytes.saturating_add(record_bytes);
        self.pending.push((spill_id, encoded_value));
        Ok(spill_id)
    }

    pub fn finish(mut self) -> Result<PropertySpillManifest, PropertySpillError> {
        self.flush_block()?;
        self.file.sync_all()?;
        let manifest = PropertySpillManifest {
            generation: self.generation,
            artifact_id: ARTIFACT_ID,
            artifact_len: self.artifact_len,
            artifact_digest: ContentDigest(self.artifact_digest.finish()),
            value_count: self.next_spill_id,
            value_bytes: self.value_bytes,
            blocks: self.blocks,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn flush_block(&mut self) -> Result<(), PropertySpillError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let block_bytes = BLOCK_FIXED_BYTES.saturating_add(self.pending_bytes);
        let hard_max = self.config.target_block_bytes.get().max(
            self.config
                .max_value_bytes
                .get()
                .saturating_add(BLOCK_FIXED_BYTES)
                .saturating_add(RECORD_FIXED_BYTES),
        );
        if block_bytes > hard_max {
            return Err(PropertySpillError::BlockTooLarge {
                block_bytes,
                max_bytes: hard_max,
            });
        }
        let min_spill_id = self.pending.first().expect("pending block is non-empty").0;
        let max_spill_id = self.pending.last().expect("pending block is non-empty").0;
        let value_count = u32::try_from(self.pending.len()).map_err(|_| {
            PropertySpillError::Corrupt("property spill block count exceeds u32".to_string())
        })?;
        let mut block_digest = DigestState::new();
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            BLOCK_HEADER,
        )?;
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            &self.generation.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            &self.next_block_id.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            &value_count.to_le_bytes(),
        )?;
        for (spill_id, value) in &self.pending {
            write_double_hashed(
                &mut self.file,
                &mut self.artifact_digest,
                &mut block_digest,
                &spill_id.to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.file,
                &mut self.artifact_digest,
                &mut block_digest,
                &(value.len() as u64).to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.file,
                &mut self.artifact_digest,
                &mut block_digest,
                value,
            )?;
        }
        let length = NonZeroU64::new(block_bytes).expect("property spill block is non-empty");
        self.blocks.push(PropertySpillBlockDescriptor {
            block_id: self.next_block_id,
            offset: self.artifact_len,
            length,
            content_digest: ContentDigest(block_digest.finish()),
            min_spill_id,
            max_spill_id,
            value_count,
        });
        self.artifact_len = self.artifact_len.saturating_add(block_bytes);
        self.next_block_id = self.next_block_id.saturating_add(1);
        self.pending.clear();
        self.pending_bytes = 0;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PropertySpillReader {
    path: PathBuf,
    manifest: PropertySpillManifest,
    range_reader: FileSegmentRangeReader,
    max_block_bytes: NonZeroU64,
}

impl PropertySpillReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: PropertySpillManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_block_bytes: NonZeroU64,
    ) -> Result<Self, PropertySpillError> {
        manifest.validate()?;
        let path = path.into();
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(PropertySpillError::Corrupt(
                "property spill artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        for block in &manifest.blocks {
            if block.length.get() > max_block_bytes.get() {
                return Err(PropertySpillError::BlockTooLarge {
                    block_bytes: block.length.get(),
                    max_bytes: max_block_bytes.get(),
                });
            }
        }
        let mut range_reader =
            FileSegmentRangeReader::new().with_cache(cache, store_id, manifest.generation);
        range_reader.register(manifest.artifact_id, path.clone());
        Ok(Self {
            path,
            manifest,
            range_reader,
            max_block_bytes,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn manifest(&self) -> &PropertySpillManifest {
        &self.manifest
    }

    pub fn get(&self, spill_id: u64) -> Result<Option<Arc<[u8]>>, PropertySpillError> {
        let Some(block) = find_block(&self.manifest.blocks, spill_id) else {
            return Ok(None);
        };
        if block.length.get() > self.max_block_bytes.get() {
            return Err(PropertySpillError::BlockTooLarge {
                block_bytes: block.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        let bytes = self.range_reader.read_range(&SegmentReadRange {
            artifact_id: self.manifest.artifact_id,
            segment_ids: vec![block.block_id],
            offset: block.offset,
            length: block.length,
            content_digest: Some(block.content_digest),
        })?;
        decode_block_value(&bytes, self.manifest.generation, block, spill_id)
    }
}

fn find_block(
    blocks: &[PropertySpillBlockDescriptor],
    spill_id: u64,
) -> Option<&PropertySpillBlockDescriptor> {
    let index = blocks.partition_point(|block| block.max_spill_id < spill_id);
    blocks
        .get(index)
        .filter(|block| block.min_spill_id <= spill_id && spill_id <= block.max_spill_id)
}

fn decode_block_value(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &PropertySpillBlockDescriptor,
    wanted: u64,
) -> Result<Option<Arc<[u8]>>, PropertySpillError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(8)? != BLOCK_HEADER {
        return Err(PropertySpillError::Corrupt(format!(
            "property spill block {} has an invalid header",
            descriptor.block_id
        )));
    }
    let stored_generation = cursor.read_u64()?;
    let block_id = cursor.read_u64()?;
    let value_count = cursor.read_u32()?;
    if stored_generation != generation.0
        || block_id != descriptor.block_id
        || value_count != descriptor.value_count
    {
        return Err(PropertySpillError::Corrupt(format!(
            "property spill block {} metadata does not match its manifest",
            descriptor.block_id
        )));
    }
    let mut first_id = None;
    let mut previous_id = None;
    let mut found = None;
    for _ in 0..value_count {
        let spill_id = cursor.read_u64()?;
        let length = usize::try_from(cursor.read_u64()?).map_err(|_| {
            PropertySpillError::Corrupt("property spill value length exceeds usize".to_string())
        })?;
        if previous_id.is_some_and(|previous| spill_id <= previous) {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill block {} ids are not strictly ordered",
                descriptor.block_id
            )));
        }
        let value = cursor.read_exact(length)?;
        if spill_id == wanted {
            found = Some(Arc::<[u8]>::from(value));
        }
        if first_id.is_none() {
            first_id = Some(spill_id);
        }
        previous_id = Some(spill_id);
    }
    if !cursor.is_empty()
        || first_id != Some(descriptor.min_spill_id)
        || previous_id != Some(descriptor.max_spill_id)
    {
        return Err(PropertySpillError::Corrupt(format!(
            "property spill block {} payload bounds are inconsistent",
            descriptor.block_id
        )));
    }
    Ok(found)
}

fn parse_u64(value: &str, name: &str) -> Result<u64, PropertySpillError> {
    value.parse().map_err(|_| {
        PropertySpillError::Corrupt(format!(
            "property spill manifest has invalid {name}: {value}"
        ))
    })
}

fn parse_u32(value: &str, name: &str) -> Result<u32, PropertySpillError> {
    value.parse().map_err(|_| {
        PropertySpillError::Corrupt(format!(
            "property spill manifest has invalid {name}: {value}"
        ))
    })
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, PropertySpillError> {
    value.ok_or_else(|| {
        PropertySpillError::Corrupt(format!("property spill manifest is missing {name}"))
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], PropertySpillError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            PropertySpillError::Corrupt("property spill cursor offset overflow".to_string())
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            PropertySpillError::Corrupt(
                "property spill block ended before its declared length".to_string(),
            )
        })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, PropertySpillError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, PropertySpillError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width u64"),
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

struct DigestState(u64);

impl DigestState {
    const fn new() -> Self {
        Self(0xcbf29ce484222325)
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    const fn finish(self) -> u64 {
        self.0
    }
}

fn write_hashed(
    writer: &mut impl Write,
    digest: &mut DigestState,
    bytes: &[u8],
) -> Result<(), PropertySpillError> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn write_double_hashed(
    writer: &mut impl Write,
    artifact_digest: &mut DigestState,
    block_digest: &mut DigestState,
    bytes: &[u8],
) -> Result<(), PropertySpillError> {
    writer.write_all(bytes)?;
    artifact_digest.update(bytes);
    block_digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_blocks_round_trip_and_fail_closed_on_corruption() {
        let root = std::env::temp_dir().join(format!(
            "skein-property-spill-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("properties.skein");
        let config = PropertySpillConfig {
            spill_threshold_bytes: NonZeroU64::new(8).unwrap(),
            target_block_bytes: NonZeroU64::new(64).unwrap(),
            max_value_bytes: NonZeroU64::new(1024).unwrap(),
        };
        let mut writer = PropertySpillWriter::create(&path, ManifestGeneration(3), config).unwrap();
        let first = writer.push(vec![1; 32]).unwrap();
        let second = writer.push(vec![2; 48]).unwrap();
        let manifest = writer.finish().unwrap();
        assert_eq!(manifest.blocks.len(), 2);
        let manifest = PropertySpillManifest::decode(&manifest.encode().unwrap()).unwrap();
        let reader = PropertySpillReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(1024)),
            StoreId(9),
            NonZeroU64::new(2048).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get(first).unwrap().unwrap().as_ref(), &[1; 32]);
        assert_eq!(reader.get(second).unwrap().unwrap().as_ref(), &[2; 48]);
        assert!(reader.get(99).unwrap().is_none());
        fs::remove_dir_all(root).unwrap();
    }
}
