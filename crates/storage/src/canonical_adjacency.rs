use crate::canonical::{decode_relationship, encode_relationship, CanonicalScanControl};
use crate::{
    durable_replace_file, AdjacencyDirection, AdjacencyLayout, ContentDigest,
    FileSegmentRangeReader, ManifestGeneration, NodeId, RelRecord, SegmentCache, SegmentRangeRead,
    SegmentReadError, SegmentReadRange, StoreId,
};
use skein_core::{RelTypeId, Value};
use skein_integrity::{Crc32cHasher, IntegrityHasher, Sha256Digest};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINADJACENCY01";
const BLOCK_HEADER: &[u8; 8] = b"SKNADJ01";
const RUN_HEADER: &[u8; 8] = b"SKNADJR1";
const MANIFEST_HEADER: &str = "SKEIN_CANONICAL_ADJACENCY_MANIFEST_V1";
const ARTIFACT_ID: u64 = 0x534b_4144_4a41_4331;
const BLOCK_ID_BASE: u64 = 1 << 63;
const ENTRY_FIXED_BYTES: u64 = 1 + 8 + 4 + 8 + 8 + 4;
const BLOCK_ENTRY_FIXED_BYTES: u64 = 8 + 8 + 4;
const BLOCK_FIXED_BYTES: u64 = 8 + 8 + 8 + 1 + 1 + 8 + 4 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalAdjacencyConfig {
    pub memory_budget_bytes: NonZeroU64,
    pub max_spill_bytes: NonZeroU64,
    pub max_spill_runs: NonZeroUsize,
    pub max_merge_fan_in: NonZeroUsize,
    pub target_block_bytes: NonZeroU64,
    pub max_record_bytes: NonZeroU64,
    pub dense_degree_threshold: NonZeroUsize,
}

impl Default for CanonicalAdjacencyConfig {
    fn default() -> Self {
        Self {
            memory_budget_bytes: NonZeroU64::new(32 * 1024 * 1024)
                .expect("default adjacency memory budget is non-zero"),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024)
                .expect("default adjacency spill budget is non-zero"),
            max_spill_runs: NonZeroUsize::new(4_096)
                .expect("default adjacency run budget is non-zero"),
            max_merge_fan_in: NonZeroUsize::new(32)
                .expect("default adjacency merge fan-in is non-zero"),
            target_block_bytes: NonZeroU64::new(1024 * 1024)
                .expect("default adjacency block size is non-zero"),
            max_record_bytes: NonZeroU64::new(16 * 1024 * 1024)
                .expect("default adjacency record size is non-zero"),
            dense_degree_threshold: NonZeroUsize::new(64)
                .expect("default dense degree threshold is non-zero"),
        }
    }
}

#[derive(Debug)]
pub enum CanonicalAdjacencyError {
    Io(std::io::Error),
    Read(SegmentReadError),
    Source(String),
    Corrupt(String),
    RecordTooLarge {
        record_bytes: u64,
        max_bytes: u64,
    },
    MemoryBudgetExceeded {
        required_bytes: u64,
        max_bytes: u64,
    },
    SpillBudgetExceeded {
        required_bytes: u64,
        max_bytes: u64,
    },
    SpillRunBudgetExceeded {
        required_runs: usize,
        max_runs: usize,
    },
    BlockTooLarge {
        block_bytes: u64,
        max_bytes: u64,
    },
}

impl Display for CanonicalAdjacencyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Source(message) | Self::Corrupt(message) => formatter.write_str(message),
            Self::RecordTooLarge {
                record_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency record uses {record_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::MemoryBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency build requires {required_bytes} resident bytes, exceeding {max_bytes}"
            ),
            Self::SpillBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency build requires {required_bytes} spill bytes, exceeding {max_bytes}"
            ),
            Self::SpillRunBudgetExceeded {
                required_runs,
                max_runs,
            } => write!(
                formatter,
                "canonical adjacency build requires {required_runs} spill runs, exceeding {max_runs}"
            ),
            Self::BlockTooLarge {
                block_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical adjacency block uses {block_bytes} bytes, exceeding {max_bytes}"
            ),
        }
    }
}

impl Error for CanonicalAdjacencyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CanonicalAdjacencyError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for CanonicalAdjacencyError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAdjacencyBlockDescriptor {
    pub block_id: u64,
    pub direction: AdjacencyDirection,
    pub layout: AdjacencyLayout,
    pub endpoint: NodeId,
    pub rel_type: RelTypeId,
    pub min_neighbor: NodeId,
    pub max_neighbor: NodeId,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub record_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAdjacencyManifest {
    pub generation: ManifestGeneration,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub artifact_sha256: Sha256Digest,
    pub relationship_count: u64,
    pub entry_count: u64,
    pub blocks: Vec<CanonicalAdjacencyBlockDescriptor>,
}

impl CanonicalAdjacencyManifest {
    pub fn validate(&self) -> Result<(), CanonicalAdjacencyError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency manifest has an unsupported artifact id".to_string(),
            ));
        }
        if self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8 {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency artifact is shorter than its header".to_string(),
            ));
        }
        if self.entry_count != self.relationship_count.saturating_mul(2) {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency entry count is not twice its relationship count".to_string(),
            ));
        }
        let mut previous_end = ARTIFACT_HEADER.len() as u64 + 8;
        let mut previous_key = None;
        let mut total_entries = 0u64;
        for block in &self.blocks {
            if block.block_id < BLOCK_ID_BASE || block.record_count == 0 {
                return Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency block {} has invalid identity or cardinality",
                    block.block_id
                )));
            }
            if block.min_neighbor > block.max_neighbor || block.offset < previous_end {
                return Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency block {} has invalid bounds",
                    block.block_id
                )));
            }
            let key = descriptor_key(block);
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency blocks are not strictly ordered".to_string(),
                ));
            }
            previous_key = Some(key);
            previous_end = block
                .offset
                .checked_add(block.length.get())
                .ok_or_else(|| {
                    CanonicalAdjacencyError::Corrupt(
                        "canonical adjacency block range overflows u64".to_string(),
                    )
                })?;
            if previous_end > self.artifact_len {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency block exceeds its artifact".to_string(),
                ));
            }
            total_entries = total_entries.saturating_add(u64::from(block.record_count));
        }
        if total_entries != self.entry_count {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency manifest counts {total_entries} entries but declares {}",
                self.entry_count
            )));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, CanonicalAdjacencyError> {
        self.validate()?;
        let mut output = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nartifact_sha256\t{}\nrelationship_count\t{}\nentry_count\t{}\n",
            self.generation.0,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.artifact_sha256,
            self.relationship_count,
            self.entry_count
        );
        for block in &self.blocks {
            output.push_str(&format!(
                "block\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                block.block_id,
                direction_tag(block.direction),
                layout_tag(block.layout),
                block.endpoint.0,
                block.rel_type.0,
                block.min_neighbor.0,
                block.max_neighbor.0,
                block.offset,
                block.length.get(),
                block.content_digest.0,
                block.record_count
            ));
        }
        Ok(output)
    }

    pub fn decode(encoded: &str) -> Result<Self, CanonicalAdjacencyError> {
        let mut generation = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut artifact_sha256 = None;
        let mut relationship_count = None;
        let mut entry_count = None;
        let mut blocks = Vec::new();
        for (line_number, line) in encoded.lines().enumerate() {
            if line_number == 0 {
                if line != MANIFEST_HEADER {
                    return Err(CanonicalAdjacencyError::Corrupt(
                        "canonical adjacency manifest has an invalid header".to_string(),
                    ));
                }
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
                ["artifact_sha256", value] => {
                    artifact_sha256 = Some(value.parse().map_err(|error| {
                        CanonicalAdjacencyError::Corrupt(format!(
                            "invalid artifact SHA-256 digest: {error}"
                        ))
                    })?)
                }
                ["relationship_count", value] => {
                    relationship_count = Some(parse_u64(value, "relationship count")?)
                }
                ["entry_count", value] => entry_count = Some(parse_u64(value, "entry count")?),
                ["block", block_id, direction, layout, endpoint, rel_type, min_neighbor, max_neighbor, offset, length, digest, record_count] => {
                    blocks.push(CanonicalAdjacencyBlockDescriptor {
                        block_id: parse_u64(block_id, "block id")?,
                        direction: direction_from_tag(parse_u64(direction, "direction")? as u8)?,
                        layout: layout_from_tag(parse_u64(layout, "layout")? as u8)?,
                        endpoint: NodeId(parse_u64(endpoint, "endpoint")?),
                        rel_type: RelTypeId(parse_u32(rel_type, "relationship type")?),
                        min_neighbor: NodeId(parse_u64(min_neighbor, "minimum neighbor")?),
                        max_neighbor: NodeId(parse_u64(max_neighbor, "maximum neighbor")?),
                        offset: parse_u64(offset, "block offset")?,
                        length: NonZeroU64::new(parse_u64(length, "block length")?).ok_or_else(
                            || {
                                CanonicalAdjacencyError::Corrupt(
                                    "canonical adjacency block length is zero".to_string(),
                                )
                            },
                        )?,
                        content_digest: ContentDigest(parse_u64(digest, "block digest")?),
                        record_count: parse_u32(record_count, "block record count")?,
                    })
                }
                _ => {
                    return Err(CanonicalAdjacencyError::Corrupt(format!(
                        "invalid canonical adjacency manifest line: {line}"
                    )));
                }
            }
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            artifact_sha256: required(artifact_sha256, "artifact SHA-256 digest")?,
            relationship_count: required(relationship_count, "relationship count")?,
            entry_count: required(entry_count, "entry count")?,
            blocks,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalAdjacencyBuildReport {
    pub relationship_count: u64,
    pub entry_count: u64,
    pub sparse_block_count: u64,
    pub dense_block_count: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_resident_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct CanonicalAdjacencyWriteOutput {
    pub manifest: CanonicalAdjacencyManifest,
    pub report: CanonicalAdjacencyBuildReport,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalAdjacencyEntry {
    Inline(RelRecord),
    CanonicalReference { relationship_id: crate::RelId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey {
    direction: u8,
    endpoint: u64,
    rel_type: u32,
    neighbor: u64,
    rel_id: u64,
}

#[derive(Debug)]
struct EncodedEntry {
    key: EntryKey,
    payload: Vec<u8>,
}

impl EncodedEntry {
    fn run_encoded_len(&self) -> u64 {
        ENTRY_FIXED_BYTES.saturating_add(self.payload.len() as u64)
    }

    fn block_encoded_len(&self) -> u64 {
        BLOCK_ENTRY_FIXED_BYTES.saturating_add(self.payload.len() as u64)
    }

    fn resident_bytes(&self) -> u64 {
        self.run_encoded_len()
            .saturating_add(std::mem::size_of::<Self>() as u64)
    }
}

pub struct CanonicalAdjacencyWriter {
    config: CanonicalAdjacencyConfig,
}

impl CanonicalAdjacencyWriter {
    pub const fn new(config: CanonicalAdjacencyConfig) -> Self {
        Self { config }
    }

    pub fn write_fallible<R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        relationships: R,
    ) -> Result<CanonicalAdjacencyWriteOutput, CanonicalAdjacencyError>
    where
        R: IntoIterator<Item = Result<RelRecord, CanonicalAdjacencyError>>,
    {
        let mut runs = SpillRuns::new(path, generation, self.config);
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0u64;
        let mut relationship_count = 0u64;
        let mut peak_resident_bytes = 0u64;
        for relationship in relationships {
            let relationship = relationship?;
            let payload = if estimated_relationship_payload_bytes(&relationship)
                <= self.config.max_record_bytes.get()
            {
                let payload = encode_relationship(&relationship)
                    .map_err(|error| CanonicalAdjacencyError::Source(error.to_string()))?;
                if payload.len() as u64 <= self.config.max_record_bytes.get() {
                    payload
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let outgoing = EncodedEntry {
                key: EntryKey {
                    direction: direction_tag(AdjacencyDirection::Outgoing),
                    endpoint: relationship.source.0,
                    rel_type: relationship.rel_type.0,
                    neighbor: relationship.target.0,
                    rel_id: relationship.id.0,
                },
                payload: payload.clone(),
            };
            let incoming = EncodedEntry {
                key: EntryKey {
                    direction: direction_tag(AdjacencyDirection::Incoming),
                    endpoint: relationship.target.0,
                    rel_type: relationship.rel_type.0,
                    neighbor: relationship.source.0,
                    rel_id: relationship.id.0,
                },
                payload,
            };
            for entry in [outgoing, incoming] {
                let entry_bytes = entry.resident_bytes();
                if entry_bytes > self.config.memory_budget_bytes.get() {
                    return Err(CanonicalAdjacencyError::MemoryBudgetExceeded {
                        required_bytes: entry_bytes,
                        max_bytes: self.config.memory_budget_bytes.get(),
                    });
                }
                if !chunk.is_empty()
                    && chunk_bytes.saturating_add(entry_bytes)
                        > self.config.memory_budget_bytes.get()
                {
                    runs.spill(&mut chunk)?;
                    chunk_bytes = 0;
                }
                chunk_bytes = chunk_bytes.saturating_add(entry_bytes);
                peak_resident_bytes = peak_resident_bytes.max(chunk_bytes);
                chunk.push(entry);
            }
            relationship_count = relationship_count.saturating_add(1);
        }
        if !chunk.is_empty() {
            runs.spill(&mut chunk)?;
        }
        runs.compact()?;

        let tmp_path = path.with_extension("skein.tmp");
        let result = self.merge_runs(
            &tmp_path,
            generation,
            relationship_count,
            peak_resident_bytes,
            &runs,
        );
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        durable_replace_file(&tmp_path, path)?;
        Ok(output)
    }

    fn merge_runs(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        relationship_count: u64,
        peak_resident_bytes: u64,
        runs: &SpillRuns,
    ) -> Result<CanonicalAdjacencyWriteOutput, CanonicalAdjacencyError> {
        let file = File::create(path)?;
        let mut artifact = ArtifactBuilder::new(file, generation, self.config)?;
        let mut readers = runs
            .paths
            .iter()
            .map(|path| RunReader::open(path))
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = Vec::with_capacity(readers.len());
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            let key = reader.next_key()?;
            if let Some(key) = key {
                heap.push(Reverse((key, index)));
            }
            current.push(key);
        }
        let mut previous_key = None;
        while let Some(Reverse((key, run_index))) = heap.pop() {
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency spill merge encountered duplicate or unordered keys"
                        .to_string(),
                ));
            }
            if current[run_index] != Some(key) {
                return Err(CanonicalAdjacencyError::Corrupt(
                    "canonical adjacency spill heap does not match its reader".to_string(),
                ));
            }
            let entry = readers[run_index].take_entry()?;
            artifact.push(entry)?;
            previous_key = Some(key);
            current[run_index] = readers[run_index].next_key()?;
            if let Some(next) = current[run_index] {
                heap.push(Reverse((next, run_index)));
            }
        }
        let (manifest, sparse_block_count, dense_block_count) =
            artifact.finish(relationship_count)?;
        Ok(CanonicalAdjacencyWriteOutput {
            manifest,
            report: CanonicalAdjacencyBuildReport {
                relationship_count,
                entry_count: relationship_count.saturating_mul(2),
                sparse_block_count,
                dense_block_count,
                spill_run_count: runs.next_run_sequence,
                spill_bytes: runs.spill_bytes,
                peak_resident_bytes,
            },
        })
    }
}

struct SpillRuns {
    prefix: PathBuf,
    generation: ManifestGeneration,
    config: CanonicalAdjacencyConfig,
    paths: Vec<PathBuf>,
    spill_bytes: u64,
    next_run_sequence: usize,
}

impl SpillRuns {
    fn new(path: &Path, generation: ManifestGeneration, config: CanonicalAdjacencyConfig) -> Self {
        Self {
            prefix: path.to_path_buf(),
            generation,
            config,
            paths: Vec::new(),
            spill_bytes: 0,
            next_run_sequence: 0,
        }
    }

    fn spill(&mut self, entries: &mut Vec<EncodedEntry>) -> Result<(), CanonicalAdjacencyError> {
        let required_runs = self.paths.len().saturating_add(1);
        if required_runs > self.config.max_spill_runs.get() {
            return Err(CanonicalAdjacencyError::SpillRunBudgetExceeded {
                required_runs,
                max_runs: self.config.max_spill_runs.get(),
            });
        }
        entries.sort_unstable_by_key(|entry| entry.key);
        let run_bytes = RUN_HEADER.len() as u64
            + entries
                .iter()
                .map(EncodedEntry::run_encoded_len)
                .fold(0u64, u64::saturating_add);
        let required_bytes = self.spill_bytes.saturating_add(run_bytes);
        if required_bytes > self.config.max_spill_bytes.get() {
            return Err(CanonicalAdjacencyError::SpillBudgetExceeded {
                required_bytes,
                max_bytes: self.config.max_spill_bytes.get(),
            });
        }
        let path = self.next_path();
        let mut writer = BufWriter::new(File::create(&path)?);
        writer.write_all(RUN_HEADER)?;
        for entry in entries.iter() {
            write_entry(&mut writer, entry)?;
        }
        writer.flush()?;
        self.paths.push(path);
        self.spill_bytes = required_bytes;
        entries.clear();
        Ok(())
    }

    fn compact(&mut self) -> Result<(), CanonicalAdjacencyError> {
        let fan_in = self.config.max_merge_fan_in.get();
        if fan_in < 2 {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency merge fan-in must be at least two".to_string(),
            ));
        }
        while self.paths.len() > fan_in {
            let old_paths = std::mem::take(&mut self.paths);
            let mut merged_paths = Vec::with_capacity(old_paths.len().div_ceil(fan_in));
            for group in old_paths.chunks(fan_in) {
                let path = self.next_path();
                let bytes = match merge_run_group(group, &path) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = fs::remove_file(&path);
                        for stale in old_paths.iter().chain(merged_paths.iter()) {
                            let _ = fs::remove_file(stale);
                        }
                        return Err(error);
                    }
                };
                let required_bytes = self.spill_bytes.saturating_add(bytes);
                if required_bytes > self.config.max_spill_bytes.get() {
                    let _ = fs::remove_file(&path);
                    for stale in old_paths.iter().chain(merged_paths.iter()) {
                        let _ = fs::remove_file(stale);
                    }
                    return Err(CanonicalAdjacencyError::SpillBudgetExceeded {
                        required_bytes,
                        max_bytes: self.config.max_spill_bytes.get(),
                    });
                }
                self.spill_bytes = required_bytes;
                merged_paths.push(path);
                for source in group {
                    fs::remove_file(source)?;
                }
            }
            self.paths = merged_paths;
        }
        Ok(())
    }

    fn next_path(&mut self) -> PathBuf {
        let sequence = self.next_run_sequence;
        self.next_run_sequence = self.next_run_sequence.saturating_add(1);
        self.prefix.with_file_name(format!(
            ".adjacency.{}.run.{sequence}.tmp",
            self.generation.0
        ))
    }
}

impl Drop for SpillRuns {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

struct RunReader {
    reader: BufReader<File>,
    pending: Option<(EntryKey, usize)>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self, CanonicalAdjacencyError> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        if &header != RUN_HEADER {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill run has an invalid header".to_string(),
            ));
        }
        Ok(Self {
            reader,
            pending: None,
        })
    }

    fn next_key(&mut self) -> Result<Option<EntryKey>, CanonicalAdjacencyError> {
        if self.pending.is_some() {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill reader advanced before consuming its payload"
                    .to_string(),
            ));
        }
        let mut direction = [0u8; 1];
        match self.reader.read(&mut direction)? {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one byte read buffer"),
        }
        let endpoint = read_u64(&mut self.reader)?;
        let rel_type = read_u32(&mut self.reader)?;
        let neighbor = read_u64(&mut self.reader)?;
        let rel_id = read_u64(&mut self.reader)?;
        let payload_len = read_u32(&mut self.reader)? as usize;
        let key = EntryKey {
            direction: direction[0],
            endpoint,
            rel_type,
            neighbor,
            rel_id,
        };
        self.pending = Some((key, payload_len));
        Ok(Some(key))
    }

    fn take_entry(&mut self) -> Result<EncodedEntry, CanonicalAdjacencyError> {
        let (key, payload_len) = self.pending.take().ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill reader has no pending entry".to_string(),
            )
        })?;
        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload)?;
        Ok(EncodedEntry { key, payload })
    }
}

fn merge_run_group(
    sources: &[PathBuf],
    destination: &Path,
) -> Result<u64, CanonicalAdjacencyError> {
    let mut readers = sources
        .iter()
        .map(|path| RunReader::open(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut current = Vec::with_capacity(readers.len());
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        let key = reader.next_key()?;
        if let Some(key) = key {
            heap.push(Reverse((key, index)));
        }
        current.push(key);
    }
    let mut writer = BufWriter::new(File::create(destination)?);
    writer.write_all(RUN_HEADER)?;
    let mut bytes = RUN_HEADER.len() as u64;
    let mut previous_key = None;
    while let Some(Reverse((key, run_index))) = heap.pop() {
        if current[run_index] != Some(key) || previous_key.is_some_and(|previous| previous >= key) {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency spill compaction encountered unordered keys".to_string(),
            ));
        }
        let entry = readers[run_index].take_entry()?;
        write_entry(&mut writer, &entry)?;
        bytes = bytes.saturating_add(entry.run_encoded_len());
        previous_key = Some(key);
        current[run_index] = readers[run_index].next_key()?;
        if let Some(next) = current[run_index] {
            heap.push(Reverse((next, run_index)));
        }
    }
    writer.flush()?;
    Ok(bytes)
}

struct ArtifactBuilder {
    writer: BufWriter<File>,
    digest: IntegrityHasher,
    generation: ManifestGeneration,
    config: CanonicalAdjacencyConfig,
    artifact_len: u64,
    blocks: Vec<CanonicalAdjacencyBlockDescriptor>,
    group: Option<PendingGroup>,
    next_block_id: u64,
    entry_count: u64,
    sparse_block_count: u64,
    dense_block_count: u64,
}

impl ArtifactBuilder {
    fn new(
        file: File,
        generation: ManifestGeneration,
        config: CanonicalAdjacencyConfig,
    ) -> Result<Self, CanonicalAdjacencyError> {
        let mut writer = BufWriter::new(file);
        let mut digest = IntegrityHasher::new();
        write_hashed(&mut writer, &mut digest, ARTIFACT_HEADER)?;
        write_hashed(&mut writer, &mut digest, &generation.0.to_le_bytes())?;
        Ok(Self {
            writer,
            digest,
            generation,
            config,
            artifact_len: ARTIFACT_HEADER.len() as u64 + 8,
            blocks: Vec::new(),
            group: None,
            next_block_id: BLOCK_ID_BASE,
            entry_count: 0,
            sparse_block_count: 0,
            dense_block_count: 0,
        })
    }

    fn push(&mut self, entry: EncodedEntry) -> Result<(), CanonicalAdjacencyError> {
        let key = GroupKey::from_entry(&entry)?;
        if self.group.as_ref().is_some_and(|group| group.key != key) {
            self.finish_group()?;
        }
        if self.group.is_none() {
            self.group = Some(PendingGroup::new(key));
        }
        let threshold = self.config.dense_degree_threshold.get();
        let mut group = self.group.take().expect("adjacency group was initialized");
        if !group.dense {
            let entry_bytes = entry.resident_bytes();
            let exceeds_group_budget = !group.buffer.is_empty()
                && group.buffer_bytes.saturating_add(entry_bytes)
                    > self.config.memory_budget_bytes.get();
            if exceeds_group_budget {
                group.dense = true;
                for buffered in std::mem::take(&mut group.buffer) {
                    self.push_dense_entry(&mut group, buffered)?;
                }
                group.buffer_bytes = 0;
                self.push_dense_entry(&mut group, entry)?;
            } else {
                group.buffer_bytes = group.buffer_bytes.saturating_add(entry_bytes);
                group.buffer.push(entry);
                if group.buffer.len() >= threshold {
                    group.dense = true;
                    for buffered in std::mem::take(&mut group.buffer) {
                        self.push_dense_entry(&mut group, buffered)?;
                    }
                    group.buffer_bytes = 0;
                }
            }
        } else {
            self.push_dense_entry(&mut group, entry)?;
        }
        self.group = Some(group);
        self.entry_count = self.entry_count.saturating_add(1);
        Ok(())
    }

    fn push_dense_entry(
        &mut self,
        group: &mut PendingGroup,
        entry: EncodedEntry,
    ) -> Result<(), CanonicalAdjacencyError> {
        let prospective = group
            .block_bytes
            .saturating_add(entry.block_encoded_len())
            .saturating_add(BLOCK_FIXED_BYTES);
        let target = self
            .config
            .target_block_bytes
            .get()
            .min(self.config.memory_budget_bytes.get());
        if !group.block.is_empty() && prospective > target {
            self.flush_group_block(group, AdjacencyLayout::Dense)?;
        }
        group.block_bytes = group.block_bytes.saturating_add(entry.block_encoded_len());
        group.block.push(entry);
        Ok(())
    }

    fn finish_group(&mut self) -> Result<(), CanonicalAdjacencyError> {
        let Some(mut group) = self.group.take() else {
            return Ok(());
        };
        if group.dense {
            self.flush_group_block(&mut group, AdjacencyLayout::Dense)?;
        } else if !group.buffer.is_empty() {
            group.block_bytes = group
                .buffer
                .iter()
                .map(EncodedEntry::block_encoded_len)
                .fold(0u64, u64::saturating_add);
            group.block = std::mem::take(&mut group.buffer);
            group.buffer_bytes = 0;
            self.flush_group_block(&mut group, AdjacencyLayout::Sparse)?;
        }
        Ok(())
    }

    fn flush_group_block(
        &mut self,
        group: &mut PendingGroup,
        layout: AdjacencyLayout,
    ) -> Result<(), CanonicalAdjacencyError> {
        if group.block.is_empty() {
            return Ok(());
        }
        let record_count = u32::try_from(group.block.len()).map_err(|_| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency block record count exceeds u32".to_string(),
            )
        })?;
        let block_bytes = BLOCK_FIXED_BYTES.saturating_add(group.block_bytes);
        let hard_max = self.config.target_block_bytes.get().max(
            self.config
                .max_record_bytes
                .get()
                .saturating_add(BLOCK_FIXED_BYTES),
        );
        if block_bytes > hard_max {
            return Err(CanonicalAdjacencyError::BlockTooLarge {
                block_bytes,
                max_bytes: hard_max,
            });
        }
        let min_neighbor = NodeId(
            group
                .block
                .first()
                .expect("flushed adjacency block is non-empty")
                .key
                .neighbor,
        );
        let max_neighbor = NodeId(
            group
                .block
                .last()
                .expect("flushed adjacency block is non-empty")
                .key
                .neighbor,
        );
        let length = NonZeroU64::new(block_bytes).expect("canonical adjacency block is non-empty");
        let mut block_digest = Crc32cHasher::new();
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            BLOCK_HEADER,
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &self.generation.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &self.next_block_id.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &[direction_tag(group.key.direction), layout_tag(layout)],
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &group.key.endpoint.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &group.key.rel_type.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.writer,
            &mut self.digest,
            &mut block_digest,
            &record_count.to_le_bytes(),
        )?;
        for entry in &group.block {
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &entry.key.neighbor.to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &entry.key.rel_id.to_le_bytes(),
            )?;
            let payload_len = u32::try_from(entry.payload.len()).map_err(|_| {
                CanonicalAdjacencyError::RecordTooLarge {
                    record_bytes: entry.payload.len() as u64,
                    max_bytes: u64::from(u32::MAX),
                }
            })?;
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &payload_len.to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.digest,
                &mut block_digest,
                &entry.payload,
            )?;
        }
        let descriptor = CanonicalAdjacencyBlockDescriptor {
            block_id: self.next_block_id,
            direction: group.key.direction,
            layout,
            endpoint: group.key.endpoint,
            rel_type: group.key.rel_type,
            min_neighbor,
            max_neighbor,
            offset: self.artifact_len,
            length,
            content_digest: ContentDigest(block_digest.finish()),
            record_count,
        };
        self.artifact_len = self.artifact_len.saturating_add(length.get());
        self.next_block_id = self.next_block_id.saturating_add(1);
        match layout {
            AdjacencyLayout::Sparse => {
                self.sparse_block_count = self.sparse_block_count.saturating_add(1)
            }
            AdjacencyLayout::Dense => {
                self.dense_block_count = self.dense_block_count.saturating_add(1)
            }
        }
        self.blocks.push(descriptor);
        group.block.clear();
        group.block_bytes = 0;
        Ok(())
    }

    fn finish(
        mut self,
        relationship_count: u64,
    ) -> Result<(CanonicalAdjacencyManifest, u64, u64), CanonicalAdjacencyError> {
        self.finish_group()?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let artifact_integrity = self.digest.finish();
        let manifest = CanonicalAdjacencyManifest {
            generation: self.generation,
            artifact_id: ARTIFACT_ID,
            artifact_len: self.artifact_len,
            artifact_digest: ContentDigest(artifact_integrity.crc32c.as_u64()),
            artifact_sha256: artifact_integrity.sha256,
            relationship_count,
            entry_count: self.entry_count,
            blocks: self.blocks,
        };
        manifest.validate()?;
        Ok((manifest, self.sparse_block_count, self.dense_block_count))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GroupKey {
    direction: AdjacencyDirection,
    endpoint: NodeId,
    rel_type: RelTypeId,
}

impl GroupKey {
    fn from_entry(entry: &EncodedEntry) -> Result<Self, CanonicalAdjacencyError> {
        Ok(Self {
            direction: direction_from_tag(entry.key.direction)?,
            endpoint: NodeId(entry.key.endpoint),
            rel_type: RelTypeId(entry.key.rel_type),
        })
    }
}

struct PendingGroup {
    key: GroupKey,
    dense: bool,
    buffer: Vec<EncodedEntry>,
    buffer_bytes: u64,
    block: Vec<EncodedEntry>,
    block_bytes: u64,
}

impl PendingGroup {
    fn new(key: GroupKey) -> Self {
        Self {
            key,
            dense: false,
            buffer: Vec::new(),
            buffer_bytes: 0,
            block: Vec::new(),
            block_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalAdjacencyReadReport {
    pub blocks_considered: u64,
    pub blocks_read: u64,
    pub bytes_read: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub records_decoded: u64,
    pub sparse_blocks_read: u64,
    pub dense_blocks_read: u64,
}

#[derive(Debug, Clone)]
pub struct CanonicalAdjacencyReader {
    path: PathBuf,
    manifest: CanonicalAdjacencyManifest,
    range_reader: FileSegmentRangeReader,
    max_block_bytes: NonZeroU64,
}

impl CanonicalAdjacencyReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: CanonicalAdjacencyManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_block_bytes: NonZeroU64,
    ) -> Result<Self, CanonicalAdjacencyError> {
        manifest.validate()?;
        let path = path.into();
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(CanonicalAdjacencyError::Corrupt(
                "canonical adjacency artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        for block in &manifest.blocks {
            if block.length.get() > max_block_bytes.get() {
                return Err(CanonicalAdjacencyError::BlockTooLarge {
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

    pub fn manifest(&self) -> &CanonicalAdjacencyManifest {
        &self.manifest
    }

    pub fn estimate_endpoint_entries(
        &self,
        endpoint: NodeId,
        direction: AdjacencyDirection,
        rel_type: Option<RelTypeId>,
    ) -> u64 {
        let direction = direction_tag(direction);
        let start = self.manifest.blocks.partition_point(|block| {
            (direction_tag(block.direction), block.endpoint.0) < (direction, endpoint.0)
        });
        let end = self.manifest.blocks.partition_point(|block| {
            (direction_tag(block.direction), block.endpoint.0) <= (direction, endpoint.0)
        });
        self.manifest.blocks[start..end]
            .iter()
            .filter(|block| rel_type.is_none_or(|expected| block.rel_type == expected))
            .map(|block| u64::from(block.record_count))
            .sum()
    }

    pub fn scan_endpoint_control(
        &self,
        endpoint: NodeId,
        direction: AdjacencyDirection,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> Result<CanonicalScanControl, CanonicalAdjacencyError>,
    ) -> Result<(CanonicalAdjacencyReadReport, CanonicalScanControl), CanonicalAdjacencyError> {
        self.scan_endpoint_entries_control(endpoint, direction, rel_type, |entry| match entry {
            CanonicalAdjacencyEntry::Inline(relationship) => consumer(relationship),
            CanonicalAdjacencyEntry::CanonicalReference { relationship_id } => {
                Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency relationship {} requires canonical resolution",
                    relationship_id.0
                )))
            }
        })
    }

    pub fn scan_endpoint_entries_control(
        &self,
        endpoint: NodeId,
        direction: AdjacencyDirection,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(
            CanonicalAdjacencyEntry,
        ) -> Result<CanonicalScanControl, CanonicalAdjacencyError>,
    ) -> Result<(CanonicalAdjacencyReadReport, CanonicalScanControl), CanonicalAdjacencyError> {
        let direction = direction_tag(direction);
        let start = self.manifest.blocks.partition_point(|block| {
            (direction_tag(block.direction), block.endpoint.0) < (direction, endpoint.0)
        });
        let end = self.manifest.blocks.partition_point(|block| {
            (direction_tag(block.direction), block.endpoint.0) <= (direction, endpoint.0)
        });
        let mut report = CanonicalAdjacencyReadReport::default();
        for block in &self.manifest.blocks[start..end] {
            report.blocks_considered = report.blocks_considered.saturating_add(1);
            if rel_type.is_some_and(|expected| block.rel_type != expected) {
                continue;
            }
            let read = self.read_block(block)?;
            report.blocks_read = report.blocks_read.saturating_add(1);
            report.bytes_read = report.bytes_read.saturating_add(read.payload.len() as u64);
            report.cache_hits = report.cache_hits.saturating_add(u64::from(read.cache_hit));
            report.cache_misses = report
                .cache_misses
                .saturating_add(u64::from(read.cache_miss));
            match block.layout {
                AdjacencyLayout::Sparse => {
                    report.sparse_blocks_read = report.sparse_blocks_read.saturating_add(1)
                }
                AdjacencyLayout::Dense => {
                    report.dense_blocks_read = report.dense_blocks_read.saturating_add(1)
                }
            }
            let mut control = CanonicalScanControl::Continue;
            decode_block(
                &read.payload,
                self.manifest.generation,
                block,
                |relationship| {
                    if control == CanonicalScanControl::Stop {
                        return Ok(());
                    }
                    control = consumer(relationship)?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    Ok(())
                },
            )?;
            if control == CanonicalScanControl::Stop {
                return Ok((report, control));
            }
        }
        Ok((report, CanonicalScanControl::Continue))
    }

    fn read_block(
        &self,
        block: &CanonicalAdjacencyBlockDescriptor,
    ) -> Result<SegmentRangeRead, CanonicalAdjacencyError> {
        if block.length.get() > self.max_block_bytes.get() {
            return Err(CanonicalAdjacencyError::BlockTooLarge {
                block_bytes: block.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        self.range_reader
            .read_range_with_report(&SegmentReadRange {
                artifact_id: self.manifest.artifact_id,
                segment_ids: vec![block.block_id],
                offset: block.offset,
                length: block.length,
                content_digest: Some(block.content_digest),
            })
            .map_err(CanonicalAdjacencyError::from)
    }
}

fn decode_block(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &CanonicalAdjacencyBlockDescriptor,
    mut consumer: impl FnMut(CanonicalAdjacencyEntry) -> Result<(), CanonicalAdjacencyError>,
) -> Result<(), CanonicalAdjacencyError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(8)? != BLOCK_HEADER {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency block {} has an invalid header",
            descriptor.block_id
        )));
    }
    let stored_generation = cursor.read_u64()?;
    let block_id = cursor.read_u64()?;
    let direction = direction_from_tag(cursor.read_u8()?)?;
    let layout = layout_from_tag(cursor.read_u8()?)?;
    let endpoint = NodeId(cursor.read_u64()?);
    let rel_type = RelTypeId(cursor.read_u32()?);
    let record_count = cursor.read_u32()?;
    if stored_generation != generation.0
        || block_id != descriptor.block_id
        || direction != descriptor.direction
        || layout != descriptor.layout
        || endpoint != descriptor.endpoint
        || rel_type != descriptor.rel_type
        || record_count != descriptor.record_count
    {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency block {} metadata does not match its manifest",
            descriptor.block_id
        )));
    }
    let mut previous = None;
    for _ in 0..record_count {
        let neighbor = NodeId(cursor.read_u64()?);
        let rel_id = cursor.read_u64()?;
        let payload_len = cursor.read_u32()? as usize;
        let payload = cursor.read_exact(payload_len)?;
        let key = (neighbor.0, rel_id);
        if previous.is_some_and(|previous| previous >= key) {
            return Err(CanonicalAdjacencyError::Corrupt(format!(
                "canonical adjacency block {} records are not strictly ordered",
                descriptor.block_id
            )));
        }
        if payload.is_empty() {
            consumer(CanonicalAdjacencyEntry::CanonicalReference {
                relationship_id: crate::RelId(rel_id),
            })?;
        } else {
            let relationship = decode_relationship(rel_id, payload)
                .map_err(|error| CanonicalAdjacencyError::Corrupt(error.to_string()))?;
            let (actual_endpoint, actual_neighbor) = match direction {
                AdjacencyDirection::Outgoing => (relationship.source, relationship.target),
                AdjacencyDirection::Incoming => (relationship.target, relationship.source),
            };
            if actual_endpoint != endpoint
                || actual_neighbor != neighbor
                || relationship.rel_type != rel_type
            {
                return Err(CanonicalAdjacencyError::Corrupt(format!(
                    "canonical adjacency block {} relationship {} does not match its key",
                    descriptor.block_id, rel_id
                )));
            }
            consumer(CanonicalAdjacencyEntry::Inline(relationship))?;
        }
        previous = Some(key);
    }
    if !cursor.is_empty()
        || previous.is_none()
        || previous.map(|value| NodeId(value.0)) != Some(descriptor.max_neighbor)
    {
        return Err(CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency block {} payload bounds are inconsistent",
            descriptor.block_id
        )));
    }
    Ok(())
}

fn descriptor_key(block: &CanonicalAdjacencyBlockDescriptor) -> (u8, u64, u32, u64, u64) {
    (
        direction_tag(block.direction),
        block.endpoint.0,
        block.rel_type.0,
        block.min_neighbor.0,
        block.block_id,
    )
}

fn write_entry(
    writer: &mut impl Write,
    entry: &EncodedEntry,
) -> Result<(), CanonicalAdjacencyError> {
    writer.write_all(&[entry.key.direction])?;
    writer.write_all(&entry.key.endpoint.to_le_bytes())?;
    writer.write_all(&entry.key.rel_type.to_le_bytes())?;
    writer.write_all(&entry.key.neighbor.to_le_bytes())?;
    writer.write_all(&entry.key.rel_id.to_le_bytes())?;
    writer.write_all(
        &u32::try_from(entry.payload.len())
            .map_err(|_| CanonicalAdjacencyError::RecordTooLarge {
                record_bytes: entry.payload.len() as u64,
                max_bytes: u64::from(u32::MAX),
            })?
            .to_le_bytes(),
    )?;
    writer.write_all(&entry.payload)?;
    Ok(())
}

fn estimated_relationship_payload_bytes(relationship: &RelRecord) -> u64 {
    8u64.saturating_add(8)
        .saturating_add(4)
        .saturating_add(4)
        .saturating_add(
            relationship
                .properties
                .iter()
                .map(|(key, value)| {
                    4u64.saturating_add(key.len() as u64)
                        .saturating_add(estimated_value_bytes(value))
                })
                .fold(0u64, u64::saturating_add),
        )
}

fn estimated_value_bytes(value: &Value) -> u64 {
    match value {
        Value::Null => 1,
        Value::Bool(_) => 2,
        Value::Int(_) | Value::Float(_) => 9,
        Value::String(value) => 5u64.saturating_add(value.len() as u64),
        Value::List(values) => 5u64.saturating_add(
            values
                .iter()
                .map(estimated_value_bytes)
                .fold(0u64, u64::saturating_add),
        ),
        Value::Map(values) => 5u64.saturating_add(
            values
                .iter()
                .map(|(key, value)| {
                    4u64.saturating_add(key.len() as u64)
                        .saturating_add(estimated_value_bytes(value))
                })
                .fold(0u64, u64::saturating_add),
        ),
    }
}

fn direction_tag(direction: AdjacencyDirection) -> u8 {
    match direction {
        AdjacencyDirection::Outgoing => 1,
        AdjacencyDirection::Incoming => 2,
    }
}

fn direction_from_tag(tag: u8) -> Result<AdjacencyDirection, CanonicalAdjacencyError> {
    match tag {
        1 => Ok(AdjacencyDirection::Outgoing),
        2 => Ok(AdjacencyDirection::Incoming),
        _ => Err(CanonicalAdjacencyError::Corrupt(format!(
            "invalid canonical adjacency direction {tag}"
        ))),
    }
}

fn layout_tag(layout: AdjacencyLayout) -> u8 {
    match layout {
        AdjacencyLayout::Sparse => 1,
        AdjacencyLayout::Dense => 2,
    }
}

fn layout_from_tag(tag: u8) -> Result<AdjacencyLayout, CanonicalAdjacencyError> {
    match tag {
        1 => Ok(AdjacencyLayout::Sparse),
        2 => Ok(AdjacencyLayout::Dense),
        _ => Err(CanonicalAdjacencyError::Corrupt(format!(
            "invalid canonical adjacency layout {tag}"
        ))),
    }
}

fn parse_u64(value: &str, name: &str) -> Result<u64, CanonicalAdjacencyError> {
    value.parse().map_err(|_| {
        CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency manifest has an invalid {name}: {value}"
        ))
    })
}

fn parse_u32(value: &str, name: &str) -> Result<u32, CanonicalAdjacencyError> {
    value.parse().map_err(|_| {
        CanonicalAdjacencyError::Corrupt(format!(
            "canonical adjacency manifest has an invalid {name}: {value}"
        ))
    })
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, CanonicalAdjacencyError> {
    value.ok_or_else(|| {
        CanonicalAdjacencyError::Corrupt(format!("canonical adjacency manifest is missing {name}"))
    })
}

fn read_u32(reader: &mut impl Read) -> Result<u32, CanonicalAdjacencyError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, CanonicalAdjacencyError> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], CanonicalAdjacencyError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency cursor offset overflow".to_string(),
            )
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            CanonicalAdjacencyError::Corrupt(
                "canonical adjacency block ended before its declared length".to_string(),
            )
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, CanonicalAdjacencyError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CanonicalAdjacencyError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, CanonicalAdjacencyError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width u64"),
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn write_hashed(
    writer: &mut impl Write,
    digest: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), CanonicalAdjacencyError> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn write_double_hashed(
    writer: &mut impl Write,
    artifact_digest: &mut IntegrityHasher,
    block_digest: &mut Crc32cHasher,
    bytes: &[u8],
) -> Result<(), CanonicalAdjacencyError> {
    writer.write_all(bytes)?;
    artifact_digest.update(bytes);
    block_digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RelId;
    use std::collections::BTreeMap;

    fn test_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein-canonical-adjacency-{name}-{}-{nonce}",
            std::process::id(),
        ))
    }

    fn relationship(id: u64, source: u64, target: u64, rel_type: u32) -> RelRecord {
        RelRecord {
            id: RelId(id),
            source: NodeId(source),
            target: NodeId(target),
            rel_type: RelTypeId(rel_type),
            properties: BTreeMap::new(),
        }
    }

    #[test]
    fn external_sort_builds_sparse_and_dense_endpoint_blocks() {
        let root = test_path("sparse-dense");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("adjacency.skein");
        let config = CanonicalAdjacencyConfig {
            memory_budget_bytes: NonZeroU64::new(128).unwrap(),
            target_block_bytes: NonZeroU64::new(256).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            dense_degree_threshold: NonZeroUsize::new(4).unwrap(),
            ..CanonicalAdjacencyConfig::default()
        };
        let relationships = vec![
            relationship(7, 1, 17, 3),
            relationship(2, 1, 12, 3),
            relationship(9, 2, 19, 3),
            relationship(1, 1, 11, 3),
            relationship(5, 1, 15, 3),
        ];
        let output = CanonicalAdjacencyWriter::new(config)
            .write_fallible(
                &path,
                ManifestGeneration(4),
                relationships.into_iter().map(Ok),
            )
            .unwrap();
        assert!(output.report.spill_run_count > 1);
        assert!(output.report.sparse_block_count > 0);
        assert!(output.report.dense_block_count > 1);
        let manifest =
            CanonicalAdjacencyManifest::decode(&output.manifest.encode().unwrap()).unwrap();
        let cache = Arc::new(SegmentCache::new(1024 * 1024));
        let reader = CanonicalAdjacencyReader::open(
            &path,
            manifest,
            cache,
            StoreId(1),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let mut ids = Vec::new();
        let (report, control) = reader
            .scan_endpoint_control(
                NodeId(1),
                AdjacencyDirection::Outgoing,
                Some(RelTypeId(3)),
                |relationship| {
                    ids.push(relationship.id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(ids, vec![1, 2, 5, 7]);
        assert_eq!(report.records_decoded, 4);
        assert!(report.dense_blocks_read > 1);
        assert!(fs::read_dir(&root).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".run.")));
        fs::remove_dir_all(root).unwrap();
    }
}
