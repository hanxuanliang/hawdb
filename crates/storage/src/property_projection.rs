use crate::canonical::{
    decode_standalone_value, encode_standalone_value, CanonicalScanControl, CanonicalSegmentError,
};
use crate::{
    content_digest, ContentDigest, FileSegmentRangeReader, ManifestGeneration, NodeId, NodeRecord,
    SegmentCache, SegmentRangeReader, SegmentReadError, SegmentReadRange, StoreId,
};
use skein_core::{LabelId, Value};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINPROPINDEX01";
const BLOCK_HEADER: &[u8; 8] = b"SKNIDX01";
const RUN_HEADER: &[u8; 8] = b"SKNIDXR1";
const MANIFEST_HEADER: &str = "SKEIN_PROPERTY_PROJECTION_MANIFEST_V1";
const ARTIFACT_ID: u64 = 0x534b_5052_4944_5831;
const BLOCK_ID_BASE: u64 = 3 << 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PersistentPropertyProjectionKind {
    Range,
    FullText,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PersistentPropertyProjectionDefinition {
    pub label_id: LabelId,
    pub property: String,
    pub kind: PersistentPropertyProjectionKind,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistentPropertyProjectionConfig {
    pub memory_budget_bytes: NonZeroU64,
    pub max_spill_bytes: NonZeroU64,
    pub max_spill_runs: NonZeroUsize,
    pub max_merge_fan_in: NonZeroUsize,
    pub target_block_bytes: NonZeroU64,
    pub max_index_key_bytes: NonZeroU64,
    pub max_generated_entries: NonZeroU64,
}

impl Default for PersistentPropertyProjectionConfig {
    fn default() -> Self {
        Self {
            memory_budget_bytes: NonZeroU64::new(32 * 1024 * 1024)
                .expect("default property projection memory budget is non-zero"),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024)
                .expect("default property projection spill budget is non-zero"),
            max_spill_runs: NonZeroUsize::new(4_096)
                .expect("default property projection run budget is non-zero"),
            max_merge_fan_in: NonZeroUsize::new(32)
                .expect("default property projection merge fan-in is non-zero"),
            target_block_bytes: NonZeroU64::new(1024 * 1024)
                .expect("default property projection block size is non-zero"),
            max_index_key_bytes: NonZeroU64::new(4 * 1024)
                .expect("default property projection key limit is non-zero"),
            max_generated_entries: NonZeroU64::new(100_000_000)
                .expect("default property projection fact budget is non-zero"),
        }
    }
}

#[derive(Debug)]
pub enum PersistentPropertyProjectionError {
    Io(std::io::Error),
    Read(SegmentReadError),
    Canonical(CanonicalSegmentError),
    Source(String),
    Corrupt(String),
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
    GeneratedEntryBudgetExceeded {
        required_entries: u64,
        max_entries: u64,
    },
    BlockTooLarge {
        block_bytes: u64,
        max_bytes: u64,
    },
}

impl Display for PersistentPropertyProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Canonical(error) => Display::fmt(error, formatter),
            Self::Source(message) | Self::Corrupt(message) => formatter.write_str(message),
            Self::MemoryBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection build requires {required_bytes} resident bytes, exceeding {max_bytes}"
            ),
            Self::SpillBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection build requires {required_bytes} spill bytes, exceeding {max_bytes}"
            ),
            Self::SpillRunBudgetExceeded {
                required_runs,
                max_runs,
            } => write!(
                formatter,
                "property projection build requires {required_runs} spill runs, exceeding {max_runs}"
            ),
            Self::GeneratedEntryBudgetExceeded {
                required_entries,
                max_entries,
            } => write!(
                formatter,
                "property projection build requires {required_entries} generated entries, exceeding {max_entries}"
            ),
            Self::BlockTooLarge {
                block_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection block uses {block_bytes} bytes, exceeding {max_bytes}"
            ),
        }
    }
}

impl Error for PersistentPropertyProjectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::Canonical(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PersistentPropertyProjectionError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for PersistentPropertyProjectionError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

impl From<CanonicalSegmentError> for PersistentPropertyProjectionError {
    fn from(error: CanonicalSegmentError) -> Self {
        Self::Canonical(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentPropertyProjectionBlockDescriptor {
    pub block_id: u64,
    pub label_id: LabelId,
    pub property: String,
    pub kind: PersistentPropertyProjectionKind,
    pub min_key: Value,
    pub max_key: Value,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub entry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentPropertyProjectionManifest {
    pub generation: ManifestGeneration,
    pub source_commit_epoch: u64,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub entry_count: u64,
    pub definitions: Vec<PersistentPropertyProjectionDefinition>,
    pub blocks: Vec<PersistentPropertyProjectionBlockDescriptor>,
}

impl PersistentPropertyProjectionManifest {
    pub fn validate(&self) -> Result<(), PersistentPropertyProjectionError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection manifest has an unsupported artifact id".to_string(),
            ));
        }
        if self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8 {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection artifact is shorter than its header".to_string(),
            ));
        }
        if !self
            .definitions
            .windows(2)
            .all(|pair| definition_key(&pair[0]) < definition_key(&pair[1]))
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection definitions are not strictly ordered".to_string(),
            ));
        }
        let mut previous_end = ARTIFACT_HEADER.len() as u64 + 8;
        let mut previous_key = None;
        let mut entries = 0u64;
        for block in &self.blocks {
            if block.block_id < BLOCK_ID_BASE
                || block.entry_count == 0
                || block.min_key > block.max_key
                || block.offset < previous_end
            {
                return Err(PersistentPropertyProjectionError::Corrupt(format!(
                    "property projection block {} has invalid bounds",
                    block.block_id
                )));
            }
            if !self.definitions.iter().any(|definition| {
                definition.label_id == block.label_id
                    && definition.property == block.property
                    && definition.kind == block.kind
            }) {
                return Err(PersistentPropertyProjectionError::Corrupt(format!(
                    "property projection block {} has no complete definition",
                    block.block_id
                )));
            }
            let key = block_descriptor_key(block);
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(PersistentPropertyProjectionError::Corrupt(
                    "property projection blocks are not strictly ordered".to_string(),
                ));
            }
            previous_key = Some(key);
            previous_end = block
                .offset
                .checked_add(block.length.get())
                .ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(
                        "property projection block range overflows u64".to_string(),
                    )
                })?;
            if previous_end > self.artifact_len {
                return Err(PersistentPropertyProjectionError::Corrupt(
                    "property projection block exceeds its artifact".to_string(),
                ));
            }
            entries = entries.saturating_add(u64::from(block.entry_count));
        }
        if previous_end != self.artifact_len || entries != self.entry_count {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection manifest counts or artifact length are inconsistent"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub fn supports(
        &self,
        label_id: LabelId,
        property: &str,
        kind: PersistentPropertyProjectionKind,
    ) -> bool {
        self.definitions
            .binary_search_by(|definition| {
                definition_key(definition).cmp(&(kind, label_id, property))
            })
            .ok()
            .is_some_and(|index| self.definitions[index].complete)
    }

    pub fn encode(&self) -> Result<String, PersistentPropertyProjectionError> {
        self.validate()?;
        let mut body = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nsource_commit_epoch\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nentry_count\t{}\n",
            self.generation.0,
            self.source_commit_epoch,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.entry_count
        );
        for definition in &self.definitions {
            body.push_str(&format!(
                "definition\t{}\t{}\t{}\t{}\n",
                kind_tag(definition.kind),
                definition.label_id.0,
                encode_hex(definition.property.as_bytes()),
                u8::from(definition.complete)
            ));
        }
        for block in &self.blocks {
            body.push_str(&format!(
                "block\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                block.block_id,
                kind_tag(block.kind),
                block.label_id.0,
                encode_hex(block.property.as_bytes()),
                encode_hex(&encode_standalone_value(&block.min_key)?),
                encode_hex(&encode_standalone_value(&block.max_key)?),
                block.offset,
                block.length.get(),
                block.content_digest.0,
                block.entry_count,
                self.generation.0
            ));
        }
        let checksum = content_digest(body.as_bytes()).0;
        Ok(format!("{body}checksum\t{checksum}\n"))
    }

    pub fn decode(encoded: &str) -> Result<Self, PersistentPropertyProjectionError> {
        let marker = "checksum\t";
        let checksum_offset = encoded.rfind(marker).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection manifest is missing checksum".to_string(),
            )
        })?;
        let body = &encoded[..checksum_offset];
        let checksum_line = encoded[checksum_offset..].trim_end();
        if checksum_line.contains('\n') {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection manifest has data after checksum".to_string(),
            ));
        }
        let expected = parse_u64(
            checksum_line.strip_prefix(marker).unwrap_or_default(),
            "manifest checksum",
        )?;
        let actual = content_digest(body.as_bytes()).0;
        if expected != actual {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection manifest checksum mismatch: expected {expected}, got {actual}"
            )));
        }
        let mut generation = None;
        let mut source_commit_epoch = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut entry_count = None;
        let mut definitions = Vec::new();
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
                ["source_commit_epoch", value] => {
                    source_commit_epoch = Some(parse_u64(value, "source commit epoch")?)
                }
                ["artifact_id", value] => artifact_id = Some(parse_u64(value, "artifact id")?),
                ["artifact_len", value] => {
                    artifact_len = Some(parse_u64(value, "artifact length")?)
                }
                ["artifact_digest", value] => {
                    artifact_digest = Some(parse_u64(value, "artifact digest")?)
                }
                ["entry_count", value] => entry_count = Some(parse_u64(value, "entry count")?),
                ["definition", kind, label, property, complete] => {
                    definitions.push(PersistentPropertyProjectionDefinition {
                        label_id: LabelId(parse_u32(label, "definition label")?),
                        property: decode_utf8_hex(property, "definition property")?,
                        kind: kind_from_tag(parse_u8(kind, "definition kind")?)?,
                        complete: match parse_u8(complete, "definition completeness")? {
                            0 => false,
                            1 => true,
                            value => {
                                return Err(PersistentPropertyProjectionError::Corrupt(format!(
                                    "invalid property projection completeness {value}"
                                )));
                            }
                        },
                    });
                }
                ["block", block_id, kind, label, property, min_key, max_key, offset, length, digest, count, block_generation] =>
                {
                    let block_generation = parse_u64(block_generation, "block generation")?;
                    if generation != Some(block_generation) {
                        return Err(PersistentPropertyProjectionError::Corrupt(
                            "property projection block generation does not match manifest"
                                .to_string(),
                        ));
                    }
                    blocks.push(PersistentPropertyProjectionBlockDescriptor {
                        block_id: parse_u64(block_id, "block id")?,
                        kind: kind_from_tag(parse_u8(kind, "block kind")?)?,
                        label_id: LabelId(parse_u32(label, "block label")?),
                        property: decode_utf8_hex(property, "block property")?,
                        min_key: decode_standalone_value(&decode_hex(min_key, "minimum key")?)?,
                        max_key: decode_standalone_value(&decode_hex(max_key, "maximum key")?)?,
                        offset: parse_u64(offset, "block offset")?,
                        length: NonZeroU64::new(parse_u64(length, "block length")?).ok_or_else(
                            || {
                                PersistentPropertyProjectionError::Corrupt(
                                    "property projection block length is zero".to_string(),
                                )
                            },
                        )?,
                        content_digest: ContentDigest(parse_u64(digest, "block digest")?),
                        entry_count: parse_u32(count, "block entry count")?,
                    });
                }
                [""] => {}
                _ => {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "invalid property projection manifest line: {line}"
                    )));
                }
            }
        }
        if !saw_header {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection manifest has an invalid header".to_string(),
            ));
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            source_commit_epoch: required(source_commit_epoch, "source commit epoch")?,
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            entry_count: required(entry_count, "entry count")?,
            definitions,
            blocks,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistentPropertyProjectionBuildReport {
    pub input_node_count: u64,
    pub generated_entry_count: u64,
    pub persisted_entry_count: u64,
    pub block_count: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_resident_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct PersistentPropertyProjectionWriteOutput {
    pub manifest: PersistentPropertyProjectionManifest,
    pub report: PersistentPropertyProjectionBuildReport,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey {
    kind: PersistentPropertyProjectionKind,
    label_id: LabelId,
    property: String,
    value: Value,
    node_id: NodeId,
}

impl EntryKey {
    fn encoded_len(&self) -> Result<u64, PersistentPropertyProjectionError> {
        Ok(1u64
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(self.property.len() as u64)
            .saturating_add(4)
            .saturating_add(encode_standalone_value(&self.value)?.len() as u64)
            .saturating_add(8))
    }

    fn resident_bytes(&self) -> Result<u64, PersistentPropertyProjectionError> {
        Ok(self
            .encoded_len()?
            .saturating_add(std::mem::size_of::<Self>() as u64))
    }
}

pub struct PersistentPropertyProjectionWriter {
    config: PersistentPropertyProjectionConfig,
}

impl PersistentPropertyProjectionWriter {
    pub const fn new(config: PersistentPropertyProjectionConfig) -> Self {
        Self { config }
    }

    pub fn write_fallible<N>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        mut definitions: Vec<PersistentPropertyProjectionDefinition>,
        nodes: N,
    ) -> Result<PersistentPropertyProjectionWriteOutput, PersistentPropertyProjectionError>
    where
        N: IntoIterator<Item = Result<NodeRecord, PersistentPropertyProjectionError>>,
    {
        definitions.sort_by(|left, right| definition_key(left).cmp(&definition_key(right)));
        definitions.dedup_by(|left, right| {
            left.label_id == right.label_id
                && left.property == right.property
                && left.kind == right.kind
        });
        let mut by_label: BTreeMap<LabelId, Vec<usize>> = BTreeMap::new();
        for (index, definition) in definitions.iter_mut().enumerate() {
            definition.complete = true;
            by_label.entry(definition.label_id).or_default().push(index);
        }
        let mut runs = ProjectionSpillRuns::new(path, generation, self.config);
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0u64;
        let mut generated_entries = 0u64;
        let mut input_nodes = 0u64;
        let mut peak_resident_bytes = 0u64;
        for node in nodes {
            let node = node?;
            input_nodes = input_nodes.saturating_add(1);
            for label in &node.labels {
                let Some(indexes) = by_label.get(label) else {
                    continue;
                };
                for definition_index in indexes {
                    let definition = &definitions[*definition_index];
                    let Some(value) = node.properties.get(&definition.property) else {
                        continue;
                    };
                    match definition.kind {
                        PersistentPropertyProjectionKind::Range => {
                            if !is_range_value(value) {
                                continue;
                            }
                            let encoded = encode_standalone_value(value)?;
                            if encoded.len() as u64 > self.config.max_index_key_bytes.get() {
                                definitions[*definition_index].complete = false;
                                continue;
                            }
                            self.emit(
                                EntryKey {
                                    kind: definition.kind,
                                    label_id: *label,
                                    property: definition.property.clone(),
                                    value: value.clone(),
                                    node_id: node.id,
                                },
                                &mut runs,
                                &mut chunk,
                                &mut chunk_bytes,
                                &mut generated_entries,
                                &mut peak_resident_bytes,
                            )?;
                        }
                        PersistentPropertyProjectionKind::FullText => {
                            let Value::String(value) = value else {
                                continue;
                            };
                            for token in full_text_tokens_streaming(value) {
                                self.emit(
                                    EntryKey {
                                        kind: definition.kind,
                                        label_id: *label,
                                        property: definition.property.clone(),
                                        value: Value::String(token),
                                        node_id: node.id,
                                    },
                                    &mut runs,
                                    &mut chunk,
                                    &mut chunk_bytes,
                                    &mut generated_entries,
                                    &mut peak_resident_bytes,
                                )?;
                            }
                        }
                    }
                }
            }
        }
        if !chunk.is_empty() {
            runs.spill(&mut chunk)?;
        }
        runs.compact()?;
        let tmp_path = path.with_extension("skein.tmp");
        let output = self.merge_runs(
            &tmp_path,
            generation,
            source_commit_epoch,
            definitions,
            input_nodes,
            generated_entries,
            peak_resident_bytes,
            &runs,
        );
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        fs::rename(&tmp_path, path)?;
        sync_parent(path)?;
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(
        &self,
        entry: EntryKey,
        runs: &mut ProjectionSpillRuns,
        chunk: &mut Vec<EntryKey>,
        chunk_bytes: &mut u64,
        generated_entries: &mut u64,
        peak_resident_bytes: &mut u64,
    ) -> Result<(), PersistentPropertyProjectionError> {
        let required_entries = generated_entries.saturating_add(1);
        if required_entries > self.config.max_generated_entries.get() {
            return Err(
                PersistentPropertyProjectionError::GeneratedEntryBudgetExceeded {
                    required_entries,
                    max_entries: self.config.max_generated_entries.get(),
                },
            );
        }
        let entry_bytes = entry.resident_bytes()?;
        if entry_bytes > self.config.memory_budget_bytes.get() {
            return Err(PersistentPropertyProjectionError::MemoryBudgetExceeded {
                required_bytes: entry_bytes,
                max_bytes: self.config.memory_budget_bytes.get(),
            });
        }
        if !chunk.is_empty()
            && chunk_bytes.saturating_add(entry_bytes) > self.config.memory_budget_bytes.get()
        {
            runs.spill(chunk)?;
            *chunk_bytes = 0;
        }
        *chunk_bytes = chunk_bytes.saturating_add(entry_bytes);
        *peak_resident_bytes = (*peak_resident_bytes).max(*chunk_bytes);
        *generated_entries = required_entries;
        chunk.push(entry);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_runs(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        definitions: Vec<PersistentPropertyProjectionDefinition>,
        input_nodes: u64,
        generated_entries: u64,
        peak_resident_bytes: u64,
        runs: &ProjectionSpillRuns,
    ) -> Result<PersistentPropertyProjectionWriteOutput, PersistentPropertyProjectionError> {
        let mut readers = runs
            .paths
            .iter()
            .map(|path| ProjectionRunReader::open(path, self.config.max_index_key_bytes))
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = Vec::with_capacity(readers.len());
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            let key = reader.next_key()?;
            if let Some(key) = &key {
                heap.push(Reverse((key.clone(), index)));
            }
            current.push(key);
        }
        let file = File::create(path)?;
        let mut artifact = ProjectionArtifactBuilder::new(
            file,
            generation,
            source_commit_epoch,
            definitions,
            self.config,
        )?;
        let mut previous = None;
        while let Some(Reverse((key, run_index))) = heap.pop() {
            if current[run_index].as_ref() != Some(&key) {
                return Err(PersistentPropertyProjectionError::Corrupt(
                    "property projection spill heap does not match its reader".to_string(),
                ));
            }
            if previous.as_ref() != Some(&key) {
                artifact.push(key.clone())?;
                previous = Some(key);
            }
            current[run_index] = readers[run_index].next_key()?;
            if let Some(next) = &current[run_index] {
                heap.push(Reverse((next.clone(), run_index)));
            }
        }
        let manifest = artifact.finish()?;
        Ok(PersistentPropertyProjectionWriteOutput {
            report: PersistentPropertyProjectionBuildReport {
                input_node_count: input_nodes,
                generated_entry_count: generated_entries,
                persisted_entry_count: manifest.entry_count,
                block_count: manifest.blocks.len() as u64,
                spill_run_count: runs.next_run_sequence,
                spill_bytes: runs.spill_bytes,
                peak_resident_bytes,
            },
            manifest,
        })
    }
}

struct ProjectionSpillRuns {
    prefix: PathBuf,
    generation: ManifestGeneration,
    config: PersistentPropertyProjectionConfig,
    paths: Vec<PathBuf>,
    spill_bytes: u64,
    next_run_sequence: usize,
}

impl ProjectionSpillRuns {
    fn new(
        path: &Path,
        generation: ManifestGeneration,
        config: PersistentPropertyProjectionConfig,
    ) -> Self {
        Self {
            prefix: path.to_path_buf(),
            generation,
            config,
            paths: Vec::new(),
            spill_bytes: 0,
            next_run_sequence: 0,
        }
    }

    fn spill(
        &mut self,
        entries: &mut Vec<EntryKey>,
    ) -> Result<(), PersistentPropertyProjectionError> {
        let required_runs = self.paths.len().saturating_add(1);
        if required_runs > self.config.max_spill_runs.get() {
            return Err(PersistentPropertyProjectionError::SpillRunBudgetExceeded {
                required_runs,
                max_runs: self.config.max_spill_runs.get(),
            });
        }
        entries.sort_unstable();
        entries.dedup();
        let run_bytes = entries
            .iter()
            .try_fold(RUN_HEADER.len() as u64, |bytes, entry| {
                entry
                    .encoded_len()
                    .map(|entry_bytes| bytes.saturating_add(entry_bytes))
            })?;
        let required_bytes = self.spill_bytes.saturating_add(run_bytes);
        if required_bytes > self.config.max_spill_bytes.get() {
            return Err(PersistentPropertyProjectionError::SpillBudgetExceeded {
                required_bytes,
                max_bytes: self.config.max_spill_bytes.get(),
            });
        }
        let path = self.next_path()?;
        let mut writer = BufWriter::new(File::create(&path)?);
        writer.write_all(RUN_HEADER)?;
        for entry in entries.iter() {
            write_entry_key(&mut writer, entry)?;
        }
        writer.flush()?;
        self.paths.push(path);
        self.spill_bytes = required_bytes;
        entries.clear();
        Ok(())
    }

    fn compact(&mut self) -> Result<(), PersistentPropertyProjectionError> {
        let fan_in = self.config.max_merge_fan_in.get();
        if fan_in < 2 {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection merge fan-in must be at least two".to_string(),
            ));
        }
        while self.paths.len() > fan_in {
            let old_paths = std::mem::take(&mut self.paths);
            let mut merged_paths = Vec::with_capacity(old_paths.len().div_ceil(fan_in));
            for group in old_paths.chunks(fan_in) {
                let path = self.next_path()?;
                let bytes = match merge_projection_run_group(group, &path, self.config) {
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
                    return Err(PersistentPropertyProjectionError::SpillBudgetExceeded {
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

    fn next_path(&mut self) -> Result<PathBuf, PersistentPropertyProjectionError> {
        let required_runs = self.next_run_sequence.saturating_add(1);
        if required_runs > self.config.max_spill_runs.get() {
            return Err(PersistentPropertyProjectionError::SpillRunBudgetExceeded {
                required_runs,
                max_runs: self.config.max_spill_runs.get(),
            });
        }
        let sequence = self.next_run_sequence;
        self.next_run_sequence = self.next_run_sequence.saturating_add(1);
        Ok(self.prefix.with_file_name(format!(
            ".property-index.{}.run.{sequence}.tmp",
            self.generation.0
        )))
    }
}

impl Drop for ProjectionSpillRuns {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

struct ProjectionRunReader {
    reader: BufReader<File>,
    max_key_bytes: NonZeroU64,
}

impl ProjectionRunReader {
    fn open(
        path: &Path,
        max_key_bytes: NonZeroU64,
    ) -> Result<Self, PersistentPropertyProjectionError> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        if &header != RUN_HEADER {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection spill run has an invalid header".to_string(),
            ));
        }
        Ok(Self {
            reader,
            max_key_bytes,
        })
    }

    fn next_key(&mut self) -> Result<Option<EntryKey>, PersistentPropertyProjectionError> {
        let mut kind = [0u8; 1];
        match self.reader.read(&mut kind)? {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one byte read buffer"),
        }
        let label_id = LabelId(read_u32(&mut self.reader)?);
        let property = read_bounded_string(&mut self.reader, self.max_key_bytes.get())?;
        let value_len = read_u32(&mut self.reader)? as usize;
        if value_len as u64 > self.max_key_bytes.get() {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection spill key uses {value_len} bytes, exceeding {}",
                self.max_key_bytes
            )));
        }
        let mut value = vec![0u8; value_len];
        self.reader.read_exact(&mut value)?;
        Ok(Some(EntryKey {
            kind: kind_from_tag(kind[0])?,
            label_id,
            property,
            value: decode_standalone_value(&value)?,
            node_id: NodeId(read_u64(&mut self.reader)?),
        }))
    }
}

fn merge_projection_run_group(
    sources: &[PathBuf],
    destination: &Path,
    config: PersistentPropertyProjectionConfig,
) -> Result<u64, PersistentPropertyProjectionError> {
    let mut readers = sources
        .iter()
        .map(|path| ProjectionRunReader::open(path, config.max_index_key_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let mut current = Vec::with_capacity(readers.len());
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        let key = reader.next_key()?;
        if let Some(key) = &key {
            heap.push(Reverse((key.clone(), index)));
        }
        current.push(key);
    }
    let mut writer = BufWriter::new(File::create(destination)?);
    writer.write_all(RUN_HEADER)?;
    let mut bytes = RUN_HEADER.len() as u64;
    let mut previous = None;
    while let Some(Reverse((key, run_index))) = heap.pop() {
        if current[run_index].as_ref() != Some(&key) {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection spill compaction heap mismatch".to_string(),
            ));
        }
        if previous.as_ref() != Some(&key) {
            write_entry_key(&mut writer, &key)?;
            bytes = bytes.saturating_add(key.encoded_len()?);
            previous = Some(key);
        }
        current[run_index] = readers[run_index].next_key()?;
        if let Some(next) = &current[run_index] {
            heap.push(Reverse((next.clone(), run_index)));
        }
    }
    writer.flush()?;
    Ok(bytes)
}

struct ProjectionArtifactBuilder {
    writer: BufWriter<File>,
    artifact_digest: DigestState,
    generation: ManifestGeneration,
    source_commit_epoch: u64,
    definitions: Vec<PersistentPropertyProjectionDefinition>,
    config: PersistentPropertyProjectionConfig,
    artifact_len: u64,
    next_block_id: u64,
    entry_count: u64,
    pending: Vec<EntryKey>,
    pending_bytes: u64,
    pending_resident_bytes: u64,
    blocks: Vec<PersistentPropertyProjectionBlockDescriptor>,
}

impl ProjectionArtifactBuilder {
    fn new(
        file: File,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        definitions: Vec<PersistentPropertyProjectionDefinition>,
        config: PersistentPropertyProjectionConfig,
    ) -> Result<Self, PersistentPropertyProjectionError> {
        let mut writer = BufWriter::new(file);
        let mut artifact_digest = DigestState::new();
        write_hashed(&mut writer, &mut artifact_digest, ARTIFACT_HEADER)?;
        write_hashed(
            &mut writer,
            &mut artifact_digest,
            &generation.0.to_le_bytes(),
        )?;
        Ok(Self {
            writer,
            artifact_digest,
            generation,
            source_commit_epoch,
            definitions,
            config,
            artifact_len: ARTIFACT_HEADER.len() as u64 + 8,
            next_block_id: BLOCK_ID_BASE,
            entry_count: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            pending_resident_bytes: 0,
            blocks: Vec::new(),
        })
    }

    fn push(&mut self, entry: EntryKey) -> Result<(), PersistentPropertyProjectionError> {
        let entry_bytes = 4u64
            .saturating_add(encode_standalone_value(&entry.value)?.len() as u64)
            .saturating_add(8);
        let group_changed = self.pending.first().is_some_and(|first| {
            first.kind != entry.kind
                || first.label_id != entry.label_id
                || first.property != entry.property
        });
        let projected_header = 8u64
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(1)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(entry.property.len() as u64)
            .saturating_add(4);
        let entry_resident_bytes = entry.resident_bytes()?;
        if entry_resident_bytes > self.config.memory_budget_bytes.get() {
            return Err(PersistentPropertyProjectionError::MemoryBudgetExceeded {
                required_bytes: entry_resident_bytes,
                max_bytes: self.config.memory_budget_bytes.get(),
            });
        }
        if !self.pending.is_empty()
            && (group_changed
                || projected_header
                    .saturating_add(self.pending_bytes)
                    .saturating_add(entry_bytes)
                    > self.config.target_block_bytes.get()
                || self
                    .pending_resident_bytes
                    .saturating_add(entry_resident_bytes)
                    > self.config.memory_budget_bytes.get())
        {
            self.flush_block()?;
        }
        self.pending_bytes = self.pending_bytes.saturating_add(entry_bytes);
        self.pending_resident_bytes = self
            .pending_resident_bytes
            .saturating_add(entry_resident_bytes);
        self.pending.push(entry);
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), PersistentPropertyProjectionError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let first = self.pending.first().expect("projection block is non-empty");
        let property_len = u32::try_from(first.property.len()).map_err(|_| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection property name exceeds u32".to_string(),
            )
        })?;
        let entry_count = u32::try_from(self.pending.len()).map_err(|_| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection block count exceeds u32".to_string(),
            )
        })?;
        let header_bytes = 8u64
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(1)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(first.property.len() as u64)
            .saturating_add(4);
        let block_bytes = header_bytes.saturating_add(self.pending_bytes);
        let hard_max = self.config.target_block_bytes.get().max(
            self.config
                .max_index_key_bytes
                .get()
                .saturating_add(header_bytes)
                .saturating_add(16),
        );
        if block_bytes > hard_max {
            return Err(PersistentPropertyProjectionError::BlockTooLarge {
                block_bytes,
                max_bytes: hard_max,
            });
        }
        let min_key = first.value.clone();
        let max_key = self
            .pending
            .last()
            .expect("projection block is non-empty")
            .value
            .clone();
        let mut block_digest = DigestState::new();
        for bytes in [
            BLOCK_HEADER.as_slice(),
            &self.generation.0.to_le_bytes(),
            &self.next_block_id.to_le_bytes(),
            &[kind_tag(first.kind)],
            &first.label_id.0.to_le_bytes(),
            &property_len.to_le_bytes(),
            first.property.as_bytes(),
            &entry_count.to_le_bytes(),
        ] {
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                bytes,
            )?;
        }
        for entry in &self.pending {
            let value = encode_standalone_value(&entry.value)?;
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                &(value.len() as u32).to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                &value,
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                &entry.node_id.0.to_le_bytes(),
            )?;
        }
        let length = NonZeroU64::new(block_bytes).expect("projection block is non-empty");
        self.blocks
            .push(PersistentPropertyProjectionBlockDescriptor {
                block_id: self.next_block_id,
                label_id: first.label_id,
                property: first.property.clone(),
                kind: first.kind,
                min_key,
                max_key,
                offset: self.artifact_len,
                length,
                content_digest: ContentDigest(block_digest.finish()),
                entry_count,
            });
        self.artifact_len = self.artifact_len.saturating_add(block_bytes);
        self.next_block_id = self.next_block_id.saturating_add(1);
        self.entry_count = self.entry_count.saturating_add(u64::from(entry_count));
        self.pending.clear();
        self.pending_bytes = 0;
        self.pending_resident_bytes = 0;
        Ok(())
    }

    fn finish(
        mut self,
    ) -> Result<PersistentPropertyProjectionManifest, PersistentPropertyProjectionError> {
        self.flush_block()?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let manifest = PersistentPropertyProjectionManifest {
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            artifact_id: ARTIFACT_ID,
            artifact_len: self.artifact_len,
            artifact_digest: ContentDigest(self.artifact_digest.finish()),
            entry_count: self.entry_count,
            definitions: self.definitions,
            blocks: self.blocks,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistentPropertyProjectionReadReport {
    pub blocks_considered: u64,
    pub blocks_pruned: u64,
    pub blocks_read: u64,
    pub bytes_read: u64,
    pub entries_decoded: u64,
    pub candidates_returned: u64,
}

#[derive(Debug, Clone)]
pub struct PersistentPropertyProjectionReader {
    path: PathBuf,
    manifest: PersistentPropertyProjectionManifest,
    range_reader: FileSegmentRangeReader,
    max_block_bytes: NonZeroU64,
}

impl PersistentPropertyProjectionReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: PersistentPropertyProjectionManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_block_bytes: NonZeroU64,
    ) -> Result<Self, PersistentPropertyProjectionError> {
        manifest.validate()?;
        let path = path.into();
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        for block in &manifest.blocks {
            if block.length.get() > max_block_bytes.get() {
                return Err(PersistentPropertyProjectionError::BlockTooLarge {
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

    pub fn manifest(&self) -> &PersistentPropertyProjectionManifest {
        &self.manifest
    }

    pub fn scan_range_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.scan_candidates(
            label_id,
            property,
            PersistentPropertyProjectionKind::Range,
            |value| range_bounds_match(value, lower, upper),
            |block| range_block_might_match(block, lower, upper),
            &mut consumer,
        )
    }

    pub fn scan_full_text_token_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        token: &str,
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        let token = Value::String(token.to_string());
        self.scan_candidates(
            label_id,
            property,
            PersistentPropertyProjectionKind::FullText,
            |value| value == &token,
            |block| block.min_key <= token && token <= block.max_key,
            &mut consumer,
        )
    }

    pub fn estimate_full_text_token_entries(
        &self,
        label_id: LabelId,
        property: &str,
        token: &str,
    ) -> u64 {
        let token = Value::String(token.to_string());
        self.manifest
            .blocks
            .iter()
            .filter(|block| {
                block.kind == PersistentPropertyProjectionKind::FullText
                    && block.label_id == label_id
                    && block.property == property
                    && block.min_key <= token
                    && token <= block.max_key
            })
            .map(|block| u64::from(block.entry_count))
            .sum()
    }

    fn scan_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        kind: PersistentPropertyProjectionKind,
        mut value_matches: impl FnMut(&Value) -> bool,
        mut block_matches: impl FnMut(&PersistentPropertyProjectionBlockDescriptor) -> bool,
        consumer: &mut impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        if !self.manifest.supports(label_id, property, kind) {
            return Err(PersistentPropertyProjectionError::Source(
                "requested persistent property projection is unavailable or incomplete".to_string(),
            ));
        }
        let mut report = PersistentPropertyProjectionReadReport::default();
        for block in self.manifest.blocks.iter().filter(|block| {
            block.kind == kind && block.label_id == label_id && block.property == property
        }) {
            report.blocks_considered = report.blocks_considered.saturating_add(1);
            if !block_matches(block) {
                report.blocks_pruned = report.blocks_pruned.saturating_add(1);
                continue;
            }
            let bytes = self.read_block(block)?;
            report.blocks_read = report.blocks_read.saturating_add(1);
            report.bytes_read = report.bytes_read.saturating_add(bytes.len() as u64);
            let mut control = CanonicalScanControl::Continue;
            decode_projection_block(&bytes, self.manifest.generation, block, |value, node_id| {
                report.entries_decoded = report.entries_decoded.saturating_add(1);
                if control == CanonicalScanControl::Continue && value_matches(&value) {
                    report.candidates_returned = report.candidates_returned.saturating_add(1);
                    control = consumer(node_id)?;
                }
                Ok(())
            })?;
            if control == CanonicalScanControl::Stop {
                return Ok((report, control));
            }
        }
        Ok((report, CanonicalScanControl::Continue))
    }

    fn read_block(
        &self,
        block: &PersistentPropertyProjectionBlockDescriptor,
    ) -> Result<Arc<[u8]>, PersistentPropertyProjectionError> {
        if block.length.get() > self.max_block_bytes.get() {
            return Err(PersistentPropertyProjectionError::BlockTooLarge {
                block_bytes: block.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        Ok(self.range_reader.read_range(&SegmentReadRange {
            artifact_id: self.manifest.artifact_id,
            segment_ids: vec![block.block_id],
            offset: block.offset,
            length: block.length,
            content_digest: Some(block.content_digest),
        })?)
    }
}

fn decode_projection_block(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &PersistentPropertyProjectionBlockDescriptor,
    mut consumer: impl FnMut(Value, NodeId) -> Result<(), PersistentPropertyProjectionError>,
) -> Result<(), PersistentPropertyProjectionError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(8)? != BLOCK_HEADER {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection block {} has an invalid header",
            descriptor.block_id
        )));
    }
    let stored_generation = cursor.read_u64()?;
    let block_id = cursor.read_u64()?;
    let kind = kind_from_tag(cursor.read_u8()?)?;
    let label_id = LabelId(cursor.read_u32()?);
    let property = cursor.read_string()?;
    let entry_count = cursor.read_u32()?;
    if stored_generation != generation.0
        || block_id != descriptor.block_id
        || kind != descriptor.kind
        || label_id != descriptor.label_id
        || property != descriptor.property
        || entry_count != descriptor.entry_count
    {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection block {} metadata does not match its manifest",
            descriptor.block_id
        )));
    }
    let mut first = None;
    let mut previous = None;
    for _ in 0..entry_count {
        let value_len = cursor.read_u32()? as usize;
        let value = decode_standalone_value(cursor.read_exact(value_len)?)?;
        let node_id = NodeId(cursor.read_u64()?);
        let key = (value.clone(), node_id);
        if previous.as_ref().is_some_and(|previous| previous >= &key) {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection block {} entries are not strictly ordered",
                descriptor.block_id
            )));
        }
        consumer(value, node_id)?;
        if first.is_none() {
            first = Some(key.clone());
        }
        previous = Some(key);
    }
    if !cursor.is_empty()
        || first.as_ref().map(|value| &value.0) != Some(&descriptor.min_key)
        || previous.as_ref().map(|value| &value.0) != Some(&descriptor.max_key)
    {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection block {} payload bounds are inconsistent",
            descriptor.block_id
        )));
    }
    Ok(())
}

fn write_entry_key(
    writer: &mut impl Write,
    entry: &EntryKey,
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(&[kind_tag(entry.kind)])?;
    writer.write_all(&entry.label_id.0.to_le_bytes())?;
    write_string(writer, &entry.property)?;
    let value = encode_standalone_value(&entry.value)?;
    writer.write_all(&(value.len() as u32).to_le_bytes())?;
    writer.write_all(&value)?;
    writer.write_all(&entry.node_id.0.to_le_bytes())?;
    Ok(())
}

fn full_text_tokens_streaming(value: &str) -> impl Iterator<Item = String> + '_ {
    let mut window = VecDeque::with_capacity(3);
    value
        .chars()
        .flat_map(char::to_lowercase)
        .flat_map(move |ch| {
            if window.len() == 3 {
                window.pop_front();
            }
            window.push_back(ch);
            let chars = window.iter().copied().collect::<Vec<_>>();
            (1..=chars.len())
                .rev()
                .filter_map(|width| {
                    let token = chars[chars.len() - width..].iter().collect::<String>();
                    (!token.chars().all(char::is_whitespace)).then_some(token)
                })
                .collect::<Vec<_>>()
        })
}

fn is_range_value(value: &Value) -> bool {
    matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_))
}

fn range_bounds_match(
    value: &Value,
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> bool {
    if let Some((bound, inclusive)) = lower {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering.is_lt() || (ordering.is_eq() && !inclusive) {
            return false;
        }
    }
    if let Some((bound, inclusive)) = upper {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering.is_gt() || (ordering.is_eq() && !inclusive) {
            return false;
        }
    }
    true
}

fn range_block_might_match(
    block: &PersistentPropertyProjectionBlockDescriptor,
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> bool {
    if let Some((lower, inclusive)) = lower
        && let Some(ordering) = comparable_value_ordering(&block.max_key, lower)
        && (ordering.is_lt() || (ordering.is_eq() && !inclusive))
    {
        return false;
    }
    if let Some((upper, inclusive)) = upper
        && let Some(ordering) = comparable_value_ordering(&block.min_key, upper)
        && (ordering.is_gt() || (ordering.is_eq() && !inclusive))
    {
        return false;
    }
    true
}

fn comparable_value_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn definition_key(
    definition: &PersistentPropertyProjectionDefinition,
) -> (PersistentPropertyProjectionKind, LabelId, &str) {
    (definition.kind, definition.label_id, &definition.property)
}

fn block_descriptor_key(
    block: &PersistentPropertyProjectionBlockDescriptor,
) -> (PersistentPropertyProjectionKind, LabelId, &str, &Value, u64) {
    (
        block.kind,
        block.label_id,
        &block.property,
        &block.min_key,
        block.block_id,
    )
}

fn kind_tag(kind: PersistentPropertyProjectionKind) -> u8 {
    match kind {
        PersistentPropertyProjectionKind::Range => 1,
        PersistentPropertyProjectionKind::FullText => 2,
    }
}

fn kind_from_tag(
    tag: u8,
) -> Result<PersistentPropertyProjectionKind, PersistentPropertyProjectionError> {
    match tag {
        1 => Ok(PersistentPropertyProjectionKind::Range),
        2 => Ok(PersistentPropertyProjectionKind::FullText),
        _ => Err(PersistentPropertyProjectionError::Corrupt(format!(
            "invalid property projection kind {tag}"
        ))),
    }
}

fn write_string(
    writer: &mut impl Write,
    value: &str,
) -> Result<(), PersistentPropertyProjectionError> {
    let length = u32::try_from(value.len()).map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(
            "property projection string exceeds u32".to_string(),
        )
    })?;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

fn read_bounded_string(
    reader: &mut impl Read,
    max_bytes: u64,
) -> Result<String, PersistentPropertyProjectionError> {
    let length = read_u32(reader)? as usize;
    if length as u64 > max_bytes {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection spill string uses {length} bytes, exceeding {max_bytes}"
        )));
    }
    let mut bytes = vec![0u8; length];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|error| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection spill string is not UTF-8: {error}"
        ))
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex(value: &str, name: &str) -> Result<Vec<u8>, PersistentPropertyProjectionError> {
    if !value.len().is_multiple_of(2) {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection {name} has an invalid hexadecimal length"
        )));
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&value[offset..offset + 2], 16).map_err(|_| {
                PersistentPropertyProjectionError::Corrupt(format!(
                    "property projection {name} has invalid hexadecimal data"
                ))
            })
        })
        .collect()
}

fn decode_utf8_hex(value: &str, name: &str) -> Result<String, PersistentPropertyProjectionError> {
    String::from_utf8(decode_hex(value, name)?).map_err(|error| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection {name} is not UTF-8: {error}"
        ))
    })
}

fn parse_u8(value: &str, name: &str) -> Result<u8, PersistentPropertyProjectionError> {
    value.parse().map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest has invalid {name}: {value}"
        ))
    })
}

fn parse_u32(value: &str, name: &str) -> Result<u32, PersistentPropertyProjectionError> {
    value.parse().map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest has invalid {name}: {value}"
        ))
    })
}

fn parse_u64(value: &str, name: &str) -> Result<u64, PersistentPropertyProjectionError> {
    value.parse().map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest has invalid {name}: {value}"
        ))
    })
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, PersistentPropertyProjectionError> {
    value.ok_or_else(|| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest is missing {name}"
        ))
    })
}

fn read_u32(reader: &mut impl Read) -> Result<u32, PersistentPropertyProjectionError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, PersistentPropertyProjectionError> {
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

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], PersistentPropertyProjectionError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection cursor offset overflow".to_string(),
            )
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection block ended before its declared length".to_string(),
            )
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, PersistentPropertyProjectionError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, PersistentPropertyProjectionError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, PersistentPropertyProjectionError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width u64"),
        ))
    }

    fn read_string(&mut self) -> Result<String, PersistentPropertyProjectionError> {
        let length = self.read_u32()? as usize;
        String::from_utf8(self.read_exact(length)?.to_vec()).map_err(|error| {
            PersistentPropertyProjectionError::Corrupt(format!(
                "property projection block string is not UTF-8: {error}"
            ))
        })
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
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn write_double_hashed(
    writer: &mut impl Write,
    artifact_digest: &mut DigestState,
    block_digest: &mut DigestState,
    bytes: &[u8],
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(bytes)?;
    artifact_digest.update(bytes);
    block_digest.update(bytes);
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), PersistentPropertyProjectionError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    #[cfg(not(windows))]
    let directory = File::open(parent)?;
    #[cfg(windows)]
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x0200_0000)
        .open(parent)?;
    directory.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn node(id: u64, rank: i64, text: &str) -> NodeRecord {
        NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([
                ("rank".to_string(), Value::Int(rank)),
                ("text".to_string(), Value::String(text.to_string())),
            ]),
        }
    }

    #[test]
    fn external_projection_round_trips_range_and_full_text_candidates() {
        let root = std::env::temp_dir().join(format!(
            "skein-property-projection-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("projection.skein");
        let config = PersistentPropertyProjectionConfig {
            memory_budget_bytes: NonZeroU64::new(256).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            target_block_bytes: NonZeroU64::new(128).unwrap(),
            ..PersistentPropertyProjectionConfig::default()
        };
        let definitions = vec![
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::Range,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "text".to_string(),
                kind: PersistentPropertyProjectionKind::FullText,
                complete: false,
            },
        ];
        let output = PersistentPropertyProjectionWriter::new(config)
            .write_fallible(
                &path,
                ManifestGeneration(2),
                11,
                definitions,
                vec![
                    Ok(node(1, 10, "Graph Memory")),
                    Ok(node(2, 20, "Other")),
                    Ok(node(3, 30, "Memory Graph")),
                ],
            )
            .unwrap();
        assert!(output.report.spill_run_count > 1);
        let manifest =
            PersistentPropertyProjectionManifest::decode(&output.manifest.encode().unwrap())
                .unwrap();
        let reader = PersistentPropertyProjectionReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(4),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap();
        let mut range = Vec::new();
        reader
            .scan_range_candidates(
                LabelId(1),
                "rank",
                Some(&(Value::Int(15), true)),
                Some(&(Value::Int(30), false)),
                |id| {
                    range.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(range, vec![2]);
        let mut text = Vec::new();
        reader
            .scan_full_text_token_candidates(LabelId(1), "text", "mem", |id| {
                text.push(id.0);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(text, vec![1, 3]);
        fs::remove_dir_all(root).unwrap();
    }
}
