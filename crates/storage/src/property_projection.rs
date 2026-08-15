use crate::canonical::{
    decode_standalone_value, encode_standalone_value, CanonicalScanControl, CanonicalSegmentError,
};
use crate::{
    content_digest, durable_replace_file, ContentDigest, FileSegmentRangeReader,
    ManifestGeneration, NodeId, NodeRecord, RelId, RelRecord, SegmentCache, SegmentRangeReader,
    SegmentReadError, SegmentReadRange, StoreId,
};
use skein_core::{LabelId, RelTypeId, Value};
use skein_integrity::{Crc32cHasher, IntegrityHasher, Sha256Digest};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINPROPINDEX01";
const BLOCK_HEADER: &[u8; 8] = b"SKNIDX01";
const RUN_HEADER: &[u8; 8] = b"SKNIDXR1";
const MANIFEST_HEADER: &str = "SKEIN_PROPERTY_PROJECTION_MANIFEST_V1";
const ARTIFACT_ID: u64 = 0x534b_5052_4944_5831;
const BLOCK_ID_BASE: u64 = 3 << 60;
const COMPOSITE_PROPERTY_IDENTITY_PREFIX: &str = "skein-composite-property-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PersistentPropertyProjectionKind {
    Equality,
    Range,
    FullText,
    CompositeEquality,
    RelationshipEquality,
    RelationshipRange,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PersistentPropertyProjectionRecord {
    Node(NodeRecord),
    Relationship(RelRecord),
}

pub fn persistent_composite_property_identity(
    properties: &[String],
) -> Result<String, PersistentPropertyProjectionError> {
    if properties.len() < 2 {
        return Err(PersistentPropertyProjectionError::Source(
            "persistent composite property projection requires at least two properties".to_string(),
        ));
    }
    let mut identity = String::from(COMPOSITE_PROPERTY_IDENTITY_PREFIX);
    for property in properties {
        identity.push(':');
        identity.push_str(&encode_hex(property.as_bytes()));
    }
    Ok(identity)
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
    pub max_definition_count: NonZeroUsize,
    pub max_definition_bytes: NonZeroU64,
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
            max_definition_count: NonZeroUsize::new(65_536)
                .expect("default property projection definition limit is non-zero"),
            max_definition_bytes: NonZeroU64::new(8 * 1024 * 1024)
                .expect("default property projection definition byte limit is non-zero"),
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
    DefinitionCountBudgetExceeded {
        required_definitions: usize,
        max_definitions: usize,
    },
    DefinitionBytesBudgetExceeded {
        required_bytes: u64,
        max_bytes: u64,
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
            Self::DefinitionCountBudgetExceeded {
                required_definitions,
                max_definitions,
            } => write!(
                formatter,
                "property projection build requires {required_definitions} definitions, exceeding {max_definitions}"
            ),
            Self::DefinitionBytesBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection definitions require {required_bytes} resident bytes, exceeding {max_bytes}"
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

#[derive(Debug, Clone, Copy)]
pub struct PersistentPropertyProjectionDefinitionAdmission {
    max_definitions: usize,
    max_bytes: u64,
    definition_count: usize,
    resident_bytes: u64,
}

impl PersistentPropertyProjectionDefinitionAdmission {
    pub fn new(config: PersistentPropertyProjectionConfig) -> Self {
        Self {
            max_definitions: config.max_definition_count.get(),
            max_bytes: config.max_definition_bytes.get(),
            definition_count: 0,
            resident_bytes: 0,
        }
    }

    pub fn admit(
        &mut self,
        definition: &PersistentPropertyProjectionDefinition,
    ) -> Result<(), PersistentPropertyProjectionError> {
        let required_definitions = self.definition_count.saturating_add(1);
        if required_definitions > self.max_definitions {
            return Err(
                PersistentPropertyProjectionError::DefinitionCountBudgetExceeded {
                    required_definitions,
                    max_definitions: self.max_definitions,
                },
            );
        }
        let definition_bytes = (std::mem::size_of::<PersistentPropertyProjectionDefinition>()
            as u64)
            .saturating_add(definition.property.len() as u64);
        let required_bytes = self.resident_bytes.saturating_add(definition_bytes);
        if required_bytes > self.max_bytes {
            return Err(
                PersistentPropertyProjectionError::DefinitionBytesBudgetExceeded {
                    required_bytes,
                    max_bytes: self.max_bytes,
                },
            );
        }
        self.definition_count = required_definitions;
        self.resident_bytes = required_bytes;
        Ok(())
    }

    pub const fn definition_count(&self) -> usize {
        self.definition_count
    }

    pub const fn resident_bytes(&self) -> u64 {
        self.resident_bytes
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
    pub artifact_sha256: Sha256Digest,
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
        for definition in &self.definitions {
            if definition.kind == PersistentPropertyProjectionKind::CompositeEquality {
                decode_composite_property_identity(&definition.property)?;
            }
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
            if block.kind == PersistentPropertyProjectionKind::CompositeEquality {
                let arity = decode_composite_property_identity(&block.property)?.len();
                if !composite_key_has_arity(&block.min_key, arity)
                    || !composite_key_has_arity(&block.max_key, arity)
                {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "composite property projection block {} has invalid key arity",
                        block.block_id
                    )));
                }
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

    pub fn supports_composite_equality(&self, label_id: LabelId, properties: &[String]) -> bool {
        persistent_composite_property_identity(properties)
            .ok()
            .is_some_and(|identity| {
                self.supports(
                    label_id,
                    &identity,
                    PersistentPropertyProjectionKind::CompositeEquality,
                )
            })
    }

    pub fn supports_relationship(
        &self,
        rel_type: RelTypeId,
        property: &str,
        kind: PersistentPropertyProjectionKind,
    ) -> bool {
        matches!(
            kind,
            PersistentPropertyProjectionKind::RelationshipEquality
                | PersistentPropertyProjectionKind::RelationshipRange
        ) && self.supports(LabelId(rel_type.0), property, kind)
    }

    pub fn encode(&self) -> Result<String, PersistentPropertyProjectionError> {
        self.validate()?;
        let mut body = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nsource_commit_epoch\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nartifact_sha256\t{}\nentry_count\t{}\n",
            self.generation.0,
            self.source_commit_epoch,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.artifact_sha256,
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
        let mut artifact_sha256 = None;
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
                ["artifact_sha256", value] => {
                    artifact_sha256 = Some(value.parse().map_err(|error| {
                        PersistentPropertyProjectionError::Corrupt(format!(
                            "invalid artifact SHA-256 digest: {error}"
                        ))
                    })?)
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
            artifact_sha256: required(artifact_sha256, "artifact SHA-256 digest")?,
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
    pub definition_count: usize,
    pub definition_bytes: u64,
    pub input_record_count: u64,
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
        N: IntoIterator<
            Item = Result<PersistentPropertyProjectionRecord, PersistentPropertyProjectionError>,
        >,
    {
        definitions.sort_by(|left, right| definition_key(left).cmp(&definition_key(right)));
        definitions.dedup_by(|left, right| {
            left.label_id == right.label_id
                && left.property == right.property
                && left.kind == right.kind
        });
        let mut definition_admission =
            PersistentPropertyProjectionDefinitionAdmission::new(self.config);
        for definition in &definitions {
            definition_admission.admit(definition)?;
        }
        let definition_count = definition_admission.definition_count();
        let definition_bytes = definition_admission.resident_bytes();
        let mut by_subject: BTreeMap<ProjectionSubject, Vec<PreparedProjectionDefinition>> =
            BTreeMap::new();
        for (index, definition) in definitions.iter_mut().enumerate() {
            definition.complete = true;
            let value_source =
                if definition.kind == PersistentPropertyProjectionKind::CompositeEquality {
                    ProjectionValueSource::Composite(decode_composite_property_identity(
                        &definition.property,
                    )?)
                } else {
                    ProjectionValueSource::Scalar
                };
            by_subject
                .entry(definition_subject(definition))
                .or_default()
                .push(PreparedProjectionDefinition {
                    definition_index: index,
                    value_source,
                });
        }
        let mut runs = ProjectionSpillRuns::new(path, generation, self.config);
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0u64;
        let mut generated_entries = 0u64;
        let mut input_records = 0u64;
        let mut peak_resident_bytes = 0u64;
        for record in nodes {
            let record = record?;
            input_records = input_records.saturating_add(1);
            let (subjects, properties, entity_id) = match &record {
                PersistentPropertyProjectionRecord::Node(node) => (
                    node.labels
                        .iter()
                        .copied()
                        .map(ProjectionSubject::Node)
                        .collect::<Vec<_>>(),
                    &node.properties,
                    NodeId(node.id.0),
                ),
                PersistentPropertyProjectionRecord::Relationship(relationship) => (
                    vec![ProjectionSubject::Relationship(relationship.rel_type)],
                    &relationship.properties,
                    NodeId(relationship.id.0),
                ),
            };
            for subject in subjects {
                let Some(indexes) = by_subject.get(&subject) else {
                    continue;
                };
                for prepared in indexes {
                    let definition_index = prepared.definition_index;
                    let definition = &definitions[definition_index];
                    match definition.kind {
                        PersistentPropertyProjectionKind::Equality
                        | PersistentPropertyProjectionKind::RelationshipEquality => {
                            let Some(value) = properties.get(&definition.property) else {
                                continue;
                            };
                            let encoded = encode_standalone_value(value)?;
                            if encoded.len() as u64 > self.config.max_index_key_bytes.get() {
                                definitions[definition_index].complete = false;
                                continue;
                            }
                            self.emit(
                                EntryKey {
                                    kind: definition.kind,
                                    label_id: definition.label_id,
                                    property: definition.property.clone(),
                                    value: value.clone(),
                                    node_id: entity_id,
                                },
                                &mut runs,
                                &mut chunk,
                                &mut chunk_bytes,
                                &mut generated_entries,
                                &mut peak_resident_bytes,
                            )?;
                        }
                        PersistentPropertyProjectionKind::Range
                        | PersistentPropertyProjectionKind::RelationshipRange => {
                            let Some(value) = properties.get(&definition.property) else {
                                continue;
                            };
                            if !is_range_value(value) {
                                continue;
                            }
                            let encoded = encode_standalone_value(value)?;
                            if encoded.len() as u64 > self.config.max_index_key_bytes.get() {
                                definitions[definition_index].complete = false;
                                continue;
                            }
                            self.emit(
                                EntryKey {
                                    kind: definition.kind,
                                    label_id: definition.label_id,
                                    property: definition.property.clone(),
                                    value: value.clone(),
                                    node_id: entity_id,
                                },
                                &mut runs,
                                &mut chunk,
                                &mut chunk_bytes,
                                &mut generated_entries,
                                &mut peak_resident_bytes,
                            )?;
                        }
                        PersistentPropertyProjectionKind::FullText => {
                            let Some(value) = properties.get(&definition.property) else {
                                continue;
                            };
                            let Value::String(value) = value else {
                                continue;
                            };
                            for token in full_text_tokens_streaming(value) {
                                self.emit(
                                    EntryKey {
                                        kind: definition.kind,
                                        label_id: definition.label_id,
                                        property: definition.property.clone(),
                                        value: Value::String(token),
                                        node_id: entity_id,
                                    },
                                    &mut runs,
                                    &mut chunk,
                                    &mut chunk_bytes,
                                    &mut generated_entries,
                                    &mut peak_resident_bytes,
                                )?;
                            }
                        }
                        PersistentPropertyProjectionKind::CompositeEquality => {
                            let ProjectionValueSource::Composite(composite_properties) =
                                &prepared.value_source
                            else {
                                return Err(PersistentPropertyProjectionError::Corrupt(
                                    "composite property projection has a scalar value source"
                                        .to_string(),
                                ));
                            };
                            let Some(values) = composite_properties
                                .iter()
                                .map(|property| properties.get(property).cloned())
                                .collect::<Option<Vec<_>>>()
                            else {
                                continue;
                            };
                            let value = Value::List(values);
                            let encoded = encode_standalone_value(&value)?;
                            if encoded.len() as u64 > self.config.max_index_key_bytes.get() {
                                definitions[definition_index].complete = false;
                                continue;
                            }
                            self.emit(
                                EntryKey {
                                    kind: definition.kind,
                                    label_id: definition.label_id,
                                    property: definition.property.clone(),
                                    value,
                                    node_id: entity_id,
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
            definition_count,
            definition_bytes,
            input_records,
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
        durable_replace_file(&tmp_path, path)?;
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
        definition_count: usize,
        definition_bytes: u64,
        input_records: u64,
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
                definition_count,
                definition_bytes,
                input_record_count: input_records,
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

struct PreparedProjectionDefinition {
    definition_index: usize,
    value_source: ProjectionValueSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ProjectionSubject {
    Node(LabelId),
    Relationship(RelTypeId),
}

enum ProjectionValueSource {
    Scalar,
    Composite(Vec<String>),
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
    artifact_digest: IntegrityHasher,
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
        let mut artifact_digest = IntegrityHasher::new();
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
        let mut block_digest = Crc32cHasher::new();
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
        let artifact_integrity = self.artifact_digest.finish();
        let manifest = PersistentPropertyProjectionManifest {
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            artifact_id: ARTIFACT_ID,
            artifact_len: self.artifact_len,
            artifact_digest: ContentDigest(artifact_integrity.crc32c.as_u64()),
            artifact_sha256: artifact_integrity.sha256,
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

    pub fn scan_equality_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        value: &Value,
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
            PersistentPropertyProjectionKind::Equality,
            |candidate| candidate == value,
            |block| block.min_key <= *value && *value <= block.max_key,
            &mut consumer,
        )
    }

    pub fn scan_composite_equality_candidates(
        &self,
        label_id: LabelId,
        properties: &[String],
        values: &[&Value],
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        if properties.len() != values.len() {
            return Err(PersistentPropertyProjectionError::Source(
                "composite property projection key arity does not match its definition".to_string(),
            ));
        }
        let identity = persistent_composite_property_identity(properties)?;
        self.scan_candidates(
            label_id,
            &identity,
            PersistentPropertyProjectionKind::CompositeEquality,
            |candidate| {
                composite_key_ordering(candidate, values).is_some_and(|order| order.is_eq())
            },
            |block| {
                composite_key_ordering(&block.min_key, values).is_some_and(|order| order.is_le())
                    && composite_key_ordering(&block.max_key, values)
                        .is_some_and(|order| order.is_ge())
            },
            &mut consumer,
        )
    }

    pub fn scan_relationship_equality_candidates(
        &self,
        rel_type: RelTypeId,
        property: &str,
        value: &Value,
        mut consumer: impl FnMut(
            RelId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.scan_candidates(
            LabelId(rel_type.0),
            property,
            PersistentPropertyProjectionKind::RelationshipEquality,
            |candidate| candidate == value,
            |block| block.min_key <= *value && *value <= block.max_key,
            &mut |id| consumer(RelId(id.0)),
        )
    }

    pub fn scan_relationship_range_candidates(
        &self,
        rel_type: RelTypeId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        mut consumer: impl FnMut(
            RelId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.scan_candidates(
            LabelId(rel_type.0),
            property,
            PersistentPropertyProjectionKind::RelationshipRange,
            |value| range_bounds_match(value, lower, upper),
            |block| range_block_might_match(block, lower, upper),
            &mut |id| consumer(RelId(id.0)),
        )
    }

    pub fn estimate_relationship_equality_entries(
        &self,
        rel_type: RelTypeId,
        property: &str,
        value: &Value,
    ) -> u64 {
        self.blocks_for_definition(
            PersistentPropertyProjectionKind::RelationshipEquality,
            LabelId(rel_type.0),
            property,
        )
        .iter()
        .filter(|block| block.min_key <= *value && *value <= block.max_key)
        .map(|block| u64::from(block.entry_count))
        .sum()
    }

    pub fn estimate_relationship_range_entries(
        &self,
        rel_type: RelTypeId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> u64 {
        self.blocks_for_definition(
            PersistentPropertyProjectionKind::RelationshipRange,
            LabelId(rel_type.0),
            property,
        )
        .iter()
        .filter(|block| range_block_might_match(block, lower, upper))
        .map(|block| u64::from(block.entry_count))
        .sum()
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
        self.blocks_for_definition(
            PersistentPropertyProjectionKind::FullText,
            label_id,
            property,
        )
        .iter()
        .filter(|block| block.min_key <= token && token <= block.max_key)
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
        for block in self.blocks_for_definition(kind, label_id, property) {
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

    fn blocks_for_definition(
        &self,
        kind: PersistentPropertyProjectionKind,
        label_id: LabelId,
        property: &str,
    ) -> &[PersistentPropertyProjectionBlockDescriptor] {
        let target = (kind, label_id, property);
        let start = self.manifest.blocks.partition_point(|block| {
            (block.kind, block.label_id, block.property.as_str()) < target
        });
        let length = self.manifest.blocks[start..].partition_point(|block| {
            (block.kind, block.label_id, block.property.as_str()) == target
        });
        &self.manifest.blocks[start..start + length]
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
    let composite_arity = if kind == PersistentPropertyProjectionKind::CompositeEquality {
        Some(decode_composite_property_identity(&property)?.len())
    } else {
        None
    };
    let mut first = None;
    let mut previous = None;
    for _ in 0..entry_count {
        let value_len = cursor.read_u32()? as usize;
        let value = decode_standalone_value(cursor.read_exact(value_len)?)?;
        if composite_arity.is_some_and(|arity| !composite_key_has_arity(&value, arity)) {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "composite property projection block {} contains a key with invalid arity",
                descriptor.block_id
            )));
        }
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

fn composite_key_has_arity(value: &Value, arity: usize) -> bool {
    matches!(value, Value::List(values) if values.len() == arity)
}

fn composite_key_ordering(value: &Value, expected: &[&Value]) -> Option<std::cmp::Ordering> {
    let Value::List(values) = value else {
        return None;
    };
    for (left, right) in values.iter().zip(expected) {
        let ordering = left.cmp(right);
        if !ordering.is_eq() {
            return Some(ordering);
        }
    }
    Some(values.len().cmp(&expected.len()))
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

fn definition_subject(definition: &PersistentPropertyProjectionDefinition) -> ProjectionSubject {
    match definition.kind {
        PersistentPropertyProjectionKind::RelationshipEquality
        | PersistentPropertyProjectionKind::RelationshipRange => {
            ProjectionSubject::Relationship(RelTypeId(definition.label_id.0))
        }
        PersistentPropertyProjectionKind::Equality
        | PersistentPropertyProjectionKind::Range
        | PersistentPropertyProjectionKind::FullText
        | PersistentPropertyProjectionKind::CompositeEquality => {
            ProjectionSubject::Node(definition.label_id)
        }
    }
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
        PersistentPropertyProjectionKind::Equality => 3,
        PersistentPropertyProjectionKind::Range => 1,
        PersistentPropertyProjectionKind::FullText => 2,
        PersistentPropertyProjectionKind::CompositeEquality => 4,
        PersistentPropertyProjectionKind::RelationshipEquality => 5,
        PersistentPropertyProjectionKind::RelationshipRange => 6,
    }
}

fn kind_from_tag(
    tag: u8,
) -> Result<PersistentPropertyProjectionKind, PersistentPropertyProjectionError> {
    match tag {
        1 => Ok(PersistentPropertyProjectionKind::Range),
        2 => Ok(PersistentPropertyProjectionKind::FullText),
        3 => Ok(PersistentPropertyProjectionKind::Equality),
        4 => Ok(PersistentPropertyProjectionKind::CompositeEquality),
        5 => Ok(PersistentPropertyProjectionKind::RelationshipEquality),
        6 => Ok(PersistentPropertyProjectionKind::RelationshipRange),
        _ => Err(PersistentPropertyProjectionError::Corrupt(format!(
            "invalid property projection kind {tag}"
        ))),
    }
}

fn decode_composite_property_identity(
    identity: &str,
) -> Result<Vec<String>, PersistentPropertyProjectionError> {
    let encoded = identity
        .strip_prefix(COMPOSITE_PROPERTY_IDENTITY_PREFIX)
        .and_then(|suffix| suffix.strip_prefix(':'))
        .ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "composite property projection has an invalid identity prefix".to_string(),
            )
        })?;
    let properties = encoded
        .split(':')
        .map(|property| decode_utf8_hex(property, "composite property identity"))
        .collect::<Result<Vec<_>, _>>()?;
    if properties.len() < 2 {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "composite property projection identity has fewer than two properties".to_string(),
        ));
    }
    Ok(properties)
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

fn write_hashed(
    writer: &mut impl Write,
    digest: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn write_double_hashed(
    writer: &mut impl Write,
    artifact_digest: &mut IntegrityHasher,
    block_digest: &mut Crc32cHasher,
    bytes: &[u8],
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(bytes)?;
    artifact_digest.update(bytes);
    block_digest.update(bytes);
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

    fn relationship(id: u64, rank: i64) -> RelRecord {
        RelRecord {
            id: RelId(id),
            source: NodeId(1),
            target: NodeId(2),
            rel_type: RelTypeId(1),
            properties: BTreeMap::from([("rank".to_string(), Value::Int(rank))]),
        }
    }

    #[test]
    fn external_projection_round_trips_node_composite_and_relationship_candidates() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "skein-property-projection-{}-{nonce}",
            std::process::id(),
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
        let composite_properties = vec!["rank".to_string(), "text".to_string()];
        let definitions = vec![
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::Equality,
                complete: false,
            },
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
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: persistent_composite_property_identity(&composite_properties).unwrap(),
                kind: PersistentPropertyProjectionKind::CompositeEquality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipEquality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipRange,
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
                    Ok(PersistentPropertyProjectionRecord::Node(node(
                        1,
                        10,
                        "Graph Memory",
                    ))),
                    Ok(PersistentPropertyProjectionRecord::Node(node(
                        2, 20, "Other",
                    ))),
                    Ok(PersistentPropertyProjectionRecord::Node(node(
                        3,
                        30,
                        "Memory Graph",
                    ))),
                    Ok(PersistentPropertyProjectionRecord::Relationship(
                        relationship(7, 20),
                    )),
                ],
            )
            .unwrap();
        assert!(output.report.spill_run_count > 1);
        let manifest =
            PersistentPropertyProjectionManifest::decode(&output.manifest.encode().unwrap())
                .unwrap();
        let block_count = manifest.blocks.len();
        let cache = Arc::new(SegmentCache::new(1024 * 1024));
        let reader = PersistentPropertyProjectionReader::open(
            &path,
            manifest,
            Arc::clone(&cache),
            StoreId(4),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(cache.snapshot().resident_bytes, 0);
        let mut equality = Vec::new();
        let (equality_report, _) = reader
            .scan_equality_candidates(LabelId(1), "rank", &Value::Int(20), |id| {
                equality.push(id.0);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(equality, vec![2]);
        assert_eq!(equality_report.blocks_read, 1);
        assert!(equality_report.blocks_considered < block_count as u64);
        assert!(equality_report.blocks_read < block_count as u64);
        assert_eq!(cache.snapshot().entry_count, 1);
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
        assert!(reader
            .manifest()
            .supports_composite_equality(LabelId(1), &composite_properties));
        let mut composite = Vec::new();
        let (composite_report, _) = reader
            .scan_composite_equality_candidates(
                LabelId(1),
                &composite_properties,
                &[&Value::Int(20), &Value::String("Other".to_string())],
                |id| {
                    composite.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(composite, vec![2]);
        assert_eq!(composite_report.candidates_returned, 1);

        composite.clear();
        reader
            .scan_composite_equality_candidates(
                LabelId(1),
                &composite_properties,
                &[&Value::Int(20), &Value::String("Graph Memory".to_string())],
                |id| {
                    composite.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert!(composite.is_empty());
        let mut relationships = Vec::new();
        reader
            .scan_relationship_equality_candidates(RelTypeId(1), "rank", &Value::Int(20), |id| {
                relationships.push(id.0);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(relationships, vec![7]);
        relationships.clear();
        reader
            .scan_relationship_range_candidates(
                RelTypeId(1),
                "rank",
                Some(&(Value::Int(15), true)),
                Some(&(Value::Int(25), true)),
                |id| {
                    relationships.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(relationships, vec![7]);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn composite_property_identity_is_unambiguous_and_validated() {
        let properties = vec!["a:b".to_string(), "".to_string(), "\u{1f9f5}".to_string()];
        let identity = persistent_composite_property_identity(&properties).unwrap();
        assert_eq!(
            decode_composite_property_identity(&identity).unwrap(),
            properties
        );

        let error = persistent_composite_property_identity(&["only".to_string()]).unwrap_err();
        assert!(error.to_string().contains("at least two properties"));
        let error =
            decode_composite_property_identity("skein-composite-property-v1:zz:61").unwrap_err();
        assert!(error.to_string().contains("invalid hexadecimal data"));
    }

    #[test]
    fn oversized_composite_key_disables_the_projection_without_partial_coverage() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "skein-oversized-composite-projection-{}-{nonce}",
            std::process::id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("projection.skein");
        let properties = vec!["rank".to_string(), "text".to_string()];
        let definition = PersistentPropertyProjectionDefinition {
            label_id: LabelId(1),
            property: persistent_composite_property_identity(&properties).unwrap(),
            kind: PersistentPropertyProjectionKind::CompositeEquality,
            complete: false,
        };
        let output = PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig {
            max_index_key_bytes: NonZeroU64::new(8).unwrap(),
            ..PersistentPropertyProjectionConfig::default()
        })
        .write_fallible(
            &path,
            ManifestGeneration(3),
            12,
            vec![definition],
            vec![Ok(PersistentPropertyProjectionRecord::Node(node(
                1,
                10,
                "long composite value",
            )))],
        )
        .unwrap();

        assert_eq!(output.manifest.entry_count, 0);
        assert!(!output
            .manifest
            .supports_composite_equality(LabelId(1), &properties));
        let reader = PersistentPropertyProjectionReader::open(
            &path,
            output.manifest,
            Arc::new(SegmentCache::new(1024)),
            StoreId(5),
            NonZeroU64::new(1024).unwrap(),
        )
        .unwrap();
        let error = reader
            .scan_composite_equality_candidates(
                LabelId(1),
                &properties,
                &[
                    &Value::Int(10),
                    &Value::String("long composite value".to_string()),
                ],
                |_| Ok(CanonicalScanControl::Continue),
            )
            .unwrap_err();
        assert!(error.to_string().contains("unavailable or incomplete"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn definition_admission_rejects_before_artifact_creation() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "skein-property-projection-definition-budget-{}-{nonce}",
            std::process::id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let definitions = vec![
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipEquality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipRange,
                complete: false,
            },
        ];
        let count_path = root.join("count.skein");
        let count_error =
            PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig {
                max_definition_count: NonZeroUsize::new(1).unwrap(),
                ..PersistentPropertyProjectionConfig::default()
            })
            .write_fallible(
                &count_path,
                ManifestGeneration(1),
                1,
                definitions.clone(),
                Vec::<Result<_, PersistentPropertyProjectionError>>::new(),
            )
            .unwrap_err();
        assert!(matches!(
            count_error,
            PersistentPropertyProjectionError::DefinitionCountBudgetExceeded {
                required_definitions: 2,
                max_definitions: 1,
            }
        ));
        assert!(!count_path.exists());

        let bytes_path = root.join("bytes.skein");
        let bytes_error =
            PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig {
                max_definition_bytes: NonZeroU64::new(1).unwrap(),
                ..PersistentPropertyProjectionConfig::default()
            })
            .write_fallible(
                &bytes_path,
                ManifestGeneration(1),
                1,
                definitions,
                Vec::<Result<_, PersistentPropertyProjectionError>>::new(),
            )
            .unwrap_err();
        assert!(matches!(
            bytes_error,
            PersistentPropertyProjectionError::DefinitionBytesBudgetExceeded { max_bytes: 1, .. }
        ));
        assert!(!bytes_path.exists());
        fs::remove_dir_all(root).unwrap();
    }
}
