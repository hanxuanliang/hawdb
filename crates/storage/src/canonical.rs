use crate::{
    content_digest, durable_replace_file, ContentDigest, FileSegmentRangeReader,
    ManifestGeneration, NodeId, NodeRecord, PropertySpillConfig, PropertySpillError,
    PropertySpillManifest, PropertySpillReader, PropertySpillWriter, RelId, RelRecord,
    SegmentCache, SegmentRangeReader, SegmentReadError, SegmentReadRange, StoreId,
};
use skein_core::{LabelId, RelTypeId, Value};
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINCANONICAL01";
const MANIFEST_HEADER: &str = "SKEIN_CANONICAL_MANIFEST_V1";
const SEGMENT_HEADER: &[u8; 8] = b"SKNSEG01";
const ARTIFACT_ID: u64 = 0x534b_4341_4e4f_4e31;
const MAX_VALUE_DEPTH: usize = 32;
const LEGACY_BLOOM_WORDS: usize = 4;
const BLOOM_MIN_WORDS: usize = 4;
const BLOOM_MAX_WORDS: usize = 16 * 1024;
const BLOOM_BITS_PER_ITEM: usize = 10;
const BLOOM_HASHES: u8 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CanonicalSegmentKind {
    Nodes,
    Relationships,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalScanControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalEndpointDirection {
    Source,
    Target,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalEndpointBloom {
    words: Box<[u64]>,
    hash_count: u8,
}

impl Default for CanonicalEndpointBloom {
    fn default() -> Self {
        Self {
            words: vec![0; BLOOM_MIN_WORDS].into_boxed_slice(),
            hash_count: BLOOM_HASHES,
        }
    }
}

impl CanonicalEndpointBloom {
    fn legacy_unbounded() -> Self {
        Self {
            words: vec![u64::MAX].into_boxed_slice(),
            hash_count: 1,
        }
    }

    fn from_keys(keys: &[u64]) -> Self {
        if keys.is_empty() {
            return Self::default();
        }
        let requested_bits = keys.len().saturating_mul(BLOOM_BITS_PER_ITEM);
        let word_count = requested_bits
            .div_ceil(u64::BITS as usize)
            .clamp(BLOOM_MIN_WORDS, BLOOM_MAX_WORDS);
        let mut bloom = Self {
            words: vec![0; word_count].into_boxed_slice(),
            hash_count: BLOOM_HASHES,
        };
        for key in keys {
            bloom.insert(*key);
        }
        bloom
    }

    fn insert(&mut self, id: u64) {
        for seed in 0..u64::from(self.hash_count) {
            let bit =
                endpoint_bloom_hash(id, seed) as usize % (self.words.len() * u64::BITS as usize);
            self.words[bit / u64::BITS as usize] |= 1u64 << (bit % u64::BITS as usize);
        }
    }

    pub fn might_contain(&self, id: u64) -> bool {
        (0..u64::from(self.hash_count)).all(|seed| {
            let bit =
                endpoint_bloom_hash(id, seed) as usize % (self.words.len() * u64::BITS as usize);
            self.words[bit / u64::BITS as usize] & (1u64 << (bit % u64::BITS as usize)) != 0
        })
    }

    pub fn bit_len(&self) -> usize {
        self.words.len().saturating_mul(u64::BITS as usize)
    }

    pub fn estimated_false_positive_rate(&self) -> f64 {
        let set_bits = self
            .words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum::<usize>();
        if set_bits == 0 {
            return 0.0;
        }
        (set_bits as f64 / self.bit_len() as f64).powi(i32::from(self.hash_count))
    }

    fn encode(&self) -> String {
        format!("{:02x}", self.hash_count)
            + &self
                .words
                .iter()
                .map(|word| format!("{word:016x}"))
                .collect::<String>()
    }

    fn decode(value: &str) -> Result<Self, CanonicalSegmentError> {
        let (hash_count, words) = if value.len() == LEGACY_BLOOM_WORDS * 16 {
            (3, value)
        } else {
            if value.len() < 18 || !(value.len() - 2).is_multiple_of(16) {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical endpoint bloom has an invalid length".to_string(),
                ));
            }
            let hash_count = u8::from_str_radix(&value[..2], 16).map_err(|_| {
                CanonicalSegmentError::Corrupt(
                    "canonical endpoint bloom has an invalid hash count".to_string(),
                )
            })?;
            (hash_count, &value[2..])
        };
        let word_count = words.len() / 16;
        if hash_count == 0 || word_count == 0 || word_count > BLOOM_MAX_WORDS {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical endpoint bloom exceeds its format bounds".to_string(),
            ));
        }
        let mut decoded = vec![0u64; word_count];
        for (index, word) in decoded.iter_mut().enumerate() {
            let start = index * 16;
            *word = u64::from_str_radix(&words[start..start + 16], 16).map_err(|_| {
                CanonicalSegmentError::Corrupt(
                    "canonical endpoint bloom contains invalid hexadecimal data".to_string(),
                )
            })?;
        }
        Ok(Self {
            words: decoded.into_boxed_slice(),
            hash_count,
        })
    }
}

impl CanonicalSegmentKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Nodes => 1,
            Self::Relationships => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, CanonicalSegmentError> {
        match tag {
            1 => Ok(Self::Nodes),
            2 => Ok(Self::Relationships),
            _ => Err(CanonicalSegmentError::Corrupt(format!(
                "unknown canonical segment kind {tag}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSegmentDescriptor {
    pub segment_id: u64,
    pub kind: CanonicalSegmentKind,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub min_record_id: u64,
    pub max_record_id: u64,
    pub record_count: u32,
    pub source_endpoint_bloom: CanonicalEndpointBloom,
    pub target_endpoint_bloom: CanonicalEndpointBloom,
    pub node_property_bloom: CanonicalEndpointBloom,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSegmentManifest {
    pub generation: ManifestGeneration,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub node_count: u64,
    pub relationship_count: u64,
    pub segments: Vec<CanonicalSegmentDescriptor>,
}

impl CanonicalSegmentManifest {
    pub fn node_segments(&self) -> impl Iterator<Item = &CanonicalSegmentDescriptor> {
        self.segments
            .iter()
            .filter(|segment| segment.kind == CanonicalSegmentKind::Nodes)
    }

    pub fn relationship_segments(&self) -> impl Iterator<Item = &CanonicalSegmentDescriptor> {
        self.segments
            .iter()
            .filter(|segment| segment.kind == CanonicalSegmentKind::Relationships)
    }

    pub fn validate(&self) -> Result<(), CanonicalSegmentError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest has an unsupported artifact id".to_string(),
            ));
        }
        let mut previous_end = ARTIFACT_HEADER.len() as u64 + 8;
        let mut previous_id = None;
        let mut node_count = 0u64;
        let mut relationship_count = 0u64;
        for segment in &self.segments {
            if segment.offset < previous_end {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical segment ranges overlap or are out of order".to_string(),
                ));
            }
            if previous_id.is_some_and(|id| segment.segment_id <= id) {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical segment ids are not strictly increasing".to_string(),
                ));
            }
            if segment.record_count == 0 || segment.min_record_id > segment.max_record_id {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "canonical segment {} has invalid record bounds",
                    segment.segment_id
                )));
            }
            previous_end = segment
                .offset
                .checked_add(segment.length.get())
                .ok_or_else(|| {
                    CanonicalSegmentError::Corrupt(
                        "canonical segment range overflows u64".to_string(),
                    )
                })?;
            previous_id = Some(segment.segment_id);
            match segment.kind {
                CanonicalSegmentKind::Nodes => {
                    node_count = node_count.saturating_add(u64::from(segment.record_count));
                }
                CanonicalSegmentKind::Relationships => {
                    relationship_count =
                        relationship_count.saturating_add(u64::from(segment.record_count));
                }
            }
        }
        if previous_end != self.artifact_len
            || node_count != self.node_count
            || relationship_count != self.relationship_count
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest counts or artifact length are inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, CanonicalSegmentError> {
        self.validate()?;
        let mut body = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nnode_count\t{}\nrelationship_count\t{}\n",
            self.generation.0,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.node_count,
            self.relationship_count
        );
        for segment in &self.segments {
            body.push_str(&format!(
                "segment\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                segment.segment_id,
                segment.kind.tag(),
                segment.offset,
                segment.length,
                segment.content_digest.0,
                segment.min_record_id,
                segment.max_record_id,
                segment.record_count,
                segment.source_endpoint_bloom.encode(),
                segment.target_endpoint_bloom.encode(),
                segment.node_property_bloom.encode()
            ));
        }
        let checksum = content_digest(body.as_bytes()).0;
        Ok(format!("{body}checksum\t{checksum}\n"))
    }

    pub fn decode(text: &str) -> Result<Self, CanonicalSegmentError> {
        let marker = "checksum\t";
        let checksum_offset = text.rfind(marker).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical manifest is missing its checksum".to_string())
        })?;
        let body = &text[..checksum_offset];
        let checksum_line = text[checksum_offset..].trim_end();
        if checksum_line.contains('\n') {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest has data after its checksum".to_string(),
            ));
        }
        let expected = parse_u64(
            checksum_line.strip_prefix(marker).unwrap_or_default(),
            "canonical manifest checksum",
        )?;
        let actual = content_digest(body.as_bytes()).0;
        if actual != expected {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical manifest checksum mismatch: expected {expected}, got {actual}"
            )));
        }

        let mut generation = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut node_count = None;
        let mut relationship_count = None;
        let mut segments = Vec::new();
        let mut saw_header = false;
        for line in body.lines() {
            if line == MANIFEST_HEADER {
                saw_header = true;
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["generation", value] => set_once(
                    &mut generation,
                    parse_u64(value, "generation")?,
                    "generation",
                )?,
                ["artifact_id", value] => set_once(
                    &mut artifact_id,
                    parse_u64(value, "artifact id")?,
                    "artifact id",
                )?,
                ["artifact_len", value] => set_once(
                    &mut artifact_len,
                    parse_u64(value, "artifact length")?,
                    "artifact length",
                )?,
                ["artifact_digest", value] => set_once(
                    &mut artifact_digest,
                    parse_u64(value, "artifact digest")?,
                    "artifact digest",
                )?,
                ["node_count", value] => set_once(
                    &mut node_count,
                    parse_u64(value, "node count")?,
                    "node count",
                )?,
                ["relationship_count", value] => set_once(
                    &mut relationship_count,
                    parse_u64(value, "relationship count")?,
                    "relationship count",
                )?,
                ["segment", id, kind, offset, length, digest, min_id, max_id, count] => {
                    let kind = CanonicalSegmentKind::from_tag(parse_u8(kind, "segment kind")?)?;
                    segments.push(CanonicalSegmentDescriptor {
                        segment_id: parse_u64(id, "segment id")?,
                        kind,
                        offset: parse_u64(offset, "segment offset")?,
                        length: NonZeroU64::new(parse_u64(length, "segment length")?).ok_or_else(
                            || {
                                CanonicalSegmentError::Corrupt(
                                    "canonical segment length must be non-zero".to_string(),
                                )
                            },
                        )?,
                        content_digest: ContentDigest(parse_u64(digest, "segment digest")?),
                        min_record_id: parse_u64(min_id, "minimum record id")?,
                        max_record_id: parse_u64(max_id, "maximum record id")?,
                        record_count: parse_u32(count, "segment record count")?,
                        source_endpoint_bloom: if kind == CanonicalSegmentKind::Relationships {
                            CanonicalEndpointBloom::legacy_unbounded()
                        } else {
                            CanonicalEndpointBloom::default()
                        },
                        target_endpoint_bloom: if kind == CanonicalSegmentKind::Relationships {
                            CanonicalEndpointBloom::legacy_unbounded()
                        } else {
                            CanonicalEndpointBloom::default()
                        },
                        node_property_bloom: if kind == CanonicalSegmentKind::Nodes {
                            CanonicalEndpointBloom::legacy_unbounded()
                        } else {
                            CanonicalEndpointBloom::default()
                        },
                    });
                }
                ["segment", id, kind, offset, length, digest, min_id, max_id, count, source_bloom, target_bloom] =>
                {
                    let kind = CanonicalSegmentKind::from_tag(parse_u8(kind, "segment kind")?)?;
                    segments.push(CanonicalSegmentDescriptor {
                        segment_id: parse_u64(id, "segment id")?,
                        kind,
                        offset: parse_u64(offset, "segment offset")?,
                        length: NonZeroU64::new(parse_u64(length, "segment length")?).ok_or_else(
                            || {
                                CanonicalSegmentError::Corrupt(
                                    "canonical segment length must be non-zero".to_string(),
                                )
                            },
                        )?,
                        content_digest: ContentDigest(parse_u64(digest, "segment digest")?),
                        min_record_id: parse_u64(min_id, "minimum record id")?,
                        max_record_id: parse_u64(max_id, "maximum record id")?,
                        record_count: parse_u32(count, "segment record count")?,
                        source_endpoint_bloom: CanonicalEndpointBloom::decode(source_bloom)?,
                        target_endpoint_bloom: CanonicalEndpointBloom::decode(target_bloom)?,
                        node_property_bloom: if kind == CanonicalSegmentKind::Nodes {
                            CanonicalEndpointBloom::legacy_unbounded()
                        } else {
                            CanonicalEndpointBloom::default()
                        },
                    });
                }
                ["segment", id, kind, offset, length, digest, min_id, max_id, count, source_bloom, target_bloom, property_bloom] =>
                {
                    segments.push(CanonicalSegmentDescriptor {
                        segment_id: parse_u64(id, "segment id")?,
                        kind: CanonicalSegmentKind::from_tag(parse_u8(kind, "segment kind")?)?,
                        offset: parse_u64(offset, "segment offset")?,
                        length: NonZeroU64::new(parse_u64(length, "segment length")?).ok_or_else(
                            || {
                                CanonicalSegmentError::Corrupt(
                                    "canonical segment length must be non-zero".to_string(),
                                )
                            },
                        )?,
                        content_digest: ContentDigest(parse_u64(digest, "segment digest")?),
                        min_record_id: parse_u64(min_id, "minimum record id")?,
                        max_record_id: parse_u64(max_id, "maximum record id")?,
                        record_count: parse_u32(count, "segment record count")?,
                        source_endpoint_bloom: CanonicalEndpointBloom::decode(source_bloom)?,
                        target_endpoint_bloom: CanonicalEndpointBloom::decode(target_bloom)?,
                        node_property_bloom: CanonicalEndpointBloom::decode(property_bloom)?,
                    });
                }
                [""] => {}
                _ => {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "invalid canonical manifest line: {line}"
                    )));
                }
            }
        }
        if !saw_header {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest is missing its format header".to_string(),
            ));
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            node_count: required(node_count, "node count")?,
            relationship_count: required(relationship_count, "relationship count")?,
            segments,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalSegmentConfig {
    pub target_segment_bytes: NonZeroU64,
    pub max_record_bytes: NonZeroU64,
}

impl Default for CanonicalSegmentConfig {
    fn default() -> Self {
        Self {
            target_segment_bytes: NonZeroU64::new(4 * 1024 * 1024)
                .expect("default canonical segment size is non-zero"),
            max_record_bytes: NonZeroU64::new(16 * 1024 * 1024)
                .expect("default canonical record size is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalReadReport {
    pub segments_considered: u64,
    pub segments_pruned: u64,
    pub segments_read: u64,
    pub bytes_read: u64,
    pub records_decoded: u64,
    pub peak_segment_bytes: u64,
}

#[derive(Debug)]
pub enum CanonicalSegmentError {
    Io(std::io::Error),
    Read(SegmentReadError),
    PropertySpill(PropertySpillError),
    RecordTooLarge { record_bytes: u64, max_bytes: u64 },
    SegmentTooLarge { segment_bytes: u64, max_bytes: u64 },
    Source(String),
    Corrupt(String),
}

impl Display for CanonicalSegmentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::PropertySpill(error) => Display::fmt(error, formatter),
            Self::RecordTooLarge {
                record_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical record uses {record_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::SegmentTooLarge {
                segment_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical segment uses {segment_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::Source(message) | Self::Corrupt(message) => formatter.write_str(message),
        }
    }
}

impl Error for CanonicalSegmentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::PropertySpill(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CanonicalSegmentError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for CanonicalSegmentError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

impl From<PropertySpillError> for CanonicalSegmentError {
    fn from(error: PropertySpillError) -> Self {
        Self::PropertySpill(error)
    }
}

pub struct CanonicalSegmentWriter {
    config: CanonicalSegmentConfig,
}

impl CanonicalSegmentWriter {
    pub const fn new(config: CanonicalSegmentConfig) -> Self {
        Self { config }
    }

    pub fn write<N, R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        nodes: N,
        relationships: R,
    ) -> Result<CanonicalSegmentManifest, CanonicalSegmentError>
    where
        N: IntoIterator,
        N::Item: Borrow<NodeRecord>,
        R: IntoIterator,
        R::Item: Borrow<RelRecord>,
    {
        self.write_fallible(
            path,
            generation,
            nodes.into_iter().map(|node| Ok(node.borrow().clone())),
            relationships
                .into_iter()
                .map(|relationship| Ok(relationship.borrow().clone())),
        )
    }

    pub fn write_fallible<N, R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        nodes: N,
        relationships: R,
    ) -> Result<CanonicalSegmentManifest, CanonicalSegmentError>
    where
        N: IntoIterator<Item = Result<NodeRecord, CanonicalSegmentError>>,
        R: IntoIterator<Item = Result<RelRecord, CanonicalSegmentError>>,
    {
        let tmp_path = path.with_extension("skein.tmp");
        let result = self.write_inner(&tmp_path, generation, nodes, relationships, None);
        let manifest = match result {
            Ok(manifest) => manifest,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        durable_replace_file(&tmp_path, path)?;
        Ok(manifest)
    }

    pub fn write_fallible_with_property_spills<N, R>(
        &self,
        path: &Path,
        property_spill_path: &Path,
        generation: ManifestGeneration,
        nodes: N,
        relationships: R,
        property_spill_config: PropertySpillConfig,
    ) -> Result<(CanonicalSegmentManifest, PropertySpillManifest), CanonicalSegmentError>
    where
        N: IntoIterator<Item = Result<NodeRecord, CanonicalSegmentError>>,
        R: IntoIterator<Item = Result<RelRecord, CanonicalSegmentError>>,
    {
        let tmp_path = path.with_extension("skein.tmp");
        let spill_tmp_path = property_spill_path.with_extension("skein.tmp");
        let mut spill_writer =
            PropertySpillWriter::create(&spill_tmp_path, generation, property_spill_config)?;
        let result = self.write_inner(
            &tmp_path,
            generation,
            nodes,
            relationships,
            Some(&mut spill_writer),
        );
        let canonical_manifest = match result {
            Ok(manifest) => manifest,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                let _ = fs::remove_file(&spill_tmp_path);
                return Err(error);
            }
        };
        let spill_manifest = match spill_writer.finish() {
            Ok(manifest) => manifest,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                let _ = fs::remove_file(&spill_tmp_path);
                return Err(error.into());
            }
        };
        durable_replace_file(&spill_tmp_path, property_spill_path)?;
        durable_replace_file(&tmp_path, path)?;
        Ok((canonical_manifest, spill_manifest))
    }

    fn write_inner<N, R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        nodes: N,
        relationships: R,
        mut property_spills: Option<&mut PropertySpillWriter>,
    ) -> Result<CanonicalSegmentManifest, CanonicalSegmentError>
    where
        N: IntoIterator<Item = Result<NodeRecord, CanonicalSegmentError>>,
        R: IntoIterator<Item = Result<RelRecord, CanonicalSegmentError>>,
    {
        let mut file = File::create(path)?;
        let mut artifact_digest = DigestState::new();
        write_hashed(&mut file, &mut artifact_digest, ARTIFACT_HEADER)?;
        write_hashed(&mut file, &mut artifact_digest, &generation.0.to_le_bytes())?;
        let mut artifact_len = ARTIFACT_HEADER.len() as u64 + 8;
        let mut segment_id = 1u64;
        let mut segments = Vec::new();
        let mut node_count = 0u64;
        let mut relationship_count = 0u64;

        let mut accumulator = SegmentAccumulator::new(
            CanonicalSegmentKind::Nodes,
            generation,
            segment_id,
            self.config,
        );
        for node in nodes {
            let node = node?;
            let payload = encode_node_with_property_spills(&node, property_spills.as_deref_mut())?;
            if accumulator.would_exceed(node.id.0, payload.len()) && !accumulator.is_empty() {
                let descriptor =
                    accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
                artifact_len = artifact_len.saturating_add(descriptor.length.get());
                node_count = node_count.saturating_add(u64::from(descriptor.record_count));
                segments.push(descriptor);
                segment_id = segment_id.saturating_add(1);
                accumulator = SegmentAccumulator::new(
                    CanonicalSegmentKind::Nodes,
                    generation,
                    segment_id,
                    self.config,
                );
            }
            accumulator.add_node_properties(&node)?;
            accumulator.push(node.id.0, &payload, None)?;
        }
        if !accumulator.is_empty() {
            let descriptor = accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
            artifact_len = artifact_len.saturating_add(descriptor.length.get());
            node_count = node_count.saturating_add(u64::from(descriptor.record_count));
            segments.push(descriptor);
            segment_id = segment_id.saturating_add(1);
        }

        let mut accumulator = SegmentAccumulator::new(
            CanonicalSegmentKind::Relationships,
            generation,
            segment_id,
            self.config,
        );
        for relationship in relationships {
            let relationship = relationship?;
            let payload = encode_relationship_with_property_spills(
                &relationship,
                property_spills.as_deref_mut(),
            )?;
            if accumulator.would_exceed(relationship.id.0, payload.len()) && !accumulator.is_empty()
            {
                let descriptor =
                    accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
                artifact_len = artifact_len.saturating_add(descriptor.length.get());
                relationship_count =
                    relationship_count.saturating_add(u64::from(descriptor.record_count));
                segments.push(descriptor);
                segment_id = segment_id.saturating_add(1);
                accumulator = SegmentAccumulator::new(
                    CanonicalSegmentKind::Relationships,
                    generation,
                    segment_id,
                    self.config,
                );
            }
            accumulator.push(
                relationship.id.0,
                &payload,
                Some((relationship.source.0, relationship.target.0)),
            )?;
        }
        if !accumulator.is_empty() {
            let descriptor = accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
            artifact_len = artifact_len.saturating_add(descriptor.length.get());
            relationship_count =
                relationship_count.saturating_add(u64::from(descriptor.record_count));
            segments.push(descriptor);
        }
        file.sync_all()?;
        let manifest = CanonicalSegmentManifest {
            generation,
            artifact_id: ARTIFACT_ID,
            artifact_len,
            artifact_digest: ContentDigest(artifact_digest.finish()),
            node_count,
            relationship_count,
            segments,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

struct SegmentAccumulator {
    kind: CanonicalSegmentKind,
    generation: ManifestGeneration,
    segment_id: u64,
    config: CanonicalSegmentConfig,
    records: Vec<u8>,
    min_record_id: Option<u64>,
    max_record_id: u64,
    record_count: u32,
    source_endpoint_keys: Vec<u64>,
    target_endpoint_keys: Vec<u64>,
    node_property_keys: Vec<u64>,
}

impl SegmentAccumulator {
    fn new(
        kind: CanonicalSegmentKind,
        generation: ManifestGeneration,
        segment_id: u64,
        config: CanonicalSegmentConfig,
    ) -> Self {
        Self {
            kind,
            generation,
            segment_id,
            config,
            records: Vec::new(),
            min_record_id: None,
            max_record_id: 0,
            record_count: 0,
            source_endpoint_keys: Vec::new(),
            target_endpoint_keys: Vec::new(),
            node_property_keys: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    fn would_exceed(&self, _record_id: u64, payload_bytes: usize) -> bool {
        segment_header_len()
            .saturating_add(self.records.len())
            .saturating_add(8 + 4)
            .saturating_add(payload_bytes)
            > usize::try_from(self.config.target_segment_bytes.get()).unwrap_or(usize::MAX)
    }

    fn push(
        &mut self,
        record_id: u64,
        payload: &[u8],
        endpoints: Option<(u64, u64)>,
    ) -> Result<(), CanonicalSegmentError> {
        let record_bytes = 8u64.saturating_add(4).saturating_add(payload.len() as u64);
        if record_bytes > self.config.max_record_bytes.get() {
            return Err(CanonicalSegmentError::RecordTooLarge {
                record_bytes,
                max_bytes: self.config.max_record_bytes.get(),
            });
        }
        self.records.extend_from_slice(&record_id.to_le_bytes());
        self.records
            .extend_from_slice(&u32_len(payload.len(), "canonical record")?.to_le_bytes());
        self.records.extend_from_slice(payload);
        self.min_record_id.get_or_insert(record_id);
        if record_id < self.max_record_id && self.record_count > 0 {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical records must be ordered by id".to_string(),
            ));
        }
        self.max_record_id = record_id;
        if let Some((source, target)) = endpoints {
            self.source_endpoint_keys.push(source);
            self.target_endpoint_keys.push(target);
        }
        self.record_count = self.record_count.checked_add(1).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical segment record count overflow".to_string())
        })?;
        Ok(())
    }

    fn add_node_properties(&mut self, node: &NodeRecord) -> Result<(), CanonicalSegmentError> {
        for label in &node.labels {
            for (property, value) in &node.properties {
                self.node_property_keys
                    .push(node_property_bloom_key(*label, property, value)?);
            }
        }
        Ok(())
    }

    fn flush(
        self,
        file: &mut File,
        artifact_digest: &mut DigestState,
        offset: u64,
    ) -> Result<CanonicalSegmentDescriptor, CanonicalSegmentError> {
        let mut bytes = Vec::with_capacity(segment_header_len().saturating_add(self.records.len()));
        bytes.extend_from_slice(SEGMENT_HEADER);
        bytes.push(self.kind.tag());
        bytes.extend_from_slice(&self.generation.0.to_le_bytes());
        bytes.extend_from_slice(&self.segment_id.to_le_bytes());
        bytes.extend_from_slice(&self.record_count.to_le_bytes());
        bytes.extend_from_slice(&self.records);
        let length = NonZeroU64::new(bytes.len() as u64).expect("segment bytes are non-zero");
        let hard_max = self.config.target_segment_bytes.get().max(
            self.config
                .max_record_bytes
                .get()
                .saturating_add(segment_header_len() as u64),
        );
        if length.get() > hard_max {
            return Err(CanonicalSegmentError::SegmentTooLarge {
                segment_bytes: length.get(),
                max_bytes: hard_max,
            });
        }
        let digest = content_digest(&bytes);
        write_hashed(file, artifact_digest, &bytes)?;
        Ok(CanonicalSegmentDescriptor {
            segment_id: self.segment_id,
            kind: self.kind,
            offset,
            length,
            content_digest: digest,
            min_record_id: self.min_record_id.expect("flushed segment is non-empty"),
            max_record_id: self.max_record_id,
            record_count: self.record_count,
            source_endpoint_bloom: CanonicalEndpointBloom::from_keys(&self.source_endpoint_keys),
            target_endpoint_bloom: CanonicalEndpointBloom::from_keys(&self.target_endpoint_keys),
            node_property_bloom: CanonicalEndpointBloom::from_keys(&self.node_property_keys),
        })
    }
}

#[derive(Debug, Clone)]
pub struct CanonicalSegmentReader {
    path: PathBuf,
    manifest: CanonicalSegmentManifest,
    range_reader: FileSegmentRangeReader,
    property_spills: Option<PropertySpillReader>,
    max_segment_bytes: NonZeroU64,
}

impl CanonicalSegmentReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: CanonicalSegmentManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_segment_bytes: NonZeroU64,
    ) -> Result<Self, CanonicalSegmentError> {
        Self::open_inner(
            path.into(),
            manifest,
            cache,
            store_id,
            max_segment_bytes,
            None,
        )
    }

    pub fn open_with_property_spills(
        path: impl Into<PathBuf>,
        manifest: CanonicalSegmentManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_segment_bytes: NonZeroU64,
        property_spills: PropertySpillReader,
    ) -> Result<Self, CanonicalSegmentError> {
        Self::open_inner(
            path.into(),
            manifest,
            cache,
            store_id,
            max_segment_bytes,
            Some(property_spills),
        )
    }

    fn open_inner(
        path: PathBuf,
        manifest: CanonicalSegmentManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_segment_bytes: NonZeroU64,
        property_spills: Option<PropertySpillReader>,
    ) -> Result<Self, CanonicalSegmentError> {
        manifest.validate()?;
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        for segment in &manifest.segments {
            if segment.length.get() > max_segment_bytes.get() {
                return Err(CanonicalSegmentError::SegmentTooLarge {
                    segment_bytes: segment.length.get(),
                    max_bytes: max_segment_bytes.get(),
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
            property_spills,
            max_segment_bytes,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn manifest(&self) -> &CanonicalSegmentManifest {
        &self.manifest
    }

    pub fn property_spill_manifest(&self) -> Option<&PropertySpillManifest> {
        self.property_spills
            .as_ref()
            .map(PropertySpillReader::manifest)
    }

    pub fn get_node(&self, id: NodeId) -> Result<Option<NodeRecord>, CanonicalSegmentError> {
        let Some(segment) = find_segment(self.manifest.node_segments(), id.0) else {
            return Ok(None);
        };
        let bytes = self.read_segment(segment)?;
        decode_node_by_id(
            &bytes,
            self.manifest.generation,
            segment,
            id.0,
            self.property_spills.as_ref(),
        )
    }

    pub fn get_relationship(&self, id: RelId) -> Result<Option<RelRecord>, CanonicalSegmentError> {
        let Some(segment) = find_segment(self.manifest.relationship_segments(), id.0) else {
            return Ok(None);
        };
        let bytes = self.read_segment(segment)?;
        decode_relationship_by_id(
            &bytes,
            self.manifest.generation,
            segment,
            id.0,
            self.property_spills.as_ref(),
        )
    }

    pub fn node_records(&self) -> CanonicalNodeIterator {
        CanonicalNodeIterator::new(self.clone())
    }

    pub fn relationship_records(&self) -> CanonicalRelationshipIterator {
        CanonicalRelationshipIterator::new(self.clone())
    }

    pub fn scan_nodes(
        &self,
        mut consumer: impl FnMut(NodeRecord) -> Result<(), CanonicalSegmentError>,
    ) -> Result<CanonicalReadReport, CanonicalSegmentError> {
        self.scan_nodes_control(|node| {
            consumer(node)?;
            Ok(CanonicalScanControl::Continue)
        })
        .map(|(report, _)| report)
    }

    pub fn scan_nodes_control(
        &self,
        mut consumer: impl FnMut(NodeRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        let mut report = CanonicalReadReport::default();
        for descriptor in self.manifest.node_segments() {
            report.segments_considered = report.segments_considered.saturating_add(1);
            let bytes = self.read_segment(descriptor)?;
            update_report_for_segment(&mut report, bytes.len());
            let mut control = CanonicalScanControl::Continue;
            decode_segment_records(
                &bytes,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    if control == CanonicalScanControl::Stop {
                        return Ok(());
                    }
                    control = consumer(decode_node_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                    )?)?;
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

    pub fn scan_relationships(
        &self,
        mut consumer: impl FnMut(RelRecord) -> Result<(), CanonicalSegmentError>,
    ) -> Result<CanonicalReadReport, CanonicalSegmentError> {
        self.scan_relationships_control(|relationship| {
            consumer(relationship)?;
            Ok(CanonicalScanControl::Continue)
        })
        .map(|(report, _)| report)
    }

    pub fn scan_relationships_control(
        &self,
        mut consumer: impl FnMut(RelRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        let mut report = CanonicalReadReport::default();
        for descriptor in self.manifest.relationship_segments() {
            report.segments_considered = report.segments_considered.saturating_add(1);
            let bytes = self.read_segment(descriptor)?;
            update_report_for_segment(&mut report, bytes.len());
            let mut control = CanonicalScanControl::Continue;
            decode_segment_records(
                &bytes,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    if control == CanonicalScanControl::Stop {
                        return Ok(());
                    }
                    control = consumer(decode_relationship_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                    )?)?;
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

    pub fn scan_relationships_for_endpoint_control(
        &self,
        node_id: NodeId,
        direction: CanonicalEndpointDirection,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        let mut report = CanonicalReadReport::default();
        for descriptor in self.manifest.relationship_segments() {
            report.segments_considered = report.segments_considered.saturating_add(1);
            let might_contain = match direction {
                CanonicalEndpointDirection::Source => &descriptor.source_endpoint_bloom,
                CanonicalEndpointDirection::Target => &descriptor.target_endpoint_bloom,
            }
            .might_contain(node_id.0);
            if !might_contain {
                report.segments_pruned = report.segments_pruned.saturating_add(1);
                continue;
            }
            let bytes = self.read_segment(descriptor)?;
            update_report_for_segment(&mut report, bytes.len());
            let mut control = CanonicalScanControl::Continue;
            decode_segment_records(
                &bytes,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    if control == CanonicalScanControl::Stop {
                        return Ok(());
                    }
                    let relationship = decode_relationship_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                    )?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    let endpoint_matches = match direction {
                        CanonicalEndpointDirection::Source => relationship.source == node_id,
                        CanonicalEndpointDirection::Target => relationship.target == node_id,
                    };
                    if endpoint_matches
                        && rel_type.is_none_or(|rel_type| relationship.rel_type == rel_type)
                    {
                        control = consumer(relationship)?;
                    }
                    Ok(())
                },
            )?;
            if control == CanonicalScanControl::Stop {
                return Ok((report, control));
            }
        }
        Ok((report, CanonicalScanControl::Continue))
    }

    pub fn scan_nodes_by_property_control(
        &self,
        label_id: LabelId,
        property: &str,
        value: &Value,
        mut consumer: impl FnMut(NodeRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        let bloom_key = node_property_bloom_key(label_id, property, value)?;
        let mut report = CanonicalReadReport::default();
        for descriptor in self.manifest.node_segments() {
            report.segments_considered = report.segments_considered.saturating_add(1);
            if !descriptor.node_property_bloom.might_contain(bloom_key) {
                report.segments_pruned = report.segments_pruned.saturating_add(1);
                continue;
            }
            let bytes = self.read_segment(descriptor)?;
            update_report_for_segment(&mut report, bytes.len());
            let mut control = CanonicalScanControl::Continue;
            decode_segment_records(
                &bytes,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    if control == CanonicalScanControl::Stop {
                        return Ok(());
                    }
                    let node = decode_node_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                    )?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    if node.labels.contains(&label_id)
                        && node.properties.get(property) == Some(value)
                    {
                        control = consumer(node)?;
                    }
                    Ok(())
                },
            )?;
            if control == CanonicalScanControl::Stop {
                return Ok((report, control));
            }
        }
        Ok((report, CanonicalScanControl::Continue))
    }

    fn read_segment(
        &self,
        descriptor: &CanonicalSegmentDescriptor,
    ) -> Result<Arc<[u8]>, CanonicalSegmentError> {
        if descriptor.length.get() > self.max_segment_bytes.get() {
            return Err(CanonicalSegmentError::SegmentTooLarge {
                segment_bytes: descriptor.length.get(),
                max_bytes: self.max_segment_bytes.get(),
            });
        }
        self.range_reader
            .read_range(
                &SegmentReadRange::new(
                    self.manifest.artifact_id,
                    descriptor.segment_id,
                    descriptor.offset,
                    descriptor.length,
                )
                .with_content_digest(descriptor.content_digest),
            )
            .map_err(Into::into)
    }
}

pub struct CanonicalNodeIterator {
    reader: CanonicalSegmentReader,
    descriptors: Vec<usize>,
    next_descriptor: usize,
    current: std::vec::IntoIter<NodeRecord>,
    failed: bool,
}

impl CanonicalNodeIterator {
    fn new(reader: CanonicalSegmentReader) -> Self {
        let descriptors = reader
            .manifest
            .segments
            .iter()
            .enumerate()
            .filter_map(|(index, descriptor)| {
                (descriptor.kind == CanonicalSegmentKind::Nodes).then_some(index)
            })
            .collect();
        Self {
            reader,
            descriptors,
            next_descriptor: 0,
            current: Vec::new().into_iter(),
            failed: false,
        }
    }
}

impl Iterator for CanonicalNodeIterator {
    type Item = Result<NodeRecord, CanonicalSegmentError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(node) = self.current.next() {
                return Some(Ok(node));
            }
            if self.failed || self.next_descriptor == self.descriptors.len() {
                return None;
            }
            let descriptor = &self.reader.manifest.segments[self.descriptors[self.next_descriptor]];
            self.next_descriptor += 1;
            let bytes = match self.reader.read_segment(descriptor) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let mut records = Vec::with_capacity(descriptor.record_count as usize);
            if let Err(error) = decode_segment_records(
                &bytes,
                self.reader.manifest.generation,
                descriptor,
                |id, payload| {
                    records.push(decode_node_with_property_spills(
                        id,
                        payload,
                        self.reader.property_spills.as_ref(),
                    )?);
                    Ok(())
                },
            ) {
                self.failed = true;
                return Some(Err(error));
            }
            self.current = records.into_iter();
        }
    }
}

pub struct CanonicalRelationshipIterator {
    reader: CanonicalSegmentReader,
    descriptors: Vec<usize>,
    next_descriptor: usize,
    current: std::vec::IntoIter<RelRecord>,
    failed: bool,
}

impl CanonicalRelationshipIterator {
    fn new(reader: CanonicalSegmentReader) -> Self {
        let descriptors = reader
            .manifest
            .segments
            .iter()
            .enumerate()
            .filter_map(|(index, descriptor)| {
                (descriptor.kind == CanonicalSegmentKind::Relationships).then_some(index)
            })
            .collect();
        Self {
            reader,
            descriptors,
            next_descriptor: 0,
            current: Vec::new().into_iter(),
            failed: false,
        }
    }
}

impl Iterator for CanonicalRelationshipIterator {
    type Item = Result<RelRecord, CanonicalSegmentError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(relationship) = self.current.next() {
                return Some(Ok(relationship));
            }
            if self.failed || self.next_descriptor == self.descriptors.len() {
                return None;
            }
            let descriptor = &self.reader.manifest.segments[self.descriptors[self.next_descriptor]];
            self.next_descriptor += 1;
            let bytes = match self.reader.read_segment(descriptor) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let mut records = Vec::with_capacity(descriptor.record_count as usize);
            if let Err(error) = decode_segment_records(
                &bytes,
                self.reader.manifest.generation,
                descriptor,
                |id, payload| {
                    records.push(decode_relationship_with_property_spills(
                        id,
                        payload,
                        self.reader.property_spills.as_ref(),
                    )?);
                    Ok(())
                },
            ) {
                self.failed = true;
                return Some(Err(error));
            }
            self.current = records.into_iter();
        }
    }
}

fn find_segment<'a>(
    mut segments: impl Iterator<Item = &'a CanonicalSegmentDescriptor>,
    id: u64,
) -> Option<&'a CanonicalSegmentDescriptor> {
    segments.find(|segment| segment.min_record_id <= id && id <= segment.max_record_id)
}

fn endpoint_bloom_hash(id: u64, seed: u64) -> u64 {
    let mut value = id.wrapping_add(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn node_property_bloom_key(
    label: LabelId,
    property: &str,
    value: &Value,
) -> Result<u64, CanonicalSegmentError> {
    let mut bytes = Vec::with_capacity(8usize.saturating_add(property.len()));
    bytes.extend_from_slice(&label.0.to_le_bytes());
    encode_string(property, &mut bytes)?;
    encode_value(value, &mut bytes, 0)?;
    Ok(content_digest(&bytes).0)
}

fn update_report_for_segment(report: &mut CanonicalReadReport, bytes: usize) {
    report.segments_read = report.segments_read.saturating_add(1);
    report.bytes_read = report.bytes_read.saturating_add(bytes as u64);
    report.peak_segment_bytes = report.peak_segment_bytes.max(bytes as u64);
}

fn decode_node_by_id(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    id: u64,
    property_spills: Option<&PropertySpillReader>,
) -> Result<Option<NodeRecord>, CanonicalSegmentError> {
    let mut found = None;
    decode_segment_records(bytes, generation, descriptor, |record_id, payload| {
        if record_id == id {
            found = Some(decode_node_with_property_spills(
                record_id,
                payload,
                property_spills,
            )?);
        }
        Ok(())
    })?;
    Ok(found)
}

fn decode_relationship_by_id(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    id: u64,
    property_spills: Option<&PropertySpillReader>,
) -> Result<Option<RelRecord>, CanonicalSegmentError> {
    let mut found = None;
    decode_segment_records(bytes, generation, descriptor, |record_id, payload| {
        if record_id == id {
            found = Some(decode_relationship_with_property_spills(
                record_id,
                payload,
                property_spills,
            )?);
        }
        Ok(())
    })?;
    Ok(found)
}

fn decode_segment_records(
    bytes: &[u8],
    expected_generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    mut consumer: impl FnMut(u64, &[u8]) -> Result<(), CanonicalSegmentError>,
) -> Result<(), CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(bytes);
    if cursor.read_exact(8)? != SEGMENT_HEADER {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} has an invalid header",
            descriptor.segment_id
        )));
    }
    let kind = CanonicalSegmentKind::from_tag(cursor.read_u8()?)?;
    let generation = cursor.read_u64()?;
    let segment_id = cursor.read_u64()?;
    let record_count = cursor.read_u32()?;
    if kind != descriptor.kind
        || generation != expected_generation.0
        || segment_id != descriptor.segment_id
        || record_count != descriptor.record_count
    {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} metadata does not match its manifest",
            descriptor.segment_id
        )));
    }
    let mut previous_id = None;
    for _ in 0..record_count {
        let id = cursor.read_u64()?;
        if previous_id.is_some_and(|previous| id <= previous) {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical segment {} record ids are not strictly increasing",
                descriptor.segment_id
            )));
        }
        let payload_len = cursor.read_u32()? as usize;
        let payload = cursor.read_exact(payload_len)?;
        consumer(id, payload)?;
        previous_id = Some(id);
    }
    if !cursor.is_empty() || previous_id.is_none() || previous_id != Some(descriptor.max_record_id)
    {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} payload bounds are inconsistent",
            descriptor.segment_id
        )));
    }
    Ok(())
}

fn encode_node_with_property_spills(
    node: &NodeRecord,
    property_spills: Option<&mut PropertySpillWriter>,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u32_len(node.labels.len(), "node labels")?.to_le_bytes());
    for label in &node.labels {
        bytes.extend_from_slice(&label.0.to_le_bytes());
    }
    encode_properties_with_property_spills(&node.properties, &mut bytes, property_spills)?;
    Ok(bytes)
}

fn decode_node_with_property_spills(
    id: u64,
    payload: &[u8],
    property_spills: Option<&PropertySpillReader>,
) -> Result<NodeRecord, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    let label_count = cursor.read_u32()? as usize;
    let mut labels = BTreeSet::new();
    for _ in 0..label_count {
        labels.insert(LabelId(cursor.read_u32()?));
    }
    let properties = decode_properties_with_property_spills(&mut cursor, 0, property_spills)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "node record has trailing bytes".to_string(),
        ));
    }
    Ok(NodeRecord {
        id: NodeId(id),
        labels,
        properties,
    })
}

pub(crate) fn encode_relationship(
    relationship: &RelRecord,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    encode_relationship_with_property_spills(relationship, None)
}

fn encode_relationship_with_property_spills(
    relationship: &RelRecord,
    property_spills: Option<&mut PropertySpillWriter>,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&relationship.source.0.to_le_bytes());
    bytes.extend_from_slice(&relationship.target.0.to_le_bytes());
    bytes.extend_from_slice(&relationship.rel_type.0.to_le_bytes());
    encode_properties_with_property_spills(&relationship.properties, &mut bytes, property_spills)?;
    Ok(bytes)
}

pub(crate) fn decode_relationship(
    id: u64,
    payload: &[u8],
) -> Result<RelRecord, CanonicalSegmentError> {
    decode_relationship_with_property_spills(id, payload, None)
}

fn decode_relationship_with_property_spills(
    id: u64,
    payload: &[u8],
    property_spills: Option<&PropertySpillReader>,
) -> Result<RelRecord, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    let source = NodeId(cursor.read_u64()?);
    let target = NodeId(cursor.read_u64()?);
    let rel_type = RelTypeId(cursor.read_u32()?);
    let properties = decode_properties_with_property_spills(&mut cursor, 0, property_spills)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "relationship record has trailing bytes".to_string(),
        ));
    }
    Ok(RelRecord {
        id: RelId(id),
        source,
        target,
        rel_type,
        properties,
    })
}

fn encode_properties(
    properties: &BTreeMap<String, Value>,
    output: &mut Vec<u8>,
    depth: usize,
) -> Result<(), CanonicalSegmentError> {
    output.extend_from_slice(&u32_len(properties.len(), "property map")?.to_le_bytes());
    for (key, value) in properties {
        encode_string(key, output)?;
        encode_value(value, output, depth.saturating_add(1))?;
    }
    Ok(())
}

fn encode_properties_with_property_spills(
    properties: &BTreeMap<String, Value>,
    output: &mut Vec<u8>,
    mut property_spills: Option<&mut PropertySpillWriter>,
) -> Result<(), CanonicalSegmentError> {
    output.extend_from_slice(&u32_len(properties.len(), "property map")?.to_le_bytes());
    for (key, value) in properties {
        encode_string(key, output)?;
        let mut encoded_value = Vec::new();
        encode_value(value, &mut encoded_value, 1)?;
        if let Some(spills) = property_spills.as_deref_mut()
            && spills.should_spill(encoded_value.len())
        {
            let spill_id = spills.push(encoded_value)?;
            output.push(7);
            output.extend_from_slice(&spill_id.to_le_bytes());
        } else {
            output.extend_from_slice(&encoded_value);
        }
    }
    Ok(())
}

fn decode_properties_with_property_spills(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
    property_spills: Option<&PropertySpillReader>,
) -> Result<BTreeMap<String, Value>, CanonicalSegmentError> {
    ensure_depth(depth)?;
    let count = cursor.read_u32()? as usize;
    let mut properties = BTreeMap::new();
    for _ in 0..count {
        let key = cursor.read_string()?;
        if properties
            .insert(
                key,
                decode_value_with_property_spills(
                    cursor,
                    depth.saturating_add(1),
                    property_spills,
                )?,
            )
            .is_some()
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical property map has duplicate keys".to_string(),
            ));
        }
    }
    Ok(properties)
}

fn encode_value(
    value: &Value,
    output: &mut Vec<u8>,
    depth: usize,
) -> Result<(), CanonicalSegmentError> {
    ensure_depth(depth)?;
    match value {
        Value::Null => output.push(0),
        Value::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        Value::Int(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            output.push(4);
            encode_string(value, output)?;
        }
        Value::List(values) => {
            output.push(5);
            output.extend_from_slice(&u32_len(values.len(), "value list")?.to_le_bytes());
            for value in values {
                encode_value(value, output, depth.saturating_add(1))?;
            }
        }
        Value::Map(values) => {
            output.push(6);
            encode_properties(values, output, depth.saturating_add(1))?;
        }
    }
    Ok(())
}

pub(crate) fn encode_standalone_value(value: &Value) -> Result<Vec<u8>, CanonicalSegmentError> {
    let mut encoded = Vec::new();
    encode_value(value, &mut encoded, 0)?;
    Ok(encoded)
}

fn decode_value(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
) -> Result<Value, CanonicalSegmentError> {
    decode_value_with_property_spills(cursor, depth, None)
}

pub(crate) fn decode_standalone_value(encoded: &[u8]) -> Result<Value, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(encoded);
    let value = decode_value(&mut cursor, 0)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "standalone canonical value has trailing bytes".to_string(),
        ));
    }
    Ok(value)
}

fn decode_value_with_property_spills(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
    property_spills: Option<&PropertySpillReader>,
) -> Result<Value, CanonicalSegmentError> {
    ensure_depth(depth)?;
    match cursor.read_u8()? {
        0 => Ok(Value::Null),
        1 => match cursor.read_u8()? {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            value => Err(CanonicalSegmentError::Corrupt(format!(
                "invalid canonical boolean {value}"
            ))),
        },
        2 => Ok(Value::Int(cursor.read_i64()?)),
        3 => Ok(Value::Float(f64::from_bits(cursor.read_u64()?))),
        4 => Ok(Value::String(cursor.read_string()?)),
        5 => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                values.push(decode_value_with_property_spills(
                    cursor,
                    depth.saturating_add(1),
                    property_spills,
                )?);
            }
            Ok(Value::List(values))
        }
        6 => Ok(Value::Map(decode_properties_with_property_spills(
            cursor,
            depth.saturating_add(1),
            property_spills,
        )?)),
        7 => {
            let spill_id = cursor.read_u64()?;
            let reader = property_spills.ok_or_else(|| {
                CanonicalSegmentError::Corrupt(format!(
                    "canonical value references property spill {spill_id} without a published spill artifact"
                ))
            })?;
            let encoded = reader.get(spill_id)?.ok_or_else(|| {
                CanonicalSegmentError::Corrupt(format!(
                    "canonical value references missing property spill {spill_id}"
                ))
            })?;
            let mut spilled = SliceCursor::new(&encoded);
            let value = decode_value(&mut spilled, depth)?;
            if !spilled.is_empty() {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "property spill {spill_id} has trailing bytes"
                )));
            }
            Ok(value)
        }
        tag => Err(CanonicalSegmentError::Corrupt(format!(
            "unknown canonical value tag {tag}"
        ))),
    }
}

fn encode_string(value: &str, output: &mut Vec<u8>) -> Result<(), CanonicalSegmentError> {
    output.extend_from_slice(&u32_len(value.len(), "string")?.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn ensure_depth(depth: usize) -> Result<(), CanonicalSegmentError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical value nesting exceeds {MAX_VALUE_DEPTH}"
        )));
    }
    Ok(())
}

fn u32_len(length: usize, name: &str) -> Result<u32, CanonicalSegmentError> {
    u32::try_from(length).map_err(|_| {
        CanonicalSegmentError::Corrupt(format!("{name} length exceeds the storage format"))
    })
}

fn parse_u64(value: &str, name: &str) -> Result<u64, CanonicalSegmentError> {
    value
        .parse()
        .map_err(|_| CanonicalSegmentError::Corrupt(format!("invalid canonical {name}: {value}")))
}

fn parse_u32(value: &str, name: &str) -> Result<u32, CanonicalSegmentError> {
    value
        .parse()
        .map_err(|_| CanonicalSegmentError::Corrupt(format!("invalid canonical {name}: {value}")))
}

fn parse_u8(value: &str, name: &str) -> Result<u8, CanonicalSegmentError> {
    value
        .parse()
        .map_err(|_| CanonicalSegmentError::Corrupt(format!("invalid canonical {name}: {value}")))
}

fn set_once<T>(target: &mut Option<T>, value: T, name: &str) -> Result<(), CanonicalSegmentError> {
    if target.replace(value).is_some() {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical manifest has duplicate {name}"
        )));
    }
    Ok(())
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, CanonicalSegmentError> {
    value.ok_or_else(|| {
        CanonicalSegmentError::Corrupt(format!("canonical manifest is missing {name}"))
    })
}

const fn segment_header_len() -> usize {
    8 + 1 + 8 + 8 + 4
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SliceCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], CanonicalSegmentError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical decode offset overflow".to_string())
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical record is truncated".to_string())
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, CanonicalSegmentError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CanonicalSegmentError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, CanonicalSegmentError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width u64"),
        ))
    }

    fn read_i64(&mut self) -> Result<i64, CanonicalSegmentError> {
        Ok(i64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width i64"),
        ))
    }

    fn read_string(&mut self) -> Result<String, CanonicalSegmentError> {
        let length = self.read_u32()? as usize;
        String::from_utf8(self.read_exact(length)?.to_vec()).map_err(|error| {
            CanonicalSegmentError::Corrupt(format!("canonical string is not UTF-8: {error}"))
        })
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
    file: &mut File,
    digest: &mut DigestState,
    bytes: &[u8],
) -> Result<(), CanonicalSegmentError> {
    file.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_segments_round_trip_with_bounded_cache() {
        let path = unique_path("round_trip");
        let nodes = (0..200)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId((id % 3) as u32)]),
                properties: BTreeMap::from([
                    ("id".to_string(), Value::Int(id as i64)),
                    ("body".to_string(), Value::String("x".repeat(128))),
                ]),
            })
            .collect::<Vec<_>>();
        let relationships = (0..100)
            .map(|id| RelRecord {
                id: RelId(id),
                source: NodeId(id),
                target: NodeId(id + 1),
                rel_type: RelTypeId(1),
                properties: BTreeMap::from([("weight".to_string(), Value::Int(id as i64))]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(4096).unwrap(),
            max_record_bytes: NonZeroU64::new(2048).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(&path, ManifestGeneration(7), &nodes, &relationships)
            .unwrap();
        assert_eq!(
            CanonicalSegmentManifest::decode(&manifest.encode().unwrap()).unwrap(),
            manifest
        );
        assert!(manifest.segments.len() > 2);
        let cache = Arc::new(SegmentCache::new(8192));
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::clone(&cache),
            StoreId(11),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        assert_eq!(
            reader.get_node(NodeId(117)).unwrap(),
            Some(nodes[117].clone())
        );
        assert_eq!(
            reader.get_relationship(RelId(44)).unwrap(),
            Some(relationships[44].clone())
        );
        let mut scanned = Vec::new();
        let report = reader
            .scan_nodes(|node| {
                scanned.push(node.id);
                Ok(())
            })
            .unwrap();
        assert_eq!(scanned.len(), nodes.len());
        assert!(report.peak_segment_bytes <= 4096);
        assert!(cache.snapshot().resident_bytes <= 8192);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn canonical_manifest_decodes_legacy_segment_summaries_without_false_negatives() {
        let path = unique_path("legacy_manifest");
        let nodes = vec![NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        }];
        let relationships = vec![RelRecord {
            id: RelId(2),
            source: NodeId(1),
            target: NodeId(3),
            rel_type: RelTypeId(1),
            properties: BTreeMap::new(),
        }];
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(&path, ManifestGeneration(8), &nodes, &relationships)
            .unwrap();
        let encoded = manifest.encode().unwrap();
        let body = encoded
            .lines()
            .filter(|line| !line.starts_with("checksum\t"))
            .map(|line| {
                if !line.starts_with("segment\t") {
                    return line.to_string();
                }
                line.split('\t').take(9).collect::<Vec<_>>().join("\t")
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let legacy = format!("{body}checksum\t{}\n", content_digest(body.as_bytes()).0);
        let decoded = CanonicalSegmentManifest::decode(&legacy).unwrap();
        for descriptor in decoded.node_segments() {
            assert_eq!(
                descriptor.node_property_bloom,
                CanonicalEndpointBloom::legacy_unbounded()
            );
        }
        for descriptor in decoded.relationship_segments() {
            assert_eq!(
                descriptor.source_endpoint_bloom,
                CanonicalEndpointBloom::legacy_unbounded()
            );
            assert_eq!(
                descriptor.target_endpoint_bloom,
                CanonicalEndpointBloom::legacy_unbounded()
            );
        }

        let reader = CanonicalSegmentReader::open(
            &path,
            decoded,
            Arc::new(SegmentCache::new(4096)),
            StoreId(14),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        let mut found = Vec::new();
        reader
            .scan_relationships_for_endpoint_control(
                NodeId(1),
                CanonicalEndpointDirection::Source,
                Some(RelTypeId(1)),
                |relationship| {
                    found.push(relationship.id);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(found, vec![RelId(2)]);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn endpoint_bloom_prunes_relationship_segments_without_false_negatives() {
        let path = unique_path("endpoint_bloom");
        let relationships = (0..256)
            .map(|id| RelRecord {
                id: RelId(id),
                source: NodeId(id),
                target: NodeId(10_000 + id),
                rel_type: RelTypeId(1),
                properties: BTreeMap::from([(
                    "payload".to_string(),
                    Value::String("x".repeat(96)),
                )]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(256).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(9),
                std::iter::empty::<NodeRecord>(),
                &relationships,
            )
            .unwrap();
        let relationship_segment_count = manifest.relationship_segments().count() as u64;
        assert!(relationship_segment_count > 8);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(4096)),
            StoreId(12),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        let mut found = Vec::new();
        let (report, control) = reader
            .scan_relationships_for_endpoint_control(
                NodeId(127),
                CanonicalEndpointDirection::Source,
                Some(RelTypeId(1)),
                |relationship| {
                    found.push(relationship.id);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(found, vec![RelId(127)]);
        assert!(report.segments_read < relationship_segment_count);
        assert_eq!(report.segments_considered, relationship_segment_count);
        assert_eq!(
            report.segments_pruned,
            report.segments_considered - report.segments_read
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn endpoint_bloom_scales_with_default_segment_cardinality() {
        let path = unique_path("adaptive_endpoint_bloom");
        let relationships = (0..30_000)
            .map(|id| RelRecord {
                id: RelId(id),
                source: NodeId(id),
                target: NodeId(100_000 + id),
                rel_type: RelTypeId(1),
                properties: BTreeMap::new(),
            })
            .collect::<Vec<_>>();
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(11),
                std::iter::empty::<NodeRecord>(),
                &relationships,
            )
            .unwrap();
        let descriptors = manifest.relationship_segments().collect::<Vec<_>>();
        assert!(!descriptors.is_empty());
        assert!(descriptors
            .iter()
            .all(|descriptor| descriptor.source_endpoint_bloom.bit_len() > 256));
        assert!(descriptors.iter().all(|descriptor| {
            descriptor
                .source_endpoint_bloom
                .estimated_false_positive_rate()
                < 0.02
        }));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn property_bloom_prunes_node_segments_without_false_negatives() {
        let path = unique_path("property_bloom");
        let nodes = (0..256)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([
                    ("id".to_string(), Value::Int(id as i64)),
                    ("payload".to_string(), Value::String("x".repeat(96))),
                ]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(256).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(10),
                &nodes,
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let node_segment_count = manifest.node_segments().count() as u64;
        assert!(node_segment_count > 8);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(4096)),
            StoreId(13),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        let mut found = Vec::new();
        let (report, control) = reader
            .scan_nodes_by_property_control(LabelId(1), "id", &Value::Int(127), |node| {
                found.push(node.id);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(found, vec![NodeId(127)]);
        assert!(report.segments_read < node_segment_count);
        assert_eq!(report.segments_considered, node_segment_count);
        assert_eq!(
            report.segments_pruned,
            report.segments_considered - report.segments_read
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn canonical_reader_fails_closed_on_segment_corruption() {
        let path = unique_path("corrupt");
        let nodes = vec![NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        }];
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(1),
                &nodes,
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let descriptor = &manifest.segments[0];
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[descriptor.offset as usize] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(1),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            reader.get_node(NodeId(1)),
            Err(CanonicalSegmentError::Read(
                SegmentReadError::DigestMismatch { .. }
            ))
        ));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    #[ignore = "resource profile; run explicitly for larger-than-memory-budget evidence"]
    fn canonical_scan_keeps_resident_cache_below_a_smaller_memory_budget() {
        let path = unique_path("larger_than_memory_budget");
        let node_count = 16_384u64;
        let payload_bytes = 4096usize;
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(9),
                (0..node_count).map(|id| NodeRecord {
                    id: NodeId(id),
                    labels: BTreeSet::from([LabelId(1)]),
                    properties: BTreeMap::from([(
                        "body".to_string(),
                        Value::String("x".repeat(payload_bytes)),
                    )]),
                }),
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let cache_budget = 8 * 1024 * 1024;
        assert!(manifest.artifact_len > cache_budget * 4);
        let cache = Arc::new(SegmentCache::new(cache_budget));
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::clone(&cache),
            StoreId(19),
            NonZeroU64::new(16 * 1024 * 1024 + 64).unwrap(),
        )
        .unwrap();
        let mut decoded = 0u64;
        let report = reader
            .scan_nodes(|_| {
                decoded = decoded.saturating_add(1);
                Ok(())
            })
            .unwrap();
        assert_eq!(decoded, node_count);
        assert!(report.peak_segment_bytes <= 4 * 1024 * 1024);
        let cache_snapshot = cache.snapshot();
        assert!(cache_snapshot.resident_bytes <= cache_budget);
        assert!(cache_snapshot.eviction_count > 0);
        std::fs::remove_file(path).unwrap();
    }

    fn unique_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-canonical-{name}-{nonce}.skein"))
    }
}
