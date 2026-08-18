use crate::cache::SegmentCacheIdentity;
use crate::{
    content_digest, decode_residual_row_properties, durable_replace_file,
    encode_residual_row_properties, ManifestGeneration, RepresentationKind, SegmentCache,
    SegmentCacheError, SegmentCacheKey, StoreId, StoreStableIdMapping,
};
use skein_core::Value;
use skein_integrity::{IntegrityHasher, SHA256_BYTES};
use std::cmp::Ordering;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

const FILE_MAGIC: &[u8; 8] = b"SKSIDMP1";
const PAGE_MAGIC: &[u8; 8] = b"SKSIDPG1";
const FORMAT_VERSION: u16 = 1;
const FILE_HEADER_BYTES: usize = 96;
const PAGE_HEADER_BYTES: usize = 96;
const ENCODED_KEY_BYTES: usize = 9;
const ENTRY_HEADER_BYTES: usize = ENCODED_KEY_BYTES + 4;

pub const DEFAULT_STABLE_IDENTITY_PAGE_BYTES: usize = 64 * 1024;
pub const DEFAULT_STABLE_IDENTITY_PAGE_ENTRIES: usize = 4096;
pub const DEFAULT_STABLE_IDENTITY_VALUE_BYTES: usize = 32 * 1024;
pub const DEFAULT_STABLE_IDENTITY_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const DEFAULT_STABLE_IDENTITY_LOOKUP_PAGES: usize = 64;
pub const DEFAULT_STABLE_IDENTITY_LOOKUP_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_STABLE_IDENTITY_MATERIALIZED_ENTRIES: usize = 1_000_000;
pub const DEFAULT_STABLE_IDENTITY_MATERIALIZED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StableIdentityKind {
    Node,
    Relationship,
}

impl StableIdentityKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Node => 1,
            Self::Relationship => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, StableIdentityMappingError> {
        match tag {
            1 => Ok(Self::Node),
            2 => Ok(Self::Relationship),
            _ => Err(StableIdentityMappingError::Corrupt(format!(
                "unknown stable identity kind {tag}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StableIdentityKey {
    pub kind: StableIdentityKind,
    pub physical_id: u64,
}

impl StableIdentityKey {
    pub const fn node(physical_id: u64) -> Self {
        Self {
            kind: StableIdentityKind::Node,
            physical_id,
        }
    }

    pub const fn relationship(physical_id: u64) -> Self {
        Self {
            kind: StableIdentityKind::Relationship,
            physical_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableIdentityMappingConfig {
    pub page_bytes: NonZeroUsize,
    pub max_entries_per_page: NonZeroUsize,
    pub max_value_bytes: NonZeroUsize,
    pub max_artifact_bytes: NonZeroU64,
}

impl Default for StableIdentityMappingConfig {
    fn default() -> Self {
        Self {
            page_bytes: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_PAGE_BYTES)
                .expect("default stable identity page size is non-zero"),
            max_entries_per_page: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_PAGE_ENTRIES)
                .expect("default stable identity page entry limit is non-zero"),
            max_value_bytes: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_VALUE_BYTES)
                .expect("default stable identity value limit is non-zero"),
            max_artifact_bytes: NonZeroU64::new(DEFAULT_STABLE_IDENTITY_ARTIFACT_BYTES)
                .expect("default stable identity artifact limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableIdentityReadLimits {
    pub max_pages: NonZeroUsize,
    pub max_storage_bytes: NonZeroUsize,
}

impl Default for StableIdentityReadLimits {
    fn default() -> Self {
        Self {
            max_pages: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_LOOKUP_PAGES)
                .expect("default stable identity lookup page limit is non-zero"),
            max_storage_bytes: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_LOOKUP_BYTES)
                .expect("default stable identity lookup byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableIdentityMaterializeLimits {
    pub max_pages: NonZeroUsize,
    pub max_storage_bytes: NonZeroUsize,
    pub max_entries: NonZeroUsize,
    pub max_resident_bytes: NonZeroUsize,
}

impl Default for StableIdentityMaterializeLimits {
    fn default() -> Self {
        Self {
            max_pages: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_MATERIALIZED_ENTRIES)
                .expect("default stable identity materialization page limit is non-zero"),
            max_storage_bytes: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_MATERIALIZED_BYTES)
                .expect("default stable identity materialization read limit is non-zero"),
            max_entries: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_MATERIALIZED_ENTRIES)
                .expect("default stable identity materialization entry limit is non-zero"),
            max_resident_bytes: NonZeroUsize::new(DEFAULT_STABLE_IDENTITY_MATERIALIZED_BYTES)
                .expect("default stable identity materialization resident limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StableIdentityReadReport {
    pub visited_pages: usize,
    pub storage_bytes_read: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub decoded_entries: usize,
    pub estimated_resident_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StableIdentityScrubReport {
    pub checked_pages: u64,
    pub checked_entries: u64,
    pub checked_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableIdentityMappingHeader {
    pub generation: u64,
    pub covered_commit_epoch: u64,
    pub page_bytes: u64,
    pub page_count: u64,
    pub node_count: u64,
    pub relationship_count: u64,
}

impl StableIdentityMappingHeader {
    pub const fn entry_count(self) -> u64 {
        self.node_count.saturating_add(self.relationship_count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableIdentityMappingWriteOutput {
    pub header: StableIdentityMappingHeader,
    pub encoded_len: u64,
}

#[derive(Debug)]
pub enum StableIdentityMappingError {
    Admission(String),
    Corrupt(String),
    Durability(String),
}

impl fmt::Display for StableIdentityMappingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "stable identity mapping admission failed: {message}"
                )
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt stable identity mapping: {message}")
            }
            Self::Durability(message) => {
                write!(
                    formatter,
                    "stable identity mapping durability failed: {message}"
                )
            }
        }
    }
}

impl std::error::Error for StableIdentityMappingError {}

pub struct StableIdentityMappingWriter;

impl StableIdentityMappingWriter {
    pub fn publish<'a, I>(
        path: &Path,
        covered_commit_epoch: u64,
        entries: I,
        config: StableIdentityMappingConfig,
    ) -> Result<StableIdentityMappingWriteOutput, StableIdentityMappingError>
    where
        I: IntoIterator<Item = (StableIdentityKey, &'a Value)>,
    {
        validate_config(config)?;
        let generation = if path.exists() {
            read_header(path, config)?
                .generation
                .checked_add(1)
                .ok_or_else(|| {
                    StableIdentityMappingError::Admission(
                        "stable identity generation overflow".to_string(),
                    )
                })?
        } else {
            1
        };
        let temporary = path.with_extension("skein.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temporary)
            .map_err(durability("create stable identity candidate"))?;
        file.write_all(&[0; FILE_HEADER_BYTES])
            .map_err(durability("reserve stable identity header"))?;
        let summary = {
            let mut pages = StableIdentityPageWriter::new(&mut file, generation, config);
            for (key, value) in entries {
                pages.push(key, value)?;
            }
            pages.finish()?
        };
        let header = StableIdentityMappingHeader {
            generation,
            covered_commit_epoch,
            page_bytes: config.page_bytes.get() as u64,
            page_count: summary.page_count,
            node_count: summary.node_count,
            relationship_count: summary.relationship_count,
        };
        file.seek(SeekFrom::Start(0))
            .map_err(durability("seek stable identity header"))?;
        file.write_all(&encode_header(header))
            .map_err(durability("write stable identity header"))?;
        file.sync_all()
            .map_err(durability("sync stable identity candidate"))?;
        let encoded_len = expected_file_len(header)?;
        let actual_len = file
            .metadata()
            .map_err(durability("inspect stable identity candidate"))?
            .len();
        if actual_len != encoded_len {
            return Err(StableIdentityMappingError::Corrupt(format!(
                "candidate length mismatch: expected {encoded_len}, got {actual_len}"
            )));
        }
        drop(file);
        durable_replace_file(&temporary, path).map_err(|error| {
            StableIdentityMappingError::Durability(format!(
                "publish stable identity mapping: {error}"
            ))
        })?;
        Ok(StableIdentityMappingWriteOutput {
            header,
            encoded_len,
        })
    }
}

pub struct StableIdentityMappingReader {
    path: PathBuf,
    header: StableIdentityMappingHeader,
    config: StableIdentityMappingConfig,
    page_cache: Option<Arc<SegmentCache>>,
    store_id: StoreId,
    file: OnceLock<File>,
    poisoned: AtomicBool,
}

impl fmt::Debug for StableIdentityMappingReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StableIdentityMappingReader")
            .field("path", &self.path)
            .field("header", &self.header)
            .field("config", &self.config)
            .field("poisoned", &self.is_poisoned())
            .finish_non_exhaustive()
    }
}

impl StableIdentityMappingReader {
    pub fn open(
        path: &Path,
        config: StableIdentityMappingConfig,
    ) -> Result<Self, StableIdentityMappingError> {
        Self::open_inner(path, config, None, StoreId::default())
    }

    pub fn open_with_cache(
        path: &Path,
        config: StableIdentityMappingConfig,
        page_cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, StableIdentityMappingError> {
        Self::open_inner(path, config, Some(page_cache), store_id)
    }

    fn open_inner(
        path: &Path,
        config: StableIdentityMappingConfig,
        page_cache: Option<Arc<SegmentCache>>,
        store_id: StoreId,
    ) -> Result<Self, StableIdentityMappingError> {
        validate_config(config)?;
        let (opened, header) = open_and_read_header(path, config)?;
        let file = OnceLock::new();
        let _ = file.set(opened);
        Ok(Self {
            path: path.to_path_buf(),
            header,
            config,
            page_cache,
            store_id,
            file,
            poisoned: AtomicBool::new(false),
        })
    }

    pub const fn header(&self) -> StableIdentityMappingHeader {
        self.header
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(AtomicOrdering::Acquire)
    }

    pub fn lookup(
        &self,
        key: StableIdentityKey,
        limits: StableIdentityReadLimits,
    ) -> Result<(Option<Value>, StableIdentityReadReport), StableIdentityMappingError> {
        self.ensure_healthy()?;
        let mut report = StableIdentityReadReport::default();
        let result = self.lookup_inner(key, limits, &mut report);
        self.poison_on_physical_failure(&result);
        result.map(|value| (value, report))
    }

    pub fn materialize(
        &self,
        limits: StableIdentityMaterializeLimits,
    ) -> Result<(StoreStableIdMapping, StableIdentityReadReport), StableIdentityMappingError> {
        self.ensure_healthy()?;
        let result = self.materialize_inner(limits);
        self.poison_on_physical_failure(&result);
        result
    }

    /// Validates every page without retaining decoded identities or warming the
    /// shared page cache. The configured artifact limit bounds total work, and
    /// only one fixed-size page plus one decoded value is resident at a time.
    pub fn deep_scrub(&self) -> Result<StableIdentityScrubReport, StableIdentityMappingError> {
        self.ensure_healthy()?;
        let result = self.deep_scrub_inner();
        self.poison_on_physical_failure(&result);
        result
    }

    fn lookup_inner(
        &self,
        key: StableIdentityKey,
        limits: StableIdentityReadLimits,
        report: &mut StableIdentityReadReport,
    ) -> Result<Option<Value>, StableIdentityMappingError> {
        if self.header.page_count == 0 {
            return Ok(None);
        }
        let mut lower = 1u64;
        let mut upper = self.header.page_count.saturating_add(1);
        while lower < upper {
            let page_id = lower + (upper - lower) / 2;
            let decision = self.with_page(page_id, limits, report, |page| {
                if key < page.first_key {
                    Ok(PageLookup::Before)
                } else if key > page.last_key {
                    Ok(PageLookup::After)
                } else {
                    page.lookup(key).map(PageLookup::Found)
                }
            })?;
            match decision {
                PageLookup::Before => upper = page_id,
                PageLookup::After => lower = page_id.saturating_add(1),
                PageLookup::Found(value) => return Ok(value),
            }
        }
        Ok(None)
    }

    fn materialize_inner(
        &self,
        limits: StableIdentityMaterializeLimits,
    ) -> Result<(StoreStableIdMapping, StableIdentityReadReport), StableIdentityMappingError> {
        let read_limits = StableIdentityReadLimits {
            max_pages: limits.max_pages,
            max_storage_bytes: limits.max_storage_bytes,
        };
        let mut report = StableIdentityReadReport::default();
        let mut mapping = StoreStableIdMapping::default();
        let mut decoded_entries = 0usize;
        let mut estimated_resident_bytes = 0usize;
        for page_id in 1..=self.header.page_count {
            self.with_page(page_id, read_limits, &mut report, |page| {
                page.visit_entries(|key, encoded_value| {
                    let next_entries = decoded_entries.checked_add(1).ok_or_else(|| {
                        StableIdentityMappingError::Admission(
                            "materialized entry count overflow".to_string(),
                        )
                    })?;
                    if next_entries > limits.max_entries.get() {
                        return Err(StableIdentityMappingError::Admission(format!(
                            "materialization needs {next_entries} entries, exceeding limit {}",
                            limits.max_entries
                        )));
                    }
                    let value = decode_stable_value(encoded_value)?;
                    let value_bytes = estimated_value_resident_bytes(&value);
                    let next_resident = estimated_resident_bytes
                        .checked_add(std::mem::size_of::<StableIdentityKey>())
                        .and_then(|bytes| bytes.checked_add(value_bytes))
                        .ok_or_else(|| {
                            StableIdentityMappingError::Admission(
                                "materialized stable identity bytes overflow".to_string(),
                            )
                        })?;
                    if next_resident > limits.max_resident_bytes.get() {
                        return Err(StableIdentityMappingError::Admission(format!(
                            "materialization needs {next_resident} resident bytes, exceeding limit {}",
                            limits.max_resident_bytes
                        )));
                    }
                    let prior = match key.kind {
                        StableIdentityKind::Node => mapping
                            .node_stable_ids
                            .insert(crate::NodeId(key.physical_id), value),
                        StableIdentityKind::Relationship => mapping
                            .relationship_stable_ids
                            .insert(crate::RelId(key.physical_id), value),
                    };
                    if prior.is_some() {
                        return Err(StableIdentityMappingError::Corrupt(format!(
                            "stable identity key {:?}/{} appears more than once",
                            key.kind, key.physical_id
                        )));
                    }
                    decoded_entries = next_entries;
                    estimated_resident_bytes = next_resident;
                    Ok(())
                })
            })?;
        }
        report.decoded_entries = decoded_entries;
        report.estimated_resident_bytes = estimated_resident_bytes;
        if mapping.node_stable_ids.len() as u64 != self.header.node_count
            || mapping.relationship_stable_ids.len() as u64 != self.header.relationship_count
        {
            return Err(StableIdentityMappingError::Corrupt(format!(
                "materialized counts {}/{} do not match header {}/{}",
                mapping.node_stable_ids.len(),
                mapping.relationship_stable_ids.len(),
                self.header.node_count,
                self.header.relationship_count
            )));
        }
        Ok((mapping, report))
    }

    fn deep_scrub_inner(&self) -> Result<StableIdentityScrubReport, StableIdentityMappingError> {
        let mut report = StableIdentityScrubReport {
            checked_bytes: FILE_HEADER_BYTES as u64,
            ..StableIdentityScrubReport::default()
        };
        let mut previous_key = None;
        let mut node_count = 0u64;
        let mut relationship_count = 0u64;
        for page_id in 1..=self.header.page_count {
            let offset = page_offset(page_id, self.header)?;
            let mut bytes = vec![0; self.config.page_bytes.get()];
            read_exact_at(self.file()?, &mut bytes, offset)
                .map_err(durability("scrub stable identity page"))?;
            let page = decode_page_slot(&bytes, self.header.generation, page_id, self.config)?;
            if previous_key.is_some_and(|previous| previous >= page.first_key) {
                return Err(StableIdentityMappingError::Corrupt(format!(
                    "stable identity page {page_id} overlaps or precedes its prior page"
                )));
            }
            page.visit_entries(|key, encoded_value| {
                if previous_key.is_some_and(|previous| previous >= key) {
                    return Err(StableIdentityMappingError::Corrupt(format!(
                        "stable identity key {:?}/{} is not globally ordered",
                        key.kind, key.physical_id
                    )));
                }
                let _ = decode_stable_value(encoded_value)?;
                match key.kind {
                    StableIdentityKind::Node => {
                        node_count = node_count.checked_add(1).ok_or_else(|| {
                            StableIdentityMappingError::Corrupt(
                                "stable identity node count overflow".to_string(),
                            )
                        })?;
                    }
                    StableIdentityKind::Relationship => {
                        relationship_count =
                            relationship_count.checked_add(1).ok_or_else(|| {
                                StableIdentityMappingError::Corrupt(
                                    "stable identity relationship count overflow".to_string(),
                                )
                            })?;
                    }
                }
                previous_key = Some(key);
                report.checked_entries =
                    report.checked_entries.checked_add(1).ok_or_else(|| {
                        StableIdentityMappingError::Corrupt(
                            "stable identity scrub entry count overflow".to_string(),
                        )
                    })?;
                Ok(())
            })?;
            report.checked_pages = report.checked_pages.checked_add(1).ok_or_else(|| {
                StableIdentityMappingError::Corrupt(
                    "stable identity scrub page count overflow".to_string(),
                )
            })?;
            report.checked_bytes = report
                .checked_bytes
                .checked_add(self.header.page_bytes)
                .ok_or_else(|| {
                    StableIdentityMappingError::Corrupt(
                        "stable identity scrub byte count overflow".to_string(),
                    )
                })?;
        }
        if report.checked_pages != self.header.page_count
            || node_count != self.header.node_count
            || relationship_count != self.header.relationship_count
            || report.checked_bytes != expected_file_len(self.header)?
        {
            return Err(StableIdentityMappingError::Corrupt(format!(
                "scrubbed pages/counts/bytes {}/{}/{}/{} do not match header {}/{}/{}/{}",
                report.checked_pages,
                node_count,
                relationship_count,
                report.checked_bytes,
                self.header.page_count,
                self.header.node_count,
                self.header.relationship_count,
                expected_file_len(self.header)?
            )));
        }
        Ok(report)
    }

    fn with_page<T>(
        &self,
        page_id: u64,
        limits: StableIdentityReadLimits,
        report: &mut StableIdentityReadReport,
        consumer: impl FnOnce(StableIdentityPageView<'_>) -> Result<T, StableIdentityMappingError>,
    ) -> Result<T, StableIdentityMappingError> {
        let next_pages = report.visited_pages.checked_add(1).ok_or_else(|| {
            StableIdentityMappingError::Admission("stable identity page count overflow".to_string())
        })?;
        if next_pages > limits.max_pages.get() {
            return Err(StableIdentityMappingError::Admission(format!(
                "lookup needs {next_pages} pages, exceeding limit {}",
                limits.max_pages
            )));
        }
        report.visited_pages = next_pages;
        let identity = SegmentCacheIdentity {
            store_id: self.store_id,
            manifest_generation: ManifestGeneration(self.header.generation),
            segment_id: page_id,
            representation: RepresentationKind::StableIdentityPageSlot,
        };
        if let Some(cache) = &self.page_cache
            && let Some(slot) = cache.get_by_identity(&identity)
        {
            report.cache_hits = report.cache_hits.saturating_add(1);
            return consumer(decode_page_slot(
                &slot,
                self.header.generation,
                page_id,
                self.config,
            )?);
        }
        if self.page_cache.is_some() {
            report.cache_misses = report.cache_misses.saturating_add(1);
        }
        let page_bytes = self.config.page_bytes.get();
        let next_storage_bytes = report
            .storage_bytes_read
            .checked_add(page_bytes)
            .ok_or_else(|| {
                StableIdentityMappingError::Admission(
                    "stable identity storage-byte accounting overflow".to_string(),
                )
            })?;
        if next_storage_bytes > limits.max_storage_bytes.get() {
            return Err(StableIdentityMappingError::Admission(format!(
                "lookup needs {next_storage_bytes} storage bytes, exceeding limit {}",
                limits.max_storage_bytes
            )));
        }
        report.storage_bytes_read = next_storage_bytes;
        let offset = page_offset(page_id, self.header)?;
        let mut bytes = vec![0; page_bytes];
        read_exact_at(self.file()?, &mut bytes, offset)
            .map_err(durability("read stable identity page"))?;
        let bytes: Arc<[u8]> = bytes.into();
        let value = consumer(decode_page_slot(
            &bytes,
            self.header.generation,
            page_id,
            self.config,
        )?)?;
        if let Some(cache) = &self.page_cache {
            let key = SegmentCacheKey {
                store_id: identity.store_id,
                manifest_generation: identity.manifest_generation,
                segment_id: identity.segment_id,
                content_digest: content_digest(&bytes),
                representation: identity.representation,
            };
            match cache.insert(key, Arc::clone(&bytes)) {
                Ok(_) => {}
                Err(SegmentCacheError::EntryTooLarge { .. })
                | Err(SegmentCacheError::PinnedCapacity { .. }) => {
                    report.cache_admission_rejections =
                        report.cache_admission_rejections.saturating_add(1);
                }
                Err(error) => {
                    return Err(StableIdentityMappingError::Corrupt(format!(
                        "stable identity cache rejected immutable page identity: {error}"
                    )));
                }
            }
        }
        Ok(value)
    }

    fn file(&self) -> Result<&File, StableIdentityMappingError> {
        if let Some(file) = self.file.get() {
            return Ok(file);
        }
        let opened = File::open(&self.path).map_err(durability("open stable identity mapping"))?;
        let _ = self.file.set(opened);
        self.file.get().ok_or_else(|| {
            StableIdentityMappingError::Durability(
                "stable identity file handle was not initialized".to_string(),
            )
        })
    }

    fn ensure_healthy(&self) -> Result<(), StableIdentityMappingError> {
        if self.is_poisoned() {
            return Err(StableIdentityMappingError::Corrupt(
                "stable identity reader is poisoned by an earlier physical failure".to_string(),
            ));
        }
        Ok(())
    }

    fn poison_on_physical_failure<T>(&self, result: &Result<T, StableIdentityMappingError>) {
        if matches!(
            result,
            Err(StableIdentityMappingError::Corrupt(_))
                | Err(StableIdentityMappingError::Durability(_))
        ) {
            self.poisoned.store(true, AtomicOrdering::Release);
        }
    }
}

enum PageLookup {
    Before,
    After,
    Found(Option<Value>),
}

struct StableIdentityPageWriter<'a> {
    file: &'a mut File,
    generation: u64,
    config: StableIdentityMappingConfig,
    payload: Vec<u8>,
    entry_count: usize,
    first_key: Option<StableIdentityKey>,
    last_key: Option<StableIdentityKey>,
    previous_key: Option<StableIdentityKey>,
    page_count: u64,
    node_count: u64,
    relationship_count: u64,
}

struct StableIdentityPageSummary {
    page_count: u64,
    node_count: u64,
    relationship_count: u64,
}

impl<'a> StableIdentityPageWriter<'a> {
    fn new(file: &'a mut File, generation: u64, config: StableIdentityMappingConfig) -> Self {
        Self {
            file,
            generation,
            config,
            payload: Vec::with_capacity(config.page_bytes.get().saturating_sub(PAGE_HEADER_BYTES)),
            entry_count: 0,
            first_key: None,
            last_key: None,
            previous_key: None,
            page_count: 0,
            node_count: 0,
            relationship_count: 0,
        }
    }

    fn push(
        &mut self,
        key: StableIdentityKey,
        value: &Value,
    ) -> Result<(), StableIdentityMappingError> {
        if self.previous_key.is_some_and(|previous| previous >= key) {
            return Err(StableIdentityMappingError::Admission(format!(
                "stable identity entries are not strictly ordered at {:?}/{}",
                key.kind, key.physical_id
            )));
        }
        let encoded_value = encode_stable_value(value)?;
        if encoded_value.len() > self.config.max_value_bytes.get() {
            return Err(StableIdentityMappingError::Admission(format!(
                "stable identity value contains {} bytes, exceeding limit {}",
                encoded_value.len(),
                self.config.max_value_bytes
            )));
        }
        let entry_bytes = ENTRY_HEADER_BYTES
            .checked_add(encoded_value.len())
            .ok_or_else(|| {
                StableIdentityMappingError::Admission(
                    "stable identity entry size overflow".to_string(),
                )
            })?;
        let max_payload = self
            .config
            .page_bytes
            .get()
            .checked_sub(PAGE_HEADER_BYTES)
            .ok_or_else(|| {
                StableIdentityMappingError::Admission(
                    "stable identity page is smaller than its header".to_string(),
                )
            })?;
        if entry_bytes > max_payload {
            return Err(StableIdentityMappingError::Admission(format!(
                "one stable identity entry needs {entry_bytes} bytes, exceeding page payload {max_payload}"
            )));
        }
        if self.entry_count > 0
            && (self.entry_count >= self.config.max_entries_per_page.get()
                || self.payload.len().saturating_add(entry_bytes) > max_payload)
        {
            self.flush()?;
        }
        self.payload.extend_from_slice(&encode_key(key));
        let value_len = u32::try_from(encoded_value.len()).map_err(|_| {
            StableIdentityMappingError::Admission(
                "stable identity value length does not fit u32".to_string(),
            )
        })?;
        self.payload.extend_from_slice(&value_len.to_le_bytes());
        self.payload.extend_from_slice(&encoded_value);
        self.first_key.get_or_insert(key);
        self.last_key = Some(key);
        self.previous_key = Some(key);
        self.entry_count += 1;
        match key.kind {
            StableIdentityKind::Node => {
                self.node_count = self.node_count.checked_add(1).ok_or_else(|| {
                    StableIdentityMappingError::Admission(
                        "stable identity node count overflow".to_string(),
                    )
                })?;
            }
            StableIdentityKind::Relationship => {
                self.relationship_count =
                    self.relationship_count.checked_add(1).ok_or_else(|| {
                        StableIdentityMappingError::Admission(
                            "stable identity relationship count overflow".to_string(),
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<StableIdentityPageSummary, StableIdentityMappingError> {
        if self.entry_count > 0 {
            self.flush()?;
        }
        Ok(StableIdentityPageSummary {
            page_count: self.page_count,
            node_count: self.node_count,
            relationship_count: self.relationship_count,
        })
    }

    fn flush(&mut self) -> Result<(), StableIdentityMappingError> {
        if self.entry_count == 0 {
            return Ok(());
        }
        let page_id = self.page_count.checked_add(1).ok_or_else(|| {
            StableIdentityMappingError::Admission("stable identity page count overflow".to_string())
        })?;
        let next_file_bytes = page_id
            .checked_mul(self.config.page_bytes.get() as u64)
            .and_then(|bytes| bytes.checked_add(FILE_HEADER_BYTES as u64))
            .ok_or_else(|| {
                StableIdentityMappingError::Admission(
                    "stable identity artifact size overflow".to_string(),
                )
            })?;
        if next_file_bytes > self.config.max_artifact_bytes.get() {
            return Err(StableIdentityMappingError::Admission(format!(
                "stable identity artifact needs {next_file_bytes} bytes, exceeding limit {}",
                self.config.max_artifact_bytes
            )));
        }
        let first_key = self
            .first_key
            .take()
            .expect("non-empty page has a first key");
        let last_key = self.last_key.expect("non-empty page has a last key");
        let slot = encode_page_slot(
            self.generation,
            page_id,
            self.entry_count,
            first_key,
            last_key,
            &self.payload,
            self.config,
        )?;
        self.file
            .write_all(&slot)
            .map_err(durability("write stable identity page"))?;
        self.page_count = page_id;
        self.payload.clear();
        self.entry_count = 0;
        self.last_key = None;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct StableIdentityPageView<'a> {
    first_key: StableIdentityKey,
    last_key: StableIdentityKey,
    entry_count: usize,
    payload: &'a [u8],
}

impl StableIdentityPageView<'_> {
    fn lookup(
        &self,
        target: StableIdentityKey,
    ) -> Result<Option<Value>, StableIdentityMappingError> {
        let mut found = None;
        self.visit_encoded_entries(|key, value| {
            match key.cmp(&target) {
                Ordering::Less => {}
                Ordering::Equal => found = Some(decode_stable_value(value)?),
                Ordering::Greater => return Ok(false),
            }
            Ok(found.is_none())
        })?;
        Ok(found)
    }

    fn visit_entries(
        &self,
        mut consumer: impl FnMut(StableIdentityKey, &[u8]) -> Result<(), StableIdentityMappingError>,
    ) -> Result<(), StableIdentityMappingError> {
        self.visit_encoded_entries(|key, value| {
            consumer(key, value)?;
            Ok(true)
        })
    }

    fn visit_encoded_entries(
        &self,
        mut consumer: impl FnMut(StableIdentityKey, &[u8]) -> Result<bool, StableIdentityMappingError>,
    ) -> Result<(), StableIdentityMappingError> {
        let mut offset = 0usize;
        let mut decoded = 0usize;
        while offset < self.payload.len() {
            let key_end = offset.checked_add(ENCODED_KEY_BYTES).ok_or_else(|| {
                StableIdentityMappingError::Corrupt(
                    "stable identity key offset overflow".to_string(),
                )
            })?;
            let key = decode_key(self.payload.get(offset..key_end).ok_or_else(|| {
                StableIdentityMappingError::Corrupt("truncated stable identity key".to_string())
            })?)?;
            let length_end = key_end.checked_add(4).ok_or_else(|| {
                StableIdentityMappingError::Corrupt(
                    "stable identity value-length offset overflow".to_string(),
                )
            })?;
            let value_len = read_u32(self.payload.get(key_end..length_end).ok_or_else(|| {
                StableIdentityMappingError::Corrupt(
                    "truncated stable identity value length".to_string(),
                )
            })?) as usize;
            let value_end = length_end.checked_add(value_len).ok_or_else(|| {
                StableIdentityMappingError::Corrupt(
                    "stable identity value offset overflow".to_string(),
                )
            })?;
            let value = self.payload.get(length_end..value_end).ok_or_else(|| {
                StableIdentityMappingError::Corrupt("truncated stable identity value".to_string())
            })?;
            decoded += 1;
            offset = value_end;
            if !consumer(key, value)? {
                return Ok(());
            }
        }
        if decoded != self.entry_count {
            return Err(StableIdentityMappingError::Corrupt(format!(
                "page declares {} entries but contains {decoded}",
                self.entry_count
            )));
        }
        Ok(())
    }
}

fn validate_config(config: StableIdentityMappingConfig) -> Result<(), StableIdentityMappingError> {
    if config.max_artifact_bytes.get() < FILE_HEADER_BYTES as u64 {
        return Err(StableIdentityMappingError::Admission(format!(
            "artifact limit {} is smaller than header {FILE_HEADER_BYTES}",
            config.max_artifact_bytes
        )));
    }
    if config.page_bytes.get() <= PAGE_HEADER_BYTES {
        return Err(StableIdentityMappingError::Admission(format!(
            "page size {} is not larger than header {PAGE_HEADER_BYTES}",
            config.page_bytes
        )));
    }
    if config.max_value_bytes.get() > config.page_bytes.get() - PAGE_HEADER_BYTES {
        return Err(StableIdentityMappingError::Admission(format!(
            "value limit {} exceeds page payload {}",
            config.max_value_bytes,
            config.page_bytes.get() - PAGE_HEADER_BYTES
        )));
    }
    Ok(())
}

fn encode_header(header: StableIdentityMappingHeader) -> [u8; FILE_HEADER_BYTES] {
    let mut encoded = [0u8; FILE_HEADER_BYTES];
    encoded[..8].copy_from_slice(FILE_MAGIC);
    encoded[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    encoded[12..20].copy_from_slice(&header.generation.to_le_bytes());
    encoded[20..28].copy_from_slice(&header.covered_commit_epoch.to_le_bytes());
    encoded[28..36].copy_from_slice(&header.page_bytes.to_le_bytes());
    encoded[36..44].copy_from_slice(&header.page_count.to_le_bytes());
    encoded[44..52].copy_from_slice(&header.node_count.to_le_bytes());
    encoded[52..60].copy_from_slice(&header.relationship_count.to_le_bytes());
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..60]);
    let digest = hasher.finish();
    encoded[60..64].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[64..96].copy_from_slice(digest.sha256.as_bytes());
    encoded
}

fn decode_header(
    encoded: &[u8],
    config: StableIdentityMappingConfig,
) -> Result<StableIdentityMappingHeader, StableIdentityMappingError> {
    if encoded.len() != FILE_HEADER_BYTES || &encoded[..8] != FILE_MAGIC {
        return Err(StableIdentityMappingError::Corrupt(
            "invalid stable identity file header".to_string(),
        ));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    if version != FORMAT_VERSION || flags != 0 {
        return Err(StableIdentityMappingError::Corrupt(format!(
            "unsupported stable identity version {version} or flags {flags}"
        )));
    }
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..60]);
    let digest = hasher.finish();
    if digest.crc32c.get() != read_u32(&encoded[60..64])
        || digest.sha256.as_bytes() != &encoded[64..64 + SHA256_BYTES]
    {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity header checksum mismatch".to_string(),
        ));
    }
    let header = StableIdentityMappingHeader {
        generation: read_u64(&encoded[12..20]),
        covered_commit_epoch: read_u64(&encoded[20..28]),
        page_bytes: read_u64(&encoded[28..36]),
        page_count: read_u64(&encoded[36..44]),
        node_count: read_u64(&encoded[44..52]),
        relationship_count: read_u64(&encoded[52..60]),
    };
    if header.generation == 0 {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity generation must be non-zero".to_string(),
        ));
    }
    if header.page_bytes != config.page_bytes.get() as u64 {
        return Err(StableIdentityMappingError::Corrupt(format!(
            "stable identity page size {} does not match configured {}",
            header.page_bytes, config.page_bytes
        )));
    }
    if header.entry_count() == 0 && header.page_count != 0 {
        return Err(StableIdentityMappingError::Corrupt(
            "empty stable identity mapping declares pages".to_string(),
        ));
    }
    if header.entry_count() > 0 && header.page_count == 0 {
        return Err(StableIdentityMappingError::Corrupt(
            "non-empty stable identity mapping declares no pages".to_string(),
        ));
    }
    Ok(header)
}

fn read_header(
    path: &Path,
    config: StableIdentityMappingConfig,
) -> Result<StableIdentityMappingHeader, StableIdentityMappingError> {
    open_and_read_header(path, config).map(|(_, header)| header)
}

fn open_and_read_header(
    path: &Path,
    config: StableIdentityMappingConfig,
) -> Result<(File, StableIdentityMappingHeader), StableIdentityMappingError> {
    let mut file = File::open(path).map_err(durability("open stable identity header"))?;
    let mut encoded = [0u8; FILE_HEADER_BYTES];
    file.read_exact(&mut encoded)
        .map_err(durability("read stable identity header"))?;
    let header = decode_header(&encoded, config)?;
    let actual_len = file
        .metadata()
        .map_err(durability("inspect stable identity mapping"))?
        .len();
    let expected_len = expected_file_len(header)?;
    if expected_len > config.max_artifact_bytes.get() {
        return Err(StableIdentityMappingError::Admission(format!(
            "stable identity artifact contains {expected_len} bytes, exceeding limit {}",
            config.max_artifact_bytes
        )));
    }
    if actual_len != expected_len {
        return Err(StableIdentityMappingError::Corrupt(format!(
            "stable identity file length mismatch: expected {expected_len}, got {actual_len}"
        )));
    }
    Ok((file, header))
}

fn expected_file_len(
    header: StableIdentityMappingHeader,
) -> Result<u64, StableIdentityMappingError> {
    header
        .page_count
        .checked_mul(header.page_bytes)
        .and_then(|bytes| bytes.checked_add(FILE_HEADER_BYTES as u64))
        .ok_or_else(|| {
            StableIdentityMappingError::Corrupt("stable identity file length overflow".to_string())
        })
}

fn page_offset(
    page_id: u64,
    header: StableIdentityMappingHeader,
) -> Result<u64, StableIdentityMappingError> {
    if page_id == 0 || page_id > header.page_count {
        return Err(StableIdentityMappingError::Corrupt(format!(
            "stable identity page {page_id} exceeds page count {}",
            header.page_count
        )));
    }
    page_id
        .checked_sub(1)
        .and_then(|ordinal| ordinal.checked_mul(header.page_bytes))
        .and_then(|offset| offset.checked_add(FILE_HEADER_BYTES as u64))
        .ok_or_else(|| {
            StableIdentityMappingError::Corrupt("stable identity page offset overflow".to_string())
        })
}

fn encode_page_slot(
    generation: u64,
    page_id: u64,
    entry_count: usize,
    first_key: StableIdentityKey,
    last_key: StableIdentityKey,
    payload: &[u8],
    config: StableIdentityMappingConfig,
) -> Result<Vec<u8>, StableIdentityMappingError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        StableIdentityMappingError::Admission(
            "stable identity page payload does not fit u32".to_string(),
        )
    })?;
    let entry_count = u32::try_from(entry_count).map_err(|_| {
        StableIdentityMappingError::Admission(
            "stable identity page entry count does not fit u32".to_string(),
        )
    })?;
    if payload.len().saturating_add(PAGE_HEADER_BYTES) > config.page_bytes.get() {
        return Err(StableIdentityMappingError::Admission(
            "stable identity page exceeds configured slot".to_string(),
        ));
    }
    let mut slot = vec![0u8; config.page_bytes.get()];
    slot[..8].copy_from_slice(PAGE_MAGIC);
    slot[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    slot[12..20].copy_from_slice(&generation.to_le_bytes());
    slot[20..28].copy_from_slice(&page_id.to_le_bytes());
    slot[28..32].copy_from_slice(&entry_count.to_le_bytes());
    slot[32..36].copy_from_slice(&payload_len.to_le_bytes());
    slot[36..45].copy_from_slice(&encode_key(first_key));
    slot[45..54].copy_from_slice(&encode_key(last_key));
    slot[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + payload.len()].copy_from_slice(payload);
    let mut hasher = IntegrityHasher::new();
    hasher.update(&slot[..56]);
    hasher.update(payload);
    let digest = hasher.finish();
    slot[56..60].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    slot[60..92].copy_from_slice(digest.sha256.as_bytes());
    Ok(slot)
}

fn decode_page_slot(
    slot: &[u8],
    expected_generation: u64,
    expected_page_id: u64,
    config: StableIdentityMappingConfig,
) -> Result<StableIdentityPageView<'_>, StableIdentityMappingError> {
    if slot.len() != config.page_bytes.get() || &slot[..8] != PAGE_MAGIC {
        return Err(StableIdentityMappingError::Corrupt(
            "invalid stable identity page header".to_string(),
        ));
    }
    let version = read_u16(&slot[8..10]);
    let flags = read_u16(&slot[10..12]);
    if version != FORMAT_VERSION || flags != 0 || slot[54..56] != [0, 0] || slot[92..96] != [0; 4] {
        return Err(StableIdentityMappingError::Corrupt(format!(
            "unsupported stable identity page version {version}, flags {flags}, or reserved bytes"
        )));
    }
    let generation = read_u64(&slot[12..20]);
    let page_id = read_u64(&slot[20..28]);
    if generation != expected_generation || page_id != expected_page_id {
        return Err(StableIdentityMappingError::Corrupt(format!(
            "stable identity page generation/id {generation}/{page_id} does not match selected {expected_generation}/{expected_page_id}"
        )));
    }
    let entry_count = read_u32(&slot[28..32]) as usize;
    if entry_count == 0 || entry_count > config.max_entries_per_page.get() {
        return Err(StableIdentityMappingError::Corrupt(format!(
            "stable identity page declares invalid entry count {entry_count}"
        )));
    }
    let payload_len = read_u32(&slot[32..36]) as usize;
    let payload_end = PAGE_HEADER_BYTES.checked_add(payload_len).ok_or_else(|| {
        StableIdentityMappingError::Corrupt(
            "stable identity page payload length overflow".to_string(),
        )
    })?;
    if payload_end > slot.len() || slot[payload_end..].iter().any(|byte| *byte != 0) {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity page payload exceeds slot or has non-zero padding".to_string(),
        ));
    }
    let payload = &slot[PAGE_HEADER_BYTES..payload_end];
    let mut hasher = IntegrityHasher::new();
    hasher.update(&slot[..56]);
    hasher.update(payload);
    let digest = hasher.finish();
    if digest.crc32c.get() != read_u32(&slot[56..60])
        || digest.sha256.as_bytes() != &slot[60..60 + SHA256_BYTES]
    {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity page checksum mismatch".to_string(),
        ));
    }
    let first_key = decode_key(&slot[36..45])?;
    let last_key = decode_key(&slot[45..54])?;
    if first_key > last_key {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity page key bounds are reversed".to_string(),
        ));
    }
    let view = StableIdentityPageView {
        first_key,
        last_key,
        entry_count,
        payload,
    };
    let mut first = None;
    let mut previous = None;
    let mut last = None;
    let mut validated = 0usize;
    view.visit_encoded_entries(|key, value| {
        if value.len() > config.max_value_bytes.get() {
            return Err(StableIdentityMappingError::Corrupt(format!(
                "stable identity value contains {} bytes, exceeding limit {}",
                value.len(),
                config.max_value_bytes
            )));
        }
        if previous.is_some_and(|prior| prior >= key) {
            return Err(StableIdentityMappingError::Corrupt(
                "stable identity page keys are not strictly ordered".to_string(),
            ));
        }
        first.get_or_insert(key);
        previous = Some(key);
        last = Some(key);
        validated += 1;
        Ok(true)
    })?;
    if validated != entry_count || first != Some(first_key) || last != Some(last_key) {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity page count or key bounds do not match its payload".to_string(),
        ));
    }
    Ok(view)
}

fn encode_key(key: StableIdentityKey) -> [u8; ENCODED_KEY_BYTES] {
    let mut encoded = [0u8; ENCODED_KEY_BYTES];
    encoded[0] = key.kind.tag();
    encoded[1..].copy_from_slice(&key.physical_id.to_be_bytes());
    encoded
}

fn decode_key(encoded: &[u8]) -> Result<StableIdentityKey, StableIdentityMappingError> {
    if encoded.len() != ENCODED_KEY_BYTES {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity key has an invalid length".to_string(),
        ));
    }
    Ok(StableIdentityKey {
        kind: StableIdentityKind::from_tag(encoded[0])?,
        physical_id: u64::from_be_bytes(
            encoded[1..]
                .try_into()
                .expect("stable identity physical id has a fixed length"),
        ),
    })
}

fn encode_stable_value(value: &Value) -> Result<Vec<u8>, StableIdentityMappingError> {
    encode_residual_row_properties(&[(0, value)]).map_err(|error| {
        StableIdentityMappingError::Admission(format!("encode stable identity value: {error}"))
    })
}

fn decode_stable_value(encoded: &[u8]) -> Result<Value, StableIdentityMappingError> {
    let mut entries = decode_residual_row_properties(encoded).map_err(|error| {
        StableIdentityMappingError::Corrupt(format!("decode stable identity value: {error}"))
    })?;
    if entries.len() != 1 || entries[0].0 != 0 {
        return Err(StableIdentityMappingError::Corrupt(
            "stable identity value must contain exactly field zero".to_string(),
        ));
    }
    Ok(entries.pop().expect("one value was validated").1)
}

fn estimated_value_resident_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 0,
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::List(values) => values
            .iter()
            .map(estimated_value_resident_bytes)
            .fold(0usize, usize::saturating_add),
        Value::Map(values) => values.iter().fold(0usize, |bytes, (key, value)| {
            bytes
                .saturating_add(key.len())
                .saturating_add(estimated_value_resident_bytes(value))
        }),
    })
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("u16 field has a fixed length"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("u32 field has a fixed length"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("u64 field has a fixed length"))
}

fn durability(context: &'static str) -> impl FnOnce(std::io::Error) -> StableIdentityMappingError {
    move |error| StableIdentityMappingError::Durability(format!("{context}: {error}"))
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    FileExt::read_exact_at(file, buffer, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;
    while !buffer.is_empty() {
        let read = file.seek_read(buffer, offset)?;
        if read == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill buffer",
            ));
        }
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "read offset overflow"))?;
        buffer = &mut buffer[read..];
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::sync::Mutex;
    static POSITIONED_READ_LOCK: Mutex<()> = Mutex::new(());
    let _guard = POSITIONED_READ_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut file = file;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "skein-stable-identity-{name}-{}-{}",
            std::process::id(),
            TEST_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn mapping(count: u64) -> StoreStableIdMapping {
        StoreStableIdMapping {
            node_stable_ids: (0..count)
                .map(|id| (crate::NodeId(id), Value::String(format!("node-{id}"))))
                .collect(),
            relationship_stable_ids: (0..count)
                .map(|id| {
                    (
                        crate::RelId(id),
                        Value::List(vec![
                            Value::String("rel".to_string()),
                            Value::Int(id as i64),
                        ]),
                    )
                })
                .collect(),
        }
    }

    fn entries(
        mapping: &StoreStableIdMapping,
    ) -> impl Iterator<Item = (StableIdentityKey, &Value)> {
        mapping
            .node_stable_ids
            .iter()
            .map(|(id, value)| (StableIdentityKey::node(id.0), value))
            .chain(
                mapping
                    .relationship_stable_ids
                    .iter()
                    .map(|(id, value)| (StableIdentityKey::relationship(id.0), value)),
            )
    }

    #[test]
    fn cold_open_reads_only_header_and_lookup_is_demand_paged() {
        let path = test_path("cold-open");
        let mapping = mapping(500);
        let config = StableIdentityMappingConfig {
            page_bytes: NonZeroUsize::new(1024).unwrap(),
            max_entries_per_page: NonZeroUsize::new(8).unwrap(),
            max_value_bytes: NonZeroUsize::new(512).unwrap(),
            ..StableIdentityMappingConfig::default()
        };
        let output = StableIdentityMappingWriter::publish(&path, 7, entries(&mapping), config)
            .expect("publish stable identity mapping");
        assert!(output.header.page_count > 2);
        let cache = Arc::new(SegmentCache::new(4 * 1024));
        let reader = StableIdentityMappingReader::open_with_cache(
            &path,
            config,
            Arc::clone(&cache),
            StoreId(9),
        )
        .expect("open stable identity mapping");

        assert_eq!(cache.snapshot().resident_bytes, 0);
        let (value, report) = reader
            .lookup(
                StableIdentityKey::relationship(321),
                StableIdentityReadLimits::default(),
            )
            .expect("lookup stable identity");
        assert_eq!(
            value,
            Some(Value::List(vec![
                Value::String("rel".to_string()),
                Value::Int(321)
            ]))
        );
        assert!(report.visited_pages < output.header.page_count as usize);
        assert!(report.storage_bytes_read <= DEFAULT_STABLE_IDENTITY_LOOKUP_BYTES);
        assert!(cache.snapshot().resident_bytes > 0);

        fs::remove_file(&path).expect("remove stable identity fixture");
    }

    #[test]
    fn materialization_round_trips_and_enforces_resident_budget() {
        let path = test_path("materialize");
        let expected = mapping(32);
        StableIdentityMappingWriter::publish(
            &path,
            11,
            entries(&expected),
            StableIdentityMappingConfig::default(),
        )
        .expect("publish stable identity mapping");
        let reader =
            StableIdentityMappingReader::open(&path, StableIdentityMappingConfig::default())
                .expect("open stable identity mapping");
        let (actual, report) = reader
            .materialize(StableIdentityMaterializeLimits::default())
            .expect("materialize stable identity mapping");
        assert_eq!(actual, expected);
        assert_eq!(report.decoded_entries, 64);

        let error = reader
            .materialize(StableIdentityMaterializeLimits {
                max_resident_bytes: NonZeroUsize::new(1).unwrap(),
                ..StableIdentityMaterializeLimits::default()
            })
            .expect_err("materialization must honor resident budget");
        assert!(matches!(error, StableIdentityMappingError::Admission(_)));
        assert!(!reader.is_poisoned());

        fs::remove_file(path).expect("remove stable identity fixture");
    }

    #[test]
    fn selected_page_corruption_poison_fails_closed() {
        let path = test_path("corrupt");
        let expected = mapping(4);
        StableIdentityMappingWriter::publish(
            &path,
            3,
            entries(&expected),
            StableIdentityMappingConfig::default(),
        )
        .expect("publish stable identity mapping");
        let mut bytes = fs::read(&path).expect("read stable identity fixture");
        bytes[FILE_HEADER_BYTES + PAGE_HEADER_BYTES] ^= 0x80;
        fs::write(&path, bytes).expect("corrupt stable identity fixture");
        let reader =
            StableIdentityMappingReader::open(&path, StableIdentityMappingConfig::default())
                .expect("header remains valid");

        let error = reader
            .lookup(
                StableIdentityKey::node(0),
                StableIdentityReadLimits::default(),
            )
            .expect_err("selected corrupt page must fail");
        assert!(matches!(error, StableIdentityMappingError::Corrupt(_)));
        assert!(reader.is_poisoned());
        assert!(reader
            .lookup(
                StableIdentityKey::node(1),
                StableIdentityReadLimits::default(),
            )
            .expect_err("poisoned reader must reject later reads")
            .to_string()
            .contains("poisoned"));

        fs::remove_file(path).expect("remove stable identity fixture");
    }

    #[test]
    fn deep_scrub_detects_corruption_outside_the_lookup_path() {
        let path = test_path("deep-scrub");
        let expected = mapping(64);
        let config = StableIdentityMappingConfig {
            page_bytes: NonZeroUsize::new(1024).unwrap(),
            max_entries_per_page: NonZeroUsize::new(4).unwrap(),
            max_value_bytes: NonZeroUsize::new(512).unwrap(),
            ..StableIdentityMappingConfig::default()
        };
        let output = StableIdentityMappingWriter::publish(&path, 4, entries(&expected), config)
            .expect("publish stable identity mapping");
        assert!(output.header.page_count > 2);
        let mut bytes = fs::read(&path).expect("read stable identity fixture");
        let last_page_offset = FILE_HEADER_BYTES
            + usize::try_from(output.header.page_count - 1).unwrap() * config.page_bytes.get();
        bytes[last_page_offset + PAGE_HEADER_BYTES] ^= 0x80;
        fs::write(&path, bytes).expect("corrupt an unvisited stable identity page");
        let reader = StableIdentityMappingReader::open(&path, config)
            .expect("open mapping with valid header and length");

        assert_eq!(
            reader
                .lookup(
                    StableIdentityKey::node(0),
                    StableIdentityReadLimits::default()
                )
                .expect("lookup must not visit the corrupt last page")
                .0,
            expected.node_stable_ids.get(&crate::NodeId(0)).cloned()
        );
        assert!(!reader.is_poisoned());
        let error = reader
            .deep_scrub()
            .expect_err("deep scrub must inspect the corrupt page");
        assert!(matches!(error, StableIdentityMappingError::Corrupt(_)));
        assert!(reader.is_poisoned());

        fs::remove_file(path).expect("remove stable identity fixture");
    }

    #[test]
    fn publication_replaces_one_complete_generation() {
        let path = test_path("replace");
        let first = mapping(2);
        let first_output = StableIdentityMappingWriter::publish(
            &path,
            5,
            entries(&first),
            StableIdentityMappingConfig::default(),
        )
        .expect("publish first stable identity mapping");
        let second = mapping(3);
        let second_output = StableIdentityMappingWriter::publish(
            &path,
            6,
            entries(&second),
            StableIdentityMappingConfig::default(),
        )
        .expect("publish second stable identity mapping");

        assert_eq!(
            second_output.header.generation,
            first_output.header.generation + 1
        );
        assert_eq!(second_output.header.covered_commit_epoch, 6);
        let reader =
            StableIdentityMappingReader::open(&path, StableIdentityMappingConfig::default())
                .expect("open replacement generation");
        assert_eq!(reader.header(), second_output.header);
        assert_eq!(
            reader
                .materialize(StableIdentityMaterializeLimits::default())
                .expect("materialize replacement")
                .0,
            second
        );

        fs::remove_file(path).expect("remove stable identity fixture");
    }

    #[test]
    fn pinned_reader_keeps_its_generation_across_replacement() {
        let path = test_path("pinned-generation");
        let config = StableIdentityMappingConfig::default();
        let first = StoreStableIdMapping {
            node_stable_ids: BTreeMap::from([(
                crate::NodeId(1),
                Value::String("first".to_string()),
            )]),
            ..StoreStableIdMapping::default()
        };
        StableIdentityMappingWriter::publish(&path, 1, entries(&first), config)
            .expect("publish first mapping");
        let pinned = StableIdentityMappingReader::open(&path, config)
            .expect("open pinned stable identity reader");
        let second = StoreStableIdMapping {
            node_stable_ids: BTreeMap::from([(
                crate::NodeId(1),
                Value::String("second".to_string()),
            )]),
            ..StoreStableIdMapping::default()
        };
        StableIdentityMappingWriter::publish(&path, 2, entries(&second), config)
            .expect("publish replacement mapping");
        let latest = StableIdentityMappingReader::open(&path, config)
            .expect("open latest stable identity reader");

        assert_eq!(
            pinned
                .lookup(
                    StableIdentityKey::node(1),
                    StableIdentityReadLimits::default()
                )
                .expect("read pinned mapping")
                .0,
            Some(Value::String("first".to_string()))
        );
        assert_eq!(
            latest
                .lookup(
                    StableIdentityKey::node(1),
                    StableIdentityReadLimits::default()
                )
                .expect("read latest mapping")
                .0,
            Some(Value::String("second".to_string()))
        );

        fs::remove_file(path).expect("remove stable identity fixture");
    }

    #[test]
    fn writer_rejects_out_of_order_and_oversized_values_without_replacing_target() {
        let path = test_path("reject");
        let original = mapping(1);
        let config = StableIdentityMappingConfig::default();
        StableIdentityMappingWriter::publish(&path, 1, entries(&original), config)
            .expect("publish original mapping");
        let original_bytes = fs::read(&path).expect("read original mapping");
        let values = BTreeMap::from([
            (StableIdentityKey::node(2), Value::String("two".to_string())),
            (StableIdentityKey::node(1), Value::String("one".to_string())),
        ]);
        let reverse = values.iter().rev().map(|(key, value)| (*key, value));
        assert!(StableIdentityMappingWriter::publish(&path, 2, reverse, config).is_err());
        assert_eq!(fs::read(&path).unwrap(), original_bytes);

        let huge = Value::String("x".repeat(DEFAULT_STABLE_IDENTITY_VALUE_BYTES + 1));
        assert!(StableIdentityMappingWriter::publish(
            &path,
            2,
            [(StableIdentityKey::node(1), &huge)],
            config,
        )
        .is_err());
        assert_eq!(fs::read(&path).unwrap(), original_bytes);

        fs::remove_file(&path).expect("remove stable identity fixture");
        let temporary = path.with_extension("skein.tmp");
        if temporary.exists() {
            fs::remove_file(temporary).expect("remove rejected candidate");
        }
    }

    #[test]
    fn writer_enforces_cumulative_artifact_limit_before_replacement() {
        let path = test_path("artifact-limit");
        let page_bytes = 256usize;
        let config = StableIdentityMappingConfig {
            page_bytes: NonZeroUsize::new(page_bytes).unwrap(),
            max_entries_per_page: NonZeroUsize::new(1).unwrap(),
            max_value_bytes: NonZeroUsize::new(64).unwrap(),
            max_artifact_bytes: NonZeroU64::new((FILE_HEADER_BYTES + page_bytes) as u64).unwrap(),
        };
        let original = Value::String("original".to_string());
        StableIdentityMappingWriter::publish(
            &path,
            1,
            [(StableIdentityKey::node(1), &original)],
            config,
        )
        .expect("publish one-page mapping");
        let original_bytes = fs::read(&path).expect("read original mapping");
        let first = Value::String("first".to_string());
        let second = Value::String("second".to_string());

        let error = StableIdentityMappingWriter::publish(
            &path,
            2,
            [
                (StableIdentityKey::node(1), &first),
                (StableIdentityKey::node(2), &second),
            ],
            config,
        )
        .expect_err("two-page mapping must exceed the artifact limit");
        assert!(matches!(error, StableIdentityMappingError::Admission(_)));
        assert_eq!(fs::read(&path).unwrap(), original_bytes);

        fs::remove_file(&path).expect("remove stable identity fixture");
        let temporary = path.with_extension("skein.tmp");
        if temporary.exists() {
            fs::remove_file(temporary).expect("remove rejected candidate");
        }
    }
}
