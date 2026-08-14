use super::{
    RelationalError, RelationalForeignKeySchema, RelationalKey, RelationalKeySetPages,
    RelationalReferentialAction, RelationalScalarType, RelationalState, RelationalTableSchema,
    RelationalValue,
};
use crate::{
    durable_replace_file, ImmutableIndexPage, ImmutableIndexPageBody, ImmutableIndexPageError,
    ImmutableIndexPageLimits, IndexIdentity, IndexInteriorEntry, IndexInteriorPage, IndexLeafEntry,
    IndexLeafPage, IndexLeafPosting, IndexPageId, IndexPostingPage, IndexRootPage, IndexRowId,
};
use fs2::FileExt;
use skein_integrity::{integrity_digest, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MANIFEST_MAGIC: &[u8; 8] = b"SKRIDXM1";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_HEADER_BYTES: usize = 92;
const PRIMARY_INDEX_NAME: &str = "__primary__";
const RELATIONAL_INDEX_SHADOW_LOCK_FILE: &str = "relational-index-shadow.lock";

pub const RELATIONAL_INDEX_SHADOW_MANIFEST_FILE: &str = "relational-index-shadow.manifest.skein";
pub const DEFAULT_RELATIONAL_INDEX_SHADOW_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_SHADOW_ROOTS: usize = 4096;
pub const DEFAULT_RELATIONAL_INDEX_SHADOW_BUILD_METADATA_BYTES: usize = 64 * 1024 * 1024;

pub fn relational_index_shadow_artifact_file(generation: u64) -> String {
    format!("relational-index-shadow-{generation}.pages.skein")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexShadowConfig {
    pub page_limits: ImmutableIndexPageLimits,
    pub max_manifest_bytes: NonZeroUsize,
    pub max_roots: NonZeroUsize,
    pub max_build_metadata_bytes: NonZeroUsize,
}

impl Default for RelationalIndexShadowConfig {
    fn default() -> Self {
        Self {
            page_limits: ImmutableIndexPageLimits::default(),
            max_manifest_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_SHADOW_MANIFEST_BYTES)
                .expect("default relational index manifest limit is non-zero"),
            max_roots: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_SHADOW_ROOTS)
                .expect("default relational index root limit is non-zero"),
            max_build_metadata_bytes: NonZeroUsize::new(
                DEFAULT_RELATIONAL_INDEX_SHADOW_BUILD_METADATA_BYTES,
            )
            .expect("default relational index build metadata limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexRootDescriptor {
    pub identity: IndexIdentity,
    pub schema_digest: Sha256Digest,
    pub root_page_id: IndexPageId,
    pub height: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexShadowManifest {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub page_bytes: u64,
    pub page_count: u64,
    pub roots: Vec<RelationalIndexRootDescriptor>,
}

impl RelationalIndexShadowManifest {
    pub fn root(&self, table: &str, index: &str) -> Option<&RelationalIndexRootDescriptor> {
        self.roots
            .binary_search_by(|root| {
                (
                    root.identity.namespace.as_str(),
                    root.identity.name.as_str(),
                )
                    .cmp(&(table, index))
            })
            .ok()
            .map(|position| &self.roots[position])
    }

    fn encode(
        &self,
        config: RelationalIndexShadowConfig,
    ) -> Result<Vec<u8>, RelationalIndexShadowError> {
        validate_manifest(self, config, ErrorClass::Admission)?;
        let mut payload = Vec::new();
        for root in &self.roots {
            encode_bytes(&mut payload, root.identity.namespace.as_bytes())?;
            encode_bytes(&mut payload, root.identity.name.as_bytes())?;
            payload.extend_from_slice(root.schema_digest.as_bytes());
            payload.extend_from_slice(&root.root_page_id.get().to_le_bytes());
            payload.extend_from_slice(&root.height.to_le_bytes());
            if MANIFEST_HEADER_BYTES.saturating_add(payload.len()) > config.max_manifest_bytes.get()
            {
                return Err(RelationalIndexShadowError::Admission(format!(
                    "relational index manifest exceeds {} bytes",
                    config.max_manifest_bytes
                )));
            }
        }
        let payload_len = u64::try_from(payload.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "relational index manifest payload length does not fit u64".to_string(),
            )
        })?;
        let root_count = u32::try_from(self.roots.len()).map_err(|_| {
            RelationalIndexShadowError::Admission(
                "relational index root count does not fit u32".to_string(),
            )
        })?;
        let mut encoded = Vec::with_capacity(MANIFEST_HEADER_BYTES + payload.len());
        encoded.extend_from_slice(MANIFEST_MAGIC);
        encoded.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        encoded.extend_from_slice(&self.generation.to_le_bytes());
        encoded.extend_from_slice(&self.source_commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.page_bytes.to_le_bytes());
        encoded.extend_from_slice(&self.page_count.to_le_bytes());
        encoded.extend_from_slice(&root_count.to_le_bytes());
        encoded.extend_from_slice(&payload_len.to_le_bytes());
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded);
        hasher.update(&payload);
        let digest = hasher.finish();
        encoded.extend_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded.extend_from_slice(digest.sha256.as_bytes());
        debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    fn decode(
        encoded: &[u8],
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        if encoded.len() > config.max_manifest_bytes.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "relational index manifest contains {} bytes, exceeding limit {}",
                encoded.len(),
                config.max_manifest_bytes
            )));
        }
        if encoded.len() < MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
            return Err(RelationalIndexShadowError::Corrupt(
                "invalid relational index manifest header".to_string(),
            ));
        }
        let version = read_u16(&encoded[8..10]);
        let flags = read_u16(&encoded[10..12]);
        if version != MANIFEST_VERSION || flags != 0 {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "unsupported relational index manifest version {version} or flags {flags}"
            )));
        }
        let generation = read_u64(&encoded[12..20]);
        let source_commit_epoch = read_u64(&encoded[20..28]);
        let page_bytes = read_u64(&encoded[28..36]);
        let page_count = read_u64(&encoded[36..44]);
        let root_count = read_u32(&encoded[44..48]) as usize;
        if root_count > config.max_roots.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "relational index manifest declares {root_count} roots, exceeding limit {}",
                config.max_roots
            )));
        }
        let payload_len = usize::try_from(read_u64(&encoded[48..56])).map_err(|_| {
            RelationalIndexShadowError::Corrupt(
                "relational index manifest payload length overflows usize".to_string(),
            )
        })?;
        let expected_len = MANIFEST_HEADER_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(
                    "relational index manifest length overflow".to_string(),
                )
            })?;
        if encoded.len() != expected_len {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index manifest length mismatch: expected {expected_len}, got {}",
                encoded.len()
            )));
        }
        let payload = &encoded[MANIFEST_HEADER_BYTES..];
        let mut hasher = IntegrityHasher::new();
        hasher.update(&encoded[..56]);
        hasher.update(payload);
        let digest = hasher.finish();
        let expected_crc = read_u32(&encoded[56..60]);
        if digest.crc32c.get() != expected_crc
            || digest.sha256.as_bytes() != &encoded[60..60 + SHA256_BYTES]
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index manifest checksum mismatch".to_string(),
            ));
        }
        let mut offset = 0usize;
        let mut roots = Vec::with_capacity(root_count);
        for _ in 0..root_count {
            let (namespace, next) = decode_bytes(
                payload,
                offset,
                config.page_limits.max_identity_bytes.get(),
                "index namespace",
            )?;
            offset = next;
            let remaining = config
                .page_limits
                .max_identity_bytes
                .get()
                .saturating_sub(namespace.len());
            let (name, next) = decode_bytes(payload, offset, remaining, "index name")?;
            offset = next;
            let digest_bytes = take(payload, &mut offset, SHA256_BYTES, "schema digest")?;
            let root_id_bytes = take(payload, &mut offset, 8, "root page id")?;
            let height_bytes = take(payload, &mut offset, 4, "root height")?;
            roots.push(RelationalIndexRootDescriptor {
                identity: IndexIdentity {
                    namespace: decode_utf8(namespace, "index namespace")?,
                    name: decode_utf8(name, "index name")?,
                },
                schema_digest: Sha256Digest::from_bytes(
                    digest_bytes
                        .try_into()
                        .expect("schema digest length was checked"),
                ),
                root_page_id: page_id(read_u64(root_id_bytes), "root page id")?,
                height: read_u32(height_bytes),
            });
        }
        if offset != payload.len() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index manifest contains trailing bytes".to_string(),
            ));
        }
        let manifest = Self {
            generation,
            source_commit_epoch,
            page_bytes,
            page_count,
            roots,
        };
        validate_manifest(&manifest, config, ErrorClass::Corrupt)?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalIndexShadowBuildReport {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub index_roots: usize,
    pub pages_written: u64,
    pub artifact_bytes: u64,
    pub manifest_bytes: u64,
    pub peak_build_metadata_bytes: usize,
}

#[derive(Debug)]
pub enum RelationalIndexShadowError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    StaleGeneration {
        expected_previous: Option<u64>,
        actual_previous: Option<u64>,
    },
}

impl fmt::Display for RelationalIndexShadowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(formatter, "relational index shadow admission failed: {message}")
            }
            Self::Corrupt(message) => write!(formatter, "corrupt relational index shadow: {message}"),
            Self::Durability(message) => {
                write!(formatter, "relational index shadow durability failed: {message}")
            }
            Self::StaleGeneration {
                expected_previous,
                actual_previous,
            } => write!(
                formatter,
                "relational index shadow generation changed: expected {expected_previous:?}, found {actual_previous:?}"
            ),
        }
    }
}

impl std::error::Error for RelationalIndexShadowError {}

impl From<ImmutableIndexPageError> for RelationalIndexShadowError {
    fn from(error: ImmutableIndexPageError) -> Self {
        match error {
            ImmutableIndexPageError::Admission(message) => Self::Admission(message),
            ImmutableIndexPageError::Corrupt(message) => Self::Corrupt(message),
        }
    }
}

pub struct RelationalIndexShadowWriter {
    config: RelationalIndexShadowConfig,
}

impl RelationalIndexShadowWriter {
    pub const fn new(config: RelationalIndexShadowConfig) -> Self {
        Self { config }
    }

    pub fn publish(
        &self,
        directory: &Path,
        state: &RelationalState,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
    ) -> Result<RelationalIndexShadowBuildReport, RelationalIndexShadowError> {
        if generation == 0 {
            return Err(RelationalIndexShadowError::Admission(
                "relational index generation must be non-zero".to_string(),
            ));
        }
        fs::create_dir_all(directory).map_err(durability("create shadow directory"))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(RELATIONAL_INDEX_SHADOW_LOCK_FILE))
            .map_err(durability("open shadow publication lock"))?;
        lock.lock_exclusive()
            .map_err(durability("lock shadow publication"))?;
        let paths = ShadowPublicationPaths::new(directory, generation);
        let actual_previous = current_generation(&paths.manifest, self.config)?;
        if actual_previous != expected_previous_generation {
            return Err(RelationalIndexShadowError::StaleGeneration {
                expected_previous: expected_previous_generation,
                actual_previous,
            });
        }
        if actual_previous.is_some_and(|previous| generation <= previous) {
            return Err(RelationalIndexShadowError::Admission(format!(
                "new relational index generation {generation} must exceed published generation {}",
                actual_previous.expect("checked as present")
            )));
        }
        let result = self.build_and_publish(
            state,
            generation,
            source_commit_epoch,
            expected_previous_generation,
            &paths,
        );
        if result.is_err() {
            let _ = fs::remove_file(&paths.artifact_tmp);
            let _ = fs::remove_file(&paths.manifest_tmp);
        }
        result
    }

    fn build_and_publish(
        &self,
        state: &RelationalState,
        generation: u64,
        source_commit_epoch: u64,
        expected_previous_generation: Option<u64>,
        paths: &ShadowPublicationPaths,
    ) -> Result<RelationalIndexShadowBuildReport, RelationalIndexShadowError> {
        let file =
            File::create(&paths.artifact_tmp).map_err(durability("create shadow artifact"))?;
        let mut pages = SlotWriter::new(
            file,
            generation,
            source_commit_epoch,
            self.config.page_limits,
        );
        let mut roots = Vec::new();
        let mut peak_build_metadata_bytes = 0usize;
        for (table, schema) in &state.schemas {
            let segment = state.segments.get(table).ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(format!(
                    "table {table} is missing its relational segment"
                ))
            })?;
            let schema_digest = relational_schema_digest(schema)?;
            let identity = IndexIdentity {
                namespace: table.clone(),
                name: PRIMARY_INDEX_NAME.to_string(),
            };
            let mut tree = TreeWriter::new(
                &mut pages,
                identity,
                schema_digest,
                self.config.max_build_metadata_bytes.get(),
            );
            for (primary_key, _) in segment.rows.iter() {
                let encoded = encode_relational_key(primary_key)?;
                tree.push(IndexLeafEntry {
                    key: encoded.clone(),
                    posting: IndexLeafPosting::Inline(vec![IndexRowId::new(encoded)]),
                })?;
            }
            let (root, peak) = tree.finish()?;
            peak_build_metadata_bytes = peak_build_metadata_bytes.max(peak);
            roots.push(root);

            for (name, index) in &segment.indexes {
                let identity = IndexIdentity {
                    namespace: table.clone(),
                    name: name.clone(),
                };
                let mut tree = TreeWriter::new(
                    &mut pages,
                    identity,
                    schema_digest,
                    self.config.max_build_metadata_bytes.get(),
                );
                for (key, postings) in index.iter() {
                    let key = encode_relational_key(key)?;
                    let posting = tree.write_postings(postings)?;
                    tree.push(IndexLeafEntry { key, posting })?;
                }
                let (root, peak) = tree.finish()?;
                peak_build_metadata_bytes = peak_build_metadata_bytes.max(peak);
                roots.push(root);
            }
            if roots.len() > self.config.max_roots.get() {
                return Err(RelationalIndexShadowError::Admission(format!(
                    "relational index shadow contains {} roots, exceeding limit {}",
                    roots.len(),
                    self.config.max_roots
                )));
            }
        }
        roots.sort_by(|left, right| {
            (&left.identity.namespace, &left.identity.name)
                .cmp(&(&right.identity.namespace, &right.identity.name))
        });
        if roots
            .windows(2)
            .any(|pair| pair[0].identity == pair[1].identity)
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index shadow contains duplicate index identities".to_string(),
            ));
        }
        let page_count = pages.finish()?;
        let page_bytes = self.config.page_limits.max_page_bytes.get() as u64;
        let artifact_bytes = page_count.checked_mul(page_bytes).ok_or_else(|| {
            RelationalIndexShadowError::Admission("shadow artifact size overflow".to_string())
        })?;
        durable_replace_file(&paths.artifact_tmp, &paths.artifact)
            .map_err(durability("publish shadow artifact"))?;

        let manifest = RelationalIndexShadowManifest {
            generation,
            source_commit_epoch,
            page_bytes,
            page_count,
            roots,
        };
        let encoded_manifest = manifest.encode(self.config)?;
        {
            let mut file = File::create(&paths.manifest_tmp)
                .map_err(durability("create shadow manifest candidate"))?;
            file.write_all(&encoded_manifest)
                .map_err(durability("write shadow manifest candidate"))?;
            file.sync_all()
                .map_err(durability("sync shadow manifest candidate"))?;
        }
        let actual_previous = current_generation(&paths.manifest, self.config)?;
        if actual_previous != expected_previous_generation {
            return Err(RelationalIndexShadowError::StaleGeneration {
                expected_previous: expected_previous_generation,
                actual_previous,
            });
        }
        durable_replace_file(&paths.manifest_tmp, &paths.manifest)
            .map_err(durability("publish shadow manifest"))?;
        Ok(RelationalIndexShadowBuildReport {
            generation,
            source_commit_epoch,
            index_roots: manifest.roots.len(),
            pages_written: page_count,
            artifact_bytes,
            manifest_bytes: encoded_manifest.len() as u64,
            peak_build_metadata_bytes,
        })
    }
}

struct ShadowPublicationPaths {
    artifact: PathBuf,
    artifact_tmp: PathBuf,
    manifest: PathBuf,
    manifest_tmp: PathBuf,
}

impl ShadowPublicationPaths {
    fn new(directory: &Path, generation: u64) -> Self {
        let artifact = directory.join(relational_index_shadow_artifact_file(generation));
        let manifest = directory.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE);
        Self {
            artifact_tmp: artifact.with_extension("skein.tmp"),
            manifest_tmp: manifest.with_extension("skein.tmp"),
            artifact,
            manifest,
        }
    }
}

pub struct RelationalIndexShadowReader {
    directory: PathBuf,
    manifest: RelationalIndexShadowManifest,
    config: RelationalIndexShadowConfig,
    poisoned: AtomicBool,
}

impl RelationalIndexShadowReader {
    pub fn open_latest(
        directory: &Path,
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        let manifest_path = directory.join(RELATIONAL_INDEX_SHADOW_MANIFEST_FILE);
        let encoded = read_bounded_file(
            &manifest_path,
            config.max_manifest_bytes.get(),
            "relational index shadow manifest",
        )?;
        let manifest = RelationalIndexShadowManifest::decode(&encoded, config)?;
        Self::from_manifest(directory, manifest, config)
    }

    pub fn open(
        directory: &Path,
        expected_generation: u64,
        expected_source_commit_epoch: u64,
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        let reader = Self::open_latest(directory, config)?;
        if reader.manifest.generation != expected_generation
            || reader.manifest.source_commit_epoch != expected_source_commit_epoch
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index shadow fence mismatch: expected generation/epoch {expected_generation}/{expected_source_commit_epoch}, found {}/{}",
                reader.manifest.generation, reader.manifest.source_commit_epoch
            )));
        }
        Ok(reader)
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalIndexShadowManifest,
        config: RelationalIndexShadowConfig,
    ) -> Result<Self, RelationalIndexShadowError> {
        let artifact_path =
            directory.join(relational_index_shadow_artifact_file(manifest.generation));
        let actual_bytes = fs::metadata(&artifact_path)
            .map_err(durability("inspect shadow artifact"))?
            .len();
        let expected_bytes = manifest
            .page_count
            .checked_mul(manifest.page_bytes)
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(
                    "relational index shadow artifact size overflow".to_string(),
                )
            })?;
        if actual_bytes != expected_bytes {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index shadow artifact length mismatch: expected {expected_bytes}, got {actual_bytes}"
            )));
        }
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest,
            config,
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn manifest(&self) -> &RelationalIndexShadowManifest {
        &self.manifest
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub fn read_page(
        &self,
        page_id: IndexPageId,
    ) -> Result<ImmutableIndexPage, RelationalIndexShadowError> {
        if page_id.get() > self.manifest.page_count {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "page {} exceeds published page count {}",
                page_id.get(),
                self.manifest.page_count
            )));
        }
        if self.is_poisoned() {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index shadow reader is poisoned by an earlier page failure".to_string(),
            ));
        }
        let result = self.read_page_inner(page_id);
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn read_page_inner(
        &self,
        page_id: IndexPageId,
    ) -> Result<ImmutableIndexPage, RelationalIndexShadowError> {
        let page_bytes = self.config.page_limits.max_page_bytes.get();
        let offset = page_id
            .get()
            .checked_sub(1)
            .and_then(|ordinal| ordinal.checked_mul(page_bytes as u64))
            .ok_or_else(|| {
                RelationalIndexShadowError::Corrupt("index page offset overflow".to_string())
            })?;
        let path = self.directory.join(relational_index_shadow_artifact_file(
            self.manifest.generation,
        ));
        let mut file = File::open(path).map_err(durability("open shadow artifact"))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(durability("seek shadow page"))?;
        let mut slot = vec![0; page_bytes];
        file.read_exact(&mut slot)
            .map_err(durability("read shadow page"))?;
        let page = ImmutableIndexPage::decode_slot(&slot, self.config.page_limits)?;
        if page.generation != self.manifest.generation
            || page.source_commit_epoch != self.manifest.source_commit_epoch
            || page.page_id != page_id
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "page {} does not match the selected generation/epoch/id",
                page_id.get()
            )));
        }
        Ok(page)
    }

    pub fn read_root(
        &self,
        descriptor: &RelationalIndexRootDescriptor,
    ) -> Result<IndexRootPage, RelationalIndexShadowError> {
        if self
            .manifest
            .root(&descriptor.identity.namespace, &descriptor.identity.name)
            != Some(descriptor)
        {
            return Err(RelationalIndexShadowError::Corrupt(
                "root descriptor is not selected by this shadow manifest".to_string(),
            ));
        }
        let page = self.read_page(descriptor.root_page_id)?;
        let ImmutableIndexPageBody::Root(root) = page.body else {
            self.poisoned.store(true, Ordering::Release);
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "root descriptor {}.{} references a non-root page",
                descriptor.identity.namespace, descriptor.identity.name
            )));
        };
        if root.identity != descriptor.identity
            || root.schema_digest != descriptor.schema_digest
            || root.height != descriptor.height
        {
            self.poisoned.store(true, Ordering::Release);
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "root page {} disagrees with its manifest descriptor",
                descriptor.root_page_id.get()
            )));
        }
        Ok(root)
    }
}

struct SlotWriter {
    file: File,
    generation: u64,
    source_commit_epoch: u64,
    limits: ImmutableIndexPageLimits,
    next_page_id: u64,
    written_pages: u64,
}

impl SlotWriter {
    fn new(
        file: File,
        generation: u64,
        source_commit_epoch: u64,
        limits: ImmutableIndexPageLimits,
    ) -> Self {
        Self {
            file,
            generation,
            source_commit_epoch,
            limits,
            next_page_id: 1,
            written_pages: 0,
        }
    }

    fn allocate(&mut self) -> Result<IndexPageId, RelationalIndexShadowError> {
        let page_id = page_id(self.next_page_id, "allocated page id")?;
        self.next_page_id = self.next_page_id.checked_add(1).ok_or_else(|| {
            RelationalIndexShadowError::Admission("index page id overflow".to_string())
        })?;
        Ok(page_id)
    }

    fn reserve(&mut self, count: usize) -> Result<IndexPageId, RelationalIndexShadowError> {
        if count == 0 {
            return Err(RelationalIndexShadowError::Admission(
                "cannot reserve zero index pages".to_string(),
            ));
        }
        let first = page_id(self.next_page_id, "reserved page id")?;
        self.next_page_id = self.next_page_id.checked_add(count as u64).ok_or_else(|| {
            RelationalIndexShadowError::Admission("index page id overflow".to_string())
        })?;
        Ok(first)
    }

    fn write(
        &mut self,
        page_id: IndexPageId,
        body: ImmutableIndexPageBody,
    ) -> Result<(), RelationalIndexShadowError> {
        let expected_page_id = self.written_pages.checked_add(1).ok_or_else(|| {
            RelationalIndexShadowError::Admission("written page count overflow".to_string())
        })?;
        if page_id.get() >= self.next_page_id || page_id.get() != expected_page_id {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "index page {} was written out of order; expected {expected_page_id}",
                page_id.get(),
            )));
        }
        let slot = ImmutableIndexPage {
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            page_id,
            body,
        }
        .encode_slot(self.limits)?;
        let offset = page_id
            .get()
            .checked_sub(1)
            .and_then(|ordinal| ordinal.checked_mul(self.limits.max_page_bytes.get() as u64))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission("index page offset overflow".to_string())
            })?;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(durability("seek shadow page slot"))?;
        self.file
            .write_all(&slot)
            .map_err(durability("write shadow page slot"))?;
        self.written_pages = expected_page_id;
        Ok(())
    }

    fn finish(self) -> Result<u64, RelationalIndexShadowError> {
        let page_count = self.next_page_id.saturating_sub(1);
        if self.written_pages != page_count {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "relational index writer reserved {page_count} pages but wrote {}",
                self.written_pages
            )));
        }
        self.file
            .sync_all()
            .map_err(durability("sync shadow page artifact"))?;
        Ok(page_count)
    }
}

#[derive(Clone)]
struct ChildPage {
    upper_bound: Vec<u8>,
    page_id: IndexPageId,
}

struct TreeWriter<'a> {
    pages: &'a mut SlotWriter,
    identity: IndexIdentity,
    schema_digest: Sha256Digest,
    leaf_entries: Vec<IndexLeafEntry>,
    leaf_payload_bytes: usize,
    children: Vec<ChildPage>,
    child_metadata_bytes: usize,
    peak_metadata_bytes: usize,
    max_metadata_bytes: usize,
    last_key: Option<Vec<u8>>,
}

impl<'a> TreeWriter<'a> {
    fn new(
        pages: &'a mut SlotWriter,
        identity: IndexIdentity,
        schema_digest: Sha256Digest,
        max_metadata_bytes: usize,
    ) -> Self {
        Self {
            pages,
            identity,
            schema_digest,
            leaf_entries: Vec::new(),
            leaf_payload_bytes: 0,
            children: Vec::new(),
            child_metadata_bytes: 0,
            peak_metadata_bytes: 0,
            max_metadata_bytes,
            last_key: None,
        }
    }

    fn push(&mut self, entry: IndexLeafEntry) -> Result<(), RelationalIndexShadowError> {
        if self
            .last_key
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= entry.key.as_slice())
        {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "index {}.{} does not encode to strictly ordered keys",
                self.identity.namespace, self.identity.name
            )));
        }
        let entry_bytes = leaf_entry_payload_bytes(&entry)?;
        let max_payload = max_page_payload(self.pages.limits)?;
        if entry_bytes > max_payload {
            return Err(RelationalIndexShadowError::Admission(format!(
                "one leaf entry for {}.{} needs {entry_bytes} bytes, exceeding page payload {max_payload}",
                self.identity.namespace, self.identity.name
            )));
        }
        if !self.leaf_entries.is_empty()
            && (self.leaf_entries.len() >= self.pages.limits.max_entries.get()
                || self.leaf_payload_bytes.saturating_add(entry_bytes) > max_payload)
        {
            self.flush_leaf()?;
        }
        self.last_key = Some(entry.key.clone());
        self.leaf_payload_bytes = self.leaf_payload_bytes.saturating_add(entry_bytes);
        self.leaf_entries.push(entry);
        Ok(())
    }

    fn write_postings(
        &mut self,
        postings: &RelationalKeySetPages,
    ) -> Result<IndexLeafPosting, RelationalIndexShadowError> {
        if postings.len <= self.pages.limits.max_inline_postings.get() {
            let inline_bytes = postings.iter().try_fold(0usize, |bytes, key| {
                let encoded = encode_relational_key(key)?;
                bytes.checked_add(4 + encoded.len()).ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "inline posting size overflow".to_string(),
                    )
                })
            })?;
            if inline_bytes <= max_page_payload(self.pages.limits)? / 2 {
                let row_ids = postings
                    .iter()
                    .map(|key| encode_relational_key(key).map(IndexRowId::new))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(IndexLeafPosting::Inline(row_ids));
            }
        }

        let chunk_count = posting_chunk_count(postings, self.pages.limits)?;
        let first = self.pages.reserve(chunk_count)?;
        let max_payload = max_page_payload(self.pages.limits)?;
        let next_field_bytes = 6usize + 8;
        let mut chunk = Vec::new();
        let mut chunk_bytes = next_field_bytes;
        let mut ordinal = 0usize;
        for key in postings.iter() {
            let row_id = IndexRowId::new(encode_relational_key(key)?);
            let entry_bytes = 6usize.checked_add(row_id.as_bytes().len()).ok_or_else(|| {
                RelationalIndexShadowError::Admission("posting entry size overflow".to_string())
            })?;
            if !chunk.is_empty()
                && (chunk.len() >= self.pages.limits.max_entries.get()
                    || chunk_bytes.saturating_add(entry_bytes) > max_payload)
            {
                self.write_posting_chunk(first, ordinal, chunk_count, std::mem::take(&mut chunk))?;
                ordinal += 1;
                chunk_bytes = next_field_bytes;
            }
            chunk_bytes = chunk_bytes.saturating_add(entry_bytes);
            chunk.push(row_id);
        }
        if !chunk.is_empty() {
            self.write_posting_chunk(first, ordinal, chunk_count, chunk)?;
            ordinal += 1;
        }
        if ordinal != chunk_count {
            return Err(RelationalIndexShadowError::Corrupt(format!(
                "posting chunk count changed between sizing and write: expected {chunk_count}, wrote {ordinal}"
            )));
        }
        Ok(IndexLeafPosting::Page {
            first,
            total_rows: postings.len as u64,
        })
    }

    fn write_posting_chunk(
        &mut self,
        first: IndexPageId,
        ordinal: usize,
        chunk_count: usize,
        row_ids: Vec<IndexRowId>,
    ) -> Result<(), RelationalIndexShadowError> {
        let current = page_id(first.get() + ordinal as u64, "posting page id")?;
        let next = (ordinal + 1 < chunk_count)
            .then(|| page_id(current.get() + 1, "next posting page id"))
            .transpose()?;
        self.pages.write(
            current,
            ImmutableIndexPageBody::Posting(IndexPostingPage { next, row_ids }),
        )
    }

    fn flush_leaf(&mut self) -> Result<(), RelationalIndexShadowError> {
        let page_id = self.pages.allocate()?;
        let upper_bound = self
            .leaf_entries
            .last()
            .map_or_else(Vec::new, |entry| entry.key.clone());
        let entries = std::mem::take(&mut self.leaf_entries);
        self.leaf_payload_bytes = 0;
        self.pages.write(
            page_id,
            ImmutableIndexPageBody::Leaf(IndexLeafPage { entries }),
        )?;
        self.push_child(ChildPage {
            upper_bound,
            page_id,
        })
    }

    fn push_child(&mut self, child: ChildPage) -> Result<(), RelationalIndexShadowError> {
        let bytes = child
            .upper_bound
            .len()
            .checked_add(std::mem::size_of::<ChildPage>())
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "index build metadata size overflow".to_string(),
                )
            })?;
        self.child_metadata_bytes =
            self.child_metadata_bytes
                .checked_add(bytes)
                .ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "index build metadata size overflow".to_string(),
                    )
                })?;
        if self.child_metadata_bytes > self.max_metadata_bytes {
            return Err(RelationalIndexShadowError::Admission(format!(
                "index build metadata uses {} bytes, exceeding limit {}",
                self.child_metadata_bytes, self.max_metadata_bytes
            )));
        }
        self.peak_metadata_bytes = self.peak_metadata_bytes.max(self.child_metadata_bytes);
        self.children.push(child);
        Ok(())
    }

    fn finish(
        mut self,
    ) -> Result<(RelationalIndexRootDescriptor, usize), RelationalIndexShadowError> {
        if !self.leaf_entries.is_empty() || self.children.is_empty() {
            self.flush_leaf()?;
        }
        let mut children = std::mem::take(&mut self.children);
        let mut height = 1u32;
        while children.len() > 1 {
            children = write_interior_level(
                self.pages,
                children,
                self.max_metadata_bytes,
                &mut self.peak_metadata_bytes,
            )?;
            height = height.checked_add(1).ok_or_else(|| {
                RelationalIndexShadowError::Admission("index tree height overflow".to_string())
            })?;
        }
        let child = children.pop().expect("tree always has one child page");
        let root_page_id = self.pages.allocate()?;
        self.pages.write(
            root_page_id,
            ImmutableIndexPageBody::Root(IndexRootPage {
                identity: self.identity.clone(),
                schema_digest: self.schema_digest,
                child: child.page_id,
                height,
            }),
        )?;
        Ok((
            RelationalIndexRootDescriptor {
                identity: self.identity,
                schema_digest: self.schema_digest,
                root_page_id,
                height,
            },
            self.peak_metadata_bytes,
        ))
    }
}

fn write_interior_level(
    pages: &mut SlotWriter,
    children: Vec<ChildPage>,
    max_metadata_bytes: usize,
    peak_metadata_bytes: &mut usize,
) -> Result<Vec<ChildPage>, RelationalIndexShadowError> {
    let max_payload = max_page_payload(pages.limits)?;
    let mut next = Vec::new();
    let mut entries = Vec::new();
    let mut payload_bytes = 0usize;
    for child in children {
        let entry_bytes = 6usize
            .checked_add(4)
            .and_then(|bytes| bytes.checked_add(child.upper_bound.len()))
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission("interior entry size overflow".to_string())
            })?;
        if entry_bytes > max_payload {
            return Err(RelationalIndexShadowError::Admission(
                "one interior separator exceeds the page payload limit".to_string(),
            ));
        }
        if !entries.is_empty()
            && (entries.len() >= pages.limits.max_entries.get()
                || payload_bytes.saturating_add(entry_bytes) > max_payload)
        {
            flush_interior(pages, &mut entries, &mut next)?;
            payload_bytes = 0;
        }
        payload_bytes = payload_bytes.saturating_add(entry_bytes);
        entries.push(IndexInteriorEntry {
            upper_bound: child.upper_bound,
            child: child.page_id,
        });
    }
    if !entries.is_empty() {
        flush_interior(pages, &mut entries, &mut next)?;
    }
    let metadata_bytes = next.iter().try_fold(0usize, |bytes, child| {
        bytes
            .checked_add(std::mem::size_of::<ChildPage>())
            .and_then(|bytes| bytes.checked_add(child.upper_bound.len()))
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "index build metadata size overflow".to_string(),
                )
            })
    })?;
    if metadata_bytes > max_metadata_bytes {
        return Err(RelationalIndexShadowError::Admission(format!(
            "index build metadata uses {metadata_bytes} bytes, exceeding limit {max_metadata_bytes}"
        )));
    }
    *peak_metadata_bytes = (*peak_metadata_bytes).max(metadata_bytes);
    Ok(next)
}

fn flush_interior(
    pages: &mut SlotWriter,
    entries: &mut Vec<IndexInteriorEntry>,
    next: &mut Vec<ChildPage>,
) -> Result<(), RelationalIndexShadowError> {
    let upper_bound = entries
        .last()
        .expect("interior flush requires at least one entry")
        .upper_bound
        .clone();
    let page_id = pages.allocate()?;
    pages.write(
        page_id,
        ImmutableIndexPageBody::Interior(IndexInteriorPage {
            entries: std::mem::take(entries),
        }),
    )?;
    next.push(ChildPage {
        upper_bound,
        page_id,
    });
    Ok(())
}

fn posting_chunk_count(
    postings: &RelationalKeySetPages,
    limits: ImmutableIndexPageLimits,
) -> Result<usize, RelationalIndexShadowError> {
    let max_payload = max_page_payload(limits)?;
    let mut chunks = 0usize;
    let mut entries = 0usize;
    let mut payload_bytes = 6usize + 8;
    for key in postings.iter() {
        let row_id = encode_relational_key(key)?;
        if row_id.len() > limits.max_row_id_bytes.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "encoded primary key contains {} bytes, exceeding row-id limit {}",
                row_id.len(),
                limits.max_row_id_bytes
            )));
        }
        let entry_bytes = 6usize.checked_add(row_id.len()).ok_or_else(|| {
            RelationalIndexShadowError::Admission("posting entry size overflow".to_string())
        })?;
        if entry_bytes + 6 + 8 > max_payload {
            return Err(RelationalIndexShadowError::Admission(
                "one posting row id exceeds the page payload limit".to_string(),
            ));
        }
        if entries > 0
            && (entries >= limits.max_entries.get()
                || payload_bytes.saturating_add(entry_bytes) > max_payload)
        {
            chunks = chunks.saturating_add(1);
            entries = 0;
            payload_bytes = 6 + 8;
        }
        entries += 1;
        payload_bytes = payload_bytes.saturating_add(entry_bytes);
    }
    if entries > 0 {
        chunks = chunks.saturating_add(1);
    }
    if chunks == 0 {
        return Err(RelationalIndexShadowError::Corrupt(
            "materialized index contains an empty posting list".to_string(),
        ));
    }
    Ok(chunks)
}

fn leaf_entry_payload_bytes(entry: &IndexLeafEntry) -> Result<usize, RelationalIndexShadowError> {
    let posting_bytes = match &entry.posting {
        IndexLeafPosting::Inline(row_ids) => {
            row_ids.iter().try_fold(1usize + 4, |bytes, row_id| {
                bytes
                    .checked_add(4)
                    .and_then(|bytes| bytes.checked_add(row_id.as_bytes().len()))
                    .ok_or_else(|| {
                        RelationalIndexShadowError::Admission(
                            "inline posting size overflow".to_string(),
                        )
                    })
            })?
        }
        IndexLeafPosting::Page { .. } => 1 + 8 + 8,
    };
    6usize
        .checked_add(4)
        .and_then(|bytes| bytes.checked_add(entry.key.len()))
        .and_then(|bytes| bytes.checked_add(posting_bytes))
        .ok_or_else(|| {
            RelationalIndexShadowError::Admission("leaf entry size overflow".to_string())
        })
}

fn max_page_payload(limits: ImmutableIndexPageLimits) -> Result<usize, RelationalIndexShadowError> {
    limits.max_payload_bytes().ok_or_else(|| {
        RelationalIndexShadowError::Admission(
            "immutable index page limit is smaller than its header".to_string(),
        )
    })
}

fn encode_relational_key(key: &RelationalKey) -> Result<Vec<u8>, RelationalIndexShadowError> {
    let mut encoded = Vec::new();
    for value in &key.0 {
        encode_ordered_value(&mut encoded, value)?;
    }
    Ok(encoded)
}

fn encode_ordered_value(
    encoded: &mut Vec<u8>,
    value: &RelationalValue,
) -> Result<(), RelationalIndexShadowError> {
    match value {
        RelationalValue::Null => encoded.push(0),
        RelationalValue::Boolean(value) => {
            encoded.push(1);
            encoded.push(u8::from(*value));
        }
        RelationalValue::BigInt(value) => {
            encoded.push(2);
            encoded.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        RelationalValue::DoublePrecision(value) => {
            encoded.push(3);
            let bits = value.to_bits();
            let ordered = if bits >> 63 == 0 {
                bits ^ (1_u64 << 63)
            } else {
                !bits
            };
            encoded.extend_from_slice(&ordered.to_be_bytes());
        }
        RelationalValue::Text(value) => {
            encoded.push(4);
            encode_escaped_bytes(encoded, value.as_bytes());
        }
        RelationalValue::Bytea(value) => {
            encoded.push(5);
            encode_escaped_bytes(encoded, value);
        }
        RelationalValue::Overflow(_) => {
            return Err(RelationalIndexShadowError::Corrupt(
                "indexed relational values must not use overflow references".to_string(),
            ))
        }
    }
    Ok(())
}

fn encode_escaped_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            encoded.extend_from_slice(&[0, 255]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
}

fn relational_schema_digest(
    schema: &RelationalTableSchema,
) -> Result<Sha256Digest, RelationalIndexShadowError> {
    let mut encoded = Vec::new();
    encode_bytes(&mut encoded, schema.name.as_bytes())?;
    encode_count(&mut encoded, schema.columns.len(), "columns")?;
    for column in &schema.columns {
        encode_bytes(&mut encoded, column.name.as_bytes())?;
        encoded.push(scalar_type_tag(column.scalar_type));
        encoded.push(u8::from(column.nullable));
        encoded.push(u8::from(column.default.is_some()));
        if let Some(default) = &column.default {
            encode_ordered_value(&mut encoded, default)?;
        }
    }
    encode_string_list(&mut encoded, &schema.primary_key)?;
    encode_count(
        &mut encoded,
        schema.unique_constraints.len(),
        "unique constraints",
    )?;
    for columns in &schema.unique_constraints {
        encode_string_list(&mut encoded, columns)?;
    }
    encode_count(&mut encoded, schema.foreign_keys.len(), "foreign keys")?;
    for foreign_key in &schema.foreign_keys {
        encode_foreign_key(&mut encoded, foreign_key)?;
    }
    encode_count(&mut encoded, schema.indexes.len(), "indexes")?;
    for index in &schema.indexes {
        encode_bytes(&mut encoded, index.name.as_bytes())?;
        encode_string_list(&mut encoded, &index.columns)?;
        encoded.push(u8::from(index.unique));
    }
    Ok(integrity_digest(&encoded).sha256)
}

fn encode_foreign_key(
    encoded: &mut Vec<u8>,
    foreign_key: &RelationalForeignKeySchema,
) -> Result<(), RelationalIndexShadowError> {
    encode_string_list(encoded, &foreign_key.columns)?;
    encode_bytes(encoded, foreign_key.referenced_table.as_bytes())?;
    encode_string_list(encoded, &foreign_key.referenced_columns)?;
    encoded.push(referential_action_tag(foreign_key.on_delete));
    encoded.push(referential_action_tag(foreign_key.on_update));
    Ok(())
}

fn scalar_type_tag(scalar_type: RelationalScalarType) -> u8 {
    match scalar_type {
        RelationalScalarType::Boolean => 1,
        RelationalScalarType::BigInt => 2,
        RelationalScalarType::DoublePrecision => 3,
        RelationalScalarType::Text => 4,
        RelationalScalarType::Bytea => 5,
    }
}

fn referential_action_tag(action: RelationalReferentialAction) -> u8 {
    match action {
        RelationalReferentialAction::NoAction => 0,
        RelationalReferentialAction::Restrict => 1,
    }
}

fn encode_string_list(
    encoded: &mut Vec<u8>,
    values: &[String],
) -> Result<(), RelationalIndexShadowError> {
    encode_count(encoded, values.len(), "string list")?;
    for value in values {
        encode_bytes(encoded, value.as_bytes())?;
    }
    Ok(())
}

fn encode_count(
    encoded: &mut Vec<u8>,
    count: usize,
    context: &str,
) -> Result<(), RelationalIndexShadowError> {
    let count = u32::try_from(count).map_err(|_| {
        RelationalIndexShadowError::Admission(format!("{context} count does not fit u32"))
    })?;
    encoded.extend_from_slice(&count.to_le_bytes());
    Ok(())
}

fn validate_manifest(
    manifest: &RelationalIndexShadowManifest,
    config: RelationalIndexShadowConfig,
    error_class: ErrorClass,
) -> Result<(), RelationalIndexShadowError> {
    if manifest.generation == 0 {
        return Err(invalid(error_class, "manifest generation must be non-zero"));
    }
    if manifest.page_bytes != config.page_limits.max_page_bytes.get() as u64 {
        return Err(invalid(
            error_class,
            format!(
                "manifest page size {} does not match configured page size {}",
                manifest.page_bytes, config.page_limits.max_page_bytes
            ),
        ));
    }
    if manifest.roots.len() > config.max_roots.get() {
        return Err(invalid(
            error_class,
            "manifest root count exceeds its configured limit",
        ));
    }
    let mut previous: Option<(&str, &str)> = None;
    for root in &manifest.roots {
        let identity = (
            root.identity.namespace.as_str(),
            root.identity.name.as_str(),
        );
        let identity_bytes = root
            .identity
            .namespace
            .len()
            .checked_add(root.identity.name.len())
            .ok_or_else(|| invalid(error_class, "root identity size overflow"))?;
        if root.identity.namespace.is_empty()
            || root.identity.name.is_empty()
            || identity_bytes > config.page_limits.max_identity_bytes.get()
            || root.height == 0
            || root.root_page_id.get() > manifest.page_count
        {
            return Err(invalid(
                error_class,
                "manifest contains an invalid root descriptor",
            ));
        }
        if previous.is_some_and(|previous| previous >= identity) {
            return Err(invalid(
                error_class,
                "manifest roots must be strictly ordered",
            ));
        }
        previous = Some(identity);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

fn invalid(error_class: ErrorClass, message: impl Into<String>) -> RelationalIndexShadowError {
    match error_class {
        ErrorClass::Admission => RelationalIndexShadowError::Admission(message.into()),
        ErrorClass::Corrupt => RelationalIndexShadowError::Corrupt(message.into()),
    }
}

fn current_generation(
    manifest_path: &Path,
    config: RelationalIndexShadowConfig,
) -> Result<Option<u64>, RelationalIndexShadowError> {
    match fs::metadata(manifest_path) {
        Ok(_) => read_bounded_file(
            manifest_path,
            config.max_manifest_bytes.get(),
            "current relational index shadow manifest",
        )
        .and_then(|bytes| RelationalIndexShadowManifest::decode(&bytes, config))
        .map(|manifest| Some(manifest.generation)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(RelationalIndexShadowError::Durability(format!(
            "failed to inspect current relational index shadow manifest: {error}"
        ))),
    }
}

fn read_bounded_file(
    path: &Path,
    max_bytes: usize,
    context: &str,
) -> Result<Vec<u8>, RelationalIndexShadowError> {
    let len = fs::metadata(path)
        .map_err(durability("inspect bounded file"))?
        .len();
    if len > max_bytes as u64 {
        return Err(RelationalIndexShadowError::Admission(format!(
            "{context} contains {len} bytes, exceeding limit {max_bytes}"
        )));
    }
    fs::read(path).map_err(durability("read bounded file"))
}

fn encode_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RelationalIndexShadowError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RelationalIndexShadowError::Admission("byte string length does not fit u32".to_string())
    })?;
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn decode_bytes<'a>(
    encoded: &'a [u8],
    offset: usize,
    max_bytes: usize,
    context: &str,
) -> Result<(&'a [u8], usize), RelationalIndexShadowError> {
    let len_bytes = encoded.get(offset..offset + 4).ok_or_else(|| {
        RelationalIndexShadowError::Corrupt(format!("truncated {context} length"))
    })?;
    let len = read_u32(len_bytes) as usize;
    if len > max_bytes {
        return Err(RelationalIndexShadowError::Admission(format!(
            "{context} contains {len} bytes, exceeding limit {max_bytes}"
        )));
    }
    let start = offset + 4;
    let end = start
        .checked_add(len)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("{context} length overflow")))?;
    let bytes = encoded
        .get(start..end)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("truncated {context}")))?;
    Ok((bytes, end))
}

fn take<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    len: usize,
    context: &str,
) -> Result<&'a [u8], RelationalIndexShadowError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("{context} offset overflow")))?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("truncated {context}")))?;
    *offset = end;
    Ok(bytes)
}

fn decode_utf8(bytes: &[u8], context: &str) -> Result<String, RelationalIndexShadowError> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| RelationalIndexShadowError::Corrupt(format!("{context} is not UTF-8")))
}

fn page_id(value: u64, context: &str) -> Result<IndexPageId, RelationalIndexShadowError> {
    NonZeroU64::new(value)
        .map(IndexPageId::new)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("{context} must be non-zero")))
}

fn durability(context: &'static str) -> impl FnOnce(std::io::Error) -> RelationalIndexShadowError {
    move |error| RelationalIndexShadowError::Durability(format!("{context}: {error}"))
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

impl From<RelationalError> for RelationalIndexShadowError {
    fn from(error: RelationalError) -> Self {
        Self::Corrupt(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_checksum_covers_generation_fence() {
        let config = RelationalIndexShadowConfig::default();
        let manifest = RelationalIndexShadowManifest {
            generation: 7,
            source_commit_epoch: 11,
            page_bytes: config.page_limits.max_page_bytes.get() as u64,
            page_count: 1,
            roots: vec![RelationalIndexRootDescriptor {
                identity: IndexIdentity {
                    namespace: "documents".to_string(),
                    name: PRIMARY_INDEX_NAME.to_string(),
                },
                schema_digest: integrity_digest(b"documents schema").sha256,
                root_page_id: page_id(1, "test root").unwrap(),
                height: 1,
            }],
        };
        let mut encoded = manifest.encode(config).unwrap();
        encoded[12] ^= 1;

        assert!(matches!(
            RelationalIndexShadowManifest::decode(&encoded, config),
            Err(RelationalIndexShadowError::Corrupt(message))
                if message.contains("checksum mismatch")
        ));
    }

    #[test]
    fn relational_key_encoding_preserves_total_order() {
        let values = vec![
            RelationalValue::Null,
            RelationalValue::Boolean(false),
            RelationalValue::Boolean(true),
            RelationalValue::BigInt(i64::MIN),
            RelationalValue::BigInt(-1),
            RelationalValue::BigInt(0),
            RelationalValue::BigInt(i64::MAX),
            RelationalValue::DoublePrecision(f64::from_bits(u64::MAX)),
            RelationalValue::DoublePrecision(f64::NEG_INFINITY),
            RelationalValue::DoublePrecision(-0.0),
            RelationalValue::DoublePrecision(0.0),
            RelationalValue::DoublePrecision(f64::INFINITY),
            RelationalValue::DoublePrecision(f64::NAN),
            RelationalValue::Text(String::new()),
            RelationalValue::Text("a".to_string()),
            RelationalValue::Text("a\0b".to_string()),
            RelationalValue::Text("aa".to_string()),
            RelationalValue::Bytea(Vec::new()),
            RelationalValue::Bytea(vec![0]),
            RelationalValue::Bytea(vec![0, 1]),
            RelationalValue::Bytea(vec![1]),
        ];
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        let encoded = values
            .iter()
            .map(|value| encode_relational_key(&RelationalKey(vec![value.clone()])).unwrap())
            .collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn composite_key_encoding_is_prefix_safe() {
        let keys = [
            RelationalKey(vec![
                RelationalValue::Text("a".to_string()),
                RelationalValue::BigInt(1),
            ]),
            RelationalKey(vec![
                RelationalValue::Text("a\0".to_string()),
                RelationalValue::BigInt(0),
            ]),
            RelationalKey(vec![
                RelationalValue::Text("aa".to_string()),
                RelationalValue::BigInt(-1),
            ]),
        ];
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        let encoded = keys
            .iter()
            .map(|key| encode_relational_key(key).unwrap())
            .collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
