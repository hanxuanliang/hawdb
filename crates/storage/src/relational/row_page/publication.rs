use super::{
    ImmutableRelationalRowPage, RelationalRowPageError, RelationalRowPageId,
    RelationalRowPageLimits,
};
use skein_integrity::Sha256Digest;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};

mod manifest;
mod publisher;
mod reader;
mod root;

pub use publisher::RelationalRowPagePublisher;
pub use reader::RelationalRowPageRootReader;

pub const RELATIONAL_ROW_PAGE_MANIFEST_FILE: &str = "relational-row-pages.manifest.skein";
const RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE: &str = "relational-row-pages.lock";

pub const DEFAULT_RELATIONAL_ROW_PAGE_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_TABLES: usize = 4096;
pub const DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_PAGES: usize = 4096;
pub const DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_ROOT_PAGES: u64 = 16 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_ROW_PAGE_ROOT_KEY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_RELATIONAL_ROW_PAGE_TABLE_NAME_BYTES: usize = 1024;

pub fn relational_row_page_artifact_file(generation: u64) -> String {
    format!("relational-row-pages-{generation}.pages.skein")
}

pub fn relational_row_page_root_descriptor_file(generation: u64) -> String {
    format!("relational-row-root-{generation}.descriptors.skein")
}

pub fn relational_row_page_root_key_file(generation: u64) -> String {
    format!("relational-row-root-{generation}.keys.skein")
}

pub fn relational_row_page_manifest_generation_file(generation: u64) -> String {
    format!("relational-row-pages-{generation}.manifest.skein")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPagePublicationConfig {
    pub page_limits: RelationalRowPageLimits,
    pub max_manifest_bytes: NonZeroUsize,
    pub max_tables: NonZeroUsize,
    pub max_table_name_bytes: NonZeroUsize,
    pub max_dirty_pages: NonZeroUsize,
    pub max_dirty_bytes: NonZeroU64,
    pub max_root_pages: NonZeroU64,
    pub max_root_key_bytes: NonZeroU64,
}

impl Default for RelationalRowPagePublicationConfig {
    fn default() -> Self {
        Self {
            page_limits: RelationalRowPageLimits::default(),
            max_manifest_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_MANIFEST_BYTES)
                .expect("default row-page manifest limit is non-zero"),
            max_tables: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_TABLES)
                .expect("default row-page table limit is non-zero"),
            max_table_name_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_TABLE_NAME_BYTES)
                .expect("default row-page table-name limit is non-zero"),
            max_dirty_pages: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_PAGES)
                .expect("default row-page dirty-page limit is non-zero"),
            max_dirty_bytes: NonZeroU64::new(DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_BYTES)
                .expect("default row-page dirty-byte limit is non-zero"),
            max_root_pages: NonZeroU64::new(DEFAULT_RELATIONAL_ROW_PAGE_ROOT_PAGES)
                .expect("default row-page root-page limit is non-zero"),
            max_root_key_bytes: NonZeroU64::new(DEFAULT_RELATIONAL_ROW_PAGE_ROOT_KEY_BYTES)
                .expect("default row-page root-key limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageTableDelta {
    pub table: String,
    pub schema_digest: Sha256Digest,
    pub dirty_pages: Vec<ImmutableRelationalRowPage>,
    pub deleted_page_ids: Vec<RelationalRowPageId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageArtifactMetadata {
    pub encoded_len: u64,
    pub encoded_crc32c: u32,
    pub encoded_sha256: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageSlotIntegrity {
    pub encoded_len: u32,
    pub slot_crc32c: u32,
    pub slot_sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageRootDescriptor {
    pub logical_page_id: RelationalRowPageId,
    pub physical_generation: u64,
    pub physical_slot: u64,
    pub source_commit_epoch: u64,
    pub row_count: u32,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
    pub slot_integrity: RelationalRowPageSlotIntegrity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageTableRoot {
    pub table: String,
    pub schema_digest: Sha256Digest,
    pub first_descriptor: u64,
    pub page_count: u64,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageRootManifest {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub previous_generation: Option<u64>,
    pub page_bytes: u64,
    pub dirty_page_count: u64,
    pub root_page_count: u64,
    pub page_artifact: RelationalRowPageArtifactMetadata,
    pub root_descriptor_artifact: RelationalRowPageArtifactMetadata,
    pub root_key_artifact: RelationalRowPageArtifactMetadata,
    pub root_set_digest: Sha256Digest,
    pub tables: Vec<RelationalRowPageTableRoot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalRowPagePublicationPhase {
    CandidateStarted,
    CandidatePagesDurable,
    CandidateRootDurable,
    CandidateManifestDurable,
    BaseRevalidated,
    LatestManifestPublished,
}

const COMPLETE_PUBLICATION_TRACE: [RelationalRowPagePublicationPhase; 6] = [
    RelationalRowPagePublicationPhase::CandidateStarted,
    RelationalRowPagePublicationPhase::CandidatePagesDurable,
    RelationalRowPagePublicationPhase::CandidateRootDurable,
    RelationalRowPagePublicationPhase::CandidateManifestDurable,
    RelationalRowPagePublicationPhase::BaseRevalidated,
    RelationalRowPagePublicationPhase::LatestManifestPublished,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPagePublicationReport {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub dirty_pages_written: u64,
    pub root_pages: u64,
    pub reused_pages: u64,
    pub page_artifact_bytes: u64,
    pub root_descriptor_bytes: u64,
    pub root_key_bytes: u64,
    pub manifest_bytes: u64,
    pub events: [RelationalRowPagePublicationPhase; 6],
}

#[derive(Debug)]
pub enum RelationalRowPagePublicationError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    MissingTable(String),
    StaleGeneration {
        expected_previous: Option<u64>,
        actual_previous: Option<u64>,
    },
}

impl fmt::Display for RelationalRowPagePublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(formatter, "relational row-page publication admission failed: {message}")
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt relational row-page publication: {message}")
            }
            Self::Durability(message) => {
                write!(formatter, "relational row-page publication durability failed: {message}")
            }
            Self::MissingTable(table) => {
                write!(formatter, "relational row-page root has no table {table}")
            }
            Self::StaleGeneration {
                expected_previous,
                actual_previous,
            } => write!(
                formatter,
                "relational row-page generation changed: expected {expected_previous:?}, found {actual_previous:?}"
            ),
        }
    }
}

impl std::error::Error for RelationalRowPagePublicationError {}

impl From<RelationalRowPageError> for RelationalRowPagePublicationError {
    fn from(error: RelationalRowPageError) -> Self {
        match error {
            RelationalRowPageError::Admission(message) => Self::Admission(message),
            RelationalRowPageError::Corrupt(message) => Self::Corrupt(message),
        }
    }
}

fn durability(
    context: &'static str,
) -> impl FnOnce(std::io::Error) -> RelationalRowPagePublicationError {
    move |error| RelationalRowPagePublicationError::Durability(format!("{context}: {error}"))
}

#[cfg(test)]
#[path = "publication/tests.rs"]
mod tests;
