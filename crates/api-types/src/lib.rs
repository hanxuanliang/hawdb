use skein_core::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryDecayDetailRequest {
    pub memory_id: String,
    pub property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryDecayDetail {
    pub memory_id: Option<String>,
    pub node_id: u64,
    pub title: Option<String>,
    pub content: Option<String>,
    pub unit_type: Option<String>,
    pub source: Option<String>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: String,
    pub created_at: Option<Value>,
    pub decay_score_cached: Option<Value>,
    pub metadata: Option<Value>,
    pub is_latest: Option<bool>,
    pub lifecycle_state: Option<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryDecayDetailOutput {
    pub graph_commit_epoch: u64,
    pub found: bool,
    pub memory: Option<KnowledgeMemoryDecayDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCleanupFingerprintRequest {
    pub memory_ids: Vec<String>,
    pub property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCleanupFingerprintRow {
    pub memory_id: Option<String>,
    pub node_id: u64,
    pub title: Option<String>,
    pub metadata: Option<Value>,
    pub is_latest: Option<bool>,
    pub decay_score_cached: Option<Value>,
    pub created_at: Option<Value>,
    pub last_accessed_at: Option<Value>,
    pub last_clicked_at: Option<Value>,
    pub access_count: Option<Value>,
    pub appearances: Option<Value>,
    pub clicks: Option<Value>,
    pub total_dwell_time_ms: Option<Value>,
    pub importance: Option<Value>,
    pub unit_type: Option<String>,
    pub semantic_field: Option<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCleanupFingerprintOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeMemoryCleanupFingerprintRow>,
    pub matched_count: usize,
    pub returned_count: usize,
    pub missing_memory_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryDecayRefreshUpdate {
    pub memory_id: String,
    pub decay_score_cached: Value,
    pub confidence: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryDecayRefreshBatchRequest {
    pub updates: Vec<KnowledgeMemoryDecayRefreshUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryDecayRefreshBatchRow {
    pub memory_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryDecayRefreshBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeMemoryDecayRefreshBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}
