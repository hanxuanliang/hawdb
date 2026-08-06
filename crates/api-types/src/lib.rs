use skein_core::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeNeighborDirection {
    Outgoing,
    Incoming,
    Both,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesRelationCountRequest {
    pub memory_ids: Vec<String>,
    pub content_relations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesRelationCountRow {
    pub memory_id: String,
    pub node_id: u64,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesRelationCountOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeMemoryEvolvesRelationCountRow>,
    pub matched_memory_count: usize,
    pub missing_memory_ids: Vec<String>,
    pub matched_relationship_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCrystalSynthesisCountRequest {
    pub memory_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCrystalSynthesisCountRow {
    pub memory_id: String,
    pub node_id: u64,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCrystalSynthesisCountOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeMemoryCrystalSynthesisCountRow>,
    pub matched_memory_count: usize,
    pub missing_memory_ids: Vec<String>,
    pub matched_relationship_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesNeighborRequest {
    pub memory_id: String,
    pub direction: KnowledgeNeighborDirection,
    pub neighbor_property_names: Vec<String>,
    pub relationship_property_names: Vec<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesNeighborRow {
    pub anchor_memory_id: String,
    pub anchor_node_id: u64,
    pub neighbor_memory_id: Option<String>,
    pub neighbor_node_id: u64,
    pub neighbor_properties: BTreeMap<String, Value>,
    pub relationship_id: u64,
    pub relationship_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesNeighborOutput {
    pub graph_commit_epoch: u64,
    pub anchor_found: bool,
    pub anchor_node_id: Option<u64>,
    pub rows: Vec<KnowledgeMemoryEvolvesNeighborRow>,
    pub matched_relationship_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KnowledgeMemoryEvolvesProjectedSuccessorOrder {
    #[default]
    StableMemoryIdAsc,
    UpdatedAtDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesProjectedSuccessorCursor {
    pub new_memory_id: Option<String>,
    pub new_node_id: u64,
    pub relationship_id: u64,
    pub updated_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesProjectedSuccessorPageCursor {
    pub old_memory_id: String,
    pub cursor: KnowledgeMemoryEvolvesProjectedSuccessorCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesProjectedSuccessorRequest {
    pub old_memory_ids: Vec<String>,
    pub limit_per_old_memory: usize,
    pub order: KnowledgeMemoryEvolvesProjectedSuccessorOrder,
    pub page_cursors: Vec<KnowledgeMemoryEvolvesProjectedSuccessorPageCursor>,
    pub new_memory_property_names: Vec<String>,
    pub relationship_property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesProjectedSuccessorRow {
    pub new_memory_id: Option<String>,
    pub new_node_id: u64,
    pub relationship_id: u64,
    pub page_cursor: KnowledgeMemoryEvolvesProjectedSuccessorCursor,
    pub new_memory_properties: BTreeMap<String, Value>,
    pub relationship_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesProjectedSuccessorGroup {
    pub old_memory_id: String,
    pub old_node_id: Option<u64>,
    pub found_old_memory: bool,
    pub rows: Vec<KnowledgeMemoryEvolvesProjectedSuccessorRow>,
    pub matched_relationship_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEvolvesProjectedSuccessorOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeMemoryEvolvesProjectedSuccessorGroup>,
    pub found_old_memory_count: usize,
    pub missing_old_memory_count: usize,
    pub matched_relationship_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelRegexMemoryConnectionsRequest {
    pub label_name_pattern: String,
    pub memory_property_names: Vec<String>,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelRegexMemoryConnectionRow {
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub memory_properties: BTreeMap<String, Value>,
    pub label_id: Option<String>,
    pub label_node_id: u64,
    pub label_name: Option<String>,
    pub label_connections: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelRegexMemoryConnectionsOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeLabelRegexMemoryConnectionRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}
