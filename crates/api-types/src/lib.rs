use skein_core::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeNeighborDirection {
    Outgoing,
    Incoming,
    Both,
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
