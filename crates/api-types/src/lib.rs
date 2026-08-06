use skein_core::Value;

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
