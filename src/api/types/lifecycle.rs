use super::super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsUpdate {
    pub skill_id: String,
    pub use_count: i64,
    pub success_rate: Option<Value>,
    pub last_activity_at: Value,
    pub updated_at: Value,
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsBatchRequest {
    pub updates: Vec<KnowledgeSkillUsageStatsUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillUsageStatsBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillUsageStatsBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataUpdate {
    pub skill_id: String,
    pub metadata: Value,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataBatchRequest {
    pub updates: Vec<KnowledgeSkillMetadataUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMetadataBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillMetadataBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillSourceMergeRequest {
    pub skill_id: String,
    pub memory_id: String,
    pub occasion_key: String,
    pub created_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillSourceMergeOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub skill_id: String,
    pub memory_id: String,
    pub skill_node_id: Option<u64>,
    pub memory_node_id: Option<u64>,
    pub relationship_id: Option<u64>,
    pub matched: bool,
    pub created: bool,
    pub already_exists: bool,
    pub missing_endpoint: bool,
    pub non_writable: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleUpdate {
    pub skill_id: String,
    pub stage: Option<String>,
    pub rejected_at: Option<Value>,
    pub rationale: Option<Value>,
    pub version: Option<Value>,
    pub title: Option<Value>,
    pub name: Option<Value>,
    pub description: Option<Value>,
    pub triggers: Option<Value>,
    pub tools: Option<Value>,
    pub bundle_path: Option<Value>,
    pub content_hash: Option<Value>,
    pub write_origin: Option<String>,
    pub metadata: Option<Value>,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleBatchRequest {
    pub updates: Vec<KnowledgeSkillLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillLifecycleBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDeleteBatchRequest {
    pub skill_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDeleteBatchRow {
    pub skill_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSkillDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeSkillMemoryListOrder {
    CreatedAtAsc,
    CreatedAtDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMemoryListRequest {
    pub skill_id: Option<String>,
    pub stages: Vec<String>,
    pub limit: usize,
    pub order: KnowledgeSkillMemoryListOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMemoryRow {
    pub skill_id: Option<String>,
    pub skill_node_id: u64,
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub relationship_id: u64,
    pub title: Option<String>,
    pub content: Option<String>,
    pub unit_type: Option<String>,
    pub created_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillMemoryListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeSkillMemoryRow>,
    pub matched_count: usize,
    pub returned_count: usize,
    pub matched_skill_count: usize,
    pub missing_skill_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillThreadSourceListRequest {
    pub skill_id: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillThreadSourceRow {
    pub skill_id: Option<String>,
    pub skill_node_id: u64,
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub skill_memory_relationship_id: u64,
    pub thread_id: Option<String>,
    pub thread_node_id: u64,
    pub thread_logical_id: Option<String>,
    pub title: Option<String>,
    pub source: Option<String>,
    pub compacts_to_relationship_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillThreadSourceListOutput {
    pub graph_commit_epoch: u64,
    pub skill_id: String,
    pub skill_node_id: Option<u64>,
    pub found_skill: bool,
    pub rows: Vec<KnowledgeSkillThreadSourceRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDetailLookupRequest {
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillDetailLookupOutput {
    pub graph_commit_epoch: u64,
    pub key: String,
    pub skill_node_id: Option<u64>,
    pub found_skill: bool,
    pub id: Option<String>,
    pub name: Option<String>,
    pub title: Option<String>,
    pub stage: Option<String>,
    pub version: Option<Value>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub matched_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillStateRequest {
    pub skill_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillStateOutput {
    pub graph_commit_epoch: u64,
    pub skill_id: String,
    pub skill_node_id: Option<u64>,
    pub found_skill: bool,
    pub id: Option<String>,
    pub stage: Option<String>,
    pub metadata: Option<Value>,
    pub version: Option<Value>,
    pub use_count: Option<Value>,
    pub bundle_path: Option<String>,
    pub content_hash: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KnowledgeSkillListOrder {
    IdAsc,
    #[default]
    UpdatedAtDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KnowledgeSkillListRequest {
    pub ids: Vec<String>,
    pub lookup_key: Option<String>,
    pub stages: Vec<String>,
    pub after_id: Option<String>,
    pub limit: usize,
    pub order: KnowledgeSkillListOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillRow {
    pub id: Option<String>,
    pub node_id: u64,
    pub title: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub stage: Option<String>,
    pub version: Option<Value>,
    pub use_count: i64,
    pub success_rate: Option<Value>,
    pub metadata: Option<Value>,
    pub bundle_path: Option<String>,
    pub triggers: Option<Value>,
    pub content_hash: Option<String>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: String,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub evidence_count: i64,
    pub scope: Option<String>,
    pub rationale: Option<String>,
    pub kind: Option<String>,
    pub confidence: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeSkillRow>,
    pub matched_count: usize,
    pub returned_count: usize,
    pub missing_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillProjectedListRequest {
    pub list: KnowledgeSkillListRequest,
    pub property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillProjectedRow {
    pub id: Option<String>,
    pub node_id: u64,
    pub properties: BTreeMap<String, Value>,
    pub normalized_space_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSkillProjectedListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeSkillProjectedRow>,
    pub matched_count: usize,
    pub returned_count: usize,
    pub missing_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataUpdate {
    pub thread_id: String,
    pub metadata: Value,
    pub updated_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataBatchRequest {
    pub updates: Vec<KnowledgeThreadMetadataUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataBatchRow {
    pub thread_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetadataBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeThreadMetadataBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountUpdate {
    pub thread_id: String,
    pub message_count: i64,
    pub updated_at: Option<Value>,
    pub preserve_newer_existing_updated_at: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountBatchRequest {
    pub updates: Vec<KnowledgeThreadMessageCountUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountBatchRow {
    pub thread_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_at_changed: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageCountBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeThreadMessageCountBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_at_changed_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDeleteBatchRequest {
    pub thread_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDeleteBatchRow {
    pub thread_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeThreadDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KnowledgeThreadListOrder {
    #[default]
    ThreadIdAsc,
    IdAsc,
    MessageCountDesc,
    UpdatedAtDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KnowledgeThreadListRequest {
    pub ids: Vec<String>,
    pub thread_ids: Vec<String>,
    pub lookup_key: Option<String>,
    pub source: Option<String>,
    pub normalized_space_id: Option<String>,
    pub metadata_contains: Option<String>,
    pub require_thread_id: bool,
    pub after_id: Option<String>,
    pub limit: usize,
    pub offset: usize,
    pub order: KnowledgeThreadListOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadListRow {
    pub id: Option<String>,
    pub thread_id: Option<String>,
    pub node_id: u64,
    pub display_title: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub source: Option<String>,
    pub project: Option<String>,
    pub workspace: Option<String>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: String,
    pub metadata: Option<Value>,
    pub message_count: i64,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub import_date: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeThreadListRow>,
    pub matched_count: usize,
    pub returned_count: usize,
    pub missing_ids: Vec<String>,
    pub missing_thread_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KnowledgeThreadSourceListRequest {
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadSourceListOutput {
    pub graph_commit_epoch: u64,
    pub sources: Vec<String>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadTitleLookupRequest {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadTitleLookupOutput {
    pub graph_commit_epoch: u64,
    pub id: String,
    pub thread_node_id: Option<u64>,
    pub found_thread: bool,
    pub title: Option<String>,
    pub matched_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadSourceLookupRequest {
    pub sid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadSourceLookupOutput {
    pub graph_commit_epoch: u64,
    pub sid: String,
    pub thread_node_id: Option<u64>,
    pub found_thread: bool,
    pub thread_id: Option<String>,
    pub title: Option<String>,
    pub source: Option<String>,
    pub created_at: Option<Value>,
    pub matched_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageLookupRequest {
    pub key: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageLookupOutput {
    pub graph_commit_epoch: u64,
    pub key: String,
    pub source_filter: String,
    pub thread_node_id: Option<u64>,
    pub found_thread: bool,
    pub id: Option<String>,
    pub message_count: Option<Value>,
    pub raw_space_id: Option<String>,
    pub matched_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetaLookupRequest {
    pub key: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMetaLookupOutput {
    pub graph_commit_epoch: u64,
    pub key: String,
    pub source_filter: String,
    pub thread_node_id: Option<u64>,
    pub found_thread: bool,
    pub id: Option<String>,
    pub thread_id: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub message_count: Option<Value>,
    pub source: Option<String>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub raw_space_id: Option<String>,
    pub project: Option<String>,
    pub workspace: Option<String>,
    pub matched_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityRequest {
    pub identity_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityOutput {
    pub graph_commit_epoch: u64,
    pub identity_key: String,
    pub identity_node_id: Option<u64>,
    pub found_identity: bool,
    pub thread_node_id: Option<String>,
    pub thread_id: Option<String>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: Option<String>,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityCascadeDeleteKeys {
    pub public_thread_id: String,
    pub input_thread_id: String,
    pub thread_uuid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityDeleteRequest {
    pub identity_key: Option<String>,
    pub cascade_keys: Option<KnowledgeThreadIdentityCascadeDeleteKeys>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadIdentityDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub matched_identity_count: usize,
    pub deleted_identity_count: usize,
    pub deleted_node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadSyncMetadataRequest {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadSyncMetadataOutput {
    pub graph_commit_epoch: u64,
    pub id: String,
    pub thread_node_id: Option<u64>,
    pub found_thread: bool,
    pub title: String,
    pub source: String,
    pub project: String,
    pub workspace: String,
    pub space_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDistillationCandidateRequest {
    pub normalized_space_id: String,
    pub source: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDistillationCandidateRow {
    pub id: Option<String>,
    pub thread_id: String,
    pub node_id: u64,
    pub source: Option<String>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: String,
    pub recent_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadDistillationCandidateOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeThreadDistillationCandidateRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactedMemoryListRequest {
    pub thread_id: String,
    pub identity_property: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactedMemoryRow {
    pub thread_id: Option<String>,
    pub thread_node_id: u64,
    pub thread_logical_id: Option<String>,
    pub relationship_id: u64,
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub display_title: String,
    pub title: Option<String>,
    pub content: Option<String>,
    pub content_preview: Option<String>,
    pub importance: Option<Value>,
    pub pagerank_score: Option<Value>,
    pub confidence: Option<Value>,
    pub source_range: Option<Value>,
    pub source: Option<String>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub metadata: Option<Value>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: String,
    pub last_reindexed_at: Option<Value>,
    pub reindex_needed: Option<bool>,
    pub unit_type: String,
    pub is_latest: bool,
    pub version: i64,
    pub is_crystal: bool,
    pub crystal_title: Option<String>,
    pub source_unit_count: Option<i64>,
    pub extraction_method: String,
    pub access_count: i64,
    pub appearances: i64,
    pub clicks: i64,
    pub decay_score_cached: Option<Value>,
    pub event_end: Option<Value>,
    pub event_start: Option<Value>,
    pub last_accessed_at: Option<Value>,
    pub last_clicked_at: Option<Value>,
    pub last_evaluated_at: Option<Value>,
    pub review_status: String,
    pub temporal_confidence: Option<Value>,
    pub temporal_context: Option<String>,
    pub temporal_precision: Option<String>,
    pub temporal_type: Option<String>,
    pub total_dwell_time_ms: i64,
    pub compaction_method: Option<String>,
    pub relationship_created_at: Option<Value>,
    pub relationship_properties: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactedMemoryListOutput {
    pub graph_commit_epoch: u64,
    pub thread_id: String,
    pub thread_node_id: Option<u64>,
    pub found: bool,
    pub rows: Vec<KnowledgeThreadCompactedMemoryRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactedMemoryProjectedListRequest {
    pub list: KnowledgeThreadCompactedMemoryListRequest,
    pub memory_property_names: Vec<String>,
    pub relationship_property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactedMemoryProjectedRow {
    pub thread_id: Option<String>,
    pub thread_node_id: u64,
    pub thread_logical_id: Option<String>,
    pub relationship_id: u64,
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub memory_properties: BTreeMap<String, Value>,
    pub relationship_properties: BTreeMap<String, Value>,
    pub normalized_space_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactedMemoryProjectedListOutput {
    pub graph_commit_epoch: u64,
    pub thread_id: String,
    pub thread_node_id: Option<u64>,
    pub found: bool,
    pub rows: Vec<KnowledgeThreadCompactedMemoryProjectedRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactionLinkRequest {
    pub thread_id: String,
    pub memory_id: String,
    pub compaction_method: String,
    pub created_at: Value,
    pub properties: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadCompactionLinkOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub thread_id: String,
    pub memory_id: String,
    pub thread_node_id: Option<u64>,
    pub memory_node_id: Option<u64>,
    pub matched: bool,
    pub missing_endpoint: bool,
    pub non_writable: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCompactingThreadListRequest {
    pub memory_ids: Vec<String>,
    pub limit_per_memory: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCompactingThreadRow {
    pub memory_id: String,
    pub memory_node_id: Option<u64>,
    pub found_memory: bool,
    pub thread_id: Option<String>,
    pub thread_node_id: Option<u64>,
    pub thread_logical_id: Option<String>,
    pub title: Option<String>,
    pub source: Option<String>,
    pub metadata: Option<Value>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: Option<String>,
    pub relationship_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCompactingThreadListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeMemoryCompactingThreadRow>,
    pub found_memory_count: usize,
    pub missing_memory_count: usize,
    pub returned_thread_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCompactingThreadProjectedListRequest {
    pub list: KnowledgeMemoryCompactingThreadListRequest,
    pub thread_property_names: Vec<String>,
    pub relationship_property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCompactingThreadProjectedRow {
    pub memory_id: String,
    pub memory_node_id: Option<u64>,
    pub found_memory: bool,
    pub thread_id: Option<String>,
    pub thread_node_id: Option<u64>,
    pub thread_logical_id: Option<String>,
    pub thread_properties: BTreeMap<String, Value>,
    pub normalized_space_id: Option<String>,
    pub relationship_id: Option<u64>,
    pub relationship_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryCompactingThreadProjectedListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeMemoryCompactingThreadProjectedRow>,
    pub found_memory_count: usize,
    pub missing_memory_count: usize,
    pub returned_thread_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageListRequest {
    pub thread_id: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageRow {
    pub message_id: Option<String>,
    pub node_id: u64,
    pub relationship_id: u64,
    pub role: Option<String>,
    pub content: Option<String>,
    pub order_index: Option<i64>,
    pub relationship_order_index: Option<i64>,
    pub message_order_index: Option<i64>,
    pub timestamp: Option<Value>,
    pub token_count: Option<i64>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageListOutput {
    pub graph_commit_epoch: u64,
    pub thread_id: String,
    pub thread_node_id: Option<u64>,
    pub found: bool,
    pub rows: Vec<KnowledgeThreadMessageRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageDeleteRequest {
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeThreadMessageDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub thread_id: String,
    pub thread_node_id: Option<u64>,
    pub found_thread: bool,
    pub matched_relationship_count: usize,
    pub deleted_message_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleUpdate {
    pub label_id: String,
    pub name: Option<String>,
    pub canonical_name: Option<String>,
    pub metadata: Option<Value>,
    pub updated_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleBatchRequest {
    pub updates: Vec<KnowledgeLabelLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleBatchRow {
    pub label_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeLabelLifecycleBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelCanonicalLookupRequest {
    pub canonical_name: String,
    pub exclude_label_id: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelBackfillScanRequest {
    pub exclude_label_id: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelUsageRequest {
    pub label_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelUsageListRequest {
    pub canonical_only: bool,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryDistributionRequest {
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelUsageRow {
    pub label_id: Option<String>,
    pub node_id: u64,
    pub name: Option<String>,
    pub canonical_name: Option<String>,
    pub color: Option<Value>,
    pub description: Option<Value>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub usage_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelUsageOutput {
    pub graph_commit_epoch: u64,
    pub found: bool,
    pub row: Option<KnowledgeLabelUsageRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelUsageListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeLabelUsageRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryDistributionRow {
    pub label_id: Option<String>,
    pub label_node_id: u64,
    pub label_name: Option<String>,
    pub memory_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryDistributionOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeLabelMemoryDistributionRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelDeleteRequest {
    pub memory_id: String,
    pub label_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub memory_id: String,
    pub label_id: Option<String>,
    pub memory_node_id: Option<u64>,
    pub label_node_id: Option<u64>,
    pub found_memory: bool,
    pub found_label: bool,
    pub non_writable: bool,
    pub matched_relationship_count: usize,
    pub deleted_relationship_count: usize,
    pub deleted_relationship_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryTransferRequest {
    pub source_label_id: String,
    pub target_label_id: String,
    pub created_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryTransferRow {
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub relationship_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLabelMemoryTransferOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_label_id: String,
    pub target_label_id: String,
    pub source_label_node_id: Option<u64>,
    pub target_label_node_id: Option<u64>,
    pub found_source_label: bool,
    pub found_target_label: bool,
    pub rows: Vec<KnowledgeLabelMemoryTransferRow>,
    pub matched_memory_count: usize,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub non_writable_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelTransferRequest {
    pub older_memory_id: String,
    pub newer_memory_id: String,
    pub space_id: String,
    pub created_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelTransferRow {
    pub label_id: Option<String>,
    pub label_node_id: u64,
    pub relationship_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLabelTransferOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub older_memory_id: String,
    pub newer_memory_id: String,
    pub space_id: String,
    pub older_memory_node_id: Option<u64>,
    pub newer_memory_node_id: Option<u64>,
    pub found_older_memory: bool,
    pub found_newer_memory: bool,
    pub older_space_matches: bool,
    pub newer_space_matches: bool,
    pub rows: Vec<KnowledgeMemoryLabelTransferRow>,
    pub matched_label_count: usize,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub non_writable_count: usize,
    pub duplicate_source_edge_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelListRequest {
    pub entity_label: String,
    pub external_ids: Vec<String>,
    pub limit_per_entity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelRow {
    pub label_id: Option<String>,
    pub node_id: u64,
    pub name: Option<String>,
    pub canonical_name: Option<String>,
    pub color: Option<Value>,
    pub description: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelGroup {
    pub external_id: String,
    pub node_id: Option<u64>,
    pub found: bool,
    pub labels: Vec<KnowledgeEntityLabelRow>,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelListOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeEntityLabelGroup>,
    pub found_entity_count: usize,
    pub missing_entity_count: usize,
    pub label_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedListRequest {
    pub list: KnowledgeEntityLabelListRequest,
    pub label_property_names: Vec<String>,
    pub relationship_property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedRow {
    pub label_id: Option<String>,
    pub label_node_id: u64,
    pub relationship_id: u64,
    pub label_properties: BTreeMap<String, Value>,
    pub relationship_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedGroup {
    pub external_id: String,
    pub node_id: Option<u64>,
    pub found: bool,
    pub labels: Vec<KnowledgeEntityLabelProjectedRow>,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityLabelProjectedListOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeEntityLabelProjectedGroup>,
    pub found_entity_count: usize,
    pub missing_entity_count: usize,
    pub label_count: usize,
}
