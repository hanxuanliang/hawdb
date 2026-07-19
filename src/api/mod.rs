use crate::analytics::ProjectedGraph;
use crate::cypher;
use crate::error::{Result, SkeinError};
use crate::executor::{self, Row};
use crate::optimizer::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
    OptimizerConfig, OptimizerTrace, PhysicalPlan,
};
use crate::planner;
use crate::qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy,
    LocalQosScheduler, LocalQosState, QosAdmission, QosAdmissionCode, WorkClass, WorkPriority,
    WorkRequest,
};
use crate::schema::{
    Catalog, CompositeIndexDescriptor, ConstraintDescriptor, GraphStatistics, IndexDescriptor,
    IndexKind, LabelId, PropertyDescriptor, SchemaObjectState, TableDescriptor,
};
use crate::search::{
    projection_row_from_node, MetadataRepairOptions, MetadataRepairSummary,
    SearchCandidateSetReport, SearchDerivedArtifactReport, SearchEmptyReasonCode,
    SearchFallbackReasonCode, SearchFusionWeights, SearchIndex, SearchMatchedSpan, SearchMode,
    SearchProjectionDelta, SearchProjectionDeltaReport, SearchProjectionFreshness,
    SearchQueryOptions, SearchRebuildOptions, SearchRebuildSummary, SearchResultSet,
    SearchRetrieverCandidateSetReport, SearchTruncationReasonCode,
};
use crate::store::{
    AdjacencyDirection, AdjacencyLayout, DurabilityPolicy, GraphMutation, GraphStore, NodeId,
    NodeRecord, ProjectedGraphStatus, PropertyIndexProjectionRebuildAction, RecoveryMode,
    RelRecord, SchemaMaintenanceAction, StorageReclamationWatermark, StorageRecoveryReport,
    StoreStableIdMapping, WalReplayConfig,
};
use crate::value::Value;
use plan_cache::{CachedPlan, PlanCache, PlanCacheKey, DEFAULT_PLAN_CACHE_MAX_ENTRIES};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::rc::Rc;
use std::str::FromStr;

mod artifact_jobs;
mod plan_cache;

const DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_ENTRIES: usize = 4096;

pub use artifact_jobs::{
    DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest,
};
pub use plan_cache::PlanCacheStats;

#[derive(Debug)]
pub struct Database {
    catalog: Catalog,
    store: GraphStore,
    optimizer: CascadesOptimizer,
    plan_cache: RefCell<PlanCache>,
    config: DatabaseConfig,
    reader_pins: Rc<RefCell<ReaderPins>>,
    next_derived_artifact_job_id: u64,
    derived_artifact_jobs: Vec<DerivedArtifactJob>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub read_only: bool,
    pub max_read_result_rows: Option<usize>,
    pub max_optimizer_groups: Option<usize>,
    pub recovery_mode: RecoveryMode,
    pub max_wal_replay_entries: Option<usize>,
    pub max_search_projection_change_log_entries: Option<usize>,
    pub max_plan_cache_entries: Option<usize>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            max_read_result_rows: None,
            max_optimizer_groups: None,
            recovery_mode: RecoveryMode::default(),
            max_wal_replay_entries: None,
            max_search_projection_change_log_entries: Some(
                DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_ENTRIES,
            ),
            max_plan_cache_entries: Some(DEFAULT_PLAN_CACHE_MAX_ENTRIES),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput {
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphStatement {
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeGraphExplainOutput {
    pub plan: String,
    pub trace: OptimizerTrace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphTransactionOutput {
    pub statement_outputs: Vec<QueryOutput>,
    pub commit_output: QueryOutput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalGraphSnapshotExport {
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub stable_identity: CanonicalSnapshotIdentityAudit,
    pub nodes: Vec<CanonicalSnapshotNode>,
    pub relationships: Vec<CanonicalSnapshotRelationship>,
}

impl CanonicalGraphSnapshotExport {
    pub fn with_stable_id_mapping(&self, mapping: &CanonicalStableIdMapping) -> Self {
        let mut export = self.clone();
        for node in &mut export.nodes {
            if node.stable_id.is_none() {
                node.stable_id = mapping.node_stable_ids.get(&node.node_id).cloned();
            }
        }
        for relationship in &mut export.relationships {
            if relationship.stable_id.is_none() {
                relationship.stable_id = mapping
                    .relationship_stable_ids
                    .get(&relationship.relationship_id)
                    .cloned();
            }
        }
        export.stable_identity =
            canonical_snapshot_identity_audit(&export.nodes, &export.relationships);
        export.logical_checksum =
            canonical_graph_snapshot_checksum(&export.nodes, &export.relationships);
        export
    }

    pub fn graph_lightning_bootstrap_manifest(&self) -> GraphLightningBootstrapManifest {
        let validation = self.validate();
        let graph_stream_body = encode_graph_lightning_graph_stream_body(self);
        let graph_stream_checksum = checksum_bytes(graph_stream_body.as_bytes());
        let graph_stream_byte_len =
            graph_stream_body.len() + format!("checksum\t{graph_stream_checksum}\n").len();
        GraphLightningBootstrapManifest {
            protocol_version: GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
            graph_commit_epoch: self.graph_commit_epoch,
            logical_checksum: self.logical_checksum,
            graph_stream_checksum,
            graph_stream_byte_len,
            schema_checksum: canonical_graph_snapshot_schema_checksum(
                &self.nodes,
                &self.relationships,
            ),
            node_count: self.nodes.len(),
            relationship_count: self.relationships.len(),
            label_count: self
                .nodes
                .iter()
                .flat_map(|node| node.labels.iter().cloned())
                .collect::<BTreeSet<_>>()
                .len(),
            relationship_type_count: self
                .relationships
                .iter()
                .map(|relationship| relationship.rel_type.clone())
                .collect::<BTreeSet<_>>()
                .len(),
            node_property_count: self.nodes.iter().map(|node| node.properties.len()).sum(),
            relationship_property_count: self
                .relationships
                .iter()
                .map(|relationship| relationship.properties.len())
                .sum(),
            validation,
        }
    }

    pub fn graph_lightning_graph_stream(&self) -> GraphLightningGraphStream {
        let body = encode_graph_lightning_graph_stream_body(self);
        let stream_checksum = checksum_bytes(body.as_bytes());
        let encoded = format!("{body}checksum\t{stream_checksum}\n");
        GraphLightningGraphStream {
            format_version: GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
            graph_commit_epoch: self.graph_commit_epoch,
            logical_checksum: self.logical_checksum,
            stream_checksum,
            byte_len: encoded.len(),
            node_count: self.nodes.len(),
            relationship_count: self.relationships.len(),
            encoded,
        }
    }

    pub fn validate(&self) -> CanonicalGraphSnapshotValidation {
        let expected_logical_checksum =
            canonical_graph_snapshot_checksum(&self.nodes, &self.relationships);
        let expected_stable_identity =
            canonical_snapshot_identity_audit(&self.nodes, &self.relationships);
        let duplicate_node_ids = duplicate_u64s(self.nodes.iter().map(|node| node.node_id));
        let duplicate_relationship_ids = duplicate_u64s(
            self.relationships
                .iter()
                .map(|relationship| relationship.relationship_id),
        );
        let node_ids = self
            .nodes
            .iter()
            .map(|node| node.node_id)
            .collect::<BTreeSet<_>>();
        let missing_sources = self
            .relationships
            .iter()
            .filter(|relationship| !node_ids.contains(&relationship.source_node_id))
            .map(|relationship| CanonicalSnapshotEndpointViolation {
                relationship_id: relationship.relationship_id,
                missing_node_id: relationship.source_node_id,
            })
            .collect::<Vec<_>>();
        let missing_targets = self
            .relationships
            .iter()
            .filter(|relationship| !node_ids.contains(&relationship.target_node_id))
            .map(|relationship| CanonicalSnapshotEndpointViolation {
                relationship_id: relationship.relationship_id,
                missing_node_id: relationship.target_node_id,
            })
            .collect::<Vec<_>>();
        let checksum_matches = self.logical_checksum == expected_logical_checksum;
        let stable_identity_matches = self.stable_identity == expected_stable_identity;
        let stable_identity_ready = !expected_stable_identity.requires_stable_id_mapping;
        let is_valid = checksum_matches
            && stable_identity_matches
            && duplicate_node_ids.is_empty()
            && duplicate_relationship_ids.is_empty()
            && missing_sources.is_empty()
            && missing_targets.is_empty();
        let is_import_ready = is_valid && stable_identity_ready;
        CanonicalGraphSnapshotValidation {
            is_valid,
            is_import_ready,
            checksum_matches,
            expected_logical_checksum,
            stable_identity_matches,
            stable_identity_ready,
            expected_stable_identity,
            duplicate_node_ids,
            duplicate_relationship_ids,
            missing_sources,
            missing_targets,
        }
    }
}

impl GraphLightningGraphStream {
    pub fn validate_against_manifest(
        &self,
        manifest: &GraphLightningBootstrapManifest,
    ) -> GraphLightningGraphStreamValidation {
        validate_graph_lightning_graph_stream(&self.encoded, Some(manifest))
    }
}

pub const GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION: u64 = 1;
pub const GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningBootstrapExport {
    pub snapshot: CanonicalGraphSnapshotExport,
    pub manifest: GraphLightningBootstrapManifest,
    pub graph_stream: GraphLightningGraphStream,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningBootstrapManifest {
    pub protocol_version: u64,
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub graph_stream_checksum: u64,
    pub graph_stream_byte_len: usize,
    pub schema_checksum: u64,
    pub node_count: usize,
    pub relationship_count: usize,
    pub label_count: usize,
    pub relationship_type_count: usize,
    pub node_property_count: usize,
    pub relationship_property_count: usize,
    pub validation: CanonicalGraphSnapshotValidation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningGraphStream {
    pub format_version: u64,
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub stream_checksum: u64,
    pub byte_len: usize,
    pub node_count: usize,
    pub relationship_count: usize,
    pub encoded: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningGraphStreamValidation {
    pub is_valid: bool,
    pub checksum_matches: bool,
    pub format_version_matches: bool,
    pub count_matches: bool,
    pub endpoint_integrity: bool,
    pub manifest_matches: bool,
    pub expected_stream_checksum: Option<u64>,
    pub actual_stream_checksum: u64,
    pub format_version: Option<u64>,
    pub graph_commit_epoch: Option<u64>,
    pub logical_checksum: Option<u64>,
    pub node_count: usize,
    pub relationship_count: usize,
    pub duplicate_node_ids: Vec<u64>,
    pub duplicate_relationship_ids: Vec<u64>,
    pub missing_sources: Vec<CanonicalSnapshotEndpointViolation>,
    pub missing_targets: Vec<CanonicalSnapshotEndpointViolation>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalGraphSnapshotValidation {
    pub is_valid: bool,
    pub is_import_ready: bool,
    pub checksum_matches: bool,
    pub expected_logical_checksum: u64,
    pub stable_identity_matches: bool,
    pub stable_identity_ready: bool,
    pub expected_stable_identity: CanonicalSnapshotIdentityAudit,
    pub duplicate_node_ids: Vec<u64>,
    pub duplicate_relationship_ids: Vec<u64>,
    pub missing_sources: Vec<CanonicalSnapshotEndpointViolation>,
    pub missing_targets: Vec<CanonicalSnapshotEndpointViolation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CanonicalStableIdMapping {
    pub node_stable_ids: BTreeMap<u64, Value>,
    pub relationship_stable_ids: BTreeMap<u64, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotEndpointViolation {
    pub relationship_id: u64,
    pub missing_node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotIdentityAudit {
    pub requires_stable_id_mapping: bool,
    pub nodes_without_stable_id: Vec<u64>,
    pub relationships_without_stable_id: Vec<u64>,
    pub duplicate_node_stable_ids: Vec<Value>,
    pub duplicate_relationship_stable_ids: Vec<Value>,
}

impl From<StoreStableIdMapping> for CanonicalStableIdMapping {
    fn from(mapping: StoreStableIdMapping) -> Self {
        Self {
            node_stable_ids: mapping
                .node_stable_ids
                .into_iter()
                .map(|(id, stable_id)| (id.0, stable_id))
                .collect(),
            relationship_stable_ids: mapping
                .relationship_stable_ids
                .into_iter()
                .map(|(id, stable_id)| (id.0, stable_id))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotNode {
    pub node_id: u64,
    pub stable_id: Option<Value>,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotRelationship {
    pub relationship_id: u64,
    pub stable_id: Option<Value>,
    pub source_node_id: u64,
    pub target_node_id: u64,
    pub rel_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrievalRequest {
    pub query_text: String,
    pub query_embedding: Option<Vec<f32>>,
    pub mode: SearchMode,
    pub limit: usize,
    pub rank_window: Option<usize>,
    pub search_fusion_weights: SearchFusionWeights,
    pub metadata_filters: BTreeMap<String, String>,
    pub candidate_limit: Option<usize>,
    pub candidate_scoring: KnowledgeCandidateScoringPolicy,
    pub graph_seed_limit: usize,
    pub graph_context_limit: usize,
    pub graph_context_max_hops: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchProjectionGraphDeltaRequest {
    pub upsert_node_ids: Vec<u64>,
    pub delete_document_ids: Vec<String>,
    pub max_operations: Option<usize>,
    pub complete_through_graph_commit_epoch: Option<u64>,
}

impl SearchProjectionGraphDeltaRequest {
    pub fn operation_count(&self) -> usize {
        self.upsert_node_ids.len() + self.delete_document_ids.len()
    }

    fn background_work_request(&self) -> WorkRequest {
        WorkRequest::background(WorkClass::Projection, self.operation_count())
    }

    pub fn background_work_plan(&self, hint: BackgroundWorkHint) -> Option<BackgroundWorkPlan> {
        let operation_count = self.operation_count();
        if operation_count == 0 {
            return None;
        }
        if self
            .max_operations
            .is_some_and(|limit| operation_count > limit)
        {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            operation_count,
            hint,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceOptions {
    pub hint: BackgroundWorkHint,
    pub include_schema_maintenance: bool,
    pub include_property_index_projection: bool,
    pub include_search_projection_graph_delta_freshness: bool,
    pub include_search_projection_rebuild: bool,
    pub include_search_projection_metadata_repair: bool,
    pub include_graph_lightning_bootstrap_export: bool,
    pub include_external_content_artifact_jobs: bool,
    pub external_content_artifact_estimated_operations: usize,
    pub search_projection_graph_delta: Option<SearchProjectionGraphDeltaRequest>,
}

impl Default for BackgroundMaintenanceOptions {
    fn default() -> Self {
        Self {
            hint: BackgroundWorkHint::default(),
            include_schema_maintenance: true,
            include_property_index_projection: true,
            include_search_projection_graph_delta_freshness: true,
            include_search_projection_rebuild: true,
            include_search_projection_metadata_repair: true,
            include_graph_lightning_bootstrap_export: true,
            include_external_content_artifact_jobs: true,
            external_content_artifact_estimated_operations: 1,
            search_projection_graph_delta: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceCandidate {
    pub kind: BackgroundMaintenanceKind,
    pub name: String,
    pub plan: BackgroundWorkPlan,
    pub search_projection_graph_delta: Option<SearchProjectionGraphDeltaRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedBackgroundMaintenance {
    pub kind: BackgroundMaintenanceKind,
    pub name: String,
    pub plan: BackgroundWorkPlan,
    pub decision: BackgroundWorkDecision,
    pub search_projection_graph_delta: Option<SearchProjectionGraphDeltaRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackgroundMaintenanceSummary {
    pub total_candidates: usize,
    pub admitted_count: usize,
    pub deferred_count: usize,
    pub rejected_count: usize,
    pub total_estimated_operations: usize,
    pub admitted_estimated_operations: usize,
    pub deferred_estimated_operations: usize,
    pub rejected_estimated_operations: usize,
    pub top_admitted_kind: Option<BackgroundMaintenanceKind>,
    pub top_admitted_name: Option<String>,
    pub ranked: Vec<BackgroundMaintenanceSummaryItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceSummaryItem {
    pub kind: BackgroundMaintenanceKind,
    pub name: String,
    pub work_class: WorkClass,
    pub work_class_name: String,
    pub priority: WorkPriority,
    pub priority_name: String,
    pub estimated_operations: usize,
    pub hint_active_topic: bool,
    pub hint_recent_delta_operations: usize,
    pub hint_source_graph_commit_lag: u64,
    pub hint_query_probability_per_million: u32,
    pub hint_staleness_millis: u64,
    pub hint_staleness_ttl_millis: Option<u64>,
    pub hint_freshness_slo_millis: Option<u64>,
    pub hint_tenant_budget_remaining_operations: Option<usize>,
    pub admission: QosAdmission,
    pub admission_name: String,
    pub admission_code: Option<QosAdmissionCode>,
    pub admission_code_name: Option<String>,
    pub score: u64,
    pub reason_code_names: Vec<String>,
    pub reasons: Vec<String>,
    pub has_executable_search_projection_graph_delta: bool,
    pub search_projection_graph_delta_operation_count: Option<usize>,
    pub search_projection_graph_delta_upsert_node_count: Option<usize>,
    pub search_projection_graph_delta_delete_document_count: Option<usize>,
    pub search_projection_graph_delta_complete_through_graph_commit_epoch: Option<u64>,
    pub search_projection_graph_delta_max_operations: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundMaintenanceKind {
    SchemaMaintenance,
    PropertyIndexProjection,
    SearchProjectionGraphDelta,
    SearchProjectionRebuild,
    SearchProjectionMetadataRepair,
    GraphLightningBootstrapExport,
    ExternalContentArtifactJob,
}

impl BackgroundMaintenanceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            BackgroundMaintenanceKind::SchemaMaintenance => "schema_maintenance",
            BackgroundMaintenanceKind::PropertyIndexProjection => "property_index_projection",
            BackgroundMaintenanceKind::SearchProjectionGraphDelta => {
                "search_projection_graph_delta"
            }
            BackgroundMaintenanceKind::SearchProjectionRebuild => "search_projection_rebuild",
            BackgroundMaintenanceKind::SearchProjectionMetadataRepair => {
                "search_projection_metadata_repair"
            }
            BackgroundMaintenanceKind::GraphLightningBootstrapExport => {
                "graph_lightning_bootstrap_export"
            }
            BackgroundMaintenanceKind::ExternalContentArtifactJob => {
                "external_content_artifact_job"
            }
        }
    }
}

impl FromStr for BackgroundMaintenanceKind {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "schema_maintenance" => Ok(BackgroundMaintenanceKind::SchemaMaintenance),
            "property_index_projection" => Ok(BackgroundMaintenanceKind::PropertyIndexProjection),
            "search_projection_graph_delta" => {
                Ok(BackgroundMaintenanceKind::SearchProjectionGraphDelta)
            }
            "search_projection_rebuild" => Ok(BackgroundMaintenanceKind::SearchProjectionRebuild),
            "search_projection_metadata_repair" => {
                Ok(BackgroundMaintenanceKind::SearchProjectionMetadataRepair)
            }
            "graph_lightning_bootstrap_export" => {
                Ok(BackgroundMaintenanceKind::GraphLightningBootstrapExport)
            }
            "external_content_artifact_job" => {
                Ok(BackgroundMaintenanceKind::ExternalContentArtifactJob)
            }
            _ => Err("unknown background maintenance kind"),
        }
    }
}

impl BackgroundMaintenanceCandidate {
    pub fn new(kind: BackgroundMaintenanceKind, plan: BackgroundWorkPlan) -> Self {
        Self {
            kind,
            name: kind.as_str().to_string(),
            plan,
            search_projection_graph_delta: None,
        }
    }

    pub fn with_search_projection_graph_delta(
        mut self,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Self {
        self.search_projection_graph_delta = Some(request);
        self
    }
}

impl BackgroundMaintenanceSummary {
    fn from_ranked(ranked: Vec<RankedBackgroundMaintenance>) -> Self {
        let mut summary = Self {
            total_candidates: ranked.len(),
            ..Self::default()
        };

        for ranked_item in ranked {
            let item = BackgroundMaintenanceSummaryItem::from_ranked(ranked_item);
            summary.total_estimated_operations = summary
                .total_estimated_operations
                .saturating_add(item.estimated_operations);
            match item.admission {
                QosAdmission::Admit => {
                    summary.admitted_count += 1;
                    summary.admitted_estimated_operations = summary
                        .admitted_estimated_operations
                        .saturating_add(item.estimated_operations);
                    if summary.top_admitted_kind.is_none() {
                        summary.top_admitted_kind = Some(item.kind);
                        summary.top_admitted_name = Some(item.name.clone());
                    }
                }
                QosAdmission::Defer { .. } => {
                    summary.deferred_count += 1;
                    summary.deferred_estimated_operations = summary
                        .deferred_estimated_operations
                        .saturating_add(item.estimated_operations);
                }
                QosAdmission::Reject { .. } => {
                    summary.rejected_count += 1;
                    summary.rejected_estimated_operations = summary
                        .rejected_estimated_operations
                        .saturating_add(item.estimated_operations);
                }
            }
            summary.ranked.push(item);
        }

        summary
    }
}

impl BackgroundMaintenanceSummaryItem {
    fn from_ranked(ranked: RankedBackgroundMaintenance) -> Self {
        let admission_code = ranked.decision.admission.code();
        let search_projection_graph_delta = ranked.search_projection_graph_delta.as_ref();
        Self {
            kind: ranked.kind,
            name: ranked.name,
            work_class: ranked.plan.request.class,
            work_class_name: ranked.plan.request.class.as_str().to_string(),
            priority: ranked.plan.request.priority,
            priority_name: ranked.plan.request.priority.as_str().to_string(),
            estimated_operations: ranked.plan.request.estimated_operations,
            hint_active_topic: ranked.plan.hint.active_topic,
            hint_recent_delta_operations: ranked.plan.hint.recent_delta_operations,
            hint_source_graph_commit_lag: ranked.plan.hint.source_graph_commit_lag,
            hint_query_probability_per_million: ranked.plan.hint.query_probability_per_million,
            hint_staleness_millis: ranked.plan.hint.staleness_millis,
            hint_staleness_ttl_millis: ranked.plan.hint.staleness_ttl_millis,
            hint_freshness_slo_millis: ranked.plan.hint.freshness_slo_millis,
            hint_tenant_budget_remaining_operations: ranked
                .plan
                .hint
                .tenant_budget_remaining_operations,
            admission_name: qos_admission_name(&ranked.decision.admission).to_string(),
            admission_code,
            admission_code_name: admission_code.map(|code| code.as_str().to_string()),
            admission: ranked.decision.admission,
            score: ranked.decision.score,
            reason_code_names: ranked
                .decision
                .reason_codes
                .iter()
                .map(|code| code.as_str().to_string())
                .collect(),
            reasons: ranked.decision.reasons,
            has_executable_search_projection_graph_delta: search_projection_graph_delta.is_some(),
            search_projection_graph_delta_operation_count: search_projection_graph_delta
                .map(SearchProjectionGraphDeltaRequest::operation_count),
            search_projection_graph_delta_upsert_node_count: search_projection_graph_delta
                .map(|request| request.upsert_node_ids.len()),
            search_projection_graph_delta_delete_document_count: search_projection_graph_delta
                .map(|request| request.delete_document_ids.len()),
            search_projection_graph_delta_complete_through_graph_commit_epoch:
                search_projection_graph_delta
                    .and_then(|request| request.complete_through_graph_commit_epoch),
            search_projection_graph_delta_max_operations: search_projection_graph_delta
                .and_then(|request| request.max_operations),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrievalOutput {
    pub graph_commit_epoch: u64,
    pub projection_freshness: SearchProjectionFreshness,
    pub search: SearchResultSet,
    pub retrievers: Vec<KnowledgeRetrieverReport>,
    pub diagnostics: KnowledgeRetrievalDiagnostics,
    pub candidates: Vec<KnowledgeCandidate>,
    pub evidence: Vec<KnowledgeEvidence>,
    pub graph_seeds: Vec<KnowledgeGraphSeed>,
    pub graph_context_paths: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrievalDiagnostics {
    pub graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_commit_lag: u64,
    pub projection_stale: bool,
    pub projection_full_reindex_needed: bool,
    pub projection_full_reindex_reasons: Vec<String>,
    pub projection_metadata_repair_needed: bool,
    pub projection_metadata_repair_reasons: Vec<String>,
    pub search_document_count: usize,
    pub search_filtered_document_count: usize,
    pub search_total_hits: usize,
    pub search_candidate_set: SearchCandidateSetReport,
    pub search_candidate_filtered_out_count: usize,
    pub search_limit: usize,
    pub search_truncated: bool,
    pub search_truncation_reason_codes: Vec<SearchTruncationReasonCode>,
    pub search_truncation_reasons: Vec<String>,
    pub search_fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub search_fallback_reasons: Vec<String>,
    pub rank_window: Option<usize>,
    pub search_fusion_weights: SearchFusionWeights,
    pub graph_seed_input_candidate_set: SearchCandidateSetReport,
    pub graph_seed_candidate_set: SearchRetrieverCandidateSetReport,
    pub graph_seed_candidate_count: usize,
    pub graph_seed_returned_count: usize,
    pub graph_seed_limit: usize,
    pub graph_seed_truncated: bool,
    pub graph_seed_truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub graph_seed_truncation_reasons: Vec<String>,
    pub graph_context_input_candidate_set: SearchCandidateSetReport,
    pub graph_context_candidate_set: SearchRetrieverCandidateSetReport,
    pub graph_context_path_count: usize,
    pub graph_context_node_count: usize,
    pub graph_context_relationship_count: usize,
    pub graph_context_limit: usize,
    pub graph_context_max_hops: usize,
    pub graph_context_truncated: bool,
    pub graph_context_truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub graph_context_truncation_reasons: Vec<String>,
    pub graph_context_fallback_reason_codes: Vec<KnowledgeFallbackReasonCode>,
    pub graph_context_fallback_reasons: Vec<String>,
    pub fanout_reason_count: usize,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub candidate_count: usize,
    pub candidate_total_count: usize,
    pub candidate_limit: Option<usize>,
    pub candidate_truncated: bool,
    pub candidate_truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub candidate_truncation_reasons: Vec<String>,
    pub warnings: Vec<String>,
    pub empty_reason_codes: Vec<KnowledgeRetrievalEmptyReasonCode>,
    pub empty_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeFanoutReasonDetail {
    pub code: KnowledgeFanoutReasonCode,
    pub message: String,
    pub operation: Option<String>,
    pub limit: Option<usize>,
    pub total: Option<usize>,
    pub seed_hit_id: Option<String>,
    pub node_id: Option<u64>,
    pub relationship_type: Option<String>,
    pub direction: Option<String>,
    pub degree: Option<usize>,
}

impl KnowledgeFanoutReasonDetail {
    fn dense_adjacency(
        operation: &str,
        relationship_type: &str,
        direction: &str,
        node_id: u64,
        degree: usize,
    ) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::DenseAdjacency,
            message: format!(
                "{operation} dense_adjacency {relationship_type} {direction} node {node_id} degree {degree}"
            ),
            operation: Some(operation.to_string()),
            limit: None,
            total: None,
            seed_hit_id: None,
            node_id: Some(node_id),
            relationship_type: Some(relationship_type.to_string()),
            direction: Some(direction.to_string()),
            degree: Some(degree),
        }
    }

    fn graph_context_limit(limit: usize, seed_hit_id: &str) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::GraphContextLimitReached,
            message: format!(
                "graph_context_limit {limit} reached while expanding hit {seed_hit_id}"
            ),
            operation: Some("graph_context".to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: Some(seed_hit_id.to_string()),
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    fn graph_seed_limit(limit: usize, total: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::GraphSeedLimitReached,
            message: format!(
                "knowledge_graph_seed_limit {limit} returned from {total} matching graph seeds"
            ),
            operation: Some("graph_seed".to_string()),
            limit: Some(limit),
            total: Some(total),
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    fn candidate_limit(limit: usize, total: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::CandidateLimitReached,
            message: format!(
                "knowledge_candidate_limit {limit} returned from {total} merged candidates"
            ),
            operation: Some("candidate".to_string()),
            limit: Some(limit),
            total: Some(total),
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    fn path_limit(operation: &str, limit: usize, target: &str) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::PathLimitReached,
            message: format!("{operation} limit {limit} reached while expanding {target}"),
            operation: Some(operation.to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    fn node_limit(limit: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::NodeLimitReached,
            message: format!("knowledge_subgraph node_limit {limit} reached"),
            operation: Some("knowledge_subgraph".to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    fn relationship_limit(limit: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::RelationshipLimitReached,
            message: format!("knowledge_subgraph relationship_limit {limit} reached"),
            operation: Some("knowledge_subgraph".to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeFanoutReasonCode {
    DenseAdjacency,
    GraphContextLimitReached,
    GraphSeedLimitReached,
    CandidateLimitReached,
    PathLimitReached,
    NodeLimitReached,
    RelationshipLimitReached,
}

impl KnowledgeFanoutReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DenseAdjacency => "dense_adjacency",
            Self::GraphContextLimitReached => "graph_context_limit_reached",
            Self::GraphSeedLimitReached => "graph_seed_limit_reached",
            Self::CandidateLimitReached => "candidate_limit_reached",
            Self::PathLimitReached => "path_limit_reached",
            Self::NodeLimitReached => "node_limit_reached",
            Self::RelationshipLimitReached => "relationship_limit_reached",
        }
    }
}

impl FromStr for KnowledgeFanoutReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "dense_adjacency" => Ok(Self::DenseAdjacency),
            "graph_context_limit_reached" => Ok(Self::GraphContextLimitReached),
            "graph_seed_limit_reached" => Ok(Self::GraphSeedLimitReached),
            "candidate_limit_reached" => Ok(Self::CandidateLimitReached),
            "path_limit_reached" => Ok(Self::PathLimitReached),
            "node_limit_reached" => Ok(Self::NodeLimitReached),
            "relationship_limit_reached" => Ok(Self::RelationshipLimitReached),
            _ => Err("unknown knowledge fanout reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeFallbackReasonCode {
    GraphSeedLimitZero,
    GraphContextLimitZero,
    GraphContextMaxHopsZero,
}

impl KnowledgeFallbackReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GraphSeedLimitZero => "graph_seed_limit_zero",
            Self::GraphContextLimitZero => "graph_context_limit_zero",
            Self::GraphContextMaxHopsZero => "graph_context_max_hops_zero",
        }
    }
}

impl FromStr for KnowledgeFallbackReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "graph_seed_limit_zero" => Ok(Self::GraphSeedLimitZero),
            "graph_context_limit_zero" => Ok(Self::GraphContextLimitZero),
            "graph_context_max_hops_zero" => Ok(Self::GraphContextMaxHopsZero),
            _ => Err("unknown knowledge fallback reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeTruncationReasonCode {
    RankWindowExceeded,
    SearchLimitExceeded,
    PartialCandidateReturn,
    GraphSeedLimitExceeded,
    GraphContextLimitExceeded,
    CandidateLimitExceeded,
}

impl KnowledgeTruncationReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RankWindowExceeded => "rank_window_exceeded",
            Self::SearchLimitExceeded => "search_limit_exceeded",
            Self::PartialCandidateReturn => "partial_candidate_return",
            Self::GraphSeedLimitExceeded => "graph_seed_limit_exceeded",
            Self::GraphContextLimitExceeded => "graph_context_limit_exceeded",
            Self::CandidateLimitExceeded => "candidate_limit_exceeded",
        }
    }
}

impl FromStr for KnowledgeTruncationReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "rank_window_exceeded" => Ok(Self::RankWindowExceeded),
            "search_limit_exceeded" => Ok(Self::SearchLimitExceeded),
            "partial_candidate_return" => Ok(Self::PartialCandidateReturn),
            "graph_seed_limit_exceeded" => Ok(Self::GraphSeedLimitExceeded),
            "graph_context_limit_exceeded" => Ok(Self::GraphContextLimitExceeded),
            "candidate_limit_exceeded" => Ok(Self::CandidateLimitExceeded),
            _ => Err("unknown knowledge truncation reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeRetrievalEmptyReasonCode {
    SearchProjectionEmpty,
    SearchMetadataFilterEmpty,
    SearchRetrieverNoHits,
    SearchLimitExcludedAllHits,
    GraphSeedLimitZero,
    GraphSeedNoCandidates,
    CandidateLimitExcludedAllCandidates,
    NoCandidates,
}

impl KnowledgeRetrievalEmptyReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SearchProjectionEmpty => "search_projection_empty",
            Self::SearchMetadataFilterEmpty => "search_metadata_filter_empty",
            Self::SearchRetrieverNoHits => "search_retriever_no_hits",
            Self::SearchLimitExcludedAllHits => "search_limit_excluded_all_hits",
            Self::GraphSeedLimitZero => "graph_seed_limit_zero",
            Self::GraphSeedNoCandidates => "graph_seed_no_candidates",
            Self::CandidateLimitExcludedAllCandidates => "candidate_limit_excluded_all_candidates",
            Self::NoCandidates => "no_candidates",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrieverReport {
    pub name: String,
    pub available: bool,
    pub input_candidate_set: SearchCandidateSetReport,
    pub candidate_count: usize,
    pub candidate_set: SearchRetrieverCandidateSetReport,
    pub limit: Option<usize>,
    pub rank_window: Option<usize>,
    pub fusion_weight: Option<f64>,
    pub fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub knowledge_fallback_reason_codes: Vec<KnowledgeFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
    pub truncated: bool,
    pub truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub truncation_reasons: Vec<String>,
    pub top_candidates: Vec<KnowledgeRetrieverCandidate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrieverCandidate {
    pub id: String,
    pub kind: Option<String>,
    pub external_id: Option<String>,
    pub source_id: Option<String>,
    pub canonical_node_id: Option<u64>,
    pub rank: usize,
    pub score: f64,
    pub matched_spans: Vec<SearchMatchedSpan>,
    pub graph_context_path_count: usize,
    pub projection_freshness: Option<SearchProjectionFreshness>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCandidate {
    pub id: String,
    pub canonical_node_id: Option<u64>,
    pub source: KnowledgeCandidateSource,
    pub source_rank: usize,
    pub merged_sources: Vec<KnowledgeCandidateSource>,
    pub score: f64,
    pub score_breakdown: KnowledgeCandidateScoreBreakdown,
    pub entity: Option<KnowledgeEntity>,
    pub evidence: Option<KnowledgeEvidence>,
    pub matched_properties: Vec<String>,
    pub graph_context_path_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KnowledgeCandidateScoringPolicy {
    Max,
    WeightedSum {
        search_weight: f64,
        graph_seed_weight: f64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KnowledgeCandidateScoreBreakdown {
    pub search_score: Option<f64>,
    pub graph_seed_score: Option<f64>,
    pub combined_score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCandidateSource {
    SearchHit,
    GraphSeed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeGraphSeed {
    pub entity: KnowledgeEntity,
    pub score: f64,
    pub matched_properties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEvidence {
    pub hit_id: String,
    pub kind: Option<String>,
    pub external_id: Option<String>,
    pub source_id: Option<String>,
    pub canonical_node_id: Option<u64>,
    pub graph_context_path_count: usize,
    pub matched_terms: Vec<String>,
    pub matched_spans: Vec<SearchMatchedSpan>,
    pub score: f64,
    pub rrf_score: f64,
    pub vector_rrf_score: f64,
    pub text_rrf_score: f64,
    pub vector_score: f64,
    pub text_score: f64,
    pub vector_rank: Option<usize>,
    pub text_rank: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeNeighborsRequest {
    pub label: String,
    pub external_id: String,
    pub relationship_type: Option<String>,
    pub direction: KnowledgeNeighborDirection,
    pub limit: usize,
    pub max_hops: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedNeighborsRequest {
    pub navigation: KnowledgeNeighborsRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeNeighborsOutput {
    pub graph_commit_epoch: u64,
    pub seed_node_id: Option<u64>,
    pub paths: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipsRequest {
    pub seeds: Vec<KnowledgeEntityRequest>,
    pub relationship_type: Option<String>,
    pub direction: KnowledgeNeighborDirection,
    pub limit_per_seed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipsRequest {
    pub relationships: KnowledgeRelationshipsRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipGroup {
    pub seed: KnowledgeEntityRequest,
    pub seed_node_id: Option<u64>,
    pub filtered_out: bool,
    pub relationships: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipsOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeRelationshipGroup>,
    pub relationship_type_found: bool,
    pub found_seed_count: usize,
    pub missing_seed_count: usize,
    pub filtered_out_seed_count: usize,
    pub relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeInducedEdgeListRequest {
    pub external_ids: Vec<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeInducedEdgeRow {
    pub source_id: Option<String>,
    pub source_node_id: u64,
    pub target_id: Option<String>,
    pub target_node_id: u64,
    pub relationship_id: u64,
    pub relationship_type: String,
    pub strength: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeInducedEdgeListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeInducedEdgeRow>,
    pub matched_node_count: usize,
    pub missing_external_ids: Vec<String>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePathRequest {
    pub source_label: String,
    pub source_external_id: String,
    pub target_label: String,
    pub target_external_id: String,
    pub relationship_type: Option<String>,
    pub direction: KnowledgeNeighborDirection,
    pub max_hops: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedPathRequest {
    pub navigation: KnowledgePathRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePathOutput {
    pub graph_commit_epoch: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub paths: Vec<KnowledgeGraphPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphPath {
    pub segments: Vec<KnowledgeGraphContextPath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSubgraphRequest {
    pub label: String,
    pub external_id: String,
    pub relationship_type: Option<String>,
    pub direction: KnowledgeNeighborDirection,
    pub max_hops: usize,
    pub node_limit: usize,
    pub relationship_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedSubgraphRequest {
    pub navigation: KnowledgeSubgraphRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeSubgraphOutput {
    pub graph_commit_epoch: u64,
    pub seed_node_id: Option<u64>,
    pub nodes: Vec<KnowledgeEntity>,
    pub relationships: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeTraversalDiagnostics {
    pub seed_found: bool,
    pub target_found: Option<bool>,
    pub input_candidate_set: SearchCandidateSetReport,
    pub candidate_set: SearchRetrieverCandidateSetReport,
    pub path_count: usize,
    pub node_count: usize,
    pub relationship_count: usize,
    pub fanout_reason_count: usize,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub fallback_reason_codes: Vec<KnowledgeTraversalFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
    pub max_hops: usize,
    pub path_limit: Option<usize>,
    pub node_limit: Option<usize>,
    pub relationship_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeTraversalFallbackReasonCode {
    SeedNotFound,
    TargetNotFound,
    MaxHopsZero,
    PathLimitZero,
    NodeLimitZero,
    RelationshipLimitZero,
    RelationshipTypeNotFound,
}

impl KnowledgeTraversalFallbackReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SeedNotFound => "seed_not_found",
            Self::TargetNotFound => "target_not_found",
            Self::MaxHopsZero => "max_hops_zero",
            Self::PathLimitZero => "path_limit_zero",
            Self::NodeLimitZero => "node_limit_zero",
            Self::RelationshipLimitZero => "relationship_limit_zero",
            Self::RelationshipTypeNotFound => "relationship_type_not_found",
        }
    }
}

impl FromStr for KnowledgeTraversalFallbackReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "seed_not_found" => Ok(Self::SeedNotFound),
            "target_not_found" => Ok(Self::TargetNotFound),
            "max_hops_zero" => Ok(Self::MaxHopsZero),
            "path_limit_zero" => Ok(Self::PathLimitZero),
            "node_limit_zero" => Ok(Self::NodeLimitZero),
            "relationship_limit_zero" => Ok(Self::RelationshipLimitZero),
            "relationship_type_not_found" => Ok(Self::RelationshipTypeNotFound),
            _ => Err("unknown knowledge traversal fallback reason code"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityRequest {
    pub label: String,
    pub external_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityRequest {
    pub entity: KnowledgeEntityRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityBatchRequest {
    pub entities: Vec<KnowledgeEntityRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityBatchRequest {
    pub entities: Vec<KnowledgeEntityRequest>,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntityOutput {
    pub graph_commit_epoch: u64,
    pub entity: Option<KnowledgeEntity>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntityBatchOutput {
    pub graph_commit_epoch: u64,
    pub entities: Vec<Option<KnowledgeEntity>>,
    pub found_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEntityListRequest {
    pub memory_ids: Vec<String>,
    pub limit_per_memory: usize,
    pub distinct_name_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEntityRow {
    pub entity_id: Option<String>,
    pub node_id: u64,
    pub relationship_id: u64,
    pub name: Option<String>,
    pub entity_type: Option<String>,
    pub confidence: Option<Value>,
    pub relationship_confidence: Option<Value>,
    pub mention_count: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEntityGroup {
    pub memory_id: String,
    pub memory_node_id: Option<u64>,
    pub found: bool,
    pub entities: Vec<KnowledgeMemoryEntityRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryEntityListOutput {
    pub graph_commit_epoch: u64,
    pub groups: Vec<KnowledgeMemoryEntityGroup>,
    pub distinct_entity_names: Vec<String>,
    pub found_memory_count: usize,
    pub missing_memory_count: usize,
    pub entity_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeContextMemoryLatestFilter {
    NullOrTrue,
    TrueOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeContextMemoryPreviewRequest {
    pub unit_types: Vec<String>,
    pub latest_filter: KnowledgeContextMemoryLatestFilter,
    pub include_labels: bool,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeContextMemoryPreviewRow {
    pub memory_id: Option<String>,
    pub memory_node_id: u64,
    pub title: Option<String>,
    pub unit_type: Option<String>,
    pub created_at: Option<Value>,
    pub label_id: Option<String>,
    pub label_node_id: Option<u64>,
    pub label_canonical_name: Option<String>,
    pub label_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeContextMemoryPreviewOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeContextMemoryPreviewRow>,
    pub matched_memory_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeMemoryListOrder {
    ExternalIdAsc,
    CreatedAtDesc,
    ScoreDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryListRequest {
    pub external_ids: Vec<String>,
    pub normalized_space_id: Option<String>,
    pub exclude_normalized_space_id: Option<String>,
    pub unit_type: Option<String>,
    pub is_latest: Option<bool>,
    pub is_crystal: Option<bool>,
    pub limit: usize,
    pub order: KnowledgeMemoryListOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryListRow {
    pub memory_id: Option<String>,
    pub node_id: u64,
    pub title: Option<String>,
    pub content: Option<String>,
    pub metadata: Option<Value>,
    pub is_latest: Option<bool>,
    pub lifecycle_state: Option<String>,
    pub review_status: Option<String>,
    pub unit_type: Option<String>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: String,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub importance: Option<Value>,
    pub pagerank_score: Option<Value>,
    pub community_id: Option<Value>,
    pub source: Option<String>,
    pub event_start: Option<Value>,
    pub event_end: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeMemoryListRow>,
    pub matched_count: usize,
    pub returned_count: usize,
    pub missing_external_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityCreateRequest {
    pub label: String,
    pub external_id: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityCreateOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub node_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
    pub created_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityCreateBatchRequest {
    pub creates: Vec<KnowledgeEntityCreateRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityCreateBatchRow {
    pub label: String,
    pub external_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityCreateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeEntityCreateBatchRow>,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub created_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityUpsertRequest {
    pub label: String,
    pub external_id: String,
    pub create_properties: BTreeMap<String, Value>,
    pub update_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityUpsertOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub node_id: Option<u64>,
    pub created: bool,
    pub updated: bool,
    pub already_exists: bool,
    pub non_writable: bool,
    pub created_node_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityUpsertBatchRequest {
    pub upserts: Vec<KnowledgeEntityUpsertRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityUpsertBatchRow {
    pub label: String,
    pub external_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub updated: bool,
    pub already_exists: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityUpsertBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeEntityUpsertBatchRow>,
    pub created_count: usize,
    pub updated_count: usize,
    pub already_exists_count: usize,
    pub non_writable_count: usize,
    pub created_node_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePropertyBatchRequest {
    pub entities: Vec<KnowledgeEntityRequest>,
    pub property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedPropertyBatchRequest {
    pub projection: KnowledgePropertyBatchRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgePropertyRow {
    pub entity: KnowledgeEntityRequest,
    pub node_id: Option<u64>,
    pub filtered_out: bool,
    pub properties: BTreeMap<String, Option<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgePropertyBatchOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgePropertyRow>,
    pub found_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
    pub property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePropertyUpdateRequest {
    pub entity: KnowledgeEntityRequest,
    pub assignments: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedPropertyUpdateRequest {
    pub update: KnowledgePropertyUpdateRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePropertyUpdateOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub filtered_out: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePropertyUpdateBatchRequest {
    pub updates: Vec<KnowledgePropertyUpdateRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedPropertyUpdateBatchRequest {
    pub updates: Vec<KnowledgePropertyUpdateRequest>,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePropertyUpdateBatchRow {
    pub entity: KnowledgeEntityRequest,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub filtered_out: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePropertyUpdateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgePropertyUpdateBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
    pub non_writable_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeNormalizedSpaceMoveBatchRequest {
    pub label: String,
    pub identity_property: String,
    pub external_ids: Vec<String>,
    pub source_space_id: Option<String>,
    pub target_space_id: String,
    pub updated_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeNormalizedSpaceMoveBatchRow {
    pub external_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub moved: bool,
    pub source_mismatch: bool,
    pub already_in_target: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeNormalizedSpaceMoveBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeNormalizedSpaceMoveBatchRow>,
    pub moved_external_ids: Vec<String>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub source_mismatch_count: usize,
    pub already_in_target_count: usize,
    pub duplicate_count: usize,
    pub moved_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryAccessTouch {
    pub memory_id: String,
    pub accessed_at: Value,
    pub click_dwell_time_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryAccessBatchRequest {
    pub touches: Vec<KnowledgeMemoryAccessTouch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryAccessBatchRow {
    pub memory_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub touched: bool,
    pub clicked: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryAccessBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeMemoryAccessBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub touched_count: usize,
    pub click_touch_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceMemoryCountAdjustment {
    pub source_id: String,
    pub delta: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceMemoryCountBatchRequest {
    pub adjustments: Vec<KnowledgeSourceMemoryCountAdjustment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceMemoryCountBatchRow {
    pub source_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub adjusted: bool,
    pub non_writable: bool,
    pub invalid_current_count: bool,
    pub old_count: Option<i64>,
    pub new_count: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceMemoryCountBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSourceMemoryCountBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub invalid_current_count_count: usize,
    pub adjusted_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceLifecycleUpdate {
    pub source_id: String,
    pub current_lifecycle_state: Option<String>,
    pub lifecycle_state: String,
    pub chunk_count: Option<i64>,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceLifecycleBatchRequest {
    pub updates: Vec<KnowledgeSourceLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceLifecycleBatchRow {
    pub source_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub filtered_out: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSourceLifecycleBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceRequest {
    pub source_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KnowledgeSourceIdListRequest {
    pub lifecycle_state: Option<String>,
    pub normalized_space_id: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KnowledgeSourceListOrder {
    #[default]
    SourceIdAsc,
    MemoryCountDesc,
    CreatedAtDesc,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KnowledgeSourceListRequest {
    pub source_ids: Vec<String>,
    pub after_source_id: Option<String>,
    pub lifecycle_states: Vec<String>,
    pub normalized_space_id: Option<String>,
    pub source_type: Option<String>,
    pub metadata_contains: Option<String>,
    pub parsed_path_required: bool,
    pub limit: usize,
    pub offset: usize,
    pub order: KnowledgeSourceListOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceListRow {
    pub source_id: Option<String>,
    pub node_id: u64,
    pub display_name: String,
    pub original_name: Option<String>,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub source_type: Option<String>,
    pub lifecycle_state: Option<String>,
    pub raw_space_id: Option<String>,
    pub normalized_space_id: String,
    pub parsed_path: Option<String>,
    pub file_path: Option<String>,
    pub mime_type: Option<String>,
    pub source_url: Option<String>,
    pub metadata: Option<Value>,
    pub memory_count: i64,
    pub chunk_count: i64,
    pub size_bytes: i64,
    pub version: i64,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub sourced_memory_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeSourceListRow>,
    pub matched_count: usize,
    pub returned_count: usize,
    pub missing_source_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceRow {
    pub source_id: Option<String>,
    pub node_id: u64,
    pub original_name: Option<String>,
    pub title: Option<String>,
    pub source_type: Option<String>,
    pub lifecycle_state: Option<String>,
    pub normalized_space_id: String,
    pub parsed_path: Option<String>,
    pub file_path: Option<String>,
    pub mime_type: Option<String>,
    pub memory_count: Option<i64>,
    pub chunk_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub sourced_memory_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceOutput {
    pub graph_commit_epoch: u64,
    pub found: bool,
    pub row: Option<KnowledgeSourceRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceIdListOutput {
    pub graph_commit_epoch: u64,
    pub source_ids: Vec<String>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceCountOutput {
    pub graph_commit_epoch: u64,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceMemoryListRequest {
    pub source_id: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceMemoryRow {
    pub memory_id: Option<String>,
    pub node_id: u64,
    pub relationship_id: u64,
    pub title: Option<String>,
    pub content: Option<String>,
    pub unit_type: Option<String>,
    pub confidence: Option<Value>,
    pub chunk_index: Option<i64>,
    pub chunk_range: Option<String>,
    pub source_version: Option<String>,
    pub created_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceMemoryListOutput {
    pub graph_commit_epoch: u64,
    pub source_id: String,
    pub source_node_id: Option<u64>,
    pub found: bool,
    pub rows: Vec<KnowledgeSourceMemoryRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLifecycleUpdate {
    pub memory_id: String,
    pub metadata: Value,
    pub is_latest: bool,
    pub lifecycle_state: String,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLifecycleBatchRequest {
    pub updates: Vec<KnowledgeMemoryLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLifecycleBatchRow {
    pub memory_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeMemoryLifecycleBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLatestUpdate {
    pub memory_id: String,
    pub is_latest: bool,
    pub space_id_filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLatestBatchRequest {
    pub updates: Vec<KnowledgeMemoryLatestUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLatestBatchRow {
    pub memory_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub filtered_out: bool,
    pub duplicate: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeMemoryLatestBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeMemoryLatestBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgePageRankScoreUpdate {
    pub label: String,
    pub external_id: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgePageRankScoreBatchRequest {
    pub updates: Vec<KnowledgePageRankScoreUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankScoreBatchRow {
    pub label: String,
    pub external_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankScoreBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgePageRankScoreBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub duplicate_count: usize,
    pub non_writable_count: usize,
    pub updated_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankClearRequest {
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankClearRow {
    pub label: String,
    pub external_id: Option<String>,
    pub node_id: u64,
    pub cleared: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankClearOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgePageRankClearRow>,
    pub candidate_count: usize,
    pub cleared_count: usize,
    pub non_writable_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KnowledgePageRankPlanRequest {
    pub changed_since_epoch_nanos: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankPlanOutput {
    pub graph_commit_epoch: u64,
    pub memory_node_count: usize,
    pub entity_node_count: usize,
    pub entity_relation_count: usize,
    pub mention_edge_count: usize,
    pub active_memory_relation_count: usize,
    pub changed_memory_count: usize,
    pub changed_entity_count: usize,
    pub changed_mention_edge_count: usize,
    pub changed_entity_relation_count: usize,
    pub changed_memory_relation_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankMembershipRequest {
    pub label: String,
    pub external_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankMembershipRow {
    pub label: String,
    pub external_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankMembershipOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgePageRankMembershipRow>,
    pub matched_count: usize,
    pub missing_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankMemoryVisibilityRequest {
    pub memory_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankMemoryVisibilityRow {
    pub memory_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub metadata: Option<Value>,
    pub is_latest: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankMemoryVisibilityOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgePageRankMemoryVisibilityRow>,
    pub matched_count: usize,
    pub missing_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankCentralEntityRequest {
    pub entity_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgePageRankCentralEntityOutput {
    pub graph_commit_epoch: u64,
    pub found: bool,
    pub node_id: Option<u64>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityAssignmentClearRequest {
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityAssignmentClearRow {
    pub labels: Vec<String>,
    pub external_id: Option<String>,
    pub node_id: u64,
    pub cleared: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityAssignmentClearOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeCommunityAssignmentClearRow>,
    pub candidate_count: usize,
    pub cleared_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityMembershipCreate {
    pub entity_id: String,
    pub community_id: String,
    pub strength: f64,
    pub created_at: Value,
    pub properties: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityMembershipCreateBatchRequest {
    pub memberships: Vec<KnowledgeCommunityMembershipCreate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityMembershipCreateBatchRow {
    pub entity_id: String,
    pub community_id: String,
    pub entity_node_id: Option<u64>,
    pub community_node_id: Option<u64>,
    pub matched: bool,
    pub non_writable: bool,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityMembershipCreateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeCommunityMembershipCreateBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub non_writable_count: usize,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityCreate {
    pub id: String,
    pub community_id: i64,
    pub name: String,
    pub description: Value,
    pub ai_summary: Value,
    pub member_count: i64,
    pub resolution: f64,
    pub created_at: Value,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunitySummaryUpdate {
    pub id: String,
    pub name: String,
    pub description: Value,
    pub ai_summary: Value,
    pub updated_at: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCommunityLifecycleBatchRequest {
    pub creates: Vec<KnowledgeCommunityCreate>,
    pub summary_updates: Vec<KnowledgeCommunitySummaryUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCreateBatchRow {
    pub id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub already_exists: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunitySummaryUpdateBatchRow {
    pub id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub updated: bool,
    pub missing: bool,
    pub duplicate: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub create_rows: Vec<KnowledgeCommunityCreateBatchRow>,
    pub summary_update_rows: Vec<KnowledgeCommunitySummaryUpdateBatchRow>,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub duplicate_count: usize,
    pub updated_count: usize,
    pub missing_count: usize,
    pub non_writable_count: usize,
    pub created_node_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCleanupRequest {
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCleanupRow {
    pub id: Option<String>,
    pub node_id: u64,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCommunityCleanupOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeCommunityCleanupRow>,
    pub candidate_count: usize,
    pub deleted_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStamp {
    pub meta_id: String,
    pub assignments: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStampBatchRequest {
    pub stamps: Vec<KnowledgeGraphMetaStamp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStampBatchRow {
    pub meta_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub updated: bool,
    pub duplicate: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaStampBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeGraphMetaStampBatchRow>,
    pub created_count: usize,
    pub updated_count: usize,
    pub duplicate_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaRequest {
    pub meta_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMeta {
    pub meta_id: Option<String>,
    pub node_id: u64,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaOutput {
    pub graph_commit_epoch: u64,
    pub found: bool,
    pub meta: Option<KnowledgeGraphMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphMetaDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApply {
    pub migration_id: String,
    pub applied_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApplyBatchRequest {
    pub migrations: Vec<KnowledgeSchemaMigrationApply>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApplyBatchRow {
    pub migration_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub already_applied: bool,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationApplyBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSchemaMigrationApplyBatchRow>,
    pub created_count: usize,
    pub already_applied_count: usize,
    pub duplicate_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KnowledgeSchemaMigrationListRequest {
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationRow {
    pub migration_id: String,
    pub node_id: u64,
    pub applied_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSchemaMigrationListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeSchemaMigrationRow>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum KnowledgeAugmentationJobLifecycleTransition {
    Create {
        job_type: String,
        parameters: Value,
        created_at: Value,
    },
    MarkRunning {
        started_at: Value,
    },
    UpdateProgress {
        progress: f64,
        message: String,
    },
    MarkCompleted {
        result: Value,
        completed_at: Value,
    },
    MarkFailed {
        error_message: String,
        completed_at: Value,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobLifecycleUpdate {
    pub job_id: String,
    pub transition: KnowledgeAugmentationJobLifecycleTransition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobLifecycleBatchRequest {
    pub updates: Vec<KnowledgeAugmentationJobLifecycleUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobLifecycleBatchRow {
    pub job_id: String,
    pub node_id: Option<u64>,
    pub created: bool,
    pub updated: bool,
    pub missing: bool,
    pub already_exists: bool,
    pub status_mismatch: bool,
    pub duplicate: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobLifecycleBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeAugmentationJobLifecycleBatchRow>,
    pub created_count: usize,
    pub updated_count: usize,
    pub missing_count: usize,
    pub already_exists_count: usize,
    pub status_mismatch_count: usize,
    pub duplicate_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobInterruptRequest {
    pub error_message: String,
    pub completed_at: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobInterruptRow {
    pub job_id: Option<String>,
    pub node_id: u64,
    pub previous_status: String,
    pub interrupted: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobInterruptOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeAugmentationJobInterruptRow>,
    pub candidate_count: usize,
    pub interrupted_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobRequest {
    pub job_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnowledgeAugmentationJobListOrder {
    StartedAtDesc,
    CreatedAtDesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeAugmentationJobListRequest {
    pub status_filter: Option<String>,
    pub order_by: KnowledgeAugmentationJobListOrder,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJob {
    pub job_id: Option<String>,
    pub node_id: u64,
    pub job_type: Option<String>,
    pub status: Option<String>,
    pub progress: Option<f64>,
    pub message: Option<String>,
    pub result: Option<Value>,
    pub error_message: Option<String>,
    pub started_at: Option<Value>,
    pub completed_at: Option<Value>,
    pub created_at: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobOutput {
    pub graph_commit_epoch: u64,
    pub found: bool,
    pub job: Option<KnowledgeAugmentationJob>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeAugmentationJobListOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeAugmentationJob>,
    pub matched_count: usize,
    pub returned_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteRequest {
    pub entity: KnowledgeEntityRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityDeleteRequest {
    pub delete: KnowledgeEntityDeleteRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub filtered_out: bool,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteBatchRequest {
    pub label: String,
    pub external_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedEntityDeleteBatchRequest {
    pub delete: KnowledgeEntityDeleteBatchRequest,
    pub metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteBatchRow {
    pub external_id: String,
    pub node_id: Option<u64>,
    pub matched: bool,
    pub filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeEntityDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_count: usize,
    pub filtered_out_count: usize,
    pub non_writable_count: usize,
    pub deleted_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipCreateRequest {
    pub create: KnowledgeRelationshipCreateRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateBatchRequest {
    pub creates: Vec<KnowledgeRelationshipCreateRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipCreateBatchRequest {
    pub creates: Vec<KnowledgeRelationshipCreateRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipCreateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipCreateBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpsertRequest {
    pub upsert: KnowledgeRelationshipUpsertRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub relationship_id: Option<u64>,
    pub matched: bool,
    pub created: bool,
    pub already_exists: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertBatchRequest {
    pub upserts: Vec<KnowledgeRelationshipUpsertRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpsertBatchRequest {
    pub upserts: Vec<KnowledgeRelationshipUpsertRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub relationship_id: Option<u64>,
    pub matched: bool,
    pub created: bool,
    pub already_exists: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpsertBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipUpsertBatchRow>,
    pub matched_count: usize,
    pub created_count: usize,
    pub already_exists_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub created_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub relationship_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipDeleteRequest {
    pub delete: KnowledgeRelationshipDeleteRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub deleted_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateRequest {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub relationship_properties: BTreeMap<String, Value>,
    pub assignments: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpdateRequest {
    pub update: KnowledgeRelationshipUpdateRequest,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub updated_relationship_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateBatchRequest {
    pub updates: Vec<KnowledgeRelationshipUpdateRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipUpdateBatchRequest {
    pub updates: Vec<KnowledgeRelationshipUpdateRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipUpdateBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipUpdateBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub updated_relationship_count: usize,
    pub updated_property_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteBatchRequest {
    pub deletes: Vec<KnowledgeRelationshipDeleteRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeScopedRelationshipDeleteBatchRequest {
    pub deletes: Vec<KnowledgeRelationshipDeleteRequest>,
    pub source_metadata_filters: BTreeMap<String, String>,
    pub target_metadata_filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteBatchRow {
    pub source: KnowledgeEntityRequest,
    pub target: KnowledgeEntityRequest,
    pub relationship_type: String,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub matched: bool,
    pub source_filtered_out: bool,
    pub target_filtered_out: bool,
    pub non_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelationshipDeleteBatchOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeRelationshipDeleteBatchRow>,
    pub matched_count: usize,
    pub missing_endpoint_count: usize,
    pub source_filtered_out_count: usize,
    pub target_filtered_out_count: usize,
    pub non_writable_count: usize,
    pub deleted_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceReferenceRelationshipCleanupRequest {
    pub source_reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceReferenceRelationshipCleanupRow {
    pub relationship_id: u64,
    pub source_node_id: u64,
    pub target_node_id: u64,
    pub source_external_id: Option<String>,
    pub target_external_id: Option<String>,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceReferenceRelationshipCleanupOutput {
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub rows: Vec<KnowledgeSourceReferenceRelationshipCleanupRow>,
    pub candidate_count: usize,
    pub deleted_relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntity {
    pub node_id: u64,
    pub labels: Vec<String>,
    pub external_id: Option<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeNeighborDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeGraphPathDirection {
    Outgoing,
    Incoming,
}

impl KnowledgeGraphPathDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeGraphContextPath {
    pub seed_hit_id: String,
    pub hop: usize,
    pub direction: KnowledgeGraphPathDirection,
    pub relationship_id: u64,
    pub relationship_type: String,
    pub relationship_properties: BTreeMap<String, Value>,
    pub source_node_id: u64,
    pub source_labels: Vec<String>,
    pub source_external_id: Option<String>,
    pub target_node_id: u64,
    pub target_labels: Vec<String>,
    pub target_external_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainOutput {
    pub physical_plan: PhysicalPlan,
    pub trace: OptimizerTrace,
}

#[derive(Debug)]
pub struct DatabaseTransaction<'a> {
    db: &'a mut Database,
    mutations: Vec<GraphMutation>,
    committed: bool,
}

#[derive(Debug)]
pub struct DatabaseSession<'a> {
    db: &'a mut Database,
    transaction_mutations: Option<Vec<GraphMutation>>,
}

#[derive(Debug)]
pub struct DatabaseReadTransaction {
    catalog: Catalog,
    store: GraphStore,
    optimizer: CascadesOptimizer,
    plan_cache: RefCell<PlanCache>,
    config: DatabaseConfig,
    _pin: ReaderPin,
}

#[derive(Debug)]
pub struct NowledgeGraphAdapter<'a> {
    db: &'a mut Database,
}

#[derive(Debug, Default)]
struct ReaderPins {
    next_reader_id: u64,
    active_epochs: BTreeMap<u64, u64>,
}

#[derive(Debug)]
struct ReaderPin {
    id: u64,
    pins: Rc<RefCell<ReaderPins>>,
}

impl Default for Database {
    fn default() -> Self {
        let config = DatabaseConfig::default();
        let mut store = GraphStore::default();
        store.set_max_search_projection_change_log_entries(
            config.max_search_projection_change_log_entries,
        );
        Self {
            catalog: Catalog::default(),
            store,
            optimizer: CascadesOptimizer::new(optimizer_config_from_database_config(&config)),
            plan_cache: RefCell::new(PlanCache::new(config.max_plan_cache_entries)),
            config,
            reader_pins: Rc::new(RefCell::new(ReaderPins::default())),
            next_derived_artifact_job_id: 1,
            derived_artifact_jobs: Vec::new(),
        }
    }
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_config(config: DatabaseConfig) -> Self {
        let mut store = GraphStore::default();
        store.set_max_search_projection_change_log_entries(
            config.max_search_projection_change_log_entries,
        );
        let optimizer = CascadesOptimizer::new(optimizer_config_from_database_config(&config));
        Self {
            catalog: Catalog::default(),
            store,
            optimizer,
            plan_cache: RefCell::new(PlanCache::new(config.max_plan_cache_entries)),
            config,
            reader_pins: Rc::new(RefCell::new(ReaderPins::default())),
            next_derived_artifact_job_id: 1,
            derived_artifact_jobs: Vec::new(),
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_durability(path, DurabilityPolicy::default())
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        Self::open_with_durability_and_config(path, DurabilityPolicy::default(), config)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        Self::open_with_durability_and_config(path, durability, DatabaseConfig::default())
    }

    pub fn open_with_durability_and_config(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
        config: DatabaseConfig,
    ) -> Result<Self> {
        let mut catalog = Catalog::default();
        let replay_config = WalReplayConfig {
            recovery_mode: config.recovery_mode,
            max_entries: config.max_wal_replay_entries,
        };
        let mut store = if config.read_only {
            GraphStore::open_read_only_with_durability_and_replay_config(
                path,
                &mut catalog,
                durability,
                replay_config,
            )?
        } else {
            GraphStore::open_with_durability_and_replay_config(
                path,
                &mut catalog,
                durability,
                replay_config,
            )?
        };
        store.set_max_search_projection_change_log_entries(
            config.max_search_projection_change_log_entries,
        );
        Ok(Self {
            catalog,
            store,
            optimizer: CascadesOptimizer::new(optimizer_config_from_database_config(&config)),
            plan_cache: RefCell::new(PlanCache::new(config.max_plan_cache_entries)),
            config,
            reader_pins: Rc::new(RefCell::new(ReaderPins::default())),
            next_derived_artifact_job_id: 1,
            derived_artifact_jobs: Vec::new(),
        })
    }

    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        if matches!(statement, cypher::Statement::Checkpoint) {
            if !parameters.is_empty() {
                return Err(SkeinError::Semantic(
                    "CHECKPOINT does not accept parameters".to_string(),
                ));
            }
            self.checkpoint()?;
            return Ok(QueryOutput { rows: Vec::new() });
        }
        let (physical, _) = self.optimized_query_plan(cypher_text, &statement, parameters)?;
        let is_mutation = executor::is_mutation_plan(&physical)?;
        if is_mutation {
            self.ensure_writable()?;
        }
        let rows = executor::execute(&physical, &mut self.catalog, &mut self.store)?;
        if !is_mutation {
            enforce_read_result_row_limit(&rows, &self.config)?;
        }
        Ok(QueryOutput { rows })
    }

    pub fn begin_transaction(&mut self) -> DatabaseTransaction<'_> {
        DatabaseTransaction {
            db: self,
            mutations: Vec::new(),
            committed: false,
        }
    }

    pub fn session(&mut self) -> DatabaseSession<'_> {
        DatabaseSession {
            db: self,
            transaction_mutations: None,
        }
    }

    pub fn begin_read_transaction(&self) -> DatabaseReadTransaction {
        let pin = {
            let mut pins = self.reader_pins.borrow_mut();
            let id = pins.next_reader_id;
            pins.next_reader_id += 1;
            pins.active_epochs.insert(id, self.store.commit_epoch());
            ReaderPin::new(id, Rc::clone(&self.reader_pins))
        };
        DatabaseReadTransaction {
            catalog: self.catalog.clone(),
            store: self.store.snapshot(),
            optimizer: self.optimizer.clone(),
            plan_cache: RefCell::new(PlanCache::new(self.config.max_plan_cache_entries)),
            config: self.config.clone(),
            _pin: pin,
        }
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        let statement = cypher::parse(cypher_text)?;
        let (physical_plan, trace) =
            self.optimized_query_plan(cypher_text, &statement, parameters)?;
        Ok(ExplainOutput {
            physical_plan,
            trace,
        })
    }

    pub fn plan_cache_stats(&self) -> PlanCacheStats {
        self.plan_cache.borrow().stats()
    }

    fn optimized_query_plan(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<(PhysicalPlan, OptimizerTrace)> {
        let cache_mode = if statement_uses_plan_cache(statement) {
            PlanCacheMode::Use
        } else {
            PlanCacheMode::Bypass(PlanCacheBypassReason::StatementNotCacheable)
        };
        optimized_query_plan_for(
            cypher_text,
            statement,
            parameters,
            cache_mode,
            PlanCacheContext {
                catalog: &self.catalog,
                store: &self.store,
                optimizer: &self.optimizer,
                config: &self.config,
                cache: &self.plan_cache,
            },
        )
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.ensure_writable()?;
        let oldest_reader_epoch = self.reader_pins.borrow().oldest_epoch();
        self.store
            .checkpoint_with_reader_epoch(&self.catalog, oldest_reader_epoch)
    }

    pub fn storage_reclamation_watermark(&self) -> StorageReclamationWatermark {
        let oldest_reader_epoch = self.reader_pins.borrow().oldest_epoch();
        self.store
            .storage_reclamation_watermark(oldest_reader_epoch)
    }

    pub fn storage_recovery_report(&self) -> StorageRecoveryReport {
        self.store.storage_recovery_report()
    }

    pub fn export_canonical_graph_snapshot(&self) -> CanonicalGraphSnapshotExport {
        export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }

    pub fn export_canonical_graph_snapshot_with_persisted_stable_ids(
        &mut self,
    ) -> Result<CanonicalGraphSnapshotExport> {
        self.ensure_writable()?;
        let mapping = CanonicalStableIdMapping::from(self.store.ensure_stable_id_mapping()?);
        Ok(self
            .export_canonical_graph_snapshot()
            .with_stable_id_mapping(&mapping))
    }

    pub fn prepare_graph_lightning_bootstrap_export(
        &mut self,
    ) -> Result<GraphLightningBootstrapExport> {
        let snapshot = self.export_canonical_graph_snapshot_with_persisted_stable_ids()?;
        let manifest = snapshot.graph_lightning_bootstrap_manifest();
        let graph_stream = snapshot.graph_lightning_graph_stream();
        Ok(GraphLightningBootstrapExport {
            snapshot,
            manifest,
            graph_stream,
        })
    }

    pub fn graph_lightning_bootstrap_export_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = self.graph_lightning_bootstrap_export_estimated_operations();
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Import,
            estimated_operations,
            hint,
        ))
    }

    pub fn prepare_background_graph_lightning_bootstrap_export(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
    ) -> Result<GraphLightningBootstrapExport> {
        let estimated_operations = self.graph_lightning_bootstrap_export_estimated_operations();
        if estimated_operations == 0 {
            return self.prepare_graph_lightning_bootstrap_export();
        }
        let request = WorkRequest::background(WorkClass::Import, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.prepare_graph_lightning_bootstrap_export(),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background graph lightning bootstrap export deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background graph lightning bootstrap export rejected: {reason}"
            ))),
        }
    }

    pub fn prepare_scheduled_background_graph_lightning_bootstrap_export(
        &mut self,
        scheduler: &mut LocalQosScheduler,
    ) -> Result<GraphLightningBootstrapExport> {
        let estimated_operations = self.graph_lightning_bootstrap_export_estimated_operations();
        if estimated_operations == 0 {
            return self.prepare_graph_lightning_bootstrap_export();
        }
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Import,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background graph lightning bootstrap export deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background graph lightning bootstrap export rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.prepare_graph_lightning_bootstrap_export();
        scheduler.finish(permit);
        result
    }

    fn graph_lightning_bootstrap_export_estimated_operations(&self) -> usize {
        let statistics = self.store.statistics();
        let total = statistics
            .node_count
            .saturating_add(statistics.relationship_count);
        usize::try_from(total).unwrap_or(usize::MAX)
    }

    pub fn storage_version(&self) -> &'static str {
        self.store.storage_version()
    }

    pub fn statistics(&self) -> GraphStatistics {
        self.store.statistics()
    }

    pub fn property_indexes(&self) -> Vec<IndexDescriptor> {
        self.catalog.property_indexes().cloned().collect()
    }

    pub fn composite_property_indexes(&self) -> Vec<CompositeIndexDescriptor> {
        self.catalog.composite_property_indexes().cloned().collect()
    }

    pub fn rebuild_bounded_property_index_projections(
        &mut self,
        max_estimated_operations: usize,
    ) -> QueryOutput {
        property_index_projection_rebuild_output(
            self.store.rebuild_bounded_property_index_projections(
                &self.catalog,
                max_estimated_operations,
            ),
        )
    }

    pub fn property_index_projection_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = self
            .store
            .property_index_projection_estimated_operations(&self.catalog);
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            estimated_operations,
            hint,
        ))
    }

    pub fn rebuild_bounded_background_property_index_projections(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        let estimated_operations = self
            .store
            .bounded_property_index_projection_estimated_operations(
                &self.catalog,
                max_estimated_operations,
            );
        if estimated_operations == 0 {
            return Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations));
        }
        let request = WorkRequest::background(WorkClass::Projection, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => {
                Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations))
            }
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background property index projection rebuild deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background property index projection rebuild rejected: {reason}"
            ))),
        }
    }

    pub fn rebuild_bounded_scheduled_background_property_index_projections(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        let estimated_operations = self
            .store
            .bounded_property_index_projection_estimated_operations(
                &self.catalog,
                max_estimated_operations,
            );
        if estimated_operations == 0 {
            return Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations));
        }
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Projection,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background property index projection rebuild deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background property index projection rebuild rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations));
        scheduler.finish(permit);
        result
    }

    pub fn unique_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog.unique_constraints().cloned().collect()
    }

    pub fn node_property_exists_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .node_property_exists_constraints()
            .cloned()
            .collect()
    }

    pub fn relationship_property_exists_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .relationship_property_exists_constraints()
            .cloned()
            .collect()
    }

    pub fn relationship_unique_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .relationship_unique_constraints()
            .cloned()
            .collect()
    }

    pub fn table_descriptors(&self) -> Vec<TableDescriptor> {
        self.catalog.table_descriptors().cloned().collect()
    }

    pub fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        self.catalog.property_descriptors().cloned().collect()
    }

    pub fn plan_schema_maintenance(&self) -> QueryOutput {
        let rows = self
            .store
            .plan_schema_maintenance(&self.catalog)
            .into_iter()
            .map(|item| {
                BTreeMap::from([
                    ("object_type".to_string(), Value::String(item.object_type)),
                    ("object".to_string(), Value::String(item.object)),
                    (
                        "from_state".to_string(),
                        schema_state_value(item.from_state),
                    ),
                    (
                        "to_state".to_string(),
                        item.to_state.map(schema_state_value).unwrap_or(Value::Null),
                    ),
                    ("action".to_string(), Value::String(item.action)),
                    (
                        "estimated_operations".to_string(),
                        Value::Int(i64::try_from(item.estimated_operations).unwrap_or(i64::MAX)),
                    ),
                ])
            })
            .collect();
        QueryOutput { rows }
    }

    pub fn schema_maintenance_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = self.schema_maintenance_estimated_operations();
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Mutation,
            estimated_operations,
            hint,
        ))
    }

    pub fn run_schema_maintenance(&mut self) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let actions = self.store.run_schema_maintenance(&mut self.catalog)?;
        Ok(schema_maintenance_actions_output(actions))
    }

    pub fn run_bounded_schema_maintenance(
        &mut self,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let actions = self
            .store
            .run_bounded_schema_maintenance(&mut self.catalog, max_estimated_operations)?;
        Ok(schema_maintenance_actions_output(actions))
    }

    pub fn run_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        estimated_operations: usize,
    ) -> Result<QueryOutput> {
        let request = WorkRequest::background(WorkClass::Mutation, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.run_schema_maintenance(),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance rejected: {reason}"
            ))),
        }
    }

    pub fn run_bounded_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        let estimated_operations =
            self.bounded_schema_maintenance_estimated_operations(max_estimated_operations);
        if estimated_operations == 0 {
            return self.run_bounded_schema_maintenance(max_estimated_operations);
        }
        let request = WorkRequest::background(WorkClass::Mutation, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.run_bounded_schema_maintenance(max_estimated_operations),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance rejected: {reason}"
            ))),
        }
    }

    pub fn run_planned_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
    ) -> Result<QueryOutput> {
        self.run_background_schema_maintenance(
            policy,
            state,
            self.schema_maintenance_estimated_operations(),
        )
    }

    pub fn run_scheduled_background_schema_maintenance(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        estimated_operations: usize,
    ) -> Result<QueryOutput> {
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_schema_maintenance();
        scheduler.finish(permit);
        result
    }

    pub fn run_bounded_scheduled_background_schema_maintenance(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        let estimated_operations =
            self.bounded_schema_maintenance_estimated_operations(max_estimated_operations);
        if estimated_operations == 0 {
            return self.run_bounded_schema_maintenance(max_estimated_operations);
        }
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_bounded_schema_maintenance(max_estimated_operations);
        scheduler.finish(permit);
        result
    }

    pub fn run_planned_scheduled_background_schema_maintenance(
        &mut self,
        scheduler: &mut LocalQosScheduler,
    ) -> Result<QueryOutput> {
        self.run_scheduled_background_schema_maintenance(
            scheduler,
            self.schema_maintenance_estimated_operations(),
        )
    }

    fn schema_maintenance_estimated_operations(&self) -> usize {
        self.store
            .plan_schema_maintenance(&self.catalog)
            .into_iter()
            .map(|item| item.estimated_operations)
            .fold(0usize, usize::saturating_add)
    }

    fn bounded_schema_maintenance_estimated_operations(
        &self,
        max_estimated_operations: usize,
    ) -> usize {
        let mut used_estimated_operations = 0usize;
        for item in self.store.plan_schema_maintenance(&self.catalog) {
            let Some(next) = used_estimated_operations.checked_add(item.estimated_operations)
            else {
                continue;
            };
            if next <= max_estimated_operations {
                used_estimated_operations = next;
            }
        }
        used_estimated_operations
    }

    pub fn projected_graph_statuses(&self) -> Vec<ProjectedGraphStatus> {
        self.store.projected_graph_statuses()
    }

    pub fn rebuild_projected_graph_artifacts(&mut self) -> Result<()> {
        self.ensure_writable()?;
        self.store.rebuild_projected_graph_artifacts(&self.catalog)
    }

    pub fn rebuild_search_projection(
        &self,
        search_index: &mut SearchIndex,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        search_index.rebuild_from_graph(&self.catalog, &self.store, options)
    }

    pub fn search_projection_rebuild_background_work_plan(
        &self,
        search_index: &SearchIndex,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        search_index.rebuild_background_work_plan(&self.store, hint)
    }

    pub fn rebuild_background_search_projection(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        search_index.rebuild_background_derived_artifacts(
            policy,
            state,
            &self.catalog,
            &self.store,
            options,
        )
    }

    pub fn rebuild_scheduled_background_search_projection(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        search_index.rebuild_scheduled_background_derived_artifacts(
            scheduler,
            &self.catalog,
            &self.store,
            options,
        )
    }

    pub fn repair_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_metadata_from_graph(&self.catalog, &self.store, options)
    }

    pub fn search_projection_metadata_repair_background_work_plan(
        &self,
        search_index: &SearchIndex,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        search_index.metadata_repair_background_work_plan(&self.store, hint)
    }

    pub fn repair_background_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_background_metadata_from_graph(
            policy,
            state,
            &self.catalog,
            &self.store,
            options,
            estimated_operations,
        )
    }

    pub fn repair_scheduled_background_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_scheduled_background_metadata_from_graph(
            scheduler,
            &self.catalog,
            &self.store,
            options,
            estimated_operations,
        )
    }

    pub fn search_projection_delta_background_work_plan(
        &self,
        delta: &SearchProjectionDelta,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        delta.background_work_plan(hint)
    }

    pub fn search_projection_graph_delta_background_work_plan(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        request.background_work_plan(hint)
    }

    pub fn search_projection_graph_delta_freshness_background_work_plan(
        &self,
        search_index: &SearchIndex,
        request: &SearchProjectionGraphDeltaRequest,
        mut hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        if hint.recent_delta_operations == 0 {
            hint.recent_delta_operations = request.operation_count();
        }
        if hint.source_graph_commit_lag == 0 {
            hint.source_graph_commit_lag =
                search_projection_commit_lag(search_index, self.store.commit_epoch());
        }
        if request.operation_count() == 0
            && hint.source_graph_commit_lag > 0
            && request.complete_through_graph_commit_epoch.is_some()
        {
            if hint.recent_delta_operations == 0 {
                hint.recent_delta_operations = 1;
            }
            return Some(BackgroundWorkPlan::background(
                WorkClass::Projection,
                1,
                hint,
            ));
        }
        request.background_work_plan(hint)
    }

    pub fn search_projection_freshness_lag_background_work_plan(
        &self,
        search_index: &SearchIndex,
        mut hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let source_graph_commit_lag =
            search_projection_commit_lag(search_index, self.store.commit_epoch());
        if source_graph_commit_lag == 0 {
            return None;
        }
        let operation_count = usize::try_from(source_graph_commit_lag).unwrap_or(usize::MAX);
        if hint.recent_delta_operations == 0 {
            hint.recent_delta_operations = operation_count;
        }
        if hint.source_graph_commit_lag == 0 {
            hint.source_graph_commit_lag = source_graph_commit_lag;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            operation_count,
            hint,
        ))
    }

    pub fn build_search_projection_graph_delta_request_after(
        &self,
        source_graph_commit_epoch: u64,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        let current_epoch = self.store.commit_epoch();
        if source_graph_commit_epoch >= current_epoch {
            return Ok(None);
        }
        let change_log_start_epoch = self.store.search_projection_change_log_start_epoch();
        if source_graph_commit_epoch < change_log_start_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection change log starts at commit epoch {change_log_start_epoch}; requested source graph commit epoch {source_graph_commit_epoch}; full search projection rebuild required"
            )));
        }

        let mut upsert_node_ids = BTreeSet::new();
        let mut delete_document_ids = BTreeSet::new();
        for change in self
            .store
            .search_projection_graph_changes_after(source_graph_commit_epoch)
        {
            upsert_node_ids.extend(change.upsert_node_ids);
            delete_document_ids.extend(change.delete_document_ids);
        }

        Ok(Some(SearchProjectionGraphDeltaRequest {
            upsert_node_ids: upsert_node_ids.into_iter().collect(),
            delete_document_ids: delete_document_ids.into_iter().collect(),
            max_operations,
            complete_through_graph_commit_epoch: Some(current_epoch),
        }))
    }

    pub fn build_search_projection_graph_delta_request_from_freshness(
        &self,
        search_index: &SearchIndex,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        self.build_search_projection_graph_delta_request_after(
            search_index
                .projection_freshness()
                .source_graph_commit_epoch
                .unwrap_or(0),
            max_operations,
        )
    }

    pub fn background_maintenance_candidates(
        &self,
        search_index: Option<&SearchIndex>,
        options: BackgroundMaintenanceOptions,
    ) -> Vec<BackgroundMaintenanceCandidate> {
        let mut candidates = Vec::new();

        if options.include_schema_maintenance {
            if let Some(plan) = self.schema_maintenance_background_work_plan(options.hint.clone()) {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::SchemaMaintenance,
                    plan,
                ));
            }
        }

        if options.include_property_index_projection {
            if let Some(plan) =
                self.property_index_projection_background_work_plan(options.hint.clone())
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::PropertyIndexProjection,
                    plan,
                ));
            }
        }

        if let Some(delta_request) = &options.search_projection_graph_delta {
            let plan = match search_index {
                Some(search_index) => self
                    .search_projection_graph_delta_freshness_background_work_plan(
                        search_index,
                        delta_request,
                        options.hint.clone(),
                    ),
                None => self.search_projection_graph_delta_background_work_plan(
                    delta_request,
                    options.hint.clone(),
                ),
            };
            if let Some(plan) = plan {
                candidates.push(
                    BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                        plan,
                    )
                    .with_search_projection_graph_delta(delta_request.clone()),
                );
            }
        } else if options.include_search_projection_graph_delta_freshness {
            if let Some(search_index) = search_index {
                let executable_request = self
                    .build_search_projection_graph_delta_request_from_freshness(search_index, None)
                    .ok()
                    .flatten();
                if let Some(request) = executable_request {
                    if let Some(plan) = self
                        .search_projection_graph_delta_freshness_background_work_plan(
                            search_index,
                            &request,
                            options.hint.clone(),
                        )
                    {
                        candidates.push(
                            BackgroundMaintenanceCandidate::new(
                                BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                                plan,
                            )
                            .with_search_projection_graph_delta(request),
                        );
                    }
                } else if let Some(plan) = self
                    .search_projection_freshness_lag_background_work_plan(
                        search_index,
                        options.hint.clone(),
                    )
                {
                    candidates.push(BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                        plan,
                    ));
                }
            }
        }

        if let Some(search_index) = search_index {
            if options.include_search_projection_rebuild {
                let mut hint = options.hint.clone();
                if hint.source_graph_commit_lag == 0 {
                    hint.source_graph_commit_lag =
                        search_projection_commit_lag(search_index, self.store.commit_epoch());
                }
                if let Some(plan) =
                    self.search_projection_rebuild_background_work_plan(search_index, hint)
                {
                    candidates.push(BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionRebuild,
                        plan,
                    ));
                }
            }

            if options.include_search_projection_metadata_repair {
                if let Some(plan) = self.search_projection_metadata_repair_background_work_plan(
                    search_index,
                    options.hint.clone(),
                ) {
                    candidates.push(BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionMetadataRepair,
                        plan,
                    ));
                }
            }
        }

        if options.include_graph_lightning_bootstrap_export {
            if let Some(plan) =
                self.graph_lightning_bootstrap_export_background_work_plan(options.hint.clone())
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::GraphLightningBootstrapExport,
                    plan,
                ));
            }
        }

        if options.include_external_content_artifact_jobs {
            if let Some(plan) = self.external_content_artifact_job_background_work_plan(
                options.hint,
                options.external_content_artifact_estimated_operations,
            ) {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::ExternalContentArtifactJob,
                    plan,
                ));
            }
        }

        candidates
    }

    pub fn rank_background_maintenance(
        &self,
        search_index: Option<&SearchIndex>,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> Vec<RankedBackgroundMaintenance> {
        let candidates = self.background_maintenance_candidates(search_index, options);
        let plans = candidates
            .iter()
            .map(|candidate| candidate.plan.clone())
            .collect::<Vec<_>>();
        policy
            .rank_background_work(state, &plans)
            .into_iter()
            .map(|ranked| {
                let candidate = &candidates[ranked.index];
                RankedBackgroundMaintenance {
                    kind: candidate.kind,
                    name: candidate.name.clone(),
                    plan: candidate.plan.clone(),
                    decision: ranked.decision,
                    search_projection_graph_delta: candidate.search_projection_graph_delta.clone(),
                }
            })
            .collect()
    }

    pub fn background_maintenance_summary(
        &self,
        search_index: Option<&SearchIndex>,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> BackgroundMaintenanceSummary {
        let ranked = self.rank_background_maintenance(search_index, policy, state, options);
        BackgroundMaintenanceSummary::from_ranked(ranked)
    }

    pub fn build_search_projection_graph_delta(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDelta> {
        search_projection_graph_delta_for(&self.catalog, &self.store, request)
    }

    pub fn apply_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        search_index.apply_projection_delta(delta)
    }

    pub fn apply_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let delta = self.build_search_projection_graph_delta(&request)?;
        search_index.apply_projection_delta(delta)
    }

    pub fn apply_background_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        search_index.apply_background_projection_delta(policy, state, delta)
    }

    pub fn apply_background_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        match policy.admit(state, &request.background_work_request()) {
            QosAdmission::Admit => self.apply_search_projection_graph_delta(search_index, request),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection graph delta deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection graph delta rejected: {reason}"
            ))),
        }
    }

    pub fn apply_scheduled_background_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        search_index.apply_scheduled_background_projection_delta(scheduler, delta)
    }

    pub fn apply_scheduled_background_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let permit = match scheduler.try_start(request.background_work_request()) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection graph delta deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection graph delta rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.apply_search_projection_graph_delta(search_index, request);
        scheduler.finish(permit);
        result
    }

    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
        }
        .retrieve_knowledge(search_index, request)
    }

    pub fn knowledge_entity(&self, request: &KnowledgeEntityRequest) -> KnowledgeEntityOutput {
        knowledge_entity_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_entity_batch(
        &self,
        request: &KnowledgeEntityBatchRequest,
    ) -> KnowledgeEntityBatchOutput {
        knowledge_entity_batch_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_memory_entities(
        &self,
        request: &KnowledgeMemoryEntityListRequest,
    ) -> Result<KnowledgeMemoryEntityListOutput> {
        knowledge_memory_entities_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_context_memory_preview(
        &self,
        request: &KnowledgeContextMemoryPreviewRequest,
    ) -> Result<KnowledgeContextMemoryPreviewOutput> {
        knowledge_context_memory_preview_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_memories(
        &self,
        request: &KnowledgeMemoryListRequest,
    ) -> Result<KnowledgeMemoryListOutput> {
        knowledge_memories_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_entity(
        &self,
        request: &KnowledgeScopedEntityRequest,
    ) -> KnowledgeEntityOutput {
        knowledge_scoped_entity_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_entity_batch(
        &self,
        request: &KnowledgeScopedEntityBatchRequest,
    ) -> KnowledgeEntityBatchOutput {
        knowledge_scoped_entity_batch_for(&self.catalog, &self.store, request)
    }

    pub fn create_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityCreateRequest,
    ) -> Result<KnowledgeEntityCreateOutput> {
        create_knowledge_entity_for(self, request)
    }

    pub fn create_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityCreateBatchRequest,
    ) -> Result<KnowledgeEntityCreateBatchOutput> {
        create_knowledge_entity_batch_for(self, request)
    }

    pub fn upsert_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityUpsertRequest,
    ) -> Result<KnowledgeEntityUpsertOutput> {
        upsert_knowledge_entity_for(self, request)
    }

    pub fn upsert_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityUpsertBatchRequest,
    ) -> Result<KnowledgeEntityUpsertBatchOutput> {
        upsert_knowledge_entity_batch_for(self, request)
    }

    pub fn knowledge_property_batch(
        &self,
        request: &KnowledgePropertyBatchRequest,
    ) -> KnowledgePropertyBatchOutput {
        knowledge_property_batch_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_property_batch(
        &self,
        request: &KnowledgeScopedPropertyBatchRequest,
    ) -> KnowledgePropertyBatchOutput {
        knowledge_scoped_property_batch_for(&self.catalog, &self.store, request)
    }

    pub fn update_knowledge_properties(
        &mut self,
        request: &KnowledgePropertyUpdateRequest,
    ) -> Result<KnowledgePropertyUpdateOutput> {
        update_knowledge_properties_for(self, request)
    }

    pub fn update_scoped_knowledge_properties(
        &mut self,
        request: &KnowledgeScopedPropertyUpdateRequest,
    ) -> Result<KnowledgePropertyUpdateOutput> {
        update_scoped_knowledge_properties_for(self, request)
    }

    pub fn update_knowledge_properties_batch(
        &mut self,
        request: &KnowledgePropertyUpdateBatchRequest,
    ) -> Result<KnowledgePropertyUpdateBatchOutput> {
        update_knowledge_properties_batch_for(self, request)
    }

    pub fn update_scoped_knowledge_properties_batch(
        &mut self,
        request: &KnowledgeScopedPropertyUpdateBatchRequest,
    ) -> Result<KnowledgePropertyUpdateBatchOutput> {
        update_scoped_knowledge_properties_batch_for(self, request)
    }

    pub fn move_knowledge_normalized_space_batch(
        &mut self,
        request: &KnowledgeNormalizedSpaceMoveBatchRequest,
    ) -> Result<KnowledgeNormalizedSpaceMoveBatchOutput> {
        move_knowledge_normalized_space_batch_for(self, request)
    }

    pub fn touch_knowledge_memory_access_batch(
        &mut self,
        request: &KnowledgeMemoryAccessBatchRequest,
    ) -> Result<KnowledgeMemoryAccessBatchOutput> {
        touch_knowledge_memory_access_batch_for(self, request)
    }

    pub fn adjust_knowledge_source_memory_count_batch(
        &mut self,
        request: &KnowledgeSourceMemoryCountBatchRequest,
    ) -> Result<KnowledgeSourceMemoryCountBatchOutput> {
        adjust_knowledge_source_memory_count_batch_for(self, request)
    }

    pub fn update_knowledge_source_lifecycle_batch(
        &mut self,
        request: &KnowledgeSourceLifecycleBatchRequest,
    ) -> Result<KnowledgeSourceLifecycleBatchOutput> {
        update_knowledge_source_lifecycle_batch_for(self, request)
    }

    pub fn knowledge_source(
        &self,
        request: &KnowledgeSourceRequest,
    ) -> Result<KnowledgeSourceOutput> {
        knowledge_source_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_source_ids(
        &self,
        request: &KnowledgeSourceIdListRequest,
    ) -> Result<KnowledgeSourceIdListOutput> {
        knowledge_source_ids_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_sources(
        &self,
        request: &KnowledgeSourceListRequest,
    ) -> Result<KnowledgeSourceListOutput> {
        knowledge_sources_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_source_count(&self) -> KnowledgeSourceCountOutput {
        KnowledgeSourceCountOutput {
            graph_commit_epoch: self.store.commit_epoch(),
            count: count_nodes_with_label(&self.catalog, &self.store, "Source"),
        }
    }

    pub fn knowledge_source_memories(
        &self,
        request: &KnowledgeSourceMemoryListRequest,
    ) -> Result<KnowledgeSourceMemoryListOutput> {
        knowledge_source_memories_for(&self.catalog, &self.store, request)
    }

    pub fn update_knowledge_memory_lifecycle_batch(
        &mut self,
        request: &KnowledgeMemoryLifecycleBatchRequest,
    ) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
        update_knowledge_memory_lifecycle_batch_for(self, request)
    }

    pub fn update_knowledge_memory_latest_batch(
        &mut self,
        request: &KnowledgeMemoryLatestBatchRequest,
    ) -> Result<KnowledgeMemoryLatestBatchOutput> {
        update_knowledge_memory_latest_batch_for(self, request)
    }

    pub fn update_knowledge_skill_usage_stats_batch(
        &mut self,
        request: &KnowledgeSkillUsageStatsBatchRequest,
    ) -> Result<KnowledgeSkillUsageStatsBatchOutput> {
        update_knowledge_skill_usage_stats_batch_for(self, request)
    }

    pub fn update_knowledge_skill_lifecycle_batch(
        &mut self,
        request: &KnowledgeSkillLifecycleBatchRequest,
    ) -> Result<KnowledgeSkillLifecycleBatchOutput> {
        update_knowledge_skill_lifecycle_batch_for(self, request)
    }

    pub fn knowledge_skill_memories(
        &self,
        request: &KnowledgeSkillMemoryListRequest,
    ) -> Result<KnowledgeSkillMemoryListOutput> {
        knowledge_skill_memories_for(&self.catalog, &self.store, request)
    }

    pub fn update_knowledge_thread_metadata_batch(
        &mut self,
        request: &KnowledgeThreadMetadataBatchRequest,
    ) -> Result<KnowledgeThreadMetadataBatchOutput> {
        update_knowledge_thread_metadata_batch_for(self, request)
    }

    pub fn update_knowledge_thread_message_count_batch(
        &mut self,
        request: &KnowledgeThreadMessageCountBatchRequest,
    ) -> Result<KnowledgeThreadMessageCountBatchOutput> {
        update_knowledge_thread_message_count_batch_for(self, request)
    }

    pub fn knowledge_thread_messages(
        &self,
        request: &KnowledgeThreadMessageListRequest,
    ) -> Result<KnowledgeThreadMessageListOutput> {
        knowledge_thread_messages_for(&self.catalog, &self.store, request)
    }

    pub fn update_knowledge_label_lifecycle_batch(
        &mut self,
        request: &KnowledgeLabelLifecycleBatchRequest,
    ) -> Result<KnowledgeLabelLifecycleBatchOutput> {
        update_knowledge_label_lifecycle_batch_for(self, request)
    }

    pub fn lookup_knowledge_labels_by_canonical_name(
        &self,
        request: &KnowledgeLabelCanonicalLookupRequest,
    ) -> Result<KnowledgeLabelUsageListOutput> {
        lookup_knowledge_labels_by_canonical_name_for(&self.catalog, &self.store, request)
    }

    pub fn scan_knowledge_labels_missing_canonical_name(
        &self,
        request: &KnowledgeLabelBackfillScanRequest,
    ) -> Result<KnowledgeLabelUsageListOutput> {
        scan_knowledge_labels_missing_canonical_name_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_label_usage(
        &self,
        request: &KnowledgeLabelUsageRequest,
    ) -> Result<KnowledgeLabelUsageOutput> {
        knowledge_label_usage_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_label_canonical_usage(
        &self,
        request: &KnowledgeLabelUsageListRequest,
    ) -> KnowledgeLabelUsageListOutput {
        knowledge_label_canonical_usage_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_entity_labels(
        &self,
        request: &KnowledgeEntityLabelListRequest,
    ) -> Result<KnowledgeEntityLabelListOutput> {
        knowledge_entity_labels_for(&self.catalog, &self.store, request)
    }

    pub fn update_knowledge_pagerank_scores_batch(
        &mut self,
        request: &KnowledgePageRankScoreBatchRequest,
    ) -> Result<KnowledgePageRankScoreBatchOutput> {
        update_knowledge_pagerank_scores_batch_for(self, request)
    }

    pub fn clear_knowledge_pagerank_scores(
        &mut self,
        request: &KnowledgePageRankClearRequest,
    ) -> Result<KnowledgePageRankClearOutput> {
        clear_knowledge_pagerank_scores_for(self, request)
    }

    pub fn knowledge_pagerank_plan(
        &self,
        request: &KnowledgePageRankPlanRequest,
    ) -> KnowledgePageRankPlanOutput {
        knowledge_pagerank_plan_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_pagerank_membership(
        &self,
        request: &KnowledgePageRankMembershipRequest,
    ) -> Result<KnowledgePageRankMembershipOutput> {
        knowledge_pagerank_membership_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_pagerank_memory_visibility(
        &self,
        request: &KnowledgePageRankMemoryVisibilityRequest,
    ) -> Result<KnowledgePageRankMemoryVisibilityOutput> {
        knowledge_pagerank_memory_visibility_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_pagerank_central_entity(
        &self,
        request: &KnowledgePageRankCentralEntityRequest,
    ) -> Result<KnowledgePageRankCentralEntityOutput> {
        knowledge_pagerank_central_entity_for(&self.catalog, &self.store, request)
    }

    pub fn clear_knowledge_community_assignments(
        &mut self,
        request: &KnowledgeCommunityAssignmentClearRequest,
    ) -> Result<KnowledgeCommunityAssignmentClearOutput> {
        clear_knowledge_community_assignments_for(self, request)
    }

    pub fn create_knowledge_community_memberships_batch(
        &mut self,
        request: &KnowledgeCommunityMembershipCreateBatchRequest,
    ) -> Result<KnowledgeCommunityMembershipCreateBatchOutput> {
        create_knowledge_community_memberships_batch_for(self, request)
    }

    pub fn update_knowledge_communities_batch(
        &mut self,
        request: &KnowledgeCommunityLifecycleBatchRequest,
    ) -> Result<KnowledgeCommunityLifecycleBatchOutput> {
        update_knowledge_communities_batch_for(self, request)
    }

    pub fn delete_knowledge_communities(
        &mut self,
        request: &KnowledgeCommunityCleanupRequest,
    ) -> Result<KnowledgeCommunityCleanupOutput> {
        delete_knowledge_communities_for(self, request)
    }

    pub fn stamp_knowledge_graph_meta_batch(
        &mut self,
        request: &KnowledgeGraphMetaStampBatchRequest,
    ) -> Result<KnowledgeGraphMetaStampBatchOutput> {
        stamp_knowledge_graph_meta_batch_for(self, request)
    }

    pub fn knowledge_graph_meta(
        &self,
        request: &KnowledgeGraphMetaRequest,
    ) -> Result<KnowledgeGraphMetaOutput> {
        knowledge_graph_meta_for(&self.catalog, &self.store, request)
    }

    pub fn delete_knowledge_graph_meta(
        &mut self,
        request: &KnowledgeGraphMetaRequest,
    ) -> Result<KnowledgeGraphMetaDeleteOutput> {
        delete_knowledge_graph_meta_for(self, request)
    }

    pub fn apply_knowledge_schema_migrations_batch(
        &mut self,
        request: &KnowledgeSchemaMigrationApplyBatchRequest,
    ) -> Result<KnowledgeSchemaMigrationApplyBatchOutput> {
        apply_knowledge_schema_migrations_batch_for(self, request)
    }

    pub fn knowledge_schema_migrations(
        &self,
        request: &KnowledgeSchemaMigrationListRequest,
    ) -> KnowledgeSchemaMigrationListOutput {
        knowledge_schema_migrations_for(&self.catalog, &self.store, request)
    }

    pub fn update_knowledge_augmentation_jobs_batch(
        &mut self,
        request: &KnowledgeAugmentationJobLifecycleBatchRequest,
    ) -> Result<KnowledgeAugmentationJobLifecycleBatchOutput> {
        update_knowledge_augmentation_jobs_batch_for(self, request)
    }

    pub fn knowledge_augmentation_job(
        &self,
        request: &KnowledgeAugmentationJobRequest,
    ) -> Result<KnowledgeAugmentationJobOutput> {
        knowledge_augmentation_job_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_augmentation_jobs(
        &self,
        request: &KnowledgeAugmentationJobListRequest,
    ) -> Result<KnowledgeAugmentationJobListOutput> {
        knowledge_augmentation_jobs_for(&self.catalog, &self.store, request)
    }

    pub fn interrupt_knowledge_augmentation_jobs(
        &mut self,
        request: &KnowledgeAugmentationJobInterruptRequest,
    ) -> Result<KnowledgeAugmentationJobInterruptOutput> {
        interrupt_knowledge_augmentation_jobs_for(self, request)
    }

    pub fn delete_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityDeleteRequest,
    ) -> Result<KnowledgeEntityDeleteOutput> {
        delete_knowledge_entity_for(self, request)
    }

    pub fn delete_scoped_knowledge_entity(
        &mut self,
        request: &KnowledgeScopedEntityDeleteRequest,
    ) -> Result<KnowledgeEntityDeleteOutput> {
        delete_scoped_knowledge_entity_for(self, request)
    }

    pub fn delete_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityDeleteBatchRequest,
    ) -> Result<KnowledgeEntityDeleteBatchOutput> {
        delete_knowledge_entity_batch_for(self, request)
    }

    pub fn delete_scoped_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeScopedEntityDeleteBatchRequest,
    ) -> Result<KnowledgeEntityDeleteBatchOutput> {
        delete_scoped_knowledge_entity_batch_for(self, request)
    }

    pub fn create_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipCreateRequest,
    ) -> Result<KnowledgeRelationshipCreateOutput> {
        create_knowledge_relationship_for(self, request)
    }

    pub fn create_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipCreateRequest,
    ) -> Result<KnowledgeRelationshipCreateOutput> {
        create_scoped_knowledge_relationship_for(self, request)
    }

    pub fn create_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipCreateBatchRequest,
    ) -> Result<KnowledgeRelationshipCreateBatchOutput> {
        create_knowledge_relationship_batch_for(self, request)
    }

    pub fn create_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipCreateBatchRequest,
    ) -> Result<KnowledgeRelationshipCreateBatchOutput> {
        create_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn upsert_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipUpsertRequest,
    ) -> Result<KnowledgeRelationshipUpsertOutput> {
        upsert_knowledge_relationship_for(self, request)
    }

    pub fn upsert_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipUpsertRequest,
    ) -> Result<KnowledgeRelationshipUpsertOutput> {
        upsert_scoped_knowledge_relationship_for(self, request)
    }

    pub fn upsert_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipUpsertBatchRequest,
    ) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
        upsert_knowledge_relationship_batch_for(self, request)
    }

    pub fn upsert_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipUpsertBatchRequest,
    ) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
        upsert_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipDeleteRequest,
    ) -> Result<KnowledgeRelationshipDeleteOutput> {
        delete_knowledge_relationship_for(self, request)
    }

    pub fn delete_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipDeleteRequest,
    ) -> Result<KnowledgeRelationshipDeleteOutput> {
        delete_scoped_knowledge_relationship_for(self, request)
    }

    pub fn update_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipUpdateRequest,
    ) -> Result<KnowledgeRelationshipUpdateOutput> {
        update_knowledge_relationship_for(self, request)
    }

    pub fn update_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipUpdateRequest,
    ) -> Result<KnowledgeRelationshipUpdateOutput> {
        update_scoped_knowledge_relationship_for(self, request)
    }

    pub fn update_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipUpdateBatchRequest,
    ) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
        update_knowledge_relationship_batch_for(self, request)
    }

    pub fn update_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipUpdateBatchRequest,
    ) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
        update_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipDeleteBatchRequest,
    ) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
        delete_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipDeleteBatchRequest,
    ) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
        delete_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_knowledge_source_reference_relationships(
        &mut self,
        request: &KnowledgeSourceReferenceRelationshipCleanupRequest,
    ) -> Result<KnowledgeSourceReferenceRelationshipCleanupOutput> {
        delete_knowledge_source_reference_relationships_for(self, request)
    }

    pub fn knowledge_neighbors(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        knowledge_neighbors_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_neighbors(
        &self,
        request: &KnowledgeScopedNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        knowledge_scoped_neighbors_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_relationships(
        &self,
        request: &KnowledgeRelationshipsRequest,
    ) -> KnowledgeRelationshipsOutput {
        knowledge_relationships_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_relationships(
        &self,
        request: &KnowledgeScopedRelationshipsRequest,
    ) -> KnowledgeRelationshipsOutput {
        knowledge_scoped_relationships_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_induced_edges(
        &self,
        request: &KnowledgeInducedEdgeListRequest,
    ) -> Result<KnowledgeInducedEdgeListOutput> {
        knowledge_induced_edges_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_paths(&self, request: &KnowledgePathRequest) -> KnowledgePathOutput {
        knowledge_paths_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_paths(
        &self,
        request: &KnowledgeScopedPathRequest,
    ) -> KnowledgePathOutput {
        knowledge_scoped_paths_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_subgraph(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        knowledge_subgraph_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_subgraph(
        &self,
        request: &KnowledgeScopedSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        knowledge_scoped_subgraph_for(&self.catalog, &self.store, request)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.config.read_only {
            return Err(SkeinError::Execution(
                "database is opened in read-only mode".to_string(),
            ));
        }
        Ok(())
    }

    pub fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph {
        match rel_type {
            Some(name) => self
                .catalog
                .rel_type_id(name)
                .map(|rel_type_id| ProjectedGraph::from_store(&self.store, Some(rel_type_id)))
                .unwrap_or_else(|| ProjectedGraph::from_store_without_edges(&self.store)),
            None => ProjectedGraph::from_store(&self.store, None),
        }
    }
}

struct KnowledgeRetrievalGraphContext<'a> {
    catalog: &'a Catalog,
    store: &'a GraphStore,
}

impl KnowledgeRetrievalGraphContext<'_> {
    fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        let search = search_index.search_with_options(
            &request.query_text,
            request.query_embedding.as_deref(),
            request.mode,
            SearchQueryOptions {
                limit: request.limit,
                rank_window: request.rank_window,
                fusion_weights: request.search_fusion_weights,
                metadata_filters: request.metadata_filters.clone(),
                policy_epoch: None,
            },
        );
        let graph_seed_search = self.search_knowledge_graph_seeds(
            &request.query_text,
            request.graph_seed_limit,
            &request.metadata_filters,
        );
        let graph_context_search = self.expand_knowledge_context(
            &search,
            &graph_seed_search.seeds,
            request.graph_context_limit,
            request.graph_context_max_hops,
        );
        let evidence = self.knowledge_evidence_for_search(&search, &graph_context_search.paths);
        let projection_freshness = search_index.projection_freshness();
        let graph_commit_epoch = self.store.commit_epoch();
        let retrievers = knowledge_retriever_reports(
            &search,
            &evidence,
            &graph_seed_search.seeds,
            &graph_context_search.paths,
            &projection_freshness,
            KnowledgeGraphSeedRetrieverInput {
                limit: request.graph_seed_limit,
                input_candidate_count: graph_seed_search.input_candidate_count,
                input_filtered_out_count: graph_seed_search.input_filtered_out_count,
                metadata_filters: request.metadata_filters.clone(),
                candidate_count: graph_seed_search.candidate_count,
                graph_commit_epoch,
            },
        );
        let (candidates, candidate_total_count, candidate_fanout_details) = self
            .knowledge_candidates(
                &search,
                &evidence,
                &graph_seed_search.seeds,
                &graph_context_search.paths,
                request.candidate_limit,
                request.candidate_scoring,
            );
        let mut fanout_reason_details = graph_context_search.fanout_reason_details.clone();
        fanout_reason_details.extend(graph_seed_search.fanout_reason_details.clone());
        fanout_reason_details.extend(candidate_fanout_details);
        let fanout_reason_codes = knowledge_fanout_reason_codes(&fanout_reason_details);
        let fanout_reasons = knowledge_fanout_reason_messages(&fanout_reason_details);
        let diagnostics = knowledge_retrieval_diagnostics(
            &search,
            request,
            &projection_freshness,
            graph_commit_epoch,
            KnowledgeRetrievalDiagnosticsInput {
                graph_seed_input_candidate_set: knowledge_graph_seed_input_candidate_set_report(
                    graph_seed_search.input_candidate_count,
                    graph_commit_epoch,
                    graph_seed_search.input_filtered_out_count,
                    request.metadata_filters.clone(),
                ),
                graph_seed_candidate_set: knowledge_graph_seed_candidate_set_report(
                    graph_seed_search.seeds.len(),
                    graph_commit_epoch,
                ),
                graph_seed_candidate_count: graph_seed_search.candidate_count,
                graph_seed_returned_count: graph_seed_search.seeds.len(),
                graph_context_input_candidate_set:
                    knowledge_graph_context_input_candidate_set_report(
                        graph_context_search.input_seed_count,
                        graph_commit_epoch,
                    ),
                graph_context_candidate_set: knowledge_graph_context_candidate_set_report(
                    graph_context_search.expanded_relationship_count,
                    graph_commit_epoch,
                ),
                graph_context_path_count: graph_context_search.paths.len(),
                graph_context_node_count: knowledge_context_path_node_count(
                    &graph_context_search.paths,
                ),
                graph_context_relationship_count: graph_context_search.paths.len(),
                graph_context_truncation_reasons: graph_context_search.truncation_reasons.clone(),
                fanout_reason_details: fanout_reason_details.clone(),
                candidate_count: candidates.len(),
                candidate_total_count,
            },
        );
        KnowledgeRetrievalOutput {
            graph_commit_epoch,
            projection_freshness,
            search,
            retrievers,
            diagnostics,
            candidates,
            evidence,
            graph_seeds: graph_seed_search.seeds,
            graph_context_paths: graph_context_search.paths,
            fanout_reason_codes,
            fanout_reason_details,
            fanout_reasons,
        }
    }

    fn expand_knowledge_context(
        &self,
        search: &SearchResultSet,
        graph_seeds: &[KnowledgeGraphSeed],
        graph_context_limit: usize,
        graph_context_max_hops: usize,
    ) -> KnowledgeGraphContextSearchOutput {
        let mut paths = Vec::new();
        let mut fanout_reasons = Vec::new();
        let mut truncation_reasons = Vec::new();
        let mut seen_relationships = BTreeSet::new();
        let mut seen_frontier_nodes = BTreeSet::new();
        let mut reported_dense_groups = BTreeSet::new();
        let mut frontier = VecDeque::new();

        for hit in &search.hits {
            let Some(seed) =
                self.seed_node_for_hit(hit.kind.as_deref(), hit.external_id.as_deref())
            else {
                continue;
            };
            if seen_frontier_nodes.insert((hit.id.clone(), seed.id.0)) {
                frontier.push_back((hit.id.clone(), seed.id, 0usize));
            }
        }
        for seed in graph_seeds {
            let seed_id = graph_seed_candidate_id(seed);
            let seed_node = NodeId(seed.entity.node_id);
            if seen_frontier_nodes.insert((seed_id.clone(), seed_node.0)) {
                frontier.push_back((seed_id, seed_node, 0usize));
            }
        }
        let input_seed_count = seen_frontier_nodes.len();

        while let Some((seed_hit_id, current_node, depth)) = frontier.pop_front() {
            if depth >= graph_context_max_hops {
                continue;
            }
            record_dense_adjacency_diagnostics(
                DenseAdjacencyDiagnosticContext {
                    catalog: self.catalog,
                    store: self.store,
                    operation: "graph_context",
                    relationship_type: None,
                    requested_direction: KnowledgeNeighborDirection::Both,
                },
                current_node,
                &mut reported_dense_groups,
                &mut fanout_reasons,
            );
            for edge in knowledge_expansion_edges_for_node(
                self.store,
                current_node,
                None,
                KnowledgeNeighborDirection::Both,
            ) {
                if !seen_relationships.insert((seed_hit_id.clone(), edge.relationship.id.0)) {
                    continue;
                }
                if paths.len() >= graph_context_limit {
                    let detail = KnowledgeFanoutReasonDetail::graph_context_limit(
                        graph_context_limit,
                        &seed_hit_id,
                    );
                    truncation_reasons.push(detail.message.clone());
                    fanout_reasons.push(detail);
                    let expanded_relationship_count = paths.len();
                    return KnowledgeGraphContextSearchOutput {
                        paths,
                        input_seed_count,
                        expanded_relationship_count,
                        fanout_reason_details: fanout_reasons,
                        truncation_reasons,
                    };
                }
                let Some(path) = self.context_path_for_relationship(
                    &seed_hit_id,
                    depth + 1,
                    edge.direction,
                    edge.relationship,
                ) else {
                    continue;
                };
                paths.push(path);
                if seen_frontier_nodes.insert((seed_hit_id.clone(), edge.next_node.0)) {
                    frontier.push_back((seed_hit_id.clone(), edge.next_node, depth + 1));
                }
            }
        }

        let expanded_relationship_count = paths.len();
        KnowledgeGraphContextSearchOutput {
            paths,
            input_seed_count,
            expanded_relationship_count,
            fanout_reason_details: fanout_reasons,
            truncation_reasons,
        }
    }

    fn seed_node_for_hit(
        &self,
        kind: Option<&str>,
        external_id: Option<&str>,
    ) -> Option<&NodeRecord> {
        let external_id = external_id?;
        let label = kind.and_then(search_kind_to_label)?;
        let label_id = self.catalog.label_id(label)?;
        self.store
            .scan_nodes(Some(label_id))
            .find(|node| projected_node_external_id(node) == external_id)
    }

    fn context_path_for_relationship(
        &self,
        seed_hit_id: &str,
        hop: usize,
        direction: KnowledgeGraphPathDirection,
        relationship: &RelRecord,
    ) -> Option<KnowledgeGraphContextPath> {
        context_path_for_relationship(
            self.catalog,
            self.store,
            seed_hit_id,
            hop,
            direction,
            relationship,
        )
    }

    fn knowledge_entity_from_node(&self, node: &NodeRecord) -> KnowledgeEntity {
        knowledge_entity_from_node(self.catalog, node)
    }

    fn knowledge_evidence_for_search(
        &self,
        search: &SearchResultSet,
        graph_context_paths: &[KnowledgeGraphContextPath],
    ) -> Vec<KnowledgeEvidence> {
        search
            .hits
            .iter()
            .map(|hit| {
                let canonical_node_id = self
                    .seed_node_for_hit(hit.kind.as_deref(), hit.external_id.as_deref())
                    .map(|node| node.id.0);
                let graph_context_path_count = graph_context_paths
                    .iter()
                    .filter(|path| path.seed_hit_id == hit.id)
                    .count();
                KnowledgeEvidence {
                    hit_id: hit.id.clone(),
                    kind: hit.kind.clone(),
                    external_id: hit.external_id.clone(),
                    source_id: hit.source_id.clone(),
                    canonical_node_id,
                    graph_context_path_count,
                    matched_terms: hit.matched_terms.clone(),
                    matched_spans: hit.matched_spans.clone(),
                    score: hit.score,
                    rrf_score: hit.rrf_score,
                    vector_rrf_score: hit.vector_rrf_score,
                    text_rrf_score: hit.text_rrf_score,
                    vector_score: hit.vector_score,
                    text_score: hit.text_score,
                    vector_rank: hit.vector_rank,
                    text_rank: hit.text_rank,
                }
            })
            .collect()
    }

    fn knowledge_candidates(
        &self,
        search: &SearchResultSet,
        evidence: &[KnowledgeEvidence],
        graph_seeds: &[KnowledgeGraphSeed],
        graph_context_paths: &[KnowledgeGraphContextPath],
        candidate_limit: Option<usize>,
        scoring: KnowledgeCandidateScoringPolicy,
    ) -> (
        Vec<KnowledgeCandidate>,
        usize,
        Vec<KnowledgeFanoutReasonDetail>,
    ) {
        let mut candidates = search
            .hits
            .iter()
            .zip(evidence.iter())
            .enumerate()
            .map(|(index, (hit, evidence))| {
                let score_breakdown =
                    knowledge_candidate_score_breakdown(Some(hit.score), None, scoring);
                KnowledgeCandidate {
                    id: hit.id.clone(),
                    canonical_node_id: evidence.canonical_node_id,
                    source: KnowledgeCandidateSource::SearchHit,
                    source_rank: index + 1,
                    merged_sources: vec![KnowledgeCandidateSource::SearchHit],
                    score: score_breakdown.combined_score,
                    score_breakdown,
                    entity: self
                        .seed_node_for_hit(hit.kind.as_deref(), hit.external_id.as_deref())
                        .map(|node| self.knowledge_entity_from_node(node)),
                    evidence: Some(evidence.clone()),
                    matched_properties: Vec::new(),
                    graph_context_path_count: evidence.graph_context_path_count,
                }
            })
            .collect::<Vec<_>>();

        for (index, seed) in graph_seeds.iter().enumerate() {
            let seed_candidate_id = graph_seed_candidate_id(seed);
            let seed_graph_context_path_count = graph_context_paths
                .iter()
                .filter(|path| path.seed_hit_id == seed_candidate_id)
                .count();
            if let Some(candidate) = candidates.iter_mut().find(|candidate| {
                candidate
                    .entity
                    .as_ref()
                    .is_some_and(|entity| entity.node_id == seed.entity.node_id)
            }) {
                candidate.score_breakdown = knowledge_candidate_score_breakdown(
                    candidate.score_breakdown.search_score,
                    Some(seed.score),
                    scoring,
                );
                candidate.score = candidate.score_breakdown.combined_score;
                if !candidate
                    .merged_sources
                    .contains(&KnowledgeCandidateSource::GraphSeed)
                {
                    candidate
                        .merged_sources
                        .push(KnowledgeCandidateSource::GraphSeed);
                }
                for property in &seed.matched_properties {
                    if !candidate.matched_properties.contains(property) {
                        candidate.matched_properties.push(property.clone());
                    }
                }
                candidate.graph_context_path_count += seed_graph_context_path_count;
                continue;
            }
            let score_breakdown =
                knowledge_candidate_score_breakdown(None, Some(seed.score), scoring);
            candidates.push(KnowledgeCandidate {
                id: seed_candidate_id,
                canonical_node_id: Some(seed.entity.node_id),
                source: KnowledgeCandidateSource::GraphSeed,
                source_rank: index + 1,
                merged_sources: vec![KnowledgeCandidateSource::GraphSeed],
                score: score_breakdown.combined_score,
                score_breakdown,
                entity: Some(seed.entity.clone()),
                evidence: None,
                matched_properties: seed.matched_properties.clone(),
                graph_context_path_count: seed_graph_context_path_count,
            });
        }

        candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_rank.cmp(&right.source_rank))
                .then_with(|| left.id.cmp(&right.id))
        });
        let total = candidates.len();
        let mut fanout_reasons = Vec::new();
        if let Some(limit) = candidate_limit {
            candidates.truncate(limit);
            if total > limit {
                fanout_reasons.push(KnowledgeFanoutReasonDetail::candidate_limit(limit, total));
            }
        }
        (candidates, total, fanout_reasons)
    }

    fn search_knowledge_graph_seeds(
        &self,
        query_text: &str,
        limit: usize,
        metadata_filters: &BTreeMap<String, String>,
    ) -> KnowledgeGraphSeedSearchOutput {
        if limit == 0 {
            return KnowledgeGraphSeedSearchOutput::default();
        }
        let query_terms = knowledge_query_terms(query_text);
        if query_terms.is_empty() {
            return KnowledgeGraphSeedSearchOutput::default();
        }
        let normalized_query = query_text.trim().to_ascii_lowercase();
        let mut input_candidate_count = 0usize;
        let mut input_filtered_out_count = 0usize;
        let mut scored = Vec::new();
        for node in self.store.scan_nodes(None) {
            if !knowledge_graph_seed_matches_filters(self.catalog, node, metadata_filters) {
                input_filtered_out_count += 1;
                continue;
            }
            input_candidate_count += 1;
            if let Some(seed) = {
                let (score, matched_properties) =
                    graph_seed_score(node, &query_terms, &normalized_query);
                (score > 0.0).then(|| KnowledgeGraphSeed {
                    entity: self.knowledge_entity_from_node(node),
                    score,
                    matched_properties,
                })
            } {
                scored.push(seed);
            }
        }

        scored.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.entity.node_id.cmp(&right.entity.node_id))
        });
        let total = scored.len();
        scored.truncate(limit);
        let fanout_reasons = if total > limit {
            vec![KnowledgeFanoutReasonDetail::graph_seed_limit(limit, total)]
        } else {
            Vec::new()
        };
        KnowledgeGraphSeedSearchOutput {
            seeds: scored,
            input_candidate_count,
            input_filtered_out_count,
            candidate_count: total,
            fanout_reason_details: fanout_reasons,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct KnowledgeGraphContextSearchOutput {
    paths: Vec<KnowledgeGraphContextPath>,
    input_seed_count: usize,
    expanded_relationship_count: usize,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    truncation_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct KnowledgeGraphSeedSearchOutput {
    seeds: Vec<KnowledgeGraphSeed>,
    input_candidate_count: usize,
    input_filtered_out_count: usize,
    candidate_count: usize,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
}

fn knowledge_retriever_reports(
    search: &SearchResultSet,
    evidence: &[KnowledgeEvidence],
    graph_seeds: &[KnowledgeGraphSeed],
    graph_context_paths: &[KnowledgeGraphContextPath],
    projection_freshness: &SearchProjectionFreshness,
    graph_seed_input: KnowledgeGraphSeedRetrieverInput,
) -> Vec<KnowledgeRetrieverReport> {
    let evidence_by_hit = evidence
        .iter()
        .map(|evidence| (evidence.hit_id.as_str(), evidence))
        .collect::<BTreeMap<_, _>>();
    let mut reports = search
        .retrievers
        .iter()
        .map(|report| KnowledgeRetrieverReport {
            name: report.name.clone(),
            available: report.available,
            input_candidate_set: report.input_candidate_set.clone(),
            candidate_count: report.candidate_count,
            candidate_set: report.candidate_set.clone(),
            limit: Some(search.limit),
            rank_window: search.rank_window,
            fusion_weight: knowledge_search_retriever_fusion_weight(
                report.name.as_str(),
                search.fusion_weights,
            ),
            fallback_reason_codes: report.fallback_reason_codes.clone(),
            knowledge_fallback_reason_codes: Vec::new(),
            fallback_reasons: report.fallback_reasons.clone(),
            truncated: report.candidate_count > report.top_candidates.len(),
            truncation_reason_codes: knowledge_search_retriever_truncation_reason_codes(
                report.candidate_count,
                report.top_candidates.len(),
                search.limit,
                search.rank_window,
            ),
            truncation_reasons: knowledge_search_retriever_truncation_reasons(
                report.name.as_str(),
                report.candidate_count,
                report.top_candidates.len(),
                search.limit,
                search.rank_window,
            ),
            top_candidates: report
                .top_candidates
                .iter()
                .map(|candidate| {
                    let evidence = evidence_by_hit.get(candidate.id.as_str()).copied();
                    KnowledgeRetrieverCandidate {
                        id: candidate.id.clone(),
                        kind: evidence.and_then(|evidence| evidence.kind.clone()),
                        external_id: evidence.and_then(|evidence| evidence.external_id.clone()),
                        source_id: evidence.and_then(|evidence| evidence.source_id.clone()),
                        canonical_node_id: evidence.and_then(|evidence| evidence.canonical_node_id),
                        rank: candidate.rank,
                        score: candidate.score,
                        matched_spans: evidence
                            .map(|evidence| evidence.matched_spans.clone())
                            .unwrap_or_default(),
                        graph_context_path_count: evidence
                            .map(|evidence| evidence.graph_context_path_count)
                            .unwrap_or_default(),
                        projection_freshness: Some(projection_freshness.clone()),
                    }
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    reports.push(KnowledgeRetrieverReport {
        name: "graph_seed".to_string(),
        available: graph_seed_input.limit > 0,
        input_candidate_set: knowledge_graph_seed_input_candidate_set_report(
            graph_seed_input.input_candidate_count,
            graph_seed_input.graph_commit_epoch,
            graph_seed_input.input_filtered_out_count,
            graph_seed_input.metadata_filters,
        ),
        candidate_count: graph_seed_input.candidate_count,
        candidate_set: knowledge_graph_seed_candidate_set_report(
            graph_seeds.len(),
            graph_seed_input.graph_commit_epoch,
        ),
        limit: Some(graph_seed_input.limit),
        rank_window: None,
        fusion_weight: None,
        fallback_reason_codes: Vec::new(),
        knowledge_fallback_reason_codes: knowledge_graph_seed_fallback_reason_codes(
            graph_seed_input.limit,
        ),
        fallback_reasons: knowledge_graph_seed_fallback_reasons(graph_seed_input.limit),
        truncated: graph_seed_input.candidate_count > graph_seeds.len(),
        truncation_reason_codes: knowledge_graph_seed_truncation_reason_codes(
            graph_seed_input.candidate_count,
            graph_seeds.len(),
        ),
        truncation_reasons: knowledge_graph_seed_truncation_reasons(
            graph_seed_input.candidate_count,
            graph_seeds.len(),
            graph_seed_input.limit,
        ),
        top_candidates: graph_seeds
            .iter()
            .enumerate()
            .map(|(index, seed)| {
                let id = graph_seed_candidate_id(seed);
                let graph_context_path_count = graph_context_paths
                    .iter()
                    .filter(|path| path.seed_hit_id == id)
                    .count();
                KnowledgeRetrieverCandidate {
                    id,
                    kind: seed.entity.labels.first().cloned(),
                    external_id: seed.entity.external_id.clone(),
                    source_id: None,
                    canonical_node_id: Some(seed.entity.node_id),
                    rank: index + 1,
                    score: seed.score,
                    matched_spans: Vec::new(),
                    graph_context_path_count,
                    projection_freshness: None,
                }
            })
            .collect(),
    });
    reports
}

#[derive(Debug, Clone)]
struct KnowledgeGraphSeedRetrieverInput {
    limit: usize,
    input_candidate_count: usize,
    input_filtered_out_count: usize,
    metadata_filters: BTreeMap<String, String>,
    candidate_count: usize,
    graph_commit_epoch: u64,
}

fn knowledge_search_retriever_truncation_reasons(
    name: &str,
    candidate_count: usize,
    returned_count: usize,
    search_limit: usize,
    rank_window: Option<usize>,
) -> Vec<String> {
    if candidate_count <= returned_count {
        return Vec::new();
    }
    let mut reasons = Vec::new();
    if let Some(rank_window) = rank_window {
        if candidate_count > rank_window && returned_count <= rank_window {
            reasons.push(format!(
                "{name} rank_window {rank_window} returned from {candidate_count} candidates"
            ));
        }
    }
    if returned_count >= search_limit && candidate_count > search_limit {
        reasons.push(format!(
            "{name} search_limit {search_limit} returned from {candidate_count} candidates"
        ));
    }
    if reasons.is_empty() {
        reasons.push(format!(
            "{name} returned {returned_count} of {candidate_count} candidates"
        ));
    }
    reasons
}

fn knowledge_search_retriever_truncation_reason_codes(
    candidate_count: usize,
    returned_count: usize,
    search_limit: usize,
    rank_window: Option<usize>,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_count <= returned_count {
        return Vec::new();
    }
    let mut codes = Vec::new();
    if let Some(rank_window) = rank_window {
        if candidate_count > rank_window && returned_count <= rank_window {
            codes.push(KnowledgeTruncationReasonCode::RankWindowExceeded);
        }
    }
    if returned_count >= search_limit && candidate_count > search_limit {
        codes.push(KnowledgeTruncationReasonCode::SearchLimitExceeded);
    }
    if codes.is_empty() {
        codes.push(KnowledgeTruncationReasonCode::PartialCandidateReturn);
    }
    codes
}

fn knowledge_search_retriever_fusion_weight(
    name: &str,
    weights: SearchFusionWeights,
) -> Option<f64> {
    match name {
        "vector" => Some(weights.vector_weight),
        "text" => Some(weights.text_weight),
        _ => None,
    }
}

fn knowledge_graph_seed_truncation_reasons(
    candidate_count: usize,
    returned_count: usize,
    graph_seed_limit: usize,
) -> Vec<String> {
    if candidate_count > returned_count {
        vec![format!(
            "graph_seed limit {graph_seed_limit} returned from {candidate_count} candidates"
        )]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_truncation_reason_codes(
    candidate_count: usize,
    returned_count: usize,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_count > returned_count {
        vec![KnowledgeTruncationReasonCode::GraphSeedLimitExceeded]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_fallback_reasons(graph_seed_limit: usize) -> Vec<String> {
    if graph_seed_limit == 0 {
        vec!["graph seed retriever disabled by limit 0".to_string()]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_fallback_reason_codes(
    graph_seed_limit: usize,
) -> Vec<KnowledgeFallbackReasonCode> {
    if graph_seed_limit == 0 {
        vec![KnowledgeFallbackReasonCode::GraphSeedLimitZero]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "ranked_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
    }
}

fn knowledge_graph_seed_input_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
    filtered_out_count: usize,
    metadata_filters: BTreeMap<String, String>,
) -> SearchCandidateSetReport {
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "filtered_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count,
        metadata_filters,
    }
}

fn knowledge_graph_context_input_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchCandidateSetReport {
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "context_seed_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count: 0,
        metadata_filters: BTreeMap::new(),
    }
}

fn knowledge_graph_context_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "canonical_graph_relationship_id".to_string(),
        representation: "expanded_relationship_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
    }
}

fn knowledge_graph_context_fallback_reasons(request: &KnowledgeRetrievalRequest) -> Vec<String> {
    let mut reasons = Vec::new();
    if request.graph_context_limit == 0 {
        reasons.push("graph context expansion disabled by limit 0".to_string());
    }
    if request.graph_context_max_hops == 0 {
        reasons.push("graph context expansion disabled by max_hops 0".to_string());
    }
    reasons
}

fn knowledge_graph_context_fallback_reason_codes(
    request: &KnowledgeRetrievalRequest,
) -> Vec<KnowledgeFallbackReasonCode> {
    let mut codes = Vec::new();
    if request.graph_context_limit == 0 {
        codes.push(KnowledgeFallbackReasonCode::GraphContextLimitZero);
    }
    if request.graph_context_max_hops == 0 {
        codes.push(KnowledgeFallbackReasonCode::GraphContextMaxHopsZero);
    }
    codes
}

#[derive(Debug, Clone)]
struct KnowledgeRetrievalDiagnosticsInput {
    graph_seed_input_candidate_set: SearchCandidateSetReport,
    graph_seed_candidate_set: SearchRetrieverCandidateSetReport,
    graph_seed_candidate_count: usize,
    graph_seed_returned_count: usize,
    graph_context_input_candidate_set: SearchCandidateSetReport,
    graph_context_candidate_set: SearchRetrieverCandidateSetReport,
    graph_context_path_count: usize,
    graph_context_node_count: usize,
    graph_context_relationship_count: usize,
    graph_context_truncation_reasons: Vec<String>,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    candidate_count: usize,
    candidate_total_count: usize,
}

fn knowledge_retrieval_diagnostics(
    search: &SearchResultSet,
    request: &KnowledgeRetrievalRequest,
    projection_freshness: &SearchProjectionFreshness,
    graph_commit_epoch: u64,
    input: KnowledgeRetrievalDiagnosticsInput,
) -> KnowledgeRetrievalDiagnostics {
    let mut empty_reasons = Vec::new();
    let mut empty_reason_codes = Vec::new();
    let candidate_truncation_reasons = knowledge_candidate_truncation_reasons(
        input.candidate_total_count,
        input.candidate_count,
        request.candidate_limit,
    );
    let candidate_truncation_reason_codes = knowledge_candidate_truncation_reason_codes(
        input.candidate_total_count,
        input.candidate_count,
    );
    if input.candidate_count == 0 {
        empty_reason_codes.extend(
            search
                .empty_reason_codes
                .iter()
                .map(knowledge_empty_reason_code_from_search),
        );
        empty_reasons.extend(search.empty_reasons.iter().cloned());
        if input.candidate_total_count == 0 {
            if request.graph_seed_limit == 0 {
                empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero);
                empty_reasons.push("graph seed retriever disabled by limit 0".to_string());
            } else if input.graph_seed_candidate_count == 0 {
                empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::GraphSeedNoCandidates);
                empty_reasons.push("graph seed retriever returned no candidates".to_string());
            }
        }
        if !candidate_truncation_reasons.is_empty() {
            empty_reason_codes
                .push(KnowledgeRetrievalEmptyReasonCode::CandidateLimitExcludedAllCandidates);
        }
        empty_reasons.extend(candidate_truncation_reasons.iter().cloned());
        empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::NoCandidates);
        empty_reasons.push("retrieval produced no candidates".to_string());
    }
    let graph_seed_truncation_reasons = knowledge_graph_seed_truncation_reasons(
        input.graph_seed_candidate_count,
        input.graph_seed_returned_count,
        request.graph_seed_limit,
    );
    let graph_seed_truncation_reason_codes = knowledge_graph_seed_truncation_reason_codes(
        input.graph_seed_candidate_count,
        input.graph_seed_returned_count,
    );
    let graph_context_truncation_reason_codes =
        knowledge_graph_context_truncation_reason_codes(&input.graph_context_truncation_reasons);
    KnowledgeRetrievalDiagnostics {
        graph_commit_epoch,
        projection_source_graph_commit_epoch: projection_freshness.source_graph_commit_epoch,
        projection_commit_lag: search_projection_freshness_commit_lag(
            projection_freshness,
            graph_commit_epoch,
        ),
        projection_stale: search_projection_is_stale(projection_freshness, graph_commit_epoch),
        projection_full_reindex_needed: projection_freshness.full_reindex_needed,
        projection_full_reindex_reasons: projection_freshness.full_reindex_reasons.clone(),
        projection_metadata_repair_needed: projection_freshness.metadata_repair_needed,
        projection_metadata_repair_reasons: projection_freshness.metadata_repair_reasons.clone(),
        search_document_count: search.document_count,
        search_filtered_document_count: search.filtered_document_count,
        search_total_hits: search.total_hits,
        search_candidate_set: search.candidate_set.clone(),
        search_candidate_filtered_out_count: search.candidate_set.filtered_out_count,
        search_limit: search.limit,
        search_truncated: search.truncated,
        search_truncation_reason_codes: search.truncation_reason_codes.clone(),
        search_truncation_reasons: search.truncation_reasons.clone(),
        search_fallback_reason_codes: search.fallback_reason_codes.clone(),
        search_fallback_reasons: search.fallback_reasons.clone(),
        rank_window: search.rank_window,
        search_fusion_weights: search.fusion_weights,
        graph_seed_input_candidate_set: input.graph_seed_input_candidate_set,
        graph_seed_candidate_set: input.graph_seed_candidate_set,
        graph_seed_candidate_count: input.graph_seed_candidate_count,
        graph_seed_returned_count: input.graph_seed_returned_count,
        graph_seed_limit: request.graph_seed_limit,
        graph_seed_truncated: !graph_seed_truncation_reasons.is_empty(),
        graph_seed_truncation_reason_codes,
        graph_seed_truncation_reasons,
        graph_context_input_candidate_set: input.graph_context_input_candidate_set,
        graph_context_candidate_set: input.graph_context_candidate_set,
        graph_context_path_count: input.graph_context_path_count,
        graph_context_node_count: input.graph_context_node_count,
        graph_context_relationship_count: input.graph_context_relationship_count,
        graph_context_limit: request.graph_context_limit,
        graph_context_max_hops: request.graph_context_max_hops,
        graph_context_truncated: !input.graph_context_truncation_reasons.is_empty(),
        graph_context_truncation_reason_codes,
        graph_context_truncation_reasons: input.graph_context_truncation_reasons,
        graph_context_fallback_reason_codes: knowledge_graph_context_fallback_reason_codes(request),
        graph_context_fallback_reasons: knowledge_graph_context_fallback_reasons(request),
        fanout_reason_count: input.fanout_reason_details.len(),
        fanout_reason_codes: knowledge_fanout_reason_codes(&input.fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&input.fanout_reason_details),
        fanout_reason_details: input.fanout_reason_details,
        candidate_count: input.candidate_count,
        candidate_total_count: input.candidate_total_count,
        candidate_limit: request.candidate_limit,
        candidate_truncated: !candidate_truncation_reasons.is_empty(),
        candidate_truncation_reason_codes,
        candidate_truncation_reasons,
        warnings: knowledge_retrieval_warnings(projection_freshness, graph_commit_epoch),
        empty_reason_codes,
        empty_reasons,
    }
}

fn knowledge_fanout_reason_codes(
    details: &[KnowledgeFanoutReasonDetail],
) -> Vec<KnowledgeFanoutReasonCode> {
    details.iter().map(|detail| detail.code).collect()
}

fn knowledge_fanout_reason_messages(details: &[KnowledgeFanoutReasonDetail]) -> Vec<String> {
    details
        .iter()
        .map(|detail| detail.message.clone())
        .collect()
}

fn knowledge_empty_reason_code_from_search(
    code: &SearchEmptyReasonCode,
) -> KnowledgeRetrievalEmptyReasonCode {
    match code {
        SearchEmptyReasonCode::ProjectionEmpty => {
            KnowledgeRetrievalEmptyReasonCode::SearchProjectionEmpty
        }
        SearchEmptyReasonCode::MetadataFilterEmpty => {
            KnowledgeRetrievalEmptyReasonCode::SearchMetadataFilterEmpty
        }
        SearchEmptyReasonCode::RetrieverNoHits => {
            KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits
        }
        SearchEmptyReasonCode::LimitExcludedAllHits => {
            KnowledgeRetrievalEmptyReasonCode::SearchLimitExcludedAllHits
        }
    }
}

fn knowledge_candidate_truncation_reasons(
    candidate_total_count: usize,
    candidate_count: usize,
    candidate_limit: Option<usize>,
) -> Vec<String> {
    match candidate_limit {
        Some(limit) if candidate_total_count > candidate_count => vec![format!(
            "knowledge_candidate_limit {limit} returned from {candidate_total_count} merged candidates"
        )],
        _ => Vec::new(),
    }
}

fn knowledge_candidate_truncation_reason_codes(
    candidate_total_count: usize,
    candidate_count: usize,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_total_count > candidate_count {
        vec![KnowledgeTruncationReasonCode::CandidateLimitExceeded]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_context_truncation_reason_codes(
    truncation_reasons: &[String],
) -> Vec<KnowledgeTruncationReasonCode> {
    if truncation_reasons.is_empty() {
        Vec::new()
    } else {
        vec![KnowledgeTruncationReasonCode::GraphContextLimitExceeded]
    }
}

fn knowledge_retrieval_warnings(
    projection_freshness: &SearchProjectionFreshness,
    graph_commit_epoch: u64,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if search_projection_is_stale(projection_freshness, graph_commit_epoch) {
        warnings.push("search projection is older than graph snapshot".to_string());
    }
    if projection_freshness.full_reindex_needed {
        warnings.push("search projection requires full reindex".to_string());
        warnings.extend(
            projection_freshness
                .full_reindex_reasons
                .iter()
                .map(|reason| format!("search projection full reindex reason: {reason}")),
        );
    }
    if projection_freshness.metadata_repair_needed {
        warnings.push("search projection metadata repair is needed".to_string());
        warnings.extend(
            projection_freshness
                .metadata_repair_reasons
                .iter()
                .map(|reason| format!("search projection metadata repair reason: {reason}")),
        );
    }
    warnings
}

fn search_projection_is_stale(
    projection_freshness: &SearchProjectionFreshness,
    graph_commit_epoch: u64,
) -> bool {
    projection_freshness
        .source_graph_commit_epoch
        .map(|projection_epoch| projection_epoch < graph_commit_epoch)
        .unwrap_or(false)
}

fn search_projection_freshness_commit_lag(
    projection_freshness: &SearchProjectionFreshness,
    graph_commit_epoch: u64,
) -> u64 {
    graph_commit_epoch.saturating_sub(projection_freshness.source_graph_commit_epoch.unwrap_or(0))
}

fn graph_seed_candidate_id(seed: &KnowledgeGraphSeed) -> String {
    let label = seed
        .entity
        .labels
        .first()
        .map(String::as_str)
        .unwrap_or("node");
    match seed.entity.external_id.as_deref() {
        Some(external_id) if !external_id.is_empty() => format!("{label}:{external_id}"),
        _ => format!("node:{}", seed.entity.node_id),
    }
}

fn knowledge_candidate_score_breakdown(
    search_score: Option<f64>,
    graph_seed_score: Option<f64>,
    scoring: KnowledgeCandidateScoringPolicy,
) -> KnowledgeCandidateScoreBreakdown {
    let combined_score = match scoring {
        KnowledgeCandidateScoringPolicy::Max => search_score
            .into_iter()
            .chain(graph_seed_score)
            .fold(0.0, f64::max),
        KnowledgeCandidateScoringPolicy::WeightedSum {
            search_weight,
            graph_seed_weight,
        } => {
            search_score.unwrap_or(0.0) * search_weight
                + graph_seed_score.unwrap_or(0.0) * graph_seed_weight
        }
    };
    KnowledgeCandidateScoreBreakdown {
        search_score,
        graph_seed_score,
        combined_score,
    }
}

fn graph_seed_score(
    node: &NodeRecord,
    query_terms: &BTreeSet<String>,
    normalized_query: &str,
) -> (f64, Vec<String>) {
    const GRAPH_SEED_PROPERTIES: &[&str] =
        &["id", "title", "name", "summary", "content", "body", "text"];
    let mut score = 0.0;
    let mut matched_properties = Vec::new();
    for property in GRAPH_SEED_PROPERTIES {
        let Some(value) = node.properties.get(*property) else {
            continue;
        };
        let text = value_to_external_id(value);
        let normalized_text = text.to_ascii_lowercase();
        let property_terms = knowledge_query_terms(&text);
        let matched_term_count = query_terms
            .iter()
            .filter(|term| property_terms.contains(*term))
            .count();
        let exact_match = !normalized_query.is_empty() && normalized_text == normalized_query;
        let contains_query =
            !normalized_query.is_empty() && normalized_text.contains(normalized_query);
        if matched_term_count > 0 || exact_match || contains_query {
            matched_properties.push((*property).to_string());
            score += matched_term_count as f64;
            if contains_query {
                score += 2.0;
            }
            if *property == "id" && exact_match {
                score += 8.0;
            }
        }
    }
    (score, matched_properties)
}

fn knowledge_graph_seed_matches_filters(
    catalog: &Catalog,
    node: &NodeRecord,
    metadata_filters: &BTreeMap<String, String>,
) -> bool {
    metadata_filters
        .iter()
        .all(|(key, value)| knowledge_graph_seed_matches_filter(catalog, node, key, value))
}

fn knowledge_graph_seed_matches_filter(
    catalog: &Catalog,
    node: &NodeRecord,
    key: &str,
    value: &str,
) -> bool {
    match key {
        "kind" => {
            let Some(label) = search_kind_to_label(value) else {
                return false;
            };
            catalog
                .label_id(label)
                .is_some_and(|label_id| node.labels.contains(&label_id))
        }
        "external_id" => projected_node_external_id(node) == value,
        "source_id" => node_projection_source_id(node).as_deref() == Some(value),
        "space_id" => normalized_node_space_id(node) == value,
        _ => node
            .properties
            .get(key)
            .is_some_and(|property| value_to_external_id(property) == value),
    }
}

fn normalized_node_space_id(node: &NodeRecord) -> String {
    node.properties
        .get("space_id")
        .map(value_to_external_id)
        .filter(|space_id| !space_id.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

fn node_projection_source_id(node: &NodeRecord) -> Option<String> {
    ["source_id", "thread_id", "source"]
        .into_iter()
        .filter_map(|key| node.properties.get(key).map(value_to_external_id))
        .find(|source_id| !source_id.is_empty())
}

fn knowledge_query_terms(text: &str) -> BTreeSet<String> {
    text.split(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn knowledge_entity_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeEntityRequest,
) -> KnowledgeEntityOutput {
    let entity = match knowledge_scoped_entity_match(catalog, store, request, &BTreeMap::new()) {
        KnowledgeScopedEntityMatch::Found(entity) => Some(entity),
        KnowledgeScopedEntityMatch::Missing | KnowledgeScopedEntityMatch::FilteredOut => None,
    };
    KnowledgeEntityOutput {
        graph_commit_epoch: store.commit_epoch(),
        entity,
    }
}

fn knowledge_scoped_entity_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeScopedEntityRequest,
) -> KnowledgeEntityOutput {
    let entity = match knowledge_scoped_entity_match(
        catalog,
        store,
        &request.entity,
        &request.metadata_filters,
    ) {
        KnowledgeScopedEntityMatch::Found(entity) => Some(entity),
        KnowledgeScopedEntityMatch::Missing | KnowledgeScopedEntityMatch::FilteredOut => None,
    };
    KnowledgeEntityOutput {
        graph_commit_epoch: store.commit_epoch(),
        entity,
    }
}

fn knowledge_entity_batch_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeEntityBatchRequest,
) -> KnowledgeEntityBatchOutput {
    knowledge_scoped_entity_batch_for(
        catalog,
        store,
        &KnowledgeScopedEntityBatchRequest {
            entities: request.entities.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn knowledge_scoped_entity_batch_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeScopedEntityBatchRequest,
) -> KnowledgeEntityBatchOutput {
    let mut entities = Vec::with_capacity(request.entities.len());
    let mut found_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    for entity_request in &request.entities {
        match knowledge_scoped_entity_match(
            catalog,
            store,
            entity_request,
            &request.metadata_filters,
        ) {
            KnowledgeScopedEntityMatch::Found(entity) => {
                found_count += 1;
                entities.push(Some(entity));
            }
            KnowledgeScopedEntityMatch::Missing => {
                missing_count += 1;
                entities.push(None);
            }
            KnowledgeScopedEntityMatch::FilteredOut => {
                filtered_out_count += 1;
                entities.push(None);
            }
        }
    }
    KnowledgeEntityBatchOutput {
        graph_commit_epoch: store.commit_epoch(),
        entities,
        found_count,
        missing_count,
        filtered_out_count,
    }
}

fn knowledge_memory_entities_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeMemoryEntityListRequest,
) -> Result<KnowledgeMemoryEntityListOutput> {
    if request.memory_ids.is_empty() || request.memory_ids.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge memory entity read requires non-empty memory ids".to_string(),
        ));
    }

    let mut groups = Vec::with_capacity(request.memory_ids.len());
    let mut distinct_entity_names = BTreeSet::new();
    let mut found_memory_count = 0;
    let mut missing_memory_count = 0;
    let mut entity_count = 0;

    for memory_id in &request.memory_ids {
        let Some(memory) = seed_node_by_label_and_external_id(catalog, store, "Memory", memory_id)
        else {
            missing_memory_count += 1;
            groups.push(KnowledgeMemoryEntityGroup {
                memory_id: memory_id.clone(),
                memory_node_id: None,
                found: false,
                entities: Vec::new(),
                matched_count: 0,
                returned_count: 0,
            });
            continue;
        };

        found_memory_count += 1;
        let mut rows = memory_entity_rows(catalog, store, memory.id);
        let matched_count = rows.len();
        for name in rows
            .iter()
            .filter_map(|row| row.name.as_ref())
            .filter(|name| !name.is_empty())
        {
            distinct_entity_names.insert(name.clone());
        }
        if request.limit_per_memory > 0 {
            rows.truncate(request.limit_per_memory);
        }
        entity_count += rows.len();
        groups.push(KnowledgeMemoryEntityGroup {
            memory_id: memory_id.clone(),
            memory_node_id: Some(memory.id.0),
            found: true,
            matched_count,
            returned_count: rows.len(),
            entities: rows,
        });
    }

    let mut distinct_entity_names = distinct_entity_names.into_iter().collect::<Vec<_>>();
    if request.distinct_name_limit > 0 {
        distinct_entity_names.truncate(request.distinct_name_limit);
    }

    Ok(KnowledgeMemoryEntityListOutput {
        graph_commit_epoch: store.commit_epoch(),
        groups,
        distinct_entity_names,
        found_memory_count,
        missing_memory_count,
        entity_count,
    })
}

fn memory_entity_rows(
    catalog: &Catalog,
    store: &GraphStore,
    memory_node_id: NodeId,
) -> Vec<KnowledgeMemoryEntityRow> {
    let Some(rel_type_id) = catalog.rel_type_id("MENTIONS") else {
        return Vec::new();
    };
    let Some(entity_label_id) = catalog.label_id("Entity") else {
        return Vec::new();
    };
    let mut rows = store
        .outgoing_relationships(memory_node_id, rel_type_id)
        .filter_map(|relationship| {
            store
                .node(relationship.target)
                .filter(|entity| entity.labels.contains(&entity_label_id))
                .map(|entity| memory_entity_row(entity, relationship))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.entity_id.cmp(&right.entity_id))
            .then_with(|| left.relationship_id.cmp(&right.relationship_id))
    });
    rows
}

fn memory_entity_row(entity: &NodeRecord, relationship: &RelRecord) -> KnowledgeMemoryEntityRow {
    KnowledgeMemoryEntityRow {
        entity_id: node_external_id(entity),
        node_id: entity.id.0,
        relationship_id: relationship.id.0,
        name: string_property(entity, "name"),
        entity_type: string_property(entity, "entity_type"),
        confidence: entity.properties.get("confidence").cloned(),
        relationship_confidence: relationship.properties.get("confidence").cloned(),
        mention_count: relationship_integer_property(relationship, "mention_count"),
    }
}

fn knowledge_context_memory_preview_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeContextMemoryPreviewRequest,
) -> Result<KnowledgeContextMemoryPreviewOutput> {
    if request.unit_types.is_empty() || request.unit_types.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge context memory preview requires non-empty unit types".to_string(),
        ));
    }

    let graph_commit_epoch = store.commit_epoch();
    let Some(memory_label_id) = catalog.label_id("Memory") else {
        return Ok(KnowledgeContextMemoryPreviewOutput {
            graph_commit_epoch,
            rows: Vec::new(),
            matched_memory_count: 0,
            returned_count: 0,
        });
    };
    let unit_types = request.unit_types.iter().collect::<BTreeSet<_>>();
    let mut memories = store
        .scan_nodes(Some(memory_label_id))
        .filter(|memory| context_memory_matches_preview(memory, &unit_types, request))
        .collect::<Vec<_>>();
    memories.sort_by(|left, right| {
        compare_skill_memory_created_at(
            &left.properties.get("created_at").cloned(),
            &right.properties.get("created_at").cloned(),
            KnowledgeSkillMemoryListOrder::CreatedAtDesc,
        )
        .then_with(|| node_external_id(left).cmp(&node_external_id(right)))
        .then_with(|| left.id.0.cmp(&right.id.0))
    });
    let matched_memory_count = memories.len();

    let mut rows = if request.include_labels {
        context_memory_label_preview_rows(catalog, store, memories)
    } else {
        memories
            .into_iter()
            .map(|memory| context_memory_preview_row(memory, None))
            .collect::<Vec<_>>()
    };
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeContextMemoryPreviewOutput {
        graph_commit_epoch,
        rows,
        matched_memory_count,
        returned_count,
    })
}

fn context_memory_matches_preview(
    memory: &NodeRecord,
    unit_types: &BTreeSet<&String>,
    request: &KnowledgeContextMemoryPreviewRequest,
) -> bool {
    memory
        .properties
        .get("unit_type")
        .map(value_to_external_id)
        .as_ref()
        .is_some_and(|unit_type| unit_types.contains(unit_type))
        && context_memory_matches_latest(memory, request.latest_filter)
        && context_memory_is_not_crystal(memory)
}

fn context_memory_matches_latest(
    memory: &NodeRecord,
    filter: KnowledgeContextMemoryLatestFilter,
) -> bool {
    match filter {
        KnowledgeContextMemoryLatestFilter::NullOrTrue => {
            !matches!(memory.properties.get("is_latest"), Some(Value::Bool(false)))
        }
        KnowledgeContextMemoryLatestFilter::TrueOnly => {
            memory.properties.get("is_latest") == Some(&Value::Bool(true))
        }
    }
}

fn context_memory_is_not_crystal(memory: &NodeRecord) -> bool {
    !matches!(memory.properties.get("is_crystal"), Some(Value::Bool(true)))
}

fn context_memory_label_preview_rows(
    catalog: &Catalog,
    store: &GraphStore,
    memories: Vec<&NodeRecord>,
) -> Vec<KnowledgeContextMemoryPreviewRow> {
    let Some(rel_type_id) = catalog.rel_type_id("HAS_LABEL") else {
        return Vec::new();
    };
    let Some(label_label_id) = catalog.label_id("Label") else {
        return Vec::new();
    };
    memories
        .into_iter()
        .flat_map(|memory| {
            let mut labels = store
                .outgoing_relationships(memory.id, rel_type_id)
                .filter_map(|relationship| {
                    store
                        .node(relationship.target)
                        .filter(|label| label.labels.contains(&label_label_id))
                })
                .collect::<Vec<_>>();
            labels.sort_by(|left, right| {
                string_property(left, "canonical_name")
                    .cmp(&string_property(right, "canonical_name"))
                    .then_with(|| {
                        string_property(left, "name").cmp(&string_property(right, "name"))
                    })
                    .then_with(|| node_external_id(left).cmp(&node_external_id(right)))
                    .then_with(|| left.id.0.cmp(&right.id.0))
            });
            labels
                .into_iter()
                .map(|label| context_memory_preview_row(memory, Some(label)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn context_memory_preview_row(
    memory: &NodeRecord,
    label: Option<&NodeRecord>,
) -> KnowledgeContextMemoryPreviewRow {
    KnowledgeContextMemoryPreviewRow {
        memory_id: node_external_id(memory),
        memory_node_id: memory.id.0,
        title: string_property(memory, "title"),
        unit_type: string_property(memory, "unit_type"),
        created_at: memory.properties.get("created_at").cloned(),
        label_id: label.and_then(node_external_id),
        label_node_id: label.map(|label| label.id.0),
        label_canonical_name: label.and_then(|label| string_property(label, "canonical_name")),
        label_name: label.and_then(|label| string_property(label, "name")),
    }
}

fn knowledge_memories_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeMemoryListRequest,
) -> Result<KnowledgeMemoryListOutput> {
    validate_knowledge_memory_list_request(request)?;
    let graph_commit_epoch = store.commit_epoch();
    let Some(memory_label_id) = catalog.label_id("Memory") else {
        return Ok(KnowledgeMemoryListOutput {
            graph_commit_epoch,
            rows: Vec::new(),
            matched_count: 0,
            returned_count: 0,
            missing_external_ids: request.external_ids.clone(),
        });
    };

    let requested_ids = request
        .external_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut matched_external_ids = BTreeSet::new();
    let mut rows = store
        .scan_nodes(Some(memory_label_id))
        .filter(|memory| {
            if requested_ids.is_empty() {
                true
            } else {
                node_external_id(memory).is_some_and(|memory_id| requested_ids.contains(&memory_id))
            }
        })
        .filter(|memory| memory_matches_memory_list(memory, request))
        .map(|memory| {
            if let Some(memory_id) = node_external_id(memory) {
                matched_external_ids.insert(memory_id);
            }
            knowledge_memory_list_row(memory)
        })
        .collect::<Vec<_>>();
    sort_memory_list_rows(&mut rows, request.order);
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    let mut missing_external_ids = Vec::new();
    let mut seen_missing = BTreeSet::new();
    for external_id in &request.external_ids {
        if !matched_external_ids.contains(external_id) && seen_missing.insert(external_id.clone()) {
            missing_external_ids.push(external_id.clone());
        }
    }

    Ok(KnowledgeMemoryListOutput {
        graph_commit_epoch,
        rows,
        matched_count,
        returned_count,
        missing_external_ids,
    })
}

fn validate_knowledge_memory_list_request(request: &KnowledgeMemoryListRequest) -> Result<()> {
    if request.external_ids.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge memory list requires non-empty external ids".to_string(),
        ));
    }
    if request
        .normalized_space_id
        .as_ref()
        .is_some_and(String::is_empty)
        || request
            .exclude_normalized_space_id
            .as_ref()
            .is_some_and(String::is_empty)
    {
        return Err(SkeinError::Semantic(
            "knowledge memory list requires non-empty normalized space ids".to_string(),
        ));
    }
    if request.unit_type.as_ref().is_some_and(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge memory list requires a non-empty unit type".to_string(),
        ));
    }
    if request.external_ids.is_empty()
        && request.normalized_space_id.is_none()
        && request.exclude_normalized_space_id.is_none()
        && request.unit_type.is_none()
        && request.is_latest.is_none()
        && request.is_crystal.is_none()
        && request.limit == 0
    {
        return Err(SkeinError::Semantic(
            "knowledge memory list requires a filter or bounded limit".to_string(),
        ));
    }
    Ok(())
}

fn memory_matches_memory_list(memory: &NodeRecord, request: &KnowledgeMemoryListRequest) -> bool {
    request
        .normalized_space_id
        .as_ref()
        .is_none_or(|space_id| normalized_node_space_id(memory) == *space_id)
        && request
            .exclude_normalized_space_id
            .as_ref()
            .is_none_or(|space_id| normalized_node_space_id(memory) != *space_id)
        && request.unit_type.as_ref().is_none_or(|unit_type| {
            memory
                .properties
                .get("unit_type")
                .map(value_to_external_id)
                .as_ref()
                == Some(unit_type)
        })
        && request.is_latest.is_none_or(|is_latest| {
            memory.properties.get("is_latest") == Some(&Value::Bool(is_latest))
        })
        && request.is_crystal.is_none_or(|is_crystal| {
            memory.properties.get("is_crystal") == Some(&Value::Bool(is_crystal))
        })
}

fn knowledge_memory_list_row(memory: &NodeRecord) -> KnowledgeMemoryListRow {
    KnowledgeMemoryListRow {
        memory_id: node_external_id(memory),
        node_id: memory.id.0,
        title: string_property(memory, "title"),
        content: string_property(memory, "content"),
        metadata: memory.properties.get("metadata").cloned(),
        is_latest: boolean_property(memory, "is_latest"),
        lifecycle_state: string_property(memory, "lifecycle_state"),
        review_status: string_property(memory, "review_status"),
        unit_type: string_property(memory, "unit_type"),
        raw_space_id: memory.properties.get("space_id").map(value_to_external_id),
        normalized_space_id: normalized_node_space_id(memory),
        created_at: memory.properties.get("created_at").cloned(),
        updated_at: memory.properties.get("updated_at").cloned(),
        importance: memory.properties.get("importance").cloned(),
        pagerank_score: memory.properties.get("pagerank_score").cloned(),
        community_id: memory.properties.get("community_id").cloned(),
        source: string_property(memory, "source"),
        event_start: memory.properties.get("event_start").cloned(),
        event_end: memory.properties.get("event_end").cloned(),
    }
}

fn boolean_property(node: &NodeRecord, property: &str) -> Option<bool> {
    match node.properties.get(property) {
        Some(Value::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn sort_memory_list_rows(rows: &mut [KnowledgeMemoryListRow], order: KnowledgeMemoryListOrder) {
    rows.sort_by(|left, right| match order {
        KnowledgeMemoryListOrder::ExternalIdAsc => compare_memory_ids(left, right),
        KnowledgeMemoryListOrder::CreatedAtDesc => compare_skill_memory_created_at(
            &left.created_at,
            &right.created_at,
            KnowledgeSkillMemoryListOrder::CreatedAtDesc,
        )
        .then_with(|| compare_memory_ids(left, right)),
        KnowledgeMemoryListOrder::ScoreDesc => compare_memory_scores(left, right)
            .then_with(|| {
                compare_skill_memory_created_at(
                    &left.created_at,
                    &right.created_at,
                    KnowledgeSkillMemoryListOrder::CreatedAtDesc,
                )
            })
            .then_with(|| compare_memory_ids(left, right)),
    });
}

fn compare_memory_ids(
    left: &KnowledgeMemoryListRow,
    right: &KnowledgeMemoryListRow,
) -> std::cmp::Ordering {
    left.memory_id
        .cmp(&right.memory_id)
        .then_with(|| left.node_id.cmp(&right.node_id))
}

fn compare_memory_scores(
    left: &KnowledgeMemoryListRow,
    right: &KnowledgeMemoryListRow,
) -> std::cmp::Ordering {
    compare_skill_memory_values(&memory_score(right), &memory_score(left))
}

fn memory_score(row: &KnowledgeMemoryListRow) -> Value {
    row.pagerank_score
        .clone()
        .or_else(|| row.importance.clone())
        .unwrap_or(Value::Float(0.5))
}

enum KnowledgeScopedEntityMatch {
    Found(KnowledgeEntity),
    Missing,
    FilteredOut,
}

fn knowledge_scoped_entity_match(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeEntityRequest,
    metadata_filters: &BTreeMap<String, String>,
) -> KnowledgeScopedEntityMatch {
    let Some(node) = seed_node_by_label_and_external_id(
        catalog,
        store,
        request.label.as_str(),
        request.external_id.as_str(),
    ) else {
        return KnowledgeScopedEntityMatch::Missing;
    };
    if !metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(catalog, node, metadata_filters)
    {
        return KnowledgeScopedEntityMatch::FilteredOut;
    }
    KnowledgeScopedEntityMatch::Found(knowledge_entity_from_node(catalog, node))
}

fn create_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeEntityCreateRequest,
) -> Result<KnowledgeEntityCreateOutput> {
    db.ensure_writable()?;
    validate_knowledge_entity_create(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    if let Some(existing) = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    ) {
        return Ok(KnowledgeEntityCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(existing.id.0),
            created: false,
            already_exists: true,
            created_node_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_entity_create_statement(request);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let created_node_count = output.rows.len();
    let created = created_node_count > 0;
    let node_id = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    )
    .map(|node| node.id.0);
    Ok(KnowledgeEntityCreateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id,
        created,
        already_exists: false,
        created_node_count,
    })
}

fn create_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeEntityCreateBatchRequest,
) -> Result<KnowledgeEntityCreateBatchOutput> {
    db.ensure_writable()?;
    for create in &request.creates {
        validate_knowledge_entity_create(create)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.creates.len());
    let mut created_count = 0;
    let mut already_exists_count = 0;
    let mut eligible_creates = Vec::new();
    let mut pending_identities = BTreeSet::new();

    for create in &request.creates {
        if let Some(existing) = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            create.label.as_str(),
            create.external_id.as_str(),
        ) {
            already_exists_count += 1;
            rows.push(KnowledgeEntityCreateBatchRow {
                label: create.label.clone(),
                external_id: create.external_id.clone(),
                node_id: Some(existing.id.0),
                created: false,
                already_exists: true,
            });
            continue;
        }
        let identity = (create.label.clone(), create.external_id.clone());
        if !pending_identities.insert(identity) {
            already_exists_count += 1;
            rows.push(KnowledgeEntityCreateBatchRow {
                label: create.label.clone(),
                external_id: create.external_id.clone(),
                node_id: None,
                created: false,
                already_exists: true,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(create.clone());
        rows.push(KnowledgeEntityCreateBatchRow {
            label: create.label.clone(),
            external_id: create.external_id.clone(),
            node_id: None,
            created: true,
            already_exists: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeEntityCreateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count,
            already_exists_count,
            created_node_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    for row in &mut rows {
        if row.created {
            row.node_id = seed_node_by_label_and_external_id(
                &db.catalog,
                &db.store,
                row.label.as_str(),
                row.external_id.as_str(),
            )
            .map(|node| node.id.0);
        }
    }
    Ok(KnowledgeEntityCreateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        already_exists_count,
        created_node_count: output.rows.len(),
    })
}

fn validate_knowledge_entity_create(request: &KnowledgeEntityCreateRequest) -> Result<()> {
    validate_cypher_identifier(&request.label, "label")?;
    if request.external_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge entity create requires a non-empty external id".to_string(),
        ));
    }
    for property in request.properties.keys() {
        validate_cypher_identifier(property, "property")?;
    }
    if let Some(id) = request.properties.get("id") {
        let property_external_id = value_to_external_id(id);
        if property_external_id != request.external_id {
            return Err(SkeinError::Semantic(format!(
                "knowledge entity create id property {property_external_id:?} does not match external id {:?}",
                request.external_id
            )));
        }
    }
    Ok(())
}

fn knowledge_entity_create_statement(
    request: &KnowledgeEntityCreateRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!("CREATE (:{} {{id: $external_id", request.label);
    let mut parameters = BTreeMap::from([(
        "external_id".to_string(),
        Value::String(request.external_id.clone()),
    )]);
    for (index, (property, value)) in request
        .properties
        .iter()
        .filter(|(property, _)| property.as_str() != "id")
        .enumerate()
    {
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!(", {property}: ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    cypher.push_str("})");
    (cypher, parameters)
}

fn upsert_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeEntityUpsertRequest,
) -> Result<KnowledgeEntityUpsertOutput> {
    db.ensure_writable()?;
    validate_knowledge_entity_upsert(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let update_properties = knowledge_entity_upsert_update_properties(request);
    if let Some(existing) = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    ) {
        let node_id = existing.id.0;
        if !node_has_external_id_property(existing, request.external_id.as_str()) {
            return Ok(KnowledgeEntityUpsertOutput {
                graph_commit_epoch_before,
                graph_commit_epoch_after: graph_commit_epoch_before,
                node_id: Some(node_id),
                created: false,
                updated: false,
                already_exists: true,
                non_writable: true,
                created_node_count: 0,
                updated_property_count: 0,
            });
        }
        if update_properties.is_empty() {
            return Ok(KnowledgeEntityUpsertOutput {
                graph_commit_epoch_before,
                graph_commit_epoch_after: graph_commit_epoch_before,
                node_id: Some(node_id),
                created: false,
                updated: false,
                already_exists: true,
                non_writable: false,
                created_node_count: 0,
                updated_property_count: 0,
            });
        }
        let (cypher, parameters) = knowledge_property_update_statement(
            request.label.as_str(),
            node_id,
            &update_properties,
        );
        db.query_with_params(cypher.as_str(), &parameters)?;
        return Ok(KnowledgeEntityUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: db.store.commit_epoch(),
            node_id: Some(node_id),
            created: false,
            updated: true,
            already_exists: true,
            non_writable: false,
            created_node_count: 0,
            updated_property_count: update_properties.len(),
        });
    }

    let create = knowledge_entity_upsert_create_request(request);
    let (cypher, parameters) = knowledge_entity_create_statement(&create);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let node_id = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    )
    .map(|node| node.id.0);
    Ok(KnowledgeEntityUpsertOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id,
        created: true,
        updated: false,
        already_exists: false,
        non_writable: false,
        created_node_count: output.rows.len(),
        updated_property_count: 0,
    })
}

fn upsert_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeEntityUpsertBatchRequest,
) -> Result<KnowledgeEntityUpsertBatchOutput> {
    db.ensure_writable()?;
    for upsert in &request.upserts {
        validate_knowledge_entity_upsert(upsert)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.upserts.len());
    let mut created_count = 0;
    let mut updated_count = 0;
    let mut already_exists_count = 0;
    let mut non_writable_count = 0;
    let mut updated_property_count = 0;
    let mut eligible_creates = Vec::new();
    let mut eligible_updates = Vec::new();
    let mut pending_identities = BTreeSet::new();

    for upsert in &request.upserts {
        if let Some(existing) = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            upsert.label.as_str(),
            upsert.external_id.as_str(),
        ) {
            let node_id = existing.id.0;
            already_exists_count += 1;
            if !node_has_external_id_property(existing, upsert.external_id.as_str()) {
                non_writable_count += 1;
                rows.push(KnowledgeEntityUpsertBatchRow {
                    label: upsert.label.clone(),
                    external_id: upsert.external_id.clone(),
                    node_id: Some(node_id),
                    created: false,
                    updated: false,
                    already_exists: true,
                    non_writable: true,
                    updated_property_count: 0,
                });
                continue;
            }
            let update_properties = knowledge_entity_upsert_update_properties(upsert);
            if update_properties.is_empty() {
                rows.push(KnowledgeEntityUpsertBatchRow {
                    label: upsert.label.clone(),
                    external_id: upsert.external_id.clone(),
                    node_id: Some(node_id),
                    created: false,
                    updated: false,
                    already_exists: true,
                    non_writable: false,
                    updated_property_count: 0,
                });
                continue;
            }
            let row_updated_property_count = update_properties.len();
            updated_count += 1;
            updated_property_count += row_updated_property_count;
            eligible_updates.push((upsert.label.clone(), node_id, update_properties));
            rows.push(KnowledgeEntityUpsertBatchRow {
                label: upsert.label.clone(),
                external_id: upsert.external_id.clone(),
                node_id: Some(node_id),
                created: false,
                updated: true,
                already_exists: true,
                non_writable: false,
                updated_property_count: row_updated_property_count,
            });
            continue;
        }

        let identity = (upsert.label.clone(), upsert.external_id.clone());
        if !pending_identities.insert(identity) {
            already_exists_count += 1;
            rows.push(KnowledgeEntityUpsertBatchRow {
                label: upsert.label.clone(),
                external_id: upsert.external_id.clone(),
                node_id: None,
                created: false,
                updated: false,
                already_exists: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(knowledge_entity_upsert_create_request(upsert));
        rows.push(KnowledgeEntityUpsertBatchRow {
            label: upsert.label.clone(),
            external_id: upsert.external_id.clone(),
            node_id: None,
            created: true,
            updated: false,
            already_exists: false,
            non_writable: false,
            updated_property_count: 0,
        });
    }

    if eligible_creates.is_empty() && eligible_updates.is_empty() {
        return Ok(KnowledgeEntityUpsertBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count,
            updated_count,
            already_exists_count,
            non_writable_count,
            created_node_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    for (label, node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(label.as_str(), *node_id, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    for row in &mut rows {
        if row.created {
            row.node_id = seed_node_by_label_and_external_id(
                &db.catalog,
                &db.store,
                row.label.as_str(),
                row.external_id.as_str(),
            )
            .map(|node| node.id.0);
        }
    }
    Ok(KnowledgeEntityUpsertBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        updated_count,
        already_exists_count,
        non_writable_count,
        created_node_count: eligible_creates.len(),
        updated_property_count,
    })
}

fn validate_knowledge_entity_upsert(request: &KnowledgeEntityUpsertRequest) -> Result<()> {
    validate_cypher_identifier(&request.label, "label")?;
    if request.external_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge entity upsert requires a non-empty external id".to_string(),
        ));
    }
    validate_knowledge_entity_upsert_properties(
        request.external_id.as_str(),
        "create",
        &request.create_properties,
    )?;
    validate_knowledge_entity_upsert_properties(
        request.external_id.as_str(),
        "update",
        &request.update_properties,
    )
}

fn validate_knowledge_entity_upsert_properties(
    external_id: &str,
    phase: &str,
    properties: &BTreeMap<String, Value>,
) -> Result<()> {
    for property in properties.keys() {
        validate_cypher_identifier(property, "property")?;
    }
    if let Some(id) = properties.get("id") {
        let property_external_id = value_to_external_id(id);
        if property_external_id != external_id {
            return Err(SkeinError::Semantic(format!(
                "knowledge entity upsert {phase} id property {property_external_id:?} does not match external id {external_id:?}"
            )));
        }
    }
    Ok(())
}

fn knowledge_entity_upsert_create_request(
    request: &KnowledgeEntityUpsertRequest,
) -> KnowledgeEntityCreateRequest {
    KnowledgeEntityCreateRequest {
        label: request.label.clone(),
        external_id: request.external_id.clone(),
        properties: request.create_properties.clone(),
    }
}

fn knowledge_entity_upsert_update_properties(
    request: &KnowledgeEntityUpsertRequest,
) -> BTreeMap<String, Value> {
    request
        .update_properties
        .iter()
        .filter(|(property, _)| property.as_str() != "id")
        .map(|(property, value)| (property.clone(), value.clone()))
        .collect()
}

fn knowledge_property_batch_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgePropertyBatchRequest,
) -> KnowledgePropertyBatchOutput {
    knowledge_scoped_property_batch_for(
        catalog,
        store,
        &KnowledgeScopedPropertyBatchRequest {
            projection: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn knowledge_scoped_property_batch_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeScopedPropertyBatchRequest,
) -> KnowledgePropertyBatchOutput {
    let property_names = dedup_property_names(&request.projection.property_names);
    let mut rows = Vec::with_capacity(request.projection.entities.len());
    let mut found_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    for entity_request in &request.projection.entities {
        let node = seed_node_by_label_and_external_id(
            catalog,
            store,
            entity_request.label.as_str(),
            entity_request.external_id.as_str(),
        );
        let Some(node) = node else {
            missing_count += 1;
            rows.push(KnowledgePropertyRow {
                entity: entity_request.clone(),
                node_id: None,
                filtered_out: false,
                properties: empty_property_projection(&property_names),
            });
            continue;
        };
        if !request.metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(catalog, node, &request.metadata_filters)
        {
            filtered_out_count += 1;
            rows.push(KnowledgePropertyRow {
                entity: entity_request.clone(),
                node_id: Some(node.id.0),
                filtered_out: true,
                properties: empty_property_projection(&property_names),
            });
            continue;
        }
        found_count += 1;
        rows.push(KnowledgePropertyRow {
            entity: entity_request.clone(),
            node_id: Some(node.id.0),
            filtered_out: false,
            properties: project_node_properties(node, &property_names),
        });
    }
    KnowledgePropertyBatchOutput {
        graph_commit_epoch: store.commit_epoch(),
        rows,
        found_count,
        missing_count,
        filtered_out_count,
        property_names,
    }
}

fn dedup_property_names(property_names: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    property_names
        .iter()
        .filter(|name| seen.insert((*name).clone()))
        .cloned()
        .collect()
}

fn empty_property_projection(property_names: &[String]) -> BTreeMap<String, Option<Value>> {
    property_names
        .iter()
        .cloned()
        .map(|name| (name, None))
        .collect()
}

fn project_node_properties(
    node: &NodeRecord,
    property_names: &[String],
) -> BTreeMap<String, Option<Value>> {
    property_names
        .iter()
        .cloned()
        .map(|name| {
            let value = node.properties.get(&name).cloned();
            (name, value)
        })
        .collect()
}

fn update_knowledge_properties_for(
    db: &mut Database,
    request: &KnowledgePropertyUpdateRequest,
) -> Result<KnowledgePropertyUpdateOutput> {
    update_scoped_knowledge_properties_for(
        db,
        &KnowledgeScopedPropertyUpdateRequest {
            update: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn update_scoped_knowledge_properties_for(
    db: &mut Database,
    request: &KnowledgeScopedPropertyUpdateRequest,
) -> Result<KnowledgePropertyUpdateOutput> {
    db.ensure_writable()?;
    if request.update.assignments.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge property update requires at least one assignment".to_string(),
        ));
    }
    validate_cypher_identifier(&request.update.entity.label, "label")?;
    for property in request.update.assignments.keys() {
        validate_cypher_identifier(property, "property")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(seed) = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.update.entity.label.as_str(),
        request.update.entity.external_id.as_str(),
    ) else {
        return Ok(KnowledgePropertyUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: None,
            matched: false,
            filtered_out: false,
            updated_property_count: 0,
        });
    };
    if !request.metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(&db.catalog, seed, &request.metadata_filters)
    {
        let node_id = seed.id.0;
        return Ok(KnowledgePropertyUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(node_id),
            matched: false,
            filtered_out: true,
            updated_property_count: 0,
        });
    }
    let node_id = seed.id.0;

    let (cypher, parameters) = knowledge_property_update_statement(
        request.update.entity.label.as_str(),
        node_id,
        &request.update.assignments,
    );
    db.query_with_params(cypher.as_str(), &parameters)?;
    Ok(KnowledgePropertyUpdateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id: Some(node_id),
        matched: true,
        filtered_out: false,
        updated_property_count: request.update.assignments.len(),
    })
}

fn knowledge_property_update_statement(
    label: &str,
    node_id: u64,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!("MATCH (n:{label}) WHERE id(n) = $node_id SET ");
    let mut parameters = BTreeMap::from([("node_id".to_string(), Value::Int(node_id as i64))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        let parameter_name = format!("value_{index}");
        cypher.push_str(&format!("n.{property} = ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    (cypher, parameters)
}

fn update_knowledge_properties_batch_for(
    db: &mut Database,
    request: &KnowledgePropertyUpdateBatchRequest,
) -> Result<KnowledgePropertyUpdateBatchOutput> {
    update_scoped_knowledge_properties_batch_for(
        db,
        &KnowledgeScopedPropertyUpdateBatchRequest {
            updates: request.updates.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn update_scoped_knowledge_properties_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedPropertyUpdateBatchRequest,
) -> Result<KnowledgePropertyUpdateBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.assignments.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge property batch update requires every row to have at least one assignment"
                    .to_string(),
            ));
        }
        validate_cypher_identifier(&update.entity.label, "label")?;
        for property in update.assignments.keys() {
            validate_cypher_identifier(property, "property")?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut updated_property_count = 0;
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            update.entity.label.as_str(),
            update.entity.external_id.as_str(),
        ) else {
            missing_count += 1;
            rows.push(KnowledgePropertyUpdateBatchRow {
                entity: update.entity.clone(),
                node_id: None,
                matched: false,
                filtered_out: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id.0;
        if !node_has_external_id_property(seed, update.entity.external_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgePropertyUpdateBatchRow {
                entity: update.entity.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !request.metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(&db.catalog, seed, &request.metadata_filters)
        {
            filtered_out_count += 1;
            rows.push(KnowledgePropertyUpdateBatchRow {
                entity: update.entity.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let row_updated_property_count = update.assignments.len();
        matched_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((update.clone(), node_id));
        rows.push(KnowledgePropertyUpdateBatchRow {
            entity: update.entity.clone(),
            node_id: Some(node_id),
            matched: true,
            filtered_out: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgePropertyUpdateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            non_writable_count,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (update, node_id) in &eligible_updates {
        let (cypher, parameters) = knowledge_property_update_statement(
            update.entity.label.as_str(),
            *node_id,
            &update.assignments,
        );
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    Ok(KnowledgePropertyUpdateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        non_writable_count,
        updated_property_count,
    })
}

fn move_knowledge_normalized_space_batch_for(
    db: &mut Database,
    request: &KnowledgeNormalizedSpaceMoveBatchRequest,
) -> Result<KnowledgeNormalizedSpaceMoveBatchOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.label, "label")?;
    validate_cypher_identifier(&request.identity_property, "identity property")?;
    if request.target_space_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge normalized space move requires a non-empty target space id".to_string(),
        ));
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.external_ids.len());
    let mut moved_external_ids = Vec::new();
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut source_mismatch_count = 0;
    let mut already_in_target_count = 0;
    let mut duplicate_count = 0;
    let mut eligible_updates = Vec::new();
    let mut pending_node_ids = BTreeSet::new();

    for external_id in &request.external_ids {
        let Some(node) = node_by_label_property_external_id(
            &db.catalog,
            &db.store,
            request.label.as_str(),
            request.identity_property.as_str(),
            external_id.as_str(),
        ) else {
            missing_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: None,
                matched: false,
                moved: false,
                source_mismatch: false,
                already_in_target: false,
                duplicate: false,
            });
            continue;
        };
        matched_count += 1;
        let node_id = node.id.0;
        let normalized_space_id = normalized_node_space_id(node);
        if request
            .source_space_id
            .as_deref()
            .is_some_and(|source_space_id| normalized_space_id != source_space_id)
        {
            source_mismatch_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: true,
                moved: false,
                source_mismatch: true,
                already_in_target: false,
                duplicate: false,
            });
            continue;
        }
        if normalized_space_id == request.target_space_id {
            already_in_target_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: true,
                moved: false,
                source_mismatch: false,
                already_in_target: true,
                duplicate: false,
            });
            continue;
        }
        if !pending_node_ids.insert(node.id) {
            duplicate_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: true,
                moved: false,
                source_mismatch: false,
                already_in_target: false,
                duplicate: true,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([(
            "space_id".to_string(),
            Value::String(request.target_space_id.clone()),
        )]);
        if let Some(updated_at) = &request.updated_at {
            assignments.insert("updated_at".to_string(), updated_at.clone());
        }
        eligible_updates.push((node.id, assignments));
        moved_external_ids.push(external_id.clone());
        rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
            external_id: external_id.clone(),
            node_id: Some(node_id),
            matched: true,
            moved: true,
            source_mismatch: false,
            already_in_target: false,
            duplicate: false,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeNormalizedSpaceMoveBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            moved_external_ids,
            matched_count,
            missing_count,
            source_mismatch_count,
            already_in_target_count,
            duplicate_count,
            moved_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(request.label.as_str(), node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    Ok(KnowledgeNormalizedSpaceMoveBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        moved_external_ids,
        matched_count,
        missing_count,
        source_mismatch_count,
        already_in_target_count,
        duplicate_count,
        moved_count: eligible_updates.len(),
    })
}

fn touch_knowledge_memory_access_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryAccessBatchRequest,
) -> Result<KnowledgeMemoryAccessBatchOutput> {
    db.ensure_writable()?;
    for touch in &request.touches {
        if touch.memory_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory access touch requires a non-empty memory id".to_string(),
            ));
        }
        if touch
            .click_dwell_time_ms
            .is_some_and(|dwell_time_ms| dwell_time_ms < 0)
        {
            return Err(SkeinError::Semantic(
                "knowledge memory access touch requires non-negative dwell time".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.touches.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut non_writable_count = 0;
    let mut touched_count = 0;
    let mut click_touch_count = 0;
    let mut eligible_touches = BTreeMap::new();

    for touch in &request.touches {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Memory", &touch.memory_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryAccessBatchRow {
                memory_id: touch.memory_id.clone(),
                node_id: None,
                matched: false,
                touched: false,
                clicked: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id.0;
        if !node_has_external_id_property(seed, touch.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryAccessBatchRow {
                memory_id: touch.memory_id.clone(),
                node_id: Some(node_id),
                matched: false,
                touched: false,
                clicked: false,
                non_writable: true,
            });
            continue;
        }

        matched_count += 1;
        touched_count += 1;
        let clicked = touch.click_dwell_time_ms.is_some();
        if clicked {
            click_touch_count += 1;
        }
        eligible_touches
            .entry(seed.id)
            .and_modify(|aggregated: &mut AggregatedMemoryAccessTouch| {
                aggregated.access_count_increment += 1;
                aggregated.last_accessed_at = touch.accessed_at.clone();
                if let Some(dwell_time_ms) = touch.click_dwell_time_ms {
                    aggregated.click_increment += 1;
                    aggregated.dwell_time_ms_increment += dwell_time_ms;
                    aggregated.last_clicked_at = Some(touch.accessed_at.clone());
                }
            })
            .or_insert_with(|| AggregatedMemoryAccessTouch {
                access_count_increment: 1,
                last_accessed_at: touch.accessed_at.clone(),
                click_increment: usize::from(clicked),
                dwell_time_ms_increment: touch.click_dwell_time_ms.unwrap_or(0),
                last_clicked_at: clicked.then(|| touch.accessed_at.clone()),
            });
        rows.push(KnowledgeMemoryAccessBatchRow {
            memory_id: touch.memory_id.clone(),
            node_id: Some(node_id),
            matched: true,
            touched: true,
            clicked,
            non_writable: false,
        });
    }

    if eligible_touches.is_empty() {
        return Ok(KnowledgeMemoryAccessBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            non_writable_count,
            touched_count: 0,
            click_touch_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, touch) in &eligible_touches {
        let (cypher, parameters) = knowledge_memory_access_touch_statement(*node_id, touch);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryAccessBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        non_writable_count,
        touched_count,
        click_touch_count,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AggregatedMemoryAccessTouch {
    access_count_increment: usize,
    last_accessed_at: Value,
    click_increment: usize,
    dwell_time_ms_increment: i64,
    last_clicked_at: Option<Value>,
}

fn knowledge_memory_access_touch_statement(
    node_id: NodeId,
    touch: &AggregatedMemoryAccessTouch,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "MATCH (m:Memory) WHERE id(m) = $node_id SET m.access_count = COALESCE(m.access_count, 0) + $access_count_increment, m.last_accessed_at = $last_accessed_at".to_string();
    let mut parameters = BTreeMap::from([
        ("node_id".to_string(), Value::Int(node_id.0 as i64)),
        (
            "access_count_increment".to_string(),
            Value::Int(touch.access_count_increment as i64),
        ),
        (
            "last_accessed_at".to_string(),
            touch.last_accessed_at.clone(),
        ),
    ]);
    if touch.click_increment > 0 {
        cypher.push_str(", m.clicks = COALESCE(m.clicks, 0) + $click_increment, m.last_clicked_at = $last_clicked_at, m.total_dwell_time_ms = COALESCE(m.total_dwell_time_ms, 0) + $dwell_time_ms_increment");
        parameters.insert(
            "click_increment".to_string(),
            Value::Int(touch.click_increment as i64),
        );
        parameters.insert(
            "last_clicked_at".to_string(),
            touch
                .last_clicked_at
                .clone()
                .expect("click aggregate should carry last_clicked_at"),
        );
        parameters.insert(
            "dwell_time_ms_increment".to_string(),
            Value::Int(touch.dwell_time_ms_increment),
        );
    }
    (cypher, parameters)
}

fn adjust_knowledge_source_memory_count_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceMemoryCountBatchRequest,
) -> Result<KnowledgeSourceMemoryCountBatchOutput> {
    db.ensure_writable()?;
    for adjustment in &request.adjustments {
        if adjustment.source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source memory count adjustment requires a non-empty source id"
                    .to_string(),
            ));
        }
        if adjustment.delta == 0 {
            return Err(SkeinError::Semantic(
                "knowledge source memory count adjustment requires a non-zero delta".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.adjustments.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut non_writable_count = 0;
    let mut invalid_current_count_count = 0;
    let mut adjusted_count = 0;
    let mut aggregates: BTreeMap<NodeId, AggregatedSourceMemoryCountAdjustment> = BTreeMap::new();

    for adjustment in &request.adjustments {
        let Some(seed) = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Source",
            &adjustment.source_id,
        ) else {
            missing_count += 1;
            rows.push(KnowledgeSourceMemoryCountBatchRow {
                source_id: adjustment.source_id.clone(),
                node_id: None,
                matched: false,
                adjusted: false,
                non_writable: false,
                invalid_current_count: false,
                old_count: None,
                new_count: None,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, adjustment.source_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSourceMemoryCountBatchRow {
                source_id: adjustment.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                adjusted: false,
                non_writable: true,
                invalid_current_count: false,
                old_count: None,
                new_count: None,
            });
            continue;
        }
        let Some(current_count) = source_memory_count(seed) else {
            invalid_current_count_count += 1;
            rows.push(KnowledgeSourceMemoryCountBatchRow {
                source_id: adjustment.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                adjusted: false,
                non_writable: false,
                invalid_current_count: true,
                old_count: None,
                new_count: None,
            });
            continue;
        };

        let aggregate =
            aggregates
                .entry(node_id)
                .or_insert_with(|| AggregatedSourceMemoryCountAdjustment {
                    old_count: current_count,
                    new_count: current_count,
                });
        let old_count = aggregate.new_count;
        aggregate.new_count = apply_source_memory_count_delta(old_count, adjustment.delta);
        matched_count += 1;
        adjusted_count += 1;
        rows.push(KnowledgeSourceMemoryCountBatchRow {
            source_id: adjustment.source_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            adjusted: true,
            non_writable: false,
            invalid_current_count: false,
            old_count: Some(old_count),
            new_count: Some(aggregate.new_count),
        });
    }

    if aggregates.is_empty() {
        return Ok(KnowledgeSourceMemoryCountBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            non_writable_count,
            invalid_current_count_count,
            adjusted_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, adjustment) in &aggregates {
        let (cypher, parameters) =
            knowledge_source_memory_count_set_statement(*node_id, adjustment.new_count);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSourceMemoryCountBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        non_writable_count,
        invalid_current_count_count,
        adjusted_count,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AggregatedSourceMemoryCountAdjustment {
    old_count: i64,
    new_count: i64,
}

fn source_memory_count(node: &NodeRecord) -> Option<i64> {
    match node.properties.get("memory_count") {
        None | Some(Value::Null) => Some(0),
        Some(Value::Int(value)) => Some((*value).max(0)),
        Some(_) => None,
    }
}

fn apply_source_memory_count_delta(current_count: i64, delta: i64) -> i64 {
    current_count.saturating_add(delta).max(0)
}

fn knowledge_source_memory_count_set_statement(
    node_id: NodeId,
    memory_count: i64,
) -> (String, BTreeMap<String, Value>) {
    (
        "MATCH (s:Source) WHERE id(s) = $node_id SET s.memory_count = $memory_count".to_string(),
        BTreeMap::from([
            ("node_id".to_string(), Value::Int(node_id.0 as i64)),
            ("memory_count".to_string(), Value::Int(memory_count)),
        ]),
    )
}

fn update_knowledge_source_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceLifecycleBatchRequest,
) -> Result<KnowledgeSourceLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires a non-empty source id".to_string(),
            ));
        }
        if update.lifecycle_state.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires a non-empty lifecycle state"
                    .to_string(),
            ));
        }
        if update
            .current_lifecycle_state
            .as_deref()
            .is_some_and(str::is_empty)
        {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires a non-empty current lifecycle state"
                    .to_string(),
            ));
        }
        if update
            .chunk_count
            .is_some_and(|chunk_count| chunk_count < 0)
        {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires non-negative chunk count".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Source", &update.source_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.source_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if update
            .current_lifecycle_state
            .as_deref()
            .is_some_and(|state| {
                seed.properties
                    .get("lifecycle_state")
                    .map(value_to_external_id)
                    .as_deref()
                    != Some(state)
            })
        {
            filtered_out_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: true,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                filtered_out: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([
            (
                "lifecycle_state".to_string(),
                Value::String(update.lifecycle_state.clone()),
            ),
            ("updated_at".to_string(), update.updated_at.clone()),
        ]);
        if let Some(chunk_count) = update.chunk_count {
            assignments.insert("chunk_count".to_string(), Value::Int(chunk_count));
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSourceLifecycleBatchRow {
            source_id: update.source_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            filtered_out: false,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSourceLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Source", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSourceLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

fn knowledge_source_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSourceRequest,
) -> Result<KnowledgeSourceOutput> {
    if request.source_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source read requires a non-empty source id".to_string(),
        ));
    }
    let row = seed_node_by_label_and_external_id(catalog, store, "Source", &request.source_id)
        .map(|node| knowledge_source_row(catalog, store, node));
    Ok(KnowledgeSourceOutput {
        graph_commit_epoch: store.commit_epoch(),
        found: row.is_some(),
        row,
    })
}

fn knowledge_source_ids_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSourceIdListRequest,
) -> Result<KnowledgeSourceIdListOutput> {
    if request
        .lifecycle_state
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(SkeinError::Semantic(
            "knowledge source id list requires a non-empty lifecycle state".to_string(),
        ));
    }
    if request
        .normalized_space_id
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(SkeinError::Semantic(
            "knowledge source id list requires a non-empty normalized space id".to_string(),
        ));
    }

    let Some(label_id) = catalog.label_id("Source") else {
        return Ok(KnowledgeSourceIdListOutput {
            graph_commit_epoch: store.commit_epoch(),
            source_ids: Vec::new(),
            matched_count: 0,
            returned_count: 0,
        });
    };
    let mut source_ids = store
        .scan_nodes(Some(label_id))
        .filter(|node| {
            request.lifecycle_state.as_ref().is_none_or(|state| {
                node.properties
                    .get("lifecycle_state")
                    .map(value_to_external_id)
                    .as_ref()
                    == Some(state)
            }) && request
                .normalized_space_id
                .as_ref()
                .is_none_or(|space_id| normalized_node_space_id(node) == *space_id)
        })
        .filter_map(node_external_id)
        .collect::<Vec<_>>();
    source_ids.sort();
    let matched_count = source_ids.len();
    if request.limit > 0 {
        source_ids.truncate(request.limit);
    }
    let returned_count = source_ids.len();
    Ok(KnowledgeSourceIdListOutput {
        graph_commit_epoch: store.commit_epoch(),
        source_ids,
        matched_count,
        returned_count,
    })
}

fn knowledge_sources_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSourceListRequest,
) -> Result<KnowledgeSourceListOutput> {
    validate_knowledge_source_list_request(request)?;
    let graph_commit_epoch = store.commit_epoch();
    let Some(label_id) = catalog.label_id("Source") else {
        return Ok(KnowledgeSourceListOutput {
            graph_commit_epoch,
            rows: Vec::new(),
            matched_count: 0,
            returned_count: 0,
            missing_source_ids: request.source_ids.clone(),
        });
    };

    let requested_ids = request.source_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut matched_source_ids = BTreeSet::new();
    let mut rows = store
        .scan_nodes(Some(label_id))
        .filter(|node| source_matches_list_request(node, request, &requested_ids))
        .map(|node| {
            if let Some(source_id) = node_external_id(node) {
                matched_source_ids.insert(source_id);
            }
            knowledge_source_list_row(catalog, store, node)
        })
        .collect::<Vec<_>>();

    sort_source_list_rows(&mut rows, request.order);
    let matched_count = rows.len();
    if request.offset > 0 {
        rows = rows.into_iter().skip(request.offset).collect();
    }
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();
    let missing_source_ids = request
        .source_ids
        .iter()
        .filter(|source_id| !matched_source_ids.contains(*source_id))
        .cloned()
        .collect::<Vec<_>>();

    Ok(KnowledgeSourceListOutput {
        graph_commit_epoch,
        rows,
        matched_count,
        returned_count,
        missing_source_ids,
    })
}

fn validate_knowledge_source_list_request(request: &KnowledgeSourceListRequest) -> Result<()> {
    if request
        .source_ids
        .iter()
        .any(|source_id| source_id.is_empty())
    {
        return Err(SkeinError::Semantic(
            "knowledge source list requires non-empty source ids".to_string(),
        ));
    }
    if request
        .after_source_id
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(SkeinError::Semantic(
            "knowledge source list requires a non-empty after source id".to_string(),
        ));
    }
    if request
        .lifecycle_states
        .iter()
        .any(|state| state.is_empty())
    {
        return Err(SkeinError::Semantic(
            "knowledge source list requires non-empty lifecycle states".to_string(),
        ));
    }
    if request
        .normalized_space_id
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(SkeinError::Semantic(
            "knowledge source list requires a non-empty normalized space id".to_string(),
        ));
    }
    if request.source_type.as_deref().is_some_and(str::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge source list requires a non-empty source type".to_string(),
        ));
    }
    if request
        .metadata_contains
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(SkeinError::Semantic(
            "knowledge source list requires a non-empty metadata marker".to_string(),
        ));
    }
    if request.source_ids.is_empty()
        && request.after_source_id.is_none()
        && request.lifecycle_states.is_empty()
        && request.normalized_space_id.is_none()
        && request.source_type.is_none()
        && request.metadata_contains.is_none()
        && !request.parsed_path_required
        && request.limit == 0
    {
        return Err(SkeinError::Semantic(
            "knowledge source list requires a bounded limit or a filter".to_string(),
        ));
    }
    Ok(())
}

fn source_matches_list_request(
    node: &NodeRecord,
    request: &KnowledgeSourceListRequest,
    requested_ids: &BTreeSet<String>,
) -> bool {
    let source_id = node_external_id(node);
    if !requested_ids.is_empty()
        && !source_id
            .as_ref()
            .is_some_and(|source_id| requested_ids.contains(source_id))
    {
        return false;
    }
    if request.after_source_id.as_ref().is_some_and(|after| {
        source_id
            .as_ref()
            .is_none_or(|source_id| source_id.as_str() <= after.as_str())
    }) {
        return false;
    }
    if !request.lifecycle_states.is_empty()
        && !string_property(node, "lifecycle_state")
            .as_ref()
            .is_some_and(|state| request.lifecycle_states.contains(state))
    {
        return false;
    }
    if request
        .normalized_space_id
        .as_ref()
        .is_some_and(|space_id| normalized_node_space_id(node) != *space_id)
    {
        return false;
    }
    if request.source_type.as_ref().is_some_and(|source_type| {
        string_property(node, "source_type").as_ref() != Some(source_type)
    }) {
        return false;
    }
    if request.parsed_path_required && string_property(node, "parsed_path").is_none() {
        return false;
    }
    if request
        .metadata_contains
        .as_ref()
        .is_some_and(|marker| !source_metadata_contains(node, marker))
    {
        return false;
    }
    true
}

fn source_metadata_contains(node: &NodeRecord, marker: &str) -> bool {
    node.properties
        .get("metadata")
        .map(value_to_external_id)
        .is_some_and(|metadata| metadata.contains(marker))
}

fn knowledge_source_list_row(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
) -> KnowledgeSourceListRow {
    let original_name = string_property(node, "original_name");
    let title = string_property(node, "title");
    let file_path = string_property(node, "file_path");
    let source_type = string_property(node, "source_type");
    let display_name = original_name
        .clone()
        .or_else(|| title.clone())
        .or_else(|| file_path.clone())
        .or_else(|| source_type.clone())
        .unwrap_or_else(|| "Source".to_string());
    KnowledgeSourceListRow {
        source_id: node_external_id(node),
        node_id: node.id.0,
        display_name,
        original_name,
        title,
        summary: string_property(node, "summary"),
        source_type,
        lifecycle_state: string_property(node, "lifecycle_state"),
        raw_space_id: string_property(node, "space_id"),
        normalized_space_id: normalized_node_space_id(node),
        parsed_path: string_property(node, "parsed_path"),
        file_path,
        mime_type: string_property(node, "mime_type"),
        source_url: string_property(node, "source_url"),
        metadata: node.properties.get("metadata").cloned(),
        memory_count: integer_property(node, "memory_count").unwrap_or(0),
        chunk_count: integer_property(node, "chunk_count").unwrap_or(0),
        size_bytes: integer_property(node, "size_bytes").unwrap_or(0),
        version: integer_property(node, "version").unwrap_or(1),
        created_at: node.properties.get("created_at").cloned(),
        updated_at: node.properties.get("updated_at").cloned(),
        sourced_memory_count: source_sourced_memory_count(catalog, store, node.id),
    }
}

fn sort_source_list_rows(rows: &mut [KnowledgeSourceListRow], order: KnowledgeSourceListOrder) {
    rows.sort_by(|left, right| match order {
        KnowledgeSourceListOrder::SourceIdAsc => compare_source_list_ids(left, right),
        KnowledgeSourceListOrder::MemoryCountDesc => right
            .memory_count
            .cmp(&left.memory_count)
            .then_with(|| compare_source_list_ids(left, right)),
        KnowledgeSourceListOrder::CreatedAtDesc => compare_skill_memory_created_at(
            &left.created_at,
            &right.created_at,
            KnowledgeSkillMemoryListOrder::CreatedAtDesc,
        )
        .then_with(|| compare_source_list_ids(left, right)),
    });
}

fn compare_source_list_ids(
    left: &KnowledgeSourceListRow,
    right: &KnowledgeSourceListRow,
) -> std::cmp::Ordering {
    left.source_id
        .cmp(&right.source_id)
        .then_with(|| left.node_id.cmp(&right.node_id))
}

fn knowledge_source_row(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
) -> KnowledgeSourceRow {
    KnowledgeSourceRow {
        source_id: node_external_id(node),
        node_id: node.id.0,
        original_name: string_property(node, "original_name"),
        title: string_property(node, "title"),
        source_type: string_property(node, "source_type"),
        lifecycle_state: string_property(node, "lifecycle_state"),
        normalized_space_id: normalized_node_space_id(node),
        parsed_path: string_property(node, "parsed_path"),
        file_path: string_property(node, "file_path"),
        mime_type: string_property(node, "mime_type"),
        memory_count: integer_property(node, "memory_count"),
        chunk_count: integer_property(node, "chunk_count"),
        size_bytes: integer_property(node, "size_bytes"),
        created_at: node.properties.get("created_at").cloned(),
        updated_at: node.properties.get("updated_at").cloned(),
        sourced_memory_count: source_sourced_memory_count(catalog, store, node.id),
    }
}

fn knowledge_source_memories_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSourceMemoryListRequest,
) -> Result<KnowledgeSourceMemoryListOutput> {
    if request.source_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source memory read requires a non-empty source id".to_string(),
        ));
    }
    let graph_commit_epoch = store.commit_epoch();
    let Some(source) =
        seed_node_by_label_and_external_id(catalog, store, "Source", &request.source_id)
    else {
        return Ok(KnowledgeSourceMemoryListOutput {
            graph_commit_epoch,
            source_id: request.source_id.clone(),
            source_node_id: None,
            found: false,
            rows: Vec::new(),
            matched_count: 0,
            returned_count: 0,
        });
    };

    let mut rows = source_memory_rows(catalog, store, source.id);
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();
    Ok(KnowledgeSourceMemoryListOutput {
        graph_commit_epoch,
        source_id: request.source_id.clone(),
        source_node_id: Some(source.id.0),
        found: true,
        rows,
        matched_count,
        returned_count,
    })
}

fn source_memory_rows(
    catalog: &Catalog,
    store: &GraphStore,
    source_node_id: NodeId,
) -> Vec<KnowledgeSourceMemoryRow> {
    let Some(rel_type_id) = catalog.rel_type_id("SOURCED_FROM") else {
        return Vec::new();
    };
    let Some(memory_label_id) = catalog.label_id("Memory") else {
        return Vec::new();
    };
    let mut rows = store
        .incoming_relationships(source_node_id, rel_type_id)
        .filter_map(|relationship| {
            store
                .node(relationship.source)
                .filter(|memory| memory.labels.contains(&memory_label_id))
                .map(|memory| source_memory_row(memory, relationship))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.chunk_index
            .cmp(&right.chunk_index)
            .then_with(|| left.memory_id.cmp(&right.memory_id))
            .then_with(|| left.relationship_id.cmp(&right.relationship_id))
    });
    rows
}

fn source_memory_row(memory: &NodeRecord, relationship: &RelRecord) -> KnowledgeSourceMemoryRow {
    KnowledgeSourceMemoryRow {
        memory_id: node_external_id(memory),
        node_id: memory.id.0,
        relationship_id: relationship.id.0,
        title: string_property(memory, "title"),
        content: string_property(memory, "content"),
        unit_type: string_property(memory, "unit_type"),
        confidence: memory.properties.get("confidence").cloned(),
        chunk_index: relationship_integer_property(relationship, "chunk_index"),
        chunk_range: relationship_string_property(relationship, "chunk_range"),
        source_version: relationship_string_property(relationship, "source_version"),
        created_at: relationship.properties.get("created_at").cloned(),
    }
}

fn relationship_string_property(relationship: &RelRecord, property: &str) -> Option<String> {
    relationship
        .properties
        .get(property)
        .map(value_to_external_id)
        .filter(|value| !value.is_empty())
}

fn relationship_integer_property(relationship: &RelRecord, property: &str) -> Option<i64> {
    match relationship.properties.get(property) {
        Some(Value::Int(value)) => Some(*value),
        _ => None,
    }
}

fn source_sourced_memory_count(
    catalog: &Catalog,
    store: &GraphStore,
    source_node_id: NodeId,
) -> usize {
    let Some(rel_type_id) = catalog.rel_type_id("SOURCED_FROM") else {
        return 0;
    };
    let memory_label_id = catalog.label_id("Memory");
    store
        .scan_relationships(Some(rel_type_id))
        .filter(|relationship| {
            relationship.target == source_node_id
                && memory_label_id.is_none_or(|label_id| {
                    store
                        .node(relationship.source)
                        .is_some_and(|node| node.labels.contains(&label_id))
                })
        })
        .count()
}

fn string_property(node: &NodeRecord, property: &str) -> Option<String> {
    node.properties
        .get(property)
        .map(value_to_external_id)
        .filter(|value| !value.is_empty())
}

fn integer_property(node: &NodeRecord, property: &str) -> Option<i64> {
    match node.properties.get(property) {
        Some(Value::Int(value)) => Some(*value),
        _ => None,
    }
}

fn update_knowledge_memory_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryLifecycleBatchRequest,
) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.memory_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory lifecycle update requires a non-empty memory id".to_string(),
            ));
        }
        if update.lifecycle_state.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory lifecycle update requires a non-empty lifecycle state"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Memory", &update.memory_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryLifecycleBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryLifecycleBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryLifecycleBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = BTreeMap::from([
            ("metadata".to_string(), update.metadata.clone()),
            ("is_latest".to_string(), Value::Bool(update.is_latest)),
            (
                "lifecycle_state".to_string(),
                Value::String(update.lifecycle_state.clone()),
            ),
            ("updated_at".to_string(), update.updated_at.clone()),
        ]);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeMemoryLifecycleBatchRow {
            memory_id: update.memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

fn update_knowledge_memory_latest_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryLatestBatchRequest,
) -> Result<KnowledgeMemoryLatestBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.memory_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory latest update requires a non-empty memory id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Memory", &update.memory_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: true,
            });
            continue;
        }
        if !memory_latest_update_matches_space(seed, update) {
            filtered_out_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: true,
                duplicate: false,
                non_writable: false,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                filtered_out: false,
                duplicate: true,
                non_writable: false,
            });
            continue;
        }

        let assignments =
            BTreeMap::from([("is_latest".to_string(), Value::Bool(update.is_latest))]);
        matched_count += 1;
        updated_count += 1;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeMemoryLatestBatchRow {
            memory_id: update.memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            filtered_out: false,
            duplicate: false,
            non_writable: false,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryLatestBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryLatestBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        duplicate_count,
        non_writable_count,
        updated_count,
    })
}

fn memory_latest_update_matches_space(
    node: &NodeRecord,
    update: &KnowledgeMemoryLatestUpdate,
) -> bool {
    let Some(space_id_filter) = &update.space_id_filter else {
        return true;
    };
    node.properties
        .get("space_id")
        .is_some_and(|value| value_to_external_id(value) == *space_id_filter)
}

fn update_knowledge_skill_usage_stats_batch_for(
    db: &mut Database,
    request: &KnowledgeSkillUsageStatsBatchRequest,
) -> Result<KnowledgeSkillUsageStatsBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.skill_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge skill usage stats update requires a non-empty skill id".to_string(),
            ));
        }
        if update.use_count < 0 {
            return Err(SkeinError::Semantic(
                "knowledge skill usage stats update requires non-negative use count".to_string(),
            ));
        }
        if let Some(success_rate) = &update.success_rate {
            validate_skill_success_rate(success_rate)?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Skill", &update.skill_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeSkillUsageStatsBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.skill_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSkillUsageStatsBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSkillUsageStatsBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([
            ("use_count".to_string(), Value::Int(update.use_count)),
            (
                "last_activity_at".to_string(),
                update.last_activity_at.clone(),
            ),
            ("updated_at".to_string(), update.updated_at.clone()),
            ("metadata".to_string(), update.metadata.clone()),
        ]);
        if let Some(success_rate) = &update.success_rate {
            assignments.insert("success_rate".to_string(), success_rate.clone());
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSkillUsageStatsBatchRow {
            skill_id: update.skill_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSkillUsageStatsBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Skill", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSkillUsageStatsBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

fn validate_skill_success_rate(value: &Value) -> Result<()> {
    let valid = match value {
        Value::Float(rate) => rate.is_finite() && (0.0..=1.0).contains(rate),
        Value::Int(rate) => (0..=1).contains(rate),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SkeinError::Semantic(
            "knowledge skill usage stats update requires success rate between 0 and 1".to_string(),
        ))
    }
}

fn update_knowledge_skill_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeSkillLifecycleBatchRequest,
) -> Result<KnowledgeSkillLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.skill_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires a non-empty skill id".to_string(),
            ));
        }
        if update.stage.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires a non-empty stage".to_string(),
            ));
        }
        if update.write_origin.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires a non-empty write origin".to_string(),
            ));
        }
        if !skill_lifecycle_update_has_business_field(update) {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires at least one lifecycle field"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Skill", &update.skill_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeSkillLifecycleBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.skill_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSkillLifecycleBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSkillLifecycleBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = skill_lifecycle_assignments(update);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSkillLifecycleBatchRow {
            skill_id: update.skill_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSkillLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Skill", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSkillLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

fn skill_lifecycle_update_has_business_field(update: &KnowledgeSkillLifecycleUpdate) -> bool {
    update.stage.is_some()
        || update.rejected_at.is_some()
        || update.rationale.is_some()
        || update.version.is_some()
        || update.title.is_some()
        || update.name.is_some()
        || update.description.is_some()
        || update.triggers.is_some()
        || update.tools.is_some()
        || update.bundle_path.is_some()
        || update.content_hash.is_some()
        || update.write_origin.is_some()
        || update.metadata.is_some()
}

fn skill_lifecycle_assignments(update: &KnowledgeSkillLifecycleUpdate) -> BTreeMap<String, Value> {
    let mut assignments = BTreeMap::new();
    if let Some(stage) = &update.stage {
        assignments.insert("stage".to_string(), Value::String(stage.clone()));
    }
    insert_optional_assignment(&mut assignments, "rejected_at", &update.rejected_at);
    insert_optional_assignment(&mut assignments, "rationale", &update.rationale);
    insert_optional_assignment(&mut assignments, "version", &update.version);
    insert_optional_assignment(&mut assignments, "title", &update.title);
    insert_optional_assignment(&mut assignments, "name", &update.name);
    insert_optional_assignment(&mut assignments, "description", &update.description);
    insert_optional_assignment(&mut assignments, "triggers", &update.triggers);
    insert_optional_assignment(&mut assignments, "tools", &update.tools);
    insert_optional_assignment(&mut assignments, "bundle_path", &update.bundle_path);
    insert_optional_assignment(&mut assignments, "content_hash", &update.content_hash);
    if let Some(write_origin) = &update.write_origin {
        assignments.insert(
            "write_origin".to_string(),
            Value::String(write_origin.clone()),
        );
    }
    insert_optional_assignment(&mut assignments, "metadata", &update.metadata);
    assignments.insert("updated_at".to_string(), update.updated_at.clone());
    assignments
}

fn insert_optional_assignment(
    assignments: &mut BTreeMap<String, Value>,
    property_name: &str,
    value: &Option<Value>,
) {
    if let Some(value) = value {
        assignments.insert(property_name.to_string(), value.clone());
    }
}

fn update_knowledge_thread_metadata_batch_for(
    db: &mut Database,
    request: &KnowledgeThreadMetadataBatchRequest,
) -> Result<KnowledgeThreadMetadataBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.thread_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge thread metadata update requires a non-empty thread id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Thread", &update.thread_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeThreadMetadataBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.thread_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeThreadMetadataBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeThreadMetadataBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([("metadata".to_string(), update.metadata.clone())]);
        if let Some(updated_at) = &update.updated_at {
            assignments.insert("updated_at".to_string(), updated_at.clone());
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeThreadMetadataBatchRow {
            thread_id: update.thread_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeThreadMetadataBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Thread", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeThreadMetadataBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

fn update_knowledge_thread_message_count_batch_for(
    db: &mut Database,
    request: &KnowledgeThreadMessageCountBatchRequest,
) -> Result<KnowledgeThreadMessageCountBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.thread_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge thread message-count update requires a non-empty thread id".to_string(),
            ));
        }
        if update.message_count < 0 {
            return Err(SkeinError::Semantic(
                "knowledge thread message-count update requires non-negative message count"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_at_changed_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Thread", &update.thread_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeThreadMessageCountBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_at_changed: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.thread_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeThreadMessageCountBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_at_changed: false,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeThreadMessageCountBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_at_changed: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([(
            "message_count".to_string(),
            Value::Int(update.message_count),
        )]);
        let updated_at_changed = should_update_thread_updated_at(seed, update);
        if updated_at_changed {
            if let Some(updated_at) = &update.updated_at {
                assignments.insert("updated_at".to_string(), updated_at.clone());
            }
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        if updated_at_changed {
            updated_at_changed_count += 1;
        }
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeThreadMessageCountBatchRow {
            thread_id: update.thread_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_at_changed,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeThreadMessageCountBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_at_changed_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Thread", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeThreadMessageCountBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_at_changed_count,
        updated_property_count,
    })
}

fn should_update_thread_updated_at(
    seed: &NodeRecord,
    update: &KnowledgeThreadMessageCountUpdate,
) -> bool {
    let Some(candidate) = &update.updated_at else {
        return false;
    };
    if !update.preserve_newer_existing_updated_at {
        return true;
    }
    match seed.properties.get("updated_at") {
        Some(current) if current != &Value::Null => !value_is_greater(current, candidate),
        _ => true,
    }
}

fn value_is_greater(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left > right,
        (Value::Float(left), Value::Float(right)) => left > right,
        (Value::Int(left), Value::Float(right)) => (*left as f64) > *right,
        (Value::Float(left), Value::Int(right)) => *left > (*right as f64),
        (Value::String(left), Value::String(right)) => left > right,
        _ => false,
    }
}

fn knowledge_skill_memories_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSkillMemoryListRequest,
) -> Result<KnowledgeSkillMemoryListOutput> {
    validate_knowledge_skill_memory_request(request)?;
    let graph_commit_epoch = store.commit_epoch();
    let skill_nodes = skill_memory_seed_nodes(catalog, store, request);
    let matched_skill_count = skill_nodes.len();
    let mut missing_skill_count = 0;

    if request.skill_id.is_some() && skill_nodes.is_empty() {
        missing_skill_count = 1;
    }

    let mut rows = skill_nodes
        .into_iter()
        .flat_map(|skill| skill_memory_rows(catalog, store, skill))
        .collect::<Vec<_>>();
    sort_skill_memory_rows(&mut rows, request.order);
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeSkillMemoryListOutput {
        graph_commit_epoch,
        rows,
        matched_count,
        returned_count,
        matched_skill_count,
        missing_skill_count,
    })
}

fn validate_knowledge_skill_memory_request(
    request: &KnowledgeSkillMemoryListRequest,
) -> Result<()> {
    let has_skill_id = match request.skill_id.as_ref() {
        Some(skill_id) if skill_id.is_empty() => {
            return Err(SkeinError::Semantic(
                "knowledge skill memory read requires a non-empty skill id".to_string(),
            ));
        }
        Some(_) => true,
        None => false,
    };
    if request.stages.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge skill memory read requires non-empty stages".to_string(),
        ));
    }
    let has_stages = !request.stages.is_empty();
    if has_skill_id == has_stages {
        return Err(SkeinError::Semantic(
            "knowledge skill memory read requires exactly one skill id or stage filter".to_string(),
        ));
    }
    Ok(())
}

fn skill_memory_seed_nodes<'a>(
    catalog: &Catalog,
    store: &'a GraphStore,
    request: &KnowledgeSkillMemoryListRequest,
) -> Vec<&'a NodeRecord> {
    if let Some(skill_id) = request.skill_id.as_ref() {
        return seed_node_by_label_and_external_id(catalog, store, "Skill", skill_id)
            .into_iter()
            .collect();
    }

    let Some(skill_label_id) = catalog.label_id("Skill") else {
        return Vec::new();
    };
    let stages = request.stages.iter().collect::<BTreeSet<_>>();
    let mut nodes = store
        .scan_nodes(Some(skill_label_id))
        .filter(|node| {
            node.properties
                .get("stage")
                .map(value_to_external_id)
                .as_ref()
                .is_some_and(|stage| stages.contains(stage))
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        node_external_id(left)
            .cmp(&node_external_id(right))
            .then_with(|| left.id.0.cmp(&right.id.0))
    });
    nodes
}

fn skill_memory_rows(
    catalog: &Catalog,
    store: &GraphStore,
    skill: &NodeRecord,
) -> Vec<KnowledgeSkillMemoryRow> {
    let Some(rel_type_id) = catalog.rel_type_id("SYNTHESIZED_FROM") else {
        return Vec::new();
    };
    let Some(memory_label_id) = catalog.label_id("Memory") else {
        return Vec::new();
    };
    store
        .outgoing_relationships(skill.id, rel_type_id)
        .filter_map(|relationship| {
            store
                .node(relationship.target)
                .filter(|memory| memory.labels.contains(&memory_label_id))
                .map(|memory| skill_memory_row(skill, memory, relationship))
        })
        .collect()
}

fn skill_memory_row(
    skill: &NodeRecord,
    memory: &NodeRecord,
    relationship: &RelRecord,
) -> KnowledgeSkillMemoryRow {
    KnowledgeSkillMemoryRow {
        skill_id: node_external_id(skill),
        skill_node_id: skill.id.0,
        memory_id: node_external_id(memory),
        memory_node_id: memory.id.0,
        relationship_id: relationship.id.0,
        title: string_property(memory, "title"),
        content: string_property(memory, "content"),
        unit_type: string_property(memory, "unit_type"),
        created_at: memory.properties.get("created_at").cloned(),
    }
}

fn sort_skill_memory_rows(
    rows: &mut [KnowledgeSkillMemoryRow],
    order: KnowledgeSkillMemoryListOrder,
) {
    rows.sort_by(|left, right| {
        compare_skill_memory_created_at(&left.created_at, &right.created_at, order)
            .then_with(|| left.memory_id.cmp(&right.memory_id))
            .then_with(|| left.skill_id.cmp(&right.skill_id))
            .then_with(|| left.relationship_id.cmp(&right.relationship_id))
    });
}

fn compare_skill_memory_created_at(
    left: &Option<Value>,
    right: &Option<Value>,
    order: KnowledgeSkillMemoryListOrder,
) -> std::cmp::Ordering {
    let base = match (left, right) {
        (Some(left), Some(right)) => compare_skill_memory_values(left, right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    };
    match order {
        KnowledgeSkillMemoryListOrder::CreatedAtAsc => base,
        KnowledgeSkillMemoryListOrder::CreatedAtDesc => {
            if left.is_some() && right.is_some() {
                base.reverse()
            } else {
                base
            }
        }
    }
}

fn compare_skill_memory_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left.cmp(right),
        (Value::Float(left), Value::Float(right)) => left.total_cmp(right),
        (Value::Int(left), Value::Float(right)) => (*left as f64).total_cmp(right),
        (Value::Float(left), Value::Int(right)) => left.total_cmp(&(*right as f64)),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        _ => value_to_external_id(left).cmp(&value_to_external_id(right)),
    }
}

fn knowledge_thread_messages_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeThreadMessageListRequest,
) -> Result<KnowledgeThreadMessageListOutput> {
    if request.thread_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge thread message read requires a non-empty thread id".to_string(),
        ));
    }
    let graph_commit_epoch = store.commit_epoch();
    let Some(thread) =
        seed_node_by_label_and_external_id(catalog, store, "Thread", &request.thread_id)
    else {
        return Ok(KnowledgeThreadMessageListOutput {
            graph_commit_epoch,
            thread_id: request.thread_id.clone(),
            thread_node_id: None,
            found: false,
            rows: Vec::new(),
            matched_count: 0,
            returned_count: 0,
        });
    };
    let mut rows = thread_message_rows(catalog, store, thread.id);
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();
    Ok(KnowledgeThreadMessageListOutput {
        graph_commit_epoch,
        thread_id: request.thread_id.clone(),
        thread_node_id: Some(thread.id.0),
        found: true,
        rows,
        matched_count,
        returned_count,
    })
}

fn thread_message_rows(
    catalog: &Catalog,
    store: &GraphStore,
    thread_node_id: NodeId,
) -> Vec<KnowledgeThreadMessageRow> {
    let Some(rel_type_id) = catalog.rel_type_id("CONTAINS") else {
        return Vec::new();
    };
    let Some(message_label_id) = catalog.label_id("Message") else {
        return Vec::new();
    };
    let mut rows = store
        .outgoing_relationships(thread_node_id, rel_type_id)
        .filter_map(|relationship| {
            store
                .node(relationship.target)
                .filter(|message| message.labels.contains(&message_label_id))
                .map(|message| thread_message_row(message, relationship))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.order_index
            .cmp(&right.order_index)
            .then_with(|| left.message_id.cmp(&right.message_id))
            .then_with(|| left.relationship_id.cmp(&right.relationship_id))
    });
    rows
}

fn thread_message_row(message: &NodeRecord, relationship: &RelRecord) -> KnowledgeThreadMessageRow {
    let relationship_order_index = relationship_integer_property(relationship, "order_index");
    let message_order_index = integer_property(message, "order_index");
    KnowledgeThreadMessageRow {
        message_id: node_external_id(message),
        node_id: message.id.0,
        relationship_id: relationship.id.0,
        role: string_property(message, "role"),
        content: string_property(message, "content"),
        order_index: relationship_order_index.or(message_order_index),
        relationship_order_index,
        message_order_index,
        timestamp: message.properties.get("timestamp").cloned(),
        token_count: integer_property(message, "token_count"),
        created_at: message.properties.get("created_at").cloned(),
        updated_at: message.properties.get("updated_at").cloned(),
        metadata: message.properties.get("metadata").cloned(),
    }
}

fn update_knowledge_label_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeLabelLifecycleBatchRequest,
) -> Result<KnowledgeLabelLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.label_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires a non-empty label id".to_string(),
            ));
        }
        if update.name.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires a non-empty name".to_string(),
            ));
        }
        if update.canonical_name.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires a non-empty canonical name".to_string(),
            ));
        }
        if !label_lifecycle_update_has_business_field(update) {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires at least one lifecycle field"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Label", &update.label_id)
        else {
            missing_count += 1;
            rows.push(KnowledgeLabelLifecycleBatchRow {
                label_id: update.label_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.label_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeLabelLifecycleBatchRow {
                label_id: update.label_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeLabelLifecycleBatchRow {
                label_id: update.label_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = label_lifecycle_assignments(update);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeLabelLifecycleBatchRow {
            label_id: update.label_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeLabelLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Label", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeLabelLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

fn label_lifecycle_update_has_business_field(update: &KnowledgeLabelLifecycleUpdate) -> bool {
    update.name.is_some() || update.canonical_name.is_some() || update.metadata.is_some()
}

fn label_lifecycle_assignments(update: &KnowledgeLabelLifecycleUpdate) -> BTreeMap<String, Value> {
    let mut assignments = BTreeMap::new();
    if let Some(name) = &update.name {
        assignments.insert("name".to_string(), Value::String(name.clone()));
    }
    if let Some(canonical_name) = &update.canonical_name {
        assignments.insert(
            "canonical_name".to_string(),
            Value::String(canonical_name.clone()),
        );
    }
    insert_optional_assignment(&mut assignments, "metadata", &update.metadata);
    insert_optional_assignment(&mut assignments, "updated_at", &update.updated_at);
    assignments
}

fn lookup_knowledge_labels_by_canonical_name_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeLabelCanonicalLookupRequest,
) -> Result<KnowledgeLabelUsageListOutput> {
    if request.canonical_name.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge label canonical lookup requires a non-empty canonical name".to_string(),
        ));
    }
    validate_optional_label_id(request.exclude_label_id.as_deref())?;
    let graph_commit_epoch = store.commit_epoch();
    let rows = label_nodes(catalog, store)
        .into_iter()
        .filter(|node| {
            node_string_property(node, "canonical_name").as_deref()
                == Some(request.canonical_name.as_str())
                && request
                    .exclude_label_id
                    .as_ref()
                    .is_none_or(|label_id| node_external_id(node).as_ref() != Some(label_id))
        })
        .collect::<Vec<_>>();
    Ok(label_usage_list_output(
        catalog,
        store,
        graph_commit_epoch,
        rows,
        request.limit,
    ))
}

fn scan_knowledge_labels_missing_canonical_name_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeLabelBackfillScanRequest,
) -> Result<KnowledgeLabelUsageListOutput> {
    validate_optional_label_id(request.exclude_label_id.as_deref())?;
    let graph_commit_epoch = store.commit_epoch();
    let rows = label_nodes(catalog, store)
        .into_iter()
        .filter(|node| {
            matches!(
                node.properties.get("canonical_name"),
                None | Some(Value::Null)
            ) && request
                .exclude_label_id
                .as_ref()
                .is_none_or(|label_id| node_external_id(node).as_ref() != Some(label_id))
        })
        .collect::<Vec<_>>();
    Ok(label_usage_list_output(
        catalog,
        store,
        graph_commit_epoch,
        rows,
        request.limit,
    ))
}

fn knowledge_label_usage_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeLabelUsageRequest,
) -> Result<KnowledgeLabelUsageOutput> {
    if request.label_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge label usage requires a non-empty label id".to_string(),
        ));
    }
    let graph_commit_epoch = store.commit_epoch();
    let row = seed_node_by_label_and_external_id(catalog, store, "Label", &request.label_id)
        .map(|node| label_usage_row(catalog, store, node));
    Ok(KnowledgeLabelUsageOutput {
        graph_commit_epoch,
        found: row.is_some(),
        row,
    })
}

fn knowledge_label_canonical_usage_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeLabelUsageListRequest,
) -> KnowledgeLabelUsageListOutput {
    let graph_commit_epoch = store.commit_epoch();
    let rows = label_nodes(catalog, store)
        .into_iter()
        .filter(|node| {
            !request.canonical_only
                || node
                    .properties
                    .get("canonical_name")
                    .is_some_and(|value| !matches!(value, Value::Null))
        })
        .collect::<Vec<_>>();
    label_usage_list_output(catalog, store, graph_commit_epoch, rows, request.limit)
}

fn knowledge_entity_labels_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeEntityLabelListRequest,
) -> Result<KnowledgeEntityLabelListOutput> {
    validate_cypher_identifier(&request.entity_label, "knowledge entity label")?;
    if request.external_ids.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge entity label read requires non-empty external ids".to_string(),
        ));
    }
    validate_non_empty_external_ids(
        &request.external_ids,
        "knowledge entity label read requires non-empty external ids",
    )?;
    let graph_commit_epoch = store.commit_epoch();
    let mut groups = Vec::with_capacity(request.external_ids.len());
    let mut found_entity_count = 0;
    let mut missing_entity_count = 0;
    let mut label_count = 0;

    for external_id in &request.external_ids {
        let Some(entity) =
            seed_node_by_label_and_external_id(catalog, store, &request.entity_label, external_id)
        else {
            missing_entity_count += 1;
            groups.push(KnowledgeEntityLabelGroup {
                external_id: external_id.clone(),
                node_id: None,
                found: false,
                labels: Vec::new(),
                returned_count: 0,
            });
            continue;
        };
        found_entity_count += 1;
        let labels = entity_label_rows(catalog, store, entity, request.limit_per_entity);
        label_count += labels.len();
        groups.push(KnowledgeEntityLabelGroup {
            external_id: external_id.clone(),
            node_id: Some(entity.id.0),
            found: true,
            returned_count: labels.len(),
            labels,
        });
    }

    Ok(KnowledgeEntityLabelListOutput {
        graph_commit_epoch,
        groups,
        found_entity_count,
        missing_entity_count,
        label_count,
    })
}

fn validate_optional_label_id(label_id: Option<&str>) -> Result<()> {
    if label_id.is_some_and(str::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge label read requires a non-empty excluded label id".to_string(),
        ));
    }
    Ok(())
}

fn label_nodes<'a>(catalog: &Catalog, store: &'a GraphStore) -> Vec<&'a NodeRecord> {
    let Some(label_id) = catalog.label_id("Label") else {
        return Vec::new();
    };
    let mut nodes = store.scan_nodes(Some(label_id)).collect::<Vec<_>>();
    nodes.sort_by_key(|node| node.id.0);
    nodes
}

fn label_usage_list_output(
    catalog: &Catalog,
    store: &GraphStore,
    graph_commit_epoch: u64,
    nodes: Vec<&NodeRecord>,
    limit: usize,
) -> KnowledgeLabelUsageListOutput {
    let matched_count = nodes.len();
    let mut rows = nodes
        .into_iter()
        .take(limit)
        .map(|node| label_usage_row(catalog, store, node))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.node_id);
    let returned_count = rows.len();
    KnowledgeLabelUsageListOutput {
        graph_commit_epoch,
        rows,
        matched_count,
        returned_count,
    }
}

fn label_usage_row(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
) -> KnowledgeLabelUsageRow {
    KnowledgeLabelUsageRow {
        label_id: node_external_id(node),
        node_id: node.id.0,
        name: node_string_property(node, "name"),
        canonical_name: node_string_property(node, "canonical_name"),
        color: node.properties.get("color").cloned(),
        description: node.properties.get("description").cloned(),
        created_at: node.properties.get("created_at").cloned(),
        updated_at: node.properties.get("updated_at").cloned(),
        usage_count: label_usage_count(catalog, store, node.id),
    }
}

fn label_usage_count(catalog: &Catalog, store: &GraphStore, label_node_id: NodeId) -> usize {
    let Some(rel_type_id) = catalog.rel_type_id("HAS_LABEL") else {
        return 0;
    };
    store
        .scan_relationships(Some(rel_type_id))
        .filter(|relationship| relationship.target == label_node_id)
        .count()
}

fn entity_label_rows(
    catalog: &Catalog,
    store: &GraphStore,
    entity: &NodeRecord,
    limit: usize,
) -> Vec<KnowledgeEntityLabelRow> {
    let Some(rel_type_id) = catalog.rel_type_id("HAS_LABEL") else {
        return Vec::new();
    };
    let Some(label_label_id) = catalog.label_id("Label") else {
        return Vec::new();
    };
    let mut rows = store
        .outgoing_relationships(entity.id, rel_type_id)
        .filter_map(|relationship| store.node(relationship.target))
        .filter(|node| node.labels.contains(&label_label_id))
        .map(entity_label_row)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.label_id.cmp(&right.label_id))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    if limit > 0 {
        rows.truncate(limit);
    }
    rows
}

fn entity_label_row(node: &NodeRecord) -> KnowledgeEntityLabelRow {
    KnowledgeEntityLabelRow {
        label_id: node_external_id(node),
        node_id: node.id.0,
        name: node_string_property(node, "name"),
        canonical_name: node_string_property(node, "canonical_name"),
        color: node.properties.get("color").cloned(),
        description: node.properties.get("description").cloned(),
    }
}

fn update_knowledge_pagerank_scores_batch_for(
    db: &mut Database,
    request: &KnowledgePageRankScoreBatchRequest,
) -> Result<KnowledgePageRankScoreBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_pagerank_label(update.label.as_str())?;
        if update.external_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge pagerank score update requires a non-empty external id".to_string(),
            ));
        }
        if !update.score.is_finite() || update.score < 0.0 {
            return Err(SkeinError::Semantic(
                "knowledge pagerank score update requires a finite non-negative score".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let label = pagerank_label(update.label.as_str());
        let Some(seed) = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            label,
            update.external_id.as_str(),
        ) else {
            missing_count += 1;
            rows.push(KnowledgePageRankScoreBatchRow {
                label: label.to_string(),
                external_id: update.external_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(seed, update.external_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgePageRankScoreBatchRow {
                label: label.to_string(),
                external_id: update.external_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgePageRankScoreBatchRow {
                label: label.to_string(),
                external_id: update.external_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        updated_count += 1;
        let assignments =
            BTreeMap::from([("pagerank_score".to_string(), Value::Float(update.score))]);
        eligible_updates.push((label.to_string(), node_id, assignments));
        rows.push(KnowledgePageRankScoreBatchRow {
            label: label.to_string(),
            external_id: update.external_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgePageRankScoreBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (label, node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(label.as_str(), node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgePageRankScoreBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
    })
}

fn clear_knowledge_pagerank_scores_for(
    db: &mut Database,
    request: &KnowledgePageRankClearRequest,
) -> Result<KnowledgePageRankClearOutput> {
    db.ensure_writable()?;
    if request.labels.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge pagerank clear requires at least one label".to_string(),
        ));
    }
    for label in &request.labels {
        validate_pagerank_label(label.as_str())?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::new();
    let mut candidate_count = 0;
    let mut cleared_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_updates = Vec::new();
    let mut seen_node_ids = BTreeSet::new();

    for requested_label in &request.labels {
        let label = pagerank_label(requested_label.as_str());
        let Some(label_id) = db.catalog.label_id(label) else {
            continue;
        };
        for node in db.store.scan_nodes(Some(label_id)) {
            if !seen_node_ids.insert(node.id) {
                continue;
            }
            if node
                .properties
                .get("pagerank_score")
                .is_none_or(|value| value == &Value::Null)
            {
                continue;
            }
            candidate_count += 1;
            let external_id = node_external_id(node);
            if external_id.is_none() {
                non_writable_count += 1;
                rows.push(KnowledgePageRankClearRow {
                    label: label.to_string(),
                    external_id,
                    node_id: node.id.0,
                    cleared: false,
                    non_writable: true,
                });
                continue;
            }
            let assignments = BTreeMap::from([("pagerank_score".to_string(), Value::Null)]);
            eligible_updates.push((label.to_string(), node.id, assignments));
            cleared_count += 1;
            rows.push(KnowledgePageRankClearRow {
                label: label.to_string(),
                external_id,
                node_id: node.id.0,
                cleared: true,
                non_writable: false,
            });
        }
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgePageRankClearOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count,
            cleared_count: 0,
            non_writable_count,
        });
    }

    let mut tx = db.begin_transaction();
    for (label, node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(label.as_str(), node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgePageRankClearOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        candidate_count,
        cleared_count,
        non_writable_count,
    })
}

fn knowledge_pagerank_plan_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgePageRankPlanRequest,
) -> KnowledgePageRankPlanOutput {
    let cutoff = request.changed_since_epoch_nanos.map(Value::Int);
    KnowledgePageRankPlanOutput {
        graph_commit_epoch: store.commit_epoch(),
        memory_node_count: count_nodes_with_label(catalog, store, "Memory"),
        entity_node_count: count_nodes_with_label(catalog, store, "Entity"),
        entity_relation_count: count_relationships_between_labels(
            catalog,
            store,
            "RELATES_TO",
            "Entity",
            "Entity",
            |_| true,
        ),
        mention_edge_count: count_relationships_between_labels(
            catalog,
            store,
            "MENTIONS",
            "Memory",
            "Entity",
            |_| true,
        ),
        active_memory_relation_count: count_relationships_between_labels(
            catalog,
            store,
            "MEMORY_RELATES_TO",
            "Memory",
            "Memory",
            relationship_is_active,
        ),
        changed_memory_count: cutoff.as_ref().map_or(0, |cutoff| {
            count_changed_nodes_with_label(catalog, store, "Memory", cutoff)
        }),
        changed_entity_count: cutoff.as_ref().map_or(0, |cutoff| {
            count_changed_nodes_with_label(catalog, store, "Entity", cutoff)
        }),
        changed_mention_edge_count: cutoff.as_ref().map_or(0, |cutoff| {
            count_relationships_between_labels(
                catalog,
                store,
                "MENTIONS",
                "Memory",
                "Entity",
                |r| relationship_changed_since(r, cutoff),
            )
        }),
        changed_entity_relation_count: cutoff.as_ref().map_or(0, |cutoff| {
            count_relationships_between_labels(
                catalog,
                store,
                "RELATES_TO",
                "Entity",
                "Entity",
                |r| relationship_changed_since(r, cutoff),
            )
        }),
        changed_memory_relation_count: cutoff.as_ref().map_or(0, |cutoff| {
            count_relationships_between_labels(
                catalog,
                store,
                "MEMORY_RELATES_TO",
                "Memory",
                "Memory",
                |r| relationship_is_active(r) && relationship_changed_since(r, cutoff),
            )
        }),
    }
}

fn knowledge_pagerank_membership_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgePageRankMembershipRequest,
) -> Result<KnowledgePageRankMembershipOutput> {
    validate_pagerank_label(request.label.as_str())?;
    validate_non_empty_external_ids(
        &request.external_ids,
        "knowledge pagerank membership requires non-empty external ids",
    )?;
    let label = pagerank_label(request.label.as_str());
    let mut rows = Vec::with_capacity(request.external_ids.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    for external_id in &request.external_ids {
        let node = seed_node_by_label_and_external_id(catalog, store, label, external_id);
        if let Some(node) = node {
            matched_count += 1;
            rows.push(KnowledgePageRankMembershipRow {
                label: label.to_string(),
                external_id: external_id.clone(),
                node_id: Some(node.id.0),
                matched: true,
            });
        } else {
            missing_count += 1;
            rows.push(KnowledgePageRankMembershipRow {
                label: label.to_string(),
                external_id: external_id.clone(),
                node_id: None,
                matched: false,
            });
        }
    }

    Ok(KnowledgePageRankMembershipOutput {
        graph_commit_epoch: store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
    })
}

fn knowledge_pagerank_memory_visibility_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgePageRankMemoryVisibilityRequest,
) -> Result<KnowledgePageRankMemoryVisibilityOutput> {
    validate_non_empty_external_ids(
        &request.memory_ids,
        "knowledge pagerank memory visibility requires non-empty memory ids",
    )?;
    let mut rows = Vec::with_capacity(request.memory_ids.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    for memory_id in &request.memory_ids {
        let Some(node) = seed_node_by_label_and_external_id(catalog, store, "Memory", memory_id)
        else {
            missing_count += 1;
            rows.push(KnowledgePageRankMemoryVisibilityRow {
                memory_id: memory_id.clone(),
                node_id: None,
                matched: false,
                metadata: None,
                is_latest: true,
            });
            continue;
        };
        matched_count += 1;
        rows.push(KnowledgePageRankMemoryVisibilityRow {
            memory_id: memory_id.clone(),
            node_id: Some(node.id.0),
            matched: true,
            metadata: node.properties.get("metadata").cloned(),
            is_latest: node.properties.get("is_latest") != Some(&Value::Bool(false)),
        });
    }

    Ok(KnowledgePageRankMemoryVisibilityOutput {
        graph_commit_epoch: store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
    })
}

fn knowledge_pagerank_central_entity_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgePageRankCentralEntityRequest,
) -> Result<KnowledgePageRankCentralEntityOutput> {
    if request.entity_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge pagerank central entity requires a non-empty entity id".to_string(),
        ));
    }
    let node = seed_node_by_label_and_external_id(catalog, store, "Entity", &request.entity_id);
    Ok(KnowledgePageRankCentralEntityOutput {
        graph_commit_epoch: store.commit_epoch(),
        found: node.is_some(),
        node_id: node.map(|node| node.id.0),
        name: node
            .and_then(|node| node.properties.get("name"))
            .map(value_to_external_id)
            .filter(|name| !name.is_empty()),
    })
}

fn count_nodes_with_label(catalog: &Catalog, store: &GraphStore, label: &str) -> usize {
    let Some(label_id) = catalog.label_id(label) else {
        return 0;
    };
    store.scan_nodes(Some(label_id)).count()
}

fn count_changed_nodes_with_label(
    catalog: &Catalog,
    store: &GraphStore,
    label: &str,
    cutoff: &Value,
) -> usize {
    let Some(label_id) = catalog.label_id(label) else {
        return 0;
    };
    store
        .scan_nodes(Some(label_id))
        .filter(|node| node_changed_since(node, cutoff))
        .count()
}

fn count_relationships_between_labels(
    catalog: &Catalog,
    store: &GraphStore,
    rel_type: &str,
    source_label: &str,
    target_label: &str,
    predicate: impl Fn(&RelRecord) -> bool,
) -> usize {
    let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
        return 0;
    };
    let Some(source_label_id) = catalog.label_id(source_label) else {
        return 0;
    };
    let Some(target_label_id) = catalog.label_id(target_label) else {
        return 0;
    };
    store
        .scan_relationships(Some(rel_type_id))
        .filter(|relationship| {
            predicate(relationship)
                && store
                    .node(relationship.source)
                    .is_some_and(|node| node.labels.contains(&source_label_id))
                && store
                    .node(relationship.target)
                    .is_some_and(|node| node.labels.contains(&target_label_id))
        })
        .count()
}

fn node_changed_since(node: &NodeRecord, cutoff: &Value) -> bool {
    node.properties
        .get("created_at")
        .is_some_and(|value| value_is_greater(value, cutoff))
        || node
            .properties
            .get("updated_at")
            .is_some_and(|value| value_is_greater(value, cutoff))
}

fn relationship_changed_since(relationship: &RelRecord, cutoff: &Value) -> bool {
    relationship
        .properties
        .get("created_at")
        .is_some_and(|value| value_is_greater(value, cutoff))
        || relationship
            .properties
            .get("updated_at")
            .is_some_and(|value| value_is_greater(value, cutoff))
}

fn relationship_is_active(relationship: &RelRecord) -> bool {
    relationship
        .properties
        .get("status")
        .is_some_and(|value| value_to_external_id(value) == "active")
}

fn validate_non_empty_external_ids(external_ids: &[String], message: &str) -> Result<()> {
    if external_ids.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(message.to_string()));
    }
    Ok(())
}

fn validate_pagerank_label(label: &str) -> Result<()> {
    match label {
        "Memory" | "memory" | "Entity" | "entity" => Ok(()),
        _ => Err(SkeinError::Semantic(
            "knowledge pagerank operations support only Memory and Entity labels".to_string(),
        )),
    }
}

fn pagerank_label(label: &str) -> &'static str {
    match label {
        "Memory" | "memory" => "Memory",
        "Entity" | "entity" => "Entity",
        _ => unreachable!("pagerank label should be validated before canonicalization"),
    }
}

fn clear_knowledge_community_assignments_for(
    db: &mut Database,
    request: &KnowledgeCommunityAssignmentClearRequest,
) -> Result<KnowledgeCommunityAssignmentClearOutput> {
    db.ensure_writable()?;
    for label in &request.labels {
        validate_cypher_identifier(label.as_str(), "node label")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::new();
    let mut candidate_count = 0;
    let mut cleared_count = 0;
    let mut eligible_updates = Vec::new();
    let mut seen_node_ids = BTreeSet::new();

    if request.labels.is_empty() {
        collect_knowledge_community_assignment_clears(
            db,
            None,
            &mut seen_node_ids,
            &mut rows,
            &mut eligible_updates,
            &mut candidate_count,
            &mut cleared_count,
        );
    } else {
        for label in &request.labels {
            let Some(label_id) = db.catalog.label_id(label.as_str()) else {
                continue;
            };
            collect_knowledge_community_assignment_clears(
                db,
                Some(label_id),
                &mut seen_node_ids,
                &mut rows,
                &mut eligible_updates,
                &mut candidate_count,
                &mut cleared_count,
            );
        }
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeCommunityAssignmentClearOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count,
            cleared_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for node_id in &eligible_updates {
        let (cypher, parameters) = community_assignment_clear_statement(*node_id);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeCommunityAssignmentClearOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        candidate_count,
        cleared_count,
    })
}

fn collect_knowledge_community_assignment_clears(
    db: &Database,
    label_id: Option<LabelId>,
    seen_node_ids: &mut BTreeSet<NodeId>,
    rows: &mut Vec<KnowledgeCommunityAssignmentClearRow>,
    eligible_updates: &mut Vec<NodeId>,
    candidate_count: &mut usize,
    cleared_count: &mut usize,
) {
    for node in db.store.scan_nodes(label_id) {
        if !seen_node_ids.insert(node.id) {
            continue;
        }
        if node
            .properties
            .get("community_id")
            .is_none_or(|value| value == &Value::Null)
        {
            continue;
        }
        *candidate_count += 1;
        *cleared_count += 1;
        eligible_updates.push(node.id);
        rows.push(KnowledgeCommunityAssignmentClearRow {
            labels: node_label_names(&db.catalog, node),
            external_id: node_external_id(node),
            node_id: node.id.0,
            cleared: true,
        });
    }
}

fn community_assignment_clear_statement(node_id: NodeId) -> (String, BTreeMap<String, Value>) {
    (
        "MATCH (n) WHERE id(n) = $node_id SET n.community_id = $community_id".to_string(),
        BTreeMap::from([
            ("node_id".to_string(), Value::Int(node_id.0 as i64)),
            ("community_id".to_string(), Value::Null),
        ]),
    )
}

fn create_knowledge_community_memberships_batch_for(
    db: &mut Database,
    request: &KnowledgeCommunityMembershipCreateBatchRequest,
) -> Result<KnowledgeCommunityMembershipCreateBatchOutput> {
    db.ensure_writable()?;
    for membership in &request.memberships {
        validate_knowledge_community_membership_create(membership)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let creates = request
        .memberships
        .iter()
        .map(knowledge_community_membership_relationship_create)
        .collect::<Vec<_>>();
    let output = create_knowledge_relationship_batch_for(
        db,
        &KnowledgeRelationshipCreateBatchRequest { creates },
    )?;
    let rows = output
        .rows
        .into_iter()
        .map(|row| KnowledgeCommunityMembershipCreateBatchRow {
            entity_id: row.source.external_id,
            community_id: row.target.external_id,
            entity_node_id: row.source_node_id,
            community_node_id: row.target_node_id,
            matched: row.matched,
            non_writable: row.non_writable,
            created: row.matched,
        })
        .collect::<Vec<_>>();

    Ok(KnowledgeCommunityMembershipCreateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows,
        matched_count: output.matched_count,
        missing_endpoint_count: output.missing_endpoint_count,
        non_writable_count: output.non_writable_count,
        created_relationship_count: output.created_relationship_count,
    })
}

fn validate_knowledge_community_membership_create(
    membership: &KnowledgeCommunityMembershipCreate,
) -> Result<()> {
    if membership.entity_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community membership create requires a non-empty entity_id".to_string(),
        ));
    }
    if membership.community_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community membership create requires a non-empty community_id".to_string(),
        ));
    }
    if !membership.strength.is_finite() {
        return Err(SkeinError::Semantic(
            "knowledge community membership strength must be finite".to_string(),
        ));
    }
    Ok(())
}

fn knowledge_community_membership_relationship_create(
    membership: &KnowledgeCommunityMembershipCreate,
) -> KnowledgeRelationshipCreateRequest {
    KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: membership.entity_id.clone(),
        },
        target: KnowledgeEntityRequest {
            label: "Community".to_string(),
            external_id: membership.community_id.clone(),
        },
        relationship_type: "BELONGS_TO".to_string(),
        properties: BTreeMap::from([
            ("strength".to_string(), Value::Float(membership.strength)),
            ("created_at".to_string(), membership.created_at.clone()),
            ("properties".to_string(), membership.properties.clone()),
        ]),
    }
}

fn update_knowledge_communities_batch_for(
    db: &mut Database,
    request: &KnowledgeCommunityLifecycleBatchRequest,
) -> Result<KnowledgeCommunityLifecycleBatchOutput> {
    db.ensure_writable()?;
    for create in &request.creates {
        validate_knowledge_community_create(create)?;
    }
    for update in &request.summary_updates {
        validate_knowledge_community_summary_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut create_rows = Vec::with_capacity(request.creates.len());
    let mut summary_update_rows = Vec::with_capacity(request.summary_updates.len());
    let mut created_count = 0;
    let mut already_exists_count = 0;
    let mut duplicate_count = 0;
    let mut updated_count = 0;
    let mut missing_count = 0;
    let mut non_writable_count = 0;
    let mut updated_property_count = 0;
    let mut pending_create_ids = BTreeSet::new();
    let mut pending_update_node_ids = BTreeSet::new();
    let mut eligible_creates = Vec::new();
    let mut eligible_updates = Vec::new();

    for create in &request.creates {
        if let Some(existing) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Community", &create.id)
        {
            already_exists_count += 1;
            create_rows.push(KnowledgeCommunityCreateBatchRow {
                id: create.id.clone(),
                node_id: Some(existing.id.0),
                created: false,
                already_exists: true,
                duplicate: false,
            });
            continue;
        }
        if !pending_create_ids.insert(create.id.clone()) {
            duplicate_count += 1;
            create_rows.push(KnowledgeCommunityCreateBatchRow {
                id: create.id.clone(),
                node_id: None,
                created: false,
                already_exists: false,
                duplicate: true,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(knowledge_community_create_entity_request(create));
        create_rows.push(KnowledgeCommunityCreateBatchRow {
            id: create.id.clone(),
            node_id: None,
            created: true,
            already_exists: false,
            duplicate: false,
        });
    }

    for update in &request.summary_updates {
        let Some(seed) =
            seed_node_by_label_and_external_id(&db.catalog, &db.store, "Community", &update.id)
        else {
            missing_count += 1;
            summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
                id: update.id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                missing: true,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        if !node_has_external_id_property(seed, update.id.as_str()) {
            non_writable_count += 1;
            summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
                id: update.id.clone(),
                node_id: Some(seed.id.0),
                matched: false,
                updated: false,
                missing: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_update_node_ids.insert(seed.id) {
            duplicate_count += 1;
            summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
                id: update.id.clone(),
                node_id: Some(seed.id.0),
                matched: true,
                updated: false,
                missing: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = knowledge_community_summary_assignments(update);
        let row_updated_property_count = assignments.len();
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((seed.id, assignments));
        summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
            id: update.id.clone(),
            node_id: Some(seed.id.0),
            matched: true,
            updated: true,
            missing: false,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_creates.is_empty() && eligible_updates.is_empty() {
        return Ok(KnowledgeCommunityLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            create_rows,
            summary_update_rows,
            created_count,
            already_exists_count,
            duplicate_count,
            updated_count,
            missing_count,
            non_writable_count,
            created_node_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_community_summary_update_statement(*node_id, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    for row in &mut create_rows {
        if row.created {
            row.node_id =
                seed_node_by_label_and_external_id(&db.catalog, &db.store, "Community", &row.id)
                    .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeCommunityLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        create_rows,
        summary_update_rows,
        created_count,
        already_exists_count,
        duplicate_count,
        updated_count,
        missing_count,
        non_writable_count,
        created_node_count: output.rows.len().saturating_sub(eligible_updates.len()),
        updated_property_count,
    })
}

fn validate_knowledge_community_create(create: &KnowledgeCommunityCreate) -> Result<()> {
    if create.id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community create requires a non-empty id".to_string(),
        ));
    }
    if create.name.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community create requires a non-empty name".to_string(),
        ));
    }
    if create.community_id < 0 {
        return Err(SkeinError::Semantic(
            "knowledge community create requires non-negative community_id".to_string(),
        ));
    }
    if create.member_count < 0 {
        return Err(SkeinError::Semantic(
            "knowledge community create requires non-negative member_count".to_string(),
        ));
    }
    if !create.resolution.is_finite() {
        return Err(SkeinError::Semantic(
            "knowledge community create resolution must be finite".to_string(),
        ));
    }
    Ok(())
}

fn validate_knowledge_community_summary_update(
    update: &KnowledgeCommunitySummaryUpdate,
) -> Result<()> {
    if update.id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community summary update requires a non-empty id".to_string(),
        ));
    }
    if update.name.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community summary update requires a non-empty name".to_string(),
        ));
    }
    Ok(())
}

fn knowledge_community_create_entity_request(
    create: &KnowledgeCommunityCreate,
) -> KnowledgeEntityCreateRequest {
    KnowledgeEntityCreateRequest {
        label: "Community".to_string(),
        external_id: create.id.clone(),
        properties: BTreeMap::from([
            ("community_id".to_string(), Value::Int(create.community_id)),
            ("name".to_string(), Value::String(create.name.clone())),
            ("description".to_string(), create.description.clone()),
            ("ai_summary".to_string(), create.ai_summary.clone()),
            ("member_count".to_string(), Value::Int(create.member_count)),
            (
                "algorithm".to_string(),
                Value::String("louvain".to_string()),
            ),
            ("resolution".to_string(), Value::Float(create.resolution)),
            ("created_at".to_string(), create.created_at.clone()),
            ("updated_at".to_string(), create.updated_at.clone()),
        ]),
    }
}

fn knowledge_community_summary_assignments(
    update: &KnowledgeCommunitySummaryUpdate,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("name".to_string(), Value::String(update.name.clone())),
        ("description".to_string(), update.description.clone()),
        ("ai_summary".to_string(), update.ai_summary.clone()),
        ("updated_at".to_string(), update.updated_at.clone()),
    ])
}

fn knowledge_community_summary_update_statement(
    node_id: NodeId,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "MATCH (c:Community) WHERE id(c) = $node_id SET ".to_string();
    let mut parameters = BTreeMap::from([("node_id".to_string(), Value::Int(node_id.0 as i64))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!("c.{property} = ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    (cypher, parameters)
}

fn delete_knowledge_communities_for(
    db: &mut Database,
    request: &KnowledgeCommunityCleanupRequest,
) -> Result<KnowledgeCommunityCleanupOutput> {
    db.ensure_writable()?;
    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(label_id) = db.catalog.label_id("Community") else {
        return Ok(KnowledgeCommunityCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            deleted_count: 0,
        });
    };

    let mut rows = db
        .store
        .scan_nodes(Some(label_id))
        .map(|node| KnowledgeCommunityCleanupRow {
            id: node_external_id(node),
            node_id: node.id.0,
            deleted: true,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.node_id);

    if rows.is_empty() {
        return Ok(KnowledgeCommunityCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count: 0,
            deleted_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for row in &rows {
        let (cypher, parameters) =
            knowledge_community_cleanup_statement(NodeId(row.node_id), request.detach)?;
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    let deleted_count = rows.len();

    Ok(KnowledgeCommunityCleanupOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        candidate_count: rows.len(),
        rows,
        deleted_count,
    })
}

fn knowledge_community_cleanup_statement(
    node_id: NodeId,
    detach: bool,
) -> Result<(String, BTreeMap<String, Value>)> {
    let node_id = i64::try_from(node_id.0)
        .map_err(|_| SkeinError::Semantic("node id does not fit Cypher integer".to_string()))?;
    let verb = if detach { "DETACH DELETE" } else { "DELETE" };
    Ok((
        format!("MATCH (c:Community) WHERE id(c) = $node_id {verb} c"),
        BTreeMap::from([("node_id".to_string(), Value::Int(node_id))]),
    ))
}

fn knowledge_graph_meta_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeGraphMetaRequest,
) -> Result<KnowledgeGraphMetaOutput> {
    validate_graph_meta_request(request)?;
    let graph_commit_epoch = store.commit_epoch();
    let meta = node_by_label_property_external_id(
        catalog,
        store,
        "GraphMeta",
        "meta_id",
        &request.meta_id,
    )
    .map(knowledge_graph_meta_from_node);
    Ok(KnowledgeGraphMetaOutput {
        graph_commit_epoch,
        found: meta.is_some(),
        meta,
    })
}

fn delete_knowledge_graph_meta_for(
    db: &mut Database,
    request: &KnowledgeGraphMetaRequest,
) -> Result<KnowledgeGraphMetaDeleteOutput> {
    db.ensure_writable()?;
    validate_graph_meta_request(request)?;
    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(node_id) = node_by_label_property_external_id(
        &db.catalog,
        &db.store,
        "GraphMeta",
        "meta_id",
        &request.meta_id,
    )
    .map(|node| node.id) else {
        return Ok(KnowledgeGraphMetaDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: None,
            matched: false,
            deleted: false,
        });
    };

    let (cypher, parameters) = knowledge_graph_meta_delete_statement(node_id)?;
    let mut tx = db.begin_transaction();
    tx.query_with_params(cypher.as_str(), &parameters)?;
    tx.commit()?;

    Ok(KnowledgeGraphMetaDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id: Some(node_id.0),
        matched: true,
        deleted: true,
    })
}

fn validate_graph_meta_request(request: &KnowledgeGraphMetaRequest) -> Result<()> {
    if request.meta_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge graph meta request requires a non-empty meta id".to_string(),
        ));
    }
    Ok(())
}

fn knowledge_graph_meta_from_node(node: &NodeRecord) -> KnowledgeGraphMeta {
    KnowledgeGraphMeta {
        meta_id: node
            .properties
            .get("meta_id")
            .map(value_to_external_id)
            .filter(|meta_id| !meta_id.is_empty()),
        node_id: node.id.0,
        properties: node.properties.clone(),
    }
}

fn knowledge_graph_meta_delete_statement(
    node_id: NodeId,
) -> Result<(String, BTreeMap<String, Value>)> {
    let node_id = i64::try_from(node_id.0)
        .map_err(|_| SkeinError::Semantic("node id does not fit Cypher integer".to_string()))?;
    Ok((
        "MATCH (m:GraphMeta) WHERE id(m) = $node_id DELETE m".to_string(),
        BTreeMap::from([("node_id".to_string(), Value::Int(node_id))]),
    ))
}

fn stamp_knowledge_graph_meta_batch_for(
    db: &mut Database,
    request: &KnowledgeGraphMetaStampBatchRequest,
) -> Result<KnowledgeGraphMetaStampBatchOutput> {
    db.ensure_writable()?;
    for stamp in &request.stamps {
        if stamp.meta_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge graph meta stamp requires a non-empty meta id".to_string(),
            ));
        }
        if stamp.assignments.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge graph meta stamp requires at least one assignment".to_string(),
            ));
        }
        for property in stamp.assignments.keys() {
            validate_cypher_identifier(property, "property")?;
            if property == "meta_id" {
                return Err(SkeinError::Semantic(
                    "knowledge graph meta stamp cannot update meta_id".to_string(),
                ));
            }
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.stamps.len());
    let mut created_count = 0;
    let mut updated_count = 0;
    let mut duplicate_count = 0;
    let mut updated_property_count = 0;
    let mut pending_meta_ids = BTreeSet::new();
    let mut eligible_stamps = Vec::new();

    for stamp in &request.stamps {
        if !pending_meta_ids.insert(stamp.meta_id.clone()) {
            duplicate_count += 1;
            rows.push(KnowledgeGraphMetaStampBatchRow {
                meta_id: stamp.meta_id.clone(),
                node_id: None,
                created: false,
                updated: false,
                duplicate: true,
                updated_property_count: 0,
            });
            continue;
        }

        let existing = node_by_label_property_external_id(
            &db.catalog,
            &db.store,
            "GraphMeta",
            "meta_id",
            stamp.meta_id.as_str(),
        );
        let updated_properties = stamp.assignments.len();
        updated_property_count += updated_properties;
        match existing {
            Some(node) => {
                updated_count += 1;
                eligible_stamps.push(GraphMetaStampOperation::Update {
                    node_id: node.id,
                    assignments: stamp.assignments.clone(),
                });
                rows.push(KnowledgeGraphMetaStampBatchRow {
                    meta_id: stamp.meta_id.clone(),
                    node_id: Some(node.id.0),
                    created: false,
                    updated: true,
                    duplicate: false,
                    updated_property_count: updated_properties,
                });
            }
            None => {
                created_count += 1;
                eligible_stamps.push(GraphMetaStampOperation::Create {
                    meta_id: stamp.meta_id.clone(),
                    assignments: stamp.assignments.clone(),
                });
                rows.push(KnowledgeGraphMetaStampBatchRow {
                    meta_id: stamp.meta_id.clone(),
                    node_id: None,
                    created: true,
                    updated: false,
                    duplicate: false,
                    updated_property_count: updated_properties,
                });
            }
        }
    }

    if eligible_stamps.is_empty() {
        return Ok(KnowledgeGraphMetaStampBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count: 0,
            updated_count: 0,
            duplicate_count,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for operation in &eligible_stamps {
        let (cypher, parameters) = graph_meta_stamp_statement(operation);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    for row in &mut rows {
        if row.created {
            row.node_id = node_by_label_property_external_id(
                &db.catalog,
                &db.store,
                "GraphMeta",
                "meta_id",
                row.meta_id.as_str(),
            )
            .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeGraphMetaStampBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        updated_count,
        duplicate_count,
        updated_property_count,
    })
}

enum GraphMetaStampOperation {
    Create {
        meta_id: String,
        assignments: BTreeMap<String, Value>,
    },
    Update {
        node_id: NodeId,
        assignments: BTreeMap<String, Value>,
    },
}

fn graph_meta_stamp_statement(
    operation: &GraphMetaStampOperation,
) -> (String, BTreeMap<String, Value>) {
    match operation {
        GraphMetaStampOperation::Create {
            meta_id,
            assignments,
        } => graph_meta_create_statement(meta_id, assignments),
        GraphMetaStampOperation::Update {
            node_id,
            assignments,
        } => knowledge_property_update_statement("GraphMeta", node_id.0, assignments),
    }
}

fn graph_meta_create_statement(
    meta_id: &str,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "CREATE (:GraphMeta {meta_id: $meta_id".to_string();
    let mut parameters =
        BTreeMap::from([("meta_id".to_string(), Value::String(meta_id.to_string()))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!(", {property}: ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    cypher.push_str("})");
    (cypher, parameters)
}

fn apply_knowledge_schema_migrations_batch_for(
    db: &mut Database,
    request: &KnowledgeSchemaMigrationApplyBatchRequest,
) -> Result<KnowledgeSchemaMigrationApplyBatchOutput> {
    db.ensure_writable()?;
    for migration in &request.migrations {
        if migration.migration_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge schema migration apply requires a non-empty migration id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.migrations.len());
    let mut created_count = 0;
    let mut already_applied_count = 0;
    let mut duplicate_count = 0;
    let mut pending_migration_ids = BTreeSet::new();
    let mut eligible_creates = Vec::new();

    for migration in &request.migrations {
        let existing = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "SchemaMigrationLog",
            migration.migration_id.as_str(),
        );
        if let Some(node) = existing {
            already_applied_count += 1;
            rows.push(KnowledgeSchemaMigrationApplyBatchRow {
                migration_id: migration.migration_id.clone(),
                node_id: Some(node.id.0),
                created: false,
                already_applied: true,
                duplicate: false,
            });
            continue;
        }
        if !pending_migration_ids.insert(migration.migration_id.clone()) {
            duplicate_count += 1;
            rows.push(KnowledgeSchemaMigrationApplyBatchRow {
                migration_id: migration.migration_id.clone(),
                node_id: None,
                created: false,
                already_applied: false,
                duplicate: true,
            });
            continue;
        }

        created_count += 1;
        let create = KnowledgeEntityCreateRequest {
            label: "SchemaMigrationLog".to_string(),
            external_id: migration.migration_id.clone(),
            properties: BTreeMap::from([("applied_at".to_string(), migration.applied_at.clone())]),
        };
        eligible_creates.push(create);
        rows.push(KnowledgeSchemaMigrationApplyBatchRow {
            migration_id: migration.migration_id.clone(),
            node_id: None,
            created: true,
            already_applied: false,
            duplicate: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeSchemaMigrationApplyBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count: 0,
            already_applied_count,
            duplicate_count,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    for row in &mut rows {
        if row.created {
            row.node_id = seed_node_by_label_and_external_id(
                &db.catalog,
                &db.store,
                "SchemaMigrationLog",
                row.migration_id.as_str(),
            )
            .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeSchemaMigrationApplyBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        already_applied_count,
        duplicate_count,
    })
}

fn knowledge_schema_migrations_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSchemaMigrationListRequest,
) -> KnowledgeSchemaMigrationListOutput {
    let Some(label_id) = catalog.label_id("SchemaMigrationLog") else {
        return KnowledgeSchemaMigrationListOutput {
            graph_commit_epoch: store.commit_epoch(),
            rows: Vec::new(),
            matched_count: 0,
            returned_count: 0,
        };
    };
    let mut rows = store
        .scan_nodes(Some(label_id))
        .filter_map(|node| {
            let migration_id = node_external_id(node)?;
            Some(KnowledgeSchemaMigrationRow {
                migration_id,
                node_id: node.id.0,
                applied_at: node.properties.get("applied_at").cloned(),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.migration_id.cmp(&right.migration_id));
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();
    KnowledgeSchemaMigrationListOutput {
        graph_commit_epoch: store.commit_epoch(),
        rows,
        matched_count,
        returned_count,
    }
}

fn update_knowledge_augmentation_jobs_batch_for(
    db: &mut Database,
    request: &KnowledgeAugmentationJobLifecycleBatchRequest,
) -> Result<KnowledgeAugmentationJobLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_augmentation_job_lifecycle_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut created_count = 0;
    let mut updated_count = 0;
    let mut missing_count = 0;
    let mut already_exists_count = 0;
    let mut status_mismatch_count = 0;
    let mut duplicate_count = 0;
    let mut updated_property_count = 0;
    let mut pending_job_ids = BTreeSet::new();
    let mut eligible_operations = Vec::new();

    for update in &request.updates {
        if !pending_job_ids.insert(update.job_id.clone()) {
            duplicate_count += 1;
            rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                job_id: update.job_id.clone(),
                node_id: None,
                created: false,
                updated: false,
                missing: false,
                already_exists: false,
                status_mismatch: false,
                duplicate: true,
                updated_property_count: 0,
            });
            continue;
        }

        match &update.transition {
            KnowledgeAugmentationJobLifecycleTransition::Create { .. } => {
                if let Some(existing) = node_by_label_property_external_id(
                    &db.catalog,
                    &db.store,
                    "AugmentationJob",
                    "job_id",
                    update.job_id.as_str(),
                ) {
                    already_exists_count += 1;
                    rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                        job_id: update.job_id.clone(),
                        node_id: Some(existing.id.0),
                        created: false,
                        updated: false,
                        missing: false,
                        already_exists: true,
                        status_mismatch: false,
                        duplicate: false,
                        updated_property_count: 0,
                    });
                    continue;
                }
                let assignments = augmentation_job_create_assignments(update);
                let row_updated_property_count = assignments.len();
                updated_property_count += row_updated_property_count;
                created_count += 1;
                eligible_operations.push(AugmentationJobLifecycleOperation::Create {
                    job_id: update.job_id.clone(),
                    assignments,
                });
                rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                    job_id: update.job_id.clone(),
                    node_id: None,
                    created: true,
                    updated: false,
                    missing: false,
                    already_exists: false,
                    status_mismatch: false,
                    duplicate: false,
                    updated_property_count: row_updated_property_count,
                });
            }
            _ => {
                let Some(existing) = node_by_label_property_external_id(
                    &db.catalog,
                    &db.store,
                    "AugmentationJob",
                    "job_id",
                    update.job_id.as_str(),
                ) else {
                    missing_count += 1;
                    rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                        job_id: update.job_id.clone(),
                        node_id: None,
                        created: false,
                        updated: false,
                        missing: true,
                        already_exists: false,
                        status_mismatch: false,
                        duplicate: false,
                        updated_property_count: 0,
                    });
                    continue;
                };
                if !augmentation_job_transition_allows_status(
                    &update.transition,
                    augmentation_job_status(existing).as_deref(),
                ) {
                    status_mismatch_count += 1;
                    rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                        job_id: update.job_id.clone(),
                        node_id: Some(existing.id.0),
                        created: false,
                        updated: false,
                        missing: false,
                        already_exists: false,
                        status_mismatch: true,
                        duplicate: false,
                        updated_property_count: 0,
                    });
                    continue;
                }
                let assignments = augmentation_job_transition_assignments(&update.transition);
                let row_updated_property_count = assignments.len();
                updated_property_count += row_updated_property_count;
                updated_count += 1;
                eligible_operations.push(AugmentationJobLifecycleOperation::Update {
                    node_id: existing.id,
                    assignments,
                });
                rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                    job_id: update.job_id.clone(),
                    node_id: Some(existing.id.0),
                    created: false,
                    updated: true,
                    missing: false,
                    already_exists: false,
                    status_mismatch: false,
                    duplicate: false,
                    updated_property_count: row_updated_property_count,
                });
            }
        }
    }

    if eligible_operations.is_empty() {
        return Ok(KnowledgeAugmentationJobLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count: 0,
            updated_count: 0,
            missing_count,
            already_exists_count,
            status_mismatch_count,
            duplicate_count,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for operation in &eligible_operations {
        let (cypher, parameters) = augmentation_job_lifecycle_statement(operation);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    for row in &mut rows {
        if row.created {
            row.node_id = node_by_label_property_external_id(
                &db.catalog,
                &db.store,
                "AugmentationJob",
                "job_id",
                row.job_id.as_str(),
            )
            .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeAugmentationJobLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        updated_count,
        missing_count,
        already_exists_count,
        status_mismatch_count,
        duplicate_count,
        updated_property_count,
    })
}

enum AugmentationJobLifecycleOperation {
    Create {
        job_id: String,
        assignments: BTreeMap<String, Value>,
    },
    Update {
        node_id: NodeId,
        assignments: BTreeMap<String, Value>,
    },
}

fn validate_augmentation_job_lifecycle_update(
    update: &KnowledgeAugmentationJobLifecycleUpdate,
) -> Result<()> {
    if update.job_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge augmentation job update requires a non-empty job id".to_string(),
        ));
    }
    match &update.transition {
        KnowledgeAugmentationJobLifecycleTransition::Create { job_type, .. } => {
            if job_type.is_empty() {
                return Err(SkeinError::Semantic(
                    "knowledge augmentation job create requires a non-empty job type".to_string(),
                ));
            }
        }
        KnowledgeAugmentationJobLifecycleTransition::UpdateProgress { progress, message } => {
            if !progress.is_finite() || *progress < 0.0 || *progress > 100.0 {
                return Err(SkeinError::Semantic(
                    "knowledge augmentation job progress requires a finite percentage".to_string(),
                ));
            }
            if message.is_empty() {
                return Err(SkeinError::Semantic(
                    "knowledge augmentation job progress requires a non-empty message".to_string(),
                ));
            }
        }
        KnowledgeAugmentationJobLifecycleTransition::MarkFailed { error_message, .. }
            if error_message.is_empty() =>
        {
            return Err(SkeinError::Semantic(
                "knowledge augmentation job failure requires a non-empty error message".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn augmentation_job_create_assignments(
    update: &KnowledgeAugmentationJobLifecycleUpdate,
) -> BTreeMap<String, Value> {
    let KnowledgeAugmentationJobLifecycleTransition::Create {
        job_type,
        parameters,
        created_at,
    } = &update.transition
    else {
        unreachable!("augmentation job create assignments require create transition");
    };
    BTreeMap::from([
        ("job_type".to_string(), Value::String(job_type.clone())),
        ("status".to_string(), Value::String("pending".to_string())),
        ("progress".to_string(), Value::Float(0.0)),
        (
            "message".to_string(),
            Value::String("Job created".to_string()),
        ),
        ("parameters".to_string(), parameters.clone()),
        ("result".to_string(), Value::String("{}".to_string())),
        ("error_message".to_string(), Value::String(String::new())),
        ("started_at".to_string(), Value::Null),
        ("completed_at".to_string(), Value::Null),
        ("created_at".to_string(), created_at.clone()),
    ])
}

fn augmentation_job_transition_assignments(
    transition: &KnowledgeAugmentationJobLifecycleTransition,
) -> BTreeMap<String, Value> {
    match transition {
        KnowledgeAugmentationJobLifecycleTransition::MarkRunning { started_at } => {
            BTreeMap::from([
                ("status".to_string(), Value::String("running".to_string())),
                ("started_at".to_string(), started_at.clone()),
                (
                    "message".to_string(),
                    Value::String("Job started".to_string()),
                ),
            ])
        }
        KnowledgeAugmentationJobLifecycleTransition::UpdateProgress { progress, message } => {
            BTreeMap::from([
                ("progress".to_string(), Value::Float(*progress)),
                ("message".to_string(), Value::String(message.clone())),
            ])
        }
        KnowledgeAugmentationJobLifecycleTransition::MarkCompleted {
            result,
            completed_at,
        } => BTreeMap::from([
            ("status".to_string(), Value::String("completed".to_string())),
            ("progress".to_string(), Value::Float(100.0)),
            (
                "message".to_string(),
                Value::String("Job completed successfully".to_string()),
            ),
            ("result".to_string(), result.clone()),
            ("completed_at".to_string(), completed_at.clone()),
        ]),
        KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
            error_message,
            completed_at,
        } => BTreeMap::from([
            ("status".to_string(), Value::String("failed".to_string())),
            (
                "message".to_string(),
                Value::String("Job failed".to_string()),
            ),
            (
                "error_message".to_string(),
                Value::String(error_message.clone()),
            ),
            ("completed_at".to_string(), completed_at.clone()),
        ]),
        KnowledgeAugmentationJobLifecycleTransition::Create { .. } => {
            unreachable!("augmentation job create is handled separately")
        }
    }
}

fn augmentation_job_transition_allows_status(
    transition: &KnowledgeAugmentationJobLifecycleTransition,
    status: Option<&str>,
) -> bool {
    match transition {
        KnowledgeAugmentationJobLifecycleTransition::MarkRunning { .. } => {
            status == Some("pending")
        }
        KnowledgeAugmentationJobLifecycleTransition::UpdateProgress { .. }
        | KnowledgeAugmentationJobLifecycleTransition::MarkCompleted { .. } => {
            status == Some("running")
        }
        KnowledgeAugmentationJobLifecycleTransition::MarkFailed { .. } => {
            matches!(status, Some("pending" | "running"))
        }
        KnowledgeAugmentationJobLifecycleTransition::Create { .. } => false,
    }
}

fn augmentation_job_status(node: &NodeRecord) -> Option<String> {
    match node.properties.get("status") {
        Some(Value::String(status)) => Some(status.clone()),
        _ => None,
    }
}

fn augmentation_job_lifecycle_statement(
    operation: &AugmentationJobLifecycleOperation,
) -> (String, BTreeMap<String, Value>) {
    match operation {
        AugmentationJobLifecycleOperation::Create {
            job_id,
            assignments,
        } => augmentation_job_create_statement(job_id, assignments),
        AugmentationJobLifecycleOperation::Update {
            node_id,
            assignments,
        } => knowledge_property_update_statement("AugmentationJob", node_id.0, assignments),
    }
}

fn augmentation_job_create_statement(
    job_id: &str,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "CREATE (:AugmentationJob {job_id: $job_id".to_string();
    let mut parameters =
        BTreeMap::from([("job_id".to_string(), Value::String(job_id.to_string()))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!(", {property}: ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    cypher.push_str("})");
    (cypher, parameters)
}

fn knowledge_augmentation_job_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeAugmentationJobRequest,
) -> Result<KnowledgeAugmentationJobOutput> {
    if request.job_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge augmentation job read requires a non-empty job id".to_string(),
        ));
    }

    let graph_commit_epoch = store.commit_epoch();
    let job = node_by_label_property_external_id(
        catalog,
        store,
        "AugmentationJob",
        "job_id",
        request.job_id.as_str(),
    )
    .map(knowledge_augmentation_job_from_node);
    Ok(KnowledgeAugmentationJobOutput {
        graph_commit_epoch,
        found: job.is_some(),
        job,
    })
}

fn knowledge_augmentation_jobs_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeAugmentationJobListRequest,
) -> Result<KnowledgeAugmentationJobListOutput> {
    if request
        .status_filter
        .as_ref()
        .is_some_and(|status| status.is_empty())
    {
        return Err(SkeinError::Semantic(
            "knowledge augmentation job list requires a non-empty status filter".to_string(),
        ));
    }

    let graph_commit_epoch = store.commit_epoch();
    let Some(label_id) = catalog.label_id("AugmentationJob") else {
        return Ok(KnowledgeAugmentationJobListOutput {
            graph_commit_epoch,
            rows: Vec::new(),
            matched_count: 0,
            returned_count: 0,
        });
    };

    let mut rows = store
        .scan_nodes(Some(label_id))
        .filter(|node| {
            request
                .status_filter
                .as_ref()
                .is_none_or(|status| node_string_property(node, "status").as_ref() == Some(status))
        })
        .map(knowledge_augmentation_job_from_node)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| compare_augmentation_jobs_for_order(left, right, &request.order_by));
    let matched_count = rows.len();
    rows.truncate(request.limit);
    let returned_count = rows.len();

    Ok(KnowledgeAugmentationJobListOutput {
        graph_commit_epoch,
        rows,
        matched_count,
        returned_count,
    })
}

fn knowledge_augmentation_job_from_node(node: &NodeRecord) -> KnowledgeAugmentationJob {
    KnowledgeAugmentationJob {
        job_id: node
            .properties
            .get("job_id")
            .map(value_to_external_id)
            .filter(|job_id| !job_id.is_empty()),
        node_id: node.id.0,
        job_type: node_string_property(node, "job_type"),
        status: node_string_property(node, "status"),
        progress: node_number_property(node, "progress"),
        message: node_string_property(node, "message"),
        result: node.properties.get("result").cloned(),
        error_message: node_string_property(node, "error_message"),
        started_at: node.properties.get("started_at").cloned(),
        completed_at: node.properties.get("completed_at").cloned(),
        created_at: node.properties.get("created_at").cloned(),
    }
}

fn node_string_property(node: &NodeRecord, property_name: &str) -> Option<String> {
    match node.properties.get(property_name) {
        Some(Value::String(value)) => Some(value.clone()),
        _ => None,
    }
}

fn node_number_property(node: &NodeRecord, property_name: &str) -> Option<f64> {
    match node.properties.get(property_name) {
        Some(Value::Int(value)) => Some(*value as f64),
        Some(Value::Float(value)) if value.is_finite() => Some(*value),
        _ => None,
    }
}

fn compare_augmentation_jobs_for_order(
    left: &KnowledgeAugmentationJob,
    right: &KnowledgeAugmentationJob,
    order: &KnowledgeAugmentationJobListOrder,
) -> std::cmp::Ordering {
    let left_order = match order {
        KnowledgeAugmentationJobListOrder::StartedAtDesc => left.started_at.as_ref(),
        KnowledgeAugmentationJobListOrder::CreatedAtDesc => left.created_at.as_ref(),
    };
    let right_order = match order {
        KnowledgeAugmentationJobListOrder::StartedAtDesc => right.started_at.as_ref(),
        KnowledgeAugmentationJobListOrder::CreatedAtDesc => right.created_at.as_ref(),
    };
    compare_optional_values_desc(left_order, right_order)
        .then_with(|| left.node_id.cmp(&right.node_id))
}

fn compare_optional_values_desc(left: Option<&Value>, right: Option<&Value>) -> std::cmp::Ordering {
    match (
        left.and_then(value_sort_key),
        right.and_then(value_sort_key),
    ) {
        (Some(left), Some(right)) => right
            .partial_cmp(&left)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn value_sort_key(value: &Value) -> Option<f64> {
    match value {
        Value::Int(value) => Some(*value as f64),
        Value::Float(value) if value.is_finite() => Some(*value),
        _ => None,
    }
}

fn interrupt_knowledge_augmentation_jobs_for(
    db: &mut Database,
    request: &KnowledgeAugmentationJobInterruptRequest,
) -> Result<KnowledgeAugmentationJobInterruptOutput> {
    db.ensure_writable()?;
    if request.error_message.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge augmentation job interrupt requires a non-empty error message".to_string(),
        ));
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(label_id) = db.catalog.label_id("AugmentationJob") else {
        return Ok(KnowledgeAugmentationJobInterruptOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            interrupted_count: 0,
            updated_property_count: 0,
        });
    };

    let mut rows = db
        .store
        .scan_nodes(Some(label_id))
        .filter_map(|node| {
            let previous_status = augmentation_job_status(node)?;
            matches!(previous_status.as_str(), "pending" | "running").then(|| {
                KnowledgeAugmentationJobInterruptRow {
                    job_id: node
                        .properties
                        .get("job_id")
                        .map(value_to_external_id)
                        .filter(|job_id| !job_id.is_empty()),
                    node_id: node.id.0,
                    previous_status,
                    interrupted: true,
                    updated_property_count: 4,
                }
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.node_id);

    if rows.is_empty() {
        return Ok(KnowledgeAugmentationJobInterruptOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count: 0,
            interrupted_count: 0,
            updated_property_count: 0,
        });
    }

    let assignments = BTreeMap::from([
        ("status".to_string(), Value::String("failed".to_string())),
        (
            "message".to_string(),
            Value::String("Interrupted before completion".to_string()),
        ),
        (
            "error_message".to_string(),
            Value::String(request.error_message.clone()),
        ),
        ("completed_at".to_string(), request.completed_at.clone()),
    ]);
    let mut tx = db.begin_transaction();
    for row in &rows {
        let (cypher, parameters) =
            knowledge_property_update_statement("AugmentationJob", row.node_id, &assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    let interrupted_count = rows.len();
    let updated_property_count = rows
        .iter()
        .map(|row| row.updated_property_count)
        .sum::<usize>();

    Ok(KnowledgeAugmentationJobInterruptOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        candidate_count: rows.len(),
        rows,
        interrupted_count,
        updated_property_count,
    })
}

fn delete_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeEntityDeleteRequest,
) -> Result<KnowledgeEntityDeleteOutput> {
    delete_scoped_knowledge_entity_for(
        db,
        &KnowledgeScopedEntityDeleteRequest {
            delete: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn delete_scoped_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeScopedEntityDeleteRequest,
) -> Result<KnowledgeEntityDeleteOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.delete.entity.label, "label")?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(seed) = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.delete.entity.label.as_str(),
        request.delete.entity.external_id.as_str(),
    ) else {
        return Ok(KnowledgeEntityDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: None,
            matched: false,
            filtered_out: false,
            deleted_node_count: 0,
        });
    };
    let node_id = seed.id.0;
    if !node_has_external_id_property(seed, request.delete.entity.external_id.as_str()) {
        return Ok(KnowledgeEntityDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(node_id),
            matched: false,
            filtered_out: false,
            deleted_node_count: 0,
        });
    }
    if !request.metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(&db.catalog, seed, &request.metadata_filters)
    {
        return Ok(KnowledgeEntityDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(node_id),
            matched: false,
            filtered_out: true,
            deleted_node_count: 0,
        });
    }

    let cypher = format!(
        "MATCH (n:{} {{id: $external_id}}) DETACH DELETE n",
        request.delete.entity.label
    );
    let parameters = BTreeMap::from([(
        "external_id".to_string(),
        Value::String(request.delete.entity.external_id.clone()),
    )]);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let deleted_node_count = output.rows.len();
    Ok(KnowledgeEntityDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id: Some(node_id),
        matched: deleted_node_count > 0,
        filtered_out: false,
        deleted_node_count,
    })
}

fn delete_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeEntityDeleteBatchRequest,
) -> Result<KnowledgeEntityDeleteBatchOutput> {
    delete_scoped_knowledge_entity_batch_for(
        db,
        &KnowledgeScopedEntityDeleteBatchRequest {
            delete: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn delete_scoped_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedEntityDeleteBatchRequest,
) -> Result<KnowledgeEntityDeleteBatchOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.delete.label, "label")?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.delete.external_ids.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_external_ids = Vec::new();
    let mut seen_eligible_external_ids = BTreeSet::new();

    for external_id in &request.delete.external_ids {
        let Some(seed) = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            request.delete.label.as_str(),
            external_id.as_str(),
        ) else {
            missing_count += 1;
            rows.push(KnowledgeEntityDeleteBatchRow {
                external_id: external_id.clone(),
                node_id: None,
                matched: false,
                filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id.0;
        if !node_has_external_id_property(seed, external_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeEntityDeleteBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: false,
                non_writable: true,
            });
            continue;
        }
        if !request.metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(&db.catalog, seed, &request.metadata_filters)
        {
            filtered_out_count += 1;
            rows.push(KnowledgeEntityDeleteBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: true,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        if seen_eligible_external_ids.insert(external_id.clone()) {
            eligible_external_ids.push(external_id.clone());
        }
        rows.push(KnowledgeEntityDeleteBatchRow {
            external_id: external_id.clone(),
            node_id: Some(node_id),
            matched: true,
            filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_external_ids.is_empty() {
        return Ok(KnowledgeEntityDeleteBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            non_writable_count,
            deleted_node_count: 0,
        });
    }

    let cypher = format!(
        "MATCH (n:{}) WHERE n.id IN $external_ids DETACH DELETE n",
        request.delete.label
    );
    let parameters = BTreeMap::from([(
        "external_ids".to_string(),
        Value::List(
            eligible_external_ids
                .into_iter()
                .map(Value::String)
                .collect(),
        ),
    )]);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let deleted_node_count = output.rows.len();
    Ok(KnowledgeEntityDeleteBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        non_writable_count,
        deleted_node_count,
    })
}

fn create_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipCreateRequest,
) -> Result<KnowledgeRelationshipCreateOutput> {
    create_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipCreateRequest {
            create: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn create_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipCreateRequest,
) -> Result<KnowledgeRelationshipCreateOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.create.source.label, "source label")?;
    validate_cypher_identifier(&request.create.target.label, "target label")?;
    validate_cypher_identifier(&request.create.relationship_type, "relationship type")?;
    for property in request.create.properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.create.source.label.as_str(),
        request.create.source.external_id.as_str(),
    );
    let target = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.create.target.label.as_str(),
        request.create.target.external_id.as_str(),
    );
    let source_node_id = source.map(|node| node.id.0);
    let target_node_id = target.map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            created_relationship_count: 0,
        });
    };
    if !node_has_external_id_property(source, request.create.source.external_id.as_str())
        || !node_has_external_id_property(target, request.create.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            created_relationship_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out,
            target_filtered_out,
            created_relationship_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_relationship_create_statement(&request.create);
    db.query_with_params(cypher.as_str(), &parameters)?;
    Ok(KnowledgeRelationshipCreateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        matched: true,
        source_filtered_out: false,
        target_filtered_out: false,
        created_relationship_count: 1,
    })
}

fn knowledge_relationship_create_statement(
    request: &KnowledgeRelationshipCreateRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!(
        "MATCH (source:{} {{id: $source_external_id}}), (target:{} {{id: $target_external_id}}) CREATE (source)-[:{}",
        request.source.label, request.target.label, request.relationship_type
    );
    let mut parameters = BTreeMap::from([
        (
            "source_external_id".to_string(),
            Value::String(request.source.external_id.clone()),
        ),
        (
            "target_external_id".to_string(),
            Value::String(request.target.external_id.clone()),
        ),
    ]);
    if !request.properties.is_empty() {
        cypher.push_str(" {");
        for (index, (property, value)) in request.properties.iter().enumerate() {
            if index > 0 {
                cypher.push_str(", ");
            }
            let parameter_name = format!("relationship_value_{index}");
            cypher.push_str(&format!("{property}: ${parameter_name}"));
            parameters.insert(parameter_name, value.clone());
        }
        cypher.push('}');
    }
    cypher.push_str("]->(target)");
    (cypher, parameters)
}

fn create_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipCreateBatchRequest,
) -> Result<KnowledgeRelationshipCreateBatchOutput> {
    create_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipCreateBatchRequest {
            creates: request.creates.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn create_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipCreateBatchRequest,
) -> Result<KnowledgeRelationshipCreateBatchOutput> {
    db.ensure_writable()?;
    for create in &request.creates {
        validate_cypher_identifier(&create.source.label, "source label")?;
        validate_cypher_identifier(&create.target.label, "target label")?;
        validate_cypher_identifier(&create.relationship_type, "relationship type")?;
        for property in create.properties.keys() {
            validate_cypher_identifier(property, "relationship property")?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.creates.len());
    let mut matched_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_creates = Vec::new();

    for create in &request.creates {
        let source = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            create.source.label.as_str(),
            create.source.external_id.as_str(),
        );
        let target = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            create.target.label.as_str(),
            create.target.external_id.as_str(),
        );
        let source_node_id = source.map(|node| node.id.0);
        let target_node_id = target.map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipCreateBatchRow {
                source: create.source.clone(),
                target: create.target.clone(),
                relationship_type: create.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        if !node_has_external_id_property(source, create.source.external_id.as_str())
            || !node_has_external_id_property(target, create.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipCreateBatchRow {
                source: create.source.clone(),
                target: create.target.clone(),
                relationship_type: create.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipCreateBatchRow {
                source: create.source.clone(),
                target: create.target.clone(),
                relationship_type: create.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        eligible_creates.push(create.clone());
        rows.push(KnowledgeRelationshipCreateBatchRow {
            source: create.source.clone(),
            target: create.target.clone(),
            relationship_type: create.relationship_type.clone(),
            source_node_id,
            target_node_id,
            matched: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeRelationshipCreateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            created_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_relationship_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    let created_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipCreateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        created_relationship_count,
    })
}

fn upsert_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpsertRequest,
) -> Result<KnowledgeRelationshipUpsertOutput> {
    upsert_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipUpsertRequest {
            upsert: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn upsert_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpsertRequest,
) -> Result<KnowledgeRelationshipUpsertOutput> {
    db.ensure_writable()?;
    validate_knowledge_relationship_upsert(&request.upsert)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.upsert.source.label.as_str(),
        request.upsert.source.external_id.as_str(),
    );
    let target = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.upsert.target.label.as_str(),
        request.upsert.target.external_id.as_str(),
    );
    let source_node_id = source.map(|node| node.id.0);
    let target_node_id = target.map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: false,
            created: false,
            already_exists: false,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
            created_relationship_count: 0,
        });
    };
    if !node_has_external_id_property(source, request.upsert.source.external_id.as_str())
        || !node_has_external_id_property(target, request.upsert.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: false,
            created: false,
            already_exists: false,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: true,
            created_relationship_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: false,
            created: false,
            already_exists: false,
            source_filtered_out,
            target_filtered_out,
            non_writable: false,
            created_relationship_count: 0,
        });
    }

    let source_id = source.id;
    let target_id = target.id;
    if let Some(relationship_id) = existing_knowledge_relationship_id(
        &db.catalog,
        &db.store,
        source_id,
        target_id,
        request.upsert.relationship_type.as_str(),
    ) {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: Some(relationship_id),
            matched: true,
            created: false,
            already_exists: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
            created_relationship_count: 0,
        });
    }

    let create = knowledge_relationship_upsert_create_request(&request.upsert);
    let (cypher, parameters) = knowledge_relationship_create_statement(&create);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let relationship_id = existing_knowledge_relationship_id(
        &db.catalog,
        &db.store,
        source_id,
        target_id,
        request.upsert.relationship_type.as_str(),
    );
    Ok(KnowledgeRelationshipUpsertOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        relationship_id,
        matched: true,
        created: true,
        already_exists: false,
        source_filtered_out: false,
        target_filtered_out: false,
        non_writable: false,
        created_relationship_count: output.rows.len(),
    })
}

fn upsert_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpsertBatchRequest,
) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
    upsert_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipUpsertBatchRequest {
            upserts: request.upserts.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn upsert_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpsertBatchRequest,
) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
    db.ensure_writable()?;
    for upsert in &request.upserts {
        validate_knowledge_relationship_upsert(upsert)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.upserts.len());
    let mut matched_count = 0;
    let mut created_count = 0;
    let mut already_exists_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_creates = Vec::new();
    let mut pending_relationships = BTreeSet::new();

    for upsert in &request.upserts {
        let source = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            upsert.source.label.as_str(),
            upsert.source.external_id.as_str(),
        );
        let target = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            upsert.target.label.as_str(),
            upsert.target.external_id.as_str(),
        );
        let source_node_id = source.map(|node| node.id.0);
        let target_node_id = target.map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: false,
                created: false,
                already_exists: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        if !node_has_external_id_property(source, upsert.source.external_id.as_str())
            || !node_has_external_id_property(target, upsert.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: false,
                created: false,
                already_exists: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: false,
                created: false,
                already_exists: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        if let Some(relationship_id) = existing_knowledge_relationship_id(
            &db.catalog,
            &db.store,
            source.id,
            target.id,
            upsert.relationship_type.as_str(),
        ) {
            already_exists_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: Some(relationship_id),
                matched: true,
                created: false,
                already_exists: true,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        }

        let identity = (source.id.0, target.id.0, upsert.relationship_type.clone());
        if !pending_relationships.insert(identity) {
            already_exists_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: true,
                created: false,
                already_exists: true,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(knowledge_relationship_upsert_create_request(upsert));
        rows.push(KnowledgeRelationshipUpsertBatchRow {
            source: upsert.source.clone(),
            target: upsert.target.clone(),
            relationship_type: upsert.relationship_type.clone(),
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: true,
            created: true,
            already_exists: false,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeRelationshipUpsertBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            created_count,
            already_exists_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            created_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_relationship_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    for row in &mut rows {
        if row.created {
            if let (Some(source_node_id), Some(target_node_id)) =
                (row.source_node_id, row.target_node_id)
            {
                row.relationship_id = existing_knowledge_relationship_id(
                    &db.catalog,
                    &db.store,
                    NodeId(source_node_id),
                    NodeId(target_node_id),
                    row.relationship_type.as_str(),
                );
            }
        }
    }
    Ok(KnowledgeRelationshipUpsertBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        created_count,
        already_exists_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        created_relationship_count: eligible_creates.len(),
    })
}

fn validate_knowledge_relationship_upsert(
    request: &KnowledgeRelationshipUpsertRequest,
) -> Result<()> {
    validate_cypher_identifier(&request.source.label, "source label")?;
    validate_cypher_identifier(&request.target.label, "target label")?;
    validate_cypher_identifier(&request.relationship_type, "relationship type")?;
    for property in request.create_properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }
    Ok(())
}

fn knowledge_relationship_upsert_create_request(
    request: &KnowledgeRelationshipUpsertRequest,
) -> KnowledgeRelationshipCreateRequest {
    KnowledgeRelationshipCreateRequest {
        source: request.source.clone(),
        target: request.target.clone(),
        relationship_type: request.relationship_type.clone(),
        properties: request.create_properties.clone(),
    }
}

fn existing_knowledge_relationship_id(
    catalog: &Catalog,
    store: &GraphStore,
    source: NodeId,
    target: NodeId,
    relationship_type: &str,
) -> Option<u64> {
    let rel_type_id = catalog.rel_type_id(relationship_type)?;
    store
        .outgoing_relationships(source, rel_type_id)
        .find(|relationship| relationship.target == target)
        .map(|relationship| relationship.id.0)
}

fn delete_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipDeleteRequest,
) -> Result<KnowledgeRelationshipDeleteOutput> {
    delete_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipDeleteRequest {
            delete: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn delete_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipDeleteRequest,
) -> Result<KnowledgeRelationshipDeleteOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.delete.source.label, "source label")?;
    validate_cypher_identifier(&request.delete.target.label, "target label")?;
    validate_cypher_identifier(&request.delete.relationship_type, "relationship type")?;
    for property in request.delete.relationship_properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.delete.source.label.as_str(),
        request.delete.source.external_id.as_str(),
    );
    let target = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.delete.target.label.as_str(),
        request.delete.target.external_id.as_str(),
    );
    let source_node_id = source.map(|node| node.id.0);
    let target_node_id = target.map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            deleted_relationship_count: 0,
        });
    };
    if !node_has_external_id_property(source, request.delete.source.external_id.as_str())
        || !node_has_external_id_property(target, request.delete.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            deleted_relationship_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out,
            target_filtered_out,
            deleted_relationship_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_relationship_delete_statement(&request.delete);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let deleted_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        matched: deleted_relationship_count > 0,
        source_filtered_out: false,
        target_filtered_out: false,
        deleted_relationship_count,
    })
}

fn knowledge_relationship_delete_statement(
    request: &KnowledgeRelationshipDeleteRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!(
        "MATCH (source:{} {{id: $source_external_id}})-[r:{}",
        request.source.label, request.relationship_type
    );
    let mut parameters = BTreeMap::from([
        (
            "source_external_id".to_string(),
            Value::String(request.source.external_id.clone()),
        ),
        (
            "target_external_id".to_string(),
            Value::String(request.target.external_id.clone()),
        ),
    ]);
    if !request.relationship_properties.is_empty() {
        cypher.push_str(" {");
        for (index, (property, value)) in request.relationship_properties.iter().enumerate() {
            if index > 0 {
                cypher.push_str(", ");
            }
            let parameter_name = format!("relationship_value_{index}");
            cypher.push_str(&format!("{property}: ${parameter_name}"));
            parameters.insert(parameter_name, value.clone());
        }
        cypher.push('}');
    }
    cypher.push_str(&format!(
        "]->(target:{} {{id: $target_external_id}}) DELETE r",
        request.target.label
    ));
    (cypher, parameters)
}

fn update_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpdateRequest,
) -> Result<KnowledgeRelationshipUpdateOutput> {
    update_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipUpdateRequest {
            update: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn update_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpdateRequest,
) -> Result<KnowledgeRelationshipUpdateOutput> {
    db.ensure_writable()?;
    validate_knowledge_relationship_update(&request.update)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.update.source.label.as_str(),
        request.update.source.external_id.as_str(),
    );
    let target = seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.update.target.label.as_str(),
        request.update.target.external_id.as_str(),
    );
    let source_node_id = source.map(|node| node.id.0);
    let target_node_id = target.map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    };
    if !node_has_external_id_property(source, request.update.source.external_id.as_str())
        || !node_has_external_id_property(target, request.update.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out,
            target_filtered_out,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_relationship_update_statement(&request.update);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let updated_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipUpdateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        matched: updated_relationship_count > 0,
        source_filtered_out: false,
        target_filtered_out: false,
        updated_relationship_count,
        updated_property_count: updated_relationship_count * request.update.assignments.len(),
    })
}

fn update_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpdateBatchRequest,
) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
    update_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipUpdateBatchRequest {
            updates: request.updates.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn update_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpdateBatchRequest,
) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_knowledge_relationship_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_updates = Vec::new();
    let mut updated_property_count = 0;

    for update in &request.updates {
        let source = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            update.source.label.as_str(),
            update.source.external_id.as_str(),
        );
        let target = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            update.target.label.as_str(),
            update.target.external_id.as_str(),
        );
        let source_node_id = source.map(|node| node.id.0);
        let target_node_id = target.map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipUpdateBatchRow {
                source: update.source.clone(),
                target: update.target.clone(),
                relationship_type: update.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        if !node_has_external_id_property(source, update.source.external_id.as_str())
            || !node_has_external_id_property(target, update.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipUpdateBatchRow {
                source: update.source.clone(),
                target: update.target.clone(),
                relationship_type: update.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipUpdateBatchRow {
                source: update.source.clone(),
                target: update.target.clone(),
                relationship_type: update.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let row_updated_property_count = update.assignments.len();
        matched_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push(update.clone());
        rows.push(KnowledgeRelationshipUpdateBatchRow {
            source: update.source.clone(),
            target: update.target.clone(),
            relationship_type: update.relationship_type.clone(),
            source_node_id,
            target_node_id,
            matched: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeRelationshipUpdateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for update in &eligible_updates {
        let (cypher, parameters) = knowledge_relationship_update_statement(update);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    let updated_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipUpdateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        updated_relationship_count,
        updated_property_count,
    })
}

fn validate_knowledge_relationship_update(
    request: &KnowledgeRelationshipUpdateRequest,
) -> Result<()> {
    if request.assignments.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge relationship update requires at least one assignment".to_string(),
        ));
    }
    validate_cypher_identifier(&request.source.label, "source label")?;
    validate_cypher_identifier(&request.target.label, "target label")?;
    validate_cypher_identifier(&request.relationship_type, "relationship type")?;
    for property in request.relationship_properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }
    for property in request.assignments.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }
    Ok(())
}

fn knowledge_relationship_update_statement(
    request: &KnowledgeRelationshipUpdateRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!(
        "MATCH (source:{} {{id: $source_external_id}})-[r:{}",
        request.source.label, request.relationship_type
    );
    let mut parameters = BTreeMap::from([
        (
            "source_external_id".to_string(),
            Value::String(request.source.external_id.clone()),
        ),
        (
            "target_external_id".to_string(),
            Value::String(request.target.external_id.clone()),
        ),
    ]);
    if !request.relationship_properties.is_empty() {
        cypher.push_str(" {");
        for (index, (property, value)) in request.relationship_properties.iter().enumerate() {
            if index > 0 {
                cypher.push_str(", ");
            }
            let parameter_name = format!("relationship_filter_value_{index}");
            cypher.push_str(&format!("{property}: ${parameter_name}"));
            parameters.insert(parameter_name, value.clone());
        }
        cypher.push('}');
    }
    cypher.push_str(&format!(
        "]->(target:{} {{id: $target_external_id}}) SET ",
        request.target.label
    ));
    for (index, (property, value)) in request.assignments.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        let parameter_name = format!("relationship_update_value_{index}");
        cypher.push_str(&format!("r.{property} = ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    (cypher, parameters)
}

fn delete_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipDeleteBatchRequest,
) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
    delete_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipDeleteBatchRequest {
            deletes: request.deletes.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn delete_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipDeleteBatchRequest,
) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
    db.ensure_writable()?;
    for delete in &request.deletes {
        validate_cypher_identifier(&delete.source.label, "source label")?;
        validate_cypher_identifier(&delete.target.label, "target label")?;
        validate_cypher_identifier(&delete.relationship_type, "relationship type")?;
        for property in delete.relationship_properties.keys() {
            validate_cypher_identifier(property, "relationship property")?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.deletes.len());
    let mut matched_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_deletes = Vec::new();

    for delete in &request.deletes {
        let source = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            delete.source.label.as_str(),
            delete.source.external_id.as_str(),
        );
        let target = seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            delete.target.label.as_str(),
            delete.target.external_id.as_str(),
        );
        let source_node_id = source.map(|node| node.id.0);
        let target_node_id = target.map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipDeleteBatchRow {
                source: delete.source.clone(),
                target: delete.target.clone(),
                relationship_type: delete.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        if !node_has_external_id_property(source, delete.source.external_id.as_str())
            || !node_has_external_id_property(target, delete.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipDeleteBatchRow {
                source: delete.source.clone(),
                target: delete.target.clone(),
                relationship_type: delete.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipDeleteBatchRow {
                source: delete.source.clone(),
                target: delete.target.clone(),
                relationship_type: delete.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        eligible_deletes.push(delete.clone());
        rows.push(KnowledgeRelationshipDeleteBatchRow {
            source: delete.source.clone(),
            target: delete.target.clone(),
            relationship_type: delete.relationship_type.clone(),
            source_node_id,
            target_node_id,
            matched: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_deletes.is_empty() {
        return Ok(KnowledgeRelationshipDeleteBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            deleted_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for delete in &eligible_deletes {
        let (cypher, parameters) = knowledge_relationship_delete_statement(delete);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    let deleted_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipDeleteBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        deleted_relationship_count,
    })
}

struct KnowledgeSourceReferenceRelationshipDeleteCandidate {
    relationship_id: u64,
    source_node_id: u64,
    target_node_id: u64,
    source_external_id: Option<String>,
    target_external_id: Option<String>,
}

fn delete_knowledge_source_reference_relationships_for(
    db: &mut Database,
    request: &KnowledgeSourceReferenceRelationshipCleanupRequest,
) -> Result<KnowledgeSourceReferenceRelationshipCleanupOutput> {
    db.ensure_writable()?;
    if request.source_reference.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source-reference relationship cleanup requires a non-empty source_reference"
                .to_string(),
        ));
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(rel_type_id) = db.catalog.rel_type_id("RELATES_TO") else {
        return Ok(KnowledgeSourceReferenceRelationshipCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            deleted_relationship_count: 0,
        });
    };

    let mut candidates = db
        .store
        .scan_relationships(Some(rel_type_id))
        .filter(|relationship| {
            relationship
                .properties
                .get("source_reference")
                .is_some_and(|value| value_to_external_id(value) == request.source_reference)
        })
        .map(
            |relationship| KnowledgeSourceReferenceRelationshipDeleteCandidate {
                relationship_id: relationship.id.0,
                source_node_id: relationship.source.0,
                target_node_id: relationship.target.0,
                source_external_id: db
                    .store
                    .node(relationship.source)
                    .and_then(node_external_id),
                target_external_id: db
                    .store
                    .node(relationship.target)
                    .and_then(node_external_id),
            },
        )
        .collect::<Vec<_>>();
    candidates.sort_by_key(|candidate| candidate.relationship_id);

    if candidates.is_empty() {
        return Ok(KnowledgeSourceReferenceRelationshipCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            deleted_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for candidate in &candidates {
        let (cypher, parameters) =
            knowledge_source_reference_relationship_delete_statement(candidate.relationship_id)?;
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    let deleted_relationship_count = candidates.len();
    let rows = candidates
        .into_iter()
        .map(|candidate| KnowledgeSourceReferenceRelationshipCleanupRow {
            relationship_id: candidate.relationship_id,
            source_node_id: candidate.source_node_id,
            target_node_id: candidate.target_node_id,
            source_external_id: candidate.source_external_id,
            target_external_id: candidate.target_external_id,
            deleted: true,
        })
        .collect::<Vec<_>>();

    Ok(KnowledgeSourceReferenceRelationshipCleanupOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        candidate_count: rows.len(),
        rows,
        deleted_relationship_count,
    })
}

fn knowledge_source_reference_relationship_delete_statement(
    relationship_id: u64,
) -> Result<(String, BTreeMap<String, Value>)> {
    let relationship_id = i64::try_from(relationship_id).map_err(|_| {
        SkeinError::Semantic("relationship id does not fit Cypher integer".to_string())
    })?;
    Ok((
        "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE id(r) = $relationship_id DELETE r"
            .to_string(),
        BTreeMap::from([("relationship_id".to_string(), Value::Int(relationship_id))]),
    ))
}

fn node_has_external_id_property(node: &NodeRecord, external_id: &str) -> bool {
    node.properties
        .get("id")
        .is_some_and(|value| value_to_external_id(value) == external_id)
}

fn validate_cypher_identifier(value: &str, kind: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(SkeinError::Semantic(format!("{kind} identifier is empty")));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(SkeinError::Semantic(format!(
            "{kind} identifier {value:?} must start with an ASCII letter or underscore"
        )));
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(SkeinError::Semantic(format!(
            "{kind} identifier {value:?} must contain only ASCII letters, digits, or underscores"
        )));
    }
    Ok(())
}

fn knowledge_neighbors_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeNeighborsRequest,
) -> KnowledgeNeighborsOutput {
    knowledge_scoped_neighbors_for(
        catalog,
        store,
        &KnowledgeScopedNeighborsRequest {
            navigation: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn knowledge_scoped_neighbors_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeScopedNeighborsRequest,
) -> KnowledgeNeighborsOutput {
    let navigation = &request.navigation;
    let Some(seed) = seed_node_by_label_and_external_id(
        catalog,
        store,
        navigation.label.as_str(),
        navigation.external_id.as_str(),
    ) else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch: store.commit_epoch(),
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: Some(knowledge_identity_description(
                navigation.label.as_str(),
                navigation.external_id.as_str(),
            )),
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
        return KnowledgeNeighborsOutput {
            graph_commit_epoch: store.commit_epoch(),
            seed_node_id: None,
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        };
    };
    if !request.metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(catalog, seed, &request.metadata_filters)
    {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch: store.commit_epoch(),
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 1);
        return KnowledgeNeighborsOutput {
            graph_commit_epoch: store.commit_epoch(),
            seed_node_id: Some(seed.id.0),
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        };
    }

    let relationship_type = match navigation.relationship_type.as_deref() {
        Some(name) => match catalog.rel_type_id(name) {
            Some(rel_type_id) => Some(rel_type_id),
            None => {
                let mut diagnostics =
                    knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                        graph_commit_epoch: store.commit_epoch(),
                        seed_found: true,
                        target_found: None,
                        path_count: 0,
                        node_count: 0,
                        relationship_count: 0,
                        fanout_reason_details: Vec::new(),
                        missing_seed_identity: None,
                        missing_target_identity: None,
                        missing_relationship_type: Some(name.to_string()),
                        max_hops: navigation.max_hops,
                        path_limit: Some(navigation.limit),
                        node_limit: None,
                        relationship_limit: None,
                    });
                attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
                return KnowledgeNeighborsOutput {
                    graph_commit_epoch: store.commit_epoch(),
                    seed_node_id: Some(seed.id.0),
                    paths: Vec::new(),
                    fanout_reason_codes: Vec::new(),
                    fanout_reason_details: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics,
                };
            }
        },
        None => None,
    };
    let (paths, fanout_reason_details) = expand_knowledge_neighbors_for(
        catalog,
        store,
        KnowledgeNeighborExpansion {
            seed_hit_id: "seed",
            seed_node_id: seed.id,
            requested_direction: navigation.direction,
            relationship_type,
            limit: navigation.limit,
            max_hops: navigation.max_hops,
        },
    );
    let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
        graph_commit_epoch: store.commit_epoch(),
        seed_found: true,
        target_found: None,
        path_count: paths.len(),
        node_count: knowledge_context_path_node_count(&paths),
        relationship_count: paths.len(),
        fanout_reason_details: fanout_reason_details.clone(),
        missing_seed_identity: None,
        missing_target_identity: None,
        missing_relationship_type: None,
        max_hops: navigation.max_hops,
        path_limit: Some(navigation.limit),
        node_limit: None,
        relationship_limit: None,
    });
    attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
    KnowledgeNeighborsOutput {
        graph_commit_epoch: store.commit_epoch(),
        seed_node_id: Some(seed.id.0),
        diagnostics,
        paths,
        fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
        fanout_reason_details,
    }
}

fn knowledge_relationships_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeRelationshipsRequest,
) -> KnowledgeRelationshipsOutput {
    knowledge_scoped_relationships_for(
        catalog,
        store,
        &KnowledgeScopedRelationshipsRequest {
            relationships: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn knowledge_scoped_relationships_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeScopedRelationshipsRequest,
) -> KnowledgeRelationshipsOutput {
    let relationship_type = match request.relationships.relationship_type.as_deref() {
        Some(name) => match catalog.rel_type_id(name) {
            Some(rel_type_id) => Some(rel_type_id),
            None => {
                return knowledge_empty_relationship_groups_for_missing_type(store, request);
            }
        },
        None => None,
    };
    let mut groups = Vec::with_capacity(request.relationships.seeds.len());
    let mut found_seed_count = 0;
    let mut missing_seed_count = 0;
    let mut filtered_out_seed_count = 0;
    let mut relationship_count = 0;
    for (index, seed_request) in request.relationships.seeds.iter().enumerate() {
        let seed = seed_node_by_label_and_external_id(
            catalog,
            store,
            seed_request.label.as_str(),
            seed_request.external_id.as_str(),
        );
        let Some(seed) = seed else {
            missing_seed_count += 1;
            groups.push(KnowledgeRelationshipGroup {
                seed: seed_request.clone(),
                seed_node_id: None,
                filtered_out: false,
                relationships: Vec::new(),
                fanout_reason_codes: Vec::new(),
                fanout_reason_details: Vec::new(),
                fanout_reasons: Vec::new(),
            });
            continue;
        };
        if !request.metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(catalog, seed, &request.metadata_filters)
        {
            filtered_out_seed_count += 1;
            groups.push(KnowledgeRelationshipGroup {
                seed: seed_request.clone(),
                seed_node_id: Some(seed.id.0),
                filtered_out: true,
                relationships: Vec::new(),
                fanout_reason_codes: Vec::new(),
                fanout_reason_details: Vec::new(),
                fanout_reasons: Vec::new(),
            });
            continue;
        }
        found_seed_count += 1;
        let (relationships, fanout_reason_details) = expand_knowledge_neighbors_for(
            catalog,
            store,
            KnowledgeNeighborExpansion {
                seed_hit_id: &format!("seed_{index}"),
                seed_node_id: seed.id,
                requested_direction: request.relationships.direction,
                relationship_type,
                limit: request.relationships.limit_per_seed,
                max_hops: 1,
            },
        );
        relationship_count += relationships.len();
        groups.push(KnowledgeRelationshipGroup {
            seed: seed_request.clone(),
            seed_node_id: Some(seed.id.0),
            filtered_out: false,
            relationships,
            fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
            fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
            fanout_reason_details,
        });
    }
    KnowledgeRelationshipsOutput {
        graph_commit_epoch: store.commit_epoch(),
        groups,
        relationship_type_found: true,
        found_seed_count,
        missing_seed_count,
        filtered_out_seed_count,
        relationship_count,
    }
}

fn knowledge_empty_relationship_groups_for_missing_type(
    store: &GraphStore,
    request: &KnowledgeScopedRelationshipsRequest,
) -> KnowledgeRelationshipsOutput {
    KnowledgeRelationshipsOutput {
        graph_commit_epoch: store.commit_epoch(),
        groups: request
            .relationships
            .seeds
            .iter()
            .cloned()
            .map(|seed| KnowledgeRelationshipGroup {
                seed,
                seed_node_id: None,
                filtered_out: false,
                relationships: Vec::new(),
                fanout_reason_codes: Vec::new(),
                fanout_reason_details: Vec::new(),
                fanout_reasons: Vec::new(),
            })
            .collect(),
        relationship_type_found: false,
        found_seed_count: 0,
        missing_seed_count: 0,
        filtered_out_seed_count: 0,
        relationship_count: 0,
    }
}

fn knowledge_paths_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgePathRequest,
) -> KnowledgePathOutput {
    knowledge_scoped_paths_for(
        catalog,
        store,
        &KnowledgeScopedPathRequest {
            navigation: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

fn knowledge_scoped_paths_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeScopedPathRequest,
) -> KnowledgePathOutput {
    let navigation = &request.navigation;
    let source = seed_node_by_label_and_external_id(
        catalog,
        store,
        navigation.source_label.as_str(),
        navigation.source_external_id.as_str(),
    );
    let target = seed_node_by_label_and_external_id(
        catalog,
        store,
        navigation.target_label.as_str(),
        navigation.target_external_id.as_str(),
    );
    let source_node_id = source.map(|node| node.id.0);
    let target_node_id = target.map(|node| node.id.0);

    let Some(source) = source else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch: store.commit_epoch(),
            seed_found: false,
            target_found: Some(target_node_id.is_some()),
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: Some(knowledge_identity_description(
                navigation.source_label.as_str(),
                navigation.source_external_id.as_str(),
            )),
            missing_target_identity: target_node_id.is_none().then(|| {
                knowledge_identity_description(
                    navigation.target_label.as_str(),
                    navigation.target_external_id.as_str(),
                )
            }),
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_path_endpoint_metadata_filters(
            &mut diagnostics,
            &request.source_metadata_filters,
            &request.target_metadata_filters,
            0,
        );
        return KnowledgePathOutput {
            graph_commit_epoch: store.commit_epoch(),
            source_node_id,
            target_node_id,
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        };
    };
    let Some(target) = target else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch: store.commit_epoch(),
            seed_found: true,
            target_found: Some(false),
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: Some(knowledge_identity_description(
                navigation.target_label.as_str(),
                navigation.target_external_id.as_str(),
            )),
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_path_endpoint_metadata_filters(
            &mut diagnostics,
            &request.source_metadata_filters,
            &request.target_metadata_filters,
            0,
        );
        return KnowledgePathOutput {
            graph_commit_epoch: store.commit_epoch(),
            source_node_id,
            target_node_id,
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        };
    };
    let source_filtered = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(catalog, source, &request.source_metadata_filters);
    let target_filtered = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(catalog, target, &request.target_metadata_filters);
    if source_filtered || target_filtered {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch: store.commit_epoch(),
            seed_found: !source_filtered,
            target_found: Some(!target_filtered),
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_path_endpoint_metadata_filters(
            &mut diagnostics,
            &request.source_metadata_filters,
            &request.target_metadata_filters,
            usize::from(source_filtered) + usize::from(target_filtered),
        );
        return KnowledgePathOutput {
            graph_commit_epoch: store.commit_epoch(),
            source_node_id,
            target_node_id,
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        };
    }
    let relationship_type = match navigation.relationship_type.as_deref() {
        Some(name) => match catalog.rel_type_id(name) {
            Some(rel_type_id) => Some(rel_type_id),
            None => {
                let mut diagnostics =
                    knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                        graph_commit_epoch: store.commit_epoch(),
                        seed_found: true,
                        target_found: Some(true),
                        path_count: 0,
                        node_count: 0,
                        relationship_count: 0,
                        fanout_reason_details: Vec::new(),
                        missing_seed_identity: None,
                        missing_target_identity: None,
                        missing_relationship_type: Some(name.to_string()),
                        max_hops: navigation.max_hops,
                        path_limit: Some(navigation.limit),
                        node_limit: None,
                        relationship_limit: None,
                    });
                attach_path_endpoint_metadata_filters(
                    &mut diagnostics,
                    &request.source_metadata_filters,
                    &request.target_metadata_filters,
                    0,
                );
                return KnowledgePathOutput {
                    graph_commit_epoch: store.commit_epoch(),
                    source_node_id,
                    target_node_id,
                    paths: Vec::new(),
                    fanout_reason_codes: Vec::new(),
                    fanout_reason_details: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics,
                };
            }
        },
        None => None,
    };

    let (paths, fanout_reason_details) = expand_knowledge_paths_for(
        catalog,
        store,
        KnowledgePathExpansion {
            source_node_id: source.id,
            target_node_id: target.id,
            requested_direction: navigation.direction,
            relationship_type,
            max_hops: navigation.max_hops,
            limit: navigation.limit,
        },
    );
    let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
        graph_commit_epoch: store.commit_epoch(),
        seed_found: true,
        target_found: Some(true),
        path_count: paths.len(),
        node_count: knowledge_graph_path_node_count(&paths),
        relationship_count: paths.iter().map(|path| path.segments.len()).sum::<usize>(),
        fanout_reason_details: fanout_reason_details.clone(),
        missing_seed_identity: None,
        missing_target_identity: None,
        missing_relationship_type: None,
        max_hops: navigation.max_hops,
        path_limit: Some(navigation.limit),
        node_limit: None,
        relationship_limit: None,
    });
    attach_path_endpoint_metadata_filters(
        &mut diagnostics,
        &request.source_metadata_filters,
        &request.target_metadata_filters,
        0,
    );
    KnowledgePathOutput {
        graph_commit_epoch: store.commit_epoch(),
        source_node_id,
        target_node_id,
        diagnostics,
        paths,
        fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
        fanout_reason_details,
    }
}

fn knowledge_subgraph_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSubgraphRequest,
) -> KnowledgeSubgraphOutput {
    knowledge_scoped_subgraph_for(
        catalog,
        store,
        &KnowledgeScopedSubgraphRequest {
            navigation: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

fn knowledge_scoped_subgraph_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeScopedSubgraphRequest,
) -> KnowledgeSubgraphOutput {
    let navigation = &request.navigation;
    let Some(seed) = seed_node_by_label_and_external_id(
        catalog,
        store,
        navigation.label.as_str(),
        navigation.external_id.as_str(),
    ) else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch: store.commit_epoch(),
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: Some(knowledge_identity_description(
                navigation.label.as_str(),
                navigation.external_id.as_str(),
            )),
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: None,
            node_limit: Some(navigation.node_limit),
            relationship_limit: Some(navigation.relationship_limit),
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
        return KnowledgeSubgraphOutput {
            graph_commit_epoch: store.commit_epoch(),
            seed_node_id: None,
            nodes: Vec::new(),
            relationships: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        };
    };
    if !request.metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(catalog, seed, &request.metadata_filters)
    {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch: store.commit_epoch(),
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: None,
            node_limit: Some(navigation.node_limit),
            relationship_limit: Some(navigation.relationship_limit),
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 1);
        return KnowledgeSubgraphOutput {
            graph_commit_epoch: store.commit_epoch(),
            seed_node_id: Some(seed.id.0),
            nodes: Vec::new(),
            relationships: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        };
    }
    let relationship_type = match navigation.relationship_type.as_deref() {
        Some(name) => match catalog.rel_type_id(name) {
            Some(rel_type_id) => Some(rel_type_id),
            None => {
                let mut diagnostics =
                    knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                        graph_commit_epoch: store.commit_epoch(),
                        seed_found: true,
                        target_found: None,
                        path_count: 0,
                        node_count: 0,
                        relationship_count: 0,
                        fanout_reason_details: Vec::new(),
                        missing_seed_identity: None,
                        missing_target_identity: None,
                        missing_relationship_type: Some(name.to_string()),
                        max_hops: navigation.max_hops,
                        path_limit: None,
                        node_limit: Some(navigation.node_limit),
                        relationship_limit: Some(navigation.relationship_limit),
                    });
                attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
                return KnowledgeSubgraphOutput {
                    graph_commit_epoch: store.commit_epoch(),
                    seed_node_id: Some(seed.id.0),
                    nodes: Vec::new(),
                    relationships: Vec::new(),
                    fanout_reason_codes: Vec::new(),
                    fanout_reason_details: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics,
                };
            }
        },
        None => None,
    };
    let (nodes, relationships, fanout_reason_details) = expand_knowledge_subgraph_for(
        catalog,
        store,
        KnowledgeSubgraphExpansion {
            seed_node_id: seed.id,
            requested_direction: navigation.direction,
            relationship_type,
            max_hops: navigation.max_hops,
            node_limit: navigation.node_limit,
            relationship_limit: navigation.relationship_limit,
        },
    );
    let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
        graph_commit_epoch: store.commit_epoch(),
        seed_found: true,
        target_found: None,
        path_count: relationships.len(),
        node_count: nodes.len(),
        relationship_count: relationships.len(),
        fanout_reason_details: fanout_reason_details.clone(),
        missing_seed_identity: None,
        missing_target_identity: None,
        missing_relationship_type: None,
        max_hops: navigation.max_hops,
        path_limit: None,
        node_limit: Some(navigation.node_limit),
        relationship_limit: Some(navigation.relationship_limit),
    });
    attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
    KnowledgeSubgraphOutput {
        graph_commit_epoch: store.commit_epoch(),
        seed_node_id: Some(seed.id.0),
        diagnostics,
        nodes,
        relationships,
        fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
        fanout_reason_details,
    }
}

struct KnowledgeTraversalDiagnosticInput {
    graph_commit_epoch: u64,
    seed_found: bool,
    target_found: Option<bool>,
    path_count: usize,
    node_count: usize,
    relationship_count: usize,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    missing_seed_identity: Option<String>,
    missing_target_identity: Option<String>,
    missing_relationship_type: Option<String>,
    max_hops: usize,
    path_limit: Option<usize>,
    node_limit: Option<usize>,
    relationship_limit: Option<usize>,
}

fn knowledge_traversal_diagnostics(
    input: KnowledgeTraversalDiagnosticInput,
) -> KnowledgeTraversalDiagnostics {
    let fallback_reason_codes = knowledge_traversal_fallback_reason_codes(&input);
    let fallback_reasons = knowledge_traversal_fallback_reasons(&input);
    let input_candidate_set = knowledge_traversal_input_candidate_set_report(&input);
    let candidate_set = knowledge_traversal_candidate_set_report(&input);
    KnowledgeTraversalDiagnostics {
        seed_found: input.seed_found,
        target_found: input.target_found,
        input_candidate_set,
        candidate_set,
        path_count: input.path_count,
        node_count: input.node_count,
        relationship_count: input.relationship_count,
        fanout_reason_count: input.fanout_reason_details.len(),
        fanout_reason_codes: knowledge_fanout_reason_codes(&input.fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&input.fanout_reason_details),
        fanout_reason_details: input.fanout_reason_details,
        fallback_reason_codes,
        fallback_reasons,
        max_hops: input.max_hops,
        path_limit: input.path_limit,
        node_limit: input.node_limit,
        relationship_limit: input.relationship_limit,
    }
}

fn attach_traversal_metadata_filters(
    diagnostics: &mut KnowledgeTraversalDiagnostics,
    metadata_filters: &BTreeMap<String, String>,
    filtered_out_count: usize,
) {
    if metadata_filters.is_empty() && filtered_out_count == 0 {
        return;
    }
    diagnostics.input_candidate_set.metadata_filters = metadata_filters.clone();
    diagnostics.input_candidate_set.filtered_out_count = filtered_out_count;
}

fn attach_path_endpoint_metadata_filters(
    diagnostics: &mut KnowledgeTraversalDiagnostics,
    source_metadata_filters: &BTreeMap<String, String>,
    target_metadata_filters: &BTreeMap<String, String>,
    filtered_out_count: usize,
) {
    if source_metadata_filters.is_empty()
        && target_metadata_filters.is_empty()
        && filtered_out_count == 0
    {
        return;
    }
    diagnostics.input_candidate_set.metadata_filters =
        prefixed_path_endpoint_metadata_filters(source_metadata_filters, target_metadata_filters);
    diagnostics.input_candidate_set.filtered_out_count = filtered_out_count;
}

fn prefixed_path_endpoint_metadata_filters(
    source_metadata_filters: &BTreeMap<String, String>,
    target_metadata_filters: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    source_metadata_filters
        .iter()
        .map(|(key, value)| (format!("source.{key}"), value.clone()))
        .chain(
            target_metadata_filters
                .iter()
                .map(|(key, value)| (format!("target.{key}"), value.clone())),
        )
        .collect()
}

fn knowledge_traversal_input_candidate_set_report(
    input: &KnowledgeTraversalDiagnosticInput,
) -> SearchCandidateSetReport {
    let cardinality = match input.target_found {
        Some(target_found) => usize::from(input.seed_found) + usize::from(target_found),
        None => usize::from(input.seed_found),
    };
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "traversal_seed_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(input.graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count: 0,
        metadata_filters: BTreeMap::new(),
    }
}

fn knowledge_traversal_candidate_set_report(
    input: &KnowledgeTraversalDiagnosticInput,
) -> SearchRetrieverCandidateSetReport {
    let (id_space, representation, cardinality) =
        if input.node_limit.is_some() || input.relationship_limit.is_some() {
            (
                "mixed_graph_id",
                "subgraph_node_and_relationship_ids",
                input.node_count + input.relationship_count,
            )
        } else if input.target_found.is_some() {
            ("canonical_graph_path", "bounded_paths", input.path_count)
        } else {
            (
                "canonical_graph_relationship_id",
                "neighbor_relationship_ids",
                input.relationship_count,
            )
        };
    SearchRetrieverCandidateSetReport {
        id_space: id_space.to_string(),
        representation: representation.to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(input.graph_commit_epoch),
        policy_epoch: None,
    }
}

fn knowledge_traversal_fallback_reason_codes(
    input: &KnowledgeTraversalDiagnosticInput,
) -> Vec<KnowledgeTraversalFallbackReasonCode> {
    let mut codes = Vec::new();
    if input.missing_seed_identity.is_some() {
        codes.push(KnowledgeTraversalFallbackReasonCode::SeedNotFound);
    }
    if input.missing_target_identity.is_some() {
        codes.push(KnowledgeTraversalFallbackReasonCode::TargetNotFound);
    }
    if input.max_hops == 0 {
        codes.push(KnowledgeTraversalFallbackReasonCode::MaxHopsZero);
    }
    if input.path_limit == Some(0) {
        codes.push(KnowledgeTraversalFallbackReasonCode::PathLimitZero);
    }
    if input.node_limit == Some(0) {
        codes.push(KnowledgeTraversalFallbackReasonCode::NodeLimitZero);
    }
    if input.relationship_limit == Some(0) {
        codes.push(KnowledgeTraversalFallbackReasonCode::RelationshipLimitZero);
    }
    if input.missing_relationship_type.is_some() {
        codes.push(KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound);
    }
    codes
}

fn knowledge_traversal_fallback_reasons(input: &KnowledgeTraversalDiagnosticInput) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some(seed_identity) = &input.missing_seed_identity {
        reasons.push(format!("seed {seed_identity} not found"));
    }
    if let Some(target_identity) = &input.missing_target_identity {
        reasons.push(format!("target {target_identity} not found"));
    }
    if input.max_hops == 0 {
        reasons.push("traversal disabled by max_hops 0".to_string());
    }
    if input.path_limit == Some(0) {
        reasons.push("path traversal disabled by limit 0".to_string());
    }
    if input.node_limit == Some(0) {
        reasons.push("subgraph traversal disabled by node_limit 0".to_string());
    }
    if input.relationship_limit == Some(0) {
        reasons.push("subgraph traversal disabled by relationship_limit 0".to_string());
    }
    if let Some(relationship_type) = &input.missing_relationship_type {
        reasons.push(format!("relationship type {relationship_type} not found"));
    }
    reasons
}

fn knowledge_identity_description(label: &str, external_id: &str) -> String {
    format!("{label}:{external_id}")
}

fn knowledge_context_path_node_count(paths: &[KnowledgeGraphContextPath]) -> usize {
    paths
        .iter()
        .flat_map(|path| [path.source_node_id, path.target_node_id])
        .collect::<BTreeSet<_>>()
        .len()
}

fn knowledge_graph_path_node_count(paths: &[KnowledgeGraphPath]) -> usize {
    paths
        .iter()
        .flat_map(|path| path.segments.iter())
        .flat_map(|segment| [segment.source_node_id, segment.target_node_id])
        .collect::<BTreeSet<_>>()
        .len()
}

struct KnowledgeNeighborExpansion<'a> {
    seed_hit_id: &'a str,
    seed_node_id: NodeId,
    requested_direction: KnowledgeNeighborDirection,
    relationship_type: Option<crate::schema::RelTypeId>,
    limit: usize,
    max_hops: usize,
}

struct KnowledgePathExpansion {
    source_node_id: NodeId,
    target_node_id: NodeId,
    requested_direction: KnowledgeNeighborDirection,
    relationship_type: Option<crate::schema::RelTypeId>,
    max_hops: usize,
    limit: usize,
}

struct KnowledgeSubgraphExpansion {
    seed_node_id: NodeId,
    requested_direction: KnowledgeNeighborDirection,
    relationship_type: Option<crate::schema::RelTypeId>,
    max_hops: usize,
    node_limit: usize,
    relationship_limit: usize,
}

struct KnowledgeExpansionEdge<'a> {
    direction: KnowledgeGraphPathDirection,
    next_node: NodeId,
    relationship: &'a RelRecord,
}

struct DenseAdjacencyDiagnosticContext<'a> {
    catalog: &'a Catalog,
    store: &'a GraphStore,
    operation: &'a str,
    relationship_type: Option<crate::schema::RelTypeId>,
    requested_direction: KnowledgeNeighborDirection,
}

fn expand_knowledge_neighbors_for(
    catalog: &Catalog,
    store: &GraphStore,
    expansion: KnowledgeNeighborExpansion<'_>,
) -> (
    Vec<KnowledgeGraphContextPath>,
    Vec<KnowledgeFanoutReasonDetail>,
) {
    let mut paths = Vec::new();
    let mut fanout_reasons = Vec::new();
    let mut seen_relationships = BTreeSet::new();
    let mut seen_frontier_nodes = BTreeSet::new();
    let mut reported_dense_groups = BTreeSet::new();
    let mut frontier = VecDeque::from([(expansion.seed_node_id, 0usize)]);
    seen_frontier_nodes.insert(expansion.seed_node_id.0);

    while let Some((current_node, depth)) = frontier.pop_front() {
        if depth >= expansion.max_hops {
            continue;
        }
        record_dense_adjacency_diagnostics(
            DenseAdjacencyDiagnosticContext {
                catalog,
                store,
                operation: "knowledge_neighbors",
                relationship_type: expansion.relationship_type,
                requested_direction: expansion.requested_direction,
            },
            current_node,
            &mut reported_dense_groups,
            &mut fanout_reasons,
        );
        for edge in knowledge_expansion_edges_for_node(
            store,
            current_node,
            expansion.relationship_type,
            expansion.requested_direction,
        ) {
            let relationship = edge.relationship;
            if !seen_relationships.insert(relationship.id.0) {
                continue;
            }
            if paths.len() >= expansion.limit {
                fanout_reasons.push(KnowledgeFanoutReasonDetail::path_limit(
                    "knowledge_neighbors",
                    expansion.limit,
                    expansion.seed_hit_id,
                ));
                return (paths, fanout_reasons);
            }
            let Some(path) = context_path_for_relationship(
                catalog,
                store,
                expansion.seed_hit_id,
                depth + 1,
                edge.direction,
                relationship,
            ) else {
                continue;
            };
            paths.push(path);
            if seen_frontier_nodes.insert(edge.next_node.0) {
                frontier.push_back((edge.next_node, depth + 1));
            }
        }
    }

    (paths, fanout_reasons)
}

fn expand_knowledge_paths_for(
    catalog: &Catalog,
    store: &GraphStore,
    expansion: KnowledgePathExpansion,
) -> (Vec<KnowledgeGraphPath>, Vec<KnowledgeFanoutReasonDetail>) {
    let mut paths = Vec::new();
    let mut fanout_reasons = Vec::new();
    let mut reported_dense_groups = BTreeSet::new();
    let mut frontier = VecDeque::from([(
        expansion.source_node_id,
        Vec::<KnowledgeGraphContextPath>::new(),
        BTreeSet::from([expansion.source_node_id.0]),
    )]);

    while let Some((current_node, current_path, visited_nodes)) = frontier.pop_front() {
        if current_path.len() >= expansion.max_hops {
            continue;
        }
        record_dense_adjacency_diagnostics(
            DenseAdjacencyDiagnosticContext {
                catalog,
                store,
                operation: "knowledge_paths",
                relationship_type: expansion.relationship_type,
                requested_direction: expansion.requested_direction,
            },
            current_node,
            &mut reported_dense_groups,
            &mut fanout_reasons,
        );
        for edge in knowledge_expansion_edges_for_node(
            store,
            current_node,
            expansion.relationship_type,
            expansion.requested_direction,
        ) {
            if visited_nodes.contains(&edge.next_node.0)
                && edge.next_node != expansion.target_node_id
            {
                continue;
            }
            let Some(segment) = context_path_for_relationship(
                catalog,
                store,
                "path",
                current_path.len() + 1,
                edge.direction,
                edge.relationship,
            ) else {
                continue;
            };
            let mut next_path = current_path.clone();
            next_path.push(segment);
            if edge.next_node == expansion.target_node_id {
                if paths.len() >= expansion.limit {
                    fanout_reasons.push(KnowledgeFanoutReasonDetail::path_limit(
                        "knowledge_paths",
                        expansion.limit,
                        "path",
                    ));
                    return (paths, fanout_reasons);
                }
                paths.push(KnowledgeGraphPath {
                    segments: next_path,
                });
                continue;
            }
            let mut next_visited = visited_nodes.clone();
            next_visited.insert(edge.next_node.0);
            frontier.push_back((edge.next_node, next_path, next_visited));
        }
    }

    (paths, fanout_reasons)
}

fn expand_knowledge_subgraph_for(
    catalog: &Catalog,
    store: &GraphStore,
    expansion: KnowledgeSubgraphExpansion,
) -> (
    Vec<KnowledgeEntity>,
    Vec<KnowledgeGraphContextPath>,
    Vec<KnowledgeFanoutReasonDetail>,
) {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    let mut fanout_reasons = Vec::new();
    let mut seen_nodes = BTreeSet::new();
    let mut seen_relationships = BTreeSet::new();
    let mut reported_dense_groups = BTreeSet::new();
    let mut frontier = VecDeque::from([(expansion.seed_node_id, 0usize)]);

    if expansion.node_limit == 0 {
        fanout_reasons.push(KnowledgeFanoutReasonDetail::node_limit(0));
        return (nodes, relationships, fanout_reasons);
    }
    if let Some(seed) = store.node(expansion.seed_node_id) {
        nodes.push(knowledge_entity_from_node(catalog, seed));
        seen_nodes.insert(expansion.seed_node_id.0);
    }

    while let Some((current_node, depth)) = frontier.pop_front() {
        if depth >= expansion.max_hops {
            continue;
        }
        record_dense_adjacency_diagnostics(
            DenseAdjacencyDiagnosticContext {
                catalog,
                store,
                operation: "knowledge_subgraph",
                relationship_type: expansion.relationship_type,
                requested_direction: expansion.requested_direction,
            },
            current_node,
            &mut reported_dense_groups,
            &mut fanout_reasons,
        );
        for edge in knowledge_expansion_edges_for_node(
            store,
            current_node,
            expansion.relationship_type,
            expansion.requested_direction,
        ) {
            let relationship = edge.relationship;
            if !seen_relationships.insert(relationship.id.0) {
                continue;
            }
            let new_node = !seen_nodes.contains(&edge.next_node.0);
            if new_node && nodes.len() >= expansion.node_limit {
                fanout_reasons.push(KnowledgeFanoutReasonDetail::node_limit(
                    expansion.node_limit,
                ));
                return (nodes, relationships, fanout_reasons);
            }
            if relationships.len() >= expansion.relationship_limit {
                fanout_reasons.push(KnowledgeFanoutReasonDetail::relationship_limit(
                    expansion.relationship_limit,
                ));
                return (nodes, relationships, fanout_reasons);
            }
            let Some(path) = context_path_for_relationship(
                catalog,
                store,
                "subgraph",
                depth + 1,
                edge.direction,
                relationship,
            ) else {
                continue;
            };
            relationships.push(path);
            if new_node && seen_nodes.insert(edge.next_node.0) {
                if let Some(node) = store.node(edge.next_node) {
                    nodes.push(knowledge_entity_from_node(catalog, node));
                }
                frontier.push_back((edge.next_node, depth + 1));
            }
        }
    }

    (nodes, relationships, fanout_reasons)
}

fn knowledge_expansion_edges_for_node<'a>(
    store: &'a GraphStore,
    node_id: NodeId,
    relationship_type: Option<crate::schema::RelTypeId>,
    requested_direction: KnowledgeNeighborDirection,
) -> Vec<KnowledgeExpansionEdge<'a>> {
    let Some(rel_type) = relationship_type else {
        let mut edges = Vec::new();
        let mut seen_relationships = BTreeSet::new();
        for adjacency_direction in adjacency_directions_for_request(requested_direction) {
            for entry in store.ordered_adjacency_entries_for_node(node_id, adjacency_direction) {
                let Some(relationship) = store.relationship(entry.relationship_id) else {
                    continue;
                };
                if !seen_relationships.insert(entry.relationship_id.0) {
                    continue;
                }
                edges.push(KnowledgeExpansionEdge {
                    direction: knowledge_path_direction_for_adjacency(adjacency_direction),
                    next_node: entry.neighbor_id,
                    relationship,
                });
            }
        }
        return edges;
    };

    let mut edges = Vec::new();
    let mut seen_relationships = BTreeSet::new();
    for adjacency_direction in adjacency_directions_for_request(requested_direction) {
        for entry in store.ordered_adjacency_entries(node_id, rel_type, adjacency_direction) {
            let Some(relationship) = store.relationship(entry.relationship_id) else {
                continue;
            };
            if !seen_relationships.insert(entry.relationship_id.0) {
                continue;
            }
            edges.push(KnowledgeExpansionEdge {
                direction: knowledge_path_direction_for_adjacency(adjacency_direction),
                next_node: entry.neighbor_id,
                relationship,
            });
        }
    }
    edges
}

fn record_dense_adjacency_diagnostics(
    context: DenseAdjacencyDiagnosticContext<'_>,
    node_id: NodeId,
    reported_dense_groups: &mut BTreeSet<String>,
    fanout_reasons: &mut Vec<KnowledgeFanoutReasonDetail>,
) {
    for adjacency_direction in adjacency_directions_for_request(context.requested_direction) {
        let stats = match context.relationship_type {
            Some(rel_type) => {
                vec![context
                    .store
                    .adjacency_group_stats(node_id, rel_type, adjacency_direction)]
            }
            None => context
                .store
                .adjacency_group_stats_for_node(node_id, adjacency_direction),
        };
        for stats in stats {
            if stats.layout != AdjacencyLayout::Dense {
                continue;
            }
            let rel_type_name = context
                .catalog
                .rel_type_name(stats.rel_type)
                .unwrap_or("<unknown>");
            let direction = adjacency_direction_name(adjacency_direction);
            let key = format!(
                "{}:{rel_type_name}:{direction}:{}",
                context.operation, node_id.0
            );
            if reported_dense_groups.insert(key) {
                fanout_reasons.push(KnowledgeFanoutReasonDetail::dense_adjacency(
                    context.operation,
                    rel_type_name,
                    direction,
                    node_id.0,
                    stats.degree,
                ));
            }
        }
    }
}

fn adjacency_directions_for_request(
    requested_direction: KnowledgeNeighborDirection,
) -> Vec<AdjacencyDirection> {
    match requested_direction {
        KnowledgeNeighborDirection::Outgoing => vec![AdjacencyDirection::Outgoing],
        KnowledgeNeighborDirection::Incoming => vec![AdjacencyDirection::Incoming],
        KnowledgeNeighborDirection::Both => {
            vec![AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming]
        }
    }
}

fn knowledge_path_direction_for_adjacency(
    adjacency_direction: AdjacencyDirection,
) -> KnowledgeGraphPathDirection {
    match adjacency_direction {
        AdjacencyDirection::Outgoing => KnowledgeGraphPathDirection::Outgoing,
        AdjacencyDirection::Incoming => KnowledgeGraphPathDirection::Incoming,
    }
}

fn adjacency_direction_name(adjacency_direction: AdjacencyDirection) -> &'static str {
    match adjacency_direction {
        AdjacencyDirection::Outgoing => "outgoing",
        AdjacencyDirection::Incoming => "incoming",
    }
}

fn qos_admission_name(admission: &QosAdmission) -> &'static str {
    match admission {
        QosAdmission::Admit => "admit",
        QosAdmission::Defer { .. } => "defer",
        QosAdmission::Reject { .. } => "reject",
    }
}

fn seed_node_by_label_and_external_id<'a>(
    catalog: &Catalog,
    store: &'a GraphStore,
    label: &str,
    external_id: &str,
) -> Option<&'a NodeRecord> {
    let label_id = catalog.label_id(label)?;
    store
        .scan_nodes(Some(label_id))
        .find(|node| projected_node_external_id(node) == external_id)
}

fn node_by_label_property_external_id<'a>(
    catalog: &Catalog,
    store: &'a GraphStore,
    label: &str,
    property: &str,
    external_id: &str,
) -> Option<&'a NodeRecord> {
    let label_id = catalog.label_id(label)?;
    store.scan_nodes(Some(label_id)).find(|node| {
        node.properties
            .get(property)
            .is_some_and(|value| value_to_external_id(value) == external_id)
    })
}

fn knowledge_induced_edges_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeInducedEdgeListRequest,
) -> Result<KnowledgeInducedEdgeListOutput> {
    if request.external_ids.is_empty() || request.external_ids.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge induced edge read requires non-empty external ids".to_string(),
        ));
    }

    let graph_commit_epoch = store.commit_epoch();
    let requested_ids = request
        .external_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut matched_external_ids = BTreeSet::new();
    let mut matched_node_ids = BTreeSet::new();
    for node in store.scan_nodes(None) {
        if let Some(external_id) = node_external_id(node) {
            if requested_ids.contains(&external_id) {
                matched_external_ids.insert(external_id);
                matched_node_ids.insert(node.id);
            }
        }
    }
    let mut missing_external_ids = Vec::new();
    let mut seen_missing = BTreeSet::new();
    for external_id in &request.external_ids {
        if !matched_external_ids.contains(external_id) && seen_missing.insert(external_id.clone()) {
            missing_external_ids.push(external_id.clone());
        }
    }

    let mut rows = store
        .scan_relationships(None)
        .filter(|relationship| {
            matched_node_ids.contains(&relationship.source)
                && matched_node_ids.contains(&relationship.target)
        })
        .filter_map(|relationship| induced_edge_row(catalog, store, relationship))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.source_id
            .cmp(&right.source_id)
            .then_with(|| left.target_id.cmp(&right.target_id))
            .then_with(|| left.relationship_type.cmp(&right.relationship_type))
            .then_with(|| left.relationship_id.cmp(&right.relationship_id))
    });
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeInducedEdgeListOutput {
        graph_commit_epoch,
        rows,
        matched_node_count: matched_node_ids.len(),
        missing_external_ids,
        matched_count,
        returned_count,
    })
}

fn induced_edge_row(
    catalog: &Catalog,
    store: &GraphStore,
    relationship: &RelRecord,
) -> Option<KnowledgeInducedEdgeRow> {
    let source = store.node(relationship.source)?;
    let target = store.node(relationship.target)?;
    Some(KnowledgeInducedEdgeRow {
        source_id: node_external_id(source),
        source_node_id: source.id.0,
        target_id: node_external_id(target),
        target_node_id: target.id.0,
        relationship_id: relationship.id.0,
        relationship_type: catalog
            .rel_type_name(relationship.rel_type)
            .unwrap_or("<unknown>")
            .to_string(),
        strength: relationship_strength_value(relationship),
    })
}

fn relationship_strength_value(relationship: &RelRecord) -> Value {
    relationship
        .properties
        .get("strength")
        .or_else(|| relationship.properties.get("confidence"))
        .cloned()
        .unwrap_or(Value::Float(0.5))
}

fn context_path_for_relationship(
    catalog: &Catalog,
    store: &GraphStore,
    seed_hit_id: &str,
    hop: usize,
    direction: KnowledgeGraphPathDirection,
    relationship: &RelRecord,
) -> Option<KnowledgeGraphContextPath> {
    let source = store.node(relationship.source)?;
    let target = store.node(relationship.target)?;
    Some(KnowledgeGraphContextPath {
        seed_hit_id: seed_hit_id.to_string(),
        hop,
        direction,
        relationship_id: relationship.id.0,
        relationship_type: catalog
            .rel_type_name(relationship.rel_type)
            .unwrap_or("<unknown>")
            .to_string(),
        relationship_properties: relationship.properties.clone(),
        source_node_id: relationship.source.0,
        source_labels: node_label_names(catalog, source),
        source_external_id: Some(projected_node_external_id(source)),
        target_node_id: relationship.target.0,
        target_labels: node_label_names(catalog, target),
        target_external_id: Some(projected_node_external_id(target)),
    })
}

fn search_projection_graph_delta_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &SearchProjectionGraphDeltaRequest,
) -> Result<SearchProjectionDelta> {
    let operation_count = request.operation_count();
    if let Some(limit) = request.max_operations {
        if operation_count > limit {
            return Err(SkeinError::Storage(format!(
                "search projection graph delta operation count {operation_count} exceeded configured limit {limit}"
            )));
        }
    }
    if let Some(epoch) = request.complete_through_graph_commit_epoch {
        let current_epoch = store.commit_epoch();
        if epoch > current_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection graph delta complete-through epoch {epoch} is ahead of graph commit epoch {current_epoch}"
            )));
        }
    }

    let mut upserts = Vec::new();
    for node_id in &request.upsert_node_ids {
        let node = store.node(NodeId(*node_id)).ok_or_else(|| {
            SkeinError::Storage(format!(
                "search projection graph delta missing node {node_id}"
            ))
        })?;
        if let Some(row) = projection_row_from_node(catalog, node) {
            upserts.push(row);
        }
    }

    Ok(SearchProjectionDelta {
        upserts,
        deletes: request.delete_document_ids.clone(),
        max_operations: request.max_operations,
        source_graph_commit_epoch: request.complete_through_graph_commit_epoch,
    })
}

fn search_projection_commit_lag(search_index: &SearchIndex, graph_commit_epoch: u64) -> u64 {
    search_projection_freshness_commit_lag(&search_index.projection_freshness(), graph_commit_epoch)
}

fn knowledge_entity_from_node(catalog: &Catalog, node: &NodeRecord) -> KnowledgeEntity {
    KnowledgeEntity {
        node_id: node.id.0,
        labels: node_label_names(catalog, node),
        external_id: Some(projected_node_external_id(node)),
        properties: node.properties.clone(),
    }
}

fn export_canonical_graph_snapshot_for(
    catalog: &Catalog,
    store: &GraphStore,
) -> CanonicalGraphSnapshotExport {
    let nodes = store
        .scan_nodes(None)
        .map(|node| CanonicalSnapshotNode {
            node_id: node.id.0,
            stable_id: canonical_stable_id(&node.properties),
            labels: node
                .labels
                .iter()
                .filter_map(|label_id| catalog.label_name(*label_id))
                .map(str::to_string)
                .collect(),
            properties: node.properties.clone(),
        })
        .collect::<Vec<_>>();
    let relationships = store
        .scan_relationships(None)
        .map(|relationship| CanonicalSnapshotRelationship {
            relationship_id: relationship.id.0,
            stable_id: canonical_stable_id(&relationship.properties),
            source_node_id: relationship.source.0,
            target_node_id: relationship.target.0,
            rel_type: catalog
                .rel_type_name(relationship.rel_type)
                .unwrap_or_default()
                .to_string(),
            properties: relationship.properties.clone(),
        })
        .collect::<Vec<_>>();
    let graph_commit_epoch = store.commit_epoch();
    let stable_identity = canonical_snapshot_identity_audit(&nodes, &relationships);
    let logical_checksum = canonical_graph_snapshot_checksum(&nodes, &relationships);
    CanonicalGraphSnapshotExport {
        graph_commit_epoch,
        logical_checksum,
        stable_identity,
        nodes,
        relationships,
    }
}

fn canonical_stable_id(properties: &BTreeMap<String, Value>) -> Option<Value> {
    properties.get("id").cloned()
}

fn canonical_snapshot_identity_audit(
    nodes: &[CanonicalSnapshotNode],
    relationships: &[CanonicalSnapshotRelationship],
) -> CanonicalSnapshotIdentityAudit {
    let nodes_without_stable_id = nodes
        .iter()
        .filter(|node| node.stable_id.is_none())
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    let relationships_without_stable_id = relationships
        .iter()
        .filter(|relationship| relationship.stable_id.is_none())
        .map(|relationship| relationship.relationship_id)
        .collect::<Vec<_>>();
    let duplicate_node_stable_ids =
        duplicate_stable_ids(nodes.iter().filter_map(|node| node.stable_id.as_ref()));
    let duplicate_relationship_stable_ids = duplicate_stable_ids(
        relationships
            .iter()
            .filter_map(|relationship| relationship.stable_id.as_ref()),
    );
    let requires_stable_id_mapping = !nodes_without_stable_id.is_empty()
        || !relationships_without_stable_id.is_empty()
        || !duplicate_node_stable_ids.is_empty()
        || !duplicate_relationship_stable_ids.is_empty();
    CanonicalSnapshotIdentityAudit {
        requires_stable_id_mapping,
        nodes_without_stable_id,
        relationships_without_stable_id,
        duplicate_node_stable_ids,
        duplicate_relationship_stable_ids,
    }
}

fn duplicate_stable_ids<'a>(values: impl Iterator<Item = &'a Value>) -> Vec<Value> {
    let mut counts = BTreeMap::<Value, usize>::new();
    for value in values {
        *counts.entry(value.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter_map(|(value, count)| (count > 1).then_some(value))
        .collect()
}

fn duplicate_u64s(values: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut counts = BTreeMap::<u64, usize>::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
        .into_iter()
        .filter_map(|(value, count)| (count > 1).then_some(value))
        .collect()
}

fn canonical_graph_snapshot_checksum(
    nodes: &[CanonicalSnapshotNode],
    relationships: &[CanonicalSnapshotRelationship],
) -> u64 {
    let mut body = String::new();
    body.push_str("SKEIN_CANONICAL_GRAPH_SNAPSHOT_V1\n");
    body.push_str(&format!("node_count\t{}\n", nodes.len()));
    for node in nodes {
        body.push_str(&format!("node\t{}\n", node.node_id));
        append_optional_canonical_value(&mut body, "stable_id", node.stable_id.as_ref());
        for label in &node.labels {
            append_canonical_string(&mut body, "label", label);
        }
        append_canonical_properties(&mut body, &node.properties);
    }
    body.push_str(&format!("relationship_count\t{}\n", relationships.len()));
    for relationship in relationships {
        body.push_str(&format!(
            "rel\t{}\t{}\t{}\n",
            relationship.relationship_id, relationship.source_node_id, relationship.target_node_id
        ));
        append_optional_canonical_value(&mut body, "stable_id", relationship.stable_id.as_ref());
        append_canonical_string(&mut body, "type", &relationship.rel_type);
        append_canonical_properties(&mut body, &relationship.properties);
    }
    checksum_bytes(body.as_bytes())
}

fn canonical_graph_snapshot_schema_checksum(
    nodes: &[CanonicalSnapshotNode],
    relationships: &[CanonicalSnapshotRelationship],
) -> u64 {
    let labels = nodes
        .iter()
        .flat_map(|node| node.labels.iter())
        .cloned()
        .collect::<BTreeSet<_>>();
    let relationship_types = relationships
        .iter()
        .map(|relationship| relationship.rel_type.clone())
        .collect::<BTreeSet<_>>();
    let node_properties = nodes
        .iter()
        .flat_map(|node| node.properties.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let relationship_properties = relationships
        .iter()
        .flat_map(|relationship| relationship.properties.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut body = String::new();
    body.push_str("SKEIN_GRAPH_LIGHTNING_BOOTSTRAP_SCHEMA_V1\n");
    body.push_str(&format!("label_count\t{}\n", labels.len()));
    for label in labels {
        append_canonical_string(&mut body, "label", &label);
    }
    body.push_str(&format!(
        "relationship_type_count\t{}\n",
        relationship_types.len()
    ));
    for relationship_type in relationship_types {
        append_canonical_string(&mut body, "relationship_type", &relationship_type);
    }
    body.push_str(&format!(
        "node_property_key_count\t{}\n",
        node_properties.len()
    ));
    for property in node_properties {
        append_canonical_string(&mut body, "node_property", &property);
    }
    body.push_str(&format!(
        "relationship_property_key_count\t{}\n",
        relationship_properties.len()
    ));
    for property in relationship_properties {
        append_canonical_string(&mut body, "relationship_property", &property);
    }
    checksum_bytes(body.as_bytes())
}

fn encode_graph_lightning_graph_stream_body(snapshot: &CanonicalGraphSnapshotExport) -> String {
    let node_stable_keys = snapshot
        .nodes
        .iter()
        .map(|node| (node.node_id, canonical_stable_key(node.stable_id.as_ref())))
        .collect::<BTreeMap<_, _>>();
    let mut nodes = snapshot.nodes.iter().collect::<Vec<_>>();
    nodes.sort_by_key(|node| {
        (
            node.labels.clone(),
            canonical_stable_key(node.stable_id.as_ref()),
            node.node_id,
        )
    });
    let mut relationships = snapshot.relationships.iter().collect::<Vec<_>>();
    relationships.sort_by_key(|relationship| {
        (
            relationship.rel_type.clone(),
            node_stable_keys
                .get(&relationship.source_node_id)
                .cloned()
                .unwrap_or_default(),
            node_stable_keys
                .get(&relationship.target_node_id)
                .cloned()
                .unwrap_or_default(),
            canonical_stable_key(relationship.stable_id.as_ref()),
            relationship.relationship_id,
        )
    });

    let mut body = String::new();
    body.push_str("SKEIN_GRAPH_LIGHTNING_GRAPH_STREAM_V1\n");
    body.push_str(&format!(
        "format_version\t{}\n",
        GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION
    ));
    body.push_str(&format!(
        "graph_commit_epoch\t{}\n",
        snapshot.graph_commit_epoch
    ));
    body.push_str(&format!(
        "logical_checksum\t{}\n",
        snapshot.logical_checksum
    ));
    body.push_str(&format!("node_count\t{}\n", nodes.len()));
    for node in nodes {
        body.push_str(&format!("node\t{}\n", node.node_id));
        append_optional_canonical_value(&mut body, "stable_id", node.stable_id.as_ref());
        body.push_str(&format!("label_count\t{}\n", node.labels.len()));
        for label in &node.labels {
            append_canonical_string(&mut body, "label", label);
        }
        append_canonical_properties(&mut body, &node.properties);
    }
    body.push_str(&format!("relationship_count\t{}\n", relationships.len()));
    for relationship in relationships {
        body.push_str(&format!(
            "relationship\t{}\t{}\t{}\n",
            relationship.relationship_id, relationship.source_node_id, relationship.target_node_id
        ));
        append_optional_canonical_value(&mut body, "stable_id", relationship.stable_id.as_ref());
        append_canonical_string(&mut body, "relationship_type", &relationship.rel_type);
        append_canonical_properties(&mut body, &relationship.properties);
    }
    body
}

fn canonical_stable_key(value: Option<&Value>) -> String {
    let mut key = String::new();
    append_optional_canonical_value(&mut key, "stable_id", value);
    key
}

pub fn validate_graph_lightning_graph_stream(
    encoded: &str,
    manifest: Option<&GraphLightningBootstrapManifest>,
) -> GraphLightningGraphStreamValidation {
    let (body, expected_stream_checksum, mut errors) = split_graph_stream_checksum(encoded);
    let actual_stream_checksum = checksum_bytes(body.as_bytes());
    let checksum_matches = expected_stream_checksum == Some(actual_stream_checksum);
    if !checksum_matches {
        errors.push("graph stream checksum mismatch".to_string());
    }

    let mut parsed = parse_graph_lightning_graph_stream_body(body, &mut errors);

    let duplicate_node_ids = duplicate_u64s(parsed.node_ids.iter().copied());
    let duplicate_relationship_ids = duplicate_u64s(parsed.relationship_ids.iter().copied());
    let node_id_set = parsed.node_ids.iter().copied().collect::<BTreeSet<_>>();
    let missing_sources = parsed
        .relationships
        .iter()
        .filter(|(_, source, _)| !node_id_set.contains(source))
        .map(
            |(relationship_id, source, _)| CanonicalSnapshotEndpointViolation {
                relationship_id: *relationship_id,
                missing_node_id: *source,
            },
        )
        .collect::<Vec<_>>();
    let missing_targets = parsed
        .relationships
        .iter()
        .filter(|(_, _, target)| !node_id_set.contains(target))
        .map(
            |(relationship_id, _, target)| CanonicalSnapshotEndpointViolation {
                relationship_id: *relationship_id,
                missing_node_id: *target,
            },
        )
        .collect::<Vec<_>>();
    let format_version_matches =
        parsed.format_version == Some(GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION);
    let count_matches = parsed.declared_node_count == Some(parsed.node_ids.len() as u64)
        && parsed.declared_relationship_count == Some(parsed.relationship_ids.len() as u64);
    let endpoint_integrity = duplicate_node_ids.is_empty()
        && duplicate_relationship_ids.is_empty()
        && missing_sources.is_empty()
        && missing_targets.is_empty();
    let manifest_matches = manifest.is_none_or(|manifest| {
        parsed.graph_commit_epoch == Some(manifest.graph_commit_epoch)
            && parsed.logical_checksum == Some(manifest.logical_checksum)
            && expected_stream_checksum == Some(manifest.graph_stream_checksum)
            && encoded.len() == manifest.graph_stream_byte_len
            && parsed.declared_node_count == Some(manifest.node_count as u64)
            && parsed.declared_relationship_count == Some(manifest.relationship_count as u64)
    });
    if !format_version_matches {
        errors.push("graph stream format version mismatch".to_string());
    }
    if !count_matches {
        errors.push("graph stream count mismatch".to_string());
    }
    if !endpoint_integrity {
        errors.push("graph stream endpoint integrity failed".to_string());
    }
    if !manifest_matches {
        errors.push("graph stream manifest mismatch".to_string());
    }
    let is_valid = checksum_matches
        && format_version_matches
        && count_matches
        && endpoint_integrity
        && manifest_matches
        && errors.is_empty();

    GraphLightningGraphStreamValidation {
        is_valid,
        checksum_matches,
        format_version_matches,
        count_matches,
        endpoint_integrity,
        manifest_matches,
        expected_stream_checksum,
        actual_stream_checksum,
        format_version: parsed.format_version.take(),
        graph_commit_epoch: parsed.graph_commit_epoch.take(),
        logical_checksum: parsed.logical_checksum.take(),
        node_count: parsed.node_ids.len(),
        relationship_count: parsed.relationship_ids.len(),
        duplicate_node_ids,
        duplicate_relationship_ids,
        missing_sources,
        missing_targets,
        errors,
    }
}

#[derive(Debug, Default)]
struct ParsedGraphLightningGraphStream {
    format_version: Option<u64>,
    graph_commit_epoch: Option<u64>,
    logical_checksum: Option<u64>,
    declared_node_count: Option<u64>,
    declared_relationship_count: Option<u64>,
    node_ids: Vec<u64>,
    relationship_ids: Vec<u64>,
    relationships: Vec<(u64, u64, u64)>,
}

fn parse_graph_lightning_graph_stream_body(
    body: &str,
    errors: &mut Vec<String>,
) -> ParsedGraphLightningGraphStream {
    let mut cursor = GraphStreamCursor::new(body);
    let mut parsed = ParsedGraphLightningGraphStream::default();

    match cursor.read_line() {
        Some("SKEIN_GRAPH_LIGHTNING_GRAPH_STREAM_V1") => {}
        Some(line) => {
            errors.push(format!("invalid graph stream header: {line}"));
            return parsed;
        }
        None => {
            errors.push("missing graph stream header".to_string());
            return parsed;
        }
    }

    parsed.format_version =
        cursor.read_tagged_u64("format_version", "graph stream format version", errors);
    parsed.graph_commit_epoch =
        cursor.read_tagged_u64("graph_commit_epoch", "graph stream commit epoch", errors);
    parsed.logical_checksum =
        cursor.read_tagged_u64("logical_checksum", "graph stream logical checksum", errors);
    parsed.declared_node_count =
        cursor.read_tagged_u64("node_count", "graph stream node count", errors);

    let node_count = parsed.declared_node_count.unwrap_or(0);
    for _ in 0..node_count {
        if let Some(node_id) = cursor.read_node_id(errors) {
            parsed.node_ids.push(node_id);
        }
        cursor.skip_optional_canonical_value("stable_id", errors);
        let label_count = cursor
            .read_tagged_u64("label_count", "graph stream label count", errors)
            .unwrap_or(0);
        for _ in 0..label_count {
            cursor.skip_canonical_string_line("label", errors);
        }
        skip_graph_stream_properties(&mut cursor, errors);
    }

    parsed.declared_relationship_count = cursor.read_tagged_u64(
        "relationship_count",
        "graph stream relationship count",
        errors,
    );
    let relationship_count = parsed.declared_relationship_count.unwrap_or(0);
    for _ in 0..relationship_count {
        if let Some((relationship_id, source, target)) = cursor.read_relationship(errors) {
            parsed.relationship_ids.push(relationship_id);
            parsed.relationships.push((relationship_id, source, target));
        }
        cursor.skip_optional_canonical_value("stable_id", errors);
        cursor.skip_canonical_string_line("relationship_type", errors);
        skip_graph_stream_properties(&mut cursor, errors);
    }

    if !cursor.is_finished() {
        let remaining = cursor.remaining_preview();
        errors.push(format!("trailing graph stream data: {remaining}"));
    }

    parsed
}

fn skip_graph_stream_properties(cursor: &mut GraphStreamCursor<'_>, errors: &mut Vec<String>) {
    let property_count = cursor
        .read_tagged_u64("property_count", "graph stream property count", errors)
        .unwrap_or(0);
    for _ in 0..property_count {
        cursor.skip_canonical_string_line("property", errors);
        cursor.skip_canonical_value(errors);
        cursor.expect_byte(b'\n', "graph stream property value terminator", errors);
    }
}

struct GraphStreamCursor<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> GraphStreamCursor<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn is_finished(&self) -> bool {
        self.offset >= self.input.len()
    }

    fn remaining_preview(&self) -> String {
        self.input[self.offset..]
            .chars()
            .take(64)
            .collect::<String>()
            .replace('\n', "\\n")
    }

    fn read_line(&mut self) -> Option<&'a str> {
        if self.is_finished() {
            return None;
        }
        let remaining = &self.input[self.offset..];
        if let Some(line_len) = remaining.find('\n') {
            let start = self.offset;
            let end = start + line_len;
            self.offset = end + 1;
            Some(&self.input[start..end])
        } else {
            let start = self.offset;
            self.offset = self.input.len();
            Some(&self.input[start..])
        }
    }

    fn read_tagged_u64(&mut self, tag: &str, name: &str, errors: &mut Vec<String>) -> Option<u64> {
        let Some(line) = self.read_line() else {
            errors.push(format!("missing {name}"));
            return None;
        };
        let Some(raw) = line
            .strip_prefix(tag)
            .and_then(|line| line.strip_prefix('\t'))
        else {
            errors.push(format!("expected {tag} line, found {line}"));
            return None;
        };
        parse_api_u64(raw, name, errors)
    }

    fn read_node_id(&mut self, errors: &mut Vec<String>) -> Option<u64> {
        self.read_tagged_u64("node", "graph stream node id", errors)
    }

    fn read_relationship(&mut self, errors: &mut Vec<String>) -> Option<(u64, u64, u64)> {
        let Some(line) = self.read_line() else {
            errors.push("missing graph stream relationship".to_string());
            return None;
        };
        let fields = line.split('\t').collect::<Vec<_>>();
        let ["relationship", raw_id, raw_source, raw_target] = fields.as_slice() else {
            errors.push(format!("invalid graph stream relationship line: {line}"));
            return None;
        };
        let id = parse_api_u64(raw_id, "graph stream relationship id", errors);
        let source = parse_api_u64(raw_source, "graph stream relationship source", errors);
        let target = parse_api_u64(raw_target, "graph stream relationship target", errors);
        match (id, source, target) {
            (Some(id), Some(source), Some(target)) => Some((id, source, target)),
            _ => None,
        }
    }

    fn skip_optional_canonical_value(&mut self, prefix: &str, errors: &mut Vec<String>) {
        if !self.expect_str(prefix, errors) {
            return;
        }
        if !self.expect_byte(b'\t', "graph stream optional value separator", errors) {
            return;
        }
        if self.remaining().starts_with("missing") {
            self.offset += "missing".len();
        } else {
            self.skip_canonical_value(errors);
        }
        self.expect_byte(b'\n', "graph stream optional value terminator", errors);
    }

    fn skip_canonical_string_line(&mut self, prefix: &str, errors: &mut Vec<String>) {
        if !self.expect_str(prefix, errors) {
            return;
        }
        if !self.expect_byte(b'\t', "graph stream canonical string separator", errors) {
            return;
        }
        self.skip_length_prefixed_bytes("graph stream canonical string", errors);
        self.expect_byte(b'\n', "graph stream canonical string terminator", errors);
    }

    fn skip_canonical_value(&mut self, errors: &mut Vec<String>) {
        if self.remaining().starts_with("null") {
            self.offset += "null".len();
        } else if self.remaining().starts_with("bool:true") {
            self.offset += "bool:true".len();
        } else if self.remaining().starts_with("bool:false") {
            self.offset += "bool:false".len();
        } else if self.remaining().starts_with("int:") {
            self.skip_scalar_value("int:", errors);
        } else if self.remaining().starts_with("float:") {
            self.skip_scalar_value("float:", errors);
        } else if self.remaining().starts_with("string:") {
            self.offset += "string:".len();
            self.skip_length_prefixed_bytes("graph stream string value", errors);
        } else if self.remaining().starts_with("list:") {
            self.offset += "list:".len();
            let count = self.parse_decimal("graph stream list item count", errors);
            if !self.expect_byte(b':', "graph stream list count separator", errors)
                || !self.expect_byte(b'[', "graph stream list opener", errors)
            {
                return;
            }
            for _ in 0..count.unwrap_or(0) {
                self.skip_canonical_value(errors);
                self.expect_byte(b';', "graph stream list item terminator", errors);
            }
            self.expect_byte(b']', "graph stream list closer", errors);
        } else if self.remaining().starts_with("map:") {
            self.offset += "map:".len();
            let count = self.parse_decimal("graph stream map item count", errors);
            if !self.expect_byte(b':', "graph stream map count separator", errors)
                || !self.expect_byte(b'{', "graph stream map opener", errors)
            {
                return;
            }
            for _ in 0..count.unwrap_or(0) {
                self.skip_length_prefixed_bytes("graph stream map key", errors);
                if !self.expect_byte(b'=', "graph stream map key separator", errors) {
                    return;
                }
                self.skip_canonical_value(errors);
                self.expect_byte(b';', "graph stream map item terminator", errors);
            }
            self.expect_byte(b'}', "graph stream map closer", errors);
        } else {
            errors.push(format!(
                "invalid graph stream canonical value: {}",
                self.remaining_preview()
            ));
        }
    }

    fn skip_length_prefixed_bytes(&mut self, name: &str, errors: &mut Vec<String>) {
        let Some(len) = self.parse_decimal(name, errors) else {
            return;
        };
        if !self.expect_byte(b':', "graph stream length separator", errors) {
            return;
        }
        let end = self.offset.saturating_add(len as usize);
        if end > self.input.len() {
            errors.push(format!("{name} exceeds graph stream length"));
            self.offset = self.input.len();
            return;
        }
        if !self.input.is_char_boundary(end) {
            errors.push(format!("{name} ends inside a UTF-8 codepoint"));
            self.offset = self.input.len();
            return;
        }
        self.offset = end;
    }

    fn skip_scalar_value(&mut self, prefix: &str, errors: &mut Vec<String>) {
        if !self.expect_str(prefix, errors) {
            return;
        }
        while let Some(byte) = self.current_byte() {
            if matches!(byte, b';' | b'\n' | b']' | b'}') {
                break;
            }
            self.offset += 1;
        }
    }

    fn parse_decimal(&mut self, name: &str, errors: &mut Vec<String>) -> Option<u64> {
        let start = self.offset;
        while let Some(byte) = self.current_byte() {
            if !byte.is_ascii_digit() {
                break;
            }
            self.offset += 1;
        }
        if start == self.offset {
            errors.push(format!("missing {name}"));
            return None;
        }
        match self.input[start..self.offset].parse::<u64>() {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(format!(
                    "invalid {name}: {}",
                    &self.input[start..self.offset]
                ));
                None
            }
        }
    }

    fn expect_str(&mut self, expected: &str, errors: &mut Vec<String>) -> bool {
        if self.remaining().starts_with(expected) {
            self.offset += expected.len();
            true
        } else {
            errors.push(format!(
                "expected {expected}, found {}",
                self.remaining_preview()
            ));
            false
        }
    }

    fn expect_byte(&mut self, expected: u8, name: &str, errors: &mut Vec<String>) -> bool {
        if self.current_byte() == Some(expected) {
            self.offset += 1;
            true
        } else {
            errors.push(format!("expected {name}"));
            false
        }
    }

    fn current_byte(&self) -> Option<u8> {
        self.input.as_bytes().get(self.offset).copied()
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.offset..]
    }
}

fn split_graph_stream_checksum(encoded: &str) -> (&str, Option<u64>, Vec<String>) {
    let Some((body, footer)) = encoded.rsplit_once("checksum\t") else {
        return (
            encoded,
            None,
            vec!["graph stream missing checksum footer".to_string()],
        );
    };
    let raw = footer.trim();
    match raw.parse::<u64>() {
        Ok(checksum) => (body, Some(checksum), Vec::new()),
        Err(_) => (
            body,
            None,
            vec![format!("invalid graph stream checksum: {raw}")],
        ),
    }
}

fn parse_api_u64(input: &str, name: &str, errors: &mut Vec<String>) -> Option<u64> {
    match input.parse::<u64>() {
        Ok(value) => Some(value),
        Err(_) => {
            errors.push(format!("invalid {name}: {input}"));
            None
        }
    }
}

fn append_canonical_properties(body: &mut String, properties: &BTreeMap<String, Value>) {
    body.push_str(&format!("property_count\t{}\n", properties.len()));
    for (property, value) in properties {
        append_canonical_string(body, "property", property);
        append_canonical_value(body, value);
        body.push('\n');
    }
}

fn append_canonical_string(body: &mut String, prefix: &str, value: &str) {
    body.push_str(prefix);
    body.push('\t');
    body.push_str(&value.len().to_string());
    body.push(':');
    body.push_str(value);
    body.push('\n');
}

fn append_optional_canonical_value(body: &mut String, prefix: &str, value: Option<&Value>) {
    body.push_str(prefix);
    body.push('\t');
    match value {
        Some(value) => append_canonical_value(body, value),
        None => body.push_str("missing"),
    }
    body.push('\n');
}

fn append_canonical_value(body: &mut String, value: &Value) {
    match value {
        Value::Null => body.push_str("null"),
        Value::Bool(value) => body.push_str(if *value { "bool:true" } else { "bool:false" }),
        Value::Int(value) => body.push_str(&format!("int:{value}")),
        Value::Float(value) => body.push_str(&format!("float:{:016x}", value.to_bits())),
        Value::String(value) => {
            body.push_str("string:");
            body.push_str(&value.len().to_string());
            body.push(':');
            body.push_str(value);
        }
        Value::List(values) => {
            body.push_str(&format!("list:{}:[", values.len()));
            for value in values {
                append_canonical_value(body, value);
                body.push(';');
            }
            body.push(']');
        }
        Value::Map(values) => {
            body.push_str(&format!("map:{}:{{", values.len()));
            for (key, value) in values {
                body.push_str(&key.len().to_string());
                body.push(':');
                body.push_str(key);
                body.push('=');
                append_canonical_value(body, value);
                body.push(';');
            }
            body.push('}');
        }
    }
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

impl ReaderPins {
    fn oldest_epoch(&self) -> Option<u64> {
        self.active_epochs.values().min().copied()
    }
}

impl ReaderPin {
    fn new(id: u64, pins: Rc<RefCell<ReaderPins>>) -> Self {
        Self { id, pins }
    }
}

impl Drop for ReaderPin {
    fn drop(&mut self) {
        self.pins.borrow_mut().active_epochs.remove(&self.id);
    }
}

fn optimizer_catalog(catalog: &Catalog, statistics: &GraphStatistics) -> OptimizerCatalog {
    let equality_property_indexes = catalog.property_indexes().filter_map(|index| {
        if index.kind != IndexKind::Equality {
            return None;
        }
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.property.clone()))
    });
    let composite_property_indexes = catalog.composite_property_indexes().filter_map(|index| {
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.properties.clone()))
    });
    let range_property_indexes = catalog.property_indexes().filter_map(|index| {
        if index.kind != IndexKind::Range {
            return None;
        }
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.property.clone()))
    });
    let full_text_property_indexes = catalog.property_indexes().filter_map(|index| {
        if index.kind != IndexKind::FullText {
            return None;
        }
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.property.clone()))
    });
    let label_counts = statistics
        .label_counts
        .iter()
        .filter_map(|(label_id, count)| {
            catalog
                .label_name(*label_id)
                .map(|label| (label.to_string(), *count))
        });
    let rel_type_counts = statistics
        .rel_type_counts
        .iter()
        .filter_map(|(rel_type_id, count)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| (rel_type.to_string(), *count))
        });
    let rel_type_source_counts =
        statistics
            .rel_type_source_counts
            .iter()
            .filter_map(|(rel_type_id, count)| {
                catalog
                    .rel_type_name(*rel_type_id)
                    .map(|rel_type| (rel_type.to_string(), *count))
            });
    let rel_type_target_counts =
        statistics
            .rel_type_target_counts
            .iter()
            .filter_map(|(rel_type_id, count)| {
                catalog
                    .rel_type_name(*rel_type_id)
                    .map(|rel_type| (rel_type.to_string(), *count))
            });
    let path_counts = statistics.path_counts.iter().filter_map(
        |((source_label_id, rel_type_id, target_label_id), count)| {
            Some((
                (
                    catalog.label_name(*source_label_id)?.to_string(),
                    catalog.rel_type_name(*rel_type_id)?.to_string(),
                    catalog.label_name(*target_label_id)?.to_string(),
                ),
                *count,
            ))
        },
    );
    let path_source_distinct_counts = statistics.path_source_distinct_counts.iter().filter_map(
        |((source_label_id, rel_type_id, target_label_id), count)| {
            Some((
                (
                    catalog.label_name(*source_label_id)?.to_string(),
                    catalog.rel_type_name(*rel_type_id)?.to_string(),
                    catalog.label_name(*target_label_id)?.to_string(),
                ),
                *count,
            ))
        },
    );
    let path_target_distinct_counts = statistics.path_target_distinct_counts.iter().filter_map(
        |((source_label_id, rel_type_id, target_label_id), count)| {
            Some((
                (
                    catalog.label_name(*source_label_id)?.to_string(),
                    catalog.rel_type_name(*rel_type_id)?.to_string(),
                    catalog.label_name(*target_label_id)?.to_string(),
                ),
                *count,
            ))
        },
    );
    let bounded_path_counts = statistics.bounded_path_counts.iter().filter_map(
        |((source_label_id, rel_type_id, target_label_id, hops), count)| {
            Some((
                (
                    catalog.label_name(*source_label_id)?.to_string(),
                    catalog.rel_type_name(*rel_type_id)?.to_string(),
                    catalog.label_name(*target_label_id)?.to_string(),
                    *hops,
                ),
                *count,
            ))
        },
    );
    let bounded_path_source_distinct_counts = statistics
        .bounded_path_source_distinct_counts
        .iter()
        .filter_map(
            |((source_label_id, rel_type_id, target_label_id, hops), count)| {
                Some((
                    (
                        catalog.label_name(*source_label_id)?.to_string(),
                        catalog.rel_type_name(*rel_type_id)?.to_string(),
                        catalog.label_name(*target_label_id)?.to_string(),
                        *hops,
                    ),
                    *count,
                ))
            },
        );
    let bounded_path_target_distinct_counts = statistics
        .bounded_path_target_distinct_counts
        .iter()
        .filter_map(
            |((source_label_id, rel_type_id, target_label_id, hops), count)| {
                Some((
                    (
                        catalog.label_name(*source_label_id)?.to_string(),
                        catalog.rel_type_name(*rel_type_id)?.to_string(),
                        catalog.label_name(*target_label_id)?.to_string(),
                        *hops,
                    ),
                    *count,
                ))
            },
        );
    let property_distinct_counts =
        statistics
            .property_distinct_counts
            .iter()
            .filter_map(|((label_id, property), count)| {
                catalog
                    .label_name(*label_id)
                    .map(|label| ((label.to_string(), property.clone()), *count))
            });
    let property_histograms =
        statistics
            .property_histograms
            .iter()
            .filter_map(|((label_id, property), values)| {
                catalog
                    .label_name(*label_id)
                    .map(|label| ((label.to_string(), property.clone()), values.clone()))
            });
    let rel_property_distinct_counts = statistics.rel_property_distinct_counts.iter().filter_map(
        |((rel_type_id, property), count)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| ((rel_type.to_string(), property.clone()), *count))
        },
    );
    let rel_property_histograms = statistics.rel_property_histograms.iter().filter_map(
        |((rel_type_id, property), values)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| ((rel_type.to_string(), property.clone()), values.clone()))
        },
    );
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            equality_property_indexes,
            composite_property_indexes,
            range_property_indexes,
            full_text_property_indexes,
        ),
        OptimizerCatalogStatistics::new(
            label_counts,
            rel_type_counts,
            rel_type_source_counts,
            path_counts,
            bounded_path_counts,
            property_distinct_counts,
            property_histograms,
        )
        .with_relationship_type_target_counts(rel_type_target_counts)
        .with_path_source_distinct_counts(path_source_distinct_counts)
        .with_path_target_distinct_counts(path_target_distinct_counts)
        .with_bounded_path_source_distinct_counts(bounded_path_source_distinct_counts)
        .with_bounded_path_target_distinct_counts(bounded_path_target_distinct_counts)
        .with_relationship_property_distinct_counts(rel_property_distinct_counts)
        .with_relationship_property_histograms(rel_property_histograms),
    )
}

fn search_kind_to_label(kind: &str) -> Option<&'static str> {
    match kind {
        "Memory" | "memory" => Some("Memory"),
        "Message" | "message" => Some("Message"),
        "Entity" | "entity" => Some("Entity"),
        "Source" | "source" => Some("Source"),
        "SourceChunk" | "source_chunk" | "sourcechunk" | "chunk" => Some("SourceChunk"),
        "Community" | "community" => Some("Community"),
        _ => None,
    }
}

fn node_label_names(catalog: &Catalog, node: &NodeRecord) -> Vec<String> {
    node.labels
        .iter()
        .filter_map(|label_id| catalog.label_name(*label_id))
        .map(str::to_string)
        .collect()
}

fn node_external_id(node: &NodeRecord) -> Option<String> {
    node.properties
        .get("id")
        .map(value_to_external_id)
        .filter(|external_id| !external_id.is_empty())
}

fn projected_node_external_id(node: &NodeRecord) -> String {
    node_external_id(node).unwrap_or_else(|| node.id.0.to_string())
}

fn value_to_external_id(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::List(values) => values
            .iter()
            .map(value_to_external_id)
            .collect::<Vec<_>>()
            .join(","),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| format!("{key}:{}", value_to_external_id(value)))
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn schema_state_value(state: SchemaObjectState) -> Value {
    let value = match state {
        SchemaObjectState::DeleteOnly => "delete_only",
        SchemaObjectState::WriteOnly => "write_only",
        SchemaObjectState::Backfill => "backfill",
        SchemaObjectState::Validating => "validating",
        SchemaObjectState::Public => "public",
        SchemaObjectState::Gc => "gc",
    };
    Value::String(value.to_string())
}

fn schema_maintenance_actions_output(actions: Vec<SchemaMaintenanceAction>) -> QueryOutput {
    let rows = actions
        .into_iter()
        .map(|action| {
            BTreeMap::from([
                ("object_type".to_string(), Value::String(action.object_type)),
                ("object".to_string(), Value::String(action.object)),
                (
                    "from_state".to_string(),
                    schema_state_value(action.from_state),
                ),
                (
                    "to_state".to_string(),
                    action
                        .to_state
                        .map(schema_state_value)
                        .unwrap_or(Value::Null),
                ),
                ("action".to_string(), Value::String(action.action)),
            ])
        })
        .collect();
    QueryOutput { rows }
}

fn property_index_projection_rebuild_output(
    actions: Vec<PropertyIndexProjectionRebuildAction>,
) -> QueryOutput {
    let rows = actions
        .into_iter()
        .map(|action| {
            BTreeMap::from([
                ("index_kind".to_string(), Value::String(action.index_kind)),
                ("label".to_string(), Value::String(action.label)),
                (
                    "properties".to_string(),
                    Value::List(action.properties.into_iter().map(Value::String).collect()),
                ),
                (
                    "estimated_operations".to_string(),
                    Value::Int(i64::try_from(action.estimated_operations).unwrap_or(i64::MAX)),
                ),
                (
                    "indexed_entries".to_string(),
                    Value::Int(i64::try_from(action.indexed_entries).unwrap_or(i64::MAX)),
                ),
            ])
        })
        .collect();
    QueryOutput { rows }
}

fn optional_u64_value(value: Option<u64>) -> Value {
    value
        .map(|value| Value::Int(value as i64))
        .unwrap_or(Value::Null)
}

fn optional_usize_value(value: Option<usize>) -> Value {
    value
        .map(|value| Value::Int(value as i64))
        .unwrap_or(Value::Null)
}

fn enforce_read_result_row_limit(rows: &[Row], config: &DatabaseConfig) -> Result<()> {
    let Some(max_rows) = config.max_read_result_rows else {
        return Ok(());
    };
    if rows.len() > max_rows {
        return Err(SkeinError::Execution(format!(
            "read query returned {} rows, exceeding max_read_result_rows {max_rows}",
            rows.len()
        )));
    }
    Ok(())
}

fn optimizer_config_from_database_config(config: &DatabaseConfig) -> OptimizerConfig {
    let mut optimizer = OptimizerConfig::default();
    if let Some(max_groups) = config.max_optimizer_groups {
        optimizer.max_groups = max_groups;
    }
    optimizer
}

impl<'a> NowledgeGraphAdapter<'a> {
    pub fn new(db: &'a mut Database) -> Self {
        Self { db }
    }

    pub fn query(&mut self, statement: &NowledgeGraphStatement) -> Result<QueryOutput> {
        self.db
            .query_with_params(&statement.cypher, &statement.parameters)
    }

    pub fn explain(
        &self,
        statement: &NowledgeGraphStatement,
    ) -> Result<NowledgeGraphExplainOutput> {
        let output = self
            .db
            .explain_query_with_params(&statement.cypher, &statement.parameters)?;
        Ok(NowledgeGraphExplainOutput {
            plan: output.physical_plan.explain(0),
            trace: output.trace,
        })
    }

    pub fn transaction(
        &mut self,
        statements: &[NowledgeGraphStatement],
    ) -> Result<NowledgeGraphTransactionOutput> {
        let mut tx = self.db.begin_transaction();
        let mut statement_outputs = Vec::with_capacity(statements.len());
        for statement in statements {
            statement_outputs.push(tx.query_with_params(&statement.cypher, &statement.parameters)?);
        }
        let commit_output = tx.commit()?;
        Ok(NowledgeGraphTransactionOutput {
            statement_outputs,
            commit_output,
        })
    }

    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        self.db.retrieve_knowledge(search_index, request)
    }

    pub fn knowledge_entity(&self, request: &KnowledgeEntityRequest) -> KnowledgeEntityOutput {
        self.db.knowledge_entity(request)
    }

    pub fn knowledge_entity_batch(
        &self,
        request: &KnowledgeEntityBatchRequest,
    ) -> KnowledgeEntityBatchOutput {
        self.db.knowledge_entity_batch(request)
    }

    pub fn knowledge_memory_entities(
        &self,
        request: &KnowledgeMemoryEntityListRequest,
    ) -> Result<KnowledgeMemoryEntityListOutput> {
        self.db.knowledge_memory_entities(request)
    }

    pub fn knowledge_context_memory_preview(
        &self,
        request: &KnowledgeContextMemoryPreviewRequest,
    ) -> Result<KnowledgeContextMemoryPreviewOutput> {
        self.db.knowledge_context_memory_preview(request)
    }

    pub fn knowledge_memories(
        &self,
        request: &KnowledgeMemoryListRequest,
    ) -> Result<KnowledgeMemoryListOutput> {
        self.db.knowledge_memories(request)
    }

    pub fn knowledge_scoped_entity(
        &self,
        request: &KnowledgeScopedEntityRequest,
    ) -> KnowledgeEntityOutput {
        self.db.knowledge_scoped_entity(request)
    }

    pub fn knowledge_scoped_entity_batch(
        &self,
        request: &KnowledgeScopedEntityBatchRequest,
    ) -> KnowledgeEntityBatchOutput {
        self.db.knowledge_scoped_entity_batch(request)
    }

    pub fn create_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityCreateRequest,
    ) -> Result<KnowledgeEntityCreateOutput> {
        self.db.create_knowledge_entity(request)
    }

    pub fn create_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityCreateBatchRequest,
    ) -> Result<KnowledgeEntityCreateBatchOutput> {
        self.db.create_knowledge_entity_batch(request)
    }

    pub fn upsert_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityUpsertRequest,
    ) -> Result<KnowledgeEntityUpsertOutput> {
        self.db.upsert_knowledge_entity(request)
    }

    pub fn upsert_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityUpsertBatchRequest,
    ) -> Result<KnowledgeEntityUpsertBatchOutput> {
        self.db.upsert_knowledge_entity_batch(request)
    }

    pub fn knowledge_property_batch(
        &self,
        request: &KnowledgePropertyBatchRequest,
    ) -> KnowledgePropertyBatchOutput {
        self.db.knowledge_property_batch(request)
    }

    pub fn knowledge_scoped_property_batch(
        &self,
        request: &KnowledgeScopedPropertyBatchRequest,
    ) -> KnowledgePropertyBatchOutput {
        self.db.knowledge_scoped_property_batch(request)
    }

    pub fn update_knowledge_properties(
        &mut self,
        request: &KnowledgePropertyUpdateRequest,
    ) -> Result<KnowledgePropertyUpdateOutput> {
        self.db.update_knowledge_properties(request)
    }

    pub fn update_scoped_knowledge_properties(
        &mut self,
        request: &KnowledgeScopedPropertyUpdateRequest,
    ) -> Result<KnowledgePropertyUpdateOutput> {
        self.db.update_scoped_knowledge_properties(request)
    }

    pub fn update_knowledge_properties_batch(
        &mut self,
        request: &KnowledgePropertyUpdateBatchRequest,
    ) -> Result<KnowledgePropertyUpdateBatchOutput> {
        self.db.update_knowledge_properties_batch(request)
    }

    pub fn update_scoped_knowledge_properties_batch(
        &mut self,
        request: &KnowledgeScopedPropertyUpdateBatchRequest,
    ) -> Result<KnowledgePropertyUpdateBatchOutput> {
        self.db.update_scoped_knowledge_properties_batch(request)
    }

    pub fn move_knowledge_normalized_space_batch(
        &mut self,
        request: &KnowledgeNormalizedSpaceMoveBatchRequest,
    ) -> Result<KnowledgeNormalizedSpaceMoveBatchOutput> {
        self.db.move_knowledge_normalized_space_batch(request)
    }

    pub fn touch_knowledge_memory_access_batch(
        &mut self,
        request: &KnowledgeMemoryAccessBatchRequest,
    ) -> Result<KnowledgeMemoryAccessBatchOutput> {
        self.db.touch_knowledge_memory_access_batch(request)
    }

    pub fn adjust_knowledge_source_memory_count_batch(
        &mut self,
        request: &KnowledgeSourceMemoryCountBatchRequest,
    ) -> Result<KnowledgeSourceMemoryCountBatchOutput> {
        self.db.adjust_knowledge_source_memory_count_batch(request)
    }

    pub fn update_knowledge_source_lifecycle_batch(
        &mut self,
        request: &KnowledgeSourceLifecycleBatchRequest,
    ) -> Result<KnowledgeSourceLifecycleBatchOutput> {
        self.db.update_knowledge_source_lifecycle_batch(request)
    }

    pub fn knowledge_source(
        &self,
        request: &KnowledgeSourceRequest,
    ) -> Result<KnowledgeSourceOutput> {
        self.db.knowledge_source(request)
    }

    pub fn knowledge_source_ids(
        &self,
        request: &KnowledgeSourceIdListRequest,
    ) -> Result<KnowledgeSourceIdListOutput> {
        self.db.knowledge_source_ids(request)
    }

    pub fn knowledge_sources(
        &self,
        request: &KnowledgeSourceListRequest,
    ) -> Result<KnowledgeSourceListOutput> {
        self.db.knowledge_sources(request)
    }

    pub fn knowledge_source_count(&self) -> KnowledgeSourceCountOutput {
        self.db.knowledge_source_count()
    }

    pub fn knowledge_source_memories(
        &self,
        request: &KnowledgeSourceMemoryListRequest,
    ) -> Result<KnowledgeSourceMemoryListOutput> {
        self.db.knowledge_source_memories(request)
    }

    pub fn update_knowledge_memory_lifecycle_batch(
        &mut self,
        request: &KnowledgeMemoryLifecycleBatchRequest,
    ) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
        self.db.update_knowledge_memory_lifecycle_batch(request)
    }

    pub fn update_knowledge_memory_latest_batch(
        &mut self,
        request: &KnowledgeMemoryLatestBatchRequest,
    ) -> Result<KnowledgeMemoryLatestBatchOutput> {
        self.db.update_knowledge_memory_latest_batch(request)
    }

    pub fn update_knowledge_skill_usage_stats_batch(
        &mut self,
        request: &KnowledgeSkillUsageStatsBatchRequest,
    ) -> Result<KnowledgeSkillUsageStatsBatchOutput> {
        self.db.update_knowledge_skill_usage_stats_batch(request)
    }

    pub fn update_knowledge_skill_lifecycle_batch(
        &mut self,
        request: &KnowledgeSkillLifecycleBatchRequest,
    ) -> Result<KnowledgeSkillLifecycleBatchOutput> {
        self.db.update_knowledge_skill_lifecycle_batch(request)
    }

    pub fn knowledge_skill_memories(
        &self,
        request: &KnowledgeSkillMemoryListRequest,
    ) -> Result<KnowledgeSkillMemoryListOutput> {
        self.db.knowledge_skill_memories(request)
    }

    pub fn update_knowledge_thread_metadata_batch(
        &mut self,
        request: &KnowledgeThreadMetadataBatchRequest,
    ) -> Result<KnowledgeThreadMetadataBatchOutput> {
        self.db.update_knowledge_thread_metadata_batch(request)
    }

    pub fn update_knowledge_thread_message_count_batch(
        &mut self,
        request: &KnowledgeThreadMessageCountBatchRequest,
    ) -> Result<KnowledgeThreadMessageCountBatchOutput> {
        self.db.update_knowledge_thread_message_count_batch(request)
    }

    pub fn knowledge_thread_messages(
        &self,
        request: &KnowledgeThreadMessageListRequest,
    ) -> Result<KnowledgeThreadMessageListOutput> {
        self.db.knowledge_thread_messages(request)
    }

    pub fn update_knowledge_label_lifecycle_batch(
        &mut self,
        request: &KnowledgeLabelLifecycleBatchRequest,
    ) -> Result<KnowledgeLabelLifecycleBatchOutput> {
        self.db.update_knowledge_label_lifecycle_batch(request)
    }

    pub fn lookup_knowledge_labels_by_canonical_name(
        &self,
        request: &KnowledgeLabelCanonicalLookupRequest,
    ) -> Result<KnowledgeLabelUsageListOutput> {
        self.db.lookup_knowledge_labels_by_canonical_name(request)
    }

    pub fn scan_knowledge_labels_missing_canonical_name(
        &self,
        request: &KnowledgeLabelBackfillScanRequest,
    ) -> Result<KnowledgeLabelUsageListOutput> {
        self.db
            .scan_knowledge_labels_missing_canonical_name(request)
    }

    pub fn knowledge_label_usage(
        &self,
        request: &KnowledgeLabelUsageRequest,
    ) -> Result<KnowledgeLabelUsageOutput> {
        self.db.knowledge_label_usage(request)
    }

    pub fn knowledge_label_canonical_usage(
        &self,
        request: &KnowledgeLabelUsageListRequest,
    ) -> KnowledgeLabelUsageListOutput {
        self.db.knowledge_label_canonical_usage(request)
    }

    pub fn knowledge_entity_labels(
        &self,
        request: &KnowledgeEntityLabelListRequest,
    ) -> Result<KnowledgeEntityLabelListOutput> {
        self.db.knowledge_entity_labels(request)
    }

    pub fn update_knowledge_pagerank_scores_batch(
        &mut self,
        request: &KnowledgePageRankScoreBatchRequest,
    ) -> Result<KnowledgePageRankScoreBatchOutput> {
        self.db.update_knowledge_pagerank_scores_batch(request)
    }

    pub fn clear_knowledge_pagerank_scores(
        &mut self,
        request: &KnowledgePageRankClearRequest,
    ) -> Result<KnowledgePageRankClearOutput> {
        self.db.clear_knowledge_pagerank_scores(request)
    }

    pub fn knowledge_pagerank_plan(
        &self,
        request: &KnowledgePageRankPlanRequest,
    ) -> KnowledgePageRankPlanOutput {
        self.db.knowledge_pagerank_plan(request)
    }

    pub fn knowledge_pagerank_membership(
        &self,
        request: &KnowledgePageRankMembershipRequest,
    ) -> Result<KnowledgePageRankMembershipOutput> {
        self.db.knowledge_pagerank_membership(request)
    }

    pub fn knowledge_pagerank_memory_visibility(
        &self,
        request: &KnowledgePageRankMemoryVisibilityRequest,
    ) -> Result<KnowledgePageRankMemoryVisibilityOutput> {
        self.db.knowledge_pagerank_memory_visibility(request)
    }

    pub fn knowledge_pagerank_central_entity(
        &self,
        request: &KnowledgePageRankCentralEntityRequest,
    ) -> Result<KnowledgePageRankCentralEntityOutput> {
        self.db.knowledge_pagerank_central_entity(request)
    }

    pub fn clear_knowledge_community_assignments(
        &mut self,
        request: &KnowledgeCommunityAssignmentClearRequest,
    ) -> Result<KnowledgeCommunityAssignmentClearOutput> {
        self.db.clear_knowledge_community_assignments(request)
    }

    pub fn create_knowledge_community_memberships_batch(
        &mut self,
        request: &KnowledgeCommunityMembershipCreateBatchRequest,
    ) -> Result<KnowledgeCommunityMembershipCreateBatchOutput> {
        self.db
            .create_knowledge_community_memberships_batch(request)
    }

    pub fn update_knowledge_communities_batch(
        &mut self,
        request: &KnowledgeCommunityLifecycleBatchRequest,
    ) -> Result<KnowledgeCommunityLifecycleBatchOutput> {
        self.db.update_knowledge_communities_batch(request)
    }

    pub fn delete_knowledge_communities(
        &mut self,
        request: &KnowledgeCommunityCleanupRequest,
    ) -> Result<KnowledgeCommunityCleanupOutput> {
        self.db.delete_knowledge_communities(request)
    }

    pub fn stamp_knowledge_graph_meta_batch(
        &mut self,
        request: &KnowledgeGraphMetaStampBatchRequest,
    ) -> Result<KnowledgeGraphMetaStampBatchOutput> {
        self.db.stamp_knowledge_graph_meta_batch(request)
    }

    pub fn knowledge_graph_meta(
        &self,
        request: &KnowledgeGraphMetaRequest,
    ) -> Result<KnowledgeGraphMetaOutput> {
        self.db.knowledge_graph_meta(request)
    }

    pub fn delete_knowledge_graph_meta(
        &mut self,
        request: &KnowledgeGraphMetaRequest,
    ) -> Result<KnowledgeGraphMetaDeleteOutput> {
        self.db.delete_knowledge_graph_meta(request)
    }

    pub fn apply_knowledge_schema_migrations_batch(
        &mut self,
        request: &KnowledgeSchemaMigrationApplyBatchRequest,
    ) -> Result<KnowledgeSchemaMigrationApplyBatchOutput> {
        self.db.apply_knowledge_schema_migrations_batch(request)
    }

    pub fn knowledge_schema_migrations(
        &self,
        request: &KnowledgeSchemaMigrationListRequest,
    ) -> KnowledgeSchemaMigrationListOutput {
        self.db.knowledge_schema_migrations(request)
    }

    pub fn update_knowledge_augmentation_jobs_batch(
        &mut self,
        request: &KnowledgeAugmentationJobLifecycleBatchRequest,
    ) -> Result<KnowledgeAugmentationJobLifecycleBatchOutput> {
        self.db.update_knowledge_augmentation_jobs_batch(request)
    }

    pub fn knowledge_augmentation_job(
        &self,
        request: &KnowledgeAugmentationJobRequest,
    ) -> Result<KnowledgeAugmentationJobOutput> {
        self.db.knowledge_augmentation_job(request)
    }

    pub fn knowledge_augmentation_jobs(
        &self,
        request: &KnowledgeAugmentationJobListRequest,
    ) -> Result<KnowledgeAugmentationJobListOutput> {
        self.db.knowledge_augmentation_jobs(request)
    }

    pub fn interrupt_knowledge_augmentation_jobs(
        &mut self,
        request: &KnowledgeAugmentationJobInterruptRequest,
    ) -> Result<KnowledgeAugmentationJobInterruptOutput> {
        self.db.interrupt_knowledge_augmentation_jobs(request)
    }

    pub fn delete_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityDeleteRequest,
    ) -> Result<KnowledgeEntityDeleteOutput> {
        self.db.delete_knowledge_entity(request)
    }

    pub fn delete_scoped_knowledge_entity(
        &mut self,
        request: &KnowledgeScopedEntityDeleteRequest,
    ) -> Result<KnowledgeEntityDeleteOutput> {
        self.db.delete_scoped_knowledge_entity(request)
    }

    pub fn delete_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityDeleteBatchRequest,
    ) -> Result<KnowledgeEntityDeleteBatchOutput> {
        self.db.delete_knowledge_entity_batch(request)
    }

    pub fn delete_scoped_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeScopedEntityDeleteBatchRequest,
    ) -> Result<KnowledgeEntityDeleteBatchOutput> {
        self.db.delete_scoped_knowledge_entity_batch(request)
    }

    pub fn create_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipCreateRequest,
    ) -> Result<KnowledgeRelationshipCreateOutput> {
        self.db.create_knowledge_relationship(request)
    }

    pub fn create_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipCreateRequest,
    ) -> Result<KnowledgeRelationshipCreateOutput> {
        self.db.create_scoped_knowledge_relationship(request)
    }

    pub fn create_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipCreateBatchRequest,
    ) -> Result<KnowledgeRelationshipCreateBatchOutput> {
        self.db.create_knowledge_relationship_batch(request)
    }

    pub fn create_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipCreateBatchRequest,
    ) -> Result<KnowledgeRelationshipCreateBatchOutput> {
        self.db.create_scoped_knowledge_relationship_batch(request)
    }

    pub fn upsert_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipUpsertRequest,
    ) -> Result<KnowledgeRelationshipUpsertOutput> {
        self.db.upsert_knowledge_relationship(request)
    }

    pub fn upsert_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipUpsertRequest,
    ) -> Result<KnowledgeRelationshipUpsertOutput> {
        self.db.upsert_scoped_knowledge_relationship(request)
    }

    pub fn upsert_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipUpsertBatchRequest,
    ) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
        self.db.upsert_knowledge_relationship_batch(request)
    }

    pub fn upsert_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipUpsertBatchRequest,
    ) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
        self.db.upsert_scoped_knowledge_relationship_batch(request)
    }

    pub fn delete_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipDeleteRequest,
    ) -> Result<KnowledgeRelationshipDeleteOutput> {
        self.db.delete_knowledge_relationship(request)
    }

    pub fn delete_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipDeleteRequest,
    ) -> Result<KnowledgeRelationshipDeleteOutput> {
        self.db.delete_scoped_knowledge_relationship(request)
    }

    pub fn update_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipUpdateRequest,
    ) -> Result<KnowledgeRelationshipUpdateOutput> {
        self.db.update_knowledge_relationship(request)
    }

    pub fn update_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipUpdateRequest,
    ) -> Result<KnowledgeRelationshipUpdateOutput> {
        self.db.update_scoped_knowledge_relationship(request)
    }

    pub fn update_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipUpdateBatchRequest,
    ) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
        self.db.update_knowledge_relationship_batch(request)
    }

    pub fn update_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipUpdateBatchRequest,
    ) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
        self.db.update_scoped_knowledge_relationship_batch(request)
    }

    pub fn delete_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipDeleteBatchRequest,
    ) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
        self.db.delete_knowledge_relationship_batch(request)
    }

    pub fn delete_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipDeleteBatchRequest,
    ) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
        self.db.delete_scoped_knowledge_relationship_batch(request)
    }

    pub fn delete_knowledge_source_reference_relationships(
        &mut self,
        request: &KnowledgeSourceReferenceRelationshipCleanupRequest,
    ) -> Result<KnowledgeSourceReferenceRelationshipCleanupOutput> {
        self.db
            .delete_knowledge_source_reference_relationships(request)
    }

    pub fn knowledge_neighbors(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        self.db.knowledge_neighbors(request)
    }

    pub fn knowledge_scoped_neighbors(
        &self,
        request: &KnowledgeScopedNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        self.db.knowledge_scoped_neighbors(request)
    }

    pub fn knowledge_relationships(
        &self,
        request: &KnowledgeRelationshipsRequest,
    ) -> KnowledgeRelationshipsOutput {
        self.db.knowledge_relationships(request)
    }

    pub fn knowledge_scoped_relationships(
        &self,
        request: &KnowledgeScopedRelationshipsRequest,
    ) -> KnowledgeRelationshipsOutput {
        self.db.knowledge_scoped_relationships(request)
    }

    pub fn knowledge_induced_edges(
        &self,
        request: &KnowledgeInducedEdgeListRequest,
    ) -> Result<KnowledgeInducedEdgeListOutput> {
        self.db.knowledge_induced_edges(request)
    }

    pub fn knowledge_paths(&self, request: &KnowledgePathRequest) -> KnowledgePathOutput {
        self.db.knowledge_paths(request)
    }

    pub fn knowledge_scoped_paths(
        &self,
        request: &KnowledgeScopedPathRequest,
    ) -> KnowledgePathOutput {
        self.db.knowledge_scoped_paths(request)
    }

    pub fn knowledge_subgraph(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        self.db.knowledge_subgraph(request)
    }

    pub fn knowledge_scoped_subgraph(
        &self,
        request: &KnowledgeScopedSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        self.db.knowledge_scoped_subgraph(request)
    }
}

impl DatabaseTransaction<'_> {
    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        let (physical, _) = self
            .db
            .optimized_query_plan(cypher_text, &statement, parameters)?;
        let Some(mutation) = executor::mutation_command(&physical)? else {
            return Err(SkeinError::Execution(
                "transaction query must be a mutation".to_string(),
            ));
        };
        self.db.ensure_writable()?;
        self.mutations.push(mutation);
        Ok(QueryOutput { rows: Vec::new() })
    }

    pub fn commit(mut self) -> Result<QueryOutput> {
        self.db.ensure_writable()?;
        let summary = self
            .db
            .store
            .commit_mutations(&mut self.db.catalog, std::mem::take(&mut self.mutations))?;
        self.committed = true;
        Ok(QueryOutput { rows: summary.rows })
    }

    pub fn rollback(mut self) {
        self.mutations.clear();
        self.committed = true;
    }
}

impl DatabaseSession<'_> {
    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        match statement {
            cypher::Statement::BeginTransaction => {
                reject_transaction_control_parameters("BEGIN TRANSACTION", parameters)?;
                if self.transaction_mutations.is_some() {
                    return Err(SkeinError::Execution(
                        "transaction is already active".to_string(),
                    ));
                }
                self.db.ensure_writable()?;
                self.transaction_mutations = Some(Vec::new());
                Ok(QueryOutput { rows: Vec::new() })
            }
            cypher::Statement::Commit => {
                reject_transaction_control_parameters("COMMIT", parameters)?;
                let Some(mut mutations) = self.transaction_mutations.take() else {
                    return Err(SkeinError::Execution(
                        "COMMIT requires an active transaction".to_string(),
                    ));
                };
                self.db.ensure_writable()?;
                let summary = self
                    .db
                    .store
                    .commit_mutations(&mut self.db.catalog, std::mem::take(&mut mutations))?;
                Ok(QueryOutput { rows: summary.rows })
            }
            cypher::Statement::Rollback => {
                reject_transaction_control_parameters("ROLLBACK", parameters)?;
                if self.transaction_mutations.take().is_none() {
                    return Err(SkeinError::Execution(
                        "ROLLBACK requires an active transaction".to_string(),
                    ));
                }
                Ok(QueryOutput { rows: Vec::new() })
            }
            cypher::Statement::Checkpoint if self.transaction_mutations.is_some() => {
                Err(SkeinError::Execution(
                    "CHECKPOINT is not allowed inside an active transaction".to_string(),
                ))
            }
            statement if self.transaction_mutations.is_some() => {
                let mutation =
                    mutation_command_for_statement(self.db, cypher_text, &statement, parameters)?
                        .ok_or_else(|| {
                        SkeinError::Execution(
                            "session transaction query must be a mutation".to_string(),
                        )
                    })?;
                self.db.ensure_writable()?;
                self.transaction_mutations
                    .as_mut()
                    .expect("checked active transaction")
                    .push(mutation);
                Ok(QueryOutput { rows: Vec::new() })
            }
            _ => self.db.query_with_params(cypher_text, parameters),
        }
    }
}

fn reject_transaction_control_parameters(
    statement: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    if parameters.is_empty() {
        Ok(())
    } else {
        Err(SkeinError::Semantic(format!(
            "{statement} does not accept parameters"
        )))
    }
}

fn mutation_command_for_statement(
    db: &Database,
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<GraphMutation>> {
    let (physical, _) = optimized_query_plan_for(
        cypher_text,
        statement,
        parameters,
        PlanCacheMode::Bypass(PlanCacheBypassReason::MutationPlanning),
        PlanCacheContext {
            catalog: &db.catalog,
            store: &db.store,
            optimizer: &db.optimizer,
            config: &db.config,
            cache: &db.plan_cache,
        },
    )?;
    executor::mutation_command(&physical)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanCacheMode {
    Use,
    Bypass(PlanCacheBypassReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanCacheBypassReason {
    MutationPlanning,
    StatementNotCacheable,
}

impl PlanCacheBypassReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::MutationPlanning => "mutation_planning",
            Self::StatementNotCacheable => "statement_not_cacheable",
        }
    }
}

struct PlanCacheContext<'a> {
    catalog: &'a Catalog,
    store: &'a GraphStore,
    optimizer: &'a CascadesOptimizer,
    config: &'a DatabaseConfig,
    cache: &'a RefCell<PlanCache>,
}

fn optimized_query_plan_for(
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    cache_mode: PlanCacheMode,
    context: PlanCacheContext<'_>,
) -> Result<(PhysicalPlan, OptimizerTrace)> {
    let key = (cache_mode == PlanCacheMode::Use).then(|| PlanCacheKey {
        cypher: cypher_text.to_string(),
        parameters: parameters.clone(),
        graph_commit_epoch: context.store.commit_epoch(),
        max_optimizer_groups: context.config.max_optimizer_groups,
    });
    if cache_mode == PlanCacheMode::Use {
        let key = key.as_ref().expect("cache key exists in use mode");
        if let Some(cached) = context.cache.borrow_mut().get(key) {
            let mut trace = cached.trace;
            trace
                .decisions
                .push("plan cache hit: exact parameterized physical plan".to_string());
            return Ok((cached.physical_plan, trace));
        }
    }

    let logical = planner::plan_with_params(statement, parameters)?;
    let (physical_plan, trace) = context.optimizer.optimize_with_catalog(
        &logical,
        &optimizer_catalog(context.catalog, &context.store.statistics()),
    );
    let mut trace = trace;
    if cache_mode == PlanCacheMode::Use {
        let key = key.expect("cache key exists in use mode");
        context.cache.borrow_mut().insert(
            key,
            CachedPlan {
                physical_plan: physical_plan.clone(),
                trace: trace.clone(),
            },
        );
        trace
            .decisions
            .push("plan cache miss: optimized exact parameterized physical plan".to_string());
    } else if let PlanCacheMode::Bypass(reason) = cache_mode {
        context.cache.borrow_mut().record_bypass();
        trace
            .decisions
            .push(format!("plan cache bypass: {}", reason.as_str()));
    }
    Ok((physical_plan, trace))
}

fn statement_uses_plan_cache(statement: &cypher::Statement) -> bool {
    matches!(
        statement,
        cypher::Statement::MatchReturn(_)
            | cypher::Statement::ShortestPathReturn(_)
            | cypher::Statement::MatchNodesReturn(_)
            | cypher::Statement::MatchOptionalRelationshipCountSum(_)
            | cypher::Statement::MatchThreadRepairStats(_)
            | cypher::Statement::GraphAlgorithm(_)
    )
}

impl DatabaseReadTransaction {
    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        if matches!(statement, cypher::Statement::Checkpoint) {
            reject_transaction_control_parameters("CHECKPOINT", parameters)?;
            return Err(SkeinError::Execution(
                "CHECKPOINT is not allowed inside a read transaction".to_string(),
            ));
        }
        let (physical, _) = self.optimized_query_plan(cypher_text, &statement, parameters)?;
        if executor::is_mutation_plan(&physical)? {
            return Err(SkeinError::Execution(
                "read transaction query must not be a mutation".to_string(),
            ));
        }
        let rows = executor::execute(&physical, &mut self.catalog, &mut self.store)?;
        enforce_read_result_row_limit(&rows, &self.config)?;
        Ok(QueryOutput { rows })
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        let statement = cypher::parse(cypher_text)?;
        let (physical_plan, trace) =
            self.optimized_query_plan(cypher_text, &statement, parameters)?;
        if executor::is_mutation_plan(&physical_plan)? {
            return Err(SkeinError::Execution(
                "read transaction query must not be a mutation".to_string(),
            ));
        }
        Ok(ExplainOutput {
            physical_plan,
            trace,
        })
    }

    pub fn plan_cache_stats(&self) -> PlanCacheStats {
        self.plan_cache.borrow().stats()
    }

    fn optimized_query_plan(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<(PhysicalPlan, OptimizerTrace)> {
        let cache_mode = if statement_uses_plan_cache(statement) {
            PlanCacheMode::Use
        } else {
            PlanCacheMode::Bypass(PlanCacheBypassReason::StatementNotCacheable)
        };
        optimized_query_plan_for(
            cypher_text,
            statement,
            parameters,
            cache_mode,
            PlanCacheContext {
                catalog: &self.catalog,
                store: &self.store,
                optimizer: &self.optimizer,
                config: &self.config,
                cache: &self.plan_cache,
            },
        )
    }

    pub fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph {
        match rel_type {
            Some(name) => self
                .catalog
                .rel_type_id(name)
                .map(|rel_type_id| ProjectedGraph::from_store(&self.store, Some(rel_type_id)))
                .unwrap_or_else(|| ProjectedGraph::from_store_without_edges(&self.store)),
            None => ProjectedGraph::from_store(&self.store, None),
        }
    }

    pub fn export_canonical_graph_snapshot(&self) -> CanonicalGraphSnapshotExport {
        export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }

    pub fn rebuild_search_projection(
        &self,
        search_index: &mut SearchIndex,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        search_index.rebuild_from_graph(&self.catalog, &self.store, options)
    }

    pub fn repair_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_metadata_from_graph(&self.catalog, &self.store, options)
    }

    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
        }
        .retrieve_knowledge(search_index, request)
    }

    pub fn knowledge_entity(&self, request: &KnowledgeEntityRequest) -> KnowledgeEntityOutput {
        knowledge_entity_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_entity_batch(
        &self,
        request: &KnowledgeEntityBatchRequest,
    ) -> KnowledgeEntityBatchOutput {
        knowledge_entity_batch_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_memory_entities(
        &self,
        request: &KnowledgeMemoryEntityListRequest,
    ) -> Result<KnowledgeMemoryEntityListOutput> {
        knowledge_memory_entities_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_entity(
        &self,
        request: &KnowledgeScopedEntityRequest,
    ) -> KnowledgeEntityOutput {
        knowledge_scoped_entity_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_entity_batch(
        &self,
        request: &KnowledgeScopedEntityBatchRequest,
    ) -> KnowledgeEntityBatchOutput {
        knowledge_scoped_entity_batch_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_property_batch(
        &self,
        request: &KnowledgePropertyBatchRequest,
    ) -> KnowledgePropertyBatchOutput {
        knowledge_property_batch_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_property_batch(
        &self,
        request: &KnowledgeScopedPropertyBatchRequest,
    ) -> KnowledgePropertyBatchOutput {
        knowledge_scoped_property_batch_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_neighbors(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        knowledge_neighbors_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_neighbors(
        &self,
        request: &KnowledgeScopedNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        knowledge_scoped_neighbors_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_relationships(
        &self,
        request: &KnowledgeRelationshipsRequest,
    ) -> KnowledgeRelationshipsOutput {
        knowledge_relationships_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_relationships(
        &self,
        request: &KnowledgeScopedRelationshipsRequest,
    ) -> KnowledgeRelationshipsOutput {
        knowledge_scoped_relationships_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_paths(&self, request: &KnowledgePathRequest) -> KnowledgePathOutput {
        knowledge_paths_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_paths(
        &self,
        request: &KnowledgeScopedPathRequest,
    ) -> KnowledgePathOutput {
        knowledge_scoped_paths_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_subgraph(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        knowledge_subgraph_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_scoped_subgraph(
        &self,
        request: &KnowledgeScopedSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        knowledge_scoped_subgraph_for(&self.catalog, &self.store, request)
    }

    pub fn statistics(&self) -> GraphStatistics {
        self.store.statistics()
    }

    pub fn property_indexes(&self) -> Vec<IndexDescriptor> {
        self.catalog.property_indexes().cloned().collect()
    }

    pub fn composite_property_indexes(&self) -> Vec<CompositeIndexDescriptor> {
        self.catalog.composite_property_indexes().cloned().collect()
    }

    pub fn unique_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog.unique_constraints().cloned().collect()
    }

    pub fn node_property_exists_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .node_property_exists_constraints()
            .cloned()
            .collect()
    }

    pub fn relationship_property_exists_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .relationship_property_exists_constraints()
            .cloned()
            .collect()
    }

    pub fn relationship_unique_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .relationship_unique_constraints()
            .cloned()
            .collect()
    }

    pub fn table_descriptors(&self) -> Vec<TableDescriptor> {
        self.catalog.table_descriptors().cloned().collect()
    }

    pub fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        self.catalog.property_descriptors().cloned().collect()
    }
}

#[cfg(test)]
mod tests;
