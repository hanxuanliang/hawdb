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
    IndexKind, PropertyDescriptor, SchemaObjectState, TableDescriptor,
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

    pub fn update_knowledge_memory_lifecycle_batch(
        &mut self,
        request: &KnowledgeMemoryLifecycleBatchRequest,
    ) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
        update_knowledge_memory_lifecycle_batch_for(self, request)
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

    pub fn update_knowledge_label_lifecycle_batch(
        &mut self,
        request: &KnowledgeLabelLifecycleBatchRequest,
    ) -> Result<KnowledgeLabelLifecycleBatchOutput> {
        update_knowledge_label_lifecycle_batch_for(self, request)
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

    pub fn update_knowledge_memory_lifecycle_batch(
        &mut self,
        request: &KnowledgeMemoryLifecycleBatchRequest,
    ) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
        self.db.update_knowledge_memory_lifecycle_batch(request)
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

    pub fn update_knowledge_label_lifecycle_batch(
        &mut self,
        request: &KnowledgeLabelLifecycleBatchRequest,
    ) -> Result<KnowledgeLabelLifecycleBatchOutput> {
        self.db.update_knowledge_label_lifecycle_batch(request)
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
