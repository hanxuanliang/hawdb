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
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    QosAdmission, WorkClass, WorkRequest,
};
use crate::schema::{
    Catalog, CompositeIndexDescriptor, ConstraintDescriptor, GraphStatistics, IndexDescriptor,
    IndexKind, PropertyDescriptor, SchemaObjectState, TableDescriptor,
};
use crate::search::{
    SearchFusionWeights, SearchIndex, SearchMatchedSpan, SearchMode, SearchProjectionFreshness,
    SearchQueryOptions, SearchRebuildOptions, SearchRebuildSummary, SearchResultSet,
};
use crate::store::{
    AdjacencyDirection, AdjacencyLayout, DurabilityPolicy, GraphMutation, GraphStore, NodeId,
    NodeRecord, ProjectedGraphStatus, PropertyIndexProjectionRebuildAction, RecoveryMode,
    RelRecord, SchemaMaintenanceAction, StorageReclamationWatermark, StoreStableIdMapping,
    WalReplayConfig,
};
use crate::value::Value;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::rc::Rc;

mod artifact_jobs;

pub use artifact_jobs::{
    DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExternalContentArtifactJobSummary,
};

#[derive(Debug)]
pub struct Database {
    catalog: Catalog,
    store: GraphStore,
    optimizer: CascadesOptimizer,
    config: DatabaseConfig,
    reader_pins: Rc<RefCell<ReaderPins>>,
    next_derived_artifact_job_id: u64,
    derived_artifact_jobs: Vec<DerivedArtifactJob>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DatabaseConfig {
    pub read_only: bool,
    pub max_read_result_rows: Option<usize>,
    pub max_optimizer_groups: Option<usize>,
    pub recovery_mode: RecoveryMode,
    pub max_wal_replay_entries: Option<usize>,
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
    pub fanout_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrievalDiagnostics {
    pub graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub search_document_count: usize,
    pub search_filtered_document_count: usize,
    pub search_total_hits: usize,
    pub search_candidate_filtered_out_count: usize,
    pub search_limit: usize,
    pub search_truncated: bool,
    pub search_truncation_reasons: Vec<String>,
    pub rank_window: Option<usize>,
    pub search_fusion_weights: SearchFusionWeights,
    pub graph_seed_candidate_count: usize,
    pub graph_seed_returned_count: usize,
    pub graph_seed_limit: usize,
    pub graph_seed_truncated: bool,
    pub graph_seed_truncation_reasons: Vec<String>,
    pub graph_context_path_count: usize,
    pub graph_context_limit: usize,
    pub graph_context_max_hops: usize,
    pub graph_context_truncated: bool,
    pub graph_context_truncation_reasons: Vec<String>,
    pub fanout_reason_count: usize,
    pub fanout_reasons: Vec<String>,
    pub candidate_count: usize,
    pub candidate_total_count: usize,
    pub candidate_limit: Option<usize>,
    pub candidate_truncated: bool,
    pub candidate_truncation_reasons: Vec<String>,
    pub warnings: Vec<String>,
    pub empty_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrieverReport {
    pub name: String,
    pub available: bool,
    pub candidate_count: usize,
    pub limit: Option<usize>,
    pub rank_window: Option<usize>,
    pub fusion_weight: Option<f64>,
    pub truncated: bool,
    pub truncation_reasons: Vec<String>,
    pub top_candidates: Vec<KnowledgeRetrieverCandidate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrieverCandidate {
    pub id: String,
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
pub struct KnowledgeNeighborsOutput {
    pub graph_commit_epoch: u64,
    pub seed_node_id: Option<u64>,
    pub paths: Vec<KnowledgeGraphContextPath>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
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
pub struct KnowledgePathOutput {
    pub graph_commit_epoch: u64,
    pub source_node_id: Option<u64>,
    pub target_node_id: Option<u64>,
    pub paths: Vec<KnowledgeGraphPath>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeSubgraphOutput {
    pub graph_commit_epoch: u64,
    pub seed_node_id: Option<u64>,
    pub nodes: Vec<KnowledgeEntity>,
    pub relationships: Vec<KnowledgeGraphContextPath>,
    pub fanout_reasons: Vec<String>,
    pub diagnostics: KnowledgeTraversalDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeTraversalDiagnostics {
    pub seed_found: bool,
    pub target_found: Option<bool>,
    pub path_count: usize,
    pub node_count: usize,
    pub relationship_count: usize,
    pub fanout_reason_count: usize,
    pub max_hops: usize,
    pub path_limit: Option<usize>,
    pub node_limit: Option<usize>,
    pub relationship_limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntityRequest {
    pub label: String,
    pub external_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEntityOutput {
    pub graph_commit_epoch: u64,
    pub entity: Option<KnowledgeEntity>,
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
        Self {
            catalog: Catalog::default(),
            store: GraphStore::default(),
            optimizer: CascadesOptimizer::new(optimizer_config_from_database_config(
                &DatabaseConfig::default(),
            )),
            config: DatabaseConfig::default(),
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
        Self {
            optimizer: CascadesOptimizer::new(optimizer_config_from_database_config(&config)),
            config,
            ..Self::default()
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
        let store = if config.read_only {
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
        Ok(Self {
            catalog,
            store,
            optimizer: CascadesOptimizer::new(optimizer_config_from_database_config(&config)),
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
        let logical = planner::plan_with_params(&statement, parameters)?;
        let physical = self
            .optimizer
            .optimize_with_catalog(
                &logical,
                &optimizer_catalog(&self.catalog, &self.store.statistics()),
            )
            .0;
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
        let logical = planner::plan_with_params(&statement, parameters)?;
        let (physical_plan, trace) = self.optimizer.optimize_with_catalog(
            &logical,
            &optimizer_catalog(&self.catalog, &self.store.statistics()),
        );
        Ok(ExplainOutput {
            physical_plan,
            trace,
        })
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
            QosAdmission::Defer { reason } => Err(SkeinError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason } => Err(SkeinError::Storage(format!(
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
        let request = WorkRequest::background(WorkClass::Mutation, max_estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.run_bounded_schema_maintenance(max_estimated_operations),
            QosAdmission::Defer { reason } => Err(SkeinError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason } => Err(SkeinError::Storage(format!(
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
            Err(QosAdmission::Defer { reason }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason }) => {
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
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            max_estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason }) => {
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

    pub fn knowledge_neighbors(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        knowledge_neighbors_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_paths(&self, request: &KnowledgePathRequest) -> KnowledgePathOutput {
        knowledge_paths_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_subgraph(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        knowledge_subgraph_for(&self.catalog, &self.store, request)
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
        let (graph_seeds, graph_seed_candidate_count, graph_seed_fanout_reasons) = self
            .search_knowledge_graph_seeds(
                &request.query_text,
                request.graph_seed_limit,
                &request.metadata_filters,
            );
        let (graph_context_paths, fanout_reasons, graph_context_truncation_reasons) = self
            .expand_knowledge_context(
                &search,
                &graph_seeds,
                request.graph_context_limit,
                request.graph_context_max_hops,
            );
        let evidence = self.knowledge_evidence_for_search(&search, &graph_context_paths);
        let projection_freshness = search_index.projection_freshness();
        let retrievers = knowledge_retriever_reports(
            &search,
            &evidence,
            &graph_seeds,
            &graph_context_paths,
            &projection_freshness,
            request.graph_seed_limit,
            graph_seed_candidate_count,
        );
        let (candidates, candidate_total_count, candidate_fanout_reasons) = self
            .knowledge_candidates(
                &search,
                &evidence,
                &graph_seeds,
                &graph_context_paths,
                request.candidate_limit,
                request.candidate_scoring,
            );
        let mut fanout_reasons = fanout_reasons;
        fanout_reasons.extend(graph_seed_fanout_reasons);
        fanout_reasons.extend(candidate_fanout_reasons);
        let graph_commit_epoch = self.store.commit_epoch();
        let diagnostics = knowledge_retrieval_diagnostics(
            &search,
            request,
            &projection_freshness,
            graph_commit_epoch,
            KnowledgeRetrievalDiagnosticsInput {
                graph_seed_candidate_count,
                graph_seed_returned_count: graph_seeds.len(),
                graph_context_path_count: graph_context_paths.len(),
                graph_context_truncation_reasons,
                fanout_reason_count: fanout_reasons.len(),
                fanout_reasons: fanout_reasons.clone(),
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
            graph_seeds,
            graph_context_paths,
            fanout_reasons,
        }
    }

    fn expand_knowledge_context(
        &self,
        search: &SearchResultSet,
        graph_seeds: &[KnowledgeGraphSeed],
        graph_context_limit: usize,
        graph_context_max_hops: usize,
    ) -> (Vec<KnowledgeGraphContextPath>, Vec<String>, Vec<String>) {
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
                    let reason = format!(
                        "graph_context_limit {graph_context_limit} reached while expanding hit {seed_hit_id}"
                    );
                    fanout_reasons.push(reason.clone());
                    truncation_reasons.push(reason);
                    return (paths, fanout_reasons, truncation_reasons);
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

        (paths, fanout_reasons, truncation_reasons)
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
    ) -> (Vec<KnowledgeCandidate>, usize, Vec<String>) {
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
                fanout_reasons.push(format!(
                    "knowledge_candidate_limit {limit} returned from {total} merged candidates"
                ));
            }
        }
        (candidates, total, fanout_reasons)
    }

    fn search_knowledge_graph_seeds(
        &self,
        query_text: &str,
        limit: usize,
        metadata_filters: &BTreeMap<String, String>,
    ) -> (Vec<KnowledgeGraphSeed>, usize, Vec<String>) {
        if limit == 0 {
            return (Vec::new(), 0, Vec::new());
        }
        let query_terms = knowledge_query_terms(query_text);
        if query_terms.is_empty() {
            return (Vec::new(), 0, Vec::new());
        }
        let normalized_query = query_text.trim().to_ascii_lowercase();
        let mut scored = self
            .store
            .scan_nodes(None)
            .filter(|node| {
                knowledge_graph_seed_matches_filters(self.catalog, node, metadata_filters)
            })
            .filter_map(|node| {
                let (score, matched_properties) =
                    graph_seed_score(node, &query_terms, &normalized_query);
                (score > 0.0).then(|| KnowledgeGraphSeed {
                    entity: self.knowledge_entity_from_node(node),
                    score,
                    matched_properties,
                })
            })
            .collect::<Vec<_>>();

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
            vec![format!(
                "knowledge_graph_seed_limit {limit} returned from {total} matching graph seeds"
            )]
        } else {
            Vec::new()
        };
        (scored, total, fanout_reasons)
    }
}

fn knowledge_retriever_reports(
    search: &SearchResultSet,
    evidence: &[KnowledgeEvidence],
    graph_seeds: &[KnowledgeGraphSeed],
    graph_context_paths: &[KnowledgeGraphContextPath],
    projection_freshness: &SearchProjectionFreshness,
    graph_seed_limit: usize,
    graph_seed_candidate_count: usize,
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
            candidate_count: report.candidate_count,
            limit: Some(search.limit),
            rank_window: search.rank_window,
            fusion_weight: knowledge_search_retriever_fusion_weight(
                report.name.as_str(),
                search.fusion_weights,
            ),
            truncated: report.candidate_count > report.top_candidates.len(),
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
        available: graph_seed_limit > 0,
        candidate_count: graph_seed_candidate_count,
        limit: Some(graph_seed_limit),
        rank_window: None,
        fusion_weight: None,
        truncated: graph_seed_candidate_count > graph_seeds.len(),
        truncation_reasons: knowledge_graph_seed_truncation_reasons(
            graph_seed_candidate_count,
            graph_seeds.len(),
            graph_seed_limit,
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

#[derive(Debug, Clone)]
struct KnowledgeRetrievalDiagnosticsInput {
    graph_seed_candidate_count: usize,
    graph_seed_returned_count: usize,
    graph_context_path_count: usize,
    graph_context_truncation_reasons: Vec<String>,
    fanout_reason_count: usize,
    fanout_reasons: Vec<String>,
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
    let candidate_truncation_reasons = knowledge_candidate_truncation_reasons(
        input.candidate_total_count,
        input.candidate_count,
        request.candidate_limit,
    );
    if input.candidate_count == 0 {
        if search.document_count == 0 {
            empty_reasons.push("search projection has no documents".to_string());
        } else if search.filtered_document_count == 0 {
            empty_reasons.push("metadata filters matched no search documents".to_string());
        } else if search.total_hits == 0 {
            empty_reasons
                .push("search retrievers returned no hits inside filtered scope".to_string());
        } else if search.hits.is_empty() && search.truncated {
            empty_reasons.extend(search.truncation_reasons.iter().cloned());
        }
        if input.candidate_total_count == 0 {
            if request.graph_seed_limit == 0 {
                empty_reasons.push("graph seed retriever disabled by limit 0".to_string());
            } else if input.graph_seed_candidate_count == 0 {
                empty_reasons.push("graph seed retriever returned no candidates".to_string());
            }
        }
        empty_reasons.extend(candidate_truncation_reasons.iter().cloned());
        empty_reasons.push("retrieval produced no candidates".to_string());
    }
    let graph_seed_truncation_reasons = knowledge_graph_seed_truncation_reasons(
        input.graph_seed_candidate_count,
        input.graph_seed_returned_count,
        request.graph_seed_limit,
    );
    KnowledgeRetrievalDiagnostics {
        graph_commit_epoch,
        projection_source_graph_commit_epoch: projection_freshness.source_graph_commit_epoch,
        search_document_count: search.document_count,
        search_filtered_document_count: search.filtered_document_count,
        search_total_hits: search.total_hits,
        search_candidate_filtered_out_count: search.candidate_set.filtered_out_count,
        search_limit: search.limit,
        search_truncated: search.truncated,
        search_truncation_reasons: search.truncation_reasons.clone(),
        rank_window: search.rank_window,
        search_fusion_weights: search.fusion_weights,
        graph_seed_candidate_count: input.graph_seed_candidate_count,
        graph_seed_returned_count: input.graph_seed_returned_count,
        graph_seed_limit: request.graph_seed_limit,
        graph_seed_truncated: !graph_seed_truncation_reasons.is_empty(),
        graph_seed_truncation_reasons,
        graph_context_path_count: input.graph_context_path_count,
        graph_context_limit: request.graph_context_limit,
        graph_context_max_hops: request.graph_context_max_hops,
        graph_context_truncated: !input.graph_context_truncation_reasons.is_empty(),
        graph_context_truncation_reasons: input.graph_context_truncation_reasons,
        fanout_reason_count: input.fanout_reason_count,
        fanout_reasons: input.fanout_reasons,
        candidate_count: input.candidate_count,
        candidate_total_count: input.candidate_total_count,
        candidate_limit: request.candidate_limit,
        candidate_truncated: !candidate_truncation_reasons.is_empty(),
        candidate_truncation_reasons,
        warnings: knowledge_retrieval_warnings(projection_freshness, graph_commit_epoch),
        empty_reasons,
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

fn knowledge_retrieval_warnings(
    projection_freshness: &SearchProjectionFreshness,
    graph_commit_epoch: u64,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if projection_freshness
        .source_graph_commit_epoch
        .map(|projection_epoch| projection_epoch < graph_commit_epoch)
        .unwrap_or(false)
    {
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
    let entity = seed_node_by_label_and_external_id(
        catalog,
        store,
        request.label.as_str(),
        request.external_id.as_str(),
    )
    .map(|node| knowledge_entity_from_node(catalog, node));
    KnowledgeEntityOutput {
        graph_commit_epoch: store.commit_epoch(),
        entity,
    }
}

fn knowledge_neighbors_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeNeighborsRequest,
) -> KnowledgeNeighborsOutput {
    let Some(seed) = seed_node_by_label_and_external_id(
        catalog,
        store,
        request.label.as_str(),
        request.external_id.as_str(),
    ) else {
        return KnowledgeNeighborsOutput {
            graph_commit_epoch: store.commit_epoch(),
            seed_node_id: None,
            paths: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics: knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                seed_found: false,
                target_found: None,
                path_count: 0,
                node_count: 0,
                relationship_count: 0,
                fanout_reason_count: 0,
                max_hops: request.max_hops,
                path_limit: Some(request.limit),
                node_limit: None,
                relationship_limit: None,
            }),
        };
    };

    let relationship_type = match request.relationship_type.as_deref() {
        Some(name) => match catalog.rel_type_id(name) {
            Some(rel_type_id) => Some(rel_type_id),
            None => {
                return KnowledgeNeighborsOutput {
                    graph_commit_epoch: store.commit_epoch(),
                    seed_node_id: Some(seed.id.0),
                    paths: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics: knowledge_traversal_diagnostics(
                        KnowledgeTraversalDiagnosticInput {
                            seed_found: true,
                            target_found: None,
                            path_count: 0,
                            node_count: 0,
                            relationship_count: 0,
                            fanout_reason_count: 0,
                            max_hops: request.max_hops,
                            path_limit: Some(request.limit),
                            node_limit: None,
                            relationship_limit: None,
                        },
                    ),
                };
            }
        },
        None => None,
    };
    let (paths, fanout_reasons) = expand_knowledge_neighbors_for(
        catalog,
        store,
        KnowledgeNeighborExpansion {
            seed_hit_id: "seed",
            seed_node_id: seed.id,
            requested_direction: request.direction,
            relationship_type,
            limit: request.limit,
            max_hops: request.max_hops,
        },
    );
    KnowledgeNeighborsOutput {
        graph_commit_epoch: store.commit_epoch(),
        seed_node_id: Some(seed.id.0),
        diagnostics: knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            seed_found: true,
            target_found: None,
            path_count: paths.len(),
            node_count: knowledge_context_path_node_count(&paths),
            relationship_count: paths.len(),
            fanout_reason_count: fanout_reasons.len(),
            max_hops: request.max_hops,
            path_limit: Some(request.limit),
            node_limit: None,
            relationship_limit: None,
        }),
        paths,
        fanout_reasons,
    }
}

fn knowledge_paths_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgePathRequest,
) -> KnowledgePathOutput {
    let source = seed_node_by_label_and_external_id(
        catalog,
        store,
        request.source_label.as_str(),
        request.source_external_id.as_str(),
    );
    let target = seed_node_by_label_and_external_id(
        catalog,
        store,
        request.target_label.as_str(),
        request.target_external_id.as_str(),
    );
    let source_node_id = source.map(|node| node.id.0);
    let target_node_id = target.map(|node| node.id.0);

    let Some(source) = source else {
        return KnowledgePathOutput {
            graph_commit_epoch: store.commit_epoch(),
            source_node_id,
            target_node_id,
            paths: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics: knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                seed_found: false,
                target_found: Some(target_node_id.is_some()),
                path_count: 0,
                node_count: 0,
                relationship_count: 0,
                fanout_reason_count: 0,
                max_hops: request.max_hops,
                path_limit: Some(request.limit),
                node_limit: None,
                relationship_limit: None,
            }),
        };
    };
    let Some(target) = target else {
        return KnowledgePathOutput {
            graph_commit_epoch: store.commit_epoch(),
            source_node_id,
            target_node_id,
            paths: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics: knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                seed_found: true,
                target_found: Some(false),
                path_count: 0,
                node_count: 0,
                relationship_count: 0,
                fanout_reason_count: 0,
                max_hops: request.max_hops,
                path_limit: Some(request.limit),
                node_limit: None,
                relationship_limit: None,
            }),
        };
    };
    let relationship_type = match request.relationship_type.as_deref() {
        Some(name) => match catalog.rel_type_id(name) {
            Some(rel_type_id) => Some(rel_type_id),
            None => {
                return KnowledgePathOutput {
                    graph_commit_epoch: store.commit_epoch(),
                    source_node_id,
                    target_node_id,
                    paths: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics: knowledge_traversal_diagnostics(
                        KnowledgeTraversalDiagnosticInput {
                            seed_found: true,
                            target_found: Some(true),
                            path_count: 0,
                            node_count: 0,
                            relationship_count: 0,
                            fanout_reason_count: 0,
                            max_hops: request.max_hops,
                            path_limit: Some(request.limit),
                            node_limit: None,
                            relationship_limit: None,
                        },
                    ),
                };
            }
        },
        None => None,
    };

    let (paths, fanout_reasons) = expand_knowledge_paths_for(
        catalog,
        store,
        KnowledgePathExpansion {
            source_node_id: source.id,
            target_node_id: target.id,
            requested_direction: request.direction,
            relationship_type,
            max_hops: request.max_hops,
            limit: request.limit,
        },
    );
    KnowledgePathOutput {
        graph_commit_epoch: store.commit_epoch(),
        source_node_id,
        target_node_id,
        diagnostics: knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            seed_found: true,
            target_found: Some(true),
            path_count: paths.len(),
            node_count: knowledge_graph_path_node_count(&paths),
            relationship_count: paths.iter().map(|path| path.segments.len()).sum::<usize>(),
            fanout_reason_count: fanout_reasons.len(),
            max_hops: request.max_hops,
            path_limit: Some(request.limit),
            node_limit: None,
            relationship_limit: None,
        }),
        paths,
        fanout_reasons,
    }
}

fn knowledge_subgraph_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSubgraphRequest,
) -> KnowledgeSubgraphOutput {
    let Some(seed) = seed_node_by_label_and_external_id(
        catalog,
        store,
        request.label.as_str(),
        request.external_id.as_str(),
    ) else {
        return KnowledgeSubgraphOutput {
            graph_commit_epoch: store.commit_epoch(),
            seed_node_id: None,
            nodes: Vec::new(),
            relationships: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics: knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                seed_found: false,
                target_found: None,
                path_count: 0,
                node_count: 0,
                relationship_count: 0,
                fanout_reason_count: 0,
                max_hops: request.max_hops,
                path_limit: None,
                node_limit: Some(request.node_limit),
                relationship_limit: Some(request.relationship_limit),
            }),
        };
    };
    let relationship_type = match request.relationship_type.as_deref() {
        Some(name) => match catalog.rel_type_id(name) {
            Some(rel_type_id) => Some(rel_type_id),
            None => {
                return KnowledgeSubgraphOutput {
                    graph_commit_epoch: store.commit_epoch(),
                    seed_node_id: Some(seed.id.0),
                    nodes: Vec::new(),
                    relationships: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics: knowledge_traversal_diagnostics(
                        KnowledgeTraversalDiagnosticInput {
                            seed_found: true,
                            target_found: None,
                            path_count: 0,
                            node_count: 0,
                            relationship_count: 0,
                            fanout_reason_count: 0,
                            max_hops: request.max_hops,
                            path_limit: None,
                            node_limit: Some(request.node_limit),
                            relationship_limit: Some(request.relationship_limit),
                        },
                    ),
                };
            }
        },
        None => None,
    };
    let (nodes, relationships, fanout_reasons) = expand_knowledge_subgraph_for(
        catalog,
        store,
        KnowledgeSubgraphExpansion {
            seed_node_id: seed.id,
            requested_direction: request.direction,
            relationship_type,
            max_hops: request.max_hops,
            node_limit: request.node_limit,
            relationship_limit: request.relationship_limit,
        },
    );
    KnowledgeSubgraphOutput {
        graph_commit_epoch: store.commit_epoch(),
        seed_node_id: Some(seed.id.0),
        diagnostics: knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            seed_found: true,
            target_found: None,
            path_count: relationships.len(),
            node_count: nodes.len(),
            relationship_count: relationships.len(),
            fanout_reason_count: fanout_reasons.len(),
            max_hops: request.max_hops,
            path_limit: None,
            node_limit: Some(request.node_limit),
            relationship_limit: Some(request.relationship_limit),
        }),
        nodes,
        relationships,
        fanout_reasons,
    }
}

struct KnowledgeTraversalDiagnosticInput {
    seed_found: bool,
    target_found: Option<bool>,
    path_count: usize,
    node_count: usize,
    relationship_count: usize,
    fanout_reason_count: usize,
    max_hops: usize,
    path_limit: Option<usize>,
    node_limit: Option<usize>,
    relationship_limit: Option<usize>,
}

fn knowledge_traversal_diagnostics(
    input: KnowledgeTraversalDiagnosticInput,
) -> KnowledgeTraversalDiagnostics {
    KnowledgeTraversalDiagnostics {
        seed_found: input.seed_found,
        target_found: input.target_found,
        path_count: input.path_count,
        node_count: input.node_count,
        relationship_count: input.relationship_count,
        fanout_reason_count: input.fanout_reason_count,
        max_hops: input.max_hops,
        path_limit: input.path_limit,
        node_limit: input.node_limit,
        relationship_limit: input.relationship_limit,
    }
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
) -> (Vec<KnowledgeGraphContextPath>, Vec<String>) {
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
                fanout_reasons.push(format!(
                    "knowledge_neighbors limit {} reached while expanding {}",
                    expansion.limit, expansion.seed_hit_id
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
) -> (Vec<KnowledgeGraphPath>, Vec<String>) {
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
                    fanout_reasons.push(format!(
                        "knowledge_paths limit {} reached while expanding path",
                        expansion.limit
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
    Vec<String>,
) {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    let mut fanout_reasons = Vec::new();
    let mut seen_nodes = BTreeSet::new();
    let mut seen_relationships = BTreeSet::new();
    let mut reported_dense_groups = BTreeSet::new();
    let mut frontier = VecDeque::from([(expansion.seed_node_id, 0usize)]);

    if expansion.node_limit == 0 {
        fanout_reasons.push("knowledge_subgraph node_limit 0 reached".to_string());
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
                fanout_reasons.push(format!(
                    "knowledge_subgraph node_limit {} reached",
                    expansion.node_limit
                ));
                return (nodes, relationships, fanout_reasons);
            }
            if relationships.len() >= expansion.relationship_limit {
                fanout_reasons.push(format!(
                    "knowledge_subgraph relationship_limit {} reached",
                    expansion.relationship_limit
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
    fanout_reasons: &mut Vec<String>,
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
                fanout_reasons.push(format!(
                    "{} dense_adjacency {rel_type_name} {direction} node {} degree {}",
                    context.operation, node_id.0, stats.degree
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
        source_node_id: relationship.source.0,
        source_labels: node_label_names(catalog, source),
        source_external_id: Some(projected_node_external_id(source)),
        target_node_id: relationship.target.0,
        target_labels: node_label_names(catalog, target),
        target_external_id: Some(projected_node_external_id(target)),
    })
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

    pub fn knowledge_neighbors(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        self.db.knowledge_neighbors(request)
    }

    pub fn knowledge_paths(&self, request: &KnowledgePathRequest) -> KnowledgePathOutput {
        self.db.knowledge_paths(request)
    }

    pub fn knowledge_subgraph(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        self.db.knowledge_subgraph(request)
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
        let logical = planner::plan_with_params(&statement, parameters)?;
        let physical = self
            .db
            .optimizer
            .optimize_with_catalog(
                &logical,
                &optimizer_catalog(&self.db.catalog, &self.db.store.statistics()),
            )
            .0;
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
                let mutation = mutation_command_for_statement(self.db, &statement, parameters)?
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
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<GraphMutation>> {
    let logical = planner::plan_with_params(statement, parameters)?;
    let physical = db
        .optimizer
        .optimize_with_catalog(
            &logical,
            &optimizer_catalog(&db.catalog, &db.store.statistics()),
        )
        .0;
    executor::mutation_command(&physical)
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
        let logical = planner::plan_with_params(&statement, parameters)?;
        let physical = self
            .optimizer
            .optimize_with_catalog(
                &logical,
                &optimizer_catalog(&self.catalog, &self.store.statistics()),
            )
            .0;
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
        let logical = planner::plan_with_params(&statement, parameters)?;
        let (physical_plan, trace) = self.optimizer.optimize_with_catalog(
            &logical,
            &optimizer_catalog(&self.catalog, &self.store.statistics()),
        );
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

    pub fn knowledge_neighbors(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> KnowledgeNeighborsOutput {
        knowledge_neighbors_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_paths(&self, request: &KnowledgePathRequest) -> KnowledgePathOutput {
        knowledge_paths_for(&self.catalog, &self.store, request)
    }

    pub fn knowledge_subgraph(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> KnowledgeSubgraphOutput {
        knowledge_subgraph_for(&self.catalog, &self.store, request)
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
