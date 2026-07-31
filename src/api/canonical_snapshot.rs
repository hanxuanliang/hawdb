use crate::schema::Catalog;
use crate::search::{SearchProjectionDelta, SearchProjectionFreshness, SearchProjectionKind};
use crate::store::{GraphStore, StoreStableIdMapping};
use crate::value::Value;
use crate::{Result, SkeinError};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalGraphSnapshotExport {
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub stable_identity: CanonicalSnapshotIdentityAudit,
    pub nodes: Vec<CanonicalSnapshotNode>,
    pub relationships: Vec<CanonicalSnapshotRelationship>,
}

impl CanonicalGraphSnapshotExport {
    /// Build a canonical snapshot at a host-owned source watermark.
    ///
    /// Importers use this at the boundary where a foreign graph has already
    /// been read consistently. Keeping checksum and stable-identity derivation
    /// here prevents each host integration from reimplementing that contract.
    pub fn from_rows(
        graph_commit_epoch: u64,
        nodes: Vec<CanonicalSnapshotNode>,
        relationships: Vec<CanonicalSnapshotRelationship>,
    ) -> Self {
        let stable_identity = canonical_snapshot_identity_audit(&nodes, &relationships);
        let logical_checksum = canonical_graph_snapshot_checksum(&nodes, &relationships);
        Self {
            graph_commit_epoch,
            logical_checksum,
            stable_identity,
            nodes,
            relationships,
        }
    }

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
pub const GRAPH_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL: &str =
    "skein-graph-lightning-initial-import-durable-state-v1";

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
pub struct GraphLightningInitialImportReadiness {
    pub ready: bool,
    pub manifest_import_ready: bool,
    pub projection_present: bool,
    pub graph_import_caught_up: bool,
    pub projection_watermark_caught_up: bool,
    pub projection_checkpointed: bool,
    pub manifest_graph_commit_epoch: u64,
    pub target_graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_durable_source_graph_commit_epoch: Option<u64>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportCheckpoint {
    pub protocol_version: u64,
    pub import_id: String,
    pub task_id: String,
    pub fencing_token: String,
    pub object_digest: String,
    pub schema_checksum: u64,
    pub graph_stream_checksum: u64,
    pub graph_stream_byte_len: usize,
    pub manifest_graph_commit_epoch: u64,
    pub applied_graph_commit_epoch: u64,
    pub applied_search_projection_commit_epoch: Option<u64>,
    pub durable_search_projection_commit_epoch: Option<u64>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub document_identity_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportIdempotencyKey {
    pub import_id: String,
    pub task_id: String,
    pub fencing_token: String,
    pub object_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportCheckpointReadiness {
    pub ready: bool,
    pub idempotency_key_present: bool,
    pub idempotency_key: Option<GraphLightningInitialImportIdempotencyKey>,
    pub checkpoint_matches_manifest: bool,
    pub graph_checkpoint_caught_up: bool,
    pub search_projection_applied_caught_up: bool,
    pub search_projection_durable_caught_up: bool,
    pub batches_complete: bool,
    pub document_identities_present: bool,
    pub manifest_graph_commit_epoch: u64,
    pub applied_graph_commit_epoch: u64,
    pub applied_search_projection_commit_epoch: Option<u64>,
    pub durable_search_projection_commit_epoch: Option<u64>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub document_identity_count: usize,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportCheckpointProgress {
    pub applied_graph_commit_epoch: u64,
    pub applied_search_projection_commit_epoch: Option<u64>,
    pub durable_search_projection_commit_epoch: Option<u64>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub document_identity_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportCheckpointProgressReport {
    pub accepted: bool,
    pub checkpoint: GraphLightningInitialImportCheckpoint,
    pub readiness: GraphLightningInitialImportCheckpointReadiness,
    pub resume_action: GraphLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportDocumentIdentity {
    pub kind: SearchProjectionKind,
    pub document_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportDocumentIdentityKindReport {
    pub kind: SearchProjectionKind,
    pub document_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportDocumentIdentityCoverage {
    pub ready: bool,
    pub document_identity_count: usize,
    pub unique_document_identity_count: usize,
    pub expected_kinds: Vec<SearchProjectionKind>,
    pub observed_kinds: Vec<SearchProjectionKind>,
    pub kind_reports: Vec<GraphLightningInitialImportDocumentIdentityKindReport>,
    pub missing_kinds: Vec<SearchProjectionKind>,
    pub duplicate_document_ids: Vec<String>,
    pub empty_document_id_count: usize,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphLightningInitialImportResumeActionKind {
    Start,
    Resume,
    ReadyForCutover,
    Quarantine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportResumeAction {
    pub kind: GraphLightningInitialImportResumeActionKind,
    pub next_batch: Option<u64>,
    pub idempotency_key: Option<GraphLightningInitialImportIdempotencyKey>,
    pub completed_batches: u64,
    pub total_batches: u64,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportPlan {
    pub ready_for_graph_import: bool,
    pub ready_for_cutover: bool,
    pub graph_stream_validation: GraphLightningGraphStreamValidation,
    pub decoded_snapshot_import_ready: bool,
    pub decoded_graph_commit_epoch: Option<u64>,
    pub decoded_node_count: Option<usize>,
    pub decoded_relationship_count: Option<usize>,
    pub target_readiness: GraphLightningInitialImportReadiness,
    pub checkpoint_readiness: Option<GraphLightningInitialImportCheckpointReadiness>,
    pub document_identity_coverage: Option<GraphLightningInitialImportDocumentIdentityCoverage>,
    pub resume_action: GraphLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportApplyReport {
    pub applied: bool,
    pub ready_for_cutover: bool,
    pub graph_commit_epoch: u64,
    pub node_count: usize,
    pub relationship_count: usize,
    pub plan: GraphLightningInitialImportPlan,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportSearchProjectionBatchReport {
    pub ready: bool,
    pub checkpoint_present: bool,
    pub checkpoint_matches_manifest: bool,
    pub checkpoint_idempotency_key_present: bool,
    pub total_batches_match_checkpoint: bool,
    pub source_graph_commit_epoch_matches: bool,
    pub batch_position_valid: bool,
    pub operation_limit_ok: bool,
    pub empty_batch: bool,
    pub delete_count: usize,
    pub document_identity_coverage: GraphLightningInitialImportDocumentIdentityCoverage,
    pub source_graph_commit_epoch: Option<u64>,
    pub batch_index: u64,
    pub total_batches: u64,
    pub operation_count: usize,
    pub checkpoint_progress: Option<GraphLightningInitialImportCheckpointProgress>,
    pub checkpoint_progress_accepted: bool,
    pub checkpoint_progress_readiness: Option<GraphLightningInitialImportCheckpointReadiness>,
    pub checkpoint_resume_action: Option<GraphLightningInitialImportResumeAction>,
    pub checkpoint_progress_blocker_codes: Vec<String>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportSourceBundleReadiness {
    pub ready: bool,
    pub graph_source_import_ready: bool,
    pub checkpoint_present: bool,
    pub projection_batch_count: usize,
    pub ready_projection_batch_count: usize,
    pub total_batches: u64,
    pub source_fingerprint: GraphLightningInitialImportSourceFingerprint,
    pub document_identity_coverage: GraphLightningInitialImportDocumentIdentityCoverage,
    pub batch_reports: Vec<GraphLightningInitialImportSearchProjectionBatchReport>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportSourceFingerprint {
    pub protocol_version: u64,
    pub graph_commit_epoch: u64,
    pub logical_checksum: u64,
    pub graph_stream_checksum: u64,
    pub graph_stream_byte_len: usize,
    pub schema_checksum: u64,
    pub node_count: usize,
    pub relationship_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportDurableState {
    pub source_fingerprint: GraphLightningInitialImportSourceFingerprint,
    pub checkpoint: GraphLightningInitialImportCheckpoint,
    pub document_identities: Vec<GraphLightningInitialImportDocumentIdentity>,
    pub document_identity_coverage: GraphLightningInitialImportDocumentIdentityCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportDurableStateReport {
    pub persistable: bool,
    pub ready_for_cutover: bool,
    pub state: Option<GraphLightningInitialImportDurableState>,
    pub checkpoint_readiness: GraphLightningInitialImportCheckpointReadiness,
    pub resume_action: GraphLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportDurableBatchAdvanceReport {
    pub ready: bool,
    pub idempotent_replay: bool,
    pub batch_report: GraphLightningInitialImportSearchProjectionBatchReport,
    pub durable_state_report: GraphLightningInitialImportDurableStateReport,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportSessionReport {
    pub ready_for_graph_import: bool,
    pub ready_for_cutover: bool,
    pub durable_state_present: bool,
    pub durable_state_source_matches_manifest: bool,
    pub plan: GraphLightningInitialImportPlan,
    pub durable_state_report: Option<GraphLightningInitialImportDurableStateReport>,
    pub next_action: GraphLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportCutoverCatchUpReport {
    pub ready: bool,
    pub session_ready_for_cutover: bool,
    pub durable_state_present: bool,
    pub live_projection_present: bool,
    pub import_graph_commit_epoch: Option<u64>,
    pub import_durable_search_projection_commit_epoch: Option<u64>,
    pub live_graph_commit_epoch: u64,
    pub live_search_projection_commit_epoch: Option<u64>,
    pub live_durable_search_projection_commit_epoch: Option<u64>,
    pub graph_watermark_caught_up: bool,
    pub search_projection_watermark_caught_up: bool,
    pub live_projection_checkpointed: bool,
    pub live_projection_healthy: bool,
    pub cutover_watermark: Option<u64>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportSessionBundleReadiness {
    pub ready: bool,
    pub resumable: bool,
    pub ready_for_cutover: bool,
    pub source_bundle_ready: bool,
    pub session_ready_for_graph_import: bool,
    pub session_ready_for_cutover: bool,
    pub durable_state_present: bool,
    pub durable_state_source_matches_manifest: bool,
    pub catch_up_required: bool,
    pub catch_up_present: bool,
    pub catch_up_ready: bool,
    pub cutover_watermark: Option<u64>,
    pub next_action: GraphLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportStartupReadinessReport {
    pub ready: bool,
    pub source_bundle: GraphLightningInitialImportSourceBundleReadiness,
    pub session: GraphLightningInitialImportSessionReport,
    pub cutover_catch_up: Option<GraphLightningInitialImportCutoverCatchUpReport>,
    pub readiness: GraphLightningInitialImportSessionBundleReadiness,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportDurableStateCodecReport {
    pub ready: bool,
    pub protocol: String,
    pub source_fingerprint_matches_manifest: bool,
    pub state: Option<GraphLightningInitialImportDurableState>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphLightningInitialImportRecoveryReadinessReport {
    pub ready: bool,
    pub durable_state_payload_present: bool,
    pub durable_state_codec: Option<GraphLightningInitialImportDurableStateCodecReport>,
    pub startup: GraphLightningInitialImportStartupReadinessReport,
    pub next_action: GraphLightningInitialImportResumeAction,
    pub blocker_codes: Vec<String>,
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

pub(super) fn export_canonical_graph_snapshot_for(
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
    CanonicalGraphSnapshotExport::from_rows(graph_commit_epoch, nodes, relationships)
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

pub fn parse_graph_lightning_graph_stream_export(
    encoded: &str,
    manifest: Option<&GraphLightningBootstrapManifest>,
) -> Result<CanonicalGraphSnapshotExport> {
    let validation = validate_graph_lightning_graph_stream(encoded, manifest);
    if !validation.is_valid {
        return Err(SkeinError::Storage(format!(
            "graph lightning graph stream is not import ready: {}",
            validation.errors.join("; ")
        )));
    }
    let (body, _, mut errors) = split_graph_stream_checksum(encoded);
    let parsed = parse_graph_lightning_graph_stream_body(body, &mut errors);
    if !errors.is_empty() {
        return Err(SkeinError::Storage(format!(
            "graph lightning graph stream parse failed: {}",
            errors.join("; ")
        )));
    }
    let stable_identity =
        canonical_snapshot_identity_audit(&parsed.nodes, &parsed.snapshot_relationships);
    let logical_checksum =
        canonical_graph_snapshot_checksum(&parsed.nodes, &parsed.snapshot_relationships);
    let export = CanonicalGraphSnapshotExport {
        graph_commit_epoch: parsed.graph_commit_epoch.unwrap_or(0),
        logical_checksum,
        stable_identity,
        nodes: parsed.nodes,
        relationships: parsed.snapshot_relationships,
    };
    let snapshot_validation = export.validate();
    if !snapshot_validation.is_import_ready {
        return Err(SkeinError::Storage(
            "graph lightning graph stream decoded to a snapshot that is not import ready"
                .to_string(),
        ));
    }
    Ok(export)
}

pub fn graph_lightning_initial_import_plan(
    encoded_graph_stream: &str,
    manifest: &GraphLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
    checkpoint: Option<&GraphLightningInitialImportCheckpoint>,
) -> GraphLightningInitialImportPlan {
    graph_lightning_initial_import_plan_with_document_identities(
        encoded_graph_stream,
        manifest,
        target_graph_commit_epoch,
        projection_freshness,
        checkpoint,
        None,
    )
}

pub fn graph_lightning_initial_import_plan_with_document_identities(
    encoded_graph_stream: &str,
    manifest: &GraphLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
    checkpoint: Option<&GraphLightningInitialImportCheckpoint>,
    document_identities: Option<&[GraphLightningInitialImportDocumentIdentity]>,
) -> GraphLightningInitialImportPlan {
    let graph_stream_validation =
        validate_graph_lightning_graph_stream(encoded_graph_stream, Some(manifest));
    let decoded_snapshot = if graph_stream_validation.is_valid {
        parse_graph_lightning_graph_stream_export(encoded_graph_stream, Some(manifest)).ok()
    } else {
        None
    };
    let decoded_snapshot_import_ready = decoded_snapshot
        .as_ref()
        .is_some_and(|snapshot| snapshot.validate().is_import_ready);
    let decoded_graph_commit_epoch = decoded_snapshot
        .as_ref()
        .map(|snapshot| snapshot.graph_commit_epoch);
    let decoded_node_count = decoded_snapshot
        .as_ref()
        .map(|snapshot| snapshot.nodes.len());
    let decoded_relationship_count = decoded_snapshot
        .as_ref()
        .map(|snapshot| snapshot.relationships.len());
    let target_readiness = graph_lightning_initial_import_readiness(
        manifest,
        target_graph_commit_epoch,
        projection_freshness,
    );
    let checkpoint_readiness = checkpoint.map(|checkpoint| {
        graph_lightning_initial_import_checkpoint_readiness(manifest, checkpoint)
    });
    let document_identity_coverage =
        document_identities.map(graph_lightning_initial_import_document_identity_coverage);
    let resume_action = graph_lightning_initial_import_resume_action(manifest, checkpoint);
    let ready_for_graph_import = graph_stream_validation.is_valid && decoded_snapshot_import_ready;
    let document_identities_ready = document_identity_coverage
        .as_ref()
        .map(|coverage| coverage.ready)
        .unwrap_or(true);
    let ready_for_cutover = ready_for_graph_import
        && target_readiness.ready
        && checkpoint_readiness
            .as_ref()
            .map(|readiness| readiness.ready)
            .unwrap_or(false)
        && document_identities_ready
        && resume_action.kind == GraphLightningInitialImportResumeActionKind::ReadyForCutover;
    let mut blocker_codes = BTreeSet::new();
    if !graph_stream_validation.is_valid {
        blocker_codes.insert("graph_lightning_graph_stream_invalid".to_string());
    }
    if graph_stream_validation.is_valid && !decoded_snapshot_import_ready {
        blocker_codes.insert("graph_lightning_graph_stream_decode_not_import_ready".to_string());
    }
    if !target_readiness.ready {
        blocker_codes.extend(target_readiness.blocker_codes.iter().cloned());
    }
    match &checkpoint_readiness {
        Some(readiness) => {
            if !readiness.ready {
                blocker_codes.extend(readiness.blocker_codes.iter().cloned());
            }
        }
        None => {
            blocker_codes.insert("initial_import_checkpoint_missing".to_string());
        }
    }
    if let Some(coverage) = &document_identity_coverage
        && !coverage.ready
    {
        blocker_codes.extend(coverage.blocker_codes.iter().cloned());
    }
    if ready_for_graph_import && !ready_for_cutover {
        match resume_action.kind {
            GraphLightningInitialImportResumeActionKind::Start => {
                blocker_codes.insert("initial_import_not_started".to_string());
            }
            GraphLightningInitialImportResumeActionKind::Resume => {
                blocker_codes.insert("initial_import_checkpoint_incomplete".to_string());
            }
            GraphLightningInitialImportResumeActionKind::Quarantine => {
                blocker_codes.insert("initial_import_checkpoint_quarantined".to_string());
            }
            GraphLightningInitialImportResumeActionKind::ReadyForCutover => {}
        }
    }
    GraphLightningInitialImportPlan {
        ready_for_graph_import,
        ready_for_cutover,
        graph_stream_validation,
        decoded_snapshot_import_ready,
        decoded_graph_commit_epoch,
        decoded_node_count,
        decoded_relationship_count,
        target_readiness,
        checkpoint_readiness,
        document_identity_coverage,
        resume_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn graph_lightning_initial_import_readiness(
    manifest: &GraphLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
) -> GraphLightningInitialImportReadiness {
    let manifest_import_ready = manifest.validation.is_import_ready;
    let graph_import_caught_up = target_graph_commit_epoch >= manifest.graph_commit_epoch;
    let projection_present = projection_freshness.is_some();
    let projection_source_graph_commit_epoch =
        projection_freshness.and_then(|freshness| freshness.source_graph_commit_epoch);
    let projection_durable_source_graph_commit_epoch =
        projection_freshness.and_then(|freshness| freshness.durable_source_graph_commit_epoch);
    let projection_watermark_caught_up = projection_durable_source_graph_commit_epoch
        .is_some_and(|epoch| epoch >= target_graph_commit_epoch);
    let projection_checkpointed = projection_freshness
        .map(|freshness| !freshness.has_uncheckpointed_changes)
        .unwrap_or(false);
    let projection_healthy = projection_freshness
        .map(|freshness| !freshness.full_reindex_needed && !freshness.metadata_repair_needed)
        .unwrap_or(false);
    let mut blocker_codes = BTreeSet::new();
    if !manifest_import_ready {
        blocker_codes.insert("graph_lightning_manifest_not_import_ready".to_string());
    }
    if !graph_import_caught_up {
        blocker_codes.insert("graph_import_watermark_behind_manifest".to_string());
    }
    if !projection_present {
        blocker_codes.insert("search_projection_missing".to_string());
    }
    if projection_present && !projection_watermark_caught_up {
        blocker_codes.insert("search_projection_watermark_behind_graph".to_string());
    }
    if projection_present && !projection_checkpointed {
        blocker_codes.insert("search_projection_not_checkpointed".to_string());
    }
    if projection_present && !projection_healthy {
        blocker_codes.insert("search_projection_repair_required".to_string());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    GraphLightningInitialImportReadiness {
        ready: blocker_codes.is_empty(),
        manifest_import_ready,
        projection_present,
        graph_import_caught_up,
        projection_watermark_caught_up,
        projection_checkpointed,
        manifest_graph_commit_epoch: manifest.graph_commit_epoch,
        target_graph_commit_epoch,
        projection_source_graph_commit_epoch,
        projection_durable_source_graph_commit_epoch,
        blocker_codes,
    }
}

pub fn graph_lightning_initial_import_checkpoint_readiness(
    manifest: &GraphLightningBootstrapManifest,
    checkpoint: &GraphLightningInitialImportCheckpoint,
) -> GraphLightningInitialImportCheckpointReadiness {
    let idempotency_key = graph_lightning_initial_import_idempotency_key(checkpoint);
    let idempotency_key_present = idempotency_key.is_some();
    let checkpoint_matches_manifest = checkpoint.protocol_version == 1
        && checkpoint.schema_checksum == manifest.schema_checksum
        && checkpoint.graph_stream_checksum == manifest.graph_stream_checksum
        && checkpoint.graph_stream_byte_len == manifest.graph_stream_byte_len
        && checkpoint.manifest_graph_commit_epoch == manifest.graph_commit_epoch;
    let graph_checkpoint_caught_up =
        checkpoint.applied_graph_commit_epoch >= manifest.graph_commit_epoch;
    let search_projection_applied_caught_up = checkpoint
        .applied_search_projection_commit_epoch
        .is_some_and(|epoch| epoch >= checkpoint.applied_graph_commit_epoch);
    let search_projection_durable_caught_up = checkpoint
        .durable_search_projection_commit_epoch
        .is_some_and(|epoch| epoch >= checkpoint.applied_graph_commit_epoch);
    let batches_complete =
        checkpoint.total_batches > 0 && checkpoint.completed_batches == checkpoint.total_batches;
    let document_identities_present = checkpoint.document_identity_count > 0;
    let mut blocker_codes = BTreeSet::new();
    if !idempotency_key_present {
        blocker_codes.insert("initial_import_checkpoint_idempotency_key_missing".to_string());
    }
    if !checkpoint_matches_manifest {
        blocker_codes.insert("initial_import_checkpoint_manifest_mismatch".to_string());
    }
    if !graph_checkpoint_caught_up {
        blocker_codes.insert("initial_import_graph_checkpoint_behind_manifest".to_string());
    }
    if !search_projection_applied_caught_up {
        blocker_codes.insert("initial_import_search_projection_apply_behind_graph".to_string());
    }
    if !search_projection_durable_caught_up {
        blocker_codes
            .insert("initial_import_search_projection_checkpoint_behind_graph".to_string());
    }
    if !batches_complete {
        blocker_codes.insert("initial_import_batches_incomplete".to_string());
    }
    if !document_identities_present {
        blocker_codes.insert("initial_import_document_identities_missing".to_string());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    GraphLightningInitialImportCheckpointReadiness {
        ready: blocker_codes.is_empty(),
        idempotency_key_present,
        idempotency_key,
        checkpoint_matches_manifest,
        graph_checkpoint_caught_up,
        search_projection_applied_caught_up,
        search_projection_durable_caught_up,
        batches_complete,
        document_identities_present,
        manifest_graph_commit_epoch: manifest.graph_commit_epoch,
        applied_graph_commit_epoch: checkpoint.applied_graph_commit_epoch,
        applied_search_projection_commit_epoch: checkpoint.applied_search_projection_commit_epoch,
        durable_search_projection_commit_epoch: checkpoint.durable_search_projection_commit_epoch,
        completed_batches: checkpoint.completed_batches,
        total_batches: checkpoint.total_batches,
        document_identity_count: checkpoint.document_identity_count,
        blocker_codes,
    }
}

pub fn graph_lightning_initial_import_advance_checkpoint(
    manifest: &GraphLightningBootstrapManifest,
    checkpoint: &GraphLightningInitialImportCheckpoint,
    progress: GraphLightningInitialImportCheckpointProgress,
) -> GraphLightningInitialImportCheckpointProgressReport {
    let previous_readiness =
        graph_lightning_initial_import_checkpoint_readiness(manifest, checkpoint);
    let mut blocker_codes = BTreeSet::new();
    if !previous_readiness.idempotency_key_present {
        blocker_codes.insert("initial_import_checkpoint_idempotency_key_missing".to_string());
    }
    if !previous_readiness.checkpoint_matches_manifest {
        blocker_codes.insert("initial_import_checkpoint_manifest_mismatch".to_string());
    }
    if progress.applied_graph_commit_epoch < checkpoint.applied_graph_commit_epoch {
        blocker_codes.insert("initial_import_graph_checkpoint_regressed".to_string());
    }
    if optional_epoch_regressed(
        checkpoint.applied_search_projection_commit_epoch,
        progress.applied_search_projection_commit_epoch,
    ) {
        blocker_codes.insert("initial_import_search_projection_apply_regressed".to_string());
    }
    if optional_epoch_regressed(
        checkpoint.durable_search_projection_commit_epoch,
        progress.durable_search_projection_commit_epoch,
    ) {
        blocker_codes.insert("initial_import_search_projection_checkpoint_regressed".to_string());
    }
    if progress.completed_batches < checkpoint.completed_batches {
        blocker_codes.insert("initial_import_completed_batches_regressed".to_string());
    }
    if progress.total_batches < checkpoint.total_batches {
        blocker_codes.insert("initial_import_total_batches_regressed".to_string());
    }
    if progress.completed_batches > progress.total_batches {
        blocker_codes.insert("initial_import_completed_batches_exceed_total".to_string());
    }
    if progress.document_identity_count < checkpoint.document_identity_count {
        blocker_codes.insert("initial_import_document_identities_regressed".to_string());
    }

    if !blocker_codes.is_empty() {
        let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
        return GraphLightningInitialImportCheckpointProgressReport {
            accepted: false,
            checkpoint: checkpoint.clone(),
            readiness: previous_readiness,
            resume_action: graph_lightning_initial_import_resume_action(manifest, Some(checkpoint)),
            blocker_codes,
        };
    }

    let advanced = GraphLightningInitialImportCheckpoint {
        applied_graph_commit_epoch: progress.applied_graph_commit_epoch,
        applied_search_projection_commit_epoch: progress.applied_search_projection_commit_epoch,
        durable_search_projection_commit_epoch: progress.durable_search_projection_commit_epoch,
        completed_batches: progress.completed_batches,
        total_batches: progress.total_batches,
        document_identity_count: progress.document_identity_count,
        ..checkpoint.clone()
    };
    let readiness = graph_lightning_initial_import_checkpoint_readiness(manifest, &advanced);
    let resume_action = graph_lightning_initial_import_resume_action(manifest, Some(&advanced));
    GraphLightningInitialImportCheckpointProgressReport {
        accepted: true,
        checkpoint: advanced,
        readiness,
        resume_action,
        blocker_codes: Vec::new(),
    }
}

pub fn graph_lightning_initial_import_search_projection_batch_report(
    manifest: &GraphLightningBootstrapManifest,
    checkpoint: Option<&GraphLightningInitialImportCheckpoint>,
    delta: &SearchProjectionDelta,
    batch_index: u64,
    total_batches: u64,
) -> GraphLightningInitialImportSearchProjectionBatchReport {
    let document_identities = search_projection_delta_document_identities(delta);
    graph_lightning_initial_import_search_projection_batch_report_with_document_identities(
        manifest,
        checkpoint,
        delta,
        batch_index,
        total_batches,
        &document_identities,
    )
}

pub fn graph_lightning_initial_import_source_bundle_readiness(
    manifest: &GraphLightningBootstrapManifest,
    checkpoint: Option<&GraphLightningInitialImportCheckpoint>,
    projection_batches: &[SearchProjectionDelta],
) -> GraphLightningInitialImportSourceBundleReadiness {
    let source_fingerprint = graph_lightning_initial_import_source_fingerprint(manifest);
    let total_batches = projection_batches.len() as u64;
    let document_identities = projection_batches
        .iter()
        .flat_map(search_projection_delta_document_identities)
        .collect::<Vec<_>>();
    let document_identity_coverage =
        graph_lightning_initial_import_document_identity_coverage(&document_identities);
    let batch_reports = projection_batches
        .iter()
        .enumerate()
        .map(|(batch_index, delta)| {
            graph_lightning_initial_import_search_projection_batch_report_with_document_identities(
                manifest,
                checkpoint,
                delta,
                batch_index as u64,
                total_batches,
                &document_identities,
            )
        })
        .collect::<Vec<_>>();
    let ready_projection_batch_count = batch_reports.iter().filter(|report| report.ready).count();
    let graph_source_import_ready = manifest.validation.is_import_ready;
    let checkpoint_present = checkpoint.is_some();
    let mut blocker_codes = BTreeSet::new();
    if !graph_source_import_ready {
        blocker_codes.insert("initial_import_source_bundle_graph_not_import_ready".to_string());
    }
    if !checkpoint_present {
        blocker_codes.insert("initial_import_source_bundle_checkpoint_missing".to_string());
    }
    if projection_batches.is_empty() {
        blocker_codes.insert("initial_import_source_bundle_projection_batches_missing".to_string());
    }
    if !document_identity_coverage.ready {
        blocker_codes.extend(document_identity_coverage.blocker_codes.iter().cloned());
    }
    for report in &batch_reports {
        blocker_codes.extend(report.blocker_codes.iter().cloned());
        blocker_codes.extend(report.checkpoint_progress_blocker_codes.iter().cloned());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    let ready = blocker_codes.is_empty()
        && graph_source_import_ready
        && checkpoint_present
        && !projection_batches.is_empty()
        && ready_projection_batch_count == projection_batches.len();

    GraphLightningInitialImportSourceBundleReadiness {
        ready,
        graph_source_import_ready,
        checkpoint_present,
        projection_batch_count: projection_batches.len(),
        ready_projection_batch_count,
        total_batches,
        source_fingerprint,
        document_identity_coverage,
        batch_reports,
        blocker_codes,
    }
}

pub fn graph_lightning_initial_import_search_projection_batch_report_with_document_identities(
    manifest: &GraphLightningBootstrapManifest,
    checkpoint: Option<&GraphLightningInitialImportCheckpoint>,
    delta: &SearchProjectionDelta,
    batch_index: u64,
    total_batches: u64,
    document_identities: &[GraphLightningInitialImportDocumentIdentity],
) -> GraphLightningInitialImportSearchProjectionBatchReport {
    let source_graph_commit_epoch_matches =
        delta.source_graph_commit_epoch == Some(manifest.graph_commit_epoch);
    let batch_position_valid = total_batches > 0 && batch_index < total_batches;
    let operation_count = delta.operation_count();
    let operation_limit_ok = delta
        .max_operations
        .is_none_or(|limit| operation_count <= limit);
    let checkpoint_readiness = checkpoint.map(|checkpoint| {
        graph_lightning_initial_import_checkpoint_readiness(manifest, checkpoint)
    });
    let checkpoint_present = checkpoint.is_some();
    let checkpoint_matches_manifest = checkpoint_readiness
        .as_ref()
        .is_some_and(|readiness| readiness.checkpoint_matches_manifest);
    let checkpoint_idempotency_key_present = checkpoint_readiness
        .as_ref()
        .is_some_and(|readiness| readiness.idempotency_key_present);
    let total_batches_match_checkpoint = checkpoint
        .map(|checkpoint| checkpoint.total_batches == total_batches)
        .unwrap_or(false);
    let empty_batch = operation_count == 0;
    let delete_count = delta.deletes.len();
    let document_identity_coverage =
        graph_lightning_initial_import_document_identity_coverage(document_identities);
    let mut blocker_codes = BTreeSet::new();
    if !checkpoint_present {
        blocker_codes
            .insert("initial_import_search_projection_batch_checkpoint_missing".to_string());
    }
    if checkpoint_present && !checkpoint_matches_manifest {
        blocker_codes
            .insert("initial_import_search_projection_batch_checkpoint_mismatch".to_string());
    }
    if checkpoint_present && !checkpoint_idempotency_key_present {
        blocker_codes.insert(
            "initial_import_search_projection_batch_checkpoint_idempotency_missing".to_string(),
        );
    }
    if checkpoint_present && !total_batches_match_checkpoint {
        blocker_codes.insert("initial_import_search_projection_batch_total_mismatch".to_string());
    }
    if !source_graph_commit_epoch_matches {
        blocker_codes.insert("initial_import_search_projection_batch_epoch_mismatch".to_string());
    }
    if !batch_position_valid {
        blocker_codes.insert("initial_import_search_projection_batch_position_invalid".to_string());
    }
    if !operation_limit_ok {
        blocker_codes.insert("initial_import_search_projection_batch_limit_exceeded".to_string());
    }
    if empty_batch {
        blocker_codes.insert("initial_import_search_projection_batch_empty".to_string());
    }
    if delete_count > 0 {
        blocker_codes.insert("initial_import_search_projection_batch_has_deletes".to_string());
    }
    if !document_identity_coverage.ready {
        blocker_codes.extend(document_identity_coverage.blocker_codes.iter().cloned());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    let ready = blocker_codes.is_empty();
    let checkpoint_progress = if ready {
        checkpoint.map(|checkpoint| {
            let completed_batches = checkpoint
                .completed_batches
                .max(batch_index.saturating_add(1));
            GraphLightningInitialImportCheckpointProgress {
                applied_graph_commit_epoch: checkpoint
                    .applied_graph_commit_epoch
                    .max(manifest.graph_commit_epoch),
                applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
                durable_search_projection_commit_epoch: checkpoint
                    .durable_search_projection_commit_epoch,
                completed_batches,
                total_batches: checkpoint.total_batches.max(total_batches),
                document_identity_count: checkpoint
                    .document_identity_count
                    .max(document_identities.len()),
            }
        })
    } else {
        None
    };
    let checkpoint_progress_report = checkpoint_progress.as_ref().and_then(|progress| {
        checkpoint.map(|checkpoint| {
            graph_lightning_initial_import_advance_checkpoint(
                manifest,
                checkpoint,
                progress.clone(),
            )
        })
    });
    let checkpoint_progress_accepted = checkpoint_progress_report
        .as_ref()
        .is_some_and(|report| report.accepted);
    let checkpoint_progress_readiness = checkpoint_progress_report
        .as_ref()
        .map(|report| report.readiness.clone());
    let checkpoint_resume_action = checkpoint_progress_report
        .as_ref()
        .map(|report| report.resume_action.clone());
    let checkpoint_progress_blocker_codes = checkpoint_progress_report
        .map(|report| report.blocker_codes)
        .unwrap_or_default();
    GraphLightningInitialImportSearchProjectionBatchReport {
        ready,
        checkpoint_present,
        checkpoint_matches_manifest,
        checkpoint_idempotency_key_present,
        total_batches_match_checkpoint,
        source_graph_commit_epoch_matches,
        batch_position_valid,
        operation_limit_ok,
        empty_batch,
        delete_count,
        document_identity_coverage,
        source_graph_commit_epoch: delta.source_graph_commit_epoch,
        batch_index,
        total_batches,
        operation_count,
        checkpoint_progress,
        checkpoint_progress_accepted,
        checkpoint_progress_readiness,
        checkpoint_resume_action,
        checkpoint_progress_blocker_codes,
        blocker_codes,
    }
}

pub fn graph_lightning_initial_import_durable_state_report(
    manifest: &GraphLightningBootstrapManifest,
    checkpoint: &GraphLightningInitialImportCheckpoint,
    document_identities: &[GraphLightningInitialImportDocumentIdentity],
) -> GraphLightningInitialImportDurableStateReport {
    let checkpoint_readiness =
        graph_lightning_initial_import_checkpoint_readiness(manifest, checkpoint);
    let resume_action = graph_lightning_initial_import_resume_action(manifest, Some(checkpoint));
    let document_identity_coverage =
        graph_lightning_initial_import_document_identity_coverage(document_identities);
    let mut blocker_codes = BTreeSet::new();
    if !checkpoint_readiness.idempotency_key_present {
        blocker_codes.insert("initial_import_durable_state_idempotency_missing".to_string());
    }
    if !checkpoint_readiness.checkpoint_matches_manifest {
        blocker_codes.insert("initial_import_durable_state_manifest_mismatch".to_string());
    }
    if checkpoint.total_batches == 0 {
        blocker_codes.insert("initial_import_durable_state_total_batches_missing".to_string());
    }
    if checkpoint.completed_batches > checkpoint.total_batches {
        blocker_codes
            .insert("initial_import_durable_state_completed_batches_exceed_total".to_string());
    }
    if checkpoint.document_identity_count > document_identities.len() {
        blocker_codes
            .insert("initial_import_durable_state_document_identity_regressed".to_string());
    }
    let persistable = blocker_codes.is_empty();
    if !document_identity_coverage.ready {
        blocker_codes.extend(document_identity_coverage.blocker_codes.iter().cloned());
    }
    let ready_for_cutover =
        persistable && checkpoint_readiness.ready && document_identity_coverage.ready;
    let state = persistable.then(|| GraphLightningInitialImportDurableState {
        source_fingerprint: graph_lightning_initial_import_source_fingerprint(manifest),
        checkpoint: checkpoint.clone(),
        document_identities: document_identities.to_vec(),
        document_identity_coverage: document_identity_coverage.clone(),
    });
    GraphLightningInitialImportDurableStateReport {
        persistable,
        ready_for_cutover,
        state,
        checkpoint_readiness,
        resume_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

fn graph_lightning_initial_import_durable_state_json(
    state: &GraphLightningInitialImportDurableState,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": GRAPH_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL,
        "source_fingerprint": graph_lightning_initial_import_source_fingerprint_json(&state.source_fingerprint),
        "checkpoint": graph_lightning_initial_import_checkpoint_json(&state.checkpoint),
        "document_identities": state
            .document_identities
            .iter()
            .map(graph_lightning_initial_import_document_identity_json)
            .collect::<Vec<_>>(),
        "document_identity_coverage": graph_lightning_initial_import_document_identity_coverage_json(&state.document_identity_coverage),
    })
}

pub fn graph_lightning_initial_import_encode_durable_state(
    state: &GraphLightningInitialImportDurableState,
) -> Result<String> {
    serde_json::to_string(&graph_lightning_initial_import_durable_state_json(state)).map_err(|_| {
        SkeinError::Execution(
            "initial import durable state serialization failed: invalid_json".to_string(),
        )
    })
}

fn graph_lightning_initial_import_decode_durable_state_value(
    manifest: &GraphLightningBootstrapManifest,
    value: &serde_json::Value,
) -> Result<GraphLightningInitialImportDurableStateCodecReport> {
    let mut blocker_codes = BTreeSet::new();
    let protocol = required_json_string(value, "protocol")?.to_string();
    if protocol != GRAPH_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL {
        blocker_codes.insert("initial_import_durable_state_codec_protocol_mismatch".to_string());
    }
    let source_fingerprint = parse_graph_lightning_initial_import_source_fingerprint(
        required_json_object(value, "source_fingerprint")?,
    )?;
    let source_fingerprint_matches_manifest =
        source_fingerprint == graph_lightning_initial_import_source_fingerprint(manifest);
    if !source_fingerprint_matches_manifest {
        blocker_codes.insert("initial_import_durable_state_codec_source_mismatch".to_string());
    }
    let checkpoint = parse_graph_lightning_initial_import_checkpoint(required_json_object(
        value,
        "checkpoint",
    )?)?;
    let document_identities = parse_graph_lightning_initial_import_document_identities(
        required_json_array(value, "document_identities")?,
    )?;
    let state_report = graph_lightning_initial_import_durable_state_report(
        manifest,
        &checkpoint,
        &document_identities,
    );
    if !state_report.persistable {
        blocker_codes.extend(state_report.blocker_codes.iter().cloned());
    }
    let ready = blocker_codes.is_empty();
    let state = (ready && source_fingerprint_matches_manifest)
        .then_some(state_report.state)
        .flatten();
    Ok(GraphLightningInitialImportDurableStateCodecReport {
        ready,
        protocol,
        source_fingerprint_matches_manifest,
        state,
        blocker_codes: blocker_codes.into_iter().collect(),
    })
}

pub fn graph_lightning_initial_import_decode_durable_state(
    manifest: &GraphLightningBootstrapManifest,
    raw: &str,
) -> Result<GraphLightningInitialImportDurableStateCodecReport> {
    let value = serde_json::from_str::<serde_json::Value>(raw).map_err(|_| {
        SkeinError::Semantic("initial import durable state parse failed: invalid_json".to_string())
    })?;
    graph_lightning_initial_import_decode_durable_state_value(manifest, &value)
}

pub fn graph_lightning_initial_import_advance_durable_state_with_search_projection_batch(
    manifest: &GraphLightningBootstrapManifest,
    state: &GraphLightningInitialImportDurableState,
    delta: &SearchProjectionDelta,
    batch_index: u64,
    total_batches: u64,
) -> GraphLightningInitialImportDurableBatchAdvanceReport {
    let document_identities = merge_initial_import_document_identities(
        &state.document_identities,
        &search_projection_delta_document_identities(delta),
    );
    let batch_report =
        graph_lightning_initial_import_search_projection_batch_report_with_document_identities(
            manifest,
            Some(&state.checkpoint),
            delta,
            batch_index,
            total_batches,
            &document_identities,
        );
    let idempotent_replay = batch_index < state.checkpoint.completed_batches;
    let durable_state_report = if let Some(progress) = batch_report.checkpoint_progress.as_ref() {
        let progress_report = graph_lightning_initial_import_advance_checkpoint(
            manifest,
            &state.checkpoint,
            progress.clone(),
        );
        if progress_report.accepted {
            graph_lightning_initial_import_durable_state_report(
                manifest,
                &progress_report.checkpoint,
                &document_identities,
            )
        } else {
            graph_lightning_initial_import_durable_state_report(
                manifest,
                &state.checkpoint,
                &state.document_identities,
            )
        }
    } else {
        graph_lightning_initial_import_durable_state_report(
            manifest,
            &state.checkpoint,
            &state.document_identities,
        )
    };
    let mut blocker_codes = BTreeSet::new();
    if !batch_report.ready {
        blocker_codes.extend(batch_report.blocker_codes.iter().cloned());
    }
    if !batch_report.checkpoint_progress_accepted {
        blocker_codes.extend(
            batch_report
                .checkpoint_progress_blocker_codes
                .iter()
                .cloned(),
        );
        if batch_report.checkpoint_progress.is_none() {
            blocker_codes
                .insert("initial_import_durable_batch_checkpoint_progress_missing".to_string());
        }
    }
    if !durable_state_report.persistable {
        blocker_codes.extend(durable_state_report.blocker_codes.iter().cloned());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    GraphLightningInitialImportDurableBatchAdvanceReport {
        ready: blocker_codes.is_empty(),
        idempotent_replay,
        batch_report,
        durable_state_report,
        blocker_codes,
    }
}

pub fn graph_lightning_initial_import_session_report(
    encoded_graph_stream: &str,
    manifest: &GraphLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_freshness: Option<&SearchProjectionFreshness>,
    durable_state: Option<&GraphLightningInitialImportDurableState>,
) -> GraphLightningInitialImportSessionReport {
    let checkpoint = durable_state.map(|state| &state.checkpoint);
    let document_identities = durable_state.map(|state| state.document_identities.as_slice());
    let plan = graph_lightning_initial_import_plan_with_document_identities(
        encoded_graph_stream,
        manifest,
        target_graph_commit_epoch,
        projection_freshness,
        checkpoint,
        document_identities,
    );
    let durable_state_report = durable_state.map(|state| {
        graph_lightning_initial_import_durable_state_report(
            manifest,
            &state.checkpoint,
            &state.document_identities,
        )
    });
    let durable_state_source_matches_manifest = durable_state
        .map(|state| {
            state.source_fingerprint == graph_lightning_initial_import_source_fingerprint(manifest)
        })
        .unwrap_or(true);
    let mut blocker_codes = BTreeSet::new();
    blocker_codes.extend(plan.blocker_codes.iter().cloned());
    if !durable_state_source_matches_manifest {
        blocker_codes.insert("initial_import_durable_state_source_mismatch".to_string());
    }
    if let Some(report) = &durable_state_report
        && !report.persistable
    {
        blocker_codes.extend(report.blocker_codes.iter().cloned());
    }

    let next_action = if !durable_state_source_matches_manifest {
        GraphLightningInitialImportResumeAction {
            kind: GraphLightningInitialImportResumeActionKind::Quarantine,
            next_batch: None,
            idempotency_key: None,
            completed_batches: durable_state
                .map(|state| state.checkpoint.completed_batches)
                .unwrap_or(0),
            total_batches: durable_state
                .map(|state| state.checkpoint.total_batches)
                .unwrap_or(0),
            blocker_codes: vec!["initial_import_durable_state_source_mismatch".to_string()],
        }
    } else {
        plan.resume_action.clone()
    };
    let ready_for_graph_import =
        plan.ready_for_graph_import && durable_state_source_matches_manifest;
    let ready_for_cutover = plan.ready_for_cutover
        && durable_state_source_matches_manifest
        && durable_state_report
            .as_ref()
            .is_some_and(|report| report.ready_for_cutover);

    GraphLightningInitialImportSessionReport {
        ready_for_graph_import,
        ready_for_cutover,
        durable_state_present: durable_state.is_some(),
        durable_state_source_matches_manifest,
        plan,
        durable_state_report,
        next_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn graph_lightning_initial_import_cutover_catch_up_report(
    session: &GraphLightningInitialImportSessionReport,
    live_graph_commit_epoch: u64,
    live_projection_freshness: Option<&SearchProjectionFreshness>,
) -> GraphLightningInitialImportCutoverCatchUpReport {
    let durable_state = session
        .durable_state_report
        .as_ref()
        .and_then(|report| report.state.as_ref());
    let import_graph_commit_epoch =
        durable_state.map(|state| state.checkpoint.applied_graph_commit_epoch);
    let import_durable_search_projection_commit_epoch =
        durable_state.and_then(|state| state.checkpoint.durable_search_projection_commit_epoch);
    let live_projection_present = live_projection_freshness.is_some();
    let live_search_projection_commit_epoch =
        live_projection_freshness.and_then(|freshness| freshness.source_graph_commit_epoch);
    let live_durable_search_projection_commit_epoch =
        live_projection_freshness.and_then(|freshness| freshness.durable_source_graph_commit_epoch);
    let live_projection_checkpointed = live_projection_freshness
        .map(|freshness| !freshness.has_uncheckpointed_changes)
        .unwrap_or(false);
    let live_projection_healthy = live_projection_freshness
        .map(|freshness| !freshness.full_reindex_needed && !freshness.metadata_repair_needed)
        .unwrap_or(false);
    let graph_watermark_caught_up = import_graph_commit_epoch
        .is_some_and(|import_epoch| import_epoch >= live_graph_commit_epoch);
    let search_projection_watermark_caught_up = import_durable_search_projection_commit_epoch
        .zip(live_durable_search_projection_commit_epoch)
        .is_some_and(|(import_epoch, live_epoch)| {
            import_epoch >= live_graph_commit_epoch && live_epoch >= live_graph_commit_epoch
        });
    let cutover_watermark = if graph_watermark_caught_up
        && search_projection_watermark_caught_up
        && live_projection_checkpointed
        && live_projection_healthy
    {
        Some(live_graph_commit_epoch)
    } else {
        None
    };

    let mut blocker_codes = BTreeSet::new();
    if !session.ready_for_cutover {
        blocker_codes.insert("initial_import_session_not_ready_for_cutover".to_string());
    }
    if durable_state.is_none() {
        blocker_codes.insert("initial_import_durable_state_missing".to_string());
    }
    if !live_projection_present {
        blocker_codes.insert("initial_import_live_projection_missing".to_string());
    }
    if !graph_watermark_caught_up {
        blocker_codes.insert("initial_import_live_graph_watermark_not_caught_up".to_string());
    }
    if !search_projection_watermark_caught_up {
        blocker_codes
            .insert("initial_import_live_search_projection_watermark_not_caught_up".to_string());
    }
    if live_projection_present && !live_projection_checkpointed {
        blocker_codes.insert("initial_import_live_projection_not_checkpointed".to_string());
    }
    if live_projection_present && !live_projection_healthy {
        blocker_codes.insert("initial_import_live_projection_repair_required".to_string());
    }

    GraphLightningInitialImportCutoverCatchUpReport {
        ready: blocker_codes.is_empty(),
        session_ready_for_cutover: session.ready_for_cutover,
        durable_state_present: durable_state.is_some(),
        live_projection_present,
        import_graph_commit_epoch,
        import_durable_search_projection_commit_epoch,
        live_graph_commit_epoch,
        live_search_projection_commit_epoch,
        live_durable_search_projection_commit_epoch,
        graph_watermark_caught_up,
        search_projection_watermark_caught_up,
        live_projection_checkpointed,
        live_projection_healthy,
        cutover_watermark,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn graph_lightning_initial_import_session_bundle_readiness(
    source_bundle: &GraphLightningInitialImportSourceBundleReadiness,
    session: &GraphLightningInitialImportSessionReport,
    catch_up: Option<&GraphLightningInitialImportCutoverCatchUpReport>,
) -> GraphLightningInitialImportSessionBundleReadiness {
    let catch_up_required = session.ready_for_cutover;
    let catch_up_present = catch_up.is_some();
    let catch_up_ready = catch_up.is_some_and(|report| report.ready);
    let cutover_watermark = catch_up.and_then(|report| report.cutover_watermark);
    let resumable = source_bundle.ready
        && session.ready_for_graph_import
        && session.durable_state_present
        && session.durable_state_source_matches_manifest
        && session.next_action.kind != GraphLightningInitialImportResumeActionKind::Quarantine;
    let ready_for_cutover = resumable && session.ready_for_cutover && catch_up_ready;
    let mut blocker_codes = BTreeSet::new();
    if !source_bundle.ready {
        blocker_codes.extend(source_bundle.blocker_codes.iter().cloned());
        blocker_codes.insert("initial_import_session_bundle_source_not_ready".to_string());
    }
    if !session.ready_for_graph_import {
        blocker_codes.insert("initial_import_session_bundle_graph_import_not_ready".to_string());
    }
    if !session.durable_state_present {
        blocker_codes.insert("initial_import_session_bundle_durable_state_missing".to_string());
    }
    if !session.durable_state_source_matches_manifest {
        blocker_codes.insert("initial_import_session_bundle_source_mismatch".to_string());
    }
    if session.next_action.kind == GraphLightningInitialImportResumeActionKind::Quarantine {
        blocker_codes.insert("initial_import_session_bundle_quarantine_required".to_string());
    }
    blocker_codes.extend(session.blocker_codes.iter().cloned());
    if catch_up_required {
        if !catch_up_present {
            blocker_codes
                .insert("initial_import_session_bundle_cutover_catch_up_missing".to_string());
        } else if !catch_up_ready {
            blocker_codes
                .insert("initial_import_session_bundle_cutover_catch_up_not_ready".to_string());
        }
    }
    if let Some(report) = catch_up
        && !report.ready
    {
        blocker_codes.extend(report.blocker_codes.iter().cloned());
    }
    GraphLightningInitialImportSessionBundleReadiness {
        ready: blocker_codes.is_empty(),
        resumable,
        ready_for_cutover,
        source_bundle_ready: source_bundle.ready,
        session_ready_for_graph_import: session.ready_for_graph_import,
        session_ready_for_cutover: session.ready_for_cutover,
        durable_state_present: session.durable_state_present,
        durable_state_source_matches_manifest: session.durable_state_source_matches_manifest,
        catch_up_required,
        catch_up_present,
        catch_up_ready,
        cutover_watermark,
        next_action: session.next_action.clone(),
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

pub fn graph_lightning_initial_import_startup_readiness(
    encoded_graph_stream: &str,
    manifest: &GraphLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_batches: &[SearchProjectionDelta],
    target_projection_freshness: Option<&SearchProjectionFreshness>,
    live_projection_freshness: Option<&SearchProjectionFreshness>,
    durable_state: Option<&GraphLightningInitialImportDurableState>,
) -> GraphLightningInitialImportStartupReadinessReport {
    let checkpoint = durable_state.map(|state| &state.checkpoint);
    let source_bundle = graph_lightning_initial_import_source_bundle_readiness(
        manifest,
        checkpoint,
        projection_batches,
    );
    let session = graph_lightning_initial_import_session_report(
        encoded_graph_stream,
        manifest,
        target_graph_commit_epoch,
        target_projection_freshness,
        durable_state,
    );
    let cutover_catch_up = session.ready_for_cutover.then(|| {
        graph_lightning_initial_import_cutover_catch_up_report(
            &session,
            target_graph_commit_epoch,
            live_projection_freshness,
        )
    });
    let readiness = graph_lightning_initial_import_session_bundle_readiness(
        &source_bundle,
        &session,
        cutover_catch_up.as_ref(),
    );
    GraphLightningInitialImportStartupReadinessReport {
        ready: readiness.ready,
        blocker_codes: readiness.blocker_codes.clone(),
        source_bundle,
        session,
        cutover_catch_up,
        readiness,
    }
}

pub fn graph_lightning_initial_import_recovery_readiness(
    encoded_graph_stream: &str,
    manifest: &GraphLightningBootstrapManifest,
    target_graph_commit_epoch: u64,
    projection_batches: &[SearchProjectionDelta],
    target_projection_freshness: Option<&SearchProjectionFreshness>,
    live_projection_freshness: Option<&SearchProjectionFreshness>,
    durable_state_payload: Option<&str>,
) -> GraphLightningInitialImportRecoveryReadinessReport {
    let durable_state_payload_present = durable_state_payload.is_some();
    let mut decode_blocker_codes = BTreeSet::new();
    let durable_state_codec = durable_state_payload.and_then(|payload| {
        match graph_lightning_initial_import_decode_durable_state(manifest, payload) {
            Ok(report) => {
                if !report.ready {
                    decode_blocker_codes.extend(report.blocker_codes.iter().cloned());
                }
                Some(report)
            }
            Err(_) => {
                decode_blocker_codes
                    .insert("initial_import_durable_state_codec_decode_failed".to_string());
                None
            }
        }
    });
    let durable_state = durable_state_codec
        .as_ref()
        .filter(|report| report.ready)
        .and_then(|report| report.state.as_ref());
    let startup = graph_lightning_initial_import_startup_readiness(
        encoded_graph_stream,
        manifest,
        target_graph_commit_epoch,
        projection_batches,
        target_projection_freshness,
        live_projection_freshness,
        durable_state,
    );
    let invalid_payload = durable_state_payload_present && durable_state.is_none();
    let next_action = if invalid_payload {
        GraphLightningInitialImportResumeAction {
            kind: GraphLightningInitialImportResumeActionKind::Quarantine,
            next_batch: None,
            idempotency_key: None,
            completed_batches: 0,
            total_batches: 0,
            blocker_codes: decode_blocker_codes.iter().cloned().collect(),
        }
    } else {
        startup.readiness.next_action.clone()
    };
    let mut blocker_codes = BTreeSet::new();
    blocker_codes.extend(startup.blocker_codes.iter().cloned());
    blocker_codes.extend(decode_blocker_codes);
    if invalid_payload {
        blocker_codes
            .insert("initial_import_recovery_durable_state_quarantine_required".to_string());
    }
    GraphLightningInitialImportRecoveryReadinessReport {
        ready: !invalid_payload && startup.ready,
        durable_state_payload_present,
        durable_state_codec,
        startup,
        next_action,
        blocker_codes: blocker_codes.into_iter().collect(),
    }
}

fn graph_lightning_initial_import_source_fingerprint_json(
    fingerprint: &GraphLightningInitialImportSourceFingerprint,
) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": fingerprint.protocol_version,
        "graph_commit_epoch": fingerprint.graph_commit_epoch,
        "logical_checksum": fingerprint.logical_checksum,
        "graph_stream_checksum": fingerprint.graph_stream_checksum,
        "graph_stream_byte_len": fingerprint.graph_stream_byte_len,
        "schema_checksum": fingerprint.schema_checksum,
        "node_count": fingerprint.node_count,
        "relationship_count": fingerprint.relationship_count,
    })
}

fn graph_lightning_initial_import_checkpoint_json(
    checkpoint: &GraphLightningInitialImportCheckpoint,
) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": checkpoint.protocol_version,
        "import_id": checkpoint.import_id,
        "task_id": checkpoint.task_id,
        "fencing_token": checkpoint.fencing_token,
        "object_digest": checkpoint.object_digest,
        "schema_checksum": checkpoint.schema_checksum,
        "graph_stream_checksum": checkpoint.graph_stream_checksum,
        "graph_stream_byte_len": checkpoint.graph_stream_byte_len,
        "manifest_graph_commit_epoch": checkpoint.manifest_graph_commit_epoch,
        "applied_graph_commit_epoch": checkpoint.applied_graph_commit_epoch,
        "applied_search_projection_commit_epoch": checkpoint.applied_search_projection_commit_epoch,
        "durable_search_projection_commit_epoch": checkpoint.durable_search_projection_commit_epoch,
        "completed_batches": checkpoint.completed_batches,
        "total_batches": checkpoint.total_batches,
        "document_identity_count": checkpoint.document_identity_count,
    })
}

fn graph_lightning_initial_import_document_identity_json(
    identity: &GraphLightningInitialImportDocumentIdentity,
) -> serde_json::Value {
    serde_json::json!({
        "kind": identity.kind.as_str(),
        "document_id": identity.document_id,
    })
}

fn graph_lightning_initial_import_document_identity_coverage_json(
    coverage: &GraphLightningInitialImportDocumentIdentityCoverage,
) -> serde_json::Value {
    serde_json::json!({
        "ready": coverage.ready,
        "document_identity_count": coverage.document_identity_count,
        "unique_document_identity_count": coverage.unique_document_identity_count,
        "expected_kinds": coverage
            .expected_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        "observed_kinds": coverage
            .observed_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        "kind_reports": coverage
            .kind_reports
            .iter()
            .map(|report| {
                serde_json::json!({
                    "kind": report.kind.as_str(),
                    "document_count": report.document_count,
                })
            })
            .collect::<Vec<_>>(),
        "missing_kinds": coverage
            .missing_kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        "empty_document_id_count": coverage.empty_document_id_count,
        "blocker_codes": coverage.blocker_codes,
    })
}

fn parse_graph_lightning_initial_import_source_fingerprint(
    value: &serde_json::Value,
) -> Result<GraphLightningInitialImportSourceFingerprint> {
    Ok(GraphLightningInitialImportSourceFingerprint {
        protocol_version: required_json_u64(value, "protocol_version")?,
        graph_commit_epoch: required_json_u64(value, "graph_commit_epoch")?,
        logical_checksum: required_json_u64(value, "logical_checksum")?,
        graph_stream_checksum: required_json_u64(value, "graph_stream_checksum")?,
        graph_stream_byte_len: required_json_usize(value, "graph_stream_byte_len")?,
        schema_checksum: required_json_u64(value, "schema_checksum")?,
        node_count: required_json_usize(value, "node_count")?,
        relationship_count: required_json_usize(value, "relationship_count")?,
    })
}

fn parse_graph_lightning_initial_import_checkpoint(
    value: &serde_json::Value,
) -> Result<GraphLightningInitialImportCheckpoint> {
    Ok(GraphLightningInitialImportCheckpoint {
        protocol_version: required_json_u64(value, "protocol_version")?,
        import_id: required_json_string(value, "import_id")?.to_string(),
        task_id: required_json_string(value, "task_id")?.to_string(),
        fencing_token: required_json_string(value, "fencing_token")?.to_string(),
        object_digest: required_json_string(value, "object_digest")?.to_string(),
        schema_checksum: required_json_u64(value, "schema_checksum")?,
        graph_stream_checksum: required_json_u64(value, "graph_stream_checksum")?,
        graph_stream_byte_len: required_json_usize(value, "graph_stream_byte_len")?,
        manifest_graph_commit_epoch: required_json_u64(value, "manifest_graph_commit_epoch")?,
        applied_graph_commit_epoch: required_json_u64(value, "applied_graph_commit_epoch")?,
        applied_search_projection_commit_epoch: optional_json_u64(
            value,
            "applied_search_projection_commit_epoch",
        )?,
        durable_search_projection_commit_epoch: optional_json_u64(
            value,
            "durable_search_projection_commit_epoch",
        )?,
        completed_batches: required_json_u64(value, "completed_batches")?,
        total_batches: required_json_u64(value, "total_batches")?,
        document_identity_count: required_json_usize(value, "document_identity_count")?,
    })
}

fn parse_graph_lightning_initial_import_document_identities(
    items: &[serde_json::Value],
) -> Result<Vec<GraphLightningInitialImportDocumentIdentity>> {
    items
        .iter()
        .map(|value| {
            Ok(GraphLightningInitialImportDocumentIdentity {
                kind: parse_search_projection_kind(required_json_string(value, "kind")?)?,
                document_id: required_json_string(value, "document_id")?.to_string(),
            })
        })
        .collect()
}

fn parse_search_projection_kind(raw: &str) -> Result<SearchProjectionKind> {
    match raw {
        "memory" => Ok(SearchProjectionKind::Memory),
        "message" => Ok(SearchProjectionKind::Message),
        "entity" => Ok(SearchProjectionKind::Entity),
        "source" => Ok(SearchProjectionKind::Source),
        "source_chunk" => Ok(SearchProjectionKind::SourceChunk),
        "community" => Ok(SearchProjectionKind::Community),
        _ => Err(SkeinError::Semantic(
            "initial import durable state field kind is invalid".to_string(),
        )),
    }
}

fn required_json_object<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<&'a serde_json::Value> {
    let item = value
        .get(field)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "object"))?;
    if item.is_object() {
        Ok(item)
    } else {
        Err(durable_state_codec_invalid_field(field, "object"))
    }
}

fn required_json_array<'a>(
    value: &'a serde_json::Value,
    field: &str,
) -> Result<&'a [serde_json::Value]> {
    value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "array"))
}

fn required_json_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "string"))
}

fn required_json_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| durable_state_codec_invalid_field(field, "u64"))
}

fn required_json_usize(value: &serde_json::Value, field: &str) -> Result<usize> {
    required_json_u64(value, field)?
        .try_into()
        .map_err(|_| durable_state_codec_invalid_field(field, "usize"))
}

fn optional_json_u64(value: &serde_json::Value, field: &str) -> Result<Option<u64>> {
    match value.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(item) => item
            .as_u64()
            .map(Some)
            .ok_or_else(|| durable_state_codec_invalid_field(field, "optional_u64")),
    }
}

fn durable_state_codec_invalid_field(field: &str, expected: &str) -> SkeinError {
    SkeinError::Semantic(format!(
        "initial import durable state field {field} is invalid: expected_{expected}"
    ))
}

pub fn graph_lightning_initial_import_source_fingerprint(
    manifest: &GraphLightningBootstrapManifest,
) -> GraphLightningInitialImportSourceFingerprint {
    GraphLightningInitialImportSourceFingerprint {
        protocol_version: manifest.protocol_version,
        graph_commit_epoch: manifest.graph_commit_epoch,
        logical_checksum: manifest.logical_checksum,
        graph_stream_checksum: manifest.graph_stream_checksum,
        graph_stream_byte_len: manifest.graph_stream_byte_len,
        schema_checksum: manifest.schema_checksum,
        node_count: manifest.node_count,
        relationship_count: manifest.relationship_count,
    }
}

fn merge_initial_import_document_identities(
    existing: &[GraphLightningInitialImportDocumentIdentity],
    incoming: &[GraphLightningInitialImportDocumentIdentity],
) -> Vec<GraphLightningInitialImportDocumentIdentity> {
    let mut identities = Vec::with_capacity(existing.len().saturating_add(incoming.len()));
    let mut seen = BTreeSet::new();
    for identity in existing.iter().chain(incoming.iter()) {
        if seen.insert((identity.kind, identity.document_id.clone())) {
            identities.push(identity.clone());
        }
    }
    identities
}

fn search_projection_delta_document_identities(
    delta: &SearchProjectionDelta,
) -> Vec<GraphLightningInitialImportDocumentIdentity> {
    delta
        .upserts
        .iter()
        .map(|row| GraphLightningInitialImportDocumentIdentity {
            kind: row.kind,
            document_id: format!("{}:{}", row.kind.as_str(), row.external_id),
        })
        .collect()
}

pub fn graph_lightning_initial_import_document_identity_coverage(
    identities: &[GraphLightningInitialImportDocumentIdentity],
) -> GraphLightningInitialImportDocumentIdentityCoverage {
    let expected_kinds = graph_lightning_initial_import_required_search_projection_kinds();
    let mut document_ids = BTreeMap::<String, usize>::new();
    let mut kind_counts = BTreeMap::<SearchProjectionKind, usize>::new();
    let mut empty_document_id_count = 0;
    for identity in identities {
        if identity.document_id.is_empty() {
            empty_document_id_count += 1;
        } else {
            *document_ids
                .entry(identity.document_id.clone())
                .or_default() += 1;
        }
        *kind_counts.entry(identity.kind).or_default() += 1;
    }
    let observed_kinds = kind_counts.keys().copied().collect::<Vec<_>>();
    let kind_reports = kind_counts
        .iter()
        .map(
            |(kind, document_count)| GraphLightningInitialImportDocumentIdentityKindReport {
                kind: *kind,
                document_count: *document_count,
            },
        )
        .collect::<Vec<_>>();
    let missing_kinds = expected_kinds
        .iter()
        .copied()
        .filter(|kind| !kind_counts.contains_key(kind))
        .collect::<Vec<_>>();
    let duplicate_document_ids = document_ids
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(document_id, _)| document_id.clone())
        .collect::<Vec<_>>();
    let mut blocker_codes = BTreeSet::new();
    if !missing_kinds.is_empty() {
        blocker_codes.insert("initial_import_document_identity_kind_missing".to_string());
    }
    if empty_document_id_count > 0 {
        blocker_codes.insert("initial_import_document_identity_empty".to_string());
    }
    if !duplicate_document_ids.is_empty() {
        blocker_codes.insert("initial_import_document_identity_duplicate".to_string());
    }
    let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
    GraphLightningInitialImportDocumentIdentityCoverage {
        ready: blocker_codes.is_empty(),
        document_identity_count: identities.len(),
        unique_document_identity_count: document_ids.len(),
        expected_kinds,
        observed_kinds,
        kind_reports,
        missing_kinds,
        duplicate_document_ids,
        empty_document_id_count,
        blocker_codes,
    }
}

pub fn graph_lightning_initial_import_resume_action(
    manifest: &GraphLightningBootstrapManifest,
    checkpoint: Option<&GraphLightningInitialImportCheckpoint>,
) -> GraphLightningInitialImportResumeAction {
    let Some(checkpoint) = checkpoint else {
        return GraphLightningInitialImportResumeAction {
            kind: GraphLightningInitialImportResumeActionKind::Start,
            next_batch: Some(0),
            idempotency_key: None,
            completed_batches: 0,
            total_batches: 0,
            blocker_codes: Vec::new(),
        };
    };
    let readiness = graph_lightning_initial_import_checkpoint_readiness(manifest, checkpoint);
    let hard_mismatch = readiness.blocker_codes.iter().any(|code| {
        code == "initial_import_checkpoint_manifest_mismatch"
            || code == "initial_import_checkpoint_idempotency_key_missing"
    });
    let kind = if hard_mismatch {
        GraphLightningInitialImportResumeActionKind::Quarantine
    } else if readiness.ready {
        GraphLightningInitialImportResumeActionKind::ReadyForCutover
    } else {
        GraphLightningInitialImportResumeActionKind::Resume
    };
    let next_batch = match kind {
        GraphLightningInitialImportResumeActionKind::Start => Some(0),
        GraphLightningInitialImportResumeActionKind::Resume => {
            Some(checkpoint.completed_batches.min(checkpoint.total_batches))
        }
        GraphLightningInitialImportResumeActionKind::ReadyForCutover
        | GraphLightningInitialImportResumeActionKind::Quarantine => None,
    };
    GraphLightningInitialImportResumeAction {
        kind,
        next_batch,
        idempotency_key: readiness.idempotency_key,
        completed_batches: checkpoint.completed_batches,
        total_batches: checkpoint.total_batches,
        blocker_codes: readiness.blocker_codes,
    }
}

fn graph_lightning_initial_import_idempotency_key(
    checkpoint: &GraphLightningInitialImportCheckpoint,
) -> Option<GraphLightningInitialImportIdempotencyKey> {
    if checkpoint.import_id.is_empty()
        || checkpoint.task_id.is_empty()
        || checkpoint.fencing_token.is_empty()
        || checkpoint.object_digest.is_empty()
    {
        return None;
    }
    Some(GraphLightningInitialImportIdempotencyKey {
        import_id: checkpoint.import_id.clone(),
        task_id: checkpoint.task_id.clone(),
        fencing_token: checkpoint.fencing_token.clone(),
        object_digest: checkpoint.object_digest.clone(),
    })
}

fn optional_epoch_regressed(previous: Option<u64>, next: Option<u64>) -> bool {
    match (previous, next) {
        (Some(_), None) => true,
        (Some(previous), Some(next)) => next < previous,
        (None, _) => false,
    }
}

fn graph_lightning_initial_import_required_search_projection_kinds() -> Vec<SearchProjectionKind> {
    vec![
        SearchProjectionKind::Memory,
        SearchProjectionKind::Message,
        SearchProjectionKind::Entity,
        SearchProjectionKind::Source,
        SearchProjectionKind::SourceChunk,
        SearchProjectionKind::Community,
    ]
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
    nodes: Vec<CanonicalSnapshotNode>,
    snapshot_relationships: Vec<CanonicalSnapshotRelationship>,
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
        let node_id = cursor.read_node_id(errors);
        if let Some(node_id) = node_id {
            parsed.node_ids.push(node_id);
        }
        let stable_id = cursor.read_optional_canonical_value("stable_id", errors);
        let label_count = cursor
            .read_tagged_u64("label_count", "graph stream label count", errors)
            .unwrap_or(0);
        let mut labels = Vec::new();
        for _ in 0..label_count {
            if let Some(label) = cursor.read_canonical_string_line("label", errors) {
                labels.push(label);
            }
        }
        let properties = read_graph_stream_properties(&mut cursor, errors);
        if let Some(node_id) = node_id {
            parsed.nodes.push(CanonicalSnapshotNode {
                node_id,
                stable_id,
                labels,
                properties,
            });
        }
    }

    parsed.declared_relationship_count = cursor.read_tagged_u64(
        "relationship_count",
        "graph stream relationship count",
        errors,
    );
    let relationship_count = parsed.declared_relationship_count.unwrap_or(0);
    for _ in 0..relationship_count {
        let relationship = cursor.read_relationship(errors);
        if let Some((relationship_id, source, target)) = relationship {
            parsed.relationship_ids.push(relationship_id);
            parsed.relationships.push((relationship_id, source, target));
        }
        let stable_id = cursor.read_optional_canonical_value("stable_id", errors);
        let rel_type = cursor
            .read_canonical_string_line("relationship_type", errors)
            .unwrap_or_default();
        let properties = read_graph_stream_properties(&mut cursor, errors);
        if let Some((relationship_id, source, target)) = relationship {
            parsed
                .snapshot_relationships
                .push(CanonicalSnapshotRelationship {
                    relationship_id,
                    stable_id,
                    source_node_id: source,
                    target_node_id: target,
                    rel_type,
                    properties,
                });
        }
    }

    if !cursor.is_finished() {
        let remaining = cursor.remaining_preview();
        errors.push(format!("trailing graph stream data: {remaining}"));
    }

    parsed.relationships = parsed
        .snapshot_relationships
        .iter()
        .map(|relationship| {
            (
                relationship.relationship_id,
                relationship.source_node_id,
                relationship.target_node_id,
            )
        })
        .collect();
    parsed
}

fn read_graph_stream_properties(
    cursor: &mut GraphStreamCursor<'_>,
    errors: &mut Vec<String>,
) -> BTreeMap<String, Value> {
    let property_count = cursor
        .read_tagged_u64("property_count", "graph stream property count", errors)
        .unwrap_or(0);
    let mut properties = BTreeMap::new();
    for _ in 0..property_count {
        let property = cursor.read_canonical_string_line("property", errors);
        let value = cursor.read_canonical_value(errors);
        cursor.expect_byte(b'\n', "graph stream property value terminator", errors);
        if let (Some(property), Some(value)) = (property, value) {
            properties.insert(property, value);
        }
    }
    properties
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

    fn read_optional_canonical_value(
        &mut self,
        prefix: &str,
        errors: &mut Vec<String>,
    ) -> Option<Value> {
        if !self.expect_str(prefix, errors) {
            return None;
        }
        if !self.expect_byte(b'\t', "graph stream optional value separator", errors) {
            return None;
        }
        let value = if self.remaining().starts_with("missing") {
            self.offset += "missing".len();
            None
        } else {
            self.read_canonical_value(errors)
        };
        self.expect_byte(b'\n', "graph stream optional value terminator", errors);
        value
    }

    fn read_canonical_string_line(
        &mut self,
        prefix: &str,
        errors: &mut Vec<String>,
    ) -> Option<String> {
        if !self.expect_str(prefix, errors) {
            return None;
        }
        if !self.expect_byte(b'\t', "graph stream canonical string separator", errors) {
            return None;
        }
        let value = self.read_length_prefixed_string("graph stream canonical string", errors);
        self.expect_byte(b'\n', "graph stream canonical string terminator", errors);
        value
    }

    fn read_canonical_value(&mut self, errors: &mut Vec<String>) -> Option<Value> {
        if self.remaining().starts_with("null") {
            self.offset += "null".len();
            Some(Value::Null)
        } else if self.remaining().starts_with("bool:true") {
            self.offset += "bool:true".len();
            Some(Value::Bool(true))
        } else if self.remaining().starts_with("bool:false") {
            self.offset += "bool:false".len();
            Some(Value::Bool(false))
        } else if self.remaining().starts_with("int:") {
            self.offset += "int:".len();
            self.read_scalar_token()
                .and_then(|raw| match raw.parse::<i64>() {
                    Ok(value) => Some(Value::Int(value)),
                    Err(_) => {
                        errors.push(format!("invalid graph stream int value: {raw}"));
                        None
                    }
                })
        } else if self.remaining().starts_with("float:") {
            self.offset += "float:".len();
            self.read_scalar_token()
                .and_then(|raw| match u64::from_str_radix(raw, 16) {
                    Ok(bits) => Some(Value::Float(f64::from_bits(bits))),
                    Err(_) => {
                        errors.push(format!("invalid graph stream float value: {raw}"));
                        None
                    }
                })
        } else if self.remaining().starts_with("string:") {
            self.offset += "string:".len();
            self.read_length_prefixed_string("graph stream string value", errors)
                .map(Value::String)
        } else if self.remaining().starts_with("list:") {
            self.offset += "list:".len();
            let count = self.parse_decimal("graph stream list item count", errors);
            if !self.expect_byte(b':', "graph stream list count separator", errors)
                || !self.expect_byte(b'[', "graph stream list opener", errors)
            {
                return None;
            }
            let mut values = Vec::new();
            for _ in 0..count.unwrap_or(0) {
                if let Some(value) = self.read_canonical_value(errors) {
                    values.push(value);
                }
                self.expect_byte(b';', "graph stream list item terminator", errors);
            }
            self.expect_byte(b']', "graph stream list closer", errors);
            Some(Value::List(values))
        } else if self.remaining().starts_with("map:") {
            self.offset += "map:".len();
            let count = self.parse_decimal("graph stream map item count", errors);
            if !self.expect_byte(b':', "graph stream map count separator", errors)
                || !self.expect_byte(b'{', "graph stream map opener", errors)
            {
                return None;
            }
            let mut values = BTreeMap::new();
            for _ in 0..count.unwrap_or(0) {
                let key = self.read_length_prefixed_string("graph stream map key", errors);
                if !self.expect_byte(b'=', "graph stream map key separator", errors) {
                    return None;
                }
                let value = self.read_canonical_value(errors);
                self.expect_byte(b';', "graph stream map item terminator", errors);
                if let (Some(key), Some(value)) = (key, value) {
                    values.insert(key, value);
                }
            }
            self.expect_byte(b'}', "graph stream map closer", errors);
            Some(Value::Map(values))
        } else {
            errors.push(format!(
                "invalid graph stream canonical value: {}",
                self.remaining_preview()
            ));
            None
        }
    }

    fn read_length_prefixed_string(
        &mut self,
        name: &str,
        errors: &mut Vec<String>,
    ) -> Option<String> {
        let len = self.parse_decimal(name, errors)?;
        if !self.expect_byte(b':', "graph stream length separator", errors) {
            return None;
        }
        let end = self.offset.saturating_add(len as usize);
        if end > self.input.len() {
            errors.push(format!("{name} exceeds graph stream length"));
            self.offset = self.input.len();
            return None;
        }
        if !self.input.is_char_boundary(end) {
            errors.push(format!("{name} ends inside a UTF-8 codepoint"));
            self.offset = self.input.len();
            return None;
        }
        let value = self.input[self.offset..end].to_string();
        self.offset = end;
        Some(value)
    }

    fn read_scalar_token(&mut self) -> Option<&'a str> {
        let start = self.offset;
        while let Some(byte) = self.current_byte() {
            if matches!(byte, b';' | b'\n' | b']' | b'}') {
                break;
            }
            self.offset += 1;
        }
        (start != self.offset).then_some(&self.input[start..self.offset])
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
