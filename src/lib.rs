pub mod analytics;
pub mod api;
pub mod compat;
pub mod cypher;
pub mod executor;
pub mod nowledge_inventory;
pub mod optimizer;
pub mod planner;
pub mod qos;
pub mod search;
pub mod store;

mod regex_cache;

pub mod error {
    pub use skein_core::error::*;
}

pub mod schema {
    pub use skein_core::schema::*;
}

pub mod value {
    pub use skein_core::value::*;
}

pub use analytics::{
    CommunityAssignment, LouvainOptions, PageRankOptions, PageRankScore, ProjectedGraph,
};
pub use api::{
    validate_graph_lightning_graph_stream, BackgroundMaintenanceCandidate,
    BackgroundMaintenanceKind, BackgroundMaintenanceOptions, CanonicalGraphSnapshotExport,
    CanonicalGraphSnapshotValidation, CanonicalSnapshotEndpointViolation,
    CanonicalSnapshotIdentityAudit, CanonicalSnapshotNode, CanonicalSnapshotRelationship,
    CanonicalStableIdMapping, Database, DatabaseConfig, DatabaseReadTransaction,
    DatabaseTransaction, DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest, GraphLightningBootstrapExport,
    GraphLightningBootstrapManifest, GraphLightningGraphStream,
    GraphLightningGraphStreamValidation, KnowledgeCandidate, KnowledgeCandidateScoreBreakdown,
    KnowledgeCandidateScoringPolicy, KnowledgeCandidateSource, KnowledgeEntity,
    KnowledgeEntityOutput, KnowledgeEntityRequest, KnowledgeEvidence, KnowledgeFallbackReasonCode,
    KnowledgeFanoutReasonCode, KnowledgeFanoutReasonDetail, KnowledgeGraphContextPath,
    KnowledgeGraphPath, KnowledgeGraphPathDirection, KnowledgeGraphSeed,
    KnowledgeNeighborDirection, KnowledgeNeighborsOutput, KnowledgeNeighborsRequest,
    KnowledgePathOutput, KnowledgePathRequest, KnowledgeRetrievalDiagnostics,
    KnowledgeRetrievalEmptyReasonCode, KnowledgeRetrievalOutput, KnowledgeRetrievalRequest,
    KnowledgeRetrieverCandidate, KnowledgeRetrieverReport, KnowledgeScopedNeighborsRequest,
    KnowledgeScopedSubgraphRequest, KnowledgeSubgraphOutput, KnowledgeSubgraphRequest,
    KnowledgeTraversalDiagnostics, KnowledgeTraversalFallbackReasonCode,
    KnowledgeTruncationReasonCode, NowledgeGraphAdapter, NowledgeGraphExplainOutput,
    NowledgeGraphStatement, NowledgeGraphTransactionOutput, PlanCacheStats, QueryOutput,
    RankedBackgroundMaintenance, SearchProjectionGraphDeltaRequest,
    GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION, GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
};
pub use compat::{
    assess_compatibility_cutover, assess_compatibility_cypher_migration_gate_bundle,
    assess_compatibility_cypher_migration_gate_bundle_with_rollback,
    assess_compatibility_migration_gate, assess_compatibility_migration_gate_bundle,
    assess_compatibility_migration_gate_with_rollback, assess_query_inventory_coverage,
    assess_query_inventory_cypher_coverage, assess_query_inventory_gate,
    build_compatibility_query_inventory, build_compatibility_query_inventory_from_json,
    build_compatibility_query_inventory_from_json_str, compatibility_cutover_report_to_json,
    compatibility_inventory_coverage_report_to_json, compatibility_inventory_gate_report_to_json,
    compatibility_migration_gate_bundle_to_json, compatibility_migration_gate_report_to_json,
    compatibility_query_inventory_to_json, external_shadow_json_from_value,
    external_shadow_ready_missing_capabilities, external_shadow_trace_health_from_bundle,
    external_shadow_trace_report_json, external_shadow_value_from_json,
    nowledge_memory_core_fixture, nowledge_memory_core_inventory, run_compatibility_fixture,
    run_compatibility_fixture_with_shadow, CompatibilityCheck, CompatibilityCheckReport,
    CompatibilityCutoverDecision, CompatibilityCutoverPolicy, CompatibilityCutoverReport,
    CompatibilityFixture, CompatibilityInventoryCoveragePolicy,
    CompatibilityInventoryCoverageReport, CompatibilityInventoryGateReport,
    CompatibilityMigrationGateBundle, CompatibilityMigrationGateReport, CompatibilityQueryCallSite,
    CompatibilityQueryInventory, CompatibilityQueryInventoryItem, CompatibilityReport,
    CompatibilityRollbackEvidence, CompatibilityShadowCheckReport, CompatibilityShadowEngine,
    CompatibilityShadowReport, CompatibilityShadowStatus, CypherFixtureCheck,
    CypherFixtureStatement, ExpectedErrorClass, ExpectedRows, ExternalShadowCommand,
    ExternalShadowProjectGraphReply, ExternalShadowProjectGraphRequest,
    ExternalShadowProtocolBackend, ExternalShadowProtocolServer, ExternalShadowReady,
    ExternalShadowStatementRequest, ExternalShadowTraceHealth, ExternalShadowTraceSummary,
    ProjectedGraphFixtureCheck, ProjectedGraphShadowOutput, EXTERNAL_SHADOW_PROTOCOL_VERSION,
    REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
};
pub use cypher::RelationshipDirection;
pub use error::{Result, SkeinError};
pub use nowledge_inventory::{
    background_maintenance_evidence_health, background_maintenance_evidence_health_from_bundle,
    replacement_readiness_family_evidence_health,
    replacement_readiness_family_evidence_health_from_bundle, scan_nowledge_query_inventory,
    scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
    scan_nowledge_query_inventory_cypher_coverage_to_json,
    scan_nowledge_query_inventory_cypher_migration_gate_to_json,
    scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json,
    scan_nowledge_query_inventory_to_json, scan_nowledge_query_inventory_with_options,
    storage_recovery_evidence_health, storage_recovery_evidence_health_from_bundle,
    BackgroundMaintenanceEvidenceHealth, NowledgeCypherMigrationGateJsonOptions,
    NowledgeInventoryScanOptions, ReplacementReadinessFamilyEvidenceHealth,
    StorageRecoveryEvidenceHealth,
};
pub use qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, BackgroundWorkReasonCode,
    LocalQosPermit, LocalQosPolicy, LocalQosScheduler, LocalQosState, QosAdmission,
    QosAdmissionCode, RankedBackgroundWork, WorkClass, WorkPriority, WorkRequest, WORK_CLASS_COUNT,
};
pub use schema::{
    CompositeIndexDescriptor, ConstraintDescriptor, ConstraintId, ConstraintKind,
    ConstraintSubject, GraphStatistics, IndexDescriptor, IndexId, IndexKind, PropertyDescriptor,
    PropertyId, PropertyType, SchemaObjectState, TableDescriptor, TableId, TableKind,
};
pub use search::{
    MetadataRepairOptions, MetadataRepairSummary, SearchAnalyzerLexicon,
    SearchDerivedArtifactReport, SearchDocument, SearchEmbeddingManifest, SearchEmptyReasonCode,
    SearchFallbackReasonCode, SearchHit, SearchIndex, SearchMode, SearchProjectionDelta,
    SearchProjectionDeltaReport, SearchProjectionFreshness, SearchProjectionKind,
    SearchProjectionRow, SearchRebuildOptions, SearchRebuildSummary, SearchResultSet,
    SearchRetrieverCandidateSetReport, SearchTruncationReasonCode,
};
pub use skein_optimizer::{
    Distribution, GroupId, Memo as OptimizerMemo, MemoGroup as OptimizerMemoGroup,
    PhysicalProperties, RequiredProperties,
};
pub use store::{
    AdjacencyDirection, AdjacencyGroupStats, AdjacencyLayout, DurabilityPolicy,
    OrderedAdjacencyEntry, RecoveryMode, StorageReclamationWatermark, StorageRecoveryReport,
    WalReplayConfig, DENSE_ADJACENCY_DEGREE_THRESHOLD,
};
pub use value::Value;

#[cfg(test)]
mod tests {
    use super::{
        Database, KnowledgeEntityRequest, KnowledgeNeighborDirection, KnowledgeNeighborsRequest,
        KnowledgePathRequest, KnowledgeSubgraphRequest,
    };

    #[test]
    fn crate_root_exports_typed_knowledge_navigation_api() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
        )
        .unwrap();

        let entity = db.knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
        });
        assert!(entity.entity.is_some());

        let neighbors = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: 4,
            max_hops: 1,
        });
        assert_eq!(neighbors.diagnostics.path_count, 1);

        let paths = db.knowledge_paths(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "leaf".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            limit: 4,
        });
        assert_eq!(paths.diagnostics.target_found, Some(true));

        let subgraph = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 4,
            relationship_limit: 4,
        });
        assert_eq!(subgraph.diagnostics.node_count, 2);
    }
}
