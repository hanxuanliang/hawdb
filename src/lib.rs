pub mod analytics;
pub mod api;
pub mod compat;
pub mod cypher;
pub mod executor;
pub mod nowledge_inventory;
pub mod nowledge_mem;
pub mod optimizer;
pub mod planner;
pub mod qos;
pub mod search;
pub mod store;

mod regex_cache;
#[path = "cli_search_projection_evidence.rs"]
pub mod search_projection_evidence;

pub mod error {
    pub use skein_core::error::*;
}

pub mod schema {
    pub use skein_core::schema::*;
}

pub mod value {
    pub use skein_core::value::*;
}

pub mod sql {
    pub use skein_sql::*;
}

pub use analytics::{
    CommunityAssignment, LouvainOptions, PageRankOptions, PageRankScore, ProjectedGraph,
};
pub use api::{
    validate_graph_lightning_graph_stream, BackgroundMaintenanceCandidate,
    BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundMaintenanceSummary,
    BackgroundMaintenanceSummaryItem, BoundedReadQueryOutput, CanonicalGraphSnapshotExport,
    CanonicalGraphSnapshotValidation, CanonicalSnapshotEndpointViolation,
    CanonicalSnapshotIdentityAudit, CanonicalSnapshotNode, CanonicalSnapshotRelationship,
    CanonicalStableIdMapping, Database, DatabaseConfig, DatabaseReadTransaction,
    DatabaseTransaction, DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExplainAnalyzeOutput, ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest, GraphLightningBootstrapExport,
    GraphLightningBootstrapManifest, GraphLightningGraphStream,
    GraphLightningGraphStreamValidation, KnowledgeCandidate, KnowledgeCandidateScoreBreakdown,
    KnowledgeCandidateScoringPolicy, KnowledgeCandidateSource, KnowledgeEvidence,
    KnowledgeFallbackReasonCode, KnowledgeFanoutReasonCode, KnowledgeFanoutReasonDetail,
    KnowledgeGraphContextPath, KnowledgeGraphPath, KnowledgeGraphPathDirection, KnowledgeGraphSeed,
    KnowledgeRetrievalDiagnostics, KnowledgeRetrievalEmptyReasonCode, KnowledgeRetrievalOutput,
    KnowledgeRetrievalRequest, KnowledgeRetrieverCandidate, KnowledgeRetrieverReport,
    KnowledgeTraversalDiagnostics, KnowledgeTraversalFallbackReasonCode,
    KnowledgeTruncationReasonCode, NowledgeGraphAdapter, NowledgeGraphExplainOutput,
    NowledgeGraphStatement, NowledgeGraphTransactionOutput, PlanCacheBypassReason, PlanCacheLookup,
    PlanCacheStats, QueryOutput, QuerySystemVariables, RankedBackgroundMaintenance,
    SearchProjectionGraphDeltaRequest, GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
    GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
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
pub use executor::ReadExecutionProfile;
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
    StorageRecoveryEvidenceHealth, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
pub use nowledge_mem::{
    nowledge_mem_bounded_read_evidence_json, nowledge_mem_bounded_read_evidence_json_with_routes,
    nowledge_mem_graph_config, nowledge_mem_graph_config_with_search_mode,
    nowledge_mem_search_candidate_shadow_evidence_json, NowledgeMemEmbeddedStore, NowledgeMemGraph,
    NowledgeMemGraphMode, NowledgeMemOpenOptions, NowledgeMemOpenReport,
    NowledgeMemQueryExecutionPath, NowledgeMemQueryOutput, NowledgeMemQueryReport,
    NowledgeMemQueryReportOptions, NowledgeMemReadOptions, NowledgeMemReadOutput,
    NowledgeMemReadReport, NowledgeMemReadinessOptions, NowledgeMemRetrievalOutput,
    NowledgeMemRetrievalReport, NowledgeMemSearchCandidateShadowAccumulator,
    NowledgeMemSearchCandidateShadowEvidence, NowledgeMemSearchProjection,
    NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
    NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
    NOWLEDGE_MEM_READ_REPORT_PROTOCOL, NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
pub use qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, BackgroundWorkReasonCode,
    LocalQosPermit, LocalQosPolicy, LocalQosScheduler, LocalQosState, QosAdmission,
    QosAdmissionCode, RankedBackgroundWork, WorkClass, WorkPriority, WorkRequest, WORK_CLASS_COUNT,
};
pub use schema::{
    BasicGraphStatistics, CompositeIndexDescriptor, ConstraintDescriptor, ConstraintId,
    ConstraintKind, ConstraintSubject, GraphStatistics, IndexDescriptor, IndexId, IndexKind,
    PropertyDescriptor, PropertyId, PropertyType, SchemaObjectState, TableDescriptor, TableId,
    TableKind,
};
pub use search::{
    CompressedVectorSearchMode, MetadataRepairOptions, MetadataRepairSummary,
    SearchAnalyzerLexicon, SearchDerivedArtifactReport, SearchDocument, SearchEmbeddingManifest,
    SearchEmptyReasonCode, SearchFallbackReasonCode, SearchHit, SearchIndex, SearchMode,
    SearchProjectionDelta, SearchProjectionDeltaReport, SearchProjectionFreshness,
    SearchProjectionKind, SearchProjectionProbeOptions, SearchProjectionRow, SearchRebuildOptions,
    SearchRebuildSummary, SearchResultSet, SearchRetrieverCandidateSetReport,
    SearchTruncationReasonCode, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
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
    use super::{Database, NowledgeGraphAdapter, NowledgeGraphStatement, Value};
    use std::collections::BTreeMap;

    #[test]
    fn crate_root_exports_query_runtime_front_door() {
        let mut db = Database::new();
        let mut adapter = NowledgeGraphAdapter::new(&mut db);
        let create = NowledgeGraphStatement {
            cypher: "CREATE (:Memory {id: $id, title: $title})".to_string(),
            parameters: BTreeMap::from([
                ("id".to_string(), Value::String("root".to_string())),
                ("title".to_string(), Value::String("Root".to_string())),
            ]),
        };
        adapter.query(&create).unwrap();

        let read = NowledgeGraphStatement {
            cypher: "MATCH (m:Memory {id: $id}) RETURN m.title AS title".to_string(),
            parameters: BTreeMap::from([("id".to_string(), Value::String("root".to_string()))]),
        };
        let output = adapter.query(&read).unwrap();

        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Root".to_string()))
        );
    }
}
