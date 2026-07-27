pub mod analytics;
pub mod api;
pub mod background_maintenance_evidence;
pub mod blackbox;
pub mod bounded_read_evidence;
pub mod compat;
pub mod cypher;
pub mod embedded;
pub mod executor;
pub mod graph_route_evidence;
pub mod graph_route_readiness;
pub mod mem_integration_bundle;
pub mod mem_integration_readiness;
pub mod mem_library_readiness;
pub mod nowledge_fuzz;
pub mod nowledge_inventory;
pub mod nowledge_mem;
pub mod optimizer;
pub mod planner;
pub mod previous_wrapper_preflight;
pub mod qos;
pub mod query_family_evidence;
pub mod query_runtime_preflight;
pub mod replacement_summary;
pub mod search;
pub mod search_candidate_shadow_evidence;
pub mod storage_recovery_evidence;
pub mod store;

mod regex_cache;
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
    SearchProjectionGraphDeltaRequest, SlowQueryLogExportOptions, SlowQueryLogRecordSummary,
    GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION, GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
    SLOW_QUERY_LOG_EVENT_PROTOCOL,
};
pub use background_maintenance_evidence::{
    nowledge_background_maintenance_evidence_json, nowledge_background_maintenance_evidence_usage,
    run_nowledge_background_maintenance_evidence,
};
pub use blackbox::{
    blackbox_readiness_from_manifest_json, blackbox_report, blackbox_report_json,
    write_blackbox_report, write_blackbox_report_typed, BlackboxArtifactReport,
    BlackboxBackgroundQosSummary, BlackboxEventReport, BlackboxJsonArtifactSummary,
    BlackboxJsonlArtifactSummary, BlackboxReadinessReport, BlackboxRedactionReport, BlackboxReport,
    BlackboxReportOptions, BlackboxRunStatus, BLACKBOX_EVENT_PROTOCOL, BLACKBOX_REPORT_PROTOCOL,
};
pub use bounded_read_evidence::{
    nowledge_bounded_read_evidence_usage, parse_covered_routes_json,
    parse_graph_route_readiness_json, parse_read_report_json, run_nowledge_bounded_read_evidence,
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
pub use embedded::{SkeinEmbedded, SkeinEmbeddedOpenOptions};
pub use error::{Result, SkeinError};
pub use executor::ReadExecutionProfile;
pub use graph_route_evidence::{
    nowledge_graph_route_evidence_json, nowledge_graph_route_evidence_usage,
    parse_route_parity_evidence, parse_route_query_inventory, run_nowledge_graph_route_evidence,
    RouteCypherQuery, RouteParityEvidence, RouteParityEvidenceRoute, RouteQuery,
    NMEM_GRAPH_ROUTE_PARITY_EVIDENCE_PROTOCOL,
};
pub use graph_route_readiness::{
    nowledge_graph_route_readiness_json, nowledge_graph_route_readiness_usage,
    run_nowledge_graph_route_readiness, NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
    NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
};
pub use mem_integration_bundle::{
    nowledge_mem_integration_bundle_json, nowledge_mem_integration_bundle_usage,
    run_nowledge_mem_integration_bundle, IntegrationBundleInputs,
};
pub use mem_integration_readiness::{
    background_maintenance_cutover_readiness, bounded_read_cutover_readiness,
    graph_route_cutover_readiness, library_readiness_cutover_readiness,
    nowledge_mem_integration_readiness, nowledge_mem_integration_readiness_json,
    nowledge_mem_integration_readiness_usage, run_nowledge_mem_integration_readiness,
    search_candidate_cutover_readiness, search_projection_cutover_readiness,
    storage_recovery_cutover_readiness, BackgroundMaintenanceCutoverReadiness,
    BoundedReadCutoverReadiness, GraphRouteCutoverReadiness, LibraryReadinessCutoverReadiness,
    NowledgeMemIntegrationCheckReport, NowledgeMemIntegrationNextAction,
    NowledgeMemIntegrationReadinessReport, SearchCandidateCutoverReadiness,
    SearchProjectionCutoverReadiness, StorageRecoveryCutoverReadiness,
    NOWLEDGE_MEM_INTEGRATION_READINESS_PROTOCOL, NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL,
};
pub use mem_library_readiness::{
    nowledge_mem_library_readiness_usage, parse_bounded_probe_json,
    parse_mem_library_covered_routes_json, parse_mem_library_graph_route_readiness_json,
    parse_mem_library_readiness_mode, parse_parameters_json, run_nowledge_mem_library_readiness,
    value_from_json,
};
pub use nowledge_fuzz::{
    nowledge_query_fuzz_harness, NowledgeQueryFuzzCaseReport, NowledgeQueryFuzzHarnessOptions,
    NowledgeQueryFuzzHarnessReport, NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL,
};
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
    nowledge_mem_bounded_read_evidence_json,
    nowledge_mem_bounded_read_evidence_json_with_route_readiness,
    nowledge_mem_bounded_read_evidence_json_with_routes, nowledge_mem_graph_config,
    nowledge_mem_graph_config_with_search_mode, nowledge_mem_graph_read_route_catalog_digest,
    nowledge_mem_graph_read_route_spec, nowledge_mem_graph_read_route_spec_json,
    nowledge_mem_graph_read_route_specs_json, nowledge_mem_required_query_families_for_route,
    nowledge_mem_search_candidate_shadow_evidence_json, NowledgeMemBackgroundMaintenanceReport,
    NowledgeMemEmbeddedStore, NowledgeMemEmbeddedStoreHandle, NowledgeMemGraph,
    NowledgeMemGraphMode, NowledgeMemGraphReadRouteEvidenceKind, NowledgeMemGraphReadRouteOwner,
    NowledgeMemGraphReadRouteSpec, NowledgeMemLibraryReadinessReport, NowledgeMemOpenOptions,
    NowledgeMemOpenReport, NowledgeMemQueryExecutionPath, NowledgeMemQueryOutput,
    NowledgeMemQueryReport, NowledgeMemQueryReportOptions, NowledgeMemReadOptions,
    NowledgeMemReadOutput, NowledgeMemReadReport, NowledgeMemReadinessAreaSummary,
    NowledgeMemReadinessDashboard, NowledgeMemReadinessOptions, NowledgeMemRetrievalOutput,
    NowledgeMemRetrievalReport, NowledgeMemRouteReadinessSummary,
    NowledgeMemSearchCandidateFieldSummary, NowledgeMemSearchCandidateFilterPushdownEvidence,
    NowledgeMemSearchCandidateOutput, NowledgeMemSearchCandidateReadinessOptions,
    NowledgeMemSearchCandidateReadinessReport, NowledgeMemSearchCandidateReport,
    NowledgeMemSearchCandidateRequest, NowledgeMemSearchCandidateShadowAccumulator,
    NowledgeMemSearchCandidateShadowEvidence, NowledgeMemSearchProjection,
    NowledgeMemSlowQueryRecord, NowledgeMemSlowQueryReport, NowledgeMemStorageRecoveryReport,
    NowledgeQueryRuntimePreflightProbe, NowledgeQueryRuntimePreflightProbeReport,
    NowledgeQueryRuntimePreflightReport, NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
    NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
    NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
    NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE, NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL,
    NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
pub use previous_wrapper_preflight::{
    nowledge_previous_wrapper_preflight_check, nowledge_previous_wrapper_preflight_check_json,
    IntoNowledgePreviousWrapperPreflightInputs, NowledgePreviousWrapperPreflightCheckReport,
    NowledgePreviousWrapperPreflightInputs, NowledgePreviousWrapperPreflightReport,
    NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
};
pub use qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, BackgroundWorkReasonCode,
    LocalQosPermit, LocalQosPolicy, LocalQosScheduler, LocalQosState, QosAdmission,
    QosAdmissionCode, RankedBackgroundWork, WorkClass, WorkPriority, WorkRequest, WORK_CLASS_COUNT,
};
pub use query_family_evidence::{
    nowledge_query_family_evidence_json, nowledge_query_family_evidence_usage,
    run_nowledge_query_family_evidence,
};
pub use query_runtime_preflight::{
    nowledge_query_runtime_preflight_usage, parse_query_runtime_preflight_probes,
    query_runtime_preflight_json, run_nowledge_query_runtime_preflight,
};
pub use replacement_summary::{
    nowledge_graph_route_readiness_summary, nowledge_graph_route_readiness_summary_from_bundle,
    nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
    nowledge_replacement_summary_usage, GraphRouteReadinessSummary,
    NowledgeReplacementSummaryOptions,
};
pub use schema::{
    BasicGraphStatistics, CompositeIndexDescriptor, ConstraintDescriptor, ConstraintId,
    ConstraintKind, ConstraintSubject, GraphStatistics, IndexDescriptor, IndexId, IndexKind,
    PropertyDescriptor, PropertyId, PropertyType, SchemaObjectState, TableDescriptor, TableId,
    TableKind,
};
pub use search::{
    CompressedVectorSearchMode, MetadataRepairOptions, MetadataRepairSummary,
    SearchAnalyzerLexicon, SearchCandidateSetReport, SearchDerivedArtifactReport, SearchDocument,
    SearchEmbeddingManifest, SearchEmptyReasonCode, SearchFallbackReasonCode, SearchFusionWeights,
    SearchHit, SearchIndex, SearchMode, SearchPredicateFieldPruningReport,
    SearchPredicatePushdownReport, SearchProjectionDelta, SearchProjectionDeltaReport,
    SearchProjectionFreshness, SearchProjectionKind, SearchProjectionProbeOptions,
    SearchProjectionRow, SearchQueryOptions, SearchRebuildOptions, SearchRebuildSummary,
    SearchResultSet, SearchRetrieverCandidateSetReport, SearchTruncationReasonCode,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
};
pub use search_candidate_shadow_evidence::{
    nowledge_search_candidate_shadow_evidence_usage, parse_search_candidate_shadow_probe,
    run_nowledge_search_candidate_shadow_evidence,
};
pub use search_projection_evidence::{
    nowledge_search_projection_evidence_usage, nowledge_search_projection_probe_contract_json,
    nowledge_search_projection_probe_contract_usage,
    nowledge_search_projection_shadow_evidence_usage, run_nowledge_search_projection_evidence,
    run_nowledge_search_projection_shadow_evidence, run_skein_search_projection_probe,
    skein_search_projection_probe_usage, NowledgeSearchProjectionEvidenceReport,
};
pub use skein_optimizer::{
    Distribution, GroupId, Memo as OptimizerMemo, MemoGroup as OptimizerMemoGroup,
    PhysicalProperties, RequiredProperties,
};
pub use storage_recovery_evidence::{
    nowledge_storage_recovery_evidence_json, nowledge_storage_recovery_evidence_usage,
    run_nowledge_storage_recovery_evidence,
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
