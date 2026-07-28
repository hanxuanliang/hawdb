use crate::search::{
    CompressedVectorSearchMode, SearchCandidateSetReport, SearchFusionWeights, SearchMode,
    SearchQueryOptions, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
};
use crate::search_projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_shadow_evidence_json,
    NowledgeSearchProjectionEvidenceReport,
};
use crate::{
    cypher, BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundMaintenanceSummary,
    BackgroundWorkHint, BackgroundWorkPlan, Database, DatabaseConfig, KnowledgeRetrievalOutput,
    KnowledgeRetrievalRequest, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    NowledgeGraphStatement, PlanCacheLookup, QueryOutput, ReadExecutionProfile, Result,
    SearchIndex, SearchProjectionDeltaReport, SearchProjectionFreshness,
    SearchProjectionGraphDeltaRequest, SearchProjectionProbeOptions, SearchResultSet, SkeinError,
    SlowQueryLogRecordSummary, Value,
};
use crate::{
    graph_route_readiness::NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
    nowledge_inventory::{
        background_maintenance_evidence_health, background_maintenance_summary_to_json,
        replacement_readiness_family_evidence_health, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
    },
    store::{RecoveryMode, ScanPruningReport, ScanPruningStrategy, StorageRecoveryReport},
    workload_fixtures::{
        NowledgeGraphRouteWorkloadFixtureReport, NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
    },
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphMode {
    ShadowReadOnly,
    WritableCutover,
}

pub fn nowledge_mem_graph_config(mode: NowledgeMemGraphMode) -> DatabaseConfig {
    nowledge_mem_graph_config_with_search_mode(mode, CompressedVectorSearchMode::Disabled)
}

pub fn nowledge_mem_graph_config_with_search_mode(
    mode: NowledgeMemGraphMode,
    compressed_vector_search_mode: CompressedVectorSearchMode,
) -> DatabaseConfig {
    DatabaseConfig {
        read_only: matches!(mode, NowledgeMemGraphMode::ShadowReadOnly),
        compressed_vector_search_mode,
        ..DatabaseConfig::default()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NowledgeMemRetrievalProjectionAdvisor {
    pub recall_evidence_ready: bool,
    pub parity_evidence_ready: bool,
    pub cold_or_constrained_local_segment: bool,
}

impl NowledgeMemRetrievalProjectionAdvisor {
    pub fn cold_local_with_recall_parity() -> Self {
        Self {
            recall_evidence_ready: true,
            parity_evidence_ready: true,
            cold_or_constrained_local_segment: true,
        }
    }

    pub fn ready(&self) -> bool {
        self.recall_evidence_ready
            && self.parity_evidence_ready
            && self.cold_or_constrained_local_segment
    }

    fn blocker_codes(&self) -> Vec<String> {
        let mut blockers = Vec::new();
        if !self.recall_evidence_ready {
            blockers.push("retrieval_projection_recall_evidence_missing".to_string());
        }
        if !self.parity_evidence_ready {
            blockers.push("retrieval_projection_parity_evidence_missing".to_string());
        }
        if !self.cold_or_constrained_local_segment {
            blockers.push("retrieval_projection_segment_not_advised".to_string());
        }
        blockers
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "recall_evidence_ready": self.recall_evidence_ready,
            "parity_evidence_ready": self.parity_evidence_ready,
            "cold_or_constrained_local_segment": self.cold_or_constrained_local_segment,
            "blocker_codes": self.blocker_codes(),
        })
    }
}

fn advised_compressed_vector_search_mode(
    requested: CompressedVectorSearchMode,
    advisor: &NowledgeMemRetrievalProjectionAdvisor,
) -> CompressedVectorSearchMode {
    if requested == CompressedVectorSearchMode::Disabled || advisor.ready() {
        requested
    } else {
        CompressedVectorSearchMode::Disabled
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenOptions {
    pub graph_path: PathBuf,
    pub search_projection_path: Option<PathBuf>,
    pub mode: NowledgeMemGraphMode,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NowledgeMemOpenDiagnosticOptions {
    pub include_local_paths: bool,
}

impl NowledgeMemOpenOptions {
    pub fn graph_only(graph_path: impl Into<PathBuf>, mode: NowledgeMemGraphMode) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: None,
            mode,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn with_search_projection(
        graph_path: impl Into<PathBuf>,
        search_projection_path: impl Into<PathBuf>,
        mode: NowledgeMemGraphMode,
    ) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: Some(search_projection_path.into()),
            mode,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn with_compressed_vector_search_mode(mut self, mode: CompressedVectorSearchMode) -> Self {
        self.compressed_vector_search_mode = mode;
        self
    }

    pub fn with_retrieval_projection_advisor(
        mut self,
        advisor: NowledgeMemRetrievalProjectionAdvisor,
    ) -> Self {
        self.retrieval_projection_advisor = advisor;
        self
    }

    fn effective_compressed_vector_search_mode(&self) -> CompressedVectorSearchMode {
        advised_compressed_vector_search_mode(
            self.compressed_vector_search_mode,
            &self.retrieval_projection_advisor,
        )
    }

    pub fn sanitized_report(&self) -> NowledgeMemOpenReport {
        let effective_compressed_vector_search_mode =
            self.effective_compressed_vector_search_mode();
        NowledgeMemOpenReport {
            protocol: NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL.to_string(),
            mode: self.mode,
            graph_configured: true,
            search_projection_configured: self.search_projection_path.is_some(),
            compressed_vector_search_mode: effective_compressed_vector_search_mode,
            requested_compressed_vector_search_mode: self.compressed_vector_search_mode,
            retrieval_projection_advisor: self.retrieval_projection_advisor.clone(),
            retrieval_projection_advisor_blocker_codes: if self.compressed_vector_search_mode
                == effective_compressed_vector_search_mode
            {
                Vec::new()
            } else {
                self.retrieval_projection_advisor.blocker_codes()
            },
            graph_opened: false,
            search_projection_opened: false,
        }
    }

    pub fn diagnostic_report_json(
        &self,
        options: NowledgeMemOpenDiagnosticOptions,
    ) -> serde_json::Value {
        let mut report = self.sanitized_report().json();
        if let Some(object) = report.as_object_mut() {
            object.insert(
                "debug_local_paths_included".to_string(),
                serde_json::Value::Bool(options.include_local_paths),
            );
            object.insert(
                "local_paths_redacted".to_string(),
                serde_json::Value::Bool(!options.include_local_paths),
            );
            if options.include_local_paths {
                object.insert(
                    "graph_path".to_string(),
                    serde_json::Value::String(self.graph_path.to_string_lossy().into_owned()),
                );
                object.insert(
                    "search_projection_path".to_string(),
                    self.search_projection_path
                        .as_ref()
                        .map(|path| serde_json::Value::String(path.to_string_lossy().into_owned()))
                        .unwrap_or(serde_json::Value::Null),
                );
            }
        }
        report
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub graph_configured: bool,
    pub search_projection_configured: bool,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub requested_compressed_vector_search_mode: CompressedVectorSearchMode,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
    pub retrieval_projection_advisor_blocker_codes: Vec<String>,
    pub graph_opened: bool,
    pub search_projection_opened: bool,
}

impl NowledgeMemOpenReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "graph_configured": self.graph_configured,
            "search_projection_configured": self.search_projection_configured,
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "requested_compressed_vector_search_mode": self.requested_compressed_vector_search_mode.as_str(),
            "retrieval_projection_advisor": self.retrieval_projection_advisor.json(),
            "retrieval_projection_advisor_blocker_codes": self.retrieval_projection_advisor_blocker_codes,
            "graph_opened": self.graph_opened,
            "search_projection_opened": self.search_projection_opened,
        })
    }
}

pub const NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL: &str = "skein-nowledge-mem-open-report";
pub const NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-query-report-v1";
pub const NOWLEDGE_MEM_READ_REPORT_PROTOCOL: &str = "skein-nowledge-mem-read-report";
pub const NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-overview-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-sample-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-node-details-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-community-members-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-community-recent-memories-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-community-subgraph-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-augmentation-state-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-pagerank-plan-route-report-v1";
pub const NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-graph-orphans-route-report-v1";
pub const NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL: &str = "skein-nowledge-mem-retrieval-report";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-search-candidate-report-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-search-candidate-readiness-v1";
pub const NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-slow-query-report-v1";
pub const NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL: &str =
    "skein-nowledge-mem-readiness-dashboard-v1";
pub const NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v1";
pub const NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL: &str = "skein-nowledge-mem-library-readiness-v1";
pub const NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-candidate-shadow-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE: &str = "nmem-rust-bridge";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE: &str =
    "search_candidate_shadow_trace";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE: &str =
    "/search-index/skein-shadow/candidate-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE: &str = "skein";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE: &str = "skein-shadow";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE: &str = "lancedb";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE: &str = "skein";
const NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS: &[&str] = &[
    "kind",
    "external_id",
    "source_id",
    "space_id",
    "unit_type",
    "lifecycle_state",
    "is_latest",
];
const NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS: &[&str] = &["importance", "confidence"];
const NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS: &[&str] =
    &["created_at", "updated_at", "event_start", "event_end"];
const NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-cli";
const SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY: &str =
    "search_projection_shadow_pushdown_evidence_not_ready";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING: &str =
    "skein_search_projection_segment_descriptor_missing";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING: &str =
    "skein_search_projection_segment_descriptor_fields_missing";
pub const NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE: &str = "/graph/overview";
pub const NOWLEDGE_MEM_GRAPH_OVERVIEW_MEMORY_RANKING_QUERY: &str = "\
MATCH (m:Memory) \
RETURN m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
COALESCE(m.pagerank_score, m.importance, 0.5) AS score, \
m.community_id AS community_id, \
m.space_id AS raw_space_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.source AS source, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance \
ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC \
LIMIT $limit";
pub const NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE: &str = "/graph/sample";
pub const NOWLEDGE_MEM_GRAPH_SAMPLE_MEMORY_QUERY: &str = "\
MATCH (m:Memory) \
RETURN m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
COALESCE(m.pagerank_score, m.importance, 0.5) AS score, \
m.community_id AS community_id, \
m.space_id AS raw_space_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.source AS source, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance \
ORDER BY m.id ASC \
LIMIT $limit";
pub const NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE: &str = "/graph/node-details/{node_id}";
pub const NOWLEDGE_MEM_GRAPH_NODE_DETAILS_MEMORY_QUERY: &str = "\
MATCH (m:Memory) \
WHERE id(m) = $node_id \
RETURN id(m) AS node_id, \
m.id AS memory_id, \
'Memory' AS node_kind, \
COALESCE(m.title, LEFT(m.content, 60), m.id, 'Memory') AS label, \
m.title AS title, \
m.content AS content, \
LEFT(COALESCE(m.content, ''), 500) AS content_preview, \
m.summary AS summary, \
m.source AS source, \
m.space_id AS raw_space_id, \
m.community_id AS community_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance, \
m.confidence AS confidence, \
m.is_latest AS is_latest, \
m.is_deleted AS is_deleted \
LIMIT 1";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE: &str =
    "/graph/community-members/{community_id}";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY: &str = "\
MATCH (m:Memory) \
WHERE m.community_id = $community_id \
RETURN m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
COALESCE(m.pagerank_score, m.importance, 0.5) AS score, \
m.community_id AS community_id, \
m.space_id AS raw_space_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.source AS source, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance \
ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC \
LIMIT $limit";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE: &str =
    "/library/community/{community_id}/recent-memories";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY: &str = "\
MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
WHERE e.community_id = $community_id \
WITH m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
m.content AS content, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
m.importance AS importance, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.is_crystal AS is_crystal, \
COUNT(DISTINCT e) AS mention_breadth \
ORDER BY created_at DESC \
LIMIT $limit \
RETURN memory_id AS memory_id, \
node_id AS node_id, \
label AS label, \
title AS title, \
content AS content, \
content_preview AS content_preview, \
importance AS importance, \
created_at AS created_at, \
updated_at AS updated_at, \
is_crystal AS is_crystal, \
mention_breadth AS mention_breadth";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE: &str =
    "/library/community/{community_id}/subgraph";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY: &str = "\
MATCH (e:Entity) \
WHERE e.community_id = $community_id \
OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) \
RETURN e.id AS entity_id, \
id(e) AS node_id, \
COALESCE(e.name, e.id) AS label, \
e.name AS name, \
e.entity_type AS entity_type, \
e.confidence AS confidence, \
COUNT(r) AS mention_count \
ORDER BY mention_count DESC, e.name ASC \
LIMIT $max_entities";
pub const NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY: &str = "\
MATCH (e1:Entity)-[r:RELATES_TO]-(e2:Entity) \
WHERE e1.id IN $entity_ids \
AND e2.id IN $entity_ids \
AND e1.id < e2.id \
RETURN e1.id AS source_entity_id, \
e2.id AS target_entity_id, \
id(r) AS relationship_id, \
r.confidence AS confidence, \
r.relation_type AS relation_type \
LIMIT $max_edges";
pub const NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE: &str = "/graph/augmentation/state";
pub const NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_QUERY: &str = "\
MATCH (m:GraphMeta {meta_id: 'main'}) \
RETURN m.community_detection_applied AS community_detection_applied, \
m.pagerank_applied AS pagerank_applied, \
m.community_algorithm AS community_algorithm, \
m.community_resolution AS community_resolution, \
m.community_count AS community_count, \
m.pagerank_algorithm AS pagerank_algorithm, \
m.pagerank_damping AS pagerank_damping, \
m.pagerank_iterations AS pagerank_iterations, \
m.last_augmentation_at AS last_augmentation_at, \
m.schema_version AS schema_version, \
m.community_detection_computed_at AS community_detection_computed_at, \
m.pagerank_computed_at AS pagerank_computed_at \
LIMIT 1";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE: &str = "/graph/augmentation/pagerank/plan";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY: &str = "\
MATCH (m:GraphMeta {meta_id: 'main'}) \
RETURN m.pagerank_applied AS pagerank_applied, \
m.pagerank_computed_at AS pagerank_computed_at \
LIMIT 1";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY: &str =
    "MATCH (m:Memory) RETURN count(m) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY: &str =
    "MATCH (e:Entity) RETURN count(e) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY: &str =
    "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) RETURN count(r) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY: &str =
    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY: &str =
    "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' RETURN count(r) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY: &str =
    "MATCH (m:Memory) WHERE m.created_at > $cutoff OR m.updated_at > $cutoff RETURN count(m) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY: &str =
    "MATCH (e:Entity) WHERE e.created_at > $cutoff OR e.updated_at > $cutoff RETURN count(e) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY: &str =
    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY: &str =
    "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total";
pub const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY: &str =
    "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' AND (r.created_at > $cutoff OR r.updated_at > $cutoff) RETURN count(r) AS total";
pub const NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE: &str = "/graph/orphans";
pub const NOWLEDGE_MEM_GRAPH_ORPHAN_ENTITIES_QUERY: &str = "\
MATCH (e:Entity) \
WHERE NOT (e)<-[:MENTIONS]-(:Memory) \
AND NOT (e)-[:RELATES_TO]-() \
AND NOT (e)-[:HAS_LABEL]-() \
RETURN e.id AS entity_id, \
id(e) AS node_id, \
COALESCE(e.name, e.id) AS label, \
e.name AS name, \
e.entity_type AS entity_type, \
e.description AS description, \
e.community_id AS community_id, \
e.confidence AS confidence, \
e.pagerank_score AS pagerank_score \
ORDER BY e.id ASC \
LIMIT $limit";
pub const NOWLEDGE_MEM_SEARCH_ROUTE: &str = "/graph/search";
pub const REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES: &[&str] = &[
    "/communities",
    "/communities/{community_id}",
    "/graph/overview",
    "/graph/sample",
    NOWLEDGE_MEM_SEARCH_ROUTE,
    "/graph/explore",
    "/graph/expand/{node_id}",
    "/graph/live-preview",
    "/graph/live-preview/{node_id}",
    "/graph/community-members/{community_id}",
    "/library/community/{community_id}/subgraph",
    "/library/community/{community_id}/recent-memories",
    "/library/community/{community_id}/related",
    "/graph/analysis",
    "/graph/augmentation/state",
    "/graph/augmentation/pagerank/plan",
    "/graph/node-details/{node_id}",
    "/graph/orphans",
    "/graph/shortest-path",
    "/sources/{source_id}",
    "/stats/entity-relations",
    "/stats/sources",
    "/stats/top-communities",
    "/entities",
    "/entities/{entity_id}/relationships",
    "/agent/evolves",
];
pub const NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION: &str =
    "nowledge-mem-graph-read-route-catalog-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphReadRouteOwner {
    GraphRuntime,
    SearchRuntime,
    ReadBatchRuntime,
}

impl NowledgeMemGraphReadRouteOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GraphRuntime => "graph_runtime",
            Self::SearchRuntime => "search_runtime",
            Self::ReadBatchRuntime => "read_batch_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphReadRouteEvidenceKind {
    GraphRouteExecution,
    SearchCandidateShadow,
    ReadBatchRuntime,
}

impl NowledgeMemGraphReadRouteEvidenceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GraphRouteExecution => "graph_route_execution",
            Self::SearchCandidateShadow => "search_candidate_shadow",
            Self::ReadBatchRuntime => "read_batch_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemGraphReadRouteSpec {
    pub route: &'static str,
    pub owner: NowledgeMemGraphReadRouteOwner,
    pub required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind,
    pub stale_on_catalog_change: bool,
}

const fn graph_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::GraphRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::GraphRouteExecution,
        stale_on_catalog_change: true,
    }
}

const fn search_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::SearchRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::SearchCandidateShadow,
        stale_on_catalog_change: true,
    }
}

const fn read_batch_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::ReadBatchRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::ReadBatchRuntime,
        stale_on_catalog_change: true,
    }
}

pub const NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS: &[NowledgeMemGraphReadRouteSpec] = &[
    read_batch_route("/communities"),
    read_batch_route("/communities/{community_id}"),
    graph_route("/graph/overview"),
    graph_route("/graph/sample"),
    search_route(NOWLEDGE_MEM_SEARCH_ROUTE),
    graph_route("/graph/explore"),
    graph_route("/graph/expand/{node_id}"),
    graph_route("/graph/live-preview"),
    graph_route("/graph/live-preview/{node_id}"),
    graph_route("/graph/community-members/{community_id}"),
    graph_route("/library/community/{community_id}/subgraph"),
    graph_route("/library/community/{community_id}/recent-memories"),
    graph_route("/library/community/{community_id}/related"),
    graph_route("/graph/analysis"),
    graph_route("/graph/augmentation/state"),
    graph_route("/graph/augmentation/pagerank/plan"),
    graph_route("/graph/node-details/{node_id}"),
    graph_route("/graph/orphans"),
    graph_route("/graph/shortest-path"),
    read_batch_route("/sources/{source_id}"),
    read_batch_route("/stats/entity-relations"),
    read_batch_route("/stats/sources"),
    read_batch_route("/stats/top-communities"),
    read_batch_route("/entities"),
    read_batch_route("/entities/{entity_id}/relationships"),
    read_batch_route("/agent/evolves"),
];

pub fn nowledge_mem_graph_read_route_spec(
    route: &str,
) -> Option<&'static NowledgeMemGraphReadRouteSpec> {
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
        .iter()
        .find(|spec| spec.route == route)
}

pub fn nowledge_mem_graph_read_route_spec_json(
    spec: &NowledgeMemGraphReadRouteSpec,
) -> serde_json::Value {
    serde_json::json!({
        "route": spec.route,
        "owner": spec.owner.as_str(),
        "required_evidence_kind": spec.required_evidence_kind.as_str(),
        "stale_on_catalog_change": spec.stale_on_catalog_change,
    })
}

pub fn nowledge_mem_graph_read_route_specs_json() -> serde_json::Value {
    serde_json::json!(NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
        .iter()
        .map(nowledge_mem_graph_read_route_spec_json)
        .collect::<Vec<_>>())
}

pub fn nowledge_mem_graph_read_route_catalog_digest() -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for spec in NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS {
        fnv1a_update(&mut hash, spec.route.as_bytes());
        fnv1a_update(&mut hash, spec.owner.as_str().as_bytes());
        fnv1a_update(&mut hash, spec.required_evidence_kind.as_str().as_bytes());
        fnv1a_update(
            &mut hash,
            if spec.stale_on_catalog_change {
                b"true"
            } else {
                b"false"
            },
        );
    }
    format!("fnv1a64:{hash:016x}")
}

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    *hash ^= 0xff;
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

pub fn nowledge_mem_required_query_families_for_route(route: &str) -> &'static [&'static str] {
    match route {
        "/communities"
        | "/communities/{community_id}"
        | "/sources/{source_id}"
        | "/stats/entity-relations"
        | "/stats/sources"
        | "/stats/top-communities"
        | "/entities"
        | "/entities/{entity_id}/relationships"
        | "/agent/evolves" => &["label_stats_read"],
        NOWLEDGE_MEM_SEARCH_ROUTE => &["search_projection"],
        "/graph/overview"
        | "/graph/sample"
        | "/graph/live-preview"
        | "/graph/live-preview/{node_id}"
        | "/graph/community-members/{community_id}"
        | "/library/community/{community_id}/recent-memories"
        | "/graph/node-details/{node_id}" => &["memory_lookup"],
        "/graph/explore"
        | "/graph/expand/{node_id}"
        | "/library/community/{community_id}/subgraph"
        | "/library/community/{community_id}/related"
        | "/graph/orphans"
        | "/graph/shortest-path" => &["graph_traversal"],
        "/graph/analysis" | "/graph/augmentation/state" | "/graph/augmentation/pagerank/plan" => {
            &["projected_graph"]
        }
        _ => &[],
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbe {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
    pub require_scan_pruning: bool,
    pub require_pruned: bool,
    pub min_scan_pruning_reports: usize,
    pub max_output_rows: Option<usize>,
}

impl NowledgeQueryRuntimePreflightProbe {
    pub fn new(name: impl Into<String>, cypher: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            route: None,
            query_family: None,
            cypher: cypher.into(),
            parameters: BTreeMap::new(),
            require_scan_pruning: false,
            require_pruned: false,
            min_scan_pruning_reports: 1,
            max_output_rows: None,
        }
    }

    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }

    pub fn with_query_family(mut self, query_family: impl Into<String>) -> Self {
        self.query_family = Some(query_family.into());
        self
    }

    pub fn with_parameters(mut self, parameters: BTreeMap<String, Value>) -> Self {
        self.parameters = parameters;
        self
    }

    pub fn require_scan_pruning(mut self, min_scan_pruning_reports: usize) -> Self {
        self.require_scan_pruning = true;
        self.min_scan_pruning_reports = min_scan_pruning_reports;
        self
    }

    pub fn require_pruned(mut self) -> Self {
        self.require_pruned = true;
        self
    }

    pub fn with_max_output_rows(mut self, max_output_rows: usize) -> Self {
        self.max_output_rows = Some(max_output_rows);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightReport {
    pub protocol: String,
    pub ready: bool,
    pub database_opened: bool,
    pub redaction: NowledgeQueryRuntimePreflightRedactionSummary,
    pub probe_count: usize,
    pub passed_probe_count: usize,
    pub failed_probe_count: usize,
    pub required_route_count: usize,
    pub covered_route_count: usize,
    pub covered_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub required_routes_covered: bool,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub route_coverage_ready: bool,
    pub route_coverage_blocker_codes: Vec<String>,
    pub blocker_codes: Vec<String>,
    pub probes: Vec<NowledgeQueryRuntimePreflightProbeReport>,
}

impl NowledgeQueryRuntimePreflightReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "database_opened": self.database_opened,
            "redaction": self.redaction.json(),
            "probe_count": self.probe_count,
            "passed_probe_count": self.passed_probe_count,
            "failed_probe_count": self.failed_probe_count,
            "required_route_count": self.required_route_count,
            "covered_route_count": self.covered_route_count,
            "covered_routes": self.covered_routes,
            "missing_required_routes": self.missing_required_routes,
            "required_routes_covered": self.required_routes_covered,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "route_coverage_ready": self.route_coverage_ready,
            "route_coverage_blocker_codes": self.route_coverage_blocker_codes,
            "blocker_codes": self.blocker_codes,
            "probes": self.probes.iter().map(NowledgeQueryRuntimePreflightProbeReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NowledgeQueryRuntimePreflightRedactionSummary {
    pub rows_copied: bool,
    pub parameters_copied: bool,
    pub local_paths_copied: bool,
    pub raw_errors_copied: bool,
}

impl NowledgeQueryRuntimePreflightRedactionSummary {
    pub fn ready(&self) -> bool {
        !self.rows_copied
            && !self.parameters_copied
            && !self.local_paths_copied
            && !self.raw_errors_copied
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "rows_copied": self.rows_copied,
            "parameters_copied": self.parameters_copied,
            "local_paths_copied": self.local_paths_copied,
            "raw_errors_copied": self.raw_errors_copied,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbeReport {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub ready: bool,
    pub success: bool,
    pub output_row_count: usize,
    pub selected_plan_fingerprint: Option<String>,
    pub search_mode: Option<String>,
    pub selected_plan_operator_counts: BTreeMap<String, usize>,
    pub selected_plan_class_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub plan_cache_lookup: Option<String>,
    pub plan_cache_bypass_reason: Option<String>,
    pub plan_cache_cacheable: bool,
    pub plan_cache_hit: bool,
    pub plan_cache_miss: bool,
    pub plan_cache_bypassed: bool,
    pub work_priority: Option<String>,
    pub work_class: Option<String>,
    pub estimated_operations: Option<usize>,
    pub max_rows: Option<usize>,
    pub detection_row_cap: Option<usize>,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_kinds: Vec<String>,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
    pub pruned_scan_count: usize,
    pub error_class: Option<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeQueryRuntimePreflightProbeReport {
    pub fn json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "name": self.name,
            "route": self.route,
            "query_family": self.query_family,
            "ready": self.ready,
            "success": self.success,
            "blocker_codes": self.blocker_codes,
        });
        let object = value
            .as_object_mut()
            .expect("query runtime preflight probe report is an object");
        if self.success {
            object.insert(
                "output_row_count".to_string(),
                serde_json::json!(self.output_row_count),
            );
            object.insert(
                "selected_plan_fingerprint".to_string(),
                serde_json::json!(self.selected_plan_fingerprint),
            );
            object.insert(
                "search_mode".to_string(),
                serde_json::json!(self.search_mode),
            );
            object.insert(
                "selected_plan_operator_counts".to_string(),
                serde_json::json!(self.selected_plan_operator_counts),
            );
            object.insert(
                "selected_plan_class_counts".to_string(),
                serde_json::json!(self.selected_plan_class_counts),
            );
            object.insert(
                "optimizer_decision_count".to_string(),
                serde_json::json!(self.optimizer_decision_count),
            );
            object.insert(
                "plan_cache_lookup".to_string(),
                serde_json::json!(self.plan_cache_lookup),
            );
            object.insert(
                "plan_cache".to_string(),
                serde_json::json!({
                    "lookup": self.plan_cache_lookup,
                    "bypass_reason": self.plan_cache_bypass_reason,
                    "cacheable": self.plan_cache_cacheable,
                    "hit": self.plan_cache_hit,
                    "miss": self.plan_cache_miss,
                    "bypassed": self.plan_cache_bypassed,
                }),
            );
            object.insert(
                "work_request".to_string(),
                serde_json::json!({
                    "priority": self.work_priority,
                    "class": self.work_class,
                    "estimated_operations": self.estimated_operations,
                }),
            );
            object.insert(
                "execution_profile".to_string(),
                serde_json::json!({
                    "max_rows": self.max_rows,
                    "detection_row_cap": self.detection_row_cap,
                    "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
                    "operator_row_cap_enabled": self.operator_row_cap_enabled,
                    "blocking_operator_kinds": self.blocking_operator_kinds,
                    "scan_pruning_report_count": self.scan_pruning_reports.len(),
                    "pruned_scan_count": self.pruned_scan_count,
                    "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
                }),
            );
        } else {
            object.insert(
                "error_class".to_string(),
                serde_json::json!(self.error_class),
            );
        }
        value
    }
}

pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ROWS: usize = 512;
pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ESTIMATED_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

impl NowledgeMemGraphMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShadowReadOnly => "shadow_read_only",
            Self::WritableCutover => "writable_cutover",
        }
    }
}

#[derive(Debug)]
pub struct NowledgeMemGraph {
    db: Database,
    mode: NowledgeMemGraphMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadOptions {
    pub max_rows: Option<usize>,
    pub max_estimated_payload_bytes: Option<usize>,
}

impl Default for NowledgeMemReadOptions {
    fn default() -> Self {
        Self {
            max_rows: Some(DEFAULT_NOWLEDGE_MEM_READ_MAX_ROWS),
            max_estimated_payload_bytes: Some(
                DEFAULT_NOWLEDGE_MEM_READ_MAX_ESTIMATED_PAYLOAD_BYTES,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub row_count: usize,
    pub max_rows: Option<usize>,
    pub execution_row_cap: Option<usize>,
    pub estimated_payload_bytes: usize,
    pub max_estimated_payload_bytes: Option<usize>,
    pub row_budget_exceeded: bool,
    pub payload_budget_exceeded: bool,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_count: usize,
    pub blocking_operator_kinds: Vec<String>,
    pub streaming: bool,
}

impl NowledgeMemReadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "row_count": self.row_count,
            "max_rows": self.max_rows,
            "execution_row_cap": self.execution_row_cap,
            "estimated_payload_bytes": self.estimated_payload_bytes,
            "max_estimated_payload_bytes": self.max_estimated_payload_bytes,
            "row_budget_exceeded": self.row_budget_exceeded,
            "payload_budget_exceeded": self.payload_budget_exceeded,
            "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
            "operator_row_cap_enabled": self.operator_row_cap_enabled,
            "blocking_operator_count": self.blocking_operator_count,
            "blocking_operator_kinds": self.blocking_operator_kinds,
            "streaming": self.streaming,
        })
    }

    pub fn bounded_read_evidence_json(&self) -> serde_json::Value {
        nowledge_mem_bounded_read_evidence_json(self)
    }
}

pub fn nowledge_mem_bounded_read_evidence_json(
    report: &NowledgeMemReadReport,
) -> serde_json::Value {
    nowledge_mem_bounded_read_evidence_json_with_routes(report, &[])
}

pub fn nowledge_mem_bounded_read_evidence_json_with_routes(
    report: &NowledgeMemReadReport,
    covered_routes: &[String],
) -> serde_json::Value {
    nowledge_mem_bounded_read_evidence_json_with_route_readiness(report, covered_routes, None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRouteReadinessSummary {
    pub route_primary_ready: bool,
    pub primary_ready_routes: Vec<String>,
    pub route_query_plan_evidence_ready: bool,
    pub route_query_profile_evidence_ready: bool,
    pub relationship_property_pruning_required_count: u64,
    pub relationship_property_pruning_report_count: u64,
    pub route_relationship_property_pruning_evidence_ready: bool,
}

pub fn nowledge_mem_bounded_read_evidence_json_with_route_readiness(
    report: &NowledgeMemReadReport,
    covered_routes: &[String],
    route_readiness: Option<&NowledgeMemRouteReadinessSummary>,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_bounded_read_blocker_codes(report);
    let missing_covered_routes = missing_nowledge_mem_bounded_read_routes(covered_routes);
    let route_readiness_blocker = route_readiness.and_then(|summary| {
        (!summary.route_primary_ready
            || !summary.route_query_plan_evidence_ready
            || !summary.route_query_profile_evidence_ready
            || !summary.route_relationship_property_pruning_evidence_ready
            || summary.relationship_property_pruning_required_count
                != summary.relationship_property_pruning_report_count)
            .then_some("graph_route_readiness_not_ready")
    });
    let blocker_codes = blocker_codes
        .into_iter()
        .chain((!missing_covered_routes.is_empty()).then_some("missing_covered_routes"))
        .chain((route_readiness.is_none()).then_some("graph_route_readiness_missing"))
        .chain(route_readiness_blocker)
        .collect::<Vec<_>>();
    let ready = blocker_codes.is_empty();
    let route_primary_ready = route_readiness.map(|summary| summary.route_primary_ready);
    let primary_ready_routes = route_readiness
        .map(|summary| summary.primary_ready_routes.clone())
        .unwrap_or_default();
    let route_query_plan_evidence_ready =
        route_readiness.map(|summary| summary.route_query_plan_evidence_ready);
    let route_query_profile_evidence_ready =
        route_readiness.map(|summary| summary.route_query_profile_evidence_ready);
    let relationship_property_pruning_required_count =
        route_readiness.map(|summary| summary.relationship_property_pruning_required_count);
    let relationship_property_pruning_report_count =
        route_readiness.map(|summary| summary.relationship_property_pruning_report_count);
    let route_relationship_property_pruning_evidence_ready =
        route_readiness.map(|summary| summary.route_relationship_property_pruning_evidence_ready);

    serde_json::json!({
        "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
        "present": true,
        "ready": ready,
        "mode": report.mode.as_str(),
        "max_rows": report.max_rows,
        "execution_row_cap": report.execution_row_cap,
        "estimated_payload_bytes": report.estimated_payload_bytes,
        "max_estimated_payload_bytes": report.max_estimated_payload_bytes,
        "row_limit_enforced_before_output": report.row_limit_enforced_before_output,
        "operator_row_cap_enabled": report.operator_row_cap_enabled,
        "streaming": report.streaming,
        "blocking_operator_count": report.blocking_operator_count,
        "blocking_operator_kinds": report.blocking_operator_kinds,
        "row_budget_exceeded": report.row_budget_exceeded,
        "payload_budget_exceeded": report.payload_budget_exceeded,
        "covered_routes": covered_routes,
        "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        "missing_covered_routes": missing_covered_routes,
        "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
        "route_primary_ready": route_primary_ready,
        "primary_ready_routes": primary_ready_routes,
        "route_query_plan_evidence_ready": route_query_plan_evidence_ready,
        "route_query_profile_evidence_ready": route_query_profile_evidence_ready,
        "relationship_property_pruning_required_count": relationship_property_pruning_required_count,
        "relationship_property_pruning_report_count": relationship_property_pruning_report_count,
        "route_relationship_property_pruning_evidence_ready": route_relationship_property_pruning_evidence_ready,
        "blocker_codes": blocker_codes,
    })
}

fn nowledge_mem_bounded_read_blocker_codes(report: &NowledgeMemReadReport) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    let expected_row_cap = match report.max_rows {
        Some(0) => {
            blockers.push("invalid_max_rows");
            None
        }
        Some(max_rows) => max_rows.checked_add(1),
        None => {
            blockers.push("missing_max_rows");
            None
        }
    };
    if report.mode != NowledgeMemGraphMode::ShadowReadOnly {
        blockers.push("not_shadow_read_only");
    }

    match (report.execution_row_cap, expected_row_cap) {
        (Some(execution_row_cap), Some(expected_row_cap))
            if execution_row_cap == expected_row_cap => {}
        (Some(_), _) => blockers.push("execution_row_cap_mismatch"),
        (None, _) => blockers.push("missing_execution_row_cap"),
    }
    if !report.row_limit_enforced_before_output {
        blockers.push("row_limit_not_enforced_before_output");
    }
    if !report.operator_row_cap_enabled {
        blockers.push("operator_row_cap_disabled");
    }
    if report.row_budget_exceeded {
        blockers.push("row_budget_exceeded");
    }
    if report.payload_budget_exceeded {
        blockers.push("payload_budget_exceeded");
    }
    blockers
}

fn missing_nowledge_mem_bounded_read_routes(covered_routes: &[String]) -> Vec<&'static str> {
    let covered_routes = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_routes.contains(route))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateShadowEvidence {
    pub request_count: u64,
    pub primary_candidate_count: u64,
    pub shadow_candidate_count: u64,
    pub matched_candidate_count: u64,
    pub primary_only_candidate_count: u64,
    pub text_retriever_available: bool,
    pub vector_retriever_available: bool,
    pub text_retriever_candidate_count: u64,
    pub vector_retriever_candidate_count: u64,
    pub fts_top_k_overlap_observed: bool,
    pub fts_top_k_overlap_ready: bool,
    pub vector_top_k_overlap_observed: bool,
    pub vector_top_k_overlap_ready: bool,
    pub source_chunk_identity_ready: bool,
    pub fail_soft_observed: bool,
    pub projection_marker_status_visible: bool,
    pub projection_watermark_ready: bool,
    pub embedding_identity_ready: bool,
    pub primary_candidate_identity_checksum: Option<u64>,
    pub shadow_candidate_identity_checksum: Option<u64>,
    pub matched_candidate_identity_checksum: Option<u64>,
    pub filter_pushdown: Option<NowledgeMemSearchCandidateFilterPushdownEvidence>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateFilterPushdownEvidence {
    pub pushed_predicate_count: u64,
    pub shadow_scan_present: bool,
    pub field_summaries: Vec<NowledgeMemSearchCandidateFieldSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateFieldSummary {
    pub field: String,
    pub source: String,
    pub segment_count: usize,
    pub value_summary_used: bool,
    pub value_summary_segment_count: usize,
    pub numeric_range_summary_used: bool,
    pub numeric_range_segment_count: usize,
    pub timestamp_range_summary_used: bool,
    pub timestamp_range_segment_count: usize,
}

impl NowledgeMemSearchCandidateFieldSummary {
    fn merge_capabilities(&mut self, other: &Self) {
        self.segment_count = self.segment_count.max(other.segment_count);
        self.value_summary_used |= other.value_summary_used;
        self.value_summary_segment_count = self
            .value_summary_segment_count
            .max(other.value_summary_segment_count);
        self.numeric_range_summary_used |= other.numeric_range_summary_used;
        self.numeric_range_segment_count = self
            .numeric_range_segment_count
            .max(other.numeric_range_segment_count);
        self.timestamp_range_summary_used |= other.timestamp_range_summary_used;
        self.timestamp_range_segment_count = self
            .timestamp_range_segment_count
            .max(other.timestamp_range_segment_count);
        if self.source != other.source {
            self.source = "merged".to_string();
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateShadowAccumulator {
    request_count: u64,
    primary_candidate_count: u64,
    shadow_candidate_count: u64,
    matched_candidate_count: u64,
    primary_only_candidate_count: u64,
    text_retriever_available: bool,
    vector_retriever_available: bool,
    text_retriever_candidate_count: u64,
    vector_retriever_candidate_count: u64,
    fts_top_k_overlap_observed: bool,
    fts_top_k_overlap_ready: bool,
    vector_top_k_overlap_observed: bool,
    vector_top_k_overlap_ready: bool,
    source_chunk_identity_ready: bool,
    fail_soft_observed: bool,
    projection_marker_status_visible: bool,
    projection_watermark_ready: bool,
    embedding_identity_ready: bool,
    primary_candidate_identity_checksum: Option<u64>,
    shadow_candidate_identity_checksum: Option<u64>,
    matched_candidate_identity_checksum: Option<u64>,
    filter_pushdown: Option<NowledgeMemSearchCandidateFilterPushdownEvidence>,
    blocker_codes: BTreeSet<String>,
}

impl NowledgeMemSearchCandidateShadowAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_compare(
        &mut self,
        primary_candidate_count: u64,
        shadow_candidate_count: u64,
        matched_candidate_count: u64,
    ) {
        self.request_count = self.request_count.saturating_add(1);
        self.primary_candidate_count = self
            .primary_candidate_count
            .saturating_add(primary_candidate_count);
        self.shadow_candidate_count = self
            .shadow_candidate_count
            .saturating_add(shadow_candidate_count);
        self.matched_candidate_count = self
            .matched_candidate_count
            .saturating_add(matched_candidate_count);
        self.primary_only_candidate_count = self
            .primary_only_candidate_count
            .saturating_add(primary_candidate_count.saturating_sub(matched_candidate_count));
        if matched_candidate_count > primary_candidate_count
            || matched_candidate_count > shadow_candidate_count
        {
            self.blocker_codes
                .insert("search_candidate_invalid_match_count".to_string());
        }
    }

    pub fn add_blocker_code(&mut self, code: impl Into<String>) {
        self.blocker_codes.insert(code.into());
    }

    pub fn record_retriever_leg(
        &mut self,
        name: impl AsRef<str>,
        available: bool,
        candidate_count: u64,
    ) {
        match name.as_ref() {
            "text" => {
                self.text_retriever_available |= available;
                self.text_retriever_candidate_count = self
                    .text_retriever_candidate_count
                    .saturating_add(candidate_count);
            }
            "vector" => {
                self.vector_retriever_available |= available;
                self.vector_retriever_candidate_count = self
                    .vector_retriever_candidate_count
                    .saturating_add(candidate_count);
            }
            _ => {
                self.blocker_codes
                    .insert("search_candidate_unknown_retriever_leg".to_string());
            }
        }
    }

    pub fn record_filter_pushdown_fields<I, S>(&mut self, pushed_predicate_count: u64, fields: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut observed_fields = self
            .filter_pushdown
            .as_ref()
            .map(|filter| {
                filter
                    .field_summaries
                    .iter()
                    .map(|summary| summary.field.clone())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        observed_fields.extend(fields.into_iter().map(|field| field.as_ref().to_string()));
        self.filter_pushdown = Some(NowledgeMemSearchCandidateFilterPushdownEvidence {
            pushed_predicate_count: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.pushed_predicate_count)
                .unwrap_or_default()
                .saturating_add(pushed_predicate_count),
            shadow_scan_present: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.shadow_scan_present)
                .unwrap_or(true),
            field_summaries: observed_fields
                .into_iter()
                .map(|field| {
                    nowledge_mem_search_candidate_descriptor_contract_field_summary(&field)
                })
                .collect(),
        });
    }

    pub fn record_filter_pushdown_report(&mut self, report: &NowledgeMemSearchCandidateReport) {
        let field_summaries = if report.persisted_segment_descriptor_used {
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .map(|field| nowledge_mem_search_candidate_descriptor_contract_field_summary(field))
                .collect::<Vec<_>>()
        } else {
            report
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries
                .iter()
                .map(nowledge_mem_search_candidate_field_summary_from_pruning_report)
                .collect::<Vec<_>>()
        };
        self.record_filter_pushdown_summaries(
            report.pushed_predicate_count as u64,
            true,
            field_summaries,
        );
        if !report.persisted_segment_descriptor_used {
            self.add_blocker_code("search_candidate_segment_descriptor_not_used");
        }
        if report.residual_predicate_count > 0 {
            self.add_blocker_code("search_candidate_metadata_filter_residual");
        }
    }

    pub(crate) fn record_filter_pushdown_summaries(
        &mut self,
        pushed_predicate_count: u64,
        shadow_scan_present: bool,
        summaries: Vec<NowledgeMemSearchCandidateFieldSummary>,
    ) {
        let mut observed = self
            .filter_pushdown
            .as_ref()
            .map(|filter| {
                filter
                    .field_summaries
                    .iter()
                    .map(|summary| (summary.field.clone(), summary.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        for summary in summaries {
            observed
                .entry(summary.field.clone())
                .and_modify(|existing| existing.merge_capabilities(&summary))
                .or_insert(summary);
        }
        self.filter_pushdown = Some(NowledgeMemSearchCandidateFilterPushdownEvidence {
            pushed_predicate_count: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.pushed_predicate_count)
                .unwrap_or_default()
                .saturating_add(pushed_predicate_count),
            shadow_scan_present: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.shadow_scan_present && shadow_scan_present)
                .unwrap_or(shadow_scan_present),
            field_summaries: observed.into_values().collect(),
        });
    }

    pub fn record_compare_candidate_ids(
        &mut self,
        primary_candidate_ids: &[impl AsRef<str>],
        shadow_candidate_ids: &[impl AsRef<str>],
    ) {
        let primary = primary_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<BTreeSet<_>>();
        let shadow = shadow_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<BTreeSet<_>>();
        let matched = primary
            .intersection(&shadow)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.record_compare(
            primary.len() as u64,
            shadow.len() as u64,
            matched.len() as u64,
        );
        update_search_candidate_identity_checksum(
            &mut self.primary_candidate_identity_checksum,
            &primary,
        );
        update_search_candidate_identity_checksum(
            &mut self.shadow_candidate_identity_checksum,
            &shadow,
        );
        update_search_candidate_identity_checksum(
            &mut self.matched_candidate_identity_checksum,
            &matched,
        );
    }

    pub fn record_search_candidate_output<I, S>(
        &mut self,
        primary_candidate_ids: I,
        shadow_output: &NowledgeMemSearchCandidateOutput,
    ) where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let primary_candidate_ids = primary_candidate_ids
            .into_iter()
            .map(|id| id.as_ref().to_string())
            .collect::<Vec<_>>();
        let shadow_candidate_ids = shadow_output
            .result
            .hits
            .iter()
            .map(|hit| hit.id.clone())
            .collect::<Vec<_>>();
        self.record_compare_candidate_ids(&primary_candidate_ids, &shadow_candidate_ids);
        self.record_top_k_overlap_candidate_ids(
            shadow_output.report.mode,
            &primary_candidate_ids,
            &shadow_candidate_ids,
        );
        self.record_retriever_leg_report(&shadow_output.report);
        self.record_filter_pushdown_report(&shadow_output.report);
        self.record_candidate_readiness_report(&shadow_output.readiness_report(
            &NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read(),
        ));
    }

    pub fn record_candidate_readiness_report(
        &mut self,
        report: &NowledgeMemSearchCandidateReadinessReport,
    ) {
        self.record_candidate_readiness_signals(
            report.source_chunk_identity_ready,
            report.fail_soft_observed,
            report.projection_marker_status_visible,
            report.projection_watermark_ready,
            report.embedding_identity_ready,
        );
    }

    pub fn record_candidate_readiness_signals(
        &mut self,
        source_chunk_identity_ready: bool,
        fail_soft_observed: bool,
        projection_marker_status_visible: bool,
        projection_watermark_ready: bool,
        embedding_identity_ready: bool,
    ) {
        self.source_chunk_identity_ready |= source_chunk_identity_ready;
        self.fail_soft_observed |= fail_soft_observed;
        self.projection_marker_status_visible |= projection_marker_status_visible;
        self.projection_watermark_ready |= projection_watermark_ready;
        self.embedding_identity_ready |= embedding_identity_ready;
    }

    pub fn record_top_k_overlap_candidate_ids(
        &mut self,
        mode: SearchMode,
        primary_candidate_ids: &[impl AsRef<str>],
        shadow_candidate_ids: &[impl AsRef<str>],
    ) {
        let primary_candidate_ids = primary_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<Vec<_>>();
        let shadow_candidate_ids = shadow_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<Vec<_>>();
        self.record_top_k_overlap(mode, &primary_candidate_ids, &shadow_candidate_ids);
    }

    fn record_top_k_overlap(
        &mut self,
        mode: SearchMode,
        primary_candidate_ids: &[String],
        shadow_candidate_ids: &[String],
    ) {
        let ready = !primary_candidate_ids.is_empty()
            && primary_candidate_ids.len() == shadow_candidate_ids.len()
            && primary_candidate_ids
                .iter()
                .zip(shadow_candidate_ids)
                .all(|(primary, shadow)| primary == shadow);
        match mode {
            SearchMode::Text => {
                self.fts_top_k_overlap_observed = true;
                self.fts_top_k_overlap_ready |= ready;
            }
            SearchMode::Vector => {
                self.vector_top_k_overlap_observed = true;
                self.vector_top_k_overlap_ready |= ready;
            }
            SearchMode::Hybrid => {}
        }
    }

    fn record_retriever_leg_report(&mut self, report: &NowledgeMemSearchCandidateReport) {
        self.record_retriever_leg(
            "text",
            report
                .retriever_available
                .get("text")
                .copied()
                .unwrap_or(false),
            retriever_candidate_count(report, "text"),
        );
        self.record_retriever_leg(
            "vector",
            report
                .retriever_available
                .get("vector")
                .copied()
                .unwrap_or(false),
            retriever_candidate_count(report, "vector"),
        );
    }

    pub fn evidence(&self) -> NowledgeMemSearchCandidateShadowEvidence {
        NowledgeMemSearchCandidateShadowEvidence {
            request_count: self.request_count,
            primary_candidate_count: self.primary_candidate_count,
            shadow_candidate_count: self.shadow_candidate_count,
            matched_candidate_count: self.matched_candidate_count,
            primary_only_candidate_count: self.primary_only_candidate_count,
            text_retriever_available: self.text_retriever_available,
            vector_retriever_available: self.vector_retriever_available,
            text_retriever_candidate_count: self.text_retriever_candidate_count,
            vector_retriever_candidate_count: self.vector_retriever_candidate_count,
            fts_top_k_overlap_observed: self.fts_top_k_overlap_observed,
            fts_top_k_overlap_ready: self.fts_top_k_overlap_observed
                && self.fts_top_k_overlap_ready,
            vector_top_k_overlap_observed: self.vector_top_k_overlap_observed,
            vector_top_k_overlap_ready: self.vector_top_k_overlap_observed
                && self.vector_top_k_overlap_ready,
            source_chunk_identity_ready: self.source_chunk_identity_ready,
            fail_soft_observed: self.fail_soft_observed,
            projection_marker_status_visible: self.projection_marker_status_visible,
            projection_watermark_ready: self.projection_watermark_ready,
            embedding_identity_ready: self.embedding_identity_ready,
            primary_candidate_identity_checksum: self.primary_candidate_identity_checksum,
            shadow_candidate_identity_checksum: self.shadow_candidate_identity_checksum,
            matched_candidate_identity_checksum: self.matched_candidate_identity_checksum,
            filter_pushdown: self.filter_pushdown.clone(),
            blocker_codes: self.blocker_codes.iter().cloned().collect(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        self.evidence().json()
    }
}

impl NowledgeMemSearchCandidateShadowEvidence {
    pub fn ready(
        request_count: u64,
        primary_candidate_count: u64,
        shadow_candidate_count: u64,
        matched_candidate_count: u64,
    ) -> Self {
        Self {
            request_count,
            primary_candidate_count,
            shadow_candidate_count,
            matched_candidate_count,
            primary_only_candidate_count: 0,
            text_retriever_available: false,
            vector_retriever_available: false,
            text_retriever_candidate_count: 0,
            vector_retriever_candidate_count: 0,
            fts_top_k_overlap_observed: false,
            fts_top_k_overlap_ready: false,
            vector_top_k_overlap_observed: false,
            vector_top_k_overlap_ready: false,
            source_chunk_identity_ready: false,
            fail_soft_observed: false,
            projection_marker_status_visible: false,
            projection_watermark_ready: false,
            embedding_identity_ready: false,
            primary_candidate_identity_checksum: None,
            shadow_candidate_identity_checksum: None,
            matched_candidate_identity_checksum: None,
            filter_pushdown: None,
            blocker_codes: Vec::new(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        nowledge_mem_search_candidate_shadow_evidence_json(self)
    }
}

pub fn nowledge_mem_search_candidate_shadow_evidence_json(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_search_candidate_shadow_blocker_codes(evidence);
    let candidate_identity = nowledge_mem_search_candidate_shadow_identity_json(evidence);
    let filter_pushdown = nowledge_mem_search_candidate_filter_pushdown_json(evidence);
    let ready = blocker_codes.is_empty();
    let row_count_parity = evidence.request_count > 0
        && evidence.primary_candidate_count == evidence.shadow_candidate_count
        && evidence.matched_candidate_count == evidence.shadow_candidate_count
        && evidence.primary_only_candidate_count == 0;
    let shadow_scan_filter_pushdown_ready = filter_pushdown
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let shadow_scan_field_pruning_ready = filter_pushdown
        .get("field_capabilities_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        && filter_pushdown
            .get("missing_required_fields")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty);
    let shadow_scan_field_summary_count = filter_pushdown
        .get("field_summary_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        "route": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        "evidence_source": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        "engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
        "ready": ready,
        "candidate_primary_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
        "request_count": evidence.request_count,
        "primary_candidate_count": evidence.primary_candidate_count,
        "shadow_candidate_count": evidence.shadow_candidate_count,
        "matched_candidate_count": evidence.matched_candidate_count,
        "primary_only_candidate_count": evidence.primary_only_candidate_count,
        "row_count_parity": row_count_parity,
        "text_retriever_ready": evidence.text_retriever_available
            && evidence.text_retriever_candidate_count > 0,
        "vector_retriever_ready": evidence.vector_retriever_available
            && evidence.vector_retriever_candidate_count > 0,
        "fts_top_k_overlap_ready": evidence.fts_top_k_overlap_ready,
        "vector_top_k_overlap_ready": evidence.vector_top_k_overlap_ready,
        "top_k_overlap_observed": {
            "fts": evidence.fts_top_k_overlap_observed,
            "vector": evidence.vector_top_k_overlap_observed,
        },
        "candidate_readiness": {
            "source_chunk_identity_ready": evidence.source_chunk_identity_ready,
            "fail_soft_observed": evidence.fail_soft_observed,
            "projection_marker_status_visible": evidence.projection_marker_status_visible,
            "projection_watermark_ready": evidence.projection_watermark_ready,
            "embedding_identity_ready": evidence.embedding_identity_ready,
        },
        "retriever_leg_candidate_counts": {
            "text": evidence.text_retriever_candidate_count,
            "vector": evidence.vector_retriever_candidate_count,
        },
        "candidate_identity": candidate_identity,
        "shadow_scan_present": evidence
            .filter_pushdown
            .as_ref()
            .map(|filter| filter.shadow_scan_present)
            .unwrap_or(false),
        "shadow_scan_filter_pushdown_ready": shadow_scan_filter_pushdown_ready,
        "shadow_scan_field_pruning_ready": shadow_scan_field_pruning_ready,
        "shadow_scan_field_summary_count": shadow_scan_field_summary_count,
        "filter_pushdown_ready": filter_pushdown
            .get("ready")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        "filter_pushdown": filter_pushdown,
        "blocker_codes": blocker_codes,
    })
}

fn retriever_candidate_count(report: &NowledgeMemSearchCandidateReport, name: &str) -> u64 {
    report
        .retriever_candidate_counts
        .get(name)
        .copied()
        .unwrap_or_default() as u64
}

fn nowledge_mem_search_candidate_shadow_identity_json(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> serde_json::Value {
    let primary_checksum = evidence.primary_candidate_identity_checksum;
    let shadow_checksum = evidence.shadow_candidate_identity_checksum;
    let matched_checksum = evidence.matched_candidate_identity_checksum;
    let parity = primary_checksum.is_some()
        && primary_checksum == shadow_checksum
        && matched_checksum == shadow_checksum;
    serde_json::json!({
        "ready": parity,
        "id_space": "search_candidate_id",
        "representation": "per_request_sorted_candidate_ids",
        "primary_checksum": primary_checksum,
        "shadow_checksum": shadow_checksum,
        "matched_checksum": matched_checksum,
        "parity": parity,
    })
}

fn nowledge_mem_search_candidate_shadow_blocker_codes(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> Vec<String> {
    let mut blockers = evidence
        .blocker_codes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if evidence.request_count == 0 {
        blockers.insert("search_candidate_shadow_no_requests".to_string());
    }
    if evidence.primary_candidate_count != evidence.shadow_candidate_count
        || evidence.matched_candidate_count != evidence.shadow_candidate_count
    {
        blockers.insert("search_candidate_mismatch".to_string());
    }
    if evidence.primary_only_candidate_count != 0 {
        blockers.insert("search_candidate_primary_only".to_string());
    }
    let identity_ready = evidence.primary_candidate_identity_checksum.is_some()
        && evidence.primary_candidate_identity_checksum
            == evidence.shadow_candidate_identity_checksum
        && evidence.matched_candidate_identity_checksum
            == evidence.shadow_candidate_identity_checksum;
    if evidence.primary_candidate_identity_checksum.is_none()
        || evidence.shadow_candidate_identity_checksum.is_none()
        || evidence.matched_candidate_identity_checksum.is_none()
    {
        blockers.insert("search_candidate_identity_missing".to_string());
    } else if !identity_ready {
        blockers.insert("search_candidate_identity_mismatch".to_string());
    }
    blockers.extend(nowledge_mem_search_candidate_filter_pushdown_blockers(
        evidence,
    ));
    blockers.into_iter().collect()
}

fn nowledge_mem_search_candidate_filter_pushdown_json(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_search_candidate_filter_pushdown_blockers(evidence);
    let missing_required_fields =
        nowledge_mem_search_candidate_missing_filter_fields(evidence.filter_pushdown.as_ref());
    let missing_value_summary_fields = nowledge_mem_search_candidate_missing_capability_fields(
        evidence.filter_pushdown.as_ref(),
        NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS,
        CandidateFieldCapability::Value,
    );
    let missing_numeric_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        evidence.filter_pushdown.as_ref(),
        NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS,
        CandidateFieldCapability::NumericRange,
    );
    let missing_timestamp_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        evidence.filter_pushdown.as_ref(),
        NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS,
        CandidateFieldCapability::TimestampRange,
    );
    let field_summaries = evidence
        .filter_pushdown
        .as_ref()
        .map(|filter| {
            filter
                .field_summaries
                .iter()
                .map(|summary| {
                    serde_json::json!({
                        "field": summary.field,
                        "source": summary.source,
                        "segment_count": summary.segment_count,
                        "value_summary_used": summary.value_summary_used,
                        "value_summary_segment_count": summary.value_summary_segment_count,
                        "numeric_range_summary_used": summary.numeric_range_summary_used,
                        "numeric_range_segment_count": summary.numeric_range_segment_count,
                        "timestamp_range_summary_used": summary.timestamp_range_summary_used,
                        "timestamp_range_segment_count": summary.timestamp_range_segment_count,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    serde_json::json!({
        "ready": blocker_codes.is_empty(),
        "pushed_predicate_count": evidence
            .filter_pushdown
            .as_ref()
            .map(|filter| filter.pushed_predicate_count),
        "shadow_scan_present": evidence
            .filter_pushdown
            .as_ref()
            .map(|filter| filter.shadow_scan_present),
        "required_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
        "missing_required_fields": missing_required_fields,
        "missing_value_summary_fields": missing_value_summary_fields,
        "missing_numeric_range_fields": missing_numeric_range_fields,
        "missing_timestamp_range_fields": missing_timestamp_range_fields,
        "field_capabilities_ready": missing_value_summary_fields.is_empty()
            && missing_numeric_range_fields.is_empty()
            && missing_timestamp_range_fields.is_empty(),
        "field_summary_count": field_summaries.len(),
        "field_summaries": field_summaries,
        "blocker_codes": blocker_codes,
    })
}

fn nowledge_mem_search_candidate_filter_pushdown_blockers(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    let Some(filter_pushdown) = evidence.filter_pushdown.as_ref() else {
        return vec!["search_candidate_filter_pushdown_missing".to_string()];
    };
    if filter_pushdown.pushed_predicate_count == 0 {
        blockers.insert("search_candidate_filter_pushdown_no_predicates".to_string());
    }
    if !filter_pushdown.shadow_scan_present {
        blockers.insert("search_candidate_shadow_scan_missing".to_string());
    }
    let missing_required_fields =
        nowledge_mem_search_candidate_missing_filter_fields(Some(filter_pushdown));
    if !missing_required_fields.is_empty() {
        blockers.insert("search_candidate_field_pruning_missing".to_string());
    }
    let missing_value_summary_fields = nowledge_mem_search_candidate_missing_capability_fields(
        Some(filter_pushdown),
        NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS,
        CandidateFieldCapability::Value,
    );
    let missing_numeric_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        Some(filter_pushdown),
        NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS,
        CandidateFieldCapability::NumericRange,
    );
    let missing_timestamp_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        Some(filter_pushdown),
        NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS,
        CandidateFieldCapability::TimestampRange,
    );
    if !missing_value_summary_fields.is_empty()
        || !missing_numeric_range_fields.is_empty()
        || !missing_timestamp_range_fields.is_empty()
    {
        blockers.insert("search_candidate_field_pruning_capability_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn nowledge_mem_search_candidate_missing_filter_fields(
    filter_pushdown: Option<&NowledgeMemSearchCandidateFilterPushdownEvidence>,
) -> Vec<&'static str> {
    let observed_fields = filter_pushdown
        .map(|filter| {
            filter
                .field_summaries
                .iter()
                .map(|summary| summary.field.as_str())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .copied()
        .filter(|field| !observed_fields.contains(field))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFieldCapability {
    Value,
    NumericRange,
    TimestampRange,
}

fn nowledge_mem_search_candidate_missing_capability_fields(
    filter_pushdown: Option<&NowledgeMemSearchCandidateFilterPushdownEvidence>,
    required_fields: &'static [&'static str],
    capability: CandidateFieldCapability,
) -> Vec<&'static str> {
    let summaries = filter_pushdown
        .map(|filter| filter.field_summaries.as_slice())
        .unwrap_or(&[]);
    required_fields
        .iter()
        .copied()
        .filter(|field| {
            !summaries.iter().any(|summary| {
                summary.field == *field && candidate_field_has_capability(summary, capability)
            })
        })
        .collect()
}

fn candidate_field_has_capability(
    summary: &NowledgeMemSearchCandidateFieldSummary,
    capability: CandidateFieldCapability,
) -> bool {
    match capability {
        CandidateFieldCapability::Value => {
            summary.value_summary_used && summary.value_summary_segment_count > 0
        }
        CandidateFieldCapability::NumericRange => {
            summary.numeric_range_summary_used && summary.numeric_range_segment_count > 0
        }
        CandidateFieldCapability::TimestampRange => {
            (summary.timestamp_range_summary_used && summary.timestamp_range_segment_count > 0)
                || (summary.numeric_range_summary_used && summary.numeric_range_segment_count > 0)
        }
    }
}

fn nowledge_mem_search_candidate_descriptor_contract_field_summary(
    field: &str,
) -> NowledgeMemSearchCandidateFieldSummary {
    NowledgeMemSearchCandidateFieldSummary {
        field: field.to_string(),
        source: "persisted_segment_descriptor_contract".to_string(),
        segment_count: 1,
        value_summary_used: NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS.contains(&field),
        value_summary_segment_count: usize::from(
            NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS.contains(&field),
        ),
        numeric_range_summary_used: NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS.contains(&field)
            || NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS.contains(&field),
        numeric_range_segment_count: usize::from(
            NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS.contains(&field)
                || NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS.contains(&field),
        ),
        timestamp_range_summary_used: NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS
            .contains(&field),
        timestamp_range_segment_count: usize::from(
            NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS.contains(&field),
        ),
    }
}

fn nowledge_mem_search_candidate_field_summary_from_pruning_report(
    report: &crate::search::SearchPredicateFieldPruningReport,
) -> NowledgeMemSearchCandidateFieldSummary {
    NowledgeMemSearchCandidateFieldSummary {
        field: report.field.clone(),
        source: "search_predicate_pruning_report".to_string(),
        segment_count: report.segment_count,
        value_summary_used: report.value_summary_used,
        value_summary_segment_count: usize::from(report.value_summary_used) * report.segment_count,
        numeric_range_summary_used: report.numeric_range_summary_used,
        numeric_range_segment_count: usize::from(report.numeric_range_summary_used)
            * report.segment_count,
        timestamp_range_summary_used: report.timestamp_range_summary_used,
        timestamp_range_segment_count: usize::from(report.timestamp_range_summary_used)
            * report.segment_count,
    }
}

fn update_search_candidate_identity_checksum(
    checksum: &mut Option<u64>,
    candidate_ids: &BTreeSet<String>,
) {
    let mut value = checksum.unwrap_or(FNV64_OFFSET);
    value = fnv64_update(value, b"request\n");
    for candidate_id in candidate_ids {
        value = fnv64_update(value, candidate_id.as_bytes());
        value = fnv64_update(value, b"\0");
    }
    *checksum = Some(value);
}

const FNV64_OFFSET: u64 = 0xcbf29ce484222325;
const FNV64_PRIME: u64 = 0x100000001b3;

fn fnv64_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV64_PRIME);
    }
    hash
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemGraphOverviewOptions {
    pub limit: usize,
    pub read_options: NowledgeMemReadOptions,
}

impl Default for NowledgeMemGraphOverviewOptions {
    fn default() -> Self {
        Self {
            limit: 64,
            read_options: NowledgeMemReadOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphOverviewRow {
    pub memory_id: Option<String>,
    pub node_id: u64,
    pub label: Option<String>,
    pub title: Option<String>,
    pub content_preview: Option<String>,
    pub score: Option<f64>,
    pub community_id: Option<Value>,
    pub raw_space_id: Option<String>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub source: Option<String>,
    pub event_start: Option<Value>,
    pub event_end: Option<Value>,
    pub importance: Option<Value>,
}

impl NowledgeMemGraphOverviewRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "memory_id": self.memory_id,
            "node_id": self.node_id,
            "label": self.label,
            "title": self.title,
            "content_preview": self.content_preview,
            "score": self.score,
            "community_id": self.community_id.as_ref().map(nowledge_value_json),
            "raw_space_id": self.raw_space_id,
            "created_at": self.created_at.as_ref().map(nowledge_value_json),
            "updated_at": self.updated_at.as_ref().map(nowledge_value_json),
            "source": self.source,
            "event_start": self.event_start.as_ref().map(nowledge_value_json),
            "event_end": self.event_end.as_ref().map(nowledge_value_json),
            "importance": self.importance.as_ref().map(nowledge_value_json),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphOverviewRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub row_count: usize,
    pub read_report: NowledgeMemReadReport,
}

impl NowledgeMemGraphOverviewRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "row_count": self.row_count,
            "read_report": self.read_report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphOverviewOutput {
    pub rows: Vec<NowledgeMemGraphOverviewRow>,
    pub report: NowledgeMemGraphOverviewRouteReport,
}

impl NowledgeMemGraphOverviewOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "rows": self.rows.iter().map(NowledgeMemGraphOverviewRow::json).collect::<Vec<_>>(),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemGraphSampleOptions {
    pub limit: usize,
    pub read_options: NowledgeMemReadOptions,
}

impl Default for NowledgeMemGraphSampleOptions {
    fn default() -> Self {
        Self {
            limit: 64,
            read_options: NowledgeMemReadOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphSampleRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub row_count: usize,
    pub read_report: NowledgeMemReadReport,
}

impl NowledgeMemGraphSampleRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "row_count": self.row_count,
            "read_report": self.read_report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphSampleOutput {
    pub rows: Vec<NowledgeMemGraphOverviewRow>,
    pub report: NowledgeMemGraphSampleRouteReport,
}

impl NowledgeMemGraphSampleOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "rows": self.rows.iter().map(NowledgeMemGraphOverviewRow::json).collect::<Vec<_>>(),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemGraphNodeDetailsOptions {
    pub node_id: u64,
    pub read_options: NowledgeMemReadOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphNodeDetailsRow {
    pub node_id: u64,
    pub memory_id: Option<String>,
    pub node_kind: String,
    pub label: Option<String>,
    pub title: Option<String>,
    pub content: Option<String>,
    pub content_preview: Option<String>,
    pub summary: Option<String>,
    pub source: Option<String>,
    pub raw_space_id: Option<String>,
    pub community_id: Option<Value>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub event_start: Option<Value>,
    pub event_end: Option<Value>,
    pub importance: Option<Value>,
    pub confidence: Option<Value>,
    pub is_latest: Option<bool>,
    pub is_deleted: Option<bool>,
}

impl NowledgeMemGraphNodeDetailsRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "node_id": self.node_id,
            "memory_id": self.memory_id,
            "node_kind": self.node_kind,
            "label": self.label,
            "title": self.title,
            "content": self.content,
            "content_preview": self.content_preview,
            "summary": self.summary,
            "source": self.source,
            "raw_space_id": self.raw_space_id,
            "community_id": self.community_id.as_ref().map(nowledge_value_json),
            "created_at": self.created_at.as_ref().map(nowledge_value_json),
            "updated_at": self.updated_at.as_ref().map(nowledge_value_json),
            "event_start": self.event_start.as_ref().map(nowledge_value_json),
            "event_end": self.event_end.as_ref().map(nowledge_value_json),
            "importance": self.importance.as_ref().map(nowledge_value_json),
            "confidence": self.confidence.as_ref().map(nowledge_value_json),
            "is_latest": self.is_latest,
            "is_deleted": self.is_deleted,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphNodeDetailsRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub node_id: u64,
    pub row_count: usize,
    pub read_report: NowledgeMemReadReport,
}

impl NowledgeMemGraphNodeDetailsRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "node_id": self.node_id,
            "row_count": self.row_count,
            "read_report": self.read_report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphNodeDetailsOutput {
    pub node: Option<NowledgeMemGraphNodeDetailsRow>,
    pub report: NowledgeMemGraphNodeDetailsRouteReport,
}

impl NowledgeMemGraphNodeDetailsOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "node": self.node.as_ref().map(NowledgeMemGraphNodeDetailsRow::json),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemGraphCommunityMembersOptions {
    pub community_id: i64,
    pub limit: usize,
    pub read_options: NowledgeMemReadOptions,
}

impl NowledgeMemGraphCommunityMembersOptions {
    pub fn new(community_id: i64, limit: usize) -> Self {
        Self {
            community_id,
            limit,
            read_options: NowledgeMemReadOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunityMembersRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub community_id: i64,
    pub row_count: usize,
    pub read_report: NowledgeMemReadReport,
}

impl NowledgeMemGraphCommunityMembersRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "community_id": self.community_id,
            "row_count": self.row_count,
            "read_report": self.read_report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunityMembersOutput {
    pub rows: Vec<NowledgeMemGraphOverviewRow>,
    pub report: NowledgeMemGraphCommunityMembersRouteReport,
}

impl NowledgeMemGraphCommunityMembersOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "rows": self.rows.iter().map(NowledgeMemGraphOverviewRow::json).collect::<Vec<_>>(),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemGraphCommunityRecentMemoriesOptions {
    pub community_id: i64,
    pub limit: usize,
    pub read_options: NowledgeMemReadOptions,
}

impl NowledgeMemGraphCommunityRecentMemoriesOptions {
    pub fn new(community_id: i64, limit: usize) -> Self {
        Self {
            community_id,
            limit,
            read_options: NowledgeMemReadOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunityRecentMemoryRow {
    pub memory_id: Option<String>,
    pub node_id: u64,
    pub label: Option<String>,
    pub title: Option<String>,
    pub content: Option<String>,
    pub content_preview: Option<String>,
    pub importance: Option<Value>,
    pub created_at: Option<Value>,
    pub updated_at: Option<Value>,
    pub is_crystal: Option<bool>,
    pub mention_breadth: u64,
}

impl NowledgeMemGraphCommunityRecentMemoryRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "memory_id": self.memory_id,
            "node_id": self.node_id,
            "label": self.label,
            "title": self.title,
            "content": self.content,
            "content_preview": self.content_preview,
            "importance": self.importance.as_ref().map(nowledge_value_json),
            "created_at": self.created_at.as_ref().map(nowledge_value_json),
            "updated_at": self.updated_at.as_ref().map(nowledge_value_json),
            "is_crystal": self.is_crystal,
            "mention_breadth": self.mention_breadth,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunityRecentMemoriesRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub community_id: i64,
    pub row_count: usize,
    pub read_report: NowledgeMemReadReport,
}

impl NowledgeMemGraphCommunityRecentMemoriesRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "community_id": self.community_id,
            "row_count": self.row_count,
            "read_report": self.read_report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunityRecentMemoriesOutput {
    pub rows: Vec<NowledgeMemGraphCommunityRecentMemoryRow>,
    pub report: NowledgeMemGraphCommunityRecentMemoriesRouteReport,
}

impl NowledgeMemGraphCommunityRecentMemoriesOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "rows": self.rows.iter().map(NowledgeMemGraphCommunityRecentMemoryRow::json).collect::<Vec<_>>(),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemGraphCommunitySubgraphOptions {
    pub community_id: i64,
    pub max_entities: usize,
    pub max_edges: usize,
    pub read_options: NowledgeMemReadOptions,
}

impl NowledgeMemGraphCommunitySubgraphOptions {
    pub fn new(community_id: i64, max_entities: usize, max_edges: usize) -> Self {
        Self {
            community_id,
            max_entities,
            max_edges,
            read_options: NowledgeMemReadOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunitySubgraphEntityRow {
    pub entity_id: Option<String>,
    pub node_id: u64,
    pub label: Option<String>,
    pub name: Option<String>,
    pub entity_type: Option<String>,
    pub confidence: Option<Value>,
    pub mention_count: u64,
}

impl NowledgeMemGraphCommunitySubgraphEntityRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "entity_id": self.entity_id,
            "node_id": self.node_id,
            "label": self.label,
            "name": self.name,
            "entity_type": self.entity_type,
            "confidence": self.confidence.as_ref().map(nowledge_value_json),
            "mention_count": self.mention_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunitySubgraphEdgeRow {
    pub source_entity_id: Option<String>,
    pub target_entity_id: Option<String>,
    pub relationship_id: u64,
    pub confidence: Option<Value>,
    pub relation_type: Option<String>,
}

impl NowledgeMemGraphCommunitySubgraphEdgeRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "source_entity_id": self.source_entity_id,
            "target_entity_id": self.target_entity_id,
            "relationship_id": self.relationship_id,
            "confidence": self.confidence.as_ref().map(nowledge_value_json),
            "relation_type": self.relation_type,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunitySubgraphRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub community_id: i64,
    pub entity_count: usize,
    pub edge_count: usize,
    pub entity_read_report: NowledgeMemReadReport,
    pub edge_read_report: Option<NowledgeMemReadReport>,
}

impl NowledgeMemGraphCommunitySubgraphRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "community_id": self.community_id,
            "entity_count": self.entity_count,
            "edge_count": self.edge_count,
            "entity_read_report": self.entity_read_report.json(),
            "edge_read_report": self.edge_read_report.as_ref().map(NowledgeMemReadReport::json),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphCommunitySubgraphOutput {
    pub entities: Vec<NowledgeMemGraphCommunitySubgraphEntityRow>,
    pub edges: Vec<NowledgeMemGraphCommunitySubgraphEdgeRow>,
    pub report: NowledgeMemGraphCommunitySubgraphRouteReport,
}

impl NowledgeMemGraphCommunitySubgraphOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "entities": self.entities.iter().map(NowledgeMemGraphCommunitySubgraphEntityRow::json).collect::<Vec<_>>(),
            "edges": self.edges.iter().map(NowledgeMemGraphCommunitySubgraphEdgeRow::json).collect::<Vec<_>>(),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NowledgeMemGraphAugmentationStateOptions {
    pub read_options: NowledgeMemReadOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphAugmentationStateRow {
    pub community_detection_applied: Option<bool>,
    pub pagerank_applied: Option<bool>,
    pub community_algorithm: Option<String>,
    pub community_resolution: Option<Value>,
    pub community_count: Option<Value>,
    pub pagerank_algorithm: Option<String>,
    pub pagerank_damping: Option<Value>,
    pub pagerank_iterations: Option<Value>,
    pub last_augmentation_at: Option<Value>,
    pub schema_version: Option<Value>,
    pub community_detection_computed_at: Option<Value>,
    pub pagerank_computed_at: Option<Value>,
}

impl NowledgeMemGraphAugmentationStateRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "community_detection_applied": self.community_detection_applied,
            "pagerank_applied": self.pagerank_applied,
            "community_algorithm": self.community_algorithm,
            "community_resolution": self.community_resolution.as_ref().map(nowledge_value_json),
            "community_count": self.community_count.as_ref().map(nowledge_value_json),
            "pagerank_algorithm": self.pagerank_algorithm,
            "pagerank_damping": self.pagerank_damping.as_ref().map(nowledge_value_json),
            "pagerank_iterations": self.pagerank_iterations.as_ref().map(nowledge_value_json),
            "last_augmentation_at": self.last_augmentation_at.as_ref().map(nowledge_value_json),
            "schema_version": self.schema_version.as_ref().map(nowledge_value_json),
            "community_detection_computed_at": self.community_detection_computed_at.as_ref().map(nowledge_value_json),
            "pagerank_computed_at": self.pagerank_computed_at.as_ref().map(nowledge_value_json),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphAugmentationStateRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub row_count: usize,
    pub read_report: NowledgeMemReadReport,
}

impl NowledgeMemGraphAugmentationStateRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "row_count": self.row_count,
            "read_report": self.read_report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphAugmentationStateOutput {
    pub state: Option<NowledgeMemGraphAugmentationStateRow>,
    pub report: NowledgeMemGraphAugmentationStateRouteReport,
}

impl NowledgeMemGraphAugmentationStateOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "state": self.state.as_ref().map(NowledgeMemGraphAugmentationStateRow::json),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NowledgeMemGraphPageRankPlanOptions {
    pub changed_since_epoch_nanos: Option<i64>,
    pub read_options: NowledgeMemReadOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphPageRankPlanMetaRow {
    pub pagerank_applied: Option<bool>,
    pub pagerank_computed_at: Option<Value>,
}

impl NowledgeMemGraphPageRankPlanMetaRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "pagerank_applied": self.pagerank_applied,
            "pagerank_computed_at": self.pagerank_computed_at.as_ref().map(nowledge_value_json),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphPageRankPlanRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub changed_since_epoch_nanos: Option<i64>,
    pub query_count: usize,
    pub read_reports: Vec<NowledgeMemReadReport>,
}

impl NowledgeMemGraphPageRankPlanRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "changed_since_epoch_nanos": self.changed_since_epoch_nanos,
            "query_count": self.query_count,
            "read_reports": self.read_reports.iter().map(NowledgeMemReadReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphPageRankPlanOutput {
    pub graph_commit_epoch: u64,
    pub graph_meta: Option<NowledgeMemGraphPageRankPlanMetaRow>,
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
    pub report: NowledgeMemGraphPageRankPlanRouteReport,
}

impl NowledgeMemGraphPageRankPlanOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "graph_commit_epoch": self.graph_commit_epoch,
            "graph_meta": self.graph_meta.as_ref().map(NowledgeMemGraphPageRankPlanMetaRow::json),
            "memory_node_count": self.memory_node_count,
            "entity_node_count": self.entity_node_count,
            "entity_relation_count": self.entity_relation_count,
            "mention_edge_count": self.mention_edge_count,
            "active_memory_relation_count": self.active_memory_relation_count,
            "changed_memory_count": self.changed_memory_count,
            "changed_entity_count": self.changed_entity_count,
            "changed_mention_edge_count": self.changed_mention_edge_count,
            "changed_entity_relation_count": self.changed_entity_relation_count,
            "changed_memory_relation_count": self.changed_memory_relation_count,
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemGraphOrphansOptions {
    pub limit: usize,
    pub read_options: NowledgeMemReadOptions,
}

impl Default for NowledgeMemGraphOrphansOptions {
    fn default() -> Self {
        Self {
            limit: 64,
            read_options: NowledgeMemReadOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphOrphanEntityRow {
    pub entity_id: Option<String>,
    pub node_id: u64,
    pub label: Option<String>,
    pub name: Option<String>,
    pub entity_type: Option<String>,
    pub description: Option<String>,
    pub community_id: Option<Value>,
    pub confidence: Option<Value>,
    pub pagerank_score: Option<Value>,
}

impl NowledgeMemGraphOrphanEntityRow {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "entity_id": self.entity_id,
            "node_id": self.node_id,
            "label": self.label,
            "name": self.name,
            "entity_type": self.entity_type,
            "description": self.description,
            "community_id": self.community_id.as_ref().map(nowledge_value_json),
            "confidence": self.confidence.as_ref().map(nowledge_value_json),
            "pagerank_score": self.pagerank_score.as_ref().map(nowledge_value_json),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphOrphansRouteReport {
    pub protocol: String,
    pub route: String,
    pub read_engine: crate::route_ownership::NowledgeMemRouteReadEngine,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub row_count: usize,
    pub read_report: NowledgeMemReadReport,
}

impl NowledgeMemGraphOrphansRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "route": self.route,
            "read_engine": self.read_engine.as_str(),
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "row_count": self.row_count,
            "read_report": self.read_report.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemGraphOrphansOutput {
    pub rows: Vec<NowledgeMemGraphOrphanEntityRow>,
    pub report: NowledgeMemGraphOrphansRouteReport,
}

impl NowledgeMemGraphOrphansOutput {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "rows": self.rows.iter().map(NowledgeMemGraphOrphanEntityRow::json).collect::<Vec<_>>(),
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemQueryExecutionPath {
    FastPath,
    OptimizedPath,
}

impl NowledgeMemQueryExecutionPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FastPath => "fast_path",
            Self::OptimizedPath => "optimized_path",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub statement_kind: String,
    pub execution_path: NowledgeMemQueryExecutionPath,
    pub fast_path_reason: Option<String>,
    pub elapsed_micros: u128,
    pub slow_log_threshold_micros: Option<u128>,
    pub slow_log_candidate: bool,
    pub physical_plan_captured: bool,
    pub plan_cache_lookup: Option<String>,
    pub plan_cache_bypass_reason: Option<String>,
    pub plan_cache_cacheable: bool,
    pub plan_cache_hit: bool,
    pub plan_cache_miss: bool,
    pub plan_cache_bypassed: bool,
    pub physical_operator_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
    pub output_row_shape: NowledgeMemQueryOutputRowShape,
}

impl NowledgeMemQueryReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "statement_kind": self.statement_kind,
            "execution_path": self.execution_path.as_str(),
            "fast_path_reason": self.fast_path_reason,
            "fast_path_selected": self.execution_path == NowledgeMemQueryExecutionPath::FastPath,
            "elapsed_micros": self.elapsed_micros,
            "slow_log_threshold_micros": self.slow_log_threshold_micros,
            "slow_log_candidate": self.slow_log_candidate,
            "physical_plan_captured": self.physical_plan_captured,
            "plan_cache_lookup": self.plan_cache_lookup,
            "plan_cache_bypass_reason": self.plan_cache_bypass_reason,
            "plan_cache_cacheable": self.plan_cache_cacheable,
            "plan_cache_hit": self.plan_cache_hit,
            "plan_cache_miss": self.plan_cache_miss,
            "plan_cache_bypassed": self.plan_cache_bypassed,
            "plan_cache": {
                "lookup": self.plan_cache_lookup,
                "bypass_reason": self.plan_cache_bypass_reason,
                "cacheable": self.plan_cache_cacheable,
                "hit": self.plan_cache_hit,
                "miss": self.plan_cache_miss,
                "bypassed": self.plan_cache_bypassed,
            },
            "physical_operator_counts": self.physical_operator_counts,
            "optimizer_decision_count": self.optimizer_decision_count,
            "scan_pruning_report_count": self.scan_pruning_reports.len(),
            "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
            "output_row_shape": self.output_row_shape.json(),
            "api_behavior": {
                "include_metadata_false_strips_metadata": true,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryOutputRowShape {
    pub row_count: usize,
    pub column_count: usize,
    pub columns: Vec<String>,
}

impl NowledgeMemQueryOutputRowShape {
    fn from_output(output: &QueryOutput) -> Self {
        let columns = output
            .rows
            .iter()
            .flat_map(|row| row.keys().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Self {
            row_count: output.rows.len(),
            column_count: columns.len(),
            columns,
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": self.columns,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemQueryReport,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NowledgeMemQueryReportOptions {
    pub capture_physical_plan: bool,
    pub slow_log_threshold_micros: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSlowQueryRecord {
    pub sequence: u64,
    pub query_language: String,
    pub query_digest: String,
    pub started_unix_micros: i64,
    pub elapsed_micros: i64,
    pub row_count: i64,
    pub success: bool,
    pub slow_log_candidate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSlowQueryReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub present: bool,
    pub ready: bool,
    pub capacity: usize,
    pub threshold_micros: u128,
    pub record_count: usize,
    pub latest_sequence: Option<u64>,
    pub max_elapsed_micros: Option<i64>,
    pub total_row_count: i64,
    pub records: Vec<NowledgeMemSlowQueryRecord>,
}

impl NowledgeMemSlowQueryReport {
    fn from_summaries(
        mode: NowledgeMemGraphMode,
        capacity: usize,
        threshold_micros: u128,
        records: Vec<SlowQueryLogRecordSummary>,
    ) -> Self {
        let records = records
            .into_iter()
            .map(|record| NowledgeMemSlowQueryRecord {
                sequence: record.sequence,
                query_language: record.query_language,
                query_digest: record.query_digest,
                started_unix_micros: record.started_unix_micros,
                elapsed_micros: record.elapsed_micros,
                row_count: record.row_count,
                success: record.success,
                slow_log_candidate: record.slow_log_candidate,
            })
            .collect::<Vec<_>>();
        let latest_sequence = records.iter().map(|record| record.sequence).max();
        let max_elapsed_micros = records.iter().map(|record| record.elapsed_micros).max();
        let total_row_count = records
            .iter()
            .map(|record| record.row_count)
            .fold(0i64, i64::saturating_add);

        Self {
            protocol: NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL.to_string(),
            mode,
            present: true,
            ready: true,
            capacity,
            threshold_micros,
            record_count: records.len(),
            latest_sequence,
            max_elapsed_micros,
            total_row_count,
            records,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "present": self.present,
            "ready": self.ready,
            "capacity": self.capacity,
            "threshold_micros": self.threshold_micros,
            "record_count": self.record_count,
            "latest_sequence": self.latest_sequence,
            "max_elapsed_micros": self.max_elapsed_micros,
            "total_row_count": self.total_row_count,
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
            "records": self.records.iter().map(|record| {
                serde_json::json!({
                    "sequence": record.sequence,
                    "query_language": record.query_language,
                    "query_digest": record.query_digest,
                    "started_unix_micros": record.started_unix_micros,
                    "elapsed_micros": record.elapsed_micros,
                    "row_count": record.row_count,
                    "success": record.success,
                    "slow_log_candidate": record.slow_log_candidate,
                })
            }).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NowledgeMemReadinessOptions {
    pub bounded_read_probe: Option<NowledgeGraphStatement>,
    pub bounded_read_evidence: Option<serde_json::Value>,
    pub covered_routes: Vec<String>,
    pub graph_route_readiness: Option<NowledgeMemRouteReadinessSummary>,
    pub replacement_readiness_by_query_family: Option<serde_json::Value>,
    pub read_options: NowledgeMemReadOptions,
    pub search_projection_evidence: Option<serde_json::Value>,
    pub search_projection_probe_options: SearchProjectionProbeOptions,
    pub primary_search_projection_probe: Option<serde_json::Value>,
    pub search_projection_shadow_evidence: Option<serde_json::Value>,
    pub search_candidate_shadow_evidence: Option<serde_json::Value>,
    pub workload_fixture_evidence: Option<NowledgeGraphRouteWorkloadFixtureReport>,
    pub qos_policy: LocalQosPolicy,
    pub qos_state: LocalQosState,
    pub background_maintenance_options: BackgroundMaintenanceOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemLibraryReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub mode: NowledgeMemGraphMode,
    pub redaction: NowledgeMemReadinessRedactionSummary,
    pub production_path: NowledgeMemLibraryProductionPathSummary,
    pub blocker_codes: Vec<String>,
    pub readiness_by_area: NowledgeMemReadinessAreaMap,
    pub ready_area_count: usize,
    pub blocked_area_count: usize,
    pub graph_open: bool,
    pub graph_read_only: bool,
    pub graph_route_readiness: serde_json::Value,
    pub bounded_read_evidence: serde_json::Value,
    pub storage_recovery: serde_json::Value,
    pub background_maintenance: serde_json::Value,
    pub query_family_evidence: serde_json::Value,
    pub search_projection_evidence: serde_json::Value,
    pub search_projection_shadow_evidence: serde_json::Value,
    pub search_candidate_shadow_evidence: serde_json::Value,
    pub workload_fixture_evidence: serde_json::Value,
}

impl NowledgeMemLibraryReadinessReport {
    pub fn areas(&self) -> Vec<NowledgeMemReadinessAreaSummary> {
        self.readiness_by_area.areas()
    }

    pub fn json(&self) -> serde_json::Value {
        let areas = self.areas();
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "redaction": self.redaction.json(),
            "production_path": self.production_path.json(),
            "blocker_codes": self.blocker_codes,
            "readiness_by_area": self.readiness_by_area.json(),
            "areas": areas.iter().map(NowledgeMemReadinessAreaSummary::json).collect::<Vec<_>>(),
            "ready_area_count": self.ready_area_count,
            "blocked_area_count": self.blocked_area_count,
            "graph": {
                "open": self.graph_open,
                "mode": self.mode.as_str(),
                "read_only": self.graph_read_only,
            },
            "graph_route_readiness": self.graph_route_readiness,
            "bounded_read_evidence": self.bounded_read_evidence,
            "storage_recovery": self.storage_recovery,
            "background_maintenance": self.background_maintenance,
            "query_family_evidence": self.query_family_evidence,
            "search_projection_evidence": self.search_projection_evidence,
            "search_projection_shadow_evidence": self.search_projection_shadow_evidence,
            "search_candidate_shadow_evidence": self.search_candidate_shadow_evidence,
            "workload_fixture_evidence": self.workload_fixture_evidence,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemLibraryProductionPathSummary {
    pub in_process: bool,
    pub cli_required: bool,
    pub env_control_plane_required: bool,
    pub spawned_helper_required: bool,
}

impl Default for NowledgeMemLibraryProductionPathSummary {
    fn default() -> Self {
        Self {
            in_process: true,
            cli_required: false,
            env_control_plane_required: false,
            spawned_helper_required: false,
        }
    }
}

impl NowledgeMemLibraryProductionPathSummary {
    pub fn ready(&self) -> bool {
        self.in_process
            && !self.cli_required
            && !self.env_control_plane_required
            && !self.spawned_helper_required
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "in_process": self.in_process,
            "cli_required": self.cli_required,
            "env_control_plane_required": self.env_control_plane_required,
            "spawned_helper_required": self.spawned_helper_required,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NowledgeMemReadinessRedactionSummary {
    pub query_text_copied: bool,
    pub parameters_copied: bool,
    pub local_paths_copied: bool,
}

impl NowledgeMemReadinessRedactionSummary {
    pub fn ready(&self) -> bool {
        !self.query_text_copied && !self.parameters_copied && !self.local_paths_copied
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "query_text_copied": self.query_text_copied,
            "parameters_copied": self.parameters_copied,
            "local_paths_copied": self.local_paths_copied,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadinessAreaMap {
    pub graph: NowledgeMemReadinessAreaSummary,
    pub query: NowledgeMemReadinessAreaSummary,
    pub query_family: NowledgeMemReadinessAreaSummary,
    pub graph_route: NowledgeMemReadinessAreaSummary,
    pub storage: NowledgeMemReadinessAreaSummary,
    pub search_projection: NowledgeMemReadinessAreaSummary,
    pub search_projection_shadow: NowledgeMemReadinessAreaSummary,
    pub search_candidate_shadow: NowledgeMemReadinessAreaSummary,
    pub workload_fixture: NowledgeMemReadinessAreaSummary,
    pub background: NowledgeMemReadinessAreaSummary,
}

impl NowledgeMemReadinessAreaMap {
    pub fn areas(&self) -> Vec<NowledgeMemReadinessAreaSummary> {
        vec![
            self.graph.clone(),
            self.query.clone(),
            self.query_family.clone(),
            self.graph_route.clone(),
            self.storage.clone(),
            self.search_projection.clone(),
            self.search_projection_shadow.clone(),
            self.search_candidate_shadow.clone(),
            self.workload_fixture.clone(),
            self.background.clone(),
        ]
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "graph": self.graph.state_json(),
            "query": self.query.state_json(),
            "storage": self.storage.state_json(),
            "background": self.background.state_json(),
            "query_family": self.query_family.state_json(),
            "graph_route": self.graph_route.state_json(),
            "search_projection": self.search_projection.state_json(),
            "search_projection_shadow": self.search_projection_shadow.state_json(),
            "search_candidate_shadow": self.search_candidate_shadow.state_json(),
            "workload_fixture": self.workload_fixture.state_json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadinessAreaSummary {
    pub name: String,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemReadinessAreaSummary {
    fn new(name: impl Into<String>, ready: bool, blocker_codes: impl Into<Vec<String>>) -> Self {
        Self {
            name: name.into(),
            ready,
            blocker_codes: blocker_codes.into(),
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
        })
    }

    fn state_json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadinessDashboard {
    pub protocol: String,
    pub ready: bool,
    pub mode: NowledgeMemGraphMode,
    pub area_count: usize,
    pub ready_area_count: usize,
    pub blocked_area_count: usize,
    pub blocker_codes: Vec<String>,
    pub areas: Vec<NowledgeMemReadinessAreaSummary>,
    pub slow_query_ready: bool,
    pub slow_query_record_count: usize,
}

impl NowledgeMemReadinessDashboard {
    fn from_reports(
        library: &NowledgeMemLibraryReadinessReport,
        slow_query: &NowledgeMemSlowQueryReport,
    ) -> Self {
        let areas = nowledge_mem_readiness_dashboard_areas(library, slow_query);
        let blocked_area_count = areas.iter().filter(|area| !area.ready).count();
        let ready_area_count = areas.len().saturating_sub(blocked_area_count);

        Self {
            protocol: NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL.to_string(),
            ready: library.ready && slow_query.ready,
            mode: library.mode,
            area_count: areas.len(),
            ready_area_count,
            blocked_area_count,
            blocker_codes: library.blocker_codes.clone(),
            areas,
            slow_query_ready: slow_query.ready,
            slow_query_record_count: slow_query.record_count,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "area_count": self.area_count,
            "ready_area_count": self.ready_area_count,
            "blocked_area_count": self.blocked_area_count,
            "blocker_codes": self.blocker_codes,
            "areas": self.areas.iter().map(NowledgeMemReadinessAreaSummary::json).collect::<Vec<_>>(),
            "slow_query": {
                "ready": self.slow_query_ready,
                "record_count": self.slow_query_record_count,
            },
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemStorageRecoveryReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub durable: bool,
    pub recovery_mode: RecoveryMode,
    pub checkpoint_epoch: Option<u64>,
    pub checkpoint_commit_epoch: Option<u64>,
    pub wal_present: bool,
    pub wal_replay_start_lsn: Option<u64>,
    pub next_lsn_after_replay: Option<u64>,
    pub replayed_wal_entries: usize,
    pub max_wal_replay_entries: Option<usize>,
    pub torn_tail_ignored: bool,
    pub torn_tail_reason: Option<String>,
    pub recovered_commit_epoch: u64,
    pub durable_recovery_observed: bool,
    pub checkpoint_boundary_present: bool,
    pub wal_replay_bounded: bool,
    pub replay_boundary_consistent: bool,
    pub torn_tail_clean: bool,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemStorageRecoveryReport {
    pub fn from_storage_report(report: &StorageRecoveryReport) -> Self {
        let durable_recovery_observed = report.durable;
        let checkpoint_boundary_present =
            report.checkpoint_epoch.is_some() && report.checkpoint_commit_epoch.is_some();
        let wal_replay_bounded = report
            .max_wal_replay_entries
            .is_some_and(|limit| report.replayed_wal_entries <= limit);
        let replay_boundary_consistent = storage_recovery_replay_boundary_consistent(report);
        let torn_tail_clean = !report.torn_tail_ignored && report.torn_tail_reason.is_none();
        let mut blocker_codes = Vec::new();
        if !durable_recovery_observed {
            blocker_codes.push("durable_recovery_not_observed".to_string());
        }
        if !checkpoint_boundary_present {
            blocker_codes.push("checkpoint_boundary_missing".to_string());
        }
        if !wal_replay_bounded {
            blocker_codes.push("wal_replay_unbounded".to_string());
        }
        if !replay_boundary_consistent {
            blocker_codes.push("replay_boundary_inconsistent".to_string());
        }
        if !torn_tail_clean {
            blocker_codes.push("torn_tail_observed".to_string());
        }

        Self {
            protocol: "skein-storage-recovery-report".to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            durable: report.durable,
            recovery_mode: report.recovery_mode,
            checkpoint_epoch: report.checkpoint_epoch,
            checkpoint_commit_epoch: report.checkpoint_commit_epoch,
            wal_present: report.wal_present,
            wal_replay_start_lsn: report.wal_replay_start_lsn,
            next_lsn_after_replay: report.next_lsn_after_replay,
            replayed_wal_entries: report.replayed_wal_entries,
            max_wal_replay_entries: report.max_wal_replay_entries,
            torn_tail_ignored: report.torn_tail_ignored,
            torn_tail_reason: report.torn_tail_reason.clone(),
            recovered_commit_epoch: report.recovered_commit_epoch,
            durable_recovery_observed,
            checkpoint_boundary_present,
            wal_replay_bounded,
            replay_boundary_consistent,
            torn_tail_clean,
            blocker_codes,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "durable": self.durable,
            "recovery_mode": recovery_mode_name(self.recovery_mode),
            "checkpoint_epoch": self.checkpoint_epoch,
            "checkpoint_commit_epoch": self.checkpoint_commit_epoch,
            "wal_present": self.wal_present,
            "wal_replay_start_lsn": self.wal_replay_start_lsn,
            "next_lsn_after_replay": self.next_lsn_after_replay,
            "replayed_wal_entries": self.replayed_wal_entries,
            "max_wal_replay_entries": self.max_wal_replay_entries,
            "torn_tail_ignored": self.torn_tail_ignored,
            "torn_tail_reason": self.torn_tail_reason,
            "recovered_commit_epoch": self.recovered_commit_epoch,
            "readiness": {
                "durable_recovery_observed": self.durable_recovery_observed,
                "checkpoint_boundary_present": self.checkpoint_boundary_present,
                "wal_replay_bounded": self.wal_replay_bounded,
                "replay_boundary_consistent": self.replay_boundary_consistent,
                "torn_tail_clean": self.torn_tail_clean,
            },
            "blocker_codes": self.blocker_codes,
        })
    }
}

fn storage_recovery_replay_boundary_consistent(report: &StorageRecoveryReport) -> bool {
    let Some(checkpoint_commit_epoch) = report.checkpoint_commit_epoch else {
        return false;
    };
    let Some(wal_replay_start_lsn) = report.wal_replay_start_lsn else {
        return false;
    };
    let Some(next_lsn_after_replay) = report.next_lsn_after_replay else {
        return false;
    };
    let Ok(replayed_wal_entries) = u64::try_from(report.replayed_wal_entries) else {
        return false;
    };
    checkpoint_commit_epoch <= report.recovered_commit_epoch
        && wal_replay_start_lsn.checked_add(replayed_wal_entries) == Some(next_lsn_after_replay)
        && checkpoint_commit_epoch.checked_add(replayed_wal_entries)
            == Some(report.recovered_commit_epoch)
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemBackgroundMaintenanceReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub total_candidates: usize,
    pub admitted_count: usize,
    pub deferred_count: usize,
    pub rejected_count: usize,
    pub total_estimated_operations: usize,
    pub admitted_estimated_operations: usize,
    pub deferred_estimated_operations: usize,
    pub rejected_estimated_operations: usize,
    pub executable_search_projection_graph_delta_count: usize,
    pub admitted_search_projection_graph_delta_count: usize,
    pub deferred_search_projection_graph_delta_count: usize,
    pub rejected_search_projection_graph_delta_count: usize,
    pub executable_search_projection_graph_delta_operations: usize,
    pub admitted_search_projection_graph_delta_operations: usize,
    pub max_search_projection_graph_delta_complete_through_graph_commit_epoch: Option<u64>,
    pub foreground_admission_probe_ready: Option<bool>,
    pub foreground_admission_probe_admission_name: Option<String>,
    pub memory_pressure_ready: Option<bool>,
    pub memory_budget_bytes: Option<u64>,
    pub estimated_memory_bytes: Option<u64>,
    pub slow_query_ready: Option<bool>,
    pub slow_query_record_count: Option<u64>,
    pub slow_query_capacity: Option<u64>,
    pub slow_query_redaction_ready: Option<bool>,
    pub top_admitted_kind: Option<BackgroundMaintenanceKind>,
    pub top_admitted_name: Option<String>,
    pub ranked_count: u64,
    pub foreground_ranked_count: u64,
    pub unknown_admission_count: u64,
    pub blocker_codes: Vec<String>,
    summary: serde_json::Value,
}

impl NowledgeMemBackgroundMaintenanceReport {
    pub fn from_summary(summary: &BackgroundMaintenanceSummary) -> Self {
        Self::from_summary_with_slow_query(summary, None)
    }

    fn from_summary_with_slow_query(
        summary: &BackgroundMaintenanceSummary,
        slow_query: Option<&NowledgeMemSlowQueryReport>,
    ) -> Self {
        let mut json = background_maintenance_summary_to_json(summary);
        if let Some(object) = json.as_object_mut() {
            object.insert(
                "protocol".to_string(),
                serde_json::Value::String("skein-background-maintenance-report".to_string()),
            );
            if let Some(slow_query) = slow_query {
                object.insert(
                    "slow_query".to_string(),
                    background_maintenance_slow_query_json(slow_query),
                );
                object.insert(
                    "slow_query_ready".to_string(),
                    serde_json::Value::Bool(slow_query.ready),
                );
                object.insert(
                    "slow_query_record_count".to_string(),
                    serde_json::json!(slow_query.record_count),
                );
                object.insert(
                    "slow_query_capacity".to_string(),
                    serde_json::json!(slow_query.capacity),
                );
            }
        }
        let health = background_maintenance_evidence_health(Some(&json), true);

        Self {
            protocol: "skein-background-maintenance-report".to_string(),
            present: true,
            ready: health.ready,
            total_candidates: summary.total_candidates,
            admitted_count: summary.admitted_count,
            deferred_count: summary.deferred_count,
            rejected_count: summary.rejected_count,
            total_estimated_operations: summary.total_estimated_operations,
            admitted_estimated_operations: summary.admitted_estimated_operations,
            deferred_estimated_operations: summary.deferred_estimated_operations,
            rejected_estimated_operations: summary.rejected_estimated_operations,
            executable_search_projection_graph_delta_count: summary
                .executable_search_projection_graph_delta_count,
            admitted_search_projection_graph_delta_count: summary
                .admitted_search_projection_graph_delta_count,
            deferred_search_projection_graph_delta_count: summary
                .deferred_search_projection_graph_delta_count,
            rejected_search_projection_graph_delta_count: summary
                .rejected_search_projection_graph_delta_count,
            executable_search_projection_graph_delta_operations: summary
                .executable_search_projection_graph_delta_operations,
            admitted_search_projection_graph_delta_operations: summary
                .admitted_search_projection_graph_delta_operations,
            max_search_projection_graph_delta_complete_through_graph_commit_epoch: summary
                .max_search_projection_graph_delta_complete_through_graph_commit_epoch,
            foreground_admission_probe_ready: health.foreground_admission_probe_ready,
            foreground_admission_probe_admission_name: health
                .foreground_admission_probe_admission_name,
            memory_pressure_ready: health.memory_pressure_ready,
            memory_budget_bytes: health.memory_budget_bytes,
            estimated_memory_bytes: health.estimated_memory_bytes,
            slow_query_ready: health.slow_query_ready,
            slow_query_record_count: health.slow_query_record_count,
            slow_query_capacity: health.slow_query_capacity,
            slow_query_redaction_ready: health.slow_query_redaction_ready,
            top_admitted_kind: summary.top_admitted_kind,
            top_admitted_name: summary.top_admitted_name.clone(),
            ranked_count: health.ranked_count.unwrap_or_default(),
            foreground_ranked_count: health.foreground_ranked_count,
            unknown_admission_count: health.unknown_admission_count,
            blocker_codes: health.blocker_codes,
            summary: json,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        self.summary.clone()
    }
}

fn background_maintenance_slow_query_json(
    slow_query: &NowledgeMemSlowQueryReport,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": slow_query.protocol,
        "ready": slow_query.ready,
        "capacity": slow_query.capacity,
        "record_count": slow_query.record_count,
        "latest_sequence": slow_query.latest_sequence,
        "max_elapsed_micros": slow_query.max_elapsed_micros,
        "redaction": {
            "query_text_copied": false,
            "parameters_copied": false,
            "local_paths_copied": false,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRetrievalReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_commit_lag: u64,
    pub projection_stale: bool,
    pub search_document_count: usize,
    pub search_filtered_document_count: usize,
    pub search_total_hits: usize,
    pub candidate_count: usize,
    pub candidate_total_count: usize,
    pub evidence_count: usize,
    pub graph_seed_count: usize,
    pub graph_context_path_count: usize,
    pub search_backend: Option<String>,
    pub vector_backend: Option<String>,
    pub text_backend: Option<String>,
    pub search_fallback_reason_codes: Vec<String>,
    pub retriever_fallback_reason_codes: Vec<String>,
    pub knowledge_fallback_reason_codes: Vec<String>,
    pub truncation_reason_codes: Vec<String>,
    pub warning_count: usize,
    pub warnings: Vec<String>,
}

impl NowledgeMemRetrievalReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "graph_commit_epoch": self.graph_commit_epoch,
            "projection_source_graph_commit_epoch": self.projection_source_graph_commit_epoch,
            "projection_commit_lag": self.projection_commit_lag,
            "projection_stale": self.projection_stale,
            "search_document_count": self.search_document_count,
            "search_filtered_document_count": self.search_filtered_document_count,
            "search_total_hits": self.search_total_hits,
            "candidate_count": self.candidate_count,
            "candidate_total_count": self.candidate_total_count,
            "evidence_count": self.evidence_count,
            "graph_seed_count": self.graph_seed_count,
            "graph_context_path_count": self.graph_context_path_count,
            "search_backend": self.search_backend,
            "vector_backend": self.vector_backend,
            "text_backend": self.text_backend,
            "search_fallback_reason_codes": self.search_fallback_reason_codes,
            "retriever_fallback_reason_codes": self.retriever_fallback_reason_codes,
            "knowledge_fallback_reason_codes": self.knowledge_fallback_reason_codes,
            "truncation_reason_codes": self.truncation_reason_codes,
            "warning_count": self.warning_count,
            "warnings": self.warnings,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemRetrievalOutput {
    pub output: KnowledgeRetrievalOutput,
    pub report: NowledgeMemRetrievalReport,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateRequest {
    pub query_text: String,
    pub query_embedding: Option<Vec<f32>>,
    pub mode: SearchMode,
    pub limit: usize,
    pub rank_window: Option<usize>,
    pub fusion_weights: SearchFusionWeights,
    pub metadata_filters: BTreeMap<String, String>,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
}

impl NowledgeMemSearchCandidateRequest {
    pub fn text(query_text: impl Into<String>, limit: usize) -> Self {
        Self {
            query_text: query_text.into(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn vector(query_embedding: Vec<f32>, limit: usize) -> Self {
        Self {
            query_text: String::new(),
            query_embedding: Some(query_embedding),
            mode: SearchMode::Vector,
            limit,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn hybrid(query_text: impl Into<String>, query_embedding: Vec<f32>, limit: usize) -> Self {
        Self {
            query_text: query_text.into(),
            query_embedding: Some(query_embedding),
            mode: SearchMode::Hybrid,
            limit,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn with_rank_window(mut self, rank_window: Option<usize>) -> Self {
        self.rank_window = rank_window;
        self
    }

    pub fn with_fusion_weights(mut self, fusion_weights: SearchFusionWeights) -> Self {
        self.fusion_weights = fusion_weights;
        self
    }

    pub fn with_metadata_filters(mut self, metadata_filters: BTreeMap<String, String>) -> Self {
        self.metadata_filters = metadata_filters;
        self
    }

    pub fn with_compressed_vector_search_mode(
        mut self,
        compressed_vector_search_mode: CompressedVectorSearchMode,
    ) -> Self {
        self.compressed_vector_search_mode = compressed_vector_search_mode;
        self
    }

    pub fn with_retrieval_projection_advisor(
        mut self,
        advisor: NowledgeMemRetrievalProjectionAdvisor,
    ) -> Self {
        self.retrieval_projection_advisor = advisor;
        self
    }

    fn effective_compressed_vector_search_mode(&self) -> CompressedVectorSearchMode {
        advised_compressed_vector_search_mode(
            self.compressed_vector_search_mode,
            &self.retrieval_projection_advisor,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateReport {
    pub protocol: String,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub requested_compressed_vector_search_mode: CompressedVectorSearchMode,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
    pub retrieval_projection_advisor_blocker_codes: Vec<String>,
    pub mode: SearchMode,
    pub query_embedding_dimension: Option<usize>,
    pub limit: usize,
    pub rank_window: Option<usize>,
    pub document_count: usize,
    pub filtered_document_count: usize,
    pub total_hits: usize,
    pub returned_hit_count: usize,
    pub returned_kind_counts: BTreeMap<String, usize>,
    pub returned_missing_external_id_count: usize,
    pub returned_missing_source_id_count: usize,
    pub truncated: bool,
    pub candidate_set: SearchCandidateSetReport,
    pub filtered_out_count: usize,
    pub metadata_filter_count: usize,
    pub pushed_predicate_count: usize,
    pub residual_predicate_count: usize,
    pub segment_count: usize,
    pub pruned_segment_count: usize,
    pub scanned_segment_count: usize,
    pub segment_pruning_candidate_document_count: usize,
    pub segment_pruned_document_count: usize,
    pub segment_scanned_document_count: usize,
    pub persisted_segment_descriptor_used: bool,
    pub retriever_backends: BTreeMap<String, String>,
    pub retriever_available: BTreeMap<String, bool>,
    pub retriever_candidate_counts: BTreeMap<String, usize>,
    pub fallback_reason_codes: Vec<String>,
    pub empty_reason_codes: Vec<String>,
    pub truncation_reason_codes: Vec<String>,
    pub projection_full_reindex_needed: bool,
    pub projection_metadata_repair_needed: bool,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_embedding_model: Option<String>,
    pub projection_embedding_version: Option<String>,
    pub projection_embedding_dimension: Option<usize>,
}

impl NowledgeMemSearchCandidateReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "requested_compressed_vector_search_mode": self.requested_compressed_vector_search_mode.as_str(),
            "retrieval_projection_advisor": self.retrieval_projection_advisor.json(),
            "retrieval_projection_advisor_blocker_codes": self.retrieval_projection_advisor_blocker_codes,
            "mode": search_mode_name(self.mode),
            "query_embedding_dimension": self.query_embedding_dimension,
            "limit": self.limit,
            "rank_window": self.rank_window,
            "document_count": self.document_count,
            "filtered_document_count": self.filtered_document_count,
            "total_hits": self.total_hits,
            "returned_hit_count": self.returned_hit_count,
            "returned_kind_counts": self.returned_kind_counts,
            "returned_missing_external_id_count": self.returned_missing_external_id_count,
            "returned_missing_source_id_count": self.returned_missing_source_id_count,
            "truncated": self.truncated,
            "candidate_set": search_candidate_set_report_json(&self.candidate_set),
            "filtered_out_count": self.filtered_out_count,
            "metadata_filter_count": self.metadata_filter_count,
            "pushed_predicate_count": self.pushed_predicate_count,
            "residual_predicate_count": self.residual_predicate_count,
            "segment_count": self.segment_count,
            "pruned_segment_count": self.pruned_segment_count,
            "scanned_segment_count": self.scanned_segment_count,
            "segment_pruning_candidate_document_count": self.segment_pruning_candidate_document_count,
            "segment_pruned_document_count": self.segment_pruned_document_count,
            "segment_scanned_document_count": self.segment_scanned_document_count,
            "persisted_segment_descriptor_used": self.persisted_segment_descriptor_used,
            "retriever_backends": self.retriever_backends,
            "retriever_available": self.retriever_available,
            "retriever_candidate_counts": self.retriever_candidate_counts,
            "fallback_reason_codes": self.fallback_reason_codes,
            "empty_reason_codes": self.empty_reason_codes,
            "truncation_reason_codes": self.truncation_reason_codes,
            "projection_full_reindex_needed": self.projection_full_reindex_needed,
            "projection_metadata_repair_needed": self.projection_metadata_repair_needed,
            "projection_source_graph_commit_epoch": self.projection_source_graph_commit_epoch,
            "projection_embedding_model": self.projection_embedding_model,
            "projection_embedding_version": self.projection_embedding_version,
            "projection_embedding_dimension": self.projection_embedding_dimension,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateOutput {
    pub result: SearchResultSet,
    pub report: NowledgeMemSearchCandidateReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateReadinessOptions {
    pub require_hits: bool,
    pub require_metadata_pushdown: bool,
    pub require_segment_descriptor: bool,
    pub require_text_retriever: bool,
    pub require_vector_retriever: bool,
    pub require_source_chunk_identity: bool,
    pub require_fail_soft_observation: bool,
    pub require_projection_marker_status: bool,
    pub require_projection_watermark: bool,
    pub require_embedding_identity: bool,
    pub active_embedding_model: Option<String>,
    pub active_embedding_dimension: Option<usize>,
}

impl Default for NowledgeMemSearchCandidateReadinessOptions {
    fn default() -> Self {
        Self {
            require_hits: true,
            require_metadata_pushdown: false,
            require_segment_descriptor: false,
            require_text_retriever: false,
            require_vector_retriever: false,
            require_source_chunk_identity: false,
            require_fail_soft_observation: false,
            require_projection_marker_status: true,
            require_projection_watermark: false,
            require_embedding_identity: false,
            active_embedding_model: None,
            active_embedding_dimension: None,
        }
    }
}

impl NowledgeMemSearchCandidateReadinessOptions {
    pub fn lancedb_replacement_candidate_read() -> Self {
        Self {
            require_metadata_pushdown: true,
            require_segment_descriptor: true,
            require_projection_marker_status: true,
            require_projection_watermark: true,
            require_embedding_identity: true,
            ..Self::default()
        }
    }

    pub fn with_source_chunk_identity(mut self, required: bool) -> Self {
        self.require_source_chunk_identity = required;
        self
    }

    pub fn with_vector_retriever(mut self, required: bool) -> Self {
        self.require_vector_retriever = required;
        self
    }

    pub fn with_text_retriever(mut self, required: bool) -> Self {
        self.require_text_retriever = required;
        self
    }

    pub fn with_fail_soft_observation(mut self, required: bool) -> Self {
        self.require_fail_soft_observation = required;
        self
    }

    pub fn with_projection_watermark(mut self, required: bool) -> Self {
        self.require_projection_watermark = required;
        self
    }

    pub fn with_embedding_identity(
        mut self,
        active_model: impl Into<String>,
        active_dimension: usize,
    ) -> Self {
        self.require_embedding_identity = true;
        self.active_embedding_model = Some(active_model.into());
        self.active_embedding_dimension = Some(active_dimension);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub mode: SearchMode,
    pub blocker_codes: Vec<String>,
    pub candidate_report: NowledgeMemSearchCandidateReport,
    pub metadata_pushdown_ready: bool,
    pub segment_descriptor_ready: bool,
    pub text_retriever_ready: bool,
    pub vector_retriever_ready: bool,
    pub source_chunk_identity_ready: bool,
    pub fail_soft_observed: bool,
    pub projection_marker_status_visible: bool,
    pub projection_watermark_ready: bool,
    pub embedding_identity_ready: bool,
}

impl NowledgeMemSearchCandidateReadinessReport {
    pub fn from_candidate_report(
        candidate_report: NowledgeMemSearchCandidateReport,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> Self {
        let metadata_pushdown_ready = candidate_report.metadata_filter_count > 0
            && candidate_report.pushed_predicate_count >= candidate_report.metadata_filter_count
            && candidate_report.residual_predicate_count == 0;
        let segment_descriptor_ready = candidate_report.persisted_segment_descriptor_used;
        let text_retriever_ready = candidate_report
            .retriever_available
            .get("text")
            .copied()
            .unwrap_or(false);
        let vector_retriever_ready = candidate_report
            .retriever_available
            .get("vector")
            .copied()
            .unwrap_or(false);
        let source_chunk_identity_ready = candidate_report
            .returned_kind_counts
            .get("source_chunk")
            .copied()
            .unwrap_or_default()
            > 0
            && candidate_report.returned_missing_external_id_count == 0
            && candidate_report.returned_missing_source_id_count == 0;
        let fail_soft_observed = !candidate_report.fallback_reason_codes.is_empty()
            && candidate_report.returned_hit_count > 0;
        let projection_marker_status_visible =
            candidate_report.protocol == NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL;
        let projection_watermark_ready = candidate_report
            .projection_source_graph_commit_epoch
            .is_some();
        let embedding_identity_ready =
            search_candidate_embedding_identity_ready(&candidate_report, options);
        let blocker_codes = search_candidate_readiness_blocker_codes(
            &candidate_report,
            options,
            metadata_pushdown_ready,
            segment_descriptor_ready,
            text_retriever_ready,
            vector_retriever_ready,
            source_chunk_identity_ready,
            fail_soft_observed,
            projection_marker_status_visible,
            projection_watermark_ready,
            embedding_identity_ready,
        );

        Self {
            protocol: NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL.to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            mode: candidate_report.mode,
            blocker_codes,
            candidate_report,
            metadata_pushdown_ready,
            segment_descriptor_ready,
            text_retriever_ready,
            vector_retriever_ready,
            source_chunk_identity_ready,
            fail_soft_observed,
            projection_marker_status_visible,
            projection_watermark_ready,
            embedding_identity_ready,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "mode": search_mode_name(self.mode),
            "blocker_codes": self.blocker_codes,
            "candidate_report": self.candidate_report.json(),
            "metadata_pushdown_ready": self.metadata_pushdown_ready,
            "segment_descriptor_ready": self.segment_descriptor_ready,
            "text_retriever_ready": self.text_retriever_ready,
            "vector_retriever_ready": self.vector_retriever_ready,
            "source_chunk_identity_ready": self.source_chunk_identity_ready,
            "fail_soft_observed": self.fail_soft_observed,
            "projection_marker_status_visible": self.projection_marker_status_visible,
            "projection_watermark_ready": self.projection_watermark_ready,
            "embedding_identity_ready": self.embedding_identity_ready,
        })
    }
}

impl NowledgeMemSearchCandidateOutput {
    pub fn readiness_report(
        &self,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> NowledgeMemSearchCandidateReadinessReport {
        NowledgeMemSearchCandidateReadinessReport::from_candidate_report(
            self.report.clone(),
            options,
        )
    }
}

impl NowledgeMemGraph {
    pub fn open(path: impl AsRef<Path>, mode: NowledgeMemGraphMode) -> Result<Self> {
        let db = Database::open_with_config(path, nowledge_mem_graph_config(mode))?;
        Ok(Self { db, mode })
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        let mode = if config.read_only {
            NowledgeMemGraphMode::ShadowReadOnly
        } else {
            NowledgeMemGraphMode::WritableCutover
        };
        let db = Database::open_with_config(path, config)?;
        Ok(Self { db, mode })
    }

    pub fn from_database(db: Database, mode: NowledgeMemGraphMode) -> Self {
        Self { db, mode }
    }

    pub fn mode(&self) -> NowledgeMemGraphMode {
        self.mode
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn database_mut(&mut self) -> &mut Database {
        &mut self.db
    }

    pub fn into_database(self) -> Database {
        self.db
    }

    pub fn query(&mut self, cypher: &str) -> Result<QueryOutput> {
        self.db.query(cypher)
    }

    pub fn query_with_report(&mut self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report(cypher, &BTreeMap::new())
    }

    pub fn query_with_report_options(
        &mut self,
        cypher: &str,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(cypher, &BTreeMap::new(), options)
    }

    pub fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.db.query_with_params(cypher, parameters)
    }

    pub fn query_with_params_with_report(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(
            cypher,
            parameters,
            NowledgeMemQueryReportOptions::default(),
        )
    }

    pub fn query_with_params_with_report_options(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        let started = Instant::now();
        let (output, execution_trace) =
            self.db
                .query_with_params_trace(cypher, parameters, options.capture_physical_plan)?;
        let elapsed_micros = started.elapsed().as_micros();
        let report = nowledge_mem_query_report(NowledgeMemQueryReportInput {
            mode: self.mode,
            statement: &execution_trace.statement,
            trace: execution_trace.optimizer_trace.as_ref(),
            plan_cache_lookup: execution_trace.plan_cache_lookup,
            execution_profile: execution_trace.execution_profile.as_ref(),
            output: &output,
            options,
            elapsed_micros,
        });
        Ok(NowledgeMemQueryOutput { output, report })
    }

    pub fn slow_query_report(&self) -> NowledgeMemSlowQueryReport {
        let config = self.db.config();
        NowledgeMemSlowQueryReport::from_summaries(
            self.mode,
            config.slow_query_log_capacity,
            config.slow_query_log_threshold_micros,
            self.db.slow_query_log_snapshot(),
        )
    }

    pub fn slow_query_report_json(&self) -> serde_json::Value {
        self.slow_query_report().json()
    }

    pub fn read_query(&mut self, cypher: &str) -> Result<NowledgeMemReadOutput> {
        self.read_query_with_params(cypher, &BTreeMap::new(), &NowledgeMemReadOptions::default())
    }

    pub fn read_query_with_options(
        &mut self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_query_with_params(cypher, &BTreeMap::new(), options)
    }

    pub fn read_query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        let bounded = self
            .db
            .begin_read_transaction()
            .query_with_params_bounded_profile(cypher, parameters, options.max_rows)?;
        let report = nowledge_mem_read_report(
            self.mode,
            &bounded.output,
            options,
            &bounded.execution_profile,
        );
        if report.row_budget_exceeded {
            return Err(SkeinError::Execution(format!(
                "nowledge mem read query returned {} rows, exceeding max_rows {}",
                report.row_count,
                report.max_rows.unwrap_or_default()
            )));
        }
        if report.payload_budget_exceeded {
            return Err(SkeinError::Execution(format!(
                "nowledge mem read query estimated {} payload bytes, exceeding max_estimated_payload_bytes {}",
                report.estimated_payload_bytes,
                report.max_estimated_payload_bytes.unwrap_or_default()
            )));
        }
        Ok(NowledgeMemReadOutput {
            output: bounded.output,
            report,
        })
    }

    pub fn read_graph_overview(
        &mut self,
        options: &NowledgeMemGraphOverviewOptions,
    ) -> Result<NowledgeMemGraphOverviewOutput> {
        let limit = i64::try_from(options.limit).map_err(|_| {
            SkeinError::Semantic("graph overview limit exceeds supported range".to_string())
        })?;
        if limit <= 0 {
            return Err(SkeinError::Semantic(
                "graph overview limit must be greater than zero".to_string(),
            ));
        }
        let mut parameters = BTreeMap::new();
        parameters.insert("limit".to_string(), Value::Int(limit));
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_OVERVIEW_MEMORY_RANKING_QUERY,
            &parameters,
            &graph_overview_read_options(options),
        )?;
        let rows = read
            .output
            .rows
            .iter()
            .map(decode_graph_overview_row)
            .collect::<Result<Vec<_>>>()?;
        let report = NowledgeMemGraphOverviewRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            row_count: rows.len(),
            read_report: read.report,
        };
        Ok(NowledgeMemGraphOverviewOutput { rows, report })
    }

    pub fn read_graph_sample(
        &mut self,
        options: &NowledgeMemGraphSampleOptions,
    ) -> Result<NowledgeMemGraphSampleOutput> {
        let limit = i64::try_from(options.limit).map_err(|_| {
            SkeinError::Semantic("graph sample limit exceeds supported range".to_string())
        })?;
        if limit <= 0 {
            return Err(SkeinError::Semantic(
                "graph sample limit must be greater than zero".to_string(),
            ));
        }
        let parameters = BTreeMap::from([("limit".to_string(), Value::Int(limit))]);
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_SAMPLE_MEMORY_QUERY,
            &parameters,
            &graph_sample_read_options(options),
        )?;
        let rows = read
            .output
            .rows
            .iter()
            .map(decode_graph_overview_row)
            .collect::<Result<Vec<_>>>()?;
        let report = NowledgeMemGraphSampleRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            row_count: rows.len(),
            read_report: read.report,
        };
        Ok(NowledgeMemGraphSampleOutput { rows, report })
    }

    pub fn read_graph_node_details(
        &mut self,
        options: &NowledgeMemGraphNodeDetailsOptions,
    ) -> Result<NowledgeMemGraphNodeDetailsOutput> {
        let node_id = i64::try_from(options.node_id).map_err(|_| {
            SkeinError::Semantic("graph node details node_id exceeds supported range".to_string())
        })?;
        let mut parameters = BTreeMap::new();
        parameters.insert("node_id".to_string(), Value::Int(node_id));
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_NODE_DETAILS_MEMORY_QUERY,
            &parameters,
            &graph_node_details_read_options(options),
        )?;
        let node = read
            .output
            .rows
            .first()
            .map(decode_graph_node_details_row)
            .transpose()?;
        let report = NowledgeMemGraphNodeDetailsRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            node_id: options.node_id,
            row_count: usize::from(node.is_some()),
            read_report: read.report,
        };
        Ok(NowledgeMemGraphNodeDetailsOutput { node, report })
    }

    pub fn read_graph_community_members(
        &mut self,
        options: &NowledgeMemGraphCommunityMembersOptions,
    ) -> Result<NowledgeMemGraphCommunityMembersOutput> {
        let limit = i64::try_from(options.limit).map_err(|_| {
            SkeinError::Semantic(
                "graph community members limit exceeds supported range".to_string(),
            )
        })?;
        if limit <= 0 {
            return Err(SkeinError::Semantic(
                "graph community members limit must be greater than zero".to_string(),
            ));
        }
        let mut parameters = BTreeMap::new();
        parameters.insert("community_id".to_string(), Value::Int(options.community_id));
        parameters.insert("limit".to_string(), Value::Int(limit));
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY,
            &parameters,
            &graph_community_members_read_options(options),
        )?;
        let rows = read
            .output
            .rows
            .iter()
            .map(decode_graph_overview_row)
            .collect::<Result<Vec<_>>>()?;
        let report = NowledgeMemGraphCommunityMembersRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            community_id: options.community_id,
            row_count: rows.len(),
            read_report: read.report,
        };
        Ok(NowledgeMemGraphCommunityMembersOutput { rows, report })
    }

    pub fn read_graph_community_recent_memories(
        &mut self,
        options: &NowledgeMemGraphCommunityRecentMemoriesOptions,
    ) -> Result<NowledgeMemGraphCommunityRecentMemoriesOutput> {
        let limit = i64::try_from(options.limit).map_err(|_| {
            SkeinError::Semantic(
                "graph community recent memories limit exceeds supported range".to_string(),
            )
        })?;
        if limit <= 0 {
            return Err(SkeinError::Semantic(
                "graph community recent memories limit must be greater than zero".to_string(),
            ));
        }
        let parameters = BTreeMap::from([
            ("community_id".to_string(), Value::Int(options.community_id)),
            ("limit".to_string(), Value::Int(limit)),
        ]);
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY,
            &parameters,
            &graph_community_recent_memories_read_options(options),
        )?;
        let rows = read
            .output
            .rows
            .iter()
            .map(decode_graph_community_recent_memory_row)
            .collect::<Result<Vec<_>>>()?;
        let report = NowledgeMemGraphCommunityRecentMemoriesRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE_REPORT_PROTOCOL
                .to_string(),
            route: NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            community_id: options.community_id,
            row_count: rows.len(),
            read_report: read.report,
        };
        Ok(NowledgeMemGraphCommunityRecentMemoriesOutput { rows, report })
    }

    pub fn read_graph_community_subgraph(
        &mut self,
        options: &NowledgeMemGraphCommunitySubgraphOptions,
    ) -> Result<NowledgeMemGraphCommunitySubgraphOutput> {
        let max_entities = i64::try_from(options.max_entities).map_err(|_| {
            SkeinError::Semantic(
                "graph community subgraph max_entities exceeds supported range".to_string(),
            )
        })?;
        let max_edges = i64::try_from(options.max_edges).map_err(|_| {
            SkeinError::Semantic(
                "graph community subgraph max_edges exceeds supported range".to_string(),
            )
        })?;
        if max_entities <= 0 {
            return Err(SkeinError::Semantic(
                "graph community subgraph max_entities must be greater than zero".to_string(),
            ));
        }
        if max_edges < 0 {
            return Err(SkeinError::Semantic(
                "graph community subgraph max_edges must not be negative".to_string(),
            ));
        }
        let entity_parameters = BTreeMap::from([
            ("community_id".to_string(), Value::Int(options.community_id)),
            ("max_entities".to_string(), Value::Int(max_entities)),
        ]);
        let entity_read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY,
            &entity_parameters,
            &graph_community_subgraph_entity_read_options(options),
        )?;
        let entities = entity_read
            .output
            .rows
            .iter()
            .map(decode_graph_community_subgraph_entity_row)
            .collect::<Result<Vec<_>>>()?;
        let entity_ids = entities
            .iter()
            .filter_map(|entity| entity.entity_id.as_ref())
            .map(|entity_id| Value::String(entity_id.clone()))
            .collect::<Vec<_>>();
        let (edges, edge_read_report) = if entity_ids.is_empty() || options.max_edges == 0 {
            (Vec::new(), None)
        } else {
            let edge_parameters = BTreeMap::from([
                ("entity_ids".to_string(), Value::List(entity_ids)),
                ("max_edges".to_string(), Value::Int(max_edges)),
            ]);
            let edge_read = self.read_query_with_params(
                NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY,
                &edge_parameters,
                &graph_community_subgraph_edge_read_options(options),
            )?;
            let edges = edge_read
                .output
                .rows
                .iter()
                .map(decode_graph_community_subgraph_edge_row)
                .collect::<Result<Vec<_>>>()?;
            (edges, Some(edge_read.report))
        };
        let report = NowledgeMemGraphCommunitySubgraphRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            community_id: options.community_id,
            entity_count: entities.len(),
            edge_count: edges.len(),
            entity_read_report: entity_read.report,
            edge_read_report,
        };
        Ok(NowledgeMemGraphCommunitySubgraphOutput {
            entities,
            edges,
            report,
        })
    }

    pub fn read_graph_augmentation_state(
        &mut self,
        options: &NowledgeMemGraphAugmentationStateOptions,
    ) -> Result<NowledgeMemGraphAugmentationStateOutput> {
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_QUERY,
            &BTreeMap::new(),
            &graph_augmentation_state_read_options(options),
        )?;
        let state = read
            .output
            .rows
            .first()
            .map(decode_graph_augmentation_state_row)
            .transpose()?;
        let report = NowledgeMemGraphAugmentationStateRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            row_count: usize::from(state.is_some()),
            read_report: read.report,
        };
        Ok(NowledgeMemGraphAugmentationStateOutput { state, report })
    }

    pub fn read_graph_pagerank_plan(
        &mut self,
        options: &NowledgeMemGraphPageRankPlanOptions,
    ) -> Result<NowledgeMemGraphPageRankPlanOutput> {
        let graph_commit_epoch = self.db.commit_epoch();
        let mut read_reports = Vec::new();
        let graph_meta = self.read_graph_pagerank_plan_meta(options, &mut read_reports)?;
        let memory_node_count = self.read_graph_pagerank_plan_count(
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY,
            &BTreeMap::new(),
            options,
            &mut read_reports,
        )?;
        let entity_node_count = self.read_graph_pagerank_plan_count(
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY,
            &BTreeMap::new(),
            options,
            &mut read_reports,
        )?;
        let entity_relation_count = self.read_graph_pagerank_plan_count(
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY,
            &BTreeMap::new(),
            options,
            &mut read_reports,
        )?;
        let mention_edge_count = self.read_graph_pagerank_plan_count(
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY,
            &BTreeMap::new(),
            options,
            &mut read_reports,
        )?;
        let active_memory_relation_count = self.read_graph_pagerank_plan_count(
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY,
            &BTreeMap::new(),
            options,
            &mut read_reports,
        )?;
        let (
            changed_memory_count,
            changed_entity_count,
            changed_mention_edge_count,
            changed_entity_relation_count,
            changed_memory_relation_count,
        ) = if let Some(cutoff) = options.changed_since_epoch_nanos {
            let parameters = BTreeMap::from([("cutoff".to_string(), Value::Int(cutoff))]);
            (
                self.read_graph_pagerank_plan_count(
                    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY,
                    &parameters,
                    options,
                    &mut read_reports,
                )?,
                self.read_graph_pagerank_plan_count(
                    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY,
                    &parameters,
                    options,
                    &mut read_reports,
                )?,
                self.read_graph_pagerank_plan_count(
                    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY,
                    &parameters,
                    options,
                    &mut read_reports,
                )?,
                self.read_graph_pagerank_plan_count(
                    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY,
                    &parameters,
                    options,
                    &mut read_reports,
                )?,
                self.read_graph_pagerank_plan_count(
                    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY,
                    &parameters,
                    options,
                    &mut read_reports,
                )?,
            )
        } else {
            (0, 0, 0, 0, 0)
        };
        let query_count = read_reports.len();
        let report = NowledgeMemGraphPageRankPlanRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            changed_since_epoch_nanos: options.changed_since_epoch_nanos,
            query_count,
            read_reports,
        };
        Ok(NowledgeMemGraphPageRankPlanOutput {
            graph_commit_epoch,
            graph_meta,
            memory_node_count,
            entity_node_count,
            entity_relation_count,
            mention_edge_count,
            active_memory_relation_count,
            changed_memory_count,
            changed_entity_count,
            changed_mention_edge_count,
            changed_entity_relation_count,
            changed_memory_relation_count,
            report,
        })
    }

    fn read_graph_pagerank_plan_meta(
        &mut self,
        options: &NowledgeMemGraphPageRankPlanOptions,
        read_reports: &mut Vec<NowledgeMemReadReport>,
    ) -> Result<Option<NowledgeMemGraphPageRankPlanMetaRow>> {
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY,
            &BTreeMap::new(),
            &graph_pagerank_plan_read_options(options),
        )?;
        let meta = read
            .output
            .rows
            .first()
            .map(decode_graph_pagerank_plan_meta_row)
            .transpose()?;
        read_reports.push(read.report);
        Ok(meta)
    }

    fn read_graph_pagerank_plan_count(
        &mut self,
        query: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemGraphPageRankPlanOptions,
        read_reports: &mut Vec<NowledgeMemReadReport>,
    ) -> Result<usize> {
        let read = self.read_query_with_params(
            query,
            parameters,
            &graph_pagerank_plan_read_options(options),
        )?;
        let count = read
            .output
            .rows
            .first()
            .map(|row| required_usize_field(row, "total"))
            .transpose()?
            .unwrap_or(0);
        read_reports.push(read.report);
        Ok(count)
    }

    pub fn read_graph_orphans(
        &mut self,
        options: &NowledgeMemGraphOrphansOptions,
    ) -> Result<NowledgeMemGraphOrphansOutput> {
        let limit = i64::try_from(options.limit).map_err(|_| {
            SkeinError::Semantic("graph orphans limit exceeds supported range".to_string())
        })?;
        if limit <= 0 {
            return Err(SkeinError::Semantic(
                "graph orphans limit must be greater than zero".to_string(),
            ));
        }
        let parameters = BTreeMap::from([("limit".to_string(), Value::Int(limit))]);
        let read = self.read_query_with_params(
            NOWLEDGE_MEM_GRAPH_ORPHAN_ENTITIES_QUERY,
            &parameters,
            &graph_orphans_read_options(options),
        )?;
        let rows = read
            .output
            .rows
            .iter()
            .map(decode_graph_orphan_entity_row)
            .collect::<Result<Vec<_>>>()?;
        let report = NowledgeMemGraphOrphansRouteReport {
            protocol: NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE_REPORT_PROTOCOL.to_string(),
            route: NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE.to_string(),
            read_engine: crate::route_ownership::NowledgeMemRouteReadEngine::Skein,
            route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
            route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
            row_count: rows.len(),
            read_report: read.report,
        };
        Ok(NowledgeMemGraphOrphansOutput { rows, report })
    }
}

fn graph_overview_read_options(
    options: &NowledgeMemGraphOverviewOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(options.limit),
        None => options.limit,
    });
    read_options
}

fn graph_sample_read_options(options: &NowledgeMemGraphSampleOptions) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(options.limit),
        None => options.limit,
    });
    read_options
}

fn graph_node_details_read_options(
    options: &NowledgeMemGraphNodeDetailsOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(1),
        None => 1,
    });
    read_options
}

fn graph_community_members_read_options(
    options: &NowledgeMemGraphCommunityMembersOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(options.limit),
        None => options.limit,
    });
    read_options
}

fn graph_community_recent_memories_read_options(
    options: &NowledgeMemGraphCommunityRecentMemoriesOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(options.limit),
        None => options.limit,
    });
    read_options
}

fn graph_community_subgraph_entity_read_options(
    options: &NowledgeMemGraphCommunitySubgraphOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(options.max_entities),
        None => options.max_entities,
    });
    read_options
}

fn graph_community_subgraph_edge_read_options(
    options: &NowledgeMemGraphCommunitySubgraphOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(options.max_edges),
        None => options.max_edges,
    });
    read_options
}

fn graph_augmentation_state_read_options(
    options: &NowledgeMemGraphAugmentationStateOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(1),
        None => 1,
    });
    read_options
}

fn graph_pagerank_plan_read_options(
    options: &NowledgeMemGraphPageRankPlanOptions,
) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(1),
        None => 1,
    });
    read_options
}

fn graph_orphans_read_options(options: &NowledgeMemGraphOrphansOptions) -> NowledgeMemReadOptions {
    let mut read_options = options.read_options.clone();
    read_options.max_rows = Some(match read_options.max_rows {
        Some(max_rows) => max_rows.min(options.limit),
        None => options.limit,
    });
    read_options
}

fn decode_graph_overview_row(row: &BTreeMap<String, Value>) -> Result<NowledgeMemGraphOverviewRow> {
    Ok(NowledgeMemGraphOverviewRow {
        memory_id: optional_string_field(row, "memory_id")?,
        node_id: required_u64_field(row, "node_id")?,
        label: optional_string_field(row, "label")?,
        title: optional_string_field(row, "title")?,
        content_preview: optional_string_field(row, "content_preview")?,
        score: optional_f64_field(row, "score")?,
        community_id: optional_value_field(row, "community_id"),
        raw_space_id: optional_string_field(row, "raw_space_id")?,
        created_at: optional_value_field(row, "created_at"),
        updated_at: optional_value_field(row, "updated_at"),
        source: optional_string_field(row, "source")?,
        event_start: optional_value_field(row, "event_start"),
        event_end: optional_value_field(row, "event_end"),
        importance: optional_value_field(row, "importance"),
    })
}

fn decode_graph_node_details_row(
    row: &BTreeMap<String, Value>,
) -> Result<NowledgeMemGraphNodeDetailsRow> {
    Ok(NowledgeMemGraphNodeDetailsRow {
        node_id: required_u64_field(row, "node_id")?,
        memory_id: optional_string_field(row, "memory_id")?,
        node_kind: required_string_field(row, "node_kind")?,
        label: optional_string_field(row, "label")?,
        title: optional_string_field(row, "title")?,
        content: optional_string_field(row, "content")?,
        content_preview: optional_string_field(row, "content_preview")?,
        summary: optional_string_field(row, "summary")?,
        source: optional_string_field(row, "source")?,
        raw_space_id: optional_string_field(row, "raw_space_id")?,
        community_id: optional_value_field(row, "community_id"),
        created_at: optional_value_field(row, "created_at"),
        updated_at: optional_value_field(row, "updated_at"),
        event_start: optional_value_field(row, "event_start"),
        event_end: optional_value_field(row, "event_end"),
        importance: optional_value_field(row, "importance"),
        confidence: optional_value_field(row, "confidence"),
        is_latest: optional_bool_field(row, "is_latest")?,
        is_deleted: optional_bool_field(row, "is_deleted")?,
    })
}

fn decode_graph_community_recent_memory_row(
    row: &BTreeMap<String, Value>,
) -> Result<NowledgeMemGraphCommunityRecentMemoryRow> {
    Ok(NowledgeMemGraphCommunityRecentMemoryRow {
        memory_id: optional_string_field(row, "memory_id")?,
        node_id: required_u64_field(row, "node_id")?,
        label: optional_string_field(row, "label")?,
        title: optional_string_field(row, "title")?,
        content: optional_string_field(row, "content")?,
        content_preview: optional_string_field(row, "content_preview")?,
        importance: optional_value_field(row, "importance"),
        created_at: optional_value_field(row, "created_at"),
        updated_at: optional_value_field(row, "updated_at"),
        is_crystal: optional_bool_field(row, "is_crystal")?,
        mention_breadth: required_u64_field(row, "mention_breadth")?,
    })
}

fn decode_graph_community_subgraph_entity_row(
    row: &BTreeMap<String, Value>,
) -> Result<NowledgeMemGraphCommunitySubgraphEntityRow> {
    Ok(NowledgeMemGraphCommunitySubgraphEntityRow {
        entity_id: optional_string_field(row, "entity_id")?,
        node_id: required_u64_field(row, "node_id")?,
        label: optional_string_field(row, "label")?,
        name: optional_string_field(row, "name")?,
        entity_type: optional_string_field(row, "entity_type")?,
        confidence: optional_value_field(row, "confidence"),
        mention_count: required_u64_field(row, "mention_count")?,
    })
}

fn decode_graph_community_subgraph_edge_row(
    row: &BTreeMap<String, Value>,
) -> Result<NowledgeMemGraphCommunitySubgraphEdgeRow> {
    Ok(NowledgeMemGraphCommunitySubgraphEdgeRow {
        source_entity_id: optional_string_field(row, "source_entity_id")?,
        target_entity_id: optional_string_field(row, "target_entity_id")?,
        relationship_id: required_u64_field(row, "relationship_id")?,
        confidence: optional_value_field(row, "confidence"),
        relation_type: optional_string_field(row, "relation_type")?,
    })
}

fn decode_graph_augmentation_state_row(
    row: &BTreeMap<String, Value>,
) -> Result<NowledgeMemGraphAugmentationStateRow> {
    Ok(NowledgeMemGraphAugmentationStateRow {
        community_detection_applied: optional_bool_field(row, "community_detection_applied")?,
        pagerank_applied: optional_bool_field(row, "pagerank_applied")?,
        community_algorithm: optional_string_field(row, "community_algorithm")?,
        community_resolution: optional_value_field(row, "community_resolution"),
        community_count: optional_value_field(row, "community_count"),
        pagerank_algorithm: optional_string_field(row, "pagerank_algorithm")?,
        pagerank_damping: optional_value_field(row, "pagerank_damping"),
        pagerank_iterations: optional_value_field(row, "pagerank_iterations"),
        last_augmentation_at: optional_value_field(row, "last_augmentation_at"),
        schema_version: optional_value_field(row, "schema_version"),
        community_detection_computed_at: optional_value_field(
            row,
            "community_detection_computed_at",
        ),
        pagerank_computed_at: optional_value_field(row, "pagerank_computed_at"),
    })
}

fn decode_graph_pagerank_plan_meta_row(
    row: &BTreeMap<String, Value>,
) -> Result<NowledgeMemGraphPageRankPlanMetaRow> {
    Ok(NowledgeMemGraphPageRankPlanMetaRow {
        pagerank_applied: optional_bool_field(row, "pagerank_applied")?,
        pagerank_computed_at: optional_value_field(row, "pagerank_computed_at"),
    })
}

fn decode_graph_orphan_entity_row(
    row: &BTreeMap<String, Value>,
) -> Result<NowledgeMemGraphOrphanEntityRow> {
    Ok(NowledgeMemGraphOrphanEntityRow {
        entity_id: optional_string_field(row, "entity_id")?,
        node_id: required_u64_field(row, "node_id")?,
        label: optional_string_field(row, "label")?,
        name: optional_string_field(row, "name")?,
        entity_type: optional_string_field(row, "entity_type")?,
        description: optional_string_field(row, "description")?,
        community_id: optional_value_field(row, "community_id"),
        confidence: optional_value_field(row, "confidence"),
        pagerank_score: optional_value_field(row, "pagerank_score"),
    })
}

fn optional_value_field(row: &BTreeMap<String, Value>, field: &str) -> Option<Value> {
    match row.get(field) {
        Some(Value::Null) | None => None,
        Some(value) => Some(value.clone()),
    }
}

fn optional_string_field(row: &BTreeMap<String, Value>, field: &str) -> Result<Option<String>> {
    match row.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(value) => Err(SkeinError::Semantic(format!(
            "graph route field {field} expected string or null, got {value}"
        ))),
    }
}

fn required_string_field(row: &BTreeMap<String, Value>, field: &str) -> Result<String> {
    match row.get(field) {
        Some(Value::String(value)) => Ok(value.clone()),
        Some(value) => Err(SkeinError::Semantic(format!(
            "graph route field {field} expected string, got {value}"
        ))),
        None => Err(SkeinError::Semantic(format!(
            "graph route field {field} is missing"
        ))),
    }
}

fn optional_bool_field(row: &BTreeMap<String, Value>, field: &str) -> Result<Option<bool>> {
    match row.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(value) => Err(SkeinError::Semantic(format!(
            "graph route field {field} expected boolean or null, got {value}"
        ))),
    }
}

fn optional_f64_field(row: &BTreeMap<String, Value>, field: &str) -> Result<Option<f64>> {
    match row.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Int(value)) => Ok(Some(*value as f64)),
        Some(Value::Float(value)) => Ok(Some(*value)),
        Some(value) => Err(SkeinError::Semantic(format!(
            "graph route field {field} expected number or null, got {value}"
        ))),
    }
}

fn required_u64_field(row: &BTreeMap<String, Value>, field: &str) -> Result<u64> {
    match row.get(field) {
        Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
        Some(value) => Err(SkeinError::Semantic(format!(
            "graph route field {field} expected non-negative integer, got {value}"
        ))),
        None => Err(SkeinError::Semantic(format!(
            "graph route field {field} is missing"
        ))),
    }
}

fn required_usize_field(row: &BTreeMap<String, Value>, field: &str) -> Result<usize> {
    let value = required_u64_field(row, field)?;
    usize::try_from(value).map_err(|_| {
        SkeinError::Execution(format!(
            "nowledge mem field '{field}' exceeds supported usize range"
        ))
    })
}

#[derive(Debug)]
pub struct NowledgeMemSearchProjection {
    index: SearchIndex,
}

impl NowledgeMemSearchProjection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            index: SearchIndex::open(path)?,
        })
    }

    pub fn from_index(index: SearchIndex) -> Self {
        Self { index }
    }

    pub fn index(&self) -> &SearchIndex {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut SearchIndex {
        &mut self.index
    }

    pub fn into_index(self) -> SearchIndex {
        self.index
    }

    pub fn probe_json(&self, options: SearchProjectionProbeOptions) -> serde_json::Value {
        self.index.nowledge_search_projection_probe_json(options)
    }

    pub fn evidence_json(&self, options: SearchProjectionProbeOptions) -> serde_json::Value {
        nowledge_search_projection_evidence_json(&self.probe_json(options))
    }

    pub fn evidence_report(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> NowledgeSearchProjectionEvidenceReport {
        NowledgeSearchProjectionEvidenceReport::from_probe(&self.probe_json(options))
    }

    pub fn shadow_evidence_json(
        &self,
        primary_probe: &serde_json::Value,
        options: SearchProjectionProbeOptions,
    ) -> serde_json::Value {
        nowledge_search_projection_shadow_evidence_json(primary_probe, &self.probe_json(options))
    }

    pub fn freshness(&self) -> SearchProjectionFreshness {
        self.index.projection_freshness()
    }

    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> SearchResultSet {
        self.search_candidates_with_report(request).result
    }

    pub fn search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> NowledgeMemSearchCandidateOutput {
        let effective_compressed_vector_search_mode =
            request.effective_compressed_vector_search_mode();
        let result = self
            .index
            .search_with_options_compressed_vector_projection_mode(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                SearchQueryOptions {
                    limit: request.limit,
                    rank_window: request.rank_window,
                    fusion_weights: request.fusion_weights,
                    metadata_filters: request.metadata_filters.clone(),
                    policy_epoch: None,
                },
                effective_compressed_vector_search_mode,
            );
        let report = nowledge_mem_search_candidate_report(
            request,
            effective_compressed_vector_search_mode,
            &result,
        );
        NowledgeMemSearchCandidateOutput { result, report }
    }

    pub fn search_candidate_readiness(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> NowledgeMemSearchCandidateReadinessReport {
        self.search_candidates_with_report(request)
            .readiness_report(options)
    }

    pub fn search_candidate_shadow_evidence<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> NowledgeMemSearchCandidateShadowEvidence
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let output = self.search_candidates_with_report(request);
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_search_candidate_output(primary_candidate_ids, &output);
        accumulator.evidence()
    }

    pub fn search_candidate_shadow_evidence_json<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> serde_json::Value
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.search_candidate_shadow_evidence(request, primary_candidate_ids)
            .json()
    }
}

#[derive(Debug)]
pub struct NowledgeMemEmbeddedStore {
    graph: NowledgeMemGraph,
    search_projection: Option<NowledgeMemSearchProjection>,
}

#[derive(Debug, Clone)]
pub struct NowledgeMemEmbeddedStoreHandle {
    inner: Arc<Mutex<NowledgeMemEmbeddedStore>>,
}

impl NowledgeMemEmbeddedStoreHandle {
    pub fn new(store: NowledgeMemEmbeddedStore) -> Self {
        Self {
            inner: Arc::new(Mutex::new(store)),
        }
    }

    pub fn open_with_options(
        options: NowledgeMemOpenOptions,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let (store, report) = NowledgeMemEmbeddedStore::open_with_options(options)?;
        Ok((Self::new(store), report))
    }

    pub fn query_with_report(&self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.lock_store()?.query_with_report(cypher)
    }

    pub fn query_with_report_options(
        &self,
        cypher: &str,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.lock_store()?
            .query_with_report_options(cypher, options)
    }

    pub fn query_with_params_with_report(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<NowledgeMemQueryOutput> {
        self.lock_store()?
            .query_with_params_with_report(cypher, parameters)
    }

    pub fn query_with_params_with_report_options(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.lock_store()?
            .query_with_params_with_report_options(cypher, parameters, options)
    }

    pub fn read_query(
        &self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.lock_store()?.read_query_with_options(cypher, options)
    }

    pub fn read_query_with_params(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.lock_store()?
            .read_query_with_params(cypher, parameters, options)
    }

    pub fn read_graph_overview(
        &self,
        options: &NowledgeMemGraphOverviewOptions,
    ) -> Result<NowledgeMemGraphOverviewOutput> {
        self.lock_store()?.read_graph_overview(options)
    }

    pub fn read_graph_sample(
        &self,
        options: &NowledgeMemGraphSampleOptions,
    ) -> Result<NowledgeMemGraphSampleOutput> {
        self.lock_store()?.read_graph_sample(options)
    }

    pub fn read_graph_node_details(
        &self,
        options: &NowledgeMemGraphNodeDetailsOptions,
    ) -> Result<NowledgeMemGraphNodeDetailsOutput> {
        self.lock_store()?.read_graph_node_details(options)
    }

    pub fn read_graph_community_members(
        &self,
        options: &NowledgeMemGraphCommunityMembersOptions,
    ) -> Result<NowledgeMemGraphCommunityMembersOutput> {
        self.lock_store()?.read_graph_community_members(options)
    }

    pub fn read_graph_community_recent_memories(
        &self,
        options: &NowledgeMemGraphCommunityRecentMemoriesOptions,
    ) -> Result<NowledgeMemGraphCommunityRecentMemoriesOutput> {
        self.lock_store()?
            .read_graph_community_recent_memories(options)
    }

    pub fn read_graph_community_subgraph(
        &self,
        options: &NowledgeMemGraphCommunitySubgraphOptions,
    ) -> Result<NowledgeMemGraphCommunitySubgraphOutput> {
        self.lock_store()?.read_graph_community_subgraph(options)
    }

    pub fn read_graph_augmentation_state(
        &self,
        options: &NowledgeMemGraphAugmentationStateOptions,
    ) -> Result<NowledgeMemGraphAugmentationStateOutput> {
        self.lock_store()?.read_graph_augmentation_state(options)
    }

    pub fn read_graph_pagerank_plan(
        &self,
        options: &NowledgeMemGraphPageRankPlanOptions,
    ) -> Result<NowledgeMemGraphPageRankPlanOutput> {
        self.lock_store()?.read_graph_pagerank_plan(options)
    }

    pub fn read_graph_orphans(
        &self,
        options: &NowledgeMemGraphOrphansOptions,
    ) -> Result<NowledgeMemGraphOrphansOutput> {
        self.lock_store()?.read_graph_orphans(options)
    }

    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<SearchResultSet> {
        Ok(self.lock_store()?.search_candidates(request)?.result)
    }

    pub fn search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        self.lock_store()?.search_candidates(request)
    }

    pub fn search_candidate_readiness(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> Result<NowledgeMemSearchCandidateReadinessReport> {
        self.lock_store()?
            .search_candidate_readiness(request, options)
    }

    pub fn search_candidate_shadow_evidence_json<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.lock_store()?
            .search_candidate_shadow_evidence_json(request, primary_candidate_ids)
    }

    pub fn slow_query_report(&self) -> Result<NowledgeMemSlowQueryReport> {
        Ok(self.lock_store()?.slow_query_report())
    }

    pub fn slow_query_report_json(&self) -> Result<serde_json::Value> {
        Ok(self.lock_store()?.slow_query_report_json())
    }

    pub fn library_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemLibraryReadinessReport> {
        Ok(self.lock_store()?.library_readiness(options))
    }

    pub fn library_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.lock_store()?.library_readiness_json(options))
    }

    pub fn readiness_dashboard(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemReadinessDashboard> {
        Ok(self.lock_store()?.readiness_dashboard(options))
    }

    pub fn readiness_dashboard_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.lock_store()?.readiness_dashboard_json(options))
    }

    pub fn query_runtime_preflight(
        &self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> Result<NowledgeQueryRuntimePreflightReport> {
        Ok(self.lock_store()?.query_runtime_preflight(probes))
    }

    pub fn query_runtime_preflight_json(
        &self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> Result<serde_json::Value> {
        Ok(self.lock_store()?.query_runtime_preflight_json(probes))
    }

    fn lock_store(&self) -> Result<MutexGuard<'_, NowledgeMemEmbeddedStore>> {
        self.inner.lock().map_err(|_| {
            SkeinError::Execution("nowledge mem embedded store lock poisoned".to_string())
        })
    }
}

impl NowledgeMemEmbeddedStore {
    pub fn new(
        graph: NowledgeMemGraph,
        search_projection: Option<NowledgeMemSearchProjection>,
    ) -> Self {
        Self {
            graph,
            search_projection,
        }
    }

    pub fn open_with_options(
        options: NowledgeMemOpenOptions,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let mut report = options.sanitized_report();
        let graph = NowledgeMemGraph::open_with_config(
            &options.graph_path,
            nowledge_mem_graph_config_with_search_mode(
                options.mode,
                options.effective_compressed_vector_search_mode(),
            ),
        )?;
        report.graph_opened = true;
        let search_projection = match options.search_projection_path.as_ref() {
            Some(path) => {
                let projection = NowledgeMemSearchProjection::open(path)?;
                report.search_projection_opened = true;
                Some(projection)
            }
            None => None,
        };
        Ok((Self::new(graph, search_projection), report))
    }

    pub fn graph(&self) -> &NowledgeMemGraph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut NowledgeMemGraph {
        &mut self.graph
    }

    pub fn search_projection(&self) -> Option<&NowledgeMemSearchProjection> {
        self.search_projection.as_ref()
    }

    pub fn search_projection_mut(&mut self) -> Option<&mut NowledgeMemSearchProjection> {
        self.search_projection.as_mut()
    }

    pub fn storage_recovery_report(&self) -> NowledgeMemStorageRecoveryReport {
        NowledgeMemStorageRecoveryReport::from_storage_report(
            &self.graph.database().storage_recovery_report(),
        )
    }

    pub fn storage_recovery_report_json(&self) -> serde_json::Value {
        self.storage_recovery_report().json()
    }

    pub fn build_search_projection_graph_delta_request_from_freshness(
        &self,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        let search_projection = self.require_search_projection()?;
        self.graph
            .database()
            .build_search_projection_graph_delta_request_from_freshness(
                search_projection.index(),
                max_operations,
            )
    }

    pub fn search_projection_graph_delta_background_work_plan(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let search_projection = self.search_projection.as_ref()?;
        self.graph
            .database()
            .search_projection_graph_delta_freshness_background_work_plan(
                search_projection.index(),
                request,
                hint,
            )
    }

    pub fn apply_search_projection_graph_delta(
        &mut self,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let Self {
            graph,
            search_projection,
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph
            .database()
            .apply_search_projection_graph_delta(search_projection.index_mut(), request)
    }

    pub fn apply_scheduled_background_search_projection_graph_delta(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let Self {
            graph,
            search_projection,
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph
            .database()
            .apply_scheduled_background_search_projection_graph_delta(
                search_projection.index_mut(),
                scheduler,
                request,
            )
    }

    pub fn search_projection_probe_json(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.require_search_projection()?.probe_json(options))
    }

    pub fn search_projection_evidence_json(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.require_search_projection()?.evidence_json(options))
    }

    pub fn search_projection_evidence_report(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<NowledgeSearchProjectionEvidenceReport> {
        Ok(self.require_search_projection()?.evidence_report(options))
    }

    pub fn search_projection_shadow_evidence_json(
        &self,
        primary_probe: &serde_json::Value,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self
            .require_search_projection()?
            .shadow_evidence_json(primary_probe, options))
    }

    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        Ok(self
            .require_search_projection()?
            .search_candidates_with_report(request))
    }

    pub fn search_candidate_readiness(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> Result<NowledgeMemSearchCandidateReadinessReport> {
        Ok(self
            .require_search_projection()?
            .search_candidate_readiness(request, options))
    }

    pub fn search_candidate_shadow_evidence_json<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(self
            .require_search_projection()?
            .search_candidate_shadow_evidence_json(request, primary_candidate_ids))
    }

    pub fn retrieve_knowledge(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        Ok(self.retrieve_knowledge_with_report(request)?.output)
    }

    pub fn retrieve_knowledge_with_report(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<NowledgeMemRetrievalOutput> {
        let search_projection = self.require_search_projection()?;
        let output = self
            .graph
            .database()
            .retrieve_knowledge(search_projection.index(), request);
        let report = nowledge_mem_retrieval_report(
            self.graph.mode(),
            self.graph.database().config().compressed_vector_search_mode,
            &output,
        );
        Ok(NowledgeMemRetrievalOutput { output, report })
    }

    pub fn read_query(&mut self, cypher: &str) -> Result<NowledgeMemReadOutput> {
        self.graph.read_query(cypher)
    }

    pub fn query_with_report(&mut self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.graph.query_with_report(cypher)
    }

    pub fn query_with_report_options(
        &mut self,
        cypher: &str,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.graph.query_with_report_options(cypher, options)
    }

    pub fn query_with_params_with_report(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<NowledgeMemQueryOutput> {
        self.graph.query_with_params_with_report(cypher, parameters)
    }

    pub fn query_with_params_with_report_options(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.graph
            .query_with_params_with_report_options(cypher, parameters, options)
    }

    pub fn query_runtime_preflight(
        &mut self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> NowledgeQueryRuntimePreflightReport {
        nowledge_query_runtime_preflight_report(self.graph.database_mut(), probes)
    }

    pub fn query_runtime_preflight_json(
        &mut self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> serde_json::Value {
        self.query_runtime_preflight(probes).json()
    }

    pub fn slow_query_report(&self) -> NowledgeMemSlowQueryReport {
        self.graph.slow_query_report()
    }

    pub fn slow_query_report_json(&self) -> serde_json::Value {
        self.slow_query_report().json()
    }

    pub fn read_query_with_options(
        &mut self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph.read_query_with_options(cypher, options)
    }

    pub fn read_query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph
            .read_query_with_params(cypher, parameters, options)
    }

    pub fn read_graph_overview(
        &mut self,
        options: &NowledgeMemGraphOverviewOptions,
    ) -> Result<NowledgeMemGraphOverviewOutput> {
        self.graph.read_graph_overview(options)
    }

    pub fn read_graph_sample(
        &mut self,
        options: &NowledgeMemGraphSampleOptions,
    ) -> Result<NowledgeMemGraphSampleOutput> {
        self.graph.read_graph_sample(options)
    }

    pub fn read_graph_node_details(
        &mut self,
        options: &NowledgeMemGraphNodeDetailsOptions,
    ) -> Result<NowledgeMemGraphNodeDetailsOutput> {
        self.graph.read_graph_node_details(options)
    }

    pub fn read_graph_community_members(
        &mut self,
        options: &NowledgeMemGraphCommunityMembersOptions,
    ) -> Result<NowledgeMemGraphCommunityMembersOutput> {
        self.graph.read_graph_community_members(options)
    }

    pub fn read_graph_community_recent_memories(
        &mut self,
        options: &NowledgeMemGraphCommunityRecentMemoriesOptions,
    ) -> Result<NowledgeMemGraphCommunityRecentMemoriesOutput> {
        self.graph.read_graph_community_recent_memories(options)
    }

    pub fn read_graph_community_subgraph(
        &mut self,
        options: &NowledgeMemGraphCommunitySubgraphOptions,
    ) -> Result<NowledgeMemGraphCommunitySubgraphOutput> {
        self.graph.read_graph_community_subgraph(options)
    }

    pub fn read_graph_augmentation_state(
        &mut self,
        options: &NowledgeMemGraphAugmentationStateOptions,
    ) -> Result<NowledgeMemGraphAugmentationStateOutput> {
        self.graph.read_graph_augmentation_state(options)
    }

    pub fn read_graph_pagerank_plan(
        &mut self,
        options: &NowledgeMemGraphPageRankPlanOptions,
    ) -> Result<NowledgeMemGraphPageRankPlanOutput> {
        self.graph.read_graph_pagerank_plan(options)
    }

    pub fn read_graph_orphans(
        &mut self,
        options: &NowledgeMemGraphOrphansOptions,
    ) -> Result<NowledgeMemGraphOrphansOutput> {
        self.graph.read_graph_orphans(options)
    }

    pub fn background_maintenance_summary(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> BackgroundMaintenanceSummary {
        self.graph.database().background_maintenance_summary(
            self.search_projection
                .as_ref()
                .map(NowledgeMemSearchProjection::index),
            policy,
            state,
            options,
        )
    }

    pub fn background_maintenance_report(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> NowledgeMemBackgroundMaintenanceReport {
        let summary = self.background_maintenance_summary(policy, state, options);
        let slow_query = self.slow_query_report();
        NowledgeMemBackgroundMaintenanceReport::from_summary_with_slow_query(
            &summary,
            Some(&slow_query),
        )
    }

    pub fn background_maintenance_report_json(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> serde_json::Value {
        self.background_maintenance_report(policy, state, options)
            .json()
    }

    pub fn library_readiness_json(
        &mut self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        self.library_readiness(options).json()
    }

    pub fn library_readiness(
        &mut self,
        options: &NowledgeMemReadinessOptions,
    ) -> NowledgeMemLibraryReadinessReport {
        let bounded_read_evidence = options
            .bounded_read_evidence
            .clone()
            .unwrap_or_else(|| self.bounded_read_probe_evidence_json(options));
        let storage_recovery = self.storage_recovery_report_json();
        let background_maintenance = self.background_maintenance_report_json(
            &options.qos_policy,
            &options.qos_state,
            options.background_maintenance_options.clone(),
        );
        let query_family_evidence = query_family_replacement_evidence_json(
            options.replacement_readiness_by_query_family.as_ref(),
        );
        let graph_route_readiness =
            nowledge_mem_graph_route_readiness_json(options.graph_route_readiness.as_ref());
        let search_projection_evidence =
            options
                .search_projection_evidence
                .clone()
                .unwrap_or_else(|| {
                    self.search_projection_evidence_json(
                        options.search_projection_probe_options.clone(),
                    )
                    .unwrap_or_else(|_| missing_search_projection_evidence_json())
                });
        let search_projection_shadow_evidence = options
            .search_projection_shadow_evidence
            .clone()
            .unwrap_or_else(|| {
                options
                    .primary_search_projection_probe
                    .as_ref()
                    .map(|primary_probe| {
                        self.search_projection_shadow_evidence_json(
                            primary_probe,
                            options.search_projection_probe_options.clone(),
                        )
                        .unwrap_or_else(|_| missing_search_projection_shadow_evidence_json())
                    })
                    .unwrap_or_else(missing_primary_search_projection_probe_json)
            });
        let search_candidate_shadow_evidence = options
            .search_candidate_shadow_evidence
            .clone()
            .unwrap_or_else(missing_search_candidate_shadow_evidence_json);
        let workload_fixture_evidence = options
            .workload_fixture_evidence
            .as_ref()
            .map(NowledgeGraphRouteWorkloadFixtureReport::json)
            .unwrap_or_else(missing_workload_fixture_evidence_json);
        let evidence = LibraryReadinessEvidence {
            bounded_read_evidence: &bounded_read_evidence,
            storage_recovery: &storage_recovery,
            background_maintenance: &background_maintenance,
            query_family_evidence: &query_family_evidence,
            graph_route_readiness: &graph_route_readiness,
            search_projection_evidence: &search_projection_evidence,
            search_projection_shadow_evidence: &search_projection_shadow_evidence,
            search_candidate_shadow_evidence: &search_candidate_shadow_evidence,
            workload_fixture_evidence: &workload_fixture_evidence,
        };
        let blocker_codes = library_readiness_blocker_codes(&evidence);
        let readiness_by_area = library_readiness_by_area(&evidence);
        let areas = readiness_by_area.areas();
        let ready_area_count = areas.iter().filter(|area| area.ready).count();
        let blocked_area_count = areas.len().saturating_sub(ready_area_count);
        let blocker_codes = blocker_codes
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let ready = blocker_codes.is_empty();

        NowledgeMemLibraryReadinessReport {
            protocol: NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL.to_string(),
            present: true,
            ready,
            mode: self.graph.mode(),
            redaction: NowledgeMemReadinessRedactionSummary::default(),
            production_path: NowledgeMemLibraryProductionPathSummary::default(),
            blocker_codes,
            readiness_by_area,
            ready_area_count,
            blocked_area_count,
            graph_open: true,
            graph_read_only: self.graph.database().config().read_only,
            graph_route_readiness,
            bounded_read_evidence,
            storage_recovery,
            background_maintenance,
            query_family_evidence,
            search_projection_evidence,
            search_projection_shadow_evidence,
            search_candidate_shadow_evidence,
            workload_fixture_evidence,
        }
    }

    pub fn readiness_dashboard(
        &mut self,
        options: &NowledgeMemReadinessOptions,
    ) -> NowledgeMemReadinessDashboard {
        let library = self.library_readiness(options);
        let slow_query = self.slow_query_report();
        NowledgeMemReadinessDashboard::from_reports(&library, &slow_query)
    }

    pub fn readiness_dashboard_json(
        &mut self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        self.readiness_dashboard(options).json()
    }

    fn bounded_read_probe_evidence_json(
        &mut self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        let Some(probe) = options.bounded_read_probe.as_ref() else {
            return serde_json::json!({
                "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
                "present": false,
                "ready": false,
                "blocker_codes": ["bounded_read_probe_missing"],
            });
        };
        match self.read_query_with_params(&probe.cypher, &probe.parameters, &options.read_options) {
            Ok(read) => nowledge_mem_bounded_read_evidence_json_with_route_readiness(
                &read.report,
                &options.covered_routes,
                options.graph_route_readiness.as_ref(),
            ),
            Err(_) => serde_json::json!({
                "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
                "present": true,
                "ready": false,
                "blocker_codes": ["bounded_read_probe_failed"],
            }),
        }
    }

    fn require_search_projection(&self) -> Result<&NowledgeMemSearchProjection> {
        self.search_projection
            .as_ref()
            .ok_or_else(missing_search_projection_error)
    }
}

fn missing_search_projection_error() -> SkeinError {
    SkeinError::Storage("nowledge mem search projection is not configured".to_string())
}

fn require_search_projection_mut(
    search_projection: &mut Option<NowledgeMemSearchProjection>,
) -> Result<&mut NowledgeMemSearchProjection> {
    search_projection
        .as_mut()
        .ok_or_else(missing_search_projection_error)
}

struct NowledgeMemQueryReportInput<'a> {
    mode: NowledgeMemGraphMode,
    statement: &'a cypher::Statement,
    trace: Option<&'a crate::optimizer::OptimizerTrace>,
    plan_cache_lookup: Option<PlanCacheLookup>,
    execution_profile: Option<&'a ReadExecutionProfile>,
    output: &'a QueryOutput,
    options: NowledgeMemQueryReportOptions,
    elapsed_micros: u128,
}

fn nowledge_mem_query_report(input: NowledgeMemQueryReportInput<'_>) -> NowledgeMemQueryReport {
    let statement_kind = crate::api::statement_kind(nowledge_statement_body(input.statement));
    let decision = nowledge_mem_query_execution_path(input.statement);
    let slow_log_candidate = input
        .options
        .slow_log_threshold_micros
        .is_some_and(|threshold| input.elapsed_micros >= threshold);
    let plan_cache = NowledgeMemPlanCacheReport::from_lookup(input.plan_cache_lookup);
    NowledgeMemQueryReport {
        protocol: NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL.to_string(),
        mode: input.mode,
        statement_kind: statement_kind.to_string(),
        execution_path: decision.execution_path,
        fast_path_reason: decision.fast_path_reason.map(str::to_string),
        elapsed_micros: input.elapsed_micros,
        slow_log_threshold_micros: input.options.slow_log_threshold_micros,
        slow_log_candidate,
        physical_plan_captured: input.trace.is_some(),
        plan_cache_lookup: input
            .plan_cache_lookup
            .map(|lookup| lookup.as_str().to_string()),
        plan_cache_bypass_reason: input
            .plan_cache_lookup
            .and_then(|lookup| lookup.bypass_reason())
            .map(|reason| reason.as_str().to_string()),
        plan_cache_cacheable: plan_cache.cacheable,
        plan_cache_hit: plan_cache.hit,
        plan_cache_miss: plan_cache.miss,
        plan_cache_bypassed: plan_cache.bypassed,
        physical_operator_counts: input
            .trace
            .map(|trace| trace.selected_plan_operator_counts.clone())
            .unwrap_or_default(),
        optimizer_decision_count: input
            .trace
            .map(|trace| trace.decisions.len())
            .unwrap_or_default(),
        scan_pruning_reports: input
            .execution_profile
            .map(|profile| profile.scan_pruning_reports.clone())
            .unwrap_or_default(),
        output_row_shape: NowledgeMemQueryOutputRowShape::from_output(input.output),
    }
}

fn scan_pruning_report_json(report: &ScanPruningReport) -> serde_json::Value {
    serde_json::json!({
        "target_kind": report.target_kind.as_str(),
        "label_id": report.label_id.map(|label_id| label_id.0),
        "rel_type_id": report.rel_type_id.map(|rel_type_id| rel_type_id.0),
        "strategy": scan_pruning_strategy_json(&report.strategy),
        "pruned": report.pruned,
        "exact_empty": report.exact_empty,
        "candidate_count_before_pruning": report.candidate_count_before_pruning,
        "pruned_candidate_count": report.pruned_candidate_count,
        "candidate_count_before_filter": report.candidate_count_before_filter,
        "output_count": report.output_count,
        "filtered_out_count": report.filtered_out_count,
    })
}

fn scan_pruning_strategy_json(strategy: &ScanPruningStrategy) -> serde_json::Value {
    match strategy {
        ScanPruningStrategy::FullLabelScan => serde_json::json!({"kind": "full_label_scan"}),
        ScanPruningStrategy::Empty => serde_json::json!({"kind": "empty"}),
        ScanPruningStrategy::IdEq => serde_json::json!({"kind": "id_eq"}),
        ScanPruningStrategy::IdIn => serde_json::json!({"kind": "id_in"}),
        ScanPruningStrategy::IdRange => serde_json::json!({"kind": "id_range"}),
        ScanPruningStrategy::PropertyEq { property } => {
            serde_json::json!({"kind": "property_eq", "property": property})
        }
        ScanPruningStrategy::PropertyNotEq { property } => {
            serde_json::json!({"kind": "property_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyMissingOrNull { property } => {
            serde_json::json!({"kind": "property_missing_or_null", "property": property})
        }
        ScanPruningStrategy::PropertyExists { property } => {
            serde_json::json!({"kind": "property_exists", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_eq", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullNotEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
    }
}

#[derive(Debug)]
struct NowledgeQueryRuntimeRouteCoverage {
    required_route_count: usize,
    covered_route_count: usize,
    covered_routes: Vec<String>,
    missing_required_routes: Vec<String>,
    required_routes_covered: bool,
    unknown_routes: Vec<String>,
    duplicate_routes: Vec<String>,
    ready: bool,
    blocker_codes: Vec<String>,
}

fn nowledge_query_runtime_preflight_report(
    db: &mut Database,
    probes: &[NowledgeQueryRuntimePreflightProbe],
) -> NowledgeQueryRuntimePreflightReport {
    let route_coverage = nowledge_query_runtime_route_coverage(probes);
    let probe_reports = probes
        .iter()
        .map(|probe| nowledge_query_runtime_probe_report(db, probe))
        .collect::<Vec<_>>();
    let passed_probe_count = probe_reports.iter().filter(|probe| probe.ready).count();
    let failed_probe_count = probe_reports.len().saturating_sub(passed_probe_count);
    let blocker_codes =
        query_runtime_preflight_blocker_codes(probes.len(), failed_probe_count, &route_coverage);

    NowledgeQueryRuntimePreflightReport {
        protocol: NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL.to_string(),
        ready: blocker_codes.is_empty(),
        database_opened: true,
        redaction: NowledgeQueryRuntimePreflightRedactionSummary::default(),
        probe_count: probes.len(),
        passed_probe_count,
        failed_probe_count,
        required_route_count: route_coverage.required_route_count,
        covered_route_count: route_coverage.covered_route_count,
        covered_routes: route_coverage.covered_routes,
        missing_required_routes: route_coverage.missing_required_routes,
        required_routes_covered: route_coverage.required_routes_covered,
        unknown_routes: route_coverage.unknown_routes,
        duplicate_routes: route_coverage.duplicate_routes,
        route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
        route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
        route_coverage_ready: route_coverage.ready,
        route_coverage_blocker_codes: route_coverage.blocker_codes,
        blocker_codes,
        probes: probe_reports,
    }
}

fn nowledge_query_runtime_route_coverage(
    probes: &[NowledgeQueryRuntimePreflightProbe],
) -> NowledgeQueryRuntimeRouteCoverage {
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    for route in probes.iter().filter_map(|probe| probe.route.as_deref()) {
        *route_counts.entry(route).or_default() += 1;
    }
    let observed_routes = route_counts.keys().copied().collect::<BTreeSet<_>>();
    let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| observed_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !observed_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_routes = observed_routes
        .iter()
        .filter(|route| !required_routes.contains(**route))
        .map(|route| (*route).to_string())
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let required_routes_covered = missing_required_routes.is_empty();
    let mut blocker_codes = Vec::new();
    if !required_routes_covered {
        blocker_codes.push("query_runtime_route_coverage_missing".to_string());
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("query_runtime_unknown_routes".to_string());
    }
    let ready = blocker_codes.is_empty();

    NowledgeQueryRuntimeRouteCoverage {
        required_route_count: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        covered_route_count: covered_routes.len(),
        covered_routes,
        missing_required_routes,
        required_routes_covered,
        unknown_routes,
        duplicate_routes,
        ready,
        blocker_codes,
    }
}

fn nowledge_query_runtime_probe_report(
    db: &mut Database,
    probe: &NowledgeQueryRuntimePreflightProbe,
) -> NowledgeQueryRuntimePreflightProbeReport {
    match db.explain_analyze_query_with_params(&probe.cypher, &probe.parameters) {
        Ok(output) => {
            let scan_pruning_report_count = output.execution_profile.scan_pruning_reports.len();
            let pruned_scan_count = output
                .execution_profile
                .scan_pruning_reports
                .iter()
                .filter(|report| report.pruned)
                .count();
            let output_row_count = output.output.rows.len();
            let blocker_codes = query_runtime_probe_blocker_codes(
                probe,
                scan_pruning_report_count,
                pruned_scan_count,
                output_row_count,
            );
            let plan_cache_lookup = output.plan_cache_lookup;
            let plan_cache = NowledgeMemPlanCacheReport::from_lookup(Some(plan_cache_lookup));
            NowledgeQueryRuntimePreflightProbeReport {
                name: probe.name.clone(),
                route: probe.route.clone(),
                query_family: probe.query_family.clone(),
                ready: blocker_codes.is_empty(),
                success: true,
                output_row_count,
                selected_plan_fingerprint: Some(output.trace.selected_plan_fingerprint),
                search_mode: Some(output.trace.search_mode.as_str().to_string()),
                selected_plan_operator_counts: output.trace.selected_plan_operator_counts,
                selected_plan_class_counts: output.trace.selected_plan_class_counts,
                optimizer_decision_count: output.trace.decisions.len(),
                plan_cache_lookup: Some(plan_cache_lookup.as_str().to_string()),
                plan_cache_bypass_reason: plan_cache_lookup
                    .bypass_reason()
                    .map(|reason| reason.as_str().to_string()),
                plan_cache_cacheable: plan_cache.cacheable,
                plan_cache_hit: plan_cache.hit,
                plan_cache_miss: plan_cache.miss,
                plan_cache_bypassed: plan_cache.bypassed,
                work_priority: Some(output.work_request.priority.as_str().to_string()),
                work_class: Some(output.work_request.class.as_str().to_string()),
                estimated_operations: Some(output.work_request.estimated_operations),
                max_rows: output.execution_profile.max_rows,
                detection_row_cap: output.execution_profile.detection_row_cap,
                row_limit_enforced_before_output: output
                    .execution_profile
                    .row_limit_enforced_before_output,
                operator_row_cap_enabled: output.execution_profile.operator_row_cap_enabled,
                blocking_operator_kinds: output.execution_profile.blocking_operator_kinds,
                scan_pruning_reports: output.execution_profile.scan_pruning_reports,
                pruned_scan_count,
                error_class: None,
                blocker_codes,
            }
        }
        Err(error) => NowledgeQueryRuntimePreflightProbeReport {
            name: probe.name.clone(),
            route: probe.route.clone(),
            query_family: probe.query_family.clone(),
            ready: false,
            success: false,
            output_row_count: 0,
            selected_plan_fingerprint: None,
            search_mode: None,
            selected_plan_operator_counts: BTreeMap::new(),
            selected_plan_class_counts: BTreeMap::new(),
            optimizer_decision_count: 0,
            plan_cache_lookup: None,
            plan_cache_bypass_reason: None,
            plan_cache_cacheable: false,
            plan_cache_hit: false,
            plan_cache_miss: false,
            plan_cache_bypassed: false,
            work_priority: None,
            work_class: None,
            estimated_operations: None,
            max_rows: None,
            detection_row_cap: None,
            row_limit_enforced_before_output: false,
            operator_row_cap_enabled: false,
            blocking_operator_kinds: Vec::new(),
            scan_pruning_reports: Vec::new(),
            pruned_scan_count: 0,
            error_class: Some(skein_error_class(&error).to_string()),
            blocker_codes: vec!["query_runtime_failed".to_string()],
        },
    }
}

fn query_runtime_preflight_blocker_codes(
    probe_count: usize,
    failed_probe_count: usize,
    route_coverage: &NowledgeQueryRuntimeRouteCoverage,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if probe_count == 0 {
        blockers.push("query_runtime_probes_missing".to_string());
    }
    if failed_probe_count > 0 {
        blockers.push("query_runtime_probe_failed".to_string());
    }
    blockers.extend(route_coverage.blocker_codes.iter().cloned());
    blockers
}

fn query_runtime_probe_blocker_codes(
    probe: &NowledgeQueryRuntimePreflightProbe,
    scan_pruning_report_count: usize,
    pruned_scan_count: usize,
    output_row_count: usize,
) -> Vec<String> {
    let mut blockers = Vec::new();
    blockers.extend(query_runtime_probe_identity_blocker_codes(probe));
    if probe.require_scan_pruning && scan_pruning_report_count < probe.min_scan_pruning_reports {
        blockers.push("scan_pruning_report_missing".to_string());
    }
    if probe.require_pruned && pruned_scan_count == 0 {
        blockers.push("scan_pruning_not_pruned".to_string());
    }
    if let Some(max_output_rows) = probe.max_output_rows {
        if output_row_count > max_output_rows {
            blockers.push("output_row_count_exceeded".to_string());
        }
    }
    blockers
}

fn query_runtime_probe_identity_blocker_codes(
    probe: &NowledgeQueryRuntimePreflightProbe,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if probe.name.trim().is_empty() || probe.name == "unnamed" {
        blockers.push("query_runtime_probe_name_missing".to_string());
    }
    match probe.route.as_deref() {
        Some(route) if REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route) => {}
        Some(_) => blockers.push("query_runtime_probe_unknown_route".to_string()),
        None => blockers.push("query_runtime_probe_route_missing".to_string()),
    }
    match probe.query_family.as_deref() {
        Some(family) if REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.contains(&family) => {}
        Some(_) => blockers.push("query_runtime_probe_unknown_query_family".to_string()),
        None => blockers.push("query_runtime_probe_query_family_missing".to_string()),
    }
    blockers
}

fn skein_error_class(error: &SkeinError) -> &'static str {
    match error {
        SkeinError::Parse(_) => "parse",
        SkeinError::Semantic(_) => "semantic",
        SkeinError::Storage(_) => "storage",
        SkeinError::Execution(_) => "execution",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NowledgeMemQueryPathDecision {
    execution_path: NowledgeMemQueryExecutionPath,
    fast_path_reason: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NowledgeMemPlanCacheReport {
    cacheable: bool,
    hit: bool,
    miss: bool,
    bypassed: bool,
}

impl NowledgeMemPlanCacheReport {
    fn from_lookup(lookup: Option<PlanCacheLookup>) -> Self {
        match lookup {
            Some(PlanCacheLookup::Hit) => Self {
                cacheable: true,
                hit: true,
                miss: false,
                bypassed: false,
            },
            Some(PlanCacheLookup::Miss) => Self {
                cacheable: true,
                hit: false,
                miss: true,
                bypassed: false,
            },
            Some(PlanCacheLookup::Bypass(_)) => Self {
                cacheable: false,
                hit: false,
                miss: false,
                bypassed: true,
            },
            None => Self {
                cacheable: false,
                hit: false,
                miss: false,
                bypassed: false,
            },
        }
    }
}

fn nowledge_mem_query_execution_path(
    statement: &cypher::Statement,
) -> NowledgeMemQueryPathDecision {
    let body = nowledge_statement_body(statement);
    let fast_path_reason = match body {
        cypher::Statement::MatchReturn(query) if is_simple_node_lookup(query) => {
            Some("simple_node_lookup")
        }
        cypher::Statement::MatchReturn(query) if is_simple_one_hop_expand(query) => {
            Some("simple_one_hop_expand")
        }
        cypher::Statement::MatchNodesReturn(query) if is_simple_two_node_lookup(query) => {
            Some("simple_two_node_lookup")
        }
        cypher::Statement::ShortestPathReturn(_) => Some("bounded_shortest_path"),
        _ => None,
    };
    NowledgeMemQueryPathDecision {
        execution_path: if fast_path_reason.is_some() {
            NowledgeMemQueryExecutionPath::FastPath
        } else {
            NowledgeMemQueryExecutionPath::OptimizedPath
        },
        fast_path_reason,
    }
}

fn nowledge_statement_body(statement: &cypher::Statement) -> &cypher::Statement {
    match statement {
        cypher::Statement::CypherQuery(query) => &query.statement,
        _ => statement,
    }
}

fn is_simple_node_lookup(query: &cypher::MatchReturn) -> bool {
    !query.properties.is_empty()
        && query.expand.is_none()
        && query.post_match_expand.is_none()
        && query.optional_expand.is_none()
        && query.optional_with.is_none()
        && query.collect_with.is_none()
        && query.distinct_with.is_none()
        && query.with_projection.is_none()
        && query.with_order_by.is_empty()
        && query.with_offset.is_none()
        && query.with_limit.is_none()
        && query.aggregate_with.is_none()
        && query.aggregate_with_filter.is_none()
        && query.post_with_match.is_none()
        && query.predicate.is_none()
        && !query.distinct
        && query.order_by.is_empty()
        && query.offset.is_none()
}

fn is_simple_one_hop_expand(query: &cypher::MatchReturn) -> bool {
    query.expand.as_ref().is_some_and(|expand| {
        expand.min_hops == 1
            && expand.max_hops == 1
            && !query.properties.is_empty()
            && query.post_match_expand.is_none()
            && query.optional_expand.is_none()
            && query.optional_with.is_none()
            && query.collect_with.is_none()
            && query.distinct_with.is_none()
            && query.with_projection.is_none()
            && query.with_order_by.is_empty()
            && query.with_offset.is_none()
            && query.with_limit.is_none()
            && query.aggregate_with.is_none()
            && query.aggregate_with_filter.is_none()
            && query.post_with_match.is_none()
            && query.predicate.is_none()
            && !query.distinct
            && query.order_by.is_empty()
            && query.offset.is_none()
    })
}

fn is_simple_two_node_lookup(query: &cypher::MatchNodesReturn) -> bool {
    !query.left_properties.is_empty()
        && !query.right_properties.is_empty()
        && query.predicate.is_none()
}

fn recovery_mode_name(mode: RecoveryMode) -> &'static str {
    match mode {
        RecoveryMode::TolerateTornTail => "tolerate_torn_tail",
        RecoveryMode::Strict => "strict",
    }
}

fn missing_search_projection_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["search_projection_not_configured"],
    })
}

fn missing_search_projection_shadow_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-shadow-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["search_projection_not_configured"],
    })
}

fn missing_primary_search_projection_probe_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-shadow-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["primary_search_projection_probe_missing"],
    })
}

fn missing_search_candidate_shadow_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        "route": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        "evidence_source": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        "present": false,
        "ready": false,
        "candidate_primary_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
        "request_count": null,
        "primary_candidate_count": null,
        "shadow_candidate_count": null,
        "matched_candidate_count": null,
        "primary_only_candidate_count": null,
        "candidate_identity": {
            "ready": false,
            "parity": false,
        },
        "filter_pushdown_ready": false,
        "filter_pushdown": {
            "ready": false,
            "required_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
            "missing_required_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
            "field_summary_count": 0,
            "field_summaries": [],
            "blocker_codes": ["search_candidate_shadow_evidence_missing"],
        },
        "blocker_codes": ["search_candidate_shadow_evidence_missing"],
    })
}

fn missing_workload_fixture_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
        "present": false,
        "ready": false,
        "route_count": null,
        "query_count": null,
        "failed_query_count": null,
        "bounded_expansion_probe_count": null,
        "failed_bounded_expansion_probe_count": null,
        "search_metadata_probe_count": null,
        "failed_search_metadata_probe_count": null,
        "blocker_codes": ["workload_fixture_evidence_missing"],
    })
}

fn query_family_replacement_evidence_json(
    replacement_readiness_by_query_family: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(families) =
        query_family_replacement_readiness_array(replacement_readiness_by_query_family)
    else {
        return serde_json::json!({
            "protocol": "skein-nowledge-query-family-evidence-v1",
            "present": false,
            "ready": false,
            "required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
            "missing_required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
            "blocker_codes": ["query_family_evidence_missing"],
        });
    };
    let health = replacement_readiness_family_evidence_health(Some(families));
    let blocker_codes = query_family_replacement_blocker_codes(&health);
    serde_json::json!({
        "protocol": "skein-nowledge-query-family-evidence-v1",
        "present": health.present,
        "ready": health.ready,
        "min_replacement_readiness_per_million": health.min_replacement_readiness_per_million,
        "invalid_family_count": health.invalid_family_count,
        "blocked_query_families": health.blocked_query_families,
        "required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
        "missing_required_query_families": health.missing_required_query_families,
        "blocker_codes": blocker_codes,
        "blockers": health.blockers,
        "replacement_readiness_by_query_family": families,
    })
}

fn query_family_replacement_readiness_array(
    value: Option<&serde_json::Value>,
) -> Option<&serde_json::Value> {
    let value = value?;
    if value.is_array() {
        return Some(value);
    }
    value
        .get("replacement_readiness_by_query_family")
        .filter(|families| families.is_array())
}

fn query_family_replacement_blocker_codes(
    health: &crate::nowledge_inventory::ReplacementReadinessFamilyEvidenceHealth,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if health.invalid_family_count > 0 {
        blockers.push("invalid_family_entries");
    }
    if !health.blocked_query_families.is_empty() {
        blockers.push("blocked_query_families");
    }
    if !health.missing_required_query_families.is_empty() {
        blockers.push("missing_required_query_families");
    }
    blockers
}

fn nowledge_mem_graph_route_readiness_json(
    route_readiness: Option<&NowledgeMemRouteReadinessSummary>,
) -> serde_json::Value {
    let Some(summary) = route_readiness else {
        return serde_json::json!({
            "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
            "present": false,
            "ready": false,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "primary_ready_route_count": 0,
            "primary_ready_routes": [],
            "missing_required_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_primary_ready": null,
            "route_query_plan_evidence_ready": null,
            "route_query_profile_evidence_ready": null,
            "relationship_property_pruning_required_count": null,
            "relationship_property_pruning_report_count": null,
            "route_relationship_property_pruning_evidence_ready": null,
            "blocker_codes": ["graph_route_readiness_missing"],
        });
    };

    let missing_required_routes =
        missing_nowledge_mem_bounded_read_routes(&summary.primary_ready_routes);
    let relationship_property_pruning_count_matches = summary
        .relationship_property_pruning_required_count
        == summary.relationship_property_pruning_report_count;
    let mut blocker_codes = Vec::new();
    if !summary.route_primary_ready || !missing_required_routes.is_empty() {
        blocker_codes.push("route_primary_not_ready");
    }
    if !summary.route_query_plan_evidence_ready {
        blocker_codes.push("route_query_plan_evidence_not_ready");
    }
    if !summary.route_query_profile_evidence_ready {
        blocker_codes.push("route_query_profile_evidence_not_ready");
    }
    if !summary.route_relationship_property_pruning_evidence_ready
        || !relationship_property_pruning_count_matches
    {
        blocker_codes.push("route_relationship_property_pruning_not_ready");
    }

    serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
        "present": true,
        "ready": blocker_codes.is_empty(),
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "primary_ready_route_count": summary.primary_ready_routes.len(),
        "primary_ready_routes": summary.primary_ready_routes,
        "missing_required_routes": missing_required_routes,
        "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
        "route_primary_ready": summary.route_primary_ready,
        "route_query_plan_evidence_ready": summary.route_query_plan_evidence_ready,
        "route_query_profile_evidence_ready": summary.route_query_profile_evidence_ready,
        "relationship_property_pruning_required_count": summary.relationship_property_pruning_required_count,
        "relationship_property_pruning_report_count": summary.relationship_property_pruning_report_count,
        "route_relationship_property_pruning_evidence_ready": summary.route_relationship_property_pruning_evidence_ready,
        "blocker_codes": blocker_codes,
    })
}

struct LibraryReadinessEvidence<'a> {
    bounded_read_evidence: &'a serde_json::Value,
    storage_recovery: &'a serde_json::Value,
    background_maintenance: &'a serde_json::Value,
    query_family_evidence: &'a serde_json::Value,
    graph_route_readiness: &'a serde_json::Value,
    search_projection_evidence: &'a serde_json::Value,
    search_projection_shadow_evidence: &'a serde_json::Value,
    search_candidate_shadow_evidence: &'a serde_json::Value,
    workload_fixture_evidence: &'a serde_json::Value,
}

fn library_readiness_blocker_codes(evidence: &LibraryReadinessEvidence<'_>) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !bounded_read_evidence_ready(evidence.bounded_read_evidence) {
        blockers.push("bounded_read_evidence_not_ready");
    }
    if evidence
        .storage_recovery
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("storage_recovery_not_ready");
    }
    if !library_background_maintenance_ready(evidence.background_maintenance) {
        blockers.push("background_maintenance_not_ready");
    }
    if evidence
        .query_family_evidence
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("query_family_evidence_not_ready");
    }
    if evidence
        .graph_route_readiness
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("graph_route_readiness_not_ready");
    }
    if !search_projection_evidence_ready(evidence.search_projection_evidence) {
        blockers.push("search_projection_evidence_not_ready");
    }
    if !search_projection_shadow_evidence_ready(evidence.search_projection_shadow_evidence) {
        blockers.push("search_projection_shadow_evidence_not_ready");
    }
    if !search_candidate_shadow_evidence_ready(evidence.search_candidate_shadow_evidence) {
        blockers.push("search_candidate_shadow_evidence_not_ready");
    }
    if !workload_fixture_evidence_ready(evidence.workload_fixture_evidence) {
        blockers.push("workload_fixture_evidence_not_ready");
    }
    blockers
}

fn library_readiness_by_area(
    evidence: &LibraryReadinessEvidence<'_>,
) -> NowledgeMemReadinessAreaMap {
    NowledgeMemReadinessAreaMap {
        graph: NowledgeMemReadinessAreaSummary::new("graph", true, Vec::new()),
        query: bounded_read_readiness_area(evidence.bounded_read_evidence),
        query_family: readiness_area(
            "query_family",
            evidence.query_family_evidence,
            "query_family_evidence_not_ready",
        ),
        graph_route: readiness_area(
            "graph_route",
            evidence.graph_route_readiness,
            "graph_route_readiness_not_ready",
        ),
        storage: readiness_area(
            "storage",
            evidence.storage_recovery,
            "storage_recovery_not_ready",
        ),
        search_projection: search_projection_readiness_area(evidence.search_projection_evidence),
        search_projection_shadow: search_projection_shadow_readiness_area(
            evidence.search_projection_shadow_evidence,
        ),
        search_candidate_shadow: search_candidate_shadow_readiness_area(
            evidence.search_candidate_shadow_evidence,
        ),
        workload_fixture: workload_fixture_readiness_area(evidence.workload_fixture_evidence),
        background: background_maintenance_readiness_area(evidence.background_maintenance),
    }
}

fn bounded_read_readiness_area(evidence: &serde_json::Value) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = bounded_read_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new("query", blocker_codes.is_empty(), blocker_codes)
}

fn bounded_read_evidence_ready(evidence: &serde_json::Value) -> bool {
    bounded_read_readiness_blocker_codes(evidence).is_empty()
}

fn bounded_read_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("bounded_read_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL) {
        blockers.insert("bounded_read_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("bounded_read_not_ready".to_string());
    }
    if evidence_string(evidence, "mode") != Some(NowledgeMemGraphMode::ShadowReadOnly.as_str()) {
        blockers.insert("bounded_read_not_shadow_read_only".to_string());
    }
    let max_rows = evidence_u64(evidence, "max_rows");
    if !max_rows.is_some_and(|value| value > 0) {
        blockers.insert("bounded_read_missing_max_rows".to_string());
    }
    let expected_execution_row_cap = max_rows.and_then(|value| value.checked_add(1));
    if expected_execution_row_cap.is_none()
        || evidence_u64(evidence, "execution_row_cap") != expected_execution_row_cap
    {
        blockers.insert("bounded_read_execution_row_cap_mismatch".to_string());
    }
    if evidence_u64(evidence, "estimated_payload_bytes").is_none() {
        blockers.insert("bounded_read_estimated_payload_bytes_missing".to_string());
    }
    if !evidence_u64(evidence, "max_estimated_payload_bytes").is_some_and(|value| value > 0) {
        blockers.insert("bounded_read_max_estimated_payload_bytes_missing".to_string());
    }
    if evidence_bool(evidence, "payload_budget_exceeded") != Some(false) {
        blockers.insert("bounded_read_payload_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "row_limit_enforced_before_output") != Some(true) {
        blockers.insert("bounded_read_row_limit_not_enforced_before_output".to_string());
    }
    if evidence_bool(evidence, "operator_row_cap_enabled") != Some(true) {
        blockers.insert("bounded_read_operator_row_cap_disabled".to_string());
    }
    if evidence_bool(evidence, "row_budget_exceeded") == Some(true) {
        blockers.insert("bounded_read_row_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "streaming") != Some(false) {
        blockers.insert("bounded_read_streaming_enabled".to_string());
    }
    if evidence_u64(evidence, "blocking_operator_count") != Some(0) {
        blockers.insert("bounded_read_blocking_operator_present".to_string());
    }
    if !string_array_at(evidence, &["missing_covered_routes"])
        .is_some_and(|routes| routes.is_empty())
    {
        blockers.insert("bounded_read_missing_covered_routes".to_string());
    }
    if evidence_string(evidence, "route_catalog_version")
        != Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
        || evidence_string(evidence, "route_catalog_digest")
            != Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
    {
        blockers.insert("bounded_read_route_catalog_stale".to_string());
    }
    if evidence_bool(evidence, "route_primary_ready") != Some(true)
        || evidence_bool(evidence, "route_query_plan_evidence_ready") != Some(true)
        || evidence_bool(evidence, "route_query_profile_evidence_ready") != Some(true)
        || evidence_bool(
            evidence,
            "route_relationship_property_pruning_evidence_ready",
        ) != Some(true)
    {
        blockers.insert("bounded_read_graph_route_readiness_not_ready".to_string());
    }
    let required_pruning_count =
        evidence_u64(evidence, "relationship_property_pruning_required_count");
    if required_pruning_count.is_none()
        || required_pruning_count
            != evidence_u64(evidence, "relationship_property_pruning_report_count")
    {
        blockers.insert("bounded_read_relationship_property_pruning_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn search_projection_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_projection_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_projection",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_projection_shadow_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_projection_shadow_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_projection_shadow",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_projection_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_projection_readiness_blocker_codes(evidence).is_empty()
}

fn search_projection_shadow_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_projection_shadow_readiness_blocker_codes(evidence).is_empty()
}

fn search_projection_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_projection_not_configured".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL) {
        blockers.insert("search_projection_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_projection_not_ready".to_string());
    }
    if evidence_bool(evidence, "derived_projection") != Some(true) {
        blockers.insert("search_projection_not_derived".to_string());
    }
    if evidence_bool(evidence, "all_tables_covered") != Some(true)
        || !evidence_u64(evidence, "covered_table_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "covered_table_count")
            != evidence_u64(evidence, "required_table_count")
    {
        blockers.insert("search_projection_tables_not_ready".to_string());
    }
    if evidence_bool(evidence, "fts_ready") != Some(true) {
        blockers.insert("search_projection_fts_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_ready") != Some(true) {
        blockers.insert("search_projection_vector_not_ready".to_string());
    }
    if evidence_bool(evidence, "document_identity_ready") != Some(true) {
        blockers.insert("search_projection_document_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "embedding_identity_ready") != Some(true) {
        blockers.insert("search_projection_embedding_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "fail_soft_ready") != Some(true) {
        blockers.insert("search_projection_fail_soft_not_ready".to_string());
    }
    if evidence_bool(evidence, "rebuild_marker_ready") != Some(true) {
        blockers.insert("search_projection_rebuild_marker_not_ready".to_string());
    }
    if evidence_bool(evidence, "metadata_repair_marker_ready") != Some(true) {
        blockers.insert("search_projection_metadata_repair_marker_not_ready".to_string());
    }
    if evidence_bool(evidence, "incremental_update_ready") != Some(true) {
        blockers.insert("search_projection_incremental_update_not_ready".to_string());
    }
    if evidence_bool(evidence, "source_chunk_ready") != Some(true) {
        blockers.insert("search_projection_source_chunk_not_ready".to_string());
    }
    if evidence_bool(evidence, "predicate_pushdown_ready") != Some(true) {
        blockers.insert("search_projection_predicate_pushdown_not_ready".to_string());
    }
    if evidence_bool(evidence, "production_filter_pruning_ready") != Some(true) {
        blockers.insert("search_projection_production_filter_pruning_not_ready".to_string());
    }
    if evidence_bool(evidence, "compressed_vector_projection_ready") == Some(false) {
        blockers.insert("search_projection_compressed_vector_not_ready".to_string());
    }
    blockers.into_iter().collect()
}

fn search_projection_shadow_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_projection_not_configured".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol")
        != Some(NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
    {
        blockers.insert("search_projection_shadow_protocol_mismatch".to_string());
    }
    if evidence_string(evidence, "evidence_source")
        != Some(NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE)
    {
        blockers.insert("search_projection_shadow_evidence_source_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_projection_shadow_not_ready".to_string());
    }
    if evidence_bool(evidence, "primary_ready") != Some(true) {
        blockers.insert("search_projection_shadow_primary_not_ready".to_string());
    }
    if evidence_bool(evidence, "shadow_ready") != Some(true) {
        blockers.insert("search_projection_shadow_shadow_not_ready".to_string());
    }
    if evidence_bool(evidence, "document_count_parity") != Some(true)
        || evidence_bool(evidence, "document_identity_parity") != Some(true)
    {
        blockers.insert("search_projection_shadow_document_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["table_parity", "ready"]) != Some(true)
        && evidence_bool(evidence, "table_parity_ready") != Some(true)
    {
        blockers.insert("search_projection_shadow_table_parity_not_ready".to_string());
    }
    if evidence_bool(evidence, "embedding_identity_parity") != Some(true) {
        blockers.insert("search_projection_shadow_embedding_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "lifecycle_parity") != Some(true) {
        blockers.insert("search_projection_shadow_lifecycle_not_ready".to_string());
    }
    if evidence_bool(evidence, "incremental_watermark_parity") != Some(true) {
        blockers.insert("search_projection_shadow_incremental_watermark_not_ready".to_string());
    }
    if evidence_bool(evidence, "predicate_pushdown_parity") != Some(true) {
        blockers.insert("search_projection_shadow_predicate_pushdown_not_ready".to_string());
    }
    if nested_bool(evidence, &["pushdown_evidence", "ready"]) != Some(true) {
        blockers.insert(SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY.to_string());
    }
    if nested_bool(
        evidence,
        &[
            "pushdown_evidence",
            "shadow_persisted_segment_descriptor_ready",
        ],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING.to_string());
    }
    if nested_bool(
        evidence,
        &[
            "pushdown_evidence",
            "shadow_segment_descriptor_scan_filter_fields_ready",
        ],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING.to_string());
    }
    blockers.into_iter().collect()
}

fn search_candidate_shadow_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_candidate_shadow_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_candidate_shadow",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_candidate_shadow_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_candidate_shadow_readiness_blocker_codes(evidence).is_empty()
}

fn workload_fixture_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = workload_fixture_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "workload_fixture",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn workload_fixture_evidence_ready(evidence: &serde_json::Value) -> bool {
    workload_fixture_readiness_blocker_codes(evidence).is_empty()
}

fn workload_fixture_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("workload_fixture_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL)
    {
        blockers.insert("workload_fixture_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("workload_fixture_not_ready".to_string());
    }
    if !evidence_u64(evidence, "route_count").is_some_and(|count| count > 0)
        || !evidence_u64(evidence, "query_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_query_count") != Some(0)
    {
        blockers.insert("workload_fixture_route_queries_not_ready".to_string());
    }
    if !evidence_u64(evidence, "bounded_expansion_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_bounded_expansion_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_bounded_expansion_not_ready".to_string());
    }
    if !evidence_u64(evidence, "search_metadata_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_search_metadata_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_search_metadata_not_ready".to_string());
    }
    blockers.into_iter().collect()
}

fn search_candidate_shadow_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_candidate_shadow_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
    {
        blockers.insert("search_candidate_shadow_protocol_mismatch".to_string());
    }
    if evidence_string(evidence, "route") != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE) {
        blockers.insert("search_candidate_shadow_route_mismatch".to_string());
    }
    if evidence_string(evidence, "evidence_source")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE)
    {
        blockers.insert("search_candidate_shadow_evidence_source_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_candidate_shadow_not_ready".to_string());
    }
    if evidence_string(evidence, "candidate_primary_engine")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE)
    {
        blockers.insert("search_candidate_primary_engine_not_skein".to_string());
    }
    if !search_candidate_shadow_counts_ready(evidence) {
        blockers.insert("search_candidate_counts_not_ready".to_string());
    }
    if evidence_bool(evidence, "text_retriever_ready") != Some(true) {
        blockers.insert("search_candidate_text_retriever_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_retriever_ready") != Some(true) {
        blockers.insert("search_candidate_vector_retriever_not_ready".to_string());
    }
    if evidence_bool(evidence, "fts_top_k_overlap_ready") != Some(true) {
        blockers.insert("search_candidate_fts_top_k_overlap_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_top_k_overlap_ready") != Some(true) {
        blockers.insert("search_candidate_vector_top_k_overlap_not_ready".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "source_chunk_identity_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_source_chunk_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["candidate_readiness", "fail_soft_observed"]) != Some(true) {
        blockers.insert("search_candidate_fail_soft_not_observed".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "projection_marker_status_visible"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_projection_marker_status_missing".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "projection_watermark_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_projection_watermark_missing".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "embedding_identity_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_embedding_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["candidate_identity", "ready"]) != Some(true)
        || nested_bool(evidence, &["candidate_identity", "parity"]) != Some(true)
    {
        blockers.insert("search_candidate_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["filter_pushdown", "ready"]) != Some(true)
        || evidence_bool(evidence, "filter_pushdown_ready") != Some(true)
    {
        blockers.insert("search_candidate_filter_pushdown_not_ready".to_string());
    }
    if !nested_u64(evidence, &["filter_pushdown", "field_summary_count"])
        .is_some_and(|count| count > 0)
        || !string_array_at(evidence, &["filter_pushdown", "missing_required_fields"])
            .is_some_and(|fields| fields.is_empty())
    {
        blockers.insert("search_candidate_field_pruning_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn evidence_string<'a>(evidence: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    evidence.get(field).and_then(serde_json::Value::as_str)
}

fn evidence_bool(evidence: &serde_json::Value, field: &str) -> Option<bool> {
    evidence.get(field).and_then(serde_json::Value::as_bool)
}

fn evidence_u64(evidence: &serde_json::Value, field: &str) -> Option<u64> {
    evidence.get(field).and_then(serde_json::Value::as_u64)
}

fn nested_value<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

fn nested_bool(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    nested_value(value, path).and_then(serde_json::Value::as_bool)
}

fn nested_u64(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    nested_value(value, path).and_then(serde_json::Value::as_u64)
}

fn string_array_at(value: &serde_json::Value, path: &[&str]) -> Option<Vec<String>> {
    nested_value(value, path)?
        .as_array()?
        .iter()
        .map(|item| item.as_str().map(str::to_string))
        .collect()
}

fn evidence_blocker_codes(evidence: &serde_json::Value) -> BTreeSet<String> {
    evidence
        .get("blocker_codes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn search_candidate_shadow_counts_ready(evidence: &serde_json::Value) -> bool {
    let request_count = evidence_u64(evidence, "request_count");
    let primary_candidate_count = evidence_u64(evidence, "primary_candidate_count");
    let shadow_candidate_count = evidence_u64(evidence, "shadow_candidate_count");
    let matched_candidate_count = evidence_u64(evidence, "matched_candidate_count");
    let primary_only_candidate_count = evidence_u64(evidence, "primary_only_candidate_count");
    request_count.is_some_and(|count| count > 0)
        && primary_candidate_count.is_some()
        && primary_candidate_count == shadow_candidate_count
        && matched_candidate_count == shadow_candidate_count
        && primary_only_candidate_count == Some(0)
}

fn readiness_area(
    name: &'static str,
    evidence: &serde_json::Value,
    fallback_blocker_code: &'static str,
) -> NowledgeMemReadinessAreaSummary {
    let ready = evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true);
    NowledgeMemReadinessAreaSummary::new(
        name,
        ready,
        readiness_blocker_codes(evidence, fallback_blocker_code, ready),
    )
}

fn background_maintenance_readiness_area(
    background_maintenance: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let health = background_maintenance_evidence_health(Some(background_maintenance), true);
    NowledgeMemReadinessAreaSummary::new("background", health.ready, health.blocker_codes)
}

fn library_background_maintenance_ready(background_maintenance: &serde_json::Value) -> bool {
    background_maintenance_evidence_health(Some(background_maintenance), true).ready
}

fn readiness_blocker_codes(
    evidence: &serde_json::Value,
    fallback_blocker_code: &'static str,
    ready: bool,
) -> Vec<String> {
    if ready {
        return Vec::new();
    }
    let codes = evidence
        .get("blocker_codes")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if codes.is_empty() {
        vec![fallback_blocker_code.to_string()]
    } else {
        codes
    }
}

fn nowledge_mem_readiness_dashboard_areas(
    library: &NowledgeMemLibraryReadinessReport,
    slow_query: &NowledgeMemSlowQueryReport,
) -> Vec<NowledgeMemReadinessAreaSummary> {
    let mut areas = library.areas();
    areas.push(NowledgeMemReadinessAreaSummary {
        name: "slow_query".to_string(),
        ready: slow_query.ready,
        blocker_codes: if slow_query.ready {
            Vec::new()
        } else {
            vec!["slow_query_report_not_ready".to_string()]
        },
    });
    areas
}

fn nowledge_mem_search_candidate_report(
    request: &NowledgeMemSearchCandidateRequest,
    effective_compressed_vector_search_mode: CompressedVectorSearchMode,
    result: &SearchResultSet,
) -> NowledgeMemSearchCandidateReport {
    let pushdown = &result.candidate_set.metadata_predicate_pushdown;
    let returned_kind_counts = search_candidate_returned_kind_counts(&result.hits);
    let returned_missing_external_id_count = result
        .hits
        .iter()
        .filter(|hit| hit.external_id.is_none())
        .count();
    let returned_missing_source_id_count = result
        .hits
        .iter()
        .filter(|hit| hit.source_id.is_none())
        .count();
    NowledgeMemSearchCandidateReport {
        protocol: NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL.to_string(),
        compressed_vector_search_mode: effective_compressed_vector_search_mode,
        requested_compressed_vector_search_mode: request.compressed_vector_search_mode,
        retrieval_projection_advisor: request.retrieval_projection_advisor.clone(),
        retrieval_projection_advisor_blocker_codes: if request.compressed_vector_search_mode
            == effective_compressed_vector_search_mode
        {
            Vec::new()
        } else {
            request.retrieval_projection_advisor.blocker_codes()
        },
        mode: request.mode,
        query_embedding_dimension: request.query_embedding.as_ref().map(std::vec::Vec::len),
        limit: result.limit,
        rank_window: result.rank_window,
        document_count: result.document_count,
        filtered_document_count: result.filtered_document_count,
        total_hits: result.total_hits,
        returned_hit_count: result.hits.len(),
        returned_kind_counts,
        returned_missing_external_id_count,
        returned_missing_source_id_count,
        truncated: result.truncated,
        candidate_set: result.candidate_set.clone(),
        filtered_out_count: result.candidate_set.filtered_out_count,
        metadata_filter_count: result.candidate_set.metadata_filters.len(),
        pushed_predicate_count: pushdown.pushed_predicate_count,
        residual_predicate_count: pushdown.residual_predicate_count,
        segment_count: pushdown.segment_count,
        pruned_segment_count: pushdown.pruned_segment_count,
        scanned_segment_count: pushdown.scanned_segment_count,
        segment_pruning_candidate_document_count: pushdown.segment_pruning_candidate_document_count,
        segment_pruned_document_count: pushdown.segment_pruned_document_count,
        segment_scanned_document_count: pushdown.segment_scanned_document_count,
        persisted_segment_descriptor_used: pushdown.persisted_segment_descriptor_used,
        retriever_backends: result
            .retrievers
            .iter()
            .map(|retriever| (retriever.name.clone(), retriever.backend.clone()))
            .collect(),
        retriever_available: result
            .retrievers
            .iter()
            .map(|retriever| (retriever.name.clone(), retriever.available))
            .collect(),
        retriever_candidate_counts: result
            .retrievers
            .iter()
            .map(|retriever| (retriever.name.clone(), retriever.candidate_count))
            .collect(),
        fallback_reason_codes: result
            .fallback_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        empty_reason_codes: result
            .empty_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        truncation_reason_codes: result
            .truncation_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        projection_full_reindex_needed: result.projection_freshness.full_reindex_needed,
        projection_metadata_repair_needed: result.projection_freshness.metadata_repair_needed,
        projection_source_graph_commit_epoch: result.projection_freshness.source_graph_commit_epoch,
        projection_embedding_model: result.projection_freshness.embedding_model.clone(),
        projection_embedding_version: result.projection_freshness.embedding_version.clone(),
        projection_embedding_dimension: result.projection_freshness.embedding_dimension,
    }
}

#[allow(clippy::too_many_arguments)]
fn search_candidate_readiness_blocker_codes(
    candidate_report: &NowledgeMemSearchCandidateReport,
    options: &NowledgeMemSearchCandidateReadinessOptions,
    metadata_pushdown_ready: bool,
    segment_descriptor_ready: bool,
    text_retriever_ready: bool,
    vector_retriever_ready: bool,
    source_chunk_identity_ready: bool,
    fail_soft_observed: bool,
    projection_marker_status_visible: bool,
    projection_watermark_ready: bool,
    embedding_identity_ready: bool,
) -> Vec<String> {
    let mut blockers = BTreeSet::new();

    if candidate_report.protocol != NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL {
        blockers.insert("search_candidate_report_protocol_mismatch");
    }
    if options.require_hits && candidate_report.returned_hit_count == 0 {
        blockers.insert("search_candidate_no_hits");
    }
    if options.require_metadata_pushdown {
        if candidate_report.metadata_filter_count == 0 {
            blockers.insert("search_candidate_metadata_filter_missing");
        }
        if !metadata_pushdown_ready {
            blockers.insert("search_candidate_metadata_filter_not_fully_pushed");
        }
    }
    if candidate_report.residual_predicate_count > 0 {
        blockers.insert("search_candidate_metadata_filter_residual");
    }
    if options.require_segment_descriptor && !segment_descriptor_ready {
        blockers.insert("search_candidate_segment_descriptor_not_used");
    }
    if options.require_text_retriever && !text_retriever_ready {
        blockers.insert("search_candidate_text_retriever_unavailable");
    }
    if options.require_vector_retriever && !vector_retriever_ready {
        blockers.insert("search_candidate_vector_retriever_unavailable");
    }
    if options.require_source_chunk_identity && !source_chunk_identity_ready {
        blockers.insert("search_candidate_source_chunk_identity_missing");
    }
    if options.require_fail_soft_observation && !fail_soft_observed {
        blockers.insert("search_candidate_fail_soft_not_observed");
    }
    if options.require_projection_marker_status && !projection_marker_status_visible {
        blockers.insert("search_candidate_projection_marker_status_missing");
    }
    if options.require_projection_watermark && !projection_watermark_ready {
        blockers.insert("search_candidate_projection_watermark_missing");
    }
    if options.require_embedding_identity && !embedding_identity_ready {
        blockers.insert("search_candidate_embedding_identity_not_ready");
    }

    blockers
        .into_iter()
        .map(std::string::ToString::to_string)
        .collect()
}

fn search_candidate_embedding_identity_ready(
    candidate_report: &NowledgeMemSearchCandidateReport,
    options: &NowledgeMemSearchCandidateReadinessOptions,
) -> bool {
    if !options.require_embedding_identity {
        return true;
    }
    let manifest_present = candidate_report.projection_embedding_model.is_some()
        && candidate_report.projection_embedding_dimension.is_some();
    if !manifest_present {
        return false;
    }
    let model_matches = options
        .active_embedding_model
        .as_deref()
        .is_none_or(|active| {
            candidate_report.projection_embedding_model.as_deref() == Some(active)
        });
    let dimension_matches = options
        .active_embedding_dimension
        .is_none_or(|active| candidate_report.projection_embedding_dimension == Some(active));
    model_matches && dimension_matches
}

fn search_candidate_returned_kind_counts(
    hits: &[crate::search::SearchHit],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for hit in hits {
        let kind = hit.kind.as_deref().unwrap_or("unknown");
        *counts.entry(kind.to_string()).or_insert(0) += 1;
    }
    counts
}

fn search_candidate_set_report_json(report: &SearchCandidateSetReport) -> serde_json::Value {
    serde_json::json!({
        "id_space": report.id_space,
        "representation": report.representation,
        "cardinality": report.cardinality,
        "exact": report.exact,
        "snapshot_source_graph_commit_epoch": report.snapshot_source_graph_commit_epoch,
        "policy_epoch": report.policy_epoch,
        "filtered_out_count": report.filtered_out_count,
        "metadata_filters": report.metadata_filters,
        "metadata_predicate_pushdown": search_predicate_pushdown_report_json(&report.metadata_predicate_pushdown),
    })
}

fn search_predicate_pushdown_report_json(
    report: &crate::search::SearchPredicatePushdownReport,
) -> serde_json::Value {
    serde_json::json!({
        "input_predicate_count": report.input_predicate_count,
        "pushed_predicate_count": report.pushed_predicate_count,
        "residual_predicate_count": report.residual_predicate_count,
        "unsatisfiable": report.unsatisfiable,
        "parse_error": report.parse_error,
        "segment_count": report.segment_count,
        "pruned_segment_count": report.pruned_segment_count,
        "scanned_segment_count": report.scanned_segment_count,
        "segment_pruning_candidate_document_count": report.segment_pruning_candidate_document_count,
        "segment_pruned_document_count": report.segment_pruned_document_count,
        "segment_scanned_document_count": report.segment_scanned_document_count,
        "persisted_segment_descriptor_used": report.persisted_segment_descriptor_used,
        "field_summaries": report.field_summaries.iter().map(search_predicate_field_pruning_report_json).collect::<Vec<_>>(),
    })
}

fn search_predicate_field_pruning_report_json(
    report: &crate::search::SearchPredicateFieldPruningReport,
) -> serde_json::Value {
    serde_json::json!({
        "field": report.field,
        "value_kind": report.value_kind,
        "operation_kinds": report.operation_kinds,
        "segment_count": report.segment_count,
        "pruned_segment_count": report.pruned_segment_count,
        "scanned_segment_count": report.scanned_segment_count,
        "numeric_range_summary_used": report.numeric_range_summary_used,
        "timestamp_range_summary_used": report.timestamp_range_summary_used,
        "value_summary_used": report.value_summary_used,
    })
}

fn search_mode_name(mode: SearchMode) -> &'static str {
    match mode {
        SearchMode::Hybrid => "hybrid",
        SearchMode::Vector => "vector",
        SearchMode::Text => "text",
    }
}

fn nowledge_mem_retrieval_report(
    mode: NowledgeMemGraphMode,
    compressed_vector_search_mode: CompressedVectorSearchMode,
    output: &KnowledgeRetrievalOutput,
) -> NowledgeMemRetrievalReport {
    let vector_backend = output
        .search
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .map(|retriever| retriever.backend.clone());
    let text_backend = output
        .search
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .map(|retriever| retriever.backend.clone());
    let search_backend = vector_backend
        .clone()
        .or_else(|| text_backend.clone())
        .or_else(|| {
            output
                .search
                .retrievers
                .first()
                .map(|retriever| retriever.backend.clone())
        });
    let knowledge_fallback_reason_codes = output
        .diagnostics
        .graph_context_fallback_reason_codes
        .iter()
        .map(|code| code.as_str().to_string())
        .collect::<Vec<_>>();
    let retriever_fallback_reason_codes = output
        .retrievers
        .iter()
        .flat_map(|retriever| retriever.fallback_reason_codes.iter())
        .map(|code| code.as_str().to_string())
        .collect::<Vec<_>>();
    let truncation_reason_codes = output
        .diagnostics
        .search_truncation_reason_codes
        .iter()
        .map(|code| code.as_str().to_string())
        .chain(
            output
                .diagnostics
                .graph_seed_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .chain(
            output
                .diagnostics
                .graph_context_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .chain(
            output
                .diagnostics
                .candidate_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .collect::<Vec<_>>();
    NowledgeMemRetrievalReport {
        protocol: NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL.to_string(),
        mode,
        compressed_vector_search_mode,
        graph_commit_epoch: output.graph_commit_epoch,
        projection_source_graph_commit_epoch: output.projection_freshness.source_graph_commit_epoch,
        projection_commit_lag: output.diagnostics.projection_commit_lag,
        projection_stale: output.diagnostics.projection_stale,
        search_document_count: output.search.document_count,
        search_filtered_document_count: output.search.filtered_document_count,
        search_total_hits: output.search.total_hits,
        candidate_count: output.candidates.len(),
        candidate_total_count: output.diagnostics.candidate_total_count,
        evidence_count: output.evidence.len(),
        graph_seed_count: output.graph_seeds.len(),
        graph_context_path_count: output.graph_context_paths.len(),
        search_backend,
        vector_backend,
        text_backend,
        search_fallback_reason_codes: output
            .diagnostics
            .search_fallback_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        retriever_fallback_reason_codes,
        knowledge_fallback_reason_codes,
        truncation_reason_codes,
        warning_count: output.diagnostics.warnings.len(),
        warnings: output.diagnostics.warnings.clone(),
    }
}

fn nowledge_mem_read_report(
    mode: NowledgeMemGraphMode,
    output: &QueryOutput,
    options: &NowledgeMemReadOptions,
    execution_profile: &ReadExecutionProfile,
) -> NowledgeMemReadReport {
    let estimated_payload_bytes = estimate_query_output_payload_bytes(output);
    NowledgeMemReadReport {
        protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
        mode,
        row_count: output.rows.len(),
        max_rows: options.max_rows,
        execution_row_cap: execution_profile.detection_row_cap,
        estimated_payload_bytes,
        max_estimated_payload_bytes: options.max_estimated_payload_bytes,
        row_budget_exceeded: options
            .max_rows
            .is_some_and(|max_rows| output.rows.len() > max_rows),
        payload_budget_exceeded: options
            .max_estimated_payload_bytes
            .is_some_and(|max_bytes| estimated_payload_bytes > max_bytes),
        row_limit_enforced_before_output: execution_profile.row_limit_enforced_before_output,
        operator_row_cap_enabled: execution_profile.operator_row_cap_enabled,
        blocking_operator_count: execution_profile.blocking_operator_count(),
        blocking_operator_kinds: execution_profile.blocking_operator_kinds.clone(),
        streaming: false,
    }
}

fn estimate_query_output_payload_bytes(output: &QueryOutput) -> usize {
    output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(key, value)| key.len() + estimate_value_payload_bytes(value))
                .sum::<usize>()
        })
        .sum()
}

fn estimate_value_payload_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => std::mem::size_of::<i64>(),
        Value::String(value) => value.len(),
        Value::List(values) => values.iter().map(estimate_value_payload_bytes).sum(),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| key.len() + estimate_value_payload_bytes(value))
            .sum(),
    }
}

fn nowledge_value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::json!(value),
        Value::Int(value) => serde_json::json!(value),
        Value::Float(value) => serde_json::json!(value),
        Value::String(value) => serde_json::json!(value),
        Value::List(values) => {
            serde_json::Value::Array(values.iter().map(nowledge_value_json).collect())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), nowledge_value_json(value)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_bounded_read_evidence_json,
        nowledge_mem_bounded_read_evidence_json_with_route_readiness, nowledge_mem_graph_config,
        nowledge_mem_graph_config_with_search_mode,
        nowledge_mem_search_candidate_shadow_evidence_json, required_u64_field,
        NowledgeMemEmbeddedStore, NowledgeMemEmbeddedStoreHandle, NowledgeMemGraph,
        NowledgeMemGraphAugmentationStateOptions, NowledgeMemGraphCommunityMembersOptions,
        NowledgeMemGraphCommunityRecentMemoriesOptions, NowledgeMemGraphCommunitySubgraphOptions,
        NowledgeMemGraphMode, NowledgeMemGraphNodeDetailsOptions, NowledgeMemGraphOrphansOptions,
        NowledgeMemGraphOverviewOptions, NowledgeMemGraphPageRankPlanOptions,
        NowledgeMemGraphSampleOptions, NowledgeMemOpenDiagnosticOptions, NowledgeMemOpenOptions,
        NowledgeMemQueryExecutionPath, NowledgeMemQueryReportOptions, NowledgeMemReadOptions,
        NowledgeMemReadReport, NowledgeMemReadinessAreaSummary, NowledgeMemReadinessDashboard,
        NowledgeMemReadinessOptions, NowledgeMemRetrievalProjectionAdvisor,
        NowledgeMemRouteReadinessSummary, NowledgeMemSearchCandidateReadinessOptions,
        NowledgeMemSearchCandidateRequest, NowledgeMemSearchCandidateShadowAccumulator,
        NowledgeMemSearchCandidateShadowEvidence, NowledgeMemSearchProjection,
        NowledgeMemStorageRecoveryReport, NowledgeQueryRuntimePreflightProbe,
        NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL, NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE,
        NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE_REPORT_PROTOCOL,
        NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE,
        NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE_REPORT_PROTOCOL,
        NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE,
        NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE_REPORT_PROTOCOL,
        NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE,
        NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE_REPORT_PROTOCOL,
        NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE,
        NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE_REPORT_PROTOCOL, NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE,
        NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE_REPORT_PROTOCOL, NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE,
        NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE_REPORT_PROTOCOL, NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE,
        NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE_REPORT_PROTOCOL, NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE,
        NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE_REPORT_PROTOCOL, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
        NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
        NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
        NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL, NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES, SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY,
    };
    use crate::mem_integration_readiness::nowledge_mem_final_cutover_preflight;
    use crate::search::CompressedVectorSearchMode;
    use crate::search::SearchFusionWeights;
    use crate::workload_fixtures::{
        nowledge_graph_route_workload_fixture_report, NowledgeGraphRouteWorkloadFixtureOptions,
    };
    use crate::Value;
    use crate::{
        BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundWorkHint, Database,
        DatabaseConfig, KnowledgeCandidateScoringPolicy, KnowledgeRetrievalRequest, LocalQosPolicy,
        LocalQosScheduler, LocalQosState, NowledgeGraphStatement, RecoveryMode,
        SearchEmbeddingManifest, SearchIndex, SearchMode, SearchProjectionDelta,
        SearchProjectionKind, SearchProjectionProbeOptions, SearchProjectionRow,
        StorageRecoveryReport, WorkClass,
    };
    use std::collections::BTreeMap;
    use std::thread;

    #[test]
    fn graph_config_tracks_shadow_vs_cutover_mode() {
        assert!(nowledge_mem_graph_config(NowledgeMemGraphMode::ShadowReadOnly).read_only);
        assert!(!nowledge_mem_graph_config(NowledgeMemGraphMode::WritableCutover).read_only);
        assert_eq!(
            nowledge_mem_graph_config(NowledgeMemGraphMode::ShadowReadOnly)
                .compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert_eq!(
            nowledge_mem_graph_config_with_search_mode(
                NowledgeMemGraphMode::ShadowReadOnly,
                CompressedVectorSearchMode::Required,
            )
            .compressed_vector_search_mode,
            CompressedVectorSearchMode::Required
        );
    }

    #[test]
    fn graph_facade_executes_cypher_through_library_api() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

        graph
            .query("CREATE (:Memory {id: 'mem-1', title: 'Library seam'})")
            .unwrap();
        let output = graph
            .query("MATCH (m:Memory {id: 'mem-1'}) RETURN m.title AS title")
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(graph.mode(), NowledgeMemGraphMode::WritableCutover);
    }

    #[test]
    fn graph_overview_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_overview_memories(&mut graph);

        let output = graph
            .read_graph_overview(&NowledgeMemGraphOverviewOptions {
                limit: 2,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 2);
        assert_eq!(
            output.rows[0].memory_id.as_deref(),
            Some("overview-memory-1")
        );
        assert_eq!(output.rows[0].label.as_deref(), Some("Overview One"));
        assert_eq!(output.rows[0].content_preview.as_deref(), Some("body one"));
        assert_eq!(output.rows[0].score, Some(3.0));
        assert_eq!(
            output.rows[1].memory_id.as_deref(),
            Some("overview-memory-2")
        );
        assert_eq!(output.rows[1].label.as_deref(), Some("Fallback body"));
        assert_eq!(output.rows[1].title, None);
        assert_eq!(output.rows[1].score, Some(2.0));
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE);
        assert_eq!(
            output.report.read_engine,
            crate::route_ownership::NowledgeMemRouteReadEngine::Skein
        );
        assert_eq!(output.report.row_count, 2);
        assert_eq!(output.report.read_report.row_count, 2);
        assert!(output.report.read_report.row_limit_enforced_before_output);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(output.json()["rows"][0]["memory_id"], "overview-memory-1");
    }

    #[test]
    fn embedded_store_handle_exposes_graph_overview_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_overview_memories(store.graph_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_overview(&NowledgeMemGraphOverviewOptions {
                limit: 1,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].memory_id.as_deref(),
            Some("overview-memory-1")
        );
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE);
    }

    #[test]
    fn graph_sample_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_sample_memories(&mut graph);

        let output = graph
            .read_graph_sample(&NowledgeMemGraphSampleOptions {
                limit: 2,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].memory_id.as_deref(), Some("sample-memory-a"));
        assert_eq!(output.rows[0].label.as_deref(), Some("Sample A"));
        assert_eq!(output.rows[0].score, Some(1.0));
        assert_eq!(output.rows[1].memory_id.as_deref(), Some("sample-memory-b"));
        assert_eq!(output.rows[1].label.as_deref(), Some("Sample body B"));
        assert_eq!(output.rows[1].title, None);
        assert_eq!(output.rows[1].score, Some(2.0));
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE);
        assert_eq!(
            output.report.read_engine,
            crate::route_ownership::NowledgeMemRouteReadEngine::Skein
        );
        assert_eq!(output.report.row_count, 2);
        assert_eq!(output.report.read_report.row_count, 2);
        assert!(output.report.read_report.row_limit_enforced_before_output);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(output.json()["rows"][0]["memory_id"], "sample-memory-a");
    }

    #[test]
    fn embedded_store_handle_exposes_graph_sample_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_sample_memories(store.graph_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_sample(&NowledgeMemGraphSampleOptions {
                limit: 1,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].memory_id.as_deref(), Some("sample-memory-a"));
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE);
    }

    #[test]
    fn graph_node_details_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'detail-memory-1', title: 'Detail One', content: 'detail body', summary: 'detail summary', source: 'detail-source', space_id: 'default', community_id: 42, created_at: 101, updated_at: 201, event_start: 301, event_end: 401, importance: 0.8, confidence: 0.7, is_latest: true, is_deleted: false})")
            .unwrap();
        let node_id = memory_node_id(&mut graph, "detail-memory-1");

        let output = graph
            .read_graph_node_details(&NowledgeMemGraphNodeDetailsOptions {
                node_id,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();
        let node = output.node.as_ref().expect("node details row");

        assert_eq!(node.node_id, node_id);
        assert_eq!(node.memory_id.as_deref(), Some("detail-memory-1"));
        assert_eq!(node.node_kind, "Memory");
        assert_eq!(node.label.as_deref(), Some("Detail One"));
        assert_eq!(node.content.as_deref(), Some("detail body"));
        assert_eq!(node.content_preview.as_deref(), Some("detail body"));
        assert_eq!(node.summary.as_deref(), Some("detail summary"));
        assert_eq!(node.raw_space_id.as_deref(), Some("default"));
        assert_eq!(node.is_latest, Some(true));
        assert_eq!(node.is_deleted, Some(false));
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE);
        assert_eq!(output.report.node_id, node_id);
        assert_eq!(output.report.row_count, 1);
        assert_eq!(output.report.read_report.row_count, 1);
        assert!(output.report.read_report.row_limit_enforced_before_output);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(output.json()["node"]["memory_id"], "detail-memory-1");
    }

    #[test]
    fn embedded_store_handle_exposes_graph_node_details_route() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'detail-memory-handle', title: 'Handle Detail'})")
            .unwrap();
        let node_id = memory_node_id(&mut graph, "detail-memory-handle");
        let store = NowledgeMemEmbeddedStore::new(graph, None);
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_node_details(&NowledgeMemGraphNodeDetailsOptions {
                node_id,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(
            output
                .node
                .as_ref()
                .and_then(|node| node.memory_id.as_deref()),
            Some("detail-memory-handle")
        );
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE);

        let missing = handle
            .read_graph_node_details(&NowledgeMemGraphNodeDetailsOptions {
                node_id: node_id + 10_000,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();
        assert!(missing.node.is_none());
        assert_eq!(missing.report.row_count, 0);
    }

    #[test]
    fn graph_community_members_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_community_members_memories(&mut graph);

        let output = graph
            .read_graph_community_members(&NowledgeMemGraphCommunityMembersOptions {
                community_id: 42,
                limit: 2,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 2);
        assert_eq!(
            output.rows[0].memory_id.as_deref(),
            Some("community-memory-high")
        );
        assert_eq!(output.rows[0].score, Some(3.0));
        assert_eq!(
            output.rows[1].memory_id.as_deref(),
            Some("community-memory-low")
        );
        assert_eq!(output.rows[1].score, Some(1.0));
        assert!(output
            .rows
            .iter()
            .all(|row| row.community_id.as_ref() == Some(&Value::Int(42))));
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE
        );
        assert_eq!(output.report.community_id, 42);
        assert_eq!(output.report.row_count, 2);
        assert_eq!(output.report.read_report.row_count, 2);
        assert!(output.report.read_report.row_limit_enforced_before_output);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(
            output.json()["rows"][0]["memory_id"],
            "community-memory-high"
        );
    }

    #[test]
    fn embedded_store_handle_exposes_graph_community_members_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_community_members_memories(store.graph_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_community_members(&NowledgeMemGraphCommunityMembersOptions::new(42, 1))
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].memory_id.as_deref(),
            Some("community-memory-high")
        );
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE
        );

        let missing = handle
            .read_graph_community_members(&NowledgeMemGraphCommunityMembersOptions::new(404, 10))
            .unwrap();
        assert!(missing.rows.is_empty());
        assert_eq!(missing.report.row_count, 0);
    }

    #[test]
    fn graph_community_recent_memories_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_community_recent_memories(&mut graph);

        let output = graph
            .read_graph_community_recent_memories(&NowledgeMemGraphCommunityRecentMemoriesOptions {
                community_id: 3676,
                limit: 2,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 2);
        assert_eq!(
            output.rows[0].memory_id.as_deref(),
            Some("community-recent-new")
        );
        assert_eq!(output.rows[0].title.as_deref(), Some("Recent New"));
        assert_eq!(output.rows[0].content.as_deref(), Some("new body"));
        assert_eq!(output.rows[0].is_crystal, Some(false));
        assert_eq!(output.rows[0].mention_breadth, 2);
        assert_eq!(
            output.rows[1].memory_id.as_deref(),
            Some("community-recent-old")
        );
        assert_eq!(output.rows[1].mention_breadth, 1);
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE
        );
        assert_eq!(output.report.community_id, 3676);
        assert_eq!(output.report.row_count, 2);
        assert_eq!(output.report.read_report.row_count, 2);
        assert!(output.report.read_report.row_limit_enforced_before_output);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(
            output.json()["rows"][0]["memory_id"],
            "community-recent-new"
        );
        assert_eq!(output.json()["rows"][0]["mention_breadth"], 2);
    }

    #[test]
    fn embedded_store_handle_exposes_graph_community_recent_memories_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_community_recent_memories(store.graph_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_community_recent_memories(
                &NowledgeMemGraphCommunityRecentMemoriesOptions::new(3676, 1),
            )
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].memory_id.as_deref(),
            Some("community-recent-new")
        );
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE
        );

        let missing = handle
            .read_graph_community_recent_memories(
                &NowledgeMemGraphCommunityRecentMemoriesOptions::new(4242, 10),
            )
            .unwrap();
        assert!(missing.rows.is_empty());
        assert_eq!(missing.report.row_count, 0);
    }

    #[test]
    fn graph_community_subgraph_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_community_subgraph(&mut graph);

        let output = graph
            .read_graph_community_subgraph(&NowledgeMemGraphCommunitySubgraphOptions {
                community_id: 3505,
                max_entities: 3,
                max_edges: 5,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.entities.len(), 2);
        assert_eq!(
            output.entities[0].entity_id.as_deref(),
            Some("community-subgraph-alpha")
        );
        assert_eq!(output.entities[0].mention_count, 2);
        assert_eq!(
            output.entities[1].entity_id.as_deref(),
            Some("community-subgraph-beta")
        );
        assert_eq!(output.entities[1].mention_count, 1);
        assert_eq!(output.edges.len(), 1);
        assert_eq!(
            output.edges[0].source_entity_id.as_deref(),
            Some("community-subgraph-alpha")
        );
        assert_eq!(
            output.edges[0].target_entity_id.as_deref(),
            Some("community-subgraph-beta")
        );
        assert_eq!(output.edges[0].relation_type.as_deref(), Some("related"));
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE
        );
        assert_eq!(output.report.community_id, 3505);
        assert_eq!(output.report.entity_count, 2);
        assert_eq!(output.report.edge_count, 1);
        assert_eq!(output.report.entity_read_report.row_count, 2);
        assert!(output.report.edge_read_report.is_some());
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(
            output.json()["entities"][0]["entity_id"],
            "community-subgraph-alpha"
        );
        assert_eq!(
            output.json()["edges"][0]["target_entity_id"],
            "community-subgraph-beta"
        );
    }

    #[test]
    fn embedded_store_handle_exposes_graph_community_subgraph_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_community_subgraph(store.graph_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_community_subgraph(&NowledgeMemGraphCommunitySubgraphOptions::new(
                3505, 1, 5,
            ))
            .unwrap();

        assert_eq!(output.entities.len(), 1);
        assert_eq!(
            output.entities[0].entity_id.as_deref(),
            Some("community-subgraph-alpha")
        );
        assert!(output.edges.is_empty());
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE
        );

        let missing = handle
            .read_graph_community_subgraph(&NowledgeMemGraphCommunitySubgraphOptions::new(
                4242, 10, 10,
            ))
            .unwrap();
        assert!(missing.entities.is_empty());
        assert!(missing.edges.is_empty());
        assert!(missing.report.edge_read_report.is_none());
    }

    #[test]
    fn graph_augmentation_state_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_augmentation_state(&mut graph);

        let output = graph
            .read_graph_augmentation_state(&NowledgeMemGraphAugmentationStateOptions::default())
            .unwrap();
        let state = output.state.as_ref().expect("augmentation state row");

        assert_eq!(state.community_detection_applied, Some(true));
        assert_eq!(state.pagerank_applied, Some(true));
        assert_eq!(state.community_algorithm.as_deref(), Some("louvain"));
        assert_eq!(state.pagerank_algorithm.as_deref(), Some("pagerank"));
        assert_eq!(state.community_count, Some(Value::Int(12)));
        assert_eq!(state.pagerank_iterations, Some(Value::Int(20)));
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE
        );
        assert_eq!(output.report.row_count, 1);
        assert_eq!(output.report.read_report.row_count, 1);
        assert!(output.report.read_report.row_limit_enforced_before_output);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(output.json()["state"]["pagerank_applied"], true);
    }

    #[test]
    fn embedded_store_handle_exposes_graph_augmentation_state_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_augmentation_state(store.graph_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_augmentation_state(&NowledgeMemGraphAugmentationStateOptions::default())
            .unwrap();

        assert!(output.state.is_some());
        assert_eq!(
            output.report.route,
            NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE
        );
    }

    #[test]
    fn graph_pagerank_plan_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_pagerank_plan(graph.database_mut());
        let graph_commit_epoch = graph.database().commit_epoch();

        let output = graph
            .read_graph_pagerank_plan(&NowledgeMemGraphPageRankPlanOptions {
                changed_since_epoch_nanos: Some(100),
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
        assert_eq!(
            output
                .graph_meta
                .as_ref()
                .and_then(|meta| meta.pagerank_applied),
            Some(true)
        );
        assert_eq!(output.memory_node_count, 2);
        assert_eq!(output.entity_node_count, 2);
        assert_eq!(output.entity_relation_count, 1);
        assert_eq!(output.mention_edge_count, 2);
        assert_eq!(output.active_memory_relation_count, 1);
        assert_eq!(output.changed_memory_count, 1);
        assert_eq!(output.changed_entity_count, 1);
        assert_eq!(output.changed_mention_edge_count, 1);
        assert_eq!(output.changed_entity_relation_count, 1);
        assert_eq!(output.changed_memory_relation_count, 1);
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE);
        assert_eq!(output.report.query_count, 11);
        assert_eq!(output.report.read_reports.len(), 11);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(output.json()["changed_memory_count"], 1);
    }

    #[test]
    fn embedded_store_handle_exposes_graph_pagerank_plan_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_pagerank_plan(store.graph_mut().database_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_pagerank_plan(&NowledgeMemGraphPageRankPlanOptions::default())
            .unwrap();

        assert_eq!(output.memory_node_count, 2);
        assert_eq!(output.changed_memory_count, 0);
        assert_eq!(output.report.query_count, 6);
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE);
    }

    #[test]
    fn graph_orphans_route_runs_through_bounded_query_runtime() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        seed_graph_orphan_entities(&mut graph);

        let output = graph
            .read_graph_orphans(&NowledgeMemGraphOrphansOptions {
                limit: 10,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].entity_id.as_deref(), Some("orphan-entity"));
        assert_eq!(output.rows[0].label.as_deref(), Some("Orphan Entity"));
        assert_eq!(output.rows[0].entity_type.as_deref(), Some("concept"));
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE_REPORT_PROTOCOL
        );
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE);
        assert_eq!(output.report.row_count, 1);
        assert_eq!(output.report.read_report.row_count, 1);
        assert!(output.report.read_report.row_limit_enforced_before_output);
        assert_eq!(output.json()["report"]["read_engine"], "skein");
        assert_eq!(output.json()["rows"][0]["entity_id"], "orphan-entity");
    }

    #[test]
    fn embedded_store_handle_exposes_graph_orphans_route() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        seed_graph_orphan_entities(store.graph_mut());
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let output = handle
            .read_graph_orphans(&NowledgeMemGraphOrphansOptions {
                limit: 1,
                read_options: NowledgeMemReadOptions::default(),
            })
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].entity_id.as_deref(), Some("orphan-entity"));
        assert_eq!(output.report.route, NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE);
    }

    #[test]
    fn graph_query_with_report_marks_simple_lookup_fast_path() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-fast', title: 'Fast path'})")
            .unwrap();

        let query = graph
            .query_with_report("MATCH (m:Memory {id: 'mem-fast'}) RETURN m.title AS title")
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert_eq!(query.report.protocol, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL);
        assert_eq!(query.report.statement_kind, "match_return");
        assert_eq!(
            query.report.execution_path,
            NowledgeMemQueryExecutionPath::FastPath
        );
        assert_eq!(
            query.report.fast_path_reason.as_deref(),
            Some("simple_node_lookup")
        );
        assert_eq!(query.report.optimizer_decision_count, 0);
        assert!(!query.report.physical_plan_captured);
        assert!(query.report.physical_operator_counts.is_empty());
        assert!(!query.report.slow_log_candidate);
        assert_eq!(query.report.json()["execution_path"], "fast_path");
        assert_eq!(query.report.json()["statement_kind"], "match_return");
        assert_eq!(query.report.json()["fast_path_selected"], true);
        assert_eq!(query.report.json()["physical_plan_captured"], false);
        assert_eq!(query.report.json()["plan_cache"]["cacheable"], true);
        assert_eq!(query.report.output_row_shape.row_count, 1);
        assert_eq!(query.report.output_row_shape.column_count, 1);
        assert_eq!(query.report.output_row_shape.columns, vec!["title"]);
        assert_eq!(query.report.json()["output_row_shape"]["row_count"], 1);
        assert_eq!(
            query.report.json()["output_row_shape"]["columns"],
            serde_json::json!(["title"])
        );
    }

    #[test]
    fn graph_query_with_report_keeps_ordered_scan_on_optimized_path() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-slow-1', title: 'B'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-slow-2', title: 'A'})")
            .unwrap();

        let query = graph
            .query_with_report("MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1")
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert_eq!(
            query.report.execution_path,
            NowledgeMemQueryExecutionPath::OptimizedPath
        );
        assert_eq!(query.report.statement_kind, "match_return");
        assert_eq!(query.report.fast_path_reason, None);
        assert_eq!(query.report.optimizer_decision_count, 0);
        assert!(!query.report.physical_plan_captured);
        assert!(query.report.physical_operator_counts.is_empty());
        assert!(query.report.plan_cache_cacheable);
        assert!(query.report.plan_cache_miss);
        assert!(!query.report.plan_cache_hit);
        assert!(!query.report.plan_cache_bypassed);
        assert_eq!(query.report.json()["execution_path"], "optimized_path");
        assert_eq!(query.report.json()["fast_path_selected"], false);
        assert_eq!(query.report.json()["plan_cache"]["lookup"], "miss");
        assert_eq!(query.report.json()["plan_cache"]["cacheable"], true);
        assert_eq!(query.report.json()["plan_cache"]["miss"], true);
    }

    #[test]
    fn graph_query_with_report_can_capture_physical_plan_on_demand() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-plan-1', title: 'B'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-plan-2', title: 'A'})")
            .unwrap();

        let query = graph
            .query_with_params_with_report_options(
                "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
                &BTreeMap::new(),
                NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_log_threshold_micros: Some(0),
                },
            )
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert!(query.report.physical_plan_captured);
        assert_eq!(query.report.statement_kind, "match_return");
        assert!(query.report.optimizer_decision_count > 0);
        assert!(query
            .report
            .physical_operator_counts
            .contains_key("SortExec"));
        assert_eq!(query.report.plan_cache_lookup.as_deref(), Some("miss"));
        assert_eq!(query.report.plan_cache_bypass_reason, None);
        assert!(query.report.slow_log_candidate);
        assert_eq!(query.report.json()["physical_plan_captured"], true);
        assert_eq!(query.report.json()["plan_cache_lookup"], "miss");
        assert_eq!(query.report.json()["slow_log_candidate"], true);
        assert_eq!(
            query.report.json()["physical_operator_counts"]["SortExec"],
            1
        );
    }

    #[test]
    fn graph_query_with_report_captures_plan_without_extra_cache_lookup() {
        let db = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(8),
            ..DatabaseConfig::default()
        });
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-report-cache-1', title: 'B'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-report-cache-2', title: 'A'})")
            .unwrap();
        let before = graph.database().plan_cache_stats();

        let query = graph
            .query_with_params_with_report_options(
                "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
                &BTreeMap::new(),
                NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_log_threshold_micros: None,
                },
            )
            .unwrap();
        let after = graph.database().plan_cache_stats();

        assert_eq!(query.output.rows.len(), 1);
        assert!(query.report.physical_plan_captured);
        assert_eq!(query.report.plan_cache_lookup.as_deref(), Some("miss"));
        assert_eq!(query.report.plan_cache_bypass_reason, None);
        assert!(query.report.plan_cache_cacheable);
        assert!(query.report.plan_cache_miss);
        assert!(!query.report.plan_cache_bypassed);
        assert_eq!(after.entries, before.entries + 1);
        assert_eq!(after.misses, before.misses + 1);
        assert_eq!(after.hits, before.hits);
    }

    #[test]
    fn graph_query_with_report_exposes_storage_scan_pruning() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-prune-1', kind: 'note', title: 'Keep'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-prune-2', kind: 'note', title: 'Also keep'})")
            .unwrap();

        let query = graph
            .query_with_report("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.title AS title")
            .unwrap();

        assert_eq!(query.output.rows.len(), 2);
        assert_eq!(query.report.scan_pruning_reports.len(), 1);
        let scan = &query.report.scan_pruning_reports[0];
        assert!(scan.pruned);
        assert_eq!(scan.candidate_count_before_filter, 2);
        assert_eq!(scan.output_count, 2);
        assert_eq!(query.report.json()["scan_pruning_report_count"], 1);
        assert_eq!(
            query.report.json()["scan_pruning_reports"][0]["strategy"]["kind"],
            "property_eq"
        );
        assert_eq!(
            query.report.json()["scan_pruning_reports"][0]["strategy"]["property"],
            "kind"
        );
    }

    #[test]
    fn graph_query_with_report_keeps_system_statement_out_of_plan_cache() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

        let query = graph
            .query_with_report("SET system.work_priority = 'background'")
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert_eq!(query.report.statement_kind, "set_system_variable");
        assert_eq!(
            query.report.execution_path,
            NowledgeMemQueryExecutionPath::OptimizedPath
        );
        assert_eq!(query.report.plan_cache_lookup, None);
        assert_eq!(query.report.plan_cache_bypass_reason, None);
        assert!(!query.report.plan_cache_cacheable);
        assert!(!query.report.plan_cache_hit);
        assert!(!query.report.plan_cache_miss);
        assert!(!query.report.plan_cache_bypassed);
        assert_eq!(
            query.report.json()["plan_cache"]["lookup"],
            serde_json::Value::Null
        );
        assert_eq!(query.report.json()["plan_cache"]["cacheable"], false);
    }

    #[test]
    fn embedded_store_exposes_redacted_typed_slow_query_report() {
        let db = Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 4,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        store
            .query_with_report("CREATE (:Memory {id: 'slow-secret-id', title: 'Slow Secret'})")
            .unwrap();
        store
            .query_with_report("MATCH (m:Memory {id: 'slow-secret-id'}) RETURN m.title AS title")
            .unwrap();

        let report = store.slow_query_report();
        let json = report.json();
        let encoded = json.to_string();

        assert_eq!(report.protocol, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL);
        assert_eq!(report.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert!(report.present);
        assert!(report.ready);
        assert_eq!(report.capacity, 4);
        assert_eq!(report.threshold_micros, 0);
        assert_eq!(report.record_count, 2);
        assert_eq!(report.latest_sequence, Some(2));
        assert_eq!(report.records.len(), 2);
        assert!(report.records.iter().all(|record| record.success));
        assert!(report
            .records
            .iter()
            .all(|record| record.slow_log_candidate));
        assert_eq!(json["protocol"], NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL);
        assert_eq!(json["record_count"], 2);
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert!(json["records"][0].get("query_digest").is_some());
        assert!(!encoded.contains("Slow Secret"));
        assert!(!encoded.contains("slow-secret-id"));
        assert!(!encoded.contains("MATCH (m:Memory"));
    }

    #[test]
    fn embedded_store_handle_serializes_shared_library_state() {
        let db = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(8),
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 8,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report("CREATE (:Memory {id: 'shared-1', title: 'Shared State'})")
            .unwrap();
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let reader = {
            let handle = handle.clone();
            thread::spawn(move || {
                for _ in 0..4 {
                    let query = handle
                        .query_with_report(
                            "MATCH (m:Memory {id: 'shared-1'}) RETURN m.title AS title",
                        )
                        .unwrap();
                    assert_eq!(query.output.rows.len(), 1);
                    assert!(query.report.plan_cache_cacheable);
                    assert!(!query.report.plan_cache_bypassed);
                }
            })
        };
        let observer = {
            let handle = handle.clone();
            thread::spawn(move || {
                for _ in 0..4 {
                    let slow_query = handle.slow_query_report().unwrap();
                    assert!(slow_query.ready);
                    assert!(slow_query.record_count <= slow_query.capacity);

                    let dashboard = handle
                        .readiness_dashboard(&NowledgeMemReadinessOptions::default())
                        .unwrap();
                    assert!(readiness_dashboard_area(&dashboard, "slow_query").ready);
                    assert!(dashboard.slow_query_record_count <= slow_query.capacity);
                }
            })
        };

        reader.join().unwrap();
        observer.join().unwrap();

        let final_slow_query = handle.slow_query_report().unwrap();
        assert!(final_slow_query.ready);
        assert_eq!(final_slow_query.latest_sequence, Some(5));
        assert_eq!(final_slow_query.record_count, 5);
    }

    #[test]
    fn embedded_store_exposes_typed_query_runtime_preflight() {
        let db = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(8),
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report("CREATE (:Memory {id: 'preflight-1', title: 'Preflight'})")
            .unwrap();
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                NowledgeQueryRuntimePreflightProbe::new(
                    format!("probe:{route}"),
                    "MATCH (m:Memory {id: 'preflight-1'}) RETURN m.title AS title",
                )
                .with_route(*route)
                .with_query_family(super::nowledge_mem_required_query_families_for_route(route)[0])
                .require_scan_pruning(1)
                .require_pruned()
                .with_max_output_rows(1)
            })
            .collect::<Vec<_>>();

        let report = store.query_runtime_preflight(&probes);
        let json = report.json();

        assert_eq!(report.protocol, NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL);
        assert!(report.ready);
        assert!(report.database_opened);
        assert!(report.redaction.ready());
        assert!(!report.redaction.rows_copied);
        assert!(!report.redaction.parameters_copied);
        assert!(!report.redaction.local_paths_copied);
        assert!(!report.redaction.raw_errors_copied);
        assert_eq!(
            report.probe_count,
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(report.passed_probe_count, report.probe_count);
        assert_eq!(report.failed_probe_count, 0);
        assert!(report.required_routes_covered);
        assert!(report.route_coverage_ready);
        assert!(report.blocker_codes.is_empty());
        assert!(report.probes.iter().all(|probe| probe.ready));
        assert!(report
            .probes
            .iter()
            .all(|probe| probe.selected_plan_fingerprint.is_some()));
        assert!(report
            .probes
            .iter()
            .all(|probe| !probe.scan_pruning_reports.is_empty()));
        assert_eq!(json["protocol"], NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL);
        assert_eq!(json["ready"], true);
        assert_eq!(json["redaction"]["ready"], true);
        assert_eq!(json["redaction"]["rows_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["local_paths_copied"], false);
        assert_eq!(json["redaction"]["raw_errors_copied"], false);
        assert_eq!(json["probe_count"], probes.len());
        assert_eq!(json["probes"][0]["output_row_count"], 1);
        assert_eq!(
            json["probes"][0]["execution_profile"]["scan_pruning_report_count"],
            1
        );
        assert!(json["probes"][0]["selected_plan_fingerprint"]
            .as_str()
            .is_some_and(|fingerprint| fingerprint.contains("IndexNodeSeek")));
    }

    #[test]
    fn embedded_store_query_runtime_preflight_redacts_failed_probe_values() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        let probe = NowledgeQueryRuntimePreflightProbe::new(
            "probe:/graph/node-details/{node_id}",
            "MATCH (m:Memory {id: 'secret-preflight-id'}) RETURN missing(",
        )
        .with_route("/graph/node-details/{node_id}")
        .with_query_family("memory_lookup");

        let report = store.query_runtime_preflight(&[probe]);
        let json = report.json();
        let encoded = json.to_string();

        assert!(!report.ready);
        assert!(report.redaction.ready());
        assert_eq!(report.failed_probe_count, 1);
        assert_eq!(
            report.probes[0].blocker_codes,
            vec!["query_runtime_failed".to_string()]
        );
        assert_eq!(json["probes"][0]["success"], false);
        assert_eq!(json["probes"][0]["error_class"], "parse");
        assert_eq!(json["redaction"]["ready"], true);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["raw_errors_copied"], false);
        assert!(!encoded.contains("secret-preflight-id"));
        assert!(!encoded.contains("RETURN missing"));
    }

    #[test]
    fn graph_read_query_reports_bounded_payload() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read', title: 'Bounded read'})")
            .unwrap();

        let read = graph
            .read_query_with_options(
                "MATCH (m:Memory {id: 'mem-read'}) RETURN m.title AS title",
                &NowledgeMemReadOptions {
                    max_rows: Some(4),
                    max_estimated_payload_bytes: Some(128),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.protocol, NOWLEDGE_MEM_READ_REPORT_PROTOCOL);
        assert_eq!(read.report.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert_eq!(read.report.row_count, 1);
        assert_eq!(read.report.max_rows, Some(4));
        assert_eq!(read.report.execution_row_cap, Some(5));
        assert!(read.report.estimated_payload_bytes <= 128);
        assert!(!read.report.row_budget_exceeded);
        assert!(!read.report.payload_budget_exceeded);
        assert!(read.report.row_limit_enforced_before_output);
        assert!(read.report.operator_row_cap_enabled);
        assert_eq!(read.report.blocking_operator_count, 0);
        assert!(read.report.blocking_operator_kinds.is_empty());
        assert!(!read.report.streaming);
        assert_eq!(read.report.json()["execution_row_cap"], 5);
        assert_eq!(read.report.json()["row_limit_enforced_before_output"], true);
        assert_eq!(read.report.json()["operator_row_cap_enabled"], true);
        assert_eq!(read.report.json()["blocking_operator_count"], 0);
        assert_eq!(read.report.json()["streaming"], false);
        assert_eq!(
            read.report.bounded_read_evidence_json()["protocol"],
            NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
        );
        assert_eq!(
            read.report.bounded_read_evidence_json()["mode"],
            "shadow_read_only"
        );
        assert_eq!(read.report.bounded_read_evidence_json()["ready"], false);
        assert_eq!(
            read.report.bounded_read_evidence_json()["blocker_codes"],
            serde_json::json!(["missing_covered_routes", "graph_route_readiness_missing"])
        );
        let covered_routes = full_bounded_read_routes();
        let route_readiness = ready_route_readiness_summary();
        let evidence = nowledge_mem_bounded_read_evidence_json_with_route_readiness(
            &read.report,
            &covered_routes,
            Some(&route_readiness),
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["covered_routes"],
            serde_json::json!(covered_routes)
        );
        assert_eq!(evidence["missing_covered_routes"], serde_json::json!([]));
    }

    #[test]
    fn bounded_read_evidence_fails_closed_for_missing_row_cap() {
        let report = NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::ShadowReadOnly,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap: None,
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output: false,
            operator_row_cap_enabled: false,
            blocking_operator_count: 1,
            blocking_operator_kinds: vec!["Sort".to_string()],
            streaming: false,
        };

        let evidence = nowledge_mem_bounded_read_evidence_json(&report);

        assert_eq!(
            evidence["protocol"],
            NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
        );
        assert_eq!(evidence["present"], true);
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["max_rows"], 512);
        assert_eq!(evidence["execution_row_cap"], serde_json::Value::Null);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "missing_execution_row_cap",
                "row_limit_not_enforced_before_output",
                "operator_row_cap_disabled",
                "missing_covered_routes",
                "graph_route_readiness_missing"
            ])
        );
    }

    #[test]
    fn bounded_read_evidence_requires_shadow_read_only_mode() {
        let report = NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::WritableCutover,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap: Some(513),
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            blocking_operator_count: 0,
            blocking_operator_kinds: Vec::new(),
            streaming: false,
        };

        let evidence = nowledge_mem_bounded_read_evidence_json(&report);

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["mode"], "writable_cutover");
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "not_shadow_read_only",
                "missing_covered_routes",
                "graph_route_readiness_missing"
            ])
        );
    }

    #[test]
    fn search_candidate_shadow_evidence_reports_ready_counts() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1", "mem_2"]);
        accumulator.record_compare_candidate_ids(
            &["mem_3", "mem_4", "mem_5"],
            &["mem_3", "mem_4", "mem_5"],
        );
        accumulator.record_filter_pushdown_fields(
            2,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );
        let evidence = accumulator.json();

        assert_eq!(
            evidence["protocol"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL
        );
        assert_eq!(
            evidence["route"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE
        );
        assert_eq!(
            evidence["evidence_source"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["primary_candidate_count"], 5);
        assert_eq!(evidence["shadow_candidate_count"], 5);
        assert_eq!(evidence["matched_candidate_count"], 5);
        assert_eq!(evidence["primary_only_candidate_count"], 0);
        assert_eq!(evidence["row_count_parity"], true);
        assert_eq!(evidence["text_retriever_ready"], false);
        assert_eq!(evidence["vector_retriever_ready"], false);
        assert_eq!(evidence["fts_top_k_overlap_ready"], false);
        assert_eq!(evidence["vector_top_k_overlap_ready"], false);
        assert_eq!(
            evidence["candidate_readiness"]["source_chunk_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_readiness"]["fail_soft_observed"], false);
        assert_eq!(
            evidence["candidate_readiness"]["projection_marker_status_visible"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["projection_watermark_ready"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["embedding_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["candidate_identity"]["parity"], true);
        assert_eq!(evidence["shadow_scan_present"], true);
        assert_eq!(evidence["shadow_scan_filter_pushdown_ready"], true);
        assert_eq!(evidence["shadow_scan_field_pruning_ready"], true);
        assert_eq!(
            evidence["shadow_scan_field_summary_count"],
            serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len())
        );
        assert_eq!(evidence["filter_pushdown_ready"], true);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
        assert_eq!(evidence["filter_pushdown"]["pushed_predicate_count"], 2);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_value_summary_fields"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_numeric_range_fields"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_timestamp_range_fields"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["filter_pushdown"]["field_summary_count"],
            serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len())
        );
        assert!(evidence["filter_pushdown"]["field_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary["field"] == "importance"
                && summary["source"] == "persisted_segment_descriptor_contract"
                && summary["numeric_range_summary_used"] == true));
        assert!(evidence["candidate_identity"]
            .get("candidate_ids")
            .is_none());
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_candidate_shadow_evidence_fails_closed_on_weak_counts() {
        let evidence = nowledge_mem_search_candidate_shadow_evidence_json(
            &NowledgeMemSearchCandidateShadowEvidence {
                request_count: 0,
                primary_candidate_count: 3,
                shadow_candidate_count: 2,
                matched_candidate_count: 1,
                primary_only_candidate_count: 1,
                text_retriever_available: false,
                vector_retriever_available: false,
                text_retriever_candidate_count: 0,
                vector_retriever_candidate_count: 0,
                fts_top_k_overlap_observed: false,
                fts_top_k_overlap_ready: false,
                vector_top_k_overlap_observed: false,
                vector_top_k_overlap_ready: false,
                source_chunk_identity_ready: false,
                fail_soft_observed: false,
                projection_marker_status_visible: false,
                projection_watermark_ready: false,
                embedding_identity_ready: false,
                primary_candidate_identity_checksum: None,
                shadow_candidate_identity_checksum: None,
                matched_candidate_identity_checksum: None,
                filter_pushdown: None,
                blocker_codes: vec!["bridge_timeout".to_string()],
            },
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "bridge_timeout",
                "search_candidate_filter_pushdown_missing",
                "search_candidate_identity_missing",
                "search_candidate_mismatch",
                "search_candidate_primary_only",
                "search_candidate_shadow_no_requests"
            ])
        );
    }

    #[test]
    fn search_candidate_shadow_accumulator_generates_bridge_evidence() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1", "mem_2"]);
        accumulator.record_compare_candidate_ids(&["mem_3"], &["mem_3"]);
        accumulator.record_filter_pushdown_fields(
            1,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );

        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["primary_candidate_count"], 3);
        assert_eq!(evidence["shadow_candidate_count"], 3);
        assert_eq!(evidence["matched_candidate_count"], 3);
        assert_eq!(evidence["primary_only_candidate_count"], 0);
        assert_eq!(evidence["row_count_parity"], true);
        assert_eq!(evidence["text_retriever_ready"], false);
        assert_eq!(evidence["vector_retriever_ready"], false);
        assert_eq!(evidence["fts_top_k_overlap_ready"], false);
        assert_eq!(evidence["vector_top_k_overlap_ready"], false);
        assert_eq!(
            evidence["candidate_readiness"]["source_chunk_identity_ready"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["projection_watermark_ready"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["embedding_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["shadow_scan_filter_pushdown_ready"], true);
        assert_eq!(evidence["shadow_scan_field_pruning_ready"], true);
        assert_eq!(evidence["filter_pushdown_ready"], true);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_candidate_shadow_accumulator_preserves_request_blockers() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1"]);
        accumulator.record_filter_pushdown_fields(
            1,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );
        accumulator.add_blocker_code("bridge_error");

        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["request_count"], 1);
        assert_eq!(evidence["primary_candidate_count"], 2);
        assert_eq!(evidence["shadow_candidate_count"], 1);
        assert_eq!(evidence["matched_candidate_count"], 1);
        assert_eq!(evidence["primary_only_candidate_count"], 1);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "bridge_error",
                "search_candidate_identity_mismatch",
                "search_candidate_mismatch",
                "search_candidate_primary_only"
            ])
        );
    }

    #[test]
    fn search_candidate_shadow_evidence_requires_filter_pushdown_fields() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1"], &["mem_1"]);

        let missing = accumulator.json();

        assert_eq!(missing["ready"], false);
        assert_eq!(missing["filter_pushdown_ready"], false);
        assert!(missing["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_filter_pushdown_missing"));

        accumulator.record_filter_pushdown_fields(1, ["unit_type"]);
        let partial = accumulator.json();

        assert_eq!(partial["ready"], false);
        assert!(!partial["filter_pushdown"]["missing_required_fields"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(partial["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_field_pruning_missing"));
    }

    #[test]
    fn graph_read_query_rejects_payload_budget_excess() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-large', title: 'Large read payload'})")
            .unwrap();

        let error = graph
            .read_query_with_options(
                "MATCH (m:Memory {id: 'mem-large'}) RETURN m.title AS title",
                &NowledgeMemReadOptions {
                    max_rows: Some(4),
                    max_estimated_payload_bytes: Some(4),
                },
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("exceeding max_estimated_payload_bytes 4"));
    }

    #[test]
    fn read_transaction_rejects_rows_above_configured_limit() {
        let mut db = Database::new_with_config(DatabaseConfig {
            max_read_result_rows: Some(1),
            ..DatabaseConfig::default()
        });
        db.query("CREATE (:Memory {id: 'mem-limit-1', title: 'Limit one'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-limit-2', title: 'Limit two'})")
            .unwrap();

        let error = db
            .begin_read_transaction()
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap_err();

        assert!(error.to_string().contains("more than 1 rows"));
    }

    #[test]
    fn graph_read_query_rejects_rows_before_returning_oversized_output() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-1', title: 'Limit one'})")
            .unwrap();
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-2', title: 'Limit two'})")
            .unwrap();

        let error = graph
            .read_query_with_options(
                "MATCH (m:Memory) RETURN m.id AS id",
                &NowledgeMemReadOptions {
                    max_rows: Some(1),
                    max_estimated_payload_bytes: Some(4096),
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("more than 1 rows"));
    }

    #[test]
    fn graph_read_query_allows_cypher_limit_within_row_budget() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-pass-1', title: 'Limit one'})")
            .unwrap();
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-pass-2', title: 'Limit two'})")
            .unwrap();

        let read = graph
            .read_query_with_options(
                "MATCH (m:Memory) RETURN m.id AS id LIMIT 1",
                &NowledgeMemReadOptions {
                    max_rows: Some(1),
                    max_estimated_payload_bytes: Some(4096),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.row_count, 1);
        assert!(!read.report.row_budget_exceeded);
    }

    #[test]
    fn graph_read_query_reports_blocking_operators() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-sort-profile-1', title: 'B'})")
            .unwrap();
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-sort-profile-2', title: 'A'})")
            .unwrap();

        let read = graph
            .read_query_with_options(
                "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
                &NowledgeMemReadOptions {
                    max_rows: Some(4),
                    max_estimated_payload_bytes: Some(4096),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.blocking_operator_kinds, vec!["SortExec"]);
        assert_eq!(read.report.blocking_operator_count, 1);
        assert_eq!(read.report.json()["blocking_operator_kinds"][0], "SortExec");
    }

    #[test]
    fn embedded_store_read_query_does_not_require_search_projection() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-store-read', title: 'Store read'})")
            .unwrap();
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let read = store
            .read_query_with_options(
                "MATCH (m:Memory {id: 'mem-store-read'}) RETURN m.title AS title",
                &NowledgeMemReadOptions::default(),
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.row_count, 1);
        assert!(!read.report.row_budget_exceeded);
        assert!(!read.report.payload_budget_exceeded);
    }

    #[test]
    fn embedded_store_library_readiness_fails_closed_without_required_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions::default());

        assert_eq!(
            readiness["protocol"],
            NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL
        );
        assert_eq!(readiness["present"], true);
        assert_eq!(readiness["ready"], false);
        assert_eq!(readiness["mode"], "shadow_read_only");
        assert_eq!(readiness["production_path"]["ready"], true);
        assert_eq!(readiness["production_path"]["in_process"], true);
        assert_eq!(readiness["production_path"]["cli_required"], false);
        assert_eq!(
            readiness["production_path"]["env_control_plane_required"],
            false
        );
        assert_eq!(
            readiness["production_path"]["spawned_helper_required"],
            false
        );
        assert_eq!(readiness["bounded_read_evidence"]["present"], false);
        assert_eq!(
            readiness["bounded_read_evidence"]["blocker_codes"],
            serde_json::json!(["bounded_read_probe_missing"])
        );
        assert_eq!(
            readiness["search_projection_evidence"]["blocker_codes"],
            serde_json::json!(["search_projection_not_configured"])
        );
        assert_eq!(
            readiness["search_projection_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["primary_search_projection_probe_missing"])
        );
        assert_eq!(
            readiness["search_candidate_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["search_candidate_shadow_evidence_missing"])
        );
        assert_eq!(
            readiness["query_family_evidence"]["blocker_codes"],
            serde_json::json!(["query_family_evidence_missing"])
        );
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "bounded_read_evidence_not_ready"));
        assert_eq!(readiness["readiness_by_area"]["graph"]["ready"], true);
        assert_eq!(readiness["readiness_by_area"]["query"]["ready"], false);
        assert_eq!(
            readiness["readiness_by_area"]["query"]["blocker_codes"],
            serde_json::json!(["bounded_read_probe_missing"])
        );
        assert_eq!(readiness["readiness_by_area"]["storage"]["ready"], false);
        assert_eq!(readiness["readiness_by_area"]["background"]["ready"], false);
        assert_eq!(
            readiness["readiness_by_area"]["background"]["blocker_codes"],
            serde_json::json!(["no_candidates", "no_ranked_work"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["query_family"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["query_family"]["blocker_codes"],
            serde_json::json!(["query_family_evidence_missing"])
        );
        assert_eq!(
            readiness["graph_route_readiness"]["blocker_codes"],
            serde_json::json!(["graph_route_readiness_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["graph_route"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["graph_route"]["blocker_codes"],
            serde_json::json!(["graph_route_readiness_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_projection"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_projection_shadow"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_candidate_shadow"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_candidate_shadow"]["blocker_codes"],
            serde_json::json!(["search_candidate_shadow_evidence_missing"])
        );
        assert_eq!(
            readiness["workload_fixture_evidence"]["blocker_codes"],
            serde_json::json!(["workload_fixture_evidence_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["workload_fixture"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["workload_fixture"]["blocker_codes"],
            serde_json::json!(["workload_fixture_evidence_missing"])
        );
        assert_eq!(readiness["ready_area_count"], 1);
        assert_eq!(readiness["blocked_area_count"], 9);
        assert!(!readiness.to_string().contains("redacted"));
    }

    #[test]
    fn embedded_store_exposes_typed_library_readiness_report() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.library_readiness(&NowledgeMemReadinessOptions::default());
        let json = report.json();

        assert_eq!(report.protocol, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL);
        assert!(report.present);
        assert!(!report.ready);
        assert_eq!(report.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert!(report.redaction.ready());
        assert!(!report.redaction.query_text_copied);
        assert!(!report.redaction.parameters_copied);
        assert!(!report.redaction.local_paths_copied);
        assert!(report.graph_open);
        assert!(!report.graph_read_only);
        let areas = report.areas();
        assert_eq!(areas.len(), 10);
        assert_eq!(report.ready_area_count, 1);
        assert_eq!(report.blocked_area_count, 9);
        assert!(report.readiness_by_area.graph.ready);
        assert!(!report.readiness_by_area.query.ready);
        assert_eq!(
            report.readiness_by_area.query.blocker_codes,
            vec!["bounded_read_probe_missing".to_string()]
        );
        assert!(!report.readiness_by_area.graph_route.ready);
        assert_eq!(
            report.readiness_by_area.graph_route.blocker_codes,
            vec!["graph_route_readiness_missing".to_string()]
        );
        assert!(!report.readiness_by_area.workload_fixture.ready);
        assert_eq!(
            report.readiness_by_area.workload_fixture.blocker_codes,
            vec!["workload_fixture_evidence_missing".to_string()]
        );
        let graph_area = areas
            .iter()
            .find(|area| area.name == "graph")
            .expect("graph readiness area");
        let query_area = areas
            .iter()
            .find(|area| area.name == "query")
            .expect("query readiness area");
        assert!(graph_area.ready);
        assert!(!query_area.ready);
        assert_eq!(
            query_area.blocker_codes,
            vec!["bounded_read_probe_missing".to_string()]
        );
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "bounded_read_evidence_not_ready"));
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "graph_route_readiness_not_ready"));
        assert_eq!(json["ready"], false);
        assert_eq!(json["redaction"]["ready"], true);
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["local_paths_copied"], false);
        assert_eq!(
            json["blocker_codes"],
            serde_json::json!(report.blocker_codes)
        );
        assert_eq!(json["areas"].as_array().unwrap().len(), areas.len());
        assert_eq!(
            json["areas"][0],
            serde_json::json!({
                "name": "graph",
                "ready": true,
                "blocker_codes": [],
            })
        );
        assert_eq!(json["graph"]["read_only"], false);
        assert_eq!(
            json["bounded_read_evidence"]["blocker_codes"],
            serde_json::json!(["bounded_read_probe_missing"])
        );
    }

    #[test]
    fn embedded_store_library_readiness_accepts_typed_workload_fixture_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        let workload_fixture = nowledge_graph_route_workload_fixture_report(
            NowledgeGraphRouteWorkloadFixtureOptions {
                capture_physical_plan: true,
                include_bounded_expansion_probes: true,
                ..NowledgeGraphRouteWorkloadFixtureOptions::default()
            },
        )
        .unwrap();

        let report = store.library_readiness(&NowledgeMemReadinessOptions {
            workload_fixture_evidence: Some(workload_fixture.clone()),
            ..NowledgeMemReadinessOptions::default()
        });
        let json = report.json();

        assert!(workload_fixture.ready);
        assert!(report.readiness_by_area.workload_fixture.ready);
        assert_eq!(
            report.readiness_by_area.workload_fixture.blocker_codes,
            Vec::<String>::new()
        );
        assert!(report
            .blocker_codes
            .iter()
            .all(|code| code != "workload_fixture_evidence_not_ready"));
        assert_eq!(json["workload_fixture_evidence"]["ready"], true);
        assert_eq!(json["readiness_by_area"]["workload_fixture"]["ready"], true);
        assert_eq!(
            json["workload_fixture_evidence"]["failed_query_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["failed_bounded_expansion_probe_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["failed_search_metadata_probe_count"],
            serde_json::json!(0)
        );
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_search_candidate_shadow_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            search_candidate_shadow_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(
            readiness["readiness_by_area"]["search_candidate_shadow"]["ready"],
            false
        );
        let blocker_codes = readiness["readiness_by_area"]["search_candidate_shadow"]
            ["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_candidate_shadow_protocol_mismatch"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_candidate_counts_not_ready"));
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_shadow_evidence_not_ready"));
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_search_projection_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            search_projection_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(
            readiness["readiness_by_area"]["search_projection"]["ready"],
            false
        );
        let blocker_codes = readiness["readiness_by_area"]["search_projection"]["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_protocol_mismatch"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_tables_not_ready"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_document_identity_not_ready"));
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_projection_evidence_not_ready"));
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_search_projection_shadow_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            search_projection_shadow_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(
            readiness["readiness_by_area"]["search_projection_shadow"]["ready"],
            false
        );
        let blocker_codes = readiness["readiness_by_area"]["search_projection_shadow"]
            ["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_shadow_protocol_mismatch"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_shadow_document_identity_not_ready"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY));
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_projection_shadow_evidence_not_ready"));
    }

    #[test]
    fn embedded_store_exposes_compact_readiness_dashboard() {
        let db = Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 4,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report(
                "CREATE (:Memory {id: 'dashboard-secret', title: 'Dashboard Secret'})",
            )
            .unwrap();

        let dashboard = store.readiness_dashboard(&NowledgeMemReadinessOptions::default());
        let json = dashboard.json();
        let encoded = json.to_string();

        assert_eq!(
            dashboard.protocol,
            NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL
        );
        assert_eq!(dashboard.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert!(!dashboard.ready);
        assert_eq!(dashboard.area_count, 11);
        assert_eq!(dashboard.ready_area_count, 3);
        assert_eq!(dashboard.blocked_area_count, 8);
        assert!(dashboard.slow_query_ready);
        assert_eq!(dashboard.slow_query_record_count, 1);
        assert!(readiness_dashboard_area(&dashboard, "graph").ready);
        assert!(readiness_dashboard_area(&dashboard, "background").ready);
        assert_eq!(
            readiness_dashboard_area(&dashboard, "query").blocker_codes,
            vec!["bounded_read_probe_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "query_family").blocker_codes,
            vec!["query_family_evidence_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "graph_route").blocker_codes,
            vec!["graph_route_readiness_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "storage").blocker_codes,
            vec![
                "durable_recovery_not_observed".to_string(),
                "checkpoint_boundary_missing".to_string(),
                "wal_replay_unbounded".to_string(),
                "replay_boundary_inconsistent".to_string()
            ]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "search_projection").blocker_codes,
            vec!["search_projection_not_configured".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "search_projection_shadow").blocker_codes,
            vec!["primary_search_projection_probe_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "search_candidate_shadow").blocker_codes,
            vec!["search_candidate_shadow_evidence_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "workload_fixture").blocker_codes,
            vec!["workload_fixture_evidence_missing".to_string()]
        );
        assert!(readiness_dashboard_area(&dashboard, "slow_query").ready);
        assert_eq!(json["protocol"], NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL);
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["local_paths_copied"], false);
        assert!(!encoded.contains("Dashboard Secret"));
        assert!(!encoded.contains("dashboard-secret"));
    }

    #[test]
    fn storage_recovery_report_exposes_typed_readiness_summary() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(16),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: Some(11),
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(7),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: None,
                recovered_commit_epoch: 14,
            });
        let json = report.json();

        assert_eq!(report.protocol, "skein-storage-recovery-report");
        assert!(report.present);
        assert!(report.ready);
        assert!(report.durable_recovery_observed);
        assert!(report.checkpoint_boundary_present);
        assert!(report.wal_replay_bounded);
        assert!(report.replay_boundary_consistent);
        assert!(report.torn_tail_clean);
        assert!(report.blocker_codes.is_empty());
        assert_eq!(json["ready"], true);
        assert_eq!(json["readiness"]["wal_replay_bounded"], true);
        assert_eq!(json["readiness"]["replay_boundary_consistent"], true);
        assert_eq!(json["max_wal_replay_entries"], 16);
    }

    #[test]
    fn storage_recovery_report_recomputes_typed_readiness_from_raw_fields() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(2),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: None,
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(8),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: Some("partial wal entry".to_string()),
                recovered_commit_epoch: 13,
            });
        let json = report.json();

        assert!(report.durable_recovery_observed);
        assert!(!report.ready);
        assert!(!report.checkpoint_boundary_present);
        assert!(!report.wal_replay_bounded);
        assert!(!report.replay_boundary_consistent);
        assert!(!report.torn_tail_clean);
        assert_eq!(
            report.blocker_codes,
            vec![
                "checkpoint_boundary_missing".to_string(),
                "wal_replay_unbounded".to_string(),
                "replay_boundary_inconsistent".to_string(),
                "torn_tail_observed".to_string()
            ]
        );
        assert_eq!(json["readiness"]["checkpoint_boundary_present"], false);
        assert_eq!(json["readiness"]["wal_replay_bounded"], false);
        assert_eq!(json["readiness"]["replay_boundary_consistent"], false);
        assert_eq!(json["readiness"]["torn_tail_clean"], false);
    }

    #[test]
    fn storage_recovery_report_rejects_inconsistent_replay_boundary() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(16),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: Some(11),
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(7),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: None,
                recovered_commit_epoch: 13,
            });
        let json = report.json();

        assert!(report.durable_recovery_observed);
        assert!(report.checkpoint_boundary_present);
        assert!(report.wal_replay_bounded);
        assert!(!report.replay_boundary_consistent);
        assert!(report.torn_tail_clean);
        assert!(!report.ready);
        assert_eq!(
            report.blocker_codes,
            vec!["replay_boundary_inconsistent".to_string()]
        );
        assert_eq!(json["readiness"]["replay_boundary_consistent"], false);
    }

    #[test]
    fn embedded_store_exposes_storage_recovery_report_through_library_api() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.storage_recovery_report();
        let json = store.storage_recovery_report_json();

        assert_eq!(report.protocol, "skein-storage-recovery-report");
        assert!(report.present);
        assert!(!report.ready);
        assert_eq!(
            report.blocker_codes,
            vec![
                "durable_recovery_not_observed".to_string(),
                "checkpoint_boundary_missing".to_string(),
                "wal_replay_unbounded".to_string(),
                "replay_boundary_inconsistent".to_string()
            ]
        );
        assert_eq!(json["ready"], false);
        assert_eq!(
            json["blocker_codes"],
            serde_json::json!(report.blocker_codes)
        );
    }

    #[test]
    fn embedded_store_library_readiness_runs_bounded_probe() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-readiness', title: 'Readiness'})")
            .unwrap();
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            bounded_read_probe: Some(NowledgeGraphStatement {
                cypher: "MATCH (m:Memory {id: 'mem-readiness'}) RETURN m.title AS title"
                    .to_string(),
                parameters: BTreeMap::new(),
            }),
            covered_routes: full_bounded_read_routes(),
            graph_route_readiness: Some(ready_route_readiness_summary()),
            replacement_readiness_by_query_family: Some(ready_query_family_replacement()),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(readiness["bounded_read_evidence"]["present"], true);
        assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
        assert_eq!(
            readiness["bounded_read_evidence"]["mode"],
            "shadow_read_only"
        );
        assert_eq!(readiness["bounded_read_evidence"]["execution_row_cap"], 513);
        assert!(
            readiness["bounded_read_evidence"]["estimated_payload_bytes"]
                .as_u64()
                .is_some()
        );
        assert_eq!(
            readiness["bounded_read_evidence"]["max_estimated_payload_bytes"],
            serde_json::json!(4 * 1024 * 1024)
        );
        assert_eq!(
            readiness["bounded_read_evidence"]["payload_budget_exceeded"],
            false
        );
        assert_eq!(
            readiness["bounded_read_evidence"]["missing_covered_routes"],
            serde_json::json!([])
        );
        assert_eq!(
            readiness["background_maintenance"]["total_candidates"]
                .as_u64()
                .unwrap_or_default(),
            readiness["background_maintenance"]["ranked"]
                .as_array()
                .unwrap()
                .len() as u64
        );
        assert_eq!(readiness["readiness_by_area"]["query"]["ready"], true);
        assert_eq!(readiness["readiness_by_area"]["background"]["ready"], true);
        assert_eq!(
            readiness["readiness_by_area"]["background"]["blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(
            readiness["readiness_by_area"]["query_family"]["ready"],
            true
        );
        assert_eq!(readiness["readiness_by_area"]["graph_route"]["ready"], true);
        assert_eq!(
            readiness["graph_route_readiness"]["primary_ready_route_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            readiness["query_family_evidence"]["missing_required_query_families"],
            serde_json::json!([])
        );
        assert_eq!(
            readiness["query_family_evidence"]["min_replacement_readiness_per_million"],
            1_000_000
        );
    }

    #[test]
    fn embedded_store_library_readiness_feeds_final_preflight_without_cli() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-preflight', title: 'Preflight'})")
            .unwrap();
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let library = store.library_readiness(&NowledgeMemReadinessOptions {
            bounded_read_probe: Some(NowledgeGraphStatement {
                cypher: "MATCH (m:Memory {id: 'mem-preflight'}) RETURN m.title AS title"
                    .to_string(),
                parameters: BTreeMap::new(),
            }),
            covered_routes: full_bounded_read_routes(),
            graph_route_readiness: Some(ready_route_readiness_summary()),
            replacement_readiness_by_query_family: Some(ready_query_family_replacement()),
            ..NowledgeMemReadinessOptions::default()
        });
        let mut library_json = library.json();
        library_json["open_report"] = serde_json::json!({
            "protocol": NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
            "mode": "shadow_read_only",
            "graph_configured": true,
            "search_projection_configured": false,
            "compressed_vector_search_mode": "disabled",
            "graph_opened": true,
            "search_projection_opened": false,
        });

        let bundle = serde_json::json!({
            "library_readiness": library_json,
        });
        let preflight = nowledge_mem_final_cutover_preflight(&bundle);

        assert!(library.readiness_by_area.query.ready);
        assert!(library.readiness_by_area.graph_route.ready);
        assert!(library.readiness_by_area.query_family.ready);
        assert!(library.readiness_by_area.background.ready);
        assert!(!library.readiness_by_area.storage.ready);
        assert!(!library.readiness_by_area.search_projection.ready);
        assert!(!preflight.production_cutover_ready);
        assert!(!preflight.library_only_ready);
        assert!(preflight
            .failed_checks
            .iter()
            .any(|check| check == "library_readiness"));
        assert!(preflight
            .failed_evidence_fields
            .iter()
            .any(|field| field == "library_readiness.open_report.search_projection_opened"));
        assert!(preflight
            .next_action_names
            .iter()
            .any(|action| action == "attach_library_readiness_evidence"));
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_bounded_read_payload_budget() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            bounded_read_evidence: Some(serde_json::json!({
                "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
                "present": true,
                "ready": true,
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "streaming": false,
                "blocking_operator_count": 0,
                "row_budget_exceeded": false,
                "payload_budget_exceeded": false,
                "missing_covered_routes": [],
                "route_primary_ready": true,
                "route_query_plan_evidence_ready": true,
                "route_query_profile_evidence_ready": true,
                "relationship_property_pruning_required_count": 0,
                "relationship_property_pruning_report_count": 0,
                "route_relationship_property_pruning_evidence_ready": true,
                "blocker_codes": [],
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
        assert_eq!(readiness["readiness_by_area"]["query"]["ready"], false);
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "bounded_read_evidence_not_ready"));
        let query_blockers = readiness["readiness_by_area"]["query"]["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(query_blockers
            .iter()
            .any(|code| code == "bounded_read_estimated_payload_bytes_missing"));
        assert!(query_blockers
            .iter()
            .any(|code| code == "bounded_read_max_estimated_payload_bytes_missing"));
    }

    #[test]
    fn embedded_store_exposes_search_projection_probe() {
        let index = SearchIndex::default();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let probe = store
            .search_projection()
            .unwrap()
            .probe_json(SearchProjectionProbeOptions::default());

        assert_eq!(probe["protocol"], "skein-nowledge-search-projection-probe");
    }

    #[test]
    fn embedded_store_exposes_search_projection_replacement_evidence() {
        let mut index = SearchIndex::in_memory();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 8,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: nowledge_projection_evidence_rows(),
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(17),
            })
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let evidence = store
            .search_projection_evidence_json(SearchProjectionProbeOptions {
                active_embedding_model: Some("bge-m3".to_string()),
                active_embedding_dimension: Some(8),
            })
            .unwrap();

        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-search-projection-evidence"
        );
        #[cfg(feature = "turbovec")]
        assert_eq!(evidence["ready"], true);
        #[cfg(not(feature = "turbovec"))]
        {
            assert_eq!(evidence["ready"], false);
            assert_eq!(
                evidence["compressed_vector_projection_ready"],
                serde_json::json!(false)
            );
            assert!(evidence["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == "compressed_vector_projection_not_ready"));
        }
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["required_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        assert_eq!(evidence["incremental_update_ready"], true);
        #[cfg(feature = "turbovec")]
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn embedded_store_exposes_typed_search_projection_replacement_evidence() {
        let mut index = SearchIndex::in_memory();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 8,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: nowledge_projection_evidence_rows(),
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(17),
            })
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let report = store
            .search_projection_evidence_report(SearchProjectionProbeOptions {
                active_embedding_model: Some("bge-m3".to_string()),
                active_embedding_dimension: Some(8),
            })
            .unwrap();

        assert_eq!(report.protocol, "skein-nowledge-search-projection-evidence");
        #[cfg(feature = "turbovec")]
        assert!(report.ready);
        #[cfg(not(feature = "turbovec"))]
        {
            assert!(!report.ready);
            assert!(!report.compressed_vector_projection_ready);
            assert!(report
                .blocker_codes
                .iter()
                .any(|code| code == "compressed_vector_projection_not_ready"));
        }
        assert!(report.derived_projection);
        assert!(report.all_tables_covered);
        assert_eq!(report.covered_table_count, 6);
        assert_eq!(report.required_table_count, 6);
        assert!(report.source_chunk_ready);
        assert!(report.incremental_update_ready);
        assert_eq!(report.json()["covered_table_count"], 6);
    }

    #[test]
    fn embedded_store_exposes_search_projection_shadow_evidence() {
        let mut index = SearchIndex::in_memory();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 8,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: nowledge_projection_evidence_rows(),
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(17),
            })
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let probe_options = SearchProjectionProbeOptions {
            active_embedding_model: Some("bge-m3".to_string()),
            active_embedding_dimension: Some(8),
        };
        let primary_probe = store
            .search_projection_probe_json(probe_options.clone())
            .unwrap();

        let evidence = store
            .search_projection_shadow_evidence_json(&primary_probe, probe_options)
            .unwrap();

        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-search-projection-shadow-evidence"
        );
        #[cfg(feature = "turbovec")]
        {
            assert_eq!(evidence["ready"], true);
            assert_eq!(evidence["primary_ready"], true);
            assert_eq!(evidence["shadow_ready"], true);
        }
        #[cfg(not(feature = "turbovec"))]
        {
            assert_eq!(evidence["ready"], false);
            assert_eq!(evidence["primary_ready"], false);
            assert_eq!(evidence["shadow_ready"], false);
            assert!(!evidence["blocker_codes"].as_array().unwrap().is_empty());
        }
        assert_eq!(evidence["document_count_parity"], true);
        assert_eq!(evidence["table_parity"]["ready"], true);
        assert_eq!(evidence["embedding_identity_parity"], true);
        assert_eq!(evidence["incremental_watermark_parity"], true);
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_document_pruning_ready"],
            true
        );
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_pruning_candidate_document_count"],
            6
        );
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_pruned_document_count"],
            4
        );
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_scanned_document_count"],
            2
        );
        #[cfg(feature = "turbovec")]
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn embedded_store_search_projection_evidence_requires_projection() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let error = store
            .search_projection_evidence_json(SearchProjectionProbeOptions::default())
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "storage error: nowledge mem search projection is not configured"
        );
    }

    #[test]
    fn embedded_store_search_projection_shadow_evidence_requires_projection() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let error = store
            .search_projection_shadow_evidence_json(
                &serde_json::json!({ "engine": "lancedb" }),
                SearchProjectionProbeOptions::default(),
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "storage error: nowledge mem search projection is not configured"
        );
    }

    #[test]
    fn open_options_report_is_sanitized() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        );

        let report = options.sanitized_report().json();

        assert_eq!(report["protocol"], NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL);
        assert_eq!(report["mode"], "shadow_read_only");
        assert_eq!(report["graph_configured"], true);
        assert_eq!(report["search_projection_configured"], true);
        assert_eq!(report["compressed_vector_search_mode"], "disabled");
        assert!(report.get("graph_path").is_none());
        assert!(report.get("search_projection_path").is_none());
        assert!(!report.to_string().contains("redacted_graph_path"));
        assert!(!report.to_string().contains("redacted_search_path"));
    }

    #[test]
    fn open_options_diagnostics_include_local_paths_only_with_debug_flag() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "debug_graph_path",
            "debug_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        );

        let redacted = options.diagnostic_report_json(NowledgeMemOpenDiagnosticOptions::default());
        assert_eq!(redacted["debug_local_paths_included"], false);
        assert_eq!(redacted["local_paths_redacted"], true);
        assert!(redacted.get("graph_path").is_none());
        assert!(redacted.get("search_projection_path").is_none());
        assert!(!redacted.to_string().contains("debug_graph_path"));
        assert!(!redacted.to_string().contains("debug_search_path"));

        let debug = options.diagnostic_report_json(NowledgeMemOpenDiagnosticOptions {
            include_local_paths: true,
        });
        assert_eq!(debug["debug_local_paths_included"], true);
        assert_eq!(debug["local_paths_redacted"], false);
        assert_eq!(debug["graph_path"], "debug_graph_path");
        assert_eq!(debug["search_projection_path"], "debug_search_path");
    }

    #[test]
    fn open_options_report_gates_advanced_compressed_vector_search_mode() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

        let report = options.sanitized_report().json();

        assert_eq!(report["compressed_vector_search_mode"], "disabled");
        assert_eq!(
            report["requested_compressed_vector_search_mode"],
            "preferred"
        );
        assert_eq!(report["retrieval_projection_advisor"]["ready"], false);
        assert_eq!(
            report["retrieval_projection_advisor_blocker_codes"],
            serde_json::json!([
                "retrieval_projection_recall_evidence_missing",
                "retrieval_projection_parity_evidence_missing",
                "retrieval_projection_segment_not_advised"
            ])
        );
        assert!(!report.to_string().contains("redacted_graph_path"));
        assert!(!report.to_string().contains("redacted_search_path"));
    }

    #[test]
    fn open_options_report_allows_compressed_vector_search_with_advisor_evidence() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred)
        .with_retrieval_projection_advisor(
            NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(),
        );

        let report = options.sanitized_report().json();

        assert_eq!(report["compressed_vector_search_mode"], "preferred");
        assert_eq!(
            report["requested_compressed_vector_search_mode"],
            "preferred"
        );
        assert_eq!(report["retrieval_projection_advisor"]["ready"], true);
        assert_eq!(
            report["retrieval_projection_advisor_blocker_codes"],
            serde_json::json!([])
        );
    }

    #[test]
    fn embedded_store_opens_from_options_with_sanitized_report() {
        let root = unique_nowledge_mem_test_dir("open_options");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::WritableCutover,
        );

        let (mut store, report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        store
            .graph_mut()
            .query("CREATE (:Memory {id: 'mem-open', title: 'Open options'})")
            .unwrap();

        assert_eq!(report.protocol, NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL);
        assert_eq!(report.mode, NowledgeMemGraphMode::WritableCutover);
        assert_eq!(
            report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert!(report.graph_opened);
        assert!(report.search_projection_opened);
        assert!(store.search_projection().is_some());
        assert_eq!(
            store
                .graph_mut()
                .query("MATCH (m:Memory {id: 'mem-open'}) RETURN m.title AS title")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn embedded_store_applies_incremental_graph_search_projection_delta() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'new', title: 'Incremental facade', content: 'Graph changes feed search projection'})")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let request = store
            .build_search_projection_graph_delta_request_from_freshness(Some(4))
            .unwrap()
            .expect("expected graph delta request");
        let plan = store
            .search_projection_graph_delta_background_work_plan(
                &request,
                BackgroundWorkHint::default(),
            )
            .expect("expected background work plan");

        let report = store.apply_search_projection_graph_delta(request).unwrap();

        assert_eq!(plan.request.class, WorkClass::Projection);
        assert_eq!(report.upserted_documents, 1);
        assert_eq!(
            store
                .search_projection()
                .unwrap()
                .index()
                .document("memory:new")
                .unwrap()
                .title,
            "Incremental facade"
        );
    }

    #[test]
    fn embedded_store_background_delta_uses_scheduler_qos() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'new', title: 'Scheduled facade'})")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = store
            .build_search_projection_graph_delta_request_from_freshness(Some(4))
            .unwrap()
            .expect("expected graph delta request");
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(0),
            ..LocalQosPolicy::default()
        });

        let error = store
            .apply_scheduled_background_search_projection_graph_delta(&mut scheduler, request)
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("background search projection graph delta"));
        assert!(store
            .search_projection()
            .unwrap()
            .index()
            .document("memory:new")
            .is_none());
    }

    #[test]
    fn embedded_store_retrieves_knowledge_through_search_projection() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-search', title: 'Facade retrieval', content: 'Skein replaces LanceDB retrieval'})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'entity-skein', name: 'Skein'})")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'mem-search'}), (e:Entity {id: 'entity-skein'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let delta = store
            .build_search_projection_graph_delta_request_from_freshness(Some(8))
            .unwrap()
            .expect("expected search projection delta");
        store.apply_search_projection_graph_delta(delta).unwrap();

        let retrieval = store
            .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
                query_text: "facade retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 4,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            })
            .unwrap();
        let output = retrieval.output;
        let report = retrieval.report;

        assert_eq!(output.search.total_hits, 1);
        assert_eq!(output.search.hits[0].id, "memory:mem-search");
        assert_eq!(output.diagnostics.projection_commit_lag, 0);
        assert!(!output.evidence.is_empty());
        assert_eq!(report.protocol, NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL);
        assert_eq!(report.mode, NowledgeMemGraphMode::WritableCutover);
        assert_eq!(
            report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert_eq!(report.search_total_hits, 1);
        assert!(report.candidate_count >= 1);
        assert!(report.evidence_count >= 1);
        assert_eq!(report.text_backend, Some("bm25_text".to_string()));
        assert_eq!(
            report.json()["protocol"],
            NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL
        );
    }

    #[test]
    fn search_projection_candidate_api_reports_metadata_pushdown() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_pushdown");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            for (external_id, lifecycle_state) in [
                ("deleted", "deleted"),
                ("forgotten", "forgotten"),
                ("active", "active"),
            ] {
                index
                    .upsert_projection_row(SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: external_id.to_string(),
                        title: format!("{external_id} candidate"),
                        body: "metadata filtered candidate read".to_string(),
                        embedding: None,
                        source_id: Some("source-1".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                        ]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::text("candidate read", 10)
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 1);
        assert_eq!(output.result.hits[0].id, "memory:active");
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL
        );
        assert_eq!(output.report.metadata_filter_count, 1);
        assert_eq!(output.report.pushed_predicate_count, 1);
        assert_eq!(output.report.residual_predicate_count, 0);
        assert_eq!(output.report.segment_count, 2);
        assert_eq!(output.report.pruned_segment_count, 1);
        assert_eq!(output.report.scanned_segment_count, 1);
        assert!(output.report.persisted_segment_descriptor_used);
        assert_eq!(output.report.filtered_out_count, 2);
        assert_eq!(
            output.report.json()["candidate_set"]["metadata_predicate_pushdown"]["field_summaries"]
                [0]["field"],
            "lifecycle_state"
        );

        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_search_candidate_output(["memory:active"], &output);
        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["evidence_source"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
        );
        assert_eq!(evidence["request_count"], 1);
        assert_eq!(evidence["candidate_primary_engine"], "skein");
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(
            evidence["filter_pushdown"]["field_summary_count"],
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len()
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_required_fields"],
            serde_json::json!([])
        );
        assert!(evidence["filter_pushdown"]["field_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary["field"] == "lifecycle_state"
                && summary["source"] == "persisted_segment_descriptor_contract"));
        assert!(evidence["filter_pushdown"]["field_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary["field"] == "confidence"
                && summary["numeric_range_summary_used"] == true));

        let direct_evidence = store
            .search_candidate_shadow_evidence_json(&request, ["memory:active"])
            .unwrap();
        assert_eq!(direct_evidence["ready"], true);
        assert_eq!(
            direct_evidence["evidence_source"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
        );
        assert_eq!(direct_evidence["candidate_identity"]["ready"], true);
        assert_eq!(direct_evidence["filter_pushdown"]["ready"], true);
        assert_eq!(
            direct_evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_reopens_search_projection_with_descriptor_pruning() {
        let root = unique_nowledge_mem_test_dir("search_candidate_library_reopen_pruning");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        {
            let mut db = Database::open(&graph_path).unwrap();
            db.query(
                "CREATE (:Memory {id: 'active', title: 'Active candidate', space_id: 'default'})",
            )
            .unwrap();
            db.checkpoint().unwrap();
        }
        {
            let mut index = SearchIndex::open(&search_path).unwrap();
            for (external_id, lifecycle_state, importance) in [
                ("deleted", "deleted", "0.95"),
                ("forgotten", "forgotten", "0.90"),
                ("active", "active", "0.80"),
            ] {
                index
                    .upsert_projection_row(SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: external_id.to_string(),
                        title: format!("{external_id} candidate"),
                        body: "checkpointed candidate read".to_string(),
                        embedding: None,
                        source_id: Some("source-1".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("unit_type".to_string(), "memory".to_string()),
                            ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                            ("importance".to_string(), importance.to_string()),
                        ]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::ShadowReadOnly,
        );
        let (store, open_report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("candidate read", 10)
            .with_metadata_filters(BTreeMap::from([
                (
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                ),
                ("importance__gte".to_string(), "0.8".to_string()),
            ]));

        let output = store.search_candidates(&request).unwrap();

        assert!(open_report.graph_opened);
        assert!(open_report.search_projection_opened);
        assert_eq!(output.result.total_hits, 1);
        assert_eq!(output.result.hits[0].id, "memory:active");
        assert_eq!(output.report.metadata_filter_count, 2);
        assert_eq!(output.report.pushed_predicate_count, 2);
        assert_eq!(output.report.residual_predicate_count, 0);
        assert!(output.report.persisted_segment_descriptor_used);
        assert_eq!(output.report.segment_count, 2);
        assert_eq!(output.report.pruned_segment_count, 1);
        assert_eq!(output.report.scanned_segment_count, 1);
        assert_eq!(output.report.filtered_out_count, 2);
        assert!(output
            .report
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries
            .iter()
            .any(|summary| summary.field == "lifecycle_state" && summary.value_summary_used));
        assert!(output
            .report
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries
            .iter()
            .any(|summary| summary.field == "importance" && summary.numeric_range_summary_used));

        let direct_evidence = store
            .search_candidate_shadow_evidence_json(&request, ["memory:active"])
            .unwrap();
        assert_eq!(direct_evidence["ready"], true);
        assert_eq!(
            direct_evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(
            direct_evidence["filter_pushdown"]["missing_numeric_range_fields"],
            serde_json::json!([])
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_reports_vector_generation_inputs() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_vector");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            for (external_id, lifecycle_state, embedding) in [
                ("aaa-deleted", "deleted", vec![1.0, 0.0]),
                ("aab-forgotten", "forgotten", vec![1.0, 0.0]),
                ("zza-active", "active", vec![1.0, 0.0]),
                ("zzb-other", "active", vec![0.6, 0.8]),
            ] {
                index
                    .upsert_projection_row(SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: external_id.to_string(),
                        title: format!("{external_id} vector candidate"),
                        body: "vector candidate read".to_string(),
                        embedding: Some(embedding),
                        source_id: Some("source-vector".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                        ]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
            .with_rank_window(Some(2))
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 2);
        assert_eq!(output.result.hits[0].id, "memory:zza-active");
        assert_eq!(output.result.hits[0].vector_rank, Some(1));
        assert_eq!(output.report.mode, SearchMode::Vector);
        assert_eq!(output.report.query_embedding_dimension, Some(2));
        assert_eq!(output.report.rank_window, Some(2));
        assert_eq!(
            output.report.retriever_backends.get("vector"),
            Some(&"scalar_vector_scan".to_string())
        );
        assert_eq!(output.report.retriever_available.get("vector"), Some(&true));
        assert_eq!(output.report.retriever_available.get("text"), Some(&false));
        assert_eq!(
            output.report.retriever_candidate_counts.get("vector"),
            Some(&2)
        );
        assert_eq!(output.report.pushed_predicate_count, 1);
        assert_eq!(output.report.pruned_segment_count, 1);
        assert_eq!(output.report.filtered_out_count, 2);
        assert!(output.report.persisted_segment_descriptor_used);
        assert_eq!(output.report.json()["query_embedding_dimension"], 2);
        assert_eq!(
            output.report.json()["retriever_candidate_counts"]["vector"],
            2
        );
        assert!(!output.report.json().to_string().contains("[1.0,0.0]"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_shadow_bridge_aggregates_text_and_vector_leg_evidence() {
        let root = unique_nowledge_mem_test_dir("search_candidate_bridge_retrievers");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "mem-leg".to_string(),
                        title: "Retriever leg candidate".to_string(),
                        body: "retriever leg candidate read".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-leg".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(31),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let text_request = NowledgeMemSearchCandidateRequest::text("retriever leg", 10)
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));
        let vector_request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));
        let text_output = projection.search_candidates_with_report(&text_request);
        let vector_output = projection.search_candidates_with_report(&vector_request);
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();

        accumulator.record_search_candidate_output(["memory:mem-leg"], &text_output);
        accumulator.record_search_candidate_output(["memory:mem-leg"], &vector_output);
        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["text_retriever_ready"], true);
        assert_eq!(evidence["vector_retriever_ready"], true);
        assert_eq!(evidence["fts_top_k_overlap_ready"], true);
        assert_eq!(evidence["vector_top_k_overlap_ready"], true);
        assert_eq!(evidence["top_k_overlap_observed"]["fts"], true);
        assert_eq!(evidence["top_k_overlap_observed"]["vector"], true);
        assert_eq!(
            evidence["candidate_readiness"]["source_chunk_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_readiness"]["fail_soft_observed"], false);
        assert_eq!(
            evidence["candidate_readiness"]["projection_marker_status_visible"],
            true
        );
        assert_eq!(
            evidence["candidate_readiness"]["projection_watermark_ready"],
            true
        );
        assert_eq!(
            evidence["candidate_readiness"]["embedding_identity_ready"],
            true
        );
        assert_eq!(evidence["retriever_leg_candidate_counts"]["text"], 1);
        assert_eq!(evidence["retriever_leg_candidate_counts"]["vector"], 1);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_preserves_source_chunk_identity() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_source_chunk");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::SourceChunk,
                    external_id: "chunk-1".to_string(),
                    title: "Source chunk candidate".to_string(),
                    body: "source chunk identity candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-1".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "memory-1".to_string(),
                    title: "Memory candidate".to_string(),
                    body: "source chunk identity candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-1".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::text("source chunk identity", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 1);
        let hit = &output.result.hits[0];
        assert_eq!(hit.id, "source_chunk:chunk-1");
        assert_eq!(hit.kind.as_deref(), Some("source_chunk"));
        assert_eq!(hit.external_id.as_deref(), Some("chunk-1"));
        assert_eq!(hit.source_id.as_deref(), Some("source-1"));
        assert_eq!(
            output.report.returned_kind_counts.get("source_chunk"),
            Some(&1)
        );
        assert_eq!(output.report.returned_missing_external_id_count, 0);
        assert_eq!(output.report.returned_missing_source_id_count, 0);
        assert_eq!(output.report.metadata_filter_count, 1);
        assert_eq!(output.report.pushed_predicate_count, 1);
        assert_eq!(
            output.report.json()["returned_kind_counts"]["source_chunk"],
            1
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("source chunk identity candidate read"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_reports_lancedb_replacement_ready_shape() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_ready");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::SourceChunk,
                        external_id: "chunk-ready".to_string(),
                        title: "Ready source chunk".to_string(),
                        body: "ready candidate replacement body".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-ready".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(19),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));
        let request = NowledgeMemSearchCandidateRequest::text("ready source chunk", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true)
                .with_embedding_identity("bge-m3", 2);

        let readiness = handle
            .search_candidate_readiness(&request, &options)
            .unwrap();

        assert!(readiness.ready);
        assert!(readiness.present);
        assert_eq!(
            readiness.protocol,
            NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL
        );
        assert!(readiness.metadata_pushdown_ready);
        assert!(readiness.segment_descriptor_ready);
        assert!(readiness.text_retriever_ready);
        assert!(readiness.source_chunk_identity_ready);
        assert!(readiness.projection_marker_status_visible);
        assert!(readiness.projection_watermark_ready);
        assert!(readiness.embedding_identity_ready);
        assert!(readiness.blocker_codes.is_empty());
        assert_eq!(
            readiness
                .candidate_report
                .projection_source_graph_commit_epoch,
            Some(19)
        );
        assert_eq!(
            readiness
                .candidate_report
                .projection_embedding_model
                .as_deref(),
            Some("bge-m3")
        );
        assert_eq!(
            readiness.candidate_report.projection_embedding_dimension,
            Some(2)
        );
        assert_eq!(
            readiness
                .candidate_report
                .returned_kind_counts
                .get("source_chunk"),
            Some(&1)
        );
        assert_eq!(readiness.json()["projection_watermark_ready"], true);
        assert_eq!(readiness.json()["embedding_identity_ready"], true);
        assert!(!readiness
            .json()
            .to_string()
            .contains("ready candidate replacement body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_blocks_missing_source_chunk_identity() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_blocked");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "memory-only".to_string(),
                    title: "Memory only candidate".to_string(),
                    body: "memory only candidate replacement body".to_string(),
                    embedding: None,
                    source_id: Some("source-memory".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("memory only candidate", 10);
        let options =
            NowledgeMemSearchCandidateReadinessOptions::default().with_source_chunk_identity(true);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert_eq!(readiness.candidate_report.returned_hit_count, 1);
        assert!(!readiness.source_chunk_identity_ready);
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_source_chunk_identity_missing"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("memory only candidate replacement body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_blocks_missing_projection_watermark() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_watermark");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::SourceChunk,
                    external_id: "chunk-no-watermark".to_string(),
                    title: "No watermark source chunk".to_string(),
                    body: "candidate watermark body".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-no-watermark".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("no watermark source chunk", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true)
                .with_embedding_identity("bge-m3", 2);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert!(readiness.text_retriever_ready);
        assert!(readiness.source_chunk_identity_ready);
        assert!(!readiness.projection_watermark_ready);
        assert!(readiness.embedding_identity_ready);
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_projection_watermark_missing"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("candidate watermark body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_lancedb_default_requires_embedding_identity() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_manifest_required");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::SourceChunk,
                        external_id: "chunk-no-manifest".to_string(),
                        title: "No manifest source chunk".to_string(),
                        body: "candidate manifest body".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-no-manifest".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(29),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("no manifest source chunk", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert!(readiness.projection_watermark_ready);
        assert!(!readiness.embedding_identity_ready);
        assert_eq!(readiness.candidate_report.projection_embedding_model, None);
        assert_eq!(
            readiness.candidate_report.projection_embedding_dimension,
            Some(2)
        );
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_embedding_identity_not_ready"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("candidate manifest body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_blocks_embedding_identity_mismatch() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_embedding");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::SourceChunk,
                        external_id: "chunk-embedding".to_string(),
                        title: "Embedding identity source chunk".to_string(),
                        body: "candidate embedding body".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-embedding".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(23),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request =
            NowledgeMemSearchCandidateRequest::text("embedding identity source chunk", 10)
                .with_metadata_filters(BTreeMap::from([(
                    "kind__in".to_string(),
                    r#"["source_chunk"]"#.to_string(),
                )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true)
                .with_embedding_identity("bge-m3", 3);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert!(readiness.projection_watermark_ready);
        assert!(!readiness.embedding_identity_ready);
        assert_eq!(
            readiness.candidate_report.projection_embedding_dimension,
            Some(2)
        );
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_embedding_identity_not_ready"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("candidate embedding body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_reports_fail_soft_vector_fallback() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_fail_soft");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "mem-fallback".to_string(),
                    title: "Fallback candidate".to_string(),
                    body: "hybrid fallback candidate read".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-fallback".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request =
            NowledgeMemSearchCandidateRequest::hybrid("hybrid fallback", vec![1.0, 0.0, 0.0], 10);

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 1);
        assert_eq!(output.result.hits[0].id, "memory:mem-fallback");
        assert_eq!(output.report.mode, SearchMode::Hybrid);
        assert_eq!(output.report.query_embedding_dimension, Some(3));
        assert_eq!(
            output.report.retriever_available.get("vector"),
            Some(&false)
        );
        assert_eq!(output.report.retriever_available.get("text"), Some(&true));
        assert_eq!(
            output.report.retriever_candidate_counts.get("text"),
            Some(&1)
        );
        assert!(output
            .report
            .fallback_reason_codes
            .iter()
            .any(|code| code == "vector_dimension_mismatch"));
        assert!(output.report.empty_reason_codes.is_empty());
        assert_eq!(
            output.report.json()["fallback_reason_codes"],
            serde_json::json!(["vector_dimension_mismatch"])
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("hybrid fallback candidate read"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_gates_compressed_vector_preference() {
        let mut index = SearchIndex::in_memory();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem-advisor".to_string(),
                title: "Advisor gated candidate".to_string(),
                body: "advisor gated compressed candidate".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: Some("source-advisor".to_string()),
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
            .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

        let output = projection.search_candidates_with_report(&request);

        assert_eq!(
            output.report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert_eq!(
            output.report.requested_compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert_eq!(
            output.report.retriever_backends.get("vector"),
            Some(&"scalar_vector_scan".to_string())
        );
        assert_eq!(
            output.report.retrieval_projection_advisor_blocker_codes,
            vec![
                "retrieval_projection_recall_evidence_missing".to_string(),
                "retrieval_projection_parity_evidence_missing".to_string(),
                "retrieval_projection_segment_not_advised".to_string()
            ]
        );
        assert_eq!(
            output.report.json()["retrieval_projection_advisor"]["ready"],
            false
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("advisor gated compressed candidate"));
    }

    #[test]
    fn search_projection_candidate_api_reports_repair_markers_without_hits() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_repair_markers");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "mem-marker".to_string(),
                    title: "Marker candidate".to_string(),
                    body: "repair marker candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-marker".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.mark_full_reindex_needed("stale projection").unwrap();
            index
                .mark_metadata_repair_needed("missing metadata")
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::text("not present", 10);

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 0);
        assert!(output.report.projection_full_reindex_needed);
        assert!(output.report.projection_metadata_repair_needed);
        assert!(output
            .report
            .empty_reason_codes
            .iter()
            .any(|code| code == "retriever_no_hits"));
        assert_eq!(output.report.json()["projection_full_reindex_needed"], true);
        assert_eq!(
            output.report.json()["projection_metadata_repair_needed"],
            true
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("stale projection"));
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("missing metadata"));
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("repair marker candidate read"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(feature = "turbovec")]
    fn embedded_store_open_options_can_prefer_compressed_vector_search() {
        let root = unique_nowledge_mem_test_dir("compressed_vector_open_options");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        {
            let mut db = Database::open(&graph_path).unwrap();
            db.query(
                "CREATE (:Memory {id: 'mem-vector', title: 'Vector facade', content: 'Compressed vector retrieval'})",
            )
            .unwrap();
            db.checkpoint().unwrap();
        }
        {
            let mut index = SearchIndex::open(&search_path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 8,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "mem-vector".to_string(),
                        title: "Vector facade".to_string(),
                        body: "Compressed vector retrieval".to_string(),
                        embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(1),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred)
        .with_retrieval_projection_advisor(
            NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(),
        );

        let (store, report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        let retrieval = store
            .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
                query_text: String::new(),
                query_embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                mode: SearchMode::Vector,
                limit: 10,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 0,
                graph_context_max_hops: 0,
            })
            .unwrap();
        let output = retrieval.output;

        assert_eq!(
            report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert_eq!(
            report.requested_compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert!(report.retrieval_projection_advisor.ready());
        assert_eq!(output.search.hits[0].id, "memory:mem-vector");
        assert_eq!(output.search.retrievers[0].backend, "turbovec_projection");
        assert_eq!(
            retrieval.report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert_eq!(
            retrieval.report.vector_backend,
            Some("turbovec_projection".to_string())
        );
        assert_eq!(
            retrieval.report.json()["vector_backend"],
            "turbovec_projection"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_retrieval_requires_search_projection() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let error = store
            .retrieve_knowledge(&KnowledgeRetrievalRequest {
                query_text: "missing projection".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 4,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            })
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "storage error: nowledge mem search projection is not configured"
        );
    }

    #[test]
    fn embedded_store_reports_background_maintenance_summary() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-maintenance', title: 'Maintenance summary'})")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let summary = store.background_maintenance_summary(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_graph_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        );

        assert_eq!(summary.total_candidates, 1);
        assert_eq!(summary.admitted_count, 1);
        assert_eq!(
            summary.top_admitted_kind,
            Some(BackgroundMaintenanceKind::SearchProjectionGraphDelta)
        );
        assert_eq!(summary.executable_search_projection_graph_delta_count, 1);
        assert_eq!(summary.admitted_search_projection_graph_delta_count, 1);
        let item = &summary.ranked[0];
        assert_eq!(
            summary.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
            item.search_projection_graph_delta_complete_through_graph_commit_epoch
        );
        assert!(summary
            .max_search_projection_graph_delta_complete_through_graph_commit_epoch
            .is_some());
        assert_eq!(item.name, "search_projection_graph_delta");
        assert_eq!(item.admission_name, "admit");
        assert_eq!(
            item.search_projection_graph_delta_upsert_node_count,
            Some(1)
        );
        assert_eq!(
            item.search_projection_graph_delta_delete_document_count,
            Some(0)
        );

        let report = store.background_maintenance_report(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_graph_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        );
        let json = report.json();

        assert_eq!(report.protocol, "skein-background-maintenance-report");
        assert!(report.present);
        assert!(report.ready);
        assert_eq!(report.total_candidates, 1);
        assert_eq!(report.ranked_count, 1);
        assert_eq!(report.foreground_ranked_count, 0);
        assert_eq!(report.unknown_admission_count, 0);
        assert_eq!(report.executable_search_projection_graph_delta_count, 1);
        assert_eq!(report.admitted_search_projection_graph_delta_count, 1);
        assert_eq!(report.slow_query_ready, Some(true));
        assert_eq!(report.slow_query_record_count, Some(0));
        assert_eq!(report.slow_query_capacity, Some(256));
        assert_eq!(report.slow_query_redaction_ready, Some(true));
        assert!(report.blocker_codes.is_empty());
        assert_eq!(json["protocol"], "skein-background-maintenance-report");
        assert_eq!(json["slow_query"]["ready"], true);
        assert_eq!(json["slow_query"]["record_count"], 0);
        assert_eq!(json["slow_query"]["capacity"], 256);
        assert_eq!(json["ranked"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn embedded_store_background_maintenance_report_fails_closed_without_work() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.background_maintenance_report(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_graph_delta_freshness: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_graph_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        );

        assert!(!report.ready);
        assert_eq!(report.total_candidates, 0);
        assert_eq!(report.ranked_count, 0);
        assert_eq!(
            report.blocker_codes,
            vec!["no_candidates".to_string(), "no_ranked_work".to_string()]
        );
    }

    fn nowledge_projection_evidence_rows() -> Vec<SearchProjectionRow> {
        vec![
            nowledge_projection_evidence_row(SearchProjectionKind::Memory, "mem_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Message, "msg_1", false),
            nowledge_projection_evidence_row(SearchProjectionKind::Community, "community_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Entity, "entity_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Source, "source_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::SourceChunk, "chunk_1", true),
        ]
    }

    fn nowledge_projection_evidence_row(
        kind: SearchProjectionKind,
        external_id: &str,
        include_embedding: bool,
    ) -> SearchProjectionRow {
        SearchProjectionRow {
            kind,
            external_id: external_id.to_string(),
            title: format!("{external_id} title"),
            body: format!("{external_id} body"),
            embedding: include_embedding.then_some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
        }
    }

    fn full_bounded_read_routes() -> Vec<String> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| (*route).to_string())
            .collect()
    }

    fn ready_route_readiness_summary() -> NowledgeMemRouteReadinessSummary {
        NowledgeMemRouteReadinessSummary {
            route_primary_ready: true,
            primary_ready_routes: full_bounded_read_routes(),
            route_query_plan_evidence_ready: true,
            route_query_profile_evidence_ready: true,
            relationship_property_pruning_required_count: 0,
            relationship_property_pruning_report_count: 0,
            route_relationship_property_pruning_evidence_ready: true,
        }
    }

    fn seed_graph_overview_memories(graph: &mut NowledgeMemGraph) {
        graph
            .query("CREATE (:Memory {id: 'overview-memory-1', title: 'Overview One', content: 'body one', pagerank_score: 3.0, importance: 0.1, community_id: 7001, space_id: 'default', created_at: 101, updated_at: 201, source: 'overview', event_start: 301, event_end: 401})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'overview-memory-2', content: 'Fallback body', importance: 2.0, community_id: 7002, space_id: 'default', created_at: 102, updated_at: 202, source: 'overview', event_start: 302, event_end: 402})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'overview-memory-3', title: 'Overview Three', pagerank_score: 1.0, importance: 0.3, community_id: 7003, space_id: 'default', created_at: 103, updated_at: 203, source: 'overview', event_start: 303, event_end: 403})")
            .unwrap();
    }

    fn seed_graph_sample_memories(graph: &mut NowledgeMemGraph) {
        graph
            .query("CREATE (:Memory {id: 'sample-memory-a', title: 'Sample A', content: 'sample body A', pagerank_score: 1.0, importance: 0.1, community_id: 8001, space_id: 'default', created_at: 101, updated_at: 201, source: 'sample'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'sample-memory-b', content: 'Sample body B', importance: 2.0, community_id: 8002, space_id: 'default', created_at: 102, updated_at: 202, source: 'sample'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'sample-memory-c', title: 'Sample C', pagerank_score: 3.0, importance: 0.3, community_id: 8003, space_id: 'default', created_at: 103, updated_at: 203, source: 'sample'})")
            .unwrap();
    }

    fn seed_graph_community_members_memories(graph: &mut NowledgeMemGraph) {
        graph
            .query("CREATE (:Memory {id: 'community-memory-high', title: 'Community High', content: 'high body', pagerank_score: 3.0, importance: 0.1, community_id: 42, space_id: 'default', created_at: 101, updated_at: 201, source: 'community'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'community-memory-low', title: 'Community Low', content: 'low body', importance: 1.0, community_id: 42, space_id: 'default', created_at: 102, updated_at: 202, source: 'community'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'community-memory-other', title: 'Community Other', content: 'other body', pagerank_score: 10.0, community_id: 7, space_id: 'default', created_at: 103, updated_at: 203, source: 'community'})")
            .unwrap();
    }

    fn seed_graph_community_recent_memories(graph: &mut NowledgeMemGraph) {
        graph
            .query("CREATE (:Memory {id: 'community-recent-old', title: 'Recent Old', content: 'old body', importance: 0.4, created_at: 10, updated_at: 20, is_crystal: false})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'community-recent-new', title: 'Recent New', content: 'new body', importance: 0.9, created_at: 30, updated_at: 40, is_crystal: false})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'community-recent-other', title: 'Recent Other', content: 'other body', importance: 1.0, created_at: 50, updated_at: 60, is_crystal: true})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'community-recent-entity-a', community_id: 3676})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'community-recent-entity-b', community_id: 3676})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'community-recent-entity-other', community_id: 9999})")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'community-recent-old'}), (e:Entity {id: 'community-recent-entity-a'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'community-recent-new'}), (e:Entity {id: 'community-recent-entity-a'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'community-recent-new'}), (e:Entity {id: 'community-recent-entity-b'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'community-recent-other'}), (e:Entity {id: 'community-recent-entity-other'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
    }

    fn seed_graph_community_subgraph(graph: &mut NowledgeMemGraph) {
        graph
            .query("CREATE (:Entity {id: 'community-subgraph-alpha', name: 'Alpha Entity', entity_type: 'concept', community_id: 3505, confidence: 0.9})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'community-subgraph-beta', name: 'Beta Entity', entity_type: 'concept', community_id: 3505, confidence: 0.7})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'community-subgraph-outside', name: 'Outside Entity', entity_type: 'concept', community_id: 9999, confidence: 1.0})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'community-subgraph-memory-a'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'community-subgraph-memory-b'})")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'community-subgraph-memory-a'}), (e:Entity {id: 'community-subgraph-alpha'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'community-subgraph-memory-b'}), (e:Entity {id: 'community-subgraph-alpha'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'community-subgraph-memory-a'}), (e:Entity {id: 'community-subgraph-beta'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        graph
            .query("MATCH (a:Entity {id: 'community-subgraph-alpha'}), (b:Entity {id: 'community-subgraph-beta'}) CREATE (a)-[:RELATES_TO {confidence: 0.77, relation_type: 'related'}]->(b)")
            .unwrap();
        graph
            .query("MATCH (a:Entity {id: 'community-subgraph-alpha'}), (b:Entity {id: 'community-subgraph-outside'}) CREATE (a)-[:RELATES_TO {confidence: 0.99, relation_type: 'outside'}]->(b)")
            .unwrap();
    }

    fn seed_graph_augmentation_state(graph: &mut NowledgeMemGraph) {
        graph
            .query("CREATE (:GraphMeta {meta_id: 'main', community_detection_applied: true, pagerank_applied: true, community_algorithm: 'louvain', community_resolution: 1.0, community_count: 12, pagerank_algorithm: 'pagerank', pagerank_damping: 0.85, pagerank_iterations: 20, last_augmentation_at: 1000, schema_version: 2, community_detection_computed_at: 900, pagerank_computed_at: 950})")
            .unwrap();
    }

    fn seed_graph_pagerank_plan(db: &mut Database) {
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true, pagerank_computed_at: 404})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'pagerank-plan-m1', created_at: 10, updated_at: 20})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'pagerank-plan-m2', created_at: 120, updated_at: 130})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'pagerank-plan-e1', name: 'Entity One', created_at: 15, updated_at: 25})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'pagerank-plan-e2', name: 'Entity Two', created_at: 140, updated_at: 150})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'pagerank-plan-m1'}), (e:Entity {id: 'pagerank-plan-e1'}) CREATE (m)-[:MENTIONS {created_at: 30}]->(e)")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'pagerank-plan-m2'}), (e:Entity {id: 'pagerank-plan-e2'}) CREATE (m)-[:MENTIONS {created_at: 160}]->(e)")
            .unwrap();
        db.query("MATCH (a:Entity {id: 'pagerank-plan-e1'}), (b:Entity {id: 'pagerank-plan-e2'}) CREATE (a)-[:RELATES_TO {created_at: 170}]->(b)")
            .unwrap();
        db.query("MATCH (a:Memory {id: 'pagerank-plan-m1'}), (b:Memory {id: 'pagerank-plan-m2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 180}]->(b)")
            .unwrap();
        db.query("MATCH (a:Memory {id: 'pagerank-plan-m2'}), (b:Memory {id: 'pagerank-plan-m1'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'inactive', created_at: 190}]->(b)")
            .unwrap();
    }

    fn seed_graph_orphan_entities(graph: &mut NowledgeMemGraph) {
        graph
            .query("CREATE (:Entity {id: 'orphan-entity', name: 'Orphan Entity', entity_type: 'concept', description: 'orphan'})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'mentioned-entity', name: 'Mentioned Entity', entity_type: 'concept'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'orphan-blocking-memory', title: 'Blocking Memory'})")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'orphan-blocking-memory'}), (e:Entity {id: 'mentioned-entity'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'related-entity', name: 'Related Entity', entity_type: 'concept'})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'related-peer', name: 'Related Peer', entity_type: 'concept'})")
            .unwrap();
        graph
            .query("MATCH (a:Entity {id: 'related-entity'}), (b:Entity {id: 'related-peer'}) CREATE (a)-[:RELATES_TO]->(b)")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'labeled-entity', name: 'Labeled Entity', entity_type: 'concept'})")
            .unwrap();
        graph
            .query("CREATE (:Label {id: 'orphan-blocking-label', name: 'Blocking Label'})")
            .unwrap();
        graph
            .query("MATCH (e:Entity {id: 'labeled-entity'}), (l:Label {id: 'orphan-blocking-label'}) CREATE (e)-[:HAS_LABEL]->(l)")
            .unwrap();
    }

    fn memory_node_id(graph: &mut NowledgeMemGraph, memory_id: &str) -> u64 {
        let mut parameters = BTreeMap::new();
        parameters.insert("id".to_string(), Value::String(memory_id.to_string()));
        let output = graph
            .query_with_params(
                "MATCH (m:Memory {id: $id}) RETURN id(m) AS node_id",
                &parameters,
            )
            .unwrap();
        required_u64_field(&output.rows[0], "node_id").unwrap()
    }

    fn ready_query_family_replacement() -> serde_json::Value {
        serde_json::json!(REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
            .iter()
            .map(|family| serde_json::json!({
                "query_family": family,
                "required_checks": 1,
                "covered_checks": 1,
                "shadow_matched_checks": 1,
                "replacement_readiness_per_million": 1_000_000,
            }))
            .collect::<Vec<_>>())
    }

    fn readiness_dashboard_area<'a>(
        dashboard: &'a NowledgeMemReadinessDashboard,
        name: &str,
    ) -> &'a NowledgeMemReadinessAreaSummary {
        dashboard
            .areas
            .iter()
            .find(|area| area.name == name)
            .expect("readiness dashboard area")
    }

    fn unique_nowledge_mem_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_nowledge_mem_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }
}
