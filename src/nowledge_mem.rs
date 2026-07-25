use crate::search::CompressedVectorSearchMode;
use crate::search_projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_shadow_evidence_json,
};
use crate::{
    cypher, BackgroundMaintenanceOptions, BackgroundMaintenanceSummary, BackgroundWorkHint,
    BackgroundWorkPlan, Database, DatabaseConfig, KnowledgeRetrievalOutput,
    KnowledgeRetrievalRequest, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    NowledgeGraphStatement, PlanCacheLookup, QueryOutput, ReadExecutionProfile, Result,
    SearchIndex, SearchProjectionDeltaReport, SearchProjectionFreshness,
    SearchProjectionGraphDeltaRequest, SearchProjectionProbeOptions, SkeinError, Value,
};
use crate::{
    nowledge_inventory::{
        background_maintenance_evidence_health, background_maintenance_summary_to_json,
        replacement_readiness_family_evidence_health, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
    },
    store::{RecoveryMode, ScanPruningReport, ScanPruningStrategy, StorageRecoveryReport},
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenOptions {
    pub graph_path: PathBuf,
    pub search_projection_path: Option<PathBuf>,
    pub mode: NowledgeMemGraphMode,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
}

impl NowledgeMemOpenOptions {
    pub fn graph_only(graph_path: impl Into<PathBuf>, mode: NowledgeMemGraphMode) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: None,
            mode,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
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
        }
    }

    pub fn with_compressed_vector_search_mode(mut self, mode: CompressedVectorSearchMode) -> Self {
        self.compressed_vector_search_mode = mode;
        self
    }

    pub fn sanitized_report(&self) -> NowledgeMemOpenReport {
        NowledgeMemOpenReport {
            protocol: NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL.to_string(),
            mode: self.mode,
            graph_configured: true,
            search_projection_configured: self.search_projection_path.is_some(),
            compressed_vector_search_mode: self.compressed_vector_search_mode,
            graph_opened: false,
            search_projection_opened: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub graph_configured: bool,
    pub search_projection_configured: bool,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
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
            "graph_opened": self.graph_opened,
            "search_projection_opened": self.search_projection_opened,
        })
    }
}

pub const NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL: &str = "skein-nowledge-mem-open-report";
pub const NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-query-report-v1";
pub const NOWLEDGE_MEM_READ_REPORT_PROTOCOL: &str = "skein-nowledge-mem-read-report";
pub const NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL: &str = "skein-nowledge-mem-retrieval-report";
pub const NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v1";
pub const NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL: &str = "skein-nowledge-mem-library-readiness-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-candidate-shadow-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE: &str = "nmem-rust-bridge";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE: &str =
    "/search-index/skein-shadow/candidate-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE: &str = "skein";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE: &str = "skein-shadow";
pub const REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES: &[&str] = &[
    "/graph/overview",
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
];

pub fn nowledge_mem_required_query_families_for_route(route: &str) -> &'static [&'static str] {
    match route {
        "/graph/overview"
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
    let blocker_codes = nowledge_mem_bounded_read_blocker_codes(report);
    let missing_covered_routes = missing_nowledge_mem_bounded_read_routes(covered_routes);
    let blocker_codes = blocker_codes
        .into_iter()
        .chain((!missing_covered_routes.is_empty()).then_some("missing_covered_routes"))
        .collect::<Vec<_>>();
    let ready = blocker_codes.is_empty();

    serde_json::json!({
        "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
        "present": true,
        "ready": ready,
        "mode": report.mode.as_str(),
        "max_rows": report.max_rows,
        "execution_row_cap": report.execution_row_cap,
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
    pub primary_candidate_identity_checksum: Option<u64>,
    pub shadow_candidate_identity_checksum: Option<u64>,
    pub matched_candidate_identity_checksum: Option<u64>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateShadowAccumulator {
    request_count: u64,
    primary_candidate_count: u64,
    shadow_candidate_count: u64,
    matched_candidate_count: u64,
    primary_only_candidate_count: u64,
    primary_candidate_identity_checksum: Option<u64>,
    shadow_candidate_identity_checksum: Option<u64>,
    matched_candidate_identity_checksum: Option<u64>,
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

    pub fn evidence(&self) -> NowledgeMemSearchCandidateShadowEvidence {
        NowledgeMemSearchCandidateShadowEvidence {
            request_count: self.request_count,
            primary_candidate_count: self.primary_candidate_count,
            shadow_candidate_count: self.shadow_candidate_count,
            matched_candidate_count: self.matched_candidate_count,
            primary_only_candidate_count: self.primary_only_candidate_count,
            primary_candidate_identity_checksum: self.primary_candidate_identity_checksum,
            shadow_candidate_identity_checksum: self.shadow_candidate_identity_checksum,
            matched_candidate_identity_checksum: self.matched_candidate_identity_checksum,
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
            primary_candidate_identity_checksum: None,
            shadow_candidate_identity_checksum: None,
            matched_candidate_identity_checksum: None,
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
    let ready = blocker_codes.is_empty();
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
        "candidate_identity": candidate_identity,
        "blocker_codes": blocker_codes,
    })
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
    blockers.into_iter().collect()
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

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NowledgeMemReadinessOptions {
    pub bounded_read_probe: Option<NowledgeGraphStatement>,
    pub bounded_read_evidence: Option<serde_json::Value>,
    pub covered_routes: Vec<String>,
    pub replacement_readiness_by_query_family: Option<serde_json::Value>,
    pub read_options: NowledgeMemReadOptions,
    pub search_projection_evidence: Option<serde_json::Value>,
    pub search_projection_probe_options: SearchProjectionProbeOptions,
    pub primary_search_projection_probe: Option<serde_json::Value>,
    pub search_projection_shadow_evidence: Option<serde_json::Value>,
    pub qos_policy: LocalQosPolicy,
    pub qos_state: LocalQosState,
    pub background_maintenance_options: BackgroundMaintenanceOptions,
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
        let report = nowledge_mem_query_report(
            self.mode,
            &execution_trace.statement,
            execution_trace.optimizer_trace.as_ref(),
            execution_trace.plan_cache_lookup,
            execution_trace.execution_profile.as_ref(),
            options,
            elapsed_micros,
        );
        Ok(NowledgeMemQueryOutput { output, report })
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
}

#[derive(Debug)]
pub struct NowledgeMemEmbeddedStore {
    graph: NowledgeMemGraph,
    search_projection: Option<NowledgeMemSearchProjection>,
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
                options.compressed_vector_search_mode,
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

    pub fn search_projection_shadow_evidence_json(
        &self,
        primary_probe: &serde_json::Value,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self
            .require_search_projection()?
            .shadow_evidence_json(primary_probe, options))
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

    pub fn library_readiness_json(
        &mut self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        let bounded_read_evidence = options
            .bounded_read_evidence
            .clone()
            .unwrap_or_else(|| self.bounded_read_probe_evidence_json(options));
        let storage_recovery =
            storage_recovery_report_json(&self.graph.database().storage_recovery_report());
        let background_maintenance =
            background_maintenance_summary_to_json(&self.background_maintenance_summary(
                &options.qos_policy,
                &options.qos_state,
                options.background_maintenance_options.clone(),
            ));
        let query_family_evidence = query_family_replacement_evidence_json(
            options.replacement_readiness_by_query_family.as_ref(),
        );
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
        let blocker_codes = library_readiness_blocker_codes(
            &bounded_read_evidence,
            &storage_recovery,
            &background_maintenance,
            &query_family_evidence,
            &search_projection_evidence,
            &search_projection_shadow_evidence,
        );
        let readiness_by_area = library_readiness_by_area_json(
            &bounded_read_evidence,
            &storage_recovery,
            &background_maintenance,
            &query_family_evidence,
            &search_projection_evidence,
            &search_projection_shadow_evidence,
        );
        let ready_area_count = readiness_area_count(&readiness_by_area, true);
        let blocked_area_count = readiness_area_count(&readiness_by_area, false);
        let ready = blocker_codes.is_empty();

        serde_json::json!({
            "protocol": NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
            "present": true,
            "ready": ready,
            "mode": self.graph.mode().as_str(),
            "blocker_codes": blocker_codes,
            "readiness_by_area": readiness_by_area,
            "ready_area_count": ready_area_count,
            "blocked_area_count": blocked_area_count,
            "graph": {
                "open": true,
                "mode": self.graph.mode().as_str(),
                "read_only": self.graph.database().config().read_only,
            },
            "bounded_read_evidence": bounded_read_evidence,
            "storage_recovery": storage_recovery,
            "background_maintenance": background_maintenance,
            "query_family_evidence": query_family_evidence,
            "search_projection_evidence": search_projection_evidence,
            "search_projection_shadow_evidence": search_projection_shadow_evidence,
        })
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
            Ok(read) => nowledge_mem_bounded_read_evidence_json_with_routes(
                &read.report,
                &options.covered_routes,
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

fn nowledge_mem_query_report(
    mode: NowledgeMemGraphMode,
    statement: &cypher::Statement,
    trace: Option<&crate::optimizer::OptimizerTrace>,
    plan_cache_lookup: Option<PlanCacheLookup>,
    execution_profile: Option<&ReadExecutionProfile>,
    options: NowledgeMemQueryReportOptions,
    elapsed_micros: u128,
) -> NowledgeMemQueryReport {
    let statement_kind = crate::api::statement_kind(nowledge_statement_body(statement));
    let decision = nowledge_mem_query_execution_path(statement);
    let slow_log_candidate = options
        .slow_log_threshold_micros
        .is_some_and(|threshold| elapsed_micros >= threshold);
    let plan_cache = NowledgeMemPlanCacheReport::from_lookup(plan_cache_lookup);
    NowledgeMemQueryReport {
        protocol: NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL.to_string(),
        mode,
        statement_kind: statement_kind.to_string(),
        execution_path: decision.execution_path,
        fast_path_reason: decision.fast_path_reason.map(str::to_string),
        elapsed_micros,
        slow_log_threshold_micros: options.slow_log_threshold_micros,
        slow_log_candidate,
        physical_plan_captured: trace.is_some(),
        plan_cache_lookup: plan_cache_lookup.map(|lookup| lookup.as_str().to_string()),
        plan_cache_bypass_reason: plan_cache_lookup
            .and_then(|lookup| lookup.bypass_reason())
            .map(|reason| reason.as_str().to_string()),
        plan_cache_cacheable: plan_cache.cacheable,
        plan_cache_hit: plan_cache.hit,
        plan_cache_miss: plan_cache.miss,
        plan_cache_bypassed: plan_cache.bypassed,
        physical_operator_counts: trace
            .map(|trace| trace.selected_plan_operator_counts.clone())
            .unwrap_or_default(),
        optimizer_decision_count: trace.map(|trace| trace.decisions.len()).unwrap_or_default(),
        scan_pruning_reports: execution_profile
            .map(|profile| profile.scan_pruning_reports.clone())
            .unwrap_or_default(),
    }
}

fn scan_pruning_report_json(report: &ScanPruningReport) -> serde_json::Value {
    serde_json::json!({
        "label_id": report.label_id.map(|label_id| label_id.0),
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
        ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
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

fn storage_recovery_report_json(report: &StorageRecoveryReport) -> serde_json::Value {
    let durable_recovery_observed = report.durable;
    let checkpoint_boundary_present =
        report.checkpoint_epoch.is_some() || report.checkpoint_commit_epoch.is_some();
    let wal_replay_bounded = report
        .max_wal_replay_entries
        .is_some_and(|limit| report.replayed_wal_entries <= limit);
    let torn_tail_clean = !report.torn_tail_ignored;
    let mut blocker_codes = Vec::new();
    if !durable_recovery_observed {
        blocker_codes.push("durable_recovery_not_observed");
    }
    if !checkpoint_boundary_present {
        blocker_codes.push("checkpoint_boundary_missing");
    }
    if !wal_replay_bounded {
        blocker_codes.push("wal_replay_unbounded");
    }
    if !torn_tail_clean {
        blocker_codes.push("torn_tail_observed");
    }
    serde_json::json!({
        "protocol": "skein-storage-recovery-report",
        "present": true,
        "ready": blocker_codes.is_empty(),
        "durable": report.durable,
        "recovery_mode": recovery_mode_name(report.recovery_mode),
        "checkpoint_epoch": report.checkpoint_epoch,
        "checkpoint_commit_epoch": report.checkpoint_commit_epoch,
        "wal_present": report.wal_present,
        "wal_replay_start_lsn": report.wal_replay_start_lsn,
        "next_lsn_after_replay": report.next_lsn_after_replay,
        "replayed_wal_entries": report.replayed_wal_entries,
        "torn_tail_ignored": report.torn_tail_ignored,
        "recovered_commit_epoch": report.recovered_commit_epoch,
        "readiness": {
            "durable_recovery_observed": durable_recovery_observed,
            "checkpoint_boundary_present": checkpoint_boundary_present,
            "wal_replay_bounded": wal_replay_bounded,
            "torn_tail_clean": torn_tail_clean,
        },
        "blocker_codes": blocker_codes,
    })
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

fn library_readiness_blocker_codes(
    bounded_read_evidence: &serde_json::Value,
    storage_recovery: &serde_json::Value,
    background_maintenance: &serde_json::Value,
    query_family_evidence: &serde_json::Value,
    search_projection_evidence: &serde_json::Value,
    search_projection_shadow_evidence: &serde_json::Value,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if bounded_read_evidence
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("bounded_read_evidence_not_ready");
    }
    if storage_recovery
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("storage_recovery_not_ready");
    }
    if !library_background_maintenance_ready(background_maintenance) {
        blockers.push("background_maintenance_not_ready");
    }
    if query_family_evidence
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("query_family_evidence_not_ready");
    }
    if search_projection_evidence
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("search_projection_evidence_not_ready");
    }
    if search_projection_shadow_evidence
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("search_projection_shadow_evidence_not_ready");
    }
    blockers
}

fn library_readiness_by_area_json(
    bounded_read_evidence: &serde_json::Value,
    storage_recovery: &serde_json::Value,
    background_maintenance: &serde_json::Value,
    query_family_evidence: &serde_json::Value,
    search_projection_evidence: &serde_json::Value,
    search_projection_shadow_evidence: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "graph": {
            "ready": true,
            "blocker_codes": [],
        },
        "query": readiness_area_json(
            bounded_read_evidence,
            "bounded_read_evidence_not_ready"
        ),
        "storage": readiness_area_json(
            storage_recovery,
            "storage_recovery_not_ready"
        ),
        "background": background_maintenance_readiness_area_json(background_maintenance),
        "query_family": readiness_area_json(
            query_family_evidence,
            "query_family_evidence_not_ready"
        ),
        "search_projection": readiness_area_json(
            search_projection_evidence,
            "search_projection_evidence_not_ready"
        ),
        "search_projection_shadow": readiness_area_json(
            search_projection_shadow_evidence,
            "search_projection_shadow_evidence_not_ready"
        ),
    })
}

fn readiness_area_json(
    evidence: &serde_json::Value,
    fallback_blocker_code: &'static str,
) -> serde_json::Value {
    let ready = evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true);
    serde_json::json!({
        "ready": ready,
        "blocker_codes": readiness_blocker_codes(evidence, fallback_blocker_code, ready),
    })
}

fn background_maintenance_readiness_area_json(
    background_maintenance: &serde_json::Value,
) -> serde_json::Value {
    let health = background_maintenance_evidence_health(Some(background_maintenance), true);
    serde_json::json!({
        "ready": health.ready,
        "blocker_codes": health.blocker_codes,
    })
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

fn readiness_area_count(readiness_by_area: &serde_json::Value, ready: bool) -> usize {
    readiness_by_area
        .as_object()
        .into_iter()
        .flat_map(serde_json::Map::values)
        .filter(|area| area.get("ready").and_then(serde_json::Value::as_bool) == Some(ready))
        .count()
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

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_bounded_read_evidence_json,
        nowledge_mem_bounded_read_evidence_json_with_routes, nowledge_mem_graph_config,
        nowledge_mem_graph_config_with_search_mode,
        nowledge_mem_search_candidate_shadow_evidence_json, NowledgeMemEmbeddedStore,
        NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemOpenOptions,
        NowledgeMemQueryExecutionPath, NowledgeMemQueryReportOptions, NowledgeMemReadOptions,
        NowledgeMemReadReport, NowledgeMemReadinessOptions,
        NowledgeMemSearchCandidateShadowAccumulator, NowledgeMemSearchCandidateShadowEvidence,
        NowledgeMemSearchProjection, NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
        NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL, NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
        NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
        NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
    };
    use crate::search::CompressedVectorSearchMode;
    use crate::search::SearchFusionWeights;
    use crate::{
        BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundWorkHint, Database,
        DatabaseConfig, KnowledgeCandidateScoringPolicy, KnowledgeRetrievalRequest, LocalQosPolicy,
        LocalQosScheduler, LocalQosState, NowledgeGraphStatement, SearchEmbeddingManifest,
        SearchIndex, SearchMode, SearchProjectionDelta, SearchProjectionKind,
        SearchProjectionProbeOptions, SearchProjectionRow, WorkClass,
    };
    use std::collections::BTreeMap;

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
            serde_json::json!(["missing_covered_routes"])
        );
        let covered_routes = full_bounded_read_routes();
        let evidence =
            nowledge_mem_bounded_read_evidence_json_with_routes(&read.report, &covered_routes);
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
                "missing_covered_routes"
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
            serde_json::json!(["not_shadow_read_only", "missing_covered_routes"])
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
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["candidate_identity"]["parity"], true);
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
                primary_candidate_identity_checksum: None,
                shadow_candidate_identity_checksum: None,
                matched_candidate_identity_checksum: None,
                blocker_codes: vec!["bridge_timeout".to_string()],
            },
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "bridge_timeout",
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

        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["primary_candidate_count"], 3);
        assert_eq!(evidence["shadow_candidate_count"], 3);
        assert_eq!(evidence["matched_candidate_count"], 3);
        assert_eq!(evidence["primary_only_candidate_count"], 0);
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_candidate_shadow_accumulator_preserves_request_blockers() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1"]);
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
            readiness["readiness_by_area"]["search_projection"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_projection_shadow"]["ready"],
            false
        );
        assert_eq!(readiness["ready_area_count"], 1);
        assert_eq!(readiness["blocked_area_count"], 6);
        assert!(!readiness.to_string().contains("redacted"));
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
    fn open_options_report_exposes_advanced_compressed_vector_search_mode() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

        let report = options.sanitized_report().json();

        assert_eq!(report["compressed_vector_search_mode"], "preferred");
        assert!(!report.to_string().contains("redacted_graph_path"));
        assert!(!report.to_string().contains("redacted_search_path"));
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
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

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
