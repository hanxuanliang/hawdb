use std::collections::{BTreeMap, BTreeSet};

pub const NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL: &str =
    "skein-nowledge-mem-search-route-ownership-v1";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-active-search-route-readiness-v1";

pub const NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY: &str = "memory";
pub const NOWLEDGE_MEM_SEARCH_ROUTE_MESSAGE: &str = "message";
pub const NOWLEDGE_MEM_SEARCH_ROUTE_COMMUNITY: &str = "community";
pub const NOWLEDGE_MEM_SEARCH_ROUTE_ENTITY: &str = "entity";
pub const NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE: &str = "source";
pub const NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE_CHUNK: &str = "source_chunk";

pub const REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES: &[&str] = &[
    NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY,
    NOWLEDGE_MEM_SEARCH_ROUTE_MESSAGE,
    NOWLEDGE_MEM_SEARCH_ROUTE_COMMUNITY,
    NOWLEDGE_MEM_SEARCH_ROUTE_ENTITY,
    NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE,
    NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE_CHUNK,
];

pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS: &str = "thread_message_fts";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_ENTITY_DISCOVERY: &str = "entity_discovery";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_COMMUNITY_DISCOVERY: &str = "community_discovery";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_RECALL: &str = "source_recall";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_CHUNK_RECALL: &str = "source_chunk_recall";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_FS_RECALL: &str = "fs_recall";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH: &str = "mcp_search";
pub const NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_DEEP_SEARCH_GRAPH_EXPANSION: &str =
    "deep_search_graph_expansion";

pub const REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES: &[&str] = &[
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_ENTITY_DISCOVERY,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_COMMUNITY_DISCOVERY,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_RECALL,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_CHUNK_RECALL,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_FS_RECALL,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_DEEP_SEARCH_GRAPH_EXPANSION,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemSearchRouteOwnershipPolicy {
    pub require_all_skein: bool,
}

impl NowledgeMemSearchRouteOwnershipPolicy {
    pub const fn migration() -> Self {
        Self {
            require_all_skein: false,
        }
    }

    pub const fn production_cutover() -> Self {
        Self {
            require_all_skein: true,
        }
    }
}

impl Default for NowledgeMemSearchRouteOwnershipPolicy {
    fn default() -> Self {
        Self::migration()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NowledgeMemSearchReadEngine {
    LanceDb,
    Skein,
}

impl NowledgeMemSearchReadEngine {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LanceDb => "lancedb",
            Self::Skein => "skein",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchRouteOwnership {
    pub route: String,
    pub read_engine: NowledgeMemSearchReadEngine,
}

impl NowledgeMemSearchRouteOwnership {
    pub fn new(route: impl Into<String>, read_engine: NowledgeMemSearchReadEngine) -> Self {
        Self {
            route: route.into(),
            read_engine,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemActiveSearchRouteOwnership {
    pub route: String,
    pub projection_route: String,
    pub read_engine: NowledgeMemSearchReadEngine,
}

impl NowledgeMemActiveSearchRouteOwnership {
    pub fn new(
        route: impl Into<String>,
        projection_route: impl Into<String>,
        read_engine: NowledgeMemSearchReadEngine,
    ) -> Self {
        Self {
            route: route.into(),
            projection_route: projection_route.into(),
            read_engine,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchRouteOwnershipReadinessReport {
    pub protocol: String,
    pub ready: bool,
    pub production_cutover_ready: bool,
    pub require_all_skein: bool,
    pub required_route_count: usize,
    pub explicit_route_count: usize,
    pub skein_route_count: usize,
    pub lancedb_route_count: usize,
    pub routes: Vec<NowledgeMemSearchRouteOwnership>,
    pub skein_routes: Vec<String>,
    pub lancedb_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub conflicting_routes: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemSearchRouteOwnershipReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "production_cutover_ready": self.production_cutover_ready,
            "require_all_skein": self.require_all_skein,
            "required_route_count": self.required_route_count,
            "explicit_route_count": self.explicit_route_count,
            "skein_route_count": self.skein_route_count,
            "lancedb_route_count": self.lancedb_route_count,
            "routes": self.routes.iter().map(search_route_ownership_json).collect::<Vec<_>>(),
            "skein_routes": self.skein_routes,
            "lancedb_routes": self.lancedb_routes,
            "missing_required_routes": self.missing_required_routes,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "conflicting_routes": self.conflicting_routes,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemActiveSearchRouteOwnershipReadinessReport {
    pub protocol: String,
    pub ready: bool,
    pub production_cutover_ready: bool,
    pub require_all_skein: bool,
    pub required_route_count: usize,
    pub explicit_route_count: usize,
    pub skein_route_count: usize,
    pub lancedb_route_count: usize,
    pub routes: Vec<NowledgeMemActiveSearchRouteOwnership>,
    pub skein_routes: Vec<String>,
    pub lancedb_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub invalid_projection_routes: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemActiveSearchRouteOwnershipReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL,
            "ready": self.ready,
            "production_cutover_ready": self.production_cutover_ready,
            "require_all_skein": self.require_all_skein,
            "required_route_count": self.required_route_count,
            "explicit_route_count": self.explicit_route_count,
            "skein_route_count": self.skein_route_count,
            "lancedb_route_count": self.lancedb_route_count,
            "routes": self.routes.iter().map(active_search_route_ownership_json).collect::<Vec<_>>(),
            "skein_routes": self.skein_routes,
            "lancedb_routes": self.lancedb_routes,
            "missing_required_routes": self.missing_required_routes,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "invalid_projection_routes": self.invalid_projection_routes,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemActiveSearchRouteReadEvidence {
    pub route: String,
    pub projection_route: String,
    pub read_engine: NowledgeMemSearchReadEngine,
    pub candidate_readiness_ready: bool,
    pub candidate_identity_ready: bool,
    pub embedding_identity_ready: bool,
    pub zero_vector_semantics_ready: bool,
    pub cjk_tokenization_ready: bool,
    pub metadata_pushdown_ready: bool,
    pub ranking_window_ready: bool,
    pub ranking_ready: bool,
    pub fail_soft_ready: bool,
    pub fail_soft_reason_codes_ready: bool,
    pub repair_rebuild_markers_ready: bool,
    pub lancedb_handle_required: bool,
}

impl NowledgeMemActiveSearchRouteReadEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        route: impl Into<String>,
        projection_route: impl Into<String>,
        read_engine: NowledgeMemSearchReadEngine,
        candidate_readiness_ready: bool,
        candidate_identity_ready: bool,
        metadata_pushdown_ready: bool,
        ranking_ready: bool,
        fail_soft_ready: bool,
        lancedb_handle_required: bool,
    ) -> Self {
        Self {
            route: route.into(),
            projection_route: projection_route.into(),
            read_engine,
            candidate_readiness_ready,
            candidate_identity_ready,
            embedding_identity_ready: true,
            zero_vector_semantics_ready: true,
            cjk_tokenization_ready: true,
            metadata_pushdown_ready,
            ranking_window_ready: true,
            ranking_ready,
            fail_soft_ready,
            fail_soft_reason_codes_ready: true,
            repair_rebuild_markers_ready: true,
            lancedb_handle_required,
        }
    }

    pub fn ready_skein(route: impl Into<String>, projection_route: impl Into<String>) -> Self {
        Self::new(
            route,
            projection_route,
            NowledgeMemSearchReadEngine::Skein,
            true,
            true,
            true,
            true,
            true,
            false,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemActiveSearchRouteReadinessReport {
    pub protocol: String,
    pub ready: bool,
    pub production_cutover_ready: bool,
    pub require_all_skein: bool,
    pub required_route_count: usize,
    pub evidence_route_count: usize,
    pub ready_route_count: usize,
    pub skein_route_count: usize,
    pub lancedb_handle_required_route_count: usize,
    pub routes: Vec<NowledgeMemActiveSearchRouteReadEvidence>,
    pub ready_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub invalid_projection_routes: Vec<String>,
    pub non_skein_routes: Vec<String>,
    pub lancedb_handle_required_routes: Vec<String>,
    pub candidate_not_ready_routes: Vec<String>,
    pub candidate_identity_not_ready_routes: Vec<String>,
    pub embedding_identity_not_ready_routes: Vec<String>,
    pub zero_vector_semantics_not_ready_routes: Vec<String>,
    pub cjk_tokenization_not_ready_routes: Vec<String>,
    pub metadata_pushdown_not_ready_routes: Vec<String>,
    pub ranking_window_not_ready_routes: Vec<String>,
    pub ranking_not_ready_routes: Vec<String>,
    pub fail_soft_not_ready_routes: Vec<String>,
    pub fail_soft_reason_codes_not_ready_routes: Vec<String>,
    pub repair_rebuild_markers_not_ready_routes: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemActiveSearchRouteReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "production_cutover_ready": self.production_cutover_ready,
            "require_all_skein": self.require_all_skein,
            "required_route_count": self.required_route_count,
            "evidence_route_count": self.evidence_route_count,
            "ready_route_count": self.ready_route_count,
            "skein_route_count": self.skein_route_count,
            "lancedb_handle_required_route_count": self.lancedb_handle_required_route_count,
            "routes": self.routes.iter().map(active_search_route_read_evidence_json).collect::<Vec<_>>(),
            "ready_routes": self.ready_routes,
            "missing_required_routes": self.missing_required_routes,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "invalid_projection_routes": self.invalid_projection_routes,
            "non_skein_routes": self.non_skein_routes,
            "lancedb_handle_required_routes": self.lancedb_handle_required_routes,
            "candidate_not_ready_routes": self.candidate_not_ready_routes,
            "candidate_identity_not_ready_routes": self.candidate_identity_not_ready_routes,
            "embedding_identity_not_ready_routes": self.embedding_identity_not_ready_routes,
            "zero_vector_semantics_not_ready_routes": self.zero_vector_semantics_not_ready_routes,
            "cjk_tokenization_not_ready_routes": self.cjk_tokenization_not_ready_routes,
            "metadata_pushdown_not_ready_routes": self.metadata_pushdown_not_ready_routes,
            "ranking_window_not_ready_routes": self.ranking_window_not_ready_routes,
            "ranking_not_ready_routes": self.ranking_not_ready_routes,
            "fail_soft_not_ready_routes": self.fail_soft_not_ready_routes,
            "fail_soft_reason_codes_not_ready_routes": self.fail_soft_reason_codes_not_ready_routes,
            "repair_rebuild_markers_not_ready_routes": self.repair_rebuild_markers_not_ready_routes,
            "blocker_codes": self.blocker_codes,
        })
    }
}

pub fn nowledge_mem_search_route_ownership_all_lancedb() -> Vec<NowledgeMemSearchRouteOwnership> {
    nowledge_mem_search_route_ownership_for_engine(NowledgeMemSearchReadEngine::LanceDb)
}

pub fn nowledge_mem_search_route_ownership_all_skein() -> Vec<NowledgeMemSearchRouteOwnership> {
    nowledge_mem_search_route_ownership_for_engine(NowledgeMemSearchReadEngine::Skein)
}

pub fn nowledge_mem_search_route_ownership_for_engine(
    read_engine: NowledgeMemSearchReadEngine,
) -> Vec<NowledgeMemSearchRouteOwnership> {
    REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES
        .iter()
        .map(|route| NowledgeMemSearchRouteOwnership::new(*route, read_engine))
        .collect()
}

pub fn nowledge_mem_active_search_route_ownership_all_lancedb(
) -> Vec<NowledgeMemActiveSearchRouteOwnership> {
    nowledge_mem_active_search_route_ownership_for_engine(NowledgeMemSearchReadEngine::LanceDb)
}

pub fn nowledge_mem_active_search_route_ownership_all_skein(
) -> Vec<NowledgeMemActiveSearchRouteOwnership> {
    nowledge_mem_active_search_route_ownership_for_engine(NowledgeMemSearchReadEngine::Skein)
}

pub fn nowledge_mem_active_search_route_ownership_for_engine(
    read_engine: NowledgeMemSearchReadEngine,
) -> Vec<NowledgeMemActiveSearchRouteOwnership> {
    REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
        .iter()
        .map(|route| {
            NowledgeMemActiveSearchRouteOwnership::new(
                *route,
                required_projection_route_for_active_search_route(route),
                read_engine,
            )
        })
        .collect()
}

pub fn nowledge_mem_active_search_route_read_evidence_all_skein_ready(
) -> Vec<NowledgeMemActiveSearchRouteReadEvidence> {
    REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
        .iter()
        .map(|route| {
            NowledgeMemActiveSearchRouteReadEvidence::ready_skein(
                *route,
                required_projection_route_for_active_search_route(route),
            )
        })
        .collect()
}

pub fn nowledge_mem_search_route_ownership_readiness(
    routes: &[NowledgeMemSearchRouteOwnership],
    policy: NowledgeMemSearchRouteOwnershipPolicy,
) -> NowledgeMemSearchRouteOwnershipReadinessReport {
    let required_routes = REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    let mut route_engines = BTreeMap::<&str, BTreeSet<NowledgeMemSearchReadEngine>>::new();
    for route in routes {
        *route_counts.entry(route.route.as_str()).or_default() += 1;
        route_engines
            .entry(route.route.as_str())
            .or_default()
            .insert(route.read_engine);
    }

    let explicit_required_routes = route_counts
        .keys()
        .copied()
        .filter(|route| required_routes.contains(route))
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES
        .iter()
        .copied()
        .filter(|route| !explicit_required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_routes = route_counts
        .keys()
        .copied()
        .filter(|route| !required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let conflicting_routes = route_engines
        .iter()
        .filter(|(_, engines)| engines.len() > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let skein_routes = search_routes_by_engine(routes, NowledgeMemSearchReadEngine::Skein);
    let lancedb_routes = search_routes_by_engine(routes, NowledgeMemSearchReadEngine::LanceDb);

    let mut blocker_codes = Vec::new();
    if !missing_required_routes.is_empty() {
        blocker_codes.push("search_route_ownership_missing_required_routes".to_string());
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("search_route_ownership_unknown_routes".to_string());
    }
    if !duplicate_routes.is_empty() {
        blocker_codes.push("search_route_ownership_duplicate_routes".to_string());
    }
    if !conflicting_routes.is_empty() {
        blocker_codes.push("search_route_ownership_conflicting_routes".to_string());
    }
    if policy.require_all_skein && !lancedb_routes.is_empty() {
        blocker_codes.push("search_route_ownership_lancedb_routes_remaining".to_string());
    }

    let ready = blocker_codes.is_empty();
    NowledgeMemSearchRouteOwnershipReadinessReport {
        protocol: NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL.to_string(),
        ready,
        production_cutover_ready: ready && policy.require_all_skein,
        require_all_skein: policy.require_all_skein,
        required_route_count: REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES.len(),
        explicit_route_count: explicit_required_routes.len(),
        skein_route_count: skein_routes.len(),
        lancedb_route_count: lancedb_routes.len(),
        routes: normalized_search_routes(routes),
        skein_routes,
        lancedb_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        conflicting_routes,
        blocker_codes,
    }
}

pub fn nowledge_mem_active_search_route_ownership_readiness(
    routes: &[NowledgeMemActiveSearchRouteOwnership],
    policy: NowledgeMemSearchRouteOwnershipPolicy,
) -> NowledgeMemActiveSearchRouteOwnershipReadinessReport {
    let required_routes = REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let required_projection_routes = REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *route_counts.entry(route.route.as_str()).or_default() += 1;
    }

    let explicit_required_routes = route_counts
        .keys()
        .copied()
        .filter(|route| required_routes.contains(route))
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
        .iter()
        .copied()
        .filter(|route| !explicit_required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_routes = route_counts
        .keys()
        .copied()
        .filter(|route| !required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let invalid_projection_routes = routes
        .iter()
        .filter(|route| !required_projection_routes.contains(route.projection_route.as_str()))
        .map(|route| route.route.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let skein_routes = active_search_routes_by_engine(routes, NowledgeMemSearchReadEngine::Skein);
    let lancedb_routes =
        active_search_routes_by_engine(routes, NowledgeMemSearchReadEngine::LanceDb);

    let mut blocker_codes = Vec::new();
    if !missing_required_routes.is_empty() {
        blocker_codes.push("active_search_route_ownership_missing_required_routes".to_string());
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("active_search_route_ownership_unknown_routes".to_string());
    }
    if !duplicate_routes.is_empty() {
        blocker_codes.push("active_search_route_ownership_duplicate_routes".to_string());
    }
    if !invalid_projection_routes.is_empty() {
        blocker_codes.push("active_search_route_ownership_invalid_projection_routes".to_string());
    }
    if policy.require_all_skein && !lancedb_routes.is_empty() {
        blocker_codes.push("active_search_route_ownership_lancedb_routes_remaining".to_string());
    }

    let ready = blocker_codes.is_empty();
    NowledgeMemActiveSearchRouteOwnershipReadinessReport {
        protocol: NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL.to_string(),
        ready,
        production_cutover_ready: ready && policy.require_all_skein,
        require_all_skein: policy.require_all_skein,
        required_route_count: REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        explicit_route_count: explicit_required_routes.len(),
        skein_route_count: skein_routes.len(),
        lancedb_route_count: lancedb_routes.len(),
        routes: normalized_active_search_routes(routes),
        skein_routes,
        lancedb_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        invalid_projection_routes,
        blocker_codes,
    }
}

pub fn nowledge_mem_active_search_route_readiness(
    evidence: &[NowledgeMemActiveSearchRouteReadEvidence],
    policy: NowledgeMemSearchRouteOwnershipPolicy,
) -> NowledgeMemActiveSearchRouteReadinessReport {
    let required_routes = REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let required_projection_routes = REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    for route in evidence {
        *route_counts.entry(route.route.as_str()).or_default() += 1;
    }

    let explicit_required_routes = route_counts
        .keys()
        .copied()
        .filter(|route| required_routes.contains(route))
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
        .iter()
        .copied()
        .filter(|route| !explicit_required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_routes = route_counts
        .keys()
        .copied()
        .filter(|route| !required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let invalid_projection_routes = evidence
        .iter()
        .filter(|route| !required_projection_routes.contains(route.projection_route.as_str()))
        .map(|route| route.route.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let non_skein_routes = active_search_read_routes_where(evidence, |route| {
        route.read_engine != NowledgeMemSearchReadEngine::Skein
    });
    let lancedb_handle_required_routes =
        active_search_read_routes_where(evidence, |route| route.lancedb_handle_required);
    let candidate_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.candidate_readiness_ready);
    let candidate_identity_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.candidate_identity_ready);
    let embedding_identity_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.embedding_identity_ready);
    let zero_vector_semantics_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.zero_vector_semantics_ready);
    let cjk_tokenization_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.cjk_tokenization_ready);
    let metadata_pushdown_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.metadata_pushdown_ready);
    let ranking_window_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.ranking_window_ready);
    let ranking_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.ranking_ready);
    let fail_soft_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.fail_soft_ready);
    let fail_soft_reason_codes_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.fail_soft_reason_codes_ready);
    let repair_rebuild_markers_not_ready_routes =
        active_search_read_routes_where(evidence, |route| !route.repair_rebuild_markers_ready);
    let ready_routes = active_search_read_routes_where(evidence, active_search_read_evidence_ready);
    let skein_route_count = evidence
        .iter()
        .filter(|route| route.read_engine == NowledgeMemSearchReadEngine::Skein)
        .map(|route| route.route.as_str())
        .collect::<BTreeSet<_>>()
        .len();

    let mut blocker_codes = Vec::new();
    if !missing_required_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_missing_required_routes".to_string());
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_unknown_routes".to_string());
    }
    if !duplicate_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_duplicate_routes".to_string());
    }
    if !invalid_projection_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_invalid_projection_routes".to_string());
    }
    if policy.require_all_skein && !non_skein_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_non_skein_routes".to_string());
    }
    if !lancedb_handle_required_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_lancedb_handle_required".to_string());
    }
    if !candidate_not_ready_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_candidate_not_ready".to_string());
    }
    if !candidate_identity_not_ready_routes.is_empty() {
        blocker_codes
            .push("active_search_route_readiness_candidate_identity_not_ready".to_string());
    }
    if !embedding_identity_not_ready_routes.is_empty() {
        blocker_codes
            .push("active_search_route_readiness_embedding_identity_not_ready".to_string());
    }
    if !zero_vector_semantics_not_ready_routes.is_empty() {
        blocker_codes
            .push("active_search_route_readiness_zero_vector_semantics_not_ready".to_string());
    }
    if !cjk_tokenization_not_ready_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_cjk_tokenization_not_ready".to_string());
    }
    if !metadata_pushdown_not_ready_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_metadata_pushdown_not_ready".to_string());
    }
    if !ranking_window_not_ready_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_ranking_window_not_ready".to_string());
    }
    if !ranking_not_ready_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_ranking_not_ready".to_string());
    }
    if !fail_soft_not_ready_routes.is_empty() {
        blocker_codes.push("active_search_route_readiness_fail_soft_not_ready".to_string());
    }
    if !fail_soft_reason_codes_not_ready_routes.is_empty() {
        blocker_codes
            .push("active_search_route_readiness_fail_soft_reason_codes_not_ready".to_string());
    }
    if !repair_rebuild_markers_not_ready_routes.is_empty() {
        blocker_codes
            .push("active_search_route_readiness_repair_rebuild_markers_not_ready".to_string());
    }

    let ready = blocker_codes.is_empty();
    NowledgeMemActiveSearchRouteReadinessReport {
        protocol: NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL.to_string(),
        ready,
        production_cutover_ready: ready && policy.require_all_skein,
        require_all_skein: policy.require_all_skein,
        required_route_count: REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        evidence_route_count: explicit_required_routes.len(),
        ready_route_count: ready_routes.len(),
        skein_route_count,
        lancedb_handle_required_route_count: lancedb_handle_required_routes.len(),
        routes: normalized_active_search_read_evidence(evidence),
        ready_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        invalid_projection_routes,
        non_skein_routes,
        lancedb_handle_required_routes,
        candidate_not_ready_routes,
        candidate_identity_not_ready_routes,
        embedding_identity_not_ready_routes,
        zero_vector_semantics_not_ready_routes,
        cjk_tokenization_not_ready_routes,
        metadata_pushdown_not_ready_routes,
        ranking_window_not_ready_routes,
        ranking_not_ready_routes,
        fail_soft_not_ready_routes,
        fail_soft_reason_codes_not_ready_routes,
        repair_rebuild_markers_not_ready_routes,
        blocker_codes,
    }
}

pub fn required_projection_route_for_active_search_route(route: &str) -> &'static str {
    match route {
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS => NOWLEDGE_MEM_SEARCH_ROUTE_MESSAGE,
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_ENTITY_DISCOVERY => NOWLEDGE_MEM_SEARCH_ROUTE_ENTITY,
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_COMMUNITY_DISCOVERY => NOWLEDGE_MEM_SEARCH_ROUTE_COMMUNITY,
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_RECALL => NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE,
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_CHUNK_RECALL => {
            NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE_CHUNK
        }
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_FS_RECALL => NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE_CHUNK,
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH => NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY,
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_DEEP_SEARCH_GRAPH_EXPANSION => {
            NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY
        }
        _ => "",
    }
}

fn search_routes_by_engine(
    routes: &[NowledgeMemSearchRouteOwnership],
    read_engine: NowledgeMemSearchReadEngine,
) -> Vec<String> {
    let mut matching = routes
        .iter()
        .filter(|route| route.read_engine == read_engine)
        .map(|route| route.route.clone())
        .collect::<Vec<_>>();
    matching.sort();
    matching.dedup();
    matching
}

fn active_search_routes_by_engine(
    routes: &[NowledgeMemActiveSearchRouteOwnership],
    read_engine: NowledgeMemSearchReadEngine,
) -> Vec<String> {
    let mut matching = routes
        .iter()
        .filter(|route| route.read_engine == read_engine)
        .map(|route| route.route.clone())
        .collect::<Vec<_>>();
    matching.sort();
    matching.dedup();
    matching
}

fn normalized_search_routes(
    routes: &[NowledgeMemSearchRouteOwnership],
) -> Vec<NowledgeMemSearchRouteOwnership> {
    let mut normalized = routes.to_vec();
    normalized.sort_by(|left, right| {
        left.route
            .cmp(&right.route)
            .then(left.read_engine.cmp(&right.read_engine))
    });
    normalized
}

fn normalized_active_search_routes(
    routes: &[NowledgeMemActiveSearchRouteOwnership],
) -> Vec<NowledgeMemActiveSearchRouteOwnership> {
    let mut normalized = routes.to_vec();
    normalized.sort_by(|left, right| {
        left.route
            .cmp(&right.route)
            .then(left.projection_route.cmp(&right.projection_route))
            .then(left.read_engine.cmp(&right.read_engine))
    });
    normalized
}

fn normalized_active_search_read_evidence(
    evidence: &[NowledgeMemActiveSearchRouteReadEvidence],
) -> Vec<NowledgeMemActiveSearchRouteReadEvidence> {
    let mut normalized = evidence.to_vec();
    normalized.sort_by(|left, right| {
        left.route
            .cmp(&right.route)
            .then(left.projection_route.cmp(&right.projection_route))
            .then(left.read_engine.cmp(&right.read_engine))
    });
    normalized
}

fn active_search_read_routes_where(
    evidence: &[NowledgeMemActiveSearchRouteReadEvidence],
    predicate: impl Fn(&NowledgeMemActiveSearchRouteReadEvidence) -> bool,
) -> Vec<String> {
    let mut routes = evidence
        .iter()
        .filter(|route| predicate(route))
        .map(|route| route.route.clone())
        .collect::<Vec<_>>();
    routes.sort();
    routes.dedup();
    routes
}

fn active_search_read_evidence_ready(route: &NowledgeMemActiveSearchRouteReadEvidence) -> bool {
    route.read_engine == NowledgeMemSearchReadEngine::Skein
        && route.candidate_readiness_ready
        && route.candidate_identity_ready
        && route.embedding_identity_ready
        && route.zero_vector_semantics_ready
        && route.cjk_tokenization_ready
        && route.metadata_pushdown_ready
        && route.ranking_window_ready
        && route.ranking_ready
        && route.fail_soft_ready
        && route.fail_soft_reason_codes_ready
        && route.repair_rebuild_markers_ready
        && !route.lancedb_handle_required
        && route.projection_route == required_projection_route_for_active_search_route(&route.route)
}

fn search_route_ownership_json(route: &NowledgeMemSearchRouteOwnership) -> serde_json::Value {
    serde_json::json!({
        "route": route.route,
        "read_engine": route.read_engine.as_str(),
    })
}

fn active_search_route_ownership_json(
    route: &NowledgeMemActiveSearchRouteOwnership,
) -> serde_json::Value {
    serde_json::json!({
        "route": route.route,
        "projection_route": route.projection_route,
        "read_engine": route.read_engine.as_str(),
    })
}

fn active_search_route_read_evidence_json(
    route: &NowledgeMemActiveSearchRouteReadEvidence,
) -> serde_json::Value {
    serde_json::json!({
        "route": route.route,
        "projection_route": route.projection_route,
        "read_engine": route.read_engine.as_str(),
        "candidate_readiness_ready": route.candidate_readiness_ready,
        "candidate_identity_ready": route.candidate_identity_ready,
        "embedding_identity_ready": route.embedding_identity_ready,
        "zero_vector_semantics_ready": route.zero_vector_semantics_ready,
        "cjk_tokenization_ready": route.cjk_tokenization_ready,
        "metadata_pushdown_ready": route.metadata_pushdown_ready,
        "ranking_window_ready": route.ranking_window_ready,
        "ranking_ready": route.ranking_ready,
        "fail_soft_ready": route.fail_soft_ready,
        "fail_soft_reason_codes_ready": route.fail_soft_reason_codes_ready,
        "repair_rebuild_markers_ready": route.repair_rebuild_markers_ready,
        "lancedb_handle_required": route.lancedb_handle_required,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_route_ownership_accepts_all_skein_for_cutover() {
        let report = nowledge_mem_search_route_ownership_readiness(
            &nowledge_mem_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(report.ready);
        assert!(report.production_cutover_ready);
        assert_eq!(
            report.skein_route_count,
            REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES.len()
        );
        assert_eq!(report.lancedb_route_count, 0);
        assert!(report.blocker_codes.is_empty());
    }

    #[test]
    fn search_route_ownership_blocks_lancedb_routes_for_cutover() {
        let report = nowledge_mem_search_route_ownership_readiness(
            &nowledge_mem_search_route_ownership_all_lancedb(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert!(!report.production_cutover_ready);
        assert_eq!(
            report.blocker_codes,
            vec!["search_route_ownership_lancedb_routes_remaining".to_string()]
        );
        let mut expected_lancedb_routes = REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES
            .iter()
            .map(|route| (*route).to_string())
            .collect::<Vec<_>>();
        expected_lancedb_routes.sort();
        assert_eq!(report.lancedb_routes, expected_lancedb_routes);
    }

    #[test]
    fn search_route_ownership_fails_closed_on_missing_unknown_and_conflicting_routes() {
        let routes = vec![
            NowledgeMemSearchRouteOwnership::new(
                NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY,
                NowledgeMemSearchReadEngine::Skein,
            ),
            NowledgeMemSearchRouteOwnership::new(
                NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY,
                NowledgeMemSearchReadEngine::LanceDb,
            ),
            NowledgeMemSearchRouteOwnership::new("unknown", NowledgeMemSearchReadEngine::Skein),
        ];

        let report = nowledge_mem_search_route_ownership_readiness(
            &routes,
            NowledgeMemSearchRouteOwnershipPolicy::migration(),
        );

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&"search_route_ownership_missing_required_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"search_route_ownership_unknown_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"search_route_ownership_duplicate_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"search_route_ownership_conflicting_routes".to_string()));
        assert_eq!(report.unknown_routes, vec!["unknown".to_string()]);
        assert_eq!(
            report.conflicting_routes,
            vec![NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY.to_string()]
        );
    }

    #[test]
    fn active_search_route_ownership_accepts_all_skein_for_cutover() {
        let report = nowledge_mem_active_search_route_ownership_readiness(
            &nowledge_mem_active_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(report.ready);
        assert!(report.production_cutover_ready);
        assert_eq!(
            report.skein_route_count,
            REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len()
        );
        assert_eq!(report.lancedb_route_count, 0);
        assert!(report.blocker_codes.is_empty());
        assert!(report.routes.iter().any(|route| {
            route.route == NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS
                && route.projection_route == NOWLEDGE_MEM_SEARCH_ROUTE_MESSAGE
        }));
    }

    #[test]
    fn active_search_route_ownership_blocks_lancedb_routes_for_cutover() {
        let report = nowledge_mem_active_search_route_ownership_readiness(
            &nowledge_mem_active_search_route_ownership_all_lancedb(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert!(!report.production_cutover_ready);
        assert_eq!(
            report.blocker_codes,
            vec!["active_search_route_ownership_lancedb_routes_remaining".to_string()]
        );
        let mut expected_lancedb_routes = REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
            .iter()
            .map(|route| (*route).to_string())
            .collect::<Vec<_>>();
        expected_lancedb_routes.sort();
        assert_eq!(report.lancedb_routes, expected_lancedb_routes);
    }

    #[test]
    fn active_search_route_ownership_fails_closed_for_inventory_gaps() {
        let routes = vec![
            NowledgeMemActiveSearchRouteOwnership::new(
                NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS,
                NOWLEDGE_MEM_SEARCH_ROUTE_MESSAGE,
                NowledgeMemSearchReadEngine::Skein,
            ),
            NowledgeMemActiveSearchRouteOwnership::new(
                NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS,
                "invalid_projection",
                NowledgeMemSearchReadEngine::Skein,
            ),
            NowledgeMemActiveSearchRouteOwnership::new(
                "unknown_active_route",
                NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY,
                NowledgeMemSearchReadEngine::Skein,
            ),
        ];

        let report = nowledge_mem_active_search_route_ownership_readiness(
            &routes,
            NowledgeMemSearchRouteOwnershipPolicy::migration(),
        );

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_ownership_missing_required_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_ownership_unknown_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_ownership_duplicate_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_ownership_invalid_projection_routes".to_string()));
        assert_eq!(
            report.duplicate_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS.to_string()]
        );
        assert_eq!(
            report.unknown_routes,
            vec!["unknown_active_route".to_string()]
        );
        assert_eq!(
            report.invalid_projection_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS.to_string()]
        );
    }

    #[test]
    fn active_search_route_readiness_accepts_all_skein_ready_evidence() {
        let report = nowledge_mem_active_search_route_readiness(
            &nowledge_mem_active_search_route_read_evidence_all_skein_ready(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(report.ready);
        assert!(report.production_cutover_ready);
        assert_eq!(
            report.evidence_route_count,
            REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len()
        );
        assert_eq!(
            report.ready_route_count,
            REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len()
        );
        assert_eq!(report.lancedb_handle_required_route_count, 0);
        assert!(report.blocker_codes.is_empty());
        assert_eq!(
            report.json()["protocol"],
            NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL
        );
        assert_eq!(report.json()["lancedb_handle_required_route_count"], 0);
    }

    #[test]
    fn active_search_route_readiness_preserves_business_search_semantics() {
        let mut evidence = nowledge_mem_active_search_route_read_evidence_all_skein_ready();
        let mcp_search = evidence
            .iter_mut()
            .find(|route| route.route == NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH)
            .unwrap();
        mcp_search.embedding_identity_ready = false;
        mcp_search.zero_vector_semantics_ready = false;
        mcp_search.cjk_tokenization_ready = false;
        mcp_search.ranking_window_ready = false;
        mcp_search.fail_soft_reason_codes_ready = false;
        mcp_search.repair_rebuild_markers_ready = false;

        let report = nowledge_mem_active_search_route_readiness(
            &evidence,
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert_eq!(
            report.ready_route_count,
            REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len() - 1
        );
        assert_eq!(
            report.embedding_identity_not_ready_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH.to_string()]
        );
        assert_eq!(
            report.zero_vector_semantics_not_ready_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH.to_string()]
        );
        assert_eq!(
            report.cjk_tokenization_not_ready_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH.to_string()]
        );
        assert_eq!(
            report.ranking_window_not_ready_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH.to_string()]
        );
        assert_eq!(
            report.fail_soft_reason_codes_not_ready_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH.to_string()]
        );
        assert_eq!(
            report.repair_rebuild_markers_not_ready_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH.to_string()]
        );
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_embedding_identity_not_ready".to_string()));
        assert!(report.blocker_codes.contains(
            &"active_search_route_readiness_zero_vector_semantics_not_ready".to_string()
        ));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_cjk_tokenization_not_ready".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_ranking_window_not_ready".to_string()));
        assert!(report.blocker_codes.contains(
            &"active_search_route_readiness_fail_soft_reason_codes_not_ready".to_string()
        ));
        assert!(report.blocker_codes.contains(
            &"active_search_route_readiness_repair_rebuild_markers_not_ready".to_string()
        ));
    }

    #[test]
    fn active_search_route_readiness_blocks_lancedb_handle_and_weak_candidate_evidence() {
        let mut evidence = nowledge_mem_active_search_route_read_evidence_all_skein_ready();
        let source_chunk = evidence
            .iter_mut()
            .find(|route| route.route == NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_CHUNK_RECALL)
            .unwrap();
        source_chunk.lancedb_handle_required = true;
        source_chunk.candidate_readiness_ready = false;
        source_chunk.metadata_pushdown_ready = false;
        source_chunk.ranking_ready = false;

        let report = nowledge_mem_active_search_route_readiness(
            &evidence,
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert!(!report.production_cutover_ready);
        assert_eq!(
            report.lancedb_handle_required_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_CHUNK_RECALL.to_string()]
        );
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_lancedb_handle_required".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_candidate_not_ready".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_metadata_pushdown_not_ready".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_ranking_not_ready".to_string()));
    }

    #[test]
    fn active_search_route_readiness_fails_closed_for_inventory_and_engine_gaps() {
        let evidence = vec![
            NowledgeMemActiveSearchRouteReadEvidence::ready_skein(
                NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS,
                NOWLEDGE_MEM_SEARCH_ROUTE_MESSAGE,
            ),
            NowledgeMemActiveSearchRouteReadEvidence::new(
                NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS,
                "invalid_projection",
                NowledgeMemSearchReadEngine::LanceDb,
                true,
                false,
                true,
                true,
                false,
                true,
            ),
            NowledgeMemActiveSearchRouteReadEvidence::ready_skein(
                "unknown_active_route",
                NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY,
            ),
        ];

        let report = nowledge_mem_active_search_route_readiness(
            &evidence,
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_missing_required_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_unknown_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_duplicate_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_invalid_projection_routes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"active_search_route_readiness_non_skein_routes".to_string()));
        assert_eq!(
            report.duplicate_routes,
            vec![NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS.to_string()]
        );
        assert_eq!(
            report.unknown_routes,
            vec!["unknown_active_route".to_string()]
        );
    }
}
