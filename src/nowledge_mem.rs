use crate::search_projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_shadow_evidence_json,
};
use crate::{
    BackgroundMaintenanceOptions, BackgroundMaintenanceSummary, BackgroundWorkHint,
    BackgroundWorkPlan, Database, DatabaseConfig, KnowledgeRetrievalOutput,
    KnowledgeRetrievalRequest, LocalQosPolicy, LocalQosScheduler, LocalQosState, QueryOutput,
    Result, SearchIndex, SearchProjectionDeltaReport, SearchProjectionFreshness,
    SearchProjectionGraphDeltaRequest, SearchProjectionProbeOptions, SkeinError, Value,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphMode {
    ShadowReadOnly,
    WritableCutover,
}

pub fn nowledge_mem_graph_config(mode: NowledgeMemGraphMode) -> DatabaseConfig {
    DatabaseConfig {
        read_only: matches!(mode, NowledgeMemGraphMode::ShadowReadOnly),
        ..DatabaseConfig::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenOptions {
    pub graph_path: PathBuf,
    pub search_projection_path: Option<PathBuf>,
    pub mode: NowledgeMemGraphMode,
}

impl NowledgeMemOpenOptions {
    pub fn graph_only(graph_path: impl Into<PathBuf>, mode: NowledgeMemGraphMode) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: None,
            mode,
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
        }
    }

    pub fn sanitized_report(&self) -> NowledgeMemOpenReport {
        NowledgeMemOpenReport {
            protocol: NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL.to_string(),
            mode: self.mode,
            graph_configured: true,
            search_projection_configured: self.search_projection_path.is_some(),
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
            "graph_opened": self.graph_opened,
            "search_projection_opened": self.search_projection_opened,
        })
    }
}

pub const NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL: &str = "skein-nowledge-mem-open-report";

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

impl NowledgeMemGraph {
    pub fn open(path: impl AsRef<Path>, mode: NowledgeMemGraphMode) -> Result<Self> {
        let db = Database::open_with_config(path, nowledge_mem_graph_config(mode))?;
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

    pub fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.db.query_with_params(cypher, parameters)
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
        let graph = NowledgeMemGraph::open(&options.graph_path, options.mode)?;
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
        let search_projection = self.require_search_projection()?;
        Ok(self
            .graph
            .database()
            .retrieve_knowledge(search_projection.index(), request))
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

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_graph_config, NowledgeMemEmbeddedStore, NowledgeMemGraph,
        NowledgeMemGraphMode, NowledgeMemOpenOptions, NowledgeMemSearchProjection,
        NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
    };
    use crate::search::SearchFusionWeights;
    use crate::{
        BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundWorkHint, Database,
        KnowledgeCandidateScoringPolicy, KnowledgeRetrievalRequest, LocalQosPolicy,
        LocalQosScheduler, LocalQosState, SearchEmbeddingManifest, SearchIndex, SearchMode,
        SearchProjectionDelta, SearchProjectionKind, SearchProjectionProbeOptions,
        SearchProjectionRow, WorkClass,
    };
    use std::collections::BTreeMap;

    #[test]
    fn graph_config_tracks_shadow_vs_cutover_mode() {
        assert!(nowledge_mem_graph_config(NowledgeMemGraphMode::ShadowReadOnly).read_only);
        assert!(!nowledge_mem_graph_config(NowledgeMemGraphMode::WritableCutover).read_only);
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
                dimension: 2,
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
                active_embedding_dimension: Some(2),
            })
            .unwrap();

        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-search-projection-evidence"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["required_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        assert_eq!(evidence["incremental_update_ready"], true);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn embedded_store_exposes_search_projection_shadow_evidence() {
        let mut index = SearchIndex::in_memory();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
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
            active_embedding_dimension: Some(2),
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
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["primary_ready"], true);
        assert_eq!(evidence["shadow_ready"], true);
        assert_eq!(evidence["document_count_parity"], true);
        assert_eq!(evidence["table_parity"]["ready"], true);
        assert_eq!(evidence["embedding_identity_parity"], true);
        assert_eq!(evidence["incremental_watermark_parity"], true);
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
        assert!(report.get("graph_path").is_none());
        assert!(report.get("search_projection_path").is_none());
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

        let output = store
            .retrieve_knowledge(&KnowledgeRetrievalRequest {
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

        assert_eq!(output.search.total_hits, 1);
        assert_eq!(output.search.hits[0].id, "memory:mem-search");
        assert_eq!(output.diagnostics.projection_commit_lag, 0);
        assert!(!output.evidence.is_empty());
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
            embedding: include_embedding.then_some(vec![1.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
        }
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
