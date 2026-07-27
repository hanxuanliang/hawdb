use crate::{
    nowledge_mem_graph_augmentation_state_route_query,
    nowledge_mem_graph_community_members_route_query,
    nowledge_mem_graph_community_recent_memories_route_query,
    nowledge_mem_graph_community_subgraph_route_query, nowledge_mem_graph_node_details_route_query,
    nowledge_mem_graph_orphans_route_query, nowledge_mem_graph_overview_route_query,
    nowledge_mem_graph_pagerank_plan_route_query, nowledge_mem_graph_sample_route_query, Database,
    NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemQueryExecutionPath,
    NowledgeMemQueryReportOptions, Result, RouteQuery,
};
use std::collections::BTreeMap;

pub const NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL: &str =
    "skein-nowledge-graph-route-workload-fixture-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadFixtureOptions {
    pub limit: usize,
    pub max_entities: usize,
    pub max_edges: usize,
    pub changed_since_epoch_nanos: Option<i64>,
    pub capture_physical_plan: bool,
}

impl Default for NowledgeGraphRouteWorkloadFixtureOptions {
    fn default() -> Self {
        Self {
            limit: 8,
            max_entities: 8,
            max_edges: 16,
            changed_since_epoch_nanos: Some(100),
            capture_physical_plan: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadFixtureReport {
    pub protocol: &'static str,
    pub ready: bool,
    pub route_count: usize,
    pub query_count: usize,
    pub failed_query_count: usize,
    pub total_rows: usize,
    pub total_elapsed_micros: u128,
    pub routes: Vec<NowledgeGraphRouteWorkloadRouteReport>,
}

impl NowledgeGraphRouteWorkloadFixtureReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "route_count": self.route_count,
            "query_count": self.query_count,
            "failed_query_count": self.failed_query_count,
            "total_rows": self.total_rows,
            "total_elapsed_micros": self.total_elapsed_micros,
            "routes": self.routes.iter().map(NowledgeGraphRouteWorkloadRouteReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadRouteReport {
    pub route: String,
    pub ready: bool,
    pub query_count: usize,
    pub failed_query_count: usize,
    pub total_rows: usize,
    pub total_elapsed_micros: u128,
    pub queries: Vec<NowledgeGraphRouteWorkloadQueryReport>,
}

impl NowledgeGraphRouteWorkloadRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "route": self.route,
            "ready": self.ready,
            "query_count": self.query_count,
            "failed_query_count": self.failed_query_count,
            "total_rows": self.total_rows,
            "total_elapsed_micros": self.total_elapsed_micros,
            "queries": self.queries.iter().map(NowledgeGraphRouteWorkloadQueryReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadQueryReport {
    pub name: String,
    pub query_family: Option<String>,
    pub ready: bool,
    pub row_count: usize,
    pub elapsed_micros: u128,
    pub execution_path: Option<NowledgeMemQueryExecutionPath>,
    pub physical_plan_captured: bool,
    pub scan_pruning_report_count: usize,
    pub physical_operator_counts: BTreeMap<String, usize>,
    pub error_class: Option<String>,
}

impl NowledgeGraphRouteWorkloadQueryReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "query_family": self.query_family,
            "ready": self.ready,
            "row_count": self.row_count,
            "elapsed_micros": self.elapsed_micros,
            "execution_path": self.execution_path.map(NowledgeMemQueryExecutionPath::as_str),
            "physical_plan_captured": self.physical_plan_captured,
            "scan_pruning_report_count": self.scan_pruning_report_count,
            "physical_operator_counts": self.physical_operator_counts,
            "error_class": self.error_class,
        })
    }
}

pub fn nowledge_graph_route_workload_fixture_report(
    options: NowledgeGraphRouteWorkloadFixtureOptions,
) -> Result<NowledgeGraphRouteWorkloadFixtureReport> {
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    seed_graph_route_workload_fixture(&mut graph)?;
    let route_queries = nowledge_graph_route_workload_fixture_queries(options)?;
    run_graph_route_workload_fixture(&mut graph, &route_queries, options)
}

pub fn nowledge_graph_route_workload_fixture_queries(
    options: NowledgeGraphRouteWorkloadFixtureOptions,
) -> Result<Vec<RouteQuery>> {
    Ok(vec![
        nowledge_mem_graph_overview_route_query(options.limit)?,
        nowledge_mem_graph_sample_route_query(options.limit)?,
        nowledge_mem_graph_node_details_route_query(0)?,
        nowledge_mem_graph_community_members_route_query(42, options.limit)?,
        nowledge_mem_graph_community_recent_memories_route_query(3676, options.limit)?,
        nowledge_mem_graph_community_subgraph_route_query(
            3505,
            options.max_entities,
            ["community-subgraph-alpha", "community-subgraph-beta"],
            options.max_edges,
        )?,
        nowledge_mem_graph_augmentation_state_route_query(),
        nowledge_mem_graph_pagerank_plan_route_query(options.changed_since_epoch_nanos),
        nowledge_mem_graph_orphans_route_query(options.limit)?,
    ])
}

fn run_graph_route_workload_fixture(
    graph: &mut NowledgeMemGraph,
    route_queries: &[RouteQuery],
    options: NowledgeGraphRouteWorkloadFixtureOptions,
) -> Result<NowledgeGraphRouteWorkloadFixtureReport> {
    let query_options = NowledgeMemQueryReportOptions {
        capture_physical_plan: options.capture_physical_plan,
        slow_log_threshold_micros: None,
    };
    let routes = route_queries
        .iter()
        .map(|route| run_graph_route_workload_route(graph, route, query_options))
        .collect::<Vec<_>>();
    let query_count = routes.iter().map(|route| route.query_count).sum();
    let failed_query_count = routes.iter().map(|route| route.failed_query_count).sum();
    let total_rows = routes.iter().map(|route| route.total_rows).sum();
    let total_elapsed_micros = routes.iter().map(|route| route.total_elapsed_micros).sum();
    Ok(NowledgeGraphRouteWorkloadFixtureReport {
        protocol: NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
        ready: !routes.is_empty()
            && failed_query_count == 0
            && routes.iter().all(|route| route.ready),
        route_count: routes.len(),
        query_count,
        failed_query_count,
        total_rows,
        total_elapsed_micros,
        routes,
    })
}

fn run_graph_route_workload_route(
    graph: &mut NowledgeMemGraph,
    route: &RouteQuery,
    options: NowledgeMemQueryReportOptions,
) -> NowledgeGraphRouteWorkloadRouteReport {
    let queries = route
        .queries
        .iter()
        .map(|query| {
            match graph.query_with_params_with_report_options(
                &query.cypher,
                &query.parameters,
                options,
            ) {
                Ok(output) => NowledgeGraphRouteWorkloadQueryReport {
                    name: query.name.clone(),
                    query_family: query.query_family.clone(),
                    ready: true,
                    row_count: output.output.rows.len(),
                    elapsed_micros: output.report.elapsed_micros,
                    execution_path: Some(output.report.execution_path),
                    physical_plan_captured: output.report.physical_plan_captured,
                    scan_pruning_report_count: output.report.scan_pruning_reports.len(),
                    physical_operator_counts: output.report.physical_operator_counts,
                    error_class: None,
                },
                Err(error) => NowledgeGraphRouteWorkloadQueryReport {
                    name: query.name.clone(),
                    query_family: query.query_family.clone(),
                    ready: false,
                    row_count: 0,
                    elapsed_micros: 0,
                    execution_path: None,
                    physical_plan_captured: false,
                    scan_pruning_report_count: 0,
                    physical_operator_counts: BTreeMap::new(),
                    error_class: Some(error_class(&error)),
                },
            }
        })
        .collect::<Vec<_>>();
    let failed_query_count = queries.iter().filter(|query| !query.ready).count();
    let total_rows = queries.iter().map(|query| query.row_count).sum();
    let total_elapsed_micros = queries.iter().map(|query| query.elapsed_micros).sum();
    NowledgeGraphRouteWorkloadRouteReport {
        route: route.route.clone(),
        ready: !queries.is_empty() && failed_query_count == 0,
        query_count: queries.len(),
        failed_query_count,
        total_rows,
        total_elapsed_micros,
        queries,
    }
}

fn seed_graph_route_workload_fixture(graph: &mut NowledgeMemGraph) -> Result<()> {
    for statement in GRAPH_ROUTE_WORKLOAD_FIXTURE_STATEMENTS {
        graph.query(statement)?;
    }
    Ok(())
}

fn error_class(error: &crate::SkeinError) -> String {
    match error {
        crate::SkeinError::Parse(_) => "parse",
        crate::SkeinError::Semantic(_) => "semantic",
        crate::SkeinError::Execution(_) => "execution",
        crate::SkeinError::Storage(_) => "storage",
    }
    .to_string()
}

const GRAPH_ROUTE_WORKLOAD_FIXTURE_STATEMENTS: &[&str] = &[
    "CREATE (:GraphMeta {meta_id: 'main', community_detection_applied: true, pagerank_applied: true, community_algorithm: 'louvain', community_resolution: 1.0, community_count: 3, pagerank_algorithm: 'pagerank', pagerank_damping: 0.85, pagerank_iterations: 20, last_augmentation_at: 1000, schema_version: 2, community_detection_computed_at: 900, pagerank_computed_at: 950})",
    "CREATE (:Memory {id: 'overview-memory-1', title: 'Overview One', content: 'body one', pagerank_score: 3.0, importance: 0.1, community_id: 7001, space_id: 'default', created_at: 101, updated_at: 201, source: 'overview', event_start: 301, event_end: 401})",
    "CREATE (:Memory {id: 'overview-memory-2', content: 'Fallback body', importance: 2.0, community_id: 7002, space_id: 'default', created_at: 102, updated_at: 202, source: 'overview', event_start: 302, event_end: 402})",
    "CREATE (:Memory {id: 'sample-memory-a', title: 'Sample A', content: 'sample body A', pagerank_score: 1.0, importance: 0.1, community_id: 8001, space_id: 'default', created_at: 101, updated_at: 201, source: 'sample'})",
    "CREATE (:Memory {id: 'sample-memory-b', content: 'Sample body B', importance: 2.0, community_id: 8002, space_id: 'default', created_at: 102, updated_at: 202, source: 'sample'})",
    "CREATE (:Memory {id: 'community-memory-high', title: 'Community High', content: 'high body', pagerank_score: 3.0, importance: 0.1, community_id: 42, space_id: 'default', created_at: 101, updated_at: 201, source: 'community'})",
    "CREATE (:Memory {id: 'community-memory-low', title: 'Community Low', content: 'low body', importance: 1.0, community_id: 42, space_id: 'default', created_at: 102, updated_at: 202, source: 'community'})",
    "CREATE (:Memory {id: 'community-recent-old', title: 'Recent Old', content: 'old body', importance: 0.4, created_at: 10, updated_at: 20, is_crystal: false})",
    "CREATE (:Memory {id: 'community-recent-new', title: 'Recent New', content: 'new body', importance: 0.9, created_at: 30, updated_at: 40, is_crystal: false})",
    "CREATE (:Entity {id: 'community-recent-entity-a', community_id: 3676})",
    "CREATE (:Entity {id: 'community-recent-entity-b', community_id: 3676})",
    "MATCH (m:Memory {id: 'community-recent-old'}), (e:Entity {id: 'community-recent-entity-a'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-recent-new'}), (e:Entity {id: 'community-recent-entity-a'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-recent-new'}), (e:Entity {id: 'community-recent-entity-b'}) CREATE (m)-[:MENTIONS]->(e)",
    "CREATE (:Entity {id: 'community-subgraph-alpha', name: 'Alpha Entity', entity_type: 'concept', community_id: 3505, confidence: 0.9})",
    "CREATE (:Entity {id: 'community-subgraph-beta', name: 'Beta Entity', entity_type: 'concept', community_id: 3505, confidence: 0.7})",
    "CREATE (:Entity {id: 'community-subgraph-outside', name: 'Outside Entity', entity_type: 'concept', community_id: 9999, confidence: 1.0})",
    "CREATE (:Memory {id: 'community-subgraph-memory-a', created_at: 10, updated_at: 20})",
    "CREATE (:Memory {id: 'community-subgraph-memory-b', created_at: 120, updated_at: 130})",
    "MATCH (m:Memory {id: 'community-subgraph-memory-a'}), (e:Entity {id: 'community-subgraph-alpha'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-subgraph-memory-b'}), (e:Entity {id: 'community-subgraph-alpha'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-subgraph-memory-a'}), (e:Entity {id: 'community-subgraph-beta'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (a:Entity {id: 'community-subgraph-alpha'}), (b:Entity {id: 'community-subgraph-beta'}) CREATE (a)-[:RELATES_TO {confidence: 0.77, relation_type: 'related', created_at: 170}]->(b)",
    "CREATE (:Memory {id: 'pagerank-plan-m1', created_at: 10, updated_at: 20})",
    "CREATE (:Memory {id: 'pagerank-plan-m2', created_at: 120, updated_at: 130})",
    "CREATE (:Entity {id: 'pagerank-plan-e1', name: 'Entity One', created_at: 15, updated_at: 25})",
    "CREATE (:Entity {id: 'pagerank-plan-e2', name: 'Entity Two', created_at: 140, updated_at: 150})",
    "MATCH (m:Memory {id: 'pagerank-plan-m1'}), (e:Entity {id: 'pagerank-plan-e1'}) CREATE (m)-[:MENTIONS {created_at: 30}]->(e)",
    "MATCH (m:Memory {id: 'pagerank-plan-m2'}), (e:Entity {id: 'pagerank-plan-e2'}) CREATE (m)-[:MENTIONS {created_at: 160}]->(e)",
    "MATCH (a:Entity {id: 'pagerank-plan-e1'}), (b:Entity {id: 'pagerank-plan-e2'}) CREATE (a)-[:RELATES_TO {created_at: 170}]->(b)",
    "MATCH (a:Memory {id: 'pagerank-plan-m1'}), (b:Memory {id: 'pagerank-plan-m2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 180}]->(b)",
    "MATCH (a:Memory {id: 'pagerank-plan-m2'}), (b:Memory {id: 'pagerank-plan-m1'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'inactive', created_at: 190}]->(b)",
    "CREATE (:Entity {id: 'orphan-entity', name: 'Orphan Entity', entity_type: 'concept', description: 'orphan'})",
    "CREATE (:Entity {id: 'mentioned-entity', name: 'Mentioned Entity', entity_type: 'concept'})",
    "CREATE (:Memory {id: 'orphan-blocking-memory', title: 'Blocking Memory'})",
    "MATCH (m:Memory {id: 'orphan-blocking-memory'}), (e:Entity {id: 'mentioned-entity'}) CREATE (m)-[:MENTIONS]->(e)",
];

#[cfg(test)]
mod tests {
    use super::{
        nowledge_graph_route_workload_fixture_queries,
        nowledge_graph_route_workload_fixture_report, NowledgeGraphRouteWorkloadFixtureOptions,
        NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
    };

    #[test]
    fn graph_route_workload_fixture_runs_real_route_queries() {
        let report = nowledge_graph_route_workload_fixture_report(
            NowledgeGraphRouteWorkloadFixtureOptions::default(),
        )
        .unwrap();

        assert_eq!(
            report.protocol,
            NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL
        );
        assert!(report.ready);
        assert_eq!(report.route_count, 9);
        assert_eq!(report.failed_query_count, 0);
        assert!(report.query_count >= report.route_count);
        assert!(report.total_rows > 0);
        assert!(report.routes.iter().all(|route| route.ready));
        assert!(report
            .routes
            .iter()
            .flat_map(|route| route.queries.iter())
            .all(|query| query.physical_plan_captured));
        assert!(report
            .routes
            .iter()
            .flat_map(|route| route.queries.iter())
            .any(|query| query.scan_pruning_report_count > 0));
    }

    #[test]
    fn graph_route_workload_fixture_query_catalog_is_stable() {
        let queries = nowledge_graph_route_workload_fixture_queries(
            NowledgeGraphRouteWorkloadFixtureOptions::default(),
        )
        .unwrap();
        let routes = queries
            .iter()
            .map(|query| query.route.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            routes,
            vec![
                "/graph/overview",
                "/graph/sample",
                "/graph/node-details/{node_id}",
                "/graph/community-members/{community_id}",
                "/library/community/{community_id}/recent-memories",
                "/library/community/{community_id}/subgraph",
                "/graph/augmentation/state",
                "/graph/augmentation/pagerank/plan",
                "/graph/orphans",
            ]
        );
    }
}
