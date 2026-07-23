use skein::{
    Result, SkeinError, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use std::collections::BTreeSet;
use std::path::Path;

const NMEM_GRAPH_ROUTE_READINESS_PROTOCOL: &str = "nmem-graph-route-readiness-v1";

pub fn nowledge_graph_route_readiness_usage() -> String {
    "nowledge-graph-route-readiness requires [--require-ready] <route-evidence-json>".to_string()
}

pub fn run_nowledge_graph_route_readiness(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut evidence_path = None;
    for arg in args {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
            }
            path => {
                if evidence_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
                }
            }
        }
    }
    let Some(evidence_path) = evidence_path else {
        return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
    };
    Ok((
        nowledge_graph_route_readiness_json(&read_json_file(Path::new(&evidence_path))?)?,
        require_ready,
    ))
}

fn nowledge_graph_route_readiness_json(evidence: &serde_json::Value) -> Result<serde_json::Value> {
    let routes = parse_route_evidence(evidence)?;
    let route_names = routes
        .iter()
        .map(|route| route.route.clone())
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| !route_names.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let route_primary_blocker_codes =
        route_primary_blocker_codes(&routes, &missing_required_routes);
    let shadow_compare_route_count = routes
        .iter()
        .filter(|route| route.shadow_compare_ready)
        .count();
    let primary_ready_route_count = routes.iter().filter(|route| route.primary_ready).count();
    let query_runtime_ready_route_names = routes
        .iter()
        .filter(|route| route.query_runtime_ready())
        .map(|route| route.route.as_str())
        .collect::<BTreeSet<_>>();
    let missing_query_runtime_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| !query_runtime_ready_route_names.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let query_runtime_route_count =
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - missing_query_runtime_routes.len();
    let query_runtime_report_count = routes
        .iter()
        .map(|route| route.query_reports.len())
        .sum::<usize>();
    let route_primary_ready =
        missing_required_routes.is_empty() && route_primary_blocker_codes.is_empty();
    let route_count = routes.len();

    Ok(serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
        "route_count": route_count,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "missing_required_routes": missing_required_routes,
        "shadow_compare_route_count": shadow_compare_route_count,
        "primary_ready_route_count": primary_ready_route_count,
        "query_runtime_route_count": query_runtime_route_count,
        "query_runtime_report_count": query_runtime_report_count,
        "missing_query_runtime_routes": missing_query_runtime_routes,
        "route_query_runtime_ready": missing_query_runtime_routes.is_empty(),
        "route_primary_ready": route_primary_ready,
        "route_primary_blocker_codes": route_primary_blocker_codes,
        "routes": routes.into_iter().map(RouteEvidence::json).collect::<Vec<_>>(),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteEvidence {
    route: String,
    shadow_compare_ready: bool,
    primary_ready: bool,
    blocker_codes: Vec<String>,
    query_reports: Vec<QueryRuntimeReport>,
}

impl RouteEvidence {
    fn query_runtime_ready(&self) -> bool {
        !self.query_reports.is_empty() && self.query_reports.iter().all(QueryRuntimeReport::ready)
    }

    fn json(self) -> serde_json::Value {
        let query_runtime_ready = self.query_runtime_ready();
        let query_report_count = self.query_reports.len();
        let query_reports = self
            .query_reports
            .into_iter()
            .map(QueryRuntimeReport::json)
            .collect::<Vec<_>>();
        serde_json::json!({
            "route": self.route,
            "shadow_compare_ready": self.shadow_compare_ready,
            "primary_ready": self.primary_ready,
            "query_runtime_ready": query_runtime_ready,
            "query_report_count": query_report_count,
            "query_reports": query_reports,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryRuntimeReport {
    protocol: Option<String>,
    statement_kind: Option<String>,
    execution_path: Option<String>,
    fast_path_selected: Option<bool>,
    slow_log_candidate: Option<bool>,
    physical_plan_captured: Option<bool>,
    plan_cache_lookup: Option<String>,
    plan_cache_cacheable: Option<bool>,
    plan_cache_hit: Option<bool>,
    plan_cache_miss: Option<bool>,
    plan_cache_bypassed: Option<bool>,
    blocker_codes: Vec<String>,
}

impl QueryRuntimeReport {
    fn parse(value: &serde_json::Value) -> Self {
        let plan_cache_lookup = str_path(value, &["plan_cache", "lookup"])
            .or_else(|| str_path(value, &["plan_cache_lookup"]))
            .map(str::to_string);
        let plan_cache_cacheable = bool_path(value, &["plan_cache", "cacheable"])
            .or_else(|| bool_path(value, &["plan_cache_cacheable"]));
        let plan_cache_hit = bool_path(value, &["plan_cache", "hit"])
            .or_else(|| bool_path(value, &["plan_cache_hit"]));
        let plan_cache_miss = bool_path(value, &["plan_cache", "miss"])
            .or_else(|| bool_path(value, &["plan_cache_miss"]));
        let plan_cache_bypassed = bool_path(value, &["plan_cache", "bypassed"])
            .or_else(|| bool_path(value, &["plan_cache_bypassed"]));
        let mut report = Self {
            protocol: str_path(value, &["protocol"]).map(str::to_string),
            statement_kind: str_path(value, &["statement_kind"]).map(str::to_string),
            execution_path: str_path(value, &["execution_path"]).map(str::to_string),
            fast_path_selected: bool_path(value, &["fast_path_selected"]),
            slow_log_candidate: bool_path(value, &["slow_log_candidate"]),
            physical_plan_captured: bool_path(value, &["physical_plan_captured"]),
            plan_cache_lookup,
            plan_cache_cacheable,
            plan_cache_hit,
            plan_cache_miss,
            plan_cache_bypassed,
            blocker_codes: Vec::new(),
        };
        report.blocker_codes = report.computed_blocker_codes();
        report
    }

    fn ready(&self) -> bool {
        self.blocker_codes.is_empty()
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "statement_kind": self.statement_kind,
            "execution_path": self.execution_path,
            "fast_path_selected": self.fast_path_selected,
            "slow_log_candidate": self.slow_log_candidate,
            "physical_plan_captured": self.physical_plan_captured,
            "plan_cache_lookup": self.plan_cache_lookup.clone(),
            "plan_cache": {
                "lookup": self.plan_cache_lookup,
                "cacheable": self.plan_cache_cacheable,
                "hit": self.plan_cache_hit,
                "miss": self.plan_cache_miss,
                "bypassed": self.plan_cache_bypassed,
            },
            "ready": self.ready(),
            "blocker_codes": self.blocker_codes,
        })
    }

    fn computed_blocker_codes(&self) -> Vec<String> {
        let mut blockers = BTreeSet::new();
        if self.protocol.as_deref() != Some(NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL) {
            blockers.insert("query_report_protocol_mismatch".to_string());
        }
        if !self
            .statement_kind
            .as_deref()
            .is_some_and(is_nowledge_graph_read_statement_kind)
        {
            blockers.insert("query_report_not_graph_read".to_string());
        }
        if !matches!(
            self.execution_path.as_deref(),
            Some("fast_path" | "optimized_path")
        ) {
            blockers.insert("query_report_execution_path_missing".to_string());
        }
        if self.fast_path_selected.is_none() {
            blockers.insert("query_report_fast_path_selected_missing".to_string());
        }
        if self.slow_log_candidate.is_none() {
            blockers.insert("query_report_slow_log_candidate_missing".to_string());
        }
        if self.physical_plan_captured.is_none() {
            blockers.insert("query_report_physical_plan_flag_missing".to_string());
        }
        if self.plan_cache_cacheable.is_none()
            || self.plan_cache_hit.is_none()
            || self.plan_cache_miss.is_none()
            || self.plan_cache_bypassed.is_none()
        {
            blockers.insert("query_report_plan_cache_state_missing".to_string());
        }
        if matches!(self.plan_cache_lookup.as_deref(), Some("bypass"))
            || self.plan_cache_bypassed == Some(true)
        {
            blockers.insert("query_report_plan_cache_bypassed".to_string());
        }
        blockers.into_iter().collect()
    }
}

fn parse_route_evidence(evidence: &serde_json::Value) -> Result<Vec<RouteEvidence>> {
    let routes = if evidence.is_array() {
        evidence.as_array()
    } else {
        evidence.get("routes").and_then(serde_json::Value::as_array)
    }
    .ok_or_else(|| {
        SkeinError::Semantic("graph route evidence JSON must contain a routes array".to_string())
    })?;
    routes.iter().map(parse_route).collect()
}

fn parse_route(value: &serde_json::Value) -> Result<RouteEvidence> {
    let route = str_path(value, &["route"])
        .filter(|route| !route.trim().is_empty())
        .ok_or_else(|| SkeinError::Semantic("graph route evidence route is required".to_string()))?
        .to_string();
    Ok(RouteEvidence {
        route,
        shadow_compare_ready: bool_path(value, &["shadow_compare_ready"]) == Some(true),
        primary_ready: bool_path(value, &["primary_ready"]) == Some(true),
        blocker_codes: string_array_path(value, &["blocker_codes"]),
        query_reports: value_path(value, &["query_reports"])
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .map(QueryRuntimeReport::parse)
            .collect(),
    })
}

fn route_primary_blocker_codes(
    routes: &[RouteEvidence],
    missing_required_routes: &[&str],
) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    if !missing_required_routes.is_empty() {
        blockers.insert("missing_required_routes".to_string());
    }
    for route in routes {
        if !route.shadow_compare_ready {
            blockers.insert("route_shadow_compare_not_ready".to_string());
        }
        if !route.primary_ready {
            blockers.insert("route_primary_not_ready".to_string());
        }
        if route.query_reports.is_empty() {
            blockers.insert("missing_query_runtime_reports".to_string());
        }
        if !route.query_runtime_ready() {
            blockers.insert("route_query_runtime_not_ready".to_string());
        }
        for report in &route.query_reports {
            blockers.extend(report.blocker_codes.iter().cloned());
        }
        blockers.extend(route.blocker_codes.iter().cloned());
    }
    blockers.into_iter().collect()
}

fn is_nowledge_graph_read_statement_kind(kind: &str) -> bool {
    matches!(
        kind,
        "match_return"
            | "match_nodes_return"
            | "shortest_path_return"
            | "match_optional_relationship_count_sum"
            | "match_thread_repair_stats"
            | "graph_algorithm"
            | "project_graph"
    )
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read graph route readiness evidence: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse graph route readiness evidence: {error}"
        ))
    })
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    value_path(value, path).and_then(serde_json::Value::as_bool)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    value_path(value, path).and_then(serde_json::Value::as_str)
}

fn string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn value_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::nowledge_graph_route_readiness_json;
    use skein::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES;

    #[test]
    fn route_readiness_reports_ready_for_all_required_routes() {
        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": ready_routes()
        }))
        .unwrap();

        assert_eq!(readiness["protocol"], "nmem-graph-route-readiness-v1");
        assert_eq!(
            readiness["route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(readiness["route_primary_ready"], true);
        assert_eq!(readiness["route_query_runtime_ready"], true);
        assert_eq!(
            readiness["query_runtime_route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(
            readiness["route_primary_blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(readiness["missing_required_routes"], serde_json::json!([]));
    }

    #[test]
    fn route_readiness_fails_closed_for_missing_required_route() {
        let mut routes = ready_routes();
        routes.pop();

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(
            readiness["route_primary_blocker_codes"],
            serde_json::json!(["missing_required_routes"])
        );
        assert_eq!(
            readiness["missing_required_routes"],
            serde_json::json!(["/graph/shortest-path"])
        );
    }

    #[test]
    fn route_readiness_fails_closed_for_unready_route() {
        let mut routes = ready_routes();
        routes[0]["primary_ready"] = serde_json::json!(false);
        routes[0]["blocker_codes"] = serde_json::json!(["primary_route_disabled"]);

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(
            readiness["route_primary_blocker_codes"],
            serde_json::json!(["primary_route_disabled", "route_primary_not_ready"])
        );
    }

    #[test]
    fn route_readiness_fails_closed_without_query_runtime_reports() {
        let mut routes = ready_routes();
        routes[0]["query_reports"] = serde_json::json!([]);

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(readiness["route_query_runtime_ready"], false);
        assert!(readiness["route_primary_blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "missing_query_runtime_reports"));
    }

    #[test]
    fn route_readiness_fails_closed_for_non_graph_query_report() {
        let mut routes = ready_routes();
        routes[0]["query_reports"][0]["statement_kind"] = serde_json::json!("set_system_variable");

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(readiness["route_query_runtime_ready"], false);
        assert!(readiness["route_primary_blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_report_not_graph_read"));
    }

    fn ready_routes() -> Vec<serde_json::Value> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "route": route,
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "query_reports": [ready_query_report()],
                    "blocker_codes": []
                })
            })
            .collect()
    }

    fn ready_query_report() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-query-report-v1",
            "statement_kind": "match_return",
            "execution_path": "fast_path",
            "fast_path_selected": true,
            "slow_log_candidate": false,
            "physical_plan_captured": false,
            "plan_cache": {
                "lookup": "miss",
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            }
        })
    }
}
