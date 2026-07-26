use skein::{
    Database, DatabaseConfig, Result, SkeinError, Value, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str = "skein-nowledge-query-runtime-preflight-v1";

pub fn nowledge_query_runtime_preflight_usage() -> String {
    "nowledge-query-runtime-preflight requires [--require-ready] --probe-json <path> <database-path>; probe JSON may be a probes array or graph route query inventory"
        .to_string()
}

#[derive(Debug, Clone)]
struct QueryRuntimeProbe {
    name: String,
    route: Option<String>,
    query_family: Option<String>,
    cypher: String,
    parameters: BTreeMap<String, Value>,
    require_scan_pruning: bool,
    require_pruned: bool,
    min_scan_pruning_reports: usize,
    max_output_rows: Option<usize>,
}

pub fn run_nowledge_query_runtime_preflight(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut probe_path = None;
    let mut database_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--probe-json" => {
                probe_path = Some(args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_query_runtime_preflight_usage())
                })?);
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(
                    nowledge_query_runtime_preflight_usage(),
                ));
            }
            path => {
                if database_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_query_runtime_preflight_usage(),
                    ));
                }
            }
        }
    }
    let probe_path =
        probe_path.ok_or_else(|| SkeinError::Semantic(nowledge_query_runtime_preflight_usage()))?;
    let database_path = database_path
        .ok_or_else(|| SkeinError::Semantic(nowledge_query_runtime_preflight_usage()))?;
    let probes = parse_probe_file(&read_json_file(Path::new(&probe_path))?)?;
    let report = query_runtime_preflight_json(&database_path, &probes);
    Ok((report, require_ready))
}

fn query_runtime_preflight_json(
    database_path: &str,
    probes: &[QueryRuntimeProbe],
) -> serde_json::Value {
    let route_coverage = query_runtime_route_coverage(probes);
    let mut db = match Database::open_with_config(
        database_path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    ) {
        Ok(db) => db,
        Err(error) => {
            return serde_json::json!({
                "protocol": QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
                "ready": false,
                "database_opened": false,
                "probe_count": probes.len(),
                "passed_probe_count": 0,
                "failed_probe_count": probes.len(),
                "required_route_count": route_coverage.required_route_count,
                "covered_route_count": route_coverage.covered_route_count,
                "covered_routes": route_coverage.covered_routes,
                "missing_required_routes": route_coverage.missing_required_routes,
                "required_routes_covered": route_coverage.required_routes_covered,
                "unknown_routes": route_coverage.unknown_routes,
                "duplicate_routes": route_coverage.duplicate_routes,
                "route_coverage_ready": route_coverage.ready,
                "route_coverage_blocker_codes": route_coverage.blocker_codes,
                "blocker_codes": database_open_blocker_codes(
                    probes.len(),
                    probes.len(),
                    &route_coverage,
                ),
                "error_class": error_class(&error),
                "probes": [],
            });
        }
    };

    let probe_reports = probes
        .iter()
        .map(|probe| run_probe(&mut db, probe))
        .collect::<Vec<_>>();
    let passed_probe_count = probe_reports
        .iter()
        .filter(|probe| probe.get("ready").and_then(serde_json::Value::as_bool) == Some(true))
        .count();
    let failed_probe_count = probe_reports.len().saturating_sub(passed_probe_count);
    let blocker_codes = preflight_blocker_codes(probes.len(), failed_probe_count, &route_coverage);

    serde_json::json!({
        "protocol": QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
        "ready": blocker_codes.is_empty(),
        "database_opened": true,
        "probe_count": probes.len(),
        "passed_probe_count": passed_probe_count,
        "failed_probe_count": failed_probe_count,
        "required_route_count": route_coverage.required_route_count,
        "covered_route_count": route_coverage.covered_route_count,
        "covered_routes": route_coverage.covered_routes,
        "missing_required_routes": route_coverage.missing_required_routes,
        "required_routes_covered": route_coverage.required_routes_covered,
        "unknown_routes": route_coverage.unknown_routes,
        "duplicate_routes": route_coverage.duplicate_routes,
        "route_coverage_ready": route_coverage.ready,
        "route_coverage_blocker_codes": route_coverage.blocker_codes,
        "blocker_codes": blocker_codes,
        "probes": probe_reports,
    })
}

#[derive(Debug)]
struct QueryRuntimeRouteCoverage {
    required_route_count: usize,
    covered_route_count: usize,
    covered_routes: Vec<&'static str>,
    missing_required_routes: Vec<&'static str>,
    required_routes_covered: bool,
    unknown_routes: Vec<String>,
    duplicate_routes: Vec<String>,
    ready: bool,
    blocker_codes: Vec<&'static str>,
}

fn query_runtime_route_coverage(probes: &[QueryRuntimeProbe]) -> QueryRuntimeRouteCoverage {
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
        .collect::<Vec<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !observed_routes.contains(route))
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
        blocker_codes.push("query_runtime_route_coverage_missing");
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("query_runtime_unknown_routes");
    }
    let ready = blocker_codes.is_empty();
    QueryRuntimeRouteCoverage {
        required_route_count: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        covered_route_count: covered_routes.len(),
        covered_routes,
        required_routes_covered,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        ready,
        blocker_codes,
    }
}

fn run_probe(db: &mut Database, probe: &QueryRuntimeProbe) -> serde_json::Value {
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
            let blocker_codes = probe_blocker_codes(
                probe,
                scan_pruning_report_count,
                pruned_scan_count,
                output_row_count,
            );
            serde_json::json!({
                "name": probe.name,
                "route": probe.route,
                "query_family": probe.query_family,
                "ready": blocker_codes.is_empty(),
                "success": true,
                "output_row_count": output_row_count,
                "selected_plan_fingerprint": output.trace.selected_plan_fingerprint,
                "search_mode": output.trace.search_mode.as_str(),
                "selected_plan_operator_counts": output.trace.selected_plan_operator_counts,
                "selected_plan_class_counts": output.trace.selected_plan_class_counts,
                "optimizer_decision_count": output.trace.decisions.len(),
                "plan_cache_lookup": output.plan_cache_lookup.as_str(),
                "plan_cache": {
                    "lookup": output.plan_cache_lookup.as_str(),
                    "bypass_reason": output.plan_cache_lookup.bypass_reason().map(|reason| reason.as_str()),
                    "cacheable": output.plan_cache_lookup.bypass_reason().is_none(),
                    "hit": matches!(output.plan_cache_lookup, skein::PlanCacheLookup::Hit),
                    "miss": matches!(output.plan_cache_lookup, skein::PlanCacheLookup::Miss),
                    "bypassed": matches!(output.plan_cache_lookup, skein::PlanCacheLookup::Bypass(_)),
                },
                "work_request": {
                    "priority": output.work_request.priority.as_str(),
                    "class": output.work_request.class.as_str(),
                    "estimated_operations": output.work_request.estimated_operations,
                },
                "execution_profile": {
                    "max_rows": output.execution_profile.max_rows,
                    "detection_row_cap": output.execution_profile.detection_row_cap,
                    "row_limit_enforced_before_output": output.execution_profile.row_limit_enforced_before_output,
                    "operator_row_cap_enabled": output.execution_profile.operator_row_cap_enabled,
                    "blocking_operator_kinds": output.execution_profile.blocking_operator_kinds,
                    "scan_pruning_report_count": scan_pruning_report_count,
                    "pruned_scan_count": pruned_scan_count,
                    "scan_pruning_reports": output.execution_profile.scan_pruning_reports
                        .iter()
                        .map(scan_pruning_report_json)
                        .collect::<Vec<_>>(),
                },
                "blocker_codes": blocker_codes,
            })
        }
        Err(error) => serde_json::json!({
            "name": probe.name,
            "route": probe.route,
            "query_family": probe.query_family,
            "ready": false,
            "success": false,
            "error_class": error_class(&error),
            "blocker_codes": ["query_runtime_failed"],
        }),
    }
}

fn preflight_blocker_codes(
    probe_count: usize,
    failed_probe_count: usize,
    route_coverage: &QueryRuntimeRouteCoverage,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if probe_count == 0 {
        blockers.push("query_runtime_probes_missing");
    }
    if failed_probe_count > 0 {
        blockers.push("query_runtime_probe_failed");
    }
    blockers.extend(route_coverage.blocker_codes.iter().copied());
    blockers
}

fn database_open_blocker_codes(
    probe_count: usize,
    failed_probe_count: usize,
    route_coverage: &QueryRuntimeRouteCoverage,
) -> Vec<&'static str> {
    let mut blockers = vec!["database_open_failed"];
    blockers.extend(preflight_blocker_codes(
        probe_count,
        failed_probe_count,
        route_coverage,
    ));
    blockers
}

fn probe_blocker_codes(
    probe: &QueryRuntimeProbe,
    scan_pruning_report_count: usize,
    pruned_scan_count: usize,
    output_row_count: usize,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    blockers.extend(query_runtime_probe_identity_blocker_codes(probe));
    if probe.require_scan_pruning && scan_pruning_report_count < probe.min_scan_pruning_reports {
        blockers.push("scan_pruning_report_missing");
    }
    if probe.require_pruned && pruned_scan_count == 0 {
        blockers.push("scan_pruning_not_pruned");
    }
    if let Some(max_output_rows) = probe.max_output_rows {
        if output_row_count > max_output_rows {
            blockers.push("output_row_count_exceeded");
        }
    }
    blockers
}

fn query_runtime_probe_identity_blocker_codes(probe: &QueryRuntimeProbe) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if probe.name.trim().is_empty() || probe.name == "unnamed" {
        blockers.push("query_runtime_probe_name_missing");
    }
    match probe.route.as_deref() {
        Some(route) if REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route) => {}
        Some(_) => blockers.push("query_runtime_probe_unknown_route"),
        None => blockers.push("query_runtime_probe_route_missing"),
    }
    match probe.query_family.as_deref() {
        Some(family) if REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.contains(&family) => {}
        Some(_) => blockers.push("query_runtime_probe_unknown_query_family"),
        None => blockers.push("query_runtime_probe_query_family_missing"),
    }
    blockers
}

fn scan_pruning_report_json(report: &skein::store::ScanPruningReport) -> serde_json::Value {
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

fn scan_pruning_strategy_json(strategy: &skein::store::ScanPruningStrategy) -> serde_json::Value {
    match strategy {
        skein::store::ScanPruningStrategy::FullLabelScan => {
            serde_json::json!({"kind": "full_label_scan"})
        }
        skein::store::ScanPruningStrategy::Empty => serde_json::json!({"kind": "empty"}),
        skein::store::ScanPruningStrategy::IdEq => serde_json::json!({"kind": "id_eq"}),
        skein::store::ScanPruningStrategy::IdIn => serde_json::json!({"kind": "id_in"}),
        skein::store::ScanPruningStrategy::IdRange => serde_json::json!({"kind": "id_range"}),
        skein::store::ScanPruningStrategy::PropertyEq { property } => {
            serde_json::json!({"kind": "property_eq", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyNotEq { property } => {
            serde_json::json!({"kind": "property_not_eq", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        skein::store::ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        skein::store::ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
    }
}

fn parse_probe_file(value: &serde_json::Value) -> Result<Vec<QueryRuntimeProbe>> {
    if let Some(array) = value.as_array() {
        return array.iter().map(parse_probe).collect();
    }
    if let Some(array) = value.get("probes").and_then(serde_json::Value::as_array) {
        return array.iter().map(parse_probe).collect();
    }
    if let Some(array) = value.get("routes").and_then(serde_json::Value::as_array) {
        return parse_route_query_inventory_probes(array);
    }
    Ok(vec![parse_probe(value)?])
}

fn parse_route_query_inventory_probes(
    routes: &[serde_json::Value],
) -> Result<Vec<QueryRuntimeProbe>> {
    routes
        .iter()
        .flat_map(|route| match parse_route_query_probes(route) {
            Ok(probes) => probes.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(error) => vec![Err(error)],
        })
        .collect()
}

fn parse_route_query_probes(value: &serde_json::Value) -> Result<Vec<QueryRuntimeProbe>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("graph route query inventory route must be a JSON object".to_string())
    })?;
    let route = object
        .get("route")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "graph route query inventory field 'route' must be a string".to_string(),
            )
        })?
        .to_string();
    if route.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query inventory route must be non-empty".to_string(),
        ));
    }
    let queries = object
        .get("queries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "graph route query inventory field 'queries' must be an array".to_string(),
            )
        })?;
    queries
        .iter()
        .enumerate()
        .map(|(query_index, query)| parse_route_query_probe(&route, query, query_index))
        .collect()
}

fn parse_route_query_probe(
    route: &str,
    value: &serde_json::Value,
    query_index: usize,
) -> Result<QueryRuntimeProbe> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("graph route query inventory query must be a JSON object".to_string())
    })?;
    let name =
        optional_query_name(value).unwrap_or_else(|| format!("{route}:query-{}", query_index + 1));
    if name.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query inventory query name must be non-empty when provided".to_string(),
        ));
    }
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "graph route query inventory field 'cypher' must be a string".to_string(),
            )
        })?
        .to_string();
    if cypher.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query inventory field 'cypher' must be non-empty".to_string(),
        ));
    }
    let parameters = object
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    let min_scan_pruning_reports = optional_usize(object, "min_scan_pruning_reports")?.unwrap_or(1);
    Ok(QueryRuntimeProbe {
        name,
        route: Some(route.to_string()),
        query_family: optional_string(object, "query_family")?,
        cypher,
        parameters,
        require_scan_pruning: optional_bool(object, "require_scan_pruning")?.unwrap_or(false),
        require_pruned: optional_bool(object, "require_pruned")?.unwrap_or(false),
        min_scan_pruning_reports,
        max_output_rows: optional_usize(object, "max_output_rows")?,
    })
}

fn parse_probe(value: &serde_json::Value) -> Result<QueryRuntimeProbe> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("query runtime probe must be a JSON object".to_string())
    })?;
    let name = object
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unnamed")
        .to_string();
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic("query runtime probe field 'cypher' must be a string".to_string())
        })?
        .to_string();
    let parameters = object
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    let min_scan_pruning_reports = optional_usize(object, "min_scan_pruning_reports")?.unwrap_or(1);
    Ok(QueryRuntimeProbe {
        name,
        route: optional_string(object, "route")?,
        query_family: optional_string(object, "query_family")?,
        cypher,
        parameters,
        require_scan_pruning: optional_bool(object, "require_scan_pruning")?.unwrap_or(false),
        require_pruned: optional_bool(object, "require_pruned")?.unwrap_or(false),
        min_scan_pruning_reports,
        max_output_rows: optional_usize(object, "max_output_rows")?,
    })
}

fn optional_query_name(value: &serde_json::Value) -> Option<String> {
    ["name", "query_id", "id"]
        .iter()
        .find_map(|field| value.get(*field).and_then(serde_json::Value::as_str))
        .map(str::to_string)
}

fn parse_parameters_json(value: &serde_json::Value) -> Result<BTreeMap<String, Value>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("query runtime probe field 'parameters' must be an object".to_string())
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
        .collect()
}

fn value_from_json(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Semantic(
                    "unsupported JSON number in query runtime probe parameters".to_string(),
                ))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

fn optional_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<String>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "query runtime probe field '{field}' must be a string"
            ))
        })
}

fn optional_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<bool>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_bool().map(Some).ok_or_else(|| {
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' must be a boolean"
        ))
    })
}

fn optional_usize(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<usize>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let raw = value.as_u64().ok_or_else(|| {
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' must be an integer"
        ))
    })?;
    usize::try_from(raw).map(Some).map_err(|_| {
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' exceeds usize range"
        ))
    })
}

fn error_class(error: &SkeinError) -> &'static str {
    match error {
        SkeinError::Parse(_) => "parse",
        SkeinError::Semantic(_) => "semantic",
        SkeinError::Storage(_) => "storage",
        SkeinError::Execution(_) => "execution",
    }
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read query runtime preflight JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse query runtime preflight JSON: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_query_runtime_preflight;
    use skein::{Database, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn query_runtime_preflight_accepts_pruned_probe() {
        let root = unique_test_dir("query-runtime-preflight");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-b', kind: 'task', title: 'B'})")
            .unwrap();
        drop(db);
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "name": format!("memory-by-kind:{route}"),
                    "route": route,
                    "query_family": "memory_lookup",
                    "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
                    "parameters": {"kind": "note"},
                    "require_scan_pruning": true,
                    "require_pruned": true,
                    "max_output_rows": 1
                })
            })
            .collect::<Vec<_>>();
        std::fs::write(
            &probe_path,
            serde_json::json!({ "probes": probes }).to_string(),
        )
        .unwrap();

        let (report, require_ready) = run_nowledge_query_runtime_preflight(
            [
                "--require-ready",
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            report["protocol"],
            "skein-nowledge-query-runtime-preflight-v1"
        );
        assert_eq!(report["ready"], true);
        assert_eq!(
            report["probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["passed_probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["required_route_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["covered_route_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(report["required_routes_covered"], true);
        assert_eq!(report["missing_required_routes"], serde_json::json!([]));
        assert_eq!(report["unknown_routes"], serde_json::json!([]));
        assert_eq!(report["duplicate_routes"], serde_json::json!([]));
        assert_eq!(report["route_coverage_ready"], true);
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(report["probes"][0]["ready"], true);
        assert_eq!(report["probes"][0]["output_row_count"], 1);
        assert_eq!(
            report["probes"][0]["execution_profile"]["scan_pruning_report_count"],
            1
        );
        assert_eq!(
            report["probes"][0]["execution_profile"]["pruned_scan_count"],
            1
        );
        assert_eq!(
            report["probes"][0]["selected_plan_operator_counts"]["IndexNodeSeek"],
            1
        );
        assert_eq!(
            report["probes"][0]["selected_plan_class_counts"]["access"],
            1
        );
        assert!(report["probes"][0]["optimizer_decision_count"]
            .as_u64()
            .is_some());
        assert_eq!(report["probes"][0]["plan_cache"]["lookup"], "miss");
        assert_eq!(report["probes"][0]["plan_cache"]["bypassed"], false);
        assert_eq!(
            report["probes"][0]["execution_profile"]["scan_pruning_reports"][0]["strategy"]["kind"],
            "property_eq"
        );
        assert_eq!(
            report["probes"][0]["execution_profile"]["scan_pruning_reports"][0]["strategy"]
                ["property"],
            "kind"
        );
        assert!(report["probes"][0].get("rows").is_none());
        assert!(report["probes"][0].get("parameters").is_none());
        assert!(!report.to_string().contains(graph_path.to_str().unwrap()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_accepts_graph_route_query_inventory() {
        let root = unique_test_dir("query-runtime-preflight-route-inventory");
        let graph_path = root.join("graph");
        let probe_path = root.join("graph-route-queries.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-b', kind: 'task', title: 'B'})")
            .unwrap();
        drop(db);
        let routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "route": route,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": format!("memory-by-kind:{route}"),
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
                            "parameters": {"kind": "note"},
                            "require_scan_pruning": true,
                            "require_pruned": true,
                            "max_output_rows": 1
                        }
                    ],
                    "blocker_codes": []
                })
            })
            .collect::<Vec<_>>();
        std::fs::write(
            &probe_path,
            serde_json::json!({ "routes": routes }).to_string(),
        )
        .unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], true);
        assert_eq!(
            report["probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["passed_probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(report["route_coverage_ready"], true);
        assert_eq!(report["probes"][0]["route"], "/graph/overview");
        assert_eq!(report["probes"][0]["query_family"], "memory_lookup");
        assert_eq!(report["probes"][0]["ready"], true);
        assert!(report["probes"][0].get("rows").is_none());
        assert!(report["probes"][0].get("parameters").is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_fails_closed_without_probes() {
        let root = unique_test_dir("query-runtime-preflight-empty");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let db = Database::open(&graph_path).unwrap();
        drop(db);
        std::fs::write(&probe_path, serde_json::json!({"probes": []}).to_string()).unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "query_runtime_probes_missing",
                "query_runtime_route_coverage_missing"
            ])
        );
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!(["query_runtime_route_coverage_missing"])
        );
        assert_eq!(report["route_coverage_ready"], false);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_rejects_unknown_route_probe() {
        let root = unique_test_dir("query-runtime-preflight-unknown-route");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        drop(db);
        let mut probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_probe(route))
            .collect::<Vec<_>>();
        probes.push(ready_probe("/graph/manual-extra-route"));
        std::fs::write(
            &probe_path,
            serde_json::json!({ "probes": probes }).to_string(),
        )
        .unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(report["required_routes_covered"], true);
        assert_eq!(report["route_coverage_ready"], false);
        assert_eq!(
            report["unknown_routes"],
            serde_json::json!(["/graph/manual-extra-route"])
        );
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!(["query_runtime_unknown_routes"])
        );
        assert!(report["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_unknown_routes"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_reports_duplicate_route_probe_without_blocking() {
        let root = unique_test_dir("query-runtime-preflight-duplicate-route");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        drop(db);
        let mut probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_probe(route))
            .collect::<Vec<_>>();
        probes.push(ready_probe("/graph/overview"));
        std::fs::write(
            &probe_path,
            serde_json::json!({ "probes": probes }).to_string(),
        )
        .unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], true);
        assert_eq!(report["required_routes_covered"], true);
        assert_eq!(report["route_coverage_ready"], true);
        assert_eq!(
            report["duplicate_routes"],
            serde_json::json!(["/graph/overview"])
        );
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_rejects_probe_without_identity() {
        let root = unique_test_dir("query-runtime-preflight-missing-identity");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        drop(db);
        std::fs::write(
            &probe_path,
            serde_json::json!({
                "probes": [
                    {
                        "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
                        "parameters": {"kind": "note"}
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(report["probes"][0]["success"], true);
        assert_eq!(report["probes"][0]["ready"], false);
        assert_eq!(
            report["probes"][0]["blocker_codes"],
            serde_json::json!([
                "query_runtime_probe_name_missing",
                "query_runtime_probe_route_missing",
                "query_runtime_probe_query_family_missing"
            ])
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_reports_query_error_class_without_raw_values() {
        let root = unique_test_dir("query-runtime-preflight-error");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let db = Database::open(&graph_path).unwrap();
        drop(db);
        std::fs::write(
            &probe_path,
            serde_json::json!({
                "name": "bad-query",
                "cypher": "MATCH (m:Memory RETURN m",
                "parameters": {"secret": "do-not-emit"}
            })
            .to_string(),
        )
        .unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(report["probes"][0]["success"], false);
        assert_eq!(report["probes"][0]["error_class"], "parse");
        assert!(!report.to_string().contains("do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }

    fn ready_probe(route: &str) -> serde_json::Value {
        serde_json::json!({
            "name": format!("memory-by-kind:{route}"),
            "route": route,
            "query_family": "memory_lookup",
            "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
            "parameters": {"kind": "note"},
            "require_scan_pruning": true,
            "require_pruned": true,
            "max_output_rows": 1
        })
    }
}
