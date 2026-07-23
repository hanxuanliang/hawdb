use skein::{Result, SkeinError, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES};
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
}

impl RouteEvidence {
    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "route": self.route,
            "shadow_compare_ready": self.shadow_compare_ready,
            "primary_ready": self.primary_ready,
            "blocker_codes": self.blocker_codes,
        })
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
        blockers.extend(route.blocker_codes.iter().cloned());
    }
    blockers.into_iter().collect()
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

    fn ready_routes() -> Vec<serde_json::Value> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "route": route,
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "blocker_codes": []
                })
            })
            .collect()
    }
}
