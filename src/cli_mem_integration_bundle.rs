use skein::{Result, SkeinError, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES};
use std::collections::BTreeSet;
use std::path::Path;

const NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL: &str =
    "nowledge-mem-skein-integration-bundle";

pub fn nowledge_mem_integration_bundle_usage() -> String {
    "nowledge-mem-integration-bundle requires [--require-ready] --submodule-path <path> --submodule-commit <commit> --legacy-data-retained --coexistence-mode shadow|side_by_side --content-store-present --content-store-engine sqlite --content-store-messages-available --content-store-source-chunks-available --previous-wrapper-preflight-json <path> --replacement-summary-json <path> --bounded-read-evidence-json <path> --graph-route-readiness-json <path> --query-runtime-preflight-json <path> --search-candidate-shadow-evidence-json <path> --library-readiness-json <path>"
        .to_string()
}

#[derive(Debug, Clone, Default)]
struct IntegrationBundleInputs {
    require_ready: bool,
    submodule_path: Option<String>,
    submodule_commit: Option<String>,
    legacy_data_retained: bool,
    legacy_data_deleted: bool,
    coexistence_mode: Option<String>,
    content_store_present: bool,
    content_store_engine: Option<String>,
    content_store_messages_available: bool,
    content_store_source_chunks_available: bool,
    previous_wrapper_preflight: Option<serde_json::Value>,
    replacement_summary: Option<serde_json::Value>,
    bounded_read_evidence: Option<serde_json::Value>,
    graph_route_readiness: Option<serde_json::Value>,
    query_runtime_preflight: Option<serde_json::Value>,
    search_candidate_shadow_evidence: Option<serde_json::Value>,
    library_readiness: Option<serde_json::Value>,
}

pub fn run_nowledge_mem_integration_bundle(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut inputs = IntegrationBundleInputs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                inputs.require_ready = true;
            }
            "--submodule-path" => {
                inputs.submodule_path = Some(next_arg(&mut args)?);
            }
            "--submodule-commit" => {
                inputs.submodule_commit = Some(next_arg(&mut args)?);
            }
            "--legacy-data-retained" => {
                inputs.legacy_data_retained = true;
            }
            "--legacy-data-deleted" => {
                inputs.legacy_data_deleted = true;
            }
            "--coexistence-mode" => {
                inputs.coexistence_mode = Some(next_arg(&mut args)?);
            }
            "--content-store-present" => {
                inputs.content_store_present = true;
            }
            "--content-store-engine" => {
                inputs.content_store_engine = Some(next_arg(&mut args)?);
            }
            "--content-store-messages-available" => {
                inputs.content_store_messages_available = true;
            }
            "--content-store-source-chunks-available" => {
                inputs.content_store_source_chunks_available = true;
            }
            "--previous-wrapper-preflight-json" => {
                inputs.previous_wrapper_preflight = Some(read_json_arg(&mut args)?);
            }
            "--replacement-summary-json" => {
                inputs.replacement_summary = Some(read_json_arg(&mut args)?);
            }
            "--bounded-read-evidence-json" => {
                inputs.bounded_read_evidence = Some(read_json_arg(&mut args)?);
            }
            "--graph-route-readiness-json" => {
                inputs.graph_route_readiness = Some(read_json_arg(&mut args)?);
            }
            "--query-runtime-preflight-json" => {
                inputs.query_runtime_preflight = Some(read_json_arg(&mut args)?);
            }
            "--search-candidate-shadow-evidence-json" => {
                inputs.search_candidate_shadow_evidence = Some(read_json_arg(&mut args)?);
            }
            "--library-readiness-json" => {
                inputs.library_readiness = Some(read_json_arg(&mut args)?);
            }
            _ => {
                return Err(SkeinError::Semantic(nowledge_mem_integration_bundle_usage()));
            }
        }
    }

    let require_ready = inputs.require_ready;
    Ok((nowledge_mem_integration_bundle_json(inputs)?, require_ready))
}

fn nowledge_mem_integration_bundle_json(
    inputs: IntegrationBundleInputs,
) -> Result<serde_json::Value> {
    let submodule_path = require_non_empty(inputs.submodule_path, "--submodule-path")?;
    let submodule_commit = require_non_empty(inputs.submodule_commit, "--submodule-commit")?;
    let coexistence_mode = require_non_empty(inputs.coexistence_mode, "--coexistence-mode")?;
    if !matches!(coexistence_mode.as_str(), "shadow" | "side_by_side") {
        return Err(SkeinError::Semantic(
            "--coexistence-mode must be shadow or side_by_side".to_string(),
        ));
    }
    let content_store_engine =
        require_non_empty(inputs.content_store_engine, "--content-store-engine")?;
    let previous_wrapper_preflight = require_json(
        inputs.previous_wrapper_preflight,
        "--previous-wrapper-preflight-json",
    )?;
    let replacement_summary =
        require_json(inputs.replacement_summary, "--replacement-summary-json")?;
    let bounded_read_evidence =
        require_json(inputs.bounded_read_evidence, "--bounded-read-evidence-json")?;
    let graph_route_readiness =
        require_json(inputs.graph_route_readiness, "--graph-route-readiness-json")?;
    let query_runtime_preflight = require_json(
        inputs.query_runtime_preflight,
        "--query-runtime-preflight-json",
    )?;
    let search_candidate_shadow_evidence = require_json(
        inputs.search_candidate_shadow_evidence,
        "--search-candidate-shadow-evidence-json",
    )?;
    let library_readiness = require_json(inputs.library_readiness, "--library-readiness-json")?;
    let bounded_alignment =
        bounded_read_alignment_json(&bounded_read_evidence, &replacement_summary);
    let graph_route_alignment =
        graph_route_alignment_json(&graph_route_readiness, &replacement_summary);

    Ok(serde_json::json!({
        "protocol": NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL,
        "submodule": {
            "present": true,
            "path": sanitize_path_label(&submodule_path),
            "commit": submodule_commit,
            "blocker_codes": []
        },
        "coexistence": {
            "old_database_retained": inputs.legacy_data_retained,
            "old_database_deleted": inputs.legacy_data_deleted,
            "mode": coexistence_mode,
            "blocker_codes": coexistence_blocker_codes(
                inputs.legacy_data_retained,
                inputs.legacy_data_deleted
            )
        },
        "content_store": {
            "present": inputs.content_store_present,
            "engine": content_store_engine,
            "messages_available": inputs.content_store_messages_available,
            "source_chunks_available": inputs.content_store_source_chunks_available,
            "blocker_codes": content_store_blocker_codes(
                inputs.content_store_present,
                inputs.content_store_messages_available,
                inputs.content_store_source_chunks_available
            )
        },
        "previous_wrapper_preflight": previous_wrapper_preflight,
        "replacement_summary": replacement_summary,
        "bounded_read_evidence": bounded_read_evidence,
        "replacement_summary_bounded_read_alignment": bounded_alignment,
        "graph_route_readiness": graph_route_readiness,
        "replacement_summary_graph_route_alignment": graph_route_alignment,
        "query_runtime_preflight": query_runtime_preflight,
        "search_candidate_shadow_evidence": search_candidate_shadow_evidence,
        "library_readiness": library_readiness,
    }))
}

fn graph_route_alignment_json(
    graph_route_readiness: &serde_json::Value,
    replacement_summary: &serde_json::Value,
) -> serde_json::Value {
    let summary = replacement_summary
        .get("bounded_read_evidence")
        .unwrap_or(&serde_json::Value::Null);
    let evidence_present = !graph_route_readiness.is_null();
    let summary_present = !summary.is_null();
    let evidence_route_primary_ready =
        bool_path(graph_route_readiness, &["route_primary_ready"]) == Some(true);
    let summary_route_primary_ready = bool_path(summary, &["route_primary_ready"]) == Some(true);
    let evidence_primary_ready_routes = graph_route_primary_ready_routes(graph_route_readiness);
    let summary_primary_ready_routes = string_set_path(summary, &["primary_ready_routes"]);
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| (*route).to_string())
        .collect::<BTreeSet<_>>();
    let route_primary_ready_matches = evidence_route_primary_ready == summary_route_primary_ready;
    let primary_ready_routes_match = evidence_primary_ready_routes == summary_primary_ready_routes;
    let evidence_required_routes_covered = required_routes
        .iter()
        .all(|route| evidence_primary_ready_routes.contains(route));
    let summary_required_routes_covered = required_routes
        .iter()
        .all(|route| summary_primary_ready_routes.contains(route));
    let alignment = GraphRouteAlignment {
        evidence_present,
        summary_present,
        evidence_route_primary_ready,
        summary_route_primary_ready,
        route_primary_ready_matches,
        primary_ready_routes_match,
        evidence_required_routes_covered,
        summary_required_routes_covered,
    };

    serde_json::json!({
        "ready": alignment.ready(),
        "evidence_present": evidence_present,
        "summary_present": summary_present,
        "evidence_route_primary_ready": evidence_route_primary_ready,
        "summary_route_primary_ready": summary_route_primary_ready,
        "route_primary_ready_matches": route_primary_ready_matches,
        "primary_ready_routes_match": primary_ready_routes_match,
        "evidence_required_routes_covered": evidence_required_routes_covered,
        "summary_required_routes_covered": summary_required_routes_covered,
        "evidence_primary_ready_routes": evidence_primary_ready_routes,
        "summary_primary_ready_routes": summary_primary_ready_routes,
        "blocker_codes": alignment.blocker_codes()
    })
}

fn graph_route_primary_ready_routes(value: &serde_json::Value) -> BTreeSet<String> {
    value
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|route| bool_path(route, &["primary_ready"]) == Some(true))
        .filter_map(|route| str_path(route, &["route"]))
        .map(str::to_string)
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct GraphRouteAlignment {
    evidence_present: bool,
    summary_present: bool,
    evidence_route_primary_ready: bool,
    summary_route_primary_ready: bool,
    route_primary_ready_matches: bool,
    primary_ready_routes_match: bool,
    evidence_required_routes_covered: bool,
    summary_required_routes_covered: bool,
}

impl GraphRouteAlignment {
    fn ready(&self) -> bool {
        self.evidence_present
            && self.summary_present
            && self.evidence_route_primary_ready
            && self.summary_route_primary_ready
            && self.route_primary_ready_matches
            && self.primary_ready_routes_match
            && self.evidence_required_routes_covered
            && self.summary_required_routes_covered
    }

    fn blocker_codes(&self) -> Vec<&'static str> {
        let mut blockers = Vec::new();
        if !self.evidence_present {
            blockers.push("graph_route_readiness_missing");
        }
        if !self.summary_present {
            blockers.push("replacement_summary_bounded_read_evidence_missing");
        }
        if !self.evidence_route_primary_ready {
            blockers.push("graph_route_readiness_not_primary_ready");
        }
        if !self.summary_route_primary_ready {
            blockers.push("replacement_summary_route_primary_not_ready");
        }
        if !self.route_primary_ready_matches {
            blockers.push("graph_route_primary_ready_mismatch");
        }
        if !self.primary_ready_routes_match {
            blockers.push("graph_route_primary_ready_routes_mismatch");
        }
        if !self.evidence_required_routes_covered {
            blockers.push("graph_route_readiness_required_routes_missing");
        }
        if !self.summary_required_routes_covered {
            blockers.push("replacement_summary_primary_ready_routes_missing");
        }
        blockers
    }
}

fn bounded_read_alignment_json(
    bounded_read_evidence: &serde_json::Value,
    replacement_summary: &serde_json::Value,
) -> serde_json::Value {
    let summary = replacement_summary
        .get("bounded_read_evidence")
        .unwrap_or(&serde_json::Value::Null);
    let evidence_present = !bounded_read_evidence.is_null();
    let summary_present = !summary.is_null();
    let evidence_ready = bool_path(bounded_read_evidence, &["ready"]) == Some(true);
    let summary_ready = bool_path(summary, &["ready"]) == Some(true);
    let protocol_matches =
        str_path(bounded_read_evidence, &["protocol"]) == str_path(summary, &["protocol"]);
    let readiness_matches =
        bool_path(bounded_read_evidence, &["ready"]) == bool_path(summary, &["ready"]);
    let mode_matches = str_path(bounded_read_evidence, &["mode"]) == str_path(summary, &["mode"]);
    let max_rows_matches =
        u64_path(bounded_read_evidence, &["max_rows"]) == u64_path(summary, &["max_rows"]);
    let streaming_matches =
        bool_path(bounded_read_evidence, &["streaming"]) == bool_path(summary, &["streaming"]);
    let covered_routes_matches = string_set_path(bounded_read_evidence, &["covered_routes"])
        == string_set_path(summary, &["covered_routes"]);
    let alignment = BoundedReadAlignment {
        evidence_present,
        summary_present,
        evidence_ready,
        summary_ready,
        protocol_matches,
        readiness_matches,
        mode_matches,
        max_rows_matches,
        streaming_matches,
        covered_routes_matches,
    };

    serde_json::json!({
        "ready": alignment.ready(),
        "evidence_present": alignment.evidence_present,
        "summary_present": alignment.summary_present,
        "evidence_ready": alignment.evidence_ready,
        "summary_ready": alignment.summary_ready,
        "protocol_matches": alignment.protocol_matches,
        "readiness_matches": alignment.readiness_matches,
        "mode_matches": alignment.mode_matches,
        "max_rows_matches": alignment.max_rows_matches,
        "streaming_matches": alignment.streaming_matches,
        "covered_routes_matches": alignment.covered_routes_matches,
        "blocker_codes": alignment.blocker_codes()
    })
}

#[derive(Debug, Clone, Copy)]
struct BoundedReadAlignment {
    evidence_present: bool,
    summary_present: bool,
    evidence_ready: bool,
    summary_ready: bool,
    protocol_matches: bool,
    readiness_matches: bool,
    mode_matches: bool,
    max_rows_matches: bool,
    streaming_matches: bool,
    covered_routes_matches: bool,
}

impl BoundedReadAlignment {
    fn ready(&self) -> bool {
        self.evidence_present
            && self.summary_present
            && self.evidence_ready
            && self.summary_ready
            && self.protocol_matches
            && self.readiness_matches
            && self.mode_matches
            && self.max_rows_matches
            && self.streaming_matches
            && self.covered_routes_matches
    }

    fn blocker_codes(&self) -> Vec<&'static str> {
        let mut blockers = Vec::new();
        if !self.evidence_present {
            blockers.push("bounded_read_evidence_missing");
        }
        if !self.summary_present {
            blockers.push("replacement_summary_bounded_read_evidence_missing");
        }
        if !self.evidence_ready {
            blockers.push("bounded_read_evidence_not_ready");
        }
        if !self.summary_ready {
            blockers.push("replacement_summary_bounded_read_evidence_not_ready");
        }
        if !self.protocol_matches {
            blockers.push("bounded_read_protocol_mismatch");
        }
        if !self.readiness_matches {
            blockers.push("bounded_read_readiness_mismatch");
        }
        if !self.mode_matches {
            blockers.push("bounded_read_mode_mismatch");
        }
        if !self.max_rows_matches {
            blockers.push("bounded_read_max_rows_mismatch");
        }
        if !self.streaming_matches {
            blockers.push("bounded_read_streaming_mismatch");
        }
        if !self.covered_routes_matches {
            blockers.push("bounded_read_covered_routes_mismatch");
        }
        blockers
    }
}

fn coexistence_blocker_codes(
    legacy_data_retained: bool,
    legacy_data_deleted: bool,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !legacy_data_retained {
        blockers.push("legacy_data_not_retained");
    }
    if legacy_data_deleted {
        blockers.push("legacy_data_deleted");
    }
    blockers
}

fn content_store_blocker_codes(
    present: bool,
    messages_available: bool,
    source_chunks_available: bool,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !present {
        blockers.push("content_store_missing");
    }
    if !messages_available {
        blockers.push("content_store_messages_missing");
    }
    if !source_chunks_available {
        blockers.push("content_store_source_chunks_missing");
    }
    blockers
}

fn sanitize_path_label(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("<redacted>")
        .to_string()
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let path = next_arg(args)?;
    read_json_file(Path::new(&path))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read Nowledge Mem integration bundle input: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse Nowledge Mem integration bundle input: {error}"
        ))
    })
}

fn require_non_empty(value: Option<String>, flag: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SkeinError::Semantic(format!("{flag} is required")))
}

fn require_json(value: Option<serde_json::Value>, flag: &str) -> Result<serde_json::Value> {
    value.ok_or_else(|| SkeinError::Semantic(format!("{flag} is required")))
}

fn next_arg(args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_mem_integration_bundle_usage()))
}

fn string_set_path(value: &serde_json::Value, path: &[&str]) -> BTreeSet<String> {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    value_path(value, path).and_then(serde_json::Value::as_bool)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    value_path(value, path).and_then(serde_json::Value::as_str)
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    value_path(value, path).and_then(serde_json::Value::as_u64)
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
    use super::{nowledge_mem_integration_bundle_json, IntegrationBundleInputs};
    use crate::cli_mem_integration_readiness::nowledge_mem_integration_readiness_json;
    use skein::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES;

    #[test]
    fn generated_bundle_feeds_integration_readiness_gate() {
        let bundle = nowledge_mem_integration_bundle_json(ready_inputs()).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(bundle["protocol"], "nowledge-mem-skein-integration-bundle");
        assert_eq!(bundle["submodule"]["path"], "skein");
        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["ready"],
            true
        );
        assert_eq!(readiness["ready"], true);
        assert_eq!(readiness["failed_checks"], serde_json::json!([]));
    }

    #[test]
    fn generated_bundle_fails_closed_when_legacy_data_is_not_retained() {
        let mut inputs = ready_inputs();
        inputs.legacy_data_retained = false;

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["coexistence"]["blocker_codes"],
            serde_json::json!(["legacy_data_not_retained"])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["legacy_coexistence"])
        );
    }

    #[test]
    fn generated_bundle_detects_bounded_read_alignment_mismatch() {
        let mut inputs = ready_inputs();
        inputs.bounded_read_evidence.as_mut().unwrap()["max_rows"] = serde_json::json!(128);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"],
            serde_json::json!(["bounded_read_max_rows_mismatch"])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
    }

    fn ready_inputs() -> IntegrationBundleInputs {
        IntegrationBundleInputs {
            require_ready: false,
            submodule_path: Some("/redacted/vendor/skein".to_string()),
            submodule_commit: Some("abc1234".to_string()),
            legacy_data_retained: true,
            legacy_data_deleted: false,
            coexistence_mode: Some("shadow".to_string()),
            content_store_present: true,
            content_store_engine: Some("sqlite".to_string()),
            content_store_messages_available: true,
            content_store_source_chunks_available: true,
            previous_wrapper_preflight: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": [],
                "failed_checks": []
            })),
            replacement_summary: Some(ready_replacement_summary()),
            bounded_read_evidence: Some(ready_bounded_read_evidence()),
            graph_route_readiness: Some(ready_graph_route_readiness()),
            query_runtime_preflight: Some(ready_query_runtime_preflight()),
            search_candidate_shadow_evidence: Some(ready_search_candidate_shadow_evidence()),
            library_readiness: Some(ready_library_readiness()),
        }
    }

    fn ready_replacement_summary() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-replacement-summary",
            "production_cutover_ready": true,
            "blocking_categories": [],
            "missing_evidence": [],
            "shadow_evidence": {
                "ready": true
            },
            "dual_engine_evidence": {
                "present": true,
                "ready": true,
                "consistent": true
            },
            "replacement_readiness_family_summary": {
                "min_replacement_readiness_per_million": 1_000_000,
                "blocked_query_families": [],
                "required_query_families": [
                    "memory_lookup",
                    "graph_traversal",
                    "projected_graph",
                    "search_projection"
                ],
                "missing_required_query_families": []
            },
            "search_projection_evidence": {
                "protocol": "skein-nowledge-search-projection-evidence",
                "ready": true,
                "fts_ready": true,
                "vector_ready": true,
                "incremental_update_ready": true,
                "predicate_pushdown_ready": true,
                "compressed_vector_projection_required": true,
                "compressed_vector_projection_ready": true,
                "blocker_codes": []
            },
            "search_projection_shadow_evidence": {
                "protocol": "skein-nowledge-search-projection-shadow-evidence",
                "present": true,
                "ready": true,
                "document_count_parity": true,
                "table_parity_ready": true,
                "embedding_identity_parity": true,
                "incremental_watermark_parity": true,
                "blocker_codes": []
            },
            "bounded_read_evidence": ready_bounded_read_evidence(),
            "cutover_evidence": {
                "storage_recovery_required": true,
                "storage_recovery_ready": true,
                "storage_recovery_protocol_matches": true,
                "storage_recovery_durable": true,
                "storage_recovery_checkpoint_boundary_present": true,
                "storage_recovery_wal_replay_bounded": true,
                "storage_recovery_torn_tail_clean": true,
                "storage_recovery_blocker_codes": [],
                "storage_recovery_blockers": [],
                "background_maintenance_required": true,
                "background_maintenance_ready": true,
                "background_maintenance_protocol_matches": true,
                "background_maintenance_executable_search_projection_graph_delta_count": 1,
                "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                "background_maintenance_deferred_search_projection_graph_delta_count": 0,
                "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                "background_maintenance_executable_search_projection_graph_delta_operations": 2,
                "background_maintenance_admitted_search_projection_graph_delta_operations": 2,
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                "background_maintenance_blocker_codes": [],
                "background_maintenance_blockers": []
            }
        })
    }

    fn ready_bounded_read_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-bounded-read-evidence-v1",
            "present": true,
            "ready": true,
            "route_primary_ready": true,
            "primary_ready_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "mode": "shadow_read_only",
            "max_rows": 512,
            "execution_row_cap": 513,
            "row_limit_enforced_before_output": true,
            "operator_row_cap_enabled": true,
            "blocking_operator_count": 0,
            "streaming": false,
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_covered_routes": [],
            "blocker_codes": []
        })
    }

    fn ready_graph_route_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": "nmem-graph-route-readiness-v1",
            "route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "missing_required_routes": [],
            "primary_ready_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "missing_query_runtime_routes": [],
            "route_query_runtime_ready": true,
            "route_primary_ready": true,
            "route_primary_blocker_codes": [],
            "routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .map(|route| {
                    serde_json::json!({
                        "route": route,
                        "shadow_compare_ready": true,
                        "primary_ready": true,
                        "query_runtime_ready": true,
                        "query_report_count": 1,
                        "query_reports": [ready_graph_route_query_report()],
                        "blocker_codes": []
                    })
                })
                .collect::<Vec<_>>()
        })
    }

    fn ready_graph_route_query_report() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-query-report-v1",
            "statement_kind": "match_return",
            "execution_path": "fast_path",
            "fast_path_selected": true,
            "slow_log_candidate": false,
            "physical_plan_captured": false,
            "elapsed_micros": 12,
            "physical_operator_counts_present": true,
            "optimizer_decision_count": 2,
            "scan_pruning_report_count": 1,
            "scan_pruning_reports_present": true,
            "plan_cache_lookup": "miss",
            "plan_cache": {
                "lookup": "miss",
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            },
            "ready": true,
            "blocker_codes": []
        })
    }

    fn ready_search_candidate_shadow_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-search-candidate-shadow-evidence",
            "route": "/search-index/skein-shadow/candidate-evidence",
            "engine": "skein-shadow",
            "ready": true,
            "candidate_primary_engine": "skein",
            "blocker_codes": []
        })
    }

    fn ready_query_runtime_preflight() -> serde_json::Value {
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_query_runtime_preflight_probe(route))
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "ready": true,
            "database_opened": true,
            "probe_count": probes.len(),
            "passed_probe_count": probes.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "blocker_codes": [],
            "probes": probes
        })
    }

    fn ready_query_runtime_preflight_probe(route: &str) -> serde_json::Value {
        serde_json::json!({
            "name": format!("probe:{route}"),
            "route": route,
            "query_family": "memory_lookup",
            "ready": true,
            "success": true,
            "output_row_count": 1,
            "selected_plan_fingerprint": "IndexNodeSeek(1:m:6:Memory)",
            "selected_plan_operator_counts": {
                "IndexNodeSeek": 1,
                "ProjectExec": 1
            },
            "selected_plan_class_counts": {
                "access": 1,
                "relational": 1
            },
            "optimizer_decision_count": 2,
            "plan_cache_lookup": "miss",
            "plan_cache": {
                "lookup": "miss",
                "bypass_reason": null,
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            },
            "execution_profile": {
                "scan_pruning_report_count": 1,
                "pruned_scan_count": 1,
                "scan_pruning_reports": [
                    {
                        "label_id": 1,
                        "strategy": {
                            "kind": "property_eq",
                            "property": "id"
                        },
                        "pruned": true,
                        "exact_empty": false,
                        "candidate_count_before_filter": 1,
                        "output_count": 1,
                        "filtered_out_count": 0
                    }
                ]
            },
            "blocker_codes": []
        })
    }

    fn ready_library_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-library-readiness-v1",
            "present": true,
            "ready": true,
            "ready_area_count": 7,
            "blocked_area_count": 0,
            "blocker_codes": [],
            "open_report": {
                "graph_opened": true,
                "search_projection_opened": true
            },
            "readiness_by_area": {
                "graph": {"ready": true, "blocker_codes": []},
                "query": {"ready": true, "blocker_codes": []},
                "storage": {"ready": true, "blocker_codes": []},
                "background": {"ready": true, "blocker_codes": []},
                "query_family": {"ready": true, "blocker_codes": []},
                "search_projection": {"ready": true, "blocker_codes": []},
                "search_projection_shadow": {"ready": true, "blocker_codes": []}
            }
        })
    }
}
