use std::collections::BTreeSet;

pub fn nowledge_replacement_summary_usage() -> String {
    "nowledge-replacement-summary requires [--require-production-ready] <migration-gate-json>"
        .to_string()
}

pub fn nowledge_replacement_summary_json(bundle: &serde_json::Value) -> serde_json::Value {
    let covered_business_surface_per_million =
        json_get_u64_path(bundle, &["inventory_gate", "coverage_per_million"])
            .or_else(|| json_get_u64_path(bundle, &["coverage", "coverage_per_million"]));
    let shadow_parity_per_million = json_get_u64_path(bundle, &["cutover", "matched_per_million"])
        .or_else(|| json_get_u64_path(bundle, &["migration_gate", "shadow_matched_per_million"]));
    let replacement_readiness_per_million =
        json_get_u64_path(bundle, &["replacement_readiness_per_million"]);
    let migration_gate_decision = json_get_str_path(bundle, &["migration_gate", "decision"]);
    let cutover_decision = json_get_str_path(bundle, &["cutover", "decision"]);
    let cutover_evidence_eligible =
        json_get_bool_path(bundle, &["cutover_evidence", "eligible"]).unwrap_or(false);
    let production_cutover_ready = migration_gate_decision == Some("ready")
        && cutover_decision == Some("ready")
        && cutover_evidence_eligible
        && replacement_readiness_per_million == Some(1_000_000);
    let production_replacement_per_million = if production_cutover_ready {
        1_000_000
    } else {
        0
    };
    let blocking_categories = nowledge_replacement_blocking_categories(
        bundle,
        covered_business_surface_per_million,
        shadow_parity_per_million,
        replacement_readiness_per_million,
        migration_gate_decision,
        cutover_decision,
        cutover_evidence_eligible,
    );
    let blockers = nowledge_replacement_blockers(bundle);
    let missing_evidence = nowledge_replacement_missing_evidence(bundle);

    serde_json::json!({
        "protocol": "skein-nowledge-replacement-summary",
        "business_surface": {
            "covered_per_million": covered_business_surface_per_million,
            "ready": covered_business_surface_per_million == Some(1_000_000),
        },
        "shadow_parity": {
            "matched_per_million": shadow_parity_per_million,
            "decision": cutover_decision,
            "ready": cutover_decision == Some("ready") && shadow_parity_per_million == Some(1_000_000),
        },
        "replacement_readiness_per_million": replacement_readiness_per_million,
        "production_replacement_per_million": production_replacement_per_million,
        "production_cutover_ready": production_cutover_ready,
        "migration_gate_decision": migration_gate_decision,
        "cutover_evidence": {
            "eligible": cutover_evidence_eligible,
            "evidence_kind": json_get_str_path(bundle, &["cutover_evidence", "evidence_kind"]),
            "ready_engine_kind": json_get_str_path(bundle, &["cutover_evidence", "ready_engine_kind"]),
            "storage_recovery_required": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]),
            "storage_recovery_ready": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]),
            "background_maintenance_required": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_required"]),
            "background_maintenance_ready": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_ready"]),
            "replacement_readiness_min_per_million": json_get_u64_path(bundle, &["cutover_evidence", "replacement_readiness_min_per_million"]),
        },
        "blocking_categories": blocking_categories,
        "blockers": blockers,
        "missing_evidence": missing_evidence,
        "replacement_readiness_by_query_family": bundle
            .get("replacement_readiness_by_query_family")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    })
}

fn nowledge_replacement_blocking_categories(
    bundle: &serde_json::Value,
    covered_business_surface_per_million: Option<u64>,
    shadow_parity_per_million: Option<u64>,
    replacement_readiness_per_million: Option<u64>,
    migration_gate_decision: Option<&str>,
    cutover_decision: Option<&str>,
    cutover_evidence_eligible: bool,
) -> Vec<String> {
    let mut categories = BTreeSet::new();
    if covered_business_surface_per_million != Some(1_000_000) {
        categories.insert("scanner_coverage".to_string());
    }
    if shadow_parity_per_million != Some(1_000_000) || cutover_decision != Some("ready") {
        categories.insert("shadow_parity".to_string());
    }
    if replacement_readiness_per_million != Some(1_000_000) {
        categories.insert("query_family_readiness".to_string());
    }
    if migration_gate_decision != Some("ready") {
        categories.insert("migration_gate".to_string());
    }
    if !cutover_evidence_eligible {
        categories.insert("cutover_evidence".to_string());
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) != Some(true)
    {
        categories.insert("storage_recovery".to_string());
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_ready"],
        ) != Some(true)
    {
        categories.insert("background_maintenance".to_string());
    }
    if json_get_u64_path(
        bundle,
        &[
            "cutover_evidence",
            "replacement_readiness_invalid_family_count",
        ],
    )
    .unwrap_or(0)
        > 0
    {
        categories.insert("query_family_readiness".to_string());
    }
    categories.into_iter().collect()
}

fn nowledge_replacement_missing_evidence(bundle: &serde_json::Value) -> Vec<String> {
    let mut missing = Vec::new();
    if bundle.get("cutover_evidence").is_none() {
        missing.push("cutover_evidence".to_string());
    }
    if bundle
        .get("replacement_readiness_by_query_family")
        .is_none()
    {
        missing.push("replacement_readiness_by_query_family".to_string());
    }
    if bundle.get("shadow_run").is_none() {
        missing.push("shadow_run".to_string());
    }
    if bundle.get("shadow_ready").is_none() {
        missing.push("shadow_ready".to_string());
    }
    missing
}

fn nowledge_replacement_blockers(bundle: &serde_json::Value) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    for path in [
        &["inventory_gate", "blockers"][..],
        &["cutover", "blockers"][..],
        &["migration_gate", "blockers"][..],
        &["cutover_evidence", "blockers"][..],
        &["cutover_evidence", "storage_recovery_blockers"][..],
        &["cutover_evidence", "background_maintenance_blockers"][..],
        &["cutover_evidence", "replacement_readiness_blockers"][..],
    ] {
        for blocker in json_get_string_array_path(bundle, path) {
            blockers.insert(blocker);
        }
    }
    blockers.into_iter().collect()
}

fn json_get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn json_get_u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

fn json_get_bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

fn json_get_str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

fn json_get_string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{nowledge_replacement_summary_json, nowledge_replacement_summary_usage};

    #[test]
    fn replacement_summary_keeps_production_replacement_zero_without_cutover_evidence() {
        let bundle = serde_json::json!({
            "coverage": {
                "coverage_per_million": 1_000_000
            },
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": []
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["business_surface"]["ready"], true);
        assert_eq!(summary["shadow_parity"]["ready"], true);
        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["blocking_categories"],
            serde_json::json!(["cutover_evidence"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "cutover_evidence"));
    }

    #[test]
    fn replacement_summary_reports_production_ready_when_all_evidence_is_ready() {
        let bundle = serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "cutover_evidence": {
                "eligible": true,
                "evidence_kind": "previous_wrapper",
                "ready_engine_kind": "previous_wrapper",
                "storage_recovery_required": true,
                "storage_recovery_ready": true,
                "storage_recovery_blockers": [],
                "background_maintenance_required": true,
                "background_maintenance_ready": true,
                "background_maintenance_blockers": [],
                "replacement_readiness_min_per_million": 1_000_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [],
                "blockers": []
            },
            "shadow_run": {
                "evidence_kind": "previous_wrapper"
            },
            "shadow_ready": {
                "engine_kind": "previous_wrapper"
            },
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "memory_lookup",
                    "replacement_readiness_per_million": 1_000_000
                }
            ]
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], true);
        assert_eq!(summary["production_replacement_per_million"], 1_000_000);
        assert_eq!(summary["blocking_categories"], serde_json::json!([]));
        assert_eq!(summary["missing_evidence"], serde_json::json!([]));
        assert_eq!(
            summary["replacement_readiness_by_query_family"][0]["query_family"],
            "memory_lookup"
        );
    }

    #[test]
    fn replacement_summary_groups_storage_and_background_blockers() {
        let bundle = serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "blocked",
                "matched_per_million": 500_000,
                "blockers": ["primary-only projected graph"]
            },
            "migration_gate": {
                "decision": "blocked",
                "blockers": ["shadow parity blocked"]
            },
            "cutover_evidence": {
                "eligible": false,
                "storage_recovery_required": true,
                "storage_recovery_ready": false,
                "storage_recovery_blockers": [
                    "WAL replay was not opened with a configured entry bound"
                ],
                "background_maintenance_required": true,
                "background_maintenance_ready": false,
                "background_maintenance_blockers": [
                    "background maintenance summary is missing"
                ],
                "replacement_readiness_min_per_million": 500_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [
                    "query family search is below full replacement readiness"
                ],
                "blockers": ["cutover evidence blocked"]
            },
            "replacement_readiness_per_million": 500_000,
            "replacement_readiness_by_query_family": []
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(
            summary["blocking_categories"],
            serde_json::json!([
                "background_maintenance",
                "cutover_evidence",
                "migration_gate",
                "query_family_readiness",
                "shadow_parity",
                "storage_recovery"
            ])
        );
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "background maintenance summary is missing"));
        assert_eq!(summary["production_replacement_per_million"], 0);
    }

    #[test]
    fn validates_nowledge_replacement_summary_usage_text() {
        assert!(nowledge_replacement_summary_usage().contains("<migration-gate-json>"));
        assert!(nowledge_replacement_summary_usage().contains("--require-production-ready"));
    }
}
