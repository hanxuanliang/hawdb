use skein::{Result, SkeinError};
use std::path::Path;

pub fn nowledge_previous_wrapper_preflight_check_usage() -> String {
    "nowledge-previous-wrapper-preflight-check requires [--require-ready] --wrapper-identity <id> --contract-evidence-json <path> --adapter-smoke-json <path> --migration-gate-json <path> --replacement-summary-json <path>".to_string()
}

#[derive(Debug, Clone, Default)]
struct PreviousWrapperPreflightCheckInputs {
    wrapper_identity: Option<String>,
    contract_evidence: Option<serde_json::Value>,
    adapter_smoke: Option<serde_json::Value>,
    migration_gate: Option<serde_json::Value>,
    replacement_summary: Option<serde_json::Value>,
}

pub fn run_nowledge_previous_wrapper_preflight_check(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut inputs = PreviousWrapperPreflightCheckInputs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--wrapper-identity" => {
                let value = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage())
                })?;
                if value.trim().is_empty() {
                    return Err(SkeinError::Semantic(
                        "--wrapper-identity must not be empty".to_string(),
                    ));
                }
                inputs.wrapper_identity = Some(value);
            }
            "--contract-evidence-json" => {
                inputs.contract_evidence = Some(read_json_arg(&mut args)?);
            }
            "--adapter-smoke-json" => {
                inputs.adapter_smoke = Some(read_json_arg(&mut args)?);
            }
            "--migration-gate-json" => {
                inputs.migration_gate = Some(read_json_arg(&mut args)?);
            }
            "--replacement-summary-json" => {
                inputs.replacement_summary = Some(read_json_arg(&mut args)?);
            }
            _ => {
                return Err(SkeinError::Semantic(
                    nowledge_previous_wrapper_preflight_check_usage(),
                ));
            }
        }
    }
    let report = nowledge_previous_wrapper_preflight_check_json(inputs)?;
    Ok((report, require_ready))
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let path = args
        .next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    read_json_file(Path::new(&path))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read previous-wrapper preflight JSON '{}': {error}",
            path.display()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to parse previous-wrapper preflight JSON '{}': {error}",
            path.display()
        ))
    })
}

fn nowledge_previous_wrapper_preflight_check_json(
    inputs: PreviousWrapperPreflightCheckInputs,
) -> Result<serde_json::Value> {
    let wrapper_identity = inputs
        .wrapper_identity
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let contract_evidence = inputs
        .contract_evidence
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let adapter_smoke = inputs
        .adapter_smoke
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let migration_gate = inputs
        .migration_gate
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;
    let replacement_summary = inputs
        .replacement_summary
        .ok_or_else(|| SkeinError::Semantic(nowledge_previous_wrapper_preflight_check_usage()))?;

    let checks = vec![
        preflight_check(
            "full_contract",
            [
                bool_path(&contract_evidence, &["required_contract_ready"]) == Some(true),
                bool_path(
                    &contract_evidence,
                    &["previous_wrapper_contract_evidence", "ready"],
                ) == Some(true),
                str_path(
                    &contract_evidence,
                    &["previous_wrapper_contract_evidence", "wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
            ],
            [
                "required_contract_ready",
                "previous_wrapper_contract_evidence.ready",
                "previous_wrapper_contract_evidence.wrapper_identity",
            ],
            blocker_codes(
                &contract_evidence,
                &[
                    &["required_contract_blocker_codes"][..],
                    &["previous_wrapper_contract_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        preflight_check(
            "adapter_smoke",
            [
                bool_path(&adapter_smoke, &["adapter_smoke_ready"]) == Some(true),
                str_path(&adapter_smoke, &["engine_kind"]) == Some("previous_wrapper"),
                str_path(&adapter_smoke, &["wrapper_identity"]) == Some(wrapper_identity.as_str()),
                u64_path(&adapter_smoke, &["primary_only_checks"]) == Some(0),
            ],
            [
                "adapter_smoke_ready",
                "engine_kind",
                "wrapper_identity",
                "primary_only_checks",
            ],
            blocker_codes(&adapter_smoke, &[&["blocker_codes"][..]]),
        ),
        preflight_check(
            "migration_gate",
            [
                str_path(&migration_gate, &["migration_gate", "decision"]) == Some("ready"),
                str_path(&migration_gate, &["cutover", "decision"]) == Some("ready"),
                bool_path(&migration_gate, &["cutover_evidence", "eligible"]) == Some(true),
                str_path(&migration_gate, &["cutover_evidence", "ready_engine_kind"])
                    == Some("previous_wrapper"),
                str_path(
                    &migration_gate,
                    &["cutover_evidence", "ready_wrapper_identity"],
                ) == Some(wrapper_identity.as_str()),
                bool_path(
                    &migration_gate,
                    &["previous_wrapper_contract_evidence", "ready"],
                ) == Some(true),
                u64_path(&migration_gate, &["replacement_readiness_per_million"])
                    == Some(1_000_000),
            ],
            [
                "migration_gate.decision",
                "cutover.decision",
                "cutover_evidence.eligible",
                "cutover_evidence.ready_engine_kind",
                "cutover_evidence.ready_wrapper_identity",
                "previous_wrapper_contract_evidence.ready",
                "replacement_readiness_per_million",
            ],
            blocker_codes(
                &migration_gate,
                &[
                    &["migration_gate", "blockers"][..],
                    &["cutover", "blockers"][..],
                    &["cutover_evidence", "blockers"][..],
                ],
            ),
        ),
        preflight_check(
            "replacement_summary",
            [
                bool_path(&replacement_summary, &["production_cutover_ready"]) == Some(true),
                u64_path(
                    &replacement_summary,
                    &["production_replacement_per_million"],
                ) == Some(1_000_000),
                empty_array_path(&replacement_summary, &["blocking_categories"]),
                empty_array_path(&replacement_summary, &["missing_evidence"]),
                empty_array_path(&replacement_summary, &["next_actions"]),
            ],
            [
                "production_cutover_ready",
                "production_replacement_per_million",
                "blocking_categories",
                "missing_evidence",
                "next_actions",
            ],
            blocker_codes(
                &replacement_summary,
                &[
                    &["blocking_categories"][..],
                    &["missing_evidence"][..],
                    &["next_actions"][..],
                ],
            ),
        ),
    ];
    let ready = checks.iter().all(|check| {
        check
            .get("ready")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    });
    let failed_checks = checks
        .iter()
        .filter(|check| check.get("ready").and_then(serde_json::Value::as_bool) != Some(true))
        .filter_map(|check| check.get("name").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();

    Ok(serde_json::json!({
        "protocol": "skein-nowledge-previous-wrapper-preflight-check",
        "ready": ready,
        "wrapper_identity": wrapper_identity,
        "failed_checks": failed_checks,
        "checks": checks,
    }))
}

fn preflight_check(
    name: &str,
    conditions: impl IntoIterator<Item = bool>,
    evidence_fields: impl IntoIterator<Item = &'static str>,
    blocker_codes: Vec<String>,
) -> serde_json::Value {
    let ready = conditions.into_iter().all(|condition| condition);
    serde_json::json!({
        "name": name,
        "ready": ready,
        "evidence_fields": evidence_fields.into_iter().collect::<Vec<_>>(),
        "blocker_codes": blocker_codes,
    })
}

fn value_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
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

fn empty_array_path(value: &serde_json::Value, path: &[&str]) -> bool {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn blocker_codes(value: &serde_json::Value, paths: &[&[&str]]) -> Vec<String> {
    let mut codes = Vec::new();
    for path in paths {
        if let Some(items) = value_path(value, path).and_then(serde_json::Value::as_array) {
            for item in items {
                if let Some(code) = item.as_str() {
                    codes.push(code.to_string());
                } else if let Some(action) = item.get("action").and_then(serde_json::Value::as_str)
                {
                    codes.push(action.to_string());
                }
            }
        }
    }
    codes.sort();
    codes.dedup();
    codes
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_previous_wrapper_preflight_check_json, PreviousWrapperPreflightCheckInputs,
    };

    #[test]
    fn preflight_check_reports_ready_when_all_artifacts_are_ready() {
        let report = nowledge_previous_wrapper_preflight_check_json(ready_inputs()).unwrap();

        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        assert_eq!(report["wrapper_identity"], "nowledge-previous-wrapper:test");
        assert!(report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|check| check["ready"] == true));
    }

    #[test]
    fn preflight_check_fails_closed_on_partial_evidence() {
        let mut inputs = ready_inputs();
        inputs.replacement_summary = Some(serde_json::json!({
            "production_cutover_ready": false,
            "production_replacement_per_million": 0,
            "blocking_categories": ["cutover_evidence"],
            "missing_evidence": [],
            "next_actions": [
                {
                    "action": "provide_eligible_cutover_evidence"
                }
            ]
        }));

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary"])
        );
        assert_eq!(
            report["checks"][3]["blocker_codes"],
            serde_json::json!(["cutover_evidence", "provide_eligible_cutover_evidence"])
        );
    }

    #[test]
    fn preflight_check_requires_matching_wrapper_identity() {
        let mut inputs = ready_inputs();
        inputs.wrapper_identity = Some("nowledge-previous-wrapper:other".to_string());

        let report = nowledge_previous_wrapper_preflight_check_json(inputs).unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["full_contract", "adapter_smoke", "migration_gate"])
        );
    }

    fn ready_inputs() -> PreviousWrapperPreflightCheckInputs {
        PreviousWrapperPreflightCheckInputs {
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
            contract_evidence: Some(serde_json::json!({
                "required_contract_ready": true,
                "previous_wrapper_contract_evidence": {
                    "ready": true,
                    "wrapper_identity": "nowledge-previous-wrapper:test",
                    "blocker_codes": []
                },
                "required_contract_blocker_codes": []
            })),
            adapter_smoke: Some(serde_json::json!({
                "adapter_smoke_ready": true,
                "engine_kind": "previous_wrapper",
                "wrapper_identity": "nowledge-previous-wrapper:test",
                "primary_only_checks": 0,
                "blocker_codes": []
            })),
            migration_gate: Some(serde_json::json!({
                "migration_gate": {
                    "decision": "ready",
                    "blockers": []
                },
                "cutover": {
                    "decision": "ready",
                    "blockers": []
                },
                "cutover_evidence": {
                    "eligible": true,
                    "ready_engine_kind": "previous_wrapper",
                    "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                    "blockers": []
                },
                "previous_wrapper_contract_evidence": {
                    "ready": true
                },
                "replacement_readiness_per_million": 1_000_000
            })),
            replacement_summary: Some(serde_json::json!({
                "production_cutover_ready": true,
                "production_replacement_per_million": 1_000_000,
                "blocking_categories": [],
                "missing_evidence": [],
                "next_actions": []
            })),
        }
    }
}
