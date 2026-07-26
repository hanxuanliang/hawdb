use crate::{
    nowledge_mem_search_candidate_shadow_evidence_json,
    NowledgeMemSearchCandidateShadowAccumulator, Result, SkeinError,
};
use std::path::Path;

pub fn nowledge_search_candidate_shadow_evidence_usage() -> String {
    "nowledge-search-candidate-shadow-evidence requires [--require-ready] <candidate-shadow-probe-json>".to_string()
}

pub fn run_nowledge_search_candidate_shadow_evidence(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut probe_path = None;
    for arg in args {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(
                    nowledge_search_candidate_shadow_evidence_usage(),
                ));
            }
            path => {
                if probe_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_search_candidate_shadow_evidence_usage(),
                    ));
                }
            }
        }
    }
    let Some(probe_path) = probe_path else {
        return Err(SkeinError::Semantic(
            nowledge_search_candidate_shadow_evidence_usage(),
        ));
    };
    let probe = read_json_file(Path::new(&probe_path))?;
    let accumulator = parse_search_candidate_shadow_probe(&probe)?;
    Ok((
        nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence()),
        require_ready,
    ))
}

pub fn parse_search_candidate_shadow_probe(
    value: &serde_json::Value,
) -> Result<NowledgeMemSearchCandidateShadowAccumulator> {
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    let requests = value
        .get("requests")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid_field("requests", "array"))?;
    for request in requests {
        let primary_candidate_ids = required_string_array(request, "primary_candidate_ids")?;
        let shadow_candidate_ids = required_string_array(request, "shadow_candidate_ids")?;
        accumulator.record_compare_candidate_ids(&primary_candidate_ids, &shadow_candidate_ids);
    }
    let filter_pushdown = value
        .get("filter_pushdown")
        .ok_or_else(|| invalid_field("filter_pushdown", "object"))?;
    accumulator.record_filter_pushdown_fields(
        required_u64(filter_pushdown, "pushed_predicate_count")?,
        required_string_array(filter_pushdown, "fields")?,
    );
    for blocker in optional_string_array(value, "blocker_codes")? {
        accumulator.add_blocker_code(blocker);
    }
    Ok(accumulator)
}

fn required_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid_field(field, "string array"))?;
    string_array_items(items, field)
}

fn optional_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let Some(items) = value.get(field) else {
        return Ok(Vec::new());
    };
    let items = items
        .as_array()
        .ok_or_else(|| invalid_field(field, "string array"))?;
    string_array_items(items, field)
}

fn string_array_items(items: &[serde_json::Value], field: &str) -> Result<Vec<String>> {
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid_field(field, "string array"))
        })
        .collect()
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| invalid_field(field, "integer"))
}

fn invalid_field(field: &str, expected: &str) -> SkeinError {
    SkeinError::Semantic(format!(
        "search candidate shadow probe field '{field}' must be a {expected}"
    ))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read search candidate shadow probe JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse search candidate shadow probe JSON: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        parse_search_candidate_shadow_probe, run_nowledge_search_candidate_shadow_evidence,
    };
    use crate::{
        nowledge_mem_search_candidate_shadow_evidence_json,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
    };
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn search_candidate_shadow_evidence_command_accepts_ready_probe() {
        let path = unique_test_file("search_candidate_shadow_probe");
        std::fs::write(&path, ready_probe().to_string()).unwrap();

        let (evidence, require_ready) = run_nowledge_search_candidate_shadow_evidence(
            ["--require-ready", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-search-candidate-shadow-evidence"
        );
        assert_eq!(
            evidence["route"],
            "/search-index/skein-shadow/candidate-evidence"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["primary_candidate_count"], 3);
        assert_eq!(evidence["shadow_candidate_count"], 3);
        assert_eq!(evidence["matched_candidate_count"], 3);
        assert_eq!(evidence["primary_only_candidate_count"], 0);
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
        assert_eq!(
            evidence["filter_pushdown"]["field_summary_count"],
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len()
        );
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn search_candidate_shadow_probe_parser_is_available_to_library_callers() {
        let accumulator = parse_search_candidate_shadow_probe(&ready_probe()).unwrap();
        let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
    }

    #[test]
    fn search_candidate_shadow_evidence_command_fails_closed_for_mismatch() {
        let path = unique_test_file("search_candidate_shadow_probe_mismatch");
        let mut probe = ready_probe();
        probe["requests"][0]["shadow_candidate_ids"] = serde_json::json!(["mem_1"]);
        std::fs::write(&path, probe.to_string()).unwrap();

        let (evidence, _) = run_nowledge_search_candidate_shadow_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["candidate_identity"]["ready"], false);
        assert!(evidence["primary_only_candidate_count"].as_u64().unwrap() > 0);
        std::fs::remove_file(path).unwrap();
    }

    fn ready_probe() -> serde_json::Value {
        serde_json::json!({
            "requests": [
                {
                    "primary_candidate_ids": ["mem_1", "mem_2"],
                    "shadow_candidate_ids": ["mem_1", "mem_2"]
                },
                {
                    "primary_candidate_ids": ["mem_3"],
                    "shadow_candidate_ids": ["mem_3"]
                }
            ],
            "filter_pushdown": {
                "pushed_predicate_count": 1,
                "fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            }
        })
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}.json", std::process::id()))
    }
}
