use crate::search::SearchMode;
use crate::{
    nowledge_mem_search_candidate_shadow_evidence_json, NowledgeMemSearchCandidateFieldSummary,
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
    let pushed_predicate_count = required_u64(filter_pushdown, "pushed_predicate_count")?;
    if let Some(summaries) = optional_field_summaries(filter_pushdown)? {
        accumulator.record_filter_pushdown_summaries(pushed_predicate_count, true, summaries);
    } else {
        accumulator.record_filter_pushdown_fields(
            pushed_predicate_count,
            required_string_array(filter_pushdown, "fields")?,
        );
    }
    let retriever_legs = required_object(value, "retriever_legs")?;
    parse_retriever_leg(retriever_legs, "text", &mut accumulator)?;
    parse_retriever_leg(retriever_legs, "vector", &mut accumulator)?;
    let top_k_overlap = required_object(value, "top_k_overlap")?;
    parse_top_k_overlap(top_k_overlap, "fts", SearchMode::Text, &mut accumulator)?;
    parse_top_k_overlap(
        top_k_overlap,
        "vector",
        SearchMode::Vector,
        &mut accumulator,
    )?;
    for blocker in optional_string_array(value, "blocker_codes")? {
        accumulator.add_blocker_code(blocker);
    }
    Ok(accumulator)
}

fn parse_top_k_overlap(
    value: &serde_json::Value,
    name: &'static str,
    mode: SearchMode,
    accumulator: &mut NowledgeMemSearchCandidateShadowAccumulator,
) -> Result<()> {
    let top_k = required_object(value, name)?;
    accumulator.record_top_k_overlap_candidate_ids(
        mode,
        &required_string_array(top_k, "primary_candidate_ids")?,
        &required_string_array(top_k, "shadow_candidate_ids")?,
    );
    Ok(())
}

fn parse_retriever_leg(
    value: &serde_json::Value,
    name: &'static str,
    accumulator: &mut NowledgeMemSearchCandidateShadowAccumulator,
) -> Result<()> {
    let leg = required_object(value, name)?;
    let available = required_bool(leg, "available")?;
    let candidate_count = required_u64(leg, "candidate_count")?;
    accumulator.record_retriever_leg(name, available, candidate_count);
    if available && candidate_count > 0 {
        return Ok(());
    }
    accumulator.add_blocker_code(format!("search_candidate_{name}_retriever_unavailable"));
    Ok(())
}

fn optional_field_summaries(
    value: &serde_json::Value,
) -> Result<Option<Vec<NowledgeMemSearchCandidateFieldSummary>>> {
    let Some(items) = value.get("field_summaries") else {
        return Ok(None);
    };
    let items = items
        .as_array()
        .ok_or_else(|| invalid_field("field_summaries", "object array"))?;
    items
        .iter()
        .map(parse_field_summary)
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn parse_field_summary(
    value: &serde_json::Value,
) -> Result<NowledgeMemSearchCandidateFieldSummary> {
    Ok(NowledgeMemSearchCandidateFieldSummary {
        field: required_string(value, "field")?,
        source: value
            .get("source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("search_candidate_shadow_probe")
            .to_string(),
        segment_count: optional_usize(value, "segment_count")?.unwrap_or(1),
        value_summary_used: optional_bool(value, "value_summary_used")?.unwrap_or(false),
        value_summary_segment_count: optional_usize(value, "value_summary_segment_count")?
            .unwrap_or(0),
        numeric_range_summary_used: optional_bool(value, "numeric_range_summary_used")?
            .unwrap_or(false),
        numeric_range_segment_count: optional_usize(value, "numeric_range_segment_count")?
            .unwrap_or(0),
        timestamp_range_summary_used: optional_bool(value, "timestamp_range_summary_used")?
            .unwrap_or(false),
        timestamp_range_segment_count: optional_usize(value, "timestamp_range_segment_count")?
            .unwrap_or(0),
    })
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

fn required_string(value: &serde_json::Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| invalid_field(field, "string"))
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| invalid_field(field, "integer"))
}

fn required_bool(value: &serde_json::Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| invalid_field(field, "boolean"))
}

fn required_object<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a serde_json::Value> {
    value
        .get(field)
        .filter(|raw| raw.is_object())
        .ok_or_else(|| invalid_field(field, "object"))
}

fn optional_bool(value: &serde_json::Value, field: &str) -> Result<Option<bool>> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    raw.as_bool()
        .map(Some)
        .ok_or_else(|| invalid_field(field, "boolean"))
}

fn optional_usize(value: &serde_json::Value, field: &str) -> Result<Option<usize>> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    let raw = raw
        .as_u64()
        .ok_or_else(|| invalid_field(field, "integer"))?;
    usize::try_from(raw)
        .map(Some)
        .map_err(|_| invalid_field(field, "usize-sized integer"))
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
        assert_eq!(evidence["text_retriever_ready"], true);
        assert_eq!(evidence["vector_retriever_ready"], true);
        assert_eq!(evidence["retriever_leg_candidate_counts"]["text"], 3);
        assert_eq!(evidence["retriever_leg_candidate_counts"]["vector"], 3);
        assert_eq!(evidence["fts_top_k_overlap_ready"], true);
        assert_eq!(evidence["vector_top_k_overlap_ready"], true);
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
        assert_eq!(evidence["text_retriever_ready"], true);
        assert_eq!(evidence["vector_retriever_ready"], true);
        assert_eq!(evidence["fts_top_k_overlap_ready"], true);
        assert_eq!(evidence["vector_top_k_overlap_ready"], true);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
    }

    #[test]
    fn search_candidate_shadow_probe_accepts_structured_field_summaries() {
        let accumulator = parse_search_candidate_shadow_probe(&ready_structured_probe()).unwrap();
        let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert!(evidence["filter_pushdown"]["field_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary["field"] == "importance"
                && summary["source"] == "search_candidate_shadow_probe"
                && summary["numeric_range_segment_count"] == 1));
    }

    #[test]
    fn search_candidate_shadow_probe_rejects_zero_summary_counts() {
        let mut probe = ready_structured_probe();
        let summaries = probe["filter_pushdown"]["field_summaries"]
            .as_array_mut()
            .unwrap();
        let lifecycle_state = summaries
            .iter_mut()
            .find(|summary| summary["field"] == "lifecycle_state")
            .unwrap();
        lifecycle_state["value_summary_used"] = serde_json::json!(true);
        lifecycle_state["value_summary_segment_count"] = serde_json::json!(0);

        let accumulator = parse_search_candidate_shadow_probe(&probe).unwrap();
        let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            false
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_value_summary_fields"],
            serde_json::json!(["lifecycle_state"])
        );
        assert!(evidence["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_field_pruning_capability_missing"));
    }

    #[test]
    fn search_candidate_shadow_probe_requires_retriever_leg_evidence() {
        let mut probe = ready_probe();
        probe.as_object_mut().unwrap().remove("retriever_legs");

        let error = parse_search_candidate_shadow_probe(&probe).unwrap_err();

        assert_eq!(
            error.to_string(),
            "semantic error: search candidate shadow probe field 'retriever_legs' must be a object"
        );
    }

    #[test]
    fn search_candidate_shadow_probe_requires_top_k_overlap_evidence() {
        let mut probe = ready_probe();
        probe.as_object_mut().unwrap().remove("top_k_overlap");

        let error = parse_search_candidate_shadow_probe(&probe).unwrap_err();

        assert_eq!(
            error.to_string(),
            "semantic error: search candidate shadow probe field 'top_k_overlap' must be a object"
        );
    }

    #[test]
    fn search_candidate_shadow_probe_fails_closed_for_unavailable_retriever_leg() {
        let mut probe = ready_probe();
        probe["retriever_legs"]["vector"]["available"] = serde_json::json!(false);
        probe["retriever_legs"]["vector"]["candidate_count"] = serde_json::json!(0);

        let accumulator = parse_search_candidate_shadow_probe(&probe).unwrap();
        let evidence = nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["text_retriever_ready"], true);
        assert_eq!(evidence["vector_retriever_ready"], false);
        assert!(evidence["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_vector_retriever_unavailable"));
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
            },
            "retriever_legs": {
                "text": {
                    "available": true,
                    "candidate_count": 3
                },
                "vector": {
                    "available": true,
                    "candidate_count": 3
                }
            },
            "top_k_overlap": {
                "fts": {
                    "primary_candidate_ids": ["mem_1", "mem_2"],
                    "shadow_candidate_ids": ["mem_1", "mem_2"]
                },
                "vector": {
                    "primary_candidate_ids": ["mem_3"],
                    "shadow_candidate_ids": ["mem_3"]
                }
            }
        })
    }

    fn ready_structured_probe() -> serde_json::Value {
        let field_summaries = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .map(|field| {
                serde_json::json!({
                    "field": field,
                    "source": "search_candidate_shadow_probe",
                    "segment_count": 1,
                    "value_summary_used": true,
                    "value_summary_segment_count": 1,
                    "numeric_range_summary_used": matches!(*field, "importance" | "confidence"),
                    "numeric_range_segment_count": usize::from(matches!(
                        *field,
                        "importance" | "confidence"
                    )),
                    "timestamp_range_summary_used": matches!(
                        *field,
                        "created_at" | "updated_at" | "event_start" | "event_end"
                    ),
                    "timestamp_range_segment_count": usize::from(matches!(
                        *field,
                        "created_at" | "updated_at" | "event_start" | "event_end"
                    )),
                })
            })
            .collect::<Vec<_>>();
        let mut probe = ready_probe();
        probe["filter_pushdown"]
            .as_object_mut()
            .unwrap()
            .remove("fields");
        probe["filter_pushdown"]["field_summaries"] = serde_json::json!(field_summaries);
        probe
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}.json", std::process::id()))
    }
}
