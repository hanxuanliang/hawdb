use skein::{
    nowledge_mem_bounded_read_evidence_json, NowledgeMemGraphMode, NowledgeMemReadReport, Result,
    SkeinError,
};
use std::path::Path;

pub fn nowledge_bounded_read_evidence_usage() -> String {
    "nowledge-bounded-read-evidence requires [--require-ready] <read-report-json>".to_string()
}

pub fn run_nowledge_bounded_read_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(nowledge_bounded_read_evidence_usage()));
                }
                let report = parse_read_report_json(&read_json_file(Path::new(path))?)?;
                return Ok((
                    nowledge_mem_bounded_read_evidence_json(&report),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(nowledge_bounded_read_evidence_usage()))
}

fn parse_read_report_json(value: &serde_json::Value) -> Result<NowledgeMemReadReport> {
    Ok(NowledgeMemReadReport {
        protocol: required_string(value, "protocol")?.to_string(),
        mode: parse_mode(required_string(value, "mode")?)?,
        row_count: required_usize(value, "row_count")?,
        max_rows: optional_usize(value, "max_rows")?,
        execution_row_cap: optional_usize(value, "execution_row_cap")?,
        estimated_payload_bytes: required_usize(value, "estimated_payload_bytes")?,
        max_estimated_payload_bytes: optional_usize(value, "max_estimated_payload_bytes")?,
        row_budget_exceeded: required_bool(value, "row_budget_exceeded")?,
        payload_budget_exceeded: required_bool(value, "payload_budget_exceeded")?,
        row_limit_enforced_before_output: required_bool(value, "row_limit_enforced_before_output")?,
        operator_row_cap_enabled: required_bool(value, "operator_row_cap_enabled")?,
        blocking_operator_count: required_usize(value, "blocking_operator_count")?,
        blocking_operator_kinds: required_string_array(value, "blocking_operator_kinds")?,
        streaming: required_bool(value, "streaming")?,
    })
}

fn parse_mode(value: &str) -> Result<NowledgeMemGraphMode> {
    match value {
        "shadow_read_only" => Ok(NowledgeMemGraphMode::ShadowReadOnly),
        "writable_cutover" => Ok(NowledgeMemGraphMode::WritableCutover),
        _ => Err(SkeinError::Semantic(format!(
            "invalid read report mode: {value}"
        ))),
    }
}

fn required_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid_field(field, "string"))
}

fn required_bool(value: &serde_json::Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| invalid_field(field, "boolean"))
}

fn required_usize(value: &serde_json::Value, field: &str) -> Result<usize> {
    optional_usize(value, field)?.ok_or_else(|| invalid_field(field, "integer"))
}

fn optional_usize(value: &serde_json::Value, field: &str) -> Result<Option<usize>> {
    let Some(value) = value.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let raw = value
        .as_u64()
        .ok_or_else(|| invalid_field(field, "integer"))?;
    usize::try_from(raw).map(Some).map_err(|_| {
        SkeinError::Semantic(format!("read report field '{field}' exceeds usize range"))
    })
}

fn required_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid_field(field, "string array"))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid_field(field, "string array"))
        })
        .collect()
}

fn invalid_field(field: &str, expected: &str) -> SkeinError {
    SkeinError::Semantic(format!("read report field '{field}' must be a {expected}"))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!("failed to read bounded read report JSON: {error}"))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!("failed to parse bounded read report JSON: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_bounded_read_evidence;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn bounded_read_evidence_command_accepts_ready_read_report() {
        let path = unique_test_file("bounded_read_ready");
        std::fs::write(&path, ready_report().to_string()).unwrap();

        let (evidence, require_ready) = run_nowledge_bounded_read_evidence(
            ["--require-ready", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-mem-bounded-read-evidence-v1"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["mode"], "shadow_read_only");
        assert_eq!(evidence["execution_row_cap"], 513);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn bounded_read_evidence_command_fails_closed_for_incomplete_report() {
        let path = unique_test_file("bounded_read_incomplete");
        let mut report = ready_report();
        report.as_object_mut().unwrap().remove("execution_row_cap");
        std::fs::write(&path, report.to_string()).unwrap();

        let (evidence, _) = run_nowledge_bounded_read_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!(["missing_execution_row_cap"])
        );
        std::fs::remove_file(path).unwrap();
    }

    fn ready_report() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-read-report",
            "mode": "shadow_read_only",
            "row_count": 4,
            "max_rows": 512,
            "execution_row_cap": 513,
            "estimated_payload_bytes": 128,
            "max_estimated_payload_bytes": 4194304,
            "row_budget_exceeded": false,
            "payload_budget_exceeded": false,
            "row_limit_enforced_before_output": true,
            "operator_row_cap_enabled": true,
            "blocking_operator_count": 0,
            "blocking_operator_kinds": [],
            "streaming": false
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
