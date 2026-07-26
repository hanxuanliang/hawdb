use skein::{Result, SkeinError};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const BLACKBOX_REPORT_PROTOCOL: &str = "skein-blackbox-report-v1";
const BLACKBOX_EVENT_PROTOCOL: &str = "skein-blackbox-event-v1";

const KNOWN_ARTIFACTS: &[&str] = &[
    "contract.json",
    "contract-evidence.json",
    "previous-wrapper-contract-evidence.json",
    "adapter-smoke.json",
    "adapter-shadow.jsonl",
    "slow-query-log.jsonl",
    "skein-log.jsonl",
    "skein-demo.out",
    "storage-recovery.json",
    "storage-recovery-evidence.json",
    "background-maintenance.json",
    "background-maintenance-evidence.json",
    "migration-shadow.jsonl",
    "migration-gate.json",
    "query-family-evidence.json",
    "graph-route-evidence.json",
    "graph-route-readiness.json",
    "bounded-read-report.json",
    "bounded-read-evidence.json",
    "search-projection-evidence.json",
    "search-projection-shadow-evidence.json",
    "search-candidate-shadow-evidence.json",
    "query-runtime-preflight.json",
    "replacement-summary.json",
    "library-readiness.json",
    "preflight-check.json",
    "integration-bundle.json",
];

pub fn nowledge_blackbox_report_usage() -> String {
    "nowledge-blackbox-report requires --artifact-dir <dir> --output-dir <dir> [--run-id <id>] [--run-status completed|failed|running] [--exit-code <n>]".to_string()
}

pub fn run_nowledge_blackbox_report(
    args: impl IntoIterator<Item = String>,
) -> Result<serde_json::Value> {
    let mut artifact_dir = None;
    let mut output_dir = None;
    let mut run_id = None;
    let mut run_status = "completed".to_string();
    let mut exit_code = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--artifact-dir" => {
                artifact_dir =
                    Some(PathBuf::from(iter.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_blackbox_report_usage())
                    })?));
            }
            "--output-dir" => {
                output_dir =
                    Some(PathBuf::from(iter.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_blackbox_report_usage())
                    })?));
            }
            "--run-id" => {
                run_id = Some(
                    iter.next()
                        .ok_or_else(|| SkeinError::Semantic(nowledge_blackbox_report_usage()))?,
                );
            }
            "--run-status" => {
                run_status = iter
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_blackbox_report_usage()))?;
                if !matches!(run_status.as_str(), "completed" | "failed" | "running") {
                    return Err(SkeinError::Semantic(nowledge_blackbox_report_usage()));
                }
            }
            "--exit-code" => {
                let raw = iter
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_blackbox_report_usage()))?;
                exit_code = Some(
                    raw.parse::<i64>()
                        .map_err(|_| SkeinError::Semantic(nowledge_blackbox_report_usage()))?,
                );
            }
            "--help" | "-h" => {
                return Err(SkeinError::Semantic(nowledge_blackbox_report_usage()));
            }
            _ => return Err(SkeinError::Semantic(nowledge_blackbox_report_usage())),
        }
    }

    let artifact_dir =
        artifact_dir.ok_or_else(|| SkeinError::Semantic(nowledge_blackbox_report_usage()))?;
    let output_dir =
        output_dir.ok_or_else(|| SkeinError::Semantic(nowledge_blackbox_report_usage()))?;
    if !artifact_dir.is_dir() {
        return Err(SkeinError::Semantic(
            "--artifact-dir does not exist or is not a directory".to_string(),
        ));
    }
    fs::create_dir_all(&output_dir)?;

    let generated_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SkeinError::Execution(format!("system clock before unix epoch: {error}")))?
        .as_secs();
    let run_id = run_id.unwrap_or_else(|| format!("skein-blackbox-{generated_unix_seconds}"));
    let artifacts = collect_blackbox_artifacts(&artifact_dir)?;

    let events_path = output_dir.join("events.jsonl");
    write_blackbox_events(&events_path, &run_id, &artifacts)?;

    let manifest = serde_json::json!({
        "protocol": BLACKBOX_REPORT_PROTOCOL,
        "protocol_version": 1,
        "run_id": run_id,
        "run_status": run_status,
        "exit_code": exit_code,
        "generated_unix_seconds": generated_unix_seconds,
        "artifact_dir_present": true,
        "artifact_count": artifacts.len(),
        "events_path": "events.jsonl",
        "artifacts": artifacts,
        "redaction": {
            "raw_query_text_copied": false,
            "raw_parameters_copied": false,
            "raw_artifact_payloads_copied": false,
            "artifact_paths_are_relative": true
        }
    });
    let manifest_path = output_dir.join("manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| SkeinError::Execution(format!("blackbox manifest JSON error: {error}")))?;
    fs::write(&manifest_path, manifest_bytes)?;
    Ok(manifest)
}

fn collect_blackbox_artifacts(artifact_dir: &Path) -> Result<Vec<serde_json::Value>> {
    let mut artifacts = Vec::new();
    for artifact_name in KNOWN_ARTIFACTS {
        let path = artifact_dir.join(artifact_name);
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path)?;
        let mut artifact = BTreeMap::new();
        artifact.insert(
            "name".to_string(),
            serde_json::Value::String((*artifact_name).to_string()),
        );
        artifact.insert(
            "format".to_string(),
            serde_json::Value::String(artifact_format(artifact_name).to_string()),
        );
        artifact.insert("byte_len".to_string(), serde_json::json!(bytes.len()));
        artifact.insert(
            "checksum".to_string(),
            serde_json::json!(checksum_bytes(&bytes)),
        );
        if artifact_name.ends_with(".json") {
            artifact.insert("json".to_string(), json_artifact_summary(&bytes));
        } else if artifact_name.ends_with(".jsonl") {
            artifact.insert("jsonl".to_string(), jsonl_artifact_summary(&bytes));
        }
        artifacts.push(serde_json::Value::Object(artifact.into_iter().collect()));
    }
    Ok(artifacts)
}

fn artifact_format(name: &str) -> &'static str {
    if name.ends_with(".json") {
        "json"
    } else if name.ends_with(".jsonl") {
        "jsonl"
    } else {
        "opaque"
    }
}

fn json_artifact_summary(bytes: &[u8]) -> serde_json::Value {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return serde_json::json!({
            "parse_ready": false,
            "parse_error_kind": "invalid_json"
        });
    };
    serde_json::json!({
        "parse_ready": true,
        "protocol": string_field(&value, "protocol"),
        "ready": first_bool_field(&value, &[
            "ready",
            "production_cutover_ready",
            "route_primary_ready",
            "required_contract_ready",
            "library_ready",
            "integration_ready"
        ]),
        "decision": string_field(&value, "decision"),
        "blocker_codes": string_array_field(&value, "blocker_codes"),
        "blocking_categories": string_array_field(&value, "blocking_categories"),
        "failed_checks": string_array_field(&value, "failed_checks"),
        "missing_evidence": string_array_field(&value, "missing_evidence"),
        "production_cutover_ready": value
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool),
        "production_replacement_per_million": value
            .get("production_replacement_per_million")
            .and_then(serde_json::Value::as_u64),
    })
}

fn jsonl_artifact_summary(bytes: &[u8]) -> serde_json::Value {
    let text = String::from_utf8_lossy(bytes);
    let line_count = text.lines().count();
    let nonempty_line_count = text.lines().filter(|line| !line.trim().is_empty()).count();
    serde_json::json!({
        "line_count": line_count,
        "nonempty_line_count": nonempty_line_count,
    })
}

fn string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn first_bool_field(value: &serde_json::Value, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_bool))
}

fn string_array_field(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn write_blackbox_events(
    events_path: &Path,
    run_id: &str,
    artifacts: &[serde_json::Value],
) -> Result<()> {
    let mut file = fs::File::create(events_path)?;
    for (index, artifact) in artifacts.iter().enumerate() {
        let event = serde_json::json!({
            "protocol": BLACKBOX_EVENT_PROTOCOL,
            "protocol_version": 1,
            "run_id": run_id,
            "sequence": index + 1,
            "event": "artifact_observed",
            "artifact": artifact,
        });
        let event_line = serde_json::to_string(&event).map_err(|error| {
            SkeinError::Execution(format!("blackbox event JSON error: {error}"))
        })?;
        writeln!(file, "{event_line}")?;
    }
    Ok(())
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn blackbox_report_writes_redacted_manifest_and_events() {
        let root = unique_test_dir("blackbox-report");
        let artifact_dir = root.join("artifacts");
        let output_dir = root.join("blackbox");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("query-runtime-preflight.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-nowledge-query-runtime-preflight-v1",
                "ready": false,
                "blocker_codes": ["query_runtime_probe_failed"],
                "query": "MATCH (secret {token: $token}) RETURN secret",
                "parameters": {"token": "secret-token"}
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            artifact_dir.join("adapter-shadow.jsonl"),
            "{\"event\":\"request\",\"payload\":{\"query\":\"secret\"}}\n",
        )
        .unwrap();
        fs::write(
            artifact_dir.join("slow-query-log.jsonl"),
            "{\"event\":\"slow_query\",\"digest\":\"digest-1\"}\n",
        )
        .unwrap();

        let manifest = run_nowledge_blackbox_report(vec![
            "--artifact-dir".to_string(),
            artifact_dir.display().to_string(),
            "--output-dir".to_string(),
            output_dir.display().to_string(),
            "--run-id".to_string(),
            "run-1".to_string(),
            "--run-status".to_string(),
            "failed".to_string(),
            "--exit-code".to_string(),
            "7".to_string(),
        ])
        .unwrap();

        assert_eq!(manifest["protocol"], BLACKBOX_REPORT_PROTOCOL);
        assert_eq!(manifest["run_id"], "run-1");
        assert_eq!(manifest["run_status"], "failed");
        assert_eq!(manifest["exit_code"], 7);
        assert_eq!(manifest["artifact_count"], 3);
        let rendered = serde_json::to_string(&manifest).unwrap();
        assert!(!rendered.contains("MATCH"));
        assert!(!rendered.contains("secret-token"));
        assert!(rendered.contains("query_runtime_probe_failed"));
        assert!(output_dir.join("manifest.json").is_file());
        let events = fs::read_to_string(output_dir.join("events.jsonl")).unwrap();
        assert_eq!(events.lines().count(), 3);
        assert!(events.contains(BLACKBOX_EVENT_PROTOCOL));
        assert!(events.contains("slow-query-log.jsonl"));
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("skein-{prefix}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }
}
