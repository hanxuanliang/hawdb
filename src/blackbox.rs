use crate::{Result, SkeinError};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const BLACKBOX_REPORT_PROTOCOL: &str = "skein-blackbox-report-v1";
pub const BLACKBOX_EVENT_PROTOCOL: &str = "skein-blackbox-event-v1";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxReportOptions {
    pub artifact_dir: PathBuf,
    pub output_dir: PathBuf,
    pub run_id: Option<String>,
    pub run_status: BlackboxRunStatus,
    pub exit_code: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlackboxRunStatus {
    Completed,
    Failed,
    Running,
}

impl BlackboxRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Running => "running",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxRedactionReport {
    pub raw_query_text_copied: bool,
    pub raw_parameters_copied: bool,
    pub raw_artifact_payloads_copied: bool,
    pub artifact_paths_are_relative: bool,
}

impl Default for BlackboxRedactionReport {
    fn default() -> Self {
        Self {
            raw_query_text_copied: false,
            raw_parameters_copied: false,
            raw_artifact_payloads_copied: false,
            artifact_paths_are_relative: true,
        }
    }
}

impl BlackboxRedactionReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "raw_query_text_copied": self.raw_query_text_copied,
            "raw_parameters_copied": self.raw_parameters_copied,
            "raw_artifact_payloads_copied": self.raw_artifact_payloads_copied,
            "artifact_paths_are_relative": self.artifact_paths_are_relative,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxJsonArtifactSummary {
    pub parse_ready: bool,
    pub parse_error_kind: Option<String>,
    pub protocol: Option<String>,
    pub ready: Option<bool>,
    pub decision: Option<String>,
    pub blocker_codes: Vec<String>,
    pub blocking_categories: Vec<String>,
    pub failed_checks: Vec<String>,
    pub missing_evidence: Vec<String>,
    pub production_cutover_ready: Option<bool>,
    pub production_replacement_per_million: Option<u64>,
}

impl BlackboxJsonArtifactSummary {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "parse_ready": self.parse_ready,
            "parse_error_kind": self.parse_error_kind,
            "protocol": self.protocol,
            "ready": self.ready,
            "decision": self.decision,
            "blocker_codes": self.blocker_codes,
            "blocking_categories": self.blocking_categories,
            "failed_checks": self.failed_checks,
            "missing_evidence": self.missing_evidence,
            "production_cutover_ready": self.production_cutover_ready,
            "production_replacement_per_million": self.production_replacement_per_million,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxJsonlArtifactSummary {
    pub line_count: usize,
    pub nonempty_line_count: usize,
}

impl BlackboxJsonlArtifactSummary {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "line_count": self.line_count,
            "nonempty_line_count": self.nonempty_line_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxArtifactReport {
    pub name: String,
    pub format: String,
    pub byte_len: usize,
    pub checksum: u64,
    pub json: Option<BlackboxJsonArtifactSummary>,
    pub jsonl: Option<BlackboxJsonlArtifactSummary>,
}

impl BlackboxArtifactReport {
    pub fn json(&self) -> serde_json::Value {
        let mut artifact = serde_json::Map::new();
        artifact.insert("name".to_string(), serde_json::json!(self.name));
        artifact.insert("format".to_string(), serde_json::json!(self.format));
        artifact.insert("byte_len".to_string(), serde_json::json!(self.byte_len));
        artifact.insert("checksum".to_string(), serde_json::json!(self.checksum));
        if let Some(summary) = self.json.as_ref() {
            artifact.insert("json".to_string(), summary.json());
        }
        if let Some(summary) = self.jsonl.as_ref() {
            artifact.insert("jsonl".to_string(), summary.json());
        }
        serde_json::Value::Object(artifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxEventReport {
    pub protocol: String,
    pub protocol_version: u64,
    pub run_id: String,
    pub sequence: usize,
    pub event: String,
    pub artifact: BlackboxArtifactReport,
}

impl BlackboxEventReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "event": self.event,
            "artifact": self.artifact.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackboxReport {
    pub protocol: String,
    pub protocol_version: u64,
    pub run_id: String,
    pub run_status: BlackboxRunStatus,
    pub exit_code: Option<i64>,
    pub generated_unix_seconds: u64,
    pub artifact_dir_present: bool,
    pub artifact_count: usize,
    pub events_path: String,
    pub artifacts: Vec<BlackboxArtifactReport>,
    pub redaction: BlackboxRedactionReport,
}

impl BlackboxReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "run_id": self.run_id,
            "run_status": self.run_status.as_str(),
            "exit_code": self.exit_code,
            "generated_unix_seconds": self.generated_unix_seconds,
            "artifact_dir_present": self.artifact_dir_present,
            "artifact_count": self.artifact_count,
            "events_path": self.events_path,
            "artifacts": self.artifacts.iter().map(BlackboxArtifactReport::json).collect::<Vec<_>>(),
            "redaction": self.redaction.json(),
        })
    }

    pub fn events(&self) -> Vec<BlackboxEventReport> {
        self.artifacts
            .iter()
            .enumerate()
            .map(|(index, artifact)| BlackboxEventReport {
                protocol: BLACKBOX_EVENT_PROTOCOL.to_string(),
                protocol_version: 1,
                run_id: self.run_id.clone(),
                sequence: index + 1,
                event: "artifact_observed".to_string(),
                artifact: artifact.clone(),
            })
            .collect()
    }
}

impl std::str::FromStr for BlackboxRunStatus {
    type Err = SkeinError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "running" => Ok(Self::Running),
            _ => Err(SkeinError::Semantic(
                "blackbox run status must be completed, failed, or running".to_string(),
            )),
        }
    }
}

pub fn blackbox_report(options: &BlackboxReportOptions) -> Result<BlackboxReport> {
    if !options.artifact_dir.is_dir() {
        return Err(SkeinError::Semantic(
            "blackbox artifact_dir does not exist or is not a directory".to_string(),
        ));
    }
    let generated_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SkeinError::Execution(format!("system clock before unix epoch: {error}")))?
        .as_secs();
    let run_id = options
        .run_id
        .clone()
        .unwrap_or_else(|| format!("skein-blackbox-{generated_unix_seconds}"));
    let artifacts = collect_blackbox_artifacts(&options.artifact_dir)?;
    Ok(BlackboxReport {
        protocol: BLACKBOX_REPORT_PROTOCOL.to_string(),
        protocol_version: 1,
        run_id,
        run_status: options.run_status,
        exit_code: options.exit_code,
        generated_unix_seconds,
        artifact_dir_present: true,
        artifact_count: artifacts.len(),
        events_path: "events.jsonl".to_string(),
        artifacts,
        redaction: BlackboxRedactionReport::default(),
    })
}

pub fn blackbox_report_json(options: &BlackboxReportOptions) -> Result<serde_json::Value> {
    Ok(blackbox_report(options)?.json())
}

pub fn write_blackbox_report(options: &BlackboxReportOptions) -> Result<serde_json::Value> {
    Ok(write_blackbox_report_typed(options)?.json())
}

pub fn write_blackbox_report_typed(options: &BlackboxReportOptions) -> Result<BlackboxReport> {
    fs::create_dir_all(&options.output_dir)?;
    let manifest = blackbox_report(options)?;
    write_blackbox_events(&options.output_dir.join("events.jsonl"), &manifest.events())?;
    let manifest_json = manifest.json();
    let manifest_bytes = serde_json::to_vec_pretty(&manifest_json)
        .map_err(|error| SkeinError::Execution(format!("blackbox manifest JSON error: {error}")))?;
    fs::write(options.output_dir.join("manifest.json"), manifest_bytes)?;
    Ok(manifest)
}

fn collect_blackbox_artifacts(artifact_dir: &Path) -> Result<Vec<BlackboxArtifactReport>> {
    let mut artifacts = Vec::new();
    for artifact_name in KNOWN_ARTIFACTS {
        let path = artifact_dir.join(artifact_name);
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path)?;
        let json = artifact_name
            .ends_with(".json")
            .then(|| json_artifact_summary(&bytes));
        let jsonl = artifact_name
            .ends_with(".jsonl")
            .then(|| jsonl_artifact_summary(&bytes));
        artifacts.push(BlackboxArtifactReport {
            name: (*artifact_name).to_string(),
            format: artifact_format(artifact_name).to_string(),
            byte_len: bytes.len(),
            checksum: checksum_bytes(&bytes),
            json,
            jsonl,
        });
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

fn json_artifact_summary(bytes: &[u8]) -> BlackboxJsonArtifactSummary {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return BlackboxJsonArtifactSummary {
            parse_ready: false,
            parse_error_kind: Some("invalid_json".to_string()),
            protocol: None,
            ready: None,
            decision: None,
            blocker_codes: Vec::new(),
            blocking_categories: Vec::new(),
            failed_checks: Vec::new(),
            missing_evidence: Vec::new(),
            production_cutover_ready: None,
            production_replacement_per_million: None,
        };
    };
    BlackboxJsonArtifactSummary {
        parse_ready: true,
        parse_error_kind: None,
        protocol: string_field(&value, "protocol"),
        ready: first_bool_field(
            &value,
            &[
                "ready",
                "production_cutover_ready",
                "route_primary_ready",
                "required_contract_ready",
                "library_ready",
                "integration_ready",
            ],
        ),
        decision: string_field(&value, "decision"),
        blocker_codes: string_array_field(&value, "blocker_codes"),
        blocking_categories: string_array_field(&value, "blocking_categories"),
        failed_checks: string_array_field(&value, "failed_checks"),
        missing_evidence: string_array_field(&value, "missing_evidence"),
        production_cutover_ready: value
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool),
        production_replacement_per_million: value
            .get("production_replacement_per_million")
            .and_then(serde_json::Value::as_u64),
    }
}

fn jsonl_artifact_summary(bytes: &[u8]) -> BlackboxJsonlArtifactSummary {
    let text = String::from_utf8_lossy(bytes);
    let line_count = text.lines().count();
    let nonempty_line_count = text.lines().filter(|line| !line.trim().is_empty()).count();
    BlackboxJsonlArtifactSummary {
        line_count,
        nonempty_line_count,
    }
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

fn write_blackbox_events(events_path: &Path, events: &[BlackboxEventReport]) -> Result<()> {
    let mut file = fs::File::create(events_path)?;
    for event in events {
        let event_line = serde_json::to_string(&event.json()).map_err(|error| {
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

        let manifest = write_blackbox_report(&BlackboxReportOptions {
            artifact_dir,
            output_dir: output_dir.clone(),
            run_id: Some("run-1".to_string()),
            run_status: BlackboxRunStatus::Failed,
            exit_code: Some(7),
        })
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

    #[test]
    fn blackbox_report_typed_api_exposes_redacted_manifest_and_events() {
        let root = unique_test_dir("blackbox-report-typed");
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&artifact_dir).unwrap();
        fs::write(
            artifact_dir.join("replacement-summary.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "protocol": "skein-nowledge-replacement-summary",
                "production_cutover_ready": false,
                "production_replacement_per_million": 500_000,
                "blocking_categories": ["query_runtime_preflight"],
                "query": "MATCH (secret {token: $token}) RETURN secret",
                "parameters": {"token": "secret-token"}
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            artifact_dir.join("skein-log.jsonl"),
            "{\"event\":\"started\"}\n\n",
        )
        .unwrap();

        let report = blackbox_report(&BlackboxReportOptions {
            artifact_dir,
            output_dir: root.join("blackbox"),
            run_id: Some("run-typed".to_string()),
            run_status: BlackboxRunStatus::Running,
            exit_code: None,
        })
        .unwrap();

        assert_eq!(report.protocol, BLACKBOX_REPORT_PROTOCOL);
        assert_eq!(report.run_id, "run-typed");
        assert_eq!(report.run_status, BlackboxRunStatus::Running);
        assert_eq!(report.artifact_count, 2);
        assert!(report.redaction.artifact_paths_are_relative);
        assert!(!report.redaction.raw_query_text_copied);
        assert!(!report.redaction.raw_parameters_copied);
        let replacement = report
            .artifacts
            .iter()
            .find(|artifact| artifact.name == "replacement-summary.json")
            .unwrap();
        let summary = replacement.json.as_ref().unwrap();
        assert!(summary.parse_ready);
        assert_eq!(
            summary.protocol.as_deref(),
            Some("skein-nowledge-replacement-summary")
        );
        assert_eq!(summary.ready, Some(false));
        assert_eq!(
            summary.blocking_categories,
            vec!["query_runtime_preflight".to_string()]
        );
        assert_eq!(summary.production_replacement_per_million, Some(500_000));
        let log = report
            .artifacts
            .iter()
            .find(|artifact| artifact.name == "skein-log.jsonl")
            .unwrap();
        assert_eq!(log.jsonl.as_ref().unwrap().line_count, 2);
        assert_eq!(log.jsonl.as_ref().unwrap().nonempty_line_count, 1);
        let events = report.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].protocol, BLACKBOX_EVENT_PROTOCOL);
        assert_eq!(events[0].run_id, "run-typed");
        let rendered = serde_json::to_string(&report.json()).unwrap();
        assert!(!rendered.contains("MATCH"));
        assert!(!rendered.contains("secret-token"));
        assert!(!rendered.contains(root.to_string_lossy().as_ref()));
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("skein-{prefix}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }
}
