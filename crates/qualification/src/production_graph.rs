use super::{latency_percentiles, runtime_report, LatencyPercentiles, MixedSoakRuntimeReport};
use serde::Serialize;
use sha2::{Digest, Sha256};
use skein::{
    IoConcurrencyBudget, NowledgeGraphStatement, NowledgeMemEmbeddedStoreHandle,
    NowledgeMemGraphMode, NowledgeMemOpenOptions, ProductionEvidenceBinding,
    ProductionQualificationIdentity, RuntimeGovernor, RuntimeGovernorConfig, StorageDeviceProfile,
    StorageResourceProfileLimits, StorageResourceProfileReport, Value,
};
use skein_query::QueryIdentity;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::time::Instant;

pub const PRODUCTION_GRAPH_STORAGE_QUALIFICATION_PROTOCOL: &str =
    "skein-production-graph-storage-qualification-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionGraphStorageQualificationConfig {
    pub open_options: NowledgeMemOpenOptions,
    pub runtime_governor_config: RuntimeGovernorConfig,
    pub statement: NowledgeGraphStatement,
    pub limits: StorageResourceProfileLimits,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub measurement_runs: usize,
}

impl ProductionGraphStorageQualificationConfig {
    fn validate(&self) -> Result<(), ProductionGraphQualificationError> {
        if self.open_options.mode != NowledgeMemGraphMode::ShadowReadOnly {
            return Err(ProductionGraphQualificationError::new(
                "production graph qualification requires shadow read-only mode",
            ));
        }
        let database_config = self.open_options.database_config.as_ref().ok_or_else(|| {
            ProductionGraphQualificationError::new(
                "production graph qualification requires an explicit database config",
            )
        })?;
        if database_config.storage_residency_mode != skein::StorageResidencyMode::OutOfCore {
            return Err(ProductionGraphQualificationError::new(
                "production graph qualification requires out-of-core storage",
            ));
        }
        if database_config.segment_cache_capacity_bytes == 0 {
            return Err(ProductionGraphQualificationError::new(
                "production graph qualification requires a non-zero segment cache budget",
            ));
        }
        if self.measurement_runs == 0 {
            return Err(ProductionGraphQualificationError::new(
                "production graph qualification measurement_runs must be greater than zero",
            ));
        }
        self.expected_identity
            .validate()
            .map_err(ProductionGraphQualificationError::from_error)?;
        self.evidence_binding
            .validate_for(&self.expected_identity)
            .map_err(ProductionGraphQualificationError::from_error)?;
        if self.expected_identity.target_os != std::env::consts::OS {
            return Err(ProductionGraphQualificationError::new(format!(
                "qualification target_os {} does not match current target {}",
                self.expected_identity.target_os,
                std::env::consts::OS
            )));
        }
        if self.expected_identity.target_arch != std::env::consts::ARCH {
            return Err(ProductionGraphQualificationError::new(format!(
                "qualification target_arch {} does not match current target {}",
                self.expected_identity.target_arch,
                std::env::consts::ARCH
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionGraphQualificationError {
    message: String,
}

impl ProductionGraphQualificationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn from_error(error: impl Display) -> Self {
        Self::new(error.to_string())
    }
}

impl Display for ProductionGraphQualificationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProductionGraphQualificationError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProductionGraphExecutionSummary {
    pub measurement_runs: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub intermediate_rows: usize,
    pub intermediate_payload_bytes: usize,
    pub blocking_operator_count: usize,
    pub peak_tracked_operator_bytes: usize,
    pub spilled_bytes: u64,
    pub spill_run_count: usize,
    pub spilled_rows: usize,
    pub morsel_count: usize,
    pub morsel_max_admitted_workers: usize,
    pub morsel_peak_active_workers: usize,
    pub latency: LatencyPercentiles,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionGraphStorageQualificationReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub query_digest: String,
    pub parameter_digest: String,
    pub open_report: serde_json::Value,
    pub evidence_binding: ProductionEvidenceBinding,
    pub execution: ProductionGraphExecutionSummary,
    pub runtime: MixedSoakRuntimeReport,
    pub storage_resource_profile: StorageResourceProfileReport,
}

impl ProductionGraphStorageQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_GRAPH_STORAGE_QUALIFICATION_PROTOCOL,
            "evidence_kind": "representative_production_replica",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "query_identity": {
                "query_digest": self.query_digest,
                "parameter_digest": self.parameter_digest,
            },
            "open_report": self.open_report,
            "evidence_binding": self.evidence_binding.json(),
            "execution": self.execution,
            "runtime": self.runtime,
            "storage_resource_profile": self.storage_resource_profile.json(),
        })
    }
}

pub fn run_production_graph_storage_qualification(
    config: ProductionGraphStorageQualificationConfig,
) -> Result<ProductionGraphStorageQualificationReport, ProductionGraphQualificationError> {
    config.validate()?;
    let query_identity = QueryIdentity::new("cypher", &config.statement.cypher);
    let parameter_digest = parameter_digest(&config.statement.parameters);
    let storage_io = IoConcurrencyBudget::desktop_bound_for_device(StorageDeviceProfile::detect(
        &config.open_options.graph_path,
    ));
    let governor = RuntimeGovernor::detect(config.runtime_governor_config, storage_io);
    let (store, open_report) =
        NowledgeMemEmbeddedStoreHandle::open_with_options_and_runtime_governor(
            config.open_options,
            governor,
        )
        .map_err(ProductionGraphQualificationError::from_error)?;
    let runtime_before = store
        .runtime_governor_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    let mut durations = Vec::with_capacity(config.measurement_runs);
    let mut profiles = Vec::with_capacity(config.measurement_runs);
    for _ in 0..config.measurement_runs {
        let started = Instant::now();
        let profile = store
            .production_resource_profile(
                &config.statement,
                config.limits.clone(),
                config.evidence_binding.clone(),
                config.expected_identity.clone(),
            )
            .map_err(ProductionGraphQualificationError::from_error)?;
        durations.push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
        profiles.push(profile);
    }
    let runtime_after = store
        .runtime_governor_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    let runtime = runtime_report(runtime_before, runtime_after);
    let execution = execution_summary(&profiles, &durations);
    let mut blocker_codes = profiles
        .iter()
        .flat_map(StorageResourceProfileReport::production_blocker_codes)
        .collect::<Vec<_>>();
    if runtime.admissions_delta < config.measurement_runs as u64 {
        blocker_codes.push("runtime_admission_not_observed_for_every_run".to_string());
    }
    if runtime.completions_delta < config.measurement_runs as u64 {
        blocker_codes.push("runtime_completion_not_observed_for_every_run".to_string());
    }
    if runtime.admission_rejections_delta != 0 {
        blocker_codes.push("runtime_admission_rejection_observed".to_string());
    }
    if runtime.final_active_foreground_tasks != 0
        || runtime.final_active_background_tasks != 0
        || runtime.final_active_blocking_tasks != 0
        || runtime.final_admitted_memory_bytes != 0
    {
        blocker_codes.push("runtime_permit_leak".to_string());
    }
    if runtime.final_overcommitted {
        blocker_codes.push("runtime_overcommitted".to_string());
    }
    blocker_codes.sort();
    blocker_codes.dedup();
    let storage_resource_profile = profiles
        .pop()
        .expect("a positive measurement run count always produces a profile");

    Ok(ProductionGraphStorageQualificationReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        query_digest: query_identity.query_digest().to_string(),
        parameter_digest,
        open_report: open_report.json(),
        evidence_binding: config.evidence_binding,
        execution,
        runtime,
        storage_resource_profile,
    })
}

fn execution_summary(
    profiles: &[StorageResourceProfileReport],
    durations: &[u64],
) -> ProductionGraphExecutionSummary {
    let mut summary = ProductionGraphExecutionSummary {
        measurement_runs: profiles.len(),
        latency: latency_percentiles(durations),
        ..ProductionGraphExecutionSummary::default()
    };
    for profile in profiles {
        let pipeline = &profile.query.execution_profile.pipeline_memory_report;
        summary.output_rows = summary
            .output_rows
            .saturating_add(profile.query.output_rows);
        summary.output_payload_bytes = summary
            .output_payload_bytes
            .saturating_add(profile.query.output_payload_bytes);
        summary.intermediate_rows = summary
            .intermediate_rows
            .saturating_add(pipeline.intermediate_rows);
        summary.intermediate_payload_bytes = summary
            .intermediate_payload_bytes
            .saturating_add(pipeline.intermediate_payload_bytes);
        summary.morsel_count = summary.morsel_count.saturating_add(pipeline.morsel_count);
        summary.morsel_max_admitted_workers = summary
            .morsel_max_admitted_workers
            .max(pipeline.morsel_max_admitted_workers);
        summary.morsel_peak_active_workers = summary
            .morsel_peak_active_workers
            .max(pipeline.morsel_peak_active_workers);
        summary.blocking_operator_count = summary.blocking_operator_count.saturating_add(
            profile
                .query
                .execution_profile
                .blocking_operator_memory_reports
                .len(),
        );
        for operator in &profile
            .query
            .execution_profile
            .blocking_operator_memory_reports
        {
            summary.peak_tracked_operator_bytes = summary
                .peak_tracked_operator_bytes
                .max(operator.peak_tracked_bytes);
            summary.spilled_bytes = summary.spilled_bytes.saturating_add(operator.spilled_bytes);
            summary.spill_run_count = summary
                .spill_run_count
                .saturating_add(operator.spill_run_count);
            summary.spilled_rows = summary.spilled_rows.saturating_add(operator.spilled_rows);
        }
    }
    summary
}

fn parameter_digest(parameters: &std::collections::BTreeMap<String, Value>) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"skein-production-query-parameters-v1");
    for (name, value) in parameters {
        hash_field(&mut hasher, name.as_bytes());
        hash_value(&mut hasher, value);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn hash_value(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Null => hasher.update([0]),
        Value::Bool(value) => {
            hasher.update([1]);
            hasher.update([u8::from(*value)]);
        }
        Value::Int(value) => {
            hasher.update([2]);
            hasher.update(value.to_le_bytes());
        }
        Value::Float(value) => {
            hasher.update([3]);
            hasher.update(value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            hasher.update([4]);
            hash_field(hasher, value.as_bytes());
        }
        Value::List(values) => {
            hasher.update([5]);
            hasher.update((values.len() as u64).to_le_bytes());
            for value in values {
                hash_value(hasher, value);
            }
        }
        Value::Map(values) => {
            hasher.update([6]);
            hasher.update((values.len() as u64).to_le_bytes());
            for (name, value) in values {
                hash_field(hasher, name.as_bytes());
                hash_value(hasher, value);
            }
        }
    }
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::{
        Database, DatabaseConfig, StorageResidencyMode, PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn production_runner_uses_admitted_handle_and_redacts_inputs() {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!(
            "skein-production-graph-qualification-{}-{id}",
            std::process::id()
        ));
        let graph_path = root.join("database");
        let database_config = DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            segment_cache_capacity_bytes: 1024,
            max_read_result_rows: Some(128),
            max_read_result_payload_bytes: Some(1024 * 1024),
            ..DatabaseConfig::default()
        };
        let graph_commit_epoch = {
            let mut database = Database::open_with_config(&graph_path, database_config.clone())
                .expect("fixture database should open");
            let mut transaction = database.begin_transaction();
            for row in 0..64 {
                transaction
                    .query_with_params(
                        "CREATE (:Memory {id: $id, body: $body})",
                        &BTreeMap::from([
                            ("id".to_string(), Value::Int(row)),
                            ("body".to_string(), Value::String("secret".repeat(256))),
                        ]),
                    )
                    .expect("fixture row should be inserted");
            }
            transaction.commit().expect("fixture commit should succeed");
            database
                .checkpoint()
                .expect("fixture checkpoint should succeed");
            database.commit_epoch()
        };
        let identity = ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".to_string(),
            deployment_profile: "representative-production-replica".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: graph_commit_epoch,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let report =
            run_production_graph_storage_qualification(ProductionGraphStorageQualificationConfig {
                open_options: NowledgeMemOpenOptions::graph_only(
                    &graph_path,
                    NowledgeMemGraphMode::ShadowReadOnly,
                )
                .with_database_config(database_config),
                runtime_governor_config: RuntimeGovernorConfig {
                    cpu_slot_limit: NonZeroUsize::new(4),
                    foreground_task_limit: NonZeroUsize::new(4),
                    background_task_limit: NonZeroUsize::new(1),
                    blocking_task_limit: NonZeroUsize::new(2),
                    memory_budget_bytes: Some(64 * 1024 * 1024),
                    result_budget_bytes: 1024 * 1024,
                    ..RuntimeGovernorConfig::desktop_bound()
                },
                statement: NowledgeGraphStatement {
                    cypher: "MATCH (m:Memory) RETURN m.id AS memory_id".to_string(),
                    parameters: BTreeMap::new(),
                },
                limits: StorageResourceProfileLimits {
                    min_canonical_artifact_bytes: 4096,
                    max_steady_resident_bytes: u64::MAX,
                    max_peak_resident_bytes: u64::MAX,
                    max_total_page_faults: Some(u64::MAX),
                    max_minor_page_faults: cfg!(unix).then_some(u64::MAX),
                    max_major_page_faults: cfg!(unix).then_some(u64::MAX),
                    max_intermediate_rows: 4096,
                    max_intermediate_payload_bytes: 16 * 1024 * 1024,
                    max_output_rows: 128,
                    max_output_payload_bytes: 1024 * 1024,
                    require_fully_streamed: true,
                },
                evidence_binding: ProductionEvidenceBinding {
                    identity: identity.clone(),
                    generated_at_unix_seconds: 1,
                },
                expected_identity: identity,
                measurement_runs: 3,
            })
            .expect("qualification should complete");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(report.runtime.admissions_delta, 3);
        assert_eq!(report.runtime.completions_delta, 3);
        assert_eq!(report.execution.measurement_runs, 3);
        assert!(report.storage_resource_profile.production_ready());
        let json = report.json().to_string();
        assert!(!json.contains(graph_path.to_string_lossy().as_ref()));
        assert!(!json.contains("MATCH (m:Memory)"));
        assert!(!json.contains("secret"));

        drop(report);
        std::fs::remove_dir_all(root).expect("fixture should be removable");
    }

    #[test]
    fn parameter_digest_binds_values_without_exposing_them() {
        let first = parameter_digest(&BTreeMap::from([(
            "needle".to_string(),
            Value::String("sensitive-a".to_string()),
        )]));
        let second = parameter_digest(&BTreeMap::from([(
            "needle".to_string(),
            Value::String("sensitive-b".to_string()),
        )]));

        assert_ne!(first, second);
        assert!(!first.contains("sensitive-a"));
    }
}
