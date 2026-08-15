use super::{latency_percentiles, runtime_report, LatencyPercentiles, MixedSoakRuntimeReport};
use crate::evidence_digest::{hash_bytes, hash_value};
use serde::Serialize;
use sha2::{Digest, Sha256};
use skein::{
    IoConcurrencyBudget, NowledgeGraphStatement, NowledgeMemEmbeddedStoreHandle,
    NowledgeMemGraphMode, NowledgeMemOpenOptions, NowledgeMemReadOptions,
    PersistentGraphIndexClass, ProductionEvidenceBinding, ProductionQualificationIdentity,
    RuntimeGovernor, RuntimeGovernorConfig, StorageDeviceProfile, StorageResourceProfileLimits,
    StorageResourceProfileReport, Value,
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
    pub persistent_index_requirement: Option<PersistentGraphIndexProductionRequirement>,
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
        if let Some(requirement) = &self.persistent_index_requirement {
            requirement.validate()?;
        }
        validate_production_identity_for_current_target(
            &self.evidence_binding,
            &self.expected_identity,
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentGraphIndexProductionRequirement {
    pub class: PersistentGraphIndexClass,
    pub reference_output_digest: String,
    pub reference_output_rows: usize,
    pub max_blocks_read_per_run: u64,
    pub max_bytes_read_per_run: u64,
}

impl PersistentGraphIndexProductionRequirement {
    fn validate(&self) -> Result<(), ProductionGraphQualificationError> {
        if !valid_sha256_digest(&self.reference_output_digest) {
            return Err(ProductionGraphQualificationError::new(
                "persistent graph index reference_output_digest must be a sha256 digest",
            ));
        }
        if self.max_blocks_read_per_run == 0 || self.max_bytes_read_per_run == 0 {
            return Err(ProductionGraphQualificationError::new(
                "persistent graph index read budgets must be greater than zero",
            ));
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

    pub(crate) fn from_error(error: impl Display) -> Self {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersistentGraphIndexProductionEvidence {
    pub class: String,
    pub reference_output_digest: String,
    pub observed_output_digest: String,
    pub reference_output_rows: usize,
    pub observed_output_rows: usize,
    pub exact_result_parity: bool,
    pub digest_runs: usize,
    pub measurement_runs: usize,
    pub runs_using_required_class: usize,
    pub operation_count: u64,
    pub max_blocks_read: u64,
    pub max_bytes_read: u64,
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
    pub persistent_index_evidence: Option<PersistentGraphIndexProductionEvidence>,
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
            "persistent_index_evidence": self.persistent_index_evidence,
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
    let observed_index_output = config
        .persistent_index_requirement
        .as_ref()
        .map(|_| stream_graph_result_digest(&store, &config.statement, &config.limits))
        .transpose()?;
    let runtime_after = store
        .runtime_governor_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    let runtime = runtime_report(runtime_before, runtime_after);
    let execution = execution_summary(&profiles, &durations);
    let mut blocker_codes = profiles
        .iter()
        .flat_map(StorageResourceProfileReport::production_blocker_codes)
        .collect::<Vec<_>>();
    let expected_runtime_operations = u64::try_from(config.measurement_runs)
        .unwrap_or(u64::MAX)
        .saturating_add(if config.persistent_index_requirement.is_some() {
            1
        } else {
            0
        });
    if runtime.admissions_delta < expected_runtime_operations {
        blocker_codes.push("runtime_admission_not_observed_for_every_run".to_string());
    }
    if runtime.completions_delta < expected_runtime_operations {
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
    let persistent_index_evidence = if let Some(requirement) = &config.persistent_index_requirement
    {
        let (observed_output_digest, observed_output_rows) = observed_index_output
            .expect("a persistent graph index requirement produces one observed digest");
        let mut runs_using_required_class = 0usize;
        let mut operation_count = 0u64;
        let mut max_blocks_read = 0u64;
        let mut max_bytes_read = 0u64;
        for profile in &profiles {
            let reads = profile
                .after
                .graph_index_reads
                .delta_since(profile.before.graph_index_reads);
            let class_operations = reads.operation_count(requirement.class);
            if class_operations > 0 {
                runs_using_required_class = runs_using_required_class.saturating_add(1);
            }
            operation_count = operation_count.saturating_add(class_operations);
            let blocks_read = reads.blocks_read(requirement.class);
            let bytes_read = reads.bytes_read(requirement.class);
            max_blocks_read = max_blocks_read.max(blocks_read);
            max_bytes_read = max_bytes_read.max(bytes_read);
            if class_operations == 0 {
                blocker_codes.push(format!(
                    "persistent_graph_index_{}_not_observed",
                    requirement.class.as_str()
                ));
            }
            if blocks_read == 0 || bytes_read == 0 {
                blocker_codes.push(format!(
                    "persistent_graph_index_{}_page_read_not_observed",
                    requirement.class.as_str()
                ));
            }
            if blocks_read > requirement.max_blocks_read_per_run {
                blocker_codes.push(format!(
                    "persistent_graph_index_{}_block_budget_exceeded",
                    requirement.class.as_str()
                ));
            }
            if bytes_read > requirement.max_bytes_read_per_run {
                blocker_codes.push(format!(
                    "persistent_graph_index_{}_byte_budget_exceeded",
                    requirement.class.as_str()
                ));
            }
        }
        let exact_result_parity = observed_output_digest == requirement.reference_output_digest
            && observed_output_rows == requirement.reference_output_rows;
        if !exact_result_parity {
            blocker_codes.push("persistent_graph_index_result_mismatch".to_string());
        }
        Some(PersistentGraphIndexProductionEvidence {
            class: requirement.class.as_str().to_string(),
            reference_output_digest: requirement.reference_output_digest.clone(),
            observed_output_digest,
            reference_output_rows: requirement.reference_output_rows,
            observed_output_rows,
            exact_result_parity,
            digest_runs: 1,
            measurement_runs: profiles.len(),
            runs_using_required_class,
            operation_count,
            max_blocks_read,
            max_bytes_read,
        })
    } else {
        None
    };
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
        persistent_index_evidence,
    })
}

fn stream_graph_result_digest(
    store: &NowledgeMemEmbeddedStoreHandle,
    statement: &NowledgeGraphStatement,
    limits: &StorageResourceProfileLimits,
) -> Result<(String, usize), ProductionGraphQualificationError> {
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, b"skein-persistent-graph-index-result-v1");
    let mut row_count = 0usize;
    let report = store
        .read_query_with_params_streaming(
            &statement.cypher,
            &statement.parameters,
            &NowledgeMemReadOptions {
                max_rows: Some(limits.max_output_rows),
                max_estimated_payload_bytes: Some(limits.max_output_payload_bytes),
            },
            |row| {
                hash_graph_result_row(&mut hasher, &row);
                row_count = row_count.saturating_add(1);
                Ok(())
            },
        )
        .map_err(ProductionGraphQualificationError::from_error)?;
    if !report.fully_streamed {
        return Err(ProductionGraphQualificationError::new(
            "persistent graph index result digest requires a fully streamed query",
        ));
    }
    hash_bytes(&mut hasher, &(row_count as u64).to_le_bytes());
    Ok((format!("sha256:{:x}", hasher.finalize()), row_count))
}

fn hash_graph_result_row(hasher: &mut Sha256, row: &std::collections::BTreeMap<String, Value>) {
    hash_bytes(hasher, b"row");
    hash_bytes(hasher, &(row.len() as u64).to_le_bytes());
    for (name, value) in row {
        hash_bytes(hasher, name.as_bytes());
        hash_value(hasher, value);
    }
}

fn valid_sha256_digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
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

pub(crate) fn validate_production_identity_for_current_target(
    evidence_binding: &ProductionEvidenceBinding,
    expected_identity: &ProductionQualificationIdentity,
) -> Result<(), ProductionGraphQualificationError> {
    expected_identity
        .validate()
        .map_err(ProductionGraphQualificationError::from_error)?;
    evidence_binding
        .validate_for(expected_identity)
        .map_err(ProductionGraphQualificationError::from_error)?;
    if expected_identity.target_os != std::env::consts::OS {
        return Err(ProductionGraphQualificationError::new(format!(
            "qualification target_os {} does not match current target {}",
            expected_identity.target_os,
            std::env::consts::OS
        )));
    }
    if expected_identity.target_arch != std::env::consts::ARCH {
        return Err(ProductionGraphQualificationError::new(format!(
            "qualification target_arch {} does not match current target {}",
            expected_identity.target_arch,
            std::env::consts::ARCH
        )));
    }
    Ok(())
}

pub(crate) fn parameter_digest(parameters: &std::collections::BTreeMap<String, Value>) -> String {
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, b"skein-production-query-parameters-v1");
    for (name, value) in parameters {
        hash_bytes(&mut hasher, name.as_bytes());
        hash_value(&mut hasher, value);
    }
    format!("sha256:{:x}", hasher.finalize())
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
            database
                .query("CREATE INDEX ON :Memory(id)")
                .expect("fixture property index should be created");
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
        let expected_output_digest = {
            let mut hasher = Sha256::new();
            hash_bytes(&mut hasher, b"skein-persistent-graph-index-result-v1");
            hash_graph_result_row(
                &mut hasher,
                &BTreeMap::from([("memory_id".to_string(), Value::Int(7))]),
            );
            hash_bytes(&mut hasher, &1u64.to_le_bytes());
            format!("sha256:{:x}", hasher.finalize())
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
                    cypher: "MATCH (m:Memory) WHERE m.id = $id RETURN m.id AS memory_id"
                        .to_string(),
                    parameters: BTreeMap::from([("id".to_string(), Value::Int(7))]),
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
                persistent_index_requirement: Some(PersistentGraphIndexProductionRequirement {
                    class: PersistentGraphIndexClass::NodeEquality,
                    reference_output_digest: expected_output_digest,
                    reference_output_rows: 1,
                    max_blocks_read_per_run: 8,
                    max_bytes_read_per_run: 1024 * 1024,
                }),
            })
            .expect("qualification should complete");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(report.runtime.admissions_delta, 4);
        assert_eq!(report.runtime.completions_delta, 4);
        assert_eq!(report.execution.measurement_runs, 3);
        assert!(report.storage_resource_profile.production_ready());
        let index_evidence = report
            .persistent_index_evidence
            .as_ref()
            .expect("persistent index evidence should be present");
        assert!(index_evidence.exact_result_parity);
        assert_eq!(index_evidence.digest_runs, 1);
        assert_eq!(index_evidence.runs_using_required_class, 3);
        assert_eq!(index_evidence.operation_count, 3);
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
