use serde::Deserialize;
use skein::{
    DatabaseConfig, ProductionEvidenceBinding, ProductionQualificationIdentity,
    RelationalIndexMode, StorageResidencyMode, Value,
};
use std::fs::File;
use std::io::Read;
use std::num::NonZeroUsize;
use std::path::Path;

const MAX_PLAN_BYTES: u64 = 32 * 1024 * 1024;

pub(crate) fn read_bounded_json<T: serde::de::DeserializeOwned>(
    path: &Path,
    kind: &str,
) -> Result<T, String> {
    let file = File::open(path).map_err(|error| format!("failed to open {kind}: {error}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_PLAN_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read {kind}: {error}"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_PLAN_BYTES {
        return Err(format!("{kind} exceeds {MAX_PLAN_BYTES} bytes"));
    }
    serde_json::from_slice(&bytes).map_err(|error| format!("invalid {kind}: {error}"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvidenceBindingInput {
    identity: ProductionIdentityInput,
    generated_at_unix_seconds: u64,
}

impl From<EvidenceBindingInput> for ProductionEvidenceBinding {
    fn from(input: EvidenceBindingInput) -> Self {
        Self {
            identity: input.identity.into(),
            generated_at_unix_seconds: input.generated_at_unix_seconds,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProductionIdentityInput {
    source_revision: String,
    rust_toolchain: String,
    target_os: String,
    target_arch: String,
    enabled_features: Vec<String>,
    durable_format_version: u64,
    schema_version: u64,
    configuration_digest: String,
    deployment_profile: String,
    dataset_fingerprint: String,
    canonical_graph_commit_epoch: u64,
    policy_version: u64,
}

impl From<ProductionIdentityInput> for ProductionQualificationIdentity {
    fn from(input: ProductionIdentityInput) -> Self {
        Self {
            source_revision: input.source_revision,
            rust_toolchain: input.rust_toolchain,
            target_os: input.target_os,
            target_arch: input.target_arch,
            enabled_features: input.enabled_features,
            durable_format_version: input.durable_format_version,
            schema_version: input.schema_version,
            configuration_digest: input.configuration_digest,
            deployment_profile: input.deployment_profile,
            dataset_fingerprint: input.dataset_fingerprint,
            canonical_graph_commit_epoch: input.canonical_graph_commit_epoch,
            policy_version: input.policy_version,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatabaseInput {
    max_read_result_rows: usize,
    max_read_result_payload_bytes: usize,
    execution_batch_rows: usize,
    execution_batch_payload_bytes: usize,
    blocking_operator_bytes: usize,
    segment_cache_capacity_bytes: u64,
    max_relational_index_read_bytes: usize,
    max_relational_hydration_bytes: usize,
}

impl DatabaseInput {
    pub(crate) fn resolve(self, read_only: bool) -> Result<DatabaseConfig, String> {
        let mut config = DatabaseConfig {
            read_only,
            max_read_result_rows: Some(require_nonzero_usize(
                "max_read_result_rows",
                self.max_read_result_rows,
            )?),
            max_read_result_payload_bytes: Some(require_nonzero_usize(
                "max_read_result_payload_bytes",
                self.max_read_result_payload_bytes,
            )?),
            segment_cache_capacity_bytes: require_nonzero_u64(
                "segment_cache_capacity_bytes",
                self.segment_cache_capacity_bytes,
            )?,
            max_relational_index_read_bytes: NonZeroUsize::new(
                self.max_relational_index_read_bytes,
            )
            .ok_or_else(|| "max_relational_index_read_bytes must be non-zero".to_string())?,
            max_relational_hydration_bytes: NonZeroUsize::new(self.max_relational_hydration_bytes)
                .ok_or_else(|| "max_relational_hydration_bytes must be non-zero".to_string())?,
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..DatabaseConfig::default()
        };
        config.execution_memory.batch_rows = NonZeroUsize::new(self.execution_batch_rows)
            .ok_or_else(|| "execution_batch_rows must be non-zero".to_string())?;
        config.execution_memory.batch_payload_bytes =
            NonZeroUsize::new(self.execution_batch_payload_bytes)
                .ok_or_else(|| "execution_batch_payload_bytes must be non-zero".to_string())?;
        config.execution_memory.blocking_operator_bytes =
            NonZeroUsize::new(self.blocking_operator_bytes)
                .ok_or_else(|| "blocking_operator_bytes must be non-zero".to_string())?;
        Ok(config)
    }
}

pub(crate) fn value_from_json(value: &serde_json::Value) -> Result<Value, String> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_u64() {
                i64::try_from(value)
                    .map(Value::Int)
                    .map_err(|_| "qualification parameter integer exceeds i64".to_string())
            } else {
                value
                    .as_f64()
                    .map(Value::Float)
                    .ok_or_else(|| "qualification parameter number is invalid".to_string())
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
            .collect::<Result<_, _>>()
            .map(Value::Map),
    }
}

fn require_nonzero_usize(name: &str, value: usize) -> Result<usize, String> {
    (value > 0)
        .then_some(value)
        .ok_or_else(|| format!("{name} must be non-zero"))
}

fn require_nonzero_u64(name: &str, value: u64) -> Result<u64, String> {
    (value > 0)
        .then_some(value)
        .ok_or_else(|| format!("{name} must be non-zero"))
}
