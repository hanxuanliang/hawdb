use serde::Deserialize;
use skein::{ProductionEvidenceBinding, ProductionQualificationIdentity, Value};
use std::fs::File;
use std::io::Read;
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
