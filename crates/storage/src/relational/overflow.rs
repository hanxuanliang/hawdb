use super::{
    RelationalError, RelationalOverflowSegment, RelationalRow, RelationalScalarType,
    RelationalState, RelationalTableSchema, RelationalValue,
};
use skein_integrity::Sha256Digest;
use std::collections::BTreeSet;
use std::sync::Arc;

mod envelope;

use envelope::DEFAULT_ZSTD_LEVEL;
pub(in crate::relational) use envelope::{decode_overflow_envelope, encode_overflow_envelope};

pub const DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES: usize = 4 * 1024;
pub const DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowConfig {
    pub threshold_bytes: usize,
    pub compression_level: i32,
    pub max_value_bytes: usize,
}

impl Default for RelationalOverflowConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES,
            compression_level: DEFAULT_ZSTD_LEVEL,
            max_value_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalOverflowRef {
    pub digest: Sha256Digest,
    pub scalar_type: RelationalScalarType,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalHydrationBudget {
    pub max_rows: usize,
    pub max_compressed_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_memory_bytes: usize,
    pub hydrated_rows: usize,
    pub compressed_bytes: usize,
    pub decompressed_bytes: usize,
    pub memory_bytes: usize,
}

impl Default for RelationalHydrationBudget {
    fn default() -> Self {
        Self {
            max_rows: 1_000,
            max_compressed_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            max_decompressed_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            max_memory_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            hydrated_rows: 0,
            compressed_bytes: 0,
            decompressed_bytes: 0,
            memory_bytes: 0,
        }
    }
}

pub(super) fn externalize_row(
    state: &mut RelationalState,
    schema: &RelationalTableSchema,
    row: &mut RelationalRow,
    config: RelationalOverflowConfig,
) -> Result<(), RelationalError> {
    let protected = protected_column_positions(schema)?;
    let values = Arc::make_mut(&mut row.values);
    for (position, value) in values.iter_mut().enumerate() {
        if protected.contains(&position) {
            continue;
        }
        let (scalar_type, raw) = match value {
            RelationalValue::Text(text) if text.len() >= config.threshold_bytes => {
                (RelationalScalarType::Text, text.as_bytes())
            }
            RelationalValue::Bytea(bytes) if bytes.len() >= config.threshold_bytes => {
                (RelationalScalarType::Bytea, bytes.as_slice())
            }
            _ => continue,
        };
        let encoded = encode_overflow_envelope(scalar_type, raw, config)?;
        let digest = encoded.reference.digest;
        state
            .overflow_segments
            .entry(digest)
            .or_insert_with(|| RelationalOverflowSegment::Inline(Arc::clone(&encoded.bytes)));
        *value = RelationalValue::Overflow(encoded.reference);
    }
    Ok(())
}

pub(super) fn hydrate_row(
    state: &RelationalState,
    row: &RelationalRow,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<RelationalRow, RelationalError> {
    runtime_checkpoint(task_context)?;
    let mut staged_budget = *budget;
    if staged_budget.hydrated_rows >= staged_budget.max_rows {
        return Err(RelationalError::Admission(format!(
            "relational hydration exceeds max_rows {}",
            staged_budget.max_rows
        )));
    }
    staged_budget.hydrated_rows += 1;
    let mut values = row.values.to_vec();
    for value in &mut values {
        let RelationalValue::Overflow(reference) = value else {
            continue;
        };
        let segment = state
            .overflow_segments
            .get(&reference.digest)
            .ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "missing overflow segment {}",
                    reference.digest
                ))
            })?;
        runtime_checkpoint(task_context)?;
        let envelope = segment.read()?;
        *value = decode_overflow_envelope(reference, &envelope, &mut staged_budget, task_context)?;
    }
    *budget = staged_budget;
    Ok(RelationalRow::new(values))
}

fn runtime_checkpoint(
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<(), RelationalError> {
    task_context.map_or(Ok(()), |context| {
        context.checkpoint().map_err(|reason| {
            RelationalError::Admission(format!("relational hydration stopped: {reason}"))
        })
    })
}

pub(super) fn prune_unreachable_segments(state: &mut RelationalState) {
    let reachable = state
        .segments
        .values()
        .flat_map(|segment| segment.rows.values())
        .flat_map(|row| row.values.iter())
        .filter_map(|value| match value {
            RelationalValue::Overflow(reference) => Some(reference.digest),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    state
        .overflow_segments
        .retain(|digest, _| reachable.contains(digest));
}

fn protected_column_positions(
    schema: &RelationalTableSchema,
) -> Result<BTreeSet<usize>, RelationalError> {
    let names = schema
        .primary_key
        .iter()
        .chain(schema.unique_constraints.iter().flatten())
        .chain(
            schema
                .foreign_keys
                .iter()
                .flat_map(|key| key.columns.iter()),
        )
        .chain(schema.indexes.iter().flat_map(|index| index.columns.iter()))
        .collect::<BTreeSet<_>>();
    names
        .into_iter()
        .map(|name| {
            schema.column_position(name).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "overflow protection references unknown column {name}"
                ))
            })
        })
        .collect()
}
