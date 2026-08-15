use super::{
    ContentStoreRowPageCacheDelta, ContentStoreRowPageExecutionEvidence,
    ContentStoreRowPageReadPhase, ContentStoreRowPageReadReport,
};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlStatementSpec;
use skein::{Database, QueryStreamOptions, Result, SkeinError, Value};

const EXPLAIN_MAX_ROWS: usize = 128;
const EXPLAIN_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn execute_read_set(
    database: &mut Database,
    specs: &[(&ContentStoreSqlStatementSpec, Vec<Value>, usize)],
    phase: ContentStoreRowPageReadPhase,
) -> Result<Vec<ContentStoreRowPageReadReport>> {
    specs
        .iter()
        .map(|(statement, parameters, expected_rows)| {
            execute_qualified_read(
                database,
                statement,
                parameters.clone(),
                phase,
                *expected_rows,
            )
        })
        .collect()
}

pub(super) fn execute_qualified_read(
    database: &mut Database,
    statement: &ContentStoreSqlStatementSpec,
    parameters: Vec<Value>,
    phase: ContentStoreRowPageReadPhase,
    expected_rows: usize,
) -> Result<ContentStoreRowPageReadReport> {
    let before = database.segment_cache_snapshot().ok_or_else(|| {
        SkeinError::Execution(
            "content-store row-page qualification requires a segment cache".to_string(),
        )
    })?;
    let output = database.query_sql_with_params_options(
        &statement.sql,
        &parameters,
        QueryStreamOptions {
            max_rows: Some(statement.max_rows),
            max_payload_bytes: Some(statement.max_payload_bytes),
        },
    )?;
    let after = database.segment_cache_snapshot().ok_or_else(|| {
        SkeinError::Execution(
            "content-store row-page qualification lost its segment cache".to_string(),
        )
    })?;
    if output.rows.len() != expected_rows {
        return Err(SkeinError::Execution(format!(
            "content-store statement {} returned {} rows, expected {expected_rows}",
            statement.name,
            output.rows.len()
        )));
    }
    let output_payload_bytes = output.payload_bytes();
    if output_payload_bytes > statement.max_payload_bytes {
        return Err(SkeinError::Execution(format!(
            "content-store statement {} returned {output_payload_bytes} payload bytes, exceeding {}",
            statement.name, statement.max_payload_bytes
        )));
    }
    let execution = explain_execution(database, statement, &parameters)?;
    Ok(ContentStoreRowPageReadReport {
        statement_name: statement.name.clone(),
        phase,
        max_rows: statement.max_rows,
        max_payload_bytes: statement.max_payload_bytes,
        output_rows: output.rows.len(),
        output_payload_bytes,
        output_sha256: rows_sha256(&output.rows),
        cache: cache_delta(before, after)?,
        execution,
    })
}

fn explain_execution(
    database: &mut Database,
    statement: &ContentStoreSqlStatementSpec,
    parameters: &[Value],
) -> Result<ContentStoreRowPageExecutionEvidence> {
    let explain = database.query_sql_with_params_options(
        &format!("EXPLAIN ANALYZE {}", statement.sql),
        parameters,
        QueryStreamOptions {
            max_rows: Some(EXPLAIN_MAX_ROWS),
            max_payload_bytes: Some(EXPLAIN_MAX_PAYLOAD_BYTES),
        },
    )?;
    let info = explain
        .rows
        .iter()
        .find_map(|row| match row.get("operator info") {
            Some(Value::String(info)) if info.contains("row_runtime_path=") => Some(info.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "content-store statement {} has no row-page execution evidence",
                statement.name
            ))
        })?;
    let evidence = ContentStoreRowPageExecutionEvidence {
        index_runtime_path: info_field(info, "runtime_path")
            .unwrap_or("none")
            .to_string(),
        row_runtime_path: required_info_field(info, "row_runtime_path", &statement.name)?
            .to_string(),
        base_generation: required_info_u64(info, "row_base_generation", &statement.name)?,
        delta_generation: optional_info_u64(info, "row_delta_generation", &statement.name)?,
        base_commit_epoch: required_info_u64(info, "row_base_epoch", &statement.name)?,
        visible_commit_epoch: required_info_u64(info, "row_visible_epoch", &statement.name)?,
        root_set_digest: required_info_field(info, "row_root_set_digest", &statement.name)?
            .to_string(),
        logical_pages: required_info_u64(info, "row_logical_pages", &statement.name)?,
        logical_bytes: required_info_u64(info, "row_logical_bytes", &statement.name)?,
        physical_pages: required_info_u64(info, "row_physical_pages", &statement.name)?,
        physical_bytes: required_info_u64(info, "row_physical_bytes", &statement.name)?,
        cache_hits: required_info_u64(info, "row_cache_hits", &statement.name)?,
        cache_misses: required_info_u64(info, "row_cache_misses", &statement.name)?,
        cache_admission_rejections: required_info_u64(
            info,
            "row_cache_admission_rejections",
            &statement.name,
        )?,
        overlay_entries: required_info_u64(info, "row_overlay_entries", &statement.name)?,
        overlay_bytes: required_info_u64(info, "row_overlay_bytes", &statement.name)?,
        rows_visited: required_info_u64(info, "row_rows", &statement.name)?,
    };
    if evidence.index_runtime_path != "authoritative" {
        return Err(SkeinError::Execution(format!(
            "content-store statement {} used index runtime {}, expected authoritative",
            statement.name, evidence.index_runtime_path
        )));
    }
    if evidence.row_runtime_path != "snapshot_rows" {
        return Err(SkeinError::Execution(format!(
            "content-store statement {} used row runtime {}, expected snapshot_rows",
            statement.name, evidence.row_runtime_path
        )));
    }
    if evidence.root_set_digest == "none" {
        return Err(SkeinError::Execution(format!(
            "content-store statement {} did not bind a row root-set digest",
            statement.name
        )));
    }
    Ok(evidence)
}

fn cache_delta(
    before: skein::SegmentCacheSnapshot,
    after: skein::SegmentCacheSnapshot,
) -> Result<ContentStoreRowPageCacheDelta> {
    if after.pinned_bytes != 0 {
        return Err(SkeinError::Execution(format!(
            "content-store row-page qualification leaked {} pinned cache bytes",
            after.pinned_bytes
        )));
    }
    Ok(ContentStoreRowPageCacheDelta {
        hits: monotonic_delta("cache hits", before.hit_count, after.hit_count)?,
        misses: monotonic_delta("cache misses", before.miss_count, after.miss_count)?,
        insertions: monotonic_delta(
            "cache insertions",
            before.insertion_count,
            after.insertion_count,
        )?,
        evictions: monotonic_delta(
            "cache evictions",
            before.eviction_count,
            after.eviction_count,
        )?,
        admission_rejections: monotonic_delta(
            "cache admission rejections",
            before.admission_rejection_count,
            after.admission_rejection_count,
        )?,
        resident_bytes_after: after.resident_bytes,
        pinned_bytes_after: after.pinned_bytes,
    })
}

fn monotonic_delta(name: &str, before: u64, after: u64) -> Result<u64> {
    after.checked_sub(before).ok_or_else(|| {
        SkeinError::Execution(format!(
            "content-store row-page qualification observed non-monotonic {name}"
        ))
    })
}

pub(super) fn require_matching_results(
    cold: &[ContentStoreRowPageReadReport],
    warm: &[ContentStoreRowPageReadReport],
) -> Result<()> {
    if cold.len() != warm.len() {
        return Err(SkeinError::Execution(
            "content-store cold and warm read sets have different lengths".to_string(),
        ));
    }
    for (cold, warm) in cold.iter().zip(warm) {
        if cold.statement_name != warm.statement_name
            || cold.output_rows != warm.output_rows
            || cold.output_sha256 != warm.output_sha256
        {
            return Err(SkeinError::Execution(format!(
                "content-store cold/warm result mismatch for {}",
                cold.statement_name
            )));
        }
    }
    Ok(())
}

fn info_field<'a>(info: &'a str, name: &str) -> Option<&'a str> {
    info.split(", ").find_map(|field| {
        let (key, value) = field.split_once('=')?;
        (key == name).then_some(value)
    })
}

fn required_info_field<'a>(info: &'a str, name: &str, statement: &str) -> Result<&'a str> {
    info_field(info, name).ok_or_else(|| {
        SkeinError::Execution(format!(
            "content-store statement {statement} has no {name} execution evidence"
        ))
    })
}

fn required_info_u64(info: &str, name: &str, statement: &str) -> Result<u64> {
    let value = required_info_field(info, name, statement)?;
    if value == "none" {
        return Err(SkeinError::Execution(format!(
            "content-store statement {statement} has no value for {name}"
        )));
    }
    value.parse::<u64>().map_err(|error| {
        SkeinError::Execution(format!(
            "content-store statement {statement} has invalid {name} value {value}: {error}"
        ))
    })
}

fn optional_info_u64(info: &str, name: &str, statement: &str) -> Result<Option<u64>> {
    let value = required_info_field(info, name, statement)?;
    if value == "none" {
        return Ok(None);
    }
    value.parse::<u64>().map(Some).map_err(|error| {
        SkeinError::Execution(format!(
            "content-store statement {statement} has invalid {name} value {value}: {error}"
        ))
    })
}
