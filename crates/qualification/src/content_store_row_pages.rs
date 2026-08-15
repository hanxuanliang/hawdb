mod corruption;
mod evidence;
mod fixture;
mod isolation;
mod resource;
#[cfg(test)]
mod tests;
mod transaction;

use crate::{
    nowledge_content_store_schema_identity, nowledge_content_store_sql_corpus,
    ContentStoreSchemaIdentity, ContentStoreSqlCorpus, ContentStoreSqlCorpusIdentity,
};
use corruption::qualify_content_store_corruption;
use evidence::{execute_qualified_read, execute_read_set, require_matching_results};
use fixture::{
    bootstrap_checkpoint, corpus_statement, database_config, initial_read_specs,
    thread_page_parameters, upsert_thread_message, QUALIFIED_TABLES,
};
use isolation::qualify_content_store_isolation;
use resource::{qualify_content_store_resources, ContentStoreResourceProbeConfig};
use serde::Serialize;
use skein::{Database, DurabilityPolicy, RelationalIndexMode, Result, SkeinError};
use std::path::PathBuf;
use transaction::qualify_multi_statement_transaction;

pub const CONTENT_STORE_INITIAL_ROW_PAGE_QUALIFICATION_PROTOCOL: &str =
    "skein-content-store-initial-row-page-qualification-v1";
pub const CONTENT_STORE_SUPPORTED_LOW_MEMORY_PROFILE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStoreResourceProfileKind {
    SupportedLowMemory,
    ConfiguredWorkload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentStoreInitialRowPageQualificationConfig {
    pub database_path: PathBuf,
    pub source_revision: String,
    pub base_message_count: usize,
    pub message_payload_bytes: usize,
    pub segment_cache_capacity_bytes: u64,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub resource_read_samples: usize,
}

impl ContentStoreInitialRowPageQualificationConfig {
    pub fn synthetic(
        database_path: impl Into<PathBuf>,
        source_revision: impl Into<String>,
    ) -> Self {
        Self {
            database_path: database_path.into(),
            source_revision: source_revision.into(),
            base_message_count: 8,
            message_payload_bytes: 8 * 1024,
            segment_cache_capacity_bytes: 512 * 1024,
            resource_profile_kind: ContentStoreResourceProfileKind::SupportedLowMemory,
            configured_available_memory_bytes: CONTENT_STORE_SUPPORTED_LOW_MEMORY_PROFILE_BYTES,
            resource_read_samples: 16,
        }
    }

    fn validate(&self, corpus: &ContentStoreSqlCorpus) -> Result<()> {
        if self.database_path.exists() {
            return Err(SkeinError::Semantic(
                "content-store row-page qualification requires a new database path".to_string(),
            ));
        }
        if self.source_revision.trim().is_empty() {
            return Err(SkeinError::Semantic(
                "content-store row-page qualification source revision must not be empty"
                    .to_string(),
            ));
        }
        if self.base_message_count == 0 || self.message_payload_bytes == 0 {
            return Err(SkeinError::Semantic(
                "content-store row-page qualification message count and payload must be non-zero"
                    .to_string(),
            ));
        }
        if self.segment_cache_capacity_bytes == 0 {
            return Err(SkeinError::Semantic(
                "content-store row-page qualification cache capacity must be non-zero".to_string(),
            ));
        }
        if self.configured_available_memory_bytes == 0 {
            return Err(SkeinError::Semantic(
                "content-store row-page qualification configured memory must be non-zero"
                    .to_string(),
            ));
        }
        if self.resource_profile_kind == ContentStoreResourceProfileKind::SupportedLowMemory
            && self.configured_available_memory_bytes
                != CONTENT_STORE_SUPPORTED_LOW_MEMORY_PROFILE_BYTES
        {
            return Err(SkeinError::Semantic(format!(
                "content-store supported low-memory profile must declare {CONTENT_STORE_SUPPORTED_LOW_MEMORY_PROFILE_BYTES} available bytes"
            )));
        }
        if self.segment_cache_capacity_bytes > self.configured_available_memory_bytes {
            return Err(SkeinError::Semantic(format!(
                "content-store row-page qualification cache capacity {} exceeds configured available memory {}",
                self.segment_cache_capacity_bytes, self.configured_available_memory_bytes
            )));
        }
        if self.resource_read_samples == 0 || self.resource_read_samples > 1024 {
            return Err(SkeinError::Semantic(
                "content-store row-page qualification resource read samples must be between 1 and 1024"
                    .to_string(),
            ));
        }
        let page = corpus_statement(corpus, "thread_messages_page")?;
        let final_message_count = self.base_message_count.saturating_add(3);
        if final_message_count > page.max_rows {
            return Err(SkeinError::Semantic(format!(
                "content-store row-page qualification needs {final_message_count} rows but thread_messages_page admits {}",
                page.max_rows
            )));
        }
        let minimum_payload = self
            .message_payload_bytes
            .checked_mul(final_message_count)
            .ok_or_else(|| {
                SkeinError::Semantic(
                    "content-store row-page qualification payload size overflow".to_string(),
                )
            })?;
        if minimum_payload > page.max_payload_bytes {
            return Err(SkeinError::Semantic(format!(
                "content-store row-page qualification message payloads need at least {minimum_payload} bytes but thread_messages_page admits {}",
                page.max_payload_bytes
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStoreRowPageReadPhase {
    ColdCheckpoint,
    WarmCheckpoint,
    WalRecovery,
    LiveOverlay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRowPageCacheDelta {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub admission_rejections: u64,
    pub resident_bytes_after: u64,
    pub pinned_bytes_after: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRowPageExecutionEvidence {
    pub index_runtime_path: String,
    pub row_runtime_path: String,
    pub base_generation: u64,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: u64,
    pub visible_commit_epoch: u64,
    pub root_set_digest: String,
    pub logical_pages: u64,
    pub logical_bytes: u64,
    pub physical_pages: u64,
    pub physical_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_admission_rejections: u64,
    pub overlay_entries: u64,
    pub overlay_bytes: u64,
    pub rows_visited: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRowPageReadReport {
    pub statement_name: String,
    pub phase: ContentStoreRowPageReadPhase,
    pub max_rows: usize,
    pub max_payload_bytes: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub output_sha256: String,
    pub cache: ContentStoreRowPageCacheDelta,
    pub execution: ContentStoreRowPageExecutionEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreInitialRowPageQualificationReport {
    pub protocol: String,
    pub source_revision: String,
    pub corpus: ContentStoreSqlCorpusIdentity,
    pub schema: ContentStoreSchemaIdentity,
    pub qualified_tables: Vec<String>,
    pub base_message_count: usize,
    pub final_message_count: usize,
    pub message_payload_bytes: usize,
    pub segment_cache_capacity_bytes: u64,
    pub checkpoint_generation: u64,
    pub checkpoint_commit_epoch: u64,
    pub cold_checkpoint_reads: Vec<ContentStoreRowPageReadReport>,
    pub warm_checkpoint_reads: Vec<ContentStoreRowPageReadReport>,
    pub wal_replayed_entries: usize,
    pub wal_replayed_bytes: u64,
    pub wal_recovery_read: ContentStoreRowPageReadReport,
    pub live_overlay_read: ContentStoreRowPageReadReport,
    pub multi_statement_transaction: ContentStoreTransactionQualificationReport,
    pub resources: ContentStoreResourceEvidence,
    pub corruption: ContentStoreCorruptionQualificationReport,
    pub isolation: ContentStoreIsolationQualificationReport,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreTransactionQualificationReport {
    pub inserted_content_message_id: String,
    pub page_output_rows: usize,
    pub page_output_sha256: String,
    pub summary_item_count: i64,
    pub summary_size_bytes: i64,
    pub index_runtime_path: String,
    pub row_runtime_path: String,
    pub transaction_workspace_lookups: u64,
    pub canonical_fallback_lookups: u64,
    pub rejected_statement_atomic: bool,
    pub committed_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreIsolationQualificationReport {
    pub content_message_id: String,
    pub point_max_rows: usize,
    pub point_max_payload_bytes: usize,
    pub row_sha256: String,
    pub cancellation_non_poisoning: bool,
    pub cancellation_pinned_bytes_after: u64,
    pub waiter_lock_timeout_micros: u64,
    pub waiter_timed_out: bool,
    pub waiter_aborted: bool,
    pub owner_rollback_preserved_row: bool,
    pub commit_epoch_before: u64,
    pub commit_epoch_after: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreCorruptionQualificationReport {
    pub artifact_name: String,
    pub artifact_generation: u64,
    pub artifact_bytes: u64,
    pub bit_flip_offset: u64,
    pub scrub_rejected: bool,
    pub damaged_handle_poisoned: bool,
    pub post_failure_sql_rejected: bool,
    pub source_preserved: bool,
    pub source_row_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreResourceEvidence {
    pub profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub segment_cache_capacity_bytes: u64,
    pub max_read_result_rows: Option<usize>,
    pub max_read_result_payload_bytes: Option<usize>,
    pub execution_batch_rows: usize,
    pub execution_batch_payload_bytes: usize,
    pub blocking_operator_bytes: usize,
    pub max_wal_replay_bytes: Option<u64>,
    pub max_out_of_core_delta_bytes: Option<u64>,
    pub read_samples: usize,
    pub read_latency: crate::LatencyPercentiles,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub output_sha256: String,
    pub mutation_latency_micros: u64,
    pub checkpoint_latency_micros: u64,
    pub probe_latency_micros: u64,
    pub logical_mutation_bytes: u64,
    pub wal_append_bytes: u64,
    pub new_generation_artifact_bytes: u64,
    pub durable_write_bytes_lower_bound: u64,
    pub durable_write_amplification_lower_bound_per_million: u64,
    pub write_measurement_scope: String,
    pub process: ContentStoreProcessResourceEvidence,
    pub runtime_memory: ContentStoreRuntimeMemoryEvidence,
    pub observed_peak_within_configured_profile: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreProcessResourceEvidence {
    pub resident_memory_supported: bool,
    pub total_page_faults_supported: bool,
    pub split_page_faults_supported: bool,
    pub start_resident_bytes: u64,
    pub steady_resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub steady_resident_growth_bytes: u64,
    pub lifetime_peak_resident_growth_bytes: u64,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreRuntimeMemoryEvidence {
    pub host_total_bytes: Option<u64>,
    pub host_available_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
    pub cgroup_high_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub effective_limit_bytes: Option<u64>,
    pub effective_available_bytes: Option<u64>,
    pub pressure: String,
}

impl ContentStoreInitialRowPageQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("content-store row-page report is serializable")
    }
}

/// Qualifies the first relational Content Store tables through the public SQL
/// path. The caller owns the new database directory and may retain it as an
/// evidence artifact after this function returns.
pub fn run_content_store_initial_row_page_qualification(
    config: ContentStoreInitialRowPageQualificationConfig,
) -> Result<ContentStoreInitialRowPageQualificationReport> {
    let corpus = nowledge_content_store_sql_corpus()?;
    config.validate(&corpus)?;

    let checkpoint = bootstrap_checkpoint(&config, &corpus)?;
    let authoritative_config = database_config(&config, RelationalIndexMode::Authoritative);
    let mut database = Database::open_with_durability_and_config(
        &config.database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        authoritative_config.clone(),
    )?;

    let read_specs = initial_read_specs(&corpus, config.base_message_count)?;
    let cold_checkpoint_reads = execute_read_set(
        &mut database,
        &read_specs,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
    )?;
    let warm_checkpoint_reads = execute_read_set(
        &mut database,
        &read_specs,
        ContentStoreRowPageReadPhase::WarmCheckpoint,
    )?;
    require_matching_results(&cold_checkpoint_reads, &warm_checkpoint_reads)?;

    upsert_thread_message(
        &mut database,
        &corpus,
        config.base_message_count,
        config.message_payload_bytes,
        "wal",
    )?;
    drop(database);

    let mut database = Database::open_with_durability_and_config(
        &config.database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        authoritative_config.clone(),
    )?;
    let recovery = database.storage_recovery_report();
    if recovery.replayed_wal_entries == 0 {
        return Err(SkeinError::Execution(
            "content-store row-page qualification did not replay the post-checkpoint WAL mutation"
                .to_string(),
        ));
    }
    let page = corpus_statement(&corpus, "thread_messages_page")?;
    let wal_recovery_read = execute_qualified_read(
        &mut database,
        page,
        thread_page_parameters(config.base_message_count + 1),
        ContentStoreRowPageReadPhase::WalRecovery,
        config.base_message_count + 1,
    )?;
    if wal_recovery_read.execution.delta_generation.is_none() {
        return Err(SkeinError::Execution(
            "content-store row-page qualification WAL read did not use a recovery delta"
                .to_string(),
        ));
    }

    upsert_thread_message(
        &mut database,
        &corpus,
        config.base_message_count + 1,
        config.message_payload_bytes,
        "live",
    )?;
    let live_overlay_read = execute_qualified_read(
        &mut database,
        page,
        thread_page_parameters(config.base_message_count + 2),
        ContentStoreRowPageReadPhase::LiveOverlay,
        config.base_message_count + 2,
    )?;
    if live_overlay_read.execution.overlay_entries == 0 {
        return Err(SkeinError::Execution(
            "content-store row-page qualification live read did not use the row overlay"
                .to_string(),
        ));
    }

    let multi_statement_transaction = qualify_multi_statement_transaction(
        &mut database,
        &corpus,
        config.base_message_count + 2,
        config.message_payload_bytes,
    )?;
    let resources = qualify_content_store_resources(
        &mut database,
        &corpus,
        ContentStoreResourceProbeConfig {
            profile_kind: config.resource_profile_kind,
            configured_available_memory_bytes: config.configured_available_memory_bytes,
            read_samples: config.resource_read_samples,
            database_path: &config.database_path,
            database_config: &authoritative_config,
            message_position: config.base_message_count + 2,
            message_payload_bytes: config.message_payload_bytes,
        },
    )?;
    let corruption = qualify_content_store_corruption(
        &mut database,
        &config.database_path,
        &authoritative_config,
        &multi_statement_transaction.inserted_content_message_id,
    )?;
    let isolation = qualify_content_store_isolation(
        database,
        &corpus,
        config.base_message_count + 2,
        config.message_payload_bytes,
    )?;

    Ok(ContentStoreInitialRowPageQualificationReport {
        protocol: CONTENT_STORE_INITIAL_ROW_PAGE_QUALIFICATION_PROTOCOL.to_string(),
        source_revision: config.source_revision,
        corpus: corpus.identity(),
        schema: nowledge_content_store_schema_identity(),
        qualified_tables: QUALIFIED_TABLES
            .iter()
            .map(|table| (*table).to_string())
            .collect(),
        base_message_count: config.base_message_count,
        final_message_count: config.base_message_count + 3,
        message_payload_bytes: config.message_payload_bytes,
        segment_cache_capacity_bytes: config.segment_cache_capacity_bytes,
        checkpoint_generation: checkpoint.generation,
        checkpoint_commit_epoch: checkpoint.commit_epoch,
        cold_checkpoint_reads,
        warm_checkpoint_reads,
        wal_replayed_entries: recovery.replayed_wal_entries,
        wal_replayed_bytes: recovery.replayed_wal_bytes,
        wal_recovery_read,
        live_overlay_read,
        multi_statement_transaction,
        resources,
        corruption,
        isolation,
        ready: true,
    })
}
