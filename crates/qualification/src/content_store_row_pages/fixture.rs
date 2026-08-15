use super::ContentStoreInitialRowPageQualificationConfig;
use crate::{
    nowledge_content_store_schema_statements, ContentStoreSqlCorpus, ContentStoreSqlStatementSpec,
};
use skein::{
    Database, DatabaseConfig, DurabilityPolicy, RelationalIndexMode, Result, SkeinError,
    StorageResidencyMode, Value,
};

pub(super) const QUALIFIED_TABLES: [&str; 2] = ["content_documents", "thread_messages"];
const THREAD_DOCUMENT_ID: &str = "content-doc-thread-1";
const THREAD_OWNER_ID: &str = "thread-1";
const THREAD_STORAGE_ID: &str = "thread-storage-1";

#[derive(Debug, Clone, Copy)]
pub(super) struct CheckpointIdentity {
    pub(super) generation: u64,
    pub(super) commit_epoch: u64,
}

pub(super) fn bootstrap_checkpoint(
    config: &ContentStoreInitialRowPageQualificationConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<CheckpointIdentity> {
    let mut database = Database::open_with_durability_and_config(
        &config.database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config(config, RelationalIndexMode::Shadow),
    )?;
    let mut transaction = database.begin_transaction();
    for statement in nowledge_content_store_schema_statements()? {
        transaction.query_sql(statement)?;
    }
    transaction.commit()?;

    let document = corpus_statement(corpus, "upsert_content_document")?;
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    database.query_sql_with_params(&document.sql, &thread_document_parameters())?;
    for position in 0..config.base_message_count {
        database.query_sql_with_params(
            &message.sql,
            &thread_message_parameters(position, config.message_payload_bytes, "base"),
        )?;
    }
    database.checkpoint()?;
    let report = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            SkeinError::Execution(
                "content-store row-page qualification checkpoint did not publish relational indexes"
                    .to_string(),
            )
        })?;
    Ok(CheckpointIdentity {
        generation: report.generation,
        commit_epoch: report.source_commit_epoch,
    })
}

pub(super) fn database_config(
    config: &ContentStoreInitialRowPageQualificationConfig,
    relational_index_mode: RelationalIndexMode,
) -> DatabaseConfig {
    DatabaseConfig {
        max_read_result_rows: Some(100_000),
        max_read_result_payload_bytes: Some(128 * 1024 * 1024),
        segment_cache_capacity_bytes: config.segment_cache_capacity_bytes,
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        relational_index_mode,
        ..DatabaseConfig::default()
    }
}

pub(super) fn initial_read_specs(
    corpus: &ContentStoreSqlCorpus,
    message_count: usize,
) -> Result<Vec<(&ContentStoreSqlStatementSpec, Vec<Value>, usize)>> {
    Ok(vec![
        (
            corpus_statement(corpus, "thread_owned_document_ids")?,
            vec![Value::String(THREAD_OWNER_ID.to_string())],
            1,
        ),
        (
            corpus_statement(corpus, "thread_messages_page")?,
            thread_page_parameters(message_count),
            message_count,
        ),
        (
            corpus_statement(corpus, "thread_message_summary")?,
            vec![Value::String(THREAD_STORAGE_ID.to_string())],
            1,
        ),
    ])
}

pub(super) fn upsert_thread_message(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    position: usize,
    payload_bytes: usize,
    phase: &str,
) -> Result<()> {
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    database.query_sql_with_params(
        &message.sql,
        &thread_message_parameters(position, payload_bytes, phase),
    )?;
    Ok(())
}

pub(super) fn corpus_statement<'a>(
    corpus: &'a ContentStoreSqlCorpus,
    name: &str,
) -> Result<&'a ContentStoreSqlStatementSpec> {
    corpus.statement(name).ok_or_else(|| {
        SkeinError::Semantic(format!(
            "content-store row-page qualification requires statement {name}"
        ))
    })
}

fn thread_document_parameters() -> Vec<Value> {
    vec![
        Value::String(THREAD_DOCUMENT_ID.to_string()),
        Value::String("thread".to_string()),
        Value::String(THREAD_OWNER_ID.to_string()),
        Value::String("default".to_string()),
        Value::String("application/x-nowledge-thread".to_string()),
        Value::Int(1),
        Value::String("2026-01-01T00:00:00Z".to_string()),
        Value::String("2026-01-01T00:00:00Z".to_string()),
    ]
}

fn thread_message_parameters(position: usize, payload_bytes: usize, phase: &str) -> Vec<Value> {
    let content_message_id = format!("content-message-{position:08}");
    vec![
        Value::String(content_message_id.clone()),
        Value::String(format!("message-{position:08}")),
        Value::String(THREAD_STORAGE_ID.to_string()),
        Value::String(THREAD_OWNER_ID.to_string()),
        Value::String(THREAD_DOCUMENT_ID.to_string()),
        Value::String("default".to_string()),
        Value::Int(position as i64),
        Value::String(
            if position.is_multiple_of(2) {
                "user"
            } else {
                "assistant"
            }
            .to_string(),
        ),
        Value::String(format!("{phase}:{}", "x".repeat(payload_bytes))),
        Value::String(format!("2026-01-01T00:00:{:02}Z", position % 60)),
        Value::Int((position + 1) as i64),
        Value::String(format!("{{\"phase\":\"{phase}\"}}")),
        Value::String(format!("external-{position:08}")),
        Value::Bool(false),
        Value::String(format!("hash-{phase}-{position:08}")),
        Value::String("2026-01-01T00:00:00Z".to_string()),
        Value::String("2026-01-01T00:00:00Z".to_string()),
    ]
}

pub(super) fn thread_page_parameters(limit: usize) -> Vec<Value> {
    vec![
        Value::String(THREAD_STORAGE_ID.to_string()),
        Value::Int(i64::try_from(limit).unwrap_or(i64::MAX)),
        Value::Int(0),
    ]
}
