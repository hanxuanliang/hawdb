use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn initial_content_store_tables_are_qualified_through_canonical_row_pages() {
    let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "skein-content-store-row-page-qualification-{}-{id}",
        std::process::id()
    ));
    let mut config =
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-source-revision");
    config.base_message_count = 4;
    config.message_payload_bytes = 16 * 1024;
    config.base_chunk_count = 5;
    config.chunk_payload_bytes = 12 * 1024;

    let report = run_content_store_initial_row_page_qualification(config)
        .expect("initial Content Store row pages should qualify");

    assert!(report.ready);
    assert_eq!(report.qualified_tables, QUALIFIED_TABLES);
    assert_eq!(report.final_message_count, 7);
    assert_eq!(report.base_chunk_count, 5);
    assert_eq!(report.final_chunk_count, 7);
    assert_eq!(report.cold_checkpoint_reads.len(), 9);
    assert_eq!(report.warm_checkpoint_reads.len(), 9);
    assert!(report.wal_replayed_entries > 0);
    assert!(report
        .wal_recovery_read
        .execution
        .delta_generation
        .is_some());
    assert_eq!(
        report.wal_recovery_read.execution.visible_commit_epoch,
        report.wal_content_commit_epoch
    );
    assert!(report
        .wal_recovery_chunk_read
        .execution
        .delta_generation
        .is_some());
    assert!(report
        .wal_recovery_anchor_read
        .execution
        .delta_generation
        .is_some());
    assert_eq!(
        report
            .wal_recovery_chunk_read
            .execution
            .visible_commit_epoch,
        report.wal_content_commit_epoch
    );
    assert_eq!(
        report
            .wal_recovery_anchor_read
            .execution
            .visible_commit_epoch,
        report.wal_content_commit_epoch
    );
    assert!(report.live_overlay_read.execution.overlay_entries > 0);
    assert_eq!(
        report.live_overlay_read.execution.visible_commit_epoch,
        report.live_content_commit_epoch
    );
    assert!(report.live_overlay_chunk_read.execution.overlay_entries > 0);
    assert!(report.live_overlay_anchor_read.execution.overlay_entries > 0);
    assert_eq!(
        report
            .live_overlay_chunk_read
            .execution
            .visible_commit_epoch,
        report.live_content_commit_epoch
    );
    assert_eq!(
        report
            .live_overlay_anchor_read
            .execution
            .visible_commit_epoch,
        report.live_content_commit_epoch
    );
    assert_eq!(
        report.multi_statement_transaction.index_runtime_path,
        "transaction_workspace"
    );
    assert_eq!(
        report.multi_statement_transaction.row_runtime_path,
        "canonical_memory"
    );
    assert!(report.multi_statement_transaction.rejected_statement_atomic);
    assert_eq!(
        report
            .multi_statement_transaction
            .canonical_fallback_lookups,
        0
    );
    assert!(
        report
            .multi_statement_transaction
            .transaction_workspace_lookups
            > 0
    );
    assert_eq!(
        report.multi_statement_transaction.page_output_rows,
        report.final_message_count
    );
    assert_eq!(report.multi_statement_transaction.summary_item_count, 7);
    assert!(report.multi_statement_transaction.summary_size_bytes > 0);
    assert!(
        report.multi_statement_transaction.committed_epoch
            > report.live_overlay_read.execution.visible_commit_epoch
    );
    assert!(report.isolation.cancellation_non_poisoning);
    assert_eq!(report.isolation.cancellation_pinned_bytes_after, 0);
    assert!(report.isolation.waiter_timed_out);
    assert!(report.isolation.waiter_aborted);
    assert!(report.isolation.owner_rollback_preserved_row);
    assert_eq!(
        report.isolation.commit_epoch_before,
        report.isolation.commit_epoch_after
    );
    assert_eq!(
        report.isolation.content_message_id,
        report
            .multi_statement_transaction
            .inserted_content_message_id
    );
    assert!(report.corruption.scrub_rejected);
    assert!(report.corruption.damaged_handle_poisoned);
    assert!(report.corruption.post_failure_sql_rejected);
    assert!(report.corruption.source_preserved);
    assert!(report.corruption.artifact_bytes > 0);
    assert!(report.corruption.bit_flip_offset < report.corruption.artifact_bytes);
    assert_eq!(
        report.resources.profile_kind,
        ContentStoreResourceProfileKind::Capability512Mib
    );
    assert_eq!(
        report.resources.configured_available_memory_bytes,
        CONTENT_STORE_512_MIB_CAPABILITY_BYTES
    );
    assert_eq!(report.resources.read_samples, 16);
    assert_eq!(
        report.resources.segment_cache_capacity_bytes,
        report.segment_cache_capacity_bytes
    );
    assert_eq!(
        report.resources.max_relational_index_read_bytes,
        16 * 1024 * 1024
    );
    assert_eq!(
        report.resources.max_relational_hydration_bytes,
        64 * 1024 * 1024
    );
    assert_eq!(report.resources.max_read_result_rows, Some(100_000));
    assert_eq!(
        report.resources.max_read_result_payload_bytes,
        Some(128 * 1024 * 1024)
    );
    assert!(report.resources.execution_batch_rows > 0);
    assert!(report.resources.execution_batch_payload_bytes > 0);
    assert!(report.resources.blocking_operator_bytes > 0);
    assert_eq!(report.resources.read_latency.sample_count, 16);
    assert_eq!(report.resources.output_rows, report.final_message_count);
    assert!(report.resources.logical_mutation_bytes > 0);
    assert!(report.resources.wal_append_bytes > 0);
    assert!(report.resources.new_generation_artifact_bytes > 0);
    assert!(report.resources.durable_write_bytes_lower_bound > 0);
    assert!(report.resources.process.resident_memory_supported);
    assert!(report.resources.process.total_page_faults_supported);
    assert!(report.resources.observed_peak_within_configured_profile);
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .chain(&report.warm_checkpoint_reads)
        .all(|read| read.execution.row_runtime_path == "snapshot_rows"));
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .map(|read| read.statement_name.as_str())
        .eq([
            "thread_owned_document_ids",
            "thread_messages_page",
            "thread_message_summary",
            "source_chunks_page",
            "source_chunks_by_source",
            "source_chunk_count_by_source",
            "source_document_payload_summary",
            "thread_covered_message_count",
            "content_status_anchor_count",
        ]));
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .chain(&report.warm_checkpoint_reads)
        .all(|read| matches!(
            read.execution.index_runtime_path.as_str(),
            "authoritative" | "none"
        )));
    assert!(!report
        .json()
        .to_string()
        .contains(path.to_string_lossy().as_ref()));

    std::fs::remove_dir_all(path).expect("remove qualification database");
}

#[test]
fn qualification_rejects_an_existing_database_path() {
    let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "skein-content-store-row-page-existing-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("create existing qualification path");

    let error = run_content_store_initial_row_page_qualification(
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-revision"),
    )
    .expect_err("existing qualification path must be rejected");

    assert!(error.to_string().contains("requires a new database path"));
    std::fs::remove_dir_all(path).expect("remove qualification path");
}

#[test]
fn capability_512_mib_is_evidence_identity_not_a_universal_limit() {
    let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "skein-content-store-row-page-memory-profile-{}-{id}",
        std::process::id()
    ));
    let mut low_memory =
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-revision");
    low_memory.configured_available_memory_bytes = 256 * 1024 * 1024;
    let low_memory_error = run_content_store_initial_row_page_qualification(low_memory)
        .expect_err("the 512 MiB capability profile must retain its exact identity");
    assert!(low_memory_error
        .to_string()
        .contains("512 MiB capability profile must declare"));

    let mut configured =
        ContentStoreInitialRowPageQualificationConfig::synthetic(&path, "test-revision");
    configured.resource_profile_kind = ContentStoreResourceProfileKind::ConfiguredWorkload;
    configured.configured_available_memory_bytes = 2 * 1024 * 1024 * 1024;
    assert!(configured
        .validate(&nowledge_content_store_sql_corpus().unwrap())
        .is_ok());
}
