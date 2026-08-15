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

    let report = run_content_store_initial_row_page_qualification(config)
        .expect("initial Content Store row pages should qualify");

    assert!(report.ready);
    assert_eq!(report.qualified_tables, QUALIFIED_TABLES);
    assert_eq!(report.final_message_count, 7);
    assert_eq!(report.cold_checkpoint_reads.len(), 3);
    assert_eq!(report.warm_checkpoint_reads.len(), 3);
    assert!(report.wal_replayed_entries > 0);
    assert!(report
        .wal_recovery_read
        .execution
        .delta_generation
        .is_some());
    assert!(report.live_overlay_read.execution.overlay_entries > 0);
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
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .chain(&report.warm_checkpoint_reads)
        .all(|read| read.execution.row_runtime_path == "snapshot_rows"));
    assert!(report
        .cold_checkpoint_reads
        .iter()
        .chain(&report.warm_checkpoint_reads)
        .all(|read| read.execution.index_runtime_path == "authoritative"));
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
