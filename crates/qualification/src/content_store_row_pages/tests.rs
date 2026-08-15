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
    assert_eq!(report.final_message_count, 6);
    assert_eq!(report.cold_checkpoint_reads.len(), 3);
    assert_eq!(report.warm_checkpoint_reads.len(), 3);
    assert!(report.wal_replayed_entries > 0);
    assert!(report
        .wal_recovery_read
        .execution
        .delta_generation
        .is_some());
    assert!(report.live_overlay_read.execution.overlay_entries > 0);
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
