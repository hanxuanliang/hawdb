use super::*;

#[test]
fn persists_nodes_across_reopen_with_wal_replay() {
    let path = unique_test_dir("wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn strict_recovery_rejects_torn_wal_tail() {
    let path = unique_test_dir("strict_torn_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(path.join("wal.skein"))
        .unwrap()
        .write_all(b"torn-entry-without-checksum")
        .unwrap();

    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            recovery_mode: RecoveryMode::Strict,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("strict WAL recovery rejected torn tail"));

    let mut tolerant = Database::open(&path).unwrap();
    let output = tolerant
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_replay_entry_limit_rejects_long_recovery() {
    let path = unique_test_dir("wal_replay_limit");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    }

    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_wal_replay_entries: Some(1),
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("WAL replay entry limit exceeded"));

    let mut db = Database::open(&path).unwrap();
    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn storage_recovery_report_tracks_wal_replay_boundary() {
    let path = unique_test_dir("storage_recovery_report");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Checkpointed'})")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Replayed'})")
            .unwrap();
    }

    let db = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_wal_replay_entries: Some(8),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let report = db.storage_recovery_report();
    assert!(report.durable);
    assert_eq!(report.recovery_mode, RecoveryMode::TolerateTornTail);
    assert_eq!(report.max_wal_replay_entries, Some(8));
    assert_eq!(report.checkpoint_epoch, Some(1));
    assert_eq!(report.checkpoint_commit_epoch, Some(1));
    assert!(report.wal_present);
    assert_eq!(report.wal_replay_start_lsn, Some(1));
    assert_eq!(report.next_lsn_after_replay, Some(2));
    assert_eq!(report.replayed_wal_entries, 1);
    assert!(!report.torn_tail_ignored);
    assert_eq!(report.torn_tail_reason, None);
    assert_eq!(report.recovered_commit_epoch, 2);

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mem_shaped_graph_mutations_recover_across_checkpoint_and_wal() {
    let path = unique_test_dir("mem_shaped_recovery");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Source {id: 'source:one', space_id: 'default', kind: 'thread', metadata: '{}', memory_count: 1})",
        )
        .unwrap();
        db.query(
            "CREATE (:Thread {id: 'thread:one', thread_id: 'thread:one', source_id: 'source:one', space_id: 'default', message_count: 2})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'mem:checkpointed', title: 'Checkpointed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default', importance: 0.4, confidence: 0.8, is_latest: true})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        {
            let mut tx = db.begin_transaction();
            tx.query(
                "CREATE (:Memory {id: 'mem:replayed', title: 'Replayed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default', importance: 0.9, confidence: 0.7, lifecycle_state: 'active', is_latest: true})-[:MENTIONS {thread_id: 'thread:one', message_index: 1, confidence: 0.7}]->(:Entity {id: 'entity:rust', name: 'Rust', space_id: 'default', unit_type: 'entity'})",
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert!(wal.contains("\tbatch\t"));
    assert!(wal.contains("create_node"));
    assert!(wal.contains("create_rel"));

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                max_wal_replay_entries: Some(8),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);

        let recovery = db.storage_recovery_report();
        assert_eq!(recovery.checkpoint_epoch, Some(1));
        assert_eq!(recovery.checkpoint_commit_epoch, Some(3));
        assert_eq!(recovery.replayed_wal_entries, 1);
        assert_eq!(recovery.max_wal_replay_entries, Some(8));
        assert_eq!(recovery.recovered_commit_epoch, 4);

        let mem_recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&recovery);
        assert!(mem_recovery.ready);
        assert!(mem_recovery.durable_recovery_observed);
        assert!(mem_recovery.checkpoint_boundary_present);
        assert!(mem_recovery.wal_replay_bounded);
        assert!(mem_recovery.torn_tail_clean);
        assert!(mem_recovery.blocker_codes.is_empty());
        assert_eq!(mem_recovery.json()["readiness"]["wal_replay_bounded"], true);
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mem_shaped_post_checkpoint_batch_replays_before_torn_tail() {
    let path = unique_test_dir("mem_shaped_recovery_torn_tail");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Source {id: 'source:one', space_id: 'default', kind: 'thread', metadata: '{}', memory_count: 1})",
        )
        .unwrap();
        db.query(
            "CREATE (:Thread {id: 'thread:one', thread_id: 'thread:one', source_id: 'source:one', space_id: 'default', message_count: 2})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'mem:checkpointed', title: 'Checkpointed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default'})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        let mut tx = db.begin_transaction();
        tx.query(
            "CREATE (:Memory {id: 'mem:replayed', title: 'Replayed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default'})-[:MENTIONS {thread_id: 'thread:one', message_index: 1, confidence: 0.7}]->(:Entity {id: 'entity:rust', name: 'Rust', space_id: 'default', unit_type: 'entity'})",
        )
        .unwrap();
        tx.commit().unwrap();

        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    std::fs::OpenOptions::new()
        .append(true)
        .open(path.join("wal.skein"))
        .unwrap()
        .write_all(b"torn-entry-without-checksum")
        .unwrap();

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                max_wal_replay_entries: Some(8),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);

        let recovery = db.storage_recovery_report();
        assert_eq!(recovery.checkpoint_epoch, Some(1));
        assert_eq!(recovery.checkpoint_commit_epoch, Some(3));
        assert_eq!(recovery.replayed_wal_entries, 1);
        assert_eq!(recovery.max_wal_replay_entries, Some(8));
        assert_eq!(recovery.recovered_commit_epoch, 4);
        assert!(recovery.torn_tail_ignored);
        assert!(recovery.torn_tail_reason.is_some());

        let mem_recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&recovery);
        assert!(!mem_recovery.ready);
        assert!(mem_recovery.durable_recovery_observed);
        assert!(mem_recovery.checkpoint_boundary_present);
        assert!(mem_recovery.wal_replay_bounded);
        assert!(!mem_recovery.torn_tail_clean);
        assert_eq!(
            mem_recovery.blocker_codes,
            vec!["torn_tail_observed".to_string()]
        );
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn in_memory_storage_recovery_report_is_non_durable() {
    let db = Database::new();
    let report = db.storage_recovery_report();
    assert!(!report.durable);
    assert_eq!(report.recovered_commit_epoch, 0);
    assert_eq!(report.replayed_wal_entries, 0);
    assert_eq!(report.max_wal_replay_entries, None);
    assert_eq!(report.checkpoint_epoch, None);
    assert_eq!(report.next_lsn_after_replay, None);
}

#[test]
fn read_only_open_does_not_create_missing_database_path() {
    let path = unique_test_dir("read_only_missing");
    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("read-only database path does not exist"));
    assert!(!path.exists());
}

#[test]
fn read_only_open_loads_existing_database_without_allowing_writes() {
    let path = unique_test_dir("read_only_existing");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );

        let error = db.query("CREATE (:Memory {id: 2})").unwrap_err();
        assert!(error.to_string().contains("read-only mode"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoints_nodes_and_truncates_wal() {
    let path = unique_test_dir("checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_query_invokes_storage_checkpoint() {
    let path = unique_test_dir("checkpoint_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        let output = db.query("CHECKPOINT;").unwrap();
        assert!(output.rows.is_empty());
    }

    let checkpoint = read_test_durable_text(&path.join("checkpoint.skein")).unwrap();
    assert!(checkpoint.contains("node\t"));
    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");

    std::fs::remove_dir_all(path).unwrap();
}
