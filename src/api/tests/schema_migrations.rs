use super::*;

#[test]
fn applies_schema_migration_log_batch_idempotently() {
    let mut db = Database::new();
    db.query("CREATE (:SchemaMigrationLog {id: 'existing', applied_at: 10})")
        .unwrap();

    let output = db
        .apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![
                KnowledgeSchemaMigrationApply {
                    migration_id: "existing".to_string(),
                    applied_at: Value::Int(100),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_1".to_string(),
                    applied_at: Value::Int(101),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_1".to_string(),
                    applied_at: Value::Int(102),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_2".to_string(),
                    applied_at: Value::Int(103),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.created_count, 2);
    assert_eq!(output.already_applied_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert!(output.rows[0].already_applied);
    assert!(output.rows[1].created);
    assert!(output.rows[1].node_id.is_some());
    assert!(output.rows[2].duplicate);
    assert!(output.rows[3].created);

    let rows = db
        .query("MATCH (m:SchemaMigrationLog) RETURN m.id AS id, m.applied_at AS applied_at ORDER BY id ASC")
        .unwrap();
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(
        rows.rows[0].get("id"),
        Some(&Value::String("existing".to_string()))
    );
    assert_eq!(rows.rows[0].get("applied_at"), Some(&Value::Int(10)));
    assert_eq!(
        rows.rows[1].get("id"),
        Some(&Value::String("new_1".to_string()))
    );
    assert_eq!(rows.rows[1].get("applied_at"), Some(&Value::Int(101)));
    assert_eq!(
        rows.rows[2].get("id"),
        Some(&Value::String("new_2".to_string()))
    );
    assert_eq!(rows.rows[2].get("applied_at"), Some(&Value::Int(103)));

    let graph_commit_epoch = db.store.commit_epoch();
    let applied = db.knowledge_schema_migrations(&KnowledgeSchemaMigrationListRequest { limit: 0 });
    assert_eq!(applied.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(applied.matched_count, 3);
    assert_eq!(applied.returned_count, 3);
    assert_eq!(
        applied
            .rows
            .iter()
            .map(|row| row.migration_id.as_str())
            .collect::<Vec<_>>(),
        vec!["existing", "new_1", "new_2"]
    );
    assert_eq!(applied.rows[0].applied_at, Some(Value::Int(10)));
    assert_eq!(applied.rows[1].applied_at, Some(Value::Int(101)));
    assert_eq!(applied.rows[2].applied_at, Some(Value::Int(103)));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);

    let limited = db.knowledge_schema_migrations(&KnowledgeSchemaMigrationListRequest { limit: 2 });
    assert_eq!(limited.matched_count, 3);
    assert_eq!(limited.returned_count, 2);
    assert_eq!(
        limited
            .rows
            .iter()
            .map(|row| row.migration_id.as_str())
            .collect::<Vec<_>>(),
        vec!["existing", "new_1"]
    );
}

#[test]
fn schema_migration_apply_rejects_empty_id_before_wal() {
    let mut db = Database::new();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![KnowledgeSchemaMigrationApply {
                migration_id: String::new(),
                applied_at: Value::Int(100),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty migration id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_schema_migration_apply_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_schema_migration_apply_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .ok()
            .map_or(0, |wal| wal.matches("\tbatch\t").count());
        db.apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![
                KnowledgeSchemaMigrationApply {
                    migration_id: "migration_1".to_string(),
                    applied_at: Value::Int(100),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "migration_2".to_string(),
                    applied_at: Value::Int(200),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_node"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (m:SchemaMigrationLog) RETURN m.id AS id, m.applied_at AS applied_at ORDER BY id ASC")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("id"),
            Some(&Value::String("migration_1".to_string()))
        );
        assert_eq!(rows.rows[0].get("applied_at"), Some(&Value::Int(100)));
        assert_eq!(
            rows.rows[1].get("id"),
            Some(&Value::String("migration_2".to_string()))
        );
        assert_eq!(rows.rows[1].get("applied_at"), Some(&Value::Int(200)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
