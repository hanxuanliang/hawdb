use super::*;

#[test]
fn reads_memory_prefix_ownership_for_mcp_skill_guard() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'skill:alpha:1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'skill:alpha:2', space_id: 'team'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'skill:beta:1', space_id: 'team'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'skill:alpha:entity', space_id: 'team'})")
        .unwrap();
    db.query("CREATE (:Memory {title: 'Idless'})").unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_memory_prefix_ownership(&KnowledgeMemoryPrefixOwnershipRequest {
            prefix: "skill:alpha:".to_string(),
            limit: 2,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(output.prefix, "skill:alpha:");
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.returned_count, 2);
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row.memory_id.as_str())
            .collect::<Vec<_>>(),
        vec!["skill:alpha:1", "skill:alpha:2"]
    );
    assert_eq!(output.rows[0].raw_space_id.as_deref(), Some(""));
    assert_eq!(output.rows[0].normalized_space_id, "default");
    assert_eq!(output.rows[1].raw_space_id.as_deref(), Some("team"));
    assert_eq!(output.rows[1].normalized_space_id, "team");

    let snapshot = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'skill:alpha:3', space_id: 'late'})")
        .unwrap();
    let snapshot_output = snapshot
        .knowledge_memory_prefix_ownership(&KnowledgeMemoryPrefixOwnershipRequest {
            prefix: "skill:alpha:".to_string(),
            limit: 0,
        })
        .unwrap();
    assert_eq!(snapshot_output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(snapshot_output.matched_count, 2);
    assert_eq!(
        snapshot_output
            .rows
            .iter()
            .map(|row| row.memory_id.as_str())
            .collect::<Vec<_>>(),
        vec!["skill:alpha:1", "skill:alpha:2"]
    );
}

#[test]
fn memory_prefix_ownership_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'skill:cache:1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'skill:cache:2', space_id: 'team'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'skill:other:1', space_id: 'team'})")
        .unwrap();
    let request = KnowledgeMemoryPrefixOwnershipRequest {
        prefix: "skill:cache:".to_string(),
        limit: 1,
    };

    let first = db.knowledge_memory_prefix_ownership(&request).unwrap();
    let second = db.knowledge_memory_prefix_ownership(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].memory_id, "skill:cache:1");
    assert_eq!(first.rows[0].raw_space_id.as_deref(), Some(""));
    assert_eq!(first.rows[0].normalized_space_id, "default");
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn memory_prefix_ownership_rejects_empty_prefix_without_wal() {
    let path = unique_test_dir("memory_prefix_ownership_empty_prefix_without_wal");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'skill:alpha:1', space_id: 'team'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let error = db
        .knowledge_memory_prefix_ownership(&KnowledgeMemoryPrefixOwnershipRequest {
            prefix: String::new(),
            limit: 2,
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty prefix"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
}
