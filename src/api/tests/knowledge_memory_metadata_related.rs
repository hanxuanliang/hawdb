use super::*;

#[test]
fn projects_metadata_related_memories_for_rest_list_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'metadata_related_old', title: 'Old related', content: 'old body', metadata: '{\"source_id\": \"external-thread-1\"}', space_id: '', created_at: 10, importance: 0.4, future_field: 'old-future'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'metadata_related_new', title: 'New related', content: 'new body', metadata: '{\"source_thread_id\":\"external-thread-1\"}', space_id: 'default', created_at: 20, importance: 0.9, future_field: 'new-future'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'metadata_related_other_space', title: 'Other space', metadata: '{\"source_id\":\"external-thread-1\"}', space_id: 'team', created_at: 30, future_field: 'wrong-space'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'metadata_related_other_source', title: 'Other source', metadata: '{\"source_id\":\"external-thread-2\"}', space_id: 'default', created_at: 40, future_field: 'wrong-source'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'metadata_related_after_snapshot', title: 'After snapshot', metadata: '{\"source_id\":\"external-thread-1\"}', space_id: 'default', created_at: 50, future_field: 'after-snapshot'})")
        .unwrap();

    let projected = db
        .knowledge_memory_metadata_related_projected_list(
            &KnowledgeMemoryMetadataRelatedProjectedListRequest {
                normalized_space_id: "default".to_string(),
                source_id: "external-thread-1".to_string(),
                limit: 2,
                property_names: vec![
                    "title".to_string(),
                    "future_field".to_string(),
                    "space_id".to_string(),
                    "title".to_string(),
                ],
            },
        )
        .unwrap();
    assert_eq!(projected.matched_count, 3);
    assert_eq!(projected.returned_count, 2);
    assert_eq!(
        projected
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("metadata_related_after_snapshot"),
            Some("metadata_related_new")
        ]
    );
    assert_eq!(
        projected.rows[0].properties.get("future_field"),
        Some(&Value::String("after-snapshot".to_string()))
    );
    assert!(!projected.rows[0].properties.contains_key("created_at"));
    assert_eq!(projected.rows[1].normalized_space_id, "default");

    let snapshot_projected = snapshot
        .knowledge_memory_metadata_related_projected_list(
            &KnowledgeMemoryMetadataRelatedProjectedListRequest {
                normalized_space_id: "default".to_string(),
                source_id: "external-thread-1".to_string(),
                limit: 10,
                property_names: vec!["title".to_string(), "space_id".to_string()],
            },
        )
        .unwrap();
    assert_eq!(snapshot_projected.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(snapshot_projected.matched_count, 2);
    assert_eq!(
        snapshot_projected
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("metadata_related_new"), Some("metadata_related_old")]
    );
    assert_eq!(
        snapshot_projected.rows[1].properties.get("space_id"),
        Some(&Value::String(String::new()))
    );
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch + 1);
}

#[test]
fn metadata_related_memory_projected_list_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'metadata-cache-old', title: 'Old related', metadata: '{\"source_id\": \"external-thread-cache\"}', space_id: '', created_at: 10, future_field: 'old-future'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'metadata-cache-new', title: 'New related', metadata: '{\"source_thread_id\":\"external-thread-cache\"}', space_id: 'default', created_at: 20, future_field: 'new-future'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'metadata-cache-other', title: 'Other', metadata: '{\"source_id\":\"external-thread-other\"}', space_id: 'default', created_at: 30, future_field: 'wrong-source'})")
        .unwrap();
    let request = KnowledgeMemoryMetadataRelatedProjectedListRequest {
        normalized_space_id: "default".to_string(),
        source_id: "external-thread-cache".to_string(),
        limit: 1,
        property_names: vec!["title".to_string(), "future_field".to_string()],
    };

    let first = db
        .knowledge_memory_metadata_related_projected_list(&request)
        .unwrap();
    let second = db
        .knowledge_memory_metadata_related_projected_list(&request)
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(
        first.rows[0].memory_id.as_deref(),
        Some("metadata-cache-new")
    );
    assert_eq!(
        first.rows[0].properties.get("future_field"),
        Some(&Value::String("new-future".to_string()))
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn metadata_related_memory_projected_list_rejects_invalid_input_without_wal() {
    let path = unique_test_dir("metadata_related_memory_projected_invalid_without_wal");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'metadata_related_wal', metadata: '{\"source_id\":\"source\"}', space_id: 'default'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    for request in [
        KnowledgeMemoryMetadataRelatedProjectedListRequest {
            normalized_space_id: String::new(),
            source_id: "source".to_string(),
            limit: 10,
            property_names: vec!["title".to_string()],
        },
        KnowledgeMemoryMetadataRelatedProjectedListRequest {
            normalized_space_id: "default".to_string(),
            source_id: String::new(),
            limit: 10,
            property_names: vec!["title".to_string()],
        },
        KnowledgeMemoryMetadataRelatedProjectedListRequest {
            normalized_space_id: "default".to_string(),
            source_id: "source".to_string(),
            limit: 0,
            property_names: vec!["title".to_string()],
        },
        KnowledgeMemoryMetadataRelatedProjectedListRequest {
            normalized_space_id: "default".to_string(),
            source_id: "source".to_string(),
            limit: 10,
            property_names: vec![String::new()],
        },
    ] {
        db.knowledge_memory_metadata_related_projected_list(&request)
            .unwrap_err();
    }

    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
}
