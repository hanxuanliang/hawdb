use super::*;

#[test]
fn lists_memories_for_nowledge_bulk_detail_and_space_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_bulk_1', title: 'Bulk One', content: 'body one', metadata: '{\"rank\":1}', is_latest: true, lifecycle_state: 'active', review_status: 'accepted', unit_type: 'note', space_id: '', created_at: 10, updated_at: 20, importance: 0.7, pagerank_score: 0.9, community_id: 7, source: 'feed', event_start: 30, event_end: 40})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_bulk_2', title: 'Bulk Two', content: 'body two', metadata: '{\"rank\":2}', is_latest: false, lifecycle_state: 'archived', review_status: 'pending', unit_type: 'note', space_id: 'archive', created_at: 11, updated_at: 21, importance: 0.3})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_bulk_3', title: 'Bulk Three', unit_type: 'learning', is_latest: true, is_crystal: false, space_id: 'default', created_at: 30})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let bulk = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: vec![
                "memory_bulk_1".to_string(),
                "memory_bulk_2".to_string(),
                "missing".to_string(),
            ],
            normalized_space_id: None,
            exclude_normalized_space_id: None,
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 0,
            order: KnowledgeMemoryListOrder::ExternalIdAsc,
        })
        .unwrap();

    assert_eq!(bulk.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(bulk.matched_count, 2);
    assert_eq!(bulk.returned_count, 2);
    assert_eq!(bulk.missing_external_ids, vec!["missing".to_string()]);
    assert_eq!(bulk.rows[0].memory_id.as_deref(), Some("memory_bulk_1"));
    assert_eq!(bulk.rows[0].title.as_deref(), Some("Bulk One"));
    assert_eq!(bulk.rows[0].content.as_deref(), Some("body one"));
    assert_eq!(
        bulk.rows[0].metadata,
        Some(Value::String("{\"rank\":1}".to_string()))
    );
    assert_eq!(bulk.rows[0].is_latest, Some(true));
    assert_eq!(bulk.rows[0].lifecycle_state.as_deref(), Some("active"));
    assert_eq!(bulk.rows[0].review_status.as_deref(), Some("accepted"));
    assert_eq!(bulk.rows[0].normalized_space_id, "default");
    assert_eq!(bulk.rows[0].raw_space_id.as_deref(), Some(""));
    assert_eq!(bulk.rows[0].created_at, Some(Value::Int(10)));
    assert_eq!(bulk.rows[0].updated_at, Some(Value::Int(20)));
    assert_eq!(bulk.rows[0].importance, Some(Value::Float(0.7)));
    assert_eq!(bulk.rows[0].pagerank_score, Some(Value::Float(0.9)));
    assert_eq!(bulk.rows[0].community_id, Some(Value::Int(7)));
    assert_eq!(bulk.rows[0].source.as_deref(), Some("feed"));
    assert_eq!(bulk.rows[0].event_start, Some(Value::Int(30)));
    assert_eq!(bulk.rows[0].event_end, Some(Value::Int(40)));

    let default_space = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: Vec::new(),
            normalized_space_id: Some("default".to_string()),
            exclude_normalized_space_id: None,
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 0,
            order: KnowledgeMemoryListOrder::ExternalIdAsc,
        })
        .unwrap();
    assert_eq!(default_space.matched_count, 2);
    assert_eq!(
        default_space.rows[0].memory_id.as_deref(),
        Some("memory_bulk_1")
    );
    assert_eq!(
        default_space.rows[1].memory_id.as_deref(),
        Some("memory_bulk_3")
    );

    let candidate_default = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: vec![
                "memory_bulk_1".to_string(),
                "memory_bulk_2".to_string(),
                "missing".to_string(),
            ],
            normalized_space_id: Some("default".to_string()),
            exclude_normalized_space_id: None,
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 0,
            order: KnowledgeMemoryListOrder::ExternalIdAsc,
        })
        .unwrap();
    assert_eq!(candidate_default.matched_count, 1);
    assert_eq!(
        candidate_default.missing_external_ids,
        vec!["memory_bulk_2".to_string(), "missing".to_string()]
    );

    let not_default = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: vec!["memory_bulk_1".to_string(), "memory_bulk_2".to_string()],
            normalized_space_id: None,
            exclude_normalized_space_id: Some("default".to_string()),
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 0,
            order: KnowledgeMemoryListOrder::ExternalIdAsc,
        })
        .unwrap();
    assert_eq!(not_default.matched_count, 1);
    assert_eq!(
        not_default.rows[0].memory_id.as_deref(),
        Some("memory_bulk_2")
    );
}

#[test]
fn lists_memories_for_learning_latest_and_ranked_overview_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'learning_old', title: 'Learning Old', content: 'old', unit_type: 'learning', is_latest: true, is_crystal: false, created_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'learning_new', title: 'Learning New', content: 'new', unit_type: 'learning', is_latest: true, is_crystal: false, created_at: 20})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'learning_crystal', unit_type: 'learning', is_latest: true, is_crystal: true, created_at: 30})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'overview_high', title: 'High', pagerank_score: 3.0, importance: 0.1, created_at: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'overview_mid', content: 'mid body', importance: 2.0, created_at: 2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'overview_default', title: 'Default', created_at: 3})")
        .unwrap();

    let learning = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: Vec::new(),
            normalized_space_id: None,
            exclude_normalized_space_id: None,
            unit_type: Some("learning".to_string()),
            is_latest: Some(true),
            is_crystal: Some(false),
            limit: 40,
            order: KnowledgeMemoryListOrder::CreatedAtDesc,
        })
        .unwrap();
    assert_eq!(learning.matched_count, 2);
    assert_eq!(learning.rows[0].memory_id.as_deref(), Some("learning_new"));
    assert_eq!(learning.rows[1].memory_id.as_deref(), Some("learning_old"));

    let ranked = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: vec![
                "overview_default".to_string(),
                "overview_high".to_string(),
                "overview_mid".to_string(),
            ],
            normalized_space_id: None,
            exclude_normalized_space_id: None,
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 2,
            order: KnowledgeMemoryListOrder::ScoreDesc,
        })
        .unwrap();
    assert_eq!(ranked.matched_count, 3);
    assert_eq!(ranked.returned_count, 2);
    assert_eq!(ranked.rows[0].memory_id.as_deref(), Some("overview_high"));
    assert_eq!(ranked.rows[1].memory_id.as_deref(), Some("overview_mid"));
}

#[test]
fn memory_list_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory-list-cache-a', title: 'Cache A', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: '', created_at: 10, importance: 0.4})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-list-cache-b', title: 'Cache B', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'default', created_at: 20, pagerank_score: 0.9})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-list-cache-team', title: 'Team', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'team', created_at: 30})")
        .unwrap();
    let request = KnowledgeMemoryListRequest {
        external_ids: vec![
            "memory-list-cache-a".to_string(),
            "memory-list-cache-b".to_string(),
            "memory-list-cache-missing".to_string(),
        ],
        normalized_space_id: Some("default".to_string()),
        exclude_normalized_space_id: None,
        unit_type: Some("fact".to_string()),
        is_latest: Some(true),
        is_crystal: Some(false),
        limit: 1,
        order: KnowledgeMemoryListOrder::ScoreDesc,
    };

    let first = db.knowledge_memories(&request).unwrap();
    let second = db.knowledge_memories(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(
        first.rows[0].memory_id.as_deref(),
        Some("memory-list-cache-b")
    );
    assert_eq!(first.rows[0].title.as_deref(), Some("Cache B"));
    assert_eq!(first.rows[0].normalized_space_id, "default");
    assert_eq!(
        first.missing_external_ids,
        vec!["memory-list-cache-missing".to_string()]
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn projects_memory_list_fields_for_nowledge_growth() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_projected_a', title: 'Projected A', content: 'body a', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: '', created_at: 10, importance: 0.4, metadata: '{\"rank\":1}', future_field: 'future-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_projected_b', title: 'Projected B', content: 'body b', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'team', created_at: 20, pagerank_score: 0.9, importance: 0.1, metadata: '{\"rank\":2}', future_field: 'future-b'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_projected_crystal', title: 'Crystal', unit_type: 'fact', is_latest: true, is_crystal: true, created_at: 30, pagerank_score: 2.0})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let projected = db
        .knowledge_memory_projected_list(&KnowledgeMemoryProjectedListRequest {
            list: KnowledgeMemoryListRequest {
                external_ids: Vec::new(),
                normalized_space_id: None,
                exclude_normalized_space_id: None,
                unit_type: Some("fact".to_string()),
                is_latest: Some(true),
                is_crystal: Some(false),
                limit: 10,
                order: KnowledgeMemoryListOrder::ScoreDesc,
            },
            property_names: vec![
                "title".to_string(),
                "content".to_string(),
                "metadata".to_string(),
                "space_id".to_string(),
                "future_field".to_string(),
                "title".to_string(),
            ],
        })
        .unwrap();

    assert_eq!(projected.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(projected.matched_count, 2);
    assert_eq!(projected.returned_count, 2);
    assert_eq!(
        projected
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("memory_projected_b"), Some("memory_projected_a")]
    );
    assert_eq!(projected.rows[0].normalized_space_id, "team");
    assert_eq!(projected.rows[1].normalized_space_id, "default");
    assert_eq!(
        projected.rows[0].properties.get("future_field"),
        Some(&Value::String("future-b".to_string()))
    );
    assert_eq!(
        projected.rows[0].properties.get("title"),
        Some(&Value::String("Projected B".to_string()))
    );
    assert_eq!(projected.rows[0].properties.get("pagerank_score"), None);
    assert_eq!(
        projected.rows[1].properties.get("space_id"),
        Some(&Value::String(String::new()))
    );

    let created_at_order = db
        .knowledge_memory_projected_list(&KnowledgeMemoryProjectedListRequest {
            list: KnowledgeMemoryListRequest {
                external_ids: Vec::new(),
                normalized_space_id: None,
                exclude_normalized_space_id: None,
                unit_type: Some("fact".to_string()),
                is_latest: Some(true),
                is_crystal: Some(false),
                limit: 10,
                order: KnowledgeMemoryListOrder::CreatedAtDesc,
            },
            property_names: vec!["title".to_string()],
        })
        .unwrap();
    assert_eq!(
        created_at_order
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("memory_projected_b"), Some("memory_projected_a")]
    );
    assert_eq!(created_at_order.rows[0].properties.get("created_at"), None);
}

#[test]
fn memory_projected_list_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory-cache-a', title: 'Cache A', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: '', created_at: 10, importance: 0.4, future_field: 'future-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-cache-b', title: 'Cache B', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'default', created_at: 20, pagerank_score: 0.9, future_field: 'future-b'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-cache-team', title: 'Team', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'team', created_at: 30, future_field: 'wrong-space'})")
        .unwrap();
    let request = KnowledgeMemoryProjectedListRequest {
        list: KnowledgeMemoryListRequest {
            external_ids: vec![
                "memory-cache-a".to_string(),
                "memory-cache-b".to_string(),
                "memory-cache-missing".to_string(),
            ],
            normalized_space_id: Some("default".to_string()),
            exclude_normalized_space_id: None,
            unit_type: Some("fact".to_string()),
            is_latest: Some(true),
            is_crystal: Some(false),
            limit: 1,
            order: KnowledgeMemoryListOrder::ScoreDesc,
        },
        property_names: vec!["title".to_string(), "future_field".to_string()],
    };

    let first = db.knowledge_memory_projected_list(&request).unwrap();
    let second = db.knowledge_memory_projected_list(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].memory_id.as_deref(), Some("memory-cache-b"));
    assert_eq!(
        first.rows[0].properties.get("future_field"),
        Some(&Value::String("future-b".to_string()))
    );
    assert_eq!(
        first.missing_external_ids,
        vec!["memory-cache-missing".to_string()]
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn projected_memory_list_rejects_empty_property_names_without_wal() {
    let path = unique_test_dir("projected_memory_list_empty_property_without_wal");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'memory_projected_wal', unit_type: 'fact'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let error = db
        .knowledge_memory_projected_list(&KnowledgeMemoryProjectedListRequest {
            list: KnowledgeMemoryListRequest {
                external_ids: vec!["memory_projected_wal".to_string()],
                normalized_space_id: None,
                exclude_normalized_space_id: None,
                unit_type: None,
                is_latest: None,
                is_crystal: None,
                limit: 10,
                order: KnowledgeMemoryListOrder::ExternalIdAsc,
            },
            property_names: vec![String::new()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty property names"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
}

#[test]
fn memory_list_rejects_unbounded_or_empty_filters() {
    let db = Database::new();

    let empty_id_error = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: vec![String::new()],
            normalized_space_id: None,
            exclude_normalized_space_id: None,
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 10,
            order: KnowledgeMemoryListOrder::ExternalIdAsc,
        })
        .unwrap_err();
    assert!(empty_id_error
        .to_string()
        .contains("non-empty external ids"));

    let empty_space_error = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: Vec::new(),
            normalized_space_id: Some(String::new()),
            exclude_normalized_space_id: None,
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 10,
            order: KnowledgeMemoryListOrder::ExternalIdAsc,
        })
        .unwrap_err();
    assert!(empty_space_error
        .to_string()
        .contains("non-empty normalized space ids"));

    let unbounded_error = db
        .knowledge_memories(&KnowledgeMemoryListRequest {
            external_ids: Vec::new(),
            normalized_space_id: None,
            exclude_normalized_space_id: None,
            unit_type: None,
            is_latest: None,
            is_crystal: None,
            limit: 0,
            order: KnowledgeMemoryListOrder::ExternalIdAsc,
        })
        .unwrap_err();
    assert!(unbounded_error
        .to_string()
        .contains("filter or bounded limit"));
}
