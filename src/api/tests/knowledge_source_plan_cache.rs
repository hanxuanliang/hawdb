use super::*;

#[test]
fn knowledge_source_ids_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Source {id: 'source_a', lifecycle_state: 'extracted', space_id: 'default'})",
    )
    .unwrap();
    db.query("CREATE (:Source {id: 'source_b', lifecycle_state: 'raw', space_id: 'default'})")
        .unwrap();
    let request = KnowledgeSourceIdListRequest {
        lifecycle_state: Some("extracted".to_string()),
        normalized_space_id: Some("default".to_string()),
        limit: 10,
    };

    let first = db.knowledge_source_ids(&request).unwrap();
    let second = db.knowledge_source_ids(&request).unwrap();

    assert_eq!(first.source_ids, vec!["source_a".to_string()]);
    assert_eq!(first, second);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);

    let summary = db
        .query_sql(
            "SELECT execution_count FROM system.statement_summary \
             WHERE statement_kind = 'match_return' \
             ORDER BY execution_count DESC LIMIT 1",
        )
        .unwrap();

    assert_eq!(summary.rows[0].get("execution_count"), Some(&Value::Int(2)));
}

#[test]
fn knowledge_source_count_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source_a'})").unwrap();
    db.query("CREATE (:Source {id: 'source_b'})").unwrap();

    let first = db.knowledge_source_count();
    let second = db.knowledge_source_count();

    assert_eq!(first.count, 2);
    assert_eq!(first, second);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);

    let summary = db
        .query_sql(
            "SELECT execution_count, total_row_count FROM system.statement_summary \
             WHERE query_text = 'MATCH (s:Source) RETURN count(s) AS count'",
        )
        .unwrap();

    assert_eq!(summary.rows.len(), 1);
    assert_eq!(summary.rows[0].get("execution_count"), Some(&Value::Int(2)));
    assert_eq!(summary.rows[0].get("total_row_count"), Some(&Value::Int(2)));
}

#[test]
fn knowledge_sources_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source_a', original_name: 'Alpha', memory_count: 2})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_b', original_name: 'Beta', memory_count: 5})")
        .unwrap();
    let request = KnowledgeSourceListRequest {
        source_ids: vec!["source_b".to_string(), "source_a".to_string()],
        limit: 1,
        order: KnowledgeSourceListOrder::MemoryCountDesc,
        ..KnowledgeSourceListRequest::default()
    };

    let first = db.knowledge_sources(&request).unwrap();
    let second = db.knowledge_sources(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].source_id.as_deref(), Some("source_b"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 3);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 3);
}

#[test]
fn knowledge_source_projected_list_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source_a', original_name: 'Alpha', memory_count: 2})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_b', original_name: 'Beta', memory_count: 5})")
        .unwrap();
    let request = KnowledgeSourceProjectedListRequest {
        list: KnowledgeSourceListRequest {
            limit: 1,
            order: KnowledgeSourceListOrder::MemoryCountDesc,
            ..KnowledgeSourceListRequest::default()
        },
        property_names: vec!["original_name".to_string()],
    };

    let first = db.knowledge_source_projected_list(&request).unwrap();
    let second = db.knowledge_source_projected_list(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].source_id.as_deref(), Some("source_b"));
    assert_eq!(
        first.rows[0].properties.get("original_name"),
        Some(&Value::String("Beta".to_string()))
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}

#[test]
fn knowledge_source_detail_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source_a', title: 'Alpha', space_id: '', memory_count: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_a'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_a'}), (s:Source {id: 'source_a'}) \
         CREATE (m)-[:SOURCED_FROM]->(s)",
    )
    .unwrap();
    let request = KnowledgeSourceRequest {
        source_id: "source_a".to_string(),
    };

    let first = db.knowledge_source(&request).unwrap();
    let second = db.knowledge_source(&request).unwrap();

    assert_eq!(first, second);
    let row = first.row.expect("source row");
    assert_eq!(row.source_id.as_deref(), Some("source_a"));
    assert_eq!(row.title.as_deref(), Some("Alpha"));
    assert_eq!(row.normalized_space_id, "default");
    assert_eq!(row.memory_count, Some(1));
    assert_eq!(row.sourced_memory_count, 1);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}

#[test]
fn knowledge_source_sourced_memory_count_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source_a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_a'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_a'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_a'}), (s:Source {id: 'source_a'}) \
         CREATE (m)-[:SOURCED_FROM]->(s)",
    )
    .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity_a'}), (s:Source {id: 'source_a'}) \
         CREATE (e)-[:SOURCED_FROM]->(s)",
    )
    .unwrap();
    let request = KnowledgeSourceSourcedMemoryCountRequest {
        source_id: "source_a".to_string(),
    };

    let first = db.knowledge_source_sourced_memory_count(&request).unwrap();
    let second = db.knowledge_source_sourced_memory_count(&request).unwrap();

    assert_eq!(first, second);
    assert!(first.found);
    assert_eq!(first.sourced_memory_count, 1);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}

#[test]
fn knowledge_source_memories_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source_a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_a', title: 'Alpha', content: 'alpha', unit_type: 'fact', confidence: 0.8})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_b', title: 'Beta', content: 'beta', unit_type: 'note', confidence: 0.6})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_a'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_a'}), (s:Source {id: 'source_a'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 2, chunk_range: '10..20', source_version: 'v1', created_at: 200}]->(s)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_b'}), (s:Source {id: 'source_a'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 1, chunk_range: '0..10', source_version: 'v1', created_at: 100}]->(s)")
        .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity_a'}), (s:Source {id: 'source_a'}) \
         CREATE (e)-[:SOURCED_FROM {chunk_index: 0}]->(s)",
    )
    .unwrap();
    let request = KnowledgeSourceMemoryListRequest {
        source_id: "source_a".to_string(),
        limit: 1,
    };

    let first = db.knowledge_source_memories(&request).unwrap();
    let second = db.knowledge_source_memories(&request).unwrap();

    assert_eq!(first, second);
    assert!(first.found);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].memory_id.as_deref(), Some("memory_b"));
    assert_eq!(first.rows[0].chunk_index, Some(1));
    assert_eq!(first.rows[0].created_at, Some(Value::Int(100)));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 3);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 3);
}

#[test]
fn knowledge_source_memory_projected_list_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source_a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_a', title: 'Alpha', unit_type: 'fact', space_id: 'space_a', hidden: 'no'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_b', title: 'Beta', unit_type: 'note', space_id: '', hidden: 'no'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_a', title: 'Entity'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_a'}), (s:Source {id: 'source_a'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 2, chunk_range: '10..20', source_version: 'v1', hidden: 'no'}]->(s)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_b'}), (s:Source {id: 'source_a'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 1, chunk_range: '0..10', source_version: 'v2', hidden: 'no'}]->(s)")
        .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity_a'}), (s:Source {id: 'source_a'}) \
         CREATE (e)-[:SOURCED_FROM {chunk_index: 0, chunk_range: 'ignored'}]->(s)",
    )
    .unwrap();
    let request = KnowledgeSourceMemoryProjectedListRequest {
        list: KnowledgeSourceMemoryListRequest {
            source_id: "source_a".to_string(),
            limit: 1,
        },
        memory_property_names: vec!["title".to_string(), "unit_type".to_string()],
        relationship_property_names: vec!["chunk_range".to_string()],
    };

    let first = db.knowledge_source_memory_projected_list(&request).unwrap();
    let second = db.knowledge_source_memory_projected_list(&request).unwrap();

    assert_eq!(first, second);
    assert!(first.found);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].memory_id.as_deref(), Some("memory_b"));
    assert_eq!(first.rows[0].normalized_space_id, "default");
    assert_eq!(
        first.rows[0].memory_properties,
        BTreeMap::from([
            ("title".to_string(), Value::String("Beta".to_string())),
            ("unit_type".to_string(), Value::String("note".to_string())),
        ])
    );
    assert_eq!(
        first.rows[0].relationship_properties,
        BTreeMap::from([(
            "chunk_range".to_string(),
            Value::String("0..10".to_string())
        )])
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 3);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 3);
}
