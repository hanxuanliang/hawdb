use super::*;

#[test]
fn reads_memory_entities_for_nowledge_mentions_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    db.query(
        "CREATE (:Entity {id: 'entity_b', name: 'Beta', entity_type: 'concept', confidence: 0.7})",
    )
    .unwrap();
    db.query(
        "CREATE (:Entity {id: 'entity_a', name: 'Alpha', entity_type: 'person', confidence: 0.9})",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_b'}) CREATE (m)-[:MENTIONS {confidence: 0.42, mention_count: 2}]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_a'}) CREATE (m)-[:MENTIONS {confidence: 0.84, mention_count: 3}]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_2'}), (e:Entity {id: 'entity_a'}) CREATE (m)-[:MENTIONS {confidence: 0.21, mention_count: 1}]->(e)",
    )
    .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_memory_entities(&KnowledgeMemoryEntityListRequest {
            memory_ids: vec![
                "memory_1".to_string(),
                "memory_2".to_string(),
                "missing".to_string(),
            ],
            limit_per_memory: 1,
            distinct_name_limit: 20,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(output.found_memory_count, 2);
    assert_eq!(output.missing_memory_count, 1);
    assert_eq!(output.entity_count, 2);
    assert_eq!(
        output.distinct_entity_names,
        vec!["Alpha".to_string(), "Beta".to_string()]
    );
    assert_eq!(output.groups.len(), 3);
    assert!(output.groups[0].found);
    assert_eq!(output.groups[0].memory_id, "memory_1");
    assert_eq!(output.groups[0].matched_count, 2);
    assert_eq!(output.groups[0].returned_count, 1);
    assert_eq!(
        output.groups[0].entities[0].entity_id.as_deref(),
        Some("entity_a")
    );
    assert_eq!(output.groups[0].entities[0].name.as_deref(), Some("Alpha"));
    assert_eq!(
        output.groups[0].entities[0].entity_type.as_deref(),
        Some("person")
    );
    assert_eq!(
        output.groups[0].entities[0].confidence,
        Some(Value::Float(0.9))
    );
    assert_eq!(
        output.groups[0].entities[0].relationship_confidence,
        Some(Value::Float(0.84))
    );
    assert_eq!(output.groups[0].entities[0].mention_count, Some(3));
    assert!(output.groups[1].found);
    assert_eq!(output.groups[1].matched_count, 1);
    assert_eq!(output.groups[1].returned_count, 1);
    assert!(!output.groups[2].found);
    assert_eq!(output.groups[2].memory_node_id, None);

    let limited_names = db
        .knowledge_memory_entities(&KnowledgeMemoryEntityListRequest {
            memory_ids: vec!["memory_1".to_string()],
            limit_per_memory: 0,
            distinct_name_limit: 1,
        })
        .unwrap();
    assert_eq!(limited_names.groups[0].returned_count, 2);
    assert_eq!(
        limited_names.distinct_entity_names,
        vec!["Alpha".to_string()]
    );
}

#[test]
fn memory_entities_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory-entity-cache-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-entity-cache-b'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-cache-b', name: 'Beta', entity_type: 'concept', confidence: 0.7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-cache-a', name: 'Alpha', entity_type: 'person', confidence: 0.9})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-entity-cache-a'}), (e:Entity {id: 'entity-cache-b'}) CREATE (m)-[:MENTIONS {confidence: 0.42, mention_count: 2}]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-entity-cache-a'}), (e:Entity {id: 'entity-cache-a'}) CREATE (m)-[:MENTIONS {confidence: 0.84, mention_count: 3}]->(e)")
        .unwrap();
    let request = KnowledgeMemoryEntityListRequest {
        memory_ids: vec![
            "memory-entity-cache-a".to_string(),
            "memory-entity-cache-b".to_string(),
            "memory-entity-cache-missing".to_string(),
        ],
        limit_per_memory: 1,
        distinct_name_limit: 10,
    };

    let first = db.knowledge_memory_entities(&request).unwrap();
    let second = db.knowledge_memory_entities(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.found_memory_count, 2);
    assert_eq!(first.missing_memory_count, 1);
    assert_eq!(first.entity_count, 1);
    assert_eq!(first.groups[0].matched_count, 2);
    assert_eq!(first.groups[0].returned_count, 1);
    assert_eq!(
        first.groups[0].entities[0].entity_id.as_deref(),
        Some("entity-cache-a")
    );
    assert!(first.groups[1].found);
    assert_eq!(first.groups[1].matched_count, 0);
    assert!(!first.groups[2].found);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}

#[test]
fn memory_entity_read_rejects_empty_memory_ids() {
    let db = Database::new();

    let empty_list_error = db
        .knowledge_memory_entities(&KnowledgeMemoryEntityListRequest {
            memory_ids: Vec::new(),
            limit_per_memory: 10,
            distinct_name_limit: 10,
        })
        .unwrap_err();
    assert!(empty_list_error
        .to_string()
        .contains("non-empty memory ids"));

    let empty_id_error = db
        .knowledge_memory_entities(&KnowledgeMemoryEntityListRequest {
            memory_ids: vec![String::new()],
            limit_per_memory: 10,
            distinct_name_limit: 10,
        })
        .unwrap_err();
    assert!(empty_id_error.to_string().contains("non-empty memory ids"));
}
