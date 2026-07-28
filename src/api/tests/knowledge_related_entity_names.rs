use super::*;

#[test]
fn reads_related_entity_names_for_memory_and_thread_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_a', thread_id: 'logical_a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_b'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_c'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_alpha', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_beta', name: 'Beta'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_empty', name: ''})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_a'}), (e:Entity {id: 'entity_beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_a'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_b'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_b'}), (e:Entity {id: 'entity_empty'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread_a'}), (m:Memory {id: 'memory_a'}) CREATE (t)-[:COMPACTS_TO]->(m)")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread_a'}), (m:Memory {id: 'memory_b'}) CREATE (t)-[:COMPACTS_TO]->(m)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let by_memory_ids = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::MemoryIds(vec![
                "memory_a".to_string(),
                "missing".to_string(),
                "memory_c".to_string(),
            ]),
            limit: 0,
        })
        .unwrap();
    assert_eq!(by_memory_ids.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(
        by_memory_ids.entity_names,
        vec!["Alpha".to_string(), "Beta".to_string()]
    );
    assert_eq!(by_memory_ids.matched_memory_count, 2);
    assert_eq!(by_memory_ids.returned_count, 2);
    assert_eq!(by_memory_ids.missing_memory_ids, vec!["missing"]);
    assert_eq!(by_memory_ids.thread_node_id, None);
    assert_eq!(by_memory_ids.found_thread, None);

    let by_thread = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::Thread {
                thread_id: "thread_a".to_string(),
                identity_property: "id".to_string(),
            },
            limit: 1,
        })
        .unwrap();
    assert_eq!(by_thread.entity_names, vec!["Alpha".to_string()]);
    assert_eq!(by_thread.matched_memory_count, 2);
    assert_eq!(by_thread.returned_count, 1);
    assert!(by_thread.thread_node_id.is_some());
    assert_eq!(by_thread.found_thread, Some(true));

    let by_logical_thread = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::Thread {
                thread_id: "logical_a".to_string(),
                identity_property: "thread_id".to_string(),
            },
            limit: 20,
        })
        .unwrap();
    assert_eq!(
        by_logical_thread.entity_names,
        vec!["Alpha".to_string(), "Beta".to_string()]
    );
    assert_eq!(by_logical_thread.found_thread, Some(true));

    let missing_thread = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::Thread {
                thread_id: "missing".to_string(),
                identity_property: "id".to_string(),
            },
            limit: 20,
        })
        .unwrap();
    assert_eq!(missing_thread.found_thread, Some(false));
    assert_eq!(missing_thread.entity_names, Vec::<String>::new());
    assert_eq!(missing_thread.matched_memory_count, 0);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
}

#[test]
fn related_entity_names_use_query_runtime_plan_cache_for_memory_ids() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'related-cache-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'related-cache-b'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'related-cache-alpha', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'related-cache-beta', name: 'Beta'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'related-cache-empty', name: ''})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'related-cache-a'}), (e:Entity {id: 'related-cache-beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'related-cache-a'}), (e:Entity {id: 'related-cache-alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'related-cache-b'}), (e:Entity {id: 'related-cache-empty'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    let request = KnowledgeRelatedEntityNameListRequest {
        scope: KnowledgeRelatedEntityNameScope::MemoryIds(vec![
            "related-cache-a".to_string(),
            "missing-related-cache".to_string(),
            "related-cache-b".to_string(),
            "missing-related-cache".to_string(),
        ]),
        limit: 1,
    };

    let first = db.knowledge_related_entity_names(&request).unwrap();
    let second = db.knowledge_related_entity_names(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.entity_names, vec!["Alpha".to_string()]);
    assert_eq!(first.matched_memory_count, 2);
    assert_eq!(
        first.missing_memory_ids,
        vec![
            "missing-related-cache".to_string(),
            "missing-related-cache".to_string()
        ]
    );
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.thread_node_id, None);
    assert_eq!(first.found_thread, None);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}

#[test]
fn related_entity_name_read_rejects_invalid_scopes() {
    let db = Database::new();

    let empty_memory_ids = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::MemoryIds(Vec::new()),
            limit: 20,
        })
        .unwrap_err();
    assert!(empty_memory_ids
        .to_string()
        .contains("non-empty memory ids"));

    let empty_memory_id = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::MemoryIds(vec![String::new()]),
            limit: 20,
        })
        .unwrap_err();
    assert!(empty_memory_id.to_string().contains("non-empty memory ids"));

    let empty_thread_id = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::Thread {
                thread_id: String::new(),
                identity_property: "id".to_string(),
            },
            limit: 20,
        })
        .unwrap_err();
    assert!(empty_thread_id.to_string().contains("non-empty thread id"));

    let invalid_identity = db
        .knowledge_related_entity_names(&KnowledgeRelatedEntityNameListRequest {
            scope: KnowledgeRelatedEntityNameScope::Thread {
                thread_id: "thread_a".to_string(),
                identity_property: "metadata".to_string(),
            },
            limit: 20,
        })
        .unwrap_err();
    assert!(invalid_identity
        .to_string()
        .contains("id or thread_id identity"));
}
