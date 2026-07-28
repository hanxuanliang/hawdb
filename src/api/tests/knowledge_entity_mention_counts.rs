use super::*;

#[test]
fn reads_entity_mention_counts_for_wiki_listing_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_b'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_alpha', name: 'Alpha', updated_at: 10})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_beta', name: 'Beta', updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_gamma', name: 'Gamma', updated_at: 30})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_noname', updated_at: 40})")
        .unwrap();
    db.query("CREATE (:Entity {name: 'No Id', updated_at: 50})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_a'}), (e:Entity {id: 'entity_beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_b'}), (e:Entity {id: 'entity_beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_a'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (e1:Entity {id: 'entity_alpha'}), (e2:Entity {id: 'entity_gamma'}) CREATE (e1)-[:MENTIONS]->(e2)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_entity_mention_counts(&KnowledgeEntityMentionCountListRequest {
            cursor: None,
            limit: 0,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.returned_count, 3);
    assert_eq!(output.rows[0].entity_id, "entity_beta");
    assert_eq!(output.rows[0].name, "Beta");
    assert_eq!(output.rows[0].updated_at, Some(Value::Int(20)));
    assert_eq!(output.rows[0].mention_count, 2);
    assert_eq!(output.rows[1].entity_id, "entity_alpha");
    assert_eq!(output.rows[1].mention_count, 1);
    assert_eq!(output.rows[2].entity_id, "entity_gamma");
    assert_eq!(output.rows[2].mention_count, 0);

    let cursor_output = db
        .knowledge_entity_mention_counts(&KnowledgeEntityMentionCountListRequest {
            cursor: Some(KnowledgeEntityMentionCountCursor {
                after_count: 1,
                after_name: "Alpha".to_string(),
            }),
            limit: 1,
        })
        .unwrap();
    assert_eq!(cursor_output.matched_count, 1);
    assert_eq!(cursor_output.returned_count, 1);
    assert_eq!(cursor_output.rows[0].entity_id, "entity_gamma");

    let snapshot = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'memory_c'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_c'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    let snapshot_output = snapshot
        .knowledge_entity_mention_counts(&KnowledgeEntityMentionCountListRequest {
            cursor: None,
            limit: 2,
        })
        .unwrap();
    assert_eq!(snapshot_output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(snapshot_output.rows[0].entity_id, "entity_beta");
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch + 2);
}

#[test]
fn entity_mention_counts_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'mention-count-cache-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mention-count-cache-b'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'mention-count-entity-alpha', name: 'Alpha', updated_at: 10})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'mention-count-entity-beta', name: 'Beta', updated_at: 20})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'mention-count-cache-a'}), (e:Entity {id: 'mention-count-entity-beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'mention-count-cache-b'}), (e:Entity {id: 'mention-count-entity-beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    let request = KnowledgeEntityMentionCountListRequest {
        cursor: Some(KnowledgeEntityMentionCountCursor {
            after_count: 2,
            after_name: "Beta".to_string(),
        }),
        limit: 1,
    };

    let first = db.knowledge_entity_mention_counts(&request).unwrap();
    let second = db.knowledge_entity_mention_counts(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 1);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].entity_id, "mention-count-entity-alpha");
    assert_eq!(first.rows[0].mention_count, 0);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn entity_mention_count_read_rejects_empty_cursor_name() {
    let db = Database::new();

    let error = db
        .knowledge_entity_mention_counts(&KnowledgeEntityMentionCountListRequest {
            cursor: Some(KnowledgeEntityMentionCountCursor {
                after_count: 1,
                after_name: String::new(),
            }),
            limit: 10,
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty after_name"));
}
