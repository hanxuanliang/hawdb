use super::*;

#[test]
fn reads_memory_evolves_latest_for_rest_skills_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'old_a', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'old_b', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'new_a', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'new_b', is_latest: true})")
        .unwrap();
    db.query("CREATE (:Source {id: 'not_memory_target'})")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old_a'}), (new:Memory {id: 'new_b'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old_a'}), (new:Memory {id: 'new_b'}) CREATE (old)-[:EVOLVES {duplicate: true}]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old_b'}), (new:Memory {id: 'new_a'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old_a'}), (source:Source {id: 'not_memory_target'}) CREATE (old)-[:EVOLVES]->(source)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_memory_evolves_latest(&KnowledgeMemoryEvolvesLatestRequest {
            old_memory_ids: vec![
                "old_a".to_string(),
                "old_b".to_string(),
                "missing".to_string(),
                "old_a".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(output.matched_old_memory_count, 2);
    assert_eq!(output.missing_old_memory_ids, vec!["missing".to_string()]);
    assert_eq!(output.matched_relationship_count, 3);
    assert_eq!(output.returned_count, 2);
    assert_eq!(output.rows[0].new_memory_id.as_deref(), Some("new_a"));
    assert_eq!(output.rows[0].new_is_latest, Some(false));
    assert_eq!(output.rows[1].new_memory_id.as_deref(), Some("new_b"));
    assert_eq!(output.rows[1].new_is_latest, Some(true));

    let tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'new_c', is_latest: true})")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old_b'}), (new:Memory {id: 'new_c'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();
    let snapshot = tx
        .knowledge_memory_evolves_latest(&KnowledgeMemoryEvolvesLatestRequest {
            old_memory_ids: vec!["old_b".to_string()],
        })
        .unwrap();
    assert_eq!(snapshot.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(snapshot.returned_count, 1);
    assert_eq!(snapshot.rows[0].new_memory_id.as_deref(), Some("new_a"));
}

#[test]
fn memory_evolves_latest_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'latest-cache-old-a', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'latest-cache-old-b', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'latest-cache-new-a', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'latest-cache-new-b', is_latest: true})")
        .unwrap();
    db.query("CREATE (:Source {id: 'latest-cache-not-memory'})")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'latest-cache-old-a'}), (new:Memory {id: 'latest-cache-new-b'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'latest-cache-old-a'}), (new:Memory {id: 'latest-cache-new-b'}) CREATE (old)-[:EVOLVES {duplicate: true}]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'latest-cache-old-b'}), (new:Memory {id: 'latest-cache-new-a'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'latest-cache-old-a'}), (source:Source {id: 'latest-cache-not-memory'}) CREATE (old)-[:EVOLVES]->(source)")
        .unwrap();
    let request = KnowledgeMemoryEvolvesLatestRequest {
        old_memory_ids: vec![
            "latest-cache-old-a".to_string(),
            "missing-latest-cache".to_string(),
            "latest-cache-old-b".to_string(),
            "latest-cache-old-a".to_string(),
        ],
    };

    let first = db.knowledge_memory_evolves_latest(&request).unwrap();
    let second = db.knowledge_memory_evolves_latest(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_old_memory_count, 2);
    assert_eq!(
        first.missing_old_memory_ids,
        vec!["missing-latest-cache".to_string()]
    );
    assert_eq!(first.matched_relationship_count, 3);
    assert_eq!(first.returned_count, 2);
    assert_eq!(
        first.rows[0].new_memory_id.as_deref(),
        Some("latest-cache-new-a")
    );
    assert_eq!(first.rows[0].new_is_latest, Some(false));
    assert_eq!(
        first.rows[1].new_memory_id.as_deref(),
        Some("latest-cache-new-b")
    );
    assert_eq!(first.rows[1].new_is_latest, Some(true));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 3);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 3);
}

#[test]
fn memory_evolves_latest_handles_empty_input_and_rejects_empty_ids() {
    let db = Database::new();
    let empty = db
        .knowledge_memory_evolves_latest(&KnowledgeMemoryEvolvesLatestRequest {
            old_memory_ids: Vec::new(),
        })
        .unwrap();
    assert_eq!(empty.returned_count, 0);
    assert_eq!(empty.matched_old_memory_count, 0);
    assert!(empty.missing_old_memory_ids.is_empty());

    let empty_id = db
        .knowledge_memory_evolves_latest(&KnowledgeMemoryEvolvesLatestRequest {
            old_memory_ids: vec![String::new()],
        })
        .unwrap_err();
    assert!(empty_id.to_string().contains("non-empty memory ids"));
}
