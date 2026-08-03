use super::*;

#[test]
fn counts_memory_evolves_relations_for_decay_scheduler_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'decay-source-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'decay-source-b'})").unwrap();
    db.query("CREATE (:Memory {id: 'decay-target-confirm'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'decay-target-enrich'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'decay-target-ignore'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'decay-not-memory'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (n:Memory {id: 'decay-target-confirm'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (n:Memory {id: 'decay-target-enrich'}) CREATE (m)-[:EVOLVES {content_relation: 'enriches'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (n:Memory {id: 'decay-target-ignore'}) CREATE (m)-[:EVOLVES {content_relation: 'contradicts'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (s:Source {id: 'decay-not-memory'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(s)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-b'}), (n:Memory {id: 'decay-target-confirm'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(n)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let snapshot = db.begin_read_transaction();

    db.query("MATCH (m:Memory {id: 'decay-source-b'}), (n:Memory {id: 'decay-target-enrich'}) CREATE (m)-[:EVOLVES {content_relation: 'enriches'}]->(n)")
        .unwrap();

    let counts = db
        .knowledge_memory_evolves_relation_counts(&KnowledgeMemoryEvolvesRelationCountRequest {
            memory_ids: vec![
                "decay-source-a".to_string(),
                "missing-decay".to_string(),
                "decay-source-b".to_string(),
                "decay-source-a".to_string(),
            ],
            content_relations: vec!["confirms".to_string(), "enriches".to_string()],
        })
        .unwrap();
    assert_eq!(counts.graph_commit_epoch, db.store.commit_epoch());
    assert_eq!(counts.matched_memory_count, 2);
    assert_eq!(counts.missing_memory_ids, vec!["missing-decay".to_string()]);
    assert_eq!(counts.matched_relationship_count, 4);
    assert_eq!(counts.returned_count, 2);
    assert_eq!(counts.rows[0].memory_id, "decay-source-a");
    assert_eq!(counts.rows[0].count, 2);
    assert_eq!(counts.rows[1].memory_id, "decay-source-b");
    assert_eq!(counts.rows[1].count, 2);

    let snapshot_counts = snapshot
        .knowledge_memory_evolves_relation_counts(&KnowledgeMemoryEvolvesRelationCountRequest {
            memory_ids: vec!["decay-source-b".to_string()],
            content_relations: vec!["confirms".to_string(), "enriches".to_string()],
        })
        .unwrap();
    assert_eq!(snapshot_counts.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(snapshot_counts.matched_relationship_count, 1);
    assert_eq!(snapshot_counts.rows[0].count, 1);
}

#[test]
fn memory_evolves_relation_counts_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'evolves-count-cache-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'evolves-count-cache-b'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'evolves-count-cache-target'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'evolves-count-cache-source'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'evolves-count-cache-a'}), (n:Memory {id: 'evolves-count-cache-target'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'evolves-count-cache-a'}), (n:Memory {id: 'evolves-count-cache-target'}) CREATE (m)-[:EVOLVES {content_relation: 'enriches'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'evolves-count-cache-a'}), (n:Memory {id: 'evolves-count-cache-target'}) CREATE (m)-[:EVOLVES {content_relation: 'contradicts'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'evolves-count-cache-b'}), (s:Source {id: 'evolves-count-cache-source'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(s)")
        .unwrap();
    let request = KnowledgeMemoryEvolvesRelationCountRequest {
        memory_ids: vec![
            "evolves-count-cache-a".to_string(),
            "missing-evolves-count-cache".to_string(),
            "evolves-count-cache-b".to_string(),
            "evolves-count-cache-a".to_string(),
        ],
        content_relations: vec!["confirms".to_string(), "enriches".to_string()],
    };

    let first = db
        .knowledge_memory_evolves_relation_counts(&request)
        .unwrap();
    let second = db
        .knowledge_memory_evolves_relation_counts(&request)
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_memory_count, 2);
    assert_eq!(
        first.missing_memory_ids,
        vec!["missing-evolves-count-cache".to_string()]
    );
    assert_eq!(first.matched_relationship_count, 2);
    assert_eq!(first.returned_count, 1);
    assert_eq!(first.rows[0].memory_id, "evolves-count-cache-a");
    assert_eq!(first.rows[0].count, 2);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}

#[test]
fn memory_evolves_relation_counts_rejects_empty_fields_without_wal() {
    let path = unique_test_dir("memory_evolves_relation_counts_empty_without_wal");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'decay-source'})").unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let memory_id_error = db
        .knowledge_memory_evolves_relation_counts(&KnowledgeMemoryEvolvesRelationCountRequest {
            memory_ids: vec![String::new()],
            content_relations: vec!["confirms".to_string()],
        })
        .unwrap_err();
    assert!(memory_id_error.to_string().contains("non-empty memory ids"));

    let relation_error = db
        .knowledge_memory_evolves_relation_counts(&KnowledgeMemoryEvolvesRelationCountRequest {
            memory_ids: vec!["decay-source".to_string()],
            content_relations: vec![String::new()],
        })
        .unwrap_err();
    assert!(relation_error
        .to_string()
        .contains("non-empty content relations"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}
