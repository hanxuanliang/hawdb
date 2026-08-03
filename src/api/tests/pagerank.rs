use super::*;

#[test]
fn updates_and_clears_pagerank_scores_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_rank_1', title: 'Rank One'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_rank_2', title: 'Rank Two'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_rank_1', name: 'Entity One'})")
        .unwrap();
    db.query("CREATE (:Entity {name: 'Projected Entity', pagerank_score: 0.9})")
        .unwrap();

    let output = db
        .update_knowledge_pagerank_scores_batch(&KnowledgePageRankScoreBatchRequest {
            updates: vec![
                KnowledgePageRankScoreUpdate {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                    score: 0.42,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                    score: 0.84,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                    score: 0.99,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Entity".to_string(),
                    external_id: "missing".to_string(),
                    score: 0.1,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db
        .knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                },
            ],
            property_names: vec!["pagerank_score".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("pagerank_score"),
        Some(&Some(Value::Float(0.42)))
    );
    assert_eq!(
        rows.rows[1].properties.get("pagerank_score"),
        Some(&Some(Value::Float(0.84)))
    );

    let clear = db
        .clear_knowledge_pagerank_scores(&KnowledgePageRankClearRequest {
            labels: vec!["Entity".to_string(), "Memory".to_string()],
        })
        .unwrap();
    assert_eq!(clear.graph_commit_epoch_before, 5);
    assert_eq!(clear.graph_commit_epoch_after, 6);
    assert_eq!(clear.candidate_count, 3);
    assert_eq!(clear.cleared_count, 2);
    assert_eq!(clear.non_writable_count, 1);
    assert_eq!(clear.rows.iter().filter(|row| row.cleared).count(), 2);
    assert_eq!(clear.rows.iter().filter(|row| row.non_writable).count(), 1);

    let rows = db
        .knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                },
            ],
            property_names: vec!["pagerank_score".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("pagerank_score"),
        Some(&Some(Value::Null))
    );
    assert_eq!(
        rows.rows[1].properties.get("pagerank_score"),
        Some(&Some(Value::Null))
    );
    let still_scored = db
        .query("MATCH (e:Entity) WHERE e.pagerank_score IS NOT NULL RETURN COUNT(e) AS total")
        .unwrap();
    assert_eq!(still_scored.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn pagerank_score_batch_rejects_invalid_score_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_rank_1', title: 'Rank One'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_pagerank_scores_batch(&KnowledgePageRankScoreBatchRequest {
            updates: vec![KnowledgePageRankScoreUpdate {
                label: "Memory".to_string(),
                external_id: "memory_rank_1".to_string(),
                score: f64::NAN,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("finite non-negative score"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_pagerank_score_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_pagerank_score_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_rank_1', title: 'Rank One'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity_rank_1', name: 'Entity One'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_pagerank_scores_batch(&KnowledgePageRankScoreBatchRequest {
            updates: vec![
                KnowledgePageRankScoreUpdate {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                    score: 0.42,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                    score: 0.84,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .knowledge_property_batch(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_rank_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_rank_1".to_string(),
                    },
                ],
                property_names: vec!["pagerank_score".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("pagerank_score"),
            Some(&Some(Value::Float(0.42)))
        );
        assert_eq!(
            rows.rows[1].properties.get("pagerank_score"),
            Some(&Some(Value::Float(0.84)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reads_pagerank_plan_counts_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'm1', created_at: 10, updated_at: 20, metadata: '{\"visible\":true}'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'm2', created_at: 120, updated_at: 130})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e1', name: 'Entity One', created_at: 15, updated_at: 25})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e2', name: 'Entity Two', created_at: 140, updated_at: 150})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm1'}), (e:Entity {id: 'e1'}) CREATE (m)-[:MENTIONS {created_at: 30}]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm2'}), (e:Entity {id: 'e2'}) CREATE (m)-[:MENTIONS {created_at: 160}]->(e)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'e1'}), (b:Entity {id: 'e2'}) CREATE (a)-[:RELATES_TO {created_at: 170}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'm1'}), (b:Memory {id: 'm2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 180}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'm2'}), (b:Memory {id: 'm1'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'inactive', created_at: 190}]->(b)")
        .unwrap();

    let graph_commit_epoch = db.store.commit_epoch();
    let plan = db
        .knowledge_pagerank_plan(&KnowledgePageRankPlanRequest {
            changed_since_epoch_nanos: Some(100),
        })
        .unwrap();

    assert_eq!(plan.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(plan.memory_node_count, 2);
    assert_eq!(plan.entity_node_count, 2);
    assert_eq!(plan.entity_relation_count, 1);
    assert_eq!(plan.mention_edge_count, 2);
    assert_eq!(plan.active_memory_relation_count, 1);
    assert_eq!(plan.changed_memory_count, 1);
    assert_eq!(plan.changed_entity_count, 1);
    assert_eq!(plan.changed_mention_edge_count, 1);
    assert_eq!(plan.changed_entity_relation_count, 1);
    assert_eq!(plan.changed_memory_relation_count, 1);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
}

#[test]
fn pagerank_plan_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(32),
        statement_summary_capacity: 32,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'pagerank-cache-memory-one', created_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'pagerank-cache-memory-two', updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'pagerank-cache-entity-one', created_at: 30})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'pagerank-cache-entity-two', updated_at: 40})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'pagerank-cache-memory-one'}), (e:Entity {id: 'pagerank-cache-entity-one'}) CREATE (m)-[:MENTIONS {created_at: 50}]->(e)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'pagerank-cache-entity-one'}), (b:Entity {id: 'pagerank-cache-entity-two'}) CREATE (a)-[:RELATES_TO {updated_at: 60}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'pagerank-cache-memory-one'}), (b:Memory {id: 'pagerank-cache-memory-two'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 70}]->(b)")
        .unwrap();
    let request = KnowledgePageRankPlanRequest {
        changed_since_epoch_nanos: Some(25),
    };

    let first = db.knowledge_pagerank_plan(&request).unwrap();
    let second = db.knowledge_pagerank_plan(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.memory_node_count, 2);
    assert_eq!(first.entity_node_count, 2);
    assert_eq!(first.mention_edge_count, 1);
    assert_eq!(first.active_memory_relation_count, 1);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 10);
    assert_eq!(stats.misses, 10);
    assert_eq!(stats.hits, 10);
}

#[test]
fn reads_pagerank_membership_visibility_and_central_entity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', metadata: '{\"space\":\"default\"}', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm2'})").unwrap();
    db.query("CREATE (:Entity {id: 'e1', name: 'Central Entity'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let membership = db
        .knowledge_pagerank_membership(&KnowledgePageRankMembershipRequest {
            label: "Entity".to_string(),
            external_ids: vec!["e1".to_string(), "missing".to_string()],
        })
        .unwrap();
    assert_eq!(membership.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(membership.matched_count, 1);
    assert_eq!(membership.missing_count, 1);
    assert!(membership.rows[0].matched);
    assert!(!membership.rows[1].matched);

    let visibility = db
        .knowledge_pagerank_memory_visibility(&KnowledgePageRankMemoryVisibilityRequest {
            memory_ids: vec!["m1".to_string(), "m2".to_string(), "missing".to_string()],
        })
        .unwrap();
    assert_eq!(visibility.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(visibility.matched_count, 2);
    assert_eq!(visibility.missing_count, 1);
    assert_eq!(
        visibility.rows[0].metadata,
        Some(Value::String("{\"space\":\"default\"}".to_string()))
    );
    assert!(!visibility.rows[0].is_latest);
    assert!(visibility.rows[1].is_latest);
    assert!(!visibility.rows[2].matched);

    let central = db
        .knowledge_pagerank_central_entity(&KnowledgePageRankCentralEntityRequest {
            entity_id: "e1".to_string(),
        })
        .unwrap();
    assert_eq!(central.graph_commit_epoch, graph_commit_epoch);
    assert!(central.found);
    assert_eq!(central.name.as_deref(), Some("Central Entity"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
}

#[test]
fn pagerank_lookup_reads_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'pagerank-cache-memory', metadata: '{\"space\":\"default\"}', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'pagerank-cache-entity', name: 'Cache Entity'})")
        .unwrap();

    let membership_request = KnowledgePageRankMembershipRequest {
        label: "Entity".to_string(),
        external_ids: vec!["pagerank-cache-entity".to_string(), "missing".to_string()],
    };
    let visibility_request = KnowledgePageRankMemoryVisibilityRequest {
        memory_ids: vec!["pagerank-cache-memory".to_string(), "missing".to_string()],
    };
    let central_request = KnowledgePageRankCentralEntityRequest {
        entity_id: "pagerank-cache-entity".to_string(),
    };

    let membership = db
        .knowledge_pagerank_membership(&membership_request)
        .unwrap();
    let visibility = db
        .knowledge_pagerank_memory_visibility(&visibility_request)
        .unwrap();
    let central = db
        .knowledge_pagerank_central_entity(&central_request)
        .unwrap();
    db.knowledge_pagerank_membership(&membership_request)
        .unwrap();
    db.knowledge_pagerank_memory_visibility(&visibility_request)
        .unwrap();
    db.knowledge_pagerank_central_entity(&central_request)
        .unwrap();

    assert_eq!(membership.matched_count, 1);
    assert_eq!(membership.missing_count, 1);
    assert_eq!(visibility.matched_count, 1);
    assert_eq!(visibility.missing_count, 1);
    assert!(!visibility.rows[0].is_latest);
    assert!(central.found);
    assert_eq!(central.name.as_deref(), Some("Cache Entity"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 3);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 3);
}

#[test]
fn pagerank_read_requests_validate_nowledge_inputs() {
    let db = Database::new();

    let bad_label = db
        .knowledge_pagerank_membership(&KnowledgePageRankMembershipRequest {
            label: "Source".to_string(),
            external_ids: vec!["s1".to_string()],
        })
        .unwrap_err();
    assert!(bad_label
        .to_string()
        .contains("support only Memory and Entity labels"));

    let bad_member = db
        .knowledge_pagerank_membership(&KnowledgePageRankMembershipRequest {
            label: "Memory".to_string(),
            external_ids: vec![String::new()],
        })
        .unwrap_err();
    assert!(bad_member.to_string().contains("non-empty external ids"));

    let bad_visibility = db
        .knowledge_pagerank_memory_visibility(&KnowledgePageRankMemoryVisibilityRequest {
            memory_ids: vec![String::new()],
        })
        .unwrap_err();
    assert!(bad_visibility.to_string().contains("non-empty memory ids"));

    let bad_central = db
        .knowledge_pagerank_central_entity(&KnowledgePageRankCentralEntityRequest {
            entity_id: String::new(),
        })
        .unwrap_err();
    assert!(bad_central.to_string().contains("non-empty entity id"));
}
