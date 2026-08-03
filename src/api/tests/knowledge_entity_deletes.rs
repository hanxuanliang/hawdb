use super::*;

#[test]
fn deletes_knowledge_entity_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'Skein'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.node_id, Some(0));
    assert!(output.matched);
    assert!(!output.filtered_out);
    assert_eq!(output.deleted_node_count, 1);
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .is_none());
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
    let relationships = db
        .knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            }],
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Incoming,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 0);
}

#[test]
fn scoped_knowledge_entity_delete_does_not_write_filtered_seed() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_entity(&KnowledgeScopedEntityDeleteRequest {
            delete: KnowledgeEntityDeleteRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
            },
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.matched);
    assert!(output.filtered_out);
    assert_eq!(output.deleted_node_count, 0);
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
}

#[test]
fn knowledge_entity_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "0".to_string(),
            },
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.matched);
    assert!(!output.filtered_out);
    assert_eq!(output.deleted_node_count, 0);
}

#[test]
fn knowledge_entity_delete_rejects_invalid_identifier() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();

    let error = db
        .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Bad-Label".to_string(),
                external_id: "memory_1".to_string(),
            },
        })
        .unwrap_err();

    assert!(error.to_string().contains("label identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_delete");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    }
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let error = db
            .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_entity_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
            .unwrap();
        db.delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .knowledge_entity(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            })
            .unwrap()
            .entity
            .is_none());
        assert!(db
            .knowledge_entity(&KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            })
            .unwrap()
            .entity
            .is_some());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reads_entity_delete_guard_counts_for_nowledge_rest_write() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    db.query("CREATE (:Memory {id: 'other_memory'})").unwrap();
    db.query("CREATE (:Memory {id: 'excluded_memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label'})").unwrap();
    db.query("CREATE (:Entity {id: 'out_entity'})").unwrap();
    db.query("CREATE (:Entity {id: 'in_entity'})").unwrap();
    db.query("MATCH (m:Memory {id: 'other_memory'}), (e:Entity {id: 'entity'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'excluded_memory'}), (e:Entity {id: 'entity'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity'}), (l:Label {id: 'label'}) CREATE (e)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query("MATCH (e:Entity {id: 'entity'}), (out:Entity {id: 'out_entity'}) CREATE (e)-[:RELATES_TO]->(out)")
        .unwrap();
    db.query("MATCH (incoming:Entity {id: 'in_entity'}), (e:Entity {id: 'entity'}) CREATE (incoming)-[:RELATES_TO]->(e)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_entity_delete_guard(&KnowledgeEntityDeleteGuardRequest {
            entity_id: "entity".to_string(),
            excluded_memory_id: "excluded_memory".to_string(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert!(output.found_entity);
    assert_eq!(output.other_memory_mention_count, 1);
    assert_eq!(output.label_relationship_count, 1);
    assert_eq!(output.distinct_relationship_count, 8);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);

    let cached_output = db
        .knowledge_entity_delete_guard(&KnowledgeEntityDeleteGuardRequest {
            entity_id: "entity".to_string(),
            excluded_memory_id: "excluded_memory".to_string(),
        })
        .unwrap();
    assert_eq!(cached_output, output);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.misses, 5);
    assert_eq!(stats.hits, 5);

    let snapshot = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'later_memory'})").unwrap();
    db.query("MATCH (m:Memory {id: 'later_memory'}), (e:Entity {id: 'entity'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    let snapshot_output = snapshot
        .knowledge_entity_delete_guard(&KnowledgeEntityDeleteGuardRequest {
            entity_id: "entity".to_string(),
            excluded_memory_id: "excluded_memory".to_string(),
        })
        .unwrap();
    assert_eq!(snapshot_output.other_memory_mention_count, 1);
}

#[test]
fn entity_delete_guard_reports_missing_entity_without_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_entity_delete_guard(&KnowledgeEntityDeleteGuardRequest {
            entity_id: "missing".to_string(),
            excluded_memory_id: "excluded_memory".to_string(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert!(!output.found_entity);
    assert_eq!(output.other_memory_mention_count, 0);
    assert_eq!(output.label_relationship_count, 0);
    assert_eq!(output.distinct_relationship_count, 0);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
}

#[test]
fn entity_delete_guard_rejects_empty_inputs_without_wal() {
    let path = unique_test_dir("entity_delete_guard_empty_inputs");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let entity_error = db
        .knowledge_entity_delete_guard(&KnowledgeEntityDeleteGuardRequest {
            entity_id: String::new(),
            excluded_memory_id: "memory".to_string(),
        })
        .unwrap_err();
    assert!(entity_error.to_string().contains("entity id"));

    let memory_error = db
        .knowledge_entity_delete_guard(&KnowledgeEntityDeleteGuardRequest {
            entity_id: "entity".to_string(),
            excluded_memory_id: String::new(),
        })
        .unwrap_err();
    assert!(memory_error.to_string().contains("excluded memory id"));

    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}
