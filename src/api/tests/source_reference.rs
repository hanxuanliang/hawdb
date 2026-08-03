use super::*;

#[test]
fn reads_source_reference_entities_for_nowledge_delete_flow() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_a'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_b'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_c'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_d'})").unwrap();
    db.query("MATCH (a:Entity {id: 'entity_a'}), (b:Entity {id: 'entity_b'}) CREATE (a)-[:RELATES_TO {source_reference: 'source_1'}]->(b)")
        .unwrap();
    db.query("MATCH (c:Entity {id: 'entity_c'}), (a:Entity {id: 'entity_a'}) CREATE (c)-[:RELATES_TO {source_reference: 'source_1'}]->(a)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'entity_a'}), (d:Entity {id: 'entity_d'}) CREATE (a)-[:RELATES_TO {source_reference: 'other'}]->(d)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_source_reference_entities(&KnowledgeSourceReferenceEntityListRequest {
            source_reference: "source_1".to_string(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(output.matched_relationship_count, 2);
    assert_eq!(output.returned_count, 3);
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row.entity_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["entity_a", "entity_b", "entity_c"]
    );
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);

    let snapshot = db.begin_read_transaction();
    db.query("CREATE (:Entity {id: 'entity_e'})").unwrap();
    db.query("MATCH (e:Entity {id: 'entity_e'}), (a:Entity {id: 'entity_a'}) CREATE (e)-[:RELATES_TO {source_reference: 'source_1'}]->(a)")
        .unwrap();
    let snapshot_output = snapshot
        .knowledge_source_reference_entities(&KnowledgeSourceReferenceEntityListRequest {
            source_reference: "source_1".to_string(),
        })
        .unwrap();
    assert_eq!(snapshot_output.returned_count, 3);
}

#[test]
fn counts_source_reference_relationships_for_nowledge_delete_guard() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    db.query("CREATE (:Entity {id: 'out'})").unwrap();
    db.query("CREATE (:Entity {id: 'in-empty'})").unwrap();
    db.query("CREATE (:Entity {id: 'in-excluded'})").unwrap();
    db.query("CREATE (:Entity {id: 'null-source'})").unwrap();
    db.query("MATCH (e:Entity {id: 'entity'}), (out:Entity {id: 'out'}) CREATE (e)-[:RELATES_TO {source_reference: 'other-source'}]->(out)")
        .unwrap();
    db.query("MATCH (incoming:Entity {id: 'in-empty'}), (e:Entity {id: 'entity'}) CREATE (incoming)-[:RELATES_TO {source_reference: ''}]->(e)")
        .unwrap();
    db.query("MATCH (incoming:Entity {id: 'in-excluded'}), (e:Entity {id: 'entity'}) CREATE (incoming)-[:RELATES_TO {source_reference: 'excluded-source'}]->(e)")
        .unwrap();
    db.query("MATCH (e:Entity {id: 'entity'}), (target:Entity {id: 'null-source'}) CREATE (e)-[:RELATES_TO]->(target)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_source_reference_relationship_count(
            &KnowledgeSourceReferenceRelationshipCountRequest {
                entity_id: "entity".to_string(),
                excluded_source_reference: "excluded-source".to_string(),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert!(output.found_entity);
    assert_eq!(output.relationship_count, 4);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);

    let missing = db
        .knowledge_source_reference_relationship_count(
            &KnowledgeSourceReferenceRelationshipCountRequest {
                entity_id: "missing".to_string(),
                excluded_source_reference: "excluded-source".to_string(),
            },
        )
        .unwrap();
    assert!(!missing.found_entity);
    assert_eq!(missing.relationship_count, 0);
}

#[test]
fn source_reference_entities_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Entity {id: 'entity_a'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_b'})").unwrap();
    db.query("MATCH (a:Entity {id: 'entity_a'}), (b:Entity {id: 'entity_b'}) CREATE (a)-[:RELATES_TO {source_reference: 'source_1'}]->(b)")
        .unwrap();
    let request = KnowledgeSourceReferenceEntityListRequest {
        source_reference: "source_1".to_string(),
    };

    let first = db.knowledge_source_reference_entities(&request).unwrap();
    let second = db.knowledge_source_reference_entities(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_relationship_count, 1);
    assert_eq!(first.returned_count, 2);
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| row.entity_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["entity_a", "entity_b"]
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn source_reference_relationship_count_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    db.query("CREATE (:Entity {id: 'out'})").unwrap();
    db.query("CREATE (:Entity {id: 'in-empty'})").unwrap();
    db.query("MATCH (e:Entity {id: 'entity'}), (out:Entity {id: 'out'}) CREATE (e)-[:RELATES_TO {source_reference: 'other-source'}]->(out)")
        .unwrap();
    db.query("MATCH (incoming:Entity {id: 'in-empty'}), (e:Entity {id: 'entity'}) CREATE (incoming)-[:RELATES_TO {source_reference: ''}]->(e)")
        .unwrap();
    let request = KnowledgeSourceReferenceRelationshipCountRequest {
        entity_id: "entity".to_string(),
        excluded_source_reference: "excluded-source".to_string(),
    };

    let first = db
        .knowledge_source_reference_relationship_count(&request)
        .unwrap();
    let second = db
        .knowledge_source_reference_relationship_count(&request)
        .unwrap();

    assert_eq!(first, second);
    assert!(first.found_entity);
    assert_eq!(first.relationship_count, 3);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 3);
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 3);
}

#[test]
fn source_reference_delete_reads_reject_empty_inputs_without_wal() {
    let path = unique_test_dir("source_reference_delete_reads_empty_inputs");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})-[:RELATES_TO {source_reference: 'source_1'}]->(:Entity {id: 'entity_2'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let entities_error = db
        .knowledge_source_reference_entities(&KnowledgeSourceReferenceEntityListRequest {
            source_reference: String::new(),
        })
        .unwrap_err();
    assert!(entities_error
        .to_string()
        .contains("non-empty source_reference"));

    let count_error = db
        .knowledge_source_reference_relationship_count(
            &KnowledgeSourceReferenceRelationshipCountRequest {
                entity_id: "entity_1".to_string(),
                excluded_source_reference: " ".to_string(),
            },
        )
        .unwrap_err();
    assert!(count_error
        .to_string()
        .contains("non-empty source_reference"));

    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_source_reference_relationships_for_nowledge_cleanup() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1'})-[:RELATES_TO {source_reference: 'source_1'}]->(:Entity {id: 'entity_2'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_3'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_4'})").unwrap();
    db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        },
        target: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_3".to_string(),
        },
        relationship_type: "RELATES_TO".to_string(),
        properties: BTreeMap::from([(
            "source_reference".to_string(),
            Value::String("source_1".to_string()),
        )]),
    })
    .unwrap();
    db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        },
        target: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_4".to_string(),
        },
        relationship_type: "RELATES_TO".to_string(),
        properties: BTreeMap::from([(
            "source_reference".to_string(),
            Value::String("source_2".to_string()),
        )]),
    })
    .unwrap();

    let output = db
        .delete_knowledge_source_reference_relationships(
            &KnowledgeSourceReferenceRelationshipCleanupRequest {
                source_reference: "source_1".to_string(),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 5);
    assert_eq!(output.graph_commit_epoch_after, 6);
    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.deleted_relationship_count, 2);
    assert_eq!(output.rows.len(), 2);
    assert!(output.rows.iter().all(|row| row.deleted));
    assert!(output
        .rows
        .iter()
        .all(|row| row.source_external_id.as_deref() == Some("entity_1")));

    let dropped = db
        .query("MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.source_reference = 'source_1' RETURN count(r) AS total")
        .unwrap();
    assert_eq!(dropped.rows[0].get("total"), Some(&Value::Int(0)));
    let kept = db
        .query("MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.source_reference = 'source_2' RETURN count(r) AS total")
        .unwrap();
    assert_eq!(kept.rows[0].get("total"), Some(&Value::Int(1)));
    let nodes = db
        .query("MATCH (e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(4)));
}

#[test]
fn source_reference_relationship_cleanup_rejects_empty_reference_before_wal() {
    let path = unique_test_dir("source_reference_cleanup_empty_before_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})-[:RELATES_TO {source_reference: 'source_1'}]->(:Entity {id: 'entity_2'})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let epoch_before = db.store.commit_epoch();
        let error = db
            .delete_knowledge_source_reference_relationships(
                &KnowledgeSourceReferenceRelationshipCleanupRequest {
                    source_reference: " ".to_string(),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("non-empty source_reference"));
        assert_eq!(db.store.commit_epoch(), epoch_before);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_source_reference_relationship_cleanup_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_reference_cleanup_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})-[:RELATES_TO {source_reference: 'source_1'}]->(:Entity {id: 'entity_2'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity_3'})").unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_3".to_string(),
            },
            relationship_type: "RELATES_TO".to_string(),
            properties: BTreeMap::from([(
                "source_reference".to_string(),
                Value::String("source_1".to_string()),
            )]),
        })
        .unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .delete_knowledge_source_reference_relationships(
                &KnowledgeSourceReferenceRelationshipCleanupRequest {
                    source_reference: "source_1".to_string(),
                },
            )
            .unwrap();
        assert_eq!(output.candidate_count, 2);
        assert_eq!(output.deleted_relationship_count, 2);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let relationships = db
            .query("MATCH (:Entity)-[r:RELATES_TO]->(:Entity) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(relationships.rows[0].get("total"), Some(&Value::Int(0)));
        let nodes = db
            .query("MATCH (e:Entity) RETURN count(e) AS total")
            .unwrap();
        assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(3)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
