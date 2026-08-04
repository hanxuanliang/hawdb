use super::*;
use crate::store::set_wal_apply_failpoint;
use crate::StorageResidencyMode;

#[test]
fn post_wal_apply_failure_poisons_handle_until_reopen() {
    let path = unique_test_dir("post_wal_apply_poison");
    let mut db = Database::open(&path).unwrap();
    let mut stable_read = db.begin_read_transaction();
    let mut transaction = db.begin_transaction();
    transaction.query("CREATE (:Memory {id: 'first'})").unwrap();
    transaction
        .query("CREATE (:Memory {id: 'second'})")
        .unwrap();

    set_wal_apply_failpoint(Some(1));
    let commit_error = transaction.commit().unwrap_err();
    set_wal_apply_failpoint(None);

    assert!(commit_error
        .to_string()
        .contains("injected failure while applying a durable WAL batch"));
    assert!(db.storage_handle_poisoned());
    let stable_output = stable_read
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert!(stable_output.rows.is_empty());
    let read_error = db
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap_err();
    assert!(read_error.to_string().contains("close and reopen"));
    let checkpoint_error = db.checkpoint().unwrap_err();
    assert!(checkpoint_error.to_string().contains("close and reopen"));

    drop(db);
    let mut reopened = Database::open(&path).unwrap();
    assert!(!reopened.storage_handle_poisoned());
    let output = reopened
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("first".to_string()))
    );
    assert_eq!(
        output.rows[1].get("id"),
        Some(&Value::String("second".to_string()))
    );

    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn forced_out_of_core_checkpoint_reopen_and_mutation_are_equivalent() {
    let path = unique_test_dir("forced_out_of_core");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query(
            "CREATE (:Memory {id: 1, title: 'One'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
        db.query("CALL project_graph('MemoryGraph', ['Memory', 'Entity'], ['MENTIONS'])")
            .unwrap();
        db.checkpoint().unwrap();

        let residency = db.storage_residency_report();
        assert!(residency.out_of_core);
        assert_eq!(residency.delta_node_count, 0);
        assert_eq!(residency.delta_relationship_count, 0);
        assert!(!residency.checkpoint_statistics_complete);
        assert!(residency.checkpoint_statistics_stale);

        let rows = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.id AS memory_id, e.name AS entity_name",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("memory_id"), Some(&Value::Int(1)));
        let orphans = db
            .query("MATCH (m:Memory) WHERE NOT (m)-[:MENTIONS]->(:Entity) RETURN m.id AS memory_id")
            .unwrap();
        assert_eq!(orphans.rows.len(), 1);
        assert_eq!(orphans.rows[0].get("memory_id"), Some(&Value::Int(2)));
        let existing_pair = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE NOT EXISTS { MATCH (m)-[:MENTIONS]->(e) } RETURN m.id AS memory_id",
            )
            .unwrap();
        assert!(existing_pair.rows.is_empty());
        let page_rank = db
            .query("CALL page_rank('MemoryGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(page_rank.rows.len(), 3);
        let snapshot = db.try_export_canonical_graph_snapshot().unwrap();
        assert_eq!(snapshot.nodes.len(), 3);
        assert_eq!(snapshot.relationships.len(), 1);
        let statistics = db.statistics();
        assert!(!statistics.advanced_statistics_complete);
        assert_eq!(statistics.node_count, 3);
        assert_eq!(statistics.relationship_count, 1);
        assert!(statistics.property_distinct_counts.is_empty());
        assert!(statistics.rel_property_distinct_counts.is_empty());
        assert!(statistics.path_counts.is_empty());

        let updated = db
            .query(
                "MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'Updated' RETURN m.id AS memory_id",
            )
            .unwrap();
        assert_eq!(updated.rows.len(), 1);
        assert_eq!(updated.rows[0].get("memory_id"), Some(&Value::Int(1)));
        db.query(
            "MATCH (m:Memory {id: 2}), (e:Entity {id: 10}) CREATE (m)-[:MENTIONS {weight: 2}]->(e)",
        )
        .unwrap();
        db.checkpoint().unwrap();
    }

    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        let residency = db.storage_residency_report();
        assert!(residency.out_of_core);
        assert_eq!(residency.delta_node_count, 0);
        assert_eq!(residency.delta_relationship_count, 0);

        let rows = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.id AS memory_id, m.title AS title ORDER BY memory_id ASC",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("title"),
            Some(&Value::String("Updated".to_string()))
        );
        assert_eq!(rows.rows[1].get("memory_id"), Some(&Value::Int(2)));

        let statistics = db.statistics();
        assert_eq!(statistics.node_count, 3);
        assert_eq!(statistics.relationship_count, 2);
        assert!(!statistics.advanced_statistics_complete);
        assert!(statistics.property_distinct_counts.is_empty());
        assert!(statistics.rel_property_distinct_counts.is_empty());
        assert!(statistics.path_counts.is_empty());
        assert!(
            statistics.computed_at_commit_epoch < db.basic_statistics().computed_at_commit_epoch
        );

        let residency = db.storage_residency_report();
        assert!(!residency.checkpoint_statistics_complete);
        assert!(residency.checkpoint_statistics_stale);
        assert!(residency.segment_cache_miss_count > 0);
        assert_eq!(residency.segment_cache_digest_mismatch_count, 0);
        let cache = db.segment_cache_snapshot().unwrap();
        assert!(cache.resident_bytes <= cache.capacity_bytes);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_reads_and_mutations_include_checkpointed_canonical_rows() {
    let path = unique_test_dir("out_of_core_typed_canonical_rows");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory:one', title: 'One'})-[:LINKS]->(:Entity {id: 'entity:rust', name: 'Rust'})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        let residency = db.storage_residency_report();
        assert_eq!(residency.delta_node_count, 0);
        assert_eq!(residency.delta_relationship_count, 0);

        let entity = db
            .knowledge_entity(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
            })
            .unwrap();
        assert_eq!(
            entity.entity.unwrap().properties.get("title"),
            Some(&Value::String("One".to_string()))
        );

        let properties = db
            .knowledge_property_batch(&KnowledgePropertyBatchRequest {
                entities: vec![KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity:rust".to_string(),
                }],
                property_names: vec!["name".to_string()],
            })
            .unwrap();
        assert_eq!(
            properties.rows[0].properties.get("name"),
            Some(&Some(Value::String("Rust".to_string())))
        );

        let neighbors = db
            .knowledge_neighbors(&KnowledgeNeighborsRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
                relationship_type: Some("LINKS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit: 4,
                max_hops: 1,
            })
            .unwrap();
        assert_eq!(neighbors.paths.len(), 1);
        assert_eq!(
            neighbors.paths[0].target_external_id.as_deref(),
            Some("entity:rust")
        );

        let created = db
            .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory:one".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity:rust".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                properties: BTreeMap::from([("weight".to_string(), Value::Int(2))]),
            })
            .unwrap();
        assert!(created.matched);
        assert_eq!(created.created_relationship_count, 1);

        let relationships = db
            .knowledge_relationships(&KnowledgeRelationshipsRequest {
                seeds: vec![KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory:one".to_string(),
                }],
                relationship_type: None,
                direction: KnowledgeNeighborDirection::Outgoing,
                limit_per_seed: 4,
            })
            .unwrap();
        assert_eq!(relationships.relationship_count, 2);
        db.checkpoint().unwrap();
    }

    {
        let db = Database::open_with_config(&path, config).unwrap();
        let neighbors = db
            .knowledge_neighbors(&KnowledgeNeighborsRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
                relationship_type: Some("MENTIONS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit: 4,
                max_hops: 1,
            })
            .unwrap();
        assert_eq!(neighbors.paths.len(), 1);
        assert_eq!(
            neighbors.paths[0].relationship_properties.get("weight"),
            Some(&Value::Int(2))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_read_fails_closed_when_an_out_of_core_segment_is_corrupted() {
    let path = unique_test_dir("out_of_core_typed_read_corruption");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            segment_cache_capacity_bytes: 1024 * 1024,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory:one', title: 'Corrupt me'})")
        .unwrap();
    db.checkpoint().unwrap();

    let canonical_path = path.join("canonical.1.skein");
    let mut bytes = std::fs::read(&canonical_path).unwrap();
    bytes[24] ^= 0xff;
    std::fs::write(&canonical_path, bytes).unwrap();

    let error = db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory:one".to_string(),
        })
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed content digest verification"),
        "unexpected typed read error: {error}"
    );
    assert_eq!(
        db.storage_residency_report()
            .segment_cache_digest_mismatch_count,
        1
    );

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn public_query_fails_closed_when_an_out_of_core_segment_is_corrupted() {
    let path = unique_test_dir("out_of_core_public_query_corruption");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            segment_cache_capacity_bytes: 1024 * 1024,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Corrupt me'})")
        .unwrap();
    db.checkpoint().unwrap();

    let canonical_path = path.join("canonical.1.skein");
    let mut bytes = std::fs::read(&canonical_path).unwrap();
    bytes[24] ^= 0xff;
    std::fs::write(&canonical_path, bytes).unwrap();

    let error = db
        .query("MATCH (m:Memory) RETURN m.id AS memory_id")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed content digest verification"),
        "unexpected query error: {error}"
    );
    assert_eq!(
        db.storage_residency_report()
            .segment_cache_digest_mismatch_count,
        1
    );

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[ignore = "resource profile; run explicitly for larger-than-cache evidence"]
fn larger_than_cache_query_reports_process_and_storage_resource_evidence() {
    let path = unique_test_dir("out_of_core_resource_profile");
    let node_count = 2_048usize;
    let cache_budget = 1024 * 1024;
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: cache_budget,
        max_read_result_rows: Some(node_count),
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        let mut tx = db.begin_transaction();
        for id in 0..node_count {
            tx.query(&format!(
                "CREATE (:Memory {{id: {id}, body: '{}'}})",
                "x".repeat(4096)
            ))
            .unwrap();
        }
        tx.commit().unwrap();
        db.checkpoint().unwrap();
        assert!(db.storage_residency_report().out_of_core);
    }

    let mut db = Database::open_with_config(&path, config).unwrap();
    let before = db.storage_residency_report();
    assert!(before.canonical_artifact_bytes > cache_budget.saturating_mul(4));
    let analyzed = db
        .explain_analyze_query("MATCH (m:Memory) RETURN m.id AS memory_id")
        .unwrap();
    assert_eq!(analyzed.output.rows.len(), node_count);
    let pipeline = &analyzed.execution_profile.pipeline_memory_report;
    assert!(pipeline.intermediate_rows >= node_count);
    assert!(pipeline.output_payload_bytes > 0);
    assert!(pipeline.start_resident_bytes.is_some());
    assert!(pipeline.steady_resident_bytes.is_some());
    assert!(pipeline.peak_resident_bytes.is_some());
    assert!(pipeline.minor_page_faults.is_some());
    assert!(pipeline.major_page_faults.is_some());

    let after = db.storage_residency_report();
    assert!(after.segment_cache_resident_bytes <= after.segment_cache_capacity_bytes);
    assert!(after.segment_cache_miss_count > 0);
    assert!(
        after.segment_cache_eviction_count > 0 || after.segment_cache_admission_rejection_count > 0
    );

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_storage_resource_profile_gates_larger_than_cache_reads() {
    let path = unique_test_dir("typed_storage_resource_profile");
    let cache_budget = 1024;
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: cache_budget,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    let mut transaction = db.begin_transaction();
    for id in 0..32 {
        transaction
            .query_with_params(
                "CREATE (:Memory {id: $id, body: $body})",
                &BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("body".to_string(), Value::String("x".repeat(1024))),
                ]),
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    db.checkpoint().unwrap();

    let report = db
        .storage_resource_profile(
            "MATCH (m:Memory) RETURN m.id AS memory_id",
            &BTreeMap::new(),
            crate::StorageResourceProfileLimits {
                min_canonical_artifact_bytes: 4096,
                max_steady_resident_bytes: u64::MAX,
                max_peak_resident_bytes: u64::MAX,
                max_minor_page_faults: None,
                max_major_page_faults: None,
                max_intermediate_rows: 1024,
                max_intermediate_payload_bytes: 1024 * 1024,
                max_output_rows: 64,
                max_output_payload_bytes: 1024 * 1024,
                require_fully_streamed: true,
            },
        )
        .unwrap();

    assert!(
        report.ready,
        "unexpected blockers: {:?}",
        report.blocker_codes
    );
    assert!(report.after.canonical_artifact_bytes > cache_budget);
    assert!(report.after.segment_cache_resident_bytes <= cache_budget);
    assert!(report.after.segment_cache_miss_count > report.before.segment_cache_miss_count);
    assert_eq!(report.query.output_rows, 32);
    assert_eq!(
        report.json()["protocol"],
        crate::STORAGE_RESOURCE_PROFILE_PROTOCOL
    );
    assert_eq!(report.json()["ready"], true);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn external_optimizer_statistics_refresh_spills_and_persists_exact_stats() {
    let path = unique_test_dir("external_optimizer_statistics_refresh");
    let spill_root = path.join("statistics-spill");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    let refreshed_statistics = {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE (:Memory {id: 'memory:one', kind: 'note'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity:rust', kind: 'language'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity:skein', kind: 'library'})")
            .unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory:one".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:rust".to_string(),
            },
            relationship_type: "LINKS".to_string(),
            properties: BTreeMap::from([("weight".to_string(), Value::Int(1))]),
        })
        .unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:rust".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:skein".to_string(),
            },
            relationship_type: "LINKS".to_string(),
            properties: BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        })
        .unwrap();
        let mut transaction = db.begin_transaction();
        for id in 0..32 {
            transaction
                .query_with_params(
                    "CREATE (:Memory {id: $id, body: $body})",
                    &BTreeMap::from([
                        ("id".to_string(), Value::String(format!("filler:{id}"))),
                        ("body".to_string(), Value::String("x".repeat(1024))),
                    ]),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        db.checkpoint().unwrap();
        assert!(!db.statistics().advanced_statistics_complete);

        db.query("CREATE (:Memory {id: 'memory:two', kind: 'task'})")
            .unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory:two".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity:skein".to_string(),
            },
            relationship_type: "LINKS".to_string(),
            properties: BTreeMap::from([("weight".to_string(), Value::Int(3))]),
        })
        .unwrap();
        let source_epoch = db.basic_statistics().computed_at_commit_epoch;
        let report = db
            .refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
                memory_budget_bytes: 8 * 1024,
                max_spill_bytes: 4 * 1024 * 1024,
                max_spill_runs: 128,
                max_input_records: 100_000,
                max_generated_facts: 100_000,
                max_path_expansions: 10_000,
                spill_directory: spill_root.clone(),
            })
            .unwrap();

        assert!(report.checkpoint_persisted);
        assert_eq!(report.source_commit_epoch, source_epoch);
        assert!(report.spill_run_count > 1);
        assert!(report.spilled_bytes > 0);
        assert!(report.peak_buffer_bytes <= 8 * 1024);
        let statistics = db.statistics();
        assert!(statistics.advanced_statistics_complete);
        assert_eq!(statistics.computed_at_commit_epoch, source_epoch);
        assert_eq!(
            statistics
                .property_distinct_counts
                .get(&(crate::schema::LabelId(0), "body".to_string())),
            Some(&1)
        );
        assert_eq!(
            statistics
                .rel_property_distinct_counts
                .values()
                .copied()
                .max(),
            Some(3)
        );
        assert_eq!(
            statistics.rel_type_source_counts.values().copied().max(),
            Some(3)
        );
        assert_eq!(
            statistics.rel_type_target_counts.values().copied().max(),
            Some(2)
        );
        assert!(statistics
            .bounded_path_counts
            .iter()
            .any(|((_, _, _, hop), count)| *hop == 2 && *count == 1));
        assert_eq!(
            std::fs::read_dir(&spill_root).unwrap().count(),
            0,
            "spill runs must be reclaimed after publication"
        );
        statistics
    };

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::Materialized,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_eq!(db.statistics(), refreshed_statistics);
    }

    {
        let db = Database::open_with_config(&path, config).unwrap();
        let statistics = db.statistics();
        assert!(statistics.advanced_statistics_complete);
        assert_eq!(
            statistics
                .rel_property_distinct_counts
                .values()
                .copied()
                .max(),
            Some(3)
        );
        let residency = db.storage_residency_report();
        assert!(residency.checkpoint_statistics_complete);
        assert!(!residency.checkpoint_statistics_stale);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn external_optimizer_statistics_refresh_fails_before_publication_on_work_budget() {
    let path = unique_test_dir("external_optimizer_statistics_budget");
    let spill_root = path.join("statistics-spill");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory:one', kind: 'note'})")
        .unwrap();
    db.checkpoint().unwrap();
    let generation = db.storage_residency_report().canonical_generation;

    let error = db
        .refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
            memory_budget_bytes: 4096,
            max_spill_bytes: 1024 * 1024,
            max_spill_runs: 8,
            max_input_records: 100,
            max_generated_facts: 1,
            max_path_expansions: 100,
            spill_directory: spill_root.clone(),
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("max_generated_facts 1"),
        "unexpected refresh error: {error}"
    );
    assert!(!db.statistics().advanced_statistics_complete);
    assert_eq!(
        db.storage_residency_report().canonical_generation,
        generation
    );
    assert_eq!(std::fs::read_dir(&spill_root).unwrap().count(), 0);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_delta_budget_rejects_before_wal_append() {
    let path = unique_test_dir("out_of_core_delta_budget");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        max_out_of_core_delta_bytes: Some(1),
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Original'})")
            .unwrap();
        db.checkpoint().unwrap();
        assert_eq!(read_test_wal(&path).unwrap(), "");

        let error = db
            .query("MATCH (m:Memory {id: 1}) SET m.title = 'Rejected'")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("out-of-core mutation delta admission rejected"));
        assert_eq!(read_test_wal(&path).unwrap(), "");
        let residency = db.storage_residency_report();
        assert_eq!(residency.estimated_delta_resident_bytes, 0);
        assert!(residency.delta_within_budget);
    }

    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        let output = db
            .query("MATCH (m:Memory {id: 1}) RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Original".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_delta_budget_also_bounds_wal_replay() {
    let path = unique_test_dir("out_of_core_replay_delta_budget");
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                max_out_of_core_delta_bytes: None,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Original'})")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("MATCH (m:Memory {id: 1}) SET m.title = 'Pending WAL delta'")
            .unwrap();
    }

    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(1),
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("out-of-core mutation delta admission rejected"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persists_nodes_across_reopen_with_wal_replay() {
    let path = unique_test_dir("wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn strict_recovery_rejects_torn_wal_tail() {
    let path = unique_test_dir("strict_torn_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(active_wal_path(&path))
        .unwrap()
        .write_all(b"torn-entry-without-checksum")
        .unwrap();

    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            recovery_mode: RecoveryMode::Strict,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("strict WAL recovery rejected torn tail"));

    let mut tolerant = Database::open(&path).unwrap();
    let output = tolerant
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    let recovery = tolerant.storage_recovery_report();
    assert!(recovery.torn_tail_ignored);
    assert!(recovery.torn_tail_repaired);
    assert!(recovery.discarded_wal_tail_bytes > 0);
    assert!(!read_test_wal(&path)
        .unwrap()
        .contains("torn-entry-without-checksum"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_replay_entry_limit_rejects_long_recovery() {
    let path = unique_test_dir("wal_replay_limit");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    }

    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_wal_replay_entries: Some(1),
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("WAL replay entry limit exceeded"));

    let mut db = Database::open(&path).unwrap();
    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn storage_recovery_report_tracks_wal_replay_boundary() {
    let path = unique_test_dir("storage_recovery_report");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Checkpointed'})")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Replayed'})")
            .unwrap();
    }

    let db = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_wal_replay_entries: Some(8),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let report = db.storage_recovery_report();
    assert!(report.durable);
    assert_eq!(report.recovery_mode, RecoveryMode::TolerateTornTail);
    assert_eq!(report.max_wal_replay_entries, Some(8));
    assert_eq!(report.checkpoint_epoch, Some(1));
    assert_eq!(report.checkpoint_commit_epoch, Some(1));
    assert!(report.wal_present);
    assert_eq!(report.wal_replay_start_lsn, Some(2));
    assert_eq!(report.next_lsn_after_replay, Some(3));
    assert_eq!(report.replayed_wal_entries, 1);
    assert!(!report.torn_tail_ignored);
    assert_eq!(report.torn_tail_reason, None);
    assert_eq!(report.recovered_commit_epoch, 2);

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mem_shaped_graph_mutations_recover_across_checkpoint_and_wal() {
    let path = unique_test_dir("mem_shaped_recovery");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Source {id: 'source:one', space_id: 'default', kind: 'thread', metadata: '{}', memory_count: 1})",
        )
        .unwrap();
        db.query(
            "CREATE (:Thread {id: 'thread:one', thread_id: 'thread:one', source_id: 'source:one', space_id: 'default', message_count: 2})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'mem:checkpointed', title: 'Checkpointed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default', importance: 0.4, confidence: 0.8, is_latest: true})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        {
            let mut tx = db.begin_transaction();
            tx.query(
                "CREATE (:Memory {id: 'mem:replayed', title: 'Replayed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default', importance: 0.9, confidence: 0.7, lifecycle_state: 'active', is_latest: true})-[:MENTIONS {thread_id: 'thread:one', message_index: 1, confidence: 0.7}]->(:Entity {id: 'entity:rust', name: 'Rust', space_id: 'default', unit_type: 'entity'})",
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert!(wal.contains("\tbatch\t"));
    assert!(wal.contains("create_node"));
    assert!(wal.contains("create_rel"));

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                max_wal_replay_entries: Some(8),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);

        let recovery = db.storage_recovery_report();
        assert_eq!(recovery.checkpoint_epoch, Some(1));
        assert_eq!(recovery.checkpoint_commit_epoch, Some(3));
        assert_eq!(recovery.replayed_wal_entries, 1);
        assert_eq!(recovery.max_wal_replay_entries, Some(8));
        assert_eq!(recovery.recovered_commit_epoch, 4);

        let mem_recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&recovery);
        assert!(mem_recovery.ready);
        assert!(mem_recovery.durable_recovery_observed);
        assert!(mem_recovery.checkpoint_boundary_present);
        assert!(mem_recovery.wal_replay_bounded);
        assert!(mem_recovery.torn_tail_clean);
        assert!(mem_recovery.blocker_codes.is_empty());
        assert_eq!(mem_recovery.json()["readiness"]["wal_replay_bounded"], true);
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mem_shaped_post_checkpoint_batch_replays_before_torn_tail() {
    let path = unique_test_dir("mem_shaped_recovery_torn_tail");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Source {id: 'source:one', space_id: 'default', kind: 'thread', metadata: '{}', memory_count: 1})",
        )
        .unwrap();
        db.query(
            "CREATE (:Thread {id: 'thread:one', thread_id: 'thread:one', source_id: 'source:one', space_id: 'default', message_count: 2})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'mem:checkpointed', title: 'Checkpointed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default'})",
        )
        .unwrap();
        db.checkpoint().unwrap();

        let mut tx = db.begin_transaction();
        tx.query(
            "CREATE (:Memory {id: 'mem:replayed', title: 'Replayed memory', source_id: 'source:one', thread_id: 'thread:one', space_id: 'default'})-[:MENTIONS {thread_id: 'thread:one', message_index: 1, confidence: 0.7}]->(:Entity {id: 'entity:rust', name: 'Rust', space_id: 'default', unit_type: 'entity'})",
        )
        .unwrap();
        tx.commit().unwrap();

        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    std::fs::OpenOptions::new()
        .append(true)
        .open(active_wal_path(&path))
        .unwrap()
        .write_all(b"torn-entry-without-checksum")
        .unwrap();

    {
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                max_wal_replay_entries: Some(8),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);

        let recovery = db.storage_recovery_report();
        assert_eq!(recovery.checkpoint_epoch, Some(1));
        assert_eq!(recovery.checkpoint_commit_epoch, Some(3));
        assert_eq!(recovery.replayed_wal_entries, 1);
        assert_eq!(recovery.max_wal_replay_entries, Some(8));
        assert_eq!(recovery.recovered_commit_epoch, 4);
        assert!(recovery.torn_tail_ignored);
        assert!(recovery.torn_tail_repaired);
        assert!(recovery.discarded_wal_tail_bytes > 0);
        assert!(recovery.torn_tail_reason.is_some());

        let mem_recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&recovery);
        assert!(mem_recovery.ready);
        assert!(mem_recovery.durable_recovery_observed);
        assert!(mem_recovery.checkpoint_boundary_present);
        assert!(mem_recovery.wal_replay_bounded);
        assert!(mem_recovery.torn_tail_clean);
        assert!(mem_recovery.blocker_codes.is_empty());
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn in_memory_storage_recovery_report_is_non_durable() {
    let db = Database::new();
    let report = db.storage_recovery_report();
    assert!(!report.durable);
    assert_eq!(report.recovered_commit_epoch, 0);
    assert_eq!(report.replayed_wal_entries, 0);
    assert_eq!(report.max_wal_replay_entries, None);
    assert_eq!(report.checkpoint_epoch, None);
    assert_eq!(report.next_lsn_after_replay, None);
}

#[test]
fn read_only_open_does_not_create_missing_database_path() {
    let path = unique_test_dir("read_only_missing");
    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("read-only database path does not exist"));
    assert!(!path.exists());
}

#[test]
fn durable_database_open_is_exclusive_until_owner_drops() {
    let path = unique_test_dir("exclusive_database_owner");
    let owner = Database::open(&path).unwrap();

    let write_error = Database::open(&path).unwrap_err();
    assert_eq!(
        write_error.to_string(),
        "storage error: database directory is already open by this or another application"
    );
    let read_error = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap_err();
    assert_eq!(read_error.to_string(), write_error.to_string());
    assert!(!write_error
        .to_string()
        .contains(&path.display().to_string()));

    drop(owner);
    let reopened = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn durable_database_rejects_path_alias_until_owner_drops() {
    let path = unique_test_dir("exclusive_database_alias");
    let alias = path.join(".");
    let owner = Database::open(&path).unwrap();

    let error = Database::open(&alias).unwrap_err();
    assert_eq!(
        error.to_string(),
        "storage error: database directory is already open by this or another application"
    );

    drop(owner);
    let reopened = Database::open(&alias).unwrap();
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_open_loads_existing_database_without_allowing_writes() {
    let path = unique_test_dir("read_only_existing");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
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
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );

        let error = db.query("CREATE (:Memory {id: 2})").unwrap_err();
        assert!(error.to_string().contains("read-only mode"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoints_nodes_and_truncates_wal() {
    let path = unique_test_dir("checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(read_test_wal(&path).unwrap(), "");
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_query_invokes_storage_checkpoint() {
    let path = unique_test_dir("checkpoint_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        let output = db.query("CHECKPOINT;").unwrap();
        assert!(output.rows.is_empty());
    }

    let checkpoint = read_test_durable_text(&active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint.contains("canonical_records\ttrue\n"));
    assert!(path.join("canonical.1.skein").exists());
    assert_eq!(read_test_wal(&path).unwrap(), "");

    std::fs::remove_dir_all(path).unwrap();
}
