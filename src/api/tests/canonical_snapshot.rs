use super::*;

#[test]
fn canonical_snapshot_export_uses_pinned_read_transaction_state() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'later', title: 'Later'})")
        .unwrap();

    let snapshot = read_tx.export_canonical_graph_snapshot();
    let validation = snapshot.validate();
    assert_eq!(snapshot.graph_commit_epoch, 1);
    assert_eq!(snapshot.nodes.len(), 2);
    assert_eq!(snapshot.relationships.len(), 1);
    assert!(validation.is_valid);
    assert!(!validation.is_import_ready);
    assert!(!validation.stable_identity_ready);
    assert!(snapshot.stable_identity.requires_stable_id_mapping);
    assert!(snapshot.stable_identity.nodes_without_stable_id.is_empty());
    assert_eq!(
        snapshot.stable_identity.relationships_without_stable_id,
        vec![0]
    );
    assert!(snapshot
        .stable_identity
        .duplicate_node_stable_ids
        .is_empty());
    assert_eq!(
        snapshot.nodes[0].stable_id,
        Some(Value::String("root".to_string()))
    );
    assert_eq!(snapshot.relationships[0].rel_type, "LINKS");
    assert_eq!(snapshot.relationships[0].source_node_id, 0);
    assert_eq!(snapshot.relationships[0].target_node_id, 1);
    assert_eq!(
        snapshot.relationships[0].properties.get("weight"),
        Some(&Value::Int(7))
    );
    assert!(snapshot.nodes.iter().any(|node| {
        node.labels == vec!["Memory".to_string()]
            && node.properties.get("id") == Some(&Value::String("root".to_string()))
    }));
    assert!(!snapshot
        .nodes
        .iter()
        .any(|node| { node.properties.get("id") == Some(&Value::String("later".to_string())) }));

    let latest = db.export_canonical_graph_snapshot();
    assert_eq!(latest.graph_commit_epoch, 2);
    assert_eq!(latest.nodes.len(), 3);
    assert_ne!(snapshot.logical_checksum, latest.logical_checksum);
}

#[test]
fn canonical_snapshot_export_reports_duplicate_stable_ids() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'dup', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'dup', title: 'Second'})")
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();

    assert!(snapshot.stable_identity.requires_stable_id_mapping);
    assert_eq!(
        snapshot.stable_identity.duplicate_node_stable_ids,
        vec![Value::String("dup".to_string())]
    );
    assert!(snapshot.stable_identity.nodes_without_stable_id.is_empty());
}

#[test]
fn canonical_snapshot_export_validation_accepts_consistent_snapshot() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();
    let validation = snapshot.validate();

    assert!(validation.is_valid);
    assert!(validation.is_import_ready);
    assert!(validation.checksum_matches);
    assert!(validation.stable_identity_matches);
    assert!(validation.stable_identity_ready);
    assert_eq!(
        validation.expected_logical_checksum,
        snapshot.logical_checksum
    );
    assert!(validation.duplicate_node_ids.is_empty());
    assert!(validation.duplicate_relationship_ids.is_empty());
    assert!(validation.missing_sources.is_empty());
    assert!(validation.missing_targets.is_empty());
    assert!(
        !validation
            .expected_stable_identity
            .requires_stable_id_mapping
    );
}

#[test]
fn canonical_snapshot_export_validation_reports_corrupt_snapshot() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let mut snapshot = db.export_canonical_graph_snapshot();
    snapshot.nodes[1].node_id = snapshot.nodes[0].node_id;
    snapshot.relationships[0].target_node_id = 99;
    snapshot.relationships[0].stable_id = None;

    let validation = snapshot.validate();

    assert!(!validation.is_valid);
    assert!(!validation.is_import_ready);
    assert!(!validation.checksum_matches);
    assert!(!validation.stable_identity_matches);
    assert!(!validation.stable_identity_ready);
    assert_eq!(validation.duplicate_node_ids, vec![0]);
    assert!(validation.duplicate_relationship_ids.is_empty());
    assert!(validation.missing_sources.is_empty());
    assert_eq!(validation.missing_targets.len(), 1);
    assert_eq!(validation.missing_targets[0].relationship_id, 0);
    assert_eq!(validation.missing_targets[0].missing_node_id, 99);
    assert_eq!(
        validation
            .expected_stable_identity
            .relationships_without_stable_id,
        vec![0]
    );
}

#[test]
fn canonical_snapshot_stable_id_mapping_makes_export_import_ready() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();
    assert!(snapshot.validate().is_valid);
    assert!(!snapshot.validate().is_import_ready);
    assert_eq!(
        snapshot.stable_identity.relationships_without_stable_id,
        vec![0]
    );

    let mapped = snapshot.with_stable_id_mapping(&CanonicalStableIdMapping {
        relationship_stable_ids: BTreeMap::from([(0, Value::String("rel-root-mid".to_string()))]),
        ..CanonicalStableIdMapping::default()
    });
    let validation = mapped.validate();

    assert!(validation.is_valid);
    assert!(validation.is_import_ready);
    assert!(validation.stable_identity_ready);
    assert!(mapped
        .stable_identity
        .relationships_without_stable_id
        .is_empty());
    assert_eq!(
        mapped.relationships[0].stable_id,
        Some(Value::String("rel-root-mid".to_string()))
    );
    assert_ne!(mapped.logical_checksum, snapshot.logical_checksum);
}

#[test]
fn canonical_snapshot_stable_id_mapping_rejects_duplicate_overlay() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    db.query("MATCH (m:Memory {id: 'root'}), (e:Entity {id: 'mid'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();
    let mapped = snapshot.with_stable_id_mapping(&CanonicalStableIdMapping {
        relationship_stable_ids: BTreeMap::from([
            (0, Value::String("duplicate-rel".to_string())),
            (1, Value::String("duplicate-rel".to_string())),
        ]),
        ..CanonicalStableIdMapping::default()
    });
    let validation = mapped.validate();

    assert!(validation.is_valid);
    assert!(!validation.is_import_ready);
    assert!(!validation.stable_identity_ready);
    assert_eq!(
        validation
            .expected_stable_identity
            .duplicate_relationship_stable_ids,
        vec![Value::String("duplicate-rel".to_string())]
    );
}

#[test]
fn persisted_stable_id_mapping_survives_reopen_without_wal_write() {
    let path = unique_test_dir("persisted_stable_id_mapping");
    let first_stable_id = {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        let snapshot = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        let validation = snapshot.validate();
        let wal_after = std::fs::read_to_string(path.join("wal.skein")).unwrap();

        assert!(path.join("stable_ids.skein").exists());
        assert_eq!(wal_after, wal_before);
        assert!(validation.is_import_ready);
        assert!(validation.stable_identity_ready);
        snapshot.relationships[0].stable_id.clone().unwrap()
    };

    {
        let mut db = Database::open(&path).unwrap();
        let snapshot = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();

        assert_eq!(snapshot.relationships[0].stable_id, Some(first_stable_id));
        assert!(snapshot.validate().is_import_ready);
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persisted_stable_id_mapping_respects_read_only_open() {
    let path = unique_test_dir("persisted_stable_id_mapping_read_only");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
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
        let error = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap_err();

        assert!(error.to_string().contains("read-only mode"));
        assert!(!path.join("stable_ids.skein").exists());
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_lightning_bootstrap_manifest_reports_ready_physical_export() {
    let path = unique_test_dir("graph_lightning_bootstrap_manifest");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        let manifest = &export.manifest;

        assert_eq!(
            manifest.protocol_version,
            GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION
        );
        assert_eq!(manifest.graph_commit_epoch, 1);
        assert_eq!(manifest.logical_checksum, export.snapshot.logical_checksum);
        assert_eq!(
            manifest.graph_stream_checksum,
            export.graph_stream.stream_checksum
        );
        assert_eq!(manifest.graph_stream_byte_len, export.graph_stream.byte_len);
        assert_eq!(manifest.node_count, 2);
        assert_eq!(manifest.relationship_count, 1);
        assert_eq!(manifest.label_count, 2);
        assert_eq!(manifest.relationship_type_count, 1);
        assert_eq!(manifest.node_property_count, 4);
        assert_eq!(manifest.relationship_property_count, 1);
        assert!(manifest.validation.is_import_ready);
        assert!(manifest.validation.stable_identity_ready);
        assert!(export.snapshot.relationships[0].stable_id.is_some());
        assert!(export
            .graph_stream
            .encoded
            .starts_with("SKEIN_GRAPH_LIGHTNING_GRAPH_STREAM_V1\n"));
        assert!(export.graph_stream.encoded.contains("\nchecksum\t"));
        let stream_validation = export
            .graph_stream
            .validate_against_manifest(&export.manifest);
        assert!(stream_validation.is_valid);
        assert!(stream_validation.endpoint_integrity);
        assert!(stream_validation.manifest_matches);

        let corrupted = export
            .graph_stream
            .encoded
            .replace("relationship\t0\t0\t1", "relationship\t0\t0\t99");
        let corrupted_validation =
            validate_graph_lightning_graph_stream(&corrupted, Some(&export.manifest));
        assert!(!corrupted_validation.is_valid);
        assert!(!corrupted_validation.checksum_matches);
        assert!(!corrupted_validation.endpoint_integrity);
        assert_eq!(corrupted_validation.missing_targets.len(), 1);
    }

    {
        let mut db = Database::open(&path).unwrap();
        let first = db.prepare_graph_lightning_bootstrap_export().unwrap();
        db.query("CREATE (:Source {id: 'source-1', path: '/tmp/source.md'})")
            .unwrap();
        let second = db.prepare_graph_lightning_bootstrap_export().unwrap();

        assert_ne!(
            first.manifest.logical_checksum,
            second.manifest.logical_checksum
        );
        assert_ne!(
            first.manifest.schema_checksum,
            second.manifest.schema_checksum
        );
        assert!(second.manifest.validation.is_import_ready);
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_lightning_bootstrap_export_background_plan_uses_import_lane() {
    let mut db = Database::new();
    assert!(db
        .graph_lightning_bootstrap_export_background_work_plan(BackgroundWorkHint::default())
        .is_none());

    db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
        .unwrap();
    let plan = db
        .graph_lightning_bootstrap_export_background_work_plan(BackgroundWorkHint {
            active_topic: true,
            ..BackgroundWorkHint::default()
        })
        .unwrap();

    assert_eq!(plan.request.class, WorkClass::Import);
    assert_eq!(plan.request.estimated_operations, 3);
    assert!(plan.hint.active_topic);
}

#[test]
fn graph_lightning_background_bootstrap_export_uses_qos_without_gating_direct_export() {
    let path = unique_test_dir("graph_lightning_background_export_qos");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
            .unwrap();
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[WorkClass::Import.as_index()] = Some(0);
        let policy = LocalQosPolicy {
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        };
        let error = db
            .prepare_background_graph_lightning_bootstrap_export(&policy, &LocalQosState::default())
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("background graph lightning bootstrap export deferred"));
        assert!(!path.join("stable_ids.skein").exists());

        let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
        assert_eq!(export.manifest.node_count, 2);
        assert_eq!(export.manifest.relationship_count, 1);
        assert!(path.join("stable_ids.skein").exists());
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_lightning_scheduled_background_bootstrap_export_releases_import_budget() {
    let path = unique_test_dir("graph_lightning_scheduled_background_export");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
            .unwrap();
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[WorkClass::Import.as_index()] = Some(3);
        let policy = LocalQosPolicy {
            max_background_operations: Some(3),
            max_total_background_operations: Some(3),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        };
        let mut scheduler = LocalQosScheduler::new(policy);

        let export = db
            .prepare_scheduled_background_graph_lightning_bootstrap_export(&mut scheduler)
            .unwrap();

        assert_eq!(export.manifest.node_count, 2);
        assert_eq!(export.manifest.relationship_count, 1);
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class[WorkClass::Import.as_index()],
            0
        );
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_lightning_graph_stream_validation_skips_length_coded_metadata() {
    let mut db = Database::new();
    let root = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("root".to_string())),
                (
                    "content".to_string(),
                    Value::String("first line\nrelationship\t999\t1\t2".to_string()),
                ),
                (
                    "metadata".to_string(),
                    Value::Map(BTreeMap::from([(
                        "tags".to_string(),
                        Value::List(vec![
                            Value::String("alpha\nbeta".to_string()),
                            Value::Int(7),
                        ]),
                    )])),
                ),
            ]),
        )
        .unwrap();
    let target = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("target".to_string())),
                ("name".to_string(), Value::String("Target".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            root,
            target,
            "LINKS",
            BTreeMap::from([
                (
                    "id".to_string(),
                    Value::String("relationship-id".to_string()),
                ),
                (
                    "note".to_string(),
                    Value::String("edge\nnode\t999".to_string()),
                ),
            ]),
        )
        .unwrap();

    let export = db.prepare_graph_lightning_bootstrap_export().unwrap();
    let validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);

    assert!(validation.is_valid, "{:?}", validation.errors);
    assert_eq!(validation.node_count, 2);
    assert_eq!(validation.relationship_count, 1);
    assert!(validation.endpoint_integrity);
}

#[test]
fn canonical_snapshot_export_matches_wal_and_checkpoint_recovery() {
    let path = unique_test_dir("canonical_snapshot_storage_equivalence");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid', weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        db.query(
                "MATCH (m:Memory {id: 'root'}), (e:Entity {id: 'mid'}) CREATE (m)-[:MENTIONS {id: 'edge-root-mention'}]->(e)",
            )
            .unwrap();
        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    {
        let db = Database::open(&path).unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);
    }

    {
        let mut db = Database::open(&path).unwrap();
        db.checkpoint().unwrap();
        let checkpointed = db.export_canonical_graph_snapshot();
        assert_eq!(checkpointed, live_snapshot);
        assert!(checkpointed.validate().is_valid);
    }

    {
        let db = Database::open(&path).unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);
    }
    std::fs::remove_dir_all(path).unwrap();
}
