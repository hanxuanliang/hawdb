use super::*;

#[test]
fn database_facade_builds_search_projection_delta_from_graph_nodes() {
    let mut db = Database::new();
    let node_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("new".to_string())),
                (
                    "title".to_string(),
                    Value::String("Incremental graph projection".to_string()),
                ),
                (
                    "content".to_string(),
                    Value::String("Graph node changes can feed bounded FTS deltas".to_string()),
                ),
            ]),
        )
        .unwrap();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Remove me"))
        .unwrap();
    let request = SearchProjectionGraphDeltaRequest {
        upsert_node_ids: vec![node_id.0],
        delete_document_ids: vec!["memory:old".to_string()],
        max_operations: Some(2),
        complete_through_graph_commit_epoch: Some(db.store.commit_epoch()),
    };

    let plan = db
        .search_projection_graph_delta_background_work_plan(
            &request,
            BackgroundWorkHint {
                recent_delta_operations: 2,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 2);

    let freshness_plan = db
        .search_projection_graph_delta_freshness_background_work_plan(
            &search_index,
            &request,
            BackgroundWorkHint {
                query_probability_per_million: 100_000,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(freshness_plan.request.class, WorkClass::Projection);
    assert_eq!(freshness_plan.request.estimated_operations, 2);
    assert_eq!(freshness_plan.hint.recent_delta_operations, 2);
    assert_eq!(
        freshness_plan.hint.source_graph_commit_lag,
        db.store.commit_epoch()
    );
    let ranked = LocalQosPolicy::default()
        .rank_background_work(&LocalQosState::default(), &[freshness_plan]);
    assert!(ranked[0]
        .decision
        .reasons
        .iter()
        .any(|reason| reason == "source graph commit lag 1"));

    let report = db
        .apply_search_projection_graph_delta(&mut search_index, request)
        .unwrap();

    assert_eq!(report.operation_count, 2);
    assert_eq!(report.upserted_documents, 1);
    assert_eq!(report.deleted_documents, 1);
    assert_eq!(report.source_graph_commit_epoch_before, None);
    assert_eq!(
        report.source_graph_commit_epoch_after,
        Some(db.store.commit_epoch())
    );
    assert!(report.source_graph_commit_epoch_updated);
    assert_eq!(
        search_index
            .projection_freshness()
            .source_graph_commit_epoch,
        Some(db.store.commit_epoch())
    );
    assert!(search_index.document("memory:old").is_none());
    assert!(search_index.document("memory:new").is_some());
    let hits = search_index.search("bounded FTS", None, SearchMode::Text, 10);
    assert_eq!(hits[0].id, "memory:new");
}

#[test]
fn database_facade_builds_search_projection_delta_request_from_changefeed() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Old title', content: 'Old body'})")
        .unwrap();

    let request = db
        .build_search_projection_graph_delta_request_after(0, Some(4))
        .unwrap()
        .unwrap();
    assert_eq!(request.upsert_node_ids, vec![0]);
    assert!(request.delete_document_ids.is_empty());
    assert_eq!(request.max_operations, Some(4));
    assert_eq!(
        request.complete_through_graph_commit_epoch,
        Some(db.store.commit_epoch())
    );

    let mut search_index = SearchIndex::in_memory();
    db.apply_search_projection_graph_delta(&mut search_index, request)
        .unwrap();

    db.query("MATCH (m:Memory {id: 'm1'}) SET m.id = 'm2'")
        .unwrap();
    let request = db
        .build_search_projection_graph_delta_request_from_freshness(&search_index, Some(2))
        .unwrap()
        .unwrap();
    assert_eq!(request.upsert_node_ids, vec![0]);
    assert_eq!(request.delete_document_ids, vec!["memory:m1".to_string()]);

    db.apply_search_projection_graph_delta(&mut search_index, request)
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm2'}) DETACH DELETE m")
        .unwrap();
    let request = db
        .build_search_projection_graph_delta_request_from_freshness(&search_index, Some(1))
        .unwrap()
        .unwrap();
    assert!(request.upsert_node_ids.is_empty());
    assert_eq!(request.delete_document_ids, vec!["memory:m2".to_string()]);
}

#[test]
fn durable_search_projection_catch_up_resumes_in_bounded_batches() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm2', title: 'Second'})")
        .unwrap();
    let path = unique_test_dir("durable_search_projection_catch_up");
    let mut search_index = SearchIndex::open(&path).unwrap();

    let first = db
        .catch_up_search_projection(&mut search_index, 1, 1)
        .unwrap();
    assert_eq!(first.start_durable_epoch, None);
    assert_eq!(first.applied_batch_count, 1);
    assert_eq!(first.applied_operation_count, 1);
    assert!(!first.complete);
    assert_eq!(first.end_applied_epoch, first.end_durable_epoch);

    let second = db
        .catch_up_search_projection(&mut search_index, 1, 4)
        .unwrap();
    assert!(second.applied_batch_count >= 1);
    assert!(second.complete);
    assert_eq!(second.end_durable_epoch, Some(db.commit_epoch()));
    assert!(search_index.document("memory:m1").is_some());
    assert!(search_index.document("memory:m2").is_some());

    drop(search_index);
    let reopened = SearchIndex::open(&path).unwrap();
    assert_eq!(
        reopened
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        Some(db.commit_epoch())
    );
    assert!(reopened.document("memory:m1").is_some());
    assert!(reopened.document("memory:m2").is_some());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn durable_search_projection_catch_up_rejects_unbounded_or_in_memory_usage() {
    let db = Database::new();
    let mut in_memory = SearchIndex::in_memory();
    let error = db
        .catch_up_search_projection(&mut in_memory, 1, 1)
        .unwrap_err();
    assert!(error.to_string().contains("persistent search index"));

    let path = unique_test_dir("durable_search_projection_catch_up_limits");
    let mut persistent = SearchIndex::open(&path).unwrap();
    assert!(db
        .catch_up_search_projection(&mut persistent, 0, 1)
        .unwrap_err()
        .to_string()
        .contains("max_operations_per_batch"));
    assert!(db
        .catch_up_search_projection(&mut persistent, 1, 0)
        .unwrap_err()
        .to_string()
        .contains("max_batches"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn durable_search_projection_catch_up_skips_nodes_deleted_before_projection() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Transient'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm1'}) DELETE m").unwrap();
    let path = unique_test_dir("durable_search_projection_catch_up_deleted_node");
    let mut search_index = SearchIndex::open(&path).unwrap();

    let report = db
        .catch_up_search_projection(&mut search_index, 2, 2)
        .unwrap();

    assert!(report.complete);
    assert_eq!(report.applied_operation_count, 1);
    assert_eq!(report.end_durable_epoch, Some(db.commit_epoch()));
    assert!(search_index.document("memory:m1").is_none());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_projection_changefeed_can_emit_watermark_only_delta_request() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'm1', title: 'Memory'})-[:MENTIONS]->(:Entity {id: 'e1', name: 'Entity'})",
    )
    .unwrap();

    let mut search_index = SearchIndex::in_memory();
    let request = db
        .build_search_projection_graph_delta_request_after(0, Some(4))
        .unwrap()
        .unwrap();
    db.apply_search_projection_graph_delta(&mut search_index, request)
        .unwrap();

    db.query("MATCH (m:Memory {id: 'm1'}), (e:Entity {id: 'e1'}) CREATE (m)-[:RELATES_TO]->(e)")
        .unwrap();
    let request = db
        .build_search_projection_graph_delta_request_from_freshness(&search_index, Some(1))
        .unwrap()
        .unwrap();

    assert!(request.upsert_node_ids.is_empty());
    assert!(request.delete_document_ids.is_empty());
    assert_eq!(
        request.complete_through_graph_commit_epoch,
        Some(db.store.commit_epoch())
    );

    let plan = db
        .search_projection_graph_delta_freshness_background_work_plan(
            &search_index,
            &request,
            BackgroundWorkHint::default(),
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 1);
    assert_eq!(plan.hint.recent_delta_operations, 1);
    assert_eq!(plan.hint.source_graph_commit_lag, 1);
}

#[test]
fn search_projection_changefeed_retention_forces_rebuild_for_expired_epoch() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_search_projection_change_log_entries: Some(1),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'm1', title: 'First'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    let request = db
        .build_search_projection_graph_delta_request_after(0, Some(1))
        .unwrap()
        .unwrap();
    db.apply_search_projection_graph_delta(&mut search_index, request)
        .unwrap();

    db.query("CREATE (:Memory {id: 'm2', title: 'Second'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm3', title: 'Third'})")
        .unwrap();

    let error = db
        .build_search_projection_graph_delta_request_from_freshness(&search_index, Some(2))
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("full search projection rebuild required"));
}

#[test]
fn search_projection_changefeed_retention_zero_disables_incremental_window() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_search_projection_change_log_entries: Some(0),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'm1', title: 'First'})")
        .unwrap();

    let error = db
        .build_search_projection_graph_delta_request_after(0, Some(1))
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("full search projection rebuild required"));
}

#[test]
fn search_projection_delta_request_requires_rebuild_when_changefeed_start_is_too_new() {
    let path = unique_test_dir("search_projection_changefeed_checkpoint_gap");
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                max_search_projection_change_log_entries: Some(0),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE (:Memory {id: 'm1', title: 'Checkpointed'})")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let search_index = SearchIndex::in_memory();
    let error = db
        .build_search_projection_graph_delta_request_from_freshness(&search_index, Some(4))
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("full search projection rebuild required"));
}

#[test]
fn graph_search_projection_delta_budget_failure_keeps_projection_unchanged() {
    let mut db = Database::new();
    let node_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("new".to_string())),
                (
                    "title".to_string(),
                    Value::String("Rejected graph projection".to_string()),
                ),
            ]),
        )
        .unwrap();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();

    let error = db
        .apply_search_projection_graph_delta(
            &mut search_index,
            SearchProjectionGraphDeltaRequest {
                upsert_node_ids: vec![node_id.0],
                delete_document_ids: vec!["memory:old".to_string()],
                max_operations: Some(1),
                complete_through_graph_commit_epoch: Some(db.store.commit_epoch()),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("operation count 2"));
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:new").is_none());
}

#[test]
fn graph_search_projection_delta_plan_is_absent_when_request_exceeds_limit() {
    let db = Database::new();
    let request = SearchProjectionGraphDeltaRequest {
        upsert_node_ids: vec![1, 2],
        delete_document_ids: vec!["memory:old".to_string()],
        max_operations: Some(2),
        complete_through_graph_commit_epoch: None,
    };

    assert!(db
        .search_projection_graph_delta_background_work_plan(&request, BackgroundWorkHint::default())
        .is_none());
}

#[test]
fn graph_search_projection_delta_without_watermark_keeps_freshness_epoch() {
    let mut db = Database::new();
    let node_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("new".to_string())),
                (
                    "title".to_string(),
                    Value::String("Partial graph projection".to_string()),
                ),
            ]),
        )
        .unwrap();
    let mut search_index = SearchIndex::in_memory();

    db.apply_search_projection_graph_delta(
        &mut search_index,
        SearchProjectionGraphDeltaRequest {
            upsert_node_ids: vec![node_id.0],
            delete_document_ids: Vec::new(),
            max_operations: Some(1),
            complete_through_graph_commit_epoch: None,
        },
    )
    .unwrap();

    assert_eq!(
        search_index
            .projection_freshness()
            .source_graph_commit_epoch,
        None
    );
    assert!(search_index.document("memory:new").is_some());
}

#[test]
fn graph_search_projection_delta_rejects_future_freshness_watermark() {
    let mut db = Database::new();
    let node_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("new".to_string())),
                (
                    "title".to_string(),
                    Value::String("Future graph projection".to_string()),
                ),
            ]),
        )
        .unwrap();
    let mut search_index = SearchIndex::in_memory();

    let error = db
        .apply_search_projection_graph_delta(
            &mut search_index,
            SearchProjectionGraphDeltaRequest {
                upsert_node_ids: vec![node_id.0],
                delete_document_ids: Vec::new(),
                max_operations: Some(1),
                complete_through_graph_commit_epoch: Some(db.store.commit_epoch() + 1),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("ahead of graph commit epoch"));
    assert!(search_index.document("memory:new").is_none());
}

#[test]
fn background_graph_search_projection_delta_uses_qos_admission() {
    let mut db = Database::new();
    let node_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("new".to_string())),
                (
                    "title".to_string(),
                    Value::String("Deferred graph projection".to_string()),
                ),
            ]),
        )
        .unwrap();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let policy = LocalQosPolicy {
        max_background_operations: Some(1),
        ..LocalQosPolicy::default()
    };

    let error = db
        .apply_background_search_projection_graph_delta(
            &mut search_index,
            &policy,
            &LocalQosState::default(),
            SearchProjectionGraphDeltaRequest {
                upsert_node_ids: vec![node_id.0],
                delete_document_ids: vec!["memory:old".to_string()],
                max_operations: Some(2),
                complete_through_graph_commit_epoch: Some(db.store.commit_epoch()),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:new").is_none());
}

#[test]
fn scheduled_graph_search_projection_delta_releases_budget_on_build_error() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = db
        .apply_scheduled_background_search_projection_graph_delta(
            &mut search_index,
            &mut scheduler,
            SearchProjectionGraphDeltaRequest {
                upsert_node_ids: vec![99],
                delete_document_ids: vec!["memory:old".to_string()],
                max_operations: Some(2),
                complete_through_graph_commit_epoch: Some(db.store.commit_epoch() + 1),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("ahead of graph commit epoch"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(search_index.document("memory:old").is_some());
}
