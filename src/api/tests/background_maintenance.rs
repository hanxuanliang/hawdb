use super::*;

#[test]
fn background_maintenance_candidates_are_empty_without_pending_work() {
    let db = Database::new();
    let search_index = SearchIndex::in_memory();

    assert!(db
        .background_maintenance_candidates(
            Some(&search_index),
            BackgroundMaintenanceOptions::default(),
        )
        .is_empty());
    assert!(db
        .rank_background_maintenance(
            Some(&search_index),
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions::default(),
        )
        .is_empty());
    let summary = db.background_maintenance_summary(
        Some(&search_index),
        &LocalQosPolicy::default(),
        &LocalQosState::default(),
        BackgroundMaintenanceOptions::default(),
    );
    assert_eq!(summary.total_candidates, 0);
    assert_eq!(summary.admitted_count, 0);
    assert_eq!(summary.deferred_count, 0);
    assert!(summary.top_admitted_kind.is_none());
    assert!(summary.ranked.is_empty());
}

#[test]
fn background_maintenance_kinds_have_stable_string_encodings() {
    let cases = [
        (
            BackgroundMaintenanceKind::SchemaMaintenance,
            "schema_maintenance",
        ),
        (
            BackgroundMaintenanceKind::PropertyIndexProjection,
            "property_index_projection",
        ),
        (
            BackgroundMaintenanceKind::SearchProjectionGraphDelta,
            "search_projection_graph_delta",
        ),
        (
            BackgroundMaintenanceKind::SearchProjectionRebuild,
            "search_projection_rebuild",
        ),
        (
            BackgroundMaintenanceKind::SearchProjectionMetadataRepair,
            "search_projection_metadata_repair",
        ),
        (
            BackgroundMaintenanceKind::GraphLightningBootstrapExport,
            "graph_lightning_bootstrap_export",
        ),
        (
            BackgroundMaintenanceKind::ExternalContentArtifactJob,
            "external_content_artifact_job",
        ),
    ];

    for (kind, name) in cases {
        assert_eq!(kind.as_str(), name);
        assert_eq!(name.parse::<BackgroundMaintenanceKind>(), Ok(kind));
    }
    assert!("unknown_background_work"
        .parse::<BackgroundMaintenanceKind>()
        .is_err());
}

#[test]
fn background_maintenance_skips_over_limit_search_projection_graph_delta() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let search_index = SearchIndex::in_memory();

    let candidates = db.background_maintenance_candidates(
        Some(&search_index),
        BackgroundMaintenanceOptions {
            search_projection_graph_delta: Some(SearchProjectionGraphDeltaRequest {
                upsert_node_ids: vec![0, 1],
                delete_document_ids: vec!["memory:old".to_string()],
                max_operations: Some(2),
                ..SearchProjectionGraphDeltaRequest::default()
            }),
            include_schema_maintenance: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );
    let names = candidates
        .iter()
        .map(|candidate| candidate.name.as_str())
        .collect::<Vec<_>>();

    assert!(!names.contains(&"search_projection_graph_delta"));
    assert!(names.contains(&"property_index_projection"));
    assert!(names.contains(&"search_projection_rebuild"));
}

#[test]
fn background_maintenance_includes_stale_search_projection_graph_delta() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let candidates = db.background_maintenance_candidates(
        Some(&search_index),
        BackgroundMaintenanceOptions {
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_rebuild: false,
            include_search_projection_metadata_repair: false,
            include_graph_lightning_bootstrap_export: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].kind,
        BackgroundMaintenanceKind::SearchProjectionGraphDelta
    );
    assert_eq!(candidates[0].name, "search_projection_graph_delta");
    assert_eq!(candidates[0].plan.request.class, WorkClass::Projection);
    assert_eq!(candidates[0].plan.request.estimated_operations, 1);
    assert_eq!(
        candidates[0].plan.hint.source_graph_commit_lag,
        db.store.commit_epoch()
    );
    assert_eq!(candidates[0].plan.hint.recent_delta_operations, 1);
    assert_eq!(
        candidates[0].search_projection_graph_delta.as_ref(),
        Some(&SearchProjectionGraphDeltaRequest {
            upsert_node_ids: vec![0],
            delete_document_ids: Vec::new(),
            max_operations: None,
            complete_through_graph_commit_epoch: Some(db.store.commit_epoch()),
        })
    );

    let ranked = db.rank_background_maintenance(
        Some(&search_index),
        &LocalQosPolicy::default(),
        &LocalQosState::default(),
        BackgroundMaintenanceOptions {
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_rebuild: false,
            include_search_projection_metadata_repair: false,
            include_graph_lightning_bootstrap_export: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );

    assert_eq!(ranked.len(), 1);
    assert_eq!(
        ranked[0].kind,
        BackgroundMaintenanceKind::SearchProjectionGraphDelta
    );
    assert_eq!(
        ranked[0].search_projection_graph_delta.as_ref(),
        Some(&SearchProjectionGraphDeltaRequest {
            upsert_node_ids: vec![0],
            delete_document_ids: Vec::new(),
            max_operations: None,
            complete_through_graph_commit_epoch: Some(db.store.commit_epoch()),
        })
    );
    assert!(ranked[0]
        .decision
        .reason_codes
        .contains(&BackgroundWorkReasonCode::SourceGraphCommitLag));
    assert!(ranked[0]
        .decision
        .reason_codes
        .contains(&BackgroundWorkReasonCode::RecentDeltaOperations));
}

#[test]
fn background_maintenance_can_disable_stale_search_projection_graph_delta() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let candidates = db.background_maintenance_candidates(
        Some(&search_index),
        BackgroundMaintenanceOptions {
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_graph_delta_freshness: false,
            include_search_projection_rebuild: false,
            include_search_projection_metadata_repair: false,
            include_graph_lightning_bootstrap_export: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );

    assert!(candidates.is_empty());
}

#[test]
fn background_maintenance_ranks_mixed_nowledge_background_work() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Vector search'})")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
        .unwrap();
    db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    db.schedule_external_content_artifact_job("source-parse", "parse");

    let search_index = SearchIndex::in_memory();
    let search_delta_request = SearchProjectionGraphDeltaRequest {
        upsert_node_ids: vec![0],
        complete_through_graph_commit_epoch: Some(2),
        ..SearchProjectionGraphDeltaRequest::default()
    };
    let options = BackgroundMaintenanceOptions {
        search_projection_graph_delta: Some(search_delta_request.clone()),
        external_content_artifact_estimated_operations: 1,
        ..BackgroundMaintenanceOptions::default()
    };
    let candidates = db.background_maintenance_candidates(Some(&search_index), options.clone());
    let names = candidates
        .iter()
        .map(|candidate| candidate.name.as_str())
        .collect::<Vec<_>>();
    let kinds = candidates
        .iter()
        .map(|candidate| candidate.kind)
        .collect::<Vec<_>>();

    assert!(names.contains(&"schema_maintenance"));
    assert!(names.contains(&"property_index_projection"));
    assert!(names.contains(&"search_projection_graph_delta"));
    assert!(names.contains(&"search_projection_rebuild"));
    assert!(names.contains(&"graph_lightning_bootstrap_export"));
    assert!(names.contains(&"external_content_artifact_job"));
    assert!(kinds.contains(&BackgroundMaintenanceKind::SchemaMaintenance));
    assert!(kinds.contains(&BackgroundMaintenanceKind::PropertyIndexProjection));
    assert!(kinds.contains(&BackgroundMaintenanceKind::SearchProjectionGraphDelta));
    assert!(kinds.contains(&BackgroundMaintenanceKind::SearchProjectionRebuild));
    assert!(kinds.contains(&BackgroundMaintenanceKind::GraphLightningBootstrapExport));
    assert!(kinds.contains(&BackgroundMaintenanceKind::ExternalContentArtifactJob));
    let graph_delta_candidate = candidates
        .iter()
        .find(|candidate| candidate.kind == BackgroundMaintenanceKind::SearchProjectionGraphDelta)
        .unwrap();
    assert_eq!(
        graph_delta_candidate.search_projection_graph_delta.as_ref(),
        Some(&search_delta_request)
    );
    for candidate in &candidates {
        assert_eq!(candidate.name, candidate.kind.as_str());
        assert_eq!(
            candidate.name.parse::<BackgroundMaintenanceKind>(),
            Ok(candidate.kind)
        );
    }

    let policy = LocalQosPolicy {
        max_total_background_operations: Some(5),
        ..LocalQosPolicy::default()
    };
    let state = LocalQosState {
        running_background_operations: 4,
        ..LocalQosState::default()
    };
    let ranked = db.rank_background_maintenance(Some(&search_index), &policy, &state, options);

    assert_eq!(ranked[0].name, "search_projection_graph_delta");
    assert_eq!(
        ranked[0].kind,
        BackgroundMaintenanceKind::SearchProjectionGraphDelta
    );
    assert_eq!(
        ranked[0].search_projection_graph_delta.as_ref(),
        Some(&search_delta_request)
    );
    assert_eq!(ranked[0].kind.as_str(), ranked[0].name);
    assert_eq!(
        ranked[0].name.parse::<BackgroundMaintenanceKind>(),
        Ok(ranked[0].kind)
    );
    assert_eq!(ranked[0].plan.request.class, WorkClass::Projection);
    assert_eq!(ranked[0].plan.request.class.as_str(), "projection");
    assert_eq!(ranked[0].plan.request.priority.as_str(), "background");
    assert_eq!(
        "projection".parse::<WorkClass>(),
        Ok(ranked[0].plan.request.class)
    );
    assert_eq!(
        "background".parse::<crate::WorkPriority>(),
        Ok(ranked[0].plan.request.priority)
    );
    assert!(matches!(ranked[0].decision.admission, QosAdmission::Admit));
    assert!(ranked[0]
        .decision
        .reason_codes
        .contains(&BackgroundWorkReasonCode::RecentDeltaOperations));
    assert!(ranked[0]
        .decision
        .reasons
        .iter()
        .any(|reason| reason.starts_with("recent delta operations")));
    assert!(ranked.iter().any(|item| {
        item.name == "search_projection_rebuild"
            && matches!(item.decision.admission, QosAdmission::Defer { .. })
    }));
    assert_eq!(
        policy.admit(
            &state,
            &WorkRequest::foreground(WorkClass::Query, usize::MAX),
        ),
        QosAdmission::Admit
    );
}

#[test]
fn background_maintenance_summary_exposes_qos_counts_and_stable_codes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three'})")
        .unwrap();

    let search_index = SearchIndex::in_memory();
    let search_delta_request = SearchProjectionGraphDeltaRequest {
        upsert_node_ids: vec![0],
        delete_document_ids: vec!["memory:old".to_string()],
        max_operations: Some(4),
        complete_through_graph_commit_epoch: Some(db.store.commit_epoch()),
    };
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[WorkClass::Projection.as_index()] = Some(2);
    let policy = LocalQosPolicy {
        max_background_operations_by_class: class_limits,
        ..LocalQosPolicy::default()
    };
    let summary = db.background_maintenance_summary(
        Some(&search_index),
        &policy,
        &LocalQosState::default(),
        BackgroundMaintenanceOptions {
            hint: BackgroundWorkHint {
                active_topic: true,
                query_probability_per_million: 250_000,
                staleness_millis: 750,
                staleness_ttl_millis: Some(1_000),
                freshness_slo_millis: Some(500),
                tenant_budget_remaining_operations: Some(8),
                ..BackgroundWorkHint::default()
            },
            search_projection_graph_delta: Some(search_delta_request),
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_metadata_repair: false,
            include_graph_lightning_bootstrap_export: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );

    assert_eq!(summary.total_candidates, 2);
    assert_eq!(summary.admitted_count, 1);
    assert_eq!(summary.deferred_count, 1);
    assert_eq!(summary.rejected_count, 0);
    assert_eq!(summary.admitted_estimated_operations, 2);
    assert_eq!(summary.deferred_estimated_operations, 3);
    assert_eq!(summary.executable_search_projection_graph_delta_count, 1);
    assert_eq!(summary.admitted_search_projection_graph_delta_count, 1);
    assert_eq!(summary.deferred_search_projection_graph_delta_count, 0);
    assert_eq!(summary.rejected_search_projection_graph_delta_count, 0);
    assert_eq!(
        summary.executable_search_projection_graph_delta_operations,
        2
    );
    assert_eq!(summary.admitted_search_projection_graph_delta_operations, 2);
    assert_eq!(
        summary.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
        Some(db.store.commit_epoch())
    );
    assert_eq!(
        summary.top_admitted_kind,
        Some(BackgroundMaintenanceKind::SearchProjectionGraphDelta)
    );
    assert_eq!(
        summary.top_admitted_name.as_deref(),
        Some("search_projection_graph_delta")
    );

    let admitted = summary
        .ranked
        .iter()
        .find(|item| item.admission_name == "admit")
        .unwrap();
    assert_eq!(admitted.name, "search_projection_graph_delta");
    assert_eq!(admitted.work_class_name, "projection");
    assert_eq!(admitted.priority_name, "background");
    assert!(admitted.hint_active_topic);
    assert_eq!(admitted.hint_recent_delta_operations, 2);
    assert_eq!(
        admitted.hint_source_graph_commit_lag,
        db.store.commit_epoch()
    );
    assert_eq!(admitted.hint_query_probability_per_million, 250_000);
    assert_eq!(admitted.hint_staleness_millis, 750);
    assert_eq!(admitted.hint_staleness_ttl_millis, Some(1_000));
    assert_eq!(admitted.hint_freshness_slo_millis, Some(500));
    assert_eq!(admitted.hint_tenant_budget_remaining_operations, Some(8));
    assert!(admitted.has_executable_search_projection_graph_delta);
    assert_eq!(
        admitted.search_projection_graph_delta_operation_count,
        Some(2)
    );
    assert_eq!(
        admitted.search_projection_graph_delta_upsert_node_count,
        Some(1)
    );
    assert_eq!(
        admitted.search_projection_graph_delta_delete_document_count,
        Some(1)
    );
    assert_eq!(
        admitted.search_projection_graph_delta_complete_through_graph_commit_epoch,
        Some(db.store.commit_epoch())
    );
    assert_eq!(
        admitted.search_projection_graph_delta_max_operations,
        Some(4)
    );
    assert!(admitted.admission_code_name.is_none());
    assert!(admitted
        .reason_code_names
        .contains(&"recent_delta_operations".to_string()));

    let deferred = summary
        .ranked
        .iter()
        .find(|item| item.admission_name == "defer")
        .unwrap();
    assert_eq!(deferred.name, "search_projection_rebuild");
    assert_eq!(
        deferred.admission_code_name.as_deref(),
        Some("class_background_limit_exceeded")
    );
    assert!(!deferred.has_executable_search_projection_graph_delta);
    assert_eq!(deferred.search_projection_graph_delta_operation_count, None);
    assert_eq!(
        deferred.search_projection_graph_delta_upsert_node_count,
        None
    );
    assert_eq!(
        deferred.search_projection_graph_delta_delete_document_count,
        None
    );
    assert_eq!(
        deferred.search_projection_graph_delta_complete_through_graph_commit_epoch,
        None
    );
    assert_eq!(deferred.search_projection_graph_delta_max_operations, None);
    assert!(deferred
        .reason_code_names
        .contains(&"admission_deferred".to_string()));
}

#[test]
fn background_maintenance_includes_graph_lightning_bootstrap_import_work() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
        .unwrap();
    let candidates = db.background_maintenance_candidates(
        None,
        BackgroundMaintenanceOptions {
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_rebuild: false,
            include_search_projection_metadata_repair: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].name, "graph_lightning_bootstrap_export");
    assert_eq!(candidates[0].plan.request.class, WorkClass::Import);
    assert_eq!(candidates[0].plan.request.estimated_operations, 3);
}

#[test]
fn background_maintenance_can_disable_graph_lightning_bootstrap_candidate() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})").unwrap();
    let candidates = db.background_maintenance_candidates(
        None,
        BackgroundMaintenanceOptions {
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_rebuild: false,
            include_search_projection_metadata_repair: false,
            include_graph_lightning_bootstrap_export: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );

    assert!(candidates.is_empty());
}

#[test]
fn background_maintenance_ranks_graph_lightning_against_import_lane_budget() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
        .unwrap();
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[WorkClass::Import.as_index()] = Some(2);
    let policy = LocalQosPolicy {
        max_background_operations_by_class: class_limits,
        ..LocalQosPolicy::default()
    };
    let ranked = db.rank_background_maintenance(
        None,
        &policy,
        &LocalQosState::default(),
        BackgroundMaintenanceOptions {
            include_schema_maintenance: false,
            include_property_index_projection: false,
            include_search_projection_rebuild: false,
            include_search_projection_metadata_repair: false,
            include_external_content_artifact_jobs: false,
            ..BackgroundMaintenanceOptions::default()
        },
    );

    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].name, "graph_lightning_bootstrap_export");
    assert!(matches!(
        ranked[0].decision.admission,
        QosAdmission::Defer { .. }
    ));
    assert!(ranked[0]
        .decision
        .reasons
        .iter()
        .any(|reason| reason.contains("above class limit")));
}
