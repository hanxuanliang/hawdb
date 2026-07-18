use super::{
    validate_graph_lightning_graph_stream, BackgroundMaintenanceKind, BackgroundMaintenanceOptions,
    CanonicalStableIdMapping, Database, DatabaseConfig, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactRuntimeManifest,
    KnowledgeAugmentationJobInterruptRequest, KnowledgeAugmentationJobLifecycleBatchRequest,
    KnowledgeAugmentationJobLifecycleTransition, KnowledgeAugmentationJobLifecycleUpdate,
    KnowledgeAugmentationJobListOrder, KnowledgeAugmentationJobListRequest,
    KnowledgeAugmentationJobRequest, KnowledgeCandidateScoringPolicy, KnowledgeCandidateSource,
    KnowledgeCommunityAssignmentClearRequest, KnowledgeCommunityCleanupRequest,
    KnowledgeCommunityCreate, KnowledgeCommunityLifecycleBatchRequest,
    KnowledgeCommunityMembershipCreate, KnowledgeCommunityMembershipCreateBatchRequest,
    KnowledgeCommunitySummaryUpdate, KnowledgeEntityBatchRequest,
    KnowledgeEntityCreateBatchRequest, KnowledgeEntityCreateRequest,
    KnowledgeEntityDeleteBatchRequest, KnowledgeEntityDeleteRequest, KnowledgeEntityRequest,
    KnowledgeEntityUpsertBatchRequest, KnowledgeEntityUpsertRequest, KnowledgeFallbackReasonCode,
    KnowledgeFanoutReasonCode, KnowledgeGraphMetaRequest, KnowledgeGraphMetaStamp,
    KnowledgeGraphMetaStampBatchRequest, KnowledgeGraphPathDirection,
    KnowledgeLabelBackfillScanRequest, KnowledgeLabelCanonicalLookupRequest,
    KnowledgeLabelLifecycleBatchRequest, KnowledgeLabelLifecycleUpdate,
    KnowledgeLabelUsageListRequest, KnowledgeLabelUsageRequest, KnowledgeMemoryAccessBatchRequest,
    KnowledgeMemoryAccessTouch, KnowledgeMemoryLatestBatchRequest, KnowledgeMemoryLatestUpdate,
    KnowledgeMemoryLifecycleBatchRequest, KnowledgeMemoryLifecycleUpdate,
    KnowledgeNeighborDirection, KnowledgeNeighborsRequest,
    KnowledgeNormalizedSpaceMoveBatchRequest, KnowledgePageRankClearRequest,
    KnowledgePageRankScoreBatchRequest, KnowledgePageRankScoreUpdate, KnowledgePathRequest,
    KnowledgePropertyBatchRequest, KnowledgePropertyUpdateBatchRequest,
    KnowledgePropertyUpdateRequest, KnowledgeRelationshipCreateBatchRequest,
    KnowledgeRelationshipCreateRequest, KnowledgeRelationshipDeleteBatchRequest,
    KnowledgeRelationshipDeleteRequest, KnowledgeRelationshipUpdateBatchRequest,
    KnowledgeRelationshipUpdateRequest, KnowledgeRelationshipUpsertBatchRequest,
    KnowledgeRelationshipUpsertRequest, KnowledgeRelationshipsRequest,
    KnowledgeRetrievalEmptyReasonCode, KnowledgeRetrievalRequest, KnowledgeSchemaMigrationApply,
    KnowledgeSchemaMigrationApplyBatchRequest, KnowledgeScopedEntityBatchRequest,
    KnowledgeScopedEntityDeleteBatchRequest, KnowledgeScopedEntityDeleteRequest,
    KnowledgeScopedEntityRequest, KnowledgeScopedNeighborsRequest, KnowledgeScopedPathRequest,
    KnowledgeScopedPropertyBatchRequest, KnowledgeScopedPropertyUpdateBatchRequest,
    KnowledgeScopedPropertyUpdateRequest, KnowledgeScopedRelationshipCreateBatchRequest,
    KnowledgeScopedRelationshipCreateRequest, KnowledgeScopedRelationshipDeleteBatchRequest,
    KnowledgeScopedRelationshipDeleteRequest, KnowledgeScopedRelationshipUpdateBatchRequest,
    KnowledgeScopedRelationshipUpdateRequest, KnowledgeScopedRelationshipsRequest,
    KnowledgeScopedSubgraphRequest, KnowledgeSkillLifecycleBatchRequest,
    KnowledgeSkillLifecycleUpdate, KnowledgeSkillUsageStatsBatchRequest,
    KnowledgeSkillUsageStatsUpdate, KnowledgeSourceLifecycleBatchRequest,
    KnowledgeSourceLifecycleUpdate, KnowledgeSourceMemoryCountAdjustment,
    KnowledgeSourceMemoryCountBatchRequest, KnowledgeSourceReferenceRelationshipCleanupRequest,
    KnowledgeSubgraphRequest, KnowledgeThreadMessageCountBatchRequest,
    KnowledgeThreadMessageCountUpdate, KnowledgeThreadMetadataBatchRequest,
    KnowledgeThreadMetadataUpdate, KnowledgeTraversalFallbackReasonCode,
    KnowledgeTruncationReasonCode, NowledgeGraphAdapter, NowledgeGraphStatement, QueryOutput,
    RecoveryMode, SearchProjectionGraphDeltaRequest, GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
};
use crate::optimizer::PlanCost;
use crate::qos::{
    BackgroundWorkHint, BackgroundWorkReasonCode, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    QosAdmission, WorkClass, WorkRequest,
};
use crate::schema::{
    ConstraintKind, ConstraintSubject, IndexKind, PropertyType, SchemaObjectState, TableKind,
};
use crate::search::{
    MetadataRepairOptions, SearchDocument, SearchFallbackReasonCode, SearchFusionWeights,
    SearchIndex, SearchMode, SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
    SearchRebuildOptions, SearchTruncationReasonCode,
};
use crate::store::{NodeId, DENSE_ADJACENCY_DEGREE_THRESHOLD};
use crate::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};

#[test]
fn runs_create_match_return_demo() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Runtime strategy'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
}

#[test]
fn knowledge_truncation_reason_codes_have_stable_string_encodings() {
    let cases = [
        (
            KnowledgeTruncationReasonCode::RankWindowExceeded,
            "rank_window_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::SearchLimitExceeded,
            "search_limit_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::PartialCandidateReturn,
            "partial_candidate_return",
        ),
        (
            KnowledgeTruncationReasonCode::GraphSeedLimitExceeded,
            "graph_seed_limit_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::GraphContextLimitExceeded,
            "graph_context_limit_exceeded",
        ),
        (
            KnowledgeTruncationReasonCode::CandidateLimitExceeded,
            "candidate_limit_exceeded",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(name.parse::<KnowledgeTruncationReasonCode>(), Ok(code));
    }
    assert!("limit".parse::<KnowledgeTruncationReasonCode>().is_err());
}

#[test]
fn knowledge_fallback_reason_codes_have_stable_string_encodings() {
    let cases = [
        (
            KnowledgeFallbackReasonCode::GraphSeedLimitZero,
            "graph_seed_limit_zero",
        ),
        (
            KnowledgeFallbackReasonCode::GraphContextLimitZero,
            "graph_context_limit_zero",
        ),
        (
            KnowledgeFallbackReasonCode::GraphContextMaxHopsZero,
            "graph_context_max_hops_zero",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(name.parse::<KnowledgeFallbackReasonCode>(), Ok(code));
    }
    assert!("disabled".parse::<KnowledgeFallbackReasonCode>().is_err());
}

#[test]
fn knowledge_traversal_fallback_reason_codes_have_stable_string_encodings() {
    let cases = [
        (
            KnowledgeTraversalFallbackReasonCode::SeedNotFound,
            "seed_not_found",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::TargetNotFound,
            "target_not_found",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::MaxHopsZero,
            "max_hops_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::PathLimitZero,
            "path_limit_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::NodeLimitZero,
            "node_limit_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::RelationshipLimitZero,
            "relationship_limit_zero",
        ),
        (
            KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound,
            "relationship_type_not_found",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(
            name.parse::<KnowledgeTraversalFallbackReasonCode>(),
            Ok(code)
        );
    }
    assert!("missing"
        .parse::<KnowledgeTraversalFallbackReasonCode>()
        .is_err());
}

#[test]
fn knowledge_fanout_reason_codes_have_stable_string_encodings() {
    let cases = [
        (KnowledgeFanoutReasonCode::DenseAdjacency, "dense_adjacency"),
        (
            KnowledgeFanoutReasonCode::GraphContextLimitReached,
            "graph_context_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::GraphSeedLimitReached,
            "graph_seed_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::CandidateLimitReached,
            "candidate_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::PathLimitReached,
            "path_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::NodeLimitReached,
            "node_limit_reached",
        ),
        (
            KnowledgeFanoutReasonCode::RelationshipLimitReached,
            "relationship_limit_reached",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(name.parse::<KnowledgeFanoutReasonCode>(), Ok(code));
    }
    assert!("limit".parse::<KnowledgeFanoutReasonCode>().is_err());
}

#[test]
fn nowledge_graph_adapter_runs_parameterized_query_explain_and_transaction() {
    let mut db = Database::new();
    let mut adapter = NowledgeGraphAdapter::new(&mut db);
    let transaction = adapter
        .transaction(&[
            NowledgeGraphStatement {
                cypher: "CREATE (:Memory {id: $id, title: $title})".to_string(),
                parameters: BTreeMap::from([
                    ("id".to_string(), Value::Int(1)),
                    (
                        "title".to_string(),
                        Value::String("Adapter memory".to_string()),
                    ),
                ]),
            },
            NowledgeGraphStatement {
                cypher: "CREATE (:Memory {id: $id, title: $title})".to_string(),
                parameters: BTreeMap::from([
                    ("id".to_string(), Value::Int(2)),
                    (
                        "title".to_string(),
                        Value::String("Second adapter memory".to_string()),
                    ),
                ]),
            },
        ])
        .unwrap();

    assert_eq!(transaction.statement_outputs.len(), 2);
    assert_eq!(transaction.commit_output.rows.len(), 2);

    let query = adapter
        .query(&NowledgeGraphStatement {
            cypher: "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title".to_string(),
            parameters: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        })
        .unwrap();
    assert_eq!(
        query.rows[0].get("title"),
        Some(&Value::String("Adapter memory".to_string()))
    );

    let explain = adapter
        .explain(&NowledgeGraphStatement {
            cypher: "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title".to_string(),
            parameters: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        })
        .unwrap();
    assert!(explain.plan.contains("ProjectExec"));
    assert!(explain.trace.selected_plan.contains("ProjectExec"));
}

#[test]
fn nowledge_graph_adapter_transaction_rolls_back_on_parameter_error() {
    let mut db = Database::new();
    let error = {
        let mut adapter = NowledgeGraphAdapter::new(&mut db);
        adapter
            .transaction(&[
                NowledgeGraphStatement {
                    cypher: "CREATE (:Memory {id: $id, title: 'Buffered'})".to_string(),
                    parameters: BTreeMap::from([("id".to_string(), Value::Int(1))]),
                },
                NowledgeGraphStatement {
                    cypher: "CREATE (:Memory {id: $missing, title: 'Missing'})".to_string(),
                    parameters: BTreeMap::new(),
                },
            ])
            .unwrap_err()
    };

    assert!(error.to_string().contains("missing parameter"));
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn nowledge_graph_adapter_retrieves_knowledge_with_external_projection() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root retrieval', content: 'Adapter knowledge retrieval'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'Skein'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let adapter = NowledgeGraphAdapter::new(&mut db);
    let output = adapter.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "adapter retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 4,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 2,
            graph_context_limit: 4,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.graph_commit_epoch, 1);
    assert_eq!(output.projection_freshness.document_count, 2);
    assert_eq!(output.diagnostics.search_total_hits, 1);
    assert_eq!(
        output.diagnostics.search_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(
        output.diagnostics.search_candidate_set.id_space,
        "search_projection_document_id"
    );
    assert_eq!(
        output.diagnostics.search_candidate_set.representation,
        "sorted_document_ids"
    );
    assert!(output.diagnostics.search_candidate_set.exact);
    assert_eq!(output.diagnostics.search_limit, 4);
    assert!(!output.diagnostics.search_truncated);
    assert!(output.diagnostics.search_truncation_reasons.is_empty());
    assert_eq!(output.diagnostics.rank_window, None);
    assert_eq!(
        output.diagnostics.search_fusion_weights,
        SearchFusionWeights::default()
    );
    assert_eq!(output.diagnostics.graph_seed_limit, 2);
    assert_eq!(
        output.diagnostics.graph_seed_input_candidate_set.id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .representation,
        "filtered_node_ids"
    );
    assert_eq!(
        output.diagnostics.graph_seed_candidate_set.representation,
        "ranked_node_ids"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_context_input_candidate_set
            .id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_context_input_candidate_set
            .representation,
        "context_seed_node_ids"
    );
    assert_eq!(
        output.diagnostics.graph_context_candidate_set.id_space,
        "canonical_graph_relationship_id"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_context_candidate_set
            .representation,
        "expanded_relationship_ids"
    );
    assert!(!output.diagnostics.graph_seed_truncated);
    assert!(output.diagnostics.graph_seed_truncation_reasons.is_empty());
    assert_eq!(output.diagnostics.graph_context_limit, 4);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert!(!output.diagnostics.graph_context_truncated);
    assert!(output
        .diagnostics
        .graph_context_truncation_reasons
        .is_empty());
    assert_eq!(output.diagnostics.candidate_limit, None);
    assert_eq!(
        output.diagnostics.candidate_total_count,
        output.diagnostics.candidate_count
    );
    assert!(!output.diagnostics.candidate_truncated);
    assert!(output.diagnostics.candidate_truncation_reasons.is_empty());
    assert_eq!(output.diagnostics.graph_context_path_count, 2);
    assert_eq!(output.graph_context_paths.len(), 2);
    assert!(output
        .evidence
        .iter()
        .any(|evidence| evidence.canonical_node_id == Some(0)));
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == "memory:root"));
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == "Memory:root"));
}

#[test]
fn database_facade_applies_search_projection_delta_without_background_admission() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Remove me"))
        .unwrap();

    let report = db
        .apply_search_projection_delta(
            &mut search_index,
            SearchProjectionDelta {
                upserts: vec![search_projection_row(
                    "new",
                    "Foreground projection",
                    "Caller requested incremental FTS update",
                )],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(report.action, "incremental_update");
    assert_eq!(report.operation_count, 2);
    assert!(search_index.document("memory:old").is_none());
    assert!(search_index.document("memory:new").is_some());
}

#[test]
fn database_facade_background_search_projection_delta_uses_qos_admission() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let delta = SearchProjectionDelta {
        upserts: vec![search_projection_row(
            "new",
            "Deferred projection",
            "Internal background FTS update",
        )],
        deletes: vec!["memory:old".to_string()],
        max_operations: Some(2),
        source_graph_commit_epoch: None,
    };

    let plan = db
        .search_projection_delta_background_work_plan(
            &delta,
            BackgroundWorkHint {
                recent_delta_operations: 2,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 2);

    let policy = LocalQosPolicy {
        max_background_operations: Some(1),
        ..LocalQosPolicy::default()
    };
    let error = db
        .apply_background_search_projection_delta(
            &mut search_index,
            &policy,
            &LocalQosState::default(),
            delta,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:new").is_none());
}

#[test]
fn database_facade_scheduled_search_projection_delta_releases_background_budget() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = db
        .apply_scheduled_background_search_projection_delta(
            &mut search_index,
            &mut scheduler,
            SearchProjectionDelta {
                upserts: vec![search_projection_row(
                    "new",
                    "Rejected by delta budget",
                    "Internal background FTS update",
                )],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(1),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("exceeded configured limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:new").is_none());
}

#[test]
fn database_facade_background_search_projection_rebuild_uses_qos_admission() {
    let mut db = Database::new();
    for id in ["mem_1", "mem_2"] {
        db.query(&format!(
            "CREATE (:Memory {{id: '{id}', title: '{id}', content: 'background rebuild'}})"
        ))
        .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();

    let plan = db
        .search_projection_rebuild_background_work_plan(
            &search_index,
            BackgroundWorkHint {
                active_topic: true,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 2);

    let policy = LocalQosPolicy {
        max_background_operations: Some(1),
        ..LocalQosPolicy::default()
    };
    let error = db
        .rebuild_background_search_projection(
            &mut search_index,
            &policy,
            &LocalQosState::default(),
            SearchRebuildOptions::default(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:mem_1").is_none());
}

#[test]
fn database_facade_scheduled_search_projection_rebuild_releases_background_budget() {
    let mut db = Database::new();
    for id in ["mem_1", "mem_2"] {
        db.query(&format!(
            "CREATE (:Memory {{id: '{id}', title: '{id}', content: 'scheduled rebuild'}})"
        ))
        .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = db
        .rebuild_scheduled_background_search_projection(
            &mut search_index,
            &mut scheduler,
            SearchRebuildOptions { max_rows: Some(1) },
        )
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:mem_1").is_none());
}

#[test]
fn database_facade_repairs_search_projection_metadata_from_graph() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Graph title', content: 'graph body', source_id: 'src_1', space_id: 'team'})",
    )
    .unwrap();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();

    let summary = db
        .repair_search_projection_metadata(&mut search_index, MetadataRepairOptions::default())
        .unwrap();

    assert_eq!(summary.scanned_nodes, 1);
    assert_eq!(summary.repaired_documents, 1);
    let document = search_index.document("memory:mem_1").unwrap();
    assert_eq!(document.title, "Old title");
    assert_eq!(document.content, "Old body should stay");
    assert_eq!(document.embedding, Some(vec![1.0, 0.0]));
    assert_eq!(
        document.metadata.get("kind").map(String::as_str),
        Some("memory")
    );
    assert_eq!(
        document.metadata.get("source_id").map(String::as_str),
        Some("src_1")
    );
    assert_eq!(
        document.metadata.get("space_id").map(String::as_str),
        Some("team")
    );
}

#[test]
fn database_facade_background_search_projection_metadata_repair_uses_qos_admission() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Graph title'})")
        .unwrap();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();

    let plan = db
        .search_projection_metadata_repair_background_work_plan(
            &search_index,
            BackgroundWorkHint {
                staleness_millis: 10_000,
                staleness_ttl_millis: Some(1_000),
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 1);

    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };
    let error = db
        .repair_background_search_projection_metadata(
            &mut search_index,
            &policy,
            &LocalQosState::default(),
            MetadataRepairOptions::default(),
            plan.request.estimated_operations,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    assert_eq!(
        search_index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
}

#[test]
fn database_facade_scheduled_search_projection_metadata_repair_releases_background_budget() {
    let mut db = Database::new();
    for id in ["mem_1", "mem_2"] {
        db.query(&format!("CREATE (:Memory {{id: '{id}', title: '{id}'}})"))
            .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    for id in ["mem_1", "mem_2"] {
        search_index
            .upsert(SearchDocument {
                id: format!("memory:{id}"),
                title: id.to_string(),
                content: id.to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
    }
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = db
        .repair_scheduled_background_search_projection_metadata(
            &mut search_index,
            &mut scheduler,
            MetadataRepairOptions { max_rows: Some(1) },
            2,
        )
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        search_index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
}

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
        let mut db = Database::open(&path).unwrap();
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
                complete_through_graph_commit_epoch: Some(db.store.commit_epoch()),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("missing node 99"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(search_index.document("memory:old").is_some());
}

fn search_projection_row(external_id: &str, title: &str, body: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: external_id.to_string(),
        title: title.to_string(),
        body: body.to_string(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

#[test]
fn nowledge_graph_adapter_exposes_typed_knowledge_navigation() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})")
            .unwrap();

    let adapter = NowledgeGraphAdapter::new(&mut db);
    let entity = adapter.knowledge_entity(&KnowledgeEntityRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
    });
    assert_eq!(entity.graph_commit_epoch, 1);
    assert_eq!(
        entity
            .entity
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("root")
    );

    let neighbors = adapter.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit: 4,
        max_hops: 1,
    });
    assert_eq!(neighbors.paths.len(), 1);
    assert_eq!(neighbors.diagnostics.path_count, 1);
    assert_eq!(neighbors.diagnostics.fanout_reason_count, 0);

    let paths = adapter.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "leaf".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        limit: 4,
    });
    assert_eq!(paths.paths.len(), 1);
    assert_eq!(paths.diagnostics.target_found, Some(true));
    assert_eq!(paths.diagnostics.relationship_count, 1);

    let subgraph = adapter.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 4,
        relationship_limit: 4,
    });
    assert_eq!(subgraph.nodes.len(), 2);
    assert_eq!(subgraph.relationships.len(), 1);
    assert_eq!(subgraph.diagnostics.node_count, 2);
    assert_eq!(subgraph.diagnostics.relationship_count, 1);
}

#[test]
fn database_session_runs_transaction_control_statements() {
    let mut db = Database::new();
    {
        let mut session = db.session();
        assert!(session.query("BEGIN TRANSACTION").unwrap().rows.is_empty());
        assert!(session
            .query("CREATE (:Memory {id: 1, title: 'Committed'})")
            .unwrap()
            .rows
            .is_empty());
        let commit = session.query("COMMIT;").unwrap();
        assert_eq!(commit.rows.len(), 1);
    }

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Committed".to_string()))
    );
}

#[test]
fn database_session_rolls_back_buffered_transaction() {
    let mut db = Database::new();
    {
        let mut session = db.session();
        session.query("BEGIN TRANSACTION").unwrap();
        session
            .query("CREATE (:Memory {id: 1, title: 'Rolled back'})")
            .unwrap();
        assert!(session.query("ROLLBACK").unwrap().rows.is_empty());
    }

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn database_session_rejects_reads_inside_write_transaction() {
    let mut db = Database::new();
    let mut session = db.session();
    session.query("BEGIN TRANSACTION").unwrap();
    let error = session
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("session transaction query must be a mutation"));
}

#[test]
fn returns_relationship_endpoint_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'left'})-[:RELATES_TO]->(:Entity {id: 'right'})")
        .unwrap();

    let output = db
        .query("MATCH (e1:Entity)-[r:RELATES_TO]->(e2:Entity) RETURN e1.id, e2.id")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("e1.id"),
        Some(&Value::String("left".to_string()))
    );
    assert_eq!(
        output.rows[0].get("e2.id"),
        Some(&Value::String("right".to_string()))
    );
}

#[test]
fn retrieves_knowledge_through_database_facade() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Graph retrieval', content: 'Projection freshness and truncation diagnostics', source_id: 'thread_1'})-[:MENTIONS {chunk_index: 3, source_id: 'thread_1'}]->(:Entity {id: 'entity_1', name: 'Skein'})")
            .unwrap();
    db.query(
            "CREATE (:Memory {id: 'mem_2', title: 'Graph retrieval', content: 'Search result diagnostics'})",
        )
        .unwrap();
    let extra_entity = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("entity_2".to_string())),
                ("name".to_string(), Value::String("Graph".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            extra_entity,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    let rebuild = db
        .rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "projection diagnostics".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 2,
            graph_context_limit: 1,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(rebuild.indexed_documents, 4);
    assert_eq!(output.graph_commit_epoch, 4);
    assert_eq!(output.projection_freshness.document_count, 4);
    assert_eq!(
        output.projection_freshness.source_graph_commit_epoch,
        Some(4)
    );
    assert_eq!(output.search.total_hits, 2);
    assert_eq!(output.search.hits.len(), 1);
    assert_eq!(output.diagnostics.graph_commit_epoch, 4);
    assert_eq!(
        output.diagnostics.projection_source_graph_commit_epoch,
        Some(4)
    );
    assert_eq!(output.diagnostics.projection_commit_lag, 0);
    assert!(!output.diagnostics.projection_stale);
    assert!(!output.diagnostics.projection_full_reindex_needed);
    assert!(!output.diagnostics.projection_metadata_repair_needed);
    assert_eq!(output.diagnostics.search_limit, 1);
    assert!(output.diagnostics.search_truncated);
    assert_eq!(
        output.diagnostics.search_truncation_reason_codes,
        vec![SearchTruncationReasonCode::LimitExceeded]
    );
    assert_eq!(
        output.diagnostics.search_truncation_reasons,
        vec!["limit 1 returned from 2 matching hits".to_string()]
    );
    assert_eq!(output.diagnostics.rank_window, None);
    assert_eq!(output.diagnostics.graph_seed_limit, 2);
    assert_eq!(output.diagnostics.graph_context_limit, 1);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert!(output.diagnostics.graph_context_truncated);
    assert_eq!(
        output.diagnostics.graph_context_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::GraphContextLimitExceeded]
    );
    assert_eq!(
        output.diagnostics.graph_context_truncation_reasons,
        vec!["graph_context_limit 1 reached while expanding hit memory:mem_1".to_string()]
    );
    assert_eq!(output.diagnostics.candidate_limit, None);
    assert_eq!(output.evidence.len(), 1);
    assert_eq!(output.candidates.len(), 2);
    assert_eq!(
        output.candidates[0].source,
        KnowledgeCandidateSource::SearchHit
    );
    assert_eq!(output.candidates[0].source_rank, 1);
    assert_eq!(output.candidates[0].id, output.search.hits[0].id);
    assert_eq!(output.candidates[0].canonical_node_id, Some(0));
    assert_eq!(
        output.candidates[0].merged_sources,
        vec![
            KnowledgeCandidateSource::SearchHit,
            KnowledgeCandidateSource::GraphSeed
        ]
    );
    assert_eq!(output.candidates[0].entity.as_ref().unwrap().node_id, 0);
    assert_eq!(output.candidates[0].graph_context_path_count, 1);
    assert_eq!(
        output.candidates[0].evidence.as_ref().unwrap(),
        &output.evidence[0]
    );
    assert!(output.candidates[0]
        .matched_properties
        .contains(&"content".to_string()));
    let text_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text knowledge retriever report");
    assert_eq!(text_retriever.candidate_count, 2);
    assert_eq!(text_retriever.limit, Some(1));
    assert_eq!(text_retriever.rank_window, None);
    assert_eq!(text_retriever.fusion_weight, Some(1.0));
    let search_text_retriever = output
        .search
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text search retriever report");
    assert_eq!(
        text_retriever.candidate_set,
        search_text_retriever.candidate_set
    );
    assert_eq!(
        text_retriever.top_candidates[0].kind.as_deref(),
        Some("memory")
    );
    assert_eq!(
        text_retriever.top_candidates[0].external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(
        text_retriever.top_candidates[0].source_id.as_deref(),
        Some("thread_1")
    );
    assert_eq!(text_retriever.top_candidates[0].canonical_node_id, Some(0));
    assert_eq!(text_retriever.top_candidates[0].graph_context_path_count, 1);
    assert_eq!(
        text_retriever.top_candidates[0].projection_freshness,
        Some(output.projection_freshness.clone())
    );
    assert!(text_retriever.top_candidates[0]
        .matched_spans
        .iter()
        .any(|span| span.field == "content" && span.term == "projection"));
    assert!(output.search.truncated);
    assert!(output.search.truncation_reasons[0].contains("limit 1"));
    assert_eq!(
        output.search.hits[0].projection_freshness,
        output.projection_freshness
    );
    assert_eq!(output.search.hits[0].kind.as_deref(), Some("memory"));
    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(output.graph_context_paths[0].hop, 1);
    assert_eq!(
        output.graph_context_paths[0].direction,
        KnowledgeGraphPathDirection::Outgoing
    );
    assert_eq!(
        output.graph_context_paths[0].relationship_type.as_str(),
        "MENTIONS"
    );
    assert_eq!(
        output.graph_context_paths[0]
            .relationship_properties
            .get("chunk_index"),
        Some(&Value::Int(3))
    );
    assert_eq!(
        output.graph_context_paths[0]
            .relationship_properties
            .get("source_id"),
        Some(&Value::String("thread_1".to_string()))
    );
    assert_eq!(
        output.graph_context_paths[0].source_external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("entity_1")
    );
    assert_eq!(output.evidence[0].hit_id, output.search.hits[0].id);
    assert_eq!(output.evidence[0].kind.as_deref(), Some("memory"));
    assert_eq!(output.evidence[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.evidence[0].source_id.as_deref(), Some("thread_1"));
    assert_eq!(output.evidence[0].canonical_node_id, Some(0));
    assert_eq!(output.evidence[0].graph_context_path_count, 1);
    assert!(output.evidence[0]
        .matched_terms
        .contains(&"projection".to_string()));
    assert!(output.evidence[0]
        .matched_spans
        .iter()
        .any(|span| span.field == "content"
            && span.text == "Projection"
            && span.term == "projection"));
    assert!(output.evidence[0].text_score > 0.0);
    let vector_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("vector retriever report");
    assert_eq!(
        vector_report.input_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(output.graph_seeds.len(), 2);
    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert!(graph_seed_report.available);
    assert_eq!(graph_seed_report.candidate_count, 2);
    assert_eq!(
        graph_seed_report.input_candidate_set.id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        graph_seed_report.input_candidate_set.representation,
        "filtered_node_ids"
    );
    assert_eq!(graph_seed_report.input_candidate_set.cardinality, 4);
    assert_eq!(graph_seed_report.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        graph_seed_report
            .input_candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert_eq!(graph_seed_report.limit, Some(2));
    assert_eq!(graph_seed_report.rank_window, None);
    assert_eq!(graph_seed_report.fusion_weight, None);
    assert!(graph_seed_report.fallback_reasons.is_empty());
    assert_eq!(
        graph_seed_report.candidate_set.id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        graph_seed_report.candidate_set.representation,
        "ranked_node_ids"
    );
    assert_eq!(graph_seed_report.candidate_set.cardinality, 2);
    assert!(graph_seed_report.candidate_set.exact);
    assert_eq!(
        graph_seed_report
            .candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert_eq!(graph_seed_report.candidate_set.policy_epoch, None);
    assert_eq!(graph_seed_report.top_candidates.len(), 2);
    assert_eq!(graph_seed_report.top_candidates[0].rank, 1);
    assert_eq!(
        graph_seed_report.top_candidates[0].kind.as_deref(),
        Some("Memory")
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(graph_seed_report.top_candidates[0].source_id, None);
    assert_eq!(
        graph_seed_report.top_candidates[0].canonical_node_id,
        Some(0)
    );
    assert!(graph_seed_report.top_candidates[0].matched_spans.is_empty());
    assert_eq!(
        graph_seed_report.top_candidates[0].graph_context_path_count,
        0
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].projection_freshness,
        None
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].id.as_str(),
        "Memory:mem_1"
    );
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(
        output.candidates[1].source,
        KnowledgeCandidateSource::GraphSeed
    );
    assert_eq!(output.candidates[1].source_rank, 2);
    assert_eq!(output.candidates[1].canonical_node_id, Some(2));
    assert!(output.candidates[1].evidence.is_none());
    assert_eq!(
        output.candidates[1].entity.as_ref().unwrap(),
        &output.graph_seeds[1].entity
    );
    assert!(output.graph_seeds[0]
        .matched_properties
        .contains(&"content".to_string()));
    assert!(output.graph_seeds[0].score >= output.graph_seeds[1].score);
    assert_eq!(output.diagnostics.graph_context_path_count, 1);
    assert_eq!(output.diagnostics.graph_context_node_count, 2);
    assert_eq!(output.diagnostics.graph_context_relationship_count, 1);
    assert_eq!(output.diagnostics.fanout_reason_count, 1);
    assert!(output.diagnostics.warnings.is_empty());
    assert_eq!(output.fanout_reasons.len(), 1);
    assert!(output.fanout_reasons[0].contains("graph_context_limit 1"));
    assert_eq!(
        output.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::GraphContextLimitReached]
    );
    assert_eq!(output.fanout_reason_details.len(), 1);
    assert_eq!(
        output.fanout_reason_details[0].code,
        KnowledgeFanoutReasonCode::GraphContextLimitReached
    );
    assert_eq!(output.fanout_reason_details[0].limit, Some(1));
    assert_eq!(
        output.fanout_reason_details[0].seed_hit_id.as_deref(),
        Some("memory:mem_1")
    );
    assert_eq!(
        output.diagnostics.fanout_reason_codes,
        output.fanout_reason_codes
    );
    assert_eq!(
        output.diagnostics.fanout_reason_details,
        output.fanout_reason_details
    );
    assert_eq!(output.diagnostics.fanout_reasons, output.fanout_reasons);
}

#[test]
fn knowledge_retrieval_diagnostics_expose_search_fallback_reasons() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Fallback retrieval', content: 'text leg survives vector fallback'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "fallback retrieval".to_string(),
            query_embedding: Some(vec![1.0, 0.0]),
            mode: SearchMode::Hybrid,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert!(!output.search.hits.is_empty());
    assert!(output
        .search
        .fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .search
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .diagnostics
        .search_fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
    let vector_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("expected vector retriever report");
    assert!(!vector_report.available);
    assert!(vector_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(vector_report
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
    let text_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("expected text retriever report");
    assert!(text_report.fallback_reasons.is_empty());
    assert!(output.search.hits[0]
        .fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output.search.hits[0]
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
}

#[test]
fn knowledge_retrieval_empty_reasons_include_search_fallback_reasons() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Vector-only fallback', content: 'search fallback should explain empty retrieval'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "".to_string(),
            query_embedding: Some(vec![1.0, 0.0]),
            mode: SearchMode::Vector,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert!(output.search.hits.is_empty());
    assert!(output.candidates.is_empty());
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));
}

#[test]
fn knowledge_retrieval_empty_reasons_include_missing_query_embedding() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Missing embedding query', content: 'vector retrieval should explain missing embedding'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "missing embedding query".to_string(),
            query_embedding: None,
            mode: SearchMode::Vector,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert!(output.search.hits.is_empty());
    assert!(output.candidates.is_empty());
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    let vector_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("expected vector retriever report");
    assert!(!vector_report.available);
    assert!(vector_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
}

#[test]
fn knowledge_retrieval_empty_reasons_include_empty_text_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Empty text query', content: 'text fallback should explain empty retrieval'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert!(output.search.hits.is_empty());
    assert!(output.candidates.is_empty());
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    let text_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("expected text retriever report");
    assert!(!text_report.available);
    assert!(text_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
}

#[test]
fn knowledge_retrieval_expands_graph_context_by_ordered_adjacency() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Ordered retrieval context'})")
        .unwrap();
    let lower_neighbor_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                (
                    "id".to_string(),
                    Value::String("lower-neighbor".to_string()),
                ),
                (
                    "name".to_string(),
                    Value::String("Lower neighbor".to_string()),
                ),
            ]),
        )
        .unwrap();
    let higher_neighbor_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                (
                    "id".to_string(),
                    Value::String("higher-neighbor".to_string()),
                ),
                (
                    "name".to_string(),
                    Value::String("Higher neighbor".to_string()),
                ),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            higher_neighbor_id,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            lower_neighbor_id,
            "RELATES_TO",
            BTreeMap::new(),
        )
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "ordered retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 1,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(
        output
            .diagnostics
            .graph_context_input_candidate_set
            .cardinality,
        1
    );
    assert_eq!(
        output.diagnostics.graph_context_candidate_set.cardinality,
        1
    );
    assert_eq!(
        output.diagnostics.graph_context_candidate_set.id_space,
        "canonical_graph_relationship_id"
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("lower-neighbor")
    );
    assert_eq!(
        output.graph_context_paths[0].relationship_type.as_str(),
        "RELATES_TO"
    );
    assert!(output
        .fanout_reasons
        .iter()
        .any(|reason| reason.contains("graph_context_limit 1")));
}

#[test]
fn knowledge_retrieval_reports_dense_graph_context_without_truncation() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Dense retrieval root'})")
        .unwrap();
    for index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
        let target = db
            .store
            .create_node(
                &mut db.catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String(format!("entity-{index}"))),
                    ("name".to_string(), Value::String(format!("Entity {index}"))),
                ]),
            )
            .unwrap();
        db.store
            .create_relationship(
                &mut db.catalog,
                NodeId(0),
                target,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
    }

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "dense retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(
        output.graph_context_paths.len(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
    assert_eq!(output.diagnostics.fanout_reason_count, 1);
    assert!(!output.diagnostics.graph_context_truncated);
    assert!(output
        .diagnostics
        .graph_context_truncation_reasons
        .is_empty());
    assert!(output.fanout_reasons[0]
        .contains("graph_context dense_adjacency MENTIONS outgoing node 0 degree"));
}

#[test]
fn knowledge_retrieval_applies_metadata_filters_to_search_and_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Filtered graph', content: 'metadata scoped retrieval', source_id: 'thread_1'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Filtered graph', content: 'metadata scoped retrieval', source_id: 'thread_2'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "metadata scoped retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_document_count, 2);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.search_total_hits, 1);
    assert_eq!(
        output.diagnostics.search_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(output.diagnostics.search_candidate_set.cardinality, 1);
    assert_eq!(
        output.diagnostics.search_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        output.diagnostics.search_candidate_set.metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.diagnostics.graph_seed_returned_count, 1);
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .filtered_out_count,
        1
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .cardinality,
        1
    );
    assert_eq!(output.diagnostics.graph_seed_candidate_set.cardinality, 1);
    assert_eq!(output.diagnostics.graph_context_path_count, 0);
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.candidate_count, 1);
    assert!(output.diagnostics.empty_reasons.is_empty());
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.search.hits[0].source_id.as_deref(), Some("thread_1"));
    let text_report = output
        .search
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text retriever report");
    assert_eq!(text_report.candidate_count, 1);
    assert_eq!(output.evidence.len(), 1);
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.candidates[0].id, "memory:mem_1");
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert_eq!(graph_seed_report.candidate_count, 1);
    assert_eq!(graph_seed_report.input_candidate_set.filtered_out_count, 1);
    assert_eq!(
        graph_seed_report.input_candidate_set.metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
    assert_eq!(graph_seed_report.top_candidates[0].id, "Memory:mem_1");
}

#[test]
fn knowledge_retrieval_kind_filter_accepts_canonical_labels() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Filtered graph', content: 'kind scoped retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Filtered graph', summary: 'kind scoped retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "kind scoped retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("kind".to_string(), "Memory".to_string())]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.search.hits[0].kind.as_deref(), Some("memory"));
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
}

#[test]
fn knowledge_retrieval_source_filter_uses_projection_fallbacks() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Thread scoped graph', content: 'source fallback retrieval', thread_id: 'thread_1'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Thread scoped graph', content: 'source fallback retrieval', thread_id: 'thread_2'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "source fallback retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.search.hits[0].source_id.as_deref(), Some("thread_1"));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
}

#[test]
fn knowledge_retrieval_source_filter_skips_empty_source_ids() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Thread scoped graph', content: 'empty source fallback retrieval', source_id: '', thread_id: 'thread_1'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Thread scoped graph', content: 'empty source fallback retrieval', source_id: '', thread_id: 'thread_2'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "empty source fallback retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.search.hits[0].source_id.as_deref(), Some("thread_1"));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
}

#[test]
fn knowledge_retrieval_binds_idless_search_hits_to_canonical_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Anonymous graph', content: 'idless projection retrieval'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'Skein'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "idless projection retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 4,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("0"));
    assert_eq!(output.evidence[0].canonical_node_id, Some(0));
    assert_eq!(output.evidence[0].graph_context_path_count, 1);
    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(
        output.graph_context_paths[0].source_external_id.as_deref(),
        Some("0")
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("entity_1")
    );
}

#[test]
fn knowledge_retrieval_external_filter_uses_projected_identity_for_idless_nodes() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {title: 'Anonymous graph', content: 'projected identity retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'named', title: 'Named graph', content: 'projected identity retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "projected identity retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("external_id".to_string(), "0".to_string())]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("0"));
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("0")
    );
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.candidates[0].id, "memory:0");
}

#[test]
fn knowledge_retrieval_external_filter_falls_back_for_empty_projected_ids() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: '', title: 'Empty id graph', content: 'empty projected identity retrieval'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'named', title: 'Named graph', content: 'empty projected identity retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "empty projected identity retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("external_id".to_string(), "0".to_string())]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].id, "memory:0");
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("0"));
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("0")
    );
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.candidates[0].id, "memory:0");
}

#[test]
fn knowledge_retrieval_normalizes_default_space_filters() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Scoped graph', content: 'default space retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Scoped graph', content: 'default space retrieval', space_id: ''})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_3', title: 'Scoped graph', content: 'default space retrieval', space_id: 'team'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "default space retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );
    let search_hit_ids = output
        .search
        .hits
        .iter()
        .map(|hit| hit.external_id.as_deref())
        .collect::<BTreeSet<_>>();
    let graph_seed_ids = output
        .graph_seeds
        .iter()
        .map(|seed| seed.entity.external_id.as_deref())
        .collect::<BTreeSet<_>>();

    assert_eq!(output.search.total_hits, 2);
    assert_eq!(output.diagnostics.search_filtered_document_count, 2);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 2);
    assert!(search_hit_ids.contains(&Some("mem_1")));
    assert!(search_hit_ids.contains(&Some("mem_2")));
    assert!(!search_hit_ids.contains(&Some("mem_3")));
    assert!(graph_seed_ids.contains(&Some("mem_1")));
    assert!(graph_seed_ids.contains(&Some("mem_2")));
    assert!(!graph_seed_ids.contains(&Some("mem_3")));
}

#[test]
fn knowledge_retrieval_diagnostics_explain_empty_metadata_scope() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Filtered graph', content: 'metadata scoped retrieval', source_id: 'thread_1'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "metadata scoped retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([(
                "source_id".to_string(),
                "missing_thread".to_string(),
            )]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 0);
    assert_eq!(output.graph_seeds.len(), 0);
    assert_eq!(output.candidates.len(), 0);
    assert_eq!(output.diagnostics.search_document_count, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 0);
    assert_eq!(output.diagnostics.search_total_hits, 0);
    assert_eq!(output.diagnostics.search_candidate_filtered_out_count, 1);
    assert_eq!(output.diagnostics.search_limit, 10);
    assert_eq!(output.diagnostics.rank_window, None);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 0);
    assert_eq!(output.diagnostics.graph_seed_returned_count, 0);
    assert_eq!(output.diagnostics.graph_seed_limit, 10);
    assert_eq!(output.diagnostics.graph_context_path_count, 0);
    assert_eq!(output.diagnostics.graph_context_node_count, 0);
    assert_eq!(output.diagnostics.graph_context_relationship_count, 0);
    assert_eq!(output.diagnostics.graph_context_limit, 0);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert_eq!(
        output.diagnostics.graph_context_fallback_reasons,
        vec!["graph context expansion disabled by limit 0".to_string()]
    );
    assert_eq!(
        output.diagnostics.graph_context_fallback_reason_codes,
        vec![KnowledgeFallbackReasonCode::GraphContextLimitZero]
    );
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.candidate_count, 0);
    assert_eq!(output.diagnostics.candidate_limit, None);
    assert!(output.diagnostics.warnings.is_empty());
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "metadata filters matched no search documents"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchMetadataFilterEmpty));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedNoCandidates));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever returned no candidates"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let search_disabled_by_limit = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "Graph candidate".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );
    assert_eq!(search_disabled_by_limit.search.total_hits, 1);
    assert!(search_disabled_by_limit.search.hits.is_empty());
    assert!(search_disabled_by_limit.diagnostics.search_truncated);
    assert!(search_disabled_by_limit.candidates.is_empty());
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "limit 0 returned from 1 matching hits"));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchLimitExcludedAllHits));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
    let disabled_graph_seed_report = search_disabled_by_limit
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert!(!disabled_graph_seed_report.available);
    assert_eq!(
        disabled_graph_seed_report.knowledge_fallback_reason_codes,
        vec![KnowledgeFallbackReasonCode::GraphSeedLimitZero]
    );
    assert!(disabled_graph_seed_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
}

#[test]
fn knowledge_retrieval_diagnostics_explain_disabled_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Graph candidate'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "Graph candidate".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert!(output.search.hits.is_empty());
    assert!(output.graph_seeds.is_empty());
    assert!(output.candidates.is_empty());
    assert_eq!(output.diagnostics.graph_seed_limit, 0);
    assert_eq!(output.diagnostics.candidate_count, 0);
    assert_eq!(output.diagnostics.candidate_total_count, 0);
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "search projection has no documents"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));
}

#[test]
fn knowledge_retrieval_diagnostics_report_projection_warnings() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Stale projection', content: 'projection warning retrieval'})")
            .unwrap();

    let path = unique_test_dir("knowledge_retrieval_projection_warnings");
    let mut search_index = SearchIndex::open(&path).unwrap();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    search_index
        .mark_full_reindex_needed("stale projection")
        .unwrap();
    search_index
        .mark_metadata_repair_needed("missing derived metadata")
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "projection warning".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert!(output.projection_freshness.full_reindex_needed);
    assert!(output.projection_freshness.metadata_repair_needed);
    assert!(!output.diagnostics.projection_stale);
    assert!(output.diagnostics.projection_full_reindex_needed);
    assert_eq!(
        output.diagnostics.projection_full_reindex_reasons,
        vec!["stale projection".to_string()]
    );
    assert!(output.diagnostics.projection_metadata_repair_needed);
    assert_eq!(
        output.diagnostics.projection_metadata_repair_reasons,
        vec!["missing derived metadata".to_string()]
    );
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection requires full reindex"));
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection full reindex reason: stale projection"));
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection metadata repair is needed"));
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning
            == "search projection metadata repair reason: missing derived metadata"));
}

#[test]
fn knowledge_retrieval_diagnostics_expose_stale_projection_flag() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Stale projection', content: 'projection warning retrieval'})",
    )
    .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'mem_2', title: 'New graph row', content: 'newer than projection'})",
    )
    .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "projection warning".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(
        output.diagnostics.projection_source_graph_commit_epoch,
        Some(1)
    );
    assert_eq!(output.diagnostics.projection_commit_lag, 1);
    assert!(output.diagnostics.projection_stale);
    assert!(!output.diagnostics.projection_full_reindex_needed);
    assert!(!output.diagnostics.projection_metadata_repair_needed);
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection is older than graph snapshot"));
}

#[test]
fn knowledge_retrieval_diagnostics_report_stale_projection_epoch() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Fresh projection', content: 'projection epoch retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Newer graph', content: 'new graph data'})")
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "projection epoch".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(
        output.projection_freshness.source_graph_commit_epoch,
        Some(1)
    );
    assert_eq!(output.diagnostics.graph_commit_epoch, 2);
    assert_eq!(
        output.diagnostics.projection_source_graph_commit_epoch,
        Some(1)
    );
    assert_eq!(output.diagnostics.projection_commit_lag, 1);
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection is older than graph snapshot"));
}

#[test]
fn retrieves_bounded_multi_hop_knowledge_context() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root traversal', content: 'Two hop graph context'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})")
            .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(1),
            leaf,
            "LINKS",
            BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        )
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "root traversal".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 4,
            graph_context_max_hops: 2,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.graph_context_paths.len(), 2);
    assert!(output.fanout_reasons.is_empty());
    assert!(output.graph_context_paths.iter().any(|path| path.hop == 1
        && path.source_external_id.as_deref() == Some("root")
        && path.target_external_id.as_deref() == Some("mid")));
    assert!(output.graph_context_paths.iter().any(|path| path.hop == 2
        && path.source_external_id.as_deref() == Some("mid")
        && path.target_external_id.as_deref() == Some("leaf")
        && path.relationship_properties.get("weight") == Some(&Value::Int(2))));
}

#[test]
fn knowledge_retrieval_reports_graph_context_disabled_by_max_hops() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root traversal', content: 'Zero hop graph context'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "root traversal".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 4,
            graph_context_max_hops: 0,
        },
    );

    assert_eq!(output.search.total_hits, 1);
    assert!(output.graph_context_paths.is_empty());
    assert_eq!(output.diagnostics.graph_context_path_count, 0);
    assert_eq!(output.diagnostics.graph_context_node_count, 0);
    assert_eq!(output.diagnostics.graph_context_relationship_count, 0);
    assert_eq!(
        output.diagnostics.graph_context_fallback_reasons,
        vec!["graph context expansion disabled by max_hops 0".to_string()]
    );
    assert_eq!(
        output.diagnostics.graph_context_fallback_reason_codes,
        vec![KnowledgeFallbackReasonCode::GraphContextMaxHopsZero]
    );
    assert!(output.fanout_reasons.is_empty());
    assert!(!output.diagnostics.graph_context_truncated);
}

#[test]
fn knowledge_retrieval_applies_rank_window_to_hybrid_search() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'top_vector', title: 'Vector seed', content: 'orthogonal text'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'top_text', title: 'Graph seed', content: 'graph graph retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'second_text', title: 'Fallback', content: 'graph context'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:top_vector".to_string(),
            title: "Vector seed".to_string(),
            content: "orthogonal text".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "top_vector".to_string()),
            ]),
        })
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:top_text".to_string(),
            title: "Graph seed".to_string(),
            content: "graph graph retrieval".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "top_text".to_string()),
            ]),
        })
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:second_text".to_string(),
            title: "Fallback".to_string(),
            content: "graph context".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "second_text".to_string()),
            ]),
        })
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "graph".to_string(),
            query_embedding: Some(vec![1.0, 0.0]),
            mode: SearchMode::Hybrid,
            limit: 10,
            rank_window: Some(1),
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.rank_window, Some(1));
    assert_eq!(output.diagnostics.search_limit, 10);
    assert_eq!(output.diagnostics.rank_window, Some(1));
    assert_eq!(output.diagnostics.graph_seed_limit, 0);
    assert_eq!(output.diagnostics.graph_context_limit, 0);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert_eq!(output.evidence.len(), output.search.hits.len());
    assert!(output
        .evidence
        .iter()
        .all(|evidence| evidence.rrf_score == evidence.score));
    let vector_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("vector knowledge retriever report");
    assert_eq!(
        vector_retriever.input_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(vector_retriever.limit, Some(10));
    assert_eq!(vector_retriever.rank_window, Some(1));
    assert_eq!(vector_retriever.fusion_weight, Some(1.0));
    let text_report = output
        .search
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text retriever report");
    assert_eq!(text_report.candidate_count, 2);
    assert_eq!(text_report.top_candidates.len(), 1);
    let text_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text knowledge retriever report");
    assert_eq!(text_retriever.limit, Some(10));
    assert_eq!(text_retriever.rank_window, Some(1));
    assert_eq!(text_retriever.fusion_weight, Some(1.0));
    assert_eq!(text_retriever.top_candidates[0].canonical_node_id, Some(1));
    assert!(text_retriever.truncated);
    assert_eq!(
        text_retriever.truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::RankWindowExceeded]
    );
    assert!(text_retriever
        .truncation_reasons
        .iter()
        .any(|reason| reason.contains("rank_window 1")));
    let second_text = output
        .search
        .hits
        .iter()
        .find(|hit| hit.id == "memory:second_text");
    assert!(second_text.is_none_or(|hit| hit.text_rank.is_none()));
}

#[test]
fn knowledge_retrieval_applies_search_fusion_weights() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'vector_top', title: 'Vector seed', content: 'semantic evidence'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'text_top', title: 'Graph retrieval', content: 'graph retrieval graph retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:vector_top".to_string(),
            title: "Vector seed".to_string(),
            content: "semantic evidence".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "vector_top".to_string()),
            ]),
        })
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:text_top".to_string(),
            title: "Graph retrieval".to_string(),
            content: "graph retrieval graph retrieval".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "text_top".to_string()),
            ]),
        })
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "graph retrieval".to_string(),
            query_embedding: Some(vec![1.0, 0.0]),
            mode: SearchMode::Hybrid,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights {
                vector_weight: 1.0,
                text_weight: 3.0,
            },
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(
        output.search.fusion_weights,
        SearchFusionWeights {
            vector_weight: 1.0,
            text_weight: 3.0
        }
    );
    assert_eq!(
        output.diagnostics.search_fusion_weights,
        SearchFusionWeights {
            vector_weight: 1.0,
            text_weight: 3.0
        }
    );
    let vector_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("vector knowledge retriever report");
    assert_eq!(vector_retriever.fusion_weight, Some(1.0));
    let text_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text knowledge retriever report");
    assert_eq!(text_retriever.fusion_weight, Some(3.0));
    assert_eq!(output.search.hits[0].id, "memory:text_top");
    assert_eq!(output.evidence[0].hit_id, "memory:text_top");
    assert!(output.evidence[0].text_rrf_score > 0.0);
    assert_eq!(output.evidence[0].vector_rrf_score, 0.0);
    assert_eq!(output.evidence[0].score, output.evidence[0].rrf_score);
}

#[test]
fn knowledge_retrieval_returns_graph_seeds_without_search_hits() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'mem_graph', title: 'Graph note'})-[:MENTIONS]->(:Entity {id: 'graph', name: 'Graph database'})",
        )
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_other', title: 'Graph companion'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "Graph".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 5,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 1,
            graph_context_limit: 2,
            graph_context_max_hops: 1,
        },
    );

    assert!(output.search.hits.is_empty());
    assert!(output.evidence.is_empty());
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(
        output.candidates[0].source,
        KnowledgeCandidateSource::GraphSeed
    );
    assert_eq!(output.candidates[0].source_rank, 1);
    assert_eq!(output.candidates[0].id, "Entity:graph");
    assert_eq!(output.candidates[0].canonical_node_id, Some(1));
    assert_eq!(
        output.candidates[0].entity.as_ref().unwrap(),
        &output.graph_seeds[0].entity
    );
    assert_eq!(output.candidates[0].graph_context_path_count, 1);
    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert_eq!(graph_seed_report.candidate_count, 3);
    assert_eq!(graph_seed_report.top_candidates.len(), 1);
    assert_eq!(graph_seed_report.top_candidates[0].id, "Entity:graph");
    assert_eq!(
        graph_seed_report.top_candidates[0].canonical_node_id,
        Some(1)
    );
    assert!(graph_seed_report.top_candidates[0].matched_spans.is_empty());
    assert_eq!(
        graph_seed_report.top_candidates[0].graph_context_path_count,
        1
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].projection_freshness,
        None
    );
    assert_eq!(graph_seed_report.top_candidates[0].rank, 1);
    assert_eq!(graph_seed_report.limit, Some(1));
    assert_eq!(graph_seed_report.rank_window, None);
    assert_eq!(graph_seed_report.fusion_weight, None);
    assert!(graph_seed_report.truncated);
    assert_eq!(
        graph_seed_report.truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::GraphSeedLimitExceeded]
    );
    assert!(output.diagnostics.graph_seed_truncated);
    assert_eq!(
        output.diagnostics.graph_seed_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::GraphSeedLimitExceeded]
    );
    assert_eq!(
        output.diagnostics.graph_seed_truncation_reasons,
        vec!["graph_seed limit 1 returned from 3 candidates".to_string()]
    );
    assert!(output.diagnostics.empty_reasons.is_empty());
    assert!(graph_seed_report
        .truncation_reasons
        .iter()
        .any(|reason| reason.contains("graph_seed limit 1")));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("graph")
    );
    assert!(output.graph_seeds[0]
        .matched_properties
        .contains(&"id".to_string()));
    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(
        output.graph_context_paths[0].seed_hit_id.as_str(),
        "Entity:graph"
    );
    assert_eq!(
        output.graph_context_paths[0].direction,
        KnowledgeGraphPathDirection::Incoming
    );
    assert_eq!(
        output.graph_context_paths[0].relationship_type.as_str(),
        "MENTIONS"
    );
    assert_eq!(
        output.graph_context_paths[0].source_external_id.as_deref(),
        Some("mem_graph")
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("graph")
    );
    assert_eq!(output.fanout_reasons.len(), 1);
    assert!(output.fanout_reasons[0].contains("knowledge_graph_seed_limit 1"));
    assert_eq!(
        output.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::GraphSeedLimitReached]
    );
    assert_eq!(output.fanout_reason_details[0].limit, Some(1));
    assert_eq!(output.fanout_reason_details[0].total, Some(3));
    assert_eq!(
        output.diagnostics.fanout_reason_codes,
        output.fanout_reason_codes
    );
    assert_eq!(output.diagnostics.fanout_reasons, output.fanout_reasons);
}

#[test]
fn knowledge_retrieval_keeps_context_per_retriever_seed() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'shared', title: 'Shared graph seed', content: 'shared graph seed'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'Entity'})",
        )
            .unwrap();
    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "shared graph seed".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 1,
            graph_context_limit: 4,
            graph_context_max_hops: 1,
        },
    );

    let search_evidence = output
        .evidence
        .iter()
        .find(|evidence| evidence.canonical_node_id == Some(0))
        .expect("search evidence for shared memory");
    assert_eq!(search_evidence.graph_context_path_count, 1);

    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed report");
    let graph_seed_candidate = graph_seed_report
        .top_candidates
        .iter()
        .find(|candidate| candidate.canonical_node_id == Some(0))
        .expect("graph seed candidate for shared memory");
    assert_eq!(graph_seed_candidate.graph_context_path_count, 1);
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == search_evidence.hit_id));
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == "Memory:shared"));
}

#[test]
fn knowledge_retrieval_applies_candidate_limit_after_merge() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'graph_1', title: 'Graph candidate one'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'graph_2', title: 'Graph candidate two'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'graph_3', title: 'Graph candidate three'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "Graph candidate".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 5,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: Some(1),
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 3,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.graph_seeds.len(), 3);
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.diagnostics.search_limit, 5);
    assert_eq!(output.diagnostics.candidate_limit, Some(1));
    assert_eq!(output.diagnostics.candidate_count, 1);
    assert_eq!(output.diagnostics.candidate_total_count, 3);
    assert!(output.diagnostics.candidate_truncated);
    assert_eq!(
        output.diagnostics.candidate_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::CandidateLimitExceeded]
    );
    assert!(output.diagnostics.empty_reason_codes.is_empty());
    assert_eq!(
        output.diagnostics.candidate_truncation_reasons,
        vec!["knowledge_candidate_limit 1 returned from 3 merged candidates".to_string()]
    );
    assert_eq!(output.diagnostics.graph_seed_limit, 3);
    assert_eq!(output.diagnostics.graph_context_limit, 0);
    assert_eq!(
        output.candidates[0].source,
        KnowledgeCandidateSource::GraphSeed
    );
    assert_eq!(
        output.candidates[0].merged_sources,
        vec![KnowledgeCandidateSource::GraphSeed]
    );
    assert_eq!(output.fanout_reasons.len(), 1);
    assert!(output.fanout_reasons[0].contains("knowledge_candidate_limit 1"));
    assert_eq!(
        output.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::CandidateLimitReached]
    );
    assert_eq!(output.fanout_reason_details[0].limit, Some(1));
    assert_eq!(output.fanout_reason_details[0].total, Some(3));
    assert_eq!(
        output.diagnostics.fanout_reason_codes,
        output.fanout_reason_codes
    );
    assert_eq!(output.diagnostics.fanout_reasons, output.fanout_reasons);

    let empty_by_limit = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "Graph candidate".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 5,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: Some(0),
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 3,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );
    assert!(empty_by_limit.candidates.is_empty());
    assert_eq!(empty_by_limit.diagnostics.candidate_limit, Some(0));
    assert_eq!(empty_by_limit.diagnostics.candidate_count, 0);
    assert_eq!(empty_by_limit.diagnostics.candidate_total_count, 3);
    assert!(empty_by_limit.diagnostics.candidate_truncated);
    assert_eq!(
        empty_by_limit.diagnostics.candidate_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::CandidateLimitExceeded]
    );
    assert!(empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "knowledge_candidate_limit 0 returned from 3 merged candidates"));
    assert!(empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::CandidateLimitExcludedAllCandidates));
    assert!(empty_by_limit
        .diagnostics
        .empty_reason_codes
        .iter()
        .map(|code| code.as_str())
        .any(|code| code == "candidate_limit_excluded_all_candidates"));
    assert!(empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(!empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedNoCandidates));
    assert!(empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let search_empty_by_limit = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "Graph candidate".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 5,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: Some(0),
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );
    assert!(search_empty_by_limit.search.total_hits > 0);
    assert_eq!(search_empty_by_limit.diagnostics.candidate_count, 0);
    assert!(search_empty_by_limit.diagnostics.candidate_total_count > 0);
    assert!(search_empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason.starts_with("knowledge_candidate_limit 0")));
    assert!(search_empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::CandidateLimitExcludedAllCandidates));
    assert!(search_empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(!search_empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(!search_empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
}

#[test]
fn knowledge_retrieval_applies_weighted_candidate_scoring() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'weighted', title: 'Weighted graph candidate', content: 'graph graph graph'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "weighted graph".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::WeightedSum {
                search_weight: 2.0,
                graph_seed_weight: 0.5,
            },
            graph_seed_limit: 1,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.candidates.len(), 1);
    let candidate = &output.candidates[0];
    assert_eq!(
        candidate.merged_sources,
        vec![
            KnowledgeCandidateSource::SearchHit,
            KnowledgeCandidateSource::GraphSeed
        ]
    );
    let search_score = candidate.score_breakdown.search_score.unwrap();
    let graph_seed_score = candidate.score_breakdown.graph_seed_score.unwrap();
    assert_eq!(candidate.score_breakdown.combined_score, candidate.score);
    assert_eq!(candidate.score, search_score * 2.0 + graph_seed_score * 0.5);
}

#[test]
fn retrieves_knowledge_entity_without_search_projection() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Skein', kind: 'database', score: 7})")
        .unwrap();

    let output = db.knowledge_entity(&KnowledgeEntityRequest {
        label: "Entity".to_string(),
        external_id: "entity_1".to_string(),
    });

    let entity = output.entity.expect("expected entity");
    assert_eq!(output.graph_commit_epoch, 1);
    assert_eq!(entity.node_id, 0);
    assert_eq!(entity.labels, vec!["Entity".to_string()]);
    assert_eq!(entity.external_id.as_deref(), Some("entity_1"));
    assert_eq!(
        entity.properties.get("name"),
        Some(&Value::String("Skein".to_string()))
    );
    assert_eq!(entity.properties.get("score"), Some(&Value::Int(7)));
}

#[test]
fn scoped_knowledge_entity_filters_by_metadata() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Scoped', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();

    let scoped = db.knowledge_scoped_entity(&KnowledgeScopedEntityRequest {
        entity: KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        },
        metadata_filters: BTreeMap::from([
            ("source_id".to_string(), "thread_1".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    });

    let entity = scoped.entity.expect("expected scoped entity");
    assert_eq!(scoped.graph_commit_epoch, 1);
    assert_eq!(entity.external_id.as_deref(), Some("memory_1"));

    let filtered = db.knowledge_scoped_entity(&KnowledgeScopedEntityRequest {
        entity: KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        },
        metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
    });

    assert_eq!(filtered.graph_commit_epoch, 1);
    assert!(filtered.entity.is_none());
}

#[test]
fn retrieves_knowledge_entity_batch_in_request_order() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second'})")
        .unwrap();

    let output = db.knowledge_entity_batch(&KnowledgeEntityBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "missing".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
        ],
    });

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(output.entities.len(), 3);
    assert_eq!(output.found_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(
        output.entities[0]
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("memory_2")
    );
    assert!(output.entities[1].is_none());
    assert_eq!(
        output.entities[2]
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("memory_1")
    );
}

#[test]
fn scoped_knowledge_entity_batch_reports_filtered_and_missing_items() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2', space_id: 'default'})")
        .unwrap();

    let output = db.knowledge_scoped_entity_batch(&KnowledgeScopedEntityBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "missing".to_string(),
            },
        ],
        metadata_filters: BTreeMap::from([
            ("source_id".to_string(), "thread_1".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    });

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(output.entities.len(), 3);
    assert_eq!(output.found_count, 1);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(
        output.entities[0]
            .as_ref()
            .and_then(|entity| entity.external_id.as_deref()),
        Some("memory_1")
    );
    assert!(output.entities[1].is_none());
    assert!(output.entities[2].is_none());
}

#[test]
fn creates_knowledge_entity_through_typed_api() {
    let mut db = Database::new();

    let output = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([
                ("title".to_string(), Value::String("First".to_string())),
                (
                    "source_id".to_string(),
                    Value::String("thread_1".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 0);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(output.created);
    assert!(!output.already_exists);
    assert_eq!(output.created_node_count, 1);
    let entity = db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .entity
        .expect("expected created memory");
    assert_eq!(
        entity.properties.get("title"),
        Some(&Value::String("First".to_string()))
    );
    assert_eq!(
        entity.properties.get("id"),
        Some(&Value::String("memory_1".to_string()))
    );
}

#[test]
fn create_knowledge_entity_reports_existing_identity_without_writing() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let output = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([("title".to_string(), Value::String("New".to_string()))]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.created);
    assert!(output.already_exists);
    assert_eq!(output.created_node_count, 0);
    let entity = db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .entity
        .expect("expected existing memory");
    assert_eq!(
        entity.properties.get("title"),
        Some(&Value::String("Old".to_string()))
    );
}

#[test]
fn creates_knowledge_entity_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'existing', title: 'Existing'})")
        .unwrap();

    let output = db
        .create_knowledge_entity_batch(&KnowledgeEntityCreateBatchRequest {
            creates: vec![
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("First".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Ignored".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                    properties: BTreeMap::from([(
                        "name".to_string(),
                        Value::String("Skein".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.created_count, 2);
    assert_eq!(output.already_exists_count, 1);
    assert_eq!(output.created_node_count, 2);
    assert!(output.rows[0].created);
    assert!(output.rows[0].node_id.is_some());
    assert!(output.rows[1].already_exists);
    assert_eq!(output.rows[1].node_id, Some(0));
    assert!(output.rows[2].created);
    let created = db.knowledge_entity_batch(&KnowledgeEntityBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
        ],
    });
    assert_eq!(created.found_count, 2);
}

#[test]
fn create_knowledge_entity_batch_deduplicates_pending_identity() {
    let mut db = Database::new();

    let output = db
        .create_knowledge_entity_batch(&KnowledgeEntityCreateBatchRequest {
            creates: vec![
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("First".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Duplicate".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 0);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 1);
    assert!(output.rows[0].created);
    assert!(output.rows[1].already_exists);
    assert_eq!(output.rows[1].node_id, None);
    let entities = db.knowledge_entity_batch(&KnowledgeEntityBatchRequest {
        entities: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
    });
    assert_eq!(entities.found_count, 1);
}

#[test]
fn create_knowledge_entity_rejects_invalid_identifiers_and_id_mismatch() {
    let mut db = Database::new();
    let invalid_property = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([("bad-name".to_string(), Value::Int(1))]),
        })
        .unwrap_err();
    assert!(invalid_property.to_string().contains("property identifier"));
    let id_mismatch = db
        .create_knowledge_entity(&KnowledgeEntityCreateRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            properties: BTreeMap::from([(
                "id".to_string(),
                Value::String("different".to_string()),
            )]),
        })
        .unwrap_err();
    assert!(id_mismatch
        .to_string()
        .contains("does not match external id"));
    assert_eq!(db.store.commit_epoch(), 0);
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_create() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_create");
    {
        let _db = Database::open(&path).unwrap();
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
            .create_knowledge_entity(&KnowledgeEntityCreateRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
                properties: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_batch_create_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_entity_batch_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        let batch_count_before_create = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap_or_default()
            .matches("\tbatch\t")
            .count();
        db.create_knowledge_entity_batch(&KnowledgeEntityCreateBatchRequest {
            creates: vec![
                KnowledgeEntityCreateRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                    properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("First".to_string()),
                    )]),
                },
                KnowledgeEntityCreateRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                    properties: BTreeMap::from([(
                        "name".to_string(),
                        Value::String("Skein".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_create = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_create, batch_count_before_create + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_node"));
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_entity_batch(&KnowledgeEntityBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
            ],
        });
        assert_eq!(output.found_count, 2);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn upserts_knowledge_entity_through_typed_api() {
    let mut db = Database::new();

    let created = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            create_properties: BTreeMap::from([(
                "title".to_string(),
                Value::String("First".to_string()),
            )]),
            update_properties: BTreeMap::from([(
                "updated_at".to_string(),
                Value::String("ignored-on-create".to_string()),
            )]),
        })
        .unwrap();

    assert!(created.created);
    assert!(!created.updated);
    assert!(!created.already_exists);
    assert_eq!(created.node_id, Some(0));
    assert_eq!(created.created_node_count, 1);
    assert_eq!(created.updated_property_count, 0);
    let existing = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            create_properties: BTreeMap::from([(
                "title".to_string(),
                Value::String("Should Not Replace".to_string()),
            )]),
            update_properties: BTreeMap::from([
                ("id".to_string(), Value::String("memory_1".to_string())),
                (
                    "updated_at".to_string(),
                    Value::String("2026-07-19".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert!(!existing.created);
    assert!(existing.updated);
    assert!(existing.already_exists);
    assert!(!existing.non_writable);
    assert_eq!(existing.node_id, Some(0));
    assert_eq!(existing.updated_property_count, 1);
    let entity = db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .entity
        .expect("expected upserted memory");
    assert_eq!(
        entity.properties.get("title"),
        Some(&Value::String("First".to_string()))
    );
    assert_eq!(
        entity.properties.get("updated_at"),
        Some(&Value::String("2026-07-19".to_string()))
    );
}

#[test]
fn knowledge_entity_upsert_rejects_id_mismatch_before_writing() {
    let mut db = Database::new();

    let error = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
            create_properties: BTreeMap::new(),
            update_properties: BTreeMap::from([(
                "id".to_string(),
                Value::String("different".to_string()),
            )]),
        })
        .unwrap_err();

    assert!(error.to_string().contains("does not match external id"));
    assert_eq!(db.store.commit_epoch(), 0);
}

#[test]
fn knowledge_entity_upsert_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {name: 'Skein', description: 'old'})")
        .unwrap();

    let output = db
        .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
            label: "Entity".to_string(),
            external_id: "0".to_string(),
            create_properties: BTreeMap::from([(
                "description".to_string(),
                Value::String("create".to_string()),
            )]),
            update_properties: BTreeMap::from([(
                "description".to_string(),
                Value::String("new".to_string()),
            )]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(output.already_exists);
    assert!(output.non_writable);
    assert!(!output.updated);
    let entity = db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "0".to_string(),
        })
        .entity
        .expect("expected projected entity");
    assert_eq!(
        entity.properties.get("description"),
        Some(&Value::String("old".to_string()))
    );
}

#[test]
fn upserts_knowledge_entity_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'existing', title: 'Old'})")
        .unwrap();

    let output = db
        .upsert_knowledge_entity_batch(&KnowledgeEntityUpsertBatchRequest {
            upserts: vec![
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                    create_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Created".to_string()),
                    )]),
                    update_properties: BTreeMap::new(),
                },
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                    create_properties: BTreeMap::new(),
                    update_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Updated".to_string()),
                    )]),
                },
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                    create_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Duplicate".to_string()),
                    )]),
                    update_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Duplicate Update".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.updated_count, 1);
    assert_eq!(output.already_exists_count, 2);
    assert_eq!(output.created_node_count, 1);
    assert_eq!(output.updated_property_count, 1);
    assert!(output.rows[0].created);
    assert!(output.rows[0].node_id.is_some());
    assert!(output.rows[1].updated);
    assert_eq!(output.rows[1].node_id, Some(0));
    assert!(output.rows[2].already_exists);
    assert_eq!(output.rows[2].node_id, None);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "created".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "existing".to_string(),
            },
        ],
        property_names: vec!["title".to_string()],
    });
    assert_eq!(
        rows.rows[0].properties.get("title"),
        Some(&Some(Value::String("Created".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("title"),
        Some(&Some(Value::String("Updated".to_string())))
    );
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_upsert() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_upsert");
    {
        let _db = Database::open(&path).unwrap();
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
            .upsert_knowledge_entity(&KnowledgeEntityUpsertRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
                create_properties: BTreeMap::new(),
                update_properties: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_batch_upsert_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_entity_batch_upsert_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'existing', title: 'Old'})")
            .unwrap();
        let batch_count_before_upsert = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.upsert_knowledge_entity_batch(&KnowledgeEntityUpsertBatchRequest {
            upserts: vec![
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                    create_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Created".to_string()),
                    )]),
                    update_properties: BTreeMap::new(),
                },
                KnowledgeEntityUpsertRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                    create_properties: BTreeMap::new(),
                    update_properties: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Updated".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_upsert = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_upsert, batch_count_before_upsert + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_node"));
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "created".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "existing".to_string(),
                },
            ],
            property_names: vec!["title".to_string()],
        });
        assert_eq!(rows.found_count, 2);
        assert_eq!(
            rows.rows[0].properties.get("title"),
            Some(&Some(Value::String("Created".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("title"),
            Some(&Some(Value::String("Updated".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn retrieves_knowledge_property_batch_without_hydrating_full_entities() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', metadata: 'large metadata'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2'})")
        .unwrap();

    let output = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "missing".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
        ],
        property_names: vec![
            "title".to_string(),
            "source_id".to_string(),
            "title".to_string(),
            "metadata".to_string(),
        ],
    });

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(
        output.property_names,
        vec![
            "title".to_string(),
            "source_id".to_string(),
            "metadata".to_string()
        ]
    );
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.found_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(
        output.rows[0].properties.get("title"),
        Some(&Some(Value::String("Second".to_string())))
    );
    assert_eq!(output.rows[0].properties.get("metadata"), Some(&None));
    assert!(output.rows[1].node_id.is_none());
    assert_eq!(output.rows[1].properties.get("title"), Some(&None));
    assert_eq!(
        output.rows[2].properties.get("metadata"),
        Some(&Some(Value::String("large metadata".to_string())))
    );
}

#[test]
fn scoped_knowledge_property_batch_reports_filtered_rows() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2', space_id: 'default'})")
        .unwrap();

    let output = db.knowledge_scoped_property_batch(&KnowledgeScopedPropertyBatchRequest {
        projection: KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["title".to_string(), "source_id".to_string()],
        },
        metadata_filters: BTreeMap::from([
            ("source_id".to_string(), "thread_1".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    });

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(output.found_count, 1);
    assert_eq!(output.missing_count, 0);
    assert_eq!(output.filtered_out_count, 1);
    assert!(!output.rows[0].filtered_out);
    assert_eq!(
        output.rows[0].properties.get("title"),
        Some(&Some(Value::String("First".to_string())))
    );
    assert!(output.rows[1].filtered_out);
    assert_eq!(output.rows[1].properties.get("title"), Some(&None));
}

#[test]
fn updates_knowledge_properties_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old', review_status: 'pending'})")
        .unwrap();

    let output = db
        .update_knowledge_properties(&KnowledgePropertyUpdateRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            assignments: BTreeMap::from([
                ("title".to_string(), Value::String("New".to_string())),
                (
                    "review_status".to_string(),
                    Value::String("approved".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.node_id, Some(0));
    assert!(output.matched);
    assert!(!output.filtered_out);
    assert_eq!(output.updated_property_count, 2);
    let row = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        property_names: vec!["title".to_string(), "review_status".to_string()],
    });
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("New".to_string())))
    );
    assert_eq!(
        row.rows[0].properties.get("review_status"),
        Some(&Some(Value::String("approved".to_string())))
    );
}

#[test]
fn scoped_knowledge_property_update_does_not_write_filtered_seed() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Old', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();

    let output = db
        .update_scoped_knowledge_properties(&KnowledgeScopedPropertyUpdateRequest {
            update: KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::from([(
                    "title".to_string(),
                    Value::String("New".to_string()),
                )]),
            },
            metadata_filters: BTreeMap::from([
                ("source_id".to_string(), "thread_2".to_string()),
                ("space_id".to_string(), "default".to_string()),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.matched);
    assert!(output.filtered_out);
    assert_eq!(output.updated_property_count, 0);
    let row = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        property_names: vec!["title".to_string()],
    });
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("Old".to_string())))
    );
}

#[test]
fn knowledge_property_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let error = db
        .update_knowledge_properties(&KnowledgePropertyUpdateRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            assignments: BTreeMap::from([(
                "bad-name".to_string(),
                Value::String("New".to_string()),
            )]),
        })
        .unwrap_err();

    assert!(error.to_string().contains("property identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_property_update() {
    let path = unique_test_dir("read_only_typed_knowledge_property_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
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
            .update_knowledge_properties(&KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::from([(
                    "title".to_string(),
                    Value::String("New".to_string()),
                )]),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_property_update_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_property_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
        db.update_knowledge_properties(&KnowledgePropertyUpdateRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            assignments: BTreeMap::from([("title".to_string(), Value::String("New".to_string()))]),
        })
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            property_names: vec!["title".to_string()],
        });
        assert_eq!(
            output.rows[0].properties.get("title"),
            Some(&Some(Value::String("New".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_knowledge_properties_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1', review_status: 'pending'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2', review_status: 'pending'})")
        .unwrap();

    let output = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([
                        ("title".to_string(), Value::String("New 1".to_string())),
                        (
                            "review_status".to_string(),
                            Value::String("approved".to_string()),
                        ),
                    ]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 2".to_string()),
                    )]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Ignored".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_property_count, 3);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 1);
    assert_eq!(output.rows[2].node_id, None);

    let row = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        property_names: vec!["title".to_string(), "review_status".to_string()],
    });
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("New 1".to_string())))
    );
    assert_eq!(
        row.rows[0].properties.get("review_status"),
        Some(&Some(Value::String("approved".to_string())))
    );
    assert_eq!(
        row.rows[1].properties.get("title"),
        Some(&Some(Value::String("New 2".to_string())))
    );
}

#[test]
fn moves_thread_normalized_space_batch_by_identity_property() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'storage-1', thread_id: 'thread-1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-2', thread_id: 'thread-2', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-3', thread_id: 'thread-3', space_id: 'team'})")
        .unwrap();

    let output = db
        .move_knowledge_normalized_space_batch(&KnowledgeNormalizedSpaceMoveBatchRequest {
            label: "Thread".to_string(),
            identity_property: "thread_id".to_string(),
            external_ids: vec![
                "thread-1".to_string(),
                "thread-2".to_string(),
                "thread-3".to_string(),
                "thread-2".to_string(),
                "missing".to_string(),
            ],
            source_space_id: Some("default".to_string()),
            target_space_id: "team".to_string(),
            updated_at: Some(Value::String("2026-07-19".to_string())),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.matched_count, 4);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.source_mismatch_count, 1);
    assert_eq!(output.already_in_target_count, 0);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.moved_count, 2);
    assert_eq!(
        output.moved_external_ids,
        vec!["thread-1".to_string(), "thread-2".to_string()]
    );
    assert!(output.rows[0].moved);
    assert!(output.rows[1].moved);
    assert!(output.rows[2].source_mismatch);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db
        .query("MATCH (t:Thread) RETURN t.thread_id, t.space_id, t.updated_at ORDER BY t.thread_id")
        .unwrap();
    assert_eq!(
        rows.rows[0].get("t.space_id"),
        Some(&Value::String("team".to_string()))
    );
    assert_eq!(
        rows.rows[0].get("t.updated_at"),
        Some(&Value::String("2026-07-19".to_string()))
    );
    assert_eq!(
        rows.rows[1].get("t.space_id"),
        Some(&Value::String("team".to_string()))
    );
    assert_eq!(
        rows.rows[2].get("t.space_id"),
        Some(&Value::String("team".to_string()))
    );
}

#[test]
fn knowledge_normalized_space_move_batch_rejects_invalid_identifiers() {
    let mut db = Database::new();
    let error = db
        .move_knowledge_normalized_space_batch(&KnowledgeNormalizedSpaceMoveBatchRequest {
            label: "Thread".to_string(),
            identity_property: "thread-id".to_string(),
            external_ids: vec!["thread-1".to_string()],
            source_space_id: None,
            target_space_id: "team".to_string(),
            updated_at: None,
        })
        .unwrap_err();

    assert!(error.to_string().contains("identity property identifier"));
    assert_eq!(db.store.commit_epoch(), 0);
}

#[test]
fn typed_knowledge_normalized_space_move_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_normalized_space_move_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', space_id: ''})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', space_id: 'default'})")
            .unwrap();
        let batch_count_before_move = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.move_knowledge_normalized_space_batch(&KnowledgeNormalizedSpaceMoveBatchRequest {
            label: "Memory".to_string(),
            identity_property: "id".to_string(),
            external_ids: vec!["memory_1".to_string(), "memory_2".to_string()],
            source_space_id: Some("default".to_string()),
            target_space_id: "archive".to_string(),
            updated_at: None,
        })
        .unwrap();
        let batch_count_after_move = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_move, batch_count_before_move + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["space_id".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("space_id"),
            Some(&Some(Value::String("archive".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("space_id"),
            Some(&Some(Value::String("archive".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn touches_memory_access_batch_with_incremental_counters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', access_count: 2, clicks: 1, total_dwell_time_ms: 100, last_accessed_at: 'old', last_clicked_at: 'old'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();

    let output = db
        .touch_knowledge_memory_access_batch(&KnowledgeMemoryAccessBatchRequest {
            touches: vec![
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_1".to_string(),
                    accessed_at: Value::String("2026-07-19T10:00:00Z".to_string()),
                    click_dwell_time_ms: None,
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_1".to_string(),
                    accessed_at: Value::String("2026-07-19T10:01:00Z".to_string()),
                    click_dwell_time_ms: Some(50),
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_2".to_string(),
                    accessed_at: Value::String("2026-07-19T10:02:00Z".to_string()),
                    click_dwell_time_ms: Some(25),
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "missing".to_string(),
                    accessed_at: Value::String("2026-07-19T10:03:00Z".to_string()),
                    click_dwell_time_ms: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.touched_count, 3);
    assert_eq!(output.click_touch_count, 2);
    assert!(output.rows[0].touched);
    assert!(!output.rows[0].clicked);
    assert!(output.rows[1].clicked);
    assert!(output.rows[2].clicked);
    assert!(!output.rows[3].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        property_names: vec![
            "access_count".to_string(),
            "clicks".to_string(),
            "total_dwell_time_ms".to_string(),
            "last_accessed_at".to_string(),
            "last_clicked_at".to_string(),
        ],
    });
    assert_eq!(
        rows.rows[0].properties.get("access_count"),
        Some(&Some(Value::Int(4)))
    );
    assert_eq!(
        rows.rows[0].properties.get("clicks"),
        Some(&Some(Value::Int(2)))
    );
    assert_eq!(
        rows.rows[0].properties.get("total_dwell_time_ms"),
        Some(&Some(Value::Int(150)))
    );
    assert_eq!(
        rows.rows[0].properties.get("last_accessed_at"),
        Some(&Some(Value::String("2026-07-19T10:01:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("last_clicked_at"),
        Some(&Some(Value::String("2026-07-19T10:01:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("access_count"),
        Some(&Some(Value::Int(1)))
    );
    assert_eq!(
        rows.rows[1].properties.get("clicks"),
        Some(&Some(Value::Int(1)))
    );
    assert_eq!(
        rows.rows[1].properties.get("total_dwell_time_ms"),
        Some(&Some(Value::Int(25)))
    );
}

#[test]
fn knowledge_memory_access_batch_rejects_negative_dwell_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .touch_knowledge_memory_access_batch(&KnowledgeMemoryAccessBatchRequest {
            touches: vec![KnowledgeMemoryAccessTouch {
                memory_id: "memory_1".to_string(),
                accessed_at: Value::String("2026-07-19T10:00:00Z".to_string()),
                click_dwell_time_ms: Some(-1),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative dwell time"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_knowledge_memory_access_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_memory_access_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', access_count: 4, clicks: 2, total_dwell_time_ms: 10})")
            .unwrap();
        let batch_count_before_touch = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.touch_knowledge_memory_access_batch(&KnowledgeMemoryAccessBatchRequest {
            touches: vec![
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_1".to_string(),
                    accessed_at: Value::String("2026-07-19T11:00:00Z".to_string()),
                    click_dwell_time_ms: None,
                },
                KnowledgeMemoryAccessTouch {
                    memory_id: "memory_2".to_string(),
                    accessed_at: Value::String("2026-07-19T11:01:00Z".to_string()),
                    click_dwell_time_ms: Some(90),
                },
            ],
        })
        .unwrap();
        let batch_count_after_touch = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_touch, batch_count_before_touch + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec![
                "access_count".to_string(),
                "clicks".to_string(),
                "total_dwell_time_ms".to_string(),
            ],
        });
        assert_eq!(
            rows.rows[0].properties.get("access_count"),
            Some(&Some(Value::Int(1)))
        );
        assert_eq!(
            rows.rows[1].properties.get("access_count"),
            Some(&Some(Value::Int(5)))
        );
        assert_eq!(
            rows.rows[1].properties.get("clicks"),
            Some(&Some(Value::Int(3)))
        );
        assert_eq!(
            rows.rows[1].properties.get("total_dwell_time_ms"),
            Some(&Some(Value::Int(100)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn adjusts_source_memory_count_batch_with_floor_decrements() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', memory_count: 1})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_2'})").unwrap();
    db.query("CREATE (:Source {id: 'bad', memory_count: 'many'})")
        .unwrap();

    let output = db
        .adjust_knowledge_source_memory_count_batch(&KnowledgeSourceMemoryCountBatchRequest {
            adjustments: vec![
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: 1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_2".to_string(),
                    delta: -1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "missing".to_string(),
                    delta: 1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "bad".to_string(),
                    delta: 1,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 7);
    assert_eq!(output.matched_count, 5);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.invalid_current_count_count, 1);
    assert_eq!(output.adjusted_count, 5);
    assert_eq!(output.rows[0].old_count, Some(1));
    assert_eq!(output.rows[0].new_count, Some(2));
    assert_eq!(output.rows[1].old_count, Some(2));
    assert_eq!(output.rows[1].new_count, Some(1));
    assert_eq!(output.rows[2].old_count, Some(1));
    assert_eq!(output.rows[2].new_count, Some(0));
    assert_eq!(output.rows[3].old_count, Some(0));
    assert_eq!(output.rows[3].new_count, Some(0));
    assert_eq!(output.rows[4].old_count, Some(0));
    assert_eq!(output.rows[4].new_count, Some(0));
    assert!(!output.rows[5].matched);
    assert!(output.rows[6].invalid_current_count);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: "source_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: "source_2".to_string(),
            },
        ],
        property_names: vec!["memory_count".to_string()],
    });
    assert_eq!(
        rows.rows[0].properties.get("memory_count"),
        Some(&Some(Value::Int(0)))
    );
    assert_eq!(
        rows.rows[1].properties.get("memory_count"),
        Some(&Some(Value::Int(0)))
    );
}

#[test]
fn source_memory_count_batch_rejects_zero_delta_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', memory_count: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .adjust_knowledge_source_memory_count_batch(&KnowledgeSourceMemoryCountBatchRequest {
            adjustments: vec![KnowledgeSourceMemoryCountAdjustment {
                source_id: "source_1".to_string(),
                delta: 0,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-zero delta"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_memory_count_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_memory_count_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1', memory_count: 0})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2', memory_count: 2})")
            .unwrap();
        let batch_count_before_adjust = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.adjust_knowledge_source_memory_count_batch(&KnowledgeSourceMemoryCountBatchRequest {
            adjustments: vec![
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_1".to_string(),
                    delta: 1,
                },
                KnowledgeSourceMemoryCountAdjustment {
                    source_id: "source_2".to_string(),
                    delta: -1,
                },
            ],
        })
        .unwrap();
        let batch_count_after_adjust = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_adjust, batch_count_before_adjust + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_2".to_string(),
                },
            ],
            property_names: vec!["memory_count".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("memory_count"),
            Some(&Some(Value::Int(1)))
        );
        assert_eq!(
            rows.rows[1].properties.get("memory_count"),
            Some(&Some(Value::Int(1)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_source_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'extracted', updated_at: 1})")
        .unwrap();
    db.query(
        "CREATE (:Source {id: 'source_2', lifecycle_state: 'parsed', chunk_count: 0, updated_at: 1})",
    )
    .unwrap();
    db.query("CREATE (:Source {id: 'source_3', lifecycle_state: 'parsed', updated_at: 1})")
        .unwrap();

    let output = db
        .update_knowledge_source_lifecycle_batch(&KnowledgeSourceLifecycleBatchRequest {
            updates: vec![
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_1".to_string(),
                    current_lifecycle_state: Some("extracted".to_string()),
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_2".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: Some(8),
                    updated_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_3".to_string(),
                    current_lifecycle_state: Some("extracted".to_string()),
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:02:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_2".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "archived".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:03:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "missing".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:04:00Z".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 5);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 3);
    assert!(output.rows[2].filtered_out);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: "source_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: "source_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: "source_3".to_string(),
            },
        ],
        property_names: vec![
            "lifecycle_state".to_string(),
            "chunk_count".to_string(),
            "updated_at".to_string(),
        ],
    });
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("indexed".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::String("2026-07-19T12:00:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("lifecycle_state"),
        Some(&Some(Value::String("indexed".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("chunk_count"),
        Some(&Some(Value::Int(8)))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::String("2026-07-19T12:01:00Z".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("lifecycle_state"),
        Some(&Some(Value::String("parsed".to_string())))
    );
}

#[test]
fn source_lifecycle_batch_rejects_invalid_rows_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'parsed'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_source_lifecycle_batch(&KnowledgeSourceLifecycleBatchRequest {
            updates: vec![KnowledgeSourceLifecycleUpdate {
                source_id: "source_1".to_string(),
                current_lifecycle_state: None,
                lifecycle_state: "indexed".to_string(),
                chunk_count: Some(-1),
                updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative chunk count"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_source_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source_1', lifecycle_state: 'extracted', chunk_count: 0})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source_2', lifecycle_state: 'parsed', chunk_count: 0})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_source_lifecycle_batch(&KnowledgeSourceLifecycleBatchRequest {
            updates: vec![
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_1".to_string(),
                    current_lifecycle_state: Some("extracted".to_string()),
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: None,
                    updated_at: Value::String("2026-07-19T12:00:00Z".to_string()),
                },
                KnowledgeSourceLifecycleUpdate {
                    source_id: "source_2".to_string(),
                    current_lifecycle_state: None,
                    lifecycle_state: "indexed".to_string(),
                    chunk_count: Some(3),
                    updated_at: Value::String("2026-07-19T12:01:00Z".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_2".to_string(),
                },
            ],
            property_names: vec!["lifecycle_state".to_string(), "chunk_count".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("lifecycle_state"),
            Some(&Some(Value::String("indexed".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("lifecycle_state"),
            Some(&Some(Value::String("indexed".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("chunk_count"),
            Some(&Some(Value::Int(3)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_memory_lifecycle_batch_for_metadata_state() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', is_latest: true, lifecycle_state: 'active', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', is_latest: true, lifecycle_state: 'active', updated_at: 1})")
        .unwrap();

    let output = db
        .update_knowledge_memory_lifecycle_batch(&KnowledgeMemoryLifecycleBatchRequest {
            updates: vec![
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_1".to_string(),
                    metadata: Value::String("{\"archived\":true}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:00:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"reviewed\":true}".to_string()),
                    is_latest: true,
                    lifecycle_state: "active".to_string(),
                    updated_at: Value::String("2026-07-19T13:01:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:02:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "missing".to_string(),
                    metadata: Value::String("{}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:03:00Z".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 8);
    assert_eq!(output.rows[0].updated_property_count, 4);
    assert_eq!(output.rows[1].updated_property_count, 4);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        property_names: vec![
            "metadata".to_string(),
            "is_latest".to_string(),
            "lifecycle_state".to_string(),
            "updated_at".to_string(),
        ],
    });
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"archived\":true}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("is_latest"),
        Some(&Some(Value::Bool(false)))
    );
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("archived".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String("{\"reviewed\":true}".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("lifecycle_state"),
        Some(&Some(Value::String("active".to_string())))
    );
}

#[test]
fn memory_lifecycle_batch_rejects_empty_state_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', lifecycle_state: 'active'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_memory_lifecycle_batch(&KnowledgeMemoryLifecycleBatchRequest {
            updates: vec![KnowledgeMemoryLifecycleUpdate {
                memory_id: "memory_1".to_string(),
                metadata: Value::String("{}".to_string()),
                is_latest: true,
                lifecycle_state: String::new(),
                updated_at: Value::String("2026-07-19T13:00:00Z".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty lifecycle state"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_memory_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', metadata: '{}', is_latest: true, lifecycle_state: 'active'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', metadata: '{}', is_latest: true, lifecycle_state: 'active'})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_memory_lifecycle_batch(&KnowledgeMemoryLifecycleBatchRequest {
            updates: vec![
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_1".to_string(),
                    metadata: Value::String("{\"state\":\"archived\"}".to_string()),
                    is_latest: false,
                    lifecycle_state: "archived".to_string(),
                    updated_at: Value::String("2026-07-19T13:00:00Z".to_string()),
                },
                KnowledgeMemoryLifecycleUpdate {
                    memory_id: "memory_2".to_string(),
                    metadata: Value::String("{\"state\":\"active\"}".to_string()),
                    is_latest: true,
                    lifecycle_state: "active".to_string(),
                    updated_at: Value::String("2026-07-19T13:01:00Z".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["metadata".to_string(), "is_latest".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"state\":\"archived\"}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("is_latest"),
            Some(&Some(Value::Bool(false)))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"state\":\"active\"}".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_memory_latest_batch_for_nowledge_evolution_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'older', is_latest: true, space_id: 'space_a', metadata: '{}', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', is_latest: false, space_id: 'space_a', metadata: '{}', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'other_space', is_latest: true, space_id: 'space_b'})")
        .unwrap();

    let output = db
        .update_knowledge_memory_latest_batch(&KnowledgeMemoryLatestBatchRequest {
            updates: vec![
                KnowledgeMemoryLatestUpdate {
                    memory_id: "older".to_string(),
                    is_latest: false,
                    space_id_filter: Some("space_a".to_string()),
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "newer".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "other_space".to_string(),
                    is_latest: false,
                    space_id_filter: Some("space_a".to_string()),
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "older".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "missing".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.matched_count, 2);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert!(output.rows[0].updated);
    assert!(output.rows[1].updated);
    assert!(output.rows[2].filtered_out);
    assert!(output.rows[3].duplicate);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "older".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "newer".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "other_space".to_string(),
            },
        ],
        property_names: vec![
            "is_latest".to_string(),
            "metadata".to_string(),
            "lifecycle_state".to_string(),
        ],
    });
    assert_eq!(
        rows.rows[0].properties.get("is_latest"),
        Some(&Some(Value::Bool(false)))
    );
    assert_eq!(
        rows.rows[1].properties.get("is_latest"),
        Some(&Some(Value::Bool(true)))
    );
    assert_eq!(
        rows.rows[2].properties.get("is_latest"),
        Some(&Some(Value::Bool(true)))
    );
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("lifecycle_state"),
        Some(&Some(Value::String("active".to_string())))
    );
}

#[test]
fn memory_latest_batch_rejects_empty_id_before_wal() {
    let path = unique_test_dir("memory_latest_empty_id");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'memory_1', is_latest: true})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();

    let error = db
        .update_knowledge_memory_latest_batch(&KnowledgeMemoryLatestBatchRequest {
            updates: vec![KnowledgeMemoryLatestUpdate {
                memory_id: String::new(),
                is_latest: false,
                space_id_filter: None,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty memory id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(
        std::fs::read_to_string(path.join("wal.skein")).unwrap(),
        wal_before
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_memory_latest_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_memory_latest_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'older', is_latest: true, space_id: 'space_a'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'newer', is_latest: false, space_id: 'space_a'})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_memory_latest_batch(&KnowledgeMemoryLatestBatchRequest {
            updates: vec![
                KnowledgeMemoryLatestUpdate {
                    memory_id: "older".to_string(),
                    is_latest: false,
                    space_id_filter: Some("space_a".to_string()),
                },
                KnowledgeMemoryLatestUpdate {
                    memory_id: "newer".to_string(),
                    is_latest: true,
                    space_id_filter: None,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "older".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "newer".to_string(),
                },
            ],
            property_names: vec!["is_latest".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("is_latest"),
            Some(&Some(Value::Bool(false)))
        );
        assert_eq!(
            rows.rows[1].properties.get("is_latest"),
            Some(&Some(Value::Bool(true)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_skill_usage_stats_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', use_count: 1, success_rate: 0.5, metadata: '{}', updated_at: 100})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill_2', use_count: 2, metadata: '{}', updated_at: 100})")
        .unwrap();

    let output = db
        .update_knowledge_skill_usage_stats_batch(&KnowledgeSkillUsageStatsBatchRequest {
            updates: vec![
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_1".to_string(),
                    use_count: 8,
                    success_rate: Some(Value::Float(0.75)),
                    last_activity_at: Value::Int(810),
                    updated_at: Value::Int(820),
                    metadata: Value::String("{\"runs\":8}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_2".to_string(),
                    use_count: 5,
                    success_rate: None,
                    last_activity_at: Value::Int(910),
                    updated_at: Value::Int(910),
                    metadata: Value::String("{\"runs\":5}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_2".to_string(),
                    use_count: 6,
                    success_rate: None,
                    last_activity_at: Value::Int(920),
                    updated_at: Value::Int(920),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "missing".to_string(),
                    use_count: 1,
                    success_rate: Some(Value::Float(1.0)),
                    last_activity_at: Value::Int(930),
                    updated_at: Value::Int(930),
                    metadata: Value::String("{}".to_string()),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 9);
    assert_eq!(output.rows[0].updated_property_count, 5);
    assert_eq!(output.rows[1].updated_property_count, 4);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Skill".to_string(),
                external_id: "skill_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Skill".to_string(),
                external_id: "skill_2".to_string(),
            },
        ],
        property_names: vec![
            "use_count".to_string(),
            "success_rate".to_string(),
            "last_activity_at".to_string(),
            "updated_at".to_string(),
            "metadata".to_string(),
        ],
    });
    assert_eq!(
        rows.rows[0].properties.get("use_count"),
        Some(&Some(Value::Int(8)))
    );
    assert_eq!(
        rows.rows[0].properties.get("success_rate"),
        Some(&Some(Value::Float(0.75)))
    );
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"runs\":8}".to_string())))
    );
    assert_eq!(
        rows.rows[1].properties.get("use_count"),
        Some(&Some(Value::Int(5)))
    );
    assert_eq!(rows.rows[1].properties.get("success_rate"), Some(&None));
    assert_eq!(
        rows.rows[1].properties.get("last_activity_at"),
        Some(&Some(Value::Int(910)))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String("{\"runs\":5}".to_string())))
    );
}

#[test]
fn skill_usage_stats_batch_rejects_negative_use_count_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', use_count: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_skill_usage_stats_batch(&KnowledgeSkillUsageStatsBatchRequest {
            updates: vec![KnowledgeSkillUsageStatsUpdate {
                skill_id: "skill_1".to_string(),
                use_count: -1,
                success_rate: None,
                last_activity_at: Value::Int(1),
                updated_at: Value::Int(1),
                metadata: Value::String("{}".to_string()),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative use count"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_skill_usage_stats_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_skill_usage_stats_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Skill {id: 'skill_1', use_count: 1, success_rate: 0.5, metadata: '{}'})",
        )
        .unwrap();
        db.query("CREATE (:Skill {id: 'skill_2', use_count: 1, metadata: '{}'})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_skill_usage_stats_batch(&KnowledgeSkillUsageStatsBatchRequest {
            updates: vec![
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_1".to_string(),
                    use_count: 8,
                    success_rate: Some(Value::Float(0.75)),
                    last_activity_at: Value::Int(810),
                    updated_at: Value::Int(820),
                    metadata: Value::String("{\"runs\":8}".to_string()),
                },
                KnowledgeSkillUsageStatsUpdate {
                    skill_id: "skill_2".to_string(),
                    use_count: 5,
                    success_rate: None,
                    last_activity_at: Value::Int(910),
                    updated_at: Value::Int(910),
                    metadata: Value::String("{\"runs\":5}".to_string()),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_2".to_string(),
                },
            ],
            property_names: vec![
                "use_count".to_string(),
                "success_rate".to_string(),
                "metadata".to_string(),
            ],
        });
        assert_eq!(
            rows.rows[0].properties.get("use_count"),
            Some(&Some(Value::Int(8)))
        );
        assert_eq!(
            rows.rows[0].properties.get("success_rate"),
            Some(&Some(Value::Float(0.75)))
        );
        assert_eq!(
            rows.rows[1].properties.get("use_count"),
            Some(&Some(Value::Int(5)))
        );
        assert_eq!(rows.rows[1].properties.get("success_rate"), Some(&None));
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"runs\":5}".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn skill_lifecycle_update(skill_id: &str) -> KnowledgeSkillLifecycleUpdate {
    KnowledgeSkillLifecycleUpdate {
        skill_id: skill_id.to_string(),
        stage: None,
        rejected_at: None,
        rationale: None,
        version: None,
        title: None,
        name: None,
        description: None,
        triggers: None,
        tools: None,
        bundle_path: None,
        content_hash: None,
        write_origin: None,
        metadata: None,
        updated_at: Value::Int(1),
    }
}

#[test]
fn updates_skill_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    for skill_id in [
        "skill_reject",
        "skill_compiled",
        "skill_promote",
        "skill_draft",
        "skill_content",
    ] {
        db.query(format!("CREATE (:Skill {{id: '{skill_id}', stage: 'candidate', metadata: '{{}}', updated_at: 1}})").as_str())
            .unwrap();
    }

    let output = db
        .update_knowledge_skill_lifecycle_batch(&KnowledgeSkillLifecycleBatchRequest {
            updates: vec![
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("rejected".to_string()),
                    rejected_at: Some(Value::Int(201)),
                    updated_at: Value::Int(202),
                    ..skill_lifecycle_update("skill_reject")
                },
                KnowledgeSkillLifecycleUpdate {
                    version: Some(Value::Int(2)),
                    title: Some(Value::String("compiled-skill".to_string())),
                    name: Some(Value::String("compiled-skill".to_string())),
                    description: Some(Value::String("compiled description".to_string())),
                    content_hash: Some(Value::String("hash-compiled-rest".to_string())),
                    bundle_path: Some(Value::String("/tmp/compiled-rest".to_string())),
                    metadata: Some(Value::String("{\"compiled\":true}".to_string())),
                    updated_at: Value::Int(401),
                    ..skill_lifecycle_update("skill_compiled")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("promotable".to_string()),
                    rationale: Some(Value::String("ready to promote".to_string())),
                    updated_at: Value::Int(1_700_000_032),
                    ..skill_lifecycle_update("skill_promote")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("draft".to_string()),
                    name: Some(Value::String("mcp-skill".to_string())),
                    description: Some(Value::String("compiled skill".to_string())),
                    triggers: Some(Value::List(vec![Value::String("compile".to_string())])),
                    tools: Some(Value::List(vec![Value::String("shell".to_string())])),
                    bundle_path: Some(Value::String("/tmp/mcp-skill".to_string())),
                    content_hash: Some(Value::String("hash-write".to_string())),
                    write_origin: Some("compiler".to_string()),
                    updated_at: Value::Int(1_700_000_033),
                    ..skill_lifecycle_update("skill_draft")
                },
                KnowledgeSkillLifecycleUpdate {
                    content_hash: Some(Value::String("hash-updated".to_string())),
                    metadata: Some(Value::String("{\"updated\":true}".to_string())),
                    updated_at: Value::Int(701),
                    ..skill_lifecycle_update("skill_content")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("active".to_string()),
                    updated_at: Value::Int(800),
                    ..skill_lifecycle_update("skill_draft")
                },
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("active".to_string()),
                    updated_at: Value::Int(900),
                    ..skill_lifecycle_update("missing")
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 5);
    assert_eq!(output.graph_commit_epoch_after, 6);
    assert_eq!(output.rows.len(), 7);
    assert_eq!(output.matched_count, 5);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 5);
    assert_eq!(output.updated_property_count, 26);
    assert_eq!(output.rows[0].updated_property_count, 3);
    assert_eq!(output.rows[1].updated_property_count, 8);
    assert_eq!(output.rows[2].updated_property_count, 3);
    assert_eq!(output.rows[3].updated_property_count, 9);
    assert_eq!(output.rows[4].updated_property_count, 3);
    assert!(output.rows[5].duplicate);
    assert!(!output.rows[6].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Skill".to_string(),
                external_id: "skill_reject".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Skill".to_string(),
                external_id: "skill_compiled".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Skill".to_string(),
                external_id: "skill_promote".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Skill".to_string(),
                external_id: "skill_draft".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Skill".to_string(),
                external_id: "skill_content".to_string(),
            },
        ],
        property_names: vec![
            "stage".to_string(),
            "rejected_at".to_string(),
            "rationale".to_string(),
            "version".to_string(),
            "title".to_string(),
            "name".to_string(),
            "description".to_string(),
            "triggers".to_string(),
            "tools".to_string(),
            "bundle_path".to_string(),
            "content_hash".to_string(),
            "write_origin".to_string(),
            "metadata".to_string(),
            "updated_at".to_string(),
        ],
    });
    assert_eq!(
        rows.rows[0].properties.get("stage"),
        Some(&Some(Value::String("rejected".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("rejected_at"),
        Some(&Some(Value::Int(201)))
    );
    assert_eq!(
        rows.rows[1].properties.get("version"),
        Some(&Some(Value::Int(2)))
    );
    assert_eq!(
        rows.rows[1].properties.get("content_hash"),
        Some(&Some(Value::String("hash-compiled-rest".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("stage"),
        Some(&Some(Value::String("promotable".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("rationale"),
        Some(&Some(Value::String("ready to promote".to_string())))
    );
    assert_eq!(
        rows.rows[3].properties.get("stage"),
        Some(&Some(Value::String("draft".to_string())))
    );
    assert_eq!(
        rows.rows[3].properties.get("triggers"),
        Some(&Some(Value::List(vec![Value::String(
            "compile".to_string()
        )])))
    );
    assert_eq!(
        rows.rows[3].properties.get("write_origin"),
        Some(&Some(Value::String("compiler".to_string())))
    );
    assert_eq!(
        rows.rows[4].properties.get("content_hash"),
        Some(&Some(Value::String("hash-updated".to_string())))
    );
    assert_eq!(
        rows.rows[4].properties.get("metadata"),
        Some(&Some(Value::String("{\"updated\":true}".to_string())))
    );
}

#[test]
fn skill_lifecycle_batch_rejects_empty_stage_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill_1', stage: 'candidate'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_skill_lifecycle_batch(&KnowledgeSkillLifecycleBatchRequest {
            updates: vec![KnowledgeSkillLifecycleUpdate {
                stage: Some(String::new()),
                updated_at: Value::Int(100),
                ..skill_lifecycle_update("skill_1")
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty stage"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_skill_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_skill_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Skill {id: 'skill_1', stage: 'candidate', metadata: '{}', updated_at: 1})",
        )
        .unwrap();
        db.query("CREATE (:Skill {id: 'skill_2', stage: 'draft', metadata: '{}', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_skill_lifecycle_batch(&KnowledgeSkillLifecycleBatchRequest {
            updates: vec![
                KnowledgeSkillLifecycleUpdate {
                    stage: Some("promotable".to_string()),
                    rationale: Some(Value::String("ready".to_string())),
                    updated_at: Value::Int(100),
                    ..skill_lifecycle_update("skill_1")
                },
                KnowledgeSkillLifecycleUpdate {
                    content_hash: Some(Value::String("hash-updated".to_string())),
                    metadata: Some(Value::String("{\"updated\":true}".to_string())),
                    updated_at: Value::Int(200),
                    ..skill_lifecycle_update("skill_2")
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Skill".to_string(),
                    external_id: "skill_2".to_string(),
                },
            ],
            property_names: vec![
                "stage".to_string(),
                "rationale".to_string(),
                "content_hash".to_string(),
                "metadata".to_string(),
            ],
        });
        assert_eq!(
            rows.rows[0].properties.get("stage"),
            Some(&Some(Value::String("promotable".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("rationale"),
            Some(&Some(Value::String("ready".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("content_hash"),
            Some(&Some(Value::String("hash-updated".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String("{\"updated\":true}".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_thread_metadata_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_2', metadata: '{}', updated_at: 1})")
        .unwrap();

    let output = db
        .update_knowledge_thread_metadata_batch(&KnowledgeThreadMetadataBatchRequest {
            updates: vec![
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_1".to_string(),
                    metadata: Value::String("{\"summary\":\"ready\"}".to_string()),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_2".to_string(),
                    metadata: Value::String("{\"summary\":\"metadata-only\"}".to_string()),
                    updated_at: None,
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_2".to_string(),
                    metadata: Value::String("{\"duplicate\":true}".to_string()),
                    updated_at: Some(Value::Int(200)),
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "missing".to_string(),
                    metadata: Value::String("{}".to_string()),
                    updated_at: Some(Value::Int(300)),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert_eq!(output.updated_property_count, 3);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 1);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Thread".to_string(),
                external_id: "thread_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Thread".to_string(),
                external_id: "thread_2".to_string(),
            },
        ],
        property_names: vec!["metadata".to_string(), "updated_at".to_string()],
    });
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"summary\":\"ready\"}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(100)))
    );
    assert_eq!(
        rows.rows[1].properties.get("metadata"),
        Some(&Some(Value::String(
            "{\"summary\":\"metadata-only\"}".to_string()
        )))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::Int(1)))
    );
}

#[test]
fn thread_metadata_batch_rejects_empty_thread_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', metadata: '{}', updated_at: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_thread_metadata_batch(&KnowledgeThreadMetadataBatchRequest {
            updates: vec![KnowledgeThreadMetadataUpdate {
                thread_id: String::new(),
                metadata: Value::String("{}".to_string()),
                updated_at: Some(Value::Int(100)),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty thread id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_metadata_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_metadata_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Thread {id: 'thread_2', metadata: '{}', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_thread_metadata_batch(&KnowledgeThreadMetadataBatchRequest {
            updates: vec![
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_1".to_string(),
                    metadata: Value::String("{\"summary\":\"ready\"}".to_string()),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeThreadMetadataUpdate {
                    thread_id: "thread_2".to_string(),
                    metadata: Value::String("{\"summary\":\"metadata-only\"}".to_string()),
                    updated_at: None,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_2".to_string(),
                },
            ],
            property_names: vec!["metadata".to_string(), "updated_at".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"summary\":\"ready\"}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(100)))
        );
        assert_eq!(
            rows.rows[1].properties.get("metadata"),
            Some(&Some(Value::String(
                "{\"summary\":\"metadata-only\"}".to_string()
            )))
        );
        assert_eq!(
            rows.rows[1].properties.get("updated_at"),
            Some(&Some(Value::Int(1)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_thread_message_count_batch_with_preserve_newer_timestamp() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', message_count: 1, updated_at: 200})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_2', message_count: 1, updated_at: 100})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread_3', message_count: 1, updated_at: 10})")
        .unwrap();

    let output = db
        .update_knowledge_thread_message_count_batch(&KnowledgeThreadMessageCountBatchRequest {
            updates: vec![
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_1".to_string(),
                    message_count: 7,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_2".to_string(),
                    message_count: 8,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_3".to_string(),
                    message_count: 9,
                    updated_at: None,
                    preserve_newer_existing_updated_at: false,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_2".to_string(),
                    message_count: 10,
                    updated_at: Some(Value::Int(300)),
                    preserve_newer_existing_updated_at: false,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "missing".to_string(),
                    message_count: 1,
                    updated_at: Some(Value::Int(100)),
                    preserve_newer_existing_updated_at: false,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 3);
    assert_eq!(output.updated_at_changed_count, 1);
    assert_eq!(output.updated_property_count, 4);
    assert_eq!(output.rows[0].updated_property_count, 1);
    assert!(!output.rows[0].updated_at_changed);
    assert_eq!(output.rows[1].updated_property_count, 2);
    assert!(output.rows[1].updated_at_changed);
    assert_eq!(output.rows[2].updated_property_count, 1);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Thread".to_string(),
                external_id: "thread_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Thread".to_string(),
                external_id: "thread_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Thread".to_string(),
                external_id: "thread_3".to_string(),
            },
        ],
        property_names: vec!["message_count".to_string(), "updated_at".to_string()],
    });
    assert_eq!(
        rows.rows[0].properties.get("message_count"),
        Some(&Some(Value::Int(7)))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(200)))
    );
    assert_eq!(
        rows.rows[1].properties.get("message_count"),
        Some(&Some(Value::Int(8)))
    );
    assert_eq!(
        rows.rows[1].properties.get("updated_at"),
        Some(&Some(Value::Int(150)))
    );
    assert_eq!(
        rows.rows[2].properties.get("message_count"),
        Some(&Some(Value::Int(9)))
    );
    assert_eq!(
        rows.rows[2].properties.get("updated_at"),
        Some(&Some(Value::Int(10)))
    );
}

#[test]
fn thread_message_count_batch_rejects_negative_count_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread_1', message_count: 1})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_thread_message_count_batch(&KnowledgeThreadMessageCountBatchRequest {
            updates: vec![KnowledgeThreadMessageCountUpdate {
                thread_id: "thread_1".to_string(),
                message_count: -1,
                updated_at: Some(Value::Int(100)),
                preserve_newer_existing_updated_at: false,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-negative message count"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_thread_message_count_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_thread_message_count_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Thread {id: 'thread_1', message_count: 1, updated_at: 200})")
            .unwrap();
        db.query("CREATE (:Thread {id: 'thread_2', message_count: 1, updated_at: 100})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_thread_message_count_batch(&KnowledgeThreadMessageCountBatchRequest {
            updates: vec![
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_1".to_string(),
                    message_count: 7,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
                KnowledgeThreadMessageCountUpdate {
                    thread_id: "thread_2".to_string(),
                    message_count: 8,
                    updated_at: Some(Value::Int(150)),
                    preserve_newer_existing_updated_at: true,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Thread".to_string(),
                    external_id: "thread_2".to_string(),
                },
            ],
            property_names: vec!["message_count".to_string(), "updated_at".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("message_count"),
            Some(&Some(Value::Int(7)))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(200)))
        );
        assert_eq!(
            rows.rows[1].properties.get("message_count"),
            Some(&Some(Value::Int(8)))
        );
        assert_eq!(
            rows.rows[1].properties.get("updated_at"),
            Some(&Some(Value::Int(150)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_label_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'label_meta', name: 'Meta', metadata: '{}', updated_at: 1})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_canonical', name: 'Canonical'})")
        .unwrap();
    db.query(
        "CREATE (:Label {id: 'label_rename', name: 'Old', canonical_name: 'old', updated_at: 1})",
    )
    .unwrap();

    let output = db
        .update_knowledge_label_lifecycle_batch(&KnowledgeLabelLifecycleBatchRequest {
            updates: vec![
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_meta".to_string(),
                    name: None,
                    canonical_name: None,
                    metadata: Some(Value::String("{\"owner\":\"mem\"}".to_string())),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_canonical".to_string(),
                    name: None,
                    canonical_name: Some("canonical-label".to_string()),
                    metadata: None,
                    updated_at: None,
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_rename".to_string(),
                    name: Some("Renamed".to_string()),
                    canonical_name: Some("renamed".to_string()),
                    metadata: None,
                    updated_at: Some(Value::Int(200)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_rename".to_string(),
                    name: Some("Duplicate".to_string()),
                    canonical_name: Some("duplicate".to_string()),
                    metadata: None,
                    updated_at: Some(Value::Int(300)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "missing".to_string(),
                    name: None,
                    canonical_name: Some("missing".to_string()),
                    metadata: None,
                    updated_at: None,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 3);
    assert_eq!(output.updated_property_count, 6);
    assert_eq!(output.rows[0].updated_property_count, 2);
    assert_eq!(output.rows[1].updated_property_count, 1);
    assert_eq!(output.rows[2].updated_property_count, 3);
    assert!(output.rows[3].duplicate);
    assert!(!output.rows[4].matched);

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_meta".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_canonical".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_rename".to_string(),
            },
        ],
        property_names: vec![
            "name".to_string(),
            "canonical_name".to_string(),
            "metadata".to_string(),
            "updated_at".to_string(),
        ],
    });
    assert_eq!(
        rows.rows[0].properties.get("metadata"),
        Some(&Some(Value::String("{\"owner\":\"mem\"}".to_string())))
    );
    assert_eq!(
        rows.rows[0].properties.get("updated_at"),
        Some(&Some(Value::Int(100)))
    );
    assert_eq!(
        rows.rows[1].properties.get("canonical_name"),
        Some(&Some(Value::String("canonical-label".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("name"),
        Some(&Some(Value::String("Renamed".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("canonical_name"),
        Some(&Some(Value::String("renamed".to_string())))
    );
    assert_eq!(
        rows.rows[2].properties.get("updated_at"),
        Some(&Some(Value::Int(200)))
    );
}

#[test]
fn label_lifecycle_batch_rejects_empty_canonical_name_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'label_1', name: 'Label'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_label_lifecycle_batch(&KnowledgeLabelLifecycleBatchRequest {
            updates: vec![KnowledgeLabelLifecycleUpdate {
                label_id: "label_1".to_string(),
                name: None,
                canonical_name: Some(String::new()),
                metadata: None,
                updated_at: None,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty canonical name"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_label_lifecycle_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_label_lifecycle_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Label {id: 'label_meta', name: 'Meta', metadata: '{}', updated_at: 1})")
            .unwrap();
        db.query("CREATE (:Label {id: 'label_rename', name: 'Old', canonical_name: 'old', updated_at: 1})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_label_lifecycle_batch(&KnowledgeLabelLifecycleBatchRequest {
            updates: vec![
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_meta".to_string(),
                    name: None,
                    canonical_name: None,
                    metadata: Some(Value::String("{\"owner\":\"mem\"}".to_string())),
                    updated_at: Some(Value::Int(100)),
                },
                KnowledgeLabelLifecycleUpdate {
                    label_id: "label_rename".to_string(),
                    name: Some("Renamed".to_string()),
                    canonical_name: Some("renamed".to_string()),
                    metadata: None,
                    updated_at: Some(Value::Int(200)),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_meta".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_rename".to_string(),
                },
            ],
            property_names: vec![
                "name".to_string(),
                "canonical_name".to_string(),
                "metadata".to_string(),
                "updated_at".to_string(),
            ],
        });
        assert_eq!(
            rows.rows[0].properties.get("metadata"),
            Some(&Some(Value::String("{\"owner\":\"mem\"}".to_string())))
        );
        assert_eq!(
            rows.rows[0].properties.get("updated_at"),
            Some(&Some(Value::Int(100)))
        );
        assert_eq!(
            rows.rows[1].properties.get("name"),
            Some(&Some(Value::String("Renamed".to_string())))
        );
        assert_eq!(
            rows.rows[1].properties.get("canonical_name"),
            Some(&Some(Value::String("renamed".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reads_labels_by_canonical_name_for_nowledge_collision_checks() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'source', name: 'Source', canonical_name: 'canonical_source'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'target', name: 'Target', canonical_name: 'canonical_target'})")
        .unwrap();
    db.query(
        "CREATE (:Label {id: 'target_2', name: 'Target 2', canonical_name: 'canonical_target'})",
    )
    .unwrap();

    let output = db
        .lookup_knowledge_labels_by_canonical_name(&KnowledgeLabelCanonicalLookupRequest {
            canonical_name: "canonical_target".to_string(),
            exclude_label_id: Some("source".to_string()),
            limit: 1,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.returned_count, 1);
    assert_eq!(output.rows[0].label_id.as_deref(), Some("target"));
    assert_eq!(
        output.rows[0].canonical_name.as_deref(),
        Some("canonical_target")
    );
}

#[test]
fn scans_labels_missing_canonical_name_for_nowledge_backfill() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'missing_1', name: 'Missing 1', canonical_name: NULL})")
        .unwrap();
    db.query("CREATE (:Label {id: 'missing_2', name: 'Missing 2'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'present', name: 'Present', canonical_name: 'present'})")
        .unwrap();

    let output = db
        .scan_knowledge_labels_missing_canonical_name(&KnowledgeLabelBackfillScanRequest {
            exclude_label_id: Some("missing_1".to_string()),
            limit: 10,
        })
        .unwrap();

    assert_eq!(output.matched_count, 1);
    assert_eq!(output.returned_count, 1);
    assert_eq!(output.rows[0].label_id.as_deref(), Some("missing_2"));
    assert_eq!(output.rows[0].name.as_deref(), Some("Missing 2"));
    assert_eq!(output.rows[0].canonical_name, None);
}

#[test]
fn reads_label_usage_rows_for_nowledge_label_apis() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'alpha', name: 'Alpha', canonical_name: 'alpha', color: '#fff', description: 'Alpha label', created_at: 10, updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Label {id: 'beta', name: 'Beta', canonical_name: 'beta'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity_1'}), (l:Label {id: 'alpha'}) CREATE (e)-[:HAS_LABEL]->(l)",
    )
    .unwrap();

    let row = db
        .knowledge_label_usage(&KnowledgeLabelUsageRequest {
            label_id: "alpha".to_string(),
        })
        .unwrap();
    assert_eq!(row.graph_commit_epoch, 6);
    assert!(row.found);
    let alpha = row.row.unwrap();
    assert_eq!(alpha.label_id.as_deref(), Some("alpha"));
    assert_eq!(alpha.name.as_deref(), Some("Alpha"));
    assert_eq!(alpha.canonical_name.as_deref(), Some("alpha"));
    assert_eq!(alpha.color, Some(Value::String("#fff".to_string())));
    assert_eq!(
        alpha.description,
        Some(Value::String("Alpha label".to_string()))
    );
    assert_eq!(alpha.created_at, Some(Value::Int(10)));
    assert_eq!(alpha.updated_at, Some(Value::Int(20)));
    assert_eq!(alpha.usage_count, 2);

    let list = db.knowledge_label_canonical_usage(&KnowledgeLabelUsageListRequest {
        canonical_only: true,
        limit: 10,
    });
    assert_eq!(list.matched_count, 2);
    assert_eq!(list.returned_count, 2);
    assert_eq!(list.rows[0].label_id.as_deref(), Some("alpha"));
    assert_eq!(list.rows[0].usage_count, 2);
    assert_eq!(list.rows[1].label_id.as_deref(), Some("beta"));
    assert_eq!(list.rows[1].usage_count, 0);
}

#[test]
fn label_read_requests_validate_non_empty_filters() {
    let db = Database::new();
    let canonical_error = db
        .lookup_knowledge_labels_by_canonical_name(&KnowledgeLabelCanonicalLookupRequest {
            canonical_name: String::new(),
            exclude_label_id: None,
            limit: 10,
        })
        .unwrap_err();
    assert!(canonical_error
        .to_string()
        .contains("non-empty canonical name"));

    let exclude_error = db
        .scan_knowledge_labels_missing_canonical_name(&KnowledgeLabelBackfillScanRequest {
            exclude_label_id: Some(String::new()),
            limit: 10,
        })
        .unwrap_err();
    assert!(exclude_error
        .to_string()
        .contains("non-empty excluded label id"));

    let usage_error = db
        .knowledge_label_usage(&KnowledgeLabelUsageRequest {
            label_id: String::new(),
        })
        .unwrap_err();
    assert!(usage_error.to_string().contains("non-empty label id"));
}

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

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
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
    });
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

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
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
    });
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
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
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
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
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
        });
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
fn clears_community_assignments_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_community_1', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_community_1', community_id: 8})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_community_1', community_id: 9})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_without_community'})")
        .unwrap();

    let scoped = db
        .clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: vec!["Entity".to_string(), "Memory".to_string()],
        })
        .unwrap();
    assert_eq!(scoped.candidate_count, 2);
    assert_eq!(scoped.cleared_count, 2);
    assert_eq!(scoped.rows.len(), 2);
    assert!(scoped.rows.iter().all(|row| row.cleared));

    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_community_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_community_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: "source_community_1".to_string(),
            },
        ],
        property_names: vec!["community_id".to_string()],
    });
    assert_eq!(
        rows.rows[0].properties.get("community_id"),
        Some(&Some(Value::Null))
    );
    assert_eq!(
        rows.rows[1].properties.get("community_id"),
        Some(&Some(Value::Null))
    );
    assert_eq!(
        rows.rows[2].properties.get("community_id"),
        Some(&Some(Value::Int(9)))
    );

    let all = db
        .clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: Vec::new(),
        })
        .unwrap();
    assert_eq!(all.candidate_count, 1);
    assert_eq!(all.cleared_count, 1);
    assert_eq!(
        all.rows[0].external_id,
        Some("source_community_1".to_string())
    );
    let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![KnowledgeEntityRequest {
            label: "Source".to_string(),
            external_id: "source_community_1".to_string(),
        }],
        property_names: vec!["community_id".to_string()],
    });
    assert_eq!(
        rows.rows[0].properties.get("community_id"),
        Some(&Some(Value::Null))
    );
}

#[test]
fn community_assignment_clear_rejects_invalid_label_before_wal() {
    let path = unique_test_dir("community_assignment_clear_invalid_label");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'memory_community_1', community_id: 7})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();

    let error = db
        .clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: vec!["".to_string()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("node label identifier is empty"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(
        std::fs::read_to_string(path.join("wal.skein")).unwrap(),
        wal_before
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_assignment_clear_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_assignment_clear_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_community_1', community_id: 7})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity_community_1', community_id: 8})")
            .unwrap();
        let batch_count_before_clear = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: Vec::new(),
        })
        .unwrap();
        let batch_count_after_clear = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_clear, batch_count_before_clear + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_community_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_community_1".to_string(),
                },
            ],
            property_names: vec!["community_id".to_string()],
        });
        assert_eq!(
            rows.rows[0].properties.get("community_id"),
            Some(&Some(Value::Null))
        );
        assert_eq!(
            rows.rows[1].properties.get("community_id"),
            Some(&Some(Value::Null))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn creates_community_memberships_for_nowledge_entity_lifecycle() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Entity One'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_2', name: 'Entity Two'})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_1', name: 'Community One'})")
        .unwrap();

    let output = db
        .create_knowledge_community_memberships_batch(
            &KnowledgeCommunityMembershipCreateBatchRequest {
                memberships: vec![
                    KnowledgeCommunityMembershipCreate {
                        entity_id: "entity_1".to_string(),
                        community_id: "community_1".to_string(),
                        strength: 0.7,
                        created_at: Value::Int(13),
                        properties: Value::String("{}".to_string()),
                    },
                    KnowledgeCommunityMembershipCreate {
                        entity_id: "entity_2".to_string(),
                        community_id: "missing_community".to_string(),
                        strength: 0.9,
                        created_at: Value::Int(14),
                        properties: Value::String("{\"source\":\"test\"}".to_string()),
                    },
                ],
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].created);
    assert_eq!(output.rows[0].entity_node_id, Some(0));
    assert_eq!(output.rows[0].community_node_id, Some(2));
    assert!(!output.rows[1].created);
    assert_eq!(output.rows[1].entity_node_id, Some(1));
    assert_eq!(output.rows[1].community_node_id, None);

    let relationships = db
        .query("MATCH (e:Entity {id: 'entity_1'})-[r:BELONGS_TO]->(c:Community) RETURN c.id, r.strength, r.created_at, r.properties")
        .unwrap();
    assert_eq!(relationships.rows.len(), 1);
    assert_eq!(
        relationships.rows[0].get("c.id"),
        Some(&Value::String("community_1".to_string()))
    );
    assert_eq!(
        relationships.rows[0].get("r.strength"),
        Some(&Value::Float(0.7))
    );
    assert_eq!(
        relationships.rows[0].get("r.created_at"),
        Some(&Value::Int(13))
    );
    assert_eq!(
        relationships.rows[0].get("r.properties"),
        Some(&Value::String("{}".to_string()))
    );
    let nodes = db
        .query("MATCH (n) WHERE n.id IN ['entity_1', 'entity_2', 'community_1'] RETURN count(n) AS total")
        .unwrap();
    assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(3)));
}

#[test]
fn community_membership_create_rejects_invalid_input_before_wal() {
    let path = unique_test_dir("community_membership_create_invalid_before_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.query("CREATE (:Community {id: 'community_1'})").unwrap();
    }
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let error = db
            .create_knowledge_community_memberships_batch(
                &KnowledgeCommunityMembershipCreateBatchRequest {
                    memberships: vec![KnowledgeCommunityMembershipCreate {
                        entity_id: "entity_1".to_string(),
                        community_id: "community_1".to_string(),
                        strength: f64::NAN,
                        created_at: Value::Int(13),
                        properties: Value::String("{}".to_string()),
                    }],
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("strength must be finite"));
        assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    }
    let wal_after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_membership_create_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_membership_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_2'})").unwrap();
        db.query("CREATE (:Community {id: 'community_1'})").unwrap();
    }
    let setup_wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .create_knowledge_community_memberships_batch(
                &KnowledgeCommunityMembershipCreateBatchRequest {
                    memberships: vec![
                        KnowledgeCommunityMembershipCreate {
                            entity_id: "entity_1".to_string(),
                            community_id: "community_1".to_string(),
                            strength: 0.6,
                            created_at: Value::Int(21),
                            properties: Value::String("{}".to_string()),
                        },
                        KnowledgeCommunityMembershipCreate {
                            entity_id: "entity_2".to_string(),
                            community_id: "community_1".to_string(),
                            strength: 0.8,
                            created_at: Value::Int(22),
                            properties: Value::String("{}".to_string()),
                        },
                    ],
                },
            )
            .unwrap();
        assert_eq!(output.created_relationship_count, 2);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let relationships = db
            .query("MATCH (:Entity)-[r:BELONGS_TO]->(:Community) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(relationships.rows[0].get("total"), Some(&Value::Int(2)));
        let nodes = db.query("MATCH (n) RETURN count(n) AS total").unwrap();
        assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(3)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_community_lifecycle_for_nowledge_detection_and_summary() {
    let mut db = Database::new();
    db.query("CREATE (:Community {id: 'community_existing', community_id: 7, name: 'Old Name', description: 'old description', ai_summary: 'old summary', member_count: 2})")
        .unwrap();

    let output = db
        .update_knowledge_communities_batch(&KnowledgeCommunityLifecycleBatchRequest {
            creates: vec![
                KnowledgeCommunityCreate {
                    id: "community_new".to_string(),
                    community_id: 8,
                    name: "New Community".to_string(),
                    description: Value::String("new description".to_string()),
                    ai_summary: Value::Null,
                    member_count: 3,
                    resolution: 1.25,
                    created_at: Value::Int(100),
                    updated_at: Value::Int(100),
                },
                KnowledgeCommunityCreate {
                    id: "community_existing".to_string(),
                    community_id: 7,
                    name: "Existing Community".to_string(),
                    description: Value::String("ignored".to_string()),
                    ai_summary: Value::String("ignored".to_string()),
                    member_count: 2,
                    resolution: 1.0,
                    created_at: Value::Int(101),
                    updated_at: Value::Int(101),
                },
            ],
            summary_updates: vec![
                KnowledgeCommunitySummaryUpdate {
                    id: "community_existing".to_string(),
                    name: "Updated Name".to_string(),
                    description: Value::String("updated description".to_string()),
                    ai_summary: Value::String("updated summary".to_string()),
                    updated_at: Value::Int(200),
                },
                KnowledgeCommunitySummaryUpdate {
                    id: "missing_community".to_string(),
                    name: "Missing".to_string(),
                    description: Value::String("missing".to_string()),
                    ai_summary: Value::String("missing".to_string()),
                    updated_at: Value::Int(201),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 1);
    assert_eq!(output.updated_count, 1);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.created_node_count, 1);
    assert_eq!(output.updated_property_count, 4);
    assert!(output.create_rows[0].created);
    assert_eq!(output.create_rows[0].node_id, Some(1));
    assert!(output.create_rows[1].already_exists);
    assert!(output.summary_update_rows[0].updated);
    assert!(output.summary_update_rows[1].missing);

    let created = db
        .query("MATCH (c:Community {id: 'community_new'}) RETURN c.community_id, c.name, c.description, c.ai_summary, c.member_count, c.algorithm, c.resolution, c.created_at, c.updated_at")
        .unwrap();
    assert_eq!(created.rows[0].get("c.community_id"), Some(&Value::Int(8)));
    assert_eq!(
        created.rows[0].get("c.name"),
        Some(&Value::String("New Community".to_string()))
    );
    assert_eq!(
        created.rows[0].get("c.description"),
        Some(&Value::String("new description".to_string()))
    );
    assert_eq!(created.rows[0].get("c.ai_summary"), Some(&Value::Null));
    assert_eq!(created.rows[0].get("c.member_count"), Some(&Value::Int(3)));
    assert_eq!(
        created.rows[0].get("c.algorithm"),
        Some(&Value::String("louvain".to_string()))
    );
    assert_eq!(
        created.rows[0].get("c.resolution"),
        Some(&Value::Float(1.25))
    );
    assert_eq!(created.rows[0].get("c.created_at"), Some(&Value::Int(100)));
    assert_eq!(created.rows[0].get("c.updated_at"), Some(&Value::Int(100)));

    let updated = db
        .query("MATCH (c:Community {id: 'community_existing'}) RETURN c.name, c.description, c.ai_summary, c.updated_at")
        .unwrap();
    assert_eq!(
        updated.rows[0].get("c.name"),
        Some(&Value::String("Updated Name".to_string()))
    );
    assert_eq!(
        updated.rows[0].get("c.description"),
        Some(&Value::String("updated description".to_string()))
    );
    assert_eq!(
        updated.rows[0].get("c.ai_summary"),
        Some(&Value::String("updated summary".to_string()))
    );
    assert_eq!(updated.rows[0].get("c.updated_at"), Some(&Value::Int(200)));
}

#[test]
fn community_lifecycle_rejects_invalid_create_before_wal() {
    let path = unique_test_dir("community_lifecycle_invalid_create_before_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Community {id: 'community_existing'})")
            .unwrap();
    }
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let error = db
            .update_knowledge_communities_batch(&KnowledgeCommunityLifecycleBatchRequest {
                creates: vec![KnowledgeCommunityCreate {
                    id: "community_invalid".to_string(),
                    community_id: 1,
                    name: "Invalid".to_string(),
                    description: Value::String("invalid".to_string()),
                    ai_summary: Value::Null,
                    member_count: 1,
                    resolution: f64::NAN,
                    created_at: Value::Int(1),
                    updated_at: Value::Int(1),
                }],
                summary_updates: Vec::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("resolution must be finite"));
        assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    }
    let wal_after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_lifecycle_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_lifecycle_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Community {id: 'community_existing', name: 'Old'})")
            .unwrap();
    }
    let setup_wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .update_knowledge_communities_batch(&KnowledgeCommunityLifecycleBatchRequest {
                creates: vec![KnowledgeCommunityCreate {
                    id: "community_new".to_string(),
                    community_id: 9,
                    name: "New".to_string(),
                    description: Value::String("new".to_string()),
                    ai_summary: Value::String("summary".to_string()),
                    member_count: 4,
                    resolution: 0.75,
                    created_at: Value::Int(10),
                    updated_at: Value::Int(10),
                }],
                summary_updates: vec![KnowledgeCommunitySummaryUpdate {
                    id: "community_existing".to_string(),
                    name: "Updated".to_string(),
                    description: Value::String("updated".to_string()),
                    ai_summary: Value::String("updated summary".to_string()),
                    updated_at: Value::Int(20),
                }],
            })
            .unwrap();
        assert_eq!(output.created_node_count, 1);
        assert_eq!(output.updated_count, 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_node"));
    assert!(wal.contains("set_node_property"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let communities = db
            .query("MATCH (c:Community) RETURN count(c) AS total")
            .unwrap();
        assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(2)));
        let updated = db
            .query("MATCH (c:Community {id: 'community_existing'}) RETURN c.name, c.updated_at")
            .unwrap();
        assert_eq!(
            updated.rows[0].get("c.name"),
            Some(&Value::String("Updated".to_string()))
        );
        assert_eq!(updated.rows[0].get("c.updated_at"), Some(&Value::Int(20)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_communities_for_nowledge_replace_cleanup() {
    let mut db = Database::new();
    db.query("CREATE (:Community {id: 'community_1', name: 'One'})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_2', name: 'Two'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: false })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.deleted_count, 2);
    assert_eq!(output.rows.len(), 2);
    assert!(output.rows.iter().all(|row| row.deleted));
    assert_eq!(output.rows[0].id.as_deref(), Some("community_1"));
    assert_eq!(output.rows[1].id.as_deref(), Some("community_2"));

    let communities = db
        .query("MATCH (c:Community) RETURN count(c) AS total")
        .unwrap();
    assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(0)));
    let entities = db
        .query("MATCH (e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(entities.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn detach_deletes_communities_for_nowledge_undo_cleanup() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1'})-[:BELONGS_TO]->(:Community {id: 'community_1'})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_2'})").unwrap();

    let output = db
        .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: true })
        .unwrap();

    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.deleted_count, 2);
    let communities = db
        .query("MATCH (c:Community) RETURN count(c) AS total")
        .unwrap();
    assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(0)));
    let entities = db
        .query("MATCH (e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(entities.rows[0].get("total"), Some(&Value::Int(1)));
    let relationships = db
        .query("MATCH (:Entity)-[r:BELONGS_TO]->(:Community) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(relationships.rows[0].get("total"), Some(&Value::Int(0)));
}

#[test]
fn community_cleanup_without_candidates_does_not_write_wal() {
    let path = unique_test_dir("community_cleanup_without_candidates");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
    }
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let output = db
            .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: true })
            .unwrap();
        assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
        assert_eq!(output.graph_commit_epoch_after, graph_commit_epoch_before);
        assert_eq!(output.candidate_count, 0);
        assert_eq!(output.deleted_count, 0);
    }
    let wal_after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_cleanup_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_cleanup_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Entity {id: 'entity_1'})-[:BELONGS_TO]->(:Community {id: 'community_1'})",
        )
        .unwrap();
        db.query("CREATE (:Community {id: 'community_2'})").unwrap();
    }
    let setup_wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: true })
            .unwrap();
        assert_eq!(output.deleted_count, 2);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_node"));
    assert!(wal.contains("delete_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let communities = db
            .query("MATCH (c:Community) RETURN count(c) AS total")
            .unwrap();
        assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(0)));
        let entities = db
            .query("MATCH (e:Entity) RETURN count(e) AS total")
            .unwrap();
        assert_eq!(entities.rows[0].get("total"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn stamps_graph_meta_batch_for_nowledge_algorithm_state() {
    let mut db = Database::new();
    db.query(
        "CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: false, pagerank_iterations: 5})",
    )
    .unwrap();

    let output = db
        .stamp_knowledge_graph_meta_batch(&KnowledgeGraphMetaStampBatchRequest {
            stamps: vec![
                KnowledgeGraphMetaStamp {
                    meta_id: "main".to_string(),
                    assignments: BTreeMap::from([
                        ("pagerank_applied".to_string(), Value::Bool(true)),
                        (
                            "pagerank_algorithm".to_string(),
                            Value::String("pagerank".to_string()),
                        ),
                        ("pagerank_damping".to_string(), Value::Float(0.85)),
                        ("pagerank_iterations".to_string(), Value::Int(20)),
                        ("pagerank_computed_at".to_string(), Value::Int(100)),
                        ("updated_at".to_string(), Value::Int(101)),
                    ]),
                },
                KnowledgeGraphMetaStamp {
                    meta_id: "community".to_string(),
                    assignments: BTreeMap::from([
                        ("community_detection_applied".to_string(), Value::Bool(true)),
                        (
                            "community_algorithm".to_string(),
                            Value::String("louvain".to_string()),
                        ),
                        ("community_resolution".to_string(), Value::Float(0.8)),
                        ("community_count".to_string(), Value::Int(3)),
                        (
                            "community_detection_computed_at".to_string(),
                            Value::Int(200),
                        ),
                        ("last_augmentation_at".to_string(), Value::Int(201)),
                        ("updated_at".to_string(), Value::Int(202)),
                    ]),
                },
                KnowledgeGraphMetaStamp {
                    meta_id: "main".to_string(),
                    assignments: BTreeMap::from([(
                        "pagerank_applied".to_string(),
                        Value::Bool(false),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.updated_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_property_count, 13);
    assert!(output.rows[0].updated);
    assert!(output.rows[1].created);
    assert!(output.rows[2].duplicate);
    assert!(output.rows[1].node_id.is_some());

    let rows = db
        .query("MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_algorithm AS algorithm, m.pagerank_damping AS damping, m.pagerank_iterations AS iterations, m.pagerank_computed_at AS computed_at, m.updated_at AS updated_at")
        .unwrap();
    assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(true)));
    assert_eq!(
        rows.rows[0].get("algorithm"),
        Some(&Value::String("pagerank".to_string()))
    );
    assert_eq!(rows.rows[0].get("damping"), Some(&Value::Float(0.85)));
    assert_eq!(rows.rows[0].get("iterations"), Some(&Value::Int(20)));
    assert_eq!(rows.rows[0].get("computed_at"), Some(&Value::Int(100)));
    assert_eq!(rows.rows[0].get("updated_at"), Some(&Value::Int(101)));

    let rows = db
        .query("MATCH (m:GraphMeta {meta_id: 'community'}) RETURN m.community_detection_applied AS applied, m.community_algorithm AS algorithm, m.community_resolution AS resolution, m.community_count AS count, m.community_detection_computed_at AS computed_at, m.last_augmentation_at AS augmented")
        .unwrap();
    assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(true)));
    assert_eq!(
        rows.rows[0].get("algorithm"),
        Some(&Value::String("louvain".to_string()))
    );
    assert_eq!(rows.rows[0].get("resolution"), Some(&Value::Float(0.8)));
    assert_eq!(rows.rows[0].get("count"), Some(&Value::Int(3)));
    assert_eq!(rows.rows[0].get("computed_at"), Some(&Value::Int(200)));
    assert_eq!(rows.rows[0].get("augmented"), Some(&Value::Int(201)));
}

#[test]
fn graph_meta_stamp_rejects_meta_id_assignment_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: false})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .stamp_knowledge_graph_meta_batch(&KnowledgeGraphMetaStampBatchRequest {
            stamps: vec![KnowledgeGraphMetaStamp {
                meta_id: "main".to_string(),
                assignments: BTreeMap::from([(
                    "meta_id".to_string(),
                    Value::String("other".to_string()),
                )]),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("cannot update meta_id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_graph_meta_stamp_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_graph_meta_stamp_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: false})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.stamp_knowledge_graph_meta_batch(&KnowledgeGraphMetaStampBatchRequest {
            stamps: vec![
                KnowledgeGraphMetaStamp {
                    meta_id: "main".to_string(),
                    assignments: BTreeMap::from([
                        ("pagerank_applied".to_string(), Value::Bool(true)),
                        ("pagerank_computed_at".to_string(), Value::Int(404)),
                    ]),
                },
                KnowledgeGraphMetaStamp {
                    meta_id: "community".to_string(),
                    assignments: BTreeMap::from([
                        (
                            "community_detection_applied".to_string(),
                            Value::Bool(false),
                        ),
                        (
                            "community_algorithm".to_string(),
                            Value::String(String::new()),
                        ),
                        ("community_resolution".to_string(), Value::Float(1.0)),
                        ("community_count".to_string(), Value::Int(0)),
                        ("community_detection_computed_at".to_string(), Value::Null),
                    ]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    assert!(wal.contains("create_node"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_computed_at AS computed")
            .unwrap();
        assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(true)));
        assert_eq!(rows.rows[0].get("computed"), Some(&Value::Int(404)));
        let rows = db
            .query("MATCH (m:GraphMeta {meta_id: 'community'}) RETURN m.community_detection_applied AS applied, m.community_algorithm AS algorithm, m.community_resolution AS resolution, m.community_count AS count, m.community_detection_computed_at AS computed")
            .unwrap();
        assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(false)));
        assert_eq!(
            rows.rows[0].get("algorithm"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(rows.rows[0].get("resolution"), Some(&Value::Float(1.0)));
        assert_eq!(rows.rows[0].get("count"), Some(&Value::Int(0)));
        assert_eq!(rows.rows[0].get("computed"), Some(&Value::Null));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reads_graph_meta_by_meta_id_for_nowledge_algorithm_state() {
    let mut db = Database::new();
    db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true, community_detection_applied: false, pagerank_computed_at: 100})")
        .unwrap();

    let output = db
        .knowledge_graph_meta(&KnowledgeGraphMetaRequest {
            meta_id: "main".to_string(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 1);
    assert!(output.found);
    let meta = output.meta.unwrap();
    assert_eq!(meta.meta_id.as_deref(), Some("main"));
    assert_eq!(
        meta.properties.get("pagerank_applied"),
        Some(&Value::Bool(true))
    );
    assert_eq!(
        meta.properties.get("community_detection_applied"),
        Some(&Value::Bool(false))
    );
    assert_eq!(
        meta.properties.get("pagerank_computed_at"),
        Some(&Value::Int(100))
    );

    let missing = db
        .knowledge_graph_meta(&KnowledgeGraphMetaRequest {
            meta_id: "missing".to_string(),
        })
        .unwrap();
    assert_eq!(missing.graph_commit_epoch, 1);
    assert!(!missing.found);
    assert!(missing.meta.is_none());
}

#[test]
fn graph_meta_read_and_delete_reject_empty_meta_id_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let read_error = db
        .knowledge_graph_meta(&KnowledgeGraphMetaRequest {
            meta_id: String::new(),
        })
        .unwrap_err();
    assert!(read_error.to_string().contains("non-empty meta id"));

    let delete_error = db
        .delete_knowledge_graph_meta(&KnowledgeGraphMetaRequest {
            meta_id: String::new(),
        })
        .unwrap_err();
    assert!(delete_error.to_string().contains("non-empty meta id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn graph_meta_delete_missing_does_not_write_wal() {
    let path = unique_test_dir("graph_meta_delete_missing");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true})")
            .unwrap();
    }
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let output = db
            .delete_knowledge_graph_meta(&KnowledgeGraphMetaRequest {
                meta_id: "missing".to_string(),
            })
            .unwrap();
        assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
        assert_eq!(output.graph_commit_epoch_after, graph_commit_epoch_before);
        assert!(output.node_id.is_none());
        assert!(!output.matched);
        assert!(!output.deleted);
    }
    let wal_after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_graph_meta_delete_persists_and_replays() {
    let path = unique_test_dir("typed_graph_meta_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true})")
            .unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'community', community_detection_applied: true})")
            .unwrap();
    }
    let setup_wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .delete_knowledge_graph_meta(&KnowledgeGraphMetaRequest {
                meta_id: "main".to_string(),
            })
            .unwrap();
        assert!(output.matched);
        assert!(output.deleted);
        assert!(output.node_id.is_some());
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_node"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let db = Database::open(&path).unwrap();
        let main = db
            .knowledge_graph_meta(&KnowledgeGraphMetaRequest {
                meta_id: "main".to_string(),
            })
            .unwrap();
        assert!(!main.found);
        let community = db
            .knowledge_graph_meta(&KnowledgeGraphMetaRequest {
                meta_id: "community".to_string(),
            })
            .unwrap();
        assert!(community.found);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn applies_schema_migration_log_batch_idempotently() {
    let mut db = Database::new();
    db.query("CREATE (:SchemaMigrationLog {id: 'existing', applied_at: 10})")
        .unwrap();

    let output = db
        .apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![
                KnowledgeSchemaMigrationApply {
                    migration_id: "existing".to_string(),
                    applied_at: Value::Int(100),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_1".to_string(),
                    applied_at: Value::Int(101),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_1".to_string(),
                    applied_at: Value::Int(102),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_2".to_string(),
                    applied_at: Value::Int(103),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.created_count, 2);
    assert_eq!(output.already_applied_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert!(output.rows[0].already_applied);
    assert!(output.rows[1].created);
    assert!(output.rows[1].node_id.is_some());
    assert!(output.rows[2].duplicate);
    assert!(output.rows[3].created);

    let rows = db
        .query("MATCH (m:SchemaMigrationLog) RETURN m.id AS id, m.applied_at AS applied_at ORDER BY id ASC")
        .unwrap();
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(
        rows.rows[0].get("id"),
        Some(&Value::String("existing".to_string()))
    );
    assert_eq!(rows.rows[0].get("applied_at"), Some(&Value::Int(10)));
    assert_eq!(
        rows.rows[1].get("id"),
        Some(&Value::String("new_1".to_string()))
    );
    assert_eq!(rows.rows[1].get("applied_at"), Some(&Value::Int(101)));
    assert_eq!(
        rows.rows[2].get("id"),
        Some(&Value::String("new_2".to_string()))
    );
    assert_eq!(rows.rows[2].get("applied_at"), Some(&Value::Int(103)));
}

#[test]
fn schema_migration_apply_rejects_empty_id_before_wal() {
    let mut db = Database::new();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![KnowledgeSchemaMigrationApply {
                migration_id: String::new(),
                applied_at: Value::Int(100),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty migration id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_schema_migration_apply_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_schema_migration_apply_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .ok()
            .map_or(0, |wal| wal.matches("\tbatch\t").count());
        db.apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![
                KnowledgeSchemaMigrationApply {
                    migration_id: "migration_1".to_string(),
                    applied_at: Value::Int(100),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "migration_2".to_string(),
                    applied_at: Value::Int(200),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_node"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (m:SchemaMigrationLog) RETURN m.id AS id, m.applied_at AS applied_at ORDER BY id ASC")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("id"),
            Some(&Value::String("migration_1".to_string()))
        );
        assert_eq!(rows.rows[0].get("applied_at"), Some(&Value::Int(100)));
        assert_eq!(
            rows.rows[1].get("id"),
            Some(&Value::String("migration_2".to_string()))
        );
        assert_eq!(rows.rows[1].get("applied_at"), Some(&Value::Int(200)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_augmentation_job_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'running_progress', status: 'running', progress: 1.0, message: 'old'})")
        .unwrap();
    db.query(
        "CREATE (:AugmentationJob {job_id: 'running_complete', status: 'running', progress: 50.0})",
    )
    .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'completed_job', status: 'completed'})")
        .unwrap();

    let output = db
        .update_knowledge_augmentation_jobs_batch(&KnowledgeAugmentationJobLifecycleBatchRequest {
            updates: vec![
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "created_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::Create {
                        job_type: "pagerank".to_string(),
                        parameters: Value::String("{\"graph\":\"main\"}".to_string()),
                        created_at: Value::Int(100),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "pending_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkRunning {
                        started_at: Value::Int(101),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "running_progress".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::UpdateProgress {
                        progress: 42.5,
                        message: "halfway".to_string(),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "running_complete".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkCompleted {
                        result: Value::String("{\"ok\":true}".to_string()),
                        completed_at: Value::Int(102),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "completed_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
                        error_message: "too late".to_string(),
                        completed_at: Value::Int(103),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "missing_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
                        error_message: "missing".to_string(),
                        completed_at: Value::Int(104),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "created_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::Create {
                        job_type: "duplicate".to_string(),
                        parameters: Value::String("{}".to_string()),
                        created_at: Value::Int(105),
                    },
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.updated_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.already_exists_count, 0);
    assert_eq!(output.status_mismatch_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_property_count, 20);
    assert!(output.rows[0].created);
    assert!(output.rows[1].updated);
    assert!(output.rows[4].status_mismatch);
    assert!(output.rows[5].missing);
    assert!(output.rows[6].duplicate);

    let rows = db
        .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.progress AS progress, j.message AS message, j.result AS result, j.error_message AS error, j.started_at AS started_at, j.completed_at AS completed_at, j.created_at AS created_at ORDER BY id ASC")
        .unwrap();
    let created = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("created_job".to_string())))
        .unwrap();
    assert_eq!(
        created.get("status"),
        Some(&Value::String("pending".to_string()))
    );
    assert_eq!(created.get("progress"), Some(&Value::Float(0.0)));
    assert_eq!(
        created.get("message"),
        Some(&Value::String("Job created".to_string()))
    );
    assert_eq!(created.get("created_at"), Some(&Value::Int(100)));

    let pending = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("pending_job".to_string())))
        .unwrap();
    assert_eq!(
        pending.get("status"),
        Some(&Value::String("running".to_string()))
    );
    assert_eq!(pending.get("started_at"), Some(&Value::Int(101)));

    let progress = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("running_progress".to_string())))
        .unwrap();
    assert_eq!(progress.get("progress"), Some(&Value::Float(42.5)));
    assert_eq!(
        progress.get("message"),
        Some(&Value::String("halfway".to_string()))
    );

    let completed = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("running_complete".to_string())))
        .unwrap();
    assert_eq!(
        completed.get("status"),
        Some(&Value::String("completed".to_string()))
    );
    assert_eq!(completed.get("progress"), Some(&Value::Float(100.0)));
    assert_eq!(
        completed.get("message"),
        Some(&Value::String("Job completed successfully".to_string()))
    );
    assert_eq!(
        completed.get("result"),
        Some(&Value::String("{\"ok\":true}".to_string()))
    );
    assert_eq!(completed.get("completed_at"), Some(&Value::Int(102)));
}

#[test]
fn augmentation_job_progress_rejects_invalid_percentage_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_augmentation_jobs_batch(&KnowledgeAugmentationJobLifecycleBatchRequest {
            updates: vec![KnowledgeAugmentationJobLifecycleUpdate {
                job_id: "running_job".to_string(),
                transition: KnowledgeAugmentationJobLifecycleTransition::UpdateProgress {
                    progress: 101.0,
                    message: "bad".to_string(),
                },
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("finite percentage"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_augmentation_job_lifecycle_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_augmentation_job_lifecycle_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
            .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_augmentation_jobs_batch(
            &KnowledgeAugmentationJobLifecycleBatchRequest {
                updates: vec![
                    KnowledgeAugmentationJobLifecycleUpdate {
                        job_id: "created_job".to_string(),
                        transition: KnowledgeAugmentationJobLifecycleTransition::Create {
                            job_type: "louvain".to_string(),
                            parameters: Value::String("{}".to_string()),
                            created_at: Value::Int(10),
                        },
                    },
                    KnowledgeAugmentationJobLifecycleUpdate {
                        job_id: "running_job".to_string(),
                        transition: KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
                            error_message: "runtime failed".to_string(),
                            completed_at: Value::Int(20),
                        },
                    },
                ],
            },
        )
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_node"));
    assert!(wal.contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.job_type AS job_type, j.error_message AS error, j.completed_at AS completed_at ORDER BY id ASC")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("id"),
            Some(&Value::String("created_job".to_string()))
        );
        assert_eq!(
            rows.rows[0].get("status"),
            Some(&Value::String("pending".to_string()))
        );
        assert_eq!(
            rows.rows[0].get("job_type"),
            Some(&Value::String("louvain".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("id"),
            Some(&Value::String("running_job".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("status"),
            Some(&Value::String("failed".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("error"),
            Some(&Value::String("runtime failed".to_string()))
        );
        assert_eq!(rows.rows[1].get("completed_at"), Some(&Value::Int(20)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reads_augmentation_job_status_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'job_1', job_type: 'pagerank', status: 'running', progress: 42.5, message: 'Working', result: '{}', error_message: '', started_at: 100, completed_at: NULL, created_at: 90})")
        .unwrap();

    let output = db
        .knowledge_augmentation_job(&KnowledgeAugmentationJobRequest {
            job_id: "job_1".to_string(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 1);
    assert!(output.found);
    let job = output.job.unwrap();
    assert_eq!(job.job_id.as_deref(), Some("job_1"));
    assert_eq!(job.job_type.as_deref(), Some("pagerank"));
    assert_eq!(job.status.as_deref(), Some("running"));
    assert_eq!(job.progress, Some(42.5));
    assert_eq!(job.message.as_deref(), Some("Working"));
    assert_eq!(job.result, Some(Value::String("{}".to_string())));
    assert_eq!(job.error_message.as_deref(), Some(""));
    assert_eq!(job.started_at, Some(Value::Int(100)));
    assert_eq!(job.completed_at, Some(Value::Null));
    assert_eq!(job.created_at, Some(Value::Int(90)));

    let missing = db
        .knowledge_augmentation_job(&KnowledgeAugmentationJobRequest {
            job_id: "missing".to_string(),
        })
        .unwrap();
    assert!(!missing.found);
    assert!(missing.job.is_none());
}

#[test]
fn lists_augmentation_jobs_by_started_at_for_nowledge_graph_api() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'old_running', job_type: 'pagerank', status: 'running', progress: 10.0, message: 'old', started_at: 10, created_at: 1})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'new_running', job_type: 'louvain', status: 'running', progress: 20.0, message: 'new', started_at: 30, created_at: 2})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'pending', job_type: 'louvain', status: 'pending', progress: 0.0, message: 'pending', created_at: 3})")
        .unwrap();

    let output = db
        .knowledge_augmentation_jobs(&KnowledgeAugmentationJobListRequest {
            status_filter: Some("running".to_string()),
            order_by: KnowledgeAugmentationJobListOrder::StartedAtDesc,
            limit: 1,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.returned_count, 1);
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].job_id.as_deref(), Some("new_running"));
    assert_eq!(output.rows[0].started_at, Some(Value::Int(30)));
}

#[test]
fn lists_augmentation_jobs_by_created_at_for_rest_graph_api() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'old_done', job_type: 'pagerank', status: 'done', progress: 100.0, message: 'old', created_at: 10})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'new_done', job_type: 'pagerank', status: 'done', progress: 100.0, message: 'new', created_at: 20})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'queued', job_type: 'community', status: 'queued', progress: 0.0, message: 'queued', created_at: 30})")
        .unwrap();

    let filtered = db
        .knowledge_augmentation_jobs(&KnowledgeAugmentationJobListRequest {
            status_filter: Some("done".to_string()),
            order_by: KnowledgeAugmentationJobListOrder::CreatedAtDesc,
            limit: 10,
        })
        .unwrap();
    assert_eq!(filtered.matched_count, 2);
    assert_eq!(filtered.returned_count, 2);
    assert_eq!(filtered.rows[0].job_id.as_deref(), Some("new_done"));
    assert_eq!(filtered.rows[1].job_id.as_deref(), Some("old_done"));

    let all = db
        .knowledge_augmentation_jobs(&KnowledgeAugmentationJobListRequest {
            status_filter: None,
            order_by: KnowledgeAugmentationJobListOrder::CreatedAtDesc,
            limit: 2,
        })
        .unwrap();
    assert_eq!(all.matched_count, 3);
    assert_eq!(all.returned_count, 2);
    assert_eq!(all.rows[0].job_id.as_deref(), Some("queued"));
    assert_eq!(all.rows[1].job_id.as_deref(), Some("new_done"));
}

#[test]
fn augmentation_job_reads_validate_identity_and_status_filter() {
    let db = Database::new();

    let job_error = db
        .knowledge_augmentation_job(&KnowledgeAugmentationJobRequest {
            job_id: String::new(),
        })
        .unwrap_err();
    assert!(job_error.to_string().contains("non-empty job id"));

    let list_error = db
        .knowledge_augmentation_jobs(&KnowledgeAugmentationJobListRequest {
            status_filter: Some(String::new()),
            order_by: KnowledgeAugmentationJobListOrder::StartedAtDesc,
            limit: 10,
        })
        .unwrap_err();
    assert!(list_error.to_string().contains("non-empty status filter"));
}

#[test]
fn interrupts_pending_and_running_augmentation_jobs_for_nowledge_orphans() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
        .unwrap();
    db.query(
        "CREATE (:AugmentationJob {job_id: 'completed_job', status: 'completed', message: 'done'})",
    )
    .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'failed_job', status: 'failed', message: 'old failure'})")
        .unwrap();

    let output = db
        .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
            error_message: "stale owner".to_string(),
            completed_at: Value::Int(300),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.interrupted_count, 2);
    assert_eq!(output.updated_property_count, 8);
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].job_id.as_deref(), Some("pending_job"));
    assert_eq!(output.rows[0].previous_status, "pending");
    assert_eq!(output.rows[1].job_id.as_deref(), Some("running_job"));
    assert_eq!(output.rows[1].previous_status, "running");

    let rows = db
        .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.message AS message, j.error_message AS error, j.completed_at AS completed_at ORDER BY id ASC")
        .unwrap();
    let completed = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("completed_job".to_string())))
        .unwrap();
    assert_eq!(
        completed.get("status"),
        Some(&Value::String("completed".to_string()))
    );
    assert_eq!(
        completed.get("message"),
        Some(&Value::String("done".to_string()))
    );

    for job_id in ["pending_job", "running_job"] {
        let row = rows
            .rows
            .iter()
            .find(|row| row.get("id") == Some(&Value::String(job_id.to_string())))
            .unwrap();
        assert_eq!(
            row.get("status"),
            Some(&Value::String("failed".to_string()))
        );
        assert_eq!(
            row.get("message"),
            Some(&Value::String("Interrupted before completion".to_string()))
        );
        assert_eq!(
            row.get("error"),
            Some(&Value::String("stale owner".to_string()))
        );
        assert_eq!(row.get("completed_at"), Some(&Value::Int(300)));
    }
}

#[test]
fn augmentation_job_interrupt_without_candidates_does_not_write_wal() {
    let path = unique_test_dir("augmentation_job_interrupt_without_candidates");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'completed_job', status: 'completed'})")
            .unwrap();
    }
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let output = db
            .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
                error_message: "no candidates".to_string(),
                completed_at: Value::Int(301),
            })
            .unwrap();
        assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
        assert_eq!(output.graph_commit_epoch_after, graph_commit_epoch_before);
        assert_eq!(output.candidate_count, 0);
        assert_eq!(output.interrupted_count, 0);
        assert_eq!(output.updated_property_count, 0);
    }
    let wal_after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn augmentation_job_interrupt_rejects_empty_reason_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
            error_message: String::new(),
            completed_at: Value::Int(302),
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty error message"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_augmentation_job_interrupt_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_augmentation_job_interrupt_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
            .unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
            .unwrap();
    }
    let setup_wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
                error_message: "shutdown".to_string(),
                completed_at: Value::Int(303),
            })
            .unwrap();
        assert_eq!(output.interrupted_count, 2);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.message AS message, j.error_message AS error, j.completed_at AS completed_at ORDER BY id ASC")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        for row in &rows.rows {
            assert_eq!(
                row.get("status"),
                Some(&Value::String("failed".to_string()))
            );
            assert_eq!(
                row.get("message"),
                Some(&Value::String("Interrupted before completion".to_string()))
            );
            assert_eq!(
                row.get("error"),
                Some(&Value::String("shutdown".to_string()))
            );
            assert_eq!(row.get("completed_at"), Some(&Value::Int(303)));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scoped_knowledge_property_batch_update_does_not_write_filtered_rows() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Old 1', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory_2', title: 'Old 2', source_id: 'thread_2', space_id: ''})",
    )
    .unwrap();

    let output = db
        .update_scoped_knowledge_properties_batch(&KnowledgeScopedPropertyUpdateBatchRequest {
            updates: vec![
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 1".to_string()),
                    )]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 2".to_string()),
                    )]),
                },
            ],
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.updated_property_count, 1);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].filtered_out);
    let row = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        property_names: vec!["title".to_string()],
    });
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("New 1".to_string())))
    );
    assert_eq!(
        row.rows[1].properties.get("title"),
        Some(&Some(Value::String("Old 2".to_string())))
    );
}

#[test]
fn knowledge_property_batch_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let error = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::from([(
                    "bad-name".to_string(),
                    Value::String("New".to_string()),
                )]),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("property identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_property_batch_update_rejects_empty_assignment_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let error = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::new(),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("at least one assignment"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_property_batch_update_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();

    let output = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                assignments: BTreeMap::from([(
                    "title".to_string(),
                    Value::String("New".to_string()),
                )]),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.updated_property_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_property_batch_update() {
    let path = unique_test_dir("read_only_typed_knowledge_property_batch_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
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
            .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
                updates: vec![KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New".to_string()),
                    )]),
                }],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_property_batch_update_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_property_batch_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2'})")
            .unwrap();
        db.update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 1".to_string()),
                    )]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 2".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    assert_eq!(wal.matches("\tbatch\t").count(), 1);
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_property_batch(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["title".to_string()],
        });
        assert_eq!(
            output.rows[0].properties.get("title"),
            Some(&Some(Value::String("New 1".to_string())))
        );
        assert_eq!(
            output.rows[1].properties.get("title"),
            Some(&Some(Value::String("New 2".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

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
        .entity
        .is_none());
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        })
        .entity
        .is_some());
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        }],
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Incoming,
        limit_per_seed: 4,
    });
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
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .knowledge_entity(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            })
            .entity
            .is_none());
        assert!(db
            .knowledge_entity(&KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            })
            .entity
            .is_some());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_knowledge_entity_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})-[:MENTIONS]->(:Entity {id: 'entity_2'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_3', source_id: 'thread_1'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec![
                "memory_2".to_string(),
                "missing".to_string(),
                "memory_1".to_string(),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.deleted_node_count, 2);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].external_id, "memory_2");
    assert!(output.rows[0].matched);
    assert_eq!(output.rows[1].external_id, "missing");
    assert!(!output.rows[1].matched);
    assert_eq!(output.rows[1].node_id, None);
    assert_eq!(output.rows[2].external_id, "memory_1");
    assert!(output.rows[2].matched);
    for external_id in ["memory_1", "memory_2"] {
        assert!(db
            .knowledge_entity(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: external_id.to_string(),
            })
            .entity
            .is_none());
    }
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_3".to_string(),
        })
        .entity
        .is_some());
    assert_eq!(
        db.query("MATCH (a)-[r]->(b) RETURN count(r) AS relationships")
            .unwrap()
            .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
}

#[test]
fn scoped_knowledge_entity_batch_delete_does_not_write_filtered_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_entity_batch(&KnowledgeScopedEntityDeleteBatchRequest {
            delete: KnowledgeEntityDeleteBatchRequest {
                label: "Memory".to_string(),
                external_ids: vec!["memory_1".to_string(), "memory_2".to_string()],
            },
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.deleted_node_count, 1);
    assert!(output.rows[0].matched);
    assert!(!output.rows[0].filtered_out);
    assert!(!output.rows[1].matched);
    assert!(output.rows[1].filtered_out);
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .entity
        .is_none());
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_2".to_string(),
        })
        .entity
        .is_some());
}

#[test]
fn knowledge_entity_batch_delete_deduplicates_writes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();

    let output = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec!["memory_1".to_string(), "memory_1".to_string()],
        })
        .unwrap();

    assert_eq!(output.matched_count, 2);
    assert_eq!(output.deleted_node_count, 1);
    assert_eq!(output.rows.len(), 2);
    assert!(output.rows.iter().all(|row| row.matched));
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .entity
        .is_none());
}

#[test]
fn knowledge_entity_batch_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec!["0".to_string()],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_node_count, 0);
    assert!(output.rows[0].non_writable);
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "0".to_string(),
        })
        .entity
        .is_some());
}

#[test]
fn knowledge_entity_batch_delete_rejects_invalid_identifier() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();

    let error = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Bad-Label".to_string(),
            external_ids: vec!["memory_1".to_string()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("label identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_batch_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_batch_delete");
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
            .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
                label: "Memory".to_string(),
                external_ids: vec!["memory_1".to_string()],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_batch_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_entity_batch_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec!["memory_1".to_string(), "memory_2".to_string()],
        })
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        for external_id in ["memory_1", "memory_2"] {
            assert!(db
                .knowledge_entity(&KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: external_id.to_string(),
                })
                .entity
                .is_none());
        }
        assert!(db
            .knowledge_entity(&KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            })
            .entity
            .is_some());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn creates_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Skein'})")
        .unwrap();

    let output = db
        .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            properties: BTreeMap::from([
                ("confidence".to_string(), Value::Float(0.9)),
                ("mention_count".to_string(), Value::Int(1)),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(output.matched);
    assert!(!output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.created_relationship_count, 1);

    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(
        relationships.groups[0].relationships[0]
            .target_external_id
            .as_deref(),
        Some("entity_1")
    );
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("mention_count"),
        Some(&Value::Int(1))
    );
}

#[test]
fn scoped_knowledge_relationship_create_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Skein', space_id: 'default'})")
        .unwrap();

    let output = db
        .create_scoped_knowledge_relationship(&KnowledgeScopedRelationshipCreateRequest {
            create: KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                properties: BTreeMap::new(),
            },
            source_metadata_filters: BTreeMap::from([(
                "source_id".to_string(),
                "thread_2".to_string(),
            )]),
            target_metadata_filters: BTreeMap::from([(
                "space_id".to_string(),
                "default".to_string(),
            )]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert!(output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.created_relationship_count, 0);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 0);
}

#[test]
fn knowledge_relationship_create_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let error = db
        .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS-WITH-DASH".to_string(),
            properties: BTreeMap::new(),
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 2);
}

#[test]
fn knowledge_relationship_create_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "0".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            properties: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert_eq!(output.created_relationship_count, 0);
}

#[test]
fn upserts_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();

    let created = db
        .upsert_knowledge_relationship(&KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::from([
                (
                    "assigned_by".to_string(),
                    Value::String("system".to_string()),
                ),
                (
                    "created_at".to_string(),
                    Value::String("2026-07-19".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(created.graph_commit_epoch_before, 2);
    assert_eq!(created.graph_commit_epoch_after, 3);
    assert_eq!(created.source_node_id, Some(0));
    assert_eq!(created.target_node_id, Some(1));
    assert!(created.matched);
    assert!(created.created);
    assert!(!created.already_exists);
    assert_eq!(created.created_relationship_count, 1);
    assert_eq!(created.relationship_id, Some(0));

    let epoch_before_second = db.store.commit_epoch();
    let existing = db
        .upsert_knowledge_relationship(&KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::from([(
                "assigned_by".to_string(),
                Value::String("ignored".to_string()),
            )]),
        })
        .unwrap();

    assert_eq!(existing.graph_commit_epoch_before, epoch_before_second);
    assert_eq!(existing.graph_commit_epoch_after, epoch_before_second);
    assert!(existing.matched);
    assert!(!existing.created);
    assert!(existing.already_exists);
    assert_eq!(existing.relationship_id, Some(0));
    assert_eq!(existing.created_relationship_count, 0);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 10,
    });
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("assigned_by"),
        Some(&Value::String("system".to_string()))
    );
}

#[test]
fn knowledge_relationship_upsert_does_not_write_projected_idless_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();

    let output = db
        .upsert_knowledge_relationship(&KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "0".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert!(output.non_writable);
    assert_eq!(output.created_relationship_count, 0);
}

#[test]
fn upserts_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'label_1'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'existing'}]->(l)")
        .unwrap();

    let output = db
        .upsert_knowledge_relationship_batch(&KnowledgeRelationshipUpsertBatchRequest {
            upserts: vec![
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("ignored".to_string()),
                    )]),
                },
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("created".to_string()),
                    )]),
                },
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("duplicate".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 2);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].already_exists);
    assert_eq!(output.rows[0].relationship_id, Some(0));
    assert!(output.rows[1].created);
    assert!(output.rows[1].relationship_id.is_some());
    assert!(output.rows[2].already_exists);
    assert_eq!(output.rows[2].relationship_id, None);

    for memory_id in ["memory_1", "memory_2"] {
        let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: memory_id.to_string(),
            }],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 10,
        });
        assert_eq!(relationships.relationship_count, 1);
    }
}

#[test]
fn typed_knowledge_relationship_batch_upsert_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_upsert_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.query("CREATE (:Label {id: 'label_1'})").unwrap();
        let batch_count_before_upsert = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.upsert_knowledge_relationship_batch(&KnowledgeRelationshipUpsertBatchRequest {
            upserts: vec![
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("system".to_string()),
                    )]),
                },
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("system".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_upsert = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_upsert, batch_count_before_upsert + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let db = Database::open(&path).unwrap();
        let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 10,
        });
        assert_eq!(relationships.relationship_count, 2);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_create() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_create");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
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
            .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                properties: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_create_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_relationship_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.7))]),
        })
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            relationship_type: Some("MENTIONS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        });
        assert_eq!(output.relationship_count, 1);
        assert_eq!(
            output.groups[0].relationships[0]
                .relationship_properties
                .get("confidence"),
            Some(&Value::Float(0.7))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn creates_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Skein'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_2', name: 'Graph'})")
        .unwrap();

    let output = db
        .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.9))]),
                },
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.7))]),
                },
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::new(),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.source_filtered_out_count, 0);
    assert_eq!(output.target_filtered_out_count, 0);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.created_relationship_count, 2);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].matched);
    assert!(!output.rows[2].matched);
    assert_eq!(output.rows[2].source_node_id, None);
    assert_eq!(output.rows[2].target_node_id, Some(2));

    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 2);
    assert_eq!(relationships.groups[0].relationships.len(), 1);
    assert_eq!(relationships.groups[1].relationships.len(), 1);
}

#[test]
fn scoped_knowledge_relationship_batch_create_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', space_id: 'default'})")
        .unwrap();

    let output = db
        .create_scoped_knowledge_relationship_batch(
            &KnowledgeScopedRelationshipCreateBatchRequest {
                creates: vec![
                    KnowledgeRelationshipCreateRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_1".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Entity".to_string(),
                            external_id: "entity_1".to_string(),
                        },
                        relationship_type: "MENTIONS".to_string(),
                        properties: BTreeMap::new(),
                    },
                    KnowledgeRelationshipCreateRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_2".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Entity".to_string(),
                            external_id: "entity_1".to_string(),
                        },
                        relationship_type: "MENTIONS".to_string(),
                        properties: BTreeMap::new(),
                    },
                ],
                source_metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                target_metadata_filters: BTreeMap::from([(
                    "space_id".to_string(),
                    "default".to_string(),
                )]),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.source_filtered_out_count, 1);
    assert_eq!(output.target_filtered_out_count, 0);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[1].source_filtered_out);

    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(relationships.groups[0].relationships.len(), 1);
    assert!(relationships.groups[1].relationships.is_empty());
}

#[test]
fn knowledge_relationship_batch_create_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let error = db
        .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS-WITH-DASH".to_string(),
                properties: BTreeMap::new(),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 2);
}

#[test]
fn knowledge_relationship_batch_create_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                properties: BTreeMap::new(),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.created_relationship_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_batch_create() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_batch_create");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
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
            .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
                creates: vec![KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::new(),
                }],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_batch_create_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_2'})").unwrap();
        db.create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.7))]),
                },
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.8))]),
                },
            ],
        })
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), 1);
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("MENTIONS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        });
        assert_eq!(output.relationship_count, 2);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn updates_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw', weight: 1}]->(:Entity {id: 'entity_1'})")
        .unwrap();

    let output = db
        .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            relationship_properties: BTreeMap::from([(
                "source_reference".to_string(),
                Value::String("raw".to_string()),
            )]),
            assignments: BTreeMap::from([
                ("weight".to_string(), Value::Int(9)),
                (
                    "review_status".to_string(),
                    Value::String("approved".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(output.matched);
    assert_eq!(output.updated_relationship_count, 1);
    assert_eq!(output.updated_property_count, 2);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(9))
    );
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("review_status"),
        Some(&Value::String("approved".to_string()))
    );
}

#[test]
fn scoped_knowledge_relationship_update_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL {weight: 1}]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();

    let output = db
        .update_scoped_knowledge_relationship(&KnowledgeScopedRelationshipUpdateRequest {
            update: KnowledgeRelationshipUpdateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS_LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
                assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
            },
            source_metadata_filters: BTreeMap::from([(
                "source_id".to_string(),
                "thread_2".to_string(),
            )]),
            target_metadata_filters: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert!(!output.matched);
    assert!(output.source_filtered_out);
    assert_eq!(output.updated_relationship_count, 0);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(1))
    );
}

#[test]
fn updates_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw_1', weight: 1}]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw_2', weight: 2}]->(:Entity {id: 'entity_2'})")
        .unwrap();

    let output = db
        .update_knowledge_relationship_batch(&KnowledgeRelationshipUpdateBatchRequest {
            updates: vec![
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::from([(
                        "source_reference".to_string(),
                        Value::String("raw_1".to_string()),
                    )]),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
                },
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::from([(
                        "source_reference".to_string(),
                        Value::String("raw_2".to_string()),
                    )]),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(8))]),
                },
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::new(),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(7))]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_relationship_count, 2);
    assert_eq!(output.updated_property_count, 2);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].matched);
    assert!(!output.rows[2].matched);

    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(9))
    );
    assert_eq!(
        relationships.groups[1].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(8))
    );
}

#[test]
fn scoped_knowledge_relationship_batch_update_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL {weight: 1}]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})-[:HAS_LABEL {weight: 2}]->(:Label {id: 'label_2', name: 'Rust'})")
        .unwrap();

    let output = db
        .update_scoped_knowledge_relationship_batch(
            &KnowledgeScopedRelationshipUpdateBatchRequest {
                updates: vec![
                    KnowledgeRelationshipUpdateRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_1".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Label".to_string(),
                            external_id: "label_1".to_string(),
                        },
                        relationship_type: "HAS_LABEL".to_string(),
                        relationship_properties: BTreeMap::new(),
                        assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
                    },
                    KnowledgeRelationshipUpdateRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_2".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Label".to_string(),
                            external_id: "label_2".to_string(),
                        },
                        relationship_type: "HAS_LABEL".to_string(),
                        relationship_properties: BTreeMap::new(),
                        assignments: BTreeMap::from([("weight".to_string(), Value::Int(8))]),
                    },
                ],
                source_metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                target_metadata_filters: BTreeMap::new(),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.source_filtered_out_count, 1);
    assert_eq!(output.updated_relationship_count, 1);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].source_filtered_out);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(9))
    );
    assert_eq!(
        relationships.groups[1].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(2))
    );
}

#[test]
fn knowledge_relationship_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
        .unwrap();

    let error = db
        .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            relationship_properties: BTreeMap::new(),
            assignments: BTreeMap::from([("bad-name".to_string(), Value::Int(1))]),
        })
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("relationship property identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_relationship_update_rejects_empty_assignments() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
        .unwrap();

    let error = db
        .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            relationship_properties: BTreeMap::new(),
            assignments: BTreeMap::new(),
        })
        .unwrap_err();

    assert!(error.to_string().contains("at least one assignment"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_relationship_batch_update_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .update_knowledge_relationship_batch(&KnowledgeRelationshipUpdateBatchRequest {
            updates: vec![KnowledgeRelationshipUpdateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                relationship_properties: BTreeMap::new(),
                assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.updated_relationship_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_update() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
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
            .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                relationship_properties: BTreeMap::new(),
                assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_batch_update_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {weight: 1}]->(:Entity {id: 'entity_1'})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory_2'})-[:MENTIONS {weight: 2}]->(:Entity {id: 'entity_2'})",
        )
        .unwrap();
        let batch_count_before_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        db.update_knowledge_relationship_batch(&KnowledgeRelationshipUpdateBatchRequest {
            updates: vec![
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::new(),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
                },
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::new(),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(8))]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = std::fs::read_to_string(path.join("wal.skein"))
            .unwrap()
            .matches("\tbatch\t")
            .count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_rel_property"));
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("MENTIONS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        });
        assert_eq!(
            output.groups[0].relationships[0]
                .relationship_properties
                .get("weight"),
            Some(&Value::Int(9))
        );
        assert_eq!(
            output.groups[1].relationships[0]
                .relationship_properties
                .get("weight"),
            Some(&Value::Int(8))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();

    let output = db
        .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            relationship_properties: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(output.matched);
    assert!(!output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.deleted_relationship_count, 1);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 0);
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .entity
        .is_some());
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Label".to_string(),
            external_id: "label_1".to_string(),
        })
        .entity
        .is_some());
}

#[test]
fn typed_knowledge_relationship_delete_filters_relationship_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {source_reference: 'keep'}]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_1'}) CREATE (m)-[:MENTIONS {source_reference: 'drop'}]->(e)")
        .unwrap();

    let output = db
        .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            relationship_properties: BTreeMap::from([(
                "source_reference".to_string(),
                Value::String("drop".to_string()),
            )]),
        })
        .unwrap();

    assert_eq!(output.deleted_relationship_count, 1);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("source_reference"),
        Some(&Value::String("keep".to_string()))
    );
}

#[test]
fn scoped_knowledge_relationship_delete_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_relationship(&KnowledgeScopedRelationshipDeleteRequest {
            delete: KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS_LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
            },
            source_metadata_filters: BTreeMap::from([(
                "source_id".to_string(),
                "thread_2".to_string(),
            )]),
            target_metadata_filters: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert!(output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.deleted_relationship_count, 0);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 1);
}

#[test]
fn knowledge_relationship_delete_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
        .unwrap();

    let error = db
        .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS-LABEL".to_string(),
            relationship_properties: BTreeMap::new(),
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_delete");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
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
            .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS_LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_relationship_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        db.delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            relationship_properties: BTreeMap::new(),
        })
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_rel"));
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        });
        assert_eq!(output.relationship_count, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn deletes_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_2'})")
        .unwrap();

    let output = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_2".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.deleted_relationship_count, 2);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].matched);
    assert!(!output.rows[2].matched);
    assert_eq!(output.rows[2].source_node_id, None);
    assert_eq!(output.rows[2].target_node_id, Some(1));

    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 0);
    assert!(db
        .knowledge_entity(&KnowledgeEntityRequest {
            label: "Label".to_string(),
            external_id: "label_1".to_string(),
        })
        .entity
        .is_some());
}

#[test]
fn typed_knowledge_relationship_batch_delete_filters_relationship_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {source_reference: 'keep'}]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_1'}) CREATE (m)-[:MENTIONS {source_reference: 'drop'}]->(e)")
        .unwrap();

    let output = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                relationship_properties: BTreeMap::from([(
                    "source_reference".to_string(),
                    Value::String("drop".to_string()),
                )]),
            }],
        })
        .unwrap();

    assert_eq!(output.matched_count, 1);
    assert_eq!(output.deleted_relationship_count, 1);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("MENTIONS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("source_reference"),
        Some(&Value::String("keep".to_string()))
    );
}

#[test]
fn scoped_knowledge_relationship_batch_delete_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_2', name: 'Rust'})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_relationship_batch(
            &KnowledgeScopedRelationshipDeleteBatchRequest {
                deletes: vec![
                    KnowledgeRelationshipDeleteRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_1".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Label".to_string(),
                            external_id: "label_1".to_string(),
                        },
                        relationship_type: "HAS_LABEL".to_string(),
                        relationship_properties: BTreeMap::new(),
                    },
                    KnowledgeRelationshipDeleteRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_2".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Label".to_string(),
                            external_id: "label_2".to_string(),
                        },
                        relationship_type: "HAS_LABEL".to_string(),
                        relationship_properties: BTreeMap::new(),
                    },
                ],
                source_metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                target_metadata_filters: BTreeMap::new(),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.source_filtered_out_count, 1);
    assert_eq!(output.deleted_relationship_count, 1);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[1].source_filtered_out);
    let relationships = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
        ],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });
    assert_eq!(relationships.relationship_count, 1);
    assert!(relationships.groups[0].relationships.is_empty());
    assert_eq!(relationships.groups[1].relationships.len(), 1);
}

#[test]
fn knowledge_relationship_batch_delete_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
        .unwrap();

    let error = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS-LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_relationship_batch_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();

    let output = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS_LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_relationship_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_batch_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_batch_delete");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
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
            .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
                deletes: vec![KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                }],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_batch_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})-[:HAS_LABEL]->(:Label {id: 'label_2'})")
            .unwrap();
    }
    let setup_wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        db.delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_2".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
            ],
        })
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let db = Database::open(&path).unwrap();
        let output = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        });
        assert_eq!(output.relationship_count, 0);
    }
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
    let wal_before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
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
    let wal_after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
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
    let setup_wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
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
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
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

#[test]
fn knowledge_entity_returns_none_for_missing_seed() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Skein'})")
        .unwrap();

    let output = db.knowledge_entity(&KnowledgeEntityRequest {
        label: "Entity".to_string(),
        external_id: "missing".to_string(),
    });

    assert_eq!(output.graph_commit_epoch, 1);
    assert!(output.entity.is_none());
}

#[test]
fn knowledge_entity_uses_projected_identity_for_idless_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {name: 'Anonymous entity', kind: 'concept'})")
        .unwrap();

    let output = db.knowledge_entity(&KnowledgeEntityRequest {
        label: "Entity".to_string(),
        external_id: "0".to_string(),
    });

    let entity = output.entity.expect("expected entity");
    assert_eq!(output.graph_commit_epoch, 1);
    assert_eq!(entity.node_id, 0);
    assert_eq!(entity.external_id.as_deref(), Some("0"));
    assert_eq!(
        entity.properties.get("name"),
        Some(&Value::String("Anonymous entity".to_string()))
    );
}

#[test]
fn retrieves_knowledge_neighbors_without_search_projection() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    let mention = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("mention".to_string())),
                ("name".to_string(), Value::String("Mention".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(1),
            leaf,
            "LINKS",
            BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            mention,
            NodeId(0),
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let outgoing = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit: 8,
        max_hops: 2,
    });
    assert_eq!(outgoing.seed_node_id, Some(0));
    assert_eq!(outgoing.graph_commit_epoch, 5);
    assert_eq!(outgoing.paths.len(), 2);
    assert!(outgoing.diagnostics.seed_found);
    assert_eq!(outgoing.diagnostics.target_found, None);
    assert_eq!(outgoing.diagnostics.path_count, 2);
    assert_eq!(outgoing.diagnostics.node_count, 3);
    assert_eq!(outgoing.diagnostics.relationship_count, 2);
    assert_eq!(outgoing.diagnostics.fanout_reason_count, 0);
    assert_eq!(outgoing.diagnostics.max_hops, 2);
    assert_eq!(outgoing.diagnostics.path_limit, Some(8));
    assert_eq!(
        outgoing.diagnostics.input_candidate_set.representation,
        "traversal_seed_node_ids"
    );
    assert_eq!(outgoing.diagnostics.input_candidate_set.cardinality, 1);
    assert_eq!(
        outgoing.diagnostics.candidate_set.id_space,
        "canonical_graph_relationship_id"
    );
    assert_eq!(
        outgoing.diagnostics.candidate_set.representation,
        "neighbor_relationship_ids"
    );
    assert_eq!(outgoing.diagnostics.candidate_set.cardinality, 2);
    assert_eq!(
        outgoing
            .diagnostics
            .candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(outgoing.graph_commit_epoch)
    );
    assert!(outgoing.fanout_reasons.is_empty());
    assert!(outgoing
        .paths
        .iter()
        .all(|path| path.relationship_type == "LINKS"));
    assert!(outgoing
        .paths
        .iter()
        .all(|path| { path.direction == KnowledgeGraphPathDirection::Outgoing }));
    assert!(outgoing.paths.iter().any(|path| path.hop == 2
        && path.source_external_id.as_deref() == Some("mid")
        && path.target_external_id.as_deref() == Some("leaf")
        && path.relationship_properties.get("weight") == Some(&Value::Int(2))));

    let incoming = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Incoming,
        limit: 8,
        max_hops: 1,
    });
    assert_eq!(incoming.paths.len(), 1);
    assert_eq!(incoming.paths[0].relationship_type, "MENTIONS");
    assert_eq!(
        incoming.paths[0].source_external_id.as_deref(),
        Some("mention")
    );

    let unknown_type = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("DOES_NOT_EXIST".to_string()),
        direction: KnowledgeNeighborDirection::Both,
        limit: 8,
        max_hops: 2,
    });
    assert_eq!(unknown_type.seed_node_id, Some(0));
    assert!(unknown_type.paths.is_empty());
    assert!(unknown_type.diagnostics.seed_found);
    assert_eq!(unknown_type.diagnostics.path_count, 0);
    assert_eq!(unknown_type.diagnostics.node_count, 0);
    assert_eq!(unknown_type.diagnostics.relationship_count, 0);
    assert!(unknown_type.fanout_reasons.is_empty());
    assert_eq!(
        unknown_type.diagnostics.fallback_reasons,
        vec!["relationship type DOES_NOT_EXIST not found".to_string()]
    );
    assert_eq!(
        unknown_type.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound]
    );
}

#[test]
fn scoped_knowledge_neighbors_filters_seed_by_metadata() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root', space_id: ''})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
    )
    .unwrap();

    let scoped = db.knowledge_scoped_neighbors(&KnowledgeScopedNeighborsRequest {
        navigation: KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: 4,
            max_hops: 1,
        },
        metadata_filters: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
    });

    assert_eq!(scoped.paths.len(), 1);
    assert!(scoped.diagnostics.seed_found);
    assert_eq!(scoped.diagnostics.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("space_id")
            .map(String::as_str),
        Some("default")
    );

    let filtered = db.knowledge_scoped_neighbors(&KnowledgeScopedNeighborsRequest {
        navigation: KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: 4,
            max_hops: 1,
        },
        metadata_filters: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
    });

    assert_eq!(filtered.seed_node_id, Some(0));
    assert!(filtered.paths.is_empty());
    assert!(!filtered.diagnostics.seed_found);
    assert_eq!(
        filtered.diagnostics.input_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        filtered
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("space_id")
            .map(String::as_str),
        Some("team")
    );
}

#[test]
fn retrieves_knowledge_relationships_grouped_by_seed() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First'})-[:HAS_LABEL {weight: 3}]->(:Label {id: 'label_1', name: 'Database'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory_2', title: 'Second'})-[:HAS_LABEL]->(:Label {id: 'label_2', name: 'Rust'})",
    )
    .unwrap();

    let output = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_2".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "missing".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
        ],
        relationship_type: Some("HAS_LABEL".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 4,
    });

    assert_eq!(output.graph_commit_epoch, 2);
    assert!(output.relationship_type_found);
    assert_eq!(output.groups.len(), 3);
    assert_eq!(output.found_seed_count, 2);
    assert_eq!(output.missing_seed_count, 1);
    assert_eq!(output.filtered_out_seed_count, 0);
    assert_eq!(output.relationship_count, 2);
    assert_eq!(output.groups[0].seed.external_id, "memory_2");
    assert_eq!(output.groups[0].relationships.len(), 1);
    assert_eq!(
        output.groups[0].relationships[0]
            .target_external_id
            .as_deref(),
        Some("label_2")
    );
    assert_eq!(output.groups[1].seed.external_id, "missing");
    assert!(output.groups[1].relationships.is_empty());
    assert_eq!(output.groups[2].relationships.len(), 1);
    assert_eq!(
        output.groups[2].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(3))
    );
}

#[test]
fn scoped_knowledge_relationships_report_filtered_seeds() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory_2', title: 'Second', source_id: 'thread_2', space_id: 'default'})-[:HAS_LABEL]->(:Label {id: 'label_2', name: 'Rust'})",
    )
    .unwrap();

    let output = db.knowledge_scoped_relationships(&KnowledgeScopedRelationshipsRequest {
        relationships: KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        },
        metadata_filters: BTreeMap::from([
            ("source_id".to_string(), "thread_1".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    });

    assert_eq!(output.graph_commit_epoch, 2);
    assert!(output.relationship_type_found);
    assert_eq!(output.found_seed_count, 1);
    assert_eq!(output.missing_seed_count, 0);
    assert_eq!(output.filtered_out_seed_count, 1);
    assert_eq!(output.relationship_count, 1);
    assert!(!output.groups[0].filtered_out);
    assert_eq!(output.groups[0].relationships.len(), 1);
    assert!(output.groups[1].filtered_out);
    assert!(output.groups[1].relationships.is_empty());
}

#[test]
fn knowledge_relationships_fail_soft_for_unknown_relationship_type() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})")
        .unwrap();

    let output = db.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        }],
        relationship_type: Some("DOES_NOT_EXIST".to_string()),
        direction: KnowledgeNeighborDirection::Both,
        limit_per_seed: 4,
    });

    assert_eq!(output.graph_commit_epoch, 1);
    assert!(!output.relationship_type_found);
    assert_eq!(output.groups.len(), 1);
    assert_eq!(output.found_seed_count, 0);
    assert_eq!(output.missing_seed_count, 0);
    assert_eq!(output.filtered_out_seed_count, 0);
    assert_eq!(output.relationship_count, 0);
    assert!(output.groups[0].relationships.is_empty());
}

#[test]
fn typed_knowledge_navigation_uses_projected_identity_for_idless_seed() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {title: 'Anonymous root'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
    )
    .unwrap();

    let neighbors = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "0".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit: 4,
        max_hops: 1,
    });
    assert_eq!(neighbors.seed_node_id, Some(0));
    assert_eq!(neighbors.paths.len(), 1);
    assert_eq!(neighbors.paths[0].source_external_id.as_deref(), Some("0"));
    assert_eq!(
        neighbors.paths[0].target_external_id.as_deref(),
        Some("leaf")
    );

    let paths = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "0".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "leaf".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        limit: 4,
    });
    assert_eq!(paths.source_node_id, Some(0));
    assert_eq!(paths.target_node_id, Some(1));
    assert_eq!(paths.paths.len(), 1);
    assert_eq!(
        paths.paths[0].segments[0].source_external_id.as_deref(),
        Some("0")
    );

    let subgraph = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "0".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 4,
        relationship_limit: 4,
    });
    assert_eq!(subgraph.seed_node_id, Some(0));
    assert!(subgraph
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("0")));
    assert!(subgraph
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("leaf")));
}

#[test]
fn knowledge_neighbors_reports_limit_and_missing_seed() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'left', name: 'Left'})")
            .unwrap();
    let right = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("right".to_string())),
                ("name".to_string(), Value::String("Right".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), right, "LINKS", BTreeMap::new())
        .unwrap();

    let limited = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        limit: 1,
        max_hops: 1,
    });
    assert_eq!(limited.paths.len(), 1);
    assert!(limited.diagnostics.seed_found);
    assert_eq!(limited.diagnostics.path_count, 1);
    assert_eq!(limited.diagnostics.node_count, 2);
    assert_eq!(limited.diagnostics.relationship_count, 1);
    assert_eq!(limited.diagnostics.fanout_reason_count, 1);
    assert!(limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(limited.diagnostics.path_limit, Some(1));
    assert_eq!(limited.fanout_reasons.len(), 1);
    assert!(limited.fanout_reasons[0].contains("knowledge_neighbors limit 1"));
    assert_eq!(
        limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::PathLimitReached]
    );
    assert_eq!(
        limited.diagnostics.fanout_reason_codes,
        limited.fanout_reason_codes
    );
    assert_eq!(limited.diagnostics.fanout_reasons, limited.fanout_reasons);

    let disabled = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        limit: 0,
        max_hops: 1,
    });
    assert!(disabled.paths.is_empty());
    assert_eq!(disabled.diagnostics.fanout_reason_count, 1);
    assert_eq!(
        disabled.diagnostics.fallback_reasons,
        vec!["path traversal disabled by limit 0".to_string()]
    );
    assert_eq!(
        disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::PathLimitZero]
    );
    assert_eq!(disabled.diagnostics.fanout_reasons, disabled.fanout_reasons);

    let missing = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "missing".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        limit: 8,
        max_hops: 1,
    });
    assert_eq!(missing.seed_node_id, None);
    assert!(!missing.diagnostics.seed_found);
    assert_eq!(missing.diagnostics.path_count, 0);
    assert_eq!(missing.diagnostics.fanout_reason_count, 0);
    assert_eq!(
        missing.diagnostics.fallback_reasons,
        vec!["seed Memory:missing not found".to_string()]
    );
    assert_eq!(
        missing.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::SeedNotFound]
    );
    assert_eq!(missing.diagnostics.path_limit, Some(8));
    assert!(missing.paths.is_empty());
    assert!(missing.fanout_reasons.is_empty());
}

#[test]
fn typed_knowledge_navigation_reports_dense_adjacency_groups() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})")
        .unwrap();
    for index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
        let target = db
            .store
            .create_node(
                &mut db.catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String(format!("entity-{index}"))),
                    ("name".to_string(), Value::String(format!("Entity {index}"))),
                ]),
            )
            .unwrap();
        db.store
            .create_relationship(&mut db.catalog, NodeId(0), target, "LINKS", BTreeMap::new())
            .unwrap();
    }

    let neighbors = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
        max_hops: 1,
    });
    assert_eq!(neighbors.paths.len(), DENSE_ADJACENCY_DEGREE_THRESHOLD);
    assert_eq!(neighbors.diagnostics.fanout_reason_count, 1);
    assert_eq!(neighbors.fanout_reasons.len(), 1);
    assert!(neighbors.fanout_reasons[0]
        .contains("knowledge_neighbors dense_adjacency LINKS outgoing node 0 degree"));
    assert_eq!(
        neighbors.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::DenseAdjacency]
    );
    assert_eq!(
        neighbors.fanout_reason_details[0].operation.as_deref(),
        Some("knowledge_neighbors")
    );
    assert_eq!(
        neighbors.fanout_reason_details[0]
            .relationship_type
            .as_deref(),
        Some("LINKS")
    );
    assert_eq!(
        neighbors.fanout_reason_details[0].direction.as_deref(),
        Some("outgoing")
    );
    assert_eq!(neighbors.fanout_reason_details[0].node_id, Some(0));
    assert_eq!(
        neighbors.fanout_reason_details[0].degree,
        Some(DENSE_ADJACENCY_DEGREE_THRESHOLD)
    );
    assert_eq!(
        neighbors.diagnostics.fanout_reason_codes,
        neighbors.fanout_reason_codes
    );
    assert_eq!(
        neighbors.diagnostics.fanout_reason_details,
        neighbors.fanout_reason_details
    );
    assert_eq!(
        neighbors.diagnostics.fanout_reasons,
        neighbors.fanout_reasons
    );

    let untyped_neighbors = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
        max_hops: 1,
    });
    assert_eq!(
        untyped_neighbors.paths.len(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
    assert_eq!(untyped_neighbors.diagnostics.fanout_reason_count, 1);
    assert!(untyped_neighbors.fanout_reasons[0]
        .contains("knowledge_neighbors dense_adjacency LINKS outgoing node 0 degree"));
    assert_eq!(
        untyped_neighbors.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::DenseAdjacency]
    );

    let subgraph = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: DENSE_ADJACENCY_DEGREE_THRESHOLD + 1,
        relationship_limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
    });
    assert_eq!(
        subgraph.relationships.len(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
    assert_eq!(subgraph.diagnostics.fanout_reason_count, 1);
    assert_eq!(subgraph.fanout_reasons.len(), 1);
    assert!(subgraph.fanout_reasons[0]
        .contains("knowledge_subgraph dense_adjacency LINKS outgoing node 0 degree"));
}

#[test]
fn retrieves_bounded_knowledge_paths_without_search_projection() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 1}]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(1),
            leaf,
            "LINKS",
            BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        )
        .unwrap();

    let output = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "leaf".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 2,
        limit: 4,
    });

    assert_eq!(output.graph_commit_epoch, 3);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(2));
    assert_eq!(output.paths.len(), 1);
    assert!(output.diagnostics.seed_found);
    assert_eq!(output.diagnostics.target_found, Some(true));
    assert_eq!(output.diagnostics.path_count, 1);
    assert_eq!(output.diagnostics.node_count, 3);
    assert_eq!(output.diagnostics.relationship_count, 2);
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.max_hops, 2);
    assert_eq!(output.diagnostics.path_limit, Some(4));
    assert_eq!(
        output.diagnostics.input_candidate_set.representation,
        "traversal_seed_node_ids"
    );
    assert_eq!(output.diagnostics.input_candidate_set.cardinality, 2);
    assert_eq!(
        output.diagnostics.candidate_set.id_space,
        "canonical_graph_path"
    );
    assert_eq!(
        output.diagnostics.candidate_set.representation,
        "bounded_paths"
    );
    assert_eq!(output.diagnostics.candidate_set.cardinality, 1);
    assert_eq!(
        output
            .diagnostics
            .input_candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert!(output.fanout_reasons.is_empty());
    let path = &output.paths[0];
    assert_eq!(path.segments.len(), 2);
    assert_eq!(path.segments[0].source_external_id.as_deref(), Some("root"));
    assert_eq!(path.segments[0].target_external_id.as_deref(), Some("mid"));
    assert_eq!(
        path.segments[0].relationship_properties.get("weight"),
        Some(&Value::Int(1))
    );
    assert_eq!(path.segments[1].source_external_id.as_deref(), Some("mid"));
    assert_eq!(path.segments[1].target_external_id.as_deref(), Some("leaf"));
    assert_eq!(
        path.segments[1].relationship_properties.get("weight"),
        Some(&Value::Int(2))
    );
    assert!(path
        .segments
        .iter()
        .all(|segment| segment.relationship_type == "LINKS"));
}

#[test]
fn scoped_knowledge_paths_filter_source_and_target_by_metadata() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root', source_id: 'thread_1'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf', space_id: 'default'})",
    )
    .unwrap();

    let scoped = db.knowledge_scoped_paths(&KnowledgeScopedPathRequest {
        navigation: KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "leaf".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            limit: 4,
        },
        source_metadata_filters: BTreeMap::from([(
            "source_id".to_string(),
            "thread_1".to_string(),
        )]),
        target_metadata_filters: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
    });

    assert_eq!(scoped.paths.len(), 1);
    assert!(scoped.diagnostics.seed_found);
    assert_eq!(scoped.diagnostics.target_found, Some(true));
    assert_eq!(scoped.diagnostics.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("source.source_id")
            .map(String::as_str),
        Some("thread_1")
    );
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("target.space_id")
            .map(String::as_str),
        Some("default")
    );

    let filtered = db.knowledge_scoped_paths(&KnowledgeScopedPathRequest {
        navigation: KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "leaf".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            limit: 4,
        },
        source_metadata_filters: BTreeMap::from([(
            "source_id".to_string(),
            "thread_1".to_string(),
        )]),
        target_metadata_filters: BTreeMap::from([("space_id".to_string(), "archive".to_string())]),
    });

    assert_eq!(filtered.source_node_id, Some(0));
    assert_eq!(filtered.target_node_id, Some(1));
    assert!(filtered.diagnostics.seed_found);
    assert_eq!(filtered.diagnostics.target_found, Some(false));
    assert!(filtered.paths.is_empty());
    assert_eq!(
        filtered.diagnostics.input_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        filtered
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("target.space_id")
            .map(String::as_str),
        Some("archive")
    );
    assert!(filtered.diagnostics.fallback_reasons.is_empty());
}

#[test]
fn knowledge_paths_respects_direction_type_limit_and_missing_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'left', name: 'Left'})")
            .unwrap();
    let right = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("right".to_string())),
                ("name".to_string(), Value::String("Right".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), right, "LINKS", BTreeMap::new())
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            right,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let wrong_direction = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "right".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Incoming,
        max_hops: 1,
        limit: 4,
    });
    assert!(wrong_direction.paths.is_empty());
    assert!(wrong_direction.diagnostics.seed_found);
    assert_eq!(wrong_direction.diagnostics.target_found, Some(true));
    assert_eq!(wrong_direction.diagnostics.path_count, 0);

    let unknown_type = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "right".to_string(),
        relationship_type: Some("DOES_NOT_EXIST".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        limit: 4,
    });
    assert!(unknown_type.paths.is_empty());
    assert!(unknown_type.diagnostics.seed_found);
    assert_eq!(unknown_type.diagnostics.target_found, Some(true));
    assert_eq!(
        unknown_type.diagnostics.fallback_reasons,
        vec!["relationship type DOES_NOT_EXIST not found".to_string()]
    );
    assert_eq!(
        unknown_type.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound]
    );

    let limited = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "right".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        limit: 1,
    });
    assert_eq!(limited.paths.len(), 1);
    assert_eq!(limited.diagnostics.path_count, 1);
    assert_eq!(limited.diagnostics.node_count, 2);
    assert_eq!(limited.diagnostics.relationship_count, 1);
    assert_eq!(limited.diagnostics.fanout_reason_count, 1);
    assert!(limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(limited.diagnostics.path_limit, Some(1));
    assert_eq!(limited.fanout_reasons.len(), 1);
    assert!(limited.fanout_reasons[0].contains("knowledge_paths limit 1"));
    assert_eq!(
        limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::PathLimitReached]
    );
    assert_eq!(
        limited.diagnostics.fanout_reason_codes,
        limited.fanout_reason_codes
    );
    assert_eq!(limited.diagnostics.fanout_reasons, limited.fanout_reasons);

    let disabled = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "right".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 0,
        limit: 4,
    });
    assert!(disabled.paths.is_empty());
    assert_eq!(disabled.diagnostics.fanout_reason_count, 0);
    assert_eq!(
        disabled.diagnostics.fallback_reasons,
        vec!["traversal disabled by max_hops 0".to_string()]
    );
    assert_eq!(
        disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::MaxHopsZero]
    );

    let missing = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "missing".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        max_hops: 2,
        limit: 4,
    });
    assert_eq!(missing.source_node_id, Some(0));
    assert_eq!(missing.target_node_id, None);
    assert!(missing.diagnostics.seed_found);
    assert_eq!(missing.diagnostics.target_found, Some(false));
    assert_eq!(missing.diagnostics.path_count, 0);
    assert_eq!(
        missing.diagnostics.fallback_reasons,
        vec!["target Entity:missing not found".to_string()]
    );
    assert_eq!(
        missing.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::TargetNotFound]
    );
    assert!(missing.paths.is_empty());

    let missing_source = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "missing".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "right".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        max_hops: 2,
        limit: 4,
    });
    assert_eq!(missing_source.source_node_id, None);
    assert_eq!(missing_source.target_node_id, Some(2));
    assert!(!missing_source.diagnostics.seed_found);
    assert_eq!(missing_source.diagnostics.target_found, Some(true));
    assert_eq!(missing_source.diagnostics.path_count, 0);
    assert_eq!(
        missing_source.diagnostics.fallback_reasons,
        vec!["seed Memory:missing not found".to_string()]
    );
    assert_eq!(
        missing_source.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::SeedNotFound]
    );
    assert!(missing_source.paths.is_empty());

    let missing_both = db.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "missing-source".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "missing-target".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        max_hops: 2,
        limit: 4,
    });
    assert_eq!(missing_both.source_node_id, None);
    assert_eq!(missing_both.target_node_id, None);
    assert!(!missing_both.diagnostics.seed_found);
    assert_eq!(missing_both.diagnostics.target_found, Some(false));
    assert_eq!(missing_both.diagnostics.path_count, 0);
    assert_eq!(
        missing_both.diagnostics.fallback_reasons,
        vec![
            "seed Memory:missing-source not found".to_string(),
            "target Entity:missing-target not found".to_string()
        ]
    );
    assert_eq!(
        missing_both.diagnostics.fallback_reason_codes,
        vec![
            KnowledgeTraversalFallbackReasonCode::SeedNotFound,
            KnowledgeTraversalFallbackReasonCode::TargetNotFound
        ]
    );
    assert!(missing_both.paths.is_empty());
}

#[test]
fn retrieves_bounded_knowledge_subgraph_without_search_projection() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    let mention = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("mention".to_string())),
                ("name".to_string(), Value::String("Mention".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(1), leaf, "LINKS", BTreeMap::new())
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            mention,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let output = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 2,
        node_limit: 8,
        relationship_limit: 8,
    });

    assert_eq!(output.graph_commit_epoch, 5);
    assert_eq!(output.seed_node_id, Some(0));
    assert_eq!(output.nodes.len(), 3);
    assert_eq!(output.relationships.len(), 2);
    assert!(output.diagnostics.seed_found);
    assert_eq!(output.diagnostics.target_found, None);
    assert_eq!(output.diagnostics.node_count, 3);
    assert_eq!(output.diagnostics.relationship_count, 2);
    assert_eq!(output.diagnostics.path_count, 2);
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.max_hops, 2);
    assert_eq!(output.diagnostics.node_limit, Some(8));
    assert_eq!(output.diagnostics.relationship_limit, Some(8));
    assert_eq!(
        output.diagnostics.input_candidate_set.representation,
        "traversal_seed_node_ids"
    );
    assert_eq!(output.diagnostics.input_candidate_set.cardinality, 1);
    assert_eq!(output.diagnostics.candidate_set.id_space, "mixed_graph_id");
    assert_eq!(
        output.diagnostics.candidate_set.representation,
        "subgraph_node_and_relationship_ids"
    );
    assert_eq!(output.diagnostics.candidate_set.cardinality, 5);
    assert_eq!(
        output
            .diagnostics
            .candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert!(output.fanout_reasons.is_empty());
    assert!(output
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("root")));
    assert!(output
        .nodes
        .iter()
        .any(|node| node.external_id.as_deref() == Some("leaf")));
    assert!(output
        .relationships
        .iter()
        .all(|relationship| relationship.relationship_type == "LINKS"));
    assert!(output
        .relationships
        .iter()
        .all(|relationship| relationship.direction == KnowledgeGraphPathDirection::Outgoing));
}

#[test]
fn scoped_knowledge_subgraph_filters_seed_by_metadata() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root', source_id: 'thread_1'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
    )
    .unwrap();

    let scoped = db.knowledge_scoped_subgraph(&KnowledgeScopedSubgraphRequest {
        navigation: KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 4,
            relationship_limit: 4,
        },
        metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
    });

    assert_eq!(scoped.nodes.len(), 2);
    assert_eq!(scoped.relationships.len(), 1);
    assert!(scoped.diagnostics.seed_found);
    assert_eq!(scoped.diagnostics.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        scoped
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("source_id")
            .map(String::as_str),
        Some("thread_1")
    );

    let filtered = db.knowledge_scoped_subgraph(&KnowledgeScopedSubgraphRequest {
        navigation: KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 4,
            relationship_limit: 4,
        },
        metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
    });

    assert_eq!(filtered.seed_node_id, Some(0));
    assert!(filtered.nodes.is_empty());
    assert!(filtered.relationships.is_empty());
    assert!(!filtered.diagnostics.seed_found);
    assert_eq!(
        filtered.diagnostics.input_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        filtered
            .diagnostics
            .input_candidate_set
            .metadata_filters
            .get("source_id")
            .map(String::as_str),
        Some("thread_2")
    );
    assert!(filtered.diagnostics.fallback_reasons.is_empty());
}

#[test]
fn knowledge_subgraph_reports_limits_and_missing_seed() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'left', name: 'Left'})")
            .unwrap();
    let right = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("right".to_string())),
                ("name".to_string(), Value::String("Right".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), right, "LINKS", BTreeMap::new())
        .unwrap();

    let node_limited = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 1,
        relationship_limit: 8,
    });
    assert_eq!(node_limited.nodes.len(), 1);
    assert!(node_limited.diagnostics.seed_found);
    assert_eq!(node_limited.diagnostics.node_count, 1);
    assert_eq!(node_limited.diagnostics.relationship_count, 0);
    assert_eq!(node_limited.diagnostics.fanout_reason_count, 1);
    assert!(node_limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(node_limited.diagnostics.node_limit, Some(1));
    assert!(node_limited.relationships.is_empty());
    assert!(node_limited.fanout_reasons[0].contains("node_limit 1"));
    assert_eq!(
        node_limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::NodeLimitReached]
    );
    assert_eq!(
        node_limited.diagnostics.fanout_reason_codes,
        node_limited.fanout_reason_codes
    );
    assert_eq!(
        node_limited.diagnostics.fanout_reasons,
        node_limited.fanout_reasons
    );

    let relationship_limited = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 8,
        relationship_limit: 1,
    });
    assert_eq!(relationship_limited.relationships.len(), 1);
    assert_eq!(relationship_limited.diagnostics.node_count, 2);
    assert_eq!(relationship_limited.diagnostics.relationship_count, 1);
    assert_eq!(relationship_limited.diagnostics.fanout_reason_count, 1);
    assert!(relationship_limited.diagnostics.fallback_reasons.is_empty());
    assert_eq!(relationship_limited.diagnostics.relationship_limit, Some(1));
    assert!(relationship_limited.fanout_reasons[0].contains("relationship_limit 1"));
    assert_eq!(
        relationship_limited.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::RelationshipLimitReached]
    );
    assert_eq!(
        relationship_limited.diagnostics.fanout_reason_codes,
        relationship_limited.fanout_reason_codes
    );
    assert_eq!(
        relationship_limited.diagnostics.fanout_reasons,
        relationship_limited.fanout_reasons
    );

    let node_disabled = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 0,
        relationship_limit: 8,
    });
    assert!(node_disabled.nodes.is_empty());
    assert!(node_disabled.relationships.is_empty());
    assert_eq!(
        node_disabled.diagnostics.fallback_reasons,
        vec!["subgraph traversal disabled by node_limit 0".to_string()]
    );
    assert_eq!(
        node_disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::NodeLimitZero]
    );

    let relationship_disabled = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 8,
        relationship_limit: 0,
    });
    assert_eq!(relationship_disabled.nodes.len(), 1);
    assert!(relationship_disabled.relationships.is_empty());
    assert_eq!(
        relationship_disabled.diagnostics.fallback_reasons,
        vec!["subgraph traversal disabled by relationship_limit 0".to_string()]
    );
    assert_eq!(
        relationship_disabled.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipLimitZero]
    );

    let unknown_type = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("DOES_NOT_EXIST".to_string()),
        direction: KnowledgeNeighborDirection::Both,
        max_hops: 1,
        node_limit: 8,
        relationship_limit: 8,
    });
    assert_eq!(unknown_type.seed_node_id, Some(0));
    assert!(unknown_type.diagnostics.seed_found);
    assert_eq!(unknown_type.diagnostics.node_count, 0);
    assert_eq!(unknown_type.diagnostics.relationship_count, 0);
    assert!(unknown_type.nodes.is_empty());
    assert!(unknown_type.relationships.is_empty());
    assert!(unknown_type.fanout_reasons.is_empty());
    assert_eq!(
        unknown_type.diagnostics.fallback_reasons,
        vec!["relationship type DOES_NOT_EXIST not found".to_string()]
    );
    assert_eq!(
        unknown_type.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound]
    );

    let missing = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "missing".to_string(),
        relationship_type: None,
        direction: KnowledgeNeighborDirection::Both,
        max_hops: 1,
        node_limit: 8,
        relationship_limit: 8,
    });
    assert_eq!(missing.seed_node_id, None);
    assert!(!missing.diagnostics.seed_found);
    assert_eq!(missing.diagnostics.node_count, 0);
    assert_eq!(missing.diagnostics.relationship_count, 0);
    assert_eq!(missing.diagnostics.fanout_reason_count, 0);
    assert_eq!(
        missing.diagnostics.fallback_reasons,
        vec!["seed Memory:missing not found".to_string()]
    );
    assert_eq!(
        missing.diagnostics.fallback_reason_codes,
        vec![KnowledgeTraversalFallbackReasonCode::SeedNotFound]
    );
    assert!(missing.nodes.is_empty());
    assert!(missing.relationships.is_empty());
    assert!(missing.fanout_reasons.is_empty());
}

#[test]
fn explains_query_with_optimizer_trace() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    let output = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert!(output.trace.groups >= 3);
    assert!(output.trace.selected_plan.contains("ProjectExec"));
    assert!(output.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(!output.trace.selected_plan.contains("FilterExec"));
    assert_eq!(
        output.trace.selected_plan_fingerprint,
        output.physical_plan.fingerprint()
    );
    assert!(output
        .trace
        .selected_plan_fingerprint
        .contains("IndexNodeSeek"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek")));
}

#[test]
fn plan_cache_reuses_exact_parameterized_physical_plan() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let mut parameters = BTreeMap::new();
    parameters.insert("id".to_string(), Value::Int(1));
    let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";

    let first = db.explain_query_with_params(query, &parameters).unwrap();
    let second = db.explain_query_with_params(query, &parameters).unwrap();

    assert!(
        first
            .trace
            .decisions
            .iter()
            .any(|decision| decision
                == "plan cache miss: optimized exact parameterized physical plan")
    );
    assert!(second
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache hit: exact parameterized physical plan"));
    assert_eq!(
        first.trace.selected_plan_fingerprint,
        second.trace.selected_plan_fingerprint
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 0);
}

#[test]
fn plan_cache_misses_after_graph_commit_epoch_changes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";

    db.explain_query(query).unwrap();
    db.explain_query(query).unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    let after_commit = db.explain_query(query).unwrap();

    assert!(
        after_commit
            .trace
            .decisions
            .iter()
            .any(|decision| decision
                == "plan cache miss: optimized exact parameterized physical plan")
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 1);
}

#[test]
fn plan_cache_misses_after_index_descriptor_changes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";

    let before_index = db.explain_query(query).unwrap();
    let cached_before_index = db.explain_query(query).unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let after_index = db.explain_query(query).unwrap();

    assert!(before_index.trace.selected_plan.contains("SeqNodeScan"));
    assert!(!before_index.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(cached_before_index
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache hit: exact parameterized physical plan"));
    assert!(
        after_index
            .trace
            .decisions
            .iter()
            .any(|decision| decision
                == "plan cache miss: optimized exact parameterized physical plan")
    );
    assert!(after_index.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(!after_index.trace.selected_plan.contains("SeqNodeScan"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 1);
}

#[test]
fn plan_cache_evicts_least_frequently_used_plan() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(2),
        ..DatabaseConfig::default()
    });
    let q1 = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";
    let q2 = "MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title";
    let q3 = "MATCH (m:Memory) WHERE m.id = 3 RETURN m.title AS title";

    db.explain_query(q1).unwrap();
    db.explain_query(q1).unwrap();
    db.explain_query(q2).unwrap();
    db.explain_query(q3).unwrap();

    let hot = db.explain_query(q1).unwrap();
    let evicted = db.explain_query(q2).unwrap();

    assert!(hot
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache hit: exact parameterized physical plan"));
    assert!(
        evicted
            .trace
            .decisions
            .iter()
            .any(|decision| decision
                == "plan cache miss: optimized exact parameterized physical plan")
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.hits, 2);
    assert_eq!(stats.misses, 4);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 0);
    assert_eq!(stats.evictions, 2);
}

#[test]
fn plan_cache_can_be_disabled_with_zero_capacity() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(0),
        ..DatabaseConfig::default()
    });
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";

    let first = db.explain_query(query).unwrap();
    let second = db.explain_query(query).unwrap();

    assert!(
        first
            .trace
            .decisions
            .iter()
            .any(|decision| decision
                == "plan cache miss: optimized exact parameterized physical plan")
    );
    assert!(
        second
            .trace
            .decisions
            .iter()
            .any(|decision| decision
                == "plan cache miss: optimized exact parameterized physical plan")
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.max_entries, Some(0));
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.disabled_misses, 2);
    assert_eq!(stats.bypasses, 0);
    assert_eq!(stats.evictions, 0);
}

#[test]
fn plan_cache_records_bypassed_mutation_explain_separately() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });

    let output = db
        .explain_query("CREATE (:Memory {id: 1, title: 'Bypassed'})")
        .unwrap();

    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision == "plan cache bypass: statement_not_cacheable"));
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.disabled_misses, 0);
    assert_eq!(stats.bypasses, 1);
    assert_eq!(stats.evictions, 0);
}

#[test]
fn explain_uses_scan_without_index_descriptor() {
    let db = Database::new();
    let output = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();

    assert!(output.trace.selected_plan.contains("FilterExec"));
    assert!(output.trace.selected_plan.contains("SeqNodeScan"));
    assert!(!output.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("no equality index descriptor")));
}

#[test]
fn explain_uses_statistics_to_keep_low_selectivity_filter() {
    let mut db = Database::new();
    for id in 1..=5 {
        db.query(&format!("CREATE (:Memory {{id: {id}, kind: 'note'}})"))
            .unwrap();
    }

    let output = db
        .explain_query("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.id AS id")
        .unwrap();

    assert!(output.trace.selected_plan.contains("FilterExec"));
    assert!(output.trace.selected_plan.contains("SeqNodeScan"));
    assert!(!output.trace.selected_plan.contains("IndexNodeSeek"));
    assert!(output
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose SeqNodeScan")));
}

#[test]
fn plan_fingerprint_is_deterministic_and_changes_with_plan_shape() {
    let mut db = Database::new();
    let query = "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title";
    let before = db.explain_query(query).unwrap();
    let before_again = db.explain_query(query).unwrap();

    assert_eq!(
        before.trace.selected_plan_fingerprint,
        before_again.trace.selected_plan_fingerprint
    );
    assert!(before
        .trace
        .selected_plan_fingerprint
        .contains("SeqNodeScan"));

    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let after = db.explain_query(query).unwrap();

    assert_ne!(
        before.trace.selected_plan_fingerprint,
        after.trace.selected_plan_fingerprint
    );
    assert!(after
        .trace
        .selected_plan_fingerprint
        .contains("IndexNodeSeek"));
}

#[test]
fn schema_ddl_creates_catalog_tokens_idempotently() {
    let mut db = Database::new();
    let first = db.query("CREATE NODE LABEL Memory").unwrap();
    let second = db.query("CREATE NODE LABEL Memory").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("label_id"),
        second.rows[0].get("label_id")
    );

    let first = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
    let second = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("rel_type_id"),
        second.rows[0].get("rel_type_id")
    );

    let first = db.query("CREATE NODE TABLE Memory").unwrap();
    let second = db.query("CREATE NODE TABLE Memory").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("table_id"),
        second.rows[0].get("table_id")
    );

    let first = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
    let second = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("table_id"),
        second.rows[0].get("table_id")
    );
    let tables = db.table_descriptors();
    assert!(tables.iter().any(|table| {
        table.name == "Memory"
            && table.kind == TableKind::Node
            && table.state == SchemaObjectState::Public
    }));
    assert!(tables.iter().any(|table| {
        table.name == "MENTIONS"
            && table.kind == TableKind::Relationship
            && table.state == SchemaObjectState::Public
    }));

    let first = db
        .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
        .unwrap();
    let second = db
        .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
        .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("property_id"),
        second.rows[0].get("property_id")
    );
    db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
        .unwrap();
    let properties = db.property_descriptors();
    assert!(properties.iter().any(|property| {
        property.name == "id" && property.value_type == PropertyType::Int && !property.nullable
    }));
    assert!(properties.iter().any(|property| {
        property.name == "weight" && property.value_type == PropertyType::Int && property.nullable
    }));

    let first = db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let second = db.query("CREATE INDEX ON :Memory(id)").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("index_id"),
        second.rows[0].get("index_id")
    );
    assert!(db
        .property_indexes()
        .iter()
        .any(|index| index.property == "id"));

    let first = db
        .query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    let second = db
        .query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("index_id"),
        second.rows[0].get("index_id")
    );
    assert!(db
        .composite_property_indexes()
        .iter()
        .any(|index| index.properties == ["kind", "source_id"]));

    let first = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let second = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("index_id"),
        second.rows[0].get("index_id")
    );
    assert!(db.property_indexes().iter().any(|index| {
        index.property == "title" && index.kind == crate::schema::IndexKind::FullText
    }));

    let first = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap();
    let second = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("constraint_id"),
        second.rows[0].get("constraint_id")
    );
    assert!(db
        .unique_constraints()
        .iter()
        .any(|constraint| constraint.property == "id"));
}

#[test]
fn schema_ddl_replays_from_wal_without_data_rows() {
    let path = unique_test_dir("schema_ddl_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE LABEL Memory").unwrap();
        db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_node_label"));
    assert!(wal.contains("create_rel_type"));
    assert!(wal.contains("create_node_table"));
    assert!(wal.contains("create_rel_table"));
    assert!(wal.contains("create_property"));
    assert!(wal.contains("create_index"));
    assert!(wal.contains("create_composite_index"));
    assert!(wal.contains("create_fulltext_index"));
    assert!(wal.contains("create_unique_constraint"));
    assert!(wal.contains("create_node_property_exists_constraint"));
    assert!(wal.contains("create_relationship_unique_constraint"));
    assert!(wal.contains("create_relationship_property_exists_constraint"));
    {
        let mut db = Database::open(&path).unwrap();
        let label = db.query("CREATE NODE LABEL Memory").unwrap();
        assert_eq!(label.rows[0].get("created"), Some(&Value::Bool(false)));
        let rel_type = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        assert_eq!(rel_type.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE NODE TABLE Memory").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let property = db
            .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        let property = db
            .query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE INDEX ON :Memory(id)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db
            .query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_ddl_survives_checkpoint_without_wal() {
    let path = unique_test_dir("schema_ddl_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE LABEL Memory").unwrap();
        db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
    let checkpoint = read_test_durable_text(&path.join("checkpoint.skein")).unwrap();
    assert!(checkpoint.contains("table"));
    assert!(checkpoint.contains("property\t"));
    assert!(checkpoint.contains("property_index"));
    assert!(checkpoint.contains("composite_property_index"));
    assert!(checkpoint.contains("fulltext"));
    assert!(checkpoint.contains("unique_constraint"));
    assert!(checkpoint.contains("node_property_exists_constraint"));
    assert!(checkpoint.contains("relationship_unique_constraint"));
    assert!(checkpoint.contains("relationship_property_exists_constraint"));
    {
        let mut db = Database::open(&path).unwrap();
        let label = db.query("CREATE NODE LABEL Memory").unwrap();
        assert_eq!(label.rows[0].get("created"), Some(&Value::Bool(false)));
        let rel_type = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        assert_eq!(rel_type.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE NODE TABLE Memory").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let property = db
            .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        let property = db
            .query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE INDEX ON :Memory(id)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db
            .query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_state_transitions_are_idempotent_and_persisted() {
    let path = unique_test_dir("schema_state_transition");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();

        let output = db
            .query("ALTER NODE TABLE Memory SET STATE WRITE_ONLY")
            .unwrap();
        assert_eq!(output.rows[0].get("changed"), Some(&Value::Bool(true)));
        let output = db
            .query("ALTER NODE TABLE Memory SET STATE WRITE_ONLY")
            .unwrap();
        assert_eq!(output.rows[0].get("changed"), Some(&Value::Bool(false)));

        let output = db
            .query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        assert_eq!(output.rows[0].get("changed"), Some(&Value::Bool(true)));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("alter_table_state"));
    assert!(wal.contains("write_only"));
    assert!(wal.contains("alter_property_state"));
    assert!(wal.contains("backfill"));

    {
        let mut db = Database::open(&path).unwrap();
        assert!(db.table_descriptors().iter().any(|table| {
            table.name == "Memory" && table.state == SchemaObjectState::WriteOnly
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
        db.checkpoint().unwrap();
    }

    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
    let checkpoint = read_test_durable_text(&path.join("checkpoint.skein")).unwrap();
    assert!(checkpoint.contains("write_only"));
    assert!(checkpoint.contains("backfill"));

    {
        let db = Database::open(&path).unwrap();
        assert!(db.table_descriptors().iter().any(|table| {
            table.name == "Memory" && table.state == SchemaObjectState::WriteOnly
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn non_public_property_schema_is_not_validated_until_public() {
    let path = unique_test_dir("schema_state_public_validation");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("CREATE (:Memory {title: 'Missing id'})").unwrap();

        let error = db
            .query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE PUBLIC")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("backfill"));
    assert!(!wal.contains("public"));
    {
        let db = Database::open(&path).unwrap();
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_advances_backfill_and_validation_in_batch_wal() {
    let path = unique_test_dir("schema_maintenance_advance");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert_eq!(
            output.rows[0].get("from_state"),
            Some(&Value::String("backfill".to_string()))
        );
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("public".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Public
        }));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.matches("\tbatch\t").count(), 5);
    assert!(wal.contains("alter_property_state,node,4d656d6f7279,6964,validating"));
    assert!(wal.contains("alter_property_state,node,4d656d6f7279,6964,public"));
    {
        let db = Database::open(&path).unwrap();
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Public
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_rejects_invalid_validation_before_wal() {
    let path = unique_test_dir("schema_maintenance_rejects_invalid");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE BACKFILL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE (:Memory {title: 'Missing id'})").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE VALIDATING")
            .unwrap();

        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        let error = db.run_schema_maintenance().unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(after, before);
        assert!(db.table_descriptors().iter().any(|table| {
            table.name == "Memory" && table.state == SchemaObjectState::Validating
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_plan_reports_pending_property_work_without_wal_write() {
    let path = unique_test_dir("schema_maintenance_plan_property");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();

        let plan = db.plan_schema_maintenance();

        assert_eq!(plan.rows.len(), 1);
        assert_eq!(
            plan.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("from_state"),
            Some(&Value::String("backfill".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("estimated_operations"),
            Some(&Value::Int(2))
        );
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_plan_estimates_relationship_table_validation_work() {
    let path = unique_test_dir("schema_maintenance_plan_relationship");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 3}]->(:Entity {id: 2})")
            .unwrap();
        db.query("CREATE (:Memory {id: 3})-[:MENTIONS {weight: 4}]->(:Entity {id: 4})")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER RELATIONSHIP TABLE MENTIONS SET STATE VALIDATING")
            .unwrap();

        let plan = db.plan_schema_maintenance();

        assert_eq!(plan.rows.len(), 1);
        assert_eq!(
            plan.rows[0].get("object"),
            Some(&Value::String("MENTIONS".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("from_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("to_state"),
            Some(&Value::String("public".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("estimated_operations"),
            Some(&Value::Int(2))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_schema_maintenance_skips_work_that_exceeds_budget_without_wal_write() {
    let path = unique_test_dir("bounded_schema_maintenance_budget_skip");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();

        let output = db.run_bounded_schema_maintenance(1).unwrap();

        assert!(output.rows.is_empty());
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_schema_maintenance_advances_descriptor_batches_incrementally() {
    let path = unique_test_dir("bounded_schema_maintenance_incremental");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'a'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'b'})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE BACKFILL")
            .unwrap();

        let first = db.run_bounded_schema_maintenance(2).unwrap();

        assert_eq!(first.rows.len(), 1);
        assert_eq!(
            first.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));

        let second = db.run_bounded_schema_maintenance(2).unwrap();

        assert_eq!(second.rows.len(), 1);
        assert_eq!(
            second.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert_eq!(
            second.rows[0].get("to_state"),
            Some(&Value::String("public".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Public
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));

        let third = db.run_bounded_schema_maintenance(2).unwrap();

        assert_eq!(third.rows.len(), 1);
        assert_eq!(
            third.rows[0].get("object"),
            Some(&Value::String("Memory.title".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Validating
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_background_work_plan_is_absent_without_pending_work() {
    let db = Database::new();

    assert!(db
        .schema_maintenance_background_work_plan(BackgroundWorkHint::default())
        .is_none());
}

#[test]
fn schema_maintenance_background_work_plan_uses_pending_estimate_for_ranking() {
    let path = unique_test_dir("schema_maintenance_background_plan");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();

        let plan = db
            .schema_maintenance_background_work_plan(BackgroundWorkHint {
                active_topic: true,
                query_probability_per_million: 250_000,
                ..BackgroundWorkHint::default()
            })
            .unwrap();

        assert_eq!(plan.request.class, crate::WorkClass::Mutation);
        assert_eq!(plan.request.estimated_operations, 2);

        let ranked =
            LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].index, 0);
        assert!(ranked[0]
            .decision
            .reasons
            .iter()
            .any(|reason| reason == "active topic"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn background_schema_maintenance_defers_without_mutating_schema() {
    let path = unique_test_dir("background_schema_maintenance_defers");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(0),
            ..LocalQosPolicy::default()
        };

        let error = db
            .run_background_schema_maintenance(&policy, &LocalQosState::default(), 1)
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn planned_background_schema_maintenance_uses_pending_work_estimate() {
    let path = unique_test_dir("planned_background_schema_maintenance_estimate");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };

        let error = db
            .run_planned_background_schema_maintenance(&policy, &LocalQosState::default())
            .unwrap_err();

        assert!(error.to_string().contains("estimated operations 2"));
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_background_schema_maintenance_limits_actual_execution() {
    let path = unique_test_dir("bounded_background_schema_maintenance_execution");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'a'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'b'})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE BACKFILL")
            .unwrap();

        let output = db
            .run_bounded_background_schema_maintenance(
                &LocalQosPolicy::default(),
                &LocalQosState::default(),
                2,
            )
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_background_schema_maintenance_admits_actual_work_not_caller_cap() {
    let path = unique_test_dir("bounded_background_schema_maintenance_actual_work");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(2),
            ..LocalQosPolicy::default()
        };

        let output = db
            .run_bounded_background_schema_maintenance(&policy, &LocalQosState::default(), 10)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_schema_maintenance_tracks_mutation_budget() {
    let path = unique_test_dir("scheduled_schema_maintenance_budget");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[crate::WorkClass::Mutation.as_index()] = Some(2);
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        });

        let output = db
            .run_scheduled_background_schema_maintenance(&mut scheduler, 2)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_scheduled_background_schema_maintenance_admits_actual_work_not_caller_cap() {
    let path = unique_test_dir("bounded_scheduled_schema_maintenance_actual_work");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(2),
            max_total_background_operations: Some(2),
            ..LocalQosPolicy::default()
        });

        let output = db
            .run_bounded_scheduled_background_schema_maintenance(&mut scheduler, 10)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert_eq!(scheduler.state().running_background_operations, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_scheduled_background_schema_maintenance_limits_execution_and_releases_budget() {
    let path = unique_test_dir("bounded_scheduled_schema_maintenance_budget");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'a'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'b'})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE BACKFILL")
            .unwrap();
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[crate::WorkClass::Mutation.as_index()] = Some(2);
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        });

        let output = db
            .run_bounded_scheduled_background_schema_maintenance(&mut scheduler, 2)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn planned_scheduled_background_schema_maintenance_tracks_estimated_mutation_budget() {
    let path = unique_test_dir("planned_scheduled_schema_maintenance_budget");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[crate::WorkClass::Mutation.as_index()] = Some(2);
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        });

        let output = db
            .run_planned_scheduled_background_schema_maintenance(&mut scheduler)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_schema_maintenance_releases_budget_on_validation_error() {
    let path = unique_test_dir("scheduled_schema_maintenance_error_releases");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE BACKFILL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE (:Memory {title: 'Missing id'})").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE VALIDATING")
            .unwrap();
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let error = db
            .run_scheduled_background_schema_maintenance(&mut scheduler, 1)
            .unwrap_err();

        assert!(error.to_string().contains("property schema violation"));
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_gc_removes_descriptors_and_persists() {
    let path = unique_test_dir("schema_maintenance_gc");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE GC")
            .unwrap();

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("action"),
            Some(&Value::String("gc".to_string()))
        );
        assert!(db
            .property_descriptors()
            .iter()
            .all(|property| property.name != "id"));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("gc_property_descriptor,node,4d656d6f7279,6964"));
    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .property_descriptors()
            .iter()
            .all(|property| property.name != "id"));
        db.query("ALTER NODE TABLE Memory SET STATE GC").unwrap();
        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory".to_string()))
        );
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .table_descriptors()
            .iter()
            .all(|table| table.name != "Memory"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_ddl_transaction_commits_and_rolls_back() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE NODE LABEL RolledBack").unwrap();
        tx.rollback();
    }
    let output = db.query("CREATE NODE LABEL RolledBack").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE NODE TABLE RolledBack").unwrap();
        tx.rollback();
    }
    let output = db.query("CREATE NODE TABLE RolledBack").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE INDEX ON :RolledBack(id)").unwrap();
        tx.rollback();
    }
    let output = db.query("CREATE INDEX ON :RolledBack(id)").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE INDEX ON :RolledBack(kind, source_id)")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE INDEX ON :RolledBack(kind, source_id)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE FULLTEXT INDEX ON :RolledBack(title)")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE FULLTEXT INDEX ON :RolledBack(title)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE PROPERTY ON NODE TABLE RolledBack(id) TYPE INT NOT NULL")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE PROPERTY ON NODE TABLE RolledBack(id) TYPE INT NOT NULL")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE CONSTRAINT ON :RolledBack(id) ASSERT UNIQUE")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE CONSTRAINT ON :RolledBack(id) ASSERT UNIQUE")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE RELATIONSHIP TYPE COMMITTED").unwrap();
        tx.query("CREATE NODE TABLE Committed").unwrap();
        tx.query("CREATE RELATIONSHIP TABLE COMMITTED").unwrap();
        tx.query("CREATE PROPERTY ON NODE TABLE Committed(id) TYPE INT NOT NULL")
            .unwrap();
        tx.query("CREATE INDEX ON :Committed(id)").unwrap();
        tx.query("CREATE INDEX ON :Committed(kind, source_id)")
            .unwrap();
        tx.query("CREATE FULLTEXT INDEX ON :Committed(title)")
            .unwrap();
        tx.query("CREATE CONSTRAINT ON :Committed(id) ASSERT UNIQUE")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[2].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[3].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[4].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[5].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[6].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[7].get("created"), Some(&Value::Bool(true)));
    }
    let output = db.query("CREATE RELATIONSHIP TYPE COMMITTED").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db.query("CREATE NODE TABLE Committed").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db.query("CREATE RELATIONSHIP TABLE COMMITTED").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE PROPERTY ON NODE TABLE Committed(id) TYPE INT NOT NULL")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db.query("CREATE INDEX ON :Committed(id)").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE INDEX ON :Committed(kind, source_id)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE FULLTEXT INDEX ON :Committed(title)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE CONSTRAINT ON :Committed(id) ASSERT UNIQUE")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
}

#[test]
fn explicit_index_ddl_enables_index_seek_plans() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap();

    assert!(explain.physical_plan.explain(0).contains("IndexNodeSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek")));
}

#[test]
fn explicit_index_ddl_enables_index_multi_seek_plans_for_property_in() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'a', title: 'A'})").unwrap();
    db.query("CREATE (:Memory {id: 'b', title: 'B'})").unwrap();
    db.query("CREATE (:Memory {id: 'c', title: 'C'})").unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'extra-{id}', title: 'Extra {id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id IN ['a', 'b', 'a'] RETURN m.id AS id")
        .unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeMultiSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeMultiSeek")));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_in_index_multi_seek:")
    }));

    let output = db
        .query("MATCH (m:Memory) WHERE m.id IN ['a', 'b', 'a'] RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("a".to_string()))
    );
    assert_eq!(
        output.rows[1].get("id"),
        Some(&Value::String("b".to_string()))
    );
}

#[test]
fn indexed_property_in_parameter_list_keeps_residual_filters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'a', title: 'A', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'b', title: 'B', lifecycle_state: 'archived'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'c', title: 'C', lifecycle_state: 'active'})")
        .unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'extra-{id}', title: 'Extra {id}', lifecycle_state: 'active'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let parameters = BTreeMap::from([(
        "ids".to_string(),
        Value::List(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("a".to_string()),
        ]),
    )]);
    let cypher =
        "MATCH (m:Memory) WHERE m.id IN $ids AND m.lifecycle_state = 'active' RETURN m.id AS id";

    let explain = db.explain_query_with_params(cypher, &parameters).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeMultiSeek"));
    assert!(physical_plan.contains("FilterExec"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeMultiSeek")));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_conjunction_index_seek:")
    }));

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN $ids AND m.lifecycle_state = 'active' RETURN m.id AS id ORDER BY id ASC",
            &parameters,
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("a".to_string()))
    );
}

#[test]
fn composite_index_ddl_enables_composite_index_seek_plans() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'One'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Two'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Three'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.kind = 'note' AND m.source_id = 'a' RETURN m.title AS title",
        )
        .unwrap();

    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeCompositeSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeCompositeSeek")));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_composite_index_seek:")
    }));

    let output = db
        .query(
            "MATCH (m:Memory) WHERE m.kind = 'note' AND m.source_id = 'a' RETURN m.title AS title",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );

    db.query("MATCH (m:Memory) WHERE m.title = 'Two' SET m.source_id = 'a'")
        .unwrap();
    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.kind = 'note' AND m.source_id = 'a' RETURN m.title AS title ORDER BY title ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Two".to_string()))
    );
}

#[test]
fn full_text_index_ddl_enables_text_seek_plans() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Graph query planning'})")
        .unwrap();

    let scan = db
        .explain_query("MATCH (m:Memory) WHERE m.title CONTAINS 'raph' RETURN m.id AS id")
        .unwrap();
    assert!(scan.physical_plan.explain(0).contains("SeqNodeScan"));
    assert!(!scan.physical_plan.explain(0).contains("IndexNodeTextSeek"));

    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.title CONTAINS 'raph' RETURN m.id AS id")
        .unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeTextSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeTextSeek")));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| { decision.starts_with("apply implementation:node_text_index_seek:") }));

    let output = db
        .query("MATCH (m:Memory) WHERE m.title CONTAINS 'Graph' RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));

    db.query("MATCH (m:Memory) WHERE m.id = 2 SET m.title = 'Graph search'")
        .unwrap();
    let output = db
        .query("MATCH (m:Memory) WHERE m.title CONTAINS 'Graph' RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[2].get("id"), Some(&Value::Int(3)));
}

#[test]
fn bounded_property_index_projection_rebuild_skips_descriptors_over_budget_without_wal_write() {
    let path = unique_test_dir("bounded_index_projection_rebuild_skip");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
            .unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
            .unwrap();
        db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();

        let output = db.rebuild_bounded_property_index_projections(2);

        assert!(output.rows.is_empty());
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(after, before);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_property_index_projection_rebuild_reports_descriptor_batches() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();

    let first = db.rebuild_bounded_property_index_projections(3);

    assert_eq!(first.rows.len(), 1);
    assert_eq!(
        first.rows[0].get("index_kind"),
        Some(&Value::String("composite".to_string()))
    );
    assert_eq!(
        first.rows[0].get("label"),
        Some(&Value::String("Memory".to_string()))
    );
    assert_eq!(
        first.rows[0].get("properties"),
        Some(&Value::List(vec![
            Value::String("kind".to_string()),
            Value::String("source_id".to_string())
        ]))
    );
    assert_eq!(
        first.rows[0].get("estimated_operations"),
        Some(&Value::Int(3))
    );
    assert_eq!(first.rows[0].get("indexed_entries"), Some(&Value::Int(3)));

    let second = db.rebuild_bounded_property_index_projections(6);

    assert_eq!(second.rows.len(), 2);
    assert_eq!(
        second.rows[1].get("index_kind"),
        Some(&Value::String("full_text".to_string()))
    );
    assert_eq!(
        second.rows[1].get("properties"),
        Some(&Value::List(vec![Value::String("title".to_string())]))
    );
    assert_eq!(
        second.rows[1].get("estimated_operations"),
        Some(&Value::Int(3))
    );
    assert!(matches!(
        second.rows[1].get("indexed_entries"),
        Some(Value::Int(value)) if *value > 0
    ));
}

#[test]
fn property_index_projection_background_work_plan_is_absent_without_descriptors() {
    let db = Database::new();

    assert!(db
        .property_index_projection_background_work_plan(BackgroundWorkHint::default())
        .is_none());
}

#[test]
fn property_index_projection_background_work_plan_uses_projection_lane() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();

    let plan = db
        .property_index_projection_background_work_plan(BackgroundWorkHint {
            active_topic: true,
            query_probability_per_million: 100_000,
            ..BackgroundWorkHint::default()
        })
        .unwrap();

    assert_eq!(plan.request.class, crate::WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 6);
    let ranked = LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);
    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].index, 0);
    assert!(ranked[0]
        .decision
        .reasons
        .iter()
        .any(|reason| reason == "active topic"));
}

#[test]
fn bounded_background_property_index_projection_rebuild_defers_without_rebuilding() {
    let path = unique_test_dir("bounded_background_index_projection_defer");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
            .unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };

        let error = db
            .rebuild_bounded_background_property_index_projections(
                &policy,
                &LocalQosState::default(),
                2,
            )
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(after, before);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_background_property_index_projection_rebuild_admits_actual_batch_estimate() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let policy = LocalQosPolicy {
        max_background_operations: Some(3),
        ..LocalQosPolicy::default()
    };

    let output = db
        .rebuild_bounded_background_property_index_projections(
            &policy,
            &LocalQosState::default(),
            4,
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("index_kind"),
        Some(&Value::String("composite".to_string()))
    );
    assert_eq!(
        output.rows[0].get("estimated_operations"),
        Some(&Value::Int(3))
    );
}

#[test]
fn bounded_scheduled_background_property_index_projection_rebuild_releases_budget() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Projection.as_index()] = Some(3);
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(4),
        max_total_background_operations: Some(4),
        max_background_operations_by_class: class_limits,
        ..LocalQosPolicy::default()
    });

    let output = db
        .rebuild_bounded_scheduled_background_property_index_projections(&mut scheduler, 3)
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("index_kind"),
        Some(&Value::String("composite".to_string()))
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Projection.as_index()],
        0
    );
}

#[test]
fn range_predicates_filter_and_use_range_index() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, created_at: 10, title: 'Old'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, created_at: 20, title: 'Current'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, created_at: 30, title: 'Future'})")
        .unwrap();

    let scan = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at >= 20 RETURN m.id AS id")
        .unwrap();
    assert!(scan.physical_plan.explain(0).contains("SeqNodeScan"));
    assert!(!scan.physical_plan.explain(0).contains("IndexNodeRangeSeek"));

    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();
    let indexed = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at >= 20 RETURN m.id AS id")
        .unwrap();
    assert!(indexed
        .physical_plan
        .explain(0)
        .contains("IndexNodeRangeSeek"));
    assert!(indexed
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeRangeSeek")));
    assert!(indexed
        .trace
        .decisions
        .iter()
        .any(|decision| { decision.starts_with("apply implementation:node_range_index_seek:") }));

    let output = db
        .query("MATCH (m:Memory) WHERE m.created_at >= 20 RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));
}

#[test]
fn and_range_predicates_use_bounded_range_index_with_residual_filter() {
    let mut db = Database::new();
    for (id, created_at) in [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25)] {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, created_at: {created_at}}})"
        ))
        .unwrap();
    }
    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.created_at >= 10 AND m.created_at < 20 RETURN m.id AS id",
        )
        .unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("FilterExec"));
    assert!(physical_plan.contains("IndexNodeRangeSeek"));
    assert!(physical_plan.contains("lower=Some((Int(10), true))"));
    assert!(physical_plan.contains("upper=Some((Int(20), false))"));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("choose IndexNodeRangeSeek")
            && decision.contains("in conjunction")
            && decision.contains("estimated_rows=2")
    }));

    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.created_at >= 10 AND m.created_at < 20 RETURN m.id AS id ORDER BY id ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));
}

#[test]
fn range_selectivity_statistics_drive_range_index_costing() {
    let mut db = Database::new();
    for id in 0..100 {
        db.query(&format!("CREATE (:Memory {{id: {id}, created_at: {id}}})"))
            .unwrap();
    }
    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at > 98 RETURN m.id AS id")
        .unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeRangeSeek"));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("choose IndexNodeRangeSeek") && decision.contains("estimated_rows=1")
    }));
}

#[test]
fn relationship_property_statistics_drive_expand_costing() {
    let mut db = Database::new();
    for id in 0..10 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}}})-[:MENTIONS {{weight: {id}}}]->(:Entity {{id: {}}})",
            id + 100
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let explain = db
            .explain_query(
                "MATCH (m:Memory {id: 1})-[r:MENTIONS]->(e:Entity) WHERE r.weight = 1 RETURN r.weight AS weight",
            )
            .unwrap();
    let physical_plan = explain.physical_plan.explain(0);

    assert!(physical_plan.contains("AdjacencyExpandExec"));
    assert!(physical_plan.contains(r#"properties={"weight": Int(1)}"#));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("rel_property_distinct_product=10")
            && decision.contains("estimated_rows=1")
    }));
    assert_eq!(
        explain.trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 27,
        }
    );
}

#[test]
fn relationship_property_histograms_drive_filter_range_costing() {
    let mut db = Database::new();
    for id in 0..10 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}}})-[:MENTIONS {{created_at: {id}}}]->(:Entity {{id: {}}})",
            id + 100
        ))
        .unwrap();
    }

    let explain = db
            .explain_query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE r.created_at > 8 RETURN r.created_at AS created_at",
            )
            .unwrap();
    let physical_plan = explain.physical_plan.explain(0);

    assert!(physical_plan.contains("FilterExec"));
    assert!(physical_plan.contains("PropertyCompare"));
    assert_eq!(
        explain.trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 45,
        }
    );
}

#[test]
fn property_histograms_are_bounded_deterministic_samples() {
    let mut db = Database::new();
    for id in 0..200 {
        db.query(&format!("CREATE (:Memory {{score: {id}}})"))
            .unwrap();
    }

    let statistics = db.statistics();
    assert_eq!(statistics.computed_at_commit_epoch, 200);
    assert_eq!(statistics.histogram_sample_limit, 512);
    let ((_, property), distinct_count) = statistics
        .property_distinct_counts
        .iter()
        .find(|((_, property), _)| property == "score")
        .unwrap();
    assert_eq!(property, "score");
    assert_eq!(*distinct_count, 200);

    let histogram = statistics
        .property_histograms
        .iter()
        .find_map(|((_, property), values)| (property == "score").then_some(values))
        .unwrap();
    assert_eq!(histogram.len(), 128);
    assert_eq!(histogram.first(), Some(&Value::Int(0)));
    assert_eq!(histogram.last(), Some(&Value::Int(199)));
    let sampled = statistics
        .sampled_property_histograms
        .iter()
        .find_map(|((_, property), sampled)| (property == "score").then_some(sampled))
        .unwrap();
    assert!(*sampled);

    db.query("CREATE (:Memory {exact_score: 1})").unwrap();
    db.query("CREATE (:Memory {exact_score: 2})").unwrap();
    let statistics = db.statistics();
    let sampled = statistics
        .sampled_property_histograms
        .iter()
        .find_map(|((_, property), sampled)| (property == "exact_score").then_some(sampled))
        .unwrap();
    assert!(!sampled);
}

#[test]
fn range_index_descriptor_persists_through_wal_and_checkpoint() {
    let path = unique_test_dir("range_index_descriptor");
    {
        let mut db = Database::open(&path).unwrap();
        let first = db
            .query("CREATE RANGE INDEX ON :Memory(created_at)")
            .unwrap();
        let second = db
            .query("CREATE RANGE INDEX ON :Memory(created_at)")
            .unwrap();
        assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("create_range_index"));

    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .property_indexes()
            .iter()
            .any(|index| { index.property == "created_at" && index.kind == IndexKind::Range }));
        db.checkpoint().unwrap();
    }
    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
    let checkpoint = read_test_durable_text(&path.join("checkpoint.skein")).unwrap();
    assert!(checkpoint.contains("property_index"));
    assert!(checkpoint.contains("range"));

    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .property_indexes()
            .iter()
            .any(|index| { index.property == "created_at" && index.kind == IndexKind::Range }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unique_constraint_rejects_duplicate_create_and_set_before_wal() {
    let path = unique_test_dir("unique_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();

        let error = db
            .query("CREATE (:Memory {id: 1, title: 'Duplicate'})")
            .unwrap_err();
        assert!(error.to_string().contains("unique constraint violation"));

        let error = db
            .query("MATCH (m:Memory) WHERE m.id = 2 SET m.id = 1")
            .unwrap_err();
        assert!(error.to_string().contains("unique constraint violation"));

        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Two".to_string()))
        );
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .unique_constraints()
            .iter()
            .any(|constraint| constraint.property == "id"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unique_constraint_rejects_existing_duplicate_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Duplicate'})")
        .unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint violation"));
    assert!(db.unique_constraints().is_empty());
}

#[test]
fn node_property_exists_constraint_rejects_missing_and_null_writes_before_wal() {
    let path = unique_test_dir("property_exists_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();

        let error = db.query("CREATE (:Memory {title: 'Missing'})").unwrap_err();
        assert!(error
            .to_string()
            .contains("node property exists constraint violation"));

        let error = db
            .query("MATCH (m:Memory) WHERE m.id = 1 SET m.id = null")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("node property exists constraint violation"));

        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("One".to_string()))
        );
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .node_property_exists_constraints()
            .iter()
            .any(|constraint| {
                constraint.property == "id" && constraint.kind == ConstraintKind::NodePropertyExists
            }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn node_property_exists_constraint_rejects_existing_bad_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {title: 'Missing'})").unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT NOT NULL")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("node property exists constraint violation"));
    assert!(db.node_property_exists_constraints().is_empty());
}

#[test]
fn relationship_property_exists_constraint_rejects_missing_and_null_writes_before_wal() {
    let path = unique_test_dir("relationship_property_exists_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 1}]->(:Memory {id: 2})")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();

        let error = db
            .query("CREATE (:Memory {id: 3})-[:MENTIONS]->(:Memory {id: 4})")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship property exists constraint violation"));

        let error = db
            .query("CREATE (:Memory {id: 5})-[:MENTIONS {weight: null}]->(:Memory {id: 6})")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship property exists constraint violation"));
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .relationship_property_exists_constraints()
            .iter()
            .any(|constraint| {
                constraint.property == "weight"
                    && constraint.kind == ConstraintKind::RelationshipPropertyExists
                    && matches!(constraint.subject, ConstraintSubject::Relationship(_))
            }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_property_exists_constraint_rejects_existing_bad_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 1}]->(:Memory {id: 2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})-[:MENTIONS]->(:Memory {id: 4})")
        .unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT NOT NULL")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship property exists constraint violation"));
    assert!(db.relationship_property_exists_constraints().is_empty());
}

#[test]
fn relationship_unique_constraint_rejects_duplicate_writes_before_wal() {
    let path = unique_test_dir("relationship_unique_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1})-[:MENTIONS {id: 10}]->(:Memory {id: 2})")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();

        let error = db
            .query("CREATE (:Memory {id: 3})-[:MENTIONS {id: 10}]->(:Memory {id: 4})")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship unique constraint violation"));

        db.query("CREATE (:Memory {id: 5})-[:MENTIONS]->(:Memory {id: 6})")
            .unwrap();
        db.query("CREATE (:Memory {id: 7})-[:MENTIONS {id: null}]->(:Memory {id: 8})")
            .unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .relationship_unique_constraints()
            .iter()
            .any(|constraint| {
                constraint.property == "id"
                    && constraint.kind == ConstraintKind::RelationshipPropertyUnique
                    && matches!(constraint.subject, ConstraintSubject::Relationship(_))
            }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_unique_constraint_rejects_existing_duplicate_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS {id: 10}]->(:Memory {id: 2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})-[:MENTIONS {id: 10}]->(:Memory {id: 4})")
        .unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship unique constraint violation"));
    assert!(db.relationship_unique_constraints().is_empty());
}

#[test]
fn property_schema_rejects_invalid_writes_before_wal() {
    let path = unique_test_dir("property_schema_write");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap();

        let error = db
            .query("CREATE (:Memory {id: 'bad', title: 'Bad'})")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        let error = db
            .query("MATCH (m:Memory) WHERE m.id = 1 SET m.id = 'bad'")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        let error = db
            .query("CREATE (:Memory {id: 2})-[:MENTIONS {weight: 'bad'}]->(:Entity {name: 'Rust'})")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));

        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap();
        assert_eq!(before, after);
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("One".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn property_schema_rejects_existing_invalid_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'bad', title: 'Bad'})")
        .unwrap();

    let error = db
        .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
        .unwrap_err();
    assert!(error.to_string().contains("property schema violation"));
    assert!(db.property_descriptors().is_empty());
}

#[test]
fn failed_transaction_does_not_publish_property_schema_or_wal() {
    let path = unique_test_dir("property_schema_failed_transaction");
    {
        let mut db = Database::open(&path).unwrap();
        let before = std::fs::read_to_string(path.join("wal.skein")).unwrap_or_default();
        let mut tx = db.begin_transaction();
        tx.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        tx.query("CREATE (:Memory {id: 'bad'})").unwrap();

        let error = tx.commit().unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        assert!(db.property_descriptors().is_empty());
        let after = std::fs::read_to_string(path.join("wal.skein")).unwrap_or_default();
        assert_eq!(before, after);
    }
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
        .open(path.join("wal.skein"))
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
    assert_eq!(report.wal_replay_start_lsn, Some(1));
    assert_eq!(report.next_lsn_after_replay, Some(2));
    assert_eq!(report.replayed_wal_entries, 1);
    assert!(!report.torn_tail_ignored);
    assert_eq!(report.torn_tail_reason, None);
    assert_eq!(report.recovered_commit_epoch, 2);

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
    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
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

    let checkpoint = read_test_durable_text(&path.join("checkpoint.skein")).unwrap();
    assert!(checkpoint.contains("node\t"));
    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn creates_and_expands_relationships_with_cypher() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
}

#[test]
fn projects_graph_for_page_rank() {
    let mut db = Database::new();
    db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
        .unwrap();
    db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
        .unwrap();
    db.query("MERGE (:Entity {id: 3, name: 'Leaf'})-[:MENTIONS]->(:Entity {id: 4, name: 'Other'})")
        .unwrap();

    let links = db.project_graph(Some("LINKS"));
    assert_eq!(links.node_count(), 4);
    assert_eq!(links.edge_count(), 2);
    assert_eq!(
        links
            .incoming_sources(NodeId(2))
            .unwrap()
            .collect::<Vec<_>>(),
        vec![NodeId(1)]
    );

    let all = db.project_graph(None);
    assert_eq!(all.edge_count(), 3);
    assert_eq!(
        all.incoming_sources(NodeId(3)).unwrap().collect::<Vec<_>>(),
        vec![NodeId(2)]
    );

    let missing = db.project_graph(Some("MISSING"));
    assert_eq!(missing.node_count(), 4);
    assert_eq!(missing.edge_count(), 0);
    assert!(missing
        .incoming_sources(NodeId(2))
        .unwrap()
        .collect::<Vec<_>>()
        .is_empty());

    let scores = links.page_rank(Default::default());
    assert_eq!(scores[0].node.0, 2);

    let projection = db
        .query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
        .unwrap();
    assert_eq!(projection.rows[0].get("node_count"), Some(&Value::Int(4)));
    assert_eq!(projection.rows[0].get("edge_count"), Some(&Value::Int(2)));

    let projection = db
        .query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
        .unwrap();
    assert_eq!(projection.rows[0].get("node_count"), Some(&Value::Int(3)));
    assert_eq!(projection.rows[0].get("edge_count"), Some(&Value::Int(1)));

    let output = db
            .query(
                "CALL page_rank('EntityGraph', dampingFactor := 0.85, maxIterations := 20) RETURN node, pagerank_score",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("node"), Some(&Value::Int(2)));
    let Some(Value::Float(score)) = output.rows[0].get("pagerank_score") else {
        panic!("expected pagerank_score float");
    };
    assert!(*score > 0.0);

    let output = db
            .query_with_params(
                "CALL page_rank('EntityGraph', dampingFactor := $damping, maxIterations := $iterations) RETURN node, pagerank_score",
                &BTreeMap::from([
                    ("damping".to_string(), Value::Float(0.85)),
                    ("iterations".to_string(), Value::Int(20)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows[0].get("node"), Some(&Value::Int(2)));

    let error = db
            .query_with_params(
                "CALL page_rank('EntityGraph', maxIterations := $iterations) RETURN node, pagerank_score",
                &BTreeMap::from([("iterations".to_string(), Value::Float(2.5))]),
            )
            .unwrap_err();
    assert!(error
        .to_string()
        .contains("maxIterations must be a non-negative integer"));

    let output = db
        .query("CALL louvain('EntityGraph') RETURN node, louvain_id")
        .unwrap();
    assert!(output.rows.iter().any(|row| {
        row.get("node") == Some(&Value::Int(3)) && row.get("louvain_id") == Some(&Value::Int(3))
    }));

    let output = db
        .query("CALL louvain('EntityGraph', maxLevels := 2) RETURN node, level, louvain_id")
        .unwrap();
    assert!(output
        .rows
        .iter()
        .any(|row| row.get("level") == Some(&Value::Int(1))));

    let output = db
        .query("CALL page_rank('EntityOnlyGraph') RETURN node, pagerank_score")
        .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert!(output
        .rows
        .iter()
        .all(|row| row.get("node") != Some(&Value::Int(0))));

    let error = db
        .query("CALL page_rank('MissingGraph') RETURN node, pagerank_score")
        .unwrap_err();
    assert!(error.to_string().contains("does not exist"));

    let communities = links
        .louvain_communities(Default::default())
        .into_iter()
        .map(|assignment| (assignment.node, assignment.community))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(communities[&NodeId(0)], NodeId(0));
    assert_eq!(communities[&NodeId(1)], NodeId(0));
    assert_eq!(communities[&NodeId(2)], NodeId(0));
    assert_eq!(communities[&NodeId(3)], NodeId(3));
}

#[test]
fn read_transaction_projects_snapshot_graph() {
    let mut db = Database::new();
    db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
        .unwrap();

    let read_tx = db.begin_read_transaction();
    db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
        .unwrap();

    assert_eq!(read_tx.project_graph(Some("LINKS")).edge_count(), 1);
    assert_eq!(db.project_graph(Some("LINKS")).edge_count(), 2);
}

#[test]
fn projected_graph_definition_replays_from_wal() {
    let path = unique_test_dir("projected_graph_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
            .unwrap();
        db.query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("CALL page_rank('EntityOnlyGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output
            .rows
            .iter()
            .all(|row| row.get("node") != Some(&Value::Int(0))));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projected_graph_definition_survives_checkpoint() {
    let path = unique_test_dir("projected_graph_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
            .unwrap();
        db.query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    assert_eq!(std::fs::read_to_string(path.join("wal.skein")).unwrap(), "");
    let checkpoint = read_test_durable_text(&path.join("checkpoint.skein")).unwrap();
    assert!(checkpoint.contains("project_graph"));

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("CALL page_rank('EntityOnlyGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output
            .rows
            .iter()
            .all(|row| row.get("node") != Some(&Value::Int(0))));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_writes_projected_graph_artifacts() {
    let path = unique_test_dir("projected_graph_artifacts");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
            .unwrap();
        db.query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let artifact = read_test_durable_text(&path.join("projected_graphs.skein")).unwrap();
    assert!(artifact.contains("SKEIN_PROJECTED_GRAPHS_V1\n"));
    assert!(artifact.contains("artifact_version\t1\n"));
    assert!(artifact.contains("projection_epoch\t1\n"));
    assert!(artifact.contains("commit_epoch\t3\n"));
    assert!(artifact.contains("graph\t456e746974794f6e6c794772617068"));
    assert!(artifact.contains("nodes\t1,2\n"));
    assert!(artifact.contains("csr_offsets\t0,1,1\n"));
    assert!(artifact.contains("csr_targets\t1\n"));
    assert!(artifact.contains("csc_offsets\t0,0,1\n"));
    assert!(artifact.contains("csc_sources\t0\n"));
    assert!(artifact.contains("checksum\t"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projected_graph_status_reports_artifact_reuse_state() {
    let path = unique_test_dir("projected_graph_status");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].name, "EntityGraph");
        assert_eq!(statuses[0].projection_epoch, Some(1));
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].node_count, Some(2));
        assert_eq!(statuses[0].edge_count, Some(1));
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].projection_epoch, None);
        assert!(!statuses[0].reusable);
        assert_eq!(statuses[0].node_count, None);
        assert_eq!(statuses[0].edge_count, None);
        db.rebuild_projected_graph_artifacts().unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses[0].projection_epoch, Some(2));
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].node_count, Some(3));
        assert_eq!(statuses[0].edge_count, Some(1));
    }
    {
        let db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].projection_epoch, Some(2));
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].node_count, Some(3));
        assert_eq!(statuses[0].edge_count, Some(1));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn derived_artifact_rebuild_reports_projected_graph_refresh() {
    let path = unique_test_dir("derived_artifact_rebuild");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        let before = db.projected_graph_statuses();
        assert_eq!(before.len(), 1);
        assert!(!before[0].reusable);

        let output = db.rebuild_derived_artifacts().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("artifact_type"),
            Some(&Value::String("projected_graph".to_string()))
        );
        assert_eq!(
            output.rows[0].get("name"),
            Some(&Value::String("EntityGraph".to_string()))
        );
        assert_eq!(
            output.rows[0].get("action"),
            Some(&Value::String("rebuilt".to_string()))
        );
        assert_eq!(
            output.rows[0].get("before_reusable"),
            Some(&Value::Bool(false))
        );
        assert_eq!(
            output.rows[0].get("after_reusable"),
            Some(&Value::Bool(true))
        );
        assert_eq!(output.rows[0].get("projection_epoch"), Some(&Value::Int(2)));
        assert_eq!(output.rows[0].get("node_count"), Some(&Value::Int(3)));
        assert_eq!(output.rows[0].get("edge_count"), Some(&Value::Int(1)));
    }
    {
        let db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].projection_epoch, Some(2));
        assert_eq!(statuses[0].node_count, Some(3));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn derived_artifact_job_rebuilds_projected_graphs() {
    let path = unique_test_dir("derived_artifact_job_success");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        let job = db.schedule_derived_artifact_rebuild();
        assert_eq!(job.id, 1);
        assert_eq!(job.status, DerivedArtifactJobStatus::Pending);
        assert_eq!(db.derived_artifact_jobs().len(), 1);

        let report = db.run_next_derived_artifact_job().unwrap().unwrap();
        assert_eq!(report.job.id, 1);
        assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
        assert_eq!(report.job.attempts, 1);
        assert!(report.job.last_error.is_none());
        assert_eq!(report.output.rows.len(), 1);
        assert_eq!(
            report.output.rows[0].get("name"),
            Some(&Value::String("EntityGraph".to_string()))
        );
        assert_eq!(
            report.output.rows[0].get("after_reusable"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            db.derived_artifact_jobs()[0].status,
            DerivedArtifactJobStatus::Succeeded
        );
        assert!(db.run_next_derived_artifact_job().unwrap().is_none());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn background_derived_artifact_job_uses_qos_admission() {
    let path = unique_test_dir("background_derived_artifact_job_deferred");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Root'})").unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory'], [])")
            .unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Later'})")
            .unwrap();

        let job = db.schedule_derived_artifact_rebuild();
        assert_eq!(job.status, DerivedArtifactJobStatus::Pending);

        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };
        let error = db
            .run_next_background_derived_artifact_job(&policy, &LocalQosState::default(), 2)
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        let jobs = db.derived_artifact_jobs();
        assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
        assert_eq!(jobs[0].attempts, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn background_derived_artifact_job_runs_when_qos_admits() {
    let path = unique_test_dir("background_derived_artifact_job_admitted");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        db.schedule_derived_artifact_rebuild();
        let report = db
            .run_next_background_derived_artifact_job(
                &LocalQosPolicy::default(),
                &LocalQosState::default(),
                2,
            )
            .unwrap()
            .unwrap();

        assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
        assert_eq!(report.job.attempts, 1);
        assert_eq!(report.output.rows.len(), 1);
        assert_eq!(
            report.output.rows[0].get("after_reusable"),
            Some(&Value::Bool(true))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_derived_artifact_job_tracks_running_budget() {
    let path = unique_test_dir("scheduled_background_derived_artifact_job");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        db.schedule_derived_artifact_rebuild();
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });

        let report = db
            .run_next_scheduled_background_derived_artifact_job(&mut scheduler, 4)
            .unwrap()
            .unwrap();

        assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_derived_artifact_job_defers_when_scheduler_is_full() {
    let path = unique_test_dir("scheduled_background_derived_artifact_job_full");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Root'})").unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory'], [])")
            .unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Later'})")
            .unwrap();

        db.schedule_derived_artifact_rebuild();
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(8),
            max_total_background_operations: Some(8),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(crate::WorkRequest::background(
                crate::WorkClass::Analytics,
                6,
            ))
            .unwrap();

        let error = db
            .run_next_scheduled_background_derived_artifact_job(&mut scheduler, 4)
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        assert_eq!(scheduler.state().running_background_operations, 6);
        assert_eq!(
            db.derived_artifact_jobs()[0].status,
            DerivedArtifactJobStatus::Pending
        );

        scheduler.finish(running);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_derived_artifact_job_releases_budget_on_execution_error() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });
    db.schedule_derived_artifact_rebuild();
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = db
        .run_next_scheduled_background_derived_artifact_job(&mut scheduler, 1)
        .unwrap_err();

    assert!(error.to_string().contains("read-only mode"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
}

#[test]
fn derived_artifact_job_reports_unknown_projected_graph_failure() {
    let mut db = Database::new();
    let job = db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    assert_eq!(job.status, DerivedArtifactJobStatus::Pending);

    let report = db.run_next_derived_artifact_job().unwrap().unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    assert!(report
        .job
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("unknown projected graph artifact")));
    assert_eq!(
        report.output.rows[0].get("status"),
        Some(&Value::String("failed".to_string()))
    );
    assert!(report.output.rows[0].get("error").is_some_and(|value| value
        == &Value::String(
            "semantic error: unknown projected graph artifact 'MissingGraph'".to_string()
        )));
    assert_eq!(
        db.derived_artifact_jobs()[0].status,
        DerivedArtifactJobStatus::Failed
    );
}

#[test]
fn external_content_artifact_jobs_are_explicitly_outside_graph_kernel() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job("source-1", "parse");
    assert_eq!(job.artifact_type, "content_artifact");
    assert_eq!(job.name, "source-1");
    assert_eq!(job.action, "parse");
    assert_eq!(job.status, DerivedArtifactJobStatus::Pending);

    let report = db.run_next_derived_artifact_job().unwrap().unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    let error = report.job.last_error.as_deref().unwrap();
    assert!(error.contains("outside the graph kernel"));
    assert!(error.contains("content artifact job runtime"));
    assert_eq!(
        report.output.rows[0].get("artifact_type"),
        Some(&Value::String("content_artifact".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("status"),
        Some(&Value::String("failed".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("error"),
        Some(&Value::String(error.to_string()))
    );
}

#[test]
fn pending_external_content_artifact_jobs_are_bounded_and_filtered() {
    let mut db = Database::new();
    db.schedule_derived_artifact_rebuild();
    let first = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-1.md".to_string()),
        )]),
    );
    let second = db.schedule_external_content_artifact_job("source-2", "parse");

    let pending_one = db.pending_external_content_artifact_jobs(1);
    assert_eq!(pending_one.len(), 1);
    assert_eq!(pending_one[0].id, first.id);
    assert_eq!(
        pending_one[0].payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );

    let pending_all = db.pending_external_content_artifact_jobs(usize::MAX);
    assert_eq!(
        pending_all.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    assert!(db.pending_external_content_artifact_jobs(0).is_empty());

    let report = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(report.job.id, first.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);

    let remaining = db.pending_external_content_artifact_jobs(8);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, second.id);
}

#[test]
fn failed_external_content_artifact_jobs_are_bounded_and_filtered() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-1.md".to_string()),
        )]),
    );
    let second = db.schedule_external_content_artifact_job("source-2", "parse");
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let first_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "source-1 parser failure".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(first_failure.job.id, first.id);
    let second_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "source-2 parser failure".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(second_failure.job.id, second.id);
    let projected_graph_failure = db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph.id);
    assert_eq!(
        projected_graph_failure.job.status,
        DerivedArtifactJobStatus::Failed
    );

    let failed_one = db.failed_external_content_artifact_jobs(1);
    assert_eq!(failed_one.len(), 1);
    assert_eq!(failed_one[0].id, first.id);
    assert_eq!(
        failed_one[0].payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );

    let failed_all = db.failed_external_content_artifact_jobs(usize::MAX);
    assert_eq!(
        failed_all.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    assert!(db.failed_external_content_artifact_jobs(0).is_empty());

    db.retry_failed_external_content_artifact_job(first.id)
        .unwrap();
    let remaining_failed = db.failed_external_content_artifact_jobs(8);
    assert_eq!(remaining_failed.len(), 1);
    assert_eq!(remaining_failed[0].id, second.id);
}

#[test]
fn failed_external_content_artifact_jobs_can_be_filtered_by_action() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let crawl = db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("https://example.invalid/source-crawl".to_string()),
        )]),
    );

    db.run_next_external_content_artifact_job_with(|_| {
        Err(crate::error::SkeinError::Execution(
            "parse failed".to_string(),
        ))
    })
    .unwrap()
    .unwrap();
    db.run_next_external_content_artifact_job_with(|_| {
        Err(crate::error::SkeinError::Execution(
            "crawl failed".to_string(),
        ))
    })
    .unwrap()
    .unwrap();

    let parse_failed = db.failed_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_failed.len(), 1);
    assert_eq!(parse_failed[0].id, parse.id);
    assert_eq!(
        parse_failed[0].payload.get("content_uri"),
        Some(&Value::String(
            "file:///nowledge/source-parse.md".to_string()
        ))
    );
    let crawl_failed = db.failed_external_content_artifact_jobs_for_action("crawl", 8);
    assert_eq!(crawl_failed.len(), 1);
    assert_eq!(crawl_failed[0].id, crawl.id);
    assert!(db
        .failed_external_content_artifact_jobs_for_action("embed", 8)
        .is_empty());
    assert!(db
        .failed_external_content_artifact_jobs_for_action("parse", 0)
        .is_empty());
}

#[test]
fn succeeded_external_content_artifact_jobs_are_bounded_and_filtered() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let crawl = db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let parse_report = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "projection_ref".to_string(),
                        Value::String("search:v1".to_string()),
                    ),
                ])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(parse_report.job.id, parse.id);

    let crawl_report = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "projection_ref".to_string(),
                        Value::String("crawl-log:v1".to_string()),
                    ),
                ])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(crawl_report.job.id, crawl.id);
    let projected_graph_failure = db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph.id);

    let succeeded_one = db.succeeded_external_content_artifact_jobs(1);
    assert_eq!(succeeded_one.len(), 1);
    assert_eq!(succeeded_one[0].id, parse.id);
    assert_eq!(succeeded_one[0].last_output, Some(parse_report.output));

    let succeeded_all = db.succeeded_external_content_artifact_jobs(usize::MAX);
    assert_eq!(
        succeeded_all.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![parse.id, crawl.id]
    );
    assert_eq!(succeeded_all[1].last_output, Some(crawl_report.output));
    assert!(db.succeeded_external_content_artifact_jobs(0).is_empty());

    let parse_succeeded = db.succeeded_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_succeeded.len(), 1);
    assert_eq!(parse_succeeded[0].id, parse.id);
    assert_eq!(
        parse_succeeded[0].last_output.as_ref().unwrap().rows[0].get("projection_ref"),
        Some(&Value::String("search:v1".to_string()))
    );
    assert!(db
        .succeeded_external_content_artifact_jobs_for_action("embed", 8)
        .is_empty());
    assert!(db
        .succeeded_external_content_artifact_jobs_for_action("parse", 0)
        .is_empty());
}

#[test]
fn external_content_artifact_job_summary_counts_runtime_work_only() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job("source-2", "crawl");
    db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let initial = db.external_content_artifact_job_summary();
    assert_eq!(initial.total, 2);
    assert_eq!(initial.pending, 2);
    assert_eq!(initial.running, 0);
    assert_eq!(initial.succeeded, 0);
    assert_eq!(initial.failed, 0);
    assert_eq!(
        initial.pending_by_action,
        BTreeMap::from([("crawl".to_string(), 1), ("parse".to_string(), 1)])
    );
    assert!(initial.failed_by_action.is_empty());
    assert_eq!(initial.next_pending_job_id, Some(first.id));
    assert_eq!(initial.oldest_failed_job_id, None);

    let initial_parse = db.external_content_artifact_job_summary_for_action("parse");
    assert_eq!(initial_parse.total, 1);
    assert_eq!(initial_parse.pending, 1);
    assert_eq!(initial_parse.failed, 0);
    assert_eq!(
        initial_parse.pending_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert!(initial_parse.failed_by_action.is_empty());
    assert_eq!(initial_parse.next_pending_job_id, Some(first.id));
    assert_eq!(initial_parse.oldest_failed_job_id, None);

    let initial_crawl = db.external_content_artifact_job_summary_for_action("crawl");
    assert_eq!(initial_crawl.total, 1);
    assert_eq!(initial_crawl.pending, 1);
    assert_eq!(initial_crawl.next_pending_job_id, Some(second.id));
    let initial_embed = db.external_content_artifact_job_summary_for_action("embed");
    assert_eq!(initial_embed.total, 0);
    assert!(initial_embed.pending_by_action.is_empty());

    let failed = db
        .run_external_content_artifact_job_with(first.id, |_| {
            Err(crate::error::SkeinError::Execution(
                "parser runtime failed".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(failed.job.status, DerivedArtifactJobStatus::Failed);
    let succeeded = db
        .run_external_content_artifact_job_with(second.id, |job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(succeeded.job.status, DerivedArtifactJobStatus::Succeeded);

    let after_run = db.external_content_artifact_job_summary();
    assert_eq!(after_run.total, 2);
    assert_eq!(after_run.pending, 0);
    assert_eq!(after_run.running, 0);
    assert_eq!(after_run.succeeded, 1);
    assert_eq!(after_run.failed, 1);
    assert!(after_run.pending_by_action.is_empty());
    assert_eq!(
        after_run.failed_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert_eq!(after_run.next_pending_job_id, None);
    assert_eq!(after_run.oldest_failed_job_id, Some(first.id));

    let parse_after_run = db.external_content_artifact_job_summary_for_action("parse");
    assert_eq!(parse_after_run.total, 1);
    assert_eq!(parse_after_run.pending, 0);
    assert_eq!(parse_after_run.failed, 1);
    assert!(parse_after_run.pending_by_action.is_empty());
    assert_eq!(
        parse_after_run.failed_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert_eq!(parse_after_run.next_pending_job_id, None);
    assert_eq!(parse_after_run.oldest_failed_job_id, Some(first.id));

    let crawl_after_run = db.external_content_artifact_job_summary_for_action("crawl");
    assert_eq!(crawl_after_run.total, 1);
    assert_eq!(crawl_after_run.pending, 0);
    assert_eq!(crawl_after_run.succeeded, 1);
    assert_eq!(crawl_after_run.failed, 0);
    assert_eq!(crawl_after_run.next_pending_job_id, None);
    assert_eq!(crawl_after_run.oldest_failed_job_id, None);

    db.retry_failed_external_content_artifact_job(first.id)
        .unwrap();
    let after_retry = db.external_content_artifact_job_summary();
    assert_eq!(after_retry.total, 2);
    assert_eq!(after_retry.pending, 1);
    assert_eq!(after_retry.succeeded, 1);
    assert_eq!(after_retry.failed, 0);
    assert_eq!(
        after_retry.pending_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert!(after_retry.failed_by_action.is_empty());
    assert_eq!(after_retry.next_pending_job_id, Some(first.id));
    assert_eq!(after_retry.oldest_failed_job_id, None);

    let parse_after_retry = db.external_content_artifact_job_summary_for_action("parse");
    assert_eq!(parse_after_retry.total, 1);
    assert_eq!(parse_after_retry.pending, 1);
    assert_eq!(parse_after_retry.failed, 0);
    assert_eq!(parse_after_retry.next_pending_job_id, Some(first.id));
    assert_eq!(parse_after_retry.oldest_failed_job_id, None);
}

#[test]
fn external_content_artifact_job_background_work_plan_is_rankable_by_action() {
    let mut db = Database::new();
    assert!(db
        .external_content_artifact_job_background_work_plan(BackgroundWorkHint::default(), 3)
        .is_none());
    assert!(db
        .external_content_artifact_job_background_work_plan_for_action(
            "parse",
            BackgroundWorkHint::default(),
            3,
        )
        .is_none());

    db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    assert!(db
        .external_content_artifact_job_background_work_plan(BackgroundWorkHint::default(), 3)
        .is_none());

    db.schedule_external_content_artifact_job("source-parse", "parse");
    db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let hint = BackgroundWorkHint {
        active_topic: true,
        recent_delta_operations: 5,
        tenant_budget_remaining_operations: Some(8),
        ..BackgroundWorkHint::default()
    };

    let any_plan = db
        .external_content_artifact_job_background_work_plan(hint.clone(), 3)
        .unwrap();
    assert_eq!(any_plan.request.class, WorkClass::Import);
    assert_eq!(any_plan.request.estimated_operations, 3);
    assert_eq!(any_plan.hint, hint);

    let parse_plan = db
        .external_content_artifact_job_background_work_plan_for_action(
            "parse",
            BackgroundWorkHint {
                query_probability_per_million: 42,
                tenant_budget_remaining_operations: Some(2),
                ..BackgroundWorkHint::default()
            },
            3,
        )
        .unwrap();
    assert_eq!(parse_plan.request.class, WorkClass::Import);
    assert_eq!(parse_plan.request.estimated_operations, 3);
    let decision =
        LocalQosPolicy::default().evaluate_background_work(&LocalQosState::default(), &parse_plan);
    assert!(matches!(
        decision.admission,
        QosAdmission::Defer { reason, .. } if reason.contains("tenant budget remaining 2")
    ));
    assert!(decision
        .reasons
        .iter()
        .any(|reason| reason.contains("tenant budget remaining 2")));

    assert!(db
        .external_content_artifact_job_background_work_plan_for_action(
            "embed",
            BackgroundWorkHint::default(),
            3,
        )
        .is_none());
}

#[test]
fn external_content_runtime_manifest_filters_claimable_jobs() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-parse.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("parse-sha".to_string())),
        ]),
    );
    db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("https://example.invalid/source-crawl".to_string()),
        )]),
    );
    db.schedule_external_content_artifact_job("source-missing", "parse");

    let manifest = ExternalContentArtifactRuntimeManifest::new("markdown-parser")
        .with_runtime_version("1.0.0")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_required_payload_key("sha256")
        .with_estimated_operations(7);

    let pending = db.pending_external_content_artifact_jobs_for_runtime(&manifest, 8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, parse.id);
    assert_eq!(pending[0].action, "parse");
    assert_eq!(
        pending[0].payload.get("sha256"),
        Some(&Value::String("parse-sha".to_string()))
    );
    assert!(db
        .pending_external_content_artifact_jobs_for_runtime(&manifest, 0)
        .is_empty());
}

#[test]
fn external_content_runtime_manifest_exposes_import_work_plan() {
    let mut db = Database::new();
    assert!(db
        .external_content_artifact_job_background_work_plan_for_runtime(
            &ExternalContentArtifactRuntimeManifest::new("parser")
                .with_supported_action("parse")
                .with_required_payload_key("content_uri"),
            BackgroundWorkHint::default(),
        )
        .is_none());

    db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let manifest = ExternalContentArtifactRuntimeManifest::new("parser")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_estimated_operations(5);
    let plan = db
        .external_content_artifact_job_background_work_plan_for_runtime(
            &manifest,
            BackgroundWorkHint {
                active_topic: true,
                query_probability_per_million: 10,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();

    assert_eq!(plan.request.class, WorkClass::Import);
    assert_eq!(plan.request.estimated_operations, 5);
    let ranked = LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);
    assert_eq!(ranked.len(), 1);
    assert!(matches!(ranked[0].decision.admission, QosAdmission::Admit));
}

#[test]
fn external_content_runtime_manifest_runs_only_claimable_jobs() {
    let mut db = Database::new();
    let missing = db.schedule_external_content_artifact_job("source-missing", "parse");
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-parse.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("parse-sha".to_string())),
        ]),
    );
    let crawl = db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("https://example.invalid/source-crawl".to_string()),
            ),
            ("sha256".to_string(), Value::String("crawl-sha".to_string())),
        ]),
    );

    let manifest = ExternalContentArtifactRuntimeManifest::new("markdown-parser")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_required_payload_key("sha256")
        .with_estimated_operations(7);

    let report = db
        .run_next_external_content_artifact_job_for_runtime_with(&manifest, |job| {
            assert_eq!(job.id, parse.id);
            assert_eq!(job.action, "parse");
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "runtime_name".to_string(),
                        Value::String("markdown-parser".to_string()),
                    ),
                ])],
            })
        })
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, parse.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, missing.id);
    assert_eq!(pending[1].id, crawl.id);
}

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

#[test]
fn failed_external_content_artifact_jobs_can_be_retried() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-1.md".to_string()),
        )]),
    );

    let failed = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "transient parser failure".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(failed.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(failed.job.attempts, 1);
    assert!(failed.job.last_output.is_none());
    assert!(failed
        .job
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("transient parser failure")));
    assert!(db.pending_external_content_artifact_jobs(8).is_empty());

    let retried = db
        .retry_failed_external_content_artifact_job(job.id)
        .unwrap();
    assert_eq!(retried.status, DerivedArtifactJobStatus::Pending);
    assert_eq!(retried.attempts, 1);
    assert!(retried.last_error.is_none());
    assert!(retried.last_output.is_none());
    assert_eq!(
        retried.payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );
    assert_eq!(db.pending_external_content_artifact_jobs(8)[0].id, job.id);

    let succeeded = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    ("attempts".to_string(), Value::Int(job.attempts as i64)),
                ])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(succeeded.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(succeeded.job.attempts, 2);
    assert!(succeeded.job.last_error.is_none());
    assert_eq!(succeeded.job.last_output, Some(succeeded.output.clone()));
    assert_eq!(
        succeeded.output.rows[0].get("attempts"),
        Some(&Value::Int(2))
    );
    assert!(db
        .retry_failed_external_content_artifact_job(job.id)
        .is_none());

    let projected_graph_job = db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    let projected_graph_failure = db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph_job.id);
    assert_eq!(
        projected_graph_failure.job.status,
        DerivedArtifactJobStatus::Failed
    );
    assert!(db
        .retry_failed_external_content_artifact_job(projected_graph_job.id)
        .is_none());
}

#[test]
fn failed_external_content_artifact_jobs_can_be_retried_by_action() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let crawl = db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("https://example.invalid/source-crawl".to_string()),
        )]),
    );

    let parse_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "parse runtime failed".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(parse_failure.job.id, parse.id);
    let crawl_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "crawl runtime failed".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(crawl_failure.job.id, crawl.id);

    assert!(db
        .retry_failed_external_content_artifact_job_for_action("parse", crawl.id)
        .is_none());
    let retried = db
        .retry_failed_external_content_artifact_job_for_action("parse", parse.id)
        .unwrap();
    assert_eq!(retried.status, DerivedArtifactJobStatus::Pending);
    assert_eq!(retried.action, "parse");
    assert_eq!(retried.attempts, 1);
    assert!(retried.last_error.is_none());
    assert_eq!(
        retried.payload.get("content_uri"),
        Some(&Value::String(
            "file:///nowledge/source-parse.md".to_string()
        ))
    );

    let parse_pending = db.pending_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_pending.len(), 1);
    assert_eq!(parse_pending[0].id, parse.id);
    assert!(db
        .failed_external_content_artifact_jobs_for_action("parse", 8)
        .is_empty());
    let crawl_failed = db.failed_external_content_artifact_jobs_for_action("crawl", 8);
    assert_eq!(crawl_failed.len(), 1);
    assert_eq!(crawl_failed[0].id, crawl.id);

    let mut graph_db = Database::new();
    let projected_graph_job = graph_db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    let projected_graph_failure = graph_db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph_job.id);
    assert_eq!(
        projected_graph_failure.job.status,
        DerivedArtifactJobStatus::Failed
    );
    assert!(graph_db
        .retry_failed_external_content_artifact_job_for_action("rebuild", projected_graph_job.id,)
        .is_none());
}

#[test]
fn caller_owned_content_artifact_runtime_can_complete_external_jobs() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([
            (
                "source_id".to_string(),
                Value::String("source-1".to_string()),
            ),
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-1.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("abc123".to_string())),
            (
                "target_projection".to_string(),
                Value::String("search".to_string()),
            ),
        ]),
    );
    assert_eq!(
        job.payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );

    let report = db
        .run_next_external_content_artifact_job_with(|job| {
            assert_eq!(job.status, DerivedArtifactJobStatus::Running);
            assert_eq!(job.attempts, 1);
            assert_eq!(
                job.payload.get("sha256"),
                Some(&Value::String("abc123".to_string()))
            );
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "artifact_type".to_string(),
                        Value::String(job.artifact_type.clone()),
                    ),
                    ("name".to_string(), Value::String(job.name.clone())),
                    ("action".to_string(), Value::String(job.action.clone())),
                    (
                        "published_projection".to_string(),
                        job.payload
                            .get("target_projection")
                            .cloned()
                            .unwrap_or(Value::String("unknown".to_string())),
                    ),
                    ("parsed_chunks".to_string(), Value::Int(2)),
                ])],
            })
        })
        .unwrap()
        .unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(report.job.attempts, 1);
    assert!(report.job.last_error.is_none());
    assert_eq!(report.job.last_output, Some(report.output.clone()));
    assert_eq!(
        report.output.rows[0].get("published_projection"),
        Some(&Value::String("search".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("parsed_chunks"),
        Some(&Value::Int(2))
    );
    assert_eq!(
        db.derived_artifact_jobs()[0].status,
        DerivedArtifactJobStatus::Succeeded
    );
    assert_eq!(
        db.derived_artifact_jobs()[0].last_output,
        Some(QueryOutput {
            rows: vec![BTreeMap::from([
                ("job_id".to_string(), Value::Int(job.id as i64)),
                (
                    "artifact_type".to_string(),
                    Value::String("content_artifact".to_string()),
                ),
                ("name".to_string(), Value::String("source-1".to_string())),
                ("action".to_string(), Value::String("parse".to_string())),
                (
                    "published_projection".to_string(),
                    Value::String("search".to_string()),
                ),
                ("parsed_chunks".to_string(), Value::Int(2)),
            ])],
        })
    );
    assert!(db
        .run_next_external_content_artifact_job_with(|_| unreachable!())
        .unwrap()
        .is_none());
}

#[test]
fn background_external_content_artifact_job_uses_qos_admission() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-1", "parse");
    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };

    let error = db
        .run_next_background_external_content_artifact_job_with(
            &policy,
            &LocalQosState::default(),
            |_| unreachable!(),
            1,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
}

#[test]
fn background_external_content_artifact_job_can_run_next_job_for_action() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job("source-parse", "parse");
    let crawl = db.schedule_external_content_artifact_job("source-crawl", "crawl");

    let report = db
        .run_next_background_external_content_artifact_job_for_action_with(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            "crawl",
            |job| {
                assert_eq!(job.id, crawl.id);
                assert_eq!(job.action, "crawl");
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                assert_eq!(job.attempts, 1);
                Ok(QueryOutput {
                    rows: vec![BTreeMap::from([(
                        "job_id".to_string(),
                        Value::Int(job.id as i64),
                    )])],
                })
            },
            2,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, crawl.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].id, parse.id);
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert_eq!(jobs[1].id, crawl.id);
    assert_eq!(jobs[1].status, DerivedArtifactJobStatus::Succeeded);
}

#[test]
fn background_external_content_artifact_job_for_action_uses_qos_admission() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-parse", "parse");
    db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };

    let error = db
        .run_next_background_external_content_artifact_job_for_action_with(
            &policy,
            &LocalQosState::default(),
            "crawl",
            |_| unreachable!(),
            1,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert_eq!(jobs[1].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[1].attempts, 0);
}

#[test]
fn scheduled_background_external_content_artifact_job_tracks_import_budget() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-1", "parse");
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Import.as_index()] = Some(2);
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(4),
        max_total_background_operations: Some(4),
        max_background_operations_by_class: class_limits,
        ..LocalQosPolicy::default()
    });

    let report = db
        .run_next_scheduled_background_external_content_artifact_job_with(
            &mut scheduler,
            |job| {
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                assert_eq!(job.attempts, 1);
                Ok(QueryOutput {
                    rows: vec![BTreeMap::from([(
                        "job_id".to_string(),
                        Value::Int(job.id as i64),
                    )])],
                })
            },
            2,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );
}

#[test]
fn scheduled_background_external_content_artifact_job_defers_when_import_lane_is_full() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-parse", "parse");
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Import.as_index()] = Some(4);
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(8),
        max_total_background_operations: Some(8),
        max_background_operations_by_class: class_limits,
        ..LocalQosPolicy::default()
    });
    let running = scheduler
        .try_start(crate::WorkRequest::background(crate::WorkClass::Import, 3))
        .unwrap();

    let error = db
        .run_next_scheduled_background_external_content_artifact_job_for_action_with(
            &mut scheduler,
            "parse",
            |_| unreachable!(),
            2,
        )
        .unwrap_err();

    assert!(error.to_string().contains("class limit 4"));
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        3
    );
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);

    scheduler.finish(running);
    assert_eq!(scheduler.state().running_background_operations, 0);
}

#[test]
fn scheduled_background_external_content_artifact_job_releases_budget_on_runtime_error() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-1", "parse");
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let report = db
        .run_next_scheduled_background_external_content_artifact_job_with(
            &mut scheduler,
            |_| {
                Err(crate::error::SkeinError::Execution(
                    "parser failed".to_string(),
                ))
            },
            1,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );
}

#[test]
fn background_external_content_artifact_job_can_run_specific_pending_job() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job("source-2", "parse");
    let policy = LocalQosPolicy::default();

    let report = db
        .run_background_external_content_artifact_job_with(
            &policy,
            &LocalQosState::default(),
            second.id,
            |job| {
                assert_eq!(job.id, second.id);
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                Ok(QueryOutput {
                    rows: vec![BTreeMap::from([(
                        "job_id".to_string(),
                        Value::Int(job.id as i64),
                    )])],
                })
            },
            1,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, second.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, first.id);
}

#[test]
fn scheduled_background_external_content_artifact_job_defers_specific_pending_job() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job("source-1", "parse");
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Import.as_index()] = Some(4);
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(8),
        max_total_background_operations: Some(8),
        max_background_operations_by_class: class_limits,
        ..LocalQosPolicy::default()
    });
    let running = scheduler
        .try_start(crate::WorkRequest::background(crate::WorkClass::Import, 3))
        .unwrap();

    let error = db
        .run_scheduled_background_external_content_artifact_job_with(
            &mut scheduler,
            job.id,
            |_| unreachable!(),
            2,
        )
        .unwrap_err();

    assert!(error.to_string().contains("class limit 4"));
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        3
    );
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);

    scheduler.finish(running);
}

#[test]
fn scheduled_background_external_content_artifact_job_releases_budget_on_specific_runtime_error() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job("source-1", "parse");
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let report = db
        .run_scheduled_background_external_content_artifact_job_with(
            &mut scheduler,
            job.id,
            |_| {
                Err(crate::error::SkeinError::Execution(
                    "parser failed".to_string(),
                ))
            },
            1,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, job.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );
}

#[test]
fn caller_owned_content_artifact_runtime_can_run_specific_pending_job() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job_with_payload(
        "source-2",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-2.md".to_string()),
        )]),
    );
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let report = db
        .run_external_content_artifact_job_with(second.id, |job| {
            assert_eq!(job.id, second.id);
            assert_eq!(job.status, DerivedArtifactJobStatus::Running);
            assert_eq!(
                job.payload.get("content_uri"),
                Some(&Value::String("file:///nowledge/source-2.md".to_string()))
            );
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(report.job.id, second.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);

    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, first.id);
    assert!(db
        .run_external_content_artifact_job_with(second.id, |_| unreachable!())
        .unwrap()
        .is_none());
    assert!(db
        .run_external_content_artifact_job_with(projected_graph.id, |_| unreachable!())
        .unwrap()
        .is_none());
    assert!(db
        .run_external_content_artifact_job_with(99, |_| unreachable!())
        .unwrap()
        .is_none());

    let next = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(next.job.id, first.id);
}

#[test]
fn caller_owned_content_artifact_runtime_can_complete_with_lineage_manifest() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-1.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("input-sha".to_string())),
        ]),
    );

    let report = db
        .complete_next_external_content_artifact_job_with(|job| {
            assert_eq!(job.status, DerivedArtifactJobStatus::Running);
            assert_eq!(
                job.payload.get("sha256"),
                Some(&Value::String("input-sha".to_string()))
            );
            Ok(ExternalContentArtifactJobCompletion::new("nowledge-parser")
                .with_runtime_version("0.3.7")
                .with_input_ref("file:///nowledge/source-1.md")
                .with_input_checksum("sha256:input-sha")
                .with_output_ref("projection://search/source-1")
                .with_output_checksum("sha256:projection-sha")
                .with_projection("search", "search:source-1:v2")
                .with_source_graph_commit_epoch(42)
                .with_rows_produced(3)
                .with_metadata("parser_mode", Value::String("markdown".to_string())))
        })
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, job.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(report.job.last_output, Some(report.output.clone()));
    let row = &report.output.rows[0];
    assert_eq!(row.get("job_id"), Some(&Value::Int(job.id as i64)));
    assert_eq!(
        row.get("runtime_name"),
        Some(&Value::String("nowledge-parser".to_string()))
    );
    assert_eq!(
        row.get("runtime_version"),
        Some(&Value::String("0.3.7".to_string()))
    );
    assert_eq!(
        row.get("input_ref"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );
    assert_eq!(
        row.get("input_checksum"),
        Some(&Value::String("sha256:input-sha".to_string()))
    );
    assert_eq!(
        row.get("output_ref"),
        Some(&Value::String("projection://search/source-1".to_string()))
    );
    assert_eq!(
        row.get("output_checksum"),
        Some(&Value::String("sha256:projection-sha".to_string()))
    );
    assert_eq!(
        row.get("projection_kind"),
        Some(&Value::String("search".to_string()))
    );
    assert_eq!(
        row.get("projection_ref"),
        Some(&Value::String("search:source-1:v2".to_string()))
    );
    assert_eq!(row.get("source_graph_commit_epoch"), Some(&Value::Int(42)));
    assert_eq!(row.get("rows_produced"), Some(&Value::Int(3)));
    assert_eq!(
        row.get("metadata"),
        Some(&Value::Map(BTreeMap::from([(
            "parser_mode".to_string(),
            Value::String("markdown".to_string())
        )])))
    );
}

#[test]
fn content_artifact_completion_runner_only_claims_external_jobs() {
    let mut db = Database::new();
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    let parse = db.schedule_external_content_artifact_job("source-parse", "parse");

    assert!(db
        .complete_external_content_artifact_job_with(projected_graph.id, |_| unreachable!())
        .unwrap()
        .is_none());

    let report = db
        .complete_external_content_artifact_job_with(parse.id, |_| {
            Ok(ExternalContentArtifactJobCompletion::new("parser").with_rows_produced(1))
        })
        .unwrap()
        .unwrap();
    assert_eq!(report.job.id, parse.id);
    assert_eq!(
        report.output.rows[0].get("runtime_version"),
        Some(&Value::Null)
    );
    assert_eq!(
        report.output.rows[0].get("rows_produced"),
        Some(&Value::Int(1))
    );
}

#[test]
fn background_content_artifact_completion_uses_qos_admission() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-parse", "parse");
    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };

    let error = db
        .complete_next_background_external_content_artifact_job_with(
            &policy,
            &LocalQosState::default(),
            |_| unreachable!(),
            1,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert!(jobs[0].last_output.is_none());
}

#[test]
fn background_content_artifact_completion_for_runtime_uses_manifest_claim_and_qos() {
    let mut db = Database::new();
    let missing = db.schedule_external_content_artifact_job("source-missing", "parse");
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let manifest = ExternalContentArtifactRuntimeManifest::new("parser")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_estimated_operations(5);
    let policy = LocalQosPolicy {
        max_background_operations: Some(4),
        ..LocalQosPolicy::default()
    };

    let error = db
        .complete_next_background_external_content_artifact_job_for_runtime_with(
            &policy,
            &LocalQosState::default(),
            &manifest,
            |_| unreachable!(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].id, missing.id);
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert_eq!(jobs[1].id, parse.id);
    assert_eq!(jobs[1].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[1].attempts, 0);

    let report = db
        .complete_next_background_external_content_artifact_job_for_runtime_with(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            &manifest,
            |job| {
                assert_eq!(job.id, parse.id);
                Ok(ExternalContentArtifactJobCompletion::new("parser")
                    .with_projection("search", "search:source-parse")
                    .with_rows_produced(2))
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, parse.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, missing.id);
}

#[test]
fn scheduled_specific_content_artifact_completion_releases_import_budget() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job("source-2", "parse");
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let report = db
        .complete_scheduled_background_external_content_artifact_job_with(
            &mut scheduler,
            second.id,
            |job| {
                assert_eq!(job.id, second.id);
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                Ok(ExternalContentArtifactJobCompletion::new("parser")
                    .with_projection("search", "search:source-2")
                    .with_rows_produced(2))
            },
            3,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, second.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(
        report.output.rows[0].get("projection_ref"),
        Some(&Value::String("search:source-2".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("rows_produced"),
        Some(&Value::Int(2))
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );

    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, first.id);
}

#[test]
fn caller_owned_content_artifact_runtime_can_poll_and_run_by_action() {
    let mut db = Database::new();
    let crawl = db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let parse_pending = db.pending_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_pending.len(), 1);
    assert_eq!(parse_pending[0].id, parse.id);
    assert!(db
        .pending_external_content_artifact_jobs_for_action("embed", 8)
        .is_empty());

    let parse_report = db
        .run_next_external_content_artifact_job_for_action_with("parse", |job| {
            assert_eq!(job.id, parse.id);
            assert_eq!(job.action, "parse");
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(parse_report.job.id, parse.id);
    assert_eq!(parse_report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert!(db
        .run_next_external_content_artifact_job_for_action_with("parse", |_| unreachable!())
        .unwrap()
        .is_none());

    let remaining = db.pending_external_content_artifact_jobs(8);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, crawl.id);
    let crawl_report = db
        .run_next_external_content_artifact_job_with(|job| {
            assert_eq!(job.id, crawl.id);
            assert_eq!(job.action, "crawl");
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])],
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(crawl_report.job.id, crawl.id);
}

#[test]
fn graph_kernel_rejects_external_content_jobs_with_payload_intact() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job_with_payload(
        "source-2",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("s3://bucket/source-2.pdf".to_string()),
        )]),
    );

    let report = db.run_next_derived_artifact_job().unwrap().unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(
        report.job.payload.get("content_uri"),
        Some(&Value::String("s3://bucket/source-2.pdf".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("payload"),
        Some(&Value::Map(BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("s3://bucket/source-2.pdf".to_string())
        )])))
    );
    assert!(report
        .job
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("outside the graph kernel")));
}

#[test]
fn corrupt_projected_graph_artifacts_do_not_block_recovery() {
    let path = unique_test_dir("projected_graph_artifact_corrupt");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let artifact_path = path.join("projected_graphs.skein");
    let artifact = read_test_durable_text(&artifact_path).unwrap();
    std::fs::write(
        &artifact_path,
        artifact.replace("csr_targets", "bad_targets"),
    )
    .unwrap();

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("CALL page_rank('EntityGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
    }
    assert!(!artifact_path.exists());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_open_ignores_corrupt_projected_graph_artifact_without_cleanup() {
    let path = unique_test_dir("projected_graph_artifact_corrupt_read_only");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let artifact_path = path.join("projected_graphs.skein");
    let artifact = read_test_durable_text(&artifact_path).unwrap();
    std::fs::write(
        &artifact_path,
        artifact.replace("csr_targets", "bad_targets"),
    )
    .unwrap();

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
            .query("CALL page_rank('EntityGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
    }
    assert!(artifact_path.exists());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn exposes_property_index_descriptors_and_statistics() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query(
        "CREATE (:Memory {id: 3, kind: 'decision'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
    )
    .unwrap();

    let indexes = db.property_indexes();
    assert!(indexes
        .iter()
        .any(|index| index.property == "id" && index.label_id.0 == 0));
    assert!(indexes
        .iter()
        .any(|index| index.property == "kind" && index.label_id.0 == 0));

    let statistics = db.statistics();
    assert_eq!(statistics.node_count, 4);
    assert_eq!(statistics.relationship_count, 1);
    assert_eq!(statistics.label_counts.values().sum::<u64>(), 4);
    assert_eq!(statistics.rel_type_counts.values().sum::<u64>(), 1);
    assert_eq!(statistics.rel_type_source_counts.values().sum::<u64>(), 1);
    assert_eq!(statistics.rel_type_target_counts.values().sum::<u64>(), 1);
    assert_eq!(statistics.path_counts.values().sum::<u64>(), 1);
    assert_eq!(
        statistics.property_distinct_counts.values().copied().max(),
        Some(3)
    );
    assert!(statistics
        .property_histograms
        .values()
        .any(|values| { values == &vec![Value::Int(1), Value::Int(2), Value::Int(3)] }));

    let read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 4, kind: 'note'})").unwrap();
    assert_eq!(read_tx.statistics().node_count, 4);
    assert_eq!(db.statistics().node_count, 5);
}

#[test]
fn bounded_multi_hop_statistics_drive_expand_estimates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 1000, name: 'Mid'})")
        .unwrap();
    for id in 0..100 {
        db.query(&format!(
                "MERGE (:Entity {{id: 1000, name: 'Mid'}})-[:LINKS]->(:Entity {{id: {id}, name: 'Leaf {id}'}})"
            ))
            .unwrap();
    }

    let statistics = db.statistics();
    let exact_two_hop_count = statistics
        .bounded_path_counts
        .iter()
        .find_map(|((source_label, _, target_label, hops), count)| {
            (source_label.0 == 0 && target_label.0 == 1 && *hops == 2).then_some(count)
        })
        .copied();
    assert_eq!(exact_two_hop_count, Some(100));

    let explain = db
        .explain_query("MATCH (m:Memory)-[:LINKS*2..2]->(e:Entity) RETURN e.name AS name")
        .unwrap();
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("*2..2")
            && decision.contains("hop_rows=[1:exact:1,2:exact:100]")
            && decision.contains("estimated_rows=100")
    }));
}

#[test]
fn checkpoint_persists_index_descriptors_and_statistics() {
    let path = unique_test_dir("catalog_stats_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
        db.query("CREATE (:Memory {id: 2, kind: 'decision'})-[:MENTIONS {weight: 4}]->(:Entity {id: 10, name: 'Rust'})")
                .unwrap();
        db.checkpoint().unwrap();
    }

    let checkpoint = read_test_durable_text(&path.join("checkpoint.skein")).unwrap();
    assert!(checkpoint.contains("property_index"));
    assert!(checkpoint.contains("stat_commit_epoch\t2\n"));
    assert!(checkpoint.contains("stat_histogram_sample_limit\t512\n"));
    assert!(checkpoint.contains("stat_node_count\t3\n"));
    assert!(checkpoint.contains("stat_relationship_count\t1\n"));
    assert!(checkpoint.contains("stat_rel_type_source_count"));
    assert!(checkpoint.contains("stat_path_count"));
    assert!(checkpoint.contains("stat_bounded_path_count"));
    assert!(checkpoint.contains("stat_bounded_path_source_distinct_count"));
    assert!(checkpoint.contains("stat_bounded_path_target_distinct_count"));
    assert!(checkpoint.contains("stat_property_distinct_count"));
    assert!(checkpoint.contains("stat_rel_property_distinct_count"));
    assert!(checkpoint.contains("stat_rel_property_histogram"));
    assert!(checkpoint.contains("stat_property_histogram"));
    assert!(checkpoint.contains("stat_rel_property_histogram_sampled"));
    assert!(checkpoint.contains("stat_property_histogram_sampled"));
    assert!(checkpoint.contains("stat_rel_type_target_count"));

    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .property_indexes()
            .iter()
            .any(|index| index.property == "kind"));
        assert_eq!(db.statistics().computed_at_commit_epoch, 2);
        assert_eq!(db.statistics().histogram_sample_limit, 512);
        assert_eq!(db.statistics().node_count, 3);
        assert_eq!(db.statistics().relationship_count, 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn expands_bounded_relationship_patterns() {
    let mut db = Database::new();
    db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
        .unwrap();
    db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[:LINKS*1..2]->(e:Entity) RETURN e.name AS name ORDER BY name ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("Leaf".to_string()))
    );
    assert_eq!(
        output.rows[1].get("name"),
        Some(&Value::String("Mid".to_string()))
    );

    let exact = db
        .query("MATCH (m:Memory)-[:LINKS*2]->(e:Entity) RETURN e.name AS name")
        .unwrap();
    assert_eq!(exact.rows.len(), 1);
    assert_eq!(
        exact.rows[0].get("name"),
        Some(&Value::String("Leaf".to_string()))
    );

    let explain = db
        .explain_query("MATCH (m:Memory)-[:LINKS*..2]->(e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("hops=1..2"));
    assert!(explain.trace.selected_plan.contains("source=m:Memory"));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand for Memory-[:LINKS*1..2]->Entity: path_count=1")
            && decision.contains("hop_rows=[1:exact:1,2:exact:1]")
            && decision.contains("estimated_rows=2")
    }));
}

#[test]
fn persists_relationship_expansion_across_reopen() {
    let path = unique_test_dir("rel_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
            )
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query(
                    "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.title AS memory, e.name AS entity",
                )
                .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("memory"),
            Some(&Value::String("Graph foundations".to_string()))
        );
        assert_eq!(
            output.rows[0].get("entity"),
            Some(&Value::String("Neo4j".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_pattern_create_uses_single_wal_batch() {
    let path = unique_test_dir("rel_query_batch_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
            )
            .unwrap();
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert!(wal.contains("\tbatch\t"));
    assert!(wal.contains("create_node"));
    assert!(wal.contains("create_rel"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_rollback_discards_buffered_mutations() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.rollback();
    }

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn transaction_commit_applies_buffered_mutations() {
    let mut db = Database::new();
    let output = {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query(
                "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        tx.commit().unwrap()
    };

    assert_eq!(output.rows.len(), 2);
    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory"),
        Some(&Value::String("Runtime strategy".to_string()))
    );
}

#[test]
fn transaction_commit_replays_as_one_wal_batch() {
    let path = unique_test_dir("transaction_batch_wal");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query(
                "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        tx.commit().unwrap();
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert!(wal.contains("\tbatch\t"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query(
                    "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.title AS memory, e.name AS entity",
                )
                .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("entity"),
            Some(&Value::String("Rust".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_rejects_reads() {
    let mut db = Database::new();
    let mut tx = db.begin_transaction();
    let error = tx
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap_err();
    assert!(error.to_string().contains("must be a mutation"));
}

#[test]
fn transaction_commit_updates_property_index() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 42, title: 'Indexed memory'})")
            .unwrap();
        tx.commit().unwrap();
    }

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 42 RETURN m.title AS title")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("IndexNodeSeek"));
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 42 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Indexed memory".to_string()))
    );
}

#[test]
fn read_transaction_keeps_snapshot_before_later_commit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Before snapshot'})")
        .unwrap();

    let mut read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 2, title: 'After snapshot'})")
        .unwrap();

    let before = read_tx
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        before.rows[0].get("title"),
        Some(&Value::String("Before snapshot".to_string()))
    );
    let after = read_tx
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
        .unwrap();
    assert!(after.rows.is_empty());

    let latest = db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        latest.rows[0].get("title"),
        Some(&Value::String("After snapshot".to_string()))
    );
}

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

#[test]
fn read_transaction_keeps_typed_knowledge_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Before snapshot'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})")
            .unwrap();

    let read_tx = db.begin_read_transaction();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(&mut db.catalog, NodeId(0), leaf, "LINKS", BTreeMap::new())
        .unwrap();

    let entity = read_tx.knowledge_entity(&KnowledgeEntityRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
    });
    assert_eq!(entity.graph_commit_epoch, 1);
    assert_eq!(
        entity.entity.as_ref().unwrap().properties.get("title"),
        Some(&Value::String("Before snapshot".to_string()))
    );
    let scoped_entity = read_tx.knowledge_scoped_entity(&KnowledgeScopedEntityRequest {
        entity: KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
        },
        metadata_filters: BTreeMap::from([("title".to_string(), "Before snapshot".to_string())]),
    });
    assert_eq!(scoped_entity.graph_commit_epoch, 1);
    assert!(scoped_entity.entity.is_some());
    let entity_batch = read_tx.knowledge_entity_batch(&KnowledgeEntityBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "root".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "leaf".to_string(),
            },
        ],
    });
    assert_eq!(entity_batch.graph_commit_epoch, 1);
    assert_eq!(entity_batch.found_count, 1);
    assert_eq!(entity_batch.missing_count, 1);
    assert_eq!(entity_batch.filtered_out_count, 0);
    assert!(entity_batch.entities[0].is_some());
    assert!(entity_batch.entities[1].is_none());
    let property_batch = read_tx.knowledge_property_batch(&KnowledgePropertyBatchRequest {
        entities: vec![
            KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "root".to_string(),
            },
            KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "leaf".to_string(),
            },
        ],
        property_names: vec!["title".to_string(), "name".to_string()],
    });
    assert_eq!(property_batch.graph_commit_epoch, 1);
    assert_eq!(property_batch.found_count, 1);
    assert_eq!(property_batch.missing_count, 1);
    assert_eq!(property_batch.filtered_out_count, 0);
    assert_eq!(
        property_batch.rows[0].properties.get("title"),
        Some(&Some(Value::String("Before snapshot".to_string())))
    );
    assert_eq!(property_batch.rows[1].properties.get("name"), Some(&None));

    let snapshot_neighbors = read_tx.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit: 8,
        max_hops: 1,
    });
    assert_eq!(snapshot_neighbors.graph_commit_epoch, 1);
    assert_eq!(snapshot_neighbors.paths.len(), 1);
    assert_eq!(
        snapshot_neighbors.paths[0].target_external_id.as_deref(),
        Some("mid")
    );
    let snapshot_relationships = read_tx.knowledge_relationships(&KnowledgeRelationshipsRequest {
        seeds: vec![KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
        }],
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit_per_seed: 8,
    });
    assert_eq!(snapshot_relationships.graph_commit_epoch, 1);
    assert_eq!(snapshot_relationships.relationship_count, 1);
    assert_eq!(
        snapshot_relationships.groups[0].relationships[0]
            .target_external_id
            .as_deref(),
        Some("mid")
    );

    let latest_neighbors = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        limit: 8,
        max_hops: 1,
    });
    assert_eq!(latest_neighbors.graph_commit_epoch, 3);
    assert_eq!(latest_neighbors.paths.len(), 2);
    assert!(latest_neighbors
        .paths
        .iter()
        .any(|path| path.target_external_id.as_deref() == Some("leaf")));

    let snapshot_paths = read_tx.knowledge_paths(&KnowledgePathRequest {
        source_label: "Memory".to_string(),
        source_external_id: "root".to_string(),
        target_label: "Entity".to_string(),
        target_external_id: "leaf".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        limit: 4,
    });
    assert!(snapshot_paths.paths.is_empty());

    let snapshot_subgraph = read_tx.knowledge_subgraph(&KnowledgeSubgraphRequest {
        label: "Memory".to_string(),
        external_id: "root".to_string(),
        relationship_type: Some("LINKS".to_string()),
        direction: KnowledgeNeighborDirection::Outgoing,
        max_hops: 1,
        node_limit: 8,
        relationship_limit: 8,
    });
    assert_eq!(snapshot_subgraph.nodes.len(), 2);
    assert!(snapshot_subgraph
        .nodes
        .iter()
        .all(|node| node.external_id.as_deref() != Some("leaf")));
}

#[test]
fn read_transaction_retrieves_knowledge_from_pinned_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Snapshot retrieval', content: 'snapshot retrieval root'})-[:MENTIONS]->(:Entity {id: 'before', name: 'Before'})")
            .unwrap();
    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let read_tx = db.begin_read_transaction();
    let after = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("after".to_string())),
                ("name".to_string(), Value::String("After".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            after,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    db.query("CREATE (:Memory {id: 'later', title: 'Snapshot retrieval', content: 'snapshot retrieval later'})")
        .unwrap();

    let snapshot_output = read_tx.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "snapshot retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 4,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 8,
            graph_context_limit: 8,
            graph_context_max_hops: 1,
        },
    );
    assert_eq!(snapshot_output.graph_commit_epoch, 1);
    assert_eq!(snapshot_output.graph_context_paths.len(), 2);
    assert!(snapshot_output
        .graph_context_paths
        .iter()
        .all(|path| path.target_external_id.as_deref() == Some("before")));
    assert!(snapshot_output
        .graph_context_paths
        .iter()
        .all(|path| path.target_external_id.as_deref() != Some("after")));
    assert!(snapshot_output
        .graph_seeds
        .iter()
        .all(|seed| seed.entity.external_id.as_deref() != Some("later")));

    let latest_output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "snapshot retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 4,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 8,
            graph_context_limit: 8,
            graph_context_max_hops: 1,
        },
    );
    assert_eq!(latest_output.graph_commit_epoch, 4);
    assert!(latest_output
        .graph_context_paths
        .iter()
        .any(|path| path.target_external_id.as_deref() == Some("after")));
    assert!(latest_output
        .graph_seeds
        .iter()
        .any(|seed| seed.entity.external_id.as_deref() == Some("later")));
}

#[test]
fn read_transaction_rebuilds_search_projection_from_pinned_snapshot() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'snapshot', title: 'Pinned projection', content: 'snapshot only'})",
    )
    .unwrap();
    let read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'live', title: 'Pinned projection', content: 'live only'})")
        .unwrap();

    let mut snapshot_index = SearchIndex::in_memory();
    let summary = read_tx
        .rebuild_search_projection(&mut snapshot_index, SearchRebuildOptions::default())
        .unwrap();

    assert_eq!(summary.scanned_nodes, 1);
    assert_eq!(summary.indexed_documents, 1);
    assert!(snapshot_index.document("memory:snapshot").is_some());
    assert!(snapshot_index.document("memory:live").is_none());
    assert_eq!(
        snapshot_index
            .projection_freshness()
            .source_graph_commit_epoch,
        Some(1)
    );

    let mut live_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut live_index, SearchRebuildOptions::default())
        .unwrap();
    assert!(live_index.document("memory:snapshot").is_some());
    assert!(live_index.document("memory:live").is_some());
    assert_eq!(
        live_index.projection_freshness().source_graph_commit_epoch,
        Some(2)
    );
}

#[test]
fn read_transaction_repairs_search_projection_metadata_from_pinned_snapshot() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'snapshot', title: 'Pinned projection', content: 'snapshot only', source_id: 'before', space_id: 'snapshot-space'})",
    )
    .unwrap();
    let read_tx = db.begin_read_transaction();
    db.query(
        "MATCH (m:Memory {id: 'snapshot'}) SET m.source_id = 'after', m.space_id = 'live-space'",
    )
    .unwrap();

    let mut snapshot_index = SearchIndex::in_memory();
    snapshot_index
        .upsert(SearchDocument {
            id: "memory:snapshot".to_string(),
            title: "Existing title".to_string(),
            content: "Existing body should stay".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();

    let summary = read_tx
        .repair_search_projection_metadata(&mut snapshot_index, MetadataRepairOptions::default())
        .unwrap();

    assert_eq!(summary.scanned_nodes, 1);
    assert_eq!(summary.repaired_documents, 1);
    let document = snapshot_index.document("memory:snapshot").unwrap();
    assert_eq!(document.title, "Existing title");
    assert_eq!(document.content, "Existing body should stay");
    assert_eq!(document.embedding, Some(vec![1.0, 0.0]));
    assert_eq!(
        document.metadata.get("source_id").map(String::as_str),
        Some("before")
    );
    assert_eq!(
        document.metadata.get("space_id").map(String::as_str),
        Some("snapshot-space")
    );

    db.repair_search_projection_metadata(&mut snapshot_index, MetadataRepairOptions::default())
        .unwrap();
    let live_document = snapshot_index.document("memory:snapshot").unwrap();
    assert_eq!(
        live_document.metadata.get("source_id").map(String::as_str),
        Some("after")
    );
    assert_eq!(
        live_document.metadata.get("space_id").map(String::as_str),
        Some("live-space")
    );
}

#[test]
fn read_transaction_survives_later_checkpoint() {
    let path = unique_test_dir("read_tx_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
            .unwrap();

        let mut read_tx = db.begin_read_transaction();
        db.query("CREATE (:Memory {id: 2, title: 'Checkpoint commit'})")
            .unwrap();
        db.checkpoint().unwrap();

        let pinned = read_tx
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            pinned.rows[0].get("title"),
            Some(&Value::String("Pinned snapshot".to_string()))
        );
        let later = read_tx
            .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
            .unwrap();
        assert!(later.rows.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_transaction_pins_checkpoint_manifest_until_drop() {
    let path = unique_test_dir("read_tx_manifest_pin");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
            .unwrap();

        {
            let _read_tx = db.begin_read_transaction();
            db.query("CREATE (:Memory {id: 2, title: 'Newer commit'})")
                .unwrap();
            db.checkpoint().unwrap();
            let manifest = std::fs::read_to_string(path.join("manifest.skein")).unwrap();
            assert!(manifest.contains("checkpoint_commit_epoch\t2\n"));
            assert!(manifest.contains("oldest_reader_commit_epoch\t1\n"));
            assert!(manifest.contains("safe_reclaim_commit_epoch\t0\n"));
            let watermark = db.storage_reclamation_watermark();
            assert_eq!(watermark.current_commit_epoch, 2);
            assert_eq!(watermark.checkpoint_epoch, Some(1));
            assert_eq!(watermark.checkpoint_commit_epoch, Some(2));
            assert_eq!(watermark.oldest_reader_commit_epoch, Some(1));
            assert_eq!(watermark.safe_reclaim_commit_epoch, 0);
            assert!(watermark.durable);
        }

        db.checkpoint().unwrap();
        let manifest = std::fs::read_to_string(path.join("manifest.skein")).unwrap();
        assert!(manifest.contains("checkpoint_commit_epoch\t2\n"));
        assert!(manifest.contains("oldest_reader_commit_epoch\tnone\n"));
        assert!(manifest.contains("safe_reclaim_commit_epoch\t2\n"));
        let watermark = db.storage_reclamation_watermark();
        assert_eq!(watermark.current_commit_epoch, 2);
        assert_eq!(watermark.checkpoint_epoch, Some(2));
        assert_eq!(watermark.checkpoint_commit_epoch, Some(2));
        assert_eq!(watermark.oldest_reader_commit_epoch, None);
        assert_eq!(watermark.safe_reclaim_commit_epoch, 2);
        assert!(watermark.durable);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn in_memory_reclamation_watermark_tracks_reader_pins() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Pinned snapshot'})")
        .unwrap();

    {
        let _read_tx = db.begin_read_transaction();
        db.query("CREATE (:Memory {id: 2, title: 'Newer commit'})")
            .unwrap();
        let watermark = db.storage_reclamation_watermark();
        assert_eq!(watermark.current_commit_epoch, 2);
        assert_eq!(watermark.checkpoint_epoch, None);
        assert_eq!(watermark.checkpoint_commit_epoch, None);
        assert_eq!(watermark.oldest_reader_commit_epoch, Some(1));
        assert_eq!(watermark.safe_reclaim_commit_epoch, 0);
        assert!(!watermark.durable);
    }

    let watermark = db.storage_reclamation_watermark();
    assert_eq!(watermark.current_commit_epoch, 2);
    assert_eq!(watermark.oldest_reader_commit_epoch, None);
    assert_eq!(watermark.safe_reclaim_commit_epoch, 2);
    assert!(!watermark.durable);
}

#[test]
fn read_transaction_rejects_mutations() {
    let db = Database::new();
    let mut read_tx = db.begin_read_transaction();
    let error = read_tx
        .query("CREATE (:Memory {id: 1, title: 'No writes'})")
        .unwrap_err();
    assert!(error.to_string().contains("must not be a mutation"));
}

#[test]
fn read_transaction_rejects_checkpoint_control() {
    let db = Database::new();
    let mut read_tx = db.begin_read_transaction();
    let error = read_tx.query("CHECKPOINT").unwrap_err();

    assert!(error
        .to_string()
        .contains("CHECKPOINT is not allowed inside a read transaction"));
}

#[test]
fn parameterized_create_and_index_seek_execute_end_to_end() {
    let mut db = Database::new();
    db.query_with_params(
        "CREATE (:Memory {id: $id, title: $title})",
        &BTreeMap::from([
            ("id".to_string(), Value::Int(42)),
            (
                "title".to_string(),
                Value::String("Parameterized memory".to_string()),
            ),
        ]),
    )
    .unwrap();

    let params = BTreeMap::from([("id".to_string(), Value::Int(42))]);
    let explain = db
        .explain_query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
            &params,
        )
        .unwrap();
    assert!(explain.trace.selected_plan.contains("IndexNodeSeek"));

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
            &params,
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Parameterized memory".to_string()))
    );
}

#[test]
fn database_config_caps_optimizer_groups_for_explain() {
    let db = Database::new_with_config(DatabaseConfig {
        max_optimizer_groups: Some(2),
        ..DatabaseConfig::default()
    });

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title ORDER BY title ASC LIMIT 1",
        )
        .unwrap();

    assert!(explain
        .trace
        .warnings
        .iter()
        .any(|warning| warning.contains("optimizer memo budget exceeded")));
    assert!(explain.trace.selected_plan.contains("LimitExec"));
}

#[test]
fn orders_and_limits_by_projected_alias() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Beta'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Gamma'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Alpha'})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC LIMIT $limit",
            &BTreeMap::from([("limit".to_string(), Value::Int(2))]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Alpha".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Beta".to_string()))
    );
}

#[test]
fn orders_by_unprojected_property_with_offset() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'First', rank: 3})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Second', rank: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Third', rank: 2})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY m.rank ASC SKIP 1 LIMIT 1")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Third".to_string()))
    );
}

#[test]
fn distinct_return_deduplicates_before_order_and_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'thread'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN DISTINCT m.kind AS kind ORDER BY kind ASC LIMIT 2")
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );

    let explain = db
        .explain_query("MATCH (m:Memory) RETURN DISTINCT m.kind AS kind ORDER BY kind ASC")
        .unwrap();
    assert!(explain.physical_plan.explain(0).contains("DistinctExec"));
    assert!(explain.trace.selected_plan.contains("DistinctExec"));
}

#[test]
fn database_config_caps_read_query_result_rows() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(2),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three'})")
        .unwrap();

    let error = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("exceeding max_read_result_rows 2"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC LIMIT 2")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
}

#[test]
fn read_transaction_inherits_database_result_row_cap() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(1),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();

    let mut read = db.begin_read_transaction();
    let error = read
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title ASC")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("exceeding max_read_result_rows 1"));
}

#[test]
fn read_only_database_rejects_cypher_mutations_before_writing() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let error = db
        .query("CREATE (:Memory {id: 1, title: 'Blocked'})")
        .unwrap_err();
    assert!(error.to_string().contains("read-only mode"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn read_only_rejected_mutations_do_not_populate_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });

    let error = db
        .query("CREATE (:Memory {id: 1, title: 'Blocked'})")
        .unwrap_err();
    assert!(error.to_string().contains("read-only mode"));

    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.hits, 0);
    assert_eq!(stats.misses, 0);
    assert_eq!(stats.evictions, 0);
}

#[test]
fn match_set_return_projects_updated_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'storage-1', thread_id: 'logical-1', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-2', thread_id: 'logical-2', space_id: 'default'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids SET t.space_id = $target_space_id, t.updated_at = $updated_at RETURN t.thread_id AS thread_id, t.space_id AS space_id",
                &BTreeMap::from([
                    (
                        "thread_ids".to_string(),
                        Value::List(vec![
                            Value::String("logical-1".to_string()),
                            Value::String("logical-2".to_string()),
                        ]),
                    ),
                    (
                        "target_space_id".to_string(),
                        Value::String("archive".to_string()),
                    ),
                    ("updated_at".to_string(), Value::Int(42)),
                ]),
            )
            .unwrap();

    assert_eq!(
        output.rows,
        vec![
            BTreeMap::from([
                (
                    "thread_id".to_string(),
                    Value::String("logical-1".to_string())
                ),
                ("space_id".to_string(), Value::String("archive".to_string())),
            ]),
            BTreeMap::from([
                (
                    "thread_id".to_string(),
                    Value::String("logical-2".to_string())
                ),
                ("space_id".to_string(), Value::String("archive".to_string())),
            ]),
        ]
    );
}

#[test]
fn match_set_return_counts_updated_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending-job', status: 'pending'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'running-job', status: 'running'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'completed-job', status: 'completed'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (j:AugmentationJob) WHERE j.status = 'pending' OR j.status = 'running' SET j.status = 'failed', j.error_message = $reason RETURN count(j)",
                &BTreeMap::from([(
                    "reason".to_string(),
                    Value::String("restart".to_string()),
                )]),
            )
            .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([("count(j)".to_string(), Value::Int(2))])]
    );

    let status = db
        .query("MATCH (j:AugmentationJob) RETURN j.job_id, j.status ORDER BY j.job_id")
        .unwrap();
    assert_eq!(
        status.rows,
        vec![
            BTreeMap::from([
                (
                    "j.job_id".to_string(),
                    Value::String("completed-job".to_string())
                ),
                (
                    "j.status".to_string(),
                    Value::String("completed".to_string())
                ),
            ]),
            BTreeMap::from([
                (
                    "j.job_id".to_string(),
                    Value::String("pending-job".to_string())
                ),
                ("j.status".to_string(), Value::String("failed".to_string())),
            ]),
            BTreeMap::from([
                (
                    "j.job_id".to_string(),
                    Value::String("running-job".to_string())
                ),
                ("j.status".to_string(), Value::String("failed".to_string())),
            ]),
        ]
    );
}

#[test]
fn read_only_database_rejects_match_set_return() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let error = db
        .query("MATCH (t:Thread) SET t.space_id = 'archive' RETURN t.thread_id")
        .unwrap_err();
    assert!(error.to_string().contains("read-only mode"));
}

#[test]
fn read_only_database_rejects_transaction_mutations() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let mut tx = db.begin_transaction();
    let error = tx.query("CREATE (:Memory {id: 1})").unwrap_err();
    assert!(error.to_string().contains("read-only mode"));

    let commit = tx.commit().unwrap_err();
    assert!(commit.to_string().contains("read-only mode"));
}

#[test]
fn read_only_database_rejects_owned_maintenance_writes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let checkpoint = db.checkpoint().unwrap_err();
    assert!(checkpoint.to_string().contains("read-only mode"));

    let maintenance = db.run_schema_maintenance().unwrap_err();
    assert!(maintenance.to_string().contains("read-only mode"));

    let rebuild = db.rebuild_projected_graph_artifacts().unwrap_err();
    assert!(rebuild.to_string().contains("read-only mode"));

    let job = db.schedule_derived_artifact_rebuild();
    assert_eq!(job.status, DerivedArtifactJobStatus::Pending);
    let run = db.run_next_derived_artifact_job().unwrap_err();
    assert!(run.to_string().contains("read-only mode"));
    assert_eq!(
        db.derived_artifact_jobs()[0].status,
        DerivedArtifactJobStatus::Pending
    );
}

#[test]
fn negative_limit_is_rejected_before_execution() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Should remain'})")
        .unwrap();

    let error = db
        .query("MATCH (m:Memory) RETURN m.title AS title LIMIT -1")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("limit must be a non-negative integer"));
    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn filters_with_null_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Missing deleted'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Explicit null', deleted_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Deleted', deleted_at: 'now'})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.deleted_at IS NULL RETURN m.title AS title ORDER BY title ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Explicit null".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Missing deleted".to_string()))
    );

    let output = db
        .query("MATCH (m:Memory) WHERE m.deleted_at IS NOT NULL RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Deleted".to_string()))
    );
}

#[test]
fn filters_with_literal_and_parameterized_in_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id IN [1, 3] RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Three".to_string()))
    );

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.title AS title ORDER BY m.id DESC",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            )]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Two".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("One".to_string()))
    );

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN [1, $id] RETURN m.title AS title ORDER BY m.id ASC",
            &BTreeMap::from([("id".to_string(), Value::Int(3))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Three".to_string()))
    );

    let error = db
        .query("MATCH (m:Memory) WHERE m.id IN [1, $id] RETURN m.title AS title")
        .unwrap_err();
    assert!(error.to_string().contains("missing parameter '$id'"));

    db.query("CREATE (:Entity {id: 'e1', name: 'Rust', aliases: ['Ferris', 'Rustacean']})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e2', name: 'Kuzu', aliases: ['Graph']})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e3', name: 'Plain', aliases: 'Ferris'})")
        .unwrap();
    let output = db
        .query_with_params(
            "MATCH (e:Entity) WHERE list_contains(e.aliases, $name) RETURN e.id AS id",
            &BTreeMap::from([("name".to_string(), Value::String("Ferris".to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("e1".to_string()))
    );
}

#[test]
fn filters_with_string_prefix_and_suffix_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Runtime graph'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Graph runtime'})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.title STARTS WITH 'Graph' RETURN m.title AS title ORDER BY title ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Graph runtime".to_string()))
    );

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.title ENDS WITH $suffix RETURN m.title AS title",
            &BTreeMap::from([("suffix".to_string(), Value::String("graph".to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Runtime graph".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory) WHERE m.title STARTS WITH 'Graph' SET m.kind = 'prefix'")
        .unwrap();
    assert_eq!(updated.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.kind = 'prefix' RETURN count(*) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn filters_with_not_equal_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', title: 'Note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', title: 'Task'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'thread', title: 'Thread'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.kind <> 'task' RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );

    let updated = db
        .query_with_params(
            "MATCH (m:Memory) WHERE id(m) <> $id SET m.visible = true",
            &BTreeMap::from([("id".to_string(), Value::Int(1))]),
        )
        .unwrap();
    assert_eq!(updated.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.visible = true RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );
}

#[test]
fn filters_with_not_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', score: 3, title: 'Note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', score: 15, title: 'Task'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', score: 25, title: 'Skip'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'thread', score: 30, title: 'Thread'})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE NOT (m.kind = 'task' OR m.score < 10) RETURN m.title AS title ORDER BY title ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Thread".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory) WHERE NOT m.kind = 'task' SET m.visible = true")
        .unwrap();
    assert_eq!(updated.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.visible = true RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );
}

#[test]
fn relationship_existence_predicates_cover_nowledge_orphan_entities() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'orphan', name: 'Orphan', entity_type: 'Concept'})")
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 'm1'})-[:MENTIONS]->(:Entity {id: 'mentioned', name: 'Mentioned', entity_type: 'Concept'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Entity {id: 'related-a', name: 'Related A', entity_type: 'Concept'})-[:RELATES_TO]->(:Entity {id: 'related-b', name: 'Related B', entity_type: 'Concept'})",
        )
        .unwrap();
    db.query("CREATE (:Entity {id: 'labeled', name: 'Labeled', entity_type: 'Concept'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label-1'})").unwrap();
    db.query(
        "MATCH (e:Entity {id: 'labeled'}), (l:Label {id: 'label-1'}) CREATE (e)-[:HAS_LABEL]->(l)",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (e:Entity)
                 WHERE NOT (e)<-[:MENTIONS]-(:Memory)
                   AND NOT (e)-[:RELATES_TO]-()
                   AND NOT (e)-[:HAS_LABEL]-()
                 RETURN e.id AS id, e.name AS name, e.entity_type AS entity_type",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("orphan".to_string()))
    );
}

#[test]
fn filters_with_or_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', score: 3, title: 'Note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', score: 15, title: 'Task'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', score: 25, title: 'Skip'})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.kind = 'note' OR m.score >= 10 AND m.score < 20 RETURN m.title AS title ORDER BY title ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Task".to_string()))
    );

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.kind = 'note' OR m.score >= 10 RETURN m.title AS title",
        )
        .unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("FilterExec"));
    assert!(physical_plan.contains("SeqNodeScan"));
    assert!(!physical_plan.contains("IndexNodeSeek"));
    assert!(!physical_plan.contains("IndexNodeRangeSeek"));
}

#[test]
fn filters_with_parenthesized_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', score: 3, title: 'Low note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note', score: 30, title: 'High note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'thread', score: 20, title: 'Thread'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'task', score: 25, title: 'Task'})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE (m.kind = 'note' OR m.kind = 'thread') AND m.score >= 10 RETURN m.title AS title ORDER BY title ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("High note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );
}

#[test]
fn match_node_property_patterns_filter_reads_and_bind_parameters() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Crystal', is_crystal: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Raw', is_crystal: false})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory {id: $id, is_crystal: true}) RETURN m.title AS title",
            &BTreeMap::from([("id".to_string(), Value::Int(1))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Crystal".to_string()))
    );

    let explain = db
        .explain_query("MATCH (m:Memory {id: 1}) RETURN m.title AS title")
        .unwrap();
    assert!(explain.physical_plan.explain(0).contains("IndexNodeSeek"));
}

#[test]
fn match_node_property_patterns_filter_relationship_endpoints() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 2, kind: 'note'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 3, is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 4, kind: 'thread'})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(s:Memory {kind: 'note'}) RETURN DISTINCT s.id AS id",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
}

#[test]
fn undirected_one_hop_relationship_patterns_match_both_directions() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:EVOLVES]->(:Memory {id: 2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})-[:EVOLVES]->(:Memory {id: 1})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory {id: 1})-[:EVOLVES]-(other:Memory) RETURN DISTINCT other.id AS id ORDER BY id ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));

    let error = db
        .query("MATCH (m:Memory)-[:EVOLVES*1..2]-(other:Memory) RETURN other.id AS id")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("non-outgoing relationship patterns are supported only for one-hop patterns"));
}

#[test]
fn one_hop_relationship_patterns_follow_ordered_adjacency_view() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 0})").unwrap();
    db.query("CREATE (:Memory {id: 10})").unwrap();
    db.query("CREATE (:Memory {id: 11})").unwrap();
    db.query("CREATE (:Memory {id: 12})").unwrap();
    db.query("CREATE (:Entity {id: 1})").unwrap();
    db.query("CREATE (:Entity {id: 2})").unwrap();
    db.query("CREATE (:Entity {id: 3})").unwrap();
    db.query("CREATE (:Entity {id: 99})").unwrap();
    db.query("MATCH (m:Memory {id: 0}), (e:Entity {id: 3}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 0}), (e:Entity {id: 1}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 0}), (e:Entity {id: 2}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 12}), (e:Entity {id: 99}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 10}), (e:Entity {id: 99}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 11}), (e:Entity {id: 99}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let outgoing = db
        .query("MATCH (m:Memory {id: 0})-[:MENTIONS]->(e:Entity) RETURN e.id AS id")
        .unwrap();
    let incoming = db
        .query("MATCH (e:Entity {id: 99})<-[:MENTIONS]-(m:Memory) RETURN m.id AS id")
        .unwrap();

    assert_eq!(
        outgoing
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(2), Value::Int(3)]
    );
    assert_eq!(
        incoming
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![Value::Int(10), Value::Int(11), Value::Int(12)]
    );
}

#[test]
fn incoming_one_hop_relationship_patterns_match_nowledge_reads() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 3}]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2})-[:MENTIONS {weight: 4}]->(:Entity {id: 11})")
        .unwrap();

    let output = db
        .query("MATCH (e:Entity {id: 10})<-[:MENTIONS]-(m:Memory) RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));

    let output = db
        .query("MATCH (e:Entity {id: 10})<-[r:MENTIONS]-(m:Memory) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));

    let error = db
        .query("MATCH (e:Entity)<-[:MENTIONS*1..2]-(m:Memory) RETURN m.id AS id")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("non-outgoing relationship patterns are supported only for one-hop patterns"));
}

#[test]
fn untyped_one_hop_relationship_patterns_project_relationship_type() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10})-[:RELATES_TO]->(:Entity {id: 11})")
        .unwrap();

    let output = db
            .query(
                "MATCH (a)-[r]->(b) WHERE a.id IN [1, 10] AND b.id IN [10, 11] RETURN a.id AS source, b.id AS target, label(r) AS rel_type ORDER BY source ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("source"), Some(&Value::Int(1)));
    assert_eq!(output.rows[0].get("target"), Some(&Value::Int(10)));
    assert_eq!(
        output.rows[0].get("rel_type"),
        Some(&Value::String("MENTIONS".to_string()))
    );
    assert_eq!(output.rows[1].get("source"), Some(&Value::Int(10)));
    assert_eq!(output.rows[1].get("target"), Some(&Value::Int(11)));
    assert_eq!(
        output.rows[1].get("rel_type"),
        Some(&Value::String("RELATES_TO".to_string()))
    );

    let error = db
        .query("MATCH (a)-[r*1..2]->(b) RETURN label(r) AS rel_type")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("untyped relationship patterns are supported only for one-hop patterns"));
}

#[test]
fn return_projection_functions_cover_nowledge_fallback_reads() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, content: 'Projection fallback content'})-[:MENTIONS {confidence: 0.7}]->(:Entity {id: 10})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN COALESCE(m.title, LEFT(COALESCE(m.content, ''), 10)) AS label, COALESCE(r.strength, r.confidence, 0.5) AS weight",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("label"),
        Some(&Value::String("Projection".to_string()))
    );
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Float(0.7)));

    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN COALESCE(null, e.id) AS fallback")
        .unwrap();
    assert_eq!(output.rows[0].get("fallback"), Some(&Value::Int(10)));
}

#[test]
fn order_by_projection_functions_cover_nowledge_rank_fallbacks() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, importance: 0.4})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, pagerank_score: 0.9, importance: 0.1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})").unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) RETURN m.id AS id ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));
    assert_eq!(output.rows[2].get("id"), Some(&Value::Int(1)));
}

#[test]
fn predicate_projection_functions_cover_nowledge_fallback_filters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations', created_at: 5})")
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy', last_accessed_at: 15, is_crystal: false})",
        )
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Graph crystal', created_at: 20, is_crystal: true})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE COALESCE(m.is_crystal, false) = false RETURN m.id AS id ORDER BY id ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));

    let output = db
            .query_with_params(
                "MATCH (m:Memory) WHERE COALESCE(m.created_at, m.last_accessed_at) >= $cutoff AND LEFT(COALESCE(m.title, ''), 5) = 'Graph' RETURN m.id AS id",
                &BTreeMap::from([("cutoff".to_string(), Value::Int(10))]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(3)));
}

#[test]
fn lower_expression_predicates_cover_nowledge_grep_filters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations', content: 'Runtime notes'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Other', content: 'needle in content'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Rust'})").unwrap();

    let output = db
            .query_with_params(
                "MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) CONTAINS LOWER($needle) OR LOWER(COALESCE(m.title, '')) CONTAINS LOWER($needle) RETURN m.id AS id ORDER BY id ASC",
                &BTreeMap::from([("needle".to_string(), Value::String("GRAPH".to_string()))]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));

    let output = db
        .query_with_params(
            "MATCH (e:Entity) WHERE LOWER(e.name) = LOWER($mention) RETURN e.id AS id",
            &BTreeMap::from([("mention".to_string(), Value::String("rust".to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(10)));
}

#[test]
fn anonymous_relationship_endpoints_support_count_reads() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2})-[:MENTIONS]->(:Entity {id: 11})")
        .unwrap();
    db.query("CREATE (:Entity {id: 12})-[:RELATES_TO]->(:Entity {id: 13})")
        .unwrap();

    let output = db
        .query("MATCH ()-[r:MENTIONS]->() RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));

    let output = db
        .query("MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn optional_match_count_after_node_match_covers_thread_message_count() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 't1'})-[:CONTAINS]->(:Message {id: 'msg1'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
                &BTreeMap::from([(
                    "thread_uuid".to_string(),
                    Value::String("t1".to_string()),
                )]),
            )
            .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(1)));

    let missing = db
            .query_with_params(
                "MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
                &BTreeMap::from([(
                    "thread_uuid".to_string(),
                    Value::String("missing".to_string()),
                )]),
            )
            .unwrap();
    assert_eq!(missing.rows[0].get("count(m)"), Some(&Value::Int(0)));
}

#[test]
fn optional_match_count_after_relationship_match_covers_legacy_tail_refs() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 't1'})-[:CONTAINS]->(:Message {id: 'msg1', order_index: 1})")
        .unwrap();
    db.query("CREATE (:Message {id: 'msg2', order_index: 2})")
        .unwrap();
    db.query("MATCH (t:Thread {id: 't1'}), (m:Message {id: 'msg2'}) CREATE (t)-[:CONTAINS]->(m)")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm1'})").unwrap();
    db.query("MATCH (mem:Memory {id: 'm1'}), (msg:Message {id: 'msg1'}) CREATE (mem)-[:EXTRACTED_FROM]->(msg)")
            .unwrap();

    let output = db
            .query_with_params(
                "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) WHERE m.order_index >= $start_index OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
                &BTreeMap::from([
                    ("thread_uuid".to_string(), Value::String("t1".to_string())),
                    ("start_index".to_string(), Value::Int(0)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows[0].get("count(r)"), Some(&Value::Int(1)));

    let missing = db
            .query_with_params(
                "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) WHERE m.order_index >= $start_index OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
                &BTreeMap::from([
                    ("thread_uuid".to_string(), Value::String("t1".to_string())),
                    ("start_index".to_string(), Value::Int(2)),
                ]),
            )
            .unwrap();
    assert_eq!(missing.rows[0].get("count(r)"), Some(&Value::Int(0)));
}

#[test]
fn optional_match_with_degree_projection_covers_top_entities() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'e1', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e2', name: 'Beta'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e3', name: 'Gamma'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e4', name: 'Isolated'})")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'e1'}), (b:Entity {id: 'e2'}) CREATE (a)-[:RELATES_TO]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'e2'}), (b:Entity {id: 'e3'}) CREATE (a)-[:RELATES_TO]->(b)")
        .unwrap();

    let output = db
        .query(
            "MATCH (e:Entity)
                 OPTIONAL MATCH (e)-[r]-()
                 WITH e, COUNT(r) as degree
                 RETURN e.id, e.name, degree
                 ORDER BY degree DESC
                 LIMIT 10",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 4);
    assert_eq!(
        output.rows[0].get("e.id"),
        Some(&Value::String("e2".into()))
    );
    assert_eq!(output.rows[0].get("degree"), Some(&Value::Int(2)));
    let isolated = output
        .rows
        .iter()
        .find(|row| row.get("e.id") == Some(&Value::String("e4".into())))
        .expect("isolated entity row");
    assert_eq!(
        isolated.get("e.name"),
        Some(&Value::String("Isolated".into()))
    );
    assert_eq!(isolated.get("degree"), Some(&Value::Int(0)));
}

#[test]
fn optional_match_with_target_count_projection_covers_label_usage() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'l1', name: 'Important', canonical_name: 'important'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'l2', name: 'Unused', canonical_name: 'unused'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm1'})").unwrap();
    db.query("CREATE (:Entity {id: 'e1'})").unwrap();
    db.query("MATCH (m:Memory {id: 'm1'}), (l:Label {id: 'l1'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (e:Entity {id: 'e1'}), (l:Label {id: 'l1'}) CREATE (e)-[:HAS_LABEL]->(l)")
        .unwrap();

    let output = db
        .query(
            "MATCH (l:Label)
                 OPTIONAL MATCH (l)<-[:HAS_LABEL]-(n)
                 WITH l, COUNT(n) as usage_count
                 RETURN l.id, l.name, usage_count
                 ORDER BY usage_count DESC, l.id ASC",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("l.id"),
        Some(&Value::String("l1".into()))
    );
    assert_eq!(output.rows[0].get("usage_count"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("l.id"),
        Some(&Value::String("l2".into()))
    );
    assert_eq!(output.rows[1].get("usage_count"), Some(&Value::Int(0)));
}

#[test]
fn normalized_space_case_predicates_cover_thread_move_selection() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'storage-1', thread_id: 'logical-1'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-2', thread_id: 'logical-2', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-3', thread_id: 'logical-3', space_id: 'team'})")
        .unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Thread {{id: 'extra-storage-{id}', thread_id: 'extra-logical-{id}', space_id: 'team'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Thread(thread_id)").unwrap();

    let source_parameters = BTreeMap::from([
        (
            "thread_ids".to_string(),
            Value::List(vec![
                Value::String("logical-1".into()),
                Value::String("logical-2".into()),
                Value::String("logical-3".into()),
            ]),
        ),
        (
            "source_space_id".to_string(),
            Value::String("default".into()),
        ),
    ]);
    let source_query = "MATCH (t:Thread)
                 WHERE t.thread_id IN $thread_ids
                   AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id
                 RETURN t.id, t.thread_id, t.space_id
                 ORDER BY t.id";
    let explain = db
        .explain_query_with_params(source_query, &source_parameters)
        .unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeMultiSeek"));
    assert!(physical_plan.contains("FilterExec"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeMultiSeek")));
    let source_rows = db
        .query_with_params(source_query, &source_parameters)
        .unwrap();
    assert_eq!(source_rows.rows.len(), 2);
    assert_eq!(
        source_rows.rows[0].get("t.id"),
        Some(&Value::String("storage-1".into()))
    );
    assert_eq!(
        source_rows.rows[1].get("t.id"),
        Some(&Value::String("storage-2".into()))
    );

    let target_rows = db
            .query_with_params(
                "MATCH (t:Thread)
                 WHERE t.thread_id IN $candidate_ids
                   AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target_space_id
                 RETURN t.thread_id
                 ORDER BY t.thread_id",
                &BTreeMap::from([
                    (
                        "candidate_ids".to_string(),
                        Value::List(vec![
                            Value::String("logical-1".into()),
                            Value::String("logical-2".into()),
                            Value::String("logical-3".into()),
                        ]),
                    ),
                    (
                        "target_space_id".to_string(),
                        Value::String("default".into()),
                    ),
                ]),
            )
            .unwrap();
    assert_eq!(target_rows.rows.len(), 1);
    assert_eq!(
        target_rows.rows[0].get("t.thread_id"),
        Some(&Value::String("logical-3".into()))
    );
}

#[test]
fn node_detail_neighbor_counts_use_distinct_neighbors_and_edges() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'n1', name: 'One'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'n2', name: 'Two'})")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n1'}), (b:Entity {id: 'n2'}) CREATE (a)-[:RELATES_TO]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n2'}), (b:Entity {id: 'n1'}) CREATE (a)-[:MENTIONS]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n1'}), (b:Entity {id: 'n1'}) CREATE (a)-[:SELF]->(b)")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (n)-[r]-(neighbor)
                 WHERE n.id = $node_id
                 RETURN COUNT(DISTINCT neighbor), COUNT(r)",
            &BTreeMap::from([("node_id".to_string(), Value::String("n1".into()))]),
        )
        .unwrap();

    assert_eq!(
        output.rows[0].get("count(DISTINCT neighbor)"),
        Some(&Value::Int(2))
    );
    assert_eq!(output.rows[0].get("count(r)"), Some(&Value::Int(3)));
}

#[test]
fn converging_relationship_pattern_counts_distinct_source_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'target'})").unwrap();
    db.query("CREATE (:Memory {id: 'other-1'})").unwrap();
    db.query("CREATE (:Memory {id: 'other-2'})").unwrap();
    db.query("CREATE (:Entity {id: 'shared'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'target'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'other-1'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'other-2'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (other:Memory)-[:MENTIONS]->(:Entity)<-[:MENTIONS]-(m:Memory {id: 'target'})
                 WHERE other.id <> 'target'
                 RETURN other.id AS id
                 ORDER BY id ASC",
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![
            BTreeMap::from([("id".to_string(), Value::String("other-1".to_string()))]),
            BTreeMap::from([("id".to_string(), Value::String("other-2".to_string()))]),
        ]
    );

    let output = db
        .query(
            "MATCH (other:Memory)-[:MENTIONS]->(:Entity)<-[:MENTIONS]-(m:Memory {id: 'target'})
                 WHERE other.id <> 'target'
                 RETURN COUNT(DISTINCT other)",
        )
        .unwrap();

    assert_eq!(
        output.rows[0].get("count(DISTINCT other)"),
        Some(&Value::Int(2))
    );
}

#[test]
fn creates_and_filters_escaped_string_literals() {
    let mut db = Database::new();
    db.query(r#"CREATE (:Memory {id: 1, title: 'It\'s graph\\ready'})"#)
        .unwrap();

    let output = db
        .query(r#"MATCH (m:Memory) WHERE m.title = 'It\'s graph\\ready' RETURN m.title AS title"#)
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("It's graph\\ready".to_string()))
    );
}

#[test]
fn in_predicate_rejects_non_list_parameter() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();

    let error = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.title AS title",
            &BTreeMap::from([("ids".to_string(), Value::Int(1))]),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("IN predicate requires a list value"));
}

#[test]
fn relationship_variable_rejects_bounded_multi_hop_return() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:LINKS {weight: 1}]->(:Entity {id: 10})")
        .unwrap();

    let error = db
        .query("MATCH (m:Memory)-[r:LINKS*1..2]->(e:Entity) RETURN r.weight AS weight")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship variables are supported only for one-hop patterns"));
}

#[test]
fn counts_single_node_matches() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Rust'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(*) AS total")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));

    let explain = db
        .explain_query("MATCH (m:Memory) RETURN count(*) AS total")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("AggregateExec"));
}

#[test]
fn counts_relationship_expansion_matches() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'One'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 2, title: 'Two'})-[:MENTIONS]->(:Entity {id: 11, name: 'Kuzu'})",
    )
    .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn returns_filters_orders_and_counts_relationship_properties() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'One'})-[:MENTIONS {weight: 2}]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, title: 'Two'})-[:MENTIONS {weight: 5}]->(:Entity {id: 11, name: 'Kuzu'})",
        )
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 3, title: 'Three'})-[:MENTIONS]->(:Entity {id: 12, name: 'Neo4j'})",
    )
    .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE r.weight > 1 RETURN e.name AS entity, r.weight AS weight ORDER BY r.weight DESC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Kuzu".to_string()))
    );
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Int(5)));
    assert_eq!(
        output.rows[1].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );
    assert_eq!(output.rows[1].get("weight"), Some(&Value::Int(2)));

    let output = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN count(r) AS rels, count(r.weight) AS weighted",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("rels"), Some(&Value::Int(3)));
    assert_eq!(output.rows[0].get("weighted"), Some(&Value::Int(2)));

    let output = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN count(DISTINCT m) AS memories, count(DISTINCT r.weight) AS weights",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("memories"), Some(&Value::Int(3)));
    assert_eq!(output.rows[0].get("weights"), Some(&Value::Int(2)));

    let output = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN id(m) AS memory_id, id(r) AS rel_id, e.name AS entity ORDER BY rel_id DESC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(output.rows[2].get("memory_id"), Some(&Value::Int(0)));
    assert_eq!(output.rows[2].get("rel_id"), Some(&Value::Int(0)));

    let output = db
            .query_with_params(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(m) = $memory_id AND id(r) IN [0, $rel_id] RETURN id(r) AS rel_id, e.name AS entity ORDER BY id(r) DESC",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::Int(0)),
                    ("rel_id".to_string(), Value::Int(2)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(0)));
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory) WHERE id(m) = 0 SET m.title = 'Updated'")
        .unwrap();
    assert_eq!(updated.rows.len(), 1);
    assert_eq!(updated.rows[0].get("node_id"), Some(&Value::Int(0)));
    let output = db
        .query("MATCH (m:Memory) WHERE id(m) = 0 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Updated".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(r) = 1 SET r.weight = 9")
        .unwrap();
    assert_eq!(updated.rows.len(), 1);
    assert_eq!(updated.rows[0].get("rel_id"), Some(&Value::Int(1)));
    let output = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(r) = 1 RETURN r.weight AS weight",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Int(9)));

    let deleted = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(r) = 2 DELETE r")
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);
    assert_eq!(deleted.rows[0].get("rel_id"), Some(&Value::Int(2)));
    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN id(r) AS rel_id")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert!(output
        .rows
        .iter()
        .all(|row| row.get("rel_id") != Some(&Value::Int(2))));
}

#[test]
fn count_property_ignores_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One', deleted_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three', deleted_at: 'now'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(m.deleted_at) AS deleted")
        .unwrap();
    assert_eq!(output.rows[0].get("deleted"), Some(&Value::Int(1)));
}

#[test]
fn count_distinct_property_ignores_duplicate_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', status: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', status: 'open'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'task', status: 'open'})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) RETURN count(DISTINCT m.kind) AS kinds, count(DISTINCT m.status) AS statuses",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("kinds"), Some(&Value::Int(2)));
    assert_eq!(output.rows[0].get("statuses"), Some(&Value::Int(1)));
}

#[test]
fn avg_property_ignores_missing_null_and_non_numeric_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, is_crystal: false, decay_score_cached: 0.2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, is_crystal: false, decay_score_cached: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, is_crystal: false, decay_score_cached: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, is_crystal: false, decay_score_cached: 'skip'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 5, is_crystal: true, decay_score_cached: 10.0})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.is_crystal = false RETURN count(m) AS total, avg(m.decay_score_cached) AS avg_decay",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(4)));
    let Some(Value::Float(avg_decay)) = output.rows[0].get("avg_decay") else {
        panic!("expected floating point average");
    };
    assert!((avg_decay - 0.6).abs() < f64::EPSILON);

    let output = db
        .query("MATCH (m:Memory) WHERE m.is_crystal = true RETURN avg(m.missing) AS avg_decay")
        .unwrap();
    assert_eq!(output.rows[0].get("avg_decay"), Some(&Value::Null));
}

#[test]
fn max_property_ignores_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, updated_at: 5})").unwrap();
    db.query("CREATE (:Memory {id: 2, updated_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, updated_at: 9})").unwrap();
    db.query("CREATE (:Memory {id: 4})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(m), max(m.updated_at)")
        .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(4)));
    assert_eq!(
        output.rows[0].get("max(m.updated_at)"),
        Some(&Value::Int(9))
    );

    let output = db
        .query("MATCH (m:Missing) RETURN max(m.updated_at)")
        .unwrap();
    assert_eq!(output.rows[0].get("max(m.updated_at)"), Some(&Value::Null));
}

#[test]
fn aggregate_order_by_alias_and_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(*) AS total ORDER BY total DESC LIMIT 1")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn grouped_count_aggregates_by_projected_property() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task'})").unwrap();

    let output = db
            .query("MATCH (m:Memory) RETURN m.kind AS kind, count(*) AS total ORDER BY total DESC, kind ASC")
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );
    assert_eq!(output.rows[1].get("total"), Some(&Value::Int(1)));
}

#[test]
fn grouped_count_aggregates_relationship_matches() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, kind: 'note'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, kind: 'note'})-[:MENTIONS {weight: 1}]->(:Entity {id: 11, name: 'Kuzu'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 3, kind: 'task'})-[:MENTIONS {weight: 5}]->(:Entity {id: 12, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.kind AS kind, count(e) AS total ORDER BY kind ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );
    assert_eq!(output.rows[1].get("total"), Some(&Value::Int(1)));

    let output = db
        .query("MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(*), min(r.weight)")
        .unwrap();
    assert_eq!(output.rows[0].get("count(*)"), Some(&Value::Int(3)));
    assert_eq!(output.rows[0].get("min(r.weight)"), Some(&Value::Int(1)));
}

#[test]
fn with_collect_distinct_property_returns_source_ids() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'crystal-1', is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 'source-2'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'source-1'})").unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-1'}), (s:Memory {id: 'source-1'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-1'}), (s:Memory {id: 'source-1'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'crystal-2', is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 'source-3'})",
    )
    .unwrap();

    let output = db
        .query_with_params(
            "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::String("crystal-1".to_string())]),
            )]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("c.id"),
        Some(&Value::String("crystal-1".to_string()))
    );
    assert_eq!(
        output.rows[0].get("source_ids"),
        Some(&Value::List(vec![
            Value::String("source-1".to_string()),
            Value::String("source-2".to_string()),
        ]))
    );
}

#[test]
fn with_count_and_collect_preserves_group_node_for_ordering() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity-1', community_id: 42})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-2', community_id: 42})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-1', title: 'First', importance: 0.8})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-2', title: 'Second', importance: 0.9})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-1'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-2'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-2'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-2'}), (e:Entity {id: 'entity-1'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (e:Entity {community_id: $community_id})<-[:MENTIONS]-(m:Memory) WITH m, COUNT(e) AS entity_count, COLLECT(DISTINCT e.id) AS entity_ids RETURN m, entity_count, entity_ids ORDER BY entity_count DESC, m.importance DESC LIMIT 1",
            &BTreeMap::from([("community_id".to_string(), Value::Int(42))]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(memory)) = output.rows[0].get("m") else {
        panic!("expected grouped memory node map");
    };
    assert_eq!(
        memory.get("id"),
        Some(&Value::String("memory-1".to_string()))
    );
    assert_eq!(output.rows[0].get("entity_count"), Some(&Value::Int(3)));
    assert_eq!(
        output.rows[0].get("entity_ids"),
        Some(&Value::List(vec![
            Value::String("entity-1".to_string()),
            Value::String("entity-2".to_string()),
        ]))
    );
}

#[test]
fn merge_node_is_idempotent() {
    let mut db = Database::new();
    let first = db
        .query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    let second = db
        .query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();

    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("node_id"), second.rows[0].get("node_id"));

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn merge_node_on_create_set_writes_only_when_created() {
    let mut db = Database::new();
    let first = db
            .query(
                "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP(), m.note = 'created'",
            )
            .unwrap();
    let second = db
        .query(
            "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.note = 'overwritten'",
        )
        .unwrap();

    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("node_id"), second.rows[0].get("node_id"));

    let output = db
            .query(
                "MATCH (m:SchemaMigrationLog {id: 'migration-1'}) RETURN m.note AS note, m.applied_at AS applied_at",
            )
            .unwrap();
    assert_eq!(
        output.rows[0].get("note"),
        Some(&Value::String("created".to_string()))
    );
    assert!(matches!(
        output.rows[0].get("applied_at"),
        Some(Value::Int(value)) if *value > 0
    ));
}

#[test]
fn merge_node_on_match_set_updates_only_when_matched() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Label {id: 'label-1', name: 'Important', canonical_name: null, updated_at: 1})",
    )
    .unwrap();

    let matched = db
            .query_with_params(
                "MERGE (l:Label {id: $label_id}) ON CREATE SET l.name = $label_name, l.canonical_name = $canonical, l.updated_at = 10 ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)",
                &BTreeMap::from([
                    ("label_id".to_string(), Value::String("label-1".to_string())),
                    ("label_name".to_string(), Value::String("Ignored".to_string())),
                    (
                        "canonical".to_string(),
                        Value::String("important".to_string()),
                    ),
                    ("now".to_string(), Value::Int(42)),
                ]),
            )
            .unwrap();
    assert_eq!(matched.rows[0].get("created"), Some(&Value::Bool(false)));

    let created = db
            .query_with_params(
                "MERGE (l:Label {id: $label_id}) ON CREATE SET l.name = $label_name, l.canonical_name = $canonical, l.updated_at = 10 ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)",
                &BTreeMap::from([
                    ("label_id".to_string(), Value::String("label-2".to_string())),
                    ("label_name".to_string(), Value::String("New".to_string())),
                    ("canonical".to_string(), Value::String("new".to_string())),
                    ("now".to_string(), Value::Int(99)),
                ]),
            )
            .unwrap();
    assert_eq!(created.rows[0].get("created"), Some(&Value::Bool(true)));

    let output = db
            .query(
                "MATCH (l:Label) RETURN l.id AS id, l.name AS name, l.canonical_name AS canonical, l.updated_at AS updated ORDER BY l.id ASC",
            )
            .unwrap();
    assert_eq!(
        output.rows[0].get("canonical"),
        Some(&Value::String("important".to_string()))
    );
    assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(42)));
    assert_eq!(
        output.rows[1].get("canonical"),
        Some(&Value::String("new".to_string()))
    );
    assert_eq!(output.rows[1].get("updated"), Some(&Value::Int(10)));
}

#[test]
fn merge_node_post_set_updates_created_and_matched_nodes() {
    let path = unique_test_dir("merge_node_post_set");
    {
        let mut db = Database::open(&path).unwrap();
        let first = db
                .query(
                    "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = true, m.pagerank_algorithm = 'pagerank', m.pagerank_iterations = 20, m.updated_at = CURRENT_TIMESTAMP()",
                )
                .unwrap();
        let second = db
                .query(
                    "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = false, m.pagerank_computed_at = null, m.updated_at = CURRENT_TIMESTAMP()",
                )
                .unwrap();

        assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
        assert_eq!(first.rows[0].get("node_id"), second.rows[0].get("node_id"));

        let output = db
                .query(
                    "MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_algorithm AS algorithm, m.pagerank_iterations AS iterations, m.pagerank_computed_at AS computed_at, count(m.updated_at) AS updated",
                )
                .unwrap();
        assert_eq!(output.rows[0].get("applied"), Some(&Value::Bool(false)));
        assert_eq!(
            output.rows[0].get("algorithm"),
            Some(&Value::String("pagerank".to_string()))
        );
        assert_eq!(output.rows[0].get("iterations"), Some(&Value::Int(20)));
        assert_eq!(output.rows[0].get("computed_at"), Some(&Value::Null));
        assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(1)));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.matches("create_node").count(), 1);
    assert_eq!(wal.matches("set_node_property").count(), 3);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn parameterized_merge_binds_before_storage_access() {
    let mut db = Database::new();
    db.query_with_params(
        "MERGE (:Memory {id: $id, title: $title})",
        &BTreeMap::from([
            ("id".to_string(), Value::Int(7)),
            (
                "title".to_string(),
                Value::String("Parameterized merge".to_string()),
            ),
        ]),
    )
    .unwrap();
    let error = db.query("MERGE (:Memory {id: $missing})").unwrap_err();
    assert!(error.to_string().contains("missing parameter '$missing'"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn merge_relationship_is_idempotent() {
    let mut db = Database::new();
    let first = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
    let second = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();

    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("rel_id"), second.rows[0].get("rel_id"));

    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn merge_relationship_reuses_existing_endpoint_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Existing memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Existing entity'})")
        .unwrap();

    let output = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Existing memory'})-[:MENTIONS]->(:Entity {id: 10, name: 'Existing entity'})",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    let memories = db.query("MATCH (m:Memory) RETURN m.id AS id").unwrap();
    assert_eq!(memories.rows.len(), 1);
    let entities = db.query("MATCH (e:Entity) RETURN e.id AS id").unwrap();
    assert_eq!(entities.rows.len(), 1);
    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert_eq!(rels.rows.len(), 1);
}

#[test]
fn create_relationship_between_matched_nodes_covers_source_provenance_write() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {chunk_index: $chunk_index}]->(s)",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("source_id".to_string(), Value::String("s1".to_string())),
                    ("chunk_index".to_string(), Value::Int(7)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
            .query(
                "MATCH (m:Memory {id: 'm1'})-[r:SOURCED_FROM]->(s:Source {id: 's1'}) RETURN count(r) AS total, min(r.chunk_index) AS first_chunk",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(rels.rows[0].get("first_chunk"), Some(&Value::Int(7)));
}

#[test]
fn create_relationship_between_where_matched_nodes_covers_evolves_write() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'older', title: 'Older'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', title: 'Newer'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {content_relation: $relation}]->(b)",
                &BTreeMap::from([
                    ("older_id".to_string(), Value::String("older".to_string())),
                    ("newer_id".to_string(), Value::String("newer".to_string())),
                    ("relation".to_string(), Value::String("replaces".to_string())),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
            .query(
                "MATCH (a:Memory {id: 'older'})-[r:EVOLVES]->(b:Memory {id: 'newer'}) RETURN count(r) AS total, min(r.content_relation) AS relation",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(
        rels.rows[0].get("relation"),
        Some(&Value::String("replaces".to_string()))
    );

    let missing = db
            .query_with_params(
                "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES]->(b)",
                &BTreeMap::from([
                    ("older_id".to_string(), Value::String("older".to_string())),
                    ("newer_id".to_string(), Value::String("missing".to_string())),
                ]),
            )
            .unwrap();
    assert!(missing.rows.is_empty());
    let rels = db
        .query("MATCH (:Memory)-[r:EVOLVES]->(:Memory) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn merge_relationship_between_matched_nodes_on_create_set_writes_only_when_created() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'l1', name: 'Important'})")
        .unwrap();

    let first = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.properties = '{}'",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("label_id".to_string(), Value::String("l1".to_string())),
                    ("assigned_by".to_string(), Value::String("system".to_string())),
                ]),
            )
            .unwrap();
    let second = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'overwritten'",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("label_id".to_string(), Value::String("l1".to_string())),
                ]),
            )
            .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("rel_id"), second.rows[0].get("rel_id"));

    let rels = db
            .query(
                "MATCH (m:Memory {id: 'm1'})-[r:HAS_LABEL]->(l:Label {id: 'l1'}) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by, min(r.properties) AS properties",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(
        rels.rows[0].get("assigned_by"),
        Some(&Value::String("system".to_string()))
    );
    assert_eq!(
        rels.rows[0].get("properties"),
        Some(&Value::String("{}".to_string()))
    );
}

#[test]
fn two_node_match_return_covers_source_provenance_existence_check() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::String("m1".to_string())),
                ("source_id".to_string(), Value::String("s1".to_string())),
            ]),
        )
        .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(1)));

    let missing = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::String("m1".to_string())),
                (
                    "source_id".to_string(),
                    Value::String("missing".to_string()),
                ),
            ]),
        )
        .unwrap();
    assert_eq!(missing.rows[0].get("count(m)"), Some(&Value::Int(0)));
}

#[test]
fn consecutive_two_node_match_return_covers_entity_endpoint_check() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'source', name: 'Source'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'target', name: 'Target'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
                &BTreeMap::from([
                    (
                        "source_entity_id".to_string(),
                        Value::String("source".to_string()),
                    ),
                    (
                        "target_entity_id".to_string(),
                        Value::String("target".to_string()),
                    ),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("source.id"),
        Some(&Value::String("source".to_string()))
    );
    assert_eq!(
        output.rows[0].get("target.id"),
        Some(&Value::String("target".to_string()))
    );

    let missing = db
            .query_with_params(
                "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
                &BTreeMap::from([
                    (
                        "source_entity_id".to_string(),
                        Value::String("source".to_string()),
                    ),
                    (
                        "target_entity_id".to_string(),
                        Value::String("missing".to_string()),
                    ),
                ]),
            )
            .unwrap();
    assert!(missing.rows.is_empty());
}

#[test]
fn matched_relationship_create_uses_one_wal_batch() {
    let path = unique_test_dir("matched_relationship_create_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'm1'})").unwrap();
        db.query("CREATE (:Source {id: 's1'})").unwrap();
        db.query("MATCH (m:Memory {id: 'm1'}), (s:Source {id: 's1'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 0}]->(s)")
                .unwrap();
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 3);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (:Memory)-[r:SOURCED_FROM]->(:Source) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn merge_relationship_existing_pattern_does_not_write_wal() {
    let path = unique_test_dir("merge_relationship_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        db.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_deduplicates_pending_nodes_in_one_wal_batch() {
    let path = unique_test_dir("merge_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert_eq!(wal.matches("create_node").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_on_create_set_deduplicates_pending_nodes_by_match_key() {
    let path = unique_test_dir("merge_on_create_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
            "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.note = 'created'",
        )
        .unwrap();
        tx.query(
            "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.note = 'duplicate'",
        )
        .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert_eq!(wal.matches("create_node").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:SchemaMigrationLog {id: 'migration-1'}) RETURN m.note AS note")
            .unwrap();
        assert_eq!(
            output.rows[0].get("note"),
            Some(&Value::String("created".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_node_on_match_set_updates_pending_create() {
    let path = unique_test_dir("merge_on_match_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MERGE (l:Label {id: 'label-1'}) ON CREATE SET l.name = 'Important', l.canonical_name = null, l.updated_at = 1 ON MATCH SET l.updated_at = 2, l.canonical_name = COALESCE(l.canonical_name, 'important')",
            )
            .unwrap();
        tx.query(
                "MERGE (l:Label {id: 'label-1'}) ON CREATE SET l.name = 'Duplicate' ON MATCH SET l.updated_at = 2, l.canonical_name = COALESCE(l.canonical_name, 'important')",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert_eq!(wal.matches("create_node").count(), 1);
    assert_eq!(wal.matches("set_node_property").count(), 0);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query("MATCH (l:Label {id: 'label-1'}) RETURN l.name AS name, l.canonical_name AS canonical, l.updated_at AS updated")
                .unwrap();
        assert_eq!(
            output.rows[0].get("name"),
            Some(&Value::String("Important".to_string()))
        );
        assert_eq!(
            output.rows[0].get("canonical"),
            Some(&Value::String("important".to_string()))
        );
        assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(2)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_node_post_set_updates_pending_create() {
    let path = unique_test_dir("merge_post_set_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = true, m.pagerank_algorithm = 'pagerank'",
            )
            .unwrap();
        tx.query(
                "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_iterations = 20, m.updated_at = CURRENT_TIMESTAMP()",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert_eq!(wal.matches("create_node").count(), 1);
    assert_eq!(wal.matches("set_node_property").count(), 0);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query(
                    "MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_algorithm AS algorithm, m.pagerank_iterations AS iterations, count(m.updated_at) AS updated",
                )
                .unwrap();
        assert_eq!(output.rows[0].get("applied"), Some(&Value::Bool(true)));
        assert_eq!(
            output.rows[0].get("algorithm"),
            Some(&Value::String("pagerank".to_string()))
        );
        assert_eq!(output.rows[0].get("iterations"), Some(&Value::Int(20)));
        assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_relationship_on_create_set_deduplicates_pending_relationships() {
    let path = unique_test_dir("merge_relationship_on_create_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'm1'})").unwrap();
        db.query("CREATE (:Label {id: 'l1'})").unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MATCH (m:Memory {id: 'm1'}), (l:Label {id: 'l1'}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'system'",
            )
            .unwrap();
        tx.query(
                "MATCH (m:Memory {id: 'm1'}), (l:Label {id: 'l1'}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'duplicate'",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("rel_id"), output.rows[1].get("rel_id"));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query(
                    "MATCH (:Memory)-[r:HAS_LABEL]->(:Label) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by",
                )
                .unwrap();
        assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));
        assert_eq!(
            output.rows[0].get("assigned_by"),
            Some(&Value::String("system".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_relationship_deduplicates_pending_pattern() {
    let path = unique_test_dir("merge_relationship_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        tx.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("rel_id"), output.rows[1].get("rel_id"));
    }

    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn set_updates_node_property_and_property_index() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Old'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'New'")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'New' RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'Old' RETURN m.id AS id")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn unlabeled_match_reads_and_updates_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 2, name: 'Entity'})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (n) WHERE n.id IN $ids RETURN n.id AS id ORDER BY id ASC",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            )]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));

    let output = db
        .query("MATCH (n) WHERE n.id = 2 SET n.community_id = 7")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let output = db
        .query("MATCH (n) WHERE n.community_id = 7 RETURN n.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
}

#[test]
fn multi_label_match_reads_any_listed_label() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Memory'})-[:MENTIONS]->(:Entity {id: 2, name: 'Entity'})",
    )
    .unwrap();
    db.query("CREATE (:Source {id: 3, original_name: 'Source'})")
        .unwrap();

    let output = db
        .query("MATCH (n:Entity:Memory) RETURN n.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));

    let output = db
            .query("MATCH (m:Memory {id: 1})-[r]-(neighbor:Entity:Memory) RETURN DISTINCT neighbor.id AS id")
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));

    let output = db
        .query("MATCH (n:MissingLabel) RETURN n.id AS id")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn variable_return_items_project_graph_records() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})-[:MENTIONS {weight: 3}]->(:Entity {id: 'e1', name: 'Entity'})")
            .unwrap();

    let output = db.query("MATCH (m:Memory {id: 'm1'}) RETURN m").unwrap();
    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(memory)) = output.rows[0].get("m") else {
        panic!("expected projected memory map");
    };
    assert_eq!(memory.get("id"), Some(&Value::String("m1".to_string())));
    assert_eq!(
        memory.get("title"),
        Some(&Value::String("Memory".to_string()))
    );
    assert_eq!(
        memory.get("labels"),
        Some(&Value::List(vec![Value::String("Memory".to_string())]))
    );

    let output = db
        .query("MATCH (m:Memory {id: 'm1'})-[r:MENTIONS]->(e:Entity) RETURN r")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(relationship)) = output.rows[0].get("r") else {
        panic!("expected projected relationship map");
    };
    assert_eq!(
        relationship.get("type"),
        Some(&Value::String("MENTIONS".to_string()))
    );
    assert_eq!(relationship.get("weight"), Some(&Value::Int(3)));
    assert_eq!(relationship.get("source_id"), Some(&Value::Int(0)));
    assert_eq!(relationship.get("target_id"), Some(&Value::Int(1)));
}

#[test]
fn property_increment_set_updates_integer_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();
    db.query("CREATE (:Source {id: 's2'})").unwrap();
    db.query("CREATE (:Source {id: 'bad', memory_count: 'zero'})")
        .unwrap();

    let output = db
        .query("MATCH (s:Source {id: 's1'}) SET s.memory_count = s.memory_count + 1")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    let output = db
        .query("MATCH (s:Source {id: 's1'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(1)));

    db.query("MATCH (s:Source {id: 's2'}) SET s.memory_count = s.memory_count + 1")
        .unwrap();
    let output = db
        .query("MATCH (s:Source {id: 's2'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(1)));

    let error = db
        .query("MATCH (s:Source {id: 'bad'}) SET s.memory_count = s.memory_count + 1")
        .unwrap_err();
    assert!(error.to_string().contains("requires an integer or null"));

    db.query(
        "MATCH (s:Source {id: 's1'})
             SET s.memory_count = CASE WHEN s.memory_count > 0 THEN s.memory_count - 1 ELSE 0 END",
    )
    .unwrap();
    let output = db
        .query("MATCH (s:Source {id: 's1'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(0)));

    db.query(
        "MATCH (s:Source {id: 's1'})
             SET s.memory_count = CASE WHEN s.memory_count > 0 THEN s.memory_count - 1 ELSE 0 END",
    )
    .unwrap();
    let output = db
        .query("MATCH (s:Source {id: 's1'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(0)));

    db.query("CREATE (:Memory {id: 'm1'})").unwrap();
    db.query(
        "MATCH (m:Memory) WHERE m.id = 'm1'
             SET m.access_count = COALESCE(m.access_count, 0) + 1,
                 m.last_accessed_at = 42",
    )
    .unwrap();
    let output = db
        .query(
            "MATCH (m:Memory {id: 'm1'})
                 RETURN m.access_count AS count, m.last_accessed_at AS last_accessed_at",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(1)));
    assert_eq!(
        output.rows[0].get("last_accessed_at"),
        Some(&Value::Int(42))
    );

    db.query("CREATE (:AugmentationJob {job_id: 'job-1', created_at: CURRENT_TIMESTAMP()})")
        .unwrap();
    let output = db
        .query(
            "MATCH (j:AugmentationJob {job_id: 'job-1'})
                 RETURN j.created_at AS created_at",
        )
        .unwrap();
    let Some(Value::Int(created_at)) = output.rows[0].get("created_at") else {
        panic!("expected integer timestamp");
    };
    assert!(*created_at > 0);

    db.query(
        "MATCH (j:AugmentationJob {job_id: 'job-1'})
             SET j.started_at = CURRENT_TIMESTAMP()",
    )
    .unwrap();
    let output = db
        .query(
            "MATCH (j:AugmentationJob {job_id: 'job-1'})
                 RETURN j.started_at AS started_at",
        )
        .unwrap();
    let Some(Value::Int(started_at)) = output.rows[0].get("started_at") else {
        panic!("expected integer timestamp");
    };
    assert!(*started_at >= *created_at);
}

#[test]
fn timestamp_function_binds_iso_strings_and_epoch_numbers() {
    let mut db = Database::new();
    db.query_with_params(
            "CREATE (:Memory {id: 'm1', created_at: timestamp($created_at), updated_at: timestamp($updated_at)})",
            &BTreeMap::from([
                (
                    "created_at".to_string(),
                    Value::String("1970-01-01T00:00:02Z".to_string()),
                ),
                ("updated_at".to_string(), Value::Int(3)),
            ]),
        )
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.created_at > timestamp($cutoff) RETURN count(m) AS total",
            &BTreeMap::from([(
                "cutoff".to_string(),
                Value::String("1970-01-01T00:00:01".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));

    let output = db
            .query_with_params(
                "MATCH (m:Memory) WHERE m.created_at >= CAST($cutoff AS TIMESTAMP) RETURN count(m) AS total",
                &BTreeMap::from([(
                    "cutoff".to_string(),
                    Value::String("1970-01-01T00:00:02".to_string()),
                )]),
            )
            .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));

    let output = db
            .query("MATCH (m:Memory {id: 'm1'}) RETURN m.created_at AS created_at, m.updated_at AS updated_at")
            .unwrap();
    assert_eq!(
        output.rows[0].get("created_at"),
        Some(&Value::Int(2_000_000_000))
    );
    assert_eq!(
        output.rows[0].get("updated_at"),
        Some(&Value::Int(3_000_000_000))
    );
}

#[test]
fn set_uses_or_predicate_filter() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', title: 'One'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', title: 'Two'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', title: 'Three'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.kind = 'note' OR m.id = 2 SET m.flag = 'selected'")
        .unwrap();
    assert_eq!(output.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.flag = 'selected' RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));
}

#[test]
fn set_persists_and_replays_from_wal() {
    let path = unique_test_dir("set_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Old'})").unwrap();
        db.query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id SET m.title = $title",
            &BTreeMap::from([
                ("id".to_string(), Value::Int(1)),
                ("title".to_string(), Value::String("New".to_string())),
            ]),
        )
        .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.title = 'New' RETURN m.id AS id")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_set_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Old'})").unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'Ignored'")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'Old' RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'Committed'")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'Committed' RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn delete_removes_node_and_property_index_entries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Remove'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Keep'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let removed = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(removed.rows.is_empty());
    let by_old_index = db
        .query("MATCH (m:Memory) WHERE m.title = 'Remove' RETURN m.id AS id")
        .unwrap();
    assert!(by_old_index.rows.is_empty());
    let kept = db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        kept.rows[0].get("title"),
        Some(&Value::String("Keep".to_string()))
    );
}

#[test]
fn delete_rejects_nodes_with_relationships() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let error = db
        .query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
        .unwrap_err();
    assert!(error.to_string().contains("DETACH DELETE"));

    let output = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.id = 1 RETURN e.name AS entity")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn detach_delete_removes_attached_relationships() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 DETACH DELETE m")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert!(rels.rows.is_empty());
    let target = db
        .query("MATCH (e:Entity) WHERE e.id = 10 RETURN e.name AS name")
        .unwrap();
    assert_eq!(
        target.rows[0].get("name"),
        Some(&Value::String("Neo4j".to_string()))
    );
}

#[test]
fn delete_relationship_removes_edge_and_keeps_endpoint_nodes() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(0)));

    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert!(rels.rows.is_empty());
    let source = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        source.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    let target = db
        .query("MATCH (e:Entity) WHERE e.id = 10 RETURN e.name AS name")
        .unwrap();
    assert_eq!(
        target.rows[0].get("name"),
        Some(&Value::String("Neo4j".to_string()))
    );
}

#[test]
fn relationship_delete_filters_target_node_property_patterns() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:HAS_LABEL]->(:Label {id: 'keep', name: 'Keep'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'drop', name: 'Drop'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 1}), (l:Label {id: 'drop'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();

    let deleted = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::Int(1)),
                ("label_id".to_string(), Value::String("drop".to_string())),
            ]),
        )
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);

    let remaining = db
        .query("MATCH (m:Memory {id: 1})-[r:HAS_LABEL]->(l:Label) RETURN l.id AS id")
        .unwrap();
    assert_eq!(remaining.rows.len(), 1);
    assert_eq!(
        remaining.rows[0].get("id"),
        Some(&Value::String("keep".to_string()))
    );
}

#[test]
fn detach_delete_after_relationship_match_removes_target_nodes() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Thread {id: 'thread-1'})-[:CONTAINS]->(:Message {id: 'msg-1', order_index: 1})",
    )
    .unwrap();
    db.query("CREATE (:Message {id: 'msg-orphan', order_index: 2})")
        .unwrap();

    let deleted = db
        .query_with_params(
            "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m",
            &BTreeMap::from([(
                "thread_uuid".to_string(),
                Value::String("thread-1".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);

    let messages = db
        .query("MATCH (m:Message) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(messages.rows.len(), 1);
    assert_eq!(
        messages.rows[0].get("id"),
        Some(&Value::String("msg-orphan".to_string()))
    );
    let thread = db
        .query("MATCH (t:Thread {id: 'thread-1'}) RETURN count(t) AS total")
        .unwrap();
    assert_eq!(thread.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn set_relationship_property_updates_edge_and_keeps_endpoint_nodes() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(0)));

    let relationship = db.store.scan_relationships(None).next().unwrap();
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(2)));
    let source = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        source.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    let target = db
        .query("MATCH (e:Entity) WHERE e.id = 10 RETURN e.name AS name")
        .unwrap();
    assert_eq!(
        target.rows[0].get("name"),
        Some(&Value::String("Neo4j".to_string()))
    );
}

#[test]
fn relationship_pattern_property_filters_reads_with_parameters() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS {weight: 2}]->(:Entity {id: 11, name: 'Cypher'})",
        )
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (m:Memory)-[r:MENTIONS {weight: $weight}]->(e:Entity) RETURN e.name AS entity, r.weight AS weight",
                &BTreeMap::from([("weight".to_string(), Value::Int(2))]),
            )
            .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Cypher".to_string()))
    );
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Int(2)));
}

#[test]
fn relationship_pattern_property_filters_set_and_delete() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS {weight: 2}]->(:Entity {id: 11, name: 'Cypher'})",
        )
        .unwrap();

    let updated = db
        .query("MATCH (m:Memory)-[r:MENTIONS {weight: 1}]->(e:Entity) SET r.weight = 9")
        .unwrap();
    assert_eq!(updated.rows.len(), 1);
    let after_set = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight ORDER BY weight ASC",
            )
            .unwrap();
    assert_eq!(after_set.rows.len(), 2);
    assert_eq!(
        after_set.rows[0].get("entity"),
        Some(&Value::String("Cypher".to_string()))
    );
    assert_eq!(after_set.rows[0].get("weight"), Some(&Value::Int(2)));
    assert_eq!(
        after_set.rows[1].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(after_set.rows[1].get("weight"), Some(&Value::Int(9)));

    let deleted = db
        .query("MATCH (m:Memory)-[r:MENTIONS {weight: 2}]->(e:Entity) DELETE r")
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);
    let remaining = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight",
        )
        .unwrap();
    assert_eq!(remaining.rows.len(), 1);
    assert_eq!(
        remaining.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(remaining.rows[0].get("weight"), Some(&Value::Int(9)));
}

#[test]
fn relationship_mutation_where_filters_relationship_properties() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 2}]->(:Entity {id: 11, name: 'Cypher'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS {weight: 1}]->(:Entity {id: 12, name: 'Rust'})",
        )
        .unwrap();

    let updated = db
            .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 AND r.weight = 1 SET r.weight = 9")
            .unwrap();
    assert_eq!(updated.rows.len(), 1);

    let after_set = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight ORDER BY entity ASC",
            )
            .unwrap();
    assert_eq!(after_set.rows.len(), 3);
    assert_eq!(
        after_set.rows[0].get("entity"),
        Some(&Value::String("Cypher".to_string()))
    );
    assert_eq!(after_set.rows[0].get("weight"), Some(&Value::Int(2)));
    assert_eq!(
        after_set.rows[1].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(after_set.rows[1].get("weight"), Some(&Value::Int(9)));
    assert_eq!(
        after_set.rows[2].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );
    assert_eq!(after_set.rows[2].get("weight"), Some(&Value::Int(1)));

    let deleted = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE r.weight = 2 DELETE r")
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);

    let remaining = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight ORDER BY entity ASC",
            )
            .unwrap();
    assert_eq!(remaining.rows.len(), 2);
    assert_eq!(
        remaining.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(remaining.rows[0].get("weight"), Some(&Value::Int(9)));
    assert_eq!(
        remaining.rows[1].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );
    assert_eq!(remaining.rows[1].get("weight"), Some(&Value::Int(1)));
}

#[test]
fn relationship_mutation_rejects_mixed_variable_or_predicates() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let error = db
            .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 OR r.weight = 1 SET r.weight = 9")
            .unwrap_err();
    assert!(error.to_string().contains(
        "relationship mutation OR predicates cannot mix node and relationship variables"
    ));
}

#[test]
fn bounded_relationship_pattern_properties_are_rejected() {
    let db = Database::new();
    let error = db
        .explain_query(
            "MATCH (m:Memory)-[:MENTIONS*1..2 {weight: 1}]->(e:Entity) RETURN e.name AS entity",
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship property patterns are supported only for one-hop patterns"));
}

#[test]
fn delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Transient'})")
            .unwrap();
        db.query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
            .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert!(output.rows.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("relationship_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
            )
            .unwrap();
        db.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
            .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("delete_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let rels = db
            .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
            .unwrap();
        assert!(rels.rows.is_empty());
        let source = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(source.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_set_persists_and_replays_from_wal() {
    let path = unique_test_dir("relationship_set_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
            )
            .unwrap();
        db.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2")
            .unwrap();
    }
    let wal = std::fs::read_to_string(path.join("wal.skein")).unwrap();
    assert!(wal.contains("set_rel_property"));
    {
        let db = Database::open(&path).unwrap();
        let relationship = db.store.scan_relationships(None).next().unwrap();
        assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(2)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_delete_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Original'})")
        .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn transaction_relationship_delete_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
            .unwrap();
        tx.rollback();
    }
    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert_eq!(rels.rows.len(), 1);

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert!(rels.rows.is_empty());
}

#[test]
fn transaction_detach_delete_after_relationship_match_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread-1'})-[:CONTAINS]->(:Message {id: 'msg-1'})")
        .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (t:Thread {id: 'thread-1'})-[:CONTAINS]->(m:Message) DETACH DELETE m")
            .unwrap();
        tx.rollback();
    }
    let messages = db
        .query("MATCH (m:Message) RETURN count(m) AS total")
        .unwrap();
    assert_eq!(messages.rows[0].get("total"), Some(&Value::Int(1)));

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (t:Thread {id: 'thread-1'})-[:CONTAINS]->(m:Message) DETACH DELETE m")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let messages = db
        .query("MATCH (m:Message) RETURN count(m) AS total")
        .unwrap();
    assert_eq!(messages.rows[0].get("total"), Some(&Value::Int(0)));
}

#[test]
fn transaction_relationship_set_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2")
            .unwrap();
        tx.rollback();
    }
    let relationship = db.store.scan_relationships(None).next().unwrap();
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(1)));

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 3")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let relationship = db.store.scan_relationships(None).next().unwrap();
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(3)));
}

#[test]
fn missing_parameter_fails_before_mutation() {
    let mut db = Database::new();
    let error = db
        .query("CREATE (:Memory {id: $id, title: 'missing'})")
        .unwrap_err();
    assert!(error.to_string().contains("missing parameter '$id'"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

fn unique_test_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("skein_{name}_{nanos}"))
}

fn read_test_durable_text(path: &std::path::Path) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    if !bytes.starts_with(b"SKEIN_COMPRESSED_V1") {
        return String::from_utf8(bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error));
    }
    let header_end = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing compressed envelope header terminator",
            )
        })?;
    let payload = &bytes[header_end + 2..];
    let decoded = zstd::stream::decode_all(Cursor::new(payload))?;
    String::from_utf8(decoded)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}
