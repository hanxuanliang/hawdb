use super::*;

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
fn knowledge_retrieval_metadata_filters_support_typed_in_and_not_in() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'fact_1', title: 'Typed filter graph', content: 'typed predicate retrieval', unit_type: 'fact', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'task_1', title: 'Typed filter graph', content: 'typed predicate retrieval', unit_type: 'task', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'deleted_1', title: 'Typed filter graph', content: 'typed predicate retrieval', unit_type: 'fact', lifecycle_state: 'deleted'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "typed predicate retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([
                (
                    "unit_type__in".to_string(),
                    r#"["fact","learning"]"#.to_string(),
                ),
                (
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                ),
            ]),
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
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("fact_1"));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("fact_1")
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .filtered_out_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .input_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .input_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .residual_predicate_count,
        0
    );
    assert!(output
        .diagnostics
        .graph_seed_input_candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .is_none());
}

#[test]
fn malformed_typed_metadata_filter_fails_closed_for_search_and_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Typed filter graph', content: 'malformed predicate retrieval', lifecycle_state: 'active'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db.retrieve_knowledge(
        &search_index,
        &KnowledgeRetrievalRequest {
            query_text: "malformed predicate retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                "deleted,forgotten".to_string(),
            )]),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 10,
            graph_context_limit: 0,
            graph_context_max_hops: 1,
        },
    );

    assert_eq!(output.search.total_hits, 0);
    assert_eq!(output.diagnostics.search_filtered_document_count, 0);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 0);
    assert_eq!(output.diagnostics.graph_seed_returned_count, 0);
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .filtered_out_count,
        1
    );
    assert!(
        output
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .unsatisfiable
    );
    assert!(output
        .diagnostics
        .search_candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .as_deref()
        .is_some_and(|error| error.contains("expected JSON string array")));
    assert!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .unsatisfiable
    );
    assert!(output
        .diagnostics
        .graph_seed_input_candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .as_deref()
        .is_some_and(|error| error.contains("expected JSON string array")));
    assert!(output.graph_seeds.is_empty());
}
