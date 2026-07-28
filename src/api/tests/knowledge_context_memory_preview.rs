use super::*;

#[test]
fn reads_context_memory_preview_for_nowledge_context_wiring_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'context-preview-older', title: 'Context Preview Older', unit_type: 'context-preview', is_crystal: false, created_at: 1000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'context-preview-newer', title: 'Context Preview Newer', unit_type: 'context-preview', is_latest: true, is_crystal: false, created_at: 2000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'context-preview-stale', title: 'Context Preview Stale', unit_type: 'context-preview', is_latest: false, is_crystal: false, created_at: 3000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'context-preview-crystal', title: 'Context Preview Crystal', unit_type: 'context-preview', is_latest: true, is_crystal: true, created_at: 4000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'context-other-type', title: 'Other Type', unit_type: 'other', is_latest: true, is_crystal: false, created_at: 5000})")
        .unwrap();
    db.query("CREATE (:Label {id: 'context-preview-label', name: 'Context Preview', canonical_name: 'context-preview'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'context-preview-newer'}), (l:Label {id: 'context-preview-label'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let title_preview = db
        .knowledge_context_memory_preview(&KnowledgeContextMemoryPreviewRequest {
            unit_types: vec!["context-preview".to_string()],
            latest_filter: KnowledgeContextMemoryLatestFilter::NullOrTrue,
            include_labels: false,
            limit: 400,
        })
        .unwrap();

    assert_eq!(title_preview.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(title_preview.matched_memory_count, 2);
    assert_eq!(title_preview.returned_count, 2);
    assert_eq!(
        title_preview.rows[0].memory_id.as_deref(),
        Some("context-preview-newer")
    );
    assert_eq!(
        title_preview.rows[0].title.as_deref(),
        Some("Context Preview Newer")
    );
    assert_eq!(
        title_preview.rows[1].memory_id.as_deref(),
        Some("context-preview-older")
    );

    let cached_title_preview = db
        .knowledge_context_memory_preview(&KnowledgeContextMemoryPreviewRequest {
            unit_types: vec!["context-preview".to_string()],
            latest_filter: KnowledgeContextMemoryLatestFilter::NullOrTrue,
            include_labels: false,
            limit: 400,
        })
        .unwrap();
    assert_eq!(cached_title_preview, title_preview);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);

    let typed_preview = db
        .knowledge_context_memory_preview(&KnowledgeContextMemoryPreviewRequest {
            unit_types: vec!["context-preview".to_string()],
            latest_filter: KnowledgeContextMemoryLatestFilter::NullOrTrue,
            include_labels: false,
            limit: 1,
        })
        .unwrap();
    assert_eq!(typed_preview.matched_memory_count, 2);
    assert_eq!(typed_preview.returned_count, 1);
    assert_eq!(
        typed_preview.rows[0].unit_type.as_deref(),
        Some("context-preview")
    );

    let label_preview = db
        .knowledge_context_memory_preview(&KnowledgeContextMemoryPreviewRequest {
            unit_types: vec!["context-preview".to_string()],
            latest_filter: KnowledgeContextMemoryLatestFilter::TrueOnly,
            include_labels: true,
            limit: 2000,
        })
        .unwrap();
    assert_eq!(label_preview.matched_memory_count, 1);
    assert_eq!(label_preview.returned_count, 1);
    assert_eq!(
        label_preview.rows[0].label_id.as_deref(),
        Some("context-preview-label")
    );
    assert_eq!(
        label_preview.rows[0].label_canonical_name.as_deref(),
        Some("context-preview")
    );
    assert_eq!(
        label_preview.rows[0].label_name.as_deref(),
        Some("Context Preview")
    );
    assert_eq!(
        label_preview.rows[0].memory_id.as_deref(),
        Some("context-preview-newer")
    );

    let cached_label_preview = db
        .knowledge_context_memory_preview(&KnowledgeContextMemoryPreviewRequest {
            unit_types: vec!["context-preview".to_string()],
            latest_filter: KnowledgeContextMemoryLatestFilter::TrueOnly,
            include_labels: true,
            limit: 2000,
        })
        .unwrap();
    assert_eq!(cached_label_preview, label_preview);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.misses, 3);
    assert_eq!(stats.hits, 4);
}

#[test]
fn context_memory_preview_rejects_empty_unit_types() {
    let db = Database::new();

    let empty_list_error = db
        .knowledge_context_memory_preview(&KnowledgeContextMemoryPreviewRequest {
            unit_types: Vec::new(),
            latest_filter: KnowledgeContextMemoryLatestFilter::NullOrTrue,
            include_labels: false,
            limit: 10,
        })
        .unwrap_err();
    assert!(empty_list_error
        .to_string()
        .contains("non-empty unit types"));

    let empty_unit_type_error = db
        .knowledge_context_memory_preview(&KnowledgeContextMemoryPreviewRequest {
            unit_types: vec![String::new()],
            latest_filter: KnowledgeContextMemoryLatestFilter::NullOrTrue,
            include_labels: false,
            limit: 10,
        })
        .unwrap_err();
    assert!(empty_unit_type_error
        .to_string()
        .contains("non-empty unit types"));
}
