use super::*;

#[test]
fn reads_augmentation_job_status_for_nowledge_shapes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:AugmentationJob {job_id: 'job_1', job_type: 'pagerank', status: 'running', progress: 42.5, message: 'Working', result: '{}', error_message: '', started_at: 100, completed_at: NULL, created_at: 90})")
        .unwrap();

    let request = KnowledgeAugmentationJobRequest {
        job_id: "job_1".to_string(),
    };
    let output = db.knowledge_augmentation_job(&request).unwrap();

    assert_eq!(output.graph_commit_epoch, 1);
    assert!(output.found);
    let job = output.job.as_ref().unwrap();
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
    let repeated = db.knowledge_augmentation_job(&request).unwrap();
    assert_eq!(repeated, output);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);

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
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:AugmentationJob {job_id: 'old_running', job_type: 'pagerank', status: 'running', progress: 10.0, message: 'old', started_at: 10, created_at: 1})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'new_running', job_type: 'louvain', status: 'running', progress: 20.0, message: 'new', started_at: 30, created_at: 2})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'pending', job_type: 'louvain', status: 'pending', progress: 0.0, message: 'pending', created_at: 3})")
        .unwrap();

    let request = KnowledgeAugmentationJobListRequest {
        status_filter: Some("running".to_string()),
        order_by: KnowledgeAugmentationJobListOrder::StartedAtDesc,
        limit: 1,
    };
    let output = db.knowledge_augmentation_jobs(&request).unwrap();

    assert_eq!(output.graph_commit_epoch, 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.returned_count, 1);
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].job_id.as_deref(), Some("new_running"));
    assert_eq!(output.rows[0].started_at, Some(Value::Int(30)));
    let repeated = db.knowledge_augmentation_jobs(&request).unwrap();
    assert_eq!(repeated, output);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
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
