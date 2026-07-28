use super::*;

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
