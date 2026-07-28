use super::*;

#[test]
fn reads_memory_title_contents_for_rest_skills_write_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_title_b', title: 'Memory B', content: 'body b', created_at: 20})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_title_a', title: 'Memory A', content: 'body a', created_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_title_missing_time', title: 'Memory Missing Time', content: 'body missing'})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'memory_title_a', title: 'Wrong Label'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .knowledge_memory_title_contents(&KnowledgeMemoryTitleContentRequest {
            memory_ids: vec![
                "memory_title_b".to_string(),
                "missing".to_string(),
                "memory_title_a".to_string(),
                "memory_title_missing_time".to_string(),
                "memory_title_b".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.returned_count, 3);
    assert_eq!(output.missing_memory_ids, vec!["missing".to_string()]);
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "memory_title_a",
            "memory_title_b",
            "memory_title_missing_time"
        ]
    );
    assert_eq!(output.rows[0].title.as_deref(), Some("Memory A"));
    assert_eq!(output.rows[0].content.as_deref(), Some("body a"));
    assert_eq!(output.rows[0].created_at, Some(Value::Int(10)));

    let tx = db.begin_read_transaction();
    db.query("MATCH (m:Memory {id: 'memory_title_a'}) SET m.title = 'Changed'")
        .unwrap();
    let snapshot = tx
        .knowledge_memory_title_contents(&KnowledgeMemoryTitleContentRequest {
            memory_ids: vec!["memory_title_a".to_string()],
        })
        .unwrap();
    assert_eq!(snapshot.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(snapshot.rows[0].title.as_deref(), Some("Memory A"));
}

#[test]
fn memory_title_contents_uses_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory_title_cache_b', title: 'Memory B', content: 'body b', created_at: 20})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_title_cache_a', title: 'Memory A', content: 'body a', created_at: 10})")
        .unwrap();
    let request = KnowledgeMemoryTitleContentRequest {
        memory_ids: vec![
            "memory_title_cache_b".to_string(),
            "memory_title_missing".to_string(),
            "memory_title_cache_a".to_string(),
        ],
    };

    let first = db.knowledge_memory_title_contents(&request).unwrap();
    let second = db.knowledge_memory_title_contents(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_count, 2);
    assert_eq!(first.returned_count, 2);
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["memory_title_cache_a", "memory_title_cache_b"]
    );
    assert_eq!(
        first.missing_memory_ids,
        vec!["memory_title_missing".to_string()]
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 1);
    assert_eq!(stats.misses, 1);
    assert_eq!(stats.hits, 1);
}

#[test]
fn memory_title_contents_handles_empty_input_and_rejects_empty_ids() {
    let db = Database::new();
    let empty = db
        .knowledge_memory_title_contents(&KnowledgeMemoryTitleContentRequest {
            memory_ids: Vec::new(),
        })
        .unwrap();
    assert_eq!(empty.returned_count, 0);
    assert_eq!(empty.matched_count, 0);
    assert!(empty.missing_memory_ids.is_empty());

    let empty_id = db
        .knowledge_memory_title_contents(&KnowledgeMemoryTitleContentRequest {
            memory_ids: vec![String::new()],
        })
        .unwrap_err();
    assert!(empty_id.to_string().contains("non-empty memory ids"));
}
