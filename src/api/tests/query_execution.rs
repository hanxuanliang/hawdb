use super::*;

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
    for id in 100..116 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'Parameterized extra {id}'}})"
        ))
        .unwrap();
    }

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
        .query(
            "MATCH (m:Memory {id: 1})-[r]-(neighbor:Entity:Memory) RETURN DISTINCT neighbor.id AS id",
        )
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
