use super::*;

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
