use super::*;

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
