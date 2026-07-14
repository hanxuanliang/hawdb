use crate::cypher;
use crate::error::{Result, SkeinError};
use crate::executor::{self, Row};
use crate::optimizer::{CascadesOptimizer, OptimizerTrace, PhysicalPlan};
use crate::planner;
use crate::schema::Catalog;
use crate::store::{DurabilityPolicy, GraphMutation, GraphStore};
use std::path::Path;

#[derive(Debug, Default)]
pub struct Database {
    catalog: Catalog,
    store: GraphStore,
    optimizer: CascadesOptimizer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput {
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainOutput {
    pub physical_plan: PhysicalPlan,
    pub trace: OptimizerTrace,
}

#[derive(Debug)]
pub struct DatabaseTransaction<'a> {
    db: &'a mut Database,
    mutations: Vec<GraphMutation>,
    committed: bool,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_durability(path, DurabilityPolicy::default())
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability(path, &mut catalog, durability)?;
        Ok(Self {
            catalog,
            store,
            optimizer: CascadesOptimizer::default(),
        })
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        let logical = planner::plan(&statement)?;
        let physical = self.optimizer.optimize(&logical);
        let rows = executor::execute(&physical, &mut self.catalog, &mut self.store)?;
        Ok(QueryOutput { rows })
    }

    pub fn begin_transaction(&mut self) -> DatabaseTransaction<'_> {
        DatabaseTransaction {
            db: self,
            mutations: Vec::new(),
            committed: false,
        }
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        let statement = cypher::parse(cypher_text)?;
        let logical = planner::plan(&statement)?;
        let (physical_plan, trace) = self.optimizer.optimize_with_trace(&logical);
        Ok(ExplainOutput {
            physical_plan,
            trace,
        })
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.store.checkpoint(&self.catalog)
    }

    pub fn storage_version(&self) -> &'static str {
        self.store.storage_version()
    }
}

impl DatabaseTransaction<'_> {
    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        let logical = planner::plan(&statement)?;
        let physical = self.db.optimizer.optimize(&logical);
        let Some(mutation) = executor::mutation_command(&physical)? else {
            return Err(SkeinError::Execution(
                "transaction query must be a mutation".to_string(),
            ));
        };
        self.mutations.push(mutation);
        Ok(QueryOutput { rows: Vec::new() })
    }

    pub fn commit(mut self) -> Result<QueryOutput> {
        let summary = self
            .db
            .store
            .commit_mutations(&mut self.db.catalog, std::mem::take(&mut self.mutations))?;
        self.committed = true;
        Ok(QueryOutput { rows: summary.rows })
    }

    pub fn rollback(mut self) {
        self.mutations.clear();
        self.committed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use crate::Value;

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
    fn explains_query_with_optimizer_trace() {
        let db = Database::new();
        let output = db
            .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();

        assert!(output.trace.groups >= 3);
        assert!(output.trace.selected_plan.contains("ProjectExec"));
        assert!(output.trace.selected_plan.contains("IndexNodeSeek"));
        assert!(!output.trace.selected_plan.contains("FilterExec"));
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

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{nanos}"))
    }
}
