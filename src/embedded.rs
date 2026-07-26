use crate::blackbox::{write_blackbox_report, BlackboxReportOptions, BlackboxRunStatus};
use crate::nowledge_mem::{
    NowledgeMemEmbeddedStore, NowledgeMemOpenOptions, NowledgeMemOpenReport,
};
use crate::store::DurabilityPolicy;
use crate::{Database, DatabaseConfig, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkeinEmbeddedOpenOptions {
    pub path: PathBuf,
    pub config: DatabaseConfig,
    pub durability: DurabilityPolicy,
}

#[derive(Debug)]
pub struct SkeinEmbedded {
    path: PathBuf,
    database: Database,
}

impl SkeinEmbeddedOpenOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            config: DatabaseConfig::default(),
            durability: DurabilityPolicy::default(),
        }
    }

    pub fn with_config(mut self, config: DatabaseConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_durability(mut self, durability: DurabilityPolicy) -> Self {
        self.durability = durability;
        self
    }
}

impl SkeinEmbedded {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(SkeinEmbeddedOpenOptions::new(path.as_ref().to_path_buf()))
    }

    pub fn open_with_options(options: SkeinEmbeddedOpenOptions) -> Result<Self> {
        let database = Database::open_with_durability_and_config(
            &options.path,
            options.durability,
            options.config,
        )?;
        Ok(Self {
            path: options.path,
            database,
        })
    }

    pub fn open_nowledge_mem(
        options: NowledgeMemOpenOptions,
    ) -> Result<(NowledgeMemEmbeddedStore, NowledgeMemOpenReport)> {
        NowledgeMemEmbeddedStore::open_with_options(options)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn database_mut(&mut self) -> &mut Database {
        &mut self.database
    }

    pub fn into_database(self) -> Database {
        self.database
    }

    pub fn write_slow_query_log_jsonl(&self, path: impl AsRef<Path>) -> Result<()> {
        self.database.write_slow_query_log_jsonl(path)
    }

    pub fn write_blackbox_report(
        &self,
        artifact_dir: impl Into<PathBuf>,
        output_dir: impl Into<PathBuf>,
        run_id: Option<String>,
        run_status: BlackboxRunStatus,
        exit_code: Option<i64>,
    ) -> Result<serde_json::Value> {
        write_blackbox_report(&BlackboxReportOptions {
            artifact_dir: artifact_dir.into(),
            output_dir: output_dir.into(),
            run_id,
            run_status,
            exit_code,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        NowledgeMemGraphMode, NowledgeMemReadinessOptions, Value,
        NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn embedded_handle_opens_database_and_writes_slow_query_log() {
        let root = unique_test_dir("embedded-open");
        let db_path = root.join("graph");
        let slow_log_path = root.join("slow-query-log.jsonl");
        let mut engine = SkeinEmbedded::open_with_options(
            SkeinEmbeddedOpenOptions::new(&db_path).with_config(DatabaseConfig {
                slow_query_log_threshold_micros: 0,
                slow_query_log_capacity: 8,
                ..DatabaseConfig::default()
            }),
        )
        .unwrap();

        engine
            .database_mut()
            .query("CREATE (:Memory {id: 'm1', title: 'Embedded'})")
            .unwrap();
        let output = engine
            .database_mut()
            .query("MATCH (m:Memory {id: 'm1'}) RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Embedded".to_string()))
        );

        engine.write_slow_query_log_jsonl(&slow_log_path).unwrap();

        let slow_log = std::fs::read_to_string(slow_log_path).unwrap();
        assert!(slow_log.contains("skein-slow-query-log-event-v1"));
        assert!(slow_log.contains("query_digest"));
        assert!(!slow_log.contains("MATCH"));
        assert!(!slow_log.contains("m1"));
    }

    #[test]
    fn embedded_handle_opens_nowledge_mem_store() {
        let root = unique_test_dir("embedded-nowledge-mem");
        let graph_path = root.join("graph");
        let (mut store, open_report) = SkeinEmbedded::open_nowledge_mem(
            NowledgeMemOpenOptions::graph_only(&graph_path, NowledgeMemGraphMode::WritableCutover),
        )
        .unwrap();

        let readiness = store.library_readiness(&NowledgeMemReadinessOptions::default());

        assert_eq!(open_report.mode, NowledgeMemGraphMode::WritableCutover);
        assert!(open_report.graph_configured);
        assert!(open_report.graph_opened);
        assert!(!open_report.search_projection_configured);
        assert!(!open_report.search_projection_opened);
        assert_eq!(readiness.protocol, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL);
        assert_eq!(readiness.mode, NowledgeMemGraphMode::WritableCutover);
        assert!(readiness.graph_open);
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("skein-{prefix}-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }
}
