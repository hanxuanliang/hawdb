use crate::blackbox::{write_blackbox_report, BlackboxReportOptions, BlackboxRunStatus};
use crate::nowledge_mem::{
    NowledgeMemEmbeddedStore, NowledgeMemOpenOptions, NowledgeMemOpenReport,
};
use crate::store::DurabilityPolicy;
use crate::{
    AdaptiveVectorBackendPolicy, Database, DatabaseConfig, Result, RuntimeCapabilities,
    SearchIndex, SearchRangeReadConfig,
};
use skein_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, StorageDeviceProfile,
};
use skein_storage::SegmentReadScheduler;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EmbeddedDeploymentProfile {
    #[default]
    DesktopBound,
    MobileEmbedded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedRuntimeResources {
    pub cpu: RuntimeResourceBudget,
    pub memory: RuntimeMemorySnapshot,
    pub storage_device: StorageDeviceProfile,
    pub storage_io: IoConcurrencyBudget,
}

impl EmbeddedRuntimeResources {
    pub fn foreground_segment_read_scheduler(
        self,
        max_coalesced_bytes: NonZeroU64,
    ) -> SegmentReadScheduler {
        SegmentReadScheduler::new(self.storage_io.foreground_depth, max_coalesced_bytes)
    }

    pub fn background_segment_read_scheduler(
        self,
        max_coalesced_bytes: NonZeroU64,
    ) -> SegmentReadScheduler {
        SegmentReadScheduler::new(self.storage_io.background_depth, max_coalesced_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkeinEmbeddedOpenOptions {
    pub path: PathBuf,
    pub config: DatabaseConfig,
    pub durability: DurabilityPolicy,
    pub deployment_profile: EmbeddedDeploymentProfile,
    pub storage_device: Option<StorageDeviceProfile>,
    pub storage_io: Option<IoConcurrencyBudget>,
    pub resource_snapshot: Option<RuntimeResourceSnapshot>,
    pub runtime_governor_config: Option<RuntimeGovernorConfig>,
}

#[derive(Debug)]
pub struct SkeinEmbedded {
    path: PathBuf,
    database: Database,
    deployment_profile: EmbeddedDeploymentProfile,
    runtime_resources: EmbeddedRuntimeResources,
    runtime_governor: RuntimeGovernor,
}

impl SkeinEmbeddedOpenOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::for_profile(path, EmbeddedDeploymentProfile::DesktopBound)
    }

    pub fn mobile(path: impl Into<PathBuf>) -> Self {
        Self::for_profile(path, EmbeddedDeploymentProfile::MobileEmbedded)
    }

    pub fn for_profile(
        path: impl Into<PathBuf>,
        deployment_profile: EmbeddedDeploymentProfile,
    ) -> Self {
        Self {
            path: path.into(),
            config: default_database_config(deployment_profile),
            durability: DurabilityPolicy::default(),
            deployment_profile,
            storage_device: None,
            storage_io: None,
            resource_snapshot: None,
            runtime_governor_config: None,
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

    pub fn with_runtime_capabilities(mut self, capabilities: RuntimeCapabilities) -> Self {
        self.config.runtime_capabilities = capabilities;
        self
    }

    pub fn with_storage_io_budget(mut self, storage_io: IoConcurrencyBudget) -> Self {
        self.storage_io = Some(storage_io);
        self
    }

    pub fn with_storage_device_profile(mut self, storage_device: StorageDeviceProfile) -> Self {
        self.storage_device = Some(storage_device);
        self
    }

    pub fn with_resource_snapshot(mut self, resource_snapshot: RuntimeResourceSnapshot) -> Self {
        self.resource_snapshot = Some(resource_snapshot);
        self
    }

    pub fn with_runtime_governor_config(
        mut self,
        runtime_governor_config: RuntimeGovernorConfig,
    ) -> Self {
        self.runtime_governor_config = Some(runtime_governor_config);
        self
    }
}

impl SkeinEmbedded {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(SkeinEmbeddedOpenOptions::new(path.as_ref().to_path_buf()))
    }

    pub fn open_with_options(options: SkeinEmbeddedOpenOptions) -> Result<Self> {
        let resource_snapshot = options
            .resource_snapshot
            .unwrap_or_else(RuntimeResourceSnapshot::detect);
        let cpu = resource_snapshot.cpu;
        let storage_device = options
            .storage_device
            .unwrap_or_else(|| StorageDeviceProfile::detect(&options.path));
        let storage_io = options
            .storage_io
            .unwrap_or_else(|| default_io_budget(options.deployment_profile, storage_device));
        let runtime_governor = RuntimeGovernor::new(
            options
                .runtime_governor_config
                .unwrap_or_else(|| default_runtime_governor_config(options.deployment_profile)),
            resource_snapshot,
            storage_io,
        );
        let database = Database::open_with_durability_and_config(
            &options.path,
            options.durability,
            options.config,
        )?;
        Ok(Self {
            path: options.path,
            database,
            deployment_profile: options.deployment_profile,
            runtime_resources: EmbeddedRuntimeResources {
                cpu,
                memory: resource_snapshot.memory,
                storage_device,
                storage_io,
            },
            runtime_governor,
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

    pub fn deployment_profile(&self) -> EmbeddedDeploymentProfile {
        self.deployment_profile
    }

    pub fn runtime_resources(&self) -> EmbeddedRuntimeResources {
        self.runtime_resources
    }

    pub fn runtime_governor(&self) -> &RuntimeGovernor {
        &self.runtime_governor
    }

    pub fn refresh_runtime_resources(&mut self) -> bool {
        self.update_runtime_resources(RuntimeResourceSnapshot::detect())
    }

    pub fn update_runtime_resources(&mut self, resources: RuntimeResourceSnapshot) -> bool {
        let changed = self.runtime_governor.update_resources(resources);
        let snapshot = self.runtime_governor.snapshot();
        self.runtime_resources.cpu = snapshot.resources.cpu;
        self.runtime_resources.memory = snapshot.resources.memory;
        changed
    }

    pub fn runtime_capabilities(&self) -> RuntimeCapabilities {
        self.database.runtime_capabilities()
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn database_mut(&mut self) -> &mut Database {
        &mut self.database
    }

    pub fn configure_search_index(&self, search_index: &mut SearchIndex) {
        search_index.set_runtime_capabilities(self.runtime_capabilities());
        let config = match self.deployment_profile {
            EmbeddedDeploymentProfile::DesktopBound => {
                SearchRangeReadConfig::desktop_bound(self.runtime_resources.storage_io)
            }
            EmbeddedDeploymentProfile::MobileEmbedded => {
                SearchRangeReadConfig::mobile_embedded(self.runtime_resources.storage_io)
            }
        };
        search_index.set_range_read_config(config);
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

fn default_database_config(profile: EmbeddedDeploymentProfile) -> DatabaseConfig {
    match profile {
        EmbeddedDeploymentProfile::DesktopBound => DatabaseConfig::default(),
        EmbeddedDeploymentProfile::MobileEmbedded => DatabaseConfig {
            max_read_result_rows: Some(512),
            max_optimizer_groups: Some(256),
            max_wal_replay_entries: Some(100_000),
            max_wal_replay_bytes: Some(128 * 1024 * 1024),
            max_wal_record_bytes: Some(4 * 1024 * 1024),
            max_wal_batch_operations: Some(25_000),
            max_checkpoint_encoded_bytes: Some(64 * 1024 * 1024 * 1024),
            max_checkpoint_decoded_bytes: Some(256 * 1024 * 1024 * 1024),
            segment_cache_capacity_bytes: 32 * 1024 * 1024,
            max_search_projection_change_log_entries: Some(512),
            max_plan_cache_entries: Some(32),
            slow_query_log_capacity: 128,
            statement_summary_capacity: 128,
            runtime_capabilities: RuntimeCapabilities::mobile_embedded()
                .intersection(crate::compiled_runtime_capabilities()),
            adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy {
                flat_scan_max_documents: 512,
                flat_scan_memory_budget_bytes: 4 * 1024 * 1024,
                ..AdaptiveVectorBackendPolicy::default()
            },
            ..DatabaseConfig::default()
        },
    }
}

fn default_io_budget(
    profile: EmbeddedDeploymentProfile,
    device: StorageDeviceProfile,
) -> IoConcurrencyBudget {
    match profile {
        EmbeddedDeploymentProfile::DesktopBound => {
            IoConcurrencyBudget::desktop_bound_for_device(device)
        }
        EmbeddedDeploymentProfile::MobileEmbedded => {
            IoConcurrencyBudget::mobile_embedded_for_device(device)
        }
    }
}

fn default_runtime_governor_config(profile: EmbeddedDeploymentProfile) -> RuntimeGovernorConfig {
    match profile {
        EmbeddedDeploymentProfile::DesktopBound => RuntimeGovernorConfig::desktop_bound(),
        EmbeddedDeploymentProfile::MobileEmbedded => RuntimeGovernorConfig::mobile_embedded(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        NowledgeMemGraphMode, NowledgeMemReadinessOptions, RuntimeCapability, SkeinError, Value,
        NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
    };
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
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
        let (store, open_report) = SkeinEmbedded::open_nowledge_mem(
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

    #[test]
    fn mobile_profile_uses_bounded_defaults() {
        let options = SkeinEmbeddedOpenOptions::mobile("mobile.db");

        assert_eq!(
            options.deployment_profile,
            EmbeddedDeploymentProfile::MobileEmbedded
        );
        assert_eq!(options.config.max_read_result_rows, Some(512));
        assert_eq!(options.config.max_plan_cache_entries, Some(32));
        assert_eq!(
            options
                .config
                .adaptive_vector_backend_policy
                .flat_scan_memory_budget_bytes,
            4 * 1024 * 1024
        );
        assert_eq!(
            options.config.max_search_projection_change_log_entries,
            Some(512)
        );
        assert!(options.config.runtime_capabilities.full_text_search);
        assert!(options.config.runtime_capabilities.vector_search);
        assert!(!options.config.runtime_capabilities.graph_analytics);
        assert!(!options.config.runtime_capabilities.background_maintenance);
    }

    #[test]
    fn host_can_override_mobile_runtime_capabilities() {
        let options = SkeinEmbeddedOpenOptions::mobile("mobile.db").with_runtime_capabilities(
            RuntimeCapabilities::mobile_embedded()
                .with(crate::RuntimeCapability::GraphAnalytics, true),
        );

        assert!(options.config.runtime_capabilities.graph_analytics);
        assert!(!options.config.runtime_capabilities.background_maintenance);
    }

    #[test]
    fn explicit_storage_io_budget_overrides_profile_default() {
        let root = unique_test_dir("embedded-io-budget");
        let storage_device = StorageDeviceProfile::host_provided(
            skein_qos::StorageMediaKind::Rotational,
            NonZeroUsize::new(1),
        );
        let options = SkeinEmbeddedOpenOptions::mobile(root.join("graph"))
            .with_storage_device_profile(storage_device)
            .with_storage_io_budget(IoConcurrencyBudget::new(7, 2));
        let engine = SkeinEmbedded::open_with_options(options).unwrap();

        assert_eq!(
            engine.deployment_profile(),
            EmbeddedDeploymentProfile::MobileEmbedded
        );
        assert_eq!(engine.runtime_resources().storage_device, storage_device);
        assert_eq!(
            engine.runtime_resources().storage_io.foreground_depth.get(),
            7
        );
        assert_eq!(
            engine.runtime_resources().storage_io.background_depth.get(),
            2
        );
        assert_eq!(
            engine
                .runtime_resources()
                .foreground_segment_read_scheduler(NonZeroU64::new(1 << 20).unwrap())
                .schedule([])
                .io_depth
                .get(),
            7
        );
        let mut search_index = SearchIndex::in_memory();
        engine.configure_search_index(&mut search_index);
        assert_eq!(
            search_index.runtime_capabilities(),
            RuntimeCapabilities::mobile_embedded()
        );
        assert_eq!(search_index.range_read_config().io_depth.get(), 7);
        assert_eq!(
            search_index.range_read_config().max_wave_bytes.get(),
            2 * 1024 * 1024
        );
    }

    #[test]
    fn device_profile_drives_default_io_budget_without_cpu_inference() {
        let root = unique_test_dir("embedded-device-profile");
        let storage_device = StorageDeviceProfile::host_provided(
            skein_qos::StorageMediaKind::NonRotational,
            NonZeroUsize::new(12),
        );
        let engine = SkeinEmbedded::open_with_options(
            SkeinEmbeddedOpenOptions::new(root.join("graph"))
                .with_storage_device_profile(storage_device),
        )
        .unwrap();

        assert_eq!(engine.runtime_resources().storage_device, storage_device);
        assert_eq!(
            engine.runtime_resources().storage_io,
            IoConcurrencyBudget::new(12, 3)
        );
    }

    #[test]
    fn embedded_governor_applies_adaptive_resource_updates() {
        let root = unique_test_dir("embedded-adaptive-runtime");
        let initial = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(8_u64 << 30),
                Some(4_u64 << 30),
                None,
                None,
                None,
            ),
        );
        let mut engine = SkeinEmbedded::open_with_options(
            SkeinEmbeddedOpenOptions::new(root.join("graph"))
                .with_resource_snapshot(initial)
                .with_storage_io_budget(IoConcurrencyBudget::new(8, 2)),
        )
        .unwrap();
        assert_eq!(
            engine
                .runtime_governor()
                .snapshot()
                .limits
                .effective_cpu_slots
                .get(),
            4
        );

        let constrained = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(1),
                NonZeroUsize::new(2),
            ),
            RuntimeMemorySnapshot::from_limits(
                Some(8_u64 << 30),
                Some(256_u64 << 20),
                Some(1_u64 << 30),
                Some(768_u64 << 20),
                Some(600_u64 << 20),
            ),
        );
        assert!(engine.update_runtime_resources(constrained));
        let snapshot = engine.runtime_governor().snapshot();
        assert_eq!(snapshot.limits.effective_cpu_slots.get(), 1);
        assert_eq!(snapshot.resources.memory.pressure.as_str(), "elevated");
        assert_eq!(
            engine.runtime_resources().cpu.effective_parallelism.get(),
            1
        );
        assert_eq!(engine.runtime_resources().memory, constrained.memory);
        assert_eq!(snapshot.pressure_adjustments, 1);
    }

    #[test]
    fn desktop_and_mobile_profiles_share_storage_and_core_cypher_semantics() {
        let root = unique_test_dir("embedded-profile-compatibility");
        let graph_path = root.join("graph");
        let storage_version = {
            let mut desktop = SkeinEmbedded::open(&graph_path).unwrap();
            desktop
                .database_mut()
                .query("CREATE NODE TABLE Memory")
                .unwrap();
            desktop
                .database_mut()
                .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")
                .unwrap();
            desktop
                .database_mut()
                .query_with_params(
                    "CREATE (:Memory {id: $id, title: $title})",
                    &BTreeMap::from([
                        ("id".to_string(), Value::String("desktop".to_string())),
                        (
                            "title".to_string(),
                            Value::String("Written on desktop".to_string()),
                        ),
                    ]),
                )
                .unwrap();
            desktop.database().storage_version()
        };

        {
            let mut mobile =
                SkeinEmbedded::open_with_options(SkeinEmbeddedOpenOptions::mobile(&graph_path))
                    .unwrap();
            assert_eq!(mobile.database().storage_version(), storage_version);
            let output = mobile
                .database_mut()
                .query_with_params(
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
                    &BTreeMap::from([("id".to_string(), Value::String("desktop".to_string()))]),
                )
                .unwrap();
            assert_eq!(
                output.rows[0].get("title"),
                Some(&Value::String("Written on desktop".to_string()))
            );

            let error = mobile
                .database_mut()
                .query("CALL project_graph('memory_graph', ['Memory'], [])")
                .unwrap_err();
            assert_eq!(
                error,
                SkeinError::CapabilityUnavailable {
                    capability: RuntimeCapability::GraphAnalytics
                }
            );

            mobile
                .database_mut()
                .query_with_params(
                    "CREATE (:Memory {id: $id, title: $title})",
                    &BTreeMap::from([
                        ("id".to_string(), Value::String("mobile".to_string())),
                        (
                            "title".to_string(),
                            Value::String("Written on mobile".to_string()),
                        ),
                    ]),
                )
                .unwrap();
        }

        let mut desktop = SkeinEmbedded::open(&graph_path).unwrap();
        let output = desktop
            .database_mut()
            .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id ASC")
            .unwrap();
        assert_eq!(
            output
                .rows
                .iter()
                .map(|row| row.get("id").cloned())
                .collect::<Vec<_>>(),
            vec![
                Some(Value::String("desktop".to_string())),
                Some(Value::String("mobile".to_string())),
            ]
        );
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("skein-{prefix}-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }
}
