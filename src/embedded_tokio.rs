use crate::{
    QueryOutput, QueryStreamOptions, SkeinEmbedded, SkeinEmbeddedOpenOptions, SkeinError, Value,
};
use skein_core::RuntimeTaskContext;
use skein_qos::{
    RuntimeGovernorSnapshot, RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest, WorkClass,
    WorkPriority,
};
use skein_runtime_tokio::{
    TokioHandle, TokioRuntimeAdapter, TokioRuntimeConfig, TokioRuntimeError, TokioRuntimeOwnership,
    TokioTaskError,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone)]
pub struct SkeinTokioEmbedded {
    embedded: Arc<Mutex<SkeinEmbedded>>,
    runtime: TokioRuntimeAdapter,
}

#[derive(Debug)]
pub enum SkeinTokioEmbeddedError {
    Database(SkeinError),
    Runtime(TokioRuntimeError),
    Task(TokioTaskError<SkeinError>),
}

impl Display for SkeinTokioEmbeddedError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => Display::fmt(error, formatter),
            Self::Runtime(error) => Display::fmt(error, formatter),
            Self::Task(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for SkeinTokioEmbeddedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Task(error) => Some(error),
        }
    }
}

impl From<SkeinError> for SkeinTokioEmbeddedError {
    fn from(error: SkeinError) -> Self {
        Self::Database(error)
    }
}

impl From<TokioRuntimeError> for SkeinTokioEmbeddedError {
    fn from(error: TokioRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<TokioTaskError<SkeinError>> for SkeinTokioEmbeddedError {
    fn from(error: TokioTaskError<SkeinError>) -> Self {
        Self::Task(error)
    }
}

impl SkeinTokioEmbedded {
    pub fn open_owned(options: SkeinEmbeddedOpenOptions) -> Result<Self, SkeinTokioEmbeddedError> {
        let embedded = SkeinEmbedded::open_with_options(options)?;
        let config = TokioRuntimeConfig::from_governor(embedded.runtime_governor());
        Self::from_owned(embedded, config)
    }

    pub fn open_owned_with_config(
        options: SkeinEmbeddedOpenOptions,
        config: TokioRuntimeConfig,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        Self::from_owned(SkeinEmbedded::open_with_options(options)?, config)
    }

    pub fn open_borrowed(
        options: SkeinEmbeddedOpenOptions,
        handle: TokioHandle,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        let embedded = SkeinEmbedded::open_with_options(options)?;
        let config = TokioRuntimeConfig::from_governor(embedded.runtime_governor());
        Ok(Self::from_borrowed(embedded, handle, config))
    }

    pub fn open_borrowed_with_config(
        options: SkeinEmbeddedOpenOptions,
        handle: TokioHandle,
        config: TokioRuntimeConfig,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        let embedded = SkeinEmbedded::open_with_options(options)?;
        Ok(Self::from_borrowed(embedded, handle, config))
    }

    pub fn from_owned(
        embedded: SkeinEmbedded,
        config: TokioRuntimeConfig,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        let runtime = TokioRuntimeAdapter::owned(embedded.runtime_governor().clone(), config)?;
        Ok(Self {
            embedded: Arc::new(Mutex::new(embedded)),
            runtime,
        })
    }

    pub fn from_borrowed(
        embedded: SkeinEmbedded,
        handle: TokioHandle,
        config: TokioRuntimeConfig,
    ) -> Self {
        let runtime =
            TokioRuntimeAdapter::borrowed(handle, embedded.runtime_governor().clone(), config);
        Self {
            embedded: Arc::new(Mutex::new(embedded)),
            runtime,
        }
    }

    pub fn runtime(&self) -> &TokioRuntimeAdapter {
        &self.runtime
    }

    pub fn ownership(&self) -> TokioRuntimeOwnership {
        self.runtime.ownership()
    }

    pub fn runtime_snapshot(&self) -> RuntimeGovernorSnapshot {
        self.runtime.governor_snapshot()
    }

    pub fn refresh_runtime_resources(&self) -> bool {
        lock_embedded(&self.embedded).refresh_runtime_resources()
    }

    pub fn with_embedded<R>(&self, operation: impl FnOnce(&SkeinEmbedded) -> R) -> R {
        operation(&lock_embedded(&self.embedded))
    }

    pub fn with_embedded_mut<R>(&self, operation: impl FnOnce(&mut SkeinEmbedded) -> R) -> R {
        operation(&mut lock_embedded(&self.embedded))
    }

    pub async fn query(
        &self,
        cypher_text: impl Into<String>,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        self.query_with_params(cypher_text, BTreeMap::new(), task_context)
            .await
    }

    pub async fn query_with_params(
        &self,
        cypher_text: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        let cypher_text = cypher_text.into();
        let admission = self.with_embedded_mut(|embedded| {
            embedded
                .database_mut()
                .runtime_admission_plan(&cypher_text, &parameters)
        })?;
        let result_budget_bytes = self.runtime_snapshot().limits.result_budget_bytes;
        let priority = match admission.work_request.priority {
            WorkPriority::Foreground => RuntimeWorkPriority::Foreground,
            WorkPriority::Background => RuntimeWorkPriority::Background,
        };
        let request = if admission.is_mutation {
            RuntimeWorkRequest::mutation(priority, admission.estimated_memory_bytes)
        } else {
            let kind = match admission.work_request.class {
                WorkClass::Query | WorkClass::Mutation | WorkClass::Analytics => {
                    RuntimeWorkKind::Query
                }
                WorkClass::Projection | WorkClass::Import => RuntimeWorkKind::Maintenance,
                WorkClass::Shadow => RuntimeWorkKind::Control,
            };
            RuntimeWorkRequest::query(
                priority,
                admission.estimated_memory_bytes,
                result_budget_bytes,
            )
            .with_kind(kind)
        };
        self.execute_query_with_request(
            cypher_text,
            parameters,
            request,
            admission.streaming_eligible,
            task_context,
        )
        .await
    }

    pub async fn query_with_request(
        &self,
        cypher_text: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        request: RuntimeWorkRequest,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        let cypher_text = cypher_text.into();
        let admission = self.with_embedded_mut(|embedded| {
            embedded
                .database_mut()
                .runtime_admission_plan(&cypher_text, &parameters)
        })?;
        let request = if admission.is_mutation {
            request
                .with_kind(RuntimeWorkKind::Mutation)
                .with_memory_bytes(request.memory_bytes.max(admission.estimated_memory_bytes))
                .with_result_bytes(0)
        } else if request.kind == RuntimeWorkKind::Mutation {
            request
                .with_kind(RuntimeWorkKind::Query)
                .with_memory_bytes(request.memory_bytes.max(admission.estimated_memory_bytes))
        } else {
            request.with_memory_bytes(request.memory_bytes.max(admission.estimated_memory_bytes))
        };
        let request = if admission.is_mutation || request.result_bytes > 0 {
            request
        } else {
            request.with_result_bytes(self.runtime_snapshot().limits.result_budget_bytes)
        };
        self.execute_query_with_request(
            cypher_text,
            parameters,
            request,
            admission.streaming_eligible,
            task_context,
        )
        .await
    }

    async fn execute_query_with_request(
        &self,
        cypher_text: String,
        parameters: BTreeMap<String, Value>,
        request: RuntimeWorkRequest,
        streaming_eligible: bool,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        let embedded = Arc::clone(&self.embedded);
        if request.kind == RuntimeWorkKind::Mutation {
            self.runtime
                .execute_blocking(request, task_context, move |task_context| {
                    lock_embedded(&embedded)
                        .database_mut()
                        .query_with_params_context(&cypher_text, &parameters, task_context)
                })
                .await
                .map_err(SkeinTokioEmbeddedError::Task)
        } else {
            let max_rows =
                self.with_embedded(|embedded| embedded.database().config().max_read_result_rows);
            let max_payload_bytes = usize::try_from(request.result_bytes).unwrap_or(usize::MAX);
            self.runtime
                .execute_blocking(request, task_context, move |task_context| {
                    let mut read_transaction =
                        lock_embedded(&embedded).database().begin_read_transaction();
                    if !streaming_eligible {
                        return read_transaction.query_with_params_context(
                            &cypher_text,
                            &parameters,
                            task_context,
                        );
                    }
                    let mut rows = Vec::new();
                    read_transaction.query_with_params_streaming_context(
                        &cypher_text,
                        &parameters,
                        QueryStreamOptions {
                            max_rows,
                            max_payload_bytes: Some(max_payload_bytes),
                        },
                        task_context,
                        |row| {
                            rows.push(row);
                            Ok(())
                        },
                    )?;
                    Ok(QueryOutput { rows })
                })
                .await
                .map_err(SkeinTokioEmbeddedError::Task)
        }
    }
}

fn lock_embedded(embedded: &Mutex<SkeinEmbedded>) -> MutexGuard<'_, SkeinEmbedded> {
    embedded
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmbeddedDeploymentProfile;
    use skein_core::RuntimeCancellationToken;
    use skein_qos::{RuntimeTelemetryEvent, RuntimeTelemetryEventKind, RuntimeTelemetrySink};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Default)]
    struct RuntimeEvents(Mutex<Vec<RuntimeTelemetryEvent>>);

    impl RuntimeTelemetrySink for RuntimeEvents {
        fn record_runtime(&self, event: RuntimeTelemetryEvent) {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event);
        }
    }

    #[test]
    fn owned_facade_runs_queries_through_the_bounded_adapter() {
        let path = unique_test_path("owned");
        let embedded =
            SkeinTokioEmbedded::open_owned(SkeinEmbeddedOpenOptions::new(&path)).unwrap();
        assert_eq!(embedded.ownership(), TokioRuntimeOwnership::Owned);

        embedded
            .runtime()
            .block_on(embedded.query(
                "CREATE (:Memory {id: 'runtime'})",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        let output = embedded
            .runtime()
            .block_on(embedded.query(
                "MATCH (m:Memory) RETURN m.id AS id",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("runtime".to_string()))
        );
        assert_eq!(embedded.runtime_snapshot().completions, 2);
    }

    #[test]
    fn borrowed_facade_keeps_the_host_runtime_alive() {
        let path = unique_test_path("borrowed");
        let host = tokio_runtime();
        let embedded = SkeinTokioEmbedded::open_borrowed(
            SkeinEmbeddedOpenOptions::mobile(&path),
            host.handle().clone(),
        )
        .unwrap();
        assert_eq!(embedded.ownership(), TokioRuntimeOwnership::Borrowed);
        assert_eq!(
            embedded.with_embedded(SkeinEmbedded::deployment_profile),
            EmbeddedDeploymentProfile::MobileEmbedded
        );
        host.block_on(embedded.query("CREATE (:Probe {value: 1})", RuntimeTaskContext::default()))
            .unwrap();
        let output = host
            .block_on(embedded.query(
                "MATCH (p:Probe) RETURN p.value AS probe",
                RuntimeTaskContext::default(),
            ))
            .unwrap();
        assert_eq!(output.rows[0].get("probe"), Some(&Value::Int(1)));
        drop(embedded);
        assert_eq!(host.block_on(async { 9 }), 9);
    }

    #[test]
    fn admission_uses_physical_mutation_semantics() {
        let path = unique_test_path("admission-semantics");
        let mut embedded = SkeinEmbedded::open(&path).unwrap();
        let create = embedded
            .database_mut()
            .runtime_admission_plan("CREATE (:Probe {value: 1})", &BTreeMap::new())
            .unwrap();
        let read = embedded
            .database_mut()
            .runtime_admission_plan("MATCH (p:Probe) RETURN p.value AS value", &BTreeMap::new())
            .unwrap();

        assert_eq!(create.work_request.class, WorkClass::Query);
        assert!(create.is_mutation);
        assert!(!read.is_mutation);
        assert!(create.estimated_memory_bytes > 0);
        assert!(read.estimated_memory_bytes > 0);
        assert!(read.streaming_eligible);
    }

    #[test]
    fn cancelled_query_is_rejected_before_database_execution() {
        let path = unique_test_path("cancelled-before-start");
        let embedded =
            SkeinTokioEmbedded::open_owned(SkeinEmbeddedOpenOptions::new(&path)).unwrap();
        let token = RuntimeCancellationToken::new();
        token.cancel();
        let result = embedded
            .runtime()
            .block_on(embedded.query(
                "CREATE (:Probe {value: 1})",
                RuntimeTaskContext::without_deadline(token),
            ))
            .unwrap();

        assert!(matches!(
            result,
            Err(SkeinTokioEmbeddedError::Task(TokioTaskError::Stopped(
                skein_core::RuntimeCancellationReason::Cancelled
            )))
        ));
        let count = embedded
            .runtime()
            .block_on(embedded.query(
                "MATCH (p:Probe) RETURN p.value AS value",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        assert!(count.rows.is_empty());
    }

    #[test]
    fn custom_request_cannot_override_mutation_semantics() {
        let path = unique_test_path("custom-request-mutation");
        let embedded =
            SkeinTokioEmbedded::open_owned(SkeinEmbeddedOpenOptions::new(&path)).unwrap();
        let events = Arc::new(RuntimeEvents::default());
        embedded
            .runtime()
            .governor()
            .set_telemetry_sink(Some(events.clone()));
        embedded
            .runtime()
            .block_on(embedded.query_with_request(
                "CREATE (:Probe {value: 1})",
                BTreeMap::new(),
                RuntimeWorkRequest::foreground_query(0, 1024),
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();

        assert_eq!(embedded.runtime_snapshot().completions, 1);
        assert!(events
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|event| {
                event.kind == RuntimeTelemetryEventKind::Admitted
                    && event.work_kind == Some(RuntimeWorkKind::Mutation)
            }));
    }

    #[test]
    fn default_query_enforces_the_governor_result_byte_budget() {
        let path = unique_test_path("result-byte-budget");
        let options = SkeinEmbeddedOpenOptions::new(&path).with_runtime_governor_config(
            skein_qos::RuntimeGovernorConfig {
                result_budget_bytes: 64,
                ..skein_qos::RuntimeGovernorConfig::desktop_bound()
            },
        );
        let embedded = SkeinTokioEmbedded::open_owned(options).unwrap();
        embedded
            .runtime()
            .block_on(embedded.query(
                format!("CREATE (:Probe {{value: '{}'}})", "x".repeat(256)),
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();

        let error = embedded
            .runtime()
            .block_on(embedded.query(
                "MATCH (p:Probe) RETURN p.value AS value",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("max_payload_bytes 64"));
    }

    fn tokio_runtime() -> skein_runtime_tokio::TokioRuntime {
        skein_runtime_tokio::TokioRuntimeBuilder::new_multi_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    fn unique_test_path(prefix: &str) -> std::path::PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "skein-tokio-embedded-{prefix}-{}-{id}",
            std::process::id()
        ))
    }
}
