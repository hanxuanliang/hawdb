use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTelemetry<'a> {
    pub query_language: &'a str,
    pub statement_kind: &'a str,
    pub success: bool,
    pub elapsed_micros: u64,
    pub row_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KernelTelemetryOperation {
    WalAppend,
    Checkpoint,
    Recovery,
    SearchCheckpoint,
    BackgroundAdmission,
}

impl KernelTelemetryOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WalAppend => "wal_append",
            Self::Checkpoint => "checkpoint",
            Self::Recovery => "recovery",
            Self::SearchCheckpoint => "search_checkpoint",
            Self::BackgroundAdmission => "background_admission",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelTelemetry {
    pub operation: KernelTelemetryOperation,
    pub success: bool,
    pub elapsed_micros: u64,
    pub item_count: usize,
}

pub trait TelemetrySink: Debug + Send + Sync {
    fn record_query(&self, event: QueryTelemetry<'_>);

    fn record_kernel(&self, _event: KernelTelemetry) {}
}

#[cfg(feature = "opentelemetry")]
#[derive(Debug)]
pub struct OpenTelemetryMetrics {
    query_count: opentelemetry::metrics::Counter<u64>,
    query_duration_micros: opentelemetry::metrics::Histogram<u64>,
    query_rows: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_count: opentelemetry::metrics::Counter<u64>,
    kernel_operation_duration_micros: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_items: opentelemetry::metrics::Histogram<u64>,
}

#[cfg(feature = "opentelemetry")]
impl OpenTelemetryMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            query_count: meter.u64_counter("skein.query.count").build(),
            query_duration_micros: meter
                .u64_histogram("skein.query.duration")
                .with_unit("us")
                .build(),
            query_rows: meter.u64_histogram("skein.query.rows").build(),
            kernel_operation_count: meter.u64_counter("skein.kernel.operation.count").build(),
            kernel_operation_duration_micros: meter
                .u64_histogram("skein.kernel.operation.duration")
                .with_unit("us")
                .build(),
            kernel_operation_items: meter.u64_histogram("skein.kernel.operation.items").build(),
        }
    }
}

#[cfg(feature = "opentelemetry")]
impl TelemetrySink for OpenTelemetryMetrics {
    fn record_query(&self, event: QueryTelemetry<'_>) {
        use opentelemetry::KeyValue;

        let attributes = [
            KeyValue::new("db.system", "skein"),
            KeyValue::new("db.query.language", event.query_language.to_string()),
            KeyValue::new("db.operation.name", event.statement_kind.to_string()),
            KeyValue::new("error.type", if event.success { "" } else { "query_error" }),
        ];
        self.query_count.add(1, &attributes);
        self.query_duration_micros
            .record(event.elapsed_micros, &attributes);
        self.query_rows.record(event.row_count as u64, &attributes);
    }

    fn record_kernel(&self, event: KernelTelemetry) {
        use opentelemetry::KeyValue;

        let attributes = [
            KeyValue::new("db.system", "skein"),
            KeyValue::new("db.operation.name", event.operation.as_str()),
            KeyValue::new(
                "error.type",
                if event.success {
                    ""
                } else {
                    "kernel_operation_error"
                },
            ),
        ];
        self.kernel_operation_count.add(1, &attributes);
        self.kernel_operation_duration_micros
            .record(event.elapsed_micros, &attributes);
        self.kernel_operation_items
            .record(event.item_count as u64, &attributes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, SearchDocument, SearchIndex};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Default)]
    struct RecordingSink {
        events: Mutex<Vec<(bool, u64, usize)>>,
        kernel_events: Mutex<Vec<KernelTelemetry>>,
    }

    impl TelemetrySink for RecordingSink {
        fn record_query(&self, event: QueryTelemetry<'_>) {
            self.events.lock().unwrap().push((
                event.success,
                event.elapsed_micros,
                event.row_count,
            ));
        }

        fn record_kernel(&self, event: KernelTelemetry) {
            self.kernel_events.lock().unwrap().push(event);
        }
    }

    #[test]
    fn sink_contract_does_not_require_query_text_or_parameters() {
        let sink = RecordingSink::default();
        sink.record_query(QueryTelemetry {
            query_language: "cypher",
            statement_kind: "match_return",
            success: true,
            elapsed_micros: 12,
            row_count: 3,
        });

        assert_eq!(*sink.events.lock().unwrap(), vec![(true, 12, 3)]);
    }

    #[test]
    fn kernel_sink_contract_uses_bounded_operation_kinds() {
        let sink = RecordingSink::default();
        sink.record_kernel(KernelTelemetry {
            operation: KernelTelemetryOperation::Checkpoint,
            success: true,
            elapsed_micros: 18,
            item_count: 4,
        });

        assert_eq!(
            *sink.kernel_events.lock().unwrap(),
            vec![KernelTelemetry {
                operation: KernelTelemetryOperation::Checkpoint,
                success: true,
                elapsed_micros: 18,
                item_count: 4,
            }]
        );
    }

    #[test]
    fn database_emits_query_metrics_without_owning_a_global_provider() {
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::new();
        database.set_telemetry_sink(Some(sink.clone()));

        database
            .query("CREATE (:Memory {id: 'telemetry-1'})")
            .unwrap();

        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].0);
    }

    #[test]
    fn durable_database_emits_recovery_wal_and_checkpoint_metrics() {
        let path = unique_test_dir("kernel_storage");
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::open(&path).unwrap();
        database.set_telemetry_sink(Some(sink.clone()));

        database
            .query("CREATE (:Memory {id: 'telemetry-durable'})")
            .unwrap();
        database.checkpoint().unwrap();

        let events = sink.kernel_events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.operation)
                .collect::<Vec<_>>(),
            vec![
                KernelTelemetryOperation::Recovery,
                KernelTelemetryOperation::WalAppend,
                KernelTelemetryOperation::Checkpoint,
            ]
        );
        assert!(events.iter().all(|event| event.success));
        assert_eq!(events[1].item_count, 1);
        drop(events);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_checkpoint_emits_bounded_index_metrics() {
        let path = unique_test_dir("kernel_search");
        let sink = Arc::new(RecordingSink::default());
        let mut index = SearchIndex::open(&path).unwrap();
        index.set_telemetry_sink(Some(sink.clone()));
        index
            .upsert(SearchDocument {
                id: "memory:telemetry-search".to_string(),
                title: "Telemetry search".to_string(),
                content: "Bounded index metric".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        index.checkpoint().unwrap();

        let events = sink.kernel_events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].operation,
            KernelTelemetryOperation::SearchCheckpoint
        );
        assert!(events[0].success);
        assert_eq!(events[0].item_count, 1);
        drop(events);
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_telemetry_{name}_{}_{}",
            std::process::id(),
            nonce
        ))
    }
}
