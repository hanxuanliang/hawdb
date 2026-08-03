use crate::qos::{QosTelemetryEvent, QosTelemetrySink};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTelemetry<'a> {
    pub query_language: &'a str,
    pub statement_kind: &'a str,
    pub success: bool,
    pub elapsed_micros: u64,
    pub row_count: usize,
    pub intermediate_rows: usize,
    pub intermediate_payload_bytes: usize,
    pub output_payload_bytes: usize,
    pub steady_resident_bytes: Option<u64>,
    pub peak_resident_bytes: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KernelTelemetryOperation {
    WalAppend,
    Checkpoint,
    Recovery,
    IndexMaintenance,
    SearchCheckpoint,
    BackgroundAdmission,
}

impl KernelTelemetryOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WalAppend => "wal_append",
            Self::Checkpoint => "checkpoint",
            Self::Recovery => "recovery",
            Self::IndexMaintenance => "index_maintenance",
            Self::SearchCheckpoint => "search_checkpoint",
            Self::BackgroundAdmission => "background_admission",
        }
    }
}

pub const REQUIRED_OPERATIONS_TELEMETRY: [KernelTelemetryOperation; 6] = [
    KernelTelemetryOperation::WalAppend,
    KernelTelemetryOperation::Checkpoint,
    KernelTelemetryOperation::Recovery,
    KernelTelemetryOperation::IndexMaintenance,
    KernelTelemetryOperation::SearchCheckpoint,
    KernelTelemetryOperation::BackgroundAdmission,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationsTelemetryReadiness {
    pub ready: bool,
    pub graph_sink_configured: bool,
    pub search_projection_sink_configured: bool,
    pub required_operations: Vec<KernelTelemetryOperation>,
    pub blocker_codes: Vec<String>,
}

pub fn operations_telemetry_readiness(
    graph_sink_configured: bool,
    search_projection_sink_configured: bool,
) -> OperationsTelemetryReadiness {
    let mut blocker_codes = Vec::new();
    if !graph_sink_configured {
        blocker_codes.push("operations_telemetry_graph_sink_missing".to_string());
    }
    if !search_projection_sink_configured {
        blocker_codes.push("operations_telemetry_search_projection_sink_missing".to_string());
    }
    OperationsTelemetryReadiness {
        ready: blocker_codes.is_empty(),
        graph_sink_configured,
        search_projection_sink_configured,
        required_operations: REQUIRED_OPERATIONS_TELEMETRY.to_vec(),
        blocker_codes,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelTelemetry {
    pub operation: KernelTelemetryOperation,
    pub success: bool,
    pub elapsed_micros: u64,
    pub item_count: usize,
    pub byte_count: u64,
    pub fsync_micros: u64,
    pub generation: Option<u64>,
}

pub trait TelemetrySink: Debug + Send + Sync {
    fn record_query(&self, event: QueryTelemetry<'_>);

    fn record_kernel(&self, _event: KernelTelemetry) {}

    fn record_qos(&self, _event: QosTelemetryEvent) {}
}

#[derive(Debug)]
struct HostQosTelemetrySink {
    telemetry: Arc<dyn TelemetrySink>,
}

impl QosTelemetrySink for HostQosTelemetrySink {
    fn record_qos(&self, event: QosTelemetryEvent) {
        self.telemetry.record_qos(event);
    }
}

pub fn qos_telemetry_sink(telemetry: Arc<dyn TelemetrySink>) -> Arc<dyn QosTelemetrySink> {
    Arc::new(HostQosTelemetrySink { telemetry })
}

#[cfg(feature = "opentelemetry")]
#[derive(Debug)]
pub struct OpenTelemetryMetrics {
    query_count: opentelemetry::metrics::Counter<u64>,
    query_duration_micros: opentelemetry::metrics::Histogram<u64>,
    query_rows: opentelemetry::metrics::Histogram<u64>,
    query_intermediate_rows: opentelemetry::metrics::Histogram<u64>,
    query_intermediate_bytes: opentelemetry::metrics::Histogram<u64>,
    query_output_bytes: opentelemetry::metrics::Histogram<u64>,
    query_steady_resident_bytes: opentelemetry::metrics::Histogram<u64>,
    query_peak_resident_bytes: opentelemetry::metrics::Histogram<u64>,
    query_minor_page_faults: opentelemetry::metrics::Histogram<u64>,
    query_major_page_faults: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_count: opentelemetry::metrics::Counter<u64>,
    kernel_operation_duration_micros: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_items: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_bytes: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_fsync_micros: opentelemetry::metrics::Histogram<u64>,
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
            query_intermediate_rows: meter.u64_histogram("skein.query.intermediate.rows").build(),
            query_intermediate_bytes: meter
                .u64_histogram("skein.query.intermediate.bytes")
                .build(),
            query_output_bytes: meter.u64_histogram("skein.query.output.bytes").build(),
            query_steady_resident_bytes: meter.u64_histogram("skein.query.resident.steady").build(),
            query_peak_resident_bytes: meter.u64_histogram("skein.query.resident.peak").build(),
            query_minor_page_faults: meter.u64_histogram("skein.query.page_faults.minor").build(),
            query_major_page_faults: meter.u64_histogram("skein.query.page_faults.major").build(),
            kernel_operation_count: meter.u64_counter("skein.kernel.operation.count").build(),
            kernel_operation_duration_micros: meter
                .u64_histogram("skein.kernel.operation.duration")
                .with_unit("us")
                .build(),
            kernel_operation_items: meter.u64_histogram("skein.kernel.operation.items").build(),
            kernel_operation_bytes: meter.u64_histogram("skein.kernel.operation.bytes").build(),
            kernel_operation_fsync_micros: meter
                .u64_histogram("skein.kernel.operation.fsync_duration")
                .with_unit("us")
                .build(),
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
        self.query_intermediate_rows
            .record(event.intermediate_rows as u64, &attributes);
        self.query_intermediate_bytes
            .record(event.intermediate_payload_bytes as u64, &attributes);
        self.query_output_bytes
            .record(event.output_payload_bytes as u64, &attributes);
        if let Some(bytes) = event.steady_resident_bytes {
            self.query_steady_resident_bytes.record(bytes, &attributes);
        }
        if let Some(bytes) = event.peak_resident_bytes {
            self.query_peak_resident_bytes.record(bytes, &attributes);
        }
        if let Some(faults) = event.minor_page_faults {
            self.query_minor_page_faults.record(faults, &attributes);
        }
        if let Some(faults) = event.major_page_faults {
            self.query_major_page_faults.record(faults, &attributes);
        }
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
        self.kernel_operation_bytes
            .record(event.byte_count, &attributes);
        self.kernel_operation_fsync_micros
            .record(event.fsync_micros, &attributes);
    }

    fn record_qos(&self, event: QosTelemetryEvent) {
        use opentelemetry::KeyValue;

        let attributes = [
            KeyValue::new("db.system", "skein"),
            KeyValue::new("db.operation.name", "background_qos"),
            KeyValue::new("skein.qos.phase", event.phase.as_str()),
            KeyValue::new("skein.qos.outcome", event.outcome.as_str()),
            KeyValue::new("skein.work.class", event.class.as_str()),
            KeyValue::new(
                "skein.qos.admission_code",
                event.admission_code.map(|code| code.as_str()).unwrap_or(""),
            ),
        ];
        self.kernel_operation_count.add(1, &attributes);
        self.kernel_operation_duration_micros
            .record(event.elapsed_micros, &attributes);
        self.kernel_operation_items
            .record(event.estimated_operations as u64, &attributes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::{QosTelemetryOutcome, QosTelemetryPhase};
    use crate::{
        Database, LocalQosPolicy, LocalQosScheduler, MetadataRepairOptions, SearchDocument,
        SearchIndex, SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
        SearchRebuildOptions,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Default)]
    struct RecordingSink {
        events: Mutex<Vec<(bool, u64, usize)>>,
        kernel_events: Mutex<Vec<KernelTelemetry>>,
        qos_events: Mutex<Vec<QosTelemetryEvent>>,
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

        fn record_qos(&self, event: QosTelemetryEvent) {
            self.qos_events.lock().unwrap().push(event);
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
            intermediate_rows: 7,
            intermediate_payload_bytes: 128,
            output_payload_bytes: 64,
            steady_resident_bytes: Some(1024),
            peak_resident_bytes: Some(2048),
            minor_page_faults: Some(3),
            major_page_faults: Some(1),
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
            byte_count: 128,
            fsync_micros: 7,
            generation: Some(3),
        });

        assert_eq!(
            *sink.kernel_events.lock().unwrap(),
            vec![KernelTelemetry {
                operation: KernelTelemetryOperation::Checkpoint,
                success: true,
                elapsed_micros: 18,
                item_count: 4,
                byte_count: 128,
                fsync_micros: 7,
                generation: Some(3),
            }]
        );
    }

    #[test]
    fn operations_telemetry_readiness_requires_host_owned_graph_and_search_sinks() {
        let missing = operations_telemetry_readiness(false, false);
        assert!(!missing.ready);
        assert_eq!(missing.required_operations, REQUIRED_OPERATIONS_TELEMETRY);
        assert!(missing
            .blocker_codes
            .contains(&"operations_telemetry_graph_sink_missing".to_string()));
        assert!(missing
            .blocker_codes
            .contains(&"operations_telemetry_search_projection_sink_missing".to_string()));

        let ready = operations_telemetry_readiness(true, true);
        assert!(ready.ready);
        assert!(ready.blocker_codes.is_empty());
        assert!(ready
            .required_operations
            .contains(&KernelTelemetryOperation::IndexMaintenance));
        assert!(ready
            .required_operations
            .contains(&KernelTelemetryOperation::BackgroundAdmission));
    }

    #[test]
    fn qos_adapter_forwards_only_typed_bounded_fields() {
        let sink = Arc::new(RecordingSink::default());
        let adapter = qos_telemetry_sink(sink.clone());

        adapter.record_qos(QosTelemetryEvent {
            phase: QosTelemetryPhase::Admission,
            outcome: QosTelemetryOutcome::Deferred,
            class: crate::qos::WorkClass::Projection,
            estimated_operations: 8,
            elapsed_micros: 0,
            admission_code: Some(crate::qos::QosAdmissionCode::PerWorkLimitExceeded),
        });

        assert_eq!(
            *sink.qos_events.lock().unwrap(),
            vec![QosTelemetryEvent {
                phase: QosTelemetryPhase::Admission,
                outcome: QosTelemetryOutcome::Deferred,
                class: crate::qos::WorkClass::Projection,
                estimated_operations: 8,
                elapsed_micros: 0,
                admission_code: Some(crate::qos::QosAdmissionCode::PerWorkLimitExceeded),
            }]
        );
    }

    #[test]
    fn scheduled_search_work_uses_the_host_telemetry_sink_automatically() {
        let sink = Arc::new(RecordingSink::default());
        let mut index = SearchIndex::in_memory();
        index.set_telemetry_sink(Some(sink.clone()));
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        index
            .apply_scheduled_background_projection_delta(
                &mut scheduler,
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "qos-telemetry".to_string(),
                        title: "QoS telemetry".to_string(),
                        body: "Scheduled projection".to_string(),
                        embedding: None,
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    ..SearchProjectionDelta::default()
                },
            )
            .unwrap();

        let events = sink.qos_events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].phase, QosTelemetryPhase::Admission);
        assert_eq!(events[0].outcome, QosTelemetryOutcome::Admitted);
        assert_eq!(events[1].phase, QosTelemetryPhase::Completion);
        assert_eq!(events[1].outcome, QosTelemetryOutcome::Completed);
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
    fn database_operations_telemetry_readiness_uses_configured_library_sinks() {
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::new();
        let mut search_index = SearchIndex::in_memory();

        let missing = database.operations_telemetry_readiness(Some(&search_index));
        assert!(!missing.ready);

        database.set_telemetry_sink(Some(sink.clone()));
        let graph_only = database.operations_telemetry_readiness(Some(&search_index));
        assert!(!graph_only.ready);
        assert!(graph_only.graph_sink_configured);
        assert!(!graph_only.search_projection_sink_configured);

        search_index.set_telemetry_sink(Some(sink));
        let ready = database.operations_telemetry_readiness(Some(&search_index));
        assert!(ready.ready);
        assert!(ready.blocker_codes.is_empty());
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

    #[test]
    fn search_projection_rebuild_and_repair_emit_index_maintenance_metrics() {
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::new();
        database
            .query("CREATE (:Memory {id: 'telemetry-index', title: 'Index telemetry'})")
            .unwrap();
        let mut index = SearchIndex::in_memory();
        index.set_telemetry_sink(Some(sink.clone()));

        database
            .rebuild_search_projection(&mut index, SearchRebuildOptions::default())
            .unwrap();
        database
            .repair_search_projection_metadata(&mut index, MetadataRepairOptions::default())
            .unwrap();

        let events = sink.kernel_events.lock().unwrap();
        let index_events = events
            .iter()
            .filter(|event| event.operation == KernelTelemetryOperation::IndexMaintenance)
            .collect::<Vec<_>>();
        assert_eq!(index_events.len(), 2);
        assert!(index_events.iter().all(|event| event.success));
        assert!(index_events.iter().all(|event| event.item_count >= 1));
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
