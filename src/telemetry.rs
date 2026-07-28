use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTelemetry<'a> {
    pub query_language: &'a str,
    pub statement_kind: &'a str,
    pub success: bool,
    pub elapsed_micros: u64,
    pub row_count: usize,
}

pub trait TelemetrySink: Debug + Send + Sync {
    fn record_query(&self, event: QueryTelemetry<'_>);
}

#[cfg(feature = "opentelemetry")]
#[derive(Debug)]
pub struct OpenTelemetryMetrics {
    query_count: opentelemetry::metrics::Counter<u64>,
    query_duration_micros: opentelemetry::metrics::Histogram<u64>,
    query_rows: opentelemetry::metrics::Histogram<u64>,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct RecordingSink {
        events: Mutex<Vec<(bool, u64, usize)>>,
    }

    impl TelemetrySink for RecordingSink {
        fn record_query(&self, event: QueryTelemetry<'_>) {
            self.events.lock().unwrap().push((
                event.success,
                event.elapsed_micros,
                event.row_count,
            ));
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
}
