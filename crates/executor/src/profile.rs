use crate::columnar::ColumnarRowRef;
use skein_core::{Value, ValueRef};
use std::collections::BTreeMap;

pub type Row = BTreeMap<String, Value>;

/// A row view whose values remain valid only for the current consumer call.
///
/// The map representation covers the scalar fallback executor. The columnar
/// representation lets a downstream consumer pull selected values directly
/// from an immutable batch without constructing an intermediate row map.
#[derive(Debug, Clone, Copy)]
pub enum RowRef<'a> {
    Map(&'a Row),
    Columnar(ColumnarRowRef<'a>),
}

impl<'a> RowRef<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Map(row) => row.len(),
            Self::Columnar(_) => self.iter().count(),
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub fn get(self, name: &str) -> Option<ValueRef<'a>> {
        match self {
            Self::Map(row) => row.get(name).map(Value::as_ref),
            Self::Columnar(row) => row.get(name),
        }
    }

    pub fn column(self, index: usize) -> Option<(&'a str, ValueRef<'a>)> {
        match self {
            Self::Map(row) => row
                .iter()
                .nth(index)
                .map(|(name, value)| (name.as_str(), value.as_ref())),
            Self::Columnar(row) => row.column(index),
        }
    }

    pub fn iter(self) -> RowRefIter<'a> {
        match self {
            Self::Map(row) => RowRefIter::Map(row.iter()),
            Self::Columnar(row) => RowRefIter::Columnar { row, index: 0 },
        }
    }

    pub fn to_owned_row(self) -> Row {
        self.iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned_value()))
            .collect()
    }
}

impl<'a> From<&'a Row> for RowRef<'a> {
    fn from(row: &'a Row) -> Self {
        Self::Map(row)
    }
}

impl<'a> From<ColumnarRowRef<'a>> for RowRef<'a> {
    fn from(row: ColumnarRowRef<'a>) -> Self {
        Self::Columnar(row)
    }
}

pub enum RowRefIter<'a> {
    Map(std::collections::btree_map::Iter<'a, String, Value>),
    Columnar {
        row: ColumnarRowRef<'a>,
        index: usize,
    },
}

impl<'a> Iterator for RowRefIter<'a> {
    type Item = (&'a str, ValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Map(iter) => iter
                .next()
                .map(|(name, value)| (name.as_str(), value.as_ref())),
            Self::Columnar { row, index } => loop {
                let current = *index;
                *index = index.saturating_add(1);
                if current >= row.schema().len() {
                    return None;
                }
                if let Some(column) = row.column(current) {
                    return Some(column);
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingOperatorMemoryReport {
    pub operator: String,
    pub budget_bytes: usize,
    pub peak_tracked_bytes: usize,
    pub input_rows: usize,
    pub max_spill_bytes: u64,
    pub max_spill_runs: usize,
    pub spilled_bytes: u64,
    pub spill_run_count: usize,
    pub spilled_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PipelineMemoryReport {
    /// Query-owned runtime ledger budget across pipeline and blocking state.
    pub query_memory_budget_bytes: usize,
    /// Highest aggregate tracked resident bytes charged to the query ledger.
    pub query_memory_peak_bytes: usize,
    /// Tracked bytes still owned at the execution completion boundary.
    pub query_memory_completion_bytes: usize,
    /// Number of operator or transfer accounts created by the query.
    pub query_memory_account_count: usize,
    /// Sum of rows emitted at every physical operator boundary.
    pub intermediate_rows: usize,
    /// Sum of retained payload estimates emitted at every physical operator boundary.
    pub intermediate_payload_bytes: usize,
    pub peak_batch_rows: usize,
    pub peak_batch_payload_bytes: usize,
    /// Number of typed columnar batches evaluated by eligible pipeline fragments.
    pub columnar_batches: usize,
    /// Rows loaded into typed columns before selection.
    pub columnar_input_rows: usize,
    /// Rows retained by columnar selections.
    pub columnar_selected_rows: usize,
    /// Morsels consumed by columnar fragments.
    pub morsel_count: usize,
    /// Highest resource-admitted worker count across morsel pipelines.
    pub morsel_max_admitted_workers: usize,
    /// Highest worker count actually used by a morsel scheduler.
    pub morsel_peak_active_workers: usize,
    /// Highest number of completed morsel outputs awaiting or crossing the
    /// coordinator's ordered consumer boundary.
    pub morsel_peak_buffered_outputs: usize,
    /// Highest estimated resident bytes held by those completed outputs.
    pub morsel_peak_buffered_output_bytes: usize,
    /// Highest number of out-of-order outputs held by the ordinal merger.
    pub morsel_peak_reorder_entries: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    /// Resident memory before execution, when process sampling is supported.
    pub start_resident_bytes: Option<u64>,
    /// Process high-water resident memory before execution.
    pub start_peak_resident_bytes: Option<u64>,
    /// Resident memory after the output rows have been materialized.
    pub steady_resident_bytes: Option<u64>,
    /// Process high-water resident memory at completion.
    pub peak_resident_bytes: Option<u64>,
    /// Positive resident-memory delta retained at completion.
    pub steady_resident_growth_bytes: Option<u64>,
    /// Positive process high-water delta observed during execution.
    pub lifetime_peak_resident_growth_bytes: Option<u64>,
    /// Aggregate page-fault delta when the platform exposes it.
    pub total_page_faults: Option<u64>,
    /// Split page-fault deltas only on platforms that expose this distinction.
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadExecutionProfile<TScanPruningReport> {
    pub max_rows: Option<usize>,
    pub detection_row_cap: Option<usize>,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_kinds: Vec<String>,
    pub scan_pruning_reports: Vec<TScanPruningReport>,
    pub vector_execution_reports: Vec<crate::VectorExecutionReport>,
    pub graph_expansion_reports: Vec<crate::GraphExpansionExecutionReport>,
    pub blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
    pub pipeline_memory_report: PipelineMemoryReport,
}

impl<TScanPruningReport> ReadExecutionProfile<TScanPruningReport> {
    pub fn blocking_operator_count(&self) -> usize {
        self.blocking_operator_kinds.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfiledQueryRows<TScanPruningReport> {
    pub rows: Vec<Row>,
    pub profile: ReadExecutionProfile<TScanPruningReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfiledQueryStream<TScanPruningReport> {
    /// True when the physical plan emitted batches directly to the consumer.
    /// False means an unsupported operator still materialized bindings before
    /// the bounded consumer boundary.
    pub fully_streamed: bool,
    pub profile: ReadExecutionProfile<TScanPruningReport>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BindingSchema, ColumnVector, ColumnarBatch, SlotDescriptor, SlotId, SlotType, Validity,
    };
    use skein_core::LogicalType;
    use std::sync::Arc;

    #[test]
    fn counts_blocking_operator_kinds() {
        let profile = ReadExecutionProfile::<()> {
            max_rows: Some(10),
            detection_row_cap: Some(11),
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            blocking_operator_kinds: vec!["sort".to_string(), "aggregate".to_string()],
            scan_pruning_reports: Vec::new(),
            vector_execution_reports: Vec::new(),
            graph_expansion_reports: Vec::new(),
            blocking_operator_memory_reports: Vec::new(),
            pipeline_memory_report: PipelineMemoryReport::default(),
        };
        assert_eq!(profile.blocking_operator_count(), 2);
    }

    #[test]
    fn borrowed_map_row_materializes_only_on_request() {
        let row = Row::from([
            ("id".to_string(), Value::Int(7)),
            ("content".to_string(), Value::String("payload".into())),
        ]);
        let row_ref = RowRef::from(&row);

        assert_eq!(row_ref.get("id"), Some(ValueRef::Int(7)));
        assert_eq!(row_ref.get("content").unwrap().as_str(), Some("payload"));
        assert_eq!(row_ref.to_owned_row(), row);
    }

    #[test]
    fn borrowed_columnar_row_pulls_values_without_a_row_map() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![
                SlotDescriptor {
                    id: SlotId(0),
                    name: "id".into(),
                    slot_type: SlotType::NodeId,
                },
                SlotDescriptor {
                    id: SlotId(1),
                    name: "content".into(),
                    slot_type: SlotType::logical(LogicalType::Text),
                },
            ])
            .unwrap(),
        );
        let batch = ColumnarBatch::try_new(
            schema,
            vec![
                Arc::new(ColumnVector::node_ids(vec![7])),
                Arc::new(ColumnVector::Utf8 {
                    values: vec!["payload".to_string()].into(),
                    validity: Validity::all(1),
                }),
            ],
        )
        .unwrap();
        let row_ref = RowRef::from(batch.rows().next().unwrap());

        assert_eq!(row_ref.get("id"), Some(ValueRef::Int(7)));
        assert_eq!(row_ref.get("content").unwrap().as_str(), Some("payload"));
        assert_eq!(
            row_ref.to_owned_row(),
            Row::from([
                ("content".into(), Value::String("payload".into())),
                ("id".into(), Value::Int(7)),
            ])
        );
    }
}
