use crate::binding::value_payload_bytes;
use crate::columnar::ColumnarRowRef;
use skein_core::{Result, SkeinError, Value, ValueRef};
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::{Arc, OnceLock};

pub type Row = BTreeMap<String, Value>;

/// Column names shared by every row of one query result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySchema {
    columns: Arc<[String]>,
}

impl QuerySchema {
    pub fn try_new(columns: impl IntoIterator<Item = String>) -> Result<Self> {
        let columns = columns.into_iter().collect::<Vec<_>>();
        let mut unique = std::collections::BTreeSet::new();
        if let Some(duplicate) = columns
            .iter()
            .find(|column| !unique.insert(column.as_str()))
        {
            return Err(SkeinError::Semantic(format!(
                "query result schema contains duplicate column {duplicate}"
            )));
        }
        Ok(Self {
            columns: Arc::from(columns),
        })
    }

    pub fn empty() -> Self {
        Self {
            columns: Arc::from([]),
        }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column == name)
    }
}

/// Schema-bearing result rows. Values are stored positionally; the legacy
/// map representation is constructed only when a compatibility caller asks
/// for it through the slice facade.
pub struct QueryRows {
    schema: QuerySchema,
    values: Vec<Vec<Value>>,
    compatibility_rows: OnceLock<Vec<Row>>,
}

impl QueryRows {
    pub fn empty() -> Self {
        Self::from_value_rows_unchecked(QuerySchema::empty(), Vec::new())
    }

    pub fn try_from_value_rows(schema: QuerySchema, values: Vec<Vec<Value>>) -> Result<Self> {
        if let Some((row, width)) = values
            .iter()
            .enumerate()
            .find_map(|(row, values)| (values.len() != schema.len()).then_some((row, values.len())))
        {
            return Err(SkeinError::Execution(format!(
                "query result row {row} has width {width}, expected {}",
                schema.len()
            )));
        }
        Ok(Self::from_value_rows_unchecked(schema, values))
    }

    fn from_value_rows_unchecked(schema: QuerySchema, values: Vec<Vec<Value>>) -> Self {
        Self {
            schema,
            values,
            compatibility_rows: OnceLock::new(),
        }
    }

    pub fn schema(&self) -> &QuerySchema {
        &self.schema
    }

    pub fn value_rows(&self) -> &[Vec<Value>] {
        &self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn value(&self, row: usize, column: usize) -> Option<ValueRef<'_>> {
        self.values.get(row)?.get(column).map(Value::as_ref)
    }

    pub fn get(&self, row: usize, column: &str) -> Option<ValueRef<'_>> {
        self.value(row, self.schema.position(column)?)
    }

    pub fn into_rows(self) -> Vec<Row> {
        let Self {
            schema,
            values,
            compatibility_rows,
        } = self;
        if let Some(rows) = compatibility_rows.into_inner() {
            return rows;
        }
        let columns = schema.columns;
        values
            .into_iter()
            .map(|values| columns.iter().cloned().zip(values).collect())
            .collect()
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&Row) -> bool) {
        let mut rows = self
            .compatibility_rows
            .take()
            .unwrap_or_else(|| materialize_rows(&self.schema, &self.values));
        rows.retain(|row| keep(row));
        *self = rows.into();
    }

    pub fn extend(&mut self, rows: impl IntoIterator<Item = Row>) {
        let mut compatibility = self
            .compatibility_rows
            .take()
            .unwrap_or_else(|| materialize_rows(&self.schema, &self.values));
        compatibility.extend(rows);
        *self = compatibility.into();
    }

    pub fn as_slice(&self) -> &[Row] {
        self.compatibility_rows()
    }

    pub fn sort(&mut self) {
        let mut rows = self
            .compatibility_rows
            .take()
            .unwrap_or_else(|| materialize_rows(&self.schema, &self.values));
        rows.sort();
        *self = rows.into();
    }

    pub fn payload_bytes(&self) -> usize {
        let names = self
            .schema
            .columns()
            .iter()
            .fold(0usize, |total, name| total.saturating_add(name.len()));
        self.values.iter().flatten().fold(names, |total, value| {
            total.saturating_add(value_payload_bytes(value))
        })
    }

    fn compatibility_rows(&self) -> &[Row] {
        self.compatibility_rows
            .get_or_init(|| materialize_rows(&self.schema, &self.values))
    }
}

impl Default for QueryRows {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<Row>> for QueryRows {
    fn from(rows: Vec<Row>) -> Self {
        let columns = rows
            .iter()
            .flat_map(|row| row.keys().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let schema = QuerySchema::try_new(columns).expect("BTreeSet columns are unique");
        let values = rows
            .into_iter()
            .map(|mut row| {
                schema
                    .columns()
                    .iter()
                    .map(|column| row.remove(column).unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        Self {
            schema,
            values,
            compatibility_rows: OnceLock::new(),
        }
    }
}

impl FromIterator<Row> for QueryRows {
    fn from_iter<T: IntoIterator<Item = Row>>(iter: T) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

impl Clone for QueryRows {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            values: self.values.clone(),
            compatibility_rows: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for QueryRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRows")
            .field("schema", &self.schema)
            .field("values", &self.values)
            .finish()
    }
}

impl PartialEq for QueryRows {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.values == other.values
    }
}

impl Eq for QueryRows {}

impl PartialEq<Vec<Row>> for QueryRows {
    fn eq(&self, other: &Vec<Row>) -> bool {
        self.compatibility_rows() == other
    }
}

impl PartialEq<QueryRows> for Vec<Row> {
    fn eq(&self, other: &QueryRows) -> bool {
        self == other.compatibility_rows()
    }
}

impl Deref for QueryRows {
    type Target = [Row];

    fn deref(&self) -> &Self::Target {
        self.compatibility_rows()
    }
}

impl<'a> IntoIterator for &'a QueryRows {
    type Item = &'a Row;
    type IntoIter = std::slice::Iter<'a, Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.compatibility_rows().iter()
    }
}

impl IntoIterator for QueryRows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_rows().into_iter()
    }
}

fn materialize_rows(schema: &QuerySchema, values: &[Vec<Value>]) -> Vec<Row> {
    values
        .iter()
        .map(|values| {
            schema
                .columns()
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .collect()
        })
        .collect()
}

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
    fn schema_bearing_rows_keep_names_once_and_materialize_maps_lazily() {
        let schema = QuerySchema::try_new(["payload".to_string(), "id".to_string()]).unwrap();
        let rows = QueryRows::try_from_value_rows(
            schema,
            vec![
                vec![Value::String("one".to_string()), Value::Int(1)],
                vec![Value::String("two".to_string()), Value::Int(2)],
            ],
        )
        .unwrap();

        assert!(rows.compatibility_rows.get().is_none());
        assert_eq!(rows.schema().columns(), ["payload", "id"]);
        assert_eq!(rows.get(1, "payload").unwrap().as_str(), Some("two"));
        assert_eq!(rows.value(0, 1), Some(ValueRef::Int(1)));
        assert_eq!(rows.payload_bytes(), "payload".len() + "id".len() + 6 + 16);
        assert!(rows.compatibility_rows.get().is_none());
        assert_eq!(rows.len(), 2);
        assert!(!rows.is_empty());
        assert!(rows.compatibility_rows.get().is_none());

        assert_eq!(rows[0]["payload"], Value::String("one".to_string()));
        assert!(rows.compatibility_rows.get().is_some());
    }

    #[test]
    fn schema_bearing_rows_reject_duplicate_columns_and_width_mismatch() {
        assert!(QuerySchema::try_new(["id".to_string(), "id".to_string()]).is_err());
        let schema = QuerySchema::try_new(["id".to_string()]).unwrap();
        assert!(QueryRows::try_from_value_rows(schema, vec![vec![]]).is_err());

        let left = QueryRows::try_from_value_rows(
            QuerySchema::try_new(["left".to_string(), "right".to_string()]).unwrap(),
            vec![vec![Value::Int(1), Value::Int(2)]],
        )
        .unwrap();
        let right = QueryRows::try_from_value_rows(
            QuerySchema::try_new(["right".to_string(), "left".to_string()]).unwrap(),
            vec![vec![Value::Int(1), Value::Int(2)]],
        )
        .unwrap();
        assert_ne!(left, right);
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
