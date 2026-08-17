use skein_core::Value;
use std::collections::BTreeMap;

pub type Row = BTreeMap<String, Value>;

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
}
