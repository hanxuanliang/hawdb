use skein_core::Value;
use std::collections::BTreeMap;

pub type Row = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingOperatorMemoryReport {
    pub operator: String,
    pub budget_bytes: usize,
    pub peak_tracked_bytes: usize,
    pub input_rows: usize,
    pub spill_run_count: usize,
    pub spilled_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PipelineMemoryReport {
    /// Sum of rows emitted at every physical operator boundary.
    pub intermediate_rows: usize,
    /// Sum of retained payload estimates emitted at every physical operator boundary.
    pub intermediate_payload_bytes: usize,
    pub peak_batch_rows: usize,
    pub peak_batch_payload_bytes: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    /// Resident memory before execution, when process sampling is supported.
    pub start_resident_bytes: Option<u64>,
    /// Resident memory after the output rows have been materialized.
    pub steady_resident_bytes: Option<u64>,
    /// Process high-water resident memory at completion.
    pub peak_resident_bytes: Option<u64>,
    /// Page-fault deltas observed during execution and output materialization.
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
