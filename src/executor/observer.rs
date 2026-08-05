//! Root query profiling and observer wiring.

use super::*;
use skein_executor::observer::ExecutionObserver;
use std::cell::RefCell;
use std::collections::BTreeSet;

#[derive(Default)]
pub(super) struct QueryExecutionReports {
    pub(super) scan_pruning: Vec<ScanPruningReport>,
    pub(super) vector_execution: Vec<skein_executor::VectorExecutionReport>,
    pub(super) graph_expansion: Vec<skein_executor::GraphExpansionExecutionReport>,
    pub(super) blocking_memory: Vec<skein_executor::BlockingOperatorMemoryReport>,
    pub(super) pipeline_memory: skein_executor::PipelineMemoryReport,
}

#[derive(Default)]
pub(super) struct QueryExecutionObserver {
    reports: RefCell<QueryExecutionReports>,
}

impl QueryExecutionObserver {
    pub(super) fn into_reports(self) -> QueryExecutionReports {
        self.reports.into_inner()
    }

    pub(super) fn record_vector_execution(&self, report: skein_executor::VectorExecutionReport) {
        self.reports.borrow_mut().vector_execution.push(report);
    }
    pub(super) fn record_graph_expansion(
        &self,
        report: skein_executor::GraphExpansionExecutionReport,
    ) {
        self.reports.borrow_mut().graph_expansion.push(report);
    }

    pub(super) fn record_blocking_memory_report(
        &self,
        report: skein_executor::BlockingOperatorMemoryReport,
    ) {
        self.reports.borrow_mut().blocking_memory.push(report);
    }

    pub(super) fn record_pipeline_batch(&self, batch: &[Binding]) {
        let payload_bytes = batch.iter().fold(0usize, |total, binding| {
            total.saturating_add(skein_executor::binding::binding_payload_bytes(binding))
        });
        let mut reports = self.reports.borrow_mut();
        let report = &mut reports.pipeline_memory;
        report.intermediate_rows = report.intermediate_rows.saturating_add(batch.len());
        report.intermediate_payload_bytes = report
            .intermediate_payload_bytes
            .saturating_add(payload_bytes);
        report.peak_batch_rows = report.peak_batch_rows.max(batch.len());
        report.peak_batch_payload_bytes = report.peak_batch_payload_bytes.max(payload_bytes);
    }

    pub(super) fn record_columnar_batch(&self, input_rows: usize, selected_rows: usize) {
        let mut reports = self.reports.borrow_mut();
        let report = &mut reports.pipeline_memory;
        report.columnar_batches = report.columnar_batches.saturating_add(1);
        report.columnar_input_rows = report.columnar_input_rows.saturating_add(input_rows);
        report.columnar_selected_rows = report.columnar_selected_rows.saturating_add(selected_rows);
    }

    pub(super) fn record_morsels(&self, count: usize) {
        let mut reports = self.reports.borrow_mut();
        let report = &mut reports.pipeline_memory;
        report.morsel_count = report.morsel_count.saturating_add(count);
    }

    pub(super) fn record_morsel_admission(&self, max_workers: usize, active_workers: usize) {
        let mut reports = self.reports.borrow_mut();
        let report = &mut reports.pipeline_memory;
        report.morsel_max_admitted_workers = report.morsel_max_admitted_workers.max(max_workers);
        report.morsel_peak_active_workers = report.morsel_peak_active_workers.max(active_workers);
    }

    pub(super) fn current_vector_rerank_count(&self) -> usize {
        self.reports
            .borrow()
            .vector_execution
            .last()
            .map_or(0, |report| report.reranked_candidate_count)
    }
}

impl ExecutionObserver for QueryExecutionObserver {
    fn record_scan_pruning_report(&self, report: ScanPruningReport) {
        self.reports.borrow_mut().scan_pruning.push(report);
    }

    fn record_blocking_memory_report(&self, report: skein_executor::BlockingOperatorMemoryReport) {
        QueryExecutionObserver::record_blocking_memory_report(self, report);
    }
}

pub(super) fn blocking_operator_kinds(plan: &PhysicalPlan) -> Vec<String> {
    let mut output = BTreeSet::new();
    collect_blocking_operator_kinds(plan, &mut output);
    output.into_iter().collect()
}

fn collect_blocking_operator_kinds(plan: &PhysicalPlan, output: &mut BTreeSet<String>) {
    match plan {
        PhysicalPlan::GraphAlgorithm { .. } | PhysicalPlan::VectorSeedScan { .. } => {
            output.insert(plan.kind().as_str().to_string());
        }
        PhysicalPlan::ShortestPathExec { .. } => {
            output.insert("ShortestPathExec".to_string());
        }
        PhysicalPlan::AggregateExec { input, .. } => {
            output.insert("AggregateExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::DistinctExec { input } => {
            output.insert("DistinctExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::SortExec { input, .. } => {
            output.insert("SortExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::TopNExec { input, .. } => {
            output.insert("TopNExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            output.insert("NodeCartesianProductExec".to_string());
            collect_blocking_operator_kinds(left, output);
            collect_blocking_operator_kinds(right, output);
        }
        PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::AdjacencyExpandExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => {
            collect_blocking_operator_kinds(input, output);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_observers_keep_reports_isolated() {
        let first = QueryExecutionObserver::default();
        let second = QueryExecutionObserver::default();
        first.record_morsel_admission(2, 1);
        second.record_morsel_admission(4, 3);

        assert_eq!(
            first
                .into_reports()
                .pipeline_memory
                .morsel_max_admitted_workers,
            2
        );
        assert_eq!(
            second
                .into_reports()
                .pipeline_memory
                .morsel_max_admitted_workers,
            4
        );
    }
}
