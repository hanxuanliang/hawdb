//! Root query profiling and observer wiring.

use super::*;
use skein_executor::observer::ExecutionObserver;
use std::cell::RefCell;
use std::collections::BTreeSet;

thread_local! {
    static SCAN_PRUNING_REPORT_CAPTURE: RefCell<Option<Vec<ScanPruningReport>>> = const { RefCell::new(None) };
    static VECTOR_EXECUTION_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::VectorExecutionReport>>> = const { RefCell::new(None) };
    static GRAPH_EXPANSION_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::GraphExpansionExecutionReport>>> = const { RefCell::new(None) };
    static BLOCKING_MEMORY_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::BlockingOperatorMemoryReport>>> = const { RefCell::new(None) };
    static PIPELINE_MEMORY_REPORT_CAPTURE: RefCell<Option<skein_executor::PipelineMemoryReport>> = const { RefCell::new(None) };
}

pub(super) struct RootExecutionObserver;

impl ExecutionObserver for RootExecutionObserver {
    fn record_scan_pruning_report(&mut self, report: ScanPruningReport) {
        self::record_scan_pruning_report(report);
    }

    fn record_blocking_memory_report(
        &mut self,
        report: skein_executor::BlockingOperatorMemoryReport,
    ) {
        self::record_blocking_memory_report(report);
    }
}

pub(super) fn capture_scan_pruning_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<ScanPruningReport>)> {
    SCAN_PRUNING_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

pub(super) fn record_scan_pruning_report(report: ScanPruningReport) {
    SCAN_PRUNING_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

pub(super) fn capture_vector_execution_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<skein_executor::VectorExecutionReport>)> {
    VECTOR_EXECUTION_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

pub(super) fn record_vector_execution_report(report: skein_executor::VectorExecutionReport) {
    VECTOR_EXECUTION_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

pub(super) fn capture_graph_expansion_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<skein_executor::GraphExpansionExecutionReport>)> {
    GRAPH_EXPANSION_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

pub(super) fn record_graph_expansion_report(report: skein_executor::GraphExpansionExecutionReport) {
    GRAPH_EXPANSION_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

pub(super) fn capture_pipeline_memory_report<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, skein_executor::PipelineMemoryReport)> {
    PIPELINE_MEMORY_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(skein_executor::PipelineMemoryReport::default()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

pub(super) fn record_pipeline_batch(batch: &[Binding]) {
    PIPELINE_MEMORY_REPORT_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(report) = capture.as_mut() else {
            return;
        };
        let payload_bytes = batch.iter().fold(0usize, |total, binding| {
            total.saturating_add(skein_executor::binding::binding_payload_bytes(binding))
        });
        report.intermediate_rows = report.intermediate_rows.saturating_add(batch.len());
        report.intermediate_payload_bytes = report
            .intermediate_payload_bytes
            .saturating_add(payload_bytes);
        report.peak_batch_rows = report.peak_batch_rows.max(batch.len());
        report.peak_batch_payload_bytes = report.peak_batch_payload_bytes.max(payload_bytes);
    });
}

pub(super) fn record_columnar_batch(input_rows: usize, selected_rows: usize) {
    PIPELINE_MEMORY_REPORT_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(report) = capture.as_mut() else {
            return;
        };
        report.columnar_batches = report.columnar_batches.saturating_add(1);
        report.columnar_input_rows = report.columnar_input_rows.saturating_add(input_rows);
        report.columnar_selected_rows = report.columnar_selected_rows.saturating_add(selected_rows);
        report.morsel_count = report.morsel_count.saturating_add(1);
    });
}

pub(super) fn record_morsel_admission(max_workers: usize, active_workers: usize) {
    PIPELINE_MEMORY_REPORT_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(report) = capture.as_mut() else {
            return;
        };
        report.morsel_max_admitted_workers = report.morsel_max_admitted_workers.max(max_workers);
        report.morsel_peak_active_workers = report.morsel_peak_active_workers.max(active_workers);
    });
}

pub(super) fn capture_blocking_memory_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<skein_executor::BlockingOperatorMemoryReport>)> {
    BLOCKING_MEMORY_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

pub(super) fn record_blocking_memory_report(report: skein_executor::BlockingOperatorMemoryReport) {
    BLOCKING_MEMORY_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

pub(super) fn current_vector_rerank_count() -> usize {
    VECTOR_EXECUTION_REPORT_CAPTURE.with(|capture| {
        capture
            .borrow()
            .as_ref()
            .and_then(|reports| reports.last())
            .map(|report| report.reranked_candidate_count)
            .unwrap_or_default()
    })
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
