//! Root query-report wiring for executor-owned operators.

use super::*;
use skein_executor::observer::ExecutionObserver;

pub(super) struct RootExecutionObserver;

impl ExecutionObserver for RootExecutionObserver {
    fn record_scan_pruning_report(&mut self, report: ScanPruningReport) {
        super::record_scan_pruning_report(report);
    }

    fn record_blocking_memory_report(
        &mut self,
        report: skein_executor::BlockingOperatorMemoryReport,
    ) {
        super::record_blocking_memory_report(report);
    }
}
