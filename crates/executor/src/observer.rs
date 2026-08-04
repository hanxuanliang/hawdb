//! Execution-report observer contract for host integration.

use crate::BlockingOperatorMemoryReport;
use skein_storage::ScanPruningReport;

pub trait ExecutionObserver {
    fn record_scan_pruning_report(&mut self, _report: ScanPruningReport) {}

    fn record_blocking_memory_report(&mut self, _report: BlockingOperatorMemoryReport) {}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopExecutionObserver;

impl ExecutionObserver for NoopExecutionObserver {}
