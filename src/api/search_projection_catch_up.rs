use super::Database;
use crate::qos::{
    LocalQosPermit, LocalQosScheduler, QosAdmission, QosAdmissionCode, WorkClass, WorkRequest,
};
use crate::search::{SearchIndex, SearchProjectionFreshness};
use crate::{Result, SkeinError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionCatchUpReport {
    pub graph_commit_epoch: u64,
    pub start_applied_epoch: Option<u64>,
    pub start_durable_epoch: Option<u64>,
    pub end_applied_epoch: Option<u64>,
    pub end_durable_epoch: Option<u64>,
    pub applied_batch_count: usize,
    pub applied_operation_count: usize,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchProjectionCatchUpStopReason {
    CaughtUp,
    BatchBudgetExhausted,
    Deferred(QosAdmissionCode),
    Rejected(QosAdmissionCode),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledSearchProjectionCatchUpReport {
    pub catch_up: SearchProjectionCatchUpReport,
    pub stop_reason: SearchProjectionCatchUpStopReason,
}

impl Database {
    pub fn catch_up_search_projection(
        &self,
        search_index: &mut SearchIndex,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<SearchProjectionCatchUpReport> {
        validate_catch_up_request(search_index, max_operations_per_batch, max_batches)?;
        let start = search_index.projection_freshness();
        if start.has_uncheckpointed_changes {
            search_index.checkpoint()?;
        }
        let graph_commit_epoch = self.store.commit_epoch();
        let mut applied_batch_count = 0usize;
        let mut applied_operation_count = 0usize;
        while applied_batch_count < max_batches {
            let Some(request) = self.build_search_projection_graph_delta_request_from_freshness(
                search_index,
                Some(max_operations_per_batch),
            )?
            else {
                break;
            };
            let report = self.apply_search_projection_graph_delta(search_index, request)?;
            search_index.checkpoint()?;
            applied_batch_count = applied_batch_count.saturating_add(1);
            applied_operation_count =
                applied_operation_count.saturating_add(report.operation_count);
        }

        Ok(catch_up_report(
            graph_commit_epoch,
            start,
            search_index.projection_freshness(),
            applied_batch_count,
            applied_operation_count,
        ))
    }

    pub fn catch_up_search_projection_with_scheduler(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<ScheduledSearchProjectionCatchUpReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        validate_catch_up_request(search_index, max_operations_per_batch, max_batches)?;
        self.configure_qos_scheduler_telemetry(scheduler);

        let start = search_index.projection_freshness();
        let graph_commit_epoch = self.store.commit_epoch();
        let mut applied_batch_count = 0usize;
        let mut applied_operation_count = 0usize;

        if start.has_uncheckpointed_changes {
            let permit = match start_background_work(
                scheduler,
                WorkRequest::background(WorkClass::Projection, max_operations_per_batch),
            ) {
                Ok(permit) => permit,
                Err(stop_reason) => {
                    return Ok(scheduled_report(
                        graph_commit_epoch,
                        start,
                        search_index.projection_freshness(),
                        applied_batch_count,
                        applied_operation_count,
                        stop_reason,
                    ));
                }
            };
            let checkpoint = search_index.checkpoint();
            scheduler.finish_with_outcome(permit, checkpoint.is_ok());
            checkpoint?;
        }

        while applied_batch_count < max_batches {
            let Some(request) = self.build_search_projection_graph_delta_request_from_freshness(
                search_index,
                Some(max_operations_per_batch),
            )?
            else {
                return Ok(scheduled_report(
                    graph_commit_epoch,
                    start,
                    search_index.projection_freshness(),
                    applied_batch_count,
                    applied_operation_count,
                    SearchProjectionCatchUpStopReason::CaughtUp,
                ));
            };
            let permit = match start_background_work(scheduler, request.background_work_request()) {
                Ok(permit) => permit,
                Err(stop_reason) => {
                    return Ok(scheduled_report(
                        graph_commit_epoch,
                        start,
                        search_index.projection_freshness(),
                        applied_batch_count,
                        applied_operation_count,
                        stop_reason,
                    ));
                }
            };
            let result = self
                .apply_search_projection_graph_delta(search_index, request)
                .and_then(|report| {
                    search_index.checkpoint()?;
                    Ok(report)
                });
            scheduler.finish_with_outcome(permit, result.is_ok());
            let report = result?;
            applied_batch_count = applied_batch_count.saturating_add(1);
            applied_operation_count =
                applied_operation_count.saturating_add(report.operation_count);
        }

        let end = search_index.projection_freshness();
        let stop_reason = if durable_epoch(&end) == graph_commit_epoch {
            SearchProjectionCatchUpStopReason::CaughtUp
        } else {
            SearchProjectionCatchUpStopReason::BatchBudgetExhausted
        };
        Ok(scheduled_report(
            graph_commit_epoch,
            start,
            end,
            applied_batch_count,
            applied_operation_count,
            stop_reason,
        ))
    }
}

fn validate_catch_up_request(
    search_index: &SearchIndex,
    max_operations_per_batch: usize,
    max_batches: usize,
) -> Result<()> {
    if !search_index.is_persistent() {
        return Err(SkeinError::Storage(
            "durable search projection catch-up requires a persistent search index".to_string(),
        ));
    }
    if max_operations_per_batch == 0 {
        return Err(SkeinError::Semantic(
            "search projection catch-up max_operations_per_batch must be greater than zero"
                .to_string(),
        ));
    }
    if max_batches == 0 {
        return Err(SkeinError::Semantic(
            "search projection catch-up max_batches must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn start_background_work(
    scheduler: &mut LocalQosScheduler,
    request: WorkRequest,
) -> std::result::Result<LocalQosPermit, SearchProjectionCatchUpStopReason> {
    scheduler
        .try_start(request)
        .map_err(|admission| match admission {
            QosAdmission::Defer { code, .. } => SearchProjectionCatchUpStopReason::Deferred(code),
            QosAdmission::Reject { code, .. } => SearchProjectionCatchUpStopReason::Rejected(code),
            QosAdmission::Admit => unreachable!("admitted work returns a permit"),
        })
}

fn scheduled_report(
    graph_commit_epoch: u64,
    start: SearchProjectionFreshness,
    end: SearchProjectionFreshness,
    applied_batch_count: usize,
    applied_operation_count: usize,
    stop_reason: SearchProjectionCatchUpStopReason,
) -> ScheduledSearchProjectionCatchUpReport {
    ScheduledSearchProjectionCatchUpReport {
        catch_up: catch_up_report(
            graph_commit_epoch,
            start,
            end,
            applied_batch_count,
            applied_operation_count,
        ),
        stop_reason,
    }
}

fn catch_up_report(
    graph_commit_epoch: u64,
    start: SearchProjectionFreshness,
    end: SearchProjectionFreshness,
    applied_batch_count: usize,
    applied_operation_count: usize,
) -> SearchProjectionCatchUpReport {
    SearchProjectionCatchUpReport {
        graph_commit_epoch,
        start_applied_epoch: start.source_graph_commit_epoch,
        start_durable_epoch: start.durable_source_graph_commit_epoch,
        end_applied_epoch: end.source_graph_commit_epoch,
        end_durable_epoch: end.durable_source_graph_commit_epoch,
        applied_batch_count,
        applied_operation_count,
        complete: durable_epoch(&end) == graph_commit_epoch,
    }
}

fn durable_epoch(freshness: &SearchProjectionFreshness) -> u64 {
    freshness.durable_source_graph_commit_epoch.unwrap_or(0)
}
