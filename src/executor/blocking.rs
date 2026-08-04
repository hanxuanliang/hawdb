//! Root facade wiring for storage-independent blocking operators.

use super::*;
use skein_executor::blocking::{
    self as executor_blocking, BindingBatchSource, BlockingExecutionContext,
};

pub(super) use executor_blocking::{
    compact_sort_runs, merge_sort_runs, spill_binding_run, spill_top_n_run,
};

struct RootBindingBatchSource<'a> {
    context: BatchReadContext<'a>,
}

impl BindingBatchSource for RootBindingBatchSource<'_> {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        execute_binding_batches(input, self.context, execution_limit, emit)
    }
}

pub(super) fn stream_distinct_batches(
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RootBindingBatchSource { context };
    executor_blocking::stream_distinct_batches(
        input,
        &mut source,
        BlockingExecutionContext {
            catalog: context.catalog,
            memory: context.memory,
            task_context: context.task_context,
            observer: &mut RootExecutionObserver,
        },
        execution_limit,
        emit,
    )
}

pub(super) fn stream_sort_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RootBindingBatchSource { context };
    executor_blocking::stream_sort_batches(
        input,
        items,
        &mut source,
        BlockingExecutionContext {
            catalog: context.catalog,
            memory: context.memory,
            task_context: context.task_context,
            observer: &mut RootExecutionObserver,
        },
        execution_limit,
        emit,
    )
}

pub(super) fn stream_aggregate_batches(
    input: &PhysicalPlan,
    group_keys: &[Projection],
    items: &[Aggregation],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RootBindingBatchSource { context };
    executor_blocking::stream_aggregate_batches(
        input,
        group_keys,
        items,
        &mut source,
        BlockingExecutionContext {
            catalog: context.catalog,
            memory: context.memory,
            task_context: context.task_context,
            observer: &mut RootExecutionObserver,
        },
        execution_limit,
        emit,
    )
}
