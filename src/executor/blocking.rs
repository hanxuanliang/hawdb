//! Root facade wiring for storage-independent blocking operators.

use super::*;
use skein_executor::blocking::{
    self as executor_blocking, BindingBatchSource, BlockingExecutionContext,
};

pub(super) use executor_blocking::spill_binding_run;

struct RootBindingBatchSource<'a> {
    context: BatchReadContext<'a>,
}

impl<'a> BatchReadContext<'a> {
    fn blocking_context(self) -> BlockingExecutionContext<'a> {
        BlockingExecutionContext {
            catalog: self.catalog,
            memory: self.memory,
            memory_ledger: self.memory_ledger,
            task_context: self.task_context,
            observer: self.observer,
        }
    }
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
        context.blocking_context(),
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
        context.blocking_context(),
        execution_limit,
        emit,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stream_top_n_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    offset: usize,
    limit: usize,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RootBindingBatchSource { context };
    executor_blocking::stream_top_n_batches(
        input,
        items,
        offset,
        limit,
        &mut source,
        context.blocking_context(),
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
        context.blocking_context(),
        execution_limit,
        emit,
    )
}

pub(super) fn stream_cartesian_product_batches(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut tracker = OperatorMemoryTracker::with_account(
        context.memory.blocking_operator_bytes,
        context.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "NodeCartesianProductExec",
            context.memory.blocking_operator_bytes,
        ),
    );
    let mut spill_budget = SpillBudgetTracker::with_ledger(
        "NodeCartesianProductExec",
        context.memory,
        context.memory_ledger,
    );
    let mut right_bindings = Vec::new();
    let mut runs = Vec::new();
    let mut right_ordinal = 0u64;
    execute_binding_batches(right, context, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            let bytes = binding_memory_bytes(&binding);
            ensure_operator_item_fits("NodeCartesianProductExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                runs.push(spill_binding_run(
                    "cartesian",
                    &mut right_bindings,
                    &mut spill_budget,
                    context.task_context,
                )?);
                tracker.reset();
            }
            tracker.try_charge(bytes)?;
            right_bindings.push(binding);
            right_ordinal = right_ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;
    if !runs.is_empty() && !right_bindings.is_empty() {
        runs.push(spill_binding_run(
            "cartesian",
            &mut right_bindings,
            &mut spill_budget,
            context.task_context,
        )?);
        tracker.reset();
    }
    context
        .observer
        .record_blocking_memory_report(executor_blocking::spill_backed_report(
            "NodeCartesianProductExec",
            &tracker,
            tracker.peak_bytes,
            right_ordinal as usize,
            &spill_budget,
            if runs.is_empty() {
                0
            } else {
                right_ordinal as usize
            },
        ));
    if right_bindings.is_empty() && runs.is_empty() {
        return Ok(BatchControl::Continue);
    }

    let mut output = Vec::with_capacity(context.memory.batch_rows.get());
    let mut emitted = 0usize;
    let control =
        execute_binding_batches(left, context, ExecutionLimit::unlimited(), &mut |batch| {
            for left_binding in batch {
                if runs.is_empty() {
                    for right_binding in &right_bindings {
                        if push_cartesian_output(
                            &left_binding,
                            right_binding,
                            context.memory.batch_rows.get(),
                            execution_limit,
                            &mut output,
                            &mut emitted,
                            emit,
                        )? == BatchControl::Stop
                        {
                            return Ok(BatchControl::Stop);
                        }
                    }
                } else {
                    for run in &runs {
                        runtime_checkpoint(context.task_context)?;
                        let mut reader = run.reader()?;
                        while let Some((_, right_binding)) =
                            reader.read(context.memory.blocking_operator_bytes.get())?
                        {
                            runtime_checkpoint(context.task_context)?;
                            if push_cartesian_output(
                                &left_binding,
                                &right_binding,
                                context.memory.batch_rows.get(),
                                execution_limit,
                                &mut output,
                                &mut emitted,
                                emit,
                            )? == BatchControl::Stop
                            {
                                return Ok(BatchControl::Stop);
                            }
                        }
                    }
                    if execution_limit.is_reached(emitted) {
                        return Ok(BatchControl::Stop);
                    }
                }
            }
            Ok(BatchControl::Continue)
        })?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(control)
}

#[allow(clippy::too_many_arguments)]
fn push_cartesian_output(
    left: &Binding,
    right: &Binding,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    output: &mut BindingBatch,
    emitted: &mut usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut values = left.values.clone();
    values.extend(right.values.clone());
    let mut nodes = left.nodes.clone();
    nodes.extend(right.nodes.clone());
    let mut relationships = left.relationships.clone();
    relationships.extend(right.relationships.clone());
    output.push(Binding {
        values,
        nodes,
        relationships,
    });
    *emitted = (*emitted).saturating_add(1);
    if output.len() == batch_rows
        && emit(std::mem::replace(output, Vec::with_capacity(batch_rows)))? == BatchControl::Stop
    {
        return Ok(BatchControl::Stop);
    }
    Ok(if execution_limit.is_reached(*emitted) {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}
