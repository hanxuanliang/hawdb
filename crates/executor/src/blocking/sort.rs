use super::*;

struct SortRunRow {
    sort_values: Vec<(Value, SortDirection)>,
    ordinal: u64,
    binding: Binding,
}

impl SortRunRow {
    fn new(catalog: &Catalog, items: &[SortItem], ordinal: u64, binding: Binding) -> Self {
        let sort_values = items
            .iter()
            .map(|item| (sort_value(catalog, &binding, &item.key), item.direction))
            .collect();
        Self {
            sort_values,
            ordinal,
            binding,
        }
    }

    fn cmp_key(&self, other: &Self) -> Ordering {
        compare_sort_values(&self.sort_values, &other.sort_values)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }

    fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding).saturating_add(self.sort_values.iter().fold(
            std::mem::size_of::<Vec<(Value, SortDirection)>>(),
            |total, (value, _)| total.saturating_add(value_memory_bytes(value)),
        ))
    }
}

fn compare_sort_values(
    left: &[(Value, SortDirection)],
    right: &[(Value, SortDirection)],
) -> Ordering {
    for ((left, direction), (right, other_direction)) in left.iter().zip(right) {
        debug_assert_eq!(direction, other_direction);
        let ordering = match direction {
            SortDirection::Asc => left.cmp(right),
            SortDirection::Desc => left.cmp(right).reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

struct SortMergeEntry {
    row: SortRunRow,
    run_index: usize,
}

impl PartialEq for SortMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.cmp_key(&other.row) == Ordering::Equal && self.run_index == other.run_index
    }
}

impl Eq for SortMergeEntry {}

impl Ord for SortMergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for SortMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn stream_sort_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BlockingExecutionContext {
        catalog,
        memory,
        task_context,
        observer,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("SortExec", memory);
    let mut rows = Vec::<SortRunRow>::new();
    let mut runs = Vec::<spill::SpillRun>::new();
    let mut ordinal = 0u64;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(task_context)?;
        for binding in batch {
            let row = SortRunRow::new(catalog, items, ordinal, binding);
            let bytes = row.memory_bytes();
            ensure_operator_item_fits("SortExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                runs.push(spill_sort_run(
                    &mut rows,
                    &memory.spill_directory,
                    &mut spill_budget,
                    task_context,
                )?);
                tracker.reset();
            }
            tracker.charge(bytes);
            rows.push(row);
            ordinal = ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;

    if runs.is_empty() {
        runtime_checkpoint(task_context)?;
        observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
            operator: "SortExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows: ordinal as usize,
            max_spill_bytes: memory.max_spill_bytes.get(),
            max_spill_runs: memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        rows.sort_by(SortRunRow::cmp_key);
        return emit_binding_iterator(
            rows.into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .map(|row| row.binding),
            memory.batch_rows.get(),
            emit,
        );
    }
    if !rows.is_empty() {
        runs.push(spill_sort_run(
            &mut rows,
            &memory.spill_directory,
            &mut spill_budget,
            task_context,
        )?);
    }
    runs = compact_sort_runs(
        runs,
        items,
        catalog,
        memory,
        &mut spill_budget,
        task_context,
    )?;
    observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
        operator: "SortExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes: tracker.peak_bytes,
        input_rows: ordinal as usize,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows: ordinal as usize,
    });
    merge_sort_runs(
        &runs,
        items,
        catalog,
        memory.blocking_operator_bytes,
        memory.batch_rows.get(),
        0,
        execution_limit.output_rows.unwrap_or(usize::MAX),
        task_context,
        emit,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn stream_top_n_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    offset: usize,
    limit: usize,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let retained = offset.saturating_add(limit);
    if retained == 0 {
        return Ok(BatchControl::Continue);
    }
    let BlockingExecutionContext {
        catalog,
        memory,
        task_context,
        observer,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("TopNExec", memory);
    let mut runs = Vec::<spill::SpillRun>::new();
    let mut heap = BinaryHeap::new();
    let mut ordinal = 0u64;
    let mut spilled_rows = 0usize;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(task_context)?;
        for binding in batch {
            let sort_values = items
                .iter()
                .map(|item| (sort_value(catalog, &binding, &item.key), item.direction))
                .collect();
            let candidate = TopNBinding {
                sort_values,
                ordinal,
                binding,
            };
            ordinal = ordinal.saturating_add(1);
            let bytes = candidate.memory_bytes();
            ensure_operator_item_fits("TopNExec", bytes, &tracker)?;
            if heap.len() < retained {
                if tracker.would_exceed(bytes) {
                    spilled_rows = spilled_rows.saturating_add(heap.len());
                    runs.push(spill_top_n_run(
                        &mut heap,
                        &memory.spill_directory,
                        &mut spill_budget,
                        task_context,
                    )?);
                    tracker.reset();
                }
                tracker.charge(bytes);
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                let worst_bytes = heap.peek().map(TopNBinding::memory_bytes).unwrap_or(0);
                if tracker
                    .used_bytes
                    .saturating_sub(worst_bytes)
                    .saturating_add(bytes)
                    > tracker.budget_bytes
                {
                    spilled_rows = spilled_rows.saturating_add(heap.len());
                    runs.push(spill_top_n_run(
                        &mut heap,
                        &memory.spill_directory,
                        &mut spill_budget,
                        task_context,
                    )?);
                    tracker.reset();
                } else {
                    heap.pop();
                    tracker.release(worst_bytes);
                }
                tracker.charge(bytes);
                heap.push(candidate);
            }
        }
        Ok(BatchControl::Continue)
    })?;

    if !runs.is_empty() {
        if !heap.is_empty() {
            spilled_rows = spilled_rows.saturating_add(heap.len());
            runs.push(spill_top_n_run(
                &mut heap,
                &memory.spill_directory,
                &mut spill_budget,
                task_context,
            )?);
        }
        runs = compact_sort_runs(
            runs,
            items,
            catalog,
            memory,
            &mut spill_budget,
            task_context,
        )?;
        observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
            operator: "TopNExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows: ordinal as usize,
            max_spill_bytes: spill_budget.max_bytes,
            max_spill_runs: spill_budget.max_runs,
            spilled_bytes: spill_budget.used_bytes,
            spill_run_count: spill_budget.run_count,
            spilled_rows,
        });
        return merge_sort_runs(
            &runs,
            items,
            catalog,
            memory.blocking_operator_bytes,
            memory.batch_rows.get(),
            offset,
            limit.min(execution_limit.output_rows.unwrap_or(usize::MAX)),
            task_context,
            emit,
        );
    }
    observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
        operator: "TopNExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes: tracker.peak_bytes,
        input_rows: ordinal as usize,
        max_spill_bytes: memory.max_spill_bytes.get(),
        max_spill_runs: memory.max_spill_runs.get(),
        spilled_bytes: 0,
        spill_run_count: 0,
        spilled_rows: 0,
    });
    let mut selected = heap.into_vec();
    selected.sort();
    let bindings = selected
        .into_iter()
        .skip(offset)
        .take(limit)
        .take(execution_limit.output_rows.unwrap_or(usize::MAX))
        .map(|entry| entry.binding);
    emit_binding_iterator(bindings, memory.batch_rows.get(), emit)
}

fn spill_sort_run(
    rows: &mut Vec<SortRunRow>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    rows.sort_by(SortRunRow::cmp_key);
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "sort")?;
    for row in rows.drain(..) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(row.ordinal, &row.binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

pub fn spill_top_n_run(
    heap: &mut BinaryHeap<TopNBinding>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut rows = std::mem::take(heap).into_vec();
    rows.sort();
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "topn")?;
    for row in rows {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(row.ordinal, &row.binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

pub fn compact_sort_runs(
    mut runs: Vec<spill::SpillRun>,
    items: &[SortItem],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<spill::SpillRun>> {
    while runs.len() > 2 {
        runtime_checkpoint(task_context)?;
        let mut compacted = Vec::with_capacity(runs.len().div_ceil(2));
        let mut pending = runs.into_iter();
        while let Some(left) = pending.next() {
            let Some(right) = pending.next() else {
                compacted.push(left);
                break;
            };
            compacted.push(merge_sort_run_pair(
                &left,
                &right,
                items,
                catalog,
                memory,
                spill_budget,
                task_context,
            )?);
        }
        runs = compacted;
    }
    Ok(runs)
}

#[allow(clippy::too_many_arguments)]
fn merge_sort_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    items: &[SortItem],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut readers = [left.reader()?, right.reader()?];
    let mut heap = BinaryHeap::new();
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let per_row_budget = memory.blocking_operator_bytes.get() / 2;
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some((ordinal, binding)) = reader.read(memory.blocking_operator_bytes.get())? {
            let entry = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            if bytes > per_row_budget {
                return Err(SkeinError::Execution(format!(
                    "SortExec spill merge row uses {bytes} bytes, exceeding half of blocking_operator_bytes {}",
                    memory.blocking_operator_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(entry);
        }
    }
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(&memory.spill_directory, "sort-merge")?;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        tracker.release(entry.row.memory_bytes());
        let run_index = entry.run_index;
        let bytes = writer.write(
            entry.row.ordinal,
            &entry.row.binding,
            spill_budget.remaining_bytes(),
        )?;
        spill_budget.charge(bytes)?;
        if let Some((ordinal, binding)) =
            readers[run_index].read(memory.blocking_operator_bytes.get())?
        {
            let next = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = next.row.memory_bytes();
            if bytes > per_row_budget || tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "SortExec spill merge exceeds blocking_operator_bytes {}",
                    memory.blocking_operator_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(next);
        }
    }
    writer.finish()?;
    Ok(run)
}

#[allow(clippy::too_many_arguments)]
pub fn merge_sort_runs(
    runs: &[spill::SpillRun],
    items: &[SortItem],
    catalog: &Catalog,
    memory_budget: NonZeroUsize,
    batch_rows: usize,
    skip_rows: usize,
    output_rows: usize,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(task_context)?;
    let mut readers = runs
        .iter()
        .map(spill::SpillRun::reader)
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for (run_index, reader) in readers.iter_mut().enumerate() {
        runtime_checkpoint(task_context)?;
        if let Some((ordinal, binding)) = reader.read(memory_budget.get())? {
            let entry = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            ensure_operator_item_fits("SortExec merge", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "SortExec merge fan-in uses more than blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(entry);
        }
    }
    if output_rows == 0 {
        return Ok(BatchControl::Continue);
    }
    let mut skipped = 0usize;
    let mut emitted = 0usize;
    let mut batch = Vec::with_capacity(batch_rows);
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        tracker.release(entry.row.memory_bytes());
        let run_index = entry.run_index;
        if let Some((ordinal, binding)) = readers[run_index].read(memory_budget.get())? {
            let next = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = next.row.memory_bytes();
            ensure_operator_item_fits("SortExec merge", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "SortExec merge fan-in uses more than blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(next);
        }
        if skipped < skip_rows {
            skipped = skipped.saturating_add(1);
            continue;
        }
        batch.push(entry.row.binding);
        emitted = emitted.saturating_add(1);
        if (batch.len() == batch_rows || emitted == output_rows)
            && emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            ))? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if emitted == output_rows {
            return Ok(BatchControl::Stop);
        }
    }
    runtime_checkpoint(task_context)?;
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}
