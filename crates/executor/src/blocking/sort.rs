use super::*;

struct SortRunRow {
    sort_values: Vec<(Value, SortDirection)>,
    ordinal: u64,
    binding: Binding,
}

struct SortOperator<'plan, 'runtime> {
    items: &'plan [SortItem],
    catalog: &'runtime Catalog,
    memory: &'runtime ExecutionMemoryConfig,
    task_context: Option<&'runtime RuntimeTaskContext>,
    observer: &'runtime dyn ExecutionObserver,
    tracker: OperatorMemoryTracker,
    spill_budget: SpillBudgetTracker,
    rows: Vec<SortRunRow>,
    runs: Vec<spill::SpillRun>,
    input_rows: u64,
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
    let mut operator = SortOperator::new(items, context);
    runtime_checkpoint(operator.task_context)?;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(operator.task_context)?;
        for binding in batch {
            operator.push(binding)?;
        }
        Ok(BatchControl::Continue)
    })?;
    operator.finish(execution_limit, emit)
}

impl<'plan, 'runtime> SortOperator<'plan, 'runtime> {
    fn new(items: &'plan [SortItem], context: BlockingExecutionContext<'runtime>) -> Self {
        Self {
            items,
            catalog: context.catalog,
            memory: context.memory,
            task_context: context.task_context,
            observer: context.observer,
            tracker: OperatorMemoryTracker::new(context.memory.blocking_operator_bytes),
            spill_budget: SpillBudgetTracker::new("SortExec", context.memory),
            rows: Vec::new(),
            runs: Vec::new(),
            input_rows: 0,
        }
    }

    fn push(&mut self, binding: Binding) -> Result<()> {
        let row = SortRunRow::new(self.catalog, self.items, self.input_rows, binding);
        let bytes = row.memory_bytes();
        ensure_operator_item_fits("SortExec", bytes, &self.tracker)?;
        if self.tracker.would_exceed(bytes) {
            self.runs.push(spill_sort_run(
                &mut self.rows,
                &self.memory.spill_directory,
                &mut self.spill_budget,
                self.task_context,
            )?);
            self.tracker.reset();
        }
        self.tracker.charge(bytes);
        self.rows.push(row);
        self.input_rows = self.input_rows.saturating_add(1);
        Ok(())
    }

    fn finish(
        mut self,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.runs.is_empty() {
            runtime_checkpoint(self.task_context)?;
            self.record_memory_report(0);
            self.rows.sort_by(SortRunRow::cmp_key);
            return emit_binding_iterator(
                self.rows
                    .into_iter()
                    .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                    .map(|row| row.binding),
                self.memory.batch_rows.get(),
                emit,
            );
        }
        if !self.rows.is_empty() {
            self.runs.push(spill_sort_run(
                &mut self.rows,
                &self.memory.spill_directory,
                &mut self.spill_budget,
                self.task_context,
            )?);
        }
        self.runs = compact_sort_runs(
            self.runs,
            self.items,
            self.catalog,
            self.memory,
            &mut self.spill_budget,
            self.task_context,
        )?;
        self.record_memory_report(self.input_rows as usize);
        merge_sort_runs(
            &self.runs,
            self.items,
            self.catalog,
            self.memory.blocking_operator_bytes,
            self.memory.batch_rows.get(),
            0,
            execution_limit.output_rows.unwrap_or(usize::MAX),
            self.task_context,
            emit,
        )
    }

    fn record_memory_report(&self, spilled_rows: usize) {
        self.observer
            .record_blocking_memory_report(spill_backed_report(
                "SortExec",
                &self.tracker,
                self.tracker.peak_bytes,
                self.input_rows as usize,
                &self.spill_budget,
                spilled_rows,
            ));
    }
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
    let mut operator = TopNOperator::new(items, offset, limit, context);
    runtime_checkpoint(operator.task_context)?;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(operator.task_context)?;
        for binding in batch {
            operator.push(binding)?;
        }
        Ok(BatchControl::Continue)
    })?;
    operator.finish(execution_limit, emit)
}

struct TopNOperator<'plan, 'runtime> {
    items: &'plan [SortItem],
    offset: usize,
    limit: usize,
    retained: usize,
    catalog: &'runtime Catalog,
    memory: &'runtime ExecutionMemoryConfig,
    task_context: Option<&'runtime RuntimeTaskContext>,
    observer: &'runtime dyn ExecutionObserver,
    tracker: OperatorMemoryTracker,
    spill_budget: SpillBudgetTracker,
    runs: Vec<spill::SpillRun>,
    heap: BinaryHeap<TopNBinding>,
    input_rows: u64,
    spilled_rows: usize,
}

impl<'plan, 'runtime> TopNOperator<'plan, 'runtime> {
    fn new(
        items: &'plan [SortItem],
        offset: usize,
        limit: usize,
        context: BlockingExecutionContext<'runtime>,
    ) -> Self {
        Self {
            items,
            offset,
            limit,
            retained: offset.saturating_add(limit),
            catalog: context.catalog,
            memory: context.memory,
            task_context: context.task_context,
            observer: context.observer,
            tracker: OperatorMemoryTracker::new(context.memory.blocking_operator_bytes),
            spill_budget: SpillBudgetTracker::new("TopNExec", context.memory),
            runs: Vec::new(),
            heap: BinaryHeap::new(),
            input_rows: 0,
            spilled_rows: 0,
        }
    }

    fn push(&mut self, binding: Binding) -> Result<()> {
        let sort_values = self
            .items
            .iter()
            .map(|item| {
                (
                    sort_value(self.catalog, &binding, &item.key),
                    item.direction,
                )
            })
            .collect();
        let candidate = TopNBinding {
            sort_values,
            ordinal: self.input_rows,
            binding,
        };
        self.input_rows = self.input_rows.saturating_add(1);
        let bytes = candidate.memory_bytes();
        ensure_operator_item_fits("TopNExec", bytes, &self.tracker)?;
        if self.heap.len() < self.retained {
            if self.tracker.would_exceed(bytes) {
                self.spill_heap()?;
            }
            self.tracker.charge(bytes);
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| candidate < *worst) {
            let worst_bytes = self.heap.peek().map(TopNBinding::memory_bytes).unwrap_or(0);
            if self
                .tracker
                .used_bytes
                .saturating_sub(worst_bytes)
                .saturating_add(bytes)
                > self.tracker.budget_bytes
            {
                self.spill_heap()?;
            } else {
                self.heap.pop();
                self.tracker.release(worst_bytes);
            }
            self.tracker.charge(bytes);
            self.heap.push(candidate);
        }
        Ok(())
    }

    fn spill_heap(&mut self) -> Result<()> {
        self.spilled_rows = self.spilled_rows.saturating_add(self.heap.len());
        self.runs.push(spill_top_n_run(
            &mut self.heap,
            &self.memory.spill_directory,
            &mut self.spill_budget,
            self.task_context,
        )?);
        self.tracker.reset();
        Ok(())
    }

    fn finish(
        mut self,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if !self.runs.is_empty() {
            if !self.heap.is_empty() {
                self.spill_heap()?;
            }
            self.runs = compact_sort_runs(
                self.runs,
                self.items,
                self.catalog,
                self.memory,
                &mut self.spill_budget,
                self.task_context,
            )?;
            self.record_memory_report();
            return merge_sort_runs(
                &self.runs,
                self.items,
                self.catalog,
                self.memory.blocking_operator_bytes,
                self.memory.batch_rows.get(),
                self.offset,
                self.limit
                    .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                self.task_context,
                emit,
            );
        }
        self.record_memory_report();
        let mut selected = self.heap.into_vec();
        selected.sort();
        let bindings = selected
            .into_iter()
            .skip(self.offset)
            .take(self.limit)
            .take(execution_limit.output_rows.unwrap_or(usize::MAX))
            .map(|entry| entry.binding);
        emit_binding_iterator(bindings, self.memory.batch_rows.get(), emit)
    }

    fn record_memory_report(&self) {
        self.observer
            .record_blocking_memory_report(spill_backed_report(
                "TopNExec",
                &self.tracker,
                self.tracker.peak_bytes,
                self.input_rows as usize,
                &self.spill_budget,
                self.spilled_rows,
            ));
    }
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
