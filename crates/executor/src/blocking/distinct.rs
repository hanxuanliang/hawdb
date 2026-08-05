use super::*;

struct DistinctOperator<'a> {
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
    observer: &'a dyn ExecutionObserver,
    tracker: OperatorMemoryTracker,
    spill_budget: SpillBudgetTracker,
    distinct: BTreeMap<Vec<(String, Value)>, (u64, Binding)>,
    runs: Vec<spill::SpillRun>,
    input_rows: u64,
}

pub fn stream_distinct_batches(
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut operator = DistinctOperator::new(context);
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            operator.push(binding)?;
        }
        Ok(BatchControl::Continue)
    })?;
    operator.finish(execution_limit, emit)
}

impl<'a> DistinctOperator<'a> {
    fn new(context: BlockingExecutionContext<'a>) -> Self {
        Self {
            memory: context.memory,
            task_context: context.task_context,
            observer: context.observer,
            tracker: OperatorMemoryTracker::new(context.memory.blocking_operator_bytes),
            spill_budget: SpillBudgetTracker::new("DistinctExec", context.memory),
            distinct: BTreeMap::new(),
            runs: Vec::new(),
            input_rows: 0,
        }
    }

    fn push(&mut self, binding: Binding) -> Result<()> {
        let key = distinct_binding_key(&binding);
        let entry_bytes =
            binding_memory_bytes(&binding).saturating_add(distinct_key_memory_bytes(&key));
        ensure_operator_item_fits("DistinctExec", entry_bytes, &self.tracker)?;
        if !self.distinct.contains_key(&key) {
            if self.tracker.would_exceed(entry_bytes) {
                self.runs.push(spill_distinct_run(
                    &mut self.distinct,
                    &mut self.spill_budget,
                    self.task_context,
                )?);
                self.tracker.reset();
            }
            self.tracker.charge(entry_bytes);
            self.distinct.insert(key, (self.input_rows, binding));
        }
        self.input_rows = self.input_rows.saturating_add(1);
        Ok(())
    }

    fn finish(
        mut self,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.runs.is_empty() {
            self.record_memory_report(0, self.tracker.peak_bytes);
            let mut selected = self.distinct.into_values().collect::<Vec<_>>();
            selected.sort_by_key(|(ordinal, _)| *ordinal);
            return emit_binding_iterator(
                selected
                    .into_iter()
                    .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                    .map(|(_, binding)| binding),
                self.memory.batch_rows.get(),
                emit,
            );
        }
        if !self.distinct.is_empty() {
            self.runs.push(spill_distinct_run(
                &mut self.distinct,
                &mut self.spill_budget,
                self.task_context,
            )?);
            self.tracker.reset();
        }
        let mut peak_tracked_bytes = self.tracker.peak_bytes;
        self.runs = compact_distinct_runs(
            self.runs,
            self.memory,
            &mut self.spill_budget,
            self.task_context,
            &mut peak_tracked_bytes,
        )?;
        self.record_memory_report(self.input_rows as usize, peak_tracked_bytes);
        emit_distinct_run(
            self.runs
                .first()
                .expect("compaction retains one distinct run"),
            self.memory.blocking_operator_bytes,
            self.memory.batch_rows.get(),
            execution_limit,
            self.task_context,
            emit,
        )
    }

    fn record_memory_report(&self, spilled_rows: usize, peak_tracked_bytes: usize) {
        self.observer
            .record_blocking_memory_report(spill_backed_report(
                "DistinctExec",
                &self.tracker,
                peak_tracked_bytes,
                self.input_rows as usize,
                &self.spill_budget,
                spilled_rows,
            ));
    }
}

fn distinct_binding_key(binding: &Binding) -> Vec<(String, Value)> {
    binding
        .values
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn spill_distinct_run(
    distinct: &mut BTreeMap<Vec<(String, Value)>, (u64, Binding)>,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let (run, mut writer) = spill_budget.create_run("distinct")?;
    for (_, (ordinal, binding)) in std::mem::take(distinct) {
        runtime_checkpoint(task_context)?;
        writer.write(ordinal, &binding, spill_budget)?;
    }
    writer.finish()?;
    Ok(run)
}

fn compact_distinct_runs(
    mut runs: Vec<spill::SpillRun>,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
    peak_tracked_bytes: &mut usize,
) -> Result<Vec<spill::SpillRun>> {
    while runs.len() > 1 {
        runtime_checkpoint(task_context)?;
        let mut compacted = Vec::with_capacity(runs.len().div_ceil(2));
        let mut pending = runs.into_iter();
        while let Some(left) = pending.next() {
            let Some(right) = pending.next() else {
                compacted.push(left);
                break;
            };
            compacted.push(merge_distinct_run_pair(
                &left,
                &right,
                memory,
                spill_budget,
                task_context,
                peak_tracked_bytes,
            )?);
        }
        runs = compacted;
    }
    Ok(runs)
}

struct DistinctRunRow {
    key: Vec<(String, Value)>,
    ordinal: u64,
    binding: Binding,
    memory_bytes: usize,
}

fn read_distinct_run_row(
    reader: &mut spill::SpillReader,
    memory_limit: usize,
) -> Result<Option<DistinctRunRow>> {
    let Some((ordinal, binding)) = reader.read(memory_limit)? else {
        return Ok(None);
    };
    let key = distinct_binding_key(&binding);
    let memory_bytes =
        binding_memory_bytes(&binding).saturating_add(distinct_key_memory_bytes(&key));
    if memory_bytes > memory_limit {
        return Err(SkeinError::Execution(format!(
            "DistinctExec spill merge row uses {memory_bytes} bytes, exceeding the per-row memory limit {memory_limit}"
        )));
    }
    Ok(Some(DistinctRunRow {
        key,
        ordinal,
        binding,
        memory_bytes,
    }))
}

fn merge_distinct_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
    peak_tracked_bytes: &mut usize,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let per_row_memory = memory.blocking_operator_bytes.get() / 2;
    if per_row_memory == 0 {
        return Err(SkeinError::Execution(
            "DistinctExec spill merge requires at least two bytes of blocking memory".to_string(),
        ));
    }
    let mut left_reader = left.reader()?;
    let mut right_reader = right.reader()?;
    let mut left_row = read_distinct_run_row(&mut left_reader, per_row_memory)?;
    let mut right_row = read_distinct_run_row(&mut right_reader, per_row_memory)?;
    let (run, mut writer) = spill_budget.create_run("distinct-merge")?;
    loop {
        runtime_checkpoint(task_context)?;
        *peak_tracked_bytes = (*peak_tracked_bytes).max(
            left_row
                .as_ref()
                .map_or(0, |row| row.memory_bytes)
                .saturating_add(right_row.as_ref().map_or(0, |row| row.memory_bytes)),
        );
        let selected = match (&left_row, &right_row) {
            (None, None) => break,
            (Some(_), None) => left_row.take(),
            (None, Some(_)) => right_row.take(),
            (Some(left), Some(right)) => match left.key.cmp(&right.key) {
                Ordering::Less => left_row.take(),
                Ordering::Greater => right_row.take(),
                Ordering::Equal => {
                    let left = left_row.take().expect("left row exists");
                    let right = right_row.take().expect("right row exists");
                    Some(if left.ordinal <= right.ordinal {
                        left
                    } else {
                        right
                    })
                }
            },
        };
        let selected = selected.expect("distinct merge selected one row");
        writer.write(selected.ordinal, &selected.binding, spill_budget)?;
        if left_row.is_none() {
            left_row = read_distinct_run_row(&mut left_reader, per_row_memory)?;
        }
        if right_row.is_none() {
            right_row = read_distinct_run_row(&mut right_reader, per_row_memory)?;
        }
    }
    writer.finish()?;
    Ok(run)
}

fn emit_distinct_run(
    run: &spill::SpillRun,
    memory_budget: NonZeroUsize,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut reader = run.reader()?;
    let mut output = Vec::with_capacity(batch_rows);
    let mut emitted = 0usize;
    while let Some((_, binding)) = reader.read(memory_budget.get())? {
        runtime_checkpoint(task_context)?;
        output.push(binding);
        emitted = emitted.saturating_add(1);
        if output.len() == batch_rows
            && emit(std::mem::replace(
                &mut output,
                Vec::with_capacity(batch_rows),
            ))? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if execution_limit.is_reached(emitted) {
            break;
        }
    }
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn distinct_key_memory_bytes(key: &[(String, Value)]) -> usize {
    std::mem::size_of::<Vec<(String, Value)>>().saturating_add(key.iter().fold(
        0usize,
        |total, (name, value)| {
            total
                .saturating_add(std::mem::size_of::<(String, Value)>())
                .saturating_add(name.len())
                .saturating_add(value_memory_bytes(value))
        },
    ))
}
