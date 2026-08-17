use super::*;

struct DistinctOperator<'a> {
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
    observer: &'a dyn ExecutionObserver,
    blocking_account: QueryMemoryAccount,
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
        let blocking_account = context.operator_account("DistinctExec");
        let tracker = OperatorMemoryTracker::with_account(
            context.memory.blocking_operator_bytes,
            blocking_account.clone(),
        );
        Self {
            memory: context.memory,
            task_context: context.task_context,
            observer: context.observer,
            blocking_account,
            tracker,
            spill_budget: SpillBudgetTracker::with_ledger(
                "DistinctExec",
                context.memory,
                context.memory_ledger,
            ),
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
            self.tracker.try_charge(entry_bytes)?;
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
            &self.blocking_account,
            self.task_context,
            &mut peak_tracked_bytes,
        )?;
        self.record_memory_report(self.input_rows as usize, peak_tracked_bytes);
        emit_distinct_run(
            self.runs
                .first()
                .expect("compaction retains one distinct run"),
            DistinctRunExecutionContext {
                memory_budget: self.memory.blocking_operator_bytes,
                spill_budget: &self.spill_budget,
                blocking_account: &self.blocking_account,
                batch_rows: self.memory.batch_rows.get(),
                execution_limit,
                task_context: self.task_context,
            },
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
    blocking_account: &QueryMemoryAccount,
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
                blocking_account,
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
    spill_budget: &SpillBudgetTracker,
    tracker: &mut OperatorMemoryTracker,
) -> Result<Option<DistinctRunRow>> {
    reader
        .read_binding_record(memory_limit, spill_budget)?
        .map(|record| {
            record.try_map(
                "DistinctExec merge",
                memory_limit,
                tracker,
                |ordinal, binding| {
                    let key = distinct_binding_key(&binding);
                    let memory_bytes = binding_memory_bytes(&binding)
                        .saturating_add(distinct_key_memory_bytes(&key));
                    Ok(DistinctRunRow {
                        key,
                        ordinal,
                        binding,
                        memory_bytes,
                    })
                },
                |row| row.memory_bytes,
            )
        })
        .transpose()
}

fn merge_distinct_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
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
    let mut tracker = OperatorMemoryTracker::with_account(
        memory.blocking_operator_bytes,
        blocking_account.clone(),
    );
    let mut left_row =
        read_distinct_run_row(&mut left_reader, per_row_memory, spill_budget, &mut tracker)?;
    let mut right_row = read_distinct_run_row(
        &mut right_reader,
        per_row_memory,
        spill_budget,
        &mut tracker,
    )?;
    let (run, mut writer) = spill_budget.create_run("distinct-merge")?;
    loop {
        runtime_checkpoint(task_context)?;
        *peak_tracked_bytes = (*peak_tracked_bytes).max(
            left_row
                .as_ref()
                .map_or(0, |row| row.memory_bytes)
                .saturating_add(right_row.as_ref().map_or(0, |row| row.memory_bytes)),
        );
        let selection = match (&left_row, &right_row) {
            (None, None) => break,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(left), Some(right)) => left.key.cmp(&right.key),
        };
        let (selected, released_bytes) = match selection {
            Ordering::Less => {
                let released_bytes = left_row.as_ref().expect("left row exists").memory_bytes;
                (left_row.take(), released_bytes)
            }
            Ordering::Greater => {
                let released_bytes = right_row.as_ref().expect("right row exists").memory_bytes;
                (right_row.take(), released_bytes)
            }
            Ordering::Equal => {
                let released_bytes = left_row
                    .as_ref()
                    .expect("left row exists")
                    .memory_bytes
                    .saturating_add(right_row.as_ref().expect("right row exists").memory_bytes);
                let left = left_row.take().expect("left row exists");
                let right = right_row.take().expect("right row exists");
                (
                    Some(if left.ordinal <= right.ordinal {
                        left
                    } else {
                        right
                    }),
                    released_bytes,
                )
            }
        };
        let selected = selected.expect("distinct merge selected one row");
        writer.write(selected.ordinal, &selected.binding, spill_budget)?;
        tracker.release(released_bytes);
        if left_row.is_none() {
            left_row = read_distinct_run_row(
                &mut left_reader,
                per_row_memory,
                spill_budget,
                &mut tracker,
            )?;
        }
        if right_row.is_none() {
            right_row = read_distinct_run_row(
                &mut right_reader,
                per_row_memory,
                spill_budget,
                &mut tracker,
            )?;
        }
    }
    writer.finish()?;
    Ok(run)
}

struct DistinctRunExecutionContext<'a> {
    memory_budget: NonZeroUsize,
    spill_budget: &'a SpillBudgetTracker,
    blocking_account: &'a QueryMemoryAccount,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    task_context: Option<&'a RuntimeTaskContext>,
}

fn emit_distinct_run(
    run: &spill::SpillRun,
    context: DistinctRunExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let DistinctRunExecutionContext {
        memory_budget,
        spill_budget,
        blocking_account,
        batch_rows,
        execution_limit,
        task_context,
    } = context;
    let mut reader = run.reader()?;
    let mut output = Vec::with_capacity(batch_rows);
    let mut tracker = OperatorMemoryTracker::with_account(memory_budget, blocking_account.clone());
    let mut emitted = 0usize;
    while let Some(record) = reader.read_binding_record(memory_budget.get(), spill_budget)? {
        runtime_checkpoint(task_context)?;
        let binding = record.try_map(
            "DistinctExec output",
            memory_budget.get(),
            &mut tracker,
            |_, binding| Ok(binding),
            binding_memory_bytes,
        )?;
        output.push(binding);
        emitted = emitted.saturating_add(1);
        if output.len() == batch_rows
            && emit_accounted_distinct_batch(&mut output, &mut tracker, batch_rows, emit)?
                == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if execution_limit.is_reached(emitted) {
            break;
        }
    }
    if !output.is_empty()
        && emit_accounted_distinct_batch(&mut output, &mut tracker, batch_rows, emit)?
            == BatchControl::Stop
    {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn emit_accounted_distinct_batch(
    batch: &mut BindingBatch,
    tracker: &mut OperatorMemoryTracker,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let outgoing = std::mem::replace(batch, Vec::with_capacity(batch_rows));
    tracker.reset();
    emit(outgoing)
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
