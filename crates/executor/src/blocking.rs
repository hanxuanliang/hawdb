//! Memory-bounded blocking operators and spill-backed execution.

use crate::binding::{binding_memory_bytes, value_memory_bytes, Binding, TopNBinding};
use crate::expression::{
    binding_has_variable, binding_identity_key, binding_property, binding_value, group_key_value,
    insert_projected_value, sort_value,
};
use crate::kernel::{ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker};
use crate::observer::ExecutionObserver;
use crate::pipeline::{emit_binding_iterator, runtime_checkpoint, BatchControl, BindingBatch};
use crate::spill;
use crate::{BlockingOperatorMemoryReport, ExecutionLimit, ExecutionMemoryConfig};
use skein_core::{Catalog, Result, RuntimeTaskContext, SkeinError, Value};
use skein_plan::{
    AggregateFunction, AggregateTarget, Aggregation, PhysicalPlan, Projection, SortDirection,
    SortItem,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::num::NonZeroUsize;

pub trait BindingBatchSource {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl>;
}

pub struct BlockingExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub memory: &'a ExecutionMemoryConfig,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a mut dyn ExecutionObserver,
}

pub fn spill_binding_run(
    operator: &str,
    bindings: &mut Vec<Binding>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, operator)?;
    for binding in bindings.drain(..) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(0, &binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    writer.finish()?;
    Ok(run)
}

pub fn stream_distinct_batches(
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BlockingExecutionContext {
        memory,
        task_context,
        observer,
        ..
    } = context;
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("DistinctExec", memory);
    let mut distinct = BTreeMap::<Vec<(String, Value)>, (u64, Binding)>::new();
    let mut runs = Vec::new();
    let mut ordinal = 0u64;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            let key = distinct_binding_key(&binding);
            let entry_bytes =
                binding_memory_bytes(&binding).saturating_add(distinct_key_memory_bytes(&key));
            ensure_operator_item_fits("DistinctExec", entry_bytes, &tracker)?;
            if !distinct.contains_key(&key) {
                if tracker.would_exceed(entry_bytes) {
                    runs.push(spill_distinct_run(
                        &mut distinct,
                        &memory.spill_directory,
                        &mut spill_budget,
                        task_context,
                    )?);
                    tracker.reset();
                }
                tracker.charge(entry_bytes);
                distinct.insert(key, (ordinal, binding));
            }
            ordinal = ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;
    if runs.is_empty() {
        observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
            operator: "DistinctExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows: ordinal as usize,
            max_spill_bytes: memory.max_spill_bytes.get(),
            max_spill_runs: memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        let mut selected = distinct.into_values().collect::<Vec<_>>();
        selected.sort_by_key(|(ordinal, _)| *ordinal);
        return emit_binding_iterator(
            selected
                .into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .map(|(_, binding)| binding),
            memory.batch_rows.get(),
            emit,
        );
    }
    if !distinct.is_empty() {
        runs.push(spill_distinct_run(
            &mut distinct,
            &memory.spill_directory,
            &mut spill_budget,
            task_context,
        )?);
        tracker.reset();
    }
    let mut peak_tracked_bytes = tracker.peak_bytes;
    runs = compact_distinct_runs(
        runs,
        memory,
        &mut spill_budget,
        task_context,
        &mut peak_tracked_bytes,
    )?;
    observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
        operator: "DistinctExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes,
        input_rows: ordinal as usize,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows: ordinal as usize,
    });
    emit_distinct_run(
        runs.first().expect("compaction retains one distinct run"),
        memory.blocking_operator_bytes,
        memory.batch_rows.get(),
        execution_limit,
        task_context,
        emit,
    )
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
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "distinct")?;
    for (_, (ordinal, binding)) in std::mem::take(distinct) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(ordinal, &binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
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
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(&memory.spill_directory, "distinct-merge")?;
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
        let bytes = writer.write(
            selected.ordinal,
            &selected.binding,
            spill_budget.remaining_bytes(),
        )?;
        spill_budget.charge(bytes)?;
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum AggregateDistinctValue {
    Identity(u8, u64),
    Value(Value),
}

enum AggregateState {
    Count {
        count: usize,
        distinct: Option<BTreeSet<AggregateDistinctValue>>,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Avg {
        sum: f64,
        count: usize,
    },
    Collect {
        values: Vec<Value>,
        distinct: Option<BTreeSet<Value>>,
    },
}

#[derive(Default)]
struct MemoryDelta {
    added_bytes: usize,
    released_bytes: usize,
}

impl MemoryDelta {
    fn between(previous: usize, next: usize) -> Self {
        if next >= previous {
            Self {
                added_bytes: next - previous,
                released_bytes: 0,
            }
        } else {
            Self {
                added_bytes: 0,
                released_bytes: previous - next,
            }
        }
    }

    fn combine(&mut self, other: Self) {
        self.added_bytes = self.added_bytes.saturating_add(other.added_bytes);
        self.released_bytes = self.released_bytes.saturating_add(other.released_bytes);
    }
}

impl AggregateState {
    fn new(item: &Aggregation) -> Self {
        match item.function {
            AggregateFunction::Count => Self::Count {
                count: 0,
                distinct: item.distinct.then(BTreeSet::new),
            },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Collect => Self::Collect {
                values: Vec::new(),
                distinct: item.distinct.then(BTreeSet::new),
            },
        }
    }

    fn update(&mut self, item: &Aggregation, catalog: &Catalog, binding: &Binding) -> MemoryDelta {
        match self {
            Self::Count { count, distinct } => {
                if distinct.is_none() {
                    let matched = match &item.target {
                        AggregateTarget::All => true,
                        AggregateTarget::Variable(variable) => {
                            binding_has_variable(binding, variable)
                        }
                        AggregateTarget::Property { variable, property } => {
                            binding_property(binding, variable, property)
                                .is_some_and(|value| value != &Value::Null)
                        }
                    };
                    if matched {
                        *count = count.saturating_add(1);
                    }
                    return MemoryDelta::default();
                }
                let value = match &item.target {
                    AggregateTarget::All => {
                        *count = count.saturating_add(1);
                        return MemoryDelta::default();
                    }
                    AggregateTarget::Variable(variable) => binding_identity_key(binding, variable)
                        .map(|(kind, id)| AggregateDistinctValue::Identity(kind, id)),
                    AggregateTarget::Property { variable, property } => binding
                        .nodes
                        .get(variable)
                        .and_then(|node| node.properties.get(property))
                        .or_else(|| {
                            binding
                                .relationships
                                .get(variable)
                                .and_then(|relationship| relationship.properties.get(property))
                        })
                        .filter(|value| *value != &Value::Null)
                        .cloned()
                        .map(AggregateDistinctValue::Value),
                };
                let Some(value) = value else {
                    return MemoryDelta::default();
                };
                let value_bytes = aggregate_distinct_value_memory_bytes(&value)
                    .saturating_add(std::mem::size_of::<usize>() * 4);
                if let Some(distinct) = distinct {
                    if distinct.insert(value) {
                        *count = count.saturating_add(1);
                        return MemoryDelta {
                            added_bytes: value_bytes,
                            released_bytes: 0,
                        };
                    }
                } else {
                    *count = count.saturating_add(1);
                }
                MemoryDelta::default()
            }
            Self::Min(current) => {
                if let Some(value) = aggregate_property_value(&item.target, binding)
                    && current.as_ref().is_none_or(|current| value < *current)
                {
                    let previous = current.as_ref().map_or(0, value_memory_bytes);
                    let next = value_memory_bytes(&value);
                    *current = Some(value);
                    return MemoryDelta::between(previous, next);
                }
                MemoryDelta::default()
            }
            Self::Max(current) => {
                if let Some(value) = aggregate_property_value(&item.target, binding)
                    && current.as_ref().is_none_or(|current| value > *current)
                {
                    let previous = current.as_ref().map_or(0, value_memory_bytes);
                    let next = value_memory_bytes(&value);
                    *current = Some(value);
                    return MemoryDelta::between(previous, next);
                }
                MemoryDelta::default()
            }
            Self::Avg { sum, count } => {
                if let Some(value) = aggregate_property_value(&item.target, binding) {
                    match value {
                        Value::Int(value) => {
                            *sum += value as f64;
                            *count = count.saturating_add(1);
                        }
                        Value::Float(value) if value.is_finite() => {
                            *sum += value;
                            *count = count.saturating_add(1);
                        }
                        _ => {}
                    }
                }
                MemoryDelta::default()
            }
            Self::Collect { values, distinct } => {
                let value = match &item.target {
                    AggregateTarget::Variable(variable) => {
                        binding_value(binding, catalog, variable)
                    }
                    AggregateTarget::Property { .. } => {
                        aggregate_property_value(&item.target, binding)
                    }
                    AggregateTarget::All => None,
                };
                let Some(value) = value.filter(|value| value != &Value::Null) else {
                    return MemoryDelta::default();
                };
                let value_bytes =
                    value_memory_bytes(&value).saturating_add(std::mem::size_of::<usize>() * 4);
                if let Some(distinct) = distinct {
                    if distinct.insert(value) {
                        return MemoryDelta {
                            added_bytes: value_bytes,
                            released_bytes: 0,
                        };
                    }
                } else {
                    values.push(value);
                    return MemoryDelta {
                        added_bytes: value_bytes,
                        released_bytes: 0,
                    };
                }
                MemoryDelta::default()
            }
        }
    }

    fn finish(self) -> Value {
        match self {
            Self::Count { count, .. } => Value::Int(count as i64),
            Self::Min(value) | Self::Max(value) => value.unwrap_or(Value::Null),
            Self::Avg { sum, count } if count > 0 => Value::Float(sum / count as f64),
            Self::Avg { .. } => Value::Null,
            Self::Collect {
                values,
                distinct: None,
            } => Value::List(values),
            Self::Collect {
                distinct: Some(values),
                ..
            } => Value::List(values.into_iter().collect()),
        }
    }
}

fn aggregate_distinct_value_memory_bytes(value: &AggregateDistinctValue) -> usize {
    std::mem::size_of::<AggregateDistinctValue>().saturating_add(match value {
        AggregateDistinctValue::Identity(_, _) => 0,
        AggregateDistinctValue::Value(value) => value_memory_bytes(value),
    })
}

fn aggregate_property_value(target: &AggregateTarget, binding: &Binding) -> Option<Value> {
    let AggregateTarget::Property { variable, property } = target else {
        return None;
    };
    binding_property(binding, variable, property)
        .filter(|value| *value != &Value::Null)
        .cloned()
}

struct GroupAccumulator<'a> {
    key: Vec<Value>,
    group_keys: &'a [Projection],
    items: &'a [Aggregation],
    states: Vec<AggregateState>,
}

impl<'a> GroupAccumulator<'a> {
    fn new(key: Vec<Value>, group_keys: &'a [Projection], items: &'a [Aggregation]) -> Self {
        Self {
            key,
            group_keys,
            items,
            states: items.iter().map(AggregateState::new).collect(),
        }
    }

    fn base_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.key.iter().fold(0usize, |total, value| {
                total.saturating_add(value_memory_bytes(value))
            }))
            .saturating_add(
                self.states
                    .len()
                    .saturating_mul(std::mem::size_of::<AggregateState>()),
            )
    }

    fn update(&mut self, catalog: &Catalog, binding: &Binding) -> MemoryDelta {
        let mut delta = MemoryDelta::default();
        for (state, item) in self.states.iter_mut().zip(self.items) {
            delta.combine(state.update(item, catalog, binding));
        }
        delta
    }

    fn finish(self) -> Binding {
        let mut values = BTreeMap::new();
        for (item, value) in self.group_keys.iter().zip(self.key) {
            insert_projected_value(&mut values, &item.name, value);
        }
        for (item, state) in self.items.iter().zip(self.states) {
            insert_projected_value(&mut values, &item.name, state.finish());
        }
        Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }
}

struct GroupRunRow {
    key: Vec<Value>,
    ordinal: u64,
    binding: Binding,
}

impl GroupRunRow {
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }

    fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding).saturating_add(
            self.key
                .iter()
                .fold(std::mem::size_of::<Vec<Value>>(), |total, value| {
                    total.saturating_add(value_memory_bytes(value))
                }),
        )
    }
}

struct GroupMergeEntry {
    row: GroupRunRow,
    run_index: usize,
}

#[derive(Clone, Copy)]
struct AggregateExecutionContext<'a> {
    group_keys: &'a [Projection],
    items: &'a [Aggregation],
    catalog: &'a Catalog,
    batch_rows: usize,
    memory_budget: NonZeroUsize,
    execution_limit: ExecutionLimit,
    task_context: Option<&'a RuntimeTaskContext>,
}

impl PartialEq for GroupMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.cmp_key(&other.row) == Ordering::Equal && self.run_index == other.run_index
    }
}

impl Eq for GroupMergeEntry {}

impl Ord for GroupMergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for GroupMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn stream_aggregate_batches(
    input: &PhysicalPlan,
    group_keys: &[Projection],
    items: &[Aggregation],
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
    if group_keys.is_empty() {
        let mut accumulator = GroupAccumulator::new(Vec::new(), group_keys, items);
        let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
        let base_bytes = accumulator.base_memory_bytes();
        ensure_operator_item_fits("AggregateExec", base_bytes, &tracker)?;
        tracker.charge(base_bytes);
        let mut input_rows = 0usize;
        source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
            runtime_checkpoint(task_context)?;
            for binding in &batch {
                update_group_accumulator(&mut accumulator, catalog, binding, &mut tracker)?;
                input_rows = input_rows.saturating_add(1);
            }
            Ok(BatchControl::Continue)
        })?;
        observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
            operator: "AggregateExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows,
            max_spill_bytes: memory.max_spill_bytes.get(),
            max_spill_runs: memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        return emit(vec![accumulator.finish()]);
    }

    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("AggregateExec", memory);
    let mut rows = Vec::<GroupRunRow>::new();
    let mut runs = Vec::<spill::SpillRun>::new();
    let mut ordinal = 0u64;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(task_context)?;
        for binding in batch {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect::<Vec<_>>();
            let bytes = binding_memory_bytes(&binding).saturating_add(
                key.iter().fold(0usize, |total, value| {
                    total.saturating_add(value_memory_bytes(value))
                }),
            );
            ensure_operator_item_fits("AggregateExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                runs.push(spill_group_run(
                    &mut rows,
                    &memory.spill_directory,
                    &mut spill_budget,
                    task_context,
                )?);
                tracker.reset();
            }
            tracker.charge(bytes);
            rows.push(GroupRunRow {
                key,
                ordinal,
                binding,
            });
            ordinal = ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;

    if runs.is_empty() {
        observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
            operator: "AggregateExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows: ordinal as usize,
            max_spill_bytes: memory.max_spill_bytes.get(),
            max_spill_runs: memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        rows.sort_by(GroupRunRow::cmp_key);
        let aggregate_context = AggregateExecutionContext {
            group_keys,
            items,
            catalog,
            batch_rows: memory.batch_rows.get(),
            memory_budget: memory.blocking_operator_bytes,
            execution_limit,
            task_context,
        };
        return aggregate_sorted_group_rows(rows, aggregate_context, emit);
    }
    if !rows.is_empty() {
        runs.push(spill_group_run(
            &mut rows,
            &memory.spill_directory,
            &mut spill_budget,
            task_context,
        )?);
    }
    runs = compact_group_runs(
        runs,
        group_keys,
        catalog,
        memory,
        &mut spill_budget,
        task_context,
    )?;
    observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
        operator: "AggregateExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes: tracker.peak_bytes,
        input_rows: ordinal as usize,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows: ordinal as usize,
    });
    let aggregate_context = AggregateExecutionContext {
        group_keys,
        items,
        catalog,
        batch_rows: memory.batch_rows.get(),
        memory_budget: memory.blocking_operator_bytes,
        execution_limit,
        task_context,
    };
    merge_group_runs(&runs, aggregate_context, emit)
}

fn spill_group_run(
    rows: &mut Vec<GroupRunRow>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    rows.sort_by(GroupRunRow::cmp_key);
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "aggregate")?;
    for row in rows.drain(..) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(row.ordinal, &row.binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

fn compact_group_runs(
    mut runs: Vec<spill::SpillRun>,
    group_keys: &[Projection],
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
            compacted.push(merge_group_run_pair(
                &left,
                &right,
                group_keys,
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
fn merge_group_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    group_keys: &[Projection],
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
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let entry = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            if bytes > per_row_budget {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec spill merge row uses {bytes} bytes, exceeding half of blocking_operator_bytes {}",
                    memory.blocking_operator_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(entry);
        }
    }
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(&memory.spill_directory, "aggregate-merge")?;
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
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let next = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = next.row.memory_bytes();
            if bytes > per_row_budget || tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec spill merge exceeds blocking_operator_bytes {}",
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

fn aggregate_sorted_group_rows(
    rows: Vec<GroupRunRow>,
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let AggregateExecutionContext {
        group_keys,
        items,
        catalog,
        batch_rows,
        memory_budget,
        execution_limit,
        task_context,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    let mut batch = Vec::with_capacity(batch_rows);
    let mut accumulator: Option<GroupAccumulator<'_>> = None;
    let mut emitted = 0usize;
    for row in rows {
        runtime_checkpoint(task_context)?;
        if accumulator
            .as_ref()
            .is_some_and(|accumulator| accumulator.key != row.key)
        {
            batch.push(accumulator.take().expect("group exists").finish());
            tracker.reset();
            emitted = emitted.saturating_add(1);
            if flush_aggregate_batch(&mut batch, batch_rows, emitted, execution_limit, emit)?
                == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
        }
        if accumulator.is_none() {
            let next = GroupAccumulator::new(row.key.clone(), group_keys, items);
            let base_bytes = next.base_memory_bytes();
            ensure_operator_item_fits("AggregateExec group state", base_bytes, &tracker)?;
            tracker.charge(base_bytes);
            accumulator = Some(next);
        }
        update_group_accumulator(
            accumulator.as_mut().expect("group exists"),
            catalog,
            &row.binding,
            &mut tracker,
        )?;
    }
    runtime_checkpoint(task_context)?;
    if let Some(accumulator) = accumulator {
        batch.push(accumulator.finish());
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn merge_group_runs(
    runs: &[spill::SpillRun],
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let AggregateExecutionContext {
        group_keys,
        items,
        catalog,
        batch_rows,
        memory_budget,
        execution_limit,
        task_context,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut accumulator_tracker = OperatorMemoryTracker::new(memory_budget);
    let mut readers = runs
        .iter()
        .map(spill::SpillRun::reader)
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    let mut merge_tracker = OperatorMemoryTracker::new(memory_budget);
    for (run_index, reader) in readers.iter_mut().enumerate() {
        runtime_checkpoint(task_context)?;
        if let Some((ordinal, binding)) = reader.read(memory_budget.get())? {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let entry = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            ensure_operator_item_fits("AggregateExec merge", bytes, &merge_tracker)?;
            if merge_tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec merge fan-in uses more than blocking_operator_bytes {}",
                    merge_tracker.budget_bytes
                )));
            }
            merge_tracker.charge(bytes);
            heap.push(entry);
        }
    }
    let mut batch = Vec::with_capacity(batch_rows);
    let mut accumulator: Option<GroupAccumulator<'_>> = None;
    let mut emitted = 0usize;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        merge_tracker.release(entry.row.memory_bytes());
        let run_index = entry.run_index;
        let row = entry.row;
        if accumulator
            .as_ref()
            .is_some_and(|accumulator| accumulator.key != row.key)
        {
            batch.push(accumulator.take().expect("group exists").finish());
            accumulator_tracker.reset();
            emitted = emitted.saturating_add(1);
            if flush_aggregate_batch(&mut batch, batch_rows, emitted, execution_limit, emit)?
                == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
        }
        if accumulator.is_none() {
            let next = GroupAccumulator::new(row.key.clone(), group_keys, items);
            let base_bytes = next.base_memory_bytes();
            ensure_operator_item_fits(
                "AggregateExec group state",
                base_bytes,
                &accumulator_tracker,
            )?;
            accumulator_tracker.charge(base_bytes);
            accumulator = Some(next);
        }
        update_group_accumulator(
            accumulator.as_mut().expect("group exists"),
            catalog,
            &row.binding,
            &mut accumulator_tracker,
        )?;
        if let Some((ordinal, binding)) = readers[run_index].read(memory_budget.get())? {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let next = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = next.row.memory_bytes();
            ensure_operator_item_fits("AggregateExec merge", bytes, &merge_tracker)?;
            if merge_tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec merge fan-in uses more than blocking_operator_bytes {}",
                    merge_tracker.budget_bytes
                )));
            }
            merge_tracker.charge(bytes);
            heap.push(next);
        }
    }
    runtime_checkpoint(task_context)?;
    if let Some(accumulator) = accumulator {
        batch.push(accumulator.finish());
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn update_group_accumulator(
    accumulator: &mut GroupAccumulator<'_>,
    catalog: &Catalog,
    binding: &Binding,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    let delta = accumulator.update(catalog, binding);
    tracker.release(delta.released_bytes);
    if tracker.would_exceed(delta.added_bytes) {
        return Err(SkeinError::Execution(format!(
            "AggregateExec state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.charge(delta.added_bytes);
    Ok(())
}

fn flush_aggregate_batch(
    batch: &mut BindingBatch,
    batch_rows: usize,
    emitted: usize,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if (batch.len() == batch_rows || execution_limit.is_reached(emitted))
        && (emit(std::mem::replace(batch, Vec::with_capacity(batch_rows)))? == BatchControl::Stop
            || execution_limit.is_reached(emitted))
    {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::NoopExecutionObserver;

    struct FixedBatchSource {
        batches: Vec<BindingBatch>,
    }

    impl BindingBatchSource for FixedBatchSource {
        fn execute(
            &mut self,
            _input: &PhysicalPlan,
            _execution_limit: ExecutionLimit,
            emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
        ) -> Result<BatchControl> {
            for batch in std::mem::take(&mut self.batches) {
                if emit(batch)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        }
    }

    fn value_binding(value: i64) -> Binding {
        Binding {
            values: BTreeMap::from([("value".to_string(), Value::Int(value))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    #[test]
    fn distinct_operator_consumes_storage_neutral_batches_in_input_order() {
        let input = PhysicalPlan::SeqNodeScan {
            variable: "node".to_string(),
            label: String::new(),
        };
        let mut source = FixedBatchSource {
            batches: vec![
                vec![value_binding(1), value_binding(1)],
                vec![value_binding(2)],
            ],
        };
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig::default();
        let mut observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_distinct_batches(
            &input,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                task_context: None,
                observer: &mut observer,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .expect("distinct execution");

        assert_eq!(output, vec![value_binding(1), value_binding(2)]);
    }
}
