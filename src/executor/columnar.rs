//! Root storage adapter for eligible vectorized read fragments.

mod lending;

use super::*;
use lending::{
    admitted_numeric_batch_rows, LendingBatchCursor, NumericNodeBatch, NumericNodeBatchCursor,
    OwnedNumericBatchBuffer,
};
use skein_executor::columnar::{
    filter_float64_values, filter_int64_values, select_float64_values_view,
    select_int64_values_view, NumericLiteral, Selection, ValidityBuilder,
};
use skein_executor::morsel::{
    MorselAdmission, MorselAdmissionRequest, PipelineId, SharedPoolMorselScheduler,
};
use skein_executor::observer::ExecutionObserver;
use skein_executor::SharedExecutorPool;
use std::borrow::Borrow;

const DEFAULT_BATCHES_PER_MORSEL: usize = 16;
const DEFAULT_MIN_MORSELS_PER_WORKER: usize = 4;

#[derive(Debug, Clone, Copy)]
struct NumericFragment<'a> {
    label: &'a str,
    property: &'a str,
    property_type: crate::schema::PropertyType,
    op: skein_plan::ComparisonOp,
    expected: NumericLiteral,
}

#[derive(Debug, Clone, Copy)]
struct LendingNumericScan {
    batch_rows: usize,
    needs_node_ids: bool,
}

struct NumericBatchEmitter<'plan, 'task, 'observer, 'emit> {
    fragment: NumericFragment<'plan>,
    items: &'plan [Projection],
    emitted: usize,
    execution_limit: ExecutionLimit,
    task_context: Option<&'task RuntimeTaskContext>,
    observer: &'observer QueryExecutionObserver,
    emit: &'emit mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    selected_rows: Vec<u32>,
}

pub(super) fn supports_parallel_morsel_execution(plan: &PhysicalPlan, catalog: &Catalog) -> bool {
    match plan {
        PhysicalPlan::ProjectExec { items, input }
            if NumericFragment::try_prepare(items, input, catalog).is_some() =>
        {
            true
        }
        _ => match plan.children() {
            PlanChildren::None => false,
            PlanChildren::Unary(input) => supports_parallel_morsel_execution(input, catalog),
            PlanChildren::Binary(left, right) => {
                supports_parallel_morsel_execution(left, catalog)
                    || supports_parallel_morsel_execution(right, catalog)
            }
        },
    }
}

pub(super) fn default_morsel_parallelism(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &GraphStore,
    memory: &ExecutionMemoryConfig,
) -> usize {
    match plan {
        PhysicalPlan::ProjectExec { items, input } => {
            if let Some(fragment) = NumericFragment::try_prepare(items, input, catalog) {
                return fragment.default_parallelism(items, catalog, store, memory);
            }
            default_morsel_parallelism(input, catalog, store, memory)
        }
        _ => match plan.children() {
            PlanChildren::None => 1,
            PlanChildren::Unary(input) => default_morsel_parallelism(input, catalog, store, memory),
            PlanChildren::Binary(left, right) => {
                default_morsel_parallelism(left, catalog, store, memory)
                    .max(default_morsel_parallelism(right, catalog, store, memory))
            }
        },
    }
}

pub(super) fn try_stream_columnar_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    let fragment = NumericFragment::try_prepare(items, input, context.catalog)?;
    Some(fragment.stream(items, context, execution_limit, emit))
}

impl<'a> NumericFragment<'a> {
    fn try_prepare(
        items: &[Projection],
        input: &'a PhysicalPlan,
        catalog: &Catalog,
    ) -> Option<Self> {
        let PhysicalPlan::FilterExec { predicate, input } = input else {
            return None;
        };
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate
        else {
            return None;
        };
        let PhysicalPlan::SeqNodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || label.is_empty() || label.contains('|') {
            return None;
        }
        if !items.iter().all(|item| {
            matches!(
                &item.expression,
                ProjectionExpression::Id { variable }
                    | ProjectionExpression::Property { variable, .. }
                    if variable == scan_variable
            ) || matches!(&item.expression, ProjectionExpression::Literal(_))
        }) {
            return None;
        }
        let expected = NumericLiteral::from_value(value)?;
        let table_id = catalog.table_id(crate::schema::TableKind::Node, label)?;
        let descriptor_id = catalog.property_descriptor_id(table_id, property)?;
        let descriptor = catalog.property_descriptor(descriptor_id)?;
        if descriptor.state != crate::schema::SchemaObjectState::Public
            || !matches!(
                descriptor.value_type,
                crate::schema::PropertyType::Int | crate::schema::PropertyType::Float
            )
        {
            return None;
        }
        Some(Self {
            label,
            property,
            property_type: descriptor.value_type,
            op: *op,
            expected,
        })
    }

    fn supports_lending_projection(self, items: &[Projection]) -> bool {
        items.iter().all(|item| match &item.expression {
            ProjectionExpression::Id { .. } | ProjectionExpression::Literal(_) => true,
            ProjectionExpression::Property { property, .. } => property == self.property,
            _ => false,
        })
    }

    fn default_parallelism(
        self,
        items: &[Projection],
        catalog: &Catalog,
        store: &GraphStore,
        memory: &ExecutionMemoryConfig,
    ) -> usize {
        if store.is_out_of_core() {
            return 1;
        }
        let Some(label_id) = catalog.label_id(self.label) else {
            return 1;
        };
        let needs_node_ids = items
            .iter()
            .any(|item| matches!(item.expression, ProjectionExpression::Id { .. }));
        let batch_rows = if self.supports_lending_projection(items) {
            admitted_numeric_batch_rows(
                memory.batch_rows.get(),
                memory.batch_payload_bytes.get(),
                needs_node_ids,
                true,
            )
            .unwrap_or(1)
        } else {
            memory.batch_rows.get()
        };
        let morsel_rows = batch_rows.saturating_mul(DEFAULT_BATCHES_PER_MORSEL);
        let morsel_count = store
            .node_count_for_label(Some(label_id))
            .div_ceil(morsel_rows.max(1));
        default_morsel_worker_count(morsel_count, MAX_MORSEL_PARALLELISM)
    }

    fn stream(
        self,
        items: &[Projection],
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Some(label_id) = context.catalog.label_id(self.label) else {
            return Ok(BatchControl::Continue);
        };
        let supports_typed_projection = self.supports_lending_projection(items);
        let use_lending = !context.store.is_out_of_core() && supports_typed_projection;
        let use_owned_typed = context.store.is_out_of_core() && supports_typed_projection;
        let needs_node_ids = items
            .iter()
            .any(|item| matches!(item.expression, ProjectionExpression::Id { .. }));
        let target_rows = if use_lending || use_owned_typed {
            let admitted = admitted_numeric_batch_rows(
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes.get(),
                needs_node_ids,
                true,
            )
            .ok_or_else(|| {
                SkeinError::Execution(format!(
                    "numeric lending scan scratch requires more than batch_payload_bytes {}",
                    context.memory.batch_payload_bytes
                ))
            })?;
            NonZeroUsize::new(admitted).expect("admitted batch rows are non-zero")
        } else {
            context.memory.batch_rows
        };
        let lending_scan = LendingNumericScan {
            batch_rows: target_rows.get(),
            needs_node_ids,
        };
        let candidate_count = context.store.node_count_for_label(Some(label_id));
        let morsel_rows =
            NonZeroUsize::new(target_rows.get().saturating_mul(DEFAULT_BATCHES_PER_MORSEL))
                .expect("morsel row target is non-zero");
        let pool = (!context.store.is_out_of_core())
            .then(SharedExecutorPool::shared_default)
            .transpose()
            .ok()
            .flatten();
        let pool_parallelism = pool
            .as_ref()
            .map_or(1, SharedExecutorPool::worker_count)
            .min(MAX_MORSEL_PARALLELISM);
        let admitted_parallelism = context
            .task_context
            .map(|task_context| task_context.admitted_parallelism().get())
            .unwrap_or(1);
        let morsel_count = candidate_count.div_ceil(morsel_rows.get());
        let requested_parallelism = NonZeroUsize::new(
            default_morsel_worker_count(morsel_count, pool_parallelism.min(admitted_parallelism))
                .max(1),
        )
        .expect("morsel parallelism is non-zero");
        let input_reference_bytes = morsel_rows
            .get()
            .saturating_mul(std::mem::size_of::<&NodeRecord>());
        let bytes_per_worker = NonZeroUsize::new(
            context
                .memory
                .batch_payload_bytes
                .get()
                .saturating_add(input_reference_bytes),
        )
        .expect("batch payload budget is non-zero");
        let memory_budget_bytes = NonZeroUsize::new(
            bytes_per_worker
                .get()
                .saturating_mul(requested_parallelism.get()),
        )
        .expect("morsel memory budget is non-zero");
        let admission = MorselAdmission::try_new(MorselAdmissionRequest {
            pipeline_id: PipelineId(0),
            input_rows: candidate_count,
            target_rows: morsel_rows,
            requested_parallelism,
            bytes_per_worker,
            memory_budget_bytes,
        })?;
        let parallel = admission.max_workers() > 1
            && execution_limit
                .output_rows
                .is_none_or(|limit| limit > morsel_rows.get())
            && pool.is_some();
        context.observer.record_morsel_admission(
            admission.max_workers(),
            if parallel {
                admission.max_workers()
            } else {
                usize::from(admission.morsel_count() > 0)
            },
        );

        let (emitted, stopped) = if use_owned_typed {
            stream_owned_typed_numeric_nodes(
                self,
                items,
                label_id,
                lending_scan,
                context,
                execution_limit,
                emit,
            )?
        } else if context.store.is_out_of_core() {
            stream_owned_numeric_nodes(self, items, label_id, context, execution_limit, emit)?
        } else if parallel {
            stream_parallel_borrowed_numeric_nodes(
                self,
                items,
                label_id,
                target_rows,
                morsel_rows,
                use_lending.then_some(lending_scan),
                admission.max_workers(),
                pool.expect("parallel morsel execution requires a shared pool"),
                context,
                execution_limit,
                emit,
            )?
        } else if use_lending {
            stream_lending_numeric_nodes(
                self,
                items,
                label_id,
                lending_scan,
                context,
                execution_limit,
                emit,
            )?
        } else {
            stream_borrowed_numeric_nodes(self, items, label_id, context, execution_limit, emit)?
        };
        context
            .observer
            .record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: crate::store::ScanPruningStrategy::FullLabelScan,
                pruned: false,
                exact_empty: candidate_count == 0,
                candidate_count_before_pruning: candidate_count,
                pruned_candidate_count: 0,
                candidate_count_before_filter: candidate_count,
                output_count: emitted,
                filtered_out_count: candidate_count.saturating_sub(emitted),
            });
        Ok(if stopped {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    }
}

fn default_morsel_worker_count(morsel_count: usize, worker_ceiling: usize) -> usize {
    worker_ceiling
        .min(morsel_count / DEFAULT_MIN_MORSELS_PER_WORKER)
        .max(1)
}

struct PreparedNumericBatch {
    input_rows: usize,
    selected_rows: usize,
    output: BindingBatch,
}

enum PreparedNumericMorsel {
    Parallel(Vec<PreparedNumericBatch>),
    Serial,
}

#[allow(clippy::too_many_arguments)]
fn stream_parallel_borrowed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: crate::schema::LabelId,
    batch_rows: NonZeroUsize,
    morsel_rows: NonZeroUsize,
    lending_scan: Option<LendingNumericScan>,
    max_workers: usize,
    pool: SharedExecutorPool,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let requested_parallelism =
        NonZeroUsize::new(max_workers).expect("parallel execution has at least one worker");
    let bytes_per_worker = NonZeroUsize::new(
        context.memory.batch_payload_bytes.get().saturating_add(
            morsel_rows
                .get()
                .saturating_mul(std::mem::size_of::<&NodeRecord>()),
        ),
    )
    .expect("batch payload budget is non-zero");
    let memory_budget_bytes = NonZeroUsize::new(
        bytes_per_worker
            .get()
            .saturating_mul(requested_parallelism.get()),
    )
    .expect("parallel morsel memory budget is non-zero");
    let scheduler = SharedPoolMorselScheduler::new(pool);
    let mut nodes = context.store.scan_nodes(Some(label_id));
    let mut wave = Vec::with_capacity(morsel_rows.get().saturating_mul(max_workers));
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    loop {
        wave.clear();
        wave.extend(nodes.by_ref().take(wave.capacity()));
        if wave.is_empty() {
            break;
        }
        let admission = MorselAdmission::try_new(MorselAdmissionRequest {
            pipeline_id: PipelineId(0),
            input_rows: wave.len(),
            target_rows: morsel_rows,
            requested_parallelism,
            bytes_per_worker,
            memory_budget_bytes,
        })?;
        let outputs = if let Some(task_context) = context.task_context {
            scheduler.execute_with_context(&admission, task_context, |morsel| {
                prepare_parallel_numeric_morsel(
                    fragment,
                    items,
                    &wave[morsel.start_row..morsel.start_row + morsel.row_count],
                    batch_rows.get(),
                    context.memory.batch_payload_bytes.get(),
                    lending_scan,
                    Some(task_context),
                )
            })?
        } else {
            scheduler.execute(&admission, |morsel| {
                prepare_parallel_numeric_morsel(
                    fragment,
                    items,
                    &wave[morsel.start_row..morsel.start_row + morsel.row_count],
                    batch_rows.get(),
                    context.memory.batch_payload_bytes.get(),
                    lending_scan,
                    None,
                )
            })?
        };
        for (morsel, output) in admission.morsels().zip(outputs) {
            context.observer.record_morsels(1);
            match output {
                PreparedNumericMorsel::Parallel(batches) => {
                    for batch in batches {
                        stopped = batch_emitter.emit_prepared(batch)? == BatchControl::Stop;
                        if stopped || batch_emitter.limit_reached() {
                            stopped = true;
                            break;
                        }
                    }
                }
                PreparedNumericMorsel::Serial => {
                    let rows = &wave[morsel.start_row..morsel.start_row + morsel.row_count];
                    for batch in rows.chunks(batch_rows.get()) {
                        stopped =
                            batch_emitter.emit_nodes_without_morsel(batch)? == BatchControl::Stop;
                        if stopped || batch_emitter.limit_reached() {
                            stopped = true;
                            break;
                        }
                    }
                }
            }
            if stopped {
                break;
            }
        }
        if stopped || wave.len() < wave.capacity() {
            break;
        }
    }
    Ok((batch_emitter.emitted, stopped))
}

fn stream_lending_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: crate::schema::LabelId,
    scan: LendingNumericScan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut cursor = NumericNodeBatchCursor::new(
        context.store.scan_nodes(Some(label_id)),
        fragment,
        scan.batch_rows,
        scan.needs_node_ids,
    );
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    while let Some(batch) = cursor.next_batch()? {
        stopped = batch_emitter.emit_typed(batch)? == BatchControl::Stop;
        if stopped || batch_emitter.limit_reached() {
            stopped = true;
            break;
        }
    }
    Ok((batch_emitter.emitted, stopped))
}

fn stream_borrowed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: crate::schema::LabelId,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut nodes = Vec::with_capacity(context.memory.batch_rows.get());
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut stopped = false;
    for node in context.store.scan_nodes(Some(label_id)) {
        nodes.push(node);
        if nodes.len() == context.memory.batch_rows.get() {
            stopped = batch_emitter.emit_nodes(&nodes)? == BatchControl::Stop;
            nodes.clear();
        }
        if stopped || batch_emitter.limit_reached() {
            stopped = true;
            break;
        }
    }
    if !stopped && !nodes.is_empty() {
        stopped = batch_emitter.emit_nodes(&nodes)? == BatchControl::Stop;
    }
    Ok((batch_emitter.emitted, stopped))
}

fn stream_owned_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: crate::schema::LabelId,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut nodes = Vec::with_capacity(context.memory.batch_rows.get());
    let mut buffered_bytes = 0usize;
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut callback_error = None;
    let mut stopped = false;
    {
        let mut consume = |node: NodeRecord| {
            if stopped {
                return GraphScanControl::Stop;
            }
            let node_bytes = skein_executor::binding::node_memory_bytes(&node);
            if !nodes.is_empty()
                && buffered_bytes.saturating_add(node_bytes)
                    > context.memory.batch_payload_bytes.get()
            {
                match batch_emitter.emit_owned(&mut nodes) {
                    Ok(BatchControl::Continue) => buffered_bytes = 0,
                    Ok(BatchControl::Stop) => {
                        stopped = true;
                        return GraphScanControl::Stop;
                    }
                    Err(error) => {
                        callback_error = Some(error);
                        stopped = true;
                        return GraphScanControl::Stop;
                    }
                }
            }
            buffered_bytes = buffered_bytes.saturating_add(node_bytes);
            nodes.push(node);
            if nodes.len() == context.memory.batch_rows.get()
                || buffered_bytes >= context.memory.batch_payload_bytes.get()
            {
                match batch_emitter.emit_owned(&mut nodes) {
                    Ok(BatchControl::Continue) => buffered_bytes = 0,
                    Ok(BatchControl::Stop) => {
                        stopped = true;
                        return GraphScanControl::Stop;
                    }
                    Err(error) => {
                        callback_error = Some(error);
                        stopped = true;
                        return GraphScanControl::Stop;
                    }
                }
            }
            if batch_emitter.limit_reached() {
                stopped = true;
                GraphScanControl::Stop
            } else {
                GraphScanControl::Continue
            }
        };
        context
            .store
            .visit_nodes_owned(Some(label_id), &mut consume)?;
    }
    if let Some(error) = callback_error {
        return Err(error);
    }
    if !stopped && !nodes.is_empty() {
        stopped = batch_emitter.emit_owned(&mut nodes)? == BatchControl::Stop;
    }
    Ok((batch_emitter.emitted, stopped))
}

#[allow(clippy::too_many_arguments)]
fn stream_owned_typed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: crate::schema::LabelId,
    scan: LendingNumericScan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut buffer = OwnedNumericBatchBuffer::new(fragment, scan.batch_rows, scan.needs_node_ids);
    let mut batch_emitter = NumericBatchEmitter::new(
        fragment,
        items,
        execution_limit,
        context.task_context,
        context.observer,
        emit,
    );
    let mut callback_error = None;
    let mut stopped = false;
    {
        let mut consume = |node: NodeRecord| {
            if stopped {
                return GraphScanControl::Stop;
            }
            if let Err(error) = buffer.push_owned(node) {
                callback_error = Some(error);
                stopped = true;
                return GraphScanControl::Stop;
            }
            if buffer.is_full() {
                match batch_emitter.emit_typed(buffer.take_batch()) {
                    Ok(BatchControl::Continue) => buffer.clear(),
                    Ok(BatchControl::Stop) => {
                        stopped = true;
                        return GraphScanControl::Stop;
                    }
                    Err(error) => {
                        callback_error = Some(error);
                        stopped = true;
                        return GraphScanControl::Stop;
                    }
                }
            }
            if batch_emitter.limit_reached() {
                stopped = true;
                GraphScanControl::Stop
            } else {
                GraphScanControl::Continue
            }
        };
        context
            .store
            .visit_nodes_owned(Some(label_id), &mut consume)?;
    }
    if let Some(error) = callback_error {
        return Err(error);
    }
    if !stopped && !buffer.is_empty() {
        stopped = batch_emitter.emit_typed(buffer.take_batch())? == BatchControl::Stop;
    }
    Ok((batch_emitter.emitted, stopped))
}

impl<'plan, 'task, 'observer, 'emit> NumericBatchEmitter<'plan, 'task, 'observer, 'emit> {
    fn new(
        fragment: NumericFragment<'plan>,
        items: &'plan [Projection],
        execution_limit: ExecutionLimit,
        task_context: Option<&'task RuntimeTaskContext>,
        observer: &'observer QueryExecutionObserver,
        emit: &'emit mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Self {
        Self {
            fragment,
            items,
            emitted: 0,
            execution_limit,
            task_context,
            observer,
            emit,
            selected_rows: Vec::new(),
        }
    }

    fn limit_reached(&self) -> bool {
        self.execution_limit.is_reached(self.emitted)
    }

    fn emit_owned(&mut self, nodes: &mut Vec<NodeRecord>) -> Result<BatchControl> {
        let control = self.emit_nodes(nodes)?;
        nodes.clear();
        Ok(control)
    }

    fn emit_typed(&mut self, input: NumericNodeBatch<'_>) -> Result<BatchControl> {
        runtime_checkpoint(self.task_context)?;
        self.observer.record_morsels(1);
        let prepared =
            prepare_typed_batch(self.fragment, self.items, input, &mut self.selected_rows)?;
        self.emit_prepared(prepared)
    }

    fn emit_nodes<N: Borrow<NodeRecord>>(&mut self, input: &[N]) -> Result<BatchControl> {
        self.observer.record_morsels(1);
        self.emit_nodes_without_morsel(input)
    }

    fn emit_nodes_without_morsel<N: Borrow<NodeRecord>>(
        &mut self,
        input: &[N],
    ) -> Result<BatchControl> {
        let prepared = prepare_numeric_batch(self.fragment, self.items, input, self.task_context)?;
        self.emit_prepared(prepared)
    }

    fn emit_prepared(&mut self, mut prepared: PreparedNumericBatch) -> Result<BatchControl> {
        self.observer
            .record_columnar_batch(prepared.input_rows, prepared.selected_rows);
        let remaining = self
            .execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(self.emitted);
        if prepared.output.len() > remaining {
            prepared.output.truncate(remaining);
        }
        self.emit_output(prepared.output)
    }

    fn emit_output(&mut self, output: BindingBatch) -> Result<BatchControl> {
        self.emitted = self.emitted.saturating_add(output.len());
        runtime_checkpoint(self.task_context)?;
        if !output.is_empty() && (self.emit)(output)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        Ok(if self.limit_reached() {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    }
}

fn prepare_typed_batch(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    input: NumericNodeBatch<'_>,
    selected_rows: &mut Vec<u32>,
) -> Result<PreparedNumericBatch> {
    match input.values {
        lending::NumericBatchValues::Int(values) => select_int64_values_view(
            values,
            input.validity,
            fragment.op,
            fragment.expected,
            selected_rows,
        )?,
        lending::NumericBatchValues::Float(values) => select_float64_values_view(
            values,
            input.validity,
            fragment.op,
            fragment.expected,
            selected_rows,
        )?,
    }
    let selected_count = selected_rows.len();
    let mut output = Vec::with_capacity(selected_count);
    for row in selected_rows.iter().copied().map(|row| row as usize) {
        let mut values = BTreeMap::new();
        for item in items {
            let value = match &item.expression {
                ProjectionExpression::Id { .. } => Value::Int(
                    input
                        .node_ids
                        .expect("typed scan retains requested node ids")[row]
                        as i64,
                ),
                ProjectionExpression::Property { .. } => input.values.value(row),
                ProjectionExpression::Literal(value) => value.clone(),
                _ => unreachable!("typed projection eligibility checks expressions"),
            };
            insert_projected_value(&mut values, &item.name, value);
        }
        output.push(Binding::values(values));
    }
    Ok(PreparedNumericBatch {
        input_rows: input.input_rows,
        selected_rows: selected_count,
        output,
    })
}

fn prepare_numeric_batch<N: Borrow<NodeRecord>>(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    input: &[N],
    task_context: Option<&RuntimeTaskContext>,
) -> Result<PreparedNumericBatch> {
    runtime_checkpoint(task_context)?;
    let mut validity = ValidityBuilder::with_capacity(input.len());
    let selection = match fragment.property_type {
        crate::schema::PropertyType::Int => {
            let mut values = Vec::with_capacity(input.len());
            for node in input {
                let node = node.borrow();
                match node.properties.get(fragment.property) {
                    Some(Value::Int(value)) => {
                        values.push(*value);
                        validity.push(true);
                    }
                    Some(Value::Null) | None => {
                        values.push(0);
                        validity.push(false);
                    }
                    Some(value) => return Err(schema_value_mismatch(fragment, value)),
                }
            }
            filter_int64_values(
                &values,
                &validity.finish(),
                &Selection::all(input.len()),
                fragment.op,
                fragment.expected,
            )?
        }
        crate::schema::PropertyType::Float => {
            let mut values = Vec::with_capacity(input.len());
            for node in input {
                let node = node.borrow();
                match node.properties.get(fragment.property) {
                    Some(Value::Float(value)) => {
                        values.push(*value);
                        validity.push(true);
                    }
                    Some(Value::Null) | None => {
                        values.push(0.0);
                        validity.push(false);
                    }
                    Some(value) => return Err(schema_value_mismatch(fragment, value)),
                }
            }
            filter_float64_values(
                &values,
                &validity.finish(),
                &Selection::all(input.len()),
                fragment.op,
                fragment.expected,
            )?
        }
        _ => unreachable!("numeric fragment eligibility checks the property type"),
    };
    runtime_checkpoint(task_context)?;
    let selected_rows = selection.selected_count();
    let mut output = Vec::with_capacity(selected_rows);
    for row in selection.iter() {
        let node = input[row].borrow();
        let mut values = BTreeMap::new();
        for item in items {
            let value = match &item.expression {
                ProjectionExpression::Id { .. } => Value::Int(node.id.0 as i64),
                ProjectionExpression::Property { property, .. } => node
                    .properties
                    .get(property)
                    .cloned()
                    .unwrap_or(Value::Null),
                ProjectionExpression::Literal(value) => value.clone(),
                _ => unreachable!("columnar projection eligibility checks expressions"),
            };
            insert_projected_value(&mut values, &item.name, value);
        }
        output.push(Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        });
    }
    Ok(PreparedNumericBatch {
        input_rows: input.len(),
        selected_rows,
        output,
    })
}

fn prepare_parallel_numeric_morsel(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    input: &[&NodeRecord],
    batch_rows: usize,
    output_budget_bytes: usize,
    lending_scan: Option<LendingNumericScan>,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<PreparedNumericMorsel> {
    if let Some(scan) = lending_scan {
        return prepare_lending_numeric_morsel(
            fragment,
            items,
            input,
            scan,
            output_budget_bytes,
            task_context,
        );
    }
    prepare_numeric_morsel(
        fragment,
        items,
        input,
        batch_rows,
        output_budget_bytes,
        task_context,
    )
}

fn prepare_lending_numeric_morsel(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    input: &[&NodeRecord],
    scan: LendingNumericScan,
    output_budget_bytes: usize,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<PreparedNumericMorsel> {
    let mut cursor = NumericNodeBatchCursor::new(
        input.iter().copied(),
        fragment,
        scan.batch_rows,
        scan.needs_node_ids,
    );
    let mut output_bytes = 0usize;
    let mut batches = Vec::with_capacity(input.len().div_ceil(scan.batch_rows));
    let mut selected_rows = Vec::with_capacity(scan.batch_rows);
    while let Some(input) = cursor.next_batch()? {
        runtime_checkpoint(task_context)?;
        let batch = prepare_typed_batch(fragment, items, input, &mut selected_rows)?;
        let batch_bytes = batch.output.iter().fold(0usize, |total, binding| {
            total.saturating_add(binding_memory_bytes(binding))
        });
        output_bytes = output_bytes.saturating_add(batch_bytes);
        if output_bytes > output_budget_bytes {
            return Ok(PreparedNumericMorsel::Serial);
        }
        batches.push(batch);
    }
    Ok(PreparedNumericMorsel::Parallel(batches))
}

fn prepare_numeric_morsel<N: Borrow<NodeRecord>>(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    input: &[N],
    batch_rows: usize,
    output_budget_bytes: usize,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<PreparedNumericMorsel> {
    let mut output_bytes = 0usize;
    let mut batches = Vec::with_capacity(input.len().div_ceil(batch_rows));
    for rows in input.chunks(batch_rows) {
        let batch = prepare_numeric_batch(fragment, items, rows, task_context)?;
        let batch_bytes = batch.output.iter().fold(0usize, |total, binding| {
            total.saturating_add(binding_memory_bytes(binding))
        });
        output_bytes = output_bytes.saturating_add(batch_bytes);
        if output_bytes > output_budget_bytes {
            return Ok(PreparedNumericMorsel::Serial);
        }
        batches.push(batch);
    }
    Ok(PreparedNumericMorsel::Parallel(batches))
}

fn schema_value_mismatch(fragment: NumericFragment<'_>, value: &Value) -> SkeinError {
    SkeinError::Execution(format!(
        "columnar scan found value {value:?} that violates {:?} schema for {}.{}",
        fragment.property_type, fragment.label, fragment.property
    ))
}

#[cfg(test)]
mod tests {
    use super::default_morsel_worker_count;

    #[test]
    fn default_worker_count_requires_enough_work_per_worker() {
        assert_eq!(default_morsel_worker_count(0, 4), 1);
        assert_eq!(default_morsel_worker_count(4, 4), 1);
        assert_eq!(default_morsel_worker_count(8, 4), 2);
        assert_eq!(default_morsel_worker_count(15, 4), 3);
        assert_eq!(default_morsel_worker_count(16, 4), 4);
        assert_eq!(default_morsel_worker_count(32, 16), 8);
        assert_eq!(default_morsel_worker_count(64, 16), 16);
        assert_eq!(default_morsel_worker_count(128, 16), 16);
        assert_eq!(default_morsel_worker_count(64, 2), 2);
    }
}
