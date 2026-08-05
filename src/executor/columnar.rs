//! Root storage adapter for eligible vectorized read fragments.

use super::*;
use skein_executor::columnar::{
    filter_float64_values, filter_int64_values, NumericLiteral, Selection, ValidityBuilder,
};
use skein_executor::morsel::{admit_morsels, MorselAdmissionRequest, PipelineId};
use std::borrow::Borrow;

#[derive(Debug, Clone, Copy)]
struct NumericFragment<'a> {
    label: &'a str,
    property: &'a str,
    property_type: crate::schema::PropertyType,
    op: skein_plan::ComparisonOp,
    expected: NumericLiteral,
}

struct NumericBatchEmitter<'plan, 'task, 'emit> {
    fragment: NumericFragment<'plan>,
    items: &'plan [Projection],
    emitted: usize,
    execution_limit: ExecutionLimit,
    task_context: Option<&'task RuntimeTaskContext>,
    emit: &'emit mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
}

pub(super) fn try_stream_columnar_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    let fragment = eligible_numeric_fragment(items, input, context.catalog)?;
    Some(stream_numeric_fragment(
        fragment,
        items,
        context,
        execution_limit,
        emit,
    ))
}

fn eligible_numeric_fragment<'a>(
    items: &[Projection],
    input: &'a PhysicalPlan,
    catalog: &Catalog,
) -> Option<NumericFragment<'a>> {
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
    Some(NumericFragment {
        label,
        property,
        property_type: descriptor.value_type,
        op: *op,
        expected,
    })
}

fn stream_numeric_fragment(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let Some(label_id) = context.catalog.label_id(fragment.label) else {
        return Ok(BatchControl::Continue);
    };
    let candidate_count = context.store.node_count_for_label(Some(label_id));
    let admission = admit_morsels(MorselAdmissionRequest {
        pipeline_id: PipelineId(0),
        input_rows: candidate_count,
        target_rows: context.memory.batch_rows,
        requested_parallelism: NonZeroUsize::MIN,
        bytes_per_worker: context.memory.batch_payload_bytes,
        memory_budget_bytes: context.memory.batch_payload_bytes,
    })?;
    record_morsel_admission(
        admission.max_workers(),
        usize::from(admission.morsel_count() > 0),
    );

    let (emitted, stopped) = if context.store.is_out_of_core() {
        stream_owned_numeric_nodes(fragment, items, label_id, context, execution_limit, emit)?
    } else {
        stream_borrowed_numeric_nodes(fragment, items, label_id, context, execution_limit, emit)?
    };
    record_scan_pruning_report(ScanPruningReport {
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

fn stream_borrowed_numeric_nodes(
    fragment: NumericFragment<'_>,
    items: &[Projection],
    label_id: crate::schema::LabelId,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(usize, bool)> {
    let mut nodes = Vec::with_capacity(context.memory.batch_rows.get());
    let mut batch_emitter =
        NumericBatchEmitter::new(fragment, items, execution_limit, context.task_context, emit);
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
    let mut batch_emitter =
        NumericBatchEmitter::new(fragment, items, execution_limit, context.task_context, emit);
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

impl<'plan, 'task, 'emit> NumericBatchEmitter<'plan, 'task, 'emit> {
    fn new(
        fragment: NumericFragment<'plan>,
        items: &'plan [Projection],
        execution_limit: ExecutionLimit,
        task_context: Option<&'task RuntimeTaskContext>,
        emit: &'emit mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Self {
        Self {
            fragment,
            items,
            emitted: 0,
            execution_limit,
            task_context,
            emit,
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

    fn emit_nodes<N: Borrow<NodeRecord>>(&mut self, input: &[N]) -> Result<BatchControl> {
        runtime_checkpoint(self.task_context)?;
        let mut validity = ValidityBuilder::with_capacity(input.len());
        let selection = match self.fragment.property_type {
            crate::schema::PropertyType::Int => {
                let mut values = Vec::with_capacity(input.len());
                for node in input {
                    let node = node.borrow();
                    match node.properties.get(self.fragment.property) {
                        Some(Value::Int(value)) => {
                            values.push(*value);
                            validity.push(true);
                        }
                        Some(Value::Null) | None => {
                            values.push(0);
                            validity.push(false);
                        }
                        Some(value) => {
                            return Err(schema_value_mismatch(self.fragment, value));
                        }
                    }
                }
                filter_int64_values(
                    &values,
                    &validity.finish(),
                    &Selection::all(input.len()),
                    self.fragment.op,
                    self.fragment.expected,
                )?
            }
            crate::schema::PropertyType::Float => {
                let mut values = Vec::with_capacity(input.len());
                for node in input {
                    let node = node.borrow();
                    match node.properties.get(self.fragment.property) {
                        Some(Value::Float(value)) => {
                            values.push(*value);
                            validity.push(true);
                        }
                        Some(Value::Null) | None => {
                            values.push(0.0);
                            validity.push(false);
                        }
                        Some(value) => {
                            return Err(schema_value_mismatch(self.fragment, value));
                        }
                    }
                }
                filter_float64_values(
                    &values,
                    &validity.finish(),
                    &Selection::all(input.len()),
                    self.fragment.op,
                    self.fragment.expected,
                )?
            }
            _ => unreachable!("numeric fragment eligibility checks the property type"),
        };
        record_columnar_batch(input.len(), selection.selected_count());

        let remaining = self
            .execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(self.emitted);
        let mut output = Vec::with_capacity(selection.selected_count().min(remaining));
        for row in selection.iter().take(remaining) {
            let node = input[row].borrow();
            let mut values = BTreeMap::new();
            for item in self.items {
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

fn schema_value_mismatch(fragment: NumericFragment<'_>, value: &Value) -> SkeinError {
    SkeinError::Execution(format!(
        "columnar scan found value {value:?} that violates {:?} schema for {}.{}",
        fragment.property_type, fragment.label, fragment.property
    ))
}
