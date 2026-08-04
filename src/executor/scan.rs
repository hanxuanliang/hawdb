//! Node, index, source-segment, and adjacency scan execution.

use super::*;

pub(super) fn stream_node_scan_batches(
    variable: &str,
    label: &str,
    filter: Option<(&Predicate, &PropertyFilter)>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    let batch_rows = memory.batch_rows.get();
    let exact_label = exact_scan_label_id(catalog, label);
    let exact_label_id = exact_label.flatten();
    if !store.is_out_of_core()
        && let Some(label_id) = exact_label
        && store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= memory.blocking_operator_bytes.get()
    {
        let scan = store.scan_nodes_with_filter_pruning(label_id, filter.map(|(_, filter)| filter));
        record_scan_pruning_report(scan.report.clone());
        let mut batch = Vec::with_capacity(batch_rows);
        let mut emitted = 0usize;
        for node in scan.nodes {
            runtime_checkpoint(context.task_context)?;
            let binding = node_binding(variable, node.clone());
            if let Some((predicate, _)) = filter
                && !evaluate_predicate(predicate, catalog, store, &binding)?
            {
                continue;
            }
            batch.push(binding);
            emitted = emitted.saturating_add(1);
            if batch.len() == batch_rows
                && emit(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(batch_rows),
                ))? == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
            if execution_limit.is_reached(emitted) {
                break;
            }
        }
        if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        return Ok(if execution_limit.is_reached(emitted) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        });
    }
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut batch = Vec::with_capacity(batch_rows);
    let mut emitted = 0usize;
    let mut callback_error = None;
    let control = store.visit_nodes_owned(exact_label_id, |node| {
        if callback_error.is_some() {
            return GraphScanControl::Stop;
        }
        if let Err(error) = runtime_checkpoint(context.task_context) {
            callback_error = Some(error);
            return GraphScanControl::Stop;
        }
        if exact_label.is_none() && !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return GraphScanControl::Continue;
        }
        if filter
            .map(|(_, property_filter)| node_matches_property_filter(&node, property_filter))
            .is_some_and(|matches| !matches)
        {
            return GraphScanControl::Continue;
        }
        let binding = node_binding(variable, node);
        if let Some((predicate, _)) = filter {
            match evaluate_predicate(predicate, catalog, store, &binding) {
                Ok(true) => {}
                Ok(false) => return GraphScanControl::Continue,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        batch.push(binding);
        emitted = emitted.saturating_add(1);
        if batch.len() == batch_rows {
            match emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            )) {
                Ok(BatchControl::Continue) => {}
                Ok(BatchControl::Stop) => return GraphScanControl::Stop,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if execution_limit.is_reached(emitted) {
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    let candidate_count = store.node_count_for_label(exact_label_id);
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: exact_label_id,
        rel_type_id: None,
        strategy: ScanPruningStrategy::FullLabelScan,
        pruned: false,
        exact_empty: candidate_count == 0,
        candidate_count_before_pruning: candidate_count,
        pruned_candidate_count: 0,
        candidate_count_before_filter: candidate_count,
        output_count: emitted,
        filtered_out_count: candidate_count.saturating_sub(emitted),
    });
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == GraphScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

pub(super) fn stream_index_node_seek_batches(
    variable: &str,
    label: &str,
    property: &str,
    values: &[Value],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    let batch_rows = memory.batch_rows.get();
    let Some(label_id) = catalog.label_id(label) else {
        return Ok(BatchControl::Continue);
    };
    let matched = std::cell::Cell::new(0usize);
    let control =
        stream_visited_node_batches(variable, batch_rows, execution_limit, emit, |consumer| {
            store.visit_nodes_by_property_owned(label_id, property, values, |node| {
                matched.set(matched.get().saturating_add(1));
                consumer(node)
            })
        })?;
    let matched = matched.get();
    let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: Some(label_id),
        rel_type_id: None,
        strategy: if values.len() == 1 {
            ScanPruningStrategy::PropertyEq {
                property: property.to_string(),
            }
        } else {
            ScanPruningStrategy::PropertyIn {
                property: property.to_string(),
            }
        },
        pruned: true,
        exact_empty: matched == 0,
        candidate_count_before_pruning,
        pruned_candidate_count: candidate_count_before_pruning.saturating_sub(matched),
        candidate_count_before_filter: matched,
        output_count: matched.min(execution_limit.output_rows.unwrap_or(usize::MAX)),
        filtered_out_count: 0,
    });
    Ok(control)
}

pub(super) fn stream_visited_node_batches(
    variable: &str,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    visit: impl FnOnce(&mut dyn FnMut(NodeRecord) -> GraphScanControl) -> Result<GraphScanControl>,
) -> Result<BatchControl> {
    let mut batch = Vec::with_capacity(batch_rows);
    let mut emitted = 0usize;
    let mut callback_error = None;
    let mut consumer = |node| {
        batch.push(node_binding(variable, node));
        emitted = emitted.saturating_add(1);
        if batch.len() == batch_rows {
            match emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            )) {
                Ok(BatchControl::Continue) => {}
                Ok(BatchControl::Stop) => return GraphScanControl::Stop,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if execution_limit.is_reached(emitted) {
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    };
    let control = visit(&mut consumer)?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == GraphScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

fn node_binding(variable: &str, node: NodeRecord) -> Binding {
    Binding {
        values: BTreeMap::new(),
        nodes: BTreeMap::from([(variable.to_string(), node)]),
        relationships: BTreeMap::new(),
    }
}

pub(super) fn emit_owned_binding_batches(
    bindings: Vec<Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    emit_binding_iterator(bindings, batch_rows, emit)
}

pub(super) fn emit_binding_iterator(
    bindings: impl IntoIterator<Item = Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut batch = Vec::with_capacity(batch_rows);
    for binding in bindings {
        batch.push(binding);
        if batch.len() == batch_rows
            && emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            ))? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

pub(super) fn execute_node_scan_with_optional_filter(
    variable: &str,
    label: &str,
    filter: Option<(&Predicate, &PropertyFilter)>,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let exact_label = exact_scan_label_id(catalog, label);
    let exact_label_id = exact_label.flatten();
    if !store.is_out_of_core()
        && let Some(label_id) = exact_label
        && store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= memory_budget.get()
    {
        let scan = store.scan_nodes_with_filter_pruning(label_id, filter.map(|(_, filter)| filter));
        record_scan_pruning_report(scan.report.clone());
        let mut output = Vec::new();
        let mut tracker = OperatorMemoryTracker::new(memory_budget);
        for node in scan.nodes {
            let binding = node_binding(variable, node.clone());
            if let Some((predicate, _)) = filter
                && !evaluate_predicate(predicate, catalog, store, &binding)?
            {
                continue;
            }
            push_bounded_operator_binding("NodeScanExec", &mut output, binding, &mut tracker)?;
            if execution_limit.is_reached(output.len()) {
                break;
            }
        }
        return Ok(output);
    }
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    let mut callback_error = None;
    store.visit_nodes_owned(exact_label_id, |node| {
        if callback_error.is_some() {
            return GraphScanControl::Stop;
        }
        if exact_label.is_none() && !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return GraphScanControl::Continue;
        }
        if filter
            .map(|(_, property_filter)| node_matches_property_filter(&node, property_filter))
            .is_some_and(|matches| !matches)
        {
            return GraphScanControl::Continue;
        }
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        };
        if let Some((predicate, _)) = filter {
            match evaluate_predicate(predicate, catalog, store, &binding) {
                Ok(true) => {}
                Ok(false) => return GraphScanControl::Continue,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if let Err(error) =
            push_bounded_operator_binding("NodeScanExec", &mut output, binding, &mut tracker)
        {
            callback_error = Some(error);
            return GraphScanControl::Stop;
        }
        if execution_limit.is_reached(output.len()) {
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    let candidate_count = store.node_count_for_label(exact_label_id);
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: exact_label_id,
        rel_type_id: None,
        strategy: ScanPruningStrategy::FullLabelScan,
        pruned: false,
        exact_empty: candidate_count == 0,
        candidate_count_before_pruning: candidate_count,
        pruned_candidate_count: 0,
        candidate_count_before_filter: candidate_count,
        output_count: output.len(),
        filtered_out_count: candidate_count.saturating_sub(output.len()),
    });
    Ok(output)
}

pub(super) fn execute_source_segment_scan(
    variable: &str,
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Binding>> {
    runtime_checkpoint(task_context)?;
    let Some(storage_predicate) = source_storage_scan_predicate(predicate, variable) else {
        return execute_node_scan_with_optional_filter(
            variable,
            "Source",
            None,
            catalog,
            store,
            execution_limit,
            memory.blocking_operator_bytes,
        );
    };
    let io_depth = NonZeroUsize::new(SOURCE_SEGMENT_SCAN_IO_DEPTH)
        .expect("source segment scan I/O depth is non-zero");
    let max_coalesced_bytes = NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES)
        .expect("source segment scan coalesced range limit is non-zero");
    let max_wave_bytes = NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES)
        .expect("source segment scan wave byte limit is non-zero");
    let read = store.read_published_source_scan_candidates_bounded(
        &storage_predicate,
        io_depth,
        max_coalesced_bytes,
        max_wave_bytes,
        memory.blocking_operator_bytes,
        task_context,
    );
    runtime_checkpoint(task_context)?;
    let rows = match read {
        Ok(SourceScanCandidateRead::Rows {
            skipped_segment_count,
            rows,
            ..
        }) => {
            let source_count = catalog
                .label_id("Source")
                .map(|label_id| store.node_count_for_label(Some(label_id)))
                .unwrap_or_default();
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: catalog.label_id("Source"),
                rel_type_id: None,
                strategy: source_scan_pruning_strategy(&storage_predicate),
                pruned: skipped_segment_count > 0 || rows.len() < source_count,
                exact_empty: rows.is_empty(),
                candidate_count_before_pruning: source_count,
                pruned_candidate_count: source_count.saturating_sub(rows.len()),
                candidate_count_before_filter: rows.len(),
                output_count: rows
                    .len()
                    .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                filtered_out_count: 0,
            });
            rows
        }
        Err(error @ SkeinError::Execution(_)) => return Err(error),
        Ok(SourceScanCandidateRead::Fallback(_)) | Err(_) => {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
                memory.blocking_operator_bytes,
            );
        }
    };
    let source_label_id = catalog.label_id("Source");
    let mut bindings = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    for row in rows {
        let Some(node) = store.node_owned(NodeId(row.node_id))? else {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
                memory.blocking_operator_bytes,
            );
        };
        if source_label_id.is_none_or(|label_id| !node.labels.contains(&label_id))
            || node.properties != row.properties
        {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
                memory.blocking_operator_bytes,
            );
        }
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        };
        push_bounded_operator_binding("SourceSegmentScan", &mut bindings, binding, &mut tracker)?;
        if execution_limit.is_reached(bindings.len()) {
            break;
        }
    }
    Ok(bindings)
}

fn source_scan_pruning_strategy(predicate: &ScanPredicate) -> ScanPruningStrategy {
    match predicate {
        ScanPredicate::False => ScanPruningStrategy::Empty,
        ScanPredicate::Eq { property, .. } => ScanPruningStrategy::PropertyEq {
            property: property.clone(),
        },
        ScanPredicate::In { property, .. } => ScanPruningStrategy::PropertyIn {
            property: property.clone(),
        },
        ScanPredicate::Range { property, .. } => ScanPruningStrategy::PropertyRange {
            property: property.clone(),
        },
        ScanPredicate::IsNull { property } | ScanPredicate::IsMissing { property } => {
            ScanPruningStrategy::PropertyMissingOrNull {
                property: property.clone(),
            }
        }
        ScanPredicate::Exists { property } => ScanPruningStrategy::PropertyExists {
            property: property.clone(),
        },
        ScanPredicate::Or(_) => ScanPruningStrategy::OrUnion,
        ScanPredicate::And(predicates) => predicates
            .iter()
            .map(source_scan_pruning_strategy)
            .find(|strategy| !matches!(strategy, ScanPruningStrategy::FullLabelScan))
            .unwrap_or(ScanPruningStrategy::FullLabelScan),
        ScanPredicate::True => ScanPruningStrategy::FullLabelScan,
    }
}

fn source_storage_scan_predicate(predicate: &Predicate, variable: &str) -> Option<ScanPredicate> {
    match predicate {
        Predicate::And(predicates) => {
            let predicates = predicates
                .iter()
                .filter_map(|predicate| source_storage_scan_predicate(predicate, variable))
                .collect::<Vec<_>>();
            match predicates.len() {
                0 => None,
                1 => predicates.into_iter().next(),
                _ => Some(ScanPredicate::And(predicates)),
            }
        }
        Predicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| source_storage_scan_predicate(predicate, variable))
            .collect::<Option<Vec<_>>>()
            .and_then(|predicates| {
                (!predicates.is_empty()).then_some(ScanPredicate::Or(predicates))
            }),
        Predicate::PropertyEq {
            variable: candidate,
            property,
            value,
        } if candidate == variable => Some(ScanPredicate::Eq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyIn {
            variable: candidate,
            property,
            values,
        } if candidate == variable => Some(ScanPredicate::In {
            property: property.clone(),
            values: values.clone(),
        }),
        Predicate::PropertyCompare {
            variable: candidate,
            property,
            op,
            value,
        } if candidate == variable => {
            let bound = RangeBound {
                value: value.clone(),
                inclusive: matches!(op, ComparisonOp::Gte | ComparisonOp::Lte),
            };
            let (lower, upper) = match op {
                ComparisonOp::Gt | ComparisonOp::Gte => (Some(bound), None),
                ComparisonOp::Lt | ComparisonOp::Lte => (None, Some(bound)),
            };
            Some(ScanPredicate::Range {
                property: property.clone(),
                lower,
                upper,
            })
        }
        Predicate::PropertyIsNull {
            variable: candidate,
            property,
        } if candidate == variable => Some(ScanPredicate::Or(vec![
            ScanPredicate::IsNull {
                property: property.clone(),
            },
            ScanPredicate::IsMissing {
                property: property.clone(),
            },
        ])),
        _ => None,
    }
}

fn exact_scan_label_id(catalog: &Catalog, label: &str) -> Option<Option<crate::schema::LabelId>> {
    if label.is_empty() {
        return Some(None);
    }
    if label.contains(':') {
        return None;
    }
    catalog.label_id(label).map(Some)
}

pub(super) fn single_node_binding(variable: &str, node: NodeRecord) -> Binding {
    Binding {
        values: BTreeMap::new(),
        nodes: BTreeMap::from([(variable.to_string(), node)]),
        relationships: BTreeMap::new(),
    }
}

pub(super) fn execute_node_column_lookup(
    spec: NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    if let Some(Some(label_id)) = exact_scan_label_id(catalog, spec.label) {
        return execute_indexed_node_column_lookup(
            &spec,
            input,
            label_id,
            store,
            execution_limit,
            memory_budget,
        );
    }

    let label_ids = label_ids_for_pattern(catalog, spec.label);
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for binding in input {
        let expected = binding.values.get(spec.column).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing column '{}' during node column lookup",
                spec.column
            ))
        })?;
        let mut matched = false;
        let mut callback_error = None;
        store.visit_nodes_owned(None, |node| {
            if node_matches_label_pattern(&node, label_ids.as_deref())
                && node.properties.get(spec.property) == Some(expected)
            {
                let mut next = binding.clone();
                next.nodes.insert(spec.variable.to_string(), node);
                if let Err(error) = push_bounded_operator_binding(
                    "NodeColumnLookupExec",
                    &mut output,
                    next,
                    &mut tracker,
                ) {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
                matched = true;
                if execution_limit.is_reached(output.len()) {
                    return GraphScanControl::Stop;
                }
            }
            GraphScanControl::Continue
        })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        if execution_limit.is_reached(output.len()) {
            return Ok(output);
        }
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            push_bounded_operator_binding("NodeColumnLookupExec", &mut output, next, &mut tracker)?;
            if execution_limit.is_reached(output.len()) {
                return Ok(output);
            }
        }
    }
    Ok(output)
}

fn execute_indexed_node_column_lookup(
    spec: &NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    label_id: crate::schema::LabelId,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let mut lookup_values = BTreeSet::new();
    for binding in &input {
        let expected = binding.values.get(spec.column).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing column '{}' during node column lookup",
                spec.column
            ))
        })?;
        lookup_values.insert(expected.clone());
    }

    let mut unique_candidate_ids = BTreeSet::new();
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for binding in input {
        let expected = binding
            .values
            .get(spec.column)
            .expect("lookup column was validated before index lookup")
            .clone();
        let mut matched = false;
        let mut callback_error = None;
        store.visit_nodes_by_property_owned(
            label_id,
            spec.property,
            std::slice::from_ref(&expected),
            |node| {
                unique_candidate_ids.insert(node.id);
                let mut next = binding.clone();
                next.nodes.insert(spec.variable.to_string(), node);
                if let Err(error) = push_bounded_operator_binding(
                    "NodeColumnLookupExec",
                    &mut output,
                    next,
                    &mut tracker,
                ) {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
                matched = true;
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            },
        )?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        if execution_limit.is_reached(output.len()) {
            record_node_column_lookup_scan_pruning_report(
                label_id,
                spec.property,
                lookup_values.len(),
                unique_candidate_ids.len(),
                output.len(),
                store,
            );
            return Ok(output);
        }
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            push_bounded_operator_binding("NodeColumnLookupExec", &mut output, next, &mut tracker)?;
            if execution_limit.is_reached(output.len()) {
                record_node_column_lookup_scan_pruning_report(
                    label_id,
                    spec.property,
                    lookup_values.len(),
                    unique_candidate_ids.len(),
                    output.len(),
                    store,
                );
                return Ok(output);
            }
        }
    }

    record_node_column_lookup_scan_pruning_report(
        label_id,
        spec.property,
        lookup_values.len(),
        unique_candidate_ids.len(),
        output.len(),
        store,
    );
    Ok(output)
}

fn record_node_column_lookup_scan_pruning_report(
    label_id: crate::schema::LabelId,
    property: &str,
    lookup_value_count: usize,
    candidate_count_before_filter: usize,
    output_count: usize,
    store: &GraphStore,
) {
    let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: Some(label_id),
        rel_type_id: None,
        strategy: if lookup_value_count == 0 {
            ScanPruningStrategy::Empty
        } else if lookup_value_count == 1 {
            ScanPruningStrategy::PropertyEq {
                property: property.to_string(),
            }
        } else {
            ScanPruningStrategy::PropertyIn {
                property: property.to_string(),
            }
        },
        pruned: true,
        exact_empty: candidate_count_before_filter == 0,
        candidate_count_before_pruning,
        pruned_candidate_count: candidate_count_before_pruning
            .saturating_sub(candidate_count_before_filter),
        candidate_count_before_filter,
        output_count,
        filtered_out_count: 0,
    });
}

#[derive(Default)]
pub(super) struct AdjacencyExpandFilters<'a> {
    pub(super) relationship_scan_filter: Option<&'a PropertyFilter>,
    pub(super) target_scan_filter: Option<&'a PropertyFilter>,
}

struct ExpandedBinding {
    binding: Binding,
    target_id: Option<NodeId>,
    hop: usize,
}

struct AdjacencyExpandSpec<'a> {
    source_variable: &'a str,
    rel_variable: Option<&'a str>,
    rel_properties: &'a BTreeMap<String, Value>,
    direction: RelationshipDirection,
    target_variable: &'a str,
    min_hops: usize,
    max_hops: usize,
    optional: bool,
}

#[allow(clippy::too_many_arguments)]
fn expand_binding(
    binding: Binding,
    spec: AdjacencyExpandSpec<'_>,
    rel_type_id: Option<crate::schema::RelTypeId>,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    filters: &AdjacencyExpandFilters<'_>,
    store: &GraphStore,
    memory_budget_bytes: usize,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<ExpandedBinding>> {
    runtime_checkpoint(task_context)?;
    let source = binding.nodes.get(spec.source_variable).ok_or_else(|| {
        SkeinError::Execution(format!(
            "missing variable '{}' during expand",
            spec.source_variable
        ))
    })?;
    let bound_target_id = binding.nodes.get(spec.target_variable).map(|node| node.id);
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    if spec.rel_variable.is_some()
        || !spec.rel_properties.is_empty()
        || filters.relationship_scan_filter.is_some()
        || spec.direction != RelationshipDirection::Outgoing
    {
        for (relationship, target) in one_hop_relationships_with_budget(
            store,
            source.id,
            rel_type_id,
            target_label_ids,
            spec.rel_properties,
            filters.relationship_scan_filter,
            spec.direction,
            memory_budget_bytes,
        )? {
            runtime_checkpoint(task_context)?;
            if bound_target_id.is_some_and(|node_id| node_id != target.id)
                || filters
                    .target_scan_filter
                    .is_some_and(|filter| !node_matches_property_filter(&target, filter))
            {
                continue;
            }
            let mut nodes = binding.nodes.clone();
            nodes.insert(spec.target_variable.to_string(), target.clone());
            let mut relationships = binding.relationships.clone();
            if let Some(rel_variable) = spec.rel_variable {
                relationships.insert(rel_variable.to_string(), relationship.clone());
            }
            let expanded = ExpandedBinding {
                binding: Binding {
                    values: binding.values.clone(),
                    nodes,
                    relationships,
                },
                target_id: Some(target.id),
                hop: 1,
            };
            admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
            output.push(expanded);
        }
    } else {
        for (target, hop) in bounded_expand_targets(
            store,
            source.id,
            rel_type_id.expect("typed bounded expand checked by planner"),
            target_label_ids,
            spec.min_hops,
            spec.max_hops,
            memory_budget_bytes,
        )? {
            runtime_checkpoint(task_context)?;
            if bound_target_id.is_some_and(|node_id| node_id != target.id)
                || filters
                    .target_scan_filter
                    .is_some_and(|filter| !node_matches_property_filter(&target, filter))
            {
                continue;
            }
            let mut nodes = binding.nodes.clone();
            nodes.insert(spec.target_variable.to_string(), target.clone());
            let expanded = ExpandedBinding {
                binding: Binding {
                    values: binding.values.clone(),
                    nodes,
                    relationships: binding.relationships.clone(),
                },
                target_id: Some(target.id),
                hop,
            };
            admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
            output.push(expanded);
        }
    }
    if spec.optional && output.is_empty() {
        let mut nodes = binding.nodes;
        nodes.insert(spec.target_variable.to_string(), null_lookup_node());
        let expanded = ExpandedBinding {
            binding: Binding {
                values: binding.values,
                nodes,
                relationships: binding.relationships,
            },
            target_id: None,
            hop: 0,
        };
        admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
        output.push(expanded);
    }
    Ok(output)
}

fn admit_expanded_binding(
    expanded: &ExpandedBinding,
    used_bytes: &mut usize,
    memory_budget_bytes: usize,
) -> Result<()> {
    let bytes = binding_memory_bytes(&expanded.binding);
    if bytes > memory_budget_bytes || used_bytes.saturating_add(bytes) > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "AdjacencyExpandExec seed state exceeds blocking_operator_bytes {memory_budget_bytes}"
        )));
    }
    *used_bytes = used_bytes.saturating_add(bytes);
    Ok(())
}

pub(super) fn stream_filtered_adjacency_expand_batches(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    predicate: &Predicate,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext { catalog, store, .. } = context;
    let mut emitted = 0usize;
    stream_adjacency_expand_batches(
        plan,
        input,
        context,
        execution_limit,
        filters,
        &mut |batch| {
            let remaining = execution_limit
                .output_rows
                .unwrap_or(usize::MAX)
                .saturating_sub(emitted);
            if remaining == 0 {
                return Ok(BatchControl::Stop);
            }
            let mut filtered = Vec::with_capacity(batch.len().min(remaining));
            for binding in batch {
                if evaluate_predicate(predicate, catalog, store, &binding)? {
                    filtered.push(binding);
                    if filtered.len() == remaining {
                        break;
                    }
                }
            }
            emitted = emitted.saturating_add(filtered.len());
            if !filtered.is_empty() && emit(filtered)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(if execution_limit.is_reached(emitted) {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            })
        },
    )
}

pub(super) fn stream_adjacency_expand_batches(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    let PhysicalPlan::AdjacencyExpandExec {
        source_variable,
        rel_variable,
        rel_type,
        rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        graph_budget,
        ..
    } = plan
    else {
        return Err(SkeinError::Execution(
            "expected adjacency expand plan".to_string(),
        ));
    };
    let mut graph_expansion =
        GraphExpansionExecutionState::new(*graph_budget, 0, current_vector_rerank_count());
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            graph_expansion.record(rel_type, *min_hops, *max_hops, 0);
            return Ok(BatchControl::Continue);
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    let batch_rows = memory.batch_rows.get();
    let mut output = Vec::with_capacity(batch_rows);
    let control =
        execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
            runtime_checkpoint(context.task_context)?;
            for binding in batch {
                runtime_checkpoint(context.task_context)?;
                graph_expansion.seed_count = graph_expansion.seed_count.saturating_add(1);
                for candidate in expand_binding(
                    binding,
                    AdjacencyExpandSpec {
                        source_variable,
                        rel_variable: rel_variable.as_deref(),
                        rel_properties,
                        direction: *direction,
                        target_variable,
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        optional: *optional,
                    },
                    rel_type_id,
                    target_label_ids.as_deref(),
                    &filters,
                    store,
                    memory.blocking_operator_bytes.get(),
                    context.task_context,
                )? {
                    runtime_checkpoint(context.task_context)?;
                    if !graph_expansion.try_push(
                        &mut output,
                        candidate.binding,
                        candidate.target_id,
                        candidate.hop,
                    ) {
                        return Ok(BatchControl::Stop);
                    }
                    if output.len() == batch_rows
                        && emit(std::mem::replace(
                            &mut output,
                            Vec::with_capacity(batch_rows),
                        ))? == BatchControl::Stop
                    {
                        return Ok(BatchControl::Stop);
                    }
                    if execution_limit.is_reached(graph_expansion.returned_count) {
                        return Ok(BatchControl::Stop);
                    }
                }
            }
            Ok(BatchControl::Continue)
        })?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        graph_expansion.record(
            rel_type,
            *min_hops,
            *max_hops,
            graph_expansion.returned_count,
        );
        return Ok(BatchControl::Stop);
    }
    graph_expansion.record(
        rel_type,
        *min_hops,
        *max_hops,
        graph_expansion.returned_count,
    );
    Ok(control)
}

pub(super) fn execute_adjacency_expand(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
) -> Result<Vec<Binding>> {
    let PhysicalPlan::AdjacencyExpandExec {
        source_variable,
        source_label: _,
        rel_variable,
        rel_type,
        rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        graph_budget,
        ..
    } = plan
    else {
        return Err(SkeinError::Execution(
            "expected adjacency expand plan".to_string(),
        ));
    };

    let input = execute_child_bindings(input, catalog, store, context)?;
    runtime_checkpoint(context.task_context)?;
    let mut graph_expansion = GraphExpansionExecutionState::new(
        *graph_budget,
        input.len(),
        current_vector_rerank_count(),
    );
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            graph_expansion.record(rel_type, *min_hops, *max_hops, 0);
            return Ok(Vec::new());
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    let mut output = Vec::new();
    for binding in input {
        runtime_checkpoint(context.task_context)?;
        for candidate in expand_binding(
            binding,
            AdjacencyExpandSpec {
                source_variable,
                rel_variable: rel_variable.as_deref(),
                rel_properties,
                direction: *direction,
                target_variable,
                min_hops: *min_hops,
                max_hops: *max_hops,
                optional: *optional,
            },
            rel_type_id,
            target_label_ids.as_deref(),
            &filters,
            store,
            context.memory.blocking_operator_bytes.get(),
            context.task_context,
        )? {
            runtime_checkpoint(context.task_context)?;
            if !graph_expansion.try_push(
                &mut output,
                candidate.binding,
                candidate.target_id,
                candidate.hop,
            ) || execution_limit.is_reached(output.len())
            {
                graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
                return Ok(output);
            }
        }
    }
    graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
    Ok(output)
}

pub(super) struct GraphExpansionExecutionState {
    budget: Option<skein_plan::GraphExpansionBudget>,
    seed_count: usize,
    expanded_nodes: BTreeSet<NodeId>,
    expanded_edge_count: usize,
    reranked_seed_count: usize,
    payload_bytes_used: usize,
    returned_count: usize,
    pub(super) truncation_reason: Option<skein_executor::GraphExpansionTruncationReason>,
}

impl GraphExpansionExecutionState {
    pub(super) fn new(
        budget: Option<skein_plan::GraphExpansionBudget>,
        seed_count: usize,
        reranked_seed_count: usize,
    ) -> Self {
        Self {
            budget,
            seed_count,
            expanded_nodes: BTreeSet::new(),
            expanded_edge_count: 0,
            reranked_seed_count,
            payload_bytes_used: 0,
            returned_count: 0,
            truncation_reason: None,
        }
    }

    pub(super) fn try_push(
        &mut self,
        output: &mut Vec<Binding>,
        candidate: Binding,
        target_id: Option<NodeId>,
        hop: usize,
    ) -> bool {
        let Some(budget) = self.budget else {
            self.returned_count = self.returned_count.saturating_add(1);
            output.push(candidate);
            return true;
        };
        if self.returned_count >= budget.candidate_limit {
            self.truncation_reason =
                Some(skein_executor::GraphExpansionTruncationReason::CandidateLimit);
            return false;
        }
        let candidate_bytes = binding_payload_bytes(&candidate);
        if self.payload_bytes_used.saturating_add(candidate_bytes) > budget.payload_byte_limit {
            self.truncation_reason =
                Some(skein_executor::GraphExpansionTruncationReason::PayloadByteLimit);
            return false;
        }
        self.payload_bytes_used = self.payload_bytes_used.saturating_add(candidate_bytes);
        self.returned_count = self.returned_count.saturating_add(1);
        if let Some(target_id) = target_id {
            self.expanded_nodes.insert(target_id);
        }
        self.expanded_edge_count = self.expanded_edge_count.saturating_add(hop);
        output.push(candidate);
        true
    }

    fn record(&self, rel_type: &str, min_hops: usize, max_hops: usize, returned_count: usize) {
        let Some(budget) = self.budget else {
            return;
        };
        record_graph_expansion_report(skein_executor::GraphExpansionExecutionReport {
            seed_count: self.seed_count,
            expanded_node_count: self.expanded_nodes.len(),
            expanded_edge_count: self.expanded_edge_count,
            relation_types: if rel_type.is_empty() {
                Vec::new()
            } else {
                vec![rel_type.to_string()]
            },
            min_hops,
            max_hops,
            reranked_seed_count: self.reranked_seed_count,
            candidate_limit: budget.candidate_limit,
            payload_byte_limit: budget.payload_byte_limit,
            payload_bytes_used: self.payload_bytes_used,
            returned_count,
            truncation_reason: self.truncation_reason,
        });
    }
}
