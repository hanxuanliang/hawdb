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
    let mut predicate = |binding: &Binding| match filter {
        Some((predicate, _)) => {
            evaluate_predicate(predicate, context.catalog, context.store, binding)
        }
        None => Ok(true),
    };
    skein_executor::scan::stream_node_scan_batches(
        NodeScanSpec {
            variable,
            label,
            property_filter: filter.map(|(_, filter)| filter),
        },
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget: context.memory.blocking_operator_bytes,
            batch_rows: context.memory.batch_rows.get(),
            task_context: context.task_context,
        },
        &mut predicate,
        &mut RootExecutionObserver,
        emit,
    )
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
    skein_executor::scan::stream_index_node_seek_batches(
        variable,
        label,
        property,
        values,
        NodeScanContext {
            catalog: context.catalog,
            store: context.store,
            execution_limit,
            memory_budget: context.memory.blocking_operator_bytes,
            batch_rows: context.memory.batch_rows.get(),
            task_context: context.task_context,
        },
        &mut RootExecutionObserver,
        emit,
    )
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
        batch.push(single_node_binding(variable, node));
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

pub(super) fn execute_node_scan_with_optional_filter(
    variable: &str,
    label: &str,
    filter: Option<(&Predicate, &PropertyFilter)>,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let mut predicate = |binding: &Binding| match filter {
        Some((predicate, _)) => evaluate_predicate(predicate, catalog, store, binding),
        None => Ok(true),
    };
    skein_executor::scan::execute_node_scan(
        NodeScanSpec {
            variable,
            label,
            property_filter: filter.map(|(_, filter)| filter),
        },
        NodeScanContext {
            catalog,
            store,
            execution_limit,
            memory_budget,
            batch_rows: 1,
            task_context: None,
        },
        &mut predicate,
        &mut RootExecutionObserver,
    )
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

pub(super) fn execute_node_column_lookup(
    spec: NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    skein_executor::scan::execute_node_column_lookup(
        spec,
        input,
        NodeScanContext {
            catalog,
            store,
            execution_limit,
            memory_budget,
            batch_rows: 1,
            task_context: None,
        },
        &mut RootExecutionObserver,
    )
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
            record_graph_expansion_state(&graph_expansion, rel_type, *min_hops, *max_hops, 0);
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
                graph_expansion.record_seed();
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
                    &mut RootExecutionObserver,
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
                    if execution_limit.is_reached(graph_expansion.returned_count()) {
                        return Ok(BatchControl::Stop);
                    }
                }
            }
            Ok(BatchControl::Continue)
        })?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        record_graph_expansion_state(
            &graph_expansion,
            rel_type,
            *min_hops,
            *max_hops,
            graph_expansion.returned_count(),
        );
        return Ok(BatchControl::Stop);
    }
    record_graph_expansion_state(
        &graph_expansion,
        rel_type,
        *min_hops,
        *max_hops,
        graph_expansion.returned_count(),
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
            record_graph_expansion_state(&graph_expansion, rel_type, *min_hops, *max_hops, 0);
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
            &mut RootExecutionObserver,
        )? {
            runtime_checkpoint(context.task_context)?;
            if !graph_expansion.try_push(
                &mut output,
                candidate.binding,
                candidate.target_id,
                candidate.hop,
            ) || execution_limit.is_reached(output.len())
            {
                record_graph_expansion_state(
                    &graph_expansion,
                    rel_type,
                    *min_hops,
                    *max_hops,
                    output.len(),
                );
                return Ok(output);
            }
        }
    }
    record_graph_expansion_state(
        &graph_expansion,
        rel_type,
        *min_hops,
        *max_hops,
        output.len(),
    );
    Ok(output)
}

fn record_graph_expansion_state(
    state: &GraphExpansionExecutionState,
    rel_type: &str,
    min_hops: usize,
    max_hops: usize,
    returned_count: usize,
) {
    if let Some(report) = state.report(rel_type, min_hops, max_hops, returned_count) {
        record_graph_expansion_report(report);
    }
}
