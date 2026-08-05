//! Streaming batch orchestration and pipeline dispatch.

use super::*;

fn stream_node_column_lookup_batches(
    spec: NodeColumnLookupSpec<'_>,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut output = Vec::with_capacity(context.memory.batch_rows.get());
    let mut emitted = 0usize;
    execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
        let remaining = execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(emitted);
        if remaining == 0 {
            return Ok(BatchControl::Stop);
        }
        let bindings = execute_node_column_lookup(
            spec,
            batch,
            context.catalog,
            context.store,
            ExecutionLimit {
                output_rows: Some(remaining),
            },
            context.memory.blocking_operator_bytes,
            context.observer,
        )?;
        for binding in bindings {
            output.push(binding);
            emitted = emitted.saturating_add(1);
            if output.len() == context.memory.batch_rows.get()
                && emit(std::mem::replace(
                    &mut output,
                    Vec::with_capacity(context.memory.batch_rows.get()),
                ))? == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
            if execution_limit.is_reached(emitted) {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    })?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

#[derive(Clone, Copy)]
struct OptionalDegreeSpec<'a> {
    source_variable: &'a str,
    rel_type: &'a str,
    rel_properties: &'a BTreeMap<String, Value>,
    direction: RelationshipDirection,
    target_label: &'a str,
    target_properties: &'a BTreeMap<String, Value>,
    alias: &'a str,
    input: &'a PhysicalPlan,
}

impl OptionalDegreeSpec<'_> {
    fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } = self;
        let rel_type_id = if rel_type.is_empty() {
            None
        } else {
            context.catalog.rel_type_id(rel_type)
        };
        let target_label_ids = label_ids_for_pattern(context.catalog, target_label);
        let mut emitted = 0usize;
        execute_binding_batches(input, context, execution_limit, &mut |batch| {
            let mut output = Vec::with_capacity(batch.len());
            for mut binding in batch {
                let degree = if !rel_type.is_empty() && rel_type_id.is_none() {
                    0
                } else {
                    let source = binding.nodes.get(source_variable).ok_or_else(|| {
                        SkeinError::Execution(format!(
                            "missing variable '{source_variable}' during optional degree"
                        ))
                    })?;
                    one_hop_relationships_with_budget(
                        context.store,
                        source.id,
                        rel_type_id,
                        target_label_ids.as_deref(),
                        rel_properties,
                        None,
                        direction,
                        context.memory.blocking_operator_bytes.get(),
                        context.observer,
                    )?
                    .into_iter()
                    .filter(|(_, target)| node_properties_match(target, target_properties))
                    .count()
                };
                binding
                    .values
                    .insert(alias.to_string(), Value::Int(degree as i64));
                output.push(binding);
            }
            emitted = emitted.saturating_add(output.len());
            if !output.is_empty() && emit(output)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(if execution_limit.is_reached(emitted) {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            })
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct BatchReadContext<'a> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a GraphStore,
    pub(super) parameters: &'a BTreeMap<String, Value>,
    pub(super) external: &'a dyn BatchExternalRead,
    pub(super) memory: &'a ExecutionMemoryConfig,
    pub(super) task_context: Option<&'a RuntimeTaskContext>,
    pub(super) observer: &'a QueryExecutionObserver,
}

pub(super) fn batch_pipeline_capable(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqNodeScan { .. }
        | PhysicalPlan::SourceSegmentScan { .. }
        | PhysicalPlan::IndexNodeSeek { .. }
        | PhysicalPlan::IndexNodeMultiSeek { .. }
        | PhysicalPlan::IndexNodeCompositeSeek { .. }
        | PhysicalPlan::IndexNodeRangeSeek { .. }
        | PhysicalPlan::IndexNodeTextSeek { .. }
        | PhysicalPlan::ThreadRepairStatsExec { .. }
        | PhysicalPlan::ShortestPathExec { .. }
        | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::VectorSeedScan { .. } => true,
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::TopNExec { input, .. }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. } => batch_pipeline_capable(input),
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            batch_pipeline_capable(left) && batch_pipeline_capable(right)
        }
        PhysicalPlan::AdjacencyExpandExec { input, .. } => batch_pipeline_capable(input),
        _ => false,
    }
}

pub(super) fn collect_batch_pipeline(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &GraphStore,
    execution_context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    let memory = execution_context.memory;
    let task_context = execution_context.task_context;
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let external = BatchExternalReadAdapter::new(&mut *execution_context.external);
    let context = BatchReadContext {
        catalog,
        store,
        parameters: execution_context.parameters,
        external: &external,
        memory,
        task_context,
        observer: execution_context.observer,
    };
    execute_binding_batches(plan, context, execution_limit, &mut |batch| {
        for binding in batch {
            push_bounded_operator_binding(
                "MaterializedBatchPipeline",
                &mut output,
                binding,
                &mut tracker,
            )?;
            if execution_limit.is_reached(output.len()) {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    })?;
    Ok(output)
}

pub(super) fn execute_binding_batches(
    plan: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let mut measured_emit = |batch: BindingBatch| {
        runtime_checkpoint(context.task_context)?;
        let control = emit_byte_bounded_batches(
            batch,
            context.memory.batch_payload_bytes.get(),
            context.observer,
            emit,
        )?;
        runtime_checkpoint(context.task_context)?;
        Ok(control)
    };
    execute_binding_batches_inner(plan, context, execution_limit, &mut measured_emit)
}

fn emit_byte_bounded_batches(
    batch: BindingBatch,
    max_payload_bytes: usize,
    observer: &QueryExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut batch_bytes = 0usize;
    let mut requires_split = false;
    for binding in &batch {
        let binding_bytes = binding_memory_bytes(binding);
        if binding_bytes > max_payload_bytes {
            return Err(SkeinError::Execution(format!(
                "intermediate row uses {binding_bytes} bytes, exceeding batch_payload_bytes {max_payload_bytes}"
            )));
        }
        batch_bytes = batch_bytes.saturating_add(binding_bytes);
        requires_split |= batch_bytes > max_payload_bytes;
    }
    if !requires_split {
        if !batch.is_empty() {
            observer.record_pipeline_batch(&batch);
            return emit(batch);
        }
        return Ok(BatchControl::Continue);
    }

    let mut bounded = Vec::with_capacity(batch.len());
    let mut bounded_bytes = 0usize;
    for binding in batch {
        let binding_bytes = binding_memory_bytes(&binding);
        if binding_bytes > max_payload_bytes {
            return Err(SkeinError::Execution(format!(
                "intermediate row uses {binding_bytes} bytes, exceeding batch_payload_bytes {max_payload_bytes}"
            )));
        }
        if !bounded.is_empty() && bounded_bytes.saturating_add(binding_bytes) > max_payload_bytes {
            observer.record_pipeline_batch(&bounded);
            if emit(std::mem::take(&mut bounded))? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            bounded_bytes = 0;
        }
        bounded_bytes = bounded_bytes.saturating_add(binding_bytes);
        bounded.push(binding);
    }
    if !bounded.is_empty() {
        observer.record_pipeline_batch(&bounded);
        if emit(bounded)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
    }
    Ok(BatchControl::Continue)
}

fn execute_binding_batches_inner(
    plan: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    debug_assert!(batch_pipeline_capable(plan));
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    match plan {
        PhysicalPlan::SeqNodeScan { variable, label } => {
            stream_node_scan_batches(variable, label, None, context, execution_limit, emit)
        }
        PhysicalPlan::SourceSegmentScan {
            variable,
            predicate,
        } => {
            let bindings =
                execute_source_segment_scan(variable, predicate, context, execution_limit)?;
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::IndexNodeSeek {
            variable,
            label,
            property,
            value,
        } => stream_index_node_seek_batches(
            variable,
            label,
            property,
            std::slice::from_ref(value),
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::IndexNodeMultiSeek {
            variable,
            label,
            property,
            values,
        } => stream_index_node_seek_batches(
            variable,
            label,
            property,
            values,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(
                variable,
                memory.batch_rows.get(),
                execution_limit,
                emit,
                |consumer| {
                    store.visit_nodes_by_composite_property_owned(label_id, predicates, consumer)
                },
            )
        }
        PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            lower,
            upper,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(
                variable,
                memory.batch_rows.get(),
                execution_limit,
                emit,
                |consumer| {
                    store.visit_nodes_by_property_range_owned(
                        label_id,
                        property,
                        lower.as_ref(),
                        upper.as_ref(),
                        consumer,
                    )
                },
            )
        }
        PhysicalPlan::IndexNodeTextSeek {
            variable,
            label,
            property,
            query,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(
                variable,
                memory.batch_rows.get(),
                execution_limit,
                emit,
                |consumer| {
                    store.visit_nodes_by_full_text_property_owned(
                        label_id, property, query, consumer,
                    )
                },
            )
        }
        PhysicalPlan::ShortestPathExec {
            source_label,
            source_id,
            source_visibility_predicate,
            rel_type,
            direction,
            target_label,
            target_id,
            target_visibility_predicate,
            min_hops,
            max_hops,
            returns,
            ..
        } => {
            let source_visibility_filter = source_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let target_visibility_filter = target_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let bindings = execute_shortest_path(
                catalog,
                store,
                ShortestPathExecInput {
                    source_label,
                    source_id,
                    source_visibility_filter: source_visibility_filter.as_ref(),
                    path_node_visibility_filter: source_visibility_filter.as_ref(),
                    rel_type,
                    direction: *direction,
                    target_label,
                    target_id,
                    target_visibility_filter: target_visibility_filter.as_ref(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    returns,
                },
                context.memory,
                execution_limit,
                context.task_context,
                context.observer,
            )?;
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::ThreadRepairStatsExec {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => {
            let bindings = thread_repair_stats_rows(
                catalog,
                store,
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
                memory.blocking_operator_bytes,
                context.observer,
            )?;
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate,
        } => {
            let Some(definition) = store.projected_graph_definition(graph_name) else {
                return Err(SkeinError::Execution(format!(
                    "projected graph '{graph_name}' does not exist"
                )));
            };
            let node_visibility_filter = node_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let layout = match algorithm {
                GraphAlgorithmKind::PageRank => ProjectionLayout::Outgoing,
                GraphAlgorithmKind::Louvain => ProjectionLayout::Undirected,
            };
            let budget = ProjectionMemoryBudget::new(memory.blocking_operator_bytes);
            let graph = if let Some(filter) = node_visibility_filter.as_ref() {
                try_projected_graph_with_node_filter(
                    catalog,
                    store,
                    &definition.node_labels,
                    &definition.rel_types,
                    |node| node_matches_property_filter(node, filter),
                    layout,
                    budget,
                )
            } else {
                try_projected_graph_with_node_filter(
                    catalog,
                    store,
                    &definition.node_labels,
                    &definition.rel_types,
                    |_| true,
                    layout,
                    budget,
                )
            }?;
            let mut bindings = match algorithm {
                GraphAlgorithmKind::PageRank => collect_bounded_operator_bindings(
                    "GraphAlgorithm",
                    graph
                        .page_rank(PageRankOptions {
                            iterations: options
                                .max_iterations
                                .unwrap_or_else(|| PageRankOptions::default().iterations),
                            damping: options
                                .damping
                                .unwrap_or_else(|| PageRankOptions::default().damping),
                        })
                        .into_iter()
                        .map(|score| Binding {
                            values: BTreeMap::from([
                                ("node".to_string(), Value::Int(score.node.0 as i64)),
                                (score_column.clone(), Value::Float(score.score)),
                            ]),
                            nodes: BTreeMap::new(),
                            relationships: BTreeMap::new(),
                        }),
                    memory.blocking_operator_bytes,
                ),
                GraphAlgorithmKind::Louvain => collect_bounded_operator_bindings(
                    "GraphAlgorithm",
                    graph
                        .hierarchical_louvain_communities(LouvainOptions {
                            max_iterations: options
                                .max_iterations
                                .unwrap_or_else(|| LouvainOptions::default().max_iterations),
                            max_levels: options
                                .max_levels
                                .unwrap_or_else(|| LouvainOptions::default().max_levels),
                        })
                        .into_iter()
                        .map(|assignment| Binding {
                            values: BTreeMap::from([
                                ("node".to_string(), Value::Int(assignment.node.0 as i64)),
                                ("level".to_string(), Value::Int(assignment.level as i64)),
                                (
                                    "louvain_id".to_string(),
                                    Value::Int(assignment.community.0 as i64),
                                ),
                            ]),
                            nodes: BTreeMap::new(),
                            relationships: BTreeMap::new(),
                        }),
                    memory.blocking_operator_bytes,
                ),
            }?;
            bindings.truncate(execution_limit.output_rows.unwrap_or(usize::MAX));
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::VectorSeedScan {
            embedding_parameter,
            output_external_id,
            metadata_filters,
            vector_plan,
        } => {
            let embedding =
                vector_embedding_parameter(context.parameters, embedding_parameter, vector_plan)?;
            let output = context
                .external
                .execute_vector_seed(VectorSeedExecutionRequest {
                    embedding: &embedding,
                    metadata_filters,
                    vector_plan,
                })?;
            context.observer.record_vector_execution(output.report);
            let mut bindings = collect_bounded_operator_bindings(
                "VectorSeedScan",
                output.rows.into_iter().map(|row| {
                    let mut values = BTreeMap::from([
                        ("id".to_string(), Value::String(row.id)),
                        ("score".to_string(), Value::Float(row.score)),
                    ]);
                    if *output_external_id && let Some(external_id) = row.external_id {
                        values.insert("external_id".to_string(), Value::String(external_id));
                    }
                    Binding {
                        values,
                        nodes: BTreeMap::new(),
                        relationships: BTreeMap::new(),
                    }
                }),
                memory.blocking_operator_bytes,
            )?;
            bindings.truncate(execution_limit.output_rows.unwrap_or(usize::MAX));
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::NodeColumnLookupExec {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => stream_node_column_lookup_batches(
            NodeColumnLookupSpec {
                variable,
                label,
                property,
                column,
                optional: *optional,
            },
            input,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::OptionalDegreeExec {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => OptionalDegreeSpec {
            source_variable,
            rel_type,
            rel_properties,
            direction: *direction,
            target_label,
            target_properties,
            alias,
            input,
        }
        .stream(context, execution_limit, emit),
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            output,
            ..
        } => {
            let label_ids = label_ids_for_pattern(catalog, label);
            let mut total = 0usize;
            let mut callback_error = None;
            store.visit_nodes_owned(None, |node| {
                if !node_matches_label_pattern(&node, label_ids.as_deref())
                    || !node_properties_match(&node, properties)
                {
                    return GraphScanControl::Continue;
                }
                for leg in legs {
                    match relationship_count_sum_leg(catalog, store, node.id, leg, context.observer)
                    {
                        Ok(count) => total = total.saturating_add(count),
                        Err(error) => {
                            callback_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                GraphScanControl::Continue
            })?;
            if let Some(error) = callback_error {
                return Err(error);
            }
            emit(vec![Binding {
                values: BTreeMap::from([(output.clone(), Value::Int(total as i64))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::AdjacencyExpandExec { input, .. } => stream_adjacency_expand_batches(
            plan,
            input,
            context,
            execution_limit,
            AdjacencyExpandFilters::default(),
            emit,
        ),
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            stream_cartesian_product_batches(left, right, context, execution_limit, emit)
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            if let PhysicalPlan::SeqNodeScan { variable, label } = input.as_ref()
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return stream_node_scan_batches(
                    variable,
                    label,
                    Some((predicate, &filter)),
                    context,
                    execution_limit,
                    emit,
                );
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                rel_variable: Some(rel_variable),
                input: expand_input,
                ..
            } = input.as_ref()
                && let Some(filter) =
                    exact_relationship_scan_filter_from_predicate(predicate, rel_variable)
            {
                return stream_filtered_adjacency_expand_batches(
                    input,
                    expand_input,
                    predicate,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: Some(&filter),
                        target_scan_filter: None,
                    },
                    emit,
                );
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                target_variable,
                input: expand_input,
                ..
            } = input.as_ref()
                && predicate_references_only_variable(predicate, target_variable)
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return stream_filtered_adjacency_expand_batches(
                    input,
                    expand_input,
                    predicate,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: None,
                        target_scan_filter: Some(&filter),
                    },
                    emit,
                );
            }
            let mut emitted = 0usize;
            execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
                let remaining = execution_limit
                    .output_rows
                    .unwrap_or(usize::MAX)
                    .saturating_sub(emitted);
                if remaining == 0 {
                    return Ok(BatchControl::Stop);
                }
                let mut filtered = Vec::with_capacity(batch.len().min(remaining));
                for binding in batch {
                    if evaluate_predicate_observed(
                        predicate,
                        catalog,
                        store,
                        &binding,
                        context.observer,
                    )? {
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
            })
        }
        PhysicalPlan::ProjectExec { items, input } => {
            if let Some(result) =
                try_stream_columnar_projection_batches(items, input, context, execution_limit, emit)
            {
                return result;
            }
            let mut emitted = 0usize;
            execute_binding_batches(input, context, execution_limit, &mut |batch| {
                let mut projected = Vec::with_capacity(batch.len());
                for binding in batch {
                    let mut values = BTreeMap::new();
                    for item in items {
                        let value = project_value(item, catalog, &binding)?;
                        insert_projected_value(&mut values, &item.name, value);
                    }
                    projected.push(Binding {
                        values,
                        nodes: binding.nodes,
                        relationships: binding.relationships,
                    });
                }
                emitted = emitted.saturating_add(projected.len());
                if !projected.is_empty() && emit(projected)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                Ok(if execution_limit.is_reached(emitted) {
                    BatchControl::Stop
                } else {
                    BatchControl::Continue
                })
            })
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => {
            let mut skipped = 0usize;
            let mut emitted = 0usize;
            let output_cap = match (limit, execution_limit.output_rows) {
                (Some(limit), Some(parent)) => (*limit).min(parent),
                (Some(limit), None) => *limit,
                (None, Some(parent)) => parent,
                (None, None) => usize::MAX,
            };
            execute_binding_batches(
                input,
                context,
                ExecutionLimit {
                    output_rows: Some(offset.saturating_add(output_cap)),
                },
                &mut |batch| {
                    let mut output = Vec::new();
                    for binding in batch {
                        if skipped < *offset {
                            skipped += 1;
                            continue;
                        }
                        if emitted == output_cap {
                            break;
                        }
                        output.push(binding);
                        emitted += 1;
                    }
                    if !output.is_empty() && emit(output)? == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                    Ok(if emitted == output_cap {
                        BatchControl::Stop
                    } else {
                        BatchControl::Continue
                    })
                },
            )
        }
        PhysicalPlan::TopNExec {
            items,
            offset,
            limit,
            input,
        } => stream_top_n_batches(
            input,
            items,
            *offset,
            *limit,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::SortExec { items, input } => {
            stream_sort_batches(input, items, context, execution_limit, emit)
        }
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => stream_aggregate_batches(input, group_keys, items, context, execution_limit, emit),
        PhysicalPlan::DistinctExec { input } => {
            stream_distinct_batches(input, context, execution_limit, emit)
        }
        _ => unreachable!("batch pipeline capability check rejected this operator"),
    }
}

#[cfg(test)]
mod byte_bounded_batch_tests {
    use super::*;

    #[test]
    fn within_budget_batch_keeps_its_allocation() {
        let batch = vec![Binding {
            values: BTreeMap::from([("value".to_string(), Value::Int(1))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }];
        let allocation = batch.as_ptr();
        let observer = QueryExecutionObserver::default();
        let mut emitted_allocation = None;

        let control = emit_byte_bounded_batches(batch, usize::MAX, &observer, &mut |emitted| {
            emitted_allocation = Some(emitted.as_ptr());
            Ok(BatchControl::Continue)
        })
        .unwrap();

        assert_eq!(control, BatchControl::Continue);
        assert_eq!(emitted_allocation, Some(allocation));
        assert_eq!(observer.into_reports().pipeline_memory.intermediate_rows, 1);
    }
}
