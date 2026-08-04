//! Physical read-plan dispatch and materialized fallback execution.

use super::*;

pub(super) fn execute_bindings_with_limit(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    runtime_checkpoint(context.task_context)?;
    if batch_pipeline_capable(plan) {
        return collect_batch_pipeline(
            plan,
            catalog,
            store,
            context.memory,
            context.task_context,
            execution_limit,
        );
    }
    match plan {
        PhysicalPlan::CreateNodeLabel { label } => {
            let existed = catalog.label_id(label);
            let id = store.create_node_label(catalog, label)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("label_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipType { rel_type } => {
            let existed = catalog.rel_type_id(rel_type);
            let id = store.create_relationship_type(catalog, rel_type)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateNodeTable { name } => {
            let existed = catalog.table_id(crate::schema::TableKind::Node, name);
            let id = store.create_node_table(catalog, name)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("table_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipTable { name } => {
            let existed = catalog.table_id(crate::schema::TableKind::Relationship, name);
            let id = store.create_relationship_table(catalog, name)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("table_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => {
            let table_kind = table_kind_to_core(*table_kind);
            let existed = catalog
                .table_id(table_kind, table)
                .and_then(|table_id| catalog.property_descriptor_id(table_id, property))
                .is_some();
            let id = store.create_property_descriptor(
                catalog,
                table_kind,
                table,
                property,
                property_type_to_core(*value_type),
                *nullable,
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("property_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => {
            let state = object_state_to_core(*state);
            let (id, changed) =
                store.alter_table_state(catalog, table_kind_to_core(*table_kind), table, state)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("table_id".to_string(), Value::Int(id.0 as i64)),
                    ("changed".to_string(), Value::Bool(changed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => {
            let state = object_state_to_core(*state);
            let (id, changed) = store.alter_property_state(
                catalog,
                table_kind_to_core(*table_kind),
                table,
                property,
                state,
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("property_id".to_string(), Value::Int(id.0 as i64)),
                    ("changed".to_string(), Value::Bool(changed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateIndex { label, property } => {
            let existed = catalog
                .label_id(label)
                .is_some_and(|label_id| catalog.property_index_id(label_id, property).is_some());
            let id = store.create_property_index(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateCompositeIndex { label, properties } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .composite_property_index_id(label_id, properties)
                    .is_some()
            });
            let id = store.create_composite_property_index(catalog, label, properties)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRangeIndex { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .property_index_id_with_kind(
                        label_id,
                        property,
                        crate::schema::IndexKind::Range,
                    )
                    .is_some()
            });
            let id = store.create_range_property_index(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateFullTextIndex { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .property_index_id_with_kind(
                        label_id,
                        property,
                        crate::schema::IndexKind::FullText,
                    )
                    .is_some()
            });
            let id = store.create_full_text_property_index(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateUniqueConstraint { label, property } => {
            let existed = catalog
                .label_id(label)
                .is_some_and(|label_id| catalog.unique_constraint_id(label_id, property).is_some());
            let id = store.create_unique_constraint(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .node_property_exists_constraint_id(label_id, property)
                    .is_some()
            });
            let id = store.create_node_property_exists_constraint(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            let existed = catalog.rel_type_id(rel_type).is_some_and(|rel_type_id| {
                catalog
                    .relationship_unique_constraint_id(rel_type_id, property)
                    .is_some()
            });
            let id = store.create_relationship_unique_constraint(catalog, rel_type, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            let existed = catalog.rel_type_id(rel_type).is_some_and(|rel_type_id| {
                catalog
                    .relationship_property_exists_constraint_id(rel_type_id, property)
                    .is_some()
            });
            let id = store
                .create_relationship_property_exists_constraint(catalog, rel_type, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => {
            let graph = try_projected_graph_with_node_filter(
                catalog,
                store,
                node_labels,
                rel_types,
                |_| true,
                ProjectionLayout::Outgoing,
                ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes),
            )?;
            store.register_projected_graph(
                name,
                ProjectedGraphDefinition {
                    node_labels: node_labels.clone(),
                    rel_types: rel_types.clone(),
                },
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("graph_name".to_string(), Value::String(name.clone())),
                    (
                        "node_count".to_string(),
                        Value::Int(graph.node_count() as i64),
                    ),
                    (
                        "edge_count".to_string(),
                        Value::Int(graph.edge_count() as i64),
                    ),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
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
            let budget = ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes);
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
            match algorithm {
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
                    context.memory.blocking_operator_bytes,
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
                    context.memory.blocking_operator_bytes,
                ),
            }
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
            record_vector_execution_report(output.report);
            collect_bounded_operator_bindings(
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
                context.memory.blocking_operator_bytes,
            )
        }
        PhysicalPlan::CreateNode { label, properties } => {
            let id = store.create_node(catalog, label, properties.clone())?;
            Ok(vec![Binding {
                values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => {
            let on_match_assignments = on_match_assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let post_merge_assignments = post_merge_assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let (id, created) = store.merge_node(
                catalog,
                label,
                match_properties.clone(),
                on_create_properties.clone(),
                &on_match_assignments,
                &post_merge_assignments,
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("node_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(created)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => {
            let (source, rel, target, created) = store.merge_connected_nodes(
                catalog,
                ConnectedNodesCreate {
                    source_label: source_label.clone(),
                    source_properties: source_properties.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_properties: target_properties.clone(),
                },
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                    ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                    ("created".to_string(), Value::Bool(created)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_between_matches(
                catalog,
                MatchedRelationshipMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    rel_type: rel_type.clone(),
                    rel_match_properties: rel_match_properties.clone(),
                    on_create_properties: on_create_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            target_label,
            target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_from_matched_relationships(
                catalog,
                MatchedRelationshipCopyMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties
                        .iter()
                        .map(|(property, value)| {
                            (
                                property.clone(),
                                relationship_on_create_property_value(value),
                            )
                        })
                        .collect(),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_target_label,
            new_target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_to_matched_target(
                catalog,
                MatchedRelationshipRetargetMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    old_target_label: old_target_label.clone(),
                    old_target_filter: property_filter_from_properties(old_target_properties),
                    new_target_label: new_target_label.clone(),
                    new_target_filter: property_filter_from_properties(new_target_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties.clone(),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label,
            old_source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_source_label,
            new_source_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_from_matched_target(
                catalog,
                MatchedRelationshipSourceRetargetMerge {
                    old_source_label: old_source_label.clone(),
                    old_source_filter: property_filter_from_properties(old_source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    old_target_label: old_target_label.clone(),
                    old_target_filter: property_filter_from_properties(old_target_properties),
                    new_source_label: new_source_label.clone(),
                    new_source_filter: property_filter_from_properties(new_source_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties.clone(),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetNodeProperty {
            label,
            predicate,
            property,
            value,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = match value {
                SetValue::Value(value) => store.set_node_property(
                    catalog,
                    label,
                    filter.as_ref(),
                    property,
                    value.clone(),
                )?,
                SetValue::Coalesce { default, .. } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::Coalesce {
                            default: default.clone(),
                        },
                    }],
                )?,
                SetValue::AddInt { amount, .. } => store.add_int_node_property(
                    catalog,
                    label,
                    filter.as_ref(),
                    property,
                    *amount,
                )?,
                SetValue::DecrementFloorZero { .. } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::DecrementFloorZero,
                    }],
                )?,
                SetValue::PreserveNewerExisting {
                    incoming, preserve, ..
                } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::PreserveNewerExisting {
                            incoming: incoming.clone(),
                            preserve: *preserve,
                        },
                    }],
                )?,
            };
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetNodeProperties {
            label,
            predicate,
            assignments,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let assignments = assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let ids = store.set_node_properties(catalog, label, filter.as_ref(), &assignments)?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetNodePropertiesReturn {
            variable,
            label,
            predicate,
            assignments,
            returns,
        } => {
            let assignments = assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let label_ids = label_ids_for_pattern(catalog, label);
            let mut ids = Vec::new();
            let mut callback_error = None;
            store.visit_nodes_owned(None, |node| {
                if callback_error.is_some() {
                    return GraphScanControl::Stop;
                }
                if !node_matches_label_pattern(&node, label_ids.as_deref()) {
                    return GraphScanControl::Continue;
                }
                let id = node.id;
                let binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                };
                if let Some(predicate) = predicate {
                    match evaluate_predicate(predicate, catalog, store, &binding) {
                        Ok(true) => {}
                        Ok(false) => return GraphScanControl::Continue,
                        Err(error) => {
                            callback_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                ids.push(id);
                GraphScanControl::Continue
            })?;
            if let Some(error) = callback_error {
                return Err(error);
            }
            let ids = store.set_node_properties_by_ids(catalog, &ids, &assignments)?;
            match returns {
                SetNodePropertiesReturnMode::Project(returns) => ids
                    .into_iter()
                    .map(|id| {
                        let node = store.node_owned(id)?.ok_or_else(|| {
                            SkeinError::Execution(format!(
                                "updated node {} is missing during SET RETURN projection",
                                id.0
                            ))
                        })?;
                        let binding = Binding {
                            values: BTreeMap::new(),
                            nodes: BTreeMap::from([(variable.clone(), node)]),
                            relationships: BTreeMap::new(),
                        };
                        let values = returns
                            .iter()
                            .map(|item| {
                                project_value(item, catalog, &binding)
                                    .map(|value| (item.name.clone(), value))
                            })
                            .collect::<Result<BTreeMap<_, _>>>()?;
                        Ok(Binding {
                            values,
                            nodes: BTreeMap::new(),
                            relationships: BTreeMap::new(),
                        })
                    })
                    .collect(),
                SetNodePropertiesReturnMode::Count { name } => Ok(vec![Binding {
                    values: BTreeMap::from([(name.clone(), Value::Int(ids.len() as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                }]),
            }
        }
        PhysicalPlan::SetRelationshipProperty {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            property,
            value,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = store.set_relationship_property(
                catalog,
                RelationshipPropertyUpdate {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter: relationship_filter_from_properties_and_predicate(
                        rel_properties,
                        rel_predicate.as_ref(),
                    )?,
                    property: property.clone(),
                    value: value.clone(),
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("rel_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetRelationshipProperties {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            assignments,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let assignments = assignments
                .iter()
                .map(|assignment| RelationshipSetAssignment {
                    property: assignment.property.clone(),
                    value: assignment.value.clone(),
                })
                .collect::<Vec<_>>();
            let ids = store.set_relationship_properties(
                catalog,
                RelationshipPropertiesUpdate {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter: relationship_filter_from_properties_and_predicate(
                        rel_properties,
                        rel_predicate.as_ref(),
                    )?,
                    assignments,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("rel_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => {
            let label_id = if label.is_empty() {
                None
            } else {
                let Some(label_id) = catalog.label_id(label) else {
                    return Ok(Vec::new());
                };
                Some(label_id)
            };
            let candidate_filter = predicate
                .as_ref()
                .and_then(|predicate| node_scan_filter_from_predicate(predicate, variable));
            let mut ids = Vec::new();
            let mut callback_error = None;
            store.visit_nodes_owned(label_id, |node| {
                if callback_error.is_some() {
                    return GraphScanControl::Stop;
                }
                if candidate_filter
                    .as_ref()
                    .is_some_and(|filter| !node_matches_property_filter(&node, filter))
                {
                    return GraphScanControl::Continue;
                }
                let id = node.id;
                let binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                };
                if let Some(predicate) = predicate {
                    match evaluate_predicate(predicate, catalog, store, &binding) {
                        Ok(true) => {}
                        Ok(false) => return GraphScanControl::Continue,
                        Err(error) => {
                            callback_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                ids.push(id);
                GraphScanControl::Continue
            })?;
            if let Some(error) = callback_error {
                return Err(error);
            }
            let ids = store.delete_node_ids(catalog, &ids, *detach)?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::DeleteRelationship {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let rel_filter = relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?;
            let ids = store.delete_relationships(
                catalog,
                RelationshipDeleteRequest {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("rel_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::DeleteRelationshipTargetNodes {
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
            detach,
            ..
        } => {
            let source_filter = source_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = store.delete_relationship_target_nodes(
                catalog,
                RelationshipTargetNodeDelete {
                    source_label: source_label.clone(),
                    source_filter,
                    rel_type: rel_type.clone(),
                    rel_filter: property_filter_from_properties(rel_properties),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    detach: *detach,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => {
            let (source, rel, target) = store.create_connected_nodes(
                catalog,
                ConnectedNodesCreate {
                    source_label: source_label.clone(),
                    source_properties: source_properties.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_properties: target_properties.clone(),
                },
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                    ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => {
            let rows = store.create_relationships_between_matches(
                catalog,
                MatchedRelationshipCreate {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SeqNodeScan { variable, label } => execute_node_scan_with_optional_filter(
            variable,
            label,
            None,
            catalog,
            store,
            execution_limit,
            context.memory.blocking_operator_bytes,
        ),
        PhysicalPlan::SourceSegmentScan {
            variable,
            predicate,
        } => execute_source_segment_scan(
            variable,
            predicate,
            catalog,
            store,
            execution_limit,
            context.memory,
            context.task_context,
        ),
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left = execute_child_bindings(left, catalog, store, context)?;
            let right = execute_child_bindings(right, catalog, store, context)?;
            let mut output = Vec::new();
            let mut tracker = OperatorMemoryTracker::new(context.memory.blocking_operator_bytes);
            for binding in left.iter().chain(&right) {
                let bytes = binding_memory_bytes(binding);
                ensure_operator_item_fits("NodeCartesianProductExec", bytes, &tracker)?;
                if tracker.would_exceed(bytes) {
                    return Err(SkeinError::Execution(format!(
                        "NodeCartesianProductExec inputs exceed blocking_operator_bytes {}",
                        tracker.budget_bytes
                    )));
                }
                tracker.charge(bytes);
            }
            for left_binding in &left {
                for right_binding in &right {
                    let mut values = left_binding.values.clone();
                    values.extend(right_binding.values.clone());
                    let mut nodes = left_binding.nodes.clone();
                    nodes.extend(right_binding.nodes.clone());
                    let mut relationships = left_binding.relationships.clone();
                    relationships.extend(right_binding.relationships.clone());
                    let binding = Binding {
                        values,
                        nodes,
                        relationships,
                    };
                    push_bounded_operator_binding(
                        "NodeCartesianProductExec",
                        &mut output,
                        binding,
                        &mut tracker,
                    )?;
                    if execution_limit.is_reached(output.len()) {
                        return Ok(output);
                    }
                }
            }
            Ok(output)
        }
        PhysicalPlan::NodeColumnLookupExec {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            execute_node_column_lookup(
                NodeColumnLookupSpec {
                    variable,
                    label,
                    property,
                    column,
                    optional: *optional,
                },
                input,
                catalog,
                store,
                execution_limit,
                context.memory.blocking_operator_bytes,
            )
        }
        PhysicalPlan::IndexNodeSeek {
            variable,
            label,
            property,
            value,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_property_owned(
                label_id,
                property,
                std::slice::from_ref(value),
                |node| {
                    output.push(single_node_binding(variable, node));
                    if execution_limit.is_reached(output.len()) {
                        GraphScanControl::Stop
                    } else {
                        GraphScanControl::Continue
                    }
                },
            )?;
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyEq {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: output.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(output.len()),
                candidate_count_before_filter: output.len(),
                output_count: output.len(),
                filtered_out_count: 0,
            });
            Ok(output)
        }
        PhysicalPlan::IndexNodeMultiSeek {
            variable,
            label,
            property,
            values,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut seen = std::collections::BTreeSet::new();
            let mut output = Vec::new();
            store.visit_nodes_by_property_owned(label_id, property, values, |node| {
                if seen.insert(node.id) {
                    output.push(single_node_binding(variable, node));
                }
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyIn {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: output.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(output.len()),
                candidate_count_before_filter: output.len(),
                output_count: output.len(),
                filtered_out_count: 0,
            });
            Ok(output)
        }
        PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_composite_property_owned(label_id, predicates, |node| {
                output.push(single_node_binding(variable, node));
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            Ok(output)
        }
        PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            lower,
            upper,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_property_range_owned(
                label_id,
                property,
                lower.as_ref(),
                upper.as_ref(),
                |node| {
                    output.push(single_node_binding(variable, node));
                    if execution_limit.is_reached(output.len()) {
                        GraphScanControl::Stop
                    } else {
                        GraphScanControl::Continue
                    }
                },
            )?;
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyRange {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: output.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(output.len()),
                candidate_count_before_filter: output.len(),
                output_count: output.len(),
                filtered_out_count: 0,
            });
            Ok(output)
        }
        PhysicalPlan::IndexNodeTextSeek {
            variable,
            label,
            property,
            query,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_full_text_property_owned(label_id, property, query, |node| {
                output.push(single_node_binding(variable, node));
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            Ok(output)
        }
        PhysicalPlan::AdjacencyExpandExec { input, .. } => execute_adjacency_expand(
            plan,
            input,
            catalog,
            store,
            context,
            execution_limit,
            AdjacencyExpandFilters::default(),
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
        } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            let rel_type_id = if rel_type.is_empty() {
                None
            } else {
                catalog.rel_type_id(rel_type)
            };
            if !rel_type.is_empty() && rel_type_id.is_none() {
                return Ok(input
                    .into_iter()
                    .map(|mut binding| {
                        binding.values.insert(alias.clone(), Value::Int(0));
                        binding
                    })
                    .collect());
            }
            let target_label_ids = label_ids_for_pattern(catalog, target_label);
            input
                .into_iter()
                .map(|mut binding| {
                    let source = binding.nodes.get(source_variable).ok_or_else(|| {
                        SkeinError::Execution(format!(
                            "missing variable '{source_variable}' during optional degree"
                        ))
                    })?;
                    let degree = one_hop_relationships_with_budget(
                        store,
                        source.id,
                        rel_type_id,
                        target_label_ids.as_deref(),
                        rel_properties,
                        None,
                        *direction,
                        context.memory.blocking_operator_bytes.get(),
                    )?
                    .into_iter()
                    .filter(|(_, target)| node_properties_match(target, target_properties))
                    .count();
                    binding
                        .values
                        .insert(alias.clone(), Value::Int(degree as i64));
                    Ok(binding)
                })
                .collect()
        }
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
                    match relationship_count_sum_leg(catalog, store, node.id, leg) {
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
            Ok(vec![Binding {
                values: BTreeMap::from([(output.clone(), Value::Int(total as i64))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
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
        } => thread_repair_stats_rows(
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
            context.memory.blocking_operator_bytes,
        ),
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
            execute_shortest_path(
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
            )
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            if let PhysicalPlan::SeqNodeScan { variable, label } = input.as_ref()
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return execute_node_scan_with_optional_filter(
                    variable,
                    label,
                    Some((predicate, &filter)),
                    catalog,
                    store,
                    execution_limit,
                    context.memory.blocking_operator_bytes,
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
                let input = execute_adjacency_expand(
                    input,
                    expand_input,
                    catalog,
                    store,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: Some(&filter),
                        target_scan_filter: None,
                    },
                )?;
                let mut output = Vec::new();
                for binding in input {
                    if evaluate_predicate(predicate, catalog, store, &binding)? {
                        output.push(binding);
                        if execution_limit.is_reached(output.len()) {
                            return Ok(output);
                        }
                    }
                }
                return Ok(output);
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                target_variable,
                input: expand_input,
                ..
            } = input.as_ref()
                && predicate_references_only_variable(predicate, target_variable)
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                let input = execute_adjacency_expand(
                    input,
                    expand_input,
                    catalog,
                    store,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: None,
                        target_scan_filter: Some(&filter),
                    },
                )?;
                let mut output = Vec::new();
                for binding in input {
                    if evaluate_predicate(predicate, catalog, store, &binding)? {
                        output.push(binding);
                        if execution_limit.is_reached(output.len()) {
                            return Ok(output);
                        }
                    }
                }
                return Ok(output);
            }
            let input = execute_child_bindings(input, catalog, store, context)?;
            let mut output = Vec::new();
            for binding in input {
                if evaluate_predicate(predicate, catalog, store, &binding)? {
                    output.push(binding);
                    if execution_limit.is_reached(output.len()) {
                        return Ok(output);
                    }
                }
            }
            Ok(output)
        }
        PhysicalPlan::ProjectExec { items, input } => {
            let input =
                execute_bindings_with_limit(input, catalog, store, context, execution_limit)?;
            let mut output = Vec::new();
            for binding in input {
                let mut values = BTreeMap::new();
                for item in items {
                    let value = project_value(item, catalog, &binding)?;
                    insert_projected_value(&mut values, &item.name, value);
                }
                output.push(Binding {
                    values,
                    nodes: binding.nodes,
                    relationships: binding.relationships,
                });
                if execution_limit.is_reached(output.len()) {
                    return Ok(output);
                }
            }
            Ok(output)
        }
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            Ok(execute_aggregate(catalog, group_keys, items, &input))
        }
        PhysicalPlan::DistinctExec { input } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            Ok(distinct_bindings(input))
        }
        PhysicalPlan::SortExec { items, input } => {
            let mut input = execute_child_bindings(input, catalog, store, context)?;
            input.sort_by(|left, right| compare_bindings(catalog, left, right, items));
            Ok(input)
        }
        PhysicalPlan::TopNExec {
            items,
            offset,
            limit,
            input,
        } => execute_top_n_bindings(input, items, *offset, *limit, catalog, store, context),
        PhysicalPlan::LimitExec {
            offset,
            limit: query_limit,
            input,
        } => {
            let child_limit = execution_limit.child_for_limit(*offset, *query_limit);
            let input = execute_bindings_with_limit(input, catalog, store, context, child_limit)?;
            let rows = input
                .into_iter()
                .skip(*offset)
                .take(query_limit.unwrap_or(usize::MAX))
                .collect();
            Ok(rows)
        }
    }
}

fn execute_top_n_bindings(
    input: &PhysicalPlan,
    items: &[SortItem],
    offset: usize,
    limit: usize,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
) -> Result<Vec<Binding>> {
    let retained = offset.saturating_add(limit);
    if retained == 0 {
        return Ok(Vec::new());
    }
    let input = execute_child_bindings(input, catalog, store, context)?;
    let mut heap = BinaryHeap::with_capacity(retained.min(input.len()));
    for (ordinal, binding) in input.into_iter().enumerate() {
        let ordinal = u64::try_from(ordinal)
            .map_err(|_| SkeinError::Execution("TopNExec input ordinal exceeds u64".to_string()))?;
        let sort_values = items
            .iter()
            .map(|item| (sort_value(catalog, &binding, &item.key), item.direction))
            .collect();
        let candidate = TopNBinding {
            sort_values,
            ordinal,
            binding,
        };
        if heap.len() < retained {
            heap.push(candidate);
        } else if heap.peek().is_some_and(|worst| candidate < *worst) {
            heap.pop();
            heap.push(candidate);
        }
    }
    let mut selected = heap.into_vec();
    selected.sort();
    Ok(selected
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|entry| entry.binding)
        .collect())
}

pub(super) fn execute_child_bindings(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
) -> Result<Vec<Binding>> {
    execute_bindings_with_limit(plan, catalog, store, context, ExecutionLimit::unlimited())
}
