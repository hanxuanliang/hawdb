use crate::error::{Result, SkeinError};
use crate::optimizer::PhysicalPlan;
use crate::planner::Predicate;
use crate::schema::Catalog;
use crate::store::{ConnectedNodesCreate, GraphMutation, GraphStore, NodeRecord};
use crate::value::Value;
use std::collections::BTreeMap;

pub type Row = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Binding {
    values: BTreeMap<String, Value>,
    nodes: BTreeMap<String, NodeRecord>,
}

pub fn execute(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
) -> Result<Vec<Row>> {
    let bindings = execute_bindings(plan, catalog, store)?;
    Ok(bindings.into_iter().map(|binding| binding.values).collect())
}

pub fn mutation_command(plan: &PhysicalPlan) -> Result<Option<GraphMutation>> {
    match plan {
        PhysicalPlan::CreateNode { label, properties } => Ok(Some(GraphMutation::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        })),
        PhysicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => Ok(Some(GraphMutation::CreateConnectedNodes(
            ConnectedNodesCreate {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
        ))),
        PhysicalPlan::SeqNodeScan { .. }
        | PhysicalPlan::IndexNodeSeek { .. }
        | PhysicalPlan::AdjacencyExpandExec { .. }
        | PhysicalPlan::FilterExec { .. }
        | PhysicalPlan::ProjectExec { .. } => Ok(None),
    }
}

fn execute_bindings(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
) -> Result<Vec<Binding>> {
    match plan {
        PhysicalPlan::CreateNode { label, properties } => {
            let id = store.create_node(catalog, label, properties.clone())?;
            Ok(vec![Binding {
                values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                nodes: BTreeMap::new(),
            }])
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
            }])
        }
        PhysicalPlan::SeqNodeScan { variable, label } => {
            let label_id = catalog.label_id(label);
            Ok(store
                .scan_nodes(label_id)
                .cloned()
                .map(|node| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                })
                .collect())
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
            Ok(store
                .seek_nodes_by_property(label_id, property, value)
                .cloned()
                .map(|node| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                })
                .collect())
        }
        PhysicalPlan::AdjacencyExpandExec {
            source_variable,
            rel_type,
            target_variable,
            target_label,
            input,
        } => {
            let input = execute_bindings(input, catalog, store)?;
            let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
                return Ok(Vec::new());
            };
            let target_label_id = catalog.label_id(target_label);
            let mut output = Vec::new();
            for binding in input {
                let source = binding.nodes.get(source_variable).ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "missing variable '{source_variable}' during expand"
                    ))
                })?;
                for relationship in store.outgoing_relationships(source.id, rel_type_id) {
                    let Some(target) = store.node(relationship.target) else {
                        continue;
                    };
                    if !target_label_id
                        .map(|label_id| target.labels.contains(&label_id))
                        .unwrap_or(true)
                    {
                        continue;
                    }
                    let mut nodes = binding.nodes.clone();
                    nodes.insert(target_variable.clone(), target.clone());
                    output.push(Binding {
                        values: binding.values.clone(),
                        nodes,
                    });
                }
            }
            Ok(output)
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            let input = execute_bindings(input, catalog, store)?;
            Ok(input
                .into_iter()
                .filter(|binding| evaluate_predicate(predicate, binding))
                .collect())
        }
        PhysicalPlan::ProjectExec { items, input } => {
            let input = execute_bindings(input, catalog, store)?;
            input
                .into_iter()
                .map(|binding| {
                    let mut values = BTreeMap::new();
                    for item in items {
                        let node = binding.nodes.get(&item.variable).ok_or_else(|| {
                            SkeinError::Execution(format!(
                                "missing variable '{}' during projection",
                                item.variable
                            ))
                        })?;
                        let value = node
                            .properties
                            .get(&item.property)
                            .cloned()
                            .unwrap_or(Value::Null);
                        values.insert(item.name.clone(), value);
                    }
                    Ok(Binding {
                        values,
                        nodes: binding.nodes,
                    })
                })
                .collect()
        }
    }
}

fn evaluate_predicate(predicate: &Predicate, binding: &Binding) -> bool {
    match predicate {
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => binding
            .nodes
            .get(variable)
            .and_then(|node| node.properties.get(property))
            .map(|actual| actual == value)
            .unwrap_or(false),
    }
}
