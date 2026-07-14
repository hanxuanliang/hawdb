use crate::cypher::{PropertyPredicate, ReturnItem, Statement, ValueExpression};
use crate::error::{Result, SkeinError};
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    CreateRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    NodeScan {
        variable: String,
        label: String,
    },
    Expand {
        source_variable: String,
        rel_type: String,
        target_variable: String,
        target_label: String,
        input: Box<LogicalPlan>,
    },
    Filter {
        predicate: Predicate,
        input: Box<LogicalPlan>,
    },
    Project {
        items: Vec<Projection>,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    PropertyEq {
        variable: String,
        property: String,
        value: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    pub variable: String,
    pub property: String,
    pub name: String,
}

pub fn plan(statement: &Statement) -> Result<LogicalPlan> {
    plan_with_params(statement, &BTreeMap::new())
}

pub fn plan_with_params(
    statement: &Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    match statement {
        Statement::CreateNode(node) => Ok(LogicalPlan::CreateNode {
            label: node.label.clone(),
            properties: bind_properties(&node.properties, parameters)?,
        }),
        Statement::CreateRelationship(relationship) => Ok(LogicalPlan::CreateRelationship {
            source_label: relationship.source.label.clone(),
            source_properties: bind_properties(&relationship.source.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: relationship.target.label.clone(),
            target_properties: bind_properties(&relationship.target.properties, parameters)?,
        }),
        Statement::MatchReturn(query) => {
            let mut scope = BTreeSet::from([query.variable.clone()]);
            if let Some(expand) = &query.expand {
                scope.insert(expand.target_variable.clone());
            }
            if let Some(predicate) = &query.predicate {
                validate_predicate(&scope, predicate)?;
            }
            let mut input = LogicalPlan::NodeScan {
                variable: query.variable.clone(),
                label: query.label.clone(),
            };
            if let Some(expand) = &query.expand {
                input = LogicalPlan::Expand {
                    source_variable: query.variable.clone(),
                    rel_type: expand.rel_type.clone(),
                    target_variable: expand.target_variable.clone(),
                    target_label: expand.target_label.clone(),
                    input: Box::new(input),
                };
            }
            if let Some(predicate) = &query.predicate {
                input = LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: predicate.variable.clone(),
                        property: predicate.property.clone(),
                        value: bind_value(&predicate.value, parameters)?,
                    },
                    input: Box::new(input),
                };
            }
            Ok(LogicalPlan::Project {
                items: plan_projections(&scope, &query.returns)?,
                input: Box::new(input),
            })
        }
    }
}

fn bind_properties(
    properties: &BTreeMap<String, ValueExpression>,
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    properties
        .iter()
        .map(|(name, value)| Ok((name.clone(), bind_value(value, parameters)?)))
        .collect()
}

fn bind_value(expression: &ValueExpression, parameters: &BTreeMap<String, Value>) -> Result<Value> {
    match expression {
        ValueExpression::Literal(value) => Ok(value.clone()),
        ValueExpression::Parameter(name) => parameters
            .get(name)
            .cloned()
            .ok_or_else(|| SkeinError::Semantic(format!("missing parameter '${name}'"))),
    }
}

fn validate_predicate(scope: &BTreeSet<String>, predicate: &PropertyPredicate) -> Result<()> {
    if !scope.contains(&predicate.variable) {
        return Err(SkeinError::Semantic(format!(
            "unknown variable '{}' in predicate",
            predicate.variable
        )));
    }
    Ok(())
}

fn plan_projections(scope: &BTreeSet<String>, items: &[ReturnItem]) -> Result<Vec<Projection>> {
    items
        .iter()
        .map(|item| {
            if !scope.contains(&item.variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{}' in return item",
                    item.variable
                )));
            }
            Ok(Projection {
                variable: item.variable.clone(),
                property: item.property.clone(),
                name: item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{}.{}", item.variable, item.property)),
            })
        })
        .collect()
}
