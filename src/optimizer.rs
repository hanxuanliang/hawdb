use crate::planner::{LogicalPlan, Predicate, Projection};
use crate::value::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
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
    SeqNodeScan {
        variable: String,
        label: String,
    },
    IndexNodeSeek {
        variable: String,
        label: String,
        property: String,
        value: Value,
    },
    AdjacencyExpandExec {
        source_variable: String,
        rel_type: String,
        target_variable: String,
        target_label: String,
        input: Box<PhysicalPlan>,
    },
    FilterExec {
        predicate: Predicate,
        input: Box<PhysicalPlan>,
    },
    ProjectExec {
        items: Vec<Projection>,
        input: Box<PhysicalPlan>,
    },
}

impl PhysicalPlan {
    pub fn explain(&self, indent: usize) -> String {
        let pad = " ".repeat(indent);
        match self {
            PhysicalPlan::CreateNode { label, .. } => {
                format!("{pad}CreateNode label={label}")
            }
            PhysicalPlan::CreateRelationship {
                source_label,
                rel_type,
                target_label,
                ..
            } => {
                format!(
                    "{pad}CreateRelationship source_label={source_label} rel_type={rel_type} target_label={target_label}"
                )
            }
            PhysicalPlan::SeqNodeScan { variable, label } => {
                format!("{pad}SeqNodeScan variable={variable} label={label}")
            }
            PhysicalPlan::IndexNodeSeek {
                variable,
                label,
                property,
                value,
            } => {
                format!(
                    "{pad}IndexNodeSeek variable={variable} label={label} property={property} value={value:?}"
                )
            }
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                rel_type,
                target_variable,
                target_label,
                input,
            } => {
                format!(
                    "{pad}AdjacencyExpandExec source={source_variable} rel_type={rel_type} target={target_variable}:{target_label}\n{}",
                    input.explain(indent + 2)
                )
            }
            PhysicalPlan::FilterExec { predicate, input } => {
                format!(
                    "{pad}FilterExec predicate={predicate:?}\n{}",
                    input.explain(indent + 2)
                )
            }
            PhysicalPlan::ProjectExec { items, input } => {
                let columns = items
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{pad}ProjectExec columns=[{columns}]\n{}",
                    input.explain(indent + 2)
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerConfig {
    pub max_groups: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self { max_groups: 128 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerTrace {
    pub groups: usize,
    pub selected_plan: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(usize);

#[derive(Debug, Default)]
pub struct Memo {
    groups: Vec<Group>,
}

#[derive(Debug)]
struct Group {
    expressions: Vec<GroupExpr>,
}

#[derive(Debug)]
struct GroupExpr {
    logical: LogicalPlan,
    children: Vec<GroupId>,
}

#[derive(Debug, Default)]
pub struct CascadesOptimizer {
    config: OptimizerConfig,
}

impl CascadesOptimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        Self { config }
    }

    pub fn optimize(&self, logical: &LogicalPlan) -> PhysicalPlan {
        self.optimize_with_trace(logical).0
    }

    pub fn optimize_with_trace(&self, logical: &LogicalPlan) -> (PhysicalPlan, OptimizerTrace) {
        let mut memo = Memo::default();
        let root = memo.insert(logical);
        let mut warnings = Vec::new();
        if memo.groups.len() > self.config.max_groups {
            warnings.push(format!(
                "memo group count {} exceeded configured limit {}",
                memo.groups.len(),
                self.config.max_groups
            ));
        }
        let plan = memo.best_physical(root);
        let selected_plan = plan.explain(0);
        (
            plan,
            OptimizerTrace {
                groups: memo.groups.len(),
                selected_plan,
                warnings,
            },
        )
    }
}

impl Memo {
    pub fn insert(&mut self, logical: &LogicalPlan) -> GroupId {
        let expr = GroupExpr::from_logical(logical, self);
        let id = GroupId(self.groups.len());
        self.groups.push(Group {
            expressions: vec![expr],
        });
        id
    }

    fn best_physical(&self, root: GroupId) -> PhysicalPlan {
        let group = &self.groups[root.0];
        group.expressions[0].to_physical(self)
    }
}

impl GroupExpr {
    fn from_logical(logical: &LogicalPlan, memo: &mut Memo) -> Self {
        match logical {
            LogicalPlan::CreateNode { .. }
            | LogicalPlan::CreateRelationship { .. }
            | LogicalPlan::NodeScan { .. } => Self {
                logical: logical.clone(),
                children: Vec::new(),
            },
            LogicalPlan::Expand { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. } => Self {
                logical: logical.clone(),
                children: vec![memo.insert(input)],
            },
        }
    }

    fn to_physical(&self, memo: &Memo) -> PhysicalPlan {
        match &self.logical {
            LogicalPlan::CreateNode { label, properties } => PhysicalPlan::CreateNode {
                label: label.clone(),
                properties: properties.clone(),
            },
            LogicalPlan::CreateRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => PhysicalPlan::CreateRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
            LogicalPlan::NodeScan { variable, label } => PhysicalPlan::SeqNodeScan {
                variable: variable.clone(),
                label: label.clone(),
            },
            LogicalPlan::Expand {
                source_variable,
                rel_type,
                target_variable,
                target_label,
                ..
            } => PhysicalPlan::AdjacencyExpandExec {
                source_variable: source_variable.clone(),
                rel_type: rel_type.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                input: Box::new(memo.best_physical(self.children[0])),
            },
            LogicalPlan::Filter { predicate, input } => {
                if let Some(plan) = index_seek_from_filter(predicate, input) {
                    plan
                } else {
                    PhysicalPlan::FilterExec {
                        predicate: predicate.clone(),
                        input: Box::new(memo.best_physical(self.children[0])),
                    }
                }
            }
            LogicalPlan::Project { items, .. } => PhysicalPlan::ProjectExec {
                items: items.clone(),
                input: Box::new(memo.best_physical(self.children[0])),
            },
        }
    }
}

fn index_seek_from_filter(predicate: &Predicate, input: &LogicalPlan) -> Option<PhysicalPlan> {
    match (predicate, input) {
        (
            Predicate::PropertyEq {
                variable,
                property,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => Some(PhysicalPlan::IndexNodeSeek {
            variable: variable.clone(),
            label: label.clone(),
            property: property.clone(),
            value: value.clone(),
        }),
        _ => None,
    }
}
