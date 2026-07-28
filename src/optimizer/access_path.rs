use super::costing::{
    estimate_node_full_scan_cost, estimate_node_index_seek_cost, node_index_seek_is_cheaper,
    NODE_INDEX_EQ_STARTUP_COST, NODE_INDEX_RANGE_STARTUP_COST, NODE_INDEX_TEXT_STARTUP_COST,
};
use super::stages::ACCESS_PATH_SELECTION_STAGE;
use super::value_range::{
    merge_lower_bound, merge_upper_bound, range_bounds_for_comparison, ValueRangeBounds,
};
use super::{OptimizerCatalog, PhysicalPlan};
use crate::planner::{LogicalPlan, Predicate};
use crate::value::Value;
use skein_optimizer::{OptimizerRule, RuleApplication, RuleId, RuleKind, RulePromise, StageTrace};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
enum GraphRuleExpr {
    Filter {
        predicate: Box<Predicate>,
        input: Box<LogicalPlan>,
    },
    Physical(Box<PhysicalPlan>),
}

struct NodeEqualitySeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeInSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeRangeSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeTextSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeCompositeSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeConjunctionSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

pub(super) fn index_seek_from_filter(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    match (predicate, input) {
        (
            Predicate::And(predicates),
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) => index_seek_from_conjunction(
            predicates,
            predicate,
            scan_variable,
            label,
            catalog,
            decisions,
            stage_events,
        ),
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
        ) if variable == scan_variable => {
            if let Some(plan) =
                equality_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no equality index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let estimated_rows = label_count.div_ceil(distinct_count).max(1);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost =
                estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                decisions.push(format!(
                    "choose IndexNodeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
                ));
                Some(PhysicalPlan::IndexNodeSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    value: value.clone(),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
                ));
                None
            }
        }
        (
            Predicate::PropertyIn {
                variable,
                property,
                values,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) =
                in_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no equality index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let rows_per_value = label_count.div_ceil(distinct_count).max(1);
            let estimated_rows = rows_per_value
                .saturating_mul(values.len() as u64)
                .min(label_count)
                .max(1);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                decisions.push(format!(
                    "choose IndexNodeMultiSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                    values.len()
                ));
                Some(PhysicalPlan::IndexNodeMultiSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    values: values.clone(),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                    values.len()
                ));
                None
            }
        }
        (
            Predicate::PropertyCompare {
                variable,
                property,
                op,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) =
                range_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_range_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no range index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let estimated_rows = catalog.estimate_range_rows(label, property, *op, value);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost =
                estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                let (lower, upper) = range_bounds_for_comparison(*op, value.clone());
                decisions.push(format!(
                    "choose IndexNodeRangeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                Some(PhysicalPlan::IndexNodeRangeSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    lower,
                    upper,
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                None
            }
        }
        (
            Predicate::PropertyContains {
                variable,
                property,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) =
                text_index_seek_from_rule(predicate, input, catalog, decisions, stage_events)
            {
                return Some(plan);
            }
            if !catalog.has_full_text_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no fulltext index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let estimated_rows = label_count.div_ceil(4).max(1);
            let scan_cost = estimate_node_full_scan_cost(label_count);
            let seek_cost =
                estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_TEXT_STARTUP_COST);
            if node_index_seek_is_cheaper(label_count, seek_cost) {
                decisions.push(format!(
                    "choose IndexNodeTextSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                Some(PhysicalPlan::FilterExec {
                    predicate: predicate.clone(),
                    input: Box::new(PhysicalPlan::IndexNodeTextSeek {
                        variable: variable.clone(),
                        label: label.clone(),
                        property: property.clone(),
                        query: value.clone(),
                    }),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                None
            }
        }
        _ => None,
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeTextSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_text_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyContains {
            variable, property, ..
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_full_text_property_index(label, property)
        {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = label_count.div_ceil(4).max(1);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_TEXT_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(85)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyContains {
            variable,
            property,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_full_text_property_index(label, property)
        {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = label_count.div_ceil(4).max(1);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_TEXT_STARTUP_COST);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::FilterExec {
                predicate: predicate.as_ref().clone(),
                input: Box::new(PhysicalPlan::IndexNodeTextSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    query: value.clone(),
                }),
            })),
            format!(
                "choose IndexNodeTextSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeCompositeSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_composite_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if composite_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .is_some()
        {
            RulePromise::new(105)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        composite_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .map(|(plan, decision)| {
                RuleApplication::new(GraphRuleExpr::Physical(Box::new(plan)), decision)
            })
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeConjunctionSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_conjunction_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if equality_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .is_some()
        {
            RulePromise::new(80)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        equality_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .map(|(_, plan, decision)| {
                RuleApplication::new(GraphRuleExpr::Physical(Box::new(plan)), decision)
            })
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeRangeSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_range_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_range_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = self
            .catalog
            .estimate_range_rows(label, property, *op, value);
        let seek_cost =
            estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(90)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_range_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = self
            .catalog
            .estimate_range_rows(label, property, *op, value);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost =
            estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        let (lower, upper) = range_bounds_for_comparison(*op, value.clone());
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeRangeSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                lower,
                upper,
            })),
            format!(
                "choose IndexNodeRangeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeInSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_in_index_multi_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let rows_per_value = label_count.div_ceil(distinct_count).max(1);
        let estimated_rows = rows_per_value
            .saturating_mul(values.len() as u64)
            .min(label_count)
            .max(1);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(95)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let rows_per_value = label_count.div_ceil(distinct_count).max(1);
        let estimated_rows = rows_per_value
            .saturating_mul(values.len() as u64)
            .min(label_count)
            .max(1);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeMultiSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                values: values.clone(),
            })),
            format!(
                "choose IndexNodeMultiSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                values.len()
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeEqualitySeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_equality_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyEq {
            variable, property, ..
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let estimated_rows = label_count.div_ceil(distinct_count).max(1);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            RulePromise::new(100)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let estimated_rows = label_count.div_ceil(distinct_count).max(1);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
        if !node_index_seek_is_cheaper(label_count, seek_cost) {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                value: value.clone(),
            })),
            format!(
                "choose IndexNodeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
            ),
        ))
    }
}

fn text_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeTextSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn composite_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeCompositeSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn conjunction_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeConjunctionSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn range_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeRangeSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn in_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeInSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn equality_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeEqualitySeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions, stage_events)
}

fn physical_plan_from_rule_batch(
    expression: &GraphRuleExpr,
    rules: &[&dyn OptimizerRule<GraphRuleExpr>],
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let batch = ACCESS_PATH_SELECTION_STAGE.execute_rule_batch(expression, rules);
    let (expressions, events, trace) = batch.into_parts();
    decisions.extend(events.into_iter().map(|event| event.into_decision()));
    stage_events.push(trace);
    expressions.into_iter().find_map(|applied| {
        let application = applied.into_application();
        decisions.push(application.detail().to_string());
        match application.into_expression() {
            GraphRuleExpr::Physical(plan) => Some(*plan),
            GraphRuleExpr::Filter { .. } => None,
        }
    })
}

fn index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    if let Some(plan) = equality_index_seek_from_conjunction(
        predicates,
        full_predicate,
        scan_variable,
        label,
        catalog,
        decisions,
        stage_events,
    ) {
        return Some(plan);
    }
    range_index_seek_from_conjunction(
        predicates,
        full_predicate,
        scan_variable,
        label,
        catalog,
        decisions,
        stage_events,
    )
}

fn equality_index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    if let Some(plan) = composite_index_seek_from_conjunction(
        predicates,
        full_predicate,
        scan_variable,
        label,
        catalog,
        decisions,
        stage_events,
    ) {
        return Some(plan);
    }
    let logical_scan = LogicalPlan::NodeScan {
        variable: scan_variable.to_string(),
        label: label.to_string(),
    };
    if let Some(plan) = conjunction_index_seek_from_rule(
        full_predicate,
        &logical_scan,
        catalog,
        decisions,
        stage_events,
    ) {
        return Some(plan);
    }
    if let Some((_, plan, decision)) =
        equality_index_seek_candidate(predicates, full_predicate, scan_variable, label, catalog)
    {
        decisions.push(decision);
        return Some(plan);
    }
    None
}

fn equality_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<(u64, PhysicalPlan, String)> {
    let label_count = catalog.label_count(label);
    let scan_cost = estimate_node_full_scan_cost(label_count);
    let mut best_candidate: Option<(u64, PhysicalPlan, String)> = None;
    for predicate in predicates {
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_property_index(label, property) {
            continue;
        }
        let distinct_count = catalog.distinct_count(label, property).max(1);
        let estimated_rows = label_count.div_ceil(distinct_count).max(1);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_EQ_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            let decision = format!(
                "choose IndexNodeSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeSeek {
                    variable: variable.clone(),
                    label: label.to_string(),
                    property: property.clone(),
                    value: value.clone(),
                }),
            };
            if best_candidate
                .as_ref()
                .is_none_or(|(best_cost, _, _)| seek_cost < *best_cost)
            {
                best_candidate = Some((seek_cost, plan, decision));
            }
        }
    }
    for predicate in predicates {
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_property_index(label, property) {
            continue;
        }
        let distinct_count = catalog.distinct_count(label, property).max(1);
        let rows_per_value = label_count.div_ceil(distinct_count).max(1);
        let estimated_rows = rows_per_value
            .saturating_mul(values.len() as u64)
            .min(label_count)
            .max(1);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, values.len() as u64);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            let decision = format!(
                "choose IndexNodeMultiSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                values.len()
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeMultiSeek {
                    variable: variable.clone(),
                    label: label.to_string(),
                    property: property.clone(),
                    values: values.clone(),
                }),
            };
            if best_candidate
                .as_ref()
                .is_none_or(|(best_cost, _, _)| seek_cost < *best_cost)
            {
                best_candidate = Some((seek_cost, plan, decision));
            }
        }
    }
    best_candidate
}

fn composite_index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let logical_scan = LogicalPlan::NodeScan {
        variable: scan_variable.to_string(),
        label: label.to_string(),
    };
    if let Some(plan) = composite_index_seek_from_rule(
        full_predicate,
        &logical_scan,
        catalog,
        decisions,
        stage_events,
    ) {
        return Some(plan);
    }
    if let Some((plan, decision)) =
        composite_index_seek_candidate(predicates, full_predicate, scan_variable, label, catalog)
    {
        decisions.push(decision);
        return Some(plan);
    }
    None
}

fn composite_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<(PhysicalPlan, String)> {
    let mut equality_values = BTreeMap::<String, Value>::new();
    for predicate in predicates {
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate
        else {
            continue;
        };
        if variable == scan_variable {
            equality_values.insert(property.clone(), value.clone());
        }
    }
    for properties in catalog.composite_property_indexes_for_label(label) {
        if properties.len() < 2 || !catalog.has_composite_property_index(label, &properties) {
            continue;
        }
        let mut seek_predicates = Vec::with_capacity(properties.len());
        for property in &properties {
            let Some(value) = equality_values.get(property) else {
                seek_predicates.clear();
                break;
            };
            seek_predicates.push((property.clone(), value.clone()));
        }
        if seek_predicates.is_empty() {
            continue;
        }
        let label_count = catalog.label_count(label);
        let distinct_product = properties
            .iter()
            .map(|property| catalog.distinct_count(label, property).max(1))
            .fold(1_u64, |acc, value| acc.saturating_mul(value))
            .max(1);
        let estimated_rows = label_count.div_ceil(distinct_product).max(1);
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost = estimate_node_index_seek_cost(estimated_rows, properties.len() as u64);
        if node_index_seek_is_cheaper(label_count, seek_cost) {
            let decision = format!(
                "choose IndexNodeCompositeSeek for {label}.{:?}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_product={distinct_product}",
                properties
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeCompositeSeek {
                    variable: scan_variable.to_string(),
                    label: label.to_string(),
                    predicates: seek_predicates,
                }),
            };
            return Some((plan, decision));
        }
    }
    None
}

fn range_index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    _stage_events: &mut Vec<StageTrace>,
) -> Option<PhysicalPlan> {
    let mut ranges = BTreeMap::<String, ValueRangeBounds>::new();
    for predicate in predicates {
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_range_property_index(label, property) {
            continue;
        }
        let (lower, upper) = ranges.entry(property.clone()).or_default();
        let (candidate_lower, candidate_upper) = range_bounds_for_comparison(*op, value.clone());
        merge_lower_bound(lower, candidate_lower);
        merge_upper_bound(upper, candidate_upper);
    }

    let mut best_plan = None;
    let mut best_seek_cost = u64::MAX;
    for (property, (lower, upper)) in ranges {
        let label_count = catalog.label_count(label);
        let estimated_rows =
            catalog.estimate_range_bounds_rows(label, &property, lower.as_ref(), upper.as_ref());
        let scan_cost = estimate_node_full_scan_cost(label_count);
        let seek_cost =
            estimate_node_index_seek_cost(estimated_rows, NODE_INDEX_RANGE_STARTUP_COST);
        if node_index_seek_is_cheaper(label_count, seek_cost) && seek_cost < best_seek_cost {
            best_seek_cost = seek_cost;
            decisions.push(format!(
                "choose IndexNodeRangeSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ));
            best_plan = Some(PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeRangeSeek {
                    variable: scan_variable.to_string(),
                    label: label.to_string(),
                    property,
                    lower,
                    upper,
                }),
            });
        }
    }
    best_plan
}
