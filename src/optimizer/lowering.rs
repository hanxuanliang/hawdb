use super::{
    access_path::index_seek_from_filter,
    costing::{
        estimate_node_cartesian_product_cost, estimate_physical_plan_cost,
        push_optional_relationship_count_sum_cost_decision,
    },
    selected_trace::selected_plan_trace,
    stages::{
        DIRECT_PHYSICAL_FALLBACK_STAGE, LOGICAL_GROUPING_STAGE, PHYSICAL_SEARCH_STAGE,
        SELECTED_PLAN_COSTING_STAGE,
    },
    OptimizationSearchReport, OptimizerCatalog, OptimizerConfig, OptimizerTrace, PhysicalPlan,
    StageStats,
};
use crate::planner::LogicalPlan;
use crate::value::Value;
use skein_optimizer::{GroupId, Memo, StageTrace};
use std::collections::BTreeMap;

mod simple;

type GraphMemo = Memo<GroupExpr>;

#[derive(Debug)]
struct GroupExpr {
    logical: LogicalPlan,
    children: Vec<GroupId>,
}

#[derive(Debug, Default, Clone)]
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
        self.optimize_with_catalog(logical, &OptimizerCatalog::optimistic())
    }

    pub fn optimize_with_catalog(
        &self,
        logical: &LogicalPlan,
        catalog: &OptimizerCatalog,
    ) -> (PhysicalPlan, OptimizerTrace) {
        let required_groups = logical_group_count(logical);
        if required_groups > self.config.max_groups {
            let mut decisions = Vec::new();
            let mut stage_events = Vec::new();
            let plan =
                logical_to_physical_direct(logical, catalog, &mut decisions, &mut stage_events);
            let mut report =
                OptimizationSearchReport::direct_fallback(required_groups, self.config.max_groups);
            report.push_stage_event(
                LOGICAL_GROUPING_STAGE.trace(StageStats::new(1, required_groups)),
            );
            for event in stage_events {
                report.push_stage_event(event);
            }
            report.push_stage_event(
                DIRECT_PHYSICAL_FALLBACK_STAGE.trace(StageStats::new(required_groups, 1)),
            );
            report.extend_decisions(decisions);
            let selected = selected_plan_trace(&plan, catalog);
            report.push_stage_event(SELECTED_PLAN_COSTING_STAGE.trace(StageStats::new(1, 1)));
            report.record_selected_plan_cost(selected.cost);
            return (plan, report.into_trace(selected));
        }
        let mut memo = GraphMemo::default();
        let root = insert_logical_group(&mut memo, logical);
        let mut decisions = Vec::new();
        let mut stage_events = Vec::new();
        let plan = best_physical(&memo, root, catalog, &mut decisions, &mut stage_events);
        let mut report = OptimizationSearchReport::memo(memo.group_count());
        report
            .push_stage_event(LOGICAL_GROUPING_STAGE.trace(StageStats::new(1, memo.group_count())));
        let (applied_rules, skipped_rules) = stage_rule_counts(&stage_events);
        for event in stage_events {
            report.push_stage_event(event);
        }
        report.push_stage_event(PHYSICAL_SEARCH_STAGE.trace(
            StageStats::new(memo.group_count(), 1).with_rule_counts(applied_rules, skipped_rules),
        ));
        report.extend_decisions(decisions);
        let selected = selected_plan_trace(&plan, catalog);
        report.push_stage_event(SELECTED_PLAN_COSTING_STAGE.trace(StageStats::new(1, 1)));
        report.record_selected_plan_cost(selected.cost);
        (plan, report.into_trace(selected))
    }
}

fn insert_logical_group(memo: &mut GraphMemo, logical: &LogicalPlan) -> GroupId {
    let expr = GroupExpr::from_logical(logical, memo);
    memo.insert_group(expr)
}

fn best_physical(
    memo: &GraphMemo,
    root: GroupId,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> PhysicalPlan {
    let group = memo.group(root).expect("memo group id should exist");
    group
        .first_expression()
        .expect("memo group should contain at least one expression")
        .to_physical(memo, catalog, decisions, stage_events)
}

fn stage_rule_counts(stage_events: &[StageTrace]) -> (usize, usize) {
    stage_events
        .iter()
        .fold((0, 0), |(applied, skipped), event| {
            let stats = event.stats();
            (
                applied.saturating_add(stats.applied_rules),
                skipped.saturating_add(stats.skipped_rules),
            )
        })
}

impl GroupExpr {
    fn from_logical(logical: &LogicalPlan, memo: &mut GraphMemo) -> Self {
        match logical {
            LogicalPlan::CreateNodeLabel { .. }
            | LogicalPlan::CreateRelationshipType { .. }
            | LogicalPlan::CreateNodeTable { .. }
            | LogicalPlan::CreateRelationshipTable { .. }
            | LogicalPlan::CreateProperty { .. }
            | LogicalPlan::AlterTableState { .. }
            | LogicalPlan::AlterPropertyState { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::CreateCompositeIndex { .. }
            | LogicalPlan::CreateRangeIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateUniqueConstraint { .. }
            | LogicalPlan::CreateNodePropertyExistsConstraint { .. }
            | LogicalPlan::CreateRelationshipUniqueConstraint { .. }
            | LogicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
            | LogicalPlan::ProjectGraph { .. }
            | LogicalPlan::GraphAlgorithm { .. }
            | LogicalPlan::CreateNode { .. }
            | LogicalPlan::MergeNode { .. }
            | LogicalPlan::MergeRelationship { .. }
            | LogicalPlan::MergeMatchedRelationship { .. }
            | LogicalPlan::MergeRelationshipFromMatchedRelationship { .. }
            | LogicalPlan::MergeRelationshipToMatchedTarget { .. }
            | LogicalPlan::MergeRelationshipFromMatchedTarget { .. }
            | LogicalPlan::CreateMatchedRelationship { .. }
            | LogicalPlan::SetNodeProperty { .. }
            | LogicalPlan::SetNodeProperties { .. }
            | LogicalPlan::SetNodePropertiesReturn { .. }
            | LogicalPlan::SetRelationshipProperty { .. }
            | LogicalPlan::SetRelationshipProperties { .. }
            | LogicalPlan::DeleteNode { .. }
            | LogicalPlan::DeleteRelationship { .. }
            | LogicalPlan::DeleteRelationshipTargetNodes { .. }
            | LogicalPlan::CreateRelationship { .. }
            | LogicalPlan::NodeScan { .. }
            | LogicalPlan::OptionalRelationshipCountSum { .. }
            | LogicalPlan::ThreadRepairStats { .. }
            | LogicalPlan::ShortestPath { .. } => Self {
                logical: logical.clone(),
                children: Vec::new(),
            },
            LogicalPlan::NodeCartesianProduct { left, right } => Self {
                logical: logical.clone(),
                children: vec![
                    insert_logical_group(memo, left),
                    insert_logical_group(memo, right),
                ],
            },
            LogicalPlan::Expand { input, .. }
            | LogicalPlan::NodeColumnLookup { input, .. }
            | LogicalPlan::OptionalDegree { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => Self {
                logical: logical.clone(),
                children: vec![insert_logical_group(memo, input)],
            },
        }
    }

    fn to_physical(
        &self,
        memo: &GraphMemo,
        catalog: &OptimizerCatalog,
        decisions: &mut Vec<String>,
        stage_events: &mut Vec<StageTrace>,
    ) -> PhysicalPlan {
        if let Some(plan) = simple::lower_simple_logical(&self.logical) {
            return plan;
        }
        match &self.logical {
            LogicalPlan::CreateNodeLabel { label } => PhysicalPlan::CreateNodeLabel {
                label: label.clone(),
            },
            LogicalPlan::CreateRelationshipType { rel_type } => {
                PhysicalPlan::CreateRelationshipType {
                    rel_type: rel_type.clone(),
                }
            }
            LogicalPlan::CreateNodeTable { name } => {
                PhysicalPlan::CreateNodeTable { name: name.clone() }
            }
            LogicalPlan::CreateRelationshipTable { name } => {
                PhysicalPlan::CreateRelationshipTable { name: name.clone() }
            }
            LogicalPlan::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => PhysicalPlan::CreateProperty {
                table_kind: *table_kind,
                table: table.clone(),
                property: property.clone(),
                value_type: *value_type,
                nullable: *nullable,
            },
            LogicalPlan::AlterTableState {
                table_kind,
                table,
                state,
            } => PhysicalPlan::AlterTableState {
                table_kind: *table_kind,
                table: table.clone(),
                state: *state,
            },
            LogicalPlan::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => PhysicalPlan::AlterPropertyState {
                table_kind: *table_kind,
                table: table.clone(),
                property: property.clone(),
                state: *state,
            },
            LogicalPlan::CreateIndex { label, property } => PhysicalPlan::CreateIndex {
                label: label.clone(),
                property: property.clone(),
            },
            LogicalPlan::CreateCompositeIndex { label, properties } => {
                PhysicalPlan::CreateCompositeIndex {
                    label: label.clone(),
                    properties: properties.clone(),
                }
            }
            LogicalPlan::CreateRangeIndex { label, property } => PhysicalPlan::CreateRangeIndex {
                label: label.clone(),
                property: property.clone(),
            },
            LogicalPlan::CreateFullTextIndex { label, property } => {
                PhysicalPlan::CreateFullTextIndex {
                    label: label.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateUniqueConstraint { label, property } => {
                PhysicalPlan::CreateUniqueConstraint {
                    label: label.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
                PhysicalPlan::CreateNodePropertyExistsConstraint {
                    label: label.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
                PhysicalPlan::CreateRelationshipUniqueConstraint {
                    rel_type: rel_type.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                PhysicalPlan::CreateRelationshipPropertyExistsConstraint {
                    rel_type: rel_type.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => PhysicalPlan::ProjectGraph {
                name: name.clone(),
                node_labels: node_labels.clone(),
                rel_types: rel_types.clone(),
            },
            LogicalPlan::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
                score_column,
            } => PhysicalPlan::GraphAlgorithm {
                algorithm: *algorithm,
                graph_name: graph_name.clone(),
                options: *options,
                score_column: score_column.clone(),
            },
            LogicalPlan::CreateNode { label, properties } => PhysicalPlan::CreateNode {
                label: label.clone(),
                properties: properties.clone(),
            },
            LogicalPlan::MergeNode {
                label,
                match_properties,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            } => PhysicalPlan::MergeNode {
                label: label.clone(),
                match_properties: match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
                on_match_assignments: on_match_assignments.clone(),
                post_merge_assignments: post_merge_assignments.clone(),
            },
            LogicalPlan::MergeRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => PhysicalPlan::MergeRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
            LogicalPlan::MergeMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_match_properties,
                on_create_properties,
            } => PhysicalPlan::MergeMatchedRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                rel_type: rel_type.clone(),
                rel_match_properties: rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label,
                source_properties,
                old_rel_variable: _,
                old_rel_type,
                old_rel_properties,
                target_label,
                target_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => PhysicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                old_rel_type: old_rel_type.clone(),
                old_rel_properties: old_rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::MergeRelationshipToMatchedTarget {
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
            } => PhysicalPlan::MergeRelationshipToMatchedTarget {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                old_rel_type: old_rel_type.clone(),
                old_rel_properties: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_properties: old_target_properties.clone(),
                new_target_label: new_target_label.clone(),
                new_target_properties: new_target_properties.clone(),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::MergeRelationshipFromMatchedTarget {
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
            } => PhysicalPlan::MergeRelationshipFromMatchedTarget {
                old_source_label: old_source_label.clone(),
                old_source_properties: old_source_properties.clone(),
                old_rel_type: old_rel_type.clone(),
                old_rel_properties: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_properties: old_target_properties.clone(),
                new_source_label: new_source_label.clone(),
                new_source_properties: new_source_properties.clone(),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::CreateMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_properties,
            } => PhysicalPlan::CreateMatchedRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
            },
            LogicalPlan::SetNodeProperty {
                variable,
                label,
                predicate,
                property,
                value,
            } => PhysicalPlan::SetNodeProperty {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                property: property.clone(),
                value: value.clone(),
            },
            LogicalPlan::SetNodeProperties {
                variable,
                label,
                predicate,
                assignments,
            } => PhysicalPlan::SetNodeProperties {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                assignments: assignments.clone(),
            },
            LogicalPlan::SetNodePropertiesReturn {
                variable,
                label,
                predicate,
                assignments,
                returns,
            } => PhysicalPlan::SetNodePropertiesReturn {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                assignments: assignments.clone(),
                returns: returns.clone(),
            },
            LogicalPlan::SetRelationshipProperty {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                property,
                value,
            } => PhysicalPlan::SetRelationshipProperty {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                predicate: predicate.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                rel_predicate: rel_predicate.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                property: property.clone(),
                value: value.clone(),
            },
            LogicalPlan::SetRelationshipProperties {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                assignments,
            } => PhysicalPlan::SetRelationshipProperties {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                predicate: predicate.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                rel_predicate: rel_predicate.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                assignments: assignments.clone(),
            },
            LogicalPlan::DeleteNode {
                variable,
                label,
                predicate,
                detach,
            } => PhysicalPlan::DeleteNode {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                detach: *detach,
            },
            LogicalPlan::DeleteRelationship {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
            } => PhysicalPlan::DeleteRelationship {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                predicate: predicate.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                rel_predicate: rel_predicate.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
            LogicalPlan::DeleteRelationshipTargetNodes {
                source_variable,
                source_label,
                source_predicate,
                rel_type,
                rel_properties,
                target_variable,
                target_label,
                target_properties,
                detach,
            } => PhysicalPlan::DeleteRelationshipTargetNodes {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                source_predicate: source_predicate.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                detach: *detach,
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
            LogicalPlan::NodeCartesianProduct { .. } => {
                let left = best_physical(memo, self.children[0], catalog, decisions, stage_events);
                let right = best_physical(memo, self.children[1], catalog, decisions, stage_events);
                let (left, right) =
                    order_single_row_cartesian_product_children(catalog, decisions, left, right);
                push_cartesian_product_cost_decision(catalog, decisions, &left, &right);
                PhysicalPlan::NodeCartesianProductExec {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            LogicalPlan::NodeColumnLookup {
                variable,
                label,
                property,
                column,
                optional,
                ..
            } => PhysicalPlan::NodeColumnLookupExec {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                column: column.clone(),
                optional: *optional,
                input: Box::new(best_physical(
                    memo,
                    self.children[0],
                    catalog,
                    decisions,
                    stage_events,
                )),
            },
            LogicalPlan::Expand {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                optional,
                ..
            } => {
                push_expand_estimate_decision(
                    catalog,
                    decisions,
                    ExpandEstimateRequest {
                        source_label,
                        rel_type,
                        rel_properties,
                        target_label,
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                    },
                );
                PhysicalPlan::AdjacencyExpandExec {
                    source_variable: source_variable.clone(),
                    source_label: source_label.clone(),
                    rel_variable: rel_variable.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    direction: *direction,
                    target_variable: target_variable.clone(),
                    target_label: target_label.clone(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    optional: *optional,
                    input: Box::new(best_physical(
                        memo,
                        self.children[0],
                        catalog,
                        decisions,
                        stage_events,
                    )),
                }
            }
            LogicalPlan::OptionalDegree {
                source_variable,
                rel_type,
                rel_properties,
                direction,
                target_label,
                target_properties,
                alias,
                ..
            } => PhysicalPlan::OptionalDegreeExec {
                source_variable: source_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                direction: *direction,
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                alias: alias.clone(),
                input: Box::new(best_physical(
                    memo,
                    self.children[0],
                    catalog,
                    decisions,
                    stage_events,
                )),
            },
            LogicalPlan::OptionalRelationshipCountSum {
                variable,
                label,
                properties,
                legs,
                output,
            } => {
                push_optional_relationship_count_sum_cost_decision(
                    catalog, decisions, label, properties, legs,
                );
                PhysicalPlan::OptionalRelationshipCountSumExec {
                    variable: variable.clone(),
                    label: label.clone(),
                    properties: properties.clone(),
                    legs: legs.clone(),
                    output: output.clone(),
                }
            }
            LogicalPlan::ThreadRepairStats {
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
            } => PhysicalPlan::ThreadRepairStatsExec {
                label: label.clone(),
                identity_label: identity_label.clone(),
                identity_ref_property: identity_ref_property.clone(),
                thread_id_property: thread_id_property.clone(),
                message_rel_type: message_rel_type.clone(),
                message_label: message_label.clone(),
                memory_rel_type: memory_rel_type.clone(),
                memory_label: memory_label.clone(),
            },
            LogicalPlan::ShortestPath {
                source_variable,
                source_label,
                source_id,
                rel_type,
                direction,
                target_variable,
                target_label,
                target_id,
                min_hops,
                max_hops,
                returns,
            } => PhysicalPlan::ShortestPathExec {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                source_id: source_id.clone(),
                rel_type: rel_type.clone(),
                direction: *direction,
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_id: target_id.clone(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                returns: returns.clone(),
            },
            LogicalPlan::Filter { predicate, input } => {
                if let Some(plan) =
                    index_seek_from_filter(predicate, input, catalog, decisions, stage_events)
                {
                    plan
                } else {
                    PhysicalPlan::FilterExec {
                        predicate: predicate.clone(),
                        input: Box::new(best_physical(
                            memo,
                            self.children[0],
                            catalog,
                            decisions,
                            stage_events,
                        )),
                    }
                }
            }
            LogicalPlan::Project { items, .. } => PhysicalPlan::ProjectExec {
                items: items.clone(),
                input: Box::new(best_physical(
                    memo,
                    self.children[0],
                    catalog,
                    decisions,
                    stage_events,
                )),
            },
            LogicalPlan::Aggregate {
                group_keys, items, ..
            } => PhysicalPlan::AggregateExec {
                group_keys: group_keys.clone(),
                items: items.clone(),
                input: Box::new(best_physical(
                    memo,
                    self.children[0],
                    catalog,
                    decisions,
                    stage_events,
                )),
            },
            LogicalPlan::Distinct { .. } => PhysicalPlan::DistinctExec {
                input: Box::new(best_physical(
                    memo,
                    self.children[0],
                    catalog,
                    decisions,
                    stage_events,
                )),
            },
            LogicalPlan::Sort { items, .. } => PhysicalPlan::SortExec {
                items: items.clone(),
                input: Box::new(best_physical(
                    memo,
                    self.children[0],
                    catalog,
                    decisions,
                    stage_events,
                )),
            },
            LogicalPlan::Limit { offset, limit, .. } => PhysicalPlan::LimitExec {
                offset: *offset,
                limit: *limit,
                input: Box::new(best_physical(
                    memo,
                    self.children[0],
                    catalog,
                    decisions,
                    stage_events,
                )),
            },
        }
    }
}

fn logical_group_count(logical: &LogicalPlan) -> usize {
    match logical {
        LogicalPlan::Expand { input, .. }
        | LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::OptionalDegree { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => 1 + logical_group_count(input),
        LogicalPlan::OptionalRelationshipCountSum { .. }
        | LogicalPlan::ThreadRepairStats { .. }
        | LogicalPlan::ShortestPath { .. } => 1,
        LogicalPlan::CreateNodeLabel { .. }
        | LogicalPlan::CreateRelationshipType { .. }
        | LogicalPlan::CreateNodeTable { .. }
        | LogicalPlan::CreateRelationshipTable { .. }
        | LogicalPlan::CreateProperty { .. }
        | LogicalPlan::AlterTableState { .. }
        | LogicalPlan::AlterPropertyState { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::CreateCompositeIndex { .. }
        | LogicalPlan::CreateRangeIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateUniqueConstraint { .. }
        | LogicalPlan::CreateNodePropertyExistsConstraint { .. }
        | LogicalPlan::CreateRelationshipUniqueConstraint { .. }
        | LogicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | LogicalPlan::ProjectGraph { .. }
        | LogicalPlan::GraphAlgorithm { .. }
        | LogicalPlan::CreateNode { .. }
        | LogicalPlan::MergeNode { .. }
        | LogicalPlan::MergeRelationship { .. }
        | LogicalPlan::MergeMatchedRelationship { .. }
        | LogicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | LogicalPlan::MergeRelationshipToMatchedTarget { .. }
        | LogicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | LogicalPlan::CreateMatchedRelationship { .. }
        | LogicalPlan::SetNodeProperty { .. }
        | LogicalPlan::SetNodeProperties { .. }
        | LogicalPlan::SetNodePropertiesReturn { .. }
        | LogicalPlan::SetRelationshipProperty { .. }
        | LogicalPlan::SetRelationshipProperties { .. }
        | LogicalPlan::DeleteNode { .. }
        | LogicalPlan::DeleteRelationship { .. }
        | LogicalPlan::DeleteRelationshipTargetNodes { .. }
        | LogicalPlan::CreateRelationship { .. }
        | LogicalPlan::NodeScan { .. } => 1,
        LogicalPlan::NodeCartesianProduct { left, right } => {
            1 + logical_group_count(left) + logical_group_count(right)
        }
    }
}

fn logical_to_physical_direct(
    logical: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    stage_events: &mut Vec<StageTrace>,
) -> PhysicalPlan {
    if let Some(plan) = simple::lower_simple_logical(logical) {
        return plan;
    }
    match logical {
        LogicalPlan::CreateNodeLabel { label } => PhysicalPlan::CreateNodeLabel {
            label: label.clone(),
        },
        LogicalPlan::CreateRelationshipType { rel_type } => PhysicalPlan::CreateRelationshipType {
            rel_type: rel_type.clone(),
        },
        LogicalPlan::CreateNodeTable { name } => {
            PhysicalPlan::CreateNodeTable { name: name.clone() }
        }
        LogicalPlan::CreateRelationshipTable { name } => {
            PhysicalPlan::CreateRelationshipTable { name: name.clone() }
        }
        LogicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => PhysicalPlan::CreateProperty {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            value_type: *value_type,
            nullable: *nullable,
        },
        LogicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => PhysicalPlan::AlterTableState {
            table_kind: *table_kind,
            table: table.clone(),
            state: *state,
        },
        LogicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => PhysicalPlan::AlterPropertyState {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            state: *state,
        },
        LogicalPlan::CreateIndex { label, property } => PhysicalPlan::CreateIndex {
            label: label.clone(),
            property: property.clone(),
        },
        LogicalPlan::CreateCompositeIndex { label, properties } => {
            PhysicalPlan::CreateCompositeIndex {
                label: label.clone(),
                properties: properties.clone(),
            }
        }
        LogicalPlan::CreateRangeIndex { label, property } => PhysicalPlan::CreateRangeIndex {
            label: label.clone(),
            property: property.clone(),
        },
        LogicalPlan::CreateFullTextIndex { label, property } => PhysicalPlan::CreateFullTextIndex {
            label: label.clone(),
            property: property.clone(),
        },
        LogicalPlan::CreateUniqueConstraint { label, property } => {
            PhysicalPlan::CreateUniqueConstraint {
                label: label.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            PhysicalPlan::CreateNodePropertyExistsConstraint {
                label: label.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            PhysicalPlan::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => PhysicalPlan::ProjectGraph {
            name: name.clone(),
            node_labels: node_labels.clone(),
            rel_types: rel_types.clone(),
        },
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
        } => PhysicalPlan::GraphAlgorithm {
            algorithm: *algorithm,
            graph_name: graph_name.clone(),
            options: *options,
            score_column: score_column.clone(),
        },
        LogicalPlan::CreateNode { label, properties } => PhysicalPlan::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        },
        LogicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => PhysicalPlan::MergeNode {
            label: label.clone(),
            match_properties: match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
            on_match_assignments: on_match_assignments.clone(),
            post_merge_assignments: post_merge_assignments.clone(),
        },
        LogicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => PhysicalPlan::MergeRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        },
        LogicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => PhysicalPlan::MergeMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            rel_type: rel_type.clone(),
            rel_match_properties: rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label,
            source_properties,
            old_rel_variable: _,
            old_rel_type,
            old_rel_properties,
            target_label,
            target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::MergeRelationshipToMatchedTarget {
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
        } => PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            old_target_label: old_target_label.clone(),
            old_target_properties: old_target_properties.clone(),
            new_target_label: new_target_label.clone(),
            new_target_properties: new_target_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::MergeRelationshipFromMatchedTarget {
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
        } => PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label: old_source_label.clone(),
            old_source_properties: old_source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            old_target_label: old_target_label.clone(),
            old_target_properties: old_target_properties.clone(),
            new_source_label: new_source_label.clone(),
            new_source_properties: new_source_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => PhysicalPlan::CreateMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
        },
        LogicalPlan::SetNodeProperty {
            variable,
            label,
            predicate,
            property,
            value,
        } => PhysicalPlan::SetNodeProperty {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            property: property.clone(),
            value: value.clone(),
        },
        LogicalPlan::SetNodeProperties {
            variable,
            label,
            predicate,
            assignments,
        } => PhysicalPlan::SetNodeProperties {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            assignments: assignments.clone(),
        },
        LogicalPlan::SetNodePropertiesReturn {
            variable,
            label,
            predicate,
            assignments,
            returns,
        } => PhysicalPlan::SetNodePropertiesReturn {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            assignments: assignments.clone(),
            returns: returns.clone(),
        },
        LogicalPlan::SetRelationshipProperty {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
            property,
            value,
        } => PhysicalPlan::SetRelationshipProperty {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            property: property.clone(),
            value: value.clone(),
        },
        LogicalPlan::SetRelationshipProperties {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
            assignments,
        } => PhysicalPlan::SetRelationshipProperties {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            assignments: assignments.clone(),
        },
        LogicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => PhysicalPlan::DeleteNode {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            detach: *detach,
        },
        LogicalPlan::DeleteRelationship {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
        } => PhysicalPlan::DeleteRelationship {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        },
        LogicalPlan::DeleteRelationshipTargetNodes {
            source_variable,
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_variable,
            target_label,
            target_properties,
            detach,
        } => PhysicalPlan::DeleteRelationshipTargetNodes {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_predicate: source_predicate.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            detach: *detach,
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
        LogicalPlan::NodeCartesianProduct { left, right } => {
            let left = logical_to_physical_direct(left, catalog, decisions, stage_events);
            let right = logical_to_physical_direct(right, catalog, decisions, stage_events);
            let (left, right) =
                order_single_row_cartesian_product_children(catalog, decisions, left, right);
            push_cartesian_product_cost_decision(catalog, decisions, &left, &right);
            PhysicalPlan::NodeCartesianProductExec {
                left: Box::new(left),
                right: Box::new(right),
            }
        }
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => PhysicalPlan::NodeColumnLookupExec {
            variable: variable.clone(),
            label: label.clone(),
            property: property.clone(),
            column: column.clone(),
            optional: *optional,
            input: Box::new(logical_to_physical_direct(
                input,
                catalog,
                decisions,
                stage_events,
            )),
        },
        LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input,
        } => {
            push_expand_estimate_decision(
                catalog,
                decisions,
                ExpandEstimateRequest {
                    source_label,
                    rel_type,
                    rel_properties,
                    target_label,
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                },
            );
            PhysicalPlan::AdjacencyExpandExec {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                direction: *direction,
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                optional: *optional,
                input: Box::new(logical_to_physical_direct(
                    input,
                    catalog,
                    decisions,
                    stage_events,
                )),
            }
        }
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => PhysicalPlan::OptionalDegreeExec {
            source_variable: source_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            direction: *direction,
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            alias: alias.clone(),
            input: Box::new(logical_to_physical_direct(
                input,
                catalog,
                decisions,
                stage_events,
            )),
        },
        LogicalPlan::OptionalRelationshipCountSum {
            variable,
            label,
            properties,
            legs,
            output,
        } => {
            push_optional_relationship_count_sum_cost_decision(
                catalog, decisions, label, properties, legs,
            );
            PhysicalPlan::OptionalRelationshipCountSumExec {
                variable: variable.clone(),
                label: label.clone(),
                properties: properties.clone(),
                legs: legs.clone(),
                output: output.clone(),
            }
        }
        LogicalPlan::ThreadRepairStats {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => PhysicalPlan::ThreadRepairStatsExec {
            label: label.clone(),
            identity_label: identity_label.clone(),
            identity_ref_property: identity_ref_property.clone(),
            thread_id_property: thread_id_property.clone(),
            message_rel_type: message_rel_type.clone(),
            message_label: message_label.clone(),
            memory_rel_type: memory_rel_type.clone(),
            memory_label: memory_label.clone(),
        },
        LogicalPlan::ShortestPath {
            source_variable,
            source_label,
            source_id,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            min_hops,
            max_hops,
            returns,
        } => PhysicalPlan::ShortestPathExec {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_id: source_id.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_id: target_id.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            returns: returns.clone(),
        },
        LogicalPlan::Filter { predicate, input } => {
            if let Some(plan) =
                index_seek_from_filter(predicate, input, catalog, decisions, stage_events)
            {
                plan
            } else {
                PhysicalPlan::FilterExec {
                    predicate: predicate.clone(),
                    input: Box::new(logical_to_physical_direct(
                        input,
                        catalog,
                        decisions,
                        stage_events,
                    )),
                }
            }
        }
        LogicalPlan::Project { items, input } => PhysicalPlan::ProjectExec {
            items: items.clone(),
            input: Box::new(logical_to_physical_direct(
                input,
                catalog,
                decisions,
                stage_events,
            )),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => PhysicalPlan::AggregateExec {
            group_keys: group_keys.clone(),
            items: items.clone(),
            input: Box::new(logical_to_physical_direct(
                input,
                catalog,
                decisions,
                stage_events,
            )),
        },
        LogicalPlan::Distinct { input } => PhysicalPlan::DistinctExec {
            input: Box::new(logical_to_physical_direct(
                input,
                catalog,
                decisions,
                stage_events,
            )),
        },
        LogicalPlan::Sort { items, input } => PhysicalPlan::SortExec {
            items: items.clone(),
            input: Box::new(logical_to_physical_direct(
                input,
                catalog,
                decisions,
                stage_events,
            )),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => PhysicalPlan::LimitExec {
            offset: *offset,
            limit: *limit,
            input: Box::new(logical_to_physical_direct(
                input,
                catalog,
                decisions,
                stage_events,
            )),
        },
    }
}

struct ExpandEstimateRequest<'a> {
    source_label: &'a str,
    rel_type: &'a str,
    rel_properties: &'a BTreeMap<String, Value>,
    target_label: &'a str,
    min_hops: usize,
    max_hops: usize,
}

fn push_expand_estimate_decision(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    request: ExpandEstimateRequest<'_>,
) {
    let estimate = catalog.estimate_expand_rows(
        request.source_label,
        request.rel_type,
        request.rel_properties,
        request.target_label,
        request.min_hops,
        request.max_hops,
    );
    let path_count = estimate
        .path_count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let hop_rows = estimate
        .hop_estimates
        .iter()
        .map(|estimate| {
            format!(
                "{}:{}:{}",
                estimate.hop,
                if estimate.exact { "exact" } else { "fallback" },
                estimate.rows
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    decisions.push(format!(
        "estimate AdjacencyExpand for {}-[:{}*{}..{}]->{}: path_count={path_count} rel_count={} rel_type_sources={} average_fanout={} rel_property_distinct_product={} hop_rows=[{}] estimated_rows={}",
        request.source_label,
        request.rel_type,
        request.min_hops,
        request.max_hops,
        request.target_label,
        estimate.rel_count,
        estimate.source_count,
        estimate.average_fanout,
        estimate.property_distinct_product,
        hop_rows,
        estimate.estimated_rows
    ));
}

fn order_single_row_cartesian_product_children(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    left: PhysicalPlan,
    right: PhysicalPlan,
) -> (PhysicalPlan, PhysicalPlan) {
    let left_cost = estimate_physical_plan_cost(&left, catalog);
    let right_cost = estimate_physical_plan_cost(&right, catalog);
    if !cartesian_product_leaves_are_single_row(catalog, &left)
        || !cartesian_product_leaves_are_single_row(catalog, &right)
    {
        decisions.push(format!(
            "keep NodeCartesianProduct input order: left_rows={} right_rows={} reason=non_single_row_input",
            left_cost.estimated_rows, right_cost.estimated_rows
        ));
        return (left, right);
    }

    let mut leaves = Vec::new();
    collect_cartesian_product_leaves(left, &mut leaves);
    collect_cartesian_product_leaves(right, &mut leaves);

    let mut keyed_leaves = leaves
        .into_iter()
        .map(|plan| {
            let cost = estimate_physical_plan_cost(&plan, catalog);
            let fingerprint = plan.fingerprint();
            (cost, fingerprint, plan)
        })
        .collect::<Vec<_>>();

    let original_order = keyed_leaves
        .iter()
        .map(|(cost, fingerprint, _)| format!("{}:{}", cost.cost, fingerprint))
        .collect::<Vec<_>>()
        .join("|");
    keyed_leaves.sort_by(
        |(left_cost, left_fingerprint, _), (right_cost, right_fingerprint, _)| {
            (left_cost.cost, left_fingerprint).cmp(&(right_cost.cost, right_fingerprint))
        },
    );
    let ordered = keyed_leaves
        .iter()
        .map(|(cost, fingerprint, _)| format!("{}:{}", cost.cost, fingerprint))
        .collect::<Vec<_>>()
        .join("|");
    if original_order == ordered {
        decisions.push(format!(
            "keep NodeCartesianProduct single-row input order: inputs={} order={ordered}",
            keyed_leaves.len()
        ));
    } else {
        decisions.push(format!(
            "order NodeCartesianProduct single-row inputs: inputs={} original={original_order} ordered={ordered}",
            keyed_leaves.len()
        ));
    }

    let mut ordered_plans = keyed_leaves.into_iter().map(|(_, _, plan)| plan);
    let left = ordered_plans
        .next()
        .expect("cartesian product has left input");
    let right = rebuild_cartesian_product(ordered_plans);
    (left, right)
}

fn cartesian_product_leaves_are_single_row(
    catalog: &OptimizerCatalog,
    plan: &PhysicalPlan,
) -> bool {
    match plan {
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            cartesian_product_leaves_are_single_row(catalog, left)
                && cartesian_product_leaves_are_single_row(catalog, right)
        }
        plan => estimate_physical_plan_cost(plan, catalog).estimated_rows == 1,
    }
}

fn collect_cartesian_product_leaves(plan: PhysicalPlan, leaves: &mut Vec<PhysicalPlan>) {
    match plan {
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            collect_cartesian_product_leaves(*left, leaves);
            collect_cartesian_product_leaves(*right, leaves);
        }
        plan => leaves.push(plan),
    }
}

fn rebuild_cartesian_product(plans: impl IntoIterator<Item = PhysicalPlan>) -> PhysicalPlan {
    let mut plans = plans.into_iter();
    let first = plans
        .next()
        .expect("cartesian product rebuild requires at least one input");
    plans.fold(first, |left, right| {
        PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(left),
            right: Box::new(right),
        }
    })
}

fn push_cartesian_product_cost_decision(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
) {
    let left_cost = estimate_physical_plan_cost(left, catalog);
    let right_cost = estimate_physical_plan_cost(right, catalog);
    let cost = estimate_node_cartesian_product_cost(left_cost, right_cost);
    decisions.push(format!(
        "estimate NodeCartesianProduct: left_rows={} right_rows={} output_rows={} left_cost={} right_cost={} cost={}",
        left_cost.estimated_rows,
        right_cost.estimated_rows,
        cost.estimated_rows,
        left_cost.cost,
        right_cost.cost,
        cost.cost
    ));
}
