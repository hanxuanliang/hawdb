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
    LogicalPlanRoot, OptimizationSearchReport, OptimizedLogicalPlanRoot, OptimizerCatalog,
    OptimizerConfig, OptimizerTrace, PhysicalPlan, PhysicalPlanRoot, StageStats,
};
use crate::{
    GroupId, Memo, OptimizerContext, OptimizerSearchDirective, OptimizerSearchDirectiveError,
    StageTrace,
};
use skein_core::Value;
use skein_plan::{GraphExpansionBudget, LogicalPlan, SortItem};
use std::collections::BTreeMap;

mod access;
mod ddl;
mod mutation;
mod procedure;
mod simple;
mod traversal;

type GraphMemo = Memo<GroupExpr>;

const GRAPH_EXPANSION_FANOUT_PER_SEED_HOP: usize = 32;
const GRAPH_EXPANSION_MAX_CANDIDATES: usize = 4_096;
const GRAPH_EXPANSION_PAYLOAD_BYTE_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug)]
struct GroupExpr {
    logical: LogicalPlan,
    children: Vec<GroupId>,
}

#[derive(Debug, Default, Clone)]
pub struct CascadesOptimizer {
    context: OptimizerContext,
}

impl CascadesOptimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            context: OptimizerContext::from_config(config),
        }
    }

    pub fn with_context(context: OptimizerContext) -> Self {
        Self { context }
    }

    pub fn context(&self) -> &OptimizerContext {
        &self.context
    }

    pub fn optimize(&self, logical: &LogicalPlan) -> PhysicalPlan {
        self.optimize_root(&LogicalPlanRoot::new(logical.clone()))
            .into_parts()
            .0
    }

    pub fn optimize_with_trace(&self, logical: &LogicalPlan) -> (PhysicalPlan, OptimizerTrace) {
        self.optimize_root(&LogicalPlanRoot::new(logical.clone()))
            .into_parts()
    }

    pub fn optimize_with_catalog(
        &self,
        logical: &LogicalPlan,
        catalog: &OptimizerCatalog,
    ) -> (PhysicalPlan, OptimizerTrace) {
        self.optimize_root_with_catalog(&LogicalPlanRoot::new(logical.clone()), catalog)
            .into_parts()
    }

    pub fn optimize_root(&self, root: &LogicalPlanRoot) -> PhysicalPlanRoot {
        self.optimize_root_with_catalog(root, &OptimizerCatalog::optimistic())
    }

    pub fn optimize_root_with_catalog(
        &self,
        root: &LogicalPlanRoot,
        catalog: &OptimizerCatalog,
    ) -> PhysicalPlanRoot {
        self.optimize_optimized_root_with_catalog_and_directive(
            &root.clone().into_optimized(),
            catalog,
            OptimizerSearchDirective::Auto,
        )
        .expect("automatic optimizer search cannot reject its directive")
    }

    pub fn optimize_optimized_root_with_catalog(
        &self,
        root: &OptimizedLogicalPlanRoot,
        catalog: &OptimizerCatalog,
    ) -> PhysicalPlanRoot {
        self.optimize_optimized_root_with_catalog_and_directive(
            root,
            catalog,
            OptimizerSearchDirective::Auto,
        )
        .expect("automatic optimizer search cannot reject its directive")
    }

    pub fn optimize_root_with_catalog_and_directive(
        &self,
        root: &LogicalPlanRoot,
        catalog: &OptimizerCatalog,
        directive: OptimizerSearchDirective,
    ) -> Result<PhysicalPlanRoot, OptimizerSearchDirectiveError> {
        self.optimize_optimized_root_with_catalog_and_directive(
            &root.clone().into_optimized(),
            catalog,
            directive,
        )
    }

    pub fn optimize_optimized_root_with_catalog_and_directive(
        &self,
        root: &OptimizedLogicalPlanRoot,
        catalog: &OptimizerCatalog,
        directive: OptimizerSearchDirective,
    ) -> Result<PhysicalPlanRoot, OptimizerSearchDirectiveError> {
        let logical = root.plan();
        let required_groups = logical_group_count(logical);
        let max_groups = self.context.optimizer_config().max_groups;
        if directive == OptimizerSearchDirective::Memo && required_groups > max_groups {
            return Err(OptimizerSearchDirectiveError::MemoGroupBudgetExceeded {
                required_groups,
                max_groups,
            });
        }
        if directive == OptimizerSearchDirective::DirectFallback
            || (directive == OptimizerSearchDirective::Auto && required_groups > max_groups)
        {
            let mut decisions = Vec::new();
            let mut stage_events = Vec::new();
            let plan =
                logical_to_physical_direct(logical, catalog, &mut decisions, &mut stage_events);
            let mut report = if directive == OptimizerSearchDirective::DirectFallback {
                OptimizationSearchReport::forced_direct_fallback(required_groups)
            } else {
                OptimizationSearchReport::direct_fallback(required_groups, max_groups)
            };
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
            let selected = selected_plan_trace(&plan, catalog, &self.context);
            report.push_stage_event(SELECTED_PLAN_COSTING_STAGE.trace(StageStats::new(1, 1)));
            report.record_selected_plan_cost(selected.cost);
            return Ok(PhysicalPlanRoot::new(plan, report.into_trace(selected)));
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
        let selected = selected_plan_trace(&plan, catalog, &self.context);
        report.push_stage_event(SELECTED_PLAN_COSTING_STAGE.trace(StageStats::new(1, 1)));
        report.record_selected_plan_cost(selected.cost);
        if directive == OptimizerSearchDirective::Memo {
            report.push_decision("selected memo search: explicit optimizer search directive");
        }
        Ok(PhysicalPlanRoot::new(plan, report.into_trace(selected)))
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

fn select_bounded_sort_plan(
    items: Vec<SortItem>,
    offset: usize,
    limit: usize,
    input: PhysicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> PhysicalPlan {
    let top_n = PhysicalPlan::TopNExec {
        items: items.clone(),
        offset,
        limit,
        input: Box::new(input.clone()),
    };
    let sort_limit = PhysicalPlan::LimitExec {
        offset,
        limit: Some(limit),
        input: Box::new(PhysicalPlan::SortExec {
            items,
            input: Box::new(input),
        }),
    };
    let top_n_cost = estimate_physical_plan_cost(&top_n, catalog);
    let sort_limit_cost = estimate_physical_plan_cost(&sort_limit, catalog);
    if top_n_cost.cost < sort_limit_cost.cost {
        decisions.push(format!(
            "choose TopN for bounded sort: offset={offset} limit={limit} top_n_cost={} sort_limit_cost={}",
            top_n_cost.cost, sort_limit_cost.cost
        ));
        top_n
    } else {
        decisions.push(format!(
            "keep Sort + Limit for bounded sort: offset={offset} limit={limit} top_n_cost={} sort_limit_cost={}",
            top_n_cost.cost, sort_limit_cost.cost
        ));
        sort_limit
    }
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
            | LogicalPlan::VectorSeed { .. }
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
            LogicalPlan::Limit {
                limit: Some(_),
                input,
                ..
            } if matches!(input.as_ref(), LogicalPlan::Sort { .. }) => {
                let LogicalPlan::Sort { input, .. } = input.as_ref() else {
                    unreachable!("guard requires a sort input");
                };
                Self {
                    logical: logical.clone(),
                    children: vec![insert_logical_group(memo, input)],
                }
            }
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
                let input = best_physical(memo, self.children[0], catalog, decisions, stage_events);
                let graph_budget =
                    vector_seed_top_k(&input).map(|top_k| graph_expansion_budget(top_k, *max_hops));
                if let Some(graph_budget) = graph_budget {
                    decisions.push(format!(
                        "bound vector-seeded graph expansion to {} candidates and {} payload bytes",
                        graph_budget.candidate_limit, graph_budget.payload_byte_limit
                    ));
                }
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
                    graph_budget,
                    input: Box::new(input),
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
            LogicalPlan::Filter { predicate, input } => {
                if let Some(plan) =
                    index_seek_from_filter(predicate, input, catalog, decisions, stage_events)
                {
                    plan
                } else if let Some(plan) = access::source_segment_scan_from_filter(predicate, input)
                {
                    decisions.push(
                        "choose SourceSegmentScan for storage-prunable Source filter".to_string(),
                    );
                    PhysicalPlan::FilterExec {
                        predicate: predicate.clone(),
                        input: Box::new(plan),
                    }
                } else {
                    let mut input =
                        best_physical(memo, self.children[0], catalog, decisions, stage_events);
                    push_vector_seed_metadata_filter(&mut input, predicate, decisions);
                    PhysicalPlan::FilterExec {
                        predicate: predicate.clone(),
                        input: Box::new(input),
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
            LogicalPlan::Limit {
                offset,
                limit: Some(limit),
                input,
            } if matches!(input.as_ref(), LogicalPlan::Sort { .. }) => {
                let LogicalPlan::Sort { items, .. } = input.as_ref() else {
                    unreachable!("guard requires a sort input");
                };
                let input = best_physical(memo, self.children[0], catalog, decisions, stage_events);
                select_bounded_sort_plan(items.clone(), *offset, *limit, input, catalog, decisions)
            }
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
            _ => unreachable!("leaf logical plans are lowered before memo child planning"),
        }
    }
}

fn push_vector_seed_metadata_filter(
    input: &mut PhysicalPlan,
    predicate: &skein_plan::Predicate,
    decisions: &mut Vec<String>,
) {
    if let skein_plan::Predicate::And(predicates) = predicate {
        for predicate in predicates {
            push_vector_seed_metadata_filter(input, predicate, decisions);
        }
        return;
    }
    let skein_plan::Predicate::PropertyEq {
        variable,
        property,
        value,
    } = predicate
    else {
        return;
    };
    let Some(filter_field) = vector_seed_metadata_field(property) else {
        return;
    };
    let Some(filter_value) = vector_seed_metadata_value(value) else {
        return;
    };
    if attach_vector_seed_metadata_filter(input, variable, filter_field, filter_value) {
        decisions.push(format!(
            "push descriptor-safe vector seed filter {variable}.{property} before candidate generation"
        ));
    }
}

fn attach_vector_seed_metadata_filter(
    plan: &mut PhysicalPlan,
    variable: &str,
    field: &str,
    value: String,
) -> bool {
    match plan {
        PhysicalPlan::NodeColumnLookupExec {
            variable: lookup_variable,
            input,
            ..
        } if lookup_variable == variable => {
            attach_metadata_filter_to_vector_seed(input, field, value)
        }
        PhysicalPlan::AdjacencyExpandExec {
            source_variable,
            input,
            ..
        } if source_variable == variable => {
            attach_vector_seed_metadata_filter(input, variable, field, value)
        }
        _ => false,
    }
}

fn attach_metadata_filter_to_vector_seed(
    plan: &mut PhysicalPlan,
    field: &str,
    value: String,
) -> bool {
    let PhysicalPlan::VectorSeedScan {
        metadata_filters,
        vector_plan,
        ..
    } = plan
    else {
        return false;
    };
    if metadata_filters
        .get(field)
        .is_some_and(|existing| existing != &value)
    {
        return false;
    }
    metadata_filters.insert(field.to_string(), value);
    attach_vector_filter_field(vector_plan, field);
    true
}

fn attach_vector_filter_field(plan: &mut skein_plan::VectorPhysicalPlan, field: &str) {
    match plan {
        skein_plan::VectorPhysicalPlan::Filter { fields } => {
            if !fields.iter().any(|existing| existing == field) {
                fields.push(field.to_string());
                fields.sort();
            }
        }
        skein_plan::VectorPhysicalPlan::VectorCandidateScan { input, .. }
        | skein_plan::VectorPhysicalPlan::ResidualFilter { input, .. }
        | skein_plan::VectorPhysicalPlan::RawVectorRerank { input, .. }
        | skein_plan::VectorPhysicalPlan::TopK { input, .. } => {
            attach_vector_filter_field(input, field);
        }
    }
}

fn vector_seed_metadata_field(property: &str) -> Option<&str> {
    match property {
        "id" => Some("external_id"),
        "kind" | "external_id" | "source_id" | "space_id" | "unit_type" | "lifecycle_state"
        | "importance" | "confidence" | "created_at" | "updated_at" | "event_start"
        | "event_end" | "is_latest" => Some(property),
        _ => None,
    }
}

fn vector_seed_metadata_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Int(value) => Some(value.to_string()),
        Value::Float(value) if value.is_finite() => Some(value.to_string()),
        Value::Null | Value::Float(_) | Value::List(_) | Value::Map(_) => None,
    }
}

fn vector_seed_top_k(plan: &PhysicalPlan) -> Option<usize> {
    match plan {
        PhysicalPlan::VectorSeedScan { vector_plan, .. } => vector_plan_top_k(vector_plan),
        PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::AdjacencyExpandExec { input, .. }
        | PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => vector_seed_top_k(input),
        _ => None,
    }
}

fn vector_plan_top_k(plan: &skein_plan::VectorPhysicalPlan) -> Option<usize> {
    match plan {
        skein_plan::VectorPhysicalPlan::TopK { limit, .. } => Some(*limit),
        skein_plan::VectorPhysicalPlan::VectorCandidateScan { input, .. }
        | skein_plan::VectorPhysicalPlan::ResidualFilter { input, .. }
        | skein_plan::VectorPhysicalPlan::RawVectorRerank { input, .. } => vector_plan_top_k(input),
        skein_plan::VectorPhysicalPlan::Filter { .. } => None,
    }
}

fn graph_expansion_budget(top_k: usize, max_hops: usize) -> GraphExpansionBudget {
    let candidate_limit = top_k
        .max(1)
        .saturating_mul(max_hops.max(1))
        .saturating_mul(GRAPH_EXPANSION_FANOUT_PER_SEED_HOP)
        .min(GRAPH_EXPANSION_MAX_CANDIDATES)
        .max(top_k);
    GraphExpansionBudget {
        candidate_limit,
        payload_byte_limit: GRAPH_EXPANSION_PAYLOAD_BYTE_LIMIT,
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
        | LogicalPlan::VectorSeed { .. }
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
            let input = logical_to_physical_direct(input, catalog, decisions, stage_events);
            let graph_budget =
                vector_seed_top_k(&input).map(|top_k| graph_expansion_budget(top_k, *max_hops));
            if let Some(graph_budget) = graph_budget {
                decisions.push(format!(
                    "bound vector-seeded graph expansion to {} candidates and {} payload bytes",
                    graph_budget.candidate_limit, graph_budget.payload_byte_limit
                ));
            }
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
                graph_budget,
                input: Box::new(input),
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
        LogicalPlan::Filter { predicate, input } => {
            if let Some(plan) =
                index_seek_from_filter(predicate, input, catalog, decisions, stage_events)
            {
                plan
            } else if let Some(plan) = access::source_segment_scan_from_filter(predicate, input) {
                decisions.push(
                    "choose SourceSegmentScan for storage-prunable Source filter".to_string(),
                );
                PhysicalPlan::FilterExec {
                    predicate: predicate.clone(),
                    input: Box::new(plan),
                }
            } else {
                let mut input = logical_to_physical_direct(input, catalog, decisions, stage_events);
                push_vector_seed_metadata_filter(&mut input, predicate, decisions);
                PhysicalPlan::FilterExec {
                    predicate: predicate.clone(),
                    input: Box::new(input),
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
        } => {
            if let (Some(limit), LogicalPlan::Sort { items, input }) = (limit, input.as_ref()) {
                let input = logical_to_physical_direct(input, catalog, decisions, stage_events);
                select_bounded_sort_plan(items.clone(), *offset, *limit, input, catalog, decisions)
            } else {
                PhysicalPlan::LimitExec {
                    offset: *offset,
                    limit: *limit,
                    input: Box::new(logical_to_physical_direct(
                        input,
                        catalog,
                        decisions,
                        stage_events,
                    )),
                }
            }
        }
        _ => unreachable!("leaf logical plans are lowered before direct child planning"),
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
            let fingerprint = plan.instance_fingerprint();
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
