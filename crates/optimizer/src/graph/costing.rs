use super::cardinality::{
    estimate_aggregate_rows, estimate_aggregate_work_rows, estimate_filter_rows,
    estimate_optional_degree_work,
};
use super::{OptimizerCatalog, PhysicalPlan, PlanCost, PlanCostBreakdown};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use skein_plan::RelationshipCountLeg;
use std::collections::BTreeMap;

pub(super) const NODE_INDEX_EQ_STARTUP_COST: u64 = 1;
pub(super) const NODE_INDEX_RANGE_STARTUP_COST: u64 = 2;
pub(super) const NODE_INDEX_TEXT_STARTUP_COST: u64 = 3;

const NODE_FULL_SCAN_STARTUP_COST: u64 = 4;
const NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD: u64 = 8;

pub(super) fn estimate_node_cartesian_product_cost(
    left_cost: PlanCost,
    right_cost: PlanCost,
) -> PlanCost {
    let rows = left_cost
        .estimated_rows
        .saturating_mul(right_cost.estimated_rows)
        .max(1);
    PlanCost {
        estimated_rows: rows,
        cost: left_cost
            .cost
            .saturating_add(right_cost.cost)
            .saturating_add(rows),
    }
}

pub(super) fn estimate_node_full_scan_cost(label_count: u64) -> u64 {
    label_count.saturating_add(NODE_FULL_SCAN_STARTUP_COST)
}

pub(super) fn estimate_node_index_seek_cost(estimated_rows: u64, startup_cost: u64) -> u64 {
    estimated_rows
        .saturating_mul(2)
        .saturating_add(startup_cost)
}

pub(super) fn node_index_seek_is_cheaper(label_count: u64, seek_cost: u64) -> bool {
    label_count > NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD
        && seek_cost <= estimate_node_full_scan_cost(label_count)
}

pub(super) fn estimate_physical_plan_cost(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> PlanCost {
    match plan {
        PhysicalPlan::SeqNodeScan { label, .. } => {
            let rows = catalog.label_count(label);
            PlanCost {
                estimated_rows: rows,
                cost: estimate_node_full_scan_cost(rows),
            }
        }
        PhysicalPlan::VectorSeedScan { vector_plan, .. } => {
            let rows = vector_top_k(vector_plan) as u64;
            PlanCost {
                estimated_rows: rows,
                cost: rows.saturating_mul(10).max(1),
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left_cost = estimate_physical_plan_cost(left, catalog);
            let right_cost = estimate_physical_plan_cost(right, catalog);
            estimate_node_cartesian_product_cost(left_cost, right_cost)
        }
        PhysicalPlan::NodeColumnLookupExec { label, input, .. } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let label_rows = catalog.label_count(label).max(1);
            PlanCost {
                estimated_rows: input_cost.estimated_rows.max(1),
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows.saturating_mul(label_rows)),
            }
        }
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        } => {
            let rows = catalog
                .label_count(label)
                .div_ceil(catalog.distinct_count(label, property).max(1))
                .max(1);
            PlanCost {
                estimated_rows: rows,
                cost: estimate_node_index_seek_cost(rows, NODE_INDEX_EQ_STARTUP_COST),
            }
        }
        PhysicalPlan::IndexNodeMultiSeek {
            label,
            property,
            values,
            ..
        } => {
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let rows_per_value = catalog.label_count(label).div_ceil(distinct_count).max(1);
            let rows = rows_per_value
                .saturating_mul(values.len() as u64)
                .min(catalog.label_count(label))
                .max(1);
            PlanCost {
                estimated_rows: rows,
                cost: estimate_node_index_seek_cost(rows, values.len() as u64),
            }
        }
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => {
            let distinct_product = predicates
                .iter()
                .map(|(property, _)| catalog.distinct_count(label, property).max(1))
                .fold(1_u64, |acc, value| acc.saturating_mul(value))
                .max(1);
            let rows = catalog.label_count(label).div_ceil(distinct_product).max(1);
            PlanCost {
                estimated_rows: rows,
                cost: estimate_node_index_seek_cost(rows, predicates.len() as u64),
            }
        }
        PhysicalPlan::IndexNodeRangeSeek {
            label,
            property,
            lower,
            upper,
            ..
        } => {
            let rows =
                catalog.estimate_range_bounds_rows(label, property, lower.as_ref(), upper.as_ref());
            PlanCost {
                estimated_rows: rows,
                cost: estimate_node_index_seek_cost(rows, NODE_INDEX_RANGE_STARTUP_COST),
            }
        }
        PhysicalPlan::IndexNodeTextSeek { label, .. } => {
            let rows = catalog.label_count(label).div_ceil(4).max(1);
            PlanCost {
                estimated_rows: rows,
                cost: estimate_node_index_seek_cost(rows, NODE_INDEX_TEXT_STARTUP_COST),
            }
        }
        PhysicalPlan::AdjacencyExpandExec {
            source_label,
            rel_type,
            rel_properties,
            target_label,
            min_hops,
            max_hops,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let expand_estimate = catalog.estimate_expand_rows(
                source_label,
                rel_type,
                rel_properties,
                target_label,
                *min_hops,
                *max_hops,
            );
            let source_rows = catalog.label_count(source_label).max(1);
            let scaled_rows = expand_estimate
                .estimated_rows
                .saturating_mul(input_cost.estimated_rows.max(1))
                .div_ceil(source_rows)
                .max(1);
            PlanCost {
                estimated_rows: scaled_rows,
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows)
                    .saturating_add(scaled_rows),
            }
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let rows = estimate_filter_rows(predicate, input, input_cost.estimated_rows, catalog);
            PlanCost {
                estimated_rows: rows,
                cost: input_cost.cost.saturating_add(input_cost.estimated_rows),
            }
        }
        PhysicalPlan::ProjectExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost.cost.saturating_add(input_cost.estimated_rows),
            }
        }
        PhysicalPlan::OptionalDegreeExec {
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let degree_work = estimate_optional_degree_work(
                rel_type,
                rel_properties,
                *direction,
                target_label,
                target_properties,
                catalog,
            );
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows.saturating_mul(degree_work)),
            }
        }
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            ..
        } => estimate_optional_relationship_count_sum(label, properties, legs, catalog).cost,
        PhysicalPlan::ThreadRepairStatsExec { .. } => PlanCost {
            estimated_rows: 1,
            cost: 32,
        },
        PhysicalPlan::ShortestPathExec { max_hops, .. } => PlanCost {
            estimated_rows: 1,
            cost: (*max_hops as u64).saturating_mul(8).saturating_add(4),
        },
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let rows =
                estimate_aggregate_rows(group_keys, input, input_cost.estimated_rows, catalog);
            let work_rows =
                estimate_aggregate_work_rows(items, input, input_cost.estimated_rows, catalog);
            PlanCost {
                estimated_rows: rows,
                cost: input_cost.cost.saturating_add(work_rows),
            }
        }
        PhysicalPlan::DistinctExec { input } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost.cost.saturating_add(input_cost.estimated_rows),
            }
        }
        PhysicalPlan::SortExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows.saturating_mul(2)),
            }
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let remaining_rows = input_cost.estimated_rows.saturating_sub(*offset as u64);
            let rows = limit
                .map(|limit| remaining_rows.min(limit as u64))
                .unwrap_or(remaining_rows)
                .max(1);
            PlanCost {
                estimated_rows: rows,
                cost: input_cost.cost.saturating_add(rows),
            }
        }
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. } => PlanCost {
            estimated_rows: 1,
            cost: 1,
        },
    }
}

pub(super) fn estimate_physical_plan_cost_breakdown(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> PlanCostBreakdown {
    match plan {
        PhysicalPlan::SeqNodeScan { label, .. } => {
            let rows = catalog.label_count(label);
            PlanCostBreakdown::new(rows, 0, 0, estimate_node_full_scan_cost(rows), 0)
        }
        PhysicalPlan::VectorSeedScan { vector_plan, .. } => {
            let rows = vector_top_k(vector_plan) as u64;
            PlanCostBreakdown::new(rows, rows.saturating_mul(10).max(1), 0, 0, rows)
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left_cost = estimate_physical_plan_cost_breakdown(left, catalog);
            let right_cost = estimate_physical_plan_cost_breakdown(right, catalog);
            let rows = left_cost
                .estimated_rows
                .saturating_mul(right_cost.estimated_rows)
                .max(1);
            PlanCostBreakdown::combine_with_cpu(left_cost, right_cost, rows, rows, 0)
        }
        PhysicalPlan::NodeColumnLookupExec { label, input, .. } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let label_rows = catalog.label_count(label).max(1);
            input_cost.with_random_io(
                input_cost.estimated_rows.max(1),
                input_cost.estimated_rows.saturating_mul(label_rows),
                0,
            )
        }
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        } => {
            let rows = catalog
                .label_count(label)
                .div_ceil(catalog.distinct_count(label, property).max(1))
                .max(1);
            PlanCostBreakdown::new(
                rows,
                0,
                estimate_node_index_seek_cost(rows, NODE_INDEX_EQ_STARTUP_COST),
                0,
                0,
            )
        }
        PhysicalPlan::IndexNodeMultiSeek {
            label,
            property,
            values,
            ..
        } => {
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let rows_per_value = catalog.label_count(label).div_ceil(distinct_count).max(1);
            let rows = rows_per_value
                .saturating_mul(values.len() as u64)
                .min(catalog.label_count(label))
                .max(1);
            PlanCostBreakdown::new(
                rows,
                0,
                estimate_node_index_seek_cost(rows, values.len() as u64),
                0,
                0,
            )
        }
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => {
            let distinct_product = predicates
                .iter()
                .map(|(property, _)| catalog.distinct_count(label, property).max(1))
                .fold(1_u64, |acc, value| acc.saturating_mul(value))
                .max(1);
            let rows = catalog.label_count(label).div_ceil(distinct_product).max(1);
            PlanCostBreakdown::new(
                rows,
                0,
                estimate_node_index_seek_cost(rows, predicates.len() as u64),
                0,
                0,
            )
        }
        PhysicalPlan::IndexNodeRangeSeek {
            label,
            property,
            lower,
            upper,
            ..
        } => {
            let rows =
                catalog.estimate_range_bounds_rows(label, property, lower.as_ref(), upper.as_ref());
            PlanCostBreakdown::new(
                rows,
                0,
                estimate_node_index_seek_cost(rows, NODE_INDEX_RANGE_STARTUP_COST),
                0,
                0,
            )
        }
        PhysicalPlan::IndexNodeTextSeek { label, .. } => {
            let rows = catalog.label_count(label).div_ceil(4).max(1);
            PlanCostBreakdown::new(
                rows,
                0,
                estimate_node_index_seek_cost(rows, NODE_INDEX_TEXT_STARTUP_COST),
                0,
                0,
            )
        }
        PhysicalPlan::AdjacencyExpandExec {
            source_label,
            rel_type,
            rel_properties,
            target_label,
            min_hops,
            max_hops,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let expand_estimate = catalog.estimate_expand_rows(
                source_label,
                rel_type,
                rel_properties,
                target_label,
                *min_hops,
                *max_hops,
            );
            let source_rows = catalog.label_count(source_label).max(1);
            let scaled_rows = expand_estimate
                .estimated_rows
                .saturating_mul(input_cost.estimated_rows.max(1))
                .div_ceil(source_rows)
                .max(1);
            input_cost.with_random_io(
                scaled_rows,
                input_cost.estimated_rows.saturating_add(scaled_rows),
                0,
            )
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let rows = estimate_filter_rows(predicate, input, input_cost.estimated_rows, catalog);
            input_cost.with_cpu(rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::ProjectExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            input_cost.with_cpu(input_cost.estimated_rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::OptionalDegreeExec {
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let degree_work = estimate_optional_degree_work(
                rel_type,
                rel_properties,
                *direction,
                target_label,
                target_properties,
                catalog,
            );
            input_cost.with_random_io(
                input_cost.estimated_rows,
                input_cost.estimated_rows.saturating_mul(degree_work),
                0,
            )
        }
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            ..
        } => {
            let scalar =
                estimate_optional_relationship_count_sum(label, properties, legs, catalog).cost;
            PlanCostBreakdown::from_scalar(scalar)
        }
        PhysicalPlan::ThreadRepairStatsExec { .. } => PlanCostBreakdown::new(1, 32, 0, 0, 0),
        PhysicalPlan::ShortestPathExec { max_hops, .. } => PlanCostBreakdown::new(
            1,
            0,
            (*max_hops as u64).saturating_mul(8).saturating_add(4),
            0,
            0,
        ),
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let rows =
                estimate_aggregate_rows(group_keys, input, input_cost.estimated_rows, catalog);
            let work_rows =
                estimate_aggregate_work_rows(items, input, input_cost.estimated_rows, catalog);
            input_cost.with_cpu(rows, work_rows, 0)
        }
        PhysicalPlan::DistinctExec { input } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            input_cost.with_cpu(input_cost.estimated_rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::SortExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            input_cost.with_cpu(
                input_cost.estimated_rows,
                input_cost.estimated_rows.saturating_mul(2),
                0,
            )
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let remaining_rows = input_cost.estimated_rows.saturating_sub(*offset as u64);
            let rows = limit
                .map(|limit| remaining_rows.min(limit as u64))
                .unwrap_or(remaining_rows)
                .max(1);
            input_cost.with_cpu(rows, rows, 0)
        }
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. } => PlanCostBreakdown::new(1, 1, 0, 0, 0),
    }
}

fn vector_top_k(plan: &skein_plan::VectorPhysicalPlan) -> usize {
    match plan {
        skein_plan::VectorPhysicalPlan::TopK { limit, .. } => *limit,
        _ => 1,
    }
}

pub(super) fn push_optional_relationship_count_sum_cost_decision(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
) {
    let estimate = estimate_optional_relationship_count_sum(label, properties, legs, catalog);
    let leg_rows = estimate
        .leg_rows
        .iter()
        .map(|leg| {
            format!(
                "{}:{}:{}",
                leg.rel_type,
                format_relationship_direction(leg.direction),
                leg.rows
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    decisions.push(format!(
        "estimate OptionalRelationshipCountSum for {label}: seed_rows={} leg_rows=[{}] estimated_rows={} cost={}",
        estimate.seed_rows, leg_rows, estimate.cost.estimated_rows, estimate.cost.cost
    ));
}

struct OptionalRelationshipCountSumEstimate {
    seed_rows: u64,
    leg_rows: Vec<OptionalRelationshipCountLegEstimate>,
    cost: PlanCost,
}

struct OptionalRelationshipCountLegEstimate {
    rel_type: String,
    direction: RelationshipDirection,
    rows: u64,
}

fn estimate_optional_relationship_count_sum(
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
    catalog: &OptimizerCatalog,
) -> OptionalRelationshipCountSumEstimate {
    let seed_rows = estimate_seed_rows_from_properties(label, properties, catalog);
    let leg_rows = legs
        .iter()
        .map(|leg| OptionalRelationshipCountLegEstimate {
            rel_type: leg.rel_type.clone(),
            direction: leg.direction,
            rows: estimate_relationship_count_leg_rows(seed_rows, leg, catalog),
        })
        .collect::<Vec<_>>();
    let relationship_rows = leg_rows
        .iter()
        .map(|leg| leg.rows)
        .fold(0_u64, |acc, rows| acc.saturating_add(rows));
    let cost = PlanCost {
        estimated_rows: 1,
        cost: seed_rows
            .saturating_add(relationship_rows)
            .saturating_add(legs.len() as u64)
            .saturating_add(4),
    };
    OptionalRelationshipCountSumEstimate {
        seed_rows,
        leg_rows,
        cost,
    }
}

fn format_relationship_direction(direction: RelationshipDirection) -> &'static str {
    match direction {
        RelationshipDirection::Outgoing => "out",
        RelationshipDirection::Incoming => "in",
        RelationshipDirection::Undirected => "both",
    }
}

fn estimate_seed_rows_from_properties(
    label: &str,
    properties: &BTreeMap<String, Value>,
    catalog: &OptimizerCatalog,
) -> u64 {
    let label_rows = catalog.label_count(label).max(1);
    let distinct_product = properties
        .keys()
        .map(|property| catalog.distinct_count(label, property).max(1))
        .fold(1_u64, |acc, value| acc.saturating_mul(value))
        .max(1);
    label_rows.div_ceil(distinct_product).max(1)
}

fn estimate_relationship_count_leg_rows(
    seed_rows: u64,
    leg: &RelationshipCountLeg,
    catalog: &OptimizerCatalog,
) -> u64 {
    let rel_count = catalog
        .rel_type_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let source_count = catalog
        .rel_type_source_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let target_count = catalog
        .rel_type_target_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let per_seed = match leg.direction {
        RelationshipDirection::Outgoing => rel_count.div_ceil(source_count).max(1),
        RelationshipDirection::Incoming => rel_count.div_ceil(target_count).max(1),
        RelationshipDirection::Undirected => rel_count
            .div_ceil(source_count)
            .saturating_add(rel_count.div_ceil(target_count))
            .max(1),
    };
    seed_rows.saturating_mul(per_seed).max(1)
}
