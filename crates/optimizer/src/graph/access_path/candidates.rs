use super::*;

pub(super) fn index_seek_from_conjunction(
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

pub(super) fn equality_index_seek_candidate(
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

pub(super) fn composite_index_seek_candidate(
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
