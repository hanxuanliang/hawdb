use super::stages::LOGICAL_REWRITE_STAGE;
use crate::{RuleEvent, StageStats, StageTrace};
use skein_core::Value;
use skein_plan::{LogicalPlan, Predicate};

const MAX_FIXED_POINT_PASSES: usize = 16;

pub(super) struct LogicalRewriteOutput {
    plan: LogicalPlan,
    events: Vec<RuleEvent>,
    trace: StageTrace,
}

impl LogicalRewriteOutput {
    pub(super) fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub(super) fn events(&self) -> &[RuleEvent] {
        &self.events
    }

    pub(super) fn trace(&self) -> &StageTrace {
        &self.trace
    }
}

pub(super) fn rewrite_logical_plan(plan: &LogicalPlan) -> LogicalRewriteOutput {
    let original = plan.clone();
    let input_count = logical_node_count(plan);
    let mut current = original.clone();
    let mut events = Vec::new();

    for pass in 1..=MAX_FIXED_POINT_PASSES {
        let mut pass_events = Vec::new();
        let next = rewrite_bottom_up(current.clone(), &mut pass_events);
        if next == current {
            let applied_rules = events.len();
            return LogicalRewriteOutput {
                trace: LOGICAL_REWRITE_STAGE.trace(
                    StageStats::new(input_count, logical_node_count(&current))
                        .with_rule_counts(applied_rules, 0),
                ),
                plan: current,
                events,
            };
        }
        events.extend(pass_events);
        current = next;

        if pass == MAX_FIXED_POINT_PASSES {
            return LogicalRewriteOutput {
                trace: LOGICAL_REWRITE_STAGE
                    .trace(StageStats::new(input_count, input_count).with_rule_counts(0, 1)),
                plan: original,
                events: vec![RuleEvent::skipped(
                    "transformation:logical_rewrite_fixed_point",
                    "fixed-point pass limit reached; retained the original logical plan",
                )],
            };
        }
    }

    unreachable!("fixed-point loop always returns")
}

fn rewrite_bottom_up(plan: LogicalPlan, events: &mut Vec<RuleEvent>) -> LogicalPlan {
    let plan = match plan {
        LogicalPlan::NodeCartesianProduct { left, right } => LogicalPlan::NodeCartesianProduct {
            left: Box::new(rewrite_bottom_up(*left, events)),
            right: Box::new(rewrite_bottom_up(*right, events)),
        },
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input: Box::new(rewrite_bottom_up(*input, events)),
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
        } => LogicalPlan::Expand {
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
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Project { items, input } => LogicalPlan::Project {
            items,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Sort { items, input } => LogicalPlan::Sort {
            items,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        leaf => leaf,
    };

    rewrite_local(plan, events)
}

fn rewrite_local(plan: LogicalPlan, events: &mut Vec<RuleEvent>) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => rewrite_filter(predicate, *input, events),
        LogicalPlan::Distinct { input } => match *input {
            LogicalPlan::Distinct { input } => {
                record(events, "remove_redundant_distinct");
                LogicalPlan::Distinct { input }
            }
            input => LogicalPlan::Distinct {
                input: Box::new(input),
            },
        },
        LogicalPlan::Sort { items, input } => match *input {
            LogicalPlan::Sort {
                items: inner_items,
                input,
            } if items == inner_items => {
                record(events, "remove_redundant_sort");
                LogicalPlan::Sort { items, input }
            }
            input => LogicalPlan::Sort {
                items,
                input: Box::new(input),
            },
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => rewrite_limit(offset, limit, *input, events),
        plan => plan,
    }
}

fn rewrite_filter(
    predicate: Predicate,
    input: LogicalPlan,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    let predicate = simplify_predicate(predicate);
    match predicate {
        Predicate::ConstantBool(true) => {
            record(events, "remove_true_filter");
            input
        }
        Predicate::ConstantBool(false) => {
            record(events, "replace_false_filter_with_empty_limit");
            canonical_empty_limit(input)
        }
        predicate if matches!(input, LogicalPlan::Expand { .. }) => {
            rewrite_filter_into_expand(predicate, input, events)
        }
        outer => match input {
            LogicalPlan::Filter {
                predicate: inner,
                input,
            } => {
                record(events, "fuse_adjacent_filters");
                rewrite_filter(Predicate::And(vec![inner, outer]), *input, events)
            }
            input => LogicalPlan::Filter {
                predicate: outer,
                input: Box::new(input),
            },
        },
    }
}

fn rewrite_filter_into_expand(
    predicate: Predicate,
    input: LogicalPlan,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    let LogicalPlan::Expand {
        source_variable,
        source_label,
        rel_variable,
        rel_type,
        mut rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        input,
    } = input
    else {
        unreachable!("filter-into-expand requires an expand input")
    };
    let predicates = match predicate {
        Predicate::And(predicates) => predicates,
        predicate => vec![predicate],
    };
    let mut source_predicates = Vec::new();
    let mut residual_predicates = Vec::new();
    let mut embedded_relationship_predicate = false;

    for predicate in predicates {
        if !optional
            && min_hops == 1
            && max_hops == 1
            && let Some(rel_variable) = &rel_variable
            && let Predicate::PropertyEq {
                variable,
                property,
                value,
            } = &predicate
            && variable == rel_variable
        {
            if rel_properties
                .get(property)
                .is_some_and(|existing| existing != value)
            {
                record(events, "detect_conflicting_relationship_filter");
                return canonical_empty_limit(*input);
            }
            rel_properties
                .entry(property.clone())
                .or_insert_with(|| value.clone());
            embedded_relationship_predicate = true;
            continue;
        }
        if predicate_references_only_variable(&predicate, &source_variable) {
            source_predicates.push(predicate);
        } else {
            residual_predicates.push(predicate);
        }
    }

    if source_predicates.is_empty() && !embedded_relationship_predicate {
        return LogicalPlan::Filter {
            predicate: predicates_from_terms(residual_predicates),
            input: Box::new(LogicalPlan::Expand {
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
            }),
        };
    }

    let input = if source_predicates.is_empty() {
        *input
    } else {
        record(events, "push_source_filter_below_expand");
        rewrite_filter(predicates_from_terms(source_predicates), *input, events)
    };
    if embedded_relationship_predicate {
        record(events, "embed_relationship_filter_into_expand");
    }
    let expand = LogicalPlan::Expand {
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
        input: Box::new(input),
    };
    if residual_predicates.is_empty() {
        expand
    } else {
        LogicalPlan::Filter {
            predicate: predicates_from_terms(residual_predicates),
            input: Box::new(expand),
        }
    }
}

fn predicates_from_terms(predicates: Vec<Predicate>) -> Predicate {
    simplify_predicate(Predicate::And(predicates))
}

fn predicate_references_only_variable(predicate: &Predicate, variable: &str) -> bool {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .all(|predicate| predicate_references_only_variable(predicate, variable)),
        Predicate::Not(predicate) => predicate_references_only_variable(predicate, variable),
        Predicate::IdEq {
            variable: current, ..
        }
        | Predicate::IdNotEq {
            variable: current, ..
        }
        | Predicate::IdCompare {
            variable: current, ..
        }
        | Predicate::IdIn {
            variable: current, ..
        }
        | Predicate::PropertyEq {
            variable: current, ..
        }
        | Predicate::PropertyNotEq {
            variable: current, ..
        }
        | Predicate::PropertyCompare {
            variable: current, ..
        }
        | Predicate::PropertyListContains {
            variable: current, ..
        }
        | Predicate::PropertyListContainsLower {
            variable: current, ..
        }
        | Predicate::PropertyContains {
            variable: current, ..
        }
        | Predicate::PropertyStartsWith {
            variable: current, ..
        }
        | Predicate::PropertyEndsWith {
            variable: current, ..
        }
        | Predicate::PropertyRegexMatch {
            variable: current, ..
        }
        | Predicate::PropertyIsNull {
            variable: current, ..
        }
        | Predicate::PropertyIsNotNull {
            variable: current, ..
        }
        | Predicate::PropertyIn {
            variable: current, ..
        } => current == variable,
        Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. }
        | Predicate::ExpressionEq { .. }
        | Predicate::ExpressionNotEq { .. }
        | Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. } => false,
    }
}

fn rewrite_limit(
    offset: usize,
    limit: Option<usize>,
    input: LogicalPlan,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    if limit == Some(0) {
        if offset != 0 {
            record(events, "canonicalize_empty_limit");
        }
        return canonical_empty_limit(input);
    }
    if offset == 0 && limit.is_none() {
        record(events, "remove_unbounded_limit");
        return input;
    }

    let LogicalPlan::Limit {
        offset: inner_offset,
        limit: inner_limit,
        input,
    } = input
    else {
        return LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(input),
        };
    };

    record(events, "compose_adjacent_limits");
    if inner_limit.is_some_and(|inner_limit| offset >= inner_limit) {
        return canonical_empty_limit(*input);
    }

    let remaining_inner = inner_limit.map(|inner_limit| inner_limit - offset);
    let combined_limit = match (limit, remaining_inner) {
        (Some(outer), Some(inner)) => Some(outer.min(inner)),
        (Some(outer), None) => Some(outer),
        (None, Some(inner)) => Some(inner),
        (None, None) => None,
    };
    if combined_limit == Some(0) {
        return canonical_empty_limit(*input);
    }

    LogicalPlan::Limit {
        offset: inner_offset.saturating_add(offset),
        limit: combined_limit,
        input,
    }
}

fn canonical_empty_limit(input: LogicalPlan) -> LogicalPlan {
    let input = match input {
        LogicalPlan::Limit { input, .. } => input,
        input => Box::new(input),
    };
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(0),
        input,
    }
}

fn simplify_predicate(predicate: Predicate) -> Predicate {
    match predicate {
        Predicate::And(predicates) => simplify_conjunction(predicates),
        Predicate::Or(predicates) => simplify_disjunction(predicates),
        Predicate::Not(predicate) => match simplify_predicate(*predicate) {
            Predicate::ConstantBool(value) => Predicate::ConstantBool(!value),
            Predicate::Not(predicate) => *predicate,
            predicate => Predicate::Not(Box::new(predicate)),
        },
        Predicate::IdIn { variable, values } => {
            let values = deduplicate_values(values);
            match values.as_slice() {
                [] => Predicate::ConstantBool(false),
                [value] => Predicate::IdEq {
                    variable,
                    value: value.clone(),
                },
                _ => Predicate::IdIn { variable, values },
            }
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => {
            let values = deduplicate_values(values);
            match values.as_slice() {
                [] => Predicate::ConstantBool(false),
                [value] => Predicate::PropertyEq {
                    variable,
                    property,
                    value: value.clone(),
                },
                _ => Predicate::PropertyIn {
                    variable,
                    property,
                    values,
                },
            }
        }
        predicate => predicate,
    }
}

fn simplify_conjunction(predicates: Vec<Predicate>) -> Predicate {
    let mut simplified = Vec::new();
    for predicate in predicates {
        match simplify_predicate(predicate) {
            Predicate::ConstantBool(false) => return Predicate::ConstantBool(false),
            Predicate::ConstantBool(true) => {}
            Predicate::And(nested) => append_unique(&mut simplified, nested),
            predicate => append_unique(&mut simplified, [predicate]),
        }
    }
    match simplified.len() {
        0 => Predicate::ConstantBool(true),
        1 => simplified.pop().expect("single predicate should exist"),
        _ => Predicate::And(simplified),
    }
}

fn simplify_disjunction(predicates: Vec<Predicate>) -> Predicate {
    let mut simplified = Vec::new();
    for predicate in predicates {
        match simplify_predicate(predicate) {
            Predicate::ConstantBool(true) => return Predicate::ConstantBool(true),
            Predicate::ConstantBool(false) => {}
            Predicate::Or(nested) => append_unique(&mut simplified, nested),
            predicate => append_unique(&mut simplified, [predicate]),
        }
    }
    match simplified.len() {
        0 => Predicate::ConstantBool(false),
        1 => simplified.pop().expect("single predicate should exist"),
        _ => Predicate::Or(simplified),
    }
}

fn append_unique(output: &mut Vec<Predicate>, predicates: impl IntoIterator<Item = Predicate>) {
    for predicate in predicates {
        if !output.contains(&predicate) {
            output.push(predicate);
        }
    }
}

fn deduplicate_values(values: Vec<Value>) -> Vec<Value> {
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        if !output.contains(&value) {
            output.push(value);
        }
    }
    output
}

fn record(events: &mut Vec<RuleEvent>, rule: &'static str) {
    events.push(RuleEvent::applied(
        format!("transformation:{rule}"),
        "logical expression was simplified",
    ));
}

fn logical_node_count(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::NodeCartesianProduct { left, right } => {
            1 + logical_node_count(left) + logical_node_count(right)
        }
        LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::Expand { input, .. }
        | LogicalPlan::OptionalDegree { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => 1 + logical_node_count(input),
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_cypher::RelationshipDirection;
    use skein_plan::{SortDirection, SortItem, SortKey};

    fn scan() -> LogicalPlan {
        LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }
    }

    fn property_eq(value: i64) -> Predicate {
        Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::Int(value),
        }
    }

    fn expand(optional: bool) -> LogicalPlan {
        LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: Default::default(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional,
            input: Box::new(scan()),
        }
    }

    #[test]
    fn rewrite_is_idempotent_and_fuses_filters() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::And(vec![Predicate::ConstantBool(true), property_eq(1)]),
            input: Box::new(LogicalPlan::Filter {
                predicate: property_eq(2),
                input: Box::new(scan()),
            }),
        };

        let first = rewrite_logical_plan(&plan);
        let second = rewrite_logical_plan(first.plan());

        assert_eq!(first.plan(), second.plan());
        assert!(matches!(
            first.plan(),
            LogicalPlan::Filter {
                predicate: Predicate::And(predicates),
                input,
            } if predicates == &vec![property_eq(2), property_eq(1)]
                && matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
        ));
        assert!(first
            .events()
            .iter()
            .any(|event| { event.rule() == "transformation:fuse_adjacent_filters" }));
    }

    #[test]
    fn false_filter_becomes_canonical_empty_limit() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::PropertyIn {
                variable: "m".to_string(),
                property: "kind".to_string(),
                values: Vec::new(),
            },
            input: Box::new(scan()),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Limit {
                offset: 0,
                limit: Some(0),
                input,
            } if matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
        ));
    }

    #[test]
    fn adjacent_limits_compose_without_changing_offset_semantics() {
        let plan = LogicalPlan::Limit {
            offset: 3,
            limit: Some(10),
            input: Box::new(LogicalPlan::Limit {
                offset: 5,
                limit: Some(20),
                input: Box::new(scan()),
            }),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Limit {
                offset: 8,
                limit: Some(10),
                input,
            } if matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
        ));
    }

    #[test]
    fn redundant_order_and_distinct_operators_are_removed() {
        let items = vec![SortItem {
            key: SortKey::Id {
                variable: "m".to_string(),
            },
            direction: SortDirection::Asc,
        }];
        let plan = LogicalPlan::Distinct {
            input: Box::new(LogicalPlan::Distinct {
                input: Box::new(LogicalPlan::Sort {
                    items: items.clone(),
                    input: Box::new(LogicalPlan::Sort {
                        items: items.clone(),
                        input: Box::new(scan()),
                    }),
                }),
            }),
        };

        assert_eq!(
            rewrite_logical_plan(&plan).plan(),
            &LogicalPlan::Distinct {
                input: Box::new(LogicalPlan::Sort {
                    items,
                    input: Box::new(scan()),
                }),
            }
        );
    }

    #[test]
    fn source_filter_moves_before_expand_and_leaves_target_filter_pushable() {
        let source = property_eq(1);
        let target = Predicate::PropertyEq {
            variable: "e".to_string(),
            property: "kind".to_string(),
            value: Value::String("person".to_string()),
        };
        let plan = LogicalPlan::Filter {
            predicate: Predicate::And(vec![source.clone(), target.clone()]),
            input: Box::new(expand(false)),
        };

        let output = rewrite_logical_plan(&plan);
        assert!(matches!(
            output.plan(),
            LogicalPlan::Filter {
                predicate,
                input,
            } if predicate == &target
                && matches!(
                    input.as_ref(),
                    LogicalPlan::Expand { input, .. }
                        if matches!(
                            input.as_ref(),
                            LogicalPlan::Filter { predicate, input }
                                if predicate == &source
                                    && matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
                        )
                )
        ));
        assert!(output
            .events()
            .iter()
            .any(|event| { event.rule() == "transformation:push_source_filter_below_expand" }));
    }

    #[test]
    fn exact_relationship_filter_is_embedded_for_required_one_hop_expand() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "r".to_string(),
                property: "role".to_string(),
                value: Value::String("subject".to_string()),
            },
            input: Box::new(expand(false)),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Expand { rel_properties, .. }
                if rel_properties.get("role") == Some(&Value::String("subject".to_string()))
        ));
    }

    #[test]
    fn optional_expand_keeps_post_expand_relationship_filter() {
        let predicate = Predicate::PropertyEq {
            variable: "r".to_string(),
            property: "role".to_string(),
            value: Value::String("subject".to_string()),
        };
        let plan = LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(expand(true)),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Filter { predicate: actual, input }
                if actual == &predicate
                    && matches!(
                        input.as_ref(),
                        LogicalPlan::Expand { rel_properties, optional: true, .. }
                            if rel_properties.is_empty()
                    )
        ));
    }
}
