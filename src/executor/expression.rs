//! Projection, aggregation, predicate, and filter evaluation helpers.

use super::*;

pub(super) fn execute_aggregate(
    catalog: &Catalog,
    group_keys: &[crate::planner::Projection],
    items: &[Aggregation],
    input: &[Binding],
) -> Vec<Binding> {
    if group_keys.is_empty() {
        let mut values = BTreeMap::new();
        for item in items {
            insert_projected_value(
                &mut values,
                &item.name,
                aggregate_value(catalog, item, input),
            );
        }
        return vec![Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }];
    }

    let mut groups = BTreeMap::<Vec<Value>, Vec<&Binding>>::new();
    for binding in input {
        let key = group_keys
            .iter()
            .map(|item| group_key_value(item, catalog, binding))
            .collect::<Vec<_>>();
        groups.entry(key).or_default().push(binding);
    }

    groups
        .into_iter()
        .map(|(key, bindings)| {
            let mut values = BTreeMap::new();
            for (item, value) in group_keys.iter().zip(key) {
                insert_projected_value(&mut values, &item.name, value);
            }
            let group = bindings.into_iter().cloned().collect::<Vec<_>>();
            for item in items {
                insert_projected_value(
                    &mut values,
                    &item.name,
                    aggregate_value(catalog, item, &group),
                );
            }
            Binding {
                values,
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }
        })
        .collect()
}

pub(super) fn insert_projected_value(
    values: &mut BTreeMap<String, Value>,
    name: &str,
    value: Value,
) {
    let mut candidate = name.to_string();
    let mut suffix = 2;
    loop {
        match values.entry(candidate) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
                return;
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                candidate = format!("{name}#{suffix}");
                suffix += 1;
            }
        }
    }
}

pub(super) fn distinct_bindings(input: Vec<Binding>) -> Vec<Binding> {
    let mut seen = std::collections::BTreeSet::new();
    let mut output = Vec::new();
    for binding in input {
        let key = binding
            .values
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        if seen.insert(key) {
            output.push(binding);
        }
    }
    output
}

pub(super) fn project_value(
    item: &Projection,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    evaluate_projection_expression(&item.expression, catalog, binding)
}

pub(super) fn evaluate_projection_expression(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    match expression {
        ProjectionExpression::Variable { variable } => binding_value(binding, catalog, variable)
            .ok_or_else(|| {
                SkeinError::Execution(format!("missing variable '{variable}' during projection"))
            }),
        ProjectionExpression::Property { variable, property } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null))
        }
        ProjectionExpression::Id { variable } => binding_id(binding, variable).ok_or_else(|| {
            SkeinError::Execution(format!("missing variable '{variable}' during projection"))
        }),
        ProjectionExpression::RelationshipType { variable } => {
            let relationship = binding.relationships.get(variable).ok_or_else(|| {
                SkeinError::Execution(format!("missing variable '{variable}' during projection"))
            })?;
            Ok(catalog
                .rel_type_name(relationship.rel_type)
                .map(|rel_type| Value::String(rel_type.to_string()))
                .unwrap_or(Value::Null))
        }
        ProjectionExpression::Literal(value) => Ok(value.clone()),
        ProjectionExpression::Coalesce(expressions) => {
            for expression in expressions {
                let value = project_expression_value(expression, catalog, binding)?;
                if value != Value::Null {
                    return Ok(value);
                }
            }
            Ok(Value::Null)
        }
        ProjectionExpression::Left { expression, length } => {
            match project_expression_value(expression, catalog, binding)? {
                Value::Null => Ok(Value::Null),
                Value::String(value) => Ok(Value::String(value.chars().take(*length).collect())),
                value => Err(SkeinError::Execution(format!(
                    "LEFT expression requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::Lower(expression) => {
            match project_expression_value(expression, catalog, binding)? {
                Value::Null => Ok(Value::Null),
                Value::String(value) => Ok(Value::String(value.to_lowercase())),
                value => Err(SkeinError::Execution(format!(
                    "LOWER expression requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::DatePart {
            part,
            variable,
            property,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::Int(nanos)) => Ok(Value::Int(timestamp_date_part(*part, *nanos))),
                Some(Value::Null) | None => Ok(Value::Null),
                Some(value) => Err(SkeinError::Execution(format!(
                    "date_part requires an integer timestamp value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value == Value::Null || value == *empty {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value == Value::Null {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value != Value::Null && value != *empty {
                Ok(non_empty.clone())
            } else {
                Ok(null_or_empty.clone())
            }
        }
        ProjectionExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            for (candidate, rank) in branches {
                if value == *candidate {
                    return Ok(rank.clone());
                }
            }
            Ok(default.clone())
        }
        ProjectionExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::String(value)) => Ok(Value::String(value.to_lowercase())),
                Some(Value::Null) | None => Ok(default.clone()),
                Some(value) => Err(SkeinError::Execution(format!(
                    "CASE lower-default requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(Value::Int(
                coalesce_difference(binding, variable, terms)?.max(0),
            ))
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            if !binding_has_variable(binding, &expression.variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{}' during projection",
                    expression.variable
                )));
            }
            let name_matches = match binding_property(
                binding,
                &expression.variable,
                &expression.name_property,
            ) {
                Some(Value::String(name)) => {
                    let lowered = name.to_lowercase();
                    matches!(&expression.raw_query, Value::String(query) if lowered == *query)
                        || matches!(&expression.normalized_query, Value::String(query) if lowered == *query)
                }
                _ => false,
            };
            if name_matches {
                return Ok(expression.exact_rank.clone());
            }
            let alias_matches =
                match binding_property(binding, &expression.variable, &expression.aliases_property)
                {
                    Some(Value::List(values)) => {
                        values.iter().any(|alias| alias == &expression.raw_input)
                    }
                    _ => false,
                };
            if alias_matches {
                Ok(expression.alias_rank.clone())
            } else {
                Ok(expression.fallback_rank.clone())
            }
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            let column = binding.values.get(&expression.column).ok_or_else(|| {
                SkeinError::Execution(format!(
                    "missing column '{}' during projection",
                    expression.column
                ))
            })?;
            let Value::String(value) = column else {
                return Ok(expression.fallback_rank.clone());
            };
            if matches!(&expression.raw_query, Value::String(query) if value == query)
                || matches!(&expression.normalized_query, Value::String(query) if value == query)
            {
                return Ok(expression.exact_rank.clone());
            }
            if matches!(&expression.raw_query, Value::String(query) if value.contains(query))
                || matches!(&expression.normalized_query, Value::String(query) if value.contains(query))
            {
                Ok(expression.contains_rank.clone())
            } else {
                Ok(expression.fallback_rank.clone())
            }
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column,
            property,
            empty,
            default,
        } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            let value = match value {
                Value::Map(values) => values.get(property).cloned().unwrap_or(Value::Null),
                Value::Null => Value::Null,
                value => {
                    return Err(SkeinError::Execution(format!(
                        "column default expression requires a map value, got {value:?}"
                    )));
                }
            };
            if value == Value::Null || value == *empty {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::ColumnValueDefaultIfNull { column, default } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            if value == &Value::Null {
                Ok(default.clone())
            } else {
                Ok(value.clone())
            }
        }
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column,
            empty,
            non_empty,
            null_or_empty,
        } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            if value == &Value::Null || value == empty {
                Ok(null_or_empty.clone())
            } else {
                Ok(non_empty.clone())
            }
        }
        ProjectionExpression::Column(name) => binding.values.get(name).cloned().ok_or_else(|| {
            SkeinError::Execution(format!("missing column '{name}' during projection"))
        }),
        ProjectionExpression::ColumnProperty { column, property } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            match value {
                Value::Map(values) => Ok(values.get(property).cloned().unwrap_or(Value::Null)),
                Value::Null => Ok(Value::Null),
                value => Err(SkeinError::Execution(format!(
                    "column property projection requires a map value, got {value:?}"
                ))),
            }
        }
    }
}

fn project_expression_value(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    evaluate_projection_expression(expression, catalog, binding)
}

fn coalesce_difference(
    binding: &Binding,
    variable: &str,
    terms: &[CoalesceDifferenceProjectionTerm],
) -> Result<i64> {
    let Some((first, rest)) = terms.split_first() else {
        return Err(SkeinError::Execution(
            "coalesce difference requires at least one term".to_string(),
        ));
    };
    let mut value = coalesce_integer_term(binding, variable, first)?;
    for term in rest {
        value -= coalesce_integer_term(binding, variable, term)?;
    }
    Ok(value)
}

fn coalesce_integer_term(
    binding: &Binding,
    variable: &str,
    term: &CoalesceDifferenceProjectionTerm,
) -> Result<i64> {
    match binding_property(binding, variable, &term.property) {
        Some(Value::Int(value)) => Ok(*value),
        Some(Value::Null) | None => integer_value(&term.default, "COALESCE default"),
        Some(value) => Err(SkeinError::Execution(format!(
            "COALESCE difference requires integer property '{}.{}', got {value:?}",
            variable, term.property
        ))),
    }
}

fn integer_value(value: &Value, context: &str) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        value => Err(SkeinError::Execution(format!(
            "{context} requires an integer value, got {value:?}"
        ))),
    }
}

pub(super) fn group_key_value(item: &Projection, catalog: &Catalog, binding: &Binding) -> Value {
    project_value(item, catalog, binding).unwrap_or(Value::Null)
}

fn timestamp_date_part(part: DatePart, nanos: i64) -> i64 {
    let days = div_floor(nanos, 86_400_000_000_000);
    let (year, month, _) = civil_from_days(days);
    match part {
        DatePart::Year => year as i64,
        DatePart::Month => month as i64,
    }
}

fn div_floor(value: i64, divisor: i64) -> i64 {
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && ((remainder < 0) != (divisor < 0)) {
        quotient - 1
    } else {
        quotient
    }
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

pub(super) fn binding_has_variable(binding: &Binding, variable: &str) -> bool {
    binding.nodes.contains_key(variable) || binding.relationships.contains_key(variable)
}

pub(super) fn binding_property<'a>(
    binding: &'a Binding,
    variable: &str,
    property: &str,
) -> Option<&'a Value> {
    binding
        .nodes
        .get(variable)
        .and_then(|node| node.properties.get(property))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .and_then(|relationship| relationship.properties.get(property))
        })
}

pub(super) fn binding_value(binding: &Binding, catalog: &Catalog, variable: &str) -> Option<Value> {
    binding
        .nodes
        .get(variable)
        .map(|node| node_value(node, catalog))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| relationship_value(relationship, catalog))
        })
}

fn node_value(node: &NodeRecord, catalog: &Catalog) -> Value {
    let mut values = node.properties.clone();
    values.insert("_id".to_string(), Value::Int(node.id.0 as i64));
    values.insert(
        "labels".to_string(),
        Value::List(
            node.labels
                .iter()
                .filter_map(|label_id| catalog.label_name(*label_id))
                .map(|label| Value::String(label.to_string()))
                .collect(),
        ),
    );
    Value::Map(values)
}

pub(super) fn null_lookup_node() -> NodeRecord {
    NodeRecord {
        id: NodeId(0),
        labels: BTreeSet::new(),
        properties: BTreeMap::new(),
    }
}

fn relationship_value(relationship: &RelRecord, catalog: &Catalog) -> Value {
    let mut values = relationship.properties.clone();
    values.insert("_id".to_string(), Value::Int(relationship.id.0 as i64));
    values.insert(
        "source_id".to_string(),
        Value::Int(relationship.source.0 as i64),
    );
    values.insert(
        "target_id".to_string(),
        Value::Int(relationship.target.0 as i64),
    );
    values.insert(
        "type".to_string(),
        catalog
            .rel_type_name(relationship.rel_type)
            .map(|rel_type| Value::String(rel_type.to_string()))
            .unwrap_or(Value::Null),
    );
    Value::Map(values)
}

fn binding_id(binding: &Binding, variable: &str) -> Option<Value> {
    binding
        .nodes
        .get(variable)
        .map(|node| Value::Int(node.id.0 as i64))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| Value::Int(relationship.id.0 as i64))
        })
}

pub(super) fn bounded_expand_targets(
    store: &GraphStore,
    source: NodeId,
    rel_type_id: crate::schema::RelTypeId,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    min_hops: usize,
    max_hops: usize,
    memory_budget_bytes: usize,
) -> Result<Vec<(NodeRecord, usize)>> {
    let mut targets = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(
        NonZeroUsize::new(memory_budget_bytes)
            .expect("execution memory budget is represented by NonZeroUsize"),
    );
    let stack_entry_bytes = std::mem::size_of::<(NodeId, usize)>();
    tracker.charge(stack_entry_bytes);
    let mut stack = vec![(source, 0usize)];
    while let Some((current, depth)) = stack.pop() {
        tracker.release(stack_entry_bytes);
        if depth >= min_hops
            && let Some(node) = store.node_owned(current)?
            && node_matches_label_pattern(&node, target_label_ids)
        {
            let bytes = node_memory_bytes(&node).saturating_add(std::mem::size_of::<usize>());
            ensure_operator_item_fits("AdjacencyExpandExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AdjacencyExpandExec traversal state exceeds blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.charge(bytes);
            targets.push((node, depth));
        }
        if depth == max_hops {
            continue;
        }
        let mut neighbors = Vec::new();
        let mut callback_error = None;
        store.visit_adjacent_relationships_owned(
            current,
            Some(rel_type_id),
            AdjacencyDirection::Outgoing,
            |relationship| {
                if tracker.would_exceed(stack_entry_bytes) {
                    callback_error = Some(SkeinError::Execution(format!(
                        "AdjacencyExpandExec traversal state exceeds blocking_operator_bytes {}",
                        tracker.budget_bytes
                    )));
                    return GraphScanControl::Stop;
                }
                tracker.charge(stack_entry_bytes);
                neighbors.push((relationship.target, relationship.id));
                GraphScanControl::Continue
            },
        )?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        neighbors.sort_unstable_by(|left, right| right.cmp(left));
        for (neighbor_id, _) in neighbors {
            stack.push((neighbor_id, depth + 1));
        }
    }
    Ok(targets)
}

fn aggregate_value(catalog: &Catalog, item: &Aggregation, input: &[Binding]) -> Value {
    match item.function {
        AggregateFunction::Count => {
            Value::Int(count_aggregate(&item.target, item.distinct, input) as i64)
        }
        AggregateFunction::Min => min_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Max => max_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Avg => avg_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Collect => {
            collect_aggregate(catalog, &item.target, item.distinct, input)
        }
    }
}

fn count_aggregate(target: &AggregateTarget, distinct: bool, input: &[Binding]) -> usize {
    if distinct {
        return count_distinct_aggregate(target, input);
    }
    match target {
        AggregateTarget::All => input.len(),
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter(|binding| binding_has_variable(binding, variable))
            .count(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter(|binding| {
                binding_property(binding, variable, property)
                    .map(|value| value != &Value::Null)
                    .unwrap_or(false)
            })
            .count(),
    }
}

fn count_distinct_aggregate(target: &AggregateTarget, input: &[Binding]) -> usize {
    match target {
        AggregateTarget::All => input.len(),
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter_map(|binding| binding_identity_key(binding, variable))
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
    }
}

fn min_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    match target {
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .min(),
        AggregateTarget::All | AggregateTarget::Variable(_) => None,
    }
}

fn max_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    match target {
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .max(),
        AggregateTarget::All | AggregateTarget::Variable(_) => None,
    }
}

fn avg_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    let AggregateTarget::Property { variable, property } = target else {
        return None;
    };
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in input
        .iter()
        .filter_map(|binding| binding_property(binding, variable, property))
    {
        match value {
            Value::Int(value) => {
                sum += *value as f64;
                count += 1;
            }
            Value::Float(value) if value.is_finite() => {
                sum += *value;
                count += 1;
            }
            _ => {}
        }
    }
    (count > 0).then_some(Value::Float(sum / count as f64))
}

fn collect_aggregate(
    catalog: &Catalog,
    target: &AggregateTarget,
    distinct: bool,
    input: &[Binding],
) -> Value {
    let values: Vec<Value> = match target {
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter_map(|binding| binding_value(binding, catalog, variable))
            .filter(|value| *value != Value::Null)
            .collect(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .collect(),
        AggregateTarget::All => Vec::new(),
    };
    if distinct {
        Value::List(
            values
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
        )
    } else {
        Value::List(values)
    }
}

pub(super) fn binding_identity_key(binding: &Binding, variable: &str) -> Option<(u8, u64)> {
    binding
        .nodes
        .get(variable)
        .map(|node| (0, node.id.0))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| (1, relationship.id.0))
        })
}

pub(super) fn evaluate_predicate(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
) -> Result<bool> {
    Ok(evaluate_predicate_truth(predicate, catalog, store, binding)?.is_true())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateTruth {
    True,
    False,
    Unknown,
}

impl PredicateTruth {
    const fn from_bool(value: bool) -> Self {
        if value {
            Self::True
        } else {
            Self::False
        }
    }

    const fn is_true(self) -> bool {
        matches!(self, Self::True)
    }

    const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::True, Self::True) => Self::True,
        }
    }

    const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::False, Self::False) => Self::False,
        }
    }
}

fn predicate_comparison_truth(
    actual: Option<&Value>,
    expected: &Value,
    compare: impl FnOnce(&Value, &Value) -> bool,
) -> PredicateTruth {
    match actual {
        Some(actual) if actual != &Value::Null && expected != &Value::Null => {
            PredicateTruth::from_bool(compare(actual, expected))
        }
        _ => PredicateTruth::Unknown,
    }
}

fn predicate_in_truth(actual: Option<&Value>, values: &[Value]) -> PredicateTruth {
    let Some(actual) = actual.filter(|actual| *actual != &Value::Null) else {
        return PredicateTruth::Unknown;
    };
    if values
        .iter()
        .any(|value| value != &Value::Null && value == actual)
    {
        PredicateTruth::True
    } else if values.iter().any(|value| value == &Value::Null) {
        PredicateTruth::Unknown
    } else {
        PredicateTruth::False
    }
}

fn evaluate_predicate_truth(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
) -> Result<PredicateTruth> {
    Ok(match predicate {
        Predicate::And(predicates) => {
            let mut truth = PredicateTruth::True;
            for predicate in predicates {
                truth = truth.and(evaluate_predicate_truth(
                    predicate, catalog, store, binding,
                )?);
                if truth == PredicateTruth::False {
                    break;
                }
            }
            truth
        }
        Predicate::Or(predicates) => {
            let mut truth = PredicateTruth::False;
            for predicate in predicates {
                truth = truth.or(evaluate_predicate_truth(
                    predicate, catalog, store, binding,
                )?);
                if truth == PredicateTruth::True {
                    break;
                }
            }
            truth
        }
        Predicate::Not(predicate) => {
            evaluate_predicate_truth(predicate, catalog, store, binding)?.not()
        }
        Predicate::ConstantBool(value) => PredicateTruth::from_bool(*value),
        Predicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => PredicateTruth::from_bool(relationship_exists(
            catalog,
            store,
            binding,
            variable,
            rel_type,
            *direction,
            target_label,
        )?),
        Predicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => PredicateTruth::from_bool(bound_relationship_exists(
            catalog,
            store,
            binding,
            source_variable,
            rel_type,
            *direction,
            target_variable,
        )?),
        Predicate::IdEq { variable, value } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                actual == expected
            })
        }
        Predicate::IdNotEq { variable, value } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                actual != expected
            })
        }
        Predicate::IdCompare {
            variable,
            op,
            value,
        } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                compare_property_values(actual, *op, expected)
            })
        }
        Predicate::IdIn { variable, values } => {
            let actual = binding_id(binding, variable);
            predicate_in_truth(actual.as_ref(), values)
        }
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| actual == expected,
        ),
        Predicate::PropertyNotEq {
            variable,
            property,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| actual != expected,
        ),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| compare_property_values(actual, *op, expected),
        ),
        Predicate::ExpressionEq { expression, value } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| actual == expected,
            )
        }
        Predicate::ExpressionNotEq { expression, value } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| actual != expected,
            )
        }
        Predicate::ExpressionCompare {
            expression,
            op,
            value,
        } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| compare_property_values(actual, *op, expected),
            )
        }
        Predicate::ExpressionContains { expression, value } => {
            match (
                predicate_expression_value(expression, catalog, binding),
                predicate_expression_value(value, catalog, binding),
            ) {
                (Some(Value::Null) | None, _) | (_, Some(Value::Null) | None) => {
                    PredicateTruth::Unknown
                }
                (Some(Value::String(actual)), Some(Value::String(expected))) => {
                    PredicateTruth::from_bool(actual.contains(&expected))
                }
                _ => PredicateTruth::False,
            }
        }
        Predicate::PropertyListContains {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::List(values)) => {
                PredicateTruth::from_bool(values.iter().any(|actual| actual == value))
            }
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyListContainsLower {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::List(values)) => {
                PredicateTruth::from_bool(values.iter().any(|actual| match actual {
                    Value::String(actual) => actual.to_lowercase().contains(value),
                    _ => false,
                }))
            }
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyContains {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.contains(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyStartsWith {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.starts_with(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyEndsWith {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.ends_with(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyRegexMatch {
            variable,
            property,
            pattern,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(pattern.is_match(actual)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyIsNull { variable, property } => PredicateTruth::from_bool(
            binding_property(binding, variable, property)
                .map(|actual| actual == &Value::Null)
                .unwrap_or(true),
        ),
        Predicate::PropertyIsNotNull { variable, property } => PredicateTruth::from_bool(
            binding_property(binding, variable, property)
                .map(|actual| actual != &Value::Null)
                .unwrap_or(false),
        ),
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => predicate_in_truth(binding_property(binding, variable, property), values),
    })
}

fn relationship_exists(
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
    variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_label: &str,
) -> Result<bool> {
    let Some(source) = binding.nodes.get(variable) else {
        return Ok(false);
    };
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            return Ok(false);
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    one_hop_relationships(
        store,
        source.id,
        rel_type_id,
        target_label_ids.as_deref(),
        &BTreeMap::new(),
        None,
        direction,
    )
    .map(|relationships| !relationships.is_empty())
}

fn bound_relationship_exists(
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
    source_variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_variable: &str,
) -> Result<bool> {
    let (Some(source), Some(target)) = (
        binding.nodes.get(source_variable),
        binding.nodes.get(target_variable),
    ) else {
        return Ok(false);
    };
    let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
        return Ok(false);
    };
    one_hop_relationships(
        store,
        source.id,
        Some(rel_type_id),
        None,
        &BTreeMap::new(),
        None,
        direction,
    )
    .map(|relationships| {
        relationships
            .iter()
            .any(|(_, candidate)| candidate.id == target.id)
    })
}

fn predicate_expression_value(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Option<Value> {
    project_expression_value(expression, catalog, binding).ok()
}

pub(super) fn compare_bindings(
    catalog: &Catalog,
    left: &Binding,
    right: &Binding,
    items: &[SortItem],
) -> std::cmp::Ordering {
    for item in items {
        let ordering =
            sort_value(catalog, left, &item.key).cmp(&sort_value(catalog, right, &item.key));
        let ordering = match item.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

pub(super) fn sort_value(catalog: &Catalog, binding: &Binding, key: &SortKey) -> Value {
    match key {
        SortKey::Property { variable, property } => binding_property(binding, variable, property)
            .cloned()
            .unwrap_or(Value::Null),
        SortKey::Id { variable } => binding_id(binding, variable).unwrap_or(Value::Null),
        SortKey::Expression(expression) => {
            project_expression_value(expression, catalog, binding).unwrap_or(Value::Null)
        }
        SortKey::Column(name) => binding.values.get(name).cloned().unwrap_or(Value::Null),
    }
}

pub(super) fn property_filter_from_predicate(predicate: &Predicate) -> Result<PropertyFilter> {
    match predicate {
        Predicate::And(predicates) => predicates
            .iter()
            .map(property_filter_from_predicate)
            .collect::<Result<Vec<_>>>()
            .map(PropertyFilter::And),
        Predicate::Or(predicates) => predicates
            .iter()
            .map(property_filter_from_predicate)
            .collect::<Result<Vec<_>>>()
            .map(PropertyFilter::Or),
        Predicate::Not(predicate) => property_filter_from_predicate(predicate)
            .map(Box::new)
            .map(PropertyFilter::Not),
        Predicate::IdEq { value, .. } => Ok(PropertyFilter::IdEq {
            value: value.clone(),
        }),
        Predicate::IdNotEq { value, .. } => Ok(PropertyFilter::IdNotEq {
            value: value.clone(),
        }),
        Predicate::IdCompare { op, value, .. } => {
            let (lower, upper) = range_bounds_from_comparison(*op, value.clone());
            Ok(PropertyFilter::IdRange { lower, upper })
        }
        Predicate::IdIn { values, .. } => Ok(PropertyFilter::IdIn {
            values: values.clone(),
        }),
        Predicate::PropertyEq {
            property, value, ..
        } => Ok(PropertyFilter::Eq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyNotEq {
            property, value, ..
        } => Ok(PropertyFilter::NotEq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyCompare {
            property,
            op,
            value,
            ..
        } => {
            let (lower, upper) = range_bounds_from_comparison(*op, value.clone());
            Ok(PropertyFilter::Range {
                property: property.clone(),
                lower,
                upper,
            })
        }
        Predicate::ExpressionEq { expression, value } => {
            property_filter_from_default_expression(expression, value, false)
        }
        Predicate::ExpressionNotEq { expression, value } => {
            property_filter_from_default_expression(expression, value, true)
        }
        Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. }
        | Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. } => Err(SkeinError::Execution(
            "expression predicates are not supported in property filters".to_string(),
        )),
        Predicate::PropertyListContains {
            property, value, ..
        } => Ok(PropertyFilter::ListContains {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyListContainsLower {
            property, value, ..
        } => Ok(PropertyFilter::ListContainsLower {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyContains {
            property, value, ..
        } => Ok(PropertyFilter::Contains {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyStartsWith {
            property, value, ..
        } => Ok(PropertyFilter::StartsWith {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyEndsWith {
            property, value, ..
        } => Ok(PropertyFilter::EndsWith {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyRegexMatch {
            property, pattern, ..
        } => Ok(PropertyFilter::RegexMatch {
            property: property.clone(),
            pattern: pattern.clone(),
        }),
        Predicate::PropertyIsNull { property, .. } => Ok(PropertyFilter::IsNull {
            property: property.clone(),
        }),
        Predicate::PropertyIsNotNull { property, .. } => Ok(PropertyFilter::IsNotNull {
            property: property.clone(),
        }),
        Predicate::PropertyIn {
            property, values, ..
        } => Ok(PropertyFilter::In {
            property: property.clone(),
            values: values.clone(),
        }),
    }
}

pub(super) fn predicate_references_only_variable(predicate: &Predicate, variable: &str) -> bool {
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

pub(super) fn node_scan_filter_from_predicate(
    predicate: &Predicate,
    variable: &str,
) -> Option<PropertyFilter> {
    if let Predicate::And(predicates) = predicate {
        let mut filters = predicates
            .iter()
            .filter_map(|predicate| node_scan_filter_from_predicate(predicate, variable))
            .collect::<Vec<_>>();
        return match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(PropertyFilter::And(filters)),
        };
    }
    predicate_references_only_variable(predicate, variable)
        .then(|| property_filter_from_predicate(predicate).ok())
        .flatten()
}

pub(super) fn exact_relationship_scan_filter_from_predicate(
    predicate: &Predicate,
    variable: &str,
) -> Option<PropertyFilter> {
    if let Predicate::And(predicates) = predicate {
        let mut filters = predicates
            .iter()
            .filter_map(|predicate| {
                exact_relationship_scan_filter_from_predicate(predicate, variable)
            })
            .collect::<Vec<_>>();
        return match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(PropertyFilter::And(filters)),
        };
    }
    if predicate_references_only_variable(predicate, variable) {
        return property_filter_from_predicate(predicate)
            .ok()
            .filter(exact_relationship_scan_filter_is_safe);
    }
    None
}

fn exact_relationship_scan_filter_is_safe(filter: &PropertyFilter) -> bool {
    match filter {
        PropertyFilter::And(filters) | PropertyFilter::Or(filters) => {
            filters.iter().all(exact_relationship_scan_filter_is_safe)
        }
        PropertyFilter::Eq { .. }
        | PropertyFilter::IdEq { .. }
        | PropertyFilter::IdRange { .. }
        | PropertyFilter::IdIn { .. }
        | PropertyFilter::IsNull { .. }
        | PropertyFilter::IsNotNull { .. }
        | PropertyFilter::In { .. }
        | PropertyFilter::Range { .. } => true,
        PropertyFilter::DefaultIfNullOrEq { negated, .. } => !negated,
        PropertyFilter::Not(_)
        | PropertyFilter::IdNotEq { .. }
        | PropertyFilter::NotEq { .. }
        | PropertyFilter::ListContains { .. }
        | PropertyFilter::ListContainsLower { .. }
        | PropertyFilter::Contains { .. }
        | PropertyFilter::StartsWith { .. }
        | PropertyFilter::EndsWith { .. }
        | PropertyFilter::RegexMatch { .. } => false,
    }
}

fn property_filter_from_default_expression(
    expression: &ProjectionExpression,
    value: &ProjectionExpression,
    negated: bool,
) -> Result<PropertyFilter> {
    match (expression, value) {
        (
            ProjectionExpression::DefaultIfNullOrEq {
                property,
                empty,
                default,
                ..
            },
            ProjectionExpression::Literal(value),
        ) => Ok(PropertyFilter::DefaultIfNullOrEq {
            property: property.clone(),
            empty: empty.clone(),
            default: default.clone(),
            value: value.clone(),
            negated,
        }),
        (
            ProjectionExpression::Literal(value),
            ProjectionExpression::DefaultIfNullOrEq {
                property,
                empty,
                default,
                ..
            },
        ) => Ok(PropertyFilter::DefaultIfNullOrEq {
            property: property.clone(),
            empty: empty.clone(),
            default: default.clone(),
            value: value.clone(),
            negated,
        }),
        _ => Err(SkeinError::Execution(
            "expression predicates are not supported in property filters".to_string(),
        )),
    }
}

pub(super) fn relationship_filter_from_properties_and_predicate(
    properties: &BTreeMap<String, Value>,
    predicate: Option<&Predicate>,
) -> Result<Option<PropertyFilter>> {
    Ok(combine_property_filters(
        property_filter_from_properties(properties),
        predicate.map(property_filter_from_predicate).transpose()?,
    ))
}

fn range_bounds_from_comparison(op: ComparisonOp, value: Value) -> ValueRangeBounds {
    match op {
        ComparisonOp::Lt => (None, Some((value, false))),
        ComparisonOp::Lte => (None, Some((value, true))),
        ComparisonOp::Gt => (Some((value, false)), None),
        ComparisonOp::Gte => (Some((value, true)), None),
    }
}
