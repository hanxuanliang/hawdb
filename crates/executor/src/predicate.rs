//! Storage record predicates shared by execution operators.

use skein_core::{Catalog, LabelId, Value};
use skein_plan::ComparisonOp;
use skein_storage::{NodeRecord, PropertyFilter, RelRecord};
use std::cmp::Ordering;
use std::collections::BTreeMap;

type ValueRangeBound = (Value, bool);

pub fn label_ids_for_pattern(catalog: &Catalog, label: &str) -> Option<Vec<LabelId>> {
    if label.is_empty() {
        return None;
    }
    Some(
        label
            .split(':')
            .filter_map(|label| catalog.label_id(label))
            .collect(),
    )
}

pub fn node_matches_label_pattern(node: &NodeRecord, label_ids: Option<&[LabelId]>) -> bool {
    match label_ids {
        None => true,
        Some(label_ids) => label_ids
            .iter()
            .any(|label_id| node.labels.contains(label_id)),
    }
}

pub fn node_properties_match(node: &NodeRecord, properties: &BTreeMap<String, Value>) -> bool {
    properties
        .iter()
        .all(|(property, value)| node.properties.get(property) == Some(value))
}

pub fn node_matches_property_filter(node: &NodeRecord, filter: &PropertyFilter) -> bool {
    property_filter_matches_values(filter, node.id.0, &node.properties)
}

pub fn property_filter_matches_values(
    filter: &PropertyFilter,
    id: u64,
    properties: &BTreeMap<String, Value>,
) -> bool {
    match filter {
        PropertyFilter::And(filters) => filters
            .iter()
            .all(|filter| property_filter_matches_values(filter, id, properties)),
        PropertyFilter::Or(filters) => filters
            .iter()
            .any(|filter| property_filter_matches_values(filter, id, properties)),
        PropertyFilter::Not(filter) => !property_filter_matches_values(filter, id, properties),
        PropertyFilter::IdEq { value } => &Value::Int(id as i64) == value,
        PropertyFilter::IdNotEq { value } => &Value::Int(id as i64) != value,
        PropertyFilter::IdRange { lower, upper } => {
            value_matches_range(&Value::Int(id as i64), lower.as_ref(), upper.as_ref())
        }
        PropertyFilter::IdIn { values } => {
            values.iter().any(|value| value == &Value::Int(id as i64))
        }
        PropertyFilter::Eq { property, value } => properties
            .get(property)
            .map(|actual| actual == value)
            .unwrap_or(false),
        PropertyFilter::NotEq { property, value } => properties
            .get(property)
            .map(|actual| actual != value)
            .unwrap_or(false),
        PropertyFilter::IsNull { property } => properties
            .get(property)
            .map(|actual| actual == &Value::Null)
            .unwrap_or(true),
        PropertyFilter::IsNotNull { property } => properties
            .get(property)
            .map(|actual| actual != &Value::Null)
            .unwrap_or(false),
        PropertyFilter::In { property, values } => properties
            .get(property)
            .map(|actual| values.iter().any(|value| value == actual))
            .unwrap_or(false),
        PropertyFilter::ListContains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| actual == value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::ListContainsLower { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| match actual {
                    Value::String(actual) => actual.to_lowercase().contains(value),
                    _ => false,
                })),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::Contains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.contains(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::StartsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.starts_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::EndsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.ends_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::RegexMatch { property, pattern } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(pattern.is_match(actual)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::DefaultIfNullOrEq {
            property,
            empty,
            default,
            value,
            negated,
        } => {
            let actual = properties.get(property).unwrap_or(&Value::Null);
            let normalized = if actual == &Value::Null || actual == empty {
                default
            } else {
                actual
            };
            let matches = normalized == value;
            if *negated {
                !matches
            } else {
                matches
            }
        }
        PropertyFilter::Range {
            property,
            lower,
            upper,
        } => properties
            .get(property)
            .map(|actual| value_matches_range(actual, lower.as_ref(), upper.as_ref()))
            .unwrap_or(false),
    }
}

pub fn relationship_properties_match(
    relationship: &RelRecord,
    properties: &BTreeMap<String, Value>,
) -> bool {
    properties
        .iter()
        .all(|(property, value)| relationship.properties.get(property) == Some(value))
}

pub fn property_filter_from_properties(
    properties: &BTreeMap<String, Value>,
) -> Option<PropertyFilter> {
    if properties.is_empty() {
        return None;
    }
    let mut filters = properties
        .iter()
        .map(|(property, value)| PropertyFilter::Eq {
            property: property.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    if filters.len() == 1 {
        filters.pop()
    } else {
        Some(PropertyFilter::And(filters))
    }
}

pub fn combine_property_filters(
    left: Option<PropertyFilter>,
    right: Option<PropertyFilter>,
) -> Option<PropertyFilter> {
    match (left, right) {
        (None, None) => None,
        (Some(filter), None) | (None, Some(filter)) => Some(filter),
        (Some(left), Some(right)) => Some(PropertyFilter::And(vec![left, right])),
    }
}

pub fn compare_property_values(actual: &Value, op: ComparisonOp, expected: &Value) -> bool {
    let Some(ordering) = comparable_value_ordering(actual, expected) else {
        return false;
    };
    match op {
        ComparisonOp::Lt => ordering == Ordering::Less,
        ComparisonOp::Lte => ordering != Ordering::Greater,
        ComparisonOp::Gt => ordering == Ordering::Greater,
        ComparisonOp::Gte => ordering != Ordering::Less,
    }
}

fn value_matches_range(
    actual: &Value,
    lower: Option<&ValueRangeBound>,
    upper: Option<&ValueRangeBound>,
) -> bool {
    let lower_matches = lower
        .map(|(lower, inclusive)| {
            if *inclusive {
                compare_property_values(actual, ComparisonOp::Gte, lower)
            } else {
                compare_property_values(actual, ComparisonOp::Gt, lower)
            }
        })
        .unwrap_or(true);
    let upper_matches = upper
        .map(|(upper, inclusive)| {
            if *inclusive {
                compare_property_values(actual, ComparisonOp::Lte, upper)
            } else {
                compare_property_values(actual, ComparisonOp::Lt, upper)
            }
        })
        .unwrap_or(true);
    lower_matches && upper_matches
}

fn comparable_value_ordering(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_filters_preserve_null_and_numeric_range_semantics() {
        let properties = BTreeMap::from([
            ("score".to_string(), Value::Int(7)),
            ("optional".to_string(), Value::Null),
        ]);

        assert!(property_filter_matches_values(
            &PropertyFilter::Range {
                property: "score".to_string(),
                lower: Some((Value::Float(6.5), false)),
                upper: Some((Value::Int(7), true)),
            },
            11,
            &properties,
        ));
        assert!(property_filter_matches_values(
            &PropertyFilter::IsNull {
                property: "missing".to_string(),
            },
            11,
            &properties,
        ));
    }

    #[test]
    fn property_maps_form_deterministic_conjunctions() {
        let properties = BTreeMap::from([
            ("kind".to_string(), Value::String("note".to_string())),
            ("space".to_string(), Value::String("default".to_string())),
        ]);
        let filter = property_filter_from_properties(&properties).expect("non-empty filter");

        assert!(matches!(filter, PropertyFilter::And(filters) if filters.len() == 2));
    }
}
