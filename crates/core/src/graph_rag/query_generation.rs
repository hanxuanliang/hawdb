use super::{GraphRagPropertySubject, GraphRagSchemaContext};
use crate::PropertyType;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter, Write};

pub const MAX_GRAPH_RAG_QUERY_LIMIT: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphRagQueryPattern {
    Node {
        label: String,
    },
    Route {
        source_label: String,
        relationship_type: String,
        target_label: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphRagQueryBinding {
    Source,
    Relationship,
    Target,
}

impl GraphRagQueryBinding {
    const fn variable(self) -> &'static str {
        match self {
            Self::Source => "n0",
            Self::Relationship => "r0",
            Self::Target => "n1",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphRagQueryPredicateOperator {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
}

impl GraphRagQueryPredicateOperator {
    const fn token(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::In => "IN",
            Self::Contains => "CONTAINS",
            Self::StartsWith => "STARTS WITH",
            Self::EndsWith => "ENDS WITH",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
        }
    }

    const fn requires_parameter(self) -> bool {
        !matches!(self, Self::IsNull | Self::IsNotNull)
    }

    const fn requires_string_property(self) -> bool {
        matches!(self, Self::Contains | Self::StartsWith | Self::EndsWith)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagQueryPredicate {
    pub binding: GraphRagQueryBinding,
    pub property: String,
    pub operator: GraphRagQueryPredicateOperator,
    pub parameter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagQueryProjection {
    pub binding: GraphRagQueryBinding,
    pub property: String,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagQueryDraft {
    pub schema_fingerprint: u64,
    pub pattern: GraphRagQueryPattern,
    pub predicates: Vec<GraphRagQueryPredicate>,
    pub projections: Vec<GraphRagQueryProjection>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRagGeneratedQuery {
    pub cypher: String,
    pub schema_fingerprint: u64,
    pub required_parameters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphRagQueryGenerationError {
    SchemaFingerprintMismatch {
        expected: u64,
        actual: u64,
    },
    InvalidIdentifier {
        kind: &'static str,
        value: String,
    },
    UnknownLabel(String),
    UnknownRoute {
        source_label: String,
        relationship_type: String,
        target_label: String,
    },
    BindingUnavailable(GraphRagQueryBinding),
    PropertyUnavailable {
        binding: GraphRagQueryBinding,
        property: String,
    },
    OperatorRequiresStringProperty {
        binding: GraphRagQueryBinding,
        property: String,
    },
    MissingParameter {
        property: String,
    },
    UnexpectedParameter {
        property: String,
    },
    EmptyProjection,
    InvalidLimit {
        limit: usize,
        maximum: usize,
    },
}

impl Display for GraphRagQueryGenerationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaFingerprintMismatch { expected, actual } => write!(
                formatter,
                "schema fingerprint mismatch: expected {expected:016x}, got {actual:016x}"
            ),
            Self::InvalidIdentifier { kind, value } => {
                write!(formatter, "invalid {kind} identifier: {value}")
            }
            Self::UnknownLabel(label) => write!(formatter, "label is not in schema context: {label}"),
            Self::UnknownRoute {
                source_label,
                relationship_type,
                target_label,
            } => write!(
                formatter,
                "route is not in schema context: ({source_label})-[:{relationship_type}]->({target_label})"
            ),
            Self::BindingUnavailable(binding) => {
                write!(formatter, "query binding is unavailable: {binding:?}")
            }
            Self::PropertyUnavailable { binding, property } => write!(
                formatter,
                "property is not in schema context for {binding:?}: {property}"
            ),
            Self::OperatorRequiresStringProperty { binding, property } => write!(
                formatter,
                "predicate requires a string property for {binding:?}: {property}"
            ),
            Self::MissingParameter { property } => {
                write!(formatter, "predicate parameter is required for: {property}")
            }
            Self::UnexpectedParameter { property } => {
                write!(formatter, "predicate parameter is not allowed for: {property}")
            }
            Self::EmptyProjection => formatter.write_str("at least one projection is required"),
            Self::InvalidLimit { limit, maximum } => {
                write!(formatter, "query limit must be between 1 and {maximum}, got {limit}")
            }
        }
    }
}

impl std::error::Error for GraphRagQueryGenerationError {}

struct ResolvedPattern<'a> {
    source_label: &'a str,
    relationship_type: Option<&'a str>,
    target_label: Option<&'a str>,
}

pub(super) fn generate_query(
    context: &GraphRagSchemaContext,
    draft: &GraphRagQueryDraft,
) -> Result<GraphRagGeneratedQuery, GraphRagQueryGenerationError> {
    if draft.schema_fingerprint != context.fingerprint {
        return Err(GraphRagQueryGenerationError::SchemaFingerprintMismatch {
            expected: context.fingerprint,
            actual: draft.schema_fingerprint,
        });
    }
    if draft.projections.is_empty() {
        return Err(GraphRagQueryGenerationError::EmptyProjection);
    }
    if draft.limit == 0 || draft.limit > MAX_GRAPH_RAG_QUERY_LIMIT {
        return Err(GraphRagQueryGenerationError::InvalidLimit {
            limit: draft.limit,
            maximum: MAX_GRAPH_RAG_QUERY_LIMIT,
        });
    }

    let pattern = resolve_pattern(context, &draft.pattern)?;
    let mut required_parameters = BTreeSet::new();
    let mut cypher = render_match_pattern(&pattern);

    if !draft.predicates.is_empty() {
        cypher.push_str(" WHERE ");
        for (index, predicate) in draft.predicates.iter().enumerate() {
            if index > 0 {
                cypher.push_str(" AND ");
            }
            let value_type =
                validate_property(context, &pattern, predicate.binding, &predicate.property)?;
            if predicate.operator.requires_string_property()
                && !matches!(value_type, PropertyType::String | PropertyType::Any)
            {
                return Err(
                    GraphRagQueryGenerationError::OperatorRequiresStringProperty {
                        binding: predicate.binding,
                        property: predicate.property.clone(),
                    },
                );
            }
            render_predicate(&mut cypher, predicate, &mut required_parameters)?;
        }
    }

    cypher.push_str(" RETURN ");
    for (index, projection) in draft.projections.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        validate_property(context, &pattern, projection.binding, &projection.property)?;
        validate_identifier("projection alias", &projection.alias)?;
        let _ = write!(
            cypher,
            "{}.{} AS {}",
            projection.binding.variable(),
            projection.property,
            projection.alias
        );
    }
    let _ = write!(cypher, " LIMIT {}", draft.limit);

    Ok(GraphRagGeneratedQuery {
        cypher,
        schema_fingerprint: context.fingerprint,
        required_parameters: required_parameters.into_iter().collect(),
    })
}

fn resolve_pattern<'a>(
    context: &GraphRagSchemaContext,
    pattern: &'a GraphRagQueryPattern,
) -> Result<ResolvedPattern<'a>, GraphRagQueryGenerationError> {
    match pattern {
        GraphRagQueryPattern::Node { label } => {
            validate_identifier("label", label)?;
            validate_label(context, label)?;
            Ok(ResolvedPattern {
                source_label: label,
                relationship_type: None,
                target_label: None,
            })
        }
        GraphRagQueryPattern::Route {
            source_label,
            relationship_type,
            target_label,
        } => {
            validate_identifier("source label", source_label)?;
            validate_identifier("relationship type", relationship_type)?;
            validate_identifier("target label", target_label)?;
            validate_label(context, source_label)?;
            validate_label(context, target_label)?;
            if !context.routes.iter().any(|route| {
                route.source_label == *source_label
                    && route.relationship_type == *relationship_type
                    && route.target_label == *target_label
            }) {
                return Err(GraphRagQueryGenerationError::UnknownRoute {
                    source_label: source_label.clone(),
                    relationship_type: relationship_type.clone(),
                    target_label: target_label.clone(),
                });
            }
            Ok(ResolvedPattern {
                source_label,
                relationship_type: Some(relationship_type),
                target_label: Some(target_label),
            })
        }
    }
}

fn render_match_pattern(pattern: &ResolvedPattern<'_>) -> String {
    match (pattern.relationship_type, pattern.target_label) {
        (Some(relationship_type), Some(target_label)) => format!(
            "MATCH (n0:{})-[r0:{}]->(n1:{})",
            pattern.source_label, relationship_type, target_label
        ),
        _ => format!("MATCH (n0:{})", pattern.source_label),
    }
}

fn render_predicate(
    output: &mut String,
    predicate: &GraphRagQueryPredicate,
    required_parameters: &mut BTreeSet<String>,
) -> Result<(), GraphRagQueryGenerationError> {
    validate_identifier("property", &predicate.property)?;
    let _ = write!(
        output,
        "{}.{} {}",
        predicate.binding.variable(),
        predicate.property,
        predicate.operator.token()
    );
    match (
        predicate.operator.requires_parameter(),
        predicate.parameter.as_deref(),
    ) {
        (true, Some(parameter)) => {
            validate_identifier("parameter", parameter)?;
            let _ = write!(output, " ${parameter}");
            required_parameters.insert(parameter.to_string());
        }
        (true, None) => {
            return Err(GraphRagQueryGenerationError::MissingParameter {
                property: predicate.property.clone(),
            });
        }
        (false, Some(_)) => {
            return Err(GraphRagQueryGenerationError::UnexpectedParameter {
                property: predicate.property.clone(),
            });
        }
        (false, None) => {}
    }
    Ok(())
}

fn validate_label(
    context: &GraphRagSchemaContext,
    label: &str,
) -> Result<(), GraphRagQueryGenerationError> {
    if context
        .labels
        .iter()
        .any(|candidate| candidate.name == label)
    {
        Ok(())
    } else {
        Err(GraphRagQueryGenerationError::UnknownLabel(
            label.to_string(),
        ))
    }
}

fn validate_property(
    context: &GraphRagSchemaContext,
    pattern: &ResolvedPattern<'_>,
    binding: GraphRagQueryBinding,
    property: &str,
) -> Result<PropertyType, GraphRagQueryGenerationError> {
    validate_identifier("property", property)?;
    let (subject, subject_name) = match binding {
        GraphRagQueryBinding::Source => (GraphRagPropertySubject::Node, pattern.source_label),
        GraphRagQueryBinding::Relationship => (
            GraphRagPropertySubject::Relationship,
            pattern
                .relationship_type
                .ok_or(GraphRagQueryGenerationError::BindingUnavailable(binding))?,
        ),
        GraphRagQueryBinding::Target => (
            GraphRagPropertySubject::Node,
            pattern
                .target_label
                .ok_or(GraphRagQueryGenerationError::BindingUnavailable(binding))?,
        ),
    };
    context
        .properties
        .iter()
        .find(|candidate| {
            candidate.subject == subject
                && candidate.subject_name == subject_name
                && candidate.name == property
        })
        .map(|property| property.value_type)
        .ok_or_else(|| GraphRagQueryGenerationError::PropertyUnavailable {
            binding,
            property: property.to_string(),
        })
}

fn validate_identifier(
    kind: &'static str,
    identifier: &str,
) -> Result<(), GraphRagQueryGenerationError> {
    let mut chars = identifier.chars();
    let valid = chars
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_');
    if valid {
        Ok(())
    } else {
        Err(GraphRagQueryGenerationError::InvalidIdentifier {
            kind,
            value: identifier.to_string(),
        })
    }
}
