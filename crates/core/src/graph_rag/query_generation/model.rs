use std::fmt::{Display, Formatter};

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
    TwoHopRoute {
        source_label: String,
        first_relationship_type: String,
        intermediate_label: String,
        second_relationship_type: String,
        target_label: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GraphRagQueryBinding {
    Source,
    Relationship,
    Intermediate,
    SecondRelationship,
    Target,
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
    pub(super) const fn token(self) -> &'static str {
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

    pub(super) const fn requires_parameter(self) -> bool {
        !matches!(self, Self::IsNull | Self::IsNotNull)
    }

    pub(super) const fn requires_string_property(self) -> bool {
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
