use skein_core::{PropertyType, SchemaObjectState, TableKind, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedNodesCreate {
    pub source_label: String,
    pub source_properties: BTreeMap<String, Value>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipCreate {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipOnCreatePropertyValue {
    Value(Value),
    MatchedRelationshipProperty { property: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipCopyMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, RelationshipOnCreatePropertyValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipRetargetMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub old_target_label: String,
    pub old_target_filter: Option<PropertyFilter>,
    pub new_target_label: String,
    pub new_target_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipSourceRetargetMerge {
    pub old_source_label: String,
    pub old_source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub old_target_label: String,
    pub old_target_filter: Option<PropertyFilter>,
    pub new_source_label: String,
    pub new_source_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPropertyUpdate {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipSetAssignment {
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPropertiesUpdate {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
    pub assignments: Vec<RelationshipSetAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDeleteRequest {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipTargetNodeDelete {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_filter: Option<PropertyFilter>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphMutation {
    CreateNodeLabel {
        label: String,
    },
    CreateRelationshipType {
        rel_type: String,
    },
    CreateNodeTable {
        name: String,
    },
    CreateRelationshipTable {
        name: String,
    },
    CreateProperty {
        table_kind: TableKind,
        table: String,
        property: String,
        value_type: PropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: TableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: TableKind,
        table: String,
        property: String,
        state: SchemaObjectState,
    },
    CreateIndex {
        label: String,
        property: String,
    },
    CreateCompositeIndex {
        label: String,
        properties: Vec<String>,
    },
    CreateRangeIndex {
        label: String,
        property: String,
    },
    CreateFullTextIndex {
        label: String,
        property: String,
    },
    CreateUniqueConstraint {
        label: String,
        property: String,
    },
    CreateNodePropertyExistsConstraint {
        label: String,
        property: String,
    },
    CreateRelationshipUniqueConstraint {
        rel_type: String,
        property: String,
    },
    CreateRelationshipPropertyExistsConstraint {
        rel_type: String,
        property: String,
    },
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    MergeNode {
        label: String,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: Vec<NodeSetAssignment>,
        post_merge_assignments: Vec<NodeSetAssignment>,
    },
    MergeConnectedNodes(ConnectedNodesCreate),
    SetNodeProperty {
        label: String,
        filter: Option<PropertyFilter>,
        property: String,
        value: Value,
    },
    SetNodePropertyAddInt {
        label: String,
        filter: Option<PropertyFilter>,
        property: String,
        amount: i64,
    },
    SetNodeProperties {
        label: String,
        filter: Option<PropertyFilter>,
        assignments: Vec<NodeSetAssignment>,
    },
    SetRelationshipProperty {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
        property: String,
        value: Value,
    },
    SetRelationshipProperties {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
        assignments: Vec<RelationshipSetAssignment>,
    },
    DeleteNode {
        label: String,
        filter: Option<PropertyFilter>,
        detach: bool,
    },
    DeleteRelationship {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
    },
    DeleteRelationshipTargetNodes(RelationshipTargetNodeDelete),
    CreateRelationshipsBetweenMatches(MatchedRelationshipCreate),
    MergeRelationshipsBetweenMatches(MatchedRelationshipMerge),
    MergeRelationshipsFromMatchedRelationships(MatchedRelationshipCopyMerge),
    MergeRelationshipsToMatchedTarget(MatchedRelationshipRetargetMerge),
    MergeRelationshipsFromMatchedTarget(MatchedRelationshipSourceRetargetMerge),
    CreateConnectedNodes(ConnectedNodesCreate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSetAssignment {
    pub property: String,
    pub value: NodeSetValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSetValue {
    Value(Value),
    Coalesce { default: Value },
    AddInt { amount: i64 },
    DecrementFloorZero,
    PreserveNewerExisting { incoming: Value, preserve: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyFilter {
    And(Vec<PropertyFilter>),
    Or(Vec<PropertyFilter>),
    Not(Box<PropertyFilter>),
    IdEq {
        value: Value,
    },
    IdNotEq {
        value: Value,
    },
    IdRange {
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    IdIn {
        values: Vec<Value>,
    },
    Eq {
        property: String,
        value: Value,
    },
    NotEq {
        property: String,
        value: Value,
    },
    IsNull {
        property: String,
    },
    IsNotNull {
        property: String,
    },
    In {
        property: String,
        values: Vec<Value>,
    },
    ListContains {
        property: String,
        value: Value,
    },
    Contains {
        property: String,
        value: String,
    },
    StartsWith {
        property: String,
        value: String,
    },
    EndsWith {
        property: String,
        value: String,
    },
    RegexMatch {
        property: String,
        pattern: String,
    },
    DefaultIfNullOrEq {
        property: String,
        empty: Value,
        default: Value,
        value: Value,
        negated: bool,
    },
    Range {
        property: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_contract_keeps_property_filters_structured() {
        let mutation = GraphMutation::DeleteNode {
            label: "Memory".to_string(),
            filter: Some(PropertyFilter::Eq {
                property: "status".to_string(),
                value: Value::String("deleted".to_string()),
            }),
            detach: false,
        };
        assert!(matches!(
            mutation,
            GraphMutation::DeleteNode {
                filter: Some(PropertyFilter::Eq { .. }),
                ..
            }
        ));
    }
}
