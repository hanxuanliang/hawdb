use crate::cypher::RelationshipDirection;
use crate::planner::{
    Aggregation, GraphAlgorithmKind, GraphAlgorithmOptions, Predicate, Projection,
    RelationshipCountLeg, RelationshipOnCreateValue, RelationshipSetAssignment, SchemaObjectState,
    SchemaPropertyType, SchemaTableKind, SetAssignment, SetNodePropertiesReturnMode, SetValue,
    ShortestPathProjection, SortItem,
};
use crate::value::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalOperatorDomain {
    Schema,
    Mutation,
    Access,
    Traversal,
    Relational,
    Procedure,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
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
        table_kind: SchemaTableKind,
        table: String,
        property: String,
        value_type: SchemaPropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: SchemaTableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: SchemaTableKind,
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
    ProjectGraph {
        name: String,
        node_labels: Vec<String>,
        rel_types: Vec<String>,
    },
    GraphAlgorithm {
        algorithm: GraphAlgorithmKind,
        graph_name: String,
        options: GraphAlgorithmOptions,
        score_column: String,
    },
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    MergeNode {
        label: String,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: Vec<SetAssignment>,
        post_merge_assignments: Vec<SetAssignment>,
    },
    MergeRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    MergeMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    MergeRelationshipFromMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, RelationshipOnCreateValue>,
    },
    MergeRelationshipToMatchedTarget {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        old_target_label: String,
        old_target_properties: BTreeMap<String, Value>,
        new_target_label: String,
        new_target_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    MergeRelationshipFromMatchedTarget {
        old_source_label: String,
        old_source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        old_target_label: String,
        old_target_properties: BTreeMap<String, Value>,
        new_source_label: String,
        new_source_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    CreateMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
    },
    SetNodeProperty {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        property: String,
        value: SetValue,
    },
    SetNodeProperties {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        assignments: Vec<SetAssignment>,
    },
    SetNodePropertiesReturn {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        assignments: Vec<SetAssignment>,
        returns: SetNodePropertiesReturnMode,
    },
    SetRelationshipProperty {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        property: String,
        value: Value,
    },
    SetRelationshipProperties {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        assignments: Vec<RelationshipSetAssignment>,
    },
    DeleteNode {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        detach: bool,
    },
    DeleteRelationship {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    DeleteRelationshipTargetNodes {
        source_variable: String,
        source_label: String,
        source_predicate: Option<Predicate>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        detach: bool,
    },
    CreateRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    SeqNodeScan {
        variable: String,
        label: String,
    },
    NodeCartesianProductExec {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
    NodeColumnLookupExec {
        variable: String,
        label: String,
        property: String,
        column: String,
        optional: bool,
        input: Box<PhysicalPlan>,
    },
    IndexNodeSeek {
        variable: String,
        label: String,
        property: String,
        value: Value,
    },
    IndexNodeMultiSeek {
        variable: String,
        label: String,
        property: String,
        values: Vec<Value>,
    },
    IndexNodeCompositeSeek {
        variable: String,
        label: String,
        predicates: Vec<(String, Value)>,
    },
    IndexNodeRangeSeek {
        variable: String,
        label: String,
        property: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    IndexNodeTextSeek {
        variable: String,
        label: String,
        property: String,
        query: String,
    },
    AdjacencyExpandExec {
        source_variable: String,
        source_label: String,
        rel_variable: Option<String>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        target_variable: String,
        target_label: String,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        input: Box<PhysicalPlan>,
    },
    OptionalDegreeExec {
        source_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        alias: String,
        input: Box<PhysicalPlan>,
    },
    OptionalRelationshipCountSumExec {
        variable: String,
        label: String,
        properties: BTreeMap<String, Value>,
        legs: Vec<RelationshipCountLeg>,
        output: String,
    },
    ThreadRepairStatsExec {
        label: String,
        identity_label: String,
        identity_ref_property: String,
        thread_id_property: String,
        message_rel_type: String,
        message_label: String,
        memory_rel_type: String,
        memory_label: String,
    },
    ShortestPathExec {
        source_variable: String,
        source_label: String,
        source_id: Value,
        rel_type: String,
        direction: RelationshipDirection,
        target_variable: String,
        target_label: String,
        target_id: Value,
        min_hops: usize,
        max_hops: usize,
        returns: Vec<ShortestPathProjection>,
    },
    FilterExec {
        predicate: Predicate,
        input: Box<PhysicalPlan>,
    },
    ProjectExec {
        items: Vec<Projection>,
        input: Box<PhysicalPlan>,
    },
    AggregateExec {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
        input: Box<PhysicalPlan>,
    },
    DistinctExec {
        input: Box<PhysicalPlan>,
    },
    SortExec {
        items: Vec<SortItem>,
        input: Box<PhysicalPlan>,
    },
    LimitExec {
        offset: usize,
        limit: Option<usize>,
        input: Box<PhysicalPlan>,
    },
}

impl PhysicalPlan {
    pub fn domain(&self) -> PhysicalOperatorDomain {
        match self {
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
            | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. } => {
                PhysicalOperatorDomain::Schema
            }
            PhysicalPlan::CreateNode { .. }
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
            | PhysicalPlan::CreateRelationship { .. } => PhysicalOperatorDomain::Mutation,
            PhysicalPlan::SeqNodeScan { .. }
            | PhysicalPlan::NodeColumnLookupExec { .. }
            | PhysicalPlan::IndexNodeSeek { .. }
            | PhysicalPlan::IndexNodeMultiSeek { .. }
            | PhysicalPlan::IndexNodeCompositeSeek { .. }
            | PhysicalPlan::IndexNodeRangeSeek { .. }
            | PhysicalPlan::IndexNodeTextSeek { .. } => PhysicalOperatorDomain::Access,
            PhysicalPlan::AdjacencyExpandExec { .. }
            | PhysicalPlan::OptionalDegreeExec { .. }
            | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
            | PhysicalPlan::ShortestPathExec { .. } => PhysicalOperatorDomain::Traversal,
            PhysicalPlan::NodeCartesianProductExec { .. }
            | PhysicalPlan::FilterExec { .. }
            | PhysicalPlan::ProjectExec { .. }
            | PhysicalPlan::AggregateExec { .. }
            | PhysicalPlan::DistinctExec { .. }
            | PhysicalPlan::SortExec { .. }
            | PhysicalPlan::LimitExec { .. } => PhysicalOperatorDomain::Relational,
            PhysicalPlan::ProjectGraph { .. }
            | PhysicalPlan::GraphAlgorithm { .. }
            | PhysicalPlan::ThreadRepairStatsExec { .. } => PhysicalOperatorDomain::Procedure,
        }
    }
}
