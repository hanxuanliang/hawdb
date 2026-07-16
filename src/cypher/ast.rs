use crate::value::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Checkpoint,
    CreateNodeLabel(String),
    CreateRelationshipType(String),
    CreateNodeTable(String),
    CreateRelationshipTable(String),
    CreateProperty(CreateProperty),
    AlterTableState(AlterTableState),
    AlterPropertyState(AlterPropertyState),
    CreateIndex(CreateIndex),
    CreateCompositeIndex(CreateCompositeIndex),
    CreateRangeIndex(CreateIndex),
    CreateFullTextIndex(CreateIndex),
    CreateUniqueConstraint(CreateIndex),
    CreateNodePropertyExistsConstraint(CreateIndex),
    CreateRelationshipUniqueConstraint(CreateIndex),
    CreateRelationshipPropertyExistsConstraint(CreateIndex),
    ProjectGraph(ProjectGraph),
    GraphAlgorithm(GraphAlgorithm),
    CreateNode(CreateNode),
    CreateRelationship(CreateRelationship),
    MergeNode(MergeNode),
    MergeRelationship(CreateRelationship),
    MatchReturn(Box<MatchReturn>),
    ShortestPathReturn(Box<ShortestPathReturn>),
    MatchNodesReturn(MatchNodesReturn),
    MatchSet(MatchSet),
    MatchSetReturn(MatchSetReturn),
    MatchDelete(MatchDelete),
    MatchCreateRelationship(MatchCreateRelationship),
    MatchMergeRelationship(MatchMergeRelationship),
    MatchExpandMergeRelationship(MatchExpandMergeRelationship),
    MatchExpandMatchMergeRelationship(MatchExpandMatchMergeRelationship),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndex {
    pub label: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCompositeIndex {
    pub label: String,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProperty {
    pub table_kind: SchemaTableKind,
    pub table: String,
    pub property: String,
    pub value_type: SchemaPropertyType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableState {
    pub table_kind: SchemaTableKind,
    pub table: String,
    pub state: SchemaObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPropertyState {
    pub table_kind: SchemaTableKind,
    pub table: String,
    pub property: String,
    pub state: SchemaObjectState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTableKind {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPropertyType {
    Any,
    Bool,
    Int,
    Float,
    String,
    List,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaObjectState {
    DeleteOnly,
    WriteOnly,
    Backfill,
    Validating,
    Public,
    Gc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectGraph {
    pub name: String,
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphAlgorithm {
    pub algorithm: GraphAlgorithmKind,
    pub graph_name: String,
    pub options: GraphAlgorithmOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphAlgorithmKind {
    PageRank,
    Louvain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphAlgorithmOptions {
    pub damping: Option<ValueExpression>,
    pub max_iterations: Option<ValueExpression>,
    pub max_levels: Option<ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNode {
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRelationship {
    pub source: CreateNode,
    pub rel_type: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub target: CreateNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeNode {
    pub variable: Option<String>,
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub on_create_sets: Vec<SetProperty>,
    pub on_match_sets: Vec<SetProperty>,
    pub post_merge_sets: Vec<SetProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchReturn {
    pub variable: String,
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub expand: Option<RelationshipExpand>,
    pub post_match_expand: Option<PostMatchRelationshipExpand>,
    pub optional_expand: Option<OptionalRelationshipExpand>,
    pub optional_with: Option<OptionalWithAggregate>,
    pub collect_with: Option<WithCollect>,
    pub distinct_with: Option<WithDistinctProjection>,
    pub aggregate_with: Option<WithAggregateProjection>,
    pub aggregate_with_filter: Option<WithAliasFilter>,
    pub post_with_match: Option<PostWithNodeLookup>,
    pub predicate: Option<PropertyPredicate>,
    pub distinct: bool,
    pub returns: Vec<ReturnItem>,
    pub order_by: Vec<OrderItem>,
    pub offset: Option<ValueExpression>,
    pub limit: Option<ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostWithNodeLookup {
    pub variable: String,
    pub label: String,
    pub property: String,
    pub column: String,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortestPathReturn {
    pub path_variable: String,
    pub source_variable: String,
    pub source_label: String,
    pub source_properties: BTreeMap<String, ValueExpression>,
    pub rel_variable: Option<String>,
    pub rel_type: String,
    pub direction: RelationshipDirection,
    pub target_variable: String,
    pub target_label: String,
    pub target_properties: BTreeMap<String, ValueExpression>,
    pub min_hops: usize,
    pub max_hops: usize,
    pub predicate: Option<PropertyPredicate>,
    pub returns: Vec<ShortestPathReturnItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortestPathReturnItem {
    pub expression: ShortestPathReturnExpression,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShortestPathReturnExpression {
    NodePropertyList {
        path_variable: String,
        property: String,
    },
    Length {
        path_variable: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalRelationshipExpand {
    pub source_variable: String,
    pub source_label: String,
    pub expand: RelationshipExpand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostMatchRelationshipExpand {
    pub source_variable: String,
    pub source_label: String,
    pub source_properties: BTreeMap<String, ValueExpression>,
    pub expand: RelationshipExpand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalWithAggregate {
    pub group_variable: String,
    pub count_variable: String,
    pub distinct: bool,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithCollect {
    pub group_variable: String,
    pub collect_variable: String,
    pub collect_property: String,
    pub distinct: bool,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithDistinctProjection {
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithAggregateProjection {
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithAliasFilterOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithAliasFilterExpression {
    Column(String),
    Property { variable: String, property: String },
    Value(ValueExpression),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithAliasFilter {
    And(Vec<WithAliasFilter>),
    Or(Vec<WithAliasFilter>),
    Comparison {
        left: WithAliasFilterExpression,
        op: WithAliasFilterOp,
        right: WithAliasFilterExpression,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchNodesReturn {
    pub left_variable: String,
    pub left_label: String,
    pub left_properties: BTreeMap<String, ValueExpression>,
    pub right_variable: String,
    pub right_label: String,
    pub right_properties: BTreeMap<String, ValueExpression>,
    pub predicate: Option<PropertyPredicate>,
    pub returns: Vec<ReturnItem>,
    pub limit: Option<ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchSet {
    pub variable: String,
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub expand: Option<RelationshipExpand>,
    pub predicate: Option<PropertyPredicate>,
    pub sets: Vec<SetProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchSetReturn {
    pub update: MatchSet,
    pub returns: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchDelete {
    pub variable: String,
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub expand: Option<RelationshipExpand>,
    pub predicate: Option<PropertyPredicate>,
    pub delete_variable: String,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchCreateRelationship {
    pub source_variable: String,
    pub source_label: String,
    pub source_properties: BTreeMap<String, ValueExpression>,
    pub target_variable: String,
    pub target_label: String,
    pub target_properties: BTreeMap<String, ValueExpression>,
    pub predicate: Option<PropertyPredicate>,
    pub create_source_variable: String,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, ValueExpression>,
    pub create_target_variable: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchMergeRelationship {
    pub source_variable: String,
    pub source_label: String,
    pub source_properties: BTreeMap<String, ValueExpression>,
    pub target_variable: String,
    pub target_label: String,
    pub target_properties: BTreeMap<String, ValueExpression>,
    pub predicate: Option<PropertyPredicate>,
    pub merge_source_variable: String,
    pub rel_variable: Option<String>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, ValueExpression>,
    pub merge_target_variable: String,
    pub on_create_sets: Vec<SetProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchExpandMergeRelationship {
    pub source_variable: String,
    pub source_label: String,
    pub source_properties: BTreeMap<String, ValueExpression>,
    pub expand: RelationshipExpand,
    pub predicate: Option<PropertyPredicate>,
    pub merge_source_variable: String,
    pub rel_variable: Option<String>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, ValueExpression>,
    pub merge_target_variable: String,
    pub on_create_sets: Vec<SetProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchExpandMatchMergeRelationship {
    pub source_variable: String,
    pub source_label: String,
    pub source_properties: BTreeMap<String, ValueExpression>,
    pub expand: RelationshipExpand,
    pub matched_target_variable: String,
    pub matched_target_label: String,
    pub matched_target_properties: BTreeMap<String, ValueExpression>,
    pub merge_source_variable: String,
    pub rel_variable: Option<String>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, ValueExpression>,
    pub merge_target_variable: String,
    pub on_create_sets: Vec<SetProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetProperty {
    pub variable: String,
    pub property: String,
    pub value: SetValueExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValueExpression {
    Value(ValueExpression),
    Property {
        variable: String,
        property: String,
    },
    CoalesceProperty {
        variable: String,
        property: String,
        default: ValueExpression,
    },
    PropertyAdd {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    DecrementFloorZero {
        variable: String,
        property: String,
    },
    CoalescePropertyAdd {
        variable: String,
        property: String,
        default: ValueExpression,
        value: ValueExpression,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipExpand {
    pub variable: Option<String>,
    pub rel_type: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub direction: RelationshipDirection,
    pub target_variable: String,
    pub target_label: String,
    pub target_properties: BTreeMap<String, ValueExpression>,
    pub min_hops: usize,
    pub max_hops: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyPredicate {
    And(Vec<PropertyPredicate>),
    Or(Vec<PropertyPredicate>),
    Not(Box<PropertyPredicate>),
    RelationshipExists {
        variable: String,
        rel_type: String,
        direction: RelationshipDirection,
        target_label: String,
    },
    BoundRelationshipExists {
        source_variable: String,
        rel_type: String,
        direction: RelationshipDirection,
        target_variable: String,
    },
    IdEq {
        variable: String,
        value: ValueExpression,
    },
    IdNotEq {
        variable: String,
        value: ValueExpression,
    },
    IdCompare {
        variable: String,
        op: ComparisonOp,
        value: ValueExpression,
    },
    IdIn {
        variable: String,
        values: ValueExpression,
    },
    Eq {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    NotEq {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    Compare {
        variable: String,
        property: String,
        op: ComparisonOp,
        value: ValueExpression,
    },
    ExpressionEq {
        expression: ReturnValueExpression,
        value: ReturnValueExpression,
    },
    ExpressionNotEq {
        expression: ReturnValueExpression,
        value: ReturnValueExpression,
    },
    ExpressionCompare {
        expression: ReturnValueExpression,
        op: ComparisonOp,
        value: ReturnValueExpression,
    },
    ExpressionContains {
        expression: ReturnValueExpression,
        value: ReturnValueExpression,
    },
    ListContains {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    Contains {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    StartsWith {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    EndsWith {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    IsNull {
        variable: String,
        property: String,
    },
    IsNotNull {
        variable: String,
        property: String,
    },
    ParameterIsNull {
        parameter: String,
    },
    ParameterIsNotNull {
        parameter: String,
    },
    In {
        variable: String,
        property: String,
        values: ValueExpression,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueExpression {
    Literal(Value),
    Parameter(String),
    List(Vec<ValueExpression>),
    CurrentTimestamp,
    Timestamp(Box<ValueExpression>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnItem {
    pub expression: ReturnExpression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnValueExpression {
    Variable(String),
    Property {
        variable: String,
        property: String,
    },
    Id(String),
    RelationshipType(String),
    Value(ValueExpression),
    Coalesce(Vec<ReturnValueExpression>),
    Left {
        expression: Box<ReturnValueExpression>,
        length: ValueExpression,
    },
    Lower(Box<ReturnValueExpression>),
    DatePart {
        part: String,
        variable: String,
        property: String,
    },
    DefaultIfNullOrEq {
        variable: String,
        property: String,
        empty: ValueExpression,
        default: ValueExpression,
    },
    DefaultIfNull {
        variable: String,
        property: String,
        default: ValueExpression,
    },
    CasePropertyNotNullOrEq {
        variable: String,
        property: String,
        empty: ValueExpression,
        non_empty: ValueExpression,
        null_or_empty: ValueExpression,
    },
    CaseCoalesceDifferenceFloorZero {
        variable: String,
        terms: Vec<CoalesceDifferenceTerm>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnExpression {
    Variable(String),
    Property {
        variable: String,
        property: String,
    },
    Id(String),
    RelationshipType(String),
    Value(ValueExpression),
    Coalesce(Vec<ReturnValueExpression>),
    Left {
        expression: Box<ReturnValueExpression>,
        length: ValueExpression,
    },
    Lower(Box<ReturnValueExpression>),
    DatePart {
        part: String,
        variable: String,
        property: String,
    },
    DefaultIfNullOrEq {
        variable: String,
        property: String,
        empty: ValueExpression,
        default: ValueExpression,
    },
    DefaultIfNull {
        variable: String,
        property: String,
        default: ValueExpression,
    },
    CasePropertyNotNullOrEq {
        variable: String,
        property: String,
        empty: ValueExpression,
        non_empty: ValueExpression,
        null_or_empty: ValueExpression,
    },
    CaseCoalesceDifferenceFloorZero {
        variable: String,
        terms: Vec<CoalesceDifferenceTerm>,
    },
    CountAll,
    CountVariable {
        variable: String,
        distinct: bool,
    },
    CountProperty {
        variable: String,
        property: String,
        distinct: bool,
    },
    CollectProperty {
        variable: String,
        property: String,
        distinct: bool,
    },
    MinProperty {
        variable: String,
        property: String,
    },
    MaxProperty {
        variable: String,
        property: String,
    },
    AvgProperty {
        variable: String,
        property: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalesceDifferenceTerm {
    pub property: String,
    pub default: ValueExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderItem {
    pub expression: OrderExpression,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderExpression {
    Property { variable: String, property: String },
    Id { variable: String },
    Value(ReturnValueExpression),
    Column(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}
