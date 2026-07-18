use super::PhysicalPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PhysicalPlanKind {
    CreateNodeLabel,
    CreateRelationshipType,
    CreateNodeTable,
    CreateRelationshipTable,
    CreateProperty,
    AlterTableState,
    AlterPropertyState,
    CreateIndex,
    CreateCompositeIndex,
    CreateRangeIndex,
    CreateFullTextIndex,
    CreateUniqueConstraint,
    CreateNodePropertyExistsConstraint,
    CreateRelationshipUniqueConstraint,
    CreateRelationshipPropertyExistsConstraint,
    ProjectGraph,
    GraphAlgorithm,
    CreateNode,
    MergeNode,
    MergeRelationship,
    MergeMatchedRelationship,
    MergeRelationshipFromMatchedRelationship,
    MergeRelationshipToMatchedTarget,
    MergeRelationshipFromMatchedTarget,
    CreateMatchedRelationship,
    SetNodeProperty,
    SetNodeProperties,
    SetNodePropertiesReturn,
    SetRelationshipProperty,
    SetRelationshipProperties,
    DeleteNode,
    DeleteRelationship,
    DeleteRelationshipTargetNodes,
    CreateRelationship,
    SeqNodeScan,
    NodeCartesianProductExec,
    NodeColumnLookupExec,
    IndexNodeSeek,
    IndexNodeMultiSeek,
    IndexNodeCompositeSeek,
    IndexNodeRangeSeek,
    IndexNodeTextSeek,
    AdjacencyExpandExec,
    OptionalDegreeExec,
    OptionalRelationshipCountSumExec,
    ThreadRepairStatsExec,
    ShortestPathExec,
    FilterExec,
    ProjectExec,
    AggregateExec,
    DistinctExec,
    SortExec,
    LimitExec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PhysicalPlanClass {
    Schema,
    Mutation,
    Access,
    Traversal,
    Relational,
    Procedure,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PhysicalPlanChildren<'a> {
    None,
    Unary(&'a PhysicalPlan),
    Binary(&'a PhysicalPlan, &'a PhysicalPlan),
}

impl PhysicalPlanKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PhysicalPlanKind::CreateNodeLabel => "CreateNodeLabel",
            PhysicalPlanKind::CreateRelationshipType => "CreateRelationshipType",
            PhysicalPlanKind::CreateNodeTable => "CreateNodeTable",
            PhysicalPlanKind::CreateRelationshipTable => "CreateRelationshipTable",
            PhysicalPlanKind::CreateProperty => "CreateProperty",
            PhysicalPlanKind::AlterTableState => "AlterTableState",
            PhysicalPlanKind::AlterPropertyState => "AlterPropertyState",
            PhysicalPlanKind::CreateIndex => "CreateIndex",
            PhysicalPlanKind::CreateCompositeIndex => "CreateCompositeIndex",
            PhysicalPlanKind::CreateRangeIndex => "CreateRangeIndex",
            PhysicalPlanKind::CreateFullTextIndex => "CreateFullTextIndex",
            PhysicalPlanKind::CreateUniqueConstraint => "CreateUniqueConstraint",
            PhysicalPlanKind::CreateNodePropertyExistsConstraint => {
                "CreateNodePropertyExistsConstraint"
            }
            PhysicalPlanKind::CreateRelationshipUniqueConstraint => {
                "CreateRelationshipUniqueConstraint"
            }
            PhysicalPlanKind::CreateRelationshipPropertyExistsConstraint => {
                "CreateRelationshipPropertyExistsConstraint"
            }
            PhysicalPlanKind::ProjectGraph => "ProjectGraph",
            PhysicalPlanKind::GraphAlgorithm => "GraphAlgorithm",
            PhysicalPlanKind::CreateNode => "CreateNode",
            PhysicalPlanKind::MergeNode => "MergeNode",
            PhysicalPlanKind::MergeRelationship => "MergeRelationship",
            PhysicalPlanKind::MergeMatchedRelationship => "MergeMatchedRelationship",
            PhysicalPlanKind::MergeRelationshipFromMatchedRelationship => {
                "MergeRelationshipFromMatchedRelationship"
            }
            PhysicalPlanKind::MergeRelationshipToMatchedTarget => {
                "MergeRelationshipToMatchedTarget"
            }
            PhysicalPlanKind::MergeRelationshipFromMatchedTarget => {
                "MergeRelationshipFromMatchedTarget"
            }
            PhysicalPlanKind::CreateMatchedRelationship => "CreateMatchedRelationship",
            PhysicalPlanKind::SetNodeProperty => "SetNodeProperty",
            PhysicalPlanKind::SetNodeProperties => "SetNodeProperties",
            PhysicalPlanKind::SetNodePropertiesReturn => "SetNodePropertiesReturn",
            PhysicalPlanKind::SetRelationshipProperty => "SetRelationshipProperty",
            PhysicalPlanKind::SetRelationshipProperties => "SetRelationshipProperties",
            PhysicalPlanKind::DeleteNode => "DeleteNode",
            PhysicalPlanKind::DeleteRelationship => "DeleteRelationship",
            PhysicalPlanKind::DeleteRelationshipTargetNodes => "DeleteRelationshipTargetNodes",
            PhysicalPlanKind::CreateRelationship => "CreateRelationship",
            PhysicalPlanKind::SeqNodeScan => "SeqNodeScan",
            PhysicalPlanKind::NodeCartesianProductExec => "NodeCartesianProductExec",
            PhysicalPlanKind::NodeColumnLookupExec => "NodeColumnLookupExec",
            PhysicalPlanKind::IndexNodeSeek => "IndexNodeSeek",
            PhysicalPlanKind::IndexNodeMultiSeek => "IndexNodeMultiSeek",
            PhysicalPlanKind::IndexNodeCompositeSeek => "IndexNodeCompositeSeek",
            PhysicalPlanKind::IndexNodeRangeSeek => "IndexNodeRangeSeek",
            PhysicalPlanKind::IndexNodeTextSeek => "IndexNodeTextSeek",
            PhysicalPlanKind::AdjacencyExpandExec => "AdjacencyExpandExec",
            PhysicalPlanKind::OptionalDegreeExec => "OptionalDegreeExec",
            PhysicalPlanKind::OptionalRelationshipCountSumExec => {
                "OptionalRelationshipCountSumExec"
            }
            PhysicalPlanKind::ThreadRepairStatsExec => "ThreadRepairStatsExec",
            PhysicalPlanKind::ShortestPathExec => "ShortestPathExec",
            PhysicalPlanKind::FilterExec => "FilterExec",
            PhysicalPlanKind::ProjectExec => "ProjectExec",
            PhysicalPlanKind::AggregateExec => "AggregateExec",
            PhysicalPlanKind::DistinctExec => "DistinctExec",
            PhysicalPlanKind::SortExec => "SortExec",
            PhysicalPlanKind::LimitExec => "LimitExec",
        }
    }

    pub fn class(self) -> PhysicalPlanClass {
        match self {
            PhysicalPlanKind::CreateNodeLabel
            | PhysicalPlanKind::CreateRelationshipType
            | PhysicalPlanKind::CreateNodeTable
            | PhysicalPlanKind::CreateRelationshipTable
            | PhysicalPlanKind::CreateProperty
            | PhysicalPlanKind::AlterTableState
            | PhysicalPlanKind::AlterPropertyState
            | PhysicalPlanKind::CreateIndex
            | PhysicalPlanKind::CreateCompositeIndex
            | PhysicalPlanKind::CreateRangeIndex
            | PhysicalPlanKind::CreateFullTextIndex
            | PhysicalPlanKind::CreateUniqueConstraint
            | PhysicalPlanKind::CreateNodePropertyExistsConstraint
            | PhysicalPlanKind::CreateRelationshipUniqueConstraint
            | PhysicalPlanKind::CreateRelationshipPropertyExistsConstraint => {
                PhysicalPlanClass::Schema
            }
            PhysicalPlanKind::CreateNode
            | PhysicalPlanKind::MergeNode
            | PhysicalPlanKind::MergeRelationship
            | PhysicalPlanKind::MergeMatchedRelationship
            | PhysicalPlanKind::MergeRelationshipFromMatchedRelationship
            | PhysicalPlanKind::MergeRelationshipToMatchedTarget
            | PhysicalPlanKind::MergeRelationshipFromMatchedTarget
            | PhysicalPlanKind::CreateMatchedRelationship
            | PhysicalPlanKind::SetNodeProperty
            | PhysicalPlanKind::SetNodeProperties
            | PhysicalPlanKind::SetNodePropertiesReturn
            | PhysicalPlanKind::SetRelationshipProperty
            | PhysicalPlanKind::SetRelationshipProperties
            | PhysicalPlanKind::DeleteNode
            | PhysicalPlanKind::DeleteRelationship
            | PhysicalPlanKind::DeleteRelationshipTargetNodes
            | PhysicalPlanKind::CreateRelationship => PhysicalPlanClass::Mutation,
            PhysicalPlanKind::SeqNodeScan
            | PhysicalPlanKind::NodeColumnLookupExec
            | PhysicalPlanKind::IndexNodeSeek
            | PhysicalPlanKind::IndexNodeMultiSeek
            | PhysicalPlanKind::IndexNodeCompositeSeek
            | PhysicalPlanKind::IndexNodeRangeSeek
            | PhysicalPlanKind::IndexNodeTextSeek => PhysicalPlanClass::Access,
            PhysicalPlanKind::AdjacencyExpandExec
            | PhysicalPlanKind::OptionalDegreeExec
            | PhysicalPlanKind::OptionalRelationshipCountSumExec
            | PhysicalPlanKind::ShortestPathExec => PhysicalPlanClass::Traversal,
            PhysicalPlanKind::NodeCartesianProductExec
            | PhysicalPlanKind::FilterExec
            | PhysicalPlanKind::ProjectExec
            | PhysicalPlanKind::AggregateExec
            | PhysicalPlanKind::DistinctExec
            | PhysicalPlanKind::SortExec
            | PhysicalPlanKind::LimitExec => PhysicalPlanClass::Relational,
            PhysicalPlanKind::ProjectGraph
            | PhysicalPlanKind::GraphAlgorithm
            | PhysicalPlanKind::ThreadRepairStatsExec => PhysicalPlanClass::Procedure,
        }
    }
}

impl PhysicalPlanClass {
    pub fn as_str(self) -> &'static str {
        match self {
            PhysicalPlanClass::Schema => "schema",
            PhysicalPlanClass::Mutation => "mutation",
            PhysicalPlanClass::Access => "access",
            PhysicalPlanClass::Traversal => "traversal",
            PhysicalPlanClass::Relational => "relational",
            PhysicalPlanClass::Procedure => "procedure",
        }
    }
}

impl<'a> PhysicalPlanChildren<'a> {
    pub fn len(self) -> usize {
        match self {
            PhysicalPlanChildren::None => 0,
            PhysicalPlanChildren::Unary(_) => 1,
            PhysicalPlanChildren::Binary(_, _) => 2,
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

impl PhysicalPlan {
    pub fn kind(&self) -> PhysicalPlanKind {
        match self {
            PhysicalPlan::CreateNodeLabel { .. } => PhysicalPlanKind::CreateNodeLabel,
            PhysicalPlan::CreateRelationshipType { .. } => PhysicalPlanKind::CreateRelationshipType,
            PhysicalPlan::CreateNodeTable { .. } => PhysicalPlanKind::CreateNodeTable,
            PhysicalPlan::CreateRelationshipTable { .. } => {
                PhysicalPlanKind::CreateRelationshipTable
            }
            PhysicalPlan::CreateProperty { .. } => PhysicalPlanKind::CreateProperty,
            PhysicalPlan::AlterTableState { .. } => PhysicalPlanKind::AlterTableState,
            PhysicalPlan::AlterPropertyState { .. } => PhysicalPlanKind::AlterPropertyState,
            PhysicalPlan::CreateIndex { .. } => PhysicalPlanKind::CreateIndex,
            PhysicalPlan::CreateCompositeIndex { .. } => PhysicalPlanKind::CreateCompositeIndex,
            PhysicalPlan::CreateRangeIndex { .. } => PhysicalPlanKind::CreateRangeIndex,
            PhysicalPlan::CreateFullTextIndex { .. } => PhysicalPlanKind::CreateFullTextIndex,
            PhysicalPlan::CreateUniqueConstraint { .. } => PhysicalPlanKind::CreateUniqueConstraint,
            PhysicalPlan::CreateNodePropertyExistsConstraint { .. } => {
                PhysicalPlanKind::CreateNodePropertyExistsConstraint
            }
            PhysicalPlan::CreateRelationshipUniqueConstraint { .. } => {
                PhysicalPlanKind::CreateRelationshipUniqueConstraint
            }
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. } => {
                PhysicalPlanKind::CreateRelationshipPropertyExistsConstraint
            }
            PhysicalPlan::ProjectGraph { .. } => PhysicalPlanKind::ProjectGraph,
            PhysicalPlan::GraphAlgorithm { .. } => PhysicalPlanKind::GraphAlgorithm,
            PhysicalPlan::CreateNode { .. } => PhysicalPlanKind::CreateNode,
            PhysicalPlan::MergeNode { .. } => PhysicalPlanKind::MergeNode,
            PhysicalPlan::MergeRelationship { .. } => PhysicalPlanKind::MergeRelationship,
            PhysicalPlan::MergeMatchedRelationship { .. } => {
                PhysicalPlanKind::MergeMatchedRelationship
            }
            PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. } => {
                PhysicalPlanKind::MergeRelationshipFromMatchedRelationship
            }
            PhysicalPlan::MergeRelationshipToMatchedTarget { .. } => {
                PhysicalPlanKind::MergeRelationshipToMatchedTarget
            }
            PhysicalPlan::MergeRelationshipFromMatchedTarget { .. } => {
                PhysicalPlanKind::MergeRelationshipFromMatchedTarget
            }
            PhysicalPlan::CreateMatchedRelationship { .. } => {
                PhysicalPlanKind::CreateMatchedRelationship
            }
            PhysicalPlan::SetNodeProperty { .. } => PhysicalPlanKind::SetNodeProperty,
            PhysicalPlan::SetNodeProperties { .. } => PhysicalPlanKind::SetNodeProperties,
            PhysicalPlan::SetNodePropertiesReturn { .. } => {
                PhysicalPlanKind::SetNodePropertiesReturn
            }
            PhysicalPlan::SetRelationshipProperty { .. } => {
                PhysicalPlanKind::SetRelationshipProperty
            }
            PhysicalPlan::SetRelationshipProperties { .. } => {
                PhysicalPlanKind::SetRelationshipProperties
            }
            PhysicalPlan::DeleteNode { .. } => PhysicalPlanKind::DeleteNode,
            PhysicalPlan::DeleteRelationship { .. } => PhysicalPlanKind::DeleteRelationship,
            PhysicalPlan::DeleteRelationshipTargetNodes { .. } => {
                PhysicalPlanKind::DeleteRelationshipTargetNodes
            }
            PhysicalPlan::CreateRelationship { .. } => PhysicalPlanKind::CreateRelationship,
            PhysicalPlan::SeqNodeScan { .. } => PhysicalPlanKind::SeqNodeScan,
            PhysicalPlan::NodeCartesianProductExec { .. } => {
                PhysicalPlanKind::NodeCartesianProductExec
            }
            PhysicalPlan::NodeColumnLookupExec { .. } => PhysicalPlanKind::NodeColumnLookupExec,
            PhysicalPlan::IndexNodeSeek { .. } => PhysicalPlanKind::IndexNodeSeek,
            PhysicalPlan::IndexNodeMultiSeek { .. } => PhysicalPlanKind::IndexNodeMultiSeek,
            PhysicalPlan::IndexNodeCompositeSeek { .. } => PhysicalPlanKind::IndexNodeCompositeSeek,
            PhysicalPlan::IndexNodeRangeSeek { .. } => PhysicalPlanKind::IndexNodeRangeSeek,
            PhysicalPlan::IndexNodeTextSeek { .. } => PhysicalPlanKind::IndexNodeTextSeek,
            PhysicalPlan::AdjacencyExpandExec { .. } => PhysicalPlanKind::AdjacencyExpandExec,
            PhysicalPlan::OptionalDegreeExec { .. } => PhysicalPlanKind::OptionalDegreeExec,
            PhysicalPlan::OptionalRelationshipCountSumExec { .. } => {
                PhysicalPlanKind::OptionalRelationshipCountSumExec
            }
            PhysicalPlan::ThreadRepairStatsExec { .. } => PhysicalPlanKind::ThreadRepairStatsExec,
            PhysicalPlan::ShortestPathExec { .. } => PhysicalPlanKind::ShortestPathExec,
            PhysicalPlan::FilterExec { .. } => PhysicalPlanKind::FilterExec,
            PhysicalPlan::ProjectExec { .. } => PhysicalPlanKind::ProjectExec,
            PhysicalPlan::AggregateExec { .. } => PhysicalPlanKind::AggregateExec,
            PhysicalPlan::DistinctExec { .. } => PhysicalPlanKind::DistinctExec,
            PhysicalPlan::SortExec { .. } => PhysicalPlanKind::SortExec,
            PhysicalPlan::LimitExec { .. } => PhysicalPlanKind::LimitExec,
        }
    }

    pub fn class(&self) -> PhysicalPlanClass {
        self.kind().class()
    }

    pub fn children(&self) -> PhysicalPlanChildren<'_> {
        match self {
            PhysicalPlan::NodeCartesianProductExec { left, right } => {
                PhysicalPlanChildren::Binary(left, right)
            }
            PhysicalPlan::NodeColumnLookupExec { input, .. }
            | PhysicalPlan::AdjacencyExpandExec { input, .. }
            | PhysicalPlan::OptionalDegreeExec { input, .. }
            | PhysicalPlan::FilterExec { input, .. }
            | PhysicalPlan::ProjectExec { input, .. }
            | PhysicalPlan::AggregateExec { input, .. }
            | PhysicalPlan::DistinctExec { input }
            | PhysicalPlan::SortExec { input, .. }
            | PhysicalPlan::LimitExec { input, .. } => PhysicalPlanChildren::Unary(input),
            _ => PhysicalPlanChildren::None,
        }
    }
}
