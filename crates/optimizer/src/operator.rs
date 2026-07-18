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
pub enum PlanChildren<'a, P> {
    None,
    Unary(&'a P),
    Binary(&'a P, &'a P),
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

impl<'a, P> PlanChildren<'a, P> {
    pub fn len(self) -> usize {
        match self {
            PlanChildren::None => 0,
            PlanChildren::Unary(_) => 1,
            PlanChildren::Binary(_, _) => 2,
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalPlanClass, PhysicalPlanKind, PlanChildren};

    #[test]
    fn physical_plan_kind_exposes_stable_strings_and_classes() {
        assert_eq!(PhysicalPlanKind::IndexNodeSeek.as_str(), "IndexNodeSeek");
        assert_eq!(
            PhysicalPlanKind::IndexNodeSeek.class(),
            PhysicalPlanClass::Access
        );
        assert_eq!(
            PhysicalPlanKind::AdjacencyExpandExec.class(),
            PhysicalPlanClass::Traversal
        );
        assert_eq!(
            PhysicalPlanKind::ProjectExec.class(),
            PhysicalPlanClass::Relational
        );
    }

    #[test]
    fn physical_plan_class_exposes_stable_strings() {
        assert_eq!(PhysicalPlanClass::Schema.as_str(), "schema");
        assert_eq!(PhysicalPlanClass::Mutation.as_str(), "mutation");
        assert_eq!(PhysicalPlanClass::Access.as_str(), "access");
        assert_eq!(PhysicalPlanClass::Traversal.as_str(), "traversal");
        assert_eq!(PhysicalPlanClass::Relational.as_str(), "relational");
        assert_eq!(PhysicalPlanClass::Procedure.as_str(), "procedure");
    }

    #[test]
    fn plan_children_reports_arity_without_knowing_plan_type() {
        assert_eq!(PlanChildren::<&str>::None.len(), 0);
        assert!(PlanChildren::<&str>::None.is_empty());

        let child = "scan";
        assert_eq!(PlanChildren::Unary(&child).len(), 1);

        let left = "left";
        let right = "right";
        assert_eq!(PlanChildren::Binary(&left, &right).len(), 2);
    }
}
