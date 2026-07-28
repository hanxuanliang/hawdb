use super::PhysicalPlan;
use skein_optimizer::{PhysicalPlanClass, PhysicalPlanKind, PhysicalPlanNode, PlanChildren};

pub type PhysicalPlanChildren<'a> = PlanChildren<'a, PhysicalPlan>;

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
                PlanChildren::Binary(left, right)
            }
            PhysicalPlan::NodeColumnLookupExec { input, .. }
            | PhysicalPlan::AdjacencyExpandExec { input, .. }
            | PhysicalPlan::OptionalDegreeExec { input, .. }
            | PhysicalPlan::FilterExec { input, .. }
            | PhysicalPlan::ProjectExec { input, .. }
            | PhysicalPlan::AggregateExec { input, .. }
            | PhysicalPlan::DistinctExec { input }
            | PhysicalPlan::SortExec { input, .. }
            | PhysicalPlan::LimitExec { input, .. } => PlanChildren::Unary(input),
            _ => PlanChildren::None,
        }
    }
}

impl PhysicalPlanNode for PhysicalPlan {
    fn kind(&self) -> PhysicalPlanKind {
        PhysicalPlan::kind(self)
    }

    fn children(&self) -> PhysicalPlanChildren<'_> {
        PhysicalPlan::children(self)
    }
}
