//! Root storage adapter for executor-owned graph read contracts.

use crate::error::Result;
use crate::store::{GraphScanControl, GraphStore};
use skein_core::{LabelId, RelTypeId};
use skein_executor::store::{GraphExecutionRead, PrunedRelationshipScan, ScanControl};
use skein_storage::{AdjacencyDirection, NodeId, NodeRecord, PropertyFilter, RelRecord};

fn to_store_control(control: ScanControl) -> GraphScanControl {
    match control {
        ScanControl::Continue => GraphScanControl::Continue,
        ScanControl::Stop => GraphScanControl::Stop,
    }
}

fn to_execution_control(control: GraphScanControl) -> ScanControl {
    match control {
        GraphScanControl::Continue => ScanControl::Continue,
        GraphScanControl::Stop => ScanControl::Stop,
    }
}

impl GraphExecutionRead for GraphStore {
    fn is_out_of_core(&self) -> bool {
        GraphStore::is_out_of_core(self)
    }

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        GraphStore::node_owned(self, id)
    }

    fn relationship_count_for_type(&self, rel_type: Option<RelTypeId>) -> usize {
        GraphStore::relationship_count_for_type(self, rel_type)
    }

    fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::try_visit_nodes_owned(self, label_id, |node| {
            consumer(node).map(to_store_control)
        })
        .map(to_execution_control)
    }

    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::try_visit_adjacent_relationships_owned(
            self,
            node_id,
            rel_type,
            direction,
            |relationship| consumer(relationship).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        rel_type: Option<RelTypeId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>> {
        let scan = GraphStore::scan_relationships_with_filter_pruning(self, rel_type, filter);
        Ok(PrunedRelationshipScan {
            relationships: Box::new(scan.relationships.into_iter().cloned()),
            report: scan.report,
        })
    }
}
