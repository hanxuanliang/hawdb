use crate::{NodeId, RelId};
use skein_core::RelTypeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdjacencyDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AdjacencyLayout {
    Sparse,
    Dense,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedAdjacencyEntry {
    pub relationship_id: RelId,
    pub neighbor_id: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjacencyGroupStats {
    pub node_id: NodeId,
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
    pub degree: usize,
    pub layout: AdjacencyLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdjacencyGroupKey {
    pub node_id: NodeId,
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacencyGroupConsistencyMismatch {
    pub key: AdjacencyGroupKey,
    pub maintained_relationship_ids: Vec<RelId>,
    pub recomputed_relationship_ids: Vec<RelId>,
}
