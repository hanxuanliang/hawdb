use crate::{NodeId, RelId};
use skein_core::RelTypeId;
use std::collections::{btree_set, BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

pub const ADJACENCY_PIVOT_MIN_DEGREE: usize = 64;
pub const ADJACENCY_MINI_DELTA_MAX_ENTRIES: usize = 64;

/// A snapshot-friendly adjacency posting list with an immutable pivot and a
/// bounded mini-delta for high-degree mutations.
///
/// Unshared pivots and small shared pivots retain the direct `BTreeSet` update
/// path. A shared high-degree pivot records overrides in a mini-delta, avoiding
/// a full posting-list clone for every mutation while an older snapshot is
/// alive. Consolidation materializes one replacement pivot after a bounded
/// number of overrides. The posting handle remains pointer-sized, and a delta
/// version materializes its immutable read view at most once.
#[derive(Debug, Clone)]
pub struct AdjacencyPostingList {
    state: Arc<AdjacencyPostingState>,
}

#[derive(Debug)]
enum AdjacencyPostingState {
    Pivot(BTreeSet<RelId>),
    Delta {
        pivot: Arc<AdjacencyPostingState>,
        overrides: BTreeMap<RelId, bool>,
        read_view: OnceLock<Arc<BTreeSet<RelId>>>,
    },
}

impl Clone for AdjacencyPostingState {
    fn clone(&self) -> Self {
        match self {
            Self::Pivot(pivot) => Self::Pivot(pivot.clone()),
            Self::Delta {
                pivot, overrides, ..
            } => Self::Delta {
                pivot: Arc::clone(pivot),
                overrides: overrides.clone(),
                read_view: OnceLock::new(),
            },
        }
    }
}

impl Default for AdjacencyPostingList {
    fn default() -> Self {
        Self {
            state: Arc::new(AdjacencyPostingState::Pivot(BTreeSet::new())),
        }
    }
}

impl From<BTreeSet<RelId>> for AdjacencyPostingList {
    fn from(pivot: BTreeSet<RelId>) -> Self {
        Self {
            state: Arc::new(AdjacencyPostingState::Pivot(pivot)),
        }
    }
}

impl PartialEq for AdjacencyPostingList {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for AdjacencyPostingList {}

impl AdjacencyPostingList {
    #[inline]
    pub fn len(&self) -> usize {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot.len(),
            AdjacencyPostingState::Delta {
                pivot, overrides, ..
            } => overrides
                .iter()
                .fold(pivot_set(pivot).len(), |len, (id, present)| {
                    match (pivot_set(pivot).contains(id), present) {
                        (false, true) => len.saturating_add(1),
                        (true, false) => len.saturating_sub(1),
                        _ => len,
                    }
                }),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn contains(&self, id: &RelId) -> bool {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot.contains(id),
            AdjacencyPostingState::Delta {
                pivot, overrides, ..
            } => overrides
                .get(id)
                .copied()
                .unwrap_or_else(|| pivot_set(pivot).contains(id)),
        }
    }

    pub fn insert(&mut self, id: RelId) -> bool {
        if self.contains(&id) {
            return false;
        }
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot)
                if Arc::strong_count(&self.state) > 1
                    && pivot.len() >= ADJACENCY_PIVOT_MIN_DEGREE =>
            {
                self.state = Arc::new(AdjacencyPostingState::Delta {
                    pivot: Arc::clone(&self.state),
                    overrides: BTreeMap::from([(id, true)]),
                    read_view: OnceLock::new(),
                });
            }
            AdjacencyPostingState::Pivot(_) => {
                let AdjacencyPostingState::Pivot(pivot) = Arc::make_mut(&mut self.state) else {
                    unreachable!("matched pivot state");
                };
                pivot.insert(id);
            }
            AdjacencyPostingState::Delta { .. } => {
                let AdjacencyPostingState::Delta {
                    pivot,
                    overrides,
                    read_view,
                } = Arc::make_mut(&mut self.state)
                else {
                    unreachable!("matched delta state");
                };
                read_view.take();
                if pivot_set(pivot).contains(&id) {
                    overrides.remove(&id);
                } else {
                    overrides.insert(id, true);
                }
            }
        }
        self.finish_mutation();
        true
    }

    pub fn remove(&mut self, id: &RelId) -> bool {
        if !self.contains(id) {
            return false;
        }
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot)
                if Arc::strong_count(&self.state) > 1
                    && pivot.len() >= ADJACENCY_PIVOT_MIN_DEGREE =>
            {
                self.state = Arc::new(AdjacencyPostingState::Delta {
                    pivot: Arc::clone(&self.state),
                    overrides: BTreeMap::from([(*id, false)]),
                    read_view: OnceLock::new(),
                });
            }
            AdjacencyPostingState::Pivot(_) => {
                let AdjacencyPostingState::Pivot(pivot) = Arc::make_mut(&mut self.state) else {
                    unreachable!("matched pivot state");
                };
                pivot.remove(id);
            }
            AdjacencyPostingState::Delta { .. } => {
                let AdjacencyPostingState::Delta {
                    pivot,
                    overrides,
                    read_view,
                } = Arc::make_mut(&mut self.state)
                else {
                    unreachable!("matched delta state");
                };
                read_view.take();
                if pivot_set(pivot).contains(id) {
                    overrides.insert(*id, false);
                } else {
                    overrides.remove(id);
                }
            }
        }
        self.finish_mutation();
        true
    }

    #[inline]
    pub fn iter(&self) -> btree_set::Iter<'_, RelId> {
        self.read_view().iter()
    }

    pub fn pivot_len(&self) -> usize {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot.len(),
            AdjacencyPostingState::Delta { pivot, .. } => pivot_set(pivot).len(),
        }
    }

    pub fn mini_delta_len(&self) -> usize {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(_) => 0,
            AdjacencyPostingState::Delta { overrides, .. } => overrides.len(),
        }
    }

    pub fn shares_pivot_with(&self, other: &Self) -> bool {
        std::ptr::eq(self.pivot_identity(), other.pivot_identity())
    }

    fn finish_mutation(&mut self) {
        let next_state = match self.state.as_ref() {
            AdjacencyPostingState::Pivot(_) => None,
            AdjacencyPostingState::Delta {
                pivot, overrides, ..
            } if overrides.is_empty() => Some(Arc::clone(pivot)),
            AdjacencyPostingState::Delta { overrides, .. }
                if overrides.len() >= ADJACENCY_MINI_DELTA_MAX_ENTRIES =>
            {
                Some(Arc::new(AdjacencyPostingState::Pivot(self.materialize())))
            }
            AdjacencyPostingState::Delta { .. } => None,
        };
        if let Some(next_state) = next_state {
            self.state = next_state;
        }
    }

    fn materialize(&self) -> BTreeSet<RelId> {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot.clone(),
            AdjacencyPostingState::Delta {
                pivot, overrides, ..
            } => materialize_delta(pivot_set(pivot), overrides),
        }
    }

    #[inline]
    fn read_view(&self) -> &BTreeSet<RelId> {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(pivot) => pivot,
            AdjacencyPostingState::Delta {
                pivot,
                overrides,
                read_view,
            } => read_view
                .get_or_init(|| Arc::new(materialize_delta(pivot_set(pivot), overrides)))
                .as_ref(),
        }
    }

    fn pivot_identity(&self) -> *const AdjacencyPostingState {
        match self.state.as_ref() {
            AdjacencyPostingState::Pivot(_) => Arc::as_ptr(&self.state),
            AdjacencyPostingState::Delta { pivot, .. } => Arc::as_ptr(pivot),
        }
    }
}

fn pivot_set(state: &AdjacencyPostingState) -> &BTreeSet<RelId> {
    let AdjacencyPostingState::Pivot(pivot) = state else {
        unreachable!("mini-delta pivots are always consolidated states");
    };
    pivot
}

fn materialize_delta(
    pivot: &BTreeSet<RelId>,
    overrides: &BTreeMap<RelId, bool>,
) -> BTreeSet<RelId> {
    let mut read_view = pivot.clone();
    for (id, present) in overrides {
        if *present {
            read_view.insert(*id);
        } else {
            read_view.remove(id);
        }
    }
    read_view
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posting_handle_stays_pointer_sized() {
        assert_eq!(
            std::mem::size_of::<AdjacencyPostingList>(),
            std::mem::size_of::<Arc<()>>()
        );
    }

    #[test]
    fn small_shared_posting_detaches_its_pivot_directly() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..8));
        let snapshot = posting.clone();

        assert!(posting.insert(RelId(8)));

        assert!(!posting.shares_pivot_with(&snapshot));
        assert_eq!(posting.mini_delta_len(), 0);
        assert_eq!(
            posting.iter().copied().collect::<Vec<_>>(),
            rel_ids_vec(0..9)
        );
        assert_eq!(
            snapshot.iter().copied().collect::<Vec<_>>(),
            rel_ids_vec(0..8)
        );
    }

    #[test]
    fn shared_dense_posting_buffers_changes_without_detaching_pivot() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..128));
        let snapshot = posting.clone();

        assert!(posting.remove(&RelId(2)));
        assert!(posting.insert(RelId(256)));

        assert!(posting.shares_pivot_with(&snapshot));
        assert_eq!(posting.mini_delta_len(), 2);
        assert!(!posting.contains(&RelId(2)));
        assert!(posting.contains(&RelId(256)));
        assert!(snapshot.contains(&RelId(2)));
        assert!(!snapshot.contains(&RelId(256)));
    }

    #[test]
    fn bounded_delta_consolidates_into_a_replacement_pivot() {
        let mut posting = AdjacencyPostingList::from(rel_ids(0..128));
        let snapshot = posting.clone();
        for id in 0..ADJACENCY_MINI_DELTA_MAX_ENTRIES {
            assert!(posting.insert(RelId(1_000 + id as u64)));
        }

        assert!(!posting.shares_pivot_with(&snapshot));
        assert_eq!(posting.mini_delta_len(), 0);
        assert_eq!(posting.pivot_len(), 128 + ADJACENCY_MINI_DELTA_MAX_ENTRIES);
        assert_eq!(snapshot.len(), 128);
    }

    #[test]
    fn mutation_sequence_matches_btree_set_reference_and_snapshots() {
        let mut posting = AdjacencyPostingList::default();
        let mut reference = BTreeSet::new();
        let mut snapshots = Vec::new();
        for step in 0..512_u64 {
            let id = RelId((step.wrapping_mul(73).wrapping_add(19)) % 181);
            if step % 3 == 0 {
                assert_eq!(posting.remove(&id), reference.remove(&id));
            } else {
                assert_eq!(posting.insert(id), reference.insert(id));
            }
            if step % 37 == 0 {
                snapshots.push((posting.clone(), reference.clone()));
            }
            assert_eq!(posting.len(), reference.len());
            assert_eq!(posting.iter().copied().collect::<BTreeSet<_>>(), reference);
        }

        for (snapshot, expected) in snapshots {
            assert_eq!(snapshot.iter().copied().collect::<BTreeSet<_>>(), expected);
        }
    }

    fn rel_ids(range: std::ops::Range<u64>) -> BTreeSet<RelId> {
        range.map(RelId).collect()
    }

    fn rel_ids_vec(range: std::ops::Range<u64>) -> Vec<RelId> {
        range.map(RelId).collect()
    }
}
