use super::PruningDecision;
use roaring::RoaringTreemap;

#[derive(Debug, Clone)]
pub struct CandidateCursor {
    candidates: RoaringTreemap,
    consumed: u64,
}

impl CandidateCursor {
    pub fn new(candidates: RoaringTreemap) -> Self {
        Self {
            candidates,
            consumed: 0,
        }
    }

    pub fn from_decision(decision: PruningDecision) -> Option<Self> {
        match decision {
            PruningDecision::Candidates { row_ids, .. } => Some(Self::new(row_ids)),
            PruningDecision::Skip { .. } | PruningDecision::Read { .. } => None,
        }
    }

    pub fn remaining(&self) -> u64 {
        self.candidates.len().saturating_sub(self.consumed)
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn next_batch(&mut self, limit: usize) -> Vec<u64> {
        if limit == 0 || self.is_empty() {
            return Vec::new();
        }
        let batch = self
            .candidates
            .iter()
            .skip(self.consumed as usize)
            .take(limit)
            .collect::<Vec<_>>();
        self.consumed = self.consumed.saturating_add(batch.len() as u64);
        batch
    }
}
