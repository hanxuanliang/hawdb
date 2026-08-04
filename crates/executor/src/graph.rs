use crate::binding::{binding_payload_bytes, Binding};
use skein_plan::GraphExpansionBudget;
use skein_storage::NodeId;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphExpansionTruncationReason {
    CandidateLimit,
    PayloadByteLimit,
}

impl GraphExpansionTruncationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CandidateLimit => "candidate_limit",
            Self::PayloadByteLimit => "payload_byte_limit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphExpansionExecutionReport {
    pub seed_count: usize,
    pub expanded_node_count: usize,
    pub expanded_edge_count: usize,
    pub relation_types: Vec<String>,
    pub min_hops: usize,
    pub max_hops: usize,
    pub reranked_seed_count: usize,
    pub candidate_limit: usize,
    pub payload_byte_limit: usize,
    pub payload_bytes_used: usize,
    pub returned_count: usize,
    pub truncation_reason: Option<GraphExpansionTruncationReason>,
}

impl GraphExpansionExecutionReport {
    pub fn truncated(&self) -> bool {
        self.truncation_reason.is_some()
    }
}

#[doc(hidden)]
pub struct GraphExpansionExecutionState {
    budget: Option<GraphExpansionBudget>,
    seed_count: usize,
    expanded_nodes: BTreeSet<NodeId>,
    expanded_edge_count: usize,
    reranked_seed_count: usize,
    payload_bytes_used: usize,
    returned_count: usize,
    pub truncation_reason: Option<GraphExpansionTruncationReason>,
}

impl GraphExpansionExecutionState {
    pub fn new(
        budget: Option<GraphExpansionBudget>,
        seed_count: usize,
        reranked_seed_count: usize,
    ) -> Self {
        Self {
            budget,
            seed_count,
            expanded_nodes: BTreeSet::new(),
            expanded_edge_count: 0,
            reranked_seed_count,
            payload_bytes_used: 0,
            returned_count: 0,
            truncation_reason: None,
        }
    }

    pub fn try_push(
        &mut self,
        output: &mut Vec<Binding>,
        candidate: Binding,
        target_id: Option<NodeId>,
        hop: usize,
    ) -> bool {
        let Some(budget) = self.budget else {
            self.returned_count = self.returned_count.saturating_add(1);
            output.push(candidate);
            return true;
        };
        if self.returned_count >= budget.candidate_limit {
            self.truncation_reason = Some(GraphExpansionTruncationReason::CandidateLimit);
            return false;
        }
        let candidate_bytes = binding_payload_bytes(&candidate);
        if self.payload_bytes_used.saturating_add(candidate_bytes) > budget.payload_byte_limit {
            self.truncation_reason = Some(GraphExpansionTruncationReason::PayloadByteLimit);
            return false;
        }
        self.payload_bytes_used = self.payload_bytes_used.saturating_add(candidate_bytes);
        self.returned_count = self.returned_count.saturating_add(1);
        if let Some(target_id) = target_id {
            self.expanded_nodes.insert(target_id);
        }
        self.expanded_edge_count = self.expanded_edge_count.saturating_add(hop);
        output.push(candidate);
        true
    }

    pub fn record_seed(&mut self) {
        self.seed_count = self.seed_count.saturating_add(1);
    }

    pub fn returned_count(&self) -> usize {
        self.returned_count
    }

    pub fn report(
        &self,
        rel_type: &str,
        min_hops: usize,
        max_hops: usize,
        returned_count: usize,
    ) -> Option<GraphExpansionExecutionReport> {
        let budget = self.budget?;
        Some(GraphExpansionExecutionReport {
            seed_count: self.seed_count,
            expanded_node_count: self.expanded_nodes.len(),
            expanded_edge_count: self.expanded_edge_count,
            relation_types: if rel_type.is_empty() {
                Vec::new()
            } else {
                vec![rel_type.to_string()]
            },
            min_hops,
            max_hops,
            reranked_seed_count: self.reranked_seed_count,
            candidate_limit: budget.candidate_limit,
            payload_byte_limit: budget.payload_byte_limit,
            payload_bytes_used: self.payload_bytes_used,
            returned_count,
            truncation_reason: self.truncation_reason,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_reason_codes_are_stable() {
        assert_eq!(
            GraphExpansionTruncationReason::CandidateLimit.as_str(),
            "candidate_limit"
        );
        assert_eq!(
            GraphExpansionTruncationReason::PayloadByteLimit.as_str(),
            "payload_byte_limit"
        );
    }

    #[test]
    fn expansion_state_enforces_candidate_and_payload_budgets_before_push() {
        let binding = Binding {
            values: std::collections::BTreeMap::from([(
                "value".to_string(),
                skein_core::Value::String("payload".to_string()),
            )]),
            nodes: std::collections::BTreeMap::new(),
            relationships: std::collections::BTreeMap::new(),
        };
        let mut state = GraphExpansionExecutionState::new(
            Some(GraphExpansionBudget {
                candidate_limit: 1,
                payload_byte_limit: usize::MAX,
            }),
            1,
            1,
        );
        let mut output = Vec::new();

        assert!(state.try_push(&mut output, binding.clone(), None, 1));
        assert!(!state.try_push(&mut output, binding, None, 1));
        assert_eq!(
            state.truncation_reason,
            Some(GraphExpansionTruncationReason::CandidateLimit)
        );

        let binding = Binding {
            values: std::collections::BTreeMap::from([(
                "value".to_string(),
                skein_core::Value::String("payload".to_string()),
            )]),
            nodes: std::collections::BTreeMap::new(),
            relationships: std::collections::BTreeMap::new(),
        };
        let mut state = GraphExpansionExecutionState::new(
            Some(GraphExpansionBudget {
                candidate_limit: 2,
                payload_byte_limit: binding_payload_bytes(&binding).saturating_sub(1),
            }),
            1,
            1,
        );
        let mut output = Vec::new();

        assert!(!state.try_push(&mut output, binding, None, 1));
        assert!(output.is_empty());
        assert_eq!(
            state.truncation_reason,
            Some(GraphExpansionTruncationReason::PayloadByteLimit)
        );
    }
}
