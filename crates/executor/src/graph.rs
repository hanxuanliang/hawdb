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
}
