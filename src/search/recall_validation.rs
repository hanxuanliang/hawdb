use super::{SearchRetrieverReport, TURBOQUANT_CANDIDATE_BACKEND};
use std::collections::BTreeSet;

pub const VECTOR_RECALL_VALIDATION_PROTOCOL: &str = "skein-vector-recall-validation-v1";
pub const MAX_VECTOR_RECALL_VALIDATION_SAMPLES: usize = 128;
pub const MAX_VECTOR_RECALL_VALIDATION_TOP_K: usize = 100;
const PER_MILLION: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorRecallValidationOptions {
    pub max_samples: usize,
    pub top_k: usize,
    pub minimum_recall_per_million: u32,
    pub metadata_filters: std::collections::BTreeMap<String, String>,
}

impl Default for VectorRecallValidationOptions {
    fn default() -> Self {
        Self {
            max_samples: 32,
            top_k: 10,
            minimum_recall_per_million: 950_000,
            metadata_filters: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum VectorRecallValidationBlocker {
    NoSamplesRequested,
    TopKZero,
    MetadataFilterInvalid,
    NoEligibleVectors,
    GroundTruthEmpty,
    ApproximateBackendUnavailable,
    ApproximateFallbackObserved,
    IndexCoverageIncomplete,
    RecallBelowThreshold,
}

impl VectorRecallValidationBlocker {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoSamplesRequested => "no_samples_requested",
            Self::TopKZero => "top_k_zero",
            Self::MetadataFilterInvalid => "metadata_filter_invalid",
            Self::NoEligibleVectors => "no_eligible_vectors",
            Self::GroundTruthEmpty => "ground_truth_empty",
            Self::ApproximateBackendUnavailable => "approximate_backend_unavailable",
            Self::ApproximateFallbackObserved => "approximate_fallback_observed",
            Self::IndexCoverageIncomplete => "index_coverage_incomplete",
            Self::RecallBelowThreshold => "recall_below_threshold",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorRecallValidationReport {
    pub protocol: String,
    pub ready: bool,
    pub approximate_backend: String,
    pub sample_candidate_count: usize,
    pub requested_sample_count: usize,
    pub executed_sample_count: usize,
    pub top_k: usize,
    pub minimum_recall_per_million: u32,
    pub exact_hit_count: usize,
    pub approximate_hit_count: usize,
    pub overlap_count: usize,
    pub recall_at_k_per_million: u32,
    pub overlap_at_k_per_million: u32,
    pub fallback_count: usize,
    pub index_coverage_incomplete_count: usize,
    pub average_filter_selectivity_per_million: u32,
    pub max_filter_selectivity_per_million: u32,
    pub blocker_codes: Vec<VectorRecallValidationBlocker>,
}

impl VectorRecallValidationReport {
    pub fn validates_required_approximate_backend(&self) -> bool {
        self.protocol == VECTOR_RECALL_VALIDATION_PROTOCOL
            && self.ready
            && self.approximate_backend == TURBOQUANT_CANDIDATE_BACKEND
            && self.requested_sample_count > 0
            && self.executed_sample_count == self.requested_sample_count
            && self.exact_hit_count > 0
            && self.fallback_count == 0
            && self.index_coverage_incomplete_count == 0
            && self.recall_at_k_per_million >= self.minimum_recall_per_million
            && self.blocker_codes.is_empty()
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "approximate_backend": self.approximate_backend,
            "sample_candidate_count": self.sample_candidate_count,
            "requested_sample_count": self.requested_sample_count,
            "executed_sample_count": self.executed_sample_count,
            "top_k": self.top_k,
            "minimum_recall_per_million": self.minimum_recall_per_million,
            "exact_hit_count": self.exact_hit_count,
            "approximate_hit_count": self.approximate_hit_count,
            "overlap_count": self.overlap_count,
            "recall_at_k_per_million": self.recall_at_k_per_million,
            "overlap_at_k_per_million": self.overlap_at_k_per_million,
            "fallback_count": self.fallback_count,
            "index_coverage_incomplete_count": self.index_coverage_incomplete_count,
            "average_filter_selectivity_per_million": self.average_filter_selectivity_per_million,
            "max_filter_selectivity_per_million": self.max_filter_selectivity_per_million,
            "blocker_codes": self.blocker_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
        })
    }
}

pub(super) struct VectorRecallValidationAccumulator {
    sample_candidate_count: usize,
    requested_sample_count: usize,
    top_k: usize,
    minimum_recall_per_million: u32,
    executed_sample_count: usize,
    exact_hit_count: usize,
    approximate_hit_count: usize,
    overlap_count: usize,
    fallback_count: usize,
    approximate_backend_count: usize,
    index_coverage_incomplete_count: usize,
    filter_selectivity_sum: u64,
    max_filter_selectivity_per_million: u32,
    metadata_filter_valid: bool,
}

impl VectorRecallValidationAccumulator {
    pub(super) fn new(
        sample_candidate_count: usize,
        options: &VectorRecallValidationOptions,
    ) -> Self {
        Self {
            sample_candidate_count,
            requested_sample_count: options
                .max_samples
                .min(MAX_VECTOR_RECALL_VALIDATION_SAMPLES)
                .min(sample_candidate_count),
            top_k: options.top_k.min(MAX_VECTOR_RECALL_VALIDATION_TOP_K),
            minimum_recall_per_million: options.minimum_recall_per_million.min(PER_MILLION as u32),
            executed_sample_count: 0,
            exact_hit_count: 0,
            approximate_hit_count: 0,
            overlap_count: 0,
            fallback_count: 0,
            approximate_backend_count: 0,
            index_coverage_incomplete_count: 0,
            filter_selectivity_sum: 0,
            max_filter_selectivity_per_million: 0,
            metadata_filter_valid: true,
        }
    }

    pub(super) fn requested_sample_count(&self) -> usize {
        self.requested_sample_count
    }

    pub(super) fn top_k(&self) -> usize {
        self.top_k
    }

    pub(super) fn mark_metadata_filter_invalid(&mut self) {
        self.metadata_filter_valid = false;
    }

    pub(super) fn record(
        &mut self,
        exact_ids: &[String],
        approximate_ids: &[String],
        approximate_retriever: &SearchRetrieverReport,
    ) {
        self.executed_sample_count = self.executed_sample_count.saturating_add(1);
        self.exact_hit_count = self.exact_hit_count.saturating_add(exact_ids.len());
        self.approximate_hit_count = self
            .approximate_hit_count
            .saturating_add(approximate_ids.len());
        let exact_ids = exact_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        self.overlap_count = self.overlap_count.saturating_add(
            approximate_ids
                .iter()
                .filter(|id| exact_ids.contains(id.as_str()))
                .count(),
        );
        if approximate_retriever.backend == TURBOQUANT_CANDIDATE_BACKEND {
            self.approximate_backend_count = self.approximate_backend_count.saturating_add(1);
        }
        if !approximate_retriever.fallback_reason_codes.is_empty() {
            self.fallback_count = self.fallback_count.saturating_add(1);
        }
        if !approximate_retriever.index_coverage_complete {
            self.index_coverage_incomplete_count =
                self.index_coverage_incomplete_count.saturating_add(1);
        }
        let selectivity = approximate_retriever
            .filter_selectivity_per_million
            .unwrap_or(0);
        self.filter_selectivity_sum = self
            .filter_selectivity_sum
            .saturating_add(u64::from(selectivity));
        self.max_filter_selectivity_per_million =
            self.max_filter_selectivity_per_million.max(selectivity);
    }

    pub(super) fn finish(self) -> VectorRecallValidationReport {
        let recall_at_k_per_million = ratio_per_million(self.overlap_count, self.exact_hit_count);
        let overlap_denominator = self.executed_sample_count.saturating_mul(self.top_k);
        let overlap_at_k_per_million = ratio_per_million(self.overlap_count, overlap_denominator);
        let average_filter_selectivity_per_million =
            average_per_million(self.filter_selectivity_sum, self.executed_sample_count);
        let mut blocker_codes = BTreeSet::new();
        if self.requested_sample_count == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::NoSamplesRequested);
        }
        if self.top_k == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::TopKZero);
        }
        if !self.metadata_filter_valid {
            blocker_codes.insert(VectorRecallValidationBlocker::MetadataFilterInvalid);
        }
        if self.sample_candidate_count == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::NoEligibleVectors);
        }
        if self.exact_hit_count == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::GroundTruthEmpty);
        }
        if self.approximate_backend_count != self.executed_sample_count {
            blocker_codes.insert(VectorRecallValidationBlocker::ApproximateBackendUnavailable);
        }
        if self.fallback_count > 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::ApproximateFallbackObserved);
        }
        if self.index_coverage_incomplete_count > 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::IndexCoverageIncomplete);
        }
        if self.exact_hit_count > 0 && recall_at_k_per_million < self.minimum_recall_per_million {
            blocker_codes.insert(VectorRecallValidationBlocker::RecallBelowThreshold);
        }
        let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
        VectorRecallValidationReport {
            protocol: VECTOR_RECALL_VALIDATION_PROTOCOL.to_string(),
            ready: blocker_codes.is_empty(),
            approximate_backend: TURBOQUANT_CANDIDATE_BACKEND.to_string(),
            sample_candidate_count: self.sample_candidate_count,
            requested_sample_count: self.requested_sample_count,
            executed_sample_count: self.executed_sample_count,
            top_k: self.top_k,
            minimum_recall_per_million: self.minimum_recall_per_million,
            exact_hit_count: self.exact_hit_count,
            approximate_hit_count: self.approximate_hit_count,
            overlap_count: self.overlap_count,
            recall_at_k_per_million,
            overlap_at_k_per_million,
            fallback_count: self.fallback_count,
            index_coverage_incomplete_count: self.index_coverage_incomplete_count,
            average_filter_selectivity_per_million,
            max_filter_selectivity_per_million: self.max_filter_selectivity_per_million,
            blocker_codes,
        }
    }
}

pub(super) fn sample_positions(candidate_count: usize, sample_count: usize) -> Vec<usize> {
    if candidate_count == 0 || sample_count == 0 {
        return Vec::new();
    }
    let sample_count = sample_count.min(candidate_count);
    if sample_count == 1 {
        return vec![0];
    }
    (0..sample_count)
        .map(|sample| {
            sample.saturating_mul(candidate_count.saturating_sub(1))
                / sample_count.saturating_sub(1)
        })
        .collect()
}

fn ratio_per_million(numerator: usize, denominator: usize) -> u32 {
    if denominator == 0 {
        return 0;
    }
    u32::try_from(
        u64::try_from(numerator)
            .unwrap_or(u64::MAX)
            .saturating_mul(PER_MILLION)
            / u64::try_from(denominator).unwrap_or(u64::MAX),
    )
    .unwrap_or(u32::MAX)
}

fn average_per_million(sum: u64, count: usize) -> u32 {
    if count == 0 {
        return 0;
    }
    u32::try_from(sum / u64::try_from(count).unwrap_or(u64::MAX)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{
        SearchCandidateSetReport, SearchPredicatePushdownReport, SearchRetrieverCandidateSetReport,
    };

    fn retriever(backend: &str) -> SearchRetrieverReport {
        SearchRetrieverReport {
            name: "vector".to_string(),
            backend: backend.to_string(),
            backend_selection_reason: None,
            estimated_raw_vector_bytes: None,
            filter_selectivity_per_million: Some(500_000),
            available: true,
            input_candidate_set: SearchCandidateSetReport {
                id_space: String::new(),
                representation: String::new(),
                cardinality: 0,
                exact: true,
                snapshot_source_graph_commit_epoch: None,
                policy_epoch: None,
                filtered_out_count: 0,
                metadata_filters: Default::default(),
                metadata_predicate_pushdown: SearchPredicatePushdownReport::default(),
            },
            candidate_score_source: String::new(),
            final_score_source: String::new(),
            generated_candidate_count: 0,
            candidate_scan_rounds: 0,
            descriptor_pruned_count: 0,
            scalar_filtered_count: 0,
            residual_filtered_count: 0,
            reranked_candidate_count: 0,
            raw_vector_bytes_read: 0,
            candidate_scan_kernel: None,
            candidate_scan_worker_count: 0,
            candidate_scan_segment_count: 0,
            candidate_scan_scanned_segment_count: 0,
            candidate_scan_scored_document_count: 0,
            candidate_scan_filtered_document_count: 0,
            candidate_scan_scanned_block_count: 0,
            candidate_scan_skipped_block_count: 0,
            candidate_scan_payload_bytes_read: 0,
            candidate_scan_admitted_working_bytes: 0,
            posting_bytes_read: 0,
            candidate_postings_visited: 0,
            segmented_lexical_projection_used: false,
            index_covered_document_count: 1,
            index_candidate_document_count: 1,
            index_coverage_complete: true,
            candidate_count: 0,
            candidate_set: SearchRetrieverCandidateSetReport {
                id_space: String::new(),
                representation: String::new(),
                cardinality: 0,
                exact: true,
                snapshot_source_graph_commit_epoch: None,
                policy_epoch: None,
            },
            fallback_reason_codes: Vec::new(),
            fallback_reasons: Vec::new(),
            top_hit_ids: Vec::new(),
            top_candidates: Vec::new(),
        }
    }

    #[test]
    fn accumulator_reports_recall_and_overlap_without_copying_ids() {
        let options = VectorRecallValidationOptions {
            max_samples: 2,
            top_k: 2,
            minimum_recall_per_million: 500_000,
            metadata_filters: Default::default(),
        };
        let mut accumulator = VectorRecallValidationAccumulator::new(2, &options);
        accumulator.record(
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "x".to_string()],
            &retriever(TURBOQUANT_CANDIDATE_BACKEND),
        );
        accumulator.record(
            &["c".to_string(), "d".to_string()],
            &["c".to_string(), "d".to_string()],
            &retriever(TURBOQUANT_CANDIDATE_BACKEND),
        );

        let report = accumulator.finish();

        assert!(report.ready);
        assert!(report.validates_required_approximate_backend());
        assert_eq!(report.recall_at_k_per_million, 750_000);
        assert_eq!(report.overlap_at_k_per_million, 750_000);
        assert_eq!(report.average_filter_selectivity_per_million, 500_000);
        let json = report.json().to_string();
        assert!(!json.contains("\"a\""));
        assert!(!json.contains("\"d\""));
    }

    #[test]
    fn missing_approximate_backend_fails_closed() {
        let options = VectorRecallValidationOptions {
            max_samples: 1,
            top_k: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: Default::default(),
        };
        let mut accumulator = VectorRecallValidationAccumulator::new(1, &options);
        accumulator.record(
            &["a".to_string()],
            &[],
            &retriever("compressed_vector_projection_required"),
        );

        let report = accumulator.finish();

        assert!(!report.ready);
        assert!(!report.validates_required_approximate_backend());
        assert!(report
            .blocker_codes
            .contains(&VectorRecallValidationBlocker::ApproximateBackendUnavailable));
        assert!(report
            .blocker_codes
            .contains(&VectorRecallValidationBlocker::RecallBelowThreshold));
    }

    #[test]
    fn sample_positions_are_bounded_and_spread() {
        assert_eq!(sample_positions(10, 3), vec![0, 4, 9]);
        assert_eq!(sample_positions(2, 10), vec![0, 1]);
        assert!(sample_positions(0, 3).is_empty());
    }
}
