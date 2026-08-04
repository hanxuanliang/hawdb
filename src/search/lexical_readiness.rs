use crate::error::{Result, SkeinError};

pub const SEARCH_LEXICAL_QUALIFICATION_PROTOCOL: &str =
    "skein-search-lexical-production-qualification";
pub const SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION: u64 = 1;
const MINIMUM_DOCUMENT_COUNT: usize = 100_000;
const MAX_RSS_BUDGET_PER_MILLION: u64 = 1_100_000;
const MAX_WRITE_REGRESSION_PER_MILLION: u64 = 1_100_000;
const MIN_SELECTIVE_P95_IMPROVEMENT_PER_MILLION: u64 = 500_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchLexicalFeasibilityCoverage {
    pub selective_identifier: bool,
    pub cjk_text: bool,
    pub common_term: bool,
    pub no_hit: bool,
    pub metadata_filter: bool,
    pub acl_filter: bool,
    pub hybrid_rrf: bool,
    pub incremental_upsert_delete: bool,
    pub checkpoint_reopen: bool,
    pub corrupt_artifact: bool,
    pub stale_manifest: bool,
    pub mixed_foreground_background: bool,
    pub larger_than_memory: bool,
}

impl SearchLexicalFeasibilityCoverage {
    fn complete(&self) -> bool {
        self.selective_identifier
            && self.cjk_text
            && self.common_term
            && self.no_hit
            && self.metadata_filter
            && self.acl_filter
            && self.hybrid_rrf
            && self.incremental_upsert_delete
            && self.checkpoint_reopen
            && self.corrupt_artifact
            && self.stale_manifest
            && self.mixed_foreground_background
            && self.larger_than_memory
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "selective_identifier": self.selective_identifier,
            "cjk_text": self.cjk_text,
            "common_term": self.common_term,
            "no_hit": self.no_hit,
            "metadata_filter": self.metadata_filter,
            "acl_filter": self.acl_filter,
            "hybrid_rrf": self.hybrid_rrf,
            "incremental_upsert_delete": self.incremental_upsert_delete,
            "checkpoint_reopen": self.checkpoint_reopen,
            "corrupt_artifact": self.corrupt_artifact,
            "stale_manifest": self.stale_manifest,
            "mixed_foreground_background": self.mixed_foreground_background,
            "larger_than_memory": self.larger_than_memory,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchLexicalFeasibilityMetrics {
    pub canonical_dataset_bytes: u64,
    pub storage_memory_budget_bytes: u64,
    pub steady_resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub baseline_selective_text_p50_micros: u64,
    pub baseline_selective_text_p95_micros: u64,
    pub baseline_selective_text_p99_micros: u64,
    pub segmented_selective_text_p50_micros: u64,
    pub segmented_selective_text_p95_micros: u64,
    pub segmented_selective_text_p99_micros: u64,
    pub baseline_throughput_per_second: u64,
    pub segmented_throughput_per_second: u64,
    pub selective_posting_bytes_read: u64,
    pub selective_candidate_postings_visited: u64,
    pub selective_matching_document_count: usize,
    pub minor_page_faults: u64,
    pub major_page_faults: u64,
    pub baseline_update_p95_micros: u64,
    pub segmented_update_p95_micros: u64,
    pub baseline_checkpoint_p95_micros: u64,
    pub segmented_checkpoint_p95_micros: u64,
    pub consolidation_write_amplification_per_million: u64,
    pub recovery_p95_micros: u64,
}

impl SearchLexicalFeasibilityMetrics {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "canonical_dataset_bytes": self.canonical_dataset_bytes,
            "storage_memory_budget_bytes": self.storage_memory_budget_bytes,
            "steady_resident_bytes": self.steady_resident_bytes,
            "peak_resident_bytes": self.peak_resident_bytes,
            "baseline_selective_text_p50_micros": self.baseline_selective_text_p50_micros,
            "baseline_selective_text_p95_micros": self.baseline_selective_text_p95_micros,
            "baseline_selective_text_p99_micros": self.baseline_selective_text_p99_micros,
            "segmented_selective_text_p50_micros": self.segmented_selective_text_p50_micros,
            "segmented_selective_text_p95_micros": self.segmented_selective_text_p95_micros,
            "segmented_selective_text_p99_micros": self.segmented_selective_text_p99_micros,
            "baseline_throughput_per_second": self.baseline_throughput_per_second,
            "segmented_throughput_per_second": self.segmented_throughput_per_second,
            "selective_posting_bytes_read": self.selective_posting_bytes_read,
            "selective_candidate_postings_visited": self.selective_candidate_postings_visited,
            "selective_matching_document_count": self.selective_matching_document_count,
            "minor_page_faults": self.minor_page_faults,
            "major_page_faults": self.major_page_faults,
            "baseline_update_p95_micros": self.baseline_update_p95_micros,
            "segmented_update_p95_micros": self.segmented_update_p95_micros,
            "baseline_checkpoint_p95_micros": self.baseline_checkpoint_p95_micros,
            "segmented_checkpoint_p95_micros": self.segmented_checkpoint_p95_micros,
            "consolidation_write_amplification_per_million": self.consolidation_write_amplification_per_million,
            "recovery_p95_micros": self.recovery_p95_micros,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchLexicalProductionQualificationReport {
    pub protocol: String,
    pub protocol_version: u64,
    pub projection_generation: u64,
    pub source_graph_commit_epoch: Option<u64>,
    pub document_count: usize,
    pub exact_topk_score_parity: bool,
    pub coverage: SearchLexicalFeasibilityCoverage,
    pub metrics: SearchLexicalFeasibilityMetrics,
    pub blocker_codes: Vec<String>,
    pub ready: bool,
}

impl SearchLexicalProductionQualificationReport {
    pub fn evaluate(
        projection_generation: u64,
        source_graph_commit_epoch: Option<u64>,
        document_count: usize,
        exact_topk_score_parity: bool,
        coverage: SearchLexicalFeasibilityCoverage,
        metrics: SearchLexicalFeasibilityMetrics,
    ) -> Self {
        let mut report = Self {
            protocol: SEARCH_LEXICAL_QUALIFICATION_PROTOCOL.to_string(),
            protocol_version: SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION,
            projection_generation,
            source_graph_commit_epoch,
            document_count,
            exact_topk_score_parity,
            coverage,
            metrics,
            blocker_codes: Vec::new(),
            ready: false,
        };
        report.blocker_codes = report.recompute_blocker_codes(
            projection_generation,
            source_graph_commit_epoch,
            document_count,
        );
        report.ready = report.blocker_codes.is_empty();
        report
    }

    pub fn validate_for_projection(
        &self,
        projection_generation: u64,
        source_graph_commit_epoch: Option<u64>,
        document_count: usize,
    ) -> Result<()> {
        let blockers = self.recompute_blocker_codes(
            projection_generation,
            source_graph_commit_epoch,
            document_count,
        );
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(SkeinError::Storage(format!(
                "segmented lexical projection is not qualified for production: {}",
                blockers.join(",")
            )))
        }
    }

    pub fn json(&self) -> serde_json::Value {
        let recomputed_blockers = self.recompute_blocker_codes(
            self.projection_generation,
            self.source_graph_commit_epoch,
            self.document_count,
        );
        serde_json::json!({
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "projection_generation": self.projection_generation,
            "source_graph_commit_epoch": self.source_graph_commit_epoch,
            "document_count": self.document_count,
            "exact_topk_score_parity": self.exact_topk_score_parity,
            "coverage": self.coverage.json(),
            "metrics": self.metrics.json(),
            "thresholds": {
                "minimum_document_count": MINIMUM_DOCUMENT_COUNT,
                "minimum_selective_p95_improvement_per_million": MIN_SELECTIVE_P95_IMPROVEMENT_PER_MILLION,
                "maximum_rss_budget_per_million": MAX_RSS_BUDGET_PER_MILLION,
                "maximum_write_regression_per_million": MAX_WRITE_REGRESSION_PER_MILLION,
            },
            "blocker_codes": recomputed_blockers,
            "ready": recomputed_blockers.is_empty(),
        })
    }

    fn recompute_blocker_codes(
        &self,
        projection_generation: u64,
        source_graph_commit_epoch: Option<u64>,
        document_count: usize,
    ) -> Vec<String> {
        let mut blockers = Vec::new();
        if self.protocol != SEARCH_LEXICAL_QUALIFICATION_PROTOCOL
            || self.protocol_version != SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION
        {
            blockers.push("protocol_mismatch".to_string());
        }
        if self.projection_generation == 0
            || self.projection_generation != projection_generation
            || self.source_graph_commit_epoch != source_graph_commit_epoch
            || self.document_count != document_count
        {
            blockers.push("projection_identity_mismatch".to_string());
        }
        if document_count < MINIMUM_DOCUMENT_COUNT {
            blockers.push("dataset_too_small".to_string());
        }
        if !self.exact_topk_score_parity {
            blockers.push("topk_score_parity_failed".to_string());
        }
        if !self.coverage.complete() {
            blockers.push("workload_coverage_incomplete".to_string());
        }
        if self.metrics.storage_memory_budget_bytes == 0
            || self.metrics.canonical_dataset_bytes <= self.metrics.storage_memory_budget_bytes
        {
            blockers.push("larger_than_memory_not_proven".to_string());
        }
        if !ratio_within(
            self.metrics.steady_resident_bytes,
            self.metrics.storage_memory_budget_bytes,
            MAX_RSS_BUDGET_PER_MILLION,
        ) || !ratio_within(
            self.metrics.peak_resident_bytes,
            self.metrics.storage_memory_budget_bytes,
            MAX_RSS_BUDGET_PER_MILLION,
        ) {
            blockers.push("resident_memory_budget_exceeded".to_string());
        }
        if self.metrics.baseline_selective_text_p95_micros == 0
            || self.metrics.segmented_selective_text_p95_micros == 0
            || !ratio_within(
                self.metrics.segmented_selective_text_p95_micros,
                self.metrics.baseline_selective_text_p95_micros,
                MIN_SELECTIVE_P95_IMPROVEMENT_PER_MILLION,
            )
        {
            blockers.push("selective_text_p95_improvement_insufficient".to_string());
        }
        if self.metrics.selective_posting_bytes_read == 0
            || self.metrics.selective_candidate_postings_visited == 0
            || self.metrics.selective_candidate_postings_visited >= document_count as u64
            || self.metrics.selective_matching_document_count >= document_count
        {
            blockers.push("selective_work_not_posting_proportional".to_string());
        }
        if !regression_within(
            self.metrics.segmented_update_p95_micros,
            self.metrics.baseline_update_p95_micros,
        ) {
            blockers.push("update_p95_regression_exceeded".to_string());
        }
        if !regression_within(
            self.metrics.segmented_checkpoint_p95_micros,
            self.metrics.baseline_checkpoint_p95_micros,
        ) {
            blockers.push("checkpoint_p95_regression_exceeded".to_string());
        }
        blockers
    }
}

fn regression_within(measured: u64, baseline: u64) -> bool {
    baseline > 0
        && measured > 0
        && ratio_within(measured, baseline, MAX_WRITE_REGRESSION_PER_MILLION)
}

fn ratio_within(measured: u64, baseline: u64, limit_per_million: u64) -> bool {
    baseline > 0
        && u128::from(measured).saturating_mul(1_000_000)
            <= u128::from(baseline).saturating_mul(u128::from(limit_per_million))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_coverage() -> SearchLexicalFeasibilityCoverage {
        SearchLexicalFeasibilityCoverage {
            selective_identifier: true,
            cjk_text: true,
            common_term: true,
            no_hit: true,
            metadata_filter: true,
            acl_filter: true,
            hybrid_rrf: true,
            incremental_upsert_delete: true,
            checkpoint_reopen: true,
            corrupt_artifact: true,
            stale_manifest: true,
            mixed_foreground_background: true,
            larger_than_memory: true,
        }
    }

    fn passing_metrics() -> SearchLexicalFeasibilityMetrics {
        SearchLexicalFeasibilityMetrics {
            canonical_dataset_bytes: 2_000_000_000,
            storage_memory_budget_bytes: 1_000_000_000,
            steady_resident_bytes: 900_000_000,
            peak_resident_bytes: 1_050_000_000,
            baseline_selective_text_p50_micros: 100,
            baseline_selective_text_p95_micros: 200,
            baseline_selective_text_p99_micros: 300,
            segmented_selective_text_p50_micros: 40,
            segmented_selective_text_p95_micros: 100,
            segmented_selective_text_p99_micros: 140,
            baseline_throughput_per_second: 1_000,
            segmented_throughput_per_second: 2_000,
            selective_posting_bytes_read: 4096,
            selective_candidate_postings_visited: 32,
            selective_matching_document_count: 16,
            minor_page_faults: 20,
            major_page_faults: 0,
            baseline_update_p95_micros: 100,
            segmented_update_p95_micros: 110,
            baseline_checkpoint_p95_micros: 1_000,
            segmented_checkpoint_p95_micros: 1_100,
            consolidation_write_amplification_per_million: 1_100_000,
            recovery_p95_micros: 20_000,
        }
    }

    #[test]
    fn accepts_complete_generation_bound_production_evidence() {
        let report = SearchLexicalProductionQualificationReport::evaluate(
            7,
            Some(42),
            100_000,
            true,
            complete_coverage(),
            passing_metrics(),
        );

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        report
            .validate_for_projection(7, Some(42), 100_000)
            .unwrap();
        assert_eq!(report.json()["ready"], true);
    }

    #[test]
    fn rejects_forged_ready_flag_and_stale_projection_identity() {
        let mut metrics = passing_metrics();
        metrics.segmented_selective_text_p95_micros = 101;
        let mut report = SearchLexicalProductionQualificationReport::evaluate(
            7,
            Some(42),
            100_000,
            true,
            complete_coverage(),
            metrics,
        );
        report.ready = true;
        report.blocker_codes.clear();

        let error = report
            .validate_for_projection(8, Some(43), 100_000)
            .unwrap_err();
        assert!(error.to_string().contains("projection_identity_mismatch"));
        assert!(error
            .to_string()
            .contains("selective_text_p95_improvement_insufficient"));
        assert_eq!(report.json()["ready"], false);
    }

    #[test]
    fn rejects_small_or_memory_resident_benchmark_fixtures() {
        let mut metrics = passing_metrics();
        metrics.canonical_dataset_bytes = metrics.storage_memory_budget_bytes;
        let report = SearchLexicalProductionQualificationReport::evaluate(
            1,
            None,
            99_999,
            true,
            complete_coverage(),
            metrics,
        );

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&"dataset_too_small".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"larger_than_memory_not_proven".to_string()));
    }
}
