use super::query::ProductionVectorQueryEvidence;
#[cfg(feature = "turbovec-oracle")]
use super::query::{
    candidate_digest, execute_query, query_options, result_digest, VectorExecutionProfile,
};
use super::{ProductionVectorQualificationConfig, ProductionVectorQualificationError};
use serde::Serialize;
use skein::{RuntimeTaskContext, SearchIndex};
#[cfg(feature = "turbovec-oracle")]
use skein::{SearchMode, SearchResultSet};
#[cfg(feature = "turbovec-oracle")]
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionVectorDifferentialOracleCase {
    pub name: String,
    pub turboquant_candidate_count: usize,
    pub turbovec_candidate_count: usize,
    pub candidate_overlap_per_million: u32,
    pub turbovec_candidate_digest: String,
    pub turbovec_result_digest: String,
    pub turbovec_final_matches_exact: bool,
    pub turboquant_turbovec_final_parity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorDifferentialOracleEvidence {
    pub required: bool,
    pub compiled: bool,
    pub available: bool,
    pub ready: bool,
    pub implementation: String,
    pub bit_width: usize,
    pub cases: Vec<ProductionVectorDifferentialOracleCase>,
}

impl ProductionVectorDifferentialOracleEvidence {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "required": self.required,
            "compiled": self.compiled,
            "available": self.available,
            "ready": self.ready,
            "implementation": self.implementation,
            "role": "development_differential_oracle_not_truth",
            "bit_width": self.bit_width,
            "cases": self.cases,
        })
    }
}

#[cfg(feature = "turbovec-oracle")]
pub(super) fn collect_differential_oracle(
    index: &SearchIndex,
    config: &ProductionVectorQualificationConfig,
    query_evidence: &[ProductionVectorQueryEvidence],
    task_context: &RuntimeTaskContext,
) -> Result<ProductionVectorDifferentialOracleEvidence, ProductionVectorQualificationError> {
    let Some(projection) = index
        .build_turbovec_projection(4)
        .map_err(ProductionVectorQualificationError::from_error)?
    else {
        return Ok(unavailable(config.require_turbovec_oracle, true));
    };
    let cases = config
        .query_cases
        .iter()
        .zip(query_evidence)
        .map(|(query_case, expected)| {
            let turboquant = execute_query(
                index,
                query_case,
                config,
                task_context,
                VectorExecutionProfile::AutoCandidate,
            )?;
            let turbovec = index.search_with_turbovec_projection_for_validation(
                &projection,
                "",
                Some(&query_case.query_embedding),
                SearchMode::Vector,
                query_options(query_case, config)?,
            );
            let turboquant_candidates = candidate_ids(&turboquant);
            let turbovec_candidates = candidate_ids(&turbovec);
            let turbovec_result_digest = result_digest(&turbovec);
            Ok(ProductionVectorDifferentialOracleCase {
                name: query_case.name.clone(),
                turboquant_candidate_count: turboquant_candidates.len(),
                turbovec_candidate_count: turbovec_candidates.len(),
                candidate_overlap_per_million: overlap_per_million(
                    &turboquant_candidates,
                    &turbovec_candidates,
                ),
                turbovec_candidate_digest: candidate_digest(&turbovec),
                turbovec_final_matches_exact: turbovec_result_digest
                    == expected.exact_result_digest,
                turboquant_turbovec_final_parity: turbovec_result_digest
                    == expected.auto_result_digest,
                turbovec_result_digest,
            })
        })
        .collect::<Result<Vec<_>, ProductionVectorQualificationError>>()?;
    let ready = cases.len() == config.query_cases.len()
        && cases
            .iter()
            .all(|case| case.turboquant_candidate_count > 0 && case.turbovec_candidate_count > 0);
    Ok(ProductionVectorDifferentialOracleEvidence {
        required: config.require_turbovec_oracle,
        compiled: true,
        available: true,
        ready,
        implementation: "upstream_turbovec".to_string(),
        bit_width: 4,
        cases,
    })
}

#[cfg(not(feature = "turbovec-oracle"))]
pub(super) fn collect_differential_oracle(
    _index: &SearchIndex,
    config: &ProductionVectorQualificationConfig,
    _query_evidence: &[ProductionVectorQueryEvidence],
    _task_context: &RuntimeTaskContext,
) -> Result<ProductionVectorDifferentialOracleEvidence, ProductionVectorQualificationError> {
    Ok(unavailable(config.require_turbovec_oracle, false))
}

fn unavailable(required: bool, compiled: bool) -> ProductionVectorDifferentialOracleEvidence {
    ProductionVectorDifferentialOracleEvidence {
        required,
        compiled,
        available: false,
        ready: false,
        implementation: "upstream_turbovec".to_string(),
        bit_width: 4,
        cases: Vec::new(),
    }
}

#[cfg(feature = "turbovec-oracle")]
fn candidate_ids(result: &SearchResultSet) -> BTreeSet<String> {
    result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .map(|retriever| retriever.candidate_top_ids.iter().cloned().collect())
        .unwrap_or_default()
}

#[cfg(feature = "turbovec-oracle")]
fn overlap_per_million(left: &BTreeSet<String>, right: &BTreeSet<String>) -> u32 {
    let denominator = left.len().max(right.len());
    if denominator == 0 {
        return 0;
    }
    u32::try_from(left.intersection(right).count().saturating_mul(1_000_000) / denominator)
        .unwrap_or(u32::MAX)
}
