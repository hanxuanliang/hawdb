use crate::{OptimizerContext, VectorPrecision};
use skein_plan::{VectorPhysicalPlan, VectorSearchLogicalPlan};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorPlanError {
    EmptyEmbedding,
    EmptyTopK,
    CandidateLimitBelowTopK,
    InitialCandidateLimitBelowTopK,
    InitialCandidateLimitAboveBudget,
    MissingFilter,
    MissingCandidateScan,
    MissingRawRerank,
    MissingTopK,
}

impl Display for VectorPlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::EmptyEmbedding => "vector embedding dimension must be non-zero",
            Self::EmptyTopK => "vector top-k must be non-zero",
            Self::CandidateLimitBelowTopK => "vector candidate limit must cover top-k",
            Self::InitialCandidateLimitBelowTopK => {
                "initial vector candidate limit must cover top-k"
            }
            Self::InitialCandidateLimitAboveBudget => {
                "initial vector candidate limit must not exceed the candidate budget"
            }
            Self::MissingFilter => "vector pipeline must start with filter",
            Self::MissingCandidateScan => {
                "vector pipeline must include candidate scan after filter"
            }
            Self::MissingRawRerank => "vector pipeline must raw-rerank candidates",
            Self::MissingTopK => "vector pipeline must apply top-k after raw rerank",
        })
    }
}

impl std::error::Error for VectorPlanError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPlanProperties {
    pub precision: VectorPrecision,
    pub max_parallelism: usize,
    pub max_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedVectorSearch {
    pub plan: VectorPhysicalPlan,
    pub properties: VectorPlanProperties,
}

pub fn plan_vector_search(
    logical: &VectorSearchLogicalPlan,
    context: &OptimizerContext,
) -> Result<PlannedVectorSearch, VectorPlanError> {
    if logical.embedding_dimension == 0 {
        return Err(VectorPlanError::EmptyEmbedding);
    }
    if logical.top_k == 0 {
        return Err(VectorPlanError::EmptyTopK);
    }
    if logical.candidate_limit < logical.top_k {
        return Err(VectorPlanError::CandidateLimitBelowTopK);
    }
    if logical.initial_candidate_limit < logical.top_k {
        return Err(VectorPlanError::InitialCandidateLimitBelowTopK);
    }
    if logical.initial_candidate_limit > logical.candidate_limit {
        return Err(VectorPlanError::InitialCandidateLimitAboveBudget);
    }

    let filter = VectorPhysicalPlan::Filter {
        fields: logical.filter_fields.clone(),
    };
    let candidates = VectorPhysicalPlan::VectorCandidateScan {
        source: logical.candidate_source,
        embedding_dimension: logical.embedding_dimension,
        candidate_limit: logical.candidate_limit,
        input: Box::new(filter),
    };
    let candidates = if logical.residual_filter_fields.is_empty() {
        candidates
    } else {
        VectorPhysicalPlan::ResidualFilter {
            fields: logical.residual_filter_fields.clone(),
            initial_candidate_limit: logical.initial_candidate_limit,
            input: Box::new(candidates),
        }
    };
    let rerank = VectorPhysicalPlan::RawVectorRerank {
        embedding_dimension: logical.embedding_dimension,
        input: Box::new(candidates),
    };
    let plan = VectorPhysicalPlan::TopK {
        limit: logical.top_k,
        input: Box::new(rerank),
    };
    validate_vector_pipeline(&plan)?;

    Ok(PlannedVectorSearch {
        plan,
        properties: VectorPlanProperties {
            precision: VectorPrecision::RawReranked,
            max_parallelism: context.resource_hints().max_parallelism.max(1),
            max_memory_bytes: context.resource_hints().max_memory_bytes,
        },
    })
}

pub fn validate_vector_pipeline(plan: &VectorPhysicalPlan) -> Result<(), VectorPlanError> {
    let VectorPhysicalPlan::TopK { input, .. } = plan else {
        return Err(VectorPlanError::MissingTopK);
    };
    let VectorPhysicalPlan::RawVectorRerank { input, .. } = input.as_ref() else {
        return Err(VectorPlanError::MissingRawRerank);
    };
    let input = match input.as_ref() {
        VectorPhysicalPlan::ResidualFilter { input, .. } => input.as_ref(),
        input => input,
    };
    let VectorPhysicalPlan::VectorCandidateScan { input, .. } = input else {
        return Err(VectorPlanError::MissingCandidateScan);
    };
    if !matches!(input.as_ref(), VectorPhysicalPlan::Filter { .. }) {
        return Err(VectorPlanError::MissingFilter);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryFamily, ResourceHints};
    use skein_plan::VectorCandidateSource;

    #[test]
    fn vector_pipeline_enforces_filter_candidate_raw_rerank_top_k() {
        let logical = VectorSearchLogicalPlan {
            embedding_dimension: 384,
            filter_fields: vec!["space_id".to_string(), "unit_type".to_string()],
            residual_filter_fields: Vec::new(),
            initial_candidate_limit: 64,
            candidate_source: VectorCandidateSource::Quantized,
            candidate_limit: 64,
            top_k: 10,
        };
        let context = OptimizerContext::default()
            .with_query_family(QueryFamily::VectorSearch)
            .with_resource_hints(ResourceHints {
                priority: 128,
                max_memory_bytes: Some(8 * 1024 * 1024),
                max_parallelism: 2,
            });

        let planned = plan_vector_search(&logical, &context).unwrap();

        assert_eq!(
            planned.plan.operator_pipeline(),
            vec!["Filter", "VectorCandidateScan", "RawVectorRerank", "TopK"]
        );
        assert_eq!(planned.properties.precision, VectorPrecision::RawReranked);
        assert_eq!(planned.properties.max_parallelism, 2);
    }

    #[test]
    fn vector_pipeline_rejects_final_candidate_scores_without_raw_rerank() {
        let invalid = VectorPhysicalPlan::TopK {
            limit: 10,
            input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
                source: VectorCandidateSource::Ann,
                embedding_dimension: 384,
                candidate_limit: 40,
                input: Box::new(VectorPhysicalPlan::Filter { fields: Vec::new() }),
            }),
        };

        assert_eq!(
            validate_vector_pipeline(&invalid),
            Err(VectorPlanError::MissingRawRerank)
        );
    }

    #[test]
    fn vector_pipeline_places_residual_filter_before_raw_rerank() {
        let logical = VectorSearchLogicalPlan {
            embedding_dimension: 384,
            filter_fields: vec!["space_id".to_string()],
            residual_filter_fields: vec!["complex_visibility".to_string()],
            initial_candidate_limit: 10,
            candidate_source: VectorCandidateSource::Ann,
            candidate_limit: 80,
            top_k: 10,
        };

        let planned = plan_vector_search(&logical, &OptimizerContext::default()).unwrap();

        assert_eq!(
            planned.plan.operator_pipeline(),
            vec![
                "Filter",
                "VectorCandidateScan",
                "ResidualFilter",
                "RawVectorRerank",
                "TopK"
            ]
        );
        assert_eq!(validate_vector_pipeline(&planned.plan), Ok(()));
    }
}
