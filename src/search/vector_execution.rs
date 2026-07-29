use super::{cosine_similarity, SearchDocument, SearchFallbackReasonCode, VectorSearchBackend};
use crate::error::{Result, SkeinError};
use skein_executor::{
    execute_vector_plan, VectorCandidate, VectorCandidateBatch, VectorCandidateScanRequest,
    VectorExecutionReport, VectorExecutionSource, VectorRawRerankRequest, VectorRawScore,
    VectorResidualFilterRequest, VectorScoreSource,
};
use skein_optimizer::{
    plan_vector_search, OptimizerContext, QueryFamily, ResourceHints, VectorPrecision,
};
use skein_plan::VectorSearchLogicalPlan;
use std::collections::BTreeMap;
#[cfg(feature = "turbovec")]
use std::collections::BTreeSet;

pub(super) struct SearchVectorExecution {
    pub scores: BTreeMap<String, f64>,
    pub report: VectorExecutionReport,
}

pub(super) struct SearchVectorExecutionRequest<'a, 'b> {
    pub query_embedding: &'a [f32],
    pub documents: &'a [&'a SearchDocument],
    pub backend: VectorSearchBackend<'a>,
    pub filter_fields: Vec<String>,
    pub limit: usize,
    pub rank_window: Option<usize>,
    pub fallback_reason_codes: &'b mut Vec<SearchFallbackReasonCode>,
    pub fallback_reasons: &'b mut Vec<String>,
}

pub(super) fn execute_search_vector_plan(
    request: SearchVectorExecutionRequest<'_, '_>,
) -> Result<SearchVectorExecution> {
    let SearchVectorExecutionRequest {
        query_embedding,
        documents,
        backend,
        filter_fields,
        limit,
        rank_window,
        fallback_reason_codes,
        fallback_reasons,
    } = request;
    let retrieval_limit = match backend {
        VectorSearchBackend::Scalar => documents.len().max(1),
        _ => limit.max(rank_window.unwrap_or(0)).max(1),
    };
    let logical = VectorSearchLogicalPlan {
        embedding_dimension: query_embedding.len(),
        filter_fields,
        residual_filter_fields: Vec::new(),
        initial_candidate_limit: retrieval_limit,
        candidate_source: backend.candidate_source(),
        candidate_limit: retrieval_limit,
        top_k: retrieval_limit,
    };
    let context = OptimizerContext::default()
        .with_query_family(QueryFamily::VectorSearch)
        .with_resource_hints(ResourceHints {
            priority: 128,
            max_memory_bytes: None,
            max_parallelism: 1,
        });
    let planned = plan_vector_search(&logical, &context)
        .map_err(|error| SkeinError::Storage(format!("vector planning failed: {error}")))?;
    debug_assert_eq!(planned.properties.precision, VectorPrecision::RawReranked);

    let mut source = SearchVectorSource {
        query_embedding,
        documents,
        backend,
        fallback_reason_codes,
        fallback_reasons,
    };
    let output = execute_vector_plan(&planned.plan, &mut source).map_err(|error| {
        SkeinError::Storage(format!("vector physical execution failed: {error}"))
    })?;
    Ok(SearchVectorExecution {
        scores: output
            .scores
            .into_iter()
            .filter(|score| score.score > 0.0)
            .map(|score| (score.id, score.score))
            .collect(),
        report: output.report,
    })
}

struct SearchVectorSource<'a, 'b> {
    query_embedding: &'a [f32],
    documents: &'a [&'a SearchDocument],
    backend: VectorSearchBackend<'a>,
    fallback_reason_codes: &'b mut Vec<SearchFallbackReasonCode>,
    fallback_reasons: &'b mut Vec<String>,
}

impl VectorExecutionSource for SearchVectorSource<'_, '_> {
    type Error = SkeinError;

    fn scan_candidates(
        &mut self,
        request: VectorCandidateScanRequest<'_>,
    ) -> Result<VectorCandidateBatch> {
        debug_assert_eq!(request.source, self.backend.candidate_source());
        debug_assert_eq!(request.embedding_dimension, self.query_embedding.len());
        match self.backend {
            VectorSearchBackend::Scalar => {
                Ok(raw_vector_candidates(self.query_embedding, self.documents))
            }
            VectorSearchBackend::CompressedRequiredUnavailable => {
                self.fallback_reason_codes
                    .push(SearchFallbackReasonCode::CompressedVectorProjectionUnavailable);
                self.fallback_reasons.push(
                    "compressed vector projection required but unavailable; scalar vector scan disabled"
                        .to_string(),
                );
                Ok(VectorCandidateBatch {
                    score_source: VectorScoreSource::Unavailable,
                    candidates: Vec::new(),
                })
            }
            #[cfg(not(feature = "turbovec"))]
            VectorSearchBackend::_Lifetime(_) => {
                unreachable!("lifetime marker is never constructed")
            }
            #[cfg(feature = "turbovec")]
            VectorSearchBackend::Turbovec(projection) => {
                let allowlist = self
                    .documents
                    .iter()
                    .map(|document| document.id.clone())
                    .collect::<BTreeSet<_>>();
                match projection.search(
                    self.query_embedding,
                    request.candidate_limit,
                    Some(&allowlist),
                ) {
                    Ok(hits) => Ok(VectorCandidateBatch {
                        score_source: VectorScoreSource::QuantizedApproximate,
                        candidates: hits
                            .into_iter()
                            .map(|hit| VectorCandidate {
                                id: hit.id,
                                score: hit.score,
                            })
                            .collect(),
                    }),
                    Err(error) => {
                        self.fallback_reason_codes
                            .push(SearchFallbackReasonCode::VectorIndexEmpty);
                        self.fallback_reasons.push(format!(
                            "compressed vector projection unavailable; fell back to scalar vector scan: {error}"
                        ));
                        Ok(raw_vector_candidates(self.query_embedding, self.documents))
                    }
                }
            }
        }
    }

    fn rerank_raw(&mut self, request: VectorRawRerankRequest<'_>) -> Result<Vec<VectorRawScore>> {
        debug_assert_eq!(request.embedding_dimension, self.query_embedding.len());
        let documents = self
            .documents
            .iter()
            .map(|document| (document.id.as_str(), *document))
            .collect::<BTreeMap<_, _>>();
        Ok(request
            .candidates
            .iter()
            .filter_map(|candidate| {
                let document = documents.get(candidate.id.as_str())?;
                let score =
                    cosine_similarity(self.query_embedding, document.embedding.as_deref()?)?;
                Some(VectorRawScore {
                    id: candidate.id.clone(),
                    score,
                })
            })
            .collect())
    }

    fn filter_residual(
        &mut self,
        request: VectorResidualFilterRequest<'_>,
    ) -> Result<Vec<VectorCandidate>> {
        debug_assert!(
            request.fields.is_empty(),
            "supported Nowledge filters must be descriptor-safe"
        );
        Ok(request.candidates)
    }
}

fn raw_vector_candidates(
    query_embedding: &[f32],
    documents: &[&SearchDocument],
) -> VectorCandidateBatch {
    VectorCandidateBatch {
        score_source: VectorScoreSource::RawVector,
        candidates: documents
            .iter()
            .filter_map(|document| {
                let score = cosine_similarity(query_embedding, document.embedding.as_deref()?)?;
                Some(VectorCandidate {
                    id: document.id.clone(),
                    score,
                })
            })
            .collect(),
    }
}
