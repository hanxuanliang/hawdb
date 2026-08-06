use super::query::ProductionVectorQueryEvidence;
#[cfg(feature = "turbovec-oracle")]
use super::query::{
    candidate_digest, execute_query, query_options, result_digest, VectorExecutionProfile,
};
use super::{ProductionVectorQualificationConfig, ProductionVectorQualificationError};
use serde::Serialize;
#[cfg(feature = "turbovec-oracle")]
use skein::SearchDocument;
use skein::{RuntimeTaskContext, SearchIndex};
#[cfg(feature = "turbovec-oracle")]
use skein::{SearchMode, SearchResultSet};
#[cfg(feature = "turbovec-oracle")]
use std::collections::{BTreeMap, BTreeSet};

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
    validate_oracle_corpus(index, &config.differential_oracle_documents)?;
    let Some(oracle) = TurbovecDifferentialOracle::build(&config.differential_oracle_documents, 4)?
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
            let metadata_filters = query_case.effective_metadata_filters()?;
            let allowed_document_ids =
                index.vector_document_ids_matching_filters_for_validation(&metadata_filters);
            let candidates = oracle.search(
                &query_case.query_embedding,
                config.candidate_limit,
                &allowed_document_ids,
            )?;
            let turbovec = index
                .search_with_external_vector_candidates_for_validation(
                    &candidates,
                    oracle.indexed_document_ids(),
                    "",
                    Some(&query_case.query_embedding),
                    SearchMode::Vector,
                    query_options(query_case, config)?,
                )
                .map_err(ProductionVectorQualificationError::from_error)?;
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

#[cfg(feature = "turbovec-oracle")]
struct TurbovecDifferentialOracle {
    index: turbovec::IdMapIndex,
    numeric_to_document_id: BTreeMap<u64, String>,
    document_to_numeric_id: BTreeMap<String, u64>,
    indexed_document_ids: BTreeSet<String>,
    dimension: usize,
}

#[cfg(feature = "turbovec-oracle")]
impl TurbovecDifferentialOracle {
    fn build(
        documents: &[SearchDocument],
        bit_width: usize,
    ) -> Result<Option<Self>, ProductionVectorQualificationError> {
        let mut vector_documents = documents
            .iter()
            .filter(|document| document.embedding.is_some())
            .collect::<Vec<_>>();
        vector_documents.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        let Some(first_embedding) = vector_documents
            .first()
            .and_then(|document| document.embedding.as_ref())
        else {
            return Ok(None);
        };
        let dimension = first_embedding.len();
        if dimension == 0 {
            return Err(ProductionVectorQualificationError::new(
                "upstream turbovec oracle rejected a zero-dimensional embedding",
            ));
        }

        let mut vectors = Vec::with_capacity(vector_documents.len().saturating_mul(dimension));
        let mut numeric_ids = Vec::with_capacity(vector_documents.len());
        let mut numeric_to_document_id = BTreeMap::new();
        let mut document_to_numeric_id = BTreeMap::new();
        for (position, document) in vector_documents.into_iter().enumerate() {
            let embedding = document
                .embedding
                .as_ref()
                .expect("vector documents were filtered above");
            if embedding.len() != dimension {
                return Err(ProductionVectorQualificationError::new(format!(
                    "upstream turbovec oracle embedding dimension mismatch for {}: expected {dimension}, got {}",
                    document.id,
                    embedding.len()
                )));
            }
            if !embedding.iter().all(|value| value.is_finite()) {
                return Err(ProductionVectorQualificationError::new(format!(
                    "upstream turbovec oracle rejected non-finite embedding for {}",
                    document.id
                )));
            }
            let numeric_id = u64::try_from(position.saturating_add(1)).map_err(|_| {
                ProductionVectorQualificationError::new(
                    "upstream turbovec oracle document count exceeds u64",
                )
            })?;
            numeric_ids.push(numeric_id);
            vectors.extend_from_slice(embedding);
            numeric_to_document_id.insert(numeric_id, document.id.clone());
            if document_to_numeric_id
                .insert(document.id.clone(), numeric_id)
                .is_some()
            {
                return Err(ProductionVectorQualificationError::new(format!(
                    "upstream turbovec oracle corpus contains duplicate document id {}",
                    document.id
                )));
            }
        }

        let mut index = turbovec::IdMapIndex::new(dimension, bit_width).map_err(|error| {
            ProductionVectorQualificationError::new(format!(
                "upstream turbovec oracle construct failed: {error}"
            ))
        })?;
        index
            .add_with_ids(&vectors, &numeric_ids)
            .map_err(|error| {
                ProductionVectorQualificationError::new(format!(
                    "upstream turbovec oracle build failed: {error}"
                ))
            })?;
        let indexed_document_ids = document_to_numeric_id.keys().cloned().collect();
        Ok(Some(Self {
            index,
            numeric_to_document_id,
            document_to_numeric_id,
            indexed_document_ids,
            dimension,
        }))
    }

    fn indexed_document_ids(&self) -> &BTreeSet<String> {
        &self.indexed_document_ids
    }

    fn search(
        &self,
        query_embedding: &[f32],
        candidate_limit: usize,
        allowed_document_ids: &BTreeSet<String>,
    ) -> Result<Vec<(String, f64)>, ProductionVectorQualificationError> {
        if query_embedding.len() != self.dimension {
            return Err(ProductionVectorQualificationError::new(format!(
                "upstream turbovec oracle query dimension mismatch: expected {}, got {}",
                self.dimension,
                query_embedding.len()
            )));
        }
        if !query_embedding.iter().all(|value| value.is_finite()) {
            return Err(ProductionVectorQualificationError::new(
                "upstream turbovec oracle query contains a non-finite coordinate",
            ));
        }
        let allowlist = allowed_document_ids
            .iter()
            .map(|document_id| {
                self.document_to_numeric_id
                    .get(document_id)
                    .copied()
                    .ok_or_else(|| {
                        ProductionVectorQualificationError::new(format!(
                            "upstream turbovec oracle is missing allowed document id {document_id}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if allowlist.is_empty() || candidate_limit == 0 {
            return Ok(Vec::new());
        }
        let effective_limit = candidate_limit.min(allowlist.len());
        let search_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.index.search_with_allowlist(
                query_embedding,
                effective_limit,
                Some(allowlist.as_slice()),
            )
        }));
        let (scores, numeric_ids) = search_result.map_err(|_| {
            ProductionVectorQualificationError::new(
                "upstream turbovec oracle search panicked while applying its allowlist",
            )
        })?;
        if scores.len() != numeric_ids.len() {
            return Err(ProductionVectorQualificationError::new(
                "upstream turbovec oracle returned mismatched score and id counts",
            ));
        }
        numeric_ids
            .into_iter()
            .zip(scores)
            .map(|(numeric_id, score)| {
                if !score.is_finite() {
                    return Err(ProductionVectorQualificationError::new(format!(
                        "upstream turbovec oracle returned a non-finite score for numeric id {numeric_id}"
                    )));
                }
                let document_id =
                    self.numeric_to_document_id
                        .get(&numeric_id)
                        .ok_or_else(|| {
                            ProductionVectorQualificationError::new(format!(
                                "upstream turbovec oracle returned unknown numeric id {numeric_id}"
                            ))
                        })?;
                Ok((document_id.clone(), f64::from(score).max(0.0)))
            })
            .collect()
    }
}

#[cfg(feature = "turbovec-oracle")]
fn validate_oracle_corpus(
    index: &SearchIndex,
    documents: &[SearchDocument],
) -> Result<(), ProductionVectorQualificationError> {
    if documents.len() != index.document_count() {
        return Err(ProductionVectorQualificationError::new(format!(
            "upstream turbovec oracle corpus has {} documents but the qualification projection has {}",
            documents.len(),
            index.document_count()
        )));
    }
    let mut seen = BTreeSet::new();
    for document in documents {
        if !seen.insert(document.id.as_str()) {
            return Err(ProductionVectorQualificationError::new(format!(
                "upstream turbovec oracle corpus contains duplicate document id {}",
                document.id
            )));
        }
        if index.document(&document.id) != Some(document) {
            return Err(ProductionVectorQualificationError::new(format!(
                "upstream turbovec oracle corpus does not match qualification document {}",
                document.id
            )));
        }
    }
    Ok(())
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

#[cfg(all(test, feature = "turbovec-oracle"))]
mod tests {
    use super::*;

    #[test]
    fn oracle_corpus_must_match_the_qualification_projection() {
        let mut index = SearchIndex::in_memory();
        let document = SearchDocument {
            id: "memory:a".to_string(),
            title: "A".to_string(),
            content: String::new(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::new(),
        };
        index.upsert(document.clone()).unwrap();

        validate_oracle_corpus(&index, std::slice::from_ref(&document)).unwrap();

        let mut mismatched = document;
        mismatched.embedding = Some(vec![0.0, 1.0]);
        let error = validate_oracle_corpus(&index, &[mismatched]).unwrap_err();
        assert!(error.to_string().contains("does not match"));
    }
}
