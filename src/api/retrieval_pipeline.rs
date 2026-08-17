use super::{KnowledgeRetrievalPipelineReport, KnowledgeRetrievalStage};
use crate::error::{Result, SkeinError};
use skein_executor::{
    QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger, QueryMemoryLedgerSnapshot,
};
use std::num::NonZeroUsize;

const STAGE_ORDER: [KnowledgeRetrievalStage; 6] = [
    KnowledgeRetrievalStage::SearchCandidate,
    KnowledgeRetrievalStage::MetadataFilter,
    KnowledgeRetrievalStage::AuthorizedGraphExpand,
    KnowledgeRetrievalStage::Rerank,
    KnowledgeRetrievalStage::TopK,
    KnowledgeRetrievalStage::CanonicalHydration,
];

pub(super) struct KnowledgeRetrievalPipelineBudget {
    ledger: QueryMemoryLedger,
    working: QueryMemoryLease,
    result: QueryMemoryLease,
    result_payload_budget: usize,
    result_payload_bytes: usize,
    stages: Vec<KnowledgeRetrievalStage>,
}

impl KnowledgeRetrievalPipelineBudget {
    pub(super) fn new(
        query_memory_budget: NonZeroUsize,
        result_payload_budget: usize,
    ) -> Result<Self> {
        if result_payload_budget == 0 {
            return Err(SkeinError::Execution(
                "knowledge retrieval requires a positive result payload budget".to_string(),
            ));
        }
        let ledger = QueryMemoryLedger::new(query_memory_budget);
        let working = ledger
            .account(
                QueryMemoryClass::BlockingState,
                "knowledge_retrieval_working",
                query_memory_budget,
            )
            .reserve(0)?;
        let result = ledger
            .account(
                QueryMemoryClass::ResultMaterialization,
                "knowledge_retrieval_result",
                query_memory_budget,
            )
            .reserve(0)?;
        Ok(Self {
            ledger,
            working,
            result,
            result_payload_budget,
            result_payload_bytes: 0,
            stages: Vec::with_capacity(STAGE_ORDER.len()),
        })
    }

    pub(super) fn enter(&mut self, stage: KnowledgeRetrievalStage) -> Result<()> {
        let expected = STAGE_ORDER.get(self.stages.len()).copied();
        if expected != Some(stage) {
            return Err(SkeinError::Execution(format!(
                "knowledge retrieval stage order violation: expected {}, got {}",
                expected.map_or("<complete>", KnowledgeRetrievalStage::as_str),
                stage.as_str(),
            )));
        }
        self.stages.push(stage);
        Ok(())
    }

    pub(super) fn retain_working(&mut self, bytes: usize) -> Result<()> {
        self.working.grow(bytes)
    }

    pub(super) fn retain_result(
        &mut self,
        memory_bytes: usize,
        payload_bytes: usize,
    ) -> Result<()> {
        let next_payload = self
            .result_payload_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| {
                SkeinError::Execution(
                    "knowledge retrieval result payload accounting overflow".to_string(),
                )
            })?;
        if next_payload > self.result_payload_budget {
            return Err(SkeinError::Execution(format!(
                "knowledge retrieval result uses {next_payload} payload bytes, exceeding max_read_result_payload_bytes {}",
                self.result_payload_budget
            )));
        }
        self.result.grow(memory_bytes)?;
        self.result_payload_bytes = next_payload;
        Ok(())
    }

    pub(super) fn finish(
        self,
        graph_snapshot_commit_epoch: u64,
        canonical_identity_filtered_out_count: usize,
        canonical_output_hydrated_node_count: usize,
        canonical_output_hydrated_candidate_count: usize,
        metadata_filter_authorized_graph_expansion: bool,
    ) -> Result<KnowledgeRetrievalPipelineReport> {
        if self.stages.as_slice() != STAGE_ORDER {
            return Err(SkeinError::Execution(format!(
                "knowledge retrieval pipeline completed after {} of {} required stages",
                self.stages.len(),
                STAGE_ORDER.len()
            )));
        }
        let QueryMemoryLedgerSnapshot {
            budget_bytes,
            peak_bytes,
            ..
        } = self.ledger.snapshot();
        Ok(KnowledgeRetrievalPipelineReport {
            stages: self.stages,
            graph_snapshot_commit_epoch,
            query_memory_budget_bytes: budget_bytes,
            peak_tracked_memory_bytes: peak_bytes,
            result_payload_budget_bytes: self.result_payload_budget,
            result_payload_bytes: self.result_payload_bytes,
            canonical_identity_filtered_out_count,
            canonical_output_hydrated_node_count,
            canonical_output_hydrated_candidate_count,
            canonical_output_hydration_after_top_k: true,
            metadata_filter_authorized_graph_expansion,
        })
    }
}
