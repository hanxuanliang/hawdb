use super::super::PhysicalPlan;
use crate::{plan_vector_search, OptimizerContext};
use skein_plan::{LogicalPlan, VectorCandidateSource, VectorSearchLogicalPlan};

pub(super) fn lower(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => Some(PhysicalPlan::ProjectGraph {
            name: name.clone(),
            node_labels: node_labels.clone(),
            rel_types: rel_types.clone(),
        }),
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
        } => Some(PhysicalPlan::GraphAlgorithm {
            algorithm: *algorithm,
            graph_name: graph_name.clone(),
            options: *options,
            score_column: score_column.clone(),
        }),
        LogicalPlan::VectorSeed {
            embedding_parameter,
            embedding_dimension,
            top_k,
        } => {
            let logical = VectorSearchLogicalPlan {
                embedding_dimension: *embedding_dimension,
                filter_fields: Vec::new(),
                residual_filter_fields: Vec::new(),
                initial_candidate_limit: *top_k,
                candidate_source: VectorCandidateSource::Scalar,
                candidate_limit: *top_k,
                top_k: *top_k,
            };
            Some(PhysicalPlan::VectorSeedScan {
                embedding_parameter: embedding_parameter.clone(),
                vector_plan: plan_vector_search(&logical, &OptimizerContext::default())
                    .ok()?
                    .plan,
            })
        }
        LogicalPlan::ThreadRepairStats {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => Some(PhysicalPlan::ThreadRepairStatsExec {
            label: label.clone(),
            identity_label: identity_label.clone(),
            identity_ref_property: identity_ref_property.clone(),
            thread_id_property: thread_id_property.clone(),
            message_rel_type: message_rel_type.clone(),
            message_label: message_label.clone(),
            memory_rel_type: memory_rel_type.clone(),
            memory_label: memory_label.clone(),
        }),
        _ => None,
    }
}
