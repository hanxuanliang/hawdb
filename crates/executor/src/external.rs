//! Host-provided physical read operators.

use crate::VectorExecutionReport;
use skein_core::{Result, SkeinError};
use skein_plan::VectorPhysicalPlan;
use std::collections::BTreeMap;

pub struct VectorSeedExecutionRequest<'a> {
    pub embedding: &'a [f32],
    pub metadata_filters: &'a BTreeMap<String, String>,
    pub vector_plan: &'a VectorPhysicalPlan,
}

pub struct VectorSeedExecutionRow {
    pub id: String,
    pub external_id: Option<String>,
    pub score: f64,
}

pub struct VectorSeedExecutionOutput {
    pub rows: Vec<VectorSeedExecutionRow>,
    pub report: VectorExecutionReport,
}

pub trait ExternalReadOperator {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput>;
}

#[doc(hidden)]
pub struct NoExternalReadOperator;

impl ExternalReadOperator for NoExternalReadOperator {
    fn execute_vector_seed(
        &mut self,
        _request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        Err(SkeinError::Execution(
            "vector search capability is unavailable without a search projection".to_string(),
        ))
    }
}
