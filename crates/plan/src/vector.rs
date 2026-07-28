#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorCandidateSource {
    Scalar,
    Ann,
    Quantized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchLogicalPlan {
    pub embedding_dimension: usize,
    pub filter_fields: Vec<String>,
    pub candidate_source: VectorCandidateSource,
    pub candidate_limit: usize,
    pub top_k: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorPhysicalPlan {
    Filter {
        fields: Vec<String>,
    },
    VectorCandidateScan {
        source: VectorCandidateSource,
        embedding_dimension: usize,
        candidate_limit: usize,
        input: Box<VectorPhysicalPlan>,
    },
    RawVectorRerank {
        embedding_dimension: usize,
        input: Box<VectorPhysicalPlan>,
    },
    TopK {
        limit: usize,
        input: Box<VectorPhysicalPlan>,
    },
}

impl VectorPhysicalPlan {
    pub fn operator_name(&self) -> &'static str {
        match self {
            Self::Filter { .. } => "Filter",
            Self::VectorCandidateScan { .. } => "VectorCandidateScan",
            Self::RawVectorRerank { .. } => "RawVectorRerank",
            Self::TopK { .. } => "TopK",
        }
    }

    pub fn input(&self) -> Option<&Self> {
        match self {
            Self::Filter { .. } => None,
            Self::VectorCandidateScan { input, .. }
            | Self::RawVectorRerank { input, .. }
            | Self::TopK { input, .. } => Some(input),
        }
    }

    pub fn operator_pipeline(&self) -> Vec<&'static str> {
        let mut operators = Vec::new();
        let mut current = Some(self);
        while let Some(plan) = current {
            operators.push(plan.operator_name());
            current = plan.input();
        }
        operators.reverse();
        operators
    }
}
