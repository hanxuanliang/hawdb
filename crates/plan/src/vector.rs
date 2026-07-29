#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorCandidateSource {
    Scalar,
    Ann,
    Quantized,
}

impl VectorCandidateSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Ann => "ann",
            Self::Quantized => "quantized",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchLogicalPlan {
    pub embedding_dimension: usize,
    pub filter_fields: Vec<String>,
    pub residual_filter_fields: Vec<String>,
    pub initial_candidate_limit: usize,
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
    ResidualFilter {
        fields: Vec<String>,
        initial_candidate_limit: usize,
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
            Self::ResidualFilter { .. } => "ResidualFilter",
            Self::RawVectorRerank { .. } => "RawVectorRerank",
            Self::TopK { .. } => "TopK",
        }
    }

    pub fn input(&self) -> Option<&Self> {
        match self {
            Self::Filter { .. } => None,
            Self::VectorCandidateScan { input, .. }
            | Self::ResidualFilter { input, .. }
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

    pub fn explain_summary(&self) -> String {
        match self {
            Self::TopK { limit, input } => format!(
                "pipeline={} top_k={limit} {}",
                self.operator_pipeline().join("->"),
                input.explain_details()
            ),
            _ => format!("pipeline={}", self.operator_pipeline().join("->")),
        }
    }

    pub fn fingerprint(&self) -> String {
        match self {
            Self::Filter { fields } => format!("Filter({})", fields.join(",")),
            Self::VectorCandidateScan {
                source,
                embedding_dimension,
                candidate_limit,
                input,
            } => format!(
                "VectorCandidateScan({}:{}:{}:{})",
                source.as_str(),
                embedding_dimension,
                candidate_limit,
                input.fingerprint()
            ),
            Self::ResidualFilter {
                fields,
                initial_candidate_limit,
                input,
            } => format!(
                "ResidualFilter({}:{}:{})",
                fields.join(","),
                initial_candidate_limit,
                input.fingerprint()
            ),
            Self::RawVectorRerank {
                embedding_dimension,
                input,
            } => format!(
                "RawVectorRerank({embedding_dimension}:{})",
                input.fingerprint()
            ),
            Self::TopK { limit, input } => {
                format!("TopK({limit}:{})", input.fingerprint())
            }
        }
    }

    fn explain_details(&self) -> String {
        match self {
            Self::RawVectorRerank {
                embedding_dimension,
                input,
            } => format!(
                "dimension={embedding_dimension} {}",
                input.explain_details()
            ),
            Self::ResidualFilter {
                fields,
                initial_candidate_limit,
                input,
            } => format!(
                "residual_fields={fields:?} initial_candidates={initial_candidate_limit} {}",
                input.explain_details()
            ),
            Self::VectorCandidateScan {
                source,
                candidate_limit,
                input,
                ..
            } => format!(
                "source={} candidates={candidate_limit} {}",
                source.as_str(),
                input.explain_details()
            ),
            Self::Filter { fields } => format!("filter_fields={fields:?}"),
            Self::TopK { input, .. } => input.explain_details(),
        }
    }
}
