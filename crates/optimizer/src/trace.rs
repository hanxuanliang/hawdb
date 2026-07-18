use crate::cost::PlanCost;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerConfig {
    pub max_groups: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self { max_groups: 128 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerTrace {
    pub groups: usize,
    pub selected_plan: String,
    pub selected_plan_fingerprint: String,
    pub selected_plan_cost: PlanCost,
    pub warnings: Vec<String>,
    pub decisions: Vec<String>,
}
