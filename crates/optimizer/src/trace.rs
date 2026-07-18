use crate::cost::PlanCost;
use std::collections::BTreeMap;

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
    pub selected_plan_operator_counts: BTreeMap<String, usize>,
    pub selected_plan_class_counts: BTreeMap<String, usize>,
    pub warnings: Vec<String>,
    pub decisions: Vec<String>,
}
