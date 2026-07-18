#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanCost {
    pub estimated_rows: u64,
    pub cost: u64,
}
