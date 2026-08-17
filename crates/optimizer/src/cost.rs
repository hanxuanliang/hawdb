#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanCost {
    pub estimated_rows: u64,
    pub cost: u64,
}

impl PlanCost {
    pub fn with_cardinality_floor(self) -> Self {
        Self {
            estimated_rows: self.estimated_rows.max(1),
            ..self
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanCostBreakdown {
    pub estimated_rows: u64,
    pub cost: u64,
    pub cpu: u64,
    pub random_io: u64,
    pub sequential_io: u64,
    pub output_rows: u64,
}

impl PlanCostBreakdown {
    pub fn with_cardinality_floor(self) -> Self {
        Self {
            estimated_rows: self.estimated_rows.max(1),
            ..self
        }
    }

    pub fn new(
        estimated_rows: u64,
        cpu: u64,
        random_io: u64,
        sequential_io: u64,
        output_rows: u64,
    ) -> Self {
        Self {
            estimated_rows,
            cost: cpu
                .saturating_add(random_io)
                .saturating_add(sequential_io)
                .saturating_add(output_rows),
            cpu,
            random_io,
            sequential_io,
            output_rows,
        }
        .with_cardinality_floor()
    }

    pub fn from_scalar(cost: PlanCost) -> Self {
        Self::new(cost.estimated_rows, cost.cost, 0, 0, 0)
    }

    pub fn with_cpu(self, estimated_rows: u64, cpu: u64, output_rows: u64) -> Self {
        Self::new(
            estimated_rows,
            self.cpu.saturating_add(cpu),
            self.random_io,
            self.sequential_io,
            self.output_rows.saturating_add(output_rows),
        )
    }

    pub fn with_random_io(self, estimated_rows: u64, random_io: u64, output_rows: u64) -> Self {
        Self::new(
            estimated_rows,
            self.cpu,
            self.random_io.saturating_add(random_io),
            self.sequential_io,
            self.output_rows.saturating_add(output_rows),
        )
    }

    pub fn combine_with_cpu(
        left: Self,
        right: Self,
        estimated_rows: u64,
        cpu: u64,
        output_rows: u64,
    ) -> Self {
        Self::new(
            estimated_rows,
            left.cpu.saturating_add(right.cpu).saturating_add(cpu),
            left.random_io.saturating_add(right.random_io),
            left.sequential_io.saturating_add(right.sequential_io),
            left.output_rows
                .saturating_add(right.output_rows)
                .saturating_add(output_rows),
        )
    }

    pub fn as_plan_cost(self) -> PlanCost {
        PlanCost {
            estimated_rows: self.estimated_rows,
            cost: self.cost,
        }
        .with_cardinality_floor()
    }
}

#[cfg(test)]
mod tests {
    use super::{PlanCost, PlanCostBreakdown};

    #[test]
    fn cost_breakdown_sums_stable_components() {
        let cost = PlanCostBreakdown::new(7, 10, 20, 30, 40);

        assert_eq!(cost.estimated_rows, 7);
        assert_eq!(cost.cost, 100);
        assert_eq!(
            cost.as_plan_cost(),
            PlanCost {
                estimated_rows: 7,
                cost: 100,
            }
        );
    }

    #[test]
    fn cost_breakdown_combines_child_costs_with_parent_cpu() {
        let left = PlanCostBreakdown::new(2, 3, 5, 7, 11);
        let right = PlanCostBreakdown::new(13, 17, 19, 23, 29);

        let combined = PlanCostBreakdown::combine_with_cpu(left, right, 26, 31, 37);

        assert_eq!(combined.estimated_rows, 26);
        assert_eq!(combined.cpu, 51);
        assert_eq!(combined.random_io, 24);
        assert_eq!(combined.sequential_io, 30);
        assert_eq!(combined.output_rows, 77);
        assert_eq!(combined.cost, 182);
    }

    #[test]
    fn cost_breakdown_preserves_a_non_zero_cardinality_floor() {
        let cost = PlanCostBreakdown::new(0, 0, 0, 0, 0);

        assert_eq!(cost.estimated_rows, 1);
        assert_eq!(cost.as_plan_cost().estimated_rows, 1);
    }

    #[test]
    fn scalar_cost_preserves_a_non_zero_cardinality_floor() {
        let cost = PlanCost {
            estimated_rows: 0,
            cost: 7,
        }
        .with_cardinality_floor();

        assert_eq!(cost.estimated_rows, 1);
        assert_eq!(cost.cost, 7);
    }
}
