use super::{costing, properties, OptimizerCatalog, PhysicalPlan};
use crate::{plan_class_counts, plan_operator_counts, SelectedPlanTrace};

pub(super) fn selected_plan_trace(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> SelectedPlanTrace {
    let selected_plan_cost = costing::estimate_physical_plan_cost(plan, catalog);
    SelectedPlanTrace {
        explain: plan.explain(0),
        fingerprint: plan.fingerprint(),
        cost: selected_plan_cost,
        cost_breakdown: costing::estimate_physical_plan_cost_breakdown(plan, catalog),
        properties: properties::selected_plan_properties(plan),
        operator_counts: plan_operator_counts(plan),
        class_counts: plan_class_counts(plan),
    }
}
