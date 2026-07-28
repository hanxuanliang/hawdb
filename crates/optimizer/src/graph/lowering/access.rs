use super::super::PhysicalPlan;
use skein_plan::LogicalPlan;

pub(super) fn lower(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::NodeScan { variable, label } => Some(PhysicalPlan::SeqNodeScan {
            variable: variable.clone(),
            label: label.clone(),
        }),
        _ => None,
    }
}
