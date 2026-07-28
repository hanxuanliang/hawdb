use super::super::PhysicalPlan;
use skein_plan::LogicalPlan;

pub(super) fn lower(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::ShortestPath {
            source_variable,
            source_label,
            source_id,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            min_hops,
            max_hops,
            returns,
        } => Some(PhysicalPlan::ShortestPathExec {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_id: source_id.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_id: target_id.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            returns: returns.clone(),
        }),
        _ => None,
    }
}
