use super::super::PhysicalPlan;
use skein_plan::LogicalPlan;

use super::{access, ddl, mutation, procedure, traversal};

pub(super) fn lower_simple_logical(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    ddl::lower(logical)
        .or_else(|| mutation::lower(logical))
        .or_else(|| access::lower(logical))
        .or_else(|| traversal::lower(logical))
        .or_else(|| procedure::lower(logical))
}
