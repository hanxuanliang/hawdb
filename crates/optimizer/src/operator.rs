//! Compatibility re-exports for optimizer operator metadata.
//!
//! New code should import logical operators from `logical` and physical
//! operators from `physical` so implementation rules stay distinct from logical
//! rewrites.

pub use crate::logical::{LogicalPlanClass, LogicalPlanKind, LogicalPlanNode};
pub use crate::physical::PhysicalPlanNode as PlanNode;
pub use crate::physical::{
    plan_class_counts, plan_operator_counts, visit_plan, PhysicalPlanClass, PhysicalPlanKind,
    PhysicalPlanNode, PlanChildren,
};
