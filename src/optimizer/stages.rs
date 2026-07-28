use skein_optimizer::{ApplyOrder, OptimizationStage};

pub(super) const LOGICAL_GROUPING_STAGE: OptimizationStage =
    OptimizationStage::new("logical_grouping", ApplyOrder::Once);
pub(super) const PHYSICAL_SEARCH_STAGE: OptimizationStage =
    OptimizationStage::new("physical_search", ApplyOrder::BottomUp);
pub(super) const ACCESS_PATH_SELECTION_STAGE: OptimizationStage =
    OptimizationStage::new("access_path_selection", ApplyOrder::BottomUp);
pub(super) const DIRECT_PHYSICAL_FALLBACK_STAGE: OptimizationStage =
    OptimizationStage::new("direct_physical_fallback", ApplyOrder::Once);
pub(super) const SELECTED_PLAN_COSTING_STAGE: OptimizationStage =
    OptimizationStage::new("selected_plan_costing", ApplyOrder::Once);
