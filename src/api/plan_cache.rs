use crate::optimizer::{OptimizerTrace, PhysicalPlan};
use crate::value::Value;
use skein_plan_cache::LfuCache;
pub use skein_plan_cache::PlanCacheStats;
use std::collections::BTreeMap;

pub(crate) const DEFAULT_PLAN_CACHE_MAX_ENTRIES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PlanCacheKey {
    pub(crate) cypher: String,
    pub(crate) parameters: BTreeMap<String, Value>,
    pub(crate) graph_commit_epoch: u64,
    pub(crate) max_optimizer_groups: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CachedPlan {
    pub(crate) physical_plan: PhysicalPlan,
    pub(crate) trace: OptimizerTrace,
}

pub(crate) type PlanCache = LfuCache<PlanCacheKey, CachedPlan>;
