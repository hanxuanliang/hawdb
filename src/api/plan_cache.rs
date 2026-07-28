use super::{
    optimizer_catalog, optimizer_config_from_database_config, statement_body, DatabaseConfig,
};
use crate::cypher;
use crate::error::Result;
use crate::optimizer::{CascadesOptimizer, OptimizerTrace, PhysicalPlan};
use crate::planner;
use crate::schema::Catalog;
use crate::store::GraphStore;
use crate::value::Value;
use skein_plan_cache::LfuCache;
pub use skein_plan_cache::PlanCacheStats;
use std::cell::RefCell;
use std::collections::BTreeMap;

pub(crate) const DEFAULT_PLAN_CACHE_MAX_ENTRIES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheLookup {
    Hit,
    Miss,
    Bypass(PlanCacheBypassReason),
}

impl PlanCacheLookup {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass(_) => "bypass",
        }
    }

    pub fn bypass_reason(self) -> Option<PlanCacheBypassReason> {
        match self {
            Self::Bypass(reason) => Some(reason),
            Self::Hit | Self::Miss => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheBypassReason {
    MutationPlanning,
    StatementNotCacheable,
}

impl PlanCacheBypassReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MutationPlanning => "mutation_planning",
            Self::StatementNotCacheable => "statement_not_cacheable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PlanCacheMode {
    Use,
    Bypass(PlanCacheBypassReason),
}

pub(super) struct PlanCacheContext<'a> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a GraphStore,
    pub(super) optimizer: &'a CascadesOptimizer,
    pub(super) config: &'a DatabaseConfig,
    pub(super) cache: &'a RefCell<PlanCache>,
}

pub(super) struct OptimizedQueryPlan {
    pub(super) physical_plan: PhysicalPlan,
    pub(super) trace: OptimizerTrace,
    pub(super) plan_cache_lookup: PlanCacheLookup,
    pub(super) configured_max_optimizer_groups: Option<usize>,
    pub(super) effective_max_optimizer_groups: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct CachedPlan {
    physical_plan: PhysicalPlan,
    trace: OptimizerTrace,
}

pub(super) type PlanCache = LfuCache<PlanCacheKey, CachedPlan>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PlanCacheKey {
    cypher: String,
    parameters: BTreeMap<String, Value>,
    graph_commit_epoch: u64,
    max_optimizer_groups: Option<usize>,
}

pub(super) fn optimized_query_plan_for(
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    cache_mode: PlanCacheMode,
    context: PlanCacheContext<'_>,
) -> Result<OptimizedQueryPlan> {
    let effective_max_optimizer_groups =
        optimizer_config_from_database_config(context.config).max_groups;
    let key = (cache_mode == PlanCacheMode::Use).then(|| PlanCacheKey {
        cypher: cypher_text.to_string(),
        parameters: parameters.clone(),
        graph_commit_epoch: context.store.commit_epoch(),
        max_optimizer_groups: context.config.max_optimizer_groups,
    });
    if cache_mode == PlanCacheMode::Use {
        let key = key.as_ref().expect("cache key exists in use mode");
        if let Some(cached) = context.cache.borrow_mut().get(key) {
            let mut trace = cached.trace;
            trace
                .decisions
                .push("plan cache hit: exact parameterized physical plan".to_string());
            return Ok(OptimizedQueryPlan {
                physical_plan: cached.physical_plan,
                trace,
                plan_cache_lookup: PlanCacheLookup::Hit,
                configured_max_optimizer_groups: context.config.max_optimizer_groups,
                effective_max_optimizer_groups,
            });
        }
    }

    let logical = planner::plan_with_params(statement_body(statement), parameters)?;
    let (physical_plan, trace) = context.optimizer.optimize_with_catalog(
        &logical,
        &optimizer_catalog(context.catalog, &context.store.statistics()),
    );
    let mut trace = trace;
    if cache_mode == PlanCacheMode::Use {
        let key = key.expect("cache key exists in use mode");
        context.cache.borrow_mut().insert(
            key,
            CachedPlan {
                physical_plan: physical_plan.clone(),
                trace: trace.clone(),
            },
        );
        trace
            .decisions
            .push("plan cache miss: optimized exact parameterized physical plan".to_string());
        return Ok(OptimizedQueryPlan {
            physical_plan,
            trace,
            plan_cache_lookup: PlanCacheLookup::Miss,
            configured_max_optimizer_groups: context.config.max_optimizer_groups,
            effective_max_optimizer_groups,
        });
    } else if let PlanCacheMode::Bypass(reason) = cache_mode {
        context.cache.borrow_mut().record_bypass();
        trace
            .decisions
            .push(format!("plan cache bypass: {}", reason.as_str()));
        return Ok(OptimizedQueryPlan {
            physical_plan,
            trace,
            plan_cache_lookup: PlanCacheLookup::Bypass(reason),
            configured_max_optimizer_groups: context.config.max_optimizer_groups,
            effective_max_optimizer_groups,
        });
    }
    unreachable!("plan cache mode must be either use or bypass")
}

pub(super) fn statement_uses_plan_cache(statement: &cypher::Statement) -> bool {
    matches!(
        statement_body(statement),
        cypher::Statement::MatchReturn(_)
            | cypher::Statement::ShortestPathReturn(_)
            | cypher::Statement::MatchNodesReturn(_)
            | cypher::Statement::MatchOptionalRelationshipCountSum(_)
            | cypher::Statement::MatchThreadRepairStats(_)
            | cypher::Statement::GraphAlgorithm(_)
    )
}
