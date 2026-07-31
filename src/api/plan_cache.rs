use super::{
    optimizer_catalog, optimizer_config_from_database_config, statement_body, DatabaseConfig,
    QueryAccessControlContext, SharedState,
};
use crate::cypher;
use crate::error::Result;
use crate::optimizer::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerTrace, PhysicalPlan, PhysicalPlanRoot,
};
use crate::planner::{self, LogicalPlan, Predicate};
use crate::schema::Catalog;
use crate::store::GraphStore;
use crate::value::Value;
use skein_plan_cache::LfuCache;
pub use skein_plan_cache::PlanCacheStats;
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
    pub(super) cache: &'a SharedState<PlanCache>,
    pub(super) access_control: Option<&'a QueryAccessControlContext>,
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
    physical_root: PhysicalPlanRoot,
}

pub(super) type PlanCache = LfuCache<PlanCacheKey, CachedPlan>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PlanCacheKey {
    cypher: String,
    parameters: BTreeMap<String, Value>,
    graph_commit_epoch: u64,
    max_optimizer_groups: Option<usize>,
    access_control_policy_epoch: Option<u64>,
}

pub(super) fn optimized_query_plan_for(
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    cache_mode: PlanCacheMode,
    context: PlanCacheContext<'_>,
) -> Result<OptimizedQueryPlan> {
    if let Some(access_control) = context.access_control {
        context
            .config
            .runtime_capabilities
            .require(skein_core::RuntimeCapability::AccessControl)?;
        access_control.validate()?;
    }
    if let Some(capability) = required_runtime_capability(statement_body(statement)) {
        context.config.runtime_capabilities.require(capability)?;
    }
    let effective_max_optimizer_groups =
        optimizer_config_from_database_config(context.config).max_groups;
    let key = (cache_mode == PlanCacheMode::Use).then(|| PlanCacheKey {
        cypher: cypher_text.to_string(),
        parameters: parameters.clone(),
        graph_commit_epoch: context.store.commit_epoch(),
        max_optimizer_groups: context.config.max_optimizer_groups,
        access_control_policy_epoch: context
            .access_control
            .map(QueryAccessControlContext::policy_epoch),
    });
    if cache_mode == PlanCacheMode::Use {
        let key = key.as_ref().expect("cache key exists in use mode");
        if let Some(cached) = context.cache.borrow_mut().get(key) {
            let (physical_plan, mut trace) = cached.physical_root.into_parts();
            trace
                .decisions
                .push("plan cache hit: exact parameterized physical plan".to_string());
            record_access_control_plan_decision(&mut trace, context.access_control);
            return Ok(OptimizedQueryPlan {
                physical_plan,
                trace,
                plan_cache_lookup: PlanCacheLookup::Hit,
                configured_max_optimizer_groups: context.config.max_optimizer_groups,
                effective_max_optimizer_groups,
            });
        }
    }

    let mut logical = planner::plan_with_params(statement_body(statement), parameters)?;
    if let Some(access_control) = context.access_control {
        logical = apply_access_control_to_logical_plan(logical, access_control);
    }
    let logical_root = LogicalPlanRoot::new(logical);
    let physical_root = context.optimizer.optimize_root_with_catalog(
        &logical_root,
        &optimizer_catalog(context.catalog, &context.store.statistics()),
    );
    let (physical_plan, mut trace) = physical_root.clone().into_parts();
    record_access_control_plan_decision(&mut trace, context.access_control);
    if cache_mode == PlanCacheMode::Use {
        let key = key.expect("cache key exists in use mode");
        context.cache.borrow_mut().insert(
            key,
            CachedPlan {
                physical_root: physical_root.clone(),
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

fn required_runtime_capability(
    statement: &cypher::Statement,
) -> Option<skein_core::RuntimeCapability> {
    match statement {
        cypher::Statement::CreateFullTextIndex(_) => {
            Some(skein_core::RuntimeCapability::FullTextSearch)
        }
        cypher::Statement::VectorSearch(_) => Some(skein_core::RuntimeCapability::VectorSearch),
        cypher::Statement::MatchReturn(query) if query.vector_seed.is_some() => {
            Some(skein_core::RuntimeCapability::VectorSearch)
        }
        cypher::Statement::ProjectGraph(_) | cypher::Statement::GraphAlgorithm(_) => {
            Some(skein_core::RuntimeCapability::GraphAnalytics)
        }
        _ => None,
    }
}

fn record_access_control_plan_decision(
    trace: &mut OptimizerTrace,
    access_control: Option<&QueryAccessControlContext>,
) {
    if let Some(access_control) = access_control {
        trace.decisions.push(format!(
            "access control policy epoch {} bound to plan cache key",
            access_control.policy_epoch()
        ));
    }
}

fn apply_access_control_to_logical_plan(
    logical: LogicalPlan,
    access_control: &QueryAccessControlContext,
) -> LogicalPlan {
    match logical {
        LogicalPlan::NodeScan { variable, label } => LogicalPlan::Filter {
            predicate: access_control_node_predicate(&variable, access_control),
            input: Box::new(LogicalPlan::NodeScan { variable, label }),
        },
        LogicalPlan::NodeCartesianProduct { left, right } => LogicalPlan::NodeCartesianProduct {
            left: Box::new(apply_access_control_to_logical_plan(*left, access_control)),
            right: Box::new(apply_access_control_to_logical_plan(*right, access_control)),
        },
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input,
        } => {
            let target_predicate = access_control_node_predicate(&target_variable, access_control);
            LogicalPlan::Filter {
                predicate: target_predicate,
                input: Box::new(LogicalPlan::Expand {
                    source_variable,
                    source_label,
                    rel_variable,
                    rel_type,
                    rel_properties,
                    direction,
                    target_variable,
                    target_label,
                    min_hops,
                    max_hops,
                    optional,
                    input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
                }),
            }
        }
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate: _,
        } => LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate: Some(access_control_node_predicate("node", access_control)),
        },
        LogicalPlan::ShortestPath {
            source_variable,
            source_label,
            source_id,
            source_visibility_predicate: _,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            target_visibility_predicate: _,
            min_hops,
            max_hops,
            returns,
        } => LogicalPlan::ShortestPath {
            source_visibility_predicate: Some(access_control_node_predicate(
                &source_variable,
                access_control,
            )),
            target_visibility_predicate: Some(access_control_node_predicate(
                &target_variable,
                access_control,
            )),
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
        },
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        LogicalPlan::Project { items, input } => LogicalPlan::Project {
            items,
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        LogicalPlan::Sort { items, input } => LogicalPlan::Sort {
            items,
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(apply_access_control_to_logical_plan(*input, access_control)),
        },
        other => other,
    }
}

fn access_control_node_predicate(
    variable: &str,
    access_control: &QueryAccessControlContext,
) -> Predicate {
    Predicate::PropertyIn {
        variable: variable.to_string(),
        property: access_control.visibility_property().to_string(),
        values: access_control
            .allowed_visibility_values()
            .iter()
            .cloned()
            .map(Value::String)
            .collect(),
    }
}

pub(super) fn statement_uses_plan_cache(statement: &cypher::Statement) -> bool {
    match statement_body(statement) {
        cypher::Statement::MatchReturn(query) => query.vector_seed.is_none(),
        cypher::Statement::ShortestPathReturn(_)
        | cypher::Statement::MatchNodesReturn(_)
        | cypher::Statement::MatchOptionalRelationshipCountSum(_)
        | cypher::Statement::MatchThreadRepairStats(_)
        | cypher::Statement::GraphAlgorithm(_) => true,
        _ => false,
    }
}
