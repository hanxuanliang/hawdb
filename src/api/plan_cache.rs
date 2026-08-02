use super::{
    optimizer_catalog, optimizer_config_from_database_config, statement_body, DatabaseConfig,
    QueryAccessControlContext, SharedState,
};
use crate::cypher;
use crate::error::{Result, SkeinError};
use crate::optimizer::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerCatalog, OptimizerSearchDirective, OptimizerTrace,
    PhysicalPlan,
};
use crate::planner::{self, LogicalPlan, Predicate};
use crate::schema::{Catalog, GraphStatistics, IndexKind};
use crate::store::GraphStore;
use crate::value::Value;
pub use skein_plan_cache::PlanCacheStats;
use skein_plan_cache::{
    bind_physical_plan_parameters, parameterize_logical_plan, LfuCache, PlanParameterCacheKey,
};
use std::collections::BTreeMap;
use std::sync::Arc;

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
    OptimizerDirective,
    StatementNotCacheable,
}

impl PlanCacheBypassReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MutationPlanning => "mutation_planning",
            Self::OptimizerDirective => "optimizer_directive",
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
    pub(super) planning_cache: &'a SharedState<OptimizerPlanningCache>,
    pub(super) access_control: Option<&'a QueryAccessControlContext>,
    pub(super) optimizer_search: OptimizerSearchDirective,
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
    physical_template: PhysicalPlan,
    trace: OptimizerTrace,
    has_parameter_slots: bool,
}

pub(super) type PlanCache = LfuCache<PlanCacheKey, CachedPlan>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PlanCacheKey {
    cypher: String,
    parameters: PlanParameterCacheKey,
    environment: OptimizerEnvironmentKey,
    max_optimizer_groups: Option<usize>,
    access_control_policy_epoch: Option<u64>,
}

impl PlanCacheKey {
    fn matches(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        environment: &OptimizerEnvironmentKey,
        max_optimizer_groups: Option<usize>,
        access_control_policy_epoch: Option<u64>,
    ) -> bool {
        self.cypher == cypher
            && &self.environment == environment
            && self.max_optimizer_groups == max_optimizer_groups
            && self.access_control_policy_epoch == access_control_policy_epoch
            && self.parameters.matches(parameters)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OptimizerEnvironmentKey {
    schema: OptimizerSchemaKey,
    statistics_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OptimizerSchemaKey {
    labels: Vec<String>,
    relationship_types: Vec<String>,
    equality_indexes: Vec<(String, String)>,
    range_indexes: Vec<(String, String)>,
    full_text_indexes: Vec<(String, String)>,
    composite_indexes: Vec<(String, Vec<String>)>,
}

#[derive(Debug, Clone)]
struct CachedOptimizerCatalog {
    environment: OptimizerEnvironmentKey,
    catalog: Arc<OptimizerCatalog>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct OptimizerPlanningCache {
    statistics: Option<Arc<GraphStatistics>>,
    catalog: Option<CachedOptimizerCatalog>,
}

struct OptimizerCatalogAccess {
    environment: OptimizerEnvironmentKey,
    catalog: Arc<OptimizerCatalog>,
    decisions: Vec<String>,
}

impl OptimizerSchemaKey {
    fn from_catalog(catalog: &Catalog) -> Self {
        let labels = catalog.labels().map(|label| label.name.clone()).collect();
        let relationship_types = catalog
            .rel_types()
            .map(|rel_type| rel_type.name.clone())
            .collect();
        let mut equality_indexes = Vec::new();
        let mut range_indexes = Vec::new();
        let mut full_text_indexes = Vec::new();
        for index in catalog.property_indexes() {
            let Some(label) = catalog.label_name(index.label_id) else {
                continue;
            };
            let descriptor = (label.to_string(), index.property.clone());
            match index.kind {
                IndexKind::Equality => equality_indexes.push(descriptor),
                IndexKind::Range => range_indexes.push(descriptor),
                IndexKind::FullText => full_text_indexes.push(descriptor),
            }
        }
        let composite_indexes = catalog
            .composite_property_indexes()
            .filter_map(|index| {
                catalog
                    .label_name(index.label_id)
                    .map(|label| (label.to_string(), index.properties.clone()))
            })
            .collect();
        Self {
            labels,
            relationship_types,
            equality_indexes,
            range_indexes,
            full_text_indexes,
            composite_indexes,
        }
    }
}

impl OptimizerPlanningCache {
    fn environment_hint(catalog: &Catalog, store: &GraphStore) -> OptimizerEnvironmentKey {
        OptimizerEnvironmentKey {
            schema: OptimizerSchemaKey::from_catalog(catalog),
            statistics_epoch: store.commit_epoch(),
        }
    }

    fn optimizer_catalog(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
    ) -> OptimizerCatalogAccess {
        let mut decisions = Vec::new();
        let refresh_statistics = self
            .statistics
            .as_ref()
            .is_none_or(|statistics| statistics_refresh_required(statistics, store));
        if refresh_statistics {
            let statistics = Arc::new(store.statistics());
            decisions.push(format!(
                "optimizer statistics cache refresh: statistics_epoch={} graph_commit_epoch={}",
                statistics.computed_at_commit_epoch,
                store.commit_epoch()
            ));
            self.statistics = Some(statistics);
            self.catalog = None;
        } else if let Some(statistics) = &self.statistics {
            decisions.push(format!(
                "optimizer statistics cache hit: statistics_epoch={} graph_commit_epoch={}",
                statistics.computed_at_commit_epoch,
                store.commit_epoch()
            ));
        }

        let statistics = self
            .statistics
            .as_ref()
            .expect("optimizer statistics exist after refresh check");
        let environment = OptimizerEnvironmentKey {
            schema: OptimizerSchemaKey::from_catalog(catalog),
            statistics_epoch: statistics.computed_at_commit_epoch,
        };
        if let Some(cached) = &self.catalog
            && cached.environment == environment
        {
            decisions.push(format!(
                "optimizer catalog cache hit: statistics_epoch={} graph_commit_epoch={}",
                statistics.computed_at_commit_epoch,
                store.commit_epoch()
            ));
            return OptimizerCatalogAccess {
                environment,
                catalog: cached.catalog.clone(),
                decisions,
            };
        }

        let optimized = Arc::new(optimizer_catalog(catalog, statistics));
        decisions.push(format!(
            "optimizer catalog cache refresh: statistics_epoch={} graph_commit_epoch={}",
            statistics.computed_at_commit_epoch,
            store.commit_epoch()
        ));
        self.catalog = Some(CachedOptimizerCatalog {
            environment: environment.clone(),
            catalog: optimized.clone(),
        });
        OptimizerCatalogAccess {
            environment,
            catalog: optimized,
            decisions,
        }
    }
}

fn statistics_refresh_required(statistics: &GraphStatistics, store: &GraphStore) -> bool {
    statistics.computed_at_commit_epoch != store.commit_epoch()
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
    let access_control_policy_epoch = context
        .access_control
        .map(QueryAccessControlContext::policy_epoch);
    let environment_hint = (cache_mode == PlanCacheMode::Use)
        .then(|| OptimizerPlanningCache::environment_hint(context.catalog, context.store));
    if cache_mode == PlanCacheMode::Use {
        let environment = environment_hint
            .as_ref()
            .expect("optimizer environment exists in use mode");
        let cached = context.cache.borrow_mut().get_matching(|key| {
            key.matches(
                cypher_text,
                parameters,
                environment,
                context.config.max_optimizer_groups,
                access_control_policy_epoch,
            )
        });
        if let Some(cached) = cached {
            let physical_plan = bind_physical_plan_parameters(
                &cached.physical_template,
                parameters,
                cached.has_parameter_slots,
            )?;
            let mut trace = cached.trace;
            refresh_materialized_plan_trace(&mut trace, &physical_plan);
            trace
                .decisions
                .push("plan cache hit: parameterized physical plan template".to_string());
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
    let parameterized = (cache_mode == PlanCacheMode::Use)
        .then(|| parameterize_logical_plan(statement_body(statement), parameters))
        .transpose()?;
    let mut logical = if let Some(parameterized) = &parameterized {
        parameterized.logical().clone()
    } else {
        planner::plan_with_params(statement_body(statement), parameters)?
    };
    if let Some(access_control) = context.access_control {
        logical = apply_access_control_to_logical_plan(logical, access_control);
    }
    let mut key = parameterized.as_ref().map(|parameterized| PlanCacheKey {
        cypher: cypher_text.to_string(),
        parameters: parameterized.cache_key().clone(),
        environment: environment_hint
            .clone()
            .expect("optimizer environment exists for a parameterized plan"),
        max_optimizer_groups: context.config.max_optimizer_groups,
        access_control_policy_epoch,
    });

    let catalog_access = context
        .planning_cache
        .borrow_mut()
        .optimizer_catalog(context.catalog, context.store);
    let logical_root = LogicalPlanRoot::new(logical);
    let physical_root = context
        .optimizer
        .optimize_root_with_catalog_and_directive(
            &logical_root,
            &catalog_access.catalog,
            context.optimizer_search,
        )
        .map_err(|error| {
            SkeinError::Execution(format!(
                "optimizer search directive could not be honored: {error}"
            ))
        })?;
    let (physical_template, mut trace) = physical_root.into_parts();
    trace.decisions.extend(catalog_access.decisions);
    let has_parameter_slots = parameterized
        .as_ref()
        .is_some_and(|parameterized| parameterized.slot_count() > 0);
    let physical_plan =
        bind_physical_plan_parameters(&physical_template, parameters, has_parameter_slots)?;
    refresh_materialized_plan_trace(&mut trace, &physical_plan);
    if let Some(parameterized) = &parameterized {
        trace.decisions.push(format!(
            "parameterized plan template: slots={} exact_variants={}",
            parameterized.slot_count(),
            parameterized.exact_variant_count()
        ));
    }
    let cached_trace = trace.clone();
    record_access_control_plan_decision(&mut trace, context.access_control);
    if cache_mode == PlanCacheMode::Use {
        let mut key = key.take().expect("cache key exists in use mode");
        key.environment = catalog_access.environment;
        context.cache.borrow_mut().insert(
            key,
            CachedPlan {
                physical_template,
                trace: cached_trace,
                has_parameter_slots,
            },
        );
        trace
            .decisions
            .push("plan cache miss: optimized parameterized physical plan template".to_string());
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

fn refresh_materialized_plan_trace(trace: &mut OptimizerTrace, physical_plan: &PhysicalPlan) {
    trace.selected_plan = physical_plan.explain(0);
    trace.selected_plan_fingerprint = physical_plan.fingerprint();
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
