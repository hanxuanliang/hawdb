use crate::analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraph, ProjectionLayout, ProjectionMemoryBudget,
};
use crate::cypher::RelationshipDirection;
use crate::error::{Result, SkeinError};
use crate::optimizer::PhysicalPlan;
use crate::planner::{
    AggregateFunction, AggregateTarget, Aggregation, CoalesceDifferenceProjectionTerm,
    ComparisonOp, DatePart, GraphAlgorithmKind, Predicate, Projection, ProjectionExpression,
    RelationshipCountFilter, RelationshipCountLeg, RelationshipOnCreateValue,
    SetNodePropertiesReturnMode, SetValue, ShortestPathProjection,
    ShortestPathProjectionExpression, SortDirection, SortItem, SortKey,
};
use crate::schema::Catalog;
use crate::store::{
    AdjacencyDirection, ConnectedNodesCreate, GraphMutation, GraphScanControl, GraphStore,
    MatchedRelationshipCopyMerge, MatchedRelationshipCreate, MatchedRelationshipMerge,
    MatchedRelationshipRetargetMerge, MatchedRelationshipSourceRetargetMerge, MutationLimits,
    NodeId, NodeRecord, NodeSetAssignment, NodeSetValue, ProjectedGraphDefinition, PropertyFilter,
    RelRecord, RelationshipDeleteRequest, RelationshipOnCreatePropertyValue,
    RelationshipPropertiesUpdate, RelationshipPropertyUpdate, RelationshipSetAssignment,
    RelationshipTargetNodeDelete, ScanPredicate, ScanPruningReport, ScanPruningStrategy,
    SourceScanCandidateRead,
};
use crate::value::Value;
use skein_core::RuntimeTaskContext;
use skein_ddl::{object_state_to_core, property_type_to_core, table_kind_to_core};
use skein_executor::{ExecutionLimit, VectorExecutionReport};
use skein_storage::RangeBound;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;

#[path = "executor/spill.rs"]
mod spill;

pub type Row = skein_executor::Row;
pub type ReadExecutionProfile = skein_executor::ReadExecutionProfile<ScanPruningReport>;
pub type ProfiledQueryRows = skein_executor::ProfiledQueryRows<ScanPruningReport>;
pub type ProfiledQueryStream = skein_executor::ProfiledQueryStream<ScanPruningReport>;
type ValueRangeBound = (Value, bool);
type ValueRangeBounds = (Option<ValueRangeBound>, Option<ValueRangeBound>);

const SOURCE_SEGMENT_SCAN_IO_DEPTH: usize = 2;
const SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES: u64 = 512 * 1024;
const SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES: u64 = 2 * 1024 * 1024;
const DEFAULT_EXECUTION_BATCH_ROWS: usize = 256;
const DEFAULT_EXECUTION_BATCH_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_EXECUTION_MAX_SPILL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_EXECUTION_MAX_SPILL_RUNS: usize = 128;
const MUTATION_OPERATION_BOOKKEEPING_BYTES: u64 = 64;
const MUTATION_AFFECTED_ROW_BOOKKEEPING_BYTES: u64 = 16;
const MUTATION_RESULT_ROW_BOOKKEEPING_BYTES: u64 = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionMemoryConfig {
    /// Maximum row count in an executor-owned transfer batch.
    pub batch_rows: NonZeroUsize,
    /// Maximum estimated resident bytes in an executor-owned transfer batch.
    pub batch_payload_bytes: NonZeroUsize,
    /// Maximum estimated resident bytes retained by one blocking operator.
    pub blocking_operator_bytes: NonZeroUsize,
    /// Maximum cumulative serialized spill bytes, including merge passes.
    pub max_spill_bytes: NonZeroU64,
    /// Maximum cumulative spill runs created, including merge passes.
    pub max_spill_runs: NonZeroUsize,
    /// Directory for query-scoped spill runs removed when execution finishes.
    pub spill_directory: PathBuf,
}

impl Default for ExecutionMemoryConfig {
    fn default() -> Self {
        Self {
            batch_rows: NonZeroUsize::new(DEFAULT_EXECUTION_BATCH_ROWS)
                .expect("default execution batch size is non-zero"),
            batch_payload_bytes: NonZeroUsize::new(DEFAULT_EXECUTION_BATCH_PAYLOAD_BYTES)
                .expect("default execution batch byte size is non-zero"),
            blocking_operator_bytes: NonZeroUsize::new(DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES)
                .expect("default blocking operator memory budget is non-zero"),
            max_spill_bytes: NonZeroU64::new(DEFAULT_EXECUTION_MAX_SPILL_BYTES)
                .expect("default spill byte budget is non-zero"),
            max_spill_runs: NonZeroUsize::new(DEFAULT_EXECUTION_MAX_SPILL_RUNS)
                .expect("default spill run budget is non-zero"),
            spill_directory: std::env::temp_dir(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExecutionMemoryEstimate {
    pub pipeline_batch_count: usize,
    pub blocking_operator_count: usize,
    pub pipeline_bytes: u64,
    pub blocking_bytes: u64,
    pub fixed_operator_bytes: u64,
    pub total_bytes: u64,
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) fn estimated_execution_memory(
    plan: &PhysicalPlan,
    memory: &ExecutionMemoryConfig,
) -> ExecutionMemoryEstimate {
    let shape = peak_execution_memory_shape(plan, memory);
    let pipeline_bytes = usize_to_u64(shape.pipeline_batch_count)
        .saturating_mul(usize_to_u64(memory.batch_payload_bytes.get()));
    let blocking_bytes = usize_to_u64(shape.blocking_operator_count)
        .saturating_mul(usize_to_u64(memory.blocking_operator_bytes.get()));
    let fixed_operator_bytes = shape.fixed_operator_bytes;
    ExecutionMemoryEstimate {
        pipeline_batch_count: shape.pipeline_batch_count,
        blocking_operator_count: shape.blocking_operator_count,
        pipeline_bytes,
        blocking_bytes,
        fixed_operator_bytes,
        total_bytes: pipeline_bytes
            .saturating_add(blocking_bytes)
            .saturating_add(fixed_operator_bytes),
    }
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) fn estimated_mutation_memory_bytes(
    limits: MutationLimits,
    max_wal_record_bytes: Option<usize>,
) -> u64 {
    let Some(max_wal_record_bytes) = max_wal_record_bytes else {
        return u64::MAX;
    };
    usize_to_u64(max_wal_record_bytes)
        .saturating_mul(2)
        .saturating_add(
            usize_to_u64(limits.max_operations.get())
                .saturating_mul(MUTATION_OPERATION_BOOKKEEPING_BYTES),
        )
        .saturating_add(
            usize_to_u64(limits.max_affected_rows.get())
                .saturating_mul(MUTATION_AFFECTED_ROW_BOOKKEEPING_BYTES),
        )
        .saturating_add(
            usize_to_u64(limits.max_result_rows.get())
                .saturating_mul(MUTATION_RESULT_ROW_BOOKKEEPING_BYTES),
        )
        .saturating_add(usize_to_u64(limits.max_result_payload_bytes.get()))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ExecutionMemoryShape {
    pipeline_batch_count: usize,
    blocking_operator_count: usize,
    fixed_operator_bytes: u64,
}

fn peak_execution_memory_shape(
    plan: &PhysicalPlan,
    memory: &ExecutionMemoryConfig,
) -> ExecutionMemoryShape {
    let mut shape = match plan.children() {
        skein_plan::PlanChildren::None => ExecutionMemoryShape::default(),
        skein_plan::PlanChildren::Unary(input) => peak_execution_memory_shape(input, memory),
        skein_plan::PlanChildren::Binary(left, right) => peak_shape_max(
            peak_execution_memory_shape(left, memory),
            peak_execution_memory_shape(right, memory),
            memory,
        ),
    };
    shape.pipeline_batch_count = shape.pipeline_batch_count.saturating_add(1);
    if retains_blocking_state(plan) {
        shape.blocking_operator_count = shape.blocking_operator_count.saturating_add(1);
    }
    if matches!(plan, PhysicalPlan::SourceSegmentScan { .. }) {
        shape.fixed_operator_bytes = shape
            .fixed_operator_bytes
            .saturating_add(SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES);
    }
    shape
}

fn peak_shape_max(
    left: ExecutionMemoryShape,
    right: ExecutionMemoryShape,
    memory: &ExecutionMemoryConfig,
) -> ExecutionMemoryShape {
    let shape_bytes = |shape: ExecutionMemoryShape| {
        usize_to_u64(shape.pipeline_batch_count)
            .saturating_mul(usize_to_u64(memory.batch_payload_bytes.get()))
            .saturating_add(
                usize_to_u64(shape.blocking_operator_count)
                    .saturating_mul(usize_to_u64(memory.blocking_operator_bytes.get())),
            )
            .saturating_add(shape.fixed_operator_bytes)
    };
    let left_total = shape_bytes(left);
    let right_total = shape_bytes(right);
    if left_total > right_total
        || (left_total == right_total && left.fixed_operator_bytes >= right.fixed_operator_bytes)
    {
        left
    } else {
        right
    }
}

fn retains_blocking_state(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::GraphAlgorithm { .. }
            | PhysicalPlan::VectorSeedScan { .. }
            | PhysicalPlan::NodeCartesianProductExec { .. }
            | PhysicalPlan::AdjacencyExpandExec { .. }
            | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
            | PhysicalPlan::ThreadRepairStatsExec { .. }
            | PhysicalPlan::ShortestPathExec { .. }
            | PhysicalPlan::AggregateExec { .. }
            | PhysicalPlan::DistinctExec { .. }
            | PhysicalPlan::SortExec { .. }
            | PhysicalPlan::TopNExec { .. }
    )
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

thread_local! {
    static SCAN_PRUNING_REPORT_CAPTURE: RefCell<Option<Vec<ScanPruningReport>>> = const { RefCell::new(None) };
    static VECTOR_EXECUTION_REPORT_CAPTURE: RefCell<Option<Vec<VectorExecutionReport>>> = const { RefCell::new(None) };
    static GRAPH_EXPANSION_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::GraphExpansionExecutionReport>>> = const { RefCell::new(None) };
    static BLOCKING_MEMORY_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::BlockingOperatorMemoryReport>>> = const { RefCell::new(None) };
    static PIPELINE_MEMORY_REPORT_CAPTURE: RefCell<Option<skein_executor::PipelineMemoryReport>> = const { RefCell::new(None) };
}

pub struct VectorSeedExecutionRequest<'a> {
    pub embedding: &'a [f32],
    pub metadata_filters: &'a BTreeMap<String, String>,
    pub vector_plan: &'a skein_plan::VectorPhysicalPlan,
}

pub struct VectorSeedExecutionRow {
    pub id: String,
    pub external_id: Option<String>,
    pub score: f64,
}

pub struct VectorSeedExecutionOutput {
    pub rows: Vec<VectorSeedExecutionRow>,
    pub report: VectorExecutionReport,
}

pub trait ExternalReadOperator {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput>;
}

pub(crate) struct NoExternalReadOperator;

impl ExternalReadOperator for NoExternalReadOperator {
    fn execute_vector_seed(
        &mut self,
        _request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        Err(SkeinError::Execution(
            "vector search capability is unavailable without a search projection".to_string(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Binding {
    values: BTreeMap<String, Value>,
    nodes: BTreeMap<String, NodeRecord>,
    relationships: BTreeMap<String, RelRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopNBinding {
    sort_values: Vec<(Value, SortDirection)>,
    ordinal: u64,
    binding: Binding,
}

impl TopNBinding {
    fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding).saturating_add(self.sort_values.iter().fold(
            size_of::<Vec<(Value, SortDirection)>>(),
            |total, (value, _)| total.saturating_add(value_memory_bytes(value)),
        ))
    }
}

impl Ord for TopNBinding {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((left, direction), (right, other_direction)) in
            self.sort_values.iter().zip(&other.sort_values)
        {
            debug_assert_eq!(direction, other_direction);
            let ordering = match direction {
                SortDirection::Asc => left.cmp(right),
                SortDirection::Desc => left.cmp(right).reverse(),
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        self.ordinal.cmp(&other.ordinal)
    }
}

impl PartialOrd for TopNBinding {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy)]
struct NodeColumnLookupSpec<'a> {
    variable: &'a str,
    label: &'a str,
    property: &'a str,
    column: &'a str,
    optional: bool,
}

fn stream_node_column_lookup_batches(
    spec: NodeColumnLookupSpec<'_>,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut output = Vec::with_capacity(context.memory.batch_rows.get());
    let mut emitted = 0usize;
    execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
        let remaining = execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(emitted);
        if remaining == 0 {
            return Ok(BatchControl::Stop);
        }
        let bindings = execute_node_column_lookup(
            spec,
            batch,
            context.catalog,
            context.store,
            ExecutionLimit {
                output_rows: Some(remaining),
            },
            context.memory.blocking_operator_bytes,
        )?;
        for binding in bindings {
            output.push(binding);
            emitted = emitted.saturating_add(1);
            if output.len() == context.memory.batch_rows.get()
                && emit(std::mem::replace(
                    &mut output,
                    Vec::with_capacity(context.memory.batch_rows.get()),
                ))? == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
            if execution_limit.is_reached(emitted) {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    })?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

#[allow(clippy::too_many_arguments)]
fn stream_optional_degree_batches(
    source_variable: &str,
    rel_type: &str,
    rel_properties: &BTreeMap<String, Value>,
    direction: RelationshipDirection,
    target_label: &str,
    target_properties: &BTreeMap<String, Value>,
    alias: &str,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        context.catalog.rel_type_id(rel_type)
    };
    let target_label_ids = label_ids_for_pattern(context.catalog, target_label);
    let mut emitted = 0usize;
    execute_binding_batches(input, context, execution_limit, &mut |batch| {
        let mut output = Vec::with_capacity(batch.len());
        for mut binding in batch {
            let degree = if !rel_type.is_empty() && rel_type_id.is_none() {
                0
            } else {
                let source = binding.nodes.get(source_variable).ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "missing variable '{source_variable}' during optional degree"
                    ))
                })?;
                one_hop_relationships_with_budget(
                    context.store,
                    source.id,
                    rel_type_id,
                    target_label_ids.as_deref(),
                    rel_properties,
                    None,
                    direction,
                    context.memory.blocking_operator_bytes.get(),
                )?
                .into_iter()
                .filter(|(_, target)| node_properties_match(target, target_properties))
                .count()
            };
            binding
                .values
                .insert(alias.to_string(), Value::Int(degree as i64));
            output.push(binding);
        }
        emitted = emitted.saturating_add(output.len());
        if !output.is_empty() && emit(output)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        Ok(if execution_limit.is_reached(emitted) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    })
}

struct ExecutionContext<'a> {
    parameters: &'a BTreeMap<String, Value>,
    external: &'a mut dyn ExternalReadOperator,
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
}

#[derive(Clone, Copy)]
struct ExecutionRuntimeControl<'a> {
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
}

#[derive(Clone, Copy)]
struct ExecutionOutputLimits {
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
}

pub fn execute(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
) -> Result<Vec<Row>> {
    execute_with_row_limit(plan, catalog, store, None)
}

pub fn execute_with_row_limit(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    execute_with_row_limit_internal(plan, catalog, store, max_rows, None)
}

pub fn execute_with_row_limit_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
    task_context: &RuntimeTaskContext,
) -> Result<Vec<Row>> {
    execute_with_row_limit_internal(plan, catalog, store, max_rows, Some(task_context))
}

fn execute_with_row_limit_internal(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let memory = ExecutionMemoryConfig::default();
    let mut rows = Vec::new();
    execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        &parameters,
        &mut external,
        max_rows,
        None,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
        ExecutionRuntimeControl {
            memory: &memory,
            task_context,
        },
    )?;
    Ok(rows)
}

pub fn execute_with_row_limit_profile(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
) -> Result<ProfiledQueryRows> {
    let mut external = NoExternalReadOperator;
    execute_with_row_limit_profile_and_external(
        plan,
        catalog,
        store,
        &BTreeMap::new(),
        &mut external,
        max_rows,
    )
}

pub fn execute_with_row_limit_profile_and_external(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
) -> Result<ProfiledQueryRows> {
    execute_with_row_limit_profile_and_external_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        &ExecutionMemoryConfig::default(),
    )
}

pub fn execute_with_output_limits_profile_and_external(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
) -> Result<ProfiledQueryRows> {
    execute_with_output_limits_profile_and_external_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_output_limits_profile_and_external_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryRows> {
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes,
        },
        ExecutionRuntimeControl {
            memory,
            task_context: None,
        },
    )
}

pub fn execute_with_row_limit_profile_and_external_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryRows> {
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes: None,
        },
        ExecutionRuntimeControl {
            memory,
            task_context: None,
        },
    )
}

pub fn execute_with_row_limit_profile_and_external_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    task_context: &RuntimeTaskContext,
) -> Result<ProfiledQueryRows> {
    let memory = ExecutionMemoryConfig::default();
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes: None,
        },
        ExecutionRuntimeControl {
            memory: &memory,
            task_context: Some(task_context),
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_output_limits_profile_and_external_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    task_context: &RuntimeTaskContext,
) -> Result<ProfiledQueryRows> {
    execute_with_output_limits_profile_and_external_and_context_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        task_context,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_output_limits_profile_and_external_and_context_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    task_context: &RuntimeTaskContext,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryRows> {
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes,
        },
        ExecutionRuntimeControl {
            memory,
            task_context: Some(task_context),
        },
    )
}

/// Executes a read plan and transfers ownership of each output row to a
/// bounded consumer. Consumer calls are provisional until this function
/// returns `Ok`: callers that cannot surface a terminal error must buffer or
/// otherwise roll back their response when a later row exceeds a budget.
pub fn execute_with_row_consumer_profile(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<ProfiledQueryStream> {
    let mut external = NoExternalReadOperator;
    execute_with_row_consumer_profile_and_external(
        plan,
        catalog,
        store,
        parameters,
        &mut external,
        max_rows,
        max_payload_bytes,
        consumer,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_and_external_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        ExecutionRuntimeControl {
            memory,
            task_context: None,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    task_context: &RuntimeTaskContext,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_and_external_and_context_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        task_context,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external_and_context_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    task_context: &RuntimeTaskContext,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        ExecutionRuntimeControl {
            memory,
            task_context: Some(task_context),
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_with_row_consumer_profile_internal(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    runtime: ExecutionRuntimeControl<'_>,
) -> Result<ProfiledQueryStream> {
    store.ensure_usable()?;
    let ExecutionRuntimeControl {
        memory,
        task_context,
    } = runtime;
    let process_memory_start = skein_qos::ProcessMemorySnapshot::capture().ok();
    let execution_limit = ExecutionLimit::from_user_max_rows(max_rows)?;
    let mut profile = read_execution_profile(plan, max_rows)?;
    let fully_streamed = batch_pipeline_capable(plan);
    let mut output_rows = 0usize;
    let mut output_payload_bytes = 0usize;
    let mut emit_binding = |binding: Binding| -> Result<()> {
        if max_rows.is_some_and(|limit| output_rows >= limit) {
            return Err(SkeinError::Execution(format!(
                "read query returned more than {} rows, exceeding max_read_result_rows {}",
                max_rows.unwrap_or_default(),
                max_rows.unwrap_or_default()
            )));
        }
        let row = binding.values;
        let row_payload_bytes = map_payload_bytes(&row);
        let next_payload_bytes = output_payload_bytes.saturating_add(row_payload_bytes);
        if max_payload_bytes.is_some_and(|limit| next_payload_bytes > limit) {
            return Err(SkeinError::Execution(format!(
                "read query payload would exceed max_payload_bytes {} (max_read_result_payload_bytes {}; next total {})",
                max_payload_bytes.unwrap_or_default(),
                max_payload_bytes.unwrap_or_default(),
                next_payload_bytes
            )));
        }
        consumer(row)?;
        output_rows = output_rows.saturating_add(1);
        output_payload_bytes = next_payload_bytes;
        Ok(())
    };
    let mut context = ExecutionContext {
        parameters,
        external,
        memory,
        task_context,
    };
    let (
        (
            ((((), scan_pruning_reports), vector_execution_reports), graph_expansion_reports),
            blocking_operator_memory_reports,
        ),
        mut pipeline_memory_report,
    ) = capture_pipeline_memory_report(|| {
        capture_blocking_memory_reports(|| {
            capture_graph_expansion_reports(|| {
                capture_vector_execution_reports(|| {
                    capture_scan_pruning_reports(|| {
                        if fully_streamed {
                            let batch_context = BatchReadContext {
                                catalog,
                                store,
                                memory,
                                task_context,
                            };
                            execute_binding_batches(
                                plan,
                                batch_context,
                                execution_limit,
                                &mut |batch| {
                                    for binding in batch {
                                        emit_binding(binding)?;
                                    }
                                    Ok(BatchControl::Continue)
                                },
                            )?;
                        } else {
                            let bindings = execute_bindings_with_limit(
                                plan,
                                catalog,
                                store,
                                &mut context,
                                execution_limit,
                            )?;
                            for binding in bindings {
                                emit_binding(binding)?;
                            }
                        }
                        Ok(())
                    })
                })
            })
        })
    })?;
    profile.scan_pruning_reports = scan_pruning_reports;
    profile.vector_execution_reports = vector_execution_reports;
    profile.graph_expansion_reports = graph_expansion_reports;
    profile.blocking_operator_memory_reports = blocking_operator_memory_reports;
    pipeline_memory_report.output_rows = output_rows;
    pipeline_memory_report.output_payload_bytes = output_payload_bytes;
    if let Ok(process_memory_end) = skein_qos::ProcessMemorySnapshot::capture() {
        pipeline_memory_report.steady_resident_bytes = Some(process_memory_end.resident_bytes);
        pipeline_memory_report.peak_resident_bytes = Some(process_memory_end.peak_resident_bytes);
        if let Some(process_memory_start) = process_memory_start {
            let process_memory =
                skein_qos::ProcessMemoryProfile::between(process_memory_start, process_memory_end);
            pipeline_memory_report.start_resident_bytes = Some(process_memory.start_resident_bytes);
            pipeline_memory_report.start_peak_resident_bytes =
                Some(process_memory.start_peak_resident_bytes);
            pipeline_memory_report.steady_resident_growth_bytes =
                Some(process_memory.steady_resident_growth_bytes);
            pipeline_memory_report.lifetime_peak_resident_growth_bytes =
                Some(process_memory.lifetime_peak_resident_growth_bytes);
            pipeline_memory_report.total_page_faults = process_memory.total_page_faults;
            pipeline_memory_report.minor_page_faults = process_memory.minor_page_faults;
            pipeline_memory_report.major_page_faults = process_memory.major_page_faults;
        }
    }
    profile.pipeline_memory_report = pipeline_memory_report;
    Ok(ProfiledQueryStream {
        fully_streamed,
        profile,
    })
}

fn execute_with_row_limit_profile_and_external_and_memory_internal(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    output_limits: ExecutionOutputLimits,
    runtime: ExecutionRuntimeControl<'_>,
) -> Result<ProfiledQueryRows> {
    let ExecutionOutputLimits {
        max_rows,
        max_payload_bytes,
    } = output_limits;
    let mut rows = Vec::new();
    let streamed = execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
        runtime,
    )?;
    Ok(ProfiledQueryRows {
        rows,
        profile: streamed.profile,
    })
}

pub fn read_execution_profile(
    plan: &PhysicalPlan,
    max_rows: Option<usize>,
) -> Result<ReadExecutionProfile> {
    let execution_limit = ExecutionLimit::from_user_max_rows(max_rows)?;
    let mut blocking_operator_kinds = BTreeSet::new();
    collect_blocking_operator_kinds(plan, &mut blocking_operator_kinds);
    Ok(ReadExecutionProfile {
        max_rows,
        detection_row_cap: execution_limit.output_rows,
        row_limit_enforced_before_output: max_rows.is_some(),
        operator_row_cap_enabled: execution_limit.output_rows.is_some(),
        blocking_operator_kinds: blocking_operator_kinds.into_iter().collect(),
        scan_pruning_reports: Vec::new(),
        vector_execution_reports: Vec::new(),
        graph_expansion_reports: Vec::new(),
        blocking_operator_memory_reports: Vec::new(),
        pipeline_memory_report: skein_executor::PipelineMemoryReport::default(),
    })
}

fn capture_scan_pruning_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<ScanPruningReport>)> {
    SCAN_PRUNING_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

fn record_scan_pruning_report(report: ScanPruningReport) {
    SCAN_PRUNING_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

fn capture_vector_execution_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<VectorExecutionReport>)> {
    VECTOR_EXECUTION_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

fn record_vector_execution_report(report: VectorExecutionReport) {
    VECTOR_EXECUTION_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

fn capture_graph_expansion_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<skein_executor::GraphExpansionExecutionReport>)> {
    GRAPH_EXPANSION_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

fn record_graph_expansion_report(report: skein_executor::GraphExpansionExecutionReport) {
    GRAPH_EXPANSION_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

fn capture_pipeline_memory_report<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, skein_executor::PipelineMemoryReport)> {
    PIPELINE_MEMORY_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(skein_executor::PipelineMemoryReport::default()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

fn record_pipeline_batch(batch: &[Binding]) {
    PIPELINE_MEMORY_REPORT_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(report) = capture.as_mut() else {
            return;
        };
        let payload_bytes = batch.iter().fold(0usize, |total, binding| {
            total.saturating_add(binding_payload_bytes(binding))
        });
        report.intermediate_rows = report.intermediate_rows.saturating_add(batch.len());
        report.intermediate_payload_bytes = report
            .intermediate_payload_bytes
            .saturating_add(payload_bytes);
        report.peak_batch_rows = report.peak_batch_rows.max(batch.len());
        report.peak_batch_payload_bytes = report.peak_batch_payload_bytes.max(payload_bytes);
    });
}

fn capture_blocking_memory_reports<T>(
    f: impl FnOnce() -> Result<T>,
) -> Result<(T, Vec<skein_executor::BlockingOperatorMemoryReport>)> {
    BLOCKING_MEMORY_REPORT_CAPTURE.with(|capture| {
        let previous = capture.replace(Some(Vec::new()));
        let result = f();
        let captured = capture.replace(previous).unwrap_or_default();
        result.map(|value| (value, captured))
    })
}

fn record_blocking_memory_report(report: skein_executor::BlockingOperatorMemoryReport) {
    BLOCKING_MEMORY_REPORT_CAPTURE.with(|capture| {
        if let Some(reports) = capture.borrow_mut().as_mut() {
            reports.push(report);
        }
    });
}

fn current_vector_rerank_count() -> usize {
    VECTOR_EXECUTION_REPORT_CAPTURE.with(|capture| {
        capture
            .borrow()
            .as_ref()
            .and_then(|reports| reports.last())
            .map(|report| report.reranked_candidate_count)
            .unwrap_or_default()
    })
}

fn collect_blocking_operator_kinds(plan: &PhysicalPlan, output: &mut BTreeSet<String>) {
    match plan {
        PhysicalPlan::GraphAlgorithm { .. } | PhysicalPlan::VectorSeedScan { .. } => {
            output.insert(plan.kind().as_str().to_string());
        }
        PhysicalPlan::ShortestPathExec { .. } => {
            output.insert("ShortestPathExec".to_string());
        }
        PhysicalPlan::AggregateExec { input, .. } => {
            output.insert("AggregateExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::DistinctExec { input } => {
            output.insert("DistinctExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::SortExec { input, .. } => {
            output.insert("SortExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::TopNExec { input, .. } => {
            output.insert("TopNExec".to_string());
            collect_blocking_operator_kinds(input, output);
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            output.insert("NodeCartesianProductExec".to_string());
            collect_blocking_operator_kinds(left, output);
            collect_blocking_operator_kinds(right, output);
        }
        PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::AdjacencyExpandExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => {
            collect_blocking_operator_kinds(input, output);
        }
        _ => {}
    }
}

fn vector_embedding_parameter(
    parameters: &BTreeMap<String, Value>,
    name: &str,
    vector_plan: &skein_plan::VectorPhysicalPlan,
) -> Result<Vec<f32>> {
    let Some(Value::List(values)) = parameters.get(name) else {
        return Err(SkeinError::Semantic(format!(
            "vector search parameter '${name}' must be a numeric list"
        )));
    };
    let embedding = values
        .iter()
        .map(|value| match value {
            Value::Float(value) if value.is_finite() => {
                let value = *value as f32;
                if value.is_finite() {
                    Ok(value)
                } else {
                    Err(SkeinError::Semantic(format!(
                        "vector search parameter '${name}' exceeds f32 range"
                    )))
                }
            }
            Value::Int(value) => Ok(*value as f32),
            _ => Err(SkeinError::Semantic(format!(
                "vector search parameter '${name}' must contain finite numbers"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    let expected_dimension = vector_plan_embedding_dimension(vector_plan);
    if embedding.len() != expected_dimension {
        return Err(SkeinError::Semantic(format!(
            "vector search parameter '${name}' dimension changed after planning"
        )));
    }
    Ok(embedding)
}

fn vector_plan_embedding_dimension(plan: &skein_plan::VectorPhysicalPlan) -> usize {
    match plan {
        skein_plan::VectorPhysicalPlan::VectorCandidateScan {
            embedding_dimension,
            ..
        }
        | skein_plan::VectorPhysicalPlan::RawVectorRerank {
            embedding_dimension,
            ..
        } => *embedding_dimension,
        skein_plan::VectorPhysicalPlan::ResidualFilter { input, .. }
        | skein_plan::VectorPhysicalPlan::TopK { input, .. } => {
            vector_plan_embedding_dimension(input)
        }
        skein_plan::VectorPhysicalPlan::Filter { .. } => 0,
    }
}

pub fn execute_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    runtime_checkpoint(task_context)?;
    if let PhysicalPlan::SetNodePropertiesReturn {
        variable,
        label,
        predicate,
        assignments,
        returns,
    } = plan
    {
        return execute_set_node_properties_return_with_limits(
            variable,
            label,
            predicate.as_ref(),
            assignments,
            returns,
            catalog,
            store,
            limits,
            task_context,
        );
    }
    match mutation_command(plan) {
        Ok(Some(mutation)) => store
            .commit_mutation_with_limits(catalog, mutation, limits)
            .map(|summary| summary.rows),
        Ok(None) => Err(SkeinError::Execution(
            "physical plan is not an executable mutation".to_string(),
        )),
        Err(error) => {
            if let Some(rows) =
                execute_node_mutation_with_limits(plan, catalog, store, limits, task_context)?
            {
                Ok(rows)
            } else {
                Err(error)
            }
        }
    }
}

fn execute_node_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Option<Vec<Row>>> {
    let (variable, label, predicate, action) = match plan {
        PhysicalPlan::SetNodeProperty {
            variable,
            label,
            predicate,
            property,
            value,
        } => (
            variable,
            label,
            predicate.as_ref(),
            NodeMutationPreflightAction::Set(vec![NodeSetAssignment {
                property: property.clone(),
                value: node_set_value(value),
            }]),
        ),
        PhysicalPlan::SetNodeProperties {
            variable,
            label,
            predicate,
            assignments,
        } => (
            variable,
            label,
            predicate.as_ref(),
            NodeMutationPreflightAction::Set(assignments.iter().map(node_set_assignment).collect()),
        ),
        PhysicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => (
            variable,
            label,
            predicate.as_ref(),
            NodeMutationPreflightAction::Delete { detach: *detach },
        ),
        _ => return Ok(None),
    };
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut ids = Vec::with_capacity(limits.max_affected_rows.get().min(1024));
    let mut visited = 0usize;
    let mut callback_error = None;
    store.visit_nodes_owned(None, |node| {
        if callback_error.is_some() {
            return GraphScanControl::Stop;
        }
        visited = visited.saturating_add(1);
        if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS)
            && let Err(error) = runtime_checkpoint(task_context)
        {
            callback_error = Some(error);
            return GraphScanControl::Stop;
        }
        if !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return GraphScanControl::Continue;
        }
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        };
        if let Some(predicate) = predicate {
            match evaluate_predicate(predicate, catalog, store, &binding) {
                Ok(true) => {}
                Ok(false) => return GraphScanControl::Continue,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if ids.len() == limits.max_affected_rows.get() {
            callback_error = Some(SkeinError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {}",
                limits.max_affected_rows
            )));
            return GraphScanControl::Stop;
        }
        ids.push(binding.nodes[variable].id);
        GraphScanControl::Continue
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    if ids.len() > limits.max_result_rows.get() {
        return Err(SkeinError::Execution(format!(
            "mutation would exceed max_mutation_result_rows {}",
            limits.max_result_rows
        )));
    }
    let output = ids
        .iter()
        .map(|id| BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]))
        .collect::<Vec<_>>();
    let payload_bytes = output.iter().fold(0usize, |total, row| {
        total.saturating_add(map_payload_bytes(row))
    });
    if payload_bytes > limits.max_result_payload_bytes.get() {
        return Err(SkeinError::Execution(format!(
            "mutation result payload would exceed max_mutation_result_payload_bytes {}",
            limits.max_result_payload_bytes
        )));
    }
    match action {
        NodeMutationPreflightAction::Set(assignments) => {
            store.set_node_properties_by_ids_with_limits(catalog, &ids, &assignments, limits)?;
        }
        NodeMutationPreflightAction::Delete { detach } => {
            store.delete_node_ids_with_limits(catalog, &ids, detach, limits)?;
        }
    }
    Ok(Some(output))
}

enum NodeMutationPreflightAction {
    Set(Vec<NodeSetAssignment>),
    Delete { detach: bool },
}

#[allow(clippy::too_many_arguments)]
fn execute_set_node_properties_return_with_limits(
    variable: &str,
    label: &str,
    predicate: Option<&Predicate>,
    assignments: &[crate::planner::SetAssignment],
    returns: &SetNodePropertiesReturnMode,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let assignments = assignments
        .iter()
        .map(node_set_assignment)
        .collect::<Vec<_>>();
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut ids = Vec::with_capacity(limits.max_affected_rows.get().min(1024));
    let mut projected_rows = Vec::new();
    let mut projected_payload_bytes = 0usize;
    let mut visited = 0usize;
    let mut callback_error = None;
    store.visit_nodes_owned(None, |node| {
        if callback_error.is_some() {
            return GraphScanControl::Stop;
        }
        visited = visited.saturating_add(1);
        if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS)
            && let Err(error) = runtime_checkpoint(task_context)
        {
            callback_error = Some(error);
            return GraphScanControl::Stop;
        }
        if !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return GraphScanControl::Continue;
        }
        let original_binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node.clone())]),
            relationships: BTreeMap::new(),
        };
        if let Some(predicate) = predicate {
            match evaluate_predicate(predicate, catalog, store, &original_binding) {
                Ok(true) => {}
                Ok(false) => return GraphScanControl::Continue,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if ids.len() == limits.max_affected_rows.get() {
            callback_error = Some(SkeinError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {}",
                limits.max_affected_rows
            )));
            return GraphScanControl::Stop;
        }
        let id = node.id;
        ids.push(id);
        if let SetNodePropertiesReturnMode::Project(returns) = returns {
            if projected_rows.len() == limits.max_result_rows.get() {
                callback_error = Some(SkeinError::Execution(format!(
                    "mutation would exceed max_mutation_result_rows {}",
                    limits.max_result_rows
                )));
                return GraphScanControl::Stop;
            }
            let mut projected_node = node;
            for assignment in &assignments {
                let value = match crate::store::evaluate_node_set_value(
                    &projected_node.properties,
                    assignment,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        callback_error = Some(error);
                        return GraphScanControl::Stop;
                    }
                };
                projected_node
                    .properties
                    .insert(assignment.property.clone(), value);
            }
            let binding = Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(variable.to_string(), projected_node)]),
                relationships: BTreeMap::new(),
            };
            let values = match returns
                .iter()
                .map(|item| {
                    project_value(item, catalog, &binding).map(|value| (item.name.clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>>>()
            {
                Ok(values) => values,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            };
            let next_payload = projected_payload_bytes.saturating_add(map_payload_bytes(&values));
            if next_payload > limits.max_result_payload_bytes.get() {
                callback_error = Some(SkeinError::Execution(format!(
                    "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                    limits.max_result_payload_bytes
                )));
                return GraphScanControl::Stop;
            }
            projected_payload_bytes = next_payload;
            projected_rows.push(values);
        }
        GraphScanControl::Continue
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }

    let output = match returns {
        SetNodePropertiesReturnMode::Project(_) => projected_rows,
        SetNodePropertiesReturnMode::Count { name } => {
            let row = BTreeMap::from([(name.clone(), Value::Int(ids.len() as i64))]);
            if map_payload_bytes(&row) > limits.max_result_payload_bytes.get() {
                return Err(SkeinError::Execution(format!(
                    "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                    limits.max_result_payload_bytes
                )));
            }
            vec![row]
        }
    };
    let operation_count = ids
        .len()
        .checked_mul(assignments.len())
        .ok_or_else(|| SkeinError::Execution("mutation operation count overflow".to_string()))?;
    if operation_count > limits.max_operations.get() {
        return Err(SkeinError::Execution(format!(
            "mutation would exceed max_mutation_operations {}",
            limits.max_operations
        )));
    }
    runtime_checkpoint(task_context)?;
    store.set_node_properties_by_ids_with_limits(catalog, &ids, &assignments, limits)?;
    Ok(output)
}

pub fn mutation_command(plan: &PhysicalPlan) -> Result<Option<GraphMutation>> {
    match plan {
        PhysicalPlan::CreateNodeLabel { label } => Ok(Some(GraphMutation::CreateNodeLabel {
            label: label.clone(),
        })),
        PhysicalPlan::CreateRelationshipType { rel_type } => {
            Ok(Some(GraphMutation::CreateRelationshipType {
                rel_type: rel_type.clone(),
            }))
        }
        PhysicalPlan::CreateNodeTable { name } => {
            Ok(Some(GraphMutation::CreateNodeTable { name: name.clone() }))
        }
        PhysicalPlan::CreateRelationshipTable { name } => {
            Ok(Some(GraphMutation::CreateRelationshipTable {
                name: name.clone(),
            }))
        }
        PhysicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => Ok(Some(GraphMutation::CreateProperty {
            table_kind: table_kind_to_core(*table_kind),
            table: table.clone(),
            property: property.clone(),
            value_type: property_type_to_core(*value_type),
            nullable: *nullable,
        })),
        PhysicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => Ok(Some(GraphMutation::AlterTableState {
            table_kind: table_kind_to_core(*table_kind),
            table: table.clone(),
            state: object_state_to_core(*state),
        })),
        PhysicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => Ok(Some(GraphMutation::AlterPropertyState {
            table_kind: table_kind_to_core(*table_kind),
            table: table.clone(),
            property: property.clone(),
            state: object_state_to_core(*state),
        })),
        PhysicalPlan::CreateIndex { label, property } => Ok(Some(GraphMutation::CreateIndex {
            label: label.clone(),
            property: property.clone(),
        })),
        PhysicalPlan::CreateCompositeIndex { label, properties } => {
            Ok(Some(GraphMutation::CreateCompositeIndex {
                label: label.clone(),
                properties: properties.clone(),
            }))
        }
        PhysicalPlan::CreateRangeIndex { label, property } => {
            Ok(Some(GraphMutation::CreateRangeIndex {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateFullTextIndex { label, property } => {
            Ok(Some(GraphMutation::CreateFullTextIndex {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateUniqueConstraint { label, property } => {
            Ok(Some(GraphMutation::CreateUniqueConstraint {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            Ok(Some(GraphMutation::CreateNodePropertyExistsConstraint {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            Ok(Some(GraphMutation::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => Ok(
            Some(GraphMutation::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }),
        ),
        PhysicalPlan::CreateNode { label, properties } => Ok(Some(GraphMutation::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        })),
        PhysicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => Ok(Some(GraphMutation::MergeNode {
            label: label.clone(),
            match_properties: match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
            on_match_assignments: on_match_assignments
                .iter()
                .map(node_set_assignment)
                .collect(),
            post_merge_assignments: post_merge_assignments
                .iter()
                .map(node_set_assignment)
                .collect(),
        })),
        PhysicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => Ok(Some(GraphMutation::MergeConnectedNodes(
            ConnectedNodesCreate {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
        ))),
        PhysicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => Ok(Some(GraphMutation::MergeRelationshipsBetweenMatches(
            MatchedRelationshipMerge {
                source_label: source_label.clone(),
                source_filter: property_filter_from_properties(source_properties),
                rel_type: rel_type.clone(),
                rel_match_properties: rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
                target_label: target_label.clone(),
                target_filter: property_filter_from_properties(target_properties),
            },
        ))),
        PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            target_label,
            target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => Ok(Some(
            GraphMutation::MergeRelationshipsFromMatchedRelationships(
                MatchedRelationshipCopyMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties
                        .iter()
                        .map(|(property, value)| {
                            (
                                property.clone(),
                                relationship_on_create_property_value(value),
                            )
                        })
                        .collect(),
                },
            ),
        )),
        PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_target_label,
            new_target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => Ok(Some(GraphMutation::MergeRelationshipsToMatchedTarget(
            MatchedRelationshipRetargetMerge {
                source_label: source_label.clone(),
                source_filter: property_filter_from_properties(source_properties),
                old_rel_type: old_rel_type.clone(),
                old_rel_filter: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_filter: property_filter_from_properties(old_target_properties),
                new_target_label: new_target_label.clone(),
                new_target_filter: property_filter_from_properties(new_target_properties),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
        ))),
        PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label,
            old_source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_source_label,
            new_source_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => Ok(Some(GraphMutation::MergeRelationshipsFromMatchedTarget(
            MatchedRelationshipSourceRetargetMerge {
                old_source_label: old_source_label.clone(),
                old_source_filter: property_filter_from_properties(old_source_properties),
                old_rel_type: old_rel_type.clone(),
                old_rel_filter: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_filter: property_filter_from_properties(old_target_properties),
                new_source_label: new_source_label.clone(),
                new_source_filter: property_filter_from_properties(new_source_properties),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
        ))),
        PhysicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => Ok(Some(GraphMutation::CreateRelationshipsBetweenMatches(
            MatchedRelationshipCreate {
                source_label: source_label.clone(),
                source_filter: property_filter_from_properties(source_properties),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_filter: property_filter_from_properties(target_properties),
            },
        ))),
        PhysicalPlan::SetNodeProperty {
            label,
            predicate,
            property,
            value,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            match value {
                SetValue::Value(value) => Ok(Some(GraphMutation::SetNodeProperty {
                    label: label.clone(),
                    filter,
                    property: property.clone(),
                    value: value.clone(),
                })),
                SetValue::Coalesce { .. } => Err(SkeinError::Semantic(
                    "COALESCE node SET is not supported in transactional MATCH SET".to_string(),
                )),
                SetValue::AddInt { amount, .. } => Ok(Some(GraphMutation::SetNodePropertyAddInt {
                    label: label.clone(),
                    filter,
                    property: property.clone(),
                    amount: *amount,
                })),
                SetValue::DecrementFloorZero { .. } => Ok(Some(GraphMutation::SetNodeProperties {
                    label: label.clone(),
                    filter,
                    assignments: vec![NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::DecrementFloorZero,
                    }],
                })),
                SetValue::PreserveNewerExisting {
                    incoming, preserve, ..
                } => Ok(Some(GraphMutation::SetNodeProperties {
                    label: label.clone(),
                    filter,
                    assignments: vec![NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::PreserveNewerExisting {
                            incoming: incoming.clone(),
                            preserve: *preserve,
                        },
                    }],
                })),
            }
        }
        PhysicalPlan::SetNodeProperties {
            label,
            predicate,
            assignments,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            Ok(Some(GraphMutation::SetNodeProperties {
                label: label.clone(),
                filter,
                assignments: assignments.iter().map(node_set_assignment).collect(),
            }))
        }
        PhysicalPlan::SetRelationshipProperty {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            property,
            value,
            ..
        } => Ok(Some(GraphMutation::SetRelationshipProperty {
            source_label: source_label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            rel_type: rel_type.clone(),
            target_label: target_label.clone(),
            rel_filter: relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?,
            target_filter: property_filter_from_properties(target_properties),
            property: property.clone(),
            value: value.clone(),
        })),
        PhysicalPlan::SetRelationshipProperties {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            assignments,
            ..
        } => Ok(Some(GraphMutation::SetRelationshipProperties {
            source_label: source_label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            rel_type: rel_type.clone(),
            target_label: target_label.clone(),
            rel_filter: relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?,
            target_filter: property_filter_from_properties(target_properties),
            assignments: assignments
                .iter()
                .map(|assignment| RelationshipSetAssignment {
                    property: assignment.property.clone(),
                    value: assignment.value.clone(),
                })
                .collect(),
        })),
        PhysicalPlan::DeleteNode {
            label,
            predicate,
            detach,
            ..
        } => Ok(Some(GraphMutation::DeleteNode {
            label: label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            detach: *detach,
        })),
        PhysicalPlan::DeleteRelationship {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            ..
        } => Ok(Some(GraphMutation::DeleteRelationship {
            source_label: source_label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            rel_type: rel_type.clone(),
            target_label: target_label.clone(),
            target_filter: property_filter_from_properties(target_properties),
            rel_filter: relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?,
        })),
        PhysicalPlan::DeleteRelationshipTargetNodes {
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
            detach,
            ..
        } => Ok(Some(GraphMutation::DeleteRelationshipTargetNodes(
            RelationshipTargetNodeDelete {
                source_label: source_label.clone(),
                source_filter: source_predicate
                    .as_ref()
                    .map(property_filter_from_predicate)
                    .transpose()?,
                rel_type: rel_type.clone(),
                rel_filter: property_filter_from_properties(rel_properties),
                target_label: target_label.clone(),
                target_filter: property_filter_from_properties(target_properties),
                detach: *detach,
            },
        ))),
        PhysicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => Ok(Some(GraphMutation::CreateConnectedNodes(
            ConnectedNodesCreate {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
        ))),
        PhysicalPlan::SeqNodeScan { .. }
        | PhysicalPlan::SourceSegmentScan { .. }
        | PhysicalPlan::NodeCartesianProductExec { .. }
        | PhysicalPlan::NodeColumnLookupExec { .. }
        | PhysicalPlan::IndexNodeSeek { .. }
        | PhysicalPlan::IndexNodeMultiSeek { .. }
        | PhysicalPlan::IndexNodeCompositeSeek { .. }
        | PhysicalPlan::IndexNodeRangeSeek { .. }
        | PhysicalPlan::IndexNodeTextSeek { .. }
        | PhysicalPlan::AdjacencyExpandExec { .. }
        | PhysicalPlan::OptionalDegreeExec { .. }
        | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
        | PhysicalPlan::ThreadRepairStatsExec { .. }
        | PhysicalPlan::ShortestPathExec { .. }
        | PhysicalPlan::FilterExec { .. }
        | PhysicalPlan::ProjectExec { .. }
        | PhysicalPlan::AggregateExec { .. }
        | PhysicalPlan::DistinctExec { .. }
        | PhysicalPlan::SortExec { .. }
        | PhysicalPlan::TopNExec { .. }
        | PhysicalPlan::LimitExec { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::VectorSeedScan { .. } => Ok(None),
    }
}

pub fn is_mutation_plan(plan: &PhysicalPlan) -> Result<bool> {
    Ok(matches!(
        plan.class(),
        skein_plan::PhysicalPlanClass::Schema | skein_plan::PhysicalPlanClass::Mutation
    ))
}

fn node_set_assignment(assignment: &crate::planner::SetAssignment) -> NodeSetAssignment {
    NodeSetAssignment {
        property: assignment.property.clone(),
        value: node_set_value(&assignment.value),
    }
}

fn node_set_value(value: &SetValue) -> NodeSetValue {
    match value {
        SetValue::Value(value) => NodeSetValue::Value(value.clone()),
        SetValue::Coalesce { default, .. } => NodeSetValue::Coalesce {
            default: default.clone(),
        },
        SetValue::AddInt { amount, .. } => NodeSetValue::AddInt { amount: *amount },
        SetValue::DecrementFloorZero { .. } => NodeSetValue::DecrementFloorZero,
        SetValue::PreserveNewerExisting {
            incoming, preserve, ..
        } => NodeSetValue::PreserveNewerExisting {
            incoming: incoming.clone(),
            preserve: *preserve,
        },
    }
}

fn relationship_on_create_property_value(
    value: &RelationshipOnCreateValue,
) -> RelationshipOnCreatePropertyValue {
    match value {
        RelationshipOnCreateValue::Value(value) => {
            RelationshipOnCreatePropertyValue::Value(value.clone())
        }
        RelationshipOnCreateValue::MatchedRelationshipProperty { property } => {
            RelationshipOnCreatePropertyValue::MatchedRelationshipProperty {
                property: property.clone(),
            }
        }
    }
}

fn try_projected_graph_with_node_filter(
    catalog: &Catalog,
    store: &GraphStore,
    node_labels: &[String],
    rel_types: &[String],
    include_node: impl Fn(&NodeRecord) -> bool,
    layout: ProjectionLayout,
    budget: ProjectionMemoryBudget,
) -> Result<ProjectedGraph> {
    if node_labels.is_empty() && rel_types.is_empty() {
        return ProjectedGraph::try_from_store_with_node_filter_and_layout(
            store,
            None,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let label_ids = node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            store,
            &[],
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let rel_type_ids = rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    if !rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::try_from_store_without_edges_with_node_filter_and_layout(
                store,
                include_node,
                layout,
                budget,
            )
            .map_err(|error| SkeinError::Execution(error.to_string()));
        }
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            store,
            &label_ids,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    ProjectedGraph::try_from_store_labels_and_rel_types_with_node_filter_and_layout(
        store,
        &label_ids,
        &rel_type_ids,
        include_node,
        layout,
        budget,
    )
    .map_err(|error| SkeinError::Execution(error.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchControl {
    Continue,
    Stop,
}

type BindingBatch = Vec<Binding>;

#[derive(Clone, Copy)]
struct BatchReadContext<'a> {
    catalog: &'a Catalog,
    store: &'a GraphStore,
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
}

fn runtime_checkpoint(task_context: Option<&RuntimeTaskContext>) -> Result<()> {
    match task_context {
        Some(task_context) => task_context
            .checkpoint()
            .map_err(|reason| SkeinError::Execution(format!("runtime task stopped: {reason}"))),
        None => Ok(()),
    }
}

fn batch_pipeline_capable(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqNodeScan { .. }
        | PhysicalPlan::SourceSegmentScan { .. }
        | PhysicalPlan::IndexNodeSeek { .. }
        | PhysicalPlan::IndexNodeMultiSeek { .. }
        | PhysicalPlan::IndexNodeCompositeSeek { .. }
        | PhysicalPlan::IndexNodeRangeSeek { .. }
        | PhysicalPlan::IndexNodeTextSeek { .. }
        | PhysicalPlan::ThreadRepairStatsExec { .. }
        | PhysicalPlan::ShortestPathExec { .. } => true,
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::TopNExec { input, .. }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. } => batch_pipeline_capable(input),
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            batch_pipeline_capable(left) && batch_pipeline_capable(right)
        }
        PhysicalPlan::AdjacencyExpandExec { input, .. } => batch_pipeline_capable(input),
        _ => false,
    }
}

fn collect_batch_pipeline(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &GraphStore,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let context = BatchReadContext {
        catalog,
        store,
        memory,
        task_context,
    };
    execute_binding_batches(plan, context, execution_limit, &mut |batch| {
        for binding in batch {
            push_bounded_operator_binding(
                "MaterializedBatchPipeline",
                &mut output,
                binding,
                &mut tracker,
            )?;
            if execution_limit.is_reached(output.len()) {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    })?;
    Ok(output)
}

fn execute_binding_batches(
    plan: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let mut measured_emit = |batch: BindingBatch| {
        runtime_checkpoint(context.task_context)?;
        let control =
            emit_byte_bounded_batches(batch, context.memory.batch_payload_bytes.get(), emit)?;
        runtime_checkpoint(context.task_context)?;
        Ok(control)
    };
    execute_binding_batches_inner(plan, context, execution_limit, &mut measured_emit)
}

fn emit_byte_bounded_batches(
    batch: BindingBatch,
    max_payload_bytes: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut bounded = Vec::with_capacity(batch.len());
    let mut bounded_bytes = 0usize;
    for binding in batch {
        let binding_bytes = binding_memory_bytes(&binding);
        if binding_bytes > max_payload_bytes {
            return Err(SkeinError::Execution(format!(
                "intermediate row uses {binding_bytes} bytes, exceeding batch_payload_bytes {max_payload_bytes}"
            )));
        }
        if !bounded.is_empty() && bounded_bytes.saturating_add(binding_bytes) > max_payload_bytes {
            record_pipeline_batch(&bounded);
            if emit(std::mem::take(&mut bounded))? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            bounded_bytes = 0;
        }
        bounded_bytes = bounded_bytes.saturating_add(binding_bytes);
        bounded.push(binding);
    }
    if !bounded.is_empty() {
        record_pipeline_batch(&bounded);
        if emit(bounded)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
    }
    Ok(BatchControl::Continue)
}

fn execute_binding_batches_inner(
    plan: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    debug_assert!(batch_pipeline_capable(plan));
    let BatchReadContext {
        catalog,
        store,
        memory,
        task_context: _,
    } = context;
    match plan {
        PhysicalPlan::SeqNodeScan { variable, label } => {
            stream_node_scan_batches(variable, label, None, context, execution_limit, emit)
        }
        PhysicalPlan::SourceSegmentScan {
            variable,
            predicate,
        } => {
            let bindings = execute_source_segment_scan(
                variable,
                predicate,
                catalog,
                store,
                execution_limit,
                memory,
                context.task_context,
            )?;
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::IndexNodeSeek {
            variable,
            label,
            property,
            value,
        } => stream_index_node_seek_batches(
            variable,
            label,
            property,
            std::slice::from_ref(value),
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::IndexNodeMultiSeek {
            variable,
            label,
            property,
            values,
        } => stream_index_node_seek_batches(
            variable,
            label,
            property,
            values,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(
                variable,
                memory.batch_rows.get(),
                execution_limit,
                emit,
                |consumer| {
                    store.visit_nodes_by_composite_property_owned(label_id, predicates, consumer)
                },
            )
        }
        PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            lower,
            upper,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(
                variable,
                memory.batch_rows.get(),
                execution_limit,
                emit,
                |consumer| {
                    store.visit_nodes_by_property_range_owned(
                        label_id,
                        property,
                        lower.as_ref(),
                        upper.as_ref(),
                        consumer,
                    )
                },
            )
        }
        PhysicalPlan::IndexNodeTextSeek {
            variable,
            label,
            property,
            query,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(BatchControl::Continue);
            };
            stream_visited_node_batches(
                variable,
                memory.batch_rows.get(),
                execution_limit,
                emit,
                |consumer| {
                    store.visit_nodes_by_full_text_property_owned(
                        label_id, property, query, consumer,
                    )
                },
            )
        }
        PhysicalPlan::ShortestPathExec {
            source_label,
            source_id,
            source_visibility_predicate,
            rel_type,
            direction,
            target_label,
            target_id,
            target_visibility_predicate,
            min_hops,
            max_hops,
            returns,
            ..
        } => {
            let source_visibility_filter = source_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let target_visibility_filter = target_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let bindings = execute_shortest_path(
                catalog,
                store,
                ShortestPathExecInput {
                    source_label,
                    source_id,
                    source_visibility_filter: source_visibility_filter.as_ref(),
                    path_node_visibility_filter: source_visibility_filter.as_ref(),
                    rel_type,
                    direction: *direction,
                    target_label,
                    target_id,
                    target_visibility_filter: target_visibility_filter.as_ref(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    returns,
                },
                context.memory,
                execution_limit,
                context.task_context,
            )?;
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::ThreadRepairStatsExec {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => {
            let bindings = thread_repair_stats_rows(
                catalog,
                store,
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
                memory.blocking_operator_bytes,
            )?;
            emit_owned_binding_batches(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::NodeColumnLookupExec {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => stream_node_column_lookup_batches(
            NodeColumnLookupSpec {
                variable,
                label,
                property,
                column,
                optional: *optional,
            },
            input,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::OptionalDegreeExec {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => stream_optional_degree_batches(
            source_variable,
            rel_type,
            rel_properties,
            *direction,
            target_label,
            target_properties,
            alias,
            input,
            context,
            execution_limit,
            emit,
        ),
        PhysicalPlan::AdjacencyExpandExec { input, .. } => stream_adjacency_expand_batches(
            plan,
            input,
            context,
            execution_limit,
            AdjacencyExpandFilters::default(),
            emit,
        ),
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            stream_cartesian_product_batches(left, right, context, execution_limit, emit)
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            if let PhysicalPlan::SeqNodeScan { variable, label } = input.as_ref()
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return stream_node_scan_batches(
                    variable,
                    label,
                    Some((predicate, &filter)),
                    context,
                    execution_limit,
                    emit,
                );
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                rel_variable: Some(rel_variable),
                input: expand_input,
                ..
            } = input.as_ref()
                && let Some(filter) =
                    exact_relationship_scan_filter_from_predicate(predicate, rel_variable)
            {
                return stream_filtered_adjacency_expand_batches(
                    input,
                    expand_input,
                    predicate,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: Some(&filter),
                        target_scan_filter: None,
                    },
                    emit,
                );
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                target_variable,
                input: expand_input,
                ..
            } = input.as_ref()
                && predicate_references_only_variable(predicate, target_variable)
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return stream_filtered_adjacency_expand_batches(
                    input,
                    expand_input,
                    predicate,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: None,
                        target_scan_filter: Some(&filter),
                    },
                    emit,
                );
            }
            let mut emitted = 0usize;
            execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
                let remaining = execution_limit
                    .output_rows
                    .unwrap_or(usize::MAX)
                    .saturating_sub(emitted);
                if remaining == 0 {
                    return Ok(BatchControl::Stop);
                }
                let mut filtered = Vec::with_capacity(batch.len().min(remaining));
                for binding in batch {
                    if evaluate_predicate(predicate, catalog, store, &binding)? {
                        filtered.push(binding);
                        if filtered.len() == remaining {
                            break;
                        }
                    }
                }
                emitted = emitted.saturating_add(filtered.len());
                if !filtered.is_empty() && emit(filtered)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                Ok(if execution_limit.is_reached(emitted) {
                    BatchControl::Stop
                } else {
                    BatchControl::Continue
                })
            })
        }
        PhysicalPlan::ProjectExec { items, input } => {
            let mut emitted = 0usize;
            execute_binding_batches(input, context, execution_limit, &mut |batch| {
                let mut projected = Vec::with_capacity(batch.len());
                for binding in batch {
                    let mut values = BTreeMap::new();
                    for item in items {
                        let value = project_value(item, catalog, &binding)?;
                        insert_projected_value(&mut values, &item.name, value);
                    }
                    projected.push(Binding {
                        values,
                        nodes: binding.nodes,
                        relationships: binding.relationships,
                    });
                }
                emitted = emitted.saturating_add(projected.len());
                if !projected.is_empty() && emit(projected)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                Ok(if execution_limit.is_reached(emitted) {
                    BatchControl::Stop
                } else {
                    BatchControl::Continue
                })
            })
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => {
            let mut skipped = 0usize;
            let mut emitted = 0usize;
            let output_cap = match (limit, execution_limit.output_rows) {
                (Some(limit), Some(parent)) => (*limit).min(parent),
                (Some(limit), None) => *limit,
                (None, Some(parent)) => parent,
                (None, None) => usize::MAX,
            };
            execute_binding_batches(
                input,
                context,
                ExecutionLimit {
                    output_rows: Some(offset.saturating_add(output_cap)),
                },
                &mut |batch| {
                    let mut output = Vec::new();
                    for binding in batch {
                        if skipped < *offset {
                            skipped += 1;
                            continue;
                        }
                        if emitted == output_cap {
                            break;
                        }
                        output.push(binding);
                        emitted += 1;
                    }
                    if !output.is_empty() && emit(output)? == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                    Ok(if emitted == output_cap {
                        BatchControl::Stop
                    } else {
                        BatchControl::Continue
                    })
                },
            )
        }
        PhysicalPlan::TopNExec {
            items,
            offset,
            limit,
            input,
        } => {
            let retained = offset.saturating_add(*limit);
            if retained == 0 {
                return Ok(BatchControl::Continue);
            }
            let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
            let mut spill_budget = SpillBudgetTracker::new("TopNExec", memory);
            let mut runs = Vec::<spill::SpillRun>::new();
            let mut heap = BinaryHeap::new();
            let mut ordinal = 0u64;
            let mut spilled_rows = 0usize;
            execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
                for binding in batch {
                    let sort_values = items
                        .iter()
                        .map(|item| (sort_value(catalog, &binding, &item.key), item.direction))
                        .collect();
                    let candidate = TopNBinding {
                        sort_values,
                        ordinal,
                        binding,
                    };
                    ordinal = ordinal.saturating_add(1);
                    let bytes = candidate.memory_bytes();
                    ensure_operator_item_fits("TopNExec", bytes, &tracker)?;
                    if heap.len() < retained {
                        if tracker.would_exceed(bytes) {
                            spilled_rows = spilled_rows.saturating_add(heap.len());
                            runs.push(spill_top_n_run(
                                &mut heap,
                                &memory.spill_directory,
                                &mut spill_budget,
                                context.task_context,
                            )?);
                            tracker.reset();
                        }
                        tracker.charge(bytes);
                        heap.push(candidate);
                    } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                        let worst_bytes = heap.peek().map(TopNBinding::memory_bytes).unwrap_or(0);
                        if tracker
                            .used_bytes
                            .saturating_sub(worst_bytes)
                            .saturating_add(bytes)
                            > tracker.budget_bytes
                        {
                            spilled_rows = spilled_rows.saturating_add(heap.len());
                            runs.push(spill_top_n_run(
                                &mut heap,
                                &memory.spill_directory,
                                &mut spill_budget,
                                context.task_context,
                            )?);
                            tracker.reset();
                        } else {
                            heap.pop();
                            tracker.release(worst_bytes);
                        }
                        tracker.charge(bytes);
                        heap.push(candidate);
                    }
                }
                Ok(BatchControl::Continue)
            })?;

            if !runs.is_empty() {
                if !heap.is_empty() {
                    spilled_rows = spilled_rows.saturating_add(heap.len());
                    runs.push(spill_top_n_run(
                        &mut heap,
                        &memory.spill_directory,
                        &mut spill_budget,
                        context.task_context,
                    )?);
                }
                runs = compact_sort_runs(
                    runs,
                    items,
                    catalog,
                    memory,
                    &mut spill_budget,
                    context.task_context,
                )?;
                record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
                    operator: "TopNExec".to_string(),
                    budget_bytes: tracker.budget_bytes,
                    peak_tracked_bytes: tracker.peak_bytes,
                    input_rows: ordinal as usize,
                    max_spill_bytes: spill_budget.max_bytes,
                    max_spill_runs: spill_budget.max_runs,
                    spilled_bytes: spill_budget.used_bytes,
                    spill_run_count: spill_budget.run_count,
                    spilled_rows,
                });
                return merge_sort_runs(
                    &runs,
                    items,
                    catalog,
                    memory.blocking_operator_bytes,
                    memory.batch_rows.get(),
                    *offset,
                    (*limit).min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                    context.task_context,
                    emit,
                );
            }
            record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
                operator: "TopNExec".to_string(),
                budget_bytes: tracker.budget_bytes,
                peak_tracked_bytes: tracker.peak_bytes,
                input_rows: ordinal as usize,
                max_spill_bytes: memory.max_spill_bytes.get(),
                max_spill_runs: memory.max_spill_runs.get(),
                spilled_bytes: 0,
                spill_run_count: 0,
                spilled_rows: 0,
            });
            let mut selected = heap.into_vec();
            selected.sort();
            let bindings = selected
                .into_iter()
                .skip(*offset)
                .take(*limit)
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .map(|entry| entry.binding);
            emit_binding_iterator(bindings, memory.batch_rows.get(), emit)
        }
        PhysicalPlan::SortExec { items, input } => {
            stream_sort_batches(input, items, context, execution_limit, emit)
        }
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => stream_aggregate_batches(input, group_keys, items, context, execution_limit, emit),
        PhysicalPlan::DistinctExec { input } => {
            stream_distinct_batches(input, context, execution_limit, emit)
        }
        _ => unreachable!("batch pipeline capability check rejected this operator"),
    }
}

fn stream_cartesian_product_batches(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut tracker = OperatorMemoryTracker::new(context.memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("NodeCartesianProductExec", context.memory);
    let mut right_bindings = Vec::new();
    let mut runs = Vec::new();
    let mut right_ordinal = 0u64;
    execute_binding_batches(right, context, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            let bytes = binding_memory_bytes(&binding);
            ensure_operator_item_fits("NodeCartesianProductExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                runs.push(spill_binding_run(
                    "cartesian",
                    &mut right_bindings,
                    &context.memory.spill_directory,
                    &mut spill_budget,
                    context.task_context,
                )?);
                tracker.reset();
            }
            tracker.charge(bytes);
            right_bindings.push(binding);
            right_ordinal = right_ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;
    if !runs.is_empty() && !right_bindings.is_empty() {
        runs.push(spill_binding_run(
            "cartesian",
            &mut right_bindings,
            &context.memory.spill_directory,
            &mut spill_budget,
            context.task_context,
        )?);
        tracker.reset();
    }
    record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
        operator: "NodeCartesianProductExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes: tracker.peak_bytes,
        input_rows: right_ordinal as usize,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows: if runs.is_empty() {
            0
        } else {
            right_ordinal as usize
        },
    });
    if right_bindings.is_empty() && runs.is_empty() {
        return Ok(BatchControl::Continue);
    }

    let mut output = Vec::with_capacity(context.memory.batch_rows.get());
    let mut emitted = 0usize;
    let control =
        execute_binding_batches(left, context, ExecutionLimit::unlimited(), &mut |batch| {
            for left_binding in batch {
                if runs.is_empty() {
                    for right_binding in &right_bindings {
                        if push_cartesian_output(
                            &left_binding,
                            right_binding,
                            context.memory.batch_rows.get(),
                            execution_limit,
                            &mut output,
                            &mut emitted,
                            emit,
                        )? == BatchControl::Stop
                        {
                            return Ok(BatchControl::Stop);
                        }
                    }
                } else {
                    for run in &runs {
                        runtime_checkpoint(context.task_context)?;
                        let mut reader = run.reader()?;
                        while let Some((_, right_binding)) =
                            reader.read(context.memory.blocking_operator_bytes.get())?
                        {
                            runtime_checkpoint(context.task_context)?;
                            if push_cartesian_output(
                                &left_binding,
                                &right_binding,
                                context.memory.batch_rows.get(),
                                execution_limit,
                                &mut output,
                                &mut emitted,
                                emit,
                            )? == BatchControl::Stop
                            {
                                return Ok(BatchControl::Stop);
                            }
                        }
                    }
                    if execution_limit.is_reached(emitted) {
                        return Ok(BatchControl::Stop);
                    }
                }
            }
            Ok(BatchControl::Continue)
        })?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(control)
}

#[allow(clippy::too_many_arguments)]
fn push_cartesian_output(
    left: &Binding,
    right: &Binding,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    output: &mut BindingBatch,
    emitted: &mut usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut values = left.values.clone();
    values.extend(right.values.clone());
    let mut nodes = left.nodes.clone();
    nodes.extend(right.nodes.clone());
    let mut relationships = left.relationships.clone();
    relationships.extend(right.relationships.clone());
    output.push(Binding {
        values,
        nodes,
        relationships,
    });
    *emitted = (*emitted).saturating_add(1);
    if output.len() == batch_rows
        && emit(std::mem::replace(output, Vec::with_capacity(batch_rows)))? == BatchControl::Stop
    {
        return Ok(BatchControl::Stop);
    }
    Ok(if execution_limit.is_reached(*emitted) {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

fn spill_binding_run(
    operator: &str,
    bindings: &mut Vec<Binding>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, operator)?;
    for binding in bindings.drain(..) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(0, &binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    writer.finish()?;
    Ok(run)
}

fn stream_distinct_batches(
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut tracker = OperatorMemoryTracker::new(context.memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("DistinctExec", context.memory);
    let mut distinct = BTreeMap::<Vec<(String, Value)>, (u64, Binding)>::new();
    let mut runs = Vec::new();
    let mut ordinal = 0u64;
    execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            let key = distinct_binding_key(&binding);
            let entry_bytes =
                binding_memory_bytes(&binding).saturating_add(distinct_key_memory_bytes(&key));
            ensure_operator_item_fits("DistinctExec", entry_bytes, &tracker)?;
            if !distinct.contains_key(&key) {
                if tracker.would_exceed(entry_bytes) {
                    runs.push(spill_distinct_run(
                        &mut distinct,
                        &context.memory.spill_directory,
                        &mut spill_budget,
                        context.task_context,
                    )?);
                    tracker.reset();
                }
                tracker.charge(entry_bytes);
                distinct.insert(key, (ordinal, binding));
            }
            ordinal = ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;
    if runs.is_empty() {
        record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
            operator: "DistinctExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows: ordinal as usize,
            max_spill_bytes: context.memory.max_spill_bytes.get(),
            max_spill_runs: context.memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        let mut selected = distinct.into_values().collect::<Vec<_>>();
        selected.sort_by_key(|(ordinal, _)| *ordinal);
        return emit_binding_iterator(
            selected
                .into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .map(|(_, binding)| binding),
            context.memory.batch_rows.get(),
            emit,
        );
    }
    if !distinct.is_empty() {
        runs.push(spill_distinct_run(
            &mut distinct,
            &context.memory.spill_directory,
            &mut spill_budget,
            context.task_context,
        )?);
        tracker.reset();
    }
    let mut peak_tracked_bytes = tracker.peak_bytes;
    runs = compact_distinct_runs(
        runs,
        context.memory,
        &mut spill_budget,
        context.task_context,
        &mut peak_tracked_bytes,
    )?;
    record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
        operator: "DistinctExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes,
        input_rows: ordinal as usize,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows: ordinal as usize,
    });
    emit_distinct_run(
        runs.first().expect("compaction retains one distinct run"),
        context.memory.blocking_operator_bytes,
        context.memory.batch_rows.get(),
        execution_limit,
        context.task_context,
        emit,
    )
}

fn distinct_binding_key(binding: &Binding) -> Vec<(String, Value)> {
    binding
        .values
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn spill_distinct_run(
    distinct: &mut BTreeMap<Vec<(String, Value)>, (u64, Binding)>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "distinct")?;
    for (_, (ordinal, binding)) in std::mem::take(distinct) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(ordinal, &binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    writer.finish()?;
    Ok(run)
}

fn compact_distinct_runs(
    mut runs: Vec<spill::SpillRun>,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
    peak_tracked_bytes: &mut usize,
) -> Result<Vec<spill::SpillRun>> {
    while runs.len() > 1 {
        runtime_checkpoint(task_context)?;
        let mut compacted = Vec::with_capacity(runs.len().div_ceil(2));
        let mut pending = runs.into_iter();
        while let Some(left) = pending.next() {
            let Some(right) = pending.next() else {
                compacted.push(left);
                break;
            };
            compacted.push(merge_distinct_run_pair(
                &left,
                &right,
                memory,
                spill_budget,
                task_context,
                peak_tracked_bytes,
            )?);
        }
        runs = compacted;
    }
    Ok(runs)
}

struct DistinctRunRow {
    key: Vec<(String, Value)>,
    ordinal: u64,
    binding: Binding,
    memory_bytes: usize,
}

fn read_distinct_run_row(
    reader: &mut spill::SpillReader,
    memory_limit: usize,
) -> Result<Option<DistinctRunRow>> {
    let Some((ordinal, binding)) = reader.read(memory_limit)? else {
        return Ok(None);
    };
    let key = distinct_binding_key(&binding);
    let memory_bytes =
        binding_memory_bytes(&binding).saturating_add(distinct_key_memory_bytes(&key));
    if memory_bytes > memory_limit {
        return Err(SkeinError::Execution(format!(
            "DistinctExec spill merge row uses {memory_bytes} bytes, exceeding the per-row memory limit {memory_limit}"
        )));
    }
    Ok(Some(DistinctRunRow {
        key,
        ordinal,
        binding,
        memory_bytes,
    }))
}

fn merge_distinct_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
    peak_tracked_bytes: &mut usize,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let per_row_memory = memory.blocking_operator_bytes.get() / 2;
    if per_row_memory == 0 {
        return Err(SkeinError::Execution(
            "DistinctExec spill merge requires at least two bytes of blocking memory".to_string(),
        ));
    }
    let mut left_reader = left.reader()?;
    let mut right_reader = right.reader()?;
    let mut left_row = read_distinct_run_row(&mut left_reader, per_row_memory)?;
    let mut right_row = read_distinct_run_row(&mut right_reader, per_row_memory)?;
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(&memory.spill_directory, "distinct-merge")?;
    loop {
        runtime_checkpoint(task_context)?;
        *peak_tracked_bytes = (*peak_tracked_bytes).max(
            left_row
                .as_ref()
                .map_or(0, |row| row.memory_bytes)
                .saturating_add(right_row.as_ref().map_or(0, |row| row.memory_bytes)),
        );
        let selected = match (&left_row, &right_row) {
            (None, None) => break,
            (Some(_), None) => left_row.take(),
            (None, Some(_)) => right_row.take(),
            (Some(left), Some(right)) => match left.key.cmp(&right.key) {
                Ordering::Less => left_row.take(),
                Ordering::Greater => right_row.take(),
                Ordering::Equal => {
                    let left = left_row.take().expect("left row exists");
                    let right = right_row.take().expect("right row exists");
                    Some(if left.ordinal <= right.ordinal {
                        left
                    } else {
                        right
                    })
                }
            },
        };
        let selected = selected.expect("distinct merge selected one row");
        let bytes = writer.write(
            selected.ordinal,
            &selected.binding,
            spill_budget.remaining_bytes(),
        )?;
        spill_budget.charge(bytes)?;
        if left_row.is_none() {
            left_row = read_distinct_run_row(&mut left_reader, per_row_memory)?;
        }
        if right_row.is_none() {
            right_row = read_distinct_run_row(&mut right_reader, per_row_memory)?;
        }
    }
    writer.finish()?;
    Ok(run)
}

fn emit_distinct_run(
    run: &spill::SpillRun,
    memory_budget: NonZeroUsize,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut reader = run.reader()?;
    let mut output = Vec::with_capacity(batch_rows);
    let mut emitted = 0usize;
    while let Some((_, binding)) = reader.read(memory_budget.get())? {
        runtime_checkpoint(task_context)?;
        output.push(binding);
        emitted = emitted.saturating_add(1);
        if output.len() == batch_rows
            && emit(std::mem::replace(
                &mut output,
                Vec::with_capacity(batch_rows),
            ))? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if execution_limit.is_reached(emitted) {
            break;
        }
    }
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

struct OperatorMemoryTracker {
    budget_bytes: usize,
    used_bytes: usize,
    peak_bytes: usize,
}

impl OperatorMemoryTracker {
    fn new(budget_bytes: NonZeroUsize) -> Self {
        Self {
            budget_bytes: budget_bytes.get(),
            used_bytes: 0,
            peak_bytes: 0,
        }
    }

    fn would_exceed(&self, bytes: usize) -> bool {
        self.used_bytes.saturating_add(bytes) > self.budget_bytes
    }

    fn charge(&mut self, bytes: usize) {
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.peak_bytes = self.peak_bytes.max(self.used_bytes);
    }

    fn release(&mut self, bytes: usize) {
        self.used_bytes = self.used_bytes.saturating_sub(bytes);
    }

    fn reset(&mut self) {
        self.used_bytes = 0;
    }
}

struct SpillBudgetTracker {
    operator: &'static str,
    max_bytes: u64,
    max_runs: usize,
    used_bytes: u64,
    run_count: usize,
}

impl SpillBudgetTracker {
    fn new(operator: &'static str, memory: &ExecutionMemoryConfig) -> Self {
        Self {
            operator,
            max_bytes: memory.max_spill_bytes.get(),
            max_runs: memory.max_spill_runs.get(),
            used_bytes: 0,
            run_count: 0,
        }
    }

    fn begin_run(&mut self) -> Result<()> {
        if self.run_count == self.max_runs {
            return Err(SkeinError::Execution(format!(
                "{} exceeded max_spill_runs {}",
                self.operator, self.max_runs
            )));
        }
        self.run_count = self.run_count.saturating_add(1);
        Ok(())
    }

    fn remaining_bytes(&self) -> u64 {
        self.max_bytes.saturating_sub(self.used_bytes)
    }

    fn charge(&mut self, bytes: u64) -> Result<()> {
        let next = self.used_bytes.saturating_add(bytes);
        if next > self.max_bytes {
            return Err(SkeinError::Execution(format!(
                "{} exceeded max_spill_bytes {} (next total {})",
                self.operator, self.max_bytes, next
            )));
        }
        self.used_bytes = next;
        Ok(())
    }
}

fn ensure_operator_item_fits(
    operator: &str,
    bytes: usize,
    tracker: &OperatorMemoryTracker,
) -> Result<()> {
    if bytes > tracker.budget_bytes {
        return Err(SkeinError::Execution(format!(
            "{operator} item uses {bytes} bytes, exceeding blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    Ok(())
}

fn push_bounded_operator_binding(
    operator: &str,
    output: &mut Vec<Binding>,
    binding: Binding,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    let bytes = binding_memory_bytes(&binding);
    ensure_operator_item_fits(operator, bytes, tracker)?;
    if tracker.would_exceed(bytes) {
        return Err(SkeinError::Execution(format!(
            "{operator} state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.charge(bytes);
    output.push(binding);
    Ok(())
}

fn collect_bounded_operator_bindings(
    operator: &str,
    bindings: impl IntoIterator<Item = Binding>,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for binding in bindings {
        push_bounded_operator_binding(operator, &mut output, binding, &mut tracker)?;
    }
    Ok(output)
}

fn binding_memory_bytes(binding: &Binding) -> usize {
    std::mem::size_of::<Binding>()
        .saturating_add(binding_payload_bytes(binding))
        .saturating_add(
            binding
                .values
                .len()
                .saturating_add(binding.nodes.len())
                .saturating_add(binding.relationships.len())
                .saturating_mul(std::mem::size_of::<usize>() * 6),
        )
}

fn node_memory_bytes(node: &NodeRecord) -> usize {
    size_of::<NodeRecord>()
        .saturating_add(
            node.labels
                .len()
                .saturating_mul(std::mem::size_of::<crate::schema::LabelId>() * 3),
        )
        .saturating_add(map_memory_bytes(&node.properties))
}

fn relationship_memory_bytes(relationship: &RelRecord) -> usize {
    std::mem::size_of::<RelRecord>().saturating_add(map_memory_bytes(&relationship.properties))
}

fn distinct_key_memory_bytes(key: &[(String, Value)]) -> usize {
    std::mem::size_of::<Vec<(String, Value)>>().saturating_add(key.iter().fold(
        0usize,
        |total, (name, value)| {
            total
                .saturating_add(std::mem::size_of::<(String, Value)>())
                .saturating_add(name.len())
                .saturating_add(value_memory_bytes(value))
        },
    ))
}

struct SortRunRow {
    sort_values: Vec<(Value, SortDirection)>,
    ordinal: u64,
    binding: Binding,
}

impl SortRunRow {
    fn new(catalog: &Catalog, items: &[SortItem], ordinal: u64, binding: Binding) -> Self {
        let sort_values = items
            .iter()
            .map(|item| (sort_value(catalog, &binding, &item.key), item.direction))
            .collect();
        Self {
            sort_values,
            ordinal,
            binding,
        }
    }

    fn cmp_key(&self, other: &Self) -> Ordering {
        compare_sort_values(&self.sort_values, &other.sort_values)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }

    fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding).saturating_add(self.sort_values.iter().fold(
            std::mem::size_of::<Vec<(Value, SortDirection)>>(),
            |total, (value, _)| total.saturating_add(value_memory_bytes(value)),
        ))
    }
}

fn compare_sort_values(
    left: &[(Value, SortDirection)],
    right: &[(Value, SortDirection)],
) -> Ordering {
    for ((left, direction), (right, other_direction)) in left.iter().zip(right) {
        debug_assert_eq!(direction, other_direction);
        let ordering = match direction {
            SortDirection::Asc => left.cmp(right),
            SortDirection::Desc => left.cmp(right).reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

struct SortMergeEntry {
    row: SortRunRow,
    run_index: usize,
}

impl PartialEq for SortMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.cmp_key(&other.row) == Ordering::Equal && self.run_index == other.run_index
    }
}

impl Eq for SortMergeEntry {}

impl Ord for SortMergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for SortMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn stream_sort_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let BatchReadContext {
        catalog, memory, ..
    } = context;
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("SortExec", memory);
    let mut rows = Vec::<SortRunRow>::new();
    let mut runs = Vec::<spill::SpillRun>::new();
    let mut ordinal = 0u64;
    execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(context.task_context)?;
        for binding in batch {
            let row = SortRunRow::new(catalog, items, ordinal, binding);
            let bytes = row.memory_bytes();
            ensure_operator_item_fits("SortExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                runs.push(spill_sort_run(
                    &mut rows,
                    &memory.spill_directory,
                    &mut spill_budget,
                    context.task_context,
                )?);
                tracker.reset();
            }
            tracker.charge(bytes);
            rows.push(row);
            ordinal = ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;

    if runs.is_empty() {
        runtime_checkpoint(context.task_context)?;
        record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
            operator: "SortExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows: ordinal as usize,
            max_spill_bytes: memory.max_spill_bytes.get(),
            max_spill_runs: memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        rows.sort_by(SortRunRow::cmp_key);
        return emit_binding_iterator(
            rows.into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .map(|row| row.binding),
            memory.batch_rows.get(),
            emit,
        );
    }
    if !rows.is_empty() {
        runs.push(spill_sort_run(
            &mut rows,
            &memory.spill_directory,
            &mut spill_budget,
            context.task_context,
        )?);
    }
    runs = compact_sort_runs(
        runs,
        items,
        catalog,
        memory,
        &mut spill_budget,
        context.task_context,
    )?;
    record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
        operator: "SortExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes: tracker.peak_bytes,
        input_rows: ordinal as usize,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows: ordinal as usize,
    });
    merge_sort_runs(
        &runs,
        items,
        catalog,
        memory.blocking_operator_bytes,
        memory.batch_rows.get(),
        0,
        execution_limit.output_rows.unwrap_or(usize::MAX),
        context.task_context,
        emit,
    )
}

fn spill_sort_run(
    rows: &mut Vec<SortRunRow>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    rows.sort_by(SortRunRow::cmp_key);
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "sort")?;
    for row in rows.drain(..) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(row.ordinal, &row.binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

fn spill_top_n_run(
    heap: &mut BinaryHeap<TopNBinding>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut rows = std::mem::take(heap).into_vec();
    rows.sort();
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "topn")?;
    for row in rows {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(row.ordinal, &row.binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

fn compact_sort_runs(
    mut runs: Vec<spill::SpillRun>,
    items: &[SortItem],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<spill::SpillRun>> {
    while runs.len() > 2 {
        runtime_checkpoint(task_context)?;
        let mut compacted = Vec::with_capacity(runs.len().div_ceil(2));
        let mut pending = runs.into_iter();
        while let Some(left) = pending.next() {
            let Some(right) = pending.next() else {
                compacted.push(left);
                break;
            };
            compacted.push(merge_sort_run_pair(
                &left,
                &right,
                items,
                catalog,
                memory,
                spill_budget,
                task_context,
            )?);
        }
        runs = compacted;
    }
    Ok(runs)
}

#[allow(clippy::too_many_arguments)]
fn merge_sort_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    items: &[SortItem],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut readers = [left.reader()?, right.reader()?];
    let mut heap = BinaryHeap::new();
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let per_row_budget = memory.blocking_operator_bytes.get() / 2;
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some((ordinal, binding)) = reader.read(memory.blocking_operator_bytes.get())? {
            let entry = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            if bytes > per_row_budget {
                return Err(SkeinError::Execution(format!(
                    "SortExec spill merge row uses {bytes} bytes, exceeding half of blocking_operator_bytes {}",
                    memory.blocking_operator_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(entry);
        }
    }
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(&memory.spill_directory, "sort-merge")?;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        tracker.release(entry.row.memory_bytes());
        let run_index = entry.run_index;
        let bytes = writer.write(
            entry.row.ordinal,
            &entry.row.binding,
            spill_budget.remaining_bytes(),
        )?;
        spill_budget.charge(bytes)?;
        if let Some((ordinal, binding)) =
            readers[run_index].read(memory.blocking_operator_bytes.get())?
        {
            let next = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = next.row.memory_bytes();
            if bytes > per_row_budget || tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "SortExec spill merge exceeds blocking_operator_bytes {}",
                    memory.blocking_operator_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(next);
        }
    }
    writer.finish()?;
    Ok(run)
}

#[allow(clippy::too_many_arguments)]
fn merge_sort_runs(
    runs: &[spill::SpillRun],
    items: &[SortItem],
    catalog: &Catalog,
    memory_budget: NonZeroUsize,
    batch_rows: usize,
    skip_rows: usize,
    output_rows: usize,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(task_context)?;
    let mut readers = runs
        .iter()
        .map(spill::SpillRun::reader)
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for (run_index, reader) in readers.iter_mut().enumerate() {
        runtime_checkpoint(task_context)?;
        if let Some((ordinal, binding)) = reader.read(memory_budget.get())? {
            let entry = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            ensure_operator_item_fits("SortExec merge", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "SortExec merge fan-in uses more than blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(entry);
        }
    }
    if output_rows == 0 {
        return Ok(BatchControl::Continue);
    }
    let mut skipped = 0usize;
    let mut emitted = 0usize;
    let mut batch = Vec::with_capacity(batch_rows);
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        tracker.release(entry.row.memory_bytes());
        let run_index = entry.run_index;
        if let Some((ordinal, binding)) = readers[run_index].read(memory_budget.get())? {
            let next = SortMergeEntry {
                row: SortRunRow::new(catalog, items, ordinal, binding),
                run_index,
            };
            let bytes = next.row.memory_bytes();
            ensure_operator_item_fits("SortExec merge", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "SortExec merge fan-in uses more than blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(next);
        }
        if skipped < skip_rows {
            skipped = skipped.saturating_add(1);
            continue;
        }
        batch.push(entry.row.binding);
        emitted = emitted.saturating_add(1);
        if (batch.len() == batch_rows || emitted == output_rows)
            && emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            ))? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if emitted == output_rows {
            return Ok(BatchControl::Stop);
        }
    }
    runtime_checkpoint(task_context)?;
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum AggregateDistinctValue {
    Identity(u8, u64),
    Value(Value),
}

enum AggregateState {
    Count {
        count: usize,
        distinct: Option<BTreeSet<AggregateDistinctValue>>,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Avg {
        sum: f64,
        count: usize,
    },
    Collect {
        values: Vec<Value>,
        distinct: Option<BTreeSet<Value>>,
    },
}

#[derive(Default)]
struct MemoryDelta {
    added_bytes: usize,
    released_bytes: usize,
}

impl MemoryDelta {
    fn between(previous: usize, next: usize) -> Self {
        if next >= previous {
            Self {
                added_bytes: next - previous,
                released_bytes: 0,
            }
        } else {
            Self {
                added_bytes: 0,
                released_bytes: previous - next,
            }
        }
    }

    fn combine(&mut self, other: Self) {
        self.added_bytes = self.added_bytes.saturating_add(other.added_bytes);
        self.released_bytes = self.released_bytes.saturating_add(other.released_bytes);
    }
}

impl AggregateState {
    fn new(item: &Aggregation) -> Self {
        match item.function {
            AggregateFunction::Count => Self::Count {
                count: 0,
                distinct: item.distinct.then(BTreeSet::new),
            },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Collect => Self::Collect {
                values: Vec::new(),
                distinct: item.distinct.then(BTreeSet::new),
            },
        }
    }

    fn update(&mut self, item: &Aggregation, catalog: &Catalog, binding: &Binding) -> MemoryDelta {
        match self {
            Self::Count { count, distinct } => {
                if distinct.is_none() {
                    let matched = match &item.target {
                        AggregateTarget::All => true,
                        AggregateTarget::Variable(variable) => {
                            binding_has_variable(binding, variable)
                        }
                        AggregateTarget::Property { variable, property } => {
                            binding_property(binding, variable, property)
                                .is_some_and(|value| value != &Value::Null)
                        }
                    };
                    if matched {
                        *count = count.saturating_add(1);
                    }
                    return MemoryDelta::default();
                }
                let value = match &item.target {
                    AggregateTarget::All => {
                        *count = count.saturating_add(1);
                        return MemoryDelta::default();
                    }
                    AggregateTarget::Variable(variable) => binding_identity_key(binding, variable)
                        .map(|(kind, id)| AggregateDistinctValue::Identity(kind, id)),
                    AggregateTarget::Property { variable, property } => binding
                        .nodes
                        .get(variable)
                        .and_then(|node| node.properties.get(property))
                        .or_else(|| {
                            binding
                                .relationships
                                .get(variable)
                                .and_then(|relationship| relationship.properties.get(property))
                        })
                        .filter(|value| *value != &Value::Null)
                        .cloned()
                        .map(AggregateDistinctValue::Value),
                };
                let Some(value) = value else {
                    return MemoryDelta::default();
                };
                let value_bytes = aggregate_distinct_value_memory_bytes(&value)
                    .saturating_add(std::mem::size_of::<usize>() * 4);
                if let Some(distinct) = distinct {
                    if distinct.insert(value) {
                        *count = count.saturating_add(1);
                        return MemoryDelta {
                            added_bytes: value_bytes,
                            released_bytes: 0,
                        };
                    }
                } else {
                    *count = count.saturating_add(1);
                }
                MemoryDelta::default()
            }
            Self::Min(current) => {
                if let Some(value) = aggregate_property_value(&item.target, binding)
                    && current.as_ref().is_none_or(|current| value < *current)
                {
                    let previous = current.as_ref().map_or(0, value_memory_bytes);
                    let next = value_memory_bytes(&value);
                    *current = Some(value);
                    return MemoryDelta::between(previous, next);
                }
                MemoryDelta::default()
            }
            Self::Max(current) => {
                if let Some(value) = aggregate_property_value(&item.target, binding)
                    && current.as_ref().is_none_or(|current| value > *current)
                {
                    let previous = current.as_ref().map_or(0, value_memory_bytes);
                    let next = value_memory_bytes(&value);
                    *current = Some(value);
                    return MemoryDelta::between(previous, next);
                }
                MemoryDelta::default()
            }
            Self::Avg { sum, count } => {
                if let Some(value) = aggregate_property_value(&item.target, binding) {
                    match value {
                        Value::Int(value) => {
                            *sum += value as f64;
                            *count = count.saturating_add(1);
                        }
                        Value::Float(value) if value.is_finite() => {
                            *sum += value;
                            *count = count.saturating_add(1);
                        }
                        _ => {}
                    }
                }
                MemoryDelta::default()
            }
            Self::Collect { values, distinct } => {
                let value = match &item.target {
                    AggregateTarget::Variable(variable) => {
                        binding_value(binding, catalog, variable)
                    }
                    AggregateTarget::Property { .. } => {
                        aggregate_property_value(&item.target, binding)
                    }
                    AggregateTarget::All => None,
                };
                let Some(value) = value.filter(|value| value != &Value::Null) else {
                    return MemoryDelta::default();
                };
                let value_bytes =
                    value_memory_bytes(&value).saturating_add(std::mem::size_of::<usize>() * 4);
                if let Some(distinct) = distinct {
                    if distinct.insert(value) {
                        return MemoryDelta {
                            added_bytes: value_bytes,
                            released_bytes: 0,
                        };
                    }
                } else {
                    values.push(value);
                    return MemoryDelta {
                        added_bytes: value_bytes,
                        released_bytes: 0,
                    };
                }
                MemoryDelta::default()
            }
        }
    }

    fn finish(self) -> Value {
        match self {
            Self::Count { count, .. } => Value::Int(count as i64),
            Self::Min(value) | Self::Max(value) => value.unwrap_or(Value::Null),
            Self::Avg { sum, count } if count > 0 => Value::Float(sum / count as f64),
            Self::Avg { .. } => Value::Null,
            Self::Collect {
                values,
                distinct: None,
            } => Value::List(values),
            Self::Collect {
                distinct: Some(values),
                ..
            } => Value::List(values.into_iter().collect()),
        }
    }
}

fn aggregate_distinct_value_memory_bytes(value: &AggregateDistinctValue) -> usize {
    std::mem::size_of::<AggregateDistinctValue>().saturating_add(match value {
        AggregateDistinctValue::Identity(_, _) => 0,
        AggregateDistinctValue::Value(value) => value_memory_bytes(value),
    })
}

fn aggregate_property_value(target: &AggregateTarget, binding: &Binding) -> Option<Value> {
    let AggregateTarget::Property { variable, property } = target else {
        return None;
    };
    binding_property(binding, variable, property)
        .filter(|value| *value != &Value::Null)
        .cloned()
}

struct GroupAccumulator<'a> {
    key: Vec<Value>,
    group_keys: &'a [Projection],
    items: &'a [Aggregation],
    states: Vec<AggregateState>,
}

impl<'a> GroupAccumulator<'a> {
    fn new(key: Vec<Value>, group_keys: &'a [Projection], items: &'a [Aggregation]) -> Self {
        Self {
            key,
            group_keys,
            items,
            states: items.iter().map(AggregateState::new).collect(),
        }
    }

    fn base_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.key.iter().fold(0usize, |total, value| {
                total.saturating_add(value_memory_bytes(value))
            }))
            .saturating_add(
                self.states
                    .len()
                    .saturating_mul(std::mem::size_of::<AggregateState>()),
            )
    }

    fn update(&mut self, catalog: &Catalog, binding: &Binding) -> MemoryDelta {
        let mut delta = MemoryDelta::default();
        for (state, item) in self.states.iter_mut().zip(self.items) {
            delta.combine(state.update(item, catalog, binding));
        }
        delta
    }

    fn finish(self) -> Binding {
        let mut values = BTreeMap::new();
        for (item, value) in self.group_keys.iter().zip(self.key) {
            insert_projected_value(&mut values, &item.name, value);
        }
        for (item, state) in self.items.iter().zip(self.states) {
            insert_projected_value(&mut values, &item.name, state.finish());
        }
        Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }
}

struct GroupRunRow {
    key: Vec<Value>,
    ordinal: u64,
    binding: Binding,
}

impl GroupRunRow {
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }

    fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding).saturating_add(
            self.key
                .iter()
                .fold(std::mem::size_of::<Vec<Value>>(), |total, value| {
                    total.saturating_add(value_memory_bytes(value))
                }),
        )
    }
}

struct GroupMergeEntry {
    row: GroupRunRow,
    run_index: usize,
}

#[derive(Clone, Copy)]
struct AggregateExecutionContext<'a> {
    group_keys: &'a [Projection],
    items: &'a [Aggregation],
    catalog: &'a Catalog,
    batch_rows: usize,
    memory_budget: NonZeroUsize,
    execution_limit: ExecutionLimit,
    task_context: Option<&'a RuntimeTaskContext>,
}

impl PartialEq for GroupMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.cmp_key(&other.row) == Ordering::Equal && self.run_index == other.run_index
    }
}

impl Eq for GroupMergeEntry {}

impl Ord for GroupMergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for GroupMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn stream_aggregate_batches(
    input: &PhysicalPlan,
    group_keys: &[Projection],
    items: &[Aggregation],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let BatchReadContext {
        catalog, memory, ..
    } = context;
    if group_keys.is_empty() {
        let mut accumulator = GroupAccumulator::new(Vec::new(), group_keys, items);
        let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
        let base_bytes = accumulator.base_memory_bytes();
        ensure_operator_item_fits("AggregateExec", base_bytes, &tracker)?;
        tracker.charge(base_bytes);
        let mut input_rows = 0usize;
        execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
            runtime_checkpoint(context.task_context)?;
            for binding in &batch {
                update_group_accumulator(&mut accumulator, catalog, binding, &mut tracker)?;
                input_rows = input_rows.saturating_add(1);
            }
            Ok(BatchControl::Continue)
        })?;
        record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
            operator: "AggregateExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows,
            max_spill_bytes: memory.max_spill_bytes.get(),
            max_spill_runs: memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        return emit(vec![accumulator.finish()]);
    }

    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let mut spill_budget = SpillBudgetTracker::new("AggregateExec", memory);
    let mut rows = Vec::<GroupRunRow>::new();
    let mut runs = Vec::<spill::SpillRun>::new();
    let mut ordinal = 0u64;
    execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(context.task_context)?;
        for binding in batch {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect::<Vec<_>>();
            let bytes = binding_memory_bytes(&binding).saturating_add(
                key.iter().fold(0usize, |total, value| {
                    total.saturating_add(value_memory_bytes(value))
                }),
            );
            ensure_operator_item_fits("AggregateExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                runs.push(spill_group_run(
                    &mut rows,
                    &memory.spill_directory,
                    &mut spill_budget,
                    context.task_context,
                )?);
                tracker.reset();
            }
            tracker.charge(bytes);
            rows.push(GroupRunRow {
                key,
                ordinal,
                binding,
            });
            ordinal = ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;

    if runs.is_empty() {
        record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
            operator: "AggregateExec".to_string(),
            budget_bytes: tracker.budget_bytes,
            peak_tracked_bytes: tracker.peak_bytes,
            input_rows: ordinal as usize,
            max_spill_bytes: memory.max_spill_bytes.get(),
            max_spill_runs: memory.max_spill_runs.get(),
            spilled_bytes: 0,
            spill_run_count: 0,
            spilled_rows: 0,
        });
        rows.sort_by(GroupRunRow::cmp_key);
        let aggregate_context = AggregateExecutionContext {
            group_keys,
            items,
            catalog,
            batch_rows: memory.batch_rows.get(),
            memory_budget: memory.blocking_operator_bytes,
            execution_limit,
            task_context: context.task_context,
        };
        return aggregate_sorted_group_rows(rows, aggregate_context, emit);
    }
    if !rows.is_empty() {
        runs.push(spill_group_run(
            &mut rows,
            &memory.spill_directory,
            &mut spill_budget,
            context.task_context,
        )?);
    }
    runs = compact_group_runs(
        runs,
        group_keys,
        catalog,
        memory,
        &mut spill_budget,
        context.task_context,
    )?;
    record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
        operator: "AggregateExec".to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes: tracker.peak_bytes,
        input_rows: ordinal as usize,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows: ordinal as usize,
    });
    let aggregate_context = AggregateExecutionContext {
        group_keys,
        items,
        catalog,
        batch_rows: memory.batch_rows.get(),
        memory_budget: memory.blocking_operator_bytes,
        execution_limit,
        task_context: context.task_context,
    };
    merge_group_runs(&runs, aggregate_context, emit)
}

fn spill_group_run(
    rows: &mut Vec<GroupRunRow>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    rows.sort_by(GroupRunRow::cmp_key);
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, "aggregate")?;
    for row in rows.drain(..) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(row.ordinal, &row.binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

fn compact_group_runs(
    mut runs: Vec<spill::SpillRun>,
    group_keys: &[Projection],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<spill::SpillRun>> {
    while runs.len() > 2 {
        runtime_checkpoint(task_context)?;
        let mut compacted = Vec::with_capacity(runs.len().div_ceil(2));
        let mut pending = runs.into_iter();
        while let Some(left) = pending.next() {
            let Some(right) = pending.next() else {
                compacted.push(left);
                break;
            };
            compacted.push(merge_group_run_pair(
                &left,
                &right,
                group_keys,
                catalog,
                memory,
                spill_budget,
                task_context,
            )?);
        }
        runs = compacted;
    }
    Ok(runs)
}

#[allow(clippy::too_many_arguments)]
fn merge_group_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    group_keys: &[Projection],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut readers = [left.reader()?, right.reader()?];
    let mut heap = BinaryHeap::new();
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    let per_row_budget = memory.blocking_operator_bytes.get() / 2;
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some((ordinal, binding)) = reader.read(memory.blocking_operator_bytes.get())? {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let entry = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            if bytes > per_row_budget {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec spill merge row uses {bytes} bytes, exceeding half of blocking_operator_bytes {}",
                    memory.blocking_operator_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(entry);
        }
    }
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(&memory.spill_directory, "aggregate-merge")?;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        tracker.release(entry.row.memory_bytes());
        let run_index = entry.run_index;
        let bytes = writer.write(
            entry.row.ordinal,
            &entry.row.binding,
            spill_budget.remaining_bytes(),
        )?;
        spill_budget.charge(bytes)?;
        if let Some((ordinal, binding)) =
            readers[run_index].read(memory.blocking_operator_bytes.get())?
        {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let next = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = next.row.memory_bytes();
            if bytes > per_row_budget || tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec spill merge exceeds blocking_operator_bytes {}",
                    memory.blocking_operator_bytes
                )));
            }
            tracker.charge(bytes);
            heap.push(next);
        }
    }
    writer.finish()?;
    Ok(run)
}

fn aggregate_sorted_group_rows(
    rows: Vec<GroupRunRow>,
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let AggregateExecutionContext {
        group_keys,
        items,
        catalog,
        batch_rows,
        memory_budget,
        execution_limit,
        task_context,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    let mut batch = Vec::with_capacity(batch_rows);
    let mut accumulator: Option<GroupAccumulator<'_>> = None;
    let mut emitted = 0usize;
    for row in rows {
        runtime_checkpoint(task_context)?;
        if accumulator
            .as_ref()
            .is_some_and(|accumulator| accumulator.key != row.key)
        {
            batch.push(accumulator.take().expect("group exists").finish());
            tracker.reset();
            emitted = emitted.saturating_add(1);
            if flush_aggregate_batch(&mut batch, batch_rows, emitted, execution_limit, emit)?
                == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
        }
        if accumulator.is_none() {
            let next = GroupAccumulator::new(row.key.clone(), group_keys, items);
            let base_bytes = next.base_memory_bytes();
            ensure_operator_item_fits("AggregateExec group state", base_bytes, &tracker)?;
            tracker.charge(base_bytes);
            accumulator = Some(next);
        }
        update_group_accumulator(
            accumulator.as_mut().expect("group exists"),
            catalog,
            &row.binding,
            &mut tracker,
        )?;
    }
    runtime_checkpoint(task_context)?;
    if let Some(accumulator) = accumulator {
        batch.push(accumulator.finish());
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn merge_group_runs(
    runs: &[spill::SpillRun],
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let AggregateExecutionContext {
        group_keys,
        items,
        catalog,
        batch_rows,
        memory_budget,
        execution_limit,
        task_context,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut accumulator_tracker = OperatorMemoryTracker::new(memory_budget);
    let mut readers = runs
        .iter()
        .map(spill::SpillRun::reader)
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    let mut merge_tracker = OperatorMemoryTracker::new(memory_budget);
    for (run_index, reader) in readers.iter_mut().enumerate() {
        runtime_checkpoint(task_context)?;
        if let Some((ordinal, binding)) = reader.read(memory_budget.get())? {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let entry = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = entry.row.memory_bytes();
            ensure_operator_item_fits("AggregateExec merge", bytes, &merge_tracker)?;
            if merge_tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec merge fan-in uses more than blocking_operator_bytes {}",
                    merge_tracker.budget_bytes
                )));
            }
            merge_tracker.charge(bytes);
            heap.push(entry);
        }
    }
    let mut batch = Vec::with_capacity(batch_rows);
    let mut accumulator: Option<GroupAccumulator<'_>> = None;
    let mut emitted = 0usize;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        merge_tracker.release(entry.row.memory_bytes());
        let run_index = entry.run_index;
        let row = entry.row;
        if accumulator
            .as_ref()
            .is_some_and(|accumulator| accumulator.key != row.key)
        {
            batch.push(accumulator.take().expect("group exists").finish());
            accumulator_tracker.reset();
            emitted = emitted.saturating_add(1);
            if flush_aggregate_batch(&mut batch, batch_rows, emitted, execution_limit, emit)?
                == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
        }
        if accumulator.is_none() {
            let next = GroupAccumulator::new(row.key.clone(), group_keys, items);
            let base_bytes = next.base_memory_bytes();
            ensure_operator_item_fits(
                "AggregateExec group state",
                base_bytes,
                &accumulator_tracker,
            )?;
            accumulator_tracker.charge(base_bytes);
            accumulator = Some(next);
        }
        update_group_accumulator(
            accumulator.as_mut().expect("group exists"),
            catalog,
            &row.binding,
            &mut accumulator_tracker,
        )?;
        if let Some((ordinal, binding)) = readers[run_index].read(memory_budget.get())? {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect();
            let next = GroupMergeEntry {
                row: GroupRunRow {
                    key,
                    ordinal,
                    binding,
                },
                run_index,
            };
            let bytes = next.row.memory_bytes();
            ensure_operator_item_fits("AggregateExec merge", bytes, &merge_tracker)?;
            if merge_tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AggregateExec merge fan-in uses more than blocking_operator_bytes {}",
                    merge_tracker.budget_bytes
                )));
            }
            merge_tracker.charge(bytes);
            heap.push(next);
        }
    }
    runtime_checkpoint(task_context)?;
    if let Some(accumulator) = accumulator {
        batch.push(accumulator.finish());
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn update_group_accumulator(
    accumulator: &mut GroupAccumulator<'_>,
    catalog: &Catalog,
    binding: &Binding,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    let delta = accumulator.update(catalog, binding);
    tracker.release(delta.released_bytes);
    if tracker.would_exceed(delta.added_bytes) {
        return Err(SkeinError::Execution(format!(
            "AggregateExec state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.charge(delta.added_bytes);
    Ok(())
}

fn flush_aggregate_batch(
    batch: &mut BindingBatch,
    batch_rows: usize,
    emitted: usize,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if (batch.len() == batch_rows || execution_limit.is_reached(emitted))
        && (emit(std::mem::replace(batch, Vec::with_capacity(batch_rows)))? == BatchControl::Stop
            || execution_limit.is_reached(emitted))
    {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn stream_node_scan_batches(
    variable: &str,
    label: &str,
    filter: Option<(&Predicate, &PropertyFilter)>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    let batch_rows = memory.batch_rows.get();
    let exact_label = exact_scan_label_id(catalog, label);
    let exact_label_id = exact_label.flatten();
    if !store.is_out_of_core()
        && let Some(label_id) = exact_label
        && store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= memory.blocking_operator_bytes.get()
    {
        let scan = store.scan_nodes_with_filter_pruning(label_id, filter.map(|(_, filter)| filter));
        record_scan_pruning_report(scan.report.clone());
        let mut batch = Vec::with_capacity(batch_rows);
        let mut emitted = 0usize;
        for node in scan.nodes {
            runtime_checkpoint(context.task_context)?;
            let binding = node_binding(variable, node.clone());
            if let Some((predicate, _)) = filter
                && !evaluate_predicate(predicate, catalog, store, &binding)?
            {
                continue;
            }
            batch.push(binding);
            emitted = emitted.saturating_add(1);
            if batch.len() == batch_rows
                && emit(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(batch_rows),
                ))? == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
            if execution_limit.is_reached(emitted) {
                break;
            }
        }
        if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        return Ok(if execution_limit.is_reached(emitted) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        });
    }
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut batch = Vec::with_capacity(batch_rows);
    let mut emitted = 0usize;
    let mut callback_error = None;
    let control = store.visit_nodes_owned(exact_label_id, |node| {
        if callback_error.is_some() {
            return GraphScanControl::Stop;
        }
        if let Err(error) = runtime_checkpoint(context.task_context) {
            callback_error = Some(error);
            return GraphScanControl::Stop;
        }
        if exact_label.is_none() && !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return GraphScanControl::Continue;
        }
        if filter
            .map(|(_, property_filter)| node_matches_property_filter(&node, property_filter))
            .is_some_and(|matches| !matches)
        {
            return GraphScanControl::Continue;
        }
        let binding = node_binding(variable, node);
        if let Some((predicate, _)) = filter {
            match evaluate_predicate(predicate, catalog, store, &binding) {
                Ok(true) => {}
                Ok(false) => return GraphScanControl::Continue,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        batch.push(binding);
        emitted = emitted.saturating_add(1);
        if batch.len() == batch_rows {
            match emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            )) {
                Ok(BatchControl::Continue) => {}
                Ok(BatchControl::Stop) => return GraphScanControl::Stop,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if execution_limit.is_reached(emitted) {
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    let candidate_count = store.node_count_for_label(exact_label_id);
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: exact_label_id,
        rel_type_id: None,
        strategy: ScanPruningStrategy::FullLabelScan,
        pruned: false,
        exact_empty: candidate_count == 0,
        candidate_count_before_pruning: candidate_count,
        pruned_candidate_count: 0,
        candidate_count_before_filter: candidate_count,
        output_count: emitted,
        filtered_out_count: candidate_count.saturating_sub(emitted),
    });
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == GraphScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

fn stream_index_node_seek_batches(
    variable: &str,
    label: &str,
    property: &str,
    values: &[Value],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    let batch_rows = memory.batch_rows.get();
    let Some(label_id) = catalog.label_id(label) else {
        return Ok(BatchControl::Continue);
    };
    let matched = std::cell::Cell::new(0usize);
    let control =
        stream_visited_node_batches(variable, batch_rows, execution_limit, emit, |consumer| {
            store.visit_nodes_by_property_owned(label_id, property, values, |node| {
                matched.set(matched.get().saturating_add(1));
                consumer(node)
            })
        })?;
    let matched = matched.get();
    let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: Some(label_id),
        rel_type_id: None,
        strategy: if values.len() == 1 {
            ScanPruningStrategy::PropertyEq {
                property: property.to_string(),
            }
        } else {
            ScanPruningStrategy::PropertyIn {
                property: property.to_string(),
            }
        },
        pruned: true,
        exact_empty: matched == 0,
        candidate_count_before_pruning,
        pruned_candidate_count: candidate_count_before_pruning.saturating_sub(matched),
        candidate_count_before_filter: matched,
        output_count: matched.min(execution_limit.output_rows.unwrap_or(usize::MAX)),
        filtered_out_count: 0,
    });
    Ok(control)
}

fn stream_visited_node_batches(
    variable: &str,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    visit: impl FnOnce(&mut dyn FnMut(NodeRecord) -> GraphScanControl) -> Result<GraphScanControl>,
) -> Result<BatchControl> {
    let mut batch = Vec::with_capacity(batch_rows);
    let mut emitted = 0usize;
    let mut callback_error = None;
    let mut consumer = |node| {
        batch.push(node_binding(variable, node));
        emitted = emitted.saturating_add(1);
        if batch.len() == batch_rows {
            match emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            )) {
                Ok(BatchControl::Continue) => {}
                Ok(BatchControl::Stop) => return GraphScanControl::Stop,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if execution_limit.is_reached(emitted) {
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    };
    let control = visit(&mut consumer)?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == GraphScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

fn node_binding(variable: &str, node: NodeRecord) -> Binding {
    Binding {
        values: BTreeMap::new(),
        nodes: BTreeMap::from([(variable.to_string(), node)]),
        relationships: BTreeMap::new(),
    }
}

fn emit_owned_binding_batches(
    bindings: Vec<Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    emit_binding_iterator(bindings, batch_rows, emit)
}

fn emit_binding_iterator(
    bindings: impl IntoIterator<Item = Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut batch = Vec::with_capacity(batch_rows);
    for binding in bindings {
        batch.push(binding);
        if batch.len() == batch_rows
            && emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            ))? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn execute_bindings_with_limit(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    runtime_checkpoint(context.task_context)?;
    if batch_pipeline_capable(plan) {
        return collect_batch_pipeline(
            plan,
            catalog,
            store,
            context.memory,
            context.task_context,
            execution_limit,
        );
    }
    match plan {
        PhysicalPlan::CreateNodeLabel { label } => {
            let existed = catalog.label_id(label);
            let id = store.create_node_label(catalog, label)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("label_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipType { rel_type } => {
            let existed = catalog.rel_type_id(rel_type);
            let id = store.create_relationship_type(catalog, rel_type)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateNodeTable { name } => {
            let existed = catalog.table_id(crate::schema::TableKind::Node, name);
            let id = store.create_node_table(catalog, name)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("table_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipTable { name } => {
            let existed = catalog.table_id(crate::schema::TableKind::Relationship, name);
            let id = store.create_relationship_table(catalog, name)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("table_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(existed.is_none())),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => {
            let table_kind = table_kind_to_core(*table_kind);
            let existed = catalog
                .table_id(table_kind, table)
                .and_then(|table_id| catalog.property_descriptor_id(table_id, property))
                .is_some();
            let id = store.create_property_descriptor(
                catalog,
                table_kind,
                table,
                property,
                property_type_to_core(*value_type),
                *nullable,
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("property_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => {
            let state = object_state_to_core(*state);
            let (id, changed) =
                store.alter_table_state(catalog, table_kind_to_core(*table_kind), table, state)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("table_id".to_string(), Value::Int(id.0 as i64)),
                    ("changed".to_string(), Value::Bool(changed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => {
            let state = object_state_to_core(*state);
            let (id, changed) = store.alter_property_state(
                catalog,
                table_kind_to_core(*table_kind),
                table,
                property,
                state,
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("property_id".to_string(), Value::Int(id.0 as i64)),
                    ("changed".to_string(), Value::Bool(changed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateIndex { label, property } => {
            let existed = catalog
                .label_id(label)
                .is_some_and(|label_id| catalog.property_index_id(label_id, property).is_some());
            let id = store.create_property_index(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateCompositeIndex { label, properties } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .composite_property_index_id(label_id, properties)
                    .is_some()
            });
            let id = store.create_composite_property_index(catalog, label, properties)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRangeIndex { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .property_index_id_with_kind(
                        label_id,
                        property,
                        crate::schema::IndexKind::Range,
                    )
                    .is_some()
            });
            let id = store.create_range_property_index(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateFullTextIndex { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .property_index_id_with_kind(
                        label_id,
                        property,
                        crate::schema::IndexKind::FullText,
                    )
                    .is_some()
            });
            let id = store.create_full_text_property_index(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("index_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateUniqueConstraint { label, property } => {
            let existed = catalog
                .label_id(label)
                .is_some_and(|label_id| catalog.unique_constraint_id(label_id, property).is_some());
            let id = store.create_unique_constraint(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .node_property_exists_constraint_id(label_id, property)
                    .is_some()
            });
            let id = store.create_node_property_exists_constraint(catalog, label, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            let existed = catalog.rel_type_id(rel_type).is_some_and(|rel_type_id| {
                catalog
                    .relationship_unique_constraint_id(rel_type_id, property)
                    .is_some()
            });
            let id = store.create_relationship_unique_constraint(catalog, rel_type, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            let existed = catalog.rel_type_id(rel_type).is_some_and(|rel_type_id| {
                catalog
                    .relationship_property_exists_constraint_id(rel_type_id, property)
                    .is_some()
            });
            let id = store
                .create_relationship_property_exists_constraint(catalog, rel_type, property)?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(!existed)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => {
            let graph = try_projected_graph_with_node_filter(
                catalog,
                store,
                node_labels,
                rel_types,
                |_| true,
                ProjectionLayout::Outgoing,
                ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes),
            )?;
            store.register_projected_graph(
                name,
                ProjectedGraphDefinition {
                    node_labels: node_labels.clone(),
                    rel_types: rel_types.clone(),
                },
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("graph_name".to_string(), Value::String(name.clone())),
                    (
                        "node_count".to_string(),
                        Value::Int(graph.node_count() as i64),
                    ),
                    (
                        "edge_count".to_string(),
                        Value::Int(graph.edge_count() as i64),
                    ),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate,
        } => {
            let Some(definition) = store.projected_graph_definition(graph_name) else {
                return Err(SkeinError::Execution(format!(
                    "projected graph '{graph_name}' does not exist"
                )));
            };
            let node_visibility_filter = node_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let layout = match algorithm {
                GraphAlgorithmKind::PageRank => ProjectionLayout::Outgoing,
                GraphAlgorithmKind::Louvain => ProjectionLayout::Undirected,
            };
            let budget = ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes);
            let graph = if let Some(filter) = node_visibility_filter.as_ref() {
                try_projected_graph_with_node_filter(
                    catalog,
                    store,
                    &definition.node_labels,
                    &definition.rel_types,
                    |node| node_matches_property_filter(node, filter),
                    layout,
                    budget,
                )
            } else {
                try_projected_graph_with_node_filter(
                    catalog,
                    store,
                    &definition.node_labels,
                    &definition.rel_types,
                    |_| true,
                    layout,
                    budget,
                )
            }?;
            match algorithm {
                GraphAlgorithmKind::PageRank => collect_bounded_operator_bindings(
                    "GraphAlgorithm",
                    graph
                        .page_rank(PageRankOptions {
                            iterations: options
                                .max_iterations
                                .unwrap_or_else(|| PageRankOptions::default().iterations),
                            damping: options
                                .damping
                                .unwrap_or_else(|| PageRankOptions::default().damping),
                        })
                        .into_iter()
                        .map(|score| Binding {
                            values: BTreeMap::from([
                                ("node".to_string(), Value::Int(score.node.0 as i64)),
                                (score_column.clone(), Value::Float(score.score)),
                            ]),
                            nodes: BTreeMap::new(),
                            relationships: BTreeMap::new(),
                        }),
                    context.memory.blocking_operator_bytes,
                ),
                GraphAlgorithmKind::Louvain => collect_bounded_operator_bindings(
                    "GraphAlgorithm",
                    graph
                        .hierarchical_louvain_communities(LouvainOptions {
                            max_iterations: options
                                .max_iterations
                                .unwrap_or_else(|| LouvainOptions::default().max_iterations),
                            max_levels: options
                                .max_levels
                                .unwrap_or_else(|| LouvainOptions::default().max_levels),
                        })
                        .into_iter()
                        .map(|assignment| Binding {
                            values: BTreeMap::from([
                                ("node".to_string(), Value::Int(assignment.node.0 as i64)),
                                ("level".to_string(), Value::Int(assignment.level as i64)),
                                (
                                    "louvain_id".to_string(),
                                    Value::Int(assignment.community.0 as i64),
                                ),
                            ]),
                            nodes: BTreeMap::new(),
                            relationships: BTreeMap::new(),
                        }),
                    context.memory.blocking_operator_bytes,
                ),
            }
        }
        PhysicalPlan::VectorSeedScan {
            embedding_parameter,
            output_external_id,
            metadata_filters,
            vector_plan,
        } => {
            let embedding =
                vector_embedding_parameter(context.parameters, embedding_parameter, vector_plan)?;
            let output = context
                .external
                .execute_vector_seed(VectorSeedExecutionRequest {
                    embedding: &embedding,
                    metadata_filters,
                    vector_plan,
                })?;
            record_vector_execution_report(output.report);
            collect_bounded_operator_bindings(
                "VectorSeedScan",
                output.rows.into_iter().map(|row| {
                    let mut values = BTreeMap::from([
                        ("id".to_string(), Value::String(row.id)),
                        ("score".to_string(), Value::Float(row.score)),
                    ]);
                    if *output_external_id && let Some(external_id) = row.external_id {
                        values.insert("external_id".to_string(), Value::String(external_id));
                    }
                    Binding {
                        values,
                        nodes: BTreeMap::new(),
                        relationships: BTreeMap::new(),
                    }
                }),
                context.memory.blocking_operator_bytes,
            )
        }
        PhysicalPlan::CreateNode { label, properties } => {
            let id = store.create_node(catalog, label, properties.clone())?;
            Ok(vec![Binding {
                values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => {
            let on_match_assignments = on_match_assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let post_merge_assignments = post_merge_assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let (id, created) = store.merge_node(
                catalog,
                label,
                match_properties.clone(),
                on_create_properties.clone(),
                &on_match_assignments,
                &post_merge_assignments,
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("node_id".to_string(), Value::Int(id.0 as i64)),
                    ("created".to_string(), Value::Bool(created)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => {
            let (source, rel, target, created) = store.merge_connected_nodes(
                catalog,
                ConnectedNodesCreate {
                    source_label: source_label.clone(),
                    source_properties: source_properties.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_properties: target_properties.clone(),
                },
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                    ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                    ("created".to_string(), Value::Bool(created)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_between_matches(
                catalog,
                MatchedRelationshipMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    rel_type: rel_type.clone(),
                    rel_match_properties: rel_match_properties.clone(),
                    on_create_properties: on_create_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            target_label,
            target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_from_matched_relationships(
                catalog,
                MatchedRelationshipCopyMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties
                        .iter()
                        .map(|(property, value)| {
                            (
                                property.clone(),
                                relationship_on_create_property_value(value),
                            )
                        })
                        .collect(),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_target_label,
            new_target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_to_matched_target(
                catalog,
                MatchedRelationshipRetargetMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    old_target_label: old_target_label.clone(),
                    old_target_filter: property_filter_from_properties(old_target_properties),
                    new_target_label: new_target_label.clone(),
                    new_target_filter: property_filter_from_properties(new_target_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties.clone(),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label,
            old_source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_source_label,
            new_source_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_from_matched_target(
                catalog,
                MatchedRelationshipSourceRetargetMerge {
                    old_source_label: old_source_label.clone(),
                    old_source_filter: property_filter_from_properties(old_source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    old_target_label: old_target_label.clone(),
                    old_target_filter: property_filter_from_properties(old_target_properties),
                    new_source_label: new_source_label.clone(),
                    new_source_filter: property_filter_from_properties(new_source_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties.clone(),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetNodeProperty {
            label,
            predicate,
            property,
            value,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = match value {
                SetValue::Value(value) => store.set_node_property(
                    catalog,
                    label,
                    filter.as_ref(),
                    property,
                    value.clone(),
                )?,
                SetValue::Coalesce { default, .. } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::Coalesce {
                            default: default.clone(),
                        },
                    }],
                )?,
                SetValue::AddInt { amount, .. } => store.add_int_node_property(
                    catalog,
                    label,
                    filter.as_ref(),
                    property,
                    *amount,
                )?,
                SetValue::DecrementFloorZero { .. } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::DecrementFloorZero,
                    }],
                )?,
                SetValue::PreserveNewerExisting {
                    incoming, preserve, ..
                } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::PreserveNewerExisting {
                            incoming: incoming.clone(),
                            preserve: *preserve,
                        },
                    }],
                )?,
            };
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetNodeProperties {
            label,
            predicate,
            assignments,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let assignments = assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let ids = store.set_node_properties(catalog, label, filter.as_ref(), &assignments)?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetNodePropertiesReturn {
            variable,
            label,
            predicate,
            assignments,
            returns,
        } => {
            let assignments = assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let label_ids = label_ids_for_pattern(catalog, label);
            let mut ids = Vec::new();
            let mut callback_error = None;
            store.visit_nodes_owned(None, |node| {
                if callback_error.is_some() {
                    return GraphScanControl::Stop;
                }
                if !node_matches_label_pattern(&node, label_ids.as_deref()) {
                    return GraphScanControl::Continue;
                }
                let id = node.id;
                let binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                };
                if let Some(predicate) = predicate {
                    match evaluate_predicate(predicate, catalog, store, &binding) {
                        Ok(true) => {}
                        Ok(false) => return GraphScanControl::Continue,
                        Err(error) => {
                            callback_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                ids.push(id);
                GraphScanControl::Continue
            })?;
            if let Some(error) = callback_error {
                return Err(error);
            }
            let ids = store.set_node_properties_by_ids(catalog, &ids, &assignments)?;
            match returns {
                SetNodePropertiesReturnMode::Project(returns) => ids
                    .into_iter()
                    .map(|id| {
                        let node = store.node_owned(id)?.ok_or_else(|| {
                            SkeinError::Execution(format!(
                                "updated node {} is missing during SET RETURN projection",
                                id.0
                            ))
                        })?;
                        let binding = Binding {
                            values: BTreeMap::new(),
                            nodes: BTreeMap::from([(variable.clone(), node)]),
                            relationships: BTreeMap::new(),
                        };
                        let values = returns
                            .iter()
                            .map(|item| {
                                project_value(item, catalog, &binding)
                                    .map(|value| (item.name.clone(), value))
                            })
                            .collect::<Result<BTreeMap<_, _>>>()?;
                        Ok(Binding {
                            values,
                            nodes: BTreeMap::new(),
                            relationships: BTreeMap::new(),
                        })
                    })
                    .collect(),
                SetNodePropertiesReturnMode::Count { name } => Ok(vec![Binding {
                    values: BTreeMap::from([(name.clone(), Value::Int(ids.len() as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                }]),
            }
        }
        PhysicalPlan::SetRelationshipProperty {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            property,
            value,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = store.set_relationship_property(
                catalog,
                RelationshipPropertyUpdate {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter: relationship_filter_from_properties_and_predicate(
                        rel_properties,
                        rel_predicate.as_ref(),
                    )?,
                    property: property.clone(),
                    value: value.clone(),
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("rel_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SetRelationshipProperties {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            assignments,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let assignments = assignments
                .iter()
                .map(|assignment| RelationshipSetAssignment {
                    property: assignment.property.clone(),
                    value: assignment.value.clone(),
                })
                .collect::<Vec<_>>();
            let ids = store.set_relationship_properties(
                catalog,
                RelationshipPropertiesUpdate {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter: relationship_filter_from_properties_and_predicate(
                        rel_properties,
                        rel_predicate.as_ref(),
                    )?,
                    assignments,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("rel_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => {
            let label_id = if label.is_empty() {
                None
            } else {
                let Some(label_id) = catalog.label_id(label) else {
                    return Ok(Vec::new());
                };
                Some(label_id)
            };
            let candidate_filter = predicate
                .as_ref()
                .and_then(|predicate| node_scan_filter_from_predicate(predicate, variable));
            let mut ids = Vec::new();
            let mut callback_error = None;
            store.visit_nodes_owned(label_id, |node| {
                if callback_error.is_some() {
                    return GraphScanControl::Stop;
                }
                if candidate_filter
                    .as_ref()
                    .is_some_and(|filter| !node_matches_property_filter(&node, filter))
                {
                    return GraphScanControl::Continue;
                }
                let id = node.id;
                let binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                };
                if let Some(predicate) = predicate {
                    match evaluate_predicate(predicate, catalog, store, &binding) {
                        Ok(true) => {}
                        Ok(false) => return GraphScanControl::Continue,
                        Err(error) => {
                            callback_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                ids.push(id);
                GraphScanControl::Continue
            })?;
            if let Some(error) = callback_error {
                return Err(error);
            }
            let ids = store.delete_node_ids(catalog, &ids, *detach)?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::DeleteRelationship {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let rel_filter = relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?;
            let ids = store.delete_relationships(
                catalog,
                RelationshipDeleteRequest {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("rel_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::DeleteRelationshipTargetNodes {
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
            detach,
            ..
        } => {
            let source_filter = source_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = store.delete_relationship_target_nodes(
                catalog,
                RelationshipTargetNodeDelete {
                    source_label: source_label.clone(),
                    source_filter,
                    rel_type: rel_type.clone(),
                    rel_filter: property_filter_from_properties(rel_properties),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    detach: *detach,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding {
                    values: BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => {
            let (source, rel, target) = store.create_connected_nodes(
                catalog,
                ConnectedNodesCreate {
                    source_label: source_label.clone(),
                    source_properties: source_properties.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_properties: target_properties.clone(),
                },
            )?;
            Ok(vec![Binding {
                values: BTreeMap::from([
                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                    ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => {
            let rows = store.create_relationships_between_matches(
                catalog,
                MatchedRelationshipCreate {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target)| Binding {
                    values: BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                    ]),
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::SeqNodeScan { variable, label } => execute_node_scan_with_optional_filter(
            variable,
            label,
            None,
            catalog,
            store,
            execution_limit,
            context.memory.blocking_operator_bytes,
        ),
        PhysicalPlan::SourceSegmentScan {
            variable,
            predicate,
        } => execute_source_segment_scan(
            variable,
            predicate,
            catalog,
            store,
            execution_limit,
            context.memory,
            context.task_context,
        ),
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left = execute_child_bindings(left, catalog, store, context)?;
            let right = execute_child_bindings(right, catalog, store, context)?;
            let mut output = Vec::new();
            let mut tracker = OperatorMemoryTracker::new(context.memory.blocking_operator_bytes);
            for binding in left.iter().chain(&right) {
                let bytes = binding_memory_bytes(binding);
                ensure_operator_item_fits("NodeCartesianProductExec", bytes, &tracker)?;
                if tracker.would_exceed(bytes) {
                    return Err(SkeinError::Execution(format!(
                        "NodeCartesianProductExec inputs exceed blocking_operator_bytes {}",
                        tracker.budget_bytes
                    )));
                }
                tracker.charge(bytes);
            }
            for left_binding in &left {
                for right_binding in &right {
                    let mut values = left_binding.values.clone();
                    values.extend(right_binding.values.clone());
                    let mut nodes = left_binding.nodes.clone();
                    nodes.extend(right_binding.nodes.clone());
                    let mut relationships = left_binding.relationships.clone();
                    relationships.extend(right_binding.relationships.clone());
                    let binding = Binding {
                        values,
                        nodes,
                        relationships,
                    };
                    push_bounded_operator_binding(
                        "NodeCartesianProductExec",
                        &mut output,
                        binding,
                        &mut tracker,
                    )?;
                    if execution_limit.is_reached(output.len()) {
                        return Ok(output);
                    }
                }
            }
            Ok(output)
        }
        PhysicalPlan::NodeColumnLookupExec {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            execute_node_column_lookup(
                NodeColumnLookupSpec {
                    variable,
                    label,
                    property,
                    column,
                    optional: *optional,
                },
                input,
                catalog,
                store,
                execution_limit,
                context.memory.blocking_operator_bytes,
            )
        }
        PhysicalPlan::IndexNodeSeek {
            variable,
            label,
            property,
            value,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_property_owned(
                label_id,
                property,
                std::slice::from_ref(value),
                |node| {
                    output.push(single_node_binding(variable, node));
                    if execution_limit.is_reached(output.len()) {
                        GraphScanControl::Stop
                    } else {
                        GraphScanControl::Continue
                    }
                },
            )?;
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyEq {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: output.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(output.len()),
                candidate_count_before_filter: output.len(),
                output_count: output.len(),
                filtered_out_count: 0,
            });
            Ok(output)
        }
        PhysicalPlan::IndexNodeMultiSeek {
            variable,
            label,
            property,
            values,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut seen = std::collections::BTreeSet::new();
            let mut output = Vec::new();
            store.visit_nodes_by_property_owned(label_id, property, values, |node| {
                if seen.insert(node.id) {
                    output.push(single_node_binding(variable, node));
                }
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyIn {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: output.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(output.len()),
                candidate_count_before_filter: output.len(),
                output_count: output.len(),
                filtered_out_count: 0,
            });
            Ok(output)
        }
        PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_composite_property_owned(label_id, predicates, |node| {
                output.push(single_node_binding(variable, node));
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            Ok(output)
        }
        PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            lower,
            upper,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_property_range_owned(
                label_id,
                property,
                lower.as_ref(),
                upper.as_ref(),
                |node| {
                    output.push(single_node_binding(variable, node));
                    if execution_limit.is_reached(output.len()) {
                        GraphScanControl::Stop
                    } else {
                        GraphScanControl::Continue
                    }
                },
            )?;
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyRange {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: output.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(output.len()),
                candidate_count_before_filter: output.len(),
                output_count: output.len(),
                filtered_out_count: 0,
            });
            Ok(output)
        }
        PhysicalPlan::IndexNodeTextSeek {
            variable,
            label,
            property,
            query,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            let mut output = Vec::new();
            store.visit_nodes_by_full_text_property_owned(label_id, property, query, |node| {
                output.push(single_node_binding(variable, node));
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            Ok(output)
        }
        PhysicalPlan::AdjacencyExpandExec { input, .. } => execute_adjacency_expand(
            plan,
            input,
            catalog,
            store,
            context,
            execution_limit,
            AdjacencyExpandFilters::default(),
        ),
        PhysicalPlan::OptionalDegreeExec {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            let rel_type_id = if rel_type.is_empty() {
                None
            } else {
                catalog.rel_type_id(rel_type)
            };
            if !rel_type.is_empty() && rel_type_id.is_none() {
                return Ok(input
                    .into_iter()
                    .map(|mut binding| {
                        binding.values.insert(alias.clone(), Value::Int(0));
                        binding
                    })
                    .collect());
            }
            let target_label_ids = label_ids_for_pattern(catalog, target_label);
            input
                .into_iter()
                .map(|mut binding| {
                    let source = binding.nodes.get(source_variable).ok_or_else(|| {
                        SkeinError::Execution(format!(
                            "missing variable '{source_variable}' during optional degree"
                        ))
                    })?;
                    let degree = one_hop_relationships_with_budget(
                        store,
                        source.id,
                        rel_type_id,
                        target_label_ids.as_deref(),
                        rel_properties,
                        None,
                        *direction,
                        context.memory.blocking_operator_bytes.get(),
                    )?
                    .into_iter()
                    .filter(|(_, target)| node_properties_match(target, target_properties))
                    .count();
                    binding
                        .values
                        .insert(alias.clone(), Value::Int(degree as i64));
                    Ok(binding)
                })
                .collect()
        }
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            output,
            ..
        } => {
            let label_ids = label_ids_for_pattern(catalog, label);
            let mut total = 0usize;
            let mut callback_error = None;
            store.visit_nodes_owned(None, |node| {
                if !node_matches_label_pattern(&node, label_ids.as_deref())
                    || !node_properties_match(&node, properties)
                {
                    return GraphScanControl::Continue;
                }
                for leg in legs {
                    match relationship_count_sum_leg(catalog, store, node.id, leg) {
                        Ok(count) => total = total.saturating_add(count),
                        Err(error) => {
                            callback_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                GraphScanControl::Continue
            })?;
            if let Some(error) = callback_error {
                return Err(error);
            }
            Ok(vec![Binding {
                values: BTreeMap::from([(output.clone(), Value::Int(total as i64))]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }])
        }
        PhysicalPlan::ThreadRepairStatsExec {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => thread_repair_stats_rows(
            catalog,
            store,
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
            context.memory.blocking_operator_bytes,
        ),
        PhysicalPlan::ShortestPathExec {
            source_label,
            source_id,
            source_visibility_predicate,
            rel_type,
            direction,
            target_label,
            target_id,
            target_visibility_predicate,
            min_hops,
            max_hops,
            returns,
            ..
        } => {
            let source_visibility_filter = source_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let target_visibility_filter = target_visibility_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            execute_shortest_path(
                catalog,
                store,
                ShortestPathExecInput {
                    source_label,
                    source_id,
                    source_visibility_filter: source_visibility_filter.as_ref(),
                    path_node_visibility_filter: source_visibility_filter.as_ref(),
                    rel_type,
                    direction: *direction,
                    target_label,
                    target_id,
                    target_visibility_filter: target_visibility_filter.as_ref(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    returns,
                },
                context.memory,
                execution_limit,
                context.task_context,
            )
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            if let PhysicalPlan::SeqNodeScan { variable, label } = input.as_ref()
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                return execute_node_scan_with_optional_filter(
                    variable,
                    label,
                    Some((predicate, &filter)),
                    catalog,
                    store,
                    execution_limit,
                    context.memory.blocking_operator_bytes,
                );
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                rel_variable: Some(rel_variable),
                input: expand_input,
                ..
            } = input.as_ref()
                && let Some(filter) =
                    exact_relationship_scan_filter_from_predicate(predicate, rel_variable)
            {
                let input = execute_adjacency_expand(
                    input,
                    expand_input,
                    catalog,
                    store,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: Some(&filter),
                        target_scan_filter: None,
                    },
                )?;
                let mut output = Vec::new();
                for binding in input {
                    if evaluate_predicate(predicate, catalog, store, &binding)? {
                        output.push(binding);
                        if execution_limit.is_reached(output.len()) {
                            return Ok(output);
                        }
                    }
                }
                return Ok(output);
            }
            if let PhysicalPlan::AdjacencyExpandExec {
                target_variable,
                input: expand_input,
                ..
            } = input.as_ref()
                && predicate_references_only_variable(predicate, target_variable)
                && let Ok(filter) = property_filter_from_predicate(predicate)
            {
                let input = execute_adjacency_expand(
                    input,
                    expand_input,
                    catalog,
                    store,
                    context,
                    execution_limit,
                    AdjacencyExpandFilters {
                        relationship_scan_filter: None,
                        target_scan_filter: Some(&filter),
                    },
                )?;
                let mut output = Vec::new();
                for binding in input {
                    if evaluate_predicate(predicate, catalog, store, &binding)? {
                        output.push(binding);
                        if execution_limit.is_reached(output.len()) {
                            return Ok(output);
                        }
                    }
                }
                return Ok(output);
            }
            let input = execute_child_bindings(input, catalog, store, context)?;
            let mut output = Vec::new();
            for binding in input {
                if evaluate_predicate(predicate, catalog, store, &binding)? {
                    output.push(binding);
                    if execution_limit.is_reached(output.len()) {
                        return Ok(output);
                    }
                }
            }
            Ok(output)
        }
        PhysicalPlan::ProjectExec { items, input } => {
            let input =
                execute_bindings_with_limit(input, catalog, store, context, execution_limit)?;
            let mut output = Vec::new();
            for binding in input {
                let mut values = BTreeMap::new();
                for item in items {
                    let value = project_value(item, catalog, &binding)?;
                    insert_projected_value(&mut values, &item.name, value);
                }
                output.push(Binding {
                    values,
                    nodes: binding.nodes,
                    relationships: binding.relationships,
                });
                if execution_limit.is_reached(output.len()) {
                    return Ok(output);
                }
            }
            Ok(output)
        }
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            Ok(execute_aggregate(catalog, group_keys, items, &input))
        }
        PhysicalPlan::DistinctExec { input } => {
            let input = execute_child_bindings(input, catalog, store, context)?;
            Ok(distinct_bindings(input))
        }
        PhysicalPlan::SortExec { items, input } => {
            let mut input = execute_child_bindings(input, catalog, store, context)?;
            input.sort_by(|left, right| compare_bindings(catalog, left, right, items));
            Ok(input)
        }
        PhysicalPlan::TopNExec {
            items,
            offset,
            limit,
            input,
        } => execute_top_n_bindings(input, items, *offset, *limit, catalog, store, context),
        PhysicalPlan::LimitExec {
            offset,
            limit: query_limit,
            input,
        } => {
            let child_limit = execution_limit.child_for_limit(*offset, *query_limit);
            let input = execute_bindings_with_limit(input, catalog, store, context, child_limit)?;
            let rows = input
                .into_iter()
                .skip(*offset)
                .take(query_limit.unwrap_or(usize::MAX))
                .collect();
            Ok(rows)
        }
    }
}

fn execute_top_n_bindings(
    input: &PhysicalPlan,
    items: &[SortItem],
    offset: usize,
    limit: usize,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
) -> Result<Vec<Binding>> {
    let retained = offset.saturating_add(limit);
    if retained == 0 {
        return Ok(Vec::new());
    }
    let input = execute_child_bindings(input, catalog, store, context)?;
    let mut heap = BinaryHeap::with_capacity(retained.min(input.len()));
    for (ordinal, binding) in input.into_iter().enumerate() {
        let ordinal = u64::try_from(ordinal)
            .map_err(|_| SkeinError::Execution("TopNExec input ordinal exceeds u64".to_string()))?;
        let sort_values = items
            .iter()
            .map(|item| (sort_value(catalog, &binding, &item.key), item.direction))
            .collect();
        let candidate = TopNBinding {
            sort_values,
            ordinal,
            binding,
        };
        if heap.len() < retained {
            heap.push(candidate);
        } else if heap.peek().is_some_and(|worst| candidate < *worst) {
            heap.pop();
            heap.push(candidate);
        }
    }
    let mut selected = heap.into_vec();
    selected.sort();
    Ok(selected
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|entry| entry.binding)
        .collect())
}

fn execute_child_bindings(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
) -> Result<Vec<Binding>> {
    execute_bindings_with_limit(plan, catalog, store, context, ExecutionLimit::unlimited())
}

fn execute_node_scan_with_optional_filter(
    variable: &str,
    label: &str,
    filter: Option<(&Predicate, &PropertyFilter)>,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let exact_label = exact_scan_label_id(catalog, label);
    let exact_label_id = exact_label.flatten();
    if !store.is_out_of_core()
        && let Some(label_id) = exact_label
        && store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= memory_budget.get()
    {
        let scan = store.scan_nodes_with_filter_pruning(label_id, filter.map(|(_, filter)| filter));
        record_scan_pruning_report(scan.report.clone());
        let mut output = Vec::new();
        let mut tracker = OperatorMemoryTracker::new(memory_budget);
        for node in scan.nodes {
            let binding = node_binding(variable, node.clone());
            if let Some((predicate, _)) = filter
                && !evaluate_predicate(predicate, catalog, store, &binding)?
            {
                continue;
            }
            push_bounded_operator_binding("NodeScanExec", &mut output, binding, &mut tracker)?;
            if execution_limit.is_reached(output.len()) {
                break;
            }
        }
        return Ok(output);
    }
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    let mut callback_error = None;
    store.visit_nodes_owned(exact_label_id, |node| {
        if callback_error.is_some() {
            return GraphScanControl::Stop;
        }
        if exact_label.is_none() && !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return GraphScanControl::Continue;
        }
        if filter
            .map(|(_, property_filter)| node_matches_property_filter(&node, property_filter))
            .is_some_and(|matches| !matches)
        {
            return GraphScanControl::Continue;
        }
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        };
        if let Some((predicate, _)) = filter {
            match evaluate_predicate(predicate, catalog, store, &binding) {
                Ok(true) => {}
                Ok(false) => return GraphScanControl::Continue,
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
        }
        if let Err(error) =
            push_bounded_operator_binding("NodeScanExec", &mut output, binding, &mut tracker)
        {
            callback_error = Some(error);
            return GraphScanControl::Stop;
        }
        if execution_limit.is_reached(output.len()) {
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    let candidate_count = store.node_count_for_label(exact_label_id);
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: exact_label_id,
        rel_type_id: None,
        strategy: ScanPruningStrategy::FullLabelScan,
        pruned: false,
        exact_empty: candidate_count == 0,
        candidate_count_before_pruning: candidate_count,
        pruned_candidate_count: 0,
        candidate_count_before_filter: candidate_count,
        output_count: output.len(),
        filtered_out_count: candidate_count.saturating_sub(output.len()),
    });
    Ok(output)
}

fn execute_source_segment_scan(
    variable: &str,
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Binding>> {
    runtime_checkpoint(task_context)?;
    let Some(storage_predicate) = source_storage_scan_predicate(predicate, variable) else {
        return execute_node_scan_with_optional_filter(
            variable,
            "Source",
            None,
            catalog,
            store,
            execution_limit,
            memory.blocking_operator_bytes,
        );
    };
    let io_depth = NonZeroUsize::new(SOURCE_SEGMENT_SCAN_IO_DEPTH)
        .expect("source segment scan I/O depth is non-zero");
    let max_coalesced_bytes = NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES)
        .expect("source segment scan coalesced range limit is non-zero");
    let max_wave_bytes = NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES)
        .expect("source segment scan wave byte limit is non-zero");
    let read = store.read_published_source_scan_candidates_bounded(
        &storage_predicate,
        io_depth,
        max_coalesced_bytes,
        max_wave_bytes,
        memory.blocking_operator_bytes,
        task_context,
    );
    runtime_checkpoint(task_context)?;
    let rows = match read {
        Ok(SourceScanCandidateRead::Rows {
            skipped_segment_count,
            rows,
            ..
        }) => {
            let source_count = catalog
                .label_id("Source")
                .map(|label_id| store.node_count_for_label(Some(label_id)))
                .unwrap_or_default();
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: catalog.label_id("Source"),
                rel_type_id: None,
                strategy: source_scan_pruning_strategy(&storage_predicate),
                pruned: skipped_segment_count > 0 || rows.len() < source_count,
                exact_empty: rows.is_empty(),
                candidate_count_before_pruning: source_count,
                pruned_candidate_count: source_count.saturating_sub(rows.len()),
                candidate_count_before_filter: rows.len(),
                output_count: rows
                    .len()
                    .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                filtered_out_count: 0,
            });
            rows
        }
        Err(error @ SkeinError::Execution(_)) => return Err(error),
        Ok(SourceScanCandidateRead::Fallback(_)) | Err(_) => {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
                memory.blocking_operator_bytes,
            );
        }
    };
    let source_label_id = catalog.label_id("Source");
    let mut bindings = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    for row in rows {
        let Some(node) = store.node_owned(NodeId(row.node_id))? else {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
                memory.blocking_operator_bytes,
            );
        };
        if source_label_id.is_none_or(|label_id| !node.labels.contains(&label_id))
            || node.properties != row.properties
        {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
                memory.blocking_operator_bytes,
            );
        }
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        };
        push_bounded_operator_binding("SourceSegmentScan", &mut bindings, binding, &mut tracker)?;
        if execution_limit.is_reached(bindings.len()) {
            break;
        }
    }
    Ok(bindings)
}

fn source_scan_pruning_strategy(predicate: &ScanPredicate) -> ScanPruningStrategy {
    match predicate {
        ScanPredicate::False => ScanPruningStrategy::Empty,
        ScanPredicate::Eq { property, .. } => ScanPruningStrategy::PropertyEq {
            property: property.clone(),
        },
        ScanPredicate::In { property, .. } => ScanPruningStrategy::PropertyIn {
            property: property.clone(),
        },
        ScanPredicate::Range { property, .. } => ScanPruningStrategy::PropertyRange {
            property: property.clone(),
        },
        ScanPredicate::IsNull { property } | ScanPredicate::IsMissing { property } => {
            ScanPruningStrategy::PropertyMissingOrNull {
                property: property.clone(),
            }
        }
        ScanPredicate::Exists { property } => ScanPruningStrategy::PropertyExists {
            property: property.clone(),
        },
        ScanPredicate::Or(_) => ScanPruningStrategy::OrUnion,
        ScanPredicate::And(predicates) => predicates
            .iter()
            .map(source_scan_pruning_strategy)
            .find(|strategy| !matches!(strategy, ScanPruningStrategy::FullLabelScan))
            .unwrap_or(ScanPruningStrategy::FullLabelScan),
        ScanPredicate::True => ScanPruningStrategy::FullLabelScan,
    }
}

fn source_storage_scan_predicate(predicate: &Predicate, variable: &str) -> Option<ScanPredicate> {
    match predicate {
        Predicate::And(predicates) => {
            let predicates = predicates
                .iter()
                .filter_map(|predicate| source_storage_scan_predicate(predicate, variable))
                .collect::<Vec<_>>();
            match predicates.len() {
                0 => None,
                1 => predicates.into_iter().next(),
                _ => Some(ScanPredicate::And(predicates)),
            }
        }
        Predicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| source_storage_scan_predicate(predicate, variable))
            .collect::<Option<Vec<_>>>()
            .and_then(|predicates| {
                (!predicates.is_empty()).then_some(ScanPredicate::Or(predicates))
            }),
        Predicate::PropertyEq {
            variable: candidate,
            property,
            value,
        } if candidate == variable => Some(ScanPredicate::Eq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyIn {
            variable: candidate,
            property,
            values,
        } if candidate == variable => Some(ScanPredicate::In {
            property: property.clone(),
            values: values.clone(),
        }),
        Predicate::PropertyCompare {
            variable: candidate,
            property,
            op,
            value,
        } if candidate == variable => {
            let bound = RangeBound {
                value: value.clone(),
                inclusive: matches!(op, ComparisonOp::Gte | ComparisonOp::Lte),
            };
            let (lower, upper) = match op {
                ComparisonOp::Gt | ComparisonOp::Gte => (Some(bound), None),
                ComparisonOp::Lt | ComparisonOp::Lte => (None, Some(bound)),
            };
            Some(ScanPredicate::Range {
                property: property.clone(),
                lower,
                upper,
            })
        }
        Predicate::PropertyIsNull {
            variable: candidate,
            property,
        } if candidate == variable => Some(ScanPredicate::Or(vec![
            ScanPredicate::IsNull {
                property: property.clone(),
            },
            ScanPredicate::IsMissing {
                property: property.clone(),
            },
        ])),
        _ => None,
    }
}

fn exact_scan_label_id(catalog: &Catalog, label: &str) -> Option<Option<crate::schema::LabelId>> {
    if label.is_empty() {
        return Some(None);
    }
    if label.contains(':') {
        return None;
    }
    catalog.label_id(label).map(Some)
}

fn single_node_binding(variable: &str, node: NodeRecord) -> Binding {
    Binding {
        values: BTreeMap::new(),
        nodes: BTreeMap::from([(variable.to_string(), node)]),
        relationships: BTreeMap::new(),
    }
}

fn execute_node_column_lookup(
    spec: NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    if let Some(Some(label_id)) = exact_scan_label_id(catalog, spec.label) {
        return execute_indexed_node_column_lookup(
            &spec,
            input,
            label_id,
            store,
            execution_limit,
            memory_budget,
        );
    }

    let label_ids = label_ids_for_pattern(catalog, spec.label);
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for binding in input {
        let expected = binding.values.get(spec.column).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing column '{}' during node column lookup",
                spec.column
            ))
        })?;
        let mut matched = false;
        let mut callback_error = None;
        store.visit_nodes_owned(None, |node| {
            if node_matches_label_pattern(&node, label_ids.as_deref())
                && node.properties.get(spec.property) == Some(expected)
            {
                let mut next = binding.clone();
                next.nodes.insert(spec.variable.to_string(), node);
                if let Err(error) = push_bounded_operator_binding(
                    "NodeColumnLookupExec",
                    &mut output,
                    next,
                    &mut tracker,
                ) {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
                matched = true;
                if execution_limit.is_reached(output.len()) {
                    return GraphScanControl::Stop;
                }
            }
            GraphScanControl::Continue
        })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        if execution_limit.is_reached(output.len()) {
            return Ok(output);
        }
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            push_bounded_operator_binding("NodeColumnLookupExec", &mut output, next, &mut tracker)?;
            if execution_limit.is_reached(output.len()) {
                return Ok(output);
            }
        }
    }
    Ok(output)
}

fn execute_indexed_node_column_lookup(
    spec: &NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    label_id: crate::schema::LabelId,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let mut lookup_values = BTreeSet::new();
    for binding in &input {
        let expected = binding.values.get(spec.column).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing column '{}' during node column lookup",
                spec.column
            ))
        })?;
        lookup_values.insert(expected.clone());
    }

    let mut unique_candidate_ids = BTreeSet::new();
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for binding in input {
        let expected = binding
            .values
            .get(spec.column)
            .expect("lookup column was validated before index lookup")
            .clone();
        let mut matched = false;
        let mut callback_error = None;
        store.visit_nodes_by_property_owned(
            label_id,
            spec.property,
            std::slice::from_ref(&expected),
            |node| {
                unique_candidate_ids.insert(node.id);
                let mut next = binding.clone();
                next.nodes.insert(spec.variable.to_string(), node);
                if let Err(error) = push_bounded_operator_binding(
                    "NodeColumnLookupExec",
                    &mut output,
                    next,
                    &mut tracker,
                ) {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
                matched = true;
                if execution_limit.is_reached(output.len()) {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            },
        )?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        if execution_limit.is_reached(output.len()) {
            record_node_column_lookup_scan_pruning_report(
                label_id,
                spec.property,
                lookup_values.len(),
                unique_candidate_ids.len(),
                output.len(),
                store,
            );
            return Ok(output);
        }
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            push_bounded_operator_binding("NodeColumnLookupExec", &mut output, next, &mut tracker)?;
            if execution_limit.is_reached(output.len()) {
                record_node_column_lookup_scan_pruning_report(
                    label_id,
                    spec.property,
                    lookup_values.len(),
                    unique_candidate_ids.len(),
                    output.len(),
                    store,
                );
                return Ok(output);
            }
        }
    }

    record_node_column_lookup_scan_pruning_report(
        label_id,
        spec.property,
        lookup_values.len(),
        unique_candidate_ids.len(),
        output.len(),
        store,
    );
    Ok(output)
}

fn record_node_column_lookup_scan_pruning_report(
    label_id: crate::schema::LabelId,
    property: &str,
    lookup_value_count: usize,
    candidate_count_before_filter: usize,
    output_count: usize,
    store: &GraphStore,
) {
    let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
    record_scan_pruning_report(ScanPruningReport {
        target_kind: crate::store::ScanPruningTargetKind::Node,
        label_id: Some(label_id),
        rel_type_id: None,
        strategy: if lookup_value_count == 0 {
            ScanPruningStrategy::Empty
        } else if lookup_value_count == 1 {
            ScanPruningStrategy::PropertyEq {
                property: property.to_string(),
            }
        } else {
            ScanPruningStrategy::PropertyIn {
                property: property.to_string(),
            }
        },
        pruned: true,
        exact_empty: candidate_count_before_filter == 0,
        candidate_count_before_pruning,
        pruned_candidate_count: candidate_count_before_pruning
            .saturating_sub(candidate_count_before_filter),
        candidate_count_before_filter,
        output_count,
        filtered_out_count: 0,
    });
}

#[derive(Default)]
struct AdjacencyExpandFilters<'a> {
    relationship_scan_filter: Option<&'a PropertyFilter>,
    target_scan_filter: Option<&'a PropertyFilter>,
}

struct ExpandedBinding {
    binding: Binding,
    target_id: Option<NodeId>,
    hop: usize,
}

struct AdjacencyExpandSpec<'a> {
    source_variable: &'a str,
    rel_variable: Option<&'a str>,
    rel_properties: &'a BTreeMap<String, Value>,
    direction: RelationshipDirection,
    target_variable: &'a str,
    min_hops: usize,
    max_hops: usize,
    optional: bool,
}

#[allow(clippy::too_many_arguments)]
fn expand_binding(
    binding: Binding,
    spec: AdjacencyExpandSpec<'_>,
    rel_type_id: Option<crate::schema::RelTypeId>,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    filters: &AdjacencyExpandFilters<'_>,
    store: &GraphStore,
    memory_budget_bytes: usize,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<ExpandedBinding>> {
    runtime_checkpoint(task_context)?;
    let source = binding.nodes.get(spec.source_variable).ok_or_else(|| {
        SkeinError::Execution(format!(
            "missing variable '{}' during expand",
            spec.source_variable
        ))
    })?;
    let bound_target_id = binding.nodes.get(spec.target_variable).map(|node| node.id);
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    if spec.rel_variable.is_some()
        || !spec.rel_properties.is_empty()
        || filters.relationship_scan_filter.is_some()
        || spec.direction != RelationshipDirection::Outgoing
    {
        for (relationship, target) in one_hop_relationships_with_budget(
            store,
            source.id,
            rel_type_id,
            target_label_ids,
            spec.rel_properties,
            filters.relationship_scan_filter,
            spec.direction,
            memory_budget_bytes,
        )? {
            runtime_checkpoint(task_context)?;
            if bound_target_id.is_some_and(|node_id| node_id != target.id)
                || filters
                    .target_scan_filter
                    .is_some_and(|filter| !node_matches_property_filter(&target, filter))
            {
                continue;
            }
            let mut nodes = binding.nodes.clone();
            nodes.insert(spec.target_variable.to_string(), target.clone());
            let mut relationships = binding.relationships.clone();
            if let Some(rel_variable) = spec.rel_variable {
                relationships.insert(rel_variable.to_string(), relationship.clone());
            }
            let expanded = ExpandedBinding {
                binding: Binding {
                    values: binding.values.clone(),
                    nodes,
                    relationships,
                },
                target_id: Some(target.id),
                hop: 1,
            };
            admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
            output.push(expanded);
        }
    } else {
        for (target, hop) in bounded_expand_targets(
            store,
            source.id,
            rel_type_id.expect("typed bounded expand checked by planner"),
            target_label_ids,
            spec.min_hops,
            spec.max_hops,
            memory_budget_bytes,
        )? {
            runtime_checkpoint(task_context)?;
            if bound_target_id.is_some_and(|node_id| node_id != target.id)
                || filters
                    .target_scan_filter
                    .is_some_and(|filter| !node_matches_property_filter(&target, filter))
            {
                continue;
            }
            let mut nodes = binding.nodes.clone();
            nodes.insert(spec.target_variable.to_string(), target.clone());
            let expanded = ExpandedBinding {
                binding: Binding {
                    values: binding.values.clone(),
                    nodes,
                    relationships: binding.relationships.clone(),
                },
                target_id: Some(target.id),
                hop,
            };
            admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
            output.push(expanded);
        }
    }
    if spec.optional && output.is_empty() {
        let mut nodes = binding.nodes;
        nodes.insert(spec.target_variable.to_string(), null_lookup_node());
        let expanded = ExpandedBinding {
            binding: Binding {
                values: binding.values,
                nodes,
                relationships: binding.relationships,
            },
            target_id: None,
            hop: 0,
        };
        admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
        output.push(expanded);
    }
    Ok(output)
}

fn admit_expanded_binding(
    expanded: &ExpandedBinding,
    used_bytes: &mut usize,
    memory_budget_bytes: usize,
) -> Result<()> {
    let bytes = binding_memory_bytes(&expanded.binding);
    if bytes > memory_budget_bytes || used_bytes.saturating_add(bytes) > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "AdjacencyExpandExec seed state exceeds blocking_operator_bytes {memory_budget_bytes}"
        )));
    }
    *used_bytes = used_bytes.saturating_add(bytes);
    Ok(())
}

fn stream_filtered_adjacency_expand_batches(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    predicate: &Predicate,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let BatchReadContext { catalog, store, .. } = context;
    let mut emitted = 0usize;
    stream_adjacency_expand_batches(
        plan,
        input,
        context,
        execution_limit,
        filters,
        &mut |batch| {
            let remaining = execution_limit
                .output_rows
                .unwrap_or(usize::MAX)
                .saturating_sub(emitted);
            if remaining == 0 {
                return Ok(BatchControl::Stop);
            }
            let mut filtered = Vec::with_capacity(batch.len().min(remaining));
            for binding in batch {
                if evaluate_predicate(predicate, catalog, store, &binding)? {
                    filtered.push(binding);
                    if filtered.len() == remaining {
                        break;
                    }
                }
            }
            emitted = emitted.saturating_add(filtered.len());
            if !filtered.is_empty() && emit(filtered)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(if execution_limit.is_reached(emitted) {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            })
        },
    )
}

fn stream_adjacency_expand_batches(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let BatchReadContext {
        catalog,
        store,
        memory,
        ..
    } = context;
    let PhysicalPlan::AdjacencyExpandExec {
        source_variable,
        rel_variable,
        rel_type,
        rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        graph_budget,
        ..
    } = plan
    else {
        return Err(SkeinError::Execution(
            "expected adjacency expand plan".to_string(),
        ));
    };
    let mut graph_expansion =
        GraphExpansionExecutionState::new(*graph_budget, 0, current_vector_rerank_count());
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            graph_expansion.record(rel_type, *min_hops, *max_hops, 0);
            return Ok(BatchControl::Continue);
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    let batch_rows = memory.batch_rows.get();
    let mut output = Vec::with_capacity(batch_rows);
    let control =
        execute_binding_batches(input, context, ExecutionLimit::unlimited(), &mut |batch| {
            runtime_checkpoint(context.task_context)?;
            for binding in batch {
                runtime_checkpoint(context.task_context)?;
                graph_expansion.seed_count = graph_expansion.seed_count.saturating_add(1);
                for candidate in expand_binding(
                    binding,
                    AdjacencyExpandSpec {
                        source_variable,
                        rel_variable: rel_variable.as_deref(),
                        rel_properties,
                        direction: *direction,
                        target_variable,
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                        optional: *optional,
                    },
                    rel_type_id,
                    target_label_ids.as_deref(),
                    &filters,
                    store,
                    memory.blocking_operator_bytes.get(),
                    context.task_context,
                )? {
                    runtime_checkpoint(context.task_context)?;
                    if !graph_expansion.try_push(
                        &mut output,
                        candidate.binding,
                        candidate.target_id,
                        candidate.hop,
                    ) {
                        return Ok(BatchControl::Stop);
                    }
                    if output.len() == batch_rows
                        && emit(std::mem::replace(
                            &mut output,
                            Vec::with_capacity(batch_rows),
                        ))? == BatchControl::Stop
                    {
                        return Ok(BatchControl::Stop);
                    }
                    if execution_limit.is_reached(graph_expansion.returned_count) {
                        return Ok(BatchControl::Stop);
                    }
                }
            }
            Ok(BatchControl::Continue)
        })?;
    if !output.is_empty() && emit(output)? == BatchControl::Stop {
        graph_expansion.record(
            rel_type,
            *min_hops,
            *max_hops,
            graph_expansion.returned_count,
        );
        return Ok(BatchControl::Stop);
    }
    graph_expansion.record(
        rel_type,
        *min_hops,
        *max_hops,
        graph_expansion.returned_count,
    );
    Ok(control)
}

fn execute_adjacency_expand(
    plan: &PhysicalPlan,
    input: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    filters: AdjacencyExpandFilters<'_>,
) -> Result<Vec<Binding>> {
    let PhysicalPlan::AdjacencyExpandExec {
        source_variable,
        source_label: _,
        rel_variable,
        rel_type,
        rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        graph_budget,
        ..
    } = plan
    else {
        return Err(SkeinError::Execution(
            "expected adjacency expand plan".to_string(),
        ));
    };

    let input = execute_child_bindings(input, catalog, store, context)?;
    runtime_checkpoint(context.task_context)?;
    let mut graph_expansion = GraphExpansionExecutionState::new(
        *graph_budget,
        input.len(),
        current_vector_rerank_count(),
    );
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            graph_expansion.record(rel_type, *min_hops, *max_hops, 0);
            return Ok(Vec::new());
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    let mut output = Vec::new();
    for binding in input {
        runtime_checkpoint(context.task_context)?;
        for candidate in expand_binding(
            binding,
            AdjacencyExpandSpec {
                source_variable,
                rel_variable: rel_variable.as_deref(),
                rel_properties,
                direction: *direction,
                target_variable,
                min_hops: *min_hops,
                max_hops: *max_hops,
                optional: *optional,
            },
            rel_type_id,
            target_label_ids.as_deref(),
            &filters,
            store,
            context.memory.blocking_operator_bytes.get(),
            context.task_context,
        )? {
            runtime_checkpoint(context.task_context)?;
            if !graph_expansion.try_push(
                &mut output,
                candidate.binding,
                candidate.target_id,
                candidate.hop,
            ) || execution_limit.is_reached(output.len())
            {
                graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
                return Ok(output);
            }
        }
    }
    graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
    Ok(output)
}

struct GraphExpansionExecutionState {
    budget: Option<skein_plan::GraphExpansionBudget>,
    seed_count: usize,
    expanded_nodes: BTreeSet<NodeId>,
    expanded_edge_count: usize,
    reranked_seed_count: usize,
    payload_bytes_used: usize,
    returned_count: usize,
    truncation_reason: Option<skein_executor::GraphExpansionTruncationReason>,
}

impl GraphExpansionExecutionState {
    fn new(
        budget: Option<skein_plan::GraphExpansionBudget>,
        seed_count: usize,
        reranked_seed_count: usize,
    ) -> Self {
        Self {
            budget,
            seed_count,
            expanded_nodes: BTreeSet::new(),
            expanded_edge_count: 0,
            reranked_seed_count,
            payload_bytes_used: 0,
            returned_count: 0,
            truncation_reason: None,
        }
    }

    fn try_push(
        &mut self,
        output: &mut Vec<Binding>,
        candidate: Binding,
        target_id: Option<NodeId>,
        hop: usize,
    ) -> bool {
        let Some(budget) = self.budget else {
            self.returned_count = self.returned_count.saturating_add(1);
            output.push(candidate);
            return true;
        };
        if self.returned_count >= budget.candidate_limit {
            self.truncation_reason =
                Some(skein_executor::GraphExpansionTruncationReason::CandidateLimit);
            return false;
        }
        let candidate_bytes = binding_payload_bytes(&candidate);
        if self.payload_bytes_used.saturating_add(candidate_bytes) > budget.payload_byte_limit {
            self.truncation_reason =
                Some(skein_executor::GraphExpansionTruncationReason::PayloadByteLimit);
            return false;
        }
        self.payload_bytes_used = self.payload_bytes_used.saturating_add(candidate_bytes);
        self.returned_count = self.returned_count.saturating_add(1);
        if let Some(target_id) = target_id {
            self.expanded_nodes.insert(target_id);
        }
        self.expanded_edge_count = self.expanded_edge_count.saturating_add(hop);
        output.push(candidate);
        true
    }

    fn record(&self, rel_type: &str, min_hops: usize, max_hops: usize, returned_count: usize) {
        let Some(budget) = self.budget else {
            return;
        };
        record_graph_expansion_report(skein_executor::GraphExpansionExecutionReport {
            seed_count: self.seed_count,
            expanded_node_count: self.expanded_nodes.len(),
            expanded_edge_count: self.expanded_edge_count,
            relation_types: if rel_type.is_empty() {
                Vec::new()
            } else {
                vec![rel_type.to_string()]
            },
            min_hops,
            max_hops,
            reranked_seed_count: self.reranked_seed_count,
            candidate_limit: budget.candidate_limit,
            payload_byte_limit: budget.payload_byte_limit,
            payload_bytes_used: self.payload_bytes_used,
            returned_count,
            truncation_reason: self.truncation_reason,
        });
    }
}

fn binding_payload_bytes(binding: &Binding) -> usize {
    map_payload_bytes(&binding.values)
        .saturating_add(binding.nodes.iter().fold(0usize, |total, (name, node)| {
            total
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of_val(&node.id))
                .saturating_add(
                    node.labels
                        .len()
                        .saturating_mul(std::mem::size_of::<crate::schema::LabelId>()),
                )
                .saturating_add(map_payload_bytes(&node.properties))
        }))
        .saturating_add(
            binding
                .relationships
                .iter()
                .fold(0usize, |total, (name, relationship)| {
                    total
                        .saturating_add(name.len())
                        .saturating_add(std::mem::size_of_val(&relationship.id))
                        .saturating_add(std::mem::size_of_val(&relationship.source))
                        .saturating_add(std::mem::size_of_val(&relationship.target))
                        .saturating_add(std::mem::size_of_val(&relationship.rel_type))
                        .saturating_add(map_payload_bytes(&relationship.properties))
                }),
        )
}

pub(crate) fn map_payload_bytes(values: &BTreeMap<String, Value>) -> usize {
    values.iter().fold(0usize, |total, (name, value)| {
        total
            .saturating_add(name.len())
            .saturating_add(value_payload_bytes(value))
    })
}

fn map_memory_bytes(values: &BTreeMap<String, Value>) -> usize {
    std::mem::size_of::<BTreeMap<String, Value>>().saturating_add(values.iter().fold(
        0usize,
        |total, (name, value)| {
            total
                .saturating_add(std::mem::size_of::<(String, Value)>() * 3)
                .saturating_add(name.len())
                .saturating_add(value_memory_bytes(value))
        },
    ))
}

fn value_memory_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 0,
        Value::String(value) => value.len(),
        Value::List(values) => values
            .iter()
            .fold(std::mem::size_of::<Vec<Value>>(), |total, value| {
                total.saturating_add(value_memory_bytes(value))
            }),
        Value::Map(values) => map_memory_bytes(values),
    })
}

fn value_payload_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => std::mem::size_of::<bool>(),
        Value::Int(_) => std::mem::size_of::<i64>(),
        Value::Float(_) => std::mem::size_of::<f64>(),
        Value::String(value) => value.len(),
        Value::List(values) => values.iter().fold(0usize, |total, value| {
            total.saturating_add(value_payload_bytes(value))
        }),
        Value::Map(values) => map_payload_bytes(values),
    }
}

fn execute_aggregate(
    catalog: &Catalog,
    group_keys: &[crate::planner::Projection],
    items: &[Aggregation],
    input: &[Binding],
) -> Vec<Binding> {
    if group_keys.is_empty() {
        let mut values = BTreeMap::new();
        for item in items {
            insert_projected_value(
                &mut values,
                &item.name,
                aggregate_value(catalog, item, input),
            );
        }
        return vec![Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }];
    }

    let mut groups = BTreeMap::<Vec<Value>, Vec<&Binding>>::new();
    for binding in input {
        let key = group_keys
            .iter()
            .map(|item| group_key_value(item, catalog, binding))
            .collect::<Vec<_>>();
        groups.entry(key).or_default().push(binding);
    }

    groups
        .into_iter()
        .map(|(key, bindings)| {
            let mut values = BTreeMap::new();
            for (item, value) in group_keys.iter().zip(key) {
                insert_projected_value(&mut values, &item.name, value);
            }
            let group = bindings.into_iter().cloned().collect::<Vec<_>>();
            for item in items {
                insert_projected_value(
                    &mut values,
                    &item.name,
                    aggregate_value(catalog, item, &group),
                );
            }
            Binding {
                values,
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }
        })
        .collect()
}

fn insert_projected_value(values: &mut BTreeMap<String, Value>, name: &str, value: Value) {
    let mut candidate = name.to_string();
    let mut suffix = 2;
    loop {
        match values.entry(candidate) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
                return;
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                candidate = format!("{name}#{suffix}");
                suffix += 1;
            }
        }
    }
}

fn distinct_bindings(input: Vec<Binding>) -> Vec<Binding> {
    let mut seen = std::collections::BTreeSet::new();
    let mut output = Vec::new();
    for binding in input {
        let key = binding
            .values
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        if seen.insert(key) {
            output.push(binding);
        }
    }
    output
}

fn project_value(item: &Projection, catalog: &Catalog, binding: &Binding) -> Result<Value> {
    evaluate_projection_expression(&item.expression, catalog, binding)
}

fn evaluate_projection_expression(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    match expression {
        ProjectionExpression::Variable { variable } => binding_value(binding, catalog, variable)
            .ok_or_else(|| {
                SkeinError::Execution(format!("missing variable '{variable}' during projection"))
            }),
        ProjectionExpression::Property { variable, property } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null))
        }
        ProjectionExpression::Id { variable } => binding_id(binding, variable).ok_or_else(|| {
            SkeinError::Execution(format!("missing variable '{variable}' during projection"))
        }),
        ProjectionExpression::RelationshipType { variable } => {
            let relationship = binding.relationships.get(variable).ok_or_else(|| {
                SkeinError::Execution(format!("missing variable '{variable}' during projection"))
            })?;
            Ok(catalog
                .rel_type_name(relationship.rel_type)
                .map(|rel_type| Value::String(rel_type.to_string()))
                .unwrap_or(Value::Null))
        }
        ProjectionExpression::Literal(value) => Ok(value.clone()),
        ProjectionExpression::Coalesce(expressions) => {
            for expression in expressions {
                let value = project_expression_value(expression, catalog, binding)?;
                if value != Value::Null {
                    return Ok(value);
                }
            }
            Ok(Value::Null)
        }
        ProjectionExpression::Left { expression, length } => {
            match project_expression_value(expression, catalog, binding)? {
                Value::Null => Ok(Value::Null),
                Value::String(value) => Ok(Value::String(value.chars().take(*length).collect())),
                value => Err(SkeinError::Execution(format!(
                    "LEFT expression requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::Lower(expression) => {
            match project_expression_value(expression, catalog, binding)? {
                Value::Null => Ok(Value::Null),
                Value::String(value) => Ok(Value::String(value.to_lowercase())),
                value => Err(SkeinError::Execution(format!(
                    "LOWER expression requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::DatePart {
            part,
            variable,
            property,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::Int(nanos)) => Ok(Value::Int(timestamp_date_part(*part, *nanos))),
                Some(Value::Null) | None => Ok(Value::Null),
                Some(value) => Err(SkeinError::Execution(format!(
                    "date_part requires an integer timestamp value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value == Value::Null || value == *empty {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value == Value::Null {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            if value != Value::Null && value != *empty {
                Ok(non_empty.clone())
            } else {
                Ok(null_or_empty.clone())
            }
        }
        ProjectionExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            let value = binding_property(binding, variable, property)
                .cloned()
                .unwrap_or(Value::Null);
            for (candidate, rank) in branches {
                if value == *candidate {
                    return Ok(rank.clone());
                }
            }
            Ok(default.clone())
        }
        ProjectionExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            match binding_property(binding, variable, property) {
                Some(Value::String(value)) => Ok(Value::String(value.to_lowercase())),
                Some(Value::Null) | None => Ok(default.clone()),
                Some(value) => Err(SkeinError::Execution(format!(
                    "CASE lower-default requires a string value, got {value:?}"
                ))),
            }
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !binding_has_variable(binding, variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{variable}' during projection"
                )));
            }
            Ok(Value::Int(
                coalesce_difference(binding, variable, terms)?.max(0),
            ))
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            if !binding_has_variable(binding, &expression.variable) {
                return Err(SkeinError::Execution(format!(
                    "missing variable '{}' during projection",
                    expression.variable
                )));
            }
            let name_matches = match binding_property(
                binding,
                &expression.variable,
                &expression.name_property,
            ) {
                Some(Value::String(name)) => {
                    let lowered = name.to_lowercase();
                    matches!(&expression.raw_query, Value::String(query) if lowered == *query)
                        || matches!(&expression.normalized_query, Value::String(query) if lowered == *query)
                }
                _ => false,
            };
            if name_matches {
                return Ok(expression.exact_rank.clone());
            }
            let alias_matches =
                match binding_property(binding, &expression.variable, &expression.aliases_property)
                {
                    Some(Value::List(values)) => {
                        values.iter().any(|alias| alias == &expression.raw_input)
                    }
                    _ => false,
                };
            if alias_matches {
                Ok(expression.alias_rank.clone())
            } else {
                Ok(expression.fallback_rank.clone())
            }
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            let column = binding.values.get(&expression.column).ok_or_else(|| {
                SkeinError::Execution(format!(
                    "missing column '{}' during projection",
                    expression.column
                ))
            })?;
            let Value::String(value) = column else {
                return Ok(expression.fallback_rank.clone());
            };
            if matches!(&expression.raw_query, Value::String(query) if value == query)
                || matches!(&expression.normalized_query, Value::String(query) if value == query)
            {
                return Ok(expression.exact_rank.clone());
            }
            if matches!(&expression.raw_query, Value::String(query) if value.contains(query))
                || matches!(&expression.normalized_query, Value::String(query) if value.contains(query))
            {
                Ok(expression.contains_rank.clone())
            } else {
                Ok(expression.fallback_rank.clone())
            }
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column,
            property,
            empty,
            default,
        } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            let value = match value {
                Value::Map(values) => values.get(property).cloned().unwrap_or(Value::Null),
                Value::Null => Value::Null,
                value => {
                    return Err(SkeinError::Execution(format!(
                        "column default expression requires a map value, got {value:?}"
                    )));
                }
            };
            if value == Value::Null || value == *empty {
                Ok(default.clone())
            } else {
                Ok(value)
            }
        }
        ProjectionExpression::ColumnValueDefaultIfNull { column, default } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            if value == &Value::Null {
                Ok(default.clone())
            } else {
                Ok(value.clone())
            }
        }
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column,
            empty,
            non_empty,
            null_or_empty,
        } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            if value == &Value::Null || value == empty {
                Ok(null_or_empty.clone())
            } else {
                Ok(non_empty.clone())
            }
        }
        ProjectionExpression::Column(name) => binding.values.get(name).cloned().ok_or_else(|| {
            SkeinError::Execution(format!("missing column '{name}' during projection"))
        }),
        ProjectionExpression::ColumnProperty { column, property } => {
            let value = binding.values.get(column).ok_or_else(|| {
                SkeinError::Execution(format!("missing column '{column}' during projection"))
            })?;
            match value {
                Value::Map(values) => Ok(values.get(property).cloned().unwrap_or(Value::Null)),
                Value::Null => Ok(Value::Null),
                value => Err(SkeinError::Execution(format!(
                    "column property projection requires a map value, got {value:?}"
                ))),
            }
        }
    }
}

fn project_expression_value(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Result<Value> {
    evaluate_projection_expression(expression, catalog, binding)
}

fn coalesce_difference(
    binding: &Binding,
    variable: &str,
    terms: &[CoalesceDifferenceProjectionTerm],
) -> Result<i64> {
    let Some((first, rest)) = terms.split_first() else {
        return Err(SkeinError::Execution(
            "coalesce difference requires at least one term".to_string(),
        ));
    };
    let mut value = coalesce_integer_term(binding, variable, first)?;
    for term in rest {
        value -= coalesce_integer_term(binding, variable, term)?;
    }
    Ok(value)
}

fn coalesce_integer_term(
    binding: &Binding,
    variable: &str,
    term: &CoalesceDifferenceProjectionTerm,
) -> Result<i64> {
    match binding_property(binding, variable, &term.property) {
        Some(Value::Int(value)) => Ok(*value),
        Some(Value::Null) | None => integer_value(&term.default, "COALESCE default"),
        Some(value) => Err(SkeinError::Execution(format!(
            "COALESCE difference requires integer property '{}.{}', got {value:?}",
            variable, term.property
        ))),
    }
}

fn integer_value(value: &Value, context: &str) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        value => Err(SkeinError::Execution(format!(
            "{context} requires an integer value, got {value:?}"
        ))),
    }
}

fn group_key_value(item: &Projection, catalog: &Catalog, binding: &Binding) -> Value {
    project_value(item, catalog, binding).unwrap_or(Value::Null)
}

fn timestamp_date_part(part: DatePart, nanos: i64) -> i64 {
    let days = div_floor(nanos, 86_400_000_000_000);
    let (year, month, _) = civil_from_days(days);
    match part {
        DatePart::Year => year as i64,
        DatePart::Month => month as i64,
    }
}

fn div_floor(value: i64, divisor: i64) -> i64 {
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && ((remainder < 0) != (divisor < 0)) {
        quotient - 1
    } else {
        quotient
    }
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

fn binding_has_variable(binding: &Binding, variable: &str) -> bool {
    binding.nodes.contains_key(variable) || binding.relationships.contains_key(variable)
}

fn binding_property<'a>(binding: &'a Binding, variable: &str, property: &str) -> Option<&'a Value> {
    binding
        .nodes
        .get(variable)
        .and_then(|node| node.properties.get(property))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .and_then(|relationship| relationship.properties.get(property))
        })
}

fn binding_value(binding: &Binding, catalog: &Catalog, variable: &str) -> Option<Value> {
    binding
        .nodes
        .get(variable)
        .map(|node| node_value(node, catalog))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| relationship_value(relationship, catalog))
        })
}

fn node_value(node: &NodeRecord, catalog: &Catalog) -> Value {
    let mut values = node.properties.clone();
    values.insert("_id".to_string(), Value::Int(node.id.0 as i64));
    values.insert(
        "labels".to_string(),
        Value::List(
            node.labels
                .iter()
                .filter_map(|label_id| catalog.label_name(*label_id))
                .map(|label| Value::String(label.to_string()))
                .collect(),
        ),
    );
    Value::Map(values)
}

fn null_lookup_node() -> NodeRecord {
    NodeRecord {
        id: NodeId(0),
        labels: BTreeSet::new(),
        properties: BTreeMap::new(),
    }
}

fn relationship_value(relationship: &RelRecord, catalog: &Catalog) -> Value {
    let mut values = relationship.properties.clone();
    values.insert("_id".to_string(), Value::Int(relationship.id.0 as i64));
    values.insert(
        "source_id".to_string(),
        Value::Int(relationship.source.0 as i64),
    );
    values.insert(
        "target_id".to_string(),
        Value::Int(relationship.target.0 as i64),
    );
    values.insert(
        "type".to_string(),
        catalog
            .rel_type_name(relationship.rel_type)
            .map(|rel_type| Value::String(rel_type.to_string()))
            .unwrap_or(Value::Null),
    );
    Value::Map(values)
}

fn binding_id(binding: &Binding, variable: &str) -> Option<Value> {
    binding
        .nodes
        .get(variable)
        .map(|node| Value::Int(node.id.0 as i64))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| Value::Int(relationship.id.0 as i64))
        })
}

fn label_ids_for_pattern(catalog: &Catalog, label: &str) -> Option<Vec<crate::schema::LabelId>> {
    if label.is_empty() {
        return None;
    }
    Some(
        label
            .split(':')
            .filter_map(|label| catalog.label_id(label))
            .collect(),
    )
}

fn node_matches_label_pattern(
    node: &NodeRecord,
    label_ids: Option<&[crate::schema::LabelId]>,
) -> bool {
    match label_ids {
        None => true,
        Some(label_ids) => label_ids
            .iter()
            .any(|label_id| node.labels.contains(label_id)),
    }
}

fn node_properties_match(node: &NodeRecord, properties: &BTreeMap<String, Value>) -> bool {
    properties
        .iter()
        .all(|(property, value)| node.properties.get(property) == Some(value))
}

struct ShortestPathExecInput<'a> {
    source_label: &'a str,
    source_id: &'a Value,
    source_visibility_filter: Option<&'a PropertyFilter>,
    path_node_visibility_filter: Option<&'a PropertyFilter>,
    rel_type: &'a str,
    direction: RelationshipDirection,
    target_label: &'a str,
    target_id: &'a Value,
    target_visibility_filter: Option<&'a PropertyFilter>,
    min_hops: usize,
    max_hops: usize,
    returns: &'a [ShortestPathProjection],
}

fn execute_shortest_path(
    catalog: &Catalog,
    store: &GraphStore,
    input: ShortestPathExecInput<'_>,
    memory: &ExecutionMemoryConfig,
    execution_limit: ExecutionLimit,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Binding>> {
    runtime_checkpoint(task_context)?;
    let Some(source) =
        find_node_by_id_property(catalog, store, input.source_label, input.source_id)?
    else {
        return Ok(Vec::new());
    };
    let Some(target) =
        find_node_by_id_property(catalog, store, input.target_label, input.target_id)?
    else {
        return Ok(Vec::new());
    };
    if input
        .source_visibility_filter
        .map(|filter| !node_matches_property_filter(&source, filter))
        .unwrap_or(false)
        || input
            .target_visibility_filter
            .map(|filter| !node_matches_property_filter(&target, filter))
            .unwrap_or(false)
    {
        return Ok(Vec::new());
    }
    let rel_type_id = if input.rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(input.rel_type) else {
            return Ok(Vec::new());
        };
        Some(rel_type_id)
    };
    let (paths, search_peak_bytes, visited_paths) = all_shortest_paths(
        store,
        ShortestPathSearch {
            source: source.id,
            target: target.id,
            rel_type_id,
            direction: input.direction,
            min_hops: input.min_hops,
            max_hops: input.max_hops,
            path_node_visibility_filter: input.path_node_visibility_filter,
        },
        memory.blocking_operator_bytes,
        execution_limit.output_rows.unwrap_or(usize::MAX),
        task_context,
    )?;
    let mut output = Vec::with_capacity(paths.len());
    let mut output_tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
    for path in paths {
        runtime_checkpoint(task_context)?;
        let binding = shortest_path_binding(store, &path, input.returns)?;
        let bytes = binding_memory_bytes(&binding);
        ensure_operator_item_fits("ShortestPathExec result", bytes, &output_tracker)?;
        if output_tracker.would_exceed(bytes) {
            return Err(SkeinError::Execution(format!(
                "ShortestPathExec result state exceeds blocking_operator_bytes {}",
                output_tracker.budget_bytes
            )));
        }
        output_tracker.charge(bytes);
        output.push(binding);
    }
    record_blocking_memory_report(skein_executor::BlockingOperatorMemoryReport {
        operator: "ShortestPathExec".to_string(),
        budget_bytes: memory.blocking_operator_bytes.get(),
        peak_tracked_bytes: search_peak_bytes.max(output_tracker.peak_bytes),
        input_rows: visited_paths,
        max_spill_bytes: memory.max_spill_bytes.get(),
        max_spill_runs: memory.max_spill_runs.get(),
        spilled_bytes: 0,
        spill_run_count: 0,
        spilled_rows: 0,
    });
    Ok(output)
}

fn find_node_by_id_property(
    catalog: &Catalog,
    store: &GraphStore,
    label: &str,
    id: &Value,
) -> Result<Option<NodeRecord>> {
    let label_id = if label.is_empty() {
        None
    } else {
        let Some(label_id) = catalog.label_id(label) else {
            return Ok(None);
        };
        Some(label_id)
    };
    let mut matched = None;
    store.visit_nodes_owned(label_id, |node| {
        if node.properties.get("id") == Some(id) {
            matched = Some(node);
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    })?;
    Ok(matched)
}

struct ShortestPathSearch<'a> {
    source: NodeId,
    target: NodeId,
    rel_type_id: Option<crate::schema::RelTypeId>,
    direction: RelationshipDirection,
    min_hops: usize,
    max_hops: usize,
    path_node_visibility_filter: Option<&'a PropertyFilter>,
}

fn all_shortest_paths(
    store: &GraphStore,
    search: ShortestPathSearch<'_>,
    memory_budget: NonZeroUsize,
    result_limit: usize,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<(Vec<Vec<NodeId>>, usize, usize)> {
    let initial_path = vec![search.source];
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    tracker.charge(path_memory_bytes(&initial_path));
    let mut queue = VecDeque::from([initial_path]);
    let mut results = Vec::new();
    let mut found_depth = None;
    let mut visited_paths = 0usize;
    while let Some(path) = queue.pop_front() {
        runtime_checkpoint(task_context)?;
        visited_paths = visited_paths.saturating_add(1);
        let path_bytes = path_memory_bytes(&path);
        let depth = path.len() - 1;
        if found_depth.is_some_and(|found| depth >= found) || depth == search.max_hops {
            tracker.release(path_bytes);
            continue;
        }
        let current = *path.last().expect("path is never empty");
        for (_, next) in one_hop_relationships_with_budget(
            store,
            current,
            search.rel_type_id,
            None,
            &BTreeMap::new(),
            None,
            search.direction,
            memory_budget.get(),
        )? {
            runtime_checkpoint(task_context)?;
            if search
                .path_node_visibility_filter
                .map(|filter| !node_matches_property_filter(&next, filter))
                .unwrap_or(false)
            {
                continue;
            }
            if path.contains(&next.id) {
                continue;
            }
            let next_depth = depth + 1;
            let mut next_path = path.clone();
            next_path.push(next.id);
            let next_path_bytes = path_memory_bytes(&next_path);
            ensure_operator_item_fits("ShortestPathExec", next_path_bytes, &tracker)?;
            if tracker.would_exceed(next_path_bytes) {
                return Err(SkeinError::Execution(format!(
                    "ShortestPathExec frontier exceeds blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.charge(next_path_bytes);
            if next.id == search.target && next_depth >= search.min_hops {
                found_depth = Some(next_depth);
                results.push(next_path);
                if results.len() >= result_limit {
                    break;
                }
            } else if found_depth.is_none() && next_depth < search.max_hops {
                queue.push_back(next_path);
            } else {
                tracker.release(next_path_bytes);
            }
        }
        tracker.release(path_bytes);
        if results.len() >= result_limit {
            break;
        }
    }
    Ok((results, tracker.peak_bytes, visited_paths))
}

fn path_memory_bytes(path: &[NodeId]) -> usize {
    std::mem::size_of::<Vec<NodeId>>()
        .saturating_add(path.len().saturating_mul(std::mem::size_of::<NodeId>()))
}

fn shortest_path_binding(
    store: &GraphStore,
    path: &[NodeId],
    returns: &[ShortestPathProjection],
) -> Result<Binding> {
    let mut values = BTreeMap::new();
    for projection in returns {
        let value = match &projection.expression {
            ShortestPathProjectionExpression::NodePropertyList { property } => Value::List(
                path.iter()
                    .map(|node_id| {
                        Ok(store
                            .node_owned(*node_id)?
                            .and_then(|node| node.properties.get(property).cloned())
                            .unwrap_or(Value::Null))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            ShortestPathProjectionExpression::Length => Value::Int(path.len() as i64 - 1),
        };
        values.insert(projection.name.clone(), value);
    }
    Ok(Binding {
        values,
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    })
}

fn one_hop_relationships(
    store: &GraphStore,
    source: NodeId,
    rel_type_id: Option<crate::schema::RelTypeId>,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    rel_properties: &BTreeMap<String, Value>,
    relationship_scan_filter: Option<&PropertyFilter>,
    direction: RelationshipDirection,
) -> Result<Vec<(RelRecord, NodeRecord)>> {
    one_hop_relationships_with_budget(
        store,
        source,
        rel_type_id,
        target_label_ids,
        rel_properties,
        relationship_scan_filter,
        direction,
        DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES,
    )
}

#[allow(clippy::too_many_arguments)]
fn one_hop_relationships_with_budget(
    store: &GraphStore,
    source: NodeId,
    rel_type_id: Option<crate::schema::RelTypeId>,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    rel_properties: &BTreeMap<String, Value>,
    relationship_scan_filter: Option<&PropertyFilter>,
    direction: RelationshipDirection,
    memory_budget_bytes: usize,
) -> Result<Vec<(RelRecord, NodeRecord)>> {
    let mut matches = Vec::new();
    let mut seen = BTreeSet::new();
    let mut used_bytes = 0usize;
    let relationship_filter = combine_property_filters(
        property_filter_from_properties(rel_properties),
        relationship_scan_filter.cloned(),
    );
    let mut admit_relationship = |relationship: RelRecord| -> Result<()> {
        let Some(target_id) =
            relationship_target_for_source_direction(&relationship, source, direction)
        else {
            return Ok(());
        };
        if !seen.insert(relationship.id)
            || !relationship_properties_match(&relationship, rel_properties)
            || relationship_filter.as_ref().is_some_and(|filter| {
                !property_filter_matches_values(filter, relationship.id.0, &relationship.properties)
            })
        {
            return Ok(());
        }
        let seen_bytes = std::mem::size_of::<crate::store::RelId>()
            .saturating_add(std::mem::size_of::<usize>() * 4);
        if used_bytes.saturating_add(seen_bytes) > memory_budget_bytes {
            return Err(SkeinError::Execution(format!(
                "adjacency state exceeds blocking_operator_bytes {memory_budget_bytes}"
            )));
        }
        used_bytes = used_bytes.saturating_add(seen_bytes);
        if let Some(target) = store.node_owned(target_id)?
            && node_matches_label_pattern(&target, target_label_ids)
        {
            let match_bytes =
                relationship_memory_bytes(&relationship).saturating_add(node_memory_bytes(&target));
            if match_bytes > memory_budget_bytes
                || used_bytes.saturating_add(match_bytes) > memory_budget_bytes
            {
                return Err(SkeinError::Execution(format!(
                    "adjacency result state exceeds blocking_operator_bytes {memory_budget_bytes}"
                )));
            }
            used_bytes = used_bytes.saturating_add(match_bytes);
            matches.push((relationship, target));
        }
        Ok(())
    };
    if !store.is_out_of_core()
        && let Some(filter) = relationship_filter.as_ref()
        && store
            .relationship_count_for_type(rel_type_id)
            .saturating_mul(std::mem::size_of::<&RelRecord>())
            <= memory_budget_bytes
    {
        let scan = store.scan_relationships_with_filter_pruning(rel_type_id, Some(filter));
        record_scan_pruning_report(scan.report.clone());
        for relationship in scan.relationships {
            admit_relationship(relationship.clone())?;
        }
        matches.sort_by_key(|(relationship, target)| (target.id, relationship.id));
        return Ok(matches);
    }
    let mut visit_direction = |adjacency_direction: AdjacencyDirection| -> Result<()> {
        store.try_visit_adjacent_relationships_owned(
            source,
            rel_type_id,
            adjacency_direction,
            |relationship| {
                admit_relationship(relationship)?;
                Ok(GraphScanControl::Continue)
            },
        )?;
        Ok(())
    };
    match direction {
        RelationshipDirection::Outgoing => visit_direction(AdjacencyDirection::Outgoing)?,
        RelationshipDirection::Incoming => visit_direction(AdjacencyDirection::Incoming)?,
        RelationshipDirection::Undirected => {
            visit_direction(AdjacencyDirection::Outgoing)?;
            visit_direction(AdjacencyDirection::Incoming)?;
        }
    }
    matches.sort_by_key(|(relationship, target)| (target.id, relationship.id));
    Ok(matches)
}

fn relationship_target_for_source_direction(
    relationship: &RelRecord,
    source: NodeId,
    direction: RelationshipDirection,
) -> Option<NodeId> {
    match direction {
        RelationshipDirection::Outgoing => {
            (relationship.source == source).then_some(relationship.target)
        }
        RelationshipDirection::Incoming => {
            (relationship.target == source).then_some(relationship.source)
        }
        RelationshipDirection::Undirected => {
            if relationship.source == source {
                Some(relationship.target)
            } else if relationship.target == source {
                Some(relationship.source)
            } else {
                None
            }
        }
    }
}

fn relationship_count_sum_leg(
    catalog: &Catalog,
    store: &GraphStore,
    source: NodeId,
    leg: &RelationshipCountLeg,
) -> Result<usize> {
    let rel_type_id = if leg.rel_type.is_empty() {
        None
    } else {
        catalog.rel_type_id(&leg.rel_type)
    };
    if !leg.rel_type.is_empty() && rel_type_id.is_none() {
        return Ok(0);
    }
    Ok(one_hop_relationships(
        store,
        source,
        rel_type_id,
        None,
        &BTreeMap::new(),
        None,
        leg.direction,
    )?
    .into_iter()
    .filter(|(relationship, _)| {
        relationship_count_filter_matches(relationship, leg.filter.as_ref())
    })
    .count())
}

fn relationship_count_filter_matches(
    relationship: &RelRecord,
    filter: Option<&RelationshipCountFilter>,
) -> bool {
    match filter {
        None => true,
        Some(RelationshipCountFilter::PropertyNotEqOrEmpty { property, value }) => {
            match relationship.properties.get(property) {
                None | Some(Value::Null) => true,
                Some(Value::String(text)) if text.is_empty() => true,
                Some(current) => current != value,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn thread_repair_stats_rows(
    catalog: &Catalog,
    store: &GraphStore,
    label: &str,
    identity_label: &str,
    identity_ref_property: &str,
    thread_id_property: &str,
    message_rel_type: &str,
    message_label: &str,
    memory_rel_type: &str,
    memory_label: &str,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let thread_label_ids = label_ids_for_pattern(catalog, label);
    let identity_label_ids = label_ids_for_pattern(catalog, identity_label);
    let message_label_ids = label_ids_for_pattern(catalog, message_label);
    let memory_label_ids = label_ids_for_pattern(catalog, memory_label);
    let message_rel_type_id = catalog.rel_type_id(message_rel_type);
    let memory_rel_type_id = catalog.rel_type_id(memory_rel_type);
    let mut identities = Vec::new();
    let mut threads = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    let mut callback_error = None;
    store.visit_nodes_owned(None, |node| {
        if node_matches_label_pattern(&node, identity_label_ids.as_deref()) {
            let bytes = node_memory_bytes(&node);
            if tracker.would_exceed(bytes) {
                callback_error = Some(SkeinError::Execution(format!(
                    "ThreadRepairStatsExec state exceeds blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
                return GraphScanControl::Stop;
            }
            tracker.charge(bytes);
            identities.push(node.clone());
        }
        if node_matches_label_pattern(&node, thread_label_ids.as_deref()) {
            let bytes = node_memory_bytes(&node);
            if tracker.would_exceed(bytes) {
                callback_error = Some(SkeinError::Execution(format!(
                    "ThreadRepairStatsExec state exceeds blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
                return GraphScanControl::Stop;
            }
            tracker.charge(bytes);
            threads.push(node);
        }
        GraphScanControl::Continue
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    threads.sort_by(|left, right| {
        left.properties
            .get("id")
            .unwrap_or(&Value::Null)
            .cmp(right.properties.get("id").unwrap_or(&Value::Null))
    });
    let mut rows = Vec::with_capacity(threads.len());
    for thread in threads {
        let thread_id = thread
            .properties
            .get(thread_id_property)
            .cloned()
            .unwrap_or(Value::Null);
        let identity_refs = identities
            .iter()
            .filter(|identity| identity.properties.get(identity_ref_property) == Some(&thread_id))
            .count();
        let legacy_messages = match message_rel_type_id {
            Some(rel_type_id) => one_hop_relationships_with_budget(
                store,
                thread.id,
                Some(rel_type_id),
                message_label_ids.as_deref(),
                &BTreeMap::new(),
                None,
                RelationshipDirection::Outgoing,
                memory_budget.get(),
            )?
            .len(),
            None => 0,
        };
        let compacted_memories = match memory_rel_type_id {
            Some(rel_type_id) => one_hop_relationships_with_budget(
                store,
                thread.id,
                Some(rel_type_id),
                memory_label_ids.as_deref(),
                &BTreeMap::new(),
                None,
                RelationshipDirection::Outgoing,
                memory_budget.get(),
            )?
            .len(),
            None => 0,
        };
        let space_id = match thread.properties.get("space_id") {
            Some(Value::String(value)) if !value.is_empty() => Value::String(value.clone()),
            _ => Value::String("default".to_string()),
        };
        let message_count = match thread.properties.get("message_count") {
            Some(Value::Null) | None => Value::Int(0),
            Some(value) => value.clone(),
        };
        let binding = Binding {
            values: BTreeMap::from([
                    (
                        "t.id".to_string(),
                        thread.properties.get("id").cloned().unwrap_or(Value::Null),
                    ),
                    (
                        "t.thread_id".to_string(),
                        thread
                            .properties
                            .get("thread_id")
                            .cloned()
                            .unwrap_or(Value::Null),
                    ),
                    (
                        "CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END"
                            .to_string(),
                        space_id,
                    ),
                    ("COALESCE(t.message_count, 0)".to_string(), message_count),
                    ("identity_refs".to_string(), Value::Int(identity_refs as i64)),
                    (
                        "legacy_messages".to_string(),
                        Value::Int(legacy_messages as i64),
                    ),
                    ("COUNT(m)".to_string(), Value::Int(compacted_memories as i64)),
                ]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };
        push_bounded_operator_binding("ThreadRepairStatsExec", &mut rows, binding, &mut tracker)?;
    }
    Ok(rows)
}

fn node_matches_property_filter(node: &NodeRecord, filter: &PropertyFilter) -> bool {
    property_filter_matches_values(filter, node.id.0, &node.properties)
}

fn property_filter_matches_values(
    filter: &PropertyFilter,
    id: u64,
    properties: &BTreeMap<String, Value>,
) -> bool {
    match filter {
        PropertyFilter::And(filters) => filters
            .iter()
            .all(|filter| property_filter_matches_values(filter, id, properties)),
        PropertyFilter::Or(filters) => filters
            .iter()
            .any(|filter| property_filter_matches_values(filter, id, properties)),
        PropertyFilter::Not(filter) => !property_filter_matches_values(filter, id, properties),
        PropertyFilter::IdEq { value } => &Value::Int(id as i64) == value,
        PropertyFilter::IdNotEq { value } => &Value::Int(id as i64) != value,
        PropertyFilter::IdRange { lower, upper } => {
            value_matches_range(&Value::Int(id as i64), lower.as_ref(), upper.as_ref())
        }
        PropertyFilter::IdIn { values } => {
            values.iter().any(|value| value == &Value::Int(id as i64))
        }
        PropertyFilter::Eq { property, value } => properties
            .get(property)
            .map(|actual| actual == value)
            .unwrap_or(false),
        PropertyFilter::NotEq { property, value } => properties
            .get(property)
            .map(|actual| actual != value)
            .unwrap_or(false),
        PropertyFilter::IsNull { property } => properties
            .get(property)
            .map(|actual| actual == &Value::Null)
            .unwrap_or(true),
        PropertyFilter::IsNotNull { property } => properties
            .get(property)
            .map(|actual| actual != &Value::Null)
            .unwrap_or(false),
        PropertyFilter::In { property, values } => properties
            .get(property)
            .map(|actual| values.iter().any(|value| value == actual))
            .unwrap_or(false),
        PropertyFilter::ListContains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| actual == value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::ListContainsLower { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| match actual {
                    Value::String(actual) => actual.to_lowercase().contains(value),
                    _ => false,
                })),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::Contains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.contains(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::StartsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.starts_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::EndsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.ends_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::RegexMatch { property, pattern } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(pattern.is_match(actual)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::DefaultIfNullOrEq {
            property,
            empty,
            default,
            value,
            negated,
        } => {
            let actual = properties.get(property).unwrap_or(&Value::Null);
            let normalized = if actual == &Value::Null || actual == empty {
                default
            } else {
                actual
            };
            let matches = normalized == value;
            if *negated {
                !matches
            } else {
                matches
            }
        }
        PropertyFilter::Range {
            property,
            lower,
            upper,
        } => properties
            .get(property)
            .map(|actual| value_matches_range(actual, lower.as_ref(), upper.as_ref()))
            .unwrap_or(false),
    }
}

fn value_matches_range(
    actual: &Value,
    lower: Option<&ValueRangeBound>,
    upper: Option<&ValueRangeBound>,
) -> bool {
    let lower_matches = lower
        .map(|(lower, inclusive)| {
            if *inclusive {
                compare_property_values(actual, ComparisonOp::Gte, lower)
            } else {
                compare_property_values(actual, ComparisonOp::Gt, lower)
            }
        })
        .unwrap_or(true);
    let upper_matches = upper
        .map(|(upper, inclusive)| {
            if *inclusive {
                compare_property_values(actual, ComparisonOp::Lte, upper)
            } else {
                compare_property_values(actual, ComparisonOp::Lt, upper)
            }
        })
        .unwrap_or(true);
    lower_matches && upper_matches
}

fn relationship_properties_match(
    relationship: &RelRecord,
    rel_properties: &BTreeMap<String, Value>,
) -> bool {
    rel_properties
        .iter()
        .all(|(property, value)| relationship.properties.get(property) == Some(value))
}

fn bounded_expand_targets(
    store: &GraphStore,
    source: NodeId,
    rel_type_id: crate::schema::RelTypeId,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    min_hops: usize,
    max_hops: usize,
    memory_budget_bytes: usize,
) -> Result<Vec<(NodeRecord, usize)>> {
    let mut targets = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(
        NonZeroUsize::new(memory_budget_bytes)
            .expect("execution memory budget is represented by NonZeroUsize"),
    );
    let stack_entry_bytes = std::mem::size_of::<(NodeId, usize)>();
    tracker.charge(stack_entry_bytes);
    let mut stack = vec![(source, 0usize)];
    while let Some((current, depth)) = stack.pop() {
        tracker.release(stack_entry_bytes);
        if depth >= min_hops
            && let Some(node) = store.node_owned(current)?
            && node_matches_label_pattern(&node, target_label_ids)
        {
            let bytes = node_memory_bytes(&node).saturating_add(std::mem::size_of::<usize>());
            ensure_operator_item_fits("AdjacencyExpandExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "AdjacencyExpandExec traversal state exceeds blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.charge(bytes);
            targets.push((node, depth));
        }
        if depth == max_hops {
            continue;
        }
        let mut neighbors = Vec::new();
        let mut callback_error = None;
        store.visit_adjacent_relationships_owned(
            current,
            Some(rel_type_id),
            AdjacencyDirection::Outgoing,
            |relationship| {
                if tracker.would_exceed(stack_entry_bytes) {
                    callback_error = Some(SkeinError::Execution(format!(
                        "AdjacencyExpandExec traversal state exceeds blocking_operator_bytes {}",
                        tracker.budget_bytes
                    )));
                    return GraphScanControl::Stop;
                }
                tracker.charge(stack_entry_bytes);
                neighbors.push((relationship.target, relationship.id));
                GraphScanControl::Continue
            },
        )?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        neighbors.sort_unstable_by(|left, right| right.cmp(left));
        for (neighbor_id, _) in neighbors {
            stack.push((neighbor_id, depth + 1));
        }
    }
    Ok(targets)
}

fn aggregate_value(catalog: &Catalog, item: &Aggregation, input: &[Binding]) -> Value {
    match item.function {
        AggregateFunction::Count => {
            Value::Int(count_aggregate(&item.target, item.distinct, input) as i64)
        }
        AggregateFunction::Min => min_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Max => max_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Avg => avg_aggregate(&item.target, input).unwrap_or(Value::Null),
        AggregateFunction::Collect => {
            collect_aggregate(catalog, &item.target, item.distinct, input)
        }
    }
}

fn count_aggregate(target: &AggregateTarget, distinct: bool, input: &[Binding]) -> usize {
    if distinct {
        return count_distinct_aggregate(target, input);
    }
    match target {
        AggregateTarget::All => input.len(),
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter(|binding| binding_has_variable(binding, variable))
            .count(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter(|binding| {
                binding_property(binding, variable, property)
                    .map(|value| value != &Value::Null)
                    .unwrap_or(false)
            })
            .count(),
    }
}

fn count_distinct_aggregate(target: &AggregateTarget, input: &[Binding]) -> usize {
    match target {
        AggregateTarget::All => input.len(),
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter_map(|binding| binding_identity_key(binding, variable))
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
    }
}

fn min_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    match target {
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .min(),
        AggregateTarget::All | AggregateTarget::Variable(_) => None,
    }
}

fn max_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    match target {
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .max(),
        AggregateTarget::All | AggregateTarget::Variable(_) => None,
    }
}

fn avg_aggregate(target: &AggregateTarget, input: &[Binding]) -> Option<Value> {
    let AggregateTarget::Property { variable, property } = target else {
        return None;
    };
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in input
        .iter()
        .filter_map(|binding| binding_property(binding, variable, property))
    {
        match value {
            Value::Int(value) => {
                sum += *value as f64;
                count += 1;
            }
            Value::Float(value) if value.is_finite() => {
                sum += *value;
                count += 1;
            }
            _ => {}
        }
    }
    (count > 0).then_some(Value::Float(sum / count as f64))
}

fn collect_aggregate(
    catalog: &Catalog,
    target: &AggregateTarget,
    distinct: bool,
    input: &[Binding],
) -> Value {
    let values: Vec<Value> = match target {
        AggregateTarget::Variable(variable) => input
            .iter()
            .filter_map(|binding| binding_value(binding, catalog, variable))
            .filter(|value| *value != Value::Null)
            .collect(),
        AggregateTarget::Property { variable, property } => input
            .iter()
            .filter_map(|binding| binding_property(binding, variable, property))
            .filter(|value| *value != &Value::Null)
            .cloned()
            .collect(),
        AggregateTarget::All => Vec::new(),
    };
    if distinct {
        Value::List(
            values
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
        )
    } else {
        Value::List(values)
    }
}

fn binding_identity_key(binding: &Binding, variable: &str) -> Option<(u8, u64)> {
    binding
        .nodes
        .get(variable)
        .map(|node| (0, node.id.0))
        .or_else(|| {
            binding
                .relationships
                .get(variable)
                .map(|relationship| (1, relationship.id.0))
        })
}

fn evaluate_predicate(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
) -> Result<bool> {
    Ok(evaluate_predicate_truth(predicate, catalog, store, binding)?.is_true())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PredicateTruth {
    True,
    False,
    Unknown,
}

impl PredicateTruth {
    const fn from_bool(value: bool) -> Self {
        if value {
            Self::True
        } else {
            Self::False
        }
    }

    const fn is_true(self) -> bool {
        matches!(self, Self::True)
    }

    const fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::True, Self::True) => Self::True,
        }
    }

    const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::False, Self::False) => Self::False,
        }
    }
}

fn predicate_comparison_truth(
    actual: Option<&Value>,
    expected: &Value,
    compare: impl FnOnce(&Value, &Value) -> bool,
) -> PredicateTruth {
    match actual {
        Some(actual) if actual != &Value::Null && expected != &Value::Null => {
            PredicateTruth::from_bool(compare(actual, expected))
        }
        _ => PredicateTruth::Unknown,
    }
}

fn predicate_in_truth(actual: Option<&Value>, values: &[Value]) -> PredicateTruth {
    let Some(actual) = actual.filter(|actual| *actual != &Value::Null) else {
        return PredicateTruth::Unknown;
    };
    if values
        .iter()
        .any(|value| value != &Value::Null && value == actual)
    {
        PredicateTruth::True
    } else if values.iter().any(|value| value == &Value::Null) {
        PredicateTruth::Unknown
    } else {
        PredicateTruth::False
    }
}

fn evaluate_predicate_truth(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
) -> Result<PredicateTruth> {
    Ok(match predicate {
        Predicate::And(predicates) => {
            let mut truth = PredicateTruth::True;
            for predicate in predicates {
                truth = truth.and(evaluate_predicate_truth(
                    predicate, catalog, store, binding,
                )?);
                if truth == PredicateTruth::False {
                    break;
                }
            }
            truth
        }
        Predicate::Or(predicates) => {
            let mut truth = PredicateTruth::False;
            for predicate in predicates {
                truth = truth.or(evaluate_predicate_truth(
                    predicate, catalog, store, binding,
                )?);
                if truth == PredicateTruth::True {
                    break;
                }
            }
            truth
        }
        Predicate::Not(predicate) => {
            evaluate_predicate_truth(predicate, catalog, store, binding)?.not()
        }
        Predicate::ConstantBool(value) => PredicateTruth::from_bool(*value),
        Predicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => PredicateTruth::from_bool(relationship_exists(
            catalog,
            store,
            binding,
            variable,
            rel_type,
            *direction,
            target_label,
        )?),
        Predicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => PredicateTruth::from_bool(bound_relationship_exists(
            catalog,
            store,
            binding,
            source_variable,
            rel_type,
            *direction,
            target_variable,
        )?),
        Predicate::IdEq { variable, value } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                actual == expected
            })
        }
        Predicate::IdNotEq { variable, value } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                actual != expected
            })
        }
        Predicate::IdCompare {
            variable,
            op,
            value,
        } => {
            let actual = binding_id(binding, variable);
            predicate_comparison_truth(actual.as_ref(), value, |actual, expected| {
                compare_property_values(actual, *op, expected)
            })
        }
        Predicate::IdIn { variable, values } => {
            let actual = binding_id(binding, variable);
            predicate_in_truth(actual.as_ref(), values)
        }
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| actual == expected,
        ),
        Predicate::PropertyNotEq {
            variable,
            property,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| actual != expected,
        ),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => predicate_comparison_truth(
            binding_property(binding, variable, property),
            value,
            |actual, expected| compare_property_values(actual, *op, expected),
        ),
        Predicate::ExpressionEq { expression, value } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| actual == expected,
            )
        }
        Predicate::ExpressionNotEq { expression, value } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| actual != expected,
            )
        }
        Predicate::ExpressionCompare {
            expression,
            op,
            value,
        } => {
            let actual = predicate_expression_value(expression, catalog, binding);
            let expected = predicate_expression_value(value, catalog, binding);
            predicate_comparison_truth(
                actual.as_ref(),
                expected.as_ref().unwrap_or(&Value::Null),
                |actual, expected| compare_property_values(actual, *op, expected),
            )
        }
        Predicate::ExpressionContains { expression, value } => {
            match (
                predicate_expression_value(expression, catalog, binding),
                predicate_expression_value(value, catalog, binding),
            ) {
                (Some(Value::Null) | None, _) | (_, Some(Value::Null) | None) => {
                    PredicateTruth::Unknown
                }
                (Some(Value::String(actual)), Some(Value::String(expected))) => {
                    PredicateTruth::from_bool(actual.contains(&expected))
                }
                _ => PredicateTruth::False,
            }
        }
        Predicate::PropertyListContains {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::List(values)) => {
                PredicateTruth::from_bool(values.iter().any(|actual| actual == value))
            }
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyListContainsLower {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::List(values)) => {
                PredicateTruth::from_bool(values.iter().any(|actual| match actual {
                    Value::String(actual) => actual.to_lowercase().contains(value),
                    _ => false,
                }))
            }
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyContains {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.contains(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyStartsWith {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.starts_with(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyEndsWith {
            variable,
            property,
            value,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(actual.ends_with(value)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyRegexMatch {
            variable,
            property,
            pattern,
        } => match binding_property(binding, variable, property) {
            None | Some(Value::Null) => PredicateTruth::Unknown,
            Some(Value::String(actual)) => PredicateTruth::from_bool(pattern.is_match(actual)),
            Some(_) => PredicateTruth::False,
        },
        Predicate::PropertyIsNull { variable, property } => PredicateTruth::from_bool(
            binding_property(binding, variable, property)
                .map(|actual| actual == &Value::Null)
                .unwrap_or(true),
        ),
        Predicate::PropertyIsNotNull { variable, property } => PredicateTruth::from_bool(
            binding_property(binding, variable, property)
                .map(|actual| actual != &Value::Null)
                .unwrap_or(false),
        ),
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => predicate_in_truth(binding_property(binding, variable, property), values),
    })
}

fn relationship_exists(
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
    variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_label: &str,
) -> Result<bool> {
    let Some(source) = binding.nodes.get(variable) else {
        return Ok(false);
    };
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            return Ok(false);
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    one_hop_relationships(
        store,
        source.id,
        rel_type_id,
        target_label_ids.as_deref(),
        &BTreeMap::new(),
        None,
        direction,
    )
    .map(|relationships| !relationships.is_empty())
}

fn bound_relationship_exists(
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
    source_variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_variable: &str,
) -> Result<bool> {
    let (Some(source), Some(target)) = (
        binding.nodes.get(source_variable),
        binding.nodes.get(target_variable),
    ) else {
        return Ok(false);
    };
    let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
        return Ok(false);
    };
    one_hop_relationships(
        store,
        source.id,
        Some(rel_type_id),
        None,
        &BTreeMap::new(),
        None,
        direction,
    )
    .map(|relationships| {
        relationships
            .iter()
            .any(|(_, candidate)| candidate.id == target.id)
    })
}

fn predicate_expression_value(
    expression: &ProjectionExpression,
    catalog: &Catalog,
    binding: &Binding,
) -> Option<Value> {
    project_expression_value(expression, catalog, binding).ok()
}

fn compare_bindings(
    catalog: &Catalog,
    left: &Binding,
    right: &Binding,
    items: &[SortItem],
) -> std::cmp::Ordering {
    for item in items {
        let ordering =
            sort_value(catalog, left, &item.key).cmp(&sort_value(catalog, right, &item.key));
        let ordering = match item.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn sort_value(catalog: &Catalog, binding: &Binding, key: &SortKey) -> Value {
    match key {
        SortKey::Property { variable, property } => binding_property(binding, variable, property)
            .cloned()
            .unwrap_or(Value::Null),
        SortKey::Id { variable } => binding_id(binding, variable).unwrap_or(Value::Null),
        SortKey::Expression(expression) => {
            project_expression_value(expression, catalog, binding).unwrap_or(Value::Null)
        }
        SortKey::Column(name) => binding.values.get(name).cloned().unwrap_or(Value::Null),
    }
}

fn property_filter_from_predicate(predicate: &Predicate) -> Result<PropertyFilter> {
    match predicate {
        Predicate::And(predicates) => predicates
            .iter()
            .map(property_filter_from_predicate)
            .collect::<Result<Vec<_>>>()
            .map(PropertyFilter::And),
        Predicate::Or(predicates) => predicates
            .iter()
            .map(property_filter_from_predicate)
            .collect::<Result<Vec<_>>>()
            .map(PropertyFilter::Or),
        Predicate::Not(predicate) => property_filter_from_predicate(predicate)
            .map(Box::new)
            .map(PropertyFilter::Not),
        Predicate::IdEq { value, .. } => Ok(PropertyFilter::IdEq {
            value: value.clone(),
        }),
        Predicate::IdNotEq { value, .. } => Ok(PropertyFilter::IdNotEq {
            value: value.clone(),
        }),
        Predicate::IdCompare { op, value, .. } => {
            let (lower, upper) = range_bounds_from_comparison(*op, value.clone());
            Ok(PropertyFilter::IdRange { lower, upper })
        }
        Predicate::IdIn { values, .. } => Ok(PropertyFilter::IdIn {
            values: values.clone(),
        }),
        Predicate::PropertyEq {
            property, value, ..
        } => Ok(PropertyFilter::Eq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyNotEq {
            property, value, ..
        } => Ok(PropertyFilter::NotEq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyCompare {
            property,
            op,
            value,
            ..
        } => {
            let (lower, upper) = range_bounds_from_comparison(*op, value.clone());
            Ok(PropertyFilter::Range {
                property: property.clone(),
                lower,
                upper,
            })
        }
        Predicate::ExpressionEq { expression, value } => {
            property_filter_from_default_expression(expression, value, false)
        }
        Predicate::ExpressionNotEq { expression, value } => {
            property_filter_from_default_expression(expression, value, true)
        }
        Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. }
        | Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. } => Err(SkeinError::Execution(
            "expression predicates are not supported in property filters".to_string(),
        )),
        Predicate::PropertyListContains {
            property, value, ..
        } => Ok(PropertyFilter::ListContains {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyListContainsLower {
            property, value, ..
        } => Ok(PropertyFilter::ListContainsLower {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyContains {
            property, value, ..
        } => Ok(PropertyFilter::Contains {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyStartsWith {
            property, value, ..
        } => Ok(PropertyFilter::StartsWith {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyEndsWith {
            property, value, ..
        } => Ok(PropertyFilter::EndsWith {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyRegexMatch {
            property, pattern, ..
        } => Ok(PropertyFilter::RegexMatch {
            property: property.clone(),
            pattern: pattern.clone(),
        }),
        Predicate::PropertyIsNull { property, .. } => Ok(PropertyFilter::IsNull {
            property: property.clone(),
        }),
        Predicate::PropertyIsNotNull { property, .. } => Ok(PropertyFilter::IsNotNull {
            property: property.clone(),
        }),
        Predicate::PropertyIn {
            property, values, ..
        } => Ok(PropertyFilter::In {
            property: property.clone(),
            values: values.clone(),
        }),
    }
}

fn predicate_references_only_variable(predicate: &Predicate, variable: &str) -> bool {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .all(|predicate| predicate_references_only_variable(predicate, variable)),
        Predicate::Not(predicate) => predicate_references_only_variable(predicate, variable),
        Predicate::IdEq {
            variable: current, ..
        }
        | Predicate::IdNotEq {
            variable: current, ..
        }
        | Predicate::IdCompare {
            variable: current, ..
        }
        | Predicate::IdIn {
            variable: current, ..
        }
        | Predicate::PropertyEq {
            variable: current, ..
        }
        | Predicate::PropertyNotEq {
            variable: current, ..
        }
        | Predicate::PropertyCompare {
            variable: current, ..
        }
        | Predicate::PropertyListContains {
            variable: current, ..
        }
        | Predicate::PropertyListContainsLower {
            variable: current, ..
        }
        | Predicate::PropertyContains {
            variable: current, ..
        }
        | Predicate::PropertyStartsWith {
            variable: current, ..
        }
        | Predicate::PropertyEndsWith {
            variable: current, ..
        }
        | Predicate::PropertyRegexMatch {
            variable: current, ..
        }
        | Predicate::PropertyIsNull {
            variable: current, ..
        }
        | Predicate::PropertyIsNotNull {
            variable: current, ..
        }
        | Predicate::PropertyIn {
            variable: current, ..
        } => current == variable,
        Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. }
        | Predicate::ExpressionEq { .. }
        | Predicate::ExpressionNotEq { .. }
        | Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. } => false,
    }
}

fn node_scan_filter_from_predicate(
    predicate: &Predicate,
    variable: &str,
) -> Option<PropertyFilter> {
    if let Predicate::And(predicates) = predicate {
        let mut filters = predicates
            .iter()
            .filter_map(|predicate| node_scan_filter_from_predicate(predicate, variable))
            .collect::<Vec<_>>();
        return match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(PropertyFilter::And(filters)),
        };
    }
    predicate_references_only_variable(predicate, variable)
        .then(|| property_filter_from_predicate(predicate).ok())
        .flatten()
}

fn exact_relationship_scan_filter_from_predicate(
    predicate: &Predicate,
    variable: &str,
) -> Option<PropertyFilter> {
    if let Predicate::And(predicates) = predicate {
        let mut filters = predicates
            .iter()
            .filter_map(|predicate| {
                exact_relationship_scan_filter_from_predicate(predicate, variable)
            })
            .collect::<Vec<_>>();
        return match filters.len() {
            0 => None,
            1 => filters.pop(),
            _ => Some(PropertyFilter::And(filters)),
        };
    }
    if predicate_references_only_variable(predicate, variable) {
        return property_filter_from_predicate(predicate)
            .ok()
            .filter(exact_relationship_scan_filter_is_safe);
    }
    None
}

fn exact_relationship_scan_filter_is_safe(filter: &PropertyFilter) -> bool {
    match filter {
        PropertyFilter::And(filters) | PropertyFilter::Or(filters) => {
            filters.iter().all(exact_relationship_scan_filter_is_safe)
        }
        PropertyFilter::Eq { .. }
        | PropertyFilter::IdEq { .. }
        | PropertyFilter::IdRange { .. }
        | PropertyFilter::IdIn { .. }
        | PropertyFilter::IsNull { .. }
        | PropertyFilter::IsNotNull { .. }
        | PropertyFilter::In { .. }
        | PropertyFilter::Range { .. } => true,
        PropertyFilter::DefaultIfNullOrEq { negated, .. } => !negated,
        PropertyFilter::Not(_)
        | PropertyFilter::IdNotEq { .. }
        | PropertyFilter::NotEq { .. }
        | PropertyFilter::ListContains { .. }
        | PropertyFilter::ListContainsLower { .. }
        | PropertyFilter::Contains { .. }
        | PropertyFilter::StartsWith { .. }
        | PropertyFilter::EndsWith { .. }
        | PropertyFilter::RegexMatch { .. } => false,
    }
}

fn property_filter_from_default_expression(
    expression: &ProjectionExpression,
    value: &ProjectionExpression,
    negated: bool,
) -> Result<PropertyFilter> {
    match (expression, value) {
        (
            ProjectionExpression::DefaultIfNullOrEq {
                property,
                empty,
                default,
                ..
            },
            ProjectionExpression::Literal(value),
        ) => Ok(PropertyFilter::DefaultIfNullOrEq {
            property: property.clone(),
            empty: empty.clone(),
            default: default.clone(),
            value: value.clone(),
            negated,
        }),
        (
            ProjectionExpression::Literal(value),
            ProjectionExpression::DefaultIfNullOrEq {
                property,
                empty,
                default,
                ..
            },
        ) => Ok(PropertyFilter::DefaultIfNullOrEq {
            property: property.clone(),
            empty: empty.clone(),
            default: default.clone(),
            value: value.clone(),
            negated,
        }),
        _ => Err(SkeinError::Execution(
            "expression predicates are not supported in property filters".to_string(),
        )),
    }
}

fn property_filter_from_properties(properties: &BTreeMap<String, Value>) -> Option<PropertyFilter> {
    if properties.is_empty() {
        return None;
    }
    let mut filters = properties
        .iter()
        .map(|(property, value)| PropertyFilter::Eq {
            property: property.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    if filters.len() == 1 {
        filters.pop()
    } else {
        Some(PropertyFilter::And(filters))
    }
}

fn relationship_filter_from_properties_and_predicate(
    properties: &BTreeMap<String, Value>,
    predicate: Option<&Predicate>,
) -> Result<Option<PropertyFilter>> {
    Ok(combine_property_filters(
        property_filter_from_properties(properties),
        predicate.map(property_filter_from_predicate).transpose()?,
    ))
}

fn combine_property_filters(
    left: Option<PropertyFilter>,
    right: Option<PropertyFilter>,
) -> Option<PropertyFilter> {
    match (left, right) {
        (None, None) => None,
        (Some(filter), None) | (None, Some(filter)) => Some(filter),
        (Some(left), Some(right)) => Some(PropertyFilter::And(vec![left, right])),
    }
}

fn compare_property_values(actual: &Value, op: ComparisonOp, expected: &Value) -> bool {
    let Some(ordering) = comparable_value_ordering(actual, expected) else {
        return false;
    };
    match op {
        ComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        ComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        ComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        ComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

fn comparable_value_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn range_bounds_from_comparison(op: ComparisonOp, value: Value) -> ValueRangeBounds {
    match op {
        ComparisonOp::Lt => (None, Some((value, false))),
        ComparisonOp::Lte => (None, Some((value, true))),
        ComparisonOp::Gt => (Some((value, false)), None),
        ComparisonOp::Gte => (Some((value, true)), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission_test_config() -> ExecutionMemoryConfig {
        ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(8).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
            max_spill_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(8).unwrap(),
            spill_directory: std::env::temp_dir(),
        }
    }

    #[test]
    fn execution_admission_uses_configured_pipeline_and_blocking_budgets() {
        let plan = PhysicalPlan::SortExec {
            items: Vec::new(),
            input: Box::new(PhysicalPlan::DistinctExec {
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "n".to_string(),
                    label: "Node".to_string(),
                }),
            }),
        };

        let estimate = estimated_execution_memory(&plan, &admission_test_config());

        assert_eq!(estimate.pipeline_batch_count, 3);
        assert_eq!(estimate.blocking_operator_count, 2);
        assert_eq!(estimate.pipeline_bytes, 3 * 1024);
        assert_eq!(estimate.blocking_bytes, 2 * 4096);
        assert_eq!(estimate.fixed_operator_bytes, 0);
        assert_eq!(estimate.total_bytes, 11 * 1024);
    }

    #[test]
    fn binary_admission_uses_the_higher_memory_child_path() {
        let plan = PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(PhysicalPlan::ProjectExec {
                items: Vec::new(),
                input: Box::new(PhysicalPlan::ProjectExec {
                    items: Vec::new(),
                    input: Box::new(PhysicalPlan::SeqNodeScan {
                        variable: "left".to_string(),
                        label: "Left".to_string(),
                    }),
                }),
            }),
            right: Box::new(PhysicalPlan::DistinctExec {
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "right".to_string(),
                    label: "Right".to_string(),
                }),
            }),
        };

        let estimate = estimated_execution_memory(&plan, &admission_test_config());

        assert_eq!(estimate.pipeline_batch_count, 3);
        assert_eq!(estimate.blocking_operator_count, 2);
        assert_eq!(estimate.total_bytes, 11 * 1024);
    }

    #[test]
    fn source_segment_admission_includes_the_fixed_io_wave() {
        let plan = PhysicalPlan::SourceSegmentScan {
            variable: "n".to_string(),
            predicate: Predicate::ConstantBool(true),
        };

        let estimate = estimated_execution_memory(&plan, &admission_test_config());

        assert_eq!(estimate.pipeline_bytes, 1024);
        assert_eq!(
            estimate.fixed_operator_bytes,
            SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES
        );
        assert_eq!(
            estimate.total_bytes,
            1024 + SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES
        );
    }

    #[test]
    fn mutation_admission_reserves_wal_staging_and_bounded_results() {
        let limits = MutationLimits {
            max_affected_rows: NonZeroUsize::new(3).unwrap(),
            max_operations: NonZeroUsize::new(5).unwrap(),
            max_result_rows: NonZeroUsize::new(7).unwrap(),
            max_result_payload_bytes: NonZeroUsize::new(11).unwrap(),
        };

        assert_eq!(
            estimated_mutation_memory_bytes(limits, Some(13)),
            2 * 13
                + 5 * MUTATION_OPERATION_BOOKKEEPING_BYTES
                + 3 * MUTATION_AFFECTED_ROW_BOOKKEEPING_BYTES
                + 7 * MUTATION_RESULT_ROW_BOOKKEEPING_BYTES
                + 11
        );
        assert_eq!(estimated_mutation_memory_bytes(limits, None), u64::MAX);
    }

    fn spill_test_config(name: &str) -> ExecutionMemoryConfig {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(2).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
            max_spill_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(64).unwrap(),
            spill_directory: std::env::temp_dir().join(format!("skein-{name}-{nonce}")),
        }
    }

    #[test]
    fn sort_pipeline_spills_runs_under_a_tight_memory_budget() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for rank in (0..12).rev() {
            store
                .create_node(
                    &mut catalog,
                    "Item",
                    properties([("rank", Value::Int(rank))]),
                )
                .unwrap();
        }
        let plan = PhysicalPlan::ProjectExec {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                name: "rank".to_string(),
            }],
            input: Box::new(PhysicalPlan::SortExec {
                items: vec![SortItem {
                    key: SortKey::Property {
                        variable: "n".to_string(),
                        property: "rank".to_string(),
                    },
                    direction: SortDirection::Asc,
                }],
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "n".to_string(),
                    label: "Item".to_string(),
                }),
            }),
        };
        let memory = spill_test_config("sort-spill");
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap();

        assert_eq!(
            output
                .rows
                .iter()
                .map(|row| row["rank"].clone())
                .collect::<Vec<_>>(),
            (0..12).map(Value::Int).collect::<Vec<_>>()
        );
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "SortExec")
            .unwrap();
        assert_eq!(report.input_rows, 12);
        assert!(report.spill_run_count > 1);
        assert_eq!(report.spilled_rows, 12);
        assert!(report.spilled_bytes > 0);
        assert!(report.spilled_bytes <= report.max_spill_bytes);
        assert!(report.spill_run_count <= report.max_spill_runs);
        let pipeline = &output.profile.pipeline_memory_report;
        assert_eq!(pipeline.intermediate_rows, 36);
        assert!(pipeline.intermediate_payload_bytes >= pipeline.output_payload_bytes);
        assert_eq!(pipeline.peak_batch_rows, 2);
        assert_eq!(pipeline.output_rows, 12);
        assert!(pipeline.output_payload_bytes > 0);
        assert!(pipeline.start_resident_bytes.is_some());
        assert!(pipeline.steady_resident_bytes.is_some());
        assert!(pipeline.peak_resident_bytes.is_some());
        assert!(pipeline.total_page_faults.is_some());
        assert_eq!(pipeline.minor_page_faults.is_some(), cfg!(unix));
        assert_eq!(pipeline.major_page_faults.is_some(), cfg!(unix));
        assert!(std::fs::read_dir(&memory.spill_directory)
            .unwrap()
            .next()
            .is_none());
        std::fs::remove_dir(memory.spill_directory).unwrap();
    }

    #[test]
    fn grouped_aggregate_pipeline_spills_and_merges_groups() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for value in 0..20 {
            store
                .create_node(
                    &mut catalog,
                    "Item",
                    properties([("group", Value::Int(value % 3))]),
                )
                .unwrap();
        }
        let plan = PhysicalPlan::AggregateExec {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "n".to_string(),
                    property: "group".to_string(),
                },
                name: "group".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::All,
                distinct: false,
                name: "count".to_string(),
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        };
        let memory = spill_test_config("aggregate-spill");
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap();

        assert_eq!(
            output
                .rows
                .iter()
                .map(|row| (row["group"].clone(), row["count"].clone()))
                .collect::<Vec<_>>(),
            vec![
                (Value::Int(0), Value::Int(7)),
                (Value::Int(1), Value::Int(7)),
                (Value::Int(2), Value::Int(6)),
            ]
        );
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "AggregateExec")
            .unwrap();
        assert_eq!(report.input_rows, 20);
        assert!(report.spill_run_count > 1);
        assert_eq!(report.spilled_rows, 20);
        assert!(report.spilled_bytes > 0);
        assert!(report.spilled_bytes <= report.max_spill_bytes);
        assert!(report.spill_run_count <= report.max_spill_runs);
        assert!(std::fs::read_dir(&memory.spill_directory)
            .unwrap()
            .next()
            .is_none());
        std::fs::remove_dir(memory.spill_directory).unwrap();
    }

    #[test]
    fn top_n_pipeline_spills_without_changing_order_or_offset() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for rank in (0..50).rev() {
            store
                .create_node(
                    &mut catalog,
                    "Item",
                    properties([("rank", Value::Int(rank))]),
                )
                .unwrap();
        }
        let plan = PhysicalPlan::ProjectExec {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                name: "rank".to_string(),
            }],
            input: Box::new(PhysicalPlan::TopNExec {
                items: vec![SortItem {
                    key: SortKey::Property {
                        variable: "n".to_string(),
                        property: "rank".to_string(),
                    },
                    direction: SortDirection::Asc,
                }],
                offset: 7,
                limit: 5,
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "n".to_string(),
                    label: "Item".to_string(),
                }),
            }),
        };
        let memory = spill_test_config("topn-spill");
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap();

        assert_eq!(
            output
                .rows
                .iter()
                .map(|row| row["rank"].clone())
                .collect::<Vec<_>>(),
            (7..12).map(Value::Int).collect::<Vec<_>>()
        );
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "TopNExec")
            .unwrap();
        assert!(report.spill_run_count > 1);
        assert!(report.spilled_rows > 0);
        assert!(report.spilled_rows <= report.input_rows);
        assert!(report.spilled_bytes > 0);
        assert!(report.spilled_bytes <= report.max_spill_bytes);
        assert!(report.spill_run_count <= report.max_spill_runs);
        assert!(std::fs::read_dir(&memory.spill_directory)
            .unwrap()
            .next()
            .is_none());
        std::fs::remove_dir(memory.spill_directory).unwrap();
    }

    #[test]
    fn distinct_spills_and_deduplicates_across_memory_bounded_runs() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for value in 0..20 {
            store
                .create_node(
                    &mut catalog,
                    "Item",
                    properties([(
                        "value",
                        Value::String(format!("{}-{}", value % 5, "x".repeat(96))),
                    )]),
                )
                .unwrap();
        }
        let plan = PhysicalPlan::DistinctExec {
            input: Box::new(PhysicalPlan::ProjectExec {
                items: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "n".to_string(),
                        property: "value".to_string(),
                    },
                    name: "value".to_string(),
                }],
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "n".to_string(),
                    label: "Item".to_string(),
                }),
            }),
        };
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
            ..spill_test_config("distinct-admission")
        };
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap();
        assert_eq!(output.rows.len(), 5);
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "DistinctExec")
            .unwrap();
        assert!(report.spilled_bytes > 0);
        assert!(report.spill_run_count > 1);
        assert_eq!(report.spilled_rows, 20);
        assert!(report.peak_tracked_bytes <= report.budget_bytes);
        assert!(std::fs::read_dir(&memory.spill_directory)
            .unwrap()
            .next()
            .is_none());
        std::fs::remove_dir(memory.spill_directory).unwrap();
    }

    #[test]
    fn collect_aggregate_rejects_unbounded_group_state() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for value in 0..20 {
            store
                .create_node(
                    &mut catalog,
                    "Item",
                    properties([(
                        "value",
                        Value::String(format!("{value}-{}", "x".repeat(64))),
                    )]),
                )
                .unwrap();
        }
        let plan = PhysicalPlan::AggregateExec {
            group_keys: Vec::new(),
            items: vec![Aggregation {
                function: AggregateFunction::Collect,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "values".to_string(),
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        };
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
            ..spill_test_config("collect-admission")
        };
        let mut external = NoExternalReadOperator;
        let error = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap_err();
        assert!(error.to_string().contains("AggregateExec state exceeds"));
    }

    #[test]
    fn cartesian_product_spills_an_oversized_build_side() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for value in 0..20 {
            store
                .create_node(
                    &mut catalog,
                    "Right",
                    properties([("value", Value::Int(value))]),
                )
                .unwrap();
        }
        store
            .create_node(&mut catalog, "Left", BTreeMap::new())
            .unwrap();
        let plan = PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "left".to_string(),
                label: "Left".to_string(),
            }),
            right: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "right".to_string(),
                label: "Right".to_string(),
            }),
        };
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
            ..spill_test_config("cartesian-admission")
        };
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap();
        assert_eq!(output.rows.len(), 20);
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "NodeCartesianProductExec")
            .unwrap();
        assert!(report.spilled_bytes > 0);
        assert!(report.spill_run_count > 0);
        assert_eq!(report.spilled_rows, 20);
        assert!(report.peak_tracked_bytes <= report.budget_bytes);
        assert!(std::fs::read_dir(&memory.spill_directory)
            .unwrap()
            .next()
            .is_none());
        std::fs::remove_dir(memory.spill_directory).unwrap();
    }

    #[test]
    fn shortest_path_rejects_an_oversized_frontier() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Node", BTreeMap::new())
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Node", BTreeMap::new())
            .unwrap();
        for _ in 0..32 {
            let middle = store
                .create_node(&mut catalog, "Node", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(&mut catalog, source, middle, "LINK", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(&mut catalog, middle, target, "LINK", BTreeMap::new())
                .unwrap();
        }
        let error = all_shortest_paths(
            &store,
            ShortestPathSearch {
                source,
                target,
                rel_type_id: catalog.rel_type_id("LINK"),
                direction: RelationshipDirection::Outgoing,
                min_hops: 1,
                max_hops: 2,
                path_node_visibility_filter: None,
            },
            NonZeroUsize::new(512).unwrap(),
            usize::MAX,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("blocking_operator_bytes"));
    }

    #[test]
    fn sort_rejects_spill_run_count_over_budget() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for rank in (0..50).rev() {
            store
                .create_node(
                    &mut catalog,
                    "Item",
                    properties([("rank", Value::Int(rank))]),
                )
                .unwrap();
        }
        let plan = PhysicalPlan::SortExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                direction: SortDirection::Asc,
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        };
        let memory = ExecutionMemoryConfig {
            max_spill_runs: NonZeroUsize::new(1).unwrap(),
            ..spill_test_config("sort-run-admission")
        };
        let mut external = NoExternalReadOperator;
        let error = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exceeded max_spill_runs 1"));
        assert!(std::fs::read_dir(&memory.spill_directory)
            .unwrap()
            .next()
            .is_none());
        std::fs::remove_dir(memory.spill_directory).unwrap();
    }

    #[test]
    fn sort_rejects_spill_bytes_over_budget_and_removes_partial_run() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for rank in (0..20).rev() {
            store
                .create_node(
                    &mut catalog,
                    "Item",
                    properties([("rank", Value::Int(rank))]),
                )
                .unwrap();
        }
        let plan = PhysicalPlan::SortExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                direction: SortDirection::Asc,
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        };
        let memory = ExecutionMemoryConfig {
            max_spill_bytes: NonZeroU64::new(32).unwrap(),
            ..spill_test_config("sort-byte-admission")
        };
        let mut external = NoExternalReadOperator;
        let error = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap_err();
        assert!(error.to_string().contains("remaining spill budget 32"));
        assert!(std::fs::read_dir(&memory.spill_directory)
            .unwrap()
            .next()
            .is_none());
        std::fs::remove_dir(memory.spill_directory).unwrap();
    }

    #[test]
    fn graph_algorithms_admit_direction_specific_projections() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .register_projected_graph(
                "MemoryGraph",
                ProjectedGraphDefinition {
                    node_labels: vec!["Memory".to_string()],
                    rel_types: vec!["MENTIONS".to_string()],
                },
            )
            .unwrap();
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
            ..spill_test_config("algorithm-admission")
        };

        for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
            let plan = PhysicalPlan::GraphAlgorithm {
                algorithm,
                graph_name: "MemoryGraph".to_string(),
                options: crate::planner::GraphAlgorithmOptions {
                    damping: None,
                    max_iterations: Some(2),
                    max_levels: Some(1),
                },
                score_column: "score".to_string(),
                node_visibility_predicate: None,
            };
            let mut external = NoExternalReadOperator;
            let output = execute_with_row_limit_profile_and_external_and_memory(
                &plan,
                &mut catalog,
                &mut store,
                &BTreeMap::new(),
                &mut external,
                None,
                &memory,
            )
            .unwrap();
            assert_eq!(output.rows.len(), 2);
        }
    }

    #[test]
    fn evaluates_nested_projection_expression_without_rebuilding_projection() {
        let expression = ProjectionExpression::Coalesce(vec![
            ProjectionExpression::Literal(Value::Null),
            ProjectionExpression::Lower(Box::new(ProjectionExpression::Left {
                expression: Box::new(ProjectionExpression::Literal(Value::String(
                    "SKEIN".to_string(),
                ))),
                length: 3,
            })),
        ]);
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };

        let value = evaluate_projection_expression(&expression, &Catalog::default(), &binding)
            .expect("nested projection expression should evaluate");

        assert_eq!(value, Value::String("ske".to_string()));
    }

    #[test]
    fn graph_expansion_state_enforces_candidate_and_payload_budgets_before_push() {
        let binding = Binding {
            values: BTreeMap::from([("value".to_string(), Value::String("payload".to_string()))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };
        let mut candidate_limited = GraphExpansionExecutionState::new(
            Some(skein_plan::GraphExpansionBudget {
                candidate_limit: 1,
                payload_byte_limit: usize::MAX,
            }),
            1,
            1,
        );
        let mut output = Vec::new();
        assert!(candidate_limited.try_push(&mut output, binding.clone(), None, 1));
        assert!(!candidate_limited.try_push(&mut output, binding.clone(), None, 1));
        assert_eq!(
            candidate_limited.truncation_reason,
            Some(skein_executor::GraphExpansionTruncationReason::CandidateLimit)
        );

        let mut payload_limited = GraphExpansionExecutionState::new(
            Some(skein_plan::GraphExpansionBudget {
                candidate_limit: 2,
                payload_byte_limit: binding_payload_bytes(&binding).saturating_sub(1),
            }),
            1,
            1,
        );
        let mut output = Vec::new();
        assert!(!payload_limited.try_push(&mut output, binding, None, 1));
        assert!(output.is_empty());
        assert_eq!(
            payload_limited.truncation_reason,
            Some(skein_executor::GraphExpansionTruncationReason::PayloadByteLimit)
        );
    }

    #[test]
    fn node_column_lookup_uses_property_index_pruning_for_exact_label() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:1".to_string())),
                    ("title", Value::String("Graph foundations".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:2".to_string())),
                    ("title", Value::String("Storage notes".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:3".to_string())),
                    ("title", Value::String("Runtime notes".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Seed",
                properties([("target_stable_id", Value::String("memory:2".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Seed",
                properties([("target_stable_id", Value::String("memory:4".to_string()))]),
            )
            .unwrap();

        let plan = PhysicalPlan::ProjectExec {
            items: vec![
                Projection {
                    expression: ProjectionExpression::Property {
                        variable: "m".to_string(),
                        property: "stable_id".to_string(),
                    },
                    name: "stable_id".to_string(),
                },
                Projection {
                    expression: ProjectionExpression::Property {
                        variable: "m".to_string(),
                        property: "title".to_string(),
                    },
                    name: "title".to_string(),
                },
            ],
            input: Box::new(PhysicalPlan::NodeColumnLookupExec {
                variable: "m".to_string(),
                label: "Memory".to_string(),
                property: "stable_id".to_string(),
                column: "lookup_id".to_string(),
                optional: true,
                input: Box::new(PhysicalPlan::ProjectExec {
                    items: vec![Projection {
                        expression: ProjectionExpression::Property {
                            variable: "s".to_string(),
                            property: "target_stable_id".to_string(),
                        },
                        name: "lookup_id".to_string(),
                    }],
                    input: Box::new(PhysicalPlan::SeqNodeScan {
                        variable: "s".to_string(),
                        label: "Seed".to_string(),
                    }),
                }),
            }),
        };

        let output = execute_with_row_limit_profile(&plan, &mut catalog, &mut store, None).unwrap();

        assert_eq!(output.rows.len(), 2);
        assert_eq!(
            output.rows[0].get("stable_id"),
            Some(&Value::String("memory:2".to_string()))
        );
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Storage notes".to_string()))
        );
        assert_eq!(output.rows[1].get("stable_id"), Some(&Value::Null));
        assert_eq!(output.rows[1].get("title"), Some(&Value::Null));
        let lookup_scan = output
            .profile
            .scan_pruning_reports
            .iter()
            .find(|report| {
                report.strategy
                    == ScanPruningStrategy::PropertyIn {
                        property: "stable_id".to_string(),
                    }
            })
            .expect("node column lookup should emit property-in pruning evidence");
        assert_eq!(
            lookup_scan.target_kind,
            crate::store::ScanPruningTargetKind::Node
        );
        assert!(lookup_scan.pruned);
        assert!(!lookup_scan.exact_empty);
        assert_eq!(lookup_scan.candidate_count_before_pruning, 3);
        assert_eq!(lookup_scan.candidate_count_before_filter, 1);
        assert_eq!(lookup_scan.pruned_candidate_count, 2);
        assert_eq!(lookup_scan.output_count, 2);
    }

    #[test]
    fn source_segment_scan_uses_checkpoint_sidecar_and_keeps_filter_semantics() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("skein-source-segment-executor-{nonce}"));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Source",
                BTreeMap::from([
                    ("id".to_string(), Value::String("source-a".to_string())),
                    ("space_id".to_string(), Value::String("alpha".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Source",
                BTreeMap::from([
                    ("id".to_string(), Value::String("source-b".to_string())),
                    ("space_id".to_string(), Value::String("beta".to_string())),
                ]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();

        let predicate = Predicate::PropertyEq {
            variable: "s".to_string(),
            property: "space_id".to_string(),
            value: Value::String("alpha".to_string()),
        };
        let plan = PhysicalPlan::FilterExec {
            predicate: predicate.clone(),
            input: Box::new(PhysicalPlan::SourceSegmentScan {
                variable: "s".to_string(),
                predicate,
            }),
        };
        let parameters = BTreeMap::new();
        let mut external = NoExternalReadOperator;
        let memory = ExecutionMemoryConfig::default();
        let mut context = ExecutionContext {
            parameters: &parameters,
            external: &mut external,
            memory: &memory,
            task_context: None,
        };
        let bindings = execute_bindings_with_limit(
            &plan,
            &mut catalog,
            &mut store,
            &mut context,
            ExecutionLimit::unlimited(),
        )
        .unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(
            bindings[0].nodes["s"].properties["id"],
            Value::String("source-a".to_string())
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    fn properties(
        items: impl IntoIterator<Item = (&'static str, Value)>,
    ) -> BTreeMap<String, Value> {
        items
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }
}
