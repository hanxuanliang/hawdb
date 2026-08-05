use crate::analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraph, ProjectionLayout, ProjectionMemoryBudget,
};
use crate::cypher::RelationshipDirection;
use crate::error::{Result, SkeinError};
use crate::optimizer::PhysicalPlan;
use crate::planner::{
    Aggregation, GraphAlgorithmKind, Predicate, Projection, RelationshipCountLeg,
    RelationshipOnCreateValue, SetNodePropertiesReturnMode, SetValue, SortItem,
};
use crate::schema::Catalog;
use crate::store::{
    ConnectedNodesCreate, GraphMutation, GraphScanControl, GraphStore,
    MatchedRelationshipCopyMerge, MatchedRelationshipCreate, MatchedRelationshipMerge,
    MatchedRelationshipRetargetMerge, MatchedRelationshipSourceRetargetMerge, MutationLimits,
    NodeId, NodeRecord, NodeSetAssignment, NodeSetValue, ProjectedGraphDefinition, PropertyFilter,
    RelRecord, RelationshipDeleteRequest, RelationshipOnCreatePropertyValue,
    RelationshipPropertiesUpdate, RelationshipPropertyUpdate, RelationshipSetAssignment,
    RelationshipTargetNodeDelete, ScanPruningReport, SourceScanCandidateRead,
};
use crate::value::Value;
use skein_core::RuntimeTaskContext;
use skein_ddl::{object_state_to_core, property_type_to_core, table_kind_to_core};
use skein_executor::{ExecutionLimit, VectorExecutionReport};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::num::{NonZeroU64, NonZeroUsize};

mod batch;
mod blocking;
mod expression;
mod mutation;
mod observer;
mod read;
mod scan;
mod store_adapter;
mod traversal;

use batch::*;
use blocking::*;
use expression::*;
pub use mutation::{execute_mutation_with_limits, is_mutation_plan, mutation_command};
use mutation::{
    node_set_assignment, relationship_on_create_property_value,
    try_projected_graph_with_node_filter,
};
use observer::RootExecutionObserver;
use read::*;
use scan::*;
#[cfg(feature = "tokio-runtime")]
pub(crate) use skein_executor::binding::map_memory_bytes;
pub(crate) use skein_executor::binding::map_payload_bytes;
use skein_executor::binding::{binding_memory_bytes, binding_payload_bytes, Binding, TopNBinding};
pub(crate) use skein_executor::external::NoExternalReadOperator;
use skein_executor::graph::GraphExpansionExecutionState;
use skein_executor::kernel::{
    collect_bounded_operator_bindings, ensure_operator_item_fits, push_bounded_operator_binding,
    OperatorMemoryTracker, SpillBudgetTracker,
};
pub(crate) use skein_executor::memory::{
    estimated_execution_memory, estimated_mutation_memory_bytes,
};
use skein_executor::memory::{DEFAULT_EXECUTION_BATCH_ROWS, SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES};
use skein_executor::pipeline::{
    emit_binding_iterator, emit_owned_binding_batches, runtime_checkpoint, BatchControl,
    BindingBatch,
};
use skein_executor::predicate::{
    label_ids_for_pattern, node_matches_label_pattern, node_matches_property_filter,
    node_properties_match, property_filter_from_properties,
};
use skein_executor::scan::{
    expand_binding, single_node_binding, source_scan_pruning_strategy,
    source_storage_scan_predicate, AdjacencyExpandFilters, AdjacencyExpandSpec,
    NodeColumnLookupSpec, NodeScanContext, NodeScanSpec,
};
use skein_executor::spill;
pub use skein_executor::ExecutionMemoryConfig;
pub use skein_executor::{
    ExternalReadOperator, VectorSeedExecutionOutput, VectorSeedExecutionRequest,
    VectorSeedExecutionRow,
};
use traversal::*;

pub type Row = skein_executor::Row;
pub type ReadExecutionProfile = skein_executor::ReadExecutionProfile<ScanPruningReport>;
pub type ProfiledQueryRows = skein_executor::ProfiledQueryRows<ScanPruningReport>;
pub type ProfiledQueryStream = skein_executor::ProfiledQueryStream<ScanPruningReport>;
const SOURCE_SEGMENT_SCAN_IO_DEPTH: usize = 2;
const SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES: u64 = 512 * 1024;
thread_local! {
    static SCAN_PRUNING_REPORT_CAPTURE: RefCell<Option<Vec<ScanPruningReport>>> = const { RefCell::new(None) };
    static VECTOR_EXECUTION_REPORT_CAPTURE: RefCell<Option<Vec<VectorExecutionReport>>> = const { RefCell::new(None) };
    static GRAPH_EXPANSION_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::GraphExpansionExecutionReport>>> = const { RefCell::new(None) };
    static BLOCKING_MEMORY_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::BlockingOperatorMemoryReport>>> = const { RefCell::new(None) };
    static PIPELINE_MEMORY_REPORT_CAPTURE: RefCell<Option<skein_executor::PipelineMemoryReport>> = const { RefCell::new(None) };
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
                            let external = BatchExternalReadAdapter::new(&mut *context.external);
                            let batch_context = BatchReadContext {
                                catalog,
                                store,
                                parameters: context.parameters,
                                external: &external,
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

#[cfg(test)]
#[path = "executor/tests.rs"]
mod tests;
