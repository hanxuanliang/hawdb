use crate::analytics::{LouvainOptions, PageRankOptions, ProjectedGraph};
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
    AdjacencyDirection, ConnectedNodesCreate, GraphMutation, GraphStore,
    MatchedRelationshipCopyMerge, MatchedRelationshipCreate, MatchedRelationshipMerge,
    MatchedRelationshipRetargetMerge, MatchedRelationshipSourceRetargetMerge, NodeId, NodeRecord,
    NodeSetAssignment, NodeSetValue, OrderedAdjacencyEntry, ProjectedGraphDefinition,
    PropertyFilter, RelRecord, RelationshipDeleteRequest, RelationshipOnCreatePropertyValue,
    RelationshipPropertiesUpdate, RelationshipPropertyUpdate, RelationshipSetAssignment,
    RelationshipTargetNodeDelete, ScanPredicate, ScanPruningReport, ScanPruningStrategy,
    SourceScanCandidateRead,
};
use crate::value::Value;
use skein_ddl::{object_state_to_core, property_type_to_core, table_kind_to_core};
use skein_executor::{ExecutionLimit, VectorExecutionReport};
use skein_storage::RangeBound;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::{NonZeroU64, NonZeroUsize};

pub type Row = skein_executor::Row;
pub type ReadExecutionProfile = skein_executor::ReadExecutionProfile<ScanPruningReport>;
pub type ProfiledQueryRows = skein_executor::ProfiledQueryRows<ScanPruningReport>;
type ValueRangeBound = (Value, bool);
type ValueRangeBounds = (Option<ValueRangeBound>, Option<ValueRangeBound>);

const SOURCE_SEGMENT_SCAN_IO_DEPTH: usize = 2;
const SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES: u64 = 512 * 1024;
const SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES: u64 = 2 * 1024 * 1024;

thread_local! {
    static SCAN_PRUNING_REPORT_CAPTURE: RefCell<Option<Vec<ScanPruningReport>>> = const { RefCell::new(None) };
    static VECTOR_EXECUTION_REPORT_CAPTURE: RefCell<Option<Vec<VectorExecutionReport>>> = const { RefCell::new(None) };
    static GRAPH_EXPANSION_REPORT_CAPTURE: RefCell<Option<Vec<skein_executor::GraphExpansionExecutionReport>>> = const { RefCell::new(None) };
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

struct NodeColumnLookupSpec<'a> {
    variable: &'a str,
    label: &'a str,
    property: &'a str,
    column: &'a str,
    optional: bool,
}

struct ExecutionContext<'a> {
    parameters: &'a BTreeMap<String, Value>,
    external: &'a mut dyn ExternalReadOperator,
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
    let execution_limit = ExecutionLimit::from_user_max_rows(max_rows)?;
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let mut context = ExecutionContext {
        parameters: &parameters,
        external: &mut external,
    };
    let bindings =
        execute_bindings_with_limit(plan, catalog, store, &mut context, execution_limit)?;
    collect_rows(bindings, max_rows)
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
    let execution_limit = ExecutionLimit::from_user_max_rows(max_rows)?;
    let mut profile = read_execution_profile(plan, max_rows)?;
    let mut context = ExecutionContext {
        parameters,
        external,
    };
    let (((bindings, scan_pruning_reports), vector_execution_reports), graph_expansion_reports) =
        capture_graph_expansion_reports(|| {
            capture_vector_execution_reports(|| {
                capture_scan_pruning_reports(|| {
                    execute_bindings_with_limit(plan, catalog, store, &mut context, execution_limit)
                })
            })
        })?;
    profile.scan_pruning_reports = scan_pruning_reports;
    profile.vector_execution_reports = vector_execution_reports;
    profile.graph_expansion_reports = graph_expansion_reports;
    let rows = collect_rows(bindings, max_rows)?;
    Ok(ProfiledQueryRows { rows, profile })
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
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
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

fn collect_rows(bindings: Vec<Binding>, max_rows: Option<usize>) -> Result<Vec<Row>> {
    let Some(max_rows) = max_rows else {
        return Ok(bindings.into_iter().map(|binding| binding.values).collect());
    };
    let mut rows = Vec::with_capacity(bindings.len().min(max_rows));
    for binding in bindings {
        if rows.len() == max_rows {
            return Err(SkeinError::Execution(format!(
                "read query returned more than {max_rows} rows, exceeding max_read_result_rows {max_rows}"
            )));
        }
        rows.push(binding.values);
    }
    Ok(rows)
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
                SetValue::Coalesce { .. } => Err(crate::error::SkeinError::Semantic(
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
        value: match &assignment.value {
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

fn projected_graph(
    catalog: &Catalog,
    store: &GraphStore,
    node_labels: &[String],
    rel_types: &[String],
) -> ProjectedGraph {
    projected_graph_with_node_filter(catalog, store, node_labels, rel_types, |_| true)
}

fn projected_graph_with_node_filter(
    catalog: &Catalog,
    store: &GraphStore,
    node_labels: &[String],
    rel_types: &[String],
    include_node: impl Fn(&NodeRecord) -> bool,
) -> ProjectedGraph {
    if node_labels.is_empty() && rel_types.is_empty() {
        return ProjectedGraph::from_store_with_node_filter(store, None, include_node);
    }
    let label_ids = node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::empty();
    }
    let rel_type_ids = rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    if !rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::from_store_without_edges_with_node_filter(store, include_node);
        }
        return ProjectedGraph::from_store_labels_without_edges_with_node_filter(
            store,
            &label_ids,
            include_node,
        );
    }
    ProjectedGraph::from_store_labels_and_rel_types_with_node_filter(
        store,
        &label_ids,
        &rel_type_ids,
        include_node,
    )
}

fn execute_bindings_with_limit(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
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
            let graph = projected_graph(catalog, store, node_labels, rel_types);
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
            let graph = if let Some(filter) = node_visibility_filter.as_ref() {
                projected_graph_with_node_filter(
                    catalog,
                    store,
                    &definition.node_labels,
                    &definition.rel_types,
                    |node| node_matches_property_filter(node, filter),
                )
            } else {
                store
                    .projected_graph_artifact(graph_name, definition)
                    .cloned()
                    .unwrap_or_else(|| {
                        projected_graph(
                            catalog,
                            store,
                            &definition.node_labels,
                            &definition.rel_types,
                        )
                    })
            };
            Ok(match algorithm {
                GraphAlgorithmKind::PageRank => graph
                    .page_rank(PageRankOptions {
                        iterations: options
                            .max_iterations
                            .unwrap_or_else(|| PageRankOptions::default().iterations),
                        damping: options
                            .damping
                            .unwrap_or_else(|| PageRankOptions::default().damping),
                    })
                    .into_iter()
                    .filter(|score| {
                        node_visibility_filter
                            .as_ref()
                            .map(|filter| {
                                store
                                    .node(score.node)
                                    .map(|node| node_matches_property_filter(node, filter))
                                    .unwrap_or(false)
                            })
                            .unwrap_or(true)
                    })
                    .map(|score| Binding {
                        values: BTreeMap::from([
                            ("node".to_string(), Value::Int(score.node.0 as i64)),
                            (score_column.clone(), Value::Float(score.score)),
                        ]),
                        nodes: BTreeMap::new(),
                        relationships: BTreeMap::new(),
                    })
                    .collect(),
                GraphAlgorithmKind::Louvain => graph
                    .hierarchical_louvain_communities(LouvainOptions {
                        max_iterations: options
                            .max_iterations
                            .unwrap_or_else(|| LouvainOptions::default().max_iterations),
                        max_levels: options
                            .max_levels
                            .unwrap_or_else(|| LouvainOptions::default().max_levels),
                    })
                    .into_iter()
                    .filter(|assignment| {
                        node_visibility_filter
                            .as_ref()
                            .map(|filter| {
                                store
                                    .node(assignment.node)
                                    .map(|node| node_matches_property_filter(node, filter))
                                    .unwrap_or(false)
                            })
                            .unwrap_or(true)
                    })
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
                    })
                    .collect(),
            })
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
            Ok(output
                .rows
                .into_iter()
                .map(|row| {
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
                })
                .collect())
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
            let ids = store
                .scan_nodes(None)
                .filter(|node| node_matches_label_pattern(node, label_ids.as_deref()))
                .filter(|node| {
                    let binding = Binding {
                        values: BTreeMap::new(),
                        nodes: BTreeMap::from([(variable.clone(), (*node).clone())]),
                        relationships: BTreeMap::new(),
                    };
                    predicate
                        .as_ref()
                        .map(|predicate| evaluate_predicate(predicate, catalog, store, &binding))
                        .unwrap_or(true)
                })
                .map(|node| node.id)
                .collect::<Vec<_>>();
            let ids = store.set_node_properties_by_ids(catalog, &ids, &assignments)?;
            match returns {
                SetNodePropertiesReturnMode::Project(returns) => ids
                    .into_iter()
                    .map(|id| {
                        let node = store.node(id).cloned().ok_or_else(|| {
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
            let ids = store
                .scan_nodes_with_filter_pruning(label_id, candidate_filter.as_ref())
                .nodes
                .into_iter()
                .filter(|node| {
                    predicate
                        .as_ref()
                        .map(|predicate| {
                            let binding = Binding {
                                values: BTreeMap::new(),
                                nodes: BTreeMap::from([(variable.clone(), (*node).clone())]),
                                relationships: BTreeMap::new(),
                            };
                            evaluate_predicate(predicate, catalog, store, &binding)
                        })
                        .unwrap_or(true)
                })
                .map(|node| node.id)
                .collect::<Vec<_>>();
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
        ),
        PhysicalPlan::SourceSegmentScan {
            variable,
            predicate,
        } => execute_source_segment_scan(variable, predicate, catalog, store, execution_limit),
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left = execute_child_bindings(left, catalog, store, context)?;
            let right = execute_child_bindings(right, catalog, store, context)?;
            let mut output = Vec::new();
            for left_binding in &left {
                for right_binding in &right {
                    let mut values = left_binding.values.clone();
                    values.extend(right_binding.values.clone());
                    let mut nodes = left_binding.nodes.clone();
                    nodes.extend(right_binding.nodes.clone());
                    let mut relationships = left_binding.relationships.clone();
                    relationships.extend(right_binding.relationships.clone());
                    output.push(Binding {
                        values,
                        nodes,
                        relationships,
                    });
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
            let nodes = store
                .seek_nodes_by_property(label_id, property, value)
                .collect::<Vec<_>>();
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyEq {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: nodes.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(nodes.len()),
                candidate_count_before_filter: nodes.len(),
                output_count: nodes
                    .len()
                    .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                filtered_out_count: 0,
            });
            Ok(nodes
                .into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .cloned()
                .map(|node| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                })
                .collect())
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
            let mut nodes = Vec::new();
            for value in values {
                for node in store.seek_nodes_by_property(label_id, property, value) {
                    if seen.insert(node.id) {
                        nodes.push(node);
                    }
                }
            }
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyIn {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: nodes.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(nodes.len()),
                candidate_count_before_filter: nodes.len(),
                output_count: nodes
                    .len()
                    .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                filtered_out_count: 0,
            });
            Ok(nodes
                .into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .cloned()
                .map(|node| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                })
                .collect())
        }
        PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        } => {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Ok(store
                .seek_nodes_by_composite_property(label_id, predicates)
                .into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .cloned()
                .map(|node| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                })
                .collect())
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
            let nodes = store
                .seek_nodes_by_property_range(label_id, property, lower.as_ref(), upper.as_ref())
                .into_iter()
                .collect::<Vec<_>>();
            let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
            record_scan_pruning_report(ScanPruningReport {
                target_kind: crate::store::ScanPruningTargetKind::Node,
                label_id: Some(label_id),
                rel_type_id: None,
                strategy: ScanPruningStrategy::PropertyRange {
                    property: property.clone(),
                },
                pruned: true,
                exact_empty: nodes.is_empty(),
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning.saturating_sub(nodes.len()),
                candidate_count_before_filter: nodes.len(),
                output_count: nodes
                    .len()
                    .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                filtered_out_count: 0,
            });
            Ok(nodes
                .into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .cloned()
                .map(|node| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                })
                .collect())
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
            Ok(store
                .seek_nodes_by_full_text_property(label_id, property, query)
                .into_iter()
                .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                .cloned()
                .map(|node| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                })
                .collect())
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
                    let degree = one_hop_relationships(
                        store,
                        source.id,
                        rel_type_id,
                        target_label_ids.as_deref(),
                        rel_properties,
                        None,
                        *direction,
                    )
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
            let total = store
                .scan_nodes(None)
                .filter(|node| node_matches_label_pattern(node, label_ids.as_deref()))
                .filter(|node| node_properties_match(node, properties))
                .map(|node| {
                    legs.iter()
                        .map(|leg| relationship_count_sum_leg(catalog, store, node.id, leg))
                        .sum::<usize>()
                })
                .sum::<usize>();
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
        } => Ok(thread_repair_stats_rows(
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
        )),
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
                    if evaluate_predicate(predicate, catalog, store, &binding) {
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
                    if evaluate_predicate(predicate, catalog, store, &binding) {
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
                if evaluate_predicate(predicate, catalog, store, &binding) {
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
) -> Result<Vec<Binding>> {
    if let Some(label_id) = exact_scan_label_id(catalog, label) {
        let scan = store.scan_nodes_with_filter_pruning(label_id, filter.map(|(_, filter)| filter));
        record_scan_pruning_report(scan.report.clone());
        let mut output = Vec::new();
        for node in scan.nodes {
            let binding = Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(variable.to_string(), node.clone())]),
                relationships: BTreeMap::new(),
            };
            if filter
                .map(|(predicate, _)| evaluate_predicate(predicate, catalog, store, &binding))
                .unwrap_or(true)
            {
                output.push(binding);
                if execution_limit.is_reached(output.len()) {
                    return Ok(output);
                }
            }
        }
        return Ok(output);
    }

    let label_ids = label_ids_for_pattern(catalog, label);
    Ok(store
        .scan_nodes(None)
        .filter(|node| node_matches_label_pattern(node, label_ids.as_deref()))
        .take(execution_limit.output_rows.unwrap_or(usize::MAX))
        .cloned()
        .map(|node| Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        })
        .collect())
}

fn execute_source_segment_scan(
    variable: &str,
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    let Some(storage_predicate) = source_storage_scan_predicate(predicate, variable) else {
        return execute_node_scan_with_optional_filter(
            variable,
            "Source",
            None,
            catalog,
            store,
            execution_limit,
        );
    };
    let read = store.read_published_source_scan_candidates(
        &storage_predicate,
        NonZeroUsize::new(SOURCE_SEGMENT_SCAN_IO_DEPTH)
            .expect("source segment scan I/O depth is non-zero"),
        NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES)
            .expect("source segment scan coalesced range limit is non-zero"),
        NonZeroU64::new(SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES)
            .expect("source segment scan wave byte limit is non-zero"),
    );
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
        Ok(SourceScanCandidateRead::Fallback(_)) | Err(_) => {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
            );
        }
    };
    let source_label_id = catalog.label_id("Source");
    let mut bindings = Vec::new();
    for row in rows {
        let Some(node) = store.node(NodeId(row.node_id)) else {
            return execute_node_scan_with_optional_filter(
                variable,
                "Source",
                None,
                catalog,
                store,
                execution_limit,
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
            );
        }
        bindings.push(Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node.clone())]),
            relationships: BTreeMap::new(),
        });
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

fn execute_node_column_lookup(
    spec: NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    catalog: &Catalog,
    store: &GraphStore,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    if let Some(Some(label_id)) = exact_scan_label_id(catalog, spec.label) {
        return execute_indexed_node_column_lookup(&spec, input, label_id, store, execution_limit);
    }

    let label_ids = label_ids_for_pattern(catalog, spec.label);
    let candidates = store
        .scan_nodes(None)
        .filter(|node| node_matches_label_pattern(node, label_ids.as_deref()))
        .cloned()
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    for binding in input {
        let expected = binding.values.get(spec.column).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing column '{}' during node column lookup",
                spec.column
            ))
        })?;
        let mut matched = false;
        for node in &candidates {
            if node.properties.get(spec.property) == Some(expected) {
                let mut next = binding.clone();
                next.nodes.insert(spec.variable.to_string(), node.clone());
                output.push(next);
                matched = true;
                if execution_limit.is_reached(output.len()) {
                    return Ok(output);
                }
            }
        }
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            output.push(next);
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

    let mut nodes_by_value = BTreeMap::<Value, Vec<NodeRecord>>::new();
    let mut unique_candidate_ids = BTreeSet::new();
    for value in &lookup_values {
        let nodes = store
            .seek_nodes_by_property(label_id, spec.property, value)
            .cloned()
            .collect::<Vec<_>>();
        for node in &nodes {
            unique_candidate_ids.insert(node.id);
        }
        nodes_by_value.insert(value.clone(), nodes);
    }

    let mut output = Vec::new();
    for binding in input {
        let expected = binding
            .values
            .get(spec.column)
            .expect("lookup column was validated before index lookup");
        let mut matched = false;
        if let Some(nodes) = nodes_by_value.get(expected) {
            for node in nodes {
                let mut next = binding.clone();
                next.nodes.insert(spec.variable.to_string(), node.clone());
                output.push(next);
                matched = true;
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
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            output.push(next);
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
        let source = binding.nodes.get(source_variable).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing variable '{source_variable}' during expand"
            ))
        })?;
        let output_len_before = output.len();
        if rel_variable.is_some()
            || !rel_properties.is_empty()
            || filters.relationship_scan_filter.is_some()
            || *direction != RelationshipDirection::Outgoing
        {
            let bound_target_id = binding.nodes.get(target_variable).map(|node| node.id);
            for (relationship, target) in one_hop_relationships(
                store,
                source.id,
                rel_type_id,
                target_label_ids.as_deref(),
                rel_properties,
                filters.relationship_scan_filter,
                *direction,
            ) {
                if bound_target_id.is_some_and(|node_id| node_id != target.id) {
                    continue;
                }
                if filters
                    .target_scan_filter
                    .map(|filter| !node_matches_property_filter(target, filter))
                    .unwrap_or(false)
                {
                    continue;
                }
                let mut nodes = binding.nodes.clone();
                nodes.insert(target_variable.clone(), target.clone());
                let mut relationships = binding.relationships.clone();
                if let Some(rel_variable) = rel_variable {
                    relationships.insert(rel_variable.clone(), relationship.clone());
                }
                let candidate = Binding {
                    values: binding.values.clone(),
                    nodes,
                    relationships,
                };
                if !graph_expansion.try_push(&mut output, candidate, Some(target.id), 1) {
                    graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
                    return Ok(output);
                }
                if execution_limit.is_reached(output.len()) {
                    graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
                    return Ok(output);
                }
            }
        } else {
            let bound_target_id = binding.nodes.get(target_variable).map(|node| node.id);
            for (target, hop) in bounded_expand_targets(
                store,
                source.id,
                rel_type_id.expect("typed bounded expand checked by planner"),
                target_label_ids.as_deref(),
                *min_hops,
                *max_hops,
            ) {
                if bound_target_id.is_some_and(|node_id| node_id != target.id) {
                    continue;
                }
                if filters
                    .target_scan_filter
                    .map(|filter| !node_matches_property_filter(target, filter))
                    .unwrap_or(false)
                {
                    continue;
                }
                let mut nodes = binding.nodes.clone();
                nodes.insert(target_variable.clone(), target.clone());
                let candidate = Binding {
                    values: binding.values.clone(),
                    nodes,
                    relationships: binding.relationships.clone(),
                };
                if !graph_expansion.try_push(&mut output, candidate, Some(target.id), hop) {
                    graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
                    return Ok(output);
                }
                if execution_limit.is_reached(output.len()) {
                    graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
                    return Ok(output);
                }
            }
        }
        if *optional && output.len() == output_len_before {
            let mut nodes = binding.nodes.clone();
            nodes.insert(target_variable.clone(), null_lookup_node());
            let candidate = Binding {
                values: binding.values,
                nodes,
                relationships: binding.relationships,
            };
            if !graph_expansion.try_push(&mut output, candidate, None, 0) {
                graph_expansion.record(rel_type, *min_hops, *max_hops, output.len());
                return Ok(output);
            }
            if execution_limit.is_reached(output.len()) {
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
            output.push(candidate);
            return true;
        };
        if output.len() >= budget.candidate_limit {
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

fn map_payload_bytes(values: &BTreeMap<String, Value>) -> usize {
    values.iter().fold(0usize, |total, (name, value)| {
        total
            .saturating_add(name.len())
            .saturating_add(value_payload_bytes(value))
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
) -> Result<Vec<Binding>> {
    let Some(source) =
        find_node_by_id_property(catalog, store, input.source_label, input.source_id)
    else {
        return Ok(Vec::new());
    };
    let Some(target) =
        find_node_by_id_property(catalog, store, input.target_label, input.target_id)
    else {
        return Ok(Vec::new());
    };
    if input
        .source_visibility_filter
        .map(|filter| !node_matches_property_filter(source, filter))
        .unwrap_or(false)
        || input
            .target_visibility_filter
            .map(|filter| !node_matches_property_filter(target, filter))
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
    let paths = all_shortest_paths(
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
    );
    paths
        .iter()
        .map(|path| shortest_path_binding(store, path, input.returns))
        .collect()
}

fn find_node_by_id_property<'a>(
    catalog: &Catalog,
    store: &'a GraphStore,
    label: &str,
    id: &Value,
) -> Option<&'a NodeRecord> {
    if label.is_empty() {
        return store
            .scan_nodes(None)
            .find(|node| node.properties.get("id") == Some(id));
    }
    let label_id = catalog.label_id(label)?;
    store.seek_nodes_by_property(label_id, "id", id).next()
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

fn all_shortest_paths(store: &GraphStore, search: ShortestPathSearch<'_>) -> Vec<Vec<NodeId>> {
    let mut queue = VecDeque::from([vec![search.source]]);
    let mut results = Vec::new();
    let mut found_depth = None;
    while let Some(path) = queue.pop_front() {
        let depth = path.len() - 1;
        if found_depth.is_some_and(|found| depth >= found) || depth == search.max_hops {
            continue;
        }
        let current = *path.last().expect("path is never empty");
        for (_, next) in one_hop_relationships(
            store,
            current,
            search.rel_type_id,
            None,
            &BTreeMap::new(),
            None,
            search.direction,
        ) {
            if search
                .path_node_visibility_filter
                .map(|filter| !node_matches_property_filter(next, filter))
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
            if next.id == search.target && next_depth >= search.min_hops {
                found_depth = Some(next_depth);
                results.push(next_path);
            } else if found_depth.is_none() && next_depth < search.max_hops {
                queue.push_back(next_path);
            }
        }
    }
    results
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
                        store
                            .node(*node_id)
                            .and_then(|node| node.properties.get(property))
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
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

fn one_hop_relationships<'a>(
    store: &'a GraphStore,
    source: NodeId,
    rel_type_id: Option<crate::schema::RelTypeId>,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    rel_properties: &BTreeMap<String, Value>,
    relationship_scan_filter: Option<&PropertyFilter>,
    direction: RelationshipDirection,
) -> Vec<(&'a RelRecord, &'a NodeRecord)> {
    let mut matches = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let relationship_filter = combine_property_filters(
        property_filter_from_properties(rel_properties),
        relationship_scan_filter.cloned(),
    );
    if let Some(filter) = relationship_filter.as_ref() {
        let scan = store.scan_relationships_with_filter_pruning(rel_type_id, Some(filter));
        record_scan_pruning_report(scan.report.clone());
        collect_one_hop_relationships(
            scan.relationships.into_iter().filter(|relationship| {
                relationship_target_for_source_direction(relationship, source, direction).is_some()
            }),
            store,
            target_label_ids,
            rel_properties,
            |relationship| {
                relationship_target_for_source_direction(relationship, source, direction)
                    .expect("relationship direction was checked before target lookup")
            },
            &mut seen,
            &mut matches,
        );
        matches.sort_by_key(|(relationship, target)| (target.id, relationship.id));
        return matches;
    }

    if let Some(rel_type_id) = rel_type_id {
        if matches!(
            direction,
            RelationshipDirection::Outgoing | RelationshipDirection::Undirected
        ) {
            collect_ordered_one_hop_relationships(
                store.ordered_adjacency_entries(source, rel_type_id, AdjacencyDirection::Outgoing),
                store,
                target_label_ids,
                rel_properties,
                &mut seen,
                &mut matches,
            );
        }
        if matches!(
            direction,
            RelationshipDirection::Incoming | RelationshipDirection::Undirected
        ) {
            collect_ordered_one_hop_relationships(
                store.ordered_adjacency_entries(source, rel_type_id, AdjacencyDirection::Incoming),
                store,
                target_label_ids,
                rel_properties,
                &mut seen,
                &mut matches,
            );
        }
        matches.sort_by_key(|(relationship, target)| (target.id, relationship.id));
        return matches;
    }

    if matches!(
        direction,
        RelationshipDirection::Outgoing | RelationshipDirection::Undirected
    ) {
        collect_one_hop_relationships(
            store
                .scan_relationships(None)
                .filter(move |relationship| relationship.source == source),
            store,
            target_label_ids,
            rel_properties,
            |relationship| relationship.target,
            &mut seen,
            &mut matches,
        );
    }
    if matches!(
        direction,
        RelationshipDirection::Incoming | RelationshipDirection::Undirected
    ) {
        collect_one_hop_relationships(
            store
                .scan_relationships(None)
                .filter(move |relationship| relationship.target == source),
            store,
            target_label_ids,
            rel_properties,
            |relationship| relationship.source,
            &mut seen,
            &mut matches,
        );
    }
    matches.sort_by_key(|(relationship, target)| (target.id, relationship.id));
    matches
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
) -> usize {
    let rel_type_id = if leg.rel_type.is_empty() {
        None
    } else {
        catalog.rel_type_id(&leg.rel_type)
    };
    if !leg.rel_type.is_empty() && rel_type_id.is_none() {
        return 0;
    }
    one_hop_relationships(
        store,
        source,
        rel_type_id,
        None,
        &BTreeMap::new(),
        None,
        leg.direction,
    )
    .into_iter()
    .filter(|(relationship, _)| {
        relationship_count_filter_matches(relationship, leg.filter.as_ref())
    })
    .count()
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
) -> Vec<Binding> {
    let thread_label_ids = label_ids_for_pattern(catalog, label);
    let identity_label_ids = label_ids_for_pattern(catalog, identity_label);
    let message_label_ids = label_ids_for_pattern(catalog, message_label);
    let memory_label_ids = label_ids_for_pattern(catalog, memory_label);
    let message_rel_type_id = catalog.rel_type_id(message_rel_type);
    let memory_rel_type_id = catalog.rel_type_id(memory_rel_type);
    let identities = store
        .scan_nodes(None)
        .filter(|node| node_matches_label_pattern(node, identity_label_ids.as_deref()))
        .collect::<Vec<_>>();
    let mut threads = store
        .scan_nodes(None)
        .filter(|node| node_matches_label_pattern(node, thread_label_ids.as_deref()))
        .collect::<Vec<_>>();
    threads.sort_by(|left, right| {
        left.properties
            .get("id")
            .unwrap_or(&Value::Null)
            .cmp(right.properties.get("id").unwrap_or(&Value::Null))
    });
    threads
        .into_iter()
        .map(|thread| {
            let thread_id = thread
                .properties
                .get(thread_id_property)
                .cloned()
                .unwrap_or(Value::Null);
            let identity_refs = identities
                .iter()
                .filter(|identity| identity.properties.get(identity_ref_property) == Some(&thread_id))
                .count();
            let legacy_messages = message_rel_type_id
                .map(|rel_type_id| {
                    one_hop_relationships(
                        store,
                        thread.id,
                        Some(rel_type_id),
                        message_label_ids.as_deref(),
                        &BTreeMap::new(),
                        None,
                        RelationshipDirection::Outgoing,
                    )
                    .len()
                })
                .unwrap_or(0);
            let compacted_memories = memory_rel_type_id
                .map(|rel_type_id| {
                    one_hop_relationships(
                        store,
                        thread.id,
                        Some(rel_type_id),
                        memory_label_ids.as_deref(),
                        &BTreeMap::new(),
                        None,
                        RelationshipDirection::Outgoing,
                    )
                    .len()
                })
                .unwrap_or(0);
            let space_id = match thread.properties.get("space_id") {
                Some(Value::String(value)) if !value.is_empty() => Value::String(value.clone()),
                _ => Value::String("default".to_string()),
            };
            let message_count = match thread.properties.get("message_count") {
                Some(Value::Null) | None => Value::Int(0),
                Some(value) => value.clone(),
            };
            Binding {
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
            }
        })
        .collect()
}

fn collect_ordered_one_hop_relationships<'a>(
    entries: Vec<OrderedAdjacencyEntry>,
    store: &'a GraphStore,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    rel_properties: &BTreeMap<String, Value>,
    seen: &mut std::collections::BTreeSet<crate::store::RelId>,
    matches: &mut Vec<(&'a RelRecord, &'a NodeRecord)>,
) {
    for entry in entries {
        if !seen.insert(entry.relationship_id) {
            continue;
        }
        let Some(relationship) = store.relationship(entry.relationship_id) else {
            continue;
        };
        if !relationship_properties_match(relationship, rel_properties) {
            continue;
        }
        let Some(target) = store.node(entry.neighbor_id) else {
            continue;
        };
        if node_matches_label_pattern(target, target_label_ids) {
            matches.push((relationship, target));
        }
    }
}

fn collect_one_hop_relationships<'a>(
    relationships: impl Iterator<Item = &'a RelRecord>,
    store: &'a GraphStore,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    rel_properties: &BTreeMap<String, Value>,
    target_id: impl Fn(&RelRecord) -> NodeId,
    seen: &mut std::collections::BTreeSet<crate::store::RelId>,
    matches: &mut Vec<(&'a RelRecord, &'a NodeRecord)>,
) {
    for relationship in relationships {
        if !seen.insert(relationship.id) {
            continue;
        }
        if !relationship_properties_match(relationship, rel_properties) {
            continue;
        }
        let Some(target) = store.node(target_id(relationship)) else {
            continue;
        };
        if node_matches_label_pattern(target, target_label_ids) {
            matches.push((relationship, target));
        }
    }
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

fn bounded_expand_targets<'a>(
    store: &'a GraphStore,
    source: NodeId,
    rel_type_id: crate::schema::RelTypeId,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    min_hops: usize,
    max_hops: usize,
) -> Vec<(&'a NodeRecord, usize)> {
    let mut targets = Vec::new();
    BoundedExpand {
        store,
        rel_type_id,
        target_label_ids: target_label_ids.map(|label_ids| label_ids.to_vec()),
        min_hops,
        max_hops,
    }
    .collect(source, 0, &mut targets);
    targets
}

struct BoundedExpand<'a> {
    store: &'a GraphStore,
    rel_type_id: crate::schema::RelTypeId,
    target_label_ids: Option<Vec<crate::schema::LabelId>>,
    min_hops: usize,
    max_hops: usize,
}

impl<'a> BoundedExpand<'a> {
    fn collect(&self, current: NodeId, depth: usize, targets: &mut Vec<(&'a NodeRecord, usize)>) {
        if depth >= self.min_hops
            && let Some(node) = self.store.node(current)
            && node_matches_label_pattern(node, self.target_label_ids.as_deref())
        {
            targets.push((node, depth));
        }
        if depth == self.max_hops {
            return;
        }
        for entry in self.store.ordered_adjacency_entries(
            current,
            self.rel_type_id,
            AdjacencyDirection::Outgoing,
        ) {
            self.collect(entry.neighbor_id, depth + 1, targets);
        }
    }
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
) -> bool {
    match predicate {
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| evaluate_predicate(predicate, catalog, store, binding)),
        Predicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| evaluate_predicate(predicate, catalog, store, binding)),
        Predicate::Not(predicate) => !evaluate_predicate(predicate, catalog, store, binding),
        Predicate::ConstantBool(value) => *value,
        Predicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => relationship_exists(
            catalog,
            store,
            binding,
            variable,
            rel_type,
            *direction,
            target_label,
        ),
        Predicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => bound_relationship_exists(
            catalog,
            store,
            binding,
            source_variable,
            rel_type,
            *direction,
            target_variable,
        ),
        Predicate::IdEq { variable, value } => binding_id(binding, variable)
            .map(|actual| actual == *value)
            .unwrap_or(false),
        Predicate::IdNotEq { variable, value } => binding_id(binding, variable)
            .map(|actual| actual != *value)
            .unwrap_or(false),
        Predicate::IdCompare {
            variable,
            op,
            value,
        } => binding_id(binding, variable)
            .map(|actual| compare_property_values(&actual, *op, value))
            .unwrap_or(false),
        Predicate::IdIn { variable, values } => binding_id(binding, variable)
            .map(|actual| values.iter().any(|value| value == &actual))
            .unwrap_or(false),
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => binding_property(binding, variable, property)
            .map(|actual| actual == value)
            .unwrap_or(false),
        Predicate::PropertyNotEq {
            variable,
            property,
            value,
        } => binding_property(binding, variable, property)
            .map(|actual| actual != value)
            .unwrap_or(false),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => binding_property(binding, variable, property)
            .map(|actual| compare_property_values(actual, *op, value))
            .unwrap_or(false),
        Predicate::ExpressionEq { expression, value } => {
            match (
                predicate_expression_value(expression, catalog, binding),
                predicate_expression_value(value, catalog, binding),
            ) {
                (Some(actual), Some(expected)) => actual == expected,
                _ => false,
            }
        }
        Predicate::ExpressionNotEq { expression, value } => {
            match (
                predicate_expression_value(expression, catalog, binding),
                predicate_expression_value(value, catalog, binding),
            ) {
                (Some(actual), Some(expected)) => actual != expected,
                _ => false,
            }
        }
        Predicate::ExpressionCompare {
            expression,
            op,
            value,
        } => {
            match (
                predicate_expression_value(expression, catalog, binding),
                predicate_expression_value(value, catalog, binding),
            ) {
                (Some(actual), Some(expected)) => compare_property_values(&actual, *op, &expected),
                _ => false,
            }
        }
        Predicate::ExpressionContains { expression, value } => {
            match (
                predicate_expression_value(expression, catalog, binding),
                predicate_expression_value(value, catalog, binding),
            ) {
                (Some(Value::String(actual)), Some(Value::String(expected))) => {
                    actual.contains(&expected)
                }
                _ => false,
            }
        }
        Predicate::PropertyListContains {
            variable,
            property,
            value,
        } => binding_property(binding, variable, property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| actual == value)),
                _ => None,
            })
            .unwrap_or(false),
        Predicate::PropertyListContainsLower {
            variable,
            property,
            value,
        } => binding_property(binding, variable, property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| match actual {
                    Value::String(actual) => actual.to_lowercase().contains(value),
                    _ => false,
                })),
                _ => None,
            })
            .unwrap_or(false),
        Predicate::PropertyContains {
            variable,
            property,
            value,
        } => binding_property(binding, variable, property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.contains(value)),
                _ => None,
            })
            .unwrap_or(false),
        Predicate::PropertyStartsWith {
            variable,
            property,
            value,
        } => binding_property(binding, variable, property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.starts_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        Predicate::PropertyEndsWith {
            variable,
            property,
            value,
        } => binding_property(binding, variable, property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.ends_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        Predicate::PropertyRegexMatch {
            variable,
            property,
            pattern,
        } => binding_property(binding, variable, property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(pattern.is_match(actual)),
                _ => None,
            })
            .unwrap_or(false),
        Predicate::PropertyIsNull { variable, property } => {
            binding_property(binding, variable, property)
                .map(|actual| actual == &Value::Null)
                .unwrap_or(true)
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            binding_property(binding, variable, property)
                .map(|actual| actual != &Value::Null)
                .unwrap_or(false)
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => binding_property(binding, variable, property)
            .map(|actual| values.iter().any(|value| value == actual))
            .unwrap_or(false),
    }
}

fn relationship_exists(
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
    variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_label: &str,
) -> bool {
    let Some(source) = binding.nodes.get(variable) else {
        return false;
    };
    let rel_type_id = if rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
            return false;
        };
        Some(rel_type_id)
    };
    let target_label_ids = label_ids_for_pattern(catalog, target_label);
    !one_hop_relationships(
        store,
        source.id,
        rel_type_id,
        target_label_ids.as_deref(),
        &BTreeMap::new(),
        None,
        direction,
    )
    .is_empty()
}

fn bound_relationship_exists(
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
    source_variable: &str,
    rel_type: &str,
    direction: RelationshipDirection,
    target_variable: &str,
) -> bool {
    let (Some(source), Some(target)) = (
        binding.nodes.get(source_variable),
        binding.nodes.get(target_variable),
    ) else {
        return false;
    };
    let Some(rel_type_id) = catalog.rel_type_id(rel_type) else {
        return false;
    };
    match direction {
        RelationshipDirection::Outgoing => store
            .outgoing_relationships(source.id, rel_type_id)
            .any(|relationship| relationship.target == target.id),
        RelationshipDirection::Incoming => store
            .incoming_relationships(source.id, rel_type_id)
            .any(|relationship| relationship.source == target.id),
        RelationshipDirection::Undirected => {
            store
                .outgoing_relationships(source.id, rel_type_id)
                .any(|relationship| relationship.target == target.id)
                || store
                    .incoming_relationships(source.id, rel_type_id)
                    .any(|relationship| relationship.source == target.id)
        }
    }
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
        let mut context = ExecutionContext {
            parameters: &parameters,
            external: &mut external,
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
