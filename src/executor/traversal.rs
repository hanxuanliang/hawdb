//! Root facade wiring for storage-independent traversal operators.

use super::*;
use skein_executor::traversal as executor_traversal;

pub(super) use executor_traversal::ShortestPathExecInput;
#[cfg(test)]
pub(super) use executor_traversal::ShortestPathSearch;

pub(super) fn execute_shortest_path(
    catalog: &Catalog,
    store: &GraphStore,
    input: ShortestPathExecInput<'_>,
    memory: &ExecutionMemoryConfig,
    execution_limit: ExecutionLimit,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Binding>> {
    executor_traversal::execute_shortest_path(
        catalog,
        store,
        input,
        memory,
        execution_limit,
        task_context,
        &mut RootExecutionObserver,
    )
}

#[cfg(test)]
pub(super) fn all_shortest_paths(
    store: &GraphStore,
    search: ShortestPathSearch<'_>,
    memory_budget: NonZeroUsize,
    result_limit: usize,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<(Vec<Vec<NodeId>>, usize, usize)> {
    executor_traversal::all_shortest_paths(
        store,
        search,
        memory_budget,
        result_limit,
        task_context,
        &mut RootExecutionObserver,
    )
}

pub(super) fn one_hop_relationships(
    store: &GraphStore,
    source: NodeId,
    rel_type_id: Option<crate::schema::RelTypeId>,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    rel_properties: &BTreeMap<String, Value>,
    relationship_scan_filter: Option<&PropertyFilter>,
    direction: RelationshipDirection,
) -> Result<Vec<(RelRecord, NodeRecord)>> {
    executor_traversal::one_hop_relationships(
        store,
        source,
        rel_type_id,
        target_label_ids,
        rel_properties,
        relationship_scan_filter,
        direction,
        &mut RootExecutionObserver,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn one_hop_relationships_with_budget(
    store: &GraphStore,
    source: NodeId,
    rel_type_id: Option<crate::schema::RelTypeId>,
    target_label_ids: Option<&[crate::schema::LabelId]>,
    rel_properties: &BTreeMap<String, Value>,
    relationship_scan_filter: Option<&PropertyFilter>,
    direction: RelationshipDirection,
    memory_budget_bytes: usize,
) -> Result<Vec<(RelRecord, NodeRecord)>> {
    executor_traversal::one_hop_relationships_with_budget(
        store,
        source,
        rel_type_id,
        target_label_ids,
        rel_properties,
        relationship_scan_filter,
        direction,
        memory_budget_bytes,
        &mut RootExecutionObserver,
    )
}

pub(super) fn relationship_count_sum_leg(
    catalog: &Catalog,
    store: &GraphStore,
    source: NodeId,
    leg: &RelationshipCountLeg,
) -> Result<usize> {
    executor_traversal::relationship_count_sum_leg(
        catalog,
        store,
        source,
        leg,
        &mut RootExecutionObserver,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn thread_repair_stats_rows(
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
    executor_traversal::thread_repair_stats_rows(
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
        memory_budget,
        &mut RootExecutionObserver,
    )
}
