//! Root facade wiring for storage-independent expression evaluation.

use super::*;

pub(super) use skein_executor::expression::{
    compare_bindings, distinct_bindings, exact_relationship_scan_filter_from_predicate,
    execute_aggregate, insert_projected_value, node_scan_filter_from_predicate,
    predicate_references_only_variable, project_value, property_filter_from_predicate,
    relationship_filter_from_properties_and_predicate, sort_value,
};

pub(super) fn evaluate_predicate(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &GraphStore,
    binding: &Binding,
) -> Result<bool> {
    skein_executor::expression::evaluate_predicate(
        predicate,
        catalog,
        store,
        binding,
        &mut RootExecutionObserver,
    )
}
