use super::{truncate, Catalog, GraphRagCommonPathSummary, GraphRagRouteSummary, GraphStatistics};
use std::collections::BTreeSet;

pub(super) fn route_summaries(
    catalog: &Catalog,
    statistics: &GraphStatistics,
    selected_labels: &BTreeSet<String>,
    selected_relationship_types: &BTreeSet<String>,
    max_routes: usize,
) -> (Vec<GraphRagRouteSummary>, bool) {
    let mut routes = statistics
        .path_counts
        .iter()
        .filter_map(
            |((source_label_id, rel_type_id, target_label_id), observed_count)| {
                let source_label = catalog.label_name(*source_label_id)?;
                let relationship_type = catalog.rel_type_name(*rel_type_id)?;
                let target_label = catalog.label_name(*target_label_id)?;
                if !selected_labels.contains(source_label)
                    || !selected_labels.contains(target_label)
                    || !selected_relationship_types.contains(relationship_type)
                {
                    return None;
                }
                let key = (*source_label_id, *rel_type_id, *target_label_id);
                Some(GraphRagRouteSummary {
                    source_label: source_label.to_string(),
                    relationship_type: relationship_type.to_string(),
                    target_label: target_label.to_string(),
                    observed_count: *observed_count,
                    distinct_source_count: statistics
                        .path_source_distinct_counts
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    distinct_target_count: statistics
                        .path_target_distinct_counts
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                })
            },
        )
        .collect::<Vec<_>>();
    routes.sort_by(|left, right| {
        right
            .observed_count
            .cmp(&left.observed_count)
            .then_with(|| left.source_label.cmp(&right.source_label))
            .then_with(|| left.relationship_type.cmp(&right.relationship_type))
            .then_with(|| left.target_label.cmp(&right.target_label))
    });
    let truncated = truncate(&mut routes, max_routes);
    (routes, truncated)
}

pub(super) fn common_path_summaries(
    catalog: &Catalog,
    statistics: &GraphStatistics,
    selected_labels: &BTreeSet<String>,
    selected_relationship_types: &BTreeSet<String>,
    max_common_paths: usize,
) -> (Vec<GraphRagCommonPathSummary>, bool) {
    let mut paths = statistics
        .bounded_path_counts
        .iter()
        .filter_map(
            |((source_label_id, rel_type_id, target_label_id, hops), observed_count)| {
                let source_label = catalog.label_name(*source_label_id)?;
                let relationship_type = catalog.rel_type_name(*rel_type_id)?;
                let target_label = catalog.label_name(*target_label_id)?;
                if *hops != 2
                    || !selected_labels.contains(source_label)
                    || !selected_labels.contains(target_label)
                    || !selected_relationship_types.contains(relationship_type)
                {
                    return None;
                }
                let key = (*source_label_id, *rel_type_id, *target_label_id, *hops);
                Some(GraphRagCommonPathSummary {
                    source_label: source_label.to_string(),
                    relationship_type: relationship_type.to_string(),
                    target_label: target_label.to_string(),
                    hops: *hops,
                    observed_count: *observed_count,
                    distinct_source_count: statistics
                        .bounded_path_source_distinct_counts
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    distinct_target_count: statistics
                        .bounded_path_target_distinct_counts
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                })
            },
        )
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        right
            .observed_count
            .cmp(&left.observed_count)
            .then_with(|| left.hops.cmp(&right.hops))
            .then_with(|| left.source_label.cmp(&right.source_label))
            .then_with(|| left.relationship_type.cmp(&right.relationship_type))
            .then_with(|| left.target_label.cmp(&right.target_label))
    });
    let truncated = truncate(&mut paths, max_common_paths);
    (paths, truncated)
}
