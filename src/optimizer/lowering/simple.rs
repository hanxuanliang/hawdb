use super::super::PhysicalPlan;
use crate::planner::LogicalPlan;

pub(super) fn lower_simple_logical(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::CreateNodeLabel { label } => Some(PhysicalPlan::CreateNodeLabel {
            label: label.clone(),
        }),
        LogicalPlan::CreateRelationshipType { rel_type } => {
            Some(PhysicalPlan::CreateRelationshipType {
                rel_type: rel_type.clone(),
            })
        }
        LogicalPlan::CreateNodeTable { name } => {
            Some(PhysicalPlan::CreateNodeTable { name: name.clone() })
        }
        LogicalPlan::CreateRelationshipTable { name } => {
            Some(PhysicalPlan::CreateRelationshipTable { name: name.clone() })
        }
        LogicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => Some(PhysicalPlan::CreateProperty {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            value_type: *value_type,
            nullable: *nullable,
        }),
        LogicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => Some(PhysicalPlan::AlterTableState {
            table_kind: *table_kind,
            table: table.clone(),
            state: *state,
        }),
        LogicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => Some(PhysicalPlan::AlterPropertyState {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            state: *state,
        }),
        LogicalPlan::CreateIndex { label, property } => Some(PhysicalPlan::CreateIndex {
            label: label.clone(),
            property: property.clone(),
        }),
        LogicalPlan::CreateCompositeIndex { label, properties } => {
            Some(PhysicalPlan::CreateCompositeIndex {
                label: label.clone(),
                properties: properties.clone(),
            })
        }
        LogicalPlan::CreateRangeIndex { label, property } => Some(PhysicalPlan::CreateRangeIndex {
            label: label.clone(),
            property: property.clone(),
        }),
        LogicalPlan::CreateFullTextIndex { label, property } => {
            Some(PhysicalPlan::CreateFullTextIndex {
                label: label.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateUniqueConstraint { label, property } => {
            Some(PhysicalPlan::CreateUniqueConstraint {
                label: label.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            Some(PhysicalPlan::CreateNodePropertyExistsConstraint {
                label: label.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            Some(PhysicalPlan::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            Some(PhysicalPlan::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => Some(PhysicalPlan::ProjectGraph {
            name: name.clone(),
            node_labels: node_labels.clone(),
            rel_types: rel_types.clone(),
        }),
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
        } => Some(PhysicalPlan::GraphAlgorithm {
            algorithm: *algorithm,
            graph_name: graph_name.clone(),
            options: *options,
            score_column: score_column.clone(),
        }),
        LogicalPlan::CreateNode { label, properties } => Some(PhysicalPlan::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        }),
        LogicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => Some(PhysicalPlan::MergeNode {
            label: label.clone(),
            match_properties: match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
            on_match_assignments: on_match_assignments.clone(),
            post_merge_assignments: post_merge_assignments.clone(),
        }),
        LogicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => Some(PhysicalPlan::MergeRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        }),
        LogicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => Some(PhysicalPlan::MergeMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            rel_type: rel_type.clone(),
            rel_match_properties: rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        }),
        LogicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label,
            source_properties,
            old_rel_variable: _,
            old_rel_type,
            old_rel_properties,
            target_label,
            target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => Some(PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        }),
        LogicalPlan::MergeRelationshipToMatchedTarget {
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
        } => Some(PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            old_target_label: old_target_label.clone(),
            old_target_properties: old_target_properties.clone(),
            new_target_label: new_target_label.clone(),
            new_target_properties: new_target_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        }),
        LogicalPlan::MergeRelationshipFromMatchedTarget {
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
        } => Some(PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label: old_source_label.clone(),
            old_source_properties: old_source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            old_target_label: old_target_label.clone(),
            old_target_properties: old_target_properties.clone(),
            new_source_label: new_source_label.clone(),
            new_source_properties: new_source_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        }),
        LogicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => Some(PhysicalPlan::CreateMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
        }),
        LogicalPlan::SetNodeProperty {
            variable,
            label,
            predicate,
            property,
            value,
        } => Some(PhysicalPlan::SetNodeProperty {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            property: property.clone(),
            value: value.clone(),
        }),
        LogicalPlan::SetNodeProperties {
            variable,
            label,
            predicate,
            assignments,
        } => Some(PhysicalPlan::SetNodeProperties {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            assignments: assignments.clone(),
        }),
        LogicalPlan::SetNodePropertiesReturn {
            variable,
            label,
            predicate,
            assignments,
            returns,
        } => Some(PhysicalPlan::SetNodePropertiesReturn {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            assignments: assignments.clone(),
            returns: returns.clone(),
        }),
        LogicalPlan::SetRelationshipProperty {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
            property,
            value,
        } => Some(PhysicalPlan::SetRelationshipProperty {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            property: property.clone(),
            value: value.clone(),
        }),
        LogicalPlan::SetRelationshipProperties {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
            assignments,
        } => Some(PhysicalPlan::SetRelationshipProperties {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            assignments: assignments.clone(),
        }),
        LogicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => Some(PhysicalPlan::DeleteNode {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            detach: *detach,
        }),
        LogicalPlan::DeleteRelationship {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
        } => Some(PhysicalPlan::DeleteRelationship {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        }),
        LogicalPlan::DeleteRelationshipTargetNodes {
            source_variable,
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_variable,
            target_label,
            target_properties,
            detach,
        } => Some(PhysicalPlan::DeleteRelationshipTargetNodes {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_predicate: source_predicate.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            detach: *detach,
        }),
        LogicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => Some(PhysicalPlan::CreateRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        }),
        LogicalPlan::NodeScan { variable, label } => Some(PhysicalPlan::SeqNodeScan {
            variable: variable.clone(),
            label: label.clone(),
        }),
        LogicalPlan::ThreadRepairStats {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => Some(PhysicalPlan::ThreadRepairStatsExec {
            label: label.clone(),
            identity_label: identity_label.clone(),
            identity_ref_property: identity_ref_property.clone(),
            thread_id_property: thread_id_property.clone(),
            message_rel_type: message_rel_type.clone(),
            message_label: message_label.clone(),
            memory_rel_type: memory_rel_type.clone(),
            memory_label: memory_label.clone(),
        }),
        LogicalPlan::ShortestPath {
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
        } => Some(PhysicalPlan::ShortestPathExec {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_id: source_id.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_id: target_id.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            returns: returns.clone(),
        }),
        _ => None,
    }
}
