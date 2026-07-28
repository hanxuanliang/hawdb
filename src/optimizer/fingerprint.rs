use super::PhysicalPlan;
use crate::cypher::RelationshipDirection;
use crate::planner::{
    AggregateFunction, AggregateTarget, Aggregation, ComparisonOp, GraphAlgorithmKind, Predicate,
    Projection, ProjectionExpression, RelationshipCountFilter, RelationshipCountLeg,
    RelationshipOnCreateValue, SetAssignment, SetNodePropertiesReturnMode, SetValue, SortDirection,
    SortItem, SortKey,
};
use crate::value::Value;
use std::collections::BTreeMap;
impl PhysicalPlan {
    pub fn fingerprint(&self) -> String {
        let mut output = String::new();
        self.write_fingerprint(&mut output);
        output
    }

    fn write_fingerprint(&self, output: &mut String) {
        match self {
            PhysicalPlan::CreateNodeLabel { label } => {
                output.push_str("CreateNodeLabel(");
                write_identifier(output, label);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipType { rel_type } => {
                output.push_str("CreateRelationshipType(");
                write_identifier(output, rel_type);
                output.push(')');
            }
            PhysicalPlan::CreateNodeTable { name } => {
                output.push_str("CreateNodeTable(");
                write_identifier(output, name);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipTable { name } => {
                output.push_str("CreateRelationshipTable(");
                write_identifier(output, name);
                output.push(')');
            }
            PhysicalPlan::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => {
                output.push_str("CreateProperty(");
                output.push_str(table_kind.as_str());
                output.push(':');
                write_identifier(output, table);
                output.push('.');
                write_identifier(output, property);
                output.push_str(value_type.fingerprint_suffix());
                output.push_str(if *nullable { ":nullable" } else { ":not_null" });
                output.push(')');
            }
            PhysicalPlan::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                output.push_str("AlterTableState(");
                output.push_str(table_kind.as_str());
                output.push(':');
                write_identifier(output, table);
                output.push(':');
                output.push_str(state.as_str());
                output.push(')');
            }
            PhysicalPlan::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                output.push_str("AlterPropertyState(");
                output.push_str(table_kind.as_str());
                output.push(':');
                write_identifier(output, table);
                output.push('.');
                write_identifier(output, property);
                output.push(':');
                output.push_str(state.as_str());
                output.push(')');
            }
            PhysicalPlan::CreateIndex { label, property } => {
                output.push_str("CreateIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateCompositeIndex { label, properties } => {
                output.push_str("CreateCompositeIndex(");
                write_identifier(output, label);
                output.push('(');
                write_identifier_list(output, properties);
                output.push(')');
            }
            PhysicalPlan::CreateRangeIndex { label, property } => {
                output.push_str("CreateRangeIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateFullTextIndex { label, property } => {
                output.push_str("CreateFullTextIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateUniqueConstraint { label, property } => {
                output.push_str("CreateUniqueConstraint(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
                output.push_str("CreateNodePropertyExistsConstraint(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
                output.push_str("CreateRelationshipUniqueConstraint(");
                write_identifier(output, rel_type);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                output.push_str("CreateRelationshipPropertyExistsConstraint(");
                write_identifier(output, rel_type);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => {
                output.push_str("ProjectGraph(");
                write_identifier(output, name);
                output.push_str(":labels=");
                write_identifier_list(output, node_labels);
                output.push_str(":rels=");
                write_identifier_list(output, rel_types);
                output.push(')');
            }
            PhysicalPlan::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
                score_column,
            } => {
                output.push_str("GraphAlgorithm(");
                output.push_str(match algorithm {
                    GraphAlgorithmKind::PageRank => "page_rank",
                    GraphAlgorithmKind::Louvain => "louvain",
                });
                output.push(':');
                write_identifier(output, graph_name);
                output.push_str(":damping=");
                if let Some(damping) = options.damping {
                    output.push_str(&damping.to_bits().to_string());
                }
                output.push_str(":iterations=");
                if let Some(iterations) = options.max_iterations {
                    output.push_str(&iterations.to_string());
                }
                output.push_str(":score=");
                write_identifier(output, score_column);
                output.push(')');
            }
            PhysicalPlan::CreateNode { label, properties } => {
                output.push_str("CreateNode(");
                write_identifier(output, label);
                output.push(',');
                write_properties(output, properties);
                output.push(')');
            }
            PhysicalPlan::MergeNode {
                label,
                match_properties,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            } => {
                output.push_str("MergeNode(");
                write_identifier(output, label);
                output.push(',');
                write_properties(output, match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(',');
                write_set_assignments(output, on_match_assignments);
                output.push(',');
                write_set_assignments(output, post_merge_assignments);
                output.push(')');
            }
            PhysicalPlan::MergeRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => {
                output.push_str("MergeRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
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
                output.push_str("MergeMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
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
                output.push_str("MergeRelationshipFromMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(")=>[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_relationship_on_create_properties(output, on_create_properties);
                output.push(']');
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
                output.push_str("MergeRelationshipToMatchedTarget(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, old_target_label);
                output.push(',');
                write_properties(output, old_target_properties);
                output.push_str("),new_target=(");
                write_identifier(output, new_target_label);
                output.push(',');
                write_properties(output, new_target_properties);
                output.push_str("),new_rel=[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(']');
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
                output.push_str("MergeRelationshipFromMatchedTarget(old_source=");
                write_identifier(output, old_source_label);
                output.push(',');
                write_properties(output, old_source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, old_target_label);
                output.push(',');
                write_properties(output, old_target_properties);
                output.push_str("),new_source=(");
                write_identifier(output, new_source_label);
                output.push(',');
                write_properties(output, new_source_properties);
                output.push_str("),new_rel=[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(']');
            }
            PhysicalPlan::SetNodeProperty {
                variable,
                label,
                predicate,
                property,
                value,
            } => {
                output.push_str("SetNodeProperty(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_identifier(output, property);
                output.push('=');
                write_set_value(output, value);
                output.push(')');
            }
            PhysicalPlan::CreateMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_properties,
            } => {
                output.push_str("CreateMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::SetNodeProperties {
                variable,
                label,
                predicate,
                assignments,
            } => {
                output.push_str("SetNodeProperties(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_set_assignments(output, assignments);
                output.push(')');
            }
            PhysicalPlan::SetNodePropertiesReturn {
                variable,
                label,
                predicate,
                assignments,
                returns,
            } => {
                output.push_str("SetNodePropertiesReturn(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_set_assignments(output, assignments);
                output.push_str(",returns=");
                write_set_return_mode(output, returns);
                output.push(')');
            }
            PhysicalPlan::SetRelationshipProperty {
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
            } => {
                output.push_str("SetRelationshipProperty(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push(',');
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
                output.push(')');
            }
            PhysicalPlan::SetRelationshipProperties {
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
            } => {
                output.push_str("SetRelationshipProperties(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push_str(",assignments=");
                for assignment in assignments {
                    write_identifier(output, &assignment.property);
                    output.push('=');
                    write_value(output, &assignment.value);
                    output.push(',');
                }
                output.push(')');
            }
            PhysicalPlan::DeleteNode {
                variable,
                label,
                predicate,
                detach,
            } => {
                output.push_str("DeleteNode(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push_str(",detach=");
                output.push_str(if *detach { "true" } else { "false" });
                output.push(')');
            }
            PhysicalPlan::DeleteRelationship {
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
            } => {
                output.push_str("DeleteRelationship(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push(')');
            }
            PhysicalPlan::DeleteRelationshipTargetNodes {
                source_variable,
                source_label,
                source_predicate,
                rel_type,
                rel_properties,
                target_variable,
                target_label,
                target_properties,
                detach,
            } => {
                output.push_str("DeleteRelationshipTargetNodes(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push(',');
                write_optional_predicate(output, source_predicate.as_ref());
                output.push_str("-[:");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(",detach=");
                output.push_str(if *detach { "true" } else { "false" });
                output.push(')');
            }
            PhysicalPlan::CreateRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => {
                output.push_str("CreateRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::SeqNodeScan { variable, label } => {
                output.push_str("SeqNodeScan(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(')');
            }
            PhysicalPlan::NodeCartesianProductExec { left, right } => {
                output.push_str("NodeCartesianProductExec(");
                left.write_fingerprint(output);
                output.push(',');
                right.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable,
                label,
                property,
                column,
                optional,
                input,
            } => {
                output.push_str("NodeColumnLookupExec(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push('=');
                write_identifier(output, column);
                output.push(',');
                output.push_str(if *optional { "optional" } else { "required" });
                output.push(',');
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::IndexNodeSeek {
                variable,
                label,
                property,
                value,
            } => {
                output.push_str("IndexNodeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
                output.push(')');
            }
            PhysicalPlan::IndexNodeMultiSeek {
                variable,
                label,
                property,
                values,
            } => {
                output.push_str("IndexNodeMultiSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(" IN [");
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_value(output, value);
                }
                output.push_str("])");
            }
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                label,
                predicates,
            } => {
                output.push_str("IndexNodeCompositeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('(');
                for (index, (property, value)) in predicates.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_identifier(output, property);
                    output.push('=');
                    write_value(output, value);
                }
                output.push(')');
            }
            PhysicalPlan::IndexNodeRangeSeek {
                variable,
                label,
                property,
                lower,
                upper,
            } => {
                output.push_str("IndexNodeRangeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(",lower=");
                write_optional_range_bound(output, lower.as_ref());
                output.push_str(",upper=");
                write_optional_range_bound(output, upper.as_ref());
                output.push(')');
            }
            PhysicalPlan::IndexNodeTextSeek {
                variable,
                label,
                property,
                query,
            } => {
                output.push_str("IndexNodeTextSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(" contains ");
                write_identifier(output, query);
                output.push(')');
            }
            PhysicalPlan::AdjacencyExpandExec {
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
                output.push_str("AdjacencyExpandExec(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                if let Some(rel_variable) = rel_variable {
                    write_identifier(output, rel_variable);
                    output.push(':');
                }
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push('*');
                output.push_str(&min_hops.to_string());
                output.push_str("..");
                output.push_str(&max_hops.to_string());
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push_str(",optional=");
                output.push_str(if *optional { "true" } else { "false" });
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
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
                output.push_str("OptionalDegreeExec(");
                write_identifier(output, source_variable);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(",alias=");
                write_identifier(output, alias);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::OptionalRelationshipCountSumExec {
                variable,
                label,
                properties,
                legs,
                output: projection,
            } => {
                output.push_str("OptionalRelationshipCountSumExec(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_properties(output, properties);
                output.push_str(",legs=[");
                for (index, leg) in legs.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_relationship_count_leg(output, leg);
                }
                output.push_str("],output=");
                write_identifier(output, projection);
                output.push(')');
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
                output.push_str("ThreadRepairStatsExec(");
                write_identifier(output, label);
                output.push_str(",identity=");
                write_identifier(output, identity_label);
                output.push('.');
                write_identifier(output, identity_ref_property);
                output.push('=');
                write_identifier(output, thread_id_property);
                output.push_str(",messages=");
                write_identifier(output, message_rel_type);
                output.push(':');
                write_identifier(output, message_label);
                output.push_str(",memories=");
                write_identifier(output, memory_rel_type);
                output.push(':');
                write_identifier(output, memory_label);
                output.push(')');
            }
            PhysicalPlan::ShortestPathExec {
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
            } => {
                output.push_str("ShortestPathExec(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str(",source_id=");
                write_value(output, source_id);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                write_identifier(output, rel_type);
                output.push('*');
                output.push_str(&min_hops.to_string());
                output.push_str("..");
                output.push_str(&max_hops.to_string());
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push_str(",target_id=");
                write_value(output, target_id);
                output.push_str(",returns=");
                for item in returns {
                    write_identifier(output, &item.name);
                    output.push(',');
                }
                output.push(')');
            }
            PhysicalPlan::FilterExec { predicate, input } => {
                output.push_str("FilterExec(");
                write_predicate(output, predicate);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::ProjectExec { items, input } => {
                output.push_str("ProjectExec(");
                write_projection_list(output, items);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::AggregateExec {
                group_keys,
                items,
                input,
            } => {
                output.push_str("AggregateExec(groups=");
                write_projection_list(output, group_keys);
                output.push_str(",aggs=");
                write_aggregation_list(output, items);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::DistinctExec { input } => {
                output.push_str("DistinctExec(input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::SortExec { items, input } => {
                output.push_str("SortExec(");
                write_sort_list(output, items);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::LimitExec {
                offset,
                limit,
                input,
            } => {
                output.push_str("LimitExec(offset=");
                output.push_str(&offset.to_string());
                output.push_str(",limit=");
                match limit {
                    Some(limit) => output.push_str(&limit.to_string()),
                    None => output.push_str("none"),
                }
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
        }
    }
}
fn write_identifier(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
}

fn write_identifier_list(output: &mut String, values: &[String]) {
    output.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, value);
    }
    output.push(']');
}

fn write_properties(output: &mut String, properties: &BTreeMap<String, Value>) {
    output.push('{');
    for (index, (key, value)) in properties.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, key);
        output.push('=');
        write_value(output, value);
    }
    output.push('}');
}

fn write_relationship_on_create_properties(
    output: &mut String,
    properties: &BTreeMap<String, RelationshipOnCreateValue>,
) {
    output.push('{');
    for (index, (key, value)) in properties.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, key);
        output.push('=');
        match value {
            RelationshipOnCreateValue::Value(value) => write_value(output, value),
            RelationshipOnCreateValue::MatchedRelationshipProperty { property } => {
                output.push_str("matched_rel.");
                write_identifier(output, property);
            }
        }
    }
    output.push('}');
}

fn write_value(output: &mut String, value: &Value) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "bool:true" } else { "bool:false" }),
        Value::Int(value) => {
            output.push_str("int:");
            output.push_str(&value.to_string());
        }
        Value::Float(value) => {
            output.push_str("float:");
            output.push_str(&value.to_bits().to_string());
        }
        Value::String(value) => {
            output.push_str("string:");
            write_identifier(output, value);
        }
        Value::List(values) => {
            output.push_str("list:[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push(']');
        }
        Value::Map(values) => {
            output.push_str("map:{");
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_identifier(output, key);
                output.push('=');
                write_value(output, value);
            }
            output.push('}');
        }
    }
}

fn write_set_value(output: &mut String, value: &SetValue) {
    match value {
        SetValue::Value(value) => write_value(output, value),
        SetValue::Coalesce { property, default } => {
            output.push_str("coalesce(");
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        SetValue::AddInt { property, amount } => {
            output.push_str("add_int(");
            write_identifier(output, property);
            output.push(',');
            output.push_str(&amount.to_string());
            output.push(')');
        }
        SetValue::DecrementFloorZero { property } => {
            output.push_str("dec_floor_zero(");
            write_identifier(output, property);
            output.push(')');
        }
        SetValue::PreserveNewerExisting {
            property,
            incoming,
            preserve,
        } => {
            output.push_str("preserve_newer(");
            write_identifier(output, property);
            output.push(',');
            write_value(output, incoming);
            output.push(',');
            output.push_str(&preserve.to_string());
            output.push(')');
        }
    }
}

fn write_set_assignments(output: &mut String, assignments: &[SetAssignment]) {
    output.push('[');
    for (index, assignment) in assignments.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, &assignment.property);
        output.push('=');
        write_set_value(output, &assignment.value);
    }
    output.push(']');
}

fn write_optional_predicate(output: &mut String, predicate: Option<&Predicate>) {
    match predicate {
        Some(predicate) => write_predicate(output, predicate),
        None => output.push_str("none"),
    }
}

fn write_optional_range_bound(output: &mut String, bound: Option<&(Value, bool)>) {
    match bound {
        Some((value, inclusive)) => {
            output.push_str(if *inclusive {
                "inclusive:"
            } else {
                "exclusive:"
            });
            write_value(output, value);
        }
        None => output.push_str("none"),
    }
}

fn write_predicate(output: &mut String, predicate: &Predicate) {
    match predicate {
        Predicate::And(predicates) => {
            output.push_str("And(");
            for (index, predicate) in predicates.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_predicate(output, predicate);
            }
            output.push(')');
        }
        Predicate::Or(predicates) => {
            output.push_str("Or(");
            for (index, predicate) in predicates.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_predicate(output, predicate);
            }
            output.push(')');
        }
        Predicate::Not(predicate) => {
            output.push_str("Not(");
            write_predicate(output, predicate);
            output.push(')');
        }
        Predicate::ConstantBool(value) => {
            output.push_str(if *value { "True" } else { "False" });
        }
        Predicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => {
            output.push_str("RelationshipExists(");
            write_identifier(output, variable);
            output.push(',');
            write_identifier(output, rel_type);
            output.push(',');
            output.push_str(match direction {
                crate::cypher::RelationshipDirection::Outgoing => "out",
                crate::cypher::RelationshipDirection::Incoming => "in",
                crate::cypher::RelationshipDirection::Undirected => "both",
            });
            output.push(',');
            write_identifier(output, target_label);
            output.push(')');
        }
        Predicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => {
            output.push_str("BoundRelationshipExists(");
            write_identifier(output, source_variable);
            output.push(',');
            write_identifier(output, rel_type);
            output.push(',');
            output.push_str(match direction {
                crate::cypher::RelationshipDirection::Outgoing => "out",
                crate::cypher::RelationshipDirection::Incoming => "in",
                crate::cypher::RelationshipDirection::Undirected => "both",
            });
            output.push(',');
            write_identifier(output, target_variable);
            output.push(')');
        }
        Predicate::IdEq { variable, value } => {
            output.push_str("IdEq(id(");
            write_identifier(output, variable);
            output.push_str(")=");
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdNotEq { variable, value } => {
            output.push_str("IdNotEq(id(");
            write_identifier(output, variable);
            output.push_str(")<>");
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdCompare {
            variable,
            op,
            value,
        } => {
            output.push_str("IdCompare(id(");
            write_identifier(output, variable);
            output.push(')');
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdIn { variable, values } => {
            output.push_str("IdIn(id(");
            write_identifier(output, variable);
            output.push_str(") in [");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push_str("])");
        }
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyEq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push('=');
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyNotEq {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyNotEq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str("<>");
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => {
            output.push_str("PropertyCompare(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_value(output, value);
            output.push(')');
        }
        Predicate::ExpressionEq { expression, value } => {
            output.push_str("ExpressionEq(");
            write_projection_expression(output, expression);
            output.push('=');
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionNotEq { expression, value } => {
            output.push_str("ExpressionNotEq(");
            write_projection_expression(output, expression);
            output.push_str("<>");
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionCompare {
            expression,
            op,
            value,
        } => {
            output.push_str("ExpressionCompare(");
            write_projection_expression(output, expression);
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionContains { expression, value } => {
            output.push_str("ExpressionContains(");
            write_projection_expression(output, expression);
            output.push_str(" contains ");
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::PropertyListContains {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyListContains(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" contains ");
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyContains {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyContains(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" contains ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyStartsWith {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyStartsWith(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" starts_with ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyEndsWith {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyEndsWith(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" ends_with ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyRegexMatch {
            variable,
            property,
            pattern,
        } => {
            output.push_str("PropertyRegexMatch(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" =~ ");
            write_identifier(output, pattern);
            output.push(')');
        }
        Predicate::PropertyIsNull { variable, property } => {
            output.push_str("PropertyIsNull(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            output.push_str("PropertyIsNotNull(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => {
            output.push_str("PropertyIn(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" in [");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push_str("])");
        }
    }
}

fn write_projection_list(output: &mut String, items: &[Projection]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_projection(output, item);
    }
    output.push(']');
}

fn write_set_return_mode(output: &mut String, returns: &SetNodePropertiesReturnMode) {
    match returns {
        SetNodePropertiesReturnMode::Project(items) => write_projection_list(output, items),
        SetNodePropertiesReturnMode::Count { name } => {
            output.push_str("Count(");
            write_identifier(output, name);
            output.push(')');
        }
    }
}

fn write_relationship_count_leg(output: &mut String, leg: &RelationshipCountLeg) {
    output.push_str("RelationshipCountLeg(");
    match leg.direction {
        RelationshipDirection::Incoming => output.push_str("in,"),
        RelationshipDirection::Outgoing => output.push_str("out,"),
        RelationshipDirection::Undirected => output.push_str("both,"),
    }
    write_identifier(output, &leg.rel_type);
    output.push_str(",distinct=");
    output.push_str(if leg.distinct { "true" } else { "false" });
    if let Some(filter) = &leg.filter {
        output.push_str(",filter=");
        match filter {
            RelationshipCountFilter::PropertyNotEqOrEmpty { property, value } => {
                output.push_str("not_eq_or_empty(");
                write_identifier(output, property);
                output.push(',');
                write_value(output, value);
                output.push(')');
            }
        }
    }
    output.push(')');
}

fn write_projection(output: &mut String, item: &Projection) {
    output.push_str("Projection(");
    write_projection_expression(output, &item.expression);
    output.push_str(" as ");
    write_identifier(output, &item.name);
    output.push(')');
}

pub(super) fn write_projection_expression(output: &mut String, expression: &ProjectionExpression) {
    match expression {
        ProjectionExpression::Variable { variable } => {
            write_identifier(output, variable);
        }
        ProjectionExpression::Property { variable, property } => {
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
        }
        ProjectionExpression::Id { variable } => {
            output.push_str("id(");
            write_identifier(output, variable);
            output.push(')');
        }
        ProjectionExpression::RelationshipType { variable } => {
            output.push_str("label(");
            write_identifier(output, variable);
            output.push(')');
        }
        ProjectionExpression::Literal(value) => {
            write_value(output, value);
        }
        ProjectionExpression::Coalesce(expressions) => {
            output.push_str("coalesce(");
            for (index, expression) in expressions.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_projection_expression(output, expression);
            }
            output.push(')');
        }
        ProjectionExpression::Left { expression, length } => {
            output.push_str("left(");
            write_projection_expression(output, expression);
            output.push(',');
            output.push_str(&length.to_string());
            output.push(')');
        }
        ProjectionExpression::Lower(expression) => {
            output.push_str("lower(");
            write_projection_expression(output, expression);
            output.push(')');
        }
        ProjectionExpression::DatePart {
            part,
            variable,
            property,
        } => {
            output.push_str("date_part(");
            output.push_str(match part {
                crate::planner::DatePart::Year => "year",
                crate::planner::DatePart::Month => "month",
            });
            output.push(',');
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        ProjectionExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            output.push_str("default_if_null_or_eq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            output.push_str("default_if_null(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            output.push_str("case_property_not_null_or_eq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, non_empty);
            output.push(',');
            write_value(output, null_or_empty);
            output.push(')');
        }
        ProjectionExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            output.push_str("case_property_equals_rank(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            for (candidate, rank) in branches {
                output.push(',');
                write_value(output, candidate);
                output.push_str("=>");
                write_value(output, rank);
            }
            output.push_str(",default=>");
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            output.push_str("case_lower_property_default(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            output.push_str("case_coalesce_difference_floor_zero(");
            for (index, term) in terms.iter().enumerate() {
                if index > 0 {
                    output.push('-');
                }
                output.push_str("coalesce(");
                write_identifier(output, variable);
                output.push('.');
                write_identifier(output, &term.property);
                output.push(',');
                write_value(output, &term.default);
                output.push(')');
            }
            output.push(')');
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            output.push_str("case_entity_search_rank(");
            write_identifier(output, &expression.variable);
            output.push('.');
            write_identifier(output, &expression.name_property);
            output.push(',');
            write_identifier(output, &expression.variable);
            output.push('.');
            write_identifier(output, &expression.aliases_property);
            output.push(',');
            write_value(output, &expression.raw_query);
            output.push(',');
            write_value(output, &expression.normalized_query);
            output.push(',');
            write_value(output, &expression.raw_input);
            output.push(',');
            write_value(output, &expression.exact_rank);
            output.push(',');
            write_value(output, &expression.alias_rank);
            output.push(',');
            write_value(output, &expression.fallback_rank);
            output.push(')');
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            output.push_str("case_column_search_rank(");
            write_identifier(output, &expression.column);
            output.push(',');
            write_value(output, &expression.raw_query);
            output.push(',');
            write_value(output, &expression.normalized_query);
            output.push(',');
            write_value(output, &expression.exact_rank);
            output.push(',');
            write_value(output, &expression.contains_rank);
            output.push(',');
            write_value(output, &expression.fallback_rank);
            output.push(')');
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column,
            property,
            empty,
            default,
        } => {
            output.push_str("column_default_if_null_or_eq(");
            write_identifier(output, column);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::ColumnValueDefaultIfNull { column, default } => {
            output.push_str("column_value_default_if_null(");
            write_identifier(output, column);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column,
            empty,
            non_empty,
            null_or_empty,
        } => {
            output.push_str("column_value_case_property_not_null_or_eq(");
            write_identifier(output, column);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, non_empty);
            output.push(',');
            write_value(output, null_or_empty);
            output.push(')');
        }
        ProjectionExpression::Column(name) => {
            output.push_str("column(");
            write_identifier(output, name);
            output.push(')');
        }
        ProjectionExpression::ColumnProperty { column, property } => {
            output.push_str("column_property(");
            write_identifier(output, column);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
    }
}

fn write_aggregation_list(output: &mut String, items: &[Aggregation]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_aggregation(output, item);
    }
    output.push(']');
}

fn write_aggregation(output: &mut String, item: &Aggregation) {
    output.push_str("Aggregation(");
    match item.function {
        AggregateFunction::Count => output.push_str("count"),
        AggregateFunction::Min => output.push_str("min"),
        AggregateFunction::Max => output.push_str("max"),
        AggregateFunction::Avg => output.push_str("avg"),
        AggregateFunction::Collect => output.push_str("collect"),
    }
    output.push('(');
    if item.distinct {
        output.push_str("distinct ");
    }
    match &item.target {
        AggregateTarget::All => output.push('*'),
        AggregateTarget::Variable(variable) => write_identifier(output, variable),
        AggregateTarget::Property { variable, property } => {
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
        }
    }
    output.push_str(") as ");
    write_identifier(output, &item.name);
    output.push(')');
}

fn write_sort_list(output: &mut String, items: &[SortItem]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str("Sort(");
        match &item.key {
            SortKey::Property { variable, property } => {
                write_identifier(output, variable);
                output.push('.');
                write_identifier(output, property);
            }
            SortKey::Id { variable } => {
                output.push_str("id(");
                write_identifier(output, variable);
                output.push(')');
            }
            SortKey::Expression(expression) => write_projection_expression(output, expression),
            SortKey::Column(column) => write_identifier(output, column),
        }
        output.push(' ');
        match item.direction {
            SortDirection::Asc => output.push_str("asc"),
            SortDirection::Desc => output.push_str("desc"),
        }
        output.push(')');
    }
    output.push(']');
}
