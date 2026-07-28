use super::PhysicalPlan;
use crate::{
    AggregateFunction, AggregateTarget, Aggregation, ComparisonOp, GraphAlgorithmKind, Predicate,
    Projection, ProjectionExpression, RelationshipCountFilter, RelationshipCountLeg,
    RelationshipOnCreateValue, SetAssignment, SetNodePropertiesReturnMode, SetValue, SortDirection,
    SortItem, SortKey,
};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use std::collections::BTreeMap;

mod common;
mod predicate;
mod projection;

use common::*;
use predicate::*;
pub use projection::write_projection_expression;
use projection::*;

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
