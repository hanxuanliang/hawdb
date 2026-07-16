use std::collections::BTreeSet;

use crate::error::Result;

use super::super::ast::*;
use super::Parser;

struct BoundRelationshipMergePattern {
    source_variable: String,
    rel_variable: Option<String>,
    rel_type: String,
    rel_properties: std::collections::BTreeMap<String, ValueExpression>,
    target_variable: String,
}

struct ParsedWithClause {
    optional_with: Option<OptionalWithAggregate>,
    collect_with: Option<WithCollect>,
    distinct_with: Option<WithDistinctProjection>,
    aggregate_with: Option<WithAggregateProjection>,
}

impl Parser<'_> {
    pub(super) fn parse_match_statement(&mut self) -> Result<Statement> {
        let path_variable = self.consume_match_path_binding_prefix();
        let (variable, label, properties) = self.parse_match_node_pattern()?;
        if let Some(path_variable) = path_variable.as_ref() {
            if self.next_relationship_pattern_is_all_shortest() {
                return self.parse_shortest_path_return(
                    path_variable.clone(),
                    variable,
                    label,
                    properties,
                );
            }
        }
        if self.consume_char(',') || self.consume_keyword("MATCH") {
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            let predicate = if self.consume_keyword("WHERE") {
                Some(self.parse_property_predicate()?)
            } else {
                None
            };
            if self.consume_keyword("RETURN") {
                let returns = self.parse_return_items()?;
                let limit = if self.consume_keyword("LIMIT") {
                    Some(self.parse_value()?)
                } else {
                    None
                };
                return Ok(Statement::MatchNodesReturn(MatchNodesReturn {
                    left_variable: variable,
                    left_label: label,
                    left_properties: properties,
                    right_variable: target_variable,
                    right_label: target_label,
                    right_properties: target_properties,
                    predicate,
                    returns,
                    limit,
                }));
            }
            if self.consume_keyword("MERGE") {
                let merge_pattern = self.parse_bound_relationship_merge_pattern()?;
                let on_create_sets = if self.consume_keyword("ON") {
                    self.expect_keyword("CREATE")?;
                    self.expect_keyword("SET")?;
                    self.parse_set_properties()?
                } else {
                    Vec::new()
                };
                return Ok(Statement::MatchMergeRelationship(MatchMergeRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    target_variable,
                    target_label,
                    target_properties,
                    predicate,
                    merge_source_variable: merge_pattern.source_variable,
                    rel_variable: merge_pattern.rel_variable,
                    rel_type: merge_pattern.rel_type,
                    rel_properties: merge_pattern.rel_properties,
                    merge_target_variable: merge_pattern.target_variable,
                    on_create_sets,
                }));
            }
            self.expect_keyword("CREATE")?;
            let (create_source_variable, rel_type, rel_properties, create_target_variable) =
                self.parse_bound_relationship_create_pattern()?;
            return Ok(Statement::MatchCreateRelationship(
                MatchCreateRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    target_variable,
                    target_label,
                    target_properties,
                    predicate,
                    create_source_variable,
                    rel_type,
                    rel_properties,
                    create_target_variable,
                },
            ));
        }
        let expand = if self.consume_char('<') {
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            self.expect_char('-')?;
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            Some(RelationshipExpand {
                variable: rel_variable,
                rel_type,
                properties,
                direction: RelationshipDirection::Incoming,
                target_variable,
                target_label,
                target_properties,
                min_hops,
                max_hops,
            })
        } else if self.consume_char('-') {
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            Some(RelationshipExpand {
                variable: rel_variable,
                rel_type,
                properties,
                direction,
                target_variable,
                target_label,
                target_properties,
                min_hops,
                max_hops,
            })
        } else {
            None
        };
        let mut predicate = if self.consume_keyword("WHERE") {
            Some(self.parse_property_predicate()?)
        } else {
            None
        };
        if self.consume_keyword("MERGE") {
            let Some(expand) = expand else {
                return Err(self.error("MATCH MERGE requires a bound relationship pattern"));
            };
            let merge_pattern = self.parse_bound_relationship_merge_pattern()?;
            let on_create_sets = if self.consume_keyword("ON") {
                self.expect_keyword("CREATE")?;
                self.expect_keyword("SET")?;
                self.parse_set_properties()?
            } else {
                Vec::new()
            };
            return Ok(Statement::MatchExpandMergeRelationship(
                MatchExpandMergeRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    expand,
                    predicate,
                    merge_source_variable: merge_pattern.source_variable,
                    rel_variable: merge_pattern.rel_variable,
                    rel_type: merge_pattern.rel_type,
                    rel_properties: merge_pattern.rel_properties,
                    merge_target_variable: merge_pattern.target_variable,
                    on_create_sets,
                },
            ));
        }
        if self.consume_keyword("MATCH") {
            let (matched_target_variable, matched_target_label, matched_target_properties) =
                self.parse_match_node_pattern()?;
            let post_match_expand = if self.peek_char() == Some('<') {
                self.expect_char('<')?;
                self.expect_char('-')?;
                let (rel_variable, rel_type, properties, min_hops, max_hops) =
                    self.parse_match_relationship_pattern()?;
                self.expect_char('-')?;
                let (target_variable, target_label, target_properties) =
                    self.parse_match_node_pattern()?;
                Some(PostMatchRelationshipExpand {
                    source_variable: matched_target_variable.clone(),
                    source_label: matched_target_label.clone(),
                    source_properties: matched_target_properties.clone(),
                    expand: RelationshipExpand {
                        variable: rel_variable,
                        rel_type,
                        properties,
                        direction: RelationshipDirection::Incoming,
                        target_variable,
                        target_label,
                        target_properties,
                        min_hops,
                        max_hops,
                    },
                })
            } else if self.peek_char() == Some('-') {
                self.expect_char('-')?;
                let (rel_variable, rel_type, properties, min_hops, max_hops) =
                    self.parse_match_relationship_pattern()?;
                self.expect_char('-')?;
                let direction = if self.consume_char('>') {
                    RelationshipDirection::Outgoing
                } else {
                    RelationshipDirection::Undirected
                };
                let (target_variable, target_label, target_properties) =
                    self.parse_match_node_pattern()?;
                Some(PostMatchRelationshipExpand {
                    source_variable: matched_target_variable.clone(),
                    source_label: matched_target_label.clone(),
                    source_properties: matched_target_properties.clone(),
                    expand: RelationshipExpand {
                        variable: rel_variable,
                        rel_type,
                        properties,
                        direction,
                        target_variable,
                        target_label,
                        target_properties,
                        min_hops,
                        max_hops,
                    },
                })
            } else {
                None
            };
            if let Some(post_match_expand) = post_match_expand {
                if self.consume_keyword("WHERE") {
                    predicate = Some(combine_match_predicates(
                        predicate,
                        self.parse_property_predicate()?,
                    ));
                }
                if !self.consume_keyword("RETURN") {
                    return Err(self.error("expected RETURN"));
                }
                let distinct = self.consume_keyword("DISTINCT");
                let returns = self.parse_return_items()?;
                let order_by = if self.consume_keyword("ORDER") {
                    self.expect_keyword("BY")?;
                    self.parse_order_items()?
                } else {
                    Vec::new()
                };
                let offset = if self.consume_keyword("SKIP") || self.consume_keyword("OFFSET") {
                    Some(self.parse_value()?)
                } else {
                    None
                };
                let limit = if self.consume_keyword("LIMIT") {
                    Some(self.parse_value()?)
                } else {
                    None
                };
                return Ok(Statement::MatchReturn(Box::new(MatchReturn {
                    variable,
                    label,
                    properties,
                    expand,
                    post_match_expand: Some(post_match_expand),
                    optional_expand: None,
                    optional_with: None,
                    collect_with: None,
                    distinct_with: None,
                    aggregate_with: None,
                    aggregate_with_filter: None,
                    post_with_match: None,
                    predicate,
                    distinct,
                    returns,
                    order_by,
                    offset,
                    limit,
                })));
            }
            self.expect_keyword("MERGE")?;
            let Some(expand) = expand else {
                return Err(self.error("MATCH MERGE requires a bound relationship pattern"));
            };
            let merge_pattern = self.parse_bound_relationship_merge_pattern()?;
            let on_create_sets = if self.consume_keyword("ON") {
                self.expect_keyword("CREATE")?;
                self.expect_keyword("SET")?;
                self.parse_set_properties()?
            } else {
                Vec::new()
            };
            return Ok(Statement::MatchExpandMatchMergeRelationship(
                MatchExpandMatchMergeRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    expand,
                    matched_target_variable,
                    matched_target_label,
                    matched_target_properties,
                    merge_source_variable: merge_pattern.source_variable,
                    rel_variable: merge_pattern.rel_variable,
                    rel_type: merge_pattern.rel_type,
                    rel_properties: merge_pattern.rel_properties,
                    merge_target_variable: merge_pattern.target_variable,
                    on_create_sets,
                },
            ));
        }
        let optional_expand = if self.consume_keyword("OPTIONAL") {
            self.expect_keyword("MATCH")?;
            let mut scope = BTreeSet::from([variable.clone()]);
            if let Some(expand) = &expand {
                scope.insert(expand.target_variable.clone());
                if let Some(rel_variable) = &expand.variable {
                    scope.insert(rel_variable.clone());
                }
            }
            Some(self.parse_optional_relationship_expand(&scope)?)
        } else {
            None
        };
        let with_clause = if self.consume_keyword("WITH") {
            self.parse_with_clause()?
        } else {
            ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                aggregate_with: None,
            }
        };
        let aggregate_with_filter =
            if with_clause.aggregate_with.is_some() && self.consume_keyword("WHERE") {
                Some(self.parse_with_alias_filter()?)
            } else {
                None
            };
        let mut with_order_by = Vec::new();
        let mut with_offset = None;
        let mut with_limit = None;
        if with_clause.aggregate_with.is_some() || with_clause.optional_with.is_some() {
            if self.consume_keyword("ORDER") {
                self.expect_keyword("BY")?;
                with_order_by = self.parse_order_items()?;
            }
            if self.consume_keyword("SKIP") || self.consume_keyword("OFFSET") {
                with_offset = Some(self.parse_value()?);
            }
            if self.consume_keyword("LIMIT") {
                with_limit = Some(self.parse_value()?);
            }
        }
        let post_with_match = if with_clause.aggregate_with.is_some() {
            if self.consume_keyword("OPTIONAL") {
                self.expect_keyword("MATCH")?;
                Some(self.parse_post_with_node_lookup(true)?)
            } else if self.consume_keyword("MATCH") {
                Some(self.parse_post_with_node_lookup(false)?)
            } else {
                None
            }
        } else {
            None
        };
        if self.consume_keyword("SET") {
            let update = MatchSet {
                variable,
                label,
                properties,
                expand,
                predicate,
                sets: self.parse_set_properties()?,
            };
            if self.consume_keyword("RETURN") {
                return Ok(Statement::MatchSetReturn(MatchSetReturn {
                    update,
                    returns: self.parse_return_items()?,
                }));
            }
            return Ok(Statement::MatchSet(update));
        }
        let detach = self.consume_keyword("DETACH");
        if detach || self.next_keyword_is("DELETE") {
            if !self.consume_keyword("DELETE") {
                return Err(self.error("expected DELETE"));
            }
            let delete_variable = self.parse_ident()?;
            return Ok(Statement::MatchDelete(MatchDelete {
                variable,
                label,
                properties,
                expand,
                predicate,
                delete_variable,
                detach,
            }));
        }
        if !self.consume_keyword("RETURN") {
            return Err(self.error("expected RETURN"));
        }
        let distinct = self.consume_keyword("DISTINCT");
        let returns = self.parse_return_items()?;
        let order_by = if self.consume_keyword("ORDER") {
            if !with_order_by.is_empty() {
                return Err(self.error("ORDER BY is already attached to WITH"));
            }
            self.expect_keyword("BY")?;
            self.parse_order_items()?
        } else {
            with_order_by
        };
        let offset = if self.consume_keyword("SKIP") || self.consume_keyword("OFFSET") {
            if with_offset.is_some() {
                return Err(self.error("offset is already attached to WITH"));
            }
            Some(self.parse_value()?)
        } else {
            with_offset
        };
        let limit = if self.consume_keyword("LIMIT") {
            if with_limit.is_some() {
                return Err(self.error("LIMIT is already attached to WITH"));
            }
            Some(self.parse_value()?)
        } else {
            with_limit
        };
        Ok(Statement::MatchReturn(Box::new(MatchReturn {
            variable,
            label,
            properties,
            expand,
            post_match_expand: None,
            optional_expand,
            optional_with: with_clause.optional_with,
            collect_with: with_clause.collect_with,
            distinct_with: with_clause.distinct_with,
            aggregate_with: with_clause.aggregate_with,
            aggregate_with_filter,
            post_with_match,
            predicate,
            distinct,
            returns,
            order_by,
            offset,
            limit,
        })))
    }

    fn consume_match_path_binding_prefix(&mut self) -> Option<String> {
        self.skip_ws();
        let start = self.pos;
        let mut index = start;
        let mut chars = self.input[index..].chars();
        let first = chars.next()?;
        if !is_path_binding_ident_start(first) {
            return None;
        }
        index += first.len_utf8();
        while let Some(ch) = self.input[index..].chars().next() {
            if !is_path_binding_ident_continue(ch) {
                break;
            }
            index += ch.len_utf8();
        }
        let variable = self.input[start..index].to_string();
        index = skip_ascii_whitespace(self.input, index);
        if !self.input[index..].starts_with('=') {
            return None;
        }
        index += '='.len_utf8();
        index = skip_ascii_whitespace(self.input, index);
        if !self.input[index..].starts_with('(') {
            return None;
        }
        self.pos = index;
        Some(variable)
    }

    fn next_relationship_pattern_is_all_shortest(&self) -> bool {
        let mut index = skip_ascii_whitespace(self.input, self.pos);
        if !self.input[index..].starts_with('-') {
            return false;
        }
        index += '-'.len_utf8();
        index = skip_ascii_whitespace(self.input, index);
        if !self.input[index..].starts_with('[') {
            return false;
        }
        let Some(end) = self.input[index..].find(']') else {
            return false;
        };
        self.input[index..index + end]
            .to_ascii_uppercase()
            .contains("ALL SHORTEST")
    }

    fn parse_shortest_path_return(
        &mut self,
        path_variable: String,
        source_variable: String,
        source_label: String,
        source_properties: std::collections::BTreeMap<String, ValueExpression>,
    ) -> Result<Statement> {
        self.expect_char('-')?;
        let (rel_variable, rel_type, min_hops, max_hops) =
            self.parse_all_shortest_relationship_pattern()?;
        self.expect_char('-')?;
        let direction = if self.consume_char('>') {
            RelationshipDirection::Outgoing
        } else {
            RelationshipDirection::Undirected
        };
        let (target_variable, target_label, target_properties) = self.parse_match_node_pattern()?;
        let predicate = if self.consume_keyword("WHERE") {
            Some(self.parse_property_predicate()?)
        } else {
            None
        };
        self.expect_keyword("RETURN")?;
        Ok(Statement::ShortestPathReturn(Box::new(
            ShortestPathReturn {
                path_variable: path_variable.clone(),
                source_variable,
                source_label,
                source_properties,
                rel_variable,
                rel_type,
                direction,
                target_variable,
                target_label,
                target_properties,
                min_hops,
                max_hops,
                predicate,
                returns: self.parse_shortest_path_return_items(&path_variable)?,
            },
        )))
    }

    fn parse_all_shortest_relationship_pattern(
        &mut self,
    ) -> Result<(Option<String>, String, usize, usize)> {
        self.expect_char('[')?;
        let (variable, rel_type) = if self.peek_char() == Some('*') {
            (None, String::new())
        } else if self.consume_char(':') {
            (None, self.parse_ident()?)
        } else {
            let variable = self.parse_ident()?;
            let rel_type = if self.consume_char(':') {
                self.parse_ident()?
            } else {
                String::new()
            };
            (Some(variable), rel_type)
        };
        self.expect_char('*')?;
        self.expect_keyword("ALL")?;
        self.expect_keyword("SHORTEST")?;
        let (min_hops, max_hops) = self.parse_bounded_hops()?;
        self.expect_char(']')?;
        Ok((variable, rel_type, min_hops, max_hops))
    }

    fn parse_shortest_path_return_items(
        &mut self,
        path_variable: &str,
    ) -> Result<Vec<ShortestPathReturnItem>> {
        let mut items = Vec::new();
        loop {
            let expression = if self.consume_keyword("PROPERTIES") {
                self.expect_char('(')?;
                self.expect_keyword("NODES")?;
                self.expect_char('(')?;
                let nodes_path_variable = self.parse_ident()?;
                self.expect_char(')')?;
                self.expect_char(',')?;
                let property = match self.parse_value()? {
                    ValueExpression::Literal(crate::value::Value::String(property)) => property,
                    _ => return Err(self.error("node property projection requires a string key")),
                };
                self.expect_char(')')?;
                ShortestPathReturnExpression::NodePropertyList {
                    path_variable: nodes_path_variable,
                    property,
                }
            } else if self.consume_keyword("LENGTH") {
                self.expect_char('(')?;
                let length_path_variable = self.parse_ident()?;
                self.expect_char(')')?;
                ShortestPathReturnExpression::Length {
                    path_variable: length_path_variable,
                }
            } else {
                return Err(self.error("expected shortest path return expression"));
            };
            self.expect_keyword("AS")?;
            let alias = self.parse_ident()?;
            match &expression {
                ShortestPathReturnExpression::NodePropertyList {
                    path_variable: expression_path,
                    ..
                }
                | ShortestPathReturnExpression::Length {
                    path_variable: expression_path,
                } if expression_path != path_variable => {
                    return Err(self.error("shortest path return references a different path"));
                }
                _ => {}
            }
            items.push(ShortestPathReturnItem { expression, alias });
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_post_with_node_lookup(&mut self, optional: bool) -> Result<PostWithNodeLookup> {
        let (variable, label, properties) = self.parse_match_node_pattern()?;
        if !properties.is_empty() {
            return Err(self.error("post-WITH MATCH lookup does not support node properties"));
        }
        self.expect_keyword("WHERE")?;
        let predicate_variable = self.parse_ident()?;
        if predicate_variable != variable {
            return Err(self.error("post-WITH MATCH lookup variable mismatch"));
        }
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_char('=')?;
        let column = self.parse_ident()?;
        Ok(PostWithNodeLookup {
            variable,
            label,
            property,
            column,
            optional,
        })
    }

    fn parse_with_clause(&mut self) -> Result<ParsedWithClause> {
        if self.consume_keyword("DISTINCT") {
            let items = self.parse_return_items()?;
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: Some(WithDistinctProjection { items }),
                aggregate_with: None,
            });
        }
        if self.next_keyword_is("date_part") {
            let items = self.parse_return_items()?;
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                aggregate_with: Some(WithAggregateProjection { items }),
            });
        }
        let group_variable = self.parse_ident()?;
        if self.consume_char('.') {
            let first_item = ReturnItem {
                expression: ReturnExpression::Property {
                    variable: group_variable,
                    property: self.parse_ident()?,
                },
                alias: if self.consume_keyword("AS") {
                    Some(self.parse_ident()?)
                } else {
                    None
                },
            };
            let mut items = vec![first_item];
            while self.consume_char(',') {
                let mut next = self.parse_return_items()?;
                items.append(&mut next);
            }
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                aggregate_with: Some(WithAggregateProjection { items }),
            });
        }
        self.expect_char(',')?;
        if self.consume_keyword("COUNT") {
            let first_count = self.parse_count_return_item_after_count_keyword()?;
            let is_simple_optional_count = matches!(
                first_count.expression,
                ReturnExpression::CountVariable { .. }
            ) && !self.peek_next_with_item_separator();
            if is_simple_optional_count {
                let ReturnExpression::CountVariable { variable, distinct } = first_count.expression
                else {
                    unreachable!("simple optional count shape checked above");
                };
                let alias = first_count
                    .alias
                    .expect("count return items always require an alias in WITH");
                return Ok(ParsedWithClause {
                    optional_with: Some(OptionalWithAggregate {
                        group_variable,
                        count_variable: variable,
                        distinct,
                        alias,
                    }),
                    collect_with: None,
                    distinct_with: None,
                    aggregate_with: None,
                });
            }
            let mut items = vec![
                ReturnItem {
                    expression: ReturnExpression::Variable(group_variable),
                    alias: None,
                },
                first_count,
            ];
            while self.consume_char(',') {
                let mut next = self.parse_return_items()?;
                items.append(&mut next);
            }
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                aggregate_with: Some(WithAggregateProjection { items }),
            });
        }
        if self.consume_keyword("COLLECT") {
            self.expect_char('(')?;
            let distinct = self.consume_keyword("DISTINCT");
            let collect_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let collect_property = self.parse_ident()?;
            self.expect_char(')')?;
            self.expect_keyword("AS")?;
            let alias = self.parse_ident()?;
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: Some(WithCollect {
                    group_variable,
                    collect_variable,
                    collect_property,
                    distinct,
                    alias,
                }),
                distinct_with: None,
                aggregate_with: None,
            });
        }
        Err(self.error("expected COUNT or COLLECT"))
    }

    fn parse_count_return_item_after_count_keyword(&mut self) -> Result<ReturnItem> {
        self.expect_char('(')?;
        let distinct = self.consume_keyword("DISTINCT");
        let expression = if self.consume_char('*') {
            if distinct {
                return Err(self.error("COUNT(DISTINCT *) is not supported"));
            }
            ReturnExpression::CountAll
        } else {
            let variable = self.parse_ident()?;
            if self.consume_char('.') {
                ReturnExpression::CountProperty {
                    variable,
                    property: self.parse_ident()?,
                    distinct,
                }
            } else {
                ReturnExpression::CountVariable { variable, distinct }
            }
        };
        self.expect_char(')')?;
        self.expect_keyword("AS")?;
        let alias = self.parse_ident()?;
        Ok(ReturnItem {
            expression,
            alias: Some(alias),
        })
    }

    fn peek_next_with_item_separator(&mut self) -> bool {
        self.skip_ws();
        self.peek_char() == Some(',')
    }

    fn parse_with_alias_filter(&mut self) -> Result<WithAliasFilter> {
        let alias = self.parse_ident()?;
        self.skip_ws();
        let op = if self.consume_token("<>") || self.consume_token("!=") {
            WithAliasFilterOp::Ne
        } else if self.consume_token("<=") {
            WithAliasFilterOp::Lte
        } else if self.consume_token(">=") {
            WithAliasFilterOp::Gte
        } else if self.consume_char('=') {
            WithAliasFilterOp::Eq
        } else if self.consume_char('<') {
            WithAliasFilterOp::Lt
        } else if self.consume_char('>') {
            WithAliasFilterOp::Gt
        } else {
            return Err(self.error("expected WITH alias comparison operator"));
        };
        Ok(WithAliasFilter {
            alias,
            op,
            value: self.parse_value()?,
        })
    }

    fn parse_bound_relationship_create_pattern(
        &mut self,
    ) -> Result<(
        String,
        String,
        std::collections::BTreeMap<String, ValueExpression>,
        String,
    )> {
        self.expect_char('(')?;
        let source_variable = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_char('-')?;
        let (rel_type, rel_properties) = self.parse_relationship_pattern()?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        self.expect_char('(')?;
        let target_variable = self.parse_ident()?;
        self.expect_char(')')?;
        Ok((source_variable, rel_type, rel_properties, target_variable))
    }

    fn parse_bound_relationship_merge_pattern(&mut self) -> Result<BoundRelationshipMergePattern> {
        self.expect_char('(')?;
        let source_variable = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_char('-')?;
        self.expect_char('[')?;
        let rel_variable = if self.peek_char() == Some(':') {
            None
        } else {
            Some(self.parse_ident()?)
        };
        self.expect_char(':')?;
        let rel_type = self.parse_ident()?;
        self.skip_ws();
        let rel_properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            std::collections::BTreeMap::new()
        };
        self.expect_char(']')?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        self.expect_char('(')?;
        let target_variable = self.parse_ident()?;
        self.expect_char(')')?;
        Ok(BoundRelationshipMergePattern {
            source_variable,
            rel_variable,
            rel_type,
            rel_properties,
            target_variable,
        })
    }

    fn parse_optional_relationship_expand(
        &mut self,
        scope: &BTreeSet<String>,
    ) -> Result<OptionalRelationshipExpand> {
        let (source_variable, source_label, source_properties) = self.parse_match_node_pattern()?;
        let (rel_variable, rel_type, rel_properties, direction) = if self.consume_char('<') {
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if min_hops != 1 || max_hops != 1 {
                return Err(self.error("OPTIONAL MATCH supports only one-hop relationships"));
            }
            self.expect_char('-')?;
            (
                rel_variable,
                rel_type,
                properties,
                RelationshipDirection::Incoming,
            )
        } else {
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if min_hops != 1 || max_hops != 1 {
                return Err(self.error("OPTIONAL MATCH supports only one-hop relationships"));
            }
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            (rel_variable, rel_type, properties, direction)
        };
        let (target_variable, target_label, target_properties) = self.parse_match_node_pattern()?;
        if scope.contains(&source_variable) {
            return Ok(OptionalRelationshipExpand {
                source_variable,
                source_label,
                expand: RelationshipExpand {
                    variable: rel_variable,
                    rel_type,
                    properties: rel_properties,
                    direction,
                    target_variable,
                    target_label,
                    target_properties,
                    min_hops: 1,
                    max_hops: 1,
                },
            });
        }
        if scope.contains(&target_variable) {
            let direction = match direction {
                RelationshipDirection::Outgoing => RelationshipDirection::Incoming,
                RelationshipDirection::Incoming => RelationshipDirection::Outgoing,
                RelationshipDirection::Undirected => RelationshipDirection::Undirected,
            };
            return Ok(OptionalRelationshipExpand {
                source_variable: target_variable,
                source_label: target_label,
                expand: RelationshipExpand {
                    variable: rel_variable,
                    rel_type,
                    properties: rel_properties,
                    direction,
                    target_variable: source_variable,
                    target_label: source_label,
                    target_properties: source_properties,
                    min_hops: 1,
                    max_hops: 1,
                },
            });
        }
        Err(self.error("OPTIONAL MATCH must reference a bound node variable"))
    }
}

fn skip_ascii_whitespace(input: &str, mut index: usize) -> usize {
    while let Some(ch) = input[index..].chars().next() {
        if !ch.is_whitespace() {
            break;
        }
        index += ch.len_utf8();
    }
    index
}

fn combine_match_predicates(
    left: Option<PropertyPredicate>,
    right: PropertyPredicate,
) -> PropertyPredicate {
    match left {
        Some(PropertyPredicate::And(mut predicates)) => {
            predicates.push(right);
            PropertyPredicate::And(predicates)
        }
        Some(left) => PropertyPredicate::And(vec![left, right]),
        None => right,
    }
}

fn is_path_binding_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_path_binding_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}
