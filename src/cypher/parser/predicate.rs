use std::collections::BTreeMap;

use crate::error::Result;

use super::super::ast::*;
use super::Parser;

type MatchRelationshipPattern = (
    Option<String>,
    String,
    BTreeMap<String, ValueExpression>,
    usize,
    usize,
);

enum PropertyPredicateRight {
    Value(ValueExpression),
    Expression(ReturnValueExpression),
}

impl Parser<'_> {
    pub(super) fn parse_property_predicate(&mut self) -> Result<PropertyPredicate> {
        let mut predicates = vec![self.parse_property_conjunction()?];
        while self.consume_keyword("OR") {
            predicates.push(self.parse_property_conjunction()?);
        }
        if predicates.len() == 1 {
            Ok(predicates.remove(0))
        } else {
            Ok(PropertyPredicate::Or(predicates))
        }
    }

    pub(super) fn parse_property_conjunction(&mut self) -> Result<PropertyPredicate> {
        let mut predicates = vec![self.parse_property_predicate_atom()?];
        while self.consume_keyword("AND") {
            predicates.push(self.parse_property_predicate_atom()?);
        }
        if predicates.len() == 1 {
            Ok(predicates.remove(0))
        } else {
            Ok(PropertyPredicate::And(predicates))
        }
    }

    pub(super) fn parse_property_predicate_atom(&mut self) -> Result<PropertyPredicate> {
        self.skip_ws();
        if self.consume_keyword("NOT") {
            return Ok(PropertyPredicate::Not(Box::new(
                self.parse_property_predicate_atom()?,
            )));
        }
        if self.peek_char() == Some('(') && self.looks_like_relationship_exists_predicate() {
            return self.parse_relationship_exists_predicate();
        }
        if self.consume_keyword("EXISTS") {
            return self.parse_bound_relationship_exists_subquery();
        }
        if self.looks_like_parenthesized_return_value_expression_predicate() {
            self.expect_char('(')?;
            let expression = self.parse_return_value_expression()?;
            self.expect_char(')')?;
            return self.parse_expression_predicate(expression);
        }
        if self.consume_char('(') {
            let predicate = self.parse_property_predicate()?;
            self.expect_char(')')?;
            return Ok(predicate);
        }
        if self.consume_char('$') {
            let parameter = self.parse_ident()?;
            self.expect_keyword("IS")?;
            let is_not = self.consume_keyword("NOT");
            self.expect_keyword("NULL")?;
            return if is_not {
                Ok(PropertyPredicate::ParameterIsNotNull { parameter })
            } else {
                Ok(PropertyPredicate::ParameterIsNull { parameter })
            };
        }
        let variable = self.parse_ident()?;
        if variable.eq_ignore_ascii_case("list_contains") && self.peek_char() == Some('(') {
            self.expect_char('(')?;
            let variable = self.parse_ident()?;
            self.expect_char('.')?;
            let property = self.parse_ident()?;
            self.expect_char(',')?;
            let value = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(PropertyPredicate::ListContains {
                variable,
                property,
                value,
            });
        }
        if matches_ignore_ascii_case(&variable, &["coalesce", "left", "lower", "case"])
            && self.peek_char() == Some('(')
        {
            self.pos -= variable.len();
            let expression = self.parse_return_value_expression()?;
            return self.parse_expression_predicate(expression);
        }
        if variable.eq_ignore_ascii_case("case") {
            self.pos -= variable.len();
            let expression = self.parse_return_value_expression()?;
            return self.parse_expression_predicate(expression);
        }
        if variable.eq_ignore_ascii_case("id") && self.consume_char('(') {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return self.parse_id_predicate(variable);
        }
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.skip_ws();
        if self.consume_keyword("IS") {
            let is_not = self.consume_keyword("NOT");
            if !self.consume_keyword("NULL") {
                return Err(self.error("expected NULL"));
            }
            if is_not {
                Ok(PropertyPredicate::IsNotNull { variable, property })
            } else {
                Ok(PropertyPredicate::IsNull { variable, property })
            }
        } else if self.consume_keyword("IN") {
            let values = self.parse_value()?;
            Ok(PropertyPredicate::In {
                variable,
                property,
                values,
            })
        } else if self.consume_keyword("CONTAINS") {
            let value = self.parse_value()?;
            Ok(PropertyPredicate::Contains {
                variable,
                property,
                value,
            })
        } else if self.consume_keyword("STARTS") {
            self.expect_keyword("WITH")?;
            let value = self.parse_value()?;
            Ok(PropertyPredicate::StartsWith {
                variable,
                property,
                value,
            })
        } else if self.consume_keyword("ENDS") {
            self.expect_keyword("WITH")?;
            let value = self.parse_value()?;
            Ok(PropertyPredicate::EndsWith {
                variable,
                property,
                value,
            })
        } else if self.consume_char('<') {
            if self.consume_char('>') {
                return match self.parse_property_predicate_right()? {
                    PropertyPredicateRight::Value(value) => Ok(PropertyPredicate::NotEq {
                        variable,
                        property,
                        value,
                    }),
                    PropertyPredicateRight::Expression(value) => {
                        Ok(PropertyPredicate::ExpressionNotEq {
                            expression: ReturnValueExpression::Property { variable, property },
                            value,
                        })
                    }
                };
            }
            let op = if self.consume_char('=') {
                ComparisonOp::Lte
            } else {
                ComparisonOp::Lt
            };
            let value = self.parse_value()?;
            Ok(PropertyPredicate::Compare {
                variable,
                property,
                op,
                value,
            })
        } else if self.consume_char('>') {
            let op = if self.consume_char('=') {
                ComparisonOp::Gte
            } else {
                ComparisonOp::Gt
            };
            let value = self.parse_value()?;
            Ok(PropertyPredicate::Compare {
                variable,
                property,
                op,
                value,
            })
        } else {
            self.expect_char('=')?;
            match self.parse_property_predicate_right()? {
                PropertyPredicateRight::Value(value) => Ok(PropertyPredicate::Eq {
                    variable,
                    property,
                    value,
                }),
                PropertyPredicateRight::Expression(value) => Ok(PropertyPredicate::ExpressionEq {
                    expression: ReturnValueExpression::Property { variable, property },
                    value,
                }),
            }
        }
    }

    fn parse_property_predicate_right(&mut self) -> Result<PropertyPredicateRight> {
        self.skip_ws();
        let value_start = self.pos;
        if matches!(self.peek_char(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_') {
            let variable = self.parse_ident()?;
            if self.consume_char('.') {
                return Ok(PropertyPredicateRight::Expression(
                    ReturnValueExpression::Property {
                        variable,
                        property: self.parse_ident()?,
                    },
                ));
            }
            self.pos = value_start;
        }
        self.parse_value().map(PropertyPredicateRight::Value)
    }

    fn looks_like_relationship_exists_predicate(&self) -> bool {
        let bytes = self.input.as_bytes();
        let mut depth = 0usize;
        let mut index = self.pos;
        while index < bytes.len() {
            let ch = self.input[index..]
                .chars()
                .next()
                .expect("index stays on a char boundary");
            match ch {
                '(' => depth += 1,
                ')' => {
                    if depth == 0 {
                        return false;
                    }
                    depth -= 1;
                    if depth == 0 {
                        let after = index + ch.len_utf8();
                        let rest = &self.input[after..];
                        return rest.trim_start().starts_with("-[")
                            || rest.trim_start().starts_with("<-[");
                    }
                }
                _ => {}
            }
            index += ch.len_utf8();
        }
        false
    }

    fn looks_like_parenthesized_return_value_expression_predicate(&self) -> bool {
        let mut index = self.pos;
        while let Some(ch) = self.input[index..].chars().next() {
            if !ch.is_whitespace() {
                break;
            }
            index += ch.len_utf8();
        }
        if !self.input[index..].starts_with('(') {
            return false;
        }
        index += '('.len_utf8();
        while let Some(ch) = self.input[index..].chars().next() {
            if !ch.is_whitespace() {
                break;
            }
            index += ch.len_utf8();
        }
        ["case", "coalesce", "left", "lower"]
            .iter()
            .any(|keyword| keyword_matches_at(self.input, index, keyword))
    }

    fn parse_relationship_exists_predicate(&mut self) -> Result<PropertyPredicate> {
        let (variable, _, _) = self.parse_match_node_pattern()?;
        let (direction, rel_type, target_label) = if self.consume_char('<') {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("relationship existence predicates support one-hop types"));
            }
            self.expect_char('-')?;
            let (_, target_label, target_properties) = self.parse_match_node_pattern()?;
            if !target_properties.is_empty() {
                return Err(self
                    .error("relationship existence predicates do not support target properties"));
            }
            (RelationshipDirection::Incoming, rel_type, target_label)
        } else {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("relationship existence predicates support one-hop types"));
            }
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let (_, target_label, target_properties) = self.parse_match_node_pattern()?;
            if !target_properties.is_empty() {
                return Err(self
                    .error("relationship existence predicates do not support target properties"));
            }
            (direction, rel_type, target_label)
        };
        Ok(PropertyPredicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        })
    }

    fn parse_bound_relationship_exists_subquery(&mut self) -> Result<PropertyPredicate> {
        self.expect_char('{')?;
        self.expect_keyword("MATCH")?;
        let (source_variable, source_label, source_properties) = self.parse_match_node_pattern()?;
        if !source_label.is_empty() || !source_properties.is_empty() {
            return Err(self.error("EXISTS relationship subqueries require bound source variables"));
        }
        let (direction, rel_type, target_variable) = if self.consume_char('<') {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("EXISTS relationship subqueries support one-hop types"));
            }
            self.expect_char('-')?;
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            if !target_label.is_empty() || !target_properties.is_empty() {
                return Err(
                    self.error("EXISTS relationship subqueries require bound target variables")
                );
            }
            (RelationshipDirection::Incoming, rel_type, target_variable)
        } else {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("EXISTS relationship subqueries support one-hop types"));
            }
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            if !target_label.is_empty() || !target_properties.is_empty() {
                return Err(
                    self.error("EXISTS relationship subqueries require bound target variables")
                );
            }
            (direction, rel_type, target_variable)
        };
        self.expect_char('}')?;
        Ok(PropertyPredicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        })
    }

    fn parse_expression_predicate(
        &mut self,
        expression: ReturnValueExpression,
    ) -> Result<PropertyPredicate> {
        self.skip_ws();
        if self.consume_char('<') {
            if self.consume_char('>') {
                return Ok(PropertyPredicate::ExpressionNotEq {
                    expression,
                    value: self.parse_return_value_expression()?,
                });
            }
            let op = if self.consume_char('=') {
                ComparisonOp::Lte
            } else {
                ComparisonOp::Lt
            };
            return Ok(PropertyPredicate::ExpressionCompare {
                expression,
                op,
                value: self.parse_value()?,
            });
        }
        if self.consume_char('>') {
            let op = if self.consume_char('=') {
                ComparisonOp::Gte
            } else {
                ComparisonOp::Gt
            };
            return Ok(PropertyPredicate::ExpressionCompare {
                expression,
                op,
                value: self.parse_value()?,
            });
        }
        if self.consume_keyword("CONTAINS") {
            return Ok(PropertyPredicate::ExpressionContains {
                expression,
                value: self.parse_return_value_expression()?,
            });
        }
        self.expect_char('=')?;
        Ok(PropertyPredicate::ExpressionEq {
            expression,
            value: self.parse_return_value_expression()?,
        })
    }

    fn parse_id_predicate(&mut self, variable: String) -> Result<PropertyPredicate> {
        self.skip_ws();
        if self.consume_keyword("IN") {
            return Ok(PropertyPredicate::IdIn {
                variable,
                values: self.parse_value()?,
            });
        }
        if self.consume_char('<') {
            if self.consume_char('>') {
                return Ok(PropertyPredicate::IdNotEq {
                    variable,
                    value: self.parse_value()?,
                });
            }
            let op = if self.consume_char('=') {
                ComparisonOp::Lte
            } else {
                ComparisonOp::Lt
            };
            return Ok(PropertyPredicate::IdCompare {
                variable,
                op,
                value: self.parse_value()?,
            });
        }
        if self.consume_char('>') {
            let op = if self.consume_char('=') {
                ComparisonOp::Gte
            } else {
                ComparisonOp::Gt
            };
            return Ok(PropertyPredicate::IdCompare {
                variable,
                op,
                value: self.parse_value()?,
            });
        }
        self.expect_char('=')?;
        Ok(PropertyPredicate::IdEq {
            variable,
            value: self.parse_value()?,
        })
    }

    pub(super) fn parse_match_relationship_pattern(&mut self) -> Result<MatchRelationshipPattern> {
        self.expect_char('[')?;
        let (variable, rel_type) = if self.consume_char(':') {
            (None, self.parse_ident()?)
        } else if self.peek_char() == Some(']') {
            (None, String::new())
        } else {
            let variable = self.parse_ident()?;
            let rel_type = if self.consume_char(':') {
                self.parse_ident()?
            } else {
                String::new()
            };
            (Some(variable), rel_type)
        };
        let (min_hops, max_hops) = if self.consume_char('*') {
            self.parse_bounded_hops()?
        } else {
            (1, 1)
        };
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.expect_char(']')?;
        Ok((variable, rel_type, properties, min_hops, max_hops))
    }

    pub(super) fn parse_bounded_hops(&mut self) -> Result<(usize, usize)> {
        self.skip_ws();
        let min = if matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
            Some(self.parse_usize()?)
        } else {
            None
        };
        if self.consume_char('.') {
            self.expect_char('.')?;
            let Some(max) = (if matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
                Some(self.parse_usize()?)
            } else {
                None
            }) else {
                return Err(self.error("bounded relationship pattern requires a finite max hop"));
            };
            let min = min.unwrap_or(1);
            if min > max {
                return Err(self.error("relationship min hop must not exceed max hop"));
            }
            return Ok((min, max));
        }
        let Some(exact) = min else {
            return Err(self.error("bounded relationship pattern requires a finite max hop"));
        };
        Ok((exact, exact))
    }

    pub(super) fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            let expression = if self.consume_keyword("COUNT") {
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
                expression
            } else if self.consume_keyword("MIN") {
                self.expect_char('(')?;
                let variable = self.parse_ident()?;
                self.expect_char('.')?;
                let property = self.parse_ident()?;
                self.expect_char(')')?;
                ReturnExpression::MinProperty { variable, property }
            } else if self.consume_keyword("MAX") {
                self.expect_char('(')?;
                let variable = self.parse_ident()?;
                self.expect_char('.')?;
                let property = self.parse_ident()?;
                self.expect_char(')')?;
                ReturnExpression::MaxProperty { variable, property }
            } else if self.consume_keyword("AVG") {
                self.expect_char('(')?;
                let variable = self.parse_ident()?;
                self.expect_char('.')?;
                let property = self.parse_ident()?;
                self.expect_char(')')?;
                ReturnExpression::AvgProperty { variable, property }
            } else {
                self.parse_return_projection_expression()?
            };
            let alias = if self.consume_keyword("AS") {
                Some(self.parse_ident()?)
            } else {
                None
            };
            items.push(ReturnItem { expression, alias });
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    pub(super) fn parse_set_properties(&mut self) -> Result<Vec<SetProperty>> {
        let mut sets = Vec::new();
        loop {
            sets.push(self.parse_set_property()?);
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(sets)
    }

    fn parse_set_property(&mut self) -> Result<SetProperty> {
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.skip_ws();
        self.expect_char('=')?;
        self.skip_ws();
        let value_start = self.pos;
        let value = if self.consume_keyword("COALESCE") {
            self.expect_char('(')?;
            let expression_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let expression_property = self.parse_ident()?;
            self.expect_char(',')?;
            let default = self.parse_value()?;
            self.expect_char(')')?;
            if self.consume_char('+') {
                SetValueExpression::CoalescePropertyAdd {
                    variable: expression_variable,
                    property: expression_property,
                    default,
                    value: self.parse_value()?,
                }
            } else {
                SetValueExpression::CoalesceProperty {
                    variable: expression_variable,
                    property: expression_property,
                    default,
                }
            }
        } else if self.peek_char().is_some_and(is_identifier_start) {
            let expression_variable = self.parse_ident()?;
            if self.consume_char('.') {
                let expression_property = self.parse_ident()?;
                self.skip_ws();
                if self.consume_char('+') {
                    SetValueExpression::PropertyAdd {
                        variable: expression_variable,
                        property: expression_property,
                        value: self.parse_value()?,
                    }
                } else {
                    SetValueExpression::Property {
                        variable: expression_variable,
                        property: expression_property,
                    }
                }
            } else {
                self.pos = value_start;
                SetValueExpression::Value(self.parse_value()?)
            }
        } else {
            SetValueExpression::Value(self.parse_value()?)
        };
        Ok(SetProperty {
            variable,
            property,
            value,
        })
    }

    pub(super) fn parse_order_items(&mut self) -> Result<Vec<OrderItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            let first = self.parse_ident()?;
            let expression = if first.eq_ignore_ascii_case("count") && self.consume_char('(') {
                let distinct = self.consume_keyword("DISTINCT");
                let name = if self.consume_char('*') {
                    if distinct {
                        return Err(self.error("COUNT(DISTINCT *) is not supported"));
                    }
                    "count(*)".to_string()
                } else {
                    let variable = self.parse_ident()?;
                    if self.consume_char('.') {
                        let property = self.parse_ident()?;
                        if distinct {
                            format!("count(DISTINCT {variable}.{property})")
                        } else {
                            format!("count({variable}.{property})")
                        }
                    } else if distinct {
                        format!("count(DISTINCT {variable})")
                    } else {
                        format!("count({variable})")
                    }
                };
                self.expect_char(')')?;
                OrderExpression::Column(name)
            } else if first.eq_ignore_ascii_case("id") && self.consume_char('(') {
                let variable = self.parse_ident()?;
                self.expect_char(')')?;
                OrderExpression::Id { variable }
            } else if matches_ignore_ascii_case(&first, &["coalesce", "left", "lower", "case"])
                && self.peek_char() == Some('(')
            {
                self.pos -= first.len();
                OrderExpression::Value(self.parse_return_value_expression()?)
            } else if first.eq_ignore_ascii_case("case") {
                OrderExpression::Value(self.parse_case_property_not_null_or_eq_expression()?)
            } else if self.consume_char('.') {
                OrderExpression::Property {
                    variable: first,
                    property: self.parse_ident()?,
                }
            } else {
                OrderExpression::Column(first)
            };
            let direction = if self.consume_keyword("DESC") {
                OrderDirection::Desc
            } else {
                let _ = self.consume_keyword("ASC");
                OrderDirection::Asc
            };
            items.push(OrderItem {
                expression,
                direction,
            });
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_return_projection_expression(&mut self) -> Result<ReturnExpression> {
        let expression = self.parse_return_value_expression()?;
        Ok(match expression {
            ReturnValueExpression::Variable(variable) => ReturnExpression::Variable(variable),
            ReturnValueExpression::Property { variable, property } => {
                ReturnExpression::Property { variable, property }
            }
            ReturnValueExpression::Id(variable) => ReturnExpression::Id(variable),
            ReturnValueExpression::RelationshipType(variable) => {
                ReturnExpression::RelationshipType(variable)
            }
            ReturnValueExpression::Coalesce(expressions) => ReturnExpression::Coalesce(expressions),
            ReturnValueExpression::Left { expression, length } => {
                ReturnExpression::Left { expression, length }
            }
            ReturnValueExpression::Lower(expression) => ReturnExpression::Lower(expression),
            ReturnValueExpression::DatePart {
                part,
                variable,
                property,
            } => ReturnExpression::DatePart {
                part,
                variable,
                property,
            },
            ReturnValueExpression::DefaultIfNullOrEq {
                variable,
                property,
                empty,
                default,
            } => ReturnExpression::DefaultIfNullOrEq {
                variable,
                property,
                empty,
                default,
            },
            ReturnValueExpression::CasePropertyNotNullOrEq {
                variable,
                property,
                empty,
                non_empty,
                null_or_empty,
            } => ReturnExpression::CasePropertyNotNullOrEq {
                variable,
                property,
                empty,
                non_empty,
                null_or_empty,
            },
            ReturnValueExpression::Value(_) => {
                return Err(self.error("literal return expressions require an aliasing function"));
            }
        })
    }

    fn parse_return_value_expression(&mut self) -> Result<ReturnValueExpression> {
        self.skip_ws();
        if matches!(
            self.peek_char(),
            Some('$' | '[' | '\'' | '"' | '-' | '0'..='9')
        ) || self.next_keyword_is("true")
            || self.next_keyword_is("false")
            || self.next_keyword_is("null")
        {
            return self.parse_value().map(ReturnValueExpression::Value);
        }

        let variable = self.parse_ident()?;
        if variable.eq_ignore_ascii_case("id") && self.consume_char('(') {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::Id(variable));
        }
        if (variable.eq_ignore_ascii_case("label") || variable.eq_ignore_ascii_case("type"))
            && self.consume_char('(')
        {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::RelationshipType(variable));
        }
        if variable.eq_ignore_ascii_case("coalesce") && self.consume_char('(') {
            let mut expressions = Vec::new();
            loop {
                expressions.push(self.parse_return_value_expression()?);
                self.skip_ws();
                if self.consume_char(')') {
                    break;
                }
                self.expect_char(',')?;
            }
            return Ok(ReturnValueExpression::Coalesce(expressions));
        }
        if variable.eq_ignore_ascii_case("left") && self.consume_char('(') {
            let expression = self.parse_return_value_expression()?;
            self.expect_char(',')?;
            let length = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::Left {
                expression: Box::new(expression),
                length,
            });
        }
        if variable.eq_ignore_ascii_case("lower") && self.consume_char('(') {
            let expression = self.parse_return_value_expression()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::Lower(Box::new(expression)));
        }
        if variable.eq_ignore_ascii_case("date_part") && self.consume_char('(') {
            let part = match self.parse_value()? {
                ValueExpression::Literal(crate::value::Value::String(part)) => part,
                _ => return Err(self.error("date_part part requires a string literal")),
            };
            self.expect_char(',')?;
            let date_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let date_property = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::DatePart {
                part,
                variable: date_variable,
                property: date_property,
            });
        }
        if variable.eq_ignore_ascii_case("case") {
            let start = self.pos;
            if let Ok(expression) = self.parse_default_if_null_or_eq_expression() {
                return Ok(expression);
            }
            self.pos = start;
            return self.parse_case_property_not_null_or_eq_expression();
        }
        if self.consume_char('.') {
            Ok(ReturnValueExpression::Property {
                variable,
                property: self.parse_ident()?,
            })
        } else {
            Ok(ReturnValueExpression::Variable(variable))
        }
    }

    fn parse_default_if_null_or_eq_expression(&mut self) -> Result<ReturnValueExpression> {
        self.expect_keyword("WHEN")?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_keyword("IS")?;
        self.expect_keyword("NULL")?;
        self.expect_keyword("OR")?;
        let eq_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let eq_property = self.parse_ident()?;
        if eq_variable != variable || eq_property != property {
            return Err(self.error("CASE expression supports only one normalized property"));
        }
        self.expect_char('=')?;
        let empty = self.parse_value()?;
        self.expect_keyword("THEN")?;
        let default = self.parse_value()?;
        self.expect_keyword("ELSE")?;
        let else_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let else_property = self.parse_ident()?;
        if else_variable != variable || else_property != property {
            return Err(self.error("CASE expression ELSE must return the normalized property"));
        }
        self.expect_keyword("END")?;
        Ok(ReturnValueExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        })
    }

    fn parse_case_property_not_null_or_eq_expression(&mut self) -> Result<ReturnValueExpression> {
        self.expect_keyword("WHEN")?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_keyword("IS")?;
        self.expect_keyword("NOT")?;
        self.expect_keyword("NULL")?;
        self.expect_keyword("AND")?;
        let neq_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let neq_property = self.parse_ident()?;
        if neq_variable != variable || neq_property != property {
            return Err(self.error("CASE sort expression supports only one property"));
        }
        if !self.consume_token("<>") && !self.consume_token("!=") {
            return Err(self.error("expected CASE sort expression inequality"));
        }
        let empty = self.parse_value()?;
        self.expect_keyword("THEN")?;
        let non_empty = self.parse_value()?;
        self.expect_keyword("ELSE")?;
        let null_or_empty = self.parse_value()?;
        self.expect_keyword("END")?;
        Ok(ReturnValueExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        })
    }
}

fn matches_ignore_ascii_case(value: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}

fn keyword_matches_at(input: &str, index: usize, keyword: &str) -> bool {
    let rest = &input[index..];
    if rest.len() < keyword.len() || !rest[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return false;
    }
    let next = rest[keyword.len()..].chars().next();
    !matches!(next, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}
