use crate::error::{Result, SkeinError};
use crate::value::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    CreateNode(CreateNode),
    CreateRelationship(CreateRelationship),
    MatchReturn(MatchReturn),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNode {
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRelationship {
    pub source: CreateNode,
    pub rel_type: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub target: CreateNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchReturn {
    pub variable: String,
    pub label: String,
    pub expand: Option<RelationshipExpand>,
    pub predicate: Option<PropertyPredicate>,
    pub returns: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipExpand {
    pub rel_type: String,
    pub target_variable: String,
    pub target_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyPredicate {
    pub variable: String,
    pub property: String,
    pub value: ValueExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueExpression {
    Literal(Value),
    Parameter(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnItem {
    pub variable: String,
    pub property: String,
    pub alias: Option<String>,
}

pub fn parse(input: &str) -> Result<Statement> {
    let mut parser = Parser::new(input);
    let statement = if parser.consume_keyword("CREATE") {
        parser.parse_create_statement()?
    } else if parser.consume_keyword("MATCH") {
        Statement::MatchReturn(parser.parse_match_return()?)
    } else {
        return Err(parser.error("expected CREATE or MATCH"));
    };
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(parser.error("unexpected trailing input"));
    }
    Ok(statement)
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse_create_statement(&mut self) -> Result<Statement> {
        let source = self.parse_create_node_pattern()?;
        if !self.consume_char('-') {
            return Ok(Statement::CreateNode(source));
        }
        let (rel_type, properties) = self.parse_relationship_pattern()?;
        self.expect_char('-')?;
        self.expect_char('>')?;
        let target = self.parse_create_node_pattern()?;
        Ok(Statement::CreateRelationship(CreateRelationship {
            source,
            rel_type,
            properties,
            target,
        }))
    }

    fn parse_create_node_pattern(&mut self) -> Result<CreateNode> {
        self.skip_ws();
        self.expect_char('(')?;
        self.skip_ws();
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.skip_ws();
        self.expect_char(')')?;
        Ok(CreateNode { label, properties })
    }

    fn parse_match_node_pattern(&mut self) -> Result<(String, String)> {
        self.skip_ws();
        self.expect_char('(')?;
        let variable = self.parse_ident()?;
        self.expect_char(':')?;
        let label = self.parse_ident()?;
        self.expect_char(')')?;
        Ok((variable, label))
    }

    fn parse_relationship_pattern(
        &mut self,
    ) -> Result<(String, BTreeMap<String, ValueExpression>)> {
        self.expect_char('[')?;
        self.expect_char(':')?;
        let rel_type = self.parse_ident()?;
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.expect_char(']')?;
        Ok((rel_type, properties))
    }

    fn parse_match_return(&mut self) -> Result<MatchReturn> {
        let (variable, label) = self.parse_match_node_pattern()?;
        let expand = if self.consume_char('-') {
            let (rel_type, properties) = self.parse_relationship_pattern()?;
            if !properties.is_empty() {
                return Err(self.error("relationship predicates in MATCH are not supported yet"));
            }
            self.expect_char('-')?;
            self.expect_char('>')?;
            let (target_variable, target_label) = self.parse_match_node_pattern()?;
            Some(RelationshipExpand {
                rel_type,
                target_variable,
                target_label,
            })
        } else {
            None
        };
        let predicate = if self.consume_keyword("WHERE") {
            Some(self.parse_property_predicate()?)
        } else {
            None
        };
        if !self.consume_keyword("RETURN") {
            return Err(self.error("expected RETURN"));
        }
        let returns = self.parse_return_items()?;
        Ok(MatchReturn {
            variable,
            label,
            expand,
            predicate,
            returns,
        })
    }

    fn parse_property_predicate(&mut self) -> Result<PropertyPredicate> {
        self.skip_ws();
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.skip_ws();
        self.expect_char('=')?;
        let value = self.parse_value()?;
        Ok(PropertyPredicate {
            variable,
            property,
            value,
        })
    }

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            let variable = self.parse_ident()?;
            self.expect_char('.')?;
            let property = self.parse_ident()?;
            let alias = if self.consume_keyword("AS") {
                Some(self.parse_ident()?)
            } else {
                None
            };
            items.push(ReturnItem {
                variable,
                property,
                alias,
            });
            self.skip_ws();
            if self.peek_char() != Some(',') {
                break;
            }
            self.pos += 1;
        }
        Ok(items)
    }

    fn parse_properties(&mut self) -> Result<BTreeMap<String, ValueExpression>> {
        let mut properties = BTreeMap::new();
        self.expect_char('{')?;
        loop {
            self.skip_ws();
            if self.peek_char() == Some('}') {
                self.pos += 1;
                break;
            }
            let key = self.parse_ident()?;
            self.skip_ws();
            self.expect_char(':')?;
            let value = self.parse_value()?;
            properties.insert(key, value);
            self.skip_ws();
            match self.peek_char() {
                Some(',') => self.pos += 1,
                Some('}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(self.error("expected ',' or '}'")),
            }
        }
        Ok(properties)
    }

    fn parse_value(&mut self) -> Result<ValueExpression> {
        self.skip_ws();
        match self.peek_char() {
            Some('$') => {
                self.pos += 1;
                self.parse_ident().map(ValueExpression::Parameter)
            }
            Some('\'') | Some('"') => self
                .parse_string()
                .map(Value::String)
                .map(ValueExpression::Literal),
            Some(ch) if ch.is_ascii_digit() || ch == '-' => self
                .parse_int()
                .map(Value::Int)
                .map(ValueExpression::Literal),
            _ if self.consume_keyword("true") => Ok(ValueExpression::Literal(Value::Bool(true))),
            _ if self.consume_keyword("false") => Ok(ValueExpression::Literal(Value::Bool(false))),
            _ if self.consume_keyword("null") => Ok(ValueExpression::Literal(Value::Null)),
            _ => Err(self.error("expected value")),
        }
    }

    fn parse_string(&mut self) -> Result<String> {
        let quote = self
            .next_char()
            .ok_or_else(|| self.error("expected string"))?;
        let mut out = String::new();
        while let Some(ch) = self.next_char() {
            if ch == quote {
                return Ok(out);
            }
            out.push(ch);
        }
        Err(self.error("unterminated string"))
    }

    fn parse_int(&mut self) -> Result<i64> {
        self.skip_ws();
        let start = self.pos;
        if self.peek_char() == Some('-') {
            self.pos += 1;
        }
        while matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
            self.pos += 1;
        }
        self.input[start..self.pos]
            .parse()
            .map_err(|_| self.error("invalid integer"))
    }

    fn parse_ident(&mut self) -> Result<String> {
        self.skip_ws();
        let start = self.pos;
        while matches!(self.peek_char(), Some(ch) if ch.is_ascii_alphanumeric() || ch == '_') {
            self.pos += 1;
        }
        if start == self.pos {
            return Err(self.error("expected identifier"));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        if rest.len() < keyword.len() || !rest[..keyword.len()].eq_ignore_ascii_case(keyword) {
            return false;
        }
        let next = rest[keyword.len()..].chars().next();
        if matches!(next, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_') {
            return false;
        }
        self.pos += keyword.len();
        true
    }

    fn consume_char(&mut self, expected: char) -> bool {
        self.skip_ws();
        if self.peek_char() != Some(expected) {
            return false;
        }
        self.pos += expected.len_utf8();
        true
    }

    fn expect_char(&mut self, expected: char) -> Result<()> {
        self.skip_ws();
        match self.next_char() {
            Some(ch) if ch == expected => Ok(()),
            _ => Err(self.error(&format!("expected '{expected}'"))),
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek_char(), Some(ch) if ch.is_whitespace()) {
            self.pos += 1;
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn error(&self, message: &str) -> SkeinError {
        SkeinError::Parse(format!("{message} at byte {}", self.pos))
    }
}

#[cfg(test)]
mod tests {
    use super::{parse, Statement, ValueExpression};

    #[test]
    fn parses_create_node() {
        let statement = parse("CREATE (:Memory {id: 1, title: 'hello'})").unwrap();
        let Statement::CreateNode(node) = statement else {
            panic!("expected create node");
        };
        assert_eq!(node.label, "Memory");
        assert_eq!(node.properties.len(), 2);
    }

    #[test]
    fn parses_match_return() {
        let statement = parse("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title").unwrap();
        let Statement::MatchReturn(query) = statement else {
            panic!("expected match return");
        };
        assert_eq!(query.variable, "m");
        assert_eq!(query.label, "Memory");
        assert_eq!(query.returns[0].alias.as_deref(), Some("title"));
    }

    #[test]
    fn parses_parameter_value_without_binding_it() {
        let statement = parse("MATCH (m:Memory) WHERE m.id = $id RETURN m.title").unwrap();
        let Statement::MatchReturn(query) = statement else {
            panic!("expected match return");
        };
        assert_eq!(
            query.predicate.unwrap().value,
            ValueExpression::Parameter("id".to_string())
        );
    }
}
