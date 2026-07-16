use super::ast::Statement;
use crate::error::Result;

mod cursor;
mod ddl;
mod mutation;
mod pattern;
mod predicate;
mod procedure;
mod projection;
mod query;
mod scalar;

pub fn parse(input: &str) -> Result<Statement> {
    let mut parser = Parser::new(input);
    let statement = parser.parse_statement()?;
    parser.consume_char(';');
    parser.expect_eof()?;
    Ok(statement)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementDispatch {
    Create,
    Alter,
    Merge,
    Match,
    Call,
    Checkpoint,
}

const TOP_LEVEL_STATEMENTS: &[(&str, StatementDispatch)] = &[
    ("CREATE", StatementDispatch::Create),
    ("ALTER", StatementDispatch::Alter),
    ("MERGE", StatementDispatch::Merge),
    ("MATCH", StatementDispatch::Match),
    ("CALL", StatementDispatch::Call),
    ("CHECKPOINT", StatementDispatch::Checkpoint),
];

pub(super) struct Parser<'a> {
    input: &'a str,
    pos: usize,
    anonymous_variable_id: usize,
}

impl<'a> Parser<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            anonymous_variable_id: 0,
        }
    }

    pub(super) fn parse_statement(&mut self) -> Result<Statement> {
        match self.parse_statement_dispatch()? {
            StatementDispatch::Create => self.parse_create_statement(),
            StatementDispatch::Alter => self.parse_alter_statement(),
            StatementDispatch::Merge => self.parse_merge_statement(),
            StatementDispatch::Match => self.parse_match_statement(),
            StatementDispatch::Call => self.parse_call_statement(),
            StatementDispatch::Checkpoint => Ok(Statement::Checkpoint),
        }
    }

    fn parse_statement_dispatch(&mut self) -> Result<StatementDispatch> {
        self.parse_keyword_choice(
            TOP_LEVEL_STATEMENTS,
            "expected CREATE, ALTER, MERGE, MATCH, CALL, or CHECKPOINT",
        )
    }

    pub(super) fn next_anonymous_variable(&mut self) -> String {
        let variable = format!("__anon{}", self.anonymous_variable_id);
        self.anonymous_variable_id += 1;
        variable
    }
}
