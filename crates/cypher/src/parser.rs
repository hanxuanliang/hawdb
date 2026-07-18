use super::ast::Statement;
use skein_core::Result;

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
    Begin,
    Create,
    Alter,
    Merge,
    Match,
    Call,
    Checkpoint,
    Commit,
    Rollback,
}

const TOP_LEVEL_STATEMENTS: &[(&str, StatementDispatch)] = &[
    ("BEGIN", StatementDispatch::Begin),
    ("CREATE", StatementDispatch::Create),
    ("ALTER", StatementDispatch::Alter),
    ("MERGE", StatementDispatch::Merge),
    ("MATCH", StatementDispatch::Match),
    ("CALL", StatementDispatch::Call),
    ("CHECKPOINT", StatementDispatch::Checkpoint),
    ("COMMIT", StatementDispatch::Commit),
    ("ROLLBACK", StatementDispatch::Rollback),
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
            StatementDispatch::Begin => {
                self.expect_keyword("TRANSACTION")?;
                Ok(Statement::BeginTransaction)
            }
            StatementDispatch::Create => self.parse_create_statement(),
            StatementDispatch::Alter => self.parse_alter_statement(),
            StatementDispatch::Merge => self.parse_merge_statement(),
            StatementDispatch::Match => self.parse_match_statement(),
            StatementDispatch::Call => self.parse_call_statement(),
            StatementDispatch::Checkpoint => Ok(Statement::Checkpoint),
            StatementDispatch::Commit => Ok(Statement::Commit),
            StatementDispatch::Rollback => Ok(Statement::Rollback),
        }
    }

    fn parse_statement_dispatch(&mut self) -> Result<StatementDispatch> {
        self.parse_keyword_choice(
            TOP_LEVEL_STATEMENTS,
            "expected BEGIN, CREATE, ALTER, MERGE, MATCH, CALL, CHECKPOINT, COMMIT, or ROLLBACK",
        )
    }

    pub(super) fn next_anonymous_variable(&mut self) -> String {
        let variable = format!("__anon{}", self.anonymous_variable_id);
        self.anonymous_variable_id += 1;
        variable
    }
}
