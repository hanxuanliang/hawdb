use std::fmt;

use crate::Span;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyntaxErrorCode {
    InputTooLarge,
    TokenLimitExceeded,
    CommentNestingLimitExceeded,
    ExpressionNestingLimitExceeded,
    GraphPatternNestingLimitExceeded,
    UnterminatedBlockComment,
    UnterminatedQuotedIdentifier,
    UnterminatedString,
    UnterminatedDollarQuotedString,
    InvalidParameter,
    InvalidCharacter,
    UnexpectedToken,
    UnexpectedEnd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxError {
    pub code: SyntaxErrorCode,
    pub span: Span,
    pub expected: Option<&'static str>,
    pub found: Option<String>,
}

impl SyntaxError {
    pub fn new(code: SyntaxErrorCode, span: Span) -> Self {
        Self {
            code,
            span,
            expected: None,
            found: None,
        }
    }

    pub fn expected(mut self, expected: &'static str) -> Self {
        self.expected = Some(expected);
        self
    }

    pub fn found(mut self, found: impl Into<String>) -> Self {
        self.found = Some(found.into());
        self
    }
}

impl fmt::Display for SyntaxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SQL syntax error {:?} at bytes {}..{}",
            self.code, self.span.start, self.span.end
        )?;
        if let Some(expected) = self.expected {
            write!(formatter, "; expected {expected}")?;
        }
        if let Some(found) = &self.found {
            write!(formatter, "; found {found}")?;
        }
        Ok(())
    }
}

impl std::error::Error for SyntaxError {}
