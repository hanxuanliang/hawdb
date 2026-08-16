use crate::Span;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenKind {
    Word,
    QuotedIdentifier,
    String,
    Number,
    Parameter(u32),
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    LeftBrace,
    RightBrace,
    Comma,
    Dot,
    Semicolon,
    Colon,
    DoubleColon,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Equal,
    Less,
    Greater,
    LessOrEqual,
    GreaterOrEqual,
    NotEqual,
    Concat,
    ArrowLeft,
    ArrowRight,
    Pipe,
    Ampersand,
    Caret,
    Question,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

impl Token {
    pub const fn new(kind: TokenKind, span: Span) -> Self {
        Self { kind, span }
    }

    pub fn text(self, input: &str) -> &str {
        input
            .get(self.span.start..self.span.end)
            .unwrap_or_default()
    }

    pub fn is_keyword(self, input: &str, keyword: &str) -> bool {
        self.kind == TokenKind::Word && self.text(input).eq_ignore_ascii_case(keyword)
    }
}
