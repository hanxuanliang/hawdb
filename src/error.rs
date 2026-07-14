use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkeinError {
    Parse(String),
    Semantic(String),
    Storage(String),
    Execution(String),
}

impl Display for SkeinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SkeinError::Parse(message) => write!(f, "parse error: {message}"),
            SkeinError::Semantic(message) => write!(f, "semantic error: {message}"),
            SkeinError::Storage(message) => write!(f, "storage error: {message}"),
            SkeinError::Execution(message) => write!(f, "execution error: {message}"),
        }
    }
}

impl std::error::Error for SkeinError {}

pub type Result<T> = std::result::Result<T, SkeinError>;

impl From<std::io::Error> for SkeinError {
    fn from(error: std::io::Error) -> Self {
        SkeinError::Storage(error.to_string())
    }
}
