#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTableKind {
    Node,
    Relationship,
}

impl SchemaTableKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Relationship => "relationship",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPropertyType {
    Any,
    Bool,
    Int,
    Float,
    String,
    Text,
    List,
}

impl SchemaPropertyType {
    pub const fn fingerprint_suffix(self) -> &'static str {
        match self {
            Self::Any => ":any",
            Self::Bool => ":bool",
            Self::Int => ":int",
            Self::Float => ":float",
            Self::String => ":string",
            Self::Text => ":text",
            Self::List => ":list",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaObjectState {
    DeleteOnly,
    WriteOnly,
    Backfill,
    Validating,
    Public,
    Gc,
}

impl SchemaObjectState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeleteOnly => "delete_only",
            Self::WriteOnly => "write_only",
            Self::Backfill => "backfill",
            Self::Validating => "validating",
            Self::Public => "public",
            Self::Gc => "gc",
        }
    }
}
