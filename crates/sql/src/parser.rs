use skein_core::{Result, SkeinError, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, LimitClause, ObjectName, ObjectNamePart, OrderByKind,
    SelectItem as ParserSelectItem, SetExpr, Statement as ParserStatement, TableFactor,
    Value as ParserValue, ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlStatement {
    Select(SelectStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub projection: Vec<SelectProjection>,
    pub from: SqlTableName,
    pub selection: Option<SqlPredicate>,
    pub order_by: Vec<SqlOrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlTableName {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjection {
    Wildcard,
    Column {
        name: SqlColumnRef,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlColumnRef {
    pub qualifier: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlOrderItem {
    pub column: SqlColumnRef,
    pub direction: SqlOrderDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlOrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlPredicate {
    And(Box<SqlPredicate>, Box<SqlPredicate>),
    Or(Box<SqlPredicate>, Box<SqlPredicate>),
    Not(Box<SqlPredicate>),
    Compare {
        left: SqlColumnRef,
        op: SqlComparisonOp,
        right: Value,
    },
    InList {
        left: SqlColumnRef,
        values: Vec<Value>,
        negated: bool,
    },
    IsNull {
        column: SqlColumnRef,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlComparisonOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

pub fn parse_postgres_sql(input: &str) -> Result<SqlStatement> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, input)
        .map_err(|error| SkeinError::Parse(format!("failed to parse PostgreSQL SQL: {error}")))?;
    let [statement] = statements.as_slice() else {
        return Err(SkeinError::Parse(
            "expected exactly one PostgreSQL SQL statement".to_string(),
        ));
    };
    lower_statement(statement)
}

fn lower_statement(statement: &ParserStatement) -> Result<SqlStatement> {
    let ParserStatement::Query(query) = statement else {
        return Err(SkeinError::Semantic(
            "only PostgreSQL SELECT statements are supported".to_string(),
        ));
    };
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL SELECT clause".to_string(),
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SkeinError::Semantic(
            "set operations and nested queries are not supported".to_string(),
        ));
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.prewhere.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL SELECT feature".to_string(),
        ));
    }
    if select.from.len() != 1 {
        return Err(SkeinError::Semantic(
            "PostgreSQL SELECT currently supports exactly one FROM item".to_string(),
        ));
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err(SkeinError::Semantic(
            "PostgreSQL SELECT joins are not supported yet".to_string(),
        ));
    }
    let TableFactor::Table { name, alias, .. } = &from.relation else {
        return Err(SkeinError::Semantic(
            "PostgreSQL SELECT currently supports base tables only".to_string(),
        ));
    };
    if alias.is_some() {
        return Err(SkeinError::Semantic(
            "PostgreSQL SELECT table aliases are not supported yet".to_string(),
        ));
    }

    Ok(SqlStatement::Select(SelectStatement {
        projection: lower_projection(&select.projection)?,
        from: lower_table_name(name)?,
        selection: select.selection.as_ref().map(lower_predicate).transpose()?,
        order_by: lower_order_by(query.order_by.as_ref())?,
        limit: lower_limit(query.limit_clause.as_ref())?,
        offset: lower_offset(query.limit_clause.as_ref())?,
    }))
}

fn lower_projection(items: &[ParserSelectItem]) -> Result<Vec<SelectProjection>> {
    items
        .iter()
        .map(|item| match item {
            ParserSelectItem::Wildcard(_) => Ok(SelectProjection::Wildcard),
            ParserSelectItem::UnnamedExpr(expr) => Ok(SelectProjection::Column {
                name: lower_column_expr(expr)?,
                alias: None,
            }),
            ParserSelectItem::ExprWithAlias { expr, alias } => Ok(SelectProjection::Column {
                name: lower_column_expr(expr)?,
                alias: Some(normalize_ident(alias)),
            }),
            ParserSelectItem::QualifiedWildcard(_, _) => Err(SkeinError::Semantic(
                "qualified wildcards are not supported yet".to_string(),
            )),
        })
        .collect()
}

fn lower_order_by(order_by: Option<&sqlparser::ast::OrderBy>) -> Result<Vec<SqlOrderItem>> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(SkeinError::Semantic(
            "ORDER BY ALL is not supported".to_string(),
        ));
    };
    expressions
        .iter()
        .map(|item| {
            if item.options.nulls_first.is_some() {
                return Err(SkeinError::Semantic(
                    "ORDER BY NULLS FIRST/LAST is not supported yet".to_string(),
                ));
            }
            Ok(SqlOrderItem {
                column: lower_column_expr(&item.expr)?,
                direction: match item.options.asc {
                    Some(false) => SqlOrderDirection::Desc,
                    Some(true) | None => SqlOrderDirection::Asc,
                },
            })
        })
        .collect()
}

fn lower_limit(limit_clause: Option<&LimitClause>) -> Result<Option<u64>> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };
    match limit_clause {
        LimitClause::LimitOffset { limit, .. } => limit
            .as_ref()
            .map(lower_nonnegative_integer_expr)
            .transpose(),
        LimitClause::OffsetCommaLimit { limit, .. } => {
            Ok(Some(lower_nonnegative_integer_expr(limit)?))
        }
    }
}

fn lower_offset(limit_clause: Option<&LimitClause>) -> Result<Option<u64>> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };
    match limit_clause {
        LimitClause::LimitOffset { offset, .. } => offset
            .as_ref()
            .map(|offset| lower_nonnegative_integer_expr(&offset.value))
            .transpose(),
        LimitClause::OffsetCommaLimit { offset, .. } => {
            Ok(Some(lower_nonnegative_integer_expr(offset)?))
        }
    }
}

fn lower_predicate(expr: &Expr) -> Result<SqlPredicate> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(SqlPredicate::And(
                Box::new(lower_predicate(left)?),
                Box::new(lower_predicate(right)?),
            )),
            BinaryOperator::Or => Ok(SqlPredicate::Or(
                Box::new(lower_predicate(left)?),
                Box::new(lower_predicate(right)?),
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => Ok(SqlPredicate::Compare {
                left: lower_column_expr(left)?,
                op: lower_comparison_op(op),
                right: lower_literal_expr(right)?,
            }),
            _ => Err(SkeinError::Semantic(format!(
                "unsupported PostgreSQL predicate operator {op}"
            ))),
        },
        Expr::Nested(inner) => lower_predicate(inner),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(SqlPredicate::Not(Box::new(lower_predicate(expr)?))),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(SqlPredicate::InList {
            left: lower_column_expr(expr)?,
            values: list
                .iter()
                .map(lower_literal_expr)
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        Expr::IsNull(expr) => Ok(SqlPredicate::IsNull {
            column: lower_column_expr(expr)?,
            negated: false,
        }),
        Expr::IsNotNull(expr) => Ok(SqlPredicate::IsNull {
            column: lower_column_expr(expr)?,
            negated: true,
        }),
        _ => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL predicate expression {expr}"
        ))),
    }
}

fn lower_comparison_op(op: &BinaryOperator) -> SqlComparisonOp {
    match op {
        BinaryOperator::Eq => SqlComparisonOp::Eq,
        BinaryOperator::NotEq => SqlComparisonOp::NotEq,
        BinaryOperator::Lt => SqlComparisonOp::Lt,
        BinaryOperator::LtEq => SqlComparisonOp::Lte,
        BinaryOperator::Gt => SqlComparisonOp::Gt,
        BinaryOperator::GtEq => SqlComparisonOp::Gte,
        _ => unreachable!("checked by caller"),
    }
}

fn lower_table_name(name: &ObjectName) -> Result<SqlTableName> {
    let parts = object_name_parts(name)?;
    match parts.as_slice() {
        [name] => Ok(SqlTableName {
            schema: None,
            name: name.clone(),
        }),
        [schema, name] => Ok(SqlTableName {
            schema: Some(schema.clone()),
            name: name.clone(),
        }),
        _ => Err(SkeinError::Semantic(
            "PostgreSQL SELECT currently supports one- or two-part table names".to_string(),
        )),
    }
}

fn lower_column_expr(expr: &Expr) -> Result<SqlColumnRef> {
    match expr {
        Expr::Identifier(ident) => Ok(SqlColumnRef {
            qualifier: None,
            name: normalize_ident(ident),
        }),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [qualifier, name] => Ok(SqlColumnRef {
                qualifier: Some(normalize_ident(qualifier)),
                name: normalize_ident(name),
            }),
            _ => Err(SkeinError::Semantic(
                "PostgreSQL SELECT currently supports one- or two-part column names".to_string(),
            )),
        },
        _ => Err(SkeinError::Semantic(format!(
            "expected a column reference, got {expr}"
        ))),
    }
}

fn lower_literal_expr(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(value) => lower_value(value),
        Expr::Nested(inner) => lower_literal_expr(inner),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match lower_literal_expr(expr)? {
            Value::Int(value) => Ok(Value::Int(-value)),
            Value::Float(value) => Ok(Value::Float(-value)),
            value => Err(SkeinError::Semantic(format!(
                "cannot negate literal value {value}"
            ))),
        },
        _ => Err(SkeinError::Semantic(format!(
            "expected a literal value, got {expr}"
        ))),
    }
}

fn lower_value(value: &ValueWithSpan) -> Result<Value> {
    match &value.value {
        ParserValue::Boolean(value) => Ok(Value::Bool(*value)),
        ParserValue::Null => Ok(Value::Null),
        ParserValue::Number(raw, _) => lower_number(raw),
        ParserValue::SingleQuotedString(value)
        | ParserValue::DoubleQuotedString(value)
        | ParserValue::TripleSingleQuotedString(value)
        | ParserValue::TripleDoubleQuotedString(value)
        | ParserValue::EscapedStringLiteral(value)
        | ParserValue::UnicodeStringLiteral(value) => Ok(Value::String(value.clone())),
        _ => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL literal {value}"
        ))),
    }
}

fn lower_number(raw: &str) -> Result<Value> {
    if raw.contains('.') {
        raw.parse::<f64>().map(Value::Float).map_err(|error| {
            SkeinError::Semantic(format!("invalid PostgreSQL number {raw}: {error}"))
        })
    } else {
        raw.parse::<i64>().map(Value::Int).map_err(|error| {
            SkeinError::Semantic(format!("invalid PostgreSQL integer {raw}: {error}"))
        })
    }
}

fn lower_nonnegative_integer_expr(expr: &Expr) -> Result<u64> {
    let value = lower_literal_expr(expr)?;
    match value {
        Value::Int(value) if value >= 0 => Ok(value as u64),
        _ => Err(SkeinError::Semantic(
            "LIMIT/OFFSET must be non-negative integer literals".to_string(),
        )),
    }
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Ok(normalize_ident(ident)),
            _ => Err(SkeinError::Semantic(
                "object name functions are not supported".to_string(),
            )),
        })
        .collect()
}

fn normalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}
