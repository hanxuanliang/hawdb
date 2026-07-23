use super::{PlanCacheStats, QueryOutput};
use crate::error::{Result, SkeinError};
use crate::executor::Row;
use crate::sql::{
    parse_postgres_sql, SelectProjection, SelectStatement, SqlColumnRef, SqlComparisonOp,
    SqlOrderDirection, SqlPredicate, SqlStatement,
};
use crate::value::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const DEFAULT_SLOW_QUERY_LOG_CAPACITY: usize = 256;
pub(crate) const DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MICROS: u128 = 300_000;
const MAX_SLOW_QUERY_TEXT_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlowQueryRecord {
    pub(crate) sequence: u64,
    pub(crate) query_language: String,
    pub(crate) query_text: String,
    pub(crate) started_unix_micros: i64,
    pub(crate) elapsed_micros: i64,
    pub(crate) row_count: i64,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
    pub(crate) slow_log_candidate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlowQueryLog {
    capacity: usize,
    next_sequence: u64,
    records: VecDeque<SlowQueryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqlLogicalPlan {
    SystemTableScan(SystemTableScan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqlPhysicalPlan {
    SystemTableScanExec(SystemTableScan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SystemTableScan {
    table: SystemTable,
    projection: Vec<SelectProjection>,
    predicate: Option<SqlPredicate>,
    order_by: Vec<crate::sql::SqlOrderItem>,
    offset: Option<u64>,
    limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemTable {
    PlanCache,
    SlowQueries,
}

impl SlowQueryRecord {
    pub(crate) fn completed(
        query_language: &str,
        query_text: &str,
        elapsed_micros: u128,
        row_count: usize,
        success: bool,
        error: Option<String>,
        slow_log_candidate: bool,
    ) -> Self {
        Self {
            sequence: 0,
            query_language: query_language.to_string(),
            query_text: truncate_utf8(query_text, MAX_SLOW_QUERY_TEXT_BYTES),
            started_unix_micros: unix_now_micros(),
            elapsed_micros: saturating_i64_from_u128(elapsed_micros),
            row_count: i64::try_from(row_count).unwrap_or(i64::MAX),
            success,
            error,
            slow_log_candidate,
        }
    }
}

impl SlowQueryLog {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_sequence: 1,
            records: VecDeque::with_capacity(capacity),
        }
    }

    pub(crate) fn push(&mut self, mut record: SlowQueryRecord) {
        if self.capacity == 0 {
            return;
        }
        record.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        while self.records.len() >= self.capacity {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }

    pub(crate) fn snapshot(&self) -> Vec<SlowQueryRecord> {
        self.records.iter().cloned().collect()
    }
}

pub(crate) fn query_sql(
    sql_text: &str,
    max_rows: Option<usize>,
    plan_cache_stats: &PlanCacheStats,
    slow_queries: &[SlowQueryRecord],
) -> Result<QueryOutput> {
    let logical = plan_sql(sql_text)?;
    let physical = optimize_sql(logical);
    let rows = execute_sql(physical, plan_cache_stats, slow_queries, max_rows)?;
    Ok(QueryOutput { rows })
}

fn plan_sql(sql_text: &str) -> Result<SqlLogicalPlan> {
    let SqlStatement::Select(select) = parse_postgres_sql(sql_text)?;
    let table = system_table(&select)?;
    validate_projection(table, &select.projection)?;
    validate_predicate_columns(table, select.selection.as_ref())?;
    validate_order_columns(table, &select.order_by)?;
    Ok(SqlLogicalPlan::SystemTableScan(SystemTableScan {
        table,
        projection: select.projection,
        predicate: select.selection,
        order_by: select.order_by,
        offset: select.offset,
        limit: select.limit,
    }))
}

fn optimize_sql(logical: SqlLogicalPlan) -> SqlPhysicalPlan {
    match logical {
        SqlLogicalPlan::SystemTableScan(scan) => SqlPhysicalPlan::SystemTableScanExec(scan),
    }
}

fn execute_sql(
    physical: SqlPhysicalPlan,
    plan_cache_stats: &PlanCacheStats,
    slow_queries: &[SlowQueryRecord],
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    match physical {
        SqlPhysicalPlan::SystemTableScanExec(scan) => {
            execute_system_table_scan(scan, plan_cache_stats, slow_queries, max_rows)
        }
    }
}

fn execute_system_table_scan(
    scan: SystemTableScan,
    plan_cache_stats: &PlanCacheStats,
    slow_queries: &[SlowQueryRecord],
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    let mut rows = match scan.table {
        SystemTable::PlanCache => plan_cache_rows(plan_cache_stats),
        SystemTable::SlowQueries => slow_query_rows(slow_queries),
    };

    if let Some(predicate) = &scan.predicate {
        rows.retain(|row| predicate_matches(predicate, row));
    }

    if !scan.order_by.is_empty() {
        rows.sort_by(|left, right| compare_ordered_rows(left, right, &scan.order_by));
    }

    let offset = scan.offset.unwrap_or(0);
    let limit = effective_limit(scan.limit, max_rows)?;
    rows = rows
        .into_iter()
        .skip(usize::try_from(offset).unwrap_or(usize::MAX))
        .take(limit.unwrap_or(usize::MAX))
        .collect();

    if let Some(max_rows) = max_rows {
        if rows.len() > max_rows {
            return Err(SkeinError::Execution(format!(
                "SQL query returned more than {max_rows} rows, exceeding max_read_result_rows {max_rows}"
            )));
        }
    }

    project_rows(rows, &scan.projection)
}

fn effective_limit(query_limit: Option<u64>, max_rows: Option<usize>) -> Result<Option<usize>> {
    let query_limit = query_limit
        .map(|limit| {
            usize::try_from(limit)
                .map_err(|_| SkeinError::Semantic("SQL LIMIT is too large".to_string()))
        })
        .transpose()?;
    Ok(match (query_limit, max_rows) {
        (Some(query_limit), Some(max_rows)) => Some(query_limit.min(max_rows.saturating_add(1))),
        (Some(query_limit), None) => Some(query_limit),
        (None, Some(max_rows)) => Some(max_rows.saturating_add(1)),
        (None, None) => None,
    })
}

fn project_rows(rows: Vec<Row>, projection: &[SelectProjection]) -> Result<Vec<Row>> {
    if projection
        .iter()
        .any(|projection| matches!(projection, SelectProjection::Wildcard))
    {
        return Ok(rows);
    }
    rows.into_iter()
        .map(|row| {
            projection
                .iter()
                .map(|projection| {
                    let SelectProjection::Column { name, alias } = projection else {
                        unreachable!("wildcard handled above");
                    };
                    let value = row.get(&name.name).cloned().unwrap_or(Value::Null);
                    Ok((alias.clone().unwrap_or_else(|| name.name.clone()), value))
                })
                .collect()
        })
        .collect()
}

fn plan_cache_rows(stats: &PlanCacheStats) -> Vec<Row> {
    [
        ("max_entries", option_usize_value(stats.max_entries)),
        ("entries", usize_value(stats.entries)),
        ("hits", u64_value(stats.hits)),
        ("misses", u64_value(stats.misses)),
        ("disabled_misses", u64_value(stats.disabled_misses)),
        ("bypasses", u64_value(stats.bypasses)),
        ("evictions", u64_value(stats.evictions)),
    ]
    .into_iter()
    .map(|(metric, value)| {
        BTreeMap::from([
            ("metric".to_string(), Value::String(metric.to_string())),
            ("value".to_string(), value),
        ])
    })
    .collect()
}

fn slow_query_rows(records: &[SlowQueryRecord]) -> Vec<Row> {
    records
        .iter()
        .map(|record| {
            BTreeMap::from([
                ("sequence".to_string(), u64_value(record.sequence)),
                (
                    "query_language".to_string(),
                    Value::String(record.query_language.clone()),
                ),
                (
                    "query_text".to_string(),
                    Value::String(record.query_text.clone()),
                ),
                (
                    "started_unix_micros".to_string(),
                    Value::Int(record.started_unix_micros),
                ),
                (
                    "elapsed_micros".to_string(),
                    Value::Int(record.elapsed_micros),
                ),
                ("row_count".to_string(), Value::Int(record.row_count)),
                ("success".to_string(), Value::Bool(record.success)),
                (
                    "error".to_string(),
                    record
                        .error
                        .as_ref()
                        .map(|error| Value::String(error.clone()))
                        .unwrap_or(Value::Null),
                ),
                (
                    "slow_log_candidate".to_string(),
                    Value::Bool(record.slow_log_candidate),
                ),
            ])
        })
        .collect()
}

fn predicate_matches(predicate: &SqlPredicate, row: &Row) -> bool {
    match predicate {
        SqlPredicate::And(left, right) => {
            predicate_matches(left, row) && predicate_matches(right, row)
        }
        SqlPredicate::Or(left, right) => {
            predicate_matches(left, row) || predicate_matches(right, row)
        }
        SqlPredicate::Not(inner) => !predicate_matches(inner, row),
        SqlPredicate::Compare { left, op, right } => row
            .get(&left.name)
            .is_some_and(|left_value| compare_values(left_value, *op, right)),
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => {
            let matched = row
                .get(&left.name)
                .is_some_and(|left_value| values.iter().any(|value| left_value == value));
            if *negated {
                !matched
            } else {
                matched
            }
        }
        SqlPredicate::IsNull { column, negated } => {
            let matched = row
                .get(&column.name)
                .is_none_or(|value| matches!(value, Value::Null));
            if *negated {
                !matched
            } else {
                matched
            }
        }
    }
}

fn compare_values(left: &Value, op: SqlComparisonOp, right: &Value) -> bool {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return false;
    }
    match op {
        SqlComparisonOp::Eq => left == right,
        SqlComparisonOp::NotEq => left != right,
        SqlComparisonOp::Lt => left < right,
        SqlComparisonOp::Lte => left <= right,
        SqlComparisonOp::Gt => left > right,
        SqlComparisonOp::Gte => left >= right,
    }
}

fn compare_ordered_rows(
    left: &Row,
    right: &Row,
    order_by: &[crate::sql::SqlOrderItem],
) -> Ordering {
    for item in order_by {
        let ordering = left
            .get(&item.column.name)
            .cmp(&right.get(&item.column.name));
        let ordering = match item.direction {
            SqlOrderDirection::Asc => ordering,
            SqlOrderDirection::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn system_table(select: &SelectStatement) -> Result<SystemTable> {
    match (select.from.schema.as_deref(), select.from.name.as_str()) {
        (Some("system"), "plan_cache") => Ok(SystemTable::PlanCache),
        (Some("system"), "slow_queries") => Ok(SystemTable::SlowQueries),
        _ => Err(SkeinError::Semantic(format!(
            "unknown SQL system table {}",
            format_table_name(select)
        ))),
    }
}

fn validate_projection(table: SystemTable, projection: &[SelectProjection]) -> Result<()> {
    for projection in projection {
        match projection {
            SelectProjection::Wildcard => {}
            SelectProjection::Column { name, .. } => validate_column(table, name)?,
        }
    }
    Ok(())
}

fn validate_predicate_columns(table: SystemTable, predicate: Option<&SqlPredicate>) -> Result<()> {
    let Some(predicate) = predicate else {
        return Ok(());
    };
    match predicate {
        SqlPredicate::And(left, right) | SqlPredicate::Or(left, right) => {
            validate_predicate_columns(table, Some(left))?;
            validate_predicate_columns(table, Some(right))
        }
        SqlPredicate::Not(inner) => validate_predicate_columns(table, Some(inner)),
        SqlPredicate::Compare { left, .. }
        | SqlPredicate::InList { left, .. }
        | SqlPredicate::IsNull { column: left, .. } => validate_column(table, left),
    }
}

fn validate_order_columns(table: SystemTable, order_by: &[crate::sql::SqlOrderItem]) -> Result<()> {
    for item in order_by {
        validate_column(table, &item.column)?;
    }
    Ok(())
}

fn validate_column(table: SystemTable, column: &SqlColumnRef) -> Result<()> {
    if let Some(qualifier) = &column.qualifier {
        let table_name = match table {
            SystemTable::PlanCache => "plan_cache",
            SystemTable::SlowQueries => "slow_queries",
        };
        if qualifier != table_name {
            return Err(SkeinError::Semantic(format!(
                "unknown SQL column qualifier {qualifier}"
            )));
        }
    }
    if table_columns(table).contains(&column.name.as_str()) {
        Ok(())
    } else {
        Err(SkeinError::Semantic(format!(
            "unknown SQL column {}",
            column.name
        )))
    }
}

fn table_columns(table: SystemTable) -> &'static [&'static str] {
    match table {
        SystemTable::PlanCache => &["metric", "value"],
        SystemTable::SlowQueries => &[
            "sequence",
            "query_language",
            "query_text",
            "started_unix_micros",
            "elapsed_micros",
            "row_count",
            "success",
            "error",
            "slow_log_candidate",
        ],
    }
}

fn format_table_name(select: &SelectStatement) -> String {
    match &select.from.schema {
        Some(schema) => format!("{schema}.{}", select.from.name),
        None => select.from.name.clone(),
    }
}

fn option_usize_value(value: Option<usize>) -> Value {
    value.map(usize_value).unwrap_or(Value::Null)
}

fn usize_value(value: usize) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn u64_value(value: u64) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn saturating_i64_from_u128(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unix_now_micros() -> i64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or(0);
    saturating_i64_from_u128(micros)
}

fn truncate_utf8(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }
    let mut end = max_bytes;
    while !input.is_char_boundary(end) {
        end -= 1;
    }
    input[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_plan_cache_virtual_table_with_predicate_and_projection() {
        let stats = PlanCacheStats {
            max_entries: Some(128),
            entries: 3,
            hits: 5,
            misses: 7,
            disabled_misses: 0,
            bypasses: 2,
            evictions: 1,
        };

        let output = query_sql(
            "SELECT value FROM system.plan_cache WHERE metric = 'hits'",
            None,
            &stats,
            &[],
        )
        .expect("system plan cache query");

        assert_eq!(
            output.rows,
            vec![BTreeMap::from([("value".to_string(), Value::Int(5))])]
        );
    }

    #[test]
    fn query_slow_queries_pushes_filter_order_and_limit_into_scan() {
        let records = vec![
            SlowQueryRecord {
                sequence: 1,
                query_language: "cypher".to_string(),
                query_text: "MATCH (m:Memory) RETURN m".to_string(),
                started_unix_micros: 10,
                elapsed_micros: 200,
                row_count: 1,
                success: true,
                error: None,
                slow_log_candidate: false,
            },
            SlowQueryRecord {
                sequence: 2,
                query_language: "cypher".to_string(),
                query_text: "MATCH (m:Memory) RETURN m ORDER BY m.id".to_string(),
                started_unix_micros: 20,
                elapsed_micros: 500,
                row_count: 2,
                success: true,
                error: None,
                slow_log_candidate: true,
            },
            SlowQueryRecord {
                sequence: 3,
                query_language: "cypher".to_string(),
                query_text: "MATCH (m:Memory {id: 'x'}) RETURN m".to_string(),
                started_unix_micros: 30,
                elapsed_micros: 300,
                row_count: 1,
                success: true,
                error: None,
                slow_log_candidate: true,
            },
        ];

        let output = query_sql(
            "SELECT sequence, elapsed_micros FROM system.slow_queries \
             WHERE slow_log_candidate = true \
             ORDER BY elapsed_micros DESC LIMIT 1",
            None,
            &PlanCacheStats {
                max_entries: None,
                entries: 0,
                hits: 0,
                misses: 0,
                disabled_misses: 0,
                bypasses: 0,
                evictions: 0,
            },
            &records,
        )
        .expect("system slow query scan");

        assert_eq!(
            output.rows,
            vec![BTreeMap::from([
                ("elapsed_micros".to_string(), Value::Int(500)),
                ("sequence".to_string(), Value::Int(2)),
            ])]
        );
    }
}
