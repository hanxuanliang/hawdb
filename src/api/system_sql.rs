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
pub(crate) const DEFAULT_STATEMENT_SUMMARY_CAPACITY: usize = 256;
const MAX_SLOW_QUERY_TEXT_BYTES: usize = 4096;
const MAX_STATEMENT_TEXT_BYTES: usize = 4096;
const MAX_STATEMENT_ERROR_BYTES: usize = 1024;

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
pub(crate) struct StatementExecution {
    pub(crate) query_language: String,
    pub(crate) query_text: String,
    pub(crate) statement_kind: String,
    pub(crate) elapsed_micros: i64,
    pub(crate) row_count: i64,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementSummaryRecord {
    pub(crate) digest: String,
    pub(crate) query_language: String,
    pub(crate) query_text: String,
    pub(crate) statement_kind: String,
    pub(crate) execution_count: i64,
    pub(crate) success_count: i64,
    pub(crate) error_count: i64,
    pub(crate) total_elapsed_micros: i64,
    pub(crate) max_elapsed_micros: i64,
    pub(crate) total_row_count: i64,
    pub(crate) last_seen_unix_micros: i64,
    pub(crate) last_elapsed_micros: i64,
    pub(crate) last_row_count: i64,
    pub(crate) last_success: bool,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementSummary {
    capacity: usize,
    records: BTreeMap<String, StatementSummaryRecord>,
    insertion_order: VecDeque<String>,
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
    StatementSummary,
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

pub(crate) fn slow_query_log_jsonl(
    records: &[SlowQueryRecord],
    include_query_text: bool,
) -> Result<String> {
    let mut jsonl = String::new();
    for record in records {
        let line = serde_json::to_string(&slow_query_record_json(record, include_query_text))
            .map_err(|error| {
                SkeinError::Execution(format!("slow query log JSON error: {error}"))
            })?;
        jsonl.push_str(&line);
        jsonl.push('\n');
    }
    Ok(jsonl)
}

pub(crate) fn slow_query_record_summary(
    record: &SlowQueryRecord,
) -> super::SlowQueryLogRecordSummary {
    super::SlowQueryLogRecordSummary {
        sequence: record.sequence,
        query_language: record.query_language.clone(),
        query_digest: statement_digest(&record.query_language, "unknown", &record.query_text),
        started_unix_micros: record.started_unix_micros,
        elapsed_micros: record.elapsed_micros,
        row_count: record.row_count,
        success: record.success,
        slow_log_candidate: record.slow_log_candidate,
    }
}

fn slow_query_record_json(record: &SlowQueryRecord, include_query_text: bool) -> serde_json::Value {
    let mut object = serde_json::json!({
        "protocol": super::SLOW_QUERY_LOG_EVENT_PROTOCOL,
        "protocol_version": 1,
        "sequence": record.sequence,
        "query_language": record.query_language,
        "query_digest": statement_digest(
            &record.query_language,
            "unknown",
            &record.query_text
        ),
        "started_unix_micros": record.started_unix_micros,
        "elapsed_micros": record.elapsed_micros,
        "row_count": record.row_count,
        "success": record.success,
        "slow_log_candidate": record.slow_log_candidate,
        "redaction": {
            "query_text_copied": include_query_text,
            "parameters_copied": false
        }
    });
    if include_query_text {
        object["query_text"] = serde_json::Value::String(record.query_text.clone());
    }
    if let Some(error) = &record.error {
        object["error"] =
            serde_json::Value::String(truncate_utf8(error, MAX_STATEMENT_ERROR_BYTES));
    }
    object
}

impl StatementExecution {
    pub(crate) fn completed(
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        elapsed_micros: u128,
        row_count: usize,
    ) -> Self {
        Self {
            query_language: query_language.to_string(),
            query_text: truncate_utf8(query_text, MAX_STATEMENT_TEXT_BYTES),
            statement_kind: statement_kind.to_string(),
            elapsed_micros: saturating_i64_from_u128(elapsed_micros),
            row_count: i64::try_from(row_count).unwrap_or(i64::MAX),
            success: true,
            error: None,
        }
    }

    pub(crate) fn failed(
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        elapsed_micros: u128,
        error: String,
    ) -> Self {
        Self {
            query_language: query_language.to_string(),
            query_text: truncate_utf8(query_text, MAX_STATEMENT_TEXT_BYTES),
            statement_kind: statement_kind.to_string(),
            elapsed_micros: saturating_i64_from_u128(elapsed_micros),
            row_count: 0,
            success: false,
            error: Some(truncate_utf8(&error, MAX_STATEMENT_ERROR_BYTES)),
        }
    }
}

impl StatementSummary {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            records: BTreeMap::new(),
            insertion_order: VecDeque::with_capacity(capacity),
        }
    }

    pub(crate) fn record(&mut self, execution: StatementExecution) {
        if self.capacity == 0 {
            return;
        }

        let digest = statement_digest(
            &execution.query_language,
            &execution.statement_kind,
            &execution.query_text,
        );
        if let Some(record) = self.records.get_mut(&digest) {
            record.apply(execution);
            return;
        }

        while self.records.len() >= self.capacity {
            let Some(evicted) = self.insertion_order.pop_front() else {
                break;
            };
            self.records.remove(&evicted);
        }

        self.insertion_order.push_back(digest.clone());
        self.records.insert(
            digest.clone(),
            StatementSummaryRecord::from_execution(digest, execution),
        );
    }

    pub(crate) fn snapshot(&self) -> Vec<StatementSummaryRecord> {
        self.insertion_order
            .iter()
            .filter_map(|digest| self.records.get(digest).cloned())
            .collect()
    }
}

impl StatementSummaryRecord {
    fn from_execution(digest: String, execution: StatementExecution) -> Self {
        let now = unix_now_micros();
        let success_count = i64::from(execution.success);
        let error_count = i64::from(!execution.success);
        Self {
            digest,
            query_language: execution.query_language,
            query_text: execution.query_text,
            statement_kind: execution.statement_kind,
            execution_count: 1,
            success_count,
            error_count,
            total_elapsed_micros: execution.elapsed_micros,
            max_elapsed_micros: execution.elapsed_micros,
            total_row_count: execution.row_count,
            last_seen_unix_micros: now,
            last_elapsed_micros: execution.elapsed_micros,
            last_row_count: execution.row_count,
            last_success: execution.success,
            last_error: execution.error,
        }
    }

    fn apply(&mut self, execution: StatementExecution) {
        self.execution_count = self.execution_count.saturating_add(1);
        if execution.success {
            self.success_count = self.success_count.saturating_add(1);
        } else {
            self.error_count = self.error_count.saturating_add(1);
        }
        self.total_elapsed_micros = self
            .total_elapsed_micros
            .saturating_add(execution.elapsed_micros);
        self.max_elapsed_micros = self.max_elapsed_micros.max(execution.elapsed_micros);
        self.total_row_count = self.total_row_count.saturating_add(execution.row_count);
        self.last_seen_unix_micros = unix_now_micros();
        self.last_elapsed_micros = execution.elapsed_micros;
        self.last_row_count = execution.row_count;
        self.last_success = execution.success;
        self.last_error = execution.error;
    }
}

pub(crate) fn query_sql(
    sql_text: &str,
    max_rows: Option<usize>,
    plan_cache_stats: &PlanCacheStats,
    slow_queries: &[SlowQueryRecord],
    statement_summaries: &[StatementSummaryRecord],
) -> Result<QueryOutput> {
    let logical = plan_sql(sql_text)?;
    let physical = optimize_sql(logical);
    let rows = execute_sql(
        physical,
        plan_cache_stats,
        slow_queries,
        statement_summaries,
        max_rows,
    )?;
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
    statement_summaries: &[StatementSummaryRecord],
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    match physical {
        SqlPhysicalPlan::SystemTableScanExec(scan) => execute_system_table_scan(
            scan,
            plan_cache_stats,
            slow_queries,
            statement_summaries,
            max_rows,
        ),
    }
}

fn execute_system_table_scan(
    scan: SystemTableScan,
    plan_cache_stats: &PlanCacheStats,
    slow_queries: &[SlowQueryRecord],
    statement_summaries: &[StatementSummaryRecord],
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    let mut rows = match scan.table {
        SystemTable::PlanCache => plan_cache_rows(plan_cache_stats),
        SystemTable::SlowQueries => slow_query_rows(slow_queries),
        SystemTable::StatementSummary => statement_summary_rows(statement_summaries),
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
        ("admissions", u64_value(stats.admissions)),
        ("disabled_misses", u64_value(stats.disabled_misses)),
        ("bypasses", u64_value(stats.bypasses)),
        ("evictions", u64_value(stats.evictions)),
        (
            "memory_pressure_events",
            u64_value(stats.memory_pressure_events),
        ),
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

fn statement_summary_rows(records: &[StatementSummaryRecord]) -> Vec<Row> {
    records
        .iter()
        .map(|record| {
            BTreeMap::from([
                ("digest".to_string(), Value::String(record.digest.clone())),
                (
                    "query_language".to_string(),
                    Value::String(record.query_language.clone()),
                ),
                (
                    "query_text".to_string(),
                    Value::String(record.query_text.clone()),
                ),
                (
                    "statement_kind".to_string(),
                    Value::String(record.statement_kind.clone()),
                ),
                (
                    "execution_count".to_string(),
                    Value::Int(record.execution_count),
                ),
                (
                    "success_count".to_string(),
                    Value::Int(record.success_count),
                ),
                ("error_count".to_string(), Value::Int(record.error_count)),
                (
                    "total_elapsed_micros".to_string(),
                    Value::Int(record.total_elapsed_micros),
                ),
                (
                    "max_elapsed_micros".to_string(),
                    Value::Int(record.max_elapsed_micros),
                ),
                (
                    "avg_elapsed_micros".to_string(),
                    Value::Int(avg_i64(record.total_elapsed_micros, record.execution_count)),
                ),
                (
                    "total_row_count".to_string(),
                    Value::Int(record.total_row_count),
                ),
                (
                    "last_seen_unix_micros".to_string(),
                    Value::Int(record.last_seen_unix_micros),
                ),
                (
                    "last_elapsed_micros".to_string(),
                    Value::Int(record.last_elapsed_micros),
                ),
                (
                    "last_row_count".to_string(),
                    Value::Int(record.last_row_count),
                ),
                ("last_success".to_string(), Value::Bool(record.last_success)),
                (
                    "last_error".to_string(),
                    record
                        .last_error
                        .as_ref()
                        .map(|error| Value::String(error.clone()))
                        .unwrap_or(Value::Null),
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
        (Some("system"), "statement_summary") => Ok(SystemTable::StatementSummary),
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
            SystemTable::StatementSummary => "statement_summary",
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
        SystemTable::StatementSummary => &[
            "digest",
            "query_language",
            "query_text",
            "statement_kind",
            "execution_count",
            "success_count",
            "error_count",
            "total_elapsed_micros",
            "max_elapsed_micros",
            "avg_elapsed_micros",
            "total_row_count",
            "last_seen_unix_micros",
            "last_elapsed_micros",
            "last_row_count",
            "last_success",
            "last_error",
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

fn avg_i64(total: i64, count: i64) -> i64 {
    if count <= 0 {
        0
    } else {
        total / count
    }
}

fn saturating_i64_from_u128(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn statement_digest(query_language: &str, statement_kind: &str, query_text: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in query_language
        .bytes()
        .chain([0xff])
        .chain(statement_kind.bytes())
        .chain([0xfe])
        .chain(query_text.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
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
            admissions: 4,
            disabled_misses: 0,
            bypasses: 2,
            evictions: 1,
            memory_pressure_events: 1,
        };

        let output = query_sql(
            "SELECT value FROM system.plan_cache WHERE metric = 'hits'",
            None,
            &stats,
            &[],
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
                admissions: 0,
                disabled_misses: 0,
                bypasses: 0,
                evictions: 0,
                memory_pressure_events: 0,
            },
            &records,
            &[],
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

#[cfg(all(test, feature = "loom-tests"))]
pub(crate) mod loom_tests {
    use super::{SlowQueryLog, SlowQueryRecord};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    #[test]
    fn slow_query_ring_preserves_bounds_under_modeled_concurrent_access() {
        loom::model(|| {
            let log = Arc::new(Mutex::new(SlowQueryLog::new(2)));

            let first_writer = spawn_slow_query_writer(Arc::clone(&log), "first");
            let second_writer = spawn_slow_query_writer(Arc::clone(&log), "second");
            let snapshotter = {
                let log = Arc::clone(&log);
                thread::spawn(move || {
                    let snapshot = log.lock().unwrap().snapshot();
                    assert_snapshot_invariants(&snapshot, 2);
                })
            };

            first_writer.join().unwrap();
            second_writer.join().unwrap();
            snapshotter.join().unwrap();

            let snapshot = log.lock().unwrap().snapshot();
            assert_snapshot_invariants(&snapshot, 2);
            assert_eq!(snapshot.len(), 2);
            assert_eq!(snapshot.last().map(|record| record.sequence), Some(2));
        });
    }

    fn spawn_slow_query_writer(
        log: Arc<Mutex<SlowQueryLog>>,
        query_text: &'static str,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            log.lock().unwrap().push(SlowQueryRecord::completed(
                "cypher", query_text, 1, 1, true, None, true,
            ));
        })
    }

    fn assert_snapshot_invariants(snapshot: &[SlowQueryRecord], capacity: usize) {
        assert!(snapshot.len() <= capacity);
        for pair in snapshot.windows(2) {
            assert!(pair[0].sequence < pair[1].sequence);
        }
    }
}
