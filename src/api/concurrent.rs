use super::transaction_locks::{LockMode, LockRequest, LockTable, WaitForGraph};
use super::{
    commit_database_transaction_state, execute_database_transaction_query,
    execute_database_transaction_sql, statement_body, Database, DatabaseConfig,
    DatabaseReadTransaction, DatabaseTransactionRuntime, DatabaseTransactionState, QueryOutput,
};
use crate::error::{Result, SkeinError};
use crate::sql::{
    SelectStatement, SqlComparisonOp, SqlPredicate, SqlStatement, SqlTableName, SqlValue,
};
use crate::store::DurabilityPolicy;
use crate::value::Value;
use skein_storage::{
    RelationalConflictAction, RelationalKey, RelationalRow, RelationalState, RelationalTableSchema,
    RelationalTransaction, RelationalValue, RelationalWrite,
};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub const DEFAULT_PESSIMISTIC_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConcurrentTransactionMode {
    Optimistic,
    #[default]
    Pessimistic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrentTransactionOptions {
    pub mode: ConcurrentTransactionMode,
    pub lock_timeout: Duration,
}

impl ConcurrentTransactionOptions {
    pub const fn optimistic() -> Self {
        Self {
            mode: ConcurrentTransactionMode::Optimistic,
            lock_timeout: DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
        }
    }

    pub const fn pessimistic(lock_timeout: Duration) -> Self {
        Self {
            mode: ConcurrentTransactionMode::Pessimistic,
            lock_timeout,
        }
    }
}

impl Default for ConcurrentTransactionOptions {
    fn default() -> Self {
        Self::pessimistic(DEFAULT_PESSIMISTIC_LOCK_TIMEOUT)
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentDatabase {
    inner: Arc<ConcurrentDatabaseInner>,
}

#[derive(Debug)]
struct ConcurrentDatabaseInner {
    state: Mutex<ConcurrentDatabaseState>,
    checkpoint_serial: Mutex<()>,
    lock_available: Condvar,
}

#[derive(Debug)]
struct ConcurrentDatabaseState {
    database: Database,
    next_transaction_id: u64,
    locks: LockTable,
    wait_for: WaitForGraph,
}

#[derive(Debug)]
pub struct ConcurrentDatabaseTransaction {
    inner: Arc<ConcurrentDatabaseInner>,
    transaction_id: u64,
    base_commit_epoch: u64,
    options: ConcurrentTransactionOptions,
    runtime: DatabaseTransactionRuntime,
    state: DatabaseTransactionState,
    successful_statements: usize,
    abort_reason: Option<String>,
    finished: bool,
}

impl ConcurrentDatabase {
    pub fn new(database: Database) -> Self {
        Self {
            inner: Arc::new(ConcurrentDatabaseInner {
                state: Mutex::new(ConcurrentDatabaseState {
                    database,
                    next_transaction_id: 1,
                    locks: LockTable::default(),
                    wait_for: WaitForGraph::default(),
                }),
                checkpoint_serial: Mutex::new(()),
                lock_available: Condvar::new(),
            }),
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Database::open(path).map(Self::new)
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        Database::open_with_config(path, config).map(Self::new)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        Database::open_with_durability(path, durability).map(Self::new)
    }

    pub fn open_with_durability_and_config(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
        config: DatabaseConfig,
    ) -> Result<Self> {
        Database::open_with_durability_and_config(path, durability, config).map(Self::new)
    }

    pub fn commit_epoch(&self) -> Result<u64> {
        Ok(self.lock_state()?.database.commit_epoch())
    }

    pub fn begin_read_transaction(&self) -> Result<DatabaseReadTransaction> {
        Ok(self.lock_state()?.database.begin_read_transaction())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _checkpoint_serial = self
            .inner
            .checkpoint_serial
            .lock()
            .map_err(|_| coordinator_poisoned_error())?;
        let source = self.lock_state()?.database.checkpoint_source()?;
        let prepared = source.prepare()?;
        let Some(prepared) = prepared else {
            return Ok(());
        };
        self.lock_state()?
            .database
            .publish_prepared_checkpoint(prepared)
    }

    pub fn begin_transaction(
        &self,
        options: ConcurrentTransactionOptions,
    ) -> Result<ConcurrentDatabaseTransaction> {
        let mut coordinator = self.lock_state()?;
        let transaction_id = coordinator.allocate_transaction_id()?;
        let base_commit_epoch = coordinator.database.commit_epoch();
        Ok(ConcurrentDatabaseTransaction {
            inner: Arc::clone(&self.inner),
            transaction_id,
            base_commit_epoch,
            options,
            runtime: DatabaseTransactionRuntime::from_database(&coordinator.database),
            state: DatabaseTransactionState::from_database(&coordinator.database),
            successful_statements: 0,
            abort_reason: None,
            finished: false,
        })
    }

    pub fn query(&self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.with_autocommit_exclusive(|database| {
            database.query_with_params(cypher_text, parameters)
        })
    }

    pub fn query_sql(&self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.with_autocommit_exclusive(|database| {
            database.query_sql_with_params(sql_text, parameters)
        })
    }

    fn with_autocommit_exclusive(
        &self,
        execute: impl FnOnce(&mut Database) -> Result<QueryOutput>,
    ) -> Result<QueryOutput> {
        let mut coordinator = self.lock_state()?;
        let transaction_id = coordinator.allocate_transaction_id()?;
        coordinator = self.inner.acquire_requests(
            coordinator,
            transaction_id,
            &[LockRequest::database(LockMode::Exclusive)],
            Instant::now(),
            DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
        )?;
        let result = execute(&mut coordinator.database);
        coordinator.release_transaction(transaction_id);
        drop(coordinator);
        self.inner.lock_available.notify_all();
        result
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, ConcurrentDatabaseState>> {
        self.inner
            .state
            .lock()
            .map_err(|_| coordinator_poisoned_error())
    }
}

impl ConcurrentDatabaseInner {
    fn acquire_requests<'a>(
        &'a self,
        mut coordinator: MutexGuard<'a, ConcurrentDatabaseState>,
        transaction_id: u64,
        requests: &[LockRequest],
        started: Instant,
        timeout: Duration,
    ) -> Result<MutexGuard<'a, ConcurrentDatabaseState>> {
        let mut requests = requests.to_vec();
        let mut unique_requests = Vec::with_capacity(requests.len());
        for request in requests.drain(..) {
            if !unique_requests.contains(&request) {
                unique_requests.push(request);
            }
        }
        for request in unique_requests {
            loop {
                let blockers = coordinator.locks.blockers(transaction_id, &request);
                if blockers.is_empty() {
                    coordinator.wait_for.clear_waiter(transaction_id);
                    coordinator.locks.grant(transaction_id, request.clone());
                    break;
                }
                coordinator.wait_for.register(transaction_id, &blockers)?;
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    coordinator.wait_for.clear_waiter(transaction_id);
                    return Err(lock_timeout_error(timeout));
                }
                let waited = self.lock_available.wait_timeout(coordinator, remaining);
                let (next, wait) = match waited {
                    Ok(waited) => waited,
                    Err(poisoned) => {
                        let (mut recovered, _) = poisoned.into_inner();
                        recovered.wait_for.clear_waiter(transaction_id);
                        return Err(coordinator_poisoned_error());
                    }
                };
                coordinator = next;
                coordinator.wait_for.clear_waiter(transaction_id);
                if wait.timed_out() {
                    return Err(lock_timeout_error(timeout));
                }
            }
        }
        Ok(coordinator)
    }
}

impl ConcurrentDatabaseState {
    fn allocate_transaction_id(&mut self) -> Result<u64> {
        let transaction_id = self.next_transaction_id;
        self.next_transaction_id = transaction_id.checked_add(1).ok_or_else(|| {
            SkeinError::Execution("concurrent transaction id space is exhausted".to_string())
        })?;
        Ok(transaction_id)
    }

    fn release_transaction(&mut self, transaction_id: u64) {
        self.locks.release_transaction(transaction_id);
        self.wait_for.remove_transaction(transaction_id);
    }
}

impl ConcurrentDatabaseTransaction {
    pub fn transaction_id(&self) -> u64 {
        self.transaction_id
    }

    pub fn base_commit_epoch(&self) -> u64 {
        self.base_commit_epoch
    }

    pub fn mode(&self) -> ConcurrentTransactionMode {
        self.options.mode
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.ensure_active()?;
        if self.options.mode == ConcurrentTransactionMode::Pessimistic {
            let statement = crate::cypher::parse(cypher_text)?;
            let mode = if cypher_statement_is_read_only(statement_body(&statement)) {
                LockMode::Shared
            } else {
                LockMode::Exclusive
            };
            self.acquire_locks(&[LockRequest::database(mode)])?;
        }
        let result = execute_database_transaction_query(
            &self.runtime,
            &mut self.state,
            cypher_text,
            parameters,
        );
        if result.is_ok() {
            self.successful_statements = self.successful_statements.saturating_add(1);
        }
        result
    }

    pub fn query_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.ensure_active()?;
        if self.options.mode == ConcurrentTransactionMode::Pessimistic {
            let requests = sql_lock_requests(sql_text, parameters, &self.state.relational_state)?;
            self.acquire_locks(&requests)?;
        }
        let result =
            execute_database_transaction_sql(&self.runtime, &mut self.state, sql_text, parameters);
        if result.is_ok() {
            self.successful_statements = self.successful_statements.saturating_add(1);
        }
        result
    }

    pub fn commit(mut self) -> Result<QueryOutput> {
        self.ensure_active()?;
        let started = Instant::now();
        let mut coordinator = self
            .inner
            .state
            .lock()
            .map_err(|_| coordinator_poisoned_error())?;
        if self.options.mode == ConcurrentTransactionMode::Optimistic {
            coordinator = match self.inner.acquire_requests(
                coordinator,
                self.transaction_id,
                &[LockRequest::database(LockMode::Exclusive)],
                started,
                self.options.lock_timeout,
            ) {
                Ok(coordinator) => coordinator,
                Err(error) => {
                    self.finished = true;
                    return Err(error);
                }
            };
        }

        let allow_stale_rebase = self.options.mode == ConcurrentTransactionMode::Pessimistic;
        let result = commit_database_transaction_state(
            &mut coordinator.database,
            &mut self.state,
            allow_stale_rebase,
        )
        .map_err(|error| self.map_commit_error(error));
        coordinator.release_transaction(self.transaction_id);
        self.finished = true;
        drop(coordinator);
        self.inner.lock_available.notify_all();
        result
    }

    pub fn rollback(mut self) {
        self.state.rollback();
        self.finish_without_commit();
    }

    fn acquire_locks(&mut self, requests: &[LockRequest]) -> Result<()> {
        let started = Instant::now();
        let inner = Arc::clone(&self.inner);
        let coordinator = inner
            .state
            .lock()
            .map_err(|_| coordinator_poisoned_error())?;
        let requests_already_covered = coordinator.locks.covers_all(self.transaction_id, requests);
        let acquired = inner.acquire_requests(
            coordinator,
            self.transaction_id,
            requests,
            started,
            self.options.lock_timeout,
        );
        let mut coordinator = match acquired {
            Ok(coordinator) => coordinator,
            Err(error) => {
                self.abort_after_lock_failure(error.to_string());
                return Err(error);
            }
        };

        let current_epoch = coordinator.database.commit_epoch();
        if current_epoch != self.base_commit_epoch && !requests_already_covered {
            if self.successful_statements == 0 {
                self.base_commit_epoch = current_epoch;
                self.runtime = DatabaseTransactionRuntime::from_database(&coordinator.database);
                self.state = DatabaseTransactionState::from_database(&coordinator.database);
            } else {
                let error = SkeinError::Execution(format!(
                    "pessimistic transaction {} cannot acquire a new lock after its snapshot changed from commit epoch {} to {}; retry the transaction",
                    self.transaction_id, self.base_commit_epoch, current_epoch
                ));
                coordinator.release_transaction(self.transaction_id);
                self.state.rollback();
                self.abort_reason = Some(error.to_string());
                drop(coordinator);
                self.inner.lock_available.notify_all();
                return Err(error);
            }
        }
        Ok(())
    }

    fn abort_after_lock_failure(&mut self, reason: String) {
        self.state.rollback();
        self.abort_reason = Some(reason);
        if let Ok(mut coordinator) = self.inner.state.lock() {
            coordinator.release_transaction(self.transaction_id);
            drop(coordinator);
            self.inner.lock_available.notify_all();
        }
    }

    fn ensure_active(&self) -> Result<()> {
        if self.finished {
            return Err(SkeinError::Execution(format!(
                "concurrent transaction {} is already finished",
                self.transaction_id
            )));
        }
        if let Some(reason) = &self.abort_reason {
            return Err(SkeinError::Execution(format!(
                "concurrent transaction {} is aborted: {reason}",
                self.transaction_id
            )));
        }
        Ok(())
    }

    fn map_commit_error(&self, error: SkeinError) -> SkeinError {
        if self.options.mode == ConcurrentTransactionMode::Optimistic
            && error.to_string().contains("transaction snapshot is stale")
        {
            return SkeinError::Execution(format!(
                "optimistic transaction conflict for transaction {}: {}",
                self.transaction_id, error
            ));
        }
        error
    }

    fn finish_without_commit(&mut self) {
        if self.finished {
            return;
        }
        let mut coordinator = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        coordinator.release_transaction(self.transaction_id);
        self.finished = true;
        drop(coordinator);
        self.inner.lock_available.notify_all();
    }
}

impl Drop for ConcurrentDatabaseTransaction {
    fn drop(&mut self) {
        if !self.finished {
            self.state.rollback();
            self.finish_without_commit();
        }
    }
}

fn sql_lock_requests(
    sql_text: &str,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<Vec<LockRequest>> {
    let prepared = skein_sql::prepare_postgres_sql(sql_text)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    match prepared.statement {
        SqlStatement::Select(select) => Ok(select_lock_requests(&select, parameters, state)
            .unwrap_or_else(|| vec![LockRequest::database(LockMode::Shared)])),
        SqlStatement::Explain(_) => Ok(vec![LockRequest::database(LockMode::Shared)]),
        SqlStatement::Insert(_) => {
            let transaction = crate::relational_sql::compile_relational_statement_sql(
                sql_text, parameters, state,
            )?;
            Ok(insert_lock_requests(&transaction, state)
                .unwrap_or_else(|| vec![LockRequest::database(LockMode::Exclusive)]))
        }
        SqlStatement::Update(_)
        | SqlStatement::Delete(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => {
            Ok(vec![LockRequest::database(LockMode::Exclusive)])
        }
    }
}

fn select_lock_requests(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Option<Vec<LockRequest>> {
    if !select.joins.is_empty() || !is_public_table(&select.from) {
        return None;
    }
    let schema = state.table_schema(&select.from.name)?;
    let ranges = match &select.selection {
        None => vec![(Bound::Unbounded, Bound::Unbounded)],
        Some(predicate) if schema.primary_key.len() == 1 => single_key_ranges(
            predicate,
            &schema.primary_key[0],
            &select.from,
            select.from_alias.as_deref(),
            parameters,
        )?,
        Some(predicate) => vec![composite_key_point(
            predicate,
            &schema.primary_key,
            &select.from,
            select.from_alias.as_deref(),
            parameters,
        )?],
    };
    Some(
        ranges
            .into_iter()
            .map(|(lower, upper)| {
                LockRequest::relational_range(
                    LockMode::Shared,
                    select.from.name.clone(),
                    schema.primary_key.clone(),
                    lower,
                    upper,
                )
            })
            .collect(),
    )
}

fn insert_lock_requests(
    transaction: &RelationalTransaction,
    state: &RelationalState,
) -> Option<Vec<LockRequest>> {
    let mut requests = Vec::new();
    for write in &transaction.writes {
        let (table, rows) = match write {
            RelationalWrite::Insert { table, rows, .. } => (table, rows),
            RelationalWrite::Upsert {
                table,
                rows,
                action: RelationalConflictAction::DoNothing,
                ..
            } => (table, rows),
            _ => return None,
        };
        let schema = state.table_schema(table)?;
        let mut unique_keys = vec![schema.primary_key.clone()];
        unique_keys.extend(schema.unique_constraints.iter().cloned());
        unique_keys.extend(
            schema
                .indexes
                .iter()
                .filter(|index| index.unique)
                .map(|index| index.columns.clone()),
        );
        for row in rows {
            for columns in &unique_keys {
                let key = relational_row_key(schema, row, columns)?;
                if key.0.iter().any(|value| value == &RelationalValue::Null) {
                    continue;
                }
                push_unique_lock_request(
                    &mut requests,
                    LockRequest::relational_point(
                        LockMode::Exclusive,
                        table.clone(),
                        columns.clone(),
                        key,
                    ),
                );
            }
            for foreign_key in &schema.foreign_keys {
                let key = relational_row_key(schema, row, &foreign_key.columns)?;
                if key.0.iter().any(|value| value == &RelationalValue::Null) {
                    continue;
                }
                push_unique_lock_request(
                    &mut requests,
                    LockRequest::relational_point(
                        LockMode::Shared,
                        foreign_key.referenced_table.clone(),
                        foreign_key.referenced_columns.clone(),
                        key,
                    ),
                );
            }
        }
    }
    Some(requests)
}

fn push_unique_lock_request(requests: &mut Vec<LockRequest>, request: LockRequest) {
    if !requests.contains(&request) {
        requests.push(request);
    }
}

fn relational_row_key(
    schema: &RelationalTableSchema,
    row: &RelationalRow,
    columns: &[String],
) -> Option<RelationalKey> {
    columns
        .iter()
        .map(|column| {
            schema
                .column_position(column)
                .and_then(|position| row.values().get(position))
                .cloned()
        })
        .collect::<Option<Vec<_>>>()
        .map(RelationalKey)
}

fn single_key_ranges(
    predicate: &SqlPredicate,
    primary_key: &str,
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
) -> Option<Vec<(Bound<RelationalKey>, Bound<RelationalKey>)>> {
    match predicate {
        SqlPredicate::And(left, right) => {
            let left = single_key_ranges(left, primary_key, table, alias, parameters)?;
            let right = single_key_ranges(right, primary_key, table, alias, parameters)?;
            Some(
                left.into_iter()
                    .flat_map(|left| {
                        right
                            .iter()
                            .filter_map(move |right| intersect_ranges(&left, right))
                    })
                    .collect(),
            )
        }
        SqlPredicate::Or(left, right) => {
            let mut ranges = single_key_ranges(left, primary_key, table, alias, parameters)?;
            ranges.extend(single_key_ranges(
                right,
                primary_key,
                table,
                alias,
                parameters,
            )?);
            Some(ranges)
        }
        SqlPredicate::Compare { left, op, right }
            if column_matches(left, primary_key, table, alias) =>
        {
            let key = RelationalKey(vec![bind_sql_lock_value(right, parameters)?]);
            match op {
                SqlComparisonOp::Eq => {
                    Some(vec![(Bound::Included(key.clone()), Bound::Included(key))])
                }
                SqlComparisonOp::Lt => Some(vec![(Bound::Unbounded, Bound::Excluded(key))]),
                SqlComparisonOp::Lte => Some(vec![(Bound::Unbounded, Bound::Included(key))]),
                SqlComparisonOp::Gt => Some(vec![(Bound::Excluded(key), Bound::Unbounded)]),
                SqlComparisonOp::Gte => Some(vec![(Bound::Included(key), Bound::Unbounded)]),
                SqlComparisonOp::NotEq => None,
            }
        }
        SqlPredicate::InList {
            left,
            values,
            negated: false,
        } if column_matches(left, primary_key, table, alias) => values
            .iter()
            .map(|value| {
                let key = RelationalKey(vec![bind_sql_lock_value(value, parameters)?]);
                Some((Bound::Included(key.clone()), Bound::Included(key)))
            })
            .collect(),
        _ => None,
    }
}

fn composite_key_point(
    predicate: &SqlPredicate,
    primary_key: &[String],
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
) -> Option<(Bound<RelationalKey>, Bound<RelationalKey>)> {
    let mut values = BTreeMap::new();
    collect_composite_equalities(
        predicate,
        primary_key,
        table,
        alias,
        parameters,
        &mut values,
    )?;
    let key = RelationalKey(
        primary_key
            .iter()
            .map(|column| values.remove(column))
            .collect::<Option<Vec<_>>>()?,
    );
    Some((Bound::Included(key.clone()), Bound::Included(key)))
}

fn collect_composite_equalities(
    predicate: &SqlPredicate,
    primary_key: &[String],
    table: &SqlTableName,
    alias: Option<&str>,
    parameters: &[Value],
    values: &mut BTreeMap<String, RelationalValue>,
) -> Option<()> {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_composite_equalities(left, primary_key, table, alias, parameters, values)?;
            collect_composite_equalities(right, primary_key, table, alias, parameters, values)
        }
        SqlPredicate::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } if primary_key
            .iter()
            .any(|column| column_matches(left, column, table, alias)) =>
        {
            if values.contains_key(&left.name) {
                return None;
            }
            values.insert(left.name.clone(), bind_sql_lock_value(right, parameters)?);
            Some(())
        }
        _ => None,
    }
}

fn bind_sql_lock_value(value: &SqlValue, parameters: &[Value]) -> Option<RelationalValue> {
    crate::relational_sql::bind_relational_value(value.clone(), parameters).ok()
}

fn column_matches(
    column: &crate::sql::SqlColumnRef,
    expected: &str,
    table: &SqlTableName,
    alias: Option<&str>,
) -> bool {
    column.name == expected
        && column
            .qualifier
            .as_deref()
            .is_none_or(|qualifier| Some(qualifier) == alias || qualifier == table.name)
}

fn is_public_table(table: &SqlTableName) -> bool {
    table
        .schema
        .as_deref()
        .is_none_or(|schema| schema.eq_ignore_ascii_case("public"))
}

fn intersect_ranges(
    left: &(Bound<RelationalKey>, Bound<RelationalKey>),
    right: &(Bound<RelationalKey>, Bound<RelationalKey>),
) -> Option<(Bound<RelationalKey>, Bound<RelationalKey>)> {
    let lower = maximum_lower_bound(&left.0, &right.0);
    let upper = minimum_upper_bound(&left.1, &right.1);
    (!upper_is_before_lower(&upper, &lower)).then_some((lower, upper))
}

fn maximum_lower_bound(
    left: &Bound<RelationalKey>,
    right: &Bound<RelationalKey>,
) -> Bound<RelationalKey> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound.clone(),
        (Bound::Included(left), Bound::Included(right)) => Bound::Included(left.max(right).clone()),
        (Bound::Excluded(left), Bound::Excluded(right)) => Bound::Excluded(left.max(right).clone()),
        (Bound::Included(left), Bound::Excluded(right)) => match left.cmp(right) {
            std::cmp::Ordering::Greater => Bound::Included(left.clone()),
            _ => Bound::Excluded(right.clone()),
        },
        (Bound::Excluded(left), Bound::Included(right)) => match left.cmp(right) {
            std::cmp::Ordering::Less => Bound::Included(right.clone()),
            _ => Bound::Excluded(left.clone()),
        },
    }
}

fn minimum_upper_bound(
    left: &Bound<RelationalKey>,
    right: &Bound<RelationalKey>,
) -> Bound<RelationalKey> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound.clone(),
        (Bound::Included(left), Bound::Included(right)) => Bound::Included(left.min(right).clone()),
        (Bound::Excluded(left), Bound::Excluded(right)) => Bound::Excluded(left.min(right).clone()),
        (Bound::Included(left), Bound::Excluded(right)) => match left.cmp(right) {
            std::cmp::Ordering::Less => Bound::Included(left.clone()),
            _ => Bound::Excluded(right.clone()),
        },
        (Bound::Excluded(left), Bound::Included(right)) => match left.cmp(right) {
            std::cmp::Ordering::Greater => Bound::Included(right.clone()),
            _ => Bound::Excluded(left.clone()),
        },
    }
}

fn upper_is_before_lower(upper: &Bound<RelationalKey>, lower: &Bound<RelationalKey>) -> bool {
    match (upper, lower) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(upper), Bound::Included(lower)) => upper < lower,
        (Bound::Included(upper), Bound::Excluded(lower))
        | (Bound::Excluded(upper), Bound::Included(lower))
        | (Bound::Excluded(upper), Bound::Excluded(lower)) => upper <= lower,
    }
}

fn cypher_statement_is_read_only(statement: &crate::cypher::Statement) -> bool {
    match statement {
        crate::cypher::Statement::CypherQuery(query) => {
            cypher_statement_is_read_only(&query.statement)
        }
        crate::cypher::Statement::Explain(explain) => {
            cypher_statement_is_read_only(&explain.statement)
        }
        crate::cypher::Statement::GraphAlgorithm(_)
        | crate::cypher::Statement::MatchNodesReturn(_)
        | crate::cypher::Statement::MatchOptionalRelationshipCountSum(_)
        | crate::cypher::Statement::MatchReturn(_)
        | crate::cypher::Statement::MatchThreadRepairStats(_)
        | crate::cypher::Statement::ShortestPathReturn(_)
        | crate::cypher::Statement::VectorSearch(_) => true,
        _ => false,
    }
}

fn coordinator_poisoned_error() -> SkeinError {
    SkeinError::Execution("concurrent database coordinator lock is poisoned".to_string())
}

fn lock_timeout_error(timeout: Duration) -> SkeinError {
    SkeinError::Execution(format!(
        "transaction lock wait timed out after {} ms",
        timeout.as_millis()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: i64) -> RelationalKey {
        RelationalKey(vec![RelationalValue::BigInt(value)])
    }

    #[test]
    fn intersects_adjacent_half_open_ranges_correctly() {
        assert!(intersect_ranges(
            &(Bound::Included(key(10)), Bound::Excluded(key(20))),
            &(Bound::Included(key(20)), Bound::Included(key(30))),
        )
        .is_none());
        assert_eq!(
            intersect_ranges(
                &(Bound::Included(key(10)), Bound::Included(key(20))),
                &(Bound::Included(key(20)), Bound::Included(key(30))),
            ),
            Some((Bound::Included(key(20)), Bound::Included(key(20))))
        );
    }
}
