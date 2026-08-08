use super::super::transaction_locks::{LockRequest, LockTable, WaitForGraph};
use super::{Database, QueryOutput, WalGroupCommitConfig, WalGroupCommitSnapshot};
use crate::error::{Result, SkeinError};
use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub(super) struct CommitSequencer {
    database: Mutex<Database>,
    group_commit: GroupCommitCoordinator,
}

impl CommitSequencer {
    pub(super) fn new(database: Database, group_commit: WalGroupCommitConfig) -> Self {
        Self {
            database: Mutex::new(database),
            group_commit: GroupCommitCoordinator::new(group_commit),
        }
    }

    pub(super) fn lock(&self) -> Result<MutexGuard<'_, Database>> {
        self.database.lock().map_err(|_| {
            SkeinError::Execution("concurrent commit sequencer is poisoned".to_string())
        })
    }

    pub(super) fn execute_grouped(
        &self,
        task: impl FnOnce(&mut Database) -> Result<QueryOutput> + Send + 'static,
    ) -> Result<QueryOutput> {
        if !self.group_commit.config.is_enabled() {
            let mut database = self.lock()?;
            return task(&mut database);
        }
        let request = Arc::new(QueuedCommit::new(Box::new(task)));
        {
            let mut state = self.group_commit.lock_state()?;
            state.metrics.submitted_commits = state.metrics.submitted_commits.saturating_add(1);
            state.queue.push_back(Arc::clone(&request));
            self.group_commit.available.notify_all();
        }
        loop {
            if let Some(result) = request.take_result()? {
                return result;
            }
            let mut state = self.group_commit.lock_state()?;
            if let Some(result) = request.take_result()? {
                return result;
            }
            let can_lead = !state.leader_active
                && state
                    .queue
                    .front()
                    .is_some_and(|front| Arc::ptr_eq(front, &request));
            if can_lead {
                state.leader_active = true;
                drop(state);
                self.run_group_commit()?;
                continue;
            }
            state = self
                .group_commit
                .available
                .wait(state)
                .map_err(|_| group_commit_coordinator_poisoned_error())?;
            drop(state);
        }
    }

    pub(super) fn group_commit_snapshot(&self) -> Result<WalGroupCommitSnapshot> {
        Ok(self.group_commit.lock_state()?.metrics)
    }

    fn run_group_commit(&self) -> Result<()> {
        self.wait_for_group_commit_peers()?;
        let mut database = match self.lock() {
            Ok(database) => database,
            Err(error) => {
                self.fail_front_group(error.to_string())?;
                return Ok(());
            }
        };
        if let Err(error) = database.begin_wal_sync_group() {
            drop(database);
            self.fail_front_group(error.to_string())?;
            return Ok(());
        }

        let mut completed = Vec::new();
        while completed.len() < self.group_commit.config.max_entries().get() {
            let request = {
                let mut state = self.group_commit.lock_state()?;
                state.queue.pop_front()
            };
            let Some(request) = request else {
                break;
            };
            let task = request.take_task()?;
            let result = task(&mut database);
            completed.push((request, result));
            let progress = database.wal_sync_group_progress();
            if progress.byte_count >= self.group_commit.config.max_bytes().get() {
                break;
            }
        }

        let flush = database.finish_wal_sync_group();
        drop(database);
        let flush = match flush {
            Ok(flush) => flush,
            Err(error) => {
                let message = format!(
                    "WAL group durability barrier failed after mutation publication; close and reopen the database: {error}"
                );
                for (_, result) in &mut completed {
                    if result.is_ok() {
                        *result = Err(SkeinError::Storage(message.clone()));
                    }
                }
                Default::default()
            }
        };

        let completed_commits = completed
            .iter()
            .filter(|(_, result)| result.is_ok())
            .count() as u64;
        {
            let mut state = self.group_commit.lock_state()?;
            state.metrics.completed_commits = state
                .metrics
                .completed_commits
                .saturating_add(completed_commits);
            state.metrics.group_count = state.metrics.group_count.saturating_add(1);
            state.metrics.shared_sync_count = state
                .metrics
                .shared_sync_count
                .saturating_add(u64::from(flush.fsync_performed));
            state.metrics.grouped_wal_entries = state
                .metrics
                .grouped_wal_entries
                .saturating_add(flush.entry_count as u64);
            state.metrics.grouped_wal_bytes = state
                .metrics
                .grouped_wal_bytes
                .saturating_add(flush.byte_count);
            state.metrics.max_observed_group_entries = state
                .metrics
                .max_observed_group_entries
                .max(flush.entry_count);
            state.metrics.max_observed_group_bytes =
                state.metrics.max_observed_group_bytes.max(flush.byte_count);
            state.metrics.total_fsync_micros = state
                .metrics
                .total_fsync_micros
                .saturating_add(flush.fsync_micros);
            state.leader_active = false;
        }
        for (request, result) in completed {
            request.complete(result)?;
        }
        self.group_commit.available.notify_all();
        Ok(())
    }

    fn wait_for_group_commit_peers(&self) -> Result<()> {
        let deadline = Instant::now() + self.group_commit.config.max_delay();
        let mut state = self.group_commit.lock_state()?;
        while state.queue.len() < self.group_commit.config.max_entries().get() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let waited = self
                .group_commit
                .available
                .wait_timeout(state, remaining)
                .map_err(|_| group_commit_coordinator_poisoned_error())?;
            state = waited.0;
            if waited.1.timed_out() {
                break;
            }
        }
        Ok(())
    }

    fn fail_front_group(&self, message: String) -> Result<()> {
        let requests = {
            let mut state = self.group_commit.lock_state()?;
            let count = state
                .queue
                .len()
                .min(self.group_commit.config.max_entries().get());
            let requests = state.queue.drain(..count).collect::<Vec<_>>();
            state.metrics.group_count = state.metrics.group_count.saturating_add(1);
            state.leader_active = false;
            requests
        };
        for request in requests {
            request.complete(Err(SkeinError::Storage(format!(
                "WAL group commit could not start: {message}"
            ))))?;
        }
        self.group_commit.available.notify_all();
        Ok(())
    }
}

impl Debug for CommitSequencer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommitSequencer")
            .field("group_commit", &self.group_commit.config)
            .finish_non_exhaustive()
    }
}

type CommitTask = Box<dyn FnOnce(&mut Database) -> Result<QueryOutput> + Send + 'static>;

struct QueuedCommit {
    task: Mutex<Option<CommitTask>>,
    result: Mutex<Option<Result<QueryOutput>>>,
}

impl QueuedCommit {
    fn new(task: CommitTask) -> Self {
        Self {
            task: Mutex::new(Some(task)),
            result: Mutex::new(None),
        }
    }

    fn take_task(&self) -> Result<CommitTask> {
        self.task
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())?
            .take()
            .ok_or_else(|| {
                SkeinError::Execution("WAL group commit task was already consumed".to_string())
            })
    }

    fn complete(&self, result: Result<QueryOutput>) -> Result<()> {
        *self
            .result
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())? = Some(result);
        Ok(())
    }

    fn take_result(&self) -> Result<Option<Result<QueryOutput>>> {
        Ok(self
            .result
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())?
            .take())
    }
}

struct GroupCommitCoordinator {
    config: WalGroupCommitConfig,
    state: Mutex<GroupCommitState>,
    available: Condvar,
}

impl GroupCommitCoordinator {
    fn new(config: WalGroupCommitConfig) -> Self {
        Self {
            config,
            state: Mutex::new(GroupCommitState {
                metrics: WalGroupCommitSnapshot {
                    activation: config.activation(),
                    ..WalGroupCommitSnapshot::default()
                },
                ..GroupCommitState::default()
            }),
            available: Condvar::new(),
        }
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, GroupCommitState>> {
        self.state
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())
    }
}

#[derive(Default)]
struct GroupCommitState {
    queue: VecDeque<Arc<QueuedCommit>>,
    leader_active: bool,
    metrics: WalGroupCommitSnapshot,
}

#[derive(Debug, Default)]
pub(super) struct LockManager {
    state: Mutex<LockManagerState>,
    available: Condvar,
}

#[derive(Debug, Default)]
struct LockManagerState {
    locks: LockTable,
    wait_for: WaitForGraph,
}

impl LockManager {
    pub(super) fn covers_all(&self, transaction_id: u64, requests: &[LockRequest]) -> Result<bool> {
        Ok(self
            .lock_state()?
            .locks
            .covers_all(transaction_id, requests))
    }

    pub(super) fn acquire(
        &self,
        transaction_id: u64,
        requests: &[LockRequest],
        started: Instant,
        timeout: Duration,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let mut unique_requests = Vec::with_capacity(requests.len());
        for request in requests {
            if !unique_requests.contains(request) {
                unique_requests.push(request.clone());
            }
        }
        for request in unique_requests {
            loop {
                let blockers = state.locks.blockers(transaction_id, &request);
                if blockers.is_empty() {
                    state.wait_for.clear_waiter(transaction_id);
                    state.locks.grant(transaction_id, request);
                    break;
                }
                state.wait_for.register(transaction_id, &blockers)?;
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    state.wait_for.clear_waiter(transaction_id);
                    return Err(lock_timeout_error(timeout));
                }
                let waited = self.available.wait_timeout(state, remaining);
                let (next, wait) = match waited {
                    Ok(waited) => waited,
                    Err(poisoned) => {
                        let (mut recovered, _) = poisoned.into_inner();
                        recovered.wait_for.clear_waiter(transaction_id);
                        return Err(lock_manager_poisoned_error());
                    }
                };
                state = next;
                state.wait_for.clear_waiter(transaction_id);
                if wait.timed_out() {
                    return Err(lock_timeout_error(timeout));
                }
            }
        }
        Ok(())
    }

    pub(super) fn release(&self, transaction_id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.locks.release_transaction(transaction_id);
        state.wait_for.remove_transaction(transaction_id);
        drop(state);
        self.available.notify_all();
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, LockManagerState>> {
        self.state.lock().map_err(|_| lock_manager_poisoned_error())
    }
}

#[derive(Debug)]
pub(super) struct TransactionIdAllocator {
    next: AtomicU64,
}

impl Default for TransactionIdAllocator {
    fn default() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }
}

impl TransactionIdAllocator {
    pub(super) fn allocate(&self) -> Result<u64> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                SkeinError::Execution("concurrent transaction id space is exhausted".to_string())
            })
    }
}

fn lock_manager_poisoned_error() -> SkeinError {
    SkeinError::Execution("concurrent lock manager is poisoned".to_string())
}

fn group_commit_coordinator_poisoned_error() -> SkeinError {
    SkeinError::Execution("WAL group commit coordinator is poisoned".to_string())
}

fn lock_timeout_error(timeout: Duration) -> SkeinError {
    SkeinError::Execution(format!(
        "transaction lock wait timed out after {} ms",
        timeout.as_millis()
    ))
}
