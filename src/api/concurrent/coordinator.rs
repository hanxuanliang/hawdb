use super::super::transaction_locks::{LockRequest, LockTable, WaitForGraph};
use super::Database;
use crate::error::{Result, SkeinError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(super) struct CommitSequencer {
    database: Mutex<Database>,
}

impl CommitSequencer {
    pub(super) fn new(database: Database) -> Self {
        Self {
            database: Mutex::new(database),
        }
    }

    pub(super) fn lock(&self) -> Result<MutexGuard<'_, Database>> {
        self.database.lock().map_err(|_| {
            SkeinError::Execution("concurrent commit sequencer is poisoned".to_string())
        })
    }
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

fn lock_timeout_error(timeout: Duration) -> SkeinError {
    SkeinError::Execution(format!(
        "transaction lock wait timed out after {} ms",
        timeout.as_millis()
    ))
}
