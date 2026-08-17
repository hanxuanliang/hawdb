//! Query-owned hierarchical memory accounting.

use skein_core::{Result, SkeinError};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueryMemoryClass {
    PipelineBatch,
    BlockingState,
    SpillStaging,
    MorselOutput,
    ResultMaterialization,
}

impl QueryMemoryClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PipelineBatch => "pipeline_batch",
            Self::BlockingState => "blocking_state",
            Self::SpillStaging => "spill_staging",
            Self::MorselOutput => "morsel_output",
            Self::ResultMaterialization => "result_materialization",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMemoryClassSnapshot {
    pub class: QueryMemoryClass,
    pub used_bytes: usize,
    pub peak_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMemoryLedgerSnapshot {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub peak_bytes: usize,
    pub account_count: usize,
    pub classes: Vec<QueryMemoryClassSnapshot>,
}

#[derive(Debug, Clone)]
pub struct QueryMemoryLedger {
    inner: Arc<QueryMemoryLedgerInner>,
}

#[derive(Debug)]
struct QueryMemoryLedgerInner {
    budget_bytes: usize,
    state: Mutex<QueryMemoryLedgerState>,
}

#[derive(Debug, Default)]
struct QueryMemoryLedgerState {
    used_bytes: usize,
    peak_bytes: usize,
    next_account_id: u64,
    accounts: BTreeMap<u64, QueryMemoryAccountState>,
    classes: BTreeMap<QueryMemoryClass, QueryMemoryClassState>,
}

#[derive(Debug)]
struct QueryMemoryAccountState {
    class: QueryMemoryClass,
    owner: Arc<str>,
    budget_bytes: usize,
    used_bytes: usize,
    peak_bytes: usize,
}

#[derive(Debug, Default)]
struct QueryMemoryClassState {
    used_bytes: usize,
    peak_bytes: usize,
}

impl QueryMemoryLedger {
    pub fn new(budget_bytes: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(QueryMemoryLedgerInner {
                budget_bytes: budget_bytes.get(),
                state: Mutex::new(QueryMemoryLedgerState::default()),
            }),
        }
    }

    pub fn account(
        &self,
        class: QueryMemoryClass,
        owner: impl Into<Arc<str>>,
        budget_bytes: NonZeroUsize,
    ) -> QueryMemoryAccount {
        let mut state = lock_recover(&self.inner.state);
        let account_id = state.next_account_id;
        state.next_account_id = state.next_account_id.saturating_add(1);
        state.accounts.insert(
            account_id,
            QueryMemoryAccountState {
                class,
                owner: owner.into(),
                budget_bytes: budget_bytes.get(),
                used_bytes: 0,
                peak_bytes: 0,
            },
        );
        QueryMemoryAccount {
            ledger: self.clone(),
            account_id,
        }
    }

    pub fn snapshot(&self) -> QueryMemoryLedgerSnapshot {
        let state = lock_recover(&self.inner.state);
        QueryMemoryLedgerSnapshot {
            budget_bytes: self.inner.budget_bytes,
            used_bytes: state.used_bytes,
            peak_bytes: state.peak_bytes,
            account_count: state.accounts.len(),
            classes: state
                .classes
                .iter()
                .map(|(class, state)| QueryMemoryClassSnapshot {
                    class: *class,
                    used_bytes: state.used_bytes,
                    peak_bytes: state.peak_bytes,
                })
                .collect(),
        }
    }

    fn reserve(&self, account_id: u64, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut state =
            self.inner.state.lock().map_err(|_| {
                SkeinError::Execution("query memory ledger is poisoned".to_string())
            })?;
        let (class, owner, account_budget, account_next) = {
            let account = state.accounts.get(&account_id).ok_or_else(|| {
                SkeinError::Execution("query memory account is no longer registered".to_string())
            })?;
            let account_next = account.used_bytes.checked_add(bytes).ok_or_else(|| {
                SkeinError::Execution(format!(
                    "query memory account {} ({}) byte accounting overflow",
                    account.owner,
                    account.class.as_str()
                ))
            })?;
            (
                account.class,
                Arc::clone(&account.owner),
                account.budget_bytes,
                account_next,
            )
        };
        if account_next > account_budget {
            return Err(SkeinError::Execution(format!(
                "query memory account {owner} ({}) would use {account_next} bytes, exceeding its {account_budget}-byte budget",
                class.as_str()
            )));
        }
        let root_next = state.used_bytes.checked_add(bytes).ok_or_else(|| {
            SkeinError::Execution(format!(
                "query memory ledger byte accounting overflow while charging {owner} ({})",
                class.as_str()
            ))
        })?;
        if root_next > self.inner.budget_bytes {
            return Err(SkeinError::Execution(format!(
                "query memory ledger would use {root_next} bytes while charging {owner} ({}), exceeding query_memory_bytes {}",
                class.as_str(),
                self.inner.budget_bytes
            )));
        }

        let account = state
            .accounts
            .get_mut(&account_id)
            .expect("validated query memory account remains registered");
        account.used_bytes = account_next;
        account.peak_bytes = account.peak_bytes.max(account_next);
        state.used_bytes = root_next;
        state.peak_bytes = state.peak_bytes.max(root_next);
        let class_state = state.classes.entry(class).or_default();
        class_state.used_bytes = class_state.used_bytes.saturating_add(bytes);
        class_state.peak_bytes = class_state.peak_bytes.max(class_state.used_bytes);
        Ok(())
    }

    fn release(&self, account_id: u64, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut state = lock_recover(&self.inner.state);
        let Some(account) = state.accounts.get_mut(&account_id) else {
            return;
        };
        let released = bytes.min(account.used_bytes);
        let class = account.class;
        account.used_bytes -= released;
        state.used_bytes = state.used_bytes.saturating_sub(released);
        if let Some(class_state) = state.classes.get_mut(&class) {
            class_state.used_bytes = class_state.used_bytes.saturating_sub(released);
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryMemoryAccount {
    ledger: QueryMemoryLedger,
    account_id: u64,
}

impl QueryMemoryAccount {
    pub fn reserve(&self, bytes: usize) -> Result<QueryMemoryLease> {
        self.ledger.reserve(self.account_id, bytes)?;
        Ok(QueryMemoryLease {
            account: self.clone(),
            bytes,
        })
    }
}

#[derive(Debug)]
pub struct QueryMemoryLease {
    account: QueryMemoryAccount,
    bytes: usize,
}

impl QueryMemoryLease {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn grow(&mut self, bytes: usize) -> Result<()> {
        self.account
            .ledger
            .reserve(self.account.account_id, bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    pub fn shrink(&mut self, bytes: usize) {
        let released = bytes.min(self.bytes);
        self.account
            .ledger
            .release(self.account.account_id, released);
        self.bytes -= released;
    }

    pub fn reset(&mut self) {
        self.shrink(self.bytes);
    }
}

impl Drop for QueryMemoryLease {
    fn drop(&mut self) {
        self.account
            .ledger
            .release(self.account.account_id, self.bytes);
        self.bytes = 0;
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sibling_accounts_share_one_root_budget() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(10).unwrap());
        let left = ledger.account(
            QueryMemoryClass::BlockingState,
            "left",
            NonZeroUsize::new(10).unwrap(),
        );
        let right = ledger.account(
            QueryMemoryClass::PipelineBatch,
            "right",
            NonZeroUsize::new(10).unwrap(),
        );
        let left_lease = left.reserve(6).unwrap();
        let error = right.reserve(5).unwrap_err();

        assert!(error.to_string().contains("query_memory_bytes 10"));
        assert_eq!(ledger.snapshot().used_bytes, 6);
        drop(left_lease);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn account_budget_is_enforced_before_root_budget() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(100).unwrap());
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "sort",
            NonZeroUsize::new(4).unwrap(),
        );

        let error = account.reserve(5).unwrap_err();

        assert!(error.to_string().contains("sort"));
        assert!(error.to_string().contains("4-byte budget"));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn lease_growth_shrink_and_drop_update_hierarchy() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(100).unwrap());
        let account = ledger.account(
            QueryMemoryClass::MorselOutput,
            "reorder",
            NonZeroUsize::new(80).unwrap(),
        );
        let mut lease = account.reserve(20).unwrap();
        lease.grow(30).unwrap();
        lease.shrink(10);
        assert_eq!(lease.bytes(), 40);
        assert_eq!(ledger.snapshot().used_bytes, 40);

        drop(lease);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.used_bytes, 0);
        assert_eq!(snapshot.peak_bytes, 50);
        assert_eq!(snapshot.classes[0].peak_bytes, 50);
    }

    #[test]
    fn unwind_releases_query_memory_lease() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(100).unwrap());
        let account = ledger.account(
            QueryMemoryClass::SpillStaging,
            "spill",
            NonZeroUsize::new(100).unwrap(),
        );
        let result = std::panic::catch_unwind(|| {
            let _lease = account.reserve(80).unwrap();
            panic!("injected panic");
        });

        assert!(result.is_err());
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
