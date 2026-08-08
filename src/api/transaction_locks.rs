use crate::error::{Result, SkeinError};
use skein_storage::RelationalKey;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LockNamespace {
    RelationalIndex { table: String, columns: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LockTarget {
    Database,
    RelationalRange {
        namespace: LockNamespace,
        lower: Bound<RelationalKey>,
        upper: Bound<RelationalKey>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LockRequest {
    pub(crate) mode: LockMode,
    pub(crate) target: LockTarget,
}

impl LockRequest {
    pub(crate) fn database(mode: LockMode) -> Self {
        Self {
            mode,
            target: LockTarget::Database,
        }
    }

    pub(crate) fn relational_point(
        mode: LockMode,
        table: impl Into<String>,
        columns: Vec<String>,
        key: RelationalKey,
    ) -> Self {
        Self::relational_range(
            mode,
            table,
            columns,
            Bound::Included(key.clone()),
            Bound::Included(key),
        )
    }

    pub(crate) fn relational_range(
        mode: LockMode,
        table: impl Into<String>,
        columns: Vec<String>,
        lower: Bound<RelationalKey>,
        upper: Bound<RelationalKey>,
    ) -> Self {
        Self {
            mode,
            target: LockTarget::RelationalRange {
                namespace: LockNamespace::RelationalIndex {
                    table: table.into(),
                    columns,
                },
                lower,
                upper,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeldLock {
    transaction_id: u64,
    request: LockRequest,
}

#[derive(Debug, Default)]
pub(crate) struct LockTable {
    locks: Vec<HeldLock>,
}

impl LockTable {
    pub(crate) fn covers_all(&self, transaction_id: u64, requests: &[LockRequest]) -> bool {
        requests.iter().all(|request| {
            self.locks.iter().any(|held| {
                held.transaction_id == transaction_id
                    && mode_covers(held.request.mode, request.mode)
                    && target_covers(&held.request.target, &request.target)
            })
        })
    }

    pub(crate) fn blockers(&self, transaction_id: u64, request: &LockRequest) -> BTreeSet<u64> {
        self.locks
            .iter()
            .filter(|held| {
                held.transaction_id != transaction_id
                    && modes_conflict(held.request.mode, request.mode)
                    && targets_overlap(&held.request.target, &request.target)
            })
            .map(|held| held.transaction_id)
            .collect()
    }

    pub(crate) fn grant(&mut self, transaction_id: u64, request: LockRequest) {
        if self
            .locks
            .iter()
            .any(|held| held.transaction_id == transaction_id && held.request == request)
        {
            return;
        }
        self.locks.push(HeldLock {
            transaction_id,
            request,
        });
    }

    pub(crate) fn release_transaction(&mut self, transaction_id: u64) {
        self.locks
            .retain(|held| held.transaction_id != transaction_id);
    }

    #[cfg(test)]
    fn lock_count(&self) -> usize {
        self.locks.len()
    }
}

#[derive(Debug, Default)]
pub(crate) struct WaitForGraph {
    edges: BTreeMap<u64, BTreeSet<u64>>,
}

impl WaitForGraph {
    pub(crate) fn register(&mut self, waiter: u64, owners: &BTreeSet<u64>) -> Result<()> {
        if owners.is_empty() {
            self.clear_waiter(waiter);
            return Ok(());
        }
        self.edges.insert(waiter, owners.clone());
        if let Some(cycle) = self.cycle_from(waiter) {
            self.edges.remove(&waiter);
            return Err(deadlock_error(waiter, &cycle));
        }
        Ok(())
    }

    pub(crate) fn clear_waiter(&mut self, transaction_id: u64) {
        self.edges.remove(&transaction_id);
    }

    pub(crate) fn remove_transaction(&mut self, transaction_id: u64) {
        self.edges.remove(&transaction_id);
        self.edges.retain(|_, owners| {
            owners.remove(&transaction_id);
            !owners.is_empty()
        });
    }

    fn cycle_from(&self, waiter: u64) -> Option<Vec<u64>> {
        let mut path = vec![waiter];
        let mut visiting = BTreeSet::from([waiter]);
        self.path_to(waiter, waiter, &mut path, &mut visiting)
    }

    fn path_to(
        &self,
        current: u64,
        target: u64,
        path: &mut Vec<u64>,
        visiting: &mut BTreeSet<u64>,
    ) -> Option<Vec<u64>> {
        for owner in self.edges.get(&current).into_iter().flatten().copied() {
            path.push(owner);
            if owner == target {
                return Some(path.clone());
            }
            if visiting.insert(owner) {
                if let Some(cycle) = self.path_to(owner, target, path, visiting) {
                    return Some(cycle);
                }
                visiting.remove(&owner);
            }
            path.pop();
        }
        None
    }
}

fn modes_conflict(left: LockMode, right: LockMode) -> bool {
    left == LockMode::Exclusive || right == LockMode::Exclusive
}

fn mode_covers(held: LockMode, requested: LockMode) -> bool {
    held == LockMode::Exclusive || requested == LockMode::Shared
}

fn target_covers(held: &LockTarget, requested: &LockTarget) -> bool {
    match (held, requested) {
        (LockTarget::Database, _) => true,
        (_, LockTarget::Database) => false,
        (
            LockTarget::RelationalRange {
                namespace: held_namespace,
                lower: held_lower,
                upper: held_upper,
            },
            LockTarget::RelationalRange {
                namespace: requested_namespace,
                lower: requested_lower,
                upper: requested_upper,
            },
        ) => {
            held_namespace == requested_namespace
                && lower_bound_covers(held_lower, requested_lower)
                && upper_bound_covers(held_upper, requested_upper)
        }
    }
}

fn lower_bound_covers(held: &Bound<RelationalKey>, requested: &Bound<RelationalKey>) -> bool {
    match (held, requested) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Included(held), Bound::Included(requested)) => held <= requested,
        (Bound::Included(held), Bound::Excluded(requested)) => held <= requested,
        (Bound::Excluded(held), Bound::Included(requested)) => held < requested,
        (Bound::Excluded(held), Bound::Excluded(requested)) => held <= requested,
    }
}

fn upper_bound_covers(held: &Bound<RelationalKey>, requested: &Bound<RelationalKey>) -> bool {
    match (held, requested) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (Bound::Included(held), Bound::Included(requested)) => held >= requested,
        (Bound::Included(held), Bound::Excluded(requested)) => held >= requested,
        (Bound::Excluded(held), Bound::Included(requested)) => held > requested,
        (Bound::Excluded(held), Bound::Excluded(requested)) => held >= requested,
    }
}

fn targets_overlap(left: &LockTarget, right: &LockTarget) -> bool {
    match (left, right) {
        (LockTarget::Database, _) | (_, LockTarget::Database) => true,
        (
            LockTarget::RelationalRange {
                namespace: left_namespace,
                lower: left_lower,
                upper: left_upper,
            },
            LockTarget::RelationalRange {
                namespace: right_namespace,
                lower: right_lower,
                upper: right_upper,
            },
        ) => {
            left_namespace == right_namespace
                && !upper_is_before_lower(left_upper, right_lower)
                && !upper_is_before_lower(right_upper, left_lower)
        }
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

fn deadlock_error(victim: u64, cycle: &[u64]) -> SkeinError {
    let cycle = cycle
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" -> ");
    SkeinError::Execution(format!(
        "deadlock detected; transaction {victim} selected as victim; wait cycle: {cycle}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_storage::RelationalValue;

    fn key(value: i64) -> RelationalKey {
        RelationalKey(vec![RelationalValue::BigInt(value)])
    }

    fn range(
        mode: LockMode,
        lower: Bound<RelationalKey>,
        upper: Bound<RelationalKey>,
    ) -> LockRequest {
        LockRequest::relational_range(mode, "messages", vec!["id".to_string()], lower, upper)
    }

    #[test]
    fn point_locks_conflict_only_on_the_same_key() {
        let mut table = LockTable::default();
        table.grant(
            1,
            LockRequest::relational_point(
                LockMode::Exclusive,
                "messages",
                vec!["id".to_string()],
                key(7),
            ),
        );

        assert_eq!(
            table.blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Shared,
                    "messages",
                    vec!["id".to_string()],
                    key(7),
                )
            ),
            BTreeSet::from([1])
        );
        assert!(table
            .blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Exclusive,
                    "messages",
                    vec!["id".to_string()],
                    key(8),
                )
            )
            .is_empty());
    }

    #[test]
    fn half_open_ranges_have_no_boundary_conflict() {
        let mut table = LockTable::default();
        table.grant(
            1,
            range(
                LockMode::Exclusive,
                Bound::Included(key(10)),
                Bound::Excluded(key(20)),
            ),
        );

        assert!(table
            .blockers(
                2,
                &range(
                    LockMode::Exclusive,
                    Bound::Included(key(20)),
                    Bound::Included(key(30)),
                )
            )
            .is_empty());
        assert_eq!(
            table.blockers(
                2,
                &LockRequest::relational_point(
                    LockMode::Shared,
                    "messages",
                    vec!["id".to_string()],
                    key(19),
                )
            ),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn shared_ranges_are_compatible_and_database_exclusive_is_universal() {
        let mut table = LockTable::default();
        table.grant(
            1,
            range(LockMode::Shared, Bound::Unbounded, Bound::Unbounded),
        );
        assert!(table
            .blockers(
                2,
                &range(
                    LockMode::Shared,
                    Bound::Included(key(1)),
                    Bound::Included(key(1)),
                )
            )
            .is_empty());
        assert_eq!(
            table.blockers(2, &LockRequest::database(LockMode::Exclusive)),
            BTreeSet::from([1])
        );
        table.release_transaction(1);
        assert_eq!(table.lock_count(), 0);
    }

    #[test]
    fn a_held_range_covers_narrower_repeated_requests() {
        let mut table = LockTable::default();
        table.grant(
            1,
            range(
                LockMode::Shared,
                Bound::Included(key(10)),
                Bound::Excluded(key(20)),
            ),
        );

        assert!(table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Shared,
                "messages",
                vec!["id".to_string()],
                key(15),
            )]
        ));
        assert!(!table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Shared,
                "messages",
                vec!["id".to_string()],
                key(20),
            )]
        ));
        assert!(!table.covers_all(
            1,
            &[LockRequest::relational_point(
                LockMode::Exclusive,
                "messages",
                vec!["id".to_string()],
                key(15),
            )]
        ));
    }

    #[test]
    fn wait_for_graph_detects_a_cycle_with_multiple_blockers() {
        let mut graph = WaitForGraph::default();
        graph.register(1, &BTreeSet::from([2, 4])).unwrap();
        graph.register(2, &BTreeSet::from([3])).unwrap();

        let error = graph.register(3, &BTreeSet::from([1, 5])).unwrap_err();

        assert!(error.to_string().contains("deadlock detected"));
        assert!(error.to_string().contains("3 -> 1 -> 2 -> 3"));
    }

    #[test]
    fn removing_an_owner_cleans_all_wait_dependencies() {
        let mut graph = WaitForGraph::default();
        graph.register(1, &BTreeSet::from([2, 3])).unwrap();
        graph.remove_transaction(2);
        assert_eq!(graph.edges, BTreeMap::from([(1, BTreeSet::from([3]))]));
        graph.remove_transaction(3);
        assert!(graph.edges.is_empty());
    }
}
