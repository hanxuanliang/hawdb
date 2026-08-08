use crate::{
    ConcurrentDatabase, ConcurrentTransactionOptions, Database, SkeinError, Value,
    WalGroupCommitActivation, WalGroupCommitConfig, WalGroupCommitEvidence,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Barrier};
use std::time::Duration;

#[test]
fn concurrent_database_checkpoint_publishes_an_immutable_cut() {
    let path = super::unique_test_dir("concurrent_checkpoint_immutable_cut");
    let db = Database::open(&path).unwrap().into_concurrent();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.commit_epoch().unwrap(), 1);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0].get("id"), Some(&Value::Int(1)));
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn optimistic_transactions_prepare_in_parallel_and_reject_the_stale_committer() {
    let db = Database::new().into_concurrent();
    let barrier = Arc::new(Barrier::new(2));
    let handles = (1..=2)
        .map(|id| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut tx = db
                    .begin_transaction(ConcurrentTransactionOptions::optimistic())
                    .unwrap();
                tx.query(&format!("CREATE (:Memory {{id: {id}}})")).unwrap();
                barrier.wait();
                tx.commit()
            })
        })
        .collect::<Vec<_>>();

    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let conflict = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("one optimistic transaction must conflict");
    assert!(matches!(conflict, SkeinError::Execution(_)));
    assert!(conflict
        .to_string()
        .contains("optimistic transaction conflict"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), 1);
}

#[test]
fn optimistic_transaction_reads_its_private_workspace() {
    let db = Database::new().into_concurrent();
    let mut tx = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    assert_eq!(tx.base_commit_epoch(), 0);
    tx.query("CREATE (:Memory {id: 1, title: 'private'})")
        .unwrap();

    let staged = tx
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        staged.rows[0].get("title"),
        Some(&Value::String("private".to_string()))
    );
    let mut outside = db.begin_read_transaction().unwrap();
    assert!(outside
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());

    tx.commit().unwrap();
    let mut committed = db.begin_read_transaction().unwrap();
    assert_eq!(
        committed
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn pessimistic_transaction_blocks_other_writers_with_a_bounded_wait() {
    let db = Database::new().into_concurrent();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner.query("CREATE (:Memory {id: 1})").unwrap();

    let contender = {
        let db = db.clone();
        std::thread::spawn(move || {
            let mut contender = db
                .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                    Duration::from_millis(25),
                ))
                .unwrap();
            contender.query("CREATE (:Memory {id: 2})")
        })
    };
    let error = contender.join().unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));

    owner.rollback();
    let next_owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    next_owner.rollback();
}

#[test]
fn disjoint_primary_key_point_locks_allow_both_pessimistic_writers_to_commit() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    let mut second = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();

    first
        .query_sql("INSERT INTO public.messages (id, body) VALUES (1, 'first')")
        .unwrap();
    second
        .query_sql("INSERT INTO public.messages (id, body) VALUES (2, 'second')")
        .unwrap();
    first.commit().unwrap();
    second.commit().unwrap();

    let rows = db
        .query_sql("SELECT id FROM public.messages ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(rows[1].get("id"), Some(&Value::Int(2)));
}

#[test]
fn wal_group_commit_requires_performance_and_recovery_evidence() {
    let bounds = (
        NonZeroUsize::new(8).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(1),
    );
    let rejected = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            commit_count: 8,
            baseline_elapsed_micros: 100,
            baseline_fsync_count: 8,
            grouped_elapsed_micros: 100,
            grouped_fsync_count: 8,
            grouped_p95_commit_micros: 20,
            max_accepted_p95_commit_micros: 25,
            strict_recovery_verified: false,
            wal_order_verified: false,
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(rejected
        .to_string()
        .contains("throughput_improvement_not_proven"));
    assert!(rejected
        .to_string()
        .contains("strict_recovery_not_verified"));

    let admitted = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            commit_count: 8,
            baseline_elapsed_micros: 200,
            baseline_fsync_count: 8,
            grouped_elapsed_micros: 100,
            grouped_fsync_count: 1,
            grouped_p95_commit_micros: 20,
            max_accepted_p95_commit_micros: 25,
            strict_recovery_verified: true,
            wal_order_verified: true,
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap();
    assert_eq!(
        admitted.activation(),
        WalGroupCommitActivation::EvidenceValidated
    );
}

#[test]
fn wal_group_commit_shares_one_sync_without_changing_record_order() {
    const WRITERS: usize = 8;
    let path = super::unique_test_dir("wal_group_commit");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(WRITERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(5),
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    let barrier = Arc::new(Barrier::new(WRITERS));
    let writers = (0..WRITERS)
        .map(|id| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut transaction = db
                    .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                        Duration::from_secs(1),
                    ))
                    .unwrap();
                transaction
                    .query_sql(&format!(
                        "INSERT INTO public.messages (id, body) VALUES ({id}, 'writer-{id}')"
                    ))
                    .unwrap();
                barrier.wait();
                transaction.commit().unwrap();
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    let snapshot = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(
        snapshot.activation,
        WalGroupCommitActivation::BenchmarkCandidate
    );
    assert_eq!(snapshot.submitted_commits, WRITERS as u64);
    assert_eq!(snapshot.completed_commits, WRITERS as u64);
    assert_eq!(snapshot.shared_sync_count, 1);
    assert_eq!(snapshot.grouped_wal_entries, WRITERS as u64);
    assert_eq!(snapshot.max_observed_group_entries, WRITERS);
    assert_eq!(db.commit_epoch().unwrap(), WRITERS as u64 + 1);
    drop(db);

    let wal = super::read_test_wal(&path).unwrap();
    let lsns = wal
        .lines()
        .map(|line| line.split('\t').next().unwrap().parse::<u64>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lsns, (1..=WRITERS as u64 + 1).collect::<Vec<_>>());

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query_sql("SELECT id FROM public.messages ORDER BY id")
        .unwrap();
    assert_eq!(rows.rows.len(), WRITERS);
    assert_eq!(reopened.commit_epoch(), WRITERS as u64 + 1);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_group_sync_failure_rejects_commit_and_poisons_until_reopen() {
    let path = super::unique_test_dir("wal_group_sync_failure");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(1).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::ZERO,
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    let mut transaction = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    transaction
        .query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap();

    crate::store::set_wal_group_sync_failpoint(true);
    let error = transaction.commit().unwrap_err();
    assert!(error
        .to_string()
        .contains("WAL group durability barrier failed"));
    assert!(db
        .query_sql("SELECT id FROM public.messages")
        .unwrap_err()
        .to_string()
        .contains("close and reopen"));
    let snapshot = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(snapshot.completed_commits, 0);
    assert_eq!(snapshot.shared_sync_count, 0);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query_sql("SELECT id FROM public.messages")
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn conflicting_primary_key_point_lock_times_out_and_aborts_the_waiter() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap();
    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();

    let error = waiter
        .query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    assert!(waiter.commit().unwrap_err().to_string().contains("aborted"));
    owner.commit().unwrap();
}

#[test]
fn shared_primary_key_range_blocks_phantoms_but_not_the_excluded_boundary() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut reader = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    assert!(reader
        .query_sql("SELECT id FROM public.messages WHERE id >= 10 AND id < 20")
        .unwrap()
        .rows
        .is_empty());

    let mut outside = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    outside
        .query_sql("INSERT INTO public.messages (id) VALUES (20)")
        .unwrap();
    outside.commit().unwrap();

    let mut inside = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = inside
        .query_sql("INSERT INTO public.messages (id) VALUES (15)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    reader.commit().unwrap();

    assert_eq!(
        db.query_sql("SELECT id FROM public.messages ORDER BY id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn repeated_covered_point_read_keeps_its_snapshot_after_a_disjoint_commit() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap();
    let mut reader = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    assert_eq!(
        reader
            .query_sql("SELECT id FROM public.messages WHERE id = 1")
            .unwrap()
            .rows
            .len(),
        1
    );

    let mut writer = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    writer
        .query_sql("INSERT INTO public.messages (id) VALUES (2)")
        .unwrap();
    writer.commit().unwrap();

    assert_eq!(
        reader
            .query_sql("SELECT id FROM public.messages WHERE id = 1")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(reader
        .query_sql("SELECT id FROM public.messages WHERE id = 2")
        .unwrap_err()
        .to_string()
        .contains("cannot acquire a new lock after its snapshot changed"));
}

#[test]
fn point_lock_upgrade_cycle_selects_one_deadlock_victim() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    let mut second = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    first
        .query_sql("SELECT id FROM public.messages WHERE id = 1")
        .unwrap();
    second
        .query_sql("SELECT id FROM public.messages WHERE id = 2")
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let first_handle = std::thread::spawn(move || {
        first_barrier.wait();
        let result = first.query_sql("INSERT INTO public.messages (id) VALUES (2)");
        first.rollback();
        result
    });
    let second_handle = std::thread::spawn(move || {
        barrier.wait();
        let result = second.query_sql("INSERT INTO public.messages (id) VALUES (1)");
        second.rollback();
        result
    });
    let results = [first_handle.join().unwrap(), second_handle.join().unwrap()];

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let deadlock = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("one lock upgrader must be selected as the deadlock victim");
    assert!(deadlock.to_string().contains("deadlock detected"));
}

#[test]
fn pessimistic_transaction_keeps_uncommitted_data_invisible_to_snapshot_readers() {
    let db = Database::new().into_concurrent();
    let mut tx = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    tx.query("CREATE (:Memory {id: 1, title: 'pending'})")
        .unwrap();

    let mut before_commit = db.begin_read_transaction().unwrap();
    assert!(before_commit
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
    tx.commit().unwrap();

    assert!(before_commit
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
    let mut after_commit = db.begin_read_transaction().unwrap();
    assert_eq!(
        after_commit
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn dropped_pessimistic_transaction_releases_the_database_lock() {
    let db = Database::new().into_concurrent();
    {
        let mut tx = db
            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                Duration::from_secs(1),
            ))
            .unwrap();
        tx.query("CREATE (:Memory {id: 1})").unwrap();
    }

    let next = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    next.rollback();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn concurrent_transaction_publishes_graph_and_relational_writes_in_one_wal_epoch() {
    let path = super::unique_test_dir("concurrent_mixed_transaction");
    {
        let db = crate::ConcurrentDatabase::open(&path).unwrap();
        let mut tx = db
            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                Duration::from_secs(1),
            ))
            .unwrap();
        tx.query("CREATE (:Marker {id: 'graph-1'})").unwrap();
        tx.query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY)")
            .unwrap();
        tx.query_sql_with_params(
            "INSERT INTO public.messages (id) VALUES ($1)",
            &[Value::String("message-1".to_string())],
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(db.commit_epoch().unwrap(), 1);
    }

    let wal = super::read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 1);
    assert!(wal.contains("\tbatch\t"));
    {
        let db = crate::ConcurrentDatabase::open(&path).unwrap();
        assert_eq!(
            db.query("MATCH (m:Marker) RETURN m.id AS id")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            db.query_sql("SELECT id FROM public.messages")
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
