use serde_json::json;
use skein::{
    ConcurrentDatabase, ConcurrentTransactionOptions, Database, WalGroupCommitConfig,
    WalGroupCommitEvidence, WalGroupCommitSnapshot,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WORKERS: usize = 8;
const COMMITS_PER_WORKER: usize = 16;
const CONCURRENT_COMMIT_COUNT: usize = WORKERS * COMMITS_PER_WORKER;
const SINGLE_WRITER_COMMIT_COUNT: usize = 64;
const MAX_CONCURRENT_P95_REGRESSION_MICROS: u64 = 100;
const MAX_SINGLE_WRITER_P95_REGRESSION_MICROS: u64 = 50;

fn main() {
    let baseline = measure_concurrent("baseline-concurrent", WalGroupCommitConfig::disabled());
    let candidate = measure_concurrent(
        "candidate-concurrent",
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(WORKERS).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::from_micros(500),
        )
        .expect("benchmark candidate bounds must be valid"),
    );
    let single_writer_baseline =
        measure_single_writer("baseline-single-writer", WalGroupCommitConfig::disabled());
    let single_writer_candidate = measure_single_writer(
        "candidate-single-writer",
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(WORKERS).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::from_micros(500),
        )
        .expect("benchmark candidate bounds must be valid"),
    );
    let evidence = WalGroupCommitEvidence {
        commit_count: CONCURRENT_COMMIT_COUNT,
        baseline_elapsed_micros: baseline.elapsed_micros,
        baseline_fsync_count: CONCURRENT_COMMIT_COUNT as u64,
        grouped_elapsed_micros: candidate.elapsed_micros,
        grouped_fsync_count: candidate.group_commit.shared_sync_count,
        grouped_p95_commit_micros: candidate.p95_commit_micros,
        max_accepted_p95_commit_micros: baseline
            .p95_commit_micros
            .saturating_add(MAX_CONCURRENT_P95_REGRESSION_MICROS),
        single_writer_commit_count: SINGLE_WRITER_COMMIT_COUNT,
        single_writer_grouped_p95_commit_micros: single_writer_candidate.p95_commit_micros,
        max_accepted_single_writer_p95_commit_micros: single_writer_baseline
            .p95_commit_micros
            .saturating_add(MAX_SINGLE_WRITER_P95_REGRESSION_MICROS),
        strict_recovery_verified: candidate.strict_recovery_verified,
        wal_order_verified: candidate.wal_order_verified,
    };
    let admission = WalGroupCommitConfig::enabled_after_evidence(
        evidence,
        NonZeroUsize::new(WORKERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_micros(500),
    );

    println!(
        "wal_group_commit {}",
        json!({
            "concurrent": {
                "workers": WORKERS,
                "commit_count": CONCURRENT_COMMIT_COUNT,
                "baseline": baseline.to_json(),
                "candidate": candidate.to_json(),
                "max_p95_regression_micros": MAX_CONCURRENT_P95_REGRESSION_MICROS,
            },
            "single_writer": {
                "commit_count": SINGLE_WRITER_COMMIT_COUNT,
                "baseline": single_writer_baseline.to_json(),
                "candidate": single_writer_candidate.to_json(),
                "max_p95_regression_micros": MAX_SINGLE_WRITER_P95_REGRESSION_MICROS,
            },
            "throughput_improvement_ratio": baseline.elapsed_micros as f64
                / candidate.elapsed_micros.max(1) as f64,
            "evidence_admitted": admission.is_ok(),
            "evidence_rejection": admission.err().map(|error| error.to_string()),
        })
    );
}

fn measure_concurrent(label: &str, group_commit: WalGroupCommitConfig) -> Measurement {
    measure(label, group_commit, CONCURRENT_COMMIT_COUNT, |database| {
        let barrier = Arc::new(Barrier::new(WORKERS));
        let writers = (0..WORKERS)
                .map(|worker| {
                    let database = database.clone();
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let mut latencies = Vec::with_capacity(COMMITS_PER_WORKER);
                        for round in 0..COMMITS_PER_WORKER {
                            let id = worker * COMMITS_PER_WORKER + round;
                            let mut transaction = database
                                .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                                    Duration::from_secs(5),
                                ))
                                .expect("benchmark transaction must begin");
                            transaction
                                .query_sql(&format!(
                                    "INSERT INTO public.messages (id, body) VALUES ({id}, 'payload-{id}')"
                                ))
                                .expect("benchmark insert must stage");
                            barrier.wait();
                            let commit_started = Instant::now();
                            transaction.commit().expect("benchmark commit must succeed");
                            latencies.push(elapsed_micros(commit_started));
                        }
                        latencies
                    })
                })
                .collect::<Vec<_>>();
        writers
            .into_iter()
            .flat_map(|writer| writer.join().expect("benchmark writer must join"))
            .collect()
    })
}

fn measure_single_writer(label: &str, group_commit: WalGroupCommitConfig) -> Measurement {
    measure(
        label,
        group_commit,
        SINGLE_WRITER_COMMIT_COUNT,
        |database| {
            let mut latencies = Vec::with_capacity(SINGLE_WRITER_COMMIT_COUNT);
            for id in 0..SINGLE_WRITER_COMMIT_COUNT {
                let mut transaction = database
                    .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                        Duration::from_secs(5),
                    ))
                    .expect("benchmark transaction must begin");
                transaction
                    .query_sql(&format!(
                        "INSERT INTO public.messages (id, body) VALUES ({id}, 'payload-{id}')"
                    ))
                    .expect("benchmark insert must stage");
                let commit_started = Instant::now();
                transaction.commit().expect("benchmark commit must succeed");
                latencies.push(elapsed_micros(commit_started));
            }
            latencies
        },
    )
}

fn measure(
    label: &str,
    group_commit: WalGroupCommitConfig,
    commit_count: usize,
    execute: impl FnOnce(&ConcurrentDatabase) -> Vec<u64>,
) -> Measurement {
    let path = benchmark_path(label);
    let mut database = Database::open(&path).expect("benchmark database must open");
    database
        .query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .expect("benchmark schema must be created");
    let database = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    let started = Instant::now();
    let mut commit_latencies = execute(&database);
    let elapsed_micros = elapsed_micros(started);
    commit_latencies.sort_unstable();
    let group_commit = database
        .wal_group_commit_snapshot()
        .expect("group commit metrics must be readable");
    let final_epoch = database
        .commit_epoch()
        .expect("benchmark commit epoch must be readable");
    drop(database);

    let wal_order_verified = verify_wal_order(&path, final_epoch);
    let strict_recovery_verified = Database::open(&path)
        .and_then(|mut reopened| {
            reopened
                .query_sql("SELECT id FROM public.messages ORDER BY id")
                .map(|rows| {
                    rows.rows.len() == commit_count && reopened.commit_epoch() == final_epoch
                })
        })
        .unwrap_or(false);
    std::fs::remove_dir_all(&path).expect("benchmark database must be removable");
    Measurement {
        commit_count,
        elapsed_micros,
        p50_commit_micros: percentile(&commit_latencies, 50),
        p95_commit_micros: percentile(&commit_latencies, 95),
        p99_commit_micros: percentile(&commit_latencies, 99),
        group_commit,
        strict_recovery_verified,
        wal_order_verified,
    }
}

fn verify_wal_order(path: &std::path::Path, final_epoch: u64) -> bool {
    let Ok(wal) = std::fs::read_to_string(path.join("wal.0.skein")) else {
        return false;
    };
    let Some((_, records)) = wal.split_once('\n') else {
        return false;
    };
    let lsns = records
        .lines()
        .map(|line| line.split('\t').next()?.parse::<u64>().ok())
        .collect::<Option<Vec<_>>>();
    lsns.is_some_and(|lsns| lsns == (1..=final_epoch).collect::<Vec<_>>())
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    let index = values
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(values.len().saturating_sub(1));
    values.get(index).copied().unwrap_or_default()
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn benchmark_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "skein-wal-group-commit-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

struct Measurement {
    commit_count: usize,
    elapsed_micros: u64,
    p50_commit_micros: u64,
    p95_commit_micros: u64,
    p99_commit_micros: u64,
    group_commit: WalGroupCommitSnapshot,
    strict_recovery_verified: bool,
    wal_order_verified: bool,
}

impl Measurement {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "elapsed_micros": self.elapsed_micros,
            "commits_per_second": self.commit_count as f64 * 1_000_000.0
                / self.elapsed_micros.max(1) as f64,
            "p50_commit_micros": self.p50_commit_micros,
            "p95_commit_micros": self.p95_commit_micros,
            "p99_commit_micros": self.p99_commit_micros,
            "shared_sync_count": self.group_commit.shared_sync_count,
            "group_count": self.group_commit.group_count,
            "coalescing_wait_count": self.group_commit.coalescing_wait_count,
            "max_observed_group_entries": self.group_commit.max_observed_group_entries,
            "max_observed_group_bytes": self.group_commit.max_observed_group_bytes,
            "strict_recovery_verified": self.strict_recovery_verified,
            "wal_order_verified": self.wal_order_verified,
        })
    }
}
