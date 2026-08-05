use crate::ExecutionMemoryConfig;
use skein_core::{Result, SkeinError};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const SPILL_FILE_PREFIX: &str = "skein-spill-v1-";
pub(super) const SPILL_FILE_SUFFIX: &str = ".spill";
static SPILL_POOLS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<SharedSpillPool>>>> = OnceLock::new();
static PROCESS_MARKER: OnceLock<String> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillPoolSnapshot {
    pub max_total_bytes: u64,
    pub max_total_runs: usize,
    pub min_free_bytes: u64,
    pub active_bytes: u64,
    pub peak_active_bytes: u64,
    pub pending_write_bytes: u64,
    pub active_runs: usize,
    pub peak_active_runs: usize,
    pub orphan_files_removed: u64,
    pub orphan_bytes_removed: u64,
    pub orphan_cleanup_failures: u64,
    pub run_delete_failures: u64,
}

#[derive(Debug, Clone, Copy)]
struct SpillPoolLimits {
    max_total_bytes: u64,
    max_total_runs: usize,
    min_free_bytes: u64,
}

impl SpillPoolLimits {
    fn from_config(memory: &ExecutionMemoryConfig) -> Self {
        Self {
            max_total_bytes: memory.max_total_spill_bytes.get(),
            max_total_runs: memory.max_total_spill_runs.get(),
            min_free_bytes: memory.min_spill_free_bytes.get(),
        }
    }

    fn tighten(&mut self, other: Self) {
        self.max_total_bytes = self.max_total_bytes.min(other.max_total_bytes);
        self.max_total_runs = self.max_total_runs.min(other.max_total_runs);
        self.min_free_bytes = self.min_free_bytes.max(other.min_free_bytes);
    }
}

#[derive(Debug, Default)]
struct OrphanCleanupStats {
    files_removed: u64,
    bytes_removed: u64,
    failures: u64,
}

#[derive(Debug)]
struct SpillPoolState {
    limits: SpillPoolLimits,
    active_bytes: u64,
    peak_active_bytes: u64,
    pending_write_bytes: u64,
    active_runs: usize,
    peak_active_runs: usize,
    orphan_cleanup: OrphanCleanupStats,
    run_delete_failures: u64,
}

#[derive(Debug)]
struct SharedSpillPool {
    directory: PathBuf,
    state: Mutex<SpillPoolState>,
}

#[derive(Debug, Clone)]
pub(crate) struct SpillPool {
    shared: Arc<SharedSpillPool>,
}

impl SpillPool {
    pub(crate) fn open(memory: &ExecutionMemoryConfig) -> Result<Self> {
        std::fs::create_dir_all(&memory.spill_directory).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to create spill directory '{}': {error}",
                memory.spill_directory.display()
            ))
        })?;
        let directory = std::fs::canonicalize(&memory.spill_directory).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to resolve spill directory '{}': {error}",
                memory.spill_directory.display()
            ))
        })?;
        let limits = SpillPoolLimits::from_config(memory);
        let mut pools = lock_unpoisoned(SPILL_POOLS.get_or_init(Default::default));
        if let Some(shared) = pools.get(&directory) {
            lock_unpoisoned(&shared.state).limits.tighten(limits);
            return Ok(Self {
                shared: Arc::clone(shared),
            });
        }
        let orphan_cleanup = cleanup_orphan_files(
            &directory,
            memory.spill_orphan_grace_period,
            process_marker(),
        )?;
        let shared = Arc::new(SharedSpillPool {
            directory: directory.clone(),
            state: Mutex::new(SpillPoolState {
                limits,
                active_bytes: 0,
                peak_active_bytes: 0,
                pending_write_bytes: 0,
                active_runs: 0,
                peak_active_runs: 0,
                orphan_cleanup,
                run_delete_failures: 0,
            }),
        });
        pools.insert(directory, Arc::clone(&shared));
        Ok(Self { shared })
    }

    pub(super) fn directory(&self) -> &Path {
        &self.shared.directory
    }

    pub(super) fn begin_run(&self, operator: &str) -> Result<()> {
        let mut state = lock_unpoisoned(&self.shared.state);
        if state.active_runs >= state.limits.max_total_runs {
            return Err(SkeinError::Execution(format!(
                "{operator} exceeded shared max_total_spill_runs {}",
                state.limits.max_total_runs
            )));
        }
        state.active_runs = state.active_runs.saturating_add(1);
        state.peak_active_runs = state.peak_active_runs.max(state.active_runs);
        Ok(())
    }

    pub(super) fn cancel_run(&self) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.active_runs = state.active_runs.saturating_sub(1);
    }

    pub(crate) fn reserve_bytes(
        &self,
        operator: &str,
        bytes: u64,
    ) -> Result<SpillWriteReservation> {
        let mut state = lock_unpoisoned(&self.shared.state);
        let next_active = state.active_bytes.saturating_add(bytes);
        if next_active > state.limits.max_total_bytes {
            return Err(SkeinError::Execution(format!(
                "{operator} exceeded shared max_total_spill_bytes {} (next active total {next_active})",
                state.limits.max_total_bytes
            )));
        }
        let available = fs2::available_space(&self.shared.directory).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to inspect free space for spill directory '{}': {error}",
                self.shared.directory.display()
            ))
        })?;
        let required = state
            .limits
            .min_free_bytes
            .saturating_add(state.pending_write_bytes)
            .saturating_add(bytes);
        if available < required {
            return Err(SkeinError::Execution(format!(
                "{operator} cannot preserve min_spill_free_bytes {}: filesystem has {available} bytes available and {bytes} bytes were requested",
                state.limits.min_free_bytes
            )));
        }
        state.active_bytes = next_active;
        state.peak_active_bytes = state.peak_active_bytes.max(next_active);
        state.pending_write_bytes = state.pending_write_bytes.saturating_add(bytes);
        drop(state);
        Ok(SpillWriteReservation {
            pool: self.clone(),
            bytes,
            committed: false,
        })
    }

    fn commit_write(&self, bytes: u64) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.pending_write_bytes = state.pending_write_bytes.saturating_sub(bytes);
    }

    fn rollback_write(&self, bytes: u64) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.pending_write_bytes = state.pending_write_bytes.saturating_sub(bytes);
        state.active_bytes = state.active_bytes.saturating_sub(bytes);
    }

    fn finish_run(&self, bytes: u64, pending_bytes: u64, deleted: bool) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.pending_write_bytes = state.pending_write_bytes.saturating_sub(pending_bytes);
        if deleted {
            state.active_bytes = state.active_bytes.saturating_sub(bytes);
            state.active_runs = state.active_runs.saturating_sub(1);
        } else {
            state.run_delete_failures = state.run_delete_failures.saturating_add(1);
        }
    }

    fn snapshot(&self) -> SpillPoolSnapshot {
        let state = lock_unpoisoned(&self.shared.state);
        SpillPoolSnapshot {
            max_total_bytes: state.limits.max_total_bytes,
            max_total_runs: state.limits.max_total_runs,
            min_free_bytes: state.limits.min_free_bytes,
            active_bytes: state.active_bytes,
            peak_active_bytes: state.peak_active_bytes,
            pending_write_bytes: state.pending_write_bytes,
            active_runs: state.active_runs,
            peak_active_runs: state.peak_active_runs,
            orphan_files_removed: state.orphan_cleanup.files_removed,
            orphan_bytes_removed: state.orphan_cleanup.bytes_removed,
            orphan_cleanup_failures: state.orphan_cleanup.failures,
            run_delete_failures: state.run_delete_failures,
        }
    }
}

pub(crate) struct SpillWriteReservation {
    pool: SpillPool,
    bytes: u64,
    committed: bool,
}

impl SpillWriteReservation {
    pub(super) fn commit(mut self, lease: &RunLease) {
        lease
            .reserved_bytes
            .fetch_add(self.bytes, Ordering::Relaxed);
        lease
            .pending_write_bytes
            .fetch_add(self.bytes, Ordering::Relaxed);
        self.committed = true;
    }
}

impl Drop for SpillWriteReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.pool.rollback_write(self.bytes);
        }
    }
}

#[derive(Debug)]
pub(super) struct RunLease {
    path: PathBuf,
    pool: SpillPool,
    reserved_bytes: AtomicU64,
    pending_write_bytes: AtomicU64,
}

impl RunLease {
    pub(super) fn new(path: PathBuf, pool: SpillPool) -> Self {
        Self {
            path,
            pool,
            reserved_bytes: AtomicU64::new(0),
            pending_write_bytes: AtomicU64::new(0),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn mark_flushed(&self) {
        let pending_bytes = self.pending_write_bytes.swap(0, Ordering::Relaxed);
        self.pool.commit_write(pending_bytes);
    }
}

impl Drop for RunLease {
    fn drop(&mut self) {
        let deleted = match std::fs::remove_file(&self.path) {
            Ok(()) => true,
            Err(error) if error.kind() == ErrorKind::NotFound => true,
            Err(_) => false,
        };
        self.pool.finish_run(
            self.reserved_bytes.load(Ordering::Relaxed),
            self.pending_write_bytes.load(Ordering::Relaxed),
            deleted,
        );
    }
}

pub(crate) fn spill_pool_snapshot(memory: &ExecutionMemoryConfig) -> Result<SpillPoolSnapshot> {
    Ok(SpillPool::open(memory)?.snapshot())
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) fn process_marker() -> &'static str {
    PROCESS_MARKER.get_or_init(|| {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        format!("{}-{started}", std::process::id())
    })
}

fn cleanup_orphan_files(
    directory: &Path,
    grace_period: Duration,
    current_process_marker: &str,
) -> Result<OrphanCleanupStats> {
    let mut stats = OrphanCleanupStats::default();
    let current_prefix = format!("{SPILL_FILE_PREFIX}{current_process_marker}-");
    let entries = std::fs::read_dir(directory).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to inspect spill directory '{}': {error}",
            directory.display()
        ))
    })?;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                stats.failures = stats.failures.saturating_add(1);
                continue;
            }
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(SPILL_FILE_PREFIX)
            || !name.ends_with(SPILL_FILE_SUFFIX)
            || name.starts_with(&current_prefix)
        {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => continue,
            Err(_) => {
                stats.failures = stats.failures.saturating_add(1);
                continue;
            }
        };
        let old_enough = metadata
            .modified()
            .ok()
            .and_then(|modified| SystemTime::now().duration_since(modified).ok())
            .is_some_and(|age| age >= grace_period);
        if !old_enough {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {
                stats.files_removed = stats.files_removed.saturating_add(1);
                stats.bytes_removed = stats.bytes_removed.saturating_add(metadata.len());
            }
            Err(_) => stats.failures = stats.failures.saturating_add(1),
        }
    }
    Ok(stats)
}
