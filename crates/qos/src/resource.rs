use crate::{StorageDeviceProfile, StorageMediaKind};
use std::num::NonZeroUsize;
#[cfg(any(target_os = "linux", test))]
use std::path::{Path, PathBuf};
use sysinfo::System;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeResourceBudget {
    pub host_parallelism: NonZeroUsize,
    pub cgroup_quota_parallelism: Option<NonZeroUsize>,
    pub cpuset_parallelism: Option<NonZeroUsize>,
    pub effective_parallelism: NonZeroUsize,
    pub foreground_parallelism: NonZeroUsize,
    pub background_parallelism: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoConcurrencyBudget {
    pub foreground_depth: NonZeroUsize,
    pub background_depth: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeMemoryPressure {
    Normal,
    Elevated,
    Critical,
    #[default]
    Unknown,
}

impl RuntimeMemoryPressure {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Elevated => "elevated",
            Self::Critical => "critical",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeMemorySnapshot {
    pub host_total_bytes: Option<u64>,
    pub host_available_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
    pub cgroup_high_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub effective_limit_bytes: Option<u64>,
    pub effective_available_bytes: Option<u64>,
    pub pressure: RuntimeMemoryPressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeResourceSnapshot {
    pub cpu: RuntimeResourceBudget,
    pub memory: RuntimeMemorySnapshot,
}

impl RuntimeResourceBudget {
    pub fn detect() -> Self {
        let host = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        #[cfg(target_os = "linux")]
        let (quota, cpuset) = {
            let cgroup = linux_cgroup_v2_directory();
            (
                read_cpu_max(&cgroup.join("cpu.max")),
                read_cpuset(&cgroup.join("cpuset.cpus.effective"))
                    .or_else(|| read_cpuset(&cgroup.join("cpuset.cpus"))),
            )
        };
        #[cfg(not(target_os = "linux"))]
        let (quota, cpuset) = (None, None);
        Self::from_limits(host, quota, cpuset)
    }

    pub fn from_limits(
        host_parallelism: NonZeroUsize,
        cgroup_quota_parallelism: Option<NonZeroUsize>,
        cpuset_parallelism: Option<NonZeroUsize>,
    ) -> Self {
        let effective = [
            Some(host_parallelism),
            cgroup_quota_parallelism,
            cpuset_parallelism,
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(NonZeroUsize::MIN);
        let background = NonZeroUsize::new((effective.get() / 4).max(1))
            .expect("background parallelism is always non-zero");
        Self {
            host_parallelism,
            cgroup_quota_parallelism,
            cpuset_parallelism,
            effective_parallelism: effective,
            foreground_parallelism: effective,
            background_parallelism: background,
        }
    }
}

impl RuntimeMemorySnapshot {
    pub fn detect() -> Self {
        let mut system = System::new();
        system.refresh_memory();
        let host_total_bytes = non_zero_memory(system.total_memory());
        let host_available_bytes = non_zero_memory(system.available_memory());

        #[cfg(target_os = "linux")]
        let (cgroup_limit_bytes, cgroup_high_bytes, cgroup_current_bytes) = {
            let cgroup = linux_cgroup_v2_directory();
            (
                read_memory_limit(&cgroup.join("memory.max")),
                read_memory_limit(&cgroup.join("memory.high")),
                read_memory_current(&cgroup.join("memory.current")),
            )
        };
        #[cfg(not(target_os = "linux"))]
        let (cgroup_limit_bytes, cgroup_high_bytes, cgroup_current_bytes) = (None, None, None);

        Self::from_limits(
            host_total_bytes,
            host_available_bytes,
            cgroup_limit_bytes,
            cgroup_high_bytes,
            cgroup_current_bytes,
        )
    }

    pub fn from_limits(
        host_total_bytes: Option<u64>,
        host_available_bytes: Option<u64>,
        cgroup_limit_bytes: Option<u64>,
        cgroup_high_bytes: Option<u64>,
        cgroup_current_bytes: Option<u64>,
    ) -> Self {
        let effective_limit_bytes = [host_total_bytes, cgroup_limit_bytes, cgroup_high_bytes]
            .into_iter()
            .flatten()
            .min();
        let cgroup_available_bytes = effective_limit_bytes
            .map(|limit| limit.saturating_sub(cgroup_current_bytes.unwrap_or_default()));
        let effective_available_bytes = [host_available_bytes, cgroup_available_bytes]
            .into_iter()
            .flatten()
            .min();
        let pressure = memory_pressure(
            effective_limit_bytes,
            effective_available_bytes,
            cgroup_high_bytes,
            cgroup_current_bytes,
        );
        Self {
            host_total_bytes,
            host_available_bytes,
            cgroup_limit_bytes,
            cgroup_high_bytes,
            cgroup_current_bytes,
            effective_limit_bytes,
            effective_available_bytes,
            pressure,
        }
    }
}

impl RuntimeResourceSnapshot {
    pub fn detect() -> Self {
        Self {
            cpu: RuntimeResourceBudget::detect(),
            memory: RuntimeMemorySnapshot::detect(),
        }
    }

    pub const fn from_parts(cpu: RuntimeResourceBudget, memory: RuntimeMemorySnapshot) -> Self {
        Self { cpu, memory }
    }
}

impl IoConcurrencyBudget {
    pub fn desktop_bound(_cpu: RuntimeResourceBudget) -> Self {
        Self::desktop_bound_for_device(StorageDeviceProfile::default())
    }

    pub fn desktop_bound_for_device(device: StorageDeviceProfile) -> Self {
        let foreground = match device.media_kind {
            StorageMediaKind::Rotational => 2,
            StorageMediaKind::NonRotational => device
                .queue_depth_hint
                .map(NonZeroUsize::get)
                .unwrap_or(8)
                .clamp(4, 32),
            StorageMediaKind::Memory => 8,
            StorageMediaKind::Network | StorageMediaKind::Virtual | StorageMediaKind::Unknown => 4,
        };
        let background = (foreground / 4).clamp(1, 4);
        Self::new(foreground, background)
    }

    pub fn mobile_embedded(_cpu: RuntimeResourceBudget) -> Self {
        Self::mobile_embedded_for_device(StorageDeviceProfile::default())
    }

    pub fn mobile_embedded_for_device(device: StorageDeviceProfile) -> Self {
        let foreground = match device.media_kind {
            StorageMediaKind::Rotational
            | StorageMediaKind::Network
            | StorageMediaKind::Virtual => 1,
            StorageMediaKind::NonRotational | StorageMediaKind::Memory => device
                .queue_depth_hint
                .map(NonZeroUsize::get)
                .unwrap_or(4)
                .clamp(1, 4),
            StorageMediaKind::Unknown => 2,
        };
        Self::new(foreground, 1)
    }

    pub fn new(foreground_depth: usize, background_depth: usize) -> Self {
        Self {
            foreground_depth: NonZeroUsize::new(foreground_depth.max(1))
                .expect("foreground I/O depth is always non-zero"),
            background_depth: NonZeroUsize::new(background_depth.max(1))
                .expect("background I/O depth is always non-zero"),
        }
    }
}

#[cfg(target_os = "linux")]
fn read_cpu_max(path: &Path) -> Option<NonZeroUsize> {
    let value = std::fs::read_to_string(path).ok()?;
    parse_cpu_max(&value)
}

#[cfg(target_os = "linux")]
fn read_cpuset(path: &Path) -> Option<NonZeroUsize> {
    let value = std::fs::read_to_string(path).ok()?;
    parse_cpuset(&value)
}

#[cfg(target_os = "linux")]
fn read_memory_limit(path: &Path) -> Option<u64> {
    let value = std::fs::read_to_string(path).ok()?;
    parse_memory_limit(&value)
}

#[cfg(target_os = "linux")]
fn read_memory_current(path: &Path) -> Option<u64> {
    let value = std::fs::read_to_string(path).ok()?;
    value.trim().parse::<u64>().ok()
}

#[cfg(target_os = "linux")]
fn linux_cgroup_v2_directory() -> PathBuf {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok();
    cgroup_v2_directory(
        Path::new("/sys/fs/cgroup"),
        cgroup.as_deref().unwrap_or_default(),
    )
}

#[cfg(any(target_os = "linux", test))]
fn cgroup_v2_directory(root: &Path, self_cgroup: &str) -> PathBuf {
    let Some(relative) = self_cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?;
        (hierarchy == "0" && controllers.is_empty()).then_some(path)
    }) else {
        return root.to_path_buf();
    };
    relative
        .trim_start_matches('/')
        .split('/')
        .filter(|component| !component.is_empty() && *component != "." && *component != "..")
        .fold(root.to_path_buf(), |path, component| path.join(component))
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpu_max(value: &str) -> Option<NonZeroUsize> {
    let mut fields = value.split_whitespace();
    let quota = fields.next()?;
    let period = fields.next()?.parse::<usize>().ok()?;
    if quota == "max" || period == 0 {
        return None;
    }
    let quota = quota.parse::<usize>().ok()?;
    NonZeroUsize::new((quota / period).max(1))
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpuset(value: &str) -> Option<NonZeroUsize> {
    let mut ranges = value
        .trim()
        .split(',')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut bounds = part.splitn(2, '-');
            let start = bounds.next()?.parse::<usize>().ok()?;
            let end = bounds
                .next()
                .map(str::parse::<usize>)
                .transpose()
                .ok()?
                .unwrap_or(start);
            (start <= end).then_some((start, end))
        })
        .collect::<Option<Vec<_>>>()?;
    ranges.sort_unstable();
    let mut count = 0usize;
    let mut current: Option<(usize, usize)> = None;
    for (start, end) in ranges {
        match current {
            Some((current_start, current_end)) if start <= current_end.saturating_add(1) => {
                current = Some((current_start, current_end.max(end)));
            }
            Some((current_start, current_end)) => {
                count = count.saturating_add(current_end - current_start + 1);
                current = Some((start, end));
            }
            None => current = Some((start, end)),
        }
    }
    if let Some((start, end)) = current {
        count = count.saturating_add(end - start + 1);
    }
    NonZeroUsize::new(count)
}

#[cfg(any(target_os = "linux", test))]
fn parse_memory_limit(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.is_empty() || value == "max" {
        return None;
    }
    value.parse::<u64>().ok()
}

fn non_zero_memory(bytes: u64) -> Option<u64> {
    (bytes > 0).then_some(bytes)
}

fn memory_pressure(
    effective_limit_bytes: Option<u64>,
    effective_available_bytes: Option<u64>,
    cgroup_high_bytes: Option<u64>,
    cgroup_current_bytes: Option<u64>,
) -> RuntimeMemoryPressure {
    if cgroup_high_bytes
        .zip(cgroup_current_bytes)
        .is_some_and(|(high, current)| current >= high)
    {
        return RuntimeMemoryPressure::Critical;
    }
    let Some((limit, available)) = effective_limit_bytes.zip(effective_available_bytes) else {
        return RuntimeMemoryPressure::Unknown;
    };
    if limit == 0 || u128::from(available) * 100 <= u128::from(limit) * 10 {
        RuntimeMemoryPressure::Critical
    } else if u128::from(available) * 100 <= u128::from(limit) * 25 {
        RuntimeMemoryPressure::Elevated
    } else {
        RuntimeMemoryPressure::Normal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_budget_uses_the_smallest_cpu_limit() {
        let budget = RuntimeResourceBudget::from_limits(
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(6),
            NonZeroUsize::new(4),
        );
        assert_eq!(budget.effective_parallelism.get(), 4);
        assert_eq!(budget.foreground_parallelism.get(), 4);
        assert_eq!(budget.background_parallelism.get(), 1);
    }

    #[test]
    fn parses_cgroup_v2_cpu_quota() {
        assert_eq!(parse_cpu_max("250000 100000"), NonZeroUsize::new(2));
        assert_eq!(parse_cpu_max("max 100000"), None);
    }

    #[test]
    fn resolves_the_process_cgroup_v2_subdirectory() {
        let root = Path::new("/sys/fs/cgroup");
        assert_eq!(
            cgroup_v2_directory(root, "0::/user.slice/app.scope\n"),
            root.join("user.slice/app.scope")
        );
        assert_eq!(cgroup_v2_directory(root, ""), root);
        assert_eq!(
            cgroup_v2_directory(root, "0::/../../escape\n"),
            root.join("escape")
        );
    }

    #[test]
    fn parses_cgroup_v2_memory_limits() {
        assert_eq!(parse_memory_limit("1073741824\n"), Some(1_073_741_824));
        assert_eq!(parse_memory_limit("max\n"), None);
        assert_eq!(parse_memory_limit("invalid"), None);
    }

    #[test]
    fn memory_snapshot_uses_cgroup_headroom_and_high_watermark() {
        let snapshot = RuntimeMemorySnapshot::from_limits(
            Some(16 << 30),
            Some(8 << 30),
            Some(4 << 30),
            Some(3 << 30),
            Some(2 << 30),
        );
        assert_eq!(snapshot.effective_limit_bytes, Some(3 << 30));
        assert_eq!(snapshot.effective_available_bytes, Some(1 << 30));
        assert_eq!(snapshot.pressure, RuntimeMemoryPressure::Normal);

        let pressured = RuntimeMemorySnapshot::from_limits(
            Some(16 << 30),
            Some(8 << 30),
            Some(4 << 30),
            Some(3 << 30),
            Some(3 << 30),
        );
        assert_eq!(pressured.pressure, RuntimeMemoryPressure::Critical);
    }

    #[test]
    fn platform_memory_detection_has_internally_consistent_limits() {
        let snapshot = RuntimeMemorySnapshot::detect();
        if let (Some(limit), Some(available)) = (
            snapshot.effective_limit_bytes,
            snapshot.effective_available_bytes,
        ) {
            assert!(available <= limit);
        }
    }

    #[test]
    fn parses_and_merges_cpuset_ranges() {
        assert_eq!(parse_cpuset("0-3,2-5,8"), NonZeroUsize::new(7));
        assert_eq!(parse_cpuset(""), None);
    }

    #[test]
    fn desktop_io_budget_uses_device_evidence_instead_of_cpu_count() {
        let device = StorageDeviceProfile::host_provided(
            StorageMediaKind::NonRotational,
            NonZeroUsize::new(12),
        );
        let io = IoConcurrencyBudget::desktop_bound_for_device(device);
        assert_eq!(io.foreground_depth.get(), 12);
        assert_eq!(io.background_depth.get(), 3);
    }

    #[test]
    fn mobile_io_budget_stays_conservative() {
        let device = StorageDeviceProfile::host_provided(
            StorageMediaKind::NonRotational,
            NonZeroUsize::new(32),
        );
        let io = IoConcurrencyBudget::mobile_embedded_for_device(device);
        assert_eq!(io.foreground_depth.get(), 4);
        assert_eq!(io.background_depth.get(), 1);
    }

    #[test]
    fn unknown_device_budget_is_not_derived_from_cpu_count() {
        let low_cpu = RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None);
        let high_cpu =
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(64).unwrap(), None, None);

        assert_eq!(
            IoConcurrencyBudget::desktop_bound(low_cpu),
            IoConcurrencyBudget::desktop_bound(high_cpu)
        );
        assert_eq!(
            IoConcurrencyBudget::mobile_embedded(low_cpu),
            IoConcurrencyBudget::mobile_embedded(high_cpu)
        );
    }
}
