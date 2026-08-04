use crate::{StorageDeviceProfile, StorageMediaKind};
#[cfg(target_os = "linux")]
use skein_cgroup::LinuxCgroupSnapshot;
#[cfg(any(target_os = "linux", test))]
use skein_cgroup::LinuxCgroupValue;
use std::num::NonZeroUsize;
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
        let (quota, cpuset) = linux_cgroup_cpu_limits();
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
        let (cgroup_limit_bytes, cgroup_high_bytes, cgroup_current_bytes) =
            linux_cgroup_memory_limits();
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
        let cgroup_effective_limit_bytes = [cgroup_limit_bytes, cgroup_high_bytes]
            .into_iter()
            .flatten()
            .min();
        let effective_limit_bytes = [host_total_bytes, cgroup_effective_limit_bytes]
            .into_iter()
            .flatten()
            .min();
        let cgroup_available_bytes = cgroup_effective_limit_bytes
            .zip(cgroup_current_bytes)
            .map(|(limit, current)| limit.saturating_sub(current));
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
fn linux_cgroup_cpu_limits() -> (Option<NonZeroUsize>, Option<NonZeroUsize>) {
    let snapshot = LinuxCgroupSnapshot::detect();
    (
        admitted_cpu_limit(snapshot.cpu_quota_parallelism),
        admitted_cpu_limit(snapshot.cpuset_parallelism),
    )
}

#[cfg(any(target_os = "linux", test))]
fn admitted_cpu_limit(value: LinuxCgroupValue<NonZeroUsize>) -> Option<NonZeroUsize> {
    match value {
        LinuxCgroupValue::Value(value) => Some(value),
        LinuxCgroupValue::Invalid => Some(NonZeroUsize::MIN),
        LinuxCgroupValue::Absent | LinuxCgroupValue::Unlimited => None,
    }
}

#[cfg(target_os = "linux")]
fn linux_cgroup_memory_limits() -> (Option<u64>, Option<u64>, Option<u64>) {
    let snapshot = LinuxCgroupSnapshot::detect();
    (
        admitted_memory_limit(snapshot.memory_limit_bytes),
        admitted_memory_limit(snapshot.memory_high_bytes),
        admitted_memory_current(snapshot.memory_current_bytes),
    )
}

#[cfg(any(target_os = "linux", test))]
fn admitted_memory_limit(value: LinuxCgroupValue<u64>) -> Option<u64> {
    match value {
        LinuxCgroupValue::Value(value) => Some(value),
        LinuxCgroupValue::Invalid => Some(0),
        LinuxCgroupValue::Absent | LinuxCgroupValue::Unlimited => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn admitted_memory_current(value: LinuxCgroupValue<u64>) -> Option<u64> {
    match value {
        LinuxCgroupValue::Value(value) => Some(value),
        LinuxCgroupValue::Invalid => Some(u64::MAX),
        LinuxCgroupValue::Absent | LinuxCgroupValue::Unlimited => None,
    }
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
    fn invalid_cgroup_values_fail_closed_at_admission_boundary() {
        assert_eq!(
            admitted_cpu_limit(LinuxCgroupValue::Invalid),
            Some(NonZeroUsize::MIN)
        );
        assert_eq!(admitted_memory_limit(LinuxCgroupValue::Invalid), Some(0));
        assert_eq!(
            admitted_memory_current(LinuxCgroupValue::Invalid),
            Some(u64::MAX)
        );

        let memory = RuntimeMemorySnapshot::from_limits(
            Some(16 << 30),
            Some(8 << 30),
            admitted_memory_limit(LinuxCgroupValue::Invalid),
            None,
            admitted_memory_current(LinuxCgroupValue::Invalid),
        );
        assert_eq!(memory.effective_limit_bytes, Some(0));
        assert_eq!(memory.effective_available_bytes, Some(0));
        assert_eq!(memory.pressure, RuntimeMemoryPressure::Critical);
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
