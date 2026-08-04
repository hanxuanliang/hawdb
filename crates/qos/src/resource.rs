use crate::{StorageDeviceProfile, StorageMediaKind};
#[cfg(any(target_os = "linux", test))]
use std::collections::BTreeSet;
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
            let cgroup = linux_cgroup_limits();
            (cgroup.cpu_quota, cgroup.cpuset)
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
            let cgroup = linux_cgroup_limits();
            (
                cgroup.memory_limit,
                cgroup.memory_high,
                cgroup.memory_current,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg(any(target_os = "linux", test))]
struct LinuxCgroupLimits {
    cpu_quota: Option<NonZeroUsize>,
    cpuset: Option<NonZeroUsize>,
    memory_limit: Option<u64>,
    memory_high: Option<u64>,
    memory_current: Option<u64>,
}

#[cfg(any(target_os = "linux", test))]
impl LinuxCgroupLimits {
    #[cfg(target_os = "linux")]
    fn fail_closed() -> Self {
        Self {
            cpu_quota: Some(NonZeroUsize::MIN),
            cpuset: Some(NonZeroUsize::MIN),
            memory_limit: Some(0),
            memory_high: None,
            memory_current: Some(u64::MAX),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(any(target_os = "linux", test))]
struct CgroupMount {
    root: PathBuf,
    mount_point: PathBuf,
    file_system: String,
    controllers: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(any(target_os = "linux", test))]
enum ControllerDirectory {
    Absent,
    Invalid,
    Resolved {
        directory: PathBuf,
        mount_point: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(target_os = "linux", test))]
enum ParsedCgroupValue<T> {
    Unlimited,
    Value(T),
    Invalid,
}

#[cfg(target_os = "linux")]
fn linux_cgroup_limits() -> LinuxCgroupLimits {
    let self_cgroup = std::fs::read_to_string("/proc/self/cgroup");
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo");
    match (self_cgroup, mountinfo) {
        (Ok(self_cgroup), Ok(mountinfo)) => detect_linux_cgroup_limits(&self_cgroup, &mountinfo),
        _ => LinuxCgroupLimits::fail_closed(),
    }
}

#[cfg(any(target_os = "linux", test))]
fn detect_linux_cgroup_limits(self_cgroup: &str, mountinfo: &str) -> LinuxCgroupLimits {
    let mounts = parse_cgroup_mounts(mountinfo);
    let unified = resolve_unified_directory(self_cgroup, &mounts);
    let mut limits = LinuxCgroupLimits::default();

    let v2_cpu = read_optional_cgroup_file(&unified, "cpu.max");
    limits.cpu_quota = match v2_cpu {
        OptionalCgroupFile::Value(value) => fail_closed_cpu(parse_cpu_max_value(&value)),
        OptionalCgroupFile::Invalid => Some(NonZeroUsize::MIN),
        OptionalCgroupFile::Absent => read_v1_cpu_limit(self_cgroup, &mounts),
    };

    limits.cpuset = match read_inherited_cpuset(&unified, "cpuset.cpus.effective") {
        OptionalCgroupValue::Value(value) => Some(value),
        OptionalCgroupValue::Invalid => Some(NonZeroUsize::MIN),
        OptionalCgroupValue::Absent => match read_inherited_cpuset(&unified, "cpuset.cpus") {
            OptionalCgroupValue::Value(value) => Some(value),
            OptionalCgroupValue::Invalid => Some(NonZeroUsize::MIN),
            OptionalCgroupValue::Absent => read_v1_cpuset(self_cgroup, &mounts),
        },
    };

    let v2_memory_max = read_optional_cgroup_file(&unified, "memory.max");
    if matches!(v2_memory_max, OptionalCgroupFile::Absent) {
        let (limit, high, current) = read_v1_memory(self_cgroup, &mounts);
        limits.memory_limit = limit;
        limits.memory_high = high;
        limits.memory_current = current;
    } else {
        limits.memory_limit = fail_closed_memory_file(v2_memory_max);
        limits.memory_high = optional_memory_limit(&unified, "memory.high");
        limits.memory_current = required_memory_current(&unified, "memory.current");
    }
    limits
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(any(target_os = "linux", test))]
enum OptionalCgroupFile {
    Absent,
    Invalid,
    Value(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(target_os = "linux", test))]
enum OptionalCgroupValue<T> {
    Absent,
    Invalid,
    Value(T),
}

#[cfg(any(target_os = "linux", test))]
fn read_optional_cgroup_file(
    controller: &ControllerDirectory,
    file_name: &str,
) -> OptionalCgroupFile {
    let ControllerDirectory::Resolved { directory, .. } = controller else {
        return match controller {
            ControllerDirectory::Absent => OptionalCgroupFile::Absent,
            ControllerDirectory::Invalid => OptionalCgroupFile::Invalid,
            ControllerDirectory::Resolved { .. } => unreachable!(),
        };
    };
    match std::fs::read_to_string(directory.join(file_name)) {
        Ok(value) => OptionalCgroupFile::Value(value),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => OptionalCgroupFile::Absent,
        Err(_) => OptionalCgroupFile::Invalid,
    }
}

#[cfg(any(target_os = "linux", test))]
fn fail_closed_cpu(value: ParsedCgroupValue<NonZeroUsize>) -> Option<NonZeroUsize> {
    match value {
        ParsedCgroupValue::Unlimited => None,
        ParsedCgroupValue::Value(value) => Some(value),
        ParsedCgroupValue::Invalid => Some(NonZeroUsize::MIN),
    }
}

#[cfg(any(target_os = "linux", test))]
fn fail_closed_memory_file(value: OptionalCgroupFile) -> Option<u64> {
    match value {
        OptionalCgroupFile::Value(value) => match parse_memory_limit_value(&value, false) {
            ParsedCgroupValue::Unlimited => None,
            ParsedCgroupValue::Value(value) => Some(value),
            ParsedCgroupValue::Invalid => Some(0),
        },
        OptionalCgroupFile::Absent | OptionalCgroupFile::Invalid => Some(0),
    }
}

#[cfg(any(target_os = "linux", test))]
fn optional_memory_limit(controller: &ControllerDirectory, file_name: &str) -> Option<u64> {
    match read_optional_cgroup_file(controller, file_name) {
        OptionalCgroupFile::Absent => None,
        value => fail_closed_memory_file(value),
    }
}

#[cfg(any(target_os = "linux", test))]
fn required_memory_current(controller: &ControllerDirectory, file_name: &str) -> Option<u64> {
    match read_optional_cgroup_file(controller, file_name) {
        OptionalCgroupFile::Value(value) => value.trim().parse::<u64>().ok().or(Some(u64::MAX)),
        OptionalCgroupFile::Absent | OptionalCgroupFile::Invalid => Some(u64::MAX),
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_mounts(mountinfo: &str) -> Vec<CgroupMount> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let (mount_fields, file_system_fields) = line.split_once(" - ")?;
            let mount_fields = mount_fields.split_whitespace().collect::<Vec<_>>();
            let file_system_fields = file_system_fields.split_whitespace().collect::<Vec<_>>();
            if mount_fields.len() < 5 || file_system_fields.len() < 3 {
                return None;
            }
            let file_system = file_system_fields[0];
            if file_system != "cgroup" && file_system != "cgroup2" {
                return None;
            }
            let root = normalized_absolute_path(&unescape_mountinfo_field(mount_fields[3]))?;
            let mount_point = normalized_absolute_path(&unescape_mountinfo_field(mount_fields[4]))?;
            let controllers = if file_system == "cgroup" {
                file_system_fields[2]
                    .split(',')
                    .filter(|option| !matches!(*option, "rw" | "ro" | "relatime"))
                    .map(str::to_string)
                    .collect()
            } else {
                BTreeSet::new()
            };
            Some(CgroupMount {
                root,
                mount_point,
                file_system: file_system.to_string(),
                controllers,
            })
        })
        .collect()
}

#[cfg(any(target_os = "linux", test))]
fn unescape_mountinfo_field(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

#[cfg(any(target_os = "linux", test))]
fn normalized_absolute_path(value: &str) -> Option<PathBuf> {
    use std::path::Component;

    let path = Path::new(value);
    if !path.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => normalized.push(component),
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

#[cfg(any(target_os = "linux", test))]
fn self_cgroup_path(self_cgroup: &str, controller: Option<&str>) -> Result<Option<PathBuf>, ()> {
    let raw = self_cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?;
        let matches = match controller {
            None => hierarchy == "0" && controllers.is_empty(),
            Some(controller) => controllers.split(',').any(|item| item == controller),
        };
        matches.then_some(path)
    });
    match raw {
        Some(path) => normalized_absolute_path(path).map(Some).ok_or(()),
        None => Ok(None),
    }
}

#[cfg(any(target_os = "linux", test))]
fn resolve_unified_directory(self_cgroup: &str, mounts: &[CgroupMount]) -> ControllerDirectory {
    resolve_controller_directory(self_cgroup, mounts, None)
}

#[cfg(any(target_os = "linux", test))]
fn resolve_controller_directory(
    self_cgroup: &str,
    mounts: &[CgroupMount],
    controller: Option<&str>,
) -> ControllerDirectory {
    let process_path = match self_cgroup_path(self_cgroup, controller) {
        Ok(Some(path)) => path,
        Ok(None) => return ControllerDirectory::Absent,
        Err(()) => return ControllerDirectory::Invalid,
    };
    let mut resolved = mounts
        .iter()
        .filter(|mount| match controller {
            None => mount.file_system == "cgroup2",
            Some(controller) => {
                mount.file_system == "cgroup" && mount.controllers.contains(controller)
            }
        })
        .filter_map(|mount| {
            let relative = process_path.strip_prefix(&mount.root).ok()?;
            Some((
                mount.root.components().count(),
                ControllerDirectory::Resolved {
                    directory: mount.mount_point.join(relative),
                    mount_point: mount.mount_point.clone(),
                },
            ))
        })
        .collect::<Vec<_>>();
    resolved.sort_by_key(|(specificity, _)| *specificity);
    resolved
        .pop()
        .map(|(_, directory)| directory)
        .unwrap_or(ControllerDirectory::Invalid)
}

#[cfg(any(target_os = "linux", test))]
fn read_inherited_cpuset(
    controller: &ControllerDirectory,
    file_name: &str,
) -> OptionalCgroupValue<NonZeroUsize> {
    let ControllerDirectory::Resolved {
        directory,
        mount_point,
    } = controller
    else {
        return match controller {
            ControllerDirectory::Absent => OptionalCgroupValue::Absent,
            ControllerDirectory::Invalid => OptionalCgroupValue::Invalid,
            ControllerDirectory::Resolved { .. } => unreachable!(),
        };
    };
    let mut current = directory.clone();
    loop {
        match std::fs::read_to_string(current.join(file_name)) {
            Ok(value) if value.trim().is_empty() => {}
            Ok(value) => {
                return parse_cpuset(&value)
                    .map(OptionalCgroupValue::Value)
                    .unwrap_or(OptionalCgroupValue::Invalid);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return OptionalCgroupValue::Absent;
            }
            Err(_) => return OptionalCgroupValue::Invalid,
        }
        if current == *mount_point || !current.starts_with(mount_point) || !current.pop() {
            return OptionalCgroupValue::Absent;
        }
    }
}

#[cfg(any(target_os = "linux", test))]
fn read_v1_cpu_limit(self_cgroup: &str, mounts: &[CgroupMount]) -> Option<NonZeroUsize> {
    let controller = resolve_controller_directory(self_cgroup, mounts, Some("cpu"));
    let ControllerDirectory::Resolved { directory, .. } = &controller else {
        return match controller {
            ControllerDirectory::Absent => None,
            ControllerDirectory::Invalid => Some(NonZeroUsize::MIN),
            ControllerDirectory::Resolved { .. } => unreachable!(),
        };
    };
    let quota = std::fs::read_to_string(directory.join("cpu.cfs_quota_us"));
    let period = std::fs::read_to_string(directory.join("cpu.cfs_period_us"));
    match (quota, period) {
        (Ok(quota), Ok(period)) => fail_closed_cpu(parse_cpu_cfs(&quota, &period)),
        _ => Some(NonZeroUsize::MIN),
    }
}

#[cfg(any(target_os = "linux", test))]
fn read_v1_cpuset(self_cgroup: &str, mounts: &[CgroupMount]) -> Option<NonZeroUsize> {
    let controller = resolve_controller_directory(self_cgroup, mounts, Some("cpuset"));
    match read_inherited_cpuset(&controller, "cpuset.cpus") {
        OptionalCgroupValue::Value(value) => Some(value),
        OptionalCgroupValue::Absent => match controller {
            ControllerDirectory::Absent => None,
            ControllerDirectory::Invalid | ControllerDirectory::Resolved { .. } => {
                Some(NonZeroUsize::MIN)
            }
        },
        OptionalCgroupValue::Invalid => Some(NonZeroUsize::MIN),
    }
}

#[cfg(any(target_os = "linux", test))]
fn read_v1_memory(
    self_cgroup: &str,
    mounts: &[CgroupMount],
) -> (Option<u64>, Option<u64>, Option<u64>) {
    let controller = resolve_controller_directory(self_cgroup, mounts, Some("memory"));
    let ControllerDirectory::Resolved { directory, .. } = &controller else {
        return match controller {
            ControllerDirectory::Absent => (None, None, None),
            ControllerDirectory::Invalid => (Some(0), None, Some(u64::MAX)),
            ControllerDirectory::Resolved { .. } => unreachable!(),
        };
    };
    let limit = match std::fs::read_to_string(directory.join("memory.limit_in_bytes")) {
        Ok(value) => match parse_memory_limit_value(&value, true) {
            ParsedCgroupValue::Unlimited => None,
            ParsedCgroupValue::Value(value) => Some(value),
            ParsedCgroupValue::Invalid => Some(0),
        },
        Err(_) => Some(0),
    };
    let current = match std::fs::read_to_string(directory.join("memory.usage_in_bytes")) {
        Ok(value) => value.trim().parse::<u64>().ok().or(Some(u64::MAX)),
        Err(_) => Some(u64::MAX),
    };
    let high = match std::fs::read_to_string(directory.join("memory.soft_limit_in_bytes")) {
        Ok(value) => match parse_memory_limit_value(&value, true) {
            ParsedCgroupValue::Unlimited => None,
            ParsedCgroupValue::Value(value) => Some(value),
            ParsedCgroupValue::Invalid => Some(0),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => Some(0),
    };
    (limit, high, current)
}

#[cfg(test)]
fn parse_cpu_max(value: &str) -> Option<NonZeroUsize> {
    match parse_cpu_max_value(value) {
        ParsedCgroupValue::Value(value) => Some(value),
        ParsedCgroupValue::Unlimited | ParsedCgroupValue::Invalid => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpu_max_value(value: &str) -> ParsedCgroupValue<NonZeroUsize> {
    let mut fields = value.split_whitespace();
    let Some(quota) = fields.next() else {
        return ParsedCgroupValue::Invalid;
    };
    let Some(period) = fields
        .next()
        .and_then(|period| period.parse::<usize>().ok())
    else {
        return ParsedCgroupValue::Invalid;
    };
    if fields.next().is_some() || period == 0 {
        return ParsedCgroupValue::Invalid;
    }
    if quota == "max" {
        return ParsedCgroupValue::Unlimited;
    }
    let Some(quota) = quota.parse::<usize>().ok().filter(|quota| *quota > 0) else {
        return ParsedCgroupValue::Invalid;
    };
    ParsedCgroupValue::Value(
        NonZeroUsize::new((quota / period).max(1)).expect("bounded CPU quota is non-zero"),
    )
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpu_cfs(quota: &str, period: &str) -> ParsedCgroupValue<NonZeroUsize> {
    let Ok(quota) = quota.trim().parse::<i64>() else {
        return ParsedCgroupValue::Invalid;
    };
    let Ok(period) = period.trim().parse::<usize>() else {
        return ParsedCgroupValue::Invalid;
    };
    if quota == -1 && period > 0 {
        return ParsedCgroupValue::Unlimited;
    }
    if quota <= 0 || period == 0 {
        return ParsedCgroupValue::Invalid;
    }
    let Ok(quota) = usize::try_from(quota) else {
        return ParsedCgroupValue::Invalid;
    };
    ParsedCgroupValue::Value(
        NonZeroUsize::new((quota / period).max(1)).expect("bounded CPU quota is non-zero"),
    )
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpuset(value: &str) -> Option<NonZeroUsize> {
    if value.trim().is_empty() {
        return None;
    }
    let mut ranges = value
        .trim()
        .split(',')
        .map(|part| {
            if part.is_empty() {
                return None;
            }
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
                count = count
                    .saturating_add(current_end.saturating_sub(current_start).saturating_add(1));
                current = Some((start, end));
            }
            None => current = Some((start, end)),
        }
    }
    if let Some((start, end)) = current {
        count = count.saturating_add(end.saturating_sub(start).saturating_add(1));
    }
    NonZeroUsize::new(count)
}

#[cfg(test)]
fn parse_memory_limit(value: &str) -> Option<u64> {
    match parse_memory_limit_value(value, false) {
        ParsedCgroupValue::Value(value) => Some(value),
        ParsedCgroupValue::Unlimited | ParsedCgroupValue::Invalid => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_memory_limit_value(value: &str, cgroup_v1: bool) -> ParsedCgroupValue<u64> {
    const CGROUP_V1_UNLIMITED_MEMORY_MIN: u64 = 1 << 60;

    let value = value.trim();
    if value == "max" {
        return ParsedCgroupValue::Unlimited;
    }
    let Ok(value) = value.parse::<u64>() else {
        return ParsedCgroupValue::Invalid;
    };
    if cgroup_v1 && value >= CGROUP_V1_UNLIMITED_MEMORY_MIN {
        ParsedCgroupValue::Unlimited
    } else if value == 0 {
        ParsedCgroupValue::Invalid
    } else {
        ParsedCgroupValue::Value(value)
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
    use std::sync::atomic::{AtomicU64, Ordering};

    static CGROUP_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

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
        assert_eq!(
            parse_cpu_max_value("invalid 100000"),
            ParsedCgroupValue::Invalid
        );
    }

    #[test]
    fn parses_cgroup_v1_cpu_quota_and_unlimited_sentinel() {
        assert_eq!(
            parse_cpu_cfs("250000", "100000"),
            ParsedCgroupValue::Value(NonZeroUsize::new(2).unwrap())
        );
        assert_eq!(parse_cpu_cfs("-1", "100000"), ParsedCgroupValue::Unlimited);
        assert_eq!(
            parse_cpu_cfs("invalid", "100000"),
            ParsedCgroupValue::Invalid
        );
    }

    #[test]
    fn parses_cgroup_v2_memory_limits() {
        assert_eq!(parse_memory_limit("1073741824\n"), Some(1_073_741_824));
        assert_eq!(parse_memory_limit("max\n"), None);
        assert_eq!(parse_memory_limit("invalid"), None);
        assert_eq!(
            parse_memory_limit_value("9223372036854771712", true),
            ParsedCgroupValue::Unlimited
        );
    }

    #[test]
    fn detects_cgroup_v1_cpu_cpuset_memory_and_headroom() {
        let fixture = cgroup_v1_fixture("valid");
        write_cgroup_v1_fixture(&fixture, "250000", "0-3", "1073741824", "268435456");

        let limits = detect_linux_cgroup_limits(&fixture.self_cgroup, &fixture.mountinfo);

        assert_eq!(limits.cpu_quota, NonZeroUsize::new(2));
        assert_eq!(limits.cpuset, NonZeroUsize::new(4));
        assert_eq!(limits.memory_limit, Some(1_073_741_824));
        assert_eq!(limits.memory_high, Some(805_306_368));
        assert_eq!(limits.memory_current, Some(268_435_456));
        let snapshot = RuntimeMemorySnapshot::from_limits(
            Some(16 << 30),
            Some(8 << 30),
            limits.memory_limit,
            limits.memory_high,
            limits.memory_current,
        );
        assert_eq!(snapshot.effective_limit_bytes, Some(805_306_368));
        assert_eq!(snapshot.effective_available_bytes, Some(536_870_912));

        std::fs::remove_dir_all(fixture.root).unwrap();
    }

    #[test]
    fn cgroup_v2_mount_resolution_and_limits_remain_supported() {
        let id = CGROUP_FIXTURE_ID.fetch_add(1, Ordering::SeqCst);
        let root =
            std::env::temp_dir().join(format!("skein-cgroup-v2-valid-{}-{id}", std::process::id()));
        let directory = root.join("tenant/job");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("cpu.max"), "300000 100000").unwrap();
        std::fs::write(directory.join("cpuset.cpus.effective"), "2-5").unwrap();
        std::fs::write(directory.join("memory.max"), "1073741824").unwrap();
        std::fs::write(directory.join("memory.high"), "805306368").unwrap();
        std::fs::write(directory.join("memory.current"), "268435456").unwrap();
        let mountinfo = format!("30 1 0:30 / {} rw - cgroup2 cgroup rw\n", root.display());

        let limits = detect_linux_cgroup_limits("0::/tenant/job\n", &mountinfo);

        assert_eq!(limits.cpu_quota, NonZeroUsize::new(3));
        assert_eq!(limits.cpuset, NonZeroUsize::new(4));
        assert_eq!(limits.memory_limit, Some(1_073_741_824));
        assert_eq!(limits.memory_high, Some(805_306_368));
        assert_eq!(limits.memory_current, Some(268_435_456));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_cgroup_process_paths_fail_closed() {
        let mountinfo = "30 1 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n";
        let limits = detect_linux_cgroup_limits("0::/../../escape\n", mountinfo);

        assert_eq!(limits.cpu_quota, Some(NonZeroUsize::MIN));
        assert_eq!(limits.cpuset, Some(NonZeroUsize::MIN));
        assert_eq!(limits.memory_limit, Some(0));
        assert_eq!(limits.memory_current, Some(u64::MAX));
    }

    #[test]
    fn cgroup_v1_parse_failures_do_not_fall_back_to_host_limits() {
        let fixture = cgroup_v1_fixture("invalid");
        write_cgroup_v1_fixture(&fixture, "invalid", "invalid", "invalid", "invalid");

        let limits = detect_linux_cgroup_limits(&fixture.self_cgroup, &fixture.mountinfo);

        assert_eq!(limits.cpu_quota, Some(NonZeroUsize::MIN));
        assert_eq!(limits.cpuset, Some(NonZeroUsize::MIN));
        assert_eq!(limits.memory_limit, Some(0));
        assert_eq!(limits.memory_current, Some(u64::MAX));

        std::fs::remove_dir_all(fixture.root).unwrap();
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

    struct CgroupV1Fixture {
        root: PathBuf,
        cpu_mount: PathBuf,
        cpuset_mount: PathBuf,
        memory_mount: PathBuf,
        self_cgroup: String,
        mountinfo: String,
    }

    fn cgroup_v1_fixture(name: &str) -> CgroupV1Fixture {
        let id = CGROUP_FIXTURE_ID.fetch_add(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!(
            "skein-cgroup-v1-{name}-{}-{id}",
            std::process::id()
        ));
        let cpu_mount = root.join("cpu,cpuacct");
        let cpuset_mount = root.join("cpuset");
        let memory_mount = root.join("memory");
        let self_cgroup = concat!(
            "2:cpu,cpuacct:/tenant/job\n",
            "3:cpuset:/tenant/job\n",
            "4:memory:/tenant/job\n"
        )
        .to_string();
        let mountinfo = format!(
            concat!(
                "20 1 0:20 / {} rw - cgroup cgroup rw,cpu,cpuacct\n",
                "21 1 0:21 / {} rw - cgroup cgroup rw,cpuset\n",
                "22 1 0:22 / {} rw - cgroup cgroup rw,memory\n"
            ),
            cpu_mount.display(),
            cpuset_mount.display(),
            memory_mount.display(),
        );
        CgroupV1Fixture {
            root,
            cpu_mount,
            cpuset_mount,
            memory_mount,
            self_cgroup,
            mountinfo,
        }
    }

    fn write_cgroup_v1_fixture(
        fixture: &CgroupV1Fixture,
        quota: &str,
        cpuset: &str,
        memory_limit: &str,
        memory_usage: &str,
    ) {
        let cpu = fixture.cpu_mount.join("tenant/job");
        let cpuset_parent = fixture.cpuset_mount.join("tenant");
        let cpuset_child = cpuset_parent.join("job");
        let memory = fixture.memory_mount.join("tenant/job");
        std::fs::create_dir_all(&cpu).unwrap();
        std::fs::create_dir_all(&cpuset_child).unwrap();
        std::fs::create_dir_all(&memory).unwrap();
        std::fs::write(cpu.join("cpu.cfs_quota_us"), quota).unwrap();
        std::fs::write(cpu.join("cpu.cfs_period_us"), "100000").unwrap();
        std::fs::write(cpuset_child.join("cpuset.cpus"), "").unwrap();
        std::fs::write(cpuset_parent.join("cpuset.cpus"), cpuset).unwrap();
        std::fs::write(memory.join("memory.limit_in_bytes"), memory_limit).unwrap();
        std::fs::write(memory.join("memory.soft_limit_in_bytes"), "805306368").unwrap();
        std::fs::write(memory.join("memory.usage_in_bytes"), memory_usage).unwrap();
    }
}
