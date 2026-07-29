use crate::{StorageDeviceProfile, StorageMediaKind};
use std::num::NonZeroUsize;
#[cfg(target_os = "linux")]
use std::path::Path;

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

impl RuntimeResourceBudget {
    pub fn detect() -> Self {
        let host = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        #[cfg(target_os = "linux")]
        let (quota, cpuset) = (
            read_cpu_max(Path::new("/sys/fs/cgroup/cpu.max")),
            read_cpuset(Path::new("/sys/fs/cgroup/cpuset.cpus.effective"))
                .or_else(|| read_cpuset(Path::new("/sys/fs/cgroup/cpuset.cpus"))),
        );
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
