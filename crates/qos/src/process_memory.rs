use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemorySnapshot {
    pub resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub minor_page_faults: u64,
    pub major_page_faults: u64,
}

impl ProcessMemorySnapshot {
    pub fn capture() -> io::Result<Self> {
        capture_process_memory()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemoryProfile {
    pub start_resident_bytes: u64,
    pub start_peak_resident_bytes: u64,
    pub steady_resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub steady_resident_growth_bytes: u64,
    pub lifetime_peak_resident_growth_bytes: u64,
    pub minor_page_faults: u64,
    pub major_page_faults: u64,
}

impl ProcessMemoryProfile {
    pub fn between(start: ProcessMemorySnapshot, end: ProcessMemorySnapshot) -> Self {
        Self {
            start_resident_bytes: start.resident_bytes,
            start_peak_resident_bytes: start.peak_resident_bytes,
            steady_resident_bytes: end.resident_bytes,
            peak_resident_bytes: end.peak_resident_bytes,
            steady_resident_growth_bytes: end.resident_bytes.saturating_sub(start.resident_bytes),
            lifetime_peak_resident_growth_bytes: end
                .peak_resident_bytes
                .saturating_sub(start.peak_resident_bytes),
            minor_page_faults: end
                .minor_page_faults
                .saturating_sub(start.minor_page_faults),
            major_page_faults: end
                .major_page_faults
                .saturating_sub(start.major_page_faults),
        }
    }
}

#[cfg(unix)]
fn capture_process_memory() -> io::Result<ProcessMemorySnapshot> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage value on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful getrusage call initialized usage.
    let usage = unsafe { usage.assume_init() };
    Ok(ProcessMemorySnapshot {
        resident_bytes: current_resident_bytes()?,
        peak_resident_bytes: peak_resident_bytes(usage.ru_maxrss),
        minor_page_faults: non_negative_counter(usage.ru_minflt),
        major_page_faults: non_negative_counter(usage.ru_majflt),
    })
}

#[cfg(target_os = "macos")]
fn current_resident_bytes() -> io::Result<u64> {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::uninit();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    #[allow(deprecated)]
    // SAFETY: reading the current process task port does not transfer ownership.
    let task = unsafe { libc::mach_task_self() };
    // SAFETY: task_info writes at most count natural_t values into the correctly sized buffer.
    let status = unsafe {
        libc::task_info(
            task,
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if status != libc::KERN_SUCCESS {
        return Err(io::Error::other(format!(
            "task_info failed with kernel status {status}"
        )));
    }
    // SAFETY: the successful task_info call initialized info.
    let info = unsafe { info.assume_init() };
    Ok(info.resident_size)
}

#[cfg(target_os = "linux")]
fn current_resident_bytes() -> io::Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    let resident_pages = statm
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing resident page count"))?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    // SAFETY: sysconf is side-effect free for _SC_PAGESIZE.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(resident_pages.saturating_mul(page_size as u64))
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
fn current_resident_bytes() -> io::Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage value on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful getrusage call initialized usage.
    let usage = unsafe { usage.assume_init() };
    Ok(peak_resident_bytes(usage.ru_maxrss))
}

#[cfg(target_os = "macos")]
fn peak_resident_bytes(max_rss: libc::c_long) -> u64 {
    non_negative_counter(max_rss)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn peak_resident_bytes(max_rss: libc::c_long) -> u64 {
    non_negative_counter(max_rss).saturating_mul(1024)
}

#[cfg(unix)]
fn non_negative_counter(value: libc::c_long) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

#[cfg(not(unix))]
fn capture_process_memory() -> io::Result<ProcessMemorySnapshot> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process memory sampling is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn captures_process_memory_and_fault_counters() {
        let snapshot = ProcessMemorySnapshot::capture().unwrap();
        assert!(snapshot.resident_bytes > 0);
        assert!(snapshot.peak_resident_bytes > 0);
    }

    #[test]
    fn profile_deltas_are_saturating() {
        let start = ProcessMemorySnapshot {
            resident_bytes: 10,
            peak_resident_bytes: 20,
            minor_page_faults: 8,
            major_page_faults: 4,
        };
        let end = ProcessMemorySnapshot {
            resident_bytes: 12,
            peak_resident_bytes: 24,
            minor_page_faults: 3,
            major_page_faults: 9,
        };
        let profile = ProcessMemoryProfile::between(start, end);
        assert_eq!(profile.start_resident_bytes, 10);
        assert_eq!(profile.start_peak_resident_bytes, 20);
        assert_eq!(profile.steady_resident_bytes, 12);
        assert_eq!(profile.peak_resident_bytes, 24);
        assert_eq!(profile.steady_resident_growth_bytes, 2);
        assert_eq!(profile.lifetime_peak_resident_growth_bytes, 4);
        assert_eq!(profile.minor_page_faults, 0);
        assert_eq!(profile.major_page_faults, 5);
    }
}
