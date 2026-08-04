#[cfg(not(windows))]
use std::fs;
use std::io;
use std::path::Path;

/// Atomically publishes a file whose contents have already been synchronized.
///
/// Unix persists the directory entry after rename. Windows uses a write-through
/// move because flushing a directory handle is not a supported durability
/// primitive there.
pub fn durable_replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        durable_replace_file_windows(source, destination)
    }
    #[cfg(not(windows))]
    {
        fs::rename(source, destination)?;
        sync_parent_directory(destination)
    }
}

/// Persists a directory entry change when the platform exposes that primitive.
pub fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_directory(parent)
}

/// Persists pending directory entry changes when supported by the platform.
pub fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        let _ = directory;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        use std::fs::File;

        File::open(directory)?.sync_all()
    }
}

#[cfg(windows)]
fn durable_replace_file_windows(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    #[link(name = "Kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both paths are live, NUL-terminated UTF-16 buffers for the call.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn durable_replace_publishes_and_replaces_content() {
        let root = unique_test_dir();
        fs::create_dir_all(&root).unwrap();
        let candidate = root.join("candidate.skein");
        let published = root.join("published.skein");

        write_synced(&candidate, b"first");
        durable_replace_file(&candidate, &published).unwrap();
        assert_eq!(fs::read(&published).unwrap(), b"first");
        assert!(!candidate.exists());

        write_synced(&candidate, b"second");
        durable_replace_file(&candidate, &published).unwrap();
        assert_eq!(fs::read(&published).unwrap(), b"second");
        assert!(!candidate.exists());

        fs::remove_dir_all(root).unwrap();
    }

    fn write_synced(path: &Path, bytes: &[u8]) {
        let mut file = fs::File::create(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

    fn unique_test_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "skein-durable-replace-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
