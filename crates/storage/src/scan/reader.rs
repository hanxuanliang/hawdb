use super::{SegmentReadRange, SegmentReadSchedule};
use skein_core::{RuntimeCancellationReason, RuntimeTaskContext};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroU64;
use std::path::PathBuf;

#[derive(Debug)]
pub enum SegmentReadError {
    ArtifactNotFound {
        artifact_id: u64,
    },
    WaveBudgetExceeded {
        wave_index: usize,
        scheduled_bytes: u64,
        max_wave_bytes: u64,
    },
    RangeTooLarge {
        artifact_id: u64,
        length: u64,
    },
    Io {
        artifact_id: u64,
        offset: u64,
        length: u64,
        source: std::io::Error,
    },
    WorkerPanicked {
        artifact_id: u64,
    },
}

impl Display for SegmentReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArtifactNotFound { artifact_id } => {
                write!(formatter, "segment artifact {artifact_id} is not registered")
            }
            Self::WaveBudgetExceeded {
                wave_index,
                scheduled_bytes,
                max_wave_bytes,
            } => write!(
                formatter,
                "segment read wave {wave_index} schedules {scheduled_bytes} bytes, exceeding the {max_wave_bytes} byte budget"
            ),
            Self::RangeTooLarge {
                artifact_id,
                length,
            } => write!(
                formatter,
                "segment artifact {artifact_id} range length {length} exceeds the platform address space"
            ),
            Self::Io {
                artifact_id,
                offset,
                length,
                ..
            } => write!(
                formatter,
                "segment artifact {artifact_id} range read failed at offset {offset} for {length} bytes"
            ),
            Self::WorkerPanicked { artifact_id } => {
                write!(formatter, "segment artifact {artifact_id} range reader panicked")
            }
        }
    }
}

impl Error for SegmentReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum SegmentReadExecutionError<E> {
    Read(SegmentReadError),
    Consume(E),
    Stopped(RuntimeCancellationReason),
}

impl<E: Display> Display for SegmentReadExecutionError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Consume(error) => write!(formatter, "segment payload consumer failed: {error}"),
            Self::Stopped(reason) => write!(formatter, "segment payload read stopped: {reason}"),
        }
    }
}

impl<E: Error + 'static> Error for SegmentReadExecutionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read(error) => Some(error),
            Self::Consume(error) => Some(error),
            Self::Stopped(reason) => Some(reason),
        }
    }
}

pub trait SegmentRangeReader: Sync {
    fn read_range(&self, range: &SegmentReadRange) -> Result<Vec<u8>, SegmentReadError>;
}

#[derive(Debug, Clone, Default)]
pub struct FileSegmentRangeReader {
    artifacts: BTreeMap<u64, PathBuf>,
}

impl FileSegmentRangeReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, artifact_id: u64, path: impl Into<PathBuf>) -> Option<PathBuf> {
        self.artifacts.insert(artifact_id, path.into())
    }
}

impl SegmentRangeReader for FileSegmentRangeReader {
    fn read_range(&self, range: &SegmentReadRange) -> Result<Vec<u8>, SegmentReadError> {
        let path =
            self.artifacts
                .get(&range.artifact_id)
                .ok_or(SegmentReadError::ArtifactNotFound {
                    artifact_id: range.artifact_id,
                })?;
        let length =
            usize::try_from(range.length.get()).map_err(|_| SegmentReadError::RangeTooLarge {
                artifact_id: range.artifact_id,
                length: range.length.get(),
            })?;
        let mut file = File::open(path).map_err(|source| range_io_error(range, source))?;
        file.seek(SeekFrom::Start(range.offset))
            .map_err(|source| range_io_error(range, source))?;
        let mut payload = vec![0; length];
        file.read_exact(&mut payload)
            .map_err(|source| range_io_error(range, source))?;
        Ok(payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadPayload {
    pub range: SegmentReadRange,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReadExecutionReport {
    pub wave_count: usize,
    pub range_count: usize,
    pub bytes_read: u64,
    pub max_wave_bytes_read: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReadExecutor {
    max_wave_bytes: NonZeroU64,
}

impl SegmentReadExecutor {
    pub fn new(max_wave_bytes: NonZeroU64) -> Self {
        Self { max_wave_bytes }
    }

    pub fn execute<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<(), E>,
    {
        self.execute_inner(reader, schedule, None, consume)
    }

    pub fn execute_with_context<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        context: &RuntimeTaskContext,
        consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<(), E>,
    {
        self.execute_inner(reader, schedule, Some(context), consume)
    }

    fn execute_inner<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        context: Option<&RuntimeTaskContext>,
        mut consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<(), E>,
    {
        segment_read_checkpoint(context)?;
        let mut range_count = 0usize;
        let mut bytes_read = 0u64;
        let mut max_wave_bytes_read = 0u64;
        for (wave_index, wave) in schedule.waves.iter().enumerate() {
            segment_read_checkpoint(context)?;
            let wave_bytes = wave
                .ranges
                .iter()
                .map(|range| range.length.get())
                .fold(0u64, u64::saturating_add);
            if wave_bytes > self.max_wave_bytes.get() {
                return Err(SegmentReadExecutionError::Read(
                    SegmentReadError::WaveBudgetExceeded {
                        wave_index,
                        scheduled_bytes: wave_bytes,
                        max_wave_bytes: self.max_wave_bytes.get(),
                    },
                ));
            }

            let payloads = std::thread::scope(|scope| {
                let workers = wave
                    .ranges
                    .iter()
                    .map(|range| {
                        (
                            range.artifact_id,
                            scope.spawn(move || {
                                reader.read_range(range).map(|bytes| SegmentReadPayload {
                                    range: range.clone(),
                                    bytes,
                                })
                            }),
                        )
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|(artifact_id, worker)| {
                        worker
                            .join()
                            .map_err(|_| SegmentReadError::WorkerPanicked { artifact_id })?
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(SegmentReadExecutionError::Read)?;

            segment_read_checkpoint(context)?;
            for payload in payloads {
                segment_read_checkpoint(context)?;
                range_count = range_count.saturating_add(1);
                bytes_read = bytes_read.saturating_add(payload.range.length.get());
                consume(payload).map_err(SegmentReadExecutionError::Consume)?;
            }
            max_wave_bytes_read = max_wave_bytes_read.max(wave_bytes);
        }
        segment_read_checkpoint(context)?;
        Ok(SegmentReadExecutionReport {
            wave_count: schedule.wave_count(),
            range_count,
            bytes_read,
            max_wave_bytes_read,
        })
    }
}

fn segment_read_checkpoint<E>(
    context: Option<&RuntimeTaskContext>,
) -> Result<(), SegmentReadExecutionError<E>> {
    match context {
        Some(context) => context
            .checkpoint()
            .map_err(SegmentReadExecutionError::Stopped),
        None => Ok(()),
    }
}

fn range_io_error(range: &SegmentReadRange, source: std::io::Error) -> SegmentReadError {
    SegmentReadError::Io {
        artifact_id: range.artifact_id,
        offset: range.offset,
        length: range.length.get(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::SegmentReadScheduler;
    use skein_core::{RuntimeCancellationReason, RuntimeCancellationToken, RuntimeTaskContext};
    use std::num::NonZeroUsize;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn executes_file_ranges_in_schedule_order() {
        let path = unique_test_file("ordered");
        std::fs::write(&path, b"abcdefghijklmnop").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, &path);
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(4).unwrap())
                .schedule([
                    SegmentReadRange::new(7, 2, 8, NonZeroU64::new(4).unwrap()),
                    SegmentReadRange::new(7, 1, 0, NonZeroU64::new(4).unwrap()),
                ]);
        let mut payloads = Vec::new();

        let report = SegmentReadExecutor::new(NonZeroU64::new(8).unwrap())
            .execute(&reader, &schedule, |payload| {
                payloads.push(payload.bytes);
                Ok::<(), std::convert::Infallible>(())
            })
            .unwrap();

        assert_eq!(payloads, vec![b"abcd".to_vec(), b"ijkl".to_vec()]);
        assert_eq!(report.wave_count, 1);
        assert_eq!(report.range_count, 2);
        assert_eq!(report.bytes_read, 8);
        assert_eq!(report.max_wave_bytes_read, 8);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_wave_before_allocating_over_budget_payloads() {
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(8).unwrap())
                .schedule([
                    SegmentReadRange::new(1, 1, 0, NonZeroU64::new(8).unwrap()),
                    SegmentReadRange::new(2, 2, 0, NonZeroU64::new(8).unwrap()),
                ]);
        let reader = FileSegmentRangeReader::new();

        let error = SegmentReadExecutor::new(NonZeroU64::new(8).unwrap())
            .execute(
                &reader,
                &schedule,
                |_| Ok::<_, std::convert::Infallible>(()),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            SegmentReadExecutionError::Read(SegmentReadError::WaveBudgetExceeded {
                wave_index: 0,
                scheduled_bytes: 16,
                max_wave_bytes: 8,
            })
        ));
    }

    #[test]
    fn controlled_reader_stops_between_io_waves() {
        let path = unique_test_file("cancelled");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(1, &path);
        let schedule = SegmentReadScheduler::new(NonZeroUsize::MIN, NonZeroU64::new(4).unwrap())
            .schedule([
                SegmentReadRange::new(1, 1, 0, NonZeroU64::new(4).unwrap()),
                SegmentReadRange::new(1, 2, 4, NonZeroU64::new(4).unwrap()),
            ]);
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let mut consumed = 0usize;

        let result = SegmentReadExecutor::new(NonZeroU64::new(4).unwrap()).execute_with_context(
            &reader,
            &schedule,
            &context,
            |_| {
                consumed += 1;
                token.cancel();
                Ok::<(), std::convert::Infallible>(())
            },
        );

        assert!(matches!(
            result,
            Err(SegmentReadExecutionError::Stopped(
                RuntimeCancellationReason::Cancelled
            ))
        ));
        assert_eq!(consumed, 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_reader_errors_do_not_expose_registered_paths() {
        let path = unique_test_file("short");
        std::fs::write(&path, b"short").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(9, &path);
        let range = SegmentReadRange::new(9, 1, 2, NonZeroU64::new(8).unwrap());

        let error = reader.read_range(&range).unwrap_err();

        assert_eq!(
            error.to_string(),
            "segment artifact 9 range read failed at offset 2 for 8 bytes"
        );
        assert!(!error.to_string().contains(path.to_string_lossy().as_ref()));
        std::fs::remove_file(path).unwrap();
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_storage_segment_reader_{name}_{}_{}",
            std::process::id(),
            nonce
        ))
    }
}
