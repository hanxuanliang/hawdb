use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadRange {
    pub artifact_id: u64,
    pub segment_ids: Vec<u64>,
    pub offset: u64,
    pub length: NonZeroU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadWave {
    pub ranges: Vec<SegmentReadRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadSchedule {
    pub io_depth: NonZeroUsize,
    pub input_range_count: usize,
    pub coalesced_range_count: usize,
    pub waves: Vec<SegmentReadWave>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReadScheduler {
    io_depth: NonZeroUsize,
    max_coalesced_bytes: NonZeroU64,
}

impl SegmentReadRange {
    pub fn new(artifact_id: u64, segment_id: u64, offset: u64, length: NonZeroU64) -> Self {
        Self {
            artifact_id,
            segment_ids: vec![segment_id],
            offset,
            length,
        }
    }

    pub fn end_offset(&self) -> u64 {
        self.offset.saturating_add(self.length.get())
    }
}

impl SegmentReadSchedule {
    pub fn wave_count(&self) -> usize {
        self.waves.len()
    }

    pub fn scheduled_bytes(&self) -> u64 {
        self.waves
            .iter()
            .flat_map(|wave| &wave.ranges)
            .map(|range| range.length.get())
            .fold(0, u64::saturating_add)
    }

    pub fn max_in_flight(&self) -> usize {
        self.waves
            .iter()
            .map(|wave| wave.ranges.len())
            .max()
            .unwrap_or(0)
    }
}

impl SegmentReadScheduler {
    pub fn new(io_depth: NonZeroUsize, max_coalesced_bytes: NonZeroU64) -> Self {
        Self {
            io_depth,
            max_coalesced_bytes,
        }
    }

    pub fn schedule<I>(self, ranges: I) -> SegmentReadSchedule
    where
        I: IntoIterator<Item = SegmentReadRange>,
    {
        let mut ranges = ranges.into_iter().collect::<Vec<_>>();
        let input_range_count = ranges.len();
        ranges.sort_by_key(|range| {
            (
                range.artifact_id,
                range.offset,
                range.segment_ids.first().copied().unwrap_or_default(),
            )
        });

        let mut coalesced = Vec::<SegmentReadRange>::new();
        for range in ranges {
            let Some(previous) = coalesced.last_mut() else {
                coalesced.push(range);
                continue;
            };
            let merged_end = previous.end_offset().max(range.end_offset());
            let merged_length = merged_end.saturating_sub(previous.offset);
            let can_merge = previous.artifact_id == range.artifact_id
                && range.offset <= previous.end_offset()
                && merged_length <= self.max_coalesced_bytes.get();
            if can_merge {
                previous.length =
                    NonZeroU64::new(merged_length).expect("merged read range remains non-zero");
                previous.segment_ids.extend(range.segment_ids);
            } else {
                coalesced.push(range);
            }
        }

        let coalesced_range_count = coalesced.len();
        let waves = coalesced
            .chunks(self.io_depth.get())
            .map(|ranges| SegmentReadWave {
                ranges: ranges.to_vec(),
            })
            .collect();
        SegmentReadSchedule {
            io_depth: self.io_depth,
            input_range_count,
            coalesced_range_count,
            waves,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(artifact_id: u64, segment_id: u64, offset: u64, length: u64) -> SegmentReadRange {
        SegmentReadRange::new(
            artifact_id,
            segment_id,
            offset,
            NonZeroU64::new(length).unwrap(),
        )
    }

    #[test]
    fn coalesces_adjacent_ranges_before_scheduling_parallel_waves() {
        let scheduler =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(256).unwrap());
        let schedule = scheduler.schedule([
            range(1, 2, 100, 100),
            range(1, 1, 0, 100),
            range(1, 3, 400, 50),
            range(2, 4, 0, 50),
        ]);

        assert_eq!(schedule.input_range_count, 4);
        assert_eq!(schedule.coalesced_range_count, 3);
        assert_eq!(schedule.wave_count(), 2);
        assert_eq!(schedule.max_in_flight(), 2);
        assert_eq!(schedule.scheduled_bytes(), 300);
        assert_eq!(schedule.waves[0].ranges[0].segment_ids, vec![1, 2]);
    }

    #[test]
    fn does_not_coalesce_across_artifacts_or_over_the_byte_limit() {
        let scheduler =
            SegmentReadScheduler::new(NonZeroUsize::new(4).unwrap(), NonZeroU64::new(128).unwrap());
        let schedule = scheduler.schedule([
            range(1, 1, 0, 100),
            range(1, 2, 100, 100),
            range(2, 3, 0, 100),
        ]);

        assert_eq!(schedule.coalesced_range_count, 3);
        assert_eq!(schedule.wave_count(), 1);
    }

    #[test]
    fn empty_schedule_has_no_waves() {
        let scheduler = SegmentReadScheduler::new(
            NonZeroUsize::new(4).unwrap(),
            NonZeroU64::new(4096).unwrap(),
        );
        let schedule = scheduler.schedule([]);

        assert_eq!(schedule.wave_count(), 0);
        assert_eq!(schedule.max_in_flight(), 0);
        assert_eq!(schedule.scheduled_bytes(), 0);
    }
}
