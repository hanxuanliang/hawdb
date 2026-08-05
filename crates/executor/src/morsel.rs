//! Storage-neutral morsel partitioning and resource admission.

use skein_core::{Result, SkeinError};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MorselOrdinal(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Morsel {
    pub pipeline_id: PipelineId,
    pub ordinal: MorselOrdinal,
    pub start_row: usize,
    pub row_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselAdmissionRequest {
    pub pipeline_id: PipelineId,
    pub input_rows: usize,
    pub target_rows: NonZeroUsize,
    pub requested_parallelism: NonZeroUsize,
    pub bytes_per_worker: NonZeroUsize,
    pub memory_budget_bytes: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MorselAdmission {
    pipeline_id: PipelineId,
    input_rows: usize,
    target_rows: NonZeroUsize,
    morsel_count: usize,
    max_workers: usize,
    reserved_bytes: usize,
}

impl MorselAdmission {
    pub fn try_new(request: MorselAdmissionRequest) -> Result<Self> {
        if request.input_rows == 0 {
            return Ok(Self {
                pipeline_id: request.pipeline_id,
                input_rows: 0,
                target_rows: request.target_rows,
                morsel_count: 0,
                max_workers: 0,
                reserved_bytes: 0,
            });
        }

        let memory_workers = request.memory_budget_bytes.get() / request.bytes_per_worker.get();
        if memory_workers == 0 {
            return Err(SkeinError::Execution(format!(
                "morsel pipeline {} requires {} bytes for one worker, exceeding memory budget {}",
                request.pipeline_id.0, request.bytes_per_worker, request.memory_budget_bytes
            )));
        }

        let morsel_count = request.input_rows.div_ceil(request.target_rows.get());
        let max_workers = request
            .requested_parallelism
            .get()
            .min(memory_workers)
            .min(morsel_count);
        let reserved_bytes = max_workers.saturating_mul(request.bytes_per_worker.get());
        Ok(Self {
            pipeline_id: request.pipeline_id,
            input_rows: request.input_rows,
            target_rows: request.target_rows,
            morsel_count,
            max_workers,
            reserved_bytes,
        })
    }

    pub fn morsels(&self) -> MorselIter {
        MorselIter {
            pipeline_id: self.pipeline_id,
            input_rows: self.input_rows,
            target_rows: self.target_rows,
            next_ordinal: 0,
            morsel_count: self.morsel_count,
        }
    }

    pub fn morsel_count(&self) -> usize {
        self.morsel_count
    }

    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }
}

pub struct MorselIter {
    pipeline_id: PipelineId,
    input_rows: usize,
    target_rows: NonZeroUsize,
    next_ordinal: usize,
    morsel_count: usize,
}

impl Iterator for MorselIter {
    type Item = Morsel;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_ordinal == self.morsel_count {
            return None;
        }
        let ordinal = self.next_ordinal;
        let start_row = ordinal.saturating_mul(self.target_rows.get());
        let row_count = self
            .target_rows
            .get()
            .min(self.input_rows.saturating_sub(start_row));
        self.next_ordinal = self.next_ordinal.saturating_add(1);
        Some(Morsel {
            pipeline_id: self.pipeline_id,
            ordinal: MorselOrdinal(ordinal as u64),
            start_row,
            row_count,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.morsel_count.saturating_sub(self.next_ordinal);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MorselIter {}

pub fn admit_morsels(request: MorselAdmissionRequest) -> Result<MorselAdmission> {
    MorselAdmission::try_new(request)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SequentialMorselScheduler;

impl SequentialMorselScheduler {
    pub fn execute<T>(
        self,
        admission: &MorselAdmission,
        mut execute: impl FnMut(Morsel) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut output = Vec::with_capacity(admission.morsel_count);
        for morsel in admission.morsels() {
            output.push(execute(morsel)?);
        }
        Ok(output)
    }
}

/// Executes morsels in ordinal order. This is the deterministic baseline and
/// differential oracle for future shared-pool parallel schedulers.
pub fn execute_morsels_ordered<T>(
    admission: &MorselAdmission,
    mut execute: impl FnMut(Morsel) -> Result<T>,
) -> Result<Vec<T>> {
    SequentialMorselScheduler.execute(admission, &mut execute)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(input_rows: usize) -> MorselAdmissionRequest {
        MorselAdmissionRequest {
            pipeline_id: PipelineId(7),
            input_rows,
            target_rows: NonZeroUsize::new(64).unwrap(),
            requested_parallelism: NonZeroUsize::new(8).unwrap(),
            bytes_per_worker: NonZeroUsize::new(1024).unwrap(),
            memory_budget_bytes: NonZeroUsize::new(3 * 1024).unwrap(),
        }
    }

    #[test]
    fn admission_is_bounded_by_work_and_memory() {
        let admission = admit_morsels(request(130)).unwrap();

        assert_eq!(admission.max_workers(), 3);
        assert_eq!(admission.reserved_bytes(), 3 * 1024);
        assert_eq!(
            admission
                .morsels()
                .map(|morsel| (morsel.start_row, morsel.row_count))
                .collect::<Vec<_>>(),
            vec![(0, 64), (64, 64), (128, 2)]
        );
    }

    #[test]
    fn admission_rejects_a_worker_that_cannot_fit() {
        let mut request = request(1);
        request.bytes_per_worker = NonZeroUsize::new(4096).unwrap();

        assert!(admit_morsels(request).is_err());
    }

    #[test]
    fn ordered_executor_preserves_morsel_ordinals() {
        let admission = MorselAdmission::try_new(request(130)).unwrap();
        let ordinals = SequentialMorselScheduler
            .execute(&admission, |morsel| Ok(morsel.ordinal.0))
            .unwrap();

        assert_eq!(ordinals, vec![0, 1, 2]);
    }

    #[test]
    fn empty_input_reserves_no_workers() {
        let admission = admit_morsels(request(0)).unwrap();

        assert_eq!(admission.morsel_count(), 0);
        assert_eq!(admission.max_workers(), 0);
        assert_eq!(admission.reserved_bytes(), 0);
    }
}
