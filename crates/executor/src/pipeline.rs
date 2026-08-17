//! Streaming pipeline control and batch emission helpers.

use crate::binding::{binding_memory_bytes, Binding};
use crate::kernel::OperatorMemoryTracker;
use skein_core::{Result, RuntimeTaskContext, SkeinError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchControl {
    Continue,
    Stop,
}

pub type BindingBatch = Vec<Binding>;

pub struct AccountedBindingSet {
    bindings: Vec<Binding>,
    tracker: OperatorMemoryTracker,
}

impl AccountedBindingSet {
    pub(crate) fn new(bindings: Vec<Binding>, tracker: OperatorMemoryTracker) -> Self {
        debug_assert_eq!(
            tracker.used_bytes,
            bindings.iter().fold(0usize, |bytes, binding| {
                bytes.saturating_add(binding_memory_bytes(binding))
            })
        );
        Self { bindings, tracker }
    }

    pub fn emit_batches(
        self,
        batch_rows: usize,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            bindings,
            mut tracker,
        } = self;
        let mut batch = Vec::with_capacity(batch_rows);
        let mut batch_bytes = 0usize;
        for binding in bindings {
            batch_bytes = batch_bytes.saturating_add(binding_memory_bytes(&binding));
            batch.push(binding);
            if batch.len() == batch_rows {
                tracker.release(batch_bytes);
                batch_bytes = 0;
                if emit(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(batch_rows),
                ))? == BatchControl::Stop
                {
                    return Ok(BatchControl::Stop);
                }
            }
        }
        if !batch.is_empty() {
            tracker.release(batch_bytes);
            if emit(batch)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    }
}

pub fn runtime_checkpoint(task_context: Option<&RuntimeTaskContext>) -> Result<()> {
    match task_context {
        Some(task_context) => task_context
            .checkpoint()
            .map_err(|reason| SkeinError::Execution(format!("runtime task stopped: {reason}"))),
        None => Ok(()),
    }
}

pub fn emit_owned_binding_batches(
    bindings: Vec<Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    emit_binding_iterator(bindings, batch_rows, emit)
}

pub fn emit_binding_iterator(
    bindings: impl IntoIterator<Item = Binding>,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut batch = Vec::with_capacity(batch_rows);
    for binding in bindings {
        batch.push(binding);
        if batch.len() == batch_rows
            && emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(batch_rows),
            ))? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
    }
    if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryMemoryClass, QueryMemoryLedger};
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    fn binding(value: i64) -> Binding {
        Binding {
            values: BTreeMap::from([("value".to_string(), skein_core::Value::Int(value))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    #[test]
    fn iterator_emits_bounded_batches_and_honors_stop() {
        let mut sizes = Vec::new();
        let control =
            emit_binding_iterator([binding(1), binding(2), binding(3)], 2, &mut |batch| {
                sizes.push(batch.len());
                Ok(BatchControl::Stop)
            })
            .unwrap();

        assert_eq!(control, BatchControl::Stop);
        assert_eq!(sizes, vec![2]);
    }

    #[test]
    fn accounted_set_releases_remaining_rows_after_consumer_stop() {
        let bindings = vec![binding(1), binding(2), binding(3)];
        let budget = NonZeroUsize::new(4096).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let mut tracker = OperatorMemoryTracker::with_account(
            budget,
            ledger.account(QueryMemoryClass::BlockingState, "test rows", budget),
        );
        for binding in &bindings {
            tracker.try_charge(binding_memory_bytes(binding)).unwrap();
        }
        let accounted = AccountedBindingSet::new(bindings, tracker);

        let control = accounted
            .emit_batches(2, &mut |_| Ok(BatchControl::Stop))
            .unwrap();

        assert_eq!(control, BatchControl::Stop);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
