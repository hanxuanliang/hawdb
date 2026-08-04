//! Streaming pipeline control and batch emission helpers.

use crate::binding::Binding;
use skein_core::{Result, RuntimeTaskContext, SkeinError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchControl {
    Continue,
    Stop,
}

pub type BindingBatch = Vec<Binding>;

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
    use std::collections::BTreeMap;

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
}
