//! Internal memory and spill-budget primitives shared by physical operators.

use crate::binding::{binding_memory_bytes, Binding};
use crate::ExecutionMemoryConfig;
use skein_core::{Result, SkeinError};
use std::num::NonZeroUsize;

pub struct OperatorMemoryTracker {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub peak_bytes: usize,
}

impl OperatorMemoryTracker {
    pub fn new(budget_bytes: NonZeroUsize) -> Self {
        Self {
            budget_bytes: budget_bytes.get(),
            used_bytes: 0,
            peak_bytes: 0,
        }
    }

    pub fn would_exceed(&self, bytes: usize) -> bool {
        self.used_bytes.saturating_add(bytes) > self.budget_bytes
    }

    pub fn charge(&mut self, bytes: usize) {
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.peak_bytes = self.peak_bytes.max(self.used_bytes);
    }

    pub fn release(&mut self, bytes: usize) {
        self.used_bytes = self.used_bytes.saturating_sub(bytes);
    }

    pub fn reset(&mut self) {
        self.used_bytes = 0;
    }
}

pub struct SpillBudgetTracker {
    operator: &'static str,
    pub max_bytes: u64,
    pub max_runs: usize,
    pub used_bytes: u64,
    pub run_count: usize,
}

impl SpillBudgetTracker {
    pub fn new(operator: &'static str, memory: &ExecutionMemoryConfig) -> Self {
        Self {
            operator,
            max_bytes: memory.max_spill_bytes.get(),
            max_runs: memory.max_spill_runs.get(),
            used_bytes: 0,
            run_count: 0,
        }
    }

    pub fn begin_run(&mut self) -> Result<()> {
        if self.run_count == self.max_runs {
            return Err(SkeinError::Execution(format!(
                "{} exceeded max_spill_runs {}",
                self.operator, self.max_runs
            )));
        }
        self.run_count = self.run_count.saturating_add(1);
        Ok(())
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.max_bytes.saturating_sub(self.used_bytes)
    }

    pub fn charge(&mut self, bytes: u64) -> Result<()> {
        let next = self.used_bytes.saturating_add(bytes);
        if next > self.max_bytes {
            return Err(SkeinError::Execution(format!(
                "{} exceeded max_spill_bytes {} (next total {})",
                self.operator, self.max_bytes, next
            )));
        }
        self.used_bytes = next;
        Ok(())
    }
}

pub fn ensure_operator_item_fits(
    operator: &str,
    bytes: usize,
    tracker: &OperatorMemoryTracker,
) -> Result<()> {
    if bytes > tracker.budget_bytes {
        return Err(SkeinError::Execution(format!(
            "{operator} item uses {bytes} bytes, exceeding blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    Ok(())
}

pub fn push_bounded_operator_binding(
    operator: &str,
    output: &mut Vec<Binding>,
    binding: Binding,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    let bytes = binding_memory_bytes(&binding);
    ensure_operator_item_fits(operator, bytes, tracker)?;
    if tracker.would_exceed(bytes) {
        return Err(SkeinError::Execution(format!(
            "{operator} state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.charge(bytes);
    output.push(binding);
    Ok(())
}

pub fn collect_bounded_operator_bindings(
    operator: &str,
    bindings: impl IntoIterator<Item = Binding>,
    memory_budget: NonZeroUsize,
) -> Result<Vec<Binding>> {
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::new(memory_budget);
    for binding in bindings {
        push_bounded_operator_binding(operator, &mut output, binding, &mut tracker)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn operator_tracker_rejects_state_beyond_budget() {
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(1).unwrap());
        let error = push_bounded_operator_binding(
            "test",
            &mut Vec::new(),
            Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            },
            &mut tracker,
        )
        .unwrap_err();

        assert!(error.to_string().contains("blocking_operator_bytes"));
    }
}
