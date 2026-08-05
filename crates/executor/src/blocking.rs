//! Memory-bounded blocking operators and spill-backed execution.

use crate::binding::{binding_memory_bytes, value_memory_bytes, Binding, TopNBinding};
use crate::expression::{
    binding_has_variable, binding_identity_key, binding_property, binding_value, group_key_value,
    insert_projected_value, sort_value,
};
use crate::kernel::{ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker};
use crate::observer::ExecutionObserver;
use crate::pipeline::{emit_binding_iterator, runtime_checkpoint, BatchControl, BindingBatch};
use crate::spill;
use crate::{BlockingOperatorMemoryReport, ExecutionLimit, ExecutionMemoryConfig};
use skein_core::{Catalog, Result, RuntimeTaskContext, SkeinError, Value};
use skein_plan::{
    AggregateFunction, AggregateTarget, Aggregation, PhysicalPlan, Projection, SortDirection,
    SortItem,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::num::NonZeroUsize;

pub trait BindingBatchSource {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl>;
}

pub struct BlockingExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub memory: &'a ExecutionMemoryConfig,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a mut dyn ExecutionObserver,
}

pub fn spill_binding_run(
    operator: &str,
    bindings: &mut Vec<Binding>,
    directory: &std::path::Path,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    spill_budget.begin_run()?;
    let (run, mut writer) = spill::SpillRun::create(directory, operator)?;
    for binding in bindings.drain(..) {
        runtime_checkpoint(task_context)?;
        let bytes = writer.write(0, &binding, spill_budget.remaining_bytes())?;
        spill_budget.charge(bytes)?;
    }
    writer.finish()?;
    Ok(run)
}

mod aggregate;
mod distinct;
mod sort;

pub use aggregate::*;
pub use distinct::*;
pub use sort::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::NoopExecutionObserver;

    struct FixedBatchSource {
        batches: Vec<BindingBatch>,
    }

    impl BindingBatchSource for FixedBatchSource {
        fn execute(
            &mut self,
            _input: &PhysicalPlan,
            _execution_limit: ExecutionLimit,
            emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
        ) -> Result<BatchControl> {
            for batch in std::mem::take(&mut self.batches) {
                if emit(batch)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        }
    }

    fn value_binding(value: i64) -> Binding {
        Binding {
            values: BTreeMap::from([("value".to_string(), Value::Int(value))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    #[test]
    fn distinct_operator_consumes_storage_neutral_batches_in_input_order() {
        let input = PhysicalPlan::SeqNodeScan {
            variable: "node".to_string(),
            label: String::new(),
        };
        let mut source = FixedBatchSource {
            batches: vec![
                vec![value_binding(1), value_binding(1)],
                vec![value_binding(2)],
            ],
        };
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig::default();
        let mut observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_distinct_batches(
            &input,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                task_context: None,
                observer: &mut observer,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .expect("distinct execution");

        assert_eq!(output, vec![value_binding(1), value_binding(2)]);
    }

    #[test]
    fn top_n_operator_applies_offset_limit_and_parent_cap() {
        let input = PhysicalPlan::SeqNodeScan {
            variable: "node".to_string(),
            label: String::new(),
        };
        let mut source = FixedBatchSource {
            batches: vec![vec![
                value_binding(5),
                value_binding(1),
                value_binding(3),
                value_binding(2),
                value_binding(4),
            ]],
        };
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig::default();
        let mut observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_top_n_batches(
            &input,
            &[SortItem {
                key: skein_plan::SortKey::Column("value".to_string()),
                direction: SortDirection::Asc,
            }],
            1,
            3,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                task_context: None,
                observer: &mut observer,
            },
            ExecutionLimit {
                output_rows: Some(2),
            },
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .expect("top-n execution");

        assert_eq!(output, vec![value_binding(2), value_binding(3)]);
    }
}
