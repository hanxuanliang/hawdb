use skein_core::{RuntimeCancellationReason, RuntimeTaskContext};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedExecutor {
    max_parallelism: NonZeroUsize,
}

impl BoundedExecutor {
    pub fn new(max_parallelism: NonZeroUsize) -> Self {
        Self { max_parallelism }
    }

    pub fn max_parallelism(self) -> usize {
        self.max_parallelism.get()
    }

    pub fn map_ordered<T, R, F>(self, inputs: &[T], operation: F) -> Vec<R>
    where
        T: Sync,
        R: Send,
        F: Fn(&T) -> R + Sync,
    {
        if inputs.is_empty() {
            return Vec::new();
        }

        let worker_count = self.max_parallelism().min(inputs.len());
        let next = AtomicUsize::new(0);
        let outputs = Mutex::new(
            std::iter::repeat_with(|| None)
                .take(inputs.len())
                .collect::<Vec<Option<R>>>(),
        );

        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(input) = inputs.get(index) else {
                        break;
                    };
                    let output = operation(input);
                    outputs
                        .lock()
                        .expect("bounded executor output lock should not be poisoned")[index] =
                        Some(output);
                });
            }
        });

        outputs
            .into_inner()
            .expect("bounded executor output lock should not be poisoned")
            .into_iter()
            .map(|output| output.expect("each bounded executor input must produce one output"))
            .collect()
    }

    pub fn map_ordered_with_context<T, R, F>(
        self,
        inputs: &[T],
        context: &RuntimeTaskContext,
        operation: F,
    ) -> Result<Vec<R>, RuntimeCancellationReason>
    where
        T: Sync,
        R: Send,
        F: Fn(&T) -> R + Sync,
    {
        context.checkpoint()?;
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let worker_count = self.max_parallelism().min(inputs.len());
        let next = AtomicUsize::new(0);
        let stopped = Mutex::new(None);
        let outputs = Mutex::new(
            std::iter::repeat_with(|| None)
                .take(inputs.len())
                .collect::<Vec<Option<R>>>(),
        );

        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| loop {
                    if let Err(reason) = context.checkpoint() {
                        let mut stopped = stopped
                            .lock()
                            .expect("bounded executor cancellation lock should not be poisoned");
                        stopped.get_or_insert(reason);
                        break;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(input) = inputs.get(index) else {
                        break;
                    };
                    let output = operation(input);
                    outputs
                        .lock()
                        .expect("bounded executor output lock should not be poisoned")[index] =
                        Some(output);
                });
            }
        });

        if let Some(reason) = *stopped
            .lock()
            .expect("bounded executor cancellation lock should not be poisoned")
        {
            return Err(reason);
        }
        context.checkpoint()?;
        Ok(outputs
            .into_inner()
            .expect("bounded executor output lock should not be poisoned")
            .into_iter()
            .map(|output| output.expect("each bounded executor input must produce one output"))
            .collect())
    }
}

impl Default for BoundedExecutor {
    fn default() -> Self {
        Self::new(NonZeroUsize::MIN)
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedExecutor;
    use skein_core::{RuntimeCancellationReason, RuntimeCancellationToken, RuntimeTaskContext};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn parallel_map_preserves_input_order() {
        let executor = BoundedExecutor::new(NonZeroUsize::new(4).unwrap());
        let output = executor.map_ordered(&[3, 1, 4, 2], |value| value * value);
        assert_eq!(output, vec![9, 1, 16, 4]);
    }

    #[test]
    fn parallel_map_respects_the_worker_bound() {
        let executor = BoundedExecutor::new(NonZeroUsize::new(2).unwrap());
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);

        let output = executor.map_ordered(&[1, 2, 3, 4], |value| {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(current, Ordering::SeqCst);
            std::thread::yield_now();
            active.fetch_sub(1, Ordering::SeqCst);
            value * 2
        });

        assert_eq!(output, vec![2, 4, 6, 8]);
        assert!(peak.load(Ordering::SeqCst) <= 2);
    }

    #[test]
    fn controlled_parallel_map_stops_between_inputs() {
        let executor = BoundedExecutor::new(NonZeroUsize::MIN);
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let visited = AtomicUsize::new(0);

        let result = executor.map_ordered_with_context(&[1, 2, 3], &context, |value| {
            visited.fetch_add(1, Ordering::SeqCst);
            if *value == 1 {
                token.cancel();
            }
            value * 2
        });

        assert_eq!(result, Err(RuntimeCancellationReason::Cancelled));
        assert_eq!(visited.load(Ordering::SeqCst), 1);
    }
}
