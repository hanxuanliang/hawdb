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
}

impl Default for BoundedExecutor {
    fn default() -> Self {
        Self::new(NonZeroUsize::MIN)
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedExecutor;
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
}
