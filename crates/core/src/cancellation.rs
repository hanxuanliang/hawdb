use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RuntimeCancellationToken {
    state: Arc<RuntimeCancellationState>,
}

#[derive(Debug)]
struct RuntimeCancellationState {
    cancelled: AtomicBool,
    parent: Option<RuntimeCancellationToken>,
}

impl Default for RuntimeCancellationToken {
    fn default() -> Self {
        Self {
            state: Arc::new(RuntimeCancellationState {
                cancelled: AtomicBool::new(false),
                parent: None,
            }),
        }
    }
}

impl RuntimeCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) -> bool {
        !self.state.cancelled.swap(true, Ordering::AcqRel)
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
            || self
                .state
                .parent
                .as_ref()
                .is_some_and(RuntimeCancellationToken::is_cancelled)
    }

    pub fn child(&self) -> Self {
        Self {
            state: Arc::new(RuntimeCancellationState {
                cancelled: AtomicBool::new(false),
                parent: Some(self.clone()),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeTaskContext {
    cancellation: RuntimeCancellationToken,
    deadline: Option<Instant>,
}

impl RuntimeTaskContext {
    pub fn new(cancellation: RuntimeCancellationToken, deadline: Option<Instant>) -> Self {
        Self {
            cancellation,
            deadline,
        }
    }

    pub fn without_deadline(cancellation: RuntimeCancellationToken) -> Self {
        Self::new(cancellation, None)
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self::new(
            RuntimeCancellationToken::new(),
            Instant::now().checked_add(timeout),
        )
    }

    pub fn cancellation(&self) -> &RuntimeCancellationToken {
        &self.cancellation
    }

    pub fn child(&self) -> Self {
        Self::new(self.cancellation.child(), self.deadline)
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn checkpoint(&self) -> Result<(), RuntimeCancellationReason> {
        if self.cancellation.is_cancelled() {
            return Err(RuntimeCancellationReason::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(RuntimeCancellationReason::DeadlineExceeded);
        }
        Ok(())
    }
}

impl Default for RuntimeTaskContext {
    fn default() -> Self {
        Self::without_deadline(RuntimeCancellationToken::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCancellationReason {
    Cancelled,
    DeadlineExceeded,
}

impl RuntimeCancellationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

impl Display for RuntimeCancellationReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Error for RuntimeCancellationReason {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_shared_across_context_clones() {
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        assert!(context.checkpoint().is_ok());
        assert!(token.cancel());
        assert!(!token.cancel());
        assert_eq!(
            context.checkpoint(),
            Err(RuntimeCancellationReason::Cancelled)
        );
    }

    #[test]
    fn expired_deadline_fails_at_a_cooperative_checkpoint() {
        let context = RuntimeTaskContext::new(
            RuntimeCancellationToken::new(),
            Some(Instant::now() - Duration::from_millis(1)),
        );
        assert_eq!(
            context.checkpoint(),
            Err(RuntimeCancellationReason::DeadlineExceeded)
        );
    }

    #[test]
    fn child_cancellation_is_local_and_parent_cancellation_propagates() {
        let parent_token = RuntimeCancellationToken::new();
        let parent = RuntimeTaskContext::without_deadline(parent_token.clone());
        let child = parent.child();

        assert!(child.cancellation().cancel());
        assert!(child.checkpoint().is_err());
        assert!(parent.checkpoint().is_ok());

        let sibling = parent.child();
        assert!(parent_token.cancel());
        assert!(parent.checkpoint().is_err());
        assert!(sibling.checkpoint().is_err());
    }
}
