#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkPriority {
    Foreground,
    Background,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkClass {
    Query,
    Mutation,
    Projection,
    Import,
    Analytics,
    Shadow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkRequest {
    pub class: WorkClass,
    pub priority: WorkPriority,
    pub estimated_operations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalQosPolicy {
    pub max_background_operations: Option<usize>,
    pub max_total_background_operations: Option<usize>,
    pub background_enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalQosState {
    pub running_background_operations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QosAdmission {
    Admit,
    Defer { reason: String },
    Reject { reason: String },
}

impl Default for LocalQosPolicy {
    fn default() -> Self {
        Self {
            max_background_operations: Some(1024),
            max_total_background_operations: Some(4096),
            background_enabled: true,
        }
    }
}

impl WorkRequest {
    pub fn foreground(class: WorkClass, estimated_operations: usize) -> Self {
        Self {
            class,
            priority: WorkPriority::Foreground,
            estimated_operations,
        }
    }

    pub fn background(class: WorkClass, estimated_operations: usize) -> Self {
        Self {
            class,
            priority: WorkPriority::Background,
            estimated_operations,
        }
    }
}

impl LocalQosPolicy {
    pub fn admit(&self, state: &LocalQosState, request: &WorkRequest) -> QosAdmission {
        match request.priority {
            WorkPriority::Foreground => QosAdmission::Admit,
            WorkPriority::Background => self.admit_background(state, request),
        }
    }

    fn admit_background(&self, state: &LocalQosState, request: &WorkRequest) -> QosAdmission {
        if !self.background_enabled {
            return QosAdmission::Defer {
                reason: "background work is disabled".to_string(),
            };
        }
        if let Some(limit) = self.max_background_operations {
            if request.estimated_operations > limit {
                return QosAdmission::Defer {
                    reason: format!(
                        "background {:?} estimated operations {} exceeded per-work limit {limit}",
                        request.class, request.estimated_operations
                    ),
                };
            }
        }
        if let Some(limit) = self.max_total_background_operations {
            let total = state
                .running_background_operations
                .saturating_add(request.estimated_operations);
            if total > limit {
                return QosAdmission::Defer {
                    reason: format!(
                        "background {:?} would raise running operations to {total}, above limit {limit}",
                        request.class
                    ),
                };
            }
        }
        QosAdmission::Admit
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalQosPolicy, LocalQosState, QosAdmission, WorkClass, WorkRequest};

    #[test]
    fn admits_foreground_work_without_budget_gate() {
        let policy = LocalQosPolicy::default();
        let request = WorkRequest::foreground(WorkClass::Query, usize::MAX);

        assert_eq!(
            policy.admit(&LocalQosState::default(), &request),
            QosAdmission::Admit
        );
    }

    #[test]
    fn defers_background_when_disabled() {
        let policy = LocalQosPolicy {
            background_enabled: false,
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::background(WorkClass::Projection, 1);

        assert!(matches!(
            policy.admit(&LocalQosState::default(), &request),
            QosAdmission::Defer { reason } if reason.contains("disabled")
        ));
    }

    #[test]
    fn defers_background_over_per_work_budget() {
        let policy = LocalQosPolicy {
            max_background_operations: Some(4),
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::background(WorkClass::Projection, 5);

        assert!(matches!(
            policy.admit(&LocalQosState::default(), &request),
            QosAdmission::Defer { reason } if reason.contains("per-work limit")
        ));
    }

    #[test]
    fn defers_background_when_running_budget_is_exhausted() {
        let policy = LocalQosPolicy {
            max_background_operations: Some(10),
            max_total_background_operations: Some(12),
            ..LocalQosPolicy::default()
        };
        let state = LocalQosState {
            running_background_operations: 8,
        };
        let request = WorkRequest::background(WorkClass::Analytics, 5);

        assert!(matches!(
            policy.admit(&state, &request),
            QosAdmission::Defer { reason } if reason.contains("above limit 12")
        ));
    }
}
