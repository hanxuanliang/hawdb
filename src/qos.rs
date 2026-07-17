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

pub const WORK_CLASS_COUNT: usize = 6;

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
    pub max_background_operations_by_class: [Option<usize>; WORK_CLASS_COUNT],
    pub background_enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalQosState {
    pub running_background_operations: usize,
    pub running_background_operations_by_class: [usize; WORK_CLASS_COUNT],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QosAdmission {
    Admit,
    Defer { reason: String },
    Reject { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalQosPermit {
    request: WorkRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalQosScheduler {
    policy: LocalQosPolicy,
    state: LocalQosState,
}

impl Default for LocalQosPolicy {
    fn default() -> Self {
        Self {
            max_background_operations: Some(1024),
            max_total_background_operations: Some(4096),
            max_background_operations_by_class: [None; WORK_CLASS_COUNT],
            background_enabled: true,
        }
    }
}

impl WorkClass {
    pub fn as_index(self) -> usize {
        match self {
            WorkClass::Query => 0,
            WorkClass::Mutation => 1,
            WorkClass::Projection => 2,
            WorkClass::Import => 3,
            WorkClass::Analytics => 4,
            WorkClass::Shadow => 5,
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
        if let Some(limit) = self.max_background_operations_by_class[request.class.as_index()] {
            let class_total = state.running_background_operations_by_class
                [request.class.as_index()]
            .saturating_add(request.estimated_operations);
            if class_total > limit {
                return QosAdmission::Defer {
                    reason: format!(
                        "background {:?} would raise class running operations to {class_total}, above class limit {limit}",
                        request.class
                    ),
                };
            }
        }
        QosAdmission::Admit
    }
}

impl LocalQosPermit {
    pub fn request(&self) -> &WorkRequest {
        &self.request
    }
}

impl LocalQosScheduler {
    pub fn new(policy: LocalQosPolicy) -> Self {
        Self {
            policy,
            state: LocalQosState::default(),
        }
    }

    pub fn policy(&self) -> &LocalQosPolicy {
        &self.policy
    }

    pub fn state(&self) -> &LocalQosState {
        &self.state
    }

    pub fn admit(&self, request: &WorkRequest) -> QosAdmission {
        self.policy.admit(&self.state, request)
    }

    pub fn try_start(
        &mut self,
        request: WorkRequest,
    ) -> std::result::Result<LocalQosPermit, QosAdmission> {
        match self.policy.admit(&self.state, &request) {
            QosAdmission::Admit => {
                if request.priority == WorkPriority::Background {
                    self.state.running_background_operations = self
                        .state
                        .running_background_operations
                        .saturating_add(request.estimated_operations);
                    let class_index = request.class.as_index();
                    self.state.running_background_operations_by_class[class_index] =
                        self.state.running_background_operations_by_class[class_index]
                            .saturating_add(request.estimated_operations);
                }
                Ok(LocalQosPermit { request })
            }
            admission => Err(admission),
        }
    }

    pub fn finish(&mut self, permit: LocalQosPermit) {
        if permit.request.priority == WorkPriority::Background {
            self.state.running_background_operations = self
                .state
                .running_background_operations
                .saturating_sub(permit.request.estimated_operations);
            let class_index = permit.request.class.as_index();
            self.state.running_background_operations_by_class[class_index] =
                self.state.running_background_operations_by_class[class_index]
                    .saturating_sub(permit.request.estimated_operations);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LocalQosPolicy, LocalQosScheduler, LocalQosState, QosAdmission, WorkClass, WorkRequest,
    };

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
            ..LocalQosState::default()
        };
        let request = WorkRequest::background(WorkClass::Analytics, 5);

        assert!(matches!(
            policy.admit(&state, &request),
            QosAdmission::Defer { reason } if reason.contains("above limit 12")
        ));
    }

    #[test]
    fn scheduler_tracks_background_running_operations() {
        let policy = LocalQosPolicy {
            max_background_operations: Some(10),
            max_total_background_operations: Some(12),
            ..LocalQosPolicy::default()
        };
        let mut scheduler = LocalQosScheduler::new(policy);

        let first = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 8))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 8);

        let second = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 5))
            .unwrap_err();
        assert!(matches!(
            second,
            QosAdmission::Defer { reason } if reason.contains("above limit 12")
        ));
        assert_eq!(scheduler.state().running_background_operations, 8);

        scheduler.finish(first);
        assert_eq!(scheduler.state().running_background_operations, 0);

        let second = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 5))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 5);
        scheduler.finish(second);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn scheduler_does_not_charge_foreground_work() {
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(1),
            ..LocalQosPolicy::default()
        });

        let permit = scheduler
            .try_start(WorkRequest::foreground(WorkClass::Query, usize::MAX))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 0);

        scheduler.finish(permit);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn scheduler_tracks_background_running_operations_by_class() {
        let mut class_limits = [None; super::WORK_CLASS_COUNT];
        class_limits[WorkClass::Projection.as_index()] = Some(4);
        class_limits[WorkClass::Analytics.as_index()] = Some(10);
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(10),
            max_total_background_operations: Some(20),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        });

        let projection = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap();
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [WorkClass::Projection.as_index()],
            3
        );

        let same_class = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 2))
            .unwrap_err();
        assert!(matches!(
            same_class,
            QosAdmission::Defer { reason } if reason.contains("class limit 4")
        ));

        let analytics = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 2))
            .unwrap();
        assert_eq!(scheduler.state().running_background_operations, 5);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [WorkClass::Analytics.as_index()],
            2
        );

        scheduler.finish(projection);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [WorkClass::Projection.as_index()],
            0
        );
        assert_eq!(scheduler.state().running_background_operations, 2);

        scheduler.finish(analytics);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn class_budget_does_not_gate_foreground_work() {
        let mut class_limits = [None; super::WORK_CLASS_COUNT];
        class_limits[WorkClass::Projection.as_index()] = Some(0);
        let policy = LocalQosPolicy {
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::foreground(WorkClass::Projection, usize::MAX);

        assert_eq!(
            policy.admit(&LocalQosState::default(), &request),
            QosAdmission::Admit
        );
    }
}
