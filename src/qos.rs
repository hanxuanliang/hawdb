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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackgroundWorkHint {
    pub active_topic: bool,
    pub recent_delta_operations: usize,
    pub query_probability_per_million: u32,
    pub staleness_millis: u64,
    pub staleness_ttl_millis: Option<u64>,
    pub freshness_slo_millis: Option<u64>,
    pub tenant_budget_remaining_operations: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundWorkPlan {
    pub request: WorkRequest,
    pub hint: BackgroundWorkHint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundWorkDecision {
    pub admission: QosAdmission,
    pub score: u64,
    pub reasons: Vec<String>,
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

impl BackgroundWorkHint {
    pub fn expected_value_score(&self) -> u64 {
        self.score_with_reasons(0).0
    }

    fn score_with_reasons(&self, estimated_operations: usize) -> (u64, Vec<String>) {
        let mut score = 0u64;
        let mut reasons = Vec::new();

        if let Some(remaining) = self.tenant_budget_remaining_operations {
            if remaining < estimated_operations {
                reasons.push(format!(
                    "tenant budget remaining {remaining} below estimated operations {estimated_operations}"
                ));
                return (0, reasons);
            }
        }

        if self.active_topic {
            score = score.saturating_add(1_000_000);
            reasons.push("active topic".to_string());
        }

        let query_probability = u64::from(self.query_probability_per_million).min(1_000_000);
        if query_probability > 0 {
            score = score.saturating_add(query_probability);
            reasons.push(format!("query probability {query_probability} per million"));
        }

        let recent_delta_score = (self.recent_delta_operations as u64).min(1_000_000);
        if recent_delta_score > 0 {
            score = score.saturating_add(recent_delta_score);
            reasons.push(format!(
                "recent delta operations {}",
                self.recent_delta_operations
            ));
        }

        if let Some(ttl) = self.staleness_ttl_millis {
            let staleness_score = scaled_staleness_score(self.staleness_millis, ttl);
            if staleness_score > 0 {
                score = score.saturating_add(staleness_score);
                if self.staleness_millis >= ttl {
                    reasons.push(format!(
                        "staleness ttl reached at {} ms",
                        self.staleness_millis
                    ));
                } else {
                    reasons.push(format!(
                        "staleness {} of ttl {} ms",
                        self.staleness_millis, ttl
                    ));
                }
            }
        }

        if let Some(slo) = self.freshness_slo_millis {
            let freshness_score = scaled_staleness_score(self.staleness_millis, slo);
            if freshness_score > 0 {
                score = score.saturating_add(freshness_score);
                if self.staleness_millis >= slo {
                    reasons.push(format!(
                        "freshness slo missed at {} ms",
                        self.staleness_millis
                    ));
                } else {
                    reasons.push(format!(
                        "freshness age {} of slo {} ms",
                        self.staleness_millis, slo
                    ));
                }
            }
        }

        (score, reasons)
    }
}

impl BackgroundWorkPlan {
    pub fn background(
        class: WorkClass,
        estimated_operations: usize,
        hint: BackgroundWorkHint,
    ) -> Self {
        Self {
            request: WorkRequest::background(class, estimated_operations),
            hint,
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

    pub fn evaluate_background_work(
        &self,
        state: &LocalQosState,
        plan: &BackgroundWorkPlan,
    ) -> BackgroundWorkDecision {
        let admission = self.admit(state, &plan.request);
        if plan.request.priority != WorkPriority::Background {
            return BackgroundWorkDecision {
                admission,
                score: 0,
                reasons: vec!["foreground work is not background-ranked".to_string()],
            };
        }

        let (score, mut reasons) = plan
            .hint
            .score_with_reasons(plan.request.estimated_operations);
        match &admission {
            QosAdmission::Admit => {}
            QosAdmission::Defer { reason } => {
                reasons.push(format!("admission deferred: {reason}"));
            }
            QosAdmission::Reject { reason } => {
                reasons.push(format!("admission rejected: {reason}"));
            }
        }

        BackgroundWorkDecision {
            admission,
            score,
            reasons,
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

fn scaled_staleness_score(age_millis: u64, limit_millis: u64) -> u64 {
    if age_millis == 0 || limit_millis == 0 {
        return 0;
    }
    if age_millis >= limit_millis {
        return 1_000_000;
    }
    age_millis.saturating_mul(1_000_000) / limit_millis
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

    pub fn evaluate_background_work(&self, plan: &BackgroundWorkPlan) -> BackgroundWorkDecision {
        self.policy.evaluate_background_work(&self.state, plan)
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
        BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosScheduler, LocalQosState,
        QosAdmission, WorkClass, WorkRequest,
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
    fn background_work_hint_scores_expected_value_signals() {
        let low = BackgroundWorkHint {
            query_probability_per_million: 10_000,
            recent_delta_operations: 2,
            staleness_millis: 100,
            staleness_ttl_millis: Some(1_000),
            ..BackgroundWorkHint::default()
        };
        let high = BackgroundWorkHint {
            active_topic: true,
            query_probability_per_million: 800_000,
            recent_delta_operations: 20,
            staleness_millis: 6_000,
            staleness_ttl_millis: Some(1_000),
            freshness_slo_millis: Some(5_000),
            ..BackgroundWorkHint::default()
        };

        assert!(high.expected_value_score() > low.expected_value_score());
        let decision = LocalQosPolicy::default().evaluate_background_work(
            &LocalQosState::default(),
            &BackgroundWorkPlan::background(WorkClass::Projection, 1, high),
        );

        assert_eq!(decision.admission, QosAdmission::Admit);
        assert!(decision.score > 0);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("active topic")));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("staleness ttl reached")));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("freshness slo missed")));
    }

    #[test]
    fn background_work_evaluation_keeps_value_when_budget_defers() {
        let policy = LocalQosPolicy {
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        };
        let state = LocalQosState {
            running_background_operations: 3,
            ..LocalQosState::default()
        };
        let decision = policy.evaluate_background_work(
            &state,
            &BackgroundWorkPlan::background(
                WorkClass::Analytics,
                2,
                BackgroundWorkHint {
                    active_topic: true,
                    query_probability_per_million: 500_000,
                    ..BackgroundWorkHint::default()
                },
            ),
        );

        assert!(matches!(
            decision.admission,
            QosAdmission::Defer { reason } if reason.contains("above limit 4")
        ));
        assert!(decision.score > 0);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("admission deferred")));
    }

    #[test]
    fn tenant_budget_can_zero_background_work_score_without_rejecting_admission() {
        let decision = LocalQosPolicy::default().evaluate_background_work(
            &LocalQosState::default(),
            &BackgroundWorkPlan::background(
                WorkClass::Import,
                8,
                BackgroundWorkHint {
                    active_topic: true,
                    query_probability_per_million: 1_000_000,
                    tenant_budget_remaining_operations: Some(4),
                    ..BackgroundWorkHint::default()
                },
            ),
        );

        assert_eq!(decision.admission, QosAdmission::Admit);
        assert_eq!(decision.score, 0);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("tenant budget remaining 4")));
    }

    #[test]
    fn scheduler_evaluates_background_work_against_running_state() {
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 3))
            .unwrap();

        let decision = scheduler.evaluate_background_work(&BackgroundWorkPlan::background(
            WorkClass::Import,
            2,
            BackgroundWorkHint {
                active_topic: true,
                ..BackgroundWorkHint::default()
            },
        ));

        assert!(matches!(
            decision.admission,
            QosAdmission::Defer { reason } if reason.contains("above limit 4")
        ));
        assert!(decision.score > 0);

        scheduler.finish(running);
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
