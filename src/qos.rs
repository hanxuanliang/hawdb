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
    pub source_graph_commit_lag: u64,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedBackgroundWork {
    pub index: usize,
    pub decision: BackgroundWorkDecision,
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
    Defer {
        code: QosAdmissionCode,
        reason: String,
    },
    Reject {
        code: QosAdmissionCode,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QosAdmissionCode {
    BackgroundDisabled,
    PerWorkLimitExceeded,
    TotalBackgroundLimitExceeded,
    ClassBackgroundLimitExceeded,
    TenantBudgetExceeded,
}

impl QosAdmissionCode {
    pub fn as_str(self) -> &'static str {
        match self {
            QosAdmissionCode::BackgroundDisabled => "background_disabled",
            QosAdmissionCode::PerWorkLimitExceeded => "per_work_limit_exceeded",
            QosAdmissionCode::TotalBackgroundLimitExceeded => "total_background_limit_exceeded",
            QosAdmissionCode::ClassBackgroundLimitExceeded => "class_background_limit_exceeded",
            QosAdmissionCode::TenantBudgetExceeded => "tenant_budget_exceeded",
        }
    }
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

        let source_graph_commit_lag_score = self.source_graph_commit_lag.min(1_000_000);
        if source_graph_commit_lag_score > 0 {
            score = score.saturating_add(source_graph_commit_lag_score);
            reasons.push(format!(
                "source graph commit lag {}",
                self.source_graph_commit_lag
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
        let policy_admission = self.admit(state, &plan.request);
        if plan.request.priority != WorkPriority::Background {
            return BackgroundWorkDecision {
                admission: policy_admission,
                score: 0,
                reasons: vec!["foreground work is not background-ranked".to_string()],
            };
        }

        let admission = match (
            policy_admission,
            plan.hint
                .tenant_budget_defer_reason(plan.request.estimated_operations),
        ) {
            (QosAdmission::Admit, Some(reason)) => QosAdmission::Defer {
                code: QosAdmissionCode::TenantBudgetExceeded,
                reason,
            },
            (admission, _) => admission,
        };
        let (score, mut reasons) = plan
            .hint
            .score_with_reasons(plan.request.estimated_operations);
        match &admission {
            QosAdmission::Admit => {}
            QosAdmission::Defer { reason, .. } => {
                reasons.push(format!("admission deferred: {reason}"));
            }
            QosAdmission::Reject { reason, .. } => {
                reasons.push(format!("admission rejected: {reason}"));
            }
        }

        BackgroundWorkDecision {
            admission,
            score,
            reasons,
        }
    }

    pub fn rank_background_work(
        &self,
        state: &LocalQosState,
        plans: &[BackgroundWorkPlan],
    ) -> Vec<RankedBackgroundWork> {
        let mut ranked = plans
            .iter()
            .enumerate()
            .map(|(index, plan)| RankedBackgroundWork {
                index,
                decision: self.evaluate_background_work(state, plan),
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            admission_rank(&left.decision.admission)
                .cmp(&admission_rank(&right.decision.admission))
                .then_with(|| right.decision.score.cmp(&left.decision.score))
                .then_with(|| left.index.cmp(&right.index))
        });
        ranked
    }

    fn admit_background(&self, state: &LocalQosState, request: &WorkRequest) -> QosAdmission {
        if !self.background_enabled {
            return QosAdmission::Defer {
                code: QosAdmissionCode::BackgroundDisabled,
                reason: "background work is disabled".to_string(),
            };
        }
        if let Some(limit) = self.max_background_operations {
            if request.estimated_operations > limit {
                return QosAdmission::Defer {
                    code: QosAdmissionCode::PerWorkLimitExceeded,
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
                    code: QosAdmissionCode::TotalBackgroundLimitExceeded,
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
                    code: QosAdmissionCode::ClassBackgroundLimitExceeded,
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

impl BackgroundWorkHint {
    fn tenant_budget_defer_reason(&self, estimated_operations: usize) -> Option<String> {
        self.tenant_budget_remaining_operations
            .filter(|remaining| *remaining < estimated_operations)
            .map(|remaining| {
                format!(
                    "tenant budget remaining {remaining} below estimated operations {estimated_operations}"
                )
            })
    }
}

impl QosAdmission {
    pub fn code(&self) -> Option<QosAdmissionCode> {
        match self {
            QosAdmission::Admit => None,
            QosAdmission::Defer { code, .. } | QosAdmission::Reject { code, .. } => Some(*code),
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            QosAdmission::Admit => None,
            QosAdmission::Defer { reason, .. } | QosAdmission::Reject { reason, .. } => {
                Some(reason)
            }
        }
    }
}

fn admission_rank(admission: &QosAdmission) -> u8 {
    match admission {
        QosAdmission::Admit => 0,
        QosAdmission::Defer { .. } => 1,
        QosAdmission::Reject { .. } => 2,
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

    pub fn rank_background_work(&self, plans: &[BackgroundWorkPlan]) -> Vec<RankedBackgroundWork> {
        self.policy.rank_background_work(&self.state, plans)
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
        QosAdmission, QosAdmissionCode, WorkClass, WorkRequest,
    };

    #[test]
    fn qos_admission_codes_have_stable_string_encodings() {
        assert_eq!(
            QosAdmissionCode::BackgroundDisabled.as_str(),
            "background_disabled"
        );
        assert_eq!(
            QosAdmissionCode::PerWorkLimitExceeded.as_str(),
            "per_work_limit_exceeded"
        );
        assert_eq!(
            QosAdmissionCode::TotalBackgroundLimitExceeded.as_str(),
            "total_background_limit_exceeded"
        );
        assert_eq!(
            QosAdmissionCode::ClassBackgroundLimitExceeded.as_str(),
            "class_background_limit_exceeded"
        );
        assert_eq!(
            QosAdmissionCode::TenantBudgetExceeded.as_str(),
            "tenant_budget_exceeded"
        );
    }

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

        let admission = policy.admit(&LocalQosState::default(), &request);

        assert_eq!(admission.code(), Some(QosAdmissionCode::BackgroundDisabled));
        assert_eq!(
            admission.code().map(QosAdmissionCode::as_str),
            Some("background_disabled")
        );
        assert!(matches!(
            admission,
            QosAdmission::Defer { reason, .. } if reason.contains("disabled")
        ));
    }

    #[test]
    fn defers_background_over_per_work_budget() {
        let policy = LocalQosPolicy {
            max_background_operations: Some(4),
            ..LocalQosPolicy::default()
        };
        let request = WorkRequest::background(WorkClass::Projection, 5);

        let admission = policy.admit(&LocalQosState::default(), &request);

        assert_eq!(
            admission.code(),
            Some(QosAdmissionCode::PerWorkLimitExceeded)
        );
        assert_eq!(
            admission.code().map(QosAdmissionCode::as_str),
            Some("per_work_limit_exceeded")
        );
        assert!(matches!(
            admission,
            QosAdmission::Defer { reason, .. } if reason.contains("per-work limit")
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

        let admission = policy.admit(&state, &request);

        assert_eq!(
            admission.code(),
            Some(QosAdmissionCode::TotalBackgroundLimitExceeded)
        );
        assert_eq!(
            admission.code().map(QosAdmissionCode::as_str),
            Some("total_background_limit_exceeded")
        );
        assert!(matches!(
            admission,
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 12")
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
            source_graph_commit_lag: 3,
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
            .any(|reason| reason.contains("source graph commit lag 3")));
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
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 4")
        ));
        assert!(decision.score > 0);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("admission deferred")));
    }

    #[test]
    fn tenant_budget_defers_background_work_without_rejecting_it() {
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

        assert_eq!(
            decision.admission.code(),
            Some(QosAdmissionCode::TenantBudgetExceeded)
        );
        assert_eq!(
            decision.admission.code().map(QosAdmissionCode::as_str),
            Some("tenant_budget_exceeded")
        );
        assert_eq!(
            decision.admission.reason(),
            Some("tenant budget remaining 4 below estimated operations 8")
        );
        assert!(matches!(
            decision.admission,
            QosAdmission::Defer { ref reason, .. } if reason.contains("tenant budget remaining 4")
        ));
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
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 4")
        ));
        assert!(decision.score > 0);

        scheduler.finish(running);
    }

    #[test]
    fn background_work_ranking_prefers_admitted_high_value_plans_stably() {
        let policy = LocalQosPolicy {
            max_total_background_operations: Some(3),
            ..LocalQosPolicy::default()
        };
        let state = LocalQosState {
            running_background_operations: 1,
            ..LocalQosState::default()
        };
        let plans = vec![
            BackgroundWorkPlan::background(
                WorkClass::Projection,
                3,
                BackgroundWorkHint {
                    active_topic: true,
                    query_probability_per_million: 1_000_000,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Import,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 10,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Analytics,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 100_000,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Shadow,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 100_000,
                    ..BackgroundWorkHint::default()
                },
            ),
        ];

        let ranked = policy.rank_background_work(&state, &plans);

        assert_eq!(
            ranked.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![2, 3, 1, 0]
        );
        assert!(matches!(ranked[0].decision.admission, QosAdmission::Admit));
        assert!(matches!(ranked[1].decision.admission, QosAdmission::Admit));
        assert!(matches!(ranked[2].decision.admission, QosAdmission::Admit));
        assert!(matches!(
            ranked[3].decision.admission,
            QosAdmission::Defer { .. }
        ));
        assert!(ranked[0].decision.score >= ranked[1].decision.score);
    }

    #[test]
    fn scheduler_ranks_background_work_against_current_state() {
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(2),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, 1))
            .unwrap();
        let plans = vec![
            BackgroundWorkPlan::background(
                WorkClass::Import,
                2,
                BackgroundWorkHint {
                    active_topic: true,
                    ..BackgroundWorkHint::default()
                },
            ),
            BackgroundWorkPlan::background(
                WorkClass::Import,
                1,
                BackgroundWorkHint {
                    query_probability_per_million: 1,
                    ..BackgroundWorkHint::default()
                },
            ),
        ];

        let ranked = scheduler.rank_background_work(&plans);

        assert_eq!(ranked[0].index, 1);
        assert_eq!(ranked[1].index, 0);
        assert!(matches!(ranked[0].decision.admission, QosAdmission::Admit));
        assert!(matches!(
            ranked[1].decision.admission,
            QosAdmission::Defer { .. }
        ));

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
            QosAdmission::Defer { reason, .. } if reason.contains("above limit 12")
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
        assert_eq!(
            same_class.code(),
            Some(QosAdmissionCode::ClassBackgroundLimitExceeded)
        );
        assert_eq!(
            same_class.code().map(QosAdmissionCode::as_str),
            Some("class_background_limit_exceeded")
        );
        assert!(matches!(
            same_class,
            QosAdmission::Defer { reason, .. } if reason.contains("class limit 4")
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
