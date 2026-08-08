use serde_json::{json, Value as JsonValue};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCoverageReport {
    pub unique_memo_plans: usize,
    pub unique_direct_fallback_plans: usize,
    pub unique_plan_pairs: usize,
    pub novel_case_count: usize,
    pub max_consecutive_non_novel_cases: usize,
}

impl PlanCoverageReport {
    pub(crate) fn json(&self) -> JsonValue {
        json!({
            "unique_memo_plans": self.unique_memo_plans,
            "unique_direct_fallback_plans": self.unique_direct_fallback_plans,
            "unique_plan_pairs": self.unique_plan_pairs,
            "novel_case_count": self.novel_case_count,
            "max_consecutive_non_novel_cases": self.max_consecutive_non_novel_cases,
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct PlanCoverageTracker {
    memo_plans: BTreeSet<String>,
    direct_fallback_plans: BTreeSet<String>,
    plan_pairs: BTreeSet<(String, String)>,
    novel_case_count: usize,
    consecutive_non_novel_cases: usize,
    max_consecutive_non_novel_cases: usize,
}

impl PlanCoverageTracker {
    pub(crate) fn observe(&mut self, memo: Option<&str>, direct_fallback: Option<&str>) -> bool {
        let mut novel = false;
        if let Some(fingerprint) = memo {
            novel |= self.memo_plans.insert(fingerprint.to_string());
        }
        if let Some(fingerprint) = direct_fallback {
            novel |= self.direct_fallback_plans.insert(fingerprint.to_string());
        }
        if let (Some(memo), Some(direct_fallback)) = (memo, direct_fallback) {
            novel |= self
                .plan_pairs
                .insert((memo.to_string(), direct_fallback.to_string()));
        }

        if novel {
            self.novel_case_count += 1;
            self.consecutive_non_novel_cases = 0;
        } else {
            self.consecutive_non_novel_cases += 1;
            self.max_consecutive_non_novel_cases = self
                .max_consecutive_non_novel_cases
                .max(self.consecutive_non_novel_cases);
        }
        novel
    }

    pub(crate) fn report(self) -> PlanCoverageReport {
        PlanCoverageReport {
            unique_memo_plans: self.memo_plans.len(),
            unique_direct_fallback_plans: self.direct_fallback_plans.len(),
            unique_plan_pairs: self.plan_pairs.len(),
            novel_case_count: self.novel_case_count,
            max_consecutive_non_novel_cases: self.max_consecutive_non_novel_cases,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_novelty_is_independent_from_repeated_plans() {
        let mut tracker = PlanCoverageTracker::default();

        assert!(tracker.observe(Some("memo-a"), Some("direct-a")));
        assert!(!tracker.observe(Some("memo-a"), Some("direct-a")));
        assert!(!tracker.observe(Some("memo-a"), Some("direct-a")));
        assert!(tracker.observe(Some("memo-b"), Some("direct-a")));
        let report = tracker.report();

        assert_eq!(report.unique_memo_plans, 2);
        assert_eq!(report.unique_direct_fallback_plans, 1);
        assert_eq!(report.unique_plan_pairs, 2);
        assert_eq!(report.novel_case_count, 2);
        assert_eq!(report.max_consecutive_non_novel_cases, 2);
    }
}
