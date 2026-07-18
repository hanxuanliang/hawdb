use crate::cost::{PlanCost, PlanCostBreakdown};
use crate::trace::OptimizerTrace;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Memo,
    DirectFallback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizationSearchReport {
    groups: usize,
    mode: SearchMode,
    warnings: Vec<String>,
    decisions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedPlanTrace {
    pub explain: String,
    pub fingerprint: String,
    pub cost: PlanCost,
    pub cost_breakdown: PlanCostBreakdown,
    pub operator_counts: BTreeMap<String, usize>,
    pub class_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleEvent {
    rule: String,
    outcome: RuleOutcome,
    detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleOutcome {
    Applied,
    Skipped,
    Estimated,
    Selected,
}

impl OptimizationSearchReport {
    pub fn memo(groups: usize) -> Self {
        Self {
            groups,
            mode: SearchMode::Memo,
            warnings: Vec::new(),
            decisions: Vec::new(),
        }
    }

    pub fn direct_fallback(required_groups: usize, max_groups: usize) -> Self {
        let mut report = Self {
            groups: required_groups,
            mode: SearchMode::DirectFallback,
            warnings: Vec::new(),
            decisions: Vec::new(),
        };
        report.warnings.push(format!(
            "optimizer memo budget exceeded: required_groups={required_groups} max_groups={max_groups}; used deterministic direct physical fallback"
        ));
        report
    }

    pub fn groups(&self) -> usize {
        self.groups
    }

    pub fn mode(&self) -> SearchMode {
        self.mode
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn decisions(&self) -> &[String] {
        &self.decisions
    }

    pub fn push_decision(&mut self, decision: impl Into<String>) {
        self.decisions.push(decision.into());
    }

    pub fn extend_decisions(&mut self, decisions: impl IntoIterator<Item = String>) {
        self.decisions.extend(decisions);
    }

    pub fn push_rule_event(&mut self, event: RuleEvent) {
        self.decisions.push(event.into_decision());
    }

    pub fn record_selected_plan_cost(&mut self, cost: PlanCost) {
        self.push_decision(format!(
            "selected physical plan cost: estimated_rows={} cost={}",
            cost.estimated_rows, cost.cost
        ));
    }

    pub fn into_trace(self, selected: SelectedPlanTrace) -> OptimizerTrace {
        OptimizerTrace {
            groups: self.groups,
            selected_plan: selected.explain,
            selected_plan_fingerprint: selected.fingerprint,
            selected_plan_cost: selected.cost,
            selected_plan_cost_breakdown: selected.cost_breakdown,
            selected_plan_operator_counts: selected.operator_counts,
            selected_plan_class_counts: selected.class_counts,
            warnings: self.warnings,
            decisions: self.decisions,
        }
    }
}

impl RuleEvent {
    pub fn applied(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Applied, detail)
    }

    pub fn skipped(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Skipped, detail)
    }

    pub fn estimated(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Estimated, detail)
    }

    pub fn selected(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Selected, detail)
    }

    pub fn new(rule: impl Into<String>, outcome: RuleOutcome, detail: impl Into<String>) -> Self {
        Self {
            rule: rule.into(),
            outcome,
            detail: detail.into(),
        }
    }

    pub fn rule(&self) -> &str {
        &self.rule
    }

    pub fn outcome(&self) -> RuleOutcome {
        self.outcome
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn into_decision(self) -> String {
        format!("{} {}: {}", self.outcome.as_str(), self.rule, self.detail)
    }
}

impl RuleOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RuleOutcome::Applied => "apply",
            RuleOutcome::Skipped => "skip",
            RuleOutcome::Estimated => "estimate",
            RuleOutcome::Selected => "selected",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OptimizationSearchReport, RuleEvent, RuleOutcome, SearchMode, SelectedPlanTrace};
    use crate::{PlanCost, PlanCostBreakdown};
    use std::collections::BTreeMap;

    #[test]
    fn direct_fallback_report_records_budget_warning() {
        let report = OptimizationSearchReport::direct_fallback(9, 4);

        assert_eq!(report.groups(), 9);
        assert_eq!(report.mode(), SearchMode::DirectFallback);
        assert_eq!(report.warnings().len(), 1);
        assert!(report.warnings()[0].contains("required_groups=9 max_groups=4"));
        assert!(report.decisions().is_empty());
    }

    #[test]
    fn rule_event_formats_stable_decision_text() {
        let event = RuleEvent::estimated("index_seek", "rows=1 cost=3");

        assert_eq!(event.rule(), "index_seek");
        assert_eq!(event.outcome(), RuleOutcome::Estimated);
        assert_eq!(event.detail(), "rows=1 cost=3");
        assert_eq!(event.into_decision(), "estimate index_seek: rows=1 cost=3");
    }

    #[test]
    fn search_report_builds_legacy_trace_surface() {
        let mut report = OptimizationSearchReport::memo(2);
        report.push_decision("choose IndexNodeSeek");
        report.record_selected_plan_cost(PlanCost {
            estimated_rows: 1,
            cost: 4,
        });

        let trace = report.into_trace(SelectedPlanTrace {
            explain: "IndexNodeSeek".to_string(),
            fingerprint: "IndexNodeSeek(1:m:6:Memory.id=i:1)".to_string(),
            cost: PlanCost {
                estimated_rows: 1,
                cost: 4,
            },
            cost_breakdown: PlanCostBreakdown::new(1, 1, 3, 0, 0),
            operator_counts: BTreeMap::from([("IndexNodeSeek".to_string(), 1)]),
            class_counts: BTreeMap::from([("access".to_string(), 1)]),
        });

        assert_eq!(trace.groups, 2);
        assert!(trace.warnings.is_empty());
        assert_eq!(trace.decisions[0], "choose IndexNodeSeek");
        assert_eq!(
            trace.decisions[1],
            "selected physical plan cost: estimated_rows=1 cost=4"
        );
        assert_eq!(trace.selected_plan_operator_counts["IndexNodeSeek"], 1);
        assert_eq!(trace.selected_plan_class_counts["access"], 1);
    }
}
