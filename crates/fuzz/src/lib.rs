use serde_json::{json, Map as JsonMap, Value as JsonValue};
use skein::api::{Database, DatabaseConfig};
use skein::executor::Row;
use skein::{SkeinError, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

pub const PLAN_DIFFERENTIAL_PROTOCOL: &str = "skein-plan-differential-fuzz-v1";
pub const REPLAY_BUNDLE_PROTOCOL: &str = "skein-plan-differential-replay-v1";
const TEMPLATE_COUNT: usize = 12;
const DEFAULT_CASE_COUNT: usize = 128;
const MAX_CASE_COUNT: usize = 10_000;

pub type Parameters = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultSemantics {
    Ordered,
    Bag,
}

impl ResultSemantics {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Bag => "bag",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutation {
    pub cypher: String,
    pub parameters: Parameters,
}

impl Mutation {
    pub fn new(cypher: impl Into<String>) -> Self {
        Self {
            cypher: cypher.into(),
            parameters: Parameters::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzCase {
    pub seed: u64,
    pub template: &'static str,
    pub mutations: Vec<Mutation>,
    pub cypher: &'static str,
    pub parameters: Parameters,
    pub result_semantics: ResultSemantics,
    pub index_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityProfile {
    pub templates: Vec<&'static str>,
    pub compares_duplicates: bool,
    pub compares_missing_and_null: bool,
    pub compares_float_bit_patterns: bool,
    pub compares_path_values: bool,
}

impl CapabilityProfile {
    pub fn plan_differential_v1() -> Self {
        Self {
            templates: vec![
                "node_scan",
                "equality_filter",
                "in_filter",
                "range_filter",
                "one_hop_expand",
                "self_loop",
                "parallel_edges",
                "cartesian_product",
                "distinct_projection",
                "aggregate",
                "top_n",
                "missing_or_null",
            ],
            compares_duplicates: true,
            compares_missing_and_null: true,
            compares_float_bit_patterns: true,
            compares_path_values: false,
        }
    }
}

pub trait Oracle {
    fn capability_profile(&self) -> CapabilityProfile;
    fn evaluate(&self, case: &FuzzCase) -> OracleResult;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleResult {
    Equivalent(DifferentialEvidence),
    Failure(FailureReport),
}

impl OracleResult {
    pub const fn is_equivalent(&self) -> bool {
        matches!(self, Self::Equivalent(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifferentialEvidence {
    pub memo: ExecutionObservation,
    pub direct_fallback: ExecutionObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureReport {
    pub reason: String,
    pub replay: ReplayBundle,
    pub memo: ExecutionObservation,
    pub direct_fallback: ExecutionObservation,
}

impl FailureReport {
    pub fn json(&self) -> JsonValue {
        json!({
            "reason": self.reason,
            "replay": self.replay.json(),
            "memo": self.memo.json(),
            "direct_fallback": self.direct_fallback.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayBundle {
    pub seed: u64,
    pub template: &'static str,
    pub mutations: Vec<Mutation>,
    pub cypher: &'static str,
    pub parameters: Parameters,
    pub result_semantics: ResultSemantics,
    pub index_enabled: bool,
}

impl ReplayBundle {
    pub fn from_case(case: &FuzzCase) -> Self {
        Self {
            seed: case.seed,
            template: case.template,
            mutations: case.mutations.clone(),
            cypher: case.cypher,
            parameters: case.parameters.clone(),
            result_semantics: case.result_semantics,
            index_enabled: case.index_enabled,
        }
    }

    pub fn json(&self) -> JsonValue {
        json!({
            "protocol": REPLAY_BUNDLE_PROTOCOL,
            "seed": self.seed,
            "template": self.template,
            "index_enabled": self.index_enabled,
            "result_semantics": self.result_semantics.as_str(),
            "mutations": self.mutations.iter().map(mutation_json).collect::<Vec<_>>(),
            "query": {
                "cypher": self.cypher,
                "parameters": parameters_json(&self.parameters),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionObservation {
    pub search_mode: Option<String>,
    pub plan_fingerprint: Option<String>,
    pub optimizer_stages: Vec<String>,
    pub outcome: ExecutionOutcome,
}

impl ExecutionObservation {
    fn json(&self) -> JsonValue {
        json!({
            "search_mode": self.search_mode,
            "plan_fingerprint": self.plan_fingerprint,
            "optimizer_stages": self.optimizer_stages,
            "outcome": self.outcome.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Rows(Vec<Row>),
    Error {
        phase: &'static str,
        class: &'static str,
        message: String,
    },
}

impl ExecutionOutcome {
    fn json(&self) -> JsonValue {
        match self {
            Self::Rows(rows) => json!({
                "status": "rows",
                "row_count": rows.len(),
                "rows": rows.iter().map(row_json).collect::<Vec<_>>(),
            }),
            Self::Error {
                phase,
                class,
                message,
            } => json!({
                "status": "error",
                "phase": phase,
                "class": class,
                "message": message,
            }),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlanDifferentialOracle;

impl Oracle for PlanDifferentialOracle {
    fn capability_profile(&self) -> CapabilityProfile {
        CapabilityProfile::plan_differential_v1()
    }

    fn evaluate(&self, case: &FuzzCase) -> OracleResult {
        let memo = execute_case(case, None);
        let direct_fallback = execute_case(case, Some(0));

        let failure_reason = compare_outcomes(
            &memo.outcome,
            &direct_fallback.outcome,
            case.result_semantics,
        )
        .err()
        .or_else(|| validate_search_modes(&memo, &direct_fallback));

        if let Some(reason) = failure_reason {
            OracleResult::Failure(FailureReport {
                reason,
                replay: ReplayBundle::from_case(case),
                memo,
                direct_fallback,
            })
        } else {
            OracleResult::Equivalent(DifferentialEvidence {
                memo,
                direct_fallback,
            })
        }
    }
}

fn execute_case(case: &FuzzCase, max_optimizer_groups: Option<usize>) -> ExecutionObservation {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_optimizer_groups,
        max_plan_cache_entries: Some(0),
        ..DatabaseConfig::default()
    });

    for mutation in &case.mutations {
        if let Err(error) = db.query_with_params(&mutation.cypher, &mutation.parameters) {
            return error_observation("mutation", error);
        }
    }

    let explain = match db.explain_query_with_params(case.cypher, &case.parameters) {
        Ok(explain) => explain,
        Err(error) => return error_observation("explain", error),
    };
    let search_mode = Some(explain.trace.search_mode.as_str().to_string());
    let plan_fingerprint = Some(explain.trace.selected_plan_fingerprint.clone());
    let optimizer_stages = explain
        .trace
        .stage_events
        .iter()
        .map(|stage| {
            let stats = stage.stats();
            format!(
                "{}:{}:{}:{}:{}:{}",
                stage.name(),
                stage.apply_order().as_str(),
                stats.input_count,
                stats.output_count,
                stats.applied_rules,
                stats.skipped_rules,
            )
        })
        .collect();

    match db.query_with_params(case.cypher, &case.parameters) {
        Ok(output) => ExecutionObservation {
            search_mode,
            plan_fingerprint,
            optimizer_stages,
            outcome: ExecutionOutcome::Rows(output.rows),
        },
        Err(error) => ExecutionObservation {
            search_mode,
            plan_fingerprint,
            optimizer_stages,
            outcome: error_outcome("execute", error),
        },
    }
}

fn error_observation(phase: &'static str, error: SkeinError) -> ExecutionObservation {
    ExecutionObservation {
        search_mode: None,
        plan_fingerprint: None,
        optimizer_stages: Vec::new(),
        outcome: error_outcome(phase, error),
    }
}

fn error_outcome(phase: &'static str, error: SkeinError) -> ExecutionOutcome {
    ExecutionOutcome::Error {
        phase,
        class: error_class(&error),
        message: error.to_string(),
    }
}

fn validate_search_modes(
    memo: &ExecutionObservation,
    direct_fallback: &ExecutionObservation,
) -> Option<String> {
    if memo.search_mode.as_deref() != Some("memo") {
        return Some(format!(
            "memo configuration selected unexpected search mode {:?}",
            memo.search_mode
        ));
    }
    if direct_fallback.search_mode.as_deref() != Some("direct_fallback") {
        return Some(format!(
            "direct fallback configuration selected unexpected search mode {:?}",
            direct_fallback.search_mode
        ));
    }
    None
}

fn compare_outcomes(
    memo: &ExecutionOutcome,
    direct_fallback: &ExecutionOutcome,
    semantics: ResultSemantics,
) -> Result<(), String> {
    match (memo, direct_fallback) {
        (ExecutionOutcome::Rows(left), ExecutionOutcome::Rows(right)) => {
            compare_rows(left, right, semantics)
        }
        (
            ExecutionOutcome::Error {
                phase: memo_phase,
                class: memo_class,
                ..
            },
            ExecutionOutcome::Error {
                phase: fallback_phase,
                class: fallback_class,
                ..
            },
        ) => Err(format!(
            "generated case failed in both optimizer paths: memo={memo_phase}/{memo_class} direct_fallback={fallback_phase}/{fallback_class}"
        )),
        (ExecutionOutcome::Error { .. }, ExecutionOutcome::Rows(_)) => {
            Err("memo optimizer failed while direct fallback returned rows".to_string())
        }
        (ExecutionOutcome::Rows(_), ExecutionOutcome::Error { .. }) => {
            Err("direct fallback failed while memo optimizer returned rows".to_string())
        }
    }
}

pub fn compare_rows(left: &[Row], right: &[Row], semantics: ResultSemantics) -> Result<(), String> {
    match semantics {
        ResultSemantics::Ordered => {
            if left == right {
                Ok(())
            } else {
                Err(format!(
                    "ordered result mismatch: memo_rows={} direct_fallback_rows={}",
                    left.len(),
                    right.len()
                ))
            }
        }
        ResultSemantics::Bag => {
            let mut left = left.to_vec();
            let mut right = right.to_vec();
            left.sort_by(compare_row);
            right.sort_by(compare_row);
            if left == right {
                Ok(())
            } else {
                Err(format!(
                    "bag result mismatch: memo_rows={} direct_fallback_rows={}",
                    left.len(),
                    right.len()
                ))
            }
        }
    }
}

fn compare_row(left: &Row, right: &Row) -> Ordering {
    left.iter().cmp(right.iter())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CampaignOptions {
    pub seed: u64,
    pub case_count: usize,
}

impl Default for CampaignOptions {
    fn default() -> Self {
        Self {
            seed: 0x9e37_79b9_7f4a_7c15,
            case_count: DEFAULT_CASE_COUNT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignCaseReport {
    pub index: usize,
    pub seed: u64,
    pub template: &'static str,
    pub index_enabled: bool,
    pub success: bool,
    pub memo_plan_fingerprint: Option<String>,
    pub direct_fallback_plan_fingerprint: Option<String>,
    pub failure: Option<FailureReport>,
}

impl CampaignCaseReport {
    fn json(&self) -> JsonValue {
        json!({
            "index": self.index,
            "seed": self.seed,
            "template": self.template,
            "index_enabled": self.index_enabled,
            "success": self.success,
            "memo_plan_fingerprint": self.memo_plan_fingerprint,
            "direct_fallback_plan_fingerprint": self.direct_fallback_plan_fingerprint,
            "failure": self.failure.as_ref().map(FailureReport::json),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignReport {
    pub seed: u64,
    pub requested_case_count: usize,
    pub executed_case_count: usize,
    pub passed_case_count: usize,
    pub failed_case_count: usize,
    pub complete_template_coverage: bool,
    pub cases: Vec<CampaignCaseReport>,
}

impl CampaignReport {
    pub const fn success(&self) -> bool {
        self.failed_case_count == 0 && self.executed_case_count > 0
    }

    pub fn json(&self) -> JsonValue {
        let profile = CapabilityProfile::plan_differential_v1();
        json!({
            "protocol": PLAN_DIFFERENTIAL_PROTOCOL,
            "success": self.success(),
            "seed": self.seed,
            "requested_case_count": self.requested_case_count,
            "executed_case_count": self.executed_case_count,
            "passed_case_count": self.passed_case_count,
            "failed_case_count": self.failed_case_count,
            "complete_template_coverage": self.complete_template_coverage,
            "capability_profile": {
                "templates": profile.templates,
                "compares_duplicates": profile.compares_duplicates,
                "compares_missing_and_null": profile.compares_missing_and_null,
                "compares_float_bit_patterns": profile.compares_float_bit_patterns,
                "compares_path_values": profile.compares_path_values,
            },
            "cases": self.cases.iter().map(CampaignCaseReport::json).collect::<Vec<_>>(),
        })
    }
}

pub fn run_campaign(options: CampaignOptions) -> Result<CampaignReport, FuzzError> {
    if options.case_count == 0 {
        return Err(FuzzError::new("case_count must be greater than zero"));
    }
    if options.case_count > MAX_CASE_COUNT {
        return Err(FuzzError::new(format!(
            "case_count exceeds the safety limit {MAX_CASE_COUNT}"
        )));
    }

    let oracle = PlanDifferentialOracle;
    let mut generator = MemCaseGenerator::new(options.seed);
    let mut cases = Vec::with_capacity(options.case_count);
    for index in 0..options.case_count {
        let case = generator.case(index);
        let report = match oracle.evaluate(&case) {
            OracleResult::Equivalent(evidence) => CampaignCaseReport {
                index,
                seed: case.seed,
                template: case.template,
                index_enabled: case.index_enabled,
                success: true,
                memo_plan_fingerprint: evidence.memo.plan_fingerprint,
                direct_fallback_plan_fingerprint: evidence.direct_fallback.plan_fingerprint,
                failure: None,
            },
            OracleResult::Failure(failure) => CampaignCaseReport {
                index,
                seed: case.seed,
                template: case.template,
                index_enabled: case.index_enabled,
                success: false,
                memo_plan_fingerprint: failure.memo.plan_fingerprint.clone(),
                direct_fallback_plan_fingerprint: failure.direct_fallback.plan_fingerprint.clone(),
                failure: Some(failure),
            },
        };
        cases.push(report);
    }

    let failed_case_count = cases.iter().filter(|case| !case.success).count();
    Ok(CampaignReport {
        seed: options.seed,
        requested_case_count: options.case_count,
        executed_case_count: cases.len(),
        passed_case_count: cases.len().saturating_sub(failed_case_count),
        failed_case_count,
        complete_template_coverage: options.case_count >= TEMPLATE_COUNT,
        cases,
    })
}

#[derive(Debug, Clone, Copy)]
struct MemCaseGenerator {
    rng: DeterministicRng,
}

impl MemCaseGenerator {
    fn new(seed: u64) -> Self {
        Self {
            rng: DeterministicRng::new(seed),
        }
    }

    fn case(&mut self, index: usize) -> FuzzCase {
        let seed = self.rng.next_u64();
        let index_enabled = seed & 1 == 0;
        let memory_count = 12 + ((seed >> 8) as usize % 5);
        let mutations = mem_mutations(memory_count, index_enabled);
        let template_index = index % TEMPLATE_COUNT;
        let selected_memory = (seed as usize) % memory_count;
        let alternate_memory = ((seed >> 16) as usize) % memory_count;
        let kind = if seed & 2 == 0 { "note" } else { "thread" };

        let mut parameters = Parameters::new();
        let (template, cypher, result_semantics) = match template_index {
            0 => (
                "node_scan",
                "MATCH (m:Memory) RETURN m.id AS id, m.kind AS kind ORDER BY id ASC",
                ResultSemantics::Ordered,
            ),
            1 => {
                parameters.insert("kind".to_string(), Value::String(kind.to_string()));
                (
                    "equality_filter",
                    "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.id AS id",
                    ResultSemantics::Bag,
                )
            }
            2 => {
                parameters.insert(
                    "ids".to_string(),
                    Value::List(vec![
                        memory_id(selected_memory),
                        memory_id(alternate_memory),
                        memory_id(selected_memory),
                    ]),
                );
                (
                    "in_filter",
                    "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id AS id ORDER BY id ASC",
                    ResultSemantics::Ordered,
                )
            }
            3 => {
                parameters.insert(
                    "minimum".to_string(),
                    Value::Int((seed % memory_count as u64) as i64),
                );
                (
                    "range_filter",
                    "MATCH (m:Memory) WHERE m.importance >= $minimum RETURN m.id AS id, m.importance AS importance ORDER BY importance ASC, id ASC",
                    ResultSemantics::Ordered,
                )
            }
            4 => {
                parameters.insert("id".to_string(), memory_id(selected_memory));
                (
                    "one_hop_expand",
                    "MATCH (m:Memory {id: $id})-[r:MENTIONS]->(e:Entity) RETURN m.id AS memory_id, e.id AS entity_id ORDER BY entity_id ASC",
                    ResultSemantics::Ordered,
                )
            }
            5 => (
                "self_loop",
                "MATCH (e:Entity)-[r:RELATES_TO]->(e) RETURN e.id AS id",
                ResultSemantics::Bag,
            ),
            6 => {
                parameters.insert("source".to_string(), entity_id(1));
                parameters.insert("target".to_string(), entity_id(2));
                (
                    "parallel_edges",
                    "MATCH (a:Entity {id: $source})-[r:RELATES_TO]->(b:Entity {id: $target}) RETURN a.id AS source, b.id AS target",
                    ResultSemantics::Bag,
                )
            }
            7 => (
                "cartesian_product",
                "MATCH (m:Memory), (e:Entity) RETURN m.id AS memory_id, e.id AS entity_id",
                ResultSemantics::Bag,
            ),
            8 => (
                "distinct_projection",
                "MATCH (m:Memory) RETURN DISTINCT m.kind AS kind ORDER BY kind ASC",
                ResultSemantics::Ordered,
            ),
            9 => (
                "aggregate",
                "MATCH (m:Memory) RETURN m.kind AS kind, count(m) AS count ORDER BY kind ASC",
                ResultSemantics::Ordered,
            ),
            10 => (
                "top_n",
                "MATCH (m:Memory) RETURN m.id AS id, m.importance AS importance ORDER BY importance DESC, id ASC LIMIT 5",
                ResultSemantics::Ordered,
            ),
            _ => (
                "missing_or_null",
                "MATCH (m:Memory) WHERE m.optional_note IS NULL RETURN m.id AS id ORDER BY id ASC",
                ResultSemantics::Ordered,
            ),
        };

        FuzzCase {
            seed,
            template,
            mutations,
            cypher,
            parameters,
            result_semantics,
            index_enabled,
        }
    }
}

fn mem_mutations(memory_count: usize, index_enabled: bool) -> Vec<Mutation> {
    let mut mutations = Vec::new();
    for index in 0..memory_count {
        let kind = if index % 2 == 0 { "note" } else { "thread" };
        let optional_note = match index % 3 {
            0 => ", optional_note: null",
            1 => ", optional_note: 'present'",
            _ => "",
        };
        mutations.push(Mutation::new(format!(
            "CREATE (:Memory {{id: 'mem-{index}', kind: '{kind}', title: 'Memory {index}', importance: {index}{optional_note}}})"
        )));
    }
    for index in 0..6 {
        mutations.push(Mutation::new(format!(
            "CREATE (:Entity {{id: 'entity-{index}', name: 'Entity {index}'}})"
        )));
    }
    for index in 0..memory_count {
        mutations.push(Mutation::new(format!(
            "MATCH (m:Memory {{id: 'mem-{index}'}}), (e:Entity {{id: 'entity-{}'}}) CREATE (m)-[:MENTIONS {{weight: {}}}]->(e)",
            index % 6,
            index % 4,
        )));
    }
    mutations.push(Mutation::new(
        "MATCH (source:Entity {id: 'entity-0'}), (target:Entity {id: 'entity-0'}) CREATE (source)-[:RELATES_TO {weight: 0}]->(target)",
    ));
    for weight in [1, 2] {
        mutations.push(Mutation::new(format!(
            "MATCH (a:Entity {{id: 'entity-1'}}), (b:Entity {{id: 'entity-2'}}) CREATE (a)-[:RELATES_TO {{weight: {weight}}}]->(b)"
        )));
    }
    mutations.push(Mutation::new(
        "MATCH (a:Entity {id: 'entity-2'}), (b:Entity {id: 'entity-3'}) CREATE (a)-[:RELATES_TO {weight: 3}]->(b)",
    ));
    if index_enabled {
        mutations.push(Mutation::new("CREATE INDEX ON :Memory(id)"));
        mutations.push(Mutation::new("CREATE RANGE INDEX ON :Memory(importance)"));
    }
    mutations
}

fn memory_id(index: usize) -> Value {
    Value::String(format!("mem-{index}"))
}

fn entity_id(index: usize) -> Value {
    Value::String(format!("entity-{index}"))
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
}

fn error_class(error: &SkeinError) -> &'static str {
    match error {
        SkeinError::Parse(_) => "parse",
        SkeinError::Semantic(_) => "semantic",
        SkeinError::Execution(_) => "execution",
        SkeinError::Storage(_) => "storage",
        SkeinError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

fn mutation_json(mutation: &Mutation) -> JsonValue {
    json!({
        "cypher": mutation.cypher,
        "parameters": parameters_json(&mutation.parameters),
    })
}

fn parameters_json(parameters: &Parameters) -> JsonValue {
    JsonValue::Object(
        parameters
            .iter()
            .map(|(key, value)| (key.clone(), typed_value_json(value)))
            .collect(),
    )
}

fn row_json(row: &Row) -> JsonValue {
    JsonValue::Object(
        row.iter()
            .map(|(key, value)| (key.clone(), typed_value_json(value)))
            .collect(),
    )
}

fn typed_value_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => json!({"type": "null"}),
        Value::Bool(value) => json!({"type": "bool", "value": value}),
        Value::Int(value) => json!({"type": "int", "value": value}),
        Value::Float(value) => json!({
            "type": "float",
            "bits": format!("{:016x}", value.to_bits()),
        }),
        Value::String(value) => json!({"type": "string", "value": value}),
        Value::List(values) => json!({
            "type": "list",
            "values": values.iter().map(typed_value_json).collect::<Vec<_>>(),
        }),
        Value::Map(values) => {
            let values = values
                .iter()
                .map(|(key, value)| (key.clone(), typed_value_json(value)))
                .collect::<JsonMap<_, _>>();
            json!({"type": "map", "values": values})
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzError {
    message: String,
}

impl FuzzError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for FuzzError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for FuzzError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Row {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    #[test]
    fn bag_comparison_preserves_duplicate_multiplicity() {
        let one = row([("id", Value::Int(1))]);
        let two = row([("id", Value::Int(2))]);

        assert!(compare_rows(
            &[one.clone(), two.clone(), one.clone()],
            &[two, one.clone(), one],
            ResultSemantics::Bag,
        )
        .is_ok());
        assert!(compare_rows(
            &[row([("id", Value::Int(1))]), row([("id", Value::Int(1))])],
            &[row([("id", Value::Int(1))])],
            ResultSemantics::Bag,
        )
        .is_err());
    }

    #[test]
    fn typed_comparison_distinguishes_missing_null_and_float_bits() {
        assert!(compare_rows(
            &[Row::new()],
            &[row([("value", Value::Null)])],
            ResultSemantics::Ordered,
        )
        .is_err());
        assert!(compare_rows(
            &[row([("value", Value::Float(0.0))])],
            &[row([("value", Value::Float(-0.0))])],
            ResultSemantics::Ordered,
        )
        .is_err());
        let first_nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0001));
        let second_nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0002));
        assert!(compare_rows(
            &[row([("value", first_nan)])],
            &[row([("value", second_nan)])],
            ResultSemantics::Ordered,
        )
        .is_err());
    }

    #[test]
    fn campaign_covers_every_mem_template_and_both_optimizer_paths() {
        let report = run_campaign(CampaignOptions {
            seed: 7,
            case_count: TEMPLATE_COUNT,
        })
        .unwrap();

        assert!(report.success(), "{}", report.json());
        assert!(report.complete_template_coverage);
        assert_eq!(report.executed_case_count, TEMPLATE_COUNT);
        assert_eq!(report.failed_case_count, 0);
        assert!(report
            .cases
            .iter()
            .all(|case| case.memo_plan_fingerprint.is_some()));
        assert!(report
            .cases
            .iter()
            .all(|case| case.direct_fallback_plan_fingerprint.is_some()));
        assert!(report.cases.iter().any(|case| case.index_enabled));
        assert!(report.cases.iter().any(|case| !case.index_enabled));
    }

    #[test]
    fn replay_bundle_retains_typed_parameters() {
        let mut generator = MemCaseGenerator::new(9);
        let case = generator.case(2);
        let replay = ReplayBundle::from_case(&case).json();

        assert_eq!(replay["protocol"], REPLAY_BUNDLE_PROTOCOL);
        assert_eq!(replay["template"], "in_filter");
        assert_eq!(replay["query"]["parameters"]["ids"]["type"], "list");
    }

    #[test]
    fn invalid_generated_case_fails_closed_with_replay() {
        let mut generator = MemCaseGenerator::new(11);
        let mut case = generator.case(0);
        case.mutations.push(Mutation::new("CREATE invalid"));

        let OracleResult::Failure(failure) = PlanDifferentialOracle.evaluate(&case) else {
            panic!("invalid generated case must fail closed");
        };
        assert!(failure.reason.contains("memo=mutation/parse"));
        assert_eq!(failure.replay.seed, case.seed);
        assert_eq!(
            failure.replay.mutations.last().unwrap().cypher,
            "CREATE invalid"
        );
    }

    #[test]
    fn campaign_is_reproducible_from_seed() {
        let options = CampaignOptions {
            seed: 17,
            case_count: 3,
        };

        assert_eq!(
            run_campaign(options).unwrap(),
            run_campaign(options).unwrap()
        );
    }

    #[test]
    fn campaign_rejects_unbounded_or_empty_runs() {
        assert_eq!(
            run_campaign(CampaignOptions {
                seed: 1,
                case_count: 0,
            })
            .unwrap_err()
            .to_string(),
            "case_count must be greater than zero"
        );
        assert!(run_campaign(CampaignOptions {
            seed: 1,
            case_count: MAX_CASE_COUNT + 1,
        })
        .is_err());
    }
}
