use crate::{
    compare_rows, error_class, row_json, typed_value_json, ExecutionOutcome, ResultSemantics,
};
use serde_json::{json, Value as JsonValue};
use skein::api::{Database, DatabaseReadTransaction};
use skein::{SkeinError, Value};

mod generator;

use generator::generate_sql_case;

pub const SQL_TLP_PROTOCOL: &str = "skein-sql-tlp-fuzz-v1";
pub const SQL_TLP_AGGREGATE_PROTOCOL: &str = "skein-sql-tlp-aggregate-fuzz-v1";
pub const SQL_REPLAY_PROTOCOL: &str = "skein-sql-fuzz-replay-v1";

const MAX_SQL_REDUCTION_ATTEMPTS: usize = 64;
pub(crate) const SQL_QUERY_SHAPES: [&str; 8] = [
    "nullable_score_range",
    "nullable_tag_equality",
    "inner_join_nullable_priority",
    "left_join_nullable_priority",
    "nullable_score_in_list",
    "nullable_column_comparison",
    "nullable_conjunction",
    "nullable_disjunction",
];
const SQL_QUERY_SHAPE_COUNT: usize = SQL_QUERY_SHAPES.len();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlMutation {
    pub sql: String,
    pub parameters: Vec<Value>,
    reducible: bool,
}

impl SqlMutation {
    fn required(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
            reducible: false,
        }
    }

    fn data(sql: impl Into<String>, parameters: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            parameters,
            reducible: true,
        }
    }

    fn index(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
            reducible: true,
        }
    }

    fn json(&self) -> JsonValue {
        json!({
            "sql": self.sql,
            "parameters": values_json(&self.parameters),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlQueryInvocation {
    pub sql: String,
    pub parameters: Vec<Value>,
    pub result_semantics: ResultSemantics,
}

impl SqlQueryInvocation {
    fn json(&self) -> JsonValue {
        json!({
            "sql": self.sql,
            "parameters": values_json(&self.parameters),
            "result_semantics": self.result_semantics.as_str(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTlpCase {
    pub name: String,
    pub original: SqlQueryInvocation,
    pub predicate_true: SqlQueryInvocation,
    pub predicate_false: SqlQueryInvocation,
    pub predicate_null: SqlQueryInvocation,
}

impl SqlTlpCase {
    fn json(&self) -> JsonValue {
        json!({
            "name": self.name,
            "original": self.original.json(),
            "predicate_true": self.predicate_true.json(),
            "predicate_false": self.predicate_false.json(),
            "predicate_null": self.predicate_null.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlFuzzCase {
    seed: u64,
    shape: String,
    setup: Vec<SqlMutation>,
    row_tlp: SqlTlpCase,
    aggregate_tlp: SqlTlpCase,
    index_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlReplayBundle {
    pub seed: u64,
    pub shape: String,
    pub setup: Vec<SqlMutation>,
    pub row_tlp: SqlTlpCase,
    pub aggregate_tlp: SqlTlpCase,
    pub index_enabled: bool,
}

impl SqlReplayBundle {
    fn from_case(case: &SqlFuzzCase) -> Self {
        Self {
            seed: case.seed,
            shape: case.shape.clone(),
            setup: case.setup.clone(),
            row_tlp: case.row_tlp.clone(),
            aggregate_tlp: case.aggregate_tlp.clone(),
            index_enabled: case.index_enabled,
        }
    }

    pub fn json(&self) -> JsonValue {
        json!({
            "protocol": SQL_REPLAY_PROTOCOL,
            "seed": self.seed,
            "shape": self.shape,
            "index_enabled": self.index_enabled,
            "setup": self.setup.iter().map(SqlMutation::json).collect::<Vec<_>>(),
            "row_tlp": self.row_tlp.json(),
            "aggregate_tlp": self.aggregate_tlp.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExecutionObservation {
    pub snapshot_epoch: Option<u64>,
    pub plan: Option<Vec<skein::executor::Row>>,
    pub outcome: ExecutionOutcome,
}

impl SqlExecutionObservation {
    fn json(&self) -> JsonValue {
        json!({
            "snapshot_epoch": self.snapshot_epoch,
            "plan": self.plan.as_ref().map(|rows| rows.iter().map(row_json).collect::<Vec<_>>()),
            "outcome": self.outcome.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTlpEvidence {
    pub original: SqlExecutionObservation,
    pub predicate_true: SqlExecutionObservation,
    pub predicate_false: SqlExecutionObservation,
    pub predicate_null: SqlExecutionObservation,
}

impl SqlTlpEvidence {
    fn observations(&self) -> [(&'static str, &SqlExecutionObservation); 4] {
        [
            ("original", &self.original),
            ("predicate_true", &self.predicate_true),
            ("predicate_false", &self.predicate_false),
            ("predicate_null", &self.predicate_null),
        ]
    }

    fn json(&self) -> JsonValue {
        json!({
            "original": self.original.json(),
            "predicate_true": self.predicate_true.json(),
            "predicate_false": self.predicate_false.json(),
            "predicate_null": self.predicate_null.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlReductionReport {
    pub oracle: &'static str,
    pub original_setup_count: usize,
    pub reduced_setup_count: usize,
    pub attempts: usize,
    pub replay: SqlReplayBundle,
}

impl SqlReductionReport {
    fn json(&self) -> JsonValue {
        json!({
            "oracle": self.oracle,
            "original_setup_count": self.original_setup_count,
            "reduced_setup_count": self.reduced_setup_count,
            "attempts": self.attempts,
            "replay": self.replay.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFailureReport {
    pub signature: String,
    pub reason: String,
    pub replay: SqlReplayBundle,
    pub reduction: SqlReductionReport,
    pub evidence: SqlTlpEvidence,
}

impl SqlFailureReport {
    fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "reason": self.reason,
            "replay": self.replay.json(),
            "reduction": self.reduction.json(),
            "evidence": self.evidence.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCaseReport {
    pub shape: String,
    pub index_enabled: bool,
    pub success: bool,
    pub row_tlp_success: bool,
    pub aggregate_tlp_success: bool,
    pub row_tlp_failure: Option<SqlFailureReport>,
    pub aggregate_tlp_failure: Option<SqlFailureReport>,
}

pub(crate) fn sql_capability_profile_json(aggregate: bool) -> JsonValue {
    json!({
        "shapes": SQL_QUERY_SHAPES,
        "aggregate": aggregate,
        "parameterized": true,
        "nullable_predicates": true,
        "in_list": true,
        "column_comparison": true,
        "boolean_composition": true,
        "inner_join": true,
        "left_join": true,
        "optional_indexes": true,
        "result_semantics": "bag",
    })
}

impl SqlCaseReport {
    pub(crate) fn json(&self) -> JsonValue {
        json!({
            "shape": self.shape,
            "index_enabled": self.index_enabled,
            "success": self.success,
            "row_tlp_success": self.row_tlp_success,
            "aggregate_tlp_success": self.aggregate_tlp_success,
            "row_tlp_failure": self.row_tlp_failure.as_ref().map(SqlFailureReport::json),
            "aggregate_tlp_failure": self.aggregate_tlp_failure.as_ref().map(SqlFailureReport::json),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlOracleKind {
    RowTlp,
    AggregateTlp,
}

impl SqlOracleKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RowTlp => "sql_tlp",
            Self::AggregateTlp => "sql_tlp_aggregate",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlFailureSignature {
    Errored {
        oracle: SqlOracleKind,
        variant: &'static str,
        phase: &'static str,
        class: &'static str,
    },
    SnapshotMismatch {
        oracle: SqlOracleKind,
    },
    RowPartitionMismatch,
    AggregateInvalidResult {
        variant: &'static str,
    },
    AggregateOverflow,
    AggregatePartitionMismatch,
}

impl SqlFailureSignature {
    fn code(&self) -> String {
        match self {
            Self::Errored {
                oracle,
                variant,
                phase,
                class,
            } => format!("{}_{variant}_{phase}_{class}", oracle.as_str()),
            Self::SnapshotMismatch { oracle } => format!("{}_snapshot_mismatch", oracle.as_str()),
            Self::RowPartitionMismatch => "sql_tlp_partition_mismatch".to_string(),
            Self::AggregateInvalidResult { variant } => {
                format!("sql_tlp_aggregate_{variant}_invalid_result")
            }
            Self::AggregateOverflow => "sql_tlp_aggregate_overflow".to_string(),
            Self::AggregatePartitionMismatch => "sql_tlp_aggregate_partition_mismatch".to_string(),
        }
    }
}

#[derive(Debug)]
struct DetectedSqlFailure {
    signature: SqlFailureSignature,
    reason: String,
}

pub(crate) fn evaluate_sql_case(seed: u64, index: usize, index_enabled: bool) -> SqlCaseReport {
    let case = generate_sql_case(seed, index, index_enabled);
    let (row_evidence, aggregate_evidence) = execute_sql_case(&case);
    let row_failure = classify_sql_row_tlp_failure(&row_evidence);
    let aggregate_failure = classify_sql_aggregate_tlp_failure(&aggregate_evidence);
    let row_tlp_success = row_failure.is_none();
    let aggregate_tlp_success = aggregate_failure.is_none();

    SqlCaseReport {
        shape: case.shape.clone(),
        index_enabled,
        success: row_tlp_success && aggregate_tlp_success,
        row_tlp_success,
        aggregate_tlp_success,
        row_tlp_failure: row_failure
            .map(|failure| failure_report(&case, SqlOracleKind::RowTlp, failure, row_evidence)),
        aggregate_tlp_failure: aggregate_failure.map(|failure| {
            failure_report(
                &case,
                SqlOracleKind::AggregateTlp,
                failure,
                aggregate_evidence,
            )
        }),
    }
}

fn failure_report(
    case: &SqlFuzzCase,
    oracle: SqlOracleKind,
    failure: DetectedSqlFailure,
    evidence: SqlTlpEvidence,
) -> SqlFailureReport {
    SqlFailureReport {
        signature: failure.signature.code(),
        reason: failure.reason,
        replay: SqlReplayBundle::from_case(case),
        reduction: reduce_sql_failure(case, oracle, &failure.signature),
        evidence,
    }
}

fn execute_sql_case(case: &SqlFuzzCase) -> (SqlTlpEvidence, SqlTlpEvidence) {
    let (snapshot, snapshot_epoch) = match prepare_sql_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => {
            let evidence = repeated_evidence(observation);
            return (evidence.clone(), evidence);
        }
    };
    (
        execute_sql_tlp_queries(&snapshot, snapshot_epoch, &case.row_tlp),
        execute_sql_tlp_queries(&snapshot, snapshot_epoch, &case.aggregate_tlp),
    )
}

fn execute_sql_oracle(case: &SqlFuzzCase, oracle: SqlOracleKind) -> SqlTlpEvidence {
    let (snapshot, snapshot_epoch) = match prepare_sql_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => return repeated_evidence(observation),
    };
    let queries = match oracle {
        SqlOracleKind::RowTlp => &case.row_tlp,
        SqlOracleKind::AggregateTlp => &case.aggregate_tlp,
    };
    execute_sql_tlp_queries(&snapshot, snapshot_epoch, queries)
}

fn prepare_sql_case(
    case: &SqlFuzzCase,
) -> Result<(DatabaseReadTransaction, u64), SqlExecutionObservation> {
    let mut database = Database::new();
    for mutation in &case.setup {
        if let Err(error) = database.query_sql_with_params(&mutation.sql, &mutation.parameters) {
            return Err(sql_error_observation("setup", error));
        }
    }
    let snapshot_epoch = database.commit_epoch();
    Ok((database.begin_read_transaction(), snapshot_epoch))
}

fn execute_sql_tlp_queries(
    snapshot: &DatabaseReadTransaction,
    snapshot_epoch: u64,
    queries: &SqlTlpCase,
) -> SqlTlpEvidence {
    let execute = |query: &SqlQueryInvocation| {
        let explain_sql = format!("EXPLAIN {}", query.sql);
        let plan = match snapshot.query_sql_with_params(&explain_sql, &query.parameters) {
            Ok(output) => Some(output.rows),
            Err(error) => {
                return SqlExecutionObservation {
                    snapshot_epoch: Some(snapshot_epoch),
                    plan: None,
                    outcome: sql_error_outcome("explain", error),
                };
            }
        };
        match snapshot.query_sql_with_params(&query.sql, &query.parameters) {
            Ok(output) => SqlExecutionObservation {
                snapshot_epoch: Some(snapshot_epoch),
                plan,
                outcome: ExecutionOutcome::Rows(output.rows),
            },
            Err(error) => SqlExecutionObservation {
                snapshot_epoch: Some(snapshot_epoch),
                plan,
                outcome: sql_error_outcome("execute", error),
            },
        }
    };
    SqlTlpEvidence {
        original: execute(&queries.original),
        predicate_true: execute(&queries.predicate_true),
        predicate_false: execute(&queries.predicate_false),
        predicate_null: execute(&queries.predicate_null),
    }
}

fn repeated_evidence(observation: SqlExecutionObservation) -> SqlTlpEvidence {
    SqlTlpEvidence {
        original: observation.clone(),
        predicate_true: observation.clone(),
        predicate_false: observation.clone(),
        predicate_null: observation,
    }
}

fn classify_sql_row_tlp_failure(evidence: &SqlTlpEvidence) -> Option<DetectedSqlFailure> {
    if let Some(failure) = classify_sql_common_failure(evidence, SqlOracleKind::RowTlp) {
        return Some(failure);
    }
    let ExecutionOutcome::Rows(original) = &evidence.original.outcome else {
        unreachable!("SQL row TLP errors were classified above")
    };
    let mut partitioned = Vec::new();
    for (_, observation) in evidence.observations().into_iter().skip(1) {
        let ExecutionOutcome::Rows(rows) = &observation.outcome else {
            unreachable!("SQL row TLP errors were classified above")
        };
        partitioned.extend_from_slice(rows);
    }
    compare_rows(original, &partitioned, ResultSemantics::Bag)
        .err()
        .map(|reason| DetectedSqlFailure {
            signature: SqlFailureSignature::RowPartitionMismatch,
            reason: format!("SQL TLP partition mismatch: {reason}"),
        })
}

fn classify_sql_aggregate_tlp_failure(evidence: &SqlTlpEvidence) -> Option<DetectedSqlFailure> {
    if let Some(failure) = classify_sql_common_failure(evidence, SqlOracleKind::AggregateTlp) {
        return Some(failure);
    }
    let mut counts = [0_u64; 4];
    for (index, (variant, observation)) in evidence.observations().into_iter().enumerate() {
        let Some(count) = sql_observation_count(observation) else {
            return Some(DetectedSqlFailure {
                signature: SqlFailureSignature::AggregateInvalidResult { variant },
                reason: format!(
                    "SQL TLP aggregate {variant} must return one non-negative integer count"
                ),
            });
        };
        counts[index] = count;
    }
    let Some(partition_count) = counts[1]
        .checked_add(counts[2])
        .and_then(|count| count.checked_add(counts[3]))
    else {
        return Some(DetectedSqlFailure {
            signature: SqlFailureSignature::AggregateOverflow,
            reason: "SQL TLP aggregate partition count overflowed u64".to_string(),
        });
    };
    (counts[0] != partition_count).then(|| DetectedSqlFailure {
        signature: SqlFailureSignature::AggregatePartitionMismatch,
        reason: format!(
            "SQL TLP aggregate mismatch: original_count={} partition_count={partition_count}",
            counts[0]
        ),
    })
}

fn classify_sql_common_failure(
    evidence: &SqlTlpEvidence,
    oracle: SqlOracleKind,
) -> Option<DetectedSqlFailure> {
    let observations = evidence.observations();
    for (variant, observation) in observations {
        if let ExecutionOutcome::Error { phase, class, .. } = &observation.outcome {
            let phase = *phase;
            let class = *class;
            return Some(DetectedSqlFailure {
                signature: SqlFailureSignature::Errored {
                    oracle,
                    variant,
                    phase,
                    class,
                },
                reason: format!("{} {variant} failed in {phase}/{class}", oracle.as_str()),
            });
        }
    }
    let snapshot_epoch = evidence.original.snapshot_epoch;
    if snapshot_epoch.is_none()
        || observations
            .iter()
            .any(|(_, observation)| observation.snapshot_epoch != snapshot_epoch)
    {
        return Some(DetectedSqlFailure {
            signature: SqlFailureSignature::SnapshotMismatch { oracle },
            reason: format!(
                "{} variants did not use one pinned snapshot",
                oracle.as_str()
            ),
        });
    }
    None
}

fn sql_observation_count(observation: &SqlExecutionObservation) -> Option<u64> {
    let ExecutionOutcome::Rows(rows) = &observation.outcome else {
        return None;
    };
    let [row] = rows.as_slice() else {
        return None;
    };
    if row.len() != 1 {
        return None;
    }
    let Value::Int(count) = row.get("count")? else {
        return None;
    };
    u64::try_from(*count).ok()
}

fn reduce_sql_failure(
    case: &SqlFuzzCase,
    oracle: SqlOracleKind,
    expected: &SqlFailureSignature,
) -> SqlReductionReport {
    let original_setup_count = case.setup.len();
    let mut reduced = case.clone();
    let mut attempts = 0;

    loop {
        let mut accepted = None;
        for index in 0..reduced.setup.len() {
            if attempts >= MAX_SQL_REDUCTION_ATTEMPTS {
                break;
            }
            if !reduced.setup[index].reducible {
                continue;
            }
            let mut candidate = reduced.clone();
            candidate.setup.remove(index);
            candidate.index_enabled = candidate
                .setup
                .iter()
                .any(|mutation| mutation.sql.starts_with("CREATE INDEX"));
            attempts += 1;
            if sql_failure_signature(&candidate, oracle).as_ref() == Some(expected) {
                accepted = Some(candidate);
                break;
            }
        }
        let Some(candidate) = accepted else {
            break;
        };
        reduced = candidate;
    }

    SqlReductionReport {
        oracle: oracle.as_str(),
        original_setup_count,
        reduced_setup_count: reduced.setup.len(),
        attempts,
        replay: SqlReplayBundle::from_case(&reduced),
    }
}

fn sql_failure_signature(case: &SqlFuzzCase, oracle: SqlOracleKind) -> Option<SqlFailureSignature> {
    let evidence = execute_sql_oracle(case, oracle);
    match oracle {
        SqlOracleKind::RowTlp => classify_sql_row_tlp_failure(&evidence),
        SqlOracleKind::AggregateTlp => classify_sql_aggregate_tlp_failure(&evidence),
    }
    .map(|failure| failure.signature)
}

fn sql_error_observation(phase: &'static str, error: SkeinError) -> SqlExecutionObservation {
    SqlExecutionObservation {
        snapshot_epoch: None,
        plan: None,
        outcome: sql_error_outcome(phase, error),
    }
}

fn sql_error_outcome(phase: &'static str, error: SkeinError) -> ExecutionOutcome {
    ExecutionOutcome::Error {
        phase,
        class: error_class(&error),
        message: error.to_string(),
    }
}

fn values_json(values: &[Value]) -> Vec<JsonValue> {
    values.iter().map(typed_value_json).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_sql_cases_cover_all_shapes_and_oracles() {
        let mut observed = std::collections::BTreeSet::new();
        for index in 0..SQL_QUERY_SHAPE_COUNT {
            let case = generate_sql_case(17 + index as u64, index, index.is_multiple_of(2));
            observed.insert(case.shape.clone());
            let (row, aggregate) = execute_sql_case(&case);
            assert!(
                classify_sql_row_tlp_failure(&row).is_none(),
                "{}",
                SqlReplayBundle::from_case(&case).json()
            );
            assert!(
                classify_sql_aggregate_tlp_failure(&aggregate).is_none(),
                "{}",
                SqlReplayBundle::from_case(&case).json()
            );
            assert!(row
                .observations()
                .iter()
                .all(|(_, observation)| observation.plan.is_some()));
            assert!(aggregate
                .observations()
                .iter()
                .all(|(_, observation)| observation.plan.is_some()));
            let ExecutionOutcome::Rows(null_rows) = &row.predicate_null.outcome else {
                panic!("{} null partition did not return rows", case.shape);
            };
            assert!(
                !null_rows.is_empty(),
                "{} null partition was empty",
                case.shape
            );
            assert!(
                sql_observation_count(&aggregate.predicate_null).is_some_and(|count| count > 0),
                "{} aggregate null partition was empty",
                case.shape
            );
        }
        assert_eq!(observed.len(), SQL_QUERY_SHAPE_COUNT);
    }

    #[test]
    fn sql_tlp_detects_and_reduces_invalid_partition() {
        let mut case = generate_sql_case(17, 0, true);
        case.row_tlp.predicate_null = case.row_tlp.original.clone();
        let evidence = execute_sql_oracle(&case, SqlOracleKind::RowTlp);
        let failure = classify_sql_row_tlp_failure(&evidence).unwrap();

        assert_eq!(failure.signature, SqlFailureSignature::RowPartitionMismatch);
        let reduction = reduce_sql_failure(&case, SqlOracleKind::RowTlp, &failure.signature);
        assert!(reduction.reduced_setup_count < reduction.original_setup_count);
        assert_eq!(reduction.oracle, "sql_tlp");
    }

    #[test]
    fn sql_aggregate_tlp_detects_and_reduces_invalid_partition() {
        let mut case = generate_sql_case(19, 3, false);
        case.aggregate_tlp.predicate_null = case.aggregate_tlp.original.clone();
        let evidence = execute_sql_oracle(&case, SqlOracleKind::AggregateTlp);
        let failure = classify_sql_aggregate_tlp_failure(&evidence).unwrap();

        assert_eq!(
            failure.signature,
            SqlFailureSignature::AggregatePartitionMismatch
        );
        let reduction = reduce_sql_failure(&case, SqlOracleKind::AggregateTlp, &failure.signature);
        assert!(reduction.reduced_setup_count < reduction.original_setup_count);
        assert_eq!(reduction.oracle, "sql_tlp_aggregate");
    }

    #[test]
    fn sql_setup_errors_fail_closed_with_replay() {
        let mut case = generate_sql_case(23, 1, false);
        case.setup
            .push(SqlMutation::data("INSERT invalid", Vec::new()));
        let evidence = execute_sql_oracle(&case, SqlOracleKind::RowTlp);
        let failure = classify_sql_row_tlp_failure(&evidence).unwrap();
        assert!(matches!(
            failure.signature,
            SqlFailureSignature::Errored {
                phase: "setup",
                class: "parse",
                ..
            }
        ));
        let report = failure_report(&case, SqlOracleKind::RowTlp, failure, evidence);
        assert_eq!(report.replay.setup.last().unwrap().sql, "INSERT invalid");
        assert_eq!(
            report.reduction.replay.setup.last().unwrap().sql,
            "INSERT invalid"
        );
    }
}
