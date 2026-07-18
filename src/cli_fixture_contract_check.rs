use skein::{Result, SkeinError};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
const COMMAND_WAIT_POLL_MS: u64 = 10;

#[derive(Debug, Clone)]
pub struct FixtureContractCommandCheckOptions {
    pub max_checks: Option<usize>,
    pub command_timeout: Duration,
    pub allow_primary_only_project_graph: bool,
}

impl Default for FixtureContractCommandCheckOptions {
    fn default() -> Self {
        Self {
            max_checks: None,
            command_timeout: Duration::from_millis(DEFAULT_COMMAND_TIMEOUT_MS),
            allow_primary_only_project_graph: false,
        }
    }
}

pub fn nowledge_fixture_contract_command_check_usage() -> String {
    "nowledge-fixture-contract-command-check requires [--max-checks <n>] [--command-timeout-ms <ms>] [--allow-primary-only-project-graph] <contract-json> <program> [args...]".to_string()
}

pub fn run_nowledge_fixture_contract_command_check(
    mut args: impl Iterator<Item = String>,
) -> Result<serde_json::Value> {
    let mut options = FixtureContractCommandCheckOptions::default();
    let mut positional = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--max-checks" => {
                let raw = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_fixture_contract_command_check_usage())
                })?;
                options.max_checks = Some(parse_positive_usize("--max-checks", &raw)?);
            }
            "--command-timeout-ms" => {
                let raw = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_fixture_contract_command_check_usage())
                })?;
                options.command_timeout =
                    Duration::from_millis(parse_positive_u64("--command-timeout-ms", &raw)?);
            }
            "--allow-primary-only-project-graph" => {
                options.allow_primary_only_project_graph = true;
            }
            _ => {
                positional.push(arg);
                positional.extend(args);
                break;
            }
        }
    }

    let contract_path = positional
        .first()
        .ok_or_else(|| SkeinError::Semantic(nowledge_fixture_contract_command_check_usage()))?;
    let program = positional
        .get(1)
        .ok_or_else(|| SkeinError::Semantic(nowledge_fixture_contract_command_check_usage()))?;
    let command_args = positional.iter().skip(2).cloned().collect::<Vec<_>>();
    let contract = read_contract(contract_path)?;
    check_contract_command(&contract, program, &command_args, &options)
}

fn read_contract(path: &str) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!("failed to read fixture contract '{path}': {error}"))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to parse fixture contract '{path}': {error}"
        ))
    })
}

fn check_contract_command(
    contract: &serde_json::Value,
    program: &str,
    command_args: &[String],
    options: &FixtureContractCommandCheckOptions,
) -> Result<serde_json::Value> {
    if contract.get("protocol").and_then(serde_json::Value::as_str)
        != Some("skein-nowledge-fixture-contract")
    {
        return Err(SkeinError::Semantic(
            "fixture contract protocol must be skein-nowledge-fixture-contract".to_string(),
        ));
    }
    let command = FixtureCommand {
        program,
        args: command_args,
        timeout: options.command_timeout,
    };
    let mut failures = Vec::new();
    let mut matched_checks = 0usize;
    let mut primary_only_project_graph_checks = Vec::new();

    for statement in contract
        .get("setup")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| SkeinError::Semantic("fixture contract missing setup array".to_string()))?
    {
        if let Err(error) = command.invoke(statement_request(statement)) {
            failures.push(failure_json("fixture_setup", statement, error.to_string()));
            return Ok(command_check_report_json(
                contract,
                options,
                0,
                matched_checks,
                &primary_only_project_graph_checks,
                failures,
            ));
        }
    }

    let checks = contract
        .get("checks")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| SkeinError::Semantic("fixture contract missing checks array".to_string()))?;
    let check_limit = options.max_checks.unwrap_or(checks.len()).min(checks.len());
    for check in checks.iter().take(check_limit) {
        match check.get("kind").and_then(serde_json::Value::as_str) {
            Some("cypher") => match check_cypher_contract_check(&command, check) {
                Ok(()) => matched_checks += 1,
                Err(error) => failures.push(failure_json("check", check, error.to_string())),
            },
            Some("projected_graph") => match check_project_graph_contract_check(&command, check) {
                Ok(ProjectGraphCheckOutcome::Matched) => matched_checks += 1,
                Ok(ProjectGraphCheckOutcome::PrimaryOnly(reason)) => {
                    let name = check
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("<unnamed>");
                    primary_only_project_graph_checks.push(serde_json::json!({
                        "name": name,
                        "reason": reason,
                    }));
                    if !options.allow_primary_only_project_graph {
                        failures.push(failure_json(
                            "check",
                            check,
                            "project_graph returned primary_only".to_string(),
                        ));
                    }
                }
                Err(error) => failures.push(failure_json("check", check, error.to_string())),
            },
            Some(kind) => failures.push(failure_json(
                "check",
                check,
                format!("unsupported fixture contract check kind '{kind}'"),
            )),
            None => failures.push(failure_json(
                "check",
                check,
                "fixture contract check missing kind".to_string(),
            )),
        }
    }

    Ok(command_check_report_json(
        contract,
        options,
        check_limit,
        matched_checks,
        &primary_only_project_graph_checks,
        failures,
    ))
}

fn check_cypher_contract_check(
    command: &FixtureCommand<'_>,
    check: &serde_json::Value,
) -> Result<()> {
    let mut session_statements = Vec::new();
    for setup in check
        .get("setup")
        .and_then(serde_json::Value::as_array)
        .unwrap_or(&Vec::new())
    {
        let request = statement_request(setup);
        if is_session_check(check) {
            session_statements.push(request);
        } else {
            command.invoke(request)?;
        }
    }

    let statement = check
        .get("statement")
        .ok_or_else(|| SkeinError::Semantic("cypher check missing statement".to_string()))?;
    let output = if is_session_check(check) {
        session_statements.push(statement_request(statement));
        let session = serde_json::json!({
            "op": "execute_session",
            "statements": session_statements,
        });
        let reply = command.invoke(session)?;
        session_last_rows(&reply)?
    } else {
        command.invoke(statement_request(statement))?
    };
    expected_rows_matches(check.get("expected_rows"), &output)?;

    if let Some(effect) = check.get("effect").filter(|value| !value.is_null()) {
        let statement = effect
            .get("statement")
            .ok_or_else(|| SkeinError::Semantic("cypher effect missing statement".to_string()))?;
        let output = command.invoke(statement_request(statement))?;
        expected_rows_matches(effect.get("expected_rows"), &output)?;
    }
    Ok(())
}

enum ProjectGraphCheckOutcome {
    Matched,
    PrimaryOnly(Option<String>),
}

fn check_project_graph_contract_check(
    command: &FixtureCommand<'_>,
    check: &serde_json::Value,
) -> Result<ProjectGraphCheckOutcome> {
    let request = check
        .get("request")
        .cloned()
        .ok_or_else(|| SkeinError::Semantic("projected_graph check missing request".to_string()))?;
    let reply = command.invoke(request)?;
    if reply
        .get("primary_only")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(ProjectGraphCheckOutcome::PrimaryOnly(
            reply
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        ));
    }
    let actual = reply.get("ok").unwrap_or(&reply);
    let expected = check.get("expected_projected_graph").ok_or_else(|| {
        SkeinError::Semantic("projected_graph check missing expected payload".to_string())
    })?;
    for key in [
        "node_count",
        "edge_count",
        "incoming",
        "communities",
        "hierarchical_communities",
        "page_rank_top_node",
    ] {
        if actual.get(key) != expected.get(key) {
            return Err(SkeinError::Execution(format!(
                "project_graph mismatch for '{key}': expected {}, got {}",
                json_debug(expected.get(key)),
                json_debug(actual.get(key))
            )));
        }
    }
    Ok(ProjectGraphCheckOutcome::Matched)
}

fn statement_request(statement: &serde_json::Value) -> serde_json::Value {
    statement
        .get("command_request")
        .cloned()
        .unwrap_or_else(|| {
            serde_json::json!({
                "op": "query",
                "cypher": statement.get("cypher").cloned().unwrap_or(serde_json::Value::Null),
                "parameters": statement.get("parameters").cloned().unwrap_or_else(|| serde_json::json!({})),
            })
        })
}

fn is_session_check(check: &serde_json::Value) -> bool {
    check
        .get("execution_mode")
        .and_then(serde_json::Value::as_str)
        == Some("session")
}

fn session_last_rows(reply: &serde_json::Value) -> Result<serde_json::Value> {
    let results = reply
        .get("results")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution("execute_session reply missing results".to_string())
        })?;
    let last = results
        .last()
        .ok_or_else(|| SkeinError::Execution("execute_session reply had no results".to_string()))?;
    if last.get("rows").is_some() {
        Ok(last.clone())
    } else {
        Ok(serde_json::json!({ "rows": last }))
    }
}

fn expected_rows_matches(
    expected_rows: Option<&serde_json::Value>,
    output: &serde_json::Value,
) -> Result<()> {
    let expected_rows = expected_rows
        .ok_or_else(|| SkeinError::Semantic("contract check missing expected_rows".to_string()))?;
    let actual = output
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| SkeinError::Execution("command reply missing rows array".to_string()))?;
    match expected_rows
        .get("kind")
        .and_then(serde_json::Value::as_str)
    {
        Some("row_count") => {
            let expected = expected_rows
                .get("count")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    SkeinError::Semantic("row_count expected_rows missing count".to_string())
                })?;
            if actual.len() as u64 != expected {
                return Err(SkeinError::Execution(format!(
                    "row count mismatch: expected {expected}, got {}",
                    actual.len()
                )));
            }
        }
        Some("exact") => {
            let expected = expected_rows
                .get("rows")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    SkeinError::Semantic("exact expected_rows missing rows".to_string())
                })?;
            if actual != expected {
                return Err(SkeinError::Execution(format!(
                    "ordered row mismatch: expected {}, got {}",
                    serde_json::Value::Array(expected.clone()),
                    serde_json::Value::Array(actual.clone())
                )));
            }
        }
        Some("unordered") => {
            let expected = expected_rows
                .get("rows")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    SkeinError::Semantic("unordered expected_rows missing rows".to_string())
                })?;
            let mut expected = expected.iter().map(json_sort_key).collect::<Vec<_>>();
            let mut actual = actual.iter().map(json_sort_key).collect::<Vec<_>>();
            expected.sort();
            actual.sort();
            if actual != expected {
                return Err(SkeinError::Execution("unordered row mismatch".to_string()));
            }
        }
        Some(kind) => {
            return Err(SkeinError::Semantic(format!(
                "unsupported expected_rows kind '{kind}'"
            )))
        }
        None => {
            return Err(SkeinError::Semantic(
                "expected_rows missing kind".to_string(),
            ))
        }
    }
    Ok(())
}

struct FixtureCommand<'a> {
    program: &'a str,
    args: &'a [String],
    timeout: Duration,
}

impl FixtureCommand<'_> {
    fn invoke(&self, request: serde_json::Value) -> Result<serde_json::Value> {
        let mut child = Command::new(self.program)
            .args(self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "failed to spawn fixture contract command '{}': {error}",
                    self.program
                ))
            })?;
        {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                SkeinError::Execution("fixture contract command stdin is not available".to_string())
            })?;
            use std::io::Write;
            writeln!(stdin, "{request}").map_err(|error| {
                SkeinError::Execution(format!(
                    "failed to write fixture contract command request: {error}"
                ))
            })?;
        }
        let output = self.wait_for_output(child)?;
        if !output.status.success() {
            return Err(SkeinError::Execution(format!(
                "fixture contract command exited with {}; stderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        serde_json::from_slice(&output.stdout).map_err(|error| {
            SkeinError::Execution(format!(
                "fixture contract command returned invalid JSON: {error}; stdout: {}",
                String::from_utf8_lossy(&output.stdout).trim()
            ))
        })
    }

    fn wait_for_output(&self, mut child: std::process::Child) -> Result<Output> {
        let started_at = Instant::now();
        loop {
            match child.try_wait() {
                Ok(Some(_status)) => {
                    return child.wait_with_output().map_err(|error| {
                        SkeinError::Execution(format!(
                            "failed to collect fixture contract command output: {error}"
                        ))
                    });
                }
                Ok(None) if started_at.elapsed() >= self.timeout => {
                    let _ = child.kill();
                    let output = child.wait_with_output().map_err(|error| {
                        SkeinError::Execution(format!(
                            "fixture contract command timed out after {} ms and failed to collect output: {error}",
                            self.timeout.as_millis()
                        ))
                    })?;
                    return Err(SkeinError::Execution(format!(
                        "fixture contract command timed out after {} ms; stderr: {}",
                        self.timeout.as_millis(),
                        String::from_utf8_lossy(&output.stderr).trim()
                    )));
                }
                Ok(None) => thread::sleep(Duration::from_millis(COMMAND_WAIT_POLL_MS)),
                Err(error) => {
                    let _ = child.kill();
                    return Err(SkeinError::Execution(format!(
                        "failed to poll fixture contract command: {error}"
                    )));
                }
            }
        }
    }
}

fn command_check_report_json(
    contract: &serde_json::Value,
    options: &FixtureContractCommandCheckOptions,
    checked_checks: usize,
    matched_checks: usize,
    primary_only_project_graph_checks: &[serde_json::Value],
    failures: Vec<serde_json::Value>,
) -> serde_json::Value {
    let total_checks = contract
        .get("check_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    serde_json::json!({
        "protocol": "skein-nowledge-fixture-contract-command-check",
        "fixture": contract.get("fixture").cloned().unwrap_or(serde_json::Value::Null),
        "total_checks": total_checks,
        "checked_checks": checked_checks,
        "matched_checks": matched_checks,
        "failed_checks": failures.len(),
        "failures": failures,
        "primary_only_project_graph_checks": primary_only_project_graph_checks,
        "options": {
            "max_checks": options.max_checks,
            "command_timeout_ms": options.command_timeout.as_millis() as u64,
            "allow_primary_only_project_graph": options.allow_primary_only_project_graph,
        },
        "contract_command_check_ready": failures.is_empty() && matched_checks == checked_checks,
    })
}

fn failure_json(phase: &str, value: &serde_json::Value, message: String) -> serde_json::Value {
    serde_json::json!({
        "phase": phase,
        "name": value.get("name").cloned().unwrap_or(serde_json::Value::Null),
        "index": value.get("index").cloned().unwrap_or(serde_json::Value::Null),
        "message": message,
    })
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value.parse::<usize>().map_err(|error| {
        SkeinError::Semantic(format!("invalid {flag} value '{value}': {error}"))
    })?;
    if parsed == 0 {
        return Err(SkeinError::Semantic(format!(
            "{flag} must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn parse_positive_u64(flag: &str, value: &str) -> Result<u64> {
    let parsed = value.parse::<u64>().map_err(|error| {
        SkeinError::Semantic(format!("invalid {flag} value '{value}': {error}"))
    })?;
    if parsed == 0 {
        return Err(SkeinError::Semantic(format!(
            "{flag} must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn json_sort_key(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
}

fn json_debug(value: Option<&serde_json::Value>) -> serde_json::Value {
    value.cloned().unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::{check_contract_command, FixtureContractCommandCheckOptions};

    #[test]
    fn contract_command_check_reports_row_count_mismatch() {
        let contract = serde_json::json!({
            "protocol": "skein-nowledge-fixture-contract",
            "fixture": "mini",
            "check_count": 1,
            "setup": [],
            "checks": [
                {
                    "index": 0,
                    "kind": "cypher",
                    "name": "count mismatch",
                    "execution_mode": "database",
                    "setup": [],
                    "statement": {
                        "command_request": {
                            "op": "query",
                            "cypher": "MATCH (n) RETURN n",
                            "parameters": {}
                        }
                    },
                    "expected_rows": {
                        "kind": "row_count",
                        "count": 1
                    }
                }
            ]
        });
        let options = FixtureContractCommandCheckOptions::default();
        let report = check_contract_command(
            &contract,
            "python3",
            &[
                "-c".to_string(),
                "import json,sys; json.load(sys.stdin); print(json.dumps({'rows': []}))"
                    .to_string(),
            ],
            &options,
        )
        .unwrap();

        assert_eq!(report["matched_checks"], 0);
        assert_eq!(report["failed_checks"], 1);
        assert_eq!(report["contract_command_check_ready"], false);
    }
}
