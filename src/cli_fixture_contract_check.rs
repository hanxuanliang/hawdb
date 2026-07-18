use skein::{Result, SkeinError};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
const COMMAND_WAIT_POLL_MS: u64 = 10;

#[derive(Debug, Clone)]
pub struct FixtureContractCommandCheckOptions {
    pub max_checks: Option<usize>,
    pub start_check: usize,
    pub check_name: Option<String>,
    pub command_timeout: Duration,
    pub allow_primary_only_project_graph: bool,
    pub require_full_contract: bool,
    pub command_mode: FixtureCommandMode,
    pub stop_after_first_failure: bool,
}

impl Default for FixtureContractCommandCheckOptions {
    fn default() -> Self {
        Self {
            max_checks: None,
            start_check: 0,
            check_name: None,
            command_timeout: Duration::from_millis(DEFAULT_COMMAND_TIMEOUT_MS),
            allow_primary_only_project_graph: false,
            require_full_contract: false,
            command_mode: FixtureCommandMode::SpawnPerRequest,
            stop_after_first_failure: false,
        }
    }
}

pub fn nowledge_fixture_contract_command_check_usage() -> String {
    "nowledge-fixture-contract-command-check requires [--require-full-contract] [--stop-after-first-failure] [--start-check <zero-based-index>] [--check-name <name>] [--max-checks <n>] [--command-timeout-ms <ms>] [--allow-primary-only-project-graph] <contract-json> [--persistent-command] <program> [args...]".to_string()
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
            "--start-check" => {
                let raw = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_fixture_contract_command_check_usage())
                })?;
                options.start_check = parse_usize("--start-check", &raw)?;
            }
            "--check-name" => {
                options.check_name = Some(args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_fixture_contract_command_check_usage())
                })?);
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
            "--require-full-contract" => {
                options.require_full_contract = true;
            }
            "--stop-after-first-failure" => {
                options.stop_after_first_failure = true;
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
    let (program, command_args) = if program == "--persistent-command" {
        options.command_mode = FixtureCommandMode::Persistent;
        let program = positional
            .get(2)
            .ok_or_else(|| SkeinError::Semantic(nowledge_fixture_contract_command_check_usage()))?;
        (
            program.as_str(),
            positional.iter().skip(3).cloned().collect::<Vec<_>>(),
        )
    } else {
        (
            program.as_str(),
            positional.iter().skip(2).cloned().collect::<Vec<_>>(),
        )
    };
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
    let mut command = FixtureCommand::new(
        program,
        command_args,
        options.command_timeout,
        options.command_mode,
    )?;
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
                CommandCheckReportStats {
                    selected_checks: 0,
                    checked_checks: 0,
                    matched_checks,
                    stopped_after_first_failure: false,
                },
                &primary_only_project_graph_checks,
                failures,
            ));
        }
    }

    let checks = contract
        .get("checks")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| SkeinError::Semantic("fixture contract missing checks array".to_string()))?;
    let selected_checks = select_contract_checks(checks, options);
    let selected_check_count = selected_checks.len();
    if selected_check_count == 0 {
        failures.push(serde_json::json!({
            "phase": "selection",
            "code": "no_checks_selected",
            "name": serde_json::Value::Null,
            "index": serde_json::Value::Null,
            "message": "no fixture checks selected",
        }));
        return Ok(command_check_report_json(
            contract,
            options,
            CommandCheckReportStats {
                selected_checks: selected_check_count,
                checked_checks: 0,
                matched_checks,
                stopped_after_first_failure: false,
            },
            &primary_only_project_graph_checks,
            failures,
        ));
    }
    let check_limit = options
        .max_checks
        .unwrap_or(selected_check_count)
        .min(selected_check_count);
    let mut stopped_after_first_failure = false;
    let mut checked_checks = 0usize;
    for check in selected_checks.iter().take(check_limit) {
        checked_checks += 1;
        match check.get("kind").and_then(serde_json::Value::as_str) {
            Some("cypher") => match check_cypher_contract_check(&mut command, check) {
                Ok(()) => matched_checks += 1,
                Err(error) => push_check_failure(&mut failures, check, error.to_string()),
            },
            Some("projected_graph") => {
                match check_project_graph_contract_check(&mut command, check) {
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
                            push_check_failure(
                                &mut failures,
                                check,
                                "project_graph returned primary_only".to_string(),
                            );
                        }
                    }
                    Err(error) => push_check_failure(&mut failures, check, error.to_string()),
                }
            }
            Some(kind) => push_check_failure(
                &mut failures,
                check,
                format!("unsupported fixture contract check kind '{kind}'"),
            ),
            None => push_check_failure(
                &mut failures,
                check,
                "fixture contract check missing kind".to_string(),
            ),
        }
        if options.stop_after_first_failure && !failures.is_empty() {
            stopped_after_first_failure = true;
            break;
        }
    }

    Ok(command_check_report_json(
        contract,
        options,
        CommandCheckReportStats {
            selected_checks: selected_check_count,
            checked_checks,
            matched_checks,
            stopped_after_first_failure,
        },
        &primary_only_project_graph_checks,
        failures,
    ))
}

fn select_contract_checks<'a>(
    checks: &'a [serde_json::Value],
    options: &FixtureContractCommandCheckOptions,
) -> Vec<&'a serde_json::Value> {
    checks
        .iter()
        .enumerate()
        .filter(|(position, check)| {
            contract_check_index(check).unwrap_or(*position) >= options.start_check
        })
        .filter(|(_, check)| match options.check_name.as_deref() {
            Some(name) => check.get("name").and_then(serde_json::Value::as_str) == Some(name),
            None => true,
        })
        .map(|(_, check)| check)
        .collect()
}

fn contract_check_index(check: &serde_json::Value) -> Option<usize> {
    check
        .get("index")
        .and_then(serde_json::Value::as_u64)
        .and_then(|index| usize::try_from(index).ok())
}

fn check_cypher_contract_check(
    command: &mut FixtureCommand,
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
    command: &mut FixtureCommand,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixtureCommandMode {
    SpawnPerRequest,
    Persistent,
}

enum FixtureCommand {
    SpawnPerRequest(SpawnPerRequestFixtureCommand),
    Persistent(PersistentFixtureCommand),
}

impl FixtureCommand {
    fn new(
        program: &str,
        args: &[String],
        timeout: Duration,
        mode: FixtureCommandMode,
    ) -> Result<Self> {
        match mode {
            FixtureCommandMode::SpawnPerRequest => {
                Ok(Self::SpawnPerRequest(SpawnPerRequestFixtureCommand {
                    program: program.to_string(),
                    args: args.to_vec(),
                    timeout,
                }))
            }
            FixtureCommandMode::Persistent => Ok(Self::Persistent(
                PersistentFixtureCommand::spawn(program, args)?,
            )),
        }
    }

    fn invoke(&mut self, request: serde_json::Value) -> Result<serde_json::Value> {
        match self {
            Self::SpawnPerRequest(command) => command.invoke(request),
            Self::Persistent(command) => command.invoke(request),
        }
    }
}

struct SpawnPerRequestFixtureCommand {
    program: String,
    args: Vec<String>,
    timeout: Duration,
}

impl SpawnPerRequestFixtureCommand {
    fn invoke(&self, request: serde_json::Value) -> Result<serde_json::Value> {
        let mut child = Command::new(&self.program)
            .args(&self.args)
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

struct PersistentFixtureCommand {
    program: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PersistentFixtureCommand {
    fn spawn(program: &str, args: &[String]) -> Result<Self> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "failed to spawn persistent fixture contract command '{program}': {error}"
                ))
            })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            let _ = child.kill();
            SkeinError::Execution(
                "persistent fixture contract command stdin is not available".to_string(),
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            let _ = child.kill();
            SkeinError::Execution(
                "persistent fixture contract command stdout is not available".to_string(),
            )
        })?;
        Ok(Self {
            program: program.to_string(),
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    fn invoke(&mut self, request: serde_json::Value) -> Result<serde_json::Value> {
        writeln!(self.stdin, "{request}").map_err(|error| {
            SkeinError::Execution(format!(
                "failed to write persistent fixture contract command request: {error}"
            ))
        })?;
        self.stdin.flush().map_err(|error| {
            SkeinError::Execution(format!(
                "failed to flush persistent fixture contract command request: {error}"
            ))
        })?;
        let mut line = String::new();
        let bytes = self.stdout.read_line(&mut line).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to read persistent fixture contract command response: {error}"
            ))
        })?;
        if bytes == 0 {
            return Err(SkeinError::Execution(format!(
                "persistent fixture contract command '{}' closed stdout",
                self.program
            )));
        }
        serde_json::from_str(&line).map_err(|error| {
            SkeinError::Execution(format!(
                "persistent fixture contract command returned invalid JSON: {error}; stdout: {}",
                line.trim()
            ))
        })
    }
}

impl Drop for PersistentFixtureCommand {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CommandCheckReportStats {
    selected_checks: usize,
    checked_checks: usize,
    matched_checks: usize,
    stopped_after_first_failure: bool,
}

fn command_check_report_json(
    contract: &serde_json::Value,
    options: &FixtureContractCommandCheckOptions,
    stats: CommandCheckReportStats,
    primary_only_project_graph_checks: &[serde_json::Value],
    failures: Vec<serde_json::Value>,
) -> serde_json::Value {
    let total_checks = contract
        .get("check_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let full_contract_checked = stats.selected_checks as u64 == total_checks
        && stats.checked_checks as u64 == total_checks
        && options.start_check == 0
        && options.check_name.is_none()
        && options.max_checks.is_none();
    let selected_subset_ready = failures.is_empty()
        && stats.selected_checks > 0
        && stats.matched_checks == stats.checked_checks;
    let failure_summary = failure_summary_json(&failures, stats.stopped_after_first_failure);
    serde_json::json!({
        "protocol": "skein-nowledge-fixture-contract-command-check",
        "fixture": contract.get("fixture").cloned().unwrap_or(serde_json::Value::Null),
        "total_checks": total_checks,
        "selected_checks": stats.selected_checks,
        "checked_checks": stats.checked_checks,
        "matched_checks": stats.matched_checks,
        "failed_checks": failures.len(),
        "failure_summary": failure_summary,
        "failures": failures,
        "primary_only_project_graph_checks": primary_only_project_graph_checks,
        "options": {
            "max_checks": options.max_checks,
            "start_check": options.start_check,
            "check_name": options.check_name,
            "command_timeout_ms": options.command_timeout.as_millis() as u64,
            "allow_primary_only_project_graph": options.allow_primary_only_project_graph,
            "require_full_contract": options.require_full_contract,
            "command_mode": fixture_command_mode_json(options.command_mode),
            "stop_after_first_failure": options.stop_after_first_failure,
        },
        "selected_subset_ready": selected_subset_ready,
        "full_contract_checked": full_contract_checked,
        "full_contract_ready": full_contract_checked && selected_subset_ready,
        "required_contract_ready": if options.require_full_contract {
            full_contract_checked && selected_subset_ready
        } else {
            selected_subset_ready
        },
        "contract_command_check_ready": selected_subset_ready,
    })
}

fn fixture_command_mode_json(mode: FixtureCommandMode) -> &'static str {
    match mode {
        FixtureCommandMode::SpawnPerRequest => "spawn_per_request",
        FixtureCommandMode::Persistent => "persistent",
    }
}

fn failure_json(phase: &str, value: &serde_json::Value, message: String) -> serde_json::Value {
    let code = failure_code(phase, &message);
    serde_json::json!({
        "phase": phase,
        "code": code,
        "name": value.get("name").cloned().unwrap_or(serde_json::Value::Null),
        "index": value.get("index").cloned().unwrap_or(serde_json::Value::Null),
        "message": message,
    })
}

fn push_check_failure(
    failures: &mut Vec<serde_json::Value>,
    check: &serde_json::Value,
    message: String,
) {
    failures.push(failure_json("check", check, message));
}

fn failure_summary_json(
    failures: &[serde_json::Value],
    stopped_after_first_failure: bool,
) -> serde_json::Value {
    let mut phase_counts = std::collections::BTreeMap::<String, usize>::new();
    let mut code_counts = std::collections::BTreeMap::<String, usize>::new();
    for failure in failures {
        let phase = failure
            .get("phase")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *phase_counts.entry(phase.to_string()).or_default() += 1;
        let code = failure
            .get("code")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *code_counts.entry(code.to_string()).or_default() += 1;
    }
    let first_failure = failures.first();
    let first_check_failure = failures
        .iter()
        .find(|failure| failure.get("phase").and_then(serde_json::Value::as_str) == Some("check"));
    serde_json::json!({
        "failed_phase_counts": phase_counts,
        "failed_code_counts": code_counts,
        "first_failure": first_failure.cloned().unwrap_or(serde_json::Value::Null),
        "first_failed_check_index": first_check_failure
            .and_then(|failure| failure.get("index"))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        "first_failed_check_name": first_check_failure
            .and_then(|failure| failure.get("name"))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        "suggested_start_check": first_check_failure
            .and_then(|failure| failure.get("index"))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        "suggested_check_name": first_check_failure
            .and_then(|failure| failure.get("name"))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        "stopped_after_first_failure": stopped_after_first_failure,
    })
}

fn failure_code(phase: &str, message: &str) -> &'static str {
    if phase == "fixture_setup" {
        return "fixture_setup_failed";
    }
    if message == "no fixture checks selected" {
        return "no_checks_selected";
    }
    if message == "project_graph returned primary_only" {
        return "project_graph_primary_only";
    }
    if message.starts_with("unsupported fixture contract check kind") {
        return "unsupported_check_kind";
    }
    if message == "fixture contract check missing kind" {
        return "missing_check_kind";
    }
    if message.contains("row count mismatch") {
        return "row_count_mismatch";
    }
    if message.contains("ordered row mismatch") {
        return "ordered_row_mismatch";
    }
    if message.contains("unordered row mismatch") {
        return "unordered_row_mismatch";
    }
    if message.contains("project_graph mismatch") {
        return "project_graph_mismatch";
    }
    if message.contains("execute_session reply missing results") {
        return "execute_session_missing_results";
    }
    if message.contains("execute_session reply had no results") {
        return "execute_session_empty_results";
    }
    if message.contains("command reply missing rows array") {
        return "command_missing_rows";
    }
    if message.contains("expected_rows missing kind") {
        return "expected_rows_missing_kind";
    }
    if message.starts_with("unsupported expected_rows kind") {
        return "unsupported_expected_rows_kind";
    }
    if message.contains("expected_rows missing") {
        return "malformed_expected_rows";
    }
    if message.contains("fixture contract command timed out") {
        return "command_timeout";
    }
    if message.contains("returned invalid JSON") {
        return "command_invalid_json";
    }
    if message.contains("failed to spawn") {
        return "command_spawn_failed";
    }
    if message.contains("exited with") {
        return "command_exit_failed";
    }
    if message.contains("persistent fixture contract command") {
        return "persistent_command_failed";
    }
    if message.contains("fixture contract command") {
        return "command_failed";
    }
    "check_failed"
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

fn parse_usize(flag: &str, value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|error| SkeinError::Semantic(format!("invalid {flag} value '{value}': {error}")))
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
    use super::{check_contract_command, FixtureCommandMode, FixtureContractCommandCheckOptions};

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
        assert_eq!(report["failures"][0]["code"], "row_count_mismatch");
        assert_eq!(
            report["failure_summary"]["failed_code_counts"]["row_count_mismatch"],
            1
        );
    }

    #[test]
    fn contract_command_check_can_start_from_later_check() {
        let contract = serde_json::json!({
            "protocol": "skein-nowledge-fixture-contract",
            "fixture": "mini",
            "check_count": 2,
            "setup": [],
            "checks": [
                {
                    "index": 0,
                    "kind": "cypher",
                    "name": "first",
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
                },
                {
                    "index": 1,
                    "kind": "cypher",
                    "name": "second",
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
                        "count": 0
                    }
                }
            ]
        });
        let options = FixtureContractCommandCheckOptions {
            start_check: 1,
            ..FixtureContractCommandCheckOptions::default()
        };
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

        assert_eq!(report["selected_checks"], 1);
        assert_eq!(report["checked_checks"], 1);
        assert_eq!(report["matched_checks"], 1);
        assert_eq!(report["full_contract_checked"], false);
        assert_eq!(report["selected_subset_ready"], true);
        assert_eq!(report["required_contract_ready"], true);
        assert_eq!(report["contract_command_check_ready"], true);
    }

    #[test]
    fn contract_command_check_can_require_full_contract() {
        let contract = serde_json::json!({
            "protocol": "skein-nowledge-fixture-contract",
            "fixture": "mini",
            "check_count": 2,
            "setup": [],
            "checks": [
                {
                    "index": 0,
                    "kind": "cypher",
                    "name": "first",
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
                },
                {
                    "index": 1,
                    "kind": "cypher",
                    "name": "second",
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
                        "count": 0
                    }
                }
            ]
        });
        let options = FixtureContractCommandCheckOptions {
            start_check: 1,
            require_full_contract: true,
            ..FixtureContractCommandCheckOptions::default()
        };
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

        assert_eq!(report["selected_subset_ready"], true);
        assert_eq!(report["full_contract_ready"], false);
        assert_eq!(report["required_contract_ready"], false);
        assert_eq!(report["contract_command_check_ready"], true);
    }

    #[test]
    fn contract_command_check_reports_empty_selection() {
        let contract = serde_json::json!({
            "protocol": "skein-nowledge-fixture-contract",
            "fixture": "mini",
            "check_count": 1,
            "setup": [],
            "checks": [
                {
                    "index": 0,
                    "kind": "cypher",
                    "name": "present",
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
                        "count": 0
                    }
                }
            ]
        });
        let options = FixtureContractCommandCheckOptions {
            check_name: Some("missing".to_string()),
            ..FixtureContractCommandCheckOptions::default()
        };
        let report = check_contract_command(&contract, "python3", &[], &options).unwrap();

        assert_eq!(report["selected_checks"], 0);
        assert_eq!(report["checked_checks"], 0);
        assert_eq!(report["failed_checks"], 1);
        assert_eq!(report["failures"][0]["phase"], "selection");
        assert_eq!(report["failures"][0]["code"], "no_checks_selected");
        assert_eq!(
            report["failure_summary"]["failed_code_counts"]["no_checks_selected"],
            1
        );
        assert_eq!(report["contract_command_check_ready"], false);
    }

    #[cfg(unix)]
    #[test]
    fn contract_command_check_persistent_command_reuses_json_lines_process() {
        let contract = serde_json::json!({
            "protocol": "skein-nowledge-fixture-contract",
            "fixture": "mini",
            "check_count": 2,
            "setup": [],
            "checks": [
                {
                    "index": 0,
                    "kind": "cypher",
                    "name": "first",
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
                        "kind": "exact",
                        "rows": [
                            {
                                "request_index": 1
                            }
                        ]
                    }
                },
                {
                    "index": 1,
                    "kind": "cypher",
                    "name": "second",
                    "execution_mode": "database",
                    "setup": [],
                    "statement": {
                        "command_request": {
                            "op": "query",
                            "cypher": "MATCH (m) RETURN m",
                            "parameters": {}
                        }
                    },
                    "expected_rows": {
                        "kind": "exact",
                        "rows": [
                            {
                                "request_index": 2
                            }
                        ]
                    }
                }
            ]
        });
        let options = FixtureContractCommandCheckOptions {
            command_mode: FixtureCommandMode::Persistent,
            ..FixtureContractCommandCheckOptions::default()
        };
        let report = check_contract_command(
            &contract,
            "/bin/sh",
            &[
                "-c".to_string(),
                "i=0; while IFS= read -r line; do i=$((i + 1)); printf '{\"rows\":[{\"request_index\":%s}]}\\n' \"$i\"; done".to_string(),
            ],
            &options,
        )
        .unwrap();

        assert_eq!(report["checked_checks"], 2);
        assert_eq!(report["matched_checks"], 2);
        assert_eq!(report["full_contract_ready"], true);
        assert_eq!(report["required_contract_ready"], true);
    }

    #[test]
    fn contract_command_check_can_stop_after_first_failure() {
        let contract = serde_json::json!({
            "protocol": "skein-nowledge-fixture-contract",
            "fixture": "mini",
            "check_count": 2,
            "setup": [],
            "checks": [
                {
                    "index": 0,
                    "kind": "cypher",
                    "name": "first failing check",
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
                },
                {
                    "index": 1,
                    "kind": "cypher",
                    "name": "second failing check",
                    "execution_mode": "database",
                    "setup": [],
                    "statement": {
                        "command_request": {
                            "op": "query",
                            "cypher": "MATCH (m) RETURN m",
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
        let options = FixtureContractCommandCheckOptions {
            stop_after_first_failure: true,
            ..FixtureContractCommandCheckOptions::default()
        };
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

        assert_eq!(report["checked_checks"], 1);
        assert_eq!(report["failed_checks"], 1);
        assert_eq!(report["failure_summary"]["failed_phase_counts"]["check"], 1);
        assert_eq!(
            report["failure_summary"]["failed_code_counts"]["row_count_mismatch"],
            1
        );
        assert_eq!(report["failure_summary"]["first_failed_check_index"], 0);
        assert_eq!(
            report["failure_summary"]["suggested_check_name"],
            "first failing check"
        );
        assert_eq!(
            report["failure_summary"]["stopped_after_first_failure"],
            true
        );
    }
}
