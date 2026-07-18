use skein::{
    external_shadow_json_from_value, external_shadow_value_from_json,
    ExternalShadowProjectGraphReply, ExternalShadowProjectGraphRequest,
    ExternalShadowProtocolBackend, ExternalShadowProtocolServer, ExternalShadowStatementRequest,
    QueryOutput, Result, SkeinError, Value,
};
use std::collections::BTreeMap;
use std::io::{self, BufReader, Write};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
const COMMAND_WAIT_POLL_MS: u64 = 10;

fn main() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let backend = PreviousWrapperShadowBackend::new(previous_wrapper_from_args()?);
    let mut server = ExternalShadowProtocolServer::new(backend);
    server.run_json_lines(BufReader::new(stdin.lock()), stdout.lock())
}

fn previous_wrapper_from_args() -> Result<PreviousWrapperAdapter> {
    let mut args = std::env::args().skip(1);
    let mut timeout = Duration::from_millis(DEFAULT_COMMAND_TIMEOUT_MS);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--command-timeout-ms" => {
                let Some(value) = args.next() else {
                    return Err(SkeinError::Semantic(command_usage()));
                };
                timeout = Duration::from_millis(parse_timeout_ms(&value)?);
            }
            "--command" => {
                let Some(program) = args.next() else {
                    return Err(SkeinError::Semantic(command_usage()));
                };
                return Ok(PreviousWrapperAdapter::Command(CommandPreviousWrapper {
                    program,
                    args: args.collect(),
                    timeout,
                }));
            }
            "--help" | "-h" => return Err(SkeinError::Semantic(command_usage())),
            other => {
                return Err(SkeinError::Semantic(format!(
                    "unknown previous-wrapper adapter option '{other}'; {}",
                    command_usage()
                )));
            }
        }
    }
    Ok(PreviousWrapperAdapter::Unavailable(
        UnavailablePreviousWrapper,
    ))
}

fn parse_timeout_ms(value: &str) -> Result<u64> {
    value.parse::<u64>().map_err(|error| {
        SkeinError::Semantic(format!(
            "invalid --command-timeout-ms value '{value}': {error}"
        ))
    })
}

fn command_usage() -> String {
    "usage: nowledge_previous_wrapper_shadow_adapter [--command-timeout-ms <ms>] [--command <program> [args...]]".to_string()
}

trait PreviousWrapperGraph {
    fn query(&mut self, cypher: &str, parameters: &BTreeMap<String, Value>)
        -> Result<Vec<JsonRow>>;

    fn execute_session(
        &mut self,
        statements: &[ExternalShadowStatementRequest],
    ) -> Result<Vec<Vec<JsonRow>>> {
        statements
            .iter()
            .map(|statement| self.query(&statement.cypher, &statement.parameters))
            .collect()
    }

    fn project_graph(
        &mut self,
        _request: &ExternalShadowProjectGraphRequest,
    ) -> Result<ExternalShadowProjectGraphReply> {
        Ok(ExternalShadowProjectGraphReply::PrimaryOnly {
            reason: Some("previous wrapper project_graph hook is not wired".to_string()),
        })
    }
}

type JsonRow = BTreeMap<String, serde_json::Value>;

enum PreviousWrapperAdapter {
    Unavailable(UnavailablePreviousWrapper),
    Command(CommandPreviousWrapper),
}

impl PreviousWrapperGraph for PreviousWrapperAdapter {
    fn query(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<Vec<JsonRow>> {
        match self {
            Self::Unavailable(graph) => graph.query(cypher, parameters),
            Self::Command(graph) => graph.query(cypher, parameters),
        }
    }

    fn execute_session(
        &mut self,
        statements: &[ExternalShadowStatementRequest],
    ) -> Result<Vec<Vec<JsonRow>>> {
        match self {
            Self::Unavailable(graph) => graph.execute_session(statements),
            Self::Command(graph) => graph.execute_session(statements),
        }
    }

    fn project_graph(
        &mut self,
        request: &ExternalShadowProjectGraphRequest,
    ) -> Result<ExternalShadowProjectGraphReply> {
        match self {
            Self::Unavailable(graph) => graph.project_graph(request),
            Self::Command(graph) => graph.project_graph(request),
        }
    }
}

struct PreviousWrapperShadowBackend<G> {
    graph: G,
}

impl<G> PreviousWrapperShadowBackend<G> {
    fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G> ExternalShadowProtocolBackend for PreviousWrapperShadowBackend<G>
where
    G: PreviousWrapperGraph,
{
    fn engine_kind(&self) -> &'static str {
        "previous_wrapper"
    }

    fn execute(&mut self, statement: ExternalShadowStatementRequest) -> Result<QueryOutput> {
        self.graph
            .query(&statement.cypher, &statement.parameters)
            .and_then(query_output_from_json_rows)
    }

    fn execute_session(
        &mut self,
        statements: Vec<ExternalShadowStatementRequest>,
    ) -> Result<Vec<QueryOutput>> {
        self.graph
            .execute_session(&statements)?
            .into_iter()
            .map(query_output_from_json_rows)
            .collect()
    }

    fn project_graph(
        &mut self,
        request: ExternalShadowProjectGraphRequest,
    ) -> Result<ExternalShadowProjectGraphReply> {
        self.graph.project_graph(&request)
    }
}

struct UnavailablePreviousWrapper;

impl PreviousWrapperGraph for UnavailablePreviousWrapper {
    fn query(
        &mut self,
        _cypher: &str,
        _parameters: &BTreeMap<String, Value>,
    ) -> Result<Vec<JsonRow>> {
        Err(SkeinError::Execution(
            "replace UnavailablePreviousWrapper with the Nowledge Kuzu/Ladybug wrapper".to_string(),
        ))
    }
}

struct CommandPreviousWrapper {
    program: String,
    args: Vec<String>,
    timeout: Duration,
}

impl PreviousWrapperGraph for CommandPreviousWrapper {
    fn query(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<Vec<JsonRow>> {
        let reply = self.invoke(command_query_request(cypher, parameters))?;
        parse_rows_reply(&reply, "query")
    }

    fn execute_session(
        &mut self,
        statements: &[ExternalShadowStatementRequest],
    ) -> Result<Vec<Vec<JsonRow>>> {
        let reply = self.invoke(command_session_request(statements))?;
        parse_session_reply(&reply)
    }

    fn project_graph(
        &mut self,
        request: &ExternalShadowProjectGraphRequest,
    ) -> Result<ExternalShadowProjectGraphReply> {
        let reply = self.invoke(command_project_graph_request(request))?;
        parse_project_graph_reply(&reply)
    }
}

impl CommandPreviousWrapper {
    fn invoke(&self, request: serde_json::Value) -> Result<serde_json::Value> {
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "failed to spawn previous-wrapper command '{}': {error}",
                    self.program
                ))
            })?;

        {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                SkeinError::Execution("previous-wrapper command stdin is not available".to_string())
            })?;
            writeln!(stdin, "{request}").map_err(|error| {
                SkeinError::Execution(format!(
                    "failed to write previous-wrapper command request: {error}"
                ))
            })?;
        }

        let output = self.wait_for_output(child)?;
        if !output.status.success() {
            return Err(SkeinError::Execution(format!(
                "previous-wrapper command exited with {}; stderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        serde_json::from_slice(&output.stdout).map_err(|error| {
            SkeinError::Execution(format!(
                "previous-wrapper command returned invalid JSON: {error}; stdout: {}",
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
                            "failed to collect previous-wrapper command output: {error}"
                        ))
                    });
                }
                Ok(None) if started_at.elapsed() >= self.timeout => {
                    let _ = child.kill();
                    let output = child.wait_with_output().map_err(|error| {
                        SkeinError::Execution(format!(
                            "previous-wrapper command timed out after {} ms and failed to collect output: {error}",
                            self.timeout.as_millis()
                        ))
                    })?;
                    return Err(SkeinError::Execution(format!(
                        "previous-wrapper command timed out after {} ms; stderr: {}",
                        self.timeout.as_millis(),
                        String::from_utf8_lossy(&output.stderr).trim()
                    )));
                }
                Ok(None) => thread::sleep(Duration::from_millis(COMMAND_WAIT_POLL_MS)),
                Err(error) => {
                    let _ = child.kill();
                    return Err(SkeinError::Execution(format!(
                        "failed to poll previous-wrapper command: {error}"
                    )));
                }
            }
        }
    }
}

fn command_query_request(cypher: &str, parameters: &BTreeMap<String, Value>) -> serde_json::Value {
    serde_json::json!({
        "op": "query",
        "cypher": cypher,
        "parameters": parameters_json(parameters),
    })
}

fn command_session_request(statements: &[ExternalShadowStatementRequest]) -> serde_json::Value {
    serde_json::json!({
        "op": "execute_session",
        "statements": statements
            .iter()
            .map(|statement| command_query_request(&statement.cypher, &statement.parameters))
            .collect::<Vec<_>>(),
    })
}

fn command_project_graph_request(request: &ExternalShadowProjectGraphRequest) -> serde_json::Value {
    serde_json::json!({
        "op": "project_graph",
        "rel_type": request.rel_type.clone(),
        "expected_incoming_nodes": request.expected_incoming_nodes.clone(),
        "include_communities": request.include_communities,
        "include_hierarchical_communities": request.include_hierarchical_communities,
    })
}

fn parameters_json(parameters: &BTreeMap<String, Value>) -> serde_json::Value {
    serde_json::Value::Object(
        parameters
            .iter()
            .map(|(key, value)| (key.clone(), external_shadow_json_from_value(value.clone())))
            .collect(),
    )
}

fn parse_rows_reply(reply: &serde_json::Value, op: &str) -> Result<Vec<JsonRow>> {
    let rows = reply
        .get("rows")
        .ok_or_else(|| SkeinError::Execution(format!("{op} reply missing rows")))?;
    json_rows(rows, op)
}

fn parse_session_reply(reply: &serde_json::Value) -> Result<Vec<Vec<JsonRow>>> {
    let results = reply
        .get("results")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution("execute_session reply missing results array".to_string())
        })?;
    results
        .iter()
        .enumerate()
        .map(|(index, result)| {
            let rows = result.get("rows").unwrap_or(result);
            json_rows(rows, &format!("execute_session result {index}"))
        })
        .collect()
}

fn parse_project_graph_reply(reply: &serde_json::Value) -> Result<ExternalShadowProjectGraphReply> {
    if reply
        .get("primary_only")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(ExternalShadowProjectGraphReply::PrimaryOnly {
            reason: reply
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        });
    }
    if let Some(output) = reply.get("ok") {
        return Ok(ExternalShadowProjectGraphReply::Ok(output.clone()));
    }
    Ok(ExternalShadowProjectGraphReply::Ok(reply.clone()))
}

fn json_rows(rows: &serde_json::Value, context: &str) -> Result<Vec<JsonRow>> {
    rows.as_array()
        .ok_or_else(|| SkeinError::Execution(format!("{context} rows is not an array")))?
        .iter()
        .map(|row| {
            row.as_object()
                .ok_or_else(|| SkeinError::Execution(format!("{context} row is not a JSON object")))
                .map(|row| {
                    row.iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                })
        })
        .collect()
}

fn query_output_from_json_rows(rows: Vec<JsonRow>) -> Result<QueryOutput> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(key, value)| Ok((key, external_shadow_value_from_json(&value)?)))
                .collect::<Result<BTreeMap<_, _>>>()
        })
        .collect::<Result<Vec<_>>>()
        .map(|rows| QueryOutput { rows })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingPreviousWrapper {
        executed: Vec<String>,
    }

    impl PreviousWrapperGraph for RecordingPreviousWrapper {
        fn query(
            &mut self,
            cypher: &str,
            parameters: &BTreeMap<String, Value>,
        ) -> Result<Vec<JsonRow>> {
            self.executed.push(cypher.to_string());
            Ok(vec![BTreeMap::from([
                (
                    "cypher".to_string(),
                    serde_json::Value::String(cypher.to_string()),
                ),
                (
                    "has_id".to_string(),
                    serde_json::Value::Bool(parameters.contains_key("id")),
                ),
            ])])
        }
    }

    #[test]
    fn scaffold_reports_previous_wrapper_ready() {
        let backend = PreviousWrapperShadowBackend::new(RecordingPreviousWrapper::default());
        let mut server = ExternalShadowProtocolServer::new(backend);

        let response = server.handle_request(&serde_json::json!({
            "protocol_version": skein::EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "ready"
        }));

        assert_eq!(response["ok"]["engine_kind"], "previous_wrapper");
        assert_eq!(
            response["ok"]["capabilities"],
            serde_json::json!(["execute", "execute_session", "project_graph"])
        );
    }

    #[test]
    fn scaffold_converts_json_rows_to_shadow_output() {
        let backend = PreviousWrapperShadowBackend::new(RecordingPreviousWrapper::default());
        let mut server = ExternalShadowProtocolServer::new(backend);

        let response = server.handle_request(&serde_json::json!({
            "protocol_version": skein::EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "execute",
            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.id",
            "parameters": {
                "id": "m1"
            }
        }));

        assert_eq!(
            response["ok"]["rows"][0]["cypher"],
            "MATCH (m:Memory {id: $id}) RETURN m.id"
        );
        assert_eq!(response["ok"]["rows"][0]["has_id"], true);
    }

    #[test]
    fn scaffold_defaults_project_graph_to_primary_only() {
        let backend = PreviousWrapperShadowBackend::new(RecordingPreviousWrapper::default());
        let mut server = ExternalShadowProtocolServer::new(backend);

        let response = server.handle_request(&serde_json::json!({
            "protocol_version": skein::EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "project_graph"
        }));

        assert_eq!(response["primary_only"], true);
        assert_eq!(
            response["reason"],
            "previous wrapper project_graph hook is not wired"
        );
    }

    #[test]
    fn command_adapter_builds_query_request_with_shadow_values() {
        let request = command_query_request(
            "MATCH (m:Memory {id: $id}) RETURN m.title",
            &BTreeMap::from([
                ("id".to_string(), Value::String("m1".to_string())),
                ("score".to_string(), Value::Float(1.5)),
            ]),
        );

        assert_eq!(request["op"], "query");
        assert_eq!(
            request["cypher"],
            "MATCH (m:Memory {id: $id}) RETURN m.title"
        );
        assert_eq!(request["parameters"]["id"], "m1");
        assert_eq!(request["parameters"]["score"], 1.5);
    }

    #[test]
    fn command_adapter_rejects_invalid_timeout() {
        let error = parse_timeout_ms("not-a-number").unwrap_err();

        assert!(error
            .to_string()
            .contains("invalid --command-timeout-ms value"));
    }

    #[cfg(unix)]
    #[test]
    fn command_adapter_times_out_hung_command() {
        let wrapper = CommandPreviousWrapper {
            program: "/bin/sh".to_string(),
            args: vec!["-c".to_string(), "sleep 1".to_string()],
            timeout: Duration::from_millis(1),
        };

        let error = wrapper
            .invoke(serde_json::json!({
                "op": "query"
            }))
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("previous-wrapper command timed out"));
    }

    #[test]
    fn command_adapter_parses_session_reply_rows() {
        let rows = parse_session_reply(&serde_json::json!({
            "results": [
                {
                    "rows": [
                        {
                            "title": "First"
                        }
                    ]
                },
                [
                    {
                        "title": "Second"
                    }
                ]
            ]
        }))
        .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0]["title"], "First");
        assert_eq!(rows[1][0]["title"], "Second");
    }

    #[test]
    fn command_adapter_parses_project_graph_primary_only_reply() {
        let reply = parse_project_graph_reply(&serde_json::json!({
            "primary_only": true,
            "reason": "wrapper does not expose projection metadata yet"
        }))
        .unwrap();

        assert_eq!(
            reply,
            ExternalShadowProjectGraphReply::PrimaryOnly {
                reason: Some("wrapper does not expose projection metadata yet".to_string())
            }
        );
    }
}
