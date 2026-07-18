use skein::{
    external_shadow_value_from_json, ExternalShadowProjectGraphReply,
    ExternalShadowProjectGraphRequest, ExternalShadowProtocolBackend, ExternalShadowProtocolServer,
    ExternalShadowStatementRequest, QueryOutput, Result, SkeinError, Value,
};
use std::collections::BTreeMap;
use std::io::{self, BufReader};

fn main() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let backend = PreviousWrapperShadowBackend::new(UnavailablePreviousWrapper);
    let mut server = ExternalShadowProtocolServer::new(backend);
    server.run_json_lines(BufReader::new(stdin.lock()), stdout.lock())
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
}
