use skein::{Database, QueryOutput, Result, SkeinError, Value};
use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};

fn main() -> Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut db = Database::new();
    for line in stdin.lock().lines() {
        let line =
            line.map_err(|error| SkeinError::Execution(format!("failed to read stdin: {error}")))?;
        let request = match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(request) => request,
            Err(error) => {
                write_response(
                    &mut stdout,
                    json_error("parse", format!("invalid JSON: {error}")),
                )?;
                continue;
            }
        };
        let response = handle_request(&mut db, &request);
        write_response(&mut stdout, response)?;
    }
    Ok(())
}

fn handle_request(db: &mut Database, request: &serde_json::Value) -> serde_json::Value {
    let op = request.get("op").and_then(serde_json::Value::as_str);
    match op {
        Some("execute") => handle_execute(db, request),
        Some("execute_session") => handle_execute_session(db, request),
        Some("project_graph") => handle_project_graph(db, request),
        Some(other) => json_error("semantic", format!("unknown shadow op '{other}'")),
        None => json_error("semantic", "shadow request missing op"),
    }
}

fn handle_execute(db: &mut Database, request: &serde_json::Value) -> serde_json::Value {
    let Some(statement) = statement_from_request(request) else {
        return json_error("semantic", "execute request missing cypher");
    };
    match db.query_with_params(&statement.cypher, &statement.parameters) {
        Ok(output) => json_output(output),
        Err(error) => json_error_from_skein(error),
    }
}

fn handle_execute_session(db: &mut Database, request: &serde_json::Value) -> serde_json::Value {
    let Some(statements) = request
        .get("statements")
        .and_then(serde_json::Value::as_array)
    else {
        return json_error("semantic", "execute_session request missing statements");
    };
    let mut decoded = Vec::with_capacity(statements.len());
    for statement in statements {
        let Some(statement) = statement_from_request(statement) else {
            return json_error("semantic", "session statement missing cypher");
        };
        decoded.push(statement);
    }

    let mut session = db.session();
    let mut outputs = Vec::with_capacity(decoded.len());
    for statement in decoded {
        match session.query_with_params(&statement.cypher, &statement.parameters) {
            Ok(output) => outputs.push(rows_json(output)),
            Err(error) => return json_error_from_skein(error),
        }
    }
    serde_json::json!({ "ok": { "outputs": outputs } })
}

fn handle_project_graph(db: &mut Database, request: &serde_json::Value) -> serde_json::Value {
    let rel_type = request.get("rel_type").and_then(serde_json::Value::as_str);
    let graph = db.project_graph(rel_type);
    let expected_incoming_nodes = request
        .get("expected_incoming_nodes")
        .and_then(serde_json::Value::as_array)
        .map(|nodes| {
            nodes
                .iter()
                .filter_map(serde_json::Value::as_u64)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let include_communities = request
        .get("include_communities")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let include_hierarchical_communities = request
        .get("include_hierarchical_communities")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let page_rank_scores = graph
        .page_rank(Default::default())
        .into_iter()
        .map(|score| serde_json::json!([score.node.0, score.score]))
        .collect::<Vec<_>>();
    let page_rank_top_node = page_rank_scores
        .first()
        .and_then(|score| score.as_array())
        .and_then(|score| score.first())
        .and_then(serde_json::Value::as_u64);
    serde_json::json!({
        "ok": {
            "node_count": graph.node_count(),
            "edge_count": graph.edge_count(),
            "incoming": expected_incoming_nodes
                .into_iter()
                .map(|node| {
                    let sources = graph
                        .incoming_sources(skein::store::NodeId(node))
                        .map(|sources| sources.map(|source| source.0).collect::<Vec<_>>())
                        .unwrap_or_default();
                    serde_json::json!([node, sources])
                })
                .collect::<Vec<_>>(),
            "communities": if include_communities {
                graph
                    .louvain_communities(Default::default())
                    .into_iter()
                    .map(|assignment| serde_json::json!([assignment.node.0, assignment.community.0]))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            },
            "hierarchical_communities": if include_hierarchical_communities {
                graph
                    .hierarchical_louvain_communities(Default::default())
                    .into_iter()
                    .map(|assignment| {
                        serde_json::json!([
                            assignment.level,
                            assignment.node.0,
                            assignment.community.0
                        ])
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            },
            "page_rank_scores": page_rank_scores,
            "page_rank_top_node": page_rank_top_node,
        }
    })
}

struct Statement {
    cypher: String,
    parameters: BTreeMap<String, Value>,
}

fn statement_from_request(request: &serde_json::Value) -> Option<Statement> {
    let cypher = request.get("cypher")?.as_str()?.to_string();
    let parameters = request
        .get("parameters")
        .and_then(serde_json::Value::as_object)
        .map(|parameters| {
            parameters
                .iter()
                .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
                .collect::<Result<BTreeMap<_, _>>>()
        })
        .transpose()
        .ok()?
        .unwrap_or_default();
    Some(Statement { cypher, parameters })
}

fn json_output(output: QueryOutput) -> serde_json::Value {
    serde_json::json!({ "ok": rows_json(output) })
}

fn rows_json(output: QueryOutput) -> serde_json::Value {
    serde_json::json!({
        "rows": output
            .rows
            .into_iter()
            .map(|row| {
                serde_json::Value::Object(
                    row.into_iter()
                        .map(|(key, value)| (key, json_from_value(value)))
                        .collect(),
                )
            })
            .collect::<Vec<_>>()
    })
}

fn value_from_json(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Execution(format!(
                    "unsupported JSON number: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

fn json_from_value(value: Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(value),
        Value::Int(value) => serde_json::Value::Number(value.into()),
        Value::Float(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(value) => serde_json::Value::String(value),
        Value::List(values) => {
            serde_json::Value::Array(values.into_iter().map(json_from_value).collect())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, json_from_value(value)))
                .collect(),
        ),
    }
}

fn json_error(class: &str, message: impl ToString) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "class": class,
            "message": message.to_string(),
        }
    })
}

fn json_error_from_skein(error: SkeinError) -> serde_json::Value {
    match error {
        SkeinError::Parse(message) => json_error("parse", message),
        SkeinError::Semantic(message) => json_error("semantic", message),
        SkeinError::Storage(message) => json_error("storage", message),
        SkeinError::Execution(message) => json_error("execution", message),
    }
}

fn write_response(stdout: &mut io::Stdout, response: serde_json::Value) -> Result<()> {
    writeln!(stdout, "{}", response)
        .map_err(|error| SkeinError::Execution(format!("failed to write stdout: {error}")))?;
    stdout
        .flush()
        .map_err(|error| SkeinError::Execution(format!("failed to flush stdout: {error}")))
}
