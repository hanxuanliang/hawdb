use super::{QueryOutput, Result, SkeinError};
use crate::cypher;
use crate::optimizer::OptimizerSearchDirective;
use crate::qos::{WorkClass, WorkPriority, WorkRequest};
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySystemVariables {
    pub work_priority: WorkPriority,
    pub work_class: WorkClass,
    pub estimated_operations: usize,
}

impl Default for QuerySystemVariables {
    fn default() -> Self {
        Self {
            work_priority: WorkPriority::Foreground,
            work_class: WorkClass::Query,
            estimated_operations: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueryStatementVariables {
    query_variables: QuerySystemVariables,
    pub(super) optimizer_search: OptimizerSearchDirective,
}

impl QueryStatementVariables {
    fn from_query_variables(query_variables: QuerySystemVariables) -> Self {
        Self {
            query_variables,
            optimizer_search: OptimizerSearchDirective::Auto,
        }
    }

    fn query_work_request(&self) -> WorkRequest {
        self.query_variables.query_work_request()
    }
}

impl QuerySystemVariables {
    pub fn query_work_request(&self) -> WorkRequest {
        match self.work_priority {
            WorkPriority::Foreground => {
                WorkRequest::foreground(self.work_class, self.estimated_operations)
            }
            WorkPriority::Background => {
                WorkRequest::background(self.work_class, self.estimated_operations)
            }
        }
    }

    pub(super) fn apply_set_system_variable(
        &mut self,
        set: &cypher::SetSystemVariable,
    ) -> Result<QueryOutput> {
        let value = literal_system_variable_value(&set.value)?;
        match set.name.as_str() {
            "work_priority" => {
                let priority = string_system_variable_value(&set.name, &value)?
                    .parse::<WorkPriority>()
                    .map_err(|_| {
                        SkeinError::Semantic(
                            "SET system.work_priority accepts foreground or background".to_string(),
                        )
                    })?;
                self.work_priority = priority;
                Ok(query_output_row(
                    "system.work_priority",
                    Value::String(priority.as_str().to_string()),
                ))
            }
            "work_class" => {
                let class = string_system_variable_value(&set.name, &value)?
                    .parse::<WorkClass>()
                    .map_err(|_| {
                        SkeinError::Semantic(
                            "SET system.work_class accepts query, mutation, projection, import, analytics, or shadow"
                                .to_string(),
                        )
                    })?;
                self.work_class = class;
                Ok(query_output_row(
                    "system.work_class",
                    Value::String(class.as_str().to_string()),
                ))
            }
            "estimated_operations" => {
                let estimated_operations = usize_system_variable_value(&set.name, &value)?;
                self.estimated_operations = estimated_operations;
                Ok(query_output_row(
                    "system.estimated_operations",
                    Value::Int(i64::try_from(estimated_operations).unwrap_or(i64::MAX)),
                ))
            }
            _ => Err(SkeinError::Semantic(format!(
                "unknown system variable system.{}",
                set.name
            ))),
        }
    }

    fn apply_system_variable_hints(
        &self,
        hints: &[cypher::SetSystemVariable],
    ) -> Result<QueryStatementVariables> {
        let mut variables = QueryStatementVariables::from_query_variables(self.clone());
        let mut seen = BTreeSet::new();
        for hint in hints {
            if !seen.insert(hint.name.as_str()) {
                return Err(SkeinError::Semantic(format!(
                    "duplicate CYPHER system hint system.{}",
                    hint.name
                )));
            }
            if hint.name == "optimizer_search" {
                let value = literal_system_variable_value(&hint.value)?;
                let value = string_system_variable_value(&hint.name, &value)?;
                variables.optimizer_search =
                    OptimizerSearchDirective::from_str(&value).map_err(|_| {
                        SkeinError::Semantic(
                            "CYPHER system.optimizer_search accepts auto, memo, or direct_fallback"
                                .to_string(),
                        )
                    })?;
            } else {
                variables.query_variables.apply_set_system_variable(hint)?;
            }
        }
        Ok(variables)
    }
}

pub(super) fn reject_system_variable_parameters(
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    if parameters.is_empty() {
        Ok(())
    } else {
        Err(SkeinError::Semantic(
            "SET system variable does not accept parameters".to_string(),
        ))
    }
}

pub(super) fn query_work_request_for_statement(
    variables: &QuerySystemVariables,
    statement: &cypher::Statement,
) -> Result<WorkRequest> {
    query_statement_variables_for_statement(variables, statement)
        .map(|variables| variables.query_work_request())
}

pub(super) fn query_statement_variables_for_statement(
    variables: &QuerySystemVariables,
    statement: &cypher::Statement,
) -> Result<QueryStatementVariables> {
    match statement {
        cypher::Statement::CypherQuery(query) => {
            variables.apply_system_variable_hints(&query.system_variables)
        }
        cypher::Statement::Explain(explain) => {
            query_statement_variables_for_statement(variables, &explain.statement)
        }
        _ => Ok(QueryStatementVariables::from_query_variables(
            variables.clone(),
        )),
    }
}

fn literal_system_variable_value(value: &cypher::ValueExpression) -> Result<Value> {
    match value {
        cypher::ValueExpression::Literal(value) => Ok(value.clone()),
        _ => Err(SkeinError::Semantic(
            "SET system variable requires a literal value".to_string(),
        )),
    }
}

fn string_system_variable_value(name: &str, value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.to_ascii_lowercase()),
        _ => Err(SkeinError::Semantic(format!(
            "SET system.{name} requires a string value"
        ))),
    }
}

fn usize_system_variable_value(name: &str, value: &Value) -> Result<usize> {
    match value {
        Value::Int(value) if *value >= 0 => usize::try_from(*value)
            .map_err(|_| SkeinError::Semantic(format!("SET system.{name} value is too large"))),
        _ => Err(SkeinError::Semantic(format!(
            "SET system.{name} requires a non-negative integer value"
        ))),
    }
}

fn query_output_row(name: &str, value: Value) -> QueryOutput {
    QueryOutput {
        rows: vec![BTreeMap::from([
            ("name".to_string(), Value::String(name.to_string())),
            ("value".to_string(), value),
        ])],
    }
}
