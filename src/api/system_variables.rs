use super::{QueryOutput, Result, SkeinError};
use crate::cypher;
use crate::qos::{WorkClass, WorkPriority, WorkRequest};
use crate::value::Value;
use std::collections::BTreeMap;

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
    ) -> Result<QuerySystemVariables> {
        let mut variables = self.clone();
        for hint in hints {
            variables.apply_set_system_variable(hint)?;
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
    match statement {
        cypher::Statement::CypherQuery(query) => variables
            .apply_system_variable_hints(&query.system_variables)
            .map(|variables| variables.query_work_request()),
        cypher::Statement::Explain(explain) => {
            query_work_request_for_statement(variables, &explain.statement)
        }
        _ => Ok(variables.query_work_request()),
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
