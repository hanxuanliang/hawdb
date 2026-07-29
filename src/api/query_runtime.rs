use super::*;

impl Database {
    pub fn query_work_request(&self) -> WorkRequest {
        self.system_variables.query_work_request()
    }

    pub fn query_work_request_for(&self, cypher_text: &str) -> Result<WorkRequest> {
        let statement = cypher::parse(cypher_text)?;
        query_work_request_for_statement(&self.system_variables, &statement)
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.query_with_params_trace(cypher_text, parameters, false)
            .map(|(output, _)| output)
    }

    pub(crate) fn query_with_params_trace(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        capture_trace: bool,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let mut external = executor::NoExternalReadOperator;
        self.query_with_params_trace_and_external(
            cypher_text,
            parameters,
            capture_trace,
            &mut external,
        )
    }

    pub(crate) fn query_with_params_trace_and_external(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        capture_trace: bool,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let started = std::time::Instant::now();
        let statement = cypher::parse(cypher_text)?;
        let body = statement_body(&statement);
        let statement_kind_name = statement_kind(&statement);
        if let cypher::Statement::Explain(explain) = &statement {
            let query_result = self
                .execute_explain_statement(cypher_text, explain, parameters, external)
                .map(|output| (output, QueryExecutionTrace::uncached(statement)));
            let statement_result = match &query_result {
                Ok((output, _)) => Ok(output),
                Err(error) => Err(error),
            };
            self.record_statement_execution(
                "cypher",
                cypher_text,
                statement_kind_name,
                started,
                statement_result,
                None,
            );
            return query_result;
        }
        if let cypher::Statement::SetSystemVariable(set) = body {
            reject_system_variable_parameters(parameters)?;
            return self
                .system_variables
                .apply_set_system_variable(set)
                .map(|output| (output, QueryExecutionTrace::uncached(statement.clone())));
        }
        if matches!(body, cypher::Statement::Checkpoint) {
            if !parameters.is_empty() {
                return Err(SkeinError::Semantic(
                    "CHECKPOINT does not accept parameters".to_string(),
                ));
            }
            self.checkpoint()?;
            return Ok((
                QueryOutput { rows: Vec::new() },
                QueryExecutionTrace::uncached(statement),
            ));
        }
        let query_result = (|| {
            query_work_request_for_statement(&self.system_variables, &statement)?;
            let optimized = self.optimized_query_plan(cypher_text, &statement, parameters)?;
            let is_mutation = executor::is_mutation_plan(&optimized.physical_plan)?;
            if is_mutation {
                self.ensure_writable()?;
            }
            let (rows, execution_profile) = if is_mutation {
                (
                    executor::execute(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                    )?,
                    None,
                )
            } else {
                let profiled = executor::execute_with_row_limit_profile_and_external(
                    &optimized.physical_plan,
                    &mut self.catalog,
                    &mut self.store,
                    parameters,
                    external,
                    self.config.max_read_result_rows,
                )?;
                (profiled.rows, Some(profiled.profile))
            };
            Ok((
                QueryOutput { rows },
                QueryExecutionTrace {
                    statement,
                    optimizer_trace: capture_trace.then_some(optimized.trace),
                    plan_cache_lookup: Some(optimized.plan_cache_lookup),
                    execution_profile,
                },
            ))
        })();
        let statement_result = match &query_result {
            Ok((output, _)) => Ok(output),
            Err(error) => Err(error),
        };
        let execution_profile = query_result
            .as_ref()
            .ok()
            .and_then(|(_, trace)| trace.execution_profile.as_ref());
        self.record_statement_execution(
            "cypher",
            cypher_text,
            statement_kind_name,
            started,
            statement_result,
            execution_profile,
        );
        query_result
    }

    fn execute_explain_statement(
        &mut self,
        cypher_text: &str,
        explain: &cypher::Explain,
        parameters: &BTreeMap<String, Value>,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<QueryOutput> {
        let work_request =
            query_work_request_for_statement(&self.system_variables, &explain.statement)?;
        let optimized = self.optimized_query_plan(cypher_text, &explain.statement, parameters)?;
        let inner_statement_kind = statement_kind(statement_body(&explain.statement));
        if explain.analyze {
            if executor::is_mutation_plan(&optimized.physical_plan)? {
                return Err(SkeinError::Execution(
                    "EXPLAIN ANALYZE only supports read queries".to_string(),
                ));
            }
            let profiled = executor::execute_with_row_limit_profile_and_external(
                &optimized.physical_plan,
                &mut self.catalog,
                &mut self.store,
                parameters,
                external,
                self.config.max_read_result_rows,
            )?;
            return Ok(QueryOutput {
                rows: vec![explain_analyze_output_row(
                    &optimized,
                    work_request,
                    inner_statement_kind,
                    profiled.rows.len(),
                    &profiled.profile,
                )],
            });
        }
        Ok(QueryOutput {
            rows: vec![explain_output_row(
                &optimized,
                work_request,
                inner_statement_kind,
            )],
        })
    }
}
