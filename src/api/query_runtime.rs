use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) struct RuntimeAdmissionPlan {
    pub work_request: WorkRequest,
    pub is_mutation: bool,
    pub estimated_memory_bytes: u64,
    pub streaming_eligible: bool,
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
const CONTROL_STATEMENT_MEMORY_BYTES: u64 = 1024 * 1024;

impl Database {
    pub fn query_work_request(&self) -> WorkRequest {
        self.system_variables.query_work_request()
    }

    pub fn query_work_request_for(&self, cypher_text: &str) -> Result<WorkRequest> {
        let statement = cypher::parse(cypher_text)?;
        query_work_request_for_statement(&self.system_variables, &statement)
    }

    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn runtime_admission_plan(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<RuntimeAdmissionPlan> {
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let body = statement_body(&statement);
        let streaming_eligible = !matches!(body, cypher::Statement::Explain(_));
        let (is_mutation, estimated_memory_bytes) = match body {
            cypher::Statement::Explain(_) => (false, CONTROL_STATEMENT_MEMORY_BYTES),
            cypher::Statement::SetSystemVariable(_)
            | cypher::Statement::Checkpoint
            | cypher::Statement::BeginTransaction
            | cypher::Statement::Commit
            | cypher::Statement::Rollback => (true, CONTROL_STATEMENT_MEMORY_BYTES),
            _ => {
                let optimized = self.optimized_query_plan(cypher_text, &statement, parameters)?;
                (
                    executor::is_mutation_plan(&optimized.physical_plan)?,
                    executor::estimated_execution_memory_bytes(&optimized.physical_plan),
                )
            }
        };
        Ok(RuntimeAdmissionPlan {
            work_request,
            is_mutation,
            estimated_memory_bytes,
            streaming_eligible,
        })
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.query_with_params_trace_internal(cypher_text, parameters, false, None, None)
            .map(|(output, _)| output)
    }

    pub fn query_with_context(
        &mut self,
        cypher_text: &str,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_context(cypher_text, &BTreeMap::new(), task_context)
    }

    pub fn query_with_params_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_trace_internal(
            cypher_text,
            parameters,
            false,
            None,
            Some(task_context),
        )
        .map(|(output, _)| output)
    }

    pub fn query_with_params_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_trace_internal(
            cypher_text,
            parameters,
            false,
            Some(access_control),
            None,
        )
        .map(|(output, _)| output)
    }

    fn query_with_params_trace_internal(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        capture_trace: bool,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let mut external = executor::NoExternalReadOperator;
        self.query_with_params_trace_and_external_with_context(
            cypher_text,
            parameters,
            capture_trace,
            &mut external,
            access_control,
            task_context,
        )
    }

    pub(crate) fn query_with_params_trace_and_external(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        capture_trace: bool,
        external: &mut dyn executor::ExternalReadOperator,
        access_control: Option<QueryAccessControlContext>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        self.query_with_params_trace_and_external_with_context(
            cypher_text,
            parameters,
            capture_trace,
            external,
            access_control,
            None,
        )
    }

    pub(crate) fn query_with_params_trace_and_external_with_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        capture_trace: bool,
        external: &mut dyn executor::ExternalReadOperator,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let started = std::time::Instant::now();
        query_runtime_checkpoint(task_context)?;
        let statement = cypher::parse(cypher_text)?;
        let body = statement_body(&statement);
        let statement_kind_name = statement_kind(&statement);
        if let cypher::Statement::Explain(explain) = &statement {
            let query_result = self
                .execute_explain_statement(
                    cypher_text,
                    explain,
                    parameters,
                    external,
                    access_control.as_ref(),
                    task_context,
                )
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
                StatementExecutionContext {
                    access_control: access_control.as_ref(),
                    ..StatementExecutionContext::default()
                },
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
            query_runtime_checkpoint(task_context)?;
            query_work_request_for_statement(&self.system_variables, &statement)?;
            let optimized = self.optimized_query_plan_with_access_control(
                cypher_text,
                &statement,
                parameters,
                access_control.as_ref(),
            )?;
            let is_mutation = executor::is_mutation_plan(&optimized.physical_plan)?;
            if is_mutation {
                self.ensure_writable()?;
            }
            let (rows, execution_profile) = if is_mutation {
                query_runtime_checkpoint(task_context)?;
                (
                    executor::execute(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                    )?,
                    None,
                )
            } else {
                let profiled = match task_context {
                    Some(task_context) => {
                        executor::execute_with_output_limits_profile_and_external_and_context_and_memory(
                            &optimized.physical_plan,
                            &mut self.catalog,
                            &mut self.store,
                            parameters,
                            external,
                            self.config.max_read_result_rows,
                            self.config.max_read_result_payload_bytes,
                            task_context,
                            &self.config.execution_memory,
                        )
                    }
                    None => executor::execute_with_output_limits_profile_and_external_and_memory(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                        parameters,
                        external,
                        self.config.max_read_result_rows,
                        self.config.max_read_result_payload_bytes,
                        &self.config.execution_memory,
                    ),
                }?;
                (profiled.rows, Some(profiled.profile))
            };
            if !is_mutation {
                query_runtime_checkpoint(task_context)?;
            }
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
            StatementExecutionContext {
                execution_profile,
                access_control: access_control.as_ref(),
            },
        );
        query_result
    }

    fn execute_explain_statement(
        &mut self,
        cypher_text: &str,
        explain: &cypher::Explain,
        parameters: &BTreeMap<String, Value>,
        external: &mut dyn executor::ExternalReadOperator,
        access_control: Option<&QueryAccessControlContext>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<QueryOutput> {
        query_runtime_checkpoint(task_context)?;
        let work_request =
            query_work_request_for_statement(&self.system_variables, &explain.statement)?;
        let optimized = self.optimized_query_plan_with_access_control(
            cypher_text,
            &explain.statement,
            parameters,
            access_control,
        )?;
        let inner_statement_kind = statement_kind(statement_body(&explain.statement));
        if explain.analyze {
            if executor::is_mutation_plan(&optimized.physical_plan)? {
                return Err(SkeinError::Execution(
                    "EXPLAIN ANALYZE only supports read queries".to_string(),
                ));
            }
            let profiled = match task_context {
                Some(task_context) => {
                    executor::execute_with_output_limits_profile_and_external_and_context_and_memory(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                        parameters,
                        external,
                        self.config.max_read_result_rows,
                        self.config.max_read_result_payload_bytes,
                        task_context,
                        &self.config.execution_memory,
                    )
                }
                None => executor::execute_with_output_limits_profile_and_external_and_memory(
                    &optimized.physical_plan,
                    &mut self.catalog,
                    &mut self.store,
                    parameters,
                    external,
                    self.config.max_read_result_rows,
                    self.config.max_read_result_payload_bytes,
                    &self.config.execution_memory,
                ),
            }?;
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

pub(super) fn query_runtime_checkpoint(
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<()> {
    match task_context {
        Some(task_context) => task_context
            .checkpoint()
            .map_err(|reason| SkeinError::Execution(format!("runtime task stopped: {reason}"))),
        None => Ok(()),
    }
}
