use super::{
    system_sql, Database, QueryOutput, SlowQueryLogExportOptions, SlowQueryLogRecordSummary,
};
use crate::error::{Result, SkeinError};
use crate::telemetry::QueryTelemetry;
use std::io::Write;
use std::path::Path;

impl Database {
    pub(super) fn record_statement_execution(
        &self,
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        started: std::time::Instant,
        result: std::result::Result<&QueryOutput, &SkeinError>,
        execution_profile: Option<&crate::executor::ReadExecutionProfile>,
    ) {
        let elapsed_micros = started.elapsed().as_micros();
        let execution = match result {
            Ok(output) => system_sql::StatementExecution::completed(
                query_language,
                query_text,
                statement_kind,
                elapsed_micros,
                output.rows.len(),
            ),
            Err(error) => system_sql::StatementExecution::failed(
                query_language,
                query_text,
                statement_kind,
                elapsed_micros,
                error.to_string(),
            ),
        };
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_query(QueryTelemetry {
                query_language,
                statement_kind,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros.min(u64::MAX as u128) as u64,
                row_count: result.map_or(0, |output| output.rows.len()),
            });
        }
        self.statement_summary.borrow_mut().record(execution);

        let Ok(output) = result else {
            return;
        };
        let slow_log_candidate = elapsed_micros >= self.config.slow_query_log_threshold_micros;
        if !slow_log_candidate {
            return;
        }
        self.slow_query_log
            .borrow_mut()
            .push(system_sql::SlowQueryRecord::completed(
                system_sql::SlowQueryCompletion {
                    query_language,
                    query_text,
                    elapsed_micros,
                    row_count: output.rows.len(),
                    success: true,
                    error: None,
                    slow_log_candidate,
                    vector_execution_reports: execution_profile
                        .map(|profile| profile.vector_execution_reports.clone())
                        .unwrap_or_default(),
                },
            ));
    }

    pub fn query_sql(&self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_bounded(sql_text, self.config.max_read_result_rows)
    }

    pub fn query_sql_bounded(
        &self,
        sql_text: &str,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        system_sql::query_sql(
            sql_text,
            max_rows,
            &self.plan_cache.borrow().stats(),
            &self.slow_query_log.borrow().snapshot(),
            &self.statement_summary.borrow().snapshot(),
        )
    }

    pub fn slow_query_log_jsonl(&self) -> Result<String> {
        self.slow_query_log_jsonl_with_options(&SlowQueryLogExportOptions::default())
    }

    pub fn slow_query_log_jsonl_with_options(
        &self,
        options: &SlowQueryLogExportOptions,
    ) -> Result<String> {
        system_sql::slow_query_log_jsonl(
            &self.slow_query_log.borrow().snapshot(),
            options.include_query_text,
        )
    }

    pub fn slow_query_log_snapshot(&self) -> Vec<SlowQueryLogRecordSummary> {
        self.slow_query_log
            .borrow()
            .snapshot()
            .iter()
            .map(system_sql::slow_query_record_summary)
            .collect()
    }

    pub fn write_slow_query_log_jsonl(&self, path: impl AsRef<Path>) -> Result<()> {
        self.write_slow_query_log_jsonl_with_options(path, &SlowQueryLogExportOptions::default())
    }

    pub fn write_slow_query_log_jsonl_with_options(
        &self,
        path: impl AsRef<Path>,
        options: &SlowQueryLogExportOptions,
    ) -> Result<()> {
        let jsonl = self.slow_query_log_jsonl_with_options(options)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(jsonl.as_bytes())?;
        Ok(())
    }
}
