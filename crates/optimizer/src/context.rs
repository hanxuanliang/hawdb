use crate::OptimizerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryFamily {
    GraphRead,
    GraphWrite,
    VectorSearch,
    Schema,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementClass {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainMode {
    None,
    Explain,
    Analyze,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceSink {
    Disabled,
    Collect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceHints {
    pub priority: u8,
    pub max_memory_bytes: Option<u64>,
    pub max_parallelism: usize,
}

impl Default for ResourceHints {
    fn default() -> Self {
        Self {
            priority: 128,
            max_memory_bytes: None,
            max_parallelism: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerContext {
    normalized_query: Option<String>,
    query_family: QueryFamily,
    statement_class: StatementClass,
    resource_hints: ResourceHints,
    explain_mode: ExplainMode,
    plan_cache_key_digest: Option<u64>,
    trace_sink: TraceSink,
    optimizer_config: OptimizerConfig,
}

impl Default for OptimizerContext {
    fn default() -> Self {
        Self {
            normalized_query: None,
            query_family: QueryFamily::GraphRead,
            statement_class: StatementClass::Read,
            resource_hints: ResourceHints::default(),
            explain_mode: ExplainMode::None,
            plan_cache_key_digest: None,
            trace_sink: TraceSink::Collect,
            optimizer_config: OptimizerConfig::default(),
        }
    }
}

impl OptimizerContext {
    pub fn from_config(optimizer_config: OptimizerConfig) -> Self {
        Self {
            optimizer_config,
            ..Self::default()
        }
    }

    pub fn with_normalized_query(mut self, normalized_query: impl Into<String>) -> Self {
        self.normalized_query = Some(normalized_query.into());
        self
    }

    pub fn with_query_family(mut self, query_family: QueryFamily) -> Self {
        self.query_family = query_family;
        self.statement_class = match query_family {
            QueryFamily::GraphWrite | QueryFamily::Schema => StatementClass::Write,
            QueryFamily::GraphRead | QueryFamily::VectorSearch | QueryFamily::System => {
                StatementClass::Read
            }
        };
        self
    }

    pub fn with_resource_hints(mut self, resource_hints: ResourceHints) -> Self {
        self.resource_hints = resource_hints;
        self
    }

    pub fn with_explain_mode(mut self, explain_mode: ExplainMode) -> Self {
        self.explain_mode = explain_mode;
        self
    }

    pub fn with_plan_cache_key_digest(mut self, plan_cache_key_digest: u64) -> Self {
        self.plan_cache_key_digest = Some(plan_cache_key_digest);
        self
    }

    pub fn with_trace_sink(mut self, trace_sink: TraceSink) -> Self {
        self.trace_sink = trace_sink;
        self
    }

    pub fn normalized_query(&self) -> Option<&str> {
        self.normalized_query.as_deref()
    }

    pub fn query_family(&self) -> QueryFamily {
        self.query_family
    }

    pub fn statement_class(&self) -> StatementClass {
        self.statement_class
    }

    pub fn resource_hints(&self) -> &ResourceHints {
        &self.resource_hints
    }

    pub fn explain_mode(&self) -> ExplainMode {
        self.explain_mode
    }

    pub fn plan_cache_key_digest(&self) -> Option<u64> {
        self.plan_cache_key_digest
    }

    pub fn trace_sink(&self) -> TraceSink {
        self.trace_sink
    }

    pub fn optimizer_config(&self) -> &OptimizerConfig {
        &self.optimizer_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_family_derives_statement_class_without_raw_cache_key() {
        let context = OptimizerContext::default()
            .with_query_family(QueryFamily::VectorSearch)
            .with_plan_cache_key_digest(42);

        assert_eq!(context.statement_class(), StatementClass::Read);
        assert_eq!(context.plan_cache_key_digest(), Some(42));
        assert_eq!(context.normalized_query(), None);
    }

    #[test]
    fn resource_hints_are_bounded_by_explicit_parallelism() {
        let context = OptimizerContext::default().with_resource_hints(ResourceHints {
            priority: 200,
            max_memory_bytes: Some(16 * 1024 * 1024),
            max_parallelism: 2,
        });

        assert_eq!(context.resource_hints().max_parallelism, 2);
        assert_eq!(
            context.resource_hints().max_memory_bytes,
            Some(16 * 1024 * 1024)
        );
    }
}
