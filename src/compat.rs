use crate::analytics::ProjectedGraph;
use crate::api::{Database, QueryOutput};
use crate::error::{Result, SkeinError};
use crate::executor::Row;
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

const DEFAULT_FLOAT_ABS_TOLERANCE: f64 = 1.0e-9;

#[derive(Debug, Clone, PartialEq)]
pub struct CompatibilityFixture {
    pub name: String,
    pub setup: Vec<CypherFixtureStatement>,
    pub checks: Vec<CompatibilityCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CypherFixtureStatement {
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityCheck {
    Cypher(CypherFixtureCheck),
    ProjectedGraph(ProjectedGraphFixtureCheck),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CypherFixtureCheck {
    pub name: String,
    pub setup_queries: Vec<CypherFixtureStatement>,
    pub statement: CypherFixtureStatement,
    pub expected_rows: ExpectedRows,
    pub expected_error: Option<ExpectedErrorClass>,
    pub effect_query: Option<CypherFixtureStatement>,
    pub effect_expected_rows: Option<ExpectedRows>,
    pub expected_plan_contains: Vec<String>,
    pub tolerance: CompatibilityTolerance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedRows {
    Exact(Vec<Row>),
    Unordered(Vec<Row>),
    RowCount(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedErrorClass {
    Parse,
    Semantic,
    Storage,
    Execution,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompatibilityTolerance {
    pub float_abs: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedGraphFixtureCheck {
    pub name: String,
    pub rel_type: Option<String>,
    pub expected_node_count: usize,
    pub expected_edge_count: usize,
    pub expected_incoming: Vec<(u64, Vec<u64>)>,
    pub expected_communities: Vec<(u64, u64)>,
    pub expected_hierarchical_communities: Vec<(usize, u64, u64)>,
    pub expected_page_rank_scores: Vec<(u64, f64)>,
    pub page_rank_top_node: Option<u64>,
    pub tolerance: CompatibilityTolerance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub fixture: String,
    pub checks: Vec<CompatibilityCheckReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityCheckReport {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityQueryInventory {
    pub name: String,
    pub required_checks: Vec<CompatibilityQueryInventoryItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityQueryInventoryItem {
    pub name: String,
    pub query_family: String,
    pub source: Option<String>,
    pub cypher: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityQueryCallSite {
    pub name: String,
    pub query_family: String,
    pub source: String,
    pub cypher: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityInventoryCoverageReport {
    pub inventory: String,
    pub fixture: String,
    pub required_checks: usize,
    pub covered_checks: usize,
    pub missing_checks: Vec<String>,
    pub extra_fixture_checks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompatibilityInventoryCoveragePolicy {
    pub require_all_required_checks: bool,
    pub allow_extra_fixture_checks: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityInventoryGateReport {
    pub inventory: String,
    pub fixture: String,
    pub decision: CompatibilityCutoverDecision,
    pub required_checks: usize,
    pub covered_checks: usize,
    pub missing_checks: Vec<String>,
    pub extra_fixture_checks: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityShadowReport {
    pub fixture: String,
    pub shadow_engine: String,
    pub primary_checks: Vec<CompatibilityCheckReport>,
    pub shadow_checks: Vec<CompatibilityShadowCheckReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityShadowCheckReport {
    pub name: String,
    pub status: CompatibilityShadowStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityShadowStatus {
    Matched,
    PrimaryOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompatibilityCutoverPolicy {
    pub require_shadow_for_all_checks: bool,
    pub min_matched_checks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityCutoverReport {
    pub fixture: String,
    pub shadow_engine: String,
    pub decision: CompatibilityCutoverDecision,
    pub total_checks: usize,
    pub matched_checks: usize,
    pub primary_only_checks: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityMigrationGateReport {
    pub fixture: String,
    pub inventory: String,
    pub shadow_engine: String,
    pub decision: CompatibilityCutoverDecision,
    pub inventory_decision: CompatibilityCutoverDecision,
    pub shadow_decision: CompatibilityCutoverDecision,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityMigrationGateBundle {
    pub coverage: CompatibilityInventoryCoverageReport,
    pub inventory_gate: CompatibilityInventoryGateReport,
    pub cutover: CompatibilityCutoverReport,
    pub migration_gate: CompatibilityMigrationGateReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityCutoverDecision {
    Ready,
    Blocked,
}

pub trait CompatibilityShadowEngine {
    fn name(&self) -> &str;
    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput>;

    fn project_graph(
        &mut self,
        _check: &ProjectedGraphFixtureCheck,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedGraphShadowOutput {
    pub node_count: usize,
    pub edge_count: usize,
    pub incoming: Vec<(u64, Vec<u64>)>,
    pub communities: Vec<(u64, u64)>,
    pub hierarchical_communities: Vec<(usize, u64, u64)>,
    pub page_rank_scores: Vec<(u64, f64)>,
    pub page_rank_top_node: Option<u64>,
}

pub struct ExternalShadowCommand {
    name: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExternalShadowErrorClass {
    Parse,
    Semantic,
    Storage,
    Execution,
}

impl ExternalShadowCommand {
    pub fn spawn(
        name: impl Into<String>,
        program: impl AsRef<std::ffi::OsStr>,
        args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
    ) -> Result<Self> {
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|error| {
            SkeinError::Execution(format!("failed to spawn external shadow engine: {error}"))
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            SkeinError::Execution("external shadow engine did not expose stdin".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            SkeinError::Execution("external shadow engine did not expose stdout".to_string())
        })?;
        Ok(Self {
            name: name.into(),
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    fn request(&mut self, request: serde_json::Value) -> Result<serde_json::Value> {
        let line = serde_json::to_string(&request).map_err(|error| {
            SkeinError::Execution(format!("failed to encode shadow request: {error}"))
        })?;
        writeln!(self.stdin, "{line}").map_err(|error| {
            SkeinError::Execution(format!(
                "failed to write request to shadow engine '{}': {error}",
                self.name
            ))
        })?;
        self.stdin.flush().map_err(|error| {
            SkeinError::Execution(format!(
                "failed to flush request to shadow engine '{}': {error}",
                self.name
            ))
        })?;

        let mut response = String::new();
        let bytes = self.stdout.read_line(&mut response).map_err(|error| {
            SkeinError::Execution(format!(
                "failed to read response from shadow engine '{}': {error}",
                self.name
            ))
        })?;
        if bytes == 0 {
            return Err(SkeinError::Execution(format!(
                "shadow engine '{}' closed stdout",
                self.name
            )));
        }
        serde_json::from_str(response.trim_end()).map_err(|error| {
            SkeinError::Execution(format!(
                "shadow engine '{}' returned invalid JSON: {error}",
                self.name
            ))
        })
    }
}

impl Drop for ExternalShadowCommand {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl CompatibilityShadowEngine for ExternalShadowCommand {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        let response = self.request(serde_json::json!({
            "op": "execute",
            "cypher": statement.cypher,
            "parameters": json_object_from_parameters(&statement.parameters),
        }))?;
        decode_external_query_response(&self.name, response)
    }

    fn project_graph(
        &mut self,
        check: &ProjectedGraphFixtureCheck,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        let response = self.request(serde_json::json!({
            "op": "project_graph",
            "rel_type": check.rel_type,
        }))?;
        decode_external_projected_graph_response(&self.name, response)
    }
}

impl Default for CompatibilityTolerance {
    fn default() -> Self {
        Self {
            float_abs: DEFAULT_FLOAT_ABS_TOLERANCE,
        }
    }
}

impl Default for CompatibilityCutoverPolicy {
    fn default() -> Self {
        Self {
            require_shadow_for_all_checks: true,
            min_matched_checks: 1,
        }
    }
}

impl Default for CompatibilityInventoryCoveragePolicy {
    fn default() -> Self {
        Self {
            require_all_required_checks: true,
            allow_extra_fixture_checks: true,
        }
    }
}

impl CompatibilityQueryInventoryItem {
    pub fn new(name: impl Into<String>, query_family: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query_family: query_family.into(),
            source: None,
            cypher: None,
        }
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_cypher(mut self, cypher: impl Into<String>) -> Self {
        self.cypher = Some(cypher.into());
        self
    }
}

impl CompatibilityQueryCallSite {
    pub fn new(
        name: impl Into<String>,
        query_family: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            query_family: query_family.into(),
            source: source.into(),
            cypher: None,
        }
    }

    pub fn with_cypher(mut self, cypher: impl Into<String>) -> Self {
        self.cypher = Some(cypher.into());
        self
    }
}

pub fn build_compatibility_query_inventory(
    name: impl Into<String>,
    call_sites: impl IntoIterator<Item = CompatibilityQueryCallSite>,
) -> Result<CompatibilityQueryInventory> {
    let name = name.into();
    if name.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "compatibility query inventory name must not be empty".to_string(),
        ));
    }

    let mut seen = BTreeMap::new();
    let mut required_checks = Vec::new();
    for call_site in call_sites {
        let check_name = call_site.name.trim();
        if check_name.is_empty() {
            return Err(SkeinError::Semantic(
                "compatibility query call site name must not be empty".to_string(),
            ));
        }
        let query_family = call_site.query_family.trim();
        if query_family.is_empty() {
            return Err(SkeinError::Semantic(format!(
                "compatibility query call site '{check_name}' has no query family"
            )));
        }
        let source = call_site.source.trim();
        if source.is_empty() {
            return Err(SkeinError::Semantic(format!(
                "compatibility query call site '{check_name}' has no source"
            )));
        }
        if let Some(previous_source) = seen.insert(check_name.to_string(), source.to_string()) {
            return Err(SkeinError::Semantic(format!(
                "duplicate compatibility query call site '{check_name}' from '{previous_source}' and '{source}'"
            )));
        }

        let mut item =
            CompatibilityQueryInventoryItem::new(check_name.to_string(), query_family.to_string())
                .with_source(source.to_string());
        if let Some(cypher) = call_site.cypher {
            let cypher = cypher.trim();
            if !cypher.is_empty() {
                item = item.with_cypher(cypher.to_string());
            }
        }
        required_checks.push(item);
    }

    Ok(CompatibilityQueryInventory {
        name,
        required_checks,
    })
}

pub fn build_compatibility_query_inventory_from_json_str(
    artifact: &str,
) -> Result<CompatibilityQueryInventory> {
    let value = serde_json::from_str(artifact).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse compatibility query inventory artifact: {error}"
        ))
    })?;
    build_compatibility_query_inventory_from_json(&value)
}

pub fn build_compatibility_query_inventory_from_json(
    artifact: &serde_json::Value,
) -> Result<CompatibilityQueryInventory> {
    let object = artifact.as_object().ok_or_else(|| {
        SkeinError::Semantic(
            "compatibility query inventory artifact must be a JSON object".to_string(),
        )
    })?;
    let name = required_string_field(object, "name")?;
    if let Some(call_sites) = object.get("call_sites") {
        return build_compatibility_query_inventory(name, call_sites_from_json(call_sites)?);
    }
    if let Some(required_checks) = object.get("required_checks") {
        return build_compatibility_query_inventory_from_items(
            name.to_string(),
            inventory_items_from_json(required_checks)?,
        );
    }
    Err(SkeinError::Semantic(
        "compatibility query inventory artifact must contain 'call_sites' or 'required_checks'"
            .to_string(),
    ))
}

pub fn compatibility_query_inventory_to_json(
    inventory: &CompatibilityQueryInventory,
) -> serde_json::Value {
    serde_json::json!({
        "name": inventory.name,
        "required_checks": inventory
            .required_checks
            .iter()
            .map(inventory_item_to_json)
            .collect::<Vec<_>>(),
    })
}

pub fn compatibility_inventory_coverage_report_to_json(
    report: &CompatibilityInventoryCoverageReport,
) -> serde_json::Value {
    serde_json::json!({
        "inventory": report.inventory,
        "fixture": report.fixture,
        "required_checks": report.required_checks,
        "covered_checks": report.covered_checks,
        "missing_checks": report.missing_checks,
        "extra_fixture_checks": report.extra_fixture_checks,
    })
}

pub fn compatibility_inventory_gate_report_to_json(
    report: &CompatibilityInventoryGateReport,
) -> serde_json::Value {
    serde_json::json!({
        "inventory": report.inventory,
        "fixture": report.fixture,
        "decision": compatibility_cutover_decision_as_str(report.decision),
        "required_checks": report.required_checks,
        "covered_checks": report.covered_checks,
        "missing_checks": report.missing_checks,
        "extra_fixture_checks": report.extra_fixture_checks,
        "blockers": report.blockers,
    })
}

pub fn compatibility_cutover_report_to_json(
    report: &CompatibilityCutoverReport,
) -> serde_json::Value {
    serde_json::json!({
        "fixture": report.fixture,
        "shadow_engine": report.shadow_engine,
        "decision": compatibility_cutover_decision_as_str(report.decision),
        "total_checks": report.total_checks,
        "matched_checks": report.matched_checks,
        "primary_only_checks": report.primary_only_checks,
        "blockers": report.blockers,
    })
}

pub fn compatibility_migration_gate_report_to_json(
    report: &CompatibilityMigrationGateReport,
) -> serde_json::Value {
    serde_json::json!({
        "fixture": report.fixture,
        "inventory": report.inventory,
        "shadow_engine": report.shadow_engine,
        "decision": compatibility_cutover_decision_as_str(report.decision),
        "inventory_decision": compatibility_cutover_decision_as_str(report.inventory_decision),
        "shadow_decision": compatibility_cutover_decision_as_str(report.shadow_decision),
        "blockers": report.blockers,
    })
}

pub fn compatibility_migration_gate_bundle_to_json(
    bundle: &CompatibilityMigrationGateBundle,
) -> serde_json::Value {
    serde_json::json!({
        "coverage": compatibility_inventory_coverage_report_to_json(&bundle.coverage),
        "inventory_gate": compatibility_inventory_gate_report_to_json(&bundle.inventory_gate),
        "cutover": compatibility_cutover_report_to_json(&bundle.cutover),
        "migration_gate": compatibility_migration_gate_report_to_json(&bundle.migration_gate),
    })
}

fn compatibility_cutover_decision_as_str(decision: CompatibilityCutoverDecision) -> &'static str {
    match decision {
        CompatibilityCutoverDecision::Ready => "ready",
        CompatibilityCutoverDecision::Blocked => "blocked",
    }
}

fn build_compatibility_query_inventory_from_items(
    name: String,
    items: Vec<CompatibilityQueryInventoryItem>,
) -> Result<CompatibilityQueryInventory> {
    if name.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "compatibility query inventory name must not be empty".to_string(),
        ));
    }
    let mut seen = BTreeMap::new();
    let mut required_checks = Vec::new();
    for item in items {
        let check_name = item.name.trim();
        if check_name.is_empty() {
            return Err(SkeinError::Semantic(
                "compatibility query inventory item name must not be empty".to_string(),
            ));
        }
        let query_family = item.query_family.trim();
        if query_family.is_empty() {
            return Err(SkeinError::Semantic(format!(
                "compatibility query inventory item '{check_name}' has no query family"
            )));
        }
        let source = item
            .source
            .as_deref()
            .map(str::trim)
            .filter(|source| !source.is_empty())
            .map(str::to_string);
        let source_label = source.as_deref().unwrap_or("<unknown>");
        if let Some(previous_source) = seen.insert(check_name.to_string(), source_label.to_string())
        {
            return Err(SkeinError::Semantic(format!(
                "duplicate compatibility query inventory item '{check_name}' from '{previous_source}' and '{source_label}'"
            )));
        }
        let cypher = item
            .cypher
            .as_deref()
            .map(str::trim)
            .filter(|cypher| !cypher.is_empty())
            .map(str::to_string);
        required_checks.push(CompatibilityQueryInventoryItem {
            name: check_name.to_string(),
            query_family: query_family.to_string(),
            source,
            cypher,
        });
    }
    Ok(CompatibilityQueryInventory {
        name: name.trim().to_string(),
        required_checks,
    })
}

fn call_sites_from_json(value: &serde_json::Value) -> Result<Vec<CompatibilityQueryCallSite>> {
    let call_sites = value.as_array().ok_or_else(|| {
        SkeinError::Semantic(
            "compatibility query inventory 'call_sites' must be an array".to_string(),
        )
    })?;
    call_sites
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let object = value.as_object().ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "compatibility query call site at index {index} must be a JSON object"
                ))
            })?;
            let mut call_site = CompatibilityQueryCallSite::new(
                required_string_field(object, "name")?,
                required_string_field(object, "query_family")?,
                required_string_field(object, "source")?,
            );
            if let Some(cypher) = optional_string_field(object, "cypher")? {
                call_site = call_site.with_cypher(cypher);
            }
            Ok(call_site)
        })
        .collect()
}

fn inventory_items_from_json(
    value: &serde_json::Value,
) -> Result<Vec<CompatibilityQueryInventoryItem>> {
    let items = value.as_array().ok_or_else(|| {
        SkeinError::Semantic(
            "compatibility query inventory 'required_checks' must be an array".to_string(),
        )
    })?;
    items
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let object = value.as_object().ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "compatibility query inventory item at index {index} must be a JSON object"
                ))
            })?;
            let mut item = CompatibilityQueryInventoryItem::new(
                required_string_field(object, "name")?,
                required_string_field(object, "query_family")?,
            );
            if let Some(source) = optional_string_field(object, "source")? {
                item = item.with_source(source);
            }
            if let Some(cypher) = optional_string_field(object, "cypher")? {
                item = item.with_cypher(cypher);
            }
            Ok(item)
        })
        .collect()
}

fn inventory_item_to_json(item: &CompatibilityQueryInventoryItem) -> serde_json::Value {
    let mut object = serde_json::Map::from_iter([
        (
            "name".to_string(),
            serde_json::Value::String(item.name.clone()),
        ),
        (
            "query_family".to_string(),
            serde_json::Value::String(item.query_family.clone()),
        ),
    ]);
    if let Some(source) = &item.source {
        object.insert(
            "source".to_string(),
            serde_json::Value::String(source.clone()),
        );
    }
    if let Some(cypher) = &item.cypher {
        object.insert(
            "cypher".to_string(),
            serde_json::Value::String(cypher.clone()),
        );
    }
    serde_json::Value::Object(object)
}

fn required_string_field<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<&'a str> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "compatibility query inventory artifact field '{field}' must be a string"
            ))
        })
}

fn optional_string_field(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<String>> {
    match object.get(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "compatibility query inventory artifact field '{field}' must be a string"
                ))
            }),
    }
}

fn json_object_from_parameters(parameters: &BTreeMap<String, Value>) -> serde_json::Value {
    serde_json::Value::Object(
        parameters
            .iter()
            .map(|(key, value)| (key.clone(), json_from_value(value)))
            .collect(),
    )
}

fn json_from_value(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::Value::Number((*value).into()),
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(value) => serde_json::Value::String(value.clone()),
        Value::List(values) => {
            serde_json::Value::Array(values.iter().map(json_from_value).collect())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_from_value(value)))
                .collect(),
        ),
    }
}

fn decode_external_query_response(
    engine_name: &str,
    response: serde_json::Value,
) -> Result<QueryOutput> {
    if let Some(error) = response.get("error") {
        return Err(error_from_external_response(engine_name, error));
    }
    let ok = response.get("ok").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' response missing 'ok' or 'error'"
        ))
    })?;
    let rows = ok.get("rows").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' query response missing rows"
        ))
    })?;
    Ok(QueryOutput {
        rows: rows_from_json(engine_name, rows)?,
    })
}

fn decode_external_projected_graph_response(
    engine_name: &str,
    response: serde_json::Value,
) -> Result<Option<ProjectedGraphShadowOutput>> {
    if response
        .get("primary_only")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    if let Some(error) = response.get("error") {
        return Err(error_from_external_response(engine_name, error));
    }
    let ok = response.get("ok").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' projected graph response missing 'ok', 'error', or primary_only"
        ))
    })?;
    Ok(Some(ProjectedGraphShadowOutput {
        node_count: required_usize(engine_name, ok, "node_count")?,
        edge_count: required_usize(engine_name, ok, "edge_count")?,
        incoming: tuple_vec_u64_list(engine_name, ok, "incoming")?,
        communities: tuple_vec_u64_u64(engine_name, ok, "communities")?,
        hierarchical_communities: tuple_vec_usize_u64_u64(
            engine_name,
            ok,
            "hierarchical_communities",
        )?,
        page_rank_scores: tuple_vec_u64_f64(engine_name, ok, "page_rank_scores")?,
        page_rank_top_node: optional_u64(engine_name, ok, "page_rank_top_node")?,
    }))
}

fn error_from_external_response(engine_name: &str, error: &serde_json::Value) -> SkeinError {
    let class = error
        .get("class")
        .and_then(serde_json::Value::as_str)
        .and_then(ExternalShadowErrorClass::decode)
        .unwrap_or(ExternalShadowErrorClass::Execution);
    let message = error
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("external shadow engine error");
    let message = format!("shadow engine '{engine_name}': {message}");
    match class {
        ExternalShadowErrorClass::Parse => SkeinError::Parse(message),
        ExternalShadowErrorClass::Semantic => SkeinError::Semantic(message),
        ExternalShadowErrorClass::Storage => SkeinError::Storage(message),
        ExternalShadowErrorClass::Execution => SkeinError::Execution(message),
    }
}

impl ExternalShadowErrorClass {
    fn decode(value: &str) -> Option<Self> {
        match value {
            "parse" => Some(Self::Parse),
            "semantic" => Some(Self::Semantic),
            "storage" => Some(Self::Storage),
            "execution" => Some(Self::Execution),
            _ => None,
        }
    }
}

fn rows_from_json(engine_name: &str, value: &serde_json::Value) -> Result<Vec<Row>> {
    let rows = value.as_array().ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' rows must be an array"
        ))
    })?;
    rows.iter()
        .map(|row| row_from_json(engine_name, row))
        .collect()
}

fn row_from_json(engine_name: &str, value: &serde_json::Value) -> Result<Row> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' row must be an object"
        ))
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(engine_name, value)?)))
        .collect()
}

fn value_from_json(engine_name: &str, value: &serde_json::Value) -> Result<Value> {
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
                    "shadow engine '{engine_name}' returned unsupported number: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| value_from_json(engine_name, value))
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(engine_name, value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

fn required_usize(engine_name: &str, object: &serde_json::Value, field: &str) -> Result<usize> {
    required_u64(engine_name, object, field).and_then(|value| {
        usize::try_from(value).map_err(|_| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' exceeds usize"
            ))
        })
    })
}

fn required_u64(engine_name: &str, object: &serde_json::Value, field: &str) -> Result<u64> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' must be a u64"
            ))
        })
}

fn optional_u64(engine_name: &str, object: &serde_json::Value, field: &str) -> Result<Option<u64>> {
    match object.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' must be a u64 or null"
            ))
        }),
    }
}

fn tuple_vec_u64_list(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(u64, Vec<u64>)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 2)?;
            let key = item[0]
                .as_u64()
                .ok_or_else(|| tuple_error(engine_name, field))?;
            let values = item[1]
                .as_array()
                .ok_or_else(|| tuple_error(engine_name, field))?
                .iter()
                .map(|value| {
                    value
                        .as_u64()
                        .ok_or_else(|| tuple_error(engine_name, field))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((key, values))
        })
        .collect()
}

fn tuple_vec_u64_u64(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(u64, u64)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 2)?;
            Ok((
                item[0]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
                item[1]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
            ))
        })
        .collect()
}

fn tuple_vec_usize_u64_u64(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(usize, u64, u64)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 3)?;
            Ok((
                usize::try_from(
                    item[0]
                        .as_u64()
                        .ok_or_else(|| tuple_error(engine_name, field))?,
                )
                .map_err(|_| tuple_error(engine_name, field))?,
                item[1]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
                item[2]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
            ))
        })
        .collect()
}

fn tuple_vec_u64_f64(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(u64, f64)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 2)?;
            Ok((
                item[0]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
                item[1]
                    .as_f64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
            ))
        })
        .collect()
}

fn required_array<'a>(
    engine_name: &str,
    object: &'a serde_json::Value,
    field: &str,
) -> Result<&'a Vec<serde_json::Value>> {
    object
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' must be an array"
            ))
        })
}

fn tuple_array<'a>(
    engine_name: &str,
    field: &str,
    value: &'a serde_json::Value,
    len: usize,
) -> Result<&'a Vec<serde_json::Value>> {
    let values = value
        .as_array()
        .ok_or_else(|| tuple_error(engine_name, field))?;
    if values.len() != len {
        return Err(tuple_error(engine_name, field));
    }
    Ok(values)
}

fn tuple_error(engine_name: &str, field: &str) -> SkeinError {
    SkeinError::Execution(format!(
        "shadow engine '{engine_name}' field '{field}' has invalid tuple shape"
    ))
}

impl CypherFixtureStatement {
    pub fn new(cypher: impl Into<String>) -> Self {
        Self {
            cypher: cypher.into(),
            parameters: BTreeMap::new(),
        }
    }

    pub fn with_parameters(cypher: impl Into<String>, parameters: BTreeMap<String, Value>) -> Self {
        Self {
            cypher: cypher.into(),
            parameters,
        }
    }
}

impl CypherFixtureCheck {
    pub fn expect_rows(
        name: impl Into<String>,
        statement: CypherFixtureStatement,
        expected_rows: ExpectedRows,
    ) -> Self {
        Self {
            name: name.into(),
            setup_queries: Vec::new(),
            statement,
            expected_rows,
            expected_error: None,
            effect_query: None,
            effect_expected_rows: None,
            expected_plan_contains: Vec::new(),
            tolerance: CompatibilityTolerance::default(),
        }
    }

    pub fn expect_error(
        name: impl Into<String>,
        statement: CypherFixtureStatement,
        expected_error: ExpectedErrorClass,
    ) -> Self {
        Self {
            name: name.into(),
            setup_queries: Vec::new(),
            statement,
            expected_rows: ExpectedRows::RowCount(0),
            expected_error: Some(expected_error),
            effect_query: None,
            effect_expected_rows: None,
            expected_plan_contains: Vec::new(),
            tolerance: CompatibilityTolerance::default(),
        }
    }

    pub fn with_plan_contains(mut self, expected_plan_contains: Vec<String>) -> Self {
        self.expected_plan_contains = expected_plan_contains;
        self
    }

    pub fn with_tolerance(mut self, tolerance: CompatibilityTolerance) -> Self {
        self.tolerance = tolerance;
        self
    }

    pub fn with_setup_query(mut self, setup_query: CypherFixtureStatement) -> Self {
        self.setup_queries.push(setup_query);
        self
    }

    pub fn with_effect_query(
        mut self,
        effect_query: CypherFixtureStatement,
        effect_expected_rows: ExpectedRows,
    ) -> Self {
        self.effect_query = Some(effect_query);
        self.effect_expected_rows = Some(effect_expected_rows);
        self
    }
}

pub fn nowledge_memory_core_fixture() -> CompatibilityFixture {
    CompatibilityFixture {
        name: "nowledge-memory-core".to_string(),
        setup: vec![
            CypherFixtureStatement::new("CREATE NODE LABEL Memory"),
            CypherFixtureStatement::new("CREATE NODE LABEL Entity"),
            CypherFixtureStatement::new("CREATE RELATIONSHIP TYPE MENTIONS"),
            CypherFixtureStatement::new("CREATE RELATIONSHIP TYPE EVOLVES"),
            CypherFixtureStatement::new("CREATE INDEX ON :Memory(id)"),
            CypherFixtureStatement::new(
                "MERGE (:Memory {id: 1, title: 'Graph foundations', kind: 'note', status: null, importance: 0.9, memory_count: 0, is_crystal: false, decay_score_cached: 0.2})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust', aliases: ['Ferris', 'Rustacean']})",
            ),
            CypherFixtureStatement::new(
                "MERGE (:Memory {id: 2, title: 'Runtime strategy', kind: 'note', importance: 0.4, is_crystal: false, decay_score_cached: 0.8})-[:MENTIONS {weight: 4}]->(:Entity {id: 11, name: 'Cypher'})",
            ),
            CypherFixtureStatement::new(
                "MERGE (:Memory {id: 2, title: 'Runtime strategy', kind: 'note'})-[:EVOLVES]->(:Memory {id: 3, title: 'Cloud projection', kind: 'decision', is_crystal: true})",
            ),
        ],
        checks: vec![
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "parameterized lookup uses index",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
                        BTreeMap::from([("id".to_string(), Value::Int(1))]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "title",
                        Value::String("Graph foundations".to_string()),
                    )])]),
                )
                .with_plan_contains(vec!["IndexNodeSeek".to_string()]),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "whole node projection",
                CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory {id: $memory_id}) RETURN m",
                    BTreeMap::from([("memory_id".to_string(), Value::Int(1))]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "source provenance endpoint existence",
                CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory {id: $memory_id}), (e:Entity {id: $source_id}) RETURN count(m)",
                    BTreeMap::from([
                        ("memory_id".to_string(), Value::Int(1)),
                        ("source_id".to_string(), Value::Int(10)),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(m)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity relationship endpoint existence",
                CypherFixtureStatement::with_parameters(
                    "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
                    BTreeMap::from([
                        ("source_entity_id".to_string(), Value::Int(10)),
                        ("target_entity_id".to_string(), Value::Int(11)),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("source.id", Value::Int(10)),
                    ("target.id", Value::Int(11)),
                ])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source memory count increment",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1}) SET m.memory_count = m.memory_count + 1",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1}) RETURN m.memory_count AS count",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("count", Value::Int(1))])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory access counter touch",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id = 2 SET m.access_count = COALESCE(m.access_count, 0) + 1, m.last_accessed_at = 42",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 2}) RETURN m.access_count AS count, m.last_accessed_at AS last_accessed_at",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("count", Value::Int(1)),
                        ("last_accessed_at", Value::Int(42)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "null predicate",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE m.status IS NULL RETURN m.id AS id ORDER BY id ASC",
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([("id", Value::Int(1))]),
                    compatibility_row([("id", Value::Int(2))]),
                    compatibility_row([("id", Value::Int(3))]),
                ]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity alias list lookup",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity) WHERE list_contains(e.aliases, $name) RETURN e.id AS id",
                    BTreeMap::from([(
                        "name".to_string(),
                        Value::String("Ferris".to_string()),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("id", Value::Int(10))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "current timestamp write",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 3}) SET m.updated_at = CURRENT_TIMESTAMP()",
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "timestamp cutoff predicate",
                CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory) WHERE m.updated_at > timestamp($cutoff) RETURN count(m) AS total",
                    BTreeMap::from([(
                        "cutoff".to_string(),
                        Value::String("1970-01-01T00:00:00".to_string()),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("total", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "cast timestamp cleanup coverage predicate",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE (m.is_crystal IS NULL OR m.is_crystal = false) AND (m.is_latest IS NULL OR m.is_latest = true) AND (m.lifecycle_state IS NULL OR m.lifecycle_state = 'active') AND m.created_at IS NOT NULL AND m.created_at >= CAST($recent_7d AS TIMESTAMP) RETURN count(m) AS total",
                        BTreeMap::from([(
                            "recent_7d".to_string(),
                            Value::String("1970-01-01T00:00:00".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("total", Value::Int(1))])]),
                )
                .with_setup_query(CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory {id: 1}) SET m.created_at = timestamp($created_at)",
                    BTreeMap::from([(
                        "created_at".to_string(),
                        Value::String("1970-01-02T00:00:00".to_string()),
                    )]),
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "analyzable corpus max updated fingerprint",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE (m.is_crystal IS NULL OR m.is_crystal = false) AND (m.is_latest IS NULL OR m.is_latest = true) AND (m.lifecycle_state IS NULL OR m.lifecycle_state = 'active') RETURN count(m), max(m.updated_at)",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("count(m)", Value::Int(2)),
                        ("max(m.updated_at)", Value::Int(172800000000000)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory {id: 1}) SET m.updated_at = timestamp($updated_at)",
                    BTreeMap::from([(
                        "updated_at".to_string(),
                        Value::String("1970-01-02T00:00:00".to_string()),
                    )]),
                ))
                .with_setup_query(CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory {id: 2}) SET m.updated_at = timestamp($updated_at)",
                    BTreeMap::from([(
                        "updated_at".to_string(),
                        Value::String("1970-01-03T00:00:00".to_string()),
                    )]),
                )),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "list predicate and pagination",
                CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory) WHERE m.id IN [1, $id, 3] RETURN m.title AS title ORDER BY title DESC SKIP 1 LIMIT 1",
                    BTreeMap::from([("id".to_string(), Value::Int(2))]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([(
                    "title",
                    Value::String("Graph foundations".to_string()),
                )])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "grouped aggregation",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) RETURN m.kind AS kind, count(*) AS total ORDER BY total DESC, kind ASC",
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([
                        ("kind", Value::String("note".to_string())),
                        ("total", Value::Int(2)),
                    ]),
                    compatibility_row([
                        ("kind", Value::String("decision".to_string())),
                        ("total", Value::Int(1)),
                    ]),
                ]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "health average read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE m.is_crystal = false RETURN count(m) AS total, avg(m.decay_score_cached) AS avg_decay",
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("total", Value::Int(2)),
                    ("avg_decay", Value::Float(0.5)),
                ])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "health stale count read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE m.is_crystal = false AND m.decay_score_cached < 0.5 RETURN count(m)",
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(m)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity digest recent memory read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.created_at >= $cutoff AND (m.is_crystal IS NULL OR m.is_crystal = false) RETURN m.id, m.title, m.unit_type, m.importance, m.created_at ORDER BY m.created_at DESC LIMIT 20",
                        BTreeMap::from([(
                            "cutoff".to_string(),
                            Value::Int(1_000_000_000_000_000),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("m.id", Value::String("digest-memory-1".to_string())),
                        ("m.title", Value::String("Digest Memory".to_string())),
                        ("m.unit_type", Value::String("fact".to_string())),
                        ("m.importance", Value::Float(0.7)),
                        ("m.created_at", Value::Int(2_000_000_000_000_000)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-memory-1', title: 'Digest Memory', unit_type: 'fact', importance: 0.7, is_crystal: false, created_at: 2000000000000000})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-memory-crystal', title: 'Digest Crystal', unit_type: 'context', importance: 0.9, is_crystal: true, created_at: 2100000000000000})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['digest-memory-1', 'digest-memory-crystal'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity digest recent evolves read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (newer:Memory)-[e:EVOLVES]->(older:Memory) WHERE e.created_at >= $cutoff RETURN newer.id, newer.title, older.id, older.title, e.content_relation, e.created_at ORDER BY e.created_at DESC LIMIT 10",
                        BTreeMap::from([("cutoff".to_string(), Value::Int(1000))]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("newer.id", Value::String("digest-evolves-newer".to_string())),
                        ("newer.title", Value::String("Newer Digest".to_string())),
                        ("older.id", Value::String("digest-evolves-older".to_string())),
                        ("older.title", Value::String("Older Digest".to_string())),
                        ("e.content_relation", Value::String("updates".to_string())),
                        ("e.created_at", Value::Int(3000)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-evolves-newer', title: 'Newer Digest'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-evolves-older', title: 'Older Digest'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-evolves-old-newer', title: 'Old Newer Digest'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-evolves-old-older', title: 'Old Older Digest'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (newer:Memory {id: 'digest-evolves-newer'}), (older:Memory {id: 'digest-evolves-older'}) CREATE (newer)-[:EVOLVES {content_relation: 'updates', created_at: 3000}]->(older)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (newer:Memory {id: 'digest-evolves-old-newer'}), (older:Memory {id: 'digest-evolves-old-older'}) CREATE (newer)-[:EVOLVES {content_relation: 'older', created_at: 500}]->(older)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['digest-evolves-newer', 'digest-evolves-older', 'digest-evolves-old-newer', 'digest-evolves-old-older'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(4),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity digest recent crystal read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (c:Memory) WHERE c.is_crystal = true AND c.created_at >= $cutoff RETURN c.id, c.title, c.created_at ORDER BY c.created_at DESC LIMIT 5",
                        BTreeMap::from([("cutoff".to_string(), Value::Int(1000))]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("c.id", Value::String("digest-crystal-1".to_string())),
                        ("c.title", Value::String("Digest Crystal One".to_string())),
                        ("c.created_at", Value::Int(4000)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-crystal-1', title: 'Digest Crystal One', is_crystal: true, created_at: 4000})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'digest-crystal-old', title: 'Digest Crystal Old', is_crystal: true, created_at: 500})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['digest-crystal-1', 'digest-crystal-old'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity digest recent source read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) WHERE s.created_at >= $cutoff RETURN s.id, s.original_name, s.source_type, s.lifecycle_state, s.memory_count ORDER BY s.created_at DESC LIMIT 10",
                        BTreeMap::from([("cutoff".to_string(), Value::Int(1000))]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("s.id", Value::String("digest-source-1".to_string())),
                        ("s.original_name", Value::String("Digest Source".to_string())),
                        ("s.source_type", Value::String("file".to_string())),
                        ("s.lifecycle_state", Value::String("indexed".to_string())),
                        ("s.memory_count", Value::Int(2)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'digest-source-1', original_name: 'Digest Source', source_type: 'file', lifecycle_state: 'indexed', memory_count: 2, created_at: 2000})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'digest-source-old', original_name: 'Old Digest Source', source_type: 'file', lifecycle_state: 'parsed', memory_count: 1, created_at: 500})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source) WHERE s.id IN ['digest-source-1', 'digest-source-old'] DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task crystal memory read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.created_at >= $cutoff AND (m.is_crystal IS NULL OR m.is_crystal = false) RETURN m.id, m.title, m.unit_type, m.importance, m.created_at ORDER BY m.created_at DESC LIMIT 30",
                        BTreeMap::from([(
                            "cutoff".to_string(),
                            Value::Int(1_000_000_000_000_000),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("m.id", Value::String("task-crystal-memory-1".to_string())),
                        ("m.title", Value::String("Task Crystal Candidate".to_string())),
                        ("m.unit_type", Value::String("fact".to_string())),
                        ("m.importance", Value::Float(0.8)),
                        ("m.created_at", Value::Int(2_200_000_000_000_000)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-crystal-memory-1', title: 'Task Crystal Candidate', unit_type: 'fact', importance: 0.8, is_crystal: false, created_at: 2200000000000000})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-crystal-memory-old', title: 'Old Task Crystal Candidate', unit_type: 'fact', importance: 0.2, is_crystal: false, created_at: 500})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['task-crystal-memory-1', 'task-crystal-memory-old'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task crystallized source id read",
                    CypherFixtureStatement::new(
                        "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(s:Memory) RETURN DISTINCT s.id",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "s.id",
                        Value::String("task-crystallized-source".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-crystallized-crystal', is_crystal: true})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-crystallized-source', is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (c:Memory {id: 'task-crystallized-crystal'}), (s:Memory {id: 'task-crystallized-source'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['task-crystallized-crystal', 'task-crystallized-source'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task challenge edge read",
                    CypherFixtureStatement::new(
                        "MATCH (newer:Memory)-[e:EVOLVES {content_relation: 'challenges'}]->(older:Memory) RETURN newer.id, newer.title, older.id, older.title, e.created_at ORDER BY e.created_at DESC LIMIT 10",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("newer.id", Value::String("task-challenge-newer".to_string())),
                        ("newer.title", Value::String("Challenge Newer".to_string())),
                        ("older.id", Value::String("task-challenge-older".to_string())),
                        ("older.title", Value::String("Challenge Older".to_string())),
                        ("e.created_at", Value::Int(2300)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-challenge-newer', title: 'Challenge Newer'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-challenge-older', title: 'Challenge Older'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (newer:Memory {id: 'task-challenge-newer'}), (older:Memory {id: 'task-challenge-older'}) CREATE (newer)-[:EVOLVES {content_relation: 'challenges', created_at: 2300}]->(older)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['task-challenge-newer', 'task-challenge-older'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task decision memory read",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.unit_type = 'decision' RETURN m.id, m.title, m.created_at, m.importance ORDER BY m.created_at DESC LIMIT 20",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("m.id", Value::String("task-decision-1".to_string())),
                        ("m.title", Value::String("Task Decision".to_string())),
                        ("m.created_at", Value::Int(2400)),
                        ("m.importance", Value::Float(0.9)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-decision-1', title: 'Task Decision', unit_type: 'decision', created_at: 2400, importance: 0.9})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'task-decision-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task source attention read",
                    CypherFixtureStatement::new(
                        "MATCH (s:Source) WHERE s.lifecycle_state = 'ingested' OR s.lifecycle_state = 'parsed' OR s.lifecycle_state = 'chunked' OR s.lifecycle_state = 'error' RETURN s.id, s.original_name, s.lifecycle_state, s.memory_count, s.created_at ORDER BY s.created_at ASC LIMIT 15",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("s.id", Value::String("task-source-attention-1".to_string())),
                        (
                            "s.original_name",
                            Value::String("Task Source Attention".to_string()),
                        ),
                        ("s.lifecycle_state", Value::String("parsed".to_string())),
                        ("s.memory_count", Value::Int(3)),
                        ("s.created_at", Value::Int(2500)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'task-source-attention-1', original_name: 'Task Source Attention', lifecycle_state: 'parsed', memory_count: 3, created_at: 2500})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'task-source-attention-done', original_name: 'Task Source Done', lifecycle_state: 'indexed', memory_count: 1, created_at: 2400})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source) WHERE s.id IN ['task-source-attention-1', 'task-source-attention-done'] DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task evolves cluster read",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory)-[:EVOLVES]-(other:Memory) WHERE (m.is_crystal IS NULL OR m.is_crystal = false) AND (other.is_crystal IS NULL OR other.is_crystal = false) RETURN m.id, m.title, m.unit_type, count(DISTINCT other) as neighbor_count ORDER BY neighbor_count DESC LIMIT 30",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("m.id", Value::String("task-evolves-cluster-a".to_string())),
                            (
                                "m.title",
                                Value::String("Task Evolves Cluster A".to_string()),
                            ),
                            ("m.unit_type", Value::String("fact".to_string())),
                            ("neighbor_count", Value::Int(1)),
                        ]),
                        compatibility_row([
                            ("m.id", Value::String("task-evolves-cluster-b".to_string())),
                            (
                                "m.title",
                                Value::String("Task Evolves Cluster B".to_string()),
                            ),
                            ("m.unit_type", Value::String("fact".to_string())),
                            ("neighbor_count", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-evolves-cluster-a', title: 'Task Evolves Cluster A', unit_type: 'fact', is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-evolves-cluster-b', title: 'Task Evolves Cluster B', unit_type: 'fact', is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Memory {id: 'task-evolves-cluster-a'}), (b:Memory {id: 'task-evolves-cluster-b'}) CREATE (a)-[:EVOLVES]->(b)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['task-evolves-cluster-a', 'task-evolves-cluster-b'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task oldest crystal read",
                    CypherFixtureStatement::new(
                        "MATCH (c:Memory) WHERE c.is_crystal = true RETURN c.id, c.title, c.created_at, c.last_evaluated_at, c.review_status ORDER BY c.created_at ASC LIMIT 10",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("c.id", Value::Int(3)),
                            ("c.title", Value::String("Cloud projection".to_string())),
                            ("c.created_at", Value::Null),
                            ("c.last_evaluated_at", Value::Null),
                            ("c.review_status", Value::Null),
                        ]),
                        compatibility_row([
                            ("c.id", Value::String("task-oldest-crystal-1".to_string())),
                            ("c.title", Value::String("Task Oldest Crystal".to_string())),
                            ("c.created_at", Value::Int(-1000)),
                            ("c.last_evaluated_at", Value::Int(-500)),
                            ("c.review_status", Value::String("pending".to_string())),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-oldest-crystal-1', title: 'Task Oldest Crystal', is_crystal: true, created_at: -1000, last_evaluated_at: -500, review_status: 'pending'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'task-oldest-crystal-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "activity task stale crystal source read",
                    CypherFixtureStatement::new(
                        "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(src:Memory) MATCH (src)-[:EVOLVES]-(newer:Memory) WHERE newer.created_at > c.created_at AND c.review_status <> 'dismissed' RETURN c.id, c.title, newer.id, newer.title, newer.created_at, c.review_status ORDER BY newer.created_at DESC LIMIT 10",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("c.id", Value::String("task-stale-crystal".to_string())),
                        ("c.title", Value::String("Task Stale Crystal".to_string())),
                        ("newer.id", Value::String("task-stale-newer".to_string())),
                        ("newer.title", Value::String("Task Stale Newer".to_string())),
                        ("newer.created_at", Value::Int(4000)),
                        ("c.review_status", Value::String("pending".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-stale-crystal', title: 'Task Stale Crystal', is_crystal: true, created_at: 3000, review_status: 'pending'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-stale-source', title: 'Task Stale Source', is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'task-stale-newer', title: 'Task Stale Newer', is_crystal: false, created_at: 4000})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (c:Memory {id: 'task-stale-crystal'}), (src:Memory {id: 'task-stale-source'}) CREATE (c)-[:SYNTHESIZED_FROM]->(src)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (src:Memory {id: 'task-stale-source'}), (newer:Memory {id: 'task-stale-newer'}) CREATE (src)-[:EVOLVES]->(newer)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id IN ['task-stale-crystal', 'task-stale-source', 'task-stale-newer'] DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "distinct relationship aggregation",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN count(DISTINCT m) AS memories, count(DISTINCT e.id) AS entities",
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("memories", Value::Int(2)),
                    ("entities", Value::Int(2)),
                ])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "relationship min aggregation",
                CypherFixtureStatement::new(
                    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(*), min(r.weight)",
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("count(*)", Value::Int(2)),
                    ("min(r.weight)", Value::Int(3)),
                ])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "node property pattern read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory {kind: 'note'})-[:MENTIONS]->(e:Entity {name: 'Rust'}) RETURN DISTINCT m.id AS id",
                ),
                ExpectedRows::Exact(vec![compatibility_row([("id", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "undirected evolves read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 3})-[:EVOLVES]-(other:Memory) RETURN DISTINCT other.id AS id",
                ),
                ExpectedRows::Exact(vec![compatibility_row([("id", Value::Int(2))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "evolves progression list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (older:Memory)-[e:EVOLVES]->(newer:Memory) WHERE (older.unit_type IN $types OR newer.unit_type IN $types) RETURN older.id, newer.id, e.content_relation, e.is_progression, older.unit_type, newer.unit_type, older.title, newer.title, older.created_at, newer.created_at, older.is_latest, newer.is_latest, older.space_id, newer.space_id ORDER BY newer.created_at DESC LIMIT 500",
                        BTreeMap::from([(
                            "types".to_string(),
                            Value::List(vec![Value::String("context".to_string())]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("older.id", Value::Int(2)),
                        ("newer.id", Value::Int(3)),
                        (
                            "e.content_relation",
                            Value::String("supersedes".to_string()),
                        ),
                        ("e.is_progression", Value::Bool(true)),
                        ("older.unit_type", Value::String("context".to_string())),
                        ("newer.unit_type", Value::String("decision".to_string())),
                        ("older.title", Value::String("Runtime strategy".to_string())),
                        ("newer.title", Value::String("Cloud projection".to_string())),
                        ("older.created_at", Value::Int(100)),
                        ("newer.created_at", Value::Int(200)),
                        ("older.is_latest", Value::Bool(false)),
                        ("newer.is_latest", Value::Bool(true)),
                        ("older.space_id", Value::String("default".to_string())),
                        ("newer.space_id", Value::String("default".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 2}) SET m.unit_type = 'context', m.created_at = 100, m.is_latest = false, m.space_id = 'default'",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 3}) SET m.unit_type = 'decision', m.created_at = 200, m.is_latest = true, m.space_id = 'default'",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (older:Memory {id: 2})-[e:EVOLVES]->(newer:Memory {id: 3}) SET e.content_relation = 'supersedes', e.is_progression = true",
                )),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "incoming mentions read",
                CypherFixtureStatement::new(
                    "MATCH (e:Entity {name: 'Rust'})<-[:MENTIONS]-(m:Memory) RETURN DISTINCT m.id AS id",
                ),
                ExpectedRows::Exact(vec![compatibility_row([("id", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "untyped relationship label read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1})-[r]->(e:Entity) RETURN label(r) AS rel_type",
                ),
                ExpectedRows::Exact(vec![compatibility_row([(
                    "rel_type",
                    Value::String("MENTIONS".to_string()),
                )])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "multi label overview read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1})-[r]-(neighbor:Entity:Memory) RETURN DISTINCT neighbor.id AS id",
                ),
                ExpectedRows::Exact(vec![compatibility_row([("id", Value::Int(10))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "where matched evolves relationship create",
                    CypherFixtureStatement::new(
                        "MATCH (a:Memory), (b:Memory) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:EVOLVES {content_relation: 'replaces'}]->(b)",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (a:Memory {id: 1})-[r:EVOLVES]->(b:Memory {id: 2}) RETURN count(r) AS total, min(r.content_relation) AS relation",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("total", Value::Int(1)),
                        ("relation", Value::String("replaces".to_string())),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source provenance relationship create",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1}), (e:Entity {id: 10}) CREATE (m)-[:SOURCED_FROM {chunk_index: 0}]->(e)",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1})-[r:SOURCED_FROM]->(e:Entity {id: 10}) RETURN count(r) AS total, min(r.chunk_index) AS first_chunk",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("total", Value::Int(1)),
                        ("first_chunk", Value::Int(0)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source attribution read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:SOURCED_FROM]->(s:Source) WHERE m.id IN $ids RETURN m.id, s.id",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![Value::Int(1), Value::Int(99)]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("m.id", Value::Int(1)),
                        ("s.id", Value::String("source-attribution-1".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-attribution-1', original_name: 'Attribution source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1}), (s:Source {id: 'source-attribution-1'}) CREATE (m)-[:SOURCED_FROM]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-attribution-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source provenance memory detail read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[r:SOURCED_FROM]->(s:Source {id: $source_id}) RETURN m.id as memory_id, m.title as title, m.content as content, r.chunk_index as chunk_index, r.chunk_range as chunk_range, m.unit_type as unit_type, m.confidence as confidence ORDER BY r.chunk_index ASC LIMIT $limit",
                        BTreeMap::from([
                            (
                                "source_id".to_string(),
                                Value::String("source-detail-memory-source-1".to_string()),
                            ),
                            ("limit".to_string(), Value::Int(10)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            (
                                "memory_id",
                                Value::String("source-detail-memory-1".to_string()),
                            ),
                            ("title", Value::String("Source Detail One".to_string())),
                            ("content", Value::String("source detail body one".to_string())),
                            ("chunk_index", Value::Int(1)),
                            ("chunk_range", Value::String("0..10".to_string())),
                            ("unit_type", Value::String("fact".to_string())),
                            ("confidence", Value::Float(0.8)),
                        ]),
                        compatibility_row([
                            (
                                "memory_id",
                                Value::String("source-detail-memory-2".to_string()),
                            ),
                            ("title", Value::String("Source Detail Two".to_string())),
                            ("content", Value::String("source detail body two".to_string())),
                            ("chunk_index", Value::Int(2)),
                            ("chunk_range", Value::String("10..20".to_string())),
                            ("unit_type", Value::String("fact".to_string())),
                            ("confidence", Value::Float(0.7)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-detail-memory-source-1', title: 'Detail Memory Source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'source-detail-memory-1', title: 'Source Detail One', content: 'source detail body one', unit_type: 'fact', confidence: 0.8})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'source-detail-memory-2', title: 'Source Detail Two', content: 'source detail body two', unit_type: 'fact', confidence: 0.7})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'source-detail-memory-1'}), (s:Source {id: 'source-detail-memory-source-1'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 1, chunk_range: '0..10'}]->(s)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'source-detail-memory-2'}), (s:Source {id: 'source-detail-memory-source-1'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 2, chunk_range: '10..20'}]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-detail-memory-source-1', 'source-detail-memory-1', 'source-detail-memory-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source memory id list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:SOURCED_FROM]->(s:Source {id: $sid}) RETURN m.id LIMIT 24",
                        BTreeMap::from([(
                            "sid".to_string(),
                            Value::String("source-memory-id-list-source-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "m.id",
                        Value::String("source-memory-id-list-memory-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-memory-id-list-source-1', title: 'Memory Id List Source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'source-memory-id-list-memory-1', title: 'Source Memory Id List'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'source-memory-id-list-memory-1'}), (s:Source {id: 'source-memory-id-list-source-1'}) CREATE (m)-[:SOURCED_FROM]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-memory-id-list-source-1', 'source-memory-id-list-memory-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory source provenance id read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory {id: $id})-[:SOURCED_FROM]->(s:Source) RETURN s.id",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("memory-source-provenance-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "s.id",
                        Value::String("source-provenance-single-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-source-provenance-1', title: 'Source provenance memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-provenance-single-1', original_name: 'Single provenance source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-source-provenance-1'}), (s:Source {id: 'source-provenance-single-1'}) CREATE (m)-[:SOURCED_FROM]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-source-provenance-1', 'source-provenance-single-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source memory-count decrement floor write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $id}) SET s.memory_count = CASE WHEN s.memory_count > 0 THEN s.memory_count - 1 ELSE 0 END",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("source-memory-count-decrement-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-memory-count-decrement-1', memory_count: 2})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-memory-count-decrement-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread compaction attribution read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread)-[:COMPACTS_TO]->(m:Memory) WHERE m.id IN $ids RETURN m.id, t.thread_id",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![Value::Int(1), Value::Int(99)]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("m.id", Value::Int(1)),
                        (
                            "t.thread_id",
                            Value::String("compact-thread-1".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'compact-thread-node-1', thread_id: 'compact-thread-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'compact-thread-node-1'}), (m:Memory {id: 1}) CREATE (t)-[:COMPACTS_TO]->(m)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (t:Thread {id: 'compact-thread-node-1'}) DETACH DELETE t",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread compacted-memory count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread {id: $thread_uuid})-[:COMPACTS_TO]->(m:Memory) RETURN COUNT(m)",
                        BTreeMap::from([(
                            "thread_uuid".to_string(),
                            Value::String("compact-count-thread-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("count(m)", Value::Int(2))])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'compact-count-thread-1', thread_id: 'compact-count-logical-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'compact-count-memory-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'compact-count-memory-2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'compact-count-thread-1'}), (m:Memory {id: 'compact-count-memory-1'}) CREATE (t)-[:COMPACTS_TO]->(m)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'compact-count-thread-1'}), (m:Memory {id: 'compact-count-memory-2'}) CREATE (t)-[:COMPACTS_TO]->(m)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['compact-count-thread-1', 'compact-count-memory-1', 'compact-count-memory-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread compacted-memory summary read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread {id: $uuid})-[:COMPACTS_TO]->(m:Memory) RETURN m.id, m.title, m.content LIMIT 200",
                        BTreeMap::from([(
                            "uuid".to_string(),
                            Value::String("compact-summary-thread-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            (
                                "m.id",
                                Value::String("compact-summary-memory-1".to_string()),
                            ),
                            ("m.title", Value::String("Compact Summary One".to_string())),
                            ("m.content", Value::String("summary one".to_string())),
                        ]),
                        compatibility_row([
                            (
                                "m.id",
                                Value::String("compact-summary-memory-2".to_string()),
                            ),
                            ("m.title", Value::String("Compact Summary Two".to_string())),
                            ("m.content", Value::String("summary two".to_string())),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'compact-summary-thread-1', thread_id: 'compact-summary-logical-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'compact-summary-memory-1', title: 'Compact Summary One', content: 'summary one'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'compact-summary-memory-2', title: 'Compact Summary Two', content: 'summary two'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'compact-summary-thread-1'}), (m:Memory {id: 'compact-summary-memory-1'}) CREATE (t)-[:COMPACTS_TO]->(m)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'compact-summary-thread-1'}), (m:Memory {id: 'compact-summary-memory-2'}) CREATE (t)-[:COMPACTS_TO]->(m)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['compact-summary-thread-1', 'compact-summary-memory-1', 'compact-summary-memory-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory created-at bulk read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id, m.created_at",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("bulk-created-1".to_string()),
                                Value::String("missing-memory".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("m.id", Value::String("bulk-created-1".to_string())),
                        ("m.created_at", Value::Int(123456789)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'bulk-created-1', title: 'Bulk created read', created_at: 123456789})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'bulk-created-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory compact detail fallback read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory {id: $memory_id}) RETURN m.id, COALESCE(m.title, ''), COALESCE(m.content, ''), COALESCE(m.unit_type, '')",
                        BTreeMap::from([(
                            "memory_id".to_string(),
                            Value::String("compact-detail-memory-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "m.id",
                            Value::String("compact-detail-memory-1".to_string()),
                        ),
                        ("coalesce", Value::String("Compact Detail".to_string())),
                        ("coalesce#2", Value::String("compact detail body".to_string())),
                        ("coalesce#3", Value::String("fact".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'compact-detail-memory-1', title: 'Compact Detail', content: 'compact detail body', unit_type: 'fact'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'compact-detail-memory-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory label name list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory {id: $memory_id})-[:HAS_LABEL]->(l:Label) RETURN l.name LIMIT $limit",
                        BTreeMap::from([
                            (
                                "memory_id".to_string(),
                                Value::String("memory-label-list-memory-1".to_string()),
                            ),
                            ("limit".to_string(), Value::Int(10)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "l.name",
                        Value::String("architecture".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-label-list-memory-1', title: 'Labeled memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'memory-label-list-label-1', name: 'architecture'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-label-list-memory-1'}), (l:Label {id: 'memory-label-list-label-1'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-label-list-memory-1', 'memory-label-list-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory label bulk name read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids RETURN m.id, l.name",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("memory-label-bulk-memory-1".to_string()),
                                Value::String("missing-memory".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "m.id",
                            Value::String("memory-label-bulk-memory-1".to_string()),
                        ),
                        ("l.name", Value::String("retrieval".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-label-bulk-memory-1', title: 'Bulk labeled memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'memory-label-bulk-label-1', name: 'retrieval'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-label-bulk-memory-1'}), (l:Label {id: 'memory-label-bulk-label-1'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-label-bulk-memory-1', 'memory-label-bulk-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory label fallback bulk read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids RETURN m.id, COALESCE(l.name, l.id)",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("memory-label-fallback-memory-1".to_string()),
                                Value::String("missing-memory".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "m.id",
                            Value::String("memory-label-fallback-memory-1".to_string()),
                        ),
                        (
                            "coalesce",
                            Value::String("memory-label-fallback-label-1".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-label-fallback-memory-1', title: 'Fallback labeled memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'memory-label-fallback-label-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-label-fallback-memory-1'}), (l:Label {id: 'memory-label-fallback-label-1'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-label-fallback-memory-1', 'memory-label-fallback-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory label endpoint bulk read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids RETURN m.id, l.id",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("memory-label-endpoint-memory-1".to_string()),
                                Value::String("missing-memory".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "m.id",
                            Value::String("memory-label-endpoint-memory-1".to_string()),
                        ),
                        (
                            "l.id",
                            Value::String("memory-label-endpoint-label-1".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-label-endpoint-memory-1', title: 'Label endpoint memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'memory-label-endpoint-label-1', name: 'endpoint'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-label-endpoint-memory-1'}), (l:Label {id: 'memory-label-endpoint-label-1'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-label-endpoint-memory-1', 'memory-label-endpoint-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory label distinct name count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids AND l.name IN $names RETURN m.id, COUNT(DISTINCT l.name)",
                        BTreeMap::from([
                            (
                                "ids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-label-count-memory-1".to_string()),
                                    Value::String("missing-memory".to_string()),
                                ]),
                            ),
                            (
                                "names".to_string(),
                                Value::List(vec![
                                    Value::String("retrieval".to_string()),
                                    Value::String("missing-label".to_string()),
                                ]),
                            ),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "m.id",
                            Value::String("memory-label-count-memory-1".to_string()),
                        ),
                        ("count(DISTINCT l.name)", Value::Int(1)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-label-count-memory-1', title: 'Label count memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'memory-label-count-label-1', name: 'retrieval'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'memory-label-count-label-2', name: 'storage'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-label-count-memory-1'}), (l:Label {id: 'memory-label-count-label-1'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-label-count-memory-1'}), (l:Label {id: 'memory-label-count-label-2'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-label-count-memory-1', 'memory-label-count-label-1', 'memory-label-count-label-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source label bulk name read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source)-[:HAS_LABEL]->(l:Label) WHERE s.id IN $ids RETURN s.id, l.name",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("source-label-bulk-source-1".to_string()),
                                Value::String("missing-source".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "s.id",
                            Value::String("source-label-bulk-source-1".to_string()),
                        ),
                        ("l.name", Value::String("source-label".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-label-bulk-source-1', original_name: 'Labeled source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'source-label-bulk-label-1', name: 'source-label'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (s:Source {id: 'source-label-bulk-source-1'}), (l:Label {id: 'source-label-bulk-label-1'}) CREATE (s)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-label-bulk-source-1', 'source-label-bulk-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source label endpoint bulk read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source)-[:HAS_LABEL]->(l:Label) WHERE s.id IN $ids RETURN s.id, l.id",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("source-label-endpoint-source-1".to_string()),
                                Value::String("missing-source".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "s.id",
                            Value::String("source-label-endpoint-source-1".to_string()),
                        ),
                        (
                            "l.id",
                            Value::String("source-label-endpoint-label-1".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-label-endpoint-source-1', original_name: 'Endpoint labeled source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'source-label-endpoint-label-1', name: 'source-endpoint'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (s:Source {id: 'source-label-endpoint-source-1'}), (l:Label {id: 'source-label-endpoint-label-1'}) CREATE (s)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-label-endpoint-source-1', 'source-label-endpoint-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source label relationship count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $source_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) RETURN COUNT(r)",
                        BTreeMap::from([
                            (
                                "source_id".to_string(),
                                Value::String("source-label-count-source-1".to_string()),
                            ),
                            (
                                "label_id".to_string(),
                                Value::String("source-label-count-label-1".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "count(r)",
                        Value::Int(1),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-label-count-source-1', original_name: 'Count labeled source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'source-label-count-label-1', name: 'source-count'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (s:Source {id: 'source-label-count-source-1'}), (l:Label {id: 'source-label-count-label-1'}) CREATE (s)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-label-count-source-1', 'source-label-count-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source label relationship merge",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $source_id}), (l:Label {id: $label_id}) MERGE (s)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.created_at = $created_at, r.properties = $properties",
                        BTreeMap::from([
                            (
                                "source_id".to_string(),
                                Value::String("source-label-merge-source-1".to_string()),
                            ),
                            (
                                "label_id".to_string(),
                                Value::String("source-label-merge-label-1".to_string()),
                            ),
                            (
                                "assigned_by".to_string(),
                                Value::String("system".to_string()),
                            ),
                            ("created_at".to_string(), Value::Int(987)),
                            (
                                "properties".to_string(),
                                Value::String("{\"scope\":\"source\"}".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-label-merge-source-1', original_name: 'Merge labeled source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'source-label-merge-label-1', name: 'source-merge'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-label-merge-source-1', 'source-label-merge-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source label relationship delete",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $source_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r",
                        BTreeMap::from([
                            (
                                "source_id".to_string(),
                                Value::String("source-label-delete-source-1".to_string()),
                            ),
                            (
                                "label_id".to_string(),
                                Value::String("source-label-delete-label-1".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-label-delete-source-1', original_name: 'Delete labeled source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'source-label-delete-label-1', name: 'source-delete'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (s:Source {id: 'source-label-delete-source-1'}), (l:Label {id: 'source-label-delete-label-1'}) CREATE (s)-[:HAS_LABEL]->(l)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-label-delete-source-1', 'source-label-delete-label-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source detail normalized space read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $source_id}) RETURN s.id, s.original_name, CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END, s.lifecycle_state, s.parsed_path",
                        BTreeMap::from([(
                            "source_id".to_string(),
                            Value::String("source-detail-space-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("s.id", Value::String("source-detail-space-1".to_string())),
                        (
                            "s.original_name",
                            Value::String("Space Normalized Source".to_string()),
                        ),
                        ("space_id", Value::String("default".to_string())),
                        ("s.lifecycle_state", Value::String("parsed".to_string())),
                        (
                            "s.parsed_path",
                            Value::String("/tmp/source-detail-space.md".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-detail-space-1', original_name: 'Space Normalized Source', space_id: '', lifecycle_state: 'parsed', parsed_path: '/tmp/source-detail-space.md'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-detail-space-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source detail chunk-count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $source_id}) RETURN s.original_name, CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END, s.chunk_count",
                        BTreeMap::from([(
                            "source_id".to_string(),
                            Value::String("source-detail-chunk-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "s.original_name",
                            Value::String("Chunk Count Source".to_string()),
                        ),
                        ("space_id", Value::String("default".to_string())),
                        ("s.chunk_count", Value::Int(7)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-detail-chunk-1', original_name: 'Chunk Count Source', space_id: '', chunk_count: 7})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-detail-chunk-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source detail file-path read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $source_id}) RETURN s.original_name, CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END, s.file_path",
                        BTreeMap::from([(
                            "source_id".to_string(),
                            Value::String("source-detail-file-path-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "s.original_name",
                            Value::String("File Path Source".to_string()),
                        ),
                        ("space_id", Value::String("default".to_string())),
                        (
                            "s.file_path",
                            Value::String("/tmp/source-detail-file-path.md".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-detail-file-path-1', original_name: 'File Path Source', space_id: '', file_path: '/tmp/source-detail-file-path.md'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-detail-file-path-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source default metadata fallback read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $id}) RETURN COALESCE(s.space_id, 'default'), COALESCE(s.source_type, 'file'), COALESCE(s.lifecycle_state, 'indexed'), COALESCE(s.mime_type, '')",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("source-default-metadata-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("coalesce", Value::String("default".to_string())),
                        ("coalesce#2", Value::String("file".to_string())),
                        ("coalesce#3", Value::String("indexed".to_string())),
                        ("coalesce#4", Value::String(String::new())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-default-metadata-1'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-default-metadata-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source list fallback page read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) RETURN s.id, COALESCE(s.original_name, ''), COALESCE(s.summary, ''), COALESCE(s.mime_type, ''), COALESCE(s.source_type, 'file'), COALESCE(s.source_url, ''), COALESCE(s.size_bytes, 0), COALESCE(s.version, 1), COALESCE(s.memory_count, 0), COALESCE(s.chunk_count, 0), COALESCE(s.lifecycle_state, 'indexed'), COALESCE(s.space_id, 'default'), s.created_at, s.updated_at ORDER BY s.id SKIP $offset LIMIT $limit",
                        BTreeMap::from([
                            ("offset".to_string(), Value::Int(0)),
                            ("limit".to_string(), Value::Int(1)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "s.id",
                            Value::String("000-source-list-fallback-1".to_string()),
                        ),
                        ("coalesce", Value::String("List Source".to_string())),
                        ("coalesce#2", Value::String("".to_string())),
                        ("coalesce#3", Value::String("text/markdown".to_string())),
                        ("coalesce#4", Value::String("file".to_string())),
                        ("coalesce#5", Value::String("".to_string())),
                        ("coalesce#6", Value::Int(0)),
                        ("coalesce#7", Value::Int(1)),
                        ("coalesce#8", Value::Int(0)),
                        ("coalesce#9", Value::Int(3)),
                        ("coalesce#10", Value::String("indexed".to_string())),
                        ("coalesce#11", Value::String("default".to_string())),
                        ("s.created_at", Value::Int(11)),
                        ("s.updated_at", Value::Int(12)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: '000-source-list-fallback-1', original_name: 'List Source', mime_type: 'text/markdown', chunk_count: 3, created_at: 11, updated_at: 12})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: '000-source-list-fallback-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source overview memory-count ranking read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) RETURN s.id, COALESCE(s.original_name, s.source_type, 'Source'), s.source_type, s.lifecycle_state, COALESCE(s.memory_count, 0) ORDER BY s.memory_count DESC LIMIT $limit",
                        BTreeMap::from([("limit".to_string(), Value::Int(1))]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "s.id",
                            Value::String("source-overview-ranking-1".to_string()),
                        ),
                        ("coalesce", Value::String("Ranked Source".to_string())),
                        ("s.source_type", Value::String("file".to_string())),
                        ("s.lifecycle_state", Value::String("indexed".to_string())),
                        ("coalesce#2", Value::Int(999)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-overview-ranking-1', original_name: 'Ranked Source', source_type: 'file', lifecycle_state: 'indexed', memory_count: 999})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-overview-ranking-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source bulk summary fallback read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) WHERE s.id IN $ids RETURN s.id, COALESCE(s.original_name, s.file_path, s.source_type, 'Source'), s.source_type, s.summary, s.file_path, s.memory_count, s.chunk_count",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("source-bulk-summary-1".to_string()),
                                Value::String("source-bulk-summary-2".to_string()),
                                Value::String("missing-source".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Unordered(vec![
                        compatibility_row([
                            ("s.id", Value::String("source-bulk-summary-1".to_string())),
                            ("coalesce", Value::String("Bulk Source One".to_string())),
                            ("s.source_type", Value::String("file".to_string())),
                            ("s.summary", Value::String("summary one".to_string())),
                            (
                                "s.file_path",
                                Value::String("/tmp/source-bulk-one.md".to_string()),
                            ),
                            ("s.memory_count", Value::Int(2)),
                            ("s.chunk_count", Value::Int(4)),
                        ]),
                        compatibility_row([
                            ("s.id", Value::String("source-bulk-summary-2".to_string())),
                            (
                                "coalesce",
                                Value::String("/tmp/source-bulk-two.md".to_string()),
                            ),
                            ("s.source_type", Value::String("file".to_string())),
                            ("s.summary", Value::String("summary two".to_string())),
                            (
                                "s.file_path",
                                Value::String("/tmp/source-bulk-two.md".to_string()),
                            ),
                            ("s.memory_count", Value::Int(0)),
                            ("s.chunk_count", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-bulk-summary-1', original_name: 'Bulk Source One', source_type: 'file', summary: 'summary one', file_path: '/tmp/source-bulk-one.md', memory_count: 2, chunk_count: 4})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-bulk-summary-2', source_type: 'file', summary: 'summary two', file_path: '/tmp/source-bulk-two.md', memory_count: 0, chunk_count: 1})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-bulk-summary-1', 'source-bulk-summary-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source count read",
                    CypherFixtureStatement::new("MATCH (s:Source) RETURN count(s)"),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "count(s)",
                        Value::Int(1),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-count-1', original_name: 'Count Source'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-count-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source extracted id list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) WHERE s.id IN $ids AND s.lifecycle_state = 'extracted' RETURN s.id",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![
                                Value::String("source-extracted-id-1".to_string()),
                                Value::String("source-extracted-id-2".to_string()),
                                Value::String("missing-source".to_string()),
                            ]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "s.id",
                        Value::String("source-extracted-id-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-extracted-id-1', lifecycle_state: 'extracted'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-extracted-id-2', lifecycle_state: 'indexed'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-extracted-id-1', 'source-extracted-id-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source extracted lifecycle mark indexed write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) WHERE s.id IN $ids AND s.lifecycle_state = 'extracted' SET s.lifecycle_state = 'indexed', s.updated_at = timestamp($updated_at)",
                        BTreeMap::from([
                            (
                                "ids".to_string(),
                                Value::List(vec![
                                    Value::String("source-lifecycle-index-1".to_string()),
                                    Value::String("source-lifecycle-index-2".to_string()),
                                    Value::String("missing-source".to_string()),
                                ]),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_001)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-lifecycle-index-1', lifecycle_state: 'extracted', updated_at: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-lifecycle-index-2', lifecycle_state: 'indexed', updated_at: 2})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-lifecycle-index-1', 'source-lifecycle-index-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source lifecycle indexed chunk-count write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $id}) SET s.lifecycle_state = 'indexed', s.chunk_count = $chunk_count, s.updated_at = timestamp($updated_at)",
                        BTreeMap::from([
                            (
                                "id".to_string(),
                                Value::String("source-lifecycle-chunk-count-1".to_string()),
                            ),
                            ("chunk_count".to_string(), Value::Int(8)),
                            ("updated_at".to_string(), Value::Int(1_700_000_008)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-lifecycle-chunk-count-1', lifecycle_state: 'parsed', chunk_count: 0, updated_at: 1})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-lifecycle-chunk-count-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source lifecycle state update write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $id}) SET s.lifecycle_state = $state, s.updated_at = timestamp($updated_at)",
                        BTreeMap::from([
                            (
                                "id".to_string(),
                                Value::String("source-lifecycle-state-1".to_string()),
                            ),
                            ("state".to_string(), Value::String("failed".to_string())),
                            ("updated_at".to_string(), Value::Int(1_700_000_009)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-lifecycle-state-1', lifecycle_state: 'parsed', updated_at: 1})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-lifecycle-state-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source space update write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $id}) SET s.space_id = $target_space_id, s.updated_at = timestamp($updated_at)",
                        BTreeMap::from([
                            (
                                "id".to_string(),
                                Value::String("source-space-update-1".to_string()),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("research".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_010)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-space-update-1', space_id: 'default', updated_at: 1})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'source-space-update-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source bulk normalized-space move write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) WHERE s.id IN $ids AND CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END = $source_space_id SET s.space_id = $target_space_id, s.updated_at = $updated_at",
                        BTreeMap::from([
                            (
                                "ids".to_string(),
                                Value::List(vec![
                                    Value::String("source-bulk-space-move-1".to_string()),
                                    Value::String("source-bulk-space-move-2".to_string()),
                                    Value::String("missing-source".to_string()),
                                ]),
                            ),
                            (
                                "source_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("research".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_011)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-bulk-space-move-1', space_id: '', updated_at: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-bulk-space-move-2', space_id: 'archive', updated_at: 2})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-bulk-space-move-1', 'source-bulk-space-move-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source normalized-space id list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) WHERE CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END = $space_id RETURN s.id",
                        BTreeMap::from([(
                            "space_id".to_string(),
                            Value::String("default".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "s.id",
                        Value::String("source-normalized-space-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-normalized-space-1', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-normalized-space-2', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-normalized-space-1', 'source-normalized-space-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory normalized-space id list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $space_id RETURN m.id",
                        BTreeMap::from([(
                            "space_id".to_string(),
                            Value::String("default".to_string()),
                        )]),
                    ),
                    ExpectedRows::Unordered(vec![
                        compatibility_row([("m.id", Value::Int(1))]),
                        compatibility_row([("m.id", Value::Int(2))]),
                        compatibility_row([("m.id", Value::Int(3))]),
                        compatibility_row([(
                            "m.id",
                            Value::String("memory-normalized-space-1".to_string()),
                        )]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-space-1', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-space-2', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-normalized-space-1', 'memory-normalized-space-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory bulk normalized-space move write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.id IN $ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id SET m.space_id = $target_space_id, m.updated_at = $updated_at",
                        BTreeMap::from([
                            (
                                "ids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-bulk-space-move-1".to_string()),
                                    Value::String("memory-bulk-space-move-2".to_string()),
                                    Value::String("missing-memory".to_string()),
                                ]),
                            ),
                            (
                                "source_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("research".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_012)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-bulk-space-move-1', space_id: '', updated_at: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-bulk-space-move-2', space_id: 'archive', updated_at: 2})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-bulk-space-move-1', 'memory-bulk-space-move-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory normalized-space limit-one read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $space_id RETURN m.id LIMIT 1",
                        BTreeMap::from([(
                            "space_id".to_string(),
                            Value::String("archive".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "m.id",
                        Value::String("memory-normalized-limit-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-limit-1', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'memory-normalized-limit-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory candidate normalized-space id read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.id IN $candidate_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id RETURN m.id",
                        BTreeMap::from([
                            (
                                "candidate_ids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-candidate-space-1".to_string()),
                                    Value::String("memory-candidate-space-2".to_string()),
                                    Value::String("missing-memory".to_string()),
                                ]),
                            ),
                            (
                                "source_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "m.id",
                        Value::String("memory-candidate-space-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-candidate-space-1', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-candidate-space-2', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-candidate-space-1', 'memory-candidate-space-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory candidate normalized-space exclusion read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.id IN $candidate_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END <> $target_space_id RETURN m.id",
                        BTreeMap::from([
                            (
                                "candidate_ids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-candidate-exclusion-1".to_string()),
                                    Value::String("memory-candidate-exclusion-2".to_string()),
                                    Value::String("memory-candidate-exclusion-3".to_string()),
                                ]),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "m.id",
                        Value::String("memory-candidate-exclusion-3".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-candidate-exclusion-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-candidate-exclusion-2', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-candidate-exclusion-3', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-candidate-exclusion-1', 'memory-candidate-exclusion-2', 'memory-candidate-exclusion-3'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory normalized-space count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id RETURN count(m)",
                        BTreeMap::from([(
                            "source_space_id".to_string(),
                            Value::String("count-space".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("count(m)", Value::Int(2))])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-count-1', space_id: 'count-space'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-count-2', space_id: 'count-space'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-count-3', space_id: 'other-space'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-normalized-count-1', 'memory-normalized-count-2', 'memory-normalized-count-3'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory normalized-space limited id read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id RETURN m.id LIMIT $limit",
                        BTreeMap::from([
                            (
                                "source_space_id".to_string(),
                                Value::String("limited-space".to_string()),
                            ),
                            ("limit".to_string(), Value::Int(1)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-limited-1', space_id: 'limited-space'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-normalized-limited-2', space_id: 'limited-space'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-normalized-limited-1', 'memory-normalized-limited-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory id-list normalized-space move returning ids",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.id IN $memory_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id SET m.space_id = $target_space_id, m.updated_at = $updated_at RETURN m.id",
                        BTreeMap::from([
                            (
                                "memory_ids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-returning-space-move-1".to_string()),
                                    Value::String("memory-returning-space-move-2".to_string()),
                                    Value::String("missing-memory".to_string()),
                                ]),
                            ),
                            (
                                "source_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("research".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_015)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "m.id",
                        Value::String("memory-returning-space-move-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-returning-space-move-1', space_id: '', updated_at: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-returning-space-move-2', space_id: 'archive', updated_at: 2})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-returning-space-move-1', 'memory-returning-space-move-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory id-list normalized-space exclusion move returning ids",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.id IN $memory_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END <> $target_space_id SET m.space_id = $target_space_id, m.updated_at = $updated_at RETURN m.id",
                        BTreeMap::from([
                            (
                                "memory_ids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-returning-exclusion-move-1".to_string()),
                                    Value::String("memory-returning-exclusion-move-2".to_string()),
                                    Value::String("memory-returning-exclusion-move-3".to_string()),
                                ]),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_016)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "m.id",
                        Value::String("memory-returning-exclusion-move-3".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-returning-exclusion-move-1', updated_at: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-returning-exclusion-move-2', space_id: '', updated_at: 2})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-returning-exclusion-move-3', space_id: 'archive', updated_at: 3})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-returning-exclusion-move-1', 'memory-returning-exclusion-move-2', 'memory-returning-exclusion-move-3'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread normalized-space id pair read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $space_id RETURN t.id, t.thread_id",
                        BTreeMap::from([(
                            "space_id".to_string(),
                            Value::String("default".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "t.id",
                            Value::String("thread-normalized-space-1".to_string()),
                        ),
                        (
                            "t.thread_id",
                            Value::String("thread-normalized-logical-1".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-normalized-space-1', thread_id: 'thread-normalized-logical-1', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-normalized-space-2', thread_id: 'thread-normalized-logical-2', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['thread-normalized-space-1', 'thread-normalized-space-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread bulk node-id normalized-space move write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread) WHERE t.id IN $ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id SET t.space_id = $target_space_id, t.updated_at = $updated_at",
                        BTreeMap::from([
                            (
                                "ids".to_string(),
                                Value::List(vec![
                                    Value::String("thread-node-space-move-1".to_string()),
                                    Value::String("thread-node-space-move-2".to_string()),
                                    Value::String("missing-thread".to_string()),
                                ]),
                            ),
                            (
                                "source_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("research".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_013)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-node-space-move-1', thread_id: 'thread-node-space-logical-1', space_id: '', updated_at: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-node-space-move-2', thread_id: 'thread-node-space-logical-2', space_id: 'archive', updated_at: 2})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['thread-node-space-move-1', 'thread-node-space-move-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread identity normalized-space id read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (ti:ThreadIdentity) WHERE CASE WHEN ti.space_id IS NULL OR ti.space_id = '' THEN 'default' ELSE ti.space_id END = $space_id RETURN ti.thread_id",
                        BTreeMap::from([(
                            "space_id".to_string(),
                            Value::String("default".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "ti.thread_id",
                        Value::String("identity-normalized-logical-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:ThreadIdentity {id: 'thread-identity-normalized-space-1', thread_id: 'identity-normalized-logical-1', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:ThreadIdentity {id: 'thread-identity-normalized-space-2', thread_id: 'identity-normalized-logical-2', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['thread-identity-normalized-space-1', 'thread-identity-normalized-space-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread identity bulk normalized-space move write",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (ti:ThreadIdentity) WHERE ti.thread_id IN $ids AND CASE WHEN ti.space_id IS NULL OR ti.space_id = '' THEN 'default' ELSE ti.space_id END = $source_space_id SET ti.space_id = $target_space_id, ti.updated_at = $updated_at",
                        BTreeMap::from([
                            (
                                "ids".to_string(),
                                Value::List(vec![
                                    Value::String("identity-bulk-space-logical-1".to_string()),
                                    Value::String("identity-bulk-space-logical-2".to_string()),
                                    Value::String("missing-identity".to_string()),
                                ]),
                            ),
                            (
                                "source_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("research".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(1_700_000_014)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:ThreadIdentity {id: 'thread-identity-bulk-space-1', thread_id: 'identity-bulk-space-logical-1', space_id: '', updated_at: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:ThreadIdentity {id: 'thread-identity-bulk-space-2', thread_id: 'identity-bulk-space-logical-2', space_id: 'archive', updated_at: 2})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['thread-identity-bulk-space-1', 'thread-identity-bulk-space-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread normalized-space logical id read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $space_id RETURN t.thread_id",
                        BTreeMap::from([(
                            "space_id".to_string(),
                            Value::String("default".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "t.thread_id",
                        Value::String("thread-normalized-logical-only-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-normalized-logical-only-node-1', thread_id: 'thread-normalized-logical-only-1', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-normalized-logical-only-node-2', thread_id: 'thread-normalized-logical-only-2', space_id: 'archive'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['thread-normalized-logical-only-node-1', 'thread-normalized-logical-only-node-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory entity name list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory {id: $memory_id})-[:MENTIONS]->(e:Entity) RETURN e.name LIMIT $limit",
                        BTreeMap::from([
                            (
                                "memory_id".to_string(),
                                Value::String("memory-entity-list-memory-1".to_string()),
                            ),
                            ("limit".to_string(), Value::Int(10)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "e.name",
                        Value::String("Skein".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-entity-list-memory-1', title: 'Entity linked memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'memory-entity-list-entity-1', name: 'Skein'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-entity-list-memory-1'}), (e:Entity {id: 'memory-entity-list-entity-1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-entity-list-memory-1', 'memory-entity-list-entity-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory entity endpoint bulk read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.id IN $mids AND e.id IN $eids RETURN m.id, e.id",
                        BTreeMap::from([
                            (
                                "mids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-entity-endpoint-memory-1".to_string()),
                                    Value::String("missing-memory".to_string()),
                                ]),
                            ),
                            (
                                "eids".to_string(),
                                Value::List(vec![
                                    Value::String("memory-entity-endpoint-entity-1".to_string()),
                                    Value::String("missing-entity".to_string()),
                                ]),
                            ),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "m.id",
                            Value::String("memory-entity-endpoint-memory-1".to_string()),
                        ),
                        (
                            "e.id",
                            Value::String("memory-entity-endpoint-entity-1".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'memory-entity-endpoint-memory-1', title: 'Endpoint memory'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'memory-entity-endpoint-entity-1', name: 'Endpoint entity'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'memory-entity-endpoint-memory-1'}), (e:Entity {id: 'memory-entity-endpoint-entity-1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['memory-entity-endpoint-memory-1', 'memory-entity-endpoint-entity-1'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(2),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source fan-in relationship count",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (:Memory)-[r:SOURCED_FROM]->(:Source {id: $id}) RETURN count(r)",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("fan-in-source-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'fan-in-source-1', original_name: 'Fan in source'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1}), (s:Source {id: 'fan-in-source-1'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 0}]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'fan-in-source-1'}) DETACH DELETE s",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source detail memory-count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source) WHERE s.id = $id OPTIONAL MATCH (m:Memory)-[:SOURCED_FROM]->(s) RETURN s.id, s.title, s.source_type, COUNT(m)",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("source-detail-count-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "s.id",
                            Value::String("source-detail-count-1".to_string()),
                        ),
                        ("s.title", Value::String("Detail Source".to_string())),
                        ("s.source_type", Value::String("markdown".to_string())),
                        ("count(m)", Value::Int(2)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-detail-count-1', title: 'Detail Source', source_type: 'markdown'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'source-detail-memory-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'source-detail-memory-2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'source-detail-memory-1'}), (s:Source {id: 'source-detail-count-1'}) CREATE (m)-[:SOURCED_FROM]->(s)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'source-detail-memory-2'}), (s:Source {id: 'source-detail-count-1'}) CREATE (m)-[:SOURCED_FROM]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-detail-count-1', 'source-detail-memory-1', 'source-detail-memory-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "projection fallback read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1})-[r:MENTIONS]->(e:Entity) RETURN COALESCE(m.title, LEFT(COALESCE(m.content, ''), 60)) AS label, COALESCE(r.strength, r.weight, 0.5) AS weight",
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("label", Value::String("Graph foundations".to_string())),
                    ("weight", Value::Int(3)),
                ])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "projection fallback ordering",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) RETURN m.id AS id ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC",
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([("id", Value::Int(1))]),
                    compatibility_row([("id", Value::Int(3))]),
                    compatibility_row([("id", Value::Int(2))]),
                ]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "memory ranked overview read",
                CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory) RETURN m.id, COALESCE(m.title, LEFT(m.content, 60)), m.title, LEFT(COALESCE(m.content, ''), 200), COALESCE(m.pagerank_score, m.importance, 0.5), m.community_id, m.space_id, m.created_at, m.updated_at, m.source, m.event_start, m.event_end, m.importance ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC LIMIT $limit",
                    BTreeMap::from([("limit".to_string(), Value::Int(3))]),
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([
                        ("m.id", Value::String("overview-memory-1".to_string())),
                        ("coalesce", Value::String("Overview One".to_string())),
                        ("m.title", Value::String("Overview One".to_string())),
                        ("left", Value::String("body one".to_string())),
                        ("coalesce#2", Value::Float(3.0)),
                        ("m.community_id", Value::Int(7001)),
                        ("m.space_id", Value::String("default".to_string())),
                        ("m.created_at", Value::Int(101)),
                        ("m.updated_at", Value::Int(201)),
                        ("m.source", Value::String("overview".to_string())),
                        ("m.event_start", Value::Int(301)),
                        ("m.event_end", Value::Int(401)),
                        ("m.importance", Value::Float(0.1)),
                    ]),
                    compatibility_row([
                        ("m.id", Value::String("overview-memory-2".to_string())),
                        ("coalesce", Value::String("Fallback body".to_string())),
                        ("m.title", Value::Null),
                        ("left", Value::String("Fallback body".to_string())),
                        ("coalesce#2", Value::Float(2.0)),
                        ("m.community_id", Value::Int(7002)),
                        ("m.space_id", Value::String("default".to_string())),
                        ("m.created_at", Value::Int(102)),
                        ("m.updated_at", Value::Int(202)),
                        ("m.source", Value::String("overview".to_string())),
                        ("m.event_start", Value::Int(302)),
                        ("m.event_end", Value::Int(402)),
                        ("m.importance", Value::Float(0.2)),
                    ]),
                    compatibility_row([
                        ("m.id", Value::String("overview-memory-3".to_string())),
                        ("coalesce", Value::String("Overview Three".to_string())),
                        ("m.title", Value::String("Overview Three".to_string())),
                        ("left", Value::String(String::new())),
                        ("coalesce#2", Value::Float(1.0)),
                        ("m.community_id", Value::Int(7003)),
                        ("m.space_id", Value::String("default".to_string())),
                        ("m.created_at", Value::Int(103)),
                        ("m.updated_at", Value::Int(203)),
                        ("m.source", Value::String("overview".to_string())),
                        ("m.event_start", Value::Int(303)),
                        ("m.event_end", Value::Int(403)),
                        ("m.importance", Value::Float(0.3)),
                    ]),
                ]),
            )
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Memory {id: 'overview-memory-1', title: 'Overview One', content: 'body one', pagerank_score: 3.0, importance: 0.1, community_id: 7001, space_id: 'default', created_at: 101, updated_at: 201, source: 'overview', event_start: 301, event_end: 401})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Memory {id: 'overview-memory-2', content: 'Fallback body', pagerank_score: 2.0, importance: 0.2, community_id: 7002, space_id: 'default', created_at: 102, updated_at: 202, source: 'overview', event_start: 302, event_end: 402})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Memory {id: 'overview-memory-3', title: 'Overview Three', pagerank_score: 1.0, importance: 0.3, community_id: 7003, space_id: 'default', created_at: 103, updated_at: 203, source: 'overview', event_start: 303, event_end: 403})",
            ))
            .with_effect_query(
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE m.id IN ['overview-memory-1', 'overview-memory-2', 'overview-memory-3'] DETACH DELETE m",
                ),
                ExpectedRows::RowCount(3),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "memory review-status bulk read",
                CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id, m.review_status, m.title, m.content",
                    BTreeMap::from([(
                        "ids".to_string(),
                        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(99)]),
                    )]),
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([
                        ("m.id", Value::Int(1)),
                        ("m.review_status", Value::Null),
                        ("m.title", Value::String("Graph foundations".to_string())),
                        ("m.content", Value::Null),
                    ]),
                    compatibility_row([
                        ("m.id", Value::Int(2)),
                        ("m.review_status", Value::Null),
                        ("m.title", Value::String("Runtime strategy".to_string())),
                        ("m.content", Value::Null),
                    ]),
                ]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "predicate fallback read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE COALESCE(m.is_crystal, false) = false RETURN m.id AS id ORDER BY id ASC",
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([("id", Value::Int(1))]),
                    compatibility_row([("id", Value::Int(2))]),
                ]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "case insensitive fallback grep",
                CypherFixtureStatement::with_parameters(
                    "MATCH (m:Memory) WHERE LOWER(COALESCE(m.title, '')) CONTAINS LOWER($needle) RETURN m.id AS id",
                    BTreeMap::from([("needle".to_string(), Value::String("GRAPH".to_string()))]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("id", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "anonymous relationship count",
                CypherFixtureStatement::new(
                    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total",
                ),
                ExpectedRows::Exact(vec![compatibility_row([("total", Value::Int(2))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "relationship property read",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory)-[r:MENTIONS {weight: 4}]->(e:Entity) RETURN e.name AS entity, r.weight AS weight",
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("entity", Value::String("Cypher".to_string())),
                    ("weight", Value::Int(4)),
                ])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "unlabeled node mutation",
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id = 1 SET n.community_id = 42",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.community_id = 42 RETURN n.id AS id",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("id", Value::Int(1))])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "relationship property mutation",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory)-[r:MENTIONS {weight: 3}]->(e:Entity) WHERE m.id = 1 AND r.weight = 3 SET r.weight = 7",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 RETURN r.weight AS weight",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("weight", Value::Int(7))])]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "cypher projected graph definition",
                CypherFixtureStatement::new(
                    "CALL project_graph('MemoryMentions', ['Memory', 'Entity'], ['MENTIONS'])",
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("graph_name", Value::String("MemoryMentions".to_string())),
                    ("node_count", Value::Int(5)),
                    ("edge_count", Value::Int(2)),
                ])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "page rank procedure",
                    CypherFixtureStatement::new(
                        "CALL page_rank('MemoryMentions', dampingFactor := 0.85, maxIterations := 20) RETURN node, pagerank_score",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("node", Value::Int(1)),
                            ("pagerank_score", Value::Float(0.2761194029526352)),
                        ]),
                        compatibility_row([
                            ("node", Value::Int(3)),
                            ("pagerank_score", Value::Float(0.2761194029526352)),
                        ]),
                        compatibility_row([
                            ("node", Value::Int(0)),
                            ("pagerank_score", Value::Float(0.1492537313649099)),
                        ]),
                        compatibility_row([
                            ("node", Value::Int(2)),
                            ("pagerank_score", Value::Float(0.1492537313649099)),
                        ]),
                        compatibility_row([
                            ("node", Value::Int(4)),
                            ("pagerank_score", Value::Float(0.1492537313649099)),
                        ]),
                    ]),
                )
                .with_tolerance(CompatibilityTolerance { float_abs: 1.0e-6 }),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "hierarchical louvain procedure",
                CypherFixtureStatement::new(
                    "CALL louvain('MemoryMentions', maxLevels := 2) RETURN node, level, louvain_id",
                ),
                ExpectedRows::RowCount(10),
            )),
            CompatibilityCheck::ProjectedGraph(ProjectedGraphFixtureCheck {
                name: "mentions projection".to_string(),
                rel_type: Some("MENTIONS".to_string()),
                expected_node_count: 5,
                expected_edge_count: 2,
                expected_incoming: vec![(1, vec![0])],
                expected_communities: vec![(0, 0), (1, 0), (2, 2), (3, 2), (4, 4)],
                expected_hierarchical_communities: vec![(0, 0, 0), (0, 4, 4)],
                expected_page_rank_scores: vec![
                    (1, 0.2761194029526352),
                    (3, 0.2761194029526352),
                ],
                page_rank_top_node: Some(1),
                tolerance: CompatibilityTolerance { float_abs: 1.0e-6 },
            }),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "undo community delete nodes",
                    CypherFixtureStatement::new("MATCH (c:Community) DETACH DELETE c"),
                    ExpectedRows::RowCount(2),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'undo-community-a', community_id: 100, name: 'Undo A'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'undo-community-b', community_id: 101, name: 'Undo B'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (c:Community) WHERE c.id IN ['undo-community-a', 'undo-community-b'] RETURN count(c) AS total",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("total", Value::Int(0))])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "undo community clear node assignments",
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.community_id IS NOT NULL SET n.community_id = NULL",
                    ),
                    ExpectedRows::RowCount(3),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'undo-community-memory', community_id: 100})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'undo-community-memory-2', community_id: 101})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['undo-community-memory', 'undo-community-memory-2'] RETURN count(n.community_id) AS remaining",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("remaining", Value::Int(0))])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "undo community reset graph meta",
                    CypherFixtureStatement::new(
                        "MATCH (m:GraphMeta {meta_id: 'main'}) SET m.community_detection_applied = false, m.community_algorithm = '', m.community_resolution = 1.0, m.community_count = 0, m.community_detection_computed_at = NULL, m.updated_at = CURRENT_TIMESTAMP()",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "MERGE (:GraphMeta {meta_id: 'main', community_detection_applied: true, community_algorithm: 'louvain', community_resolution: 0.8, community_count: 2, community_detection_computed_at: 1})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.community_detection_applied AS applied, m.community_algorithm AS algorithm, m.community_resolution AS resolution, m.community_count AS count, count(m.community_detection_computed_at) AS computed",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("applied", Value::Bool(false)),
                        ("algorithm", Value::String(String::new())),
                        ("resolution", Value::Float(1.0)),
                        ("count", Value::Int(0)),
                        ("computed", Value::Int(0)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "crystallized provenance backfill merge",
                    CypherFixtureStatement::new(
                        "MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, n.occasion_key = '', n.created_at = r.created_at",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'crystal-backfill-c'})-[:CRYSTALLIZED_FROM {contribution_weight: 0.75, created_at: 123}]->(:Memory {id: 'crystal-backfill-s'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (:Memory)-[r:SYNTHESIZED_FROM {occasion_key: ''}]->(:Memory) RETURN count(r) AS total, min(r.weight) AS weight, min(r.created_at) AS created",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("total", Value::Int(1)),
                        ("weight", Value::Float(0.75)),
                        ("created", Value::Int(123)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "crystallized provenance backfill idempotent",
                    CypherFixtureStatement::new(
                        "MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, n.occasion_key = '', n.created_at = r.created_at",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (:Memory)-[r:SYNTHESIZED_FROM {occasion_key: ''}]->(:Memory) RETURN count(r) AS total",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("total", Value::Int(1))])]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "crystallized provenance unmirrored verification",
                CypherFixtureStatement::new(
                    "MATCH (c:Memory)-[:CRYSTALLIZED_FROM]->(s:Memory) WHERE NOT EXISTS { MATCH (c)-[:SYNTHESIZED_FROM]->(s) } RETURN count(*)",
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(*)", Value::Int(0))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "crystallized provenance distinct pair count",
                    CypherFixtureStatement::new(
                        "MATCH (c:Memory)-[:CRYSTALLIZED_FROM]->(s:Memory) WITH DISTINCT c.id AS a, s.id AS b RETURN count(*)",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("count(*)", Value::Int(2))])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'crystal-distinct-c'})-[:CRYSTALLIZED_FROM]->(:Memory {id: 'crystal-distinct-s'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (c:Memory {id: 'crystal-distinct-c'}), (s:Memory {id: 'crystal-distinct-s'}) CREATE (c)-[:CRYSTALLIZED_FROM]->(s)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "synthesized source ids collect read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids",
                        BTreeMap::from([(
                            "ids".to_string(),
                            Value::List(vec![Value::String("collect-c1".to_string())]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("c.id", Value::String("collect-c1".to_string())),
                        (
                            "source_ids",
                            Value::List(vec![
                                Value::String("collect-s1".to_string()),
                                Value::String("collect-s2".to_string()),
                            ]),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'collect-c1'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'collect-s1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'collect-s2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (c:Memory {id: 'collect-c1'}), (s:Memory {id: 'collect-s2'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (c:Memory {id: 'collect-c1'}), (s:Memory {id: 'collect-s2'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "skill synthesized memory id read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (sk:Skill)-[:SYNTHESIZED_FROM]->(m:Memory) WHERE sk.stage IN $stages RETURN m.id",
                        BTreeMap::from([(
                            "stages".to_string(),
                            Value::List(vec![Value::String("active".to_string())]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "m.id",
                        Value::String("skill-source-memory-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Skill {id: 'skill-synth-1', stage: 'active'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'skill-source-memory-1', title: 'Skill source memory'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (sk:Skill {id: 'skill-synth-1'}) DETACH DELETE sk",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'skill-source-memory-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "skill stage list read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (sk:Skill) WHERE sk.stage IN $stages RETURN sk.id, sk.title, sk.name, sk.description, sk.stage, sk.evidence_count LIMIT 50",
                        BTreeMap::from([(
                            "stages".to_string(),
                            Value::List(vec![Value::String("listed".to_string())]),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("sk.id", Value::String("skill-list-1".to_string())),
                        ("sk.title", Value::String("Skill List".to_string())),
                        ("sk.name", Value::String("skill-list".to_string())),
                        (
                            "sk.description",
                            Value::String("List readable skill".to_string()),
                        ),
                        ("sk.stage", Value::String("listed".to_string())),
                        ("sk.evidence_count", Value::Int(3)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Skill {id: 'skill-list-1', title: 'Skill List', name: 'skill-list', description: 'List readable skill', stage: 'listed', evidence_count: 3})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (sk:Skill {id: 'skill-list-1'}) DETACH DELETE sk",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "skill detail read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (sk:Skill {id: $id}) RETURN sk.title, sk.name, sk.stage, sk.scope, sk.rationale, sk.evidence_count, sk.metadata",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("skill-detail-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("sk.title", Value::String("Skill Detail".to_string())),
                        ("sk.name", Value::String("skill-detail".to_string())),
                        ("sk.stage", Value::String("published".to_string())),
                        ("sk.scope", Value::String("workspace".to_string())),
                        ("sk.rationale", Value::String("detail read".to_string())),
                        ("sk.evidence_count", Value::Int(4)),
                        ("sk.metadata", Value::String("{\"kind\":\"skill\"}".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Skill {id: 'skill-detail-1', title: 'Skill Detail', name: 'skill-detail', stage: 'published', scope: 'workspace', rationale: 'detail read', evidence_count: 4, metadata: '{\"kind\":\"skill\"}'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (sk:Skill {id: 'skill-detail-1'}) DETACH DELETE sk",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "skill synthesized memory detail read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (sk:Skill {id: $id})-[:SYNTHESIZED_FROM]->(m:Memory) RETURN m.id, m.title, m.content, m.unit_type ORDER BY m.created_at",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("skill-evidence-detail-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("m.id", Value::String("skill-evidence-memory-1".to_string())),
                            ("m.title", Value::String("Evidence One".to_string())),
                            ("m.content", Value::String("first evidence".to_string())),
                            ("m.unit_type", Value::String("fact".to_string())),
                        ]),
                        compatibility_row([
                            ("m.id", Value::String("skill-evidence-memory-2".to_string())),
                            ("m.title", Value::String("Evidence Two".to_string())),
                            ("m.content", Value::String("second evidence".to_string())),
                            ("m.unit_type", Value::String("context".to_string())),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Skill {id: 'skill-evidence-detail-1', stage: 'published'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'skill-evidence-memory-2', title: 'Evidence Two', content: 'second evidence', unit_type: 'context', created_at: 20})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'skill-evidence-memory-1', title: 'Evidence One', content: 'first evidence', unit_type: 'fact', created_at: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (sk:Skill {id: 'skill-evidence-detail-1'}), (m:Memory {id: 'skill-evidence-memory-1'}) CREATE (sk)-[:SYNTHESIZED_FROM]->(m)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (sk:Skill {id: 'skill-evidence-detail-1'}), (m:Memory {id: 'skill-evidence-memory-2'}) CREATE (sk)-[:SYNTHESIZED_FROM]->(m)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (sk:Skill {id: 'skill-evidence-detail-1'}) DETACH DELETE sk",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'skill-evidence-memory-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'skill-evidence-memory-2'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "skill metadata read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (sk:Skill {id: $id}) RETURN sk.stage, sk.metadata",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("skill-metadata-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("sk.stage", Value::String("published".to_string())),
                        ("sk.metadata", Value::String("{\"owner\":\"mem\"}".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Skill {id: 'skill-metadata-1', stage: 'published', metadata: '{\"owner\":\"mem\"}'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (sk:Skill {id: 'skill-metadata-1'}) DETACH DELETE sk",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "learning memory latest read",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.unit_type = 'learning' AND m.is_crystal = false AND m.is_latest = true RETURN m.id, m.title, m.content ORDER BY m.created_at DESC LIMIT 40",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("m.id", Value::String("learning-memory-2".to_string())),
                            ("m.title", Value::String("Learning Two".to_string())),
                            ("m.content", Value::String("newer learning".to_string())),
                        ]),
                        compatibility_row([
                            ("m.id", Value::String("learning-memory-1".to_string())),
                            ("m.title", Value::String("Learning One".to_string())),
                            ("m.content", Value::String("older learning".to_string())),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'learning-memory-1', title: 'Learning One', content: 'older learning', unit_type: 'learning', is_crystal: false, is_latest: true, created_at: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'learning-memory-2', title: 'Learning Two', content: 'newer learning', unit_type: 'learning', is_crystal: false, is_latest: true, created_at: 20})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'learning-memory-crystal', title: 'Crystal Learning', content: 'excluded crystal', unit_type: 'learning', is_crystal: true, is_latest: true, created_at: 30})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'learning-memory-1'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'learning-memory-2'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'learning-memory-crystal'}) DETACH DELETE m",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source parsed path list read",
                    CypherFixtureStatement::new(
                        "MATCH (src:Source) WHERE src.parsed_path IS NOT NULL RETURN src.id, src.original_name, src.parsed_path ORDER BY src.created_at DESC LIMIT 12",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("src.id", Value::String("parsed-source-2".to_string())),
                            ("src.original_name", Value::String("Parsed Two".to_string())),
                            (
                                "src.parsed_path",
                                Value::String("/tmp/parsed-two.md".to_string()),
                            ),
                        ]),
                        compatibility_row([
                            ("src.id", Value::String("parsed-source-1".to_string())),
                            ("src.original_name", Value::String("Parsed One".to_string())),
                            (
                                "src.parsed_path",
                                Value::String("/tmp/parsed-one.md".to_string()),
                            ),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'parsed-source-1', original_name: 'Parsed One', parsed_path: '/tmp/parsed-one.md', created_at: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'parsed-source-2', original_name: 'Parsed Two', parsed_path: '/tmp/parsed-two.md', created_at: 20})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'parsed-source-null', original_name: 'Parsed Null', parsed_path: NULL, created_at: 30})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (src:Source {id: 'parsed-source-1'}) DETACH DELETE src",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (src:Source {id: 'parsed-source-2'}) DETACH DELETE src",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (src:Source {id: 'parsed-source-null'}) DETACH DELETE src",
                    ),
                    ExpectedRows::RowCount(1),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "community summarized list read",
                    CypherFixtureStatement::new(
                        "MATCH (c:Community) WHERE c.ai_summary IS NOT NULL AND c.ai_summary <> '' RETURN c.community_id, c.name, c.ai_summary ORDER BY c.member_count DESC LIMIT 40",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("c.community_id", Value::Int(9520)),
                            (
                                "c.name",
                                Value::String("Summarized Community Two".to_string()),
                            ),
                            (
                                "c.ai_summary",
                                Value::String("higher member summary".to_string()),
                            ),
                        ]),
                        compatibility_row([
                            ("c.community_id", Value::Int(9510)),
                            (
                                "c.name",
                                Value::String("Summarized Community One".to_string()),
                            ),
                            (
                                "c.ai_summary",
                                Value::String("lower member summary".to_string()),
                            ),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'summarized-community-1', community_id: 9510, name: 'Summarized Community One', ai_summary: 'lower member summary', member_count: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'summarized-community-2', community_id: 9520, name: 'Summarized Community Two', ai_summary: 'higher member summary', member_count: 20})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'summarized-community-empty', community_id: 9530, name: 'Summarized Community Empty', ai_summary: '', member_count: 100})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'summarized-community-null', community_id: 9540, name: 'Summarized Community Null', member_count: 200})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['summarized-community-1', 'summarized-community-2', 'summarized-community-empty', 'summarized-community-null'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(4),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "community memory type-filter read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e:Entity {community_id: $cid})<-[:MENTIONS]-(m:Memory) WHERE m.is_crystal = false AND m.unit_type IN $types RETURN m.id, m.title LIMIT 200",
                        BTreeMap::from([
                            ("cid".to_string(), Value::Int(9600)),
                            (
                                "types".to_string(),
                                Value::List(vec![
                                    Value::String("learning".to_string()),
                                    Value::String("note".to_string()),
                                ]),
                            ),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "m.id",
                            Value::String("community-type-memory-hit".to_string()),
                        ),
                        (
                            "m.title",
                            Value::String("Community Type Memory Hit".to_string()),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'community-type-entity', community_id: 9600})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'community-type-memory-hit', title: 'Community Type Memory Hit', unit_type: 'note', is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'community-type-memory-crystal', title: 'Community Type Memory Crystal', unit_type: 'note', is_crystal: true})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'community-type-memory-type', title: 'Community Type Memory Type', unit_type: 'message', is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'community-type-memory-hit'}), (e:Entity {id: 'community-type-entity'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'community-type-memory-crystal'}), (e:Entity {id: 'community-type-entity'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'community-type-memory-type'}), (e:Entity {id: 'community-type-entity'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['community-type-memory-hit', 'community-type-memory-crystal', 'community-type-memory-type', 'community-type-entity'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(4),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "community entity memory-count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e:Entity) WHERE e.community_id = $community_id OPTIONAL MATCH (m:Memory)-[:MENTIONS]->(e) RETURN e.id, e.name, e.entity_type, COUNT(m) AS memory_count ORDER BY memory_count DESC LIMIT 10",
                        BTreeMap::from([("community_id".to_string(), Value::Int(9700))]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            (
                                "e.id",
                                Value::String("community-count-entity-1".to_string()),
                            ),
                            ("e.name", Value::String("Count Entity One".to_string())),
                            ("e.entity_type", Value::String("concept".to_string())),
                            ("memory_count", Value::Int(2)),
                        ]),
                        compatibility_row([
                            (
                                "e.id",
                                Value::String("community-count-entity-2".to_string()),
                            ),
                            ("e.name", Value::String("Count Entity Two".to_string())),
                            ("e.entity_type", Value::String("concept".to_string())),
                            ("memory_count", Value::Int(0)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'community-count-entity-1', name: 'Count Entity One', entity_type: 'concept', community_id: 9700})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'community-count-entity-2', name: 'Count Entity Two', entity_type: 'concept', community_id: 9700})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'community-count-entity-outside', name: 'Count Entity Outside', entity_type: 'concept', community_id: 9701})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'community-count-memory-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'community-count-memory-2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'community-count-memory-1'}), (e:Entity {id: 'community-count-entity-1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'community-count-memory-2'}), (e:Entity {id: 'community-count-entity-1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['community-count-memory-1', 'community-count-memory-2', 'community-count-entity-1', 'community-count-entity-2', 'community-count-entity-outside'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(5),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "synthesized source coverage lookup",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.is_crystal = true AND s.id IN $source_ids WITH c.id AS cid, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid LIMIT 1",
                        BTreeMap::from([
                            (
                                "source_ids".to_string(),
                                Value::List(vec![
                                    Value::String("coverage-s1".to_string()),
                                    Value::String("coverage-s2".to_string()),
                                ]),
                            ),
                            ("n".to_string(), Value::Int(2)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "cid",
                        Value::String("coverage-c1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'coverage-c1', is_crystal: true, crystal_title: 'Coverage Crystal'})-[:SYNTHESIZED_FROM]->(:Memory {id: 'coverage-s1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'coverage-s2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (c:Memory {id: 'coverage-c1'}), (s:Memory {id: 'coverage-s2'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (c:Memory {id: 'coverage-c1'}), (s:Memory {id: 'coverage-s2'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)",
                )),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "synthesized source coverage title lookup",
                CypherFixtureStatement::with_parameters(
                    "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.is_crystal = true AND s.id IN $source_ids WITH c.id AS cid, c.crystal_title AS ct, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid, ct LIMIT 1",
                    BTreeMap::from([
                        (
                            "source_ids".to_string(),
                            Value::List(vec![
                                Value::String("coverage-s1".to_string()),
                                Value::String("coverage-s2".to_string()),
                            ]),
                        ),
                        ("n".to_string(), Value::Int(2)),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("cid", Value::String("coverage-c1".to_string())),
                    ("ct", Value::String("Coverage Crystal".to_string())),
                ])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "top entities by degree",
                CypherFixtureStatement::new(
                    "MATCH (e:Entity) OPTIONAL MATCH (e)-[r]-() WITH e, COUNT(r) as degree RETURN e.id, e.name, degree ORDER BY degree DESC LIMIT 10",
                ),
                ExpectedRows::RowCount(2),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity mention-count list read",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity) OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) RETURN e.id, e.name, e.entity_type, e.description, e.aliases, e.confidence, e.community_id, e.created_at, COUNT(r) AS mention_count ORDER BY mention_count DESC, e.name ASC LIMIT $limit",
                    BTreeMap::from([("limit".to_string(), Value::Int(10))]),
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([
                        ("e.id", Value::Int(11)),
                        ("e.name", Value::String("Cypher".to_string())),
                        ("e.entity_type", Value::Null),
                        ("e.description", Value::Null),
                        ("e.aliases", Value::Null),
                        ("e.confidence", Value::Null),
                        ("e.community_id", Value::Null),
                        ("e.created_at", Value::Null),
                        ("mention_count", Value::Int(1)),
                    ]),
                    compatibility_row([
                        ("e.id", Value::Int(10)),
                        ("e.name", Value::String("Rust".to_string())),
                        ("e.entity_type", Value::Null),
                        ("e.description", Value::Null),
                        (
                            "e.aliases",
                            Value::List(vec![
                                Value::String("Ferris".to_string()),
                                Value::String("Rustacean".to_string()),
                            ]),
                        ),
                        ("e.confidence", Value::Null),
                        ("e.community_id", Value::Null),
                        ("e.created_at", Value::Null),
                        ("mention_count", Value::Int(1)),
                    ]),
                ]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "node detail neighbor counts",
                CypherFixtureStatement::with_parameters(
                    "MATCH (n)-[r]-(neighbor) WHERE n.id = $node_id RETURN COUNT(DISTINCT neighbor), COUNT(r)",
                    BTreeMap::from([(
                        "node_id".to_string(),
                        Value::Int(1),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    ("count(DISTINCT neighbor)", Value::Int(2)),
                    ("count(r)", Value::Int(3)),
                ])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label usage count read",
                    CypherFixtureStatement::new(
                        "MATCH (l:Label) OPTIONAL MATCH (l)<-[:HAS_LABEL]-(n) WITH l, COUNT(n) as usage_count RETURN l.id, l.name, l.canonical_name, usage_count ORDER BY usage_count DESC, l.id ASC",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("l.id", Value::String("usage-label-1".to_string())),
                            ("l.name", Value::String("Important".to_string())),
                            ("l.canonical_name", Value::String("important".to_string())),
                            ("usage_count", Value::Int(2)),
                        ]),
                        compatibility_row([
                            ("l.id", Value::String("usage-label-2".to_string())),
                            ("l.name", Value::String("Unused".to_string())),
                            ("l.canonical_name", Value::String("unused".to_string())),
                            ("usage_count", Value::Int(0)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "MERGE (:Label {id: 'usage-label-1', name: 'Important', canonical_name: 'important'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MERGE (:Label {id: 'usage-label-2', name: 'Unused', canonical_name: 'unused'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1}), (l:Label {id: 'usage-label-1'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (e:Entity {id: 10}), (l:Label {id: 'usage-label-1'}) CREATE (e)-[:HAS_LABEL]->(l)",
                )),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "label usage direct optional count read",
                CypherFixtureStatement::with_parameters(
                    "MATCH (l:Label) OPTIONAL MATCH (m:Memory)-[:HAS_LABEL]->(l) RETURN l.id, l.name, COUNT(m) AS usage_count ORDER BY l.name ASC SKIP $offset LIMIT $limit",
                    BTreeMap::from([
                        ("offset".to_string(), Value::Int(0)),
                        ("limit".to_string(), Value::Int(200)),
                    ]),
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([
                        ("l.id", Value::String("usage-label-1".to_string())),
                        ("l.name", Value::String("Important".to_string())),
                        ("usage_count", Value::Int(1)),
                    ]),
                    compatibility_row([
                        ("l.id", Value::String("usage-label-2".to_string())),
                        ("l.name", Value::String("Unused".to_string())),
                        ("usage_count", Value::Int(0)),
                    ]),
                ]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity mention fan-in count",
                CypherFixtureStatement::with_parameters(
                    "MATCH (:Memory)-[r:MENTIONS]->(e:Entity {id: $id}) RETURN COUNT(r)",
                    BTreeMap::from([("id".to_string(), Value::Int(10))]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "entity lifecycle detail projection",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e:Entity {id: $id}) RETURN e.id, e.name, e.entity_type, e.description, e.aliases",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("entity-life-a".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("e.id", Value::String("entity-life-a".to_string())),
                        ("e.name", Value::String("Lifecycle A".to_string())),
                        ("e.entity_type", Value::String("concept".to_string())),
                        (
                            "e.description",
                            Value::String("Lifecycle entity".to_string()),
                        ),
                        (
                            "e.aliases",
                            Value::List(vec![Value::String("life-a".to_string())]),
                        ),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'entity-life-a', name: 'Lifecycle A', entity_type: 'concept', description: 'Lifecycle entity', aliases: ['life-a']})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'entity-life-b', name: 'Lifecycle B', entity_type: 'concept', description: 'Related entity', aliases: ['life-b']})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'entity-life-a'}), (b:Entity {id: 'entity-life-b'}) CREATE (a)-[:RELATES_TO {confidence: 0.9, strength: 0.8, relation_type: 'supports'}]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'entity-life-label', name: 'Lifecycle', canonical_name: 'lifecycle'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (e:Entity {id: 'entity-life-a'}), (l:Label {id: 'entity-life-label'}) CREATE (e)-[:HAS_LABEL {assigned_by: 'system'}]->(l)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'entity-life-community', name: 'Lifecycle Community'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (e:Entity {id: 'entity-life-a'}), (c:Community {id: 'entity-life-community'}) CREATE (e)-[:BELONGS_TO {score: 0.7}]->(c)",
                )),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity outgoing relates count",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity {id: $id})-[r:RELATES_TO]->(:Entity) RETURN COUNT(r)",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-a".to_string()),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity incoming relates count",
                CypherFixtureStatement::with_parameters(
                    "MATCH (:Entity)-[r:RELATES_TO]->(e:Entity {id: $id}) RETURN COUNT(r)",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-b".to_string()),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source relationship source-reference count read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e1:Entity)-[r:RELATES_TO]->(e2:Entity) WHERE r.source_reference = $id RETURN count(r)",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("source-ref-count".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'source-ref-count-e1', name: 'Source Ref Count One'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'source-ref-count-e2', name: 'Source Ref Count Two'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'source-ref-count-e3', name: 'Source Ref Count Three'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'source-ref-count-e1'}), (b:Entity {id: 'source-ref-count-e2'}) CREATE (a)-[:RELATES_TO {source_reference: 'source-ref-count'}]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'source-ref-count-e1'}), (b:Entity {id: 'source-ref-count-e3'}) CREATE (a)-[:RELATES_TO {source_reference: 'other-source'}]->(b)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-ref-count-e1', 'source-ref-count-e2', 'source-ref-count-e3'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source relationship source-reference delete",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e1:Entity)-[r:RELATES_TO]->(e2:Entity) WHERE r.source_reference = $id DELETE r",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("source-ref-delete".to_string()),
                        )]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'source-ref-delete-e1', name: 'Source Ref Delete One'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'source-ref-delete-e2', name: 'Source Ref Delete Two'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'source-ref-delete-e3', name: 'Source Ref Delete Three'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'source-ref-delete-e1'}), (b:Entity {id: 'source-ref-delete-e2'}) CREATE (a)-[:RELATES_TO {source_reference: 'source-ref-delete'}]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'source-ref-delete-e1'}), (b:Entity {id: 'source-ref-delete-e3'}) CREATE (a)-[:RELATES_TO {source_reference: 'other-source'}]->(b)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['source-ref-delete-e1', 'source-ref-delete-e2', 'source-ref-delete-e3'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity outgoing relation preview",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity {id: $id})-[r:RELATES_TO]->(other:Entity) RETURN other.id, r",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-a".to_string()),
                    )]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity incoming relation preview",
                CypherFixtureStatement::with_parameters(
                    "MATCH (other:Entity)-[r:RELATES_TO]->(e:Entity {id: $id}) RETURN other.id, r",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-b".to_string()),
                    )]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity has label count",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity {id: $id})-[r:HAS_LABEL]->(:Label) RETURN COUNT(r)",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-a".to_string()),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity has label preview",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity {id: $id})-[r:HAS_LABEL]->(n:Label) RETURN n.id, r",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-a".to_string()),
                    )]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity belongs community count",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity {id: $id})-[r:BELONGS_TO]->(:Community) RETURN COUNT(r)",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-a".to_string()),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "entity belongs community preview",
                CypherFixtureStatement::with_parameters(
                    "MATCH (e:Entity {id: $id})-[r:BELONGS_TO]->(n:Community) RETURN n.id, r",
                    BTreeMap::from([(
                        "id".to_string(),
                        Value::String("entity-life-a".to_string()),
                    )]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "entity detach delete cascade",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e:Entity {id: $id}) DETACH DELETE e",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("entity-delete-target".to_string()),
                        )]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'entity-delete-target', name: 'Delete Target'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1}), (e:Entity {id: 'entity-delete-target'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (e:Entity {id: 'entity-delete-target'}) OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) RETURN count(e) AS entities, count(r) AS rels",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("entities", Value::Int(0)),
                        ("rels", Value::Int(0)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "graph orphan entity read",
                    CypherFixtureStatement::new(
                        "MATCH (e:Entity) WHERE NOT (e)<-[:MENTIONS]-(:Memory) AND NOT (e)-[:RELATES_TO]-() AND NOT (e)-[:HAS_LABEL]-() RETURN e.id, e.name, e.entity_type",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("e.id", Value::String("orphan-entity".to_string())),
                        ("e.name", Value::String("Orphan Entity".to_string())),
                        ("e.entity_type", Value::String("concept".to_string())),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'orphan-entity', name: 'Orphan Entity', entity_type: 'concept'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'orphan-labeled', name: 'Labeled Orphan Candidate', entity_type: 'concept'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'orphan-blocking-label', name: 'Blocking Label'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (e:Entity {id: 'orphan-labeled'}), (l:Label {id: 'orphan-blocking-label'}) CREATE (e)-[:HAS_LABEL]->(l)",
                )),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "graph orphan cleanup candidate read",
                CypherFixtureStatement::new(
                    "MATCH (e:Entity) WHERE NOT (e)<-[:MENTIONS]-(:Memory) AND NOT (e)-[:RELATES_TO]-() AND NOT (e)-[:HAS_LABEL]-() RETURN e.id",
                ),
                ExpectedRows::Exact(vec![compatibility_row([(
                    "e.id",
                    Value::String("orphan-entity".to_string()),
                )])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "augmentation job create",
                    CypherFixtureStatement::with_parameters(
                        "CREATE (j:AugmentationJob {job_id: $job_id, job_type: $job_type, status: 'pending', progress: 0.0, message: 'Job created', parameters: $parameters, result: '{}', error_message: '', started_at: NULL, completed_at: NULL, created_at: CURRENT_TIMESTAMP()})",
                        BTreeMap::from([
                            (
                                "job_id".to_string(),
                                Value::String("job-ledger-1".to_string()),
                            ),
                            (
                                "job_type".to_string(),
                                Value::String("pagerank_calculation".to_string()),
                            ),
                            ("parameters".to_string(), Value::String("{}".to_string())),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (j:AugmentationJob {job_id: 'job-ledger-1'}) RETURN j.status AS status, j.progress AS progress, count(j.created_at) AS created, count(j.started_at) AS started",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("status", Value::String("pending".to_string())),
                        ("progress", Value::Float(0.0)),
                        ("created", Value::Int(1)),
                        ("started", Value::Int(0)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "augmentation job mark running",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.status = 'running', j.started_at = CURRENT_TIMESTAMP(), j.message = 'Job started'",
                        BTreeMap::from([(
                            "job_id".to_string(),
                            Value::String("job-ledger-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (j:AugmentationJob {job_id: 'job-ledger-1'}) RETURN j.status AS status, j.message AS message, count(j.started_at) AS started",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("status", Value::String("running".to_string())),
                        ("message", Value::String("Job started".to_string())),
                        ("started", Value::Int(1)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "augmentation job progress update",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.progress = $progress, j.message = $message",
                        BTreeMap::from([
                            (
                                "job_id".to_string(),
                                Value::String("job-ledger-1".to_string()),
                            ),
                            ("progress".to_string(), Value::Float(30.0)),
                            (
                                "message".to_string(),
                                Value::String("Running PageRank algorithm...".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (j:AugmentationJob {job_id: 'job-ledger-1'}) RETURN j.progress AS progress, j.message AS message",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("progress", Value::Float(30.0)),
                        (
                            "message",
                            Value::String("Running PageRank algorithm...".to_string()),
                        ),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "augmentation job mark completed",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.status = 'completed', j.progress = 100.0, j.message = 'Job completed successfully', j.result = $result, j.completed_at = CURRENT_TIMESTAMP()",
                        BTreeMap::from([
                            (
                                "job_id".to_string(),
                                Value::String("job-ledger-1".to_string()),
                            ),
                            (
                                "result".to_string(),
                                Value::String("{\"summary_only\":true}".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (j:AugmentationJob {job_id: 'job-ledger-1'}) RETURN j.status AS status, j.progress AS progress, j.message AS message, j.result AS result, count(j.completed_at) AS completed",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("status", Value::String("completed".to_string())),
                        ("progress", Value::Float(100.0)),
                        (
                            "message",
                            Value::String("Job completed successfully".to_string()),
                        ),
                        ("result", Value::String("{\"summary_only\":true}".to_string())),
                        ("completed", Value::Int(1)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "augmentation job mark failed",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.status = 'failed', j.message = 'Job failed', j.error_message = $error_message, j.completed_at = CURRENT_TIMESTAMP()",
                        BTreeMap::from([
                            (
                                "job_id".to_string(),
                                Value::String("job-ledger-fail".to_string()),
                            ),
                            (
                                "error_message".to_string(),
                                Value::String("boom".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:AugmentationJob {job_id: 'job-ledger-fail', job_type: 'community_detection', status: 'running', progress: 5.0, message: 'Running', result: '{}', error_message: '', created_at: 1})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (j:AugmentationJob {job_id: 'job-ledger-fail'}) RETURN j.status AS status, j.message AS message, j.error_message AS error, count(j.completed_at) AS completed",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("status", Value::String("failed".to_string())),
                        ("message", Value::String("Job failed".to_string())),
                        ("error", Value::String("boom".to_string())),
                        ("completed", Value::Int(1)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "augmentation job status read",
                CypherFixtureStatement::with_parameters(
                    "MATCH (j:AugmentationJob {job_id: $job_id}) RETURN j.job_type, j.status, j.progress, j.message, j.result, j.error_message, j.started_at, j.completed_at",
                    BTreeMap::from([(
                        "job_id".to_string(),
                        Value::String("job-ledger-1".to_string()),
                    )]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "augmentation job filtered list read",
                CypherFixtureStatement::with_parameters(
                    "MATCH (j:AugmentationJob) WHERE j.status = $status RETURN j.job_id, j.job_type, j.status, j.progress, j.message, j.created_at ORDER BY j.created_at DESC LIMIT $job_limit",
                    BTreeMap::from([
                        ("status".to_string(), Value::String("completed".to_string())),
                        ("job_limit".to_string(), Value::Int(10)),
                    ]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "augmentation stale interrupt write",
                    CypherFixtureStatement::new(
                        "MATCH (j:AugmentationJob) WHERE j.status = 'pending' OR j.status = 'running' SET j.status = 'failed', j.message = 'Interrupted before completion', j.error_message = 'Interrupted because the app restarted before completion.', j.completed_at = CURRENT_TIMESTAMP()",
                    ),
                    ExpectedRows::RowCount(2),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:AugmentationJob {job_id: 'job-stale-pending', job_type: 'pagerank_calculation', status: 'pending', progress: 0.0, message: 'Job created', created_at: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:AugmentationJob {job_id: 'job-stale-running', job_type: 'community_detection', status: 'running', progress: 10.0, message: 'Running', created_at: 11})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (j:AugmentationJob) WHERE j.job_id IN ['job-stale-pending', 'job-stale-running'] RETURN j.job_id AS id, j.status AS status ORDER BY id ASC",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("id", Value::String("job-stale-pending".to_string())),
                            ("status", Value::String("failed".to_string())),
                        ]),
                        compatibility_row([
                            ("id", Value::String("job-stale-running".to_string())),
                            ("status", Value::String("failed".to_string())),
                        ]),
                    ]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "thread move source space selection",
                CypherFixtureStatement::with_parameters(
                    "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id RETURN t.id, t.thread_id, t.space_id ORDER BY t.id",
                    BTreeMap::from([
                        (
                            "thread_ids".to_string(),
                            Value::List(vec![
                                Value::String("logical-1".to_string()),
                                Value::String("logical-2".to_string()),
                                Value::String("logical-3".to_string()),
                            ]),
                        ),
                        (
                            "source_space_id".to_string(),
                            Value::String("default".to_string()),
                        ),
                    ]),
                ),
                ExpectedRows::Exact(vec![
                    compatibility_row([
                        ("t.id", Value::String("storage-1".to_string())),
                        ("t.thread_id", Value::String("logical-1".to_string())),
                        ("t.space_id", Value::Null),
                    ]),
                    compatibility_row([
                        ("t.id", Value::String("storage-2".to_string())),
                        ("t.thread_id", Value::String("logical-2".to_string())),
                        ("t.space_id", Value::String(String::new())),
                    ]),
                ]),
            )
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'storage-1', thread_id: 'logical-1'})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'storage-2', thread_id: 'logical-2', space_id: ''})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'storage-3', thread_id: 'logical-3', space_id: 'team'})",
            ))),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "thread move target space exclusion",
                CypherFixtureStatement::with_parameters(
                    "MATCH (t:Thread) WHERE t.thread_id IN $candidate_ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target_space_id RETURN t.thread_id ORDER BY t.thread_id",
                    BTreeMap::from([
                        (
                            "candidate_ids".to_string(),
                            Value::List(vec![
                                Value::String("logical-4".to_string()),
                                Value::String("logical-5".to_string()),
                                Value::String("logical-6".to_string()),
                            ]),
                        ),
                        (
                            "target_space_id".to_string(),
                            Value::String("default".to_string()),
                        ),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([(
                    "t.thread_id",
                    Value::String("logical-6".to_string()),
                )])]),
            )
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'storage-4', thread_id: 'logical-4'})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'storage-5', thread_id: 'logical-5', space_id: ''})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'storage-6', thread_id: 'logical-6', space_id: 'team'})",
            ))),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread move source space update returning ids",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id SET t.space_id = $target_space_id, t.updated_at = $updated_at RETURN t.thread_id",
                        BTreeMap::from([
                            (
                                "thread_ids".to_string(),
                                Value::List(vec![
                                    Value::String("logical-7".to_string()),
                                    Value::String("logical-8".to_string()),
                                    Value::String("logical-9".to_string()),
                                ]),
                            ),
                            (
                                "source_space_id".to_string(),
                                Value::String("default".to_string()),
                            ),
                            (
                                "target_space_id".to_string(),
                                Value::String("archive".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(77)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([(
                            "t.thread_id",
                            Value::String("logical-7".to_string()),
                        )]),
                        compatibility_row([(
                            "t.thread_id",
                            Value::String("logical-8".to_string()),
                        )]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'storage-7', thread_id: 'logical-7'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'storage-8', thread_id: 'logical-8', space_id: ''})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'storage-9', thread_id: 'logical-9', space_id: 'team'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (t:Thread) WHERE t.thread_id IN ['logical-7', 'logical-8', 'logical-9'] RETURN t.thread_id, t.space_id ORDER BY t.thread_id",
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("t.thread_id", Value::String("logical-7".to_string())),
                            ("t.space_id", Value::String("archive".to_string())),
                        ]),
                        compatibility_row([
                            ("t.thread_id", Value::String("logical-8".to_string())),
                            ("t.space_id", Value::String("archive".to_string())),
                        ]),
                        compatibility_row([
                            ("t.thread_id", Value::String("logical-9".to_string())),
                            ("t.space_id", Value::String("team".to_string())),
                        ]),
                    ]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "thread distill optional source count all",
                CypherFixtureStatement::with_parameters(
                    "MATCH (t:Thread) WHERE t.thread_id IS NOT NULL AND (CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END) = $space_id AND ($source IS NULL OR t.source = $source) RETURN COUNT(t)",
                    BTreeMap::from([
                        (
                            "space_id".to_string(),
                            Value::String("distill-space".to_string()),
                        ),
                        ("source".to_string(), Value::Null),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(t)", Value::Int(2))])]),
            )
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'distill-thread-1', thread_id: 'distill-logical-1', source: 'codex', space_id: 'distill-space'})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'distill-thread-2', thread_id: 'distill-logical-2', source: 'claude', space_id: 'distill-space'})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'distill-thread-3', thread_id: 'distill-logical-3', source: 'codex', space_id: 'other-space'})",
            ))),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "thread distill optional source count filtered",
                CypherFixtureStatement::with_parameters(
                    "MATCH (t:Thread) WHERE t.thread_id IS NOT NULL AND (CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END) = $space_id AND ($source IS NULL OR t.source = $source) RETURN COUNT(t)",
                    BTreeMap::from([
                        (
                            "space_id".to_string(),
                            Value::String("distill-space".to_string()),
                        ),
                        ("source".to_string(), Value::String("codex".to_string())),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(t)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "thread optional message count",
                CypherFixtureStatement::with_parameters(
                    "MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
                    BTreeMap::from([(
                        "thread_uuid".to_string(),
                        Value::String("thread-1".to_string()),
                    )]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(m)", Value::Int(2))])]),
            )
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Thread {id: 'thread-1'})-[:CONTAINS]->(:Message {id: 'msg-1', order_index: 1})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Message {id: 'msg-2', order_index: 2})",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "MATCH (t:Thread {id: 'thread-1'}), (m:Message {id: 'msg-2'}) CREATE (t)-[:CONTAINS]->(m)",
            ))
            .with_setup_query(CypherFixtureStatement::new(
                "MATCH (mem:Memory {id: 1}), (msg:Message {id: 'msg-1'}) CREATE (mem)-[:EXTRACTED_FROM]->(msg)",
            ))),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread message ordered read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread {id: $thread_uuid})-[c:CONTAINS]->(m:Message) RETURN m.id, m.content, m.role, COALESCE(c.order_index, m.order_index), m.timestamp ORDER BY COALESCE(c.order_index, m.order_index)",
                        BTreeMap::from([(
                            "thread_uuid".to_string(),
                            Value::String("thread-ordered-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("m.id", Value::String("ordered-msg-2".to_string())),
                            ("m.content", Value::String("second by node".to_string())),
                            ("m.role", Value::String("assistant".to_string())),
                            ("coalesce", Value::Int(0)),
                            ("m.timestamp", Value::Int(20)),
                        ]),
                        compatibility_row([
                            ("m.id", Value::String("ordered-msg-1".to_string())),
                            ("m.content", Value::String("first by rel".to_string())),
                            ("m.role", Value::String("user".to_string())),
                            ("coalesce", Value::Int(1)),
                            ("m.timestamp", Value::Int(10)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-ordered-1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Message {id: 'ordered-msg-1', content: 'first by rel', role: 'user', order_index: 100, timestamp: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Message {id: 'ordered-msg-2', content: 'second by node', role: 'assistant', order_index: 0, timestamp: 20})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'thread-ordered-1'}), (m:Message {id: 'ordered-msg-1'}) CREATE (t)-[:CONTAINS {order_index: 1}]->(m)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'thread-ordered-1'}), (m:Message {id: 'ordered-msg-2'}) CREATE (t)-[:CONTAINS]->(m)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['thread-ordered-1', 'ordered-msg-1', 'ordered-msg-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(3),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "thread message detach delete",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m",
                        BTreeMap::from([(
                            "thread_uuid".to_string(),
                            Value::String("thread-delete-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::RowCount(2),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Thread {id: 'thread-delete-1'})-[:CONTAINS]->(:Message {id: 'delete-msg-1', order_index: 1})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Message {id: 'delete-msg-2', order_index: 2})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (t:Thread {id: 'thread-delete-1'}), (m:Message {id: 'delete-msg-2'}) CREATE (t)-[:CONTAINS]->(m)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Message) WHERE m.id STARTS WITH 'delete-msg-' RETURN count(m) AS messages",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("messages", Value::Int(0))])]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "legacy tail extracted refs count",
                CypherFixtureStatement::with_parameters(
                    "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) WHERE m.order_index >= $start_index OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
                    BTreeMap::from([
                        (
                            "thread_uuid".to_string(),
                            Value::String("thread-1".to_string()),
                        ),
                        ("start_index".to_string(), Value::Int(0)),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([("count(r)", Value::Int(1))])]),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "merge node on create set",
                    CypherFixtureStatement::new(
                        "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP()",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:SchemaMigrationLog {id: 'migration-1'}) RETURN count(m.applied_at) AS applied",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("applied", Value::Int(1))])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "graph meta merge post set",
                    CypherFixtureStatement::new(
                        "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = true, m.pagerank_algorithm = 'pagerank', m.pagerank_iterations = 20, m.pagerank_computed_at = CURRENT_TIMESTAMP(), m.updated_at = CURRENT_TIMESTAMP()",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_algorithm AS algorithm, m.pagerank_iterations AS iterations, count(m.pagerank_computed_at) AS computed",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("applied", Value::Bool(true)),
                        ("algorithm", Value::String("pagerank".to_string())),
                        ("iterations", Value::Int(20)),
                        ("computed", Value::Int(1)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source metadata timestamp update",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $id}) SET s.metadata = $metadata, s.updated_at = timestamp($updated_at)",
                        BTreeMap::from([
                            (
                                "id".to_string(),
                                Value::String("metadata-source-1".to_string()),
                            ),
                            ("metadata".to_string(), Value::String("{\"ocr\":true}".to_string())),
                            (
                                "updated_at".to_string(),
                                Value::String("1970-01-01T00:00:00".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'metadata-source-1', metadata: '{}', updated_at: 1})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'metadata-source-1'}) RETURN s.metadata AS metadata, s.updated_at AS updated_at",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("metadata", Value::String("{\"ocr\":true}".to_string())),
                        ("updated_at", Value::Int(0)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory relation multi property update",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (source:Memory)-[r:MEMORY_RELATES_TO]->(target:Memory) WHERE r.id = $relation_id SET r.relation_type = $relation_type, r.strength = $strength, r.confidence = $confidence, r.status = $status, r.reason = $reason, r.updated_at = timestamp($updated_at)",
                        BTreeMap::from([
                            ("relation_id".to_string(), Value::String("rel-1".to_string())),
                            ("relation_type".to_string(), Value::String("supports".to_string())),
                            ("strength".to_string(), Value::Float(0.7)),
                            ("confidence".to_string(), Value::Float(0.8)),
                            ("status".to_string(), Value::String("reviewed".to_string())),
                            ("reason".to_string(), Value::String("fixture".to_string())),
                            (
                                "updated_at".to_string(),
                                Value::String("1970-01-01T00:00:00".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE RELATIONSHIP TYPE MEMORY_RELATES_TO",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'rel-source'})-[:MEMORY_RELATES_TO {id: 'rel-1', relation_type: 'old', strength: 0.1, confidence: 0.2, status: 'draft', reason: '', updated_at: 10}]->(:Memory {id: 'rel-target'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.id = 'rel-1' RETURN r.relation_type AS relation_type, r.strength AS strength, r.confidence AS confidence, r.status AS status, r.reason AS reason, r.updated_at AS updated_at",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("relation_type", Value::String("supports".to_string())),
                        ("strength", Value::Float(0.7)),
                        ("confidence", Value::Float(0.8)),
                        ("status", Value::String("reviewed".to_string())),
                        ("reason", Value::String("fixture".to_string())),
                        ("updated_at", Value::Int(0)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source revision history path read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH p = (s:Source {id: $source_id})-[:REVISED_AS*1..10]->(older:Source) RETURN older.id as id, older.original_name as name, older.version as version, older.sha256 as sha256, older.created_at as created_at ORDER BY older.version DESC",
                        BTreeMap::from([(
                            "source_id".to_string(),
                            Value::String("source-current".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("id", Value::String("source-v2".to_string())),
                            ("name", Value::String("Draft v2".to_string())),
                            ("version", Value::Int(2)),
                            ("sha256", Value::String("sha-v2".to_string())),
                            ("created_at", Value::Int(20)),
                        ]),
                        compatibility_row([
                            ("id", Value::String("source-v1".to_string())),
                            ("name", Value::String("Draft v1".to_string())),
                            ("version", Value::Int(1)),
                            ("sha256", Value::String("sha-v1".to_string())),
                            ("created_at", Value::Int(10)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-current', original_name: 'Draft current', version: 3, sha256: 'sha-current', created_at: 30})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-v2', original_name: 'Draft v2', version: 2, sha256: 'sha-v2', created_at: 20})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'source-v1', original_name: 'Draft v1', version: 1, sha256: 'sha-v1', created_at: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (newer:Source {id: 'source-current'}), (older:Source {id: 'source-v2'}) CREATE (newer)-[:REVISED_AS]->(older)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (newer:Source {id: 'source-v2'}), (older:Source {id: 'source-v1'}) CREATE (newer)-[:REVISED_AS]->(older)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "source detach delete cascade",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (s:Source {id: $id}) DETACH DELETE s",
                        BTreeMap::from([(
                            "id".to_string(),
                            Value::String("delete-source-1".to_string()),
                        )]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Source {id: 'delete-source-1', original_name: 'Delete me'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 1}), (s:Source {id: 'delete-source-1'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 9}]->(s)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (s:Source {id: 'delete-source-1'}) OPTIONAL MATCH (:Memory)-[r:SOURCED_FROM]->(s) RETURN count(s) AS sources, count(r) AS rels",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("sources", Value::Int(0)),
                        ("rels", Value::Int(0)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label merge node on create seed",
                    CypherFixtureStatement::new(
                        "MERGE (l:Label {id: 'label-1'}) ON CREATE SET l.name = 'Important', l.canonical_name = null, l.created_at = CURRENT_TIMESTAMP(), l.updated_at = 1",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (l:Label {id: 'label-1'}) RETURN l.name AS name, count(l.created_at) AS created",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("name", Value::String("Important".to_string())),
                        ("created", Value::Int(1)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label merge node on create and match set",
                    CypherFixtureStatement::new(
                        "MERGE (l:Label {id: 'label-1'}) ON CREATE SET l.name = 'Important', l.canonical_name = null, l.created_at = CURRENT_TIMESTAMP(), l.updated_at = 1 ON MATCH SET l.updated_at = 2, l.canonical_name = COALESCE(l.canonical_name, 'important')",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (l:Label {id: 'label-1'}) RETURN l.name AS name, l.canonical_name AS canonical, l.updated_at AS updated, count(l.created_at) AS created",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("name", Value::String("Important".to_string())),
                        ("canonical", Value::String("important".to_string())),
                        ("updated", Value::Int(2)),
                        ("created", Value::Int(1)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "matched relationship merge on create set",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1}), (l:Label {id: 'label-1'}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'system', r.properties = '{}'",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1})-[r:HAS_LABEL]->(l:Label {id: 'label-1'}) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("total", Value::Int(1)),
                        ("assigned_by", Value::String("system".to_string())),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label merge transfer retarget relationship",
                    CypherFixtureStatement::new(
                        "MATCH (n:Memory)-[:HAS_LABEL]->(src:Label {id: 'label-1'}) MATCH (tgt:Label {id: 'label-2'}) MERGE (n)-[r:HAS_LABEL]->(tgt) ON CREATE SET r.assigned_by = 'label_merge', r.created_at = 7, r.properties = '{}'",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'label-2', name: 'Merged'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1})-[r:HAS_LABEL]->(l:Label {id: 'label-2'}) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by, min(r.created_at) AS created_at",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("total", Value::Int(1)),
                        ("assigned_by", Value::String("label_merge".to_string())),
                        ("created_at", Value::Int(7)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label canonical lookup",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (l:Label) WHERE l.canonical_name = $c RETURN l.id LIMIT 1",
                        BTreeMap::from([(
                            "c".to_string(),
                            Value::String("canonical_lookup_unique".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([(
                        "l.id",
                        Value::String("label-canonical-1".to_string()),
                    )])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'label-canonical-1', name: 'Canonical', canonical_name: 'canonical_lookup_unique'})",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label dynamic update set",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (l:Label {id: $label_id}) SET l.updated_at = $updated_at, l.name = $name, l.canonical_name = $canonical",
                        BTreeMap::from([
                            (
                                "label_id".to_string(),
                                Value::String("label-1".to_string()),
                            ),
                            ("updated_at".to_string(), Value::Int(3)),
                            ("name".to_string(), Value::String("Important Updated".to_string())),
                            (
                                "canonical".to_string(),
                                Value::String("important_updated".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (l:Label {id: 'label-1'}) RETURN l.name AS name, l.canonical_name AS canonical, l.updated_at AS updated",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("name", Value::String("Important Updated".to_string())),
                        ("canonical", Value::String("important_updated".to_string())),
                        ("updated", Value::Int(3)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label relationship remove from memory",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory {id: $memory_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r",
                        BTreeMap::from([
                            ("memory_id".to_string(), Value::Int(1)),
                            (
                                "label_id".to_string(),
                                Value::String("label-1".to_string()),
                            ),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 1})-[r:HAS_LABEL]->(l:Label {id: 'label-1'}) RETURN count(r) AS total",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([("total", Value::Int(0))])]),
                ),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "label detach delete",
                CypherFixtureStatement::with_parameters(
                    "MATCH (l:Label {id: $label_id}) DETACH DELETE l",
                    BTreeMap::from([(
                        "label_id".to_string(),
                        Value::String("label-1".to_string()),
                    )]),
                ),
                ExpectedRows::RowCount(1),
            )),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "mention relationship target-filter multi property update",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory {id: $memory_id})-[r:MENTIONS]->(e:Entity {id: $target_id}) SET r.confidence = $confidence, r.mention_count = $mention_count, r.created_at = $created_at",
                        BTreeMap::from([
                            (
                                "memory_id".to_string(),
                                Value::String("mention-memory".to_string()),
                            ),
                            (
                                "target_id".to_string(),
                                Value::String("mention-entity".to_string()),
                            ),
                            ("confidence".to_string(), Value::Float(0.92)),
                            ("mention_count".to_string(), Value::Int(3)),
                            ("created_at".to_string(), Value::Int(42)),
                        ]),
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'mention-memory'})-[:MENTIONS {confidence: 0.1, mention_count: 1, created_at: 1}]->(:Entity {id: 'mention-entity'})",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory {id: 'mention-memory'})-[r:MENTIONS]->(e:Entity {id: 'mention-entity'}) RETURN r.confidence AS confidence, r.mention_count AS mention_count, r.created_at AS created_at",
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("confidence", Value::Float(0.92)),
                        ("mention_count", Value::Int(3)),
                        ("created_at", Value::Int(42)),
                    ])]),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory monthly date part aggregate read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.created_at IS NOT NULL AND m.space_id = $space_id WITH date_part('year', m.created_at) AS year, date_part('month', m.created_at) AS month, COUNT(m) AS memory_count RETURN year, month, memory_count ORDER BY year DESC, month DESC LIMIT $months",
                        BTreeMap::from([
                            (
                                "space_id".to_string(),
                                Value::String("date-part-space".to_string()),
                            ),
                            ("months".to_string(), Value::Int(3)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("year", Value::Int(2024)),
                            ("month", Value::Int(2)),
                            ("memory_count", Value::Int(2)),
                        ]),
                        compatibility_row([
                            ("year", Value::Int(2024)),
                            ("month", Value::Int(1)),
                            ("memory_count", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::with_parameters(
                    "CREATE (:Memory {id: 'date-part-jan', space_id: 'date-part-space', lifecycle_state: 'archived', created_at: timestamp($created_at)})",
                    BTreeMap::from([(
                        "created_at".to_string(),
                        Value::String("2024-01-15T00:00:00".to_string()),
                    )]),
                ))
                .with_setup_query(CypherFixtureStatement::with_parameters(
                    "CREATE (:Memory {id: 'date-part-feb-1', space_id: 'date-part-space', lifecycle_state: 'archived', created_at: timestamp($created_at)})",
                    BTreeMap::from([(
                        "created_at".to_string(),
                        Value::String("2024-02-01T00:00:00".to_string()),
                    )]),
                ))
                .with_setup_query(CypherFixtureStatement::with_parameters(
                    "CREATE (:Memory {id: 'date-part-feb-2', space_id: 'date-part-space', lifecycle_state: 'archived', created_at: timestamp($created_at)})",
                    BTreeMap::from([(
                        "created_at".to_string(),
                        Value::String("2024-02-20T00:00:00".to_string()),
                    )]),
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "relationship count aggregate order read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e:Entity)-[r:RELATES_TO]-(:Entity) WHERE r.confidence >= $conf AND r.strength >= $strength RETURN e.id, COUNT(r) ORDER BY COUNT(r) DESC LIMIT $limit",
                        BTreeMap::from([
                            ("conf".to_string(), Value::Float(0.5)),
                            ("strength".to_string(), Value::Float(0.5)),
                            ("limit".to_string(), Value::Int(1)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("e.id", Value::String("degree-e1".to_string())),
                        ("count(r)", Value::Int(3)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'degree-e1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'degree-e2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'degree-e3'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'degree-e4'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'degree-e1'}), (b:Entity {id: 'degree-e2'}) CREATE (a)-[:RELATES_TO {confidence: 0.9, strength: 0.8}]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'degree-e1'}), (b:Entity {id: 'degree-e3'}) CREATE (a)-[:RELATES_TO {confidence: 0.8, strength: 0.7}]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'degree-e1'}), (b:Entity {id: 'degree-e4'}) CREATE (a)-[:RELATES_TO {confidence: 0.7, strength: 0.6}]->(b)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "memory mention entity count aggregate read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e:Entity {community_id: $community_id})<-[:MENTIONS]-(m:Memory) WHERE m.is_crystal = false WITH m, COUNT(e) AS entity_count RETURN m.id, m.title, m.content, m.unit_type, m.metadata, COALESCE(m.is_latest, true), entity_count ORDER BY entity_count DESC, m.importance DESC LIMIT $limit",
                        BTreeMap::from([
                            ("community_id".to_string(), Value::Int(77)),
                            ("limit".to_string(), Value::Int(2)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("m.id", Value::String("mention-count-m1".to_string())),
                            ("m.title", Value::String("Memory one".to_string())),
                            ("m.content", Value::String("first".to_string())),
                            ("m.unit_type", Value::String("fact".to_string())),
                            ("m.metadata", Value::String("{}".to_string())),
                            ("coalesce", Value::Bool(true)),
                            ("entity_count", Value::Int(2)),
                        ]),
                        compatibility_row([
                            ("m.id", Value::String("mention-count-m2".to_string())),
                            ("m.title", Value::String("Memory two".to_string())),
                            ("m.content", Value::String("second".to_string())),
                            ("m.unit_type", Value::String("fact".to_string())),
                            ("m.metadata", Value::String("{}".to_string())),
                            ("coalesce", Value::Bool(true)),
                            ("entity_count", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'mention-count-e1', community_id: 77})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'mention-count-e2', community_id: 77})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'mention-count-m1', title: 'Memory one', content: 'first', unit_type: 'fact', metadata: '{}', is_latest: true, is_crystal: false, importance: 0.6})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'mention-count-m2', title: 'Memory two', content: 'second', unit_type: 'fact', metadata: '{}', is_latest: true, is_crystal: false, importance: 0.9})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'mention-count-m1'}), (e:Entity {id: 'mention-count-e1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'mention-count-m1'}), (e:Entity {id: 'mention-count-e2'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'mention-count-m2'}), (e:Entity {id: 'mention-count-e1'}) CREATE (m)-[:MENTIONS]->(e)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "community memory coalesced summary read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e:Entity {community_id: $community_id})<-[:MENTIONS]-(m:Memory) WHERE COALESCE(m.is_crystal, false) = false WITH m, COUNT(e) AS entity_count RETURN m.id, COALESCE(m.title, ''), COALESCE(m.content, ''), entity_count ORDER BY entity_count DESC, COALESCE(m.importance, 0.5) DESC LIMIT 10",
                        BTreeMap::from([("community_id".to_string(), Value::Int(9800))]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            (
                                "m.id",
                                Value::String("coalesced-community-memory-2".to_string()),
                            ),
                            ("coalesce", Value::String(String::new())),
                            ("coalesce#2", Value::String("body two".to_string())),
                            ("entity_count", Value::Int(1)),
                        ]),
                        compatibility_row([
                            (
                                "m.id",
                                Value::String("coalesced-community-memory-1".to_string()),
                            ),
                            ("coalesce", Value::String("Title One".to_string())),
                            ("coalesce#2", Value::String(String::new())),
                            ("entity_count", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'coalesced-community-entity-1', community_id: 9800})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'coalesced-community-entity-2', community_id: 9800})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'coalesced-community-memory-1', title: 'Title One'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'coalesced-community-memory-2', content: 'body two', is_crystal: false, importance: 0.8})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'coalesced-community-memory-1'}), (e:Entity {id: 'coalesced-community-entity-1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'coalesced-community-memory-2'}), (e:Entity {id: 'coalesced-community-entity-2'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (n) WHERE n.id IN ['coalesced-community-memory-1', 'coalesced-community-memory-2', 'coalesced-community-entity-1', 'coalesced-community-entity-2'] DETACH DELETE n",
                    ),
                    ExpectedRows::RowCount(4),
                ),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "label distinct memory count aggregate read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WITH l, COUNT(DISTINCT m) AS memory_count RETURN l.name, memory_count ORDER BY memory_count DESC, l.name ASC SKIP $offset LIMIT $limit",
                        BTreeMap::from([
                            ("offset".to_string(), Value::Int(0)),
                            ("limit".to_string(), Value::Int(10)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("l.name", Value::String("alpha".to_string())),
                            ("memory_count", Value::Int(2)),
                        ]),
                        compatibility_row([
                            ("l.name", Value::String("Important".to_string())),
                            ("memory_count", Value::Int(1)),
                        ]),
                        compatibility_row([
                            ("l.name", Value::String("Merged".to_string())),
                            ("memory_count", Value::Int(1)),
                        ]),
                        compatibility_row([
                            ("l.name", Value::String("beta".to_string())),
                            ("memory_count", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'label-count-m1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'label-count-m2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'label-count-alpha', name: 'alpha'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Label {id: 'label-count-beta', name: 'beta'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'label-count-m1'}), (l:Label {id: 'label-count-alpha'}) CREATE (m)-[:HAS_LABEL {source: 'first'}]->(l)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'label-count-m1'}), (l:Label {id: 'label-count-alpha'}) CREATE (m)-[:HAS_LABEL {source: 'duplicate'}]->(l)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'label-count-m2'}), (l:Label {id: 'label-count-alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'label-count-m2'}), (l:Label {id: 'label-count-beta'}) CREATE (m)-[:HAS_LABEL]->(l)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "mention breadth aggregate pre-return order read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[men:MENTIONS]->(e:Entity) WHERE e.id IN $ids WITH m, COUNT(DISTINCT e) AS mention_breadth ORDER BY mention_breadth DESC, COALESCE(m.importance, 0.5) DESC LIMIT $top_n RETURN m.id, m.title, m.content, m.importance, m.is_crystal, mention_breadth",
                        BTreeMap::from([
                            (
                                "ids".to_string(),
                                Value::List(vec![
                                    Value::String("breadth-e1".to_string()),
                                    Value::String("breadth-e2".to_string()),
                                ]),
                            ),
                            ("top_n".to_string(), Value::Int(2)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("m.id", Value::String("breadth-m1".to_string())),
                            ("m.title", Value::String("Breadth one".to_string())),
                            ("m.content", Value::String("first breadth".to_string())),
                            ("m.importance", Value::Float(0.2)),
                            ("m.is_crystal", Value::Bool(false)),
                            ("mention_breadth", Value::Int(2)),
                        ]),
                        compatibility_row([
                            ("m.id", Value::String("breadth-m3".to_string())),
                            ("m.title", Value::String("Breadth three".to_string())),
                            ("m.content", Value::String("third breadth".to_string())),
                            ("m.importance", Value::Float(0.9)),
                            ("m.is_crystal", Value::Bool(false)),
                            ("mention_breadth", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'breadth-e1'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'breadth-e2'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'breadth-m1', title: 'Breadth one', content: 'first breadth', importance: 0.2, is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'breadth-m2', title: 'Breadth two', content: 'second breadth', importance: 0.4, is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'breadth-m3', title: 'Breadth three', content: 'third breadth', importance: 0.9, is_crystal: false})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'breadth-m1'}), (e:Entity {id: 'breadth-e1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'breadth-m1'}), (e:Entity {id: 'breadth-e1'}) CREATE (m)-[:MENTIONS {source: 'duplicate'}]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'breadth-m1'}), (e:Entity {id: 'breadth-e2'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'breadth-m2'}), (e:Entity {id: 'breadth-e1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'breadth-m3'}), (e:Entity {id: 'breadth-e2'}) CREATE (m)-[:MENTIONS]->(e)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "entity bridge span aggregate read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id IN $cids AND e2.community_id IN $cids AND e1.community_id <> e2.community_id WITH e1, COUNT(DISTINCT e2.community_id) AS community_span, COUNT(*) AS bridge_strength RETURN e1.id, e1.name, e1.community_id, community_span, bridge_strength ORDER BY community_span DESC, bridge_strength DESC LIMIT $limit",
                        BTreeMap::from([
                            (
                                "cids".to_string(),
                                Value::List(vec![
                                    Value::Int(9010),
                                    Value::Int(9020),
                                    Value::Int(9030),
                                ]),
                            ),
                            ("limit".to_string(), Value::Int(2)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("e1.id", Value::String("bridge-e1".to_string())),
                            ("e1.name", Value::String("Bridge One".to_string())),
                            ("e1.community_id", Value::Int(9010)),
                            ("community_span", Value::Int(2)),
                            ("bridge_strength", Value::Int(3)),
                        ]),
                        compatibility_row([
                            ("e1.id", Value::String("bridge-e2".to_string())),
                            ("e1.name", Value::String("Bridge Two".to_string())),
                            ("e1.community_id", Value::Int(9020)),
                            ("community_span", Value::Int(1)),
                            ("bridge_strength", Value::Int(2)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-e1', name: 'Bridge One', community_id: 9010})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-e2', name: 'Bridge Two', community_id: 9020})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-e3', name: 'Bridge Three', community_id: 9030})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-e4', name: 'Bridge Four', community_id: 9020})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-e5', name: 'Bridge Five', community_id: 9010})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-e1'}), (b:Entity {id: 'bridge-e2'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-e1'}), (b:Entity {id: 'bridge-e3'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-e1'}), (b:Entity {id: 'bridge-e4'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-e5'}), (b:Entity {id: 'bridge-e2'}) CREATE (a)-[:RELATES_TO]->(b)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "entity bridge span aggregate filter read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id IS NOT NULL AND e2.community_id IS NOT NULL AND e1.community_id <> e2.community_id WITH e1, COUNT(DISTINCT e2.community_id) AS community_span, COUNT(*) AS bridge_strength WHERE community_span >= 2 RETURN e1.id, e1.name, e1.community_id, community_span, bridge_strength ORDER BY community_span DESC, bridge_strength DESC LIMIT $limit",
                        BTreeMap::from([("limit".to_string(), Value::Int(1))]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        ("e1.id", Value::String("bridge-filter-e1".to_string())),
                        ("e1.name", Value::String("Bridge Filter One".to_string())),
                        ("e1.community_id", Value::Int(9110)),
                        ("community_span", Value::Int(4)),
                        ("bridge_strength", Value::Int(4)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-filter-e1', name: 'Bridge Filter One', community_id: 9110})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-filter-e2', name: 'Bridge Filter Two', community_id: 9120})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-filter-e3', name: 'Bridge Filter Three', community_id: 9130})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-filter-e4', name: 'Bridge Filter Four', community_id: 9140})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-filter-e5', name: 'Bridge Filter Five', community_id: 9150})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-filter-e1'}), (b:Entity {id: 'bridge-filter-e2'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-filter-e1'}), (b:Entity {id: 'bridge-filter-e3'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-filter-e1'}), (b:Entity {id: 'bridge-filter-e4'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-filter-e1'}), (b:Entity {id: 'bridge-filter-e5'}) CREATE (a)-[:RELATES_TO]->(b)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "community bridge lookup after aggregate read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id = $cid AND e2.community_id IS NOT NULL AND e2.community_id <> $cid WITH e2.community_id AS other_cid, COUNT(*) AS shared_edge_count ORDER BY shared_edge_count DESC LIMIT $limit MATCH (c:Community) WHERE c.community_id = other_cid RETURN c.community_id, c.name, c.ai_summary, c.description, c.member_count, shared_edge_count",
                        BTreeMap::from([
                            ("cid".to_string(), Value::Int(9210)),
                            ("limit".to_string(), Value::Int(2)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("c.community_id", Value::Int(9220)),
                            ("c.name", Value::String("Bridge Community Two".to_string())),
                            ("c.ai_summary", Value::String("two summary".to_string())),
                            ("c.description", Value::String("two description".to_string())),
                            ("c.member_count", Value::Int(20)),
                            ("shared_edge_count", Value::Int(2)),
                        ]),
                        compatibility_row([
                            ("c.community_id", Value::Int(9230)),
                            ("c.name", Value::String("Bridge Community Three".to_string())),
                            ("c.ai_summary", Value::String("three summary".to_string())),
                            ("c.description", Value::String("three description".to_string())),
                            ("c.member_count", Value::Int(10)),
                            ("shared_edge_count", Value::Int(1)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'bridge-community-2', community_id: 9220, name: 'Bridge Community Two', ai_summary: 'two summary', description: 'two description', member_count: 20})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'bridge-community-3', community_id: 9230, name: 'Bridge Community Three', ai_summary: 'three summary', description: 'three description', member_count: 10})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-lookup-e1', name: 'Bridge Lookup One', community_id: 9210})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-lookup-e2', name: 'Bridge Lookup Two', community_id: 9220})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-lookup-e3', name: 'Bridge Lookup Three', community_id: 9220})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'bridge-lookup-e4', name: 'Bridge Lookup Four', community_id: 9230})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-lookup-e1'}), (b:Entity {id: 'bridge-lookup-e2'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-lookup-e1'}), (b:Entity {id: 'bridge-lookup-e3'}) CREATE (a)-[:RELATES_TO]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'bridge-lookup-e1'}), (b:Entity {id: 'bridge-lookup-e4'}) CREATE (a)-[:RELATES_TO]->(b)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "community memory count optional lookup read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.space_id IN $space_ids AND e.community_id IS NOT NULL WITH e.community_id AS community_id, COUNT(DISTINCT m) AS memory_count OPTIONAL MATCH (c:Community) WHERE c.community_id = community_id RETURN c.name, memory_count, c.description ORDER BY memory_count DESC LIMIT $limit",
                        BTreeMap::from([
                            (
                                "space_ids".to_string(),
                                Value::List(vec![Value::String("bridge-space".to_string())]),
                            ),
                            ("limit".to_string(), Value::Int(2)),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("c.name", Value::String("Mention Community".to_string())),
                            ("memory_count", Value::Int(2)),
                            ("c.description", Value::String("mention description".to_string())),
                        ]),
                        compatibility_row([
                            ("c.name", Value::Null),
                            ("memory_count", Value::Int(1)),
                            ("c.description", Value::Null),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'mention-community', community_id: 9310, name: 'Mention Community', description: 'mention description'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'mention-community-e1', community_id: 9310})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'mention-community-e2', community_id: 9320})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'mention-community-m1', space_id: 'bridge-space'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'mention-community-m2', space_id: 'bridge-space'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 'mention-community-m3', space_id: 'bridge-space'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'mention-community-m1'}), (e:Entity {id: 'mention-community-e1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'mention-community-m2'}), (e:Entity {id: 'mention-community-e1'}) CREATE (m)-[:MENTIONS]->(e)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (m:Memory {id: 'mention-community-m3'}), (e:Entity {id: 'mention-community-e2'}) CREATE (m)-[:MENTIONS]->(e)",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "community summary presence ranked read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (c:Community) WHERE c.community_id IS NOT NULL AND c.community_id >= 0 RETURN c.community_id, c.name, c.description, c.ai_summary, c.member_count, c.updated_at ORDER BY CASE WHEN c.ai_summary IS NOT NULL AND c.ai_summary <> '' THEN 0 ELSE 1 END, c.member_count DESC LIMIT $limit",
                        BTreeMap::from([("limit".to_string(), Value::Int(2))]),
                    ),
                    ExpectedRows::Exact(vec![
                        compatibility_row([
                            ("c.community_id", Value::Int(9410)),
                            ("c.name", Value::String("Summary Ranked One".to_string())),
                            ("c.description", Value::String("ranked one".to_string())),
                            ("c.ai_summary", Value::String("summary one".to_string())),
                            ("c.member_count", Value::Int(200)),
                            ("c.updated_at", Value::Int(100)),
                        ]),
                        compatibility_row([
                            ("c.community_id", Value::Int(9420)),
                            ("c.name", Value::String("Summary Ranked Two".to_string())),
                            ("c.description", Value::String("ranked two".to_string())),
                            ("c.ai_summary", Value::String("summary two".to_string())),
                            ("c.member_count", Value::Int(150)),
                            ("c.updated_at", Value::Int(200)),
                        ]),
                    ]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'summary-ranked-one', community_id: 9410, name: 'Summary Ranked One', description: 'ranked one', ai_summary: 'summary one', member_count: 200, updated_at: 100})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'summary-ranked-two', community_id: 9420, name: 'Summary Ranked Two', description: 'ranked two', ai_summary: 'summary two', member_count: 150, updated_at: 200})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Community {id: 'summary-ranked-empty', community_id: 9430, name: 'Summary Ranked Empty', description: 'ranked empty', ai_summary: '', member_count: 1000, updated_at: 300})",
                )),
            ),
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "mcp graph all shortest path read",
                    CypherFixtureStatement::with_parameters(
                        "MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, properties(nodes(p), 'name') AS names, length(p) AS hops",
                        BTreeMap::from([
                            ("from_id".to_string(), Value::String("path-a".to_string())),
                            ("to_id".to_string(), Value::String("path-c".to_string())),
                        ]),
                    ),
                    ExpectedRows::Exact(vec![compatibility_row([
                        (
                            "node_ids",
                            Value::List(vec![
                                Value::String("path-a".to_string()),
                                Value::String("path-b".to_string()),
                                Value::String("path-c".to_string()),
                            ]),
                        ),
                        (
                            "names",
                            Value::List(vec![
                                Value::String("Alpha".to_string()),
                                Value::String("Beta".to_string()),
                                Value::String("Gamma".to_string()),
                            ]),
                        ),
                        ("hops", Value::Int(2)),
                    ])]),
                )
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'path-a', name: 'Alpha'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'path-b', name: 'Beta'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "CREATE (:Entity {id: 'path-c', name: 'Gamma'})",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (a:Entity {id: 'path-a'}), (b:Entity {id: 'path-b'}) CREATE (a)-[:MENTIONS]->(b)",
                ))
                .with_setup_query(CypherFixtureStatement::new(
                    "MATCH (b:Entity {id: 'path-b'}), (c:Entity {id: 'path-c'}) CREATE (b)-[:MENTIONS]->(c)",
                )),
            ),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "rest graph all shortest path read",
                CypherFixtureStatement::with_parameters(
                    "MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, properties(nodes(p), 'name') AS node_names, length(p) AS hops",
                    BTreeMap::from([
                        ("from_id".to_string(), Value::String("path-a".to_string())),
                        ("to_id".to_string(), Value::String("path-c".to_string())),
                    ]),
                ),
                ExpectedRows::Exact(vec![compatibility_row([
                    (
                        "node_ids",
                        Value::List(vec![
                            Value::String("path-a".to_string()),
                            Value::String("path-b".to_string()),
                            Value::String("path-c".to_string()),
                        ]),
                    ),
                    (
                        "node_names",
                        Value::List(vec![
                            Value::String("Alpha".to_string()),
                            Value::String("Beta".to_string()),
                            Value::String("Gamma".to_string()),
                        ]),
                    ),
                    ("hops", Value::Int(2)),
                ])]),
            )),
        ],
    }
}

pub fn nowledge_memory_core_inventory() -> CompatibilityQueryInventory {
    build_compatibility_query_inventory(
        "nowledge-memory-core-inventory",
        [
            CompatibilityQueryCallSite::new(
                "parameterized lookup uses index",
                "parameterized_read",
                "nowledge-memory-core::lookup",
            )
            .with_cypher("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"),
            CompatibilityQueryCallSite::new(
                "whole node projection",
                "record_projection_read",
                "nmem-graph::repo::get_by_id",
            )
            .with_cypher("MATCH (m:Memory {id: $memory_id}) RETURN m"),
            CompatibilityQueryCallSite::new(
                "source memory count increment",
                "source_provenance_write",
                "nmem-graph::source_write::create_sourced_from",
            )
            .with_cypher("MATCH (s:Source {id: $id}) SET s.memory_count = s.memory_count + 1"),
            CompatibilityQueryCallSite::new(
                "source provenance endpoint existence",
                "source_provenance_write",
                "nmem-graph::source_write::create_sourced_from",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)",
            ),
            CompatibilityQueryCallSite::new(
                "entity relationship endpoint existence",
                "entity_relationship_write",
                "nmem-graph::entity_write::create_entity_relationship",
            )
            .with_cypher(
                "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
            ),
            CompatibilityQueryCallSite::new(
                "source provenance relationship create",
                "source_provenance_write",
                "nmem-graph::source_write::create_sourced_from",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {chunk_index: $chunk_index}]->(s)",
            ),
            CompatibilityQueryCallSite::new(
                "source attribution read",
                "source_attribution_read",
                "nmem-server::context_wiring::source_attribution",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:SOURCED_FROM]->(s:Source) WHERE m.id IN $ids RETURN m.id, s.id",
            ),
            CompatibilityQueryCallSite::new(
                "source provenance memory detail read",
                "source_read",
                "nmem-server::source_repo::memories_for_source",
            )
            .with_cypher(
                "MATCH (m:Memory)-[r:SOURCED_FROM]->(s:Source {id: $source_id}) RETURN m.id as memory_id, m.title as title, m.content as content, r.chunk_index as chunk_index, r.chunk_range as chunk_range, m.unit_type as unit_type, m.confidence as confidence ORDER BY r.chunk_index ASC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "source memory id list read",
                "source_read",
                "nmem-server::source_repo::memory_ids_for_source",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:SOURCED_FROM]->(s:Source {id: $sid}) RETURN m.id LIMIT 24",
            ),
            CompatibilityQueryCallSite::new(
                "memory source provenance id read",
                "source_read",
                "nmem-server::memory_delete::source_ids_for_memory",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $id})-[:SOURCED_FROM]->(s:Source) RETURN s.id",
            ),
            CompatibilityQueryCallSite::new(
                "source memory-count decrement floor write",
                "source_write",
                "nmem-server::memory_delete::decrement_source_memory_count",
            )
            .with_cypher(
                "MATCH (s:Source {id: $id}) SET s.memory_count = CASE WHEN s.memory_count > 0 THEN s.memory_count - 1 ELSE 0 END",
            ),
            CompatibilityQueryCallSite::new(
                "source relationship source-reference count read",
                "relationship_read",
                "nmem-server::memory_delete::source_reference_relationship_count",
            )
            .with_cypher(
                "MATCH (e1:Entity)-[r:RELATES_TO]->(e2:Entity) WHERE r.source_reference = $id RETURN count(r)",
            ),
            CompatibilityQueryCallSite::new(
                "source relationship source-reference delete",
                "relationship_write",
                "nmem-server::memory_delete::delete_source_reference_relationships",
            )
            .with_cypher(
                "MATCH (e1:Entity)-[r:RELATES_TO]->(e2:Entity) WHERE r.source_reference = $id DELETE r",
            ),
            CompatibilityQueryCallSite::new(
                "thread compaction attribution read",
                "thread_compaction_read",
                "nmem-server::context_wiring::thread_compaction_attribution",
            )
            .with_cypher(
                "MATCH (t:Thread)-[:COMPACTS_TO]->(m:Memory) WHERE m.id IN $ids RETURN m.id, t.thread_id",
            ),
            CompatibilityQueryCallSite::new(
                "thread compacted-memory count read",
                "thread_compaction_read",
                "nmem-server::thread_repo::compacted_memory_count",
            )
            .with_cypher(
                "MATCH (t:Thread {id: $thread_uuid})-[:COMPACTS_TO]->(m:Memory) RETURN COUNT(m)",
            ),
            CompatibilityQueryCallSite::new(
                "thread compacted-memory summary read",
                "thread_compaction_read",
                "nmem-server::thread_repo::compacted_memory_summary",
            )
            .with_cypher(
                "MATCH (t:Thread {id: $uuid})-[:COMPACTS_TO]->(m:Memory) RETURN m.id, m.title, m.content LIMIT 200",
            ),
            CompatibilityQueryCallSite::new(
                "memory created-at bulk read",
                "memory_timestamp_read",
                "nmem-server::context_wiring::memory_created_at_bulk",
            )
            .with_cypher("MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id, m.created_at"),
            CompatibilityQueryCallSite::new(
                "memory compact detail fallback read",
                "memory_read",
                "nmem-server::memory_repo::compact_detail_fallback",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id}) RETURN m.id, COALESCE(m.title, ''), COALESCE(m.content, ''), COALESCE(m.unit_type, '')",
            ),
            CompatibilityQueryCallSite::new(
                "memory label name list read",
                "label_read",
                "nmem-server::memory_repo::label_names_for_memory",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id})-[:HAS_LABEL]->(l:Label) RETURN l.name LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "memory label bulk name read",
                "label_read",
                "nmem-server::memory_repo::bulk_label_names_for_memories",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids RETURN m.id, l.name",
            ),
            CompatibilityQueryCallSite::new(
                "memory label fallback bulk read",
                "label_read",
                "nmem-server::memory_repo::bulk_label_fallback_for_memories",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids RETURN m.id, COALESCE(l.name, l.id)",
            ),
            CompatibilityQueryCallSite::new(
                "memory label endpoint bulk read",
                "label_read",
                "nmem-graph::label_repo::bulk_label_endpoints_for_memories",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids RETURN m.id, l.id",
            ),
            CompatibilityQueryCallSite::new(
                "memory label distinct name count read",
                "label_read",
                "nmem-server::memory_repo::bulk_label_name_hits_for_memories",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE m.id IN $ids AND l.name IN $names RETURN m.id, COUNT(DISTINCT l.name)",
            ),
            CompatibilityQueryCallSite::new(
                "source label bulk name read",
                "source_label_read",
                "nmem-server::source_label_repo::bulk_label_names_for_sources",
            )
            .with_cypher(
                "MATCH (s:Source)-[:HAS_LABEL]->(l:Label) WHERE s.id IN $ids RETURN s.id, l.name",
            ),
            CompatibilityQueryCallSite::new(
                "source label endpoint bulk read",
                "source_label_read",
                "nmem-server::source_label_repo::bulk_label_endpoints_for_sources",
            )
            .with_cypher(
                "MATCH (s:Source)-[:HAS_LABEL]->(l:Label) WHERE s.id IN $ids RETURN s.id, l.id",
            ),
            CompatibilityQueryCallSite::new(
                "source label relationship count read",
                "source_label_read",
                "nmem-server::source_label_repo::source_label_relationship_count",
            )
            .with_cypher(
                "MATCH (s:Source {id: $source_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) RETURN COUNT(r)",
            ),
            CompatibilityQueryCallSite::new(
                "source label relationship merge",
                "source_label_write",
                "nmem-server::source_label_repo::assign_source_label",
            )
            .with_cypher(
                "MATCH (s:Source {id: $source_id}), (l:Label {id: $label_id}) MERGE (s)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.created_at = $created_at, r.properties = $properties",
            ),
            CompatibilityQueryCallSite::new(
                "source label relationship delete",
                "source_label_write",
                "nmem-server::source_label_repo::remove_source_label",
            )
            .with_cypher(
                "MATCH (s:Source {id: $source_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r",
            ),
            CompatibilityQueryCallSite::new(
                "source detail normalized space read",
                "source_read",
                "nmem-server::source_repo::detail_normalized_space",
            )
            .with_cypher(
                "MATCH (s:Source {id: $source_id}) RETURN s.id, s.original_name, CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END, s.lifecycle_state, s.parsed_path",
            ),
            CompatibilityQueryCallSite::new(
                "source detail chunk-count read",
                "source_read",
                "nmem-server::source_repo::detail_chunk_count",
            )
            .with_cypher(
                "MATCH (s:Source {id: $source_id}) RETURN s.original_name, CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END, s.chunk_count",
            ),
            CompatibilityQueryCallSite::new(
                "source detail file-path read",
                "source_read",
                "nmem-server::source_repo::detail_file_path",
            )
            .with_cypher(
                "MATCH (s:Source {id: $source_id}) RETURN s.original_name, CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END, s.file_path",
            ),
            CompatibilityQueryCallSite::new(
                "source default metadata fallback read",
                "source_read",
                "nmem-server::source_repo::default_metadata_for_source",
            )
            .with_cypher(
                "MATCH (s:Source {id: $id}) RETURN COALESCE(s.space_id, 'default'), COALESCE(s.source_type, 'file'), COALESCE(s.lifecycle_state, 'indexed'), COALESCE(s.mime_type, '')",
            ),
            CompatibilityQueryCallSite::new(
                "source list fallback page read",
                "source_read",
                "nmem-server::source_repo::list_sources_page",
            )
            .with_cypher(
                "MATCH (s:Source) RETURN s.id, COALESCE(s.original_name, ''), COALESCE(s.summary, ''), COALESCE(s.mime_type, ''), COALESCE(s.source_type, 'file'), COALESCE(s.source_url, ''), COALESCE(s.size_bytes, 0), COALESCE(s.version, 1), COALESCE(s.memory_count, 0), COALESCE(s.chunk_count, 0), COALESCE(s.lifecycle_state, 'indexed'), COALESCE(s.space_id, 'default'), s.created_at, s.updated_at ORDER BY s.id SKIP $offset LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "source overview memory-count ranking read",
                "source_read",
                "nmem-server::source_repo::overview_by_memory_count",
            )
            .with_cypher(
                "MATCH (s:Source) RETURN s.id, COALESCE(s.original_name, s.source_type, 'Source'), s.source_type, s.lifecycle_state, COALESCE(s.memory_count, 0) ORDER BY s.memory_count DESC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "source bulk summary fallback read",
                "source_read",
                "nmem-server::source_repo::bulk_source_summaries",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE s.id IN $ids RETURN s.id, COALESCE(s.original_name, s.file_path, s.source_type, 'Source'), s.source_type, s.summary, s.file_path, s.memory_count, s.chunk_count",
            ),
            CompatibilityQueryCallSite::new(
                "source count read",
                "source_read",
                "nmem-server::source_repo::count_sources",
            )
            .with_cypher("MATCH (s:Source) RETURN count(s)"),
            CompatibilityQueryCallSite::new(
                "source extracted id list read",
                "source_read",
                "nmem-server::source_repo::extracted_source_ids",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE s.id IN $ids AND s.lifecycle_state = 'extracted' RETURN s.id",
            ),
            CompatibilityQueryCallSite::new(
                "source extracted lifecycle mark indexed write",
                "source_write",
                "nmem-server::source_repo::mark_extracted_sources_indexed",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE s.id IN $ids AND s.lifecycle_state = 'extracted' SET s.lifecycle_state = 'indexed', s.updated_at = timestamp($updated_at)",
            ),
            CompatibilityQueryCallSite::new(
                "source lifecycle indexed chunk-count write",
                "source_write",
                "nmem-server::source_repo::mark_source_indexed",
            )
            .with_cypher(
                "MATCH (s:Source {id: $id}) SET s.lifecycle_state = 'indexed', s.chunk_count = $chunk_count, s.updated_at = timestamp($updated_at)",
            ),
            CompatibilityQueryCallSite::new(
                "source lifecycle state update write",
                "source_write",
                "nmem-server::source_repo::set_source_lifecycle_state",
            )
            .with_cypher(
                "MATCH (s:Source {id: $id}) SET s.lifecycle_state = $state, s.updated_at = timestamp($updated_at)",
            ),
            CompatibilityQueryCallSite::new(
                "source space update write",
                "source_write",
                "nmem-server::source_repo::set_source_space",
            )
            .with_cypher(
                "MATCH (s:Source {id: $id}) SET s.space_id = $target_space_id, s.updated_at = timestamp($updated_at)",
            ),
            CompatibilityQueryCallSite::new(
                "source bulk normalized-space move write",
                "source_write",
                "nmem-server::source_repo::move_sources_to_space",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE s.id IN $ids AND CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END = $source_space_id SET s.space_id = $target_space_id, s.updated_at = $updated_at",
            ),
            CompatibilityQueryCallSite::new(
                "source normalized-space id list read",
                "source_read",
                "nmem-server::source_repo::source_ids_in_space",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE CASE WHEN s.space_id IS NULL OR s.space_id = '' THEN 'default' ELSE s.space_id END = $space_id RETURN s.id",
            ),
            CompatibilityQueryCallSite::new(
                "memory normalized-space id list read",
                "memory_read",
                "nmem-server::memory_repo::memory_ids_in_space",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $space_id RETURN m.id",
            ),
            CompatibilityQueryCallSite::new(
                "memory bulk normalized-space move write",
                "memory_write",
                "nmem-server::memory_repo::move_memories_to_space",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.id IN $ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id SET m.space_id = $target_space_id, m.updated_at = $updated_at",
            ),
            CompatibilityQueryCallSite::new(
                "memory normalized-space limit-one read",
                "memory_read",
                "nmem-server::memory_repo::has_memory_in_space",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $space_id RETURN m.id LIMIT 1",
            ),
            CompatibilityQueryCallSite::new(
                "memory candidate normalized-space id read",
                "memory_read",
                "nmem-server::memory_repo::candidate_memory_ids_in_source_space",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.id IN $candidate_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id RETURN m.id",
            ),
            CompatibilityQueryCallSite::new(
                "memory candidate normalized-space exclusion read",
                "memory_read",
                "nmem-server::memory_repo::candidate_memory_ids_outside_target_space",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.id IN $candidate_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END <> $target_space_id RETURN m.id",
            ),
            CompatibilityQueryCallSite::new(
                "memory normalized-space count read",
                "memory_read",
                "nmem-server::memory_repo::count_memories_in_space",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id RETURN count(m)",
            ),
            CompatibilityQueryCallSite::new(
                "memory normalized-space limited id read",
                "memory_read",
                "nmem-server::memory_repo::limited_memory_ids_in_space",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id RETURN m.id LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "memory id-list normalized-space move returning ids",
                "memory_write",
                "nmem-server::memory_repo::move_memory_ids_to_space_returning",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.id IN $memory_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END = $source_space_id SET m.space_id = $target_space_id, m.updated_at = $updated_at RETURN m.id",
            ),
            CompatibilityQueryCallSite::new(
                "memory id-list normalized-space exclusion move returning ids",
                "memory_write",
                "nmem-server::memory_repo::move_memory_ids_outside_target_space_returning",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.id IN $memory_ids AND CASE WHEN m.space_id IS NULL OR m.space_id = '' THEN 'default' ELSE m.space_id END <> $target_space_id SET m.space_id = $target_space_id, m.updated_at = $updated_at RETURN m.id",
            ),
            CompatibilityQueryCallSite::new(
                "thread normalized-space id pair read",
                "thread_read",
                "nmem-server::thread_repo::thread_ids_in_space",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $space_id RETURN t.id, t.thread_id",
            ),
            CompatibilityQueryCallSite::new(
                "thread bulk node-id normalized-space move write",
                "thread_write",
                "nmem-server::thread_repo::move_thread_nodes_to_space",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE t.id IN $ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id SET t.space_id = $target_space_id, t.updated_at = $updated_at",
            ),
            CompatibilityQueryCallSite::new(
                "thread identity normalized-space id read",
                "thread_read",
                "nmem-server::thread_repo::thread_identity_ids_in_space",
            )
            .with_cypher(
                "MATCH (ti:ThreadIdentity) WHERE CASE WHEN ti.space_id IS NULL OR ti.space_id = '' THEN 'default' ELSE ti.space_id END = $space_id RETURN ti.thread_id",
            ),
            CompatibilityQueryCallSite::new(
                "thread identity bulk normalized-space move write",
                "thread_write",
                "nmem-server::thread_repo::move_thread_identities_to_space",
            )
            .with_cypher(
                "MATCH (ti:ThreadIdentity) WHERE ti.thread_id IN $ids AND CASE WHEN ti.space_id IS NULL OR ti.space_id = '' THEN 'default' ELSE ti.space_id END = $source_space_id SET ti.space_id = $target_space_id, ti.updated_at = $updated_at",
            ),
            CompatibilityQueryCallSite::new(
                "thread normalized-space logical id read",
                "thread_read",
                "nmem-server::thread_repo::logical_thread_ids_in_space",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $space_id RETURN t.thread_id",
            ),
            CompatibilityQueryCallSite::new(
                "memory entity name list read",
                "entity_read",
                "nmem-server::memory_repo::entity_names_for_memory",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id})-[:MENTIONS]->(e:Entity) RETURN e.name LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "memory entity endpoint bulk read",
                "entity_read",
                "nmem-server::memory_repo::bulk_entity_endpoints_for_memories",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.id IN $mids AND e.id IN $eids RETURN m.id, e.id",
            ),
            CompatibilityQueryCallSite::new(
                "source fan-in relationship count",
                "source_read",
                "nmem-server::rest_sources::move_source_to_space",
            )
            .with_cypher(
                "MATCH (:Memory)-[r:SOURCED_FROM]->(:Source {id: $id}) RETURN count(r)",
            ),
            CompatibilityQueryCallSite::new(
                "source detail memory-count read",
                "source_read",
                "nmem-server::source_repo::detail_with_memory_count",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE s.id = $id OPTIONAL MATCH (m:Memory)-[:SOURCED_FROM]->(s) RETURN s.id, s.title, s.source_type, COUNT(m)",
            ),
            CompatibilityQueryCallSite::new(
                "memory review-status bulk read",
                "memory_read",
                "nmem-server::review_repo::bulk_memory_review_state",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id, m.review_status, m.title, m.content",
            ),
            CompatibilityQueryCallSite::new(
                "memory ranked overview read",
                "memory_read",
                "nmem-server::memory_repo::ranked_overview",
            )
            .with_cypher(
                "MATCH (m:Memory) RETURN m.id, COALESCE(m.title, LEFT(m.content, 60)), m.title, LEFT(COALESCE(m.content, ''), 200), COALESCE(m.pagerank_score, m.importance, 0.5), m.community_id, m.space_id, m.created_at, m.updated_at, m.source, m.event_start, m.event_end, m.importance ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "source metadata timestamp update",
                "source_write",
                "nmem-server::rest_sources::mark_source_auto_ocr",
            )
            .with_cypher(
                "MATCH (s:Source {id: $id}) SET s.metadata = $metadata, s.updated_at = timestamp($updated_at)",
            ),
            CompatibilityQueryCallSite::new(
                "memory relation multi property update",
                "memory_relation_write",
                "nmem-server::mcp_server::update_memory_relation",
            )
            .with_cypher(
                "MATCH (source:Memory)-[r:MEMORY_RELATES_TO]->(target:Memory) WHERE r.id = $relation_id SET r.relation_type = $relation_type, r.strength = $strength, r.confidence = $confidence, r.status = $status, r.reason = $reason, r.updated_at = timestamp($updated_at)",
            ),
            CompatibilityQueryCallSite::new(
                "source revision history path read",
                "source_revision_read",
                "nmem-server::rest_sources::source_revision_history",
            )
            .with_cypher(
                "MATCH p = (s:Source {id: $source_id})-[:REVISED_AS*1..10]->(older:Source) RETURN older.id as id, older.original_name as name, older.version as version, older.sha256 as sha256, older.created_at as created_at ORDER BY older.version DESC",
            ),
            CompatibilityQueryCallSite::new(
                "evolves progression list read",
                "memory_evolution_read",
                "nmem-server::context_wiring::evolves_progression_list",
            )
            .with_cypher(
                "MATCH (older:Memory)-[e:EVOLVES]->(newer:Memory) WHERE (older.unit_type IN $types OR newer.unit_type IN $types) RETURN older.id, newer.id, e.content_relation, e.is_progression, older.unit_type, newer.unit_type, older.title, newer.title, older.created_at, newer.created_at, older.is_latest, newer.is_latest, older.space_id, newer.space_id ORDER BY newer.created_at DESC LIMIT 500",
            ),
            CompatibilityQueryCallSite::new(
                "mcp graph all shortest path read",
                "graph_path_read",
                "nmem-server::mcp_server::find_shortest_path",
            )
            .with_cypher(
                "MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, properties(nodes(p), 'name') AS names, length(p) AS hops",
            ),
            CompatibilityQueryCallSite::new(
                "rest graph all shortest path read",
                "graph_path_read",
                "nmem-server::rest_graph::find_shortest_path",
            )
            .with_cypher(
                "MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, properties(nodes(p), 'name') AS node_names, length(p) AS hops",
            ),
            CompatibilityQueryCallSite::new(
                "source detach delete cascade",
                "source_delete_write",
                "nmem-server::rest_sources::delete_source_node",
            )
            .with_cypher("MATCH (s:Source {id: $id}) DETACH DELETE s"),
            CompatibilityQueryCallSite::new(
                "thread optional message count",
                "thread_cleanup_read",
                "nmem-graph::repo::delete_thread_graph",
            )
            .with_cypher(
                "MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
            ),
            CompatibilityQueryCallSite::new(
                "thread message ordered read",
                "thread_read",
                "nmem-server::thread_repo::ordered_messages",
            )
            .with_cypher(
                "MATCH (t:Thread {id: $thread_uuid})-[c:CONTAINS]->(m:Message) RETURN m.id, m.content, m.role, COALESCE(c.order_index, m.order_index), m.timestamp ORDER BY COALESCE(c.order_index, m.order_index)",
            ),
            CompatibilityQueryCallSite::new(
                "thread message detach delete",
                "thread_cleanup_write",
                "nmem-graph::repo::delete_thread_graph",
            )
            .with_cypher(
                "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m",
            ),
            CompatibilityQueryCallSite::new(
                "legacy tail extracted refs count",
                "thread_cleanup_read",
                "nmem-graph::repo::count_legacy_tail_memory_refs",
            )
            .with_cypher(
                "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) WHERE m.order_index >= $start_index OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
            ),
            CompatibilityQueryCallSite::new(
                "top entities by degree",
                "graph_analysis_read",
                "nmem-server::rest_graph::graph_analysis",
            )
            .with_cypher(
                "MATCH (e:Entity) OPTIONAL MATCH (e)-[r]-() WITH e, COUNT(r) as degree RETURN e.id, e.name, degree ORDER BY degree DESC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "node detail neighbor counts",
                "graph_node_detail_read",
                "nmem-server::rest_graph::query_node_detail",
            )
            .with_cypher(
                "MATCH (n)-[r]-(neighbor) WHERE n.id = $node_id RETURN COUNT(DISTINCT neighbor), COUNT(r)",
            ),
            CompatibilityQueryCallSite::new(
                "undo community delete nodes",
                "community_undo_write",
                "nmem-server::rest_graph::run_undo_community_detection_inline",
            )
            .with_cypher("MATCH (c:Community) DETACH DELETE c"),
            CompatibilityQueryCallSite::new(
                "undo community clear node assignments",
                "community_undo_write",
                "nmem-server::rest_graph::run_undo_community_detection_inline",
            )
            .with_cypher("MATCH (n) WHERE n.community_id IS NOT NULL SET n.community_id = NULL"),
            CompatibilityQueryCallSite::new(
                "undo community reset graph meta",
                "community_undo_write",
                "nmem-server::rest_graph::run_undo_community_detection_inline",
            )
            .with_cypher(
                "MATCH (m:GraphMeta {meta_id: 'main'}) SET m.community_detection_applied = false, m.community_algorithm = '', m.community_resolution = 1.0, m.community_count = 0, m.community_detection_computed_at = NULL, m.updated_at = CURRENT_TIMESTAMP()",
            ),
            CompatibilityQueryCallSite::new(
                "crystallized provenance backfill merge",
                "schema_migration_write",
                "nmem-graph::schema::m_backfill_crystallized_from",
            )
            .with_cypher(
                "MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, n.occasion_key = '', n.created_at = r.created_at",
            ),
            CompatibilityQueryCallSite::new(
                "crystallized provenance backfill idempotent",
                "schema_migration_write",
                "nmem-graph::schema::m_backfill_crystallized_from",
            )
            .with_cypher(
                "MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, n.occasion_key = '', n.created_at = r.created_at",
            ),
            CompatibilityQueryCallSite::new(
                "crystallized provenance unmirrored verification",
                "schema_migration_verify",
                "nmem-server::bin::mig_verify",
            )
            .with_cypher(
                "MATCH (c:Memory)-[:CRYSTALLIZED_FROM]->(s:Memory) WHERE NOT EXISTS { MATCH (c)-[:SYNTHESIZED_FROM]->(s) } RETURN count(*)",
            ),
            CompatibilityQueryCallSite::new(
                "crystallized provenance distinct pair count",
                "schema_migration_verify",
                "nmem-server::bin::mig_verify",
            )
            .with_cypher(
                "MATCH (c:Memory)-[:CRYSTALLIZED_FROM]->(s:Memory) WITH DISTINCT c.id AS a, s.id AS b RETURN count(*)",
            ),
            CompatibilityQueryCallSite::new(
                "synthesized source ids collect read",
                "feed_read",
                "nmem-server::rest_feed.rs:306",
            )
            .with_cypher(
                "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids",
            ),
            CompatibilityQueryCallSite::new(
                "skill synthesized memory id read",
                "skill_read",
                "nmem-server::context_wiring::skill_evidence_ids",
            )
            .with_cypher(
                "MATCH (sk:Skill)-[:SYNTHESIZED_FROM]->(m:Memory) WHERE sk.stage IN $stages RETURN m.id",
            ),
            CompatibilityQueryCallSite::new(
                "skill stage list read",
                "skill_read",
                "nmem-server::context_wiring::skill_stage_list",
            )
            .with_cypher(
                "MATCH (sk:Skill) WHERE sk.stage IN $stages RETURN sk.id, sk.title, sk.name, sk.description, sk.stage, sk.evidence_count LIMIT 50",
            ),
            CompatibilityQueryCallSite::new(
                "skill detail read",
                "skill_read",
                "nmem-server::context_wiring::skill_detail",
            )
            .with_cypher(
                "MATCH (sk:Skill {id: $id}) RETURN sk.title, sk.name, sk.stage, sk.scope, sk.rationale, sk.evidence_count, sk.metadata",
            ),
            CompatibilityQueryCallSite::new(
                "skill synthesized memory detail read",
                "skill_read",
                "nmem-server::context_wiring::skill_evidence_detail",
            )
            .with_cypher(
                "MATCH (sk:Skill {id: $id})-[:SYNTHESIZED_FROM]->(m:Memory) RETURN m.id, m.title, m.content, m.unit_type ORDER BY m.created_at",
            ),
            CompatibilityQueryCallSite::new(
                "skill metadata read",
                "skill_read",
                "nmem-server::context_wiring::skill_metadata",
            )
            .with_cypher("MATCH (sk:Skill {id: $id}) RETURN sk.stage, sk.metadata"),
            CompatibilityQueryCallSite::new(
                "learning memory latest read",
                "memory_retrieval_read",
                "nmem-server::context_wiring::learning_memory_latest",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.unit_type = 'learning' AND m.is_crystal = false AND m.is_latest = true RETURN m.id, m.title, m.content ORDER BY m.created_at DESC LIMIT 40",
            ),
            CompatibilityQueryCallSite::new(
                "source parsed path list read",
                "source_read",
                "nmem-server::context_wiring::parsed_source_list",
            )
            .with_cypher(
                "MATCH (src:Source) WHERE src.parsed_path IS NOT NULL RETURN src.id, src.original_name, src.parsed_path ORDER BY src.created_at DESC LIMIT 12",
            ),
            CompatibilityQueryCallSite::new(
                "community summarized list read",
                "community_read",
                "nmem-server::context_wiring::community_summary_list",
            )
            .with_cypher(
                "MATCH (c:Community) WHERE c.ai_summary IS NOT NULL AND c.ai_summary <> '' RETURN c.community_id, c.name, c.ai_summary ORDER BY c.member_count DESC LIMIT 40",
            ),
            CompatibilityQueryCallSite::new(
                "community memory type-filter read",
                "community_read",
                "nmem-server::context_wiring::community_memory_type_filter",
            )
            .with_cypher(
                "MATCH (e:Entity {community_id: $cid})<-[:MENTIONS]-(m:Memory) WHERE m.is_crystal = false AND m.unit_type IN $types RETURN m.id, m.title LIMIT 200",
            ),
            CompatibilityQueryCallSite::new(
                "community entity memory-count read",
                "community_read",
                "nmem-server::context_wiring::community_entity_memory_count",
            )
            .with_cypher(
                "MATCH (e:Entity) WHERE e.community_id = $community_id OPTIONAL MATCH (m:Memory)-[:MENTIONS]->(e) RETURN e.id, e.name, e.entity_type, COUNT(m) AS memory_count ORDER BY memory_count DESC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "entity mention-count list read",
                "entity_read",
                "nmem-server::entity_repo::list_with_mention_counts",
            )
            .with_cypher(
                "MATCH (e:Entity) OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) RETURN e.id, e.name, e.entity_type, e.description, e.aliases, e.confidence, e.community_id, e.created_at, COUNT(r) AS mention_count ORDER BY mention_count DESC, e.name ASC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "synthesized source coverage lookup",
                "schema_migration_read",
                "nmem-graph::schema::find_existing_crystal_for_sources",
            )
            .with_cypher(
                "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.is_crystal = true AND s.id IN $source_ids WITH c.id AS cid, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid LIMIT 1",
            ),
            CompatibilityQueryCallSite::new(
                "synthesized source coverage title lookup",
                "memory_crystal_read",
                "nmem-server::mcp_server.rs:7363",
            )
            .with_cypher(
                "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.is_crystal = true AND s.id IN $source_ids WITH c.id AS cid, c.crystal_title AS ct, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid, ct LIMIT 1",
            ),
            CompatibilityQueryCallSite::new(
                "label usage count read",
                "label_list_read",
                "nmem-server::rest_lists::list_labels_handler",
            )
            .with_cypher(
                "MATCH (l:Label) OPTIONAL MATCH (l)<-[:HAS_LABEL]-(n) WITH l, COUNT(n) as usage_count RETURN l.id, l.name, l.color, l.description, l.created_at, l.updated_at, usage_count",
            ),
            CompatibilityQueryCallSite::new(
                "label usage direct optional count read",
                "label_list_read",
                "nmem-server::mcp_server.rs:1813",
            )
            .with_cypher(
                "MATCH (l:Label) OPTIONAL MATCH (m:Memory)-[:HAS_LABEL]->(l) RETURN l.id, l.name, COUNT(m) AS usage_count ORDER BY l.name ASC SKIP $offset LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "entity mention fan-in count",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::impact_counts",
            )
            .with_cypher("MATCH (:Memory)-[r:MENTIONS]->(e:Entity {id: $id}) RETURN COUNT(r)"),
            CompatibilityQueryCallSite::new(
                "entity lifecycle detail projection",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::delete_preview",
            )
            .with_cypher(
                "MATCH (e:Entity {id: $id}) RETURN e.id, e.name, e.entity_type, e.description, e.aliases",
            ),
            CompatibilityQueryCallSite::new(
                "entity outgoing relates count",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::impact_counts",
            )
            .with_cypher(
                "MATCH (e:Entity {id: $id})-[r:RELATES_TO]->(:Entity) RETURN COUNT(r)",
            ),
            CompatibilityQueryCallSite::new(
                "entity incoming relates count",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::impact_counts",
            )
            .with_cypher(
                "MATCH (:Entity)-[r:RELATES_TO]->(e:Entity {id: $id}) RETURN COUNT(r)",
            ),
            CompatibilityQueryCallSite::new(
                "entity outgoing relation preview",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::relation_preview_rows",
            )
            .with_cypher(
                "MATCH (e:Entity {id: $id})-[r:RELATES_TO]->(other:Entity) RETURN other.id, r",
            ),
            CompatibilityQueryCallSite::new(
                "entity incoming relation preview",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::relation_preview_rows",
            )
            .with_cypher(
                "MATCH (other:Entity)-[r:RELATES_TO]->(e:Entity {id: $id}) RETURN other.id, r",
            ),
            CompatibilityQueryCallSite::new(
                "entity has label count",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::impact_counts",
            )
            .with_cypher(
                "MATCH (e:Entity {id: $id})-[r:HAS_LABEL]->(:Label) RETURN COUNT(r)",
            ),
            CompatibilityQueryCallSite::new(
                "entity has label preview",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::label_preview_rows",
            )
            .with_cypher(
                "MATCH (e:Entity {id: $id})-[r:HAS_LABEL]->(n:Label) RETURN n.id, r",
            ),
            CompatibilityQueryCallSite::new(
                "entity belongs community count",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::impact_counts",
            )
            .with_cypher(
                "MATCH (e:Entity {id: $id})-[r:BELONGS_TO]->(:Community) RETURN COUNT(r)",
            ),
            CompatibilityQueryCallSite::new(
                "entity belongs community preview",
                "entity_lifecycle_read",
                "nmem-graph::entity_lifecycle::community_preview_rows",
            )
            .with_cypher(
                "MATCH (e:Entity {id: $id})-[r:BELONGS_TO]->(n:Community) RETURN n.id, r",
            ),
            CompatibilityQueryCallSite::new(
                "entity detach delete cascade",
                "entity_lifecycle_write",
                "nmem-graph::entity_lifecycle::delete_entity",
            )
            .with_cypher("MATCH (e:Entity {id: $id}) DETACH DELETE e"),
            CompatibilityQueryCallSite::new(
                "graph orphan entity read",
                "graph_orphan_read",
                "nmem-server::rest_graph::graph_orphans_handler",
            )
            .with_cypher(
                "MATCH (e:Entity) WHERE NOT (e)<-[:MENTIONS]-(:Memory) AND NOT (e)-[:RELATES_TO]-() AND NOT (e)-[:HAS_LABEL]-() RETURN e.id, e.name, e.entity_type",
            ),
            CompatibilityQueryCallSite::new(
                "graph orphan cleanup candidate read",
                "graph_orphan_cleanup_read",
                "nmem-server::rest_graph::cleanup_graph_orphans_handler",
            )
            .with_cypher(
                "MATCH (e:Entity) WHERE NOT (e)<-[:MENTIONS]-(:Memory) AND NOT (e)-[:RELATES_TO]-() AND NOT (e)-[:HAS_LABEL]-() RETURN e.id",
            ),
            CompatibilityQueryCallSite::new(
                "thread move source space selection",
                "thread_bulk_move_read",
                "nmem-server::rest_threads_write::select_thread_move_records",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id RETURN t.id, t.thread_id, t.space_id",
            ),
            CompatibilityQueryCallSite::new(
                "thread move target space exclusion",
                "thread_bulk_move_read",
                "nmem-server::rest_threads_write::select_bulk_thread_ids",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE t.thread_id IN $candidate_ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target_space_id RETURN t.thread_id",
            ),
            CompatibilityQueryCallSite::new(
                "thread move source space update returning ids",
                "thread_bulk_move_write_return",
                "nmem-server::rest_threads_write::move_threads_to_space",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id SET t.space_id = $target_space_id, t.updated_at = $updated_at RETURN t.thread_id",
            ),
            CompatibilityQueryCallSite::new(
                "thread distill optional source count all",
                "thread_distill_read",
                "nmem-server::rest_distill::list_threads_for_distill",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE t.thread_id IS NOT NULL AND (CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END) = $space_id AND ($source IS NULL OR t.source = $source) RETURN COUNT(t)",
            ),
            CompatibilityQueryCallSite::new(
                "thread distill optional source count filtered",
                "thread_distill_read",
                "nmem-server::rest_distill::list_threads_for_distill",
            )
            .with_cypher(
                "MATCH (t:Thread) WHERE t.thread_id IS NOT NULL AND (CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END) = $space_id AND ($source IS NULL OR t.source = $source) RETURN COUNT(t)",
            ),
            CompatibilityQueryCallSite::new(
                "memory access counter touch",
                "memory_access_write",
                "nmem-graph::repo::mark_memories_accessed",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.id = $id SET m.access_count = COALESCE(m.access_count, 0) + 1, m.last_accessed_at = $now",
            ),
            CompatibilityQueryCallSite::new(
                "null predicate",
                "predicate_read",
                "nowledge-memory-core::metadata-filter",
            )
            .with_cypher("MATCH (m:Memory) WHERE m.status IS NULL RETURN m.id AS id"),
            CompatibilityQueryCallSite::new(
                "entity alias list lookup",
                "entity_write_lookup",
                "nmem-graph::entity_write::find_entity_id",
            )
            .with_cypher(
                "MATCH (e:Entity) WHERE e.entity_type = $entity_type AND list_contains(e.aliases, $name)",
            ),
            CompatibilityQueryCallSite::new(
                "current timestamp write",
                "augmentation_write",
                "nmem-graph::augmentation::create_job",
            )
            .with_cypher(
                "CREATE (j:AugmentationJob {job_id: $job_id, status: $status, job_type: $job_type, progress: 0.0, created_at: CURRENT_TIMESTAMP()})",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation job create",
                "augmentation_job_write",
                "nmem-graph::augmentation::create_job",
            )
            .with_cypher(
                "CREATE (j:AugmentationJob {job_id: $job_id, job_type: $job_type, status: 'pending', progress: 0.0, message: 'Job created', parameters: $parameters, result: '{}', error_message: '', started_at: NULL, completed_at: NULL, created_at: CURRENT_TIMESTAMP()})",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation job mark running",
                "augmentation_job_write",
                "nmem-graph::augmentation::mark_running",
            )
            .with_cypher(
                "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.status = 'running', j.started_at = CURRENT_TIMESTAMP(), j.message = 'Job started'",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation job progress update",
                "augmentation_job_write",
                "nmem-graph::augmentation::update_progress",
            )
            .with_cypher(
                "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.progress = $progress, j.message = $message",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation job mark completed",
                "augmentation_job_write",
                "nmem-graph::augmentation::mark_completed",
            )
            .with_cypher(
                "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.status = 'completed', j.progress = 100.0, j.message = 'Job completed successfully', j.result = $result, j.completed_at = CURRENT_TIMESTAMP()",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation job mark failed",
                "augmentation_job_write",
                "nmem-graph::augmentation::mark_failed",
            )
            .with_cypher(
                "MATCH (j:AugmentationJob {job_id: $job_id}) SET j.status = 'failed', j.message = 'Job failed', j.error_message = $error_message, j.completed_at = CURRENT_TIMESTAMP()",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation job status read",
                "augmentation_job_read",
                "nmem-graph::augmentation::get_job",
            )
            .with_cypher(
                "MATCH (j:AugmentationJob {job_id: $job_id}) RETURN j.job_type, j.status, j.progress, j.message, j.result, j.error_message, j.started_at, j.completed_at",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation job filtered list read",
                "augmentation_job_read",
                "nmem-server::rest_graph::augmentation_jobs_handler",
            )
            .with_cypher(
                "MATCH (j:AugmentationJob) WHERE j.status = $status_filter RETURN j.job_id, j.job_type, j.status, j.progress, j.message, j.created_at ORDER BY j.created_at DESC LIMIT $job_limit",
            ),
            CompatibilityQueryCallSite::new(
                "augmentation stale interrupt write",
                "augmentation_job_write",
                "nmem-server::rest_graph::augmentation_jobs_handler",
            )
            .with_cypher(
                "MATCH (j:AugmentationJob) WHERE j.status = 'pending' OR j.status = 'running' SET j.status = 'failed', j.message = 'Interrupted before completion', j.error_message = 'Interrupted because the app restarted before completion.', j.completed_at = CURRENT_TIMESTAMP()",
            ),
            CompatibilityQueryCallSite::new(
                "merge node on create set",
                "schema_migration_write",
                "nmem-graph::schema::run_migrations",
            )
            .with_cypher(
                "MERGE (m:SchemaMigrationLog {id: $id}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP()",
            ),
            CompatibilityQueryCallSite::new(
                "graph meta merge post set",
                "pagerank_write",
                "nmem-graph::pagerank::persist_pagerank_scores",
            )
            .with_cypher(
                "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = true, m.pagerank_algorithm = 'pagerank', m.pagerank_damping = 0.85, m.pagerank_iterations = 20, m.pagerank_computed_at = CURRENT_TIMESTAMP(), m.updated_at = CURRENT_TIMESTAMP()",
            ),
            CompatibilityQueryCallSite::new(
                "label merge node on create seed",
                "label_write",
                "nmem-graph::label_write::upsert_label",
            )
            .with_cypher(
                "MERGE (l:Label {id: $label_id}) ON CREATE SET l.name = $label_name, l.canonical_name = $canonical, l.color = $color, l.description = $description, l.created_at = $now, l.updated_at = $now, l.metadata = $metadata",
            ),
            CompatibilityQueryCallSite::new(
                "label merge node on create and match set",
                "label_write",
                "nmem-graph::label_write::upsert_label",
            )
            .with_cypher(
                "MERGE (l:Label {id: $label_id}) ON CREATE SET l.name = $label_name, l.canonical_name = $canonical, l.color = $color, l.description = $description, l.created_at = $now, l.updated_at = $now, l.metadata = $metadata ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)",
            ),
            CompatibilityQueryCallSite::new(
                "matched relationship merge on create set",
                "label_write",
                "nmem-graph::repo::assign_label_to_memory",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.created_at = $created_at, r.properties = $properties",
            ),
            CompatibilityQueryCallSite::new(
                "label merge transfer retarget relationship",
                "label_write",
                "nmem-graph::label_write::merge_labels",
            )
            .with_cypher(
                "MATCH (n:Memory)-[:HAS_LABEL]->(src:Label {id: $src}) MATCH (tgt:Label {id: $tgt}) MERGE (n)-[r:HAS_LABEL]->(tgt) ON CREATE SET r.assigned_by = 'label_merge', r.created_at = $now, r.properties = '{}'",
            ),
            CompatibilityQueryCallSite::new(
                "label canonical lookup",
                "label_write",
                "nmem-graph::label_write::resolve_or_create_label",
            )
            .with_cypher("MATCH (l:Label) WHERE l.canonical_name = $c RETURN l.id LIMIT 1"),
            CompatibilityQueryCallSite::new(
                "label dynamic update set",
                "label_write",
                "nmem-graph::label_write::update_label",
            )
            .with_cypher(
                "MATCH (l:Label {id: $label_id}) SET l.updated_at = $updated_at, l.name = $name, l.canonical_name = $canonical",
            ),
            CompatibilityQueryCallSite::new(
                "label relationship remove from memory",
                "label_write",
                "nmem-graph::label_write::remove_label_from_memory",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r",
            ),
            CompatibilityQueryCallSite::new(
                "label detach delete",
                "label_write",
                "nmem-graph::label_write::delete_label",
            )
            .with_cypher("MATCH (l:Label {id: $label_id}) DETACH DELETE l"),
            CompatibilityQueryCallSite::new(
                "mention relationship target-filter multi property update",
                "entity_lifecycle_write",
                "nmem-graph::entity_lifecycle::update_entity_mention",
            )
            .with_cypher(
                "MATCH (m:Memory {id: $memory_id})-[r:MENTIONS]->(e:Entity {id: $target_id}) SET r.confidence = $confidence, r.mention_count = $mention_count, r.created_at = $created_at",
            ),
            CompatibilityQueryCallSite::new(
                "timestamp cutoff predicate",
                "graph_freshness_read",
                "nmem-graph::pagerank_plan::changed_counts",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.created_at > timestamp($cutoff) OR m.updated_at > timestamp($cutoff) RETURN COUNT(m)",
            ),
            CompatibilityQueryCallSite::new(
                "cast timestamp cleanup coverage predicate",
                "cleanup_coverage_read",
                "nmem-server::cleanup_plan::fetch_dedup_coverage",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE (m.is_crystal IS NULL OR m.is_crystal = false) AND (m.is_latest IS NULL OR m.is_latest = true) AND (m.lifecycle_state IS NULL OR m.lifecycle_state = 'active') AND m.created_at IS NOT NULL AND m.created_at >= CAST($recent_7d AS TIMESTAMP) RETURN count(m)",
            ),
            CompatibilityQueryCallSite::new(
                "memory monthly date part aggregate read",
                "memory_stats_read",
                "nmem-server::rest_read_batch::memory_monthly_stats",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.created_at IS NOT NULL AND ({filter}){space_and} WITH date_part('year', m.created_at) as year, date_part('month', m.created_at) as month, COUNT(m) as memory_count RETURN year, month, memory_count ORDER BY year DESC, month DESC LIMIT $months",
            ),
            CompatibilityQueryCallSite::new(
                "relationship count aggregate order read",
                "graph_analysis_read",
                "nmem-graph::community::topic_graph_candidates",
            )
            .with_cypher(
                "MATCH (e:Entity)-[r:RELATES_TO]-(:Entity) WHERE r.confidence >= {conf} AND r.strength >= {strength} RETURN e.id, COUNT(r) ORDER BY COUNT(r) DESC LIMIT {limit}",
            ),
            CompatibilityQueryCallSite::new(
                "memory mention entity count aggregate read",
                "community_memory_read",
                "nmem-server::mcp_server::community_memories",
            )
            .with_cypher(
                "MATCH (e:Entity {community_id: $louvain_id})<-[:MENTIONS]-(m:Memory) WHERE m.is_crystal = false WITH m, COUNT(e) AS entity_count RETURN m.id, m.title, m.content, m.unit_type, m.metadata, COALESCE(m.is_latest, true) ORDER BY entity_count DESC, m.importance DESC",
            ),
            CompatibilityQueryCallSite::new(
                "community memory coalesced summary read",
                "community_memory_read",
                "nmem-server::rest_community::community_memory_summaries",
            )
            .with_cypher(
                "MATCH (e:Entity {community_id: $community_id})<-[:MENTIONS]-(m:Memory) WHERE COALESCE(m.is_crystal, false) = false WITH m, COUNT(e) AS entity_count RETURN m.id, COALESCE(m.title, ''), COALESCE(m.content, ''), entity_count ORDER BY entity_count DESC, COALESCE(m.importance, 0.5) DESC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "label distinct memory count aggregate read",
                "label_stats_read",
                "nmem-server::rest_fs::label_distribution",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WITH l, COUNT(DISTINCT m) AS memory_count RETURN l.name, memory_count ORDER BY memory_count DESC, l.name ASC SKIP $offset LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "mention breadth aggregate pre-return order read",
                "graph_memory_read",
                "nmem-server::rest_graph::top_memories_by_mentions",
            )
            .with_cypher(
                "MATCH (m:Memory)-[men:MENTIONS]->(e:Entity) WHERE e.id IN $ids WITH m, COUNT(DISTINCT e) AS mention_breadth ORDER BY mention_breadth DESC, COALESCE(m.importance, 0.5) DESC LIMIT $top_n RETURN m.id, m.title, m.content, m.importance, m.is_crystal, mention_breadth",
            ),
            CompatibilityQueryCallSite::new(
                "entity bridge span aggregate read",
                "graph_bridge_read",
                "nmem-server::mcp_server::bridge_entities",
            )
            .with_cypher(
                "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id IN $cids AND e2.community_id IN $cids AND e1.community_id <> e2.community_id WITH e1, COUNT(DISTINCT e2.community_id) AS community_span, COUNT(*) AS bridge_strength RETURN e1.id, e1.name, e1.community_id, community_span, bridge_strength ORDER BY community_span DESC, bridge_strength DESC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "entity bridge span aggregate filter read",
                "graph_bridge_read",
                "nmem-server::mcp_server::bridge_entities_filtered",
            )
            .with_cypher(
                "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id IS NOT NULL AND e2.community_id IS NOT NULL AND e1.community_id <> e2.community_id WITH e1, COUNT(DISTINCT e2.community_id) AS community_span, COUNT(*) AS bridge_strength WHERE community_span >= 2 RETURN e1.id, e1.name, e1.community_id, community_span, bridge_strength ORDER BY community_span DESC, bridge_strength DESC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "community bridge lookup after aggregate read",
                "graph_bridge_read",
                "nmem-server::rest_graph::community_bridge_lookup",
            )
            .with_cypher(
                "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id = $cid AND e2.community_id IS NOT NULL AND e2.community_id <> $cid WITH e2.community_id AS other_cid, COUNT(*) AS shared_edge_count ORDER BY shared_edge_count DESC LIMIT $limit MATCH (c:Community) WHERE c.community_id = other_cid RETURN c.community_id, c.name, c.ai_summary, c.description, c.member_count, shared_edge_count",
            ),
            CompatibilityQueryCallSite::new(
                "community memory count optional lookup read",
                "community_memory_read",
                "nmem-server::rest_read_batch::community_memory_counts",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE {space_expr} IN $space_ids AND e.community_id IS NOT NULL WITH e.community_id AS community_id, COUNT(DISTINCT m) AS memory_count OPTIONAL MATCH (c:Community) WHERE c.community_id = community_id RETURN c.name, memory_count, c.description ORDER BY memory_count DESC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "community summary presence ranked read",
                "community_read",
                "nmem-server::rest_library::community_summary_ranked",
            )
            .with_cypher(
                "MATCH (c:Community) WHERE c.community_id IS NOT NULL AND c.community_id >= 0 RETURN c.community_id, c.name, c.description, c.ai_summary, c.member_count, c.updated_at ORDER BY CASE WHEN c.ai_summary IS NOT NULL AND c.ai_summary <> '' THEN 0 ELSE 1 END, c.member_count DESC LIMIT $limit",
            ),
            CompatibilityQueryCallSite::new(
                "analyzable corpus max updated fingerprint",
                "scheduler_fingerprint_read",
                "nmem-server::scheduler_service::analyzable_corpus_fingerprint",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE (m.is_crystal IS NULL OR m.is_crystal = false) AND (m.is_latest IS NULL OR m.is_latest = true) AND (m.lifecycle_state IS NULL OR m.lifecycle_state = 'active') RETURN count(m), max(m.updated_at)",
            ),
            CompatibilityQueryCallSite::new(
                "list predicate and pagination",
                "predicate_pagination_read",
                "nowledge-memory-core::candidate-window",
            )
            .with_cypher("MATCH (m:Memory) WHERE m.id IN $ids RETURN m.title AS title"),
            CompatibilityQueryCallSite::new(
                "grouped aggregation",
                "aggregation_read",
                "nowledge-memory-core::summary",
            )
            .with_cypher("MATCH (m:Memory) RETURN m.kind AS kind, count(*) AS total"),
            CompatibilityQueryCallSite::new(
                "health average read",
                "agent_context_read",
                "nmem-graph::agent_context::Q_HEALTH_AGG",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.is_crystal = false RETURN count(m), avg(m.decay_score_cached)",
            ),
            CompatibilityQueryCallSite::new(
                "health stale count read",
                "agent_context_read",
                "nmem-graph::agent_context::Q_HEALTH_STALE",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.is_crystal = false AND m.decay_score_cached < 0.5 RETURN count(m)",
            ),
            CompatibilityQueryCallSite::new(
                "activity digest recent memory read",
                "agent_context_read",
                "nmem-graph::agent_context::Q_DIGEST_MEMORIES",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.created_at >= $cutoff AND (m.is_crystal IS NULL OR m.is_crystal = false) RETURN m.id, m.title, m.unit_type, m.importance, m.created_at ORDER BY m.created_at DESC LIMIT 20",
            ),
            CompatibilityQueryCallSite::new(
                "activity digest recent evolves read",
                "agent_context_read",
                "nmem-graph::agent_context::Q_DIGEST_EVOLVES",
            )
            .with_cypher(
                "MATCH (newer:Memory)-[e:EVOLVES]->(older:Memory) WHERE e.created_at >= $cutoff RETURN newer.id, newer.title, older.id, older.title, e.content_relation, e.created_at ORDER BY e.created_at DESC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "activity digest recent crystal read",
                "agent_context_read",
                "nmem-graph::agent_context::Q_DIGEST_CRYSTALS",
            )
            .with_cypher(
                "MATCH (c:Memory) WHERE c.is_crystal = true AND c.created_at >= $cutoff RETURN c.id, c.title, c.created_at ORDER BY c.created_at DESC LIMIT 5",
            ),
            CompatibilityQueryCallSite::new(
                "activity digest recent source read",
                "agent_context_read",
                "nmem-graph::agent_context::Q_DIGEST_SOURCES",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE s.created_at >= $cutoff RETURN s.id, s.original_name, s.source_type, s.lifecycle_state, s.memory_count ORDER BY s.created_at DESC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "activity task crystal memory read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_CRYSTAL_MEMORIES",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.created_at >= $cutoff AND (m.is_crystal IS NULL OR m.is_crystal = false) RETURN m.id, m.title, m.unit_type, m.importance, m.created_at ORDER BY m.created_at DESC LIMIT 30",
            ),
            CompatibilityQueryCallSite::new(
                "activity task crystallized source id read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_CRYSTALLIZED_IDS",
            )
            .with_cypher(
                "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(s:Memory) RETURN DISTINCT s.id",
            ),
            CompatibilityQueryCallSite::new(
                "activity task challenge edge read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_CHALLENGE_EDGES",
            )
            .with_cypher(
                "MATCH (newer:Memory)-[e:EVOLVES {content_relation: 'challenges'}]->(older:Memory) RETURN newer.id, newer.title, older.id, older.title, e.created_at ORDER BY e.created_at DESC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "activity task decision memory read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_DECISION_MEMORIES",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE m.unit_type = 'decision' RETURN m.id, m.title, m.created_at, m.importance ORDER BY m.created_at DESC LIMIT 20",
            ),
            CompatibilityQueryCallSite::new(
                "activity task source attention read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_SOURCE_ATTENTION",
            )
            .with_cypher(
                "MATCH (s:Source) WHERE s.lifecycle_state = 'ingested' OR s.lifecycle_state = 'parsed' OR s.lifecycle_state = 'chunked' OR s.lifecycle_state = 'error' RETURN s.id, s.original_name, s.lifecycle_state, s.memory_count, s.created_at ORDER BY s.created_at ASC LIMIT 15",
            ),
            CompatibilityQueryCallSite::new(
                "activity task evolves cluster read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_EVOLVES_CLUSTERS",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:EVOLVES]-(other:Memory) WHERE (m.is_crystal IS NULL OR m.is_crystal = false) AND (other.is_crystal IS NULL OR other.is_crystal = false) RETURN m.id, m.title, m.unit_type, count(DISTINCT other) as neighbor_count ORDER BY neighbor_count DESC LIMIT 30",
            ),
            CompatibilityQueryCallSite::new(
                "activity task oldest crystal read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_OLDEST_CRYSTALS",
            )
            .with_cypher(
                "MATCH (c:Memory) WHERE c.is_crystal = true RETURN c.id, c.title, c.created_at, c.last_evaluated_at, c.review_status ORDER BY c.created_at ASC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "activity task stale crystal source read",
                "agent_context_read",
                "nmem-graph::agent_context_tasks::Q_STALE_CRYSTAL_SOURCES",
            )
            .with_cypher(
                "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(src:Memory) MATCH (src)-[:EVOLVES]-(newer:Memory) WHERE newer.created_at > c.created_at AND c.review_status <> 'dismissed' RETURN c.id, c.title, newer.id, newer.title, newer.created_at, c.review_status ORDER BY newer.created_at DESC LIMIT 10",
            ),
            CompatibilityQueryCallSite::new(
                "relationship min aggregation",
                "schema_verification_read",
                "nmem-graph::schema::backfill_synthesized_from",
            )
            .with_cypher(
                "MATCH (:Memory)-[r:SYNTHESIZED_FROM]->(:Memory) RETURN count(*), min(r.weight)",
            ),
            CompatibilityQueryCallSite::new(
                "where matched evolves relationship create",
                "memory_evolution_write",
                "nmem-graph::repo::add_evolves_edge",
            )
            .with_cypher(
                "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {content_relation: $content_relation, created_at: timestamp($now)}]->(b)",
            ),
            CompatibilityQueryCallSite::new(
                "distinct relationship aggregation",
                "aggregation_read",
                "nowledge-memory-core::distinct-summary",
            )
            .with_cypher(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN count(DISTINCT m) AS memories, count(DISTINCT e.id) AS entities",
            ),
            CompatibilityQueryCallSite::new(
                "node property pattern read",
                "predicate_read",
                "nowledge-memory-core::crystal-source-filter",
            )
            .with_cypher(
                "MATCH (m:Memory {kind: 'note'})-[:MENTIONS]->(e:Entity {name: 'Rust'}) RETURN DISTINCT m.id AS id",
            ),
            CompatibilityQueryCallSite::new(
                "undirected evolves read",
                "relationship_read",
                "nowledge-memory-core::evolves-neighbors",
            )
            .with_cypher(
                "MATCH (m:Memory {id: 3})-[:EVOLVES]-(other:Memory) RETURN DISTINCT other.id AS id",
            ),
            CompatibilityQueryCallSite::new(
                "incoming mentions read",
                "relationship_read",
                "nowledge-memory-core::incoming-mentions",
            )
            .with_cypher(
                "MATCH (e:Entity {community_id: $community_id})<-[:MENTIONS]-(m:Memory) RETURN DISTINCT m.id AS id",
            ),
            CompatibilityQueryCallSite::new(
                "untyped relationship label read",
                "relationship_read",
                "nowledge-memory-core::overview-edges",
            )
            .with_cypher("MATCH (a)-[r]->(b) RETURN a.id, b.id, label(r)"),
            CompatibilityQueryCallSite::new(
                "multi label overview read",
                "relationship_read",
                "nowledge-memory-core::graph-overview-neighbors",
            )
            .with_cypher(
                "MATCH (start)-[r]-(neighbor:Entity:Memory) WHERE start.id = $node_id RETURN neighbor.id AS id",
            ),
            CompatibilityQueryCallSite::new(
                "projection fallback read",
                "projection_read",
                "nowledge-memory-core::fallback-labels",
            )
            .with_cypher(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN COALESCE(m.title, LEFT(COALESCE(m.content, ''), 60)), COALESCE(r.strength, r.confidence, 0.5)",
            ),
            CompatibilityQueryCallSite::new(
                "projection fallback ordering",
                "projection_ordering",
                "nowledge-memory-core::rank-fallback-order",
            )
            .with_cypher(
                "MATCH (m:Memory) RETURN m.id AS id ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC",
            ),
            CompatibilityQueryCallSite::new(
                "predicate fallback read",
                "predicate_read",
                "nowledge-memory-core::crystal-filter",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE COALESCE(m.is_crystal, false) = false RETURN m.id AS id",
            ),
            CompatibilityQueryCallSite::new(
                "case insensitive fallback grep",
                "predicate_read",
                "nowledge-memory-core::kfs-grep",
            )
            .with_cypher(
                "MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) CONTAINS LOWER($needle) OR LOWER(COALESCE(m.title, '')) CONTAINS LOWER($needle) RETURN m.id AS id",
            ),
            CompatibilityQueryCallSite::new(
                "unlabeled node mutation",
                "node_mutation",
                "nowledge-memory-core::community-assignment",
            )
            .with_cypher("MATCH (n) WHERE n.id = $node_id SET n.community_id = $community_id"),
            CompatibilityQueryCallSite::new(
                "anonymous relationship count",
                "relationship_count",
                "nowledge-memory-core::graph-stats",
            )
            .with_cypher("MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total"),
            CompatibilityQueryCallSite::new(
                "relationship property read",
                "relationship_read",
                "nowledge-memory-core::relationship-properties",
            )
            .with_cypher(
                "MATCH (m:Memory)-[r:MENTIONS {weight: 4}]->(e:Entity) RETURN e.name AS entity, r.weight AS weight",
            ),
            CompatibilityQueryCallSite::new(
                "relationship property mutation",
                "relationship_mutation",
                "nowledge-memory-core::relationship-property-set",
            )
            .with_cypher(
                "MATCH (m:Memory)-[r:MENTIONS {weight: 3}]->(e:Entity) WHERE m.id = 1 AND r.weight = 3 SET r.weight = 7",
            ),
            CompatibilityQueryCallSite::new(
                "cypher projected graph definition",
                "projected_graph_definition",
                "nowledge-memory-core::projection",
            )
            .with_cypher(
                "CALL project_graph('MemoryMentions', ['Memory', 'Entity'], ['MENTIONS'])",
            ),
            CompatibilityQueryCallSite::new(
                "page rank procedure",
                "graph_algorithm",
                "nowledge-memory-core::pagerank",
            )
            .with_cypher(
                "CALL page_rank('MemoryMentions', dampingFactor := 0.85, maxIterations := 20)",
            ),
            CompatibilityQueryCallSite::new(
                "hierarchical louvain procedure",
                "graph_algorithm",
                "nowledge-memory-core::louvain",
            )
            .with_cypher("CALL louvain('MemoryMentions', maxLevels := 2)"),
            CompatibilityQueryCallSite::new(
                "mentions projection",
                "projected_graph_shadow",
                "nowledge-memory-core::projected-graph-shadow",
            ),
        ],
    )
    .expect("built-in Nowledge core compatibility inventory is valid")
}

fn compatibility_row(items: impl IntoIterator<Item = (&'static str, Value)>) -> Row {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

impl ExpectedErrorClass {
    fn from_error(error: &SkeinError) -> Self {
        match error {
            SkeinError::Parse(_) => Self::Parse,
            SkeinError::Semantic(_) => Self::Semantic,
            SkeinError::Storage(_) => Self::Storage,
            SkeinError::Execution(_) => Self::Execution,
        }
    }
}

pub fn run_compatibility_fixture(
    db: &mut Database,
    fixture: &CompatibilityFixture,
) -> Result<CompatibilityReport> {
    run_primary_setup(db, fixture)?;

    Ok(CompatibilityReport {
        fixture: fixture.name.clone(),
        checks: run_primary_checks(db, fixture)?
            .into_iter()
            .map(|check| check.report)
            .collect(),
    })
}

pub fn run_compatibility_fixture_with_shadow(
    db: &mut Database,
    fixture: &CompatibilityFixture,
    shadow: &mut impl CompatibilityShadowEngine,
) -> Result<CompatibilityShadowReport> {
    run_primary_setup(db, fixture)?;
    run_shadow_setup(fixture, shadow)?;

    let mut primary_checks = Vec::new();
    let mut shadow_checks = Vec::new();
    for check in &fixture.checks {
        match check {
            CompatibilityCheck::Cypher(check) => {
                let primary = run_cypher_check(db, fixture, check)?;
                primary_checks.push(CompatibilityCheckReport {
                    name: check.name.clone(),
                });
                run_shadow_cypher_check(fixture, check, shadow, &primary)?;
                shadow_checks.push(CompatibilityShadowCheckReport {
                    name: check.name.clone(),
                    status: CompatibilityShadowStatus::Matched,
                });
            }
            CompatibilityCheck::ProjectedGraph(check) => {
                let primary = run_projected_graph_check(db, fixture, check)?;
                primary_checks.push(CompatibilityCheckReport {
                    name: check.name.clone(),
                });
                let status = match shadow.project_graph(check)? {
                    Some(shadow_output) => {
                        compare_projected_graph_shadow(
                            fixture,
                            check,
                            shadow.name(),
                            &primary,
                            &shadow_output,
                        )?;
                        CompatibilityShadowStatus::Matched
                    }
                    None => CompatibilityShadowStatus::PrimaryOnly,
                };
                shadow_checks.push(CompatibilityShadowCheckReport {
                    name: check.name.clone(),
                    status,
                });
            }
        }
    }

    Ok(CompatibilityShadowReport {
        fixture: fixture.name.clone(),
        shadow_engine: shadow.name().to_string(),
        primary_checks,
        shadow_checks,
    })
}

pub fn assess_compatibility_cutover(
    report: &CompatibilityShadowReport,
    policy: CompatibilityCutoverPolicy,
) -> CompatibilityCutoverReport {
    let total_checks = report.shadow_checks.len();
    let matched_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::Matched)
        .count();
    let primary_only_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
        .map(|check| check.name.clone())
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();

    if total_checks == 0 {
        blockers.push("no compatibility checks were executed".to_string());
    }
    if report.primary_checks.len() != total_checks {
        blockers.push(format!(
            "primary check count {} does not match shadow check count {}",
            report.primary_checks.len(),
            total_checks
        ));
    }
    if matched_checks < policy.min_matched_checks {
        blockers.push(format!(
            "matched check count {} is below required minimum {}",
            matched_checks, policy.min_matched_checks
        ));
    }
    if policy.require_shadow_for_all_checks && !primary_only_checks.is_empty() {
        blockers.push(format!(
            "shadow engine '{}' did not cover checks: {}",
            report.shadow_engine,
            primary_only_checks.join(", ")
        ));
    }

    CompatibilityCutoverReport {
        fixture: report.fixture.clone(),
        shadow_engine: report.shadow_engine.clone(),
        decision: if blockers.is_empty() {
            CompatibilityCutoverDecision::Ready
        } else {
            CompatibilityCutoverDecision::Blocked
        },
        total_checks,
        matched_checks,
        primary_only_checks,
        blockers,
    }
}

pub fn assess_query_inventory_coverage(
    fixture: &CompatibilityFixture,
    inventory: &CompatibilityQueryInventory,
) -> CompatibilityInventoryCoverageReport {
    let fixture_checks = fixture
        .checks
        .iter()
        .map(compatibility_check_name)
        .collect::<BTreeSet<_>>();
    let required_checks = inventory
        .required_checks
        .iter()
        .map(|check| check.name.as_str())
        .collect::<BTreeSet<_>>();
    let missing_checks = required_checks
        .difference(&fixture_checks)
        .map(|check| (*check).to_string())
        .collect::<Vec<_>>();
    let extra_fixture_checks = fixture_checks
        .difference(&required_checks)
        .map(|check| (*check).to_string())
        .collect::<Vec<_>>();

    CompatibilityInventoryCoverageReport {
        inventory: inventory.name.clone(),
        fixture: fixture.name.clone(),
        required_checks: required_checks.len(),
        covered_checks: required_checks.len() - missing_checks.len(),
        missing_checks,
        extra_fixture_checks,
    }
}

pub fn assess_query_inventory_gate(
    coverage: &CompatibilityInventoryCoverageReport,
    policy: CompatibilityInventoryCoveragePolicy,
) -> CompatibilityInventoryGateReport {
    let mut blockers = Vec::new();
    if coverage.required_checks == 0 {
        blockers.push("query inventory has no required checks".to_string());
    }
    if policy.require_all_required_checks && !coverage.missing_checks.is_empty() {
        blockers.push(format!(
            "fixture '{}' is missing required query checks: {}",
            coverage.fixture,
            coverage.missing_checks.join(", ")
        ));
    }
    if !policy.allow_extra_fixture_checks && !coverage.extra_fixture_checks.is_empty() {
        blockers.push(format!(
            "fixture '{}' contains checks not declared by inventory '{}': {}",
            coverage.fixture,
            coverage.inventory,
            coverage.extra_fixture_checks.join(", ")
        ));
    }

    CompatibilityInventoryGateReport {
        inventory: coverage.inventory.clone(),
        fixture: coverage.fixture.clone(),
        decision: if blockers.is_empty() {
            CompatibilityCutoverDecision::Ready
        } else {
            CompatibilityCutoverDecision::Blocked
        },
        required_checks: coverage.required_checks,
        covered_checks: coverage.covered_checks,
        missing_checks: coverage.missing_checks.clone(),
        extra_fixture_checks: coverage.extra_fixture_checks.clone(),
        blockers,
    }
}

pub fn assess_compatibility_migration_gate(
    inventory: &CompatibilityInventoryGateReport,
    shadow: &CompatibilityCutoverReport,
) -> CompatibilityMigrationGateReport {
    let mut blockers = Vec::new();
    if inventory.fixture != shadow.fixture {
        blockers.push(format!(
            "inventory fixture '{}' does not match shadow fixture '{}'",
            inventory.fixture, shadow.fixture
        ));
    }
    blockers.extend(
        inventory
            .blockers
            .iter()
            .map(|blocker| format!("inventory: {blocker}")),
    );
    blockers.extend(
        shadow
            .blockers
            .iter()
            .map(|blocker| format!("shadow: {blocker}")),
    );

    CompatibilityMigrationGateReport {
        fixture: inventory.fixture.clone(),
        inventory: inventory.inventory.clone(),
        shadow_engine: shadow.shadow_engine.clone(),
        decision: if blockers.is_empty() {
            CompatibilityCutoverDecision::Ready
        } else {
            CompatibilityCutoverDecision::Blocked
        },
        inventory_decision: inventory.decision,
        shadow_decision: shadow.decision,
        blockers,
    }
}

pub fn assess_compatibility_migration_gate_bundle(
    fixture: &CompatibilityFixture,
    inventory: &CompatibilityQueryInventory,
    shadow: &CompatibilityShadowReport,
    inventory_policy: CompatibilityInventoryCoveragePolicy,
    cutover_policy: CompatibilityCutoverPolicy,
) -> CompatibilityMigrationGateBundle {
    let coverage = assess_query_inventory_coverage(fixture, inventory);
    let inventory_gate = assess_query_inventory_gate(&coverage, inventory_policy);
    let cutover = assess_compatibility_cutover(shadow, cutover_policy);
    let migration_gate = assess_compatibility_migration_gate(&inventory_gate, &cutover);
    CompatibilityMigrationGateBundle {
        coverage,
        inventory_gate,
        cutover,
        migration_gate,
    }
}

fn compatibility_check_name(check: &CompatibilityCheck) -> &str {
    match check {
        CompatibilityCheck::Cypher(check) => &check.name,
        CompatibilityCheck::ProjectedGraph(check) => &check.name,
    }
}

fn run_primary_setup(db: &mut Database, fixture: &CompatibilityFixture) -> Result<()> {
    for statement in &fixture.setup {
        db.query_with_params(&statement.cypher, &statement.parameters)
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' setup failed for '{}': {error}",
                    fixture.name, statement.cypher
                ))
            })?;
    }
    Ok(())
}

fn run_shadow_setup(
    fixture: &CompatibilityFixture,
    shadow: &mut impl CompatibilityShadowEngine,
) -> Result<()> {
    for statement in &fixture.setup {
        shadow.execute(statement).map_err(|error| {
            SkeinError::Execution(format!(
                "fixture '{}' shadow engine '{}' setup failed for '{}': {error}",
                fixture.name,
                shadow.name(),
                statement.cypher
            ))
        })?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrimaryCheckOutput {
    report: CompatibilityCheckReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CypherCheckOutcome {
    Rows {
        output: QueryOutput,
        effect: Option<QueryOutput>,
    },
    Error(ExpectedErrorClass),
}

fn run_primary_checks(
    db: &mut Database,
    fixture: &CompatibilityFixture,
) -> Result<Vec<PrimaryCheckOutput>> {
    let mut reports = Vec::new();
    for check in &fixture.checks {
        match check {
            CompatibilityCheck::Cypher(check) => {
                run_cypher_check(db, fixture, check)?;
                reports.push(PrimaryCheckOutput {
                    report: CompatibilityCheckReport {
                        name: check.name.clone(),
                    },
                });
            }
            CompatibilityCheck::ProjectedGraph(check) => {
                run_projected_graph_check(db, fixture, check)?;
                reports.push(PrimaryCheckOutput {
                    report: CompatibilityCheckReport {
                        name: check.name.clone(),
                    },
                });
            }
        }
    }
    Ok(reports)
}

fn run_shadow_cypher_check(
    fixture: &CompatibilityFixture,
    check: &CypherFixtureCheck,
    shadow: &mut impl CompatibilityShadowEngine,
    primary: &CypherCheckOutcome,
) -> Result<()> {
    for setup_query in &check.setup_queries {
        shadow.execute(setup_query).map_err(|error| {
            SkeinError::Execution(format!(
                "fixture '{}' check '{}' shadow engine '{}' setup failed for '{}': {error}",
                fixture.name,
                check.name,
                shadow.name(),
                setup_query.cypher
            ))
        })?;
    }
    let shadow_output = shadow.execute(&check.statement);
    match (primary, check.expected_error) {
        (CypherCheckOutcome::Error(expected), Some(_)) => {
            let Err(error) = shadow_output else {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' shadow engine '{}' expected {:?} error for '{}', got success",
                    fixture.name,
                    check.name,
                    shadow.name(),
                    expected,
                    check.statement.cypher
                )));
            };
            let actual = ExpectedErrorClass::from_error(&error);
            if actual != *expected {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' shadow engine '{}' expected {:?} error for '{}', got {:?}: {error}",
                    fixture.name,
                    check.name,
                    shadow.name(),
                    expected,
                    check.statement.cypher,
                    actual
                )));
            }
        }
        (
            CypherCheckOutcome::Rows {
                output: primary_output,
                effect: primary_effect,
            },
            None,
        ) => {
            let shadow_output = shadow_output.map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' check '{}' shadow engine '{}' failed for '{}': {error}",
                    fixture.name,
                    check.name,
                    shadow.name(),
                    check.statement.cypher
                ))
            })?;
            check
                .expected_rows
                .assert_matches(
                    &fixture.name,
                    &format!("{} shadow {}", check.name, shadow.name()),
                    &check.statement.cypher,
                    &shadow_output,
                    check.tolerance,
                )
                .map_err(|error| {
                    SkeinError::Execution(format!(
                        "fixture '{}' check '{}' shadow engine '{}' row validation failed: {error}",
                        fixture.name,
                        check.name,
                        shadow.name()
                    ))
                })?;
            check.expected_rows.assert_shadow_matches_primary(
                &fixture.name,
                &check.name,
                shadow.name(),
                primary_output,
                &shadow_output,
                check.tolerance,
            )?;
            if let Some(effect_query) = &check.effect_query {
                let Some(primary_effect) = primary_effect else {
                    return Err(SkeinError::Execution(format!(
                        "fixture '{}' check '{}' missing primary effect output",
                        fixture.name, check.name
                    )));
                };
                let shadow_effect = shadow.execute(effect_query).map_err(|error| {
                    SkeinError::Execution(format!(
                        "fixture '{}' check '{}' shadow engine '{}' failed effect query '{}': {error}",
                        fixture.name,
                        check.name,
                        shadow.name(),
                        effect_query.cypher
                    ))
                })?;
                let expected = check.effect_expected_rows.as_ref().ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "fixture '{}' check '{}' effect query is missing expected rows",
                        fixture.name, check.name
                    ))
                })?;
                expected.assert_matches(
                    &fixture.name,
                    &format!("{} effect shadow {}", check.name, shadow.name()),
                    &effect_query.cypher,
                    &shadow_effect,
                    check.tolerance,
                )?;
                expected.assert_shadow_matches_primary(
                    &fixture.name,
                    &format!("{} effect", check.name),
                    shadow.name(),
                    primary_effect,
                    &shadow_effect,
                    check.tolerance,
                )?;
            }
        }
        _ => {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' has inconsistent primary outcome and expectation",
                fixture.name, check.name
            )));
        }
    }
    Ok(())
}

fn run_cypher_check(
    db: &mut Database,
    fixture: &CompatibilityFixture,
    check: &CypherFixtureCheck,
) -> Result<CypherCheckOutcome> {
    for setup_query in &check.setup_queries {
        db.query_with_params(&setup_query.cypher, &setup_query.parameters)
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' check '{}' setup failed for '{}': {error}",
                    fixture.name, check.name, setup_query.cypher
                ))
            })?;
    }
    let output = db.query_with_params(&check.statement.cypher, &check.statement.parameters);
    if let Some(expected_error) = check.expected_error {
        let error = match output {
            Ok(output) => {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected {:?} error for '{}', got rows {:?}",
                    fixture.name, check.name, expected_error, check.statement.cypher, output.rows
                )));
            }
            Err(error) => error,
        };
        let actual = ExpectedErrorClass::from_error(&error);
        if actual != expected_error {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' expected {:?} error for '{}', got {:?}: {error}",
                fixture.name, check.name, expected_error, check.statement.cypher, actual
            )));
        }
        return Ok(CypherCheckOutcome::Error(expected_error));
    }

    let output = output?;
    check.expected_rows.assert_matches(
        &fixture.name,
        &check.name,
        &check.statement.cypher,
        &output,
        check.tolerance,
    )?;

    if !check.expected_plan_contains.is_empty() {
        let explain =
            db.explain_query_with_params(&check.statement.cypher, &check.statement.parameters)?;
        let plan = explain.physical_plan.explain(0);
        for needle in &check.expected_plan_contains {
            if !plan.contains(needle) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected plan to contain '{}', got:\n{}",
                    fixture.name, check.name, needle, plan
                )));
            }
        }
    }

    let effect = if let Some(effect_query) = &check.effect_query {
        let effect = db.query_with_params(&effect_query.cypher, &effect_query.parameters)?;
        let expected = check.effect_expected_rows.as_ref().ok_or_else(|| {
            SkeinError::Execution(format!(
                "fixture '{}' check '{}' effect query is missing expected rows",
                fixture.name, check.name
            ))
        })?;
        expected.assert_matches(
            &fixture.name,
            &format!("{} effect", check.name),
            &effect_query.cypher,
            &effect,
            check.tolerance,
        )?;
        Some(effect)
    } else {
        None
    };

    Ok(CypherCheckOutcome::Rows { output, effect })
}

fn run_projected_graph_check(
    db: &Database,
    fixture: &CompatibilityFixture,
    check: &ProjectedGraphFixtureCheck,
) -> Result<ProjectedGraphShadowOutput> {
    let graph = db.project_graph(check.rel_type.as_deref());
    let output = projected_graph_shadow_output(&graph, check);
    assert_projected_graph_matches_fixture(fixture, check, &output)?;
    Ok(output)
}

fn projected_graph_shadow_output(
    graph: &ProjectedGraph,
    check: &ProjectedGraphFixtureCheck,
) -> ProjectedGraphShadowOutput {
    let page_rank_scores = graph
        .page_rank(Default::default())
        .into_iter()
        .map(|score| (score.node.0, score.score))
        .collect::<Vec<_>>();
    ProjectedGraphShadowOutput {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        incoming: check
            .expected_incoming
            .iter()
            .map(|(node, _)| {
                let sources = graph
                    .incoming_sources(crate::store::NodeId(*node))
                    .map(|sources| sources.map(|source| source.0).collect::<Vec<_>>())
                    .unwrap_or_default();
                (*node, sources)
            })
            .collect(),
        communities: if check.expected_communities.is_empty() {
            Vec::new()
        } else {
            graph
                .louvain_communities(Default::default())
                .into_iter()
                .map(|assignment| (assignment.node.0, assignment.community.0))
                .collect()
        },
        hierarchical_communities: if check.expected_hierarchical_communities.is_empty() {
            Vec::new()
        } else {
            graph
                .hierarchical_louvain_communities(Default::default())
                .into_iter()
                .map(|assignment| (assignment.level, assignment.node.0, assignment.community.0))
                .collect()
        },
        page_rank_top_node: page_rank_scores.first().map(|(node, _)| *node),
        page_rank_scores,
    }
}

fn assert_projected_graph_matches_fixture(
    fixture: &CompatibilityFixture,
    check: &ProjectedGraphFixtureCheck,
    output: &ProjectedGraphShadowOutput,
) -> Result<()> {
    if output.node_count != check.expected_node_count {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' expected {} projected nodes, got {}",
            fixture.name, check.name, check.expected_node_count, output.node_count
        )));
    }
    if output.edge_count != check.expected_edge_count {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' expected {} projected edges, got {}",
            fixture.name, check.name, check.expected_edge_count, output.edge_count
        )));
    }
    for (node, expected_sources) in &check.expected_incoming {
        let actual = output
            .incoming
            .iter()
            .find(|(candidate, _)| candidate == node)
            .map(|(_, sources)| sources.clone())
            .unwrap_or_default();
        if actual != *expected_sources {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' expected incoming sources {:?} for node {}, got {:?}",
                fixture.name, check.name, expected_sources, node, actual
            )));
        }
    }

    if let Some(expected_node) = check.page_rank_top_node {
        if output.page_rank_top_node != Some(expected_node) {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' expected PageRank top node {}, got {:?}",
                fixture.name, check.name, expected_node, output.page_rank_top_node
            )));
        }
    }
    if !check.expected_communities.is_empty() {
        let communities = output
            .communities
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>();
        for (node, expected_community) in &check.expected_communities {
            let actual = communities.get(node).copied();
            if actual != Some(*expected_community) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected community {} for node {}, got {:?}",
                    fixture.name, check.name, expected_community, node, actual
                )));
            }
        }
    }
    if !check.expected_hierarchical_communities.is_empty() {
        let communities = output
            .hierarchical_communities
            .iter()
            .copied()
            .map(|(level, node, community)| ((level, node), community))
            .collect::<BTreeMap<_, _>>();
        for (level, node, expected_community) in &check.expected_hierarchical_communities {
            let actual = communities.get(&(*level, *node)).copied();
            if actual != Some(*expected_community) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected level {} community {} for node {}, got {:?}",
                    fixture.name, check.name, level, expected_community, node, actual
                )));
            }
        }
    }
    if !check.expected_page_rank_scores.is_empty() {
        let scores = output
            .page_rank_scores
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>();
        for (node, expected_score) in &check.expected_page_rank_scores {
            let actual = scores.get(node).copied();
            let Some(actual_score) = actual else {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected PageRank score for node {}, got none",
                    fixture.name, check.name, node
                )));
            };
            if !float_matches(actual_score, *expected_score, check.tolerance.float_abs) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected PageRank score {} for node {}, got {}",
                    fixture.name, check.name, expected_score, node, actual_score
                )));
            }
        }
    }

    Ok(())
}

fn compare_projected_graph_shadow(
    fixture: &CompatibilityFixture,
    check: &ProjectedGraphFixtureCheck,
    shadow_engine: &str,
    primary: &ProjectedGraphShadowOutput,
    shadow: &ProjectedGraphShadowOutput,
) -> Result<()> {
    assert_projected_graph_matches_fixture(fixture, check, shadow).map_err(|error| {
        SkeinError::Execution(format!(
            "fixture '{}' check '{}' shadow engine '{}' projected graph validation failed: {error}",
            fixture.name, check.name, shadow_engine
        ))
    })?;
    if !projected_graph_outputs_match(primary, shadow, check.tolerance) {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' shadow engine '{}' projected graph mismatch: primary {:?}, shadow {:?}",
            fixture.name, check.name, shadow_engine, primary, shadow
        )));
    }
    Ok(())
}

impl ExpectedRows {
    fn assert_matches(
        &self,
        fixture_name: &str,
        check_name: &str,
        cypher: &str,
        output: &QueryOutput,
        tolerance: CompatibilityTolerance,
    ) -> Result<()> {
        match self {
            ExpectedRows::Exact(expected) => {
                if !rows_match_ordered(expected, &output.rows, tolerance) {
                    return Err(row_mismatch_error(
                        fixture_name,
                        check_name,
                        cypher,
                        expected,
                        &output.rows,
                    ));
                }
            }
            ExpectedRows::Unordered(expected) => {
                let mut actual = output.rows.clone();
                let expected_matches = rows_match_unordered(expected, &actual, tolerance);
                if !expected_matches {
                    let mut expected = expected.clone();
                    expected.sort();
                    actual.sort();
                    return Err(row_mismatch_error(
                        fixture_name,
                        check_name,
                        cypher,
                        &expected,
                        &actual,
                    ));
                }
            }
            ExpectedRows::RowCount(expected) => {
                if output.rows.len() != *expected {
                    return Err(SkeinError::Execution(format!(
                        "fixture '{}' check '{}' expected {} rows for '{}', got {}",
                        fixture_name,
                        check_name,
                        expected,
                        cypher,
                        output.rows.len()
                    )));
                }
            }
        }
        Ok(())
    }

    fn assert_shadow_matches_primary(
        &self,
        fixture_name: &str,
        check_name: &str,
        shadow_engine: &str,
        primary: &QueryOutput,
        shadow: &QueryOutput,
        tolerance: CompatibilityTolerance,
    ) -> Result<()> {
        match self {
            ExpectedRows::Exact(_) => {
                if !rows_match_ordered(&primary.rows, &shadow.rows, tolerance) {
                    return Err(shadow_mismatch_error(
                        fixture_name,
                        check_name,
                        shadow_engine,
                        &primary.rows,
                        &shadow.rows,
                    ));
                }
            }
            ExpectedRows::Unordered(_) => {
                let mut primary_rows = primary.rows.clone();
                let mut shadow_rows = shadow.rows.clone();
                if !rows_match_unordered(&primary_rows, &shadow_rows, tolerance) {
                    primary_rows.sort();
                    shadow_rows.sort();
                    return Err(shadow_mismatch_error(
                        fixture_name,
                        check_name,
                        shadow_engine,
                        &primary_rows,
                        &shadow_rows,
                    ));
                }
            }
            ExpectedRows::RowCount(_) => {
                if primary.rows.len() != shadow.rows.len() {
                    return Err(SkeinError::Execution(format!(
                        "fixture '{}' check '{}' shadow engine '{}' row count mismatch: primary {}, shadow {}",
                        fixture_name,
                        check_name,
                        shadow_engine,
                        primary.rows.len(),
                        shadow.rows.len()
                    )));
                }
            }
        }
        Ok(())
    }
}

fn row_mismatch_error(
    fixture_name: &str,
    check_name: &str,
    cypher: &str,
    expected: &[Row],
    actual: &[Row],
) -> SkeinError {
    SkeinError::Execution(format!(
        "fixture '{}' check '{}' row mismatch for '{}': expected {:?}, got {:?}",
        fixture_name, check_name, cypher, expected, actual
    ))
}

fn rows_match_ordered(expected: &[Row], actual: &[Row], tolerance: CompatibilityTolerance) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| rows_match(expected, actual, tolerance))
}

fn rows_match_unordered(
    expected: &[Row],
    actual: &[Row],
    tolerance: CompatibilityTolerance,
) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut used = vec![false; actual.len()];
    for expected_row in expected {
        let Some(index) = actual.iter().enumerate().position(|(index, actual_row)| {
            !used[index] && rows_match(expected_row, actual_row, tolerance)
        }) else {
            return false;
        };
        used[index] = true;
    }
    true
}

fn rows_match(expected: &Row, actual: &Row, tolerance: CompatibilityTolerance) -> bool {
    expected.len() == actual.len()
        && expected.iter().all(|(key, expected_value)| {
            actual
                .get(key)
                .is_some_and(|actual_value| values_match(expected_value, actual_value, tolerance))
        })
}

fn values_match(expected: &Value, actual: &Value, tolerance: CompatibilityTolerance) -> bool {
    match (expected, actual) {
        (Value::Float(expected), Value::Float(actual)) => {
            float_matches(*expected, *actual, tolerance.float_abs)
        }
        (Value::List(expected), Value::List(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| values_match(expected, actual, tolerance))
        }
        (Value::Map(expected), Value::Map(actual)) => {
            expected.len() == actual.len()
                && expected.iter().all(|(key, expected)| {
                    actual
                        .get(key)
                        .is_some_and(|actual| values_match(expected, actual, tolerance))
                })
        }
        _ => expected == actual,
    }
}

fn float_matches(left: f64, right: f64, tolerance: f64) -> bool {
    if left == right {
        return true;
    }
    left.is_finite() && right.is_finite() && (left - right).abs() <= tolerance
}

fn projected_graph_outputs_match(
    primary: &ProjectedGraphShadowOutput,
    shadow: &ProjectedGraphShadowOutput,
    tolerance: CompatibilityTolerance,
) -> bool {
    primary.node_count == shadow.node_count
        && primary.edge_count == shadow.edge_count
        && primary.incoming == shadow.incoming
        && primary.communities == shadow.communities
        && primary.hierarchical_communities == shadow.hierarchical_communities
        && primary.page_rank_top_node == shadow.page_rank_top_node
        && page_rank_scores_match(
            &primary.page_rank_scores,
            &shadow.page_rank_scores,
            tolerance,
        )
}

fn page_rank_scores_match(
    primary: &[(u64, f64)],
    shadow: &[(u64, f64)],
    tolerance: CompatibilityTolerance,
) -> bool {
    primary.len() == shadow.len()
        && primary.iter().zip(shadow).all(
            |((primary_node, primary_score), (shadow_node, shadow_score))| {
                primary_node == shadow_node
                    && float_matches(*primary_score, *shadow_score, tolerance.float_abs)
            },
        )
}

fn shadow_mismatch_error(
    fixture_name: &str,
    check_name: &str,
    shadow_engine: &str,
    primary: &[Row],
    shadow: &[Row],
) -> SkeinError {
    SkeinError::Execution(format!(
        "fixture '{}' check '{}' shadow engine '{}' row mismatch: primary {:?}, shadow {:?}",
        fixture_name, check_name, shadow_engine, primary, shadow
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        assess_compatibility_cutover, assess_compatibility_migration_gate,
        assess_compatibility_migration_gate_bundle, assess_query_inventory_coverage,
        assess_query_inventory_gate, build_compatibility_query_inventory,
        nowledge_memory_core_fixture, nowledge_memory_core_inventory, run_compatibility_fixture,
        run_compatibility_fixture_with_shadow, CompatibilityCheck, CompatibilityCheckReport,
        CompatibilityCutoverDecision, CompatibilityCutoverPolicy, CompatibilityCutoverReport,
        CompatibilityFixture, CompatibilityInventoryCoveragePolicy, CompatibilityQueryCallSite,
        CompatibilityQueryInventory, CompatibilityQueryInventoryItem,
        CompatibilityShadowCheckReport, CompatibilityShadowEngine, CompatibilityShadowReport,
        CompatibilityShadowStatus, CompatibilityTolerance, CypherFixtureCheck,
        CypherFixtureStatement, ExpectedRows, ExternalShadowCommand, ProjectedGraphFixtureCheck,
        ProjectedGraphShadowOutput,
    };
    use crate::{Database, QueryOutput, Result, Value};
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn runs_nowledge_shaped_fixture() {
        let mut db = Database::new();
        let fixture = nowledge_memory_core_fixture();

        let report = run_compatibility_fixture(&mut db, &fixture).unwrap();

        assert_eq!(report.fixture, "nowledge-memory-core");
        assert_eq!(report.checks.len(), 190);
    }

    #[test]
    fn query_inventory_reports_fixture_coverage() {
        let fixture = nowledge_memory_core_fixture();
        let inventory = nowledge_memory_core_inventory();

        let coverage = assess_query_inventory_coverage(&fixture, &inventory);
        let gate =
            assess_query_inventory_gate(&coverage, CompatibilityInventoryCoveragePolicy::default());
        let coverage_json = super::compatibility_inventory_coverage_report_to_json(&coverage);
        let gate_json = super::compatibility_inventory_gate_report_to_json(&gate);

        assert_eq!(coverage.inventory, "nowledge-memory-core-inventory");
        assert_eq!(coverage.fixture, "nowledge-memory-core");
        assert_eq!(coverage.required_checks, 190);
        assert_eq!(coverage.covered_checks, 190);
        assert!(coverage.missing_checks.is_empty());
        assert!(coverage.extra_fixture_checks.is_empty());
        assert_eq!(gate.decision, CompatibilityCutoverDecision::Ready);
        assert!(gate.blockers.is_empty());
        assert_eq!(coverage_json["covered_checks"], 190);
        assert_eq!(gate_json["decision"], "ready");
        assert_eq!(gate_json["blockers"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn public_nowledge_core_fixture_and_inventory_are_gate_ready() {
        let fixture = nowledge_memory_core_fixture();
        let inventory = nowledge_memory_core_inventory();
        let mut primary = Database::new();
        let mut shadow = DatabaseShadowEngine::default();

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();
        let bundle = assess_compatibility_migration_gate_bundle(
            &fixture,
            &inventory,
            &report,
            CompatibilityInventoryCoveragePolicy::default(),
            CompatibilityCutoverPolicy::default(),
        );
        let bundle_json = super::compatibility_migration_gate_bundle_to_json(&bundle);

        assert_eq!(fixture.name, "nowledge-memory-core");
        assert_eq!(inventory.required_checks.len(), fixture.checks.len());
        assert_eq!(
            bundle.migration_gate.decision,
            CompatibilityCutoverDecision::Ready
        );
        assert!(bundle.migration_gate.blockers.is_empty());
        assert_eq!(bundle_json["coverage"]["covered_checks"], 190);
        assert_eq!(bundle_json["inventory_gate"]["decision"], "ready");
        assert_eq!(bundle_json["cutover"]["decision"], "ready");
        assert_eq!(bundle_json["cutover"]["matched_checks"], 190);
        assert_eq!(bundle_json["migration_gate"]["decision"], "ready");
        assert_eq!(bundle_json["migration_gate"]["inventory_decision"], "ready");
        assert_eq!(bundle_json["migration_gate"]["shadow_decision"], "ready");
    }

    #[test]
    fn query_inventory_builder_preserves_call_site_metadata() {
        let inventory = build_compatibility_query_inventory(
            "production-nowledge-inventory",
            [
                CompatibilityQueryCallSite::new(
                    "memory lookup",
                    "parameterized_read",
                    "memory_store.rs:42",
                )
                .with_cypher("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"),
                CompatibilityQueryCallSite::new(
                    "graph context",
                    "bounded_path_read",
                    "retrieval.rs:88",
                ),
            ],
        )
        .unwrap();

        assert_eq!(inventory.name, "production-nowledge-inventory");
        assert_eq!(inventory.required_checks.len(), 2);
        assert_eq!(inventory.required_checks[0].name, "memory lookup");
        assert_eq!(
            inventory.required_checks[0].source.as_deref(),
            Some("memory_store.rs:42")
        );
        assert_eq!(
            inventory.required_checks[0].cypher.as_deref(),
            Some("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title")
        );
        assert_eq!(inventory.required_checks[1].cypher, None);
    }

    #[test]
    fn query_inventory_builder_rejects_duplicate_check_names() {
        let error = build_compatibility_query_inventory(
            "production-nowledge-inventory",
            [
                CompatibilityQueryCallSite::new("memory lookup", "read", "first.rs:1"),
                CompatibilityQueryCallSite::new("memory lookup", "read", "second.rs:2"),
            ],
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("duplicate compatibility query call site"));
    }

    #[test]
    fn query_inventory_json_imports_scanner_call_site_artifacts() {
        let artifact = serde_json::json!({
            "name": "production-nowledge-inventory",
            "call_sites": [
                {
                    "name": "memory lookup",
                    "query_family": "parameterized_read",
                    "source": "memory_store.rs:42",
                    "cypher": "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                },
                {
                    "name": "graph context",
                    "query_family": "bounded_path_read",
                    "source": "retrieval.rs:88"
                }
            ]
        });

        let inventory = super::build_compatibility_query_inventory_from_json(&artifact).unwrap();
        let exported = super::compatibility_query_inventory_to_json(&inventory);
        let reimported = super::build_compatibility_query_inventory_from_json(&exported).unwrap();

        assert_eq!(inventory.name, "production-nowledge-inventory");
        assert_eq!(inventory.required_checks.len(), 2);
        assert_eq!(
            inventory.required_checks[0].source.as_deref(),
            Some("memory_store.rs:42")
        );
        assert_eq!(
            inventory.required_checks[0].cypher.as_deref(),
            Some("MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title")
        );
        assert_eq!(exported["required_checks"].as_array().unwrap().len(), 2);
        assert_eq!(reimported, inventory);
    }

    #[test]
    fn query_inventory_json_rejects_duplicate_required_checks() {
        let artifact = r#"{
            "name": "production-nowledge-inventory",
            "required_checks": [
                {
                    "name": "memory lookup",
                    "query_family": "parameterized_read",
                    "source": "first.rs:1"
                },
                {
                    "name": "memory lookup",
                    "query_family": "parameterized_read",
                    "source": "second.rs:2"
                }
            ]
        }"#;

        let error = super::build_compatibility_query_inventory_from_json_str(artifact).unwrap_err();

        assert!(error
            .to_string()
            .contains("duplicate compatibility query inventory item"));
    }

    #[test]
    fn query_inventory_json_rejects_malformed_artifacts() {
        let artifact = serde_json::json!({
            "name": "production-nowledge-inventory",
            "call_sites": [
                {
                    "name": "memory lookup",
                    "query_family": "parameterized_read",
                    "source": 42
                }
            ]
        });

        let error = super::build_compatibility_query_inventory_from_json(&artifact).unwrap_err();

        assert!(error
            .to_string()
            .contains("field 'source' must be a string"));
    }

    #[test]
    fn query_inventory_reports_missing_and_extra_checks() {
        let fixture = CompatibilityFixture {
            name: "partial".to_string(),
            setup: Vec::new(),
            checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "extra check",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.id AS id"),
                ExpectedRows::RowCount(0),
            ))],
        };
        let inventory = CompatibilityQueryInventory {
            name: "required".to_string(),
            required_checks: vec![CompatibilityQueryInventoryItem::new(
                "required check",
                "read",
            )],
        };

        let coverage = assess_query_inventory_coverage(&fixture, &inventory);
        let gate = assess_query_inventory_gate(
            &coverage,
            CompatibilityInventoryCoveragePolicy {
                require_all_required_checks: true,
                allow_extra_fixture_checks: false,
            },
        );

        assert_eq!(coverage.required_checks, 1);
        assert_eq!(coverage.covered_checks, 0);
        assert_eq!(coverage.missing_checks, vec!["required check".to_string()]);
        assert_eq!(
            coverage.extra_fixture_checks,
            vec!["extra check".to_string()]
        );
        assert_eq!(gate.decision, CompatibilityCutoverDecision::Blocked);
        assert_eq!(gate.blockers.len(), 2);
        assert!(gate.blockers[0].contains("missing required query checks"));
        assert!(gate.blockers[1].contains("not declared by inventory"));
        let gate_json = super::compatibility_inventory_gate_report_to_json(&gate);
        assert_eq!(gate_json["decision"], "blocked");
        assert_eq!(gate_json["missing_checks"][0], "required check");
        assert_eq!(gate_json["extra_fixture_checks"][0], "extra check");
    }

    #[test]
    fn runs_nowledge_shaped_fixture_against_shadow_engine() {
        let mut primary = Database::new();
        let mut shadow = DatabaseShadowEngine::default();
        let fixture = nowledge_memory_core_fixture();

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

        assert_eq!(report.fixture, "nowledge-memory-core");
        assert_eq!(report.shadow_engine, "skein-shadow");
        assert_eq!(report.primary_checks.len(), 190);
        assert_eq!(report.shadow_checks.len(), 190);
        assert_eq!(
            report
                .shadow_checks
                .iter()
                .filter(|check| check.status == CompatibilityShadowStatus::Matched)
                .count(),
            190
        );
        assert_eq!(
            report.shadow_checks.last().map(|check| check.status),
            Some(CompatibilityShadowStatus::Matched)
        );

        let cutover = assess_compatibility_cutover(&report, CompatibilityCutoverPolicy::default());
        assert_eq!(cutover.decision, CompatibilityCutoverDecision::Ready);
        assert_eq!(cutover.matched_checks, 190);
        assert!(cutover.primary_only_checks.is_empty());
        assert!(cutover.blockers.is_empty());

        let inventory_gate = assess_query_inventory_gate(
            &assess_query_inventory_coverage(&fixture, &nowledge_memory_core_inventory()),
            CompatibilityInventoryCoveragePolicy::default(),
        );
        let migration_gate = assess_compatibility_migration_gate(&inventory_gate, &cutover);
        assert_eq!(migration_gate.decision, CompatibilityCutoverDecision::Ready);
        assert_eq!(
            migration_gate.inventory_decision,
            CompatibilityCutoverDecision::Ready
        );
        assert_eq!(
            migration_gate.shadow_decision,
            CompatibilityCutoverDecision::Ready
        );
        assert!(migration_gate.blockers.is_empty());
    }

    #[test]
    fn migration_gate_combines_inventory_and_shadow_blockers() {
        let fixture = CompatibilityFixture {
            name: "partial".to_string(),
            setup: Vec::new(),
            checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "extra check",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.id AS id"),
                ExpectedRows::RowCount(0),
            ))],
        };
        let inventory = CompatibilityQueryInventory {
            name: "required".to_string(),
            required_checks: vec![CompatibilityQueryInventoryItem::new(
                "required check",
                "read",
            )],
        };
        let inventory_gate = assess_query_inventory_gate(
            &assess_query_inventory_coverage(&fixture, &inventory),
            CompatibilityInventoryCoveragePolicy {
                require_all_required_checks: true,
                allow_extra_fixture_checks: false,
            },
        );
        let shadow = CompatibilityCutoverReport {
            fixture: "other-fixture".to_string(),
            shadow_engine: "shadow".to_string(),
            decision: CompatibilityCutoverDecision::Blocked,
            total_checks: 1,
            matched_checks: 0,
            primary_only_checks: vec!["extra check".to_string()],
            blockers: vec!["shadow failed".to_string()],
        };

        let gate = assess_compatibility_migration_gate(&inventory_gate, &shadow);

        assert_eq!(gate.decision, CompatibilityCutoverDecision::Blocked);
        assert_eq!(
            gate.inventory_decision,
            CompatibilityCutoverDecision::Blocked
        );
        assert_eq!(gate.shadow_decision, CompatibilityCutoverDecision::Blocked);
        assert_eq!(gate.blockers.len(), 4);
        assert!(gate.blockers[0].contains("does not match"));
        assert!(gate.blockers[1].starts_with("inventory:"));
        assert!(gate.blockers[3].starts_with("shadow:"));
        let gate_json = super::compatibility_migration_gate_report_to_json(&gate);
        assert_eq!(gate_json["decision"], "blocked");
        assert_eq!(gate_json["inventory_decision"], "blocked");
        assert_eq!(gate_json["shadow_decision"], "blocked");
        assert_eq!(gate_json["blockers"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn migration_gate_bundle_reports_blocked_json() {
        let fixture = CompatibilityFixture {
            name: "partial".to_string(),
            setup: Vec::new(),
            checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "extra check",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.id AS id"),
                ExpectedRows::RowCount(0),
            ))],
        };
        let inventory = CompatibilityQueryInventory {
            name: "required".to_string(),
            required_checks: vec![CompatibilityQueryInventoryItem::new(
                "required check",
                "read",
            )],
        };
        let shadow = CompatibilityShadowReport {
            fixture: "partial".to_string(),
            shadow_engine: "shadow".to_string(),
            primary_checks: vec![CompatibilityCheckReport {
                name: "extra check".to_string(),
            }],
            shadow_checks: vec![CompatibilityShadowCheckReport {
                name: "extra check".to_string(),
                status: CompatibilityShadowStatus::PrimaryOnly,
            }],
        };

        let bundle = assess_compatibility_migration_gate_bundle(
            &fixture,
            &inventory,
            &shadow,
            CompatibilityInventoryCoveragePolicy {
                require_all_required_checks: true,
                allow_extra_fixture_checks: false,
            },
            CompatibilityCutoverPolicy::default(),
        );
        let json = super::compatibility_migration_gate_bundle_to_json(&bundle);

        assert_eq!(
            bundle.migration_gate.decision,
            CompatibilityCutoverDecision::Blocked
        );
        assert_eq!(json["coverage"]["missing_checks"][0], "required check");
        assert_eq!(json["inventory_gate"]["decision"], "blocked");
        assert_eq!(json["cutover"]["decision"], "blocked");
        assert_eq!(json["migration_gate"]["decision"], "blocked");
        assert_eq!(
            json["migration_gate"]["blockers"].as_array().unwrap().len(),
            4
        );
    }

    #[test]
    fn runs_fixture_against_external_shadow_command() {
        let mut primary = Database::new();
        let script = write_external_shadow_script(
            "external-shadow",
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *project_graph*) echo '{"primary_only":true}' ;;
    *MATCH*) echo '{"ok":{"rows":[{"title":"Graph foundations"}]}}' ;;
    *) echo '{"ok":{"rows":[]}}' ;;
  esac
done
"#,
        );
        let mut shadow = ExternalShadowCommand::spawn("external-shadow", "sh", [script]).unwrap();
        let fixture = CompatibilityFixture {
            name: "external-shadow-fixture".to_string(),
            setup: vec![CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
            )],
            checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "read title",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
                ExpectedRows::Exact(vec![row([(
                    "title",
                    Value::String("Graph foundations".to_string()),
                )])]),
            ))],
        };

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

        assert_eq!(report.shadow_engine, "external-shadow");
        assert_eq!(
            report.shadow_checks[0].status,
            CompatibilityShadowStatus::Matched
        );
    }

    #[test]
    fn reports_row_mismatch_with_fixture_context() {
        let mut db = Database::new();
        let fixture = CompatibilityFixture {
            name: "mismatch".to_string(),
            setup: vec![CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
            )],
            checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "wrong title",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
                ExpectedRows::Exact(vec![row([(
                    "title",
                    Value::String("Runtime strategy".to_string()),
                )])]),
            ))],
        };

        let error = run_compatibility_fixture(&mut db, &fixture).unwrap_err();

        assert!(error.to_string().contains("fixture 'mismatch'"));
        assert!(error.to_string().contains("wrong title"));
        assert!(error.to_string().contains("row mismatch"));
    }

    #[test]
    fn reports_shadow_row_mismatch_with_engine_context() {
        let mut primary = Database::new();
        let mut shadow = MismatchingShadowEngine;
        let fixture = CompatibilityFixture {
            name: "shadow-mismatch".to_string(),
            setup: vec![CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
            )],
            checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "title lookup",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
                ExpectedRows::Exact(vec![row([(
                    "title",
                    Value::String("Graph foundations".to_string()),
                )])]),
            ))],
        };

        let error =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

        assert!(error.to_string().contains("shadow-mismatch"));
        assert!(error.to_string().contains("title lookup"));
        assert!(error
            .to_string()
            .contains("shadow engine 'mismatching-shadow'"));
        assert!(error.to_string().contains("row mismatch"));
    }

    #[test]
    fn compares_shadow_error_classes() {
        let mut primary = Database::new();
        let mut shadow = DatabaseShadowEngine::default();
        let fixture = CompatibilityFixture {
            name: "error-class".to_string(),
            setup: Vec::new(),
            checks: vec![CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_error(
                    "missing parameter",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id = $missing RETURN m.title AS title",
                    ),
                    super::ExpectedErrorClass::Semantic,
                ),
            )],
        };

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

        assert_eq!(
            report.shadow_checks[0].status,
            CompatibilityShadowStatus::Matched
        );
    }

    #[test]
    fn compares_shadow_mutation_effects() {
        let mut primary = Database::new();
        let mut shadow = DatabaseShadowEngine::default();
        let fixture = CompatibilityFixture {
            name: "mutation-effect".to_string(),
            setup: vec![CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1, title: 'Old'})",
            )],
            checks: vec![CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "set title",
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'New'",
                    ),
                    ExpectedRows::RowCount(1),
                )
                .with_effect_query(
                    CypherFixtureStatement::new(
                        "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title",
                    ),
                    ExpectedRows::Exact(vec![row([("title", Value::String("New".to_string()))])]),
                ),
            )],
        };

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

        assert_eq!(
            report.shadow_checks[0].status,
            CompatibilityShadowStatus::Matched
        );
    }

    #[test]
    fn compares_shadow_float_rows_with_tolerance() {
        let mut primary = Database::new();
        let mut shadow = SlightlyDifferentFloatShadowEngine;
        let fixture = CompatibilityFixture {
            name: "float-tolerance".to_string(),
            setup: vec![
                CypherFixtureStatement::new(
                    "CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 2})",
                ),
                CypherFixtureStatement::new(
                    "CALL project_graph('FloatGraph', ['Memory', 'Entity'], ['MENTIONS'])",
                ),
            ],
            checks: vec![CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "pagerank score",
                    CypherFixtureStatement::new(
                        "CALL page_rank('FloatGraph') RETURN node, pagerank_score",
                    ),
                    ExpectedRows::RowCount(2),
                )
                .with_tolerance(CompatibilityTolerance { float_abs: 1.0e-6 }),
            )],
        };

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

        assert_eq!(
            report.shadow_checks[0].status,
            CompatibilityShadowStatus::Matched
        );
    }

    #[test]
    fn reports_shadow_projected_graph_mismatch_with_engine_context() {
        let mut primary = Database::new();
        let mut shadow = MismatchingProjectedGraphShadowEngine::default();
        let fixture = CompatibilityFixture {
            name: "projected-graph-mismatch".to_string(),
            setup: vec![CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 2})",
            )],
            checks: vec![CompatibilityCheck::ProjectedGraph(
                ProjectedGraphFixtureCheck {
                    name: "mentions projection".to_string(),
                    rel_type: Some("MENTIONS".to_string()),
                    expected_node_count: 2,
                    expected_edge_count: 1,
                    expected_incoming: vec![(1, vec![0])],
                    expected_communities: Vec::new(),
                    expected_hierarchical_communities: Vec::new(),
                    expected_page_rank_scores: Vec::new(),
                    page_rank_top_node: None,
                    tolerance: CompatibilityTolerance::default(),
                },
            )],
        };

        let error =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

        assert!(error.to_string().contains("projected-graph-mismatch"));
        assert!(error.to_string().contains("mentions projection"));
        assert!(error
            .to_string()
            .contains("shadow engine 'bad-projection-shadow'"));
        assert!(error
            .to_string()
            .contains("projected graph validation failed"));
    }

    #[test]
    fn keeps_projected_graph_primary_only_when_shadow_has_no_projection_hook() {
        let mut primary = Database::new();
        let mut shadow = NoProjectionShadowEngine::default();
        let fixture = CompatibilityFixture {
            name: "primary-only-projection".to_string(),
            setup: vec![CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 2})",
            )],
            checks: vec![CompatibilityCheck::ProjectedGraph(
                ProjectedGraphFixtureCheck {
                    name: "mentions projection".to_string(),
                    rel_type: Some("MENTIONS".to_string()),
                    expected_node_count: 2,
                    expected_edge_count: 1,
                    expected_incoming: vec![(1, vec![0])],
                    expected_communities: Vec::new(),
                    expected_hierarchical_communities: Vec::new(),
                    expected_page_rank_scores: Vec::new(),
                    page_rank_top_node: None,
                    tolerance: CompatibilityTolerance::default(),
                },
            )],
        };

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

        assert_eq!(
            report.shadow_checks[0].status,
            CompatibilityShadowStatus::PrimaryOnly
        );

        let cutover = assess_compatibility_cutover(&report, CompatibilityCutoverPolicy::default());
        assert_eq!(cutover.decision, CompatibilityCutoverDecision::Blocked);
        assert_eq!(
            cutover.primary_only_checks,
            vec!["mentions projection".to_string()]
        );
        assert!(cutover
            .blockers
            .iter()
            .any(|blocker| blocker.contains("did not cover checks")));

        let relaxed = assess_compatibility_cutover(
            &report,
            CompatibilityCutoverPolicy {
                require_shadow_for_all_checks: false,
                min_matched_checks: 0,
            },
        );
        assert_eq!(relaxed.decision, CompatibilityCutoverDecision::Ready);
    }

    #[test]
    fn cutover_gate_blocks_when_minimum_match_count_is_not_met() {
        let mut primary = Database::new();
        let mut shadow = DatabaseShadowEngine::default();
        let fixture = CompatibilityFixture {
            name: "single-check".to_string(),
            setup: vec![CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
            )],
            checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "title lookup",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
                ExpectedRows::Exact(vec![row([(
                    "title",
                    Value::String("Graph foundations".to_string()),
                )])]),
            ))],
        };

        let report =
            run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();
        let cutover = assess_compatibility_cutover(
            &report,
            CompatibilityCutoverPolicy {
                require_shadow_for_all_checks: true,
                min_matched_checks: 2,
            },
        );

        assert_eq!(cutover.decision, CompatibilityCutoverDecision::Blocked);
        assert!(cutover.blockers[0].contains("below required minimum"));
    }

    fn write_external_shadow_script(name: &str, content: &str) -> String {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("skein-{name}-{nonce}.sh"));
        fs::write(&path, content).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn row(items: impl IntoIterator<Item = (&'static str, Value)>) -> BTreeMap<String, Value> {
        items
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    #[derive(Debug, Default)]
    struct DatabaseShadowEngine {
        db: Database,
    }

    impl CompatibilityShadowEngine for DatabaseShadowEngine {
        fn name(&self) -> &str {
            "skein-shadow"
        }

        fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
            self.db
                .query_with_params(&statement.cypher, &statement.parameters)
        }

        fn project_graph(
            &mut self,
            check: &ProjectedGraphFixtureCheck,
        ) -> Result<Option<ProjectedGraphShadowOutput>> {
            let graph = self.db.project_graph(check.rel_type.as_deref());
            Ok(Some(super::projected_graph_shadow_output(&graph, check)))
        }
    }

    #[derive(Debug)]
    struct SlightlyDifferentFloatShadowEngine;

    impl CompatibilityShadowEngine for SlightlyDifferentFloatShadowEngine {
        fn name(&self) -> &str {
            "float-shadow"
        }

        fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
            if !statement.cypher.contains("page_rank") {
                return Ok(QueryOutput { rows: Vec::new() });
            }
            Ok(QueryOutput {
                rows: vec![
                    row([
                        ("node", Value::Int(1)),
                        ("pagerank_score", Value::Float(0.6491233)),
                    ]),
                    row([
                        ("node", Value::Int(0)),
                        ("pagerank_score", Value::Float(0.3508773)),
                    ]),
                ],
            })
        }
    }

    #[derive(Debug)]
    struct MismatchingShadowEngine;

    impl CompatibilityShadowEngine for MismatchingShadowEngine {
        fn name(&self) -> &str {
            "mismatching-shadow"
        }

        fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
            if statement.cypher.starts_with("MATCH") {
                Ok(QueryOutput {
                    rows: vec![row([(
                        "title",
                        Value::String("Runtime strategy".to_string()),
                    )])],
                })
            } else {
                Ok(QueryOutput { rows: Vec::new() })
            }
        }
    }

    #[derive(Debug, Default)]
    struct MismatchingProjectedGraphShadowEngine {
        db: Database,
    }

    impl CompatibilityShadowEngine for MismatchingProjectedGraphShadowEngine {
        fn name(&self) -> &str {
            "bad-projection-shadow"
        }

        fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
            self.db
                .query_with_params(&statement.cypher, &statement.parameters)
        }

        fn project_graph(
            &mut self,
            _check: &ProjectedGraphFixtureCheck,
        ) -> Result<Option<ProjectedGraphShadowOutput>> {
            Ok(Some(ProjectedGraphShadowOutput {
                node_count: 2,
                edge_count: 0,
                incoming: vec![(1, Vec::new())],
                communities: Vec::new(),
                hierarchical_communities: Vec::new(),
                page_rank_scores: Vec::new(),
                page_rank_top_node: None,
            }))
        }
    }

    #[derive(Debug, Default)]
    struct NoProjectionShadowEngine {
        db: Database,
    }

    impl CompatibilityShadowEngine for NoProjectionShadowEngine {
        fn name(&self) -> &str {
            "no-projection-shadow"
        }

        fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
            self.db
                .query_with_params(&statement.cypher, &statement.parameters)
        }
    }
}
