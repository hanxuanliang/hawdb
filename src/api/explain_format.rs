use super::{ExplainAnalyzeOutput, ExplainOutput};
use crate::executor::ReadExecutionProfile;
use crate::optimizer::{PhysicalPlan, PhysicalPlanChildren};
use std::fmt::{Display, Formatter, Write};
use unicode_width::UnicodeWidthStr;

const NOT_AVAILABLE: &str = "N/A";

impl Display for ExplainOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let rows = plan_rows(&self.physical_plan, |node, is_root| ExplainRow {
            id: String::new(),
            estimated_rows: is_root
                .then(|| format_estimated_rows(self.trace.selected_plan_cost.estimated_rows)),
            actual_rows: None,
            task: "root".to_string(),
            access_object: access_object(node),
            execution_info: None,
            operator_info: operator_info(node),
            memory: None,
            disk: None,
        });
        write_table(
            formatter,
            &["id", "estRows", "task", "access object", "operator info"],
            &rows
                .iter()
                .map(|row| {
                    vec![
                        row.id.as_str(),
                        optional_text(&row.estimated_rows),
                        row.task.as_str(),
                        row.access_object.as_str(),
                        row.operator_info.as_str(),
                    ]
                })
                .collect::<Vec<_>>(),
        )?;
        write!(
            formatter,
            "\noptimizer: mode={}, groups={}, cost={}, cache={}\nfingerprint: {}",
            self.trace.search_mode.as_str(),
            self.trace.groups,
            self.trace.selected_plan_cost.cost,
            self.plan_cache_lookup.as_str(),
            self.trace.selected_plan_fingerprint,
        )
    }
}

impl Display for ExplainAnalyzeOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let rows = plan_rows(&self.physical_plan, |node, is_root| {
            let blocking = self
                .execution_profile
                .blocking_operator_memory_reports
                .iter()
                .find(|report| report.operator == node.kind().as_str());
            ExplainRow {
                id: String::new(),
                estimated_rows: is_root
                    .then(|| format_estimated_rows(self.trace.selected_plan_cost.estimated_rows)),
                actual_rows: is_root.then(|| self.output.rows.len().to_string()),
                task: "root".to_string(),
                access_object: access_object(node),
                execution_info: if is_root {
                    Some(root_execution_info(&self.execution_profile))
                } else {
                    blocking.map(|report| format!("input_rows={}", report.input_rows))
                },
                operator_info: operator_info(node),
                memory: blocking
                    .map(|report| {
                        format!(
                            "peak={}/budget={}",
                            format_bytes(report.peak_tracked_bytes as u64),
                            format_bytes(report.budget_bytes as u64),
                        )
                    })
                    .or_else(|| {
                        is_root.then(|| {
                            format_bytes(
                                self.execution_profile
                                    .pipeline_memory_report
                                    .peak_batch_payload_bytes
                                    as u64,
                            )
                        })
                    }),
                disk: blocking.and_then(|report| {
                    (report.spill_run_count > 0).then(|| {
                        format!(
                            "runs={}, rows={}",
                            report.spill_run_count, report.spilled_rows
                        )
                    })
                }),
            }
        });
        write_table(
            formatter,
            &[
                "id",
                "estRows",
                "actRows",
                "task",
                "access object",
                "execution info",
                "operator info",
                "memory",
                "disk",
            ],
            &rows
                .iter()
                .map(|row| {
                    vec![
                        row.id.as_str(),
                        optional_text(&row.estimated_rows),
                        optional_text(&row.actual_rows),
                        row.task.as_str(),
                        row.access_object.as_str(),
                        optional_text(&row.execution_info),
                        row.operator_info.as_str(),
                        optional_text(&row.memory),
                        optional_text(&row.disk),
                    ]
                })
                .collect::<Vec<_>>(),
        )?;
        write!(
            formatter,
            "\noptimizer: mode={}, groups={}, cost={}, cache={}\nfingerprint: {}",
            self.trace.search_mode.as_str(),
            self.trace.groups,
            self.trace.selected_plan_cost.cost,
            self.plan_cache_lookup.as_str(),
            self.trace.selected_plan_fingerprint,
        )
    }
}

#[derive(Debug)]
struct ExplainRow {
    id: String,
    estimated_rows: Option<String>,
    actual_rows: Option<String>,
    task: String,
    access_object: String,
    execution_info: Option<String>,
    operator_info: String,
    memory: Option<String>,
    disk: Option<String>,
}

fn plan_rows(
    plan: &PhysicalPlan,
    mut make_row: impl FnMut(&PhysicalPlan, bool) -> ExplainRow,
) -> Vec<ExplainRow> {
    fn visit(
        plan: &PhysicalPlan,
        is_root: bool,
        ancestors_have_sibling: &mut Vec<bool>,
        is_last: bool,
        make_row: &mut dyn FnMut(&PhysicalPlan, bool) -> ExplainRow,
        rows: &mut Vec<ExplainRow>,
    ) {
        let mut row = make_row(plan, is_root);
        row.id = tree_identifier(
            plan.kind().as_str(),
            is_root,
            ancestors_have_sibling,
            is_last,
        );
        rows.push(row);

        match plan.children() {
            PhysicalPlanChildren::None => {}
            PhysicalPlanChildren::Unary(child) => {
                ancestors_have_sibling.push(false);
                visit(child, false, ancestors_have_sibling, true, make_row, rows);
                ancestors_have_sibling.pop();
            }
            PhysicalPlanChildren::Binary(left, right) => {
                ancestors_have_sibling.push(true);
                visit(left, false, ancestors_have_sibling, false, make_row, rows);
                ancestors_have_sibling.pop();
                ancestors_have_sibling.push(false);
                visit(right, false, ancestors_have_sibling, true, make_row, rows);
                ancestors_have_sibling.pop();
            }
        }
    }

    let mut rows = Vec::new();
    visit(plan, true, &mut Vec::new(), true, &mut make_row, &mut rows);
    rows
}

fn tree_identifier(
    kind: &str,
    is_root: bool,
    ancestors_have_sibling: &[bool],
    is_last: bool,
) -> String {
    if is_root {
        return kind.to_string();
    }
    let mut id = String::new();
    for has_sibling in ancestors_have_sibling
        .iter()
        .take(ancestors_have_sibling.len().saturating_sub(1))
    {
        id.push_str(if *has_sibling { "│ " } else { "  " });
    }
    id.push_str(if is_last { "└─" } else { "├─" });
    id.push_str(kind);
    id
}

fn access_object(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::SeqNodeScan { label, .. } => format!("label:{label}"),
        PhysicalPlan::SourceSegmentScan { .. } => "source-segments".to_string(),
        PhysicalPlan::NodeColumnLookupExec {
            label,
            property,
            column,
            ..
        } => format!("label:{label}, property:{property}, column:{column}"),
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeRangeSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeTextSeek {
            label, property, ..
        } => format!("label:{label}, index:{property}"),
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => format!(
            "label:{label}, index:{}",
            predicates
                .iter()
                .map(|(property, _)| property.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
        PhysicalPlan::AdjacencyExpandExec { rel_type, .. }
        | PhysicalPlan::OptionalDegreeExec { rel_type, .. }
        | PhysicalPlan::ShortestPathExec { rel_type, .. } => format!("rel:{rel_type}"),
        PhysicalPlan::GraphAlgorithm { graph_name, .. } => format!("graph:{graph_name}"),
        _ => String::new(),
    }
}

fn operator_info(plan: &PhysicalPlan) -> String {
    let kind = plan.kind().as_str();
    plan.explain(0)
        .lines()
        .next()
        .unwrap_or(kind)
        .strip_prefix(kind)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn root_execution_info(profile: &ReadExecutionProfile) -> String {
    let pipeline = &profile.pipeline_memory_report;
    let mut fields = vec![
        format!("output_rows={}", pipeline.output_rows),
        format!("output_bytes={}", pipeline.output_payload_bytes),
        format!("intermediate_rows={}", pipeline.intermediate_rows),
    ];
    if let Some(peak_resident_bytes) = pipeline.peak_resident_bytes {
        fields.push(format!("peak_rss={}", format_bytes(peak_resident_bytes)));
    }
    if let Some(minor_page_faults) = pipeline.minor_page_faults {
        fields.push(format!("minor_faults={minor_page_faults}"));
    }
    if let Some(major_page_faults) = pipeline.major_page_faults {
        fields.push(format!("major_faults={major_page_faults}"));
    }
    fields.join(", ")
}

fn format_estimated_rows(rows: u64) -> String {
    format!("{rows}.00")
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn optional_text(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or(NOT_AVAILABLE)
}

fn write_table(
    formatter: &mut Formatter<'_>,
    headers: &[&str],
    rows: &[Vec<&str>],
) -> std::fmt::Result {
    let widths = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            rows.iter()
                .map(|row| display_width(row[column]))
                .max()
                .unwrap_or_default()
                .max(display_width(header))
        })
        .collect::<Vec<_>>();

    write_border(formatter, &widths)?;
    write_cells(formatter, headers, &widths)?;
    write_border(formatter, &widths)?;
    for row in rows {
        write_cells(formatter, row, &widths)?;
    }
    write_border(formatter, &widths)
}

fn write_border(formatter: &mut Formatter<'_>, widths: &[usize]) -> std::fmt::Result {
    formatter.write_char('+')?;
    for width in widths {
        for _ in 0..width.saturating_add(2) {
            formatter.write_char('-')?;
        }
        formatter.write_char('+')?;
    }
    formatter.write_char('\n')
}

fn write_cells(
    formatter: &mut Formatter<'_>,
    cells: &[impl AsRef<str>],
    widths: &[usize],
) -> std::fmt::Result {
    formatter.write_char('|')?;
    for (cell, width) in cells.iter().zip(widths) {
        let cell = cell.as_ref();
        write!(
            formatter,
            " {cell}{} |",
            " ".repeat(width.saturating_sub(display_width(cell)))
        )?;
    }
    formatter.write_char('\n')
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

#[cfg(test)]
mod tests {
    use super::format_bytes;
    use crate::{Database, Value};

    #[test]
    fn explain_is_directly_printable_as_a_tree_table() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();

        let rendered = db
            .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap()
            .to_string();

        assert!(rendered.contains("| id"));
        assert!(rendered.contains("| estRows"));
        assert!(rendered.contains("ProjectExec"));
        assert!(rendered.contains("└─FilterExec"));
        assert!(rendered.contains("  └─SeqNodeScan"));
        assert!(rendered.contains("label:Memory"));
        assert!(rendered.contains("optimizer: mode=memo"));
        assert!(rendered.contains("fingerprint:"));
    }

    #[test]
    fn explain_analyze_prints_measured_pipeline_and_blocking_memory() {
        let mut db = Database::new();
        for id in [2, 1, 3] {
            db.query(&format!("CREATE (:Memory {{id: {id}}})")).unwrap();
        }

        let output = db
            .explain_analyze_query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
            .unwrap();
        assert_eq!(output.output.rows[0].get("id"), Some(&Value::Int(1)));
        let rendered = output.to_string();

        assert!(rendered.contains("| actRows"));
        assert!(rendered.contains("output_rows=3"));
        assert!(rendered.contains("intermediate_rows="));
        assert!(rendered.contains("SortExec"));
        assert!(rendered.contains("peak="));
        assert!(rendered.contains("/budget="));
    }

    #[test]
    fn formats_binary_byte_units_without_platform_dependencies() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
    }

    #[test]
    fn aligns_wide_unicode_by_terminal_column_width() {
        assert_eq!(super::display_width("Memory"), 6);
        assert_eq!(super::display_width("记忆"), 4);
    }
}
