use super::{optional_u64_value, optional_usize_value, Database, QueryOutput};
use crate::error::{Result, SkeinError};
use crate::executor::Row;
use crate::value::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedArtifactJobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl DerivedArtifactJobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DerivedArtifactJobStatus::Pending => "pending",
            DerivedArtifactJobStatus::Running => "running",
            DerivedArtifactJobStatus::Succeeded => "succeeded",
            DerivedArtifactJobStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedArtifactJob {
    pub id: u64,
    pub artifact_type: String,
    pub name: String,
    pub action: String,
    pub payload: BTreeMap<String, Value>,
    pub status: DerivedArtifactJobStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedArtifactJobReport {
    pub job: DerivedArtifactJob,
    pub output: QueryOutput,
}

impl Database {
    pub fn schedule_derived_artifact_rebuild(&mut self) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job("projected_graph", "*", "rebuild")
    }

    pub fn schedule_projected_graph_artifact_rebuild(
        &mut self,
        name: impl Into<String>,
    ) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job("projected_graph", name.into(), "rebuild")
    }

    pub fn schedule_external_content_artifact_job(
        &mut self,
        name: impl Into<String>,
        action: impl Into<String>,
    ) -> DerivedArtifactJob {
        self.schedule_external_content_artifact_job_with_payload(name, action, BTreeMap::new())
    }

    pub fn schedule_external_content_artifact_job_with_payload(
        &mut self,
        name: impl Into<String>,
        action: impl Into<String>,
        payload: BTreeMap<String, Value>,
    ) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job_with_payload(
            "content_artifact",
            name.into(),
            action.into(),
            payload,
        )
    }

    pub fn derived_artifact_jobs(&self) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs.clone()
    }

    pub fn pending_external_content_artifact_jobs(&self, limit: usize) -> Vec<DerivedArtifactJob> {
        self.derived_artifact_jobs
            .iter()
            .filter(|job| {
                job.status == DerivedArtifactJobStatus::Pending
                    && is_external_content_artifact_job(&job.artifact_type)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn run_next_derived_artifact_job(&mut self) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(index) = self
            .derived_artifact_jobs
            .iter()
            .position(|job| job.status == DerivedArtifactJobStatus::Pending)
        else {
            return Ok(None);
        };

        self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Running;
        self.derived_artifact_jobs[index].attempts += 1;
        self.derived_artifact_jobs[index].last_error = None;

        let artifact_type = self.derived_artifact_jobs[index].artifact_type.clone();
        let name = self.derived_artifact_jobs[index].name.clone();
        let action = self.derived_artifact_jobs[index].action.clone();
        let result = self.execute_derived_artifact_job(&artifact_type, &name, &action);

        match result {
            Ok(output) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Succeeded;
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output,
                }))
            }
            Err(error) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Failed;
                self.derived_artifact_jobs[index].last_error = Some(error.to_string());
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output: QueryOutput {
                        rows: vec![derived_artifact_job_failure_row(
                            &self.derived_artifact_jobs[index],
                            &error.to_string(),
                        )],
                    },
                }))
            }
        }
    }

    pub fn run_next_external_content_artifact_job_with(
        &mut self,
        mut runtime: impl FnMut(&DerivedArtifactJob) -> Result<QueryOutput>,
    ) -> Result<Option<DerivedArtifactJobReport>> {
        self.ensure_writable()?;
        let Some(index) = self.derived_artifact_jobs.iter().position(|job| {
            job.status == DerivedArtifactJobStatus::Pending
                && is_external_content_artifact_job(&job.artifact_type)
        }) else {
            return Ok(None);
        };

        self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Running;
        self.derived_artifact_jobs[index].attempts += 1;
        self.derived_artifact_jobs[index].last_error = None;

        let runtime_job = self.derived_artifact_jobs[index].clone();
        match runtime(&runtime_job) {
            Ok(output) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Succeeded;
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output,
                }))
            }
            Err(error) => {
                self.derived_artifact_jobs[index].status = DerivedArtifactJobStatus::Failed;
                self.derived_artifact_jobs[index].last_error = Some(error.to_string());
                Ok(Some(DerivedArtifactJobReport {
                    job: self.derived_artifact_jobs[index].clone(),
                    output: QueryOutput {
                        rows: vec![derived_artifact_job_failure_row(
                            &self.derived_artifact_jobs[index],
                            &error.to_string(),
                        )],
                    },
                }))
            }
        }
    }

    pub fn rebuild_derived_artifacts(&mut self) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let before = self
            .store
            .projected_graph_statuses()
            .into_iter()
            .map(|status| (status.name.clone(), status))
            .collect::<BTreeMap<_, _>>();
        self.store
            .rebuild_projected_graph_artifacts(&self.catalog)?;
        let rows = self
            .store
            .projected_graph_statuses()
            .into_iter()
            .map(|status| {
                let before_reusable = before
                    .get(&status.name)
                    .map(|status| status.reusable)
                    .unwrap_or(false);
                BTreeMap::from([
                    (
                        "artifact_type".to_string(),
                        Value::String("projected_graph".to_string()),
                    ),
                    ("name".to_string(), Value::String(status.name)),
                    ("action".to_string(), Value::String("rebuilt".to_string())),
                    ("before_reusable".to_string(), Value::Bool(before_reusable)),
                    ("after_reusable".to_string(), Value::Bool(status.reusable)),
                    (
                        "projection_epoch".to_string(),
                        optional_u64_value(status.projection_epoch),
                    ),
                    (
                        "commit_epoch".to_string(),
                        optional_u64_value(status.commit_epoch),
                    ),
                    (
                        "node_count".to_string(),
                        optional_usize_value(status.node_count),
                    ),
                    (
                        "edge_count".to_string(),
                        optional_usize_value(status.edge_count),
                    ),
                ])
            })
            .collect();
        Ok(QueryOutput { rows })
    }

    fn enqueue_derived_artifact_job(
        &mut self,
        artifact_type: impl Into<String>,
        name: impl Into<String>,
        action: impl Into<String>,
    ) -> DerivedArtifactJob {
        self.enqueue_derived_artifact_job_with_payload(artifact_type, name, action, BTreeMap::new())
    }

    fn enqueue_derived_artifact_job_with_payload(
        &mut self,
        artifact_type: impl Into<String>,
        name: impl Into<String>,
        action: impl Into<String>,
        payload: BTreeMap<String, Value>,
    ) -> DerivedArtifactJob {
        let job = DerivedArtifactJob {
            id: self.next_derived_artifact_job_id,
            artifact_type: artifact_type.into(),
            name: name.into(),
            action: action.into(),
            payload,
            status: DerivedArtifactJobStatus::Pending,
            attempts: 0,
            last_error: None,
        };
        self.next_derived_artifact_job_id += 1;
        self.derived_artifact_jobs.push(job.clone());
        job
    }

    fn execute_derived_artifact_job(
        &mut self,
        artifact_type: &str,
        name: &str,
        action: &str,
    ) -> Result<QueryOutput> {
        if is_external_content_artifact_job(artifact_type) {
            return Err(SkeinError::Semantic(format!(
                "derived artifact job {artifact_type}.{name} action {action} is outside the graph kernel; run it in the content artifact job runtime"
            )));
        }

        if artifact_type != "projected_graph" || action != "rebuild" {
            return Err(SkeinError::Semantic(format!(
                "unsupported derived artifact job {artifact_type}.{name} action {action}"
            )));
        }

        if name != "*"
            && !self
                .store
                .projected_graph_statuses()
                .iter()
                .any(|status| status.name == name)
        {
            return Err(SkeinError::Semantic(format!(
                "unknown projected graph artifact '{name}'"
            )));
        }

        let mut output = self.rebuild_derived_artifacts()?;
        if name != "*" {
            output.rows.retain(|row| {
                row.get("name")
                    .is_some_and(|value| value == &Value::String(name.to_string()))
            });
        }
        Ok(output)
    }
}

fn derived_artifact_job_failure_row(job: &DerivedArtifactJob, error: &str) -> Row {
    BTreeMap::from([
        ("job_id".to_string(), Value::Int(job.id as i64)),
        (
            "artifact_type".to_string(),
            Value::String(job.artifact_type.clone()),
        ),
        ("name".to_string(), Value::String(job.name.clone())),
        ("action".to_string(), Value::String(job.action.clone())),
        ("payload".to_string(), Value::Map(job.payload.clone())),
        (
            "status".to_string(),
            Value::String(job.status.as_str().to_string()),
        ),
        ("attempts".to_string(), Value::Int(job.attempts as i64)),
        ("error".to_string(), Value::String(error.to_string())),
    ])
}

fn is_external_content_artifact_job(artifact_type: &str) -> bool {
    matches!(
        artifact_type,
        "content_artifact" | "artifact_parse" | "content_parse" | "blob_parse" | "crawler"
    )
}
