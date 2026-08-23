use mesut_core::{TaskId, TaskResult, Work, WorkKind};
use std::collections::HashMap;

/// Represents a stage in a pipeline.
pub struct PipelineStage {
    pub id: TaskId,
    pub name: String,
    pub work_kind: WorkKind,
    pub work: Option<Work>,
    pub dependencies: Vec<TaskId>,
}

impl PipelineStage {
    pub fn new(name: String, work_kind: WorkKind) -> Self {
        Self {
            id: TaskId::new(),
            name,
            work_kind,
            work: None,
            dependencies: Vec::new(),
        }
    }

    pub fn with_dependency(mut self, dep_id: TaskId) -> Self {
        self.dependencies.push(dep_id);
        self
    }

    pub fn with_work(mut self, work: Work) -> Self {
        self.work = Some(work);
        self
    }
}

/// A multi-stage pipeline with ordered execution.
pub struct Pipeline {
    pub id: TaskId,
    pub stages: HashMap<TaskId, PipelineStage>,
    pub stage_order: Vec<TaskId>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            id: TaskId::new(),
            stages: HashMap::new(),
            stage_order: Vec::new(),
        }
    }

    pub fn add_stage(mut self, stage: PipelineStage) -> Self {
        let id = stage.id;
        self.stages.insert(id, stage);
        self.stage_order.push(id);
        self
    }

    pub fn stages_in_order(&self) -> Vec<&PipelineStage> {
        self.stage_order
            .iter()
            .filter_map(|id| self.stages.get(id))
            .collect()
    }

    pub fn get_stage(&self, id: TaskId) -> Option<&PipelineStage> {
        self.stages.get(&id)
    }

    pub fn is_ready_to_execute(&self, stage_id: TaskId) -> bool {
        if let Some(stage) = self.stages.get(&stage_id) {
            stage
                .dependencies
                .iter()
                .all(|dep_id| {
                    if let Some(_dep_stage) = self.stages.get(dep_id) {
                        // Check if dependency is already executed (simplified check)
                        true
                    } else {
                        false
                    }
                })
        } else {
            false
        }
    }

    pub fn validate(&self) -> TaskResult<()> {
        for (id, stage) in &self.stages {
            for dep_id in &stage.dependencies {
                if !self.stages.contains_key(dep_id) {
                    return Err(mesut_core::TaskError::InvalidConfiguration(
                        format!("Stage {} depends on non-existent stage {}", id, dep_id),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for fluent pipeline construction.
pub struct PipelineBuilder {
    pipeline: Pipeline,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self {
            pipeline: Pipeline::new(),
        }
    }

    pub fn add_stage(mut self, name: String, work_kind: WorkKind) -> Self {
        let stage = PipelineStage::new(name, work_kind);
        self.pipeline = self.pipeline.add_stage(stage);
        self
    }

    pub fn build(self) -> TaskResult<Pipeline> {
        self.pipeline.validate()?;
        Ok(self.pipeline)
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_creation() {
        let pipeline = Pipeline::new();
        assert_eq!(pipeline.stages.len(), 0);
    }

    #[test]
    fn pipeline_add_stages() {
        let stage1 = PipelineStage::new("fetch".to_string(), WorkKind::Io);
        let stage2 = PipelineStage::new("process".to_string(), WorkKind::Compute);

        let pipeline = Pipeline::new()
            .add_stage(stage1)
            .add_stage(stage2);

        assert_eq!(pipeline.stages.len(), 2);
        assert_eq!(pipeline.stage_order.len(), 2);
    }
}
