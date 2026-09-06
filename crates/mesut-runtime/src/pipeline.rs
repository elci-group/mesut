use mesut_core::{CancellationToken, TaskError, TaskId, TaskResult, Work, WorkKind};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Outputs keyed by stage ID. Factories receive only declared dependencies.
pub type PipelineOutputs = HashMap<TaskId, Arc<Vec<u8>>>;
type StageFactory = Box<dyn FnOnce(PipelineOutputs) -> TaskResult<Work> + Send>;

/// Represents a stage in a pipeline.
pub struct PipelineStage {
    pub id: TaskId,
    pub name: String,
    pub work_kind: WorkKind,
    pub work: Option<Work>,
    pub dependencies: Vec<TaskId>,
    factory: Option<StageFactory>,
}

impl PipelineStage {
    pub fn new(name: String, work_kind: WorkKind) -> Self {
        Self {
            id: TaskId::new(),
            name,
            work_kind,
            work: None,
            dependencies: Vec::new(),
            factory: None,
        }
    }

    pub fn with_dependency(mut self, dep_id: TaskId) -> Self {
        self.dependencies.push(dep_id);
        self
    }

    pub fn with_work(mut self, work: Work) -> Self {
        self.work = Some(work);
        self.factory = None;
        self
    }

    /// Build work from prerequisite outputs. The factory must not block;
    /// put expensive processing inside the returned work's job or future.
    pub fn with_factory(
        mut self,
        factory: impl FnOnce(PipelineOutputs) -> TaskResult<Work> + Send + 'static,
    ) -> Self {
        self.factory = Some(Box::new(factory));
        self.work = None;
        self
    }
}

/// A multi-stage pipeline with ordered execution.
pub struct Pipeline {
    pub id: TaskId,
    pub stages: HashMap<TaskId, PipelineStage>,
    pub stage_order: Vec<TaskId>,
    cancellation: CancellationToken,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            id: TaskId::new(),
            stages: HashMap::new(),
            stage_order: Vec::new(),
            cancellation: CancellationToken::new(),
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

    /// Whether a stage is ready before any stage has completed.
    pub fn is_ready_to_execute(&self, stage_id: TaskId) -> bool {
        self.is_ready_after(stage_id, &HashSet::new())
    }

    /// A stage is ready only after all prerequisites complete successfully.
    pub fn is_ready_after(&self, stage_id: TaskId, completed: &HashSet<TaskId>) -> bool {
        !completed.contains(&stage_id)
            && self.stages.get(&stage_id).is_some_and(|stage| {
                stage
                    .dependencies
                    .iter()
                    .all(|id| self.stages.contains_key(id) && completed.contains(id))
            })
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn validate(&self) -> TaskResult<()> {
        self.execution_order().map(|_| ())
    }

    /// Topological ordering with insertion order breaking ties between ready stages.
    pub fn execution_order(&self) -> TaskResult<Vec<TaskId>> {
        let positions: HashMap<_, _> = self
            .stage_order
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index))
            .collect();
        if positions.len() != self.stage_order.len()
            || positions.len() != self.stages.len()
            || self.stages.keys().any(|id| !positions.contains_key(id))
        {
            return Err(TaskError::InvalidConfiguration(
                "pipeline order must contain every stage exactly once".into(),
            ));
        }
        let mut indegree = HashMap::new();
        let mut dependents: HashMap<TaskId, Vec<TaskId>> = HashMap::new();
        for (id, stage) in &self.stages {
            if *id != stage.id {
                return Err(TaskError::InvalidConfiguration(
                    "stage ID does not match its map key".into(),
                ));
            }
            let mut unique = HashSet::new();
            for dep_id in &stage.dependencies {
                if !self.stages.contains_key(dep_id) {
                    return Err(mesut_core::TaskError::InvalidConfiguration(format!(
                        "Stage {} depends on non-existent stage {}",
                        id, dep_id
                    )));
                }
                if !unique.insert(dep_id) {
                    return Err(TaskError::InvalidConfiguration(format!(
                        "stage {id} repeats dependency {dep_id}"
                    )));
                }
                dependents.entry(*dep_id).or_default().push(*id);
            }
            indegree.insert(*id, stage.dependencies.len());
        }
        let mut ready: BTreeSet<_> = self
            .stage_order
            .iter()
            .filter(|id| indegree[id] == 0)
            .map(|id| positions[id])
            .collect();
        let mut order = Vec::with_capacity(self.stages.len());
        while let Some(index) = ready.pop_first() {
            let id = self.stage_order[index];
            order.push(id);
            for dependent in dependents.get(&id).into_iter().flatten() {
                if let Some(count) = indegree.get_mut(dependent) {
                    *count -= 1;
                    if *count == 0 {
                        ready.insert(positions[dependent]);
                    }
                }
            }
        }
        if order.len() != self.stages.len() {
            return Err(TaskError::InvalidConfiguration(
                "pipeline contains a dependency cycle".into(),
            ));
        }
        Ok(order)
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::MesuT {
    /// Execute a DAG with bounded parallelism. Insertion order breaks ties
    /// among ready stages; completion order is nondeterministic when parallel.
    /// Errors/dropping this future cancel sibling stages. Synchronous jobs must
    /// cooperate; shutdown/join can be used to await physical quiescence.
    pub async fn execute_pipeline(&self, mut pipeline: Pipeline) -> TaskResult<PipelineOutputs> {
        tokio::runtime::Handle::try_current().map_err(|_| TaskError::ExecutorUnavailable)?;
        if self.config.pipeline_concurrency == 0 {
            return Err(TaskError::InvalidConfiguration(
                "pipeline concurrency must be positive".into(),
            ));
        }
        let mut remaining = pipeline.execution_order()?;
        for stage in pipeline.stages.values() {
            if stage.work.is_none() && stage.factory.is_none() {
                return Err(TaskError::InvalidConfiguration(format!(
                    "stage {} has no work or factory",
                    stage.id
                )));
            }
            if stage
                .work
                .as_ref()
                .is_some_and(|work| work.kind != stage.work_kind)
            {
                return Err(TaskError::InvalidConfiguration(format!(
                    "stage {} has mismatched work kind",
                    stage.id
                )));
            }
        }
        let mut guard = ActiveStage {
            token: pipeline.cancellation.clone(),
            finished: false,
        };
        let mut running = tokio::task::JoinSet::new();
        let mut outputs = PipelineOutputs::new();
        loop {
            if pipeline.cancellation.is_cancelled() {
                return Err(TaskError::Cancelled);
            }
            while running.len() < self.config.pipeline_concurrency {
                let ready = remaining.iter().position(|id| {
                    pipeline.stages[id]
                        .dependencies
                        .iter()
                        .all(|id| outputs.contains_key(id))
                });
                let Some(index) = ready else { break };
                let id = remaining.remove(index);
                let stage = pipeline
                    .stages
                    .remove(&id)
                    .ok_or_else(|| TaskError::Internal("validated stage missing".into()))?;
                let inputs = stage
                    .dependencies
                    .iter()
                    .map(|id| (*id, outputs[id].clone()))
                    .collect();
                let runtime = self.clone();
                let token = pipeline.cancellation.clone();
                running.spawn(async move {
                    run_stage(&runtime, stage, inputs, token)
                        .await
                        .map(|output| (id, output))
                });
            }
            if running.is_empty() {
                if !remaining.is_empty() {
                    return Err(TaskError::Internal(
                        "validated graph made no progress".into(),
                    ));
                }
                guard.finished = true;
                return Ok(outputs);
            }
            let completed = tokio::select! {
                biased;
                _ = wait_for_cancellation(&pipeline.cancellation) => return Err(TaskError::Cancelled),
                completed = running.join_next() => completed,
            };
            let (id, output) = completed
                .ok_or_else(|| TaskError::Internal("stage join set closed".into()))?
                .map_err(|error| {
                    TaskError::ExecutionFailed(format!("stage runner failed: {error}"))
                })??;
            outputs.insert(id, Arc::new(output));
        }
    }
}

async fn run_stage(
    runtime: &crate::MesuT,
    mut stage: PipelineStage,
    inputs: PipelineOutputs,
    token: CancellationToken,
) -> TaskResult<Vec<u8>> {
    let id = stage.id;
    let mut work = if let Some(factory) = stage.factory.take() {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory(inputs)))
            .map_err(|_| TaskError::ExecutionFailed(format!("stage {id} factory panicked")))??
    } else {
        stage
            .work
            .take()
            .ok_or_else(|| TaskError::Internal("validated work missing".into()))?
    };
    if work.kind != stage.work_kind {
        let error = TaskError::InvalidConfiguration(format!("stage {id} has mismatched work kind"));
        work.finish(Err(error.clone()));
        return Err(error);
    }
    if token.is_cancelled() {
        return Err(TaskError::Cancelled);
    }
    work.id = id;
    work.cancellation =
        CancellationToken::with_parents(vec![work.cancellation.clone(), token.clone()]);
    let mut active = ActiveStage {
        token: work.cancellation.clone(),
        finished: false,
    };
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let work = work.on_complete(move |result| {
        let _ = sender.send(result.clone());
    });
    let submitted = tokio::select! {
        biased;
        _ = wait_for_cancellation(&token) => return Err(TaskError::Cancelled),
        result = runtime.submit(work) => result,
    };
    submitted?;
    let result = tokio::select! {
        biased;
        _ = wait_for_cancellation(&token) => return Err(TaskError::Cancelled),
        result = receiver => result.map_err(|_| TaskError::Internal(format!("stage {id} completion channel closed")))?,
    };
    active.finished = true;
    result
}

async fn wait_for_cancellation(token: &CancellationToken) {
    while !token.is_cancelled() {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

struct ActiveStage {
    token: CancellationToken,
    finished: bool,
}

impl Drop for ActiveStage {
    fn drop(&mut self) {
        if !self.finished {
            self.token.cancel();
        }
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

    /// Add a complete stage, including executable work and dependencies.
    pub fn with_stage(mut self, stage: PipelineStage) -> Self {
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

        let pipeline = Pipeline::new().add_stage(stage1).add_stage(stage2);

        assert_eq!(pipeline.stages.len(), 2);
        assert_eq!(pipeline.stage_order.len(), 2);
    }
}
