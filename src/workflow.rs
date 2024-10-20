use crate::step_function::Context;

use super::step_function::StepFunction;

use std::sync::Arc;

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Step {
    #[builder(setter(into))]
    pub(crate) name: String,
    #[builder(setter(custom))]
    function: Arc<StepFunction>,
    #[builder(default)]
    pub(crate) retries: usize,
    #[builder(default)]
    pub(crate) parents: Vec<String>,
    #[builder(default = "std::time::Duration::from_secs(60)")]
    pub(crate) timeout: std::time::Duration,
}

impl StepBuilder {
    pub fn function<I, O, Fut, F>(mut self, function: &'static F) -> Self
    where
        I: serde::de::DeserializeOwned,
        O: serde::ser::Serialize,
        Fut: std::future::Future<Output = anyhow::Result<O>> + 'static,
        F: Fn(Context, I) -> Fut,
    {
        use futures_util::FutureExt;
        self.function = Some(Arc::new(|context, value| {
            let result = function(
                context,
                serde_json::from_value(value).expect("must succeed"),
            );
            async { Ok(serde_json::to_value(result.await?).expect("must succeed")) }.boxed_local()
        }));
        self
    }
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Workflow {
    #[builder(setter(into))]
    pub(crate) name: String,
    #[builder(default, setter(into))]
    pub(crate) description: String,
    #[builder(default, setter(into))]
    pub(crate) version: String,
    #[builder(default, setter(custom))]
    pub(crate) steps: Vec<Step>,
    #[builder(default)]
    pub(crate) on_events: Vec<String>,
    #[builder(default)]
    pub(crate) on_crons: Vec<String>,
    #[builder(default = "std::time::Duration::from_secs(60)")]
    pub(crate) schedule_timeout: std::time::Duration,
}

impl WorkflowBuilder {
    pub fn step(mut self, step: Step) -> Self {
        let mut steps = self.steps.take().unwrap_or_default();
        steps.push(step);
        self.steps = Some(steps);
        self
    }
}

impl Workflow {
    pub(crate) fn actions<'a>(
        &'a self,
        namespace: &'a str,
    ) -> impl Iterator<Item = (String, Arc<StepFunction>)> + 'a {
        self.steps.iter().map(move |step| {
            (
                format!("{namespace}{}:{}", self.name, step.name),
                step.function.clone(),
            )
        })
    }
}
