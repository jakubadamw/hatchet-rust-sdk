use std::sync::Arc;

pub(crate) type StepFunction =
    dyn Fn(serde_json::Value) -> futures_util::future::BoxFuture<'static, serde_json::Value>;

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub(crate) struct Step {
    pub(crate) name: String,
    function: Arc<StepFunction>,
    #[builder(default)]
    pub(crate) retries: usize,
    #[builder(default)]
    pub(crate) parents: Vec<String>,
    #[builder(default = "std::time::Duration::from_secs(60)")]
    pub(crate) timeout: std::time::Duration,
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Workflow {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) version: String,
    pub(crate) steps: Vec<Step>,
    #[builder(default)]
    pub(crate) on_events: Vec<String>,
    #[builder(default)]
    pub(crate) on_crons: Vec<String>,
    #[builder(default = "std::time::Duration::from_secs(60)")]
    pub(crate) schedule_timeout: std::time::Duration,
}

impl Workflow {
    pub(crate) fn actions<'a>(
        &'a self,
        namespace: &'a str,
    ) -> impl Iterator<Item = (String, Arc<StepFunction>)> + 'a {
        self.steps
            .iter()
            .map(move |step| (format!("{namespace}{}", step.name), step.function.clone()))
    }
}
