use std::sync::Arc;

pub(crate) type StepFunction =
    dyn Fn(serde_json::Value) -> futures_util::future::BoxFuture<'static, serde_json::Value>;

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub(crate) struct Step {
    name: String,
    function: Arc<StepFunction>,
    #[builder(default = "std::time::Duration::from_secs(60)")]
    timeout: std::time::Duration,
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Workflow {
    id: String,
    description: String,
    pub(crate) steps: Vec<Step>,
    #[builder(default)]
    on_events: Vec<String>,
    #[builder(default)]
    on_crons: Vec<String>,
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
