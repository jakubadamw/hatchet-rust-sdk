#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub(crate) struct Step {
    name: String,
    function: Box<
        dyn Fn(serde_json::Value) -> futures_util::future::BoxFuture<'static, serde_json::Value>,
    >,
    #[builder(default = "std::time::Duration::from_secs(60)")]
    timeout: std::time::Duration,
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Workflow {
    id: String,
    description: String,
    steps: Vec<Step>,
    #[builder(default)]
    on_events: Vec<String>,
    #[builder(default)]
    on_crons: Vec<String>,
}
