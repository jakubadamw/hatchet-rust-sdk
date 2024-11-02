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

pub trait UserStepFunction<I, O, H> {
    fn to_step_function(self) -> Arc<StepFunction>;
}

pub struct NoArguments;
pub struct ContextArgument;

impl<I, O, Fut, F> UserStepFunction<I, O, ContextArgument> for &'static F
where
    I: serde::de::DeserializeOwned,
    O: serde::ser::Serialize,
    Fut: std::future::Future<Output = anyhow::Result<O>> + 'static,
    F: Fn(Context, I) -> Fut,
{
    fn to_step_function(self) -> Arc<StepFunction> {
        use futures_util::FutureExt;
        Arc::new(|context, value| {
            let result = (self)(
                context,
                serde_json::from_value(value).expect("must succeed"),
            );
            async {
                let json_value = serde_json::to_value(result.await?).expect("must succeed");
                if !json_value.is_object() && !json_value.is_null() {
                    anyhow::bail!(
                        "the result of a step function must be `null` serializable to a JSON value of an object: {json_value:?}"
                    );
                }
                Ok(json_value)
            }
            .boxed_local()
        })
    }
}

impl<I, O, Fut, F> UserStepFunction<I, O, NoArguments> for &'static F
where
    I: serde::de::DeserializeOwned,
    O: serde::ser::Serialize,
    Fut: std::future::Future<Output = anyhow::Result<O>> + 'static,
    F: Fn(I) -> Fut,
{
    fn to_step_function(self) -> Arc<StepFunction> {
        use futures_util::FutureExt;
        Arc::new(|_context, value| {
            let result = (self)(serde_json::from_value(value).expect("must succeed"));
            async { Ok(serde_json::to_value(result.await?).expect("must succeed")) }.boxed_local()
        })
    }
}

impl StepBuilder {
    pub fn function<
        AnyVariant,
        I: serde::de::DeserializeOwned,
        O: serde::ser::Serialize,
        F: UserStepFunction<I, O, AnyVariant>,
    >(
        mut self,
        function: F,
    ) -> Self {
        self.function = Some(function.to_step_function());
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
