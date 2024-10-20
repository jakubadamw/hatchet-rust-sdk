mod client;
mod error;
mod step_function;
mod worker;
mod workflow;

pub use error::{Error, Result};
pub(crate) use error::{InternalError, InternalResult};

#[derive(Clone, Copy, Debug, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum ClientTlStrategy {
    None,
    #[default]
    Tls,
    MTls,
}

pub use client::Client;
pub use step_function::Context;
pub use worker::{Worker, WorkerBuilder};
pub use workflow::{Step, StepBuilder, Workflow, WorkflowBuilder};

#[cfg(test)]
mod tests;
