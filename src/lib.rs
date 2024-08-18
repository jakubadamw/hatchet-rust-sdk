mod client;
mod error;
mod worker;
mod workflow;

pub use error::{Error, Result};

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum ClientTlStrategy {
    None,
    #[default]
    Tls,
    MTls,
}

pub use client::Client;
pub use worker::{Worker, WorkerBuilder};
pub use workflow::{Workflow, WorkflowBuilder};

#[cfg(test)]
mod tests;
