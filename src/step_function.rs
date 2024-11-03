use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{atomic::AtomicU16, Arc},
};

use tokio::sync::Mutex;
use tracing::info;
use ustr::Ustr;

use crate::worker::{grpc, ServiceWithAuthorization};

pub(crate) type DataMap = HashMap<TypeId, Box<dyn Any + Send + Sync>>;

pub struct Context {
    workflow_run_id: String,
    workflow_step_run_id: String,
    workflow_service_client: Mutex<
        grpc::workflow_service_client::WorkflowServiceClient<
            tonic::service::interceptor::InterceptedService<
                tonic::transport::Channel,
                ServiceWithAuthorization,
            >,
        >,
    >,
    spawn_index: AtomicU16,
    data: Arc<DataMap>,
    parents_outputs: HashMap<Ustr, serde_json::Value>,
}

impl Context {
    pub(crate) fn new(
        parents_outputs: HashMap<Ustr, serde_json::Value>,
        workflow_run_id: String,
        workflow_step_run_id: String,
        workflow_service_client: grpc::workflow_service_client::WorkflowServiceClient<
            tonic::service::interceptor::InterceptedService<
                tonic::transport::Channel,
                ServiceWithAuthorization,
            >,
        >,
        data: Arc<DataMap>,
    ) -> Self {
        Self {
            parents_outputs,
            workflow_run_id,
            workflow_service_client: Mutex::new(workflow_service_client),
            spawn_index: AtomicU16::new(0),
            workflow_step_run_id,
            data,
        }
    }

    pub fn pop_parent_output<O: serde::de::DeserializeOwned>(&mut self, step_name: &str) -> O {
        serde_json::from_value(
            self.parents_outputs
                .remove(&step_name.into())
                .unwrap_or_else(|| panic!("could not find the output for step '{step_name}'")),
        )
        .expect("could not deserialize from JSON")
    }

    pub fn datum<D: std::any::Any + Send + Sync>(&self) -> &D {
        let type_id = TypeId::of::<D>();
        self.data
            .get(&type_id)
            .and_then(|value| value.downcast_ref())
            .unwrap_or_else(|| panic!("could not find an attached datum of the type: {type_id:?}"))
    }

    pub async fn trigger_workflow<I: serde::Serialize>(
        &self,
        workflow_name: &str,
        input: I,
    ) -> anyhow::Result<()> {
        info!("Scheduling another workflow {workflow_name}");
        let spawn_index = self
            .spawn_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let response = {
            let mut workflow_service_client = self.workflow_service_client.lock().await;
            workflow_service_client
                .trigger_workflow(grpc::TriggerWorkflowRequest {
                    name: workflow_name.to_owned(),
                    input: serde_json::to_string(&input).expect("must succeed"),
                    parent_id: Some(self.workflow_run_id.clone()),
                    parent_step_run_id: Some(self.workflow_step_run_id.clone()),
                    child_index: Some(spawn_index as i32),
                    child_key: None,
                    additional_metadata: None, // FIXME: Add support.
                    desired_worker_id: None,   // FIXME: Add support.
                    priority: Some(1),         // FIXME: Add support.
                })
                .await
                .map_err(crate::InternalError::CouldNotTriggerWorkflow)
                .map_err(crate::Error::Internal)?
                .into_inner()
        };
        info!(
            "Scheduled another workflow run ID: {}",
            response.workflow_run_id
        );
        Ok(())
    }
}

pub(crate) type StepFunction = dyn Fn(
    Context,
    serde_json::Value,
) -> std::panic::AssertUnwindSafe<
    futures_util::future::LocalBoxFuture<'static, anyhow::Result<serde_json::Value>>,
>;
