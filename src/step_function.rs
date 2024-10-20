use futures_util::lock::Mutex;
use tracing::info;

use crate::worker::{grpc, ServiceWithAuthorization};

pub struct Context {
    workflow_run_id: String,
    workflow_step_run_id: String,
    workflow_service_client_and_spawn_index: Mutex<(
        grpc::workflow_service_client::WorkflowServiceClient<
            tonic::service::interceptor::InterceptedService<
                tonic::transport::Channel,
                ServiceWithAuthorization,
            >,
        >,
        u16,
    )>,
}

impl Context {
    pub(crate) fn new(
        workflow_run_id: String,
        workflow_step_run_id: String,
        workflow_service_client: grpc::workflow_service_client::WorkflowServiceClient<
            tonic::service::interceptor::InterceptedService<
                tonic::transport::Channel,
                ServiceWithAuthorization,
            >,
        >,
    ) -> Self {
        Self {
            workflow_run_id,
            workflow_service_client_and_spawn_index: Mutex::new((workflow_service_client, 0)),
            workflow_step_run_id,
        }
    }

    pub async fn trigger_workflow<I: serde::Serialize>(
        &self,
        workflow_name: &str,
        input: I,
    ) -> anyhow::Result<()> {
        info!("Scheduling another workflow {workflow_name}");
        let mut mutex_guard = self.workflow_service_client_and_spawn_index.lock().await;
        let (workflow_service_client, spawn_index) = &mut *mutex_guard;
        let response = workflow_service_client
            .trigger_workflow(grpc::TriggerWorkflowRequest {
                name: workflow_name.to_owned(),
                input: serde_json::to_string(&input).expect("must succeed"),
                parent_id: Some(self.workflow_run_id.clone()),
                parent_step_run_id: Some(self.workflow_step_run_id.clone()),
                child_index: Some(*spawn_index as i32),
                child_key: None,
                additional_metadata: None, // FIXME: Add support.
                desired_worker_id: None,   // FIXME: Add support.
                priority: Some(1),         // FIXME: Add support.
            })
            .await
            .map_err(crate::InternalError::CouldNotTriggerWorkflow)
            .map_err(crate::Error::Internal)?
            .into_inner();
        info!(
            "Scheduled another workflow run ID: {}",
            response.workflow_run_id
        );
        *spawn_index += 1;
        Ok(())
    }
}

pub(crate) type StepFunction =
    dyn Fn(
        Context,
        serde_json::Value,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<serde_json::Value>>;
