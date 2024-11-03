use std::{collections::HashMap, sync::Arc};

use futures_util::FutureExt;
use tonic::IntoRequest;
use tracing::{debug, error, info, warn};
use ustr::Ustr;

use crate::{
    step_function::Context,
    worker::{grpc::ActionType, DEFAULT_ACTION_TIMEOUT},
    Workflow,
};

use super::{
    grpc::{
        dispatcher_client::DispatcherClient, AssignedAction, StepActionEvent, StepActionEventType,
        WorkerListenRequest,
    },
    DataMap, ListenStrategy, ServiceWithAuthorization,
};

const DEFAULT_ACTION_LISTENER_RETRY_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5);
const DEFAULT_ACTION_LISTENER_RETRY_COUNT: usize = 15;

fn step_action_event(
    worker_id: &str,
    action: &AssignedAction,
    event_type: StepActionEventType,
    event_payload: String,
) -> StepActionEvent {
    StepActionEvent {
        worker_id: worker_id.to_owned(),
        job_id: action.job_id.to_owned(),
        job_run_id: action.job_run_id.to_owned(),
        step_id: action.step_id.to_owned(),
        step_run_id: action.step_run_id.to_owned(),
        action_id: action.action_id.to_owned(),
        event_timestamp: Some(std::time::SystemTime::now().into()),
        event_type: event_type as i32,
        event_payload,
    }
}

#[derive(serde::Deserialize)]
struct ActionInput<T> {
    input: T,
    parents: HashMap<Ustr, serde_json::Value>,
}

async fn handle_start_step_run(
    action_function_task_join_set: &mut tokio::task::JoinSet<crate::InternalResult<()>>,
    abort_handles: &mut HashMap<String, tokio::task::AbortHandle>,
    dispatcher: &mut DispatcherClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ServiceWithAuthorization,
        >,
    >,
    workflow_service_client: super::grpc::workflow_service_client::WorkflowServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ServiceWithAuthorization,
        >,
    >,
    namespace: &str,
    worker_id: &str,
    workflows: &[Workflow],
    action: AssignedAction,
    data: Arc<DataMap>,
) -> crate::InternalResult<()> {
    let Some(action_callable) = workflows
        .iter()
        .flat_map(|workflow| workflow.actions(namespace))
        .find(|(key, _)| *key == action.action_id)
        .map(|(_, callable)| callable)
    else {
        warn!("Could not find action {}", action.action_id);
        return Ok(());
    };

    debug!("Received a new action: {action:?}.");

    dispatcher
        .send_step_action_event(step_action_event(
            worker_id,
            &action,
            StepActionEventType::StepEventTypeStarted,
            Default::default(),
        ))
        .await
        .map_err(crate::InternalError::CouldNotSendStepStatus)?
        .into_inner();

    let input: ActionInput<serde_json::Value> = serde_json::from_str(&action.action_payload)
        .map_err(crate::InternalError::CouldNotDecodeActionPayload)?;

    let workflow_run_id = action.workflow_run_id.clone();
    let workflow_step_run_id = action.step_run_id.clone();

    let mut dispatcher = dispatcher.clone();

    let worker_id = worker_id.to_string();
    let step_run_id = action.step_run_id.clone();
    let abort_handle = action_function_task_join_set.spawn(async move {
        let context = Context::new(
            input.parents,
            workflow_run_id,
            workflow_step_run_id,
            workflow_service_client,
            data,
        );
        let action_event = match action_callable(context, input.input).catch_unwind().await {
            Ok(Ok(output_value)) => step_action_event(
                &worker_id,
                &action,
                StepActionEventType::StepEventTypeCompleted,
                serde_json::to_string(&output_value).expect("must succeed"),
            ),
            Ok(Err(error)) => step_action_event(
                &worker_id,
                &action,
                StepActionEventType::StepEventTypeFailed,
                error.to_string(),
            ),
            Err(_) => step_action_event(
                &worker_id,
                &action,
                StepActionEventType::StepEventTypeFailed,
                "action panicked".to_owned(),
            ),
        };

        dispatcher
            .send_step_action_event(action_event)
            .await
            .map_err(crate::InternalError::CouldNotSendStepStatus)?
            .into_inner();

        Ok(())
    });
    abort_handles.insert(step_run_id, abort_handle);

    Ok(())
}

async fn handle_cancel_step_run(
    abort_handles: &mut HashMap<String, tokio::task::AbortHandle>,
    action: AssignedAction,
) -> crate::InternalResult<()> {
    if let Some(abort_handle) = abort_handles.remove(&action.step_run_id) {
        abort_handle.abort();
    } else {
        warn!(
            "Could not find the abort handle for the workflow run ID: {}",
            action.step_run_id
        );
    }

    Ok(())
}

pub(crate) async fn run(
    action_function_task_join_set: &mut tokio::task::JoinSet<crate::InternalResult<()>>,
    mut dispatcher: DispatcherClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ServiceWithAuthorization,
        >,
    >,
    workflow_service_client: super::grpc::workflow_service_client::WorkflowServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ServiceWithAuthorization,
        >,
    >,
    namespace: &str,
    worker_id: &str,
    workflows: Vec<Workflow>,
    listener_v2_timeout: Option<u64>,
    mut interrupt_receiver: tokio::sync::mpsc::Receiver<()>,
    data: Arc<DataMap>,
) -> crate::InternalResult<()> {
    use futures_util::StreamExt;

    let mut retries: usize = 0;
    let mut listen_strategy = ListenStrategy::V2;

    let connection_attempt = tokio::time::Instant::now();

    let mut abort_handles = HashMap::new();

    'main_loop: loop {
        info!("Listening…");

        if connection_attempt.elapsed() > DEFAULT_ACTION_LISTENER_RETRY_INTERVAL {
            retries = 0;
        }
        if retries > DEFAULT_ACTION_LISTENER_RETRY_COUNT {
            return Err(crate::InternalError::CouldNotSubscribeToActions(
                DEFAULT_ACTION_LISTENER_RETRY_COUNT,
            ));
        }

        let response = match listen_strategy {
            ListenStrategy::V1 => {
                info!("Using strategy v1");

                let mut request = WorkerListenRequest {
                    worker_id: worker_id.to_owned(),
                }
                .into_request();
                request.set_timeout(DEFAULT_ACTION_TIMEOUT);
                Box::new(dispatcher.listen(request)).boxed()
            }
            ListenStrategy::V2 => {
                info!("Using strategy v2");

                let mut request = WorkerListenRequest {
                    worker_id: worker_id.to_owned(),
                }
                .into_request();
                if let Some(listener_v2_timeout) = listener_v2_timeout {
                    request.set_timeout(std::time::Duration::from_millis(listener_v2_timeout));
                }
                dispatcher.listen_v2(request).boxed()
            }
        };

        let mut stream = tokio::select! {
            response = response => {
                response
                    .map_err(crate::InternalError::CouldNotListenToDispatcher)?
                    .into_inner()
            }
            result = interrupt_receiver.recv() => {
                assert!(result.is_some());
                warn!("Interrupt received.");
                break 'main_loop;
            }
        };

        loop {
            tokio::select! {
                element = stream.next() => {
                    let Some(result) = element else {
                        break 'main_loop;
                    };

                    let action = match result {
                        Err(status) => match status.code() {
                            tonic::Code::Cancelled => {
                                return Ok(());
                            }
                            tonic::Code::DeadlineExceeded => {
                                continue 'main_loop;
                            }
                            tonic::Code::Unimplemented => {
                                listen_strategy = ListenStrategy::V1;
                                continue 'main_loop;
                            }
                            _other => {
                                retries += 1;
                                continue 'main_loop;
                            }
                        },
                        Ok(action) => action,
                    };

                    let action_type = match ActionType::try_from(action.action_type) {
                        Ok(action_type) => action_type,
                        Err(_) => {
                            error!("Unknown action type: {}", action.action_type);
                            continue 'main_loop;
                        }
                    };

                    match action_type {
                        ActionType::StartStepRun => {
                            handle_start_step_run(action_function_task_join_set, &mut abort_handles, &mut dispatcher, workflow_service_client.clone(),  namespace, worker_id, &workflows, action, data.clone()).await?;
                        }
                        ActionType::CancelStepRun => {
                            handle_cancel_step_run(&mut abort_handles, action).await?;
                        }
                        ActionType::StartGetGroupKey => {
                            todo!()
                        }
                    }
                }
                result = interrupt_receiver.recv() => {
                    assert!(result.is_some());
                    warn!("Interrupt received.");
                    break 'main_loop;
                }
            }
        }
    }

    Ok(())
}
