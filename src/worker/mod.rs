mod heartbeat;
mod listener;

use std::sync::Arc;

use grpc::{
    CreateWorkflowJobOpts, CreateWorkflowStepOpts, CreateWorkflowVersionOpts, PutWorkflowRequest,
    WorkerRegisterRequest, WorkerRegisterResponse, WorkflowKind,
};
use tonic::transport::Certificate;
use tracing::info;

use crate::{client::Environment, step_function::DataMap, ClientTlStrategy, Workflow};

#[derive(Clone)]
pub(crate) struct ServiceWithAuthorization {
    authorization_header_value: secrecy::SecretString,
}

impl ServiceWithAuthorization {
    fn new(token: secrecy::SecretString) -> Self {
        use secrecy::ExposeSecret;

        Self {
            authorization_header_value: format!("Bearer {token}", token = token.expose_secret())
                .into(),
        }
    }
}

impl tonic::service::Interceptor for ServiceWithAuthorization {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        use secrecy::ExposeSecret;
        let authorization_header_value: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
            self.authorization_header_value
                .expose_secret()
                .parse()
                .expect("must parse successfully");
        request
            .metadata_mut()
            .insert("authorization", authorization_header_value);
        Ok(request)
    }
}

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(private, name = "build_private"))]
pub struct Worker<'a> {
    #[builder(default, setter(into))]
    name: String,
    #[builder(default, setter(into))]
    max_runs: Option<i32>,
    #[builder(vis = "pub(crate)")]
    environment: &'a super::client::Environment,
    #[builder(default, setter(skip))]
    workflows: Vec<Workflow>,
    #[builder(default, setter(custom))]
    data: DataMap,
}

impl<'a> WorkerBuilder<'a> {
    pub fn datum<D: std::any::Any + Send + Sync>(mut self, datum: D) -> Self {
        self.data
            .get_or_insert_default()
            .insert(std::any::TypeId::of::<D>(), Box::new(datum));
        self
    }

    pub fn build(self) -> Worker<'a> {
        self.build_private().expect("must succeed")
    }
}

#[allow(clippy::enum_variant_names)]
pub mod grpc {
    tonic::include_proto!("dispatcher");
    tonic::include_proto!("workflows");
}

enum ListenStrategy {
    V1,
    V2,
}

const DEFAULT_ACTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const DEFAULT_REGISTER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(serde::Deserialize)]
struct TokenClaims {
    grpc_broadcast_address: String,
}

fn construct_endpoint_url(
    tls_strategy: ClientTlStrategy,
    host_port_in_environment: Option<&str>,
    token: &secrecy::SecretString,
) -> crate::InternalResult<String> {
    use secrecy::ExposeSecret;

    let protocol = match tls_strategy {
        ClientTlStrategy::None => "http",
        ClientTlStrategy::Tls | ClientTlStrategy::MTls => "https",
    };

    Ok(format!(
        "{protocol}://{}",
        host_port_in_environment
            .map(|value| crate::InternalResult::Ok(std::borrow::Cow::Borrowed(value)))
            .unwrap_or_else(|| {
                let key = jsonwebtoken::DecodingKey::from_secret(&[]);
                let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::ES256);
                validation.insecure_disable_signature_validation();
                validation.validate_aud = false;
                let data: jsonwebtoken::TokenData<TokenClaims> =
                    jsonwebtoken::decode(token.expose_secret(), &key, &validation)
                        .map_err(crate::InternalError::CouldNotDecodeToken)?;
                Ok(data.claims.grpc_broadcast_address.into())
            })?
    ))
}

async fn construct_endpoint(
    tls_server_name: Option<&str>,
    tls_strategy: ClientTlStrategy,
    tls_root_ca_file: Option<&str>,
    tls_root_ca: Option<&str>,
    host_port: Option<&str>,
    token: &secrecy::SecretString,
) -> crate::InternalResult<tonic::transport::Endpoint> {
    let mut endpoint =
        tonic::transport::Endpoint::new(construct_endpoint_url(tls_strategy, host_port, token)?)
            .expect("endpoint must be valid");
    endpoint = match tls_strategy {
        crate::ClientTlStrategy::None => endpoint,
        crate::ClientTlStrategy::Tls | crate::ClientTlStrategy::MTls => {
            let mut tls_config =
                tonic::transport::channel::ClientTlsConfig::new().with_enabled_roots();
            if let Some(domain_name) = tls_server_name {
                tls_config = tls_config.domain_name(domain_name);
            };
            let extra_root_certificate = match (tls_root_ca, tls_root_ca_file) {
                (Some(_), Some(_)) => {
                    return Err(crate::InternalError::CantSetBothEnvironmentVariables(
                        "HATCHET_CLIENT_TLS_ROOT_CA",
                        "HATCHET_CLIENT_TLS_ROOT_CA_FILE",
                    ));
                }
                (Some(tls_root_ca), None) => {
                    Some(std::borrow::Cow::Borrowed(tls_root_ca.as_bytes()))
                }
                (None, Some(tls_root_ca_file)) => Some(std::borrow::Cow::Owned(
                    tokio::fs::read(tls_root_ca_file).await.map_err(|err| {
                        crate::InternalError::CouldNotReadFile(err, tls_root_ca_file.to_owned())
                    })?,
                )),
                (None, None) => None,
            };
            if let Some(extra_root_certificate) = extra_root_certificate {
                tls_config =
                    tls_config.ca_certificate(Certificate::from_pem(extra_root_certificate));
            }
            endpoint
                .tls_config(tls_config)
                .expect("TLS config must be valid")
        }
    };
    Ok(endpoint)
}

impl Worker<'_> {
    pub fn register_workflow(&mut self, workflow: crate::workflow::Workflow) {
        self.workflows.push(workflow);
    }

    pub async fn start(self) -> crate::InternalResult<()> {
        use tonic::IntoRequest;

        let (heartbeat_interrupt_sender1, heartbeat_interrupt_receiver) =
            tokio::sync::mpsc::channel(1);
        let (listening_interrupt_sender1, listening_interrupt_receiver) =
            tokio::sync::mpsc::channel(1);
        let _listening_interrupt_sender2 = listening_interrupt_sender1.clone();
        let _heartbeat_interrupt_sender2 = heartbeat_interrupt_sender1.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            info!("Received an interrupt event.");
            // Ignore the error.
            let _ = heartbeat_interrupt_sender1.send(()).await;
            let _ = listening_interrupt_sender1.send(()).await;
        });

        let Self {
            workflows,
            data,
            name,
            max_runs,
            environment,
        } = self;

        let Environment {
            token,
            host_port,
            tls_strategy,
            tls_cert_file: _,
            tls_cert: _,
            tls_key_file: _,
            tls_key: _,
            tls_root_ca_file,
            tls_root_ca,
            tls_server_name,
            namespace,
            listener_v2_timeout,
        } = environment;

        let endpoint = construct_endpoint(
            tls_server_name.as_deref(),
            *tls_strategy,
            tls_root_ca_file.as_deref(),
            tls_root_ca.as_deref(),
            host_port.as_deref(),
            token,
        )
        .await?;

        let interceptor = ServiceWithAuthorization::new(token.clone());

        let mut workflow_service_client =
            grpc::workflow_service_client::WorkflowServiceClient::with_interceptor(
                endpoint
                    .connect()
                    .await
                    .map_err(crate::InternalError::CouldNotConnectToWorkflowService)?,
                interceptor.clone(),
            );

        let mut all_actions = vec![];

        let data = Arc::new(data);

        for workflow in &workflows {
            let namespaced_workflow_name =
                format!("{namespace}{workflow_name}", workflow_name = workflow.name);

            let opts = CreateWorkflowVersionOpts {
                description: workflow.description.clone(),
                version: workflow.version.clone(),
                event_triggers: workflow
                    .on_events
                    .iter()
                    .map(|event| format!("{namespace}{event}"))
                    .collect(),
                cron_triggers: vec![],
                scheduled_triggers: vec![],
                concurrency: None,
                jobs: vec![CreateWorkflowJobOpts {
                    name: workflow.name.clone(),
                    description: workflow.description.clone(),
                    steps: workflow
                        .steps
                        .iter()
                        .map(|step| CreateWorkflowStepOpts {
                            readable_id: step.name.clone(),
                            action: format!("{namespaced_workflow_name}:{name}", name = step.name),
                            timeout: format!("{}s", step.timeout.as_secs()),
                            inputs: "{}".to_owned(), // FIXME: Implement.
                            parents: step.parents.clone(),
                            user_data: Default::default(),
                            retries: step.retries as i32,
                            rate_limits: Default::default(), // FIXME: Implement.
                            worker_labels: Default::default(), // FIXME: Implement.
                        })
                        .collect(),
                }],
                name: namespaced_workflow_name,
                schedule_timeout: Some(format!("{}s", workflow.schedule_timeout.as_secs())),
                cron_input: None,     // FIXME: Implement.
                on_failure_job: None, // FIXME: Implement.
                sticky: None,         // FIXME: Implement.
                default_priority: Some(1),
                kind: Some(WorkflowKind::Dag as i32),
            };

            all_actions.extend(opts.jobs[0].steps.iter().map(|step| step.action.clone()));

            workflow_service_client
                .put_workflow(PutWorkflowRequest { opts: Some(opts) })
                .await
                .map_err(crate::InternalError::CouldNotPutWorkflow)?;
        }

        // FIXME: Account for all the settings from `self.environment`.
        let mut dispatcher = {
            grpc::dispatcher_client::DispatcherClient::with_interceptor(
                endpoint
                    .connect()
                    .await
                    .map_err(crate::InternalError::CouldNotConnectToDispatcher)?,
                interceptor.clone(),
            )
        };

        let request = {
            let mut request: tonic::Request<WorkerRegisterRequest> = WorkerRegisterRequest {
                worker_name: name,
                max_runs,
                services: vec!["default".to_owned()],
                actions: all_actions,
                // FIXME: Implement.
                ..Default::default()
            }
            .into_request();
            request.set_timeout(DEFAULT_REGISTER_TIMEOUT);
            request
        };

        let WorkerRegisterResponse { worker_id, .. } = dispatcher
            .register(request)
            .await
            .map_err(crate::InternalError::CouldNotRegisterWorker)?
            .into_inner();

        let local_pool_handle = tokio_util::task::LocalPoolHandle::new(num_cpus::get());

        futures_util::try_join! {
            heartbeat::run(dispatcher.clone(), &worker_id, heartbeat_interrupt_receiver),
            listener::run(&local_pool_handle, dispatcher, workflow_service_client, namespace, &worker_id, workflows, *listener_v2_timeout, listening_interrupt_receiver, data)
        }?;

        Ok(())
    }
}
