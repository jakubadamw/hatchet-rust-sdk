mod heartbeat;
mod listener;

use grpc::{
    CreateWorkflowJobOpts, CreateWorkflowStepOpts, CreateWorkflowVersionOpts, PutWorkflowRequest,
    WorkerRegisterRequest, WorkerRegisterResponse,
};
use secrecy::{ExposeSecret, SecretString};
use tonic::transport::Certificate;

use crate::{client::Environment, ClientTlStrategy, Workflow};

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(private, name = "build_private"))]
pub struct Worker<'a> {
    name: String,
    max_runs: Option<i32>,
    #[builder(vis = "pub(crate)")]
    environment: &'a super::client::Environment,
    #[builder(default, setter(skip))]
    workflows: Vec<Workflow>,
}

impl<'a> WorkerBuilder<'a> {
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

fn construct_endpoint_url<'a>(
    host_port_in_environment: Option<&'a str>,
    token: &SecretString,
) -> crate::Result<String> {
    use secrecy::ExposeSecret;
    Ok(format!(
        "https://{}",
        host_port_in_environment
            .map(|value| crate::Result::Ok(std::borrow::Cow::Borrowed(value)))
            .unwrap_or_else(|| {
                let key = jsonwebtoken::DecodingKey::from_secret(&[]);
                let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
                validation.insecure_disable_signature_validation();
                let data: jsonwebtoken::TokenData<TokenClaims> =
                    jsonwebtoken::decode(token.expose_secret(), &key, &validation)
                        .map_err(crate::Error::CouldNotDecodeToken)?;
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
    token: &SecretString,
) -> crate::Result<tonic::transport::Endpoint> {
    let mut endpoint = tonic::transport::Endpoint::new(construct_endpoint_url(host_port, token)?)?;
    endpoint = match tls_strategy {
        crate::ClientTlStrategy::None => todo!(),
        crate::ClientTlStrategy::Tls | crate::ClientTlStrategy::MTls => {
            let mut tls_config = tonic::transport::channel::ClientTlsConfig::new();
            if let Some(domain_name) = tls_server_name {
                tls_config = tls_config.domain_name(domain_name);
            };
            let extra_root_certificate = match (tls_root_ca, tls_root_ca_file) {
                (Some(_), Some(_)) => {
                    return Err(crate::Error::CantSetBothEnvironmentVariables(
                        "HATCHET_CLIENT_TLS_ROOT_CA",
                        "HATCHET_CLIENT_TLS_ROOT_CA_FILE",
                    ));
                }
                (Some(tls_root_ca), None) => {
                    Some(std::borrow::Cow::Borrowed(tls_root_ca.as_bytes()))
                }
                (None, Some(tls_root_ca_file)) => Some(std::borrow::Cow::Owned(
                    tokio::fs::read(tls_root_ca_file).await.map_err(|err| {
                        crate::Error::CouldNotReadFile(err, tls_root_ca_file.to_owned())
                    })?,
                )),
                (None, None) => None,
            };
            if let Some(extra_root_certificate) = extra_root_certificate {
                tls_config =
                    tls_config.ca_certificate(Certificate::from_pem(extra_root_certificate));
            }
            endpoint.tls_config(tls_config)?
        }
    };
    Ok(endpoint)
}

impl<'a> Worker<'a> {
    pub async fn register_workflow(&mut self, workflow: crate::workflow::Workflow) {
        self.workflows.push(workflow);
    }

    pub async fn start(self) -> crate::Result<()> {
        use tonic::IntoRequest;

        let (heartbeat_interrupt_sender1, heartbeat_interrupt_receiver) =
            tokio::sync::mpsc::channel(1);
        let (listening_interrupt_sender1, listening_interrupt_receiver) =
            tokio::sync::mpsc::channel(1);
        let heartbeat_interrupt_sender2 = heartbeat_interrupt_sender1.clone();
        ctrlc_async::set_async_handler(async move {
            // Ignore the error.
            let _ = heartbeat_interrupt_sender1.send(()).await;
            let _ = listening_interrupt_sender1.send(()).await;
        })
        .expect("Error setting Ctrl-C handler");

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
        } = self.environment;

        let endpoint = construct_endpoint(
            tls_server_name.as_deref(),
            *tls_strategy,
            tls_root_ca_file.as_deref(),
            tls_root_ca.as_deref(),
            host_port.as_deref(),
            token,
        )
        .await?;

        let authorization_header: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
            format!("Bearer {token}", token = token.expose_secret())
                .parse()
                .expect("must parse successfully");
        let authorization_header_cloned = authorization_header.clone();

        let mut workflow_service_client =
            grpc::workflow_service_client::WorkflowServiceClient::with_interceptor(
                endpoint.connect().await?,
                move |mut request: tonic::Request<()>| {
                    request
                        .metadata_mut()
                        .insert("authorization", authorization_header.clone());
                    Ok(request)
                },
            );

        for workflow in &self.workflows {
            let namespaced_workflow_name =
                format!("{namespace}{workflow_name}", workflow_name = workflow.name);
            workflow_service_client
                .put_workflow(PutWorkflowRequest {
                    opts: Some(CreateWorkflowVersionOpts {
                        description: workflow.description.clone(),
                        version: workflow.version.clone(),
                        event_triggers: workflow
                            .on_events
                            .iter()
                            .map(|event| format!("{namespace}{event}"))
                            .collect(),
                        cron_triggers: vec![],
                        scheduled_triggers: vec![],
                        jobs: vec![CreateWorkflowJobOpts {
                            name: workflow.name.clone(),
                            description: workflow.description.clone(),
                            steps: workflow
                                .steps
                                .iter()
                                .map(|step| CreateWorkflowStepOpts {
                                    readable_id: step.name.clone(),
                                    action: format!(
                                        "{namespaced_workflow_name}:{name}",
                                        name = step.name
                                    ),
                                    timeout: step.timeout.as_secs().to_string(),
                                    inputs: "{}".to_owned(),
                                    parents: step.parents.clone(),
                                    user_data: Default::default(),
                                    retries: step.retries as i32,
                                    rate_limits: Default::default(), // FIXME: Implement.
                                    worker_labels: Default::default(), // FIXME: Implement.
                                })
                                .collect(),
                        }],
                        name: namespaced_workflow_name,
                        concurrency: None, // FIXME: Implement.
                        schedule_timeout: Some(workflow.schedule_timeout.as_secs().to_string()),
                        cron_input: None,     // FIXME: Implement.
                        on_failure_job: None, // FIXME: Implement.
                        sticky: None,         // FIXME: Implement.
                        kind: None,
                    }),
                })
                .await?;
        }

        // FIXME: Account for all the settings from `self.environment`.
        let mut dispatcher = {
            grpc::dispatcher_client::DispatcherClient::with_interceptor(
                endpoint.connect().await?,
                move |mut request: tonic::Request<()>| {
                    request
                        .metadata_mut()
                        .insert("authorization", authorization_header_cloned.clone());
                    Ok(request)
                },
            )
        };

        let request = {
            let mut request: tonic::Request<WorkerRegisterRequest> = WorkerRegisterRequest {
                worker_name: self.name.clone(),
                max_runs: self.max_runs,
                // FIXME: Implement.
                ..Default::default()
            }
            .into_request();
            request.set_timeout(DEFAULT_REGISTER_TIMEOUT);
            request
        };

        let WorkerRegisterResponse { worker_id, .. } =
            dispatcher.register(request).await?.into_inner();

        futures_util::try_join! {
            heartbeat::run(dispatcher.clone(), &worker_id, heartbeat_interrupt_receiver),
            listener::run(dispatcher, namespace, &worker_id, self.workflows, *listener_v2_timeout, listening_interrupt_receiver, heartbeat_interrupt_sender2),
        }?;

        Ok(())
    }
}
