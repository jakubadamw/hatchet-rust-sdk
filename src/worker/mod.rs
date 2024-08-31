mod heartbeat;
mod listener;

use grpc::{WorkerRegisterRequest, WorkerRegisterResponse};
use secrecy::ExposeSecret;
use tonic::transport::Certificate;

use crate::{client::Environment, Workflow};

#[derive(derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct Worker<'a> {
    name: String,
    max_runs: Option<i32>,
    #[builder(vis = "pub(crate)")]
    environment: &'a super::client::Environment,
    #[builder(setter(skip))]
    workflows: Vec<Workflow>,
}

#[allow(clippy::enum_variant_names)]
pub mod grpc {
    tonic::include_proto!("dispatcher");
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
    token: &'a str,
) -> crate::Result<String> {
    Ok(format!(
        "https://{}",
        host_port_in_environment
            .map(|value| crate::Result::Ok(std::borrow::Cow::Borrowed(value)))
            .unwrap_or_else(|| {
                let key = jsonwebtoken::DecodingKey::from_secret(&[]);
                let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
                validation.insecure_disable_signature_validation();
                let data: jsonwebtoken::TokenData<TokenClaims> =
                    jsonwebtoken::decode(token, &key, &validation)
                        .map_err(crate::Error::CouldNotDecodeToken)?;
                Ok(data.claims.grpc_broadcast_address.into())
            })?
    ))
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

        // FIXME: Account for all the settings from `self.environment`.
        let mut dispatcher = {
            let mut endpoint = tonic::transport::Endpoint::new(construct_endpoint_url(
                host_port.as_deref(),
                token.expose_secret(),
            )?)?;
            endpoint = match tls_strategy {
                crate::ClientTlStrategy::None => todo!(),
                crate::ClientTlStrategy::Tls | crate::ClientTlStrategy::MTls => {
                    let mut tls_config = tonic::transport::channel::ClientTlsConfig::new();
                    if let Some(domain_name) = tls_server_name.as_deref() {
                        tls_config = tls_config.domain_name(domain_name);
                    };
                    let extra_root_certificate =
                        match (tls_root_ca.as_deref(), tls_root_ca_file.as_deref()) {
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
                        tls_config = tls_config
                            .ca_certificate(Certificate::from_pem(extra_root_certificate));
                    }
                    endpoint.tls_config(tls_config)?
                }
            };
            let connection = endpoint.connect().await?;
            let authorization_hedaer: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {token}", token = token.expose_secret())
                    .parse()
                    .expect("must parse successfully");

            grpc::dispatcher_client::DispatcherClient::with_interceptor(
                connection,
                move |mut request: tonic::Request<()>| {
                    request
                        .metadata_mut()
                        .insert("authorization", authorization_hedaer.clone());
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
